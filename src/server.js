require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });

const crypto = require('crypto');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');
const express = require('express');
const helmet = require('helmet');
const morgan = require('morgan');
const { DateTime } = require('luxon');
const anthropicApiKey = String(process.env.ANTHROPIC_API_KEY || '').trim();
const claudeModel = String(process.env.CLAUDE_MODEL || 'claude-haiku-4-5-20251001').trim();
const claudeTimeoutMs = Number(process.env.CLAUDE_TIMEOUT_MS || 3000);

const {
  port,
  webhookSecret,
  adminApiKey,
  timezone
} = require('./config');
const { query, withTransaction } = require('./db');
const { parseAttendanceMessage, sanitizeMessageText } = require('./parser');

const execFileAsync = promisify(execFile);

const app = express();
app.use(helmet());
app.use(express.json({ limit: '1mb' }));
app.use(morgan('combined'));

function getBiweeklyBounds() {
  const zone = 'America/Los_Angeles';

  const anchorStart = DateTime.fromISO('2026-02-23T00:00:00', { zone }).startOf('day');
  const now = DateTime.now().setZone(zone);

  const diffDays = Math.floor(now.startOf('day').diff(anchorStart, 'days').days);
  const cycleIndex = Math.floor(diffDays / 14);

  const currentStart = anchorStart.plus({ days: cycleIndex * 14 });
  const currentEnd = currentStart.plus({ days: 14 });

  const previousStart = currentStart.minus({ days: 14 });
  const previousEnd = currentStart;

  return {
    currentStart,
    currentEnd,
    previousStart,
    previousEnd
  };
}

const archiveDir = path.resolve(__dirname, '../archives');
const AUTO_ARCHIVE_ENABLED =
  String(process.env.AUTO_ARCHIVE_ENABLED || 'true').toLowerCase() === 'true';
const AUTO_ARCHIVE_INTERVAL_MS = Number(
  process.env.AUTO_ARCHIVE_INTERVAL_MS || 60 * 60 * 1000
);
const AUTO_ARCHIVE_TABLE_ORDER = [
  'attendance_events',
  'review_queue',
  'raw_messages',
  'audit_log'
];

const ARCHIVE_TABLES = {
  attendance_events: {
    orderBy: 'event_time ASC, id ASC',
    defaultDateColumn: 'event_time',
    columns: [
      'id',
      'raw_message_id',
      'employee_id',
      'employee_email',
      'event_type',
      'event_time',
      'timezone',
      'source',
      'review_id',
      'created_at'
    ]
  },
  raw_messages: {
    orderBy: 'message_timestamp ASC, id ASC',
    defaultDateColumn: 'message_timestamp',
    columns: [
      'id',
      'employee_id',
      'employee_name',
      'employee_email',
      'channel_id',
      'message_id',
      'reply_to_id',
      'message_text',
      'message_timestamp',
      'normalized_text',
      'parse_status',
      'parse_reason',
      'message_hash',
      'was_edited',
      'created_at'
    ]
  },
  review_queue: {
    orderBy: 'created_at ASC, id ASC',
    defaultDateColumn: 'created_at',
    columns: [
      'id',
      'raw_message_id',
      'employee_id',
      'employee_name',
      'employee_email',
      'probable_intent',
      'suggested_event',
      'suggested_time',
      'reason_flagged',
      'openclaw_explanation',
      'status',
      'reviewed_by',
      'reviewed_at',
      'review_notes',
      'created_at'
    ]
  },
  audit_log: {
    orderBy: 'created_at ASC, id ASC',
    defaultDateColumn: 'created_at',
    columns: [
      'id',
      'entity_type',
      'entity_id',
      'action',
      'old_value',
      'new_value',
      'performed_by',
      'notes',
      'created_at'
    ]
  }
};

function normalizeArchiveTables(input) {
  const allowed = Object.keys(ARCHIVE_TABLES);

  let raw = input;

  if (!raw || raw === 'all') {
    return { tables: AUTO_ARCHIVE_TABLE_ORDER, invalid: [] };
  }

  if (typeof raw === 'string') {
    raw = raw.split(',').map((v) => v.trim()).filter(Boolean);
  }

  if (!Array.isArray(raw) || raw.length === 0) {
    return { tables: AUTO_ARCHIVE_TABLE_ORDER, invalid: [] };
  }

  const invalid = raw.filter((t) => !allowed.includes(t));
  const tables = raw.filter((t) => allowed.includes(t));

  return {
    tables: tables.length ? tables : AUTO_ARCHIVE_TABLE_ORDER,
    invalid
  };
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  let normalized = value;

  if (normalized instanceof Date) {
    normalized = normalized.toISOString();
  } else if (typeof normalized === 'object') {
    normalized = JSON.stringify(normalized);
  } else {
    normalized = String(normalized);
  }

  const escaped = String(normalized).replace(/"/g, '""');
  return `"${escaped}"`;
}

function rowsToCsv(columns, rows) {
  const header = columns.map(csvEscape).join(',');
  const lines = rows.map((row) => columns.map((col) => csvEscape(row[col])).join(','));
  return [header, ...lines].join('');
}

async function ensureArchiveDir() {
  await fsp.mkdir(archiveDir, { recursive: true });
}

function getExactArchiveWindow() {
  const bounds = getBiweeklyBounds();

  const archiveStart = bounds.previousStart.minus({ days: 14 }).toUTC();
  const archiveEnd = bounds.previousStart.toUTC();

  return {
    archiveStartIso: archiveStart.toISO(),
    archiveEndIso: archiveEnd.toISO(),
    fileStartLabel: archiveStart.toISODate(),
    fileEndLabel: archiveEnd.minus({ days: 1 }).toISODate()
  };
}

function buildArchiveCsvFileName({ tableName, fileStartLabel, fileEndLabel, stamp }) {
  return `${tableName}_${fileStartLabel}_to_${fileEndLabel}_${stamp}.csv`;
}

function buildArchiveZipFileName({ tableName, fileStartLabel, fileEndLabel, stamp }) {
  return `${tableName}_${fileStartLabel}_to_${fileEndLabel}_${stamp}.zip`;
}

async function zipCsvFile(csvPath, zipPath) {
  await execFileAsync('zip', ['-j', '-q', '-9', zipPath, csvPath]);
}

async function hasArchiveManifestForWindow(client, tableName, archiveEndIso) {
  const result = await client.query(
    `
    SELECT id, table_name, file_name, file_path, cutoff_time, row_count, checksum_sha256, status, performed_by, created_at, notes
    FROM archive_manifest
    WHERE table_name = $1
      AND cutoff_time = $2::timestamptz
      AND status IN ('exported', 'verified')
    ORDER BY id DESC
    LIMIT 1
    `,
    [tableName, archiveEndIso]
  );

  return result.rowCount ? result.rows[0] : null;
}

async function previewArchiveTable(client, tableName) {
  const config = ARCHIVE_TABLES[tableName];
  const { archiveStartIso, archiveEndIso, fileStartLabel, fileEndLabel } = getExactArchiveWindow();

  const countResult = await client.query(
    `SELECT COUNT(*)::int AS row_count
     FROM ${tableName}
     WHERE ${config.defaultDateColumn} >= $1::timestamptz
       AND ${config.defaultDateColumn} < $2::timestamptz`,
    [archiveStartIso, archiveEndIso]
  );

  const sampleResult = await client.query(
    `SELECT ${config.columns.join(', ')}
     FROM ${tableName}
     WHERE ${config.defaultDateColumn} >= $1::timestamptz
       AND ${config.defaultDateColumn} < $2::timestamptz
     ORDER BY ${config.orderBy}
     LIMIT 5`,
    [archiveStartIso, archiveEndIso]
  );

  const alreadyArchived = await hasArchiveManifestForWindow(client, tableName, archiveEndIso);

  return {
    table_name: tableName,
    archive_start: archiveStartIso,
    archive_end: archiveEndIso,
    file_start_label: fileStartLabel,
    file_end_label: fileEndLabel,
    row_count: countResult.rows[0].row_count,
    already_archived: Boolean(alreadyArchived),
    existing_manifest_id: alreadyArchived?.id || null,
    existing_file_name: alreadyArchived?.file_name || null,
    sample_rows: sampleResult.rows
  };
}

async function exportArchiveTable(client, tableName, performedBy) {
  const config = ARCHIVE_TABLES[tableName];

  if (!config) {
    throw new Error(`Unsupported archive table: ${tableName}`);
  }

  const { archiveStartIso, archiveEndIso, fileStartLabel, fileEndLabel } =
    getExactArchiveWindow();

  const alreadyArchived = await hasArchiveManifestForWindow(
    client,
    tableName,
    archiveEndIso
  );

  if (alreadyArchived) {
    return {
      ...alreadyArchived,
      notes: `Skipped: already archived pay period ${fileStartLabel} to ${fileEndLabel}`
    };
  }

  const dateColumn = config.defaultDateColumn;

  const rowsResult = await client.query(
    `SELECT ${config.columns.join(', ')}
     FROM ${tableName}
     WHERE ${dateColumn} >= $1::timestamptz
       AND ${dateColumn} < $2::timestamptz
     ORDER BY ${config.orderBy}`,
    [archiveStartIso, archiveEndIso]
  );

  const rows = rowsResult.rows;
  const rowCount = rowsResult.rowCount;
  const csv = rowsToCsv(config.columns, rows);
  const stamp = DateTime.now()
    .setZone('America/Los_Angeles')
    .toFormat('yyyyLLdd_HHmmss');
  const csvFileName = buildArchiveCsvFileName({ tableName, fileStartLabel, fileEndLabel, stamp });
  const zipFileName = buildArchiveZipFileName({ tableName, fileStartLabel, fileEndLabel, stamp });
  const csvPath = path.join(archiveDir, csvFileName);
  const zipPath = path.join(archiveDir, zipFileName);

  await ensureArchiveDir();
  await fsp.writeFile(csvPath, csv, 'utf8');
  await zipCsvFile(csvPath, zipPath);
  await fsp.unlink(csvPath);

  const zipBuffer = await fsp.readFile(zipPath);
  const checksum = crypto.createHash('sha256').update(zipBuffer).digest('hex');

  const manifestResult = await client.query(
    `
    INSERT INTO archive_manifest (
      table_name,
      file_name,
      file_path,
      cutoff_time,
      row_count,
      checksum_sha256,
      status,
      performed_by,
      notes
    )
    VALUES ($1,$2,$3,$4::timestamptz,$5,$6,'exported',$7,$8)
    RETURNING id, table_name, file_name, file_path, cutoff_time, row_count, checksum_sha256, status, performed_by, created_at, notes
    `,
    [
      tableName,
      zipFileName,
      zipPath,
      archiveEndIso,
      rowCount,
      checksum,
      performedBy,
      rowCount > 0
        ? `Archived exact pay period ${fileStartLabel} to ${fileEndLabel}`
        : `No rows matched exact pay period ${fileStartLabel} to ${fileEndLabel}`
    ]
  );

  if (rowCount > 0) {
    await client.query(
      `DELETE FROM ${tableName}
       WHERE ${dateColumn} >= $1::timestamptz
         AND ${dateColumn} < $2::timestamptz`,
      [archiveStartIso, archiveEndIso]
    );
  }

  await client.query(
    `
    INSERT INTO audit_log (
      entity_type,
      entity_id,
      action,
      old_value,
      new_value,
      performed_by,
      notes
    )
    VALUES ('archive_manifest', $1, 'export_zip', $2::jsonb, $3::jsonb, $4, $5)
    `,
    [
      String(manifestResult.rows[0].id),
      JSON.stringify({}),
      JSON.stringify({
        tableName,
        fileName: zipFileName,
        rowCount,
        archiveStartIso,
        archiveEndIso,
        checksum
      }),
      performedBy,
      rowCount > 0
        ? `Archive export + delete completed for ${fileStartLabel} to ${fileEndLabel}`
        : `Archive window had no matching rows for ${fileStartLabel} to ${fileEndLabel}`
    ]
  );

  return manifestResult.rows[0];
}

async function runAutomaticArchive(reason = 'scheduled_auto_archive') {
  return withTransaction(async (client) => {
    const manifests = [];

    for (const tableName of AUTO_ARCHIVE_TABLE_ORDER) {
      const manifest = await exportArchiveTable(client, tableName, reason);
      manifests.push(manifest);
    }

    return {
      archiveWindow: getExactArchiveWindow(),
      manifests
    };
  });
}

function startAutoArchiveScheduler() {
  if (!AUTO_ARCHIVE_ENABLED) {
    console.log('Auto archive disabled');
    return;
  }

  const runOnce = async () => {
    try {
      const result = await runAutomaticArchive('auto_scheduler');
      console.log(
        'Auto archive completed:',
        JSON.stringify({
          archiveWindow: result.archiveWindow,
          manifests: result.manifests.map((m) => ({
            table_name: m.table_name,
            row_count: m.row_count,
            file_name: m.file_name,
            notes: m.notes
          }))
        })
      );
    } catch (err) {
      console.error('Auto archive failed:', err.message);
    }
  };

  setTimeout(runOnce, 10 * 1000);
  setInterval(runOnce, AUTO_ARCHIVE_INTERVAL_MS);

  console.log(
    `Auto archive scheduler started. Interval: ${AUTO_ARCHIVE_INTERVAL_MS} ms`
  );
}

function unauthorized(res) {
  return res.status(401).json({ ok: false, error: 'Unauthorized' });
}

function requireWebhookSecret(req, res, next) {
  if (!webhookSecret) return next();
  const provided = req.get('X-Webhook-Secret') || '';
  if (provided !== webhookSecret) return unauthorized(res);
  next();
}

function requireAdminApiKey(req, res, next) {
  if (!adminApiKey) return next();
  const provided = req.get('X-Admin-Api-Key') || '';
  if (provided !== adminApiKey) return unauthorized(res);
  next();
}

function deriveEmployeeLiveStatus(latestEventType) {
  switch (String(latestEventType || '').toUpperCase()) {
    case 'IN':
    case 'BREAK_END':
      return 'active';

    case 'BREAK_START':
    case 'OUT':
      return 'inactive';

    default:
      return 'unknown';
  }
}

function buildMessageHash({ employeeEmail, channelId, messageId, messageText, timestamp }) {
  if (messageId) return `teams:${messageId}`;
  return crypto
    .createHash('sha256')
    .update(`${employeeEmail}|${channelId}|${timestamp}|${messageText}`)
    .digest('hex');
}

async function getEmployeeByEmailOrName({ employeeEmail, employeeName }) {
  if (employeeEmail) {
    const byEmail = await query(
      `SELECT id, employee_name, employee_email, active
       FROM employees
       WHERE lower(employee_email) = lower($1)
       LIMIT 1`,
      [employeeEmail]
    );
    if (byEmail.rowCount) return byEmail.rows[0];
  }

  if (employeeName) {
    const byName = await query(
      `SELECT id, employee_name, employee_email, active
       FROM employees
       WHERE lower(employee_name) = lower($1)
       LIMIT 1`,
      [employeeName]
    );
    if (byName.rowCount) return byName.rows[0];
  }

  return null;
}

function isManualTimeEntry(messageText) {
  return /^(in|out|break|back)\s+at\s+/i.test(String(messageText || '').trim());
}

/*
  Duplicate-friendly sequence rules.

  Goal:
  - allow "in" -> "actually in"
  - allow "break" -> "actually break"
  - allow "back" -> "actually back"
  - allow repeated "out" too

  This server only decides whether the event can be logged.
  Final worked-hours calculation should still use a "latest open event wins" approach.
*/
function getAllowedNextEvents(lastEventType) {
  switch (String(lastEventType || '').toUpperCase()) {
    case '':
      return ['IN'];
    case 'IN':
      return ['IN', 'BREAK', 'OUT'];
    case 'BREAK':
      return ['BREAK', 'BACK', 'OUT'];
    case 'BACK':
      return ['BACK', 'BREAK', 'OUT'];
    case 'OUT':
      return ['IN', 'OUT'];
    default:
      return ['IN'];
  }
}

async function getLatestAttendanceEvent(client, employeeId) {
  const result = await client.query(
    `SELECT id, event_type, event_time, source
     FROM attendance_events
     WHERE employee_id = $1
     ORDER BY event_time DESC, id DESC
     LIMIT 1`,
    [employeeId]
  );

  return result.rowCount ? result.rows[0] : null;
}

function normalizeEventType(eventType) {
  const type = String(eventType || '').toUpperCase();

  if (type === 'BREAK_START') return 'BREAK';
  if (type === 'BREAK_END') return 'BACK';

  return type;
}

function normalizePayrollEventType(eventType) {
  return normalizeEventType(eventType);
}

function toPacificUtcIso(value) {
  if (!value) return null;

  if (value instanceof Date) {
    return DateTime.fromJSDate(value, { zone: 'America/Los_Angeles' }).toUTC().toISO();
  }

  if (typeof value === 'string' && value.trim()) {
    const trimmed = value.trim();

    const explicit = DateTime.fromISO(trimmed, { setZone: true });
    if (explicit.isValid) return explicit.toUTC().toISO();

    const pacific = DateTime.fromISO(trimmed, { zone: 'America/Los_Angeles' });
    if (pacific.isValid) return pacific.toUTC().toISO();
  }

  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return DateTime.fromJSDate(parsed, { zone: 'America/Los_Angeles' }).toUTC().toISO();
  }

  return null;
}

function clampSegmentHours(startDt, endDt, rangeStart, rangeEnd) {
  if (!startDt || !endDt) return 0;

  const clampedStart = startDt < rangeStart ? rangeStart : startDt;
  const clampedEnd = endDt > rangeEnd ? rangeEnd : endDt;

  if (clampedEnd <= clampedStart) return 0;

  return clampedEnd.diff(clampedStart, 'hours').hours;
}

function computeWorkedHoursForRange(events, rangeStart, rangeEnd) {
  let state = 'OFF'; // OFF | WORKING | ON_BREAK
  let currentWorkStart = null;
  let workedHours = 0;

  for (const event of events) {
    const eventType = normalizePayrollEventType(event.event_type);
    const eventTime = DateTime.fromJSDate(new Date(event.event_time)).toUTC();

    if (!eventTime.isValid) continue;

    switch (eventType) {
      case 'IN': {
        if (state === 'OFF') {
          state = 'WORKING';
          currentWorkStart = eventTime;
        } else if (state === 'WORKING') {
          // latest IN wins
          currentWorkStart = eventTime;
        } else if (state === 'ON_BREAK') {
          // still on break; do not reopen work from IN
        }
        break;
      }

      case 'OUT': {
        if (state === 'WORKING' && currentWorkStart) {
          workedHours += clampSegmentHours(
            currentWorkStart,
            eventTime,
            rangeStart,
            rangeEnd
          );
        }

        state = 'OFF';
        currentWorkStart = null;
        break;
      }

      case 'BREAK': {
        if (state === 'WORKING' && currentWorkStart) {
          workedHours += clampSegmentHours(
            currentWorkStart,
            eventTime,
            rangeStart,
            rangeEnd
          );
          state = 'ON_BREAK';
          currentWorkStart = null;
        }
        // BREAK while OFF or already ON_BREAK does nothing
        break;
      }

      case 'BACK': {
        if (state === 'ON_BREAK') {
          state = 'WORKING';
          currentWorkStart = eventTime;
        } else if (state === 'WORKING') {
          // latest BACK wins while already working
          currentWorkStart = eventTime;
        }
        // BACK while OFF does nothing
        break;
      }

      default:
        break;
    }
  }

  return Number(workedHours.toFixed(2));
}

async function validateRealtimeSequence(client, employeeId, proposedEventType) {
  const latest = await getLatestAttendanceEvent(client, employeeId);
  const nextEventType = normalizeEventType(proposedEventType);

  if (!latest) {
    if (nextEventType !== 'IN') {
      return {
        valid: false,
        reason: `Invalid event sequence: first event must be IN, not ${nextEventType}`
      };
    }

    return { valid: true, latest: null };
  }

  const lastEventType = normalizeEventType(latest.event_type);
  const allowedNext = getAllowedNextEvents(lastEventType);

  if (!allowedNext.includes(nextEventType)) {
    return {
      valid: false,
      reason: `Invalid event sequence: last event was ${lastEventType}, so next allowed event(s): ${allowedNext.join(' or ')}, not ${nextEventType}`
    };
  }

  return { valid: true, latest };
}

function extractJsonObject(text) {
  if (!text) return null;
  const trimmed = String(text).trim();

  try {
    return JSON.parse(trimmed);
  } catch (_) {}

  const match = trimmed.match(/\{[\s\S]*\}/);
  if (!match) return null;

  try {
    return JSON.parse(match[0]);
  } catch (_) {
    return null;
  }
}

function normalizeSuggestedEvent(value) {
  const type = String(value || '').trim().toUpperCase();
  if (['IN', 'OUT', 'BREAK_START', 'BREAK_END'].includes(type)) return type;
  return null;
}

function normalizeConfidenceLabel(value) {
  const v = String(value || '').trim().toLowerCase();
  if (['low', 'medium', 'high'].includes(v)) return v;
  return 'low';
}

async function callClaudeAttendanceHelper({ rawMessage, messageTimestamp, parseReason }) {
  const timestampPt = DateTime.fromISO(String(messageTimestamp), { setZone: true })
    .setZone('America/Los_Angeles');

  const referencePt = timestampPt.isValid
    ? timestampPt.toFormat("yyyy-LL-dd hh:mm a 'PT'")
    : DateTime.now().setZone('America/Los_Angeles').toFormat("yyyy-LL-dd hh:mm a 'PT'");

  const prompt = [
    'You are an attendance review assistant for a Teams attendance channel.',
    'The strict parser already rejected this message, so you are only suggesting fields for a human reviewer.',
    'Never auto-approve. shouldLog must always be false.',
    '',
    `Original message timestamp in PT: ${referencePt}`,
    `Original message text: ${rawMessage}`,
    `Reason strict parser rejected it: ${parseReason}`,
    '',
    'Allowed suggestedEvent values:',
    '- IN',
    '- OUT',
    '- BREAK_START',
    '- BREAK_END',
    '- null',
    '',
    'Rules:',
    '- Use the message timestamp date as the anchor date for implied times like "until 2pm", "back at 1", "out at 4:30".',
    '- If no specific time is stated, suggestedTime should be null.',
    '- If the message seems to mean lunch/break/stepping away, suggest BREAK_START.',
    '- If the message seems to mean returned/back, suggest BREAK_END.',
    '- If the message seems to mean starting work, suggest IN.',
    '- If the message seems to mean ending work/logging off/done, suggest OUT.',
    '- If uncertain, suggestedEvent should be null.',
    '',
    'Return JSON only with this exact shape:',
    '{',
    '  "probableIntent": "short phrase",',
    '  "suggestedEvent": "IN | OUT | BREAK_START | BREAK_END | null",',
    '  "suggestedTime": "ISO-8601 timestamp with timezone offset or null",',
    '  "reasonInvalid": "short explanation for reviewer",',
    '  "confidenceLabel": "low | medium | high",',
    '  "shouldLog": false',
    '}'
  ].join('\n');

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': anthropicApiKey,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      model: claudeModel,
      max_tokens: 180,
      temperature: 0,
      messages: [
        {
          role: 'user',
          content: prompt
        }
      ]
    })
  });

  const bodyText = await response.text();

  if (!response.ok) {
    throw new Error(`Claude HTTP ${response.status}: ${bodyText}`);
  }

  const parsed = JSON.parse(bodyText);
  const textBlocks = Array.isArray(parsed.content)
    ? parsed.content.filter(block => block.type === 'text').map(block => block.text)
    : [];

  const text = textBlocks.join('\n').trim();
  const json = extractJsonObject(text);

  if (!json) {
    throw new Error(`Claude returned non-JSON content: ${text || '[empty]'}`);
  }

  return {
    probableIntent: String(json.probableIntent || 'unknown').slice(0, 200),
    suggestedEvent: normalizeSuggestedEvent(json.suggestedEvent),
    suggestedTime: toPacificUtcIso(json.suggestedTime),
    reasonInvalid: String(json.reasonInvalid || parseReason).slice(0, 500),
    confidenceLabel: normalizeConfidenceLabel(json.confidenceLabel),
    shouldLog: false,
    explanationSource: 'claude',
    rawModelOutput: json
  };
}

async function explainForReview(rawMessage, parseReason, messageTimestamp) {
  if (!anthropicApiKey) {
    return {
      probableIntent: 'unknown',
      suggestedEvent: null,
      suggestedTime: null,
      reasonInvalid: parseReason,
      shouldLog: false,
      confidenceLabel: 'low',
      explanationSource: 'local_no_api_key'
    };
  }

  try {
    const result = await Promise.race([
      callClaudeAttendanceHelper({
        rawMessage,
        messageTimestamp,
        parseReason
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('AI timeout')), claudeTimeoutMs)
      )
    ]);

    console.log('Claude suggestion:', result);
    return result;
  } catch (error) {
    console.log('AI failed or timed out:', error.message);

    return {
      probableIntent: 'unknown',
      suggestedEvent: null,
      suggestedTime: null,
      reasonInvalid: `${parseReason}. AI suggestion unavailable: ${error.message}`,
      shouldLog: false,
      confidenceLabel: 'low',
      explanationSource: 'claude_fallback_error'
    };
  }
}

app.get('/health', async (req, res) => {
  try {
    await query('SELECT 1');
    return res.json({
      ok: true,
      timezone,
      uptimeSeconds: Math.round(process.uptime())
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/webhook/teams', requireWebhookSecret, async (req, res) => {
  try {
    const employeeName = String(req.body.employeeName || req.body.employee || '').trim();
    const employeeEmail = String(req.body.employeeEmail || req.body.email || '')
      .trim()
      .toLowerCase();
    const rawMessage = sanitizeMessageText(req.body.messageText || req.body.rawMessage || '');
    const channelId = String(req.body.channelId || req.body.channel || 'teams').trim();

    const timestamp =
      req.body.messageTimestamp ||
      req.body.timestamp ||
      new Date().toISOString();

    const messageId = String(req.body.messageId || '').trim();

    // Support both old and new payload styles
    const replyToId = String(req.body.replyToId || '').trim();
    const isReply = Boolean(req.body.isReply || replyToId);

    const wasEdited = Boolean(req.body.wasEdited || req.body.isEdited || false);

    // Ignore replies immediately
    if (isReply) {
      return res.json({ ok: true, status: 'ignored_reply' });
    }

    // Ignore edits immediately
    if (wasEdited) {
      return res.json({ ok: true, status: 'ignored_edit' });
    }

    const employee = await getEmployeeByEmailOrName({ employeeEmail, employeeName });

    const messageHash = buildMessageHash({
      employeeEmail,
      channelId,
      messageId,
      messageText: rawMessage,
      timestamp
    });

    const duplicate = await query(
      'SELECT id FROM raw_messages WHERE message_hash = $1 LIMIT 1',
      [messageHash]
    );

    if (duplicate.rowCount) {
      return res.json({ ok: true, status: 'duplicate_skipped' });
    }
	const manualTimeEntry = isManualTimeEntry(rawMessage);

	const parserBaseTimestamp = new Date().toISOString();
	const parseResult = parseAttendanceMessage(rawMessage, parserBaseTimestamp);

    const responsePayload = await withTransaction(async (client) => {
      const rawInsert = await client.query(
        `INSERT INTO raw_messages (
           employee_id,
           employee_name,
           employee_email,
           channel_id,
           message_id,
           reply_to_id,
           message_text,
           message_timestamp,
           normalized_text,
           parse_status,
           parse_reason,
           message_hash,
           was_edited
         ) VALUES (
           $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
         )
         RETURNING id`,
        [
          employee?.id || null,
          employee?.employee_name || employeeName || 'Unknown',
          employee?.employee_email || employeeEmail || '',
          channelId,
          messageId || null,
          replyToId || null,
          rawMessage,
          timestamp,
          parseResult.normalized,
          parseResult.accepted ? 'accepted' : 'needs_review',
          parseResult.reason,
          messageHash,
          wasEdited
        ]
      );

      const rawMessageId = rawInsert.rows[0].id;

      if (!employee) {
        const employeeNotFoundExplanation = {
          probableIntent: 'unknown',
          reasonInvalid: 'Employee not found',
          shouldLog: false,
          suggestedFormat: '',
          confidenceLabel: 'n/a',
          explanationSource: 'system',
          message: 'Employee was not found in the employees table. Manual review required.'
        };

        const review = await client.query(
          `INSERT INTO review_queue (
             raw_message_id, employee_name, employee_email,
             probable_intent, suggested_event, suggested_time,
             reason_flagged, openclaw_explanation, status
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending')
           RETURNING id`,
          [
            rawMessageId,
            employeeName || 'Unknown',
            employeeEmail || '',
            'unknown',
            null,
            null,
            'Employee not found',
            JSON.stringify(employeeNotFoundExplanation)
          ]
        );

        await client.query(
          `INSERT INTO audit_log (
             entity_type,
             entity_id,
             action,
             new_value,
             performed_by,
             notes
           )
           VALUES ('review_queue', $1, 'created', $2, 'system', 'Employee not found')`,
          [review.rows[0].id, JSON.stringify({ rawMessageId })]
        );

        return { ok: true, status: 'needs_review', reviewId: review.rows[0].id };
      }

      if (parseResult.accepted) {
        

        // Sequence protection only for real-time punches.
        // Backfilled/manual-time punches are allowed through for now so missed-punch
        // workflows stay smooth. Historical validation can be added later.
        if (!manualTimeEntry) {
          const sequenceCheck = await validateRealtimeSequence(
            client,
            employee.id,
            parseResult.eventType
          );

          if (!sequenceCheck.valid) {
            const sequenceExplanation = {
              probableIntent: String(parseResult.eventType || '').toLowerCase(),
              reasonInvalid: sequenceCheck.reason,
              shouldLog: false,
              suggestedFormat: '',
              confidenceLabel: 'high',
              explanationSource: 'sequence_guard'
            };

            const review = await client.query(
              `INSERT INTO review_queue (
                 raw_message_id,
                 employee_id,
                 employee_name,
                 employee_email,
                 probable_intent,
                 suggested_event,
                 suggested_time,
                 reason_flagged,
                 openclaw_explanation,
                 status
               ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')
               RETURNING id`,
              [
                rawMessageId,
                employee.id,
                employee.employee_name,
                employee.employee_email,
                sequenceExplanation.probableIntent,
                parseResult.eventType,
                parseResult.loggedEventTime,
                sequenceExplanation.reasonInvalid,
                JSON.stringify(sequenceExplanation)
              ]
            );

            await client.query(
              `INSERT INTO audit_log (
                 entity_type,
                 entity_id,
                 action,
                 new_value,
                 performed_by,
                 notes
               )
               VALUES ('review_queue', $1, 'created', $2, 'system', 'Sequence guard rejected message')`,
              [review.rows[0].id, JSON.stringify(sequenceExplanation)]
            );

            return {
              ok: true,
              status: 'needs_review',
              reviewId: review.rows[0].id
            };
          }
        }

        const attendance = await client.query(
          `INSERT INTO attendance_events (
             raw_message_id,
             employee_id,
             employee_email,
             event_type,
             event_time,
             timezone,
             source,
             review_id
           ) VALUES ($1,$2,$3,$4,$5,'PT',$6,NULL)
           RETURNING id`,
          [
            rawMessageId,
            employee.id,
            employee.employee_email,
            parseResult.eventType,
            parseResult.loggedEventTime,
            parseResult.source
          ]
        );

        await client.query(
          `INSERT INTO audit_log (
             entity_type,
             entity_id,
             action,
             new_value,
             performed_by,
             notes
           )
           VALUES ('attendance_events', $1, 'created', $2, 'system', 'Auto-logged strict policy match')`,
          [attendance.rows[0].id, JSON.stringify(parseResult)]
        );

        return {
          ok: true,
          status: 'auto_logged',
          attendanceEventId: attendance.rows[0].id
        };
      }

      const explanation = await explainForReview(rawMessage, parseResult.reason);

	const review = await client.query(
	  `INSERT INTO review_queue (
	     raw_message_id,
	     employee_id,
	     employee_name,
	     employee_email,
	     probable_intent,
	     suggested_event,
	     suggested_time,
	     reason_flagged,
	     openclaw_explanation,
	     status
	   ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')
	   RETURNING id`,
	  [
	    rawMessageId,
	    employee.id,
	    employee.employee_name,
	    employee.employee_email,
	    explanation.probableIntent,
	    explanation.suggestedEvent,
	    explanation.suggestedTime,
	    explanation.reasonInvalid,
	    JSON.stringify(explanation)
	  ]
	);

      await client.query(
        `INSERT INTO audit_log (
           entity_type,
           entity_id,
           action,
           new_value,
           performed_by,
           notes
         )
         VALUES ('review_queue', $1, 'created', $2, 'system', 'Strict parser rejected message')`,
        [review.rows[0].id, JSON.stringify(explanation)]
      );

      return { ok: true, status: 'needs_review', reviewId: review.rows[0].id };
    });

    return res.json(responsePayload);
  } catch (error) {
    console.error('Webhook processing error:', error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/api/employees', requireAdminApiKey, async (req, res) => {
  try {
    const includeInactive =
      String(req.query.includeInactive || 'false').toLowerCase() === 'true';

    const result = await query(
      `
      SELECT
        e.id,
        e.employee_name,
        e.employee_email,
        e.active,
        e.created_at,
        e.updated_at,
		e.position,
		e.hourly_pay,
		e.bonus_current,
		e.bonus_previous,
        ae_latest.event_type AS latest_event_type,
        ae_latest.event_time AS latest_event_time,
        CASE
          WHEN ae_latest.event_type IN ('IN', 'BREAK_END') THEN 'active'
          WHEN ae_latest.event_type IN ('OUT', 'BREAK_START') THEN 'inactive'
          ELSE 'unknown'
        END AS status
      FROM employees e
      LEFT JOIN LATERAL (
        SELECT ae.event_type, ae.event_time
        FROM attendance_events ae
        WHERE ae.employee_id = e.id
        ORDER BY ae.event_time DESC, ae.id DESC
        LIMIT 1
      ) ae_latest ON true
      WHERE ($1::boolean = true OR e.active = true)
      ORDER BY e.employee_name ASC
      `,
      [includeInactive]
    );

    return res.json({
      ok: true,
      rows: result.rows
    });
  } catch (err) {
    console.error('get employees failed:', err.message);
    return res.status(500).json({
      ok: false,
      error: 'get employees failed'
    });
  }
});

app.post('/api/employees', requireAdminApiKey, async (req, res) => {
  const performedBy = req.header('x-admin-user') || 'retool_admin';

  try {
    const employeeName = String(req.body.employeeName || '').trim();
    const employeeEmail = String(req.body.employeeEmail || '').trim().toLowerCase();
	const position = String(req.body.position || '').trim() || null;
	const hourlyPay =
  		req.body.hourlyPay !== undefined && req.body.hourlyPay !== ''
    		? Number(req.body.hourlyPay)
    		: null;
	const bonusCurrent =
	  req.body.bonusCurrent !== undefined && req.body.bonusCurrent !== ''
	    ? Number(req.body.bonusCurrent)
	    : null;
	const bonusPrevious =
	  req.body.bonusPrevious !== undefined && req.body.bonusPrevious !== ''
	    ? Number(req.body.bonusPrevious)
	    : null;
    const active =
      typeof req.body.active === 'boolean' ? req.body.active : true;

    if (!employeeName || !employeeEmail) {
      return res.status(400).json({
        ok: false,
        error: 'employeeName and employeeEmail are required'
      });
    }

    const created = await withTransaction(async (client) => {
      const result = await client.query(
        `
		INSERT INTO employees (
		  employee_name,
		  employee_email,
		  position,
		  hourly_pay,
		  bonus_current,
		  bonus_previous,
		  active
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        `,
        [employeeName, employeeEmail, position, hourlyPay, bonusCurrent, bonusPrevious, active]
      );

      const row = result.rows[0];

      await client.query(
        `
        INSERT INTO audit_log (
          entity_type,
          entity_id,
          action,
          old_value,
          new_value,
          performed_by,
          notes
        )
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)
        `,
        [
          'employee',
          String(row.id),
          'create',
          JSON.stringify({}),
          JSON.stringify(row),
          performedBy,
          'Employee created from Retool'
        ]
      );

      return row;
    });

    return res.json({ ok: true, row: created });
  } catch (err) {
    console.error('create employee failed:', err.message);

    if (String(err.message || '').includes('employees_employee_email_key')) {
      return res.status(400).json({
        ok: false,
        error: 'employee email already exists'
      });
    }

    return res.status(500).json({
      ok: false,
      error: 'create employee failed'
    });
  }
});

app.put('/api/employees/:id', requireAdminApiKey, async (req, res) => {
  const employeeId = String(req.params.id || '').trim();
  const performedBy = req.header('x-admin-user') || 'retool_admin';

  try {
    const employeeName = String(req.body.employeeName || '').trim();
    const employeeEmail = String(req.body.employeeEmail || '').trim().toLowerCase();
    const active =
      typeof req.body.active === 'boolean' ? req.body.active : true;
	const position = String(req.body.position || '').trim() || null;
	const hourlyPay =
  		req.body.hourlyPay !== undefined && req.body.hourlyPay !== ''
    	? Number(req.body.hourlyPay)
    	: null;
	const bonusCurrent =
	  req.body.bonusCurrent !== undefined && req.body.bonusCurrent !== ''
	    ? Number(req.body.bonusCurrent)
	    : null;
	const bonusPrevious =
	  req.body.bonusPrevious !== undefined && req.body.bonusPrevious !== ''
	    ? Number(req.body.bonusPrevious)
	    : null;

    if (!employeeId) {
      return res.status(400).json({
        ok: false,
        error: 'employee id is required'
      });
    }

    if (!employeeName || !employeeEmail) {
      return res.status(400).json({
        ok: false,
        error: 'employeeName and employeeEmail are required'
      });
    }

    const updated = await withTransaction(async (client) => {
      const existing = await client.query(
        `
        SELECT *
        FROM employees
        WHERE id = $1
        LIMIT 1
        `,
        [employeeId]
      );

      if (!existing.rowCount) {
        throw new Error('employee not found');
      }

      const oldRow = existing.rows[0];

      const result = await client.query(
        `
		UPDATE employees
		SET
		  employee_name = $1,
		  employee_email = $2,
		  position = $3,
		  hourly_pay = $4,
		  bonus_current = $5,
		  bonus_previous = $6,
		  active = $7
		WHERE id = $8
        RETURNING *
        `,
        [employeeName, employeeEmail, position, hourlyPay, bonusCurrent, bonusPrevious, active, employeeId]
      );

      const newRow = result.rows[0];

      await client.query(
        `
        INSERT INTO audit_log (
          entity_type,
          entity_id,
          action,
          old_value,
          new_value,
          performed_by,
          notes
        )
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)
        `,
        [
          'employee',
          String(employeeId),
          'update',
          JSON.stringify(oldRow),
          JSON.stringify(newRow),
          performedBy,
          'Employee updated from Retool'
        ]
      );

      return newRow;
    });

    return res.json({ ok: true, row: updated });
  } catch (err) {
    console.error('update employee failed:', err.message);

    if (String(err.message || '').includes('employees_employee_email_key')) {
      return res.status(400).json({
        ok: false,
        error: 'employee email already exists'
      });
    }

    if (err.message === 'employee not found') {
      return res.status(404).json({
        ok: false,
        error: 'employee not found'
      });
    }

    return res.status(500).json({
      ok: false,
      error: 'update employee failed'
    });
  }
});

app.get('/api/reviews', requireAdminApiKey, async (req, res) => {
  try {
    const status = String(req.query.status || 'pending');

    const result = await query(
      `SELECT
        rq.id,
        rq.employee_name,
        rq.employee_email,
        rq.probable_intent,
        rq.suggested_event,
        rq.suggested_time,
        rq.reason_flagged,
        rq.status,
        rq.reviewed_by,
        rq.reviewed_at,
        rq.raw_message_id,
        rq.openclaw_explanation,
        rq.created_at,
        rm.message_text
      FROM review_queue rq
      LEFT JOIN raw_messages rm
        ON rm.id = rq.raw_message_id
      WHERE ($1 = 'all' OR rq.status = $1)
      ORDER BY rq.created_at DESC
      LIMIT 500`,
      [status]
    );

    return res.json({ ok: true, rows: result.rows });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/api/attendance', requireAdminApiKey, async (req, res) => {
  try {
    const result = await query(
      `
      SELECT
        ae.id,
        e.employee_name,
        ae.employee_email,
        ae.event_type,
        ae.event_time,
        ae.timezone,
        ae.source,
        ae.review_id,
        ae.created_at,
        rm.message_text
      FROM attendance_events ae
      JOIN employees e
        ON e.id = ae.employee_id
      LEFT JOIN raw_messages rm
        ON rm.id = ae.raw_message_id
      WHERE e.active = true
      ORDER BY ae.event_time DESC
      `
    );

    return res.json({ ok: true, rows: result.rows });
  } catch (error) {
    console.error('get attendance failed:', error.message);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/debug/db-info', requireAdminApiKey, async (req, res) => {
  try {
    const result = await query(`
      SELECT
        current_database() AS database_name,
        current_user AS db_user,
        inet_server_addr()::text AS server_addr,
        inet_server_port() AS server_port
    `);

    return res.json({
      ok: true,
      envDatabaseUrl: process.env.DATABASE_URL,
      dbInfo: result.rows[0]
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/attendance/update', requireAdminApiKey, async (req, res) => {
  try {
    const rows = Array.isArray(req.body.rows) ? req.body.rows : [];

    console.log('incoming rows:', JSON.stringify(rows, null, 2));

    for (const row of rows) {
      const existing = await query(
        `SELECT event_type, event_time
         FROM attendance_events
         WHERE id = $1`,
        [row.id]
      );

      if (!existing.rowCount) continue;

      const current = existing.rows[0];

      const updated = await query(
        `UPDATE attendance_events
         SET event_type = $1,
             event_time = $2
         WHERE id = $3
         RETURNING *`,
        [
	  row.event_type ?? current.event_type,
	  row.event_time ? toPacificUtcIso(row.event_time) : current.event_time,
	  row.id
        ]
      );

      console.log('updated row:', updated.rows[0]);
    }

    return res.json({ ok: true, updated: rows.length });
  } catch (err) {
    console.error('attendance update error:', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/biweekly-summary', requireAdminApiKey, async (req, res) => {
  try {
    const bounds = getBiweeklyBounds();

    const employeesResult = await query(
      `
      SELECT id, employee_name, employee_email
      FROM employees
      WHERE active = true
      ORDER BY employee_name
      `
    );

    const eventsResult = await query(
      `
      SELECT
        id,
        employee_id,
        employee_email,
        event_type,
        event_time
      FROM attendance_events
      ORDER BY employee_id, event_time ASC, id ASC
      `
    );

    const eventsByEmployee = new Map();

    for (const event of eventsResult.rows) {
      const key = String(event.employee_id);
      if (!eventsByEmployee.has(key)) {
        eventsByEmployee.set(key, []);
      }
      eventsByEmployee.get(key).push(event);
    }

    const currentStart = bounds.currentStart.toUTC();
    const currentEnd = bounds.currentEnd.toUTC();
    const previousStart = bounds.previousStart.toUTC();
    const previousEnd = bounds.previousEnd.toUTC();

    const rows = employeesResult.rows.map((employee) => {
      const employeeEvents = eventsByEmployee.get(String(employee.id)) || [];

      const biweeklyCurrent = computeWorkedHoursForRange(
        employeeEvents,
        currentStart,
        currentEnd
      );

      const biweeklyPrevious = computeWorkedHoursForRange(
        employeeEvents,
        previousStart,
        previousEnd
      );

      return {
        name: employee.employee_name,
        email: employee.employee_email,
        biweekly_current: biweeklyCurrent,
        biweekly_previous: biweeklyPrevious
      };
    });

    return res.json({
      ok: true,
      pay_periods: {
        current_start: bounds.currentStart.toISO(),
        current_end: bounds.currentEnd.toISO(),
        previous_start: bounds.previousStart.toISO(),
        previous_end: bounds.previousEnd.toISO()
      },
      rows
    });
  } catch (err) {
    console.error('biweekly summary failed:', err);
    return res.status(500).json({ ok: false, error: 'biweekly summary failed' });
  }
});

app.post('/api/attendance/manual', requireAdminApiKey, async (req, res) => {
  const { employeeId, eventType, eventTime } = req.body;
  const performedBy = req.header('x-admin-user') || 'retool_admin';
  const normalizedEventTime = toPacificUtcIso(eventTime);

  try {
    if (!employeeId || !eventType || !normalizedEventTime) {
      return res.status(400).json({
        ok: false,
        error: 'employeeId, eventType, and eventTime are required'
      });
    }

    const validEventTypes = ['IN', 'OUT', 'BREAK_START', 'BREAK_END'];
    if (!validEventTypes.includes(eventType)) {
      return res.status(400).json({
        ok: false,
        error: 'invalid event type'
      });
    }

    const createdRow = await withTransaction(async (client) => {
      const employeeResult = await client.query(
        `
        SELECT id, employee_name, employee_email
        FROM employees
        WHERE id = $1
        `,
        [employeeId]
      );

      if (employeeResult.rows.length === 0) {
        throw new Error('employee not found');
      }

      const employee = employeeResult.rows[0];

      const rawMessageResult = await client.query(
        `
        INSERT INTO raw_messages (
          employee_id,
          employee_name,
          employee_email,
          channel_id,
          message_id,
          reply_to_id,
          message_text,
          message_timestamp,
          normalized_text,
          parse_status,
          parse_reason,
          message_hash,
          was_edited
        )
        VALUES (
          $1,
          $2,
          $3,
          'manual-entry',
          NULL,
          NULL,
          'no msg this is manually added',
          $4::timestamptz,
          'no msg this is manually added',
          'accepted',
          'manual admin entry',
          encode(gen_random_bytes(32), 'hex'),
          false
        )
        RETURNING id
        `,
        [
          employee.id,
          employee.employee_name,
          employee.employee_email,
          normalizedEventTime
        ]
      );

      const rawMessageId = rawMessageResult.rows[0].id;

      const attendanceResult = await client.query(
        `
        INSERT INTO attendance_events (
          raw_message_id,
          employee_id,
          employee_email,
          event_type,
          event_time,
          timezone,
          source,
          review_id
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5::timestamptz,
          'PT',
          'admin_edit',
          NULL
        )
        RETURNING *
        `,
        [
          rawMessageId,
          employee.id,
          employee.employee_email,
          eventType,
          normalizedEventTime
        ]
      );

      const newRow = attendanceResult.rows[0];

      await client.query(
        `
        INSERT INTO audit_log (
          entity_type,
          entity_id,
          action,
          old_value,
          new_value,
          performed_by,
          notes
        )
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)
        `,
        [
          'attendance_event',
          String(newRow.id),
          'create_manual',
          JSON.stringify({}),
          JSON.stringify(newRow),
          performedBy,
          'Manual attendance event added from Attendance Master'
        ]
      );

      return newRow;
    });

    return res.json({ ok: true, row: createdRow });

  } catch (err) {
    console.error('manual attendance create failed:', err.message);
    return res.status(400).json({ ok: false, error: err.message });
  }
});

app.get('/api/payroll-summary', requireAdminApiKey, async (req, res) => {
  try {
    const bounds = getBiweeklyBounds();

    const employeesResult = await query(
      `
      SELECT
        id,
        employee_name,
        employee_email,
        hourly_pay,
        bonus_current,
        bonus_previous
      FROM employees
      WHERE active = true
      ORDER BY employee_name
      `
    );

    const eventsResult = await query(
      `
      SELECT
        id,
        employee_id,
        employee_email,
        event_type,
        event_time
      FROM attendance_events
      ORDER BY employee_id, event_time ASC, id ASC
      `
    );

    const eventsByEmployee = new Map();

    for (const event of eventsResult.rows) {
      const key = String(event.employee_id);
      if (!eventsByEmployee.has(key)) {
        eventsByEmployee.set(key, []);
      }
      eventsByEmployee.get(key).push(event);
    }

    const currentStart = bounds.currentStart.toUTC();
    const currentEnd = bounds.currentEnd.toUTC();
    const previousStart = bounds.previousStart.toUTC();
    const previousEnd = bounds.previousEnd.toUTC();

    const current_rows = [];
    const previous_rows = [];

    for (const employee of employeesResult.rows) {
      const employeeEvents = eventsByEmployee.get(String(employee.id)) || [];

      const hourlyPay =
        employee.hourly_pay !== null ? Number(employee.hourly_pay) : 0;

      const bonusCurrent =
        employee.bonus_current !== null ? Number(employee.bonus_current) : 0;

      const bonusPrevious =
        employee.bonus_previous !== null ? Number(employee.bonus_previous) : 0;

      const currentHours = computeWorkedHoursForRange(
        employeeEvents,
        currentStart,
        currentEnd
      );

      const previousHours = computeWorkedHoursForRange(
        employeeEvents,
        previousStart,
        previousEnd
      );

      current_rows.push({
        id: employee.id,
        employee_name: employee.employee_name,
        employee_email: employee.employee_email,
        hourly_pay: hourlyPay,
        biweekly_current: currentHours,
        bonus_current: bonusCurrent,
        total: Number(((currentHours * hourlyPay) + bonusCurrent).toFixed(2))
      });

      previous_rows.push({
        id: employee.id,
        employee_name: employee.employee_name,
        employee_email: employee.employee_email,
        hourly_pay: hourlyPay,
        biweekly_previous: previousHours,
        bonus_previous: bonusPrevious,
        total: Number(((previousHours * hourlyPay) + bonusPrevious).toFixed(2))
      });
    }

    return res.json({
      ok: true,
      pay_periods: {
        current_start: bounds.currentStart.toISO(),
        current_end: bounds.currentEnd.toISO(),
        previous_start: bounds.previousStart.toISO(),
        previous_end: bounds.previousEnd.toISO()
      },
      current_rows,
      previous_rows
    });
  } catch (err) {
    console.error('payroll summary failed:', err);
    return res.status(500).json({
      ok: false,
      error: 'payroll summary failed'
    });
  }
});

app.delete('/api/employees/:id', requireAdminApiKey, async (req, res) => {
  const employeeId = String(req.params.id || '').trim();
  const performedBy = req.header('x-admin-user') || 'retool_admin';

  try {
    if (!employeeId) {
      return res.status(400).json({
        ok: false,
        error: 'employee id is required'
      });
    }

    const payload = await withTransaction(async (client) => {
      const employeeResult = await client.query(
        `
        SELECT *
        FROM employees
        WHERE id = $1
        LIMIT 1
        `,
        [employeeId]
      );

      if (!employeeResult.rowCount) {
        throw new Error('employee not found');
      }

      const employee = employeeResult.rows[0];

      const attendanceResult = await client.query(
        `
        SELECT id
        FROM attendance_events
        WHERE employee_id = $1
        `,
        [employeeId]
      );

      const reviewResult = await client.query(
        `
        SELECT id
        FROM review_queue
        WHERE employee_id = $1
           OR lower(employee_email) = lower($2)
        `,
        [employeeId, employee.employee_email]
      );

      const rawResult = await client.query(
        `
        SELECT id
        FROM raw_messages
        WHERE employee_id = $1
           OR lower(employee_email) = lower($2)
        `,
        [employeeId, employee.employee_email]
      );

      const attendanceIds = attendanceResult.rows.map(r => String(r.id));
      const reviewIds = reviewResult.rows.map(r => String(r.id));
      const rawIds = rawResult.rows.map(r => String(r.id));

      // remove audit rows tied to attendance events
      if (attendanceIds.length) {
        await client.query(
          `
          DELETE FROM audit_log
          WHERE entity_type IN ('attendance_event', 'attendance_events')
            AND entity_id = ANY($1::text[])
          `,
          [attendanceIds]
        );
      }

      // remove audit rows tied to reviews
      if (reviewIds.length) {
        await client.query(
          `
          DELETE FROM audit_log
          WHERE entity_type = 'review_queue'
            AND entity_id = ANY($1::text[])
          `,
          [reviewIds]
        );
      }

      // remove audit rows tied to the employee record
      await client.query(
        `
        DELETE FROM audit_log
        WHERE entity_type = 'employee'
          AND entity_id = $1
        `,
        [employeeId]
      );

      // delete attendance first because attendance_events has RESTRICT FKs
      await client.query(
        `
        DELETE FROM attendance_events
        WHERE employee_id = $1
        `,
        [employeeId]
      );

      // then delete reviews
      await client.query(
        `
        DELETE FROM review_queue
        WHERE employee_id = $1
           OR lower(employee_email) = lower($2)
        `,
        [employeeId, employee.employee_email]
      );

      // then delete raw messages
      await client.query(
        `
        DELETE FROM raw_messages
        WHERE employee_id = $1
           OR lower(employee_email) = lower($2)
        `,
        [employeeId, employee.employee_email]
      );

      // finally delete employee
      await client.query(
        `
        DELETE FROM employees
        WHERE id = $1
        `,
        [employeeId]
      );

      return {
        deletedEmployee: {
          id: employee.id,
          employee_name: employee.employee_name,
          employee_email: employee.employee_email
        },
        deletedCounts: {
          attendance_events: attendanceIds.length,
          review_queue: reviewIds.length,
          raw_messages: rawIds.length
        }
      };
    });

    return res.json({
      ok: true,
      message: 'employee and related records permanently deleted',
      ...payload
    });
  } catch (err) {
    console.error('delete employee failed:', err.message);
    return res.status(400).json({
      ok: false,
      error: err.message
    });
  }
});

app.delete('/api/attendance/:id', requireAdminApiKey, async (req, res) => {
  const attendanceId = req.params.id;
  const performedBy = req.header('x-admin-user') || 'retool_admin';

  try {
    await withTransaction(async (client) => {
      const existing = await client.query(
        `
        SELECT *
        FROM attendance_events
        WHERE id = $1
        `,
        [attendanceId]
      );

      if (existing.rows.length === 0) {
        throw new Error('attendance event not found');
      }

      const oldRow = existing.rows[0];

      await client.query(
        `
        INSERT INTO audit_log (
          entity_type,
          entity_id,
          action,
          old_value,
          new_value,
          performed_by,
          notes
        )
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)
        `,
        [
          'attendance_event',
          String(attendanceId),
          'delete',
          JSON.stringify(oldRow),
          JSON.stringify({ deleted: true }),
          performedBy,
          'Deleted from Attendance Master page'
        ]
      );

      await client.query(
        `
        DELETE FROM attendance_events
        WHERE id = $1
        `,
        [attendanceId]
      );
    });

    return res.json({ ok: true });
  } catch (err) {
    console.error('delete attendance failed:', err.message);
    return res.status(400).json({ ok: false, error: err.message });
  }
});

app.post('/api/reviews/:id/approve', requireAdminApiKey, async (req, res) => {
  try {
    const reviewId = Number(req.params.id);
    const reviewedBy = String(req.body.reviewedBy || 'retool_admin').trim();

    const payload = await withTransaction(async (client) => {
      const review = await client.query(
        `
        SELECT
          rq.*,
          rm.message_timestamp,
          rm.employee_name AS raw_employee_name,
          rm.employee_email AS raw_employee_email
        FROM review_queue rq
        JOIN raw_messages rm
          ON rm.id = rq.raw_message_id
        WHERE rq.id = $1
        LIMIT 1
        `,
        [reviewId]
      );

      if (!review.rowCount) {
        throw new Error('Review item not found');
      }

      if (review.rows[0].status !== 'pending') {
        throw new Error('Review item is no longer pending');
      }

      const row = review.rows[0];

      const eventType = String(req.body.eventType || '').trim().toUpperCase();
      const eventTimeValue = req.body.eventTime ?? row.suggested_time ?? row.message_timestamp ?? null;
      const eventTime = toPacificUtcIso(eventTimeValue);

      if (!eventType) {
        throw new Error('eventType is required for approval');
      }

      if (!eventTime) {
        throw new Error('eventTime is required for approval');
      }

      const validEventTypes = ['IN', 'OUT', 'BREAK_START', 'BREAK_END'];
      if (!validEventTypes.includes(eventType)) {
        throw new Error('invalid event type');
      }

      let employeeId = row.employee_id;
      let employeeEmail = row.employee_email;
      let employeeName = row.employee_name;

      // Re-resolve employee at approval time in case they were added later.
      if (!employeeId) {
        const lookupName =
          String(row.employee_name || row.raw_employee_name || '').trim();

        const lookupEmail =
          String(row.employee_email || row.raw_employee_email || '')
            .trim()
            .toLowerCase();

        let employeeResult = null;

        if (lookupEmail) {
          employeeResult = await client.query(
            `
            SELECT id, employee_name, employee_email, active
            FROM employees
            WHERE lower(employee_email) = lower($1)
            LIMIT 1
            `,
            [lookupEmail]
          );
        }

        if ((!employeeResult || !employeeResult.rowCount) && lookupName) {
          employeeResult = await client.query(
            `
            SELECT id, employee_name, employee_email, active
            FROM employees
            WHERE lower(employee_name) = lower($1)
            LIMIT 1
            `,
            [lookupName]
          );
        }

        if (!employeeResult || !employeeResult.rowCount) {
          throw new Error(
            'Employee still not found. Add them in Employee Master first, then approve again.'
          );
        }

        const employee = employeeResult.rows[0];

        if (!employee.active) {
          throw new Error('Employee exists but is inactive');
        }

        employeeId = employee.id;
        employeeEmail = employee.employee_email;
        employeeName = employee.employee_name;

        await client.query(
          `
          UPDATE review_queue
          SET
            employee_id = $2,
            employee_name = $3,
            employee_email = $4
          WHERE id = $1
          `,
          [reviewId, employeeId, employeeName, employeeEmail]
        );

        await client.query(
          `
          UPDATE raw_messages
          SET
            employee_id = $2,
            employee_name = $3,
            employee_email = $4
          WHERE id = $1
          `,
          [row.raw_message_id, employeeId, employeeName, employeeEmail]
        );
      }

      const inserted = await client.query(
        `
        INSERT INTO attendance_events (
          raw_message_id,
          employee_id,
          employee_email,
          event_type,
          event_time,
          timezone,
          source,
          review_id
        )
        VALUES ($1,$2,$3,$4,$5,'PT','review',$6)
        RETURNING id
        `,
        [row.raw_message_id, employeeId, employeeEmail, eventType, eventTime, reviewId]
      );

      await client.query(
        `
        UPDATE review_queue
        SET
          status = 'approved',
          reviewed_by = $2,
          reviewed_at = NOW()
        WHERE id = $1
        `,
        [reviewId, reviewedBy]
      );

      await client.query(
        `
        INSERT INTO audit_log (
          entity_type,
          entity_id,
          action,
          old_value,
          new_value,
          performed_by,
          notes
        )
        VALUES ('review_queue', $1, 'approved', $2, $3, $4, 'Approved in admin tool')
        `,
        [
          reviewId,
          JSON.stringify({ status: 'pending' }),
          JSON.stringify({
            status: 'approved',
            attendanceEventId: inserted.rows[0].id,
            employeeId,
            employeeEmail,
            eventType,
            eventTime
          }),
          reviewedBy
        ]
      );

      return {
        ok: true,
        status: 'approved',
        attendanceEventId: inserted.rows[0].id
      };
    });

    return res.json(payload);
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/api/reviews/:id/reject', requireAdminApiKey, async (req, res) => {
  try {
    const reviewId = Number(req.params.id);
    const reviewedBy = String(req.body.reviewedBy || 'retool_admin').trim();
    const reviewNotes = String(req.body.reviewNotes || '').trim();

    const payload = await withTransaction(async (client) => {
      const result = await client.query(
        `UPDATE review_queue
         SET status = 'rejected',
             reviewed_by = $2,
             reviewed_at = NOW(),
             review_notes = $3
         WHERE id = $1
           AND status = 'pending'
         RETURNING id`,
        [reviewId, reviewedBy, reviewNotes]
      );

      if (!result.rowCount) {
        throw new Error('Review item not found or no longer pending');
      }

      await client.query(
        `INSERT INTO audit_log (
           entity_type,
           entity_id,
           action,
           new_value,
           performed_by,
           notes
         )
         VALUES ('review_queue', $1, 'rejected', $2, $3, 'Rejected in admin tool')`,
        [reviewId, JSON.stringify({ reviewNotes }), reviewedBy]
      );

      return { ok: true, status: 'rejected' };
    });

    return res.json(payload);
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});


app.get('/api/archive/preview', requireAdminApiKey, async (req, res) => {
  try {
    const { tables, invalid } = normalizeArchiveTables(req.query.tables);

    if (invalid.length) {
      return res.status(400).json({
        ok: false,
        error: `invalid table(s): ${invalid.join(', ')}`
      });
    }

    const { archiveStartIso, archiveEndIso, fileStartLabel, fileEndLabel } =
      getExactArchiveWindow();

    const payload = await withTransaction(async (client) => {
      const previews = [];

      for (const tableName of tables) {
        const preview = await previewArchiveTable(client, tableName);
        previews.push(preview);
      }

      return previews;
    });

    return res.json({
      ok: true,
      archiveWindow: {
        archiveStartIso,
        archiveEndIso,
        fileStartLabel,
        fileEndLabel
      },
      tables: payload
    });
  } catch (err) {
    console.error('archive preview failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/archive/export', requireAdminApiKey, async (req, res) => {
  const performedBy = req.header('x-admin-user') || 'retool_admin';

  try {
    const { tables, invalid } = normalizeArchiveTables(req.body.tables);
    if (invalid.length) {
      return res.status(400).json({
        ok: false,
        error: `invalid table(s): ${invalid.join(', ')}`
      });
    }

    const manifests = await withTransaction(async (client) => {
      const results = [];

      for (const tableName of tables) {
        const manifest = await exportArchiveTable(client, tableName, performedBy);
        results.push(manifest);
      }

      return results;
    });

    return res.json({
      ok: true,
      archiveWindow: getExactArchiveWindow(),
      manifests
    });
  } catch (err) {
    console.error('archive export failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/archive/run-now', requireAdminApiKey, async (req, res) => {
  const performedBy = req.header('x-admin-user') || 'retool_admin';

  try {
    const result = await runAutomaticArchive(performedBy || 'manual_run_now');
    return res.json({ ok: true, ...result });
  } catch (err) {
    console.error('archive run-now failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/archive/manifests', requireAdminApiKey, async (req, res) => {
  try {
    const result = await query(
      `
      SELECT
        id,
        table_name,
        file_name,
        file_path,
        cutoff_time,
        row_count,
        checksum_sha256,
        status,
        performed_by,
        created_at,
        notes
      FROM archive_manifest
      ORDER BY created_at DESC, id DESC
      LIMIT 200
      `
    );

    return res.json({ ok: true, rows: result.rows });
  } catch (err) {
    console.error('get archive manifests failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/archive/download/:id', requireAdminApiKey, async (req, res) => {
  try {
    const manifestId = Number(req.params.id);

    if (!Number.isInteger(manifestId) || manifestId <= 0) {
      return res.status(400).json({ ok: false, error: 'invalid manifest id' });
    }

    const result = await query(
      `
      SELECT id, file_name, file_path, status
      FROM archive_manifest
      WHERE id = $1
      LIMIT 1
      `,
      [manifestId]
    );

    if (!result.rowCount) {
      return res.status(404).json({ ok: false, error: 'archive file not found' });
    }

    const row = result.rows[0];

    if (!fs.existsSync(row.file_path)) {
      return res.status(404).json({ ok: false, error: 'archive file missing on disk' });
    }

    return res.download(row.file_path, row.file_name);
  } catch (err) {
    console.error('download archive file failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.listen(port, () => {
  console.log(`ASTRO attendance backend listening on port ${port}`);
  startAutoArchiveScheduler();
});
