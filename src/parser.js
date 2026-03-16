const { DateTime } = require('luxon');

const SIMPLE_PATTERN = /^(in|out|break|back)$/i;
const BACKFILL_PATTERN = /^(in|out|break|back)\s+at\s+(\d{1,2}:\d{2})\s*(AM|PM)\s*(PST|PDT)?$/i;

const ATTENDANCE_ZONE = 'America/Los_Angeles';

function sanitizeMessageText(raw) {
  return String(raw || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<li>/gi, '\n- ')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\r/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function eventTypeForCommand(command) {
  switch (String(command || '').toLowerCase()) {
    case 'in':
      return 'IN';
    case 'out':
      return 'OUT';
    case 'break':
      return 'BREAK_START';
    case 'back':
      return 'BREAK_END';
    default:
      return 'UNKNOWN';
  }
}

function getBaseDateTime(messageTimestamp) {
  const raw = messageTimestamp || new Date().toISOString();

  let dt = DateTime.fromISO(String(raw), { zone: 'utc' });
  if (!dt.isValid) {
    dt = DateTime.fromJSDate(new Date(raw), { zone: 'utc' });
  }
  if (!dt.isValid) {
    dt = DateTime.now().toUTC();
  }

  return dt.setZone(ATTENDANCE_ZONE);
}

function parseManualTimestamp(messageTimestamp, hhmm, ampm) {
  const baseLocal = getBaseDateTime(messageTimestamp);

  const parsedTime = DateTime.fromFormat(
    `${hhmm} ${String(ampm || '').toUpperCase()}`,
    'h:mm a',
    { zone: ATTENDANCE_ZONE }
  );

  if (!parsedTime.isValid) {
    throw new Error(`Invalid manual time: ${hhmm} ${ampm}`);
  }

  const localDateTime = baseLocal.set({
    hour: parsedTime.hour,
    minute: parsedTime.minute,
    second: 0,
    millisecond: 0
  });

  return localDateTime.toUTC().toISO();
}

function parseAttendanceMessage(messageText, messageTimestamp) {
  const normalized = sanitizeMessageText(messageText);

  if (!normalized) {
    return {
      accepted: false,
      normalized,
      reason: 'Empty message'
    };
  }

  const simple = normalized.match(SIMPLE_PATTERN);
  if (simple) {
    const command = simple[1].toLowerCase();
    return {
      accepted: true,
      normalized,
      eventType: eventTypeForCommand(command),
      loggedEventTime: DateTime.fromISO(
        String(messageTimestamp || new Date().toISOString()),
        { zone: 'utc' }
      ).isValid
        ? DateTime.fromISO(String(messageTimestamp || new Date().toISOString()), { zone: 'utc' }).toUTC().toISO()
        : new Date(messageTimestamp || Date.now()).toISOString(),
      source: 'auto',
      isBackfill: false,
      reason: 'Exact command match'
    };
  }

  const backfill = normalized.match(BACKFILL_PATTERN);
  if (backfill) {
    const command = backfill[1].toLowerCase();
    const hhmm = backfill[2];
    const ampm = backfill[3].toUpperCase();

    return {
      accepted: true,
      normalized,
      eventType: eventTypeForCommand(command),
      loggedEventTime: parseManualTimestamp(messageTimestamp, hhmm, ampm),
      source: 'auto',
      isBackfill: true,
      reason: 'Approved backfill format'
    };
  }

  return {
    accepted: false,
    normalized,
    reason: 'Message does not match strict attendance policy'
  };
}

module.exports = {
  sanitizeMessageText,
  parseAttendanceMessage
};