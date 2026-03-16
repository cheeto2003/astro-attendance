CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS employees (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  employee_name TEXT NOT NULL,
  employee_email TEXT NOT NULL UNIQUE,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_messages (
  id BIGSERIAL PRIMARY KEY,
  employee_id UUID NULL REFERENCES employees(id) ON DELETE SET NULL,
  employee_name TEXT NOT NULL,
  employee_email TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  message_id TEXT NULL,
  reply_to_id TEXT NULL,
  message_text TEXT NOT NULL,
  message_timestamp TIMESTAMPTZ NOT NULL,
  normalized_text TEXT NOT NULL,
  parse_status TEXT NOT NULL CHECK (parse_status IN ('accepted', 'needs_review', 'ignored')),
  parse_reason TEXT NOT NULL,
  message_hash TEXT NOT NULL UNIQUE,
  was_edited BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_messages_employee_email ON raw_messages(employee_email);
CREATE INDEX IF NOT EXISTS idx_raw_messages_timestamp ON raw_messages(message_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_messages_parse_status ON raw_messages(parse_status);

CREATE TABLE IF NOT EXISTS review_queue (
  id BIGSERIAL PRIMARY KEY,
  raw_message_id BIGINT NOT NULL UNIQUE REFERENCES raw_messages(id) ON DELETE CASCADE,
  employee_id UUID NULL REFERENCES employees(id) ON DELETE SET NULL,
  employee_name TEXT NOT NULL,
  employee_email TEXT NOT NULL,
  probable_intent TEXT NULL,
  suggested_event TEXT NULL,
  suggested_time TIMESTAMPTZ NULL,
  reason_flagged TEXT NOT NULL,
  openclaw_explanation JSONB NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  reviewed_by TEXT NULL,
  reviewed_at TIMESTAMPTZ NULL,
  review_notes TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_review_queue_status ON review_queue(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_queue_employee_email ON review_queue(employee_email, created_at DESC);

CREATE TABLE IF NOT EXISTS attendance_events (
  id BIGSERIAL PRIMARY KEY,
  raw_message_id BIGINT NOT NULL REFERENCES raw_messages(id) ON DELETE RESTRICT,
  employee_id UUID NOT NULL REFERENCES employees(id) ON DELETE RESTRICT,
  employee_email TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('IN', 'OUT', 'BREAK_START', 'BREAK_END')),
  event_time TIMESTAMPTZ NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'PST',
  source TEXT NOT NULL CHECK (source IN ('auto', 'review', 'admin_edit')),
  review_id BIGINT NULL REFERENCES review_queue(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attendance_events_employee_time ON attendance_events(employee_email, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_attendance_events_time ON attendance_events(event_time DESC);

CREATE TABLE IF NOT EXISTS audit_log (
  id BIGSERIAL PRIMARY KEY,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  old_value JSONB NULL,
  new_value JSONB NULL,
  performed_by TEXT NOT NULL,
  notes TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id, created_at DESC);

INSERT INTO employees (employee_name, employee_email)
VALUES
  ('Gene Orias', 'gene@astroinfosec.com')
ON CONFLICT (employee_email) DO NOTHING;
