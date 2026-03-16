# Retool app build plan

## Resources
1. Add your PostgreSQL database as a Retool Resource.
2. Add your backend API as a REST Resource.
3. Store `X-Admin-Api-Key` in the API resource headers.

## Page 1: Pending Review
- Table query: `SELECT * FROM review_queue WHERE status = 'pending' ORDER BY created_at DESC LIMIT 500`
- Row details panel: show raw message by joining `raw_messages`.
- Approve button: call `POST /api/reviews/{{ table.selectedRow.data.id }}/approve`
- Reject button: call `POST /api/reviews/{{ table.selectedRow.data.id }}/reject`
- Edit form fields:
  - eventType dropdown: IN, OUT, BREAK_START, BREAK_END
  - eventTime datetime picker
  - reviewedBy text input
  - reviewNotes text area

## Page 2: Attendance Log
- Query attendance_events joined to employees.
- Filters: employee email, event type, date range, source.

## Page 3: Audit Log
- Read-only table on `audit_log`.
- Filters: entity_type, performed_by, date range.

## Row action UX
- Default state is review first.
- Never allow direct delete from attendance_events.
- Any correction should create an `admin_edit` event or a reviewed event, never overwrite prior raw_messages.
