# ASTRO attendance backend + Retool review architecture

This blueprint replaces Google Sheets as the source of truth.

## Core rules
- Only exact `in`, `out`, `break`, `back` auto-log.
- Only exact backfill format `in|out|break|back at H:MM AM/PM PST` auto-logs.
- Replies are ignored.
- Edited messages are ignored.
- Anything else goes to `review_queue`.
- OpenClaw may explain context, but it never logs attendance.

## Why this architecture
It scales better than using a spreadsheet as the system of record. Retool gives non-technical reviewers a UI, while PostgreSQL stores raw messages, final attendance events, and audit history.

## Local run
1. Create PostgreSQL database.
2. Run `schema.sql`.
3. Copy `.env.example` to `.env` and fill values.
4. Run `npm install`.
5. Run `npm start`.

## Production recommendation
- Host backend behind HTTPS.
- Keep PostgreSQL as source of truth.
- Put Retool in front of the review workflow.
- Lock write access down to only the approve/reject endpoints.

## Suggested rollout
1. Import employees into `employees`.
2. Point Teams webhook to `/webhook/teams`.
3. Build Retool Pending Review app first.
4. Verify audit trail.
5. Then retire Google Sheets logging.
