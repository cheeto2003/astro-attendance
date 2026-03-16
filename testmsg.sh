#!/usr/bin/env bash

MSG="$1"
MSG_ID="$2"

curl -s -X POST http://localhost:5000/webhook/teams \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: WVbqMZ01YK2kzO" \
  -d "{
    \"messageId\": \"${MSG_ID}\",
    \"employeeName\": \"Gene Orias\",
    \"employeeEmail\": \"gene@astroinfosec.com\",
    \"channelId\": \"in-n-out-testing\",
    \"messageText\": \"${MSG}\",
    \"messageTimestamp\": \"2026-03-12T19:10:00Z\",
    \"isReply\": false,
    \"isEdited\": false
  }"
echo
