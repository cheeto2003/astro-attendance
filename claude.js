import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY
});

export async function interpretAttendance(messageText) {
  const response = await client.messages.create({
    model: process.env.CLAUDE_MODEL,
    temperature: 0,
    max_tokens: 80,

    messages: [
      {
        role: "user",
        content: `You are an attendance message parser.

An employee sent this message in a Teams attendance channel:

"${messageText}"

Determine the most likely attendance event.

Allowed events:
IN
OUT
BREAK_START
BREAK_END
UNKNOWN

Rules:
- "in", "starting shift", "clocking in" → IN
- "out", "done", "logging off" → OUT
- "brb", "lunch", "break", "stepping away" → BREAK_START
- "back", "returned", "back from break" → BREAK_END
- If unclear → UNKNOWN

If the message mentions a time like "until 2pm pst", return the parsed time.

Return ONLY valid JSON.

{
  "event": "IN | OUT | BREAK_START | BREAK_END | UNKNOWN",
  "time": "ISO timestamp or null",
  "confidence": 0-1
}
`
      }
    ]
  });

  return response.content[0].text;
}
