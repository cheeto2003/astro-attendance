require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });

module.exports = {
  port: Number(process.env.PORT || 5000),
  databaseUrl: process.env.DATABASE_URL,
  webhookSecret: process.env.WEBHOOK_SECRET || '',
  adminApiKey: process.env.ADMIN_API_KEY || '',
  timezone: process.env.TZ || 'America/Los_Angeles',
  openclawUrl: process.env.OPENCLAW_EXPLANATION_URL || '',
  openclawKey: process.env.OPENCLAW_EXPLANATION_KEY || '',
  openclawTimeoutMs: Number(process.env.OPENCLAW_TIMEOUT_MS || 12000)
};
