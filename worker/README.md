# Finchvox Telemetry Worker

Cloudflare Worker that collects anonymous usage telemetry for Finchvox.

## Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. Create a KV namespace:
   ```bash
   wrangler kv:namespace create PINGS
   wrangler kv:namespace create PINGS --preview
   ```

3. Update `wrangler.toml` with the namespace IDs from the output above.

4. Deploy:
   ```bash
   npm run deploy
   ```

## Development

```bash
npm run dev
```

## API

**POST /** - Record an event
```json
{
  "event": "server_start" | "session_ingest" | "session_view",
  "version": "0.0.8",
  "os": "darwin" | "linux" | "windows"
}
```

**GET /** - View all stats (returns JSON object with all counters)
