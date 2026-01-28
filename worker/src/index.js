const JSON_HEADERS = { "Content-Type": "application/json" };
const STATS_KEY = "stats";

async function getStats(env) {
  return JSON.parse(await env.PINGS.get(STATS_KEY) || "{}");
}

async function handleGet(env) {
  const stats = await getStats(env);
  return new Response(JSON.stringify(stats, null, 2), { headers: JSON_HEADERS });
}

async function handlePost(request, env) {
  const data = await request.json();
  const event = data.event || "unknown";
  const version = data.version || "unknown";
  const os = data.os || "unknown";

  const stats = await getStats(env);
  const keys = [`total:${event}`, `version:${version}:${event}`, `os:${os}:${event}`];
  for (const key of keys) {
    stats[key] = (stats[key] || 0) + 1;
  }
  await env.PINGS.put(STATS_KEY, JSON.stringify(stats));

  return new Response(JSON.stringify({ ok: true }), { headers: JSON_HEADERS });
}

export default {
  async fetch(request, env) {
    if (request.method === "GET") {
      return handleGet(env);
    }
    if (request.method === "POST") {
      return handlePost(request, env);
    }
    return new Response("Method not allowed", { status: 405 });
  }
};
