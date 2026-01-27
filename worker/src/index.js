const JSON_HEADERS = { "Content-Type": "application/json" };

async function handleGet(env) {
  const keys = await env.PINGS.list();
  const stats = {};
  for (const key of keys.keys) {
    stats[key.name] = await env.PINGS.get(key.name);
  }
  return new Response(JSON.stringify(stats, null, 2), { headers: JSON_HEADERS });
}

async function incrementCounter(kv, key) {
  const current = parseInt(await kv.get(key) || "0") + 1;
  await kv.put(key, current.toString());
}

async function handlePost(request, env) {
  const data = await request.json();
  const event = data.event || "unknown";
  const version = data.version || "unknown";
  const os = data.os || "unknown";

  await Promise.all([
    incrementCounter(env.PINGS, `total:${event}`),
    incrementCounter(env.PINGS, `version:${version}:${event}`),
    incrementCounter(env.PINGS, `os:${os}:${event}`)
  ]);

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
