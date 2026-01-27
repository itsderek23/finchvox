export default {
  async fetch(request, env) {
    if (request.method === "GET") {
      const keys = await env.PINGS.list();
      const stats = {};
      for (const key of keys.keys) {
        stats[key.name] = await env.PINGS.get(key.name);
      }
      return new Response(JSON.stringify(stats, null, 2), {
        headers: { "Content-Type": "application/json" }
      });
    }

    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    const data = await request.json();
    const event = data.event || "unknown";
    const version = data.version || "unknown";
    const os = data.os || "unknown";

    const totalKey = `total:${event}`;
    const versionKey = `version:${version}:${event}`;
    const osKey = `os:${os}:${event}`;

    const total = parseInt(await env.PINGS.get(totalKey) || "0") + 1;
    const versionCount = parseInt(await env.PINGS.get(versionKey) || "0") + 1;
    const osCount = parseInt(await env.PINGS.get(osKey) || "0") + 1;

    await Promise.all([
      env.PINGS.put(totalKey, total.toString()),
      env.PINGS.put(versionKey, versionCount.toString()),
      env.PINGS.put(osKey, osCount.toString())
    ]);

    return new Response(JSON.stringify({ ok: true }), {
      headers: { "Content-Type": "application/json" }
    });
  }
};
