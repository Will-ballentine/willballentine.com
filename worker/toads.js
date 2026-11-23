// toads Worker: serves the page from dist/ and a read-only API at /api/readings.
// The API queries InfluxDB at home through the Cloudflare Tunnel, so the InfluxDB token
// stays in the Worker and never reaches the browser. Responses are cached at Cloudflare
// for 60 s, so visitors never hit the home server more than once a minute per range.

const FIELDS = ['soil_temp_c', 'air_temp_c', 'humidity_pct', 'pressure_hpa', 'soil_moisture_pct'];
const RANGES = { '24h': null, '7d': '30m', '30d': '2h' };  // range -> averaging window (null = raw 5-min readings)

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname !== '/api/readings') return env.ASSETS.fetch(request);

    const range = url.searchParams.get('range') in RANGES ? url.searchParams.get('range') : '24h';
    const key = new Request(`${url.origin}/api/readings?range=${range}`);
    const hit = await caches.default.match(key);
    if (hit) return fresh(hit);

    const base = start => `from(bucket: "${env.INFLUX_BUCKET}")
      |> range(start: -${start})
      |> filter(fn: (r) => r._measurement == "tank" and r._field =~ /^(${FIELDS.join('|')})$/)`;
    const tail = `|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") |> group()`;
    const win = RANGES[range];
    try {
      const [latest, rows] = await Promise.all([
        flux(env, `${base('2h')} ${tail} |> sort(columns: ["_time"], desc: true) |> limit(n: 1)`),
        flux(env, `${base(range)} ${win ? `|> aggregateWindow(every: ${win}, fn: mean, createEmpty: false)` : ''} ${tail} |> sort(columns: ["_time"])`),
      ]);
      const series = { time: rows.map(r => Date.parse(r._time)) };
      for (const f of FIELDS) series[f] = rows.map(r => num(r[f]));
      const l = latest[0];
      const body = { latest: l ? { time: Date.parse(l._time), ...Object.fromEntries(FIELDS.map(f => [f, num(l[f])])) } : null, series };
      const res = Response.json(body, { headers: { 'Cache-Control': 'public, max-age=60', 'Access-Control-Allow-Origin': '*' } });
      ctx.waitUntil(caches.default.put(key, res.clone()));
      return fresh(res);
    } catch (e) {
      return Response.json({ error: e.message }, { status: 502, headers: { 'Access-Control-Allow-Origin': '*' } });
    }
  },
};

// Responses from the Cloudflare cache come back with the zone's 4-hour browser TTL;
// tell browsers not to reuse the data, so every page refresh asks again (the edge cache still absorbs the load).
const fresh = r => { const x = new Response(r.body, r); x.headers.set('Cache-Control', 'no-store'); return x; };

const num = v => (v === undefined || v === '' ? null : Number(v));

// Run a Flux query and return rows as objects keyed by column name.
async function flux(env, query) {
  const res = await fetch(`${env.INFLUX_URL}/api/v2/query?org=${encodeURIComponent(env.INFLUX_ORG)}`, {
    method: 'POST',
    headers: {
      Authorization: `Token ${(env.INFLUX_TOKEN ?? '').trim()}`,  // trim: a pasted secret can carry a stray space or newline
      'Content-Type': 'application/json',
      Accept: 'application/csv',
      // Cloudflare Access service token (only sent if set)
      ...(env.CF_ACCESS_CLIENT_ID && { 'CF-Access-Client-Id': env.CF_ACCESS_CLIENT_ID, 'CF-Access-Client-Secret': env.CF_ACCESS_CLIENT_SECRET }),
    },
    body: JSON.stringify({ query, dialect: { header: true, annotations: [] } }),
  });
  if (!res.ok) {
    const t = (env.INFLUX_TOKEN ?? '').trim();  // describe the token without revealing it
    throw new Error(`InfluxDB ${res.status}: ${(await res.text()).slice(0, 200)} [INFLUX_TOKEN ${t ? `${t.length} chars, ends ...${t.slice(-4)}` : 'missing'}]`);
  }
  const lines = (await res.text()).split(/\r?\n/).filter(Boolean);
  const header = lines[0]?.split(',') ?? [];
  return lines.slice(1).filter(l => l !== lines[0]).map(l => Object.fromEntries(l.split(',').map((v, i) => [header[i], v])));
}
