// Sign-in via Cloudflare Access (2026-11-10). The team password is gone, so the
// token check in accessIdentity IS the security boundary for every data route.
// Tokens here are signed with node's own crypto.sign — a different
// implementation from the worker's Web Crypto verify — so a wrong encoding or
// signing input in the worker cannot agree with the test by construction.
const { load, fn } = require('./slice.js');
const vm = require('vm');
const nodeCrypto = require('crypto');
const path = require('path');
const src = load(process.env.WORKER_SRC || 'worker.js');
let pass = 0, fail = 0;
const ck = (n, g, w) => { const ok = JSON.stringify(g) === JSON.stringify(w); console.log((ok ? 'PASS ' : 'FAIL ') + n + '  got=' + JSON.stringify(g) + (ok ? '' : ' want=' + JSON.stringify(w))); ok ? pass++ : fail++; };

const TEAM = 'https://frontrowmd.cloudflareaccess.com';
const AUD = '7daa3d49c47e143739e3a8fbf4e43e9ccb4ff2ad9c434fc866495c8bdb29209e';
const now = () => Math.floor(Date.now() / 1000);
const b64u = b => Buffer.from(b).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
const kp = () => nodeCrypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
const K1 = kp(), K2 = kp();
const jwk = (k, kid) => Object.assign(k.publicKey.export({ format: 'jwk' }), { kid, alg: 'RS256', use: 'sig' });
function tok(payload, { key = K1, kid = 'k1', alg = 'RS256', header } = {}) {
  const h = b64u(JSON.stringify(header || { alg, kid, typ: 'JWT' }));
  const p = b64u(JSON.stringify(payload));
  const sig = nodeCrypto.sign('sha256', Buffer.from(h + '.' + p), key.privateKey);
  return h + '.' + p + '.' + b64u(sig);
}
const good = (o = {}) => Object.assign({ iss: TEAM, aud: [AUD], email: 'alice@thefrontrowhealth.com', iat: now(), nbf: now(), exp: now() + 3600, type: 'app' }, o);

// ── Part A: accessIdentity on its own ───────────────────────────────────────
const a0 = src.indexOf('// ─── Sign-in: Cloudflare Access');
const a1 = src.indexOf('export default {', a0);
ck('the sign-in block exists above the router', a0 > 0 && a1 > a0, true);
const block = src.slice(a0, a1);

function sandbox(certs) {
  const calls = [];
  const c = { console: { error() {}, log() {}, warn() {} }, crypto: globalThis.crypto, atob, TextEncoder, TextDecoder, Uint8Array, Date, JSON, Map, Array, String, Math, Error,
    fetch: async (u) => { calls.push(String(u)); const r = typeof certs === 'function' ? certs() : certs; return r; } };
  vm.createContext(c);
  vm.runInContext(fn(src, 'azDrain') + '\n' + block + '\nObject.assign(globalThis,{accessIdentity});', c);
  return { c, calls };
}
const certsOk = (...keys) => ({ ok: true, status: 200, json: async () => ({ keys }) });
const req = t => ({ headers: { get: n => (n.toLowerCase() === 'cf-access-jwt-assertion' ? t : null) } });

(async () => {
  {
    const { c, calls } = sandbox(certsOk(jwk(K1, 'k1')));
    const id = t => c.accessIdentity(req(t), {});
    ck('a valid company sign-in is accepted', await id(tok(good())), { email: 'alice@thefrontrowhealth.com' });
    ck('  and reads the team certs from the team domain', calls[0], TEAM + '/cdn-cgi/access/certs');
    ck('  keys are cached (second request fetches nothing)', (await id(tok(good())), calls.length), 1);
    ck('no token → refused', await c.accessIdentity(req(null), {}), null);
    ck('an aud string (not array) is accepted', await id(tok(good({ aud: AUD }))), { email: 'alice@thefrontrowhealth.com' });
    ck('another Access app\'s audience → refused', await id(tok(good({ aud: ['someotherapp'] }))), null);
    ck('  unless that AUD is listed in ACCESS_AUD', await c.accessIdentity(req(tok(good({ aud: ['someotherapp'] }))), { ACCESS_AUD: 'x, someotherapp' }), { email: 'alice@thefrontrowhealth.com' });
    ck('another team\'s issuer → refused', await id(tok(good({ iss: 'https://evil.cloudflareaccess.com' }))), null);
    ck('expired → refused', await id(tok(good({ exp: now() - 120 }))), null);
    ck('no exp → refused', await id(tok(good({ exp: undefined }))), null);
    ck('not yet valid → refused', await id(tok(good({ nbf: now() + 600 }))), null);
    ck('a personal Gmail → refused', await id(tok(good({ email: 'alice@gmail.com' }))), null);
    ck('a look-alike domain → refused', await id(tok(good({ email: 'a@thefrontrowhealth.com.evil.io' }))), null);
    ck('a sub-domain look-alike → refused', await id(tok(good({ email: 'a@evilthefrontrowhealth.com' }))), null);
    ck('an empty local part → refused', await id(tok(good({ email: '@thefrontrowhealth.com' }))), null);
    ck('no email (service token) → refused', await id(tok(good({ email: undefined }))), null);
    ck('domain case is ignored', await id(tok(good({ email: 'Bob@TheFrontRowHealth.com' }))), { email: 'bob@thefrontrowhealth.com' });
    const t = tok(good()).split('.');
    const forged = t[0] + '.' + b64u(JSON.stringify(good({ email: 'mallory@thefrontrowhealth.com' }))) + '.' + t[2];
    ck('an edited payload with the old signature → refused', await id(forged), null);
    ck('signed by a different key under the same kid → refused', await id(tok(good(), { key: K2 })), null);
    ck('alg none → refused', await id(b64u(JSON.stringify({ alg: 'none', kid: 'k1' })) + '.' + b64u(JSON.stringify(good())) + '.'), null);
    ck('alg HS256 → refused', await id(tok(good(), { alg: 'HS256' })), null);
    ck('two-part token → refused', await id('a.b'), null);
    ck('garbage → refused, no throw', await id('%%%.@@@.***'), null);
  }
  {
    // Rotation: a new kid triggers ONE refetch; a burst of unknown kids does not
    // turn every request into a certs fetch.
    let n = 0;
    const { c, calls } = sandbox(() => (++n === 1 ? certsOk(jwk(K1, 'k1')) : certsOk(jwk(K1, 'k1'), jwk(K2, 'k2'))));
    await c.accessIdentity(req(tok(good())), {});
    ck('a rotated key is picked up by refetching', await c.accessIdentity(req(tok(good(), { key: K2, kid: 'k2' })), {}), { email: 'alice@thefrontrowhealth.com' });
    const before = calls.length;
    for (let i = 0; i < 5; i++) await c.accessIdentity(req(tok(good(), { kid: 'bogus' + i })), {});
    ck('unknown kids within a minute cause no further fetches', calls.length - before, 0);
  }
  {
    const { c } = sandbox({ ok: false, status: 503, json: async () => ({}) });
    ck('certs outage fails CLOSED', await c.accessIdentity(req(tok(good())), {}), null);
    const s2 = sandbox(() => { throw new Error('network'); });
    ck('network error fails CLOSED', await s2.c.accessIdentity(req(tok(good())), {}), null);
  }

  // ── Part B: the router ─────────────────────────────────────────────────────
  ck('no password check is left in the worker', /TEAM_PASSWORD/.test(src.replace(/\/\/.*$/gm, '')), false);
  const routes = [];
  const re = /if \(request\.method === '(GET|POST)' && \(?url\.pathname === '([^']+)'/g;
  let m; while ((m = re.exec(src))) routes.push({ method: m[1], path: m[2] });
  const OPEN = ['GET /', 'GET /sales', 'GET /bd', 'POST /api/slack/events'];
  ck('the route list was read', routes.length > 25, true);

  const realFetch = globalThis.fetch;
  const fetched = [];
  globalThis.fetch = async (u, o) => { fetched.push(String(u)); if (String(u) === TEAM + '/cdn-cgi/access/certs') return new Response(JSON.stringify({ keys: [jwk(K1, 'k1')] })); throw new Error('unexpected fetch ' + u); };
  const kvStore = {};
  const env = { TEAM_PASSWORD: 'oldpw', SLACK_SIGNING_SECRET: 'sss', CONTENT_STORE: { get: async () => null, put: async (k, v) => { kvStore[k] = v; }, delete: async () => {} } };
  const ctx = { waitUntil() {}, passThroughOnException() {} };
  const W = (await import(path.resolve(process.env.WORKER_DIST || 'dist/worker.js'))).default;
  const hit = (method, p, body, headers = {}) => W.fetch(new Request('https://frontrowmd-dashboard-api.alex-5b6.workers.dev' + p, { method, headers: Object.assign({ 'Content-Type': 'application/json' }, headers), body: method === 'POST' ? JSON.stringify(body || {}) : undefined }), env, ctx);

  const leaked = [];
  for (const r of routes) {
    const key = r.method + ' ' + r.path;
    if (OPEN.includes(key)) continue;
    const res = await hit(r.method, r.path, { password: 'oldpw' });
    if (res.status !== 401) leaked.push(key + '→' + res.status);
  }
  ck('every data route refuses the OLD password with no sign-in', leaked, []);
  const forgedRes = await hit('POST', '/api/pq', { password: 'oldpw' }, { 'Cf-Access-Jwt-Assertion': tok(good(), { key: K2 }) });
  ck('a forged token gets 401', forgedRes.status, 401);
  const okRes = await hit('POST', '/api/pq', {}, { 'Cf-Access-Jwt-Assertion': tok(good()) });
  ck('a signed-in request with NO password gets through', okRes.status, 200);
  const gmail = await hit('POST', '/api/pq', {}, { 'Cf-Access-Jwt-Assertion': tok(good({ email: 'x@gmail.com' })) });
  ck('a signed-in non-company account gets 401', gmail.status, 401);
  ck('the HTML page loads without a token', (await hit('GET', '/')).status, 200);

  // Cross-site request forgery: the cookie (so the token) rides on requests a
  // hostile site triggers. Those must be refused even with a valid token.
  const T = { 'Cf-Access-Jwt-Assertion': tok(good()) };
  const ORIG = 'https://frontrowmd-dashboard-api.alex-5b6.workers.dev';
  ck('CSRF: a cross-site fetch is refused', (await hit('POST', '/api/pq/save', { mode: 'replace', pqMap: {} }, { ...T, 'Sec-Fetch-Site': 'cross-site', Origin: 'https://evil.example' })).status, 401);
  ck('CSRF: a same-site (other workers.dev) request is refused', (await hit('POST', '/api/pq', {}, { ...T, 'Sec-Fetch-Site': 'same-site' })).status, 401);
  ck('CSRF: a foreign Origin with no Sec-Fetch-Site is refused', (await hit('POST', '/api/pq', {}, { ...T, Origin: 'https://evil.example' })).status, 401);
  ck('CSRF: an opaque "null" Origin is refused', (await hit('POST', '/api/pq', {}, { ...T, Origin: 'null' })).status, 401);
  ck('CSRF: the page\'s own same-origin request is accepted', (await hit('POST', '/api/pq', {}, { ...T, 'Sec-Fetch-Site': 'same-origin', Origin: ORIG })).status, 200);
  ck('CSRF: nothing was written by the refused save', Object.keys(kvStore).length, 0);

  fetched.length = 0;
  const raw = JSON.stringify({ type: 'url_verification', challenge: 'chal' });
  const ts = String(now());
  const sig = 'v0=' + nodeCrypto.createHmac('sha256', 'sss').update('v0:' + ts + ':' + raw).digest('hex');
  const sl = await W.fetch(new Request('https://frontrowmd-dashboard-api.alex-5b6.workers.dev/api/slack/events', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Slack-Request-Timestamp': ts, 'X-Slack-Signature': sig }, body: raw }), env, ctx);
  const slBody = await sl.text();
  ck('Slack events still work with no sign-in (signature is the auth)', sl.status === 200 && slBody.includes('chal'), true);
  ck('  and never consult Access', fetched.filter(u => u.includes('cloudflareaccess')).length, 0);
  globalThis.fetch = realFetch;

  // ── Part C: the pages ──────────────────────────────────────────────────────
  for (const f of ['dashboard.html', 'sales-dashboard.html', 'bd-dashboard.html']) {
    const h = load(f);
    ck(f + ': no password field', /type="password"|Team password/.test(h), false);
    ck(f + ': loads on its own once the script has run', /addEventListener\('DOMContentLoaded',function\(\)\{doLogin\(\);\}\)/.test(h), true);
    ck(f + ': explains a refused account', /doesn\\u2019t have access/.test(h), true);
    ck(f + ': no listener on the removed #pw input', /getElementById\('pw'\)/.test(h), false);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
