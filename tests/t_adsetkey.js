// Ad sets are campaign + name (2026-11-17). Keyed by name alone, the Clinician
// "TOF - Clinicians - Job Titles - Clinician" ad set that lives in BOTH
// Clinician Targeting - CAPI and Clinician Targeting - 2% Lookalikes folded into
// one row: the Lookalikes ad set vanished from every Ad Sets table and its spend
// was booked to the CAPI campaign. This drives the real fold functions.
const { load, fn } = require('./slice.js');
const vm = require('vm');
const src = load(process.env.WORKER_SRC || 'worker.js');
const dash = load('dashboard.html');
let pass = 0, fail = 0;
const ck = (n, g, w) => { const ok = JSON.stringify(g) === JSON.stringify(w); console.log((ok ? 'PASS ' : 'FAIL ') + n + '  got=' + JSON.stringify(g) + (ok ? '' : ' want=' + JSON.stringify(w))); ok ? pass++ : fail++; };

// The whole worker, minus its export, so every helper the fold calls is real.
const ctx = { console: { log() {}, warn() {}, error() {} }, crypto, TextEncoder, TextDecoder, URL, URLSearchParams, atob, btoa, Response, Request, fetch: async () => { throw new Error('no fetch'); }, setTimeout, clearTimeout };
vm.createContext(ctx);
vm.runInContext(src.replace(/export default \{/, 'globalThis.__W = {') + '\nObject.assign(globalThis,{azAdsetKey,_foldAudRows,_foldCreRows});', ctx);

const J = 'TOF - Clinicians - Job Titles - Clinician';
const CAPI = 'S01 - TOF - Clinician Targeting - CAPI', LAL = 'S01 - TOF - Clinician Targeting - 2% Lookalikes';
const row = (camp, adset, spend, date) => ({ date, campaign_name: camp, adset_name: adset, spend, clicks: 1, impressions: 100, adset_effective_status: 'ACTIVE' });
const rows = [row(CAPI, J, 100, '2026-11-02'), row(LAL, J, 60, '2026-11-02'), row(CAPI, J, 50, '2026-11-03'), row(LAL, J, 40, '2026-11-03'),
              row(CAPI, 'TOF - Clinicians - Med School Interests - Clinician', 1, '2026-11-03')];
const cfg = { connector: 'facebook', nameField: 'adset_name', demoField: [], statusField: 'adset_effective_status', statusFallback: 'adset_status', onlyClinician: true };
const map = {};
ctx._foldAudRows('clinician', cfg, rows, map);
const list = Object.values(map).map(a => [a.campaign, a.name, a.spend]).sort();
ck('two same-named ad sets in two campaigns stay two rows', list.filter(x => x[1] === J).length, 2);
ck('  each keeps its own campaign and spend', list.filter(x => x[1] === J), [[LAL, J, 100], [CAPI, J, 150]].sort());
ck('the other ad set is untouched', list.filter(x => x[1] !== J).length, 1);
ck('total spend is preserved', Object.values(map).reduce((s, a) => s + a.spend, 0), 251);
ck('one ad set across many rows is still one row', Object.keys(map).length, 3);

ck('page and worker build the same key',
  (() => { const m = dash.match(/function _azAdsetKey\(campaign,adset\)\{[^\n]*\}/); if (!m) return 'missing';
    const c2 = {}; vm.createContext(c2); vm.runInContext(m[0], c2);
    return [[' S01 - X ', 'Ad Set '], ['', 'a'], [CAPI, J]].every(([c, a]) => c2._azAdsetKey(c, a) === ctx.azAdsetKey(c, a)); })(), true);
ck('the Ad Sets table looks creatives up by campaign + name', /byAdset\[_azAdsetKey\(a\.campaign,a\.name\)\]/.test(dash), true);
ck('the Campaign dropdown does too', /_byAdsetCamp\[_azAdsetKey\(a\.campaign,a\.name\)\]/.test(dash), true);
ck('no ad-set lookup by bare name is left', /byAdset(?:Camp)?\[(?:key|akey)\]/.test(dash), false);
ck('the creative fold keys by campaign + name', /const adsetKey = azAdsetKey\(campName, adsetName\)/.test(src), true);
ck('the cache version moved past a3 (cached payloads use the old keys)', /const AZ_DATA_VER = 'a3'/.test(src), false);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
