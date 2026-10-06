// Year to Date is structurally expensive (~107k Meta creative rows over ~54
// Windsor calls, ~38s measured) and was recomputed on every load, inside a
// browser request that aborted at 75s — so the cache could never populate either.
// A cron computes it where nothing can abort it; the request path reads KV.
const {load,fn}=require('./slice.js');
const src=load('worker.js'), cli=load('dashboard.html'), toml=load('wrangler.toml');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

ck('a cron trigger is configured',/\[triggers\]\s*\ncrons = \["\*\/\d+ \* \* \* \*"\]/.test(toml),true);
ck('the worker exports a scheduled handler',/async scheduled\(event, env, ctx\)/.test(src),true);
ck('it runs in waitUntil so the invocation is not cut short',/ctx\.waitUntil\(\(async \(\) => \{/.test(src),true);

// The cron is worthless if it writes a key the request path does not read.
ck('one shared key builder',/function azCacheKey\(win, from, to, vsFrom, vsTo\)/.test(src),true);
ck('the request path uses it',/const _azCk = azCacheKey\(_azWin, body\.from, body\.to, body\.vsFrom, body\.vsTo\);/.test(src),true);
ck('and the cron writes through the same builder',/azCacheKey\(win, null, null, null, null\)/.test(src),true);
ck('no hand-built analyzer key remains',/'azdata_resp_' \+ API_CACHE_VER \+ '_' \+ _azWin/.test(src),false);

// A warmed payload is useless if it expires before the next cron tick.
const cronMin=+(/crons = \["\*\/(\d+)/.exec(toml)||[])[1];
const slowTtlMs=+(/AZ_PRECOMPUTED_WINDOWS\.includes\(win\) \? (\d+) :/.exec(src)||[])[1];
ck('the cron interval parses',Number.isInteger(cronMin)&&cronMin>0,true);
ck('the warm TTL outlives the cron interval',slowTtlMs>cronMin*60*1000,true);
ck('and the KV expiry outlives the read TTL',
   /expirationTtl: Math\.round\(azCacheTtlMs\((?:win|_azWin)\)\/1000\) \+ 300/.test(src),true);

// Degraded payloads must never be warmed, or every reader inherits the gap.
// The gates diverge ON PURPOSE: a request retries on the next page load, a cron
// retry costs another 20 minutes of nothing. Both still refuse missing ranges.
ck('the cron refuses an exhausted pool and dropped chunks, like the request path',
   /const ok = !SUBREQ\.exhausted && !SUBREQ\.windsorDropped/.test(src),true);
ck('and skips caching when degraded',/if \(!ok\) \{ console\.warn\([\s\S]{0,120}?continue; \}/.test(src),true);
ck('each window gets its own subrequest budget',/for \(const win of AZ_PRECOMPUTED_WINDOWS\) \{\s*\n\s*subreqReset\(\);/.test(src),true);
ck('a cron failure cannot break the request path',/catch \(e\) \{ console\.error\(`cron warm/.test(src),true);

// A cold window still has to compute live; 75s aborted it every time.
ck('the client gives the slow windows room',
   /const _AZ_SLOW_WINS=\['ytd','allTime'\];/.test(cli)
   &&/_azBudget=_AZ_SLOW_WINS\.includes\(irfWin\)\?180000:75000/.test(cli),true);
ck('and the abort uses that budget, not a literal',/\},_azBudget\);/.test(cli),true);
ck('other windows keep the tight bound',/:75000;/.test(cli),true);
// The two lists must agree, or a window is warmed but still aborted early.
const workerWins=(/AZ_PRECOMPUTED_WINDOWS = \[([^\]]*)\]/.exec(src)||[])[1].replace(/['\s]/g,'').split(',');
const clientWins=(/_AZ_SLOW_WINS=\[([^\]]*)\]/.exec(cli)||[])[1].replace(/['\s]/g,'').split(',');
ck('the worker and client agree on which windows are slow',workerWins.sort(),clientWins.sort());

// --- placement is the row multiplier -------------------------------------
// Measured YTD: creatives were 378s of a 380s request; one facebook chunker was
// 60 calls / 115,404 rows / 151s. Windsor returns a row per creative PER DAY PER
// PLACEMENT, so that one dimension is the whole cost on a long window.
ck('placement is conditional on the window span',
   /const placement = \(isMetaConnectorCh\(ch\) && !_creWideWindow\)/.test(src),true);
ck('the span is measured in days',/const _creSpanDays = Math\.round\(/.test(src),true);
ck('and the threshold is explicit',/const _creWideWindow = _creSpanDays > 120;/.test(src),true);
ck('dropping it is logged, not silent',/placement breakdown omitted/.test(src),true);
// A YTD span must trip it; a month must not.
{
  const span=(a,b)=>Math.round((new Date(b+'T00:00:00Z')-new Date(a+'T00:00:00Z'))/86400000)+1;
  ck('a year-to-date window trips the threshold',span('2026-01-01','2026-10-05')>120,true);
  ck('a month-to-date window does not',span('2026-10-01','2026-10-05')>120,false);
  ck('nor does last month',span('2026-09-01','2026-09-30')>120,false);
}
// Every client consumer must tolerate the empty array this produces.
ck('the client guards an empty placements array',
   /var placements=metaCh&&metaCh\.placements\?metaCh\.placements:\[\];/.test(cli)
   &&/if\(!placements\.length\)\{/.test(cli),true);
ck('and guards it per creative too',/c\.placements&&c\.placements\.length>1/.test(cli),true);
ck('and before the channel-level breakdown',/if\(az&&az\.channels&&az\.channels\.meta&&az\.channels\.meta\.placements\)/.test(cli),true);

// --- the cron must not discard a 6-minute rebuild over a small gap --------
ck('the cron tolerates a bounded number of soft failures',
   /const AZ_CRON_SOFTFAIL_TOLERANCE = \d+;/.test(src),true);
ck('it still refuses a dropped chunk or an exhausted pool',
   /!SUBREQ\.exhausted && !SUBREQ\.windsorDropped\s*\n\s*&& SUBREQ\.softFail <= AZ_CRON_SOFTFAIL_TOLERANCE/.test(src),true);
ck('and says so when it caches a degraded payload',/caching WITH \$\{SUBREQ\.softFail\} soft failure/.test(src),true);
// The REQUEST path stays strict: a retry there costs one page load, not 20 minutes.
ck('the request path keeps the strict gate',
   /const _azOk = !SUBREQ\.exhausted && !SUBREQ\.windsorDropped && !SUBREQ\.softFail;/.test(src),true);
ck('the tolerance is small, not a blank cheque',
   +(/AZ_CRON_SOFTFAIL_TOLERANCE = (\d+)/.exec(src)||[])[1]<=10,true);

// --- the warm cache must survive an unrelated payload change ---------------
// The whole point of the cron is that Year to Date and All Time are too
// expensive to build on the request path. That guarantee is only as good as the
// cache key: azCacheKey used to embed API_CACHE_VER, which tracks the DASHBOARD
// PAYLOAD shape and is bumped whenever a card is added anywhere. So adding a
// clinician chart silently threw away the warmed ad data, and the Ads section
// failed to load until the next cron — reading exactly like the timeout bug the
// precompute exists to prevent.
const AZKEY=fn(src,'azCacheKey');
ck('the ad-data cache key does not ride the payload version',
   /API_CACHE_VER/.test(AZKEY),false);
ck('it has a version of its own',/azdata_resp_' \+ AZ_DATA_VER/.test(AZKEY),true);
ck('which is declared',/const AZ_DATA_VER = '[^']+';/.test(src),true);
// The response cache for /api/data is the opposite case: it caches the payload
// itself, so it MUST invalidate when the payload shape changes.
ck('but the api response cache still does ride it',
   /'apidata_resp_' \+ API_CACHE_VER/.test(src),true);
// Both the cron writer and the request reader must go through the one function,
// or they can disagree about where the warmed entry lives.
ck('the cron writes through azCacheKey',/CONTENT_STORE\.put\(azCacheKey\(win, null, null, null, null\)/.test(src),true);
ck('and nothing builds an azdata key by hand',
   (src.match(/'azdata_resp_/g)||[]).length,1);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
