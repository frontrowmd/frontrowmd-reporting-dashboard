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
// Expiry must outlive freshness, or an entry is gone the moment it goes stale
// and there is nothing to fall back on. Compared as NUMBERS rather than by
// pinning the expression, so the retention policy can change shape.
ck('and the KV expiry outlives the read TTL',
   (+(/const AZ_CACHE_RETAIN_SEC = (\d+);/.exec(src)||[])[1])*1000 > slowTtlMs, true);

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
   /_azBudget=_AZ_SLOW_WINS\.includes\(irfWin\)\?180000:75000/.test(cli),true);
// THE cross-file invariant: a window the worker treats as expensive must also
// get the longer client budget. When the two lists disagree the client aborts a
// window the worker knows is slow — and the abort kills the run before it can
// cache, so the next load is in the same position. That is the loop the MTD
// failure was stuck in.
{
  const wWins=((/const AZ_PRECOMPUTED_WINDOWS = \[([^\]]+)\]/.exec(src)||[])[1]||'')
    .split(',').map(x=>x.trim().replace(/^'|'$/g,'')).filter(Boolean);
  const cWins=((/const _AZ_SLOW_WINS=\[([^\]]+)\]/.exec(cli)||[])[1]||'')
    .split(',').map(x=>x.trim().replace(/^'|'$/g,'')).filter(Boolean);
  ck('both lists parse',[wWins.length>0,cWins.length>0],[true,true]);
  ck('every window the worker precomputes also gets the long client budget',
     wWins.filter(w=>cWins.indexOf(w)<0),[]);
}
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

// --- stale-while-revalidate: last good data beats an error -----------------
// A Year to Date rebuild cannot finish inside the browser's abort, so when one
// fails the real choice is between day-old ad data and none. Entries used to be
// dropped 5 minutes after going stale, which left nothing to fall back on after
// two bad crons or any key change — and the dashboard renders a missing channel
// as "No Meta data available for this window", which reads as "there was no
// spend" rather than "the fetch fell short".
ck('retention is separate from freshness',/const AZ_CACHE_RETAIN_SEC = (\d+);/.test(src),true);
ck('and is much longer than the freshness window',
   (+(/const AZ_CACHE_RETAIN_SEC = (\d+);/.exec(src)||[])[1]) >= 3600, true);
// Three writers now: the cron, the request path, and the background rebuild.
// Asserted as "every ad-data write uses it" rather than a fixed count, so
// adding a fourth writer that forgets the retention still fails.
ck('every ad-data writer uses it',
   (src.match(/expirationTtl: AZ_CACHE_RETAIN_SEC/g)||[]).length,
   (src.match(/CONTENT_STORE\.put\((?:_azCk|azCacheKey\()/g)||[]).length);
ck('and there are at least the three we know about',
   (src.match(/expirationTtl: AZ_CACHE_RETAIN_SEC/g)||[]).length>=3,true);
ck('no writer still uses the old ttl+300 retention',/expirationTtl: Math\.round\(azCacheTtlMs/.test(src),false);
// An expired entry is kept as a fallback rather than discarded on read.
ck('an expired entry is held, not dropped',/_azStale = pcache;/.test(src),true);
ck('a fresh entry still short-circuits',/if \(age < azCacheTtlMs\(_azWin\)\)/.test(src),true);
// Two paths fall back: a throw, and a run that came back missing a channel.
ck('a failed rebuild serves stale',/if \(_azStale\) return _azServeStale\(err && err\.message\);/.test(src),true);
ck('so does a run that lost a channel',/live run lost channel\(s\)/.test(src),true);
// Staleness is reported, never silent — on the wire and in the console.
ck('the payload says it is stale',/_stale: true, _staleAgeSec: ageSec/.test(src),true);
ck('and the console says so too',/analyzer serving STALE/.test(src),true);
ck('the dashboard labels it',/Showing the last complete ad data/.test(cli),true);
ck('with a readable age',/function _staleAge\(sec\)\{/.test(cli),true);
// The banner must ride the same element the filter bar is written into, or it
// renders on one page and not the other two.
ck('the banner reaches every page that shows the tables',
   /filterTargets\.forEach\(el=>el\.innerHTML=_staleNote\+_metaFilterBar/.test(cli),true);

// --- expensive windows must never rebuild on the request path -------------
// Observed: the Clinician Ad Tables on MTD reported "no response after 75s —
// the request was aborted". Cost here tracks campaign x ad-set x creative
// cardinality and the attribution match, not the number of days, so a SHORT
// window is not a cheap one. The abort also killed the run before it could
// cache, so every later load was in exactly the same position — a loop that a
// longer client timeout alone does not break.
ck('MTD is treated as an expensive window',
   /const AZ_PRECOMPUTED_WINDOWS = \['mtd', 'ytd', 'allTime'\];/.test(src),true);
// Being on that list is what buys it BOTH the cron warm and the long freshness
// TTL; one without the other does not help.
ck('so the cron warms it',/for \(const win of AZ_PRECOMPUTED_WINDOWS\)/.test(src),true);
ck('and it gets the long freshness TTL',
   /AZ_PRECOMPUTED_WINDOWS\.includes\(win\) \? 1800000 : 120000/.test(src),true);
ck('and the client gives it room to land once',
   /const _AZ_SLOW_WINS=\['mtd','ytd','allTime'\];/.test(cli),true);
// Last-good is returned immediately and the rebuild happens behind the response.
ck('a stale expensive window short-circuits the live compute',
   /if \(_azStale && AZ_PRECOMPUTED_WINDOWS\.includes\(_azWin\)\) \{/.test(src),true);
ck('the rebuild runs behind the response',/ctx\.waitUntil\(\(async \(\) => \{/.test(src),true);
ck('which means the handler takes ctx',/async fetch\(request, env, ctx\) \{/.test(src),true);
// Without a guard every concurrent reader kicks off the same rebuild.
ck('concurrent readers do not stampede the rebuild',
   /!AZ_REFRESHING\.has\(_azCk\)/.test(src)&&/AZ_REFRESHING\.add\(_azCk\)/.test(src),true);
ck('and the guard is always released',/finally \{ AZ_REFRESHING\.delete\(_azCk\); \}/.test(src),true);
// A background rebuild must apply the same health gate as the cron, or it warms
// the cache with gaps that then look stable.
ck('the background rebuild will not cache a degraded payload',
   /bg refresh \$\{_azWin\}: NOT caching/.test(src),true);
ck('and it uses the long retention like every other writer',
   (src.match(/expirationTtl: AZ_CACHE_RETAIN_SEC/g)||[]).length,3);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
