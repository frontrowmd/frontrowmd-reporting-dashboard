// A cache that stores a FAILED fetch serves the failure to everyone for the
// TTL. On 2026-10-09 HubSpot's edge refused every call for ~15 minutes; the
// brand-trend builder guarded against an empty WINDSOR fetch but not an empty
// HubSpot one, so it cached a series carrying real spend and ZERO demos. Every
// reader then saw Count 0 / CVR 0.00% / Cost Per — under a perfectly healthy
// Ad Spend area, for the full 30 minutes.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. The brand-trend builder refuses BOTH empty halves ──────────────────
const BT=(()=>{const i=src.indexOf('const _btWin = clnTrendWindow');
  return src.slice(i,src.indexOf('brand trend failed:',i));})();
ck('an empty Windsor fetch is not cached',
   /if \(!Array\.isArray\(_w\) \|\| !_w\.length\) throw new Error\('brand trend: no Windsor rows/.test(BT),true);
ck('a refused HubSpot fetch is not cached either',
   /throw new Error\('brand trend: HubSpot refused \u2014 not caching'\)/.test(BT),true);
// Both guards must run BEFORE the series is built, or the bad series is built
// and returned even if it is never written.
ck('  and both guard before buildBrandTrend runs',
   BT.indexOf("no Windsor rows")<BT.indexOf('buildBrandTrend(')
   && BT.indexOf("HubSpot refused")<BT.indexOf('buildBrandTrend('),true);
// Refusing to WRITE does not remove an entry already written — kvCachedFetch
// serves an expired entry as stale whenever the rebuild throws. The key version
// is what abandons the poisoned ones.
ck('the cache key was versioned past the poisoned entries',
   /brand_trend_\$\{_btWin\.from\}_\$\{_btWin\.to\}_v3/.test(src),true);
ck('  and no v2 key is still read',/brand_trend_[^`]*_v2/.test(src),false);

// ── 5. Ordering: the ad fetch must not starve the HubSpot one ─────────────
// hsSearch returns [] with ZERO http calls once SUBREQ.exhausted is set. Start
// the Windsor chunker first and it can spend the pool before the single
// HubSpot search runs — the same zero-demo series, with HubSpot perfectly
// healthy, reproducible on every rebuild.
ck('HubSpot is awaited before the ad fetch starts',
   BT.indexOf('await fetchScheduledContacts')<BT.indexOf('fetchWindsorAds'),true);
ck('  and the ad fetch is no longer kicked off up front',/const _wP = fetchWindsorAds/.test(BT),false);
ck('hsSearch still bails without a call when the pool is spent',
   /if \(SUBREQ\.exhausted\) \{ lastErr = new Error\('subrequest pool exhausted'\); break; \}/.test(src),true);

// ── 6. Refusal, not merely emptiness ──────────────────────────────────────
// A genuinely quiet window is a real zero and should cache. A refused fetch
// must not. HS_ERR (added the same day, for the banner) already knows which.
ck('the HubSpot error count is sampled before the fetch',/const _hsErr0 = HS_ERR\.n;/.test(BT),true);
ck('  and a refusal is what trips the guard',
   /const _hsRefused = \(HS_ERR\.n > _hsErr0\) \|\| SUBREQ\.exhausted;/.test(BT),true);
ck('  so an empty-but-refused result is never cached',
   /if \(!Array\.isArray\(_sch\) \|\| \(!_sch\.length && _hsRefused\)\) \{/.test(BT),true);
ck('  while a real zero is cached, and says so',
   /0 contacts with no HubSpot error — caching a real zero/.test(BT),true);

// ── 7. A stale entry cannot live in KV forever ────────────────────────────
// kvCachedFetch serves an EXPIRED entry verbatim when a rebuild throws and
// never refreshes its timestamp, so with no expirationTtl a bad payload had no
// way to age out at all.
ck('kvCachedFetch bounds how long an entry can persist',
   /await env\.CONTENT_STORE\.put\(key, blob, \{ expirationTtl: Math\.max\(ttlSec \* 6, 3600\) \}\)/.test(src),true);
ck('  matching the helper that already did',
   (src.match(/expirationTtl: Math\.max\(ttlSec ?\* ?6, 3600\)/g)||[]).length,2);

// ── 2. kvCachedFetch really does serve a stale entry on a throw ───────────
// This is why the guard alone is not enough, and it is worth pinning.
const ctx={console};vm.createContext(ctx);
const store={};
vm.runInContext([
  'function kvOp(){}',
  fn(src,'kvCachedFetch'), fn(src,'kvCachedFetchNE'),
  'Object.assign(globalThis,{kvCachedFetch,kvCachedFetchNE});',
].join('\n'),ctx);
const env={CONTENT_STORE:{get:async k=>store[k]||null, put:async (k,v)=>{store[k]=v;}}};
(async()=>{
  // a good value is cached and served
  ck('a good value is cached',await ctx.kvCachedFetch(env,'k',60,async()=>[1,2,3]),[1,2,3]);
  // an EMPTY result is cached just as happily — the behaviour the guard exists
  // to prevent at the call site, since the cache layer will not do it for you
  store.k2=undefined;
  ck('kvCachedFetch caches an empty result',await ctx.kvCachedFetch(env,'k2',60,async()=>[]),[]);
  ck('  and serves it again from cache',await ctx.kvCachedFetch(env,'k2',60,async()=>[9]),[]);
  // expire it, then make the rebuild throw: the EXPIRED entry comes back
  store.k3=JSON.stringify({t:Date.now()-999999,data:['stale']});
  ck('an expired entry is served when the rebuild throws',
     await ctx.kvCachedFetch(env,'k3',60,async()=>{throw new Error('boom');}),['stale']);
  // ...which is exactly why the version bump is needed, not just the guard.

  // ── 3. kvCachedFetchNE never stores an empty array ──────────────────────
  delete store.n1;
  ck('NE does not cache an empty array',await ctx.kvCachedFetchNE(env,'n1',60,async()=>[]),[]);
  ck('  leaving nothing behind to serve',store.n1===undefined,true);
  store.n2=JSON.stringify({t:Date.now()-999999,data:['good']});
  ck('NE prefers the last good value over a fresh empty one',
     await ctx.kvCachedFetchNE(env,'n2',60,async()=>[]),['good']);
  ck('  and over a thrown fetch',
     await ctx.kvCachedFetchNE(env,'n2',60,async()=>{throw new Error('boom');}),['good']);
  ck('  but a non-empty live result still wins',
     await ctx.kvCachedFetchNE(env,'n2',60,async()=>['fresh']),['fresh']);

  // ── 4. The HubSpot list caches use the non-poisoning helper ─────────────
  // During the outage these cached [] for 15 minutes:
  //   "allTime fetched: cw (0), funnel-qualified (0)"
  for(const key of ['alltime_qualified_v2','alltime_closedwon_v1','alltime_webinar_happened_v1']){
    ck(key+' cannot cache an empty list',
       new RegExp("kvCachedFetchNE\\(env, '"+key+"'").test(src),true);
    ck('  and no longer passes a [] fallback',
       new RegExp("kvCachedFetch\\(env, '"+key+"'").test(src),false);
  }
  console.log('\n'+pass+' passed, '+fail+' failed');
  process.exit(fail?1:0);
})();
