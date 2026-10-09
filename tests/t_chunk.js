// azWindsorFetchAll: the YTD ad-table timeout. Chunks are independent, so they
// run in bounded-concurrency waves. Correctness must be identical to the old
// serial loop -- same rows, same bisection, same budget ceiling.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const AZ_PAGE_SIZE=+(src.match(/AZ_PAGE_SIZE\s*=\s*(\d+)/)||[])[1];
const BUDGET=+(src.match(/WINDSOR_CALL_BUDGET\s*=\s*(\d+)/)||[])[1];
const CONC=+(src.match(/AZ_CHUNK_CONCURRENCY\s*=\s*(\d+)/)||[])[1];
const ATT=+(src.match(/AZ_MAX_ATTEMPTS\s*=\s*(\d+)/)||[])[1];
const RES=+(src.match(/AZ_RESERVE_PER_CHUNK\s*=\s*(\d+)/)||[])[1];
const HARD=+(src.match(/SUBREQ_HARD_MAX\s*=\s*(\d+)/)||[])[1];
ck('the real constants parse',[Number.isInteger(AZ_PAGE_SIZE),Number.isInteger(BUDGET),Number.isInteger(CONC)],[true,true,true]);
ck('concurrency is greater than one (this is the whole fix)',CONC>1,true);

function mk(opts){
  const ctx={console,Date,Math,Promise,JSON,Object,Array,Number,String,setTimeout};
  vm.createContext(ctx);
  vm.runInContext([
    'const AZ_PAGE_SIZE='+AZ_PAGE_SIZE+';',
    'const WINDSOR_CALL_BUDGET='+BUDGET+';',
    'const AZ_CHUNK_CONCURRENCY='+CONC+';',
    'const SUBREQ={n:0,windsor:0,hubspot:0,kv:0,exhausted:false,windsorDropped:0,softFail:0,windsorPlanned:0};',
    'const AZ_MAX_ATTEMPTS='+ATT+';',
    'const AZ_RESERVE_PER_CHUNK='+RES+';',
    'const SUBREQ_HARD_MAX='+HARD+';',
    'function subreqSummary(){return "stub";}',
    'function fmt(d){return d.toISOString().slice(0,10);}',
    'function windsorCapTo(c,t){return t;}',
    // Stub the single network call. Records concurrency and returns synthetic rows.
    'let inflight=0,maxInflight=0,callLog=[];',
    'async function azWindsorFetch(k,c,a,b,f,ch){',
    '  const att=(globalThis.__attempts||1);',
    // Mirror the real per-attempt hard cap: it is what stops a retry storm
    // crossing the platform ceiling now that the reservation is realistic.
    '  for(let i=0;i<att;i++){',
    '    if(SUBREQ.n>=SUBREQ_HARD_MAX){SUBREQ.softFail++;SUBREQ.exhausted=true;return [];}',
    '    SUBREQ.n++;SUBREQ.windsor++;',
    '  }',
    '  callLog.push([a,b]); inflight++; if(inflight>maxInflight)maxInflight=inflight;',
    '  await new Promise(r=>setTimeout(r,1));',
    '  inflight--;',
    '  const n=(globalThis.__rowsFor)?globalThis.__rowsFor(a,b):1;',
    '  const out=[];for(let i=0;i<n;i++)out.push({d:a,i});return out;',
    '}',
    fn(src,'azWindsorFetchAll'),
    'Object.assign(globalThis,{azWindsorFetchAll,SUBREQ,getMax:()=>maxInflight,getLog:()=>callLog});',
  ].join('\n'),ctx);
  ctx.__rowsFor=opts.rowsFor||(()=>1);
  return ctx;
}

const days=(a,b)=>Math.round((new Date(b+'T00:00:00Z')-new Date(a+'T00:00:00Z'))/86400000)+1;

(async()=>{
  // --- YTD shape: 278 days at the creative grain (chunkDays=7) --------------
  {
    const c=mk({});
    const rows=await c.azWindsorFetchAll('k','facebook','2026-01-01','2026-10-05','f',7);
    const log=c.getLog();
    ck('every day in the window is covered exactly once',
       (()=>{const seen=new Set();for(const [a,b] of log){let d=a;while(d<=b){seen.add(d);const x=new Date(d+'T00:00:00Z');x.setUTCDate(x.getUTCDate()+1);d=x.toISOString().slice(0,10);}}
         return seen.size;})(),days('2026-01-01','2026-10-05'));
    ck('one row per chunk comes back',rows.length,log.length);
    ck('a year at 7-day chunks is ~40 calls',log.length,Math.ceil(days('2026-01-01','2026-10-05')/7));
    ck('and they actually overlapped',c.getMax()>1,true);
    ck('up to the configured concurrency',c.getMax()<=CONC,true);
  }
  // --- bisection on a capped chunk still works ------------------------------
  {
    // Only the first week is dense enough to hit the cap; it must split until
    // the sub-ranges come back under it.
    const c=mk({rowsFor:(a,b)=>(a<'2026-01-08'&&a!==b)?AZ_PAGE_SIZE:1});
    const rows=await c.azWindsorFetchAll('k','facebook','2026-01-01','2026-01-14','f',7);
    const log=c.getLog();
    ck('a capped chunk is split and retried',log.length>2,true);
    ck('and no capped (truncated) page is kept',rows.filter(r=>r.i>=AZ_PAGE_SIZE).length,0);
    ck('single-day chunks are kept even at the cap',
       log.some(([a,b])=>a===b),true);
  }
  // --- budget ceiling ------------------------------------------------------
  {
    const c=mk({});
    // 10 years of single-day chunks would be ~3650 calls; the budget must stop it.
    await c.azWindsorFetchAll('k','facebook','2020-01-01','2026-10-05','f',1);
    ck('the windsor budget is never exceeded',c.SUBREQ.windsor<=BUDGET,true);
    ck('and the unfetched remainder is reported',c.SUBREQ.windsorDropped>0,true);
  }
  // Every admitted chunk can cost AZ_MAX_ATTEMPTS, not one. Budgeting against
  // attempts-already-started let a retry storm walk past Cloudflare's 1000-per-
  // invocation ceiling, which surfaces as a bare 503 with no JSON body.
  {
    const c=mk({}); c.__attempts=ATT;
    await c.azWindsorFetchAll('k','facebook','2020-01-01','2026-10-05','f',1);
    ck('the reservation bounds admitted chunks',c.SUBREQ.windsorPlanned<=BUDGET,true);
    // Reserving all 3 attempts per chunk wasted two thirds of the budget and
    // dropped data; the hard cap is what actually prevents the platform 503.
    ck('and the hard cap bounds total attempts',c.SUBREQ.n<=HARD,true);
    ck('the reservation is realistic, not worst-case',RES<ATT,true);
  }
  // The real failure: ~20 chunkers admit waves synchronously before any increment
  // lands, so each saw a near-zero counter and admitted as if it were alone.
  {
    const c=mk({}); c.__attempts=ATT;
    await Promise.all(Array.from({length:20},(_,i)=>
      c.azWindsorFetchAll('k','conn'+i,'2020-01-01','2026-10-05','f',1)));
    ck('20 concurrent chunkers collectively respect one budget',c.SUBREQ.windsorPlanned<=BUDGET,true);
    ck('and stay under the platform ceiling',c.SUBREQ.n<=HARD,true);
  }
  // --- an already-exhausted pool short-circuits -----------------------------
  {
    const c=mk({}); c.SUBREQ.exhausted=true;
    const rows=await c.azWindsorFetchAll('k','facebook','2026-01-01','2026-10-05','f',7);
    ck('an exhausted pool fetches nothing',[rows.length,c.SUBREQ.windsor],[0,0]);
  }
  // --- a window that ends before it starts ---------------------------------
  {
    const c=mk({});
    ck('an inverted window returns empty',(await c.azWindsorFetchAll('k','facebook','2026-10-05','2026-01-01','f',7)).length,0);
  }
  // --- peak residency: the OOM that concurrency created ---------------------
  // Collecting the whole wave before folding held concurrency x AZ_PAGE_SIZE rows
  // at once PER CONNECTOR, and 7 connectors fetch concurrently. Folding on
  // arrival is what bounds it. Measure rows alive but not yet folded.
  {
    const c=mk({rowsFor:()=>500});
    vm.runInContext('globalThis.__alive=0;globalThis.__peak=0;',c);
    // Each response registers rows as alive; onChunk releases them.
    const orig=c.azWindsorFetchAll;
    let alive=0,peak=0;
    const sink=rows=>{ alive-=rows.length; };
    // Wrap the stub so a resolved-but-unfolded response counts as alive.
    vm.runInContext('globalThis.__wrapFetch=(f)=>f;',c);
    c.__rowsFor=()=>{ return 500; };
    const onChunk=rows=>{ alive-=rows.length; sinkCalls++; };
    let sinkCalls=0;
    // Re-implement residency accounting around the real function: the stub
    // bumps `alive` when it returns, onChunk drops it when folded.
    const ctx2=mk({});
    vm.runInContext([
      'let _alive=0,_peak=0;',
      'async function azWindsorFetch(k,c,a,b,f,ch){ SUBREQ.n++;SUBREQ.windsor++;',
      '  await new Promise(r=>setTimeout(r,Math.random()*4));',
      '  const out=[];for(let i=0;i<500;i++)out.push({i});',
      '  _alive+=out.length; if(_alive>_peak)_peak=_alive; return out; }',
      'Object.assign(globalThis,{getPeak:()=>_peak,release:n=>{_alive-=n;}});',
    ].join('\n'),ctx2);
    await ctx2.azWindsorFetchAll('k','facebook','2026-01-01','2026-03-31','f',7,
      rows=>{ ctx2.release(rows.length); });
    const CONC=+(src.match(/AZ_CHUNK_CONCURRENCY\s*=\s*(\d+)/)||[])[1];
    ck('rows are released as each chunk lands, not after the wave',
       ctx2.getPeak()<=CONC*500,true);
    ck('so peak residency is bounded by the concurrency, not the window',
       ctx2.getPeak()<=3*500,true);
  }
  // Structural: the fold must sit INSIDE the per-chunk .then, not after Promise.all.
  ck('folding happens on arrival',
     /azWindsorFetch\(apiKey, connector, a, b, fields, true\)\s*\n\s*\.then\(rows => \{[\s\S]{0,600}?onChunk\(rows\);/.test(src),true);
  ck('no wave-wide result array is collected first',/const got = await Promise\.all/.test(src),false);

  // --- one slow connector must not take the whole phase down ----------------
  // Observed: the creatives phase never completed on MTD and the client aborted
  // at 75s. LinkedIn at creative grain is the known offender (the source comment
  // records ~240s of a 379s YTD phase), and a request that never returns is also
  // one that never caches, so the next load is in the same position.
  ck('creative fetches carry a deadline',/const AZ_CRE_DEADLINE_MS = (\d+);/.test(src),true);
  // The real invariant is not "shorter than the client budget" but "leaves room
  // inside the RESPONSE deadline for the comparison periods". A creative cap
  // that eats the whole budget silently costs every delta on the page.
  {
    const cre=+(/const AZ_CRE_DEADLINE_MS = (\d+);/.exec(src)||[])[1];
    const resp=+(/const AZ_RESP_DEADLINE_MS = (\d+);/.exec(src)||[])[1];
    const cmp=+(/const AZ_CMP_MIN_MS = (\d+);/.exec(src)||[])[1];
    ck('all three deadlines parse',[cre>0,resp>0,cmp>0],[true,true,true]);
    ck('a fully capped creatives phase still leaves room for a comparison period',
       resp - cre >= cmp*2, true);
    // And the whole thing must answer inside even the TIGHT client budget, so a
    // browser running a cached older build still gets a response.
    ck('the response deadline fits inside the tight client budget',
       resp < 75000, true);
  }
  // EVERY creative fetch is wrapped, not just the one known to be slow — the next
  // pathological connector should degrade the same way without a code change.
  // Scoped to fetchAzCreatives: other fetch phases have their own `promises`
  // array and their own cost profile, and counting file-wide made this compare
  // creative fetches against unrelated ones.
  {
    const CRE=fn(src,'fetchAzCreatives');
    ck('every creative fetch is wrapped',
       (CRE.match(/promises\.push\(_creCapped\(/g)||[]).length,
       (CRE.match(/promises\.push\(/g)||[]).length);
  }
  ck('and there are the three we know about',
     (src.match(/promises\.push\(_creCapped\(/g)||[]).length>=3,true);
  // Capping is only safe because rows fold on arrival: a capped channel keeps
  // what arrived instead of losing the lot.
  ck('the capped fetches fold on arrival',
     (src.match(/AZ_CREATIVE_CHUNK_DAYS, rows => \{ for \(const _c of _seenConn/g)||[]).length>=3,true);
  ck('a cap is a REPORTED shortfall',/CAPPED at \$\{AZ_CRE_DEADLINE_MS\}ms/.test(src),true);

  // --- the endpoint must ALWAYS answer -------------------------------------
  // A request that runs past the browser's abort returns nothing AND caches
  // nothing, so the next load repeats it. Past the fetch phase the only work
  // left is the comparison periods, which exist purely to put deltas on cards
  // that are already built — so they are skipped rather than waited for.
  ck('there is a response deadline',/const AZ_RESP_DEADLINE_MS = \d+;/.test(src),true);
  ck('both comparison periods check it',
     (src.match(/_azLeft\(\) < AZ_CMP_MIN_MS/g)||[]).length,2);
  ck('and a skip is reported, not silent',
     (src.match(/SKIPPED at \$\{Date\.now\(\)-_azT0\}ms/g)||[]).length,2);
  ck('the summary names what was dropped',
     /comparison period\(s\) skipped for the deadline/.test(src),true);
  // Dropping them is a KNOWN-GOOD shape: All Time already runs with both null,
  // so every consumer downstream handles it.
  ck('null comparison periods are an existing supported shape',
     /if \(prior && !isAllTime/.test(src)&&/if \(priorMonth && !isAllTime/.test(src),true);
  ck('and they still reach buildAzResponse',
     /buildAzResponse\(current, prior, priorMonth, windsor, creatives, priorW, pmW, attr, priorA, pmA/.test(src),true);

  // --- unread response bodies deadlock the worker --------------------------
  // Live log, many times: "A stalled HTTP response was canceled to prevent
  // deadlock ... because the Worker did not read the responses, they would
  // never complete." Cloudflare caps concurrent in-flight fetches and an unread
  // body never frees its slot, so the runtime starts cancelling OTHER
  // connectors' healthy requests. Windsor answers 400 for LinkedIn on every
  // run, so the leak was continuous.
  ck('there is a drain helper',/function azDrain\(r\) \{/.test(src),true);
  ck('it cancels only an unread body',/if \(r && r\.body && !r\.bodyUsed\) r\.body\.cancel\(\)/.test(src),true);
  ck('and never throws on a body already gone',/catch \(_\) \{ \/\* already gone \*\/ \}/.test(src),true);
  // EVERY early exit off a Response drains first. Counted as a ratio so a new
  // error path that forgets fails this rather than slipping in under a floor.
  // The rule is DRAIN OR READ: a site that does `await r.text()` has already
  // consumed the body and freed the slot, so it needs no drain. What must never
  // exist is a site that does neither.
  {
    // The window reaches BACKWARDS too: a site can legitimately read the body on
    // the line above (`const data = await r.json();` then `if (!r.ok) return ...`),
    // which consumes it just as well.
    const RE=/[\s\S]{0,140}if \(!r\.ok\)[\s\S]{0,220}?(?:continue;|break;|return[^\n]*;|throw[^\n]*;)/g;
    const sites=src.match(RE)||[];
    const leaky=sites.filter(x=>!/azDrain\(r\)/.test(x)&&!/await r\.text\(\)/.test(x)&&!/await r\.json\(\)/.test(x));
    ck('the scan found the error paths',sites.length>=8,true);
    ck('none of them leaves a body unread',leaky.length,0);
  }
  ck('both Windsor fetchers drain before throwing',
     (src.match(/azDrain\(r\); throw new Error\(`Windsor/g)||[]).length,2);

  // --- a capped fetch must not read as a failed one -------------------------
  // softFail is the CACHE GATE. Six channels capping at once pushed it to 7
  // against a tolerance of 5, so the cron built the whole payload and then threw
  // it away: "cron warm ytd: NOT caching — soft failures: 7". The cap added to
  // bound a slow fetch was what stopped anything ever being warmed.
  ck('a cap has its own counter',/SUBREQ\.creCapped\+\+;/.test(src),true);
  ck('and does NOT touch softFail',
     /SUBREQ\.softFail\+\+;\s*\n\s*console\.warn\(`Creative fetch/.test(src),false);
  ck('the counter is declared and reset',
     [/creCapped: 0/.test(src),/SUBREQ\.creCapped = 0;/.test(src)],[true,true]);
  ck('and reported in the summary',/creative fetches capped: \$\{SUBREQ\.creCapped\}/.test(src),true);
  // The gate must read softFail, not the cap — otherwise this regresses.
  ck('the cache gate does not count caps',
     /_azOk = [^;]*creCapped/.test(src),false);

  console.log(pass+' passed, '+fail+' failed');
  process.exit(fail?1:0);
})();
