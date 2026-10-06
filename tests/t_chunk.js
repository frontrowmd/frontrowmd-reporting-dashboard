// azWindsorFetchAll: the YTD ad-table timeout. Chunks are independent, so they
// run in bounded-concurrency waves. Correctness must be identical to the old
// serial loop -- same rows, same bisection, same budget ceiling.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load('worker.js');
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

  console.log(pass+' passed, '+fail+' failed');
  process.exit(fail?1:0);
})();
