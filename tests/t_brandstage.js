// Brand Conversion Funnel, Sales-pipeline deal stages (2026-10-29). A COHORT --
// deals created in the window (Eastern), pre-launch excluded -- with Reached
// from HubSpot's stage stamps + current stage + only the definitional
// implications. RUN, not grepped: what matters is which deal counts where.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{buildBrandStageFunnel,brandReachedStages,BST,BRAND_STAGE_PROPS,BRAND_FUNNEL_PATH,BRAND_FUNNEL_EXITS});',W);
const S=W.BST;
let n=0;
const D=(stage,stamps,extra)=>{const p=Object.assign({dealstage:stage,createdate:'2026-10-10T15:00:00Z'},extra||{});
  for(const id of (stamps||[]))p['hs_v2_date_entered_'+id]='2026-10-11T00:00:00Z';return {id:String(++n),properties:p};};
const reached=p=>[...W.brandReachedStages(p)].sort();

// ── 1. Reached: stamps + current + definitional implications only ───────────
// The refusal guard, run: a page HubSpot refused must not come back as data.
(async()=>{
  const G={console:{log(){},warn(){},error(){}},fetch:async()=>({ok:false,status:500,text:async()=>'boom',json:async()=>({})}),setTimeout:(f)=>f()};
  vm.createContext(G);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nObject.assign(globalThis,{fetchBrandStageDeals,SUBREQ});',G);
  let threw=false;try{await G.fetchBrandStageDeals('tok','2026-10-01','2026-10-31');}catch(e){threw=true;}
  ck('a fetch HubSpot refused throws (so the card says it did not load)',threw,true);
  let threw2=false;try{await G.fetchBrandStageDeals('','2026-10-01','2026-10-31');}catch(e){threw2=true;}
  ck('  and so does a missing token',threw2,true);
  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
ck('a stamp counts, and so does the current stage',reached({dealstage:S.cs,hs_v2_date_entered_1084214349:'x'}),[S.demo,S.appt,S.cs].sort());
ck('Demo Happened implies Appointment Scheduled',reached({dealstage:S.demo}),[S.appt,S.demo].sort());
ck('Webinar Happened implies Webinar Registered',reached({dealstage:S.webHap}),[S.webHap,S.webReg].sort());
ck('Churned implies Closed Won',reached({dealstage:S.churned}),[S.churned,S.won].sort());
ck('the optional middle stages are NOT filled in',reached({dealstage:S.won}),[S.won]);
ck('a deal created straight into Closed Won reached nothing before it',
   reached({dealstage:S.won,hs_v2_date_entered_closedwon:'x'}),[S.won]);

// ── 2. The cohort ───────────────────────────────────────────────────────────
{
  const deals=[
    D(S.won,[S.appt,S.demo,S.dm,S.cs,S.won]),           // full path
    D(S.won,[S.appt,S.demo,S.won]),                      // skipped DM + CS
    D(S.cs,[S.webReg,S.webHap,S.cs]),                    // webinar, skipped DM
    D(S.won,[S.won]),                                    // created straight into won
    D(S.demo,[S.appt,S.demo]),
    D(S.appt,[S.appt]),
    D(S.noShow,[S.appt,S.noShow]),
    D(S.lost,[S.appt,S.demo,S.lost]),
    D(S.webReg,[S.webReg]),
    D(S.appt,[S.appt],{average_monthly_web_traffic__cloned_:'Pre-launch / just launching'}),  // excluded
    D(S.appt,[S.appt],{createdate:'2026-09-30T23:30:00Z'}),  // Sept 30 7:30pm ET: outside Oct
    D(S.appt,[S.appt],{createdate:'2026-11-01T03:30:00Z'}),  // Oct 31 11:30pm ET: inside
  ];
  deals.push(deals[0]);                                  // a repeat counts once
  const f=W.buildBrandStageFunnel(deals,'2026-10-01','2026-10-31');
  const g=id=>f.stages[id];
  ck('the cohort: created in the window in Eastern time, pre-launch out, deduped',f.total,10);
  ck('Reached per stage',W.BRAND_FUNNEL_PATH.map(id=>g(id).reached),[7,2,4,1,1,2,3]);   // appt,webReg,demo,webHap,dm,cs,won
  ck('Here now per stage',W.BRAND_FUNNEL_PATH.map(id=>g(id).here),[2,1,1,0,0,1,3]);
  ck('Demo Happened from Appointment Scheduled is a plain cohort share',[g(S.demo).fromPrevNum,g(S.demo).fromPrevDen],[4,7]);
  ck('Webinar Happened from Webinar Registered',[g(S.webHap).fromPrevNum,g(S.webHap).fromPrevDen],[1,2]);
  // Strict, as on the Clinician Funnel: of the deals that reached the stage
  // above, the share that ALSO reached this one. Held = Demo or Webinar Happened (5).
  ck('Decision Maker = of the 5 that held a demo or webinar, reached DM',[g(S.dm).fromPrevNum,g(S.dm).fromPrevDen],[1,5]);
  ck('Contract Sent = of the deals that reached DM, reached CS',[g(S.cs).fromPrevNum,g(S.cs).fromPrevDen],[1,1]);
  ck('Closed Won = of the deals that reached CS, won',[g(S.won).fromPrevNum,g(S.won).fromPrevDen],[1,2]);
  ck('so a rate is never over 100%, and its numerator is inside its own Reached',
     W.BRAND_FUNNEL_PATH.every(id=>(g(id).fromPrevPct==null||g(id).fromPrevPct<=100)&&(g(id).fromPrevNum==null||g(id).fromPrevNum<=g(id).reached)),true);
  {
    // The Oct 2026 shape that broke the "at least as far as" rule: nobody at
    // Contract Sent, two won after a demo. Contract Sent must not show a rate
    // built from deals it never had.
    const f2=W.buildBrandStageFunnel([D(S.won,[S.appt,S.demo,S.won]),D(S.won,[S.appt,S.demo,S.won]),D(S.dm,[S.appt,S.demo,S.dm])],'2026-10-01','2026-10-31');
    ck('a stage nobody reached shows 0 of its base, and the next row has no base at all',
       [f2.stages[S.cs].reached,f2.stages[S.cs].fromPrevPct,f2.stages[S.won].fromPrevPct],[0,0,null]);
  }
  ck('the first rows carry no rate (the card divides Appointment Scheduled by Demo clicks)',
     [g(S.appt).fromPrevPct,g(S.webReg).fromPrevPct],[null,null]);
  ck('the skip counts the card explains a larger row with',
     [g(S.won).noDemo,g(S.won).direct,g(S.won).skippedPrev,g(S.cs).skippedPrev,g(S.dm).skippedPrev],[1,1,1,1,null]);
  {
    // No demo on record is not the same as created straight into Closed Won.
    const f3=W.buildBrandStageFunnel([D(S.won,[S.won]),D(S.won,[S.appt,S.dm,S.won]),D(S.churned,[S.churned]),D(S.won,[S.dm,S.won])],'2026-10-01','2026-10-31');
    ck('  "straight into Closed Won" counts only deals with nothing else on record',[f3.stages[S.won].noDemo,f3.stages[S.won].direct],[4,2]);
  }
  ck('off-path stages: reached and here now',W.BRAND_FUNNEL_EXITS.map(id=>[f.exits[id].reached,f.exits[id].here]),
     [[0,0],[1,1],[0,0],[1,1],[0,0]]);   // interested, noShow, notFit, lost, churned
  {
    const f4=W.buildBrandStageFunnel([D(S.noShow,[S.webReg,S.noShow]),D(S.noShow,[S.webReg,S.noShow]),D(S.noShow,[S.appt,S.noShow]),D(S.noShow,[S.webReg,S.appt,S.noShow])],'2026-10-01','2026-10-31');
    // A deal that registered AND booked a demo no-showed the demo, not the webinar route.
    ck('No Show says how many came through a webinar only',[f4.exits[S.noShow].reached,f4.exits[S.noShow].viaWebinar],[4,2]);
  }
  ck('a window with no deals is zeros, and no rate',
     (()=>{const e=W.buildBrandStageFunnel([],'2026-10-01','2026-10-31');return [e.total,e.stages[S.demo].fromPrevPct];})(),[0,null]);
  ck('no window (All Time has no prior) is null',W.buildBrandStageFunnel(deals,null,null),null);
}

// ── 3. The fetch and the wiring ────────────────────────────────────────────
{
  const body=fn(src,'fetchBrandStageDeals');
  ck('one search: the Sales pipeline, by createdate in Eastern time',
     [/propertyName: 'pipeline', operator: 'EQ', value: BRAND_PIPELINE_ID/.test(body),/value: String\(toMsET\(from\)\)/.test(body),
      /value: String\(toMsET\(to, true\)\)/.test(body),/const BRAND_PIPELINE_ID = 'default';/.test(src)],[true,true,true,true]);
  ck('  asking for every stage stamp it reads, and the tier',
     [...W.BRAND_FUNNEL_PATH,...W.BRAND_FUNNEL_EXITS].every(id=>W.BRAND_STAGE_PROPS.includes('hs_v2_date_entered_'+id))
       &&W.BRAND_STAGE_PROPS.includes('dealstage')&&W.BRAND_STAGE_PROPS.includes('average_monthly_web_traffic__cloned_'),true);
  ck('  with room for All Time (50 pages)',/\[\{ propertyName: 'createdate', direction: 'DESCENDING' \}\], 50\);/.test(body),true);
  ck('the request fetches once over the span of all three windows, both ends',
     /const _bsFrom = \[current\.from, prior && prior\.from, priorMonth && priorMonth\.from\]\.filter\(Boolean\)\.sort\(\)\[0\];/.test(src)
     &&/const _bsTo = \[current\.to, prior && prior\.to, priorMonth && priorMonth\.to\]\.filter\(Boolean\)\.sort\(\)\.pop\(\);/.test(src),true);
  ck('  a refused HubSpot page throws rather than passing a partial cohort as real',
     /if \(HS_ERR\.n > _err0 \|\| SUBREQ\.exhausted\) throw new Error/.test(body),true);
  ck('  and an empty long span is treated as refused, not as zero deals',
     /if \(_bsSpan > 92 && !_bsDeals\.length\) throw new Error/.test(src),true);
  ck('  KV-caching only long spans',/_bsSpan > 92\s*\? await kvCachedFetchNE\(env, `brand_stage_deals_v1_\$\{_bsFrom\}_\$\{_bsTo\}`, 1800,/.test(src),true);
  ck('  and ships cur / prior / lm on the response',
     /cur: buildBrandStageFunnel\(_bsDeals, current\.from, current\.to\),\s*prior: prior \? buildBrandStageFunnel\(_bsDeals, prior\.from, prior\.to\) : null,\s*lm: priorMonth \? buildBrandStageFunnel\(_bsDeals, priorMonth\.from, priorMonth\.to\) : null,/.test(src)
     &&/resp\.brandStageFunnel = _bsf;/.test(src),true);
  ck('  a failure degrades to no deal rows, not a 500',/\} catch \(e\) \{ console\.error\('brand stage funnel failed:'/.test(src),true);
  ck('the payload change bumped the cache version',/const API_CACHE_VER = 'v49';/.test(src),true);
}
// The two copies of the stage ids must agree.
{
  const cli=load(process.env.DASH_SRC||'dashboard.html');
  const C={};vm.createContext(C);
  vm.runInContext(require('./slice.js').decl(cli,'const BRAND_STAGE_IDS=','};')+';globalThis.X=BRAND_STAGE_IDS;',C);
  ck('the dashboard’s stage ids match the worker’s',JSON.stringify(C.X),JSON.stringify(W.BST));
}

