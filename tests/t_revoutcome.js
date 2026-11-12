// Revenue Outcome card, 2026-10-28: ROAS removed, and an "Include Pilot Brands"
// checkbox. A pilot brand is a signed deal with Pilot Waitlist Status filled in;
// unticked (the default), every figure on the card leaves them out. RUN, not
// grepped: what matters is which dollars end up on the card.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const J=x=>JSON.parse(JSON.stringify(x));
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. The worker's pilot split ────────────────────────────────────────────
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{isPilotBrand,processClosedWonDeals,filterActiveBrands});',W);
const D=(amt,pilot,bs)=>({properties:{amount:String(amt),pilot_waitlist_status:pilot||'',brand_status:bs||'',closedate:'2026-10-27T15:00:00Z'}});
ck('a deal with any Pilot Waitlist Status is a pilot',
   ['Outreach Sent / No Response','Onboarding Link Requested','Needs More Time','Not Interested'].map(v=>W.isPilotBrand(D(1,v))),
   [true,true,true,true]);
ck('  and one with it blank is not',[W.isPilotBrand(D(1,'')),W.isPilotBrand(D(1,'  ')),W.isPilotBrand({})],[false,false,false]);
{
  const deals=[D(2000),D(3000),D(1500,'Onboarding Link Requested'),D(2500,'Outreach Sent / No Response')];
  const r=W.processClosedWonDeals(deals);
  ck('the cached totals carry the pilot share alongside, not instead',
     [r.mrr,r.count,r.pilotMrr,r.pilotCount],[9000,4,4000,2]);
}
ck('the closed-won fetch asks HubSpot for the pilot field',
   /fetchClosedWonDeals[\s\S]{0,1000}?'pilot_waitlist_status', 'dealname'\]/.test(src),true);
{
  // The live endpoint's per-window totals, sliced out and run on stub deals.
  const body=fn(src,'processRevenueOutcome');
  const tot=/const tot = \(deals\) => \{[\s\S]*?\n  \};/.exec(body)[0];
  const T={isPilotBrand:W.isPilotBrand,filterActiveBrands:W.filterActiveBrands,parseFloat};vm.createContext(T);
  vm.runInContext(tot+';globalThis.tot=tot;',T);
  ck('the live totals do too, over the same active-brand filter',
     T.tot([D(2000),D(1500,'Needs More Time'),D(9999,'Not Interested','churned')]),
     {mrr:3500,count:2,pilotMrr:1500,pilotCount:1});
}

// ── 1b. App-based pilot deals (2026-11-12) ─────────────────────────────────
{
  const fs=require('fs');
  const W3={console:{log(){},warn(){},error(){}},BD_HTML:fs.readFileSync(require('path').join(__dirname,'..','bd-dashboard.html'),'utf8')};vm.createContext(W3);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nObject.assign(globalThis,{markAppPilots,isPilotBrand,appPilotNorm,appJoinTables});',W3);
  const ok=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>''});
  // App: pilot brands 10 (company c10), 11 (no company, name "Glow Co"), 1948 (HEVA, joined by the hand table).
  const mb={status:'completed',data:{cols:[{name:'ID'},{name:'HC'},{name:'NM'}],rows:[[10,'c10','Alpha'],[11,null,'Glow Co'],[1948,null,'HEVA'],[99,null,'Vincent']]}};
  const assoc={results:[{from:{id:'1'},to:[{toObjectId:'c10'}]},{from:{id:'2'},to:[]},{from:{id:'3'},to:[{toObjectId:'c99'}]},{from:{id:'4'},to:[{toObjectId:'338391695050'}]},{from:{id:'5'},to:[]}]};
  W3.fetch=async(u)=>/associations/.test(u)?ok(assoc):ok(mb);
  const D=(id,name,field)=>({id:String(id),properties:{dealname:name,amount:'1000',pilot_waitlist_status:field||''}});
  const ds=[D(1,'Alpha deal'),D(2,'Glow - New Deal'),D(3,'Regular Brand','Not Interested'),D(4,'My HEVA LLC'),D(5,'Vincent')];
  globalThis.__appPilotTests=(async()=>{
    const r=await W3.markAppPilots({METABASE_API_KEY:'k'},'tok',[ds]);
    ck('app rule: by associated company, by deal name, and by the BD hand-matched join',
       [r.ok,ds.map(d=>W3.isPilotBrand(d))],[true,[true,true,false,true,false]]);
    ck('  a HubSpot Pilot Waitlist Status no longer makes a non-pilot a pilot; test brands never count',
       [W3.isPilotBrand(ds[2]),W3.isPilotBrand(ds[4])],[false,false]);
    W3.fetch=async()=>({ok:false,status:500,text:async()=>'boom',json:async()=>({})});
    const ds2=[D(6,'X','Needs More Time'),D(7,'Y')];
    const r2=await W3.markAppPilots({METABASE_API_KEY:'k'},'tok',[ds2]);
    ck('  if the app or HubSpot cannot be read, the deals fall back to the HubSpot field',[r2.ok,ds2.map(d=>W3.isPilotBrand(d))],[false,[true,false]]);
    ck('the hand-matched joins are read from the bundled BD page (one copy)',[Object.keys(W3.appJoinTables().co).length>10,W3.appJoinTables().deal['322993300178']],[true,['1335','1336','1337','1338']]);
  })();
}

// ── 2. The card: two sides (2026-11-12) — brands excl. pilots | pilots ──────
const mkCtx=()=>{
  const C={console,Math,JSON,Number,String,Object,Array,document:{getElementById:()=>null}};
  vm.createContext(C);
  vm.runInContext([
    'var AP="dashboard";var S={dashboard:{win:"mtd",data:null},irfan:{},exec:{}};',
    "const f$=v=>v==null?'—':'$'+Math.round(v).toLocaleString();",
    "const fN=v=>v==null?'—':Math.round(v).toLocaleString();",
    'function _execMode(){return "brand";}',
    'var _revOutcome=null,_roFail=null;',
    fn(cli,'_roKey'),fn(cli,'_roPageKey'),
    fn(cli,'_buildRevenueOutcomeCard'),
    'Object.assign(globalThis,{_buildRevenueOutcomeCard,_roKey,S,setRO:v=>{_revOutcome=v;},setFail:v=>{_roFail=v;}});',
  ].join('\n'),C);
  return C;
};
const text=h=>h.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ');
const PIL={source:'live',current:{n:4,fees:16000,post:12000},prior:{n:2,fees:8000,post:6000},priorMonth:{n:5,fees:20000,post:15000}};
const LIVE={win:'mtd',key:'mtd',
  current:{mrr:30000,count:12,pilotMrr:6000,pilotCount:3},
  prior:{mrr:20000,count:9,pilotMrr:0,pilotCount:0},
  priorMonth:{mrr:25000,count:10,pilotMrr:2500,pilotCount:1},pilots:PIL};
const d0={signedChurnedByCloseCount:2};
{
  const C=mkCtx();C.setRO(LIVE);
  const h=C._buildRevenueOutcomeCard(d0), t=text(h);
  ck('ROAS and the pilot toggle are gone',[/ROAS/.test(h),/roSetPilotMode|aria-pressed/.test(h)],[false,false]);
  // Side 1 excludes pilots: 30,000 - 6,000 = 24,000 MRR; 12 - 3 + 2 churned add-back = 11 brands.
  ck('side 1: Signed ARR, MRR, ACV, Signed Brands, all excluding pilots',
     [/Brands excluding pilots/.test(t),/\$288,000/.test(t),/MRR \$24,000/.test(t),/Signed Brands 11/.test(t),/ACV \$2,182/.test(t)],[true,true,true,true,true]);
  ck('  compared against windows without THEIR pilots',[/vs LM[^v]*\+\$18,000/.test(t)],[true]);
  ck('side 2: Initiation Fees, Post-Pilot MRR, Pilot Avg ACV, Signed Pilot Brands',
     [/Initiation Fees net of refunds \$16,000/.test(t),/Post-Pilot MRR \$12,000/.test(t),/Pilot Avg ACV \$3,000/.test(t),/Signed Pilot Brands 4/.test(t)],[true,true,true,true]);
  ck('side 2 leads with Post-Pilot ARR (post x 12), Initiation Fees beside it on the same row',
     [/Post-Pilot ARR MRR × 12 \$144,000/.test(t),t.indexOf('Post-Pilot ARR')<t.indexOf('Initiation Fees')&&t.indexOf('Initiation Fees')<t.indexOf('Post-Pilot MRR')],[true,true]);
  ck('  with their own prior and LM deltas',[/Initiation Fees[^$]*\$16,000 vs prior \S+ \+\$8,000/.test(t),/vs LM \S+ -\$4,000/.test(t)],[true,true]);
  ck('  and the order of the sides: brands first',t.indexOf('Signed ARR')<t.indexOf('Initiation Fees'),true);
  ck('  labelled live when the app read was live',/fee paid in the window · app, live/.test(t),true);
}
{
  const C=mkCtx();C.setRO(Object.assign({},LIVE,{pilots:Object.assign({},PIL,{source:'nightly'})}));
  ck('  and as of last night when it was not',/app, as of last night/.test(text(C._buildRevenueOutcomeCard(d0))),true);
  C.setRO(Object.assign({},LIVE,{pilots:{error:'Pilot figures unavailable'}}));
  const t=text(C._buildRevenueOutcomeCard(d0));
  ck('a pilot failure leaves side 1 standing and says so on side 2',[/\$288,000/.test(t),/Pilot figures unavailable/.test(t)],[true,true]);
}
{ // Before the live numbers land: the cached /api/data figures; side 2 waits.
  const C=mkCtx();
  const d={signedChurnedByCloseCount:0,mrrArr:{
    mrr:{value:30000,sameTimePrior:{value:20000},lastMonth:{value:25000}},
    dealCount:{value:12,sameTimePrior:{value:9},lastMonth:{value:10}},
    pilot:{cur:{mrr:6000,count:3},prior:{mrr:0,count:0},lm:{mrr:2500,count:1}}}};
  const t=text(C._buildRevenueOutcomeCard(d));
  ck('first paint: side 1 already excludes pilots, side 2 says it is loading',[/\$288,000/.test(t),/Loading pilot figures/.test(t)],[true,true]);
}
{ // An older payload with no split: side 1 can only show the total, and says so.
  const C=mkCtx();
  const t=text(C._buildRevenueOutcomeCard({signedChurnedByCloseCount:0,mrrArr:{mrr:{value:30000},dealCount:{value:12}}}));
  ck('with no pilot split side 1 shows the total and says why',[/\$360,000/.test(t),/pilot split loading/.test(t)],[true,true]);
}
{ // Review fixes (2026-11-12)
  const C=mkCtx();
  ck('a failed live fetch says so on the Pilots side instead of loading forever',
     (C.setFail('mtd'),/Pilot figures unavailable/.test(text(C._buildRevenueOutcomeCard(d0)))),true);
  const C2=mkCtx();C2.S.dashboard.win='custom';C2.S.dashboard.cFrom='2026-09-01';C2.S.dashboard.cTo='2026-09-30';
  C2.setRO(Object.assign({},LIVE,{win:'custom',key:C2._roKey('custom','2026-08-01','2026-08-31')}));
  ck('a custom range never shows another custom range\u2019s live figures',/Loading pilot figures/.test(text(C2._buildRevenueOutcomeCard(d0))),true);
  C2.setRO(Object.assign({},LIVE,{win:'custom',key:C2._roKey('custom','2026-09-01','2026-09-30')}));
  ck('  and does show its own',/Signed Pilot Brands 4/.test(text(C2._buildRevenueOutcomeCard(d0))),true);
}
ck('the card asks for its own live figures once per window, whatever page drew it',
   /function _brandKeyMetricsAfter\(d,sfx\)\{[\s\S]*?_roEnsure\(\);\s*\}/.test(cli)&&/if\(\(_revOutcome&&_revOutcome\.key===k\)\|\|_roAsked===k\)return;/.test(cli),true);
{ // A refused HubSpot read (worker sends current:null, hsError) never puts $0 on side 1.
  const C=mkCtx();
  C.setRO(Object.assign({},LIVE,{current:null,prior:null,priorMonth:null,hsError:true}));
  const d={signedChurnedByCloseCount:0,mrrArr:{mrr:{value:30000},dealCount:{value:12},pilot:{cur:{mrr:6000,count:3},prior:{mrr:0,count:0},lm:{mrr:0,count:0}}}};
  const t=text(C._buildRevenueOutcomeCard(d));
  ck('a failed HubSpot read keeps the cached brand side and the live pilot side',[/\$288,000/.test(t),/Signed Pilot Brands 4/.test(t)],[true,true]);
}
ck('the worker drops the brand side when HubSpot refused a read during it',
   /const hsFailed = HS_ERR\.n > _hsErr0;/.test(src)&&/current: hsFailed \? null : tot\(cCW\)/.test(src),true);
ck('the Executive page Brand mode fetches the live card too',/\|\|\(AP==='exec'&&_execMode\(\)==='brand'\)\)&&typeof fdRevenueOutcome==='function'/.test(cli),true);
ck('the live fetch keeps the pilot side and re-renders through one function',
   /_revOutcome=\{win:win,key:key,current:d\.current,prior:d\.prior,priorMonth:d\.priorMonth,pilots:d\.pilots\|\|null\};\s*_roRerender\(\);/.test(cli),true);
// The worker's windowed pilot query.
{
  const W2={console:{log(){},warn(){},error(){}}};vm.createContext(W2);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nObject.assign(globalThis,{roPilotSql,roPilotOf});',W2);
  const wins={current:{from:'2026-11-01',to:'2026-11-12'},prior:null,priorMonth:{from:'2026-10-01',to:'2026-10-31'}};
  const pg=W2.roPilotSql(wins,true),sf=W2.roPilotSql(wins,false);
  ck('pilot SQL: one column set per window present, Eastern fee-paid day',[/"current_n"/.test(pg),/"prior_n"/.test(pg),/"priorMonth_fee"/.test(pg),/AT TIME ZONE 'America\/New_York'\)::date BETWEEN '2026-11-01' AND '2026-11-12'/.test(pg)],[true,false,true,true]);
  ck('  demo/test brands left out on both paths, distinct brands counted',[/brand_name !~\* '\(demo\|test\)'/.test(pg),/REGEXP_LIKE\(v\.BRAND_NAME/.test(sf),/count\(DISTINCT v\.health_brand_id\)/.test(pg)],[true,true,true]);
  ck('  a window whose dates are not YYYY-MM-DD never reaches the SQL',W2.roPilotSql({current:{from:"2026-11-01' OR 1=1 --",to:'2026-11-12'}},true),null);
  ck('the row maps to dollars per window',J(W2.roPilotOf({current_n:'3',current_fee:'150000',current_post:'90000'},{current:{},prior:null})),{current:{n:3,fees:1500,post:900},prior:null});
}

// ── 3. Executive Dashboard: the Brand goal band is off, the Clinician one stays ─
ck('the Brand goal is switched off on the Executive Dashboard',/const EXEC_SHOW_BRAND_GOAL=false;/.test(cli),true);
ck('  and only for the Brand mode there',
   /const _goal=\(!_cln&&!EXEC_SHOW_BRAND_GOAL\)\?'':_goalTrackerHTML\(\{/.test(cli),true);
ck('  while the Brand Dashboard keeps its own',
   /const _goal=_goalTrackerHTML\(\{cur:_gm\?_gm\.regular:null,goal:GOAL_BRAND_REGULAR_DEMOS,/.test(cli),true);

Promise.resolve(globalThis.__appPilotTests).then(()=>{
console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);});
