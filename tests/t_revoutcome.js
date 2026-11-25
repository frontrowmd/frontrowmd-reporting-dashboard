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
    'var _revOutcome=null,_roFail=null,_sbCard=null,_sbFail=null;',
    fn(cli,'_roKey'),fn(cli,'_roPageKey'),
    fn(cli,'_buildRevenueOutcomeCard'),
    'Object.assign(globalThis,{_buildRevenueOutcomeCard,_roKey,S,setRO:v=>{_revOutcome=v;},setFail:v=>{_roFail=v;},setSB:v=>{_sbCard=v;},setSBFail:v=>{_sbFail=v;}});',
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
// The BD Tracker card's counts for the window (/api/signed-brands, 2026-11-24).
const SB={key:'mtd',current:{active:11,pilots:4,signed:15,churned:1},prior:{active:9,pilots:2,signed:11},priorMonth:{active:10,pilots:5,signed:15}};
const mkCtxSB=()=>{const C=mkCtx();C.setSB(SB);return C;};
{
  const C=mkCtxSB();C.setRO(LIVE);
  const h=C._buildRevenueOutcomeCard(d0), t=text(h);
  ck('ROAS and the pilot toggle are gone',[/ROAS/.test(h),/roSetPilotMode|aria-pressed/.test(h)],[false,false]);
  // Side 1 excludes pilots: 30,000 - 6,000 = 24,000 MRR; ACV over 12 - 3 + 2 churned add-back = 11 deals.
  // Signed Brands is the BD Tracker card's Active Brands (SB.current.active = 11).
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
{ // Combined total and the Recurly fee source (2026-11-17)
  const C=mkCtxSB();C.setRO(LIVE);
  const t=text(C._buildRevenueOutcomeCard(d0));
  ck('a Brands + Pilots total sits after both sides',[t.indexOf('Total Signed ARR')>t.indexOf('Signed Pilot Brands'),/Total Signed ARR \$432,000 prior \S+ \+\$120,000 \S+ LM \S+ -\$18,000/.test(t)],[true,true]);
  ck('  Total MRR adds the pilot side; Total Signed Brands is the BD Tracker card (active + pilots)',[/Total MRR \$36,000/.test(t),/Total Signed Brands 15 prior \S+ \+4/.test(t)],[true,true]);
  const C3=mkCtx();C3.setRO(LIVE);const t3=text(C3._buildRevenueOutcomeCard(d0));
  ck('  the three counts say loading until the card\u2019s figures land, never a HubSpot deal count',[/Signed Brands loading…/.test(t3),/Signed Pilot Brands loading…/.test(t3),/Total Signed Brands loading…/.test(t3),/ACV \$2,182/.test(t3)],[true,true,true,true]);
  C3.setSBFail('mtd');const t4=text(C3._buildRevenueOutcomeCard(d0));
  ck('  a failed card read is a dash and says so',[/Total Signed Brands —/.test(t4),/unavailable just now/.test(t4)],[true,true]);
  C3.setSBFail(null);C3.setSB(Object.assign({},SB,{key:'lastMonth'}));
  ck('  another window\u2019s card figures are never shown',/Total Signed Brands loading…/.test(text(C3._buildRevenueOutcomeCard(d0))),true);
  C3.setSB(Object.assign({},SB,{warn:'Recurly could not be read'}));
  ck('  a flagged card read says why',/Recurly could not be read/.test(text(C3._buildRevenueOutcomeCard(d0))),true);
  const C2=mkCtx();C2.setRO(Object.assign({},LIVE,{pilots:Object.assign({},PIL,{feesSource:'recurly',current:Object.assign({},PIL.current,{feesFallback:2})})}));
  const t2=text(C2._buildRevenueOutcomeCard(d0));
  ck('  fees are labelled Recurly net add-ons, with the app fallbacks counted',[/fees from Recurly/.test(t2),/Initiation Fees net add-ons · 2 from app/.test(t2)],[true,true]);
  C2.setRO(Object.assign({},LIVE,{pilots:{error:'x'}}));
  ck('  no total while the pilot side is missing',/Total Signed ARR/.test(text(C2._buildRevenueOutcomeCard(d0))),false);
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
  C2.setSB(Object.assign({},SB,{key:C2._roKey('custom','2026-09-01','2026-09-30')}));
  ck('  and does show its own',/Signed Pilot Brands 4/.test(text(C2._buildRevenueOutcomeCard(d0))),true);
}
ck('the card asks for its own live figures once per window, whatever page drew it',
   /function _brandKeyMetricsAfter\(d,sfx\)\{[\s\S]*?_roEnsure\(\);\s*\}/.test(cli)&&/if\(\(_revOutcome&&_revOutcome\.key===k\)\|\|_roAsked===k\)return;/.test(cli),true);
{ // A refused HubSpot read (worker sends current:null, hsError) never puts $0 on side 1.
  const C=mkCtxSB();
  C.setRO(Object.assign({},LIVE,{current:null,prior:null,priorMonth:null,hsError:true}));
  const d={signedChurnedByCloseCount:0,mrrArr:{mrr:{value:30000},dealCount:{value:12},pilot:{cur:{mrr:6000,count:3},prior:{mrr:0,count:0},lm:{mrr:0,count:0}}}};
  const t=text(C._buildRevenueOutcomeCard(d));
  ck('a failed HubSpot read keeps the cached brand side and the live pilot side',[/\$288,000/.test(t),/Signed Pilot Brands 4/.test(t)],[true,true]);
}
ck('the worker drops the brand side when HubSpot refused a read during it',
   /const hsFailed = HS_ERR\.n > _hsErr0;/.test(src)&&/current: hsFailed \? null : tot\(cCW\)/.test(src),true);
ck('the Executive page Brand mode fetches the live card too',/\|\|\(AP==='exec'&&_execMode\(\)==='brand'\)\)&&typeof fdRevenueOutcome==='function'/.test(cli),true);
ck('Revenue Outcome fetches the BD Tracker card counts alongside its own live figures',/async function fdRevenueOutcome\(win,from,to,vsFrom,vsTo\)\{\s*fdSignedBrands\(win,from,to,vsFrom,vsTo\);/.test(cli)&&/'\/api\/signed-brands'/.test(cli),true);
ck('the live fetch keeps the pilot side and re-renders through one function',
   /_revOutcome=\{win:win,key:key,current:d\.current,prior:d\.prior,priorMonth:d\.priorMonth,pilots:d\.pilots\|\|null\};\s*_roRerender\(\);/.test(cli),true);
// The worker's windowed pilot query.
{
  const W2={console:{log(){},warn(){},error(){}}};vm.createContext(W2);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nObject.assign(globalThis,{roPilotSql,roPilotOf});',W2);
  const wins={current:{from:'2026-11-01',to:'2026-11-12'},prior:null,priorMonth:{from:'2026-10-01',to:'2026-10-31'}};
  const pg=W2.roPilotSql(wins,true),sf=W2.roPilotSql(wins,false);
  ck('pilot SQL: one row per pilot over the span of the windows, Eastern fee-paid day',[/"BID"/.test(pg),/"RA"/.test(pg),/AT TIME ZONE 'America\/New_York'\)::date BETWEEN '2026-10-01' AND '2026-11-12'/.test(pg),/BETWEEN '2026-10-01' AND '2026-11-12'/.test(sf)],[true,true,true,true]);
  ck('  demo/test brands and rolled-over pilots left out on both paths',[/brand_name !~\* '\(demo\|test\)'/.test(pg),/REGEXP_LIKE\(v\.BRAND_NAME/.test(sf),/<> 'rolled_over'/.test(pg),/<> 'rolled_over'/.test(sf)],[true,true,true,true]);
  ck('  post-pilot MRR is the BD Tracker Expected MRR (override, else price list; open pilots only)',[/COALESCE\(p\.next_price_override_in_cents, pp\.monthly_price_in_cents, 0\)/.test(pg),/pilot_prices pp ON pp\.package = p\.package AND pp\.tier = p\.starting_tier/.test(pg),/STG_PILOT_PRICES/.test(sf)],[true,true,true]);
  ck('  a window whose dates are not YYYY-MM-DD never reaches the SQL',W2.roPilotSql({current:{from:"2026-11-01' OR 1=1 --",to:'2026-11-12'}},true),null);
  const rows=[{BID:1,DAY:'2026-11-03',FEE:'150000',POST:'90000',RA:'a1'},{BID:1,DAY:'2026-11-04',FEE:'0',POST:'10000',RA:'a1'},
    {BID:2,DAY:'2026-11-10',FEE:'200000',POST:'50000',RA:''},{BID:3,DAY:'2026-10-20',FEE:'300000',POST:'0',RA:'a3'}];
  ck('rows map to dollars per window, distinct brands, app fees without Recurly',J(W2.roPilotOf(rows,wins,null)),
    {current:{n:2,fees:3500,post:1500,feesFallback:2},prior:null,priorMonth:{n:1,fees:3000,post:0,feesFallback:1}});
  ck('  Recurly net add-ons replace the app fee per brand; a brand with none falls back',
    J(W2.roPilotOf(rows,wins,(k,r)=>r.RA?1234.5:null).current),{n:2,fees:3234.5,post:1500,feesFallback:1});
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
