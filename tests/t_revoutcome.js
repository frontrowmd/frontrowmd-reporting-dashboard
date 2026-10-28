// Revenue Outcome card, 2026-10-28: ROAS removed, and an "Include Pilot Brands"
// checkbox. A pilot brand is a signed deal with Pilot Waitlist Status filled in;
// unticked (the default), every figure on the card leaves them out. RUN, not
// grepped: what matters is which dollars end up on the card.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
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
   /fetchClosedWonDeals[\s\S]{0,900}?'pilot_waitlist_status'\]/.test(src),true);
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

// ── 2. The card: All / Excluding Pilot / Pilot Only ─────────────────────────
const mkCtx=(store)=>{
  const C={console,Math,JSON,Number,String,Object,Array,
    localStorage:{getItem:k=>(k in store?store[k]:null),setItem:(k,v)=>{store[k]=String(v);}},
    document:{getElementById:()=>null}};
  vm.createContext(C);
  vm.runInContext([
    'var AP="dashboard";var S={dashboard:{win:"mtd",data:null},irfan:{},exec:{}};',
    "const f$=v=>v==null?'—':'$'+Math.round(v).toLocaleString();",
    "const fN=v=>v==null?'—':Math.round(v).toLocaleString();",
    'function _execMode(){return "brand";}',
    'var _revOutcome=null;',
    'var window=globalThis;',
    decl(cli,'var RO_PILOT_MODES=','];'),
    decl(cli,'var _roPilotMode=','})();'),
    fn(cli,'_roRerender'),
    decl(cli,'window.roSetPilotMode=function','\n};'),
    fn(cli,'_buildRevenueOutcomeCard'),
    'Object.assign(globalThis,{_buildRevenueOutcomeCard,S,setMode:window.roSetPilotMode,setRO:v=>{_revOutcome=v;},mode:()=>_roPilotMode});',
  ].join('\n'),C);
  return C;
};
const text=h=>h.replace(/<[^>]+>/g,' ').replace(/\s+/g,' ');
const LIVE={win:'mtd',
  current:{mrr:30000,count:12,pilotMrr:6000,pilotCount:3},
  prior:{mrr:20000,count:9,pilotMrr:0,pilotCount:0},
  priorMonth:{mrr:25000,count:10,pilotMrr:2500,pilotCount:1}};
const d0={signedChurnedByCloseCount:2};
const pressed=h=>((/aria-pressed="true"[^>]*>([^<]+)</.exec(h))||[])[1];
{
  const C=mkCtx({});C.setRO(LIVE);
  const h=C._buildRevenueOutcomeCard(d0), t=text(h);
  ck('ROAS is gone from the card',[/ROAS/.test(h),/Churn-Adjusted/i.test(h)],[false,false]);
  ck('the toggle offers exactly All / Excluding Pilot / Pilot Only',
     [...h.matchAll(/onclick="roSetPilotMode\('(\w+)'\)"[^>]*>([^<]+)</g)].map(m=>[m[1],m[2]]),
     [['all','All'],['excl','Excluding Pilot'],['only','Pilot Only']]);
  ck('Excluding Pilot is the default, and shown as the live choice',[C.mode(),pressed(h)],['excl','Excluding Pilot']);
  // excl: 30,000 - 6,000 = 24,000 MRR; 12 - 3 + 2 churned add-back = 11 brands.
  ck('Excluding Pilot: every figure loses the pilots',
     [/\$288,000/.test(t),/MRR \$24,000/.test(t),/Signed Brands 11/.test(t)],[true,true,true]);
  // LM non-pilot 22,500 -> Signed ARR +1,500 x 12 = +$18,000. Had LM kept its
  // pilot it would be 25,000 and the delta -$12,000.
  ck('  and compares against windows without THEIR pilots',[/vs LM[^v]*\+\$18,000/.test(t),/-\$12,000/.test(t)],[true,false]);
}
{
  const store={};const C=mkCtx(store);C.setRO(LIVE);
  C.setMode('all');
  const t=text(C._buildRevenueOutcomeCard(d0));
  ck('All: the full totals',[/\$360,000/.test(t),/MRR \$30,000/.test(t),/Signed Brands 14/.test(t)],[true,true,true]);
  ck('  remembered for this browser, across a reload',[store.roPilotMode,mkCtx(store).mode()],['all','all']);
}
{
  const C=mkCtx({});C.setRO(LIVE);C.setMode('only');
  const h=C._buildRevenueOutcomeCard(d0),t=text(h);
  // only: 6,000 MRR, 3 brands -- the churned add-back is NOT credited to pilots.
  ck('Pilot Only: just the pilots',[/\$72,000/.test(t),/MRR \$6,000/.test(t),/Signed Brands 3/.test(t),/ACV \$2,000/.test(t)],[true,true,true,true]);
  ck('  with the churned add-back left out, not credited to pilots',/Signed Brands 5/.test(t),false);
  // Prior had 0 pilots, LM had 2,500: +6,000 and +3,500 MRR -> x12 for ARR.
  ck('  compared against prior and LM pilots',
     [/vs prior[^v]*\+\$72,000/.test(t),/vs LM[^v]*\+\$42,000/.test(t)],[true,true]);
  ck('  and marked as the live choice',pressed(h),'Pilot Only');
}
{
  const C=mkCtx({});C.setMode('bogus');
  ck('an unknown mode is ignored',C.mode(),'excl');
  const C2=mkCtx({roIncludePilots:'1'});
  ck('a tick saved by the preview checkbox carries over as All',C2.mode(),'all');
}
{ // Before the live numbers land: the cached /api/data figures, same rules.
  const C=mkCtx({});
  const d={signedChurnedByCloseCount:0,mrrArr:{
    mrr:{value:30000,sameTimePrior:{value:20000},lastMonth:{value:25000}},
    dealCount:{value:12,sameTimePrior:{value:9},lastMonth:{value:10}},
    pilot:{cur:{mrr:6000,count:3},prior:{mrr:0,count:0},lm:{mrr:2500,count:1}}}};
  ck('the cached figures follow the toggle too, so the first paint is right',
     [/\$288,000/.test(text(C._buildRevenueOutcomeCard(d))),(C.setMode('only'),/\$72,000/.test(text(C._buildRevenueOutcomeCard(d))))],[true,true]);
}
{ // An older payload with no split: All is all it can show, and it says so.
  const C=mkCtx({});C.setMode('only');
  const d={signedChurnedByCloseCount:0,mrrArr:{mrr:{value:30000},dealCount:{value:12}}};
  const h=C._buildRevenueOutcomeCard(d),t=text(h);
  ck('with no pilot split it shows All, disabled, and says why',
     [/\$360,000/.test(t),/<button disabled/.test(h),/pilot split loading/.test(t),pressed(h)],[true,true,true,'All']);
}
ck('the live fetch and the toggle re-render through one function',
   /_revOutcome=\{win:win,current:d\.current,prior:d\.prior,priorMonth:d\.priorMonth\};\s*_roRerender\(\);/.test(cli),true);

// ── 3. Executive Dashboard: the Brand goal band is off, the Clinician one stays ─
ck('the Brand goal is switched off on the Executive Dashboard',/const EXEC_SHOW_BRAND_GOAL=false;/.test(cli),true);
ck('  and only for the Brand mode there',
   /const _goal=\(!_cln&&!EXEC_SHOW_BRAND_GOAL\)\?'':_goalTrackerHTML\(\{/.test(cli),true);
ck('  while the Brand Dashboard keeps its own',
   /const _goal=_goalTrackerHTML\(\{cur:_gm\?_gm\.regular:null,goal:GOAL_BRAND_REGULAR_DEMOS,/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
