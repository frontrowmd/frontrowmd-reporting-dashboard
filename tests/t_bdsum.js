// BD Tracker section 01 date filter (2026-11-19): a COHORT filter. Every card keeps
// its definition, computed over the brands signed in the range as they stand today.
// Renders the real page script in a stub DOM.
const fs=require('fs'),vm=require('vm');let pass=0,fail=0;const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const html=fs.readFileSync('bd-dashboard.html','utf8');
const scripts=[...html.matchAll(/<script(?![^>]*src)[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
const els={};
const mk=id=>els[id]||(els[id]={id,style:{},innerHTML:'',textContent:'',value:'',dataset:{},children:[],addEventListener(){},removeEventListener(){},appendChild(){},setAttribute(){},getAttribute(){return null},querySelector(){return null},querySelectorAll(){return []},classList:{add(){},remove(){},toggle(){},contains(){return false}},getBoundingClientRect(){return{top:0,left:0,width:0,height:0}},scrollIntoView(){},focus(){}});
const document={readyState:'complete',getElementById:mk,querySelector:()=>mk('q'),querySelectorAll:()=>[],addEventListener(){},removeEventListener(){},createElement:()=>mk('c'+Math.random()),body:mk('body'),documentElement:mk('html'),head:mk('head')};
const ctx={console,document,location:{protocol:'https:',hash:'',search:'',href:''},navigator:{userAgent:''},Intl,Date,Math,JSON,
  fetch:()=>new Promise(()=>{}),setTimeout:()=>0,clearTimeout(){},setInterval:()=>0,clearInterval(){},requestAnimationFrame:()=>0,
  localStorage:{getItem(){return null},setItem(){}},sessionStorage:{getItem(){return null},setItem(){}},alert(){},getComputedStyle:()=>({}),
  addEventListener(){},removeEventListener(){},matchMedia:()=>({matches:false,addEventListener(){}}),Chart:function(){return{destroy(){},update(){}}},
  ResizeObserver:function(){return{observe(){},disconnect(){}}},IntersectionObserver:function(){return{observe(){},disconnect(){}}},
  innerWidth:1400,innerHeight:900,scrollY:0,history:{replaceState(){}}};
ctx.window=ctx;ctx.self=ctx;vm.createContext(ctx);
for(const s of scripts){try{vm.runInContext(s,ctx);}catch(e){console.log('script load error:',e.message);}}

// ── Presets, for a fixed Eastern "today" ───────────────────────────────────
const R=(k,t)=>{const r=ctx._bdSumPresetRange(k,t);return [r.from,r.to];};
ck('MTD',R('mtd','2026-11-19'),['2026-11-01','2026-11-19']);
ck('Last Month',R('lastMonth','2026-11-19'),['2026-10-01','2026-10-31']);
ck('This Quarter',R('thisQuarter','2026-11-19'),['2026-10-01','2026-11-19']);
ck('Last Quarter',R('lastQuarter','2026-11-19'),['2026-07-01','2026-09-30']);
ck('Last 7 Days includes today',R('last7','2026-11-19'),['2026-11-13','2026-11-19']);
ck('Last 90 Days includes today',R('last90','2026-11-19'),['2026-08-22','2026-11-19']);
ck('January: Last Month is last December',R('lastMonth','2027-01-05'),['2026-12-01','2026-12-31']);
ck('January: Last Quarter is last Q4',R('lastQuarter','2027-01-05'),['2026-10-01','2026-12-31']);
ck('March 31 → Last Month is all of February',R('lastMonth','2028-03-31'),['2028-02-01','2028-02-29']);
ck('DST week: Last 7 Days stays 7 days',R('last7','2026-11-03'),['2026-10-28','2026-11-03']);
ck('Eastern today: 03:00 UTC is still yesterday in New York',ctx._bdEtToday(new Date('2026-11-20T03:00:00Z')),'2026-11-19');

// ── The cohort rule: Revenue Outcome's signing dates (2026-11-19) ─────────
ctx._bdSumFrom='2026-10-01';ctx._bdSumTo='2026-10-31';
const D=(dealstage,closedate)=>({dealstage,closedate});
const sd=(b,deals)=>{const r=ctx._bdSignDays(b,deals);return [r.how,r.days];};
ck('a paying brand signs on its closed-won deal\'s close date, not its app date',sd({signDate:'2025-03-02'},[D('closedwon','2026-10-10T16:00:00Z')]),['hubspot',['2026-10-10']]);
ck('  read as an Eastern day (9pm ET on Oct 31 is Oct 31)',sd({signDate:'2026-10-15'},[D('closedwon','2026-11-01T01:00:00Z')]),['hubspot',['2026-10-31']]);
ck('  a churned-stage deal is not a signing (Revenue Outcome counts closed-won)',sd({signDate:'2026-10-15'},[D('3453925110','2026-10-10T16:00:00Z')]),['',[]]);
ck('a pilot brand signs when its fee was paid, not when its deal closed',sd({pilotPaid:true,pilotHist:[{from:'2026-10-20',status:'live'}]},[D('closedwon','2026-11-05T16:00:00Z')]),['pilot',['2026-10-20']]);
ck('  a rolled-over pilot is a paying brand: its deal date counts',sd({pilotPaid:false,pilot:{status:'rolled_over'},pilotHist:[{from:'2026-06-01',status:'rolled_over'}]},[D('closedwon','2026-10-12T16:00:00Z')]),['hubspot',['2026-10-12']]);
ck('an app-only sign-up (no deal, no pilot) falls back to its app date',sd({signDate:'2026-10-03',pilot:null},[]),['app',['2026-10-03']]);
ck('an unpaid pilot sign-up with no deal has not signed',sd({signDate:'2026-10-25',pilot:{status:'awaiting_submit'},pilotPaid:false},[]),['',[]]);
ck('a brand with a deal that is not closed-won has not signed (no app-date fallback)',sd({signDate:'2026-10-03',pilot:null},[D('appointmentscheduled','')]),['',[]]);
ck('in the range when ANY signing day is',ctx._bdSumIn({pilotPaid:true,pilotHist:[{from:'2025-01-01',status:'opted_out'},{from:'2026-10-02',status:'live'}]},null),'pilot');
ck('  out when none is',ctx._bdSumIn({pilotPaid:true,pilotHist:[{from:'2026-11-02',status:'live'}]},null),'');
ctx._bdSumFrom='';ctx._bdSumTo='';

// ── Section 01, rendered ───────────────────────────────────────────────────
const B=(name,sign,mrr,extra)=>Object.assign({id:name,name,signDate:sign,createdAt:sign,state:'active',active:true,monthly:mrr,discardedAt:'',hubspotCompanyId:'',
  recurly:{mrr,subs:[{state:'active',basis:'invoice',mrr}],idlePrice:0},pilot:null,pilotPlan:null,pilotPaid:false,onboarding:false,onboardingExpected:0,subHist:[],pilotHist:[],hsDeals:[]},extra||{});
const PL=(name,sign,status,label,post,net,paid,extra)=>B(name,sign,0,Object.assign({state:'pilot',active:false,recurly:{mrr:null},pilotPaid:!!paid,
  pilotHist:paid?[{from:paid,status}]:[],
  pilot:{status,postPrice:post,depositPaidAt:paid||'',depositTotal:5000},pilotPlan:{status:label,expected:post,net}},extra||{}));
const brands=[
  B('Old Co','2025-03-02',1000,{hubspotCompanyId:'tc1'}),          // app row 2025, HubSpot signed Oct 10
  B('Oct Co','2026-10-15',2000,{hubspotCompanyId:'tc2'}),          // closed 9pm ET Oct 31
  B('Nov Co','2026-10-28',500,{hubspotCompanyId:'tc3'}),           // app row Oct, HubSpot signed Nov 10
  B('Rolled','2026-05-01',1500,{hubspotCompanyId:'tc4',pilot:{status:'rolled_over'},pilotHist:[{from:'2026-06-01',status:'rolled_over'}]}),
  B('Onb Oct','2026-10-03',0,{state:'pending',active:false,recurly:{mrr:null},onboarding:true,onboardingExpected:700}),   // app-only
  PL('Pilot Oct','2026-09-01','live','Pilot \u2013 live',3000,4800,'2026-10-20',{hubspotCompanyId:'tc5'}),   // fee Oct, deal Nov
  PL('Pilot Old','2025-01-05','collecting_data','Pilot \u2013 collecting data',1000,5000,'2025-01-06'),
  PL('Pilot Wait Oct','2026-10-25','awaiting_submit','Pilot \u2013 fee not paid',900,0,'')];
const deal=(id,co,closedate,amount)=>({id,dealname:'deal '+id,companyId:co,companyName:'',dealstage:'closedwon',closedate,brand_status:'Signed',amount});
ctx.BD.data={deals:[deal('d1','tc1','2026-10-10T16:00:00Z',1000),deal('d2','tc2','2026-11-01T01:00:00Z',2000),deal('d3','tc3','2026-11-10T16:00:00Z',500),
  deal('d4','tc4','2026-10-12T16:00:00Z',1500),deal('d5','tc5','2026-11-05T16:00:00Z',2500)],ownerMap:{},meta:{generatedAt:new Date().toISOString()}};
ctx.BD.app={brands,pilotStats:{source:'live',deposit_paid:0,collecting_data:1,live:1,waiting:1,n:2,awaiting:1,netFees:10000,post:4000},
  meta:{live:true,recurly:{ok:true,unmatched:{count:2,mrr:300}},pilotPlan:{brands:3,expected:4000,expectedBrands:2,byStatus:{},net:9800,
    netParts:{pilotFee:9800,reviews:0,analyses:0,clinicianAi:0,other:0,refunds:0}}}};
const tiles=()=>{const h=els['bd-content'].innerHTML,i=h.indexOf('id="bd-sec-summary"'),sec=h.slice(i,h.indexOf('</section>',i));
  const o={};for(const m of sec.matchAll(/<div class="tlbl">([^<]+)<\/div><div class="tval">([^<]+)</g))if(!(m[1] in o))o[m[1]]=m[2];o._sec=sec;return o;};  // first card of a name: the Signed Brands row repeats Avg ACV / Initiation Fees
const render=()=>{try{ctx.renderBD();}catch(e){/* later sections need more stub data; section 01 is written first */}return tiles();};

const sgn=x=>{const i=x._sec.indexOf('aria-label="Signed Brands"');if(i<0)return null;const seg=x._sec.slice(i);const o={};
  for(const m of seg.matchAll(/<div class="tlbl">([^<]+)<\/div><div class="tval">([^<]+)</g))o[m[1]]=m[2];o._seg=seg;return o;};
let t=render();
ck('all time: the server totals are shown untouched',[t['Active Brands'],t['Current MRR'],t['Pilot Brands'],t['Awaiting Deposit'],t['Initiation Fees'],t['Post-Pilot MRR']],['4','$5,000','2','1','$9,800','$4,000']);
ck('all time: Future MRR = MRR + post-pilot + onboarding',t['Future MRR'],'$9,700');
ck('all time: Reset is disabled',/class="bd-sum-rs" disabled/.test(t._sec),true);
ck('the bar offers all six presets',['MTD','Last Month','This Quarter','Last Quarter','Last 7 Days','Last 90 Days'].every(l=>t._sec.includes('>'+l+'</button>')),true);
const goal=ctx.BD._signedArr;
ck('all time: no Signed Brands row',sgn(t),null);
ck('all time: the Current MRR hover has no range note',/In this range:/.test(t._sec),false);

ctx._bdSumFrom='2026-10-01';ctx._bdSumTo='2026-10-31';ctx._bdSumKey='lastMonth';
t=render();
ck('October: the brands HubSpot closed in October, whatever their app date',[t['Active Brands'],t['Current MRR'],t['Current ARR'],t['Avg ACV']],['3','$4,500','$54,000','$1,500']);
ck('October: the pilot whose fee was paid in October (its deal closed in November)',[t['Pilot Brands'],t['Pilot Live'],t['Awaiting Deposit']],['1','1','0']);
ck('October: its fee and post-pilot MRR',[t['Initiation Fees'],t['Post-Pilot MRR']],['$4,800','$3,000']);
ck('October: Future MRR = 4,500 + 3,000 + 700 (the app-only onboarding sign-up)',t['Future MRR'],'$8,200');
ck('October: the active preset is highlighted',/class="bd-sum-p on" aria-pressed="true" onclick="bdSumPreset\('lastMonth'\)"/.test(t._sec),true);
ck('October: the note counts the cohort by how each signed',/Brands signed 10\/01\/2026 \u2013 10\/31\/2026 \u00b7 5 brands, as they stand today \(3 by HubSpot close date, 1 pilots by fee paid, 1 app-only sign-ups by app date\)/.test(t._sec),true);
ck('October: unmatched Recurly subscriptions are not mentioned (they have no signing date)',/match no app brand/.test(t._sec),false);
ck('the goal dialog keeps the all-time ARR',ctx.BD._signedArr,goal);
{const g=sgn(t);
 ck('October: the Signed Brands row appears, the two card rows added up',[g['Signed Brands'],g['Signed MRR'],g['Signed ARR'],g['Initiation Fees'],g['Avg ACV']],['4','$7,500','$90,000','$4,800','$1,875']);
 ck('  Signed = the Active Brands + Pilot Brands cards, Signed MRR = Current MRR + Post-Pilot MRR (Alex 2026-11-24: no HubSpot)',/3 active \+ 1 pilot/.test(g._seg)&&/\$4,500 active \+ \$3,000 pilot/.test(g._seg),true);
 ck('  and carries no HubSpot chip',/bd-chip hubspot/.test(g._seg),false);
 ck('  every card prints its calculation',[/3 active \+ 1 pilot = 4/,/\$7,500 \u00d7 12/,/\$4,500 active \+ \$3,000 pilot/,/Net add-ons of 1 pilot/,/\$7,500 \u00f7 4/].map(r=>r.test(g._seg)),[true,true,true,true,true]);
 ck('  in its own teal cards and panel',/class="bd-pil-grp bd-sgn-grp"/.test(g._seg)||/bd-sgn-grp/.test(t._sec),true);
 ck('  it sits after the pilot row',t._sec.indexOf('aria-label="Signed Brands"')>t._sec.indexOf('aria-label="Pilots"'),true);}
{const tip=lbl=>{const m=t._sec.match(new RegExp('<div class="tile [^"]*" title="([^"]*)"><div class="tlbl">'+lbl+'<'));return m?m[1]:'';};
 ck('October: every top-row card carries the range note',['Active Brands','Current ARR','Current MRR','Avg ACV','Future ARR','Future MRR'].map(l=>/^In this range: 3 brands billing in Recurly, 0 in a trial, starting later or paused, and 1 with no live Recurly subscription yet/.test(tip(l))),[true,true,true,true,true,true]);
 ck('  with a tail that fits the card',[/not counted here until they have one/.test(tip('Active Brands')),/add \$0 here\./.test(tip('Avg ACV')),/onboarding sign-ups add their expected MRR/.test(tip('Future MRR'))],[true,true,true]);}
ck('October: the Current MRR hover splits the cohort by Recurly state',/In this range: 3 brands billing in Recurly, 0 in a trial, starting later or paused, and 1 with no live Recurly subscription yet/.test(t._sec),true);

ctx._bdSumFrom='2026-11-01';ctx._bdSumTo='2026-11-30';ctx._bdSumKey='custom';
t=render();
ck('November: only the brand HubSpot closed in November (Oct app row); the pilot deal that closed in Nov is not a November pilot',
  [t['Active Brands'],t['Current MRR'],t['Pilot Brands']],['1','$500','0']);
ck('November: the pilot deal is not a signed brand; one signed brand at its deal amount',[sgn(t)['Signed Brands'],sgn(t)['Signed MRR']],['1','$500']);

ctx._bdSumFrom='2024-01-01';ctx._bdSumTo='2026-12-31';ctx._bdSumKey='custom';
t=render();
ck('a range covering every signing date reproduces the brand cards',[t['Active Brands'],t['Current MRR'],t['Future MRR']],['4','$5,000','$9,700']);
ck('  and the paid pilot counts; an unpaid sign-up never signed, so it is in no range',[t['Pilot Brands'],t['Pilot Live'],t['Awaiting Deposit']],['2','1','0']);

// Review fixes (2026-11-19).
// Section 02's current month reads the ALL-TIME pilot/onboarding totals.
{
  const src=fs.readFileSync('bd-dashboard.html','utf8');
  ck('section 02 this-month bars read _s2Cur, not section 01\'s filtered cards',/_s2\.pil\[_pi\]=_s2Cur\.postN/.test(src)&&!/_s2\.pil\[_pi\]=_postN/.test(src),true);
  ck('  and _s2Cur is built from the unfiltered book',/_s2Cur=\{post:_ppAllT\?_ppAllT\.expected:\(_psAllT\?_psAllT\.post:null\),postN:_ppAllT\?_ppAllT\.expectedBrands:\(_psAllT\?_psAllT\.n:0\),onb:_bdOnbOf\(_ba\.brands\)\};/.test(src),true);
}
// The filtered pilot chip follows the brand book's source.
ctx.BD.app.meta.live=false;t=render();
const _seg=x=>x._sec.slice(x._sec.indexOf('>Pilot Brands<'),x._sec.indexOf('>Awaiting Deposit<'));
ck('in a range, the pilot count chip follows the brand book (nightly) not the board (live)',/class="bd-chip app nightly"/.test(_seg(t)),true);
ctx.BD.app.meta.live=true;t=render();
ck('  and reads live when the brand book is live',/class="bd-chip app"/.test(_seg(t))&&!/nightly/.test(_seg(t)),true);
ctx.BD.app.meta.live=true;
// A running preset is re-anchored on today at render.
ctx._bdSumKey='last7';ctx._bdSumFrom='2000-01-01';ctx._bdSumTo='2000-01-07';render();
const want=ctx._bdSumPresetRange('last7',ctx._bdEtToday());
ck('a stale running preset is re-anchored on today',[ctx._bdSumFrom,ctx._bdSumTo],[want.from,want.to]);
ctx._bdSumKey='custom';ctx._bdSumFrom='2000-01-01';ctx._bdSumTo='2000-01-07';render();
ck('  a custom range is left alone',[ctx._bdSumFrom,ctx._bdSumTo],['2000-01-01','2000-01-07']);
ctx.bdSumReset();t=render();
ck('Reset returns to the all-time numbers',[t['Active Brands'],t['Pilot Brands'],t['Initiation Fees']],['4','2','$9,800']);
ck('Reset clears the range',[ctx._bdSumFrom,ctx._bdSumTo,ctx._bdSumKey],['','','']);

ctx.document.getElementById('bd-sum-from').value='2026-11-05';ctx.document.getElementById('bd-sum-to').value='2026-11-01';
ctx.bdSumApplyCustom();
ck('a reversed custom range is refused, not applied',ctx._bdSumFrom,'');
ctx.document.getElementById('bd-sum-to').value='';ctx.bdSumApplyCustom();
ck('an open-ended custom range is allowed',[ctx._bdSumFrom,ctx._bdSumTo,ctx._bdSumKey],['2026-11-05','','custom']);
ctx.bdSumReset();

// The client's pilot-plan rebuild must match the worker's own on the same brands.
{
  const src=fs.readFileSync('worker.js','utf8');
  const {fn}=require('./slice.js');
  const c2={};vm.createContext(c2);
  vm.runInContext(src.match(/const BDAPP_PILOT_PLAN_LABEL = [^;]+;/)[0]+'\n'+fn(src,'bdPilotPlanStats')+';globalThis.f=bdPilotPlanStats;',c2);
  const copy=JSON.parse(JSON.stringify(brands));
  c2.f({},copy,null).then(w=>{
    const c=ctx._baPilotPlanOf(brands,false);
    ck('client pilot-plan rebuild = worker bdPilotPlanStats (brands, expected, expectedBrands)',[c.brands,c.expected,c.expectedBrands],[w.brands,w.expected,w.expectedBrands]);
    ck('  and the fee-not-paid label is the worker\'s',ctx.BD_PILOT_FEE_UNPAID,c2.BDAPP_PILOT_PLAN_LABEL?c2.BDAPP_PILOT_PLAN_LABEL.awaiting_submit:vm.runInContext('BDAPP_PILOT_PLAN_LABEL.awaiting_submit',c2));
    console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
  });
}
