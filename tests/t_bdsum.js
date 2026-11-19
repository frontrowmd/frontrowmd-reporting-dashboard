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

// ── The cohort rule ────────────────────────────────────────────────────────
ctx._bdSumFrom='2026-10-01';ctx._bdSumTo='2026-10-31';
ck('a brand signed inside is in',ctx._bdSumIn({signDate:'2026-10-31'}),true);
ck('the day after is out',ctx._bdSumIn({signDate:'2026-11-01'}),false);
ck('no signing date is out of every range',ctx._bdSumIn({signDate:''}),false);
ctx._bdSumFrom='';ctx._bdSumTo='';

// ── Section 01, rendered ───────────────────────────────────────────────────
const B=(name,sign,mrr,extra)=>Object.assign({id:name,name,signDate:sign,createdAt:sign,state:'active',active:true,monthly:mrr,discardedAt:'',
  recurly:{mrr,subs:[{state:'active',basis:'invoice',mrr}],idlePrice:0},pilot:null,pilotPlan:null,onboarding:false,onboardingExpected:0,subHist:[],pilotHist:[],hsDeals:[]},extra||{});
const PL=(name,sign,status,label,post,net)=>B(name,sign,0,{state:'pilot',active:false,recurly:{mrr:null},
  pilot:{status,postPrice:post,depositPaidAt:status==='awaiting_submit'?'':sign,depositTotal:5000},pilotPlan:{status:label,expected:post,net}});
const brands=[B('Old Co','2025-03-02',1000),B('Oct Co','2026-10-15',2000),B('Nov Co','2026-11-10',500),
  B('Onb Oct','2026-10-03',0,{state:'pending',active:false,recurly:{mrr:null},onboarding:true,onboardingExpected:700}),
  PL('Pilot Oct','2026-10-20','live','Pilot – live',3000,4800),
  PL('Pilot Old','2025-01-05','collecting_data','Pilot – collecting data',1000,5000),
  PL('Pilot Wait Oct','2026-10-25','awaiting_submit','Pilot – fee not paid',900,0)];
ctx.BD.data={deals:[],ownerMap:{},meta:{generatedAt:new Date().toISOString()}};
ctx.BD.app={brands,pilotStats:{source:'live',deposit_paid:0,collecting_data:1,live:1,waiting:1,n:2,awaiting:1,netFees:10000,post:4000},
  meta:{live:true,recurly:{ok:true,unmatched:{count:2,mrr:300}},pilotPlan:{brands:3,expected:4000,expectedBrands:2,byStatus:{},net:9800,
    netParts:{pilotFee:9800,reviews:0,analyses:0,clinicianAi:0,other:0,refunds:0}}}};
const tiles=()=>{const h=els['bd-content'].innerHTML,i=h.indexOf('id="bd-sec-summary"'),sec=h.slice(i,h.indexOf('</section>',i));
  const o={};for(const m of sec.matchAll(/<div class="tlbl">([^<]+)<\/div><div class="tval">([^<]+)</g))o[m[1]]=m[2];o._sec=sec;return o;};
const render=()=>{try{ctx.renderBD();}catch(e){/* later sections need more stub data; section 01 is written first */}return tiles();};
let t=render();
ck('all time: the server totals are shown untouched',[t['Active Brands'],t['Current MRR'],t['Pilot Brands'],t['Awaiting Deposit'],t['Initiation Fees'],t['Post-Pilot MRR']],['3','$3,500','2','1','$9,800','$4,000']);
ck('all time: Future MRR = MRR + post-pilot + onboarding',t['Future MRR'],'$8,200');
ck('all time: Reset is disabled',/class="bd-sum-rs" disabled/.test(t._sec),true);
ck('the bar offers all six presets',['MTD','Last Month','This Quarter','Last Quarter','Last 7 Days','Last 90 Days'].every(l=>t._sec.includes('>'+l+'</button>')),true);
const goal=ctx.BD._signedArr;

ctx._bdSumFrom='2026-10-01';ctx._bdSumTo='2026-10-31';ctx._bdSumKey='lastMonth';
t=render();
ck('October: only the October brand is active',[t['Active Brands'],t['Current MRR'],t['Current ARR'],t['Avg ACV']],['1','$2,000','$24,000','$2,000']);
ck('October: pilots signed in October, per brand',[t['Pilot Brands'],t['Pilot Live'],t['Awaiting Deposit']],['1','1','1']);
ck('October: their fees and post-pilot MRR (fee-unpaid pilot left out)',[t['Initiation Fees'],t['Post-Pilot MRR']],['$4,800','$3,000']);
ck('October: Future MRR = 2,000 + 3,000 + 700 onboarding',t['Future MRR'],'$5,700');
ck('October: the active preset is highlighted',/class="bd-sum-p on" aria-pressed="true" onclick="bdSumPreset\('lastMonth'\)"/.test(t._sec),true);
ck('October: the note says what is shown',/Brands signed 10\/01\/2026 – 10\/31\/2026 · 4 of 7 brands/.test(t._sec),true);
ck('October: unmatched Recurly subscriptions are not mentioned (they have no signing date)',/match no app brand/.test(t._sec),false);
ck('the goal dialog keeps the all-time ARR',ctx.BD._signedArr,goal);

ctx._bdSumFrom='2024-01-01';ctx._bdSumTo='2026-12-31';ctx._bdSumKey='custom';
t=render();
ck('a range covering every signing date reproduces the brand cards',[t['Active Brands'],t['Current MRR'],t['Future MRR']],['3','$3,500','$8,200']);
ck('  and the pilot counts (per brand = the board, one pilot per brand)',[t['Pilot Brands'],t['Awaiting Deposit'],t['Pilot Live']],['2','1','1']);

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
ck('Reset returns to the all-time numbers',[t['Active Brands'],t['Pilot Brands'],t['Initiation Fees']],['3','2','$9,800']);
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
