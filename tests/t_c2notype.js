// C2 "No churn type" drill-down (2026-11-20). The table under the Churn Rate
// chart must list exactly the churns behind its amber line, period by period.
// Renders the real page script in a stub DOM.
const fs=require('fs'),vm=require('vm');
let pass=0,fail=0;const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const html=fs.readFileSync('bd-dashboard.html','utf8');
const scripts=[...html.matchAll(/<script(?![^>]*src)[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
const els={};
const mk=id=>els[id]||(els[id]={id,style:{},innerHTML:'',textContent:'',value:'',dataset:{},children:[],addEventListener(){},removeEventListener(){},appendChild(){},setAttribute(){},getAttribute(){return null},querySelector(){return null},querySelectorAll(){return []},classList:{add(){},remove(){},toggle(){},contains(){return false}},getBoundingClientRect(){return{top:0,left:0,width:0,height:0}},scrollIntoView(){},focus(){}});
const document={readyState:'complete',getElementById:mk,querySelector:()=>mk('q'),querySelectorAll:()=>[],addEventListener(){},removeEventListener(){},createElement:()=>mk('c'+Math.random()),body:mk('body'),documentElement:mk('html'),head:mk('head')};
const ctx={console:{log(){},warn(){},error(){}},document,location:{protocol:'https:',hash:'',search:'',href:''},navigator:{userAgent:''},Intl,Date,Math,JSON,
  fetch:()=>new Promise(()=>{}),setTimeout:()=>0,clearTimeout(){},setInterval:()=>0,clearInterval(){},requestAnimationFrame:()=>0,
  localStorage:{getItem(){return null},setItem(){}},sessionStorage:{getItem(){return null},setItem(){}},alert(){},getComputedStyle:()=>({}),
  addEventListener(){},removeEventListener(){},matchMedia:()=>({matches:false,addEventListener(){}}),Chart:function(){return{destroy(){},update(){}}},
  ResizeObserver:function(){return{observe(){},disconnect(){}}},IntersectionObserver:function(){return{observe(){},disconnect(){}}},
  innerWidth:1400,innerHeight:900,scrollY:0,history:{replaceState(){}}};
ctx.window=ctx;ctx.self=ctx;vm.createContext(ctx);
for(const s of scripts){try{vm.runInContext(s,ctx);}catch(e){console.log('script load error:',e.message);}}

const B=(id,name,extra)=>Object.assign({id,name,signDate:'2025-06-01',createdAt:'2025-06-01',state:'active',active:true,monthly:1000,discardedAt:'',hubspotCompanyId:'',
  recurly:{mrr:1000,subs:[{state:'active',basis:'invoice',mrr:1000}],idlePrice:0},pilot:null,pilotPlan:null,pilotPaid:false,onboarding:false,onboardingExpected:0,
  subHist:[],pilotHist:[],hsDeals:[],everLive:true,endDate:'',endOnBulkDay:false},extra||{});
const gone=(id,name,end,extra)=>B(id,name,Object.assign({state:'discarded',active:false,discardedAt:end,endDate:end,monthly:800,recurly:{mrr:null}},extra||{}));
const CH='3453925110';
const deal=(id,co,extra)=>Object.assign({id,dealname:'Deal '+id,companyId:co,companyName:'',dealstage:CH,closedate:'2025-06-01T04:00:00Z',createdate:'2025-06-01T04:00:00Z',
  entered_closedwon:'',brand_status:'Churned',amount:500,pausechurn_date:'',never_implemented_churn:'',churn_reason:''},extra||{});
const boardType=Object.keys(ctx.BD_CHURN_TYPE_GROUP).find(k=>ctx.BD_CHURN_TYPE_GROUP[k]===ctx.BD_CHURN_BOARD);
const brands=[B(1,'Live Co',{hubspotCompanyId:'c1'}),
  gone(2,'No Deal Oct','2026-10-05'),                                   // app churn, no HubSpot churn deal -> no type
  gone(3,'Blank Type Oct','2026-10-20',{hubspotCompanyId:'c3'}),        // app churn, churn deal with blank type -> no type
  gone(4,'Board Oct','2026-10-12',{hubspotCompanyId:'c4'}),             // app churn, Board Defined -> NOT in the table
  gone(5,'No Deal Sep','2026-09-03')];                                  // another period
const deals=[deal('d3','c3',{pausechurn_date:'2026-10-18T04:00:00Z'}),
  deal('d4','c4',{pausechurn_date:'2026-10-12T04:00:00Z',never_implemented_churn:boardType}),
  deal('d6','c9',{pausechurn_date:'2026-03-10T04:00:00Z',dealname:'Old HubSpot Churn'})];   // before June: HubSpot's own event, blank type
ctx.BD.data={deals,ownerMap:{},meta:{generatedAt:new Date().toISOString()}};
ctx.BD.app={brands,pilotStats:{source:'live',deposit_paid:0,collecting_data:0,live:0,waiting:0,n:0,awaiting:0,netFees:0,post:0},
  meta:{live:true,recurly:{ok:true,unmatched:{count:0,mrr:0}},pilotPlan:{brands:0,expected:0,expectedBrands:0,byStatus:{},net:0,netParts:{pilotFee:0,reviews:0,analyses:0,clinicianAi:0,other:0,refunds:0}}}};
ctx._bdTab='churn';
const render=()=>{try{ctx.renderBD();}catch(e){}const h=els['bd-content'].innerHTML;const i=h.indexOf('class="bd-c2nt"');return i<0?'':h.slice(i,h.indexOf('</details>',i));};
let t=render();
const rows=x=>[...x.matchAll(/<tr><td>([^<]+)<\/td><td><b>([^<]+)<\/b>/g)].map(m=>m[1]+' '+m[2]);
ck('the drill-down renders under C2',t.length>0,true);
ck('it lists exactly the no-type churns, newest first, Board Defined left out',rows(t),['Oct 26 Blank Type Oct','Oct 26 No Deal Oct','Sep 26 No Deal Sep','Mar 26 Old HubSpot Churn']);
ck('the summary counts them',/4 churns from 2026 onward/.test(t),true);
ck('the period picker shows each period with its count',/>Oct 26 \(2\)<\/option>/.test(t)&&/>Sep 26 \(1\)<\/option>/.test(t)&&/>All periods \(4\)</.test(t),true);
ck('why: no churn deal vs a churn deal with a blank type',
  [/No Deal Oct[\s\S]*?No HubSpot churn deal within 90 days/.test(t),/Blank Type Oct[\s\S]*?HubSpot churn deal has no Churn Type/.test(t)],[true,true]);
ck('a borrowed deal name is shown under the brand',/Blank Type Oct<\/b><span class="bd-c2nt-id">HubSpot deal: Deal d3/.test(t),true);
ck('a churn deal links to HubSpot; a brand with none says so',/0-3\/d3"/.test(t)&&/No Deal Sep[\s\S]*?no HubSpot record/.test(t),true);
// The table and the amber line come from one predicate.
const src=html;
ck('the amber line and the table share _chNoType',/else if\(_chNoType\(dd\)\)nU\[k\]/.test(src)&&/!_chNoType\(dd\)\)return;/.test(src),true);
// Picking a period narrows the rows and keeps the drill-down open.
ctx.bdC2NtPeriod('2026-10');t=render();
ck('a period narrows the table to that point of the line',rows(t),['Oct 26 Blank Type Oct','Oct 26 No Deal Oct']);
ck('  and stays open after the re-render',/class="bd-c2nt" open/.test(t),true);
ctx.bdSetChurnRateGrain('quarter');t=render();
ck('switching to quarterly regroups and clears the period pick',[rows(t)[0],/>Q4 '26 \(2\)<\/option>/.test(t),ctx._bdC2NtPer],["Q4 '26 Blank Type Oct",true,'']);
console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
