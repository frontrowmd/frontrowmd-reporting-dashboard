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
  gone(2,'No Deal Oct','2026-10-05',{hubspotCompanyId:'c2'}),           // app churn; HubSpot deal still Closed Won / Signed -> no type
  gone(3,'Blank Type Oct','2026-10-20',{hubspotCompanyId:'c3'}),        // app churn, churn deal with blank type -> no type
  gone(4,'Board Oct','2026-10-12',{hubspotCompanyId:'c4'}),             // app churn, Board Defined -> NOT in the table
  gone(5,'No Deal Sep','2026-09-03'),                                   // no HubSpot deal at all
  gone(7,'Typed Won Sep','2026-09-20',{hubspotCompanyId:'c7'}),
  gone(8,"Lane's 6/3 Test",'2026-07-15'),gone(9,'Example Brand Name','2026-06-23')];   // test accounts: never churns        // Closed Won deal with Churn Type filled (DTC Brands case)                                  // another period
const deals=[deal('d3','c3',{pausechurn_date:'2026-10-18T04:00:00Z'}),
  deal('w2','c2',{dealstage:'closedwon',brand_status:'Signed',dealname:'No Deal Oct',closedate:'2026-01-10T05:00:00Z'}),
  deal('w7','c7',{dealstage:'closedwon',brand_status:'Signed',dealname:'Typed Won Sep',never_implemented_churn:'Yes - Rejected Product(s)',pausechurn_date:'2026-09-25'}),
  deal('d4','c4',{pausechurn_date:'2026-10-12T04:00:00Z',never_implemented_churn:boardType}),
  deal('d6','c9',{pausechurn_date:'2026-03-10T04:00:00Z',dealname:'Old HubSpot Churn'})];   // before June: HubSpot's own event, blank type
ctx.BD.data={deals,ownerMap:{},meta:{generatedAt:new Date().toISOString()}};
ctx.BD.app={brands,pilotStats:{source:'live',deposit_paid:0,collecting_data:0,live:0,waiting:0,n:0,awaiting:0,netFees:0,post:0},
  meta:{live:true,recurly:{ok:true,unmatched:{count:0,mrr:0}},pilotPlan:{brands:0,expected:0,expectedBrands:0,byStatus:{},net:0,netParts:{pilotFee:0,reviews:0,analyses:0,clinicianAi:0,other:0,refunds:0}}}};
ctx._bdTab='churn';
const render=()=>{try{ctx.renderBD();}catch(e){}const h=els['bd-content'].innerHTML;const i=h.indexOf('class="bd-c2nt"');return i<0?'':h.slice(i,h.indexOf('</details>',i));};
let t=render();
// Rows: [brand, line badge, app id, churn date].
const rows=x=>[...x.matchAll(/<tr><td><b>([^<]+)<\/b>(?:<span class="bd-c2nt-id">[^<]*<\/span>)?<span class="bd-c2nt-ty (\w+)">[^<]*<\/span><\/td><td[^>]*>(?:<span[^>]*>)?([^<]*)/g)].map(m=>m[1]+'|'+m[2]+'|'+m[3]);
const hdr=x=>[...x.matchAll(/<th[^>]*>([^<]+)<\/th>/g)].map(m=>m[1]);
ck('the drill-down renders under C2',t.length>0,true);
ck('test accounts never appear as churns',/Lane's 6\/3 Test|Example Brand Name/.test(t),false);
ck('columns: Brand, App ID, Links, Churn date, Why no type, Fix',hdr(t),['Brand','App ID','Links','Churn date','Why no type','Fix']);
ck('all types by default: every churn on the chart, newest first',rows(t),['Blank Type Oct|none|3','Board Oct|board|4','No Deal Oct|none|2','Typed Won Sep|none|7','No Deal Sep|none|5','Old HubSpot Churn|none|\u2014']);
ck('the summary counts each line',/6 from 2026 onward: 1 Board Defined, 0 Never Implemented, <span class="bd-c2nt-nt">5 no churn type<\/span>/.test(t),true);
ck('the type picker lists the three lines with counts',/>All types \(6\)</.test(t)&&/value="board"[^>]*>Board Defined \(1\)</.test(t)&&/value="ni"[^>]*>Never Implemented \(0\)</.test(t)&&/value="none"[^>]*>No churn type \(5\)</.test(t),true);
ck('a typed churn has no "why" or fix',/Board Oct[\s\S]*?\u2014 has a type/.test(t),true);
const rowOf=n=>{const i=t.indexOf('<b>'+n+'</b>');return i<0?'':t.slice(i,t.indexOf('</tr>',i));};
ck('why: a churn deal with a blank type',/HubSpot churn deal has no Churn Type/.test(rowOf('Blank Type Oct'))&&/Set Churn Type on the deal/.test(rowOf('Blank Type Oct')),true);
ck('why: the brand\'s deal is still Closed Won / Signed, and it links to that deal',
  /HubSpot deal is still Closed Won \u00b7 Signed, but the app ended the brand/.test(rowOf('No Deal Oct'))&&/0-3\/w2"/.test(rowOf('No Deal Oct')),true);
ck('why: Churn Type filled but the deal never moved to a churn stage',
  /HubSpot deal has Churn Type \u201cYes - Rejected Product\(s\)\u201d but is still Closed Won \u00b7 Signed, not a churn stage/.test(rowOf('Typed Won Sep'))&&/Move the deal to Churned; its Churn Type will then count/.test(rowOf('Typed Won Sep')),true);
ck('why: no HubSpot deal joined at all',/No HubSpot deal is joined to this brand/.test(rowOf('No Deal Sep'))&&/no HubSpot record/.test(rowOf('No Deal Sep')),true);
ck('a borrowed deal name is shown under the brand',/Blank Type Oct<\/b><span class="bd-c2nt-id">HubSpot deal: Deal d3/.test(t),true);
ck('links: deal + company when there is a churn deal; "no HubSpot record" when nothing',/0-3\/d3"[^>]*>Deal \u2197<\/a> \u00b7 <a href="[^"]*0-2\/c3"/.test(t)&&/No Deal Sep[\s\S]*?no HubSpot record/.test(t),true);
ck('the lines and the table share the same tests',/else if\(_chNoType\(dd\)\)nU\[k\]/.test(html)&&/var _chLineOf=function\(dd\)\{return _bdIsBoardChurn\(dd\)\?'board':_chIsNI\(dd\)\?'ni':_chNoType\(dd\)\?'none':'';\};/.test(html),true);
ctx.bdC2NtTypeSet('none');t=render();
ck('No churn type: only the amber line\'s churns',rows(t).map(r=>r.split('|')[0]),['Blank Type Oct','No Deal Oct','Typed Won Sep','No Deal Sep','Old HubSpot Churn']);
ck('  and the period counts follow the type',/>All periods \(5\)</.test(t)&&/>Oct 26 \(2\)<\/option>/.test(t),true);
ctx.bdC2NtPeriod('2026-10');t=render();
ck('type + period = one point of one line',rows(t).map(r=>r.split('|')[0]),['Blank Type Oct','No Deal Oct']);
ck('  and stays open after the re-render',/class="bd-c2nt" open/.test(t),true);
ctx.bdC2NtTypeSet('board');t=render();
ck('Board Defined in October',rows(t).map(r=>r.split('|')[0]),['Board Oct']);
ctx.bdC2NtTypeSet('ni');t=render();
ck('an empty pick says so rather than showing nothing',/No churns of this type in this period/.test(t),true);
ctx.bdC2NtTypeSet('bogus');
ck('an unknown type falls back to all',ctx._bdC2NtType,'');
ctx.bdSetChurnRateGrain('quarter');t=render();
ck('switching to quarterly regroups and clears the period pick',[/>Q4 '26 \(3\)<\/option>/.test(t),ctx._bdC2NtPer],[true,'']);
console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
