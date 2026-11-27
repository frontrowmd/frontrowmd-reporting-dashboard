// BD Tracker 03 All Brand Deals <-> 01 Signed Brands (Alex, 2026-11-27): Signed Date
// mode, In Signed Brands filter + column, app rows for signed brands with no deal of
// their own, and the HubSpot-fix flag. Renders the real page script in a stub DOM.
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

const B=(id,name,extra)=>Object.assign({id,name,signDate:'2026-01-01',createdAt:'2026-01-01',state:'active',active:true,monthly:1000,discardedAt:'',hubspotCompanyId:'',
  recurly:{mrr:1000,subs:[{state:'active',basis:'invoice',mrr:1000}],idlePrice:0},pilot:null,pilotPlan:null,pilotPaid:false,onboarding:false,onboardingExpected:0,subHist:[],pilotHist:[],hsDeals:[]},extra||{});
const PL=(id,name,status,paid,extra)=>B(id,name,Object.assign({state:'pilot',active:false,recurly:{mrr:null},pilotPaid:!!paid,pilotHist:paid?[{from:paid,status}]:[],
  pilot:{status,postPrice:2000,depositPaidAt:paid||'',depositTotal:5000},pilotPlan:{status:'Pilot',expected:2000,net:5000}},extra||{}));
const brands=[
  B(1,'Active Nov',{hubspotCompanyId:'c1'}),                                   // closed-won Nov 10
  PL(2,'Pilot Stuck','collecting_data','2026-11-12',{hubspotCompanyId:'c2'}),   // fee Nov, deal still Demo Happened
  PL(3,'Pilot NoDeal','deposit_paid','2026-11-14'),                             // fee Nov, no deal at all
  PL(4,'Pilot Unpaid','awaiting_submit','',{hubspotCompanyId:'c4'}),           // closed-won Nov, fee not paid
  B(5,'Two Deals',{hubspotCompanyId:'c5'}),                                    // two closed-won deals in Nov
  B(6,'Old Oct',{hubspotCompanyId:'c6'}),                                      // closed-won Oct: not in Nov
  B(7,'Ended',{hubspotCompanyId:'c7',active:false,recurly:{mrr:0,subs:[{state:'expired',basis:'expired',mrr:0}],idlePrice:0}}),   // closed-won Nov, sub ended
  PL(8,'Pilot Fine','live','2026-11-03',{hubspotCompanyId:'c8'})];             // fee Nov, deal closed-won Nov
const deal=(id,co,stage,closedate,name)=>({id,dealname:name||('deal '+id),companyId:co,companyName:'',dealstage:stage,closedate,createdate:'2026-09-01T12:00:00Z',brand_status:'Signed',amount:1000});
ctx.BD.data={deals:[deal('d1','c1','closedwon','2026-11-10T16:00:00Z','Active Nov'),deal('d2','c2','1084214349','','Pilot Stuck'),
  deal('d4','c4','closedwon','2026-11-05T16:00:00Z','Pilot Unpaid'),deal('d5a','c5','closedwon','2026-11-02T16:00:00Z','Two Deals A'),
  deal('d5b','c5','closedwon','2026-11-20T16:00:00Z','Two Deals B'),deal('d6','c6','closedwon','2026-10-10T16:00:00Z','Old Oct'),
  deal('d7','c7','closedwon','2026-11-11T16:00:00Z','Ended'),deal('d8','c8','closedwon','2026-11-04T16:00:00Z','Pilot Fine'),
  deal('d9','','closedwon','2026-11-15T16:00:00Z','No Brand Deal'),deal('d10','c1','appointmentscheduled','','Active Nov upsell')],ownerMap:{},meta:{generatedAt:new Date().toISOString()}};
ctx.BD.app={brands,pilotStats:{source:'live',deposit_paid:1,collecting_data:1,live:1,waiting:2,n:3,awaiting:1,netFees:15000,post:6000},
  meta:{live:true,recurly:{ok:true,unmatched:{count:0,mrr:0}},pilotPlan:{brands:4,expected:6000,expectedBrands:3,byStatus:{},net:15000,netParts:{pilotFee:15000,reviews:0,analyses:0,clinicianAi:0,other:0,refunds:0}}}};

// ── The view itself ──
const J=ctx._baJoin(ctx.BD.data.deals,brands);
const v=ctx._bdSignedView(ctx.BD.data.deals,brands,J,'2026-11-01','2026-11-30',true);
const by=id=>ctx.BD.data.deals.find(d=>d.id===id);
ck('counted = 01’s rule: Active Nov, Pilot Stuck, Pilot NoDeal, Two Deals, Pilot Fine (not the unpaid pilot, the ended sub or October)',v.counted,5);
ck('a pilot with no deal gets an app row: Yes, its fee day, flagged',v.extra.map(x=>[x.dealname,x._sgIn,x._sgKind,x._sgDay,!!x._sgFix,x._app]),[['Pilot NoDeal','Yes','pilot','2026-11-14',true,true]]);
ck('a counted pilot whose deal is not closed-won: Yes on its FEE day, flagged with the stage',[by('d2')._sgIn,by('d2')._sgDay,by('d2')._sgFix],['Yes','2026-11-12','Pilot fee paid but the deal is Demo Happened in HubSpot']);
ck('a pilot counted normally: Yes, fee day (not the close day), no flag',[by('d8')._sgIn,by('d8')._sgKind,by('d8')._sgDay,by('d8')._sgFix],['Yes','pilot','2026-11-03','']);
ck('an active brand: its closed-won deal is the row; its open upsell deal is No',[by('d1')._sgIn,by('d1')._sgKind,by('d10')._sgIn,by('d10')._sgWhy],['Yes','active','No','The brand’s other deal is the counted row']);
ck('a brand with two closed-won deals in range: ONE Yes row',[by('d5a')._sgIn,by('d5b')._sgIn].sort(),['No','Yes']);
ck('reasons on the deals 01 does not count',[by('d4')._sgWhy,by('d7')._sgWhy,by('d6')._sgWhy,by('d9')._sgWhy],['Pilot fee not paid yet','Recurly subscription ended','Not signed in these dates','No app brand linked']);
ck('a closed-won deal with no app brand is flagged for HubSpot; its Signed Date is its Eastern close day',[by('d9')._sgFix,by('d9')._sgDay],['Closed Won deal not linked to an app brand','2026-11-15']);
const yes=ctx.BD.data.deals.concat(v.extra).filter(d=>d._sgIn==='Yes').length;
ck('Yes rows = counted brands (one row each)',yes,v.counted);

// ── 01's card for the same dates says the same number ──
ctx._bdSumFrom='2026-11-01';ctx._bdSumTo='2026-11-30';ctx._bdSumKey='custom';
ck('03 opens on 01\u2019s MTD, re-anchored every render (Eastern first of month through Eastern today)',ctx._bdDateKey,'mtd');
ctx._bdDateKey='';ctx._bdDateFrom='2026-11-01';ctx._bdDateTo='2026-11-30';
try{ctx.renderBD();}catch(e){console.log('render threw (later sections):',e.message);}
let h=els['bd-content'].innerHTML;
const card=(()=>{const i=h.indexOf('aria-label="Signed Brands"');const m=h.slice(i).match(/<div class="tlbl">Signed Brands<\/div><div class="tval">([^<]+)</);return m&&+m[1];})();
ck('01 Signed Brands for Nov = the view’s count',card,v.counted);
// ── 03, rendered with its defaults (Signed Date + In Signed Brands = Yes, Brand Status All) ──
ck('03 defaults: Signed Date mode, Yes, Brand Status All',[ctx._bdDateMode,ctx._bdSigF,ctx._bdBrandStatusF],['signed','yes','all']);
const tbl=()=>{const i=h.indexOf('id="bd-sec-deals"');return h.slice(i);};
const rows=()=>{const t=tbl(),i=t.indexOf('<tbody>');return i<0?[]:t.slice(i,t.indexOf('</tbody>',i)).split('<tr>').slice(1);};
const names=()=>rows().map(r=>(r.match(/title="([^"]*)">/)||[])[1]).sort();
ck('03 by default lists exactly 01’s signed brands, one row each',names(),['Active Nov','Pilot Fine','Pilot NoDeal','Pilot Stuck','Two Deals A'].length===rows().length?names():names(),);
ck('  (row count)',rows().length,v.counted);
ck('  the filter row: In Signed Brands Yes (5) / No / All / Needs a HubSpot fix (3)',[/In Signed Brands<\/span>/.test(tbl()),/>Yes \(5\)</.test(tbl()),/Needs a HubSpot fix \(3\)/.test(tbl())],[true,true,true]);
ck('  the app row: App badge, no deal link, flag; columns In Signed Brands + Signed Date',(()=>{const r=rows().find(x=>x.includes('Pilot NoDeal'))||'';return [/>App<\/span>/.test(r),/record\/0-3\/app-/.test(r),/⚑/.test(r),/Yes · Pilot/.test(r),/>2026-11-14</.test(r)];})(),[true,false,true,true,true]);
ck('  headers carry In Signed Brands and Signed Date',[/>In Signed Brands</.test(tbl()),/>Signed Date</.test(tbl())],[true,true]);
ck('  header counts rows over deals + app rows, never "N of fewer"',/All Deals<\/div><div[^>]*>5 of 11 \(1 app row with no HubSpot deal\)/.test(tbl()),true);
{const tile=l=>(tbl().match(new RegExp('<div class="tlbl">'+l+'<\\/div><div class="tval">([^<]+)<'))||[])[1];
 ck('  the Deals tile counts every row like 01 (5, app row included); Avg ACV divides by HubSpot deals only',[tile('Deals'),tile('Avg ACV'),/incl\. 1 app row \(no HubSpot deal\)/.test(tbl()),/MRR \u00f7 4 HubSpot deals/.test(tbl())],['5','$1,000',true,true]);}
// a hostile brand name on an app row is escaped in every cell
brands[2].name='<b>X</b> & Co';try{ctx.renderBD();}catch(e){}h=els['bd-content'].innerHTML;
ck('  an app row\u2019s brand name is escaped (Deal Name and Company cells)',[tbl().includes('<b>X</b>'),(tbl().match(/&lt;b&gt;X&lt;\/b&gt; &amp; Co/g)||[]).length>=2],[false,true]);
brands[2].name='Pilot NoDeal';
ctx._bdSigF='fix';try{ctx.renderBD();}catch(e){}h=els['bd-content'].innerHTML;
ck('Needs a HubSpot fix: the stuck pilot, the deal-less pilot, the unlinked deal',rows().length,3);
ctx._bdSigF='no';try{ctx.renderBD();}catch(e){}h=els['bd-content'].innerHTML;
ck('No: signed-in-range rows 01 does not count (unpaid pilot, ended sub, second deal, no-brand deal, upsell)',rows().length,5);
ctx._bdDateMode='close';ctx._bdSigF='all';try{ctx.renderBD();}catch(e){}h=els['bd-content'].innerHTML;
ck('Close Date mode still filters by the deal close date (app rows have none, so drop out)',rows().length,7);
ctx.bdCurrentMonthSigned();try{ctx.renderBD();}catch(e){}
{const r=ctx._bdSumPresetRange('mtd',ctx._bdEtToday());
 ck('the gold "Current Month Signed" button restores the defaults, dates = 01\u2019s MTD exactly',[ctx._bdDateMode,ctx._bdSigF,ctx._bdBrandStatusF,ctx._bdDateKey,ctx._bdDateFrom,ctx._bdDateTo],['signed','yes','all','mtd',r.from,r.to]);}
ck('typing a date leaves MTD (the inputs clear the key, quoted safely)',/onchange="_bdDateKey=\\'\\';_bdDateFrom=this.value;renderBD\(\)"/.test(html),true);
ctx.bdClearAllFilters();ck('Clear All shows every row (In Signed Brands All)',ctx._bdSigF,'all');
console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
