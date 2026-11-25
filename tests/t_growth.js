// Growth Overview (/growth, 2026-11-24): month by month from 2026-01, Qualified
// Regular Demos and Signed Brands, each exactly as the Brand Dashboard counts
// them for a one-month window.
const fs=require('fs'),vm=require('vm');
const {load,fn}=require('./slice.js');
let pass=0,fail=0;const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

(async()=>{
  // ── Worker ──
  const src=load(process.env.WORKER_SRC||'worker.js');
  const W={console:{log(){},warn(){},error(){}},Date,JSON,Math};vm.createContext(W);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default')),W);
  const run=c=>vm.runInContext(c,W);
  ck('months run Jan 2026 to this month, the last one to today',run(`growthMonths('2026-03-17')`),[{m:'2026-01',from:'2026-01-01',to:'2026-01-31',partial:false},{m:'2026-02',from:'2026-02-01',to:'2026-02-28',partial:false},{m:'2026-03',from:'2026-03-01',to:'2026-03-17',partial:true}]);
  ck('  crosses a year end',run(`growthMonths('2027-01-02').map(x=>x.m).slice(-2)`),['2026-12','2027-01']);
  // buildGrowth with HubSpot stubbed at the fetch-helper level.
  run(`HUBSPOT=[];todayET=()=>new Date(Date.UTC(2026,1,10));
    fetchPipelineDeals=async(t,f,to)=>{HUBSPOT.push('pipe '+f+'..'+to);return f==='2026-01-01'?[
      {id:'a',properties:{date_demo_booked:'2026-01-05',demo_attendance_status:'Demo Given (originally scheduled)',brand_status:'Paying'}},
      {id:'b',properties:{date_demo_booked:'2026-01-06',demo_attendance_status:'Demo Given (rescheduled)',average_monthly_web_traffic__cloned_:'Pre-launch / just launching'}},
      {id:'c',properties:{date_demo_booked:'2026-01-07',demo_attendance_status:'No Show'}},
      {id:'d',properties:{date_demo_booked:'2026-01-08',demo_attendance_status:'Demo Given (originally scheduled)',brand_status:'Churned'}},
      {id:'e',properties:{date_demo_booked:'2025-12-20',rescheduled_meeting_date:'2026-01-09',demo_attendance_status:'Demo Given (rescheduled)'}},
      {id:'f',properties:{date_demo_booked:'2026-01-10',rescheduled_meeting_date:'2026-02-02',demo_attendance_status:'Demo Given (rescheduled)'}},
      {id:'g',properties:{date_demo_booked:'2026-01-11',demo_attendance_status:'Demo Given (originally scheduled)',average_monthly_web_traffic__cloned_:'Very Small'}}]:[]};
    fetchWebinarHeldCount=async(t,f,to)=>{HUBSPOT.push('web '+f+'..'+to);return f==='2026-01-01'?3:0;};
    fetchDealCompanyIds=async(t,ids)=>{HUBSPOT.push('assoc '+ids.join(','));return {a:['co1','co2']};};
`);
  let g=await run(`buildGrowth({HUBSPOT_TOKEN:'t'})`);
  ck('one pipeline fetch per month and no webinar count, the current month to today',run('HUBSPOT'),['pipe 2026-01-01..2026-01-31','pipe 2026-02-01..2026-02-10']);
  ck('Jan: held demos excl. pre-launch, Very Small traffic and no-shows, a since-churned brand INCLUDED, rescheduled in counts, rescheduled out does not; webinars not added; Jan-Jun cut ~20% (3 -> 2, rounded)',[g.months[0].qdRaw,g.months[0].qd,g.months[0].adj,'webinars' in g.months[0]],[3,2,true,false]);
  {const keep=W.fetchPipelineDeals,kt=W.todayET;
   run(`todayET=()=>new Date(Date.UTC(2026,6,20));fetchPipelineDeals=async(t,f)=>(f==='2026-06-01'||f==='2026-07-01')?[1,2,3,4,5].map(i=>({id:f+i,properties:{date_demo_booked:f.slice(0,8)+'1'+i,demo_attendance_status:'Demo Given (originally scheduled)'}})):[];`);
   const g2=await run(`buildGrowth({HUBSPOT_TOKEN:'t'})`);
   ck('  June is cut (5 -> 4), July onwards is not (5)',g2.months.slice(5).map(r=>[r.m,r.qd,!!r.adj]),[['2026-06',4,true],['2026-07',5,false]]);
   W.fetchPipelineDeals=keep;W.todayET=kt;}
  ck('demo months only; the current month marked partial',g.months.map(r=>[r.m,r.partial,'signed' in r]),[['2026-01',false,false],['2026-02',true,false]]);
  ck('a clean build is ok to cache',g.ok,true);
  // Cache: a clean build is stored; a failed build serves the last good copy.
  const kv={};const env={HUBSPOT_TOKEN:'t',CONTENT_STORE:{get:async(k)=>kv[k]?JSON.parse(kv[k]):null,put:async(k,v)=>{kv[k]=v;}}};
  W.env=env;
  g=await run(`fetchGrowth(env,false)`);
  ck('a clean build is cached and kept as the last good copy',[!!kv.growth_v7,!!kv.growth_last_v7,!!g.cached],[true,true,false]);
  ck('  the next load is served from cache',!!(await run(`fetchGrowth(env,false)`)).cached,true);
  run(`fetchPipelineDeals=async()=>{throw new Error('HubSpot down');};`);
  g=await run(`fetchGrowth(env,true)`);
  ck('Refresh while HubSpot fails: the last good copy, marked stale',[!!g.stale,/HubSpot down/.test(g.stale&&g.stale.error),g.months.length],[true,true,2]);
  // HubSpot failing does not throw: failed months take the last good copy's figures.
  run(`fetchPipelineDeals=async()=>{const a=[];a.partial=true;return a;};`);
  g=await run(`fetchGrowth(env,true)`);
  ck('a failed build fills failed months from the last good copy, marked, and does not cache',[g.months[0].qd,!!g.months[0].old,!!(g.stale&&g.stale.partialOnly),g.ok],[2,true,true,false]);
  ck('  the current month is filled too only if the copy ran to the same day',g.months[1].demoErr,undefined);
  run(`HS_ERR.n=0;`);
  run(`fetchPipelineDeals=async()=>{const a=[];a.partial=true;return a;};`);
  g=await run(`buildGrowth({HUBSPOT_TOKEN:'t'})`);
  ck('a month whose own deal search came back short is unavailable',[g.months[0].demoErr,g.ok],[true,false]);
  run(`fetchPipelineDeals=async()=>[];noteHsError(502,'another request');`);
  g=await run(`buildGrowth({HUBSPOT_TOKEN:'t'})`);
  ck('  another request\u2019s HubSpot failure (shared HS_ERR) does not mark this one',[g.months[0].demoErr,g.ok],[undefined,true]);
  ck('  hsSearch flags its own partial results; growth reads no shared counter',[/all\.partial = true;/.test(src),/HS_ERR\.total >/.test(src)],[true,false]);

  // ── Signed Brands = the BD Tracker's card, per month ──
  const bdh=load('bd-dashboard.html');
  const COPIED=['_bdEtToday','_bdEtDayOf','_bdSignDays','_baIsTest','_baNorm','_baJoin','_baSummary','_baRcClass','_baSummaryRc','_baPilotStatsOf','_baStatusOf'];
  ck('the worker\u2019s copies of the BD Tracker helpers are verbatim (no drift)',COPIED.filter(n=>fn(src,n)!==fn(bdh,n)),[]);
  const vline=(s,k)=>(new RegExp('^var '+k+'=.*$','m').exec(s)||[''])[0];
  ck('  and so are its test-brand and counted-pilot lists',['BD_APP_TEST_BRANDS','BD_PILOT_COUNTED'].filter(k=>!vline(src,k)||vline(src,k)!==vline(bdh,k)),[]);
  ck('  the copies sit between markers, so nobody edits them by hand unknowingly',/>>> copied from bd-dashboard\.html[\s\S]*function _baStatusOf[\s\S]*<<< copied from bd-dashboard\.html/.test(src),true);
  const BILL={mrr:100,idlePrice:0,subs:[{basis:'invoice',state:'active'}]},TRIAL={mrr:0,idlePrice:50,subs:[{basis:'trial',state:'active'}]},ENDED={mrr:0,idlePrice:0,subs:[{basis:'ended',state:'expired'}]};
  const FX={brands:[
    {id:1,name:'Alpha',hubspotCompanyId:'100',recurly:BILL,active:true,state:'active'},
    {id:2,name:'Pilot Co',pilotPaid:true,pilotHist:[{status:'live',from:'2026-01-20T10:00:00Z'}],pilot:{status:'live'},state:'pilot'},
    {id:3,name:'Gone',hubspotCompanyId:'300',recurly:ENDED,state:'ended'},
    {id:4,name:'Vincent',hubspotCompanyId:'400',recurly:BILL,active:true,state:'active'},
    {id:5,name:'App Only',signDate:'2026-02-03',recurly:TRIAL,active:true,state:'future'},
    {id:6,name:'Late Night',hubspotCompanyId:'600',recurly:BILL,active:true,state:'active'},
    {id:7,name:'Awaiting',pilot:{status:'awaiting_submit'},pilotHist:[{status:'awaiting_submit',from:'2026-01-05'}]},
    {id:8,name:'By Name Inc',recurly:BILL,active:true,state:'active'}],
   meta:{recurly:{ok:true}},pilotStats:{n:1}};
  const FD=[{id:'d1',dealstage:'closedwon',closedate:'2026-01-15T15:00:00Z',companyId:'100',dealname:'Alpha'},
    {id:'d3',dealstage:'closedwon',closedate:'2026-01-10T15:00:00Z',companyId:'300',dealname:'Gone'},
    {id:'d4',dealstage:'closedwon',closedate:'2026-01-11T15:00:00Z',companyId:'400',dealname:'Vincent'},
    {id:'d6',dealstage:'closedwon',closedate:'2026-02-01T04:30:00Z',companyId:'600',dealname:'Late Night'},
    {id:'d8',dealstage:'closedwon',closedate:'2026-02-05T15:00:00Z',companyId:'999',companyName:'By Name',dealname:'By Name - New Deal'},
    {id:'d9',dealstage:'closedlost',closedate:'2026-02-06T15:00:00Z',companyId:'100',dealname:'Alpha upsell'}];
  W.FX=FX;W.FD=FD;
  const cnt=(f,to)=>run(`signBookCount(signBookRows(FD,FX),'${f}','${to}')`);
  const sc=[cnt('2026-01-01','2026-01-31'),cnt('2026-02-01','2026-02-10')];
  ck('sign book per range: active + pilots among brands signed in it; churned counted apart; test, unpaid-pilot and lost deals out; Eastern close day; name join',
    sc,[{active:2,pilots:1,signed:3,churned:1},{active:2,pilots:0,signed:2,churned:0}]);
  // The same fixture through the BD page's OWN render path (_bdSumIn with the
  // range set, then the card's two counts) must give the same numbers.
  const B={console:{log(){}},Date,Intl,JSON,Math,String,Array,Object};vm.createContext(B);
  vm.runInContext(['_bdEtToday','_bdEtDayOf','_bdSignDays','_bdSumDayIn','_bdSumIn','_baIsTest','_baNorm','_baJoin','_baSummary','_baRcClass','_baSummaryRc','_baPilotStatsOf'].map(n=>fn(bdh,n)).join('\n')
    +'\n'+vline(bdh,'BD_APP_TEST_BRANDS')+'\n'+vline(bdh,'BD_PILOT_COUNTED')+'\nvar BD_APP_JOIN_CO={},BD_APP_JOIN_DEAL={},_bdSumFrom="",_bdSumTo="";',B);
  const card=(f,to)=>{B.FX=JSON.parse(JSON.stringify(FX));B.FD=JSON.parse(JSON.stringify(FD));return vm.runInContext(`_bdSumFrom='${f}';_bdSumTo='${to}';var j=_baJoin(FD,FX.brands);var sb=FX.brands.filter(function(b){if(_baIsTest(b))return false;return !!_bdSumIn(b,j);});
    var r=_baSummaryRc(sb,FX.meta);[r?r.total:_baSummary(sb).total,_baPilotStatsOf(sb).n]`,B);};
  ck('  active + pilots equal the BD Tracker card computed by the page’s own code',[card('2026-01-01','2026-01-31'),card('2026-02-01','2026-02-10'),card('2026-01-01','2026-02-10')],
    [sc[0],sc[1],cnt('2026-01-01','2026-02-10')].map(r=>[r.active,r.pilots]));
  FX.meta={recurly:{ok:false}};
  ck('  Recurly unread: Active Brands falls back to the app’s own active count, as the card does',[cnt('2026-01-01','2026-01-31').active,card('2026-01-01','2026-01-31')[0]],[2,2]);
  FX.meta={recurly:{ok:true}};
  // A discarded duplicate row on the same HubSpot company as a live brand joins the
  // same deal; it is not a second signing that churned.
  FX.brands.push({id:10,name:'Alpha old',hubspotCompanyId:'100',recurly:ENDED,state:'ended',discardedAt:'2026-03-01'});
  ck('  an ended row sharing its company with a counted brand is not a churn',cnt('2026-01-01','2026-01-31').churned,1);
  FX.brands.pop();
  FX.brands.push({id:11,name:'Gone 2',hubspotCompanyId:'300',recurly:ENDED,state:'ended'});
  ck('  two ended rows on one company are one churn',cnt('2026-01-01','2026-01-31').churned,1);
  FX.brands.pop();
  // A restart under a NEW company: the old deal sits on the old (discarded) row's company
  // and reaches the live row by name (allDisc), so both rows carry it.
  FX.brands.push({id:12,name:'Phoenix',hubspotCompanyId:'500',recurly:ENDED,state:'ended',discardedAt:'2026-01-20'},
    {id:13,name:'Phoenix',hubspotCompanyId:'501',recurly:BILL,active:true,state:'active'});
  FD.push({id:'d12',dealstage:'closedwon',closedate:'2026-01-12T15:00:00Z',companyId:'500',dealname:'Phoenix'});
  ck('  a restart joined by name to the same deal is active once, never also churned',[cnt('2026-01-01','2026-01-31').active,cnt('2026-01-01','2026-01-31').churned],[3,1]);
  FX.brands.splice(-2);FD.pop();
  // fetchSignBook / fetchGrowthSigned / fetchSignedBrands with the two readers stubbed.
  run(`fetchBDData=async()=>({deals:JSON.parse(JSON.stringify(FD)),meta:{}});fetchBdApp=async(e,o)=>{APPOPTS=o;return FX;};todayET=()=>new Date(Date.UTC(2026,1,10));
    LOOKUPS=[];lookupBDCompanies=async(e,ids)=>{LOOKUPS.push(ids);return {companies:{},deferred:0};};`);
  const kv2={};const env2={HUBSPOT_TOKEN:'t',CONTENT_STORE:{get:async(k)=>kv2[k]?JSON.parse(kv2[k]):null,put:async(k,v)=>{kv2[k]=v;}}};W.env2=env2;
  g=await run(`fetchGrowthSigned(env2,false)`);
  ck('growth-signed: reads the app book lite (no per-account Recurly reads), caches a clean sign book',[run('APPOPTS'),g.months.map(r=>[r.signed,r.churned]),!!kv2.sign_book_v4,!!kv2.sign_book_last_v4],[{lite:true},[[3,1],[2,0]],true,true]);
  ck('  no cohort list in the payload any more','live' in g,false);
  ck('  year to date is Jan 1..today, each brand once',[g.ytd.m,g.ytd.signed,g.ytd.active,g.ytd.pilots,g.ytd.churned],['ytd',5,4,1,1]);
  ck('  looks up the uncached company names of closed-won deals only, as the BD page does',run('LOOKUPS'),[['100','300','400','600']]);
  ck('  next load from the cached book',!!(await run(`fetchGrowthSigned(env2,false)`)).cached,true);
  g=await run(`fetchSignedBrands(env2,{window:'custom',from:'2026-01-01',to:'2026-01-31',vsFrom:'2026-02-01',vsTo:'2026-02-10'})`);
  ck('signed-brands (Revenue Outcome): the card for the window and its prior window, from the same book',[g.current&&g.current.signed,g.current&&g.current.active,g.current&&g.current.pilots,g.prior&&g.prior.signed,!!g.cached],[3,2,1,2,true]);
  // A name only the company lookup supplies joins the brand (signing it on the deal's date, not its app date).
  FX.brands.push({id:9,name:'Named Later',signDate:'2026-02-07',recurly:BILL,active:true,state:'active'});
  FD.push({id:'d10',dealstage:'closedwon',closedate:'2026-01-25T15:00:00Z',companyId:'777',dealname:'Something Else'});
  run(`lookupBDCompanies=async(e,ids)=>({companies:{'777':{name:'Named Later'}},deferred:0});`);
  g=await run(`fetchGrowthSigned(env2,true)`);
  ck('  a name from the company lookup moves the brand to its deal’s month',g.months.map(r=>r.active),[3,2]);
  FX.brands.pop();FD.pop();
  run(`lookupBDCompanies=async(e,ids)=>({companies:{},deferred:3});`);
  g=await run(`fetchGrowthSigned(env2,true)`);
  ck('  lookups deferred: the last good book, marked',[!!g.stale,g.stale&&g.stale.error],[true,'Some HubSpot calls failed']);
  run(`lookupBDCompanies=async(e,ids)=>({companies:{},deferred:0});`);
  const before=kv2.sign_book_last_v4;
  FX.meta={recurly:{ok:true,stale:true}};
  g=await run(`fetchGrowthSigned(env2,true)`);
  ck('  Recurly stale: today’s cohort on its last read, as the card shows, flagged, not cached',[!!g.stale,/last good read/.test(g.warn||''),g.months.map(r=>r.active),kv2.sign_book_last_v4===before],[false,true,[2,2],true]);
  FX.meta={recurly:{ok:false}};
  g=await run(`fetchGrowthSigned(env2,true)`);
  ck('  Recurly unread: the app’s own count, flagged',[!!g.stale,/app’s own count/.test(g.warn||''),g.months.map(r=>r.active)],[false,true,[2,2]]);
  FX.meta={recurly:{ok:true}};FX.pilotStats={error:'x'};delete kv2.sign_book_last_v4;
  let threw=null;try{await run(`fetchGrowthSigned(env2,true)`);}catch(e){threw=String(e.message);}
  ck('  pilot stats missing (the card is hidden then) and no copy: an error, never zeros',/Pilot stats unavailable/.test(threw||''),true);
  FX.pilotStats={n:1};
  ck('route: POST /api/growth-signed is behind authed()',/url\.pathname === '\/api\/growth-signed'\) \{\s*let body;\s*try \{ body = await request\.json\(\); \} catch \{ return jr\(\{ error: 'Invalid JSON' \}, 400\); \}\s*if \(!\(await authed\(\)\)\) return jr/.test(src),true);
  ck('route: POST /api/signed-brands is behind authed()',/url\.pathname === '\/api\/signed-brands'\) \{\s*let body;\s*try \{ body = await request\.json\(\); \} catch \{ return jr\(\{ error: 'Invalid JSON' \}, 400\); \}\s*if \(!\(await authed\(\)\)\) return jr/.test(src),true);
  ck('route: POST /api/growth is behind authed()',/url\.pathname === '\/api\/growth'\) \{\s*let body;\s*try \{ body = await request\.json\(\); \} catch \{ return jr\(\{ error: 'Invalid JSON' \}, 400\); \}\s*if \(!\(await authed\(\)\)\) return jr/.test(src),true);
  ck('route: GET /growth serves the page; build embeds it',[/url\.pathname === '\/growth'\) \{\s*return new Response\(GROWTH_HTML/.test(src),/growth-dashboard\.html/.test(load('build.js'))],[true,true]);

  // ── Page ──
  const html=load('growth-dashboard.html');
  const js=html.slice(html.lastIndexOf('<script>')+8,html.lastIndexOf('</script>'));
  const els={};const el=id=>els[id]||(els[id]={innerHTML:'',textContent:'',style:{}});
  const P={console,localStorage:{getItem(){return null;},setItem(){}},document:{getElementById:el,addEventListener(){}},window:{innerWidth:1200,innerHeight:800}};
  vm.createContext(P);vm.runInContext(js,P);
  ck('title is Growth Overview, never "investor"',[/<title>FrontrowMD Growth Overview<\/title>/.test(html),/investor/i.test(html)],[true,false]);
  ck('first tab is Signed Brands & Demos',vm.runInContext('G_TABS.map(t=>t.label)',P),['Signed Brands & Demos']);
  P.G.data={asOf:'2026-11-24T12:00:00Z',today:'2026-03-12',months:[
    {m:'2026-01',to:'2026-01-31',qd:40},
    {m:'2026-02',to:'2026-02-28',qd:0,demoErr:true},
    {m:'2026-03',to:'2026-03-12',partial:true,qd:12}]};
  P.G.sLoading=true;
  let h=P.gRenderDemos();
  ck('signed half still loading: its cells say so, never 0, and the demos show',[/Counting Signed Brands/.test(h),/<td>Jan 2026<\/td><td class="big">…<\/td><td class="big">40<\/td><td class="big">…<\/td><\/tr>/.test(h)],[true,true]);
  ck('  no KPI cards at all',/class="kpi/.test(h),false);
  P.G.sLoading=false;P.G.signed={asOf:'2026-11-24T12:00:00Z',today:'2026-03-12',ytd:{signed:15,active:11,pilots:4,churned:3},months:[
    {m:'2026-01',to:'2026-01-31',signed:8,active:6,pilots:2,churned:2},{m:'2026-02',to:'2026-02-28',signed:5,active:5,pilots:0,churned:1},{m:'2026-03',to:'2026-03-12',signed:3,active:1,pilots:2,churned:0}]};
  h=P.gRenderDemos();
  P.G.data.months[0].adj=true;P.G.data.months[0].qdRaw=50;
  ck('a cut month is marked ‡ and the footnote says why, word for word',[/<td class="big">40<sup>‡<\/sup><\/td>/.test(P.gRenderDemos()),/‡ January through June we were not explicitly asking if brands were pre-launch, but still demo’ing them\. July onwards, brands were self-identifying pre-demo as “pre-launch”\./.test(P.gRenderDemos())],[true,true]);
  delete P.G.data.months[0].adj;delete P.G.data.months[0].qdRaw;
  ck('Signed Brands = active + pilots (the card); no Active, Pilots or churned columns',[/<td>Jan 2026<\/td><td class="big">8<\/td><td class="big">40<\/td><td class="big">20%<\/td><\/tr>/.test(h),/<th>Active<\/th>|<th>Pilots<\/th>|Churned/.test(h)],[true,false]);
  ck('monthly detail is Month, Signed Brands, Demos Held, Signed ÷ Demos; no cohort columns, no KPI cards',[/<th>Month<\/th><th>Signed Brands<\/th><th>Demos Held<\/th><th>Signed ÷ Demos<\/th><\/tr>/.test(h),/Demo → Signed|Since Signed/.test(h),/class="kpi/.test(h)],[true,false,false]);
  ck('totals: the year counted once (ytd), demos skip an unavailable month and say so; no year ratio then',[/<tr class="tot"><td>Total<\/td><td class="big">15<\/td><td class="big">52<\/td><td class="big">—<\/td><\/tr>/.test(h),/show as n\/a/.test(h)],[true,true]);
  ck('the current month is marked to date',/<tr class="part"><td>Mar 2026<\/td>/.test(h)&&/runs to 2026-03-12 \(month to date\)/.test(h),true);
  P.G.data.months[1]={m:'2026-02',to:'2026-02-28',qd:20};
  ck('the Total ratio shows once every month\u2019s demos were read: 15 ÷ 72',/<tr class="tot"><td>Total<\/td><td class="big">15<\/td><td class="big">72<\/td><td class="big">21%<\/td>/.test(P.gRenderDemos()),true);
  P.G.data.months[1]={m:'2026-02',to:'2026-02-28',qd:0,demoErr:true};
  ck('an unavailable month shows n/a, not 0',/<td>Feb 2026<\/td><td class="big">5<\/td><td class="big"><span class="na">n\/a<\/span><\/td><td class="big"><span class="na">n\/a<\/span><\/td>/.test(h),true);
  ck('chart has a column per month; Signed bar first',[(h.match(/class="band"/g)||[]).length,/Signed Brands<\/span><span><i style="background:#72A4BF"><\/i>Demos Held/.test(h)],[3,true]);
  ck('section renamed Signed Brands & Demos',/<span class="sec-t">Signed Brands &amp; Demos<\/span>/.test(h),true);
  P.G.signed=null;P.G.sErr='boom';
  h=P.gRenderDemos();
  ck('signed request failed: its band, n/a cells',[/Couldn’t load Signed Brands: boom/.test(h),/<td>Jan 2026<\/td><td class="big"><span class="na">n\/a<\/span><\/td><td class="big">40<\/td><td class="big"><span class="na">n\/a<\/span><\/td>/.test(h)],[true,true]);
  ck('a column with no month read totals n/a, not 0',/<tr class="tot"><td>Total<\/td><td class="big">—<\/td><td class="big">52<\/td><td class="big">—<\/td>/.test(h),true);
  P.G.sErr=null;P.G.signed={today:'2026-03-12',ytd:{signed:1,active:1,pilots:0,churned:0},months:[{m:'2026-01',to:'2026-01-31',signed:8,active:6,pilots:2,churned:0}],stale:{error:'Recurly could not be read just now',asOf:'2026-11-24T10:00:00Z'}};
  h=P.gRenderDemos();
  ck('a stale signed copy says why; a month the copy lacks is n/a',[/Signed Brands: Recurly could not be read just now/.test(h),/<td>Mar 2026<\/td><td class="big"><span class="na">n\/a<\/span><\/td>/.test(h)],[true,true]);
  P.G.signed.today='2026-03-11';
  ck('a year-to-date figure from another day is not used for the Total',/<tr class="tot"><td>Total<\/td><td class="big">—<\/td>/.test(P.gRenderDemos()),true);
  ck('Demos Held has no webinars column or split',[/<th>Webinars<\/th>|webinars<\/span>/.test(h),/Webinars are not counted/.test(h)],[false,true]);
  ck('definitions: the BD Tracker card; churned brands not counted',/the BD Tracker’s Signed Brands card with the month picked as its range/.test(h)&&/has churned since is not counted/.test(h),true);
  P.G.err='Growth data unavailable';
  ck('a failed Refresh keeps the numbers on screen and says so',/Refresh failed \(Growth data unavailable\)/.test(P.gRenderDemos())&&/<td>Jan 2026<\/td>/.test(P.gRenderDemos()),true);
  P.G.data=null;P.G.err='boom';
  ck('a failed load is an error band',/Couldn’t load the numbers: boom/.test(P.gRenderDemos()),true);

  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})().catch(e=>{console.error(e);process.exit(1);});
