// BD Tracker on the APP basis (2026-11-11): /api/bd-app (worker.js bdApp*)
// and the page's _ba* helpers (bd-dashboard.html). Alex's decisions: the app
// owns brand count, MRR, Future MRR, churn count and churn date; pilots are
// their own row; churn is the app's from 2026-06-01 and HubSpot's before.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const bd=load(process.env.BD_SRC||'bd-dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const J=(x)=>JSON.parse(JSON.stringify(x));

// ── Worker side ──
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{bdAppPrice,bdAppTermMonths,bdAppChooseSub,bdAppPilotPost,bdAppBrand,bdAppBuild,bdAppSql,bdAppAssocSql,fetchBdApp,MB_NEEDED_TABLES,BDAPP_BULK_DAYS,BDAPP_BACKFILL_DAYS,'
  +'fetchBdPilotStats,bdPilotStatsOf,grafanaFrameRows,GRAFANA_URL,GRAFANA_APP_DS,BD_PILOT_SQL_PG,BD_PILOT_SQL_SF,BDAPP_SQL_PG,BDAPP_PAGES,'
  +'recurlySubMrr,recurlyAttach,fetchRecurlyBook,fetchRecurlyLive,recurlyPlanMonths,RECURLY_KV,bdAppParseSubs,recurlyCheck,bdAppPilotPlan,recurlyAddOnNet,bdPilotPlanStats,recurlySubWindow});',W);
W.btoa=(x)=>Buffer.from(String(x)).toString('base64');W.setTimeout=setTimeout;
const P=(o)=>W.bdAppPrice(Object.assign({man:null,mp:null,disp:null,planp:null,planName:'Hero',planKind:'usage'},o)).monthly;

// 1. Price normalisation
ck('manual price wins over the others',P({man:250000,mp:150000,disp:120000,planp:100000}),2500);
ck('  then monthly, display, plan, in that order',[P({mp:150000,disp:120000,planp:100000}),P({disp:120000,planp:100000}),P({planp:100000})],[1500,1200,1000]);
ck('a value under 10,000 cents is dollars typed as cents: x100',P({man:1500}),1500);
ck('  so 9999 reads $9,999 and 10000 reads $100',[P({man:9999}),P({man:10000})],[9999,100]);
ck('a 6-month plan holding a term total of $3,000+ is divided by 6',P({man:1019400,planName:'6-Month FrontrowMD Subscription'}),1699);
ck('  "(6 Months)" and "(Annual)" name a term too',[P({man:1200000,planName:'Hero (6 Months)'}),P({man:1800000,planName:'Core (Annual)'})],[2000,1500]);
ck('  below $3,000 the term plan value is already monthly and kept',P({man:249900,planName:'6-Month FrontrowMD Subscription'}),2499);
ck('  x100 runs BEFORE the term test ($5,094 typed as cents is a term total: / 6)',P({man:5094,planName:'6-Month FrontrowMD Subscription'}),849);
ck('a pilot plan has no MRR',P({man:300000,planKind:'pilot'}),null);
ck('no price anywhere is null, never 0',P({}),null);
ck('term months parse',['Pro','6-Month X','Hero (6 Months)','Core (Annual)','12-Month'].map(W.bdAppTermMonths),[1,6,6,12,12]);

// 2. Subscription choice
const sub=(status,periodEnd,extra)=>Object.assign({status,periodEnd:periodEnd||'',updatedAt:'2026-10-14',planName:'',planKind:''},extra||{});
// Digestive Advantage: an active 6-month row hidden behind a NEWER expired one.
const da=[sub('active','2027-03-14',{id:1,planName:'6-Month FrontrowMD Subscription'}),sub('expired','2026-09-16',{id:2})];
ck('Digestive Advantage: the live row behind a newer expired one wins',W.bdAppChooseSub(da,'2026-11-11').id,1);
// Auric: an old canceled row behind a never-activated sign-up.
const au=[sub('canceled','2026-09-13',{id:1}),sub('pending_activation','',{id:2})];
ck('Auric: pending_activation never beats an older terminal row',W.bdAppChooseSub(au,'2026-11-11').id,1);
ck('  but a lone pending row is chosen (a sign-up never activated)',W.bdAppChooseSub([sub('pending_activation','',{id:9})],'2026-11-11').id,9);
ck('a future row counts as live whatever its period end',W.bdAppChooseSub([sub('future','',{id:3}),sub('expired','2026-01-01',{id:4})],'2026-11-11').id,3);
ck('an active row whose period ended is NOT preferred over a newer row',W.bdAppChooseSub([sub('active','2026-09-01',{id:5}),sub('canceled','2026-10-01',{id:6})],'2026-11-11').id,6);
ck('no rows, no choice',W.bdAppChooseSub([],'2026-11-11'),null);

// 3. Pilot post-pilot price precedence
const pp=(o)=>W.bdAppPilotPost(Object.assign({rollover:null,override:null,overrideRepeats:false,monthDefault:null},o));
ck('rollover price first',pp({rollover:400000,override:300000,overrideRepeats:true,monthDefault:250000}).price,4000);
ck('  then a REPEATING override',pp({override:300000,overrideRepeats:true,monthDefault:250000}).price,3000);
ck('  a one-off override is skipped for the default',pp({override:300000,overrideRepeats:false,monthDefault:250000}).price,2500);

// Brand rows as the SQL returns them.
const row=(id,o)=>Object.assign({ID:id,NM:'B'+id,HC:null,CR:'2026-03-01',DI:null,LR:null,R45:0,SUBS:null,PILOTS:null},o);
const subs=(...a)=>a.join('~');
const B=W.bdAppBuild([
  row(1,{SUBS:subs('11|active|2026-07-01|2027-03-14|2026-10-14|1019400||||6-Month FrontrowMD Subscription|custom|2026-07-01','12|expired|2026-07-01|2026-09-16|2026-10-14|||||Pro|usage|2026-08-01')}),
  row(2,{SUBS:'21|paused|2026-07-01|2026-09-01|2026-10-14||1500|||Hero|usage|2026-07-01'}),
  row(3,{SUBS:'31|pilot||||||||Pilot|pilot|2026-10-01',PILOTS:'301|collecting_data|2026-10-20|527400|||0|250000'}),
  row(4,{SUBS:'41|pilot||||||||Pilot|pilot|2026-10-01',PILOTS:'401|deposit_paid|2026-11-01|300000|400000|350000|1|250000'}),
  row(5,{SUBS:'51|pilot||||||||Pilot|pilot|2026-10-01',PILOTS:'501|awaiting_submit|||||0|150000'}),
  row(6,{DI:'2026-08-28',SUBS:'61|active|2026-07-01|2026-08-05|2026-07-01||200000|||Pro|usage|2026-07-01'}),
  row(7,{SUBS:'71|canceled|2026-07-01|2026-09-13|2026-10-14||149900|||Pro|usage|2026-07-01~72|pending_activation||||||||Pro|usage|2026-10-01'}),
],'2026-11-11',[{HC:'999',DEALS:'5,6'}]);
const by={};B.forEach(b=>by[b.id]=b);
ck('brand 1: the live 6-month row, its term total divided',[by[1].state,by[1].monthly],['active',1699]);
ck('brand 2: paused counts as active, $15 typed as cents → $1,500',[by[2].state,by[2].active,by[2].monthly],['paused',true,1500]);
ck('a current pilot is a pilot, not an active brand',[by[3].state,by[3].active],['pilot',false]);
ck('  with its deposit total and post-pilot price',[by[3].pilot.depositTotal,by[3].pilot.postPrice],[5274,2500]);
ck('an awaiting-deposit pilot is pending',by[5].state,'pending');
// 4. Bulk-day end dates
ck('a discard on a bulk day falls back to the last real period end when earlier',[by[6].endDate,/bulk|batch/.test(by[6].endSrc)],['2026-08-05',true]);
ck('the 2026-10-14 status stamp is a bulk day too',!!W.BDAPP_BULK_DAYS['2026-10-14'],true);
ck('Auric-shaped brand: ended, dated by its last period end (10-14 is the bulk stamp)',[by[7].state,by[7].endDate],['ended','2026-09-13']);
ck('the paging SQL is MOD-paged and never selects demo brands',[/MOD\(b\.ID, 4\) = 2/.test(W.bdAppSql(2)),/COALESCE\(b\.DEMO, FALSE\) = FALSE/.test(W.bdAppSql(0))],[true,true]);
ck('dates come back Eastern',/CONVERT_TIMEZONE\('UTC', 'America\/New_York'/.test(W.bdAppSql(0)),true);
ck('the tables the BD Tracker reads are known to the connection check',['BRANDS','SUBSCRIPTIONS','PILOTS','PLANS','NOTES','PRODUCTS'].every(k=>W.MB_NEEDED_TABLES[k]),true);

// ── Page side ──
const C={console};vm.createContext(C);
vm.runInContext([decl(bd,'var BD_STAGE_LABELS=','};'),decl(bd,'var BD_CHURN_STAGE=',';'),decl(bd,'var BD_CHURN_BOARD=',';'),decl(bd,'var BD_CHURN_TYPE_GROUP=','};'),fn(bd,'_bdMonthOfG'),
  'function f$(v){return "$"+Math.round(v);}',
  bd.slice(bd.indexOf('var BD_APP_CHURN_FROM='),bd.indexOf('\nfunction bdSetChurnTypeGrain'))].join('\n'),C);
const ab=(id,o)=>Object.assign({id,name:'Brand '+id,hubspotCompanyId:'',createdAt:'2025-06-01',signDate:'2025-06-01',discardedAt:'',state:'active',active:true,
  sub:{status:'active'},monthly:1000,everLive:true,lastSubCreated:'2026-06-10',pilot:null,lastReviewAt:'',reviews45:0,endDate:'',endSrc:'',hsDeals:[]},o);
const deal=(id,o)=>Object.assign({id:String(id),dealname:'Deal '+id,dealstage:'closedwon',brand_status:'Signed',amount:1000,closedate:'2025-06-01T04:00:00Z',
  entered_closedwon:'',createdate:'2025-05-01T04:00:00Z',pausechurn_date:'',never_implemented_churn:'',companyId:'',companyName:''},o);

// 5. Summary
const sumBrands=[ab(1,{monthly:2000}),ab(2,{state:'paused',monthly:1000}),ab(3,{state:'starting',monthly:500}),
  ab(4,{state:'pilot',active:false,monthly:null,pilot:{status:'deposit_paid',depositTotal:4388,postPrice:3000,current:true}}),
  ab(5,{state:'pilot',active:false,monthly:null,pilot:{status:'live',depositTotal:2000,postPrice:2500,current:true}}),
  ab(6,{state:'pending',active:false,pilot:{status:'awaiting_submit'}}),
  ab(7,{name:'Vincent',monthly:2000}),ab(8,{name:'FrontrowMD Demo Account',monthly:1300})];
const S=J(C._baSummary(sumBrands));
ck('Active Brands: active + paused + starting, pilots and test accounts out',[S.total,S.active,S.paused,S.starting],[3,1,1,1]);
ck('MRR / ARR / ACV on that book',[S.mrr,S.arr,Math.round(S.acv)],[3500,42000,1167]);
ck('pilots: count, split, upfront and post-pilot sums',[S.pilots.n,S.pilots.deposit_paid,S.pilots.live,S.pilots.upfront,S.pilots.post],[2,1,1,6388,5500]);
ck('Future MRR = MRR + the pilots\u2019 post-pilot price',S.futureMrr,9000);
ck('awaiting deposit is counted separately',S.awaiting,1);

// 6. Join + churn source switch + dedupe
ck('names normalise like norm.py',[C._baNorm('Physician\u2019s Choice (restart)'),C._baNorm('Mila Co.'),C._baNorm('Bloom & Bond - New Deal'),C._baNorm('Zenos Health, Inc.')],['physicianschoice','mila','bloomandbond','zenoshealth']);
ck('  and the join is exact, never a prefix',(()=>{const j=C._baJoin([deal(1,{dealname:'Zen'})],[ab(1,{name:'Zenos Health'})]);return j.byDeal['1'].brands.length;})(),0);
const brands=[
  ab(10,{name:'Pre',hubspotCompanyId:'c10',state:'discarded',active:false,discardedAt:'2026-08-28',endDate:'2026-08-28',lastSubCreated:''}), // app cleanup of a pre-June HubSpot churn
  ab(11,{name:'Post',hubspotCompanyId:'c11',state:'ended',active:false,endDate:'2026-09-20',monthly:1500}),                                // app churn after June, HubSpot also churned
  ab(12,{name:'Live',hubspotCompanyId:'c12'}),                                                                                            // HubSpot churned after June, app still live
  ab(13,{name:'AppOnly',state:'ended',active:false,endDate:'2026-07-15'}),                                                               // app churn, no HubSpot deal
  ab(14,{name:'Old',hubspotCompanyId:'c14',state:'discarded',active:false,discardedAt:'2026-03-01',endDate:'2026-03-01'}),               // ended before June: HubSpot's date rules
  ab(15,{name:'Never',state:'discarded',active:false,everLive:false,discardedAt:'2026-08-28',endDate:'2026-08-28'}),                     // never paid, no HubSpot deal
];
const deals=[
  deal(100,{companyId:'c10',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2026-03-10T04:00:00Z'}),
  deal(101,{companyId:'c11',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2026-09-01T04:00:00Z',never_implemented_churn:'Board Defined Churn'}),
  deal(102,{companyId:'c12',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2026-09-05T04:00:00Z'}),
  deal(103,{companyId:'c14',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2026-07-01T04:00:00Z'}),
  deal(104,{companyId:'c99',dealname:'Nobody',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2026-10-01T04:00:00Z'}),
  deal(105,{companyId:'c10',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2026-03-20T04:00:00Z'}), // second deal, same brand, same month
];
const jn=C._baJoin(deals,brands);
const ch=C._baChurn(deals,brands,jn);
const evs=ch.events.map(e=>[e._src,e._brand?e._brand.name:e.dealname,e.pausechurn_date.slice(0,10)]).sort();
ck('the churn events, by source',evs,[
  ['app','AppOnly','2026-07-15'],
  ['app','Post','2026-09-20'],
  ['hubspot','Nobody','2026-10-01'],
  ['hubspot','Old','2026-07-01'],
  ['hubspot','Pre','2026-03-10'],
]);
ck('  before June: HubSpot, and two churn deals of one brand in one month are ONE event',evs.filter(e=>e[1]==='Pre').length,1);
ck('  an app discard that only cleans up that pre-June churn is not a second one',evs.some(e=>e[0]==='app'&&e[1]==='Pre'),false);
ck('  after June: the app date, not HubSpot\u2019s',evs.find(e=>e[1]==='Post')[2],'2026-09-20');
ck('  a HubSpot churn the app contradicts (still live) is dropped',evs.some(e=>e[1]==='Live'),false);
ck('  a never-paying brand with no HubSpot deal is no churn',evs.some(e=>e[1]==='Never'),false);
ck('  an app churn carries its HubSpot deal\u2019s churn type, else blank',[ch.events.find(e=>e._brand&&e._brand.name==='Post').never_implemented_churn,ch.events.find(e=>e._brand&&e._brand.name==='AppOnly').never_implemented_churn],['Board Defined Churn','']);
ck('  and is signed on the brand\u2019s app signing date',ch.events.find(e=>e._brand&&e._brand.name==='Post').closedate.slice(0,10),'2025-06-01');
const ser=C._baSeries(brands,ch.events,'2026-11');
ck('snapshot: the churned brand is in the book before its churn month and out from it',[ser.snap['2026-08'].b,ser.snap['2026-09'].b],[3,2]);

// 7. Reconciliation classifiers
const rb=[
  ab(20,{name:'NoSub',hubspotCompanyId:'r20',state:'nosub',active:false,monthly:null}),
  ab(21,{name:'AppLive',hubspotCompanyId:'r21',monthly:1750}),
  ab(22,{name:'Restarted',hubspotCompanyId:'r22',monthly:2000}),
  ab(23,{name:'Ended',hubspotCompanyId:'r23',state:'ended',active:false,sub:{status:'canceled'},endDate:'2026-10-01'}),
  ab(24,{name:'Both',hubspotCompanyId:'r24'}),
  ab(25,{name:'Pilot',hubspotCompanyId:'r25',state:'pilot',active:false,monthly:null,pilot:{status:'deposit_paid',postPrice:3000}}),
  ab(26,{name:'Vincent'}),
];
const rd=[
  deal(200,{companyId:'r20',amount:3500}),deal(201,{companyId:'r21',dealstage:'contractsent',brand_status:''}),
  deal(202,{companyId:'r22',dealstage:'3453925110',brand_status:'Churned',pausechurn_date:'2025-01-02T05:00:00Z'}),
  deal(203,{companyId:'r23',amount:1499}),deal(204,{companyId:'r24'}),deal(205,{companyId:'r25',dealstage:'1084214349',brand_status:''}),
  deal(206,{companyId:'r30',dealname:'Ghost Co',amount:999}),
];
const R=J(C._baRecon(rd,rb,C._baJoin(rd,rb)));
const nm=(t)=>R[t].map(r=>r.name);
ck('(1) HubSpot active, not live in the app: no brand, or no subscription',nm('t1'),['NoSub','Ghost Co']);
ck('  sorted by HubSpot MRR, with the app state said',R.t1.map(r=>[r.hsMrr,r.appStatus]),[[3500,'brand, no subscription'],[999,'no app brand']]);
ck('(2) live in the app, not active in HubSpot; pilots at their post-pilot price; test accounts hidden',R.t2.map(r=>[r.name,r.appMrr]),[['Pilot',3000],['Restarted',2000],['AppLive',1750]]);
ck('(3) churned in HubSpot, live in the app',R.t3.map(r=>[r.name,r.hsChurn]),[['Restarted','2025-01-02']]);
ck('(4) HubSpot active, ended in the app',R.t4.map(r=>[r.name,r.appStatus]),[['Ended','canceled']]);
ck('a brand both sides call active is in none of them',['t1','t2','t3','t4'].some(t=>nm(t).indexOf('Both')>=0),false);
ck('each row links the deal and the company',[R.t4[0].dealId,R.t4[0].companyId],['203','r23']);
ck('BD deal associations are read live every load, cache only as fallback',
   /const idsToFetch = dealIds\.slice\(\);/.test(src)&&!/MAX_ASSOC_BATCHES/.test(src),true);
ck('the links use the HubSpot record URLs of the All Deals table',/var BD_HS_URL='https:\/\/app\.hubspot\.com\/contacts\/46815331\/record\/';/.test(bd)&&/BD_HS_URL\+'0-3\/'/.test(bd)&&/BD_HS_URL\+'0-2\/'/.test(bd),true);
// R5. Price mismatch: active on both sides, MRR apart by BD_REC_PRICE_TOL or more, or no app price.
const pb=[
  ab(60,{name:'Same',hubspotCompanyId:'p60',monthly:1000}),
  ab(61,{name:'Close',hubspotCompanyId:'p61',monthly:1050}),
  ab(62,{name:'StaleFee',hubspotCompanyId:'p62',monthly:4000}),
  ab(63,{name:'NoPrice',hubspotCompanyId:'p63',monthly:null,priceNote:'no price'}),
  ab(64,{name:'Dup',hubspotCompanyId:'p64',monthly:1000}),
  ab(65,{name:'LivePilot',hubspotCompanyId:'p65',state:'pilot',active:false,monthly:null,pilot:{status:'live',postPrice:3000}}),
];
const pd=[
  deal(300,{companyId:'p60',amount:1000}),deal(301,{companyId:'p61',amount:1000}),deal(302,{companyId:'p62',amount:9000}),
  deal(303,{companyId:'p63',amount:2500}),deal(304,{companyId:'p64',amount:1000}),deal(305,{companyId:'p64',amount:1000}),
  deal(306,{companyId:'p65',amount:12000}),
];
const PM=J(C._baRecon(pd,pb,C._baJoin(pd,pb)));
ck('(5) price mismatch: biggest gap first; within $100 and live pilots left out',PM.t5.map(r=>[r.name,r.hsMrr,r.appMrr,r.diff]),
  [['StaleFee',9000,4000,5000],['NoPrice',2500,null,2500],['Dup',2000,1000,1000]]);
ck('  the reason column names duplicate deals and a missing price',[PM.t5.find(r=>r.name==='Dup').why,PM.t5.find(r=>r.name==='NoPrice').why],['2 active HubSpot deals','no price']);
ck('  a brand in R5 is in none of R1-R4',['t1','t2','t3','t4'].some(t=>PM[t].length),false);
// One app brand reached by two units (by company and by name) counts once, under the unit named after it.
const sb=[ab(70,{name:'Sister',hubspotCompanyId:'p70',monthly:1000,priceNote:'planp'})];
const sd=[deal(307,{companyId:'p70',dealname:'Mother',companyName:'Mother',amount:3000}),deal(308,{companyId:'p71',dealname:'Sister',companyName:'Sister Co',amount:1200})];
const SM=J(C._baRecon(sd,sb,C._baJoin(sd,sb)));
ck('  a brand two units reach is counted once, under the unit named after it; the other deal shows with app MRR 0',SM.t5.map(r=>[r.name,r.hsMrr,r.appMrr,r.dealId]),[['Mother',3000,0,'307'],['Sister',1200,1000,'308']]);
ck('  price sources read in words',SM.t5.find(r=>r.name==='Sister').why,'plan list price only');
ck('  the tolerance is $100',/var BD_REC_PRICE_TOL=100;/.test(bd),true);
// C3 cohort month (2026-11-12): HubSpot close month of the brand's own deal, nearest the app month within 3.
const cb=[ab(80,{name:'Lag',hubspotCompanyId:'k80',createdAt:'2026-06-03',signDate:'2026-06-03'}),
  ab(81,{name:'Resign',hubspotCompanyId:'k81',createdAt:'2026-09-01',signDate:'2026-09-01'}),
  ab(82,{name:'NoDeal',hubspotCompanyId:'k82',createdAt:'2026-04-10',signDate:'2026-04-10'})];
const cdl=[deal(800,{companyId:'k80',closedate:'2026-05-28T04:00:00Z'}),
  deal(810,{companyId:'k81',dealstage:'3453925110',brand_status:'Churned',closedate:'2025-02-01T05:00:00Z'}),deal(811,{companyId:'k81',closedate:'2026-08-20T04:00:00Z'})];
const cj=C._baJoin(cdl,cb),co=(b,fb,o)=>C._baCohortMonth(Object.assign({_brand:b},o||{}),cj,fb);
ck('cohort: the HubSpot close month replaces the app month a month later',co(cb[0],'2026-06'),'2026-05');
ck('  a re-signed brand takes its nearest deal, not the original signing',co(cb[1],'2026-09'),'2026-08');
ck('  no HubSpot deal: the app month stands',co(cb[2],'2026-04'),'2026-04');
ck('  a HubSpot-sourced churn and a same-row restart keep their own sign',[co(cb[0],'2026-06',{_src:'hubspot'}),co(cb[0],'2026-06',{_restart:'2026-06-03'})],['2026-06','2026-06']);
ck('Never Implemented: a Never Imp type, or no type and never live; Board Defined wins',
  [C._baIsNeverImpl({never_implemented_churn:'Yes - Ghosted'}),C._baIsNeverImpl({never_implemented_churn:'',_brand:{everLive:false}}),
   C._baIsNeverImpl({never_implemented_churn:'Board Defined Churn',_brand:{everLive:false}}),C._baIsNeverImpl({never_implemented_churn:'',_brand:{everLive:true}})],[true,true,false,false]);
ck('  the C3 switch defaults to Excluded, which drops NI churns from the grid',
  /var _bdRetNI='excl';/.test(bd)&&/var _rwCh=\(_bdRetNI==='excl'\)\?_apCh\.filter\(function\(dd\)\{return !_baIsNeverImpl\(dd\);\}\):_apCh;/.test(bd),true);
ck('reconciliation rows carry the linked deal\u2019s create and close dates',[PM.t5[0].createDate!==undefined,typeof PM.t5[0].closeDate],[true,'string']);
ck('  and HubSpot MRR, App MRR and Recurly MRR sit side by side',/\['hsMrr','HubSpot MRR','num'\],\['appMrr','App MRR','num'\],\['recurlyMrr','Recurly MRR','num'\]\]/.test(bd),true);
// Recurly MRR on the page (2026-11-16): the sum over the row's brands Recurly knows.
{
  const rcB=[ab(90,{name:'Billed',hubspotCompanyId:'r90',recurly:{mrr:1850,basis:'invoice',subs:[{plan:'Core',state:'active',mrr:1850,basis:'invoice',planMo:1350,traffic:500}]}}),
    ab(91,{name:'Listed',hubspotCompanyId:'r91',monthly:2000,recurly:{mrr:2000,basis:'price',subs:[{plan:'Pro',state:'active',mrr:2000,basis:'price'}]}}),
    ab(92,{name:'Unknown',hubspotCompanyId:'r92',recurly:{mrr:null,missing:1}}),
    ab(93,{name:'NoId',hubspotCompanyId:'r93'})];
  const rcR=J(C._baRecon([],rcB,C._baJoin([],rcB)));
  const g=(n)=>rcR.t2.filter(r=>r.name===n)[0];
  ck('Recurly MRR: invoice figures plain, list prices labelled, unknown or no id a dash',
    [[g('Billed').recurlyMrr,g('Billed').recurlyBasis],[g('Listed').recurlyMrr,g('Listed').recurlyBasis],g('Unknown').recurlyMrr,g('NoId').recurlyMrr],[[1850,''],[2000,'list price'],null,null]);
  ck('  the hover splits plan and web traffic',g('Billed').recurlyTip,'Core \u00b7 active \u00b7 $1850/mo = plan $1350 + web traffic $500 (latest invoice)');
  ck('  an id Recurly does not know says so on hover',/1 brand with a Recurly id Recurly does not know/.test(g('Unknown').recurlyTip),true);
  ck('  basis words: trial, paused, canceling; invoice says nothing',[C._baRecurlyBasis('trial'),C._baRecurlyBasis('invoice \u00b7 canceled, paused'),C._baRecurlyBasis('invoice')],['trial','canceling, paused','']);
  // R5 on HubSpot vs Recurly (2026-11-16).
  const r5b=[ab(95,{name:'Agree',hubspotCompanyId:'q95',monthly:3000,recurly:{mrr:3000,basis:'invoice',planMo:3000,traffic:0,subs:[]}}),
    ab(96,{name:'RcGap',hubspotCompanyId:'q96',monthly:3000,recurly:{mrr:1500,basis:'invoice',planMo:1000,traffic:500,subs:[]}}),
    ab(97,{name:'AppGap',hubspotCompanyId:'q97',monthly:1000,recurly:{mrr:3000,basis:'invoice',planMo:3000,traffic:0,subs:[]}}),
    ab(98,{name:'NoRc',hubspotCompanyId:'q98',monthly:3000})];
  const r5d=[deal(950,{companyId:'q95',amount:3000}),deal(960,{companyId:'q96',amount:3000}),deal(970,{companyId:'q97',amount:3000}),deal(980,{companyId:'q98',amount:3000})];
  const rcOn=J(C._baRecon(r5d,r5b,C._baJoin(r5d,r5b),true)).t5,rcOff=J(C._baRecon(r5d,r5b,C._baJoin(r5d,r5b),false)).t5;
  ck('R5 with Recurly: HubSpot vs Recurly MRR; a brand with no Recurly figure is listed too',rcOn.map(r=>[r.name,r.diff,r.why]),
    [['NoRc',3000,'no Recurly subscription id in the app'],['RcGap',1500,'Recurly plan $1000 + web traffic $500']]);
  ck('  Recurly down: back to HubSpot vs the app price',rcOff.map(r=>[r.name,r.diff]),[['AppGap',2000]]);
  // R6: known to Recurly but not billing yet (2026-11-16).
  const r6b=[ab(101,{name:'Trialing',hubspotCompanyId:'w1',monthly:3000,recurly:{mrr:0,basis:'trial',idlePrice:2500,starts:'2026-12-01',subs:[{basis:'trial'}]}}),
    ab(102,{name:'Later',hubspotCompanyId:'w2',monthly:3000,recurly:{mrr:0,basis:'future',idlePrice:3000,starts:'2027-01-05',subs:[{basis:'future'}]}}),
    ab(103,{name:'Gone',hubspotCompanyId:'w3',monthly:3000,recurly:{mrr:0,basis:'expired',idlePrice:0,starts:'',subs:[{basis:'expired'}]}})];
  const r6d=[deal(1010,{companyId:'w1',amount:3000}),deal(1020,{companyId:'w2',amount:3000}),deal(1030,{companyId:'w3',amount:3000})];
  const R6=J(C._baRecon(r6d,r6b,C._baJoin(r6d,r6b),true));
  ck('R6: trial / starting later / paused go to their own table with state, start day, price and gap; an ended subscription stays in R5',
    [R6.t6.map(r=>[r.name,r.rcState,r.rcStarts,r.rcPrice,r.rcGap]),R6.t5.map(r=>r.name)],[[['Later','starts later','2027-01-05',3000,0],['Trialing','trial','2026-12-01',2500,500]],['Gone']]);
  ck('  Recurly down: no R6, those brands are R5 rows against the app price as before',J(C._baRecon(r6d,r6b,C._baJoin(r6d,r6b),false)).t6.length,0);
  const sb=[ab(111,{recurly:{mrr:1500,subs:[{basis:'invoice'}]}}),ab(112,{recurly:{mrr:2000,subs:[{basis:'invoice \u00b7 canceled'}]}}),
    ab(113,{recurly:{mrr:0,idlePrice:900,subs:[{basis:'trial'}]}}),ab(114,{recurly:{mrr:0,idlePrice:800,subs:[{basis:'future'}]}}),
    ab(115,{recurly:{mrr:0,idlePrice:700,subs:[{basis:'paused'}]}}),ab(116,{recurly:{mrr:0,subs:[{basis:'expired'}]}}),
    ab(117,{name:'Vincent',recurly:{mrr:9999,subs:[{basis:'invoice'}]}}),ab(118,{recurly:{mrr:null,missing:1}}),ab(119,{})];
  const SR=J(C._baSummaryRc(sb,{recurly:{ok:true,unmatched:{count:2,mrr:4400}}}));
  ck('section 01 on Recurly: billing/trial/starting/paused brands, all four in MRR (the last three at list price); ACV over all; test accounts out',
    [SR.total,SR.billing,SR.trial,SR.starting,SR.paused,SR.billedMrr,SR.idlePrice,SR.mrr,SR.arr,SR.acv,SR.canceling,[SR.idle_trial,SR.idle_starting,SR.idle_paused]],
    [5,2,1,1,1,3500,2400,5900,70800,1180,1,[900,800,700]]);
  ck('  null when Recurly was not read (the cards fall back to the app, labelled)',C._baSummaryRc(sb,{recurly:{ok:false}}),null);
  const st=(o)=>C._baStatusOf(Object.assign({state:'nosub'},o)),RB=(b)=>({mrr:0,subs:[{basis:b}]});
  ck('Status: Recurly first, then the app pilot / onboarding, then ended, else no plan',
    [st({recurly:{mrr:9,subs:[{basis:'invoice'}]}}),st({recurly:RB('paused')}),st({recurly:RB('trial')}),st({recurly:RB('future')}),
     st({pilotPlan:{status:'Pilot \u2013 live'},recurly:RB('expired')}),st({onboarding:true}),st({recurly:RB('expired')}),st({pilotPlan:{status:'Pilot ended'}}),st({state:'ended'}),st({})].map(x=>x.s+'/'+x.d),
    ['Billing/Billing','Paused/Paused','Not billing yet/In trial','Not billing yet/Starting later','Not billing yet/Pilot','Not billing yet/Onboarding',
     'Ended/Recurly subscription ended','Ended/Pilot ended','Ended/App subscription ended','No plan/No plan']);
  const mix=J(C._baStatusMix([ab(1,{recurly:{mrr:9,subs:[{basis:'invoice'}]}}),ab(2,{state:'nosub',onboarding:true}),ab(3,{name:'Vincent'}),ab(4,{discardedAt:'2026-01-01'})]));
  ck('  the mix leaves out test accounts and brands deleted in the app',[mix.Billing.n,mix['Not billing yet'].n,mix['Not billing yet'].d],[1,1,{Onboarding:1}]);
  ck('  the pie: one slice per status in a fixed order, No plan in the absence grey',/var BD_STATUS_ORDER=\['Billing','Not billing yet','Paused','Ended','No plan'\];/.test(bd)&&/'No plan':'#9AA7B4'/.test(bd)&&/if\(_ba\)h\+=_bdStatusPie\(_baStatusMix\(_ba\.brands\)/.test(bd),true);
  ck('  and renders a slice per non-empty status',(C._bdStatusPie(mix,true,true).match(/<path /g)||[]).length,2);
  // R6: a start date already passed is flagged and listed first.
  const r6o=[ab(121,{name:'Stuck',hubspotCompanyId:'z1',monthly:1000,recurly:{mrr:0,basis:'future',idlePrice:1000,starts:'2020-01-01',subs:[{basis:'future'}]}}),
    ab(122,{name:'Big',hubspotCompanyId:'z2',monthly:9000,recurly:{mrr:0,basis:'future',idlePrice:9000,starts:'2099-01-01',subs:[{basis:'future'}]}})];
  const r6od=[deal(1210,{companyId:'z1',amount:1000}),deal(1220,{companyId:'z2',amount:9000})];
  const R6o=J(C._baRecon(r6od,r6o,C._baJoin(r6od,r6o),true)).t6;
  ck('R6: a start date already passed is flagged and sorted first',R6o.map(r=>[r.name,r.rcOverdue,r.rcState]),[['Stuck',true,'\u26a0 start date passed \u2014 check Recurly'],['Big',false,'starts later']]);
  ck('% Paused reads Recurly (brands once each), HubSpot only when Recurly is down',/var _k=_baRcClass|var k=_baRcClass\(b\);if\(!k\|\|k==='ended'\)return;_pzLive\+\+;/.test(bd)&&/_bdChips\(\['hubspot'\]\)/.test(bd),true);
  ck('  the goal dialog takes section 01\u2019s ALL-TIME ARR, whatever its date filter',/BD\._signedArr=_rAll\?_rAll\.arr:\(_baSum\?_baSum\.arr:null\);/.test(bd)&&/var _rAll=_ba\?_baSummaryRc\(_ba\.brands,_ba\.meta\):null;/.test(bd),true);
  ck('  the header says what it nets',/net \(HubSpot \\u2212 '\+\(_bdRecVs\|\|'app'\)\+' MRR\)/.test(bd)&&/_bdRecVs=_r5Rc\?'Recurly':'app';/.test(bd),true);
  ck('the freshness note says where Recurly MRR comes from, and when it is stale or down',/Recurly MRR is '\+\(_rcm\.stale\?/.test(bd)&&/'Recurly MRR is unavailable \('/.test(bd),true);
}
ck('test accounts are an explicit list',/var BD_APP_TEST_BRANDS=\['FrontrowMD Demo Account','Vincent',"Lane's 6\/3 Test",'Example Brand Name'\];/.test(bd),true);
ck('  and the worker copy matches it',/const APP_PILOT_TEST_BRANDS = \['FrontrowMD Demo Account', 'Vincent', "Lane's 6\/3 Test", 'Example Brand Name'\];/.test(src),true);

// 10. Review fixes (2026-11-11)
// Worker: the restart signals and the bulk-day flag the page reads.
const B2=W.bdAppBuild([
  row(40,{DI:'2026-08-28'}),                                                                    // no subscription, discarded in the Aug batch
  row(41,{DI:'2026-08-27'}),                                                                    // no subscription, an ordinary day
  row(42,{DI:'2026-08-28',SUBS:'421|active|2026-07-01|2026-11-14|2026-07-22||200000|||Pro|usage|2026-07-22'}),
  row(43,{SUBS:'431|active|2026-07-14|2026-11-14|2026-07-22||200000|||Pro|usage|2026-07-22~432|active|2026-09-01|2026-12-01|2026-09-01||200000|||Pro|usage|2026-09-01'}),
],'2026-11-11',[]);
const by2={};B2.forEach(b=>by2[b.id]=b);
ck('fix 5: a brand with no subscription discarded on a bulk day has no real end date',[by2[40].endOnBulkDay,by2[41].endOnBulkDay,by2[42].endOnBulkDay],[true,false,false]);
ck('fix 4: 2026-07-22 is a backfill day, and each subscription says whether the backfill created it',[!!W.BDAPP_BACKFILL_DAYS['2026-07-22'],by2[43].subHist.map(x=>[x.start,x.bf])],[true,[['2026-07-14',true],['2026-09-01',false]]]);

const chDeal=(id,co,day,o)=>deal(id,Object.assign({companyId:co,dealstage:'3453925110',brand_status:'Churned',pausechurn_date:day?day+'T04:00:00Z':''},o||{}));
const fx=[
  // fix 4: churned in HubSpot in Jan; its only subscription was CREATED by the 2026-07-22 backfill (start after the churn); discarded in Aug.
  ab(50,{name:'Backfilled',hubspotCompanyId:'f50',state:'discarded',active:false,discardedAt:'2026-08-28',endDate:'2026-08-28',lastSubCreated:'2026-07-22',
    subHist:[{start:'2026-07-14',created:'2026-07-22',status:'active',bf:true}]}),
  // fix 4 control + fix 2: churned in March (Board Defined), REALLY restarted in July (non-backfill row), ended in October.
  ab(51,{name:'Restarted',hubspotCompanyId:'f51',state:'ended',active:false,endDate:'2026-10-05',lastSubCreated:'2026-07-01',
    subHist:[{start:'2026-07-01',created:'2026-07-01',status:'canceled',bf:false}]}),
  // fix 2 control: an app churn with its own HubSpot churn deal 20 days away.
  ab(52,{name:'Near',hubspotCompanyId:'f52',state:'ended',active:false,endDate:'2026-09-20'}),
  // fix 2: a HubSpot churn deal (June) 5 months before the app end (Nov).
  ab(53,{name:'Far',hubspotCompanyId:'f53',state:'ended',active:false,endDate:'2026-11-05'}),
  // fix 3: a discarded never-billed row on the company of a LIVE brand that holds the signed deal.
  ab(54,{name:'Twin-old',hubspotCompanyId:'f54',state:'discarded',active:false,everLive:false,discardedAt:'2026-07-27',endDate:'2026-07-27'}),
  ab(55,{name:'Twin',hubspotCompanyId:'f54'}),
  // fix 3 control: a never-billed discarded row with a signed deal and NO live sibling is still a churn.
  ab(56,{name:'Alone',hubspotCompanyId:'f56',state:'discarded',active:false,everLive:false,discardedAt:'2026-07-27',endDate:'2026-07-27'}),
  // fix 5: no subscription, discarded in the Aug batch; HubSpot dates it 2026-06-04.
  ab(57,{name:'BatchDated',hubspotCompanyId:'f57',state:'discarded',active:false,everLive:false,discardedAt:'2026-08-28',endDate:'2026-08-28',endOnBulkDay:true}),
  // fix 5: same, but HubSpot's churn deal is undated: an undated churn, counted once.
  ab(58,{name:'BatchUndated',hubspotCompanyId:'f58',state:'discarded',active:false,everLive:false,discardedAt:'2026-08-28',endDate:'2026-08-28',endOnBulkDay:true}),
  // fix 1: churned in HubSpot in Jan 2025, came back on the SAME row (real restart starting 2026-07-01), live today.
  ab(59,{name:'Back',hubspotCompanyId:'f59',signDate:'2024-06-01',createdAt:'2024-06-01',
    subHist:[{start:'2026-07-01',created:'2026-06-25',status:'active',bf:false}]}),
];
const fd=[
  chDeal(500,'f50','2026-01-21',{never_implemented_churn:'Board Defined Churn'}),
  chDeal(510,'f51','2026-03-10',{never_implemented_churn:'Board Defined Churn',churn_reason:'old reason'}),
  chDeal(520,'f52','2026-09-01',{never_implemented_churn:'Yes - Ghosted'}),
  chDeal(530,'f53','2026-06-05',{never_implemented_churn:'Board Defined Churn'}),
  deal(540,{companyId:'f54'}),deal(560,{companyId:'f56'}),
  chDeal(570,'f57','2026-06-04',{never_implemented_churn:'Board Defined Churn'}),
  chDeal(580,'f58',''),
  chDeal(590,'f59','2025-01-30'),
];
const fj=C._baJoin(fd,fx),fc=C._baChurn(fd,fx,fj);
const fev=(name)=>fc.events.filter(e=>e._brand&&e._brand.name===name).map(e=>[e._src,e.pausechurn_date.slice(0,10)]);
ck('fix 4: a subscription the backfill created is not a restart: the Aug discard only cleans up the Jan churn',fev('Backfilled'),[['hubspot','2026-01-21']]);
ck('  a real restart (a row created on its own day, starting after the churn) keeps the later app churn',fev('Restarted'),[['app','2026-10-05'],['hubspot','2026-03-10']]);
const evOf=(name,src)=>fc.events.find(e=>e._brand&&e._brand.name===name&&e._src===src);
ck('fix 2: the churn after a restart does NOT inherit the pre-restart deal’s type and reason',[evOf('Restarted','app').never_implemented_churn,evOf('Restarted','app').churn_reason],['','']);
ck('  while the historic March event keeps them',evOf('Restarted','hubspot').never_implemented_churn,'Board Defined Churn');
ck('  an app churn with its own HubSpot deal within 90 days takes that deal’s type',evOf('Near','app').never_implemented_churn,'Yes - Ghosted');
ck('  a HubSpot churn deal 5 months from the app end is another churn: Not recorded',evOf('Far','app').never_implemented_churn,'');
ck('  the churn after a restart is signed at the restart, the March one at the brand’s signing',[evOf('Restarted','app').closedate.slice(0,10),evOf('Restarted','hubspot').closedate.slice(0,10)],['2026-07-01','2025-06-01']);
ck('fix 3: a never-billed discarded duplicate beside a live brand on its company is no churn',fev('Twin-old'),[]);
ck('  without a live sibling the same row is still a churn',fev('Alone'),[['app','2026-07-27']]);
ck('fix 5: a sub-less brand discarded on a bulk day takes HubSpot’s Pause/Churn Date',fev('BatchDated'),[['app','2026-06-04']]);
ck('  with no HubSpot date it is an undated churn, not an event on the batch day',fev('BatchUndated'),[]);
ck('  and is counted once in the undated note (its undated HubSpot deal is the same churn)',fc.undated,1);
const fs1=C._baSurvivors(fx,fj,fc.events);
const back=fs1.find(o=>o._brand.name==='Back');
ck('fix 1: a brand that came back on the same row survives in its RESTART cohort',back.closedate.slice(0,10),'2026-07-01');
ck('  and its old churn stays in the original cohort, so neither cohort holds it twice',[evOf('Back','hubspot').closedate.slice(0,7),back.closedate.slice(0,7)],['2024-06','2026-07']);
ck('  a survivor with no churn behind it keeps its signing date',fs1.find(o=>o._brand.name==='Twin').closedate.slice(0,10),'2025-06-01');
ck('  a live brand with only a backfilled row after its churn survives from that row’s start, never before the churn',
  C._baRestartAfter({createdAt:'2024-01-01',subHist:[{start:'2026-02-01',bf:true}]},'2025-12-01',true),'2026-02-01');

// 8. The route is signed-in only
ck('/api/bd-app is guarded like /api/bd',/url\.pathname === '\/api\/bd-app'\) \{\s*try \{ await request\.json\(\); \} catch \{ return jr\(\{ error: 'Invalid JSON' \}, 400\); \}\s*if \(!\(await authed\(\)\)\) return jr\(\{ error: 'Unauthorized' \}, 401\);/.test(src),true);
ck('  and turns a failure into {error} with a 502',/catch \(err\) \{ console\.error\('BD app endpoint error:', err\); return jr\(\{ error: 'App data unavailable'/.test(src),true);


// 12. The brand book live through Grafana (2026-11-12), else Metabase.
async function appBookTests(){
  const TOKEN='glsa_FAKE_token_456';
  const okResp=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>''});
  const cols=['ID','NM','HC','CR','DI','LR','R45','SUBS','PILOTS'];
  const brandVals=[[7],['Livebrand'],['c7'],['2025-01-01'],[null],['2026-11-01'],[2],['70|active|2025-01-01|2026-12-01|2026-10-01||150000|||Core|usage|2025-01-01'],[null]];
  const gBrand={results:{A:{frames:[{schema:{fields:cols.map(n=>({name:n}))},data:{values:brandVals}}]}}};
  const gPilot={results:{A:{frames:[{schema:{fields:['deposit_paid','collecting_data','live','awaiting','net_fee_cents','post_cents'].map(n=>({name:n}))},data:{values:[[1],[2],[3],[4],[500],[600]]}}]}}};
  const mbBrand={status:'completed',data:{cols:cols.map(n=>({name:n})),rows:[[8,'Nightbrand','c8','2025-01-01',null,'',0,'80|active|2025-01-01|2026-12-01|2026-10-01||100000|||Core|usage|2025-01-01',null]]}};
  const mbAssoc={status:'completed',data:{cols:[{name:'HC'},{name:'DEALS'}],rows:[['c7','d1,d2']]}};
  const mbPilot={status:'completed',data:{cols:['DEPOSIT_PAID','COLLECTING_DATA','LIVE','AWAITING','NET_FEE_CENTS','POST_CENTS'].map(n=>({name:n})),rows:[[1,1,1,1,100,100]]}};
  let mbPages=0;
  const mb=(i)=>{const q=JSON.parse(i.body).native?JSON.parse(i.body).native.query:String(i.body);
    if(/STG_BRANDS b\n/.test(q))mbPages++;
    return okResp(/STG_HUBSPOT_DEAL_COMPANIES/.test(q)?mbAssoc:/FCT_PILOT_PROGRESS/.test(q)?mbPilot:mbBrand);};
  let calls=[];mbPages=0;
  W.fetch=async(u,i)=>{calls.push(u);if(/grafana/.test(u)){const q=JSON.parse(i.body).queries[0].rawSql;return okResp(q===W.BDAPP_SQL_PG?gBrand:gPilot);}return mb(i);};
  const lv=await W.fetchBdApp({GRAFANA_TOKEN:TOKEN,METABASE_API_KEY:'k'});
  ck('token set: brands come live from Grafana, labelled live',[lv.meta.live,lv.brands.map(b=>b.name),lv.brands[0].monthly,lv.meta.rows],[true,['Livebrand'],1500,1]);
  ck('  the HubSpot associations still come from Metabase',[lv.brands[0].hsDeals,calls.some(u=>/metabase/i.test(u))],[['d1','d2'],true]);
  ck('  and the Metabase brand pages are not read',mbPages,0);
  mbPages=0;
  W.fetch=async(u,i)=>{if(/grafana/.test(u))return {ok:false,status:500,body:{cancel(){}},bodyUsed:false,json:async()=>({}),text:async()=>''};return mb(i);};
  const fb=await W.fetchBdApp({GRAFANA_TOKEN:TOKEN,METABASE_API_KEY:'k'});
  ck('  (the fallback reads all '+W.BDAPP_PAGES+' Metabase pages)',mbPages,4);
  ck('Grafana down: the nightly Metabase book, labelled so, reason kept, no token',[fb.meta.live,fb.brands.map(b=>b.name).slice(0,1),/500/.test(fb.meta.liveError),JSON.stringify(fb).indexOf(TOKEN)],[false,['Nightbrand'],true,-1]);
  W.fetch=async(u,i)=>{if(/grafana/.test(u)){const q=JSON.parse(i.body).queries[0].rawSql;
    return okResp(q===W.BDAPP_SQL_PG?{results:{A:{frames:[{schema:{fields:cols.map(n=>({name:n}))},data:{values:cols.map(()=>[])}}]}}}:gPilot);}return mb(i);};
  const em=await W.fetchBdApp({GRAFANA_TOKEN:TOKEN,METABASE_API_KEY:'k'});
  ck('  zero live rows is a broken read: falls back too, never an empty book',[em.meta.live,em.brands.length>0],[false,true]);
  calls=[];W.fetch=async(u,i)=>{calls.push(u);return mb(i);};
  const nt=await W.fetchBdApp({METABASE_API_KEY:'k'});
  ck('no token: Metabase only, never Grafana',[nt.meta.live,calls.every(u=>!/grafana/.test(u))],[false,true]);
  // Company -> deal links are live from HubSpot when a token is set (2026-11-13).
  calls=[];
  W.fetch=async(u,i)=>{calls.push(u);if(/grafana/.test(u)){const q=JSON.parse(i.body).queries[0].rawSql;return okResp(q===W.BDAPP_SQL_PG?gBrand:gPilot);}
    if(/companies\/batch\/read/.test(u))return okResp({results:[{id:'c7',properties:{hs_merged_object_ids:''}}]});
    if(/companies\/deals\/batch\/read/.test(u))return okResp({results:[{from:{id:'c7'},to:[{toObjectId:'d9'}]}]});return mb(i);};
  const la=await W.fetchBdApp({GRAFANA_TOKEN:TOKEN,HUBSPOT_TOKEN:'h',METABASE_API_KEY:'k'});
  ck('company-deal links come live from HubSpot, not the nightly copy',[la.meta.assocLive,la.brands[0].hsDeals,calls.some(u=>/companies\/deals\/batch\/read/.test(u))],[true,['d9'],true]);
  W.fetch=async(u,i)=>{if(/grafana/.test(u)){const q=JSON.parse(i.body).queries[0].rawSql;return okResp(q===W.BDAPP_SQL_PG?gBrand:gPilot);}
    if(/companies\/deals\/batch\/read/.test(u))return {ok:false,status:500,body:{cancel(){}},bodyUsed:false,text:async()=>'',json:async()=>({})};return mb(i);};
  const lb=await W.fetchBdApp({GRAFANA_TOKEN:TOKEN,HUBSPOT_TOKEN:'h',METABASE_API_KEY:'k'});
  ck('  a HubSpot failure falls back to the nightly copy',[lb.meta.assocLive,lb.brands[0].hsDeals],[false,['d1','d2']]);
  // A brand pointing at a MERGED company reads the deals of the surviving record (URO, 2026-11-13).
  W.fetch=async(u,i)=>{if(/grafana/.test(u)){const q=JSON.parse(i.body).queries[0].rawSql;return okResp(q===W.BDAPP_SQL_PG?gBrand:gPilot);}
    if(/companies\/batch\/read/.test(u))return okResp({results:[{id:'new7',properties:{hs_merged_object_ids:'c7;old2'}}]});
    if(/companies\/deals\/batch\/read/.test(u)){const ins=JSON.parse(i.body).inputs.map(x=>x.id);return okResp({results:ins.indexOf('new7')>=0?[{from:{id:'new7'},to:[{toObjectId:'d42'}]}]:[]});}
    return mb(i);};
  const lm=await W.fetchBdApp({GRAFANA_TOKEN:TOKEN,HUBSPOT_TOKEN:'h',METABASE_API_KEY:'k'});
  ck('  a brand on a merged-away company id gets the surviving record\u2019s deals',[lm.meta.assocLive,lm.brands[0].hsDeals],[true,['d42']]);
  const q=W.BDAPP_SQL_PG;
  ck('the live SQL returns the Snowflake column names, upper-case',cols.every(c=>q.indexOf('"'+c+'"')>=0),true);
  ck('  same packing and Eastern days as the nightly query',[/regexp_replace\(p\.name, '\[\|~\]', ' ', 'g'\)/.test(q),/AT TIME ZONE 'UTC'\) AT TIME ZONE 'America\/New_York'/.test(q),/COALESCE\(b\.demo, FALSE\) = FALSE/.test(q)],[true,true,true]);
}
// 13. Recurly MRR (2026-11-16): Recurly's billed monthly amount per brand,
// joined by the app's subscriptions.external_subscription_id.
async function recurlyTests(){
  const KEY='rk_FAKE_recurly_789',NOW='2026-11-16T12:00:00Z';
  const okResp=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>''});
  const bad=(st)=>({ok:false,status:st,body:{cancel(){}},bodyUsed:false,json:async()=>({}),text:async()=>''});
  const M=(sub,plan,inv)=>J(W.recurlySubMrr(Object.assign({id:'s1',state:'active',unit_amount:1500,quantity:1,add_ons:[]},sub),plan||{interval_unit:'months',interval_length:1},inv||null,NOW));
  ck('list price: the plan per month only (ClinicianAI and other add-ons are not MRR)',
    (({mrr,basis,planMo,traffic})=>[mrr,basis,planMo,traffic])(M({add_ons:[{unit_amount:200,quantity:2,add_on:{add_on_type:'fixed'}},{unit_amount:9,quantity:1,add_on:{add_on_type:'usage'}}]})),[1500,'price',1500,0]);
  ck('  a 6-month plan divides by 6; a 365-day plan by ~12',[M({unit_amount:9000},{interval_unit:'months',interval_length:6}).mrr,M({unit_amount:12000},{interval_unit:'days',interval_length:365}).mrr],[1500,1000.68]);
  const inv={id:'i1',number:'1001',line_items:[
    {subscription_id:'s1',type:'charge',origin:'plan',subtotal:1500,discount:150,start_date:'2026-11-01T00:00:00Z',end_date:'2026-12-01T00:00:00Z'},
    {subscription_id:'s1',type:'charge',origin:'usage_add_on',add_on_code:'web-traffic',subtotal:300,discount:0,start_date:'2026-10-01T00:00:00Z',end_date:'2026-11-01T00:00:00Z'},
    {subscription_id:'s1',type:'charge',origin:'add_on',subtotal:200,discount:0,start_date:'2026-10-01T00:00:00Z',end_date:'2026-11-01T00:00:00Z'},
    {subscription_id:'s1',type:'charge',origin:'debit',subtotal:5000,discount:0},
    {subscription_id:'s1',type:'charge',origin:'one_time',subtotal:999,discount:0},
    {subscription_id:'s1',type:'credit',origin:'plan',subtotal:-300,discount:0},
    {subscription_id:'s2',type:'charge',origin:'plan',subtotal:7000,discount:0,start_date:'2026-11-01T00:00:00Z',end_date:'2026-12-01T00:00:00Z'}]};
  const mi=M({},null,inv);
  ck('invoice basis: MRR = plan $/mo + web traffic $/mo after coupons; ClinicianAI, one-off debits, credits and other subscriptions left out',[mi.mrr,mi.planMo,mi.traffic,mi.basis,mi.invoice],[1650,1350,300,'invoice','1001']);
  const fm=M({},null,{subscription_ids:['s1'],line_items:[{subscription_id:'s1',type:'charge',origin:'plan',subtotal:1500,discount:0,start_date:'2026-11-01T00:00:00Z',end_date:'2026-12-01T00:00:00Z'},
    {type:'charge',origin:'debit',description:'Web Traffic Plan \u2013 first month (10K-25K pageviews)',subtotal:500,discount:0},
    {type:'charge',origin:'debit',description:'Clinician Analysis setup fee for product: X',subtotal:299,discount:0},
    {type:'charge',origin:'debit',description:'Pilot Initiation Fee',subtotal:2500,discount:0}]});
  ck('  first paid month: the one-off "Web Traffic Plan \u2013 first month" charge is the traffic; other one-offs are not',[fm.mrr,fm.traffic],[2000,500]);
  ck('  a traffic charge on an invoice shared by several subscriptions is not guessed onto one',M({},null,{subscription_ids:['s1','s9'],line_items:[{subscription_id:'s1',type:'charge',origin:'plan',subtotal:1500,discount:0},
    {type:'charge',origin:'debit',description:'Web Traffic 10K\u201325K',subtotal:500,discount:0}]}).mrr,1500);
  ck('  a 6-month invoice line counts per month',M({},null,{line_items:[{subscription_id:'s1',type:'charge',origin:'plan',subtotal:9000,discount:0,start_date:'2026-11-01T00:00:00Z',end_date:'2027-05-01T00:00:00Z'}]}).mrr,1500);
  ck('  a traffic-only invoice adds the plan from its list price',(({mrr,basis})=>[mrr,basis])(M({},null,{line_items:[{subscription_id:'s1',type:'charge',origin:'usage_add_on',add_on_code:'web-traffic',subtotal:400,discount:0,start_date:'2026-10-01T00:00:00Z',end_date:'2026-11-01T00:00:00Z'}]})),[1900,'invoice+price']);
  ck('trial, paused, future, expired and failed bill nothing',['active','paused','future','expired','failed'].map((st,k)=>(({mrr,basis})=>mrr+':'+basis)(M(k?{state:st}:{trial_ends_at:'2026-12-01T00:00:00Z'},null,inv))),['0:trial','0:paused','0:future','0:expired','0:failed']);
  ck('a future subscription joins the live book when created (its activated_at is the future start)',
    J(W.recurlySubWindow({state:'future',created_at:'2026-11-01T15:00:00Z',activated_at:'2026-12-03T15:00:00Z'})).liveFrom,'2026-11-01');
  ck('  each not-billing state says when billing starts or resumes (Eastern day)',[M({trial_ends_at:'2026-12-01T15:00:00Z'}).starts,M({state:'future',current_period_started_at:'2027-01-05T15:00:00Z'}).starts,M({state:'paused',resume_at:'2026-12-20T15:00:00Z'}).starts],['2026-12-01','2027-01-05','2026-12-20']);
  const ib=[{id:20,recurlySubIds:['t1','e1']}];
  W.recurlyAttach(ib,{subs:{t1:{id:'t1',state:'active',mrr:0,basis:'trial',price:900,starts:'2026-12-01'},e1:{id:'e1',state:'expired',mrr:0,basis:'expired',price:500}}});
  ck('  a brand not billing carries the price it will bill and its start day (ended subscriptions left out)',[ib[0].recurly.basis,ib[0].recurly.idlePrice,ib[0].recurly.starts],['trial',900,'2026-12-01']);
  ck('  canceled still bills to its period end, flagged',(({mrr,basis})=>[mrr,basis])(M({state:'canceled'},null,inv)),[1650,'invoice · canceled']);
  // The fetch: US first, EU on 401/403/404; cursor paging; invoices by id.
  const subsP1={has_more:true,next:'/subscriptions?cursor=abc&limit=200',data:[
    {id:'s1',uuid:'u0000000000000000000000000000001',state:'active',unit_amount:1500,quantity:1,plan:{id:'p1',code:'core',name:'Core'},account:{code:'7',company:'Livebrand',email:'x@y.z',first_name:'A',last_name:'B'},active_invoice_id:'i1',add_ons:[]}]};
  const subsP2={has_more:false,next:null,data:[
    {id:'s2',state:'active',unit_amount:7000,quantity:1,plan:{id:'p1',code:'core',name:'Core'},account:{code:'99',company:'Orphan Co'},active_invoice_id:'i1',add_ons:[]},
    {id:'s3',state:'expired',unit_amount:500,quantity:1,plan:{id:'p1'},account:{code:'8'},add_ons:[]}]};
  let calls=[];
  const rcFetch=(eu)=>async(u,i)=>{calls.push({u,i});
    if(!eu&&/\/\/v3\.recurly\.com/.test(u))return bad(404);
    if(/\/subscriptions\?cursor=abc/.test(u))return okResp(subsP2);
    if(/\/subscriptions\?/.test(u))return okResp(subsP1);
    if(/\/plans\?/.test(u))return okResp({has_more:false,data:[{id:'p1',interval_unit:'months',interval_length:1}]});
    if(/\/invoices\?ids=/.test(u))return okResp({has_more:false,data:[inv]});
    if(/\/line_items\?/.test(u))return okResp({has_more:false,data:[{type:'charge',origin:'one_time',description:'Pilot fee',amount:2500},{type:'charge',origin:'usage_add_on',add_on_code:'traffic',amount:500}]});
    return bad(500);};
  W.fetch=rcFetch(true);
  const bk=J(await W.fetchRecurlyBook({RECURLY_API_KEY:KEY}));
  ck('reads every page, then plans, then the latest invoices by id',[calls.map(c=>c.u.replace(/^https:\/\/[^/]+/,'')),bk.meta.subscriptions,bk.meta.region],
    [['/subscriptions?limit=200&sort=created_at&order=asc','/subscriptions?cursor=abc&limit=200','/plans?limit=200&sort=created_at&order=asc','/invoices?ids=i1'],3,'US']);
  ck('  GET only, Basic auth with the key as username, the versioned Accept header',
    [calls.every(c=>c.i.method==='GET'),calls[0].i.headers.Authorization==='Basic '+Buffer.from(KEY+':').toString('base64'),calls[0].i.headers.Accept],[true,true,'application/vnd.recurly.v2021-02-25+json']);
  ck('  per subscription: MRR, basis, plan and the account’s company, never a person’s name or email',
    [bk.subs.s1.mrr,bk.subs.s2.mrr,bk.subs.s3.mrr,bk.subs.s1.account,JSON.stringify(bk).indexOf('x@y.z'),JSON.stringify(bk).indexOf('first_name')],[1650,7000,0,'Livebrand',-1,-1]);
  calls=[];W.fetch=rcFetch(false);
  const eu=J(await W.fetchRecurlyBook({RECURLY_API_KEY:KEY}));
  ck('an EU site: the US host 404s, the EU host answers',[eu.meta.region,calls[0].u.indexOf('https://v3.recurly.com/'),calls[1].u.indexOf('https://v3.eu.recurly.com/')],['EU',0,0]);
  // Joined to app brands.
  const br=[{id:7,name:'Livebrand',recurlySubIds:['s1']},{id:8,name:'Old',recurlySubIds:['s3']},{id:9,name:'Gone',recurlySubIds:['zz']},{id:10,name:'AppOnly',recurlySubIds:[]},{id:11,name:'ByUuid',recurlySubIds:['u0000000000000000000000000000001']}];
  const un=J(W.recurlyAttach(br,bk));
  const rb=J(br);
  ck('brands get their subscriptions’ MRR; unknown ids are flagged; app-only brands get nothing',[rb[0].recurly.mrr,rb[0].recurly.basis,rb[1].recurly.mrr,rb[2].recurly,rb[3].recurly,rb.every(b=>!b.recurlySubIds),rb[4].recurly.mrr],[1650,'invoice',0,{mrr:null,missing:1},undefined,true,1650]);
  const idle=[{id:12,name:'Restart',recurlySubIds:['s3','sf']}];
  W.recurlyAttach(idle,{subs:{s3:{id:'s3',state:'expired',mrr:0,basis:'expired'},sf:{id:'sf',state:'future',mrr:0,basis:'future'}}});
  ck('  nothing billing: an ended subscription beside one starting later reads "future" only',idle[0].recurly.basis,'future');
  ck('  billing subscriptions no app brand points at are summed as unmatched',[un.count,un.mrr,un.top[0].account],[1,7000,'Orphan Co']);
  // The live read never throws, never leaks the key, and falls back to KV.
  const kv={};const store={get:async(k)=>kv[k]||null,put:async(k,v)=>{kv[k]=v;}};
  W.fetch=rcFetch(true);
  const lv=await W.fetchRecurlyLive({RECURLY_API_KEY:KEY,CONTENT_STORE:store});
  ck('a good read is kept in KV for a later failure',[!!lv.subs,!!kv[W.RECURLY_KV]],[true,true]);
  W.fetch=async()=>{throw new Error('boom '+KEY);};
  const st=await W.fetchRecurlyLive({RECURLY_API_KEY:KEY,CONTENT_STORE:store});
  ck('  Recurly down: the last good read, flagged stale, key scrubbed',[st.meta.stale,/boom \[key\]/.test(st.meta.error),JSON.stringify(st).indexOf(KEY)],[true,true,-1]);
  const none=await W.fetchRecurlyLive({RECURLY_API_KEY:KEY});
  ck('  no KV either: {error}, still no key',[!!none.error,JSON.stringify(none).indexOf(KEY)],[true,-1]);
  ck('the site check is behind sign-in and reports counts only',[/url\.pathname === '\/api\/recurly-check'\) \{\n      let body = \{\}; try \{ body = await request\.json\(\); \} catch \{ body = \{\}; \}\n      if \(!\(await authed\(\)\)\)/.test(src),J(await W.recurlyCheck({})).ok],[true,false]);
  W.fetch=rcFetch(true);
  const chk=J(await W.recurlyCheck({RECURLY_API_KEY:KEY}));
  ck('  it runs, never echoing the key or an email',[chk.ok,chk.region,chk.subscriptions,JSON.stringify(chk).indexOf(KEY),JSON.stringify(chk).indexOf('x@y.z')],[true,'US',3,-1,-1]);
  ck('  one-time charges grouped by description, add-on lines by code',[chk.lineItems90d.oneTime['one_time \u00b7 Pilot fee'],chk.lineItems90d.addOnLines['traffic \u00b7 charge']],[{n:1,amount:2500},{n:1,amount:500}]);
  ck('  a brand id must be a positive integer (it goes into SQL)',[J(await W.recurlyCheck({RECURLY_API_KEY:KEY},'1 or 1=1')).error,J(await W.recurlyCheck({RECURLY_API_KEY:KEY},-3)).error],['brandId must be a positive integer','brandId must be a positive integer']);
  ck('  no secret: not connected, no Recurly call',J(await W.fetchRecurlyLive({})),{error:'not connected'});
  // Pilot plan status, Expected MRR and Net add-on revenue (Alex, 2026-11-16).
  const pp=(st,o)=>Object.assign({status:st,appOnly:true,override:null,listPrice:250000},o||{});
  ck('plan status: an open pilot labels the brand; Expected MRR = override, else the price list',
    [J(W.bdAppPilotPlan([],[pp('awaiting_submit')])),J(W.bdAppPilotPlan([],[pp('collecting_data',{override:180000})])),J(W.bdAppPilotPlan([],[pp('live')]))],
    [{status:'Pilot \u2013 fee not paid',expected:2500},{status:'Pilot \u2013 collecting data',expected:1800},{status:'Pilot \u2013 live',expected:2500}]);
  ck('  an ended pilot is "Pilot ended" with no Expected MRR; an app sign-up waiting to activate outranks it',
    [J(W.bdAppPilotPlan([],[pp('canceled')])),W.bdAppPilotPlan([{status:'pending_activation',extSubId:'',planKind:'usage'}],[pp('opted_out')])],[{status:'Pilot ended',expected:null},null]);
  ck('  a pilot on a Recurly subscription (rolled over) is not a pilot plan',W.bdAppPilotPlan([],[pp('live',{appOnly:false})]),null);
  const net=J(W.recurlyAddOnNet([
    {type:'charge',origin:'debit',description:'Pilot Initiation Fee',subtotal:2500,discount:0,state:'invoiced'},
    {type:'charge',origin:'debit',description:'Clinician Analysis for product: X',subtotal:299,discount:0,state:'invoiced'},
    {type:'charge',origin:'debit',description:'Clinician Reviews',subtotal:500,discount:100,state:'invoiced'},
    {type:'charge',origin:'add_on',add_on_code:'clinician-ai',subtotal:99,discount:0,state:'invoiced'},
    {type:'credit',origin:'debit',description:'Refund',subtotal:-299,discount:0,state:'invoiced',refund:true,original_line_item_invoice_id:'iv1'},
    {type:'credit',origin:'debit',description:'Refund applied',subtotal:-299,discount:0,state:'invoiced',legacy_category:'applied_credit',refund:true},
    {type:'credit',origin:'debit',description:'Not a refund',subtotal:-50,discount:0,state:'invoiced'},
    {type:'credit',origin:'credit',subtotal:-1000,discount:0,state:'invoiced'},
    {type:'charge',origin:'plan',subtotal:1500,discount:0,state:'invoiced'},
    {type:'charge',origin:'usage_add_on',add_on_code:'web-traffic',subtotal:500,discount:0,state:'invoiced'},
    {type:'charge',origin:'debit',description:'Web Traffic Plan \u2013 first month (0-10K pageviews)',subtotal:300,discount:0,state:'invoiced'},
    {type:'charge',origin:'debit',description:'Pilot Initiation Fee',subtotal:2500,discount:0,state:'pending'}]));
  ck('net add-on revenue: every invoiced charge but plan and web traffic, after discounts, less refunds on them; general credits, applied credits, unmarked credits and pending lines out',
    [net.net,net.pilotFee,net.analyses,net.reviews,net.clinicianAi,net.refunds],[2999,2500,299,400,99,-299]);
  const pbr=[{id:1,pilotPlan:{status:'Pilot \u2013 collecting data',expected:2500},recurlyAccount:'acc1'},
    {id:2,pilotPlan:{status:'Pilot \u2013 fee not paid',expected:2000},recurlyAccount:''},
    {id:3,pilotPlan:{status:'Pilot \u2013 live',expected:3000},recurlyAccount:'acc3',recurly:{subs:[{state:'active'}]}},
    {id:4,pilotPlan:{status:'Pilot ended',expected:null},recurlyAccount:'acc4',recurly:{subs:[{state:'expired'}]}},
    {id:5,pilotPlan:null,recurlyAccount:'acc5'}];
  calls=[];W.fetch=async(u,i)=>{calls.push(u);
    if(/\/accounts\/acc1\/line_items/.test(u))return okResp({has_more:false,data:[{type:'charge',origin:'debit',description:'Pilot Initiation Fee',subtotal:2500,discount:0,state:'invoiced'}]});
    if(/\/accounts\/acc4\/line_items/.test(u))return okResp({has_more:false,data:[{type:'charge',origin:'debit',description:'Pilot Initiation Fee',subtotal:3000,discount:0,state:'invoiced'},{id:'c1',type:'credit',origin:'debit',subtotal:-3000,discount:0,state:'invoiced',refund:true},{id:'c1',type:'credit',origin:'debit',subtotal:-3000,discount:0,state:'invoiced',refund:true},{id:'c2',type:'credit',origin:'debit',subtotal:-3000,discount:0,state:'invoiced',original_line_item_invoice_id:'x'}]});
    return bad(500);};
  const ps=J(await W.bdPilotPlanStats({RECURLY_API_KEY:KEY},pbr,{host:'https://v3.recurly.com'}));
  ck('pilot-plan brands: a live Recurly subscription takes the brand out; Expected MRR and Net add-on revenue summed',
    [ps.brands,ps.expected,ps.expectedBrands,ps.net,ps.accounts,ps.noAccount,ps.byStatus['Pilot ended']],[3,2500,1,2500,2,1,{n:1,expected:0,expectedN:0,net:0}]);
  ck('  Post-Pilot MRR leaves out fee-not-paid pilots; their Expected MRR stays in the by-status detail',ps.byStatus['Pilot \u2013 fee not paid'],{n:1,expected:2000,expectedN:1,net:0});
  ck('  a fully refunded ended pilot nets $0: a repeated line counts once, refunds never exceed charges',ps.byStatus['Pilot ended'].net,0);
  ck('  only the population\u2019s Recurly accounts are read, by account id',calls.map(u=>u.replace(/^https:\/\/[^/]+/,'').split('?')[0]).sort(),['/accounts/acc1/line_items','/accounts/acc4/line_items']);
  const pn=J(await W.bdPilotPlanStats({},pbr.slice(0,2),null));
  ck('  Recurly down: Expected MRR still counts, net is null with the reason',[pn.expected,pn.net,pn.netError],[2500,null,'Recurly unavailable']);
  const ob=W.bdAppBrand({ID:5,NM:'Onb',SUBS:'1|pending_activation||||||1200000||6-Month X|custom|2026-11-01||~2|pending_activation||||||100000||Pro|usage|2026-11-01||~3|pending_activation||||||99900||Pro|usage|2026-11-01||rcid123'},'2026-11-16');
  ck('onboarding: app-only sign-ups waiting to activate; expected = display price, a custom plan\u2019s ÷ its term; a Recurly-linked row is not onboarding',[ob.onboarding,ob.onboardingExpected],[true,3000]);
  // The app side carries the Recurly id.
  ck('both app queries pack external_subscription_id as the 14th field',[/s\.initial_web_traffic_tier'\)\}, \$\{_pgS\("regexp_replace\(s\.external_subscription_id/.test(src),/_bdS\('s\.INITIAL_WEB_TRAFFIC_TIER'\)\}, \$\{_bdS\('s\.EXTERNAL_SUBSCRIPTION_ID'\)\}/.test(src)],[true,true]);
  ck('  and the parser reads it',J(W.bdAppParseSubs('1|active||||||||P|usage||t1|abc123xyz'))[0].extSubId,'abc123xyz');
}
// 11. Pilot stats (Alex, 2026-11-11): the app's Grafana "Pilot Brands" board.
async function pilotTests(){
  const TOKEN='glsa_FAKE_token_123';
  const okResp=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>''});
  // Grafana's columnar frame for the aggregate row (live values of 2026-11-11).
  const frame={results:{A:{frames:[{schema:{fields:[{name:'deposit_paid'},{name:'collecting_data'},{name:'live'},{name:'awaiting'},{name:'net_fee_cents'},{name:'post_cents'}]},
    data:{values:[[24],[53],[6],[55],[33260600],[25965000]]}}]}}};
  const mbRow={status:'completed',data:{cols:['DEPOSIT_PAID','COLLECTING_DATA','LIVE','AWAITING','NET_FEE_CENTS','POST_CENTS'].map(n=>({name:n})),rows:[[25,49,4,53,31123900,24415000]]}};
  let calls=[];
  W.fetch=async(u,i)=>{calls.push({u,i});return /grafana/.test(u)?okResp(frame):okResp(mbRow);};
  const live=await W.fetchBdPilotStats({GRAFANA_TOKEN:TOKEN,METABASE_API_KEY:'k'});
  const g=calls[0],body=JSON.parse(g.i.body);
  ck('token set: one call, to Grafana\u2019s /api/ds/query, POST',[calls.length,g.u,g.i.method],[1,'https://grafana.app.thefrontrowhealth.com/api/ds/query','POST']);
  ck('  with the bearer token and the app datasource uid',[g.i.headers.Authorization,body.queries[0].datasource.uid],['Bearer '+TOKEN,'ed1e5e96-111d-4ed9-878b-c4a9be763751']);
  ck('  and the request shape Grafana takes',[body.queries.length,body.queries[0].refId,body.queries[0].format,body.from,body.to,body.queries[0].rawSql===W.BD_PILOT_SQL_PG],[1,'A','table','now-1h','now',true]);
  ck('the frame parses: 83 pilots = 77 waiting + 6 live',[live.source,live.n,live.waiting,live.live,live.deposit_paid,live.collecting_data],['live',83,77,6,24,53]);
  ck('  net fees $332,606, awaiting 55, post-pilot $259,650',[live.netFees,live.awaiting,live.post],[332606,55,259650]);
  ck('the token is nowhere in the result',JSON.stringify(live).indexOf(TOKEN),-1);
  ck('grafanaFrameRows turns columns into rows',J(W.grafanaFrameRows({results:{A:{frames:[{schema:{fields:[{name:'a'},{name:'b'}]},data:{values:[[1,2],['x','y']]}}]}}})),[{a:1,b:'x'},{a:2,b:'y'}]);
  let fe='';try{W.grafanaFrameRows({results:{A:{error:'db error'}}});}catch(e){fe=e.message;}
  ck('  a query error in the frame throws',/db error/.test(fe),true);
  // Grafana down: falls back to the Metabase copy, labelled nightly.
  calls=[];let drained=0;
  W.fetch=async(u,i)=>{calls.push({u,i});return /grafana/.test(u)?{ok:false,status:503,body:{cancel(){drained++;}},bodyUsed:false,json:async()=>({}),text:async()=>''}:okResp(mbRow);};
  const fb=await W.fetchBdPilotStats({GRAFANA_TOKEN:TOKEN,METABASE_API_KEY:'k'});
  ck('a Grafana failure falls back to Metabase, source nightly',[calls.length,/metabase/.test(calls[1].u),fb.source,fb.n,fb.netFees],[2,true,'nightly',78,311239]);
  ck('  the failed body is drained, the reason kept, and no token in it',[drained,/503/.test(fb.liveError),JSON.stringify(fb).indexOf(TOKEN)],[1,true,-1]);
  W.fetch=async(u,i)=>{if(/grafana/.test(u))throw new Error('connect failed for '+TOKEN);return okResp(mbRow);};
  const fb2=await W.fetchBdPilotStats({GRAFANA_TOKEN:TOKEN,METABASE_API_KEY:'k'});
  ck('  an error that echoes the token is scrubbed before it is kept',[fb2.source,JSON.stringify(fb2).indexOf(TOKEN),/\[token\]/.test(fb2.liveError)],['nightly',-1,true]);
  calls=[];W.fetch=async(u,i)=>{calls.push(u);return okResp(mbRow);};
  const nt=await W.fetchBdPilotStats({METABASE_API_KEY:'k'});
  ck('no token: straight to Metabase, nightly, never Grafana',[calls.every(u=>!/grafana/.test(u)),nt.source,nt.liveError],[true,'nightly',undefined]);
  ck('the Metabase query reads the same view\u2019s copy',/MARTS\.FCT_PILOT_PROGRESS/.test(W.BD_PILOT_SQL_SF)&&/STAGING\.STG_PILOTS/.test(W.BD_PILOT_SQL_SF),true);
  // Definitions, the same on both paths.
  const both=[W.BD_PILOT_SQL_PG,W.BD_PILOT_SQL_SF];
  ck('demo/test brand names are left out on both paths',[/brand_name !~\* '\(demo\|test\)'/.test(both[0]),/REGEXP_LIKE\(v\.BRAND_NAME, '\.\*\(demo\|test\)\.\*', 'is'\)/.test(both[1])],[true,true]);
  ck('fees: paid fee less refunds over ALL statuses (no status filter on that sum)',
    [/sum\(v\.pilot_fee_total_in_cents - COALESCE\(v\.refunded_in_cents, 0\)\) FILTER \(WHERE v\.pilot_fee_paid_at IS NOT NULL\)/.test(both[0]),
     /IFF\(v\.PILOT_FEE_PAID_AT IS NOT NULL, v\.PILOT_FEE_TOTAL_IN_CENTS - COALESCE\(v\.REFUNDED_IN_CENTS, 0\), NULL\)/.test(both[1])],[true,true]);
  ck('post-pilot price: rollover, else a repeating override, else the deposit-month default, over the counted pilots',
    both.every(q=>/ROLLOVER_PRICE_IN_CENTS, 0\)/i.test(q)&&/NEXT_PRICE_OVERRIDE_REPEATS/i.test(q)&&/DEPOSIT_MONTH_DEFAULT_IN_CENTS, 0\)/i.test(q)&&/'deposit_paid','collecting_data','live'/.test(q)),true);
  ck('awaiting = awaiting_submit',both.every(q=>/= 'awaiting_submit'/i.test(q)),true);
  ck('the stats map keys of either case',J(W.bdPilotStatsOf({DEPOSIT_PAID:1,COLLECTING_DATA:2,LIVE:3,AWAITING:4,NET_FEE_CENTS:150,POST_CENTS:250},'nightly')).n,6);
  let me='';try{W.bdPilotStatsOf({},'live');}catch(e){me=e.message;}
  ck('  a missing column is an error, not a zero',/no deposit_paid/.test(me),true);
  ck('URL and datasource are constants, the token is an optional env secret',
    [W.GRAFANA_URL,W.GRAFANA_APP_DS,/if \(env && env\.GRAFANA_TOKEN\)/.test(src)],['https://grafana.app.thefrontrowhealth.com','ed1e5e96-111d-4ed9-878b-c4a9be763751',true]);
  ck('the token is never logged',/console\.[a-z]+\([^)]*GRAFANA_TOKEN/.test(src),false);
  // The pilot stats ride on /api/bd-app without taking the book down.
  W.fetch=async(u,i)=>{const q=JSON.parse(i.body).native.query;
    if(/FCT_PILOT_PROGRESS/.test(q))return okResp({status:'failed',error:'pilot view missing'});
    return okResp(/STG_HUBSPOT_DEAL_COMPANIES/.test(q)?{status:'completed',data:{cols:[{name:'HC'},{name:'DEALS'}],rows:[]}}
      :{status:'completed',data:{cols:[{name:'ID'},{name:'NM'},{name:'CR'}],rows:[[1,'x','2026-01-01']]}});};
  const bk=await W.fetchBdApp({METABASE_API_KEY:'k'});
  ck('a pilot-stats failure leaves the brand book intact and says so',[bk.brands.length,bk.pilotStats.error],[4,'Pilot stats unavailable']);
  ck('the page shows a band, not zeros, when the pilot stats are missing',/if\(!_ps\)\{\s*h\+='<div class="bd-app-band err"/.test(bd),true);
  ck('each pilot card prints its source as a chip (app live, or the nightly copy)',/var _psSrc=_chips\(\['app'\],_psLive\);/.test(bd)&&/\+\(src\|\|_psSrc\)\+'<\/div>'/.test(bd),true);
  ck('every section 01 card carries a source chip, section 02 mini cards HubSpot; one colour per source',
    /\+\(src\|\|_chips\(\['app'\],_appLive\)\)\+'<\/div>'/.test(bd)&&/_bdChip\('hubspot',true\)/.test(bd)&&/\.bd-chip\.app\{/.test(bd)&&/\.bd-chip\.hubspot\{/.test(bd)&&/\.bd-chip\.recurly\{/.test(bd),true);
  ck('  Initiation Fees reads Recurly + app',/_rcOk\?_ppSrc\(true\):_psSrc\);/.test(bd)&&/_chips\(withRc\?\['recurly','app'\]:\['app'\],_appLive\)/.test(bd),true);
  // Worker: the brand's pilotPaid flag the toggle reads.
  const PB=W.bdAppBuild([
    row(60,{PILOTS:'601|canceled|2026-09-01|300000|||0|250000'}),        // canceled AFTER paying (DTC Brands)
    row(61,{PILOTS:'611|canceled|||||0|250000'}),                         // canceled before paying
    row(62,{PILOTS:'621|rolled_over|2026-05-01|300000|400000||0|250000'}),
    row(63,{PILOTS:'631|awaiting_submit|||||0|150000'}),
  ],'2026-11-11',[]);
  ck('pilotPaid: a paid pilot that has not rolled over; a rolled-over brand rejoins the main book',PB.map(b=>b.pilotPaid),[true,false,false,false]);
}

// 12. Section 02 on HubSpot with the pilot toggle: run the shipped block.
function pageToggleTests(){
  const a=bd.indexOf('  // Kept for the goal dialog');
  const b=bd.indexOf("h+='</section>';",bd.indexOf("id=\"bd-sec-arr\""))+"h+='</section>';".length;
  const block=bd.slice(a,b);
  const T={console};vm.createContext(T);
  vm.runInContext([fn(bd,'_bdMonthOfG'),fn(bd,'_bdMDY'),fn(bd,'_bdMonthEndMDY'),fn(bd,'_baPilotDealIds'),fn(bd,'_baIsTest'),fn(bd,'_bdChip'),fn(bd,'_bdChips'),fn(bd,'_bdEsc'),fn(bd,'_baToday'),
    fn(bd,'_bdS2Months'),fn(bd,'_baRcClass'),fn(bd,'_baStatusOf'),fn(bd,'_bdRcBillsOn'),fn(bd,'_bdRcLiveOn'),fn(bd,'_bdS2FromBilled'),fn(bd,'_bdS2FromSigned'),fn(bd,'_bdRcVal'),fn(bd,'_bdS2Billed'),fn(bd,'_bdS2Signed'),fn(bd,'_bdS2Week'),fn(bd,'_bdS2Weeks'),fn(bd,'_bdS2Chg'),fn(bd,'_bdS2Chart'),fn(bd,'_bdS2WhoBilled'),fn(bd,'_bdS2Table'),decl(bd,'var _bdS2T=',';'),decl(bd,'var BD_WF_MN=',';'),decl(bd,'var BD_HS_URL=',';'),decl(bd,'var BD_PILOT_FEE_UNPAID=',';'),
    decl(bd,"var _bdS2Mode=",';'),decl(bd,"var BD_S2_FROM=",';'),
    decl(bd,'var BD_APP_TEST_BRANDS=',';'),decl(bd,"var _bdPilotMode=",';'),
    'function f$(v){return "$"+Math.round(v);}','function buildSVGChart(l,v){return "<svg data-n=\\""+v.length+"\\"></svg>";}',
    'var _bdChartMode="current";',
    'function _bdStatusPie(){return "<div class=\\"bd-bs\\"></div>";}function _baStatusMix(){return {};}',
    'var BD={};function run(deals,app){BD={app:app};var _appLive=true;var _baBand=function(w){return "<band>"+w+"</band>";};var _ba=(app&&!app.error)?app:null;var _baSum=null;var _rAll=null;var _s2Cur=null;var _baSer={snap:{"2026-10":{b:9,a:9}}};'
      +'var closedWonDeals=deals.filter(function(dd){return dd.dealstage==="closedwon";});var h="";'
      +'var _baJ={byDeal:{}};if(_ba)deals.forEach(function(dd){_baJ.byDeal[dd.id]={brands:(_ba.map[dd.id]||[])};});'
      +block+'\nreturn {chartDeals:chartDeals.map(function(d){return d.id;}),snap:_snByMonth,app:_snByMonthApp,wk:_wkTotal,h:h,cur:_chartMonthMap};}'].join('\n'),T);
  ck('Billing Status sits in section 02, after ARR by Week',bd.indexOf('h+=_bdStatusPie(')>bd.indexOf("aria:'New ARR by week'")&&bd.indexOf('h+=_bdStatusPie(')<b,true);
  ck('the toggle defaults to Excluding Pilots',T._bdPilotMode,'exclude');
  const pb={id:1,name:'P',pilotPaid:true},nb={id:2,name:'N',pilotPaid:false},tb={id:3,name:'Vincent',pilotPaid:true};
  const D=[
    deal(1,{closedate:'2026-09-03T04:00:00Z',amount:1000}),               // pilot brand
    deal(2,{closedate:'2026-09-10T04:00:00Z',amount:2000}),               // regular brand
    deal(3,{closedate:'2026-10-02T04:00:00Z',amount:500,brand_status:'Paying'}), // test account with a paid pilot: never a pilot deal
    deal(4,{dealstage:'3453925110',brand_status:'Churned',entered_closedwon:'2026-08-01T04:00:00Z',pausechurn_date:'2026-10-15T04:00:00Z',amount:700}), // churned pilot
    deal(5,{brand_status:'Paused',closedate:'2026-08-01T04:00:00Z',amount:9}),  // Paused: never in the current line
  ];
  const app={brands:[pb,nb,tb],map:{'1':[pb],'2':[nb],'3':[tb],'4':[pb],'5':[nb]}};
  const ex=T.run(D,app);
  ck('Excluding Pilots: the HubSpot Paying/Signed deals that are not pilot deals',ex.chartDeals,['2','3']);
  ck('  the HubSpot series is back: months by closedate, amount x 12',[ex.cur['2026-09'].added,ex.cur['2026-09'].addedMRR,ex.cur['2026-10'].added],[1,2000,1]);
  ck('  and its snapshot add-back skips the churned pilot too',ex.snap['2026-09'].b,1);
  ck('  ARR by Week follows the toggle',ex.wk,(2000+500)*12);
  // Section 02 stacked (2026-11-16): one Billed / Signed toggle, Pilot on top, from 2026.
  ck('section 02: one toggle, Billed Active Brands (default) / Signed Brands Snapshot; no Current/Snapshot or pilot toggles drawn',
    [/aria-checked="true"[^>]*>Billed Active Brands/.test(ex.h),/Signed Brands Snapshot/.test(ex.h),/bdSetPilotMode|_bdChartMode=/.test(ex.h)],[true,true,false]);
  ck('  Billed needs Recurly: without it a band says so instead of zeros',/Recurly unavailable/.test(ex.h),true);
  T._bdS2Mode='signed';
  const sg=T.run(D,app);
  ck('  Signed draws stacked charts (base + pilot bands) with HubSpot, Recurly and app chips',[(sg.h.match(/class="bd-stk"/g)||[]).length,/bd-chip hubspot/.test(sg.h)],[3,true]);
  T._bdS2Mode='billed';
  const none=T.run(D,null);
  ck('no app data yet: the section shows the loading band, never zeros',/<band>the section 02 charts<\/band>/.test(none.h),true);
  const sm=T._bdS2Months('2026-03-17');
  ck('months run from Jan 2026 to this month, which ends today',sm,[{k:'2026-01',end:'2026-01-31'},{k:'2026-02',end:'2026-02-28'},{k:'2026-03',end:'2026-03-17'}]);
  const rcb=(id,subs,o)=>Object.assign({id,name:'B'+id,recurly:{mrr:1,subs}},o||{});
  const bil=T._bdS2Billed([
    rcb(1,[{billFrom:'2026-01-10',billTo:'',pausedAt:'',mrr:1000,price:900}]),
    rcb(2,[{billFrom:'2026-02-05',billTo:'2026-03-01',pausedAt:'',mrr:0,price:500}]),
    rcb(3,[{billFrom:'2026-03-20',billTo:'',pausedAt:'',mrr:2000,price:2000}],{pilotHist:[{from:'2026-01-15',to:'2026-03-20',price:1500}]}),
    rcb(4,[],{pilotHist:[{from:'2026-02-10',to:'',price:3000}]}),
    rcb(5,[{billFrom:'2026-01-02',billTo:'',pausedAt:'2026-02-15',mrr:0,price:700}]),
    {id:6,name:'Vincent',recurly:{mrr:1,subs:[{billFrom:'2026-01-01',mrr:9999}]}}],sm.concat([{k:'2026-04',end:'2026-04-30'}]));
  ck('Billed: base = brands live in Recurly at month-end (billing, trial, starting, paused; not ended), as section 01 counts; pilots on top until live; test accounts out',
    [bil.base,bil.pil,bil.baseA,bil.pilA],[[2,3,2,3],[1,2,2,1],[1700*12,2200*12,1700*12,3700*12],[1500*12,4500*12,4500*12,3000*12]]);
  // Brand lists under the charts (Alex, 2026-11-24): each month's rows add up to the chart's point.
  {const BS=[rcb(1,[{billFrom:'2026-01-10',billTo:'',pausedAt:'',mrr:1000,price:900}]),
    rcb(2,[{billFrom:'2026-02-05',billTo:'2026-03-01',pausedAt:'',mrr:0,price:500}]),
    rcb(3,[{billFrom:'2026-03-20',billTo:'',pausedAt:'',mrr:2000,price:2000}],{pilotHist:[{from:'2026-01-15',to:'2026-03-20',price:1500}]}),
    rcb(4,[],{pilotHist:[{from:'2026-02-10',to:'',price:3000}]}),
    rcb(5,[{billFrom:'2026-01-02',billTo:'',pausedAt:'2026-02-15',mrr:0,price:700}]),
    {id:6,name:'Vincent',recurly:{mrr:1,subs:[{billFrom:'2026-01-01',mrr:9999}]}}],MS=sm.concat([{k:'2026-04',end:'2026-04-30'}]);
   const ok=MS.every((mo,i)=>{const w=T._bdS2WhoBilled(BS,mo,false,null),b=w.filter(r=>r.ly==='base'),p=w.filter(r=>r.ly==='pil');
     return b.length===bil.base[i]&&p.length===bil.pil[i]&&Math.round(b.reduce((t,r)=>t+r.mrr,0)*12)===bil.baseA[i]&&Math.round(p.reduce((t,r)=>t+r.mrr,0)*12)===bil.pilA[i];});
   ck('  the brand list for every month ties to the chart, brand count and ARR, base and pilot',ok,true);
   const w=T._bdS2WhoBilled(BS,MS[1],false,null);
   ck('  each row names the brand, its layer, status and since',w.map(r=>[r.name,r.ly,r.st,r.since]),[['B1','base','Billing','2026-01-10'],['B2','base','Billing','2026-02-05'],['B3','pil','In a paid pilot','2026-01-15'],['B4','pil','In a paid pilot','2026-02-10'],['B5','base','Paused','2026-01-02']]);
   const ob=T._bdS2WhoBilled([{id:30,name:'Onb',state:'pending',onboarding:true,onboardingFrom:'2026-02-10',onboardingExpected:2000}],MS[2],false,null);
   ck('  onboarding sign-ups are listed as their own layer',ob.map(r=>[r.name,r.ly,r.mrr]),[['Onb','onb',2000]]);
   T._bdS2T={n:{open:true,m:'2026-02',ly:''},a:{open:false,m:'',ly:''}};
   const tb=T._bdS2Table('n',{months:MS,today:'2026-04-30',who:(mo,cur)=>T._bdS2WhoBilled(BS,mo,cur,null),bLbl:'Recurly',money:false,pt:i=>bil.base[i]+bil.pil[i]});
   ck('  the table opens on the picked month and says it matches the chart',/Brands behind this chart · Feb 2026: <b>5<\/b> brands/.test(tb)&&/✔ the chart’s Feb 2026 point/.test(tb)&&/<option value="2026-02" selected>/.test(tb),true);}
  const bv=T._bdS2Billed([rcb(7,[{billFrom:'2026-01-05',billTo:'',pausedAt:'',mrr:1500,price:1500},{liveFrom:'2026-03-01',billFrom:'',billTo:'',pausedAt:'',mrr:0,price:900}])],sm);
  ck('  a billing brand with a second subscription starting later counts only what it bills (as section 01)',bv.baseA,[1500*12,1500*12,1500*12]);
  ck('  a subscription billing $0 (100% coupon) counts $0, not its list price',[T._bdRcVal({basis:'invoice',mrr:0,price:1999}),T._bdRcVal({basis:'expired',mrr:0,price:1999})],[0,1999]);
  const ob2=T._bdS2Billed([{id:30,name:'Onb',state:'pending',onboarding:true,onboardingFrom:'2026-02-10',onboardingExpected:2000},
    {id:31,name:'OnbDel',state:'pending',onboarding:true,onboardingFrom:'2026-01-05',onboardingExpected:900,discardedAt:'2026-03-01'},
    rcb(32,[{billFrom:'2026-01-05',mrr:1000,basis:'invoice'}],{onboarding:true,onboardingFrom:'2026-01-01',onboardingExpected:500})],sm);
  ck('  Billed ARR stacks Onboarding (sign-ups waiting to activate) from sign-up at expected MRR; not when live in Recurly or deleted',ob2.onbA,[0,2000*12,2000*12]);
  ck('  Active Brands stacks the same Onboarding brands as a count',ob2.onbN,[0,1,1]);
  ck('  this month in Billed mode is section 01\'s pilot and onboarding cards, not a recount',/_s2\.pil\[_pi\]=_s2Cur\.postN\|\|0;_s2\.pilA\[_pi\]=_s2Cur\.post\*12;/.test(bd)&&/_s2\.onbN\[_pi\]=_s2Cur\.onb\.n;_s2\.onbA\[_pi\]=_s2Cur\.onb\.mrr\*12;/.test(bd)&&/onb:_hasOnb\?_s2\.onbN:null/.test(bd),true);
  ck('  and its chart draws a third band',(T._bdS2Chart({labels:['a','b'],base:[1,2],pil:[1,1],onb:[1,1],col:'#000',pcol:'#999',ocol:'#d4a',bLbl:'B',pLbl:'P',oLbl:'O'}).match(/<path /g)||[]).length,3);
  ck('  each mode starts at its earliest month with data',[T._bdS2FromBilled([rcb(1,[{billFrom:'2025-09-03'}]),rcb(2,[],{pilotHist:[{from:'2025-11-01'}]})]),
    T._bdS2FromSigned([deal(1,{closedate:'2024-03-05T05:00:00Z'}),deal(2,{brand_status:'Churned',dealstage:'3453925110',entered_closedwon:'2023-07-01T05:00:00Z',pausechurn_date:'2024-01-10T05:00:00Z'})])],['2025-09','2023-07']);
  ck('  weekly: new billed ARR by first bill week, new pilots by deposit week',[bil.wk['2026-01-05'],bil.wk['2026-01-12'],bil.wk['2026-03-16']],[{b:1000*12,p:0},{b:0,p:1500*12},{b:2000*12,p:0}]);
  const sgn=T._bdS2Signed([deal(11,{closedate:'2026-01-20T05:00:00Z',amount:800}),deal(12,{closedate:'2026-02-03T05:00:00Z',amount:900}),
      deal(13,{brand_status:'Churned',dealstage:'3453925110',entered_closedwon:'2026-01-05T05:00:00Z',pausechurn_date:'2026-02-20T05:00:00Z',amount:400})],
    {byDeal:{'11':{brands:[{id:'a',recurly:{subs:[{mrr:1200}]}}]},'12':{brands:[{id:'p',pilotPlan:{expected:2500}}]}}},{'12':true},sm);
  ck('Signed: the snapshot split base / pilot, valued once per brand (Recurly, else post-pilot, else HubSpot)',[sgn.base,sgn.pil,sgn.baseA,sgn.pilA],[[2,1,1],[0,1,1],[(1200+400)*12,1200*12,1200*12],[0,2500*12,2500*12]]);
  ck('the stacked chart draws two shaded bands, two lines and a hover per point',(()=>{const c=T._bdS2Chart({labels:['a','b'],base:[1,2],pil:[1,1],col:'#000',pcol:'#999',bLbl:'B',pLbl:'P'});
    return [(c.match(/<path /g)||[]).length,(c.match(/<polyline /g)||[]).length,(c.match(/onmouseenter="bdStackTip/g)||[]).length];})(),[2,2,2]);
  // Month-over-month change of the total (Alex, 2026-11-20).
  const ex$=v=>'$'+Math.round(v).toLocaleString();
  ck('_bdS2Chg: signed change and % of the month before',[T._bdS2Chg(110,100,String),T._bdS2Chg(90,100,ex$),T._bdS2Chg(5,5,String),T._bdS2Chg(3,0,String)],['+10 (+10.0%)','\u2212$10 (\u221210.0%)','\u00b10 (\u00b10.0%)','+3']);
  ck('  the hover card shows it from the second month on, only when asked',(()=>{const o={labels:['Sep 26','Oct 26','Nov 26'],base:[10,12,9],pil:[1,1,1],col:'#000',pcol:'#999',bLbl:'B',pLbl:'P'};
    const on=T._bdS2Chart(Object.assign({chg:true},o)),off=T._bdS2Chart(o);
    return [(on.match(/Change vs /g)||[]).length,/Change vs Sep 26 &lt;b style=&quot;color:#86EFAC&quot;&gt;\+2 \(\+18\.2%\)/.test(on),/Change vs Oct 26 &lt;b style=&quot;color:#FCA5A5&quot;&gt;\u22123 \(\u221223\.1%\)/.test(on),(off.match(/Change vs /g)||[]).length];})(),[2,true,true,0]);
  ck('  both monthly charts ask for it; the weekly one does not',(bd.match(/tips:_s2Tip,chg:true,/g)||[]).length,2);
  ck('  all four month cards carry a change line',(bd.match(/_s2Mini\('(?:This|Last) Month',[^;]*?_s2ChgOf\(_s2Last(?:-1)?,/g)||[]).length,4);
  // Reconciliation tab: a count on each nav anchor, and the freshness line.
  ck('each Reconciliation anchor has a count slot',[1,2,3,4,5,6].every(i=>new RegExp('bd-sec-rec'+i+"'\\)\">[^<]+<span class=\"bd-rec-n\" id=\"bd-rec-n"+i+'"></span></a>').test(bd)),true);
  ck('  filled as "(N)" from the six tables after each render, blank without app data',
    /_bdRecCounts=\[_rc\.t1\.length,_rc\.t2\.length,_rc\.t3\.length,_rc\.t4\.length,_rc\.t5\.length,_rc\.t6\.length\];/.test(bd)&&/\[1,2,3,4,5,6\]\.forEach/.test(bd)&&/c\.textContent=_bdRecCounts\?'\('\+_bdRecCounts\[i-1\]\+'\)':'';/.test(bd),true);
  ck('  and the tab opens with a Data freshness line',/<strong style="color:var\(--navy\)">Data freshness:<\/strong> HubSpot deals and their company links are live on every load/.test(bd),true);
  ck('C1/C2 keep the APP snapshot book, apart from section 02\u2019s',[ex.app['2026-10'].b,!!ex.snap['2026-10']&&ex.snap['2026-10'].b!==9],[9,true]);
}

// 9. A Metabase failure is an error, never zeros
(async()=>{
  const env={METABASE_API_KEY:'k'};
  const okResp=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>''});
  W.fetch=async()=>okResp({status:'failed',error:'SQL compilation error'});
  let e1='';try{await W.fetchBdApp(env);}catch(e){e1=e.message;}
  ck('a failed query throws (the route answers {error})',/query failed/.test(e1),true);
  W.fetch=async()=>okResp({status:'completed',data:{cols:[{name:'ID'}],rows:[]}});
  let e2='';try{await W.fetchBdApp(env);}catch(e){e2=e.message;}
  ck('zero brands back is an error too, not an empty book',/no app brands/.test(e2),true);
  W.fetch=async(u,i)=>{const q=JSON.parse(i.body).native.query;
    return okResp(/STG_HUBSPOT_DEAL_COMPANIES/.test(q)?{status:'completed',data:{cols:[{name:'HC'},{name:'DEALS'}],rows:[]}}
      :{status:'completed',data:{cols:[{name:'ID'},{name:'NM'},{name:'CR'}],rows:[[/MOD\(b\.ID, 4\) = 0/.test(q)?4:1,'x','2026-01-01']]}});};
  const ok=await W.fetchBdApp(env);
  ck('a good read returns every page\u2019s brands',ok.brands.length,4);
  ck('the page keeps an error as an error',/BD\.app=\(d&&!d\.error&&d\.brands&&d\.brands\.length\)\?d:\{error:/.test(bd),true);
  ck('  and shows a band, not numbers, while _ba is missing',/if\(!_baSum\)\{h\+=_baBand\(/.test(bd)&&/if\(!_ba\)hRecon\+=_baBand\(/.test(bd)&&/if\(!_ba\)hChurn=hChurn\.substring\(0,_hChurnAppAt\)/.test(bd),true);
  ck('both reads start together, and Refresh repeats both',/function doLogin\(\)\{\n  bdFetchApp\(\);/.test(bd)&&/bdFetchApp\(\);    \/\/ and the app brand book, in parallel/.test(bd),true);
  // Summary row order and Future ARR (Alex, 2026-11-11).
  ck('row 1: Active Brands, Current ARR, Current MRR, Avg ACV, Future ARR, Future MRR',[...new Set([...bd.matchAll(/_kt\('bd-dark(?: bd-fut)?','([^']+)'/g)].map(m=>m[1]))],['Active Brands','Current ARR','Current MRR','Avg ACV','Future ARR','Future MRR']);
  ck('row 2: six cards, Awaiting Deposit after Pilot Brands, Pilot Avg ACV left of Initiation Fees',[...bd.matchAll(/_kp\('([^']+)'/g)].map(m=>m[1]),['Pilot Brands','Awaiting Deposit','Pilot Live','Pilot Avg ACV','Initiation Fees','Post-Pilot MRR']);
  ck('  Pilot Avg ACV = Post-Pilot MRR ÷ the pilots it counts',/_kp\('Pilot Avg ACV',_postN\?f\$\(_post\/_postN\)/.test(bd),true);
  ck('Initiation Fees = Net add-on revenue and Post-Pilot MRR = Expected MRR of pilot-plan brands, app fees if Recurly fails',
    /var _post=_pp\?_pp\.expected:\(_ps\?_ps\.post:null\)/.test(bd)&&/_kp\('Initiation Fees',_rcOk\?f\$\(_pp\.net\):f\$\(_ps\.netFees\)/.test(bd),true);
  ck('Future ARR and MRR carry the lighter projection style',(bd.match(/_kt\('bd-dark bd-fut','Future (ARR|MRR)'/g)||[]).length,2);
  ck('Future ARR = Future MRR x 12, Future MRR = app MRR + the counted pilots’ post-pilot price',
    /var _fut=_ps&&_post!=null\?_mrr\+_post\+_onb\.mrr:null;/.test(bd)&&/if\(_baStatusOf\(b\)\.d==='Onboarding'\)\{o\.n\+\+;o\.mrr\+=b\.onboardingExpected\|\|0;\}/.test(bd)&&/_mrr=_r\?_r\.mrr:_s\.mrr/.test(bd)&&/_kt\('bd-dark bd-fut','Future ARR',_ps\?f\$\(_fut\*12\)/.test(bd),true);
  // Onboarding counts every sign-up, $0 ones too, so Future MRR and section 02
  // match the Billing Status card (Alex, 2026-11-17: 48, not 47).
  ck('no onboarding count requires an expected MRR',/onboardingExpected&&/.test(bd),false);
  ck('  section 02 history counts $0 sign-ups too',/var onb=bs\.filter\(function\(b\)\{return !b\.discardedAt&&b\.onboardingFrom&&_baStatusOf\(b\)\.d==='Onboarding';\}\);/.test(bd)&&/noa\+=b\.onboardingExpected\|\|0;non\+\+;/.test(bd),true);
  // Section 02 mini cards spell out their total (Alex, 2026-11-17).
  ck('mini cards show base + pilot + onb.',/var _s2Calc=function\(b,p,o,f\)\{return f\(b\)\+' '\+_bLbl\+' \+ '\+f\(p\)\+' pilot'\+\(_hasOnb\?' \+ '\+f\(o\)\+' onb\.':''\);\};/.test(bd),true);
  ck('  all four mini cards use it',(bd.match(/_s2Mini\('(?:This|Last) Month',[^;]*?_s2Calc\(/g)||[]).length,4);
  await pilotTests();
  await appBookTests();
  await recurlyTests();
  pageToggleTests();
  ck('no pilot pill reads selected while the filter is unavailable',/var _pilOpt=function\(k,lbl\)\{var on=!!_pilIds&&_bdPilotMode===k;/.test(bd),true);
  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
