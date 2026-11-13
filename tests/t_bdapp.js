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
  +'fetchBdPilotStats,bdPilotStatsOf,grafanaFrameRows,GRAFANA_URL,GRAFANA_APP_DS,BD_PILOT_SQL_PG,BD_PILOT_SQL_SF,BDAPP_SQL_PG,BDAPP_PAGES});',W);
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
ck('  and HubSpot MRR sits right next to App MRR',/\['hsMrr','HubSpot MRR','num'\],\['appMrr','App MRR','num'\]\]/.test(bd),true);
ck('test accounts are an explicit list',/var BD_APP_TEST_BRANDS=\['FrontrowMD Demo Account','Vincent'\];/.test(bd),true);

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
  ck('each pilot card prints its source',/_psLive\?'Live \\u00b7 app':'As of last night'/.test(bd)&&/\+_psSrc\+'<\/div>'/.test(bd),true);
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
  vm.runInContext([fn(bd,'_bdMonthOfG'),fn(bd,'_bdMDY'),fn(bd,'_bdMonthEndMDY'),fn(bd,'_baPilotDealIds'),fn(bd,'_baIsTest'),
    decl(bd,'var BD_APP_TEST_BRANDS=',';'),decl(bd,"var _bdPilotMode=",';'),
    'function f$(v){return "$"+Math.round(v);}','function buildSVGChart(l,v){return "<svg data-n=\\""+v.length+"\\"></svg>";}',
    'var _bdChartMode="current";',
    'function run(deals,app){var BD={app:app};var _ba=(app&&!app.error)?app:null;var _baSum=null;var _baSer={snap:{"2026-10":{b:9,a:9}}};'
      +'var closedWonDeals=deals.filter(function(dd){return dd.dealstage==="closedwon";});var h="";'
      +'var _baJ={byDeal:{}};if(_ba)deals.forEach(function(dd){_baJ.byDeal[dd.id]={brands:(_ba.map[dd.id]||[])};});'
      +block+'\nreturn {chartDeals:chartDeals.map(function(d){return d.id;}),snap:_snByMonth,app:_snByMonthApp,wk:_wkTotal,h:h,cur:_chartMonthMap};}'].join('\n'),T);
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
  ck('  mini cards carry the original labels',/Current Month Signed Deals/.test(ex.h)&&/Last Month Signed Deals/.test(ex.h)&&!/New Brands/.test(ex.h),true);
  ck('  the section says HubSpot',/<span class="bd-src">HubSpot<\/span>/.test(ex.h),true);
  T._bdPilotMode='only';
  const on=T.run(D,app);
  ck('Pilots Only: just the pilot deals',on.chartDeals,['1']);
  ck('  and the snapshot adds back the churned pilot while it was live',[on.snap['2026-09'].b,on.snap['2026-10'].b],[2,1]);
  ck('  the toggle shows Pilots Only selected',/aria-checked="true"[^>]*>Pilots Only/.test(on.h),true);
  T._bdPilotMode='exclude';
  const none=T.run(D,null);
  ck('no app data yet: every HubSpot deal, unfiltered, never blank',none.chartDeals,['1','2','3']);
  ck('  with the small "pilot filter unavailable" note and an inert toggle',/pilot filter unavailable/.test(none.h)&&/still loading/.test(none.h)&&!/onclick="bdSetPilotMode/.test(none.h),true);
  const err=T.run(D,{error:'Metabase 503'});
  ck('app read failed: the same, saying it did not load',[err.chartDeals.length,/did not load/.test(err.h)],[3,true]);
  // Reconciliation tab: a count on each nav anchor, and the freshness line.
  ck('each Reconciliation anchor has a count slot',[1,2,3,4,5].every(i=>new RegExp('bd-sec-rec'+i+"'\\)\">[^<]+<span class=\"bd-rec-n\" id=\"bd-rec-n"+i+'"></span></a>').test(bd)),true);
  ck('  filled as "(N)" from the five tables after each render, blank without app data',
    /_bdRecCounts=\[_rc\.t1\.length,_rc\.t2\.length,_rc\.t3\.length,_rc\.t4\.length,_rc\.t5\.length\];/.test(bd)&&/c\.textContent=_bdRecCounts\?'\('\+_bdRecCounts\[i-1\]\+'\)':'';/.test(bd),true);
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
  ck('row 1: Active Brands, Current ARR, Current MRR, Avg ACV, Future ARR, Future MRR',[...bd.matchAll(/_kt\('bd-dark(?: bd-fut)?','([^']+)'/g)].map(m=>m[1]),['Active Brands','Current ARR','Current MRR','Avg ACV','Future ARR','Future MRR']);
  ck('row 2: six cards, Awaiting Deposit after Pilot Brands, Pilot Avg ACV left of Initiation Fees',[...bd.matchAll(/_kp\('([^']+)'/g)].map(m=>m[1]),['Pilot Brands','Awaiting Deposit','Pilot Live','Pilot Avg ACV','Initiation Fees','Post-Pilot MRR']);
  ck('  Pilot Avg ACV = Post-Pilot MRR ÷ Pilot Brands',/_kp\('Pilot Avg ACV',_ps\.n\?f\$\(_ps\.post\/_ps\.n\)/.test(bd),true);
  ck('Future ARR and MRR carry the lighter projection style',(bd.match(/_kt\('bd-dark bd-fut','Future (ARR|MRR)'/g)||[]).length,2);
  ck('Future ARR = Future MRR x 12, Future MRR = app MRR + the counted pilots’ post-pilot price',
    /var _fut=_ps\?_s\.mrr\+_ps\.post:null;/.test(bd)&&/_kt\('bd-dark bd-fut','Future ARR',_ps\?f\$\(_fut\*12\)/.test(bd),true);
  await pilotTests();
  await appBookTests();
  pageToggleTests();
  ck('no pilot pill reads selected while the filter is unavailable',/var _pilOpt=function\(k,lbl\)\{var on=!!_pilIds&&_bdPilotMode===k;/.test(bd),true);
  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
