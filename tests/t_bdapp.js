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
  +'\nObject.assign(globalThis,{bdAppPrice,bdAppTermMonths,bdAppChooseSub,bdAppPilotPost,bdAppBrand,bdAppBuild,bdAppSql,bdAppAssocSql,fetchBdApp,MB_NEEDED_TABLES,BDAPP_BULK_DAYS,BDAPP_BACKFILL_DAYS});',W);
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
vm.runInContext([decl(bd,'var BD_STAGE_LABELS=','};'),decl(bd,'var BD_CHURN_STAGE=',';'),fn(bd,'_bdMonthOfG'),
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
ck('the links use the HubSpot record URLs of the All Deals table',/var BD_HS_URL='https:\/\/app\.hubspot\.com\/contacts\/46815331\/record\/';/.test(bd)&&/BD_HS_URL\+'0-3\/'/.test(bd)&&/BD_HS_URL\+'0-2\/'/.test(bd),true);
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
  ck('  and shows a band, not numbers, while _ba is missing',/if\(!_baSum\)\{h\+=_baBand\(/.test(bd)&&/if\(!_ba\)h\+=_baBand\('the brand and ARR charts'\)/.test(bd)&&/if\(!_ba\)hRecon\+=_baBand\(/.test(bd)&&/if\(!_ba\)hChurn=hChurn\.substring\(0,_hChurnAppAt\)/.test(bd),true);
  ck('both reads start together, and Refresh repeats both',/function doLogin\(\)\{\n  bdFetchApp\(\);/.test(bd)&&/bdFetchApp\(\);    \/\/ and the app brand book, in parallel/.test(bd),true);
  // Summary row order and Future ARR (Alex, 2026-11-11).
  ck('row 1: Active Brands, Current ARR, Current MRR, Avg ACV, Future ARR, Future MRR',[...bd.matchAll(/_kt\('bd-dark','([^']+)'/g)].map(m=>m[1]),['Active Brands','Current ARR','Current MRR','Avg ACV','Future ARR','Future MRR']);
  ck('row 2: Pilot Brands, Initiation Fees, Post-Pilot MRR, Awaiting Deposit',[...bd.matchAll(/_kt\('bd-lnavy','([^']+)'/g)].map(m=>m[1]),['Pilot Brands','Initiation Fees','Post-Pilot MRR','Awaiting Deposit']);
  ck('Future ARR = Future MRR x 12',/_kt\('bd-dark','Future ARR',f\$\(_s\.futureMrr\*12\)/.test(bd),true);
  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
