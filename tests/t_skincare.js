// Skincare Dashboard, Phase 1 (2026-11-10): the HubSpot-only parts, RUN.
// "Skincare" = skincare + hair care + clean beauty / cosmetics.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{isSkincareCategory,buildSkincareBrandWindow,buildSkincareDerm,buildClinicianComposition,clinicianReached,CLS,CLINICIAN_PIPELINE_ID,buildSkincareApp,SKN_MB_SQL,sknMbSql,sknSkCte,SKN_CATEGORIES,SKN_CATEGORY_ORDER});',W);

// ── 1. Which deals are skincare ─────────────────────────────────────────────
// Multi-checkbox values arrive as 'A;B;C'; today's field is plain text.
ck('a multi-checkbox value containing Skin & haircare',W.isSkincareCategory('General wellness;Skin & haircare'),true);
ck('  the plain text, any case and spacing',W.isSkincareCategory('  skin & haircare '),true);
ck('  the booking form\u2019s actual option, and not the other three',
   ['Skin & Haircare / Clean Beauty','Vitamins & Supplements','Medical Devices','Other'].map(W.isSkincareCategory),[true,false,false,false]);
ck('  not a near miss or another concern',[W.isSkincareCategory('Skin'),W.isSkincareCategory('Gut health'),W.isSkincareCategory('')],[false,false,false]);

// ── 2. Brand pipeline: dated by when each step was ENTERED ──────────────────
// HubSpot keeps only the CURRENT stage. A deal now at closed won still counts
// as a demo completed in the window it entered Demo Happened.
const D=(o)=>({properties:Object.assign({brand_product_category:'Skin & haircare'},o)});
const deals=[
  D({createdate:'2026-11-02T15:00:00Z',hs_v2_date_entered_appointmentscheduled:'2026-11-02T15:00:00Z',dealstage:'appointmentscheduled'}),
  // scheduled AND completed in window, now signed in the window
  D({createdate:'2026-11-03T15:00:00Z',hs_v2_date_entered_appointmentscheduled:'2026-11-03T15:00:00Z',hs_v2_date_entered_1084214349:'2026-11-05T15:00:00Z',dealstage:'closedwon',closedate:'2026-11-07T05:00:00Z'}),
  // no stage stamp: created into Appointment Scheduled, so createdate stands in
  D({createdate:'2026-11-04T15:00:00Z',dealstage:'appointmentscheduled'}),
  // scheduled in October, demo in November
  D({createdate:'2026-10-20T15:00:00Z',hs_v2_date_entered_appointmentscheduled:'2026-10-20T15:00:00Z',hs_v2_date_entered_1084214349:'2026-11-06T15:00:00Z',dealstage:'1084214349'}),
  // signed then churned: still a November signing
  D({createdate:'2026-10-01T15:00:00Z',hs_v2_date_entered_appointmentscheduled:'2026-10-01T15:00:00Z',dealstage:'3453925110',closedate:'2026-11-08T05:00:00Z'}),
  // not skincare
  {properties:{brand_product_category:'Gut health',createdate:'2026-11-02T15:00:00Z',hs_v2_date_entered_1084214349:'2026-11-05T15:00:00Z'}},
];
ck('Nov: scheduled / completed / signed',JSON.parse(JSON.stringify(W.buildSkincareBrandWindow(deals,'2026-11-01','2026-11-30'))),{scheduled:3,completed:2,signed:2});
ck('Oct: the October booking counts there, not in November',JSON.parse(JSON.stringify(W.buildSkincareBrandWindow(deals,'2026-10-01','2026-10-31'))),{scheduled:2,completed:0,signed:0});
// The page is ALL TIME (Alex, 2026-11-10): no bounds = every dated step.
ck('all time (no bounds): every skincare deal, the non-skincare one still out',JSON.parse(JSON.stringify(W.buildSkincareBrandWindow(deals,null,null))),{scheduled:5,completed:2,signed:2});

// ── 3. Dermatologists ───────────────────────────────────────────────────────
const P=W.CLINICIAN_PIPELINE_ID, S=W.CLS;
const cd=(id,stage,props)=>({id,properties:Object.assign({pipeline:P,dealstage:stage,createdate:'2026-11-03T15:00:00Z'},props||{})});
const dd=[
  cd('a',S.REVIEW_SUBMITTED),                 // reached everything
  cd('b',S.SAMPLE_SHIPPED),
  cd('c',S.INQUIRY_COMPLETE),
  cd('d',S.INQUIRY_COMPLETE,{createdate:'2026-09-03T15:00:00Z'}),   // an older inquiry
  cd('e',S.INQUIRY_COMPLETE),                 // a 2nd deal for person a
];
const C={
  a:{__cid:'pa',createdate:'2026-11-03T15:00:00Z',clinician_type:'MD',clinician_credentials_completed:'2026-11-04'},
  b:{__cid:'pb',createdate:'2026-11-03T15:00:00Z',clinician_type:'NP',clinician_credentials_completed:'2026-11-05'},
  c:{__cid:'pc',createdate:'2026-11-03T15:00:00Z',clinician_type:'DO'},
  d:{__cid:'pd',createdate:'2026-09-03T15:00:00Z',clinician_type:'MD/DO',clinician_credentials_completed:'2026-11-06'},
  e:{__cid:'pa',createdate:'2026-11-03T15:00:00Z',clinician_type:'MD',clinician_credentials_completed:'2026-11-04'},
};
const r=JSON.parse(JSON.stringify(W.buildSkincareDerm(dd,C)));
ck('one person with two deals is one dermatologist',r.people,4);
ck('no window fields: the page is all time',[r.current,r.prior,r.cohort],[undefined,undefined,undefined]);
ck('all time: furthest step per person, older inquiry included',
   [r.allTime.inquiries,r.allTime.signUps,r.allTime.samples,r.allTime.reviews],[4,3,2,1]);
ck('  and the funnel never widens',r.allTime.signUps<=r.allTime.inquiries&&r.allTime.samples<=r.allTime.signUps&&r.allTime.reviews<=r.allTime.samples,true);
ck('types are grouped: MD, DO and MD/DO are MD/DO',Object.keys(r.allTime.byType).sort(),['MD/DO','Nurse Practitioner']);
ck('  MD/DO all time',r.allTime.byType['MD/DO'],{inquiries:3,signUps:2,samples:1,reviews:1});
// A later step implies the earlier ones, so the funnel cannot widen: a legacy
// clinician at Sample Shipped with no recorded sign-up date still signed up.
{
  const dd2=[cd('x',S.SAMPLE_SHIPPED)], C2={x:{__cid:'px',createdate:'2026-11-03T15:00:00Z',clinician_type:'PA'}};
  const r2=JSON.parse(JSON.stringify(W.buildSkincareDerm(dd2,C2)));
  ck('a sample with no sign-up date still counts as signed up',[r2.allTime.signUps,r2.allTime.samples],[1,1]);
}
// Current-stage-only: a deal sitting in Review Submitted with no Sample stamp
// is credited with Sample Shipped (it cannot review a sample it never got).
ck('a review with no sample stamp still counts the sample',W.clinicianReached({pipeline:P,dealstage:S.REVIEW_SUBMITTED,createdate:'2026-11-03T15:00:00Z'}).reachedIds.has(S.SAMPLE_SHIPPED),true);

// ── 3b. Review fixes (2026-11-10) ───────────────────────────────────────────
// A webinar registration or a deal created straight into Closed Won never
// booked a demo: no createdate fallback for them.
ck('a webinar deal is not a Demo Scheduled',
   W.buildSkincareBrandWindow([D({createdate:'2026-11-05T15:00:00Z',dealstage:'3741686470',hs_v2_date_entered_3741686470:'2026-11-05T15:00:00Z'})],'2026-11-01','2026-11-30').scheduled,0);
ck('  nor a renewal created straight into Closed Won (but it IS a signing)',
   JSON.parse(JSON.stringify(W.buildSkincareBrandWindow([D({createdate:'2026-11-05T15:00:00Z',dealstage:'closedwon',closedate:'2026-11-05T05:00:00Z'})],'2026-11-01','2026-11-30'))),{scheduled:0,completed:0,signed:1});
ck('  while a demo deal with no stamp still falls back to createdate',
   W.buildSkincareBrandWindow([D({createdate:'2026-11-05T15:00:00Z',dealstage:'appointmentscheduled'})],'2026-11-01','2026-11-30').scheduled,1);
ck('Signed counts Churned Onboarding too, like the Brand Dashboard',
   W.buildSkincareBrandWindow([D({dealstage:'3517067985',closedate:'2026-11-05T05:00:00Z',createdate:'2026-10-01T15:00:00Z',hs_v2_date_entered_appointmentscheduled:'2026-10-01T15:00:00Z'})],'2026-11-01','2026-11-30').signed,1);

// fetchDermClinicians against a FAKE HubSpot: a dermatologist with a duplicate
// contact on one deal, credentials only on the duplicate. The Clinician page's
// merge picks the date up, so the Skincare page must too.
(async()=>{
  const W2={console:{log(){},warn(){},error(){}}};vm.createContext(W2);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nglobalThis.__f=fetchDermClinicians;globalThis.__b=buildSkincareDerm;globalThis.__P=CLINICIAN_PIPELINE_ID;',W2);
  const P2=W2.__P;
  const A={id:'A',properties:{clinician_specialty:'Dermatology',clinician_type:'MD',createdate:'2026-11-03T15:00:00Z'}};
  const Bd={id:'B',properties:{clinician_type:'MD',createdate:'2026-11-03T15:00:00Z',clinician_credentials_completed:'2026-11-04'}};
  vm.runInContext('hsSearch=async()=>globalThis.__contacts;',W2);
  W2.__contacts=[A];
  W2.fetch=async(url,opts)=>{
    const ok=(j)=>({ok:true,status:200,json:async()=>j,body:null});
    if(/associations\/contacts\/deals/.test(url))return ok({results:[{from:{id:'A'},to:[{toObjectId:'D1'}]}]});
    if(/objects\/deals\/batch\/read/.test(url))return ok({results:[{id:'D1',properties:{pipeline:P2,dealstage:'4201261817',createdate:'2026-11-03T15:00:00Z'}}]});
    if(/associations\/deals\/contacts/.test(url))return ok({results:[{from:{id:'D1'},to:[{toObjectId:'A'},{toObjectId:'B'}]}]});
    if(/objects\/contacts\/batch\/read/.test(url))return ok({results:[A,Bd]});
    return {ok:false,status:404,json:async()=>({}),body:null};
  };
  let got=null; try{ got=await W2.__f('tok'); }catch(e){ console.log('  (threw: '+e.message+')'); }
  ck('duplicate contacts merge: the deal keeps the dermatologist',got&&Object.keys(got.contactsByDeal),['D1']);
  ck('  and picks up the credentials from the duplicate record',got&&got.contactsByDeal.D1.clinician_credentials_completed,'2026-11-04');
  const rr=got&&JSON.parse(JSON.stringify(W2.__b(got.deals,got.contactsByDeal)));
  ck('  so the sign up counts',rr&&rr.allTime.signUps,1);
  // A read that returns nothing for deals that exist is a refusal, not zero.
  W2.fetch=async(url)=>{
    const ok=(j)=>({ok:true,status:200,json:async()=>j,body:null});
    if(/associations\/contacts\/deals/.test(url))return ok({results:[{from:{id:'A'},to:[{toObjectId:'D1'}]}]});
    if(/objects\/deals\/batch\/read/.test(url))return ok({results:[{id:'D1',properties:{pipeline:P2,dealstage:'4201261817'}}]});
    return {ok:false,status:500,json:async()=>({}),body:{cancel(){}}};
  };
  let threw=false; try{ await W2.__f('tok'); }catch(e){ threw=/refused the dermatologist contact read/.test(e.message); }
  ck('a failed contact read throws rather than showing zero dermatologists',threw,true);
  finish();
})();

// ── 3c. Phase 2: app data via Metabase (2026-11-11) ─────────────────────────
{
  const app=JSON.parse(JSON.stringify(W.buildSkincareApp({
    derm:[{T:'MD',CLINICIANS:80,SAMPLES:11,REVIEWERS:4,REVIEWS:62,SAMPLE_REVIEWERS:3,REVIEWS90:21,ACTIVE90:4,SK_REVIEWS:50,SK_REVIEWS90:20,B1:1,B2_5:1,B6_20:1,B21:1},
          {T:'DO',CLINICIANS:16,SAMPLES:1,REVIEWERS:1,REVIEWS:2,SAMPLE_REVIEWERS:1,REVIEWS90:2,ACTIVE90:1,SK_REVIEWS:2,SK_REVIEWS90:1,B1:0,B2_5:1,B6_20:0,B21:0},
          {T:'NP',CLINICIANS:54,SAMPLES:5,REVIEWERS:3,REVIEWS:9,SAMPLE_REVIEWERS:3,REVIEWS90:9,ACTIVE90:3},
          {T:'Not set',CLINICIANS:53,SAMPLES:28,REVIEWERS:30,REVIEWS:418,SAMPLE_REVIEWERS:19,REVIEWS90:55,ACTIVE90:10}],
    demand:[{K:'total',BRANDS:200,ORDERS:217,ORDERED:1216,DELIVERED:544,OUTSTANDING:641},
            {K:'month',M:'2026-04',ORDERED:600,ORDERS:9,BRANDS:9},   // 7 months back: outside the average
            {K:'month',M:'2026-05',ORDERED:60,ORDERS:6,BRANDS:5},{K:'month',M:'2026-09',ORDERED:120,ORDERS:12,BRANDS:10},
            {K:'month',M:'2026-11',ORDERED:999,ORDERS:20,BRANDS:18},  // the current month: partial, outside it
            {K:'reviewed',M:'2026-06',REVIEWS:40},{K:'reviewed',M:'2026-12',REVIEWS:5},  // a future month is dropped
            {K:'reviewed',M:'2026-01',REVIEWS:7},  // reviews before the first skincare ORDER do not start the series
            {K:'allmonth',M:'2026-05',ORDERED:300},{K:'allmonth',M:'2026-09',ORDERED:600},{K:'allmonth',M:'2026-11',ORDERED:5000},
            {K:'allmonth',M:'2025-01',ORDERED:10},
            {K:'seg',M:'paying',BRANDS:19,ORDERS:20,OUTSTANDING:50},{K:'seg',M:'stopped',BRANDS:2,ORDERS:2,OUTSTANDING:6}],
    backlog:[{NAME:'Brand A',ORDERED:30,DELIVERED:0,OUTSTANDING:30,OLDEST:412,SEG:'lapsed'}],
    ages:[{K:'open',N:136,DAYS:127,OWED90:349},{K:'fulfil',N:64,DAYS:82.5,OWED90:null}],
    writers:[{DERM:1,T:'MD',WRITERS:12,REVIEWS:153,REVIEWS90:26,WRITERS90:6,ONORDER90:20},{DERM:1,T:'DO',WRITERS:1,REVIEWS:2,REVIEWS90:2,WRITERS90:1,ONORDER90:2},
             {DERM:1,T:'RN',WRITERS:1,REVIEWS:1,REVIEWS90:0,WRITERS90:0},{DERM:0,T:'NP',WRITERS:66,REVIEWS:186,REVIEWS90:135,WRITERS90:53},
             {DERM:0,T:'RDN',WRITERS:2,REVIEWS:3,REVIEWS90:1,WRITERS90:1}],
    brands:[{S:'active',N:192,CUR:62},{S:'pilot',N:28,CUR:0},{S:'none',N:93,CUR:0},{S:'paused',N:14,CUR:1}]},'2026-11-11')));
  ck('app: MD and DO group to MD/DO, as on the Clinician pages',Object.keys(app.derm.byType).sort(),['MD/DO','Not set','Nurse Practitioner']);
  ck('  MD/DO = MD + DO',app.derm.byType['MD/DO'],{clinicians:96,samples:12,reviewers:5,reviews:64,sampleReviewers:4,reviews90:23,active90:5,
     skReviews:52,skReviews90:21,b1:1,b2_5:2,b6_20:1,b21:1});
  ck('  totals add every type',[app.derm.total.clinicians,app.derm.total.samples,app.derm.total.reviewers,app.derm.total.reviews],[203,45,38,491]);
  ck('demand totals pass through',[app.demand.ordered,app.demand.delivered,app.demand.outstanding,app.demand.brands,app.demand.orders],[1216,544,641,200,217]);
  ck('months run unbroken from the first ORDER to the current, gaps as zero',app.demand.months.map(x=>x.m+':'+x.ordered+'/'+x.reviews),
     ['2026-04:600/0','2026-05:60/0','2026-06:0/40','2026-07:0/0','2026-08:0/0','2026-09:120/0','2026-10:0/0','2026-11:999/0']);
  ck('new demand = the last 6 COMPLETE months (May-Oct), zeros included',[app.demand.avgNewPerMonth,app.demand.avgWindow],[30,['2026-05','2026-10']]);
  ck('  a year boundary steps right',W.buildSkincareApp({demand:[{K:'month',M:'2025-12',ORDERED:6}]},'2026-02-03').demand.avgWindow.join(','),'2025-08,2026-01');
  ck('backlog brands, their oldest open order, and the app brand count (every subscription state)',[app.demand.topBrands[0].name,app.demand.topBrands[0].oldestDays,app.skincareBrandsInApp],['Brand A',412,327]);
  ck('skincare brands by latest subscription, with the current-billing-period count',app.subs,{active:{n:192,current:62},pilot:{n:28,current:0},none:{n:93,current:0},paused:{n:14,current:1}});
  ck('skincare share of ALL reviews ordered, same 6 complete months',[app.demand.share6,app.demand.allOrdered6],[20,900]);
  ck('owed by brand state passes through',app.demand.bySeg,{paying:{owed:50,brands:19,orders:20},stopped:{owed:6,brands:2,orders:2}});
  ck('who writes carries the on-order count; backlog rows carry their state',[app.writers[0].onOrder90,app.demand.topBrands[0].seg],[22,'lapsed']);
  ck('backlog age and time to fulfil pass through',app.demand.ages,{openOrders:136,openMedianDays:127,owedOver90:349,fulfilledOrders:64,fulfilMedianDays:82.5});
  ck('who writes: dermatologist or not x MD/DO / NP / PA / other title, dermatologists first',
     app.writers.map(w=>w.group+':'+w.reviews90+'/'+w.writers),
     ['Dermatologist \u00b7 MD/DO:28/13','Dermatologist \u00b7 other title:0/1','Other specialty \u00b7 Nurse Practitioner:135/66','Other specialty \u00b7 other title:1/2']);
  ck('empty Metabase results are zeros, not a crash',(a=>[a.demand.ordered,a.demand.months.length,a.derm.total.clinicians])(W.buildSkincareApp({},'2026-11-11')),[0,0,0]);
  // The SQL carries the agreed definitions.
  const Q=W.SKN_MB_SQL;
  ck('SQL: only live orders (superseded "updated", cancelled, draft excluded)',/WHERE o\.STATUS IN \('confirmed', 'completed'\)/.test(Q.demand),true);
  ck('SQL: outstanding counts only still-open (confirmed) orders',/IFF\(o\.STATUS = 'confirmed', COALESCE\(i\.QUANTITY, 0\) - LEAST\(/.test(Q.demand),true);
  // 2026-11-11: DELIVERED_COUNT is stale (open orders: 152 vs 488 linked reviews).
  ck('SQL: delivered = max(app count, linked shared/approved reviews), capped at quantity',
     [/LEAST\(COALESCE\(i\.QUANTITY, 0\), GREATEST\(COALESCE\(i\.DELIVERED_COUNT, 0\), COALESCE\(lk\.n, 0\)\)\)/.test(Q.demand),
      /lk AS \(SELECT REVIEW_ORDER_ITEM_ID iid, COUNT\(\*\) n FROM STAGING\.STG_NOTES\s+WHERE REVIEW_ORDER_ITEM_ID IS NOT NULL AND STATUS IN \('shared', 'approved'\)/.test(Q.demand)],[true,true]);
  ck('SQL: brand state from the latest subscription; lapsed = active with no billing in 35 days',
     [/INT_BRAND_LATEST_SUBSCRIPTION l ON l\.BRAND_ID = o\.HEALTH_BRAND_ID/.test(Q.demand),
      /WHEN l\.SUBSCRIPTION_STATUS = 'active' AND COALESCE\(s\.CURRENT_PERIOD_ENDS_AT, SYSDATE\(\)\) >= DATEADD\(day, -35, SYSDATE\(\)\) THEN 'paying'\s+WHEN l\.SUBSCRIPTION_STATUS = 'active' THEN 'lapsed'/.test(Q.demand),
      /ELSE 'stopped' END/.test(Q.demand)],[true,true,true]);
  ck('SQL: stopped brands are out of Outstanding, the backlog and the open-order age (lapsed stay in)',
     [/SUM\(IFF\(seg <> 'stopped', owed, 0\)\) OUTSTANDING/.test(Q.demand),/WHERE it\.seg <> 'stopped'/.test(Q.backlog),/st = 'confirmed' AND seg <> 'stopped'/.test(Q.ages)],[true,true,true]);
  ck('SQL: owed by brand state, every state including stopped',/SELECT 'seg', seg, [^\n]*SUM\(owed\)[^\n]*GROUP BY seg/.test(Q.demand),true);
  ck('SQL: on-order pace counts only reviews that fill a live order line, up to its quantity',
     [/o\.STATUS IN \('confirmed', 'completed'\)\s+AND ob\.DISCARDED_AT IS NULL AND COALESCE\(ob\.DEMO, FALSE\) = FALSE\) n WHERE n\.k <= n\.q/.test(Q.writers),
      /ROW_NUMBER\(\) OVER \(PARTITION BY n\.REVIEW_ORDER_ITEM_ID ORDER BY n\.CREATED_AT, n\.ID\) k/.test(Q.writers),
      /AND fl\.nid IS NOT NULL, 1, 0\)\) o90/.test(Q.writers),/SUM\(w\.o90\) ONORDER90/.test(Q.writers)],[true,true,true,true]);
  ck('SQL: samples shipped = Accepted Samples is yes (Alex)',/ACCEPTED_SAMPLES = TRUE/.test(Q.derm),true);
  ck('SQL: reviews = shared or approved notes',[/STATUS IN \('shared', 'approved'\)/.test(Q.derm),/STATUS IN \('shared', 'approved'\)/.test(Q.demand)],[true,true]);
  ck('SQL: months bucketed in Eastern from UTC',/CONVERT_TIMEZONE\('UTC', 'America\/New_York'/.test(Q.demand),true);
  ck('SQL: skincare = the Skin & haircare concern',/h\.NAME = 'Skin & haircare'/.test(Q.demand),true);
  ck('SQL: dermatologists by provider specialty',/TRIM\(pp\.SPECIALTY\) ILIKE 'dermatology'/.test(Q.derm),true);
  // Review fixes (2026-11-11).
  ck('SQL: type is the clinician\'s own title, then the profile\'s (shared practice profiles)',
     /COALESCE\(NULLIF\(TRIM\(c\.TITLE\), ''\), NULLIF\(TRIM\(pp\.TITLE\), ''\), 'Not set'\)/.test(Q.derm),true);
  ck('SQL: discarded and demo brands are out of demand, backlog and outstanding',
     /ob\.DISCARDED_AT IS NULL AND COALESCE\(ob\.DEMO, FALSE\) = FALSE/.test(Q.demand)&&/ob\.DISCARDED_AT IS NULL/.test(Q.backlog),true);
  ck('SQL: the app brand count needs a live (not discarded) skincare product',/AND p\.DISCARDED_AT IS NULL/.test(Q.brands),true);
  // Page rework (2026-11-11).
  ck('SQL: brands by their LATEST subscription; current = period ending within 35 days or later',
     [/INT_BRAND_LATEST_SUBSCRIPTION/.test(Q.brands),/COALESCE\(s\.CURRENT_PERIOD_ENDS_AT, SYSDATE\(\)\) >= DATEADD\(day, -35, SYSDATE\(\)\)/.test(Q.brands)],[true,true]);
  ck('SQL: the all-product share uses the same order rule, without the skincare join',
     (m=>!!m&&!/JOIN sk ON/.test(m[0])&&/o\.STATUS IN \('confirmed', 'completed'\)/.test(m[0])&&/ob\.DISCARDED_AT IS NULL/.test(m[0]))(Q.demand.match(/ia AS \([\s\S]*?\)\nSELECT/)),true);
  ck('SQL: who writes = skincare notes, shared or approved, by specialty and the clinician\'s own title first',
     [/JOIN sk ON sk\.pid = n\.HEALTH_BRAND_PRODUCT_ID/.test(Q.writers),/STATUS IN \('shared', 'approved'\)/.test(Q.writers),/COALESCE\(NULLIF\(TRIM\(c\.TITLE\)/.test(Q.writers)],[true,true,true]);
  ck('SQL: derm skincare-only counts ride the any-product ones (LEFT JOIN, so no review is dropped)',/LEFT JOIN sk ON sk\.pid = nt\.HEALTH_BRAND_PRODUCT_ID/.test(Q.derm),true);
  ck('SQL: open-order age counts only confirmed orders still owed',/WHERE st = 'confirmed' AND seg <> 'stopped' GROUP BY oid HAVING SUM\(owed\) > 0/.test(Q.ages),true);
  ck('a Metabase failure blanks only the app sections',/fetchSkincareApp\(env, cat\)\.catch\(e => \{[^}]*return \{ error: e\.message \}/.test(src),true);

  // ── Brand categories (2026-11-11: All and Skin & haircare) ──
  const C=W.SKN_CATEGORIES, QA=W.sknMbSql(C.all), QS=W.sknMbSql(C.skin);
  ck('categories offered: All and Skin & haircare',[W.SKN_CATEGORY_ORDER,C.skin.concern,C.all.concern],[['all','skin'],'Skin & haircare',null]);
  ck('Skin & haircare builds exactly the skincare queries',JSON.stringify(QS)===JSON.stringify(W.SKN_MB_SQL),true);
  ck('All = every product, no dermatologist query, no placeholder left',
     [/^WITH sk AS \(SELECT p\.ID pid FROM STAGING\.STG_PRODUCTS p\)/.test(QA.demand),'derm' in QA,Object.values(QA).concat(Object.values(QS)).some(q=>q.indexOf('__SK__')>=0)],[true,false,false]);
  let threwCat=false;try{W.sknSkCte({concern:"x'; DROP"});}catch(e){threwCat=true;}
  ck('a concern with SQL characters is refused (concerns are server constants anyway)',threwCat,true);
  const pr=fn(src,'processSkincareRequest');
  ck('the request names a category KEY; only own keys count, anything else is Skin & haircare',
     /Object\.prototype\.hasOwnProperty\.call\(SKN_CATEGORIES, ck\) \? SKN_CATEGORIES\[ck\] : SKN_CATEGORIES\.skin/.test(pr),true);
  ck('the dermatologist HubSpot read runs only for the Dermatology category',/cat\.specialty === 'Dermatology' \? fetchDermClinicians\(token\) : Promise\.resolve\(null\)/.test(pr),true);
  ck('All counts HubSpot deals with any Brand Product Category',/cat\.concern \? isSkincareCategory : \(v\) => String\(v \|\| ''\)\.trim\(\) !== ''/.test(pr)&&/buildSkincareBrandWindow\(brandDeals, null, null, match\)/.test(pr),true);
  const bw=W.buildSkincareBrandWindow([{properties:{brand_product_category:'Gut health',dealstage:'closedwon',createdate:'2026-11-01T12:00:00Z',hs_v2_date_entered_appointmentscheduled:'2026-11-01T12:00:00Z',hs_v2_date_entered_closedwon:'2026-11-05T12:00:00Z',closedate:'2026-11-05T12:00:00Z'}}],null,null,v=>String(v||'').trim()!=='');
  ck('  a non-skincare tagged deal counts under All',[bw.scheduled,bw.signed],[1,1]);
  const ws=JSON.parse(JSON.stringify(W.buildSkincareApp({writers:[
    {DERM:0,T:'MD',SP:'Family Medicine',WRITERS:3,REVIEWS:10,REVIEWS90:5,WRITERS90:2,ONORDER90:4},
    {DERM:0,T:'NP',SP:'Family medicine',WRITERS:2,REVIEWS:6,REVIEWS90:4,WRITERS90:1,ONORDER90:3},
    {DERM:0,T:'MD',SP:'OB/GYN',WRITERS:1,REVIEWS:2,REVIEWS90:1,WRITERS90:1,ONORDER90:1},
    {DERM:1,T:'MD',SP:'Dermatology',WRITERS:1,REVIEWS:9,REVIEWS90:7,WRITERS90:1,ONORDER90:6}]},'2026-11-11')));
  ck('by specialty: spellings merge case-insensitively, Clinician-page names win, biggest first',
     ws.writersBySpecialty.map(x=>x.group+':'+x.reviews90+'/'+x.writers),['Family Medicine:9/5','Dermatology:7/1','Obstetrics and Gynecology:1/1']);
}
const rs=fn(cli,'renderSkincare');
ck('client: no "Waiting on app data" placeholders remain',/Waiting on app data/.test(rs),false);
ck('client: an app error shows in each app section',/appFail\('review demand'\)/.test(rs)&&/appFail\('engagement'\)/.test(rs)&&/appFail\('capacity'\)/.test(rs),true);
ck('client: capacity = every MD/DO dermatologist x 2',/md=mdA\.clinicians\|\|0, cap=md\*2/.test(rs),true);
ck('client: the headline pace is MD/DO dermatologists\' skincare reviews ON AN ORDER over 90 days, a month',
   /const mdW=WR\.find\(x=>x\.derm&&x\.type===SKN_TYPE_FIRST\)/.test(rs)&&/pace=Math\.round\(\(mdW\.onOrder90\|\|0\)\/3\)/.test(rs),true);
ck('client: all clinicians\' pace is also reviews on an order',/allPace=Math\.round\(WR\.reduce\(\(a,x\)=>a\+\(x\.onOrder90\|\|0\),0\)\/3\)/.test(rs),true);
ck('client: the brand-status table and largest-backlogs list are gone (2026-11-12)',[/Still owed, by brand status/.test(rs),/Largest backlogs/.test(rs)],[false,false]);
ck('client: gap and surplus are rounded once, so sign and months-to-clear agree',[/gap=Math\.round\(pace-nw\)/.test(rs),/capNet=Math\.round\(cap-nw\)/.test(rs)],[true,true]);
ck('client: an answer strip leads, then the sections in the order of the questions',/h\+=strip\+secBrands\+secDem\+secDerm\+secEng\+secCap\+secWho;/.test(rs),true);
ck('client: pacing gives both answers, on paper and in practice',/verdict\(capNet>=0,'On paper'/.test(rs)&&/verdict\(gap>=0,'In practice'/.test(rs)&&!/'Actual Pace'/.test(rs),true);
ck('client: HubSpot brand steps show a dash until tagged deals exist; Signed uses the app until then',
   /const hsOn=\(B\.skincareTagged\|\|0\)>0/.test(rs)&&/lt\('Signed',hsOn\?fN\(bc\.signed\|\|0\):\(appErr\?dash:fN\(sn\('active'\)\)\)/.test(rs),true);
ck('client: Engagement leads with the reviewing-after-samples rate',rs.indexOf("lt('Reviewing After Samples'")>=0&&rs.indexOf("lt('Reviewing After Samples'")<rs.indexOf("lt('Ever Reviewed'"),true);
ck('client: Reviews Submitted shows reviews, with the skincare count beneath',/lt\('Reviews Submitted',appV\('reviews'\),appErr\?'app data unavailable':'shared or approved, any product \\u00b7 '\+fN\(ap\.skReviews/.test(rs),true);
ck('client: Capacity does not follow the type filter',/const mdA=adBy\[SKN_TYPE_FIRST\]/.test(rs)&&!/const mdA=[^;]*\bap\b/.test(rs),true);
ck('client: Engagement carries the clinician type filter too (same state)',/engageBody=typeBarFor\(/.test(rs)&&/const typeBar=typeBarFor\(/.test(rs),true);
ck('client: Reviewing After Samples counts every reviewer over everyone who got samples',
   /lt\('Reviewing After Samples',_sknPct\(ap\.reviewers\|\|0,_sknGotSamples\(ap\)\)/.test(rs)&&/function _sknGotSamples\(o\)\{return \(o\.samples\|\|0\)\+Math\.max\(0,\(o\.reviewers\|\|0\)-\(o\.sampleReviewers\|\|0\)\);\}/.test(cli),true);
ck('client: All Active Skincare Brands counts signed, pilot, paused and starting only',/fN\(sn\('active'\)\+sn\('pilot'\)\+sn\('paused'\)\+sn\('future'\)\)/.test(rs),true);
ck('client: the month chart fills the card at a fixed height, redrawn on resize',/const W=Math\.max\(480,Math\.round\(pxW\|\|1000\)\),H=230,/.test(cli)&&/_sknMonthChart\(months,cw,_sknChartW\)/.test(cli)&&/el\._sknRO=new ResizeObserver\(/.test(cli)&&/class="skn-mc" style="position:relative"/.test(cli),true);
ck('client: bar labels get a column sized to them, not a third of the card',/grid-template-columns:'\+lw\+'px minmax\(0,1fr\)/.test(cli),true);
ck('client: option and group labels are escaped',/l:_sknEsc\(x\.group\)/.test(rs),true);
// Type folding: MD/DO, NP and PA keep their own rows; every other title is Other.
{
  const C2={};vm.createContext(C2);vm.runInContext(fn(cli,'_sknFold').replace(/^/,"const SKN_MAIN_TYPES=['MD/DO','Nurse Practitioner','Physician Assistant'];\n")+'\n'+fn(cli,'_sknFoldBy')+'\nglobalThis.f=_sknFoldBy;',C2);
  // Month chart: whole, distinct axis labels; a tiny bar is drawn at its true height.
  const C3={fN:v=>Math.round(v).toLocaleString(),_sknEsc:v=>String(v)};vm.createContext(C3);
  vm.runInContext(fn(cli,'_sknMonthChart')+'\nglobalThis.f=_sknMonthChart;',C3);
  const ax=o=>[...o.matchAll(/text-anchor="end" font-size="10" fill="var\(--grey\)">([^<]*)</g)].map(m=>m[1]);
  ck('client: month chart axis labels are whole and distinct (busiest month = 3)',ax(C3.f([{m:'2026-10',ordered:3,reviews:1,brands:1},{m:'2026-11',ordered:2,reviews:0,brands:1}])),['0','1','2','3','4']);
  const big=C3.f([{m:'2026-10',ordered:150,reviews:0,brands:1},{m:'2026-11',ordered:1,reviews:0,brands:1}]);
  // Drawn height = baseline minus the highest point the path reaches; the
  // corner's start (V) must not dip below the baseline.
  const bars=[...big.matchAll(/<path d="M[\d.]+,([\d.]+) V([\d.]+) q0,-([\d.]+)/g)].map(m=>({h:+m[1]-Math.min(+m[2],+m[2]-+m[3]),v:+m[2],base:+m[1]}));
  ck('client: a 1-review bar on a 200 scale is under 1px tall, not a 4px minimum',bars[1].h>0&&bars[1].h<1&&bars[1].v<=bars[1].base,true);
  const mc=C3.f([{m:'2026-10',ordered:3,reviews:1,brands:1},{m:'2026-11',ordered:2,reviews:0,brands:1}]);
  ck('client: month chart hover is instant (HTML tooltip on hit columns, no delayed native <title>)',
     [/<title>/.test(mc),(mc.match(/class="skn-hit"/g)||[]).length,/onmouseenter="_sknTip\(this,event\)"/.test(mc),/class="skn-tip"/.test(mc),/typeof window\._sknTip|window\._sknTip=function/.test(cli)],[false,2,true,true,true]);
  ck('client: the hit columns sit on top of the bars and line',mc.lastIndexOf('<circle')<mc.indexOf('class="skn-hit"'),true);
  ck('client: minor titles fold into Other, numbers summed',JSON.parse(JSON.stringify(C2.f({'MD/DO':{reviews:5},RDN:{reviews:1,clinicians:2},'Not set':{reviews:2,clinicians:8}}))),{'MD/DO':{reviews:5},Other:{reviews:3,clinicians:10}});
}

// ── 4. Route + wiring ───────────────────────────────────────────────────────
ck('the route is sign-in-guarded',/url\.pathname === '\/api\/skincare'\) \{[\s\S]{0,200}!\(await authed\(\)\)/.test(src),true);
ck('a refused HubSpot search throws, never a quiet zero',
   (src.match(/throw new Error\('HubSpot refused the (skincare deal|dermatologist contact) search'\)/g)||[]).length,2);
const nav=cli.slice(cli.indexOf('<span>Clinician Dashboard</span>'),cli.indexOf('id="dash-menu-parent"'));
ck('the nav entry sits right after Clinician Dashboard, named Brand Category Dashboard',/switchDashSub\('skincare'\)[^\n]*<span>Brand Category Dashboard<\/span>/.test(nav),true);
ck('it has a page, a title and a highlight',
   [cli.indexOf('<div class="pg" id="page-skincare"><div id="skincare-c"></div></div>')>=0,/var titleMap=\{[^}]*skincare:'Brand Category Dashboard'/.test(cli),/skincare:"switchDashSub\('skincare'\)"/.test(fn(cli,'_navToken'))],[true,true,true]);
ck('no date pills: the filter bar says all time, with Refresh only',
   (b=>/if\(AP==='skincare'\)\{[^}]*All time[^}]*refreshData\(\)[^}]*return;\}/.test(b)&&!/AP==='skincare'[^}]*selW/.test(b))(fn(cli,'bldF')),true);
ck('  and no window is sent or handled',[/skincare/.test(fn(cli,'_fetchForWindow')),/window:/.test(fn(cli,'fdSkincare')),/computeWindows/.test(fn(src,'processSkincareRequest'))],[false,false,false]);
ck('renderPage and Refresh know it',[/else if\(AP==='skincare'\)renderSkincare\(\);/.test(cli),/else if\(AP==='skincare'\)\{S\.skincare\.data=null;S\.skincare\.byCat=\{\};await fdSkincare\(\);\}/.test(cli)],[true,true]);
ck('the filter bar is the category dropdown, with all time and Refresh',(b=>/AP==='skincare'\)\{[^}]*_sknCatSelect\(\)\+[^}]*All time[^}]*refreshData\(\)/.test(b))(fn(cli,'bldF')),true);
ck('the dropdown keys match the worker\'s categories',(cli.match(/const SKN_CATS=\[([^\]]*)\]/)||[])[1].match(/key:'(\w+)'/g).map(x=>x.slice(5,-1)),['all','skin']);
ck('the request sends the chosen category; responses are kept per category',/const body=\{password:PW,category:cat\}/.test(fn(cli,'fdSkincare'))&&/S\.skincare\.byCat\[cat\]=d;/.test(fn(cli,'fdSkincare')),true);
ck('switching to a cached category re-renders the whole page (Jump To, Updated)',/if\(S\.skincare\.data\)renderPage\(\);else fdSkincare\(\);/.test(cli),true);
ck('switching category drops a load still running for the old one',/S\.skincare\._seq=\(S\.skincare\._seq\|\|0\)\+1;S\.skincare\._fetching=false;/.test(cli.slice(cli.indexOf('window.skincareSetCat'))),true);
ck('All skips the dermatologist and engagement sections',/if\(!ALL\)\{[\s\S]*card\('derm'[\s\S]*card\('engage'[\s\S]*?\n  \}/.test(rs),true);
ck('Jump To reads its headings',/skincare:'#skincare-c \.dsec-h'/.test(cli),true);

// Review fixes on the client.
ck('the login brand payload is not copied into the Skincare slot',/k!=='clinician'&&k!=='skincare'\)S\[k\]\.data=d;/.test(cli),true);
ck('a newer fetch supersedes an in-flight one (sequence, not a busy flag)',
   /const mySeq=S\.skincare\._seq=/.test(fn(cli,'fdSkincare'))&&!/if\(S\.skincare\._fetching\)return;/.test(fn(cli,'fdSkincare')),true);
ck('  and a superseded response is dropped',(fn(cli,'fdSkincare').match(/if\(mySeq!==S\.skincare\._seq\)/g)||[]).length>=3,true);
ck('coming back mid-load re-shows the page instead of restarting',/else if\(S\.skincare\._fetching\)\{[^}]*page-skincare/.test(fn(cli,'switchDashSub')),true);

let _done=0;function finish(){if(++_done<2)return;console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);}
finish();
