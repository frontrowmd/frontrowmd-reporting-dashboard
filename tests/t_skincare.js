// Skincare Dashboard, Phase 1 (2026-11-10): the HubSpot-only parts, RUN.
// "Skincare" = skincare + hair care + clean beauty / cosmetics.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{isSkincareCategory,buildSkincareBrandWindow,buildSkincareDerm,buildClinicianComposition,clinicianReached,CLS,CLINICIAN_PIPELINE_ID,buildSkincareApp,SKN_MB_SQL});',W);

// ── 1. Which deals are skincare ─────────────────────────────────────────────
// Multi-checkbox values arrive as 'A;B;C'; today's field is plain text.
ck('a multi-checkbox value containing Skin & haircare',W.isSkincareCategory('General wellness;Skin & haircare'),true);
ck('  the plain text, any case and spacing',W.isSkincareCategory('  skin & haircare '),true);
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
    derm:[{T:'MD',CLINICIANS:80,SAMPLES:11,REVIEWERS:4,REVIEWS:62,SAMPLE_REVIEWERS:3,REVIEWS90:21,ACTIVE90:4},
          {T:'DO',CLINICIANS:16,SAMPLES:1,REVIEWERS:1,REVIEWS:2,SAMPLE_REVIEWERS:1,REVIEWS90:2,ACTIVE90:1},
          {T:'NP',CLINICIANS:54,SAMPLES:5,REVIEWERS:3,REVIEWS:9,SAMPLE_REVIEWERS:3,REVIEWS90:9,ACTIVE90:3},
          {T:'Not set',CLINICIANS:53,SAMPLES:28,REVIEWERS:30,REVIEWS:418,SAMPLE_REVIEWERS:19,REVIEWS90:55,ACTIVE90:10}],
    demand:[{K:'total',BRANDS:200,ORDERS:217,ORDERED:1216,DELIVERED:544,OUTSTANDING:641},
            {K:'month',M:'2026-04',ORDERED:600,ORDERS:9,BRANDS:9},   // 7 months back: outside the average
            {K:'month',M:'2026-05',ORDERED:60,ORDERS:6,BRANDS:5},{K:'month',M:'2026-09',ORDERED:120,ORDERS:12,BRANDS:10},
            {K:'month',M:'2026-11',ORDERED:999,ORDERS:20,BRANDS:18},  // the current month: partial, outside it
            {K:'reviewed',M:'2026-06',REVIEWS:40},{K:'reviewed',M:'2026-12',REVIEWS:5}],  // a future month is dropped
    backlog:[{NAME:'Brand A',ORDERED:30,DELIVERED:0,OUTSTANDING:30}],
    brands:[{N:359}]},'2026-11-11')));
  ck('app: MD and DO group to MD/DO, as on the Clinician pages',Object.keys(app.derm.byType).sort(),['MD/DO','Not set','Nurse Practitioner']);
  ck('  MD/DO = MD + DO',app.derm.byType['MD/DO'],{clinicians:96,samples:12,reviewers:5,reviews:64,sampleReviewers:4,reviews90:23,active90:5});
  ck('  totals add every type',[app.derm.total.clinicians,app.derm.total.samples,app.derm.total.reviewers,app.derm.total.reviews],[203,45,38,491]);
  ck('demand totals pass through',[app.demand.ordered,app.demand.delivered,app.demand.outstanding,app.demand.brands,app.demand.orders],[1216,544,641,200,217]);
  ck('months run unbroken from the first to the current, gaps as zero',app.demand.months.map(x=>x.m+':'+x.ordered+'/'+x.reviews),
     ['2026-04:600/0','2026-05:60/0','2026-06:0/40','2026-07:0/0','2026-08:0/0','2026-09:120/0','2026-10:0/0','2026-11:999/0']);
  ck('new demand = the last 6 COMPLETE months (May-Oct), zeros included',[app.demand.avgNewPerMonth,app.demand.avgWindow],[30,['2026-05','2026-10']]);
  ck('  a year boundary steps right',W.buildSkincareApp({demand:[{K:'month',M:'2025-12',ORDERED:6}]},'2026-02-03').demand.avgWindow.join(','),'2025-08,2026-01');
  ck('backlog brands and app brand count',[app.demand.topBrands[0].name,app.skincareBrandsInApp],['Brand A',359]);
  ck('empty Metabase results are zeros, not a crash',(a=>[a.demand.ordered,a.demand.months.length,a.derm.total.clinicians])(W.buildSkincareApp({},'2026-11-11')),[0,0,0]);
  // The SQL carries the agreed definitions.
  const Q=W.SKN_MB_SQL;
  ck('SQL: only live orders (superseded "updated", cancelled, draft excluded)',/WHERE o\.STATUS IN \('confirmed', 'completed'\)/.test(Q.demand),true);
  ck('SQL: outstanding counts only still-open (confirmed) orders',/IFF\(o\.STATUS = 'confirmed', GREATEST\(/.test(Q.demand),true);
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
  ck('a Metabase failure blanks only the app sections',/fetchSkincareApp\(env\)\.catch\(e => \{[^}]*return \{ error: e\.message \}/.test(src),true);
}
const rs=fn(cli,'renderSkincare');
ck('client: no "Waiting on app data" placeholders remain',/Waiting on app data/.test(rs),false);
ck('client: an app error shows in each app section',/appFail\('review demand'\)/.test(rs)&&/appFail\('engagement'\)/.test(rs)&&/appFail\('capacity'\)/.test(rs),true);
ck('client: capacity = every MD/DO dermatologist x 2',/md=\(\(adBy\[SKN_TYPE_FIRST\]\)\|\|\{\}\)\.clinicians\|\|0, cap=md\*2/.test(rs),true);
ck('client: Reviews Submitted shows reviews, with the reviewer count beneath',/_kmTile\('Reviews Submitted',appV\('reviews'\),appErr\?'app data unavailable':'shared or approved, by '\+fN\(ap\.reviewers/.test(rs),true);
ck('client: the surplus is rounded once, so its sign and months-to-clear agree',/net=Math\.round\(cap-nw\)/.test(rs),true);
ck('client: brand names are escaped',/td\(_sknEsc\(x\.name\),1\)/.test(rs),true);

// ── 4. Route + wiring ───────────────────────────────────────────────────────
ck('the route is sign-in-guarded',/url\.pathname === '\/api\/skincare'\) \{[\s\S]{0,200}!\(await authed\(\)\)/.test(src),true);
ck('a refused HubSpot search throws, never a quiet zero',
   (src.match(/throw new Error\('HubSpot refused the (skincare deal|dermatologist contact) search'\)/g)||[]).length,2);
const nav=cli.slice(cli.indexOf('<span>Clinician Dashboard</span>'),cli.indexOf('id="dash-menu-parent"'));
ck('the nav entry sits right after Clinician Dashboard',/switchDashSub\('skincare'\)[^\n]*<span>Skincare Dashboard<\/span>/.test(nav),true);
ck('it has a page, a title and a highlight',
   [cli.indexOf('<div class="pg" id="page-skincare"><div id="skincare-c"></div></div>')>=0,/var titleMap=\{[^}]*skincare:'Skincare Dashboard'/.test(cli),/skincare:"switchDashSub\('skincare'\)"/.test(fn(cli,'_navToken'))],[true,true,true]);
ck('no date pills: the filter bar says all time, with Refresh only',
   (b=>/if\(AP==='skincare'\)\{[^}]*All time[^}]*refreshData\(\)[^}]*return;\}/.test(b)&&!/AP==='skincare'[^}]*selW/.test(b))(fn(cli,'bldF')),true);
ck('  and no window is sent or handled',[/skincare/.test(fn(cli,'_fetchForWindow')),/window:/.test(fn(cli,'fdSkincare')),/computeWindows/.test(fn(src,'processSkincareRequest'))],[false,false,false]);
ck('renderPage and Refresh know it',[/else if\(AP==='skincare'\)renderSkincare\(\);/.test(cli),/else if\(AP==='skincare'\)\{S\.skincare\.data=null;await fdSkincare\(\);\}/.test(cli)],[true,true]);
ck('Jump To reads its headings',/skincare:'#skincare-c \.dsec-h'/.test(cli),true);

// Review fixes on the client.
ck('the login brand payload is not copied into the Skincare slot',/k!=='clinician'&&k!=='skincare'\)S\[k\]\.data=d;/.test(cli),true);
ck('a newer fetch supersedes an in-flight one (sequence, not a busy flag)',
   /const mySeq=S\.skincare\._seq=/.test(fn(cli,'fdSkincare'))&&!/if\(S\.skincare\._fetching\)return;/.test(fn(cli,'fdSkincare')),true);
ck('  and a superseded response is dropped',(fn(cli,'fdSkincare').match(/if\(mySeq!==S\.skincare\._seq\)/g)||[]).length>=3,true);
ck('coming back mid-load re-shows the page instead of restarting',/else if\(S\.skincare\._fetching\)\{[^}]*page-skincare/.test(fn(cli,'switchDashSub')),true);

let _done=0;function finish(){if(++_done<2)return;console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);}
finish();
