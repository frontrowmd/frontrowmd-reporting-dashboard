// Skincare Dashboard, Phase 1 (2026-11-10): the HubSpot-only parts, RUN.
// "Skincare" = skincare + hair care + clean beauty / cosmetics.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{isSkincareCategory,buildSkincareBrandWindow,buildSkincareDerm,buildClinicianComposition,clinicianReached,CLS,CLINICIAN_PIPELINE_ID});',W);

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
const cur={from:'2026-11-01',to:'2026-11-30'}, prior={from:'2026-10-01',to:'2026-10-31'};
const r=JSON.parse(JSON.stringify(W.buildSkincareDerm(dd,C,cur,prior,null)));
ck('one person with two deals is one dermatologist',r.people,4);
ck('window inquiries / sign ups use the Clinician page definitions',[r.current.inquiries,r.current.signUps],[3,3]);
ck('  and equal buildClinicianComposition itself',
   [r.current.inquiries,r.current.signUps],(c=>[c.contacts,c.submission.contacts])(W.buildClinicianComposition(dd,C,cur.from,cur.to)));
// An older inquiry who signs up now counts as a sign up NOW, not in the cohort.
ck('cohort = inquired in the window; furthest step per person',
   [r.cohort.inquiries,r.cohort.signUps,r.cohort.samples,r.cohort.reviews],[3,2,2,1]);
ck('  and the funnel never widens',r.cohort.signUps<=r.cohort.inquiries&&r.cohort.samples<=r.cohort.inquiries&&r.cohort.reviews<=r.cohort.samples,true);
ck('all time includes the older inquiry',[r.allTime.inquiries,r.allTime.signUps],[4,3]);
ck('types are grouped: MD, DO and MD/DO are MD/DO',Object.keys(r.allTime.byType).sort(),['MD/DO','Nurse Practitioner']);
ck('  MD/DO all time',r.allTime.byType['MD/DO'],{inquiries:3,signUps:2,samples:1,reviews:1});
// A later step implies the earlier ones, so the funnel cannot widen: a legacy
// clinician at Sample Shipped with no recorded sign-up date still signed up.
{
  const dd2=[cd('x',S.SAMPLE_SHIPPED)], C2={x:{__cid:'px',createdate:'2026-11-03T15:00:00Z',clinician_type:'PA'}};
  const r2=JSON.parse(JSON.stringify(W.buildSkincareDerm(dd2,C2,cur,null,null)));
  ck('a sample with no sign-up date still counts as signed up',[r2.cohort.signUps,r2.cohort.samples],[1,1]);
  ck('  while the WINDOW sign-up count stays dated (it has no date, so none)',r2.current.signUps,0);
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
  const rr=got&&JSON.parse(JSON.stringify(W2.__b(got.deals,got.contactsByDeal,{from:'2026-11-01',to:'2026-11-30'},null,null)));
  ck('  so the window counts the sign up',rr&&rr.current.signUps,1);
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

// ── 4. Route + wiring ───────────────────────────────────────────────────────
ck('the route is sign-in-guarded',/url\.pathname === '\/api\/skincare'\) \{[\s\S]{0,200}!\(await authed\(\)\)/.test(src),true);
ck('a refused HubSpot search throws, never a quiet zero',
   (src.match(/throw new Error\('HubSpot refused the (skincare deal|dermatologist contact) search'\)/g)||[]).length,2);
const nav=cli.slice(cli.indexOf('<span>Clinician Dashboard</span>'),cli.indexOf('id="dash-menu-parent"'));
ck('the nav entry sits right after Clinician Dashboard',/switchDashSub\('skincare'\)[^\n]*<span>Skincare Dashboard<\/span>/.test(nav),true);
ck('it has a page, a title and a highlight',
   [cli.indexOf('<div class="pg" id="page-skincare"><div id="skincare-c"></div></div>')>=0,/var titleMap=\{[^}]*skincare:'Skincare Dashboard'/.test(cli),/skincare:"switchDashSub\('skincare'\)"/.test(fn(cli,'_navToken'))],[true,true,true]);
ck('the date pills refetch it',/if\(AP==='skincare'\)\{S\.skincare\.win=win;[^\n]*await fdSkincare\(\);return;\}/.test(fn(cli,'_fetchForWindow')),true);
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
