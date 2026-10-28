// The Clinician pipeline rebuild (live 2026-10-20, wired in 2026-10-27).
// HubSpot moved to ONE linear path, renamed four surviving stage ids, deleted
// two, and parked five as "(OLD) ...". Sign Up moved from "kickoff call booked"
// to the contact's Clinician Credentials Complete date. Everything here RUNS the
// shipped functions against fixtures shaped like the new pipeline: a stage that
// is credited wrongly, an (OLD) deal leaking into a path count, or a sign up
// filed on the wrong day all look perfectly correct in the source.
const {load,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const W={console};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{CLINICIAN_STAGES,CLINICIAN_OLD_STAGES,CLINICIAN_FLOWS,CLS,CLN_FUNNEL9,'
  +'CLINICIAN_CONTACT_PROPS,CLINICIAN_ENTERED_PROPS,CLN_APP_MILESTONES,buildClinicianFunnel,'
  +'buildClinicianComposition,buildClinicianTable,_clnDateKey,_clnDayKey});',W);
ck('the worker loaded',typeof W.buildClinicianFunnel,'function');

// ── 1. The path is HubSpot's, verbatim ─────────────────────────────────────
// Verified against the live dealstage property on 2026-10-27. The labels are a
// JOIN key for the client's colour and action maps, so a near-miss spelling
// renders a stage with no colour and an "undefined" next step.
const HS_PATH=[['4201261817','Clinician Inquiry Complete'],['4471161546','Video Watched'],
  ['4471161547','Practice Details Complete'],['4470045405','Preferences Complete'],
  ['4470045406','Credentials Submitted'],['4135230193','Call Scheduled'],
  ['4474737370','Call Happened'],['4133870318','Sample Shipped'],['4164547317','Review Submitted']];
const HS_OLD=[['4128510704','(OLD) Meeting Scheduled'],['4128510705','(OLD) Meeting Happened'],
  ['4128510709','(OLD) Signed Up'],['4133870316','(OLD) Form Submitted'],['4133870317','(OLD) Clinician Approved']];
ck('the path is the nine live stages, in order',W.CLINICIAN_STAGES.map(s=>[s.id,s.label]),HS_PATH);
ck('the five (OLD) stages are kept, off the path',W.CLINICIAN_OLD_STAGES.map(s=>[s.id,s.label]),HS_OLD);
ck('every workflow walks the same path',
   Object.values(W.CLINICIAN_FLOWS).every(f=>JSON.stringify(f.stages)===JSON.stringify(HS_PATH.map(x=>x[0]))),true);
ck('no (OLD) stage is on any path',
   Object.values(W.CLINICIAN_FLOWS).some(f=>f.stages.some(id=>HS_OLD.some(o=>o[0]===id))),false);
// The four survivors. Their ids are what every legacy deal still carries.
ck('the renamed survivors keep their old names as aliases',
   [W.CLS.CLINICIAN_INQUIRY===W.CLS.INQUIRY_COMPLETE,W.CLS.FULLY_ONBOARDED===W.CLS.CALL_SCHEDULED,
    W.CLS.SAMPLE_1===W.CLS.SAMPLE_SHIPPED,W.CLS.REVIEW_1===W.CLS.REVIEW_SUBMITTED],[true,true,true,true]);
// A wrong property name never errors; it just reads as no data forever.
ck('no stamp is requested for a deleted stage',
   W.CLINICIAN_ENTERED_PROPS.filter(p=>/4164547318|4164547319/.test(p)),[]);
ck('  while every new path stage is stamped',
   ['4471161546','4471161547','4470045405','4470045406','4474737370']
     .every(id=>W.CLINICIAN_ENTERED_PROPS.indexOf('hs_v2_date_entered_'+id)>=0),true);
ck('  and the (OLD) stamps that exist are still fetched',
   ['4128510705','4128510709','4133870316','4133870317']
     .every(id=>W.CLINICIAN_ENTERED_PROPS.indexOf('hs_v2_date_entered_'+id)>=0),true);

// ── 2. Who is credited with what ───────────────────────────────────────────
const ST=Object.fromEntries(HS_PATH.map(([id,l])=>[l,id]));
const OLD=Object.fromEntries(HS_OLD.map(([id,l])=>[l,id]));
let N=0;
const D=(stage,stamps,wf)=>{const p={dealstage:stage,createdate:'2026-10-21T15:00:00Z',
  clinician_workflow:wf||'Virtual Signup'};
  for(const id of (stamps||[]))p['hs_v2_date_entered_'+id]='2026-10-22T15:00:00Z';
  return {id:'d'+(++N),properties:p};};
const reachedOf=(f)=>Object.fromEntries(f.all.funnel.map(r=>[r.label,r.reachedCount]));
const hereOf=(f)=>Object.fromEntries(f.all.funnel.map(r=>[r.label,r.count]));
const one=(deal)=>{const f=W.buildClinicianFunnel([deal],{},{});return {f,r:reachedOf(f),h:hereOf(f)};};

{ // A new-path clinician mid-onboarding.
  const {r}=one(D(ST['Credentials Submitted'],[ST['Clinician Inquiry Complete'],ST['Video Watched'],
    ST['Practice Details Complete'],ST['Preferences Complete'],ST['Credentials Submitted']]));
  ck('a deal at Credentials Submitted is credited with everything before it',
     [r['Video Watched'],r['Practice Details Complete'],r['Preferences Complete'],r['Credentials Submitted']],[1,1,1,1]);
  ck('  and nothing after it',[r['Call Scheduled'],r['Call Happened'],r['Sample Shipped']],[0,0,0]);
}
{ // The legacy case Alex decided: past the old Fully Onboarded before the
  // rebuild, so it never saw the four intake stages, which did not exist yet.
  const {r}=one(D(ST['Sample Shipped'],[OLD['(OLD) Form Submitted'],ST['Call Scheduled'],ST['Sample Shipped']],'Onboarding Demo'));
  ck('a legacy deal past the old Fully Onboarded is credited with the new intake stages',
     [r['Video Watched'],r['Practice Details Complete'],r['Preferences Complete'],r['Credentials Submitted'],
      r['Call Scheduled'],r['Call Happened'],r['Sample Shipped']],[1,1,1,1,1,1,1]);
  ck('  but not with what it has not reached',r['Review Submitted'],0);
}
{ // Parked in an (OLD) stage with no path stamp at all.
  const {f,r}=one(D(OLD['(OLD) Clinician Approved'],[OLD['(OLD) Form Submitted']],'Onboarding Demo'));
  ck('an (OLD)-stage deal is credited with no path stage',
     Object.values(r).every(n=>n===0),true);
  ck('  and lands in the side bucket, under its own stage',
     [f.all.oldTotal,(f.all.oldStages.find(o=>o.label==='(OLD) Clinician Approved')||{}).count],[1,1]);
  ck('  which lists every (OLD) stage, empty ones included',
     f.all.oldStages.map(o=>o.label),HS_OLD.map(x=>x[1]));
}
{ // ...unless HubSpot stamped one: then that is real evidence, and only that.
  const {f,r}=one(D(OLD['(OLD) Clinician Approved'],[ST['Clinician Inquiry Complete']]));
  ck('an (OLD) deal with a real path stamp is credited with exactly that stage',
     [r['Clinician Inquiry Complete'],r['Video Watched']],[1,0]);
  ck('  and is still counted in the side bucket',f.all.oldTotal,1);
}
{ // Exits are not (OLD) stages.
  const {f}=one(D('4128854721',[ST['Clinician Inquiry Complete']]));
  ck('a No Show deal is an exit, not a side-bucket deal',
     [f.all.oldTotal,(f.all.exits.find(e=>e.label==='No Show')||{}).count],[0,1]);
}

{ // THE REGRESSION the review caught: a deal WAITING in Call Scheduled has its
  // kickoff call in the future. The old Show-Rate exclusion dropped every such
  // deal from the whole funnel, Call Scheduled's own bar included.
  const dl=D(ST['Call Scheduled'],[ST['Clinician Inquiry Complete'],ST['Credentials Submitted'],ST['Call Scheduled']]);
  dl.properties.clinician_meeting_date='2099-01-01';
  const {f,r,h}=one(dl);
  ck('a deal waiting in Call Scheduled for a future call is counted, not excluded',
     [h['Call Scheduled'],r['Credentials Submitted'],f.excludedFuture],[1,1,0]);
}

// ── 3. The invariants, over a mixed cohort ─────────────────────────────────
{
  const deals=[], ids=HS_PATH.map(x=>x[0]);
  // Every path position, with and without stamps, every workflow, plus (OLD)
  // and exit deals with stray stamps.
  ['Virtual Signup','Onboarding Demo','Field Rep',''].forEach(wf=>{
    ids.forEach((id,i)=>{
      deals.push(D(id,ids.slice(0,i+1),wf));                    // fully stamped
      deals.push(D(id,[],wf));                                  // no stamps at all
      deals.push(D(id,ids.filter((_,j)=>j<=i&&j%2===0),wf));    // holes in the stamps
    });
    HS_OLD.forEach(([id],k)=>deals.push(D(id,k%2?[ids[0]]:[],wf)));
    deals.push(D('4128510710',[ids[0],ids[1]],wf));
  });
  const f=W.buildClinicianFunnel(deals,{},{});
  const rc=f.all.funnel.map(r=>r.reachedCount);
  ck('reached counts never increase down the path',rc.every((n,i)=>i===0||n<=rc[i-1]),true);
  ck('every bar has reached >= here now',f.all.funnel.every(r=>r.reachedCount>=r.count),true);
  ck('no conversion exceeds 100%',f.all.funnel.every(r=>r.convToNext==null||r.convToNext<=100),true);
  ck('the funnel rows are exactly the path',f.all.funnel.map(r=>r.id),ids);
  ck('every deal is accounted for once: path, exit or (OLD)',
     f.all.funnel.reduce((a,r)=>a+r.count,0)+f.all.exits.reduce((a,e)=>a+e.count,0)+f.all.oldTotal,deals.length);
  ck('the (OLD) count is every deal parked in one',f.all.oldTotal,4*HS_OLD.length);
}

// ── 4. Sign Up = Clinician Credentials Complete, dated by itself ───────────
{
  const C={}, deals=[];
  const P=(cid,created,creds,booked,extra)=>{const id='s'+(++N);
    C[id]=Object.assign({__cid:cid,createdate:created,clinician_credentials_completed:creds||'',clinician_type:'NP'},extra||{});
    deals.push({id,properties:{dealstage:ST['Video Watched'],createdate:created,
      clinician_meeting_booked_date:booked?'2026-10-25T15:00:00Z':''}});};
  P('a','2026-10-05T15:00:00Z','2026-10-21');           // in, in
  P('b','2026-09-20T15:00:00Z','2026-10-22');           // created BEFORE, signs up in
  P('c','2026-10-06T15:00:00Z','2026-11-03');           // created in, signs up AFTER
  P('d','2026-10-07T15:00:00Z','',true);                // booked a call, never signed up
  P('e','2026-10-08T15:00:00Z',String(Date.UTC(2026,9,31)));   // epoch-ms midnight, 31 Oct
  P('a','2026-10-05T15:00:00Z','2026-10-21');           // a second deal for person a
  P('f','2026-09-10T15:00:00Z','',true);                // booked a call, but arrived in September
  P('g','2026-09-12T15:00:00Z','2026-10-23',true);      // arrived in Sept, booked, signs up in Oct
  const comp=W.buildClinicianComposition(deals,C,'2026-10-01','2026-10-31');
  ck('inquiries are contacts created in the window, deduped',comp.contacts,4);
  ck('sign ups are contacts whose credentials were completed in the window',comp.submission.contacts,4);
  // 22 Oct: nobody was created that day, and b completed credentials on it.
  const c22=W.buildClinicianComposition(deals,C,'2026-10-22','2026-10-22');
  ck('  so a window can have sign ups and no inquiries at all',[c22.contacts,c22.submission.contacts],[0,1]);
  // g signs up in October and has a booking, but arrived in September: its
  // booking is September's, so October's count stays at d alone.
  ck('a booked call is no longer a sign up, but is still counted',comp.bookedCall,1);
  ck('  among the window’s inquiries only, as before',
     W.buildClinicianComposition(deals,C,'2026-09-01','2026-09-30').bookedCall,2);
  // HubSpot stores a date at UTC midnight. Read through Eastern it is the
  // evening BEFORE, and 31 Oct would have dropped out of October.
  const c2=W.buildClinicianComposition(deals,C,'2026-10-31','2026-10-31');
  ck('a sign up dated 31 Oct as epoch ms is 31 Oct, not the 30th',c2.submission.contacts,1);
}
ck('the date key reads a date as the calendar day in every form',
   [W._clnDateKey('2026-10-20'),W._clnDateKey('2026-10-20T00:00:00Z'),W._clnDateKey('2026-10-20T00:00:00.000Z'),
    W._clnDateKey(String(Date.UTC(2026,9,20))),W._clnDateKey(Date.UTC(2026,9,20))],
   ['2026-10-20','2026-10-20','2026-10-20','2026-10-20','2026-10-20']);
ck('  where the datetime key would have moved it a day',W._clnDayKey(String(Date.UTC(2026,9,20))),'2026-10-19');
ck('  and blanks stay blank',[W._clnDateKey(''),W._clnDateKey(null),W._clnDateKey(undefined)],[null,null,null]);
ck('  while a real datetime still goes through Eastern',W._clnDateKey('2026-10-21T02:30:00Z'),'2026-10-20');

// ── 4b. Verification starts at Credentials Submitted ───────────────────────
// It used to start at Meeting Happened, now an (OLD) stage off the path: left
// alone, the gate matched nothing and the section went silently empty.
{
  const C={}, deals=[];
  const V=(stage)=>{const id='v'+(++N);C[id]={__cid:'vc'+N,createdate:'2026-10-05T15:00:00Z',medical_degree_status:'Approved'};
    deals.push({id,properties:{dealstage:stage,createdate:'2026-10-05T15:00:00Z'}});};
  V(ST['Video Watched']); V(ST['Preferences Complete']);                 // nothing to verify yet
  V(ST['Credentials Submitted']); V(ST['Call Happened']); V(ST['Review Submitted']);
  V(OLD['(OLD) Meeting Happened']); V('4128510710');                     // off the path
  const comp=W.buildClinicianComposition(deals,C,'2026-10-01','2026-10-31');
  // 3 on the path at Credentials Submitted or later, plus the legacy deal in
  // (OLD) Meeting Happened, which counted under the old gate and still does.
  ck('verification counts clinicians at Credentials Submitted or later, and legacy ones',comp.verifContacts,4);
  ck('  and its mix adds up to that count',comp.credentials.reduce((a,x)=>a+x.count,0),4);
  const c2={},d2=[];
  [[OLD['(OLD) Form Submitted'],1],[OLD['(OLD) Clinician Approved'],1],[OLD['(OLD) Signed Up'],1],
   [OLD['(OLD) Meeting Scheduled'],0],['4128854721',0],[ST['Video Watched'],0]].forEach(([st],k)=>{
    const id='vv'+k;c2[id]={__cid:'vvc'+k,createdate:'2026-10-05T15:00:00Z',medical_degree_status:'Pending'};
    d2.push({id,properties:{dealstage:st,createdate:'2026-10-05T15:00:00Z'}});});
  ck('  legacy deals with credentials stay in the mix; earlier stages and exits do not',
     W.buildClinicianComposition(d2,c2,'2026-10-01','2026-10-31').verifContacts,3);
}

// ── 5. The new contact fields, end to end ──────────────────────────────────
const NEW_PROPS=['clinician_inquiry_completed','clinician_video_watched','clinician_practice_completed',
  'clinician_preferences_completed','clinician_credentials_completed','clinician_first_sample_shipped',
  'eligible_samples','clinician_discount_store_url','clinician_review_link',
  'clinician_health_brand_preferences','clinician_product_category_preferences',
  'clinician_app_review','gender','years_of_experience','clinician_signup_link'];
ck('every Admin App and Inquiry Form field is fetched',
   NEW_PROPS.filter(p=>W.CLINICIAN_CONTACT_PROPS.indexOf(p)<0),[]);
// Two names that do NOT exist in this portal, verified 2026-10-27. Requesting
// either silently returns nothing forever.
ck('  under their real names, not their labels',
   ['eligible_for_samples','clinician_meeting_booked_date'].filter(p=>W.CLINICIAN_CONTACT_PROPS.indexOf(p)>=0),[]);
{
  const id='t1', c={[id]:{__cid:'c9',firstname:'Whitney',clinician_inquiry_completed:'2026-10-20',
    clinician_video_watched:'2026-10-22',clinician_practice_completed:'2026-10-22',
    clinician_preferences_completed:'2026-10-22',clinician_credentials_completed:'2026-10-22',
    eligible_samples:'Approved',clinician_discount_store_url:'https://app.thefrontrowhealth.com/provider/x',
    clinician_health_brand_preferences:'10X Health System;120/Life',clinician_app_review:'2026-10-21T00:30:00Z',
    gender:'Female',years_of_experience:'9'}};
  const rows=W.buildClinicianTable([{id,properties:{dealstage:OLD['(OLD) Signed Up']}}],c,{},{});
  const r=rows[0];
  ck('an (OLD)-stage deal shows its stage name, not a raw id',r.stageLabel,'(OLD) Signed Up');
  ck('the milestones arrive as calendar days',
     [r.inquiryCompleted,r.videoWatched,r.credentialsCompleted,r.firstSampleShipped],
     ['2026-10-20','2026-10-22','2026-10-22','']);
  ck('and the rest as HubSpot holds them',
     [r.eligibleSamples,r.discountStoreUrl,r.healthBrandPrefs,r.appMeetingBooked,r.gender,r.yearsExperience],
     ['Approved','https://app.thefrontrowhealth.com/provider/x','10X Health System;120/Life','2026-10-20','Female','9']);
  // 00:30 UTC on the 21st is 8:30pm Eastern on the 20th. The table formats dates
  // in UTC, so shipping the raw datetime would have shown the 21st.
}

// ── 6. The client joins on these labels ───────────────────────────────────
const obj=(start,end)=>{const c={};vm.createContext(c);vm.runInContext('var X='+decl(cli,start,end).replace(/^[^=]*=/,''),c);return c.X;};
const COLORS=obj('const CLINICIAN_STAGE_COLORS={','};');
const ACTIONS=obj('const CLN_ACTIONS={','};');
const every=HS_PATH.map(x=>x[1]).concat(['No Show','Not a Fit'],HS_OLD.map(x=>x[1]));
ck('every stage has a colour',every.filter(l=>!COLORS[l]),[]);
ck('every stage has a next step',every.filter(l=>!ACTIONS[l]),[]);
ck('and neither map keeps a stage that no longer exists',
   Object.keys(COLORS).concat(Object.keys(ACTIONS)).filter(l=>every.indexOf(l)<0),[]);
ck('the colour map is in pipeline order, exits then (OLD)',Object.keys(COLORS),every);
const defStages=JSON.parse((/stages:(\[[^\]]*\])/.exec(cli.slice(cli.indexOf('function _clnT()')))[1]).replace(/'/g,'"'));
ck('the table’s default stage filter names only real stages',defStages.filter(l=>!COLORS[l]),[]);
// Every key the worker adds to a row must be a column, or it ships unseen.
const COLS=obj('const CLN_COLS={','};');
const groupCols=[...cli.slice(cli.indexOf('const CLN_GROUPS=['),cli.indexOf('const CLN_COLS='))
  .matchAll(/cols:\[([^\]]*)\]/g)].flatMap(m=>m[1].split(',').map(s=>s.trim().replace(/'/g,'')));
const NEW_KEYS=['inquiryCompleted','videoWatched','practiceCompleted','preferencesCompleted','credentialsCompleted',
  'firstSampleShipped','eligibleSamples','discountStoreUrl','reviewLink','healthBrandPrefs','productCategoryPrefs',
  'appMeetingBooked','gender','yearsExperience','signupLink'];
ck('every new row field is a table column',NEW_KEYS.filter(k=>groupCols.indexOf(k)<0),[]);
ck('  and every column has a header',groupCols.filter(k=>!COLS[k]),[]);
ck('the milestone keys the client dates are the worker’s',
   JSON.parse((/const CLN_APP_DATE_COLS=(\[[^\]]*\])/.exec(cli)||[])[1].replace(/'/g,'"')),
   W.CLN_APP_MILESTONES.map(m=>m[0]));
ck('every new contact field is in the field reference',
   NEW_PROPS.filter(p=>cli.indexOf("['"+p+"',")<0),[]);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
