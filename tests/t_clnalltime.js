// Two follow-ups to the 2026-10-27 pipeline rebuild, both RUN rather than
// grepped:
//   1. Sign Up before 2026-10-20 keeps the old definition (a booked kickoff call,
//      dated by the contact's create day); from then on it is Credentials
//      Complete. One sign up per person, never two.
//   2. The Clinician Funnel's All Time toggle: its own fetch, independent of the
//      page window, sliced by month so no search cap can drop the oldest deals.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{clnSignUpKey,CLN_SIGNUP_CUTOVER,buildClinicianComposition,buildClinicianTrend,'
  +'fetchClinicianDealsByMonth,CLINICIAN_ALLTIME_FROM});',W);

// ── 1. The cutover rule ────────────────────────────────────────────────────
const K=(created,creds,booked)=>W.clnSignUpKey(
  {createdate:created,clinician_credentials_completed:creds||''},
  {clinician_meeting_booked_date:booked?'2026-11-02T15:00:00Z':''});
ck('the cutover is the day the Admin App started writing credentials',W.CLN_SIGNUP_CUTOVER,'2026-10-20');
ck('before the cutover, a booked call is a sign up on the create day',
   K('2026-09-10T15:00:00Z','',true),'2026-09-10');
ck('  without a booking it is not',K('2026-09-10T15:00:00Z','',false),null);
ck('from the cutover, a booking alone is no longer a sign up',K('2026-10-21T15:00:00Z','',true),null);
ck('  credentials are',K('2026-10-21T15:00:00Z','2026-10-23',true),'2026-10-23');
// One person, one sign up: the old-rule date wins, so a period that was already
// reported keeps its number and the clinician is not counted again later.
ck('booked before the cutover and completed after: counted once, on the old day',
   K('2026-10-05T15:00:00Z','2026-10-22',true),'2026-10-05');
ck('created before the cutover with no booking: the credentials day',
   K('2026-09-01T15:00:00Z','2026-10-22',false),'2026-10-22');
// The cutover is an EASTERN day, like every other boundary here. 03:30Z on the
// 20th is 23:30 on the 19th in New York: still the old rule.
ck('the cutover day is Eastern',
   [K('2026-10-20T03:30:00Z','',true),K('2026-10-20T05:00:00Z','',true)],['2026-10-19',null]);

{ // The whole month of October, through the real composition.
  const C={},deals=[];let n=0;
  const P=(cid,created,creds,booked)=>{const id='p'+(++n);
    C[id]={__cid:cid,createdate:created,clinician_credentials_completed:creds||'',clinician_type:'NP'};
    deals.push({id,properties:{createdate:created,clinician_meeting_booked_date:booked?'2026-10-28T15:00:00Z':''}});};
  P('a','2026-10-03T15:00:00Z','',true);              // old rule, 3 Oct
  P('b','2026-10-08T15:00:00Z','2026-10-21',true);    // old rule, 8 Oct -- not again on the 21st
  P('c','2026-10-21T15:00:00Z','2026-10-24',true);    // new rule, 24 Oct
  P('d','2026-10-22T15:00:00Z','',true);              // booked after the cutover: not a sign up
  P('e','2026-09-15T15:00:00Z','',true);              // old rule, but September
  P('b','2026-10-08T15:00:00Z','2026-10-21',true);    // b's second deal
  const oct=W.buildClinicianComposition(deals,C,'2026-10-01','2026-10-31');
  ck('October counts both rules, each person once',oct.submission.contacts,3);
  const late=W.buildClinicianComposition(deals,C,'2026-10-20','2026-10-31');
  ck('  and the post-cutover half holds only the credentials sign up',late.submission.contacts,1);
  const sep=W.buildClinicianComposition(deals,C,'2026-09-01','2026-09-30');
  ck('September keeps its old-rule sign up',sep.submission.contacts,1);
  // The trend must tie out to the card on the same rule.
  const t=W.buildClinicianTrend(deals,C,[],'2026-10-01','2026-10-31');
  ck('the trend files them on the same days',
     t.day.filter(p=>p.fullApp>0).map(p=>[p.wk,p.fullApp]),[['2026-10-03',1],['2026-10-08',1],['2026-10-24',1]]);
  ck('  and sums to the card',t.day.reduce((a,p)=>a+p.fullApp,0),oct.submission.contacts);
}

// ── 2. All Time: the monthly slicer ────────────────────────────────────────
{
  const calls=[];
  // Stubbed HubSpot: one deal per day of the slice, with ids that collide across
  // a boundary to prove the slicer de-duplicates.
  W.fetchClinicianDeals=async(tok,f,t)=>{calls.push([f,t]);return [{id:'edge'},{id:'m'+f}];};
  vm.runInContext('fetchClinicianDeals=globalThis.fetchClinicianDeals;',W);
  (async()=>{
    const out=await W.fetchClinicianDealsByMonth('tok','2025-06-01','2026-10-27');
    ck('one search per calendar month, oldest first',calls.length,17);
    ck('  starting where All Time starts',calls[0],['2025-06-01','2025-06-30']);
    ck('  each ending on its month’s last day, February included',
       calls.filter(c=>c[0].slice(5,7)==='02').map(c=>c[1]),['2026-02-28']);
    ck('  the last ending today, not at month end',calls[calls.length-1],['2026-10-01','2026-10-27']);
    ck('  with no gap and no overlap between slices',
       calls.every((c,i)=>i===0||Date.parse(c[0])-Date.parse(calls[i-1][1])===864e5),true);
    ck('a deal returned by two slices is kept once',out.filter(d=>d.id==='edge').length,1);
    ck('All Time starts before the earliest clinician deal (2025-06-23)',W.CLINICIAN_ALLTIME_FROM<='2025-06-23',true);

    // ── 3. The endpoint ──
    const ep=src.slice(src.indexOf("url.pathname === '/api/clinician-funnel'"),src.indexOf("url.pathname === '/api/clinician')"));
    ck('All Time ignores any range sent with it',
       /const from = allTime \? CLINICIAN_ALLTIME_FROM : /.test(ep)&&/const to = allTime \? fmt\(todayET\(\)\) : /.test(ep),true);
    ck('  and fetches month by month',/allTime \? await fetchClinicianDealsByMonth\(token, from, to\)/.test(ep),true);
    ck('  cached per day and per payload version',
       /'cln_funnel_alltime_' \+ API_CACHE_VER \+ '_' \+ to/.test(ep)&&/expirationTtl: 1800/.test(ep),true);
    ck('  which Refresh can bypass',/if \(_ftCk && !body\.fresh\)/.test(ep),true);
    ck('  and it ships the two things the funnel card reads beside the stages',
       /composition, virtualNoShow, \.\.\.funnel/.test(ep),true);

    // ── 4. The client ──
    const C={console,document:{},window:{}};vm.createContext(C);
    vm.runInContext(['var S={clinician:{}};var AP="clinician";',
      "const CLINICIAN_HISTORY_FROM='2026-07-01';",
      fn(cli,'_clnFunnelRange'),fn(cli,'_clnFunnelAT'),fn(cli,'_clnFunnelData'),fn(cli,'_clnFunnelTimeToggle'),
      'Object.assign(globalThis,{S,_clnFunnelRange,_clnFunnelData,_clnFunnelTimeToggle});'].join('\n'),C);
    const page={period:{from:'2026-10-01',to:'2026-10-27'},tag:'page'};
    const at={range:{from:'2025-06-01',to:'2026-10-27'},tag:'all'};
    const st=(o)=>{C.S.clinician=Object.assign({},o);};
    st({});ck('off: the funnel is the page payload',[C._clnFunnelData(page).tag,C._clnFunnelRange(page)],['page',{from:'2026-10-01',to:'2026-10-27'}]);
    st({funnelAllTime:true,funnelATLoading:true});
    ck('on but still loading: the page payload, and the card says so',
       [C._clnFunnelData(page).tag,/Loading All Time/.test(C._clnFunnelTimeToggle())],['page',true]);
    st({funnelAllTime:true,funnelAT:at});
    ck('on and loaded: All Time, payload and range',[C._clnFunnelData(page).tag,C._clnFunnelRange(page)],['all',at.range]);
    // Independence: the page moving does not move the funnel.
    const page2={period:{from:'2026-09-01',to:'2026-09-30'},tag:'page2'};
    ck('  and a different page window leaves it on All Time',[C._clnFunnelData(page2).tag,C._clnFunnelRange(page2)],['all',at.range]);
    st({funnelAllTime:true,funnelATErr:'HTTP 500'});
    const tg=C._clnFunnelTimeToggle();
    ck('a failed load never stands in for a funnel',[C._clnFunnelData(page).tag,/failed to load/.test(tg),/retry/.test(tg)],['page',true,true]);
    st({funnelAllTime:false,funnelAT:at});
    ck('turned off, the page funnel comes back even with All Time in hand',C._clnFunnelData(page).tag,'page');
    ck('the toggle marks the live choice',
       [/aria-pressed="true"[^>]*>All Time</.test((st({funnelAllTime:true,funnelAT:at}),C._clnFunnelTimeToggle())),
        /aria-pressed="true"[^>]*>Page Timeframe</.test((st({}),C._clnFunnelTimeToggle()))],[true,true]);
    ck('Refresh drops the All Time payload and refetches it fresh',
       /S\.clinician\.funnelAT=null;S\.clinician\.funnelATErr=null;S\.clinician\.funnelATFresh=true;if\(S\.clinician\.funnelAllTime\)_clnFetchFunnelAT\(\);/.test(cli),true);
    ck('the funnel card mounts the toggle',/\+_clnFunnelTimeToggle\(\)\+'<\/div>';/.test(fn(cli,'renderClinician')),true);

    console.log('\n'+pass+' passed, '+fail+' failed');
    process.exit(fail?1:0);
  })().catch(e=>{console.log('THREW',e&&e.stack);process.exit(1);});
}
