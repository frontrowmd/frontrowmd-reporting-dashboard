// Clinician Funnel rework: toggle removal, workflow gating, stat swap, and a
// nine-stage window-scoped funnel whose first four stages come from Meta + GA4.
const {load,fn}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html'), src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const RC=fn(cli,'renderClinician');

// --- 1. clinician type toggle gone --------------------------------------
ck('the Clinician Type pills are gone',/segPill\(/.test(cli),false);
ck('and its setter with them',/clinicianSetSeg/.test(cli),false);
// segData feeds the bars, every stat and the deal count — the KEY must survive.
ck('seg is pinned rather than deleted',/const seg='all';/.test(RC),true);
ck('so segData still resolves',/const segData=flowSrc\[seg\]\|\|flowSrc\.all\|\|fd\.all\|\|\{\};/.test(RC),true);

// --- 2. workflow defaults to Virtual, All removed -----------------------
// Superseded: the toggle is gone entirely, so flow is pinned rather than defaulted.
ck("no workflow pills remain",/flowPill/.test(cli),false);

// --- 3 + 4. sections removed --------------------------------------------
ck('Onboarding Rate by Entry Path is gone',/Onboarding Rate by Entry Path<\/span>/.test(cli),false);
// entryStrip is concatenated in the assembly below — deleting the variable is a
// ReferenceError that blanks the whole Clinician page.
ck('but entryStrip still exists for the assembly',/const entryStrip='';/.test(RC),true);
ck('and is still referenced there',/entryStrip\+segBar/.test(RC),true);
ck('Samples & Reviews is gone',/cardOpen\('Samples & Reviews'/.test(cli),false);

// --- 5 + 6. path cards gated on the workflow ----------------------------
// Renamed: the card is about the kick off call, not about a code path, and
// "Virtual Path" meant nothing to a reader now that the workflow toggle is gone.
// Ungated since the 2026-10-27 rebuild: the funnel shows every workflow on one
// path, so a Virtual gate would have hidden the card for good.
ck('the Kick Off Calls card renders for the all-workflow funnel',
   /sh\+=cardOpen\('Kick Off Calls'/.test(RC)&&!/if\(flow==='virtual'\)\{/.test(RC),true);
ck('and the old path name is gone',/cardOpen\('Virtual Path'/.test(cli),false);
ck('Demo Path renders only on the demo workflow',
   /if\(flow==='demo'\)\{\s*\n\s*sh\+=cardOpen\('Demo Path'/.test(RC),true);
ck('the demo gate closes its own card',
   /if\(flow==='demo'\)\{[\s\S]{0,2600}?sh\+=cardClose;\s*\n\s*\}/.test(RC),true);
// The old copy said "blank on the Demo pill" — now it simply is not rendered.
ck('the stale blank-on-pill wording is gone',/blank on the (Demo|Virtual) pill/.test(cli),false);

// --- 7. Virtual No Show Rate --------------------------------------------
ck('No Call Scheduled Yet is gone',/stat\('No Call Scheduled Yet'/.test(cli),false);
ck('Kick Off Call No-Show Rate replaces it',/stat\('Kick Off Call No-Show Rate'/.test(RC),true);
ck('and the old label is gone',/Virtual No Show Rate/.test(cli),false);
// Replaced the backlog stat. Stuck-in-Clinician-Approved was a current-stage
// snapshot of a queue; this is the entry rate the section actually turns on.
ck('Stuck in Clinician Approved is gone',/stat\('Stuck in Clinician Approved'/.test(cli),false);
// Renamed from "Call Scheduled": the funnel now has a Call Scheduled STAGE, a
// deal count on a different basis, and one name for two numbers would read as a
// contradiction.
ck('Call Booked replaces it',/stat\('Call Booked'/.test(RC)&&!/stat\('Call Scheduled'/.test(RC),true);
// CONTACT-based, straight off the Acquisition populations — the same pair Key
// Metrics divides for Full App Rate, so the three agree by construction. A
// funnel-stage count here would be deal-based and would disagree.
// Its OWN count now. It used to read submission.contacts, which was Sign Up;
// Sign Up moved to Credentials Complete and this card stayed on the booking.
ck('it divides the Acquisition populations, not a funnel stage',
   /const _inq=_comp\.contacts\|\|0;/.test(RC)
   &&/const _booked=\(_comp\.bookedCall!=null\)\?_comp\.bookedCall:null;/.test(RC),true);
ck('  and no longer reads the Sign Up population',
   /_booked=\(_comp\.submission/.test(RC),false);
ck('and guards a zero denominator',/\(_booked!=null&&_inq>0\)\?\(_booked\/_inq\)\*100:null/.test(RC),true);
// The two stats pull in opposite directions, so the shared polarity heading
// went and each states its own direction in words.
ck('the Backlog heading is gone',/Backlog/.test(cli),false);
ck('each stat states its own polarity',
   [/higher is worse/.test(RC),/higher is better/.test(RC)],[true,true]);
ck('it reads the worker-computed value',/const _vns=d\.virtualNoShow\|\|\{\};/.test(RC),true);
// The rate is now 1 - (given / booked in the window). RUN it: the whole change
// is which attendance states land in the denominator, and that is behaviour.
{
  const vm=require('vm');
  const wctx={console,Date,Math,JSON,Set,Map,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,
              fetch:()=>{},TextEncoder,TextDecoder,URL,Promise,Response,Request,Headers};
  vm.createContext(wctx);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default')),wctx,{filename:'worker.js'});
  const mk=(att,booked,flow)=>({properties:{clinician_attendance_status:att,clinician_meeting_booked_date:booked,clinician_workflow:flow||'Virtual Signup'}});
  const W=['2026-09-01','2026-09-30'];
  const r=wctx.buildClinicianVirtualNoShow([
    mk('Demo Given \u2014 Originally Scheduled','2026-09-05'),
    mk('Demo Given \u2014 Rescheduled','2026-09-06'),
    mk('No Show','2026-09-07'),
    mk('Scheduled','2026-09-08'),
    mk('Rescheduled','2026-09-09'),
    mk('Cancelled Before Demo','2026-09-10'),
    mk('','2026-09-11'),
    mk('Demo Given \u2014 Originally Scheduled','2026-08-31'),   // booked before the window
    mk('No Show','2026-10-01'),                                 // booked after it
    mk('Demo Given \u2014 Originally Scheduled',''),             // never booked
  ],W[0],W[1]);
  ck('the denominator is every call BOOKED in the window',r.booked,7);
  ck('both Demo Given variants count as given',r.given,2);
  // THE change: everything that is not a Demo Given counts against the rate.
  ck('the rate is 1 minus given over booked',Math.round(r.rate*10)/10,71.4);
  ck('and the other five states are all in the denominator',
     [r.noShow,r.pending,r.cancelled,r.blank],[1,2,1,1]);
  ck('the split accounts for every booked call',r.given+r.noShow+r.pending+r.cancelled+r.blank,r.booked);
  // Scoped on the BOOKED date, so the window excludes calls booked outside it
  // and anything never booked at all.
  ck('calls booked outside the window are excluded',r.booked,7);
  ck('no booked calls gives null, not a divide-by-zero',
     wctx.buildClinicianVirtualNoShow([],W[0],W[1]).rate,null);
  // A call still on the books has not happened, so it pushes the rate up until
  // it resolves — the card has to say how many are in that state.
  ck('pending is reported so the number can be read honestly',
     /_vns\.pending\|\|0/.test(cli),true);
  ck('and the card shows it',/still upcoming/.test(cli),true);
}
// Scoped on the booked date, so it must be handed the full history rather than
// the page cohort — a deal created in June and booked in September is September's.
ck('the endpoint passes the wider deal set and the window',
   /buildClinicianVirtualNoShow\(deals, cur\.from, cur\.to\)/.test(src),true);
ck('both Demo Given values are the held set',
   /CLN_ATT_HELD = \['Demo Given \\u2014 Originally Scheduled', 'Demo Given \\u2014 Rescheduled'\]/.test(src),true);

// --- 8. the nine stages -------------------------------------------------
const defs=(/const CLN_FUNNEL9 = \[([\s\S]*?)\n\];/.exec(src)||[])[1]||'';
// Rebuilt 2026-10-27: three ad stages, contact-based Inquiries, then the
// pipeline path from Video Watched. Clinician Inquiry Complete is not drawn --
// Inquiries stands in for it (Alex: keep Inquiries contact-based).
ck('twelve stages are defined',(defs.match(/\{ key:/g)||[]).length,12);
ck('in the order asked for',
   [...defs.matchAll(/label: '([^']+)'/g)].map(m=>m[1]),
   ['Impressions','Link Clicks','CTA Click','Inquiries','Video Watched','Practice Details Complete',
    'Preferences Complete','Credentials Submitted','Call Scheduled','Call Happened','Sample Shipped','Review Submitted']);
ck('Page Views is gone from the funnel',/key: 'pageViews'/.test(src),false);
// The GA4 page fetch that fed it still runs. Left in deliberately — nothing
// else reads topFunnel.pageViews, so if it stays unused that call can go.
ck('but the fetch that fed it is still there, and flagged',
   /out\.pageViews = n;/.test(src)&&/this GA4 call can go and save a subrequest/.test(src),true);
ck('Approved is gone',/Clinician Approved', id: CLS\.CLINICIAN_APPROVED/.test(src),false);
// Only Sample Shipped keys off a single dealstage now: the four pre-CRM stages
// have no id, Inquiries/Fully Onboarded are acq-sourced, Review uses ids[].
ck('every pipeline row keys off one dealstage',(defs.match(/\bid: CLS\./g)||[]).length,8);
ck('  and no row combines several any more',/\bids: \[/.test(defs),false);
// Key Metrics is the authority for these two; both now read d.composition, so
// the funnel and Key Metrics cannot report different numbers for the same thing.
ck('Inquiries is sourced from Acquisition, and nothing else is',
   /\{ key: 'inquiry',     label: 'Inquiries',        sub: 'clinician contacts created', acq: 'contacts' \}/.test(src)
   &&(defs.match(/\bacq: /g)||[]).length===1,true);
ck('and the client resolves them off the same object Key Metrics reads',
   /\}else if\(dfn\.acq\)\{[\s\S]{0,320}?const _cm=d\.composition\|\|\{\};/.test(RC),true);
ck('Inquiries uses composition.contacts, like Cost per Inquiry',
   /\(_cm\.contacts\|\|0\)/.test(RC)&&/const _add=_acq\.contacts\|\|0;/.test(cli),true);
ck('Key Metrics Sign Up still reads composition.submission',
   /const _fo=_acqSub\?\(_acqSub\.contacts\|\|0\):0;/.test(cli),true);
// 2nd and 3rd+ Review Submitted were deleted from the pipeline; their stamp
// properties no longer exist, so any reference to them reads nothing forever.
ck('Sample Shipped and Review Submitted key off their own stages',
   /label: 'Sample Shipped',            id: CLS\.SAMPLE_SHIPPED/.test(src)
   &&/label: 'Review Submitted',          id: CLS\.REVIEW_SUBMITTED/.test(src),true);
ck('the deleted 2nd / 3rd+ Review stages are referenced nowhere',
   /REVIEW_2|REVIEW_3|4164547318|4164547319/.test(src.split('\n').filter(l=>!/^\s*\/\//.test(l)).join('\n')),false);
// Measured, not inferred: these are the field choices live data proved.
// Both are fetched now (clicks feeds the CTR tile), but the Link Clicks STAGE
// must still key off link_clicks — measured 8,601 vs 13,060, a real difference.
ck('link_clicks is fetched',/'date,campaign_name,impressions,clicks,link_clicks'/.test(src),true);
ck('and the Link Clicks stage keys off it, not plain clicks',
   /\{ key: 'linkClicks',/.test(src)&&/lc \+= parseInt\(r\.link_clicks\) \|\| 0;/.test(src),true);
ck('page views match the two paths EXACTLY, not a contains',
   /CLN_FUNNEL_PAGES = \['\/clinicians', '\/for-clinicians'\]/.test(src)
   &&/CLN_FUNNEL_PAGES\.indexOf\(\(r\.page_path \|\| ''\)\.trim\(\)\) >= 0/.test(src),true);
// conversions_click_join_for_free does NOT exist — it is not a GA4 key event.
ck('the CTA uses event_name + event_count, not the conversions_ pattern',
   /'event_name,event_count'/.test(src),true);
// The comment names the field on purpose. What matters is that no CODE line
// does — a cross-line quote regex false-matches on apostrophes in the prose.
ck('and no code line asks Windsor for the non-existent conversions_ field',
   src.split('\n').filter(l=>l.includes('conversions_click_join_for_free')
                              &&!/^\s*(\/\/|\*)/.test(l)),[]);
ck('and matches by CONTAINS, since the real events are longer',
   /\(r\.event_name \|\| ''\)\.indexOf\(CLN_CTA_EVENT_MATCH\) >= 0/.test(src),true);
ck('page and event GA4 calls are separate (different tables)',
   (src.match(/connectors=googleanalytics4/g)||[]).length>=3,true);

// Per-stage metrics, composed client-side from segData.
ck('the stage ORDER comes from the worker, not duplicated on the client',
   /funnelStages: CLN_FUNNEL9,/.test(src)&&/const _defs=Array\.isArray\(d\.funnelStages\)/.test(RC),true);
ck('each stage carries reached / here / conversion / cost',
   /return \{label:dfn\.label,sub:dfn\.sub\|\|'',reached,here,convFromPrior:conv,cost\};/.test(RC),true);
ck('conversion is from the PRIOR stage',/const conv=\(_prev!=null&&_prev>0&&reached!=null\)/.test(RC),true);
ck('cost divides spend by that stage',/const cost=\(_spend!=null&&reached!=null&&reached>0\)/.test(RC),true);
// A missing stage must be a dash, never a 0 — a zero asserts something false.
ck('a stage with no data is null, not zero',/reached=_topF\[dfn\.key\]!=null\?_topF\[dfn\.key\]:null;/.test(RC),true);
ck('and renders as a dash',/const _n=\(v\)=>v==null\?'\\u2014':fN\(v\);/.test(RC),true);
ck('ad stages have no "here now"',/let reached=null,here=null;/.test(RC),true);
// Ever-reached is nested; "here now" is not.
ck('the combined review stage takes the max reached and the SUM of here-now',
   /reached=Math\.max\.apply\(null,rs\.map\(r=>r\.reachedCount\|\|0\)\);/.test(RC)
   &&/here=rs\.reduce\(\(a,r\)=>a\+\(r\.count\|\|0\),0\);/.test(RC),true);

// --- the two filters the user asked about --------------------------------
// Composing from segData is what makes the workflow pill and the funnel's own
// date filter actually move the nine stages; reading d.funnel9 ignored both.
ck('stages are built from segData, not a pre-baked payload array',
   /const _byId=\{\};for\(const r of fn\)_byId\[r\.id\]=r;/.test(RC)&&!/d\.funnel9/.test(RC),true);
ck('fn comes from segData, which follows the workflow pill',
   /const flowSrc=\(flow!=='both'&&fd\.byFlow&&fd\.byFlow\[flow\]\)\?fd\.byFlow\[flow\]:fd;/.test(RC)
   &&/const fn=\(segData\.funnel\)\|\|\[\];/.test(RC),true);
ck('and the ad stages follow the funnel date override too',
   /const _topF=\(fd&&fd\.topFunnel\)\|\|d\.topFunnel\|\|\{\};/.test(RC),true);
ck('the funnel endpoint returns them for ITS range',
   /const topFunnel = await fetchClinicianTopFunnel\(env\.WINDSOR_API_KEY, from, to\);/.test(src),true);
ck('the funnel date filter defaults to the page window',
   /if\(pp&&pp\.from&&pp\.to\)return \{from:pp\.from,to:pp\.to\};/.test(cli),true);

// --- colours --------------------------------------------------------------
// Every new label fell through to var(--teal), so eight of nine bars matched.
{
  const m=/const CLN_FUNNEL9_COLORS=\{([\s\S]*?)\};/.exec(cli);
  ck('a dedicated nine-stage palette exists',!!m,true);
  const hexes=[...(m?m[1]:'').matchAll(/'(#[0-9A-Fa-f]{6})'/g)].map(x=>x[1].toLowerCase());
  // Pre-pipeline rows only; every pipeline row takes its HubSpot stage colour.
  ck('it covers the five pre-pipeline rows',hexes.length,5);
  ck('with no colour used twice',hexes.length-new Set(hexes).size,0);
  const labels=[...(m?m[1]:'').matchAll(/'([^'#]+)':/g)].map(x=>x[1]);
  const sm=/const CLINICIAN_STAGE_COLORS=\{([\s\S]*?)\};/.exec(cli);
  const stageLabels=[...(sm?sm[1]:'').matchAll(/'([^'#]+)':/g)].map(x=>x[1]);
  // The JOIN, across files: every label the worker draws must resolve a colour
  // on the client, from one map or the other. A miss falls to var(--teal).
  ck('and a colour for every funnel label the worker sends',
     [...defs.matchAll(/label: '([^']+)'/g)].map(x=>x[1])
       .filter(l=>!labels.includes(l)&&!stageLabels.includes(l)),[]);
  ck('the renderer prefers it over the old stage palette',
     /CLN_FUNNEL9_COLORS\[st\.label\]\|\|CLINICIAN_STAGE_COLORS\[st\.label\]/.test(RC),true);
}
// --- the two numbers the funnel exists to show ---------------------------
ck('conversion and cost render as chips, not grey footnotes',
   /from prior<\/span><\/span>'/.test(RC)&&/border-radius:50px;'/.test(RC),true);

// --- scoping: the whole point --------------------------------------------
// Window-scoped end to end, or the CTA Click -> Inquiries rate is meaningless
// and cost per stage is off by ~3x.
ck('the top-of-funnel fetch uses the PAGE window',
   /fetchClinicianTopFunnel\(env\.WINDSOR_API_KEY, cur\.from, cur\.to\)/.test(src),true);
ck('the page payload carries spend for the default range',
   /clnSpend: \(ads && !ads\.error && typeof ads\.spend === 'number'\) \? ads\.spend : null,/.test(src),true);
ck('and the funnel endpoint carries its own',/return jr\(\{ range: \{ from, to \}, topFunnel, clnSpend,/.test(src),true);
// The old worker-side composer is gone — a second, differently-scoped copy in the
// payload is exactly how the dead multiTouchAttribution field happened.
ck('no worker-side funnel9 remains',/buildClinicianFunnel9\(/.test(src),false);

// --- the index defect the sweep found ------------------------------------
// Meeting Happened is an (OLD) stage now, off the path: left there, findIndex
// returned -1 and the Verification section went silently empty.
ck('the verification stage test is by id, not array index',
   /const _vFrom = CLINICIAN_STAGES\.findIndex\(x => x\.id === CLS\.CREDENTIALS_SUBMITTED\);/.test(src),true);
ck('and the old index comparison is gone',/if \(vIdx >= 1\) \{/.test(src),false);

// --- top-level TDZ ---------------------------------------------------------
// CLN_FUNNEL9 is an eagerly-evaluated array literal that reads CLS. Declaring it
// above CLS is a top-level ReferenceError that stops the WHOLE worker booting —
// it passed node --check and was only caught by evaluating the module.
{
  const prefix=src.slice(0,src.indexOf('\nexport default'));
  const c={console};vm.createContext(c);
  let threw=null;
  try{ vm.runInContext(prefix,c); }catch(e){ threw=e.message; }
  ck('the worker prefix evaluates without a TDZ error',threw,null);
  ck('and CLN_FUNNEL9 is declared after CLS',
     src.indexOf('const CLS = {')<src.indexOf('const CLN_FUNNEL9 = ['),true);
}

// Key Metrics now takes its denominators from the ACQUISITION section instead
// of the funnel, so the helper that fed it is gone. The funnel keeps its own
// deal-based stages; the two measure different milestones by design.
ck('no stale virtualReached helper remains',/_clnVirtualReached/.test(src),false);
ck('Key Metrics reads composition, not the funnel',/const _acq=d\.composition\|\|\{\};/.test(cli),true);

// --- scale ----------------------------------------------------------------
// 31,360 impressions against 13 onboarded left eight of nine bars as slivers.
ck('bar width is log-scaled',/const _lg=\(v\)=>\(v==null\|\|v<=0\)\?0:Math\.log10\(1\+v\);/.test(RC),true);
ck('and the renderer uses it',/const trackW=_barW\(rc\);/.test(RC),true);
ck('zero still renders empty, not at the floor',/\(v==null\|\|v<=0\)\?0:Math\.max\(3,/.test(RC),true);
{
  // The spread the user actually saw: the smallest bar must stay visible.
  const lg=v=>v<=0?0:Math.log10(1+v), mx=lg(31360)||1;
  const w=v=>v<=0?0:Math.max(3,(lg(v)/mx)*100);
  ck('a 31,360-to-13 spread keeps every bar visible',
     [w(31360),w(560),w(129),w(13)].every(x=>x>=3),true);
  ck('and linear would not have',((13/31360)*100)<1,true);
}

// --- virtual only ---------------------------------------------------------
ck('the Demo and Field pills are gone',/flowPill/.test(cli),false);
ck('and their setter with them',/clinicianSetFlow/.test(cli),false);
// One path for every workflow since 2026-10-27; the (OLD) side bucket would
// otherwise lose the demo-flow deals it exists to count.
ck('flow is every workflow',/const flow='both';/.test(RC),true);
ck('the funnel says so in its own header',/All Workflows/.test(RC)&&!/Virtual Workflow/.test(RC),true);

// --- the funnel has NO date filter of its own ----------------------------
// It used to carry one, independent of the page selector, with a pinned range
// that had to be invalidated whenever the page window moved. That whole
// mechanism is gone: the funnel now follows the page window like every other
// section, so there is no second span for the two to disagree about.
ck('the Funnel Dates bar is gone',/Funnel Dates/.test(cli),false);
ck('and its builder with it',/_clnFunnelBar/.test(cli),false);
ck('and its Apply / Reset handlers',
   [/clinicianApplyFunnelRange/.test(cli),/clinicianResetFunnelRange/.test(cli)],[false,false]);
ck('and the fetch only they could reach',/fdClinicianFunnel/.test(cli),false);
// The state they wrote is gone too — a reset that clears state nothing can set
// reads as though the feature still exists.
ck('no funnel-override state survives',
   [/funnelRange/.test(cli),/funnelData/.test(cli),/_pinnedFor/.test(cli)],[false,false,false]);
// What replaces it: the range comes straight off the payload's period.
ck('the funnel range is the page window',
   /function _clnFunnelRange\(d\)\{\s*const pp=\(d&&d\.period\)\|\|null;/.test(cli),true);
ck('with the history range as the only fallback',
   /return \{from:CLINICIAN_HISTORY_FROM,to:\(hr&&hr\.to\)\|\|CLINICIAN_HISTORY_FROM\};/.test(cli),true);
ck('and the funnel payload is simply what the endpoint returned',
   /function _clnFunnelData\(d\)\{ return d; \}/.test(cli),true);

// --- removals -------------------------------------------------------------
ck('the Exited Pipeline card is gone',/cardOpen\('Exited Pipeline'/.test(cli),false);
ck('and its segData.exits read with it',/segData\.exits/.test(cli),false);
// The scope chip is gone altogether. The whole section is pinned to the virtual
// workflow, so a per-card badge saying so repeated what the section already said
// and never distinguished one card from another.
ck('the scope chip is gone',[/follows Clinician Type/.test(cli),/const CHIP=/.test(cli)],[false,false]);
ck('and cardOpen no longer takes a scope',
   /const cardOpen=\(title,pop,first\)=>/.test(cli),true);
ck('so no caller still passes one',/cardOpen\('[^']+','(path|live)'/.test(cli),false);

// --- the cost-per-stage unit must read as a singular noun ----------------
// The label was lowercased and stripped with /s$/, which turns "Inquiries" into
// "per inquirie". Asserted by RUNNING the helper over the real stage labels, so
// renaming a stage is checked rather than assumed.
{
  const fnSrc=/function _clnSingular\(label\)\{[\s\S]*?\n\}/.exec(cli)[0];
  const sing=new Function(fnSrc+'; return _clnSingular;')();
  ck('-ies becomes -y',sing('Inquiries'),'inquiry');
  ck('a plain plural drops its s',[sing('Impressions'),sing('Link Clicks'),sing('Page Views')],
     ['impression','link click','page view']);
  ck('an already-singular label is untouched',
     [sing('CTA Click'),sing('Fully Onboarded'),sing('Sample Shipped'),sing('Review Submitted')],
     ['cta click','fully onboarded','sample shipped','review submitted']);
  ck('and a double-s ending is left alone',sing('Progress'),'progress');
  // EVERY real stage label, straight off the worker's definition.
  const stages=(src.slice(src.indexOf('const CLN_FUNNEL9'),src.indexOf('\n];',src.indexOf('const CLN_FUNNEL9')))
    .match(/label: '([^']+)'/g)||[]).map(x=>x.slice(8,-1));
  ck('the stage list was found',stages.length>=6,true);
  ck('no stage produces a mangled unit',stages.filter(l=>/(ie|u)$/.test(sing(l))),[]);
}
ck('the cost chip uses the helper, not a bare strip',
   /per '\+_clnSingular\(st\.label\)/.test(cli),true);
ck('and the bare strip is gone',/toLowerCase\(\)\.replace\(\/s\$\/,''\)/.test(cli),false);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
