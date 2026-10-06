// Clinician Funnel rework: toggle removal, workflow gating, stat swap, and a
// nine-stage window-scoped funnel whose first four stages come from Meta + GA4.
const {load,fn}=require('./slice.js');const vm=require('vm');
const cli=load('dashboard.html'), src=load('worker.js');
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
ck('Virtual Path renders only on the virtual workflow',
   /if\(flow==='virtual'\)\{\s*\n\s*sh\+=cardOpen\('Virtual Path'/.test(RC),true);
ck('Demo Path renders only on the demo workflow',
   /if\(flow==='demo'\)\{\s*\n\s*sh\+=cardOpen\('Demo Path'/.test(RC),true);
ck('each gate closes its own card',
   /if\(flow==='virtual'\)\{[\s\S]{0,1400}?sh\+=cardClose;\s*\n\s*\}/.test(RC)
   &&/if\(flow==='demo'\)\{[\s\S]{0,2600}?sh\+=cardClose;\s*\n\s*\}/.test(RC),true);
// The old copy said "blank on the Demo pill" — now it simply is not rendered.
ck('the stale blank-on-pill wording is gone',/blank on the (Demo|Virtual) pill/.test(cli),false);

// --- 7. Virtual No Show Rate --------------------------------------------
ck('No Call Scheduled Yet is gone',/stat\('No Call Scheduled Yet'/.test(cli),false);
ck('Virtual No Show Rate replaces it',/stat\('Virtual No Show Rate'/.test(RC),true);
ck('it reads the worker-computed value',/const _vns=d\.virtualNoShow\|\|\{\};/.test(RC),true);
// The virtual flow has NO No Show stage, so this must come from attendance.
ck('the worker computes it from attendance, not a stage',
   /\(p\.clinician_workflow \|\| ''\)\.trim\(\) !== 'Virtual Signup'/.test(src)
   &&/if \(att === 'No Show'\) noShow\+\+;/.test(src),true);
ck('and only counts meetings that were supposed to happen',
   /const den = held \+ noShow;/.test(src),true);
ck('still-upcoming and cancelled calls are excluded, not counted as attended',
   /CLN_ATT_HELD = \['Demo Given \\u2014 Originally Scheduled', 'Demo Given \\u2014 Rescheduled'\]/.test(src),true);

// --- 8. the nine stages -------------------------------------------------
const defs=(/const CLN_FUNNEL9 = \[([\s\S]*?)\n\];/.exec(src)||[])[1]||'';
ck('eight stages are defined (Approved removed)',(defs.match(/\{ key:/g)||[]).length,8);
ck('in the order asked for',
   [...defs.matchAll(/label: '([^']+)'/g)].map(m=>m[1]),
   ['Impressions','Link Clicks','Page Views','CTA Click','Inquiries','Fully Onboarded','Sample Shipped','Review Submitted']);
ck('Approved is gone',/Clinician Approved', id: CLS\.CLINICIAN_APPROVED/.test(src),false);
// Only Sample Shipped keys off a single dealstage now: the four pre-CRM stages
// have no id, Inquiries/Fully Onboarded are acq-sourced, Review uses ids[].
ck('only Sample Shipped keys off a single dealstage',(defs.match(/\bid: CLS\./g)||[]).length,1);
// Key Metrics is the authority for these two; both now read d.composition, so
// the funnel and Key Metrics cannot report different numbers for the same thing.
ck('Inquiries and Fully Onboarded are sourced from Acquisition',
   /\{ key: 'inquiry',     label: 'Inquiries',        sub: 'clinician contacts created', acq: 'contacts' \}/.test(src)
   &&/\{ key: 'onboarded',   label: 'Fully Onboarded',  sub: 'kickoff call booked',    acq: 'submission' \}/.test(src),true);
ck('and the client resolves them off the same object Key Metrics reads',
   /\}else if\(dfn\.acq\)\{[\s\S]{0,320}?const _cm=d\.composition\|\|\{\};/.test(RC),true);
ck('Inquiries uses composition.contacts, like Cost per Inquiry',
   /\(_cm\.contacts\|\|0\)/.test(RC)&&/const _add=_acq\.contacts\|\|0;/.test(cli),true);
ck('Fully Onboarded uses composition.submission, like Cost per Full App',
   /\(\(_cm\.submission&&_cm\.submission\.contacts\)\|\|0\)/.test(RC)&&/const _fo=_acqSub\?\(_acqSub\.contacts\|\|0\):0;/.test(cli),true);
// Sample Shipped is 1st Sample Shipped relabelled; Review Submitted folds all three.
ck('Sample Shipped maps to 1st Sample Shipped',/label: 'Sample Shipped',   sub: '1st Sample Shipped', id: CLS\.SAMPLE_1/.test(src),true);
ck('Review Submitted combines 1st + 2nd + 3rd\u002b',
   /ids: \[CLS\.REVIEW_1, CLS\.REVIEW_2, CLS\.REVIEW_3\]/.test(src),true);
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
  ck('it covers all nine stages',hexes.length,9);
  ck('with no colour used twice',hexes.length-new Set(hexes).size,0);
  const labels=[...(m?m[1]:'').matchAll(/'([^'#]+)':/g)].map(x=>x[1]);
  ck('and a colour for every stage label',
     ['Impressions','Link Clicks','Page Views','CTA Click','Inquiries','Approved','Fully Onboarded','Sample Shipped','Review Submitted'].filter(l=>!labels.includes(l)),[]);
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
ck('the verification stage test is by id, not array index',
   /const _vFrom = CLINICIAN_STAGES\.findIndex\(x => x\.id === CLS\.MEETING_HAPPENED\);/.test(src),true);
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
ck('flow is pinned to virtual',/const flow='virtual';/.test(RC),true);
ck('the funnel says so in its own header',/Virtual Workflow/.test(RC),true);

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
ck('the scope chip no longer names a toggle that was removed',
   /follows Clinician Type/.test(cli),false);
ck('it describes the Virtual pinning instead',/'Virtual workflow only'/.test(cli),true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
