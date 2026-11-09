// Clinician Dashboard restructure: new Key Metrics section, card removals/moves,
// section reorder. The KPI arithmetic matters most — a wrong denominator here is
// a wrong number on a card, which is worse than a missing card.
const {load,fn}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html'), src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const RC=fn(cli,'renderClinician');
// The Key Metrics row moved out of renderClinician into its own builder so the
// Executive Dashboard can render the identical row. Assertions about the row
// read KM; assertions about the PAGE still read RC.
const KM=fn(cli,'_clnKeyMetricsHTML');

// --- section order -------------------------------------------------------
// h STARTS as the funnel, adsH (Ads + Ad Tables) is appended, then All Clinicians,
// then the ref cards; Key Metrics is PREPENDED last.
ck('Key Metrics is prepended, not appended',
   /\+_clnKeyMetricsHTML\(d,''\)\+h;/.test(RC)&&!/h\+=_clnKeyMetricsHTML/.test(RC),true);
// The monthly goal band is the one thing allowed above it.
ck('  with the goal tracker directly above it',
   /_goalTrackerHTML\(\{[\s\S]{0,200}?\}\)\s*\n?\s*\+_clnKeyMetricsHTML\(d,''\)/.test(RC),true);
ck('All Clinicians is appended after the ad tables, not promoted to the top',
   /h\+=allClinH;/.test(RC)&&!/h=allClinH\+h;/.test(RC),true);
// The Ads KPI row is gone: its three cards (pacing, cost per inquiry, cost per
// full submission) all restated figures the Key Metrics row at the top already
// carries. The section is now the ad tables alone, appended straight to `h` —
// the adsH buffer existed only so a card could be emitted ahead of it.
ck('the Ads KPI card is gone',/card\('Ads',/.test(RC),false);
ck('and its builder with it',/_clnAdsCostCards/.test(cli),false);
ck('and the late repaint only it needed',/cln-ads-cost/.test(cli),false);
ck('the buffer is gone too',/adsH/.test(RC),false);
ck('the ad tables are appended directly',/h\+=card\('Clinician Ad Tables'/.test(RC),true);
ck('and All Clinicians still lands after them',
   RC.indexOf("card('Clinician Ad Tables'")<RC.indexOf('h+=allClinH;'),true);
// Building Key Metrics in place at the top would read consts declared below it.
// Guarded with indexOf()>=0 as well as the ordering: a const that no longer
// exists returns -1, which is "before" everything and passes vacuously.
// `dly` went with the Daily Activity section; `comp` is the one left.
// This used to guard a TDZ: the row was a bare block inside renderClinician
// that had to be built after `comp`. It is a standalone builder now, taking
// only `d`, so the hazard is gone — what matters instead is that it depends on
// nothing from renderClinician's scope.
ck('the row is a standalone builder taking only the payload',
   /function _clnKeyMetricsHTML\(d,sfx,win\)/.test(cli),true);
ck('  and reads the payload directly, not a renderClinician local',
   /d\.composition/.test(KM)&&!/\bcomp\b/.test(KM),true);

// --- removals ------------------------------------------------------------
ck('Specialty Breakdown is gone',/panel\('Specialty Breakdown'/.test(cli),false);
ck('the Clinician Composition card is gone',/card\('Clinician Composition'/.test(cli),false);
ck('the Referral Source panel is gone from Acquisition',/acqPanel\('Clinician Referral Source'/.test(cli),false);
ck('the Verification Funnel section is gone',/card\('Verification Funnel'/.test(cli),false);
ck('and its helpers went with it',
   [/vpanel\(/,/vshell\(/,/_fvdRows/,/_fvdTot/].some(r=>r.test(cli)),false);
ck('its data-source reference row is gone too',/\['Verification Funnel','HubSpot'/.test(cli),false);

// --- Type of Clinician moved into Acquisition ----------------------------
ck('Type of Clinician is now an Acquisition panel',/acqPanel\('Type of Clinician'[^\n]*_fSrc\.type\|\|\[\]/.test(cli),true);
// acqSrc follows the Inquiry / Full Submission toggle, so the submission
// population needs its own type mix or the panel blanks in that mode.
ck('acqSrc still follows the population toggle',/const acqSrc=\(acqMode==='submission'&&sub\)\?sub:comp;/.test(cli),true);
ck('the worker gives the submission population a type mix',/type: capped\(subType, 9\),/.test(src),true);
ck('and counts it alongside the referral mix',/subType\[t\] = \(subType\[t\] \|\| 0\) \+ 1;/.test(src),true);

// --- helpers had to leave their enclosing function ----------------------
ck('the delta footer is module scope, not nested in renderDash',
   /\n function _irTileFooterDash|\nfunction _irTileFooterDash/.test(cli),true);
ck('renderDash no longer declares its own copy',
   /function renderDash\(\)\{[\s\S]{0,400}?function _irTileFooterDash/.test(cli),false);
// The clinician-only tile and pacing bar are GONE. Both sections now render
// _kmTile / _kmPaceBar, so there is no second implementation to drift from the
// Brand one — which is exactly how they came to look different.
ck('the clinician-only tile helpers were removed',
   /function _clnKpiTile\(/.test(cli)||/function _clnPaceBar\(/.test(cli),false);
ck('the pacing bar is module scope and takes spend, budget and the window',
   /^function _kmPaceBar\(spend,budget,win\)/.test(fn(cli,'_kmPaceBar')),true);
ck('Key Metrics passes the window spend, budget and its own window',
   /_kmPaceBar\(_sp\|\|0,_budget,win\)/.test(KM),true);

// --- the four cards' arithmetic -----------------------------------------
// Both denominators come from the ACQUISITION section, so the two sections can
// never disagree about the same quantity. Contact-based and deduped — NOT the
// funnel's deal-based stages, so this will not equal Fully Onboarded.
ck('Cost per Full App divides by the Acquisition Full Submission count',
   /_f\$\(_div\(_sp,_fo\)\)/.test(KM)&&/const _fo=_acqSub\?\(_acqSub\.contacts\|\|0\):0;/.test(KM),true);
ck('which is composition.submission',/const _acqSub=_acq\.submission\|\|null;/.test(KM),true);
ck('and never the Meta-conversion field',/_a\.cacDenom\b/.test(KM),false);
// \b after cacDenom so cacDenomOnboarded does not match; the prose comment
// above mentions cacDenom deliberately, and must not fail this.
ck('and never reads the bare cacDenom property',/_a\.cacDenom\b/.test(RC),false);
ck('Cost per Inquiry divides by the Acquisition clinician count',
   /_f\$\(_div\(_sp,_add\)\)/.test(KM)&&/const _add=_acq\.contacts\|\|0;/.test(KM),true);
// Full App Rate must be the SAME ratio the Acquisition caption prints.
ck('Full App Rate is that pair, so it matches the Acquisition rate',
   /_rate=_add>0\?\(_fo\/_add\)\*100:null/.test(KM),true);
ck('and the Acquisition caption computes it the same way',
   /const acqRate=\(comp\.contacts>0&&sub\)\?\(\(sub\.contacts\|\|0\)\/comp\.contacts\*100\):null;/.test(cli),true);
// Both "Full App" cards must share one definition, or the row means two things.
ck('Full App Rate uses the SAME onboarded basis as Cost per Full App',
   /_rate=_add>0\?\(_fo\/_add\)\*100:null/.test(KM),true);
// SIX cost cards now: Cost per Sign Up / Inquiry in row one, and the four
// segment costs in row two (2026-11-09). Lower is better on every one.
ck('lower-is-better is set on the six cost cards only',
   (KM.match(/,'\$',true\)/g)||[]).length,6);
ck('and the rate is not inverted',/_foot\(_rate,_pRate\(_kp\),_pRate\(_km\),'%',false\)/.test(KM),true);
// A Windsor outage must blank one card's numbers, not the whole new section.
ck('a failed ad fetch degrades instead of throwing',/_a=\(d\.ads&&!d\.ads\.error\)\?d\.ads:null/.test(KM),true);
ck('and the section says so',/ad data unavailable for this window/.test(KM),true);

// --- the deltas the user asked the worker to provide --------------------
ck('the worker computes prior and last-month windows',
   /const \{ current: cur, prior, priorMonth \} = computeWindows/.test(src),true);
// The segment spends and counts ride along too, or the second row's vs-prior /
// vs-LM footers would be permanently blank.
ck('and exposes the KPI inputs for each, segments included',
   /return \{ spend, fullSub, paidNonLal, inquiries: cp\.contacts \|\| 0,\s*spendCore, spendLal, spendDerm, lalSub, dermSub, dermInq \};/.test(src),true);
// Over the whole fetched superset, not the window's deals: Sign Up is dated by
// credentials completion, and a deal created before the window can sign up in it.
ck('the comparison windows use the Acquisition basis too',
   /const cp = buildClinicianComposition\(deals, contactsByDeal, w\.from, w\.to\);/.test(src),true);
ck('  as does the page window itself',
   /const composition = buildClinicianComposition\(deals, contactsByDeal, cur\.from, cur\.to\);/.test(src),true);
// Dead payload is how the orphaned multiTouchAttribution field happened.
ck('the now-unused virtualReached field is gone',/virtualReached/.test(src),false);
ck('they ride the payload',/kpiPrior, kpiPriorMonth,/.test(src),true);
// The deal fetch is one search over a superset; widening it costs no extra page.
// Trend Analysis reaches back a fixed 100 days whatever the page filter says,
// so the deal fetch must cover it too or the section empties on a short window.
ck('the deals window widens to cover the comparison AND trend windows',
   /for \(const _w of \[prior, priorMonth, trendWin\]\) if \(_w && _w\.from < histFrom\) histFrom = _w\.from;/.test(src),true);
ck('onboarded is read from the PAGE-scoped funnel, not the history one',
   /const fp = buildClinicianFunnel\(dw, contactsByDeal, meetingStarts\);/.test(src),true);
ck('a comparison window with no counterpart returns null',/if \(!w\) return null;/.test(src),true);

// --- the funnel must follow the page window WITHOUT pressing Apply --------
// The handler shipped the HISTORY funnel while the section's header and date
// inputs showed the page window, so the two only agreed after an Apply.
ck('the page-scoped funnel is what ships',/\.\.\.funnelPage,/.test(src),true);
ck('and the history funnel is no longer spread',/\n          \.\.\.funnel,/.test(src),false);
// All Clinicians is a separate, deliberately history-scoped view.
ck('the history table still ships for All Clinicians',/historyRange: \{ from: CLINICIAN_HISTORY_FROM, to: histTo \},/.test(src),true);

// --- CTR -------------------------------------------------------------------
ck('nine Key Metrics tiles, all through the shared _kmTile',(KM.match(/_kmTile\(/g)||[]).length,9);
// Row one: five; row two (compact): Cost Per LAL leads, then Paid Non-LAL and
// the two Derm costs (2026-11-09).
ck('the tile order',
   ['Ad Spend','Cost per Sign Up','Cost per Inquiry','CTR','Sign Up Rate','Cost Per LAL','Cost Per Paid Non-LAL','Cost Per Derm Sign Up','Cost per Derm Inquiry'].map(l=>KM.indexOf("_kmTile('"+l+"'")),
   ['Ad Spend','Cost per Sign Up','Cost per Inquiry','CTR','Sign Up Rate','Cost Per LAL','Cost Per Paid Non-LAL','Cost Per Derm Sign Up','Cost per Derm Inquiry'].map(l=>KM.indexOf("_kmTile('"+l+"'")).slice().sort((a,b)=>a-b));
ck('  and none of them is missing',
   ['Ad Spend','Cost per Sign Up','Cost per Inquiry','CTR','Sign Up Rate','Cost Per LAL','Cost Per Paid Non-LAL','Cost Per Derm Sign Up','Cost per Derm Inquiry'].filter(l=>KM.indexOf("_kmTile('"+l+"'")<0),[]);
ck('the old CPD Paid Non-Lookalikes card is gone',/_kmTile\('CPD Paid Non-Lookalikes'/.test(KM),false);
ck('row one is five across, row two four across',
   [/repeat\(5,minmax\(0,1fr\)\);gap:12px;margin-bottom:12px/.test(KM),/repeat\(4,minmax\(0,1fr\)\);gap:12px;margin-bottom:16px/.test(KM)],[true,true]);
// The whole surface says "Sign Up" now — no card, toggle or calc line may
// still say Full Submission or Full App.
ck('no Full Submission / Full App label survives',
   /_kmTile\('[^']*Full (Submission|App)/.test(cli)||/l:'Full Submission'/.test(cli)
   ||/acqPill\('submission','Full Submission'/.test(cli),false);
// The rename: "Full App" was never what this card divided by.
ck('Cost per Full App is gone',/Cost per Full App/.test(cli),false);
// Cumulative, not the mean of daily rates — that would weight a 10-impression
// day the same as a 10,000-impression one.
ck('CTR is total clicks over total impressions',
   /const _ctr=_ctrOk\?\(_tf\.clicks\/_tf\.impressions\)\*100:null;/.test(KM),true);
ck('and never averages a per-row ctr field',/_tf\.ctr\b/.test(KM),false);
ck('clicks are summed alongside impressions in the worker',
   /ck \+= parseInt\(r\.clicks\) \|\| 0;/.test(src)&&/'date,campaign_name,impressions,clicks,link_clicks'/.test(src),true);
ck('on clinician campaigns only',/if \(!isClinicianCampaign\(r\.campaign_name\)\) continue;/.test(src),true);
ck('zero impressions yields a dash, not a divide-by-zero',
   /_ctrOk=\(_tf\.impressions!=null&&_tf\.impressions>0&&_tf\.clicks!=null\)/.test(KM),true);

// --- Daily Activity removed ------------------------------------------------
// The section is gone from the Clinician page. Trend Analysis covers the same
// timeline at every grain and carries the ad spend alongside it, so a
// bars-per-day card was the same data one level less useful.
ck('the Daily Activity card is gone',/card\('Daily Activity'/.test(cli),false);
ck('and its daily-average tile with it',/mini\('Daily Avg'/.test(cli),false);
// The WORKER still computes dailyAvg — nothing else reads it today, so it is
// dead weight on the payload rather than a bug, and removing a builder is a
// separate call from removing a card.
ck('the worker still computes it, unused',
   /dailyAvg: _spanDays > 0 \? total \/ _spanDays : null, days: _spanDays/.test(src),true);

// --- Clinician Specialty pie (2026-11-06) -----------------------------------
// Right after Type, on the same panel builder, and following the Inquiry / Sign
// Up toggle like every other Acquisition pie.
ck('Clinician Specialty is an Acquisition panel, right after Type',
   /acqPanel\('Type of Clinician'[^\n]*\n\s*\+acqPanel\('Clinician Specialty',acqSrc\.specialty\|\|\[\][^\n]*\n\s*\+acqPanel\(udLabel/.test(cli),true);
ck('the Sign Up population gets its own specialty mix',
   /specialty: capped\(subSpec, 9\),/.test(src)&&/subSpec\[sp\] = \(subSpec\[sp\] \|\| 0\) \+ 1;/.test(src),true);
// Run the builder: a clinician created before the window who signs up inside it
// must land in the Sign Up specialty mix only, and the two mixes must each sum
// to their own population.
{
  const sb={console:{log(){},warn(){},error(){}}};vm.createContext(sb);
  let ok=true;
  try{ vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nglobalThis.BC=buildClinicianComposition;',sb); }
  catch(e){ok=false;console.log('  (could not load worker: '+e.message+')');}
  if(ok&&typeof sb.BC==='function'){
    const deals=[{id:'1',properties:{}},{id:'2',properties:{}},{id:'3',properties:{}}];
    const C={
      '1':{__cid:'a',createdate:'2026-11-02T15:00:00Z',clinician_specialty:'Dermatology',clinician_type:'MD'},
      '2':{__cid:'b',createdate:'2026-11-03T15:00:00Z',clinician_specialty:'',clinician_type:'NP'},
      '3':{__cid:'c',createdate:'2026-10-01T15:00:00Z',clinician_specialty:'Dermatology',clinician_type:'MD',clinician_credentials_completed:'2026-11-04'},
    };
    let r=null;
    try{ r=sb.BC(deals,C,'2026-11-01','2026-11-06'); }catch(e){ console.log('  (builder threw: '+e.message+')'); }
    ck('the builder ran',!!r,true);
    if(r){
      ck('inquiry specialty mix counts the window\'s contacts, blank as Not set',
         JSON.parse(JSON.stringify(r.specialty)),[{label:'Dermatology',count:1},{label:'Not set',count:1}]);
      ck('  and sums to the inquiry count',r.specialty.reduce((a,x)=>a+x.count,0),r.contacts);
      ck('a sign up created before the window lands in the Sign Up specialty mix',
         JSON.parse(JSON.stringify(r.submission.specialty)),[{label:'Dermatology',count:1}]);
      ck('sign up specialty mix sums to the sign up count',
         ((r.submission&&r.submission.specialty)||[]).reduce((a,x)=>a+x.count,0),r.submission.contacts);
    }
  }
}

// --- Type of Clinician pie groups spellings (2026-11-09) ----------------------
{
  const sb={console:{log(){},warn(){},error(){}}};vm.createContext(sb);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nglobalThis.BC=buildClinicianComposition;globalThis.G=CLINICIAN_TYPE_GROUPS;',sb);
  const T=['MD','DO','MD/DO','NP','Nurse Practitioner','PA','RDN with Master\u2019s Degree','Pharmacist'];
  const deals=T.map((_,i)=>({id:'t'+i,properties:{}}));
  const C={};T.forEach((ty,i)=>{C['t'+i]={__cid:'p'+i,createdate:'2026-11-03T15:00:00Z',clinician_type:ty};});
  const r=sb.BC(deals,C,'2026-11-01','2026-11-06');
  ck('the Type pie counts each spelling group as one slice',
     r.type.map(x=>x.label).sort(),['MD/DO','Nurse Practitioner','Pharmacist','Physician Assistant','RDN']);
  const m=Object.fromEntries(r.type.map(x=>[x.label,x.count]));
  ck('  MD/DO, NP, PA and RDN each fold their spellings',
     [m['MD/DO'],m['Nurse Practitioner'],m['Physician Assistant'],m['RDN'],m['Pharmacist'],m['MD'],m['NP']],
     [3,2,1,1,1,undefined,undefined]);
  ck('  and the slices still sum to the clinicians', r.type.reduce((a,x)=>a+x.count,0), r.contacts);
  // One grouping, two copies (worker pie, client trend dropdown): they must agree.
  const cg=(cli.match(/groups:\{'MD\/DO':\[[\s\S]*?\]\}\}/)||[''])[0];
  const clientGroups=vm.runInContext('({'+cg.replace(/^groups:\{/,'').replace(/\}$/,'')+')',vm.createContext({}));
  ck('the worker and the Trend dropdown group types identically',
     JSON.stringify(clientGroups),JSON.stringify(JSON.parse(JSON.stringify(sb.G))));
}

// --- Specialty pie merges duplicate names (2026-11-09) -------------------------
{
  const sb={console:{log(){},warn(){},error(){}}};vm.createContext(sb);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nglobalThis.BC=buildClinicianComposition;globalThis.G=CLINICIAN_SPECIALTY_GROUPS;',sb);
  const SP=['PCP / family medicine','Family Medicine','Pediatrician','OB/GYN','Obstetrics and Gynecology','Dermatology'];
  const deals=SP.map((_,i)=>({id:'s'+i,properties:{}}));
  const C={};SP.forEach((x,i)=>{C['s'+i]={__cid:'q'+i,createdate:'2026-11-03T15:00:00Z',clinician_specialty:x};});
  const r=sb.BC(deals,C,'2026-11-01','2026-11-06');
  const m=Object.fromEntries(r.specialty.map(x=>[x.label,x.count]));
  ck('the Specialty pie merges duplicate names',
     [m['Family Medicine'],m['Pediatrics'],m['Obstetrics and Gynecology'],m['Dermatology'],m['OB/GYN'],m['Pediatrician']],
     [2,1,2,1,undefined,undefined]);
  const cg=(cli.match(/groups:\{'Family Medicine':\[[\s\S]*?\]\}\}/)||[''])[0];
  // Missing list = a failed check, not a crash that hides the rest of the file.
  const clientGroups=cg?vm.runInContext('({'+cg.replace(/^groups:\{/,'').replace(/\}$/,'')+')',vm.createContext({})):null;
  ck('the worker and the Trend dropdown merge specialties identically',
     JSON.stringify(clientGroups),JSON.stringify(JSON.parse(JSON.stringify(sb.G))));
}
// The dropdown shows plain names, not the spellings a group folds in.
ck('dropdown options carry no "(incl. ...)" suffix',/\(incl\. /.test(fn(cli,'_clnTrendCard')),false);

// --- Acquisition Specialty filter (2026-11-09) --------------------------------
{
  const sb={console:{log(){},warn(){},error(){}}};vm.createContext(sb);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nglobalThis.BC=buildClinicianComposition;',sb);
  const P=[ // [specialty, type, utm_source, signs up]
    ['Dermatology','MD','fb',true],['Dermatology','PA','ig',false],['Dermatology','NP','fb',true],
    ['Pediatrician','MD','fb',false],['Pediatrics','DO','google',true],['','NP','',false],
  ];
  const deals=P.map((_,i)=>({id:'z'+i,properties:{}}));
  const C={};P.forEach((x,i)=>{C['z'+i]={__cid:'z'+i,createdate:'2026-11-03T15:00:00Z',clinician_specialty:x[0],clinician_type:x[1],
    utm_source:x[2],clinician_credentials_completed:x[3]?'2026-11-04':''};});
  const r=sb.BC(deals,C,'2026-11-01','2026-11-06');
  const m=l=>Object.fromEntries((l||[]).map(x=>[x.label,x.count]));
  ck('inquiry cross-tab is keyed by the grouped specialty names',Object.keys(r.bySpecialty).sort(),['Dermatology','Not set','Pediatrics']);
  ck('  the Dermatology type mix',m(r.bySpecialty.Dermatology.type),{'MD/DO':1,'Physician Assistant':1,'Nurse Practitioner':1});
  ck('  Pediatrician + Pediatrics are one specialty, types grouped',m(r.bySpecialty.Pediatrics.type),{'MD/DO':2});
  ck('  the Dermatology UTM source mix',m(r.bySpecialty.Dermatology.utmSource),{fb:2,ig:1});
  ck('  per-specialty counts sum to the inquiries',Object.values(r.bySpecialty).reduce((a,x)=>a+x.contacts,0),r.contacts);
  ck('sign up cross-tab follows the Sign Up population',
     [r.submission.bySpecialty.Dermatology.contacts,m(r.submission.bySpecialty.Dermatology.type)],[2,{'MD/DO':1,'Nurse Practitioner':1}]);
  ck('  and sums to the sign ups',Object.values(r.submission.bySpecialty).reduce((a,x)=>a+x.contacts,0),r.submission.contacts);
}
{
  const RC2=fn(cli,'renderClinician');
  ck('the Specialty select sits in the Acquisition controls',/\+'<\/div>'\s*\n\s*\+specSel/.test(RC2)&&/onchange="clinicianSetAcqSpec\(this\.value\)"/.test(RC2),true);
  ck('it filters the Type and UTM pies, each over its own total',
     /acqPanel\('Type of Clinician'\+\(acqSpec\?[^)]*\):''\),_fSrc\.type\|\|\[\],null,_fTot\)/.test(RC2)
     &&/acqPanel\(udLabel\+\(acqSpec\?[^)]*\):''\),_fSrc\[ud\]\|\|\[\],utmToggle,_fTot\)/.test(RC2),true);
  ck('the Specialty pie stays the full mix',/acqPanel\('Clinician Specialty',acqSrc\.specialty\|\|\[\],null,acqTotal\)/.test(RC2),true);
  ck('the filter follows the Inquiry / Sign Up toggle',/const _bs=\(acqMode==='submission'&&sub\)\?_bsS:_bsI;/.test(RC2),true);
  ck('a pick that no longer exists falls back to All',/_specOpts\.indexOf\(S\.clinician\.acqSpec\)>=0\)\?S\.clinician\.acqSpec:''/.test(RC2),true);
}

// --- Clinician campaign matcher (2026-11-06) ---------------------------------
{
  const sb={};vm.createContext(sb);
  vm.runInContext([(src.match(/const CLINICIAN_CAMPAIGN_NAMES = new Set\(\[[\s\S]*?\]\);/)||[''])[0],
    (src.match(/const CLINICIAN_CAMPAIGN_ID_NAMES = \{[\s\S]*?\};/)||[''])[0],
    fn(src,'resolveUtmCampaign'),fn(src,'isClinicianCampaign'),'globalThis.f=isClinicianCampaign;'].join('\n'),sb);
  const f=sb.f;
  ck('Clinician Targeting campaigns still match',f('S01 - TOF - Clinician Targeting - 2% Lookalikes'),true);
  ck('the Dermatologists campaign is a Clinician campaign',f('S01 - TOF - Dermatologists - CAPI'),true);
  ck('  regardless of case and spacing',f('  s01 - tof -  dermatologists - capi '),true);
  ck('a brand campaign that mentions a specialty does not',f('S01 - TOF - Dermatologist Recommended Brands'),false);
  ck('brand campaigns stay Meta',f('S01 - TOF - Cold - Supplements'),false);
  ck('blank is not clinician',f(''),false);
}

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
