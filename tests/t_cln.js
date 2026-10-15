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
ck('Key Metrics is prepended, not appended',/h=_clnKeyMetricsHTML\(d,''\)\+h;/.test(RC),true);
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
ck('Type of Clinician is now an Acquisition panel',/acqPanel\('Type of Clinician',acqSrc\.type\|\|\[\]\)/.test(cli),true);
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
// THREE cost cards now: CPD Paid Non-Lookalikes joined the two. Each divides
// spend by a count, so lower is better on all three.
ck('lower-is-better is set on the three cost cards only',
   (KM.match(/,'\$',true\)/g)||[]).length,3);
ck('and the rate is not inverted',/_foot\(_rate,_pRate\(_kp\),_pRate\(_km\),'%',false\)/.test(KM),true);
// A Windsor outage must blank one card's numbers, not the whole new section.
ck('a failed ad fetch degrades instead of throwing',/_a=\(d\.ads&&!d\.ads\.error\)\?d\.ads:null/.test(KM),true);
ck('and the section says so',/ad data unavailable for this window/.test(KM),true);

// --- the deltas the user asked the worker to provide --------------------
ck('the worker computes prior and last-month windows',
   /const \{ current: cur, prior, priorMonth \} = computeWindows/.test(src),true);
// FOUR inputs now: paidNonLal joined, or the CPD Paid Non-Lookalikes tile's
// vs-prior / vs-LM footer would be permanently blank.
ck('and exposes the four KPI inputs for each',
   /return \{ spend, fullSub, paidNonLal, inquiries: cp\.contacts \|\| 0 \};/.test(src),true);
ck('the comparison windows use the Acquisition basis too',
   /const cp = buildClinicianComposition\(dw, contactsByDeal, w\.from, w\.to\);/.test(src),true);
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
ck('six Key Metrics tiles, all through the shared _kmTile',(KM.match(/_kmTile\(/g)||[]).length,6);
// Order is load-bearing: CPD Paid Non-Lookalikes sits directly right of Ad
// Spend, because it answers what that spend bought.
ck('the tile order',
   ['Ad Spend','CPD Paid Non-Lookalikes','Cost per Sign Up','Cost per Inquiry','CTR','Sign Up Rate']
     .map(l=>KM.indexOf("_kmTile('"+l+"'")),
   ['Ad Spend','CPD Paid Non-Lookalikes','Cost per Sign Up','Cost per Inquiry','CTR','Sign Up Rate']
     .map(l=>KM.indexOf("_kmTile('"+l+"'")).slice().sort((a,b)=>a-b));
ck('  and none of them is missing',
   ['Ad Spend','CPD Paid Non-Lookalikes','Cost per Sign Up','Cost per Inquiry','CTR','Sign Up Rate']
     .filter(l=>KM.indexOf("_kmTile('"+l+"'")<0),[]);
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

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
