// Executive Dashboard: three sections borrowed from two pages under one
// Brand/Clinician pill. The risk is not that it looks wrong — it is that it
// quietly becomes a SECOND implementation of rows that already exist, free to
// drift from the pages they came from. These assertions pin the sharing.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. The page is registered everywhere a page has to be ────────────────
// There is no page registry in this file — a page is a convention spread over
// ~10 edit sites, and missing one fails silently (a nav item that never
// highlights, a title that never updates, a render that never dispatches).
const SITES=[
  ['nav entry, first in the sidebar', /<div class="ni" onclick="switchDashSub\('exec'\)" data-page="exec" data-sub="exec">/],
  ['page container',                  /<div class="pg" id="page-exec">/],
  ['state bucket',                    /const S=\{exec:\{win:'mtd',data:null,mode:'brand'/],
  ['title map (T)',                   /const T=\{exec:'Executive Dashboard'/],
  ['sub-nav AP map',                  /var apMap=\{exec:'exec'/],
  ['sub-nav title map',               /var titleMap=\{exec:'Executive Dashboard'/],
  ['renderPage dispatch',             /if\(AP==='exec'\)renderExec\(\)/],
  ['nav highlight token',             /const map=\{exec:"switchDashSub\('exec'\)"/],
  ['section nav selector',            /_SECNAV_SEL=\{exec:'#page-exec \.dsec-h'/],
  ['section spacing CSS',             /#page-exec>div\{margin-bottom:24px\}/],
];
for(const [what,re_] of SITES)ck('registered: '+what,re_.test(cli),true);
ck('it is the landing page',/\};let AP='exec';/.test(cli),true);
ck('  so the static title names it',/id="ptitle"[^>]*>Executive Dashboard</.test(cli),true);
ck('the nav lists it before Brand and Clinician',
   cli.indexOf("switchDashSub('exec')")<cli.indexOf("switchDashSub('overview')")
   &&cli.indexOf("switchDashSub('overview')")<cli.indexOf('clinicianNav()'),true);

// ── 2. Irfan is superseded in the NAV, not deleted from the file ─────────
// S.irfan is the shared ad-table state bucket for every page that shows those
// tables, so deleting the Irfan page would take the Brand and Clinician ad
// tables with it. The nav entry goes; the code stays.
ck('the Irfan nav entry is gone',/switchDashSub\('irfan'\)">/.test(cli)||/data-sub="irfan"/.test(cli),false);
ck('but its page and renderer remain',
   /<div class="pg" id="page-irfan">/.test(cli)&&/function renderIrfan\(/.test(cli),true);
ck('  and S.irfan survives, because the ad tables are built on it',
   /S\.irfan\.metaWin/.test(cli)&&/irfan:\{win:'mtd'/.test(cli),true);

// ── 3. The pill ───────────────────────────────────────────────────────────
ck('two modes, Brand first',
   [...cli.matchAll(/pill\('(brand|clinician)','([^']+)'/g)].map(m=>m[2]),['Brand','Clinician']);
ck('Brand is the default',/mode:'brand'/.test(cli)&&/S\.exec&&S\.exec\.mode==='clinician'\)\?'clinician':'brand'/.test(cli),true);
ck('it sits above the first section',
   cli.indexOf('id="exec-mode-bar"')<cli.indexOf('id="exec-km"'),true);
ck('and the sections run Key Metrics, Trend, Ad Tables',
   cli.indexOf('id="exec-km"')<cli.indexOf('id="exec-trend"')
   &&cli.indexOf('id="exec-trend"')<cli.indexOf('id="exec-ads"'),true);

// ── 4. Nothing is copied — every section calls the owning page's builder ──
ck('Key Metrics reuses both builders',
   /_clnKeyMetricsHTML\(d,'-exec'\):_brandKeyMetricsHTML\(d,\{sfx:'-exec',hideCpqd:true\}\)/.test(cli),true);
ck('Trend reuses both builders',
   /_clnTrendCard\(d,'-exec'\):_dashTrendCard\(d,'-exec'\)/.test(cli),true);
ck('  and their post-render step',
   /_renderClnTrend\(d,'-exec'\); else _renderDashTrend\(d,'-exec'\)/.test(cli),true);
ck('the owning pages call the SAME builders',
   /_brandKeyMetricsHTML\(d,\{sfx:'',hideCpqd:false/.test(cli)&&/\+_clnKeyMetricsHTML\(d,''\)\+h;/.test(cli),true);
// Ad tables already had a multi-page target list; this extends it rather than
// adding a fourth renderer.
for(const k of ['campaigns','creatives','audiences','adsets'])
  ck('ad tables render into exec-meta-'+k,
     (cli.split("getElementById('exec-meta-"+k+"')").length-1),2);
ck('  and the filter host too',/getElementById\('exec-meta-filters'\)/.test(cli),true);
ck('the channel resolver knows the new page',
   /if\(AP==='exec'\)return _execMode\(\)==='clinician'\?'clinician':\(S\.dashboard\.activeAdChannel\|\|'meta'\)/.test(cli),true);

// ── 5. Ids are suffixed per host, because pages share one DOM ────────────
// getElementById returns the FIRST match in the document, so an unsuffixed
// second copy does not merely duplicate — it steals the original's updates.
for(const [what,pat] of [
  ['Key Metrics heading', /id="dsec-keymetrics'\+sfx\+'"/],
  ['Revenue Outcome',     /id="t-revenue'\+sfx\+'"/],
  ['Brand trend panel',   /id="dsh-trend-panels'\+sfx\+'"/],
  ['Clinician trend panel',/id="cln-trend-panels'\+sfx\+'"/],
  ['Clinician KM heading',/id="dsec-clnkm'\+sfx\+'"/],
])ck('host-scoped id: '+what,pat.test(cli),true);
ck('fdRevenueOutcome repaints the Executive copy by its own id',
   /AP==='exec'&&_execMode\(\)==='brand'[\s\S]{0,160}?getElementById\('t-revenue-exec'\)/.test(cli),true);

// ── 6. Two payloads, fetched from the right endpoint ─────────────────────
ck('Brand mode reads /api/data, Clinician mode /api/clinician',
   /_execMode\(\)==='clinician'\?\(S\.clinician&&S\.clinician\.data\):\(S\.exec&&S\.exec\.data\)/.test(cli),true);
ck('the sub-nav routes the page to its own loader',/if\(AP==='exec'\)\{ _execEnsureData\(\); return; \}/.test(cli),true);
// This routing used to be inlined in selW, and selQ/goC carried their own
// copies that had NOT grown the exec branch — so the Custom range and the
// quarter pills ran the brand fetcher while Clinician mode read a payload that
// never refetched. It lives in _fetchForWindow now, and all three go through it.
const _router=fn(cli,'_fetchForWindow');
ck('a time-frame change refetches the payload the pill is showing',
   /if\(AP==='exec'\)\{[\s\S]{0,400}?_execMode\(\)==='clinician'[\s\S]{0,200}?fdClinician\(win\);[\s\S]{0,120}?fd\(win,/.test(_router),true);
ck('  from every entry point, not just the preset pills',
   (cli.match(/await _fetchForWindow\(/g)||[]).length,3);
ck('  with no entry point dispatching on AP itself',
   ['selW','selQ','goC'].filter(n=>/AP==='(clinician|webinarPerf|exec)'/.test(fn(cli,n))),[]);
ck('fdClinician re-renders the Executive page too',
   /function _clnPage\(\)\{return AP==='clinician'\|\|\(AP==='exec'&&_execMode\(\)==='clinician'\);\}/.test(cli),true);
ck('  and is used for both its render points',(cli.match(/if\(_clnPage\(\)\)renderPage\(\)/g)||[]).length,2);

// ── 7. ONE derivation of the Key Metrics figures ─────────────────────────
// The real drift hazard: two pages computing the same numbers twice.
ck('a shared model exists',/function _dashKmModel\(d\)\{/.test(cli),true);
ck('the builder reads it',/const M=_dashKmModel\(d\);/.test(cli),true);
ck('and renderDash reads it too, rather than recomputing',
   /const _KM=d\?_dashKmModel\(d\):null;/.test(cli)&&/const qd=_KM\.qd;/.test(cli)
   &&/const cpqd=_KM\.cpqd,cpqdPV=_KM\.cpqdPV,cpqdLMV=_KM\.cpqdLMV/.test(cli),true);
ck('  so the qualified-demo derivation exists once',
   cli.split('\n').filter(l=>/qmeta\.qualDemoCount\b/.test(l)).length,1);

// ── 8. It actually renders ────────────────────────────────────────────────
// Source matching cannot tell you the row builds. Run the real builder.
const ctx={console,f$:v=>'$'+Math.round(v).toLocaleString(),fN:v=>Number(v||0).toLocaleString()};
vm.createContext(ctx);
vm.runInContext([
  'var window=this;var f$=this.f$,fN=this.fN;var S={dashboard:{win:'+"'mtd'"+'}};',
  'function _irTileFooterDash(){return "";}function _buildRevenueOutcomeCard(){return "";}',
  // _kmTile / _kmPaceBar are module scope now, shared with the Clinician
  // section, so the harness has to load them like any other dependency.
  decl(cli,'const _KM_LBL=',';'), fn(cli,'_kmTile'), fn(cli,'_kmPaceBar'),
  fn(cli,'_dashKmModel'), fn(cli,'_brandKeyMetricsHTML'),
  'Object.assign(globalThis,{_dashKmModel,_brandKeyMetricsHTML});',
].join('\n'),ctx);
const D={executiveSummary:{_meta:{totalSpend:48210,pTotalSpend:44100,pmTotalSpend:51300,demosHeldCount:59},
   totalCpqd:{value:812,sameTimePrior:{value:905},lastMonth:{value:770}},lowTrafficPct:{_meta:{}}},
 demoTracking:{_qualMeta:{qualDemoCount:59},scheduledRegularTotal:96,scheduledRegularTotalPrior:88,scheduledRegularTotalLastMonth:104,
   scheduledPreTotal:19,scheduledPreTotalPrior:22,scheduledPreTotalLastMonth:31,
   scheduledRegularPaid:27,scheduledRegularOrganicTagged:51,scheduledRegularUntagged:18,
   scheduledRegularPaidPrior:24,scheduledRegularPaidLastMonth:21},
 webPerformance:{visitors:{value:3984,sameTimePrior:{value:3600},lastMonth:{value:4200}}},
 adSpend:{channels:{meta:{budget:40000}},total:{spend:48210}}};
const labels=h=>[...h.matchAll(/class="tlbl"[^>]*>([^<]*)</g)].map(m=>m[1]);
const brand=ctx._brandKeyMetricsHTML(D,{sfx:'',hideCpqd:false,win:'mtd'});
const exec =ctx._brandKeyMetricsHTML(D,{sfx:'-exec',hideCpqd:true,win:'7d'});
ck('the Brand row still has seven tiles',labels(brand).length,7);
ck('  including CPQD',labels(brand).indexOf('CPQD · Regular Demos')>=0,true);
ck('the Executive row drops exactly CPQD',
   labels(brand).filter(l=>labels(exec).indexOf(l)<0),['CPQD · Regular Demos']);
ck('  and the grid loses a column with it',
   [(brand.match(/repeat\((\d)/)||[])[1],(exec.match(/repeat\((\d)/)||[])[1]],['7','6']);
ck('the two rows carry different ids',
   [(brand.match(/id="(t-revenue[^"]*)"/)||[])[1],(exec.match(/id="(t-revenue[^"]*)"/)||[])[1]],
   ['t-revenue','t-revenue-exec']);
ck('  and different headings',
   [(brand.match(/id="(dsec-keymetrics[^"]*)"/)||[])[1],(exec.match(/id="(dsec-keymetrics[^"]*)"/)||[])[1]],
   ['dsec-keymetrics','dsec-keymetrics-exec']);
// Every shared figure must be identical — same payload, same row.
const shared=labels(exec);
ck('every tile the two share reads the same value',
   shared.filter(l=>{
     const v=h=>{const i=h.indexOf('>'+l+'<');return h.slice(i,i+400).match(/class="tval">([^<]*)</)[1];};
     return v(brand)!==v(exec);}),[]);
// The pacing bar used to read S.dashboard.win directly — a third page would
// have silently shown the Brand page's window.
// Now inside the hoisted _kmPaceBar, which BOTH sections call — so reading a
// page global there would hand the Clinician section the Brand page's window.
const _paceSrc=fn(cli,'_kmPaceBar');
ck('the window is a parameter, not the Brand page’s global',
   /^function _kmPaceBar\(spend,budget,win\)/.test(_paceSrc)&&/const w=win\|\|'mtd';/.test(_paceSrc),true);
ck('  and the pacing bar reads no page state at all',/\bS\./.test(_paceSrc),false);


// ── Section rhythm ────────────────────────────────────────────────────────
// The three sections come from builders written for three different
// dashboards and disagree about their own bottom margin: the Brand metrics row
// emits none at all, the Clinician ones 16px, anything carrying .sec 24px. So
// the gap between Key Metrics and Trend Analysis changed when the Brand /
// Clinician pill was switched. The CONTAINER owns the gap now.
ck('the container sets the gap',/#page-exec>div\{margin-bottom:24px\}/.test(cli),true);
ck('  and the last section does not add one',/#page-exec>div:last-child\{margin-bottom:0\}/.test(cli),true);
ck('  while whatever each builder emitted is overridden',
   /#page-exec>div>:last-child\{margin-bottom:0 !important\}/.test(cli),true);
// Every exec section sits in its own wrapper, so every .dsec there was a
// :first-child and the 36px top margin was cancelled on all of them — the rule
// looked like it was spacing the page and never did.
// ".dsec-h" is the section-nav selector and must survive; only the margin rule
// goes, so this matches .dsec NOT followed by a word character or dash.
ck('the dead top-margin rule is gone',/#page-exec \.dsec(?![-\w])/.test(cli),false);
ck('  while the section-nav selector survives',
   /_SECNAV_SEL=\{exec:'#page-exec \.dsec-h'/.test(cli),true);
ck('  but the pages that still use it keep it',
   /#page-dashboard \.dsec,#irfan-c>\.dsec\{margin-top:36px\}/.test(cli),true);
// Nothing ever wrote to #exec-date-header, so it was an empty block holding
// 24px open above the mode bar.
ck('the never-populated date header is gone',/exec-date-header/.test(cli),false);
ck('  leaving the mode bar first in the page',
   /<div class="pg" id="page-exec">\s*<div id="exec-mode-bar">/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
