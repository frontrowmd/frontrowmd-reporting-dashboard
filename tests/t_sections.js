// Section order, the collapsed field appendices, the Ad Tables date seed and
// the removal of the Clinician ad channel.
const fs=require('fs');
let pass=0,fail=0;
function ck(n,g,w){const ok=JSON.stringify(g)===JSON.stringify(w);ok?pass++:fail++;
  console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));}
const html=fs.readFileSync(process.env.DASH_SRC||'dashboard.html','utf8');
const at=id=>html.indexOf(id);

// ── Order on the Detailed Dashboard ──
const order=['id="dsec-demoinsights"','id="dsec-cvrfunnel"','id="sec-row2"',
             'id="sec-adkpis"','id="dsec-adchannelperf"','id="dsec-adtables"',
             'id="dsec-demoattendance"','id="sec-field-ref"'];
ck('page sections run in the intended order',
   order.map(at).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);
ck('Ad KPIs sits above Ad Channel Performance',at('id="sec-adkpis"')<at('id="dsec-adchannelperf"'),true);
ck('Demo Attendance sits below the Ad Tables',at('id="dsec-adtables"')<at('id="dsec-demoattendance"'),true);
ck('and above the field reference',at('id="dsec-demoattendance"')<at('id="sec-field-ref"'),true);

// ── Field reference collapses, with its OWN ids ──
// All pages share one DOM, so reusing the Clinician page's cln-ref-* ids would
// put two elements with the same id in the document.
ck('field cards collapse',/id="dash-ref-'\+k\+'" style="display:none/.test(html),true);
ck('they start collapsed with a caret',/id="dash-refcaret-'\+k\+'"/.test(html),true);
ck('a dedicated toggle exists',/window\.dashRefToggle=function\(k\)\{/.test(html),true);
ck('it targets its own ids, not the Clinician page\'s',
   /getElementById\('dash-ref-'\+k\)/.test(html)&&/getElementById\('dash-refcaret-'\+k\)/.test(html),true);
ck('the Clinician toggle is untouched',/window\.clnRefToggle=function\(k\)\{/.test(html),true);
ck('field counts are surfaced on the collapsed header',/\+' fields<\/span>/.test(html),true);

// ── Ad Tables date window seeds from the page ──
// Behaviour lives in t_win.js, which exercises the real _metaSeedWin. These
// check the wiring: that the logic is reachable and that only the two
// user-facing setters can pin the window.
ck('the seed logic is its own function',/function _metaSeedWin\(winOverride\)\{/.test(html),true);
ck('the loader calls it',/  _metaSeedWin\(winOverride\);/.test(html),true);
ck('it follows the page unless pinned',/if\(S\.irfan\.metaWinPinned\)\{/.test(html),true);
ck('winOverride is actually read now',/let pw=winOverride\|\|\(S\[AP\]&&S\[AP\]\.win\)\|\|'mtd';/.test(html),true);
ck('the Detailed render still passes the page window',
   /_renderIrfanMetaTablesLoader\(S\.dashboard\?\.win\|\|'mtd'\)/.test(html),true);
ck('a custom page window carries its dates over',
   /S\.irfan\.metaWin='custom';S\.irfan\.metaCF=cr\.from;S\.irfan\.metaCT=cr\.to;/.test(html),true);
ck('the window is validated against the bar\'s own options',
   /const OK=\['7d','mtd','lastMonth','ytd','allTime','custom'\];/.test(html),true);
// Exactly two places may pin: the Table Dates pills and the custom Apply.
ck('only the user-facing setters pin the window',
   (html.match(/S\.irfan\.metaWinPinned=true/g)||[]).length,2);
ck('the pills setter pins',/window\.irfanMetaWin=function\(win\)\{S\.irfan\.metaWinPinned=true;/.test(html),true);
ck('the custom Apply pins',/S\.irfan\.metaWinPinned=true;S\.irfan\.metaWin='custom';/.test(html),true);
ck('nothing else writes metaWin outside the seed and those two',
   (html.match(/S\.irfan\.metaWin=/g)||[]).length,6);
ck('no stale "default MTD" claim remains',/default MTD/.test(html),false);

// ── Clinician ad channel removed ──
ck('no Clinician tab in the Detailed switcher',/data-ch="clinician"/.test(html),false);
ck('nor in any other switcher on the page',/\['clinician','Clinician'/.test(html),false);
// The Ad KPIs roll-up already skipped kpiExcluded channels, so only its tab
// exposed Clinician; with that gone the page has no path to it at all.
ck('the Ad KPIs roll-up still guards on kpiExcluded',/if\(c\.kpiExcluded\)\{excl\.push/.test(html),true);
ck('the perf table skips the channel',/if\(k==='clinician'\)continue;/.test(html),true);
ck('a stale saved selection cannot strand the user',
   /if\(ch==='clinician'\)\{ch='meta';S\.dashboard\.activeAdChannel='meta';\}/.test(html),true);
ck('the Clinician page still resolves its own channel',/if\(AP==='clinician'\)return 'clinician';/.test(html),true);

// ── Ad Tables loader: states, bad windows, and last-click-wins ──────
const L0=html.indexOf('function _renderIrfanMetaTablesLoader(');
const L1=html.indexOf('window.irfanMetaWin=function',L0);
const ldr=html.slice(L0,L1);

// The KPI cards read the same payload, so they get the same loading/error
// states; _renderAdKpis only ever runs on the success path.
ck('Ad KPIs share the tables\' loading and error states',
   /\.\.\.adsetTargets,\.\.\.kpiTargets\]\.forEach/.test(ldr),true);
ck('kpiTargets is resolved from the DOM',/const kpiTargets=\[document\.getElementById\('ad-kpis'\)\]\.filter\(Boolean\)/.test(ldr),true);
// The early return must stay keyed on the TABLE targets only: a page holding
// #ad-kpis in the shared DOM but no tables must not start fetching.
ck('the early return still keys on table targets only',
   /if\(!campTargets\.length&&!creTargets\.length&&!audTargets\.length&&!adsetTargets\.length\)return;/.test(ldr),true);
ck('and does not consider kpiTargets',/kpiTargets\.length\)return/.test(ldr),false);

// Custom is selected before its dates are applied, so the window is briefly
// 'custom' with no range; fetching it POSTs a request the worker rejects.
ck('a dateless custom window is not fetched',
   /if\(irfWin==='custom'&&!\(_cr\.from&&_cr\.to\)\)\{/.test(ldr),true);
ck('it explains what to do instead of erroring',/Pick a start and end date above, then click Apply\./.test(ldr),true);
ck('the guard sits before the fetch',
   ldr.indexOf("irfWin==='custom'&&!(_cr.from&&_cr.to)")<ldr.indexOf('fetch(AZ_API'),true);

// Every page-window change now issues an analyzer POST, so responses can land
// out of order. Each render path bails if a newer request has started.
ck('a monotonic sequence is taken per request',
   /const _mySeq=\(S\.irfan\._metaSeq=\(S\.irfan\._metaSeq\|\|0\)\+1\);/.test(ldr),true);
ck('all three render paths check it',(ldr.match(/_mySeq!==S\.irfan\._metaSeq\)return/g)||[]).length,3);
ck('the sequence is taken before the fetch starts',
   ldr.indexOf('const _mySeq=')<ldr.indexOf('fetch(AZ_API'),true);
// Order matters: bailing after the render would not prevent anything.
ck('the success guard precedes the render',
   ldr.indexOf('if(_mySeq!==S.irfan._metaSeq)return;')<ldr.indexOf('_renderIrfanMetaTables(ad,_getCh())'),true);
// The payload is still cached under its own key -- it is correct for that
// window even when a newer one superseded it on screen.
ck('a superseded payload is still cached',
   ldr.indexOf('_azCachePut(_ck,ad);')<ldr.indexOf('if(_mySeq!==S.irfan._metaSeq)return;'),true);

// ── Re-sync control ────────────────────────────────────────────────
// Picking a table window pins it; before this there was no way back to
// following the page short of a reload.
ck('a re-sync control exists',/window\.irfanMetaResync=function\(\)\{/.test(html),true);
ck('it drops the pin',/S\.irfan\.metaWinPinned=false;/.test(html),true);
ck('and re-renders with the page window',
   /_renderIrfanMetaTablesLoader\(AP==='dashboard'\?\(S\.dashboard&&S\.dashboard\.win\):undefined\)/.test(html),true);
ck('the button is only offered once pinned',
   /if\(S\.irfan\.metaWinPinned\)\{\n\s*wp\+='<button class="cln-fp"/.test(html),true);
ck('it is wired to the handler',/onclick="irfanMetaResync\(\)"/.test(html),true);
// Exactly one place clears the pin, and it is that control.
ck('only the re-sync clears the pin',(html.match(/metaWinPinned=false/g)||[]).length,1);

// ── Ad KPIs card leads with spend, so it is titled for spend ──
ck('the card is titled Ad Spend',/h\+=kpi\('Ad Spend',\n\s*f\$0\(src\.spend\),/.test(html),true);
// The Clinician copy of this card was removed with the Ads KPI row; the
// Detailed Dashboard's hero tile is the one that keeps the name.
ck('the genuine pacing tile elsewhere keeps its name',
   /<div class="tlbl">Ad Budget Pacing<\/div>/.test(html),true);
ck('and the Clinician duplicate is gone',/kpi\('Ad Budget Pacing'/.test(html),false);

// ── The page is called Brand Dashboard, in one voice ───────────────
// It used to answer to TWO names: the nav and titleMap said "Detailed
// Dashboard" while T.dashboard and the static topbar said "Executive
// Dashboard", so the title bar changed depending on how you arrived.
// "Executive Dashboard" is now a real, separate page, so it is no longer a
// stale alias — only "Detailed Dashboard" is. The Brand page must still not
// answer to the Executive page's name.
ck('no stale page name survives anywhere',/Detailed Dashboard/.test(html),false);
ck('the Brand page does not answer to the Executive name',
   /titleMap=\{[^}]*overview:'Executive Dashboard'/.test(html)
   ||/const T=\{[^}]*dashboard:'Executive Dashboard'/.test(html),false);
ck('the nav entry says Brand Dashboard',
   /data-sub="overview"[^>]*>(?:[^<]|<(?!\/div))*?<span>Brand Dashboard<\/span>/.test(html),true);
// The static title names the LANDING page, which is the Executive Dashboard
// now. The Brand page's title comes from titleMap/T when you navigate to it,
// and renderPage sets it on every route including boot.
ck('the static topbar title names the landing page',
   /id="ptitle"[^>]*>Executive Dashboard</.test(html),true);
ck('  and renderPage keeps the title in step on every route',
   /const _pt=document\.getElementById\('ptitle'\);if\(_pt&&T\[AP\]\)_pt\.textContent=T\[AP\];/.test(html),true);
ck('and so does the title the sub-nav writes',
   /titleMap=\{[^}]*overview:'Brand Dashboard'/.test(html),true);
ck('  including its fallback, which was the second name',
   /textContent=titleMap\[sub\]\|\|'Brand Dashboard'/.test(html),true);
ck('the AI panel label resolves to the same name',
   /const T=\{[^}]*dashboard:'Brand Dashboard'/.test(html),true);
ck('the loading skeleton too',/Loading Brand Dashboard/.test(html),true);
// Copy on OTHER pages that tells the reader to go load this one.
ck('cross-page instructions point at the new name',
   (html.match(/load the Brand Dashboard/gi)||[]).length>=2,true);
// Sibling pages keep their own names.
ck('the sibling dashboards are untouched',
   /Clinician Dashboard/.test(html)&&/Irfan Dashboard/.test(html),true);

// ── Key Metrics is now the first section ───────────────────────────
ck('Key Metrics container precedes every other section',
   ['id="sec-keymetrics"','id="dsec-demoinsights"','id="dsec-cvrfunnel"','id="sec-row2"']
     .map(at).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);

// ── Snapshot and Trend Analysis are gone from this page ────────────
ck('the Snapshot section container is gone',at('id="sec-row1"'),-1);
ck('  and the Daily Activity card inside it',at('id="sec-daily"'),-1);
ck('  and its chart target',at('id="daily-wrap"'),-1);
ck('the Trend Analysis container is gone',at('id="ov-truecpd-wrap"'),-1);
ck('  and nothing still renders into it',/ov-truecpd-week/.test(html),false);
// The All Time tab hid Daily Activity and lent its "Snapshot" header to the
// Revenue Outcome card. With no Snapshot section that header is orphaned.
ck('the borrowed All Time Snapshot header is gone',at('dsec-snapshot-alltime'),-1);
ck('no Snapshot heading remains on this page',
   /id="dsec-snapshot"/.test(html),false);

// Both builders are SHARED with the Irfan Dashboard, which still shows both
// sections — removing them here must not delete the functions.
ck('renderDemosPerDay survives for Irfan',
   /function renderDemosPerDay\(/.test(html)&&/renderDemosPerDay\(d,'irfan-daily-wrap'\)/.test(html),true);
ck('  and the Brand page no longer calls it',/renderDemosPerDay\(d\);/.test(html),false);
ck('the True CPD week builders survive for Irfan',
   /_trueCpdWeekCard\(d,'irfan-truecpd-week'\)/.test(html)
   &&/_renderTrueCpdWeek\(d,'irfan-truecpd-week'\)/.test(html),true);
ck('Irfan keeps its own Snapshot section',/id="dsec-irf-snapshot"/.test(html),true);
// The Clinician page's Trend Analysis is a different card entirely.
ck('the Clinician Trend Analysis is untouched',/id="cln-trend-h'\+sfx\+'"/.test(html),true);

// The loading skeleton must not promise a card the page will never render.
ck('the skeleton no longer previews a Daily Activity chart',
   /dash-skel-row-7-3/.test(html),false);
ck('  and leads with a single Key Metrics card instead',
   html.indexOf('Row 0: Key Metrics')>0,true);

// margin-top:0 belongs to whichever section is FIRST. It sat on #sec-row1,
// which stopped being first when Key Metrics moved up and no longer exists.
ck('the first-section margin rule targets the first section',
   /#page-dashboard>\.dsec:first-of-type,#irfan-c>\.dsec:first-child\{margin-top:0\}/.test(html),true);
ck('  and the pages using it get the section spacing',
   /#page-dashboard \.dsec,#irfan-c>\.dsec\{margin-top:36px\}/.test(html),true);
// The Executive page is NOT in that pair any more. Each of its sections sits in
// its own wrapper div, so every .dsec there matched :first-child and the 36px
// was cancelled on all of them - the rule looked like it was spacing the page
// and never did. Its containers own the rhythm instead.
ck('  with the Executive page spaced by its containers',
   /#page-exec>div\{margin-bottom:24px\}/.test(html)&&!/#page-exec \.dsec(?![-\w])/.test(html),true);
ck('six headline tiles (five via _kmTile, the split one inline)',
   (html.match(/kmh\+=_kmTile\(/g)||[]).length+(html.match(/kmh\+='<div class="tile hero"/g)||[]).length,6);
ck('in the order asked for',
   ['Ad Spend','CPD \\u00b7 Paid','Non-Paid \\u00b7 Last Click','CPD \\u00b7 Regular Demos',
    'CPQD \\u00b7 Regular Demos','Pre-Launch %','CVR \\u00b7 Regular']
     .map(t=>html.indexOf("('"+t+"'")).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);
// The paid pair sits directly right of Ad Spend, so nothing separates the spend
// from what it bought.
ck('the split tile is the second thing in the row, right after Ad Spend',
   html.indexOf('kmh+=\'<div class="tile hero"')<html.indexOf("_kmTile('CPD \\u00b7 Regular Demos'"),true);
ck('CAC gave up its tile to Pre-Launch %',
   /_kmTile\('CAC'/.test(html),false);
// Tile count and column count drift apart silently and orphan a 1/5-width row.
// Six tiles, seven units: the split tile spans two columns.
// Computed now: the Executive Dashboard omits CPQD, which drops a grid unit.
ck('the grid has one column per grid unit',
   /grid-template-columns:repeat\('\+\(hideCpqd\?6:7\)\+',minmax\(0,1fr\)\);gap:12px;margin-bottom:16px/.test(html),true);
ck('laid out five across like the Irfan page',
   /grid-template-columns:repeat\(5,minmax\(0,1fr\)\);gap:12px;margin-bottom:16px/.test(html),true);
// Ad Spend leads with spend and carries budget + pacing, like the Ad KPIs card.
ck('Ad Spend leads with the spend',/_kmTile\('Ad Spend',f\$\(_kmSpend\)/.test(html),true);
ck('its budget skips excluded and pending channels',
   /if\(_bc\.pendingConnector\|\|_bc\.kpiExcluded\)continue;/.test(html),true);
ck('pacing only on month windows',/_kmIsMonth=\(_kmWin==='mtd'\|\|_kmWin==='lastMonth'\)/.test(html),true);

// Revenue Outcome moved out of the Snapshot row into Key Metrics.
ck('Revenue Outcome lives in Key Metrics',
   /kmh\+='<div id="t-revenue'\+sfx\+'"><\/div>';/.test(html),true);
ck('the Snapshot row no longer holds it',
   /id="sec-row1"[^\n]*t-revenue/.test(html),false);
ck('and the Snapshot row is full width',/id="sec-row1" style="display:grid/.test(html),false);
// One EMISSION, suffixed per host. #t-revenue is repainted asynchronously by
// fdRevenueOutcome and getElementById takes the first match, so two unsuffixed
// copies would have the Executive page's card stealing the Brand page's repaint.
ck('only one #t-revenue is ever emitted',(html.match(/id="t-revenue'\+sfx\+'"/g)||[]).length,1);
ck('  and no unsuffixed literal survives',(html.match(/id="t-revenue"/g)||[]).length,0);
ck('  with fdRevenueOutcome repainting the Executive copy by its own id',
   /getElementById\('t-revenue-exec'\)/.test(html),true);

// CVR Regular comes from the funnel block, so the build must happen after it or
// the const is in its temporal dead zone and the whole render throws.
ck('Key Metrics builds after the Conversion Funnel',
   html.indexOf("t-funnel').innerHTML=fh")<html.indexOf("const kmEl=document.getElementById('t-keymetrics')"),true);
ck('and after every value it reads',
   ['const cacSB=','const _regC=','const _cvrReg=']
     .every(v=>html.indexOf(v)>0&&html.indexOf(v)<html.indexOf("const kmEl=")),true);

// Every tile shows the arithmetic behind its value, and CPD moved onto the
// regular-demo denominator so its label and its number agree.
ck('CPD divides by regular demos, matching its label',
   /_kmTile\('CPD \\u00b7 Regular Demos',_cRegC!=null\?f\$\(_cRegC\)/.test(html),true);
ck('and it is the same _regC the Efficiency card and CVR use',
   /f\$\(_spC\)\+' \\u00f7 '\+fN\(_regC\)\+' regular demos'/.test(html),true);
ck('every tile prints its calculation',
   (html.match(/\\u00f7 '\+fN\(|\\u00f7 '\+f\$\(/g)||[]).length>=4,true);
ck('Ad Spend shows spend over budget',
   /f\$\(_kmSpend\)\+' \\u00f7 '\+f\$\(_kmBudget\)\+' budget'/.test(html),true);
// Pacing sparkline: fill = budget spent, hairline = month elapsed.
ck('the pacing sparkline is drawn',/position:relative;height:8px;background:rgba\(255,255,255,\.18\)/.test(html),true);
ck('with an elapsed marker',/left:'\+Math\.min\(_kmElapsed,100\)\+'%;top:-2px;bottom:-2px;width:1\.5px;background:#fff/.test(html),true);
ck('and it is light-on-dark for the hero tile',/var\(--lighter\)/.test(html.slice(html.indexOf('let _kmPace='),html.indexOf('const _kmTile='))),false);
ck('no budget set is stated rather than drawn',/No monthly budget set/.test(html),true);

// --- the Clinician payload is warmed at sign-in ---------------------------
// The Detailed Dashboard is instant because its payload rides the login
// response. The Clinician page has its own endpoint, so it is started as soon
// as the password is known good — BEFORE that response is parsed and before the
// first render, both of which are slow enough that starting after them added
// their cost to the wait.
{
  const LOGIN=html.slice(html.indexOf('async function doLogin(){'),html.indexOf("document.getElementById('pw').addEventListener"));
  ck('the warm-up exists',/const _warmClinician=function\(tries\)\{/.test(LOGIN),true);
  ck('it is fired inside doLogin',/try\{_warmClinician\(1\);\}catch/.test(LOGIN),true);
  // Ordering is the whole point: before the parse and before the render.
  ck('it starts before the payload is parsed',
     LOGIN.indexOf('_warmClinician(1)')<LOGIN.indexOf('const d=await r.json();'),true);
  ck('and before the first render',
     LOGIN.indexOf('_warmClinician(1)')<LOGIN.indexOf('bldF();renderPage();'),true);
  // ...but only once the password is known good, so a bad password does not
  // fire a second request.
  ck('and only after the 401 check',
     LOGIN.indexOf('r.status===401')<LOGIN.indexOf('_warmClinician(1)'),true);
  // fdClinician swallows its own errors and only paints them when the Clinician
  // page is active, so a failed preload is silent. One retry, or the page stays
  // cold until a navigation refetches it from scratch.
  ck('a failed warm-up retries once',
     /if\(!S\.clinician\.data&&tries>0\)setTimeout\(function\(\)\{_warmClinician\(tries-1\);\},\d+\);/.test(LOGIN),true);
  ck('and it never double-fetches',/if\(S\.clinician\.data\|\|S\.clinician\._loading\)return;/.test(LOGIN),true);
  ck('the old post-render preload is gone',/Preload the Clinician payload in the background/.test(html),false);
}

// ── Ad tables default to the CHEAPEST CPD first ───────────────────────────
// Was highest-first ("worst performers at the top"). All four tables share the
// default, and all four must agree or the Campaigns and Ad Sets views disagree
// about what the same sort means.
{
  const defs=(html.match(/S\.irfan\.meta(?:Camp|Cre|Aud|Adset)Sort=\{col:'cpd',dir:'(\w+)'\}/g)||[])
    .map(x=>/dir:'(\w+)'/.exec(x)[1]);
  ck('every ad table declares a CPD default', defs.length>=4, true);
  ck('and all of them ascending', defs.filter(d=>d!=='asc'), []);
  ck('none left descending', /col:'cpd',dir:'desc'/.test(html), false);
}
// Ascending is only safe because a null CPD sinks either way — a row with no
// demos has no cost per demo, and letting it lead an ascending sort would fill
// the top of every table with blanks.
// Scoped to _irfanSortRows: that null-sink line appears in four sorters in this
// file, so an unscoped match passed even after the ad tables' own one was
// inverted.
{
  const i=html.indexOf('function _irfanSortRows(rows,col,dir){');
  const sorter=html.slice(i, html.indexOf('return rows.slice().sort(cmp);', i));
  ck('the ad-table sorter was located', sorter.length>300, true);
  ck('a null sorts to the bottom whichever way the sort runs',
     /if\(av==null\)return 1;if\(bv==null\)return -1;/.test(sorter), true);
  ck('and CPD is null when there are no demos',
     /else if\(col==='cpd'\)\{av=a\.demos>0\?a\.spend\/a\.demos:null;bv=b\.demos>0\?b\.spend\/b\.demos:null;\}/.test(sorter), true);
}

// ── Sections removed from the Clinician page ─────────────────────────────
ck('Daily Activity is gone', /card\('Daily Activity'/.test(html), false);
// On-Target Mix went with its whole machinery, not just its call site — a
// builder left behind with no caller is the thing that rots.
ck('On-Target Mix is gone',
   [/_clnTypeCard/.test(html),/_renderClnOnTarget/.test(html),/_clnTypeBands/.test(html),
    /CLN_ON_TARGET/.test(html),/CLN_OT_RATE_C/.test(html)], [false,false,false,false,false]);
// The worker still ships byType/bySubType — Trend Analysis's Doctors and
// Doctor % measures read them, so that is live, not residue.
ck('but the type cut it used is still live in Trend Analysis',
   /var types=\(sub\?p\.bySubType:p\.byType\)\|\|\{\};/.test(html), true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
