// Section order, the collapsed field appendices, the Ad Tables date seed and
// the removal of the Clinician ad channel.
const fs=require('fs');
let pass=0,fail=0;
function ck(n,g,w){const ok=JSON.stringify(g)===JSON.stringify(w);ok?pass++:fail++;
  console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));}
const html=fs.readFileSync('dashboard.html','utf8');
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

// ── Key Metrics is now the first section ───────────────────────────
ck('Key Metrics container precedes every other section',
   ['id="sec-keymetrics"','id="sec-row1"','id="ov-truecpd-wrap"','id="dsec-demoinsights"']
     .map(at).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);
ck('six headline tiles (five via _kmTile, the split one inline)',
   (html.match(/kmh\+=_kmTile\(/g)||[]).length+(html.match(/kmh\+='<div class="tile hero"/g)||[]).length,6);
ck('in the order asked for',
   ['Ad Spend','CPD \\u00b7 Regular Demos','CPQD \\u00b7 Regular Demos',
    'Paid \\u00b7 Last Click','CAC','CVR \\u00b7 Regular']
     .map(t=>html.indexOf("('"+t+"'")).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);
// Tile count and column count drift apart silently and orphan a 1/5-width row.
// Six tiles, seven units: the split tile spans two columns.
ck('the grid has one column per grid unit',
   (html.match(/grid-template-columns:repeat\((\d),minmax\(0,1fr\)\);gap:12px;margin-bottom:16px/)||[])[1],'7');
ck('laid out five across like the Irfan page',
   /grid-template-columns:repeat\(5,minmax\(0,1fr\)\);gap:12px;margin-bottom:16px/.test(html),true);
// Ad Spend leads with spend and carries budget + pacing, like the Ad KPIs card.
ck('Ad Spend leads with the spend',/_kmTile\('Ad Spend',f\$\(_kmSpend\)/.test(html),true);
ck('its budget skips excluded and pending channels',
   /if\(_bc\.pendingConnector\|\|_bc\.kpiExcluded\)continue;/.test(html),true);
ck('pacing only on month windows',/_kmIsMonth=\(_kmWin==='mtd'\|\|_kmWin==='lastMonth'\)/.test(html),true);

// Revenue Outcome moved out of the Snapshot row into Key Metrics.
ck('Revenue Outcome lives in Key Metrics',
   html.indexOf('id="sec-keymetrics"')<html.indexOf("kmh+='<div id=\"t-revenue\"></div>'"),true);
ck('the Snapshot row no longer holds it',
   /id="sec-row1"[^\n]*t-revenue/.test(html),false);
ck('and the Snapshot row is full width',/id="sec-row1" style="display:grid/.test(html),false);
ck('only one #t-revenue is ever emitted',(html.match(/id="t-revenue"/g)||[]).length,1);

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

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
