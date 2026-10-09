// BD Tracker · Churn tab · C1 Key Metrics. Scoped to 2026 onward, each figure
// split by quarter, with a Board Defined / All switch where Board Defined folds
// in Delinquent Payment.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const bd=load(process.env.BD_SRC||'bd-dashboard.html');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console};vm.createContext(ctx);
vm.runInContext([
  "var BD_CHURN_BOARD='Board Defined';",
  decl(bd,'var BD_CHURN_TYPE_GROUP={','};'),
  fn(bd,'_bdMonthOfG'), fn(bd,'_bdQuarterOf'), fn(bd,'_bdIsBoardChurn'),
  decl(bd,"var BD_CHURN_KM_FROM=","';"), "var BD_CHURN_KM_YEAR=2026;",
  'Object.assign(globalThis,{_bdMonthOfG,_bdQuarterOf,_bdIsBoardChurn,BD_CHURN_TYPE_GROUP,BD_CHURN_KM_FROM});',
].join('\n'),ctx);
ck('the helpers loaded',typeof ctx._bdQuarterOf,'function');

// ── 1. The 2026 floor ─────────────────────────────────────────────────────
// Before 2026 the book was a handful of brands, where one churn swings a rate
// by tens of points — the same reason C2 refuses a thin month.
ck('the floor is the start of 2026',ctx.BD_CHURN_KM_FROM,'2026-01-01');
ck('the scope filter compares months against it',
   /_m>=BD_CHURN_KM_FROM\.substring\(0,7\)/.test(bd),true);
ck('and it filters on the churn date, not the create date',
   /var _m=_bdMonthOf\(dd\.pausechurn_date\);\s*\n\s*return !!_m && _m>=BD_CHURN_KM_FROM/.test(bd),true);
ck('undated churns are counted out loud, not dropped silently',
   /_kmUndated\+' undated, excluded'/.test(bd),true);

// ── 1b. The page says its own scope, once, at the top ─────────────────────
// Every section reads 2026 onward, so the statement is page-level rather than
// repeated per chart — and it is the first thing emitted into hChurn.
ck('the scope band is the first thing on the page',
   bd.indexOf("2026 only")>0 &&
   bd.indexOf("2026 only")<bd.indexOf('id="bd-sec-churn"'),true);
ck('  and names the window in words',
   /Every figure on this page counts churns dated 1 January 2026 onward/.test(bd),true);
ck('  with the kept count over the whole set',
   /\+_chDeals26\.length\+'<\/strong> of '\+_chDeals\.length\+' churned deals\. '/.test(bd),true);
// What is excluded has to be named, or the band is a claim with no accounting.
ck('  the pre-2026 churns are counted',
   /var _chPre26=_chDeals\.length-_chDeals26\.length-_chNoDate;/.test(bd),true);
ck('  the undated ones too',
   /var _chNoDate=_chDeals\.filter\(function\(dd\)\{return !_bdMonthOf\(dd\.pausechurn_date\);\}\)\.length;/.test(bd),true);
ck('  and both are reported in the band',
   /_chPre26\+' churned before 2026'/.test(bd)&&/no Pause\/Churn Date/.test(bd),true);
ck('  with nothing claimed when nothing is excluded',
   /\(\(_chPre26\|\|_chNoDate\)\?', excluded throughout\.':''\)/.test(bd),true);
// The sections must actually honour it, or the band is a lie.
ck('the A/B and segment sets read the 2026 book',
   /var _chBoard=_chDeals26\.filter\(function\(dd\)\{/.test(bd),true);
ck('  and so does the churned-deals table',
   /var _chTableRows=_chDeals26\.filter\(function\(dd\)\{/.test(bd),true);
ck('  leaving no section reading the all-time set',
   (bd.match(/_chDeals\.forEach/g)||[]).length,0);

// ── 2. Quarters ───────────────────────────────────────────────────────────
ck('months map to their quarter',
   ['2026-01-15','2026-03-31','2026-04-01','2026-06-30','2026-07-02','2026-09-30','2026-10-09','2026-12-31']
     .map(d=>ctx._bdQuarterOf(d)),['Q1','Q1','Q2','Q2','Q3','Q3','Q4','Q4']);
ck('an earlier year has no quarter here',
   ['2025-12-31','2024-06-01'].map(d=>ctx._bdQuarterOf(d)),['','']);
ck('a later year does not fold into 2026 either',ctx._bdQuarterOf('2027-02-01'),'');
ck('a blank or unparseable date yields no quarter',
   ['',null,undefined,'not a date'].map(d=>ctx._bdQuarterOf(d)),['','','','']);
ck('epoch-millisecond timestamps parse too',
   ctx._bdQuarterOf(String(Date.UTC(2026,4,15))),'Q2');
// An empty Q4 shown in March reads as a real zero, which is worse than absent.
ck('only quarters that have begun are shown',
   /_kmQMax=\(_kmNow\.getFullYear\(\)>BD_CHURN_KM_YEAR\)\?4:\(Math\.floor\(_kmNow\.getMonth\(\)\/3\)\+1\)/.test(bd),true);
ck('  with at least one always shown',/_kmQs\.slice\(0,Math\.max\(1,_kmQMax\)\)/.test(bd),true);

// ── 3. Board Defined folds in Delinquent Payment ──────────────────────────
const T=t=>ctx._bdIsBoardChurn({never_implemented_churn:t});
ck('Board Defined Churn counts',T('Board Defined Churn'),true);
ck('Delinquent Payment counts too — that is the whole point',T('Delinquent Payment'),true);
ck('the Never Implemented reasons do not',
   ['Yes - Ghosted','Yes - Rejected Product(s)','Yes - Other Reason'].map(T),[false,false,false]);
ck('nor does a blank or unknown type',['','  ',null,'Something else'].map(t=>ctx._bdIsBoardChurn({never_implemented_churn:t})),[false,false,false,false]);
ck('whitespace is trimmed before matching',T('  Delinquent Payment  '),true);
// The grouping map is shared with the By Type chart, so the two cuts agree.
ck('it reuses the By Type grouping map rather than its own list',
   /BD_CHURN_TYPE_GROUP\[\(dd\.never_implemented_churn\|\|''\)\.trim\(\)\]===BD_CHURN_BOARD/.test(bd),true);
ck('  and both board types map to one label',
   [ctx.BD_CHURN_TYPE_GROUP['Board Defined Churn'],ctx.BD_CHURN_TYPE_GROUP['Delinquent Payment']],
   ['Board Defined','Board Defined']);

// ── 4. The toggle ─────────────────────────────────────────────────────────
ck('it has two pills',
   [...bd.matchAll(/_kmPill\('(board|all)','([^']+)'/g)].map(m=>m[2]),['Board Defined','All']);
ck('Board Defined is the default \u2014 the cut the board reviews',
   /var _bdChurnKmScope='board';/.test(bd),true);
ck('  and All is still reachable',/_kmPill\('all','All'/.test(bd),true);
ck('switching re-renders',/function bdSetChurnKmScope\(v\)\{if\(_bdChurnKmScope===v\)return;_bdChurnKmScope=v;renderBD\(\);\}/.test(bd),true);
ck('the scope line says which cut is on screen',
   /_kmDeals\.length\+' of '\+_kmAll\.length\+' 2026 churns'/.test(bd)
   &&/Board Defined Churn \+ Delinquent Payment/.test(bd),true);
ck('Board Defined narrows the SAME 2026 set, not the all-time one',
   /_kmDeals=_kmBoard\?_kmAll\.filter\(_bdIsBoardChurn\):_kmAll/.test(bd),true);

// ── 5. Every tile carries its quarter split ───────────────────────────────
// Churned Deals was dropped: the raw count is on the page in three other
// places, and the two tiles that remain are the ones that say something.
ck('two tiles',(bd.match(/hChurn\+=_chTile\(/g)||[]).length,2);
ck('each one is handed a quarter strip',(bd.match(/_kmQStrip\(_c(Pct|Ten)\)/g)||[]).length,2);
ck('the strip is built once',(bd.match(/var _kmQStrip=/g)||[]).length,1);
ck('and the tiles are the rate and the tenure',
   [...bd.matchAll(/hChurn\+=_chTile\('([^']+)'/g)].map(m=>m[1]),
   ['Avg Monthly Churn Rate','Avg Time to Churn']);
ck('  the grid sized itself down with them',
   /grid-template-columns:repeat\(2,minmax\(0,1fr\)\);gap:14px;align-items:stretch/.test(bd),true);
ck('  and the dropped count variable went too',/_cCount/.test(bd),false);

// ── 6. The rate is the MEAN OF THE MONTHLY RATES ──────────────────────────
// Each month's churns over that month's own opening book, averaged across the
// months in the period. Not a pooled churns ÷ book: every month weighs the
// same, so the figure reads as "the rate in a typical month".
ck('C1 takes the book at the period start',
   /_kmOpenAt=function\(m\)\{var pv=_snByMonth\[_kmPrevMonth\(m\)\];return pv\?pv\.b:0;\}/.test(bd),true);
ck('  which is what C2 does',
   /_chOpen=function\(m\)\{var p=_snByMonth\[_chPrev\(m\)\];return p\?p\.b:0;\}/.test(bd),true);
ck('churns are tallied per month',
   /_kmByMonth\[m\]=\(_kmByMonth\[m\]\|\|0\)\+1/.test(bd),true);
ck('  from the scoped set, so the toggle moves the numerator',
   /_kmDeals\.forEach\(function\(dd\)\{\s*\n\s*var m=_bdMonthOf\(dd\.pausechurn_date\); if\(m\)_kmByMonth/.test(bd),true);
ck('the headline is a mean of monthly rates',
   /var _kmTot=_kmMeanRate\(_kmMonthsOf\(null\)\);/.test(bd),true);
ck('  and each quarter is the mean over its own months',
   /var _r=_kmMeanRate\(_kmMonthsOf\(q\)\);/.test(bd),true);
ck('  with no pooled denominator left anywhere',
   /_kmTotalBase|_kmOpenQ|_kmAvgOpen/.test(bd),false);
ck('the tile says how many months it averaged',
   /'mean of '\+_kmTot\.months\+' monthly rate'/.test(bd),true);
ck('  and names the method, not just the number',
   /then averaged across the months in the period/.test(bd),true);

// ── 6b. _kmMeanRate, run for real ─────────────────────────────────────────
const rctx={console};vm.createContext(rctx);
vm.runInContext([
  'var _kmByMonth={}, _openBook={};',
  'var _kmOpenAt=function(m){return _openBook[m]||0;};',
  fn(bd,'_kmMeanRate'),
  'Object.assign(globalThis,{_kmMeanRate});',
].join('\n'),rctx);
const mean=(open,churn,months)=>{rctx._openBook=open;rctx._kmByMonth=churn;
  const r=rctx._kmMeanRate(months);return [r.pct==null?null:+r.pct.toFixed(4), r.months];};
// 10/100 = 10% and 10/200 = 5% average to 7.5%. POOLED would be 20/300 = 6.67%,
// so this case is what distinguishes the two definitions.
ck('months are averaged, not pooled',
   mean({'2026-01':100,'2026-02':200},{'2026-01':10,'2026-02':10},['2026-01','2026-02']),
   [7.5,2]);
ck('  a month with a book and no churns counts as a real zero',
   mean({'2026-01':100,'2026-02':100},{'2026-01':10},['2026-01','2026-02']),[5,2]);
// A month the Snapshot cannot describe has NO rate, not a zero one — averaging
// in a zero there would drag every figure down.
ck('  a month with no opening book is left out of both sum and divisor',
   mean({'2026-01':100},{'2026-01':10},['2026-01','2026-02','2026-03']),[10,1]);
ck('  and a period with no book at all yields no rate',
   mean({},{'2026-01':10},['2026-01','2026-02']),[null,0]);
ck('  an empty month list yields no rate either',mean({'2026-01':100},{},[]),[null,0]);
ck('  a zero-churn month is 0%, not a dash',
   mean({'2026-01':100},{},['2026-01']),[0,1]);
ck('a quarter spans its own three months',
   (()=>{const c={console};vm.createContext(c);
     vm.runInContext(['var BD_CHURN_KM_YEAR=2026;',fn(bd,'_kmMonthsOf'),
       'Object.assign(globalThis,{_kmMonthsOf});'].join('\n'),c);
     return c._kmMonthsOf('Q1');})(),['2026-01','2026-02','2026-03']);

// ── 7. One date parser on the page ────────────────────────────────────────
ck('renderBD delegates to the module-scope parser',
   /function _bdMonthOf\(v\)\{return _bdMonthOfG\(v\);\}/.test(bd),true);
ck('  so there is exactly one implementation',
   (bd.match(/if\(!isNaN\(n\)&&n>1e11\)\{var dt=new Date\(n\)/g)||[]).length,1);

// ── 8. The section links were already in the sticky nav ───────────────────
// Asserted so a future edit cannot quietly drop them: every churn section must
// have a nav entry pointing at an id that exists.
const navIds=[...bd.matchAll(/bdNavGo\(event,'(bd-sec-churn[a-z]*)'\)/g)].map(m=>m[1]);
const secIds=[...bd.matchAll(/<section class="bd-sec" id="(bd-sec-churn[a-z]*)">/g)].map(m=>m[1]);
ck('every churn section has a nav link',secIds.filter(id=>navIds.indexOf(id)<0),[]);
ck('and every churn nav link has a section',navIds.filter(id=>secIds.indexOf(id)<0),[]);
ck('there are nine of them',[navIds.length,secIds.length],[9,9]);
ck('  including the new products section',navIds.indexOf('bd-sec-churnprod')>=0,true);
ck('the churn group is revealed on the Churn tab',
   /var cn=document\.getElementById\('bd-nav-churn'\);if\(cn\)cn\.style\.display=\(t==='churn'\)\?'contents':'none';/.test(bd),true);
ck('  and the Tracker anchors hidden',
   /var an=document\.getElementById\('bd-nav-anchors'\);if\(an\)an\.style\.display=\(t==='tracker'\)\?'contents':'none';/.test(bd),true);
// The scroll-spy reads .bd-sec generically, so it marks churn sections too.
ck('the scroll-spy is generic over sections',/var secs=document\.querySelectorAll\('\.bd-sec'\)/.test(bd),true);

// ── 9. The goal calculator is a Tracker thing ────────────────────────────
// It counts deals needed to hit a monthly SIGNING goal, which says nothing
// about churn. Hidden, not removed — the Tracker still needs it and the dialog
// it opens lives in the static DOM.
ck('the goal button is hidden on the Churn tab',
   /var gb=document\.getElementById\('bd-goal-btn'\);if\(gb\)gb\.style\.display=\(t==='churn'\)\?'none':'';/.test(bd),true);
ck('  and its dialog is left in place for the Tracker',
   /id="bd-goal-btn"/.test(bd)&&/function bdGoalOpen\(/.test(bd),true);

// ── 10. C2 — two lines, a real % axis, every period labelled ─────────────
// Start at _chNITypes, not at _chRateChart: the NI predicate is declared just
// above the builder and is part of this section's logic.
const C2=(()=>{const i=bd.indexOf('var _chNITypes=');return bd.slice(i,bd.indexOf('</section>',i));})();
ck('C2 is locatable',C2.length>800,true);
ck('it draws two series, Board Defined first',
   [...C2.matchAll(/\{n:'([^']+)'/g)].map(m=>m[1]),['Board Defined','Never Implemented']);
// Only the SERIES widths — the chart's own opts carry a w: too.
ck('the primary line is thicker than the secondary',
   (()=>{const w=[...C2.matchAll(/\{n:'[^']+',c:[^,]+,w:([\d.]+)/g)].map(m=>+m[1]);
         return w.length===2&&w[0]>w[1];})(),true);
ck('  and the secondary is dashed, so they read as primary and supporting',
   /\{n:'Never Implemented'[^}]*dash:true/.test(C2),true);
ck('Never Implemented combines all three NI reasons',
   /_chNITypes=BD_CHURN_TYPE_ORDER\.filter\(function\(c\)\{return c!==BD_CHURN_BOARD;\}\)/.test(bd),true);
ck('  and Board Defined still folds in Delinquent',/_bdIsBoardChurn\(dd\)/.test(C2),true);
ck('  with the two cuts disjoint',/if\(_bdIsBoardChurn\(dd\)\)[\s\S]{0,60}?else if\(_chIsNI\(dd\)\)/.test(C2),true);
// _chTypeOf is a `var` function expression declared BELOW C2, so calling it
// from here would read undefined at render time.
// The comment above _chIsNI names it, so strip comments before testing.
ck('the NI test does not call the later-declared _chTypeOf',
   /_chTypeOf/.test(C2.replace(/^\s*\/\/.*$/gm,'')),false);

ck('C2 starts at 2026',/if\(!k\|\|k<BD_CHURN_DETAIL_FROM\)return;/.test(C2),true);
ck('  which is January 2026',/var BD_CHURN_DETAIL_FROM='2026-01';/.test(bd),true);
ck('C2 has its own Monthly⁄Quarterly toggle',
   /_chGrainSeg\(_bdChurnRateGrain,'bdSetChurnRateGrain'\)/.test(bd),true);
ck('  defaulting to monthly',/var _bdChurnRateGrain='month';/.test(bd),true);
ck('  and re-rendering on change',
   /function bdSetChurnRateGrain\(g\)\{if\(_bdChurnRateGrain===g\)return;_bdChurnRateGrain=g;renderBD\(\);\}/.test(bd),true);

// The old chart thinned X labels by floor(n/14), which is why months vanished.
const RC=(()=>{const i=bd.indexOf('function bdRateLineChart(');return bd.slice(i,bd.indexOf('\nfunction ',i+10));})();
ck('the rate chart labels EVERY period',/labels\.forEach\(function\(l,i\)\{\s*\n\s*svg\+='<text x="'\+xAt\(i\)/.test(RC),true);
ck('  with no thinning step left',/Math\.floor\(labels\.length\/14\)/.test(RC),false);
ck('the Y axis steps on a nice value',
   /var steps=\[0\.25,0\.5,1,2,2\.5,5,10,20,25,50\];/.test(RC),true);
ck('  and every gridline is a multiple of it',/var gv=g\*st,gy=yAt\(gv\);/.test(RC),true);
ck('  labelled as a percentage',/\+'%<\/text>'/.test(RC),true);
ck('a null rate breaks the line instead of drawing a zero',
   /if\(v==null\)\{flush\(\);return;\}/.test(RC),true);
ck('  and a period with no opening book IS null',
   /open>0\?Math\.round\(\(b\/open\)\*10000\)\/100:null/.test(C2),true);
// An SVG rect with no fill paints black — the bug that made the Brand trend a
// solid block. Every band class on this page needs the rule.
ck('the hover bands are declared transparent',
   /\.bd-rate-band\{fill:transparent;cursor:pointer\}/.test(bd),true);

// ── 11. C5 and C6 cover 2026, with no grain toggle ──────────────────────
// Their axes are categorical, so a grain could only scope WHICH churns are
// counted — and scoping a cross-tab to one period left 3 of 2026's 148 churns
// in the grid, which is not a cross-tab.
ck('neither carries a grain toggle',
   /bdSetChurnAbGrain|bdSetChurnTrGrain|_bdChurnAbGrain|_bdChurnTrGrain/.test(bd),false);
// Comments still name them, which is the point of the comment — test live code.
ck('  and the period-scoping helpers went with them',
   /_chLatest|_chTodayKey|_chInPeriod/.test(bd.replace(/^\s*\/\/.*$/gm,'')),false);
// Both now inherit the page-level 2026 set instead of re-filtering.
ck('both cover 2026 onward',
   /_chAbSet=_chBoard;/.test(bd)&&/_chTrSet=_chDeals26;/.test(bd),true);
ck('  because the whole tab is scoped once at the top',
   /var _chDeals26=_chDeals\.filter\(_chIn2026\);/.test(bd),true);
ck('  and no section re-derives the floor for itself',
   (bd.match(/_chIn2026\(dd\)/g)||[]).length,0);
ck('  through one shared floor test',
   /var _chIn2026=function\(dd\)\{var m=_bdMonthOf\(dd\.pausechurn_date\);return !!m&&m>=BD_CHURN_DETAIL_FROM;\}/.test(bd),true);
ck('  and each says so on the section',
   (bd.match(/Churns dated 2026 onward\.<\/span><\/div>'/g)||[]).length,2);
ck('C3 and C4 KEEP their toggles \u2014 they plot periods',
   /_chGrainSeg\(_bdChurnTypeGrain,'bdSetChurnTypeGrain'\)/.test(bd)
   &&/_chGrainSeg\(_bdChurnReasonGrain,'bdSetChurnReasonGrain'\)/.test(bd),true);
ck('  as does C2',/_chGrainSeg\(_bdChurnRateGrain,'bdSetChurnRateGrain'\)/.test(bd),true);

// ── 12. C2's bands were emitted with data and no handler ────────────────
ck('the rate bands are bound',/function bdBindRateBands\(root\)\{/.test(bd),true);
ck('  on every render, beside the stacked-chart binder',
   /bdBindStack\(\);[\s\S]{0,140}?bdBindRateBands\(\);/.test(bd),true);
ck('  reading the per-series rows the chart emits',
   /band\.getAttribute\('data-r'\+i\)/.test(bd)&&/band\.getAttribute\('data-p'\)/.test(bd),true);
// bdBindStack carries ONE data-sub for the whole tooltip; C2 needs the
// arithmetic per line, since the two series differ at the same x.
ck('  with a note per series, which bdBindStack cannot carry',
   /nt\.textContent=f\[2\];/.test(bd),true);

// ── 13. C3/C4 axis, and C4's duplicate "Other" ──────────────────────────
ck('the cluster axis snaps to a familiar step',
   /var _cSteps=\[1,2,5,10,20,25,50,100,200,250,500,1000\];/.test(bd),true);
ck('  and no longer slices the range into four',
   /var step=Math\.max\(1,Math\.ceil\(maxV\/4\)\),top=step\*4;/.test(bd),false);
// "Other" is a REAL stored churn_reason, so a rollup called "Other" put two
// identical labels in one legend.
ck('the C4 rollup cannot collide with a stored reason',
   /l:'All remaining reasons \('\+rest\.length\+'\)'/.test(bd),true);
ck('  and C6’s rollup likewise',/\{k:'__other',l:'All remaining'/.test(bd),true);
ck('  with no bare "Other" rollup left',/l:'Other \('\+rest\.length/.test(bd),false);
// Assert the LABEL IS THE RAW VALUE, not merely that one old slice expression
// is absent — any new truncation would have slipped past that.
ck('C4 category labels are the raw reason, untouched',
   /var cats=top\.map\(function\(r,i\)\{return \{k:r,l:r,c:/.test(bd),true);
ck('C6 column labels likewise',
   /_chTrCols=_chTopReasons\.map\(function\(r\)\{return \{k:r,l:r,full:r\};\}\)/.test(bd),true);
ck('  and no slice survives on either',
   /\.slice\(0,(25|17)\)/.test(bd),false);
ck('  and the legend carries the full text as hover',
   /return '<span title="'\+String\(c\.l\)\.replace/.test(bd),true);

// ── 14. C6 was dropping rows that matched no bucket ─────────────────────
// _chBandOf returns '' for measured <= 0 and _chReasonBucket '' for a blank
// reason; neither had a row or column, so those deals vanished from the grid.
ck('a no-measurement row exists',/\{k:'',l:'No measurement'\}/.test(bd),true);
ck('a no-reason column exists',/\{k:'',l:'No reason recorded'/.test(bd),true);
ck('  so the grid can total to the set it was given',
   /_chTrRows=BD_TRAFFIC_BANDS\.map[\s\S]{0,120}?\.concat\(\[\{k:'',l:'No measurement'\}\]\)/.test(bd),true);
// .stbl th sets nowrap + overflow:hidden, which clipped these with no ellipsis
// and made the inline word-break inert.
ck('matrix headers may wrap',/white-space:normal;overflow:visible/.test(bd),true);
ck('  and row labels carry hover text',/t\+='<tr><td title="'\+String\(r\.full\|\|r\.l\)/.test(bd),true);
ck('C5 headers get a tooltip too',/full:b\.k\+' measured monthly visits'/.test(bd),true);

// ── 15. C7 ───────────────────────────────────────────────────────────────
ck('empty synthetic rows are suppressed',
   /return r\.k\.indexOf\('__'\)!==0 \|\| \(_chSegCount\[r\.k\]\|\|0\)>0;/.test(bd),true);
ck('  but a real segment with zero churn still shows',
   /r\.k\.indexOf\('__'\)!==0 \|\|/.test(bd),true);
ck('the two synthetic buckets are different greys',
   /_chSegCol=\{__nosegment:'#94A3B8',__nocompany:'#CBD5E1'\}/.test(bd),true);
// Against the REAL segments only — _other is the designated fallback grey, so
// a synthetic bucket sharing it is correct, not a collision.
ck('  and neither collides with a real segment colour',
   (()=>{const m=/_chSegCol=\{__nosegment:'(#\w+)',__nocompany:'(#\w+)'\}/.exec(bd);
         const seg=/var BD_SEGMENT_COLORS=\{([^}]*)\}/.exec(bd)[1]
           .split(',').filter(p=>!/^\s*_other:/.test(p)).join(',');
         return [m[1],m[2]].filter(c=>seg.indexOf(c)>=0);})(),[]);
ck('  and the two greys differ from each other',
   (()=>{const m=/_chSegCol=\{__nosegment:'(#\w+)',__nocompany:'(#\w+)'\}/.exec(bd);
         return m[1]!==m[2];})(),true);
// .attr-row is a Tracker rule: it applies negative side margins on hover, which
// slid these grid rows sideways, and its tooltip JS never reaches them.
ck('C7 no longer borrows the Tracker row class',
   /_chSegH\+='<div class="attr-row"/.test(bd),false);
ck('  and has a hover rule of its own that does not move the row',
   /\.bd-seg-row:hover\{background:rgba\(114,164,191,\.05\)\}/.test(bd),true);

// ── 16. C8 ───────────────────────────────────────────────────────────────
// _bdDealsTable is SHARED with the Tracker's All Deals table, so the reorder is
// scoped to the tab rather than applied to the array for everyone.
ck('the churn-first order is scoped to the Churn tab',
   /var _churnFirst=\(_bdTab==='churn'\);/.test(bd),true);
ck('  moving the churn group ahead of dates',
   /cols=_rest\.slice\(0,_di\)\.concat\(_cg,_rest\.slice\(_di\)\);/.test(bd),true);
// Column order lives in THREE places; all three must switch on the same flag.
ck('the banner row follows the same flag',
   /if\(_churnFirst\)t\+=_churnBanner;/.test(bd)&&/if\(!_churnFirst\)t\+=_churnBanner;/.test(bd),true);
ck('  as do the cells',
   /if\(_churnFirst\)t\+=_churnTd;/.test(bd)&&/if\(!_churnFirst\)t\+=_churnTd;/.test(bd),true);
ck('  and the banner is built once, not duplicated per branch',
   (bd.match(/var _churnBanner=/g)||[]).length,1);
ck('Plan Details collapses to Amount and Plan Name only',
   (()=>{const m=/var cols=\[([\s\S]*?)\n    \];/.exec(bd);
         const plan=[...m[1].matchAll(/\{k:'([^']+)',l:'([^']+)'[^}]*group:'plan'([^}]*)\}/g)];
         return plan.filter(c=>!/groupCollapse/.test(c[3])).map(c=>c[2]);})(),
   ['Amount','Plan Name']);
// Pre-existing: the Avg Days headers and cells ran in different orders, so
// expanding that group shifted every value one column left.
ck('Avg Days cells match their header order',
   /_td\('days_to_close'[\s\S]{0,200}?_td\('to_demo'[\s\S]{0,200}?_td\('from_demo'/.test(bd),true);

// ── 17. C5 — what the brands that left had implemented ──────────────────
// implemented_products is written during the CHURN REVIEW. Every deal that
// carries it is churned and no active deal has it, so there is no denominator
// anywhere in this data that could make a rate. The section is built so that
// misreading is hard: one countable cell per brand, and no percentage printed
// in any value column.
const C5=(()=>{const i=bd.search(/^  \/\/ .{1,4} v\. What the brands that left had implemented/m);
  const E="  hChurn+='</div></section>';\n";
  return bd.slice(i,bd.indexOf(E,i)+E.length);})();
ck('the section is locatable',C5.length>4000,true);

// ── placement ──
ck('it is numbered C5',/<span class="bd-sec-n">C5<\/span>/.test(C5),true);
ck('  and sits above the A/B test section',
   bd.indexOf('id="bd-sec-churnprod"')<bd.indexOf('id="bd-sec-churnab"'),true);
ck('the sections below it moved down a number',
   [...bd.matchAll(/bd-sec-n">(C\d)<\/span>'?\s*\+?'?<span class="bd-sec-t">([^<]+)/g)]
     .map(m=>m[1]+':'+m[2].trim()),
   ['C1:Key Metrics','C2:Churn Rate Over Time','C3:Churn by Type',
    'C4:Churn by High Level Reason','C5:What the Brands That Left Had Implemented',
    'C6:A/B Test · Board Defined Churn','C7:Web Traffic at Churn',
    'C8:Board Defined Churn by Company Segment','C9:All Churned Deals']);
ck('the nav entry moved with it',
   (()=>{const n=[...bd.matchAll(/data-sec="(bd-sec-churn[a-z]*)"/g)].map(m=>m[1]);
     return n.indexOf('bd-sec-churnprod')===n.indexOf('bd-sec-churnab')-1;})(),true);

// ── the hoisting hazard the move creates ──
// BD_PROD_* and _chProdsOf are `var` expressions. Left where the old section
// kept them, a section placed earlier would see them hoisted-but-undefined and
// the whole tab would render blank.
ck('the shared product constants are declared above the section that uses them',
   bd.indexOf('var BD_PROD_GROUP=')<bd.search(/^  \/\/ .{1,4} v\. What the brands/m),true);
ck('  and so is the splitter',
   bd.indexOf('var _chProdsOf=')<bd.search(/^  \/\/ .{1,4} v\. What the brands/m),true);
ck('  they are declared exactly once',
   [(bd.match(/var BD_PROD_GROUP=/g)||[]).length,(bd.match(/var _chProdsOf=/g)||[]).length],[1,1]);
ck('  and the tenure helper is above it too',
   bd.indexOf('var _kmTenure=')<bd.search(/^  \/\/ .{1,4} v\. What the brands/m),true);

// ── state, in the house idiom ──
ck('the month filter has module-scope state',/^var _bdChurnProdMonth='';$/m.test(bd),true);
ck('  and a setter shaped like the grain setter',
   /function bdSetChurnProdMonth\(m\)\{if\(_bdChurnProdMonth===m\)return;_bdChurnProdMonth=m;renderBD\(\);\}/.test(bd),true);
ck('  carrying a month number, not a dated key',/_c5MonOf=function\(dd\)\{var m=_bdMonthOf\(dd\.pausechurn_date\);return m\?m\.substring\(5,7\):'';\}/.test(C5),true);

// ── the honesty rules ──
// indexOf returns -1 when the text is gone, and -1 is less than any position,
// so the ordering check has to assert presence first.
ck('the count-never-a-rate banner sits under the head, not in a footnote',
   C5.indexOf('A COUNT, NEVER A RATE')>=0 &&
   C5.indexOf('A COUNT, NEVER A RATE')<C5.indexOf('Churn month'),true);
ck('  and says why there is no denominator',
   /there is no active-brand denominator to divide by/.test(C5),true);
// The ONLY mention of _chFmtPct in this section is a comment saying it is
// deliberately unused, so the check has to read live code.
ck('the rate formatter is not borrowed for a count',
   /_chFmtPct/.test(C5.replace(/^\s*\/\/.*$/gm,'')),false);
ck('the section reads the page 2026 set',/_c5Pool=_chDeals26\.filter/.test(C5),true);
ck('  and never re-derives the floor from the year constant',
   /substring\(0,4\)===String\(BD_CHURN_KM_YEAR\)/.test(C5),false);

// ── functional: render every filter state ──
const C5BLOCK=C5;
const renderC5=(sel,pool,rest)=>{
  const c={console,_chDeals26:pool.concat(rest),_bdChurnProdMonth:sel,
    BD_CHURN_KM_YEAR:2026,hChurn:'',_bdMonthOf:v=>(v||'').substring(0,7)||''};
  vm.createContext(c);
  vm.runInContext([
    decl(bd,'var BD_PROD_GROUP={','};'), decl(bd,'var BD_PROD_ORDER=[','];'),
    decl(bd,'var BD_PROD_FULL={','};'), decl(bd,'var BD_PROD_C={','};'),
    fn(bd,'_chProdsOf'), fn(bd,'_kmTenure'), C5BLOCK,
  ].join('\n'),c);
  return c.hChurn;
};
// 40 records across Jul/Aug/Sep/Oct; Badge/Banner 39, Reviews 30, CAI 14, CAS 0.
const POOL=[];
[['07',15],['08',9],['09',15],['10',1]].forEach(([m,k])=>{for(let x=0;x<k;x++)POOL.push({m});});
POOL.forEach((d,i)=>{const p=[];
  if(i>=1)p.push(i%2?'Badge':'Banner'); if(i<30)p.push('Reviews'); if(i<14)p.push('CAI');
  if(!p.length)p.push('Reviews');
  d.implemented_products=p.join(';');
  d.pausechurn_date='2026-'+d.m+'-'+('0'+((i%27)+1)).slice(-2);
  d.entered_closedwon='2025-'+('0'+((i%12)+1)).slice(-2)+'-15'; d.dealstage='3453925110';});
const REST=[];
[['01',14],['02',11],['03',13],['04',12],['05',16],['06',15],['07',12],['08',10],['09',14],['10',3]]
  .forEach(([m,k])=>{for(let x=0;x<k;x++)REST.push({implemented_products:'',
    pausechurn_date:'2026-'+m+'-10',entered_closedwon:'2025-06-01',dealstage:'3453925110'});});
const ALL=renderC5('',POOL,REST), JUL=renderC5('07',POOL,REST), JAN=renderC5('01',POOL,REST), OCT=renderC5('10',POOL,REST);

ck('the All view renders every record',/Brands in this cut<\/div><div style="[^"]*">40</.test(ALL),true);
ck('  with the product counts out of the cut size',
   [...ALL.matchAll(/>(\d+)<\/span><span style="font-size:10\.5px;color:var\(--grey\);margin-left:5px">of (\d+)</g)]
     .map(m=>m[1]+'/'+m[2]),['39/40','30/40','14/40']);
// THE rule this section exists for: a percentage in a churn section is the one
// number it must never print.
// Inline CSS is full of width:100%, so this reads the rendered TEXT.
// Source order is not render order: a banner that is built and never appended
// still satisfies a source-position check, so this one reads the output.
ck('the banner is actually rendered, above the rail',
   ALL.indexOf('A COUNT, NEVER A RATE')>=0 &&
   ALL.indexOf('A COUNT, NEVER A RATE')<ALL.indexOf('Churn month'),true);
const ALLTEXT=ALL.replace(/<[^>]*>/g,' '), JULTEXT=JUL.replace(/<[^>]*>/g,' ');
ck('no percentage is printed anywhere in the section',/[\d.]+%/.test(ALLTEXT),false);
ck('  nor in a filtered month',/[\d.]+%/.test(JULTEXT),false);
// A real product none of them had is a FINDING and keeps its row. Grey NONE,
// never amber - amber on this page means "too few to trust", and a definite
// zero is not that.
ck('a product none of them had keeps its row',/>CAS</.test(ALL),true);
ck('  marked NONE, in grey rather than amber',
   /NONE<\/span>/.test(ALL)&&!/NONE<\/span>/.test(ALL.replace(/color:var\(--grey\);background:rgba\(23,44,69,\.06\)[^>]*>NONE<\/span>/,'')),true);
// The product rows overlap; the combination rows partition the same brands.
ck('the combinations sum to the cut size',
   (()=>{const n=[...ALL.matchAll(/font-size:14px;font-weight:700;color:var\(--navy\)">(\d+)</g)].map(m=>+m[1]);
     return n.reduce((a,b)=>a+b,0);})(),40);
ck('  and the section states that sum',/These do not overlap, so they sum to 40\./.test(ALL),true);
ck('a month filter narrows the cut',/Brands in this cut<\/div><div style="[^"]*">15</.test(JUL),true);
ck('  and the combinations still sum to it',/so they sum to 15\./.test(JUL),true);

// ── the rail ──
// Two numbers per month, recorded / churned, so an empty month says WHICH kind
// of empty it is before anyone clicks it.
ck('each month carries recorded and churned',
   [...ALL.matchAll(/>(\d+)<\/span><span style="font-size:9\.5px[^>]*>\/ (\d+)</g)].map(m=>m[1]+'/'+m[2]).slice(0,8),
   ['40/160','0/14','0/11','0/13','0/12','0/16','0/15','15/27']);
ck('  so an elapsed empty month is visibly empty, not just unclicked',
   /0<\/span><span style="font-size:9\.5px[^>]*>\/ 14</.test(ALL),true);
ck('  and the rail explains the pair',
   /recorded \/ churned that month/.test(ALL),true);
// The All row counts the whole pool, which exceeds any month, so on the month
// scale its bar ran past its own track.
ck('the rail bar is clamped so the All row cannot overflow',
   /Math\.min\(100,Math\.round\(\(rec\/_c5Max\)\*100\)\)/.test(C5),true);
// A month that has not happened is not a month with nothing in it.
ck('future months are not buttons',
   (()=>{const r=/(<div|<button)[^>]*onclick="bdSetChurnProdMonth\('12'\)"/.test(ALL);return r;})(),false);
ck('  and show a dot rather than a zero',/·<\/span>/.test(ALL),true);
// Checked on a month OTHER than the selected one: the selected month's own
// figure is unchanged either way, so it cannot tell a fixed distribution from
// one that collapses under the filter.
ck('the rail counts come from the pool, not the selection',
   /9<\/span><span style="font-size:9\.5px[^>]*>\/ 19</.test(JUL),true);
ck('  so every other month keeps its figure while one is selected',
   [...JUL.matchAll(/>(\d+)<\/span><span style="font-size:9\.5px[^>]*>\/ (\d+)</g)]
     .map(m=>m[1]+'/'+m[2]).slice(0,8),
   ['40/160','0/14','0/11','0/13','0/12','0/16','0/15','15/27']);

// ── empty and thin states ──
ck('an elapsed month with no records says so',/Nothing recorded for Jan 2026/.test(JAN),true);
ck('  distinguishing no recording from no churn',
   /brands churned that month, but none had Implemented Products filled in/.test(JAN),true);
// Eight clickable empty months need one click back, not a sentence.
ck('  and offers one click back',
   /onclick="bdSetChurnProdMonth\(''\)"[^>]*>Show all 40/.test(JAN),true);
// At n=1 a mean is a single observation and must not look like a trend.
ck('a single-brand month drops the word Avg',
   /Time to churn<\/div>/.test(OCT)&&!/Avg time to churn<\/div>/.test(OCT),true);
ck('  and says it is one brand, not an average',
   /one brand, not an average/.test(OCT),true);

// ── unknown values are never silently dropped ──
ck('an unrecognised product keeps its own row',
   (()=>{const one=[{implemented_products:'Badge;Hologram',pausechurn_date:'2026-07-01',
     entered_closedwon:'2025-01-01',dealstage:'3453925110',m:'07'}];
     const h=renderC5('',one,[]);
     // scoped to the per-product block, which is where a silent drop would hide
     const prod=h.slice(h.indexOf('By product'),h.indexOf('By combination'));
     return /Hologram/.test(prod);})(),true);
ck('  and the combination row names it too',
   (()=>{const one=[{implemented_products:'Badge;Hologram',pausechurn_date:'2026-07-01',
     entered_closedwon:'2025-01-01',dealstage:'3453925110',m:'07'}];
     const h=renderC5('',one,[]);
     return /Badge\/Banner \+ Hologram/.test(h.slice(h.indexOf('By combination')));})(),true);

// ── the caveat survives ──
ck('the coverage footnote names the churn-only field',
   /no active brand carries it at all/.test(ALL),true);
ck('  and what would make a real rate possible',
   /Setting the field on active brands too/.test(ALL),true);
ck('the worker still fetches the field',/'implemented_products',/.test(src),true);
ck('  and maps it onto the deal row',/implemented_products: p\.implemented_products\|\|'',/.test(src),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
