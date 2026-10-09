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
  fn(bd,'_bdMonthOfG'), fn(bd,'_bdQuarterOf'), fn(bd,'_bdIsBoardChurn'), fn(bd,'_kmQStartMonth'),
  decl(bd,"var BD_CHURN_KM_FROM=","';"), "var BD_CHURN_KM_YEAR=2026;",
  'Object.assign(globalThis,{_bdMonthOfG,_bdQuarterOf,_bdIsBoardChurn,_kmQStartMonth,BD_CHURN_TYPE_GROUP,BD_CHURN_KM_FROM});',
].join('\n'),ctx);
ck('the helpers loaded',typeof ctx._bdQuarterOf,'function');
// The quarter's denominator is read from _snByMonth at this month, so an
// off-by-one here silently divides by the wrong quarter's book.
ck('each quarter starts at the right month',
   ['Q1','Q2','Q3','Q4'].map(q=>ctx._kmQStartMonth(q)),
   ['2026-01','2026-04','2026-07','2026-10']);

// ── 1. The 2026 floor ─────────────────────────────────────────────────────
// Before 2026 the book was a handful of brands, where one churn swings a rate
// by tens of points — the same reason C2 refuses a thin month.
ck('the floor is the start of 2026',ctx.BD_CHURN_KM_FROM,'2026-01-01');
ck('the scope filter compares months against it',
   /_m>=BD_CHURN_KM_FROM\.substring\(0,7\)/.test(bd),true);
ck('and it filters on the churn date, not the create date',
   /var _m=_bdMonthOf\(dd\.pausechurn_date\);\s*\n\s*return !!_m && _m>=BD_CHURN_KM_FROM/.test(bd),true);
ck('undated churns are counted out loud, not dropped silently',
   /_kmUndated\?\(_kmUndated\+' undated, excluded'\)/.test(bd),true);

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
ck('three tiles',(bd.match(/hChurn\+=_chTile\(/g)||[]).length,3);
ck('each one is handed a quarter strip',(bd.match(/_kmQStrip\(_c(Count|Pct|Ten)\)/g)||[]).length,3);
ck('the strip is built once',(bd.match(/var _kmQStrip=/g)||[]).length,1);
ck('and the tiles are Churned Deals, Churn Rate, Avg Time to Churn',
   [...bd.matchAll(/hChurn\+=_chTile\('([^']+)'/g)].map(m=>m[1]),
   ['Churned Deals','Churn Rate','Avg Time to Churn']);

// ── 6. The rate divides by the opening book, like C2 ──────────────────────
// A share of the whole book would make each quarter look smaller merely for
// being shorter. C1 and C2 read the same snapshot series the same way.
ck('C1 takes the book at the period start',
   /_kmOpenAt=function\(m\)\{var pv=_snByMonth\[_kmPrevMonth\(m\)\];return pv\?pv\.b:0;\}/.test(bd),true);
ck('  which is what C2 does',
   /_chOpen=function\(m\)\{var p=_snByMonth\[_chPrev\(m\)\];return p\?p\.b:0;\}/.test(bd),true);
// Each QUARTER divides by the active book at that quarter's start — one point,
// not an average across its months.
ck('each quarter divides by its own opening book',
   /var _o=_kmOpenQ\(q\);/.test(bd),true);
ck('  which is the book at the quarter start',
   /_kmOpenQ=function\(q\)\{return _kmOpenAt\(_kmQStartMonth\(q\)\);\}/.test(bd),true);
ck('the per-month averaging is gone',/_kmAvgOpen|_kmMonthsOf/.test(bd),false);
// The TOTAL cannot use any single opening count: most of the year's churn comes
// from brands that signed after January and were never in that number.
ck('the total divides by everything on the book since 2026',
   /var _kmTotalBase=closedWonDeals\.length\+_kmAll\.length;/.test(bd),true);
ck('  and the headline rate uses it',
   /var _kmPct=_kmRate\(_kmDeals\.length,_kmTotalBase\);/.test(bd),true);
// _kmAll is every DATED 2026 churn; _kmDeals is the Board-Defined subset. Built
// from the subset, the denominator would shrink exactly when the toggle is on
// and inflate the board rate.
ck('  built from all 2026 churns, not the Board Defined subset',
   /_kmTotalBase=closedWonDeals\.length\+_kmDeals\.length/.test(bd),false);
ck('  and the tile names the denominator it used',
   /' on the book since 2026'/.test(bd),true);
ck('the tile spells out both bases',
   /active now plus churned since\. Each quarter divides by the active book at the start of that quarter/.test(bd),true);
// The quarter-start map is back: a quarter's denominator is now a single point
// (its opening book), so there IS a start month to pin again.
ck('the quarter-start map exists',
   /_kmQStartMonth=function\(q\)\{return BD_CHURN_KM_YEAR\+'-'\+\('0'\+\(\(\+q\.slice\(1\)-1\)\*3\+1\)\)\.slice\(-2\);\}/.test(bd),true);
ck('a zero book yields a dash, not a divide-by-zero',
   /_kmRate=function\(n,open\)\{return open>0\?\(\(n\/open\)\*100\):null;\}/.test(bd),true);

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
ck('both cover 2026 onward',
   (bd.match(/_ch(Ab|Tr)Set=_ch(Board|Deals)\.filter\(function\(dd\)\{return _chIn2026\(dd\);\}\)/g)||[]).length,2);
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

// ── 17. C8 — implemented products vs churn ──────────────────────────────
// implemented_products is written during the CHURN REVIEW. Every deal that
// carries it is churned and no active deal has it, so a churn RATE built on it
// divides churned brands by a denominator that can only hold churned brands and
// reads 100% for every product. The rate questions therefore run on
// of_total_reviews / of_products, which both populations carry, and the field
// itself is used only for the mix of brands that left.
const C8=(()=>{const i=bd.indexOf('var BD_PROD_GROUP=');return bd.slice(i,bd.indexOf('</section>',i));})();
ck('the section is locatable',C8.length>1500,true);
// Badge and Banner are the same placement sold two ways.
ck('Badge and Banner collapse to one product',
   /BD_PROD_GROUP=\{Badge:'Badge\/Banner',Banner:'Badge\/Banner'\}/.test(bd),true);
ck('  and the order lists four products, not five',
   (()=>{const m=/BD_PROD_ORDER=\[([^\]]*)\]/.exec(bd);
         return m[1].split(',').map(x=>x.trim().replace(/'/g,''));})(),
   ['Badge/Banner','Reviews','CAI','CAS']);
ck('the multi-select is split on its delimiter',/raw\.split\(';'\)/.test(C8),true);
ck('  and de-duplicated, so Badge+Banner counts once',
   /out\[BD_PROD_GROUP\[t\]\|\|t\]=1/.test(C8),true);

// ── regression 1: no rate may be built on implemented_products ──
ck('no rate is built on implemented_products',
   /_chProdStat|_chRevYes|_chRevNo|_chProdNone/.test(bd),false);
ck('  and the section says outright that no active brand carries the field',
   /no active brand carries it at all/.test(C8),true);
ck('  and names what would be needed to get a real per-product rate',
   /Setting Implemented Products on active brands too/.test(C8),true);

// ── regression 2: ONE axis for every rate row ──
// The previous cut scaled each block to its own maximum, so 11.6% drew a LONGER
// bar than 12.6% one block away. Every rate row now goes through one _c8X.
ck('the per-block max-scaled bar renderer is gone',/_chBandBars/.test(bd),false);
ck('one axis function serves every rate row',
   (C8.match(/_c8X\s*=\s*function/g)||[]).length,1);
ck('  and the axis top is computed once',
   (C8.match(/var _c8Ax=/g)||[]).length,1);
// The axis must reach the context rows. Cropped to the bands it would hide the
// larger fact (36-52% unrecorded) to magnify the smaller one (11-19%).
ck('the axis domain includes the context rows',
   /g\.rows\.concat\(\[g\.ctx\]\)\.forEach/.test(C8),true);
ck('  and the book rate too',/_c8Book!=null&&_c8Book>_c8Max/.test(C8),true);

// ── the rate questions run on fields BOTH populations carry ──
ck('reviews are banded on the recorded review count',
   /_chBandStat\('of_total_reviews',/.test(C8),true);
ck('product depth is banded on the recorded product count',
   /_chBandStat\('of_products',/.test(C8),true);
ck('the band denominator is every brand that ever signed',
   /var _chBook=closedWonDeals\.concat\(_chDeals\);/.test(bd),true);
ck('  and churn is decided by deal stage, not by the set it came from',
   /_chIsChurned=function\(dd\)\{return dd\.dealstage===BD_CHURN_STAGE\|\|dd\.dealstage===BD_CHURN_ONB_STAGE;\}/.test(C8),true);
ck('it is not floored at 2026',/_chIn2026/.test(C8),false);
// The excluded brands are the biggest effect in the data, so they are plotted
// rather than left in a footnote — but kept out of the ordinal run.
ck('the unrecorded brands carry their own churn count',/unsetCh/.test(C8),true);
ck('  and are plotted as a context row, not folded into a band',
   /ctx:\{l:'No '\+field\+' recorded'/.test(C8),true);
ck('  under a rule that says they are not part of the run',
   /For context — not part of the run/.test(C8),true);

// ── functional: helpers ──
const ctx8={console};vm.createContext(ctx8);
vm.runInContext([
  "var BD_CHURN_STAGE='3453925110', BD_CHURN_ONB_STAGE='3517067985';",
  'var _chBook=[];',
  decl(bd,'var BD_PROD_GROUP={','};'),
  decl(bd,'var C8_THIN=',';'),
  fn(bd,'_chProdsOf'), fn(bd,'_chIsChurned'), fn(bd,'_chNum'),
  fn(bd,'_chPctOf'), fn(bd,'_chFmtPct'), fn(bd,'_c8N'), fn(bd,'_chBandStat'),
  fn(bd,'_c8Wilson'), fn(bd,'_c8Z'), fn(bd,'_c8Nice'), fn(bd,'_c8Verdict'),
  'Object.assign(globalThis,{_chProdsOf,_chIsChurned,_chNum,_chPctOf,_chFmtPct,_c8N,'
    +'_chBandStat,_c8Wilson,_c8Z,_c8Nice,_c8Verdict,C8_THIN});',
].join('\n'),ctx8);
ck('the C8 helpers loaded',typeof ctx8._chBandStat,'function');

const shipBands=k=>{const m=new RegExp("_chBandStat\\('"+k+"',(\\[[\\s\\S]*?\\])\\)").exec(C8);
  if(!m)throw new Error('no bands for '+k);return eval(m[1]);};
const BANDS=shipBands('of_total_reviews'), PBANDS=shipBands('of_products');
ck('the shipped review bands are the ones under test',
   BANDS.map(b=>[b.lo,b.hi]),[[1,2],[3,9],[10,null]]);
ck('  and the product bands likewise',PBANDS.map(b=>[b.lo,b.hi]),[[1,1],[2,2],[3,null]]);
const contiguous=bs=>bs.every((b,i)=>i===0?b.lo===1:b.lo===bs[i-1].hi+1)&&bs[bs.length-1].hi===null;
ck('review bands tile the range with no gap and no overlap',contiguous(BANDS),true);
ck('  and so do the product bands',contiguous(PBANDS),true);

const A={dealstage:'closedwon'}, CH={dealstage:'3453925110'}, CO={dealstage:'3517067985'};
const mk=(st,v)=>Object.assign({},st,{of_total_reviews:v});
ctx8._chBook=[
  mk(A,'1'), mk(A,'2'), mk(CH,'2'),
  mk(A,'3'), mk(A,'9'), mk(CH,'5'), mk(CO,'4'),
  mk(A,'10'), mk(A,'400'),
  mk(A,''), mk(CH,''), mk(A,'0'), mk(A,'abc'),
];
const B=ctx8._chBandStat('of_total_reviews',BANDS);
ck('each band holds the brands whose count falls in it',B.rows.map(r=>r.n),[3,4,2]);
ck('  and counts the churned ones inside it',B.rows.map(r=>r.ch),[1,2,0]);
ck('  with both churn stages counting as churned',B.rows[1].ch,2);
ck('blanks, zero and junk are excluded rather than banded',B.unset,4);
ck('  and their churns are kept, not discarded',B.unsetCh,1);
const one=v=>{ctx8._chBook=[Object.assign({},A,{of_total_reviews:String(v)})];
  const s=ctx8._chBandStat('of_total_reviews',BANDS);
  const hit=s.rows.map((r,i)=>r.n===1?i:-1).filter(i=>i>=0);
  return hit.length===1?hit[0]:'landed in '+hit.length;};
ck('every value lands in exactly one band',[1,2,3,9,10,11,9999].map(one),[0,0,1,1,2,2,2]);
ctx8._chBook=[];
const pctKind=r=>{const p=ctx8._chPctOf(r);
  return p===null?'null':(typeof p==='number'&&!isFinite(p)?'NaN':'num:'+p);};
ck('an empty book yields no divide-by-zero',
   ctx8._chBandStat('of_total_reviews',BANDS).rows.map(pctKind),['null','null','null']);
ck('  and a null rate prints as a dash',ctx8._chFmtPct(null),'—');

// ── functional: Wilson interval ──
// Checked against the published values for these counts; the textbook normal
// interval would run below zero on the thinnest band.
const w=(c,n)=>ctx8._c8Wilson({c:c,n:n,ch:c}).map(v=>+v.toFixed(1));
ck('the interval matches the published Wilson values',w(62,326),[15.1,23.6]);
ck('  and widens sharply on a thin band',w(5,32),[6.9,31.8]);
ck('  staying inside 0-100 at the extremes',
   [w(0,10)[0]>=0, w(10,10)[1]<=100],[true,true]);
ck('  and it brackets the point estimate',
   (()=>{const i=w(16,127);return i[0]<=12.6&&i[1]>=12.6;})(),true);
ck('an empty band has no interval',ctx8._c8Wilson({n:0,ch:0}),null);

// ── functional: the z gate ──
const Z=(c1,n1,c2,n2)=>+ctx8._c8Z({ch:c1,n:n1},{ch:c2,n:n2}).toFixed(2);
ck('the reviews endpoints do not clear the gate',Z(62,326,16,127),1.63);
ck('the products endpoints do not come close',Z(22,189,17,128),-0.44);
ck('a real difference does clear it',Math.abs(Z(100,300,20,300))>2,true);
ck('an empty side yields no z',ctx8._c8Z({ch:0,n:0},{ch:5,n:10}),null);

// ── functional: the verdict REFUSES to overclaim ──
// This is the bug the redesign exists to kill: the previous gate tested effect
// SIZE (a flat 1pp) with no reference to how many brands were behind it, and so
// printed "6.4pp less" in bold for a gap its own counts cannot resolve.
const G=rows=>({rows});
const vRev=ctx8._c8Verdict(G([{l:'1–2 reviews',n:326,ch:62},{l:'3–9 reviews',n:258,ch:39},{l:'10+ reviews',n:127,ch:16}]));
ck('the reviews verdict declines to state a difference',vRev.tone,'nodiff');
ck('  and never prints a pp figure',/pp/.test(vRev.badge+vRev.calc),false);
ck('  and explains why in plain words',
   /smaller than the error on it/.test(vRev.calc),true);
const vPro=ctx8._c8Verdict(G([{l:'1 product',n:189,ch:22},{l:'2 products',n:32,ch:5},{l:'3+ products',n:128,ch:17}]));
// Non-monotone: the peak is the MIDDLE band, so an endpoint summary would
// report a trend that is not in the data.
ck('the products verdict reports no pattern, not a trend',vPro.tone,'nopattern');
ck('  naming the middle band as the worst',/2 products in the middle/.test(vPro.calc),true);
ck('  and flagging that it is too thin to carry it',
   /only 32 brands, so one brand moves it by 3\.1 points/.test(vPro.calc),true);
ck('  and never prints a pp figure either',/pp/.test(vPro.badge+vPro.calc),false);
// A difference that DOES clear the gate is still allowed to be stated.
const vReal=ctx8._c8Verdict(G([{l:'low',n:300,ch:100},{l:'high',n:300,ch:20}]));
ck('a difference that clears the gate is stated',vReal.tone,'less');
ck('  with the gap in points',/pp lower at high/.test(vReal.calc),true);
ck('  and still refuses to claim cause',/Association, not cause/.test(vReal.calc),true);
ck('one populated band is not a comparison',
   ctx8._c8Verdict(G([{l:'a',n:0,ch:0},{l:'b',n:10,ch:4}])).tone,'none');
// The verdict no longer restates the band percentages: they are already in the
// value column of every row, which is what made the card band below the chart
// pure duplication.
ck('the verdict carries no headline of its own',
   [vRev,vPro,vReal].map(v=>Object.keys(v).sort().join(',')),
   ['badge,calc,tone','badge,calc,tone','badge,calc,tone']);
ck('  and the card band is gone',/flex:1 1 380px/.test(bd),false);
ck('  with the badge rendered in the group header instead',
   /\+v\.badge\+'<\/span>'/.test(bd),true);
ck('  and its one reason directly beneath',
   /margin:0 0 6px;line-height:1\.5">'\+v\.calc\+'<\/div>'/.test(bd),true);

// ── functional: axis steps ──
ck('the axis snaps to a familiar step and stays under eight ticks',
   [52.5,19,8,97].map(v=>{const a=ctx8._c8Nice(v);return [a.top,a.step,a.top/a.step<=7];}),
   [[60,10,true],[20,5,true],[8,2,true],[100,20,true]]);
ck('  and always covers the value it was given',
   [52.5,19,8,97,0.4].every(v=>ctx8._c8Nice(v).top>=v),true);

// ── functional: render the real section and check the OUTPUT ──
const C8BLOCK=(()=>{const i=bd.indexOf('var BD_PROD_GROUP=');
  const m="hChurn+='</div></section>';";return bd.slice(i,bd.indexOf(m,i)+m.length);})();
const fill=(total,spec)=>{const o=[];spec.forEach(([k,v])=>{for(let i=0;i<k;i++)o.push(v);});
  while(o.length<total)o.push('');return o;};
const render=()=>{
  const cRev=fill(315,[[62,'1'],[39,'5'],[16,'12']]), cPro=fill(315,[[22,'1'],[5,'2'],[17,'3']]);
  const aRev=fill(773,[[264,'1'],[219,'5'],[111,'12']]), aPro=fill(773,[[167,'1'],[27,'2'],[111,'3']]);
  const impl=[];for(let k=0;k<41;k++){const p=[];
  // Badge/Banner on 1..40 and Reviews on 0..30, so all 41 carry something, the
  // marginals match live (40 / 31 / 15 / 0) and exactly 30 hold both.
    if(k>=1)p.push(k%2?'Badge':'Banner'); if(k<31)p.push('Reviews'); if(k<15)p.push('CAI');
    impl.push(p.join(';'));}
  const churned=[],active=[];
  for(let k=0;k<315;k++)churned.push({dealstage:'3453925110',of_total_reviews:cRev[k],of_products:cPro[k],implemented_products:impl[k]||''});
  for(let k=0;k<773;k++)active.push({dealstage:'closedwon',of_total_reviews:aRev[k],of_products:aPro[k],implemented_products:''});
  const c={console,closedWonDeals:active,_chDeals:churned,BD_CHURN_STAGE:'3453925110',
    BD_CHURN_ONB_STAGE:'3517067985',BD_CHURN_C:'#DC2626',hChurn:''};
  vm.createContext(c);vm.runInContext(C8BLOCK,c);return c.hChurn;
};
const HTML=render();
ck('the section renders',HTML.length>8000,true);
const outRows=HTML.split('class="bd-seg-row bd-rate-band"').slice(1).map(seg=>{
  const l=/data-p="([^"]*)"/.exec(seg), p=/data-r1="Churn rate\|([\d.]+)%/.exec(seg);
  const x=/left:([\d.]+)%;width:10px;height:10px/.exec(seg);
  return {l:l&&l[1], pct:p&&+p[1], x:x&&+x[1]};
});
ck('every rate row is plotted — three bands and a context row per measure',outRows.length,8);
ck('  and the live figures come out as expected',
   outRows.map(r=>r.pct),[19,15.1,12.6,52.5,11.6,15.6,13.3,36.7]);
// THE regression. Across ALL eight rows, not within a block: a higher rate must
// never draw further LEFT than a lower one.
const bySize=[...outRows].sort((a,b)=>a.pct-b.pct);
ck('no row with a higher rate sits left of a lower one',
   bySize.filter((r,i)=>i>0&&r.x<bySize[i-1].x-0.01).map(r=>r.l),[]);
ck('  because every row divides by the same axis top',
   [...new Set(outRows.map(r=>Math.round(r.pct/r.x*100)))],[60]);
ck('  and that top clears the largest context row',outRows[3].pct<=60,true);
// Copy must not leak notation at the reader.
ck('no statistical notation reaches the rendered copy',
   /z\s*=|z-score|standard error|p-value|p\s*&lt;|confidence interval|statistically/i.test(HTML),false);
ck('the thin band is called out by its count, not by jargon',
   /32 BRANDS<\/span>/.test(HTML),true);
ck('  and only the thin band is',(HTML.match(/ BRANDS<\/span>/g)||[]).length,1);
ck('the book rate is drawn as a rule',/border-left:1\.5px dashed/.test(HTML),true);
ck('  and labelled with its own arithmetic',/29\.0% · 315 of 1,088/.test(HTML),true);
ck('counts over a thousand are grouped',/1,088/.test(HTML),true);
// The hover contract: bdBindRateBands matches on class and reads getAttribute.
ck('rate rows carry the class the existing binder matches',
   (HTML.match(/class="bd-seg-row bd-rate-band"/g)||[]).length,8);
ck('  inside a chart-wrap',/class="chart-wrap"/.test(HTML),true);
ck('  with the float-tip the binder needs',/class="float-tip"/.test(HTML),true);
ck('  and every row carries a readout',
   (HTML.match(/data-r1="Churn rate/g)||[]).length,8);
ck('no new hover binder was added',/bdBindC8|bd-c8-band/.test(bd),false);

// ── functional: the churned-brand mix ──
ck('Badge and Banner collapse, and a brand counts once',
   ctx8._chProdsOf({implemented_products:'Badge;Banner;Reviews'}).sort(),['Badge/Banner','Reviews']);
ck('  an unrecorded field yields nothing',ctx8._chProdsOf({implemented_products:''}),[]);
ck('  and so does a field of empty segments',ctx8._chProdsOf({implemented_products:';;'}),[]);
ck('the mix iterates the churned deals alone',
   /_chDeals\.forEach\(function\(dd\)\{\s*\n\s*var ps=_chProdsOf\(dd\);/.test(C8),true);
ck('  and is badged a count, never a rate',
   /A COUNT, NEVER A CHURN RATE/.test(HTML),true);
ck('  and says the rows overlap',/so the rows overlap and do not sum/.test(C8),true);
// C7's rule: a real category with zero is a FINDING and keeps its row. Dropping
// it would read as "not measured".
ck('a product none of them had keeps its row at zero',
   /CAS<\/div>/.test(HTML)&&/_chMixRows=BD_PROD_ORDER\.map/.test(C8),true);
ck('  and the mix counts come out right',
   (HTML.match(/font-weight:700;color:var\(--navy\)">(\d+)<\/span><span style="font-size:10\.5px;color:var\(--grey\);margin-left:5px">of 41/g)||[]).length,3);
ck('the co-occurrence is stated as a number, not left to be counted',
   /30 of the 31 that had reviews live also had a badge or banner/.test(HTML),true);
ck('an empty mix says so rather than drawing nothing',
   /No churned brand has Implemented Products recorded/.test(C8),true);

// The worker has to actually fetch every field this section reads.
ck('implemented_products is requested',/'implemented_products',/.test(src),true);
ck('  and mapped onto the deal row',/implemented_products: p\.implemented_products\|\|'',/.test(src),true);
ck('of_total_reviews is requested',/'of_total_reviews',/.test(src),true);
ck('  and mapped onto the deal row',/of_total_reviews: p\.of_total_reviews\|\|'',/.test(src),true);
ck('of_products is requested',/'of_products',/.test(src),true);
ck('  and mapped onto the deal row',/of_products: p\.of_products\|\|'',/.test(src),true);

// The deals table moved down a number when this section was inserted.
ck('the deals table is C9 now',/<span class="bd-sec-n">C9<\/span><span class="bd-sec-t">All Churned Deals/.test(bd),true);
ck('  and the new section is C8',/<span class="bd-sec-n">C8<\/span><span class="bd-sec-t">Implemented Products/.test(bd),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
