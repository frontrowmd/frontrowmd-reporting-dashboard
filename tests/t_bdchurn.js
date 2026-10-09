// BD Tracker · Churn tab · C1 Key Metrics. Scoped to 2026 onward, each figure
// split by quarter, with a Board Defined / All switch where Board Defined folds
// in Delinquent Payment.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const bd=load(process.env.BD_SRC||'bd-dashboard.html');
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
ck('All is the default, so the tiles keep the meaning they shipped with',
   /var _bdChurnKmScope='all';/.test(bd),true);
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
// The denominator is the AVERAGE active book, not the count on day one:
// dividing a year of churn by the 1 January book ignores every brand that
// signed later, so the rate read high and got worse the faster the book grew.
ck('the rate divides by the average active book',
   /var _kmOpenYear=_kmAvgOpen\(_kmMonthsOf\(null\)\);/.test(bd),true);
ck('  and each quarter averages its OWN months',
   /var _o=_kmAvgOpen\(_kmMonthsOf\(q\)\);/.test(bd),true);
ck('  averaging each month\u2019s opening book, skipping months with none',
   /months\.forEach\(function\(m\)\{var o=_kmOpenAt\(m\); if\(o>0\)\{tot\+=o;n\+\+;\}\}\);/.test(bd),true);
ck('  never counting past the current month',
   /var last=\(n\.getFullYear\(\)>BD_CHURN_KM_YEAR\)\?12:\(n\.getMonth\(\)\+1\);/.test(bd),true);
ck('  and the tile names the denominator it used',
   /' avg active brands'/.test(bd),true);
ck('the tile says the day-one count is NOT what it divides by',
   /not the count on day one, which ignores every brand that signed later/.test(bd),true);
// The fixed quarter-start map went with the day-one denominator — each quarter
// now averages its own months, so there is nothing left to pin a start month to.
ck('no fixed quarter-start map survives',/_kmQStart/.test(bd),false);
ck('a quarter spans its own three months, capped at the current one',
   /var from=q\?\(\(\+q\.slice\(1\)-1\)\*3\+1\):1, to=q\?Math\.min\(from\+2,last\):last;/.test(bd),true);
ck('  and the headline spans January through the current month',
   /var from=q\?[^;]*:1, to=q\?[^;]*:last;/.test(bd),true);
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
ck('there are eight of them',[navIds.length,secIds.length],[8,8]);
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

// ── 11. C5 and C6 scope to a period rather than growing an axis ──────────
// Their axes are categorical (A/B answer x traffic band; traffic band x
// reason), so a grain cannot become an axis without destroying the section.
// The toggle filters WHICH churns are counted instead.
ck('both have a Monthly⁄Quarterly toggle',
   /_chGrainSeg\(_bdChurnAbGrain,'bdSetChurnAbGrain'\)/.test(bd)
   &&/_chGrainSeg\(_bdChurnTrGrain,'bdSetChurnTrGrain'\)/.test(bd),true);
ck('  with their own state, like C3 and C4',
   /var _bdChurnAbGrain='month', _bdChurnTrGrain='month';/.test(bd),true);
ck('  defaulting to monthly',
   (bd.match(/_bdChurn(Ab|Tr|Rate)Grain='month'/g)||[]).length,3);
ck('  and each re-renders on change',
   /function bdSetChurnAbGrain\(g\)\{if\(_bdChurnAbGrain===g\)return;_bdChurnAbGrain=g;renderBD\(\);\}/.test(bd)
   &&/function bdSetChurnTrGrain\(g\)\{if\(_bdChurnTrGrain===g\)return;_bdChurnTrGrain=g;renderBD\(\);\}/.test(bd),true);
// The LATEST period with data, not the current calendar one: a month with no
// churn yet would blank the matrix and read as "no churn of any kind", which
// is a different claim from "none yet this month".
// The period containing TODAY is a part-period. On 2026-10-09 that was 3 of
// 2026's 148 churns, which rendered as an almost-empty matrix reading as "no
// churn" rather than "the month has barely started".
ck('the period is the latest COMPLETE one',/if\(k<cur&&k>best\)best=k;/.test(bd),true);
ck('  with the current period excluded by comparison against today',
   /var cur=_chTodayKey\(grain\)/.test(bd),true);
ck('  and a fallback when the only data IS the current period',/return best\|\|any;/.test(bd),true);
ck('today\u2019s key respects the grain',
   /if\(grain==='quarter'\)return y\+'-Q'\+\(Math\.floor\(n\.getMonth\(\)\/3\)\+1\);/.test(bd),true);
ck('  and the label says the period is complete',
   (bd.match(/the most recent complete '\+\(_bdChurn(Ab|Tr)Grain/g)||[]).length,2);
ck('  floored at 2026 like the rest of the tab',
   /_chLatest=function\(list,grain\)[\s\S]{0,420}?k<BD_CHURN_DETAIL_FROM\)return;/.test(bd),true);
ck('C5 filters its board-defined set to that period',
   /_chAbSet=_chBoard\.filter\(function\(dd\)\{return _chInPeriod\(dd,_chAbPeriod,_bdChurnAbGrain\);\}\)/.test(bd),true);
ck('C6 filters the churned set to its own period',
   /_chTrSet=_chDeals\.filter\(function\(dd\)\{return _chInPeriod\(dd,_chTrPeriod,_bdChurnTrGrain\);\}\)/.test(bd),true);
// Each section's period must be resolved with its OWN grain, or flipping one
// toggle silently moves the other section's window.
ck('  and resolves that period with its OWN grain',
   /_chTrPeriod=_chLatest\(_chDeals,_bdChurnTrGrain\)/.test(bd),true);
ck('  as does C5',/_chAbPeriod=_chLatest\(_chBoard,_bdChurnAbGrain\)/.test(bd),true);
ck('  and each matrix counts the SCOPED set, not the full one',
   /_chAbSet\.forEach\(function\(dd\)\{/.test(bd)&&/_chTrSet\.forEach\(function\(dd\)\{/.test(bd),true);
ck('  with no stale full-set loop left behind',
   /_chBoard\.forEach\(function\(dd\)\{\s*\n\s*if\(\(dd\.ab_test/.test(bd),false);
// A scoped count is meaningless unless the scope is on screen.
ck('both name the period they are showing',
   (bd.match(/the most recent complete '\+\(_bdChurn(Ab|Tr)Grain==='quarter'\?'quarter':'month'\)\+' with churn\./g)||[]).length,2);
ck('  and say how much of the whole set that is',
   /\+' \(of '\+_chBoard\.length\+' all time\)/.test(bd)
   &&/\+_chTrSet\.length\+' of '\+_chDeals\.length\+' churned deals\. '/.test(bd),true);
ck('an empty period says so rather than showing a blank grid',
   (bd.match(/No churn dated 2026 onward yet\./g)||[]).length,2);

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

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
