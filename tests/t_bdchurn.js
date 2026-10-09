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
ck('quarters are rates, and the tile says they do not sum',
   /quarters are rates, so they do not\s*\\?n?\s*sum|quarters are rates, so they do not sum/.test(bd.replace(/\s+/g,' ')),true);
ck('the quarter opening months are declared',
   /_kmQStart=\{Q1:BD_CHURN_KM_YEAR\+'-01',Q2:BD_CHURN_KM_YEAR\+'-04',Q3:BD_CHURN_KM_YEAR\+'-07',Q4:BD_CHURN_KM_YEAR\+'-10'\}/.test(bd),true);
// Declaring them is not using them: dividing every quarter by January's book
// would make Q2-Q4 read high and still pass a declaration check.
ck('  and each quarter actually divides by ITS own',
   /var _o=_kmOpenAt\(_kmQStart\[q\]\);/.test(bd),true);
ck('  while the headline divides by the start of the year',
   /var _kmOpenYear=_kmOpenAt\(BD_CHURN_KM_YEAR\+'-01'\);/.test(bd),true);
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

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
