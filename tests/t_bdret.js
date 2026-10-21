// BD Tracker · Churn tab · C3 Retention Waterfall. Cohort rows are signing
// months, columns are whole months of tenure, cells are the share of that
// cohort still live. Its data rules deliberately DIFFER from the rest of the
// tab — a different floor and a different signing date — so most of what is
// asserted here is that those differences are real and stay deliberate.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const bd=load(process.env.BD_SRC||'bd-dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console};vm.createContext(ctx);
vm.runInContext([
  fn(bd,'_bdMonthOfG'),
  'function _bdMonthOf(v){return _bdMonthOfG(v);}',
  decl(bd,"var RW_FROM=","';"),
  decl(bd,'var RW_NEUTRAL=',';'),
  decl(bd,'var RW_BLEND_MIN=',';'),
  decl(bd,'var RW_W_LBL=',';'),
  decl(bd,'var RW_GREEN=','];'),
  decl(bd,'var RW_RED=','];'),
  decl(bd,'var _rwMN=','];'),
  fn(bd,'_rwDiff'), fn(bd,'_rwAdd'), fn(bd,'_rwLbl'),
  fn(bd,'_rwSignOf'), fn(bd,'_rwFill'), fn(bd,'_rwBuild'),
  'Object.assign(globalThis,{_rwDiff,_rwAdd,_rwLbl,_rwSignOf,_rwFill,_rwBuild,RW_FROM,RW_NEUTRAL,RW_LOW,RW_BLEND_MIN,RW_BLEND_MIN_COHORTS,RW_W_LBL,RW_W_N,RW_W_CELL,RW_W_CELL_MAX,RW_GREEN,RW_RED});',
].join('\n'),ctx);
ck('the helpers loaded',[typeof ctx._rwBuild,typeof ctx._rwFill],['function','function']);

// The whole section, sliced once — several blocks below assert against it.
const SEC=(()=>{const i=bd.indexOf('// ── iii. Retention waterfall');
  const j=bd.indexOf('// ── iv. ',i);
  if(i<0||j<0)throw new Error('cannot locate section iii between its own comment markers');
  return bd.slice(i,j);})();
ck('the section is locatable, and the slice stops before the next one',
   SEC.length>5000&&SEC.length<32000&&!/bd-sec-churntype/.test(SEC)
   &&/hChurn\+='<\/div><\/section>';/.test(SEC),true);

// Runs the SHIPPED _rwCellVal under a chosen metric/unit pair. Hoisted because
// several blocks below need it.
const CV=(metric,mode)=>{const c={console};vm.createContext(c);
  vm.runInContext([fn(bd,'_rwPct'),fn(bd,'_rwSigned'),"var _bdRetMode='"+mode+"';",
    "var _rwChurnView="+(metric==='churn')+";",
    fn(bd,'_rwCellVal'),'Object.assign(globalThis,{_rwCellVal,_rwSigned});'].join('\n'),c);
  return c;};
const CVAL=(metric,mode)=>CV(metric,mode)._rwCellVal;

// ── 1. Month arithmetic ───────────────────────────────────────────────────
ck('months count forward',ctx._rwDiff('2026-01','2026-04'),3);
ck('  and across a year boundary',ctx._rwDiff('2025-11','2026-02'),3);
ck('  and stay NEGATIVE when the churn precedes the signing',ctx._rwDiff('2026-04','2026-01'),-3);
ck('  with the same month reading zero',ctx._rwDiff('2026-04','2026-04'),0);
ck('a missing end of the pair yields null',[ctx._rwDiff('',''),ctx._rwDiff('2026-01','')],[null,null]);
ck('adding months rolls the year',[ctx._rwAdd('2026-11',3),ctx._rwAdd('2026-02',-3)],['2027-02','2025-11']);
ck('labels read as the eye expects',['2026-01','2025-12'].map(ctx._rwLbl),['Jan 26','Dec 25']);

// ── 2. The signing date is the EARLIER of the two real fields ─────────────
// entered_closedwon is the date of the LAST entry into closed-won and has been
// bulk-rewritten repeatedly; leading with it piled 163 deals into one phantom
// Feb-2025 cohort and put 50 churns at negative tenure.
ck('the earlier of the two dates wins',
   ctx._rwSignOf({closedate:'2024-06-03T04:00:00Z',entered_closedwon:'2025-02-06T19:49:22Z'}),'2024-06');
ck('  in either field order',
   ctx._rwSignOf({closedate:'2026-08-28T04:00:00Z',entered_closedwon:'2026-06-09T12:00:00Z'}),'2026-06');
ck('a missing close date falls back to the stage timestamp',
   ctx._rwSignOf({closedate:'',entered_closedwon:'2026-04-02T10:00:00Z'}),'2026-04');
ck('  and a missing pair falls back to createdate',
   ctx._rwSignOf({closedate:'',entered_closedwon:'',createdate:'2026-07-07T10:00:00Z'}),'2026-07');
ck('no date at all yields no cohort',ctx._rwSignOf({}),'');
// A HubSpot date field is stored at Eastern midnight, so slicing it is already
// an Eastern month — including across the DST change.
ck('an Eastern-midnight close date keeps its own month',
   [ctx._rwSignOf({closedate:'2024-01-03T05:00:00Z'}),ctx._rwSignOf({closedate:'2024-06-03T04:00:00Z'})],
   ['2024-01','2024-06']);

// ── 3. The triangle ───────────────────────────────────────────────────────
const B=(rows,from,now)=>ctx._rwBuild(rows,from,now);
ck('a lone survivor is retained every month',
   B([{sign:'2026-01',churned:false}],'2026-01','2026-03').cohorts[0].cells.map(c=>c.pct),[1,1,1]);
// A churn dated into month M removes the brand FROM M — the end-of-month
// convention the Snapshot chart already uses.
ck('a same-month churn is retained at NO month',
   B([{sign:'2026-01',churn:'2026-01',churned:true}],'2026-01','2026-03').cohorts[0].cells.map(c=>c.alive),[0,0,0]);
ck('a month-two churn is retained through Month 1 only',
   B([{sign:'2026-01',churn:'2026-02',churned:true}],'2026-01','2026-03').cohorts[0].cells.map(c=>c.alive),[1,0,0]);
const MIX=B([{sign:'2026-01',churn:'2026-01',churned:true},
             {sign:'2026-01',churn:'2026-03',churned:true},
             {sign:'2026-01',churned:false},{sign:'2026-01',churned:false}],'2026-01','2026-04');
ck('a mixed cohort counts down in step',MIX.cohorts[0].cells.map(c=>c.alive),[3,3,2,2]);
ck('  with the share following the counts',MIX.cohorts[0].cells.map(c=>+(c.pct*100).toFixed(1)),[75,75,50,50]);
ck('  and the cohort size counting every dated member',MIX.cohorts[0].n,4);
ck('  and lost + alive always making the cohort',MIX.cohorts[0].cells.every(c=>c.alive+c.lost===MIX.cohorts[0].n),true);
// The triangle: a cohort can only be read as far as it has actually lived.
ck('later cohorts are observable for fewer months',
   B([{sign:'2026-01',churned:false},{sign:'2026-03',churned:false}],'2026-01','2026-03')
     .cohorts.map(c=>c.cells.length),[3,2,1]);
ck('a month with no signings is still a row, with no share',
   (()=>{const r=B([{sign:'2026-03',churned:false}],'2026-01','2026-03');
     return [r.cohorts[0].n,r.cohorts[0].cells.map(c=>c.pct)];})(),[0,[null,null,null]]);

// ── 4. Every exclusion is counted, never silently dropped ────────────────
const EX=B([{sign:'',churned:false},
            {sign:'2026-02',churn:'',churned:true},
            {sign:'2026-02',churn:'2026-01',churned:true},
            {sign:'2025-06',churned:false},
            {sign:'2026-02',churned:false}],'2026-01','2026-02');
ck('each exclusion lands in its own bucket',EX.excluded,{noSign:1,preFloor:1,undated:1,negative:1});
ck('  leaving only the clean row plotted',EX.cohorts[1].n,1);
// A survivor has no churn month by definition — that must not read as undated.
ck('a blank churn month on a SURVIVOR is not an exclusion',
   B([{sign:'2026-01',churn:'',churned:false}],'2026-01','2026-02').excluded.undated,0);
ck('  and it stays retained',
   B([{sign:'2026-01',churn:'',churned:false}],'2026-01','2026-02').cohorts[0].cells.map(c=>c.alive),[1,1]);
// Clamping a negative tenure to zero would book a churn into a cohort it never
// belonged to, so the row is dropped instead.
ck('a negative tenure is excluded rather than clamped to month one',
   B([{sign:'2026-04',churn:'2026-01',churned:true}],'2026-01','2026-06').cohorts[3].n,0);

// ── 5. The blended curve has its own denominator per column ──────────────
const BL=B([{sign:'2026-01',churned:false},{sign:'2026-01',churn:'2026-02',churned:true},
            {sign:'2026-03',churned:false}],'2026-01','2026-03');
ck('Month 1 blends every cohort',BL.blend[0],{k:1,alive:3,n:3,cohorts:2,pct:1});
ck('  and Month 3 only the cohort old enough to have one',BL.blend[2],{k:3,alive:1,n:2,cohorts:1,pct:0.5});
ck('an empty cohort month never enters the blend denominator',
   B([{sign:'2026-03',churned:false}],'2026-01','2026-03').blend[0].cohorts,1);
// Far right of the grid, only the oldest cohort has got that far — a blend of
// one is not a blend, and printed there it reads as retention RECOVERING.
ck('the blend has a denominator floor',/var RW_BLEND_MIN=\d+,RW_BLEND_MIN_COHORTS=\d+;/.test(bd),true);
// A single LARGE cohort standing alone at the far right clears the brand floor
// but is still not a blend — which is exactly what the Jan-2026 row does at
// Month 10 now that the grid starts there.
ck('  and a cohort-count floor as well',ctx.RW_BLEND_MIN_COHORTS>=2,true);
ck('  which blanks the cell rather than colouring it',
   /var thin=\(b\.pct===null\|\|b\.n<RW_BLEND_MIN\|\|b\.cohorts<RW_BLEND_MIN_COHORTS\);/.test(bd)&&
   /var f=thin\?'':_rwFill\(b\.pct\);/.test(bd)&&
   /\+'">'\+\(thin\?'':_rwCellVal\(b\.alive,b\.n-b\.alive,b\.pct\)\)/.test(bd),true);
// The blend's denominator shrinks column by column, so a COUNT there falls even
// on a book where nothing churned — it would read as catastrophic churn and
// contradict the share the same row showed one toggle earlier.
// The blended row now follows the unit toggle like every other row, through the
// SAME helper — so its sign and its metric cannot drift from the grid above it.
ck('  and the blend reads through the same helper as every cell',
   /\+'">'\+\(thin\?'':_rwCellVal\(b\.alive,b\.n-b\.alive,b\.pct\)\)/.test(bd),true);
ck('  so Brands gives it a count and % gives it a share',
   (()=>{const r=CVAL('ret','n'),p=CVAL('ret','pct'),c=CVAL('churn','n');
     return [r(470,169,470/639),p(470,169,470/639),c(470,169,470/639)];})(),[470,'74%','-169']);
ck('  with the footnote warning what its counts mean',
   /cohorts age out of the window rather than because brands left/.test(bd),true);
ck('  and its tooltip conjugating the cohort count',
   /\(b\.cohorts===1\?' cohort that has':' cohorts that have'\)/.test(bd),true);
ck('  while the tooltip still says what was there',
   /too few to blend, so not shown/.test(bd),true);
ck('  and the footnote states both floors from the constants',
   /fewer than '\+RW_BLEND_MIN\+' brands or fewer than '\+RW_BLEND_MIN_COHORTS/.test(bd),true);
ck('the floor is high enough to cover a lone cohort',ctx.RW_BLEND_MIN>11,true);
// A throw mid-render blanks this whole tab with nothing in the console, so the
// blend row must not index into an array it has not checked.
ck('an inverted window yields no cohorts and no blend rather than a throw',
   (()=>{const r=B([{sign:'2026-01',churned:false}],'2027-01','2026-10');
     return [r.cohorts.length,r.blend.length];})(),[0,0]);
ck('  and the blend row is guarded before it reads blend[0]',
   /if\(_rw\.blend\.length\)\{[\s\S]{0,400}?_rw\.blend\[0\]\.n/.test(bd),true);
// The footnote presents these as holes in the DRAWN grid, so a row that was
// never in scope must not be tallied as one. Counting book-wide overstated the
// undated churns ~2.4x.
ck('a pre-floor row is counted as pre-floor, not as undated',
   B([{sign:'2024-03',churn:'',churned:true},
      {sign:'2024-05',churn:'2024-09',churned:true},
      {sign:'2025-02',churn:'',churned:true}],'2025-01','2025-06').excluded,
   {noSign:0,preFloor:2,undated:1,negative:0});
ck('  and a post-window row is counted in neither',
   B([{sign:'2027-01',churn:'',churned:true}],'2025-01','2025-06').excluded,
   {noSign:0,preFloor:0,undated:0,negative:0});
ck('  while a negative tenure below the floor is pre-floor too',
   B([{sign:'2024-04',churn:'2024-01',churned:true}],'2025-01','2025-06').excluded,
   {noSign:0,preFloor:1,undated:0,negative:0});
ck('the closed-won-with-churn-date tally is scoped to the window as well',
   /r\.pc&&r\.sign&&r\.sign>=RW_FROM&&r\.sign<=_cmKey/.test(bd),true);
// The header comment justifies RW_FROM with two counts; both were written as if
// Month 1 were the first month AFTER signing, and both came out one low.
ck('a 2026-01 floor would observe ten months, not nine',
   B([{sign:'2026-01',churned:false}],'2026-01','2026-10').cohorts[0].cells.length,10);
ck('  and the chosen floor gives a Month-12 cell to eleven cohorts',
   B([],'2025-01','2026-10').cohorts.filter(c=>c.cells.length>=12).length,11);
ck('  which the comment now states',
   /the oldest reaches Month 10/.test(bd)&&/Month 12 for eleven of them/.test(bd),true);
ck('an empty book still builds a full grid of empty cohorts',
   (()=>{const r=B([],'2026-01','2026-03');
     return [r.cohorts.length,r.cohorts.every(c=>c.n===0),r.blend[0].n];})(),[3,true,0]);

// ── 6. The fill ramp ─────────────────────────────────────────────────────
ck('a full cohort is the strongest green',ctx._rwFill(1),ctx.RW_GREEN[4]);
ck('the anchor itself is unfilled',ctx._rwFill(ctx.RW_NEUTRAL),'');
ck('just below the anchor is the palest red',ctx._rwFill(ctx.RW_NEUTRAL-0.001),ctx.RW_RED[0]);
ck('the red arm CLAMPS at its floor rather than running to zero',
   [ctx._rwFill(ctx.RW_LOW),ctx._rwFill(0.2),ctx._rwFill(0)],
   [ctx.RW_RED[4],ctx.RW_RED[4],ctx.RW_RED[4]]);
ck('an absent share takes no fill',[ctx._rwFill(null),ctx._rwFill(undefined)],['','']);
ck('no share ever lands outside the ramp',
   Array.from({length:1001},(_,i)=>ctx._rwFill(i/1000))
     .filter(f=>f!==''&&ctx.RW_GREEN.indexOf(f)<0&&ctx.RW_RED.indexOf(f)<0).length,0);
ck('every step of the ramp is reachable',
   new Set(Array.from({length:1001},(_,i)=>ctx._rwFill(i/1000))).size,11);
ck('the ramp is monotone — a better share is never a redder cell',
   (()=>{const ord=ctx.RW_RED.slice().reverse().concat(['']).concat(ctx.RW_GREEN);
     let last=-1,ok=true;
     for(let i=0;i<=1000;i++){const r=ord.indexOf(ctx._rwFill(i/1000));if(r<last)ok=false;last=r;}
     return ok;})(),true);

// ── 7. Placement and wiring ──────────────────────────────────────────────
ck('the section is numbered C3',/<span class="bd-sec-n">C3<\/span><span class="bd-sec-t">Retention Waterfall<\/span>/.test(bd),true);
ck('  and sits between the churn rate and the type breakdown',
   bd.indexOf('id="bd-sec-churnrate"')<bd.indexOf('id="bd-sec-churnret"')&&
   bd.indexOf('id="bd-sec-churnret"')<bd.indexOf('id="bd-sec-churntype"'),true);
ck('  with its nav entry in the same position',
   (()=>{const n=[...bd.matchAll(/data-sec="(bd-sec-churn[a-z]*)"/g)].map(m=>m[1]);
     return n.indexOf('bd-sec-churnret')===n.indexOf('bd-sec-churnrate')+1
         && n.indexOf('bd-sec-churnret')===n.indexOf('bd-sec-churntype')-1;})(),true);
// ── 7a. Churn-attribute filters ──────────────────────────────────────────
// Every one of these is written at the churn review, so a survivor can never
// carry one. A selection therefore narrows the NUMERATOR only.
const FCTX=(()=>{const c={console};vm.createContext(c);
  vm.runInContext([
    "var BD_CHURN_BOARD='Board Defined';",
    decl(bd,'var BD_CHURN_TYPE_GROUP={','};'),
    decl(bd,'var BD_SEGMENT_ORDER=','];'),
    decl(bd,'var BD_PROD_GROUP={','};'),
    decl(bd,'var BD_PROD_ORDER=','];'),
    fn(bd,'_chProdsOf'), fn(bd,'_rwTypeOf'),
    decl(bd,"var RW_PROD_NONE='","';"),
    "var _bdRetF={type:'all',seg:'all',wt:'all',ab:'all',prod:[]};",
    decl(bd,'var _rwFilters=[','  ];'),
    fn(bd,'_rwProdMatch'), fn(bd,'_rwKept'),
    'Object.assign(globalThis,{_rwTypeOf,_chProdsOf,_rwKept,_rwProdMatch,_bdRetF,_rwFilters,BD_PROD_ORDER,RW_PROD_NONE});',
  ].join('\n'),c); return c;})();
ck('the filter predicates loaded',
   [typeof FCTX._rwKept,FCTX._rwFilters.length],['function',3]);

// Churn Type: Board Defined folds in Delinquent Payment, as everywhere else here.
ck('every stored churn type maps to a group',
   ['Board Defined Churn','Delinquent Payment','Yes - Rejected Product(s)','Yes - Ghosted',
    'Yes - Other Reason',''].map(v=>FCTX._rwTypeOf({never_implemented_churn:v})),
   ['board','board','ni','ni','ni','']);
ck('  and an unrecognised value falls to not-recorded rather than a group',
   FCTX._rwTypeOf({never_implemented_churn:'Something New'}),'');
// _chTypeOf lives in section iv, BELOW this one, so calling it here would be a
// hoisted-but-undefined blank tab — the hazard this file is littered with.
ck('the section does not call the churn-type helper declared below it',
   /_chTypeOf/.test(SEC.replace(/^\s*\/\/.*$/gm,'')),false);

// Every filter offers "not recorded" as a real choice, because blank is the
// largest bucket in four of the five fields.
ck('each filter carries an explicit not-recorded option',
   FCTX._rwFilters.map(f=>f.opts.some(o=>o[0]===''&&/Not recorded/.test(o[1]))),
   [true,true,true]);
ck('  and the filter keys are the ones the state object holds',
   FCTX._rwFilters.map(f=>f.k),['type','seg','ab']);
ck('  with the removed Web Traffic filter gone from state and rows alike',
   /_bdRetF\.wt|monthly_web_traffic__reported_at_time_of_churn|RW_WT_OPTS/.test(SEC),false);
// Verified against the live COMPANY enum on 2026-10-21: these four and nothing
// else, so the list is complete rather than merely plausible.
ck('the segment options are the whole company_segment enum',
   FCTX._rwFilters.find(f=>f.k==='seg').opts.map(o=>o[0]),
   ['VIP','Strategic','Enterprise','Mid-Market','']);

// Products multi-select matches ANY, and is immune to HubSpot's unstable
// ordering of multi-select values because it compares a SET, never a string.
const KEEP=(f,dd)=>{Object.assign(FCTX._bdRetF,{type:'all',seg:'all',wt:'all',ab:'all',prod:[]},f);
  return FCTX._rwKept(dd);};
ck('Badge and Banner group to one product',
   [FCTX._chProdsOf({implemented_products:'Badge;Banner;Reviews'}),
    FCTX._chProdsOf({implemented_products:'Banner;Badge;Reviews'})],
   [['Badge/Banner','Reviews'],['Badge/Banner','Reviews']]);
ck('  so a reordered multi-select still matches',
   [KEEP({prod:['Badge/Banner']},{implemented_products:'Banner;Badge;Reviews'}),
    KEEP({prod:['Reviews']},{implemented_products:'Banner;Badge;Reviews'})],[true,true]);
ck('  an empty product selection filters nothing',
   KEEP({},{implemented_products:''}),true);
// INTERSECTION, not union: two selected products means the brand implemented
// BOTH. Adding a product can only ever narrow the set.
ck('  a multi-selection is the intersection of the choices',
   [KEEP({prod:['Badge/Banner','CAI']},{implemented_products:'Badge;CAI'}),
    KEEP({prod:['Badge/Banner','CAI']},{implemented_products:'Badge;Reviews'}),
    KEEP({prod:['CAI','CAS']},{implemented_products:'Badge;CAI'})],
   [true,false,false]);
ck('  so adding a product never widens the set',
   (()=>{const dd={implemented_products:'Badge;Banner;Reviews'};
     return [KEEP({prod:['Badge/Banner']},dd),
             KEEP({prod:['Badge/Banner','Reviews']},dd),
             KEEP({prod:['Badge/Banner','Reviews','CAI']},dd)];})(),
   [true,true,false]);
// "Not recorded" is its own question — a brand either implemented something or
// did not — so it cannot intersect with a product. The two sides clear each
// other in the toggle rather than letting the grid reach a guaranteed-empty state.
ck('the not-recorded option selects brands with no products',
   [KEEP({prod:[FCTX.RW_PROD_NONE]},{implemented_products:''}),
    KEEP({prod:[FCTX.RW_PROD_NONE]},{implemented_products:'Badge'})],[true,false]);
ck('  and it wins over any product left alongside it',
   KEEP({prod:[FCTX.RW_PROD_NONE,'Badge/Banner']},{implemented_products:''}),true);
ck('  so survivors, whose field is blank, pass it',
   KEEP({prod:[FCTX.RW_PROD_NONE]},{}),true);
ck('  which no real product selection does',
   KEEP({prod:['Badge/Banner']},{}),false);
// Exercised, not just regex-matched: the toggle is module-scope and calls
// renderBD, so it slices out cleanly with that stubbed. A browser click-through
// is not in the suite, and this is the behaviour that browser check verified.
const TOG=(()=>{const c={console};vm.createContext(c);
  vm.runInContext([
    decl(bd,"var RW_PROD_NONE='","';"),
    "var _bdRetF={type:'all',seg:'all',ab:'all',prod:[]};",
    'var renders=0; function renderBD(){renders++;}',
    fn(bd,'bdToggleRetFProd'), fn(bd,'bdClearRetFProd'), fn(bd,'_bdRetFActive'),
    'Object.assign(globalThis,{bdToggleRetFProd,bdClearRetFProd,_bdRetFActive,_bdRetF});',
  ].join('\n'),c); return c;})();
const seq=(...clicks)=>{TOG._bdRetF.prod.length=0;
  clicks.forEach(v=>v===null?TOG.bdClearRetFProd():TOG.bdToggleRetFProd(v));
  return TOG._bdRetF.prod.slice();};
const NONE=TOG.RW_PROD_NONE||'__none';
ck('clicking a product selects it',seq('Badge/Banner'),['Badge/Banner']);
ck('  clicking it again de-selects it',seq('Badge/Banner','Badge/Banner'),[]);
ck('  two products accumulate',seq('Badge/Banner','Reviews'),['Badge/Banner','Reviews']);
ck('not-recorded clears any products already chosen',
   seq('Badge/Banner','Reviews',NONE),[NONE]);
ck('  and choosing a product afterwards clears not-recorded',
   seq(NONE,'Reviews'),['Reviews']);
ck('  neither can ever sit beside the other, whatever the order',
   [seq(NONE,'CAI','Reviews'),seq('CAI',NONE,'Reviews'),seq('CAI','Reviews',NONE,'CAS')],
   [['CAI','Reviews'],['Reviews'],['CAS']]);
ck('  de-selecting not-recorded empties the selection',seq(NONE,NONE),[]);
ck('Clear empties whatever was chosen',
   [seq('Badge/Banner','Reviews',null),seq(NONE,null)],[[],[]]);
ck('an empty product selection is not an active filter',
   (()=>{seq('Badge/Banner',null);return TOG._bdRetFActive();})(),false);
ck('  while any product selection is',
   (()=>{seq('Badge/Banner');return TOG._bdRetFActive();})(),true);
// The confirmed review finding, pinned as behaviour: a survivor is blank on the
// churn-review fields, so their Not-recorded option keeps every one.
ck('a Not-recorded churn-review option keeps survivors',
   [KEEP({type:''},{companySegment:'Mid-Market'}),
    KEEP({ab:''},{companySegment:'Mid-Market'}),
    KEEP({prod:[NONE]},{companySegment:'Mid-Market'})],[true,true,true]);
ck('  where a NAMED value on the same field keeps none of them',
   [KEEP({type:'board'},{companySegment:'Mid-Market'}),
    KEEP({ab:'Yes'},{companySegment:'Mid-Market'}),
    KEEP({prod:['Badge/Banner']},{companySegment:'Mid-Market'})],[false,false,false]);
ck('the toggle keeps not-recorded and products mutually exclusive',
   /else if\(v===RW_PROD_NONE\)\{_bdRetF\.prod=\[v\];\}/.test(bd)
   &&/_bdRetF\.prod\.filter\(function\(x\)\{return x!==RW_PROD_NONE;\}\)\.concat\(\[v\]\)/.test(bd),true);
ck('  and the sentinel cannot collide with a real product name',
   FCTX.BD_PROD_ORDER.indexOf(FCTX.RW_PROD_NONE),-1);
ck('the not-recorded chip is rendered, with its count',
   /fpMulti\(_bdRetF\.prod\.indexOf\(RW_PROD_NONE\)>=0,'Not recorded'\+_rwCnt\(_noProd,_noProdChn\)/.test(SEC),true);
ck('  and its tooltip says a blank is an unfilled review, not an empty brand',
   /a review that never recorded any, not a brand that implemented nothing/.test(SEC),true);
ck('  and every selected product must be present, in any stored order',
   [KEEP({prod:['Badge/Banner','Reviews']},{implemented_products:'Reviews;Banner'}),
    KEEP({prod:['Badge/Banner','Reviews']},{implemented_products:'Banner'})],[true,false]);
ck('  and a deal with none recorded fails any product selection',
   KEEP({prod:['Badge/Banner']},{implemented_products:''}),false);

// Single-selects, including blank-as-a-value.
ck('a churn-type selection keeps only that group',
   [KEEP({type:'board'},{never_implemented_churn:'Delinquent Payment'}),
    KEEP({type:'board'},{never_implemented_churn:'Yes - Ghosted'}),
    KEEP({type:'ni'},{never_implemented_churn:'Yes - Ghosted'}),
    KEEP({type:''},{never_implemented_churn:''}),
    KEEP({type:''},{never_implemented_churn:'Board Defined Churn'})],
   [true,false,true,true,false]);
ck('the filters compose — all must match',
   [KEEP({type:'board',ab:'Yes'},{never_implemented_churn:'Board Defined Churn',ab_test:'Yes'}),
    KEEP({type:'board',ab:'Yes'},{never_implemented_churn:'Board Defined Churn',ab_test:'No'})],
   [true,false]);
// "(total [churned])": the brands an option keeps, and how many of them left.
ck('option counts carry both the total and the churned subset',
   /_rwCnt=function\(tot,chn\)/.test(SEC)&&/\('\+tot\+' \['\+chn\+'\]\)/.test(SEC),true);
ck('  set smaller than the label they trail',
   (()=>{const m=/_rwCnt=function\(tot,chn\)\{[\s\S]*?font-size:(\d+(?:\.\d+)?)px/.exec(SEC);
     return !!m&&parseFloat(m[1])<11;})(),true);
ck('  taken over BOTH populations, not churns alone',
   /surv\.forEach\(function\(dd\)\{var v=f\.of\(dd\);cnt\[v\]=/.test(SEC)
   &&/churns\.forEach\(function\(dd\)\{var v=f\.of\(dd\);cnt\[v\]=\(cnt\[v\]\|\|0\)\+1;chn\[v\]=/.test(SEC),true);
ck('  including the product chips',
   /fpMulti\(_bdRetF\.prod\.indexOf\(pz\)>=0,pz\+_rwCnt\(pc\[pz\]\|\|0,pchn\[pz\]\|\|0\)/.test(SEC),true);
ck('and All carries no count',
   /fp\(_bdRetF\[f\.k\]==='all','All',/.test(SEC)&&!/'All \('\+/.test(SEC),true);
ck('an A/B value of Unknown is distinct from blank',
   [KEEP({ab:'Unknown'},{ab_test:'Unknown'}),KEEP({ab:'Unknown'},{ab_test:''}),
    KEEP({ab:''},{ab_test:''}),KEEP({ab:''},{ab_test:'Unknown'})],
   [true,false,true,false]);

// THE semantic: a filtered-out churn stays in its cohort as RETAINED. Dropping
// it would shrink the denominator too and answer a different question.
// THE semantic: a filtered-out churn leaves the cohort ENTIRELY, numerator and
// denominator together, so the Signed column moves with the filters. Keeping it
// as retained would claim a brand that demonstrably left as one that stayed.
// THE semantic: the cohort IS the filtered set. Both populations go through
// the SAME predicate, so Signed counts only the brands on screen and nothing
// is recategorised as retained to keep a denominator intact.
ck('both populations go through the same predicate',
   /_chDeals\.filter\(_rwKept\)\.map\(function\(dd\)\{/.test(SEC)
   &&/var _rwBook=closedWonDeals\.filter\(_rwKept\)\.map\(/.test(SEC),true);
ck('  with no retained-instead fallback left behind',
   /filteredOut:true/.test(SEC),false);
ck('  so the cohort size falls with the filter',
   (()=>{const all=[{sign:'2026-01',churned:false},{sign:'2026-01',churned:false},
                    {sign:'2026-01',churn:'2026-03',churned:true},
                    {sign:'2026-01',churn:'2026-04',churned:true}];
     const kept=all.slice(0,3);          // one churn filtered out
     return [B(all,'2026-01','2026-06').cohorts[0].n,
             B(kept,'2026-01','2026-06').cohorts[0].n];})(),[4,3]);
// Four of the five fields are written at the churn review, so a selection on
// them leaves NO survivors and the grid becomes a time-to-churn view. That is a
// real reading, but it is not retention — the band has to say which is on screen
// rather than let a reader assume the first, and it decides from the DATA.
ck('  the band counts survivors and churns separately',
   /_rwKeptSurv\+' still live, '\+_rwKeptChurns\+' churned/.test(SEC),true);
ck('  and names the time-to-churn case when no survivor is left',
   /_rwKeptSurv===0/.test(SEC)
   &&/Every brand here has churned/.test(SEC)&&/each curve ends at 0%/.test(SEC),true);
// The survivors-remain branch must not credit ONE field for it: the
// Not-recorded product option keeps survivors too, since their
// implemented_products is blank, so naming only Company Segment reads as false
// whenever that is the filter in play.
// A survivor is blank on EVERY churn-review field, so any "Not recorded" option
// keeps them — not just the products one. The branch must not make an
// exhaustive claim it cannot keep; naming the two mechanisms is what is true.
ck('  the survivors-remain branch names the mechanism, not a closed list',
   /Survivors are in this cut/.test(SEC)
   &&/They carry a Company Segment/.test(SEC)
   &&/blank on every churn-review field/.test(SEC)
   &&/a Not-recorded option keeps them as well/.test(SEC),true);
ck('  making no "only these two" claim it cannot keep',
   /only Company Segment/.test(SEC),false);
// Removing the Web Traffic filter left these counts stale.
ck('  and the field tallies match the four filters that remain',
   /Three of these four fields are written at the churn review/.test(SEC)
   &&!/Four of these five fields/.test(SEC)&&!/the other four are/.test(SEC),true);
ck('  with both survivor tallies taken over the plotted window',
   /_rwWinSurvArr=closedWonDeals\.filter\(function\(dd\)\{/.test(SEC)
   &&/sg&&sg>=RW_FROM&&sg<=_cmKey;\}\);/.test(SEC)
   &&/_rwKeptSurv=_rwWinSurvArr\.filter\(_rwKept\)\.length;/.test(SEC),true);
ck('  and the band says so where the filters are, not only in the footnote',
   /leave their cohorts entirely/.test(SEC)
   &&/column counts only the brands shown/.test(SEC),true);
ck('  naming what the filtered grid actually reads',
   /this is a time-to-churn view/.test(SEC)
   ||/stays a retention read/.test(SEC),true);
ck('  shown only while a filter is actually on',
   /if\(_bdRetFActive\(\)\)/.test(SEC),true);
ck('  reporting how many brands survive the filter, both populations',
   /\(_rwKeptSurv\+_rwKeptChurns\)\+' of '\+\(_rwWinSurv\+_rwWinChurns\.length\)/.test(SEC),true);
// The option counts must be taken over the SAME window the grid plots, or an
// option can claim churns the grid never had.
ck('option counts come from the plotted window, on the grid\u2019s own rules',
   /_rwWinChurns=_chDeals\.filter/.test(SEC)&&/sg>=RW_FROM&&sg<=_cmKey&&_rwDiff\(sg,cm\)>=0/.test(SEC),true);
ck('every option prints its counts, through the one helper',
   /o\[1\]\+_rwCnt\(cnt\[o\[0\]\]\|\|0,chn\[o\[0\]\]\|\|0\)/.test(SEC),true);
// One filter per row, under the toggles.
ck('the filters render below the toggles, one per row',
   (()=>{const leg=SEC.indexOf("_chGrainPill(_bdRetMetric,'ret'");
     const rows=SEC.indexOf('hChurn+=_rwFilterRows(_rwWinSurvArr,_rwWinChurns);');
     const grid=SEC.indexOf('var W=_rw.maxK;');
     return leg>0&&rows>leg&&grid>rows;})(),true);
ck('the products row is labelled as an intersection, not a union',
   /font-style:italic">all of<\/span>/.test(SEC)&&!/">any of<\/span>/.test(SEC),true);
ck('  four of them: three single-selects from the loop plus products',
   FCTX._rwFilters.length+1,4);
ck('  each row labelled on the same fixed gutter',
   (()=>{const m=SEC.match(/flex:0 0 118px/g)||[];
     return m.length===2&&/_rwFilters\.forEach\(function\(f\)\{/.test(SEC);})(),true);

// ── 7b. The Retention / Churn metric toggle ──────────────────────────────
ck('the metric toggle exists and defaults to Retention',
   /var _bdRetMetric='ret';/.test(bd)&&
   /function bdSetRetMetric\(m\)\{if\(_bdRetMetric===m\)return;_bdRetMetric=m;renderBD\(\);\}/.test(bd),true);
ck('  with both pills wired to it',
   (bd.match(/_chGrainPill\(_bdRetMetric,'(ret|churn)'/g)||[]).length,2);
// Top LEFT means first inside the tile's own row, which is a different place
// from the %/Brands group — that one is right-aligned in the section HEADER and
// so comes earlier in the source. Assert the visual contract, not file order.
ck('  sitting at the top left of the tile, ahead of the scale it relabels',
   (()=>{const tile=SEC.indexOf('hChurn+=\'<div class="tile"');
     const i=SEC.indexOf("_chGrainPill(_bdRetMetric,'ret'",tile);
     const j=SEC.indexOf("(_rwChurnView?'Churned':'Retained')",tile);
     const ramp=SEC.indexOf('RW_RED.slice().reverse()',tile);
     return tile>0&&i>tile&&j>i&&ramp>j;})(),true);
// Both toggles now sit together in the tile, unit first — the unit decides
// whether the metric reads as a share or a headcount.
ck('  with the %/Brands group immediately to its left, in the same row',
   (()=>{const tile=SEC.indexOf('hChurn+=\'<div class="tile"');
     const unit=SEC.indexOf("_chGrainPill(_bdRetMode,'pct'",tile);
     const metric=SEC.indexOf("_chGrainPill(_bdRetMetric,'ret'",tile);
     return unit>tile&&metric>unit;})(),true);
ck('  and nothing left behind in the section header',
   (()=>{const tile=SEC.indexOf('hChurn+=\'<div class="tile"');
     return SEC.indexOf("_chGrainPill",0)>tile;})(),true);
// Churn is the exact complement, so INVERTING THE RAMP TOO would cancel out and
// paint high churn green. The fill must stay keyed to retention.
ck('  and the fill is never inverted with it',
   /var f=_rwFill\(c\.pct\);/.test(SEC)&&!/_rwFill\([^)]*_rwChurnView/.test(SEC),true);
// Exercise the real helper under both metrics and both units.
ck('retention % prints the share retained',CVAL('ret','pct')(23,4,23/27),'85%');
ck('churn % prints its exact complement, as a loss',CVAL('churn','pct')(23,4,23/27),'-15%');
ck('  with the two still describing the same cell',
   [0,0.25,0.5,0.731,0.92,1].map(p=>parseInt(CVAL('ret','pct')(0,0,p))-parseInt(CVAL('churn','pct')(0,0,p))),
   [100,100,100,100,100,100]);
ck('retention Brands prints the survivors',CVAL('ret','n')(23,4,23/27),23);
ck('churn Brands prints the ones that left, as a loss',CVAL('churn','n')(23,4,23/27),'-4');
// '-0%' is not a number anyone writes, and a cohort that has lost nobody must
// not look like it lost someone. The sign is decided on the FORMATTED string,
// so a churn of 0.4% — which rounds to '0%' — is unsigned too.
ck('a cohort with no churn reads 0%, never -0%',CVAL('churn','pct')(27,0,1),'0%');
ck('  and a churn too small to print is unsigned as well',CVAL('churn','pct')(27,0,0.996),'0%');
ck('  as is a zero count',CVAL('churn','n')(27,0,1),'0');
ck('retention is never signed',
   [0,0.5,1].map(p=>CVAL('ret','pct')(0,0,p)).concat(CVAL('ret','n')(23,4,0.85)),['0%','50%','100%',23]);
ck('every churn share below zero carries the minus',
   [0.1,0.4,0.73,0.92,0.994].every(p=>/^-\d+%$/.test(CVAL('churn','pct')(0,0,p))),true);
// The legend's ramp is unchanged; only the numbers labelling its ends flip.
ck('the legend relabels its ends for churn',
   /\(_rwChurnView\?'Churned':'Retained'\)/.test(SEC)&&
   /_rwChurnView\?_rwSigned\(1-RW_LOW,true\):_rwPct\(RW_LOW\)/.test(SEC)&&
   /\(_rwChurnView\?'0%':'100%'\)/.test(SEC),true);
// Once churn carries a minus the SIGN carries the direction, so the comparator
// stops flipping: the bad end is '<= -45%', not '>= 45%'.
ck('  with one comparator for both metrics, since the sign gives the direction',
   /\+'\u2264'\+\(_rwChurnView\?_rwSigned/.test(SEC)&&!/\u2265'\+_rwPct/.test(SEC),true);
ck('  including the neutral anchor',
   /\(_rwChurnView\?_rwSigned\(1-RW_NEUTRAL,true\):_rwPct\(RW_NEUTRAL\)\)/.test(SEC),true);
ck('the subtitle follows the metric',
   /_rwChurnView\?'that had LEFT by':'still live in'/.test(SEC)&&
   /_rwChurnView\?'rise':'decline'/.test(SEC),true);
ck('the tooltip leads with whichever metric is on screen',
   /_rwChurnView\s*\?\s*h\+c\.lost\+' of '\+n\+' churned by then/.test(SEC),true);
ck('  and still carries both figures either way',
   /churned by then \('\+_rwSigned\(1-c\.pct,true\)\+'\), '\+c\.alive\+' still live/.test(SEC),true);
// A cell reading -8% whose hover reads 8% looks like a bug; both go through the
// same formatter so they cannot drift apart.
ck('  quoting the same signed figure the cell prints',
   (()=>{const c=CV('churn','pct');
     return c._rwCellVal(46,4,0.92)===c._rwSigned(1-0.92,true);})(),true);

ck('the cell-mode toggle exists and defaults to the share',
   /var _bdRetMode='pct';/.test(bd)&&/function bdSetRetMode\(m\)\{if\(_bdRetMode===m\)return;_bdRetMode=m;renderBD\(\);\}/.test(bd),true);
ck('  and both pills are wired to it',
   (bd.match(/_chGrainPill\(_bdRetMode,'(pct|n)'/g)||[]).length,2);

// ── 8. The floor is its OWN constant ─────────────────────────────────────
// Reusing BD_CHURN_DETAIL_FROM would silently re-floor this section at 2026-01
// the next time that constant moves, and leave the Month-12 column empty for
// every row in the chart.
// RW_FROM and BD_CHURN_DETAIL_FROM currently hold the SAME value. The invariant
// is not that they differ — it is that they are separate constants, so moving
// the churn-detail floor cannot silently move this grid.
ck('the waterfall floor is its own constant',/var RW_FROM='\d{4}-\d{2}';/.test(bd),true);
ck('  declared apart from the churn-detail floor',
   /var BD_CHURN_DETAIL_FROM='\d{4}-\d{2}';/.test(bd)&&!/RW_FROM=BD_CHURN_DETAIL_FROM/.test(bd),true);
ck('  and the floor is the month the user asked for',ctx.RW_FROM,'2026-01');
// Comments stripped first: the section's header comment names the constant in
// order to explain why it is NOT used, and that sentence must not read as use.
ck('  and the section never reads the detail floor as CODE',
   (()=>{const i=bd.indexOf('// ── iii. Retention waterfall');
     const body=SEC.replace(/^\s*\/\/.*$/gm,'');
     return /BD_CHURN_DETAIL_FROM/.test(body);})(),false);
ck('  while the comment still explains the independence',
   /NOT BD_CHURN_DETAIL_FROM even though/.test(bd),true);

// ── 9. What the footnote has to say out loud ─────────────────────────────
ck('it refuses to be read as the churn rate C2 plots',
   /This is not the churn rate C2 plots/.test(SEC),true);
ck('  and says why the two cannot tie out',
   /on different clocks and will not tie out/.test(SEC),true);
ck('undated churns are named in the footnote, not dropped quietly',
   /_rw\.excluded\.undated\?/.test(SEC)&&/no Pause\/Churn Date/.test(SEC),true);
ck('  as are the negatives',/_rw\.excluded\.negative\?/.test(SEC),true);
ck('  and the closed-won deals carrying a churn date',/_rwCwPc\?/.test(SEC),true);
ck('the signing-date choice is stated where a reader can see it',
   /earlier of Close Date and Entered Closed-Won/.test(SEC),true);
ck('the unit is admitted to be the deal, not the brand',
   /re-signed belongs to both its cohorts/.test(SEC),true);
ck('the scale prints its own anchor',/neutral at/.test(SEC),true);
ck('the subtitle does not contradict the Month-1 convention',
   /Month 1 being the signing month itself/.test(SEC)&&!/still live N months on/.test(SEC),true);
// Comments may record a measurement; RENDERED copy may not, because nothing
// recomputes it and no test pins it. ("a third" was already wrong — it is 29%.)
ck('no one-off measurement is frozen into the rendered prose',
   /\d+ deals into a phantom/.test(SEC.replace(/^\s*\/\/.*$/gm,'')),false);
ck('  the softened wording is what ships',
   /well over a hundred deals/.test(SEC)&&/nearly a third of its churns/.test(SEC),true);
ck('the dangling "more" is conditional on the fragment before it',
   /_rw\.excluded\.negative\+\(_rw\.excluded\.undated\?' more':''\)/.test(SEC),true);
ck('the unit names the record, not the table row',
   /The unit counted is the signed deal, not the brand/.test(SEC)&&!/One row is one signed deal/.test(SEC),true);
// Every conditional fragment in the footnote is built by concatenating ternaries,
// so each one has to read grammatically at n=1 as well as n>1.
ck('the undated fragment agrees in number',
   /' churned brand'\+\(_rw\.excluded\.undated===1\?'':'s'\)[\s\S]{0,80}?\(_rw\.excluded\.undated===1\?'is':'are'\)/.test(SEC),true);
ck('  so does the negative-tenure one',
   /\(_rw\.excluded\.negative===1\?'its':'their'\)[\s\S]{0,80}?\(_rw\.excluded\.negative===1\?'is':'are'\)/.test(SEC),true);
ck('  and the no-date one carries a noun and the right verb',
   /' signed deal'\+\(_rw\.excluded\.noSign===1\?' carries':'s carry'\)/.test(SEC),true);
ck('  and the closed-won one conjugates its verb, not just the noun',
   /' closed-won deal'\+\(_rwCwPc===1\?' carries':'s carry'\)/.test(SEC),true);
ck('no fragment leaves a bare count followed by a plural verb',
   /\+' carry /.test(SEC),false);

// ── 10. Unobservable cells are blank, never zero ─────────────────────────
ck('a cell past the cohort’s age renders empty',
   /if\(!c\|\|!co\.n\)\{t\+='<td><\/td>';continue;\}/.test(SEC),true);

// ── 11. The table inherits nothing from .stbl ────────────────────────────
// .stbl's sticky header rules are what blanked the C7/C8 top rows; a triangle
// needs a sticky LEFT edge instead, so it gets its own class.
ck('the grid uses its own table class',/<table class="rwtbl"[ >]/.test(SEC),true);
ck('  with the cohort month and its size named in the header',
   /<th class="rw-c" style="text-align:left">Cohort<\/th>/.test(SEC)&&/>Signed<\/th>/.test(SEC),true);
ck('  and the size column saying it is the row\u2019s denominator',
   /the denominator for every cell in the row/.test(SEC),true);
ck('  which is never combined with stbl',/class="stbl[^"]*rwtbl|class="rwtbl[^"]*stbl/.test(bd),false);
ck('  and the cohort label column is the sticky one',
   /\.rwtbl \.rw-c\{position:sticky;left:0;/.test(bd),true);
// The sticky offset lives in CSS and the column width in JS. If they drift, the
// opaque Signed cell lands on top of the Month-1 digits — "100%" renders "00%".
ck('  with the count column pinned to exactly the label width',
   (()=>{const m=/\.rwtbl \.rw-n\{position:sticky;left:(\d+)px;/.exec(bd);
     return m&&+m[1]===ctx.RW_W_LBL;})(),true);
// A table-layout:fixed table with width:auto still SHRINK-TO-FITS: without a
// min-width the <col> widths compress, overflow-x never scrolls, and column 1
// slides out from under the cell pinned to its nominal width.
ck('  and the grid carries a min-width so it overflows instead of compressing',
   /<table class="rwtbl" style="width:100%;min-width:'\+_rwW\+'px">/.test(bd),true);
ck('  computed from the label widths plus a floor per tenure column',
   /var _rwW=RW_W_LBL\+RW_W_N\+RW_W_CELL\*W;/.test(bd)&&
   /<col style="width:'\+RW_W_LBL\+'px"><col style="width:'\+RW_W_N\+'px">/.test(bd),true);
// The tenure columns are deliberately UNSIZED so table-layout:fixed splits the
// slack between them; sizing them would freeze the grid at its minimum and
// leave the void beside it that this replaced.
ck('  while the tenure columns are left unsized so they stretch',
   /join\('<col>'\)/.test(bd)&&!/join\('<col style="width:/.test(bd),true);
ck('  with a cap so they do not stretch to absurdity',
   ctx.RW_W_CELL_MAX>ctx.RW_W_CELL&&ctx.RW_W_CELL_MAX<=140,true);
// The grid fills the section; only the prose is capped. A grid at 2000px is
// fine, a 2000px line of 10px italic text is not.
ck('  and the grid is no longer capped, so it fills the section',
   /max-width:'\+\(RW_W_LBL\+RW_W_N\+RW_W_CELL_MAX/.test(SEC),false);
ck('  while the footnote keeps a readable measure of its own',
   /font-style:italic;line-height:1\.6;max-width:\d+px/.test(SEC),true);
ck('  and the table itself is full-width',/\.rwtbl\{[^}]*width:100%/.test(bd),true);
ck('  leaving no hard-coded pixel widths in the colgroup',
   /<col style="width:(78|58|42)px">/.test(bd),false);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
