// Non-Paid · Last Click on the Brand Trend Analysis: the organic half of
// whichever cohort is selected. Over REGULAR demos it is the Key Metrics tile
// field for field; over PRE-LAUNCH it is the same split of that cohort, which
// carries its own prelaunchPaid count. Over PAID demos every contact is paid by
// construction, so the answer is a tautological 0% and the measure is hidden.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console};vm.createContext(ctx);
vm.runInContext([
  'var _dshTrendPopV=null, _dshTrendMetricV=null;',
  'var window=globalThis;',
  decl(cli,'const DSH_TR_POPS=[','];'),
  decl(cli,'const DSH_TR_METRICS=[','];'),
  decl(cli,'const DSH_TR_CARDS=[','];'),
  fn(cli,'_dshTrendPop'), fn(cli,'_dshTrendMetric'),
  fn(cli,'_dshTrMetricDef'), fn(cli,'_dshTrHidden'), fn(cli,'_dshTrShown'),
  fn(cli,'_dshTrParts'), fn(cli,'_dshTrSum'), fn(cli,'_dshTrValue'),
  'Object.assign(globalThis,{DSH_TR_METRICS,DSH_TR_CARDS,_dshTrMetricDef,_dshTrHidden,'
   +'_dshTrShown,_dshTrParts,_dshTrSum,_dshTrValue,setPop(p){_dshTrendPopV=p;},'
   +'setMetric(m){_dshTrendMetricV=m;}});',
].join('\n'),ctx);
ck('the helpers loaded',typeof ctx._dshTrValue,'function');

// ── 1. Position and shape ─────────────────────────────────────────────────
ck('the measure sits directly after Cost Per',
   ctx.DSH_TR_METRICS.map(m=>m.k),['count','cost','nonpaid','cvr','ctr']);
ck('  and so does its card',ctx.DSH_TR_CARDS.map(c=>c.k),['count','cost','nonpaid','cvr','ctr','spend']);
const NP=ctx.DSH_TR_METRICS.find(m=>m.k==='nonpaid');
ck('it is a percentage',NP.unit,'%');
ck('it is named as the Key Metrics tile names it',NP.l,'Non-Paid · Last Click');
// It differs per cohort now, so the card must say WHICH cohort it describes.
ck('it varies with the population',NP.pop,true);
ck('  and is hidden only where it is meaningless',NP.hiddenOn,['paid']);
ck('its colour is not already taken',
   ctx.DSH_TR_METRICS.filter(m=>m.c===NP.c).length,1);
// The CARD carries its own copies of both flags, and they drive the label
// suffix and the stat row independently of the measure's. Out of step, the card
// reads "Non-Paid · Last Click" over pre-launch demos with nothing saying so.
const NPC=ctx.DSH_TR_CARDS.find(c=>c.k==='nonpaid');
ck('the card varies with the population too',NPC.pop,true);
ck('  and is hidden on the same population',NPC.hiddenOn,['paid']);
ck('  so card and measure agree',[NPC.pop,NPC.hiddenOn],[NP.pop,NP.hiddenOn]);

// ── 2. The value, over any cohort ─────────────────────────────────────────
const V=(cohort,cohortPaid)=>ctx._dshTrValue('nonpaid',{cohort,cohortPaid,spend:0,clicks:0,impressions:0,pop:cohort});
ck('it is the organic share of the cohort',V(100,30),70);
ck('  all organic reads 100%',V(40,0),100);
ck('  all paid reads 0%',V(40,40),0);
// A period with no demos has no share, not a zero one — a real 0 plots on the
// floor and reads as "everything was paid".
ck('an empty cohort yields no value, not zero',V(0,0),null);
ck('  and a negative can never be produced',V(10,10),0);

// ── 3. The cohort is resolved from the population ─────────────────────────
// Chosen so the two cohorts give DIFFERENT shares (75% vs 50%): with equal
// shares the assertion below would pass even if prelaunch wrongly resolved
// to the regular cohort.
const P={spend:1000,clicks:500,impressions:20000,regular:40,paid:10,prelaunch:80,prelaunchPaid:40};
const parts=p=>ctx._dshTrParts(P,p);
ck('Regular reads the regular cohort and its paid split',
   [parts('regular').cohort,parts('regular').cohortPaid],[40,10]);
ck('Pre-Launch reads its OWN cohort and paid split',
   [parts('prelaunch').cohort,parts('prelaunch').cohortPaid],[80,40]);
ck('  so the two cohorts give DIFFERENT answers',
   [ctx._dshTrValue('nonpaid',parts('regular')),ctx._dshTrValue('nonpaid',parts('prelaunch'))],[75,50]);
ck('an unknown population falls back to regular',
   [parts('nonsense').cohort,parts('nonsense').cohortPaid],[40,10]);
// Over PAID demos the cohort IS its own paid count, so the measure is a
// truthful 0% rather than nonsense — it is simply hidden as uninformative.
ck('Paid reads a cohort equal to its paid count',
   [parts('paid').cohort,parts('paid').cohortPaid],[10,10]);
ck('  giving a truthful 0%',ctx._dshTrValue('nonpaid',parts('paid')),0);
ck('a bucket with no prelaunchPaid reads 0, not undefined',
   ctx._dshTrParts({spend:1,clicks:1,impressions:1,regular:2,paid:1,prelaunch:3},'prelaunch').cohortPaid,0);

// ── 4. A window is a sum of COMPONENTS, not an average of ratios ──────────
// 70% over 100 and 0% over 1 average to 35% as ratios, but the true pooled
// share is 69/101 = 68.3%. The components must be summed first.
const pts=[{regular:100,paid:30,prelaunch:0,prelaunchPaid:0,spend:0,clicks:0,impressions:0},
           {regular:1,  paid:1, prelaunch:0,prelaunchPaid:0,spend:0,clicks:0,impressions:0}];
const ps=pts.map(p=>ctx._dshTrParts(p,'regular'));
ck('the cohort halves are summed',(()=>{const T=ctx._dshTrSum(ps);return [T.cohort,T.cohortPaid];})(),[101,31]);
ck('the pooled share is not the mean of the ratios',
   +ctx._dshTrValue('nonpaid',ctx._dshTrSum(ps)).toFixed(1),69.3);

// ── 5. Offered on Regular AND Pre-Launch, hidden on Paid ──────────────────
ctx.setPop('regular');
ck('Regular Demos offers it',ctx._dshTrShown(ctx.DSH_TR_METRICS).map(m=>m.k),
   ['count','cost','nonpaid','cvr','ctr']);
ctx.setPop('prelaunch');
ck('Pre-Launch Demos offers it too',ctx._dshTrShown(ctx.DSH_TR_METRICS).map(m=>m.k),
   ['count','cost','nonpaid','cvr','ctr']);
ck('  including its card',ctx._dshTrShown(ctx.DSH_TR_CARDS).map(c=>c.k),
   ['count','cost','nonpaid','cvr','ctr','spend']);
ctx.setPop('paid');
ck('Paid Demos hides it',ctx._dshTrShown(ctx.DSH_TR_METRICS).map(m=>m.k),['count','cost','cvr','ctr']);
ck('  and its card',ctx._dshTrShown(ctx.DSH_TR_CARDS).map(c=>c.k),['count','cost','cvr','ctr','spend']);
ck('  while every other measure stays',
   ctx._dshTrShown(ctx.DSH_TR_METRICS).length,ctx.DSH_TR_METRICS.length-1);

// ── 6. Switching population must not leave a hidden measure selected ──────
// Left selected over paid demos it would draw a flat zero line with no pill
// lit, which looks like a broken chart rather than an unavailable measure.
ctx.setMetric('nonpaid'); ctx.setPop('paid');
ck('selected, then switched to Paid Demos, it falls back',ctx._dshTrMetricDef().k,'count');
ctx.setPop('prelaunch');
ck('  but Pre-Launch keeps it selected',ctx._dshTrMetricDef().k,'nonpaid');
ctx.setPop('regular');
ck('  as does Regular',ctx._dshTrMetricDef().k,'nonpaid');
ctx.setMetric('cvr'); ctx.setPop('paid');
ck('a normal measure is untouched by the fallback',ctx._dshTrMetricDef().k,'cvr');

// ── 7. It ties to the Key Metrics tile over the regular cohort ────────────
ck('the Key Metrics tile is the complement of the paid share',
   /const _npPct=\(_acqPct!=null\)\?\(100-_acqPct\):null;/.test(cli),true);
ck('  over the regular cohort',/const _acqPct=_acqOK\?\(_acqP\/_regC\*100\):null;/.test(cli),true);
ck('the worker counts regular paid with mapUtmToChannel on the raw pair',
   /if \(mapUtmToChannel\(_rs, _rm\)\) \{ regularPaid\+\+;/.test(src),true);
ck('  and the trend uses the same test for both cohorts',
   (src.match(/mapUtmToChannel\(rs, rm\)/g)||[]).length,2);
// paid must stay a SUBSET of regular, or Non-Paid over regular could go
// negative; the pre-launch cohort keeps its own count instead.
ck('a pre-launch contact is counted into prelaunchPaid, never paid',
   /if \(bump\(k, 'prelaunch'\) && mapUtmToChannel\(rs, rm\)\) bump\(k, 'prelaunchPaid'\);/.test(src),true);

// ── 8. Wiring ─────────────────────────────────────────────────────────────
ck('the calc line names the cohort it divided by',
   /nonpaid:fN\(Math\.max\(0,T\.cohort-T\.cohortPaid\)\)\+' ÷ '\+fN\(T\.cohort\)\+' '\+popW,/.test(cli),true);
ck('the pill row is built from the filtered list',
   /\+seg\(_dshTrShown\(DSH_TR_METRICS\)\.map\(/.test(cli),true);
ck('  and so is the stat row',
   /\+_dshTrShown\(DSH_TR_CARDS\)\.map\(function\(card\)\{/.test(cli),true);
ck('  with neither reading the unfiltered list directly',
   /\+seg\(DSH_TR_METRICS\.map\(/.test(cli)||/\+DSH_TR_CARDS\.map\(/.test(cli),false);
ck('the Clinician trend is untouched',
   (()=>{const m=/const CLN_TR_METRICS=\[([\s\S]*?)\n\];/.exec(cli);
     return [...m[1].matchAll(/\{k:'(\w+)'/g)].map(x=>x[1]);})(),
   ['count','cost','cvr','ctr','md','mdpct']);

// ── 9. Scope: every helper the filter bar calls must be REACHABLE ─────────
function _stripLiterals(s){
  let o='',i=0;const n=s.length;
  while(i<n){const c=s[i];
    if(c==='/'&&s[i+1]==='/'){while(i<n&&s[i]!=='\n')i++;continue;}
    if(c==="'"||c==='"'||c==='`'){const q=c;i++;while(i<n&&s[i]!==q){if(s[i]==='\\')i++;i++;}i++;o+=' ';continue;}
    o+=c;i++;}
  return o;
}
const MODULE_SCOPE=new Set([...cli.matchAll(/^(?:function\s+([A-Za-z_$][\w$]*)|(?:const|let|var)\s+([A-Za-z_$][\w$]*))/gm)]
  .map(m=>m[1]||m[2]));
const KW=new Set(['if','for','while','switch','catch','return','typeof','function','new','else','do','of','in','await']);
const GLOBALS=new Set(['String','Number','Math','Date','JSON','Object','Array','parseInt','parseFloat','isNaN','document','window','encodeURIComponent','Boolean']);
function unresolvedCallsIn(name){
  const body=_stripLiterals(fn(cli,name));
  const local=new Set([...body.matchAll(/(?:function|const|let|var)\s+([A-Za-z_$][\w$]*)/g)].map(m=>m[1]));
  return [...new Set([...body.matchAll(/(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(/g)].map(m=>m[1]))]
    .filter(n=>!KW.has(n)&&!GLOBALS.has(n)&&!local.has(n)&&!MODULE_SCOPE.has(n));
}
ck('the new hidden-measure helper is at module scope',MODULE_SCOPE.has('_dshTrHidden'),true);
ck('every helper the filter bar calls resolves',unresolvedCallsIn('_metaFilterBar'),[]);
ck('  and so do the trend helpers’',
   ['_dshTrHidden','_dshTrShown','_dshTrMetricDef','_dshTrParts','_dshTrValue']
     .flatMap(n=>unresolvedCallsIn(n)),[]);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
