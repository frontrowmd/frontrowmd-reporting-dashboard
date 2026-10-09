// Non-Paid · Last Click on the Brand Trend Analysis. It is the organic half of
// the REGULAR cohort, so it is meaningless over paid demos — where it is zero
// by construction — and is offered on Regular Demos only.
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
  fn(cli,'_dshTrMetricDef'), fn(cli,'_dshTrShown'),
  fn(cli,'_dshTrParts'), fn(cli,'_dshTrSum'), fn(cli,'_dshTrValue'),
  'Object.assign(globalThis,{DSH_TR_METRICS,DSH_TR_CARDS,_dshTrMetricDef,_dshTrShown,'
   +'_dshTrParts,_dshTrSum,_dshTrValue,setPop(p){_dshTrendPopV=p;},setMetric(m){_dshTrendMetricV=m;}});',
].join('\n'),ctx);
ck('the helpers loaded',typeof ctx._dshTrValue,'function');

// ── 1. Position and shape ─────────────────────────────────────────────────
ck('the measure sits directly after Cost Per',
   ctx.DSH_TR_METRICS.map(m=>m.k),['count','cost','nonpaid','cvr','ctr']);
ck('  and so does its card',ctx.DSH_TR_CARDS.map(c=>c.k),['count','cost','nonpaid','cvr','ctr','spend']);
const NP=ctx.DSH_TR_METRICS.find(m=>m.k==='nonpaid');
ck('it is a percentage',NP.unit,'%');
ck('it is named as the Key Metrics tile names it',NP.l,'Non-Paid · Last Click');
// pop:false — it never swaps to the paid cohort, so the card label must not
// append "· Paid Demos" to a figure that is always about regular ones.
ck('it does not swap with the population',NP.pop,false);
ck('  and is marked regular-only',NP.regularOnly,true);
ck('its colour is not already taken',
   ctx.DSH_TR_METRICS.filter(m=>m.c===NP.c).length,1);

// ── 2. The value ──────────────────────────────────────────────────────────
const V=(regular,paid)=>ctx._dshTrValue('nonpaid',{regular,paid,spend:0,clicks:0,impressions:0,pop:regular});
ck('it is the organic share of the regular cohort',V(100,30),70);
ck('  all organic reads 100%',V(40,0),100);
ck('  all paid reads 0%',V(40,40),0);
// A period with no regular demos has no share, not a zero one — a real 0 plots
// on the floor and reads as "everything was paid".
ck('no regular demos yields no value, not zero',V(0,0),null);
ck('  and a negative can never be produced',V(10,10),0);

// ── 3. A window is a sum of COMPONENTS, not an average of ratios ──────────
// 70% over 100 and 0% over 1 average to 35% as ratios, but the true pooled
// share is 69/101 = 68.3%. The components must be summed first.
const pts=[{regular:100,paid:30,spend:0,clicks:0,impressions:0},
           {regular:1,  paid:1, spend:0,clicks:0,impressions:0}];
const parts=pts.map(p=>ctx._dshTrParts(p,'regular'));
ck('both halves survive into the components',
   parts.map(x=>[x.regular,x.paid]),[[100,30],[1,1]]);
ck('  and are summed',(()=>{const T=ctx._dshTrSum(parts);return [T.regular,T.paid];})(),[101,31]);
ck('the pooled share is not the mean of the ratios',
   +ctx._dshTrValue('nonpaid',ctx._dshTrSum(parts)).toFixed(1),69.3);
// The population toggle must not change the components it reads.
ck('the components are the same whichever population is selected',
   (()=>{const a=ctx._dshTrParts(pts[0],'paid');return [a.regular,a.paid];})(),[100,30]);

// ── 4. Offered on Regular Demos only ──────────────────────────────────────
ctx.setPop('regular');
ck('on Regular Demos the measure is offered',
   ctx._dshTrShown(ctx.DSH_TR_METRICS).map(m=>m.k),['count','cost','nonpaid','cvr','ctr']);
ck('  and its card is shown',
   ctx._dshTrShown(ctx.DSH_TR_CARDS).map(c=>c.k),['count','cost','nonpaid','cvr','ctr','spend']);
ctx.setPop('paid');
ck('on Paid Demos it is hidden from the pills',
   ctx._dshTrShown(ctx.DSH_TR_METRICS).map(m=>m.k),['count','cost','cvr','ctr']);
ck('  and from the stat row',
   ctx._dshTrShown(ctx.DSH_TR_CARDS).map(c=>c.k),['count','cost','cvr','ctr','spend']);
// Nothing else may be hidden by accident.
ck('  while every other measure stays',
   ctx._dshTrShown(ctx.DSH_TR_METRICS).length,ctx.DSH_TR_METRICS.length-1);

// ── 5. Switching population must not leave it selected ────────────────────
// Left selected over paid demos it would draw a flat zero line with no pill
// lit, which looks like a broken chart rather than an unavailable measure.
ctx.setMetric('nonpaid'); ctx.setPop('paid');
ck('selected, then switched to Paid Demos, it falls back',ctx._dshTrMetricDef().k,'count');
ctx.setPop('regular');
ck('  and switching back restores it',ctx._dshTrMetricDef().k,'nonpaid');
ctx.setMetric('cvr'); ctx.setPop('paid');
ck('a normal measure is untouched by the fallback',ctx._dshTrMetricDef().k,'cvr');

// ── 6. It ties to the Key Metrics tile ────────────────────────────────────
// Both are (regular - paid) / regular over the same cohort, and `paid` is the
// same mapUtmToChannel test on the RAW utm pair in both places.
ck('the Key Metrics tile is the complement of the paid share',
   /const _npPct=\(_acqPct!=null\)\?\(100-_acqPct\):null;/.test(cli),true);
ck('  over the regular cohort',/const _acqPct=_acqOK\?\(_acqP\/_regC\*100\):null;/.test(cli),true);
ck('the worker counts paid with mapUtmToChannel on the raw pair',
   /if \(mapUtmToChannel\(_rs, _rm\)\) \{ regularPaid\+\+;/.test(src),true);
ck('  and the trend series uses the same test',
   /if \(mapUtmToChannel\(rs, rm\)\) bump\(k, 'paid'\);/.test(src),true);
// The trend's paid bucket is a SUBSET of regular — every paid contact is also
// counted as regular — which is what makes regular-paid the organic half.
ck('  bumping regular first, then paid',
   /if \(!bump\(k, 'regular'\)\) continue;[\s\S]{0,200}?bump\(k, 'paid'\)/.test(src),true);

// ── 7. Wiring ─────────────────────────────────────────────────────────────
// _dshTrShown filtering correctly is worth nothing if the renderer reads the
// unfiltered list — the measure would simply still be on screen over paid
// demos. Pin both call sites.
ck('the pill row is built from the filtered list',
   /\+seg\(_dshTrShown\(DSH_TR_METRICS\)\.map\(/.test(cli),true);
ck('  and so is the stat row',
   /\+_dshTrShown\(DSH_TR_CARDS\)\.map\(function\(card\)\{/.test(cli),true);
ck('  with neither reading the unfiltered list directly',
   /\+seg\(DSH_TR_METRICS\.map\(/.test(cli)||/\+DSH_TR_CARDS\.map\(/.test(cli),false);
ck('the calc line names the figures it divided',
   /nonpaid:fN\(Math\.max\(0,T\.regular-T\.paid\)\)\+' ÷ '\+fN\(T\.regular\)\+' regular demos'/.test(cli),true);
ck('the subtitle says it is regular-only rather than toggle-independent',
   /the organic share of <strong>regular demos<\/strong>, offered on that population only/.test(cli),true);
ck('the Clinician trend is untouched',
   (()=>{const m=/const CLN_TR_METRICS=\[([\s\S]*?)\n\];/.exec(cli);
     return [...m[1].matchAll(/\{k:'(\w+)'/g)].map(x=>x[1]);})(),
   ['count','cost','cvr','ctr','md','mdpct']);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
