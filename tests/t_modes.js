// Client side of the Regular / Pre-Launch toggle: series selection, defaults and
// the prior/last-month denominators that feed the deltas.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load('dashboard.html');
const ctx={console,Object,String,Number,Array,Math,JSON};
ctx.window={};
vm.createContext(ctx);
vm.runInContext([fn(src,'_cpdwMode'),fn(src,'_cpdwSeries'),fn(src,'_cpdwTotals'),
                 "function f$(v){return '$'+Math.round(v);}function fN(v){return String(Math.round(v));}"].join('\n'),ctx);

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const D={demoTracking:{
  scheduledByDay:{'2026-09-01':1},                       // legacy demos-only
  scheduledByDayAll:{'2026-09-01':10,'2026-09-02':6},
  scheduledByDayRegular:{'2026-09-01':7,'2026-09-02':5},
  scheduledByDayPre:{'2026-09-01':3,'2026-09-02':1},
  scheduledRegularTotalPrior:40, scheduledPreTotalPrior:9,
  scheduledRegularTotalLastMonth:55, scheduledPreTotalLastMonth:12,
},executiveSummary:{_meta:{totalSpend:1200,pTotalSpend:800,pmTotalSpend:1100}}};

// --- mode + series selection ---------------------------------------------
ck('default mode is Regular Demos',ctx._cpdwMode('chart-1'),'regular');
ck('an unset chart still defaults to regular',ctx._cpdwMode('never-seen'),'regular');
ck('Regular selects the regular series',ctx._cpdwSeries(D,'regular'),D.demoTracking.scheduledByDayRegular);
ck('Pre-Launch selects the pre-launch series',ctx._cpdwSeries(D,'pre'),D.demoTracking.scheduledByDayPre);
// byDayAll IS the sum of the two halves, so 'all' reads it rather than merging
// client-side -- a merge could drift from the worker's own total.
ck('All reads byDayAll, not a client-side merge',ctx._cpdwSeries(D,'all'),D.demoTracking.scheduledByDayAll);
ck('and byDayAll really is the two halves summed',
   Object.keys(D.demoTracking.scheduledByDayAll).map(k=>
     D.demoTracking.scheduledByDayRegular[k]+D.demoTracking.scheduledByDayPre[k]),
   Object.values(D.demoTracking.scheduledByDayAll));
// A payload from before the split must degrade, not blank the chart.
ck('a pre-split payload falls back to the legacy demos-only series',
   ctx._cpdwSeries({demoTracking:{scheduledByDay:{'2026-09-01':1}}},'regular'),{'2026-09-01':1});
ck('a missing payload yields an empty series, not undefined',
   ctx._cpdwSeries({},'regular'),{});
ck('an unknown mode falls through to regular',ctx._cpdwSeries(D,'nonsense'),D.demoTracking.scheduledByDayRegular);

// --- totals: numerator and denominator must share one population ----------
const T=m=>ctx._cpdwTotals(D,m);
const sum=o=>Object.values(o).reduce((a,b)=>a+b,0);
ck('regular CPD divides spend by the regular series',T('regular').curV,1200/sum(D.demoTracking.scheduledByDayRegular));
ck('pre-launch CPD divides by the pre-launch series',T('pre').curV,1200/sum(D.demoTracking.scheduledByDayPre));
ck('all CPD divides by every scheduled demo',T('all').curV,1200/sum(D.demoTracking.scheduledByDayAll));
// Last month is preferred over prior when both exist, so the comparison keeps
// one basis rather than mixing a prior window with a last-month denominator.
ck('the comparison prefers last month when it is available',T('regular').priorV,1100/55);
ck('pre-launch compares against the pre-launch last-month count',T('pre').priorV,1100/12);
ck('all compares against both halves summed',T('all').priorV,1100/(55+12));
ck('the calc text names the numbers it divided',T('regular').curCalc,'$1200 ÷ 12');
ck('and the comparison calc names its own',T('regular').priorCalc,'$1100 ÷ 55');

// A half missing must SUPPRESS the combined delta rather than halve it.
const Dhalf=JSON.parse(JSON.stringify(D));
Dhalf.demoTracking.scheduledPreTotalLastMonth=null;
Dhalf.executiveSummary._meta.pmTotalSpend=null;
ck('a missing half suppresses the combined comparison',
   ctx._cpdwTotals(Dhalf,'all').priorV,800/(40+9));
const Dnone=JSON.parse(JSON.stringify(Dhalf));
Dnone.demoTracking.scheduledPreTotalPrior=null;
ck('and with neither half it goes null rather than wrong',
   ctx._cpdwTotals(Dnone,'all').priorV,null);
// Zero demos must not produce Infinity.
const Dzero={demoTracking:{scheduledByDayRegular:{}},executiveSummary:{_meta:{totalSpend:1200}}};
ck('zero demos yields null, not Infinity',ctx._cpdwTotals(Dzero,'regular').curV,null);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
