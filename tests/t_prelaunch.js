// Pre-Launch Demos as a third population on the Brand Trend Analysis. The
// series used to DISCARD these contacts outright (`continue` on the pre-launch
// tier), so this is a payload change, not just a toggle: regular and prelaunch
// are mutually exclusive and together cover every contact, exactly as
// regularTotal / preTotal do in processScheduledContacts.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── the REAL worker builder ───────────────────────────────────────────────
const wctx={console};vm.createContext(wctx);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{buildBrandTrend});',wctx);
ck('the builder loaded',typeof wctx.buildBrandTrend,'function');

const PRE='Pre-launch / just launching';            // the live tier string
const C=(id,date,tier,src_,med)=>({id,properties:{createdate:date+'T12:00:00.000Z',
  average_monthly_web_traffic:tier, utm_source:src_||'', utm_medium:med||''}});
const ADS=[{date:'2026-10-05',spend:'100',clicks:'50',impressions:'1000',datasource:'facebook'}];
const build=(cs)=>wctx.buildBrandTrend(cs,ADS,'2026-10-05','2026-10-05').day[0];

// ── 1. Pre-launch is COUNTED now, not discarded ───────────────────────────
let d=build([C('1','2026-10-05',PRE)]);
ck('a pre-launch contact lands in its own bucket',d.prelaunch,1);
ck('  and NOT in regular',d.regular,0);
ck('  nor in paid',d.paid,0);
// Before this change the contact was dropped on the floor: no bucket at all.
d=build([C('1','2026-10-05','50-100K monthly web visitors')]);
ck('a normal-tier contact is regular, not pre-launch',[d.regular,d.prelaunch],[1,0]);

// ── 2. The two populations partition the contacts ─────────────────────────
const MIX=[C('1','2026-10-05',PRE),C('2','2026-10-05',PRE),
           C('3','2026-10-05','0-10K monthly web visitors'),
           C('4','2026-10-05','500K+ monthly web visitors','ig','paid-social')];
d=build(MIX);
ck('regular + prelaunch covers every contact',d.regular+d.prelaunch,MIX.length);
ck('  split correctly',[d.regular,d.prelaunch],[2,2]);
// paid stays INSIDE regular, so Non-Paid · Last Click keeps meaning the organic
// half of the regular cohort rather than of everything.
ck('paid remains a subset of regular',[d.paid,d.paid<=d.regular],[1,true]);

// ── 3. A paid-UTM pre-launch contact gets its OWN paid count ──────────────
// It must never land in `paid`: that would stop paid being a subset of regular
// and let Non-Paid over the regular cohort go negative. It goes to
// prelaunchPaid instead, which is what lets Non-Paid work over pre-launch too.
d=build([C('1','2026-10-05',PRE,'ig','paid-social')]);
ck('a pre-launch contact with a paid UTM stays out of regular and paid',
   [d.regular,d.paid],[0,0]);
ck('  and is counted in its own cohort, paid and all',
   [d.prelaunch,d.prelaunchPaid],[1,1]);
d=build([C('1','2026-10-05',PRE,'google','organic')]);
ck('an organic pre-launch contact is pre-launch but not pre-launch-paid',
   [d.prelaunch,d.prelaunchPaid],[1,0]);
d=build([C('1','2026-10-05',PRE)]);
ck('  and an untagged one likewise',[d.prelaunch,d.prelaunchPaid],[1,0]);
// The mix the live account actually shows: cpc/paid-social alongside
// organic/referral on the same cohort.
d=build([C('1','2026-10-05',PRE,'google','cpc'),C('2','2026-10-05',PRE,'ig','paid-social'),
         C('3','2026-10-05',PRE,'google','organic'),C('4','2026-10-05',PRE,'Zephta','referral')]);
ck('a realistic pre-launch mix splits paid from organic',
   [d.prelaunch,d.prelaunchPaid],[4,2]);

// ── 4. The tier test matches processScheduledContacts ─────────────────────
ck('the live tier string is matched',build([C('1','2026-10-05',PRE)]).prelaunch,1);
ck('  case-insensitively',build([C('1','2026-10-05','PRE-LAUNCH / JUST LAUNCHING')]).prelaunch,1);
ck('  on a substring, as Key Metrics does',
   build([C('1','2026-10-05','pre-launch')]).prelaunch,1);
ck('  and a missing tier is regular, not pre-launch',
   (()=>{const x=build([{id:'1',properties:{createdate:'2026-10-05T12:00:00.000Z'}}]);
     return [x.regular,x.prelaunch];})(),[1,0]);
ck('both sides use the same substring gate',
   (src.match(/wt\.includes\('pre-launch'\)/g)||[]).length>=2,true);

// ── 5. Dedupe and the window still apply ──────────────────────────────────
d=build([C('1','2026-10-05',PRE),C('1','2026-10-05',PRE)]);
ck('a duplicate pre-launch contact is counted once',d.prelaunch,1);
d=wctx.buildBrandTrend([C('1','2026-09-01',PRE)],ADS,'2026-10-05','2026-10-05').day[0];
ck('a pre-launch contact outside the window is not counted',d.prelaunch,0);

// ── 6. It survives the day → week → month rollup ──────────────────────────
const t=wctx.buildBrandTrend(
  [C('1','2026-10-05',PRE),C('2','2026-10-06',PRE),C('3','2026-10-06','0-10K monthly web visitors')],
  [{date:'2026-10-05',spend:'100',clicks:'50',impressions:'1000',datasource:'facebook'}],
  '2026-10-05','2026-10-06');
ck('days carry it',t.day.map(x=>x.prelaunch),[1,1]);
ck('the week sums it',t.week[0].prelaunch,2);
ck('the month sums it',t.month[0].prelaunch,2);
ck('  without disturbing regular',[t.week[0].regular,t.month[0].regular],[1,1]);
// Every population is precomputed, so a third one needs its own rates.
ck('its cost per demo is precomputed',t.week[0].cpdPre,100/2);
ck('its CVR is precomputed',t.week[0].cvrPre,(2/50)*100);
ck('  and a cost with no spend is null, not a fake-efficient 0',
   wctx.buildBrandTrend([C('1','2026-10-05',PRE)],
     [{date:'2026-10-05',spend:'0',clicks:'50',impressions:'1000',datasource:'facebook'}],
     '2026-10-05','2026-10-05').day[0].cpdPre,null);

// ── 7. The client resolves the third population ───────────────────────────
const ctx={console};vm.createContext(ctx);
vm.runInContext([
  'var _dshTrendPopV=null,_dshTrendMetricV=null; var window=globalThis;',
  decl(cli,'const DSH_TR_POPS=[','];'),
  decl(cli,'const DSH_TR_METRICS=[','];'),
  decl(cli,'const DSH_TR_CARDS=[','];'),
  fn(cli,'_dshTrendPop'),fn(cli,'_dshTrendMetric'),fn(cli,'_dshTrPopDef'),
  fn(cli,'_dshTrMetricDef'),fn(cli,'_dshTrHidden'),fn(cli,'_dshTrShown'),
  fn(cli,'_dshTrParts'),fn(cli,'_dshTrSum'),fn(cli,'_dshTrValue'),
  'Object.assign(globalThis,{DSH_TR_POPS,DSH_TR_METRICS,DSH_TR_CARDS,_dshTrPopDef,'
   +'_dshTrMetricDef,_dshTrHidden,_dshTrShown,_dshTrParts,_dshTrValue,setPop(p){_dshTrendPopV=p;},'
   +'setMetric(m){_dshTrendMetricV=m;}});',
].join('\n'),ctx);
const P={spend:1000,clicks:500,impressions:20000,regular:40,paid:10,prelaunch:7};
ck('regular resolves the regular count',ctx._dshTrParts(P,'regular').pop,40);
ck('paid resolves the paid count',ctx._dshTrParts(P,'paid').pop,10);
ck('prelaunch resolves the pre-launch count',ctx._dshTrParts(P,'prelaunch').pop,7);
// An unknown population must not silently read as pre-launch or paid.
ck('an unknown population falls back to regular',ctx._dshTrParts(P,'nonsense').pop,40);
ck('a bucket with no prelaunch field reads 0, not undefined',
   ctx._dshTrParts({spend:1,clicks:1,impressions:1,regular:2,paid:1},'prelaunch').pop,0);
ck('the toggle offers it last',ctx.DSH_TR_POPS[2].k,'prelaunch');
ck('  labelled as asked',ctx._dshTrPopDef.call(null)&&(ctx.setPop('prelaunch'),ctx._dshTrPopDef().l),'Pre-Launch Demos');

// ── 8. Measures over the new population ───────────────────────────────────
const T=ctx._dshTrParts(P,'prelaunch');
ck('Count is the pre-launch count',ctx._dshTrValue('count',T),7);
ck('Cost Per divides by it',ctx._dshTrValue('cost',T),1000/7);
ck('CVR divides by it',ctx._dshTrValue('cvr',T),(7/500)*100);
ck('CTR is unaffected by the population',ctx._dshTrValue('ctr',T),(500/20000)*100);
// Non-Paid · Last Click is offered here too: the pre-launch cohort carries its
// own paid split, so the measure is the organic half of THAT cohort.
ctx.setPop('prelaunch');
ck('Non-Paid is offered on Pre-Launch Demos',
   ctx._dshTrShown(ctx.DSH_TR_METRICS).map(m=>m.k),['count','cost','nonpaid','cvr','ctr','k10share']);
ck('  with its card',
   ctx._dshTrShown(ctx.DSH_TR_CARDS).map(c=>c.k),['count','cost','nonpaid','cvr','ctr','k10share','spend']);
ck('  reading this cohort\u2019s own split',
   ctx._dshTrValue('nonpaid',ctx._dshTrParts({...P,prelaunch:80,prelaunchPaid:20},'prelaunch')),75);
ctx.setMetric('nonpaid');
ck('  and selecting it here sticks',ctx._dshTrMetricDef().k,'nonpaid');
ctx.setPop('paid');
ck('  but Paid Demos still falls back',ctx._dshTrMetricDef().k,'count');
ctx.setPop('regular');
ck('  and Regular Demos still offers it',ctx._dshTrMetricDef().k,'nonpaid');

// ── 9. Neither cache may serve a series without the new field ─────────────
// Version-agnostic: the intent is "past every shape that lacks these fields".
ck('API_CACHE_VER is past the shapes without prelaunchPaid',
   +(/const API_CACHE_VER = 'v(\d+)'/.exec(src)||[])[1]>=41,true);
ck('the brand-trend key is too',
   +(/brand_trend_\$\{_btWin\.from\}_\$\{_btWin\.to\}_v(\d+)/.exec(src)||[])[1]>=5,true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
