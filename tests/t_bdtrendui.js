// The Brand Trend Analysis card. The port's whole risk is that it LOOKS like
// the Clinician section while quietly computing something else, so these pin
// the places the two legitimately differ and the places they must not.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console,window:{},f$:v=>'$'+Math.round(v).toLocaleString(),fN:v=>Number(v||0).toLocaleString()};
vm.createContext(ctx);
vm.runInContext([
  'var window=this;var f$=this.f$,fN=this.fN;',
  decl(src,"const CLN_TR_GRAINS=[","];"),
  decl(src,"const CLN_TR_SPEND=","};"),
  "const CLN_TR_ROLL_C='#172C45';const CLN_TR_ROLL_DAYS=7;",
  decl(src,"const CLN_TR_MON=[","];"),
  fn(src,'_clnTrFmt'),
  decl(src,"const DSH_TR_GRAINS=","];"),            // through DSH_TR_POPS
  decl(src,"const DSH_TR_METRICS=[","];"),
  decl(src,"const DSH_TR_CARDS=[","];"),
  fn(src,'_dshTrendGrain'), fn(src,'_dshTrendPop'), fn(src,'_dshTrendMetric'),
  fn(src,'_dshTrGrainDef'), fn(src,'_dshTrMetricDef'), fn(src,'_dshTrPopDef'),
  fn(src,'_dshTrendPts'), fn(src,'_dshTrParts'), fn(src,'_dshTrSum'),
  fn(src,'_dshTrValue'), fn(src,'_dshTrStat'),
  'Object.assign(globalThis,{DSH_TR_POPS,DSH_TR_METRICS,DSH_TR_CARDS,DSH_TR_GRAINS,'
   +'_dshTrParts,_dshTrSum,_dshTrValue,_dshTrStat,_dshTrendPts,_dshTrPopDef});',
].join('\n'),ctx);
ck('the real helpers loaded',typeof ctx._dshTrValue,'function');

// EVERY structural assertion below is scoped to the Brand block. The Clinician
// section is a near-twin living in the same file, so an unscoped regex happily
// matches ITS copy and passes no matter what this card does.
const _i0=src.indexOf('// \u2500\u2500 Trend Analysis (Brand Dashboard)');
const _i1=src.indexOf('// Every type present in the data',_i0);
ck('the Brand block is locatable and non-trivial',_i0>0&&_i1>_i0&&(_i1-_i0)>6000,true);
const BR=src.slice(_i0,_i1);
ck('  and does not swallow the Clinician one',/CLN_TR_CARDS=|_clnTrendCard/.test(BR),false);

// ── 1. The population toggle the user asked for ───────────────────────────
ck('two populations: Regular and Paid demos',ctx.DSH_TR_POPS.map(p=>p.l),['Regular Demos','Paid Demos']);
ck('  keyed regular/paid',ctx.DSH_TR_POPS.map(p=>p.k),['regular','paid']);
ck('the Clinician labels are NOT carried over',/Full Submission/.test(JSON.stringify(ctx.DSH_TR_POPS)),false);
const P={spend:1000,clicks:500,impressions:20000,regular:40,paid:10,signed:4};
ck('regular resolves the regular count',ctx._dshTrParts(P,'regular').pop,40);
ck('paid resolves the paid count',ctx._dshTrParts(P,'paid').pop,10);
// Signed was removed as a measure, so the components no longer carry it.
// regular/paid ride along for Non-Paid · Last Click, which needs BOTH halves of
// the regular cohort rather than whichever one the toggle selected.
ck('the components carry both halves of the regular cohort',
   Object.keys(ctx._dshTrParts(P,'regular')).sort(),
   ['clicks','impressions','paid','pop','regular','spend']);
ck('  and they do not follow the population toggle',
   (()=>{const a=ctx._dshTrParts(P,'paid');return [a.regular,a.paid];})(),[40,10]);

// ── 2. The six metrics, in the order asked for ────────────────────────────
ck('metric pills',ctx.DSH_TR_METRICS.map(m=>m.l),
   ['Count','Cost Per','Non-Paid · Last Click','CVR','CTR']);
ck('KPI cards',ctx.DSH_TR_CARDS.map(c=>c.k),['count','cost','nonpaid','cvr','ctr','spend']);
ck('Ad Spend has a card but is not a selectable measure',
   [ctx.DSH_TR_CARDS.filter(c=>c.k==='spend')[0].metric,ctx.DSH_TR_METRICS.filter(m=>m.k==='spend').length],[null,0]);
// Two measures do not swap with the toggle, for different reasons. CTR is
// clicks over impressions — a property of the ads, not of which demos are
// counted. Non-Paid is pinned to the regular cohort and is offered only there,
// which is a stronger condition and carries its own flag.
ck('the pop-invariant measures',
   ctx.DSH_TR_METRICS.filter(m=>!m.pop).map(m=>m.k),['nonpaid','ctr']);
ck('  and only one of them is regular-only',
   ctx.DSH_TR_METRICS.filter(m=>m.regularOnly).map(m=>m.k),['nonpaid']);
// Signed is gone from EVERY list, not just the pills. A card for a measure
// that cannot be selected is the half-removal this guards against.
ck('Signed is absent from the metric pills',
   ctx.DSH_TR_METRICS.some(m=>/signed/i.test(m.k)||/Signed/.test(m.l)),false);
ck('  and from the KPI cards',
   ctx.DSH_TR_CARDS.some(c=>/signed/i.test(c.k)||/signed/i.test(c.metric||'')),false);
ck('  and no card carries a split any more',ctx.DSH_TR_CARDS.filter(c=>c.withPct).length,0);
ck('  and no live code in the block still mentions it',
   /[Ss]igned/.test(BR.replace(/^\s*\/\/.*$/gm,'')),false);

// ── 3. The maths ──────────────────────────────────────────────────────────
const Treg=ctx._dshTrSum([ctx._dshTrParts(P,'regular')]);
const Tpaid=ctx._dshTrSum([ctx._dshTrParts(P,'paid')]);
ck('count',ctx._dshTrValue('count',Treg),40);
ck('cost per',ctx._dshTrValue('cost',Treg),25);
ck('  rises on the smaller paid denominator',ctx._dshTrValue('cost',Tpaid),100);
ck('cvr = population ÷ clicks',ctx._dshTrValue('cvr',Treg),8);
ck('ctr = clicks ÷ impressions, same on both populations',
   [ctx._dshTrValue('ctr',Treg),ctx._dshTrValue('ctr',Tpaid)],[2.5,2.5]);
ck('the removed measures resolve to null rather than throwing',
   [ctx._dshTrValue('signed',Treg),ctx._dshTrValue('signedpct',Treg)],[null,null]);

// Null-vs-zero has to survive the port or the chart lies on the floor.
const Z=ctx._dshTrSum([ctx._dshTrParts({spend:0,clicks:0,impressions:0,regular:0,paid:0,signed:0},'regular')]);
ck('cost with no spend is null',ctx._dshTrValue('cost',Z),null);
ck('rates with no denominator are null',[ctx._dshTrValue('cvr',Z),ctx._dshTrValue('ctr',Z)],[null,null]);
// The case the cost guard exists for: real demos, NO ad spend. Guarding only
// the denominator lets a 0 through, which plots on the floor of the cost axis
// and reads as the most efficient period on screen.
const O=ctx._dshTrSum([ctx._dshTrParts({spend:0,clicks:120,impressions:9000,regular:6,paid:0,signed:1},'regular')]);
ck('organic demos with no spend have a NULL cost, not 0',ctx._dshTrValue('cost',O),null);
ck('  while their count and CVR are real',[ctx._dshTrValue('count',O),ctx._dshTrValue('cvr',O)],[6,5]);


const R=ctx._dshTrSum([ctx._dshTrParts({spend:50,clicks:100,impressions:1000,regular:0,paid:0,signed:0},'regular')]);
ck('a real zero over a real denominator stays 0',ctx._dshTrValue('cvr',R),0);
ck('  while its cost is still null',ctx._dshTrValue('cost',R),null);

// ── 4. The remaining stats still read correctly ───────────────────────────
const pdef=ctx.DSH_TR_POPS[0];
const cnt=ctx._dshTrStat(ctx.DSH_TR_CARDS[0],Treg,pdef);
ck('a pop-dependent card carries the population suffix',cnt.lbl,'Count \u00b7 Regular Demos');
ck('  and names what it counted',cnt.calc,'40 regular demos');
// By KEY, not by index: a card inserted ahead of these silently repointed
// the old positional lookups at the wrong cards.
const _cardByK=k=>ctx.DSH_TR_CARDS.filter(c=>c.k===k)[0];
const ctrSt=ctx._dshTrStat(_cardByK('ctr'),Treg,pdef);
ck('a pop-invariant card carries no suffix',ctrSt.lbl,'CTR');
const spend=ctx._dshTrStat(_cardByK('spend'),Treg,pdef);
ck('Ad Spend is the last card',spend.lbl,'Ad Spend');
ck('  and really is last',ctx.DSH_TR_CARDS[ctx.DSH_TR_CARDS.length-1].k,'spend');
ck('  and names the channels it covers',spend.calc,'all KPI channels \u00b7 always on the chart');
// Nothing sets a split now, so no card should render one.
ck('no stat produces a split value',
   ctx.DSH_TR_CARDS.every(c=>ctx._dshTrStat(c,Treg,pdef).split==null),true);

// ── 5. Grains: the three toggles, shared with the Clinician section ───────
// Pill order on screen is Daily, Weekly, Monthly. Weekly is still the DEFAULT —
// that comes from _dshTrendGrain(), not from this array's first entry.
ck('Daily / Weekly / Monthly',ctx.DSH_TR_GRAINS.map(g=>[g.l,g.n]),[['Daily',30],['Weekly',8],['Monthly',3]]);
ck('  and Weekly is still what loads',/_dshTrendGrain\(\)\{return window\._dshTrendGrainV\|\|'week';\}/.test(src),true);
ck('  with the fallback naming week rather than taking the first entry',
   /_trGrainDefault\(DSH_TR_GRAINS\)/.test(src)&&!/\|\|DSH_TR_GRAINS\[0\]/.test(src),true);
ck('the grain list is SHARED, not a second copy',/const DSH_TR_GRAINS=CLN_TR_GRAINS;/.test(src),true);
ck('the card slices its own lookback out of one series',
   /all\.slice\(Math\.max\(0,all\.length-g\.n\)\)/.test(BR),true);
ck('reading d.trend, the Brand payload path',/var t=\(d&&d\.trend\)\|\|null/.test(BR),true);
ck('  NOT the clinician ads.trend path',/d\.ads&&d\.ads\.trend/.test(BR),false);

// ── 6. Wiring into the page ───────────────────────────────────────────────
ck('the container sits directly under Key Metrics',
   src.indexOf('id="sec-keymetrics"')<src.indexOf('id="t-dshtrend"')
   &&src.indexOf('id="t-dshtrend"')<src.indexOf('id="dsec-demoinsights"'),true);
ck('renderDash builds and renders it',
   /_dshTrEl\.innerHTML=_dashTrendCard\(d\);_renderDashTrend\(d\)/.test(src),true);
ck('it joins the Jump To nav',/data-nav="Trend Analysis"/.test(BR),true);
ck('  under a dsec-h, which is what the nav selects on this page',
   /class="dsec-h dsec-in" id="dsec-dshtrend'\+sfx\+'"/.test(BR),true);
ck('the three toggles each have a handler',
   ['toggleDshTrendGrain','toggleDshTrendPop','toggleDshTrendMetric'].every(h=>src.includes('window.'+h+'=')),true);
ck('toggling redraws from cached data, not a refetch',
   /function _dshTrendRedraw\(sfx\)\{[\s\S]{0,300}?window\._dshTrendData/.test(BR),true);
// The Executive Dashboard hosts a second copy, so every id this card emits and
// every id it looks up must carry the host suffix.
ck('the panel id is host-scoped',
   /id="dsh-trend-panels'\+sfx\+'"/.test(BR)&&/getElementById\('dsh-trend-panels'\+\(sfx\|\|''\)\)/.test(BR),true);
ck('  as is the section and its heading',
   /id="sec-dshtrend'\+sfx\+'"/.test(BR)&&/id="dsec-dshtrend'\+sfx\+'"/.test(BR),true);
ck('the toggles pass the suffix back',
   /toggleDshTrendGrain\('"\+x\.k\+"','"\+sfx\+"'\)/.test(BR),true);
// An SVG <rect> with no fill paints BLACK and covers the whole plot. Assert the
// GENERAL rule — every band class the markup uses must be declared transparent —
// so a fourth chart with a fourth class fails here instead of shipping a black box.
ck('every hover-band class in the markup has a transparent fill',
   (()=>{const used=[...new Set([...src.matchAll(/class="([a-z]+-band)"/g)].map(m=>m[1]))];
         const rule=(src.match(/^([^\n]*-band[^\n]*)\{fill:transparent\}/m)||[])[1]||'';
         return used.length>=3&&used.every(c=>rule.includes('.'+c));})(),true);
ck('  and this chart uses one of them',/class="dshwk-band"/.test(BR),true);
ck('the hover panel is the shared one',
   /class="dshwk-band"[\s\S]{0,400}?onmouseover="clnWkTip\(event,this\)"/.test(BR),true);
ck('the hover rows are built from the SAME card list, so none duplicates',
   /var rows=DSH_TR_CARDS\.map/.test(BR),true);
// A 7-day window is shorter than one period on the weekly and monthly grains,
// where the rolling line would just redraw the series.
ck('the 7-day average is daily-grain only',
   /if\(g\.k==='day'\)\{\s*\n\s*roll=parts\.map/.test(BR),true);
ck('  and a summed COUNT is divided back to a daily average',
   /if\(v!=null&&mdef\.unit==='#'\)v=v\/w\.length;/.test(BR),true);

// ── 7. The caveat the numbers cannot state themselves ─────────────────────
// The legend caveat existed only to qualify Signed; keeping it would describe
// a stat that is no longer on screen.
ck('the Signed close-date caveat went with the stat',/close date/.test(BR),false);
// "No trend data" covered two different failures with one sentence, and the
// one that actually happened (the payload never carried the field) is the one
// a reader can act on.
ck('a missing payload field reads differently from an empty window',
   /Trend data did not reach the page/.test(BR)&&/No activity in the /.test(BR),true);
ck('  and the missing-field branch is chosen by d.trend, not by the slice',
   /var why=\(d&&d\.trend\)/.test(BR),true);
ck('the card states it ignores the page date filter',
   /Independent of the page date filter/.test(BR),true);
ck('  and names the span the figures actually cover',/Figures below cover/.test(BR),true);

// ── 8. The Clinician section is untouched ─────────────────────────────────
ck('Clinician still has its own populations',/Full Submission/.test(src),true);
ck('  its own MD/DO stat',/CLN_MD_TYPE/.test(src),true);
ck('  and its own card list',/const CLN_TR_CARDS=/.test(src),true);
ck('the Brand card never reads a clinician field',
   /fullApp|inquiries|bySubType|CLN_MD_TYPE/.test(BR),false);
ck('  and the Clinician card never reads a Brand one',
   (()=>{const c0=src.indexOf('// \u2500\u2500 Clinician Trend Analysis');
         const CL=src.slice(c0,src.indexOf('// \u2500\u2500 Trend Analysis (Brand Dashboard)',c0));
         return /DSH_TR_|\.signed|\.paid\b/.test(CL);})(),false);


// ── Weekly stays the default after the pill reorder ───────────────────────
// The array order is now Daily, Weekly, Monthly. Nothing about the default may
// ride on that order — it is named explicitly in two places.
ck('the Brand section still opens on Weekly',
   /_dshTrendGrain\(\)\{return window\._dshTrendGrainV\|\|'week';\}/.test(src),true);
ck('the Clinician section too',
   /_clnTrendGrain\(\)\{return window\._clnTrendGrainV\|\|'week';\}/.test(src),true);
// And the shared fallback picks week BY NAME, so an unknown stored grain lands
// on Weekly rather than on whichever pill happens to be first.
ck('the shared fallback names week rather than taking position 0',
   /return list\.filter\(function\(g\)\{return g\.k==='week';\}\)\[0\]\|\|list\[0\];/.test(src),true);
ck('  and no lookup falls back to the first entry directly',
   /\|\|CLN_TR_GRAINS\[0\]|\|\|DSH_TR_GRAINS\[0\]/.test(src),false);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
