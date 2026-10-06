// Detailed Dashboard · Conversion Funnel.
//
// The rail is ONE continuous flow: five stages, four connectors, each stage a
// subset of the one before it. This suite slices the real rail block out of
// renderDash and RUNS it against stubbed inputs, so the stage order, the
// pass-through rates, the Biggest-drop pick and the delta suppression are
// asserted as behaviour rather than matched as text.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── the real code, lifted out of renderDash ────────────────────────────────
// The rail block runs top to bottom off locals that renderDash computed above
// it, so it is wrapped in a function whose free variables resolve to globals we
// set per scenario.
const RAIL=decl(cli,'const visitors=d.webPerformance.visitors?.value||0;',
                     "document.getElementById('t-funnel').innerHTML=fh;");
// "Demos supposed to happen" is composed from the EX-PRE-LAUNCH counts a few
// hundred lines earlier; sliced separately so the exclusion can be exercised.
const SUP=decl(cli,'const _heldAllC=demosHeldFull',
                   'const _supAllL=(_heldPreL!=null&&_nsPreL!=null&&_cxPreL!=null)?_heldPreL+_nsPreL+_cxPreL+(_wdSupL||0):null;');

const ctx={console};
vm.createContext(ctx);
vm.runInContext([
  'var __id=null,__out=null;',
  'var document={getElementById:function(id){return {set innerHTML(v){__id=id;__out=v;}};}};',
  decl(cli,'const fN=v=>','toLocaleString();'),
  fn(cli,'nfmt'),fn(cli,'fmtDC'),fn(cli,'fDelta'),
  fn(cli,'fnSub'),fn(cli,'fnStage'),fn(cli,'fnStep'),
  'function __rail(){\n'+RAIL+'\n}',
  'function __sup(){\n'+SUP+'\nreturn {c:_supAllC,p:_supAllP,l:_supAllL};\n}',
].join('\n'),ctx);

// Round numbers chosen so every connector rate is exact; nothing here is real data.
function run(o){
  o=o||{};
  const vis=o.visitors!=null?o.visitors:10000;
  const clicks=o.clicks!=null?o.clicks:1000;
  const reg=o.reg!=null?o.reg:150, pre=o.pre!=null?o.pre:50;
  const sup=o.supposed!=null?o.supposed:100, qual=o.qual!=null?o.qual:40;
  const lm=!!o.lm;
  ctx.d={webPerformance:{visitors:{value:vis,sameTimePrior:{value:o.visitorsPV!=null?o.visitorsPV:vis/2,
          lastMonth:undefined},lastMonth:lm?{value:vis/4}:undefined}},demoTracking:{}};
  ctx.wm2={demoClicks:clicks,demoClicksPrior:clicks/2,demoClicksLastMonth:lm?clicks/4:undefined};
  ctx.qmeta={cancelledBeforeDemoCount:7,noShowCount:5,webinarNoShow:3,webinarRegistrants:40};
  ctx.totalScheduled=reg+pre;
  ctx._regC=reg; ctx._regP=reg/2; ctx._regL=lm?reg/4:null;
  ctx._preC=pre; ctx._preP=pre/2; ctx._preL=lm?pre/4:null;
  ctx._bothC=reg+pre; ctx._bothP=(reg+pre)/2; ctx._bothL=lm?(reg+pre)/4:null;
  ctx._supAllC=sup; ctx._supAllP=sup/2; ctx._supAllL=lm?sup/4:null;
  ctx.qualScaleCur=qual; ctx.heldScaleCur=sup;
  ctx.qd=qual; ctx.qdPV=qual/2; ctx.qdLMV=lm?qual/4:null;
  ctx.__out=null; ctx.__id=null;
  ctx.__rail();
  return ctx.__out;
}
const nf=v=>Math.round(v).toLocaleString();   // same formatter the page uses, so no locale pinning
// Keyed on the teal/grey accent token and the label→value div pair, NOT on the
// exact px/em values around them — a restyle of the card must not fail the suite.
const STAGE=/font-weight:700;color:(var\(--teal\)|var\(--grey\));text-transform:uppercase;[^"]*">([^<]*)<\/div><div style="font-family:var\(--font-h\);font-size:\d+px;[^"]*">([^<]*)<\/div>/g;
const stages=h=>[...h.matchAll(STAGE)].map(m=>({accent:m[1]==='var(--teal)',label:m[2],value:m[3]}));
// Only fnStep's pill uses tabular-nums, so this counts connectors exactly.
const rates=h=>[...h.matchAll(/font-variant-numeric:tabular-nums">([^<]*)<\/span>/g)].map(m=>m[1]);
const biggest=h=>{const m=/Biggest drop<\/span>\s*<span style="color:var\(--navy\)">([^<]*)<\/span>\s*<span style="color:var\(--red\);font-weight:600">([^<]*)<\/span>/.exec(h);return m?[m[1],m[2]]:null;};
const e2e=h=>{const m=/End-to-end<\/span>\s*<span style="font-family:var\(--font-h\);color:var\(--navy\);font-size:15px;font-weight:700">([^<]*)<\/span>/.exec(h);return m?m[1]:null;};

const H=run();

// ── 1. five stages, in order, four connectors ──────────────────────────────
const st=stages(H);
ck('five stages, in funnel order',st.map(s=>s.label),
   ['Visitors','Demo clicks','Regular Demos Scheduled','Regular Demos Supposed to Happen','Qualified Regular Demos']);
ck('four connectors between them',rates(H).length,4);
ck('one connector per gap, never a trailing one',rates(H).length,st.length-1);
// The rail is a CSS grid with an explicit track per stage and per connector.
// Adding a stage without a column silently squashes the whole row.
{
  const tracks=(/grid-template-columns:([^;"]+)/.exec(RAIL)||[])[1].trim().split(/\s+/);
  ck('the grid declares a track for every stage and every connector',tracks.length,st.length+rates(H).length);
  // Stage tracks are fr units, connector tracks are a fixed gutter. Match on the
  // unit, not the width, so retuning the gutter is not a test failure.
  ck('and the connector tracks are the narrow fixed gutters',tracks.filter(t=>/^[\d.]+px$/.test(t)).length,4);
}

// ── 2. each stage shows the figure it claims ───────────────────────────────
ck('Visitors is web traffic',st[0].value,nf(10000));
ck('Demo clicks is the CTA click count',st[1].value,nf(1000));
// The headline on the Scheduled step is the REGULAR count, not the combined
// total — Pre-Launch and All are detail hung underneath it.
ck('Scheduled leads with Regular demos, not the combined total',st[2].value,nf(150));
ck('and carries Pre-Launch + All as sub-rows',
   [/>Pre-Launch<\/span>[\s\S]{0,180}?>50<\/span>/.test(H),/>All<\/span>[\s\S]{0,180}?>200<\/span>/.test(H)],[true,true]);
ck('the Scheduled value is labelled so the sub-rows are not read as a total',/>regular demos<\/div>/.test(H),true);
ck('Supposed to Happen is the ex-pre-launch settled population',st[3].value,nf(100));
ck('the terminal stage is Qualified Regular Demos',st[4].value,nf(40));

// ── 3. accent ──────────────────────────────────────────────────────────────
// Two accents, and they are the two steps the section exists to compare: the
// demand the ads bought and the qualified demos it turned into.
ck('exactly two stages are accented',st.filter(s=>s.accent).length,2);
ck('and they are Scheduled and the terminal stage',st.filter(s=>s.accent).map(s=>s.label),
   ['Regular Demos Scheduled','Qualified Regular Demos']);
// The accent border colour is read out of fnStage rather than pinned, so
// repainting the tint stays a style change and not a test failure.
const ACCENT_BORDER=(/border:1px solid (#[0-9A-Fa-f]{3,8})/.exec(fn(cli,'fnStage'))||[])[1];
ck('an accented card gets the teal tint, not the plain white one',
   H.split('border:1px solid '+ACCENT_BORDER+'"').length-1,2);

// ── 4. the connectors carry the real pass-through rates ────────────────────
ck('every connector states its rate to one decimal',rates(H),['10.0%','20.0%','50.0%','40.0%']);
{
  // Both connectors touching the Scheduled stage divide by the COMBINED booked
  // total (_bothC), not the Regular headline above them — the clicks produced
  // every booking, so the rail's rates describe all of them.
  const h=run({reg:50,pre:150});           // same _bothC=200, different headline
  ck('the Scheduled connectors follow the combined total, not the headline',rates(h),rates(H));
  ck('while the headline itself moves',stages(h)[2].value,nf(50));
}
// The pill states the raw pass-through share. A log/sqrt "visual" rescale would
// make the printed number disagree with the arithmetic the footer sums.
ck('the rate is a plain share of the track, never rescaled',
   /Math\.(pow|sqrt|log)/.test(fn(cli,'fnStep')),false);

// ── 5. a single delta renderer, and it SUPPRESSES vs LM ────────────────────
// Every card used to print a dead "vs LM —" row whether or not a last-month
// value existed; on a five-stage rail plus three CVR chips that was eight.
ck('fDelta renders vs prior always',/vs prior/.test(ctx.fDelta(10,8,null,'#')),true);
ck('and vs LM only when there is a last-month value',/vs LM/.test(ctx.fDelta(10,8,null,'#')),false);
ck('a present last-month value brings the row back',/vs LM/.test(ctx.fDelta(10,8,6,'#')),true);
// A real zero is a value. Gating on truthiness would hide "LM was 0".
ck('a last-month ZERO still renders',/vs LM/.test(ctx.fDelta(10,8,0,'#')),true);
ck('a missing current value is an em-dash, not NaN',
   /—/.test(ctx.fDelta(null,8,6,'#'))&&!/NaN/.test(ctx.fDelta(null,8,6,'#')),true);
{
  const withLM=run({lm:true});
  // 5 stages + 3 CVR chips all route through the ONE renderer.
  ck('with no LM data the rail shows no vs-LM rows at all',H.split('vs LM').length-1,0);
  ck('with LM data every stage and every CVR chip shows one',withLM.split('vs LM').length-1,8);
  ck('vs prior is unconditional on both',
     [H.split('vs prior').length-1,withLM.split('vs prior').length-1],[8,8]);
}
ck('every copy of fDelta gates the vs-LM row',
   (cli.match(/function fDelta\(/g)||[]).length,(cli.match(/if\(lmv!=null\)h\+=/g)||[]).length);
ck('and gates it on a value, not truthiness',/if\(lmv!=null\)/.test(fn(cli,'fDelta')),true);
// deltaBlock is the unconditional two-line renderer used elsewhere on the page.
// A stage card reaching for it is exactly how the dead rows came back last time.
ck('the stage card does not fall back to the unconditional block',/deltaBlock\(/.test(fn(cli,'fnStage')),false);
ck('the CVR chips use the same renderer as the stages',/\$\{fDelta\(v,pv,lmv,'%',2\)\}/.test(RAIL),true);

// ── 6. every step is a Biggest-drop candidate ──────────────────────────────
// A step missing from `drops` can never be named, so the footer would point at
// the second-worst leak forever.
{
  const steps=[...RAIL.matchAll(/fh\+=fnStep\((\w+)\)/g)].map(m=>m[1]);
  const cand=[...RAIL.matchAll(/pct:(\w+)!=null\?100-\1:null/g)].map(m=>m[1]);
  ck('every connector the rail draws is also a drop candidate',cand,steps);
  ck('and there are no candidates for steps it does not draw',cand.length,rates(H).length);
}
{
  // Drive each step in turn to the worst rate and check the footer names it.
  const scen=[
    {name:'Visitors → Demo clicks',      visitors:10000,clicks:1000, reg:900, pre:0, supposed:810, qual:729},
    {name:'Demo clicks → Scheduled',     visitors:10000,clicks:9000, reg:900, pre:0, supposed:810, qual:729},
    {name:'Scheduled → Supposed to happen',visitors:10000,clicks:9000,reg:8100,pre:0,supposed:810, qual:729},
    {name:'Supposed to happen → Qualified',visitors:10000,clicks:9000,reg:8100,pre:0,supposed:7290,qual:729},
  ];
  const got=scen.map(s=>{const b=biggest(run(s));return b&&b[0];});
  ck('each of the four steps can win Biggest drop',got,scen.map(s=>s.name));
  ck('and the losing share is stated, not the pass-through rate',biggest(run(scen[0]))[1],'90.0% leave');
}

// ── 7. empty / zero data degrades to dashes, never NaN ─────────────────────
{
  const z=run({visitors:0,clicks:0,reg:0,pre:0,supposed:0,qual:0});
  ck('with no traffic every connector is an em-dash',rates(z),['—','—','—','—']);
  ck('and the rail renders no NaN anywhere',/NaN/.test(z),false);
  // Every drop is null, so there is no worst step to name — the block is dropped
  // rather than printed with "undefined% leave".
  ck('Biggest drop is omitted rather than guessed',biggest(z),null);
  ck('end-to-end falls back to an em-dash',e2e(z),'—');
}
ck('end-to-end is qualified demos over visitors, to two decimals',e2e(H),'0.40%');

// ── 8. the stage figures exclude pre-launch ────────────────────────────────
{
  // Supposed to Happen = held + no-show + cancelled, all on the ex-pre-launch
  // tier, plus the webinar-date deals the worker deduped in. Pre-launch demos
  // are not Qualified Demos, so they cannot sit in the population those come from.
  ctx.demosHeldFull=100; ctx.demosHeldFullPV=50; ctx.demosHeldFullLMV=25;
  ctx.qd=40;
  ctx.qmeta={heldExPre:30,heldExPrePrior:15,heldExPreLastMonth:7,
             noShowExPre:5,noShowExPrePrior:3,noShowExPreLastMonth:2,
             cancelledExPre:5,cancelledExPrePrior:2,cancelledExPreLastMonth:1,
             noShowCount:50,cancelledBeforeDemoCount:50,
             webinarDateExtraSupposed:10,webinarDateExtraSupposedPrior:4,webinarDateExtraSupposedLastMonth:2};
  ck('Supposed to Happen sums the EX-PRE-LAUNCH outcomes plus webinar-date deals',ctx.__sup().c,50);
  ck('and not the all-traffic ones',ctx.__sup().c===100+50+50+10,false);
  // A payload predating the ExPre fields must degrade to all-traffic, not blank.
  ctx.qmeta={noShowCount:50,cancelledBeforeDemoCount:50,webinarDateExtraSupposed:10};
  ck('an older payload falls back to all-traffic rather than blanking',ctx.__sup().c,100+50+50+10);
}
ck('the Scheduled headline reads the regular-only total',
   /const _regC=_wtDW\.scheduledRegularTotal\|\|0;/.test(cli),true);
ck('which the worker computes with pre-launch split out',/scheduledRegularTotal: c\.scheduled\.regularTotal,/.test(src),true);
// The terminal stage is the canonical Qualified Demos figure. Reading it off
// executiveSummary._meta returned undefined and fell through to held-all-traffic,
// so the card and the rate beside it described different populations.
ck('the terminal stage is qmeta.qualDemoCount, not the all-traffic held count',
   /const held=qd,heldPV=qdPV,heldLMV=qdLMV;/.test(RAIL)
   &&/const qd=\(qmeta\.qualDemoCount!=null\?qmeta\.qualDemoCount:/.test(cli),true);
ck('and the worker excludes pre-launch when counting it',
   /if \(demoDidHappen\(att, rawSt\) && !_isPreLaunchDeal\) \{/.test(src),true);
ck('with Webinars Happened folded in additively',/pl\.qualifiedRawCount = \(pl\.qualifiedRawCount \|\| 0\) \+ n;/.test(src),true);

// ── 9. wiring ──────────────────────────────────────────────────────────────
ck('the rail renders into the Conversion Funnel container',ctx.__id,'t-funnel');
ck('which the static page provides under that section header',
   /id="dsec-cvrfunnel">Conversion Funnel<\/div><div id="t-funnel"><\/div>/.test(cli),true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
