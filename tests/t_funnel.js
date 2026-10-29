// Detailed Dashboard · Conversion Funnel.
//
// Redesigned 2026-10-29 as a Clinician-style stage table: the page's own steps
// (Visitors ... Qualified Regular Demos) keep their figures and step rates, and
// HubSpot's Sales-pipeline stages are interleaved as a cohort. This suite slices
// the real block out of renderDash and RUNS it against stubbed inputs, so row
// order, every figure, the rates and their hover text, the Biggest-drop pick and
// the delta suppression are asserted as behaviour rather than matched as text.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
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
  fn(cli,'fnSub'),fn(cli,'_fnPct'),
  decl(cli,'const BRAND_STAGE_IDS=','};'),decl(cli,'const BRAND_STAGE_COLORS=','};'),
  'const _CLN_INK_MIN_CONTRAST=3.0;',
  fn(cli,'_clnHexToRgb'),fn(cli,'_clnRelLum'),fn(cli,'_clnContrastWhite'),fn(cli,'_clnHslToRgb'),fn(cli,'_clnInk'),
  'function __rail(){\n'+RAIL+'\n}',
  'function __sup(){\n'+SUP+'\nreturn {c:_supAllC,p:_supAllP,l:_supAllL};\n}',
].join('\n'),ctx);

// The worker's cohort payload, shaped like buildBrandStageFunnel's output.
const SID={appt:'appointmentscheduled',demo:'1084214349',interested:'3722491612',dm:'decisionmakerboughtin',cs:'contractsent',
  won:'closedwon',lost:'closedlost',noShow:'3453957850',notFit:'1062974581',churned:'3453925110',webReg:'3741686470',webHap:'3741686471'};
const ST=(reached,here,num,den,extra)=>Object.assign({reached,here,fromPrevNum:num,fromPrevDen:den,
  fromPrevPct:(den>0&&num!=null)?num/den*100:null},extra||{});
function BSF(lm){
  const cur={total:120,stages:{
    [SID.appt]:ST(80,30,null,null),[SID.webReg]:ST(25,5,null,null),
    [SID.demo]:ST(50,20,50,80),[SID.webHap]:ST(20,15,20,25),
    [SID.won]:ST(12,11,6,50,{noDemo:5,direct:2,viaWebinar:1})},
    exits:{[SID.interested]:{reached:3,here:3},[SID.noShow]:{reached:9,here:4,viaWebinar:7},[SID.notFit]:{reached:0,here:0},
           [SID.lost]:{reached:7,here:7},[SID.churned]:{reached:1,here:1}}};
  const half=JSON.parse(JSON.stringify(cur));
  for(const k in half.stages)half.stages[k].reached=Math.round(half.stages[k].reached/2);
  return {cur,prior:half,lm:lm?half:null};
}
// Round numbers chosen so every step rate is exact; nothing here is real data.
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
  // The losses' counts and rates, as the Quality card computes them above the rail.
  const L=o.loss||{};
  ctx._cxPreC=6; ctx._supOutcomeC=80; ctx._pruneAllC=7.5;
  ctx._nsPreC=4; ctx._nsDenAllC=74; ctx._noShowAllC=4/74*100;
  ctx._wnsC='wnsC' in L?L.wnsC:62; ctx._wnrC='wnrC' in L?L.wnrC:143; ctx._wnsRateC='wnsRate' in L?L.wnsRate:62/143*100;
  ctx.cSpend='spend' in o?o.spend:2000;
  ctx.d.brandStageFunnel='bsf' in o?o.bsf:BSF(lm);
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
// One parsed row per table line: label, reached, from-prior, its hover text,
// here-now, cost-per, and the here-now share of the bar.
const tableRows=h=>{
  const chunks=h.split('gap:12px;align-items:center;padding:').slice(1);
  return chunks.map(c=>{
    const lab=/line-height:1\.2">(?:<span[^>]*>[^<]*<\/span>)?([^<]+)<\/span>/.exec(c);
    const cells=[...c.matchAll(/<span style="text-align:right[^"]*"[^>]*>([\s\S]*?)<\/span>(?=<span)/g)].map(m=>m[1]);
    const conv=/font-variant-numeric:tabular-nums">([\d.]+%)<\/span>/.exec(c);
    const base=/white-space:nowrap">([^<]+)<\/span><\/span>/.exec(c);
    const tip=/data-tip="([^"]*)"/.exec(c);
    const here=/font-size:17px;font-weight:700;color:[^;]+;font-variant-numeric:tabular-nums">([^<]*)</.exec(c);
    const cost=/font-size:11\.5px;color:var\(--grey\);font-variant-numeric:tabular-nums"[^>]*>([^<]*)</.exec(c);
    const fill=/<span style="display:block;height:100%;width:([\d.]+)%;background:[^"]*"><\/span><\/span><\/span>/.exec(c);
    const reached=/font-size:(?:19|16)px;font-weight:700;color:var\(--navy\);font-variant-numeric:tabular-nums">([^<]*)</.exec(c);
    return {label:lab&&lab[1],reached:reached&&reached[1],conv:conv?conv[1]:'\u2014',tip:tip&&tip[1].replace(/&quot;/g,'"'),
      here:here&&here[1],cost:cost&&cost[1],fill:fill?+fill[1]:null,base:base&&base[1],
      sub:/\u21b3<\/span>/.test(c),par:/>PARALLEL<\/span>/.test(c)};
  });
};
const row=(h,l)=>tableRows(h).find(r=>r.label===l);
const stages=h=>[...h.matchAll(STAGE)].map(m=>({accent:m[1]==='var(--teal)',label:m[2],value:m[3]}));
const biggest=h=>{const m=/Biggest drop<\/span>\s*<span style="color:var\(--navy\)">([^<]*)<\/span>\s*<span style="color:var\(--red\);font-weight:600">([^<]*)<\/span>/.exec(h);return m?[m[1],m[2]]:null;};
const e2e=h=>{const m=/End-to-end<\/span>\s*<span style="font-family:var\(--font-h\);color:var\(--navy\);font-size:15px;font-weight:700">([^<]*)<\/span>/.exec(h);return m?m[1]:null;};

const H=run();
const T=tableRows(H);

// ── 1. every row, in the order asked for ───────────────────────────────────
// Revised 2026-10-29: the webinar stages moved to their own box, and the three
// demo-date rows (Regular Demos Scheduled / Supposed to Happen / Qualified) came off.
ck('the rows, in order',T.map(r=>r.label),
   ['Visitors','Demo clicks','Appointment Scheduled','Demo Happened','Closed Won']);
// Decision Maker Brought-In and Contract Sent were dropped (Alex, 2026-10-29): unused.
ck('Decision Maker and Contract Sent are gone',/Decision Maker|Contract Sent/.test(H),false);
ck('no sub-cohort or parallel rows remain in the table',T.filter(r=>r.sub||r.par).length,0);
// One hairline, where web traffic hands over to the pipeline.
ck('a single hand-off line, above Appointment Scheduled',
   H.split('gap:12px;align-items:center;padding:').slice(1).map((c,i)=>c.includes('border-top:2px solid')?i:-1).filter(i=>i>=0),[2]);

// ── 2. the traffic rows ────────────────────────────────────────────────────
ck('Visitors and Demo clicks read the page figures',[row(H,'Visitors').reached,row(H,'Demo clicks').reached],[nf(10000),nf(1000)]);
ck('  Demo clicks keeps its step rate',row(H,'Demo clicks').conv,'10.0%');
ck('  and they have no Here now',[row(H,'Visitors').here,row(H,'Demo clicks').here],['—','—']);

// ── 3. the deal stages: the worker's cohort, as sent ───────────────────────
ck('deal rows show Reached and Here now from the cohort',
   ['Appointment Scheduled','Demo Happened','Closed Won']
     .map(l=>[row(H,l).reached,row(H,l).here]),
   [['80','30'],['50','20'],['12','11']]);
ck('their from-prior is the worker’s num ÷ den',
   ['Demo Happened','Closed Won'].map(l=>row(H,l).conv),
   ['62.5%','12.0%']);
ck('Appointment Scheduled is divided by Demo clicks',row(H,'Appointment Scheduled').conv,'8.0%');
ck('every rate says how it was worked out on hover',
   [row(H,'Demo clicks').tip,row(H,'Appointment Scheduled').tip,row(H,'Demo Happened').tip,row(H,'Closed Won').tip],
   ['10.0% = 1,000 demo clicks ÷ 10,000 visitors',
    '8.0% = 80 deals reached Appointment Scheduled ÷ 1,000 demo clicks',
    '62.5% = 50 of the 80 deals that reached Appointment Scheduled also reached Demo Happened',
    '12.0% = 6 of the 50 deals that reached Demo Happened also reached Closed Won']);
ck('every rate names its base, and each is the row directly above',T.filter(r=>r.conv!=='—').map(r=>[r.label,r.base]),
   [['Demo clicks','of visitors'],['Appointment Scheduled','of demo clicks'],['Demo Happened','of appt. scheduled'],
    ['Closed Won','of demo happened']]);
ck('the bar’s solid part is Here now ÷ Reached',[row(H,'Closed Won').fill,row(H,'Appointment Scheduled').fill],[11/12*100,30/80*100]);
// $100 and up rounds to whole dollars, as on the Clinician table.
ck('cost per is ad spend ÷ reached',[row(H,'Closed Won').cost,row(H,'Visitors').cost],['$167','$0.20']);
{
  // Deal rows compare against the PRIOR window's cohort (fixture prior = half).
  const seg=h=>{const i=h.indexOf('>Closed Won</span>');return h.slice(i,i+3000);};
  ck('deal rows compare with the prior window’s cohort',seg(H).includes(ctx.fDelta(12,6,null)),true);
  const z=run({spend:1000,bsf:(()=>{const b=BSF(false);b.cur.stages[SID.won].reached=0;b.cur.stages[SID.won].here=0;return b;})()});
  ck('a row nobody reached has no cost per, not $Infinity',row(z,'Closed Won').cost,'—');
}
ck('the header names the cohort, its size, and that the pre-launch test rides the deal tier',
   /Sales-pipeline deals created in this window \(120 deals\), pre-launch excluded on the deal’s traffic tier/.test(H),true);

// The webinar route, in its own box beside the table.
{
  const i=H.indexOf('Webinar route'),box=H.slice(i,H.indexOf('CVR \u00b7 Regular',i));
  ck('the webinar stages have their own box',i>0,true);
  ck('  with Reached and Here now for each',
     [/>Webinar Registered<\/div>[\s\S]*?>25<\/span>[\s\S]*?>5<\/b> here now/.test(box),/>Webinar Happened<\/div>[\s\S]*?>20<\/span>[\s\S]*?>15<\/b> here now/.test(box)],[true,true]);
  ck('  Webinar Happened’s rate is of the deals that registered',/>80\.0% of registered</.test(box),true);
  ck('  with its working on hover',/data-tip="80\.0% = 20 of the 25 deals that registered also attended"/.test(box),true);
  ck('  and its own deltas, against the prior cohort',[box.split('vs prior').length-1,box.includes(ctx.fDelta(25,13,null))],[2,true]);
  ck('Registered’s grey stays grey, not hue-shifted to red',/color:#6B7280">5<\/b>/.test(box),true);
}

// The won deals outside Closed Won's rate -- read off the payload.
ck('the note says which won deals sit outside the rate, with the payload\u2019s counts',
   [/Closed Won\u2019s 12 includes deals outside its rate/.test(H),/5 have no recorded demo or webinar \(2 with no other funnel stage on record\)/.test(H),
    /1 came through the webinar route/.test(H),/only counts the deals that reached Demo Happened/.test(H)],
   [true,true,true,true]);
{
  const b=BSF(false);b.cur.stages[SID.won].noDemo=0;b.cur.stages[SID.won].viaWebinar=0;
  const b2=BSF(false);b2.cur.stages[SID.won].noDemo=0;
  ck('  each line only when its own count is there',[/no recorded demo or webinar/.test(run({bsf:b2})),/1 came through the webinar route/.test(run({bsf:b2}))],[false,true]);
  ck('  and the note is left out when there are none',/includes deals outside its rate/.test(run({bsf:b})),false);
}
// Off the path.
{
  const chips=[...H.matchAll(/font-weight:600;color:var\(--navy\)">([^<]+)<\/span><span[^>]*>([^<]*)<\/span><span[^>]*>reached \u00b7 ([^<]*) here now/g)].map(m=>[m[1],m[2],m[3]]);
  ck('off-path stages sit in their own box with reached and here now',chips,
     [['Interested, Circle Back Later','3','3'],['No Show','9','4'],['Not a fit','0','0'],['Closed Lost','7','7'],['Churned','1','1']]);
  ck('  with No Show split by route',/reached \u00b7 4 here now \u00b7 7 webinar, 2 demo</.test(H),true);
  ck('  and the box says its deals also count above',/also counted above wherever they reached a stage/.test(H),true);
}

// ── 4. a single delta renderer, and it SUPPRESSES vs LM ────────────────────
ck('fDelta renders vs prior always',/vs prior/.test(ctx.fDelta(10,8,null,'#')),true);
ck('and vs LM only when there is a last-month value',/vs LM/.test(ctx.fDelta(10,8,null,'#')),false);
ck('a present last-month value brings the row back',/vs LM/.test(ctx.fDelta(10,8,6,'#')),true);
ck('a last-month ZERO still renders',/vs LM/.test(ctx.fDelta(10,8,0,'#')),true);
ck('a missing current value is an em-dash, not NaN',
   /—/.test(ctx.fDelta(null,8,6,'#'))&&!/NaN/.test(ctx.fDelta(null,8,6,'#')),true);
{
  const withLM=run({lm:true});
  // 5 rows + 2 webinar figures + 3 CVR chips all route through the ONE renderer.
  ck('with no LM data no row shows vs LM',H.split('vs LM').length-1,0);
  ck('with LM data every row, webinar figure and CVR chip shows one',withLM.split('vs LM').length-1,10);
  // +1: the column header itself reads "vs prior \u00b7 LM".
  ck('vs prior is unconditional on both',[H.split('vs prior').length-1,withLM.split('vs prior').length-1],[11,11]);
}
ck('every copy of fDelta gates the vs-LM row',
   (cli.match(/function fDelta\(/g)||[]).length,(cli.match(/if\(lmv!=null\)h\+=/g)||[]).length);
ck('the CVR chips use the same renderer as the rows',/\$\{fDelta\(v,pv,lmv,'%',2\)\}/.test(RAIL),true);

// ── 5. every step of the table is a Biggest-drop candidate ─────────────────
{
  // Fixture rates: clicks 10%, appt 8% of clicks, demo 62.5%, won 12%.
  ck('the worst step is named, with its losing share',biggest(H),['Demo clicks → Appointment Scheduled','92.0% leave']);
  const b=BSF(false);b.cur.stages[SID.demo].fromPrevPct=3;
  ck('  whichever required step it is',biggest(run({bsf:b})),['Appointment Scheduled → Demo Happened','97.0% leave']);
  // Closed Won is left out: in a young cohort most held demos are still open,
  // so its low rate is a deal cycle in progress, not a leak.
  const c=BSF(false);c.cur.stages[SID.won].fromPrevPct=0;
  ck('  Closed Won is never named, even at 0%',biggest(run({bsf:c}))[0],'Demo clicks → Appointment Scheduled');
  const e=BSF(false);e.cur.stages[SID.demo].fromPrevPct=null;
  ck('  a row with no rate is left out, not read as a 100% drop',biggest(run({bsf:e}))[0],'Demo clicks → Appointment Scheduled');
}

// ── 6. empty / missing data degrades to dashes, never NaN ──────────────────
{
  const z=run({visitors:0,clicks:0,reg:0,pre:0,supposed:0,qual:0,spend:0,bsf:null});
  ck('with no traffic and no stages every rate is an em-dash',T.map(r=>r.label).map(l=>row(z,l).conv),
     ['—','—','—','—','—']);
  ck('and nothing renders NaN',/NaN|Infinity/.test(z),false);
  ck('Biggest drop is omitted rather than guessed',biggest(z),null);
  ck('end-to-end falls back to an em-dash',e2e(z),'—');
}
{
  const m=run({bsf:null});
  ck('with no deal-stage payload the deal rows are dashes and say so',
     [row(m,'Closed Won').reached,row(m,'Closed Won').here,row(m,'Demo Happened').conv,/Deal stages did not load/.test(m),/NaN/.test(m)],
     ['—','—','—',true,false]);
  ck('  while the traffic rows are untouched',row(m,'Demo clicks').reached,nf(1000));
  ck('  and end-to-end is a dash, not 0.00%',e2e(m),'\u2014');
}
// Closed Won 12 of 10,000 visitors.
ck('end-to-end is the cohort’s Closed Won over visitors, to two decimals',e2e(H),'0.12%');
ck('  and says so',/visitor → closed won/.test(H),true);

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
ck('Qualified Demos stays one derivation (Demo Insights), and the funnel no longer reads it',
   [/const qd=\(qmeta\.qualDemoCount!=null\?qmeta\.qualDemoCount:/.test(cli),/\bqd\b/.test(RAIL)],[true,false]);
ck('and the worker excludes pre-launch when counting it',
   /if \(demoDidHappen\(att, rawSt\) && !_isPreLaunchDeal\) \{/.test(src),true);
ck('with Webinars Happened folded in additively',/pl\.qualifiedRawCount = \(pl\.qualifiedRawCount \|\| 0\) \+ n;/.test(src),true);

// ── 9. wiring ──────────────────────────────────────────────────────────────
ck('the table renders into the Conversion Funnel container',ctx.__id,'t-funnel');
ck('which the static page provides under that section header',
   /id="dsec-cvrfunnel">Conversion Funnel<\/div><div id="t-funnel"><\/div>/.test(cli),true);

// ── the losses, now in the strip under the table (2026-10-29) ──────────────
{
  const losses=h=>{const i=h.indexOf('Lost before the demo');const seg=h.slice(i);
    return [...seg.matchAll(/<span style="color:#B91C1C[^"]*">([^<]*)<\/span>\s*<span[^>]*>([\s\S]*?)<\/span><\/div>(?:<div[^>]*>([^<]*)<\/div>)?/g)]
      .map(m=>[m[1],m[2].replace(/<[^>]+>/g,''),m[3]||null]);};
  ck('each loss shows its count, its rate and what the rate is of',losses(H),
     [['Prune','6 \u00b7 7.5%','of 80 settled demos'],
      ['Demo no-show','4 \u00b7 5.4%','of 74 held + no-show'],
      ['Webinar no-show','62 \u00b7 43.4%','of 143 registrants']]);
  ck('  the same counts the Quality card shows, not the pre-launch-inclusive ones',
     /value:fN\(cancelledN\)|value:fN\(noShowN\)|qmeta\.webinarRegistrants\|\|0/.test(RAIL),false);
  ck('a missing rate drops the % rather than printing 0% or NaN',
     losses(run({loss:{wnsC:null,wnrC:null,wnsRate:null}}))[2],['Webinar no-show','\u2014','of \u2014 registrants']);
}

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
