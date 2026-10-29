// 2026-10-29: "% of regular demos that are 0-10K" (second line of the Pre-Launch
// % tile + a Brand Trend measure) and the webinar no-show breakout by brand size.
// RUN, not grepped: what matters is which contacts land in which count.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. Worker: the count, inside the regular cohort ─────────────────────────
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{is0to10KTier,processScheduledContacts,buildBrandTrend});',W);
ck('the tier test matches the 0-10K option, any casing',
   ['0-10K monthly web visitors','0-10k MONTHLY','10-25K monthly web visitors','Pre-launch / just launching','Very Small','',null].map(W.is0to10KTier),
   [true,true,false,false,false,false,false]);
let n=0;
const C=(day,wt,extra)=>({id:'c'+(n++),properties:Object.assign({createdate:day+'T18:00:00Z',average_monthly_web_traffic:wt},extra||{})});
const contacts=[
  C('2026-10-01','0-10K monthly web visitors'),
  C('2026-10-01','0-10K monthly web visitors',{hs_object_source_detail_1:'Livestorm Webinars'}), // webinar registrant: still regular
  C('2026-10-02','50-100K monthly web visitors'),
  C('2026-10-02','Pre-launch / just launching'),  // pre-launch: in neither count
  C('2026-10-02',''),
];
{
  const r=W.processScheduledContacts(contacts);
  ck('0-10K counted inside the regular total, webinar registrants included',
     [r.regular0to10K,r.regularTotal,r.preTotal],[2,4,1]);
  ck('  and a repeated contact counts once',
     W.processScheduledContacts([...contacts,contacts[0]]).regular0to10K,2);
}
{
  const t=W.buildBrandTrend(contacts,[],'2026-10-01','2026-10-03');
  const by={};t.day.forEach(p=>by[p.wk]=p);
  ck('the trend buckets carry the same count, per day',
     [[by['2026-10-01'].regular,by['2026-10-01'].regular10k],[by['2026-10-02'].regular,by['2026-10-02'].regular10k],[by['2026-10-03'].regular10k]],
     [[2,2],[2,0],[0]]);
}
ck('the payload ships current, prior and LM',
   [/scheduledRegular0to10K: c\.scheduled\.regular0to10K \?\? null/,/scheduledRegular0to10KPrior: prior \? \(p\.scheduled\?\.regular0to10K/,
    /scheduledRegular0to10KLastMonth: priorMonth \? \(pm\.scheduled\?\.regular0to10K/].map(re=>re.test(src)),[true,true,true]);

// The count shipped blank on the first deploy: processPeriod copies the
// scheduled result through an explicit allow-list (scheduledOut) and the new
// field was not on it. Guard EVERY field, not just this one.
{
  const out=/const scheduledOut = \{([\s\S]*?)\n  \};/.exec(src)[1].replace(/\/\/.*$/gm,'');
  const listed=new Set([...out.matchAll(/(\w+):\s*scheduled\.(\w+)/g)].filter(m=>m[1]===m[2]).map(m=>m[1]));
  const read=[...new Set([...src.matchAll(/\b(?:c|p|pm)\.scheduled\??\.(\w+)/g)].map(m=>m[1]))];
  ck('every scheduled field the response reads is on the scheduledOut allow-list',read.filter(k=>!listed.has(k)),[]);
  ck('  including the 0-10K count',listed.has('regular0to10K'),true);
}

// ── 2. Worker: webinar registrants split by brand size ──────────────────────
{
  const body=fn(src,'fetchWebinarDateDealIds');
  const loop=/const byTier = [\s\S]*?\n  \}\n/.exec(body)[0];
  const T={is0to10KTier:W.is0to10KTier,String,Set};vm.createContext(T);
  const R=(id,wt,att)=>({id,properties:{average_monthly_web_traffic:wt,webinar_has_attended:att}});
  vm.runInContext('globalThis.run=function(contacts){'+loop+'return byTier;}',T);
  const bt=T.run([R(1,'Pre-launch / just launching','true'),R(2,'pre-launch / just launching',''),
    R(3,'0-10K monthly web visitors','false'),R(4,'0-10K monthly web visitors','TRUE'),R(4,'0-10K monthly web visitors','TRUE'),
    R(5,'25-50K monthly web visitors','true'),R(6,'Very Small','true'),R(7,'',null)]);
  ck('every registrant lands in one size, pre-launch included, deduped',JSON.parse(JSON.stringify(bt)),
     {pre:{reg:2,att:1},k0to10:{reg:2,att:1},k10plus:{reg:1,att:1},unknown:{reg:2,att:1}});
  ck('the split rides the result and the payload, with prior and LM',
     [/return \{ dealIds: active,[^}]*byTier \}/.test(body),/pl\.webinarByTier = res\.byTier/.test(src),
      /webinarByTierPrior: p\.pipeline\?\.webinarByTier/.test(src),/webinarByTierLastMonth: pm\.pipeline\?\.webinarByTier/.test(src)],
     [true,true,true,true]);
  ck('  while the Regular funnel\u2019s registrant count still leaves pre-launch out',
     /if \(String\(c\.properties\?\.average_monthly_web_traffic \|\| ''\)\.toLowerCase\(\)\.includes\('pre-launch'\)\) continue;\n    seen\.set/.test(body),true);
}
// The card's headline: ALL registrants (Alex, 2026-10-29), summed from the split.
{
  const A={console:{log(){},warn(){},error(){}}};vm.createContext(A);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
    +'\nObject.assign(globalThis,{applyWebinarDateDeals,webinarNoShowRateAll,webinarTierTotals});',A);
  const bt={pre:{reg:66,att:39},k0to10:{reg:45,att:22},k10plus:{reg:10,att:5},unknown:{reg:22,att:15}};
  const per={pipeline:{extendedDealIds:new Set(),supposedDealIds:new Set(),totalExtended:0}};
  A.applyWebinarDateDeals(per,{dealIds:new Set(),registrants:77,attended:42,byTier:bt});
  const pl=per.pipeline;
  ck('the headline counts every registrant, pre-launch included',[pl.webinarRegistrantsAll,pl.webinarAttendedAll],[143,81]);
  ck('  and is the sum of the by-size rows',+A.webinarNoShowRateAll(pl).toFixed(2),+((143-81)/143*100).toFixed(2));
  ck('  while the Regular funnel keeps its pre-launch-free count',[pl.webinarRegistrants,pl.webinarNoShow],[77,35]);
  ck('no registrants, or no split, is a dash, not 0%',
     [A.webinarNoShowRateAll({webinarRegistrantsAll:0}),A.webinarNoShowRateAll({}),A.webinarNoShowRateAll(undefined),A.webinarTierTotals(null)].map(v=>v===null),[true,true,true,true]);
  ck('the payload carries it for all three windows',
     [/webinarNoShowRateAll: webinarNoShowRateAll\(c\.pipeline\)/,/webinarNoShowRateAllPrior: webinarNoShowRateAll\(p\.pipeline\)/,
      /webinarNoShowRateAllLastMonth: webinarNoShowRateAll\(pm\.pipeline\)/,/webinarRegistrantsAll: c\.pipeline\.webinarRegistrantsAll \?\? null/].map(re=>re.test(src)),
     [true,true,true,true]);
}

// ── 3. The Pre-Launch % tile's second line ──────────────────────────────────
const ctx={console,f$:v=>'$'+Math.round(v).toLocaleString(),fN:v=>Number(v||0).toLocaleString()};
vm.createContext(ctx);
vm.runInContext([
  'var window=this;var f$=this.f$,fN=this.fN;var S={dashboard:{win:'+"'mtd'"+'}};',
  'function _irTileFooterDash(){return "";}function _buildRevenueOutcomeCard(){return "";}',
  decl(cli,'const _KM_LBL=',';'), fn(cli,'_kmTile'), fn(cli,'_kmPaceBar'),
  fn(cli,'_dashKmModel'), fn(cli,'_brandKeyMetricsHTML'),
  'Object.assign(globalThis,{_dashKmModel,_brandKeyMetricsHTML});',
].join('\n'),ctx);
const D=dt=>({executiveSummary:{_meta:{totalSpend:1000,pTotalSpend:900,pmTotalSpend:1100,demosHeldCount:10},
   totalCpqd:{value:100},lowTrafficPct:{_meta:{}}},
 demoTracking:Object.assign({_qualMeta:{qualDemoCount:10},scheduledRegularTotal:80,scheduledRegularTotalPrior:50,scheduledRegularTotalLastMonth:100,
   scheduledPreTotal:20,scheduledPreTotalPrior:10,scheduledPreTotalLastMonth:25},dt),
 webPerformance:{visitors:{value:1}},adSpend:{channels:{},total:{spend:1000}}});
{
  const M=ctx._dashKmModel(D({scheduledRegular0to10K:30,scheduledRegular0to10KPrior:25,scheduledRegular0to10KLastMonth:20}));
  ck('the share is 0-10K over REGULAR demos, each window over its own',[M._k10Share,M._k10ShareP,M._k10ShareL],[37.5,50,20]);
  const h=ctx._brandKeyMetricsHTML(D({scheduledRegular0to10K:30,scheduledRegular0to10KPrior:25,scheduledRegular0to10KLastMonth:20}),{sfx:'',hideCpqd:false,win:'mtd'});
  const i=h.indexOf('0-10K % of Regular'), j=h.indexOf('Pre-Launch');
  ck('it renders inside the Pre-Launch tile, under its headline',[i>j&&j>0,/37\.5%/.test(h.slice(i,i+600)),/30 ÷ 80 regular demos/.test(h.slice(i,i+800))],[true,true,true]);
  const M0=ctx._dashKmModel(D({}));
  ck('a pre-v47 payload shows a dash, not 0% or NaN',[M0._k10Share===null,M0._k10ShareP===null],[true,true]);
}

// ── 4. The Trend measure ────────────────────────────────────────────────────
{
  const T={console,window:{},f$:v=>'$'+v,fN:v=>String(v)};vm.createContext(T);
  vm.runInContext(['var window=this;var f$=this.f$,fN=this.fN;',
    decl(cli,"const CLN_TR_GRAINS=[","];"),decl(cli,"const DSH_TR_GRAINS=","];"),decl(cli,"const DSH_TR_METRICS=[","];"),
    fn(cli,'_dshTrPopDef'),fn(cli,'_dshTrParts'),fn(cli,'_dshTrSum'),fn(cli,'_dshTrValue'),
    'Object.assign(globalThis,{_dshTrParts,_dshTrSum,_dshTrValue});'].join('\n'),T);
  const p={regular:40,regular10k:10,prelaunch:5,paid:8};
  ck('a point reads 0-10K ÷ regular',T._dshTrValue('k10share',T._dshTrParts(p,'regular')),25);
  ck('  whatever population is selected',T._dshTrValue('k10share',T._dshTrParts(p,'prelaunch')),25);
  ck('  a period with no regular demos is a gap, not 0%',T._dshTrValue('k10share',T._dshTrParts({regular:0,regular10k:0},'regular')),null);
  const sum=T._dshTrSum([T._dshTrParts(p,'regular'),T._dshTrParts({regular:60,regular10k:30},'regular')]);
  ck('  and a period total pools, it does not average',T._dshTrValue('k10share',sum),40);
}

// ── 5. Webinar no-show by brand size ────────────────────────────────────────
{
  const store={};
  const T={console,fN:v=>Number(v||0).toLocaleString(),document:{getElementById:id=>store[id]||null}};vm.createContext(T);
  vm.runInContext(['var window=this;var fN=this.fN;',
    decl(cli,'var WNS_TIERS=','];'),decl(cli,'window._wnsTierOpen=',';'),
    decl(cli,'window.toggleWnsTier=function','\n};'),fn(cli,'_webinarTierHTML'),
    'Object.assign(globalThis,{_webinarTierHTML});'].join('\n'),T);
  const qm={webinarByTier:{pre:{reg:66,att:39},k0to10:{reg:45,att:22},k10plus:{reg:10,att:5},unknown:{reg:22,att:15}},
    webinarByTierPrior:{pre:{reg:10,att:5},k0to10:{reg:0,att:0},k10plus:{reg:4,att:1},unknown:{reg:0,att:0}},
    webinarByTierLastMonth:{pre:{reg:10,att:5},k0to10:{reg:0,att:0},k10plus:{reg:4,att:1},unknown:{reg:0,att:0}}};
  const h=T._webinarTierHTML(qm);
  const rows=[...h.matchAll(/<tr style="border-top[^>]*>([\s\S]*?)<\/tr>/g)].map(m=>m[1].replace(/<[^>]+>/g,'|').split('|').filter(Boolean));
  ck('one row per size, with registered / no-shows / rate / LM -- no Prior column',rows,
     [['Pre-launch','66','27','40.9%','50.0%'],['0-10K','45','23','51.1%','—'],
      ['10K+','10','5','50.0%','75.0%'],['Tier not set','22','7','31.8%','—']]);
  ck('  and the header matches',[...h.matchAll(/<th[^>]*>([^<]*)<\/th>/g)].map(m=>m[1]),
     ['Brand size','Registered','No-shows','No-show rate','LM']);
  ck('a rate at or over 50% is flagged red, under it is not',
     [/#C0392B">51\.1%/.test(h),/#C0392B">50\.0%/.test(h),/#C0392B">40\.9%/.test(h)],[true,true,false]);
  ck('closed by default, behind a click',[/id="wns-tier" style="display:none/.test(h),/onclick="toggleWnsTier\(\)"/.test(h)],[true,true]);
  const box={style:{display:'none'}},car={textContent:''};store['wns-tier']=box;store['wns-tier-caret']=car;
  T.window.toggleWnsTier();
  ck('  the click opens it, and stays open across a re-render',[box.style.display,car.textContent,/id="wns-tier" style="display:;/.test(T._webinarTierHTML(qm))],['','▾',true]);
  ck('an older payload with no split shows nothing',T._webinarTierHTML({}),'');
  ck('the card hands it the qmeta it reads',/extra:_webinarTierHTML\(qmeta\)/.test(cli),true);
  ck('the card headline reads the ALL-registrant figures, for every window',
     /const _wnrC=qmeta\.webinarRegistrantsAll,_wnsC=qmeta\.webinarNoShowAll;\s*const _wnsRateC=qmeta\.webinarNoShowRateAll,_wnsRateP=qmeta\.webinarNoShowRateAllPrior,_wnsRateL=qmeta\.webinarNoShowRateAllLastMonth;/.test(cli),true);
  ck('  and says so, with no pre-launch exclusion in its definition',
     /statRow\('Webinar no-show rate','Registrants who did not attend \\u00f7 all registrants',/.test(cli),true);
  ck('the Regular funnel\u2019s webinar no-show keeps the pre-launch-free count',
     /const _wbNsN=qmeta\.webinarNoShow\|\|0;/.test(cli)&&/note:'of '\+fN\(qmeta\.webinarRegistrants\|\|0\)\+' registrants'/.test(cli),true);
}

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
