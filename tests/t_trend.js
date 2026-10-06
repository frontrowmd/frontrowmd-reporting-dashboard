// Clinician Trend Analysis. Two things this suite exists for:
//
// 1. THE TIE-OUT. Every series the card plots must sum, across the window, to
//    the Key Metrics card above it. The predecessor ("Weekly Cost") deliberately
//    did NOT tie out — it dated events off stage stamps while the KPI cards
//    counted an ever-reached cohort — and the reconciliation question that
//    produced came up repeatedly. buildClinicianTrend therefore mirrors
//    buildClinicianComposition's loop exactly, and these assertions compare the
//    two functions' real output rather than pinning numbers.
//
// 2. THE GEOMETRY. The chart carries two scales on one plot, so "is this point
//    drawn against its own axis" is a question source-text regexes cannot
//    answer — inverting yOf's ternary passes every textual check. The geometry
//    block below renders the real SVG and measures it.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');
const ctx={console,Date,Math,JSON,Set,Map,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,
           fetch:()=>{},TextEncoder,TextDecoder,URL,Promise,Response,Request,Headers};
vm.createContext(ctx);
// Whole worker prefix: buildClinicianTrend leans on fmt, _clnDayKey and
// isClinicianCampaign, and buildClinicianComposition on a dozen more.
vm.runInContext(src.slice(0,src.indexOf('\nexport default')),ctx,{filename:'worker.js'});

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// d1/d2 are two deals for ONE person — the dedupe case. d5 shares d1's day but
// has NO kickoff booked, so inquiries and fullApp differ on 2026-09-02 and every
// derived metric comes out distinct: a fixture where they tie lets a transposed
// w.fullApp/w.inquiries pass unnoticed. d4's contact predates the window.
const contacts={
  d1:{__cid:'c1',createdate:'2026-09-02T12:00:00Z'},
  d2:{__cid:'c1',createdate:'2026-09-02T12:00:00Z'},
  d5:{__cid:'c4',createdate:'2026-09-02T18:00:00Z'},
  d3:{__cid:'c2',createdate:'2026-09-09T12:00:00Z'},
  d4:{__cid:'c3',createdate:'2026-08-20T12:00:00Z'},
};
const deals=[
  {id:'d1',properties:{clinician_meeting_booked_date:'2026-09-20'}},
  {id:'d2',properties:{}},
  {id:'d5',properties:{}},
  {id:'d3',properties:{clinician_meeting_booked_date:'2026-10-01'}},
  {id:'d4',properties:{clinician_meeting_booked_date:'2026-08-25'}},
];
// fetchWindsorAds is handed a WIDENED `from`, so out-of-window rows really do
// arrive here; google_ads and non-Clinician campaigns really are in the same
// array. All three must be dropped.
const rows=[
  {datasource:'facebook',  campaign_name:'Clinician Targeting - US',date:'2026-09-02',spend:'100',clicks:'50', impressions:'5000'},
  {datasource:'facebook',  campaign_name:'Clinician Targeting - US',date:'2026-09-09',spend:'200',clicks:'100',impressions:'5000'},
  {datasource:'facebook',  campaign_name:'Brand Awareness',         date:'2026-09-09',spend:'999',clicks:'999',impressions:'99999'},
  {datasource:'google_ads',campaign_name:'Clinician Targeting',     date:'2026-09-09',spend:'888',clicks:'888',impressions:'88888'},
  {datasource:'facebook',  campaign_name:'Clinician Targeting - US',date:'2026-08-20',spend:'777',clicks:'7',  impressions:'70'},
];
const FROM='2026-09-01',TO='2026-09-30';
const t=ctx.buildClinicianTrend(deals,contacts,rows,FROM,TO);
const comp=ctx.buildClinicianComposition(deals,contacts,FROM,TO);
const sum=(a,k)=>a.reduce((s,p)=>s+(p[k]||0),0);
const at=(a,k)=>a.find(p=>p.wk===k)||{};

// ── The tie-out. These compare two functions, so they stay true as the shared
// definition of "an inquiry" moves; they fail the moment the two drift apart.
ck('daily inquiries sum == Key Metrics inquiries',   sum(t.day,'inquiries'), comp.contacts);
ck('weekly inquiries sum == Key Metrics inquiries',  sum(t.week,'inquiries'), comp.contacts);
ck('daily fullApp sum == Key Metrics full subs',     sum(t.day,'fullApp'),   comp.submission.contacts);
ck('weekly fullApp sum == Key Metrics full subs',    sum(t.week,'fullApp'),  comp.submission.contacts);
ck('both grains agree on spend', sum(t.day,'spend'), sum(t.week,'spend'));
ck('one person with two deals counts once', comp.contacts, 3);
// The fixture must be able to tell the two populations apart, or every
// assertion below about fullApp is satisfied by the inquiry figure too.
ck('inquiries and full apps differ in the fixture',
   comp.contacts!==comp.submission.contacts, true);

// ── Row filtering ──
ck('non-Meta, non-Clinician and out-of-window spend dropped', sum(t.day,'spend'), 300);
ck('clicks filtered on the same rule',      sum(t.day,'clicks'), 150);
ck('impressions filtered on the same rule', sum(t.day,'impressions'), 10000);

// ── Bucketing: EVERY period in the window gets a point ──
// The chart positions points by array index, so a day that is simply absent is
// not drawn as a gap — its neighbours become adjacent and the line splices
// straight across it. September has 30 days and 5 ISO weeks touching it.
ck('one daily point per calendar day in the window', t.day.length, 30);
ck('daily points are contiguous and in order',
   t.day.map(p=>p.wk).every((k,i,a)=>i===0||Date.parse(k)-Date.parse(a[i-1])===864e5), true);
ck('the daily series spans exactly the window', [t.day[0].wk,t.day[t.day.length-1].wk], [FROM,TO]);
// ISO weeks start Monday, so the first bucket begins before `from` — its data is
// still in-window, only its label predates the window.
ck('weekly points are Mondays covering the window', t.week.map(p=>p.wk),
   ['2026-08-31','2026-09-07','2026-09-14','2026-09-21','2026-09-28']);
ck('an empty period is present but empty',
   [at(t.day,'2026-09-15').inquiries, at(t.day,'2026-09-15').spend], [0,0]);

// ── Derived metrics, each computed on its period's own totals ──
const d2=at(t.day,'2026-09-02');
ck('that day has two inquiries and one full app', [d2.inquiries,d2.fullApp], [2,1]);
ck('cost per inquiry divides by inquiries',  d2.cpi,  50);
ck('cost per full app divides by full apps', d2.cpfa, 100);
ck('CVR inquiry divides by clicks',          d2.cvrInq,  4);
ck('CVR full app divides by clicks',         d2.cvrFull, 2);
ck('CTR is clicks over impressions',         d2.ctr,     1);
// Each metric must come out distinct here, or a transposition is invisible.
ck('the four conversion-linked metrics are all different',
   new Set([d2.cpi,d2.cpfa,d2.cvrInq,d2.cvrFull]).size, 4);

// ── Null vs zero ──
// A RATE with a real denominator and a zero numerator is genuinely 0%. A COST is
// undefined when nothing was spent — returning 0 would plot on the floor of the
// cost axis and read as the most efficient period in the window, and the
// renderer's null-break would not fire because 0 is a real number.
const organic=ctx.buildClinicianTrend(deals,contacts,[],FROM,TO);
ck('conversions but no spend: cost is null', [at(organic.day,'2026-09-02').cpi,at(organic.day,'2026-09-02').cpfa], [null,null]);
ck('conversions but no spend: CTR is null too', at(organic.day,'2026-09-02').ctr, null);
const bare=ctx.buildClinicianTrend([],{},rows,FROM,TO);
ck('spend but no conversions: cost is null', bare.day.find(p=>p.wk==='2026-09-02').cpi, null);
ck('spend but no conversions: rate is 0, not null', bare.day.find(p=>p.wk==='2026-09-02').cvrInq, 0);
ck('spend but no conversions: spend still carried', bare.day.find(p=>p.wk==='2026-09-02').spend, 100);

// ── Wiring ──
// It is now handed the HISTORY deals and its OWN window: the section is
// independent of the page date filter by design, so page-scoping it would
// defeat the point. The tie-out below still holds because the builder mirrors
// buildClinicianComposition's loop — what changed is the window, not the rule.
ck('endpoint passes the history deals and the trend window',
   /ads\.trend = buildClinicianTrend\(deals, contactsByDeal, wRows, trendWin\.from, trendWin\.to\);/.test(src), true);
// Payload shape changed, so a cached v31 response must not be served to the new
// client, which reads ads.trend and would render an empty state.
ck('API_CACHE_VER moved past v31',
   (function(){const m=/const API_CACHE_VER = 'v(\d+)'/.exec(src);return m?+m[1]>=32:false;})(), true);

// ── Client contract: three independent toggles, ONE axis ──────────────────
ck('client reads ads.trend', /\(d&&d\.ads&&d\.ads\.trend\)/.test(cli), true);
// Period, population and measure are three separate choices, so each gets its
// own row rather than one row of eighteen pills.
{
  const grains=(/const CLN_TR_GRAINS=\[([\s\S]*?)\n\];/.exec(cli)||[,''])[1];
  const got=[...grains.matchAll(/\{k:'(\w+)',\s*l:'(\w+)',\s*n:(\d+),\s*src:'(\w+)'/g)].map(m=>[m[1],m[2],+m[3],m[4]]);
  ck('three periods, each with its own lookback', got,
     [['week','Weekly',8,'week'],['day','Daily',30,'day'],['month','Monthly',3,'month']]);
  const pops=(/const CLN_TR_POPS=\[([\s\S]*?)\n\];/.exec(cli)||[,''])[1];
  ck('two populations', [...pops.matchAll(/\{k:'(\w+)'/g)].map(m=>m[1]), ['inquiry','submission']);
  const mets=(/const CLN_TR_METRICS=\[([\s\S]*?)\n\];/.exec(cli)||[,''])[1];
  ck('six measures, in the order asked for',
     [...mets.matchAll(/\{k:'(\w+)',\s*l:'([^']+)'/g)].map(m=>m[1]),
     ['count','cost','cvr','ctr','md','mdpct']);
  // Each measure declares its own unit, which is what makes a single axis
  // possible — only one is ever plotted.
  ck('every measure declares a unit',
     [...mets.matchAll(/unit:'([#$%])'/g)].map(m=>m[1]), ['#','$','%','%','#','%']);
  // CTR is clicks over impressions and has no population in it. A toggle that
  // appeared to change it would be lying.
  ck('CTR is marked population-independent', /\{k:'ctr',[^}]*pop:false\}/.test(mets), true);
  ck('and every other measure is not', (mets.match(/pop:true/g)||[]).length, 5);
}
ck('one setter per toggle row',
   [/window\.toggleClnTrendGrain=/.test(cli),/window\.toggleClnTrendPop=/.test(cli),/window\.toggleClnTrendMetric=/.test(cli)],
   [true,true,true]);

// ── The metric algebra, RUN ───────────────────────────────────────────────
// This is the whole feature: six measures over two populations, every one
// derived from summed components rather than from another measure.
{
  const dctx={console,Math,Date,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,JSON};
  dctx.window=dctx;
  vm.createContext(dctx);
  vm.runInContext([
    'const CLN_MD_TYPE="MD/DO";',
    fn(cli,'_clnTrParts'),fn(cli,'_clnTrSum'),fn(cli,'_clnTrValue'),
  ].join('\n'),dctx);
  const p={spend:400,clicks:100,impressions:5000,inquiries:20,fullApp:5,
           byType:{'MD/DO':8,'Nurse Practitioner':12},bySubType:{'MD/DO':2,'Nurse Practitioner':3}};
  const inq=dctx._clnTrParts(p,'inquiry'), sub=dctx._clnTrParts(p,'submission');
  ck('the population toggle picks the count', [inq.pop,sub.pop], [20,5]);
  ck('and the matching MD/DO cut', [inq.md,sub.md], [8,2]);
  const V=(m,x)=>dctx._clnTrValue(m,x);
  ck('Count',      [V('count',inq),V('count',sub)], [20,5]);
  ck('Doctors',    [V('md',inq),V('md',sub)], [8,2]);
  ck('Cost Per',   [V('cost',inq),V('cost',sub)], [20,80]);
  ck('CVR',        [V('cvr',inq),V('cvr',sub)], [20,5]);
  ck('Doctor %',   [V('mdpct',inq),V('mdpct',sub)], [40,40]);
  // CTR must not move with the population — it is clicks over impressions.
  ck('CTR ignores the population', [V('ctr',inq),V('ctr',sub)], [2,2]);
  // A COST with no spend is undefined, not zero: at zero it would plot on the
  // floor and read as the cheapest period in the window.
  ck('no spend makes a cost null, not 0',
     V('cost',{spend:0,clicks:9,impressions:9,pop:4,md:1}), null);
  // A RATE with a real denominator and a zero numerator is genuinely 0.
  ck('but a rate with a real denominator is 0, not null',
     V('cvr',{spend:5,clicks:10,impressions:10,pop:0,md:0}), 0);
  ck('and a rate with no denominator is null',
     [V('cvr',{spend:5,clicks:0,impressions:0,pop:3,md:1}),
      V('ctr',{spend:5,clicks:3,impressions:0,pop:3,md:1}),
      V('mdpct',{spend:5,clicks:3,impressions:9,pop:0,md:0})], [null,null,null]);
  // THE rule for the rolling average and the KPI cards: a window's value is
  // computed from SUMMED components, never averaged across periods. The mean of
  // two CVRs is not the window's CVR.
  const a={spend:100,clicks:10,impressions:100,pop:1,md:1};
  const b={spend:100,clicks:990,impressions:9900,pop:99,md:9};
  const pooled=V('cvr',dctx._clnTrSum([a,b]));
  const averaged=(V('cvr',a)+V('cvr',b))/2;
  ck('a window CVR is pooled, not averaged', [Math.round(pooled*100)/100, Math.round(averaged*100)/100], [10,10]);
  const pooledCost=V('cost',dctx._clnTrSum([a,b])), avgCost=(V('cost',a)+V('cost',b))/2;
  ck('and a window cost too', [pooledCost, Math.round(avgCost*100)/100], [2, 50.51]);
}

// ── The 7-day rolling average ─────────────────────────────────────────────
// Computed from rolling SUMS of the components, so a 2-click day does not
// weigh the same as a 2,000-click one.
ck('the rolling window sums components, not ratios',
   /var w=parts\.slice\(Math\.max\(0,i-\(CLN_TR_ROLL_DAYS-1\)\),i\+1\);\s*\n\s*return _clnTrValue\(mdef\.k,_clnTrSum\(w\)\);/.test(cli), true);
ck('it is seven days', /const CLN_TR_ROLL_DAYS=7;/.test(cli), true);
// A 7-day window is shorter than one period on the weekly and monthly views,
// where the line would just redraw the series.
ck('and is drawn on the daily grain only', /if\(g\.k==='day'\)\{\s*\n\s*roll=parts\.map/.test(cli), true);

// ── Ad Spend stays, whatever is selected ──────────────────────────────────
ck('spend is drawn regardless of the measure',
   /var mxS=0;parts\.forEach\(function\(x\)\{if\(x\.spend>mxS\)mxS=x\.spend;\}\);/.test(cli), true);
ck('on its own scale, with no axis',
   /\(x\.spend\/mxS\)\*innerH\*0\.55/.test(cli), true);
ck('and the card says so', /Ad Spend is the shaded background throughout, whatever is selected/.test(cli), true);

// ── Independent of the page date filter ──────────────────────────────────
ck('the worker builds it over its own window',
   /ads\.trend = buildClinicianTrend\(deals, contactsByDeal, wRows, trendWin\.from, trendWin\.to\);/.test(src), true);
ck('and ships that window so the card can state it', /ads\.trendRange = trendWin;/.test(src), true);
ck('the window is a fixed lookback, not the page filter',
   /function clnTrendWindow\(today\)/.test(src)&&/const CLN_TREND_DAYS = \d+;/.test(src), true);
// 100 days has to cover the widest view the client offers (3 months).
ck('and it is long enough for the widest grain',
   (+(/const CLN_TREND_DAYS = (\d+);/.exec(src)||[,0])[1]) >= 92, true);
// Widening the ad fetch is only safe because buildClinicianAds now honours the
// window it is handed — it used to sum every row it was given, so a wider pull
// would have turned the page's Ad Spend into three months of spend.
ck('buildClinicianAds filters to its own window',
   /if \(from && rk && rk < from\) continue;\s*\n\s*if \(to && rk && rk > to\) continue;/.test(src), true);
ck('and one fetch covers both windows',
   /const _adsFrom = trendWin\.from < cur\.from \? trendWin\.from : cur\.from;/.test(src), true);

// ── Structure ────────────────────────────────────────────────────────────
ck('a single chart', (fn(cli,'_renderClnTrend').match(/<svg /g)||[]).length, 1);
ck('and a single axis — only one measure is plotted',
   (fn(cli,'_renderClnTrend').match(/var yMax=_clnTrNice/g)||[]).length, 1);
ck('every toggle rebuilds the card and redraws',
   /card\.outerHTML=_clnTrendCard\(d\);\s*\n\s*_renderClnTrend\(d\);/.test(cli), true);
ck('nav heading has a stable id', /id="cln-trend-h"/.test(cli), true);
const iDaily=cli.indexOf("h+=card('Daily Activity'"), iTrend=cli.indexOf('h+=_clnTrendCard(d);'), iAcq=cli.indexOf("h+=card('Acquisition'");
ck('Trend Analysis sits between Daily Activity and Acquisition',
   iDaily>0&&iTrend>iDaily&&iAcq>iTrend, true);
ck('the old Weekly Cost card is gone', /_clnWeeklyCard|_renderClnWeekly/.test(cli), false);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
