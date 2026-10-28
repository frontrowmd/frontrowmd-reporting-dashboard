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
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
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
// Sign Up = Clinician Credentials Complete, dated by its OWN day (2026-10-27).
// c3 is the case that only the new definition counts: created in August, outside
// the window, credentials completed 15 Sep, inside it. c2 completes after the
// window closes, so it is an inquiry in September and a sign up in nobody's
// September. The deals still carry bookings to prove a booking is not a sign up.
const contacts={
  d1:{__cid:'c1',createdate:'2026-09-02T12:00:00Z',clinician_credentials_completed:'2026-09-02'},
  d2:{__cid:'c1',createdate:'2026-09-02T12:00:00Z',clinician_credentials_completed:'2026-09-02'},
  d5:{__cid:'c4',createdate:'2026-09-02T18:00:00Z'},
  d3:{__cid:'c2',createdate:'2026-09-09T12:00:00Z',clinician_credentials_completed:'2026-10-03'},
  d4:{__cid:'c3',createdate:'2026-08-20T12:00:00Z',clinician_credentials_completed:'2026-09-15'},
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
// The three behaviours the 2026-10-27 definition turns on.
ck('a sign up files under its credentials day, not its create day',
   [at(t.day,'2026-09-15').fullApp, at(t.day,'2026-09-15').inquiries], [1,0]);
ck('  so a contact created before the window still counts in it', comp.submission.contacts, 2);
ck('  and one completing after the window does not',
   t.day.some(p=>p.wk==='2026-09-09'&&p.fullApp>0), false);
ck('a booked call with no credentials is not a sign up',
   at(t.day,'2026-09-02').fullApp, 1);

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
     [['day','Daily',30,'day'],['week','Weekly',8,'week'],['month','Monthly',3,'month']]);
  // Reordering the pills must not move the default off Weekly.
  ck('  and Weekly remains the default grain',
     /_clnTrendGrain\(\)\{return window\._clnTrendGrainV\|\|'week';\}/.test(cli),true);
  ck('  picked by name, not by array position',
     /_trGrainDefault=function\(list\)\{\s*\n?\s*return list\.filter\(function\(g\)\{return g\.k==='week';\}\)\[0\]/.test(cli),true);
  ck('  so no lookup falls back to the first entry',
     /\|\|CLN_TR_GRAINS\[0\]|\|\|DSH_TR_GRAINS\[0\]/.test(cli),false);
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
   /var w=parts\.slice\(Math\.max\(0,i-\(CLN_TR_ROLL_DAYS-1\)\),i\+1\);\s*\n\s*var v=_clnTrValue\(mdef\.k,_clnTrSum\(w\)\);/.test(cli), true);
// ...and it is an AVERAGE. A count summed over seven days is a weekly TOTAL and
// would sit ~7x above the daily series it is drawn against. A RATIO pooled over
// the same seven days already IS the 7-day average rate, so dividing it by 7
// would be meaningless — only counts are divided.
ck('a count is divided by the days in the window',
   /if\(v!=null&&mdef\.unit==='#'\)v=v\/w\.length;/.test(cli), true);
ck('by w.length, not a hard 7, so the left edge is not understated',
   /v\/w\.length/.test(cli)&&!/v\/CLN_TR_ROLL_DAYS/.test(cli), true);
ck('and the legend calls it an average', /'-day average<\/span>'/.test(cli), true);
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
// The card now takes an id SUFFIX so the Executive Dashboard can host a second
// copy; pages share one DOM, so a fixed id would collide.
ck('every toggle rebuilds the card and redraws',
   /card\.outerHTML=_clnTrendCard\(d,sfx\);\s*\n\s*_renderClnTrend\(d,sfx\);/.test(cli), true);
ck('nav heading has a stable, host-scoped id', /id="cln-trend-h'\+sfx\+'"/.test(cli), true);
ck('  and the panel it draws into is scoped the same way',
   /id="cln-trend-panels'\+sfx\+'"/.test(cli)&&/getElementById\('cln-trend-panels'\+\(sfx\|\|''\)\)/.test(cli), true);
ck('the toggles carry the suffix back, so the right copy redraws',
   /toggleClnTrendGrain\('"\+x\.k\+"','"\+sfx\+"'\)/.test(cli), true);
// Trend Analysis now LEADS the page body: it answers "what is the shape right
// now" over its own fixed lookback, which is the question asked before the
// funnel's stage-by-stage breakdown. Key Metrics is still prepended after
// everything, so the rendered order is Key Metrics, Trend, Funnel.
{
  const iTrend=cli.indexOf('let h=_clnTrendCard(d);');
  const iFunnel=cli.indexOf("h+=card('Clinician Funnel'");
  const iAcq=cli.indexOf("h+=card('Acquisition'");
  ck('Trend Analysis opens the page body', iTrend>0, true);
  ck('above the Clinician Funnel', iTrend<iFunnel, true);
  ck('and above Acquisition', iFunnel<iAcq, true);
  // Key Metrics is PREPENDED, so it still comes first on screen.
  ck('with Key Metrics still prepended ahead of it',
     /\+_clnKeyMetricsHTML\(d,''\)\+h;/.test(cli), true);
}
ck('the Daily Activity section it used to follow is gone',
   /card\('Daily Activity'/.test(cli), false);
ck('the old Weekly Cost card is gone', /_clnWeeklyCard|_renderClnWeekly/.test(cli), false);

// ── One ordered stat list behind the cards AND the hover ──────────────────
// They used to build separate lists, which is how the hover repeated whichever
// stat was also the selected measure and carried a Clicks row the cards never
// had. Deriving both from CLN_TR_CARDS makes "same stats, same order, no
// duplicates" true by construction rather than by matching edits.
{
  const cardsSrc=(/const CLN_TR_CARDS=\[([\s\S]*?)\n\];/.exec(cli)||[,''])[1];
  ck('the stat row is declared once',
     [...cardsSrc.matchAll(/\{k:'(\w+)'/g)].map(m=>m[1]),
     ['count','cost','cvr','ctr','doctor','spend']);
  // Doctors is ONE entry carrying a count and a share: two readings of the same
  // cut, and splitting them put the same number on screen twice.
  ck('Doctors carries its count and its share together',
     /\{k:'doctor',\s*metric:'md',\s*unit:'#',\s*pop:true,\s*withPct:'mdpct'\}/.test(cardsSrc), true);
  // Ad Spend is a card but not a selectable measure — it is always the
  // background, so it always has a card.
  ck('Ad Spend is a card but not a measure',
     /\{k:'spend',\s*metric:null/.test(cardsSrc)
     && !/\{k:'spend'/.test((/const CLN_TR_METRICS=\[([\s\S]*?)\n\];/.exec(cli)||[,''])[1]), true);
  ck('the cards are rendered from it', /CLN_TR_CARDS\.map\(function\(card\)\{\s*\n\s*var st=_clnTrStat\(card,T,pdef\);/.test(cli), true);
  ck('and so is the hover', /var rows=CLN_TR_CARDS\.map\(function\(card\)\{/.test(cli), true);
  // Nothing may be pushed onto the readout outside that map, except the rolling
  // line — which is a chart series, not a card.
  const hoverZone=cli.slice(cli.indexOf('var rows=CLN_TR_CARDS.map'), cli.indexOf("var a=' data-wk=", cli.indexOf('var rows=CLN_TR_CARDS.map')));
  ck('the only extra readout row is the rolling line',
     (hoverZone.match(/rows\.push\(/g)||[]).length, 1);
  ck('and it is the rolling line', /rows\.push\(CLN_TR_ROLL_DAYS\+'-day avg/.test(hoverZone), true);
  ck('Clicks is no longer a readout row', /rows\.push\('Clicks\|/.test(cli), false);
  // The tooltip must cover six cards plus the rolling row, or the last row is
  // silently dropped.
  ck('the tooltip loops over every row the readout can emit',
     (+(/for\(var i=1;i<=(\d+);i\+\+\)/.exec(cli)||[,0])[1]) >= 7, true);
}

// ── _clnTrStat, RUN ───────────────────────────────────────────────────────
{
  const dctx={console,Math,Date,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,JSON};
  dctx.window=dctx;
  vm.createContext(dctx);
  vm.runInContext([
    'const f$=v=>v==null?"—":"$"+Math.round(v).toLocaleString();',
    'const fN=v=>v==null?"—":Math.round(v).toLocaleString();',
    // One slice: CLN_TR_CARDS, _clnTrStat and CLN_MD_TYPE all live inside this
    // range, so declaring any of them separately was a duplicate.
    cli.slice(cli.indexOf('const CLN_TR_METRICS=['), cli.indexOf('function _clnTrendGrain()')),
    fn(cli,'_clnTrValue'),fn(cli,'_clnTrFmt'),fn(cli,'_clnTrSum'),
    // `const` at the top level of a vm script does NOT become a property of the
    // context, so the bindings have to be handed out explicitly.
    'globalThis.__cards=CLN_TR_CARDS; globalThis.__stat=_clnTrStat;',
  ].join('\n'),dctx);
  const T={spend:400,clicks:100,impressions:5000,pop:20,md:8};
  const pdef={k:'inquiry',l:'Inquiry',w:'inquiries',short:'inquiries'};
  const got=dctx.__cards.map(c=>{const st=dctx.__stat(c,T,pdef);return [st.lbl,st.val];});
  ck('every card renders a value, in order', got,
     [['Count · Inquiry','20'],['Cost Per · Inquiry','$20'],['CVR · Inquiry','20.0%'],
      ['CTR','2.00%'],['MD/DO · Inquiry','8'],['Ad Spend','$400']]);
  // CTR carries no population in its label, because it has none in its maths.
  ck('CTR is the one card with no population in its label', got.filter(g=>/·/.test(g[0])).length, 4);
  // No stat may appear twice — that was the complaint.
  ck('no label repeats', new Set(got.map(g=>g[0])).size, got.length);
  // Doctors is a SPLIT stat: the count and the share are two readings of one
  // cut, so both are VALUES. The share used to sit in the caption, where it
  // read as working-out rather than as a figure.
  const doc=dctx.__stat(dctx.__cards[4],T,pdef);
  ck('the Doctors card splits count and share', [doc.val,doc.split,doc.calc],
     ['8','40.0%','of 20 inquiries']);
  // Only Doctors splits — every other stat has one value, and a stray split
  // would read as two measures on one row.
  ck('and nothing else does',
     dctx.__cards.map(c=>!!dctx.__stat(c,T,pdef).split),
     [false,false,false,false,true,false]);
  // A split must survive a zero denominator rather than printing NaN%.
  const doc0=dctx.__stat(dctx.__cards[4],{spend:0,clicks:0,impressions:0,pop:0,md:0},pdef);
  ck('with no population the share is a dash, not NaN', doc0.split, '\u2014');
}

// ── What the KPI figures cover, stated above them ─────────────────────────
// Three different spans are in play on this page at once — the page date
// filter, the worker's 100-day series, and this grain's own lookback — and the
// cards sum the LAST of those. Left unstated it is the page filter a reader
// would assume.
ck('a range band sits above the cards', /Figures below cover/.test(cli), true);
ck('and is emitted before them', /var minis=_trRangeBand\+'<div style="display:flex;gap:9px/.test(cli), true);
// It must describe the periods ON SCREEN, not the worker's full series.
ck('it is derived from the plotted points',
   /var startMs=msOf\(pts\[0\]\.wk\);/.test(cli)
   && /var endMs=Math\.min\(_clnTrEnd\(pts\[pts\.length-1\]\.wk,g\.k\)\.ms,todayMs\);/.test(cli), true);
// A week or month in progress must not advertise an end date in the future.
ck('the end is clamped to today', /Math\.min\(_clnTrEnd\([^)]*\)\.ms,todayMs\)/.test(cli), true);
// The lookback leads, and it is built from the periods ACTUALLY on screen.
// Saying both "8 weeks" and "last 8 weeks" was the same number twice; using the
// count rather than the grain's nominal label also keeps it honest when the
// series is shorter than the lookback.
ck('the lookback is the headline',
   /var lookback='Last '\+pts\.length\+' '\+unit\+\(pts\.length===1\?'':'s'\);/.test(cli), true);
ck('and it is the biggest text in the band',
   /font-size:16px;font-weight:700;color:var\(--navy\)">'\+lookback/.test(cli), true);
ck('with the date range as supporting text',
   /font-size:11\.5px;color:var\(--grey\)">'\+fmtD\(startMs,false\)/.test(cli), true);
ck('and the nominal grain label is no longer repeated beside it',
   /\+g\.note\+'<\/span>'/.test(cli), false);
ck('and says plainly it is not the page filter', /not the page date filter/.test(cli), true);

// ── Toggle captions removed ──────────────────────────────────────────────
// The pills name themselves; three stacked label columns were chrome.
{
  const cardFn=fn(cli,'_clnTrendCard');
  ck('seg() no longer takes a caption', /var seg=function\(inner\)\{/.test(cardFn), true);
  ck('and renders none', /Period<\/span>|Population<\/span>|Measure<\/span>/.test(cardFn), false);
  // The three groups must still be visually separate, or twelve pills read as
  // one strip and the three choices are no longer distinguishable.
  ck('the three groups stay separate', (cardFn.match(/\+seg\(/g)||[]).length, 3);
  ck('spaced apart on one row', /gap:18px;flex-wrap:wrap;margin:12px 0 4px 15px/.test(cardFn), true);
}

// ── The plotted measure leads the hover ───────────────────────────────────
// Order still follows the cards — only the emphasis moves. The row the chart is
// drawing is the reason the reader is hovering.
ck('the hover flags the selected row',
   /var isSel=\(card\.metric===mdef\.k\)\|\|\(card\.withPct&&card\.withPct===mdef\.k\);/.test(cli), true);
ck('and passes it through as a field', /\+\(isSel\?'1':''\)\+'\|'\+\(st\.split\|\|''\)/.test(cli), true);
// Selecting Doctor % must light the Doctors row, since that card carries both.
ck('Doctor % lights the Doctors row via withPct',
   /\{k:'doctor',\s*metric:'md',\s*unit:'#',\s*pop:true,\s*withPct:'mdpct'\}/.test(cli), true);
{
  const tip=cli.slice(cli.indexOf('function clnWkTip(e,el){'), cli.indexOf('function clnWkTipMove'));
  ck('the tooltip reads the flag', /var isPrimary=\(f\[5\]==='1'\);/.test(tip), true);
  // Prominence has to be more than colour: a bigger value, a stronger label and
  // a panel behind it.
  ck('and gives that row real weight',
     [/font-size:'\+\(isPrimary\?'18':'14'\)\+'px/.test(tip),
      /isPrimary\?';font-weight:700':''/.test(tip),
      /background:rgba\(255,255,255,\.10\)/.test(tip)], [true,true,true]);
  ck('the split value renders beside the first, not under it', /if\(f\[6\]\)\{/.test(tip), true);
  ck('and is dimmer, so the pair reads as one measure',
     /color:rgba\(255,255,255,\.74\)/.test(tip), true);
}

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
