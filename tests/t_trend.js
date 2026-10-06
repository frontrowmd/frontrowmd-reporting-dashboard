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
// The endpoint must hand it the PAGE deals and the page window — the same pair
// buildClinicianComposition gets. Passing the history deals would divide one
// month of spend by three months of signups.
ck('endpoint passes page-scoped args',
   /ads\.trend\s*=\s*buildClinicianTrend\(dealsPage,\s*contactsByDeal,\s*wRows,\s*cur\.from,\s*cur\.to\)/.test(src), true);
// Payload shape changed, so a cached v31 response must not be served to the new
// client, which reads ads.trend and would render an empty state.
ck('API_CACHE_VER moved past v31',
   (function(){const m=/const API_CACHE_VER = 'v(\d+)'/.exec(src);return m?+m[1]>=32:false;})(), true);

// ── Client contract: ONE chart, two axes by unit ────────────────────────────
ck('client reads ads.trend', /\(d&&d\.ads&&d\.ads\.trend\)/.test(cli), true);
// SER is the PLOTTED list only — it stops at CLN_TR_SPEND, which sits between
// the two and is deliberately kept off the axes.
const SER=cli.slice(cli.indexOf('const CLN_TR_SERIES=['),cli.indexOf('const CLN_TR_SPEND='));
const plotted=(SER.match(/\{k:'(\w+)'/g)||[]).map(x=>x.slice(4,-1));
// Order is specified, not incidental: it drives the tiles, the legend and the
// hover readout, and all three have to read the same way.
ck('metric order', plotted.concat(['spend']),
   ['cpi','cpfa','cvrInq','cvrFull','ctr','spend']);
ck('Ad Spend is not an axis series', plotted.indexOf('spend'), -1);
ck('but it is in the readout and the tiles', /const CLN_TR_READOUT=CLN_TR_SERIES\.concat\(\[CLN_TR_SPEND\]\)/.test(cli), true);
ck('the tiles cover all six metrics', /var minis=CLN_TR_READOUT\.map/.test(cli), true);
const axOf=k=>{const m=new RegExp("\\{k:'"+k+"'[^}]*ax:'(\\w)'").exec(SER);return m?m[1]:null;};
ck('the three rates read off the left axis', ['cvrInq','cvrFull','ctr'].map(axOf), ['L','L','L']);
ck('the two cost metrics read off the right axis', ['cpi','cpfa'].map(axOf), ['R','R']);
// Colour alone cannot say which axis a line belongs to in greyscale or under
// CVD, so the right-axis series are dashed as a second channel.
ck('right-axis series are dashed', /var dash=sr\.ax==='R'\?' stroke-dasharray/.test(cli), true);
ck('both axes are labelled on the plot', /CONVERSION<\/text>/.test(cli)&&/COST PER<\/text>/.test(cli), true);
const ALL=cli.slice(cli.indexOf('const CLN_TR_SERIES=['),cli.indexOf('const CLN_TR_READOUT='));
const hexes=(ALL.match(/#[0-9A-Fa-f]{6}/g)||[]);
ck('six colours in all', hexes.length, 6);
ck('no colour reused', new Set(hexes.map(h=>h.toLowerCase())).size, 6);
ck('a single chart', (fn(cli,'_renderClnTrend').match(/<svg /g)||[]).length, 1);
// The grain toggle is a TWO-step contract: _clnTrendCard emits an EMPTY panels
// div, so rebuilding the card without the following render leaves the chart
// permanently blank with no error.
const TOG=cli.slice(cli.indexOf('window.toggleClnTrendGrain=function'),cli.indexOf('function _clnTrendPts'));
ck('the toggle rebuilds the card', /card\.outerHTML=_clnTrendCard\(d\)/.test(TOG), true);
ck('and re-renders the chart after it',
   TOG.indexOf('_renderClnTrend(d)')>TOG.indexOf('card.outerHTML=_clnTrendCard(d)'), true);
ck('nav heading has a stable id', /id="cln-trend-h"/.test(cli), true);
const iDaily=cli.indexOf("h+=card('Daily Activity'"), iTrend=cli.indexOf('h+=_clnTrendCard(d);'), iAcq=cli.indexOf("h+=card('Acquisition'");
ck('Trend Analysis sits between Daily Activity and Acquisition',
   iDaily>0&&iTrend>iDaily&&iAcq>iTrend, true);
ck('the old Weekly Cost card is gone', /_clnWeeklyCard|_renderClnWeekly/.test(cli), false);

// ── Geometry: render the real SVG and measure it ────────────────────────────
// Everything above about axes is source text. This block runs _renderClnTrend
// and checks where points actually land, which is the only way to catch an
// inverted yOf or a series routed to the wrong ladder.
const dctx={console,Math,Date,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,JSON,Set};
dctx.window=dctx; dctx.captured='';
dctx.document={getElementById:()=>({set innerHTML(v){dctx.captured=v;},get innerHTML(){return dctx.captured;}})};
vm.createContext(dctx);
vm.runInContext([
  'const f$=v=>v==null?"—":"$"+Math.round(v).toLocaleString();',
  'const fN=v=>v==null?"—":Math.round(v).toLocaleString();',
  cli.slice(cli.indexOf('const CLN_TR_SERIES=['),cli.indexOf('function _clnTrendGrain()')),
  fn(cli,'_clnTrendGrain'),fn(cli,'_clnTrendPts'),fn(cli,'_clnTrNice'),
  fn(cli,'_clnTrFmt'),fn(cli,'_clnTrLabel'),fn(cli,'_clnTrEnd'),fn(cli,'_renderClnTrend'),
].join('\n'),dctx);
ck('the real renderer loaded', typeof dctx._renderClnTrend, 'function');
// CTR at 100% sits at the top of the % ladder; cpi at $5 sits near the bottom of
// a $ ladder topped by cpfa at $900. Route either through the other's scale and
// both land in the wrong half of the plot.
const GEO=[
  {wk:'2026-09-01',spend:100,impressions:100,clicks:100,inquiries:20,fullApp:1,cpi:5,cpfa:900,cvrInq:20,cvrFull:1,ctr:100},
  {wk:'2026-09-02',spend:100,impressions:100,clicks:100,inquiries:20,fullApp:1,cpi:5,cpfa:900,cvrInq:20,cvrFull:1,ctr:100},
];
dctx._renderClnTrend({period:{from:'2026-09-01',to:'2026-09-02'},ads:{trend:{week:GEO,day:GEO}}});
const svg=dctx.captured;
ck('it drew something', svg.indexOf('<svg ')>=0, true);
// Read the plot box off the gridlines rather than hard-coding padding, so the
// expected values stay correct if the chart is resized.
const gridY=(svg.match(/<line [^>]*y1="([\d.]+)"/g)||[]).map(x=>parseFloat(/y1="([\d.]+)"/.exec(x)[1]));
const floorY=Math.max.apply(null,gridY), topY=Math.min.apply(null,gridY);
ck('four gridline intervals', gridY.length, 5);
// Axis maxima come from the topmost tick on each side.
const pctTicks=(svg.match(/text-anchor="end"[^>]*>([\d.]+)%</g)||[]).map(x=>parseFloat(/>([\d.]+)%</.exec(x)[1]));
const dolTicks=(svg.match(/text-anchor="start"[^>]*>\$([\d,]+)</g)||[]).map(x=>parseFloat(/>\$([\d,]+)</.exec(x)[1].replace(/,/g,'')));
const yMaxL=Math.max.apply(null,pctTicks), yMaxR=Math.max.apply(null,dolTicks);
const yL=v=>floorY-(v/yMaxL)*(floorY-topY), yR=v=>floorY-(v/yMaxR)*(floorY-topY);
const firstY=hex=>{const m=new RegExp('<polyline points="[\\d.]+,([\\d.]+)[^"]*" fill="none" stroke="'+hex+'"').exec(svg);return m?parseFloat(m[1]):null;};
const near=(a,b)=>a!=null&&Math.abs(a-b)<0.75;
ck('CTR is plotted on the % ladder, not the $ one',
   [near(firstY('#1D4ED8'),yL(100)), near(firstY('#1D4ED8'),yR(100))], [true,false]);
ck('Cost per Inquiry is plotted on the $ ladder, not the % one',
   [near(firstY('#4A87A8'),yR(5)), near(firstY('#4A87A8'),yL(5))], [true,false]);
ck('Cost per Full App too', near(firstY('#9333EA'),yR(900)), true);
ck('CVR Inquiry too', near(firstY('#0E7C66'),yL(20)), true);
// Ad Spend is background shading with no ladder of its own: rects, scaled to
// their own peak, and never a polyline.
ck('Ad Spend draws no line', firstY('#64748B'), null);
ck('Ad Spend draws background columns', (svg.match(/fill="#64748B" fill-opacity/g)||[]).length, 2);
ck('the columns stay inside the plot', (function(){
  const hs=(svg.match(/fill="#64748B" fill-opacity/g)||[]).length?
    (svg.match(/<rect x="[\d.-]+" y="([\d.]+)" width="[\d.]+" height="([\d.]+)" fill="#64748B"/g)||[]):[];
  return hs.every(r=>{const m=/y="([\d.]+)" width="[\d.]+" height="([\d.]+)"/.exec(r);
                      return parseFloat(m[1])>=topY-0.5&&parseFloat(m[1])+parseFloat(m[2])<=floorY+0.5;});})(), true);
// A null value must break the line, not interpolate through it.
const GAP=[{wk:'2026-09-01',spend:10,impressions:100,clicks:10,inquiries:1,fullApp:1,cpi:10,cpfa:10,cvrInq:10,cvrFull:10,ctr:10},
           {wk:'2026-09-02',spend:0,impressions:0,clicks:0,inquiries:0,fullApp:0,cpi:null,cpfa:null,cvrInq:null,cvrFull:null,ctr:null},
           {wk:'2026-09-03',spend:10,impressions:100,clicks:10,inquiries:1,fullApp:1,cpi:10,cpfa:10,cvrInq:10,cvrFull:10,ctr:10}];
dctx._renderClnTrend({period:{from:'2026-09-01',to:'2026-09-03'},ads:{trend:{week:GAP,day:GAP}}});
ck('a null period breaks the line rather than spanning it',
   (dctx.captured.match(/<polyline [^>]*stroke="#1D4ED8"/g)||[]).length, 0);
// One point: the band maths divides by (n-1), which is 0 here.
const ONE=[{wk:'2026-09-01',spend:10,impressions:100,clicks:10,inquiries:1,fullApp:1,cpi:10,cpfa:10,cvrInq:10,cvrFull:10,ctr:10}];
dctx._renderClnTrend({period:{from:'2026-09-01',to:'2026-09-01'},ads:{trend:{week:ONE,day:ONE}}});
ck('a single point emits no NaN', /NaN/.test(dctx.captured), false);
ck('and still gets a hover band', (dctx.captured.match(/class="clnwk-band"/g)||[]).length, 1);

// ── Partial periods ──
// The point whose numbers are still moving is the one point that must say so.
// The old strict `>` never fired on the daily grain at all (a day's own end IS
// that day) and missed the current week whenever today was a Sunday.
ck('the in-progress test is inclusive', /var inProgress=covTo>=todayMs;/.test(cli), true);
// An ISO week at either edge of the window is CLIPPED by it, and saying
// "Week of Sep 28 – Oct 4" for three days of data overstates what was measured.
ck('edge weeks are clamped to the window', /var covFrom=winFrom\?Math\.max\(startMs,msOf\(winFrom\)\):startMs;/.test(cli), true);
ck('and flagged when short', /var clipped=grain!=='day'&&span<7;/.test(cli), true);
ck('the tooltip states which kind of partial it is',
   /pw\.textContent=el\.getAttribute\('data-pnote'\)/.test(cli), true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
