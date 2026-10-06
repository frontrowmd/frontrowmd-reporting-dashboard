// Clinician Trend Analysis. The point of this suite is the TIE-OUT: every series
// the card plots must sum, across the window, to the Key Metrics card above it.
// The predecessor ("Weekly Cost") deliberately did NOT tie out — it dated events
// off stage stamps while the KPI cards counted an ever-reached cohort — and the
// reconciliation question that produced came up repeatedly. buildClinicianTrend
// therefore mirrors buildClinicianComposition's loop exactly, and these
// assertions compare the two functions' real output rather than pinning numbers.
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

// d1/d2 are two deals for ONE person — the dedupe case. d4's contact was created
// before the window and must not appear at either grain.
const contacts={
  d1:{__cid:'c1',createdate:'2026-09-02T12:00:00Z'},
  d2:{__cid:'c1',createdate:'2026-09-02T12:00:00Z'},
  d3:{__cid:'c2',createdate:'2026-09-09T12:00:00Z'},
  d4:{__cid:'c3',createdate:'2026-08-20T12:00:00Z'},
};
const deals=[
  {id:'d1',properties:{clinician_meeting_booked_date:'2026-09-20'}},
  {id:'d2',properties:{}},
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

// ── The tie-out. These compare two functions, so they stay true as the shared
// definition of "an inquiry" moves; they fail the moment the two drift apart.
ck('daily inquiries sum == Key Metrics inquiries',   sum(t.day,'inquiries'), comp.contacts);
ck('weekly inquiries sum == Key Metrics inquiries',  sum(t.week,'inquiries'), comp.contacts);
ck('daily fullApp sum == Key Metrics full subs',     sum(t.day,'fullApp'),   comp.submission.contacts);
ck('weekly fullApp sum == Key Metrics full subs',    sum(t.week,'fullApp'),  comp.submission.contacts);
ck('both grains agree on spend', sum(t.day,'spend'), sum(t.week,'spend'));
ck('one person with two deals counts once', comp.contacts, 2);

// ── Row filtering ──
ck('non-Meta, non-Clinician and out-of-window spend dropped', sum(t.day,'spend'), 300);
ck('clicks filtered on the same rule',      sum(t.day,'clicks'), 150);
ck('impressions filtered on the same rule', sum(t.day,'impressions'), 10000);

// ── Bucketing ──
ck('daily keys are the days themselves', t.day.map(p=>p.wk), ['2026-09-02','2026-09-09']);
// ISO weeks start Monday, and a week may begin before `from` — the point is kept
// (its data is in-window) even though its label predates the window.
ck('weekly keys are Mondays', t.week.map(p=>p.wk), ['2026-08-31','2026-09-07']);

// ── Derived metrics are computed on each period's own totals ──
ck('CTR is a percentage',            t.day[0].ctr,     1);
ck('CVR inquiry = inquiries÷clicks', t.day[0].cvrInq,  2);
ck('cost per inquiry',               t.day[0].cpi,     100);
ck('cost per full app',              t.day[0].cpfa,    100);

// A rate with a real denominator and a zero numerator is genuinely 0%; a COST
// with a zero denominator is unknown. The distinction matters because the chart
// breaks its line on null — drawing cost-per-inquiry at $0 for a week that
// bought nothing would read as the best week in the window.
const bare=ctx.buildClinicianTrend([],{},rows,FROM,TO);
ck('zero conversions: cost is null', bare.day[0].cpi, null);
ck('zero conversions: rate is 0, not null', bare.day[0].cvrInq, 0);
ck('zero conversions: spend still plotted', bare.day[0].spend, 100);
// No ad rows at all: impressions are 0, so CTR has no denominator.
const noads=ctx.buildClinicianTrend(deals,contacts,[],FROM,TO);
ck('no ad rows: CTR is null', (noads.day[0]||{}).ctr, null);

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

// ── Client contract ──
// Six series, all distinct colours, each named in both the panel legend and the
// hover readout.
ck('client reads ads.trend', /\(d&&d\.ads&&d\.ads\.trend\)/.test(cli), true);
// ── Client contract: ONE chart, two axes by unit ────────────────────────────
// SER is the PLOTTED list only — it stops at CLN_TR_SPEND, which sits between
// the two and is deliberately excluded from the chart.
const SER=cli.slice(cli.indexOf('const CLN_TR_SERIES=['),cli.indexOf('const CLN_TR_SPEND='));
const plotted=(SER.match(/\{k:'(\w+)'/g)||[]).map(x=>x.slice(4,-1));
ck('five series are plotted', plotted.sort(), ['cpfa','cpi','ctr','cvrFull','cvrInq']);
// Ad Spend runs in thousands against cost-per figures in the tens; on the same
// axis it flattens both onto the floor. It is tracked, not plotted.
ck('Ad Spend is not a plotted series', plotted.indexOf('spend'), -1);
ck('but it is in the readout and the tiles', /const CLN_TR_READOUT=CLN_TR_SERIES\.concat\(\[CLN_TR_SPEND\]\)/.test(cli), true);
ck('the tiles cover all six metrics', /var minis=CLN_TR_READOUT\.map/.test(cli), true);
// Axis assignment is the whole point of the merge: rates left, costs right.
const axOf=k=>{const m=new RegExp("\\{k:'"+k+"'[^}]*ax:'(\\w)'").exec(SER);return m?m[1]:null;};
ck('the three rates read off the left axis', ['cvrInq','cvrFull','ctr'].map(axOf), ['L','L','L']);
ck('the two cost metrics read off the right axis', ['cpi','cpfa'].map(axOf), ['R','R']);
// Colour alone cannot say which axis a line belongs to in greyscale or under
// CVD, so the right-axis series are dashed as a second channel.
ck('right-axis series are dashed', /var dash=sr\.ax==='R'\?' stroke-dasharray/.test(cli), true);
ck('both axes are labelled on the plot', /CONVERSION<\/text>/.test(cli)&&/COST PER<\/text>/.test(cli), true);
// Six distinct hexes across every metric, plotted or not.
const hexes=(cli.slice(cli.indexOf('const CLN_TR_SERIES=['),cli.indexOf('const CLN_TR_READOUT=')).match(/#[0-9A-Fa-f]{6}/g)||[]);
ck('six colours in all', hexes.length, 6);
ck('no colour reused', new Set(hexes.map(h=>h.toLowerCase())).size, 6);
// One <svg>, not one per unit — the small-multiples layout is gone.
ck('a single chart', (fn(cli,'_renderClnTrend').match(/<svg /g)||[]).length, 1);
ck('and a single scale per axis', /var yMaxL=_clnTrNice\(mxL,'%'\),yMaxR=_clnTrNice\(mxR,'\$'\)/.test(cli), true);
ck('tooltip loops over all six readout rows', /for\(var i=1;i<=6;i\+\+\)/.test(cli), true);
// Grain toggle, mirroring the Detailed Dashboard's.
ck('Weekly/Daily toggle exists', /gPill\('week','Weekly'\)\+gPill\('day','Daily'\)/.test(cli)&&/window\.toggleClnTrendGrain=/.test(cli), true);
ck('toggle rebuilds the card', /card\.outerHTML=_clnTrendCard\(d\)/.test(cli), true);
// The heading needs an id of its own: buildSectionNav assigns one only when the
// element has none, and an assigned id would not survive the card rebuild.
ck('nav heading has a stable id', /id="cln-trend-h"/.test(cli), true);
// Section order: Trend Analysis renders immediately after Daily Activity.
const iDaily=cli.indexOf("h+=card('Daily Activity'"), iTrend=cli.indexOf('h+=_clnTrendCard(d);'), iAcq=cli.indexOf("h+=card('Acquisition'");
ck('Trend Analysis sits between Daily Activity and Acquisition',
   iDaily>0&&iTrend>iDaily&&iAcq>iTrend, true);
ck('the old Weekly Cost card is gone', /_clnWeeklyCard|_renderClnWeekly/.test(cli), false);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
