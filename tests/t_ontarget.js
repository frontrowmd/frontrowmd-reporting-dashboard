// On-Target Mix — the Acquisition card's week-over-week clinician-type chart.
//
// It answers one question ("did quality hold while cost rose?"), so the two
// metrics that answer it carry real axes: the MD/DO rate on the left (%) and
// cost per MD/DO on the right ($). Type counts are stacked columns on their own
// scale. The whole card follows the Acquisition section's Inquiry / Full
// Submission toggle, which is the part most likely to silently stop working —
// the population is threaded from `S.clinician.acqMode` through two separate
// call sites (card and renderer) that must agree.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');
const wctx={console,Date,Math,JSON,Set,Map,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,
            fetch:()=>{},TextEncoder,TextDecoder,URL,Promise,Response,Request,Headers};
vm.createContext(wctx);
vm.runInContext(src.slice(0,src.indexOf('\nexport default')),wctx,{filename:'worker.js'});

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── Worker: the type tallies the card reads ────────────────────────────────
// Four clinicians in one week: 2 MD/DO (one with a kickoff booked), 1 NP (also
// booked), 1 with no type at all. So byType and bySubType must differ, and the
// blank must land in the same 'Not set' bucket buildClinicianComposition uses —
// a different blank label would split one category across two charts.
const contacts={
  a:{__cid:'ca',createdate:'2026-09-02T12:00:00Z',clinician_type:'MD/DO'},
  b:{__cid:'cb',createdate:'2026-09-03T12:00:00Z',clinician_type:'MD/DO'},
  c:{__cid:'cc',createdate:'2026-09-03T13:00:00Z',clinician_type:'Nurse Practitioner'},
  e:{__cid:'ce',createdate:'2026-09-04T12:00:00Z',clinician_type:'   '},
};
const deals=[
  {id:'a',properties:{clinician_meeting_booked_date:'2026-09-20'}},
  {id:'b',properties:{}},
  {id:'c',properties:{clinician_meeting_booked_date:'2026-09-21'}},
  {id:'e',properties:{}},
];
const rows=[{datasource:'facebook',campaign_name:'Clinician Targeting - US',date:'2026-09-02',spend:'400',clicks:'50',impressions:'5000'}];
const FROM='2026-09-01',TO='2026-09-30';
const t=wctx.buildClinicianTrend(deals,contacts,rows,FROM,TO);
const comp=wctx.buildClinicianComposition(deals,contacts,FROM,TO);
const wk=t.week.find(p=>p.wk==='2026-08-31')||{};

ck('inquiry mix by type', wk.byType, {'MD/DO':2,'Nurse Practitioner':1,'Not set':1});
ck('full-submission mix is the subset', wk.bySubType, {'MD/DO':1,'Nurse Practitioner':1});
ck('a blank type uses the same label the Acquisition pie uses',
   (comp.type||[]).some(r=>r.label==='Not set'), true);
ck('the type map sums to the period inquiries',
   Object.values(wk.byType).reduce((a,b)=>a+b,0), wk.inquiries);
ck('and the sub map sums to its full apps',
   Object.values(wk.bySubType).reduce((a,b)=>a+b,0), wk.fullApp);
// Across the window both maps must reconcile with Key Metrics, like every other
// trend series — otherwise this card and the tiles above it disagree.
const sumMap=(a,k)=>a.reduce((s,p)=>s+Object.values(p[k]||{}).reduce((x,y)=>x+y,0),0);
ck('byType totals tie out to Key Metrics inquiries', sumMap(t.week,'byType'), comp.contacts);
ck('bySubType totals tie out to Key Metrics full subs', sumMap(t.week,'bySubType'), comp.submission.contacts);
ck('both grains carry the maps',
   [typeof t.day[0].byType, typeof t.day[0].bySubType], ['object','object']);
// Payload shape changed again.
ck('API_CACHE_VER moved past v32',
   (function(){const m=/const API_CACHE_VER = 'v(\d+)'/.exec(src);return m?+m[1]>=33:false;})(), true);

// ── Client: render the real chart and measure it ───────────────────────────
const dctx={console,Math,Date,Object,String,Number,Array,parseInt,parseFloat,isNaN,RegExp,JSON,Set};
dctx.window=dctx; dctx.captured='';
dctx.document={getElementById:()=>({set innerHTML(v){dctx.captured=v;},get innerHTML(){return dctx.captured;}})};
vm.createContext(dctx);
vm.runInContext([
  'const f$=v=>v==null?"—":"$"+Math.round(v).toLocaleString();',
  'const fN=v=>v==null?"—":Math.round(v).toLocaleString();',
  cli.slice(cli.indexOf('const CLN_TR_SERIES=['),cli.indexOf('function _clnTrendGrain()')),
  cli.slice(cli.indexOf('const CLN_ON_TARGET='),cli.indexOf('function _clnTypeBands')),
  fn(cli,'_clnTrNice'),fn(cli,'_clnTrLabel'),fn(cli,'_clnTrEnd'),
  fn(cli,'_clnTypeBands'),fn(cli,'_clnTypeCount'),fn(cli,'_clnTypeCard'),fn(cli,'_renderClnOnTarget'),
].join('\n'),dctx);
ck('the real card and renderer loaded',
   [typeof dctx._clnTypeCard,typeof dctx._renderClnOnTarget], ['function','function']);

// Two weeks: the rate rises 25% -> 75% while cost per MD/DO rises $100 -> $300,
// which is exactly the shape the card exists to show.
const WK=[
  {wk:'2026-09-07',spend:100,byType:{'MD/DO':1,'Nurse Practitioner':3},bySubType:{'MD/DO':1}},
  {wk:'2026-09-14',spend:900,byType:{'MD/DO':3,'Nurse Practitioner':1},bySubType:{'MD/DO':3}},
];
const PAY={period:{from:'2026-09-07',to:'2026-09-20'},ads:{trend:{week:WK,day:WK}}};
const card=dctx._clnTypeCard(PAY,'inquiry');
dctx._renderClnOnTarget(PAY,'inquiry');
const svg=dctx.captured;
ck('the card renders a chart container', /id="cln-ontarget-chart"/.test(card), true);
ck('and the renderer draws into it', svg.indexOf('<svg ')>=0, true);

// Window totals are recomputed from summed components, not averaged across
// weeks: the mean of two weekly rates is not the window's rate.
// Pooled, not averaged. WK's two weeks happen to pool AND average to 50%, so
// this needs its own fixture where the two answers differ: 1-of-1 then 1-of-9
// averages to 55.6% and pools to 20%.
{
  const SKEW=[{wk:'2026-09-07',spend:100,byType:{'MD/DO':1},bySubType:{}},
              {wk:'2026-09-14',spend:100,byType:{'MD/DO':1,'Nurse Practitioner':8},bySubType:{}}];
  const c=dctx._clnTypeCard({period:{from:'2026-09-07',to:'2026-09-20'},ads:{trend:{week:SKEW}}},'inquiry');
  ck('the window rate is pooled, not the mean of the weekly rates',
     [/20\.0%/.test(c), /55\.6%/.test(c)], [true,false]);
  // Same for cost: $200 over 2 MD/DO is $100; averaging $100 and $100 would
  // coincide, so skew the spend too.
  const SKEW2=[{wk:'2026-09-07',spend:100,byType:{'MD/DO':1},bySubType:{}},
               {wk:'2026-09-14',spend:900,byType:{'MD/DO':9},bySubType:{}}];
  const c2=dctx._clnTypeCard({period:{from:'2026-09-07',to:'2026-09-20'},ads:{trend:{week:SKEW2}}},'inquiry');
  ck('the window cost is pooled too', [/\$100/.test(c2), /\$550/.test(c2)], [true,false]);
}
ck('window cost divides total spend by total MD/DO', /\$250/.test(card), true);

// Geometry — read the plot box off the gridlines so this survives a resize.
const gridY=(svg.match(/<line [^>]*y1="([\d.]+)"/g)||[]).map(x=>parseFloat(/y1="([\d.]+)"/.exec(x)[1]));
const floorY=Math.max.apply(null,gridY), topY=Math.min.apply(null,gridY);
const pctTicks=(svg.match(/text-anchor="end"[^>]*>([\d.]+)%</g)||[]).map(x=>parseFloat(/>([\d.]+)%</.exec(x)[1]));
const dolTicks=(svg.match(/text-anchor="start"[^>]*>\$([\d,]+)</g)||[]).map(x=>parseFloat(/>\$([\d,]+)</.exec(x)[1].replace(/,/g,'')));
const yMaxL=Math.max.apply(null,pctTicks), yMaxR=Math.max.apply(null,dolTicks);
const yL=v=>floorY-(v/yMaxL)*(floorY-topY), yR=v=>floorY-(v/yMaxR)*(floorY-topY);
const firstY=hex=>{const m=new RegExp('<polyline points="[\\d.]+,([\\d.]+)[^"]*" fill="none" stroke="'+hex+'"').exec(svg);return m?parseFloat(m[1]):null;};
const near=(a,b)=>a!=null&&Math.abs(a-b)<0.75;
ck('the rate line is on the % ladder, not the $ one',
   [near(firstY('#0F766E'),yL(25)), near(firstY('#0F766E'),yR(25))], [true,false]);
ck('the cost line is on the $ ladder, not the % one',
   [near(firstY('#BE123C'),yR(100)), near(firstY('#BE123C'),yL(100))], [true,false]);
ck('the cost line is dashed, the rate line solid',
   [/stroke="#BE123C" stroke-width="2.5" stroke-linejoin="round" stroke-dasharray/.test(svg),
    /stroke="#0F766E" stroke-width="2.5" stroke-linejoin="round"\/>/.test(svg)], [true,true]);
// MD/DO must stack from the baseline, or its height cannot be read without
// subtracting the bands above it.
const rects=(svg.match(/<rect x="[\d.-]+" y="([\d.]+)" width="[\d.]+" height="([\d.]+)" fill="(#[0-9A-Fa-f]{6})"/g)||[])
  .map(r=>{const m=/y="([\d.]+)" width="[\d.]+" height="([\d.]+)" fill="(#[0-9A-Fa-f]{6})"/.exec(r);
           return {y:parseFloat(m[1]),h:parseFloat(m[2]),c:m[3]};});
ck('four stacked segments across two weeks', rects.length, 4);
ck('the MD/DO band sits on the baseline',
   rects.filter(r=>r.c==='#0F766E').every(r=>Math.abs(r.y+r.h-floorY)<0.75), true);
ck('the columns stay inside the plot',
   rects.every(r=>r.y>=topY-0.5&&r.y+r.h<=floorY+0.5), true);
// The columns carry no tick axis, so the total has to be printed.
ck('each column is labelled with its total',
   (svg.match(/font-weight="700" fill="var\(--grey\)" pointer-events="none">\d+<\/text>/g)||[]).length, 2);

// ── The population toggle ──────────────────────────────────────────────────
dctx._renderClnOnTarget(PAY,'submission');
const subSvg=dctx.captured;
const subCard=dctx._clnTypeCard(PAY,'submission');
ck('Full Submission mode plots a different population', subSvg===svg, false);
// 1 of 1 then 3 of 3 — every full submission in this fixture is MD/DO.
ck('and its rate is computed over that population', /100\.0%/.test(subCard), true);
ck('the cost metric renames with the population',
   [/Cost per MD\/DO Inquiry/.test(card), /Cost per MD\/DO Full App/.test(subCard)], [true,true]);
ck('the card states which population it is showing',
   [/full submissions/.test(subCard), /inquiries/.test(card)], [true,true]);
// Both call sites read the SAME state, or the chart silently shows one
// population while its own header names the other.
ck('the card is built with the Acquisition toggle', /\+_clnTypeCard\(d,acqMode\)/.test(cli), true);
ck('and the renderer is handed the same state',
   /_renderClnOnTarget\(d,\(S\.clinician\.acqMode==='submission'\)\?'submission':'inquiry'\)/.test(cli), true);
ck('which is how the Acquisition card picks its own population',
   /const acqMode=\(S\.clinician\.acqMode==='submission'\)\?'submission':'inquiry';/.test(cli), true);
ck('and the toggle re-renders the page', /window\.clinicianSetAcq=function\(mode\)\{S\.clinician\.acqMode=mode;renderClinician\(\);\}/.test(cli), true);

// ── Unmapped types and empty states ────────────────────────────────────────
// A new HubSpot option must show up somewhere rather than shrink the total.
const NEW=[{wk:'2026-09-07',spend:50,byType:{'MD/DO':1,'Acupuncturist':2},bySubType:{}}];
const nb=dctx._clnTypeBands(NEW,'byType');
ck('an unknown type is rolled into Other, not dropped',
   [nb.bands.map(b=>b.n), nb.extra], [['MD/DO','Other'],['Acupuncturist']]);
ck('and the roll-up counts it', dctx._clnTypeCount(NEW[0],'byType',nb.bands[1]), 2);
ck('the card says so', /1 unmapped type/.test(dctx._clnTypeCard({period:{from:'2026-09-07',to:'2026-09-13'},ads:{trend:{week:NEW}}},'inquiry')), true);
// A cost is undefined when nothing was spent or nothing on-target arrived —
// never 0, which would plot on the floor and read as the cheapest week.
// TWO adjacent zero-spend weeks, deliberately: a single bad point cannot form a
// polyline, so a one-week fixture would pass even if the cost fell back to 0.
const ZERO=[{wk:'2026-09-07',spend:0,byType:{'MD/DO':2},bySubType:{}},
            {wk:'2026-09-14',spend:0,byType:{'MD/DO':3},bySubType:{}},
            {wk:'2026-09-21',spend:500,byType:{'Nurse Practitioner':2},bySubType:{}}];
dctx._renderClnOnTarget({period:{from:'2026-09-07',to:'2026-09-27'},ads:{trend:{week:ZERO}}},'inquiry');
ck('no spend and no on-target both suppress the cost line',
   (dctx.captured.match(/<polyline [^>]*stroke="#BE123C"/g)||[]).length, 0);
ck('but the rate line still draws',
   (dctx.captured.match(/<polyline [^>]*stroke="#0F766E"/g)||[]).length, 1);
ck('and nothing emits NaN', /NaN/.test(dctx.captured), false);
ck('no weeks gives an empty state, not a broken chart',
   /No weekly clinician data/.test(dctx._clnTypeCard({ads:{trend:{week:[]}}},'inquiry')), true);
ck('a stale payload with no trend is handled too',
   /No weekly clinician data/.test(dctx._clnTypeCard({ads:{}},'inquiry')), true);
// The readout is 2 metrics + one row per type band; the shared tooltip must
// cover the widest case it can emit.
const widest=2+dctx._clnTypeBands(WK,'byType').bands.length;
const loopMax=+(/for\(var i=1;i<=(\d+);i\+\+\)/.exec(cli)||[])[1];
ck('the tooltip covers every row this card emits', loopMax>=widest, true);
ck('and the card never emits more rows than that', /rows\.slice\(0,8\)/.test(cli)&&loopMax>=8, true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
