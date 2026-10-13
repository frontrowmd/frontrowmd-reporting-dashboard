// Window / date handling: computeWindows, the Pacific bucketing helpers, and
// pagesForWindow.
//
// computeWindows is pure apart from todayET(), so it is sliced and run with a
// SETTABLE "today" — that way the suite asserts the real arithmetic (month
// clamps, year rollovers, leap days) instead of whatever this week happens to
// produce. Nothing below pins a date that depends on when the suite is run.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');

const ctx={console,Date,Math,Number,String,Object,Array,JSON,Error,RegExp,parseInt,parseFloat,isNaN};
vm.createContext(ctx);
vm.runInContext([
  // AD_EPOCH is a top-level const in the worker; mirrored here so the slice is
  // self-contained. The assertion below checks it still matches the source.
  'const AD_EPOCH = "2024-01-01";',
  'let _TODAY = null;',
  // The ONLY stub. Everything else below is the shipped function body.
  'function todayET(){ return new Date(_TODAY.getTime()); }',
  'globalThis.setToday = (s)=>{ const [y,m,d]=s.split("-").map(Number); _TODAY=new Date(Date.UTC(y,m-1,d)); };',
  'globalThis.setTodayMs = (ms)=>{ _TODAY=new Date(ms); };',
  fn(src,'_usDst'), fn(src,'isPDT'), fn(src,'ptOff'), fn(src,'etOff'), fn(src,'fmt'),
  fn(src,'etDayKeyOf'), fn(src,'toMsET'), fn(src,'toMsUTC'), fn(src,'dateMs'),
  fn(src,'daysInMonth'), fn(src,'computeWindows'), fn(src,'pagesForWindow'),
].join('\n'),ctx);

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const W=(t,...a)=>ctx.computeWindows(t,...a);
const day=s=>Date.parse(s+'T00:00:00Z');
const span=w=>Math.round((day(w.to)-day(w.from))/86400000)+1;
const TYPES=['7d','mtd','lastMonth','ytd','allTime','custom'];

ck('the real computeWindows loaded',typeof ctx.computeWindows,'function');
// The stub must not have shadowed a real todayET that got sliced in by accident.
ck('AD_EPOCH in the slice matches the source',/const AD_EPOCH = '2024-01-01'/.test(src),true);

// ---------------------------------------------------------------------------
// Which windows get a prior / a priorMonth
// ---------------------------------------------------------------------------
ctx.setToday('2026-10-06');

// The three-field contract every caller destructures:
//   const { current, prior, priorMonth } = computeWindows(...)
// A missing key (vs. an explicit null) is the difference between "no comparison"
// and a crash in the `prior ? fetch(...) : null` ternaries at each call site.
ck('every window type returns all three keys',
   TYPES.map(t=>{const r=W(t,'2026-05-01','2026-05-10');return ['current','prior','priorMonth'].every(k=>k in r);}),
   [true,true,true,true,true,true]);

// ytd and allTime are open-ended windows — there is no equal-length period
// before them, so the vs-prior delta is suppressed rather than faked.
ck('ytd has no prior',W('ytd').prior,null);
ck('allTime has no prior',W('allTime').prior,null);
ck('every other window does have a prior',
   ['7d','mtd','lastMonth'].map(t=>W(t).prior!==null),[true,true,true]);
ck('custom has a prior too',W('custom','2026-05-01','2026-05-10').prior!==null,true);

// priorMonth drives the "vs. Last mo" / "vs. Same time last mo" line. It only
// exists where a month-ago comparison is meaningful: lastMonth already IS a
// month, allTime has nothing to compare to, and custom is user-defined.
ck('priorMonth is null for lastMonth, allTime and custom',
   [W('lastMonth').priorMonth,W('allTime').priorMonth,W('custom','2026-05-01','2026-05-10').priorMonth],
   [null,null,null]);
ck('priorMonth is present for 7d, mtd and ytd',
   ['7d','mtd','ytd'].map(t=>W(t).priorMonth!==null),[true,true,true]);

// Only `current` carries a label — the client reads period.label for the header
// and renders prior/priorMonth as bare date ranges.
ck('only current is labelled',
   TYPES.map(t=>{const r=W(t,'2026-05-01','2026-05-10');
     return !!r.current.label && !(r.prior&&r.prior.label) && !(r.priorMonth&&r.priorMonth.label);}),
   [true,true,true,true,true,true]);
ck('the labels are the ones the header prints',
   ['7d','mtd','lastMonth','ytd','allTime'].map(t=>W(t).label||W(t).current.label),
   ['Last 7 Days','Month to Date','Last Month','Year to Date','All Time']);
ck('custom is labelled Custom Range',W('custom','2026-05-01','2026-05-10').current.label,'Custom Range');

// ---------------------------------------------------------------------------
// Per-window arithmetic, with "today" held fixed
// ---------------------------------------------------------------------------
ck('7d is 7 days ending today, with the 7 days before it as prior',
   W('7d'),{current:{from:'2026-09-30',to:'2026-10-06',label:'Last 7 Days'},
            prior:{from:'2026-09-23',to:'2026-09-29'},
            priorMonth:{from:'2026-08-30',to:'2026-09-05'}});
// 7d's priorMonth is the SAME 7 days a month back, not the whole prior month —
// that is what the client's "Same time last mo" label promises.
ck('7d priorMonth is 7 days, not a whole month',span(W('7d').priorMonth),7);

ck('mtd runs from the 1st to today and compares the same slice of last month',
   W('mtd'),{current:{from:'2026-10-01',to:'2026-10-06',label:'Month to Date'},
             prior:{from:'2026-09-01',to:'2026-09-06'},
             priorMonth:{from:'2026-09-01',to:'2026-09-30'}});

ck('lastMonth is the whole prior calendar month vs the one before it',
   W('lastMonth'),{current:{from:'2026-09-01',to:'2026-09-30',label:'Last Month'},
                   prior:{from:'2026-08-01',to:'2026-08-31'},priorMonth:null});

ck('ytd starts Jan 1 and still gets a prior-month comparison',
   W('ytd'),{current:{from:'2026-01-01',to:'2026-10-06',label:'Year to Date'},
             prior:null,priorMonth:{from:'2026-09-01',to:'2026-09-30'}});

// allTime starts at the ad epoch, not at some rolling offset — the Windsor data
// does not exist before it.
ck('allTime starts at AD_EPOCH',W('allTime').current.from,'2024-01-01');
ck('allTime still ends today',W('allTime').current.to,'2026-10-06');

// ---------------------------------------------------------------------------
// Month-length clamp, leap day, and year rollover
// ---------------------------------------------------------------------------
// On the 31st, the "same slice of last month" cannot be the 31st of February.
// Without the daysInMonth clamp this produced a March date and MTD compared
// itself against part of its own month.
ctx.setToday('2026-03-31');
ck('mtd on the 31st clamps the prior month to its last day',W('mtd').prior,{from:'2026-02-01',to:'2026-02-28'});
ctx.setToday('2024-03-31');
ck('and the clamp knows about leap years',W('mtd').prior,{from:'2024-02-01',to:'2024-02-29'});

// getUTCMonth()-1 in January must roll the YEAR back, not produce month -1.
ctx.setToday('2026-01-15');
ck('mtd in January compares against last December',W('mtd').prior,{from:'2025-12-01',to:'2025-12-15'});
ck('lastMonth in January IS last December',W('lastMonth').current,
   {from:'2025-12-01',to:'2025-12-31',label:'Last Month'});
ck('and its prior is the November before that',W('lastMonth').prior,{from:'2025-11-01',to:'2025-11-30'});
ck('ytd priorMonth in January is last December',W('ytd').priorMonth,{from:'2025-12-01',to:'2025-12-31'});
ck('ytd in January is a one-fortnight window, not a whole year',
   W('ytd').current,{from:'2026-01-01',to:'2026-01-15',label:'Year to Date'});
ck('7d spanning New Year reaches back into the old year',W('7d').priorMonth,{from:'2025-12-09',to:'2025-12-15'});

// A 7d window starting on the 31st has no 31st in the prior month; Date.UTC
// normalises the overflow forward. The window stays VALID (7 contiguous days) —
// that is the property that matters, and it is what this guards.
ctx.setToday('2026-04-06');
ck('a 7d priorMonth that overflows a short month is still a valid 7 days',
   [span(W('7d').priorMonth),W('7d').priorMonth.from<=W('7d').priorMonth.to],[7,true]);

// ---------------------------------------------------------------------------
// Custom windows
// ---------------------------------------------------------------------------
ctx.setToday('2026-10-06');
ck('custom echoes the supplied range verbatim',W('custom','2026-05-01','2026-05-10').current,
   {from:'2026-05-01',to:'2026-05-10',label:'Custom Range'});
// Default comparison: equal-length and immediately before, never overlapping.
ck('custom defaults to the equal-length window right before it',
   W('custom','2026-05-01','2026-05-10').prior,{from:'2026-04-21',to:'2026-04-30'});
ck('and that default matches the current span exactly',
   span(W('custom','2026-05-01','2026-05-10').prior),span(W('custom','2026-05-01','2026-05-10').current));
// vsFrom/vsTo win outright — the "compare to" pickers must not be second-guessed.
ck('an explicit vs range overrides the default',
   W('custom','2026-05-01','2026-05-10','2026-01-01','2026-01-05').prior,{from:'2026-01-01',to:'2026-01-05'});
ck('a vs range may be a different length and may sit after the window',
   W('custom','2026-05-01','2026-05-02','2026-12-01','2026-12-31').prior,{from:'2026-12-01',to:'2026-12-31'});
// Half a vs range is not a vs range — falls back to the computed default.
ck('vsFrom without vsTo falls back to the default prior',
   W('custom','2026-05-01','2026-05-10','2026-01-01',null).prior,{from:'2026-04-21',to:'2026-04-30'});
ck('a single-day custom window gets a single-day prior',
   W('custom','2026-05-05','2026-05-05').prior,{from:'2026-05-04',to:'2026-05-04'});

// A custom window REQUIRES from + to. computeWindows itself does not validate —
// it happily returns from:null and a NaN prior — which is exactly why the guard
// has to live at the call sites, before any date is touched.
const bad=W('custom',null,null);
ck('computeWindows does not itself reject a from-less custom window',
   [bad.current.from,bad.prior.from],[null,'NaN-NaN-NaN']);
// ...and this is what that null reaches: toMsET(null).split -> TypeError, which
// surfaced as a bare 500 on the analyzer endpoint.
let threw=false; try{ctx.toMsET(null);}catch(e){threw=true;}
ck('an unvalidated null date blows up downstream in toMsET',threw,true);
// So both async entry points guard it up front.
ck('processAzRequest guards the custom window',
   /windowType === 'custom' && \(!customFrom \|\| !customTo\)[\s\S]{0,120}Custom window requires from \+ to/.test(src),true);
ck('two entry points carry that guard',
   (src.match(/Custom window requires from \+ to date params/g)||[]).length>=2,true);

// Unknown window types throw rather than silently defaulting to one — a typo in
// a client payload must be loud.
let msg=''; try{W('last7');}catch(e){msg=e.message;}
ck('an unknown window type throws',/Unknown window: last7/.test(msg),true);
// The client's accepted list and the switch must not drift apart.
ck('the client only ever sends window types the switch handles',
   /const OK=\['7d','mtd','lastMonth','ytd','allTime','custom'\]/.test(cli),true);

// ---------------------------------------------------------------------------
// Invariant sweep: 800 consecutive "today"s across a leap year and two DST flips
// ---------------------------------------------------------------------------
const viol=[];
for(let i=0;i<800;i++){
  ctx.setTodayMs(Date.UTC(2024,0,1)+i*86400000);
  for(const t of ['7d','mtd','lastMonth','ytd','allTime']){
    const r=W(t);
    if(r.current.from>r.current.to)viol.push(['current reversed',t,i]);
    if(r.prior){
      if(r.prior.from>r.prior.to)viol.push(['prior reversed',t,i]);
      // A prior that touches the current window double-counts days in the delta.
      if(r.prior.to>=r.current.from)viol.push(['prior overlaps current',t,i]);
    }
    if(r.priorMonth&&r.priorMonth.from>r.priorMonth.to)viol.push(['priorMonth reversed',t,i]);
    if(t==='7d'&&[span(r.current),span(r.prior),span(r.priorMonth)].some(s=>s!==7))
      viol.push(['7d span drifted',i]);
    if(t==='mtd'&&r.current.from.slice(8)!=='01')viol.push(['mtd not month start',i]);
    if(t==='ytd'&&r.current.from.slice(5)!=='01-01')viol.push(['ytd not Jan 1',i]);
    if(t==='lastMonth'&&span(r.prior)<28)viol.push(['lastMonth prior too short',i]);
  }
}
ck('no window ever reverses, overlaps its prior, or drifts its span',viol.slice(0,3),[]);
ck('every date emitted is a well-formed YYYY-MM-DD',(()=>{
  ctx.setToday('2026-02-29'.replace('29','28'));
  const all=[];
  for(const t of TYPES){const r=W(t,'2026-05-01','2026-05-10');
    for(const w of [r.current,r.prior,r.priorMonth]) if(w){all.push(w.from,w.to);}}
  return all.every(s=>/^\d{4}-\d{2}-\d{2}$/.test(s));
})(),true);

// ---------------------------------------------------------------------------
// Pacific bucketing helpers
// ---------------------------------------------------------------------------
// US DST runs the same DATES in both zones but flips at 2am LOCAL, so the UTC
// instant differs: 2am STANDARD entering, 2am DAYLIGHT leaving.
ck('PDT starts at 10:00 UTC on the 2nd Sunday of March',
   [ctx.isPDT(new Date('2026-03-08T09:59:59Z')),ctx.isPDT(new Date('2026-03-08T10:00:00Z'))],[false,true]);
ck('PDT ends at 09:00 UTC on the 1st Sunday of November',
   [ctx.isPDT(new Date('2026-11-01T08:59:59Z')),ctx.isPDT(new Date('2026-11-01T09:00:00Z'))],[true,false]);
ck('the transition dates move with the year',
   [ctx.isPDT(new Date('2025-03-09T10:00:00Z')),ctx.isPDT(new Date('2024-03-10T10:00:00Z')),
    ctx.isPDT(new Date('2024-03-09T10:00:00Z'))],[true,true,false]);
ck('ptOff is -7 in summer and -8 in winter',
   [ctx.ptOff(new Date('2026-07-01T12:00:00Z')),ctx.ptOff(new Date('2026-01-01T12:00:00Z'))],[-7,-8]);
// EASTERN is the dashboard's zone: the HubSpot portal is America/New_York, and
// a number that disagrees with the UI someone checks it against is wrong.
ck('etOff is -4 in summer and -5 in winter',
   [ctx.etOff(new Date('2026-07-01T12:00:00Z')),ctx.etOff(new Date('2026-01-01T12:00:00Z'))],[-4,-5]);
// This is the regression that made a daily off-by-one hard to place: etOff used
// to `return ptOff(d)` under an ET name, so everything bucketed Pacific while
// the code read Eastern. They must now DIFFER, by exactly three hours.
ck('etOff is NOT ptOff any more',
   [1,4,7,11].map(m=>{const d=new Date(Date.UTC(2026,m,15));return ctx.etOff(d)===ctx.ptOff(d);}),
   [false,false,false,false]);
ck('  and is exactly three hours ahead year-round',
   [0,3,6,9].map(m=>{const d=new Date(Date.UTC(2026,m,15));return ctx.etOff(d)-ctx.ptOff(d);}),
   [3,3,3,3]);
// Eastern's own transition instants, 3h earlier in UTC than Pacific's.
ck('EDT starts at 07:00 UTC on the 2nd Sunday of March',
   [ctx.etOff(new Date('2026-03-08T06:59:59Z')),ctx.etOff(new Date('2026-03-08T07:00:00Z'))],[-5,-4]);
ck('EDT ends at 06:00 UTC on the 1st Sunday of November',
   [ctx.etOff(new Date('2026-11-01T05:59:59Z')),ctx.etOff(new Date('2026-11-01T06:00:00Z'))],[-4,-5]);

// A bare date is already local and must pass through untouched — shifting it
// would move every HubSpot DATE property back a day.
ck('a bare YYYY-MM-DD passes through etDayKeyOf',ctx.etDayKeyOf('2026-09-10'),'2026-09-10');
// An evening Eastern timestamp is the NEXT day in UTC; it must bucket back.
ck('a late-evening Eastern instant buckets to the Eastern day',ctx.etDayKeyOf('2026-09-11T02:00:00Z'),'2026-09-10');
ck('a morning instant stays on its own day',ctx.etDayKeyOf('2026-09-11T18:00:00Z'),'2026-09-11');
ck('epoch millis are accepted as a string',ctx.etDayKeyOf(String(Date.UTC(2026,8,11,2,0,0))),'2026-09-10');
// The 04:00–07:00 UTC band is where the two zones disagree — the whole point of
// the migration. 2026-10-12T05:00Z is Oct 12 in Eastern, Oct 11 in Pacific.
ck('the Eastern/Pacific disagreement band buckets Eastern now',
   ctx.etDayKeyOf('2026-10-12T05:00:00Z'),'2026-10-12');
ck('  and the day key agrees with the window bounds it is fetched by',
   (()=>{const k='2026-10-12';const t=Date.parse('2026-10-12T05:00:00Z');
     return t>=ctx.toMsET(k)&&t<=ctx.toMsET(k,true)&&ctx.etDayKeyOf(String(t))===k;})(),true);
ck('blank and unparseable values yield null, not a bogus day',
   [ctx.etDayKeyOf(''),ctx.etDayKeyOf(null),ctx.etDayKeyOf(undefined),ctx.etDayKeyOf('not a date')],
   [ctx.etDayKeyOf(''),ctx.etDayKeyOf(null),ctx.etDayKeyOf(undefined),ctx.etDayKeyOf('not a date')],
   [null,null,null,null]);

// Round trip: both ends of a Pacific day must bucket back to that same day.
// This is the property that keeps a window's first and last day inclusive —
// run over three whole years so no month, DST state or leap day is missed.
const rtBad=[];
for(const y of [2024,2025,2026]) for(let i=0;i<366;i++){
  const d=new Date(Date.UTC(y,0,1+i)); if(d.getUTCFullYear()!==y) break;
  const s=d.toISOString().slice(0,10);
  if(ctx.etDayKeyOf(ctx.toMsET(s))!==s) rtBad.push(['start',s]);
  if(ctx.etDayKeyOf(ctx.toMsET(s,true))!==s) rtBad.push(['end',s]);
}
// .slice(3): a broken offset fails all ~1100 days at once, and the full list
// buries every other result in the runner's output.
ck('start-of-day always buckets back to its own day',rtBad.filter(x=>x[0]==='start').slice(0,3),[]);
// toMsET computes BOTH ends with the start-of-day offset, so on the one
// spring-forward day a year the end instant lands just inside the next Pacific
// day. Pinned as a KNOWN, BOUNDED artifact: exactly one day per year, and the
// slip is under an hour — not a whole day, which is what would move rows.
ck('only the spring-forward day slips, once per year',
   rtBad.filter(x=>x[0]==='end').map(x=>x[1]),['2024-03-10','2025-03-09','2026-03-08']);
ck('and that slip is under one hour, never a whole day',
   ctx.toMsET('2026-03-08',true)-ctx.toMsET('2026-03-09')<3600000,true);
ck('end of day is just under 24h after the start',
   ctx.toMsET('2026-09-10',true)-ctx.toMsET('2026-09-10'),86399999);

// toMsUTC is the HubSpot DATE-property variant: those are stored at midnight
// UTC, so applying the Eastern offset to them shifts every deal back a day.
// The two must stay DIFFERENT functions.
ck('toMsUTC applies no offset',ctx.toMsUTC('2026-09-10'),Date.UTC(2026,8,10));
ck('toMsET does apply one, in EASTERN',ctx.toMsET('2026-09-10')-ctx.toMsUTC('2026-09-10'),4*3600000);
ck('and the offset follows DST',ctx.toMsET('2026-01-10')-ctx.toMsUTC('2026-01-10'),5*3600000);
// The window bounds and the day key have to agree, or a record inside the
// fetched window can bucket to a day outside it.
ck('the window bounds use the same zone as the day key',
   ctx.toMsET('2026-09-10')-ctx.toMsUTC('2026-09-10'),
   -ctx.etOff(new Date('2026-09-10T12:00:00Z'))*3600000);
ck('dateMs reads DATE properties at midnight UTC like toMsUTC',
   ctx.dateMs('2026-09-10'),ctx.toMsUTC('2026-09-10'));
ck('dateMs passes an epoch-ms string straight through',ctx.dateMs('1788998400000'),1788998400000);
ck('dateMs returns NaN for junk rather than 0',
   [isNaN(ctx.dateMs('')),isNaN(ctx.dateMs('nope'))],[true,true]);

// ---------------------------------------------------------------------------
// pagesForWindow — page depth scales with span, capped
// ---------------------------------------------------------------------------
// hsSearch's 10x200 default silently drops the overflow, and the default sort is
// ASCENDING, so it is the NEWEST rows that vanish. A YTD window looked like it
// was missing its latest months. Depth now scales with the span.
ck('a month or less keeps exactly the default depth — no extra latency',
   ['2026-10-01','2026-10-06','2026-10-31'].map(to=>ctx.pagesForWindow('2026-10-01',to)),[10,10,10]);
// 31 inclusive days is the last window that costs nothing extra; 32 steps up.
ck('the step sits exactly at 31 days',
   [ctx.pagesForWindow('2026-01-01','2026-01-31'),ctx.pagesForWindow('2026-01-01','2026-02-01')],[10,20]);
ck('crossing 31 days buys another block of pages',ctx.pagesForWindow('2026-01-01','2026-02-01'),20);
ck('two months is still 20',ctx.pagesForWindow('2026-01-01','2026-03-03'),20);
ck('a YTD-length window hits the cap',ctx.pagesForWindow('2026-01-01','2026-10-06'),25);
ck('an all-time window is capped at the same ceiling',ctx.pagesForWindow('2024-01-01','2026-10-06'),25);

ck('depth never drops below base or rises above max',(()=>{
  let ok=true;
  for(let d=0;d<900;d+=7){
    const to=new Date(Date.UTC(2024,0,1)+d*86400000).toISOString().slice(0,10);
    const p=ctx.pagesForWindow('2024-01-01',to);
    if(p<10||p>25)ok=false;
  }
  return ok;})(),true);
ck('depth is non-decreasing as the window grows',(()=>{
  let prev=0;
  for(let d=0;d<900;d+=3){
    const to=new Date(Date.UTC(2024,0,1)+d*86400000).toISOString().slice(0,10);
    const p=ctx.pagesForWindow('2024-01-01',to);
    if(p<prev)return false; prev=p;
  }
  return true;})(),true);

// Degenerate inputs must fall back to the default, never to 0 pages (which
// would fetch nothing at all).
ck('a missing bound falls back to the base',
   [ctx.pagesForWindow(null,'2026-10-06'),ctx.pagesForWindow('2026-01-01',null),ctx.pagesForWindow()],[10,10,10]);
ck('an unparseable bound falls back to the base',
   [ctx.pagesForWindow('nope','2026-10-06'),ctx.pagesForWindow('2026-01-01','')],[10,10]);
ck('a reversed range falls back to the base, not to zero',ctx.pagesForWindow('2026-10-06','2026-01-01'),10);
ck('base and max are overridable',
   [ctx.pagesForWindow('2026-01-01','2026-10-06',5,50),ctx.pagesForWindow('2026-10-01','2026-10-06',5,50)],[45,5]);

// Wiring: the long-window fetches must actually ask for the scaled depth, and
// they still sort ASCENDING — which is precisely why a capped fetch loses the
// most recent rows and why the scaling has to be there.
ck('the window-scaled depth reaches the hsSearch call sites',
   (src.match(/pagesForWindow\(from, to\)/g)||[]).length>=4,true);
ck('and those call sites still sort ascending',
   (src.match(/direction: 'ASCENDING' \}\], pagesForWindow\(from, to\)\)/g)||[]).length>=3,true);
ck('the deals helper lets a caller override the depth',
   /maxPages = opts\.maxPages \?\? pagesForWindow\(from, to\)/.test(src),true);

// THREE helpers bucket a timestamp into a day — etDayKeyOf, _clnDayKey and the
// local dayKey inside buildClinicianWeekly. They must agree: one returning
// Pacific while another returns Eastern is precisely the bug this migration
// fixed, and it is invisible except at the 3-hour boundary.
const _clnKey=fn(src,'_clnDayKey');
vm.runInContext(_clnKey+'\nObject.assign(globalThis,{_clnDayKey});',ctx);
const _tzProbe=['2026-10-12T01:57:42.807Z','2026-10-13T04:31:48.083Z','2026-10-12T05:00:00.000Z',
                '2026-01-15T04:30:00.000Z','2026-07-04T03:00:00.000Z','2026-09-10'];
ck('_clnDayKey agrees with etDayKeyOf on every probe',
   _tzProbe.map(v=>ctx._clnDayKey(v)===ctx.etDayKeyOf(v)),_tzProbe.map(()=>true));
// The live records behind the reported off-by-one, as HubSpot's own UI shows
// them (the portal is America/New_York).
ck('a 01:57Z create is the PREVIOUS Eastern day',
   ctx._clnDayKey('2026-10-12T01:57:42.807Z'),'2026-10-11');
ck('a 04:31Z create is the SAME Eastern day it is in UTC',
   ctx._clnDayKey('2026-10-13T04:31:48.083Z'),'2026-10-13');
// Under Pacific that second one bucketed to Oct 12; that is the record the
// migration actually moves.
ck('  which Pacific would have put a day earlier',
   (()=>{const t=Date.parse('2026-10-13T04:31:48.083Z');
     return ctx.fmt(new Date(t+ctx.ptOff(new Date(t))*3600000));})(),'2026-10-12');

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
