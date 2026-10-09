// Signed Brands / Signed Deals.
//
// Two cohort builders in the worker, both reached through the dedicated
// /api/special1-cohorts endpoint:
//   buildSignedTierCohort  — won deals bucketed by web-traffic tier for an
//                            arbitrary window, deduped by deal id because the
//                            two won-deal fetches (by closedate, by entered-won
//                            stamp) overlap heavily.
//   buildSpecial1Cohort    — the demos-booked cohort, whose demosHeld is
//                            ATTENDANCE-based plus the webinar count, NOT a
//                            sum of current dealstages.
// Plus the Detailed Dashboard section that renders them: one MTD / Last Month
// toggle for the whole section, both tiles reading the same payload.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// Both builders lean on toMsET / dateMs / demoDidHappen / isSmallBrandWT /
// attBump / attRates. Running the whole top-level prefix resolves all of them
// at once rather than hand-picking a dependency list that rots.
const prefix=src.slice(0,src.indexOf('\nexport default'));
const wctx={console};vm.createContext(wctx);
vm.runInContext(prefix+'\nObject.assign(globalThis,{buildSignedTierCohort,buildSpecial1Cohort,toMsET,demoDidHappen});',wctx);
const B=wctx.buildSignedTierCohort, S1=wctx.buildSpecial1Cohort;
ck('the real cohort builders loaded',[typeof B,typeof S1],['function','function']);

// ── buildSignedTierCohort ──────────────────────────────────────────────────
// Shape of a row from fetchAnyWonDealsByCloseDate / ...ByEnteredWon.
const won=(id,closedate,cloned,amount,live)=>({id,properties:{
  closedate, average_monthly_web_traffic__cloned_:cloned,
  average_monthly_web_traffic:live, amount,
}});
const W={from:'2026-09-01',to:'2026-09-30'};
const bt=(rows)=>B(rows,W.from,W.to);

// THE reason this builder dedupes: the caller concatenates two fetches that
// return mostly the same deals (same deal, matched once on closedate and once
// on the entered-won stamp). Without the id map every overlapping deal would
// be double-counted in total, mrr AND its tier bucket.
{
  const byClose=[won('101','2026-09-10T12:00:00Z','200K+ monthly web visitors','500')];
  const byEnter=[won('101','2026-09-10T12:00:00Z','200K+ monthly web visitors','500')];
  const r=bt([...byClose,...byEnter]);
  ck('a deal in both won fetches counts once',r.total,1);
  ck('and its MRR is counted once',r.mrr,500);
  ck('and its tier bucket is counted once',r.byWebTraffic,{'200K+ monthly web visitors':1});
}
{
  const r=bt([won('101','2026-09-10T12:00:00Z','A','500'),won('102','2026-09-11T12:00:00Z','A','250')]);
  ck('two distinct deals both count',[r.total,r.mrr],[2,750]);
}
// The tier buckets drive the donut; if they ever stopped summing to total the
// donut and the "N signed" headline would disagree.
{
  const r=bt([won('1','2026-09-02T12:00:00Z','A','1'),won('2','2026-09-03T12:00:00Z','B','1'),
              won('3','2026-09-04T12:00:00Z','A','1'),won('4','2026-09-05T12:00:00Z','C','1')]);
  const sum=Object.values(r.byWebTraffic).reduce((a,b)=>a+b,0);
  ck('the tier buckets sum to the total',[sum,r.total],[4,4]);
}
// closedate is often blank on churned deals and on some active ones, and the
// entered-won fetch happily returns those. No closedate = no window to fall in.
ck('a won deal with no closedate is dropped',bt([won('1','','A','900')]).total,0);
ck('and contributes no MRR',bt([won('1','','A','900')]).mrr,0);
// The entered-won fetch matches on the stamp, so it returns deals whose
// user-editable closedate sits OUTSIDE the window. The builder re-filters on
// closedate, which is what makes the window honest.
ck('a deal that entered won in-window but closes outside it is dropped',
   bt([won('1','2026-08-15T12:00:00Z','A','900')]).total,0);

// Tier key precedence — the cloned (deal-side) property wins, the live one is
// the fallback, and a deal with neither lands in its own bucket rather than
// silently joining a real tier.
ck('the cloned tier wins over the live property',
   bt([won('1','2026-09-10T12:00:00Z','CLONED','0','LIVE')]).byWebTraffic,{CLONED:1});
ck('the live property is the fallback',
   bt([won('1','2026-09-10T12:00:00Z','','0','LIVE')]).byWebTraffic,{LIVE:1});
ck('neither one reads as (none)',
   bt([won('1','2026-09-10T12:00:00Z','','0','')]).byWebTraffic,{'(none)':1});

// The window bounds are the shared day bounds (toMsET — legacy name, now
// Pacific). Pinned against the helper rather than a literal offset so a DST
// rule change moves both together instead of failing here.
{
  const lo=wctx.toMsET(W.from), hi=wctx.toMsET(W.to,true);
  const at=ms=>bt([won('1',new Date(ms).toISOString(),'A','0')]).total;
  ck('the first instant of the window is included',at(lo),1);
  ck('one ms earlier is not',at(lo-1),0);
  ck('the last instant of the window is included',at(hi),1);
  ck('one ms later is not',at(hi+1),0);
}
// Called straight off `[...(a||[]), ...(b||[])]`, so a failed fetch arrives as
// junk rather than as a thrown error. It must not take the section down.
ck('null rows yield an empty cohort rather than a throw',
   [B(null,W.from,W.to).total,B([null,undefined,{}],W.from,W.to).total],[0,0]);
ck('a non-numeric amount contributes 0, not NaN',
   bt([won('1','2026-09-10T12:00:00Z','A','not-a-number')]).mrr,0);
// The client prints the range under the tile from these, so they must echo
// the window it was asked for, not a window the builder re-derived.
ck('the window is echoed back for the tile label',
   [bt([]).fromDate,bt([]).toDate],[W.from,W.to]);

// ── buildSpecial1Cohort ────────────────────────────────────────────────────
let _n=0;
const deal=(o)=>({id:'d'+(++_n),properties:Object.assign({
  date_demo_booked:'2026-09-10',
  average_monthly_web_traffic__cloned_:'200K+ monthly web visitors',
},o)});
const HELD='Demo Given (originally scheduled)', HELD2='Demo Given (rescheduled)';
const S1W=(rows,webinars,from,to)=>S1(rows,from||W.from,to||W.to,webinars||0);

// THE invariant this suite exists for. A demo that was given and then lost
// still happened: it counts as held everywhere else on the dashboard, and a
// current-dealstage tally cannot see it. heldScale must come from attendance.
{
  const r=S1W([deal({demo_attendance_status:HELD,dealstage:'closedlost'})]);
  ck('a held demo that later moved to Closed Lost still counts as held',r.heldDemosOnly,1);
  ck('and the stage sum cannot see it',r.stageSumLegacy,0);
  ck('so heldScale follows attendance, not the stage sum',[r.heldScale,r.stageSumLegacy],[1,0]);
}
// The webinar fold is ADDITIVE because webinar deals carry no
// date_demo_booked — they cannot arrive through the pipeline fetch at all.
{
  const r=S1W([],4);
  ck('a webinar-only window still reports held demos',r.heldScale,4);
  ck('none of them came through the booked-demo fetch',[r.heldDemosOnly,r.allBooked],[0,0]);
}
{
  const r=S1W([deal({demo_attendance_status:HELD}),deal({demo_attendance_status:HELD2})],3);
  ck('heldScale is attendance held plus the webinar count',
     [r.heldDemosOnly,r.webinarHeld,r.heldScale],[2,3,5]);
}
// % Signed rides the same held count, so it must divide by heldScale — the
// webinar-inclusive figure — not by the stage sum.
{
  const r=S1W([deal({demo_attendance_status:HELD,dealstage:'closedwon',amount:'1000',closedate:'2026-09-20T00:00:00Z'}),
               deal({demo_attendance_status:HELD}),
               deal({demo_attendance_status:HELD})],1);
  ck('% Signed divides closed-won by heldScale',[r.signed,r.heldScale,r.pctSigned],[1,4,25]);
}
// A window with no demos and no webinars must print 0%, not NaN or Infinity.
{
  const r=S1W([deal({dealstage:'closedwon',amount:'500',closedate:'2026-09-20T00:00:00Z'})]);
  ck('no held demos gives 0% rather than a divide-by-zero',[r.heldScale,r.pctSigned,r.pctPending],[0,0,0]);
}
// Pre-launch brands are out of every Signed figure, and counted separately so
// the exclusion is visible rather than looking like missing data.
{
  const r=S1W([deal({demo_attendance_status:HELD,average_monthly_web_traffic__cloned_:'Pre-launch / just launching'}),
               deal({demo_attendance_status:HELD})]);
  ck('a pre-launch brand is excluded from the cohort',[r.allBooked,r.heldDemosOnly],[1,1]);
  ck('and reported as excluded rather than silently dropped',r.prelaunchExcluded,1);
}
// Small brands (0-10K / Very Small) DO count as held, but are kept out of the
// canonical % Pruned / % No-Show denominators — the same rule True CPD uses.
// Confusing the two is how the rates drift from the rest of the page.
{
  const r=S1W([deal({demo_attendance_status:HELD,average_monthly_web_traffic__cloned_:'0-10K monthly web visitors'})]);
  ck('a small brand counts as a held demo',r.heldDemosOnly,1);
  ck('but is excluded from the prune / no-show base',[r.attHeld,r.pruneDenom,r.noShowDenom],[0,0,0]);
}
// Attendance rates: pending demos sit in neither denominator, so an in-flight
// month is not scored as if everything had already settled.
{
  const r=S1W([deal({demo_attendance_status:HELD}),deal({demo_attendance_status:HELD}),
               deal({demo_attendance_status:'No Show'}),
               deal({demo_attendance_status:'Cancelled before demo'}),
               deal({demo_attendance_status:'Scheduled — pending'})]);
  ck('attendance splits into held / no-show / cancelled',
     [r.attHeld,r.attNoShow,r.attCancelled],[2,1,1]);
  ck('% No-Show is no-show over held + no-show',[r.noShowNum,r.noShowDenom],[1,3]);
  ck('% Pruned is cancelled over every settled outcome',[r.prunedNum,r.pruneDenom],[1,4]);
  ck('a pending demo is in neither denominator',[r.pruneDenom,r.noShowDenom],[4,3]);
}
// A Closed Won deal whose attendance stamp was never filled in is NOT counted
// as held here: this caller passes attendance only (no isWon override), so the
// rates stay purely attendance-driven.
{
  const r=S1W([deal({dealstage:'closedwon',amount:'100',closedate:'2026-09-20T00:00:00Z'})]);
  ck('a won deal with a blank attendance stamp does not inflate held',[r.signed,r.attHeld,r.heldDemosOnly],[1,0,0]);
}
// Older deals predate demo_attendance_status; the legacy field still decides.
ck('the legacy demo_given__status still marks a demo held',
   S1W([deal({demo_given__status:'Demo Given'})]).heldDemosOnly,1);

// The window filter runs on date_demo_booked, which HubSpot stores at midnight
// UTC — both edge days are inside, and a blank never is.
{
  const r=S1W([deal({date_demo_booked:'2026-08-31'}),deal({date_demo_booked:'2026-09-01'}),
               deal({date_demo_booked:'2026-09-30'}),deal({date_demo_booked:'2026-10-01'}),
               deal({date_demo_booked:''})]);
  ck('both edge days are inside the window, outside days and blanks are not',r.allBooked,2);
}
// A demo booked for next week has not happened yet, so it is not "pending" —
// counting it would make % Pending climb just from forward bookings.
{
  const r=S1W([deal({date_demo_booked:'2026-01-05',dealstage:'appointmentscheduled'}),
               deal({date_demo_booked:'2099-01-05',dealstage:'appointmentscheduled'})],
              0,'2026-01-01','2099-12-31');
  ck('only a past-dated Appointment Scheduled counts as pending',[r.stageCounts.appt,r.allBooked],[1,2]);
}
// % Pending is the four in-flight stages over the held scale.
{
  const r=S1W([deal({demo_attendance_status:HELD,dealstage:'1084214349'}),
               deal({demo_attendance_status:HELD,dealstage:'decisionmakerboughtin'}),
               deal({demo_attendance_status:HELD,dealstage:'contractsent'}),
               deal({demo_attendance_status:HELD,dealstage:'closedlost'})]);
  const pend=r.stageCounts.appt+r.stageCounts.demoHappened+r.stageCounts.dm+r.stageCounts.cs;
  ck('% Pending is the in-flight stages over heldScale',[pend,r.heldScale,r.pctPending],[3,4,75]);
}
// Avg Days: "To Demo" spans every booked deal; the two close metrics are
// Closed Won only. Mixing them would let unsigned deals move a close time.
{
  const r=S1W([deal({dealstage:'closedwon',amount:'1000',hs_createdate:'2026-09-01T00:00:00Z',
                     date_demo_booked:'2026-09-10',closedate:'2026-09-20T00:00:00Z',demo_attendance_status:HELD}),
               deal({dealstage:'1084214349',hs_createdate:'2026-09-04T00:00:00Z',
                     date_demo_booked:'2026-09-10',demo_attendance_status:HELD})]);
  ck('To Demo averages across every booked deal',[r.avgDaysToDemoN,r.avgDaysToDemo],[2,(9+6)/2]);
  ck('Close From Demo counts only the won deal',[r.avgDaysFromBookedN,r.avgDaysFromBooked],[1,10]);
  ck('Close From Created counts only the won deal',[r.avgDaysFromCreatedN,r.avgDaysFromCreated],[1,19]);
  // avgDaysToClose is the pre-rename alias; the client falls back to it.
  ck('avgDaysToClose still mirrors Close From Demo',r.avgDaysToClose,r.avgDaysFromBooked);
}
// A closedate backdated before the demo would otherwise drag the average
// negative. Negative spans are skipped, not clamped to zero.
{
  const r=S1W([deal({dealstage:'closedwon',amount:'100',date_demo_booked:'2026-09-10',
                     closedate:'2026-09-05T00:00:00Z',hs_createdate:'2026-09-01T00:00:00Z'})]);
  ck('a closedate before the demo is left out of the average',[r.avgDaysFromBookedN,r.avgDaysFromBooked],[0,null]);
}
// New ARR / ACV ride the won deals' MRR only.
{
  const r=S1W([deal({dealstage:'closedwon',amount:'1000',closedate:'2026-09-20T00:00:00Z'}),
               deal({dealstage:'closedwon',amount:'500',closedate:'2026-09-21T00:00:00Z'}),
               deal({dealstage:'contractsent',amount:'9999'})]);
  ck('New ARR is 12x the won MRR and ignores open deals',r.newArr,18000);
  ck('ACV is won MRR over won count',r.acv,750);
}
ck('no won deals means no ACV rather than NaN',S1W([deal({})]).acv,0);

// ── the Detailed Dashboard section ─────────────────────────────────────────
// The two tiles and the donut are real functions; run them so the toggle is
// tested as behaviour rather than as matching button markup.
const pieCalls=[];
const dctx={console,
  S:{dashboard:{signedMode:null,data:{}},irfan:{special1Data:null}},
  fN:v=>v==null?'—':Math.round(v).toLocaleString(),
  f$:v=>v==null?'—':'$'+Math.round(v).toLocaleString(),
  _pie:pieCalls,
  document:{getElementById:()=>({innerHTML:''})},
};
vm.createContext(dctx);
vm.runInContext(['var window=globalThis;',
  fn(cli,'_dashSignedBrandsHTML'),fn(cli,'_buildIrfanSpecial1Inner'),
  fn(cli,'_buildIrfanSpecial2Inner'),fn(cli,'_renderDashSignedPie'),
  'function _renderIrfanWtPieInto(id,buckets,kind){_pie.push([id,buckets,kind]);}',
  cli.slice(cli.indexOf('window.dashSignedMode=function(m){'),
            cli.indexOf('function _dashSignedBrandsHTML(d){')),
].join('\n'),dctx);
ck('the real section builder loaded',
   [typeof dctx._dashSignedBrandsHTML,typeof dctx._renderDashSignedPie,typeof dctx.dashSignedMode],
   ['function','function','function']);

// Deliberately distinct numbers per window so a tile reading the wrong one is
// visible rather than coincidentally equal.
const PAY={
  priorMonthHeldSigned:{heldScale:40,heldDemosOnly:35,webinarHeld:5,signed:4,pctSigned:10,
    newArr:480000,acv:30000,allBooked:50,pctPending:20,pctPruned:5,pctNoShow:3,
    prunedNum:1,pruneDenom:20,noShowNum:1,noShowDenom:30,avgDaysToDemo:5,
    avgDaysFromBooked:9,avgDaysFromCreated:14,stageCounts:{},fromDate:'2026-09-01',toDate:'2026-09-30'},
  mtdHeldSigned:{heldScale:11,heldDemosOnly:8,webinarHeld:3,signed:1,pctSigned:9.1,
    newArr:12000,acv:12000,allBooked:12,pctPending:30,pctPruned:4,pctNoShow:2,
    prunedNum:1,pruneDenom:25,noShowNum:1,noShowDenom:40,avgDaysToDemo:4,
    avgDaysFromBooked:7,avgDaysFromCreated:11,stageCounts:{},fromDate:'2026-10-01',toDate:'2026-10-06'},
  signedLastMonth:{byWebTraffic:{'200K+ monthly web visitors':7},total:7,mrr:900,fromDate:'2026-09-01',toDate:'2026-09-30'},
  signedMtd:{byWebTraffic:{'50K-200K monthly web visitors':2},total:2,mrr:100,fromDate:'2026-10-01',toDate:'2026-10-06'},
  signedTierError:null,
};
const render=(mode)=>{dctx.S.dashboard.signedMode=mode;pieCalls.length=0;
  const h=dctx._dashSignedBrandsHTML({});dctx._renderDashSignedPie();return h;};
// Tile 1's % Signed calc line and tile 2's sub-line each name their own
// window's numbers, which is how we tell which cohort each tile rendered.
const tile1Held=h=>{const m=/([\d,]+) signed ÷ ([\d,]+) qualified demos held/.exec(h);return m?+m[2].replace(/,/g,''):null;};
const tile2Signed=h=>{const m=/· ([\d,]+) signed/.exec(h);return m?+m[1].replace(/,/g,''):null;};

// Before the dedicated fetch lands the section says so. Rendering zeros here
// would read as "nothing signed this month".
{
  const h=dctx._dashSignedBrandsHTML({});
  ck('with no payload the section shows a loading state',/Loading signed-brand cohorts/.test(h),true);
  ck('and prints no cohort numbers at all',[tile1Held(h),tile2Signed(h)],[null,null]);
}
dctx.S.irfan.special1Data=PAY;

// One toggle, both tiles. The bug this guards: tile 2 kept its own MTD /
// "Last 2 Weeks" pills, so a 14-day cohort could sit under a "Last Month" pill.
{
  const hM=render('mtd'), hL=render('lastMonth');
  ck('MTD renders the MTD cohort in tile 1',tile1Held(hM),11);
  ck('and the MTD tier cohort in tile 2',tile2Signed(hM),2);
  ck('Last Month renders the Last Month cohort in tile 1',tile1Held(hL),40);
  ck('and the Last Month tier cohort in tile 2',tile2Signed(hL),7);
  ck('the tiles never disagree about the window',
     [tile1Held(hM)!==tile1Held(hL),tile2Signed(hM)!==tile2Signed(hL)],[true,true]);
  ck('each tile names its own window in the heading',
     [/Signed Deals — MTD/.test(hM),/Signed Deals — Last Month/.test(hL)],[true,true]);
}
// The donut is drawn after the markup and must follow the same toggle — a pie
// left on the other window is the classic version of this bug.
{
  render('mtd');
  ck('the donut draws into the page\'s own id',pieCalls.map(c=>c[0]),['dash-signed-pie']);
  ck('and uses the MTD buckets',pieCalls[0][1],PAY.signedMtd.byWebTraffic);
  render('lastMonth');
  ck('and the Last Month buckets on the other mode',pieCalls[0][1],PAY.signedLastMonth.byWebTraffic);
}
// Exactly one control for the section: both inner tiles are built with their
// own pills suppressed.
{
  const h=render('mtd');
  const n=(re)=>(h.match(re)||[]).length;
  ck('the section offers exactly two pills',n(/dashSignedMode\(/g),2);
  ck('the Signed Deals tile has no pills of its own',n(/irfanSetSpecial1Mode\(/g),0);
  ck('nor does the by-tier tile',n(/irfanSetSigned2Mode\(/g),0);
  ck('so "Last 2 Weeks" cannot appear in this section',/Last 2 Weeks/.test(h),false);
}
// The toggle has exactly two states; anything else falls back to MTD rather
// than leaving the section on an unrenderable mode.
{
  dctx.dashSignedMode('lastMonth');
  const a=dctx.S.dashboard.signedMode;
  dctx.dashSignedMode('bogus');
  const b=dctx.S.dashboard.signedMode;
  dctx.dashSignedMode();
  ck('the toggle normalises to exactly two states',[a,b,dctx.S.dashboard.signedMode],['lastMonth','mtd','mtd']);
}
// The whole point of the dedicated endpoint: the section is independent of the
// page date selector, so a page payload must not move these numbers.
{
  const stale={irfan:{mtdHeldSigned:{heldScale:999,signed:999,pctSigned:1,allBooked:1,stageCounts:{},
    pctPending:0,pctPruned:0,pctNoShow:0,fromDate:'2020-01-01',toDate:'2020-01-31'}}};
  dctx.S.dashboard.signedMode='mtd';
  ck('the page payload cannot override the dedicated cohort',
     tile1Held(dctx._dashSignedBrandsHTML(stale)),11);
}
// The cohort band spells out the attendance + webinar composition, so the
// additive webinar fold is visible on the page rather than buried.
{
  const h=render('mtd');
  ck('the held tile shows demos plus webinars',/8 demos \+ 3 webinars/.test(h),true);
  ck('and % Signed names heldScale as its denominator',/1 signed ÷ 11 qualified demos held/.test(h),true);
}
// A failed tier fetch surfaces as an error, not as a zero cohort.
{
  dctx.S.irfan.special1Data=Object.assign({},PAY,{signedTierError:'rate limited'});
  const h=render('mtd');
  ck('a signed-tier fetch failure is shown, not swallowed',/Failed to load: rate limited/.test(h),true);
  dctx.S.irfan.special1Data=PAY;
}

// ── wiring ────────────────────────────────────────────────────────────────
// Every re-render of the section must redraw the donut too; the markup is
// replaced wholesale, so an unpaired innerHTML leaves an empty circle.
ck('the section is rendered from three places',(cli.match(/_dashSignedBrandsHTML\(/g)||[]).length,4);
ck('and every one of them redraws the donut',
   (cli.match(/innerHTML=_dashSignedBrandsHTML\([^)]*\);_renderDashSignedPie\(\);/g)||[]).length,3);
ck('the section has its own slot on the Detailed page',
   /id="sec-signedbrands"><div id="t-signedbrands">/.test(cli),true);
// The Irfan page keeps its own card and its own toggle — only the Detailed
// copy suppresses the inner pills.
ck('the Irfan card still drives itself',/_buildIrfanSpecial1Inner\(d, S\.irfan\.special1Mode\)/.test(cli),true);

ck('the client calls the dedicated endpoint',/\/api\/special1-cohorts'/.test(cli),true);
ck('and the worker serves it',
   /url\.pathname === '\/api\/special1-cohorts'/.test(src)&&/await processSpecial1Request\(env\)/.test(src),true);
const PS1=fn(src,'processSpecial1Request');
// Each window is fetched BOTH ways; the overlap is exactly what the id dedupe
// in buildSignedTierCohort exists to absorb.
ck('each signed window is fetched by closedate and by entered-won',
   [(PS1.match(/fetchAnyWonDealsByCloseDate\(/g)||[]).length,
    (PS1.match(/fetchAnyWonDealsByEnteredWon\(/g)||[]).length],[2,2]);
ck('the two fetches are merged and brand-filtered before bucketing',
   /buildSignedTierCohort\(\s*filterActiveBrands\(\[\.\.\.\(wPcmClose\|\|\[\]\), \.\.\.\(wPcmEnter\|\|\[\]\)\]\)/.test(PS1)
   &&/buildSignedTierCohort\(\s*filterActiveBrands\(\[\.\.\.\(wMtdClose\|\|\[\]\), \.\.\.\(wMtdEnter\|\|\[\]\)\]\)/.test(PS1),true);
// The webinar count is fetched per window and handed to the cohort builder —
// the only route webinars have into demosHeld.
ck('a webinar count is fetched per window and passed in',
   /buildSpecial1Cohort\(deals, _pcmFromStr, _pcmToStr, _s1WbPcm\)/.test(PS1)
   &&/buildSpecial1Cohort\(deals, _mtdFromStr, _mtdToStr, _s1WbMtd\)/.test(PS1),true);
// All four fields the section reads must be on the payload; dropping one
// silently blanks half a tile.
for(const k of ['priorMonthHeldSigned','mtdHeldSigned','signedLastMonth','signedMtd','signedTierError'])
  ck('the payload carries '+k,new RegExp('\\n\\s*'+k+'[,:]').test(PS1),true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
