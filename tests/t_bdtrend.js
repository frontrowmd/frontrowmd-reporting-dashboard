// Brand Dashboard Trend Analysis. The section is the Clinician one's shape over
// this page's populations, so the assertions that matter are the ones about
// where the two DIFFER (Regular/Paid demos, Signed) and the ones about the
// invariants both must keep (tie-out to the tiles above, null-vs-zero, calendar
// fill, rates pooled rather than averaged).
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const prefix=src.slice(0,src.indexOf('\nexport default'));
const ctx={console};vm.createContext(ctx);
vm.runInContext(prefix+'\nObject.assign(globalThis,{buildBrandTrend,windsorRowChannel,clnTrendWindow,KPI_EXCLUDED_CHANNELS,mapUtmToChannel,processAdSpend});',ctx);
ck('the real builder loaded',typeof ctx.buildBrandTrend,'function');

// ── Fixtures ───────────────────────────────────────────────────────────────
const C=(day,wt,src_,med)=>({id:day+'|'+wt+'|'+(src_||'')+'|'+Math.random(),
  properties:{createdate:day+'T18:00:00Z',average_monthly_web_traffic:wt,utm_source:src_||'',utm_medium:med||''}});
const D=(day)=>({properties:{closedate:day+'T18:00:00Z'}});
const AD=(day,ds,camp,sp,cl,im)=>({date:day,datasource:ds,campaign_name:camp,spend:String(sp),clicks:String(cl),impressions:String(im)});

const FROM='2026-10-01', TO='2026-10-07';
const contacts=[
  C('2026-10-01','50-100K monthly web visitors','facebook','paid-social'), // regular + paid
  C('2026-10-01','50-100K monthly web visitors','',''),                     // regular, untagged
  C('2026-10-01','Pre-launch / just launching','facebook','paid-social'),   // pre-launch: neither
  C('2026-10-02','0-10K monthly web visitors','linkedin','cpc'),            // regular + paid
  C('2026-10-02','Very Small','newsletter','email'),                        // regular, organic-tagged
];
const cw=[D('2026-10-01'),D('2026-10-02')];
const churn=[D('2026-10-02')];
const ads=[
  AD('2026-10-01','facebook','S01 - TOF - Cold - Supplements',100,50,5000),
  AD('2026-10-01','google_ads','Brand Search',40,20,1000),
  // Clinician campaigns are KPI-excluded, exactly as on the Ad Spend tile.
  AD('2026-10-01','facebook','S01 - TOF - Clinician Targeting - CAPI',999,999,999),
  AD('2026-10-02','facebook','S01 - TOF - Cold - Supplements',60,30,3000),
  // GA4 rows ride the same array and are not ad rows at all.
  AD('2026-10-02','googleanalytics4','(none)',777,777,777),
];
const t=ctx.buildBrandTrend(contacts,cw,churn,ads,FROM,TO);
const byDay={};t.day.forEach(p=>byDay[p.wk]=p);
const d1=byDay['2026-10-01'],d2=byDay['2026-10-02'];

// ── 1. The two populations ────────────────────────────────────────────────
ck('regular excludes pre-launch',d1.regular,2);
ck('paid is the UTM-attributed subset of regular',d1.paid,1);
ck('  and never exceeds it',t.day.every(p=>p.paid<=p.regular),true);
ck('a pre-launch contact is in NEITHER population',d1.regular+d1.paid,3);
ck('a webinar (Very Small) contact still counts as regular',d2.regular,2);
ck('  but an organic UTM is not paid',d2.paid,1);

// ── 2. Ad spend ties to the Ad Spend tile, not to "all Windsor rows" ──────
ck('clinician spend is excluded, like the KPI total',d1.spend,140);
ck('  (the raw rows that day summed to 1139)',100+40+999,1139);
ck('GA4 rows are not ad rows',d2.spend,60);
ck('clicks and impressions follow the same exclusion',[d1.clicks,d1.impressions],[70,6000]);
// The exclusion is the SHARED mapper, so it cannot drift from processAdSpend.
ck('clinician campaigns map to their own channel',ctx.windsorRowChannel(ads[2]),'clinician');
ck('  which is the KPI-excluded one',ctx.KPI_EXCLUDED_CHANNELS.has('clinician'),true);
ck('a GA4 row maps to nothing',ctx.windsorRowChannel(ads[4]),'');
ck('meta / google still map',[ctx.windsorRowChannel(ads[0]),ctx.windsorRowChannel(ads[1])],['meta','google']);
ck('a YouTube-named Google campaign splits off',ctx.windsorRowChannel(AD('x','google_ads','YT Brand',1,1,1)),'youtube');
// processAdSpend must be the SAME mapper, or the trend and the tile diverge.
ck('processAdSpend routes through it',/const key = windsorRowChannel\(row\);/.test(src),true);
// processAdSpend's own copy of the if-chain is gone. rowToDashCh still has a
// similar chain, but it is a DIFFERENT, narrower mapper (no clinician split, no
// reddit/chatgpt) feeding processCampaigns — pinned here so nobody "dedupes"
// the two into one and silently moves clinician spend back into Meta.
ck('  and processAdSpend has no copy of its own',
   /function processAdSpend[\s\S]{0,900}?\/linkedin\/\.test\(ds\)/.test(src),false);
ck('rowToDashCh is a separate, narrower mapper and stays that way',
   /function rowToDashCh[\s\S]{0,400}?\/linkedin\/\.test\(ds\)/.test(src),true);
ck('  it does NOT split clinician out',
   /function rowToDashCh[\s\S]{0,400}?isClinicianCampaign/.test(src),false);

// ── 3. Signed = closed-won PLUS churned-that-kept-a-close-date ────────────
// The STAT was removed from the card 2026-10-08; the series still carries it,
// deliberately, so restoring it is a client-only change. These assertions keep
// it correct in the meantime rather than letting it rot into a wrong number
// that looks ready to use.
ck('the series still carries Signed, and says it is unread',
   /NOT CURRENTLY DISPLAYED/.test(src),true);
ck('signed counts both halves',[d1.signed,d2.signed],[1,2]);
ck('  so the window total is 3, not 2',t.day.reduce((a,p)=>a+p.signed,0),3);

// ── 4. Null vs zero ───────────────────────────────────────────────────────
const d3=byDay['2026-10-03'];
ck('a dead day still produces a point (calendar fill)',!!d3,true);
ck('  with zero counts',[d3.regular,d3.paid,d3.signed,d3.spend],[0,0,0,0]);
ck('a COST with no spend is null, not a fake-efficient 0',d3.cpdReg,null);
ck('a RATE with no denominator is null',[d3.cvrReg,d3.ctr],[null,null]);
// A real zero over a real denominator must survive as 0 — it is a measurement.
// THE case the cost guard exists for: real demos, zero ad spend. A 0 here would
// plot on the floor of the cost axis and read as the most efficient period in
// the window. Guarding only the denominator would let that through.
const organic=ctx.buildBrandTrend([C('2026-10-04','500K+ monthly web visitors','','')],[],[],[],FROM,TO);
const o4=organic.day.filter(p=>p.wk==='2026-10-04')[0];
ck('an organic demo with no spend is a real demo',o4.regular,1);
ck('  but its cost per demo is null, NOT zero',o4.cpdReg,null);
ck('  and the same for the paid population',o4.cpdPaid,null);

const z=ctx.buildBrandTrend([],[],[],[AD('2026-10-01','facebook','Cold',50,10,1000)],FROM,TO);
ck('zero demos against real clicks is 0%, not null',z.day[0].cvrReg,0);
ck('  but cost per zero demos is still null',z.day[0].cpdReg,null);
ck('  and CTR is a real number',z.day[0].ctr,1);

// ── 5. Rates are pooled over the period, never averaged across days ───────
const wk=t.week[0];
// Oct 1 2026 is a Thursday and Oct 5 a Monday, so this window straddles two ISO
// weeks — the fill emits both, including the one with no data in it.
ck('two ISO week buckets',t.week.length,2);
ck('  the first starting Monday Sep 28',wk.wk,'2026-09-28');
ck('  the second starting Monday Oct 5',t.week[1].wk,'2026-10-05');
ck('  and the empty one is still a point',
   [t.week[1].regular,t.week[1].spend],[0,0]);
ck('week CTR pools the period totals',wk.ctr,(wk.clicks/wk.impressions)*100);
ck('  which is NOT the mean of the daily CTRs',
   Math.abs(wk.ctr-((d1.ctr+d2.ctr)/2))>1e-9,true);
ck('week sums its days',[wk.regular,wk.paid,wk.signed,wk.spend],[4,2,3,200]);

// ── 6. Both populations are precomputed, so the toggle costs no request ───
ck('per-period cost exists for each population',
   [typeof wk.cpdReg,typeof wk.cpdPaid],['number','number']);
ck('paid costs more per demo than regular (same spend, smaller denominator)',wk.cpdPaid>wk.cpdReg,true);
ck('signed % is offered against each population',
   [wk.signedPctReg,wk.signedPctPaid],[(3/4)*100,(3/2)*100]);

// ── 7. Grains ─────────────────────────────────────────────────────────────
ck('three grains ship',Object.keys(t).sort(),['day','month','week']);
ck('day grain fills the whole window',t.day.length,7);
ck('month grain keys on the first of the month',t.month[0].wk,'2026-10-01');
ck('every grain sums to the same totals',
   ['regular','paid','signed'].every(k=>
     t.day.reduce((a,p)=>a+p[k],0)===t.month.reduce((a,p)=>a+p[k],0)),true);

// ── 8. The lookback is shared with the Clinician section ──────────────────
const win=ctx.clnTrendWindow('2026-10-08');
ck('100 days inclusive',[win.from,win.to],['2026-07-01','2026-10-08']);
ck('the Brand path reuses clnTrendWindow rather than its own constant',
   /const _btWin = clnTrendWindow\(fmt\(yd\)\)/.test(src),true);
ck('  so there is exactly one lookback constant',(src.match(/_TREND_DAYS = \d+/g)||[]).length,1);

// ── 9. Wiring: the payload, the cache, and the fetches ────────────────────
ck('the series is built onto currentData',/currentData\.trend = await kvCachedFetch/.test(src),true);
ck('  with its range, so the card can state it',/currentData\.trendRange = _btWin/.test(src),true);
// Building it is NOT shipping it. buildResponse forwards fields one by one
// instead of spreading `current`, so a field attached to currentData and not
// named there is dropped in total silence: the worker logs a healthy series and
// the client renders "No trend data available yet". That is exactly what
// happened on first deploy.
const _BR=fn(src,'buildResponse');   // brace-matched: a fixed slice stops short of the return block
ck('buildResponse actually FORWARDS the series',/\n    trend: c\.trend \|\| null,/.test(_BR),true);
ck('  and the range beside it',/\n    trendRange: c\.trendRange \|\| null,/.test(_BR),true);
// The general form: every currentData.<field> = ... in processRequest must be
// named in buildResponse, or it never reaches the page.
ck('no currentData field is built and then dropped',
   (()=>{const set=[...new Set([...src.matchAll(/\bcurrentData\.([A-Za-z_$][\w$]*)\s*=/g)].map(m=>m[1]))];
         // Leading underscore = internal, not part of the payload contract.
         // NOTE: _webinarDebug's own comment says it is surfaced "for the
         // Network-tab inspection", which it is not — it is dropped here like
         // any other unforwarded field. Pre-existing and debug-only.
         const missing=set.filter(k=>k[0]!=='_').filter(k=>!new RegExp('\\n\\s+'+k+':').test(_BR));
         if(missing.length)console.log('      dropped: '+missing.join(', '));
         return missing;})(),[]);
ck('the build is KV-cached, not recomputed per page window',
   /brand_trend_\$\{_btWin\.from\}_\$\{_btWin\.to\}_v1/.test(src),true);
ck('an empty Windsor result is never cached',
   /brand trend: no Windsor rows — not caching/.test(src),true);
ck('closed-won is sliced from the all-time set, costing no fetch',
   /filterActiveBrands\(allTimeClosedWon\)[\s\S]{0,200}?ptDayKeyOf\(\(\(d\.properties\) \|\| \{\}\)\.closedate\)/.test(src),true);
ck('HubSpot trend fetches stay sequential (429 avoidance)',
   /const _sch = await fetchScheduledContacts\(hsToken, _btWin\.from, _btWin\.to\);\n\s+const _chn = await fetchChurnedByCloseDate/.test(src),true);
// v37 added the fields to currentData; v38 is when they were actually
// forwarded, so a cached v37 response has them MISSING rather than stale.
ck('the forwarding fix bumped the cache version past v37',
   (+((src.match(/API_CACHE_VER = 'v(\d+)'/)||[])[1]||0))>=38,true);
// The page fetches must NOT be widened — their processors count every row.
ck('the page Windsor fetch still uses the page window',
   /fetchWindsorAds\(apiKey, wFrom\(current\.from\), current\.to\)/.test(src),true);
ck('the page contact fetch too',
   /fetchScheduledContacts\(hsToken, current\.from, current\.to\)/.test(src),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
