// Webinars folding into Qualified Demos.
//
// A Qualified Demo is a demo that was HELD (excl. pre-launch) OR a deal that
// reached Webinar Happened. The webinar half cannot come from the demo loop —
// those deals carry no date_demo_booked — so it is fetched separately and folded
// in. Two separate fetches feed it: fetchWebinarHeldCount (per window, for the
// page figure) and fetchWebinarHappenedAllTime (all time, for the Marketing
// Funnel's per-month column).
//
// EVERY failure mode here is silent. hsSearch logs and returns a PARTIAL array
// on an API error rather than throwing; hsCount returns null; a bad sort key is
// a 400 that becomes an empty array. On screen all of that looks exactly like
// "no webinars happened". So the assertions below are mostly about the guards
// that make a zero DETECTABLE and that stop a dead fetch from quietly rewriting
// the metric: the sort key, the requested properties, the brand filter, the
// null-vs-zero distinction, the fold order, and the log lines.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// Everything above `export default` is plain top-level declarations, so running
// that whole prefix resolves every dependency at once (t_fold.js does the same).
// Worker console output is captured rather than printed: the "say the count out
// loud" guards are asserted against it, and run.js reads the LAST line for the
// summary.
const logs=[];
const _log=(...a)=>logs.push(a.map(x=>typeof x==='string'?x:JSON.stringify(x)).join(' '));
const ctx={console:{log:_log,warn:_log,error:_log}};
vm.createContext(ctx);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+
  '\nObject.assign(globalThis,{filterActiveBrands,applyWebinarHeld,buildMarketingFunnel,' +
  'fetchWebinarHappenedAllTime,fetchWebinarHeldCount,toMsET,WEBINAR_HAPPENED_STAGE,PRE_LAUNCH_TIER});',ctx);

const STAMP='hs_v2_date_entered_'+ctx.WEBINAR_HAPPENED_STAGE;

(async () => {

// ── fetchWebinarHappenedAllTime: the query contract ───────────────────────────
ck('the real all-time fetch loaded',typeof ctx.fetchWebinarHappenedAllTime,'function');
let hsArgs=null;
// Swap the real hsSearch for a recorder. The function under test is the shipped
// one; only its one dependency is stubbed.
ctx.hsSearch=async(token,objectType,filterGroups,properties,limit,sorts,maxPages)=>{
  hsArgs={token,objectType,filterGroups,properties,limit,sorts,maxPages};
  return [{id:'w1'},{id:'w2'},{id:'w3'}];
};
const rows=await ctx.fetchWebinarHappenedAllTime('tok');
ck('it searches deals',hsArgs.objectType,'deals');
ck('selected by the presence of a Webinar Happened stage-entry stamp',
   hsArgs.filterGroups,[{filters:[{propertyName:STAMP,operator:'HAS_PROPERTY'}]}]);

// THE regression this suite exists for: the sort was on the stage-entry stamp,
// which HubSpot will not sort a search by. The request 400s, hsSearch logs the
// status and returns [], and the funnel's webinar column read zero with nothing
// on screen to say the fetch had died.
ck('it sorts on createdate',hsArgs.sorts[0].propertyName,'createdate');
ck('and never on the un-sortable stage-entry stamp',
   hsArgs.sorts.some(s=>s.propertyName===STAMP),false);
ck('no sort key is a stage-entry stamp at all',
   hsArgs.sorts.every(s=>!/^hs_v2_date_entered_/.test(s.propertyName)),true);
// hsSearch truncates silently at the page cap, so the sort direction decides
// WHICH end is lost. Newest-first keeps the months anyone is actually looking at.
ck('newest first',hsArgs.sorts[0].direction,'DESCENDING');
ck('it pages past hsSearch’s default cap of 10',hsArgs.maxPages>=20,true);
ck('with full pages',hsArgs.limit,200);

for(const p of [STAMP,'brand_status','average_monthly_web_traffic__cloned_','createdate'])
  ck('it requests '+p,hsArgs.properties.includes(p),true);

// The count is logged because an empty array here is indistinguishable on screen
// from "no webinars happened" — which is precisely how this read zero for weeks.
ck('it returns the rows',rows.length,3);
ck('and says the count out loud',logs.some(l=>new RegExp('funnel webinars: 3 deals with a '+STAMP).test(l)),true);
logs.length=0;
ctx.hsSearch=async()=>[];
const empty=await ctx.fetchWebinarHappenedAllTime('tok');
ck('a zero result is announced too, not passed over in silence',
   logs.some(l=>/funnel webinars: 0 deals/.test(l)),true);
ck('and is still an array',Array.isArray(empty),true);


// ── brand_status: why the property has to be in the request ──────────────────
const wb=(id,bs)=>({id,properties:{brand_status:bs,[STAMP]:'2026-01-15T00:00:00Z'}});
ck('paying, signed and unset webinar brands survive the filter',
   ctx.filterActiveBrands([wb('a','Paying'),wb('b','Signed'),wb('c',''),
                           wb('d','Churned'),wb('e','Paused'),
                           wb('f','Never Implemented - Churned')]).map(d=>d.id),
   ['a','b','c']);
ck('brand_status is matched case-insensitively',ctx.filterActiveBrands([wb('x','PAYING')]).length,1);
// A row fetched WITHOUT brand_status has properties.brand_status === undefined,
// and filterActiveBrands KEEPS an unset status (new pipeline deals have none).
// Dropping the property from the request therefore does not error — it silently
// re-admits every churned brand into the Qualified Demo count.
ck('a row missing brand_status is kept, which is why it must be requested',
   ctx.filterActiveBrands([{id:'n',properties:{}}]).length,1);
ck('and so is a row with no properties object at all',
   ctx.filterActiveBrands([{id:'n2'}]).length,1);

// Wiring: the fetched rows reach the funnel only through filterActiveBrands.
ck('the all-time webinars run through filterActiveBrands',
   /const _mfWebActive = filterActiveBrands\(_mfWebinars \|\| \[\]\);/.test(src),true);
ck('and the filtered list is what the funnel is built from',
   /buildMarketingFunnel\(monthlyAdSpend, allTimeQualifiedForFunnel, filterActiveBrands\(allTimeClosedWon\), _mfWebActive\)/.test(src),true);
ck('the KV cache falls back to an empty array, not null',
   /kvCachedFetch\(env, 'alltime_webinar_happened_v1', 900,\s*\n\s*\(\) => fetchWebinarHappenedAllTime\(hsToken\), \[\]\)/.test(src),true);
ck('a throw in that fetch is logged rather than swallowed',
   /catch \(e\) \{ console\.error\('funnel webinar fetch failed:'/.test(src),true);
// Logs the pre-filter count as well as the post-filter one: without "of N
// fetched" a zero cannot be attributed to a dead fetch versus a filter that
// dropped everything.
ck('both funnel inputs are logged, before and after filtering',
   /funnel inputs: \$\{\(allTimeQualifiedForFunnel\|\|\[\]\)\.length\} held, \$\{_mfWebActive\.length\} webinars \(of \$\{\(_mfWebinars\|\|\[\]\)\.length\} fetched\)/.test(src),true);


// ── buildMarketingFunnel: webinars are ADDED to the held demos ───────────────
const monthOf=(f,k)=>f.months.find(m=>m.key===k);
const skeleton=ctx.buildMarketingFunnel({},[],[],[]);
// Take the month key from the function's own output rather than from today's
// date — the row for a month outside the generated list would never be found and
// every count assertion below would pass vacuously.
ck('the funnel always starts at Jan 2026',skeleton.months[0].key,'2026-01');
const MK=skeleton.months[0].key, SPEND={byMonth:{[MK]:1200}};
const BIG='200K+ monthly web visitors';
const held=(tier)=>({properties:{date_demo_booked:MK+'-15',average_monthly_web_traffic__cloned_:tier||BIG}});
const web=(stamp,tier)=>({properties:{[STAMP]:stamp,average_monthly_web_traffic__cloned_:tier||BIG}});

const base=ctx.buildMarketingFunnel(SPEND,[held(),held()],[],[]);
ck('held demos alone',monthOf(base,MK).qualified,2);
const withWeb=ctx.buildMarketingFunnel(SPEND,[held(),held()],[],[web(MK+'-15T12:00:00Z')]);
ck('a webinar is ADDED to the held count, not substituted for it',monthOf(withWeb,MK).qualified,3);
ck('CPQD divides the month’s spend by the combined figure',monthOf(withWeb,MK).cpqd,1200/3);
// The funnel used to count held demos only, so its CPQD divided by a different
// population than the Efficiency card and the two could never agree.
ck('so a webinar lowers funnel CPQD',monthOf(withWeb,MK).cpqd<monthOf(base,MK).cpqd,true);
ck('and the totals row carries the webinars too',withWeb.totals.qualified,3);
ck('totals CPQD uses the same combined figure',withWeb.totals.cpqd,1200/3);

// The stamp arrives as epoch ms from HubSpot and as an ISO string from a cached
// payload; both have to land in the same month or a webinar jumps months
// depending on where it was read from.
ck('an epoch-ms stamp buckets into the same month as an ISO one',
   monthOf(ctx.buildMarketingFunnel(SPEND,[],[],[web(String(Date.UTC(2026,0,15,12,0,0)))]),MK).qualified,1);
ck('a pre-launch webinar is excluded, same as a pre-launch demo',
   [monthOf(ctx.buildMarketingFunnel(SPEND,[],[],[web(MK+'-15T12:00:00Z',ctx.PRE_LAUNCH_TIER)]),MK).qualified,
    monthOf(ctx.buildMarketingFunnel(SPEND,[held(ctx.PRE_LAUNCH_TIER)],[],[]),MK).qualified],[0,0]);
// An undated webinar would otherwise be bucketed under whatever month a blank
// key resolves to instead of being left out.
ck('a webinar with no stamp is skipped',
   monthOf(ctx.buildMarketingFunnel(SPEND,[],[],[web('')]),MK).qualified,0);
// The fetch failing must degrade to "held demos only", not to an exception that
// takes the whole funnel down.
ck('a null webinar list leaves the held half standing',
   monthOf(ctx.buildMarketingFunnel(SPEND,[held()],[],null),MK).qualified,1);
ck('and an undefined one does the same',
   monthOf(ctx.buildMarketingFunnel(SPEND,[held()],[],undefined),MK).qualified,1);
ck('a month with no qualified demos reports no CPQD rather than Infinity',
   monthOf(ctx.buildMarketingFunnel(SPEND,[],[],[]),MK).cpqd,null);


// ── applyWebinarHeld: the page figure ────────────────────────────────────────
const period=(q,dq)=>({pipeline:{qualifiedRawCount:q,qualifiedRawScaleCount:q,disqualifiedRawCount:dq}});
const P=period(10,5);
ctx.applyWebinarHeld(P,5);
ck('webinars are added to the held count',P.pipeline.qualifiedRawCount,15);
ck('and to the scale count, so both headline figures move together',P.pipeline.qualifiedRawScaleCount,15);
ck('the webinar half stays legible on its own',P.pipeline.webinarHeldCount,5);
ck('the demo-only half is snapshotted before the fold',P.pipeline.qualifiedDemoOnlyCount,10);
// This is what lets the card print "X demos held + Y webinars" and have the two
// reconcile against their separate sources.
ck('the two halves reconstruct the whole',
   P.pipeline.qualifiedDemoOnlyCount+P.pipeline.webinarHeldCount,P.pipeline.qualifiedRawCount);
ck('the qualification rate runs on the folded count',P.pipeline.qualificationRate,75);
ck('the denominator it used is stated alongside it',P.pipeline.qualRateDenom,20);
ck('qualification and disqualification rates partition 100%',
   P.pipeline.qualificationRate+P.pipeline.disqualificationRate,100);

// The rate used to run on qualOutcomeQualified, which webinars never touch, so
// it sat still while the count beside it climbed.
const noWeb=period(10,5);ctx.applyWebinarHeld(noWeb,0);
ck('with no webinars the rate is the demo-only one',noWeb.pipeline.qualificationRate,(10/15)*100);
ck('a webinar lifts it',P.pipeline.qualificationRate>noWeb.pipeline.qualificationRate,true);

// hsCount returns NULL (never 0) when a search is throttled or fails, and the
// fold skips a null. A failed fetch therefore leaves the demo half standing
// instead of folding a fake zero on top of it.
const failed=period(10,5);ctx.applyWebinarHeld(failed,null);
ck('a failed webinar fetch does not touch the held count',failed.pipeline.qualifiedRawCount,10);
ck('and does not invent a webinarHeldCount of 0',('webinarHeldCount' in failed.pipeline),false);
// The rate recompute is deliberately OUTSIDE the `n != null` branch: a transient
// 429 must not revert the rate to the pre-webinar definition.
ck('but the rate is still recomputed on the current definition',
   failed.pipeline.qualificationRate,(10/15)*100);
ck('an explicit zero is recorded as a real zero',
   (()=>{const z=period(10,5);ctx.applyWebinarHeld(z,0);return z.pipeline.webinarHeldCount;})(),0);

const zero=period(0,0);ctx.applyWebinarHeld(zero,0);
ck('an empty period yields 0, not NaN',
   [zero.pipeline.qualificationRate,zero.pipeline.disqualificationRate],[0,0]);
ck('a null period is a no-op rather than a throw',ctx.applyWebinarHeld(null,5),undefined);
ck('so is a period with no pipeline',ctx.applyWebinarHeld({},5),undefined);
// Folded once per period today. If it ever ran twice the demo-only snapshot must
// NOT be overwritten with the already-folded figure, or the card's split would
// start subtracting webinars from a number that already contains them.
const twice=period(10,5);ctx.applyWebinarHeld(twice,5);ctx.applyWebinarHeld(twice,5);
ck('the demo-only snapshot is taken exactly once',twice.pipeline.qualifiedDemoOnlyCount,10);


// ── fetchWebinarHeldCount: null, never a fake zero ───────────────────────────
let cntArgs=null;
ctx.hsCount=async(token,objectType,filterGroups)=>{cntArgs={token,objectType,filterGroups};return 7;};
ck('the per-window count comes back as a number',
   await ctx.fetchWebinarHeldCount('tok','2026-09-01','2026-09-30'),7);
ck('counted on deals',cntArgs.objectType,'deals');
const flt=cntArgs.filterGroups[0].filters;
ck('bounded on the stage-entry stamp, not the current stage',
   flt.map(x=>x.propertyName),[STAMP,STAMP]);
ck('as a closed GTE/LTE window',flt.map(x=>x.operator),['GTE','LTE']);
// Counted by stage ENTRY, so a webinar that happened and then moved on to Closed
// Won still counts — it has left the stage but it still happened.
// ET day boundaries, matching every other window on the page; toMsUTC here would
// shift the edges and move webinars across the month boundary.
ck('on ET day boundaries',[flt[0].value,flt[1].value],
   [String(ctx.toMsET('2026-09-01')),String(ctx.toMsET('2026-09-30',true))]);
ck('a missing window returns null, not 0',await ctx.fetchWebinarHeldCount('tok',null,'2026-09-30'),null);
ck('a missing token returns null, not 0',await ctx.fetchWebinarHeldCount('','2026-09-01','2026-09-30'),null);

// null-vs-0 is the whole mechanism by which applyWebinarHeld can tell a dead
// fetch from a genuinely webinar-free window. A 0 on any error path would be
// folded in as real.
const hc=fn(src,'hsCount');
ck('no hsCount error path returns 0',/return 0/.test(hc),false);
ck('a non-ok response returns null',
   /if \(!r\.ok\) \{[^}]*console\.error\(`hsCount \$\{objectType\} \$\{r\.status\}`\); return null; \}/.test(hc),true);
// ...and releases the connection on the way out. An unread body holds a slot in
// Cloudflare's concurrent-fetch pool until the runtime cancels something else to
// break the deadlock.
ck('and drains the body first',/if \(!r\.ok\) \{ azDrain\(r\);/.test(hc),true);
ck('a body with no numeric total returns null',/typeof d\.total === 'number' \? d\.total : null/.test(hc),true);
ck('and exhausted retries are logged, not returned as a count',
   /console\.error\(`hsCount \$\{objectType\}: still rate limited after retries`\);\s*\n\s*return null;/.test(hc),true);

// The premise behind every guard above: hsSearch NEVER throws on an API error.
// It logs and hands back whatever it collected, so a dead fetch arrives as a
// short — often empty — array that looks exactly like a real result.
// decl, not fn: hsSearch's default parameters contain a `{` of their own, and
// slice.js's brace matcher starts counting at the first brace it sees.
const hs=decl(src,'async function hsSearch(','\n  return all;\n}');
ck('hsSearch returns PARTIAL results on a non-ok response',
   /returning PARTIAL \$\{all\.length\} results[\s\S]{0,80}return all;/.test(hs),true);
ck('and on an exhausted retry loop',
   /returning PARTIAL \$\{all\.length\}:[\s\S]{0,80}break;/.test(hs),true);
ck('silent truncation at the page cap is at least warned about',
   /TRUNCATED at \$\{all\.length\}/.test(hs),true);
ck('and the warning names which end of the sort was dropped',
   /the \$\{_dir === 'ASCENDING' \? 'NEWEST' : 'OLDEST'\} records are missing/.test(hs),true);


// ── One canonical figure, read everywhere ────────────────────────────────────
ck('the canonical field is the post-fold count',
   /qualDemoCount: c\.pipeline\.qualifiedRawCount\|\|0/.test(src),true);
ck('its priors read the same field',
   /qualDemoCountPrior: p\.pipeline\?\.qualifiedRawCount \?\? null/.test(src)
   &&/qualDemoCountLastMonth: pm\.pipeline\?\.qualifiedRawCount \?\? null/.test(src),true);
ck('the Efficiency CPQD denominator is that same field',
   /const totalQualified = c\.pipeline\.qualifiedRawCount \|\| 0;/.test(src),true);
ck('so is the Qualified Demos tile',
   /totalQualifiedDemos: buildTile\(c\.pipeline\.qualifiedRawCount\|\|0/.test(src),true);
ck('the demo-only figure is published beside it, not as the headline',
   /qualDemoOnlyCount: c\.pipeline\.qualifiedDemoOnlyCount\|\|0/.test(src),true);
ck('and the webinar half is published too, so the figure can be reconciled',
   /webinarHeldCount: c\.pipeline\.webinarHeldCount\|\|0/.test(src),true);

// buildResponse reads c.pipeline.qualifiedRawCount off the period objects that
// applyWebinarHeld mutates. Fold after it and every emitted figure silently
// loses its webinar half while still being labelled "Qualified Demos".
ck('the fold runs before the response is built',
   src.indexOf('applyWebinarHeld(currentData') < src.indexOf('const resp = buildResponse('),true);
ck('all three periods are folded, so the deltas compare like with like',
   (src.match(/applyWebinarHeld\((?:currentData|priorData|priorMonthData),/g)||[]).length,3);
ck('the fold sits outside the try, so a thrown fetch still leaves the rate current',
   /\} catch \(e\) \{ console\.error\('webinar held count failed:'[\s\S]{0,400}?applyWebinarHeld\(currentData, _wbCur\);/.test(src),true);
ck('the per-period webinar counts are logged',
   /webinars held: cur=\$\{_wbCur\} prior=\$\{_wbPri\} pm=\$\{_wbPm\}/.test(src),true);

// One derivation in the client, four displays reading it. Counting lines rather
// than matches: the single derivation names the field twice in one ternary.
ck('the client derives the figure in exactly one place',
   cli.split('\n').filter(l=>/qmeta\.qualDemoCount\b/.test(l)).length,1);
for(const [what,re_] of [
  ['the Demo Insights card',/statRow\('Qualified Regular Demos','Demos held excl\. pre-launch \+ Webinars Happened',fN\(qd\)/],
  ['the Efficiency CPQD',/\$\{fN\(qd\)\} qualified demos/],
  ['the Conversion Funnel',/const held=qd,heldPV=qdPV,heldLMV=qdLMV;/],
  ['the trend item',/trendItem\('Qualified Demos','Demos held excl\. pre-launch \+ webinars held',fN\(qd\)\)/],
]) ck(what+' reads that one binding',re_.test(cli),true);
// The split is presentational: webinars are subtracted OUT of the canonical
// figure. Adding them on top would double-count them, which is what happened
// when the card read demosHeldCount (webinars never in it) and subtracted.
ck('the card splits the canonical figure',
   /const _qdWeb=qmeta\.webinarHeldCount\|\|0,_qdDemo=Math\.max\(0,qd-_qdWeb\)/.test(cli),true);
ck('nothing adds the webinar count back on top of it',
   /\bqd\s*\+\s*(?:_qdWeb|qmeta\.webinarHeldCount)/.test(cli),false);
// Clamped: a stale payload whose webinarHeldCount outruns qd must show 0 demos
// held, not a negative.
ck('the demo half is clamped at zero',/Math\.max\(0,qd-_qdWeb\)/.test(cli),true);
ck('a payload predating the field degrades to demosHeldCount instead of blanking',
   /qmeta\.qualDemoCount!=null\?qmeta\.qualDemoCount:\(esM\.demosHeldCount\|\|0\)/.test(cli),true);

// A cached response can mask all of this. Not pinning the exact version — it
// bumps every deploy — only that it never goes back below the one where the
// webinar fold landed.
ck('the response cache version is at or past the fold',
   (()=>{const m=src.match(/API_CACHE_VER = 'v(\d+)'/);return m?Number(m[1])>=21:false;})(),true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);

})();
