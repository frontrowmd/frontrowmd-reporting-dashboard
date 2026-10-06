// Qualification Rate. The rate RIDES the Qualified Demo definition: held demos
// excluding pre-launch, PLUS deals that reached Webinar Happened. It is
// qualifiedRawCount / (qualifiedRawCount + disqualifiedRawCount) — NOT the
// outcome fields (qualOutcomeQualified) it used to use, which ignored webinars
// entirely and so never moved when the webinar count did.
//
// The recompute lives in applyWebinarHeld rather than processPipelineDeals,
// because the webinar total is not known inside that loop. processPipelineDeals
// still computes an outcome-based rate; applyWebinarHeld OVERWRITES it. This
// suite runs both, in that order, so it fails if either half drifts.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');

// Everything above `export default` is plain top-level declarations, so running
// that whole prefix resolves applyWebinarHeld's and processPipelineDeals' many
// dependencies (dateMs, demoDidHappen, PRE_LAUNCH_TIER, the stage tables, ...)
// in one go rather than hand-listing them.
const prefix=src.slice(0,src.indexOf('\nexport default'));
const ctx={console};vm.createContext(ctx);
vm.runInContext(prefix+'\nObject.assign(globalThis,{applyWebinarHeld,processPipelineDeals,PRE_LAUNCH_TIER});',ctx);

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const r4=x=>x==null?x:Math.round(x*1e4)/1e4;

ck('the real functions loaded',[typeof ctx.applyWebinarHeld,typeof ctx.processPipelineDeals],['function','function']);

const HELD='Demo Given (originally scheduled)', RESCHED='Demo Given (rescheduled)';
const BIG='200K+ monthly web visitors', PRE=ctx.PRE_LAUNCH_TIER;
const F=Date.parse('2026-09-01T00:00:00Z'), T=Date.parse('2026-09-30T23:59:59Z');
let _id=0;
// Pre-launch is read off the DEAL's cloned tier (average_monthly_web_traffic__cloned_),
// not the contact-side company-name test — see t_prelaunch.js.
const mk=(att,qo,tier)=>({id:'d'+(++_id),properties:{
  date_demo_booked:'2026-09-10', demo_attendance_status:att,
  demo_qualification_outcome:qo||'', average_monthly_web_traffic__cloned_:tier||BIG,
  dealstage:'appointmentscheduled', hubspot_owner_id:'o1',
  hs_createdate:String(Date.parse('2026-09-10T00:00:00Z')),
}});
const run=deals=>ctx.processPipelineDeals(deals,F,T,F,T,new Set());

// ── A. The rate the payload ships is the POST-fold one ─────────────────────
const deals=[
  mk(HELD,'Qualified'),            // held + qualified
  mk(HELD,'Disqualified'),         // HELD then disqualified — in both numerator and denominator
  mk(RESCHED,'Not yet evaluated'), // held, no outcome yet — numerator only
  mk('No Show','Disqualified'),    // never held — denominator only
  mk(HELD,'Qualified',PRE),        // pre-launch — excluded from both sides
];
const pl=run(deals);
ck('held excl. pre-launch is the numerator population',pl.qualifiedRawCount,3);
ck('disqualified excl. pre-launch is the rest of the denominator',pl.disqualifiedRawCount,2);
ck('the outcome tally is a different, smaller number',pl.qualOutcomeQualified,1);

// processPipelineDeals still ships the OLD outcome-based rate. Pinned as a
// relationship, not a number: this is the value applyWebinarHeld must replace.
ck('before the fold the rate is still outcome-based',
   r4(pl.qualificationRate),r4(pl.qualOutcomeQualified/(pl.qualOutcomeQualified+pl.disqualifiedRawCount)*100));

const preFoldRate=pl.qualificationRate;
ctx.applyWebinarHeld({pipeline:pl},4);

ck('after the fold the numerator is the Qualified Demo count',
   r4(pl.qualificationRate),r4(pl.qualifiedRawCount/(pl.qualifiedRawCount+pl.disqualifiedRawCount)*100));
// The whole point of moving the recompute: webinars must move the rate.
ck('the fold actually changed the rate',pl.qualificationRate!==preFoldRate,true);
ck('and it is no longer the outcome-based rate',
   r4(pl.qualificationRate)===r4(pl.qualOutcomeQualified/(pl.qualOutcomeQualified+pl.disqualifiedRawCount)*100),false);
ck('webinars landed in the numerator',pl.qualifiedRawCount,7);
ck('the published denominator matches the one the rate used',
   pl.qualRateDenom,pl.qualifiedRawCount+pl.disqualifiedRawCount);
// Both rates come off the same denominator, so they must close. If they ever
// stop summing to 100 the two lines are describing different populations.
ck('qualification + disqualification rate close to 100',
   r4(pl.qualificationRate+pl.disqualificationRate),100);
// qualifiedRawScaleCount is read by True CPQD; it has to track the fold or the
// cost-per-qualified-demo divides by a different population than the rate.
ck('the scale count moved with the headline count',pl.qualifiedRawScaleCount,pl.qualifiedRawCount);

// ── B. Definitional edges of the numerator / denominator ───────────────────
{
  // A demo that HAPPENED and was then disqualified is in BOTH halves. Under the
  // old outcome rule it was numerator-excluded, denominator-included, so a single
  // such demo read as 0%.
  const p=run([mk(HELD,'Disqualified')]);
  ctx.applyWebinarHeld({pipeline:p},0);
  ck('a held-then-disqualified demo counts on both sides',
     [p.qualifiedRawCount,p.disqualifiedRawCount,r4(p.qualificationRate)],[1,1,50]);
}
{
  // A no-show that was disqualified is denominator-only — it never happened, so
  // it cannot be a Qualified Demo, but it is still a settled negative outcome.
  const p=run([mk('No Show','Disqualified')]);
  ctx.applyWebinarHeld({pipeline:p},0);
  ck('a disqualified no-show is denominator-only',
     [p.qualifiedRawCount,p.disqualifiedRawCount,r4(p.qualificationRate)],[0,1,0]);
}
{
  // "Not yet evaluated" must not enter the denominator — that was the explicit
  // revert noted in the source: an unevaluated demo would depress the rate.
  const p=run([mk(HELD,'Qualified'),mk(HELD,'Not yet evaluated'),mk(HELD,'')]);
  ctx.applyWebinarHeld({pipeline:p},0);
  ck('unevaluated held demos lift the numerator and not the denominator',
     [p.qualifiedRawCount,p.disqualifiedRawCount,r4(p.qualificationRate)],[3,0,100]);
}
{
  // Pre-launch is excluded from the rate on BOTH sides, not just the numerator —
  // otherwise excluding them from the count would push the rate down.
  const p=run([mk(HELD,'Qualified',PRE),mk(HELD,'Disqualified',PRE),mk('No Show','Disqualified',PRE)]);
  ctx.applyWebinarHeld({pipeline:p},0);
  ck('pre-launch deals reach neither side of the rate',
     [p.qualifiedRawCount,p.disqualifiedRawCount,p.qualRateDenom,r4(p.qualificationRate)],[0,0,0,0]);
}

// ── C. applyWebinarHeld's own contract ─────────────────────────────────────
// The failure this guards: the webinar fetch throws, n is null, and the rate is
// left on whatever processPipelineDeals computed — i.e. silently back on the old
// definition. applyWebinarHeld runs OUTSIDE the try precisely so this cannot happen.
{
  const p=run(deals);
  const stale=p.qualificationRate;
  ctx.applyWebinarHeld({pipeline:p},null);
  ck('a null webinar count leaves the counts alone',
     [p.qualifiedRawCount,p.webinarHeldCount],[3,undefined]);
  ck('but the rate is still recomputed onto the new rule',
     [r4(p.qualificationRate),p.qualificationRate===stale],
     [r4(p.qualifiedRawCount/(p.qualifiedRawCount+p.disqualifiedRawCount)*100),false]);
  ck('and the denominator is republished',p.qualRateDenom,5);
}
{
  // Empty window: 0/0 must read as 0, not NaN or Infinity — a NaN here renders
  // as a blank tile rather than a zero.
  const p=run([]);
  ctx.applyWebinarHeld({pipeline:p},null);
  ck('an empty denominator yields 0, not NaN',
     [p.qualRateDenom,p.qualificationRate,p.disqualificationRate],[0,0,0]);
}
// Called on every period including ones that may be absent (no prior month on a
// custom window), so a missing period must be a no-op rather than a throw.
ck('a missing period is a no-op',(()=>{
  try{ctx.applyWebinarHeld(null,3);ctx.applyWebinarHeld({},3);ctx.applyWebinarHeld(undefined,null);return true;}
  catch(e){return String(e&&e.message);}
})(),true);
{
  // The demos-only baseline is snapshotted BEFORE the fold — the Overview's
  // "demos only" quality rate rides it, and it must not drift if the function is
  // ever called twice (prior-period payloads get folded on a separate path).
  const p=run(deals);
  ctx.applyWebinarHeld({pipeline:p},4);
  const only=p.qualifiedDemoOnlyCount;
  ctx.applyWebinarHeld({pipeline:p},4);
  ck('the demos-only baseline is the pre-fold count',only,3);
  ck('and a second fold does not move it',p.qualifiedDemoOnlyCount,only);
}
{
  // Webinars enter numerator and denominator equally, so folding more of them can
  // only push the rate up toward 100 — never down. A regression that added them
  // to the denominator alone would invert this.
  const rates=[0,1,5,50,500].map(n=>{const p=run(deals);ctx.applyWebinarHeld({pipeline:p},n);return p.qualificationRate;});
  ck('more webinars never lower the rate',rates.every((v,i)=>i===0||v>=rates[i-1]),true);
  ck('and it stays inside 0..100',rates.every(v=>v>=0&&v<=100),true);
  // The demos-only rate is the floor the canonical rate rises from; the two are
  // meant to differ once webinars exist, which is why both are published.
  const p=run(deals);ctx.applyWebinarHeld({pipeline:p},4);
  const demoOnlyRate=p.qualifiedDemoOnlyCount/(p.qualifiedDemoOnlyCount+p.disqualifiedRawCount)*100;
  ck('the canonical rate sits at or above the demos-only rate',p.qualificationRate>=demoOnlyRate,true);
}

// ── D. Wiring: the fold must reach the payload, for every period ───────────
// The prose around these lines names applyWebinarHeld and qualOutcomeQualified,
// so every structural check below runs on comment-stripped source — otherwise a
// comment saying what the code must NOT do reads as the code doing it.
const nc=s=>s.replace(/\/\/[^\n]*/g,'');
// The try must not contain a fold: the folds sit after the catch, so a thrown
// webinar fetch still leaves the rate on the current definition.
const wbStart=src.indexOf('let _wbCur = null');
const catchM=/catch \(e\) \{[^\n]*\}/.exec(src.slice(Math.max(wbStart,0)));
// Both anchors are asserted rather than assumed. Keyed on the catch's SHAPE, not
// on its log text: pinning the message string meant that rewording the log (an
// unrelated change) made indexOf return -1, which turned the three structural
// checks below into vacuous passes or a failure against the whole file.
ck('the webinar fetch block is still findable',[wbStart>0,!!catchM],[true,true]);
const tryBlock=nc(src.slice(wbStart,wbStart+(catchM?catchM.index:0)));
ck('nothing folds inside the try',/applyWebinarHeld/.test(tryBlock),false);
ck('and the fetch really is wrapped in one',/try \{/.test(tryBlock),true);
ck('the catch has already closed before the first fold',
   wbStart+(catchM?catchM.index+catchM[0].length:0)<src.indexOf('applyWebinarHeld(currentData'),true);
// All three periods, or the deltas compare a new-rule current against an
// old-rule prior and every arrow points the wrong way.
for(const [label,v] of [['current','currentData'],['prior','priorData'],['prior month','priorMonthData']])
  ck(label+' period is folded',new RegExp('applyWebinarHeld\\('+v+',').test(src),true);
// Ordering is load-bearing: buildResponse snapshots pipeline.qualificationRate
// into the tile, so a fold after it would ship the pre-fold rate.
ck('the fold runs before the response is built',
   src.indexOf('applyWebinarHeld(currentData')<src.indexOf('buildResponse(currentData'),true);
// The recompute itself, in case the body is ever refactored back into the loop.
const fold=nc(fn(src,'applyWebinarHeld'));
ck('the rate is assigned inside applyWebinarHeld',/pl\.qualificationRate =/.test(fold),true);
ck('its numerator is the Qualified Demo count',/\(pl\.qualifiedRawCount \|\| 0\) \/ den/.test(fold),true);
ck('and never the outcome tally',/qualOutcomeQualified/.test(fold),false);
// The guard clause that makes the "runs even when n is null" behaviour above
// structural rather than accidental: the recompute sits outside the `n != null` block.
ck('the recompute is not inside the n-guard',
   fold.indexOf('const den =')>fold.indexOf('if (n != null)')
   &&fold.slice(fold.indexOf('if (n != null)'),fold.indexOf('const den =')).split('}').length>1,true);

// The client reproduces the fraction, so these three have to survive to it.
const meta=decl(src,'_qualMeta: {','webinarRegistrants:');
for(const k of ['qualifiedRawCount','disqualifiedRawCount','qualRateDenom','webinarHeldCount','qualDemoOnlyCount'])
  ck('_qualMeta carries '+k,new RegExp('\\b'+k+':').test(meta),true);
ck('the rate tile reads the pipeline field with both priors',
   /qualificationRate: buildTile\(c\.pipeline\.qualificationRate, p\.pipeline\?\.qualificationRate/.test(src),true);

// ── E. The client prints a fraction that produces the percentage above it ──
// Demo Quality page: numerator and denominator composed from the SAME counts the
// worker divided, or the printed "3 ÷ 5" disagrees with the "60%" beside it.
ck('Demo Quality composes the fraction from the canonical counts',
   /fN\(qmetaC\.qualifiedRawCount\|\|0\)\+' ÷ '\+fN\(\(qmetaC\.qualifiedRawCount\|\|0\)\+\(qmetaC\.disqualifiedRawCount\|\|0\)\)/.test(cli),true);
ck('and reads the worker-computed percentage, not a local recompute',
   /var qualRateC=d\.executiveSummary&&d\.executiveSummary\.qualificationRate&&d\.executiveSummary\.qualificationRate\.value/.test(cli),true);
ck('no client-side rate is composed from the outcome tally',
   /qualOutcomeQualified[^\n]*\/[^\n]*disqualifiedRawCount/.test(cli),false);
// The Overview's second rate is DELIBERATELY demos-only — it must ride the
// demos-only baseline, not the canonical count, or the two lines become the
// same number and the webinar exclusion is lost.
ck('the Overview demos-only rate rides qualDemoOnlyCount',
   /const _qdOnlyC=qmeta\.qualDemoOnlyCount!=null\?qmeta\.qualDemoOnlyCount:\(qmeta\.qualifiedRawCount\|\|0\)/.test(cli),true);
ck('its denominator adds the same disqualified count',
   /const _qdOnlyDen=_qdOnlyC\+\(qmeta\.disqualifiedRawCount\|\|0\)/.test(cli),true);
ck('and it is labelled demos only',/Qualified \\u00f7 \(Qualified \+ Disqualified\), demos only/.test(cli),true);
// Degrade rather than blank on a payload predating the field.
ck('an old payload falls back instead of printing 0',
   /_qdOnlyP=qmeta\.qualDemoOnlyCountPrior,_qdOnlyL=qmeta\.qualDemoOnlyCountLastMonth/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
