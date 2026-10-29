// The new Qualified Demo definition: a demo that was HELD, excluding pre-launch.
// The qualification OUTCOME no longer gates it; the qualification RATE still uses it.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const ctx={console,Object,String,Number,Array,Date,Set,Math,parseInt,parseFloat,isNaN,RegExp,JSON};
vm.createContext(ctx);
const names=['categorizeDemoStatus','demoDidHappen','processPipelineDeals','_usDst','isPDT','ptOff','etOff','fmt','toMsET','toMsUTC','isoMs','r2','detectFunnelStage','mapUtmToChannel'];
const code=[];
for(const n of names){try{code.push(fn(src,n));}catch(e){}}
try{code.push(src.slice(src.indexOf('const FUNNEL_ORDER'),src.indexOf(';',src.indexOf('const FUNNEL_ORDER'))+1));}catch(e){}
try{code.push(src.slice(src.indexOf('const STAGES ='),src.indexOf(';',src.indexOf('const STAGES ='))+1));}catch(e){}
vm.runInContext(code.join('\n'),ctx);

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const HELD='Demo Given (originally scheduled)', HELD2='Demo Given (rescheduled)';
// demoDidHappen is the gate the new definition leans on.
ck('a held demo counts as happened',ctx.demoDidHappen(HELD,''),true);
ck('a rescheduled-and-held demo counts',ctx.demoDidHappen(HELD2,''),true);
ck('a no-show did not happen',ctx.demoDidHappen('No Show',''),false);
ck('a cancelled-before demo did not happen',ctx.demoDidHappen('Cancelled before demo',''),false);
ck('still-scheduled did not happen',ctx.demoDidHappen('Scheduled — pending',''),false);

// The source now gates on held + not-pre-launch, and keeps the rate separate.
const code2=src.slice(src.indexOf('    // A QUALIFIED DEMO is a demo that was HELD'),
                      src.indexOf('    // Per-rep'));
ck('qualified counts held demos',/demoDidHappen\(att, rawSt\)/.test(code2),true);
ck('qualified excludes pre-launch',/&& !_isPreLaunchDeal/.test(code2),true);
// Guard the regression: isLow matched contact company names and let every
// pre-launch deal through, so the gate must not drift back onto it.
ck('and not via the unreliable isLow test',/demoDidHappen\(att, rawSt\) && !isLow/.test(code2),false);
ck('qualified no longer gates on the Qualified outcome',!/if \(qo === 'Qualified'\) \{\s*\n\s*qualifiedRawCount/.test(code2),true);
ck('a held-then-disqualified demo still increments disqualified',/qo === 'Disqualified'\) disqualifiedRawCount/.test(code2),true);

// The rate must use the OUTCOME counts, not the held count.
const rateLine=src.slice(src.indexOf('const qualRateDenom'),src.indexOf('const qualificationRate')+160);
ck('rate denominator uses outcome counts',/qualOutcomeQualified \+ disqualifiedRawCount/.test(rateLine),true);
ck('rate numerator uses outcome counts',/qualificationRate = qualRateDenom > 0 \? \(qualOutcomeQualified/.test(rateLine),true);
// Word boundaries matter here: "disqualificationRate" contains "qualificationRate"
// and "disqualifiedRawCount" contains "qualifiedRawCount", so a loose regex
// matches the disqualification line and reports a false positive.
ck('rate no longer reads the held-based count',
   !/\bqualificationRate\b[^\n]*\bqualifiedRawCount\b/.test(rateLine),true);

// Headline and scale counts are now the same number by construction.
ck('both counts increment together',/qualifiedRawCount\+\+;\s*\n\s*qualifiedRawScaleCount\+\+;/.test(code2),true);


// ── The per-channel / per-rep / per-stage counts must use the NEW rule too ──
// This is what "everywhere it's used" means: only the headline changed last time.
const loop=src.slice(src.indexOf('function processPipelineDeals'),src.indexOf('function buildAzAttribution'));
ck('a single held-and-not-pre-launch flag drives the tallies',
   /const isQualDemo = demoDidHappen\(att, rawSt\) && !_isPreLaunchDeal;/.test(loop),true);
for(const [name,re_] of [['per rep',/byRep\[ownerId\]\.qualDemo/],['per channel',/byChannel\[ch\]\.qualDemo/],
                         ['per stage',/byStage\[stage\]\.qualDemo/]]){
  ck(name+' tallies the new rule',re_.test(loop),true);
}
ck('per day no longer keys off the categoriser bucket',
   /byDay\[ds\]\.deals\+\+; if \(isQualDemo\) byDay\[ds\]\.qualified\+\+/.test(loop),true);
ck('the categoriser buckets are left intact (they must still partition)',
   /byChannel\[ch\]\[cat\]\+\+/.test(loop)&&/byRep\[ownerId\]\[cat\]\+\+/.test(loop),true);

// The Channel Performance Qual column must read the new counter.
ck('channel payload exposes the new count',
   /qualified: c\.pipeline\.byChannel\[ch\]\?\.qualDemo/.test(src),true);
ck('and no longer the categoriser bucket',
   !/qualified: c\.pipeline\.byChannel\[ch\]\?\.qualified/.test(src),true);

// Analyzer attribution (ad tables) likewise.
const az=src.slice(src.indexOf('function buildAzAttribution'),src.indexOf('function buildAzAttribution')+3000);
ck('analyzer tallies held demos',/const _qd = demoDidHappen\(/.test(az),true);
for(const k of ['byChannel\\[ch\\]','byCampaign\\[key\\]','byCreative\\[key\\]'])
  ck('analyzer '+k.replace(/\\\\/g,'')+' carries qualDemo',new RegExp(k+'\\.qualDemo').test(az),true);
ck('analyzer qualified reads qualDemo first',/a\.qualDemo!=null\?a\.qualDemo/.test(src),true);

// A cached response must not be able to mask the change.
// Pinning an exact version made this fail on every unrelated bump. What matters
// is that it never goes BACK below v21, the version where the Qualified Demo
// redefinition landed -- below that, a cached response serves the old counts.
ck('response cache version is at or past the redefinition',
   (()=>{const m=src.match(/API_CACHE_VER = 'v(\d+)'/);return m?Number(m[1])>=21:false;})(),true);


// ── One canonical figure, read by every section that shows Qualified Demos ──
const cli=require('fs').readFileSync('dashboard.html','utf8');
ck('worker exposes the canonical count + its priors',
   /qualDemoCount: c\.pipeline\.qualifiedRawCount/.test(src)
   &&/qualDemoCountPrior:/.test(src)&&/qualDemoCountLastMonth:/.test(src),true);
// Read off qmeta — the object the worker actually emits qualDemoCount into.
// See t_join.js, which proves every meta read resolves to a field that exists.
ck('client derives one figure from it',/const qd=\(qmeta\.qualDemoCount!=null/.test(cli),true);
// The Conversion Funnel no longer shows it (2026-10-29): it counts demos by demo
// date, which read as a contradiction beside the funnel's created-in-window cohort.
ck('Conversion Funnel no longer reads it',/const held=qd,heldPV=qdPV,heldLMV=qdLMV;/.test(cli),false);
ck('Efficiency CPQD divides by it',/\$\{fN\(qd\)\} qualified demos/.test(cli),true);
ck('the trend item reads it',/trendItem\('Qualified Demos','Demos held excl\. pre-launch \+ webinars held',fN\(qd\)\)/.test(cli),true);
ck('no Qualified display still reads demosHeldFull',
   !/funnelStage\('Qualified Demos[^']*',fN\(demosHeldFull\)/.test(cli),true);
// The rate now runs on the Qualified Demo count (held excl. pre-launch +
// Webinars Happened), so a webinar lifts it. Its calc text must be composed from
// the SAME counts, or the percentage and the fraction beside it disagree.
ck('rate calc text uses the canonical count',
   /const qualPlusDisqualCur = \(qmeta\.qualifiedRawCount\|\|0\) \+ \(qmeta\.disqualifiedRawCount\|\|0\)/.test(cli),true);
ck('rate numerator is the canonical count, not the outcome tally',
   !/qualPlusDisqualCur = \(qmeta\.qualOutcomeQualified/.test(cli),true);
// Recomputed after the fold, since the webinar total is unknown before it.
ck('worker recomputes the rate inside applyWebinarHeld',
   /pl\.qualificationRate = den > 0 \? \(\(pl\.qualifiedRawCount \|\| 0\) \/ den\) \* 100/.test(src),true);
ck('rate denominator is canonical \u002b disqualified',
   /const den = \(pl\.qualifiedRawCount \|\| 0\) \+ \(pl\.disqualifiedRawCount \|\| 0\)/.test(src),true);
// A failed webinar fetch must not revert the rate to the old definition.
ck('fold runs even when the webinar fetch throws',
   /\} catch \(e\) \{ console\.error\('webinar held count failed:'[^\n]*\n(?:\s*\/\/[^\n]*\n)*\s*applyWebinarHeld\(currentData, _wbCur\);/.test(src),true);
ck('rate still recomputes when n is null',
   /if \(!periodData \|\| !periodData\.pipeline\) return;/.test(src),true);
// The two Demo Insights cards the user reported as stuck on the old counts.
ck('Qualified Demos card reads the canonical figure',
   /statRow\('Qualified Regular Demos','Demos held excl\. pre-launch \+ Webinars Happened',fN\(qd\)/.test(cli),true);
ck('its calc text splits demos from webinars',
   /\$\{fN\(_qdDemo\)\} demos held \+ \$\{fN\(_qdWeb\)\} webinars/.test(cli),true);
ck('webinar component comes from the worker field',
   /const _qdWeb=qmeta\.webinarHeldCount\|\|0,_qdDemo=Math\.max\(0,qd-_qdWeb\)/.test(cli),true);
ck('no card still labels a held-only count as Qualified Demos',
   !/statRow\('Qualified Demos[^']*',[^;]*fN\(_held/.test(cli),true);
ck('stale "(Demos Held)" labels are gone',
   !/Qualified Demos \(Demos Held\)/.test(cli),true);
ck('worker exposes that tally',/qualOutcomeQualified: c\.pipeline\.qualOutcomeQualified/.test(src),true);
// Degrade, do not blank, on a payload predating the field.
ck('falls back to demosHeldCount when absent',/qmeta\.qualDemoCount!=null\?qmeta\.qualDemoCount:\(esM\.demosHeldCount\|\|0\)/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
