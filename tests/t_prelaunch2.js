// "Demos supposed to happen" counted every traffic tier, including pre-launch —
// brands that are excluded from the Qualified Demo definition those demos feed.
const fs=require('fs');
let pass=0,fail=0;
function ck(n,g,w){const ok=JSON.stringify(g)===JSON.stringify(w);ok?pass++:fail++;
  console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));}
const src=fs.readFileSync('worker.js','utf8');
const cli=fs.readFileSync('dashboard.html','utf8');

// ── Worker: three counters, keyed on the DEAL's own tier ──
ck('counters declared',/let heldExPre=0, noShowExPre=0, cancelledExPre=0;/.test(src),true);
ck('keyed on the deal tier, not contact matching',
   /const _isPreLaunchDeal = \(p\.average_monthly_web_traffic__cloned_ \|\| ''\) === PRE_LAUNCH_TIER;/.test(src),true);
ck('not keyed on isLow',/_isPreLaunchDeal = .*isLow/.test(src),false);
ck('held covers both Demo Given variants',
   /heldExPre\+\+/.test(src)&&/Demo Given \(originally scheduled\)' \|\| att === 'Demo Given \(rescheduled\)'\) heldExPre/.test(src),true);
ck('the three are mutually exclusive branches',
   /if \(!_isPreLaunchDeal\) \{[\s\S]{0,400}?else if \(att === 'No Show'\) noShowExPre\+\+;[\s\S]{0,200}?else if \(att === 'Cancelled before demo'\) cancelledExPre\+\+;/.test(src),true);
ck('returned from the builder',/heldExPre, noShowExPre, cancelledExPre,/.test(src),true);
ck('exposed with priors',
   ['heldExPrePrior','heldExPreLastMonth','noShowExPrePrior','noShowExPreLastMonth',
    'cancelledExPrePrior','cancelledExPreLastMonth'].every(f=>src.includes(f+':')),true);

// The webinar-date half must drop pre-launch too, or it puts them back.
ck('webinar-date deals fetch the tier',
   /properties: \['brand_status','average_monthly_web_traffic__cloned_'\]/.test(src),true);
ck('and skip pre-launch',
   /if \(\(dd\.properties\?\.average_monthly_web_traffic__cloned_ \|\| ''\) === PRE_LAUNCH_TIER\) continue;/.test(src),true);

// ── Client: the card and the rates share one population ──
ck('the card sums the pre-launch-excluded counts',
   /const _supAllC=_heldPreC\+_nsPreC\+_cxPreC\+_wdSupC;/.test(cli),true);
ck('its label says so',/Held \+ No shows \+ Pruned \+ Webinar Date deals, excl\. pre-launch/.test(cli),true);
ck('its calc chip prints the same components',
   /\$\{fN\(_heldPreC\)\} held \+ \$\{fN\(_nsPreC\)\} no-show \+ \$\{fN\(_cxPreC\)\} pruned/.test(cli),true);
// The Quality rates describe the same population, so they follow it.
ck('prune divides by the same base',/const _supOutcomeC=_heldPreC\+_nsPreC\+_cxPreC;/.test(cli),true);
ck('prune numerator matches',/_pruneAllC=_supOutcomeC>0\?\(_cxPreC\/_supOutcomeC\)/.test(cli),true);
ck('no-show denominator follows',/const _nsDenAllC=_heldPreC\+_nsPreC,/.test(cli),true);
ck('no-show numerator matches',/_noShowAllC=_nsDenAllC>0\?\(_nsPreC\/_nsDenAllC\)/.test(cli),true);
ck('both rate labels say excl. pre-launch',
   (cli.match(/, excl\. pre-launch'/g)||[]).length>=2,true);
// Prior and last-month must move with them or the deltas compare populations.
ck('prior prune uses the pre-launch base',/_pruneAllP=\(_supOutcomeP&&_supOutcomeP>0&&_cxPreP!=null\)\?\(_cxPreP\/_supOutcomeP\)/.test(cli),true);
ck('last-month no-show too',/_noShowAllL=\(_nsDenAllL&&_nsDenAllL>0&&_nsPreL!=null\)\?\(_nsPreL\/_nsDenAllL\)/.test(cli),true);

// Degrade, do not blank, on a payload predating the fields.
ck('falls back to the all-traffic counts',
   /qmeta\.heldExPre!=null\?qmeta\.heldExPre:_heldAllC/.test(cli),true);
// The all-traffic vars must survive only as declarations and fallbacks.
const q=cli.slice(cli.indexOf('const _heldAllC='),cli.indexOf("statRow('Qualification rate'"));
const strayAll=(q.match(/_cxAllC|_nsAllC/g)||[]).length;
ck('no rate still reads an all-traffic count',strayAll,4);   // 2 declarations + 2 fallbacks

// ── Every Volume and Quality figure drops pre-launch ───────────────
// The headline change: qualifiedRawCount was gated on `isLow`, which measured
// zero exclusions in Sept 2026. It now uses the deal's own tier.
ck('Qualified Demos excludes pre-launch on the deal tier',
   /if \(demoDidHappen\(att, rawSt\) && !_isPreLaunchDeal\) \{/.test(src),true);
ck('no Volume/Quality counter still gates on isLow',
   /demoDidHappen\(att, rawSt\) && !isLow/.test(src),false);
ck('per-channel qualified follows',
   /const isQualDemo = demoDidHappen\(att, rawSt\) && !_isPreLaunchDeal;/.test(src),true);
// Demos to occur.
ck('Demos to occur skips pre-launch',
   /if \(\(d\.properties\?\.average_monthly_web_traffic__cloned_ \|\| ''\) === PRE_LAUNCH_TIER\) continue;/.test(src),true);
// The outcome tallies must share the population, or the Qualification rate
// compares two different sets of demos.
ck('outcome tallies share the population',
   /if \(!_isPreLaunchDeal\) \{\n\s*if \(qo === 'Qualified'\) qualOutcomeQualified\+\+;/.test(src),true);
// Webinar no-show counts registrants, so it filters on the CONTACT's tier.
ck('webinar registrants fetch their tier',
   /'webinar_date','webinar_has_attended','average_monthly_web_traffic'/.test(src),true);
ck('and pre-launch registrants are dropped',
   /average_monthly_web_traffic \|\| ''\)\.toLowerCase\(\)\.includes\('pre-launch'\)\) continue;/.test(src),true);
// The isLow-based "scale" counters are deliberately untouched — other consumers
// read them, and they are a different cut.
ck('scale counters still exist for their own consumers',
   /if \(!isLow\) demoGivenScaleCount\+\+;/.test(src),true);

// ── Labels name the population ──
// 'Regular Demos Scheduled' left with the Conversion Funnel's page rows (2026-10-29).
for(const L of ['Regular Demos to Occur','Regular Demos Supposed to Happen',
                'Qualified Regular Demos']){
  ck('label: '+L,cli.includes("'"+L+"'"),true);
}
ck('no bare "Demos to occur" card remains',/statRow\('Demos to occur'/.test(cli),false);
ck('nor a bare Qualified Demos card',/statRow\('Qualified Demos'/.test(cli),false);
// Biggest drop is built from the funnel's own rows now, so it can't drift from them.
ck('funnel drop labels come from the rows themselves',/label:_ftRows\[i\]\.label\+' \\u2192 '\+st\.label/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
