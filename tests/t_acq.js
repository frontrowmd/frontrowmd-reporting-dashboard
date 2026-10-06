// Demo Acquisition: the Regular / Pre-Launch / Regular + Pre Launch toggle.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// --- worker ---------------------------------------------------------------
const sched=fn(src,'processScheduledContacts');
ck('the tier split uses the SAME pre-launch test as byDayRegular/byDayPre',
   /\{ const _pre = wt\.includes\('pre-launch'\);/.test(sched)
   &&/if \(wt\.includes\('pre-launch'\)\) \{ byDayPre\[_ak\]/.test(sched),true);
ck('it runs before the webinar skip, so a registrant counts in its real tier',
   sched.indexOf("const _pre = wt.includes('pre-launch')")<sched.indexOf('if (isVerySmall) continue;'),true);
ck('each segment is capped on its OWN distribution',
   ['utmSourceRegular: cappedCounts(byUtmSReg, 8)','utmSourcePre: cappedCounts(byUtmSPre, 8)']
     .every(k=>src.includes(k)),true);
ck('all three dimensions are split, both ways',
   ['byUtmSReg','byUtmMReg','byUtmCReg','byUtmSPre','byUtmMPre','byUtmCPre'].every(k=>sched.includes(k)),true);
ck('the combined accumulator is untouched',/byUtmS\[_us\] = \(byUtmS\[_us\] \|\| 0\) \+ 1;/.test(sched),true);
ck('segment totals are kept too',/if \(_pre\) utmTotalPre\+\+; else utmTotalRegular\+\+;/.test(sched),true);
// The allow-list silently drops anything not named in it.
ck('all eight new keys survive the scheduledOut allow-list',
   ['utmSourceRegular: scheduled.utmSourceRegular','utmMediumRegular: scheduled.utmMediumRegular',
    'utmCampaignRegular: scheduled.utmCampaignRegular','utmSourcePre: scheduled.utmSourcePre',
    'utmMediumPre: scheduled.utmMediumPre','utmCampaignPre: scheduled.utmCampaignPre',
    'utmTotalRegular: scheduled.utmTotalRegular','utmTotalPre: scheduled.utmTotalPre']
     .every(k=>src.includes(k)),true);
ck('and reach the payload under scheduledUtm* names',
   ['scheduledUtmSourceRegular:','scheduledUtmMediumRegular:','scheduledUtmCampaignRegular:',
    'scheduledUtmTotalRegular:','scheduledUtmSourcePre:','scheduledUtmMediumPre:',
    'scheduledUtmCampaignPre:','scheduledUtmTotalPre:'].every(k=>src.includes(k)),true);
ck('payload shape change bumped the cache version past v30',
   (+((src.match(/API_CACHE_VER = 'v(\d+)'/)||[])[1]||0))>=31,true);

// --- client ---------------------------------------------------------------
const acq=fn(cli,'_renderDemoAcqCard');
ck('Regular is the default, matching the sibling cards',
   /S\.dashboard\.demoAcqMode\|\|'regular'/.test(acq),true);
ck('three modes, with the page-wide labels verbatim',
   /\['regular','Regular Demos','Regular'\],\s*\n\s*\['pre','Pre-Launch','Pre'\],\s*\n\s*\['all','Regular \+ Pre Launch',''\]/.test(cli),true);
// The labels must match the Demos Booked per Day / CPD per Week pills exactly,
// or "Regular Demos" means two different things on one page.
ck('the labels match the sibling toggle character for character',
   ["'regular','Regular Demos'","'pre','Pre-Launch'","'all','Regular + Pre Launch'"]
     .every(k=>cli.includes(k)),true);
ck('rows and total are scoped by the same suffix',
   /const rows=dt\[def\[2\]\+_sfx\]\|\|dt\[def\[2\]\]\|\|\[\];/.test(acq)
   &&/const total=dt\['scheduledUtmTotal'\+_sfx\]\?\?dt\.scheduledUtmTotal\?\?0;/.test(acq),true);
ck("the 'all' suffix is empty, so it reads the pre-split field",
   /\['all','Regular \+ Pre Launch',''\]/.test(cli),true);
ck('a pre-v31 payload degrades to the combined mix rather than blanking',
   /\|\|dt\[def\[2\]\]\|\|\[\]/.test(acq),true);
// A real zero must NOT fall back to the combined mix -- ?? keeps 0, || would not.
ck('a genuinely empty segment keeps its zero',/\?\?dt\.scheduledUtmTotal\?\?0/.test(acq),true);
// Text order alone passes even with an early `return` inserted, so assert the
// thing that actually matters: nothing bails out between knowing the tier is
// empty and emitting the pills. Without this the card dead-ends with no way back.
ck('the pills render before the body, so an empty tier is not a dead end',
   acq.indexOf('DEMO_ACQ_MODES.map')<acq.indexOf('h+=_empty'),true);
// A BARE `return;` only -- the pill/mpill arrow functions legitimately return values.
ck('and nothing returns early between the empty check and the pills',
   /return\s*;/.test(acq.slice(acq.indexOf('const _empty='),acq.indexOf('DEMO_ACQ_MODES.map'))),false);
ck('both pill rows are present',
   acq.includes('DEMO_ACQ_MODES.map(x=>mpill(x[0],x[1]))')
   &&acq.includes('DEMO_UTM_DIMS.map(x=>pill(x[0],x[3]))'),true);
ck('the two rows are separate controls, not six alternatives',
   /margin:0 0 8px[\s\S]{0,260}margin:0 0 14px/.test(acq),true);
ck('the empty message names the tier that is empty',
   /\(mode==='all'\?'acquisition':mdef\[1\]\.toLowerCase\(\)\)/.test(acq),true);
ck('clicking a mode re-renders only this card',
   /window\.dashSetDemoAcqMode=function\(mode\)\{[\s\S]{0,160}_renderDemoAcqCard\(S\.dashboard\.data\)/.test(cli),true);
ck('it does not call the heavyweight renderDash',
   /window\.dashSetDemoAcqMode=function[\s\S]{0,200}?\};/.exec(cli)[0].includes('renderDash('),false);
ck('build version bumped past s5t226',
   (+((cli.match(/_BUILD_VER='s5t(\d+)'/)||[])[1]||0))>=227,true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
