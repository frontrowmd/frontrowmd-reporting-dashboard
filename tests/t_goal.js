// Monthly goal tracker, above Key Metrics on the Brand and Clinician pages.
// Two things here are easy to get subtly wrong and invisible in a screenshot:
// the count must come from the trend series' MONTH grain (not the page date
// filter, which can be 7 days or All Time), and the gradient must span the
// whole TRACK so the bar starts red — a gradient sized to the FILL is red-to-
// green at every width, which looks fine and means nothing.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console,Date};vm.createContext(ctx);
vm.runInContext([
  "const fN=v=>v==null?'\\u2014':Math.round(v).toLocaleString();",
  decl(cli,'const GOAL_BRAND_REGULAR_DEMOS=',';'),
  decl(cli,'const GOAL_CLINICIAN_SIGNUPS=',';'),
  decl(cli,'const _GOAL_MONTHS=[','];'),
  fn(cli,'_goalMonthEntry'), fn(cli,'_goalMonthElapsed'),
  fn(cli,'_goalMonthName'), fn(cli,'_goalTrackerHTML'),
  'Object.assign(globalThis,{_goalTrackerHTML,_goalMonthEntry,_goalMonthElapsed,_goalMonthName,'
   +'GOAL_BRAND_REGULAR_DEMOS,GOAL_CLINICIAN_SIGNUPS});',
].join('\n'),ctx);
ck('the helpers loaded',typeof ctx._goalTrackerHTML,'function');

// ── 1. The goals ──────────────────────────────────────────────────────────
ck('Brand goal is 150 Regular Demos',ctx.GOAL_BRAND_REGULAR_DEMOS,150);
ck('Clinician goal is 150 Sign Ups',ctx.GOAL_CLINICIAN_SIGNUPS,150);

// ── 2. The month comes from the series, not the page window ───────────────
const MONTHS=[{wk:'2026-08-01',regular:140,fullApp:40},
              {wk:'2026-09-01',regular:131,fullApp:52},
              {wk:'2026-10-01',regular:62, fullApp:44}];
ck('the CURRENT month is the last entry',ctx._goalMonthEntry(MONTHS).wk,'2026-10-01');
ck('  read by position, not by a date key',ctx._goalMonthEntry(MONTHS).regular,62);
ck('an empty series yields null, not a zero goal',
   [ctx._goalMonthEntry([]),ctx._goalMonthEntry(null),ctx._goalMonthEntry(undefined)],[null,null,null]);
ck('the month name comes off the entry key',ctx._goalMonthName('2026-10-01'),'October');
ck('  for every month',[1,6,12].map(m=>ctx._goalMonthName('2026-'+String(m).padStart(2,'0')+'-01')),
   ['January','June','December']);
ck('  and a junk key yields no name rather than Invalid Date',
   [ctx._goalMonthName(''),ctx._goalMonthName(null),ctx._goalMonthName('nonsense')],['','','']);

// ── 3. The gradient: it must start RED ────────────────────────────────────
// background-size is the fill's width scaled back up to the track's, so the
// red end stays anchored at zero. Sized to the fill instead, every bar would
// run red->green regardless of progress.
const bgOf=(h)=>{const m=/background-size:([\d.]+)% 100%/.exec(h);return m?+m[1]:null;};
const wOf=(h)=>{const m=/width:([\d.]+)%;border-radius:50px/.exec(h);return m?+m[1]:null;};
const T=(cur,goal)=>ctx._goalTrackerHTML({cur,goal:goal||150,noun:'Regular Demos',month:'October'});
ck('a full bar spans the gradient exactly once',[wOf(T(150)),bgOf(T(150))],[100,100]);
ck('a half bar stretches it to twice the fill',[wOf(T(75)),bgOf(T(75))],[50,200]);
ck('a quarter bar, four times',[wOf(T(37.5)),bgOf(T(37.5))],[25,400]);
// The product is the invariant: fill% x size% === 10000, i.e. the gradient is
// always exactly one track wide.
// Both numbers are emitted at one decimal, so the product cannot be exactly
// 10000; it must be within a rounding step of it at every width.
ck('the gradient is always one track wide, within rounding',
   [15,40,62,99,150].map(c=>Math.abs(wOf(T(c))*bgOf(T(c))-10000)<30),
   [true,true,true,true,true]);
// Sized to the FILL instead of the track, this product would be 10000 only at
// full width — which is exactly the bug that makes every bar look red-to-green.
ck('  and is NOT merely the fill width',
   [15,40,62].map(c=>bgOf(T(c))===100),[false,false,false]);
ck('the gradient runs red to green',
   /linear-gradient\(90deg,var\(--red\) 0%,var\(--yellow\) 55%,var\(--green\) 100%\)/.test(T(62)),true);

// ── 4. Over-delivery fills the bar but is not hidden ──────────────────────
ck('past the goal the bar stops at full',wOf(T(200)),100);
ck('  while the figure reads past 100%',/>133%</.test(T(200)),true);
ck('  and says the goal is met rather than a negative to-go',
   /goal met/.test(T(200))&&!/-\d+ to go/.test(T(200)),true);
ck('short of the goal it says how many are left',/88 to go/.test(T(62)),true);
ck('a zero count is a real zero, not a blank',[wOf(T(0)),/>0%</.test(T(0))],[0,true]);
// A bar of width 0 must not ask the browser for an infinite background.
ck('  and does not divide by zero',bgOf(T(0)),100);

// ── 5. Pacing ─────────────────────────────────────────────────────────────
ck('elapsed is a share of the current month',
   (()=>{const e=ctx._goalMonthElapsed();return e>0&&e<=100;})(),true);
const el=ctx._goalMonthElapsed();
// Behind/ahead is measured against the SAME elapsed share the Ad Spend pacing
// bar uses, so one word means one thing on the page.
const behind=T(Math.round(150*(el-30)/100)), ahead=T(Math.round(150*(el+30)/100));
ck('well short of pace reads behind',/behind pace/.test(behind),true);
ck('  in the light-on-dark red',/color:#FF9B8F/.test(behind),true);
ck('well ahead reads ahead',/ahead of pace/.test(ahead),true);
ck('  in the light-on-dark green',/color:#7EE0B8/.test(ahead),true);
// Within 5pp is on track; colouring it would cry wolf every day of the month.
const onPace=T(Math.round(150*el/100));
ck('on pace is not coloured as a miss',/color:rgba\(255,255,255,\.55\)">\u2191|color:rgba\(255,255,255,\.55\)">\u2193/.test(onPace),true);
ck('the on-pace marker is drawn',/On pace for this point in the month/.test(T(62)),true);

// ── 6. No data is stated, not faked ───────────────────────────────────────
const none=ctx._goalTrackerHTML({cur:null,goal:150,noun:'Regular Demos'});
ck('a missing count shows an em-dash',/—/.test(none),true);
ck('  and says why',/no trend data for this window/.test(none),true);
ck('  with no percentage invented',/of goal/.test(none),false);
ck('  and no pacing claim',/pace/.test(none)&&!/On pace for this point/.test(none),false);

// ── 7. Wired above Key Metrics on BOTH pages, with the right field ────────
ck('Brand reads the brand trend’s month grain',
   /_goalMonthEntry\(d\.trend&&d\.trend\.month\)/.test(cli),true);
ck('  counting Regular Demos against the brand goal',
   /cur:_gm\?_gm\.regular:null,goal:GOAL_BRAND_REGULAR_DEMOS/.test(cli),true);
ck('  and renders ABOVE Key Metrics',/kmEl\.innerHTML=_goal\+kmh/.test(cli),true);
ck('Clinician reads the clinician trend’s month grain',
   /_goalMonthEntry\(d\.ads&&d\.ads\.trend&&d\.ads\.trend\.month\)/.test(cli),true);
ck('  counting Sign Ups against the clinician goal',
   /cur:_gm\?_gm\.fullApp:null,goal:GOAL_CLINICIAN_SIGNUPS/.test(cli),true);
ck('  and renders ABOVE Key Metrics',/\+_clnKeyMetricsHTML\(d,''\)\+h;/.test(cli),true);
// Neither may fall back to the page window, which is the whole point.
ck('neither call site reads a page window',
   /_goalTrackerHTML\(\{cur:[^}]*S\.(dashboard|clinician)\.win/.test(cli),false);

// ── 8. It has to STAND OUT ────────────────────────────────────────────────
// A dark band on a page of white cards. Reverting it to a white card is the
// regression that would pass every other assertion here.
const H=T(62);
ck('the band is a dark hero, not another white card',
   /background:linear-gradient\(135deg,var\(--navy\),var\(--mid-navy\)\)/.test(H),true);
ck('  with the teal rule the section headers use',/border-left:4px solid var\(--teal\)/.test(H),true);
ck('  and lifted off the page',/box-shadow:0 2px 14px/.test(H),true);
ck('the count is the headline',/font-size:40px;font-weight:700;color:#fff/.test(H),true);
ck('  with the goal beside it, not competing',/\/ 150/.test(H),true);
ck('the share of goal is its own figure',/font-size:30px;font-weight:700/.test(H),true);
ck('the pace reads as a pill',/padding:3px 10px;border-radius:50px/.test(H),true);
ck('the bar is the subject, not a footnote',/height:16px/.test(H),true);
// Meeting the goal is worth seeing at a glance.
const MET=T(160);
ck('a met goal turns the share green',/font-weight:700;line-height:1;font-variant-numeric:tabular-nums;color:#7EE0B8/.test(MET),true);
ck('  and says so in the footer',/color:#7EE0B8;font-weight:700">goal met/.test(MET),true);
ck('  while an unmet one does not',/goal met/.test(H),false);

// ── 9. All THREE hosts render it ──────────────────────────────────────────
// Brand page, Clinician page, and the Executive page in either mode. The two
// Executive modes read DIFFERENT payloads, so each must take its own month
// grain and its own goal — a shared one would count the other side's people.
ck('the Executive page renders it too',/_goalTrackerHTML\(\{[\s\S]{0,260}?sfx:'-exec'\}\)/.test(cli),true);
ck('  picking the population from the mode pill',
   /cur:_gm\?\(_cln\?_gm\.fullApp:_gm\.regular\):null/.test(cli),true);
ck('  and the goal with it',
   /goal:_cln\?GOAL_CLINICIAN_SIGNUPS:GOAL_BRAND_REGULAR_DEMOS/.test(cli),true);
ck('  reading the matching month grain per mode',
   /_goalMonthEntry\(_cln\?\(d\.ads&&d\.ads\.trend&&d\.ads\.trend\.month\):\(d\.trend&&d\.trend\.month\)\)/.test(cli),true);
ck('  above Key Metrics there as well',
   /km\.innerHTML=_goal\+\(_cln\?_clnKeyMetricsHTML/.test(cli),true);
// Suffixed ids, or the Executive copy collides with the owning page's.
ck('every host uses a distinct section id',
   [...cli.matchAll(/_goalTrackerHTML\(\{[\s\S]{0,300}?sfx:'([^']*)'\}\)/g)].map(m=>m[1]).sort(),
   ['','','-exec']);
ck('  and the id is built from it',/id="sec-goal'\+sfx\+'"/.test(cli),true);

// ── 10. Scope ─────────────────────────────────────────────────────────────
// dashboard.html is one inline script; a function-local called from module
// scope is a blank page, so every piece of this has to be reachable.
const MODULE=new Set([...cli.matchAll(/^(?:function\s+([A-Za-z_$][\w$]*)|(?:const|let|var)\s+([A-Za-z_$][\w$]*))/gm)]
  .map(m=>m[1]||m[2]));
ck('every goal helper is module scope',
   ['_goalTrackerHTML','_goalMonthEntry','_goalMonthElapsed','_goalMonthName',
    'GOAL_BRAND_REGULAR_DEMOS','GOAL_CLINICIAN_SIGNUPS','_GOAL_MONTHS']
     .filter(n=>!MODULE.has(n)),[]);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
