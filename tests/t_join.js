// PAYLOAD JOINS: every key the client reads off the API payload must be a key
// the worker actually puts there.
//
// This guards a bug class that shipped once. The worker emitted the Qualified
// Demo metadata at demoTracking._qualMeta while the client read it off
// executiveSummary._meta — and because the client had a plausible fallback, the
// card rendered a number that looked right and was wrong. Nothing failed, no
// console error, no blank. A join break is invisible by construction, so it
// needs a test that compares the two sides directly rather than eyeballing one.
//
// Two layers:
//   1. Whole-payload, for the Clinician page: EVERY top-level read is checked
//      against the endpoint's own return literal, so a new read cannot be added
//      without the worker emitting it.
//   2. A frozen table of nested joins across both dashboards, for the deep paths
//      where layer 1 cannot see (d.a.b).
const {load,fn}=require('./slice.js');
const src=load('worker.js'), cli=load('dashboard.html');

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// Brace-match forward from the '{' at or after `i`.
function blockAt(s,i){
  i=s.indexOf('{',i);
  let d=0;
  for(let j=i;j<s.length;j++){
    if(s[j]==='{')d++;
    else if(s[j]==='}'){d--;if(!d)return s.slice(i,j+1);}
  }
  return '';
}
// Top-level keys of an object literal: `k:`, shorthand `k,` and `...spread`.
// Depth-aware, so keys of nested literals are not swept in.
function topKeys(s){
  const out=new Set();
  let depth=0,j=1;
  while(j<s.length-1){
    const ch=s[j];
    if('{[('.includes(ch))depth++;
    else if('}])'.includes(ch))depth--;
    else if(depth===0){
      let m=/^\.\.\.([A-Za-z_$][\w$]*)/.exec(s.slice(j));
      if(m){out.add('...'+m[1]);j+=m[0].length;continue;}
      m=/^([A-Za-z_$][\w$]*)\s*:/.exec(s.slice(j));
      if(m){out.add(m[1]);j+=m[0].length;continue;}
      m=/^([A-Za-z_$][\w$]*)\s*,/.exec(s.slice(j));
      if(m){out.add(m[1]);j+=m[0].length;continue;}
    }
    j++;
  }
  return out;
}

// ── Layer 1: the whole Clinician payload ────────────────────────────────────
const clinEmit=topKeys(blockAt(src,src.indexOf('return jr({\n          period: cur,')));
// `...funnelPage` is buildClinicianFunnel's return, spread in. Resolve it rather
// than treating the spread as a wildcard that excuses everything.
const funnelEmit=topKeys(blockAt(src,src.indexOf('  return {\n    all: seg(() => true,')));
ck('clinician payload spreads the funnel', clinEmit.has('...funnelPage'), true);
ck('funnel contributes its segments', ['all','byFlow','rows','stageOrder','flowMeta'].every(k=>funnelEmit.has(k)), true);
const emitted=new Set([...clinEmit].filter(k=>!k.startsWith('...')).concat([...funnelEmit]));

// Client side: renderClinician binds the payload to `d`, and every _cln* helper
// takes it as its first parameter. Scope the scan to those bodies so a `d` that
// happens to be a Date somewhere else on the page cannot leak in as a false read.
let scope=blockAt(cli,cli.indexOf('function renderClinician(){'));
ck('renderClinician binds the payload to d', /const d=S\.clinician\.data;/.test(scope), true);
const helperRe=/function (_cln[A-Za-z0-9_]*|_renderCln[A-Za-z0-9_]*)\(d[,)]/g;
let hm,helpers=[];
while((hm=helperRe.exec(cli))){helpers.push(hm[1]);scope+='\n'+blockAt(cli,hm.index);}
ck('helper scan found the _cln* family', helpers.length>=4, true);
// Strip nested function bodies that rebind `d` as their own parameter — none
// today, but the scan must not silently start reading someone else's `d`.
const reads=new Set();
let rm,rre=/\bd\.([A-Za-z_$][\w$]*)/g;
while((rm=rre.exec(scope)))reads.add(rm[1]);
const DATE_METHODS=['getDate','getDay','getMonth','getUTCDate','getUTCDay','getUTCMonth','getUTCFullYear','getTime','setUTCDate','toLocaleDateString','toISOString'];
DATE_METHODS.forEach(m=>reads.delete(m));
const orphan=[...reads].filter(k=>!emitted.has(k)).sort();
// THE assertion. An orphan means the client reads a key off the clinician
// payload that /api/clinician does not return — the exact shape of the bug this
// suite exists for. Fix the join; do not add the key to an exemption list.
ck('no clinician payload read is unbacked', orphan, []);
ck('the scan actually sees the payload', reads.size>=10, true);
// Spot-check that real keys are in the read set, so a scope regression that
// silently matched nothing would not pass the orphan check vacuously.
ck('composition is read and emitted', reads.has('composition')&&emitted.has('composition'), true);
ck('ads is read and emitted', reads.has('ads')&&emitted.has('ads'), true);
ck('topFunnel is read and emitted', reads.has('topFunnel')&&emitted.has('topFunnel'), true);

// ── Layer 2: nested joins, both dashboards ──────────────────────────────────
// Each row: [label, client path as the client writes it, worker-side evidence].
// The worker evidence is the ASSIGNMENT that puts the leaf on that parent — not
// merely the leaf name appearing somewhere, which is what made the original bug
// survive review.
const JOINS=[
  // The bug itself. Canonical Qualified Demo metadata hangs off demoTracking.
  ['qualified-demo meta', /d\.demoTracking(?:&&d\.demoTracking)?\.?_qualMeta|dt\._qualMeta/, /_qualMeta:\s*\{/],
  ['exec summary meta',   /es\._meta|d\.executiveSummary\.[\w.]*_meta/,                      /_meta:\s*\{/],
  // Clinician Key Metrics read the Acquisition populations, not the funnel's.
  ['acq inquiries',       /_acq\.contacts/,                        /contacts,/],
  ['acq full submission', /_acqSub\.contacts/,                     /submission:\s*\{/],
  ['top-funnel clicks',   /_tf\.clicks/,                           /out\.impressions = im; out\.linkClicks = lc; out\.clicks = ck;/],
  ['trend series',        /d\.ads\.trend/,                         /ads\.trend = buildClinicianTrend\(/],
  ['virtual no-show',     /d\.virtualNoShow/,                      /virtualNoShow,/],
  ['kpi comparison',      /d\.kpiPrior\b/,                         /kpiPrior, kpiPriorMonth,/],
  ['funnel stage defs',   /d\.funnelStages/,                       /funnelStages: CLN_FUNNEL9,/],
  ['clinician spend',     /d\.clnSpend/,                           /clnSpend:/],
  ['daily series',        /d\.daily/,                              /daily,/],
  ['history range',       /d\.historyRange/,                       /historyRange: \{ from: CLINICIAN_HISTORY_FROM/],
];
JOINS.forEach(([lbl,c,w])=>{
  ck('join · '+lbl+' · client reads it', c.test(cli), true);
  ck('join · '+lbl+' · worker emits it', w.test(src), true);
});

// A fallback is what let the original break render a plausible number instead of
// blowing up. Where the client still falls back, the fallback must not be a
// SILENT substitute for a missing join: Qualified Demos reads the canonical
// figure off qmeta and the comment says so.
ck('qualified demos reads the canonical figure',
   /Canonical Qualified Demo figure\. Read off qmeta/.test(cli), true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
