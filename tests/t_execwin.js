// The Executive Dashboard's Custom range and quarter pills must refetch the
// payload the MODE PILL is showing. selW, selQ and goC each carried their own
// copy of the routing and they drifted: only selW had the AP==='exec' branch,
// so Custom and the quarters ran the BRAND fetcher. In Clinician mode that
// stores a brand payload in S.exec.data while the page reads S.clinician.data,
// so the figures never moved.
const {load,fn}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

function mk(){
  const calls=[];
  const ctx={console};vm.createContext(ctx);
  // Minimal DOM: the Go handler reads the date inputs and toggles pills.
  // Stable element objects per id — a fresh object per lookup would silently
  // drop anything the test writes back (that is how the compare-range cases
  // first "passed" against a stub that could not remember a checkbox).
  const inputs={cf:'2026-10-09',ct:'2026-10-12',cc:false,cvf:'',cvt:''};
  const els={};
  const elFor=(id)=>els[id]||(els[id]={
    get value(){return inputs[id];}, set value(v){inputs[id]=v;},
    get checked(){return !!inputs[id];}, set checked(v){inputs[id]=v;},
    classList:{add(){},remove(){},toggle(){}},
  });
  ctx.document={getElementById:elFor,querySelectorAll:()=>[]};
  ctx.__inputs=inputs;
  vm.runInContext([
    'var AP="exec";',
    'var S={exec:{mode:"brand",win:"mtd",data:{brand:1}},clinician:{win:"mtd",data:{cln:1}},dashboard:{},webinarPerf:{}};',
    'var calls=[];',
    'async function fd(w,f,t,vf,vt){calls.push(["fd",w,f||null,t||null,vf||null,vt||null]);}',
    'async function fdClinician(w){calls.push(["fdClinician",w,S.clinician.cFrom||null,S.clinician.cTo||null]);}',
    'async function fdWebinarPerf(w,f,t){calls.push(["fdWebinarPerf",w,f||null,t||null]);}',
    fn(cli,'_execMode'),
    fn(cli,'_mirrorCustomToClinician'), fn(cli,'_fetchForWindow'),
    fn(cli,'selW'), fn(cli,'selQ'), fn(cli,'goC'),
    'Object.assign(globalThis,{S,calls,selW,selQ,goC,_fetchForWindow,setAP(a){AP=a;},reset(){calls.length=0;}});',
  ].join('\n'),ctx);
  return ctx;
}
let ctx=mk();
ck('the handlers loaded',[typeof ctx.goC,typeof ctx.selQ,typeof ctx.selW],['function','function','function']);

// ── 1. Executive + Clinician mode ─────────────────────────────────────────
const run=async()=>{
  // Custom "Go"
  ctx=mk(); ctx.S.exec.mode='clinician'; ctx.reset();
  await ctx.goC();
  ck('exec/clinician: Go fetches the CLINICIAN payload',ctx.calls.map(c=>c[0]),['fdClinician']);
  ck('  for the custom window',ctx.calls[0][1],'custom');
  ck('  carrying the dates the picker shows',[ctx.calls[0][2],ctx.calls[0][3]],['2026-10-09','2026-10-12']);
  ck('  and the stale payload is cleared so the page cannot show it',ctx.S.clinician.data,null);
  ck('  with the clinician window moved too',ctx.S.clinician.win,'custom');
  ck('  and the exec window recorded',ctx.S.exec.win,'custom');

  // Quarter pill
  ctx=mk(); ctx.S.exec.mode='clinician'; ctx.reset();
  await ctx.selQ('2026-07-01','2026-09-30');
  ck('exec/clinician: a quarter pill fetches the clinician payload',ctx.calls.map(c=>c[0]),['fdClinician']);
  ck('  with the quarter dates mirrored across',
     [ctx.calls[0][2],ctx.calls[0][3]],['2026-07-01','2026-09-30']);

  // Preset window — the one path that already worked; must not regress.
  ctx=mk(); ctx.S.exec.mode='clinician'; ctx.reset();
  await ctx.selW('lastMonth');
  ck('exec/clinician: a preset still fetches the clinician payload',
     ctx.calls.map(c=>[c[0],c[1]]),[['fdClinician','lastMonth']]);
  // Custom only reveals the picker; nothing is fetched until Go.
  ctx.reset(); await ctx.selW('custom');
  ck('  and picking Custom fetches nothing until Go',ctx.calls.length,0);

  // ── 2. Executive + Brand mode ───────────────────────────────────────────
  ctx=mk(); ctx.S.exec.mode='brand'; ctx.reset();
  await ctx.goC();
  ck('exec/brand: Go fetches the BRAND payload',ctx.calls.map(c=>c[0]),['fd']);
  ck('  with the dates forwarded',[ctx.calls[0][2],ctx.calls[0][3]],['2026-10-09','2026-10-12']);
  ck('  and the stale brand payload cleared',ctx.S.exec.data,null);
  ck('  while the clinician payload is left alone',ctx.S.clinician.data,{cln:1});
  ctx=mk(); ctx.S.exec.mode='brand'; ctx.reset();
  await ctx.selQ('2026-04-01','2026-06-30');
  ck('exec/brand: a quarter pill fetches the brand payload',
     [ctx.calls[0][0],ctx.calls[0][2],ctx.calls[0][3]],['fd','2026-04-01','2026-06-30']);

  // ── 3. The comparison range survives the route ──────────────────────────
  ctx=mk(); ctx.S.exec.mode='brand';
  ctx.document.getElementById('cc').checked=true;
  ctx.document.getElementById('cvf').value='2026-09-01';
  ctx.document.getElementById('cvt').value='2026-09-04';
  ctx.reset(); await ctx.goC();
  ck('a compare range reaches the brand fetcher',
     [ctx.calls[0][4],ctx.calls[0][5]],['2026-09-01','2026-09-04']);
  // An incomplete compare range must abort rather than fetch half of one.
  ctx=mk(); ctx.S.exec.mode='brand';
  ctx.document.getElementById('cc').checked=true;
  ctx.reset(); await ctx.goC();
  ck('an incomplete compare range fetches nothing',ctx.calls.length,0);

  // Switching to a PRESET must not touch the stored custom range — a reader
  // who flips to MTD and back expects their dates (and compare range) intact.
  ctx=mk(); ctx.S.exec.mode='clinician';
  ctx.S.clinician.cFrom='2026-03-01'; ctx.S.clinician.cTo='2026-03-31';
  ctx.S.clinician.cVsFrom='2026-02-01'; ctx.S.clinician.cVsTo='2026-02-28';
  ctx.reset(); await ctx.selW('mtd');
  ck('a preset leaves the stored custom range alone',
     [ctx.S.clinician.cFrom,ctx.S.clinician.cTo],['2026-03-01','2026-03-31']);
  ck('  including its compare range',
     [ctx.S.clinician.cVsFrom,ctx.S.clinician.cVsTo],['2026-02-01','2026-02-28']);

  // ── 4. The other pages still route as before ────────────────────────────
  ctx=mk(); ctx.setAP('clinician'); ctx.reset();
  await ctx.goC();
  ck('the Clinician page still uses its own fetcher',ctx.calls.map(c=>c[0]),['fdClinician']);
  ck('  with its range set',[ctx.calls[0][2],ctx.calls[0][3]],['2026-10-09','2026-10-12']);
  ctx=mk(); ctx.setAP('webinarPerf'); ctx.reset();
  await ctx.goC();
  ck('Webinar Performance still uses its own fetcher',ctx.calls.map(c=>c[0]),['fdWebinarPerf']);
  ctx=mk(); ctx.setAP('dashboard'); ctx.reset();
  await ctx.goC();
  ck('the Brand page still uses fd',ctx.calls.map(c=>c[0]),['fd']);

  // ── 5. ONE router, not three copies ─────────────────────────────────────
  // The bug was three hand-maintained copies of this routing; two had drifted.
  ck('all three entry points go through the router',
     (cli.match(/await _fetchForWindow\(/g)||[]).length,3);
  ck('  and none of them dispatches on AP itself',
     ['selW','selQ','goC'].filter(n=>/AP==='(clinician|webinarPerf|exec)'/.test(fn(cli,n))),[]);
  ck('the router handles the Executive page',/if\(AP==='exec'\)\{/.test(fn(cli,'_fetchForWindow')),true);
  ck('  branching on the mode pill',/_execMode\(\)==='clinician'/.test(fn(cli,'_fetchForWindow')),true);

  console.log('\n'+pass+' passed, '+fail+' failed');
  process.exit(fail?1:0);
};
run();
