// The Executive page's Clinician toggle must switch the Ad Tables with it.
// _curAdCh resolves the channel for every other call site and HAS an exec
// branch; the ad-tables loader carried its OWN copy that did not, so on the
// Executive page in Clinician mode the tables rendered the Brand channel.
const {load,fn}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console};vm.createContext(ctx);
vm.runInContext([
  'var AP="dashboard";',
  'var S={dashboard:{activeAdChannel:"meta"},exec:{mode:"brand"}};',
  fn(cli,'_execMode'), fn(cli,'_curAdCh'),
  'Object.assign(globalThis,{_curAdCh,S,setAP(a){AP=a;}});',
].join('\n'),ctx);
ck('the resolver loaded',typeof ctx._curAdCh,'function');

// ── 1. The Executive page follows its own mode pill ───────────────────────
ctx.setAP('exec');
ctx.S.exec.mode='clinician';
ck('exec + Clinician mode resolves the clinician channel',ctx._curAdCh(),'clinician');
ctx.S.exec.mode='brand';
ctx.S.dashboard.activeAdChannel='meta';
ck('exec + Brand mode follows the Brand channel tab',ctx._curAdCh(),'meta');
ctx.S.dashboard.activeAdChannel='tiktok';
ck('  including a non-default channel',ctx._curAdCh(),'tiktok');
// Switching the pill must change the answer with no other state touched —
// that IS the reported bug.
ctx.S.exec.mode='clinician';
ck('flipping the mode pill flips the channel',ctx._curAdCh(),'clinician');
ctx.S.exec.mode='brand';
ck('  and back again',ctx._curAdCh(),'tiktok');

// ── 2. The other pages are unaffected ─────────────────────────────────────
ctx.setAP('clinician');
ck('the Clinician page is always the clinician channel',ctx._curAdCh(),'clinician');
ctx.setAP('dashboard');
ctx.S.dashboard.activeAdChannel='linkedin';
ck('the Brand page follows its tab',ctx._curAdCh(),'linkedin');
// Clinician was removed from the Detailed/Irfan switchers, so a session left on
// it must be corrected rather than stranded on a channel with no tab.
ctx.S.dashboard.activeAdChannel='clinician';
ck('a stranded clinician selection falls back to meta',ctx._curAdCh(),'meta');
ck('  and is corrected in state, not just in the return',ctx.S.dashboard.activeAdChannel,'meta');

// ── 3. ONE resolver — the loader must not carry its own ───────────────────
// The local copy read `AP==='clinician'?'clinician':(activeAdChannel||'meta')`,
// which silently resolves AP==='exec' to the Brand channel.
ck('the loader no longer defines a private resolver',/_getCh/.test(cli),false);
ck('  and every render call site goes through _curAdCh',
   /_renderIrfanMetaTables\([^)]*,\s*_curAdCh\(\)\)/.test(cli),true);
// Any resolver that tests AP for 'clinician' without also handling 'exec' is
// the same bug again. _curAdCh itself is the only place allowed to do so.
const _bodies=['_renderIrfanMetaTablesLoader'].map(n=>fn(cli,n)).join('\n');
ck('the loader does not test AP for the channel at all',
   /AP\s*===\s*'clinician'/.test(_bodies),false);
// Matched on the CHANNEL resolution specifically (the activeAdChannel
// fallback); a bare _execMode()==='clinician' also appears in a loading
// message, which is not a second resolver.
ck('_curAdCh is the one place that resolves a channel from the exec mode',
   (cli.match(/_execMode\(\)==='clinician'\?'clinician':\(S\.dashboard\.activeAdChannel/g)||[]).length,1);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
