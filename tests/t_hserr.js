// A HubSpot call refused at the HTTP level is NOT an empty window, but hsSearch
// returns its partial results on a non-OK response, so both reached the banner
// as a plain 0. On 2026-10-09 every live HubSpot call came back 403 with a
// Cloudflare body ("error code: 1106" — HubSpot's edge blocking the request,
// not HubSpot answering it) and the dashboard reported "scheduledContacts 0"
// with nothing to say an outage was in progress.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. The accumulator ────────────────────────────────────────────────────
const ctx={console};vm.createContext(ctx);
vm.runInContext([
  decl(src,'const HS_ERR = {','};'), fn(src,'noteHsError'), fn(src,'hsErrReset'),
  'Object.assign(globalThis,{HS_ERR,noteHsError,hsErrReset});',
].join('\n'),ctx);
ck('the accumulator loaded',typeof ctx.noteHsError,'function');
ctx.hsErrReset();
ck('it starts clean',[ctx.HS_ERR.n,ctx.HS_ERR.sample],[0,'']);
ctx.noteHsError(403,'error code: 1106\n');
ctx.noteHsError(403,'error code: 1106\n');
ctx.noteHsError(429,'rate limited');
ck('refusals are counted',ctx.HS_ERR.n,3);
ck('  and grouped by status',ctx.HS_ERR.statuses,{'403':2,'429':1});
// The FIRST body is the useful one — later ones repeat it, and the banner has
// room for one.
ck('  keeping the first body as a sample',ctx.HS_ERR.sample,'error code: 1106');
ck('  with whitespace collapsed so it fits one line',/\n/.test(ctx.HS_ERR.sample),false);
ctx.noteHsError('network','');
ck('a thrown fetch counts as a refusal too',ctx.HS_ERR.statuses.network,1);
ck('  without clobbering the sample',ctx.HS_ERR.sample,'error code: 1106');
ctx.hsErrReset();
ck('reset clears everything',[ctx.HS_ERR.n,Object.keys(ctx.HS_ERR.statuses).length,ctx.HS_ERR.sample],[0,0,'']);

// ── 2. Wired into the request lifecycle ───────────────────────────────────
// Per-invocation state. Left unreset, one bad request would mark every later
// one as degraded.
ck('it resets with the subrequest counters',/function subreqReset\(\)[\s\S]{0,320}?hsErrReset\(\);/.test(src),true);
ck('a non-OK response is recorded',/noteHsError\(resp\.status, _b\);/.test(src),true);
ck('  and so is a thrown or exhausted one',/noteHsError\(resp \? resp\.status : 'network'/.test(src),true);
// Only when something actually failed, so a healthy payload carries no key.
ck('the diagnostic is attached only when there were refusals',
   /if \(HS_ERR\.n\) fetchDiag\.hubspotErrors = \{ n: HS_ERR\.n/.test(src),true);
// The response headers are what identified Cloudflare as the blocker.
ck('the failure log names who answered',
   /server=\$\{resp\.headers\.get\('server'\)\}/.test(src)&&/cf-mitigated=/.test(src),true);
ck('  and no longer probes the token',/tokenLen=/.test(src),false);

// ── 3. The banner says WHICH it was ───────────────────────────────────────
const dctx={console};vm.createContext(dctx);
let html='';
dctx.document={getElementById:(id)=>id==='dqb-diag'
  ? {set innerHTML(v){html=v;},get innerHTML(){return html;}}
  : {classList:{add(){}}}};
vm.runInContext(fn(cli,'_renderFetchDiag')+'\nObject.assign(globalThis,{_renderFetchDiag});',dctx);
const BASE={windsorAds:{rows:141,spend:10439.33},window:{from:'2026-10-01',to:'2026-10-09'},
  hubspot:{scheduledContacts:0,pipelineDeals:0,closedWon:0,churnedByClose:0},
  subrequests:{total:42,windsor:16,hubspot:16},failed:['hubspot.scheduledContacts','hubspot.pipelineDeals']};
// The outage exactly as it was seen.
dctx._renderFetchDiag(Object.assign({},BASE,{hubspotErrors:{n:16,statuses:{'403':16},sample:'error code: 1106'}}));
ck('the banner names the refusal and its status',/HubSpot refused 16 requests:<\/b> HTTP 403 ×16/.test(html),true);
ck('  identifies the edge, not HubSpot, as the blocker',
   /blocked at HubSpot’s edge, not by HubSpot itself \(error code: 1106\)/.test(html),true);
ck('  and says outright that the zeros are not real',
   /The zeros below are not real counts/.test(html),true);
ck('  while still printing the counts',/scheduledContacts 0/.test(html),true);
// A genuinely quiet window must NOT get the outage language.
dctx._renderFetchDiag(BASE);
ck('a quiet window is not dressed up as an outage',/HubSpot refused/.test(html),false);
ck('  and still reports its empty fetches',/Empty fetches/.test(html),true);
// A non-Cloudflare refusal keeps its own body rather than the edge wording.
dctx._renderFetchDiag(Object.assign({},BASE,{hubspotErrors:{n:1,statuses:{'401':1},sample:'{"status":"error"}'}}));
ck('a real HubSpot error is not blamed on the edge',
   /blocked at HubSpot/.test(html),false);
ck('  but is still reported with its body',/HubSpot refused 1 request:<\/b> HTTP 401 ×1/.test(html)&&/\{"status":"error"\}/.test(html),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
