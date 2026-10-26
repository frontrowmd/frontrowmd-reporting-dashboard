// Slack changelog intake — request verification and event recording.
// The signature IS the authentication for /api/slack/events (Slack cannot send
// a password), so the rejection paths here are the security boundary, not a
// formality. Expected signatures are generated with node's own createHmac, a
// different implementation from the worker's Web Crypto, so a wrong basestring
// or encoding in the worker cannot agree with the test by construction.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const nodeCrypto=require('crypto');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console,crypto,TextEncoder,Date,JSON,Array,String,Math,parseInt,isFinite,Uint8Array};
vm.createContext(ctx);
vm.runInContext([
  decl(src,"const SLACK_LOG_REACTION = '","';"),
  decl(src,"const SLACK_EVT_KEY = '","';"),
  decl(src,'const SLACK_SIG_WINDOW_SEC = ',';'),
  fn(src,'_slackEq'), fn(src,'slackVerify'), fn(src,'slackRecordEvent'),
  'Object.assign(globalThis,{_slackEq,slackVerify,slackRecordEvent,SLACK_LOG_REACTION,SLACK_EVT_KEY,SLACK_SIG_WINDOW_SEC});',
].join('\n'),ctx);

const SECRET='8f742231b10e8888abcd99yyyzzz85a5';
const now=()=>Math.floor(Date.now()/1000);
// Independent of the worker's implementation on purpose.
const sign=(raw,ts,secret=SECRET)=>
  'v0='+nodeCrypto.createHmac('sha256',secret).update('v0:'+ts+':'+raw).digest('hex');

// A KV stub that records what was written, so dedup and trimming are observable.
const kv=()=>{const m={};return {store:m,
  async get(k,t){const v=m[k];return v===undefined?null:(t==='json'?JSON.parse(v):v);},
  async put(k,v){m[k]=v;}};};

(async()=>{
ck('the helpers loaded',[typeof ctx.slackVerify,typeof ctx._slackEq],['function','function']);
ck('the trigger reaction is the wood log',ctx.SLACK_LOG_REACTION,'wood');

// ── 1. Constant-time compare ──────────────────────────────────────────────
ck('equal strings compare equal',ctx._slackEq('v0=abc','v0=abc'),true);
ck('a single differing byte is caught',ctx._slackEq('v0=abc','v0=abd'),false);
ck('  including the first byte',ctx._slackEq('a0=abc','v0=abc'),false);
ck('different lengths never match',ctx._slackEq('v0=abc','v0=abcd'),false);
ck('a non-string never matches',
   [ctx._slackEq('v0=abc',null),ctx._slackEq(undefined,'x'),ctx._slackEq('x',123)],[false,false,false]);
ck('the empty pair is equal, not an error',ctx._slackEq('',''),true);
// _slackEq's whole purpose is its TIMING, which no unit test can observe:
// `return a === b` passes every behavioural assertion above and leaks, through
// its own early exit, how much of a forged signature was correct. So the shape
// is pinned structurally — this is the one place in the suite where asserting
// on the implementation rather than the behaviour is the point.
ck('the compare stays constant-time, which no behavioural test can prove',
   (()=>{const f=fn(src,'_slackEq');
     return /for \(let i = 0; i < a\.length; i\+\+\) d \|= a\.charCodeAt\(i\) \^ b\.charCodeAt\(i\);/.test(f)
         && /return d === 0;/.test(f)
         && !/return a === b/.test(f);})(),true);
ck('  and the comment says why, so it is not simplified away',
   /Constant-time string compare[\s\S]{0,400}?leaks/.test(src),true);

// ── 2. Signature verification ─────────────────────────────────────────────
const RAW='{"type":"event_callback","event":{"type":"reaction_added","reaction":"wood"}}';
const t=String(now());
ck('a genuine Slack signature verifies',
   (await ctx.slackVerify(RAW,t,sign(RAW,t),SECRET)).ok,true);
// The body is what is signed — this is the whole point of reading text()
// before parsing, and of never re-serialising before verifying.
ck('a tampered body is rejected',
   (await ctx.slackVerify(RAW+' ',t,sign(RAW,t),SECRET)).ok,false);
ck('  even a one-character change',
   (await ctx.slackVerify(RAW.replace('wood','w00d'),t,sign(RAW,t),SECRET)).ok,false);
ck('a signature from a different secret is rejected',
   (await ctx.slackVerify(RAW,t,sign(RAW,t,'not-the-secret'),SECRET)).ok,false);
ck('a signature bound to another timestamp is rejected',
   (await ctx.slackVerify(RAW,t,sign(RAW,String(now()-60)),SECRET)).ok,false);
// Replay protection: Slack signs the timestamp, so it cannot be forged, but a
// captured request would otherwise stay replayable for ever. Each of these
// signs with the SAME timestamp it sends, so only staleness can reject them —
// a mismatched pair would fail on the signature and prove nothing about age.
const stale=String(now()-(ctx.SLACK_SIG_WINDOW_SEC+100));
ck('a stale request is rejected despite a valid signature',
   (await ctx.slackVerify(RAW,stale,sign(RAW,stale),SECRET)).ok,false);
ck('  for staleness, not for the signature',
   /outside the/.test((await ctx.slackVerify(RAW,stale,sign(RAW,stale),SECRET)).why),true);
const future=String(now()+(ctx.SLACK_SIG_WINDOW_SEC+100));
ck('a future-dated request is rejected too \u2014 the window is absolute',
   (await ctx.slackVerify(RAW,future,sign(RAW,future),SECRET)).ok,false);
const fresh=String(now()-(ctx.SLACK_SIG_WINDOW_SEC-30));
ck('but one just inside the window still verifies',
   (await ctx.slackVerify(RAW,fresh,sign(RAW,fresh),SECRET)).ok,true);
ck('no configured secret fails closed',
   (await ctx.slackVerify(RAW,t,sign(RAW,t),'')).ok,false);
ck('  and says why, so the cause is not guessed at',
   /not configured/.test((await ctx.slackVerify(RAW,t,sign(RAW,t),'')).why),true);
ck('missing headers fail closed',
   [(await ctx.slackVerify(RAW,null,sign(RAW,t),SECRET)).ok,
    (await ctx.slackVerify(RAW,t,null,SECRET)).ok],[false,false]);
ck('an unparseable timestamp fails closed',
   (await ctx.slackVerify(RAW,'not-a-number',sign(RAW,t),SECRET)).ok,false);
ck('a garbage signature fails closed rather than throwing',
   (await ctx.slackVerify(RAW,t,'garbage',SECRET)).ok,false);

// ── 3. Event recording ────────────────────────────────────────────────────
const evt=(over={})=>({event_id:'Ev01',type:'event_callback',
  event:{type:'reaction_added',reaction:'wood',user:'U1',
         item:{type:'message',channel:'C0A5366LG2D',ts:'1792600000.001'},...over}});
let k=kv();
await ctx.slackRecordEvent(evt(),{CONTENT_STORE:k});
let got=JSON.parse(k.store[ctx.SLACK_EVT_KEY]);
ck('an event is recorded',got.length,1);
ck('  with the fields the capture step will need',
   [got[0].type,got[0].reaction,got[0].channel,got[0].ts,got[0].user],
   ['reaction_added','wood','C0A5366LG2D','1792600000.001','U1']);
ck('  and flagged as a match',got[0].matches,true);

// Slack re-delivers anything it did not see a fast 200 for, reusing event_id.
await ctx.slackRecordEvent(evt(),{CONTENT_STORE:k});
ck('a redelivered event is not recorded twice',
   JSON.parse(k.store[ctx.SLACK_EVT_KEY]).length,1);

k=kv();
await ctx.slackRecordEvent({...evt(),event_id:'Ev02',
  event:{...evt().event,reaction:'wood::skin-tone-3'}},{CONTENT_STORE:k});
ck('a skin-tone suffix is stripped before matching',
   (g=>[g[0].reaction,g[0].matches])(JSON.parse(k.store[ctx.SLACK_EVT_KEY])),['wood',true]);

for(const [name,over,want] of [
  ['another reaction is not a match',{reaction:'eyes'},false],
  ['a reaction on a file is not a match',{item:{type:'file',channel:'C1',ts:'1'}},false],
]){
  const kk=kv();
  await ctx.slackRecordEvent({...evt(),event_id:'E_'+name,event:{...evt().event,...over}},{CONTENT_STORE:kk});
  ck(name,JSON.parse(kk.store[ctx.SLACK_EVT_KEY])[0].matches,want);
}
const kr=kv();
await ctx.slackRecordEvent({event_id:'E_rm',event:{type:'reaction_removed',reaction:'wood',user:'U1',
  item:{type:'message',channel:'C1',ts:'1'}}},{CONTENT_STORE:kr});
ck('a removal is a match too — it is what un-logs an entry',
   JSON.parse(kr.store[ctx.SLACK_EVT_KEY])[0].matches,true);

// The recent list is a debugging aid, not storage — it must stay bounded.
const kb=kv();
for(let i=0;i<50;i++) await ctx.slackRecordEvent({...evt(),event_id:'E'+i},{CONTENT_STORE:kb});
const bounded=JSON.parse(kb.store[ctx.SLACK_EVT_KEY]);
ck('the recent list is capped',bounded.length,40);
ck('  keeping the newest',bounded[0].eventId,'E49');
ck('no KV binding is survived, not thrown on',
   await ctx.slackRecordEvent(evt(),{}).then(()=>'ok').catch(e=>'threw: '+e.message),'ok');

// ── 3b. Capture: markup, keys, ordering ───────────────────────
const CAP=(()=>{const c={console,JSON,Date,Math,Array,String,parseFloat,parseInt,Object};
  vm.createContext(c);
  vm.runInContext([
    decl(src,"const SLACK_LOG_IDX = '","';"),
    fn(src,'slackPlain'), fn(src,'slackLogKey'), fn(src,'slackLogWrite'), fn(src,'slackLogDelete'),
    'Object.assign(globalThis,{slackPlain,slackLogKey,slackLogWrite,slackLogDelete,SLACK_LOG_IDX});',
  ].join('\n'),c); return c;})();

// Slack stores mentions and links as markup; left raw the model quotes it back.
ck('a mention resolves to the name',
   CAP.slackPlain('<@U03DCGDJN8G|Irfan Alam> good to reshare'),'Irfan Alam good to reshare');
ck('a labelled link becomes its label',
   CAP.slackPlain('see <https://docs.google.com/d/1|scripts> please'),'see scripts please');
ck('a bare link is kept whole',
   CAP.slackPlain('<https://lnkd.in/p/eHrtW8_y>'),'https://lnkd.in/p/eHrtW8_y');
ck('a channel ref keeps its hash',CAP.slackPlain('<#C04DCM8Q4GP|product>'),'#product');
ck('here/channel pings are dropped',CAP.slackPlain('<!here> New Inquiry Form is live'),'New Inquiry Form is live');
ck('entities are decoded',CAP.slackPlain('Nate &amp; Andrei &lt;test&gt;'),'Nate & Andrei <test>');
ck('a real message cleans up end to end',
   CAP.slackPlain('<@U08KK0C5DFW|Talar Malakian> can we increase the clinician budget to 9K? <https://x.co|here>'),
   'Talar Malakian can we increase the clinician budget to 9K? here');
ck('empty and null are safe',[CAP.slackPlain(''),CAP.slackPlain(null),CAP.slackPlain(undefined)],['','','']);

// Entries are chunked by month so no single KV value grows without bound.
ck('the month key comes off the message time',
   [CAP.slackLogKey(Date.UTC(2026,9,20)),CAP.slackLogKey(Date.UTC(2026,0,3))],
   ['log_2026-10_v1','log_2026-01_v1']);

const kvs=()=>{const m={};return {store:m,
  async get(k,t){const v=m[k];return v===undefined?null:(t==='json'?JSON.parse(v):v);},
  async put(k,v){m[k]=v;}};};
const ent=(o={})=>({id:'C1:100.5',channel:'C1',ts:'100.5',at:Date.UTC(2026,9,20),
  summary:'first',evTs:'1000',...o});

let K=kvs();
await CAP.slackLogWrite(ent(),{CONTENT_STORE:K});
ck('an entry is written into its month',JSON.parse(K.store['log_2026-10_v1']).length,1);
ck('  and the index counts it',
   JSON.parse(K.store[CAP.SLACK_LOG_IDX]).months['log_2026-10_v1'],1);
await CAP.slackLogWrite(ent({summary:'second',evTs:'2000'}),{CONTENT_STORE:K});
ck('the same id upserts rather than duplicating',
   (l=>[l.length,l[0].summary])(JSON.parse(K.store['log_2026-10_v1'])),[1,'second']);

// Slack does not guarantee delivery order. A remove/re-add pair a second apart
// can arrive inverted, which would delete an entry that should exist — so the
// decision is made on the EVENT clock, not on arrival.
await CAP.slackLogWrite(ent({summary:'stale',evTs:'500'}),{CONTENT_STORE:K});
ck('an out-of-order event does not overwrite a newer one',
   JSON.parse(K.store['log_2026-10_v1'])[0].summary,'second');
await CAP.slackLogDelete('C1:100.5',Date.UTC(2026,9,20),'500',{CONTENT_STORE:K});
ck('  nor delete it',JSON.parse(K.store['log_2026-10_v1']).length,1);
await CAP.slackLogDelete('C1:100.5',Date.UTC(2026,9,20),'3000',{CONTENT_STORE:K});
ck('a newer removal does delete it',JSON.parse(K.store['log_2026-10_v1']).length,0);
ck('  and the index follows',
   JSON.parse(K.store[CAP.SLACK_LOG_IDX]).months['log_2026-10_v1'],0);
ck('deleting what is not there is a no-op, not a throw',
   await CAP.slackLogDelete('C1:nope',Date.UTC(2026,9,20),'9',{CONTENT_STORE:K})
     .then(()=>'ok').catch(()=>'threw'),'ok');

K=kvs();
for(const t of ['100.5','300.5','200.5'])
  await CAP.slackLogWrite(ent({id:'C1:'+t,ts:t,summary:t}),{CONTENT_STORE:K});
ck('entries are stored newest-first',
   JSON.parse(K.store['log_2026-10_v1']).map(e=>e.ts),['300.5','200.5','100.5']);

// ── 4. Route wiring ───────────────────────────────────────────────────────
// The signature covers the exact bytes Slack sent. Calling json() first and
// re-serialising changes key order and whitespace, and nothing would ever verify.
ck('the route reads the RAW body before parsing',
   /const raw = await request\.text\(\);[\s\S]{0,200}?await slackVerify\(raw,/.test(src),true);
ck('  and only parses after verifying',
   src.indexOf('await slackVerify(raw,')<src.indexOf('body = JSON.parse(raw)'),true);
ck('a failed verification is a 401 and goes no further',
   /if \(!v\.ok\) \{[\s\S]{0,160}?return jr\(\{ error: 'Unauthorized' \}, 401\);/.test(src),true);
ck('the url_verification handshake echoes the challenge',
   /body\.type === 'url_verification'[\s\S]{0,160}?return jr\(\{ challenge: body\.challenge \}\)/.test(src),true);
// Slack allows 3 seconds and retries anything slower.
ck('event work is deferred so the 200 is immediate',
   /ctx\.waitUntil\(slackRecordEvent\(body, env\)\);[\s\S]{0,120}?return jr\(\{ ok: true \}\);/.test(src),true);
ck('the inspection route is password-guarded',
   /url\.pathname === '\/api\/slack\/recent'[\s\S]{0,260}?body\.password !== env\.TEAM_PASSWORD/.test(src),true);
ck('  and reports whether the secrets are configured',
   /signingSecretConfigured: !!env\.SLACK_SIGNING_SECRET/.test(src)
   &&/botTokenConfigured: !!env\.SLACK_BOT_TOKEN/.test(src),true);
// A reply comes back empty from conversations.history; conversations.replies
// accepts the REPLY's ts and returns the whole thread, which is where the
// parent comes from. Without it, "great, im updating now" summarises to nothing.
ck('a thread reply falls back to conversations.replies',
   /conversations\.history[\s\S]{0,420}?conversations\.replies/.test(src),true);
ck('  and the parent is carried into the summary',
   /parent = all\[0\] && all\[0\]\.ts !== ts \? all\[0\] : null;/.test(src)
   &&/slackSummarize\(raw, parentRaw, env\)/.test(src),true);
ck('  with the prompt told the change sits in the parent',
   /the decision usually sits in the parent/.test(src),true);
ck('a removal deletes rather than rewrites',
   /if \(e\.type === 'reaction_removed'\) \{ await slackLogDelete\(/.test(src),true);
ck('capture is gated on the trigger reaction',
   /if \(reaction !== SLACK_LOG_REACTION\) return;/.test(src),true);
ck('the summary is written once at capture, never on read',
   (()=>{const i=src.indexOf("url.pathname === '/api/changelog'");
     const j=src.indexOf("url.pathname === '/api/data'",i);
     return /slackSummarize/.test(src.slice(i,j));})(),false);
ck('the read endpoint does not ship raw Slack text',
   (()=>{const i=src.indexOf("url.pathname === '/api/changelog'");
     const j=src.indexOf("url.pathname === '/api/data'",i);
     const b=src.slice(i,j);
     return /summary:e\.summary/.test(b)&&!/raw:\s*e\.raw/.test(b);})(),true);
ck('  and is password-guarded',
   /url\.pathname === '\/api\/changelog'[\s\S]{0,300}?body\.password !== env\.TEAM_PASSWORD/.test(src),true);
ck('capture runs alongside the diagnostic record',
   /ctx\.waitUntil\(slackRecordEvent\(body, env\)\);\s*\n\s*ctx\.waitUntil\(slackHandleReaction\(body, env\)\);/.test(src),true);

ck('the events route is NOT password-guarded — the signature is the auth',
   (()=>{const i=src.indexOf("url.pathname === '/api/slack/events'");
     const j=src.indexOf("url.pathname === '/api/slack/recent'");
     return /TEAM_PASSWORD/.test(src.slice(i,j));})(),false);

// ── 4b. The sweep ─────────────────────────────────────────
// A capture that dies inside waitUntil leaves the reaction in place with nothing
// to retry it. These pin the worklist rules, because a wrong one either loses an
// entry for good or resurrects one the user deliberately un-reacted.
const SW=(()=>{const c={console,JSON,Date,Math,Array,String,Object,Map,parseFloat};
  c.calls=[];
  vm.createContext(c);
  vm.runInContext([
    decl(src,"const SLACK_LOG_REACTION = '","';"),
    decl(src,"const SLACK_EVT_KEY = '","';"),
    decl(src,'const SLACK_SWEEP_MAX = ',';'),
    fn(src,'slackLogKey'), fn(src,'slackSweep'),
    'async function slackHandleReaction(b,env){calls.push(b.event);}',
    'Object.assign(globalThis,{slackSweep,SLACK_SWEEP_MAX,SLACK_EVT_KEY});',
  ].join('\n'),c); return c;})();

// Oct 2026 message times, so every id lands in the same month key.
const mts=(n)=>String(1792101697+n)+'.000100';
const ev=(o)=>Object.assign({at:Date.now(),type:'reaction_added',reaction:'wood',
  user:'U09QTQ3GHFZ',channel:'C0A5366LG2D',itemType:'message',matches:true},o);
const envOf=(evts,logs)=>({SLACK_BOT_TOKEN:'xoxb-test',CONTENT_STORE:{
  async get(k,t){const v=k===SW.SLACK_EVT_KEY?evts:(logs||{})[k];
    return v===undefined?null:v;},
  async put(){},}});

const sweep=async(evts,logs)=>{SW.calls.length=0;await SW.slackSweep(envOf(evts,logs));return SW.calls;};

ck('a matched reaction with no entry is retried',
   (await sweep([ev({ts:mts(0)})])).map(e=>e.item.ts),[mts(0)]);
ck('  and is replayed as an add of the log reaction',
   (()=>{const e=SW.calls[0];return [e.type,e.reaction,e.item.type,e.item.channel];})(),
   ['reaction_added','wood','message','C0A5366LG2D']);
ck('an entry that already exists is left alone',
   (await sweep([ev({ts:mts(0)})],{'log_2026-10_v1':[{id:'C0A5366LG2D:'+mts(0)}]})).length,0);
ck('a different message in the same month is still retried',
   (await sweep([ev({ts:mts(5)})],{'log_2026-10_v1':[{id:'C0A5366LG2D:'+mts(0)}]})).map(e=>e.item.ts),
   [mts(5)]);

// The buffer is newest-first. An add that was later removed must stay removed:
// replaying the older add would undo the user's own un-react.
ck('an add that was later removed is NOT resurrected',
   (await sweep([ev({ts:mts(1),type:'reaction_removed',at:2000}),
                 ev({ts:mts(1),type:'reaction_added',at:1000})])).length,0);
ck('  but a re-add after a removal is retried',
   (await sweep([ev({ts:mts(1),type:'reaction_added',at:3000}),
                 ev({ts:mts(1),type:'reaction_removed',at:2000})])).map(e=>e.item.ts),[mts(1)]);

ck('an unmatched event is not swept',
   (await sweep([ev({ts:mts(2),reaction:'eyes',matches:false})])).length,0);
ck('a non-message item is not swept',
   (await sweep([ev({ts:mts(2),itemType:'file'})])).length,0);
ck('an event with no channel or ts is skipped, not replayed as a blank',
   (await sweep([ev({ts:''}),ev({channel:''})])).length,0);

ck('the real event clock is carried through when it was recorded',
   (await sweep([ev({ts:mts(3),evTs:'1793046047.004100'})]))[0].event_ts,'1793046047.004100');
ck('  and arrival time stands in when it was not',
   (await sweep([ev({ts:mts(3),at:1793046047786})]))[0].event_ts,'1793046047.786');

ck('the sweep is capped so one cron cannot run away',
   (await sweep(Array.from({length:SW.SLACK_SWEEP_MAX+6},(_,i)=>ev({ts:mts(100+i)})))).length,
   SW.SLACK_SWEEP_MAX);

ck('no bot token means no Slack calls at all',
   await (async()=>{SW.calls.length=0;
     await SW.slackSweep({CONTENT_STORE:envOf([ev({ts:mts(0)})]).CONTENT_STORE});
     return SW.calls.length;})(),0);
ck('an empty buffer is survived, not thrown on',
   await SW.slackSweep(envOf([])).then(()=>'ok').catch(e=>'threw: '+e.message),'ok');
ck('a missing buffer is survived too',
   await SW.slackSweep(envOf(undefined)).then(()=>'ok').catch(e=>'threw: '+e.message),'ok');

// Wiring: the sweep has to actually run, and must not share a waitUntil with the
// cache warm — a throw in either should not take the other down.
ck('the sweep runs on cron',/ctx\.waitUntil\(\(async \(\) => \{\s*\n\s*try \{ await slackSweep\(env\); \}/.test(src),true);
ck('  in its own waitUntil, not the warm loop\'s',
   (()=>{const i=src.indexOf('async scheduled(');const j=src.indexOf('async fetch(',i);
     return (src.slice(i,j).match(/ctx\.waitUntil\(/g)||[]).length;})(),2);

// Failures are invisible otherwise — waitUntil has no response to carry them.
ck('a failed capture is recorded, not only logged',
   /console\.error\('slack capture failed for'[\s\S]{0,120}?await slackRecordErr\(id, err && err\.message, env\)/.test(src),true);
ck('a message with no text is recorded as a reason, not a silent return',
   /nothing to summarise[\s\S]{0,120}?await slackRecordErr\(/.test(src),true);
ck('the error buffer is trimmed',/list\.slice\(0, 25\)/.test(src),true);
ck('the diagnostic route exposes the failures',
   (()=>{const i=src.indexOf("url.pathname === '/api/slack/recent'");
     const j=src.indexOf("url.pathname === '/api/changelog'",i);
     const b=src.slice(i,j);return /SLACK_ERR_KEY/.test(b)&&/errors:/.test(b);})(),true);
ck('  and stays password-guarded',
   /url\.pathname === '\/api\/slack\/recent'[\s\S]{0,300}?body\.password !== env\.TEAM_PASSWORD/.test(src),true);

// ── 4c. Request encoding and the summariser ───────────────
// Slack documents form encoding for these read methods. A JSON body is accepted
// by some and rejected by others with a bare invalid_arguments that names neither
// the method's problem nor the field — which is how the thread-reply capture died
// while top-level captures went through. The encoding is pinned for that reason.
const API=(()=>{const c={console,JSON,Object,String,Array,URLSearchParams,Error,Promise};
  c.sent=[];
  c.fetch=async(url,init)=>{c.sent.push({url,init});return {json:async()=>c.reply};};
  vm.createContext(c);
  vm.runInContext([fn(src,'slackApi'),'Object.assign(globalThis,{slackApi});'].join('\n'),c);
  return c;})();
const call=async(method,params,reply)=>{API.sent.length=0;API.reply=reply;
  try{return {ok:true,val:await API.slackApi(method,params,{SLACK_BOT_TOKEN:'xoxb-t'})};}
  catch(e){return {ok:false,msg:e.message};}};

await call('conversations.replies',{channel:'C1',ts:'1792692581.751559',limit:100},{ok:true,messages:[]});
ck('a Slack call is form-encoded, not JSON',
   API.sent[0].init.headers['Content-Type'],'application/x-www-form-urlencoded; charset=utf-8');
ck('  and the body is a query string',API.sent[0].init.body,
   'channel=C1&ts=1792692581.751559&limit=100');
ck('  with the ts intact — it is a decimal id, not a number',
   /ts=1792692581\.751559(&|$)/.test(API.sent[0].init.body),true);
ck('  posted to the named method',API.sent[0].url,'https://slack.com/api/conversations.replies');
ck('  carrying the bot token',API.sent[0].init.headers.Authorization,'Bearer xoxb-t');
ck('  by POST',API.sent[0].init.method,'POST');

await call('conversations.history',{channel:'C1',latest:'1.1',oldest:'1.1',inclusive:true,limit:1},{ok:true,messages:[]});
ck('a boolean is sent the way Slack reads it',/(^|&)inclusive=true(&|$)/.test(API.sent[0].init.body),true);
await call('users.info',{user:'U1',bogus:undefined,nil:null},{ok:true,user:{}});
ck('absent params are omitted, not sent as the string "undefined"',API.sent[0].init.body,'user=U1');

// A bare error code is not actionable. These are the two places Slack hides the
// part that tells you what to change.
ck('a Slack error names the method and the code',
   (await call('conversations.replies',{channel:'C1',ts:'1'},{ok:false,error:'invalid_arguments'})).msg,
   'conversations.replies: invalid_arguments');
ck('a scope failure says what was needed and what was granted',
   (await call('conversations.history',{channel:'C1'},
     {ok:false,error:'missing_scope',needed:'groups:history',provided:'channels:history'})).msg,
   'conversations.history: missing_scope (needed=groups:history provided=channels:history)');
ck('Slack\'s own detail is carried through',
   (await call('conversations.replies',{channel:'C1'},
     {ok:false,error:'invalid_arguments',response_metadata:{messages:['[ERROR] missing required field: ts']}})).msg,
   'conversations.replies: invalid_arguments ([ERROR] missing required field: ts)');
ck('a successful call returns the payload',
   (await call('users.info',{user:'U1'},{ok:true,user:{real_name:'Ada'}})).val.user.real_name,'Ada');

// An empty completion arrives looking like success — no error field, no text.
// It is rare and usually transient, so it is retried rather than lost.
const SUM=(()=>{const c={console,JSON,String,Array,Error,Promise,Object};
  c.calls=0;
  c.proxyAnthropicAI=async()=>{c.calls++;return c.replies[Math.min(c.calls-1,c.replies.length-1)];};
  vm.createContext(c);
  vm.runInContext([fn(src,'_slackDeriveHeadline'),fn(src,'_slackParseSummary'),fn(src,'slackSummarize'),
    'Object.assign(globalThis,{slackSummarize});'].join('\n'),c);
  return c;})();
const sum=async(replies)=>{SUM.calls=0;SUM.replies=replies;
  try{return {ok:true,val:await SUM.slackSummarize('increase the clinician budget to 9K','',{})};}
  catch(e){return {ok:false,msg:e.message};}};

const LABELLED='HEADLINE: Clinician budget raised\nSUMMARY: The clinician budget was raised to pace toward 9K for the month.';
const sv=await sum([{content:[{type:'text',text:'  '+LABELLED+'  '}]}]);
ck('a labelled reply comes back as both parts',[sv.val.headline,sv.val.summary],
   ['Clinician budget raised','The clinician budget was raised to pace toward 9K for the month.']);
ck('  in a single call',SUM.calls,1);
ck('an empty completion is retried rather than surfaced',
   (await sum([{content:[],stop_reason:'end_turn'},
               {content:[{type:'text',text:LABELLED}]}])).val.headline,'Clinician budget raised');
ck('  taking two attempts',SUM.calls,2);
const e2=await sum([{content:[],stop_reason:'end_turn'}]);
ck('two empties in a row do fail',e2.ok,false);
ck('  and the error says what the model actually returned',
   /stop_reason=end_turn/.test(e2.msg)&&/blocks=\[\]/.test(e2.msg),true);
ck('  after exactly two attempts, not more',SUM.calls,2);
const er=await sum([{error:true,message:'ANTHROPIC_API_KEY not configured'}]);
ck('a real API error is not retried',[er.ok,SUM.calls],[false,1]);
ck('  and is reported verbatim',er.msg,'summarise: ANTHROPIC_API_KEY not configured');
ck('a non-text block alone still counts as empty',
   (await sum([{content:[{type:'thinking'}],stop_reason:'end_turn'}])).ok,false);

// The two-part format is the model's to honour, and it will sometimes not. An
// entry with a derived headline beats an entry dropped over a missing label.
const PARSE=(()=>{const c={console,String,JSON,Array,RegExp};
  vm.createContext(c);
  vm.runInContext([fn(src,'_slackDeriveHeadline'),fn(src,'_slackParseSummary'),
    'Object.assign(globalThis,{_slackDeriveHeadline,_slackParseSummary});'].join('\n'),c);
  return c;})();
const par=(t)=>PARSE._slackParseSummary(t);

ck('both labels parse',
   (()=>{const r=par('HEADLINE: Inquiry form live\nSUMMARY: The form went live today.');
     return [r.headline,r.summary];})(),['Inquiry form live','The form went live today.']);
ck('a headline keeps its trailing period off',
   par('HEADLINE: Inquiry form live.\nSUMMARY: x.').headline,'Inquiry form live');
ck('  and loses wrapping quotes',
   par('HEADLINE: "Inquiry form live"\nSUMMARY: x.').headline,'Inquiry form live');
ck('a multi-line summary is kept whole',
   par('HEADLINE: A\nSUMMARY: One thing.\nAnd the rest.').summary,'One thing.\nAnd the rest.');
ck('an unlabelled reply becomes the summary',
   par('The clinician budget was raised to 9K.').summary,'The clinician budget was raised to 9K.');
ck('  with a headline derived from it',
   par('The clinician budget was raised to 9K.').headline,'The clinician budget was raised to 9K');
ck('a headline with no summary still yields a summary',
   par('HEADLINE: Budget raised\nThe budget went to 9K.').summary,'The budget went to 9K.');
ck('a derived headline stops at the first sentence',
   PARSE._slackDeriveHeadline('Form went live. HubSpot sync works. Stages created.'),
   'Form went live');
ck('a long first sentence is cut at a word boundary with an ellipsis',
   (()=>{const h=PARSE._slackDeriveHeadline(
     'The new inquiry form went live on the website with HubSpot contact creation syncing');
     return [h.length<=53,/\u2026$/.test(h),/ $/.test(h.slice(0,-1)),h];})().slice(0,3),
   [true,true,false]);
ck('an empty summary derives an empty headline, not a crash',
   [PARSE._slackDeriveHeadline(''),PARSE._slackDeriveHeadline(null)],['','']);

// ── 4d. Thinking, and the thread parent ───────────────────
// Sonnet 5.5 runs adaptive thinking when `thinking` is omitted, and it ate the
// whole token budget: a lone thinking block, stop_reason max_tokens, no text.
ck('thinking is turned off the way this model allows',
   (()=>{const i=src.indexOf('async function slackSummarize');
     const j=src.indexOf('function slackLogKey',i);
     const b=src.slice(i,j);
     return /thinking: \{ type: 'between_tools' \}/.test(b)
         && !/type: 'disabled'/.test(b);})(),true);
ck('  at an effort between_tools accepts',
   (()=>{const i=src.indexOf('async function slackSummarize');
     const j=src.indexOf('function slackLogKey',i);
     const m=/effort: '(\w+)'/.exec(src.slice(i,j));
     return !!m&&['low','medium','high'].includes(m[1]);})(),true);
ck('  with headroom above a one-line answer',
   (()=>{const i=src.indexOf('async function slackSummarize');
     const j=src.indexOf('function slackLogKey',i);
     const m=/max_tokens: (\d+)/.exec(src.slice(i,j));
     return !!m&&parseInt(m[1],10)>=300;})(),true);

// A reply like "great, im updating now" says nothing on its own; the decision
// sits in the parent, so the parent has to be fetched rather than hoped for.
const FETCH=(()=>{const c={console,JSON,String,Array,Object,Error,Promise};
  c.calls=[];
  c.slackApi=async(method,params)=>{const key=params.ts||params.latest;
    c.calls.push([method,key]);
    const r=c.replies[method+':'+key];
    if(r===undefined) throw new Error(method+': invalid_arguments');
    return r;};
  vm.createContext(c);
  vm.runInContext([fn(src,'slackThreadParent'),fn(src,'slackFetchForLog'),
    'Object.assign(globalThis,{slackFetchForLog});'].join('\n'),c);
  return c;})();
const fetchFor=async(replies,ts)=>{FETCH.calls=[];FETCH.replies=replies;
  try{return {ok:true,val:await FETCH.slackFetchForLog('C1',ts,{})};}
  catch(e){return {ok:false,msg:e.message};}};

const TOP={ts:'100.1',text:'top level'};
const REPLY={ts:'200.2',text:'great, im updating now',thread_ts:'100.1'};
const PARENT={ts:'100.1',text:'Should we remove the schedule call section?'};

let r=await fetchFor({'conversations.history:100.1':{messages:[TOP]}},'100.1');
ck('a top-level message comes straight from history',[r.val.msg.ts,r.val.isReply],['100.1',false]);
ck('  with no extra calls',FETCH.calls.length,1);

r=await fetchFor({'conversations.replies:200.2':{messages:[REPLY]},
                  'conversations.replies:100.1':{messages:[PARENT]}},'200.2');
ck('a reply history cannot see is fetched from replies',r.val.msg.ts,'200.2');
ck('  and its parent is fetched by thread_ts when replies omits it',
   r.val.parent&&r.val.parent.text,'Should we remove the schedule call section?');
ck('  so it is marked as a reply',r.val.isReply,true);

r=await fetchFor({'conversations.replies:200.2':{messages:[PARENT,REPLY]}},'200.2');
ck('a parent already in the thread is not re-fetched',
   [r.val.parent.ts,FETCH.calls.length],['100.1',2]);

// A thread PARENT carries thread_ts equal to its own ts. Chasing that would be
// a wasted call that can only ever return the message itself.
r=await fetchFor({'conversations.history:100.1':{messages:[{ts:'100.1',text:'x',thread_ts:'100.1'}]},
                  'conversations.replies:100.1':{messages:[{ts:'100.1',text:'x',thread_ts:'100.1'}]}},'100.1');
ck('a thread parent is not treated as its own parent',r.val.parent,null);
ck('  and is not looked up at all',FETCH.calls.length,1);

// If the lookup does run and hands back the same message, it is still not a parent.
r=await fetchFor({'conversations.replies:200.2':{messages:[REPLY]},
                  'conversations.replies:100.1':{messages:[REPLY]}},'200.2');
ck('the message is never adopted as its own parent',[r.val.parent,r.val.isReply],[null,false]);

r=await fetchFor({'conversations.replies:200.2':{messages:[REPLY]}},'200.2');
ck('a parent lookup that fails still yields the message',
   [r.ok,r.ok&&r.val.msg.ts,r.ok?r.val.parent:'threw'],[true,'200.2',null]);

r=await fetchFor({},'999.9');
ck('a message that is nowhere is an error, not a silent empty entry',r.ok,false);

// The month blob and the index are two separate puts. If the index one is lost,
// the page must not lose the month with it.
ck('the page does not derive its months from the index alone',
   (()=>{const i=src.indexOf("url.pathname === '/api/changelog'");
     const j=src.indexOf("url.pathname === '/api/data'",i);
     const b=src.slice(i,j);
     return /CONTENT_STORE\.list\(\{ prefix: 'log_' \}\)/.test(b);})(),true);
ck('  and the listing excludes the index key itself',
   (()=>{const i=src.indexOf("url.pathname === '/api/changelog'");
     const j=src.indexOf("url.pathname === '/api/data'",i);
     return src.slice(i,j).includes('/^log_\\d{4}-\\d{2}_v1$/');})(),true);
ck('  while still merging what the index knows',
   (()=>{const i=src.indexOf("url.pathname === '/api/changelog'");
     const j=src.indexOf("url.pathname === '/api/data'",i);
     const b=src.slice(i,j);
     return /concat\(Object\.keys\(idx\.months \|\| \{\}\)\)/.test(b);})(),true);
ck('  and a listing failure does not take the page down with it',
   (()=>{const i=src.indexOf("url.pathname === '/api/changelog'");
     const j=src.indexOf("url.pathname === '/api/data'",i);
     return /try \{ listed = await env\.CONTENT_STORE\.list/.test(src.slice(i,j));})(),true);

// Backfill repairs entries from earlier, more broken versions of the capture.
const BF=(()=>{const i=src.indexOf('async function slackBackfillEntries');
  const j=src.indexOf('async function slackSweep',i);
  return src.slice(i,j);})();
ck('the backfill fills a missing headline',/if \(!e\.headline\) \{/.test(BF),true);
ck('  and re-resolves an author left as a raw Slack id',
   /\/\^\[UW\]\[A-Z0-9\]\{6,\}\$\/\.test\(String\(e\.authorName \|\| ''\)\)/.test(BF)
   &&/await slackUserName\(e\.author, env\)/.test(BF),true);
ck('  touching an entry only when something actually changed',
   /if \(changed\) \{ touched = true; filled\+\+; \}/.test(BF),true);
ck('  and still capped',/filled >= SLACK_SWEEP_MAX/.test(BF),true);
ck('  with a summariser failure falling back, not looping',
   /_slackDeriveHeadline\(e\.summary \|\| ''\)/.test(BF),true);
ck('the cron runs it',/await slackBackfillEntries\(env\)/.test(src),true);
ck('  and the old name is gone',/slackBackfillHeadlines/.test(src),false);

// ── 5. The Changelog page ──────────────────────────────────
const dash=load(process.env.DASH_SRC||'dashboard.html');
const PG={console,Date,String,Number,Math,JSON,Array};
vm.createContext(PG);
vm.runInContext([fn(dash,'_clgWeekKey'),fn(dash,'_clgWeekLabel'),fn(dash,'_clgEsc'),
  fn(dash,'_clgHead'),fn(dash,'_clgName'),
  'Object.assign(globalThis,{_clgWeekKey,_clgWeekLabel,_clgEsc,_clgHead,_clgName});'].join('\n'),PG);

// Entries file under the week the change HAPPENED, not the week it was logged —
// a reaction added days later still lands in the right place.
ck('a week key is Monday-anchored',
   (()=>{const mon=PG._clgWeekKey(new Date(2026,9,19,9,0).getTime());
     return [new Date(mon).getDay(),new Date(mon).getDate()];})(),[1,19]);
ck('  every day of one week maps to the same key',
   (()=>{const ks=[19,20,21,22,23,24,25].map(d=>PG._clgWeekKey(new Date(2026,9,d,13,0).getTime()));
     return new Set(ks).size;})(),1);
ck('  and Sunday belongs to the week that preceded it',
   (()=>{const sun=PG._clgWeekKey(new Date(2026,9,25,23,0).getTime());
     const mon=PG._clgWeekKey(new Date(2026,9,19,1,0).getTime());
     return sun===mon;})(),true);
ck('  while the next Monday starts a new one',
   PG._clgWeekKey(new Date(2026,9,26,1,0).getTime())===PG._clgWeekKey(new Date(2026,9,19,1,0).getTime()),false);
// Jun 29 – Jul 5 2026: a week that straddles two months, far enough back that
// it takes a date label rather than a relative one.
ck('a week spanning two months names both',
   (()=>{const L=PG._clgWeekLabel(PG._clgWeekKey(new Date(2026,6,1).getTime()));
     return /Jun/.test(L)&&/Jul/.test(L);})(),true);
ck('  while a week inside one month names it once',
   (()=>{const L=PG._clgWeekLabel(PG._clgWeekKey(new Date(2026,6,8).getTime()));
     return (L.match(/Jul/g)||[]).length;})(),1);
// The two most-read headers are relative, because "this week" is what someone
// opening a changelog is usually looking for.
ck('the current week is named relatively',
   PG._clgWeekLabel(PG._clgWeekKey(Date.now())),'This week');
ck('  as is the one before it',
   PG._clgWeekLabel(PG._clgWeekKey(Date.now())-7*864e5),'Last week');
ck('  but not the one before that',
   /week/.test(PG._clgWeekLabel(PG._clgWeekKey(Date.now())-14*864e5)),false);

ck('summaries are escaped before they reach the page',
   PG._clgEsc('<img src=x onerror=alert(1)> & "q"'),
   '&lt;img src=x onerror=alert(1)&gt; &amp; &quot;q&quot;');
ck('  including every field rendered from the entry',
   (()=>{const i=dash.indexOf('function renderChangelog');
     const j=dash.indexOf('function _clgEsc',i);
     const body=dash.slice(i,j);
     return /_clgEsc\(_clgHead\(e\)\)/.test(body)&&/_clgEsc\(e\.summary\)/.test(body)
         && /_clgEsc\(e\.chName\)/.test(body)&&/_clgEsc\(nm\)/.test(body)
         && /_clgEsc\(e\.permalink\)/.test(body);})(),true);
ck('  and no entry text is concatenated unescaped',
   (()=>{const i=dash.indexOf('function renderChangelog');
     const j=dash.indexOf('function _clgEsc',i);
     const body=dash.slice(i,j);
     return !/\+\s*e\.(summary|chName|authorName|permalink|headline)\b/.test(body)
         && !/\+\s*nm\b/.test(body);})(),true);

// Formatting: a headline per entry, and a date big enough to read.
ck('the page prefers the stored headline',
   PG._clgHead({headline:'Inquiry form live',summary:'Long text here.'}),'Inquiry form live');
ck('  and derives one only when it is missing',
   PG._clgHead({summary:'Form went live. Sync works.'}),'Form went live');
ck('a raw Slack id is not shown as an author name',
   [PG._clgName('U077TPV2PFV'),PG._clgName('W012ABCDEF')],['','']);
ck('  but a real name is',PG._clgName('Susan Bowman'),'Susan Bowman');
ck('  and an empty name stays empty',PG._clgName(''),'');
ck('each row carries a headline, a summary and a date',
   (()=>{const i=dash.indexOf('function renderChangelog');
     const j=dash.indexOf('function _clgEsc',i);
     const b=dash.slice(i,j);
     return ['clg-head','clg-sum','clg-day','clg-dow','clg-row'].every(c=>b.includes(c));})(),true);
ck('the date is larger than the summary text',
   (()=>{const d=/\.clg-day\{[^}]*font-size:(\d+(?:\.\d+)?)px/.exec(dash);
     const s=/\.clg-sum\{[^}]*font-size:(\d+(?:\.\d+)?)px/.exec(dash);
     return !!d&&!!s&&parseFloat(d[1])>parseFloat(s[1]);})(),true);
ck('  and the headline is heavier than the summary',
   (()=>{const h=/\.clg-head\{[^}]*font-weight:(\d+)/.exec(dash);
     return !!h&&parseInt(h[1],10)>=600;})(),true);

ck('the menu row sits directly under BD Tracker',
   (()=>{const bd=dash.indexOf('data-sub="bd-dash"');
     const cl=dash.indexOf('data-sub="changelog"');
     const sl=dash.indexOf('data-sub="sales-dash"');
     return bd>0&&cl>bd&&cl<sl;})(),true);
ck('  and is wired to the in-page renderer, not a new tab',
   /onclick="switchRes\('changelog'\)" data-page="resources" data-sub="changelog"/.test(dash),true);
ck('  with a title of its own',/'changelog':'Changelog'/.test(dash),true);
ck('  and a dispatch branch',/else if\(sub==='changelog'\)renderChangelog\(el\);/.test(dash),true);
// PW is the page's password global; S.pw never existed, so the first build of
// this page could only ever 401.
ck('the page sends the password global that actually exists',
   /body:JSON\.stringify\(\{password:PW,months:12\}\)/.test(dash)&&!/S\.pw/.test(dash),true);
// U+1FAB5 is outside the BMP, so a 4-digit \u escape cannot express it and was
// being read as \u1FAB followed by a literal '5'.
ck('the wood emoji is written as a surrogate pair, not a truncated escape',
   (dash.match(/\\uD83E\\uDEB5/g)||[]).length>=2&&!/\\u1FAB5/i.test(dash),true);
// buildSectionNav clears the bar and returns early for a page with no sections;
// without the call, every Resources page keeps the previous page's bar.
ck('switching to a Resources page clears the stale Jump To bar',
   /buildSectionNav\('resources'\);/.test(dash),true);
ck('the page asks the password-guarded endpoint',
   /fetch\('\/api\/changelog'[\s\S]{0,160}?password:PW/.test(dash),true);
ck('an empty log explains how to add to it rather than looking broken',
   /Nothing logged yet\. React with/.test(dash),true);
ck('a load failure is surfaced, not swallowed',
   /if\(d\.error\) h\+=/.test(dash)&&/Could not load: /.test(dash),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
})();
