// Webinar-date deals: contacts whose `webinar_date` lands in the window, resolved
// to deals through the v4 association API and folded into "Demos to occur" /
// "Demos supposed to happen".
//
// The fold is ADDITIVE, so the only thing standing between it and double counting
// is the dedupe in applyWebinarDateDeals — a deal with both a demo booked in the
// window and a webinar-date contact must be counted once. That invariant is
// exercised against the real function with deliberately overlapping ids below.
const {load}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load('dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// fetchWebinarDateDealIds reaches hsSearch, toMsUTC, filterActiveBrands, SUBREQ and
// PRE_LAUNCH_TIER. Everything above `export default` is plain top-level
// declarations, so running the whole prefix resolves all of them at once.
const prefix=src.slice(0,src.indexOf('\nexport default'));
const logs=[];
const ctx={console:{log:()=>{},error:(...a)=>logs.push(a.join(' ')),warn:(...a)=>logs.push(a.join(' '))}};
vm.createContext(ctx);
// _mkSet / _isSet are built INSIDE the context on purpose: applyWebinarDateDeals
// gates on `extendedDealIds instanceof Set`, and a Set made out here belongs to a
// different realm, so it would silently take the empty-set fallback and every
// dedupe assertion below would pass for the wrong reason.
vm.runInContext(prefix+'\nObject.assign(globalThis,{fetchWebinarDateDealIds,applyWebinarDateDeals,'
  +'toMsUTC,toMsET,filterActiveBrands,PRE_LAUNCH_TIER,SUBREQ,subreqReset,'
  +'_mkSet:a=>new Set(a),_isSet:x=>x instanceof Set});',ctx);
ck('the real functions loaded',
   [typeof ctx.fetchWebinarDateDealIds,typeof ctx.applyWebinarDateDeals],['function','function']);

const PRE=ctx.PRE_LAUNCH_TIER;
const RES=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>JSON.stringify(j)});
const ERR=(s)=>({ok:false,status:s,json:async()=>({}),text:async()=>'boom'});

// Routes the three HubSpot endpoints the function talks to and records every
// request, so chunking and payload shape can be asserted on the real traffic.
function net(o){
  const log={search:[],assoc:[],deals:[]};
  ctx.fetch=async(url,opt)=>{
    const body=JSON.parse(opt.body||'{}');
    if(url.includes('/objects/contacts/search')){
      log.search.push(body);
      return RES({results:o.contacts||[]});
    }
    if(url.includes('/v4/associations/contacts/deals/batch/read')){
      const ids=body.inputs.map(i=>i.id); log.assoc.push(ids);
      if(o.assocFail&&o.assocFail(log.assoc.length))return ERR(500);
      return RES({results:ids.map(id=>({from:{id},to:((o.assoc||{})[id]||[]).map(d=>({toObjectId:d}))}))});
    }
    if(url.includes('/v3/objects/deals/batch/read')){
      const ids=body.inputs.map(i=>i.id); log.deals.push({ids,props:body.properties});
      if(o.dealFail&&o.dealFail(log.deals.length))return ERR(500);
      return RES({results:ids.map(id=>({id,properties:(o.deals||{})[id]||{}}))});
    }
    throw new Error('unexpected URL '+url);
  };
  return log;
}
let _c=0;
const mk=(p)=>({id:'c'+(++_c),properties:Object.assign({webinar_date:'1789000000000'},p||{})});
const run=(o)=>{const log=net(o);ctx.subreqReset();logs.length=0;
  return ctx.fetchWebinarDateDealIds('tok','2026-09-01','2026-09-30').then(r=>({r,log}));};

(async()=>{

// --- the contacts search -----------------------------------------------------
{
  const {r,log}=await run({contacts:[]});
  const b=log.search[0];
  // webinar_date is a HubSpot DATE stored at midnight UTC. Using the ET-offset
  // helper here shifts the window by 4-5h and silently drops the edge days.
  ck('the window uses the UTC date bounds, not the ET ones',
     [b.filterGroups[0].filters[0].value,b.filterGroups[0].filters[1].value],
     [String(ctx.toMsUTC('2026-09-01')),String(ctx.toMsUTC('2026-09-30',true))]);
  ck('and those bounds really differ from the ET ones',
     b.filterGroups[0].filters[0].value===String(ctx.toMsET('2026-09-01')),false);
  // A second OR-group (created in window + HAS webinar_date) would pull in people
  // registered for a webinar OUTSIDE the window — not "Webinar Date in window".
  ck('exactly one filter group: webinar ran in this window',log.search.length&&b.filterGroups.length,1);
  ck('both bounds are on webinar_date',
     b.filterGroups[0].filters.map(f=>f.propertyName+':'+f.operator),
     ['webinar_date:GTE','webinar_date:LTE']);
  // DESCENDING so a page-cap truncation drops the OLDEST, not the newest.
  ck('sorted newest webinar first',[b.sorts[0].propertyName,b.sorts[0].direction],['webinar_date','DESCENDING']);
  ck('it reads the tier and the attendance flag it needs',
     ['webinar_date','webinar_has_attended','average_monthly_web_traffic'].every(p=>b.properties.includes(p)),true);
  ck('no contacts means an empty Set, not a missing one',
     [ctx._isSet(r.dealIds),r.dealIds.size,r.contacts],[true,0,0]);
  ck('and it never touches the association API',[log.assoc.length,log.deals.length],[0,0]);
}

// --- guards ------------------------------------------------------------------
{
  // null (not 0) so a missing token reads as "unknown" and applyWebinarDateDeals
  // leaves the period untouched rather than asserting zero webinar deals.
  const a=await ctx.fetchWebinarDateDealIds('','2026-09-01','2026-09-30');
  const b=await ctx.fetchWebinarDateDealIds('tok',null,'2026-09-30');
  const c=await ctx.fetchWebinarDateDealIds('tok','2026-09-01',null);
  ck('a missing token or bound returns null, not an empty result',[a,b,c],[null,null,null]);
}

// --- pre-launch is dropped on the CONTACT side, before any lookup ------------
{
  const {r,log}=await run({
    contacts:[mk({average_monthly_web_traffic:PRE}),
              mk({average_monthly_web_traffic:'pre-launch / JUST LAUNCHING'}), // casing varies on contacts
              mk({average_monthly_web_traffic:'200K+ monthly web visitors'}),
              mk({average_monthly_web_traffic:''})],
    assoc:{}, deals:{}});
  ck('pre-launch contacts never reach the association call',r.contacts,2);
  ck('and the batch it did send holds only the survivors',log.assoc[0].length,2);
  // 0-10K / blank are NOT pre-launch — excluding them would shrink the metric.
  ck('a blank tier is kept',log.assoc[0].includes('c'+_c),true);
}

// --- dedupe of contacts, and of the deals they resolve to --------------------
{
  const dup=mk({average_monthly_web_traffic:''});
  const {r,log}=await run({
    contacts:[dup,dup,mk({}),],
    // Two contacts on the SAME deal — the Set is what stops it counting twice.
    assoc:{[dup.id]:[9001,9002],['c'+_c]:[9002]},
    deals:{'9001':{brand_status:'Paying'},'9002':{brand_status:''}}});
  ck('a contact returned twice is looked up once',[r.contacts,log.assoc[0].length],[2,2]);
  ck('two contacts sharing a deal yield one deal id',r.dealIds.size,2);
  // Ids are String()d on both sides; processPipelineDeals stores String(d.id) too,
  // so a numeric toObjectId leaking through would break the dedupe silently.
  ck('deal ids are strings, matching the pipeline side',[...r.dealIds].every(x=>typeof x==='string'),true);
  ck('resolved reports the pre-filter count',r.resolved,2);
}

// --- active-brand + pre-launch filtering on the DEAL side --------------------
{
  const a=mk({}),b=mk({}),c=mk({}),d=mk({}),e=mk({}),f=mk({});
  const {r,log}=await run({
    contacts:[a,b,c,d,e,f],
    assoc:{[a.id]:['d1'],[b.id]:['d2'],[c.id]:['d3'],[d.id]:['d4'],[e.id]:['d5'],[f.id]:['d6']},
    deals:{
      d1:{brand_status:'Paying'},
      d2:{brand_status:'Signed'},
      d3:{brand_status:''},                       // new pipeline deal — kept
      d4:{brand_status:'Churned'},
      d5:{brand_status:'Never Implemented - Churned'},
      d6:{brand_status:'Paying',average_monthly_web_traffic__cloned_:PRE},
    }});
  // The deal metrics these fold into only ever see filterActiveBrands(...), so a
  // Churned/Paused deal added back here makes the two halves describe different
  // populations.
  ck('only active brands survive',[...r.dealIds].sort(),['d1','d2','d3']);
  ck('and a pre-launch deal is dropped even when its brand is active',r.dealIds.has('d6'),false);
  ck('the resolved total still counts everything the associations returned',r.resolved,6);
  ck('the deal read asks for both filter properties',
     ['brand_status','average_monthly_web_traffic__cloned_'].every(p=>log.deals[0].props.includes(p)),true);
  // Same rule, same helper — if filterActiveBrands changes, this must move with it.
  ck('it is the shared filterActiveBrands rule',
     ctx.filterActiveBrands([{id:'d4',properties:{brand_status:'Churned'}}]).length,0);
}

// --- chunking at 100 ---------------------------------------------------------
{
  const contacts=[],assoc={};
  for(let i=0;i<250;i++){const m=mk({});contacts.push(m);assoc[m.id]=['D'+i];}
  const deals={};for(let i=0;i<250;i++)deals['D'+i]={brand_status:'Paying'};
  const {r,log}=await run({contacts,assoc,deals});
  // HubSpot batch reads cap at 100 inputs; a 250-id batch is a 400 for the whole
  // chunk, which used to zero the metric rather than losing a slice of it.
  ck('contacts are chunked at 100',log.assoc.map(b=>b.length),[100,100,50]);
  ck('and so are the resolved deals',log.deals.map(b=>b.ids.length),[100,100,50]);
  ck('every chunk contributes',[r.dealIds.size,r.contacts,r.failed],[250,250,0]);
}

// --- partial failure accumulates instead of collapsing to zero ---------------
{
  const contacts=[],assoc={},deals={};
  for(let i=0;i<150;i++){const m=mk({});contacts.push(m);assoc[m.id]=['E'+i];deals['E'+i]={brand_status:'Paying'};}
  const {r}=await run({contacts,assoc,deals,assocFail:n=>n===1});
  ck('one bad association batch loses only its own slice',r.dealIds.size,50);
  ck('and is reported as failed rather than silently absent',r.failed,100);
  ck('a degraded run says so in the log',logs.some(l=>/PARTIAL/.test(l)),true);
}
{
  const contacts=[],assoc={},deals={};
  for(let i=0;i<150;i++){const m=mk({});contacts.push(m);assoc[m.id]=['F'+i];deals['F'+i]={brand_status:'Paying'};}
  const {r}=await run({contacts,assoc,deals,dealFail:n=>n===2});
  ck('one bad deal batch loses only its own slice',r.dealIds.size,100);
  ck('and the two failure counts share one field',r.failed,50);
}

// --- webinar attendance ------------------------------------------------------
{
  const {r}=await run({
    contacts:[mk({webinar_has_attended:'true'}),
              mk({webinar_has_attended:'TRUE'}),
              mk({webinar_has_attended:'false'}),
              mk({webinar_has_attended:''}),                              // never reconciled
              mk({webinar_has_attended:'true',average_monthly_web_traffic:PRE})],
    assoc:{},deals:{}});
  ck('attendance is case-insensitive',r.attended,2);
  // A blank must read as a no-show, not vanish — otherwise the denominator
  // shrinks and the no-show rate flatters itself.
  ck('a blank flag counts as a registrant',r.registrants,4);
  ck('pre-launch is out of the no-show denominator too',r.registrants+r.attended,6);
}

// === applyWebinarDateDeals — the dedupe ======================================
const S=ctx._mkSet;
const per=(ext,sup,totalExtended)=>({pipeline:{
  extendedDealIds:S(ext),supposedDealIds:S(sup),totalExtended:totalExtended||0}});

{
  // Overlap on BOTH sides: d1 is already a booked demo, d2 is already counted as
  // supposed-to-happen, d3 is new to both. Counted twice, this reads 3/3.
  const p=per(['d1','d2'],['d2'],2);
  ctx.applyWebinarDateDeals(p,{dealIds:S(['d1','d2','d3']),contacts:7,registrants:5,attended:3});
  const pl=p.pipeline;
  ck('a deal already in Demos to occur is not added again',pl.webinarDateExtraExtended,1);
  ck('and the supposed side dedupes independently',pl.webinarDateExtraSupposed,2);
  ck('totalExtended grows by the EXTRA only',pl.totalExtended,3);
  ck('the headline count is still the full webinar-date set',pl.webinarDateDealCount,3);
  ck('contacts passes straight through',pl.webinarDateContacts,7);
  ck('no-show is registrants minus attended',[pl.webinarRegistrants,pl.webinarAttended,pl.webinarNoShow],[5,3,2]);
}
{
  // Full overlap is the case the dedupe exists for: nothing may move.
  const p=per(['d1','d2'],['d1','d2'],2);
  ctx.applyWebinarDateDeals(p,{dealIds:S(['d1','d2'])});
  ck('a fully-overlapping set adds nothing',
     [p.pipeline.webinarDateExtraExtended,p.pipeline.webinarDateExtraSupposed,p.pipeline.totalExtended],[0,0,2]);
  ck('but the webinar-date count itself is still reported',p.pipeline.webinarDateDealCount,2);
}
{
  // No overlap — every id is genuinely new.
  const p=per(['a'],['a'],1);
  ctx.applyWebinarDateDeals(p,{dealIds:S(['x','y','z'])});
  ck('a disjoint set adds all of it',[p.pipeline.webinarDateExtraExtended,p.pipeline.totalExtended],[3,4]);
}
{
  // The two sets are not the same population — supposed-to-happen only holds
  // deals with an attendance outcome, so sharing one dedupe would undercount.
  const p=per(['s1','s2','s3'],[],0);
  ctx.applyWebinarDateDeals(p,{dealIds:S(['s1','s2'])});
  ck('extended and supposed dedupe against their own sets',
     [p.pipeline.webinarDateExtraExtended,p.pipeline.webinarDateExtraSupposed],[0,2]);
}
{
  // The pipeline stores String(d.id); a number here would miss every match and
  // double every deal.
  const p=per(['123'],['123'],1);
  ctx.applyWebinarDateDeals(p,{dealIds:S(['123'])});
  const q=per(['123'],['123'],1);
  ctx.applyWebinarDateDeals(q,{dealIds:S([123])});
  ck('string ids dedupe',p.pipeline.webinarDateExtraExtended,0);
  ck('and a numeric id would NOT — which is why the fetch stringifies',q.pipeline.webinarDateExtraExtended,1);
  ck('the fetch stringifies both sides',
     /String\(t\.toObjectId/.test(src)&&/active\.add\(String\(dd\.id\)\)/.test(src),true);
}
{
  // A failed lookup hands back null. Leaving the period alone beats writing zeros
  // the deltas would then render as a real drop.
  const p=per(['d1'],['d1'],1);
  ctx.applyWebinarDateDeals(p,null);
  ctx.applyWebinarDateDeals(p,{});
  ctx.applyWebinarDateDeals(p,{dealIds:null});
  ctx.applyWebinarDateDeals(null,{dealIds:S(['x'])});
  ctx.applyWebinarDateDeals({},{dealIds:S(['x'])});
  ck('a null or shapeless result leaves the period untouched',
     [p.pipeline.totalExtended,p.pipeline.webinarDateDealCount===undefined],[1,true]);
}
{
  // A period that never ran processPipelineDeals has no id Sets; falling back to
  // an empty Set must not throw, and must count everything as new.
  const p={pipeline:{totalExtended:0}};
  ctx.applyWebinarDateDeals(p,{dealIds:S(['x','y'])});
  ck('missing id sets degrade to "nothing counted yet"',
     [p.pipeline.webinarDateExtraExtended,p.pipeline.webinarDateExtraSupposed,p.pipeline.totalExtended],[2,2,2]);
}
{
  // attended > registrants cannot happen upstream, but a clamp is cheaper than a
  // negative no-show rate on the card.
  const p=per([],[],0);
  ctx.applyWebinarDateDeals(p,{dealIds:S([]),registrants:2,attended:5});
  ck('no-show never goes negative',p.pipeline.webinarNoShow,0);
}

// --- the fetch feeds the fold end to end -------------------------------------
{
  const a=mk({}),b=mk({});
  const {r}=await run({contacts:[a,b],
    assoc:{[a.id]:['k1'],[b.id]:['k2']},
    deals:{k1:{brand_status:'Paying'},k2:{brand_status:'Paying'}}});
  const p=per(['k1'],[],5);
  ctx.applyWebinarDateDeals(p,r);
  ck('a real fetch result dedupes against the real pipeline sets',
     [p.pipeline.webinarDateDealCount,p.pipeline.webinarDateExtraExtended,p.pipeline.totalExtended],[2,1,6]);
}

// --- wiring ------------------------------------------------------------------
// Each period is folded separately; folding only `current` would leave the
// prior-window deltas comparing two different definitions.
ck('all three periods are fetched and folded',
   [(src.match(/fetchWebinarDateDealIds\(hsToken/g)||[]).length,
    (src.match(/applyWebinarDateDeals\((currentData|priorData|priorMonthData)/g)||[]).length],[3,3]);
// The lookup is best-effort: a throw here must not take down the whole response.
ck('the lookup is wrapped so a failure degrades instead of 500ing',
   /try \{\s*\[_wdCur, _wdPri, _wdPm\] = await Promise\.all\(/.test(src),true);
ck('the extras reach the payload',
   ['webinarDateDealCount','webinarDateExtraSupposed','webinarRegistrants','webinarNoShow']
     .every(f=>new RegExp(f+':\\s*c\\.pipeline\\.'+f).test(src)),true);
// The rate is derived in the payload, not on the client, so prior/last-month use
// the same formula.
ck('the no-show rate is computed server-side for all three periods',
   (src.match(/webinarNoShowRate(Prior|LastMonth)?:/g)||[]).length,3);
ck('and a zero denominator yields null, not NaN or 0',
   /webinarNoShowRate: \(c\.pipeline\.webinarRegistrants\|\|0\) > 0[\s\S]{0,120}: null/.test(src),true);
ck('the client renders the webinar-date extras',
   /qmeta\.webinarDateExtraSupposed/.test(cli)&&/qmeta\.webinarRegistrants/.test(cli),true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
})();
