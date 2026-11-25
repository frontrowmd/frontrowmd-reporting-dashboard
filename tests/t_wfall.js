// BD Tracker Churn Waterfall tab (2026-11-24): the worker runs Metabase card
// 3565 and shapes it; the page shows every brand (W1) and a C3-style cohort
// grid (W2). Alex's rules: pilots count from their first invoice; a churned
// brand's churn date is the last month it was invoiced.
const fs=require('fs'),vm=require('vm');
const {load}=require('./slice.js');
let pass=0,fail=0;const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. Worker: run the card, shape it, cache it ─────────────────────────────
const src=load(process.env.WORKER_SRC||'worker.js');
const W={console:{log(){},warn(){},error(){}},Date,JSON,Math};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nObject.assign(globalThis,{bdWfShape,bdWfNative,bdWfDefaultsNeutral,fetchBdWaterfall,BD_WF_CARD,BD_WF_KV,METABASE_URL,MB_ROW_CAP});',W);
const COLS=['BRAND_KEY','BRAND_NAME','SOURCE','GROUP_NAME','CHURN_STATUS','HUBSPOT_STAGE','CHURN_MONTH','LAST_INVOICED_MONTH','CHURN_SOURCE','MONTHS_UNTIL_CHURN','MONTHS_INVOICED',
  'EVER_IMPLEMENTED_WIDGET','EVER_PAID_INVOICE','CHURN_TYPE','COMPANY_SEGMENT','AB_TEST','WEB_TIER_AT_CHURN','WEB_TIER_SOURCE','SELF_SELECTED_TRAFFIC','IMPLIED_TRAFFIC','IMPLIED_VIEWS_30D',
  'PRODUCTS_EVER_LIVE_COUNT','PRODUCTS_LIVE_AT_CHURN_COUNT','PACKAGE','PILOT_STATUS','FIRST_MONTH',"'2026-07'","'2026-08'","'2026-09'","'2026-10'"];
const row=(k,n,st,hsCh,li,fm,a,x)=>{x=x||{};return [k,n,'Recurly',null,st,st==='Active'?'Closed Won':'Churned',hsCh,li,hsCh?'HubSpot deal':null,null,x.mi||1,x.impl||'Yes','Yes',x.ty||'Not recorded','VIP','Not recorded','0-10K','Implied traffic','Not selected','0-10K',100,
  x.pe==null?1:x.pe,0,'Pro',x.pil||'No pilot',fm+'-01T00:00:00-04:00'].concat(a);};
const rows=[row('1','Stay Co','Active',null,'2026-10','2026-07',[1000,1000,1000,1000],{mi:4}),
  row('2','Gone Co','Churned','2026-10','2026-08','2026-07',[500,500,0,null],{mi:2,ty:'Board Defined'}),
  row('3','Pilot Co','Active',null,'2026-09','2026-09',[null,null,5000,0],{pil:'Pilot (not converted)'}),
  row('4','Never Co','Churned','2026-09','2026-08','2026-08',[null,300,null,null],{impl:'No'}),
  row('5','Paused Co','Paused','2026-10','2026-09','2026-08',[null,'1,200',1200,0],{mi:2})];
const ok=(j)=>({ok:true,status:200,json:async()=>j,text:async()=>JSON.stringify(j)});
let calls=[];
// The card as GET /api/card/3565 returns it: native SQL plus filters that all carry saved defaults.
const TAGS={hide_pilot_initiation_fee:{type:'boolean',name:'hide_pilot_initiation_fee',default:[true],required:true},
  basis:{type:'text',name:'basis',default:['All invoiced'],required:true},
  package:{type:'text',name:'package',default:['Pro','Hero']},churn_type:{type:'text',name:'churn_type',default:['Board Defined'],required:false},
  products_ever_live:{type:'number',name:'products_ever_live'}};
const CARD={id:3565,database_id:67,dataset_query:{database:67,type:'native',native:{query:'select 1 -- card sql',"template-tags":TAGS}}};
let dsRows=rows;
const CURROWS=[['1','Stay Co','2026-07','2026-07',1000,1,0,'2026-11'],['2','Gone Co','2026-07','2026-07',0,0,0,'2026-11'],
  ['901','Fresh Pilot','2026-11','2026-11',3000,1,0,'2026-11'],['902','Refunded Co',null,'2026-03',0,0,0,'2026-11']];
const RCCOLS=['BRAND_KEY','LAST_GOOD','CUR_AMT','CUR_DAY','BAD_N','LAST_BAD','ZERO_N','FIRST_ZERO','LIVE_N','PAUSED_SINCE','RUNNING_N','NEXT_BILL','FUTURE_N','TRIAL_END','TODAY'];
const RCROWS=[['1','2026-11-05',1000,'2026-11-05',0,null,0,null,1,null,1,'2026-12-05',0,null,'2026-11-24']];
const CATCOLS=['CATEGORY','AMOUNT','MULTI_MONTH','INVOICES','BRANDS','MONTH','BRANDS_BILLED'];
const CATROWS=[['plan',3000,1000,3,3,'2026-10',4],['pilot_initiation_fee',2500,0,1,1,'2026-10',4],['review',300,0,2,1,'2026-10',4],['credits',-200,0,1,1,'2026-10',4]];
W.fetch=async(url,init)=>{calls.push({url,init});
  if(url.endsWith('/api/card/3565'))return ok(CARD);
  if(url.endsWith('/api/dataset')){if(/with d as/.test(JSON.parse(init.body).native.query))return ok({status:'completed',data:{cols:[{name:'BRAND_KEY'},{name:'LIVE'},{name:'DATA_FROM'}],rows:[['1','quant=2026-07,2026-10;sticker=2026-08','2025-12'],['2','sticker=2026-08','2025-12']]}});
    if(/rnames as/.test(JSON.parse(init.body).native.query))return ok({status:'completed',data:{cols:['BRAND_KEY','NAME','FG','FA','CUR_NET','LIVE','DISCARDED','MONTH'].map(name=>({name})),rows:CURROWS}});
    if(/with cm as/.test(JSON.parse(init.body).native.query))return ok({status:'completed',data:{cols:RCCOLS.map(name=>({name})),rows:RCROWS}});
    if(/with base as/.test(JSON.parse(init.body).native.query))return ok({status:'completed',data:{cols:CATCOLS.map(name=>({name})),rows:CATROWS}});
    return ok({status:'completed',data:{cols:COLS.map(name=>({name})),rows:dsRows}});}
  if(url.endsWith('/api/dataset/json'))return ok(dsRows.map(r=>{const o={};COLS.forEach((c,i)=>o[c]=r[i]);return o;}));
  return {ok:false,status:404,text:async()=>''};};
const kv={},KV={get:async(k,t)=>kv[k]?(t==='json'?JSON.parse(kv[k]):kv[k]):null,put:async(k,v)=>{kv[k]=v;}};
(async()=>{
  const env={METABASE_API_KEY:'k',CONTENT_STORE:KV};
  const d=JSON.parse(JSON.stringify(await W.fetchBdWaterfall(env,false)));
  const dsCalls=calls.filter(c=>c.url.endsWith('/api/dataset')).map(c=>JSON.parse(c.init.body));
  ck('the worker reads the saved card, runs its SQL, the category query and the Recurly status query',[calls.filter(c=>c.url.endsWith('/api/card/3565')).length,dsCalls.length],[1,5]);
  const sent=dsCalls.find(q=>/card sql/.test(q.native.query));
  const cq=dsCalls.find(q=>/with base as/.test(q.native.query));
  ck('  the category query covers last complete month, Recurly charges and credits, test accounts out',[/dateadd\(month, -1, date_trunc\('month', current_date\)\)/.test(cq.native.query),/'1475', '1433'/.test(cq.native.query),/i\.number <> '5291'/.test(cq.native.query)],[true,true,true]);
  ck('  the card\u2019s own SQL, on its database',[sent.native.query,sent.database,sent.type],['select 1 -- card sql',67,'native']);
  ck('  Hide Pilot Initiation Fee is OFF (its saved default is on), basis All invoiced',[sent.native['template-tags'].hide_pilot_initiation_fee.default,sent.native['template-tags'].basis.default],[[false],['All invoiced']]);
  ck('  every other filter has its saved default cleared, so its clause drops out',['package','churn_type','products_ever_live'].map(k=>[sent.native['template-tags'][k].default,sent.native['template-tags'][k].required]),[[null,false],[null,false],[null,false]].map(x=>[undefined,false]));
  ck('  no parameters are sent (nothing for Metabase to fill from a default)',sent.parameters,undefined);
  ck('an MBQL 5 card shape is read too',W.bdWfNative({database_id:67,dataset_query:{stages:[{native:'select 2','template-tags':{a:{}}}]}}).query,'select 2');
  ck('the quoted month columns become the month list',d.months,['2026-07','2026-08','2026-09','2026-10']);
  ck('FIRST_MONTH (a timestamp) becomes YYYY-MM',d.brands.map(b=>b.fm),['2026-07','2026-07','2026-09','2026-08','2026-08']);
  ck('amounts stay null where the card is empty, and "1,200" is a number',[d.brands[1].a,d.brands[4].a],[[500,500,0,null],[null,1200,1200,0]]);
  ck('fields by name: status, churn months, pilot, implemented',[d.brands[1].st,d.brands[1].hsCh,d.brands[1].li,d.brands[2].pil,d.brands[3].impl],['Churned','2026-10','2026-08','Pilot (not converted)',false]);
  ck('meta names the card and its Metabase link',[d.meta.card,d.meta.url],[3565,'https://frontrowmd.metabaseapp.com/question/3565']);
  ck('the result is cached in KV',!!kv[W.BD_WF_KV],true);
  calls=[];const again=await W.fetchBdWaterfall(env,false);
  ck('  a second load reads the cache, no Metabase run',[calls.length,again.cached],[0,true]);
  calls=[];await W.fetchBdWaterfall(env,true);
  ck('  Refresh (fresh) runs the card again',calls.length,6);
  ck('this month so far rides along per brand',[d.current.m,d.current.byKey['901'],d.current.byKey['902'].fg],['2026-11',{v:3000,n:'Fresh Pilot',fg:'2026-11',fa:'2026-11',live:1,disc:0},'']);
  ck('the widget types ride along per brand, gpt variants folded',[d.widgets.from,d.widgets.byKey['1'],d.widgets.byKey['2']],['2025-12',{quant:['2026-07','2026-10'],sticker:['2026-08']},{sticker:['2026-08']}]);
  ck('the Recurly status rides along, keyed by brand',[d.recurly.today,d.recurly.byKey['1']],['2026-11-24',{lg:'2026-11-05',ca:1000,cd:'2026-11-05',bn:0,lb:'',zn:0,fz:'',ln:1,ps:'',rs:'',rn:1,nb:'2026-12-05',fn:0,te:''}]);
  ck('the categories ride along in the payload',[d.categories.month,d.categories.cats.map(c=>c.cat),d.categories.cats[0].multi],['2026-10',['plan','pilot_initiation_fee','review','credits'],1000]);
  ck('  with the distinct brands billed',d.categories.brandsBilled,4);
  let err='';try{W.bdWfShape(['BRAND_KEY','BRAND_NAME'],[]);}catch(e){err=e.message;}
  ck('a card missing a needed column is an error, never an empty tab',/missing column/.test(err),true);
  // Past the 2,000-row cap the export endpoint is used.
  const big=Array.from({length:W.MB_ROW_CAP},(_,i)=>row(String(i+10),'B'+i,'Active',null,'2026-10','2026-07',[1,1,1,1]));
  const exp=big.map(r=>{const o={};COLS.forEach((c,i)=>o[c]=r[i]);return o;});
  const more=big.concat([row('9999','Extra','Active',null,'2026-10','2026-07',[1,1,1,1])]);
  W.fetch=async(url,init)=>{calls.push({url,init});if(url.endsWith('/api/card/3565'))return ok(CARD);if(url.endsWith('/api/dataset'))return ok({status:'completed',data:{cols:COLS.map(name=>({name})),rows:big}});
    if(url.endsWith('/api/dataset/json'))return ok(more.map(r=>{const o={};COLS.forEach((c,i)=>o[c]=r[i]);return o;}));return {ok:false,status:404,text:async()=>''};};
  calls=[];const d2=await W.fetchBdWaterfall(env,true);
  const cardCalls=calls.filter(c=>!(c.init&&typeof c.init.body==='string'&&/with (base|cm|d) as|rnames as/.test(c.init.body)));
  ck('at the row cap it switches to the uncapped dataset export (no card column settings)',[cardCalls.map(c=>c.url.replace(W.METABASE_URL,'')),d2.brands.length],[['/api/card/3565','/api/dataset','/api/dataset/json'],W.MB_ROW_CAP+1]);
  const xc=cardCalls[2];
  ck('  the export is sent form-encoded, unformatted, with the same query',[xc.init.headers['Content-Type'],/&format_rows=false$/.test(xc.init.body),JSON.parse(decodeURIComponent(xc.init.body.slice(6).replace(/&format_rows=false$/,''))).native.query],['application/x-www-form-urlencoded',true,'select 1 -- card sql']);
  W.fetch=async(url,init)=>{if(url.endsWith('/api/card/3565'))return ok(CARD);if(url.endsWith('/api/dataset'))return ok({status:'completed',data:{cols:COLS.map(name=>({name})),rows:big}});
    if(url.endsWith('/api/dataset/json'))return ok(more.map(r=>{const o={};COLS.slice(0,-1).forEach((c,i)=>o[c]=r[i]);return o;}));return {ok:false,status:404,text:async()=>''};};
  const r2=await W.fetchBdWaterfall(env,true);
  ck('  an export missing a month column is an error (here: the last good copy is shown, saying why)',[!!r2.stale,/month columns/.test((r2.stale||{}).error)],[true,true]);
  // Metabase timing out (Alex, 2026-11-24: "Metabase /api/dataset 524"): the last good copy, flagged stale.
  W.fetch=async(url,init)=>{if(url.endsWith('/api/card/3565'))return ok(CARD);if(url.endsWith('/api/dataset')&&/card sql/.test(JSON.parse(init.body).native.query))return {ok:false,status:524,text:async()=>'error code: 524'};return {ok:false,status:404,text:async()=>''};};
  const r3=await W.fetchBdWaterfall(env,true);
  ck('a Metabase timeout serves the last good copy with a note and when it is from',[!!r3.brands,/did not answer in time/.test(r3.stale.error),!!r3.stale.asOf],[true,true,true]);
  let e3='';try{await W.fetchBdWaterfall({METABASE_API_KEY:'k',CONTENT_STORE:{get:async()=>null,put:async()=>{}}},true);}catch(e){e3=e.message;}
  ck('  with no saved copy at all it is still an error, never an empty tab',/524/.test(e3),true);
  ck('the card is run by id when its own filters have no saved defaults (so a Metabase cache can answer)',[W.bdWfDefaultsNeutral(TAGS),W.bdWfDefaultsNeutral({basis:{default:['All invoiced']},hide_pilot_initiation_fee:{default:[false]},package:{}}),W.bdWfDefaultsNeutral({hide_pilot_initiation_fee:{default:[true]}})],[false,true,false]);
  const wsrc=fs.readFileSync('worker.js','utf8');
  ck('the route is signed-in only',/url\.pathname === '\/api\/bd-waterfall'\) \{[\s\S]{0,200}if \(!\(await authed\(\)\)\) return jr\(\{ error: 'Unauthorized' \}, 401\);/.test(wsrc),true);

  // Older page tests run without this-month data; the block that tests it puts it back.
  const dCur=d.current;delete d.current;
  // ── 2. The page ────────────────────────────────────────────────────────────
  const html=fs.readFileSync('bd-dashboard.html','utf8');
  const scripts=[...html.matchAll(/<script(?![^>]*src)[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
  const els={};
  const mk=id=>els[id]||(els[id]={id,style:{},innerHTML:'',textContent:'',value:'',dataset:{},children:[],addEventListener(){},removeEventListener(){},appendChild(){},setAttribute(){},getAttribute(){return null},querySelector(){return null},querySelectorAll(){return []},classList:{add(){},remove(){},toggle(){},contains(){return false}},getBoundingClientRect(){return{top:0,left:0,width:0,height:0}},scrollIntoView(){},focus(){}});
  const document={readyState:'complete',getElementById:mk,querySelector:()=>mk('q'),querySelectorAll:()=>[],addEventListener(){},removeEventListener(){},createElement:()=>mk('c'+Math.random()),body:mk('body'),documentElement:mk('html'),head:mk('head')};
  let pageFetches=[];
  const P={console:{log(){},warn(){},error(){}},document,location:{protocol:'https:',hash:'',search:'',href:''},navigator:{userAgent:''},Intl,Date,Math,JSON,
    fetch:(u,o)=>{pageFetches.push(u);return new Promise(()=>{});},setTimeout:()=>0,clearTimeout(){},setInterval:()=>0,clearInterval(){},requestAnimationFrame:()=>0,
    localStorage:{getItem(){return null},setItem(){}},sessionStorage:{getItem(){return null},setItem(){}},alert(){},getComputedStyle:()=>({}),scrollTo(){},
    addEventListener(){},removeEventListener(){},matchMedia:()=>({matches:false,addEventListener(){}}),Chart:function(){return{destroy(){},update(){}}},
    ResizeObserver:function(){return{observe(){},disconnect(){}}},IntersectionObserver:function(){return{observe(){},disconnect(){}}},
    innerWidth:1400,innerHeight:900,scrollY:0,history:{replaceState(){}}};
  P.window=P;P.self=P;vm.createContext(P);
  for(const s of scripts){try{vm.runInContext(s,P);}catch(e){console.log('script load error:',e.message);}}
  ck('the tab button and its nav anchors exist',/data-tab="waterfall"[^>]*>Billing</.test(html)&&/id="bd-nav-wf"/.test(html)&&/data-sec="bd-sec-wf1"/.test(html)&&/data-sec="bd-sec-wf2"/.test(html),true);
  P.BD.data={deals:[],ownerMap:{},meta:{generatedAt:new Date().toISOString()}};
  pageFetches=[];P.bdSetTab('waterfall');
  ck('opening the tab asks the worker once',pageFetches,['/api/bd-waterfall']);
  ck('  and shows a loading band meanwhile',/Running the Metabase Churn Waterfall question/.test(els['bd-content'].innerHTML),true);
  P.BD.wf=d;
  const R=()=>{P.renderBD();return els['bd-content'].innerHTML;};
  let h=R();
  // Rules
  const by=k=>P.BD.wf.brands.find(b=>b.k===k);
  ck('churn date = the last month invoiced, not HubSpot’s',[P._bdWfChurnMonth(by('2')),P._bdWfChurnMonth(by('5')),P._bdWfChurnMonth(by('1'))],['2026-08','2026-09','']);
  ck('Paused counts as churned; Active does not',[P._bdWfChurned(by('5')),P._bdWfChurned(by('1')),P._bdWfChurned(by('3'))],[true,false,false]);
  ck('Never Implemented: a never-live churn with no type; Board Defined wins',[P._bdWfIsNI(by('4')),P._bdWfIsNI(by('2')),P._bdWfIsNI(by('1'))],[true,false,false]);
  // W1
  ck('W1 lists every brand, first-invoice order',[...h.matchAll(/<td class="bd-wf-b" title="[^"]*">([^<]+)<span/g)].map(m=>m[1]),['Gone Co','Stay Co','Never Co','Paused Co','Pilot Co']);
  const rowOf=n=>{let i=h.indexOf('">'+n+'<span class="bd-wf-k">');if(i<0)return '';i=h.lastIndexOf('<tr>',i);return h.slice(i,h.indexOf('</tr>',i));};
  ck('the churn month cell is the last invoiced one, outlined',/<td class="m chm zero" title="Churn month: the last month invoiced">0<\/td>/.test(rowOf('Gone Co'))===false&&/<td class="m chm" title="Churn month: the last month invoiced">500<\/td>/.test(rowOf('Gone Co')),true);
  ck('  and the months after it are blank',(rowOf('Gone Co').match(/class="m post"/g)||[]).length,2);
  ck('months before the first invoice are blank',(rowOf('Pilot Co').match(/class="m pre"/g)||[]).length,2);
  ck('the pilot’s first invoice (its fee) starts its row',/<td class="m">5,000<\/td>/.test(rowOf('Pilot Co')),true);
  ck('the tooltip keeps Metabase’s own churn month',/Churn month used: Aug 26 \(last month invoiced\) · Metabase’s churn month: Oct 26/.test(rowOf('Gone Co')),true);
  ck('month totals: brands invoiced and dollars',/<tr class="tot"><td class="bd-wf-b">Brands invoiced<\/td><td colspan="8"><\/td><td class="m">2<\/td><td class="m">4<\/td><td class="m">3<\/td><td class="m">1<\/td><\/tr>/.test(h),true);
  ck('Never Implemented is excluded from W2 by default (Alex, 2026-11-24)',P._bdWfNI,'excl');
  const hdrs=x=>[...x.slice(x.indexOf('class="bd-wf-t"')).split('</tr>')[0].matchAll(/<th[^>]*>([^<]+)(?:<span class="bd-wf-arr">[^<]*<\/span>)?<\/th>/g)].map(m=>m[1]);
  ck('W1 by default: Brand, the seven columns, a "+ more" toggle, then the months',hdrs(h).slice(0,10),['Brand','Churn Status','Company Segment','First Month','Last Invoiced','Churn Month','Invoiced Months','Total','+ 25 more','Jul 26']);
  ck('  Gone Co: churn month = last invoiced (Aug 26), 2 invoiced months',/<td>Aug 26<\/td><td>Aug 26<\/td><td class="m">2<\/td>/.test(rowOf('Gone Co')),true);
  P.bdWfSet('cols','all');h=R();
  ck('  expanded: Months Until Churn = first to last invoice, blank while active',[/<td class="m bd-wf-x">1<\/td>/.test(rowOf('Gone Co')),/<td class="m bd-wf-x"><\/td>/.test(rowOf('Stay Co'))],[true,true]);
  P.bdWfSet('cols','default');h=R();
  // Header sorting (Alex, 2026-11-24)
  const order=x=>[...x.matchAll(/<td class="bd-wf-b"[^>]*>(?:<span class="bd-wf-flag[^>]*>[^<]*<\/span>)?([^<]+)<span/g)].map(m=>m[1]);
  P.bdWfSortBy('c:tot');h=R();const byTot=order(h);
  P.bdWfSortBy('c:tot');h=R();const byTotUp=order(h);
  ck('clicking Total sorts largest first; a second click reverses it',[byTot[0],byTotUp[0],/class="bd-wf-s m on"[^>]*>Total<span class="bd-wf-arr">▲<\/span>/.test(h)],['Pilot Co','Never Co',true]);
  P.bdWfSortBy('c:cm');h=R();
  ck('  a text/month column sorts ascending with blanks last (active brands have no churn month)',order(h).slice(-2).sort(),['Pilot Co','Stay Co']);
  P.bdWfSortBy('m:2');h=R();
  ck('  a month column sorts by that month’s amount, largest first',order(h)[0],'Pilot Co');
  ck('  the Sort dropdown names the column sort',/<option selected>Sep 26 ▼<\/option>/.test(h),true);
  P.bdWfSet('sort','first');h=R();
  ck('W2 colours run green to yellow to red (Alex, 2026-11-24)',[P._bdWfFill(1),P._bdWfFill(0.55),P._bdWfFill(0.30),P._bdWfFill(0.1),(()=>{const m=P._bdWfFill(0.75).match(/\d+/g).map(Number);return m[1]>m[0];})()],['rgb(111,199,147)','rgb(251,227,142)','rgb(244,163,163)','rgb(244,163,163)',true]);
  P.bdWfSet('ni','incl');h=R();
  // W2
  const grid=h.slice(h.indexOf('id="bd-sec-wf2"'));
  const cohort=m=>{const i=grid.indexOf('<td class="rw-c">'+m+'</td>');if(i<0)return null;const r=grid.slice(i,grid.indexOf('</tr>',i));return [...r.matchAll(/<td(?: class="rw-n")?(?: title="[^"]*")?(?: style="[^"]*")?>([^<]*)<\/td>/g)].map(x=>x[1]).filter(x=>x!=='');};
  // Jul 26 cohort: Stay (active) + Gone (last invoiced Aug = month 2). Retained M1, M2; lost from M3.
  ck('W2 Jul cohort: retained through the last invoiced month, lost after',cohort('Jul 26'),['2','100%','100%','50.0%','50.0%']);
  // Aug cohort: Never Co (last inv Aug = M1) + Paused Co (last inv Sep = M2).
  ck('  Aug cohort: Month 1 is always 100%',cohort('Aug 26'),['2','100%','50.0%','0%']);
  ck('  Sep cohort: the pilot, active',cohort('Sep 26'),['1','100%','100%']);
  P.bdWfSet('ni','excl');h=R();const g2=h.slice(h.indexOf('id="bd-sec-wf2"'));
  ck('Never Implemented: Excluded drops never-live churns from their cohort',/<td class="rw-c">Aug 26<\/td><td class="rw-n">1<\/td>/.test(g2),true);
  ck('  and the footnote counts it',/1 Never Implemented churn is left out/.test(h),true);
  P.bdWfSet('from','2027');h=R();
  ck('  a "from" year past the data draws the last cohort, never throws',[/<td class="rw-c">Oct 26<\/td>/.test(h),/0 Never Implemented churns are left out/.test(h)],[true,true]);
  P.bdWfSet('from','all');
  P.bdWfSet('ni','incl');
  P.bdWfSet('pil','excl');h=R();
  ck('Pilots: Excluded drops pilots with no plan bill yet, from both views',[/Pilot Co<span/.test(h),/<td class="rw-c">Sep 26<\/td><td class="rw-n">·<\/td>/.test(h)],[false,true]);
  P.bdWfSet('pil','incl');
  P.bdWfSetF('ty','Board Defined');h=R();
  ck('a filter narrows both views',[...h.matchAll(/<td class="bd-wf-b" title="[^"]*">([^<]+)<span/g)].map(m=>m[1]).concat([/<td class="rw-c">Jul 26<\/td><td class="rw-n">1<\/td>/.test(h)]),['Gone Co',true]);
  ck('  and its dropdown counts each value given the other filters',/<option value="Board Defined" selected>Board Defined \(1\)<\/option>/.test(h)&&/<option value="Not recorded">Not recorded \(4\)<\/option>/.test(h),true);
  P.bdWfReset();
  P.bdWfSet('metric','churn');P.bdWfSet('mode','n');h=R();
  ck('Churn + Brands shows lost brands as a count',cohort.call(null,'Jul 26')!==null&&/<td class="rw-c">Jul 26<\/td><td class="rw-n">2<\/td><td[^>]*>0<\/td><td[^>]*>0<\/td><td[^>]*>−1<\/td>/.test(h),true);
  P.bdWfSet('metric','ret');P.bdWfSet('mode','pct');
  // ── Abnormalities ──
  const M=['2026-01','2026-02','2026-03','2026-04','2026-05','2026-06','2026-07','2026-08','2026-09','2026-10'];
  const BR=(o)=>Object.assign({k:'x',n:'X',src:'Recurly',st:'Active',stage:'Closed Won',hsCh:'',chSrc:'',li:'2026-10',mi:10,impl:true,paid:true,ty:'Not recorded',pe:1,pc:1,pkg:'Pro',pil:'No pilot',fm:'2026-01',a:M.map(()=>1000)},o);
  const ids=b=>P._bdWfFlagsOf(b,M).map(f=>f.id);
  ck('a healthy brand has no flags',ids(BR({})),[]);
  ck('active but nothing invoiced for 2+ months',ids(BR({li:'2026-08',a:[1000,1000,1000,1000,1000,1000,1000,1000,0,0]})),['active-not-billed']);
  {const stale=BR({li:'2026-08',a:[1000,1000,1000,1000,1000,1000,1000,1000,0,0]});
   const rc=x=>({today:'2026-11-24',byKey:x===undefined?{}:{x:Object.assign({ln:1,rn:1,ca:0,bn:0,zn:0,lg:'',lb:'',fz:'',ps:'',nb:'',fn:0,te:''},x)}});
   const f=x=>P._bdWfFlagsOf(stale,M,rc(x)).filter(z=>z.id==='active-not-billed');
   ck('Recurly check: invoiced this month clears the flag (Happy Mammoth: Sep credited, re-invoiced Nov 23)',f({ca:2500,cd:'2026-11-23',bn:1,lb:'2026-09-23',lg:'2026-11-23'}).length,0);
   ck('  due to bill before month end, with clean history, clears it',f({nb:'2026-11-28'}).length,0);
   ck('  but not when its recent invoices failed or were credited',/2 recent invoices failed or were credited \(latest 10\/29\/2026\); next attempt 11\/29\/2026/.test((f({nb:'2026-11-29',bn:2,lb:'2026-10-29'})[0]||{}).why),true);
   ck('  one failed invoice reads in the singular',/1 recent invoice failed or was credited/.test((f({bn:1,lb:'2026-11-12'})[0]||{}).why),true);
   ck('  $0 invoices after an old failed one explain it instead (Kimba)',/invoiced \$0 each month since 09\/04\/2026/.test((f({bn:1,lb:'2026-08-04',zn:3,fz:'2026-09-04',nb:'2026-12-04'})[0]||{}).why),true);
   ck('  nor when it bills $0 (a 100% discount)',/invoiced \$0 each month since 08\/28\/2026/.test((f({nb:'2026-11-28',zn:3,fz:'2026-08-28'})[0]||{}).why),true);
   ck('  paused in Recurly says so',/paused since 09\/30\/2026/.test((f({rn:0,ps:'2026-09-30'})[0]||{}).why),true);
   ck('  no live subscription says so',/no live subscription/.test((P._bdWfFlagsOf(stale,M,{today:'2026-11-24',byKey:{}}).find(z=>z.id==='active-not-billed')||{}).why),true);
   const fs=x=>P._bdWfFlagsOf(stale,M,rc(x));
   ck('  paused with a resume date (Uplife, resumes Dec 5): not "not invoiced", a low "restarts later" flag instead',(()=>{const z=fs({rn:0,ps:'2026-09-05',rs:'2026-12-05'});return [z.some(q=>q.id==='active-not-billed'),z.filter(q=>q.id==='scheduled').map(q=>[q.sev,/paused in Recurly \(since 09\/05\/2026\) and resumes 12\/05\/2026/.test(q.why)])];})(),[false,[['low',true]]]);
   ck('  a subscription that only starts next month is "restarts later" too',(()=>{const z=fs({fn:1,nb:'2026-12-03'});return [z.some(q=>q.id==='active-not-billed'),z.some(q=>q.id==='scheduled'&&/starts billing 12\/03\/2026/.test(q.why))];})(),[false,true]);
   ck('  but not when its recent invoices failed (Elysium)',(()=>{const z=fs({fn:1,nb:'2026-12-01',bn:1,lb:'2026-10-02'});return [z.some(q=>q.id==='active-not-billed'),z.some(q=>q.id==='scheduled')];})(),[true,false]);
   ck('  paused with no resume date stays flagged as paused',/paused since 09\/30\/2026/.test((f({rn:0,ps:'2026-09-30'})[0]||{}).why),true);
   ck('  Recurly status unavailable keeps the flag and says so',/could not be read/.test((P._bdWfFlagsOf(stale,M,{error:'x'}).find(z=>z.id==='active-not-billed')||{}).why),true);}
  ck('  1 month is not enough (invoice timing)',ids(BR({li:'2026-09',a:M.map((m,i)=>i<9?1000:0)})),[]);
  ck('invoiced after HubSpot’s churn month',ids(BR({st:'Churned',stage:'Churned',hsCh:'2026-08',li:'2026-10'})).includes('billed-after-churn'),true);
  ck('never paid, past payment terms; a 1-month active brand is not flagged',[ids(BR({paid:false,mi:3})).includes('unpaid'),ids(BR({paid:false,mi:1})).includes('unpaid')],[true,false]);
  ck('a 4+ month gap, then billing again',ids(BR({a:[1000,1000,0,0,0,0,1000,1000,1000,1000]})),['long-gap']);
  ck('a 2-month gap with no catch-up; a catch-up invoice is shown as a spike instead',[ids(BR({a:[1000,1000,1000,0,0,1000,1000,1000,1000,1000]})),ids(BR({a:[1000,1000,1000,0,0,3000,1000,1000,1000,1000]}))],[['skipped'],['spike']]);
  ck('a month about double its neighbours',ids(BR({a:[1000,1000,1000,1000,2000,1000,1000,1000,1000,1000]})),['double']);
  ck('a one-month spike far above the usual',ids(BR({a:[500,500,500,500,4000,500,500,500,500,500]})),['spike']);
  ck('HubSpot churn 4+ months after the last invoice',ids(BR({st:'Churned',stage:'Churned',li:'2026-03',hsCh:'2026-08',a:[1000,1000,1000,0,0,0,0,0,null,null]})),['churn-lag']);
  ck('active but HubSpot off-pipeline',ids(BR({stage:'No Show',st:'Active (inferred)'})),['status-conflict']);
  ck('churn type contradicts the widget',ids(BR({st:'Churned',stage:'Churned',ty:'Never Implemented',impl:true,li:'2026-10'})),['type-widget']);
  ck('paying 4+ months, never live',ids(BR({impl:false,mi:5,pe:0,pc:0})),['never-live']);
  ck('widget went dark on a 3+ product brand',ids(BR({pe:4,pc:0})),['gone-dark']);
  ck('flags come high severity first, each with the months it points at',(()=>{const f=P._bdWfFlagsOf(BR({a:[1000,1000,0,0,0,0,1000,1000,1000,0],li:'2026-09',paid:false,mi:7}),M);return [f.map(x=>x.sev),f.find(x=>x.id==='long-gap').cells];})(),[['high','high'],[2,3,4,5]]);
  // Rendered: flagged rows carry a hover card index and a badge, and stay well-formed.
  P.BD.wf.brands.find(b=>b.k==='1').paid=false;P.BD.wf._flagged=false;P.BD.wf.brands.forEach(b=>delete b._fl);
  let hf=R();
  ck('a flagged row gets a badge and a hover card',/<tr class="fl-high" data-ti="\d+"><td class="bd-wf-b"><span class="bd-wf-flag high">⚠<\/span>Stay Co/.test(hf)&&P._bdWfTips.some(t=>/Invoiced, never paid/.test(t)&&/Stay Co/.test(t)),true);
  ck('every opened attribute quote is closed in W1',[...hf.slice(hf.indexOf('class="bd-wf-t"')).matchAll(/<td[^>]*>/g)].filter(m=>(m[0].match(/"/g)||[]).length%2).length,0);
  ck('the hover card lives outside the re-rendered content',/<div class="main" id="bd-content"><\/div>\s*<!--[^>]*-->\s*<div id="bd-wf-tip"/.test(html),true);
  P.bdWfSetFlagF('unpaid');hf=R();
  ck('the Abnormalities filter narrows the table to that rule',[...hf.matchAll(/<td class="bd-wf-b"[^>]*>(?:<span class="bd-wf-flag[^>]*>[^<]*<\/span>)?([^<]+)<span/g)].map(m=>m[1]),['Stay Co']);
  P.bdWfSetFlagF('');
  // Billing Summary (Alex, 2026-11-24): renamed; last month's card on row 2 beside the category pie.
  P.BD.wf=d;h=R();
  ck('section W is Billing Summary, and its nav anchor too',/<span class="bd-sec-t">Billing Summary<\/span>/.test(h)&&/'bd-sec-wf0'\)">Billing Summary<\/a>/.test(html),true);
  ck('row 1 has no invoiced card; row 2 has it beside the pie',[/<div class="bd-wf-kpis">(?:(?!bd-wf-row2)[\s\S])*Invoiced in/.test(h),/<div class="bd-wf-row2"><div class="bd-wf-kpi bd-wf-inv"[^>]*><div class="bd-wf-kpi-l">Invoiced in Oct 26<\/div>/.test(h)],[false,true]);
  ck('the pie: plan split into monthly and prepaid, credits not a slice, one-offs typed',[/data-k="Plan \(monthly\)"/.test(h),/data-k="Plan \(prepaid multi-month\)"/.test(h),/data-k="Pilot initiation fees"/.test(h),/data-k="credits"/i.test(h),/before credits and refunds/.test(h)],[true,true,true,false,true]);
  ck('the invoiced card: charges as invoiced, every brand, after credits ($5,800 - $200)',/Invoiced in Oct 26<\/div><div class="bd-wf-kpi-v">\$5,600<\/div>/.test(h)&&/after credits and refunds · 4 brands billed/.test(h),true);
  ck('  with Recurring, One-off, Credits and Total lines',[/Recurring<em>incl\. \$1,000 prepaid multi-month<\/em><\/span><span class="bd-wf-inv-v">\$3,000<\/span>/.test(h),/One-off<em>[^<]*<\/em><\/span><span class="bd-wf-inv-v">\$2,800<\/span>/.test(h),/Credits &amp; refunds<\/span><span class="bd-wf-inv-v">−\$200<\/span>/.test(h),/Total<\/span><span class="bd-wf-inv-v">\$5,600<\/span>/.test(h)],[true,true,true,true]);
  ck('  and it names the table column it differs from',/The All Brands table’s Oct 26 column \(\$1,000\)/.test(h),true);
  ck('the pie’s centre rests on brands billed, not 0',/data-ctr="4" data-ctr-l="brands billed"/.test(h)&&/<div class="bd-bs-cn">4<\/div><div class="bd-bs-cl">brands billed<\/div>/.test(h),true);
  ck('  and bdBsOut restores that resting centre',/card\.hasAttribute\('data-ctr'\)\?card\.getAttribute\('data-ctr'\)/.test(html),true);
  ck('the legend is grouped under Recurring and One-off totals',/<div class="bd-wf-leg-g"><span>Recurring<\/span><b>\$3,000<\/b>/.test(h)&&/<div class="bd-wf-leg-g"><span>One-off<\/span><b>\$2,800<\/b>/.test(h),true);
  P.bdWfSetF('ty','Board Defined');h=R();
  ck('  filtered: the card still covers every brand, and says so',/Not narrowed by the filters\./.test(h)&&/\$5,600<\/div>/.test(h),true);P.bdWfReset();
  P.BD.wf=Object.assign({},d,{categories:{error:'boom'}});h=R();
  ck('  a failed category query leaves the tab working, with a note',/The category breakdown did not load \(boom\)/.test(h)&&/id="bd-sec-wf2"/.test(h),true);
  // W2 flagged brands (Alex, 2026-11-24)
  {const prev=P.BD.wf;
   P.BD.wf={months:M,meta:{},brands:[BR({k:'a1',n:'Stale Co',li:'2026-08',a:[1000,1000,1000,1000,1000,1000,1000,1000,0,0]}),
     BR({k:'b1',n:'Double Co',fm:'2026-02',a:[null,1000,1000,1000,2000,1000,1000,1000,1000,1000]}),
     BR({k:'c1',n:'Fine Co'})]};
   P._bdWfW2T={open:true,co:'',fl:''};P.bdWfSet('from','all');
   let w=R();const sec=w.slice(w.indexOf('class="bd-c2nt bd-wf-w2f"'));
   const names=x=>[...x.matchAll(/<tr data-ti="\d+"><td><b>([^<]+)<\/b>/g)].map(m=>m[1]);
   ck('W2 lists its flagged brands under the grid, high severity first',[/Flagged brands in this grid · <b>2<\/b> of 3 brands carry a possible abnormality; <b>1<\/b> of them can move the retention numbers/.test(sec),names(sec)],[true,['Stale Co','Double Co']]);
   ck('  the flag picker counts each type and marks the ones that move the grid',/<option value="distort">Can move retention \(1\)<\/option>/.test(sec)&&/<option value="active-not-billed">Active but not invoiced ● \(1\)<\/option>/.test(sec)&&/<option value="double">Possible double billing \(1\)<\/option>/.test(sec),true);
   ck('  each row says what it does to the grid, or that it does nothing',/Kept as retained although billing stopped/.test(sec)&&/None on the numbers/.test(sec),true);
   P.bdWfW2Set('fl','distort');w=R();
   ck('  flag filter: only brands that can move retention',names(w.slice(w.indexOf('bd-wf-w2f'))),['Stale Co']);
   P.bdWfW2Set('fl','');P.bdWfW2Set('co','2026-02');w=R();const s2=w.slice(w.indexOf('bd-wf-w2f'));
   ck('  cohort filter: only that first-invoice month, and the cohort picker counts by month',[names(s2),/<option value="2026-02" selected>Feb 26 \(1\)<\/option>/.test(s2),/<option value="2026-01">Jan 26 \(1\)<\/option>/.test(s2)],[['Double Co'],true,true]);
   ck('  hovering a row opens its explanation card',P._bdWfTips.some(t=>/Double Co/.test(t)&&/Possible double billing/.test(t)),true);
   P._bdWfW2T={open:false,co:'',fl:''};P.BD.wf=prev;}
  // Product multi-selects + deal link (Alex, 2026-11-24): the card's four products.
  {const prev=P.BD.wf;
   P.BD.wf=Object.assign({},d,{widgets:{from:'2025-12',byKey:{'1':{reviews:['2026-07','2026-10'],cc:['2026-08']},'2':{cc:['2026-08']},'5':{reviews:['2026-09']}}}});
   const set=(k,o)=>Object.assign(P.BD.wf.brands.find(b=>b.k===k),o);
   set('1',{cc:2,rv:1,ca:0,cai:0});set('2',{cc:1,rv:0,ca:0,cai:0});set('5',{cc:0,rv:3,ca:0,cai:0});set('3',{cc:0,rv:0,ca:0,cai:0});set('4',{cc:null,rv:null,ca:null,cai:null});
   P.BD.wf.brands.forEach(b=>delete b._fl);P.BD.wf._flagged=false;P.bdWfReset();
   const names=x=>[...x.matchAll(/<td class="bd-wf-b"[^>]*>(?:<span class="bd-wf-flag[^>]*>[^<]*<\/span>)?([^<]+)<span/g)].map(m=>m[1]).sort();
   let x=R();
   ck('the product filters list the card’s four products by name, with counts',/<summary>Products Ever Live<b>All<\/b><\/summary>/.test(x)&&/onchange="bdWfProdToggle\('ever','reviews'\)"> Clinician Reviews<span>2<\/span>/.test(x)&&/onchange="bdWfProdToggle\('ever','cc'\)"> Clinicians’ Choice<span>2<\/span>/.test(x)&&/onchange="bdWfProdToggle\('churn','cai'\)"> ClinicianAI<span>0<\/span>/.test(x),true);
   P.bdWfProdToggle('ever','reviews');x=R();
   ck('  Ever Live: Clinician Reviews keeps brands whose REVIEWS_PRODUCTS > 0',names(x),['Paused Co','Stay Co']);
   P.bdWfProdToggle('ever','cc');x=R();
   ck('  ticking Clinicians’ Choice too is an intersection (both)',[names(x),/<summary>Products Ever Live<b>Clinician Reviews \+ Clinicians’ Choice<\/b>/.test(x)],[['Stay Co'],true]);
   P.bdWfReset();P.bdWfProdToggle('churn','reviews');x=R();
   ck('  Live at Churn: live in the churn month (Paused Co: Sep), or the last month if active (Stay Co: Oct)',names(x),['Paused Co','Stay Co']);
   P.bdWfReset();x=R();
   P.bdWfSet('cols','all');x=R();
   ck('All Brands carries every card column, then the months',['HubSpot Stage','Source','Group','Last Invoiced','Metabase Churn Month','Churn Type','Company Segment','A/B Test','Ever Implemented','Ever Paid','Web Tier at Churn','Self-selected Traffic','Implied Traffic','Measured Traffic','Clinicians’ Choice','Clinician Reviews','Product Analysis','ClinicianAI','Products Ever Live','Products Live at Churn','Package','Pilot Status'].every(h=>x.indexOf('>'+h+'</th>')>0),true);
   ck('  and a filter for each categorical column',['Source','Group','Churn Status','HubSpot Stage','Churn Source','Web Tier Source','Measured Traffic','Pilot Status'].every(l=>x.indexOf('<label>'+l+'<select')>=0),true);
   {const rows=(x.match(/<div class="bd-wf-frows">[\s\S]*?<\/div><\/div>(?=<\/section>)/)||[''])[0].split('<div class="bd-wf-bar">').slice(1).map(r=>(r.match(/<label>([^<]+)<select/g)||[]).map(m=>m.slice(7,-7)).concat(/bd-wf-ms/.test(r)?['(products)']:[]));
    ck('filters sit in four rows in Alex\u2019s order (2026-11-24)',rows,[['Possible Abnormalities'],['Company Segment','Source','Group','Churn Status','HubSpot Stage','Recurly State','Churn Source','Churn Type','Ever Implemented','Ever Paid'],['Web Traffic Tier','Web Tier Source','Implied Traffic','Self-selected Traffic','Web Tier at Churn','Measured Traffic'],['Package','A/B Test','Pilot Status','(products)']]);}
   {const f=P.BD_WF_FILTERS.find(z=>z.k==='wtt').of;
    ck('Web Traffic Tier: Implied, then Self-Selected, then Measured, then Web Tier at Churn',[f({it:'10K-25K',ss:'500K+',wt:'200K+',mt:'1M+'}),f({it:'No traffic data',ss:'500K+',wt:'200K+'}),f({it:'',ss:'Not selected',wt:'50-200K',mt:'1M+'}),f({it:'No traffic data',ss:'Not selected',wt:'50-200K',mt:'No traffic data'}),f({it:'No traffic data',ss:'Not selected',wt:'Not recorded',mt:'No traffic data'}),f({})],['10K-25K','500K+','1M+','50-200K','No traffic data','No traffic data']);}
   P.bdWfSet('cols','default');x=R();
   ck('  collapsed by default: only the seven, the extras behind "+ more"',[x.indexOf('>Measured Traffic</th>')<0,x.indexOf('>Company Segment</th>')>0,/>\+ 25 more<\/th>/.test(x)],[true,true,true]);
   P.bdWfReset();
   P._bdWfJoin={byBrand:{'1':[{id:'777',dealname:'Stay deal',dealstage:'closedwon',brand_status:'Paying',closedate:'2026-07-01'},{id:'778',dealstage:'appointmentscheduled',closedate:'2026-01-01'}]}};
   x=P._bdWfRender();
   ck('W1 rows link to the brand’s HubSpot deal (closed-won preferred)',/Stay Co<span class="bd-wf-k">1 <a class="bd-wf-dl" href="https:\/\/app\.hubspot\.com\/contacts\/46815331\/record\/0-3\/777"/.test(x),true);
   ck('  a brand with no joined deal has no link',/Gone Co<span class="bd-wf-k">2<\/span>/.test(x),true);
   P._bdWfJoin=null;P.BD.wf=prev;}
  // This month so far + brands the card leaves out (Alex, 2026-11-24)
  {const W=JSON.parse(JSON.stringify(Object.assign({},d,{current:dCur})));W.brands.forEach(b=>{delete b._fl;delete b.cur;});P.BD.wf=W;P.bdWfReset();P._bdWfNI='incl';
   let x=R();const all=x;
   ck('the card’s brands plus the ones it leaves out: 5 + 2 = 7',W.brands.length,7);
   ck('  a brand first invoiced this month is added, marked new, active',(()=>{const b=W.brands.find(z=>z.k==='901');return [b.x,b.st,b.fm,b.cur];})(),['new','Active','2026-11',3000]);
   ck('  a brand whose every invoice was credited is added and flagged',(()=>{const b=W.brands.find(z=>z.k==='902');return [b.x,b.fm,b.st,b._fl.map(f=>f.id)];})(),['credited','2026-03','Churned (inferred)',['credited-only']]);
   ck('W1 has a "Nov 26 so far" column with its totals',/>Nov 26 so far<\/th>/.test(x)&&/<td class="m cur">2<\/td>/.test(x)&&/<td class="m cur">4,000<\/td>/.test(x),true);
   const rowOf2=n=>{let i=x.indexOf('">'+n);if(i<0)return '';i=x.lastIndexOf('<tr',i);return x.slice(i,x.indexOf('</tr>',i));};
   ck('  the new brand’s row: badge, blank months, this month’s amount',/New this month<\/span>/.test(rowOf2('Fresh Pilot'))&&/<td class="m cur">3,000<\/td>/.test(rowOf2('Fresh Pilot')),true);
   ck('  the KPI counts the new brands',/every brand ever invoiced · 1 new in Nov 26/.test(x),true);
   P.bdWfSortBy('m:cur');x=R();
   ck('  sortable: largest this month first',[...x.matchAll(/<td class="bd-wf-b"[^>]*>(?:<span class="bd-wf-flag[^>]*>[^<]*<\/span>)?([^<]+)<span/g)].map(m=>m[1])[0],'Fresh Pilot');
   P.bdWfSet('sort','first');x=R();
   const g2=x.slice(x.indexOf('id="bd-sec-wf2"'));
   ck('W2 leaves the added rows out (complete months only)',[!/<td class="rw-c">Mar 26<\/td>/.test(g2),!/<td class="rw-c">Nov 26<\/td>/.test(g2),/<td class="rw-c">Jul 26<\/td><td class="rw-n">2<\/td>/.test(g2)],[true,true,true]);
   P._bdWfNI='excl';}
  {const prev=P.BD.wf;P.BD.wf=Object.assign({},d,{stale:{error:'Metabase did not answer in time (Metabase /api/dataset 524)',asOf:'2026-11-24T19:22:00.000Z'}});
   const x=R();ck('a stale copy is shown with a note saying from when, and why',/Showing the last good copy, from 2026-11-24 19:22 UTC\.<\/strong> <span>Metabase did not answer in time \(Metabase \/api\/dataset 524\)\. Refresh to try again\./.test(x)&&/id="bd-sec-wf1"/.test(x),true);
   P.BD.wf=prev;}
  // Recurly state (live) + Never Implemented excludes never-live brands (Alex, 2026-11-24)
  {const prev=P.BD.wf,prevApp=P.BD.app;
   ck('W2 Never Implemented now covers any brand never live, active too (Vitaliv)',[P._bdWfIsNI({st:'Active (inferred)',impl:false,ty:'Not recorded'}),P._bdWfIsNI({st:'Active',impl:true,ty:'Not recorded'}),P._bdWfIsNI({st:'Churned',impl:true,ty:'Never Implemented'}),P._bdWfIsNI({st:'Churned',impl:false,ty:'Board Defined'})],[true,false,true,true]);
   const stale=BR({k:'s1',li:'2026-08',a:[1000,1000,1000,1000,1000,1000,1000,1000,0,0]});
   P.BD.app={brands:[{id:'s1',recurly:{subs:[{state:'paused',pausedAt:'2026-09-25',resumeAt:'2026-12-25',pauseCycles:1}]}},{id:'s2',recurly:{subs:[{state:'expired'},{state:'active'}]}},{id:'s3',recurly:{subs:[]}}]};
   const fl=P._bdWfFlagsOf(stale,M,{today:'2026-11-24',byKey:{s1:{ln:1,rn:0,ca:0,bn:0,zn:0,ps:'2026-09-25',rs:'2026-11-25'}}});
   ck('Recurly state paused (The Plug Drink): out of "Active but not invoiced", into the low paused flag with Recurly’s dates',[fl.some(f=>f.id==='active-not-billed'),fl.filter(f=>f.id==='scheduled').map(f=>/Recurly state is paused \(since 09\/25\/2026\), resuming 12\/25\/2026 with 1 billing cycle left/.test(f.why))],[false,[true]]);
   ck('  the brand’s Recurly state: the live subscription first',[P._bdWfRcState({k:'s1'}),P._bdWfRcState({k:'s2'}),P._bdWfRcState({k:'s3'}),P._bdWfRcState({k:'zz'})],['paused','active','No Recurly subscription','Not linked']);
   P.BD.wf=Object.assign({},d);P.BD.wf.brands=d.brands.map(b=>Object.assign({},b));P.BD.wf.brands[0].k='s1';P.bdWfReset();P.bdWfSet('cols','all');
   let x=R();
   ck('  Recurly State, Paused At, Remaining Pause Cycles and Resume At are columns',['Recurly State','Paused At','Remaining Pause Cycles','Resume At'].every(h=>x.indexOf('>'+h+'</th>')>0)&&/<span class="bd-wf-rcs paused">paused<\/span><\/td><td class="bd-wf-x">2026-09-25<\/td><td class="m bd-wf-x">1<\/td><td class="bd-wf-x">2026-12-25<\/td>/.test(x),true);
   ck('  and Recurly State is a filter',/<label>Recurly State<select[^>]*><option value="">All \(5\)<\/option>[\s\S]*?<option value="paused">paused \(1\)<\/option>/.test(x),true);
   P.bdWfSet('cols','default');P.BD.wf=prev;P.BD.app=prevApp;}
  P.BD.wf={error:'Metabase /api/card/3565/query 403: You don\'t have permissions to do that.'};h=R();
  ck('a Metabase failure is an error band, never an empty grid',/Billing data unavailable/.test(h)&&/collection must be visible/.test(h)&&!/bd-sec-wf2/.test(h),true);
  ck('W2\u2019s first column matches .rwtbl\u2019s sticky offset',[P.BD_WF_W_LBL,+((html.match(/\.rwtbl \.rw-n\{[^}]*left:(\d+)px/)||[])[1])],[78,78]);
  ck('Refresh re-runs the card once the tab has been opened',/if\(BD\.wf\)bdWfFetch\(true\);/.test(html),true);
  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})().catch(e=>{console.error(e);process.exit(1);});
