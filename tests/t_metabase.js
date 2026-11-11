// Metabase client + connection check (2026-11-11), run against a FAKE Metabase.
// The key is a secret: it must be sent as x-api-key and never appear in output.
const {load}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{mbQuery,mbFetch,metabaseCheck,mbBaseName,MB_NEEDED_TABLES,METABASE_URL,MB_DB});',W);

const KEY='mb_TEST_not_a_real_key';
const env={METABASE_API_KEY:KEY};
const ok=(j,status)=>({ok:true,status:status||200,json:async()=>j,text:async()=>JSON.stringify(j)});
const bad=(status,t)=>({ok:false,status,json:async()=>({}),text:async()=>t||''});
let calls=[];
// A Metabase with every needed table but SAMPLE_BOXES, in schema ANALYTICS.
const allTables=Object.keys(W.MB_NEEDED_TABLES).filter(b=>b!=='SAMPLE_BOXES')
  .map(b=>({name:(b==='PILOT_PROGRESS'?'FCT_':'STG_')+b,schema:'ANALYTICS',active:true}));
function fakeMB(o){
  o=o||{};
  W.fetch=async(url,init)=>{
    calls.push({url,init});
    if(url.endsWith('/api/user/current'))return ok({is_superuser:!!o.admin,email:'someone@example.com'});
    if(/\/api\/database\/67\?include=tables$/.test(url))return ok({name:'Snowflake',engine:'snowflake',tables:o.tables||allTables});
    if(url.endsWith('/api/dataset')){
      if(o.queryFails)return ok({status:'failed',error:'SQL compilation error'},202);
      return ok({status:'completed',data:{cols:[{name:'N'}],rows:o.rows||[[5161]]}});
    }
    return bad(404);
  };
}

(async()=>{
  // ── 1. The client ──
  fakeMB();
  const rows=await W.mbQuery(env,'SELECT 1');
  ck('a query returns row objects keyed by column',JSON.parse(JSON.stringify(rows)),[{N:5161}]);
  const q=calls[calls.length-1];
  ck('  sent to the Metabase dataset API',q.url,W.METABASE_URL+'/api/dataset');
  ck('  with the key as x-api-key',q.init.headers['x-api-key'],KEY);
  ck('  as native SQL on the Snowflake database',(b=>[b.database,b.type,b.native.query])(JSON.parse(q.init.body)),[67,'native','SELECT 1']);
  fakeMB({queryFails:true});
  let e1=''; try{await W.mbQuery(env,'SELECT nope');}catch(e){e1=e.message;}
  ck('a failed query (HTTP 202, status failed) throws, never an empty result',/query failed: SQL compilation error/.test(e1),true);
  fakeMB({rows:Array.from({length:2000},(_, i)=>[i])});
  let e2=''; try{await W.mbQuery(env,'SELECT *');}catch(e){e2=e.message;}
  ck('a result cut at the 2000-row cap throws rather than undercount',/row cap/.test(e2),true);
  let e3=''; try{await W.mbQuery({},'SELECT 1');}catch(e){e3=e.message;}
  ck('no key set: a clear error',e3,'METABASE_API_KEY is not set');
  W.fetch=async()=>bad(401,'Unauthenticated');
  let e4=''; try{await W.mbFetch(env,'/api/user/current');}catch(e){e4=e.message;}
  ck('a refused key: status in the error, key not',[/401/.test(e4),e4.indexOf(KEY)<0],[true,true]);
  ck('dbt prefixes are ignored when matching tables',[W.mbBaseName('stg_notes'),W.mbBaseName('FCT_PILOT_PROGRESS'),W.mbBaseName('STG_HEALTH_BRAND_PRODUCTS_PRODUCT_SUBTYPES')],['NOTES','PILOT_PROGRESS','HEALTH_BRAND_PRODUCTS_PRODUCT_SUBTYPES']);

  // ── 2. The connection check ──
  fakeMB();
  const c=JSON.parse(JSON.stringify(await W.metabaseCheck(env)));
  ck('check: a view-only key that can query is ok',[c.ok,c.keyIsAdmin,c.errors],[true,false,[]]);
  ck('  reports the database',[c.databaseName,c.engine],['Snowflake','snowflake']);
  ck('  lists the missing table and what it feeds',c.missing,{SAMPLE_BOXES:'samples shipped'});
  ck('  PRODUCT_SUBTYPES is not mistaken for the join table',c.present.PRODUCT_SUBTYPES,'ANALYTICS.STG_PRODUCT_SUBTYPES');
  ck('  probes a schema-qualified table',c.probe,{table:'ANALYTICS.STG_NOTES',rows:5161});
  const txt=JSON.stringify(c);
  ck('  the output carries neither the key nor the key owner\'s email',[txt.indexOf(KEY)<0,txt.indexOf('@')<0],[true,true]);
  fakeMB({admin:true});
  const a=JSON.parse(JSON.stringify(await W.metabaseCheck(env)));
  ck('an ADMIN key is flagged and not ok',[a.ok,a.keyIsAdmin,/ADMIN/.test(a.errors[0])],[false,true,true]);
  fakeMB({queryFails:true});
  const f=JSON.parse(JSON.stringify(await W.metabaseCheck(env)));
  ck('a key that cannot query is not ok',[f.ok,f.errors.length],[false,1]);

  // ── 3. Route ──
  ck('the check route is sign-in-guarded',/url\.pathname === '\/api\/metabase-check'\) \{\s*if \(!\(await authed\(\)\)\)/.test(src),true);
  ck('the key is read from the secret, never written into the source',[/env && env\.METABASE_API_KEY/.test(src),/mb_[A-Za-z0-9+/=]{20,}/.test(src)],[true,false]);

  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
