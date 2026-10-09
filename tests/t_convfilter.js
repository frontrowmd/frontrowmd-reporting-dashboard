// Conversion dropdown on the ad-table filter set. The trap this guards is that
// the three tables do NOT share a vocabulary: only AD SETS carry the real
// optimization target (optTarget), a CAMPAIGN's is its ad sets' joined with
// ' · ', and a CREATIVE has only the legacy capiConv heuristic — which spells
// the same conversion differently ("CAPI · Demo + Webinar" against optTarget's
// "CAPI -- Demo + Webinar") and contains a ' · ' of its own.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console};vm.createContext(ctx);
vm.runInContext([
  'var _irfanMetaD=null;',
  'function _curAdCh(){return "meta";}',
  decl(cli,'let _adsConvByAdset=','};').replace(/\}$/,'}') ,
  fn(cli,'_adsConvIndex'), fn(cli,'_adsConvList'), fn(cli,'_adsConvMatch'), fn(cli,'_adsConvOptions'),
  'Object.assign(globalThis,{_adsConvIndex,_adsConvList,_adsConvMatch,_adsConvOptions,'
   +'setData(d){_irfanMetaD=d;}});',
].join('\n'),ctx);
ck('the helpers loaded',typeof ctx._adsConvList,'function');

// Live shapes, as the worker ships them.
const ADSETS=[
  {name:'TOF - 2% Lookalikes - WV + Propensity - CAPI Demo + Webinar',optTarget:'CAPI -- Demo + Webinar',spend:830},
  {name:'TOF - Cold - Skincare & Clean Beauty Brands',optTarget:'Schedule',spend:102},
  {name:'TOF - Clinicians - Job Titles',optTarget:'virtualclinicians',spend:628},
  {name:'BOF - Hot - Website Retargeting',optTarget:'Submit Application',spend:7},
];
// A campaign spanning two targets — the worker joins them with ' · '.
const CAMPS=[
  {name:'S01 - TOF - Cold - Skincare & Beauty Brands - CAPI',optTarget:'Schedule · CAPI -- Demo + Webinar',spend:661},
  {name:'S03 - BOF - Hot - Website Retargeting - CAPI',optTarget:'CAPI -- Demo + Webinar',spend:1179},
  {name:'Legacy campaign with no target',optTarget:'',spend:50},
];
ctx._adsConvIndex({audiences:ADSETS});

// ── 1. Splitting ──────────────────────────────────────────────────────────
ck('a single target is itself',ctx._adsConvList(ADSETS[0]),['CAPI -- Demo + Webinar']);
ck('a mixed campaign splits into its targets',ctx._adsConvList(CAMPS[0]),
   ['Schedule','CAPI -- Demo + Webinar']);
// THE bug this nearly shipped with: splitting on a bare '·' shreds the legacy
// label, which is a single conversion name that happens to contain one.
ck('the legacy capiConv label stays atomic',
   ctx._adsConvList({capiConv:'CAPI · Demo + Webinar'}),['CAPI · Demo + Webinar']);
ck('  and is not confused with the optTarget spelling',
   ctx._adsConvList({capiConv:'CAPI · Demo + Webinar'})[0]==='CAPI -- Demo + Webinar',false);
ck('nothing recorded yields nothing',[ctx._adsConvList({}),ctx._adsConvList(null),ctx._adsConvList({optTarget:''})],[[],[],[]]);

// ── 2. Creatives inherit their ad set ─────────────────────────────────────
// A creative carries no optTarget at all. Left on capiConv it would never match
// an optTarget value and the table would empty out.
const CRE={name:'ad-1',adsetName:'TOF - Clinicians - Job Titles',capiConv:''};
ck('a creative resolves through its ad set',ctx._adsConvList(CRE),['virtualclinicians']);
ck('  case-insensitively',
   ctx._adsConvList({adsetName:'tof - clinicians - JOB TITLES'}),['virtualclinicians']);
ck('  falling back to capiConv when the ad set is unknown',
   ctx._adsConvList({adsetName:'no such ad set',capiConv:'Lead Forms'}),['Lead Forms']);
ck('  and an own optTarget still wins',
   ctx._adsConvList({adsetName:'TOF - Clinicians - Job Titles',optTarget:'Schedule'}),['Schedule']);

// ── 3. Matching ───────────────────────────────────────────────────────────
ck('All matches everything',
   [CAMPS[0],ADSETS[1],{}].map(o=>ctx._adsConvMatch(o,'all')),[true,true,true]);
ck('  as does an unset filter',ctx._adsConvMatch(ADSETS[0],''),true);
ck('a mixed campaign matches EITHER of its targets',
   [ctx._adsConvMatch(CAMPS[0],'Schedule'),ctx._adsConvMatch(CAMPS[0],'CAPI -- Demo + Webinar')],[true,true]);
ck('  and not one it does not carry',ctx._adsConvMatch(CAMPS[0],'virtualclinicians'),false);
ck('the none bucket matches only unrecorded rows',
   [ctx._adsConvMatch(CAMPS[2],'__none'),ctx._adsConvMatch(ADSETS[0],'__none')],[true,false]);

// ── 4. Options come from the data ─────────────────────────────────────────
ctx.setData({channels:{meta:{campaigns:CAMPS,audiences:ADSETS,creatives:[CRE]}}});
const opts=ctx._adsConvOptions();
ck('All leads the list',opts[0],['all','All']);
ck('every target in the data is offered, once, sorted',
   opts.slice(1).filter(o=>o[0]!=='__none').map(o=>o[0]),
   ['CAPI -- Demo + Webinar','Schedule','Submit Application','virtualclinicians']);
ck('  with a bucket for rows carrying nothing',
   opts[opts.length-1],['__none','(none recorded)']);
// A channel where every row has a target must not offer an empty bucket.
ctx.setData({channels:{meta:{campaigns:[CAMPS[1]],audiences:ADSETS}}});
ck('no none bucket when every spending row has a target',
   ctx._adsConvOptions().some(o=>o[0]==='__none'),false);
// A zero-spend row with no target is not worth a bucket of its own.
ctx.setData({channels:{meta:{campaigns:[{name:'x',optTarget:'',spend:0}],audiences:ADSETS}}});
ck('  nor for a row that never spent',
   ctx._adsConvOptions().some(o=>o[0]==='__none'),false);
ctx.setData({channels:{meta:{campaigns:[],audiences:[]}}});
ck('an empty channel offers only All',ctx._adsConvOptions(),[['all','All']]);
ctx.setData(null);
ck('  and no payload does not throw',ctx._adsConvOptions(),[['all','All']]);

// ── 5. Wiring ─────────────────────────────────────────────────────────────
ck('the filter is registered with the shared set',
   /\['conv','Conversion',_adsConvOptions,\['metaCampConvF','metaCreConvF','metaAdsetConvF'\],'select'\]/.test(cli),true);
// Clear-all and the "any filter on" check both read ADS_TBL_FILTERS, so the
// new filter joins them with no extra wiring — assert that stays true.
ck('  so Clear All resets it too',
   /for\(const f of ADS_TBL_FILTERS\)for\(const key of f\[3\]\)S\.irfan\[key\]='all';/.test(cli),true);
ck('  and it counts toward the Clear All button appearing',
   /ADS_TBL_FILTERS\.some\(function\(f\)\{return \(S\.irfan\[f\[3\]\[0\]\]\|\|'all'\)!=='all';\}\)/.test(cli),true);
ck('it renders as a dropdown, not pills',/if\(F\[4\]==='select'\)\{/.test(cli),true);
ck('  built from the function when options are dynamic',
   /var _opts=\(typeof F\[2\]==='function'\)\?F\[2\]\(\):F\[2\];/.test(cli),true);
ck('  and option text is escaped',/_adsEsc\(o\[1\]\)/.test(cli),true);
ck('  with the control hidden when there is nothing to choose',
   /if\(_opts\.length<=1\)\{ h\+=row\(F\[1\],'<span[^']*none recorded for this channel/.test(cli),true);
// All three tables must honour it or the filter lies about what it filtered.
ck('all three tables apply it',
   [/if\(convF!=='all'\)rows=rows\.filter\(c=>_adsConvMatch\(c,convF\)\)/.test(cli),
    /if\(creConvF!=='all'\)creRows=creRows\.filter\(c=>_adsConvMatch\(c,creConvF\)\)/.test(cli),
    /if\(campConvF!=='all'\)campRows=campRows\.filter\(c=>_adsConvMatch\(c,campConvF\)\)/.test(cli)],
   [true,true,true]);
ck('the ad-set index is rebuilt per render',
   /_adsConvIndex\(meta\);/.test(cli),true);
ck('every table defaults to All',
   ['metaCampConvF','metaCreConvF','metaAdsetConvF']
     .map(k=>new RegExp("if\\(!S\\.irfan\\."+k+"\\)S\\.irfan\\."+k+"='all';").test(cli)),
   [true,true,true]);

// ── 6. Every helper the filter bar calls must be REACHABLE ────────────────
// This file has two _esc helpers and BOTH are declared inside other functions.
// Calling one from the filter bar threw "_esc is not defined" at render time
// and took every ad table down with it — and the regex assertion above still
// passed, because the name existed somewhere in the file. Existence is not
// scope, so check scope.
function _stripLiterals(src){
  let o='',i=0;const n=src.length;
  while(i<n){
    const c=src[i];
    if(c==='/'&&src[i+1]==='/'){while(i<n&&src[i]!=='\n')i++;continue;}
    if(c==="'"||c==='"'||c==='`'){const q=c;i++;while(i<n&&src[i]!==q){if(src[i]==='\\')i++;i++;}i++;o+=' ';continue;}
    o+=c;i++;
  }
  return o;
}
// Declared at column 0 = module scope. Anything indented lives inside something.
const MODULE_SCOPE=new Set([...cli.matchAll(/^(?:function\s+([A-Za-z_$][\w$]*)|(?:const|let|var)\s+([A-Za-z_$][\w$]*))/gm)]
  .map(m=>m[1]||m[2]));
ck('the ad-table escaper is at module scope',MODULE_SCOPE.has('_adsEsc'),true);
// The two that are not, and must not be mistaken for it.
ck('  and the nested _esc helpers are NOT',MODULE_SCOPE.has('_esc'),false);
ck('the filter bar uses the reachable one',/_adsEsc\(o\[0\]\)/.test(cli)&&/_adsEsc\(o\[1\]\)/.test(cli),true);
ck('  and no longer the out-of-scope one',/[^s]_esc\(o\[/.test(cli),false);

const KW=new Set(['if','for','while','switch','catch','return','typeof','function','new','else','do','of','in','await']);
const GLOBALS=new Set(['String','Number','Math','Date','JSON','Object','Array','parseInt','parseFloat','isNaN','document','window','encodeURIComponent','Boolean']);
function unresolvedCallsIn(name){
  const body=_stripLiterals(fn(cli,name));
  const local=new Set([...body.matchAll(/(?:function|const|let|var)\s+([A-Za-z_$][\w$]*)/g)].map(m=>m[1]));
  return [...new Set([...body.matchAll(/(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(/g)].map(m=>m[1]))]
    .filter(n=>!KW.has(n)&&!GLOBALS.has(n)&&!local.has(n)&&!MODULE_SCOPE.has(n));
}
ck('every helper the filter bar calls resolves',unresolvedCallsIn('_metaFilterBar'),[]);
// The conversion helpers are called from three table builders, so they have to
// be module scope too.
ck('the conversion helpers are module scope',
   ['_adsConvIndex','_adsConvList','_adsConvMatch','_adsConvOptions'].filter(n=>!MODULE_SCOPE.has(n)),[]);
ck('  and their own calls resolve',
   ['_adsConvList','_adsConvMatch','_adsConvOptions'].flatMap(n=>unresolvedCallsIn(n)),[]);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
