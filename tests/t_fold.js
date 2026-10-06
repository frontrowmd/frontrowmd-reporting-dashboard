// Creative rows are folded per CHUNK and dropped, instead of every row in the
// window being held at once — a YTD Meta fetch is 73k+ rows (creative x day x
// placement) and holding them exceeded the Worker's 128MB limit, which surfaces
// as a bare 503 with no JSON body. The fold must be associative: how the window
// is chunked must not change the result.
const {load}=require('./slice.js');const vm=require('vm');
const src=load('worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g).slice(0,150)+(ok?'':' want='+JSON.stringify(w).slice(0,150)));ok?pass++:fail++;};

// Everything above `export default` is plain top-level declarations, so running
// that whole prefix resolves every dependency at once rather than one at a time.
const prefix=src.slice(0,src.indexOf('\nexport default'));
const ctx={console};vm.createContext(ctx);
vm.runInContext(prefix+'\nObject.assign(globalThis,{_foldCreRows,_foldAudRows,AZ_CONNECTORS,azWindsorFetchAll});',ctx);
ck('the real fold function loaded',typeof ctx._foldCreRows,'function');
ck('and it is the one the fetcher calls',/_foldCreRows\(ch, cfg, rows, map/.test(src),true);

const AZC=ctx.AZ_CONNECTORS.meta;
// Meta rows multiply by creative x day x placement — the shape that makes a year
// 73k rows.
const rows=[];
const plats=[['facebook','feed'],['instagram','story'],['audience_network','rewarded']];
for(let d=1;d<=9;d++)for(let c=1;c<=4;c++)for(const [pf,ps] of plats){
  rows.push({date:'2026-03-'+String(d).padStart(2,'0'),campaign_name:'C'+(c%2),ad_name:'ad'+c,
    adset_name:'as'+(c%2),spend:String(c*1.5),clicks:String(c*2),impressions:String(c*100),
    frequency:String(1+c/10),publisher_platform:pf,platform_position:ps,
    conversions_submit_application_total:String(c%3),effective_status:d===9?'ACTIVE':'PAUSED'});
}
const run=(chunks)=>{
  const map={},camp={},adset={},plc={};
  for(const ch of chunks) ctx._foldCreRows('meta',AZC,ch,map,camp,adset,plc,{});
  return {map,camp,adset,plc};
};

const whole=run([rows]);
ck('the fixture produced several creatives',Object.keys(whole.map).length,4);
ck('and several placements',Object.keys(whole.plc).length,plats.length);

for(const size of [1,2,5,13,rows.length-1]){
  const chunks=[];for(let i=0;i<rows.length;i+=size)chunks.push(rows.slice(i,i+size));
  const split=run(chunks);
  ck('chunk size '+size+': identical creative aggregate',split.map,whole.map);
  ck('chunk size '+size+': identical per-campaign aggregate',split.camp,whole.camp);
  ck('chunk size '+size+': identical per-adset aggregate',split.adset,whole.adset);
  ck('chunk size '+size+': identical placement aggregate',split.plc,whole.plc);
}
// The chunker pops off a stack, so chunks arrive OUT of order.
{
  const chunks=[];for(let i=0;i<rows.length;i+=4)chunks.push(rows.slice(i,i+4));
  const rev=run(chunks.slice().reverse());
  ck('reversed chunk order gives the same spend per creative',
     Object.keys(rev.map).sort().map(k=>rev.map[k].spend),
     Object.keys(whole.map).sort().map(k=>whole.map[k].spend));
  ck('and the same placement totals',
     Object.keys(rev.plc).sort().map(k=>rev.plc[k].spend),
     Object.keys(whole.plc).sort().map(k=>whole.plc[k].spend));
}
ck('spend survives folding',
   Object.values(whole.map).reduce((a,c)=>a+c.spend,0),
   rows.reduce((a,r)=>a+parseFloat(r.spend),0));
ck('so do impressions',
   Object.values(whole.map).reduce((a,c)=>a+c.impressions,0),
   rows.reduce((a,r)=>a+parseInt(r.impressions),0));

// The sink is what stops rows accumulating; without it memory is unbounded.
ck('azWindsorFetchAll takes a per-chunk sink',/onChunk = null\)/.test(src),true);
ck('and does NOT accumulate when one is given',
   /\} else if \(onChunk\) \{[\s\S]{0,220}onChunk\(rows\);/.test(src),true);
ck('the per-connector creative fetches pass a sink',
   (src.match(/AZ_CREATIVE_CHUNK_DAYS, rows => \{ for \(const _c of _seenConn/g)||[]).length,3);
// LinkedIn was the last unchunked creative fetch: a whole-window azWindsorFetch
// with retries, no budget guard, and no azChunk log — invisible, and ~240s of a
// 379s creatives phase.
ck('no creative fetch uses the unchunked call any more',
   /promises\.push\(azWindsorFetch\(apiKey, 'linkedin'/.test(src),false);
ck('linkedin creatives go through the chunker with a sink',
   /azWindsorFetchAll\(apiKey, 'linkedin', from, to, 'date,campaign,ad_name,spend,clicks,impressions', AZ_CREATIVE_CHUNK_DAYS, rows =>/.test(src),true);
ck('every creative connector is now chunked',
   (src.match(/AZ_CREATIVE_CHUNK_DAYS, rows =>/g)||[]).length,3);

// --- audience fold ---------------------------------------------------------
ck('the audience fold loaded',typeof ctx._foldAudRows,'function');
{
  const acfg={connector:'facebook',nameField:'adset_name',demoField:'conversions_submit_application_total',
              statusField:'adset_effective_status',statusFallback:'adset_status'};
  const arows=[];
  for(let d=1;d<=8;d++)for(let a=1;a<=3;a++)arows.push({
    date:'2026-04-0'+d, adset_name:'as'+a, campaign_name:'Camp'+a,
    spend:String(a*2), clicks:String(a), impressions:String(a*50),
    conversions_submit_application_total:String(a%2),
    adset_effective_status:d===8?'ACTIVE':'PAUSED'});
  const runA=(chunks)=>{const m={};for(const c of chunks) ctx._foldAudRows('meta',acfg,c,m);return m;};
  const wholeA=runA([arows]);
  ck('audience fixture produced several ad sets',Object.keys(wholeA).length,3);
  for(const size of [1,3,7,arows.length-1]){
    const chunks=[];for(let i=0;i<arows.length;i+=size)chunks.push(arows.slice(i,i+size));
    ck('audience chunk size '+size+': identical aggregate',runA(chunks),wholeA);
  }
  {
    const chunks=[];for(let i=0;i<arows.length;i+=3)chunks.push(arows.slice(i,i+3));
    ck('reversed audience chunk order matches on spend',
       Object.keys(runA(chunks.slice().reverse())).sort().map(k=>runA(chunks.slice().reverse())[k].spend),
       Object.keys(wholeA).sort().map(k=>wholeA[k].spend));
  }
  ck('audience spend survives folding',
     Object.values(wholeA).reduce((a,c)=>a+c.spend,0),
     arows.reduce((a,r)=>a+parseFloat(r.spend),0));
}

// --- campaign fold wiring --------------------------------------------------
// aggRows is nested in fetchAzCampaigns (closure over from/to), so assert the
// wiring structurally rather than lifting it out.
ck('the fold half takes the accumulator',/function aggRowsFold\(rows, ch, cfg, filterFn, demoFilterFn, camps\)/.test(src),true);
ck('the one-shot aggRows still exists for unfolded callers',
   /function aggRows\(rows, ch, cfg, filterFn, demoFilterFn\) \{[\s\S]{0,200}?aggRowsFold\([\s\S]{0,120}?return aggRowsFinish\(camps\);/.test(src),true);
ck('facebook and google fold into two channels each',
   /aggRowsFold\(rows, 'meta',[\s\S]{0,200}?cm\.meta\);[\s\S]{0,200}?cm\.clinician\);/.test(src)
   &&/aggRowsFold\(rows, 'google',[\s\S]{0,200}?cm\.google\);[\s\S]{0,200}?cm\.youtube\);/.test(src),true);
ck('every aggRows-backed connector passes a sink',
   (src.match(/30, rows => aggRowsFold\(rows, '/g)||[]).length,3);
ck('and the return finalizes the folded maps, not the raw rows',
   /meta:     aggRowsFinish\(cm\.meta\),/.test(src)&&!/aggRows\(fbRows/.test(src)&&!/aggRows\(ttRows/.test(src),true);
ck('audiences pass a sink too',/rows => \{ audN\[ch\] \+= rows\.length; _foldAudRows\(ch, cfg, rows, audMaps\[ch\]\); \}/.test(src),true);
ck('no creative/campaign/audience caller still accumulates',
   (src.match(/azWindsorFetchAll\(apiKey, (?:cfg\.connector|'facebook'|'google_ads'|'tiktok'|'reddit'|'openai_ads')[^)]*\)\s*(?:\.catch|,)/g)||[])
     .filter(x=>!/30,|AZ_CREATIVE_CHUNK_DAYS,/.test(x)).length,0);

// --- campaign fold: the delete-after-first-chunk bug -----------------------
// aggRows post-processed AND deleted c.freqVals / c.dates in the same function.
// Folding the whole thing deleted them after chunk 1, so chunk 2 pushed to
// undefined -> "Cannot read properties of undefined (reading 'push')". The row
// loop and the finalize must stay separate.
const {fn}=require('./slice.js');
vm.runInContext([fn(src,'aggRowsFold'),fn(src,'aggRowsFinish'),
  'Object.assign(globalThis,{aggRowsFold,aggRowsFinish});'].join('\n'),ctx);
ck('the fold half loaded',typeof ctx.aggRowsFold,'function');
ck('the finalize half loaded',typeof ctx.aggRowsFinish,'function');
ck('the fold half deletes nothing',/delete /.test(fn(src,'aggRowsFold')),false);
ck('the finalize half owns the deletes',
   /delete c\.freqVals/.test(fn(src,'aggRowsFinish'))&&/delete c\.dates/.test(fn(src,'aggRowsFinish')),true);
{
  const crows=[];
  for(let d=1;d<=8;d++)for(let c=1;c<=3;c++)crows.push({
    date:'2026-05-0'+d, campaign_name:'Camp'+c, spend:String(c*3), clicks:String(c*4),
    impressions:String(c*200), frequency:String(1+c/10),
    conversions_submit_application_total:String(c%2), effective_status:d===8?'ACTIVE':'PAUSED'});
  const META=ctx.AZ_CONNECTORS.meta;
  const runC=(chunks)=>{const m={};for(const ch of chunks) ctx.aggRowsFold(ch,'meta',META,undefined,undefined,m);
                        return ctx.aggRowsFinish(m);};
  const wholeC=runC([crows]);
  ck('campaign fixture produced several campaigns',Object.keys(wholeC.campaigns).length,3);
  for(const size of [1,2,5,crows.length-1]){
    const chunks=[];for(let i=0;i<crows.length;i+=size)chunks.push(crows.slice(i,i+size));
    // Before the split this threw on the SECOND chunk.
    ck('campaign chunk size '+size+': identical aggregate',runC(chunks),wholeC);
  }
  ck('campaign totals survive folding',wholeC.totals.spend,crows.reduce((a,r)=>a+parseFloat(r.spend),0));
  ck('frequency is still computed (freqVals survived to the finalize)',
     Object.values(wholeC.campaigns).every(c=>c.frequency!==null&&!isNaN(c.frequency)),true);
  ck('and the date range too (dates survived)',
     Object.values(wholeC.campaigns).every(c=>c.firstDate==='2026-05-01'&&c.lastDate==='2026-05-08'),true);
  ck('the finalize shape is {campaigns,totals}, not a bare map',
     [typeof wholeC.campaigns,typeof wholeC.totals],['object','object']);
}
ck('every folded channel is finalized exactly once',
   (src.match(/aggRowsFinish\(cm\./g)||[]).length,7);
ck('and no channel returns a raw accumulator',/:\s+cm\.[a-z]+,/.test(src),false);

// --- one fetch per connector ----------------------------------------------
// meta+clinician share facebook and google+youtube share google_ads. Fetching
// each channel separately ran the SAME 36-call, 68k-row query twice (measured
// 33.9s and 34.0s) for nothing — the fold already splits rows by campaign name.
ck('creative fetches are deduped by connector',/const _seenConn = \{\};/.test(src),true);
ck('a repeat connector attaches to the existing fold instead of refetching',
   /if \(cfg\.connector && _seenConn\[cfg\.connector\]\) \{[\s\S]{0,200}?continue;/.test(src),true);
ck('and the sink folds into every channel on that connector',
   (src.match(/for \(const _c of _seenConn\[cfg\.connector\]\) _foldCre\(_c, AZ_CONNECTORS\[_c\], rows\)/g)||[]).length,3);
ck('the channel list still carries every channel',/_seenConn\[cfg\.connector\]\.push\(ch\); channels\.push\(ch\);/.test(src),true);
// The fold body is what makes one fetch safe for two channels: it routes each
// row and skips the ones that belong to the sibling.
ck('the fold routes rows per channel',/const rCh = [\s\S]{0,120}?if \(rCh !== ch\) continue;/.test(fn(src,'_foldCreRows')),true);
{
  // One facebook payload, folded into meta and clinician, must split exactly as
  // two separate fetches would have.
  const META=ctx.AZ_CONNECTORS.meta, CLIN=ctx.AZ_CONNECTORS.clinician;
  const mixed=[];
  for(let d=1;d<=4;d++){
    mixed.push({date:'2026-06-0'+d,campaign_name:'Brand Prospecting',ad_name:'adA',adset_name:'s1',
      spend:'10',clicks:'5',impressions:'100',conversions_submit_application_total:'1'});
    mixed.push({date:'2026-06-0'+d,campaign_name:'S01 - Clinician Targeting - CAPI',ad_name:'adB',adset_name:'s2',
      spend:'20',clicks:'6',impressions:'200',conversions_submit_application_total:'2'});
  }
  const foldInto=(ch,cfg)=>{const m={},c={},a={},pl={};ctx._foldCreRows(ch,cfg,mixed,m,c,a,pl,{});return m;};
  const mMap=foldInto('meta',META), cMap=foldInto('clinician',CLIN);
  ck('meta takes only the non-clinician campaign',Object.keys(mMap),['adA']);
  ck('clinician takes only the clinician campaign',Object.keys(cMap),['adB']);
  ck('and no row is counted in both',
     Object.keys(mMap).filter(k=>Object.keys(cMap).includes(k)),[]);
  ck('their spend sums to the whole payload',
     mMap.adA.spend+cMap.adB.spend, mixed.reduce((a,r)=>a+parseFloat(r.spend),0));
}

// --- thumbnails must never hold the response ------------------------------
ck('thumbnail fetches are time-capped',/const _thumbCapped = \(pr\) => Promise\.race/.test(src),true);
ck('and window-bounded',/const _thumbFrom = \(\(\) => \{/.test(src),true);
ck('every thumbnail call goes through both',
   (src.match(/_thumbCapped\(azWindsorFetch\(apiKey, [^)]*_thumbFrom, to, 'ad_name,'/g)||[]).length,2);
ck('no thumbnail call still spans the raw window',
   /thumbPromises\.push\(azWindsorFetch\(apiKey, [^)]*, from, to, 'ad_name,'/.test(src),false);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
