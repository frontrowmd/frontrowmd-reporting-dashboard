// Clinician campaigns optimize for SEVERAL different conversions at once, and
// Meta reports Results per AD SET. Counting one fixed pair of columns therefore
// both over-counted (adding conversions an ad set wasn't optimizing for) and
// silently dropped the "Clinician" raw pixel event entirely, which no custom
// conversion wraps and so has no Windsor column.
// Every fixture row below is REAL Windsor output (facebook connector, pulled
// 2026-10-07) — the numbers are the ones Meta's own Ad Sets view shows.
const {load}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const prefix=src.slice(0,src.indexOf('\nexport default'));
const ctx={console};vm.createContext(ctx);
vm.runInContext(prefix+'\nObject.assign(globalThis,{metaTargetCount,metaTargetEvent,metaEventCount,metaOptTarget,metaTargetField,clinicianConvCount,chDemoCount,_foldAudRows,metaDemos,CLINICIAN_CONV_FIELDS});',ctx);
ck('the real resolver loaded',typeof ctx.metaTargetCount,'function');

const PIX='"pixel_id": "3681546762144757", ';
const PO_VC='{'+PIX+'"custom_event_type": "OTHER", "custom_event_str": "virtualclinicians"}';
const PO_CL='{'+PIX+'"custom_event_type": "OTHER", "custom_event_str": "Clinician"}';
const PO_OLD='{'+PIX+'"custom_event_type": "OTHER", "custom_conversion_id": "1403040218341205"}';
const PO_SCHED='{'+PIX+'"custom_event_type": "SCHEDULE"}';
const ev=(n,v)=>({action_type:'offsite_conversion.fb_pixel_custom.'+n,value:String(v)});

// ── The five live clinician ad sets, Oct 1-7 2026 ──────────────────────────
const CAPI='S01 - TOF - Clinician Targeting - CAPI';
const LAL ='S01 - TOF - Clinician Targeting - 2% Lookalikes';
const NOSMP='S01 - TOF - Clinician Targeting - CAPI - No Samples';
const rows=[
  // optimizes for virtualclinicians; `Clinician` also fires 142x but is NOT its result
  {campaign_name:CAPI,adset_name:'TOF - Clinicians - Job Titles',date:'2026-10-02',spend:'866.61',
   adset_promoted_object:PO_VC,actions_capi_clinicians:'3',actions_capi_virtual_clinician:'16',
   conversions:[ev('virtualclinicians',16),ev('clinicians',3),ev('Clinician',142)]},
  // optimizes for the RAW `Clinician` event — Meta shows 11, we showed 3
  {campaign_name:CAPI,adset_name:'TOF - Clinicians - Job Titles - Clinician',date:'2026-10-02',spend:'70.02',
   adset_promoted_object:PO_CL,actions_capi_virtual_clinician:'3',
   conversions:[ev('virtualclinicians',3),ev('Clinician',11)]},
  {campaign_name:LAL,adset_name:'TOF - Clinicians - Job Titles',date:'2026-10-02',spend:'306.53',
   adset_promoted_object:PO_VC,actions_capi_clinicians:'1',actions_capi_virtual_clinician:'3',
   conversions:[ev('virtualclinicians',3),ev('Clinician',13),ev('clinicians',1)]},
  // Meta shows 2, we showed 0
  {campaign_name:LAL,adset_name:'TOF - Clinicians - Job Titles - Clinician',date:'2026-10-02',spend:'47.39',
   adset_promoted_object:PO_CL,conversions:[ev('Clinician',2)]},
  // no conversions array at all → genuine zero for its target
  {campaign_name:NOSMP,adset_name:'TOF - Clinicians - Job Titles',date:'2026-10-02',spend:'26.22',
   adset_promoted_object:PO_VC,conversions:null},
];

// ── 1. Each ad set is counted by ITS OWN target ────────────────────────────
ck('virtualclinicians ad set counts its event, not the pair',ctx.metaTargetCount(rows[0]),16);
ck('  (the fixed pair would have said 19)',3+16,19);
ck('the Clinician-event ad set counts 11, which had NO column at all',ctx.metaTargetCount(rows[1]),11);
ck('  (the fixed pair would have said 3 — a conversion it does not optimize for)',ctx.metaTargetCount(rows[1])===3,false);
ck('2% LAL virtualclinicians ad set',ctx.metaTargetCount(rows[2]),3);
ck('2% LAL Clinician ad set — was invisible, Meta shows 2',ctx.metaTargetCount(rows[3]),2);
ck('an ad set with no array falls back to its event column (null → 0)',ctx.metaTargetCount(rows[4]),0);

// The 142 `Clinician` firings on rows[0] must NOT be added: that ad set
// optimizes for virtualclinicians, so blanket-summing events would be absurd.
ck('a non-target event firing 142x is ignored',ctx.metaTargetCount(rows[0])<142,true);

// ── 2. A campaign total is the SUM of its ad sets' own targets ─────────────
const campTotal=name=>rows.filter(r=>r.campaign_name===name).reduce((a,r)=>a+ctx.clinicianConvCount(r),0);
ck('Clinician Targeting - CAPI blends 16 + 11',campTotal(CAPI),27);
ck('2% Lookalikes blends 3 + 2',campTotal(LAL),5);
ck('No Samples really is zero',campTotal(NOSMP),0);
const grand=rows.reduce((a,r)=>a+ctx.clinicianConvCount(r),0);
ck('clinician MTD total',grand,32);
// What the shipped code used to do, recomputed here from the same rows.
const oldWay=rows.reduce((a,r)=>a+ctx.CLINICIAN_CONV_FIELDS.reduce((n,f)=>n+(parseFloat(r[f])||0),0),0);
ck('the old fixed-pair total was lower',oldWay,26);
ck('so the fix recovers the missing conversions',grand>oldWay,true);

// ── 3. History must not be rewritten ───────────────────────────────────────
// August rows carry the OLD custom_conversion_id, and Windsor returns the
// promoted object as it was — so following the target reproduces August exactly.
const aug={campaign_name:'S01 - TOF - Clinician Targeting - Official Test - LIfetime Budget',
  adset_name:'TOF - Clinicians - Job Titles',date:'2026-08-12',spend:'5632.52',
  adset_promoted_object:PO_OLD,actions_capi_clinicians:'352',actions_capi_virtual_clinician:null,
  conversions:[ev('clinicians',353),ev('WebinarSQL',1)]};
ck('August resolves via custom_conversion_id to the retired column',ctx.metaTargetCount(aug),352);
ck('  and matches what the old pair produced',ctx.clinicianConvCount(aug),352);
ck('  preferring the custom CONVERSION (352) over the raw event (353)',ctx.metaTargetCount(aug)===353,false);

// Defensive precedence. In live data the two shapes are disjoint — a custom
// conversion carries pixel_rule + custom_conversion_id, a raw-event target
// carries custom_event_str — but if one ever carried both, the custom
// CONVERSION must win, because that is what Meta reports as the ad set's Result
// (and it is narrower: Aug's conversion counted 352 of the event's 353).
const BOTH='{'+PIX+'"custom_event_type": "OTHER", "custom_conversion_id": "1403040218341205", "custom_event_str": "clinicians"}';
const bothRow={adset_promoted_object:BOTH,actions_capi_clinicians:'20',conversions:[ev('clinicians',99)]};
ck('a custom_conversion_id outranks a custom_event_str',ctx.metaTargetCount(bothRow),20);
ck('  so metaTargetEvent stays silent when an id is present',ctx.metaTargetEvent(bothRow),'');

// ── 4. Rows below ad-set grain keep the legacy pair ────────────────────────
const noPO={campaign_name:CAPI,adset_name:'x',spend:'10',actions_capi_clinicians:'2',actions_capi_virtual_clinician:'5'};
ck('no adset_promoted_object → -1, undeterminable',ctx.metaTargetCount(noPO),-1);
ck('  so clinicianConvCount falls back to the two-era pair',ctx.clinicianConvCount(noPO),7);

// ── 5. metaEventCount semantics ────────────────────────────────────────────
ck('a missing array is -1, not 0',ctx.metaEventCount({},'Clinician'),-1);
ck('an array without the event is -1',ctx.metaEventCount({conversions:[ev('other',4)]},'Clinician'),-1);
ck('an event present with value 0 is 0, not -1',ctx.metaEventCount({conversions:[ev('Clinician',0)]},'Clinician'),0);
ck('exact case wins over a loose match',ctx.metaEventCount({conversions:[ev('clinician',9),ev('Clinician',4)]},'Clinician'),4);
ck('`clinicians` and `Clinician` are never conflated',ctx.metaEventCount({conversions:[ev('clinicians',3)]},'Clinician'),-1);

// ── 6. The target is now NAMED, so the tables can show the split ───────────
ck('a raw custom event names itself',ctx.metaOptTarget(PO_CL),'Clinician');
ck('and so does virtualclinicians',ctx.metaOptTarget(PO_VC),'virtualclinicians');
ck('a custom conversion still resolves to its label',ctx.metaOptTarget(PO_OLD),'CAPI -- Clinicians');
ck('a standard event still title-cases',ctx.metaOptTarget(PO_SCHED),'Schedule');
ck('garbage stays blank',ctx.metaOptTarget('not json'),'');

// ── 7. The ad-set table populates optTarget for clinician (was meta-only) ──
const cfg={nameField:'adset_name',demoField:ctx.CLINICIAN_CONV_FIELDS,onlyClinician:true,
  statusField:'adset_effective_status',statusFallback:'adset_status'};
const map={};ctx._foldAudRows('clinician',cfg,rows,map);
// _foldAudRows keys by ad-set NAME alone, and "TOF - Clinicians - Job Titles"
// exists in all three clinician campaigns — so the Ad Sets table shows ONE row
// for the five live ad sets. Pre-existing behaviour, asserted so it stays visible.
ck('same-named ad sets merge across campaigns',Object.keys(map).length,2);
ck('  and the merged row keeps only the first campaign it saw',map['TOF - Clinicians - Job Titles'].campaign,CAPI);
ck('the Clinician-targeted ad set is labelled, not blank',map['TOF - Clinicians - Job Titles - Clinician'].optTarget,'Clinician');
ck('the virtualclinicians ad set too',map['TOF - Clinicians - Job Titles'].optTarget,'virtualclinicians');
ck('ad-set demos follow the target: 11 + 2',map['TOF - Clinicians - Job Titles - Clinician'].demos,13);
ck('and the two same-named virtualclinicians ad sets sum 16 + 3 + 0',map['TOF - Clinicians - Job Titles'].demos,19);
ck('ad-set demos sum to the campaign blend',Object.values(map).reduce((a,x)=>a+x.demos,0),grand);

// ── 8. chDemoCount routes clinician through the target-aware path ──────────
ck('chDemoCount clinician == clinicianConvCount',ctx.chDemoCount('clinician',cfg,rows[1]),11);
ck('chDemoCount does NOT read the fixed pair for clinician',ctx.chDemoCount('clinician',cfg,rows[1])===3,false);
ck('other channels still read their single column',ctx.chDemoCount('tiktok',{demoField:'conversions'},{conversions:'4'}),4);
ck('google still ceils',ctx.chDemoCount('google',{demoField:'conversions'},{conversions:'2.3'}),3);
ck('meta still goes through metaDemos',ctx.chDemoCount('meta',{},{adset_promoted_object:PO_SCHED,conversions_schedule_total:'7',conversions_submit_application_total:'99'}),7);

// ── 9. Wiring: the fetches must actually request what the resolver reads ───
const cnt=(re)=>(src.match(re)||[]).length;
ck('the conversions array is fetched alongside the promoted object',cnt(/META_CONV_ARRAY_FIELD/g)>=5,true);
ck('campaign fetch requests it',/META_PROMOTED_OBJ_FIELD\+META_CONV_ARRAY_FIELD, 30, foldFb/.test(src),true);
ck('both ad-set configs request it',cnt(/adset_promoted_object'\+META_LEAD_FIELDS\+META_CAPI_FIELDS\+META_CONV_ARRAY_FIELD/g),2);
ck('the creative fetch requests it',/META_PROMOTED_OBJ_FIELD \+ META_CONV_ARRAY_FIELD/.test(src),true);
ck('no aggregation site still reads cfg.demoField for clinician',/ch === 'meta'.*metaDemos\(row\).*\n.*demoRawOf\(row, cfg\.demoField\)/.test(src),false);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
