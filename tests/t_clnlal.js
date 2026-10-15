// "CPD Paid Non-Lookalikes": clinician ad spend over the full submissions that
// spend can actually claim. Two independent tests — the campaign is not a
// Lookalike audience, AND the contact carries a paid last-click UTM. Both
// denominators are counted, because they differ materially: on the live
// Oct 2026 window it is 11 vs 14 of 15 full submissions ($244 vs $192).
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const ctx={console};vm.createContext(ctx);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{isLookalikeCampaign,mapUtmToChannel,buildClinicianComposition});',ctx);
ck('the helpers loaded',typeof ctx.isLookalikeCampaign,'function');

// ── 1. The campaign test, against LIVE names ──────────────────────────────
ck('the live clinician Lookalike campaign matches',
   ctx.isLookalikeCampaign('S01 - TOF - Clinician Targeting - 2% Lookalikes'),true);
ck('  and the brand one',ctx.isLookalikeCampaign('S01 - TOF - 2% Lookalikes - CAPI'),true);
ck('  singular and hyphenated spellings too',
   ['Lookalike','look-alike','LOOKALIKE AUD'].map(c=>ctx.isLookalikeCampaign(c)),[true,true,true]);
// The live non-Lookalike names, which must NOT match.
ck('real non-Lookalike campaigns do not match',
   ['S01 - TOF - Clinician Targeting - CAPI','FrontrowMD | Brand Search',
    'S01 - TOF - Cold - Functional Bev & Energy - CAPI','S01 - TOF - Clinician Targeting - CAPI - No Samples']
     .map(c=>ctx.isLookalikeCampaign(c)),[false,false,false,false]);
ck('a blank campaign is not a Lookalike',
   [ctx.isLookalikeCampaign(''),ctx.isLookalikeCampaign(null),ctx.isLookalikeCampaign(undefined)],
   [false,false,false]);
// 'lal' only as a whole word — unanchored it would swallow unrelated names.
ck('lal matches as a word, not as a substring',
   [ctx.isLookalikeCampaign('LAL - Clinicians'),ctx.isLookalikeCampaign('Lalitha Brand')],[true,false]);

// ── 2. The two denominators, over the live Oct 2026 cohort ────────────────
const PRE='Pre-launch / just launching';
const mk=(id,src_,med,camp,booked)=>({
  id:'d'+id, properties:{clinician_meeting_booked_date:booked?'2026-11-12T18:15:00Z':'',
    dealstage:'4201261817', createdate:'2026-10-10T12:00:00.000Z'}});
const contacts={};
const add=(id,src_,med,camp,booked)=>{
  contacts['d'+id]={__cid:'c'+id,createdate:'2026-10-10T12:00:00.000Z',
    utm_source:src_,utm_medium:med,utm_campaign:camp,clinician_type:'MD/DO'};
  return mk(id,src_,med,camp,booked);
};
// The 15 live full submissions from Oct 7-13 2026, verbatim.
const LIVE=[
  ['google','cpc','FrontrowMD | Brand Search'],              // Jirka
  ['','',''],                                                 // Misra   (untagged)
  ['fb','paid-social','S01 - TOF - Clinician Targeting - CAPI'],
  ['ig','paid-social','S01 - TOF - Clinician Targeting - CAPI'],
  ['google','cpc','FrontrowMD | Brand Search'],
  ['fb','paid-social','S01 - TOF - Clinician Targeting - CAPI'],
  ['ig','paid-social','S01 - TOF - Clinician Targeting - 2% Lookalikes'],  // Powley: LAL
  ['ig','paid-social','S01 - TOF - Clinician Targeting - CAPI'],
  ['google','cpc','FrontrowMD | Brand Search'],
  ['google','organic',''],                                    // Fassihi (organic)
  ['fb','paid-social','S01 - TOF - Clinician Targeting - CAPI'],
  ['google','cpc','FrontrowMD | Brand Search'],
  ['fb','paid-social','S01 - TOF - Clinician Targeting - CAPI'],
  ['fb','paid-social','S01 - TOF - Cold - Functional Bev & Energy - CAPI'],
  ['','',''],                                                 // Luna    (untagged)
];
const deals=LIVE.map((x,i)=>add(i,x[0],x[1],x[2],true));
const comp=ctx.buildClinicianComposition(deals,contacts,'2026-10-01','2026-10-31');
const sub=comp.submission;
ck('every row counts as a full submission',sub.contacts,15);
ck('the Lookalike one is excluded from BOTH denominators',sub.nonLookalike,14);
ck('  and the organic/untagged three only from the paid one',sub.paidNonLookalike,11);
// This is the distinction the card name turns on, so it is pinned explicitly.
ck('the two denominators genuinely differ',sub.nonLookalike!==sub.paidNonLookalike,true);
// Spend over each, as the card would read it.
const sp=2681, r0=(n)=>Math.round(sp/n);
ck('CPD paid non-Lookalike reads $244',r0(sub.paidNonLookalike),244);
ck('  where the literal reading would read $192',r0(sub.nonLookalike),192);
ck('  and plain Cost per Sign Up reads $179',r0(sub.contacts),179);

// ── 3. A Lookalike signup is excluded even when it is paid ────────────────
const only=[add(90,'ig','paid-social','S01 - TOF - Clinician Targeting - 2% Lookalikes',true)];
const c2=ctx.buildClinicianComposition(only,contacts,'2026-10-01','2026-10-31').submission;
ck('a paid Lookalike signup counts for neither',[c2.contacts,c2.nonLookalike,c2.paidNonLookalike],[1,0,0]);
// ...and an untagged non-Lookalike counts for the literal one only.
const unt=[add(91,'','','',true)];
const c3=ctx.buildClinicianComposition(unt,contacts,'2026-10-01','2026-10-31').submission;
ck('an untagged signup counts for the literal denominator only',
   [c3.contacts,c3.nonLookalike,c3.paidNonLookalike],[1,1,0]);

// ── 4. An INQUIRY without a booked call counts for nothing ────────────────
const inq=[add(92,'ig','paid-social','S01 - TOF - Clinician Targeting - CAPI',false)];
const c4=ctx.buildClinicianComposition(inq,contacts,'2026-10-01','2026-10-31');
ck('an inquiry is not a full submission',
   [c4.contacts,c4.submission.contacts,c4.submission.paidNonLookalike],[1,0,0]);

// ── 5. Wiring ─────────────────────────────────────────────────────────────
// The RAW utm pair, never the 'Not set'-normalised one: normalised, every
// untagged contact looks tagged and the paid denominator inflates to the
// literal one.
ck('the paid test reads the RAW utm pair',
   /const _rs2 = \(cp\.utm_source \|\| dp\.utm_source \|\| ''\)\.trim\(\);/.test(src),true);
ck('  and is the same mapUtmToChannel every other paid figure uses',
   /if \(mapUtmToChannel\(_rs2, _rm2\)\) subPaidNonLal\+\+;/.test(src),true);
// Prior windows must carry it or the tile's vs-prior footer is permanently blank.
ck('the prior-window KPI forwards the new count',
   /const paidNonLal = \(cp\.submission && cp\.submission\.paidNonLookalike\) \|\| 0;/.test(src),true);
ck('  and returns it',/return \{ spend, fullSub, paidNonLal, inquiries: cp\.contacts \|\| 0 \};/.test(src),true);
ck('the payload shape change bumped the cache version',
   +(/const API_CACHE_VER = 'v(\d+)'/.exec(src)||[])[1]>=43,true);

// ── 6. The RENDERED section ───────────────────────────────────────────────
// Regexes cannot tell _div(_sp,_pnl) from _div(_sp,_fo) in any way that
// survives a refactor, and that swap is precisely the bug that would make this
// card a duplicate of Cost per Sign Up. So render it and read the figures.
const cli=load(process.env.DASH_SRC||'dashboard.html');
const dctx={console};vm.createContext(dctx);
vm.runInContext([
  'var S={clinician:{win:"mtd"}};',
  "const f$=v=>v==null?'\\u2014':'$'+Math.round(v).toLocaleString();",
  "const fN=v=>v==null?'\\u2014':Math.round(v).toLocaleString();",
  decl(cli,'const _KM_LBL=',';'),
  fn(cli,'_kmTile'), fn(cli,'_kmPaceBar'),
  fn(cli,'_irTileFooterDash'), fn(cli,'_clnKeyMetricsHTML'),
  'Object.assign(globalThis,{_clnKeyMetricsHTML,S});',
].join('\n'),dctx);
// The live Oct 2026 window: $2,681 spend, 470 inquiries, 44 full submissions,
// of which 11 are paid non-Lookalike.
const PAY={
  ads:{spend:2681,budget:6000},
  composition:{contacts:470,submission:{contacts:44,nonLookalike:40,paidNonLookalike:11}},
  topFunnel:{clicks:2871,impressions:83092},
  kpiPrior:{spend:4276,fullSub:33,inquiries:383,paidNonLal:14},
  kpiPriorMonth:{spend:13796,fullSub:88,inquiries:528,paidNonLal:31},
};
const read=(payload,win)=>{
  const out=dctx._clnKeyMetricsHTML(payload,'',win||'mtd');
  const tiles=[...out.matchAll(/<div class="tile hero"[\s\S]*?<div class="tlbl"[^>]*>([^<]*)<\/div><div class="tval">([^<]*)</g)];
  const m={}; for(const t of tiles)m[t[1]]=t[2];
  return {map:m,html:out};
};
const R=read(PAY);
ck('six tiles render',Object.keys(R.map).length,6);
ck('CPD Paid Non-Lookalikes divides by the PAID non-LAL count',R.map['CPD Paid Non-Lookalikes'],'$244');
// If it divided by all full submissions it would read $61 — the same as the
// card beside it, which is the whole failure mode.
ck('  and is NOT the same as Cost per Sign Up',R.map['Cost per Sign Up'],'$61');
ck('  which the two values confirm are different cards',
   R.map['CPD Paid Non-Lookalikes']!==R.map['Cost per Sign Up'],true);
ck('Cost per Inquiry divides by inquiries',R.map['Cost per Inquiry'],'$6');
ck('Ad Spend shows the spend',R.map['Ad Spend'],'$2,681');
ck('Full App Rate is full submissions over inquiries',R.map['Full App Rate'],'9.4%');
ck('CTR is cumulative',R.map['CTR'],'3.46%');

// Pacing: only on a month window, and only on Ad Spend.
ck('the pacing bar renders on a month window',/pp (ahead|behind)/.test(R.html),true);
ck('  and not on a 7-day window',/pp (ahead|behind)/.test(read(PAY,'7d').html),false);
ck('  nor on year-to-date',/pp (ahead|behind)/.test(read(PAY,'ytd').html),false);
// One pacing bar, on Ad Spend only.
ck('exactly one pacing bar in the row',(R.html.match(/pp (ahead|behind)/g)||[]).length,1);

// A pre-v43 payload has no paidNonLookalike: the tile must say so rather than
// divide by zero or quietly fall back to the total.
const OLD=JSON.parse(JSON.stringify(PAY)); delete OLD.composition.submission.paidNonLookalike;
const R2=read(OLD);
ck('an older payload shows an em-dash, not a wrong number',R2.map['CPD Paid Non-Lookalikes'],'—');
ck('  while every other tile still renders',R2.map['Cost per Sign Up'],'$61');
// No ad data at all.
const NOADS=JSON.parse(JSON.stringify(PAY)); NOADS.ads={error:'down'};
const R3=read(NOADS);
ck('a Windsor outage blanks the cost cards, not the section',
   [R3.map['Ad Spend'],R3.map['CPD Paid Non-Lookalikes'],R3.map['Full App Rate']],
   ['—','—','9.4%']);

// ── 7. The section must survive BOTH pages' Jump To bars ──────────────────
// _SECNAV_SEL uses a DIFFERENT selector per page: the Clinician page looks for
// '.irf-card-title', every other page for '.dsec-h'. A heading carrying only
// one of them renders perfectly and vanishes from the other page's nav, which
// is invisible in any screenshot of the section itself. This section is hosted
// on both pages, so it needs both classes.
const _navSel=/const _SECNAV_SEL=\{([^}]*)\}/.exec(cli)[1];
ck('the Clinician page still selects .irf-card-title',
   /clinician:'#clinician-c \.irf-card-title'/.test(_navSel),true);
ck('  while the Executive page selects .dsec-h',
   /exec:'#page-exec \.dsec-h'/.test(_navSel),true);
const _kmFn=fn(cli,'_clnKeyMetricsHTML');
ck('the heading carries BOTH classes',
   /class="irf-card-title dsec-h dsec-in"/.test(_kmFn),true);
ck('  and is named for the Jump To bar',/data-nav="Key Metrics"/.test(_kmFn),true);
// Rendered, not just matched in source.
const _h=dctx._clnKeyMetricsHTML(PAY,'','mtd');
ck('the rendered heading would be found by the Clinician nav',
   /<div class="[^"]*\birf-card-title\b[^"]*"[^>]*>Key Metrics</.test(_h),true);
ck('  and by the Executive nav',
   /<div class="[^"]*\bdsec-h\b[^"]*"[^>]*>Key Metrics</.test(_h),true);
// No static host supplies this one's spacing: it is prepended into the page.
ck('the section carries its own bottom margin',/margin-bottom:16px/.test(_h),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
