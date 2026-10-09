// Slices the REAL processScheduledContacts to check the Regular / Pre-Launch split.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const ctx={console,Object,String,Number,Array,Date,Set,Math,parseInt,parseFloat,isNaN,RegExp};
vm.createContext(ctx);
vm.runInContext([fn(src,'isPDT'),fn(src,'ptOff'),fn(src,'etOff'),fn(src,'fmt'),
                 fn(src,'cappedCounts'),fn(src,'mapUtmToChannel'),
                 fn(src,'processScheduledContacts')].join('\n'),ctx);

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

let _id=0;
// The loop keys on createdate and reads `average_monthly_web_traffic` (NOT the
// __cloned_ variant, which is the deal-side field), and dedupes by contact id.
const mk=(tier,src_,date,us,um)=>({id:'c'+(++_id),properties:{
  createdate:date||'2026-09-10T15:00:00Z',
  average_monthly_web_traffic:tier,
  hs_object_source_detail_1:src_||'',
  utm_source:us, utm_medium:um,
  company:'Co '+Math.random(), email:'a'+Math.random()+'@x.com',
}});
const PRE='Pre-launch / just launching', TEN='0-10K monthly web visitors', BIG='200K+ monthly web visitors';

const rows=[
  mk(BIG), mk(BIG), mk(TEN),                       // 3 regular demos
  mk(PRE), mk(PRE),                                // 2 pre-launch demos
  mk(BIG,'Livestorm Webinars'),                    // regular webinar
  mk(PRE,'Livestorm Webinars'),                    // PRE-LAUNCH webinar
  mk('Very Small'),                                // webinar by tier, no real tier
];
const r=ctx.processScheduledContacts(rows);

ck('regular + pre-launch covers everyone',r.regularTotal+r.preTotal,rows.length);
ck('pre-launch counts demos AND webinars',r.preTotal,3);
ck('regular counts demos AND webinars',r.regularTotal,5);

const sum=o=>Object.values(o||{}).reduce((a,b)=>a+b,0);
ck('daily series sum to their totals',[sum(r.byDayRegular),sum(r.byDayPre)],[r.regularTotal,r.preTotal]);
ck('the two series reconstruct byDayAll',sum(r.byDayRegular)+sum(r.byDayPre),sum(r.byDayAll));

// Day for day, not just in total.
let perDayOk=true;
for(const k of new Set([...Object.keys(r.byDayAll)])){
  if(((r.byDayRegular[k]||0)+(r.byDayPre[k]||0))!==r.byDayAll[k])perDayOk=false;
}
ck('and reconcile day by day',perDayOk,true);

// The distinguishing case: a webinar from a pre-launch brand must NOT be Regular.
const onlyPreWebinar=ctx.processScheduledContacts([mk(PRE,'Livestorm Webinars')]);
ck('a pre-launch webinar lands in Pre-Launch',onlyPreWebinar.preTotal,1);
ck('and not in Regular',onlyPreWebinar.regularTotal,0);
// The old byDay series would have dropped it entirely (webinars are skipped).
ck('the old demo-only series still excludes it',sum(onlyPreWebinar.byDay),0);

// A regular-tier webinar belongs to Regular, which the old "excl small" never showed.
const onlyBigWebinar=ctx.processScheduledContacts([mk(BIG,'Livestorm Webinars')]);
ck('a regular-tier webinar lands in Regular',onlyBigWebinar.regularTotal,1);
ck('and not in Pre-Launch',onlyBigWebinar.preTotal,0);

// 0-10K is NOT pre-launch — it stays Regular under the new definition.
const only10K=ctx.processScheduledContacts([mk(TEN)]);
ck('0-10K counts as Regular',[only10K.regularTotal,only10K.preTotal],[1,0]);

// No contacts at all must not produce undefined series.
const empty=ctx.processScheduledContacts([]);
ck('empty input yields empty series, not undefined',
   [typeof empty.byDayRegular,typeof empty.byDayPre,empty.regularTotal,empty.preTotal],
   ['object','object',0,0]);


// --- last-click acquisition split -----------------------------------------
// Counted in the same else branch that defines regularTotal, so the invariant
// "the three buckets partition the regular cohort" must hold on any input.
const A=ctx.processScheduledContacts([
  mk(BIG,'', '2026-09-10T15:00:00Z','google','cpc'),          // paid
  mk(BIG,'', '2026-09-10T15:00:00Z','ig','paid-social'),      // paid
  mk(BIG,'', '2026-09-11T15:00:00Z','chatgpt.com','cpc'),     // paid (the leak that was organic)
  mk(TEN,'', '2026-09-11T15:00:00Z','google','organic'),      // tagged, not paid
  mk(BIG,'', '2026-09-11T15:00:00Z','Cymbiotika','referral'), // tagged, not paid
  mk(BIG,'', '2026-09-12T15:00:00Z'),                         // untagged
  mk(PRE,'', '2026-09-12T15:00:00Z','google','cpc'),          // PRE-LAUNCH: must not count
]);
ck('the three buckets partition the regular cohort',
   A.regularPaid+A.regularOrganicTagged+A.regularUntagged,A.regularTotal);
ck('paid counts only recognised paid UTMs',A.regularPaid,3);
ck('organic/referral is tagged-but-not-paid',A.regularOrganicTagged,2);
ck('no UTM at all is untagged',A.regularUntagged,1);
ck('a pre-launch paid click is excluded entirely',A.regularTotal,6);
ck('the per-day paid series sums to the paid total',sum(A.byDayRegularPaid),A.regularPaid);
ck('the paid series never exceeds the regular series on any day',
   Object.keys(A.byDayRegularPaid).every(k=>A.byDayRegularPaid[k]<=A.byDayRegular[k]),true);
// A blank utm_source with a present medium is still "tagged", not untagged.
const B=ctx.processScheduledContacts([mk(BIG,'','2026-09-10T15:00:00Z','','referral')]);
ck('medium alone counts as tagged',[B.regularUntagged,B.regularOrganicTagged],[0,1]);

// --- Demo Acquisition UTM mix, split by tier ------------------------------
// The split runs BEFORE the isVerySmall skip, so a webinar registrant counts in
// its brand's real tier -- the same rule byDayRegular/byDayPre use.
const U=ctx.processScheduledContacts([
  mk(BIG,'', '2026-09-10T15:00:00Z','google','cpc'),
  mk(BIG,'', '2026-09-10T15:00:00Z','google','cpc'),
  mk(TEN,'', '2026-09-11T15:00:00Z','ig','paid-social'),
  mk(PRE,'', '2026-09-11T15:00:00Z','google','organic'),
  mk(PRE,'', '2026-09-12T15:00:00Z'),                       // untagged pre-launch
  mk(BIG,'Livestorm Webinars','2026-09-12T15:00:00Z','linkedin',''),   // regular webinar
  mk(PRE,'Livestorm Webinars','2026-09-12T15:00:00Z','linkedin',''),   // pre-launch webinar
]);
const cnt=l=>(l||[]).reduce((a,x)=>a+x.count,0);
ck('the two segments partition the combined total',U.utmTotalRegular+U.utmTotalPre,U.utmTotal);
ck('regular counts demos AND webinars',U.utmTotalRegular,4);
ck('pre-launch counts demos AND webinars',U.utmTotalPre,3);
ck('each segment\'s rows sum to its own total',
   [cnt(U.utmSourceRegular),cnt(U.utmSourcePre)],[U.utmTotalRegular,U.utmTotalPre]);
ck('all three dimensions split the same way',
   [cnt(U.utmMediumRegular),cnt(U.utmCampaignRegular)],[U.utmTotalRegular,U.utmTotalRegular]);
ck('a blank UTM still reads as "Not set" inside a segment',
   (U.utmSourcePre||[]).some(r=>r.label==='Not set'&&r.count===1),true);
ck('a pre-launch UTM never leaks into the regular segment',
   (U.utmSourceRegular||[]).some(r=>r.label==='google'&&r.count===2)
   &&!(U.utmSourceRegular||[]).some(r=>r.label==='Not set'),true);
// The combined field is unchanged, so the "Regular + Pre Launch" pill and any
// pre-v31 cached payload still describe the same population.
ck('the combined mix is untouched',cnt(U.utmSource),U.utmTotal);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
