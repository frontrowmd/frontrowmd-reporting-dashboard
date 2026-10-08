// Last-click acquisition split: the real mapUtmToChannel against the values that
// are ACTUALLY in HubSpot (measured over all 134 contacts with date_demo_booked,
// createdate 2026-09-01..2026-10-02), plus the worker/client wiring.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load('worker.js'), cli=load(process.env.DASH_SRC||'dashboard.html');
const ctx={console};vm.createContext(ctx);
vm.runInContext(fn(src,'mapUtmToChannel'),ctx);
const map=ctx.mapUtmToChannel;

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// --- the live distribution, value for value -------------------------------
// n=counts observed in the Sept window; they pin the real strings, not invented ones.
ck('google+cpc is paid (n=32)',map('google','cpc'),'google');
ck('google+organic is NOT paid (n=54)',map('google','organic'),null);
ck('ig+paid-social is meta (n=3)',map('ig','paid-social'),'meta');
ck('fb+paid-social is meta (n=2)',map('fb','paid-social'),'meta');
ck('facebook+paid-social is meta (n=1)',map('facebook','paid-social'),'meta');
ck('a brand name + referral is not paid (n=11)',map('Cymbiotika','referral'),null);
ck('blank/blank is not paid (n=26)',map('',''),null);
ck('null/undefined does not throw',map(null,undefined),null);

// The leak this change fixes: the live value is 'chatgpt.com', which the old
// allowlist ('chatgpt','openai',...) never matched -- so an explicitly PAID
// ChatGPT click (utm_medium 'cpc') was being counted as organic.
ck('chatgpt.com is paid (n=4, one of them cpc)',map('chatgpt.com','cpc'),'chatgpt');
ck('chatgpt.com with no medium is NOT assumed paid',map('chatgpt.com',''),null);
ck('openai.com with no medium is NOT assumed paid',map('openai.com',''),null);
ck('the bare chatgpt alias still works',map('chatgpt','cpc'),'chatgpt');
ck('case and whitespace are normalised',map('  ChatGPT.COM  ','CPC'),'chatgpt');
ck('and normalisation does not smuggle a bare domain through',map('  ChatGPT.COM  ','  '),null);

// Documented misses -- these are WRONG in the data, not in the mapper, and the
// assertions exist so a future fix is a deliberate change rather than a surprise.
ck('google+paid-social reads as organic (known bad row, n=1)',map('google','paid-social'),null);
ck('google with a blank medium is not treated as paid',map('google',''),null);

// --- worker wiring --------------------------------------------------------
const sched=fn(src,'processScheduledContacts');
ck('the split is counted in the branch that defines regularTotal',
   /regularTotal\+\+;[\s\S]{0,400}?mapUtmToChannel\(_rs, _rm\)/.test(sched),true);
ck('it reads RAW utm properties, not the Not-set-normalised pair',
   /const _rs = \(c\.properties\?\.utm_source \|\| ''\)\.trim\(\)/.test(sched),true);
ck('untagged requires BOTH source and medium empty',
   /else if \(_rs \|\| _rm\) regularOrganicTagged\+\+;\s*\n\s*else regularUntagged\+\+;/.test(sched),true);
ck('a per-day paid series is kept',/byDayRegularPaid\[_ak\] = \(byDayRegularPaid\[_ak\]\|\|0\) \+ 1/.test(sched),true);
ck('all four are exported',
   ['regularPaid','regularOrganicTagged','regularUntagged','byDayRegularPaid']
     .every(k=>new RegExp('\\b'+k+',').test(sched)),true);
// The allow-list drops anything not named in it -- its own comment says so.
ck('all four survive the scheduledOut allow-list',
   ['regularPaid: scheduled.regularPaid','regularOrganicTagged: scheduled.regularOrganicTagged',
    'regularUntagged: scheduled.regularUntagged','byDayRegularPaid: scheduled.byDayRegularPaid']
     .every(k=>src.includes(k)),true);
ck('payload exposes the three totals + prior/LM twins',
   ['scheduledRegularPaid:','scheduledRegularOrganicTagged:','scheduledRegularUntagged:',
    'scheduledRegularPaidPrior:','scheduledRegularPaidLastMonth:'].every(k=>src.includes(k)),true);
ck('payload shape change bumped the cache version past v29',
   (+((src.match(/API_CACHE_VER = 'v(\d+)'/)||[])[1]||0))>=30,true);

// --- client wiring --------------------------------------------------------
ck('the Non-Paid stat divides by the SAME _regC the funnel card shows',
   /fN\(_acqO\+_acqU\)\+' .u00f7 '\+fN\(_regC\)\+' regular demos'/.test(cli),true);
// Non-Paid is DERIVED from the paid share, not re-summed, so the headline and
// the mix bar underneath cannot drift apart by a rounding step.
ck('Non-Paid is the paid share subtracted from 100',
   /_npPct=\(_acqPct!=null\)\?\(100-_acqPct\):null/.test(cli),true);
ck('  and its prior/LM twins are derived the same way',
   /_npPctPV=\(_acqPctPV!=null\)\?\(100-_acqPctPV\):null/.test(cli)
   &&/_npPctLM=\(_acqPctLM!=null\)\?\(100-_acqPctLM\):null/.test(cli),true);
ck('  so Organic + Untagged IS the complement, by construction',
   (()=>{const P=31,O=52,U=17,R=P+O+U;   // the three buckets partition _regC
         return Math.abs((100-(P/R*100))-((O+U)/R*100))<1e-9;})(),true);
// Six tiles over seven grid units: the acquisition tile takes two so its stats sit
// side by side. Tiles and units drift apart silently and orphan a row, so pin both.
const _kmBlock=cli.slice(cli.indexOf("repeat(7,minmax(0,1fr))"),cli.indexOf('<div id="t-revenue">'));
const _nTile=(_kmBlock.match(/kmh\+=_kmTile\(/g)||[]).length;
const _nInline=(_kmBlock.match(/kmh\+='<div class="tile hero"/g)||[]).length;
const _nSpan=(_kmBlock.match(/grid-column:span 2/g)||[]).length;
ck('seven grid columns',/grid-template-columns:repeat\(7,minmax\(0,1fr\)\)/.test(cli),true);
ck('six tiles in the row',[_nTile,_nInline,_nTile+_nInline],[5,1,6]);
ck('and their grid units add up to the column count',_nTile+_nInline+_nSpan,7);
ck('_kmTile accepts the tooltip and a column span',
   /const _kmTile=\(label,value,calc,footer,extra,tip,span\)/.test(cli),true);
// Reserved label/calc height is what keeps every value on one baseline when one
// tile's label wraps to two lines and its neighbour's does not.
ck('label and calc heights are reserved so values share a baseline',
   /const _KM_LBL='min-height:36px', _KM_CALC='min-height:46px';/.test(cli),true);
ck('both the single-stat tiles and the split tile use that same rhythm',
   (cli.match(/_KM_LBL/g)||[]).length>=2&&(cli.match(/_KM_CALC/g)||[]).length>=2,true);
ck('the caveat names the upper-bound problem',/UPPER bound/.test(cli)&&/did not click, and came back later/.test(cli),true);
ck('a pre-v30 payload degrades to an em-dash, not NaN%',
   /_acqOK=\(_acqP!=null&&_acqO!=null&&_acqU!=null&&_regC>0\)/.test(cli)
   &&/_npPct!=null\?_npPct\.toFixed\(1\)\+'%':'\\u2014'/.test(cli),true);
ck('the three-way bar is suppressed when the data is absent',/if\(!_acqOK\)return '';/.test(cli),true);
ck('build version bumped past s5t225',
   (+((cli.match(/_BUILD_VER='s5t(\d+)'/)||[])[1]||0))>=226,true);
// "Unattributed" already means Windsor-minus-matched on the Ad KPIs page; reusing
// the word here would collide with a different number.
ck('the card never says "Unattributed"',
   /Unattributed/.test(cli.slice(cli.indexOf("Paid \\u00b7 Last Click"),cli.indexOf("<div id=\"t-revenue\">"))),false);

// --- the card's own definition of each bucket ------------------------------
// The subtext names which channels count as Paid. That list is a hand-written
// copy of mapUtmToChannel's return values, so pin the two together: adding a
// channel to the mapper without naming it on the card fails here.
const _defs=cli.slice(cli.indexOf('font-weight:700">Paid</span>'),cli.indexOf('origin unknown'));
const _mapperFn=fn(src,'mapUtmToChannel');
const _channels=[...new Set([..._mapperFn.matchAll(/return '([a-z]+)';/g)].map(m=>m[1]))];
ck('the mapper returns the seven channels the card names',_channels.sort(),
   ['chatgpt','google','linkedin','meta','reddit','tiktok','youtube']);
ck('every channel the mapper can return is named in the Paid definition',
   _channels.filter(c=>!new RegExp(c,'i').test(_defs.replace(/[^A-Za-z]/g,''))),[]);
ck('Google is qualified as cpc, since google+organic is the biggest organic bucket',
   /Google \(cpc\)/.test(_defs),true);
ck('all three buckets are defined, not just the two in the question',
   ['>Paid</span>','>Organic</span>','>Untagged</span>'].every(k=>cli.includes(k)),true);
ck('the mix legend carries a colour key per bucket',
   (cli.match(/_key\('var\(--green\)','Paid'|_key\('var\(--teal\)','Organic'|_key\('rgba\(255,255,255,\.35\)','Untagged'/g)||[]).length,3);
ck('Organic is defined as a residual, not as a measured category',
   /any other '\s*\n?\s*\+'UTM: organic search, brand referrals/.test(cli),true);
ck('Untagged is called out as unknown origin',/no UTM '\s*\n?\s*\+'at all; origin unknown/.test(cli),true);
// The block lives inside _acqBar, which returns '' when the data is absent --
// so an em-dash card does not carry definitions for numbers it is not showing.
ck('definitions sit inside the mix builder, so they vanish with it',
   cli.indexOf("if(!_acqOK)return '';")<cli.indexOf('>Untagged</span>'),true);

// --- CPD Paid, the card's second stat -------------------------------------
ck('CPD Paid reuses the shared _cpdSeg helper, not a second formula',
   /const _cPaidC=_cpdSeg\(_spC,_wtDW\.scheduledRegularPaid\),/.test(cli),true);
ck('it divides the SAME spend as CPD Regular and CAC',
   /const _cRegC=_cpdSeg\(_spC,_regC\)/.test(cli)&&/_cPaidC=_cpdSeg\(_spC,/.test(cli),true);
ck('its deltas use the paid prior/LM counts, not the regular ones',
   /_cPaidP=_cpdSeg\(_spP,_wtDW\.scheduledRegularPaidPrior\)/.test(cli)
   &&/_cPaidL=_cpdSeg\(_spL,_wtDW\.scheduledRegularPaidLastMonth\)/.test(cli),true);
ck('lower CPD Paid is better, so its footer is inverted',
   /_kmFoot\(_cPaidC,_cPaidP,_cPaidL,'\$',true\)/.test(cli),true);
ck('its calc names the paid denominator',
   /f\$\(_spC\)\+' .u00f7 '\+fN\(_acqP\)\+' paid demos'/.test(cli),true);
// Both stats must live in ONE tile div, or "split the card" silently became two cards.
ck('both stats ride the same tile',(_kmBlock.match(/_acqStat\(/g)||[]).length,2);
ck('CPD Paid leads, with a divider, then Non-Paid \u2014 inside that one tile',
   /_acqStat\('CPD[\s\S]{0,400}?width:1px;align-self:stretch[\s\S]{0,400}?_acqStat\('Non-Paid/.test(_kmBlock),true);
ck('each stat keeps its own calc and deltas',
   /_kmFoot\(_npPct,_npPctPV,_npPctLM,'%',true\)/.test(_kmBlock)
   &&/_kmFoot\(_cPaidC,_cPaidP,_cPaidL,'\$',true\)/.test(_kmBlock),true);
// Up is RED on Non-Paid: a growing unpaid share is a shrinking paid one. That
// also keeps the colour continuous with the paid share this stat replaced --
// the same movement lights the same way it did before the swap.
ck('a rising Non-Paid share reads as bad (inverted)',
   /_kmFoot\(_npPct,_npPctPV,_npPctLM,'%',true\)/.test(_kmBlock),true);
ck('  so Non-Paid and Pre-Launch % agree: down is the good direction',
   /_kmFoot\(_npPct,_npPctPV,_npPctLM,'%',true\)/.test(_kmBlock)
   &&/_kmFoot\(_preShare,_preShareP,_preShareL,'%',true\)/.test(_kmBlock),true);

// Every const the moved block reads must be declared above it -- a TDZ throw here
// blanks the whole section while still passing a syntax check.
const _iEmit=cli.indexOf('kmh+=\'<div class="tile hero"');
ck('no TDZ: everything the acquisition tile reads is declared above it',
   ['const _kmFoot=','const _cPaidC=','const _acqP=','const _acqOK=','const _KM_LBL=',
    'const _acqStat=','const _acqMix=','const _ACQ_TIP=']
     .map(k=>cli.indexOf(k)).every(v=>v>0&&v<_iEmit),true);
// Ad Spend, then the paid pair directly right of it, then the regular-cohort
// costs, then Pre-Launch % where CAC used to sit, then CVR.
ck('the tiles run in the order asked for',
   ["'Ad Spend'","'CPD \\u00b7 Paid'","'Non-Paid \\u00b7 Last Click'",
    "'CPD \\u00b7 Regular Demos'","'CPQD \\u00b7 Regular Demos'",
    "'Pre-Launch %'","'CVR \\u00b7 Regular'"]
     .map(t=>_kmBlock.indexOf(t)).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);
ck('CAC no longer occupies a Key Metrics tile',/kmh\+=_kmTile\('CAC'/.test(cli),false);
// ...but it is not lost from the page: the Efficiency card and the Marketing
// Funnel both still carry it, so removing the tile did not delete the metric.
ck('  and CAC survives elsewhere on the page',
   /statRow\('Paid ads CAC'/.test(cli)&&/trendItem\('Paid ads CAC'/.test(cli),true);
// Pre-Launch % is the Demo Composition pie's first slice as a number: same
// numerator and the same regular+pre denominator.
ck('Pre-Launch % divides pre-launch by ALL demos booked',
   /fN\(_preC\)\+' .u00f7 '\+fN\(_bothC\)\+' demos booked'/.test(cli),true);
ck('  built from the shared _bothC the composition card uses',
   /_preShare=_bothC>0\?\(_preC\/_bothC\*100\):null/.test(cli),true);
ck('  with prior/LM twins that guard a zero denominator',
   /_preShareP=\(_bothP!=null&&_bothP>0&&_preP!=null\)/.test(cli)
   &&/_preShareL=\(_bothL!=null&&_bothL>0&&_preL!=null\)/.test(cli),true);
// Pre-launch brands are the low-value tier -- the page already flags this share
// orange past 30% -- so DOWN is the good direction.
ck('a falling Pre-Launch % reads as good (inverted)',
   /_kmFoot\(_preShare,_preShareP,_preShareL,'%',true\)/.test(_kmBlock),true);
// The deal's own traffic tier, never the isLow name match (see pre-launch-test).
ck('pre-launch rides scheduledPreTotal, not a name match',
   /_preC=_wtDW\.scheduledPreTotal/.test(cli)&&!/_preC=[^;]*isLow/.test(cli),true);
ck('no unused declarations left behind',/_acqPP|_acqPL/.test(cli),false);

// --- review fixes -------------------------------------------------------
// ChatGPT decorates ORGANIC outbound links from its answers with
// utm_source=chatgpt.com, so the bare domain is NOT an ad click on its own.
ck('chatgpt.com with a paid medium is paid',map('chatgpt.com','cpc'),'chatgpt');
ck('chatgpt.com with NO medium is organic',map('chatgpt.com',''),null);
ck('chatgpt.com with an organic medium is organic',map('chatgpt.com','organic'),null);
ck('openai.com follows the same rule',[map('openai.com','paid'),map('openai.com','')],['chatgpt',null]);
// The explicit ad aliases still need no medium.
ck('the explicit ad aliases are unconditional',[map('openai_ads',''),map('chatgpt','')],['chatgpt','chatgpt']);
ck('this mirrors the google rule rather than inventing a second one',
   /s === 'google' && \(m === 'cpc' \|\| m === 'paid'\)/.test(src)
   &&/\['chatgpt\.com','openai\.com'\]\.includes\(s\) && \(m === 'cpc' \|\| m === 'paid'\)/.test(src),true);

// The cache gate was blind to soft failures: a fetch that burned its retries
// returns [] and reads as "no data", so a payload with a real gap was cached.
ck('soft failures are counted',/softFail: 0/.test(src),true);
ck('a windsor give-up counts as one',/SUBREQ\.softFail\+\+; console\.error\(`Az Windsor/.test(src),true);
ck('so does a rate-limited count',/SUBREQ\.softFail\+\+;\s*\n\s*console\.error\(`hsCount/.test(src),true);
// The gate reads softFail, but as a TOLERANCE rather than a veto: a persistent
// connector error would otherwise mean nothing is ever cached. See t_warm.js.
ck('and the cache gate reads them',
   /_azOk = !SUBREQ\.exhausted && !SUBREQ\.windsorDropped[\s\S]{0,80}SUBREQ\.softFail <= AZ_CRON_SOFTFAIL_TOLERANCE/.test(src),true);
ck('softFail is reset per request',/SUBREQ\.softFail = 0;/.test(src),true);
// TTLs are window-aware (ytd/allTime are cron-warmed and long-lived). Reads and
// writes deliberately no longer share one number: FRESHNESS decides whether to
// rebuild, RETENTION decides whether there is a fallback when the rebuild
// fails. This used to assert they were the same, which is what left nothing to
// fall back on. The full relationship is asserted in t_warm.js.
ck('freshness is decided by the shared window-aware function',
   /if \(age < azCacheTtlMs\(_azWin\)\)/.test(src),true);
ck('and it is still window-aware',
   /AZ_PRECOMPUTED_WINDOWS\.includes\(win\) \? \d+ : \d+/.test(src),true);
ck('while retention is its own, longer constant',
   /expirationTtl: AZ_CACHE_RETAIN_SEC/.test(src),true);

// Refresh must bust the SERVER cache too, not just the client one.
ck('refreshData arms the fresh flag',/S\.irfan\._azFresh=true;/.test(cli),true);
ck('and the analyzer POST sends it once',
   /if\(S\.irfan\._azFresh\)\{_azBody\.fresh=true;S\.irfan\._azFresh=false;\}/.test(cli),true);

// A capped list rolls its tail into one bucket that CARRIES its children, so
// rows.length is not the category count.
ck('the category count expands the rollup',/function _catCount\(rows\)/.test(cli),true);
// Was 3; the Clinician Composition card (one of them) was removed when Specialty
// Breakdown was dropped and Type of Clinician moved into Acquisition.
ck('and every header uses it',(cli.match(/fN\(_catCount\(rows\)\)\+' categories/g)||[]).length,2);
ck('no header still counts raw rows',/fN\(rows\.length\)\+' categories/.test(cli),false);

// A raw " in the tip terminated title="..." and ate the whole caveat.
ck('the tooltip carries no attribute-breaking quote',
   (()=>{const i=cli.indexOf('const _ACQ_TIP=');return cli.slice(i,i+560).includes('"');})(),false);
ck('and still says what Organic and Untagged mean',
   /u201cOrganic.u201d means no paid UTM/.test(cli)&&/u201cUntagged.u201d carries no UTM/.test(cli),true);
// Each caveat phrase must sit inside ONE string literal -- split across the
// concatenation it still renders, but silently stops matching these assertions.
ck('no caveat phrase is split across the concatenation',
   ['UPPER bound','did not click, and came back later',
    'means no paid UTM','carries no UTM at all'].every(x=>cli.includes(x)),true);
ck('the tile now headlines the non-paid share',/Non-Paid .u00b7 Last Click/.test(cli),true);
ck('  and the old paid-share label is gone',/'Paid .u00b7 Last Click'/.test(cli),false);

console.log(pass+' passed, '+fail+' failed');
