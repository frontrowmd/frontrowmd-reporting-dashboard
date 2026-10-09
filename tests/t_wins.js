// The time-frame picker: All Time removed, Custom moved to the right of the
// quarters. The pills are built in two places and the order is produced by
// THREE loops, so "it looks right on one page" is not evidence.
const fs=require('fs');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};
const cli=fs.readFileSync(process.env.DASH_SRC||'dashboard.html','utf8');
const src=fs.readFileSync(process.env.WORKER_SRC||'worker.js','utf8');
const keys=re=>{const m=cli.match(re);return m?[...m[1].matchAll(/k:'([^']+)'/g)].map(x=>x[1]):null;};

// ── 1. All Time is gone from both pickers ─────────────────────────────────
const W=keys(/const W=\[(.*?)\];/s);
ck('the page picker lists five windows',W,['7d','mtd','lastMonth','ytd','custom']);
ck('  with All Time removed',W.includes('allTime'),false);
const MW=keys(/var wins=\[(.*?)\];/s);
ck('the ad-tables picker matches it',MW,['7d','mtd','lastMonth','ytd','custom']);
ck('neither picker still labels All Time',/l:'All Time'/.test(cli),false);

// ── 2. Order: fixed presets, then quarters, then Custom ───────────────────
// bldF emits three groups. Custom used to sit inside the first, which split the
// presets in two and put the escape hatch in the middle of the row.
const bld=(()=>{const i=cli.indexOf('function bldF(');return cli.slice(i,cli.indexOf('\nasync function selW',i));})();
ck('bldF is locatable',bld.length>500,true);
const iPresets=bld.indexOf("for(const w of W){if(w.k!=='custom')h+=_wPill(w);}");
const iQuarters=bld.indexOf('for(const q of _completedQuarters())');
const iCustom=bld.indexOf("W.filter(w=>w.k==='custom')[0]");
const iRange=bld.indexOf('h+=`<div class="cr');
ck('presets render first',iPresets>0,true);
ck('  then the quarters',iQuarters>iPresets,true);
ck('  then Custom',iCustom>iQuarters,true);
ck('  then the date inputs it reveals',iRange>iCustom,true);
ck('the preset loop SKIPS custom, so it cannot render twice',
   /for\(const w of W\)\{if\(w\.k!=='custom'\)h\+=_wPill\(w\);\}/.test(bld),true);
// One builder, two call sites — the Custom pill must not be a second copy of
// the markup, or its active rule and the presets' drift apart.
ck('both the preset and the Custom pill come from ONE builder',
   (bld.match(/_wPill\(/g)||[]).length,2);
ck('  which is defined once',(bld.match(/const _wPill=/g)||[]).length,1);

// ── 3. Custom keeps its special active rule ───────────────────────────────
// A quarter pill IS a custom window underneath (selQ sets win='custom' plus a
// quarterKey), so Custom must only light up when no quarter is selected — or
// picking Q3 lights two pills.
ck('Custom is active only when no quarter is chosen',
   /w\.k==='custom'\)\?\(s\.win==='custom'&&!s\.quarterKey\)/.test(bld),true);
ck('a quarter pill lights on its own key',
   /s\.win==='custom'&&s\.quarterKey===q\.from\+'_'\+q\.to/.test(bld),true);
ck('selQ sets both, which is why that rule is needed',
   /async function selQ\(f,t\)\{S\[AP\]\.win='custom';[\s\S]{0,200}?quarterKey=f\+'_'\+t/.test(cli),true);
ck('selW clears the quarter key',/async function selW\(w\)\{S\[AP\]\.win=w;S\[AP\]\.quarterKey=null/.test(cli),true);

// ── 4. No page defaults to, or restores, a window that no longer exists ───
ck('no page state defaults to allTime',/win:'allTime'/.test(cli),false);
// Only the S declaration's own defaults — a `x?x:'all'` ternary elsewhere is a
// label fallback, not a window key, and matching it made this read false.
const Sdecl=cli.slice(cli.indexOf('const S={'),cli.indexOf('const S={')+900);
const defaults=[...new Set([...Sdecl.matchAll(/win:'([a-zA-Z0-9]+)'/g)].map(m=>m[1]))];
ck('the page states do default to real windows',defaults.length>0,true);
ck('every defaulted window is still selectable',defaults.filter(w=>!W.includes(w)),[]);

// ── 5. The cron stops warming a window nobody can pick ────────────────────
const warmed=((src.match(/const AZ_PRECOMPUTED_WINDOWS = \[([^\]]*)\]/)||[])[1]||'')
  .split(',').map(x=>x.trim().replace(/'/g,'')).filter(Boolean);
ck('allTime is no longer precomputed',warmed.includes('allTime'),false);
ck('  and mtd/ytd still are',warmed,['mtd','ytd']);
ck('the client slow-window list agrees with the cron',
   (()=>{const m=(cli.match(/const _AZ_SLOW_WINS=\[([^\]]*)\]/)||[])[1]||'';
         return m.split(',').map(x=>x.trim().replace(/'/g,'')).filter(Boolean);})(),warmed);

// ── 6. The API still ACCEPTS allTime ──────────────────────────────────────
// Removing the pill is a UI decision. The endpoint keeps honouring the value so
// a saved link, a cached payload or a direct call does not start 400-ing.
ck('allTime is still a valid window server-side',
   /const OK=\['7d','mtd','lastMonth','ytd','allTime','custom'\]/.test(cli),true);
ck('and isAllTime handling survives in the worker',/isAllTime/.test(src),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
