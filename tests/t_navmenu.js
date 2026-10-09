// Sidebar menu groups. "Additional Insights" is collapsed on load, and the
// thing that makes that stick is the force-open gate in switchDashSub: it used
// to open the group for EVERY destination, so landing on the Executive or
// Brand dashboard re-expanded it and a collapsed default could not survive one
// click.
const {load}=require('./slice.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. Collapsed in the markup ────────────────────────────────────────────
ck('the group starts collapsed',
   /<div id="dash-submenu" style="display:none;padding-left:24px">/.test(cli),true);
ck('  and the arrow points the collapsed way',
   /<span id="dash-arrow"[^>]*>▶<\/span>/.test(cli),true);
ck('  matching how the sibling group ships',
   /<div id="content-submenu" style="display:none;padding-left:24px">/.test(cli),true);

// ── 2. The force-open gate ────────────────────────────────────────────────
ck('the member list exists',
   /const DASH_MENU_SUBS=\['signup','webinarPerf','demoQuality','outbound','strategy'\];/.test(cli),true);
ck('the group opens only for its own subs',
   /if\(DASH_MENU_SUBS\.indexOf\(sub\)>=0\)\{\s*\n\s*document\.getElementById\('dash-submenu'\)\.style\.display='block';/.test(cli),true);
// Never force it CLOSED: a reader who opened it keeps it open.
ck('  and nothing forces it closed',
   /getElementById\('dash-submenu'\)\.style\.display='none'/.test(cli),false);
ck('  with no unconditional open left',
   (cli.match(/getElementById\('dash-submenu'\)\.style\.display='block'/g)||[]).length,1);

// ── 3. The list must match the markup ─────────────────────────────────────
// If a sub-item is added to the group's HTML but not to DASH_MENU_SUBS,
// navigating to it leaves the group shut and the active row invisible.
const menu=(()=>{const i=cli.indexOf('<div id="dash-submenu"');
  return cli.slice(i,cli.indexOf('</div>\n      <div class="ni grp"',i));})();
const inMarkup=[...menu.matchAll(/switchDashSub\('(\w+)'\)/g)].map(m=>m[1]);
const declared=(()=>{const m=/const DASH_MENU_SUBS=\[([^\]]*)\]/.exec(cli);
  return m?m[1].split(',').map(x=>x.trim().replace(/'/g,'')):[];})();
ck('every sub in the group markup is declared a member',
   inMarkup.filter(s=>declared.indexOf(s)<0),[]);
ck('  and every declared member is really in the group',
   declared.filter(s=>inMarkup.indexOf(s)<0),[]);
// The top-level pages share switchDashSub but are NOT in the group - they are
// exactly the destinations that used to re-expand it.
ck('the top-level pages are not members',
   ['exec','overview','irfan','clinician','calculator'].filter(s=>declared.indexOf(s)>=0),[]);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
