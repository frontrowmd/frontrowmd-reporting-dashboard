// Div-balance guard for the Demo Insights section. Restructuring it from
// "Volume card + a 2-col Quality/Efficiency grid" into one 3-col grid moves the
// closing tags around, and an unbalanced div in a template-literal builder does
// not throw -- it silently swallows the rest of the page into a nested card.
const fs=require('fs');
let pass=0,fail=0;
function ck(n,g,w){const ok=JSON.stringify(g)===JSON.stringify(w);ok?pass++:fail++;
  console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));}

const cli=fs.readFileSync('dashboard.html','utf8').split('\n');

// The section runs from the 3-col grid open to the line that closes it.
const start=cli.findIndex(l=>l.includes("colOpen('Volume','#3B82F6'"))-1;
ck('found the Demo Insights grid',cli[start].includes('repeat(3, minmax(0, 1fr))'),true);

// panelOpen emits an unclosed card div plus a self-closed header: net +1.
// statRow/_cpdTile/etc. are self-contained: net 0. Literal markup counts as written.
let depth=0,end=-1,maxDepth=0;
const trace=[];
for(let i=start;i<start+200&&i<cli.length;i++){
  const line=cli[i];
  if(!/^\s*oh\+=/.test(line)) continue;
  let delta=0;
  // literal divs written into the builder
  delta+=(line.match(/<div/g)||[]).length;
  delta-=(line.match(/<\/div>/g)||[]).length;
  // helpers that leave a div open
  delta+=(line.match(/panelOpen\(/g)||[]).length;
  delta+=(line.match(/oh\+=colOpen\(/g)||[]).length;
  depth+=delta;
  maxDepth=Math.max(maxDepth,depth);
  if(delta) trace.push(`${i+1}: ${delta>0?'+':''}${delta} -> ${depth}`);
  if(depth===0){end=i;break;}
}
ck('section closes, and closes exactly once',end>start,true);
ck('never over-closes mid-section',depth,0);
ck('nesting stays sane (grid > card > inner grid)',maxDepth<=3,true);

// Three sibling panels, all at the same depth inside the grid.
const body=cli.slice(start,end+1).join('\n');
ck('three columns in the section',(body.match(/oh\+=colOpen\(/g)||[]).length,3);
ck('no nested cards inside the section card',(body.match(/oh\+=panelOpen\(/g)||[]).length,0);
ck('Volume, Quality, Efficiency in order',
   ['Volume','Quality','Efficiency'].map(p=>body.indexOf(`colOpen('${p}'`)).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);

// The right-hand "excl. small brands" column is gone from Volume.
const vol=body.slice(0,body.indexOf("colOpen('Quality'"));
const qual=body.slice(body.indexOf("colOpen('Quality'"),body.indexOf("colOpen('Efficiency'"));
const html=cli.join('\n');
ck('no _exTag cards remain in Volume',/_exTag/.test(vol),false);
ck('Volume has exactly three stat rows',(vol.match(/oh\+=statRow\(/g)||[]).length,3);
ck('Volume keeps all three cards, renamed for the population they count',
   ["'Regular Demos to Occur'","'Regular Demos Supposed to Happen'","'Qualified Regular Demos'"].every(x=>vol.includes(x)),true);
ck('the reconciliation "Demos held" row is gone',/statRow\('Demos held'/.test(vol),false);
// Spacing is the COLUMN's job now (flex + gap), so no card carries its own
// margin -- a stray default margin would stack on top of the gap and break the
// rhythm the three columns share.
const sectionRows=(body.match(/oh\+=statRow\(/g)||[]).length;
ck('every card in the section defers spacing to the column',
   (body.match(/oh\+=statRow\([^\n]*noMargin/g)||[]).length,sectionRows);
ck('columns are flex stacks with one gap rule',
   /min-width:0;display:flex;flex-direction:column;gap:10px/.test(html),true);
ck('the column header no longer adds its own bottom margin',
   /gap:8px;margin-bottom:0;padding-bottom:10px;border-bottom/.test(html),true);
ck('no inner grid still sets a bottom margin',
   /gap:8px;margin-bottom:10px">/.test(body),false);
// The two no-show rates share a row, which also levels Quality with the others.
ck('no-show rates sit on one row',
   /repeat\(2, minmax\(0, 1fr\)\);gap:10px">';\n\s*oh\+=statRow\('Demo no-show rate'[\s\S]*?oh\+=statRow\('Webinar no-show rate'/.test(body),true);
ck('all three columns are three rows deep',
   [vol,qual,body.slice(body.indexOf("colOpen('Efficiency'"))]
     .map(c=>(c.match(/oh\+=statRow\(/g)||[]).length),[3,4,5]);

// The old two-column wrapper must be fully gone, not just bypassed.
ck('no orphaned 2-col Quality/Efficiency grid',
   /grid-template-columns:1fr 1fr;gap:12px;margin-bottom:24px/.test(body),false);

// Demo Insights must render after Trend Analysis (#ov-truecpd-wrap), and its
// header must sit in the section card rather than inside the first column.
ck('Demo Insights sits after Trend Analysis',
   html.indexOf('id="ov-truecpd-wrap"')<html.indexOf('id="dsec-demoinsights"'),true);
ck('and before the Marketing Funnel row',
   html.indexOf('id="dsec-demoinsights"')<html.indexOf('id="sec-row2"'),true);
// The section header lives on the section card, not inside a column -- which
// is what used to push the first column's content down out of line.
ck('header is not injected into any column',/colOpen\([^;]*dsec-h/.test(html),false);
ck('it sits on the section card instead',
   /sec sec-card dsec"><div class="dsec-h dsec-in" id="dsec-demoinsights"/.test(html),true);
ck('section is one card',/sec sec-card dsec"><div class="dsec-h dsec-in" id="dsec-demoinsights"/.test(html),true);

// Quality: demos only, with the no-show split.
ck('Quality has four rows',(qual.match(/oh\+=statRow\(/g)||[]).length,4);
ck('no-show is split into demo and webinar',
   /statRow\('Demo no-show rate'/.test(qual)&&/statRow\('Webinar no-show rate'/.test(qual),true);
// Pre-launch is out of the Qualified Demo definition, so it is out of the
// population these rates describe too — numerator and denominator both.
ck('prune divides by the demo-only base, matching its own text',
   /_cxPreC\)\} \\u00f7 \$\{fN\(_supOutcomeC\)\}/.test(qual),true);
ck('and that base excludes pre-launch',/const _supOutcomeC=_heldPreC\+_nsPreC\+_cxPreC;/.test(html),true);
ck('qualification rate excludes webinars',/_qdOnlyC\)\} \\u00f7 \$\{fN\(_qdOnlyDen\)\}/.test(qual),true);
ck('no Quality row still divides by the webinar-inflated total',/fN\(_supAllC\)/.test(qual),false);

// Efficiency rename.
ck('CPD All replaces CPD Regular + Pre-Launch',
   /statRow\('CPD All'/.test(body)&&!/CPD Regular \+ Pre-Launch/.test(body),true);

// ── Conversion Funnel ──────────────────────────────────────────────
ck('Conversion Funnel sits ABOVE Demo Insights (2026-10-29)',
   html.indexOf('id="dsec-cvrfunnel"')<html.indexOf('id="dsec-demoinsights"'),true);
ck('and still precedes the Marketing Funnel',
   html.indexOf('id="dsec-cvrfunnel"')<html.indexOf('id="sec-row2"'),true);
ck('the loading skeleton matches: funnel before Volume / Quality / Efficiency',
   html.indexOf('<!-- Row 3: Conversion Funnel')>0&&html.indexOf('<!-- Row 3: Conversion Funnel')<html.indexOf('<!-- Row 4: Volume / Quality / Efficiency'),true);

const fstart=html.indexOf('const _cvrOf=(n,v)');
const fend=html.indexOf("getElementById('t-funnel')");
ck('funnel block located',fstart>0&&fend>fstart,true);
const fn_=html.slice(fstart,fend);

// The rail of five cards became a Clinician-style stage table. Every one of the
// page's own steps is still a row, in the same order, with HubSpot's deal
// stages interleaved around them.
const rowsOrder=['Visitors','Demo clicks','Appointment Scheduled','Regular Demos Scheduled','Webinar Registered',
  'Regular Demos Supposed to Happen','Demo Happened','Qualified Regular Demos','Webinar Happened',
  'Decision Maker Brought-In','Contract Sent','Closed Won'];
ck('every row, in the order asked for',
   rowsOrder.map(x=>fn_.indexOf("'"+x+"'")).every((v,i,a)=>v>0&&(i===0||v>a[i-1])),true);
ck('no rail cards or connectors remain',/fh\+=fnStage\(|fh\+=fnStep\(/.test(fn_),false);
ck('the two sub-cohorts are marked as such',(fn_.match(/kind:'page',sub:true,label:'(Regular Demos Scheduled|Qualified Regular Demos)'/g)||[]).length,2);
ck('the two webinar stages run in parallel',(fn_.match(/'Webinar (Registered|Happened)',BRAND_STAGE_COLORS\.\w+,\{par:true/g)||[]).length,2);
ck('Regular Demos Scheduled keeps its Pre-Launch and All detail',
   /note:fN\(_preC\)\+' pre-launch \\u00b7 '\+fN\(_bothC\)\+' all'/.test(fn_),true);
ck('the losses keep their loss tone, in the strip',/'loss'\)\}/.test(fn_),true);
ck('three losses',(fn_.match(/label:'(Prune|Demo no-show|Webinar no-show)'/g)||[]).length,3);
// Each loss names its own base -- the webinar one in particular is over
// registrants, not over the deals in that stage.
ck('webinar no-show names its own base',/note:'of '\+\(_wnrC!=null\?fN\(_wnrC\):'\\u2014'\)\+' registrants'/.test(fn_),true);

// Three CVRs, one per scheduled population, on the same totals as the rail.
ck('three CVR segments',(fn_.match(/_cvrSeg\(/g)||[]).length,3);
ck('CVRs read the same three totals',
   /_cvrReg=_cvrOf\(_regC/.test(fn_)&&/_cvrPre=_cvrOf\(_preC/.test(fn_)&&/_cvrAll=_cvrOf\(_bothC/.test(fn_),true);
ck('no demos+webinars CVR remains',/_cvrDW/.test(fn_),false);

// Rates: both keyed on ALL scheduled. The old divider divided a
// webinar-inclusive numerator by a demo-only denominator and printed 191%.
ck('supposed-to-happen divides by ALL scheduled',/rSupposed=_bothC>0\?\(demosSupposed\/_bothC/.test(fn_),true);
ck('clicks to scheduled uses ALL scheduled too',/r2=demoClicks>0\?\(_bothC\/demoClicks/.test(fn_),true);
ck('neither rate still divides by the demo-only count',
   /\(demosSupposed\/scheduled|\(scheduled\/demoClicks/.test(fn_),false);
// All four steps are candidates, so Biggest drop cannot miss the real worst one.
ck('every page step can win Biggest drop',(fn_.match(/',pct:/g)||[]).length,4);
ck('including Scheduled -> Supposed and Supposed -> Qualified',
   fn_.includes("Scheduled \\u2192 Supposed to happen")&&fn_.includes("Supposed to happen \\u2192 Qualified"),true);

// Chrome: one delta renderer, and it SUPPRESSES vs-LM when there is no value.
ck('a single delta renderer for the funnel',/function fDelta\(cur,pv,lmv,fmt,dec\)/.test(html),true);
ck('vs LM renders only when a value exists',/if\(lmv!=null\)h\+=`<div style="display:flex;gap:5px"><span[^`]*vs LM/.test(html),true);
ck('nothing hand-rolls its own vs-LM row',/vs LM<\/span>\$\{fmtDC/.test(fn_),false);
ck('CVR segments use it too, at 2dp',/fDelta\(v,pv,lmv,'%',2\)/.test(fn_),true);
// "(none)" read like a bug beside "N/A". They are different source values, so
// both stay -- the empty one just says what it means now.
ck('the empty tier is labelled',/'\(none\)':'Not set'/.test(html),true);
ck('and the pie no longer special-cases it',/key==='\(none\)'\?'\(none\)':key/.test(html),false);

// New ARR / ACV sat stacked beside the donut, leaving a tall empty gutter.
ck('ARR and ACV sit side by side above the donut',
   /irf-mini-kpi-grid" style="grid-template-columns:1fr 1fr;margin-bottom:14px/.test(html),true);
ck('the donut is no longer squeezed next to them',
   /flex:1;min-width:280px"><\/div>/.test(html),false);

ck('breakdown rows stay a readable width',/max-width:240px/.test(html),true);


// Demo Insights order: Efficiency, Volume, Quality. Done with CSS order, not by
// moving the blocks -- the Efficiency block declares consts the Conversion
// Funnel below reads (_regC, _spC), so relocating it risks an ordering break.
ck('columns carry an explicit order',/function colOpen\(label,dotColor,first,order\)/.test(html),true);
ck('Efficiency is first',/colOpen\('Efficiency','#BA7517',true,1\)/.test(html),true);
ck('then Volume',/colOpen\('Volume','#3B82F6',false,2\)/.test(html),true);
ck('then Quality',/colOpen\('Quality','#1D9E75',false,3\)/.test(html),true);
ck('only the visually-first column omits its left rule',
   (html.match(/colOpen\('[A-Za-z]+','#[0-9A-Fa-f]+',true,/g)||[]).length,1);
ck('the blocks stayed put, so _regC still precedes the funnel',
   html.indexOf('const _regC=')<html.indexOf('const _cvrReg='),true);

// Nav labels: the menu falls back to textContent, which swept up the caret and
// the "21 fields" count -- hence the run-together entries.
ck('field-reference headers declare a nav label',/class="dsec-h dsec-in" data-nav="'\+t\+'"/.test(html),true);
ck('the nav prefers it over textContent',/getAttribute\('data-nav'\)\|\|el\.textContent/.test(html),true);

console.log('\n'+pass+' passed, '+fail+' failed');