// The app renders at 90% (2026-11-09). CSS zoom has three side effects that
// were measured in Chromium before shipping, and each has a guard here:
//   1. 100vh inside the zoomed #app is 10% short -> #app compensates, and its
//      full-height children use 100%.
//   2. left/top set from mouse coordinates land 10% short inside #app -> every
//      in-app tooltip divides by UI_ZOOM. Tooltips on <body> are exempt.
//   3. scrollTop is in the zoomed element's own px while rects are visual ->
//      Jump To converts.
const {load}=require('./slice.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const css=(/#app\{display:none;zoom:([\d.]+);height:calc\(100vh \/ ([\d.]+)\)\}/.exec(cli)||[]);
const js=(/const UI_ZOOM=([\d.]+);/.exec(cli)||[]);
ck('#app is zoomed and compensates its height by the same factor',[css[1],css[2]],['.9','.9']);
ck('the JS factor matches the CSS one',+js[1],+css[1]);
ck('the login gate is not zoomed',/#gate\{[^}]*zoom/.test(cli),false);

// 1. Full-height things inside #app: 100%, never 100vh.
ck('the sidebar fills by %, not vh',/\.sidebar\{[^}]*height:100%;/.test(cli)&&!/\.sidebar\{[^}]*height:100vh/.test(cli),true);
ck('the content-session panel too',/\.cs-panel\{[^}]*height:100%;/.test(cli),true);
ck('the AI panel too',/id="ai-panel" style="[^"]*height:100%;/.test(cli),true);
// Markup between <div id="app"> and its close must hold no 100vh.
const app=cli.slice(cli.indexOf('<div id="app">'),cli.indexOf('<!-- Generate Variants Right Panel -->'));
ck('no 100vh left in the #app markup',/100vh/.test(app),false);

// 2. Every left/top assignment is either converted or a <body> tooltip.
const BODY_TIPS=['cln-wk-tip','cln-pie-tip','dbc-tip'];
const lines=cli.split('\n');
const bad=[];
lines.forEach((l,i)=>{
  if(!/\.style\.(left|top)=/.test(l))return;
  if(/style\.top='-9999px'/.test(l))return;                 // offscreen clipboard helper
  const assigns=l.match(/\.style\.(left|top)=[^;]*/g)||[];
  if(assigns.every(a=>/=_uz\(/.test(a)))return;
  // A body tooltip: its element was looked up / created by one of these ids
  // within the few lines above.
  const ctx=lines.slice(Math.max(0,i-12),i+1).join('\n');
  if(BODY_TIPS.some(id=>ctx.indexOf("'"+id+"'")>=0))return;
  bad.push(i+1);
});
ck('every in-app tooltip position goes through _uz()',bad,[]);
ck('the body tooltips really are appended to <body>',
   BODY_TIPS.every(id=>new RegExp("id='"+id+"'|\\.id='"+id+"'").test(cli))&&(cli.match(/document\.body\.appendChild\(tip\)/g)||[]).length>=3,true);

// 3. Jump To.
ck('Jump To converts a visual delta into scroll px',
   /main\.scrollTo\(\{top:Math\.max\(0,main\.scrollTop\+_uz\(delta-_secNavOffset\(\)\)\)/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
