// Budget table: every funded month must be explicit, because an unlisted month
// silently falls back to BUDGET_FALLBACK and the dashboard then paces against a
// stale plan with no warning.
const fs=require('fs');
let pass=0,fail=0;
function ck(n,g,w){const ok=JSON.stringify(g)===JSON.stringify(w);ok?pass++:fail++;
  console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));}
const src=fs.readFileSync('worker.js','utf8');

function monthBudget(key){
  const i=src.indexOf("'"+key+"': {");
  if(i<0) return null;
  const m=[null,src.slice(src.indexOf('{',i)+1,src.indexOf('}',i))];
  if(!m) return null;
  const o={};
  for(const part of m[1].split(',')){const [k,v]=part.split(':'); if(k&&v) o[k.trim()]=Number(v);}
  return o;
}
const EXCL=new Set(['clinician']);   // KPI_EXCLUDED_CHANNELS

const oct=monthBudget('2026-10');
ck('October is listed',oct!==null,true);
ck('Meta Brand',oct.meta,12000);
ck('Meta Clinician',oct.clinician,6000);
ck('Google',oct.google,3000);
ck('LinkedIn, ChatGPT and Reddit are explicitly zero',
   [oct.linkedin,oct.chatgpt,oct.reddit],[0,0,0]);
ck('TikTok and YouTube stay off',[oct.tiktok,oct.youtube],[0,0]);
ck('the plan totals $21,000',Object.values(oct).reduce((a,b)=>a+b,0),21000);
// The pacing card divides by the KPI roll-up, which skips clinician.
ck('the KPI roll-up is $15,000',
   Object.entries(oct).reduce((a,[k,v])=>a+(EXCL.has(k)?0:v),0),15000);
ck('clinician is still the only KPI-excluded channel',
   /const KPI_EXCLUDED_CHANNELS = new Set\(\['clinician'\]\);/.test(src),true);
// Every channel the table knows about must be named, or it inherits nothing and
// reads 0 by accident rather than by instruction.
const sept=monthBudget('2026-09');
ck('October names every channel September does',
   Object.keys(sept).filter(k=>!(k in oct)),[]);
// Guard the silent-fallback hazard.
// Pinned to a literal month, the fallback silently aged: August's plan would
// have stood in for November. It now tracks the newest month in the table.
ck('the fallback tracks the newest month, not a literal',
   /const BUDGET_FALLBACK = BUDGET_BY_MONTH\[Object\.keys\(BUDGET_BY_MONTH\)\.sort\(\)\.pop\(\)\];/.test(src),true);
ck('no literal month is pinned as the fallback',/BUDGET_FALLBACK = BUDGET_BY_MONTH\['/.test(src),false);
// A gap should be visible, not just least-wrong.
ck('an unlisted month logs a warning',/budget: no plan for \$\{key\}/.test(src),true);
ck('and names what to do about it',/Add the month to BUDGET_BY_MONTH/.test(src),true);
// The newest key must resolve to the month we just added.
ck('newest key is October',
   [...src.slice(src.indexOf('const BUDGET_BY_MONTH')).matchAll(/'(\d{4}-\d{2})':/g)].map(m=>m[1]).sort().pop(),'2026-10');

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
