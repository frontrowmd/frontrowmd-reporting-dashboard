// Pie category colours: no two DIFFERENT categories may share a colour within one
// chart. Every assertion COMPUTES the collision set from source -- none pins
// today's palette -- so a future colour edit that reintroduces a clash fails here.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const cli=load('dashboard.html'), src=load('worker.js');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// #abc -> #aabbcc, lowercased, so a 3-digit clash cannot hide behind formatting.
const norm=h=>{let x=String(h).trim().toLowerCase();
  if(/^#[0-9a-f]{3}$/.test(x))x='#'+x[1]+x[1]+x[2]+x[2]+x[3]+x[3];return x;};
const dupes=vals=>{const seen={},out=[];vals.map(norm).forEach((v,i)=>{if(seen[v]!==undefined)out.push([seen[v],i,v]);else seen[v]=i;});return out;};

// Load the real constants + helper.
// These are all single-line consts; grabbing the whole line is sturdier than
// matching an end-token that may contain non-ASCII.
const lineOf=(name)=>{
  const m=new RegExp('^const '+name+'=.*$','m').exec(cli);
  if(!m)throw new Error('missing const '+name);
  return m[0];
};
const ctx={};vm.createContext(ctx);
vm.runInContext(['PIE_ABSENCE','PIE_ROLLUP','PIE_UNMAPPED','PIE_ABSENCE_LABELS',
                 'CLINICIAN_PIE_COLORS','IRFAN_WT_COLORS','IRFAN_WT_ORDER','IRFAN_WT_LABELS']
                 .map(lineOf).concat(fn(cli,'pieRowColor'),
                 // top-level `const` is a lexical binding, not a property of the
                 // vm context -- hand them over explicitly.
                 'Object.assign(globalThis,{PIE_ABSENCE,PIE_ROLLUP,PIE_UNMAPPED,PIE_ABSENCE_LABELS,'
                 +'CLINICIAN_PIE_COLORS,IRFAN_WT_COLORS,IRFAN_WT_ORDER,IRFAN_WT_LABELS,pieRowColor});')
                 .join('\n'),ctx);

// --- the three reserved roles --------------------------------------------
const roles={PIE_ABSENCE:ctx.PIE_ABSENCE,PIE_ROLLUP:ctx.PIE_ROLLUP,PIE_UNMAPPED:ctx.PIE_UNMAPPED};
ck('three reserved role colours exist',Object.values(roles).every(v=>/^#[0-9a-fA-F]{6}$/.test(v||'')),true);
ck('and are pairwise distinct',dupes(Object.values(roles)).length,0);
ck('no role colour is reused by the clinician palette',
   Object.values(roles).filter(r=>ctx.CLINICIAN_PIE_COLORS.map(norm).includes(norm(r))),[]);
// Absence IS a legitimate value in the WT map ('Not set'), so exclude that one key.
const wtVals=Object.entries(ctx.IRFAN_WT_COLORS).filter(([k])=>k!=='Not set').map(([,v])=>v);
ck('no role colour is reused by a real web-traffic tier',
   Object.values(roles).filter(r=>wtVals.map(norm).includes(norm(r))),[]);

// --- within-map injectivity ----------------------------------------------
ck('the clinician palette has no duplicate',dupes(ctx.CLINICIAN_PIE_COLORS),[]);
const wtPairs=Object.entries(ctx.IRFAN_WT_COLORS);
ck('the web-traffic map has no two tiers sharing a colour',
   dupes(wtPairs.map(([,v])=>v)).map(([a,b,v])=>wtPairs[a][0]+'='+wtPairs[b][0]+' '+v),[]);
// The original bug: 'Not set' had no key so it fell through to a fallback that
// was byte-identical to Pre-launch.
ck("'Not set' is a real key, so it never reaches the fallback",
   Object.prototype.hasOwnProperty.call(ctx.IRFAN_WT_COLORS,'Not set'),true);
ck("and Pre-launch no longer shares the absence colour",
   norm(ctx.IRFAN_WT_COLORS['Pre-launch'])===norm(ctx.PIE_ABSENCE),false);

// --- fallbacks must not equal any mapped value ---------------------------
// This is the dominant bug class here; a literal-duplicate scan cannot see it.
const fbs=[...cli.matchAll(/IRFAN_WT_COLORS\[[^\]]*\]\s*\|\|\s*([A-Za-z_$][\w$]*|'#[0-9a-fA-F]{3,8}')/g)]
  .map(m=>m[1].startsWith("'")?m[1].slice(1,-1):ctx[m[1]]);
ck('every web-traffic lookup has a fallback',fbs.length,2);
ck('and no fallback collides with a mapped tier',
   fbs.filter(f=>wtVals.map(norm).includes(norm(f))),[]);

// --- the ordered legend covers every mapped key --------------------------
ck('every colour-mapped tier has a deterministic legend position',
   Object.keys(ctx.IRFAN_WT_COLORS).filter(k=>!ctx.IRFAN_WT_ORDER.includes(k)),[]);
// '(none)' must NOT be a colour key: line 6870 rewrites it to 'Not set' first,
// so such a key would be dead code again.
ck("'(none)' is not a colour key -- it is canonicalised before the lookup",
   Object.prototype.hasOwnProperty.call(ctx.IRFAN_WT_COLORS,'(none)'),false);
ck("the label map still performs that canonicalisation",ctx.IRFAN_WT_LABELS['(none)'],'Not set');

// --- dynamic injectivity over pieRowColor --------------------------------
// Drive the REAL helper over every shape a row list can take and assert no hex
// ever serves two distinct labels.
const run=(labels)=>{
  const m={};labels.forEach((l,i)=>{m[l]=norm(ctx.pieRowColor({label:l,children:/^Other \(/.test(l)?[{}]:undefined},i,ctx.CLINICIAN_PIE_COLORS));});
  return m;
};
const collisionsIn=m=>{const inv={},bad=[];for(const k in m){(inv[m[k]]=inv[m[k]]||[]).push(k);} 
  for(const c in inv)if(inv[c].length>1)bad.push(c+' <- '+inv[c].join('|'));return bad;};
const CAP=+(src.match(/capped\(bySpec,\s*(\d+)\)/)||[])[1];
ck('the worker caps specialty', Number.isInteger(CAP)&&CAP>0,true);
ck('the cap never exceeds the palette, so i%len cannot wrap',
   CAP<=ctx.CLINICIAN_PIE_COLORS.length,true);
for(const n of [1,2,5,8,CAP]){
  const base=Array.from({length:n},(_,i)=>'cat'+i);
  ck('n='+n+' distinct categories collide nowhere',collisionsIn(run(base)),[]);
  ck('n='+n+' plus Not set collides nowhere',collisionsIn(run(base.concat('Not set'))).length===0||n>=CAP,true);
}
// A full capped list: 8 real + the rollup, which is the worst real case.
const worst=Array.from({length:CAP-1},(_,i)=>'cat'+i).concat('Other (4 more)');
ck('a full capped list (reals + rollup) collides nowhere',collisionsIn(run(worst)),[]);
ck('the rollup does not borrow the absence colour',
   norm(ctx.pieRowColor({label:'Other (4 more)',children:[{}]},3,ctx.CLINICIAN_PIE_COLORS))===norm(ctx.PIE_ABSENCE),false);
ck('every absence spelling lands on the one absence colour',
   ['Not set','(none)','N/A','','Unknown'].map(l=>norm(ctx.pieRowColor({label:l},2,ctx.CLINICIAN_PIE_COLORS)))
     .every(c=>c===norm(ctx.PIE_ABSENCE)),true);

// --- literal duplicate scan across the file ------------------------------
// Catches a palette array that lists the same colour twice (the renderCSRules bug).
// Only FLAT palettes -- a bare comma-separated list of colour strings. Arrays of
// KPI objects legitimately repeat a hex because there the colour is a threshold
// (green = good, red = bad) applied to several independent metrics, not a
// category identity. Including them would make this assertion permanently red.
const arrays=[...cli.matchAll(/(?:const|var)\s+([A-Za-z_$][\w$]*)\s*=\s*\[([^\]]*#[0-9a-fA-F]{3}[^\]]*)\]/g)];
const isPalette=body=>/^\s*(?:'(?:#[0-9a-fA-F]{3,8}|var\(--[\w-]+\))'\s*,?\s*)+$/.test(body);
const offenders=arrays.filter(m=>isPalette(m[2])).map(m=>{
  const vals=[...m[2].matchAll(/'(#[0-9a-fA-F]{3,8})'/g)].map(x=>x[1]);
  return vals.length>1&&dupes(vals).length?m[1]+' ('+dupes(vals).map(d=>d[2]).join(',')+')':null;
}).filter(Boolean);
ck('the palette scan actually found the flat palettes',arrays.filter(m=>isPalette(m[2])).length>=3,true);
ck('no colour array in dashboard.html lists the same colour twice',offenders,[]);

// --- the four surfaces of one row agree ----------------------------------
const acq=fn(cli,'_clinicianPie');
ck('slice, legend chip and tooltip all read the same col',
   (acq.match(/fill="'\+col\+'"/g)||[]).length>=2&&/background:'\+col\+'/.test(acq)&&/hovAttrs\(r\.label,r\.count,col,/.test(acq),true);
ck('expanded children use one colour for chip and tooltip',
   /hovAttrs\(k\.label,k\.count,kCol,ofLine\)/.test(acq)&&/background:'\+kCol\+'/.test(acq),true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
