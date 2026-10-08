// The hover readout's DATE is its subject — every number under it is only
// meaningful once you know which point you are on. It used to render at 9.5px
// in 62% white, uppercase: caption styling, so it read as a label ON the panel
// rather than as the panel's headline. These assertions pin the promotion, and
// pin the two things the old label could not say — the weekday, and whether the
// bucket is a day, a week or a month.
const {load,fn,decl}=require('./slice.js');const vm=require('vm');
const src=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── a DOM small enough to be obviously honest ──────────────────────────────
function El(tag){this.tag=tag;this.children=[];this.attrs={};this.style={cssText:'',opacity:'',left:'',top:''};this._text='';}
El.prototype.appendChild=function(c){this.children.push(c);return c;};
El.prototype.removeChild=function(c){this.children=this.children.filter(x=>x!==c);return c;};
El.prototype.getAttribute=function(k){return (k in this.attrs)?this.attrs[k]:null;};
Object.defineProperty(El.prototype,'firstChild',{get(){return this.children[0]||null;}});
Object.defineProperty(El.prototype,'textContent',{get(){return this._text;},set(v){this._text=v;}});
Object.defineProperty(El.prototype,'offsetWidth',{get(){return 200;}});
Object.defineProperty(El.prototype,'offsetHeight',{get(){return 120;}});
const byId={};
const document={createElement:t=>new El(t),getElementById:id=>byId[id]||null,body:new El('body')};
document.body.appendChild=function(c){byId[c.id]=c;El.prototype.appendChild.call(this,c);return c;};

const ctx={console,document,window:{innerWidth:1400,innerHeight:900},Date};
vm.createContext(ctx);
vm.runInContext([
  decl(src,"const CLN_TR_MON=['Jan'","const CLN_TR_DOW=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];"),
  fn(src,'_clnTrLabel'), fn(src,'_clnTrEnd'), fn(src,'_clnTrHead'),
  fn(src,'clnWkTip'), fn(src,'clnWkTipMove'),
  'Object.assign(globalThis,{_clnTrHead,clnWkTip,CLN_TR_MONF,CLN_TR_DOW});'
].join('\n'),ctx);
ck('the real header formatter loaded',typeof ctx._clnTrHead,'function');

// ── 1. The headline says what the axis tick could not ──────────────────────
const YR=new Date().getFullYear();
ck('a day carries its weekday',ctx._clnTrHead(YR+'-09-28','day'),'Mon, Sep 28');
ck('  (the axis tick alone said only this)',ctx._clnTrHead(YR+'-09-28','day').indexOf('Sep 28')>=0,true);
ck('a week reads as a RANGE, not a start date',ctx._clnTrHead(YR+'-09-28','week'),'Sep 28 – Oct 4');
ck('a month spells itself out with the year',ctx._clnTrHead(YR+'-10-01','month'),'October '+YR);
ck('weekday maths is real, not a constant',ctx._clnTrHead(YR+'-09-29','day').slice(0,3),'Tue');
ck('a week spanning a month boundary names both',ctx._clnTrHead(YR+'-11-30','week'),'Nov 30 – Dec 6');

// The lookback is 100 days, so it crosses New Year. A point from last year that
// rendered as a bare "Sep 28" would be read as this year's.
ck('the current year is left implicit',ctx._clnTrHead(YR+'-09-28','day').indexOf(String(YR))<0,true);
ck('an earlier year is stated',ctx._clnTrHead((YR-1)+'-12-29','day'),'Mon, Dec 29, '+(YR-1));
// Dec 29 (YR-1) runs to Jan 4 (YR). A single trailing year would attach to the
// END date and name the wrong one, so a straddling week dates both ends.
ck('  a week straddling New Year dates BOTH ends',ctx._clnTrHead((YR-1)+'-12-29','week'),'Dec 29, '+(YR-1)+' – Jan 4, '+YR);
ck('  a fully-last-year week needs only one',ctx._clnTrHead((YR-1)+'-09-28','week'),'Sep 28 – Oct 4, '+(YR-1));
ck('  and a month always carries its year',ctx._clnTrHead((YR-1)+'-12-01','month'),'December '+(YR-1));

// ── 2. The tooltip renders it as a headline, not a caption ─────────────────
const mkEl=a=>{const e=new El('rect');e.attrs=a;return e;};
const ev={clientX:300,clientY:300};
const R1='Count · Inquiry|58|58 inquiries|#C33|solid|1|';
ctx.clnWkTip(ev,mkEl({'data-wk':'Mon, Sep 28','data-grain':'Day','data-r1':R1}));
const tip=byId['cln-wk-tip'];
ck('the tooltip was built',!!tip,true);
const cap=tip.children[0];
const headRow=cap.children[0], head=headRow.children[0];
ck('the date is the FIRST thing in the panel',head.textContent,'Mon, Sep 28');
ck('it is set in the display face',/font-family:var\(--font-h\)/.test(head.style.cssText),true);
ck('at 15px, not the old 9.5px',/font-size:15px/.test(head.style.cssText),true);
ck('in full white, not 62%',/color:#fff/.test(head.style.cssText),true);
ck('and no longer uppercased like a label',/text-transform/.test(head.style.cssText),false);
ck('a rule separates it from the stats',/border-bottom:1px solid/.test(cap.style.cssText),true);

// ── 3. The grain tag disambiguates "Sep 28" ────────────────────────────────
ck('the bucket kind sits beside the date',headRow.children[1].textContent,'Day');
ck('  styled as secondary, not competing with it',/font-size:8\.5px/.test(headRow.children[1].style.cssText),true);
ctx.clnWkTip(ev,mkEl({'data-wk':'Sep 28 – Oct 4','data-grain':'Week','data-r1':R1}));
ck('a week says Week',byId['cln-wk-tip'].children[0].children[0].children[1].textContent,'Week');
// A caller that has only one grain asserts nothing — no tag is invented for it.
ctx.clnWkTip(ev,mkEl({'data-wk':'Sep 28','data-r1':R1}));
ck('no data-grain → no tag at all',byId['cln-wk-tip'].children[0].children[0].children.length,1);

// ── 4. The partial-period warning qualifies the PERIOD, so it stays with it ─
ctx.clnWkTip(ev,mkEl({'data-wk':'Oct 8','data-grain':'Day','data-partial':'1','data-pnote':'⚠ Still in progress','data-r1':R1}));
const cap2=byId['cln-wk-tip'].children[0];
ck('the warning sits inside the header block, above the rule',cap2.children.length,2);
ck('  and is the note the caller passed',cap2.children[1].textContent,'⚠ Still in progress');
ck('rows still render after it',byId['cln-wk-tip'].children.length>1,true);

// ── 5. Rebuilding clears the old header rather than stacking on it ─────────
ctx.clnWkTip(ev,mkEl({'data-wk':'Tue, Sep 29','data-grain':'Day','data-r1':R1}));
const t2=byId['cln-wk-tip'];
ck('one header after a rebuild',t2.children.filter(c=>/border-bottom/.test(c.style.cssText)).length,1);
ck('  showing the NEW date',t2.children[0].children[0].children[0].textContent,'Tue, Sep 29');

// ── 6. Both charts that share this tooltip feed it a grain ─────────────────
ck('the Trend chart sets data-grain',/data-grain="'\+esc\(g\.k==='day'\?'Day'/.test(src),true);
ck('the CPD⁄CAC chart sets it too',/data-grain="'\+\(_isDaily\?'Day':'Week'\)/.test(src),true);
ck('the Trend chart uses the headline formatter',/var head=_clnTrHead\(p\.wk,g\.k\)/.test(src),true);
ck('"Week of" is gone from both headers',/'Week of '/.test(src),false);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
