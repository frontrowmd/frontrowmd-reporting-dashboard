// Change Log markers on the Trend charts. The markers bridge two things that
// were independent — the Slack-fed Change Log and the trend series — so what
// needs pinning is the JOIN: that a reaction lands on the column the chart
// actually plots it under, that Monthly stays bare, and that the marker can
// take a hover the band would otherwise swallow.
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
const mkEl=a=>{const e=new El('rect');e.attrs=a;return e;};

const C={console,Date,String,Number,Math,JSON,Array,Object,Set,Intl,parseInt,document,
         window:{innerWidth:1400,innerHeight:900}};
vm.createContext(C);
vm.runInContext([
  'var _clgCache=null;',
  decl(src,"const CLN_TR_MON=['Jan'","const CLN_TR_DOW=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];"),
  fn(src,'_clnTrLabel'), fn(src,'_clnTrEnd'), fn(src,'_clnTrHead'), fn(src,'_clgHead'),
  decl(src,"var CLG_MK_C=","var _clgMkTried=false;"),
  fn(src,'_clgPad2'), fn(src,'_clgEtDay'), fn(src,'_clgEtWeek'), fn(src,'_clgMkBucket'),
  fn(src,'_clgMkMap'), fn(src,'_clgMkWhen'), fn(src,'_clgMkSvg'),
  fn(src,'_wkTipEl'), fn(src,'clnWkTipMove'), fn(src,'clnWkTipHide'), fn(src,'clgMkTip'),
  'Object.assign(globalThis,{_clgEtDay,_clgEtWeek,_clgMkBucket,_clgMkMap,_clgMkSvg,clgMkTip,clnWkTipHide,CLG_MK_MAX});'
].join('\n'),C);
ck('the real source loaded',[typeof C._clgMkSvg,typeof C._clgEtDay],['function','function']);

// ── 1. Eastern, because that is what the series bucket on ──────────────────
// These timestamps are chosen to DISCRIMINATE, not merely to pass. This repo
// has already shipped a zone bug of exactly this shape once — etOff returning
// Pacific under an Eastern name — and a fixture at midday agrees in every US
// zone, so it would have sailed straight past that.
//
// 05:00Z on 27 Oct is 01:00 EDT on the 27th but 22:00 PDT on the 26th.
ck('the zone is Eastern, not merely American',
   C._clgEtDay(Date.UTC(2026,9,27,5,0)),'2026-10-27');
ck('  and the same instant is the previous day on the west coast',
   new Intl.DateTimeFormat('en-CA',{timeZone:'America/Los_Angeles',year:'numeric',
     month:'2-digit',day:'2-digit'}).format(new Date(Date.UTC(2026,9,27,5,0))),'2026-10-26');
// 01:00 UTC is still the previous evening in New York, whatever the browser's
// own zone is.
ck('an evening reaction keeps the Eastern day it happened on',
   C._clgEtDay(Date.UTC(2026,9,27,1,0)),'2026-10-26');
ck('  and a midday one is unremarkable',C._clgEtDay(Date.UTC(2026,9,27,16,0)),'2026-10-27');
// The offset is READ, not assumed, and each fixture kills one hardcoding:
// 03:30Z in June is 23:30 EDT on the 14th (a fixed -5 would say 22:30, still
// the 14th — so this one alone proves nothing); 04:30Z in June is 00:30 EDT on
// the 15th, which a fixed -5 would put on the 14th. January pins the winter
// offset the other way.
ck('a fixed -5 cannot produce this date',
   C._clgEtDay(Date.UTC(2026,5,15,4,30)),'2026-06-15');
ck('  a fixed -4 cannot produce this one',
   C._clgEtDay(Date.UTC(2026,0,15,4,30)),'2026-01-14');
ck('  and summer still lands where EDT puts it',
   C._clgEtDay(Date.UTC(2026,5,15,3,30)),'2026-06-14');
// The DST transitions themselves: 2026 springs forward 8 Mar and falls back
// 1 Nov, both at 06:00Z.
ck('the spring-forward boundary is on the right side',
   [C._clgEtDay(Date.UTC(2026,2,8,6,30)),C._clgEtDay(Date.UTC(2026,2,8,4,30))],
   ['2026-03-08','2026-03-07']);
// ISO weeks start Monday. Sunday must fall BACK a full six days, not forward.
ck('a week key is the ISO Monday',C._clgEtWeek(Date.UTC(2026,9,28,16,0)),'2026-10-26');
ck('  and a Sunday belongs to the week that already started',
   C._clgEtWeek(Date.UTC(2026,10,1,16,0)),'2026-10-26');
ck('  a Monday is its own week',C._clgEtWeek(Date.UTC(2026,9,26,16,0)),'2026-10-26');
ck('the bucket follows the grain',
   [C._clgMkBucket(Date.UTC(2026,9,28,16,0),'day'),
    C._clgMkBucket(Date.UTC(2026,9,28,16,0),'week')],['2026-10-28','2026-10-26']);

// ── 2. Grouping, and the Monthly exclusion the request asked for ───────────
const at=(y,m,d,h)=>Date.UTC(y,m-1,d,(h==null?16:h),0);
const E=(y,m,d,o)=>Object.assign({at:at(y,m,d),chName:'gtm',headline:'Shipped a thing',
  summary:'A thing was shipped.'},o||{});
C._clgCache={entries:[E(2026,10,26,{headline:'One'}),E(2026,10,26,{headline:'Two'}),
  E(2026,10,28,{headline:'Three',chName:'sessioninteractive'}),E(2026,10,20,{headline:'Old'})]};
ck('daily grouping keys each entry on its own day',
   Object.keys(C._clgMkMap('day')).sort(),['2026-10-20','2026-10-26','2026-10-28']);
ck('  and two on one day share a bucket',C._clgMkMap('day')['2026-10-26'].length,2);
ck('weekly grouping collapses the week onto its Monday',
   Object.keys(C._clgMkMap('week')).sort(),['2026-10-19','2026-10-26']);
ck('  with the whole week in one bucket',C._clgMkMap('week')['2026-10-26'].length,3);
// The explicit ask: Daily and Weekly only.
ck('the monthly grain carries no markers at all',
   [Object.keys(C._clgMkMap('month')).length,Object.keys(C._clgMkMap('quarter')).length],[0,0]);
ck('an entry with no timestamp is dropped rather than bucketed as 1970',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:[{chName:'gtm',headline:'No stamp'},E(2026,10,26)]};
     const m=C._clgMkMap('day'); C._clgCache=keep;
     return Object.keys(m);})(),['2026-10-26']);
ck('entries inside a bucket read oldest first',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:[E(2026,10,26,{headline:'Late',at:at(2026,10,26,22)}),
                           E(2026,10,26,{headline:'Early',at:at(2026,10,26,13)})]};
     const g=C._clgMkMap('day')['2026-10-26'].map(e=>e.headline); C._clgCache=keep;
     return g;})(),['Early','Late']);

// ── 3. The drawn layer ─────────────────────────────────────────────────────
const xAt=i=>58+i*50, padT=18, padL=58;
const PTS=[{wk:'2026-10-26'},{wk:'2026-10-27'},{wk:'2026-10-28'}];
const dayH=C._clgMkSvg(PTS,'day',xAt,padT,padL);
ck('a bucket with entries gets a marker and an empty one does not',
   [(dayH.match(/<path /g)||[]).length,(dayH.match(/data-mkhead=/g)||[]).length],[2,2]);
ck('  the marker sits on the top of the plot area',
   new RegExp('M '+xAt(0)+' '+(padT-6)+' L ').test(dayH),true);
ck('  and its hover target is centred there too',
   new RegExp('<rect x="'+(xAt(0)-9)+'" y="'+(padT-10)+'"').test(dayH),true);
ck('a crowded bucket shows the count, a single one does not',
   [/>2<\/text>/.test(dayH),/>1<\/text>/.test(dayH)],[true,false]);
ck('the hover carries every entry in the bucket',
   [/data-mk1h=/.test(dayH),/data-mk2h=/.test(dayH),/data-mkn="2"/.test(dayH)],[true,true,true]);
ck('the readout heading is the chart’s own, not a second format',
   dayH.indexOf('data-mkhead="'+C._clnTrHead('2026-10-26','day')+'"')>=0,true);
ck('a key names the diamond, once',(dayH.match(/CHANGE LOG/g)||[]).length,1);
ck('nothing is drawn, key included, when no bucket has entries',
   C._clgMkSvg([{wk:'2025-01-06'}],'day',xAt,padT,padL),'');
ck('the monthly grain draws nothing even where entries exist',
   C._clgMkSvg([{wk:'2026-10-26'}],'month',xAt,padT,padL),'');
// Weekly rows name the day inside the range; daily rows would only repeat the
// heading, so they do not.
const wkH=C._clgMkSvg([{wk:'2026-10-26'}],'week',xAt,padT,padL);
ck('a weekly row names the day inside the week',/Mon 26 Oct/.test(wkH),true);
ck('  a daily row does not repeat its own date',/Mon 26 Oct/.test(dayH),false);
// The payload surviving as TEXT is fine and expected; what must not survive is
// a raw < or a raw " , either of which would end the attribute early.
ck('markup from a headline cannot break out of the attribute',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:[E(2026,10,26,{headline:'<img src=x onerror=alert(1)> & "q"'})]};
     const h=C._clgMkSvg([{wk:'2026-10-26'}],'day',xAt,padT,padL); C._clgCache=keep;
     const v=/data-mk1h="([^"]*)"/.exec(h);
     return [!!v, v?/<img/.test(v[1]):true, v?/&lt;img/.test(v[1]):false,
             v?/&quot;q&quot;/.test(v[1]):false];})(),[true,false,true,true]);
ck('a bucket past the panel cap still reports its true size',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:Array.from({length:C.CLG_MK_MAX+4},(_,i)=>E(2026,10,26,{headline:'H'+i}))};
     const h=C._clgMkSvg([{wk:'2026-10-26'}],'day',xAt,padT,padL); C._clgCache=keep;
     return [(h.match(/data-mk\d+h=/g)||[]).length,/data-mkn="(\d+)"/.exec(h)[1]];})(),
   [require('fs').readFileSync('dashboard.html','utf8').match(/var CLG_MK_MAX=(\d+)/)[1]*1,
    String(require('fs').readFileSync('dashboard.html','utf8').match(/var CLG_MK_MAX=(\d+)/)[1]*1+4)]);

// ── 4. The hover panel ─────────────────────────────────────────────────────
const ev={clientX:400,clientY:300};
C.clgMkTip(ev,mkEl({'data-mkhead':'Mon, Oct 26','data-mkn':'2',
  'data-mk1h':'Calendar view ships','data-mk1c':'gtm',
  'data-mk2h':'Refresh button','data-mk2c':'sessioninteractive'}));
const tip=byId['cln-wk-tip'];
const texts=(function walk(e,out){out.push(e._text);(e.children||[]).forEach(c=>walk(c,out));return out;})(tip,[]);
ck('the panel leads with the date',texts.indexOf('Mon, Oct 26')>=0,true);
ck('  says how many changes',texts.indexOf('2 changes')>=0,true);
ck('  and lists each one with its channel',
   [texts.indexOf('Calendar view ships')>=0,texts.indexOf('#sessioninteractive')>=0],[true,true]);
ck('one change is not called "1 changes"',
   (()=>{C.clgMkTip(ev,mkEl({'data-mkhead':'Tue, Oct 27','data-mkn':'1','data-mk1h':'Only one','data-mk1c':'gtm'}));
     const t=(function w(e,o){o.push(e._text);(e.children||[]).forEach(c=>w(c,o));return o;})(byId['cln-wk-tip'],[]);
     return [t.indexOf('1 change')>=0,t.indexOf('1 changes')>=0];})(),[true,false]);
ck('a bucket bigger than the panel says what it is not showing',
   (()=>{const a={'data-mkhead':'Mon, Oct 26','data-mkn':'12'};
     for(let i=1;i<=C.CLG_MK_MAX;i++){a['data-mk'+i+'h']='H'+i;a['data-mk'+i+'c']='gtm';}
     C.clgMkTip(ev,mkEl(a));
     const t=(function w(e,o){o.push(e._text);(e.children||[]).forEach(c=>w(c,o));return o;})(byId['cln-wk-tip'],[]);
     return t.some(x=>/^\+5 more/.test(String(x)));})(),true);
ck('reopening the panel does not stack the previous one underneath',
   (()=>{C.clgMkTip(ev,mkEl({'data-mkhead':'A','data-mkn':'1','data-mk1':'X|gtm|'}));
     C.clgMkTip(ev,mkEl({'data-mkhead':'B','data-mkn':'1','data-mk1':'Y|gtm|'}));
     const t=(function w(e,o){o.push(e._text);(e.children||[]).forEach(c=>w(c,o));return o;})(byId['cln-wk-tip'],[]);
     return [t.indexOf('A')>=0,t.indexOf('X')>=0];})(),[false,false]);
ck('the band readout and the marker readout share one panel element',
   Object.keys(byId).filter(k=>/tip/.test(k)),['cln-wk-tip']);
// _wkTipEl builds the panel at opacity 0 and clnWkTipHide puts it back there
// after every hover, so a readout that never raises the opacity is invisible
// from the second hover onward — and one that never positions itself opens in
// the top-left corner of the window.
ck('the panel is made visible and positioned when it opens',
   (()=>{C.clnWkTipHide();
     C.clgMkTip(ev,mkEl({'data-mkhead':'Mon, Oct 26','data-mkn':'1','data-mk1h':'X','data-mk1c':'gtm'}));
     const t=byId['cln-wk-tip'];
     return [t.style.opacity,t.style.left!=='',t.style.top!==''];})(),['1',true,true]);
ck('  and raises it again after a previous hover hid it',
   (()=>{C.clnWkTipHide();const a=byId['cln-wk-tip'].style.opacity;
     C.clgMkTip(ev,mkEl({'data-mkhead':'B','data-mkn':'1','data-mk1h':'Y','data-mk1c':'gtm'}));
     return [a,byId['cln-wk-tip'].style.opacity];})(),['0','1']);
// A weekly bucket spans five days, so the row has to say which one it was.
ck('a weekly row shows the day beside the channel',
   (()=>{C.clgMkTip(ev,mkEl({'data-mkhead':'Oct 26 \u2013 Nov 1','data-mkn':'1',
     'data-mk1h':'Shipped on Wednesday','data-mk1c':'gtm','data-mk1d':'Wed 28 Oct'}));
     const t=(function w(e,o){o.push(e._text);(e.children||[]).forEach(c=>w(c,o));return o;})(byId['cln-wk-tip'],[]);
     return t.indexOf('Wed 28 Oct \u00b7 #gtm')>=0;})(),true);
// The bug this encoding exists to prevent: a pipe in a model-written headline
// used to shift the channel into the date slot and show the headline's tail as
// the channel name.
ck('a pipe in a headline survives intact, fields unshifted',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:[E(2026,10,26,{headline:'Calendar | list toggle ships',chName:'sessioninteractive'})]};
     const h=C._clgMkSvg([{wk:'2026-10-26'}],'day',xAt,padT,padL); C._clgCache=keep;
     const at1=k=>{const m=new RegExp('data-mk1'+k+'="([^"]*)"').exec(h);return m?m[1]:null;};
     C.clgMkTip(ev,mkEl({'data-mkhead':'Mon, Oct 26','data-mkn':'1',
       'data-mk1h':at1('h'),'data-mk1c':at1('c')}));
     const t=(function w(e,o){o.push(e._text);(e.children||[]).forEach(c=>w(c,o));return o;})(byId['cln-wk-tip'],[]);
     return [at1('h'),t.indexOf('Calendar | list toggle ships')>=0,t.indexOf('#sessioninteractive')>=0];})(),
   ['Calendar | list toggle ships',true,true]);

// ── 5. Wired into both charts, and only after the bands ────────────────────
const CLN=src.slice(src.indexOf('function _renderClnTrend'),src.indexOf('function _dashTrendCard'));
const DSH=src.slice(src.indexOf('function _renderDashTrend'),src.indexOf('function _renderTrueCpdWeek'));
[['Clinician',CLN,'clnwk'],['Brand',DSH,'dshwk']].forEach(([name,body,cls])=>{
  ck(name+' Trend draws the marker layer',/svg\+=_clgMkSvg\(pts,g\.k,xAt,padT,padL\);/.test(body),true);
  // A band painted over a marker swallows its hover, so order is the feature.
  ck('  after its hover bands, so the markers stay hoverable',
     body.indexOf('_clgMkSvg')>body.indexOf('class="'+cls+'-band"'),true);
  ck('  and inside the svg, not appended after it closes',
     body.indexOf('_clgMkSvg')<body.indexOf("svg+='</svg>'"),true);
  ck('  loading the entries only on the two grains that show them',
     /if\(g\.k==='day'\|\|g\.k==='week'\)_clgMkEnsure\(/.test(body),true);
});
// The third card is also titled "Trend Analysis" and was deliberately left out.
// Sliced by brace matching, not by a fixed character window: that function is
// ~8k long and its own svg-close sits past 6000, so a window would have been
// blind to exactly the insertion this assertion exists to forbid.
ck('the True CPD per Week chart is untouched',
   (()=>{const body=fn(src,'_renderTrueCpdWeek');
     return [body.length>7000,/_clgMkSvg/.test(body)];})(),[true,false]);
// One cache, so the page and the charts cannot show different history.
ck('the markers read the Change Log’s own cache, not a second copy',
   (()=>{const i=src.indexOf('function _clgMkMap'),b=src.slice(i,i+600);
     return /_clgCache&&_clgCache\.entries/.test(b)&&!/fetch\(/.test(b);})(),true);

// ── 6. The load path, RUN rather than grepped ──────────────────────────────
// The charts fetch from pages the user never asked the Change Log for, so this
// path has three ways to go wrong that a regex over the source cannot see: a
// duplicate request, a dropped redraw, and a failure that poisons a page the
// user has not visited yet. Each scenario below drives the real functions.
function loadCtx(){
  const L={console,Date,JSON,Array,Object,String,Number};
  L.posts=0; L.pending=null; L.pageRenders=0; L.pageLive=false;
  vm.createContext(L);
  vm.runInContext([
    'var _clgCache=null;',
    decl(src,"var _clgWaiters=null;","var _clgMkTried=false;"),
    // A fetch we can hold open, so "during the flight" is a state we can be in.
    'function _clgFetch(done){posts++;pending=function(res){_clgCache=res;done();};}',
    'function _clgLive(){return pageLive;}',
    'function renderChangelog(){pageRenders++;}',
    fn(src,'_clgLoadOnce'), fn(src,'_clgMkEnsure'),
    'Object.assign(globalThis,{_clgLoadOnce,_clgMkEnsure});'
  ].join('\n'),L);
  return L;
}
const OK={entries:[{at:Date.UTC(2026,9,26,16),chName:'gtm',headline:'H'}]};
const ERR={entries:[],error:'Unauthorized'};

ck('a chart with no entries loaded asks for them',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});return L.posts;})(),1);
ck('  and the redraw runs once they land',
   (()=>{const L=loadCtx();let drew=0;L._clgMkEnsure(()=>drew++);L.pending(OK);return drew;})(),1);
ck('  but not before',
   (()=>{const L=loadCtx();let drew=0;L._clgMkEnsure(()=>drew++);return drew;})(),0);
// Two charts can render inside one round trip \u2014 page to page, or the Exec
// mode toggle. A single redraw slot would leave the second one permanently bare.
ck('two charts asking during one flight share the request',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});L._clgMkEnsure(function(){});
     return L.posts;})(),1);
ck('  and BOTH get redrawn, not just the first',
   (()=>{const L=loadCtx();const drew=[];
     L._clgMkEnsure(()=>drew.push('cln'));L._clgMkEnsure(()=>drew.push('dsh'));
     L.pending(OK);return drew;})(),['cln','dsh']);
// Catching the escape matters as much as the ordering: without the guard this
// does not fail, it throws out of the fetch callback and takes the suite with it.
ck('a waiter that throws does not strand the ones behind it',
   (()=>{const L=loadCtx();const drew=[];let escaped=null;
     L._clgMkEnsure(()=>{throw new Error('boom');});L._clgMkEnsure(()=>drew.push('second'));
     try{L.pending(OK);}catch(e){escaped=e.message;}
     return [drew,escaped];})(),[['second'],null]);
// The Change Log page and the markers read one cache; before they shared a
// queue they each kept their own flag and both fired.
ck('the page and a chart asking together make ONE request, either order',
   (()=>{const A=loadCtx();A._clgLoadOnce(function(){});A._clgMkEnsure(function(){});
     const B=loadCtx();B._clgMkEnsure(function(){});B._clgLoadOnce(function(){});
     return [A.posts,B.posts];})(),[1,1]);
ck('  and the page is repainted when it was the one waiting',
   (()=>{const L=loadCtx();L.pageLive=true;L._clgMkEnsure(function(){});L.pending(OK);
     return L.pageRenders;})(),1);
ck('  but not when the user is nowhere near it',
   (()=>{const L=loadCtx();L.pageLive=false;L._clgMkEnsure(function(){});L.pending(OK);
     return L.pageRenders;})(),0);
// Once loaded, drawing a chart must not keep re-fetching.
ck('a loaded cache is never re-fetched',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});L.pending(OK);
     L._clgMkEnsure(function(){});L._clgMkEnsure(function(){});return L.posts;})(),1);
// A redraw re-enters the renderer, which calls _clgMkEnsure again. That must
// terminate on both the success and the failure path.
ck('a redraw that asks again does not loop',
   (()=>{const L=loadCtx();let n=0;
     const draw=()=>{if(++n>50)return;L._clgMkEnsure(draw);};
     draw();L.pending(OK);return [n,L.posts];})(),[2,1]);
// THE REGRESSION THIS GUARDS: _clgFetch writes a non-null cache even when it
// fails, and renderChangelog treats any non-null cache as loaded. A silent
// background failure would make the user's first ever visit to the Change Log
// show "Could not load" over an empty list, issue no request, and never retry.
ck('a failed background load does not poison the Change Log page',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});L.pending(ERR);
     return L._clgCache;})(),null);
ck('  so the page still fetches for itself afterwards',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});L.pending(ERR);
     const before=L.posts;L._clgLoadOnce(function(){});return [before,L.posts];})(),[1,2]);
ck('  while the charts stop after one background attempt, not retry forever',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});L.pending(ERR);
     L._clgMkEnsure(function(){});L._clgMkEnsure(function(){});return L.posts;})(),1);
// A failure the user DID ask for is theirs to see, so it is kept.
ck('an error with entries behind it is kept, not thrown away',
   (()=>{const L=loadCtx();L._clgMkEnsure(function(){});
     L.pending({entries:[{at:1,headline:'H'}],error:'stale'});
     return L._clgCache&&L._clgCache.error;})(),'stale');

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
