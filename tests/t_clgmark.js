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
// A browser parses a cssText assignment into properties, and the panel reads
// style.display back out to decide which way a row toggles. A stub that only
// stored the string would report undefined and invert every disclosure.
function Style(){this._t='';this.opacity='';this.left='';this.top='';
  this.display='';this.pointerEvents='';this.background='';this.transform='';}
Object.defineProperty(Style.prototype,'cssText',{get(){return this._t;},
  set(v){this._t=String(v||'');
    this._t.split(';').forEach(d=>{const i=d.indexOf(':');if(i<0)return;
      const k=d.slice(0,i).trim().replace(/-([a-z])/g,(m,c)=>c.toUpperCase());
      if(k)this[k]=d.slice(i+1).trim();});}});
function El(tag){this.tag=tag;this.children=[];this.attrs={};this.style=new Style();this._text='';}
El.prototype.appendChild=function(c){this.children.push(c);return c;};
El.prototype.removeChild=function(c){this.children=this.children.filter(x=>x!==c);return c;};
El.prototype.getAttribute=function(k){return (k in this.attrs)?this.attrs[k]:null;};
El.prototype.contains=function(n){return n===this||this.children.some(c=>c.contains&&c.contains(n));};
Object.defineProperty(El.prototype,'nodeType',{get(){return 1;}});
Object.defineProperty(El.prototype,'firstChild',{get(){return this.children[0]||null;}});
Object.defineProperty(El.prototype,'textContent',{get(){return this._text;},set(v){this._text=v;}});
Object.defineProperty(El.prototype,'offsetWidth',{get(){return 200;}});
// Height is settable so a test can make the panel GROW the way an expanded row
// grows it. A fixed height cannot tell re-clamping from clamping once.
let OH=120;
Object.defineProperty(El.prototype,'offsetHeight',{get(){return OH;}});
const byId={};
const document={createElement:t=>new El(t),getElementById:id=>byId[id]||null,body:new El('body')};
document.body.appendChild=function(c){byId[c.id]=c;El.prototype.appendChild.call(this,c);return c;};
const mkEl=a=>{const e=new El('rect');e.attrs=a;return e;};
// The pinned panel binds and unbinds document/window listeners. Recording them
// by type is what lets the pin tests fire a real outside click rather than
// asserting that a string containing "addEventListener" exists.
const LSN={};
document.addEventListener=(t,f)=>{(LSN[t]=LSN[t]||[]).push(f);};
document.removeEventListener=(t,f)=>{LSN[t]=(LSN[t]||[]).filter(x=>x!==f);};
const win={innerWidth:1400,innerHeight:900,
  addEventListener:(t,f)=>{(LSN[t]=LSN[t]||[]).push(f);},
  removeEventListener:(t,f)=>{LSN[t]=(LSN[t]||[]).filter(x=>x!==f);}};
const fire=(t,ev)=>{(LSN[t]||[]).slice().forEach(f=>f(ev));};
const bound=t=>(LSN[t]||[]).length;

const C={console,Date,String,Number,Math,JSON,Array,Object,Set,Intl,parseInt,document,
         window:win};
vm.createContext(C);
vm.runInContext([
  'var _clgCache=null;',
  decl(src,"const CLN_TR_MON=['Jan'","const CLN_TR_DOW=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];"),
  fn(src,'_clnTrLabel'), fn(src,'_clnTrEnd'), fn(src,'_clnTrHead'), fn(src,'_clgHead'),
  decl(src,"var CLG_MK_C=","var _clgMkAnchor=null;"),
  decl(src,'var _CLG_CH=','};'), decl(src,'var _CLG_CH_ALT=','];'), fn(src,'_clgColour'),
  fn(src,'_clgName'), fn(src,'_clgTime'), fn(src,'_clgSafeUrl'),
  fn(src,'_clgPad2'), fn(src,'_clgEtDay'), fn(src,'_clgEtWeek'), fn(src,'_clgMkBucket'),
  fn(src,'_clgMkMap'), fn(src,'_clgMkWhen'), fn(src,'_clgMkSvg'), fn(src,'_clgMkRows'),
  fn(src,'_clgMkClock'), fn(src,'_clgMkRow'), fn(src,'_clgMkPanel'), fn(src,'_clgMkPlace'),
  fn(src,'_wkTipEl'), fn(src,'clnWkTip'), fn(src,'clnWkTipMove'), fn(src,'clnWkTipHide'),
  fn(src,'clgMkTip'), fn(src,'clgMkPin'), fn(src,'clgMkUnpin'),
  fn(src,'_clgMkOutside'), fn(src,'_clgMkEsc'), fn(src,'_clgMkScroll'),
  'Object.assign(globalThis,{_clgEtDay,_clgEtWeek,_clgMkBucket,_clgMkMap,_clgMkSvg,_clgMkRows,'
  +'_clgMkClock,clgMkTip,clgMkPin,clgMkUnpin,clnWkTip,clnWkTipHide,CLG_MK_MAX});'
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
ck('the rect carries the bucket and the grain, so the panel can find the entries',
   [/data-mkkey="2026-10-26"/.test(dayH),/data-mkg="day"/.test(dayH),/data-mkn="2"/.test(dayH)],
   [true,true,true]);
ck('the readout heading is the chart’s own, not a second format',
   dayH.indexOf('data-mkhead="'+C._clnTrHead('2026-10-26','day')+'"')>=0,true);
ck('a key names the diamond, once',(dayH.match(/CHANGE LOG/g)||[]).length,1);
ck('nothing is drawn, key included, when no bucket has entries',
   C._clgMkSvg([{wk:'2025-01-06'}],'day',xAt,padT,padL),'');
ck('the monthly grain draws nothing even where entries exist',
   C._clgMkSvg([{wk:'2026-10-26'}],'month',xAt,padT,padL),'');
ck('a marker is both hoverable and clickable',
   [/onmouseover="clgMkTip\(event,this\)"/.test(dayH),
    /onclick="clgMkPin\(event,this\)"/.test(dayH)],[true,true]);
// Nothing a model wrote reaches the markup at all now: the rect carries a
// bucket key, a grain, a count and the chart’s own heading, every one of them
// generated. The panel reads the entries out of the cache instead, which is
// what lets it show a summary and a permalink without six free-text fields per
// row going through an escaper.
ck('no entry text reaches an attribute, however hostile the headline',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:[E(2026,10,26,{headline:'<img src=x onerror=alert(1)> & "q"',
       summary:'" onload="x',chName:'gt"m'})]};
     const h=C._clgMkSvg([{wk:'2026-10-26'}],'day',xAt,padT,padL); C._clgCache=keep;
     return [/<img/.test(h),/onerror/.test(h),/onload/.test(h),
             (h.match(/data-mk/g)||[]).length];})(),[false,false,false,4]);
ck('a bucket past the panel cap still reports its true size on the rect',
   (()=>{const keep=C._clgCache;
     C._clgCache={entries:Array.from({length:C.CLG_MK_MAX+4},(_,i)=>E(2026,10,26,{headline:'H'+i}))};
     const h=C._clgMkSvg([{wk:'2026-10-26'}],'day',xAt,padT,padL); C._clgCache=keep;
     return /data-mkn="(\d+)"/.exec(h)[1];})(),String(C.CLG_MK_MAX+4));
ck('  and CLG_MK_MAX is the source’s own, not a number copied into the test',
   C.CLG_MK_MAX,require('fs').readFileSync(process.env.DASH_SRC||'dashboard.html','utf8')
     .match(/var CLG_MK_MAX=(\d+)/)[1]*1);

// ── 4. The hover panel ─────────────────────────────────
const ev={clientX:400,clientY:300};
const TXT=e=>(function w(x,o){o.push(x._text);(x.children||[]).forEach(c=>w(c,o));return o;})(e,[]);
const tipTxt=()=>TXT(byId['cln-wk-tip']);
const setCache=ents=>{C._clgCache={entries:ents};};
// Built exactly the way _clgMkSvg builds it: the count comes off the same map
// the panel will read, so a test can never hand the panel a count it disagrees with.
const mk=(key,grain,head)=>mkEl({'data-mkhead':head||'Mon, Oct 26',
  'data-mkn':String((C._clgMkMap(grain)[key]||[]).length),'data-mkkey':key,'data-mkg':grain});

setCache([E(2026,10,26,{headline:'Calendar view ships'}),
          E(2026,10,26,{headline:'Refresh button',chName:'sessioninteractive',at:at(2026,10,26,20)})]);
C.clgMkTip(ev,mk('2026-10-26','day'));
ck('the panel leads with the date',tipTxt().indexOf('Mon, Oct 26')>=0,true);
ck('  says how many changes',tipTxt().indexOf('2 changes')>=0,true);
ck('  and lists each one with its channel',
   [tipTxt().indexOf('Calendar view ships')>=0,tipTxt().indexOf('#sessioninteractive')>=0],[true,true]);
ck('  in the order the bucket holds them, oldest first',
   (()=>{const t=tipTxt();
     return t.indexOf('Calendar view ships')<t.indexOf('Refresh button');})(),true);
ck('  and says it opens into something',tipTxt().indexOf('Click for details')>=0,true);
// The dot is the channel, as it is on the Change Log page: two changes on one
// day usually came from two different places.
ck('each row is dotted in its own channel’s colour',
   (()=>{const b=byId['cln-wk-tip'].children[1];
     return b.children.map(w=>w.children[0].children[0].style.background);})(),
   [C._clgColour('gtm'),C._clgColour('sessioninteractive')]);
ck('  which is the Change Log page’s mapping, not a second palette',
   C._clgColour('gtm'),'#72A4BF');
ck('one change is not called "1 changes"',
   (()=>{setCache([E(2026,10,27,{headline:'Only one'})]);
     C.clgMkTip(ev,mk('2026-10-27','day','Tue, Oct 27'));
     const t=tipTxt();return [t.indexOf('1 change')>=0,t.indexOf('1 changes')>=0];})(),[true,false]);
ck('a bucket bigger than the panel says what it is not showing',
   (()=>{setCache(Array.from({length:C.CLG_MK_MAX+5},(_,i)=>E(2026,10,26,{headline:'H'+i})));
     C.clgMkTip(ev,mk('2026-10-26','day'));
     const t=tipTxt();
     return [t.filter(x=>/^H\d+$/.test(String(x))).length,t.indexOf('+5 more')>=0];})(),
   [C.CLG_MK_MAX,true]);
ck('reopening the panel does not stack the previous one underneath',
   (()=>{setCache([E(2026,10,26,{headline:'X'})]);
     C.clgMkTip(ev,mk('2026-10-26','day','A'));
     setCache([E(2026,10,26,{headline:'Y'})]);
     C.clgMkTip(ev,mk('2026-10-26','day','B'));
     const t=tipTxt();return [t.indexOf('A')>=0,t.indexOf('X')>=0];})(),[false,false]);
ck('the band readout and the marker readout share one panel element',
   Object.keys(byId).filter(k=>/tip/.test(k)),['cln-wk-tip']);
// _wkTipEl builds the panel at opacity 0 and clnWkTipHide puts it back there
// after every hover, so a readout that never raises the opacity is invisible
// from the second hover onward — and one that never positions itself opens in
// the top-left corner of the window.
ck('the panel is made visible and positioned when it opens',
   (()=>{C.clnWkTipHide();
     const t=byId['cln-wk-tip']; t.style.left=''; t.style.top='';
     C.clgMkTip(ev,mk('2026-10-26','day'));
     return [t.style.opacity,t.style.left!=='',t.style.top!==''];})(),['1',true,true]);
ck('  and raises it again after a previous hover hid it',
   (()=>{C.clnWkTipHide();const a=byId['cln-wk-tip'].style.opacity;
     C.clgMkTip(ev,mk('2026-10-26','day','B'));
     return [a,byId['cln-wk-tip'].style.opacity];})(),['0','1']);
ck('  but stays untouchable while it is only hovered',
   byId['cln-wk-tip'].style.pointerEvents,'none');
// A weekly bucket spans five days, so the row has to say which one it was.
ck('a weekly row shows the day beside the channel',
   (()=>{setCache([E(2026,10,28,{headline:'Shipped on Wednesday'})]);
     C.clgMkTip(ev,mk('2026-10-26','week','Oct 26 \u2013 Nov 1'));
     return tipTxt().indexOf('Wed 28 Oct \u00b7 #gtm')>=0;})(),true);
ck('  a daily row does not repeat its own date',
   (()=>{setCache([E(2026,10,26,{headline:'Shipped'})]);
     C.clgMkTip(ev,mk('2026-10-26','day'));
     return tipTxt().some(x=>/26 Oct/.test(String(x)));})(),false);
// The bug the old encoding existed to prevent: a pipe in a model-written
// headline shifted the channel into the date slot. Nothing is packed now, but
// the headline still has to arrive whole.
ck('a pipe in a headline survives intact, fields unshifted',
   (()=>{setCache([E(2026,10,26,{headline:'Calendar | list toggle ships',
     chName:'sessioninteractive'})]);
     C.clgMkTip(ev,mk('2026-10-26','day'));
     const t=tipTxt();
     return [t.indexOf('Calendar | list toggle ships')>=0,t.indexOf('#sessioninteractive')>=0];})(),
   [true,true]);
// The cache is read at hover time, so it could in principle have been replaced
// since the marker drew. The heading and count came off the rect and are still
// true; saying so beats an empty panel under a live marker.
ck('a marker whose entries have gone still names its day and sends you somewhere',
   (()=>{setCache([E(2026,10,26,{headline:'Gone'})]);
     const el=mk('2026-10-26','day');
     setCache([]);
     C.clgMkTip(ev,el);
     const t=tipTxt();
     return [t.indexOf('Mon, Oct 26')>=0,t.some(x=>/Open the Change Log/.test(String(x)))];})(),
   [true,true]);

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

// ── 7. Pinned, and expandable ─────────────────────────────
// A hover panel is pointer-events:none and dies on mouseout, so the detail has
// to live behind a click. These run the real handlers against the stub DOM
// rather than grepping for them: a disclosure that never opens, a listener that
// is bound but never unbound, and a guard that lets a band hover overwrite a
// pinned panel all look identical in the source.
const click=()=>({clientX:400,clientY:300,stopPropagation(){}});
const body=()=>byId['cln-wk-tip'].children[1];
const rowOf=i=>body().children[i];
const FULL=[E(2026,10,26,{headline:'Calendar view ships',summary:'The Change Log calendar now runs Monday to Friday.',
              authorName:'Alex Melkonian',at:at(2026,10,26,14),permalink:'https://slack.com/x'}),
            E(2026,10,26,{headline:'Refresh button',summary:'A refresh control was added.',
              chName:'sessioninteractive',authorName:'U077TPV2PFV',at:at(2026,10,26,20),isReply:true})];

C.clgMkUnpin();
setCache(FULL);
const pinEl=mk('2026-10-26','day');
C.clgMkPin(click(),pinEl);
ck('clicking a marker pins the panel and makes it reachable',
   [byId['cln-wk-tip'].style.opacity,byId['cln-wk-tip'].style.pointerEvents],['1','auto']);
ck('  with the close control the hover panel has no use for',
   tipTxt().indexOf('\u00d7')>=0,true);
ck('  and no "click for details" prompt once you already did',
   tipTxt().indexOf('Click for details')>=0,false);
// The cap is a hover affordance, not a limit on what you can read.
ck('pinning lifts the preview cap',
   (()=>{C.clgMkUnpin();setCache(Array.from({length:C.CLG_MK_MAX+5},(_,i)=>E(2026,10,26,{headline:'H'+i})));
     const el=mk('2026-10-26','day');
     C.clgMkTip(ev,el); const hovered=tipTxt().filter(x=>/^H\d+$/.test(String(x))).length;
     C.clgMkPin(click(),el); const pinned=tipTxt().filter(x=>/^H\d+$/.test(String(x))).length;
     C.clgMkUnpin();
     return [hovered,pinned];})(),[C.CLG_MK_MAX,C.CLG_MK_MAX+5]);

setCache(FULL);
C.clgMkPin(click(),mk('2026-10-26','day'));
ck('a row starts collapsed',rowOf(0).children[1].style.display,'none');
ck('  and opens onto the full summary',
   (()=>{rowOf(0).children[0].onclick(click());
     return [rowOf(0).children[1].style.display,
             TXT(rowOf(0)).indexOf('The Change Log calendar now runs Monday to Friday.')>=0];})(),
   ['block',true]);
// The only thing on a collapsed row that says it opens. It is 9.5px of 50%
// white, so if it does not move there is nothing at all to distinguish an open
// row from a shut one but the detail itself.
ck('  and the arrow turns to say so',
   (()=>{const ch=rowOf(0).children[0].children[1];
     return [ch.textContent,ch.style.transform];})(),['\u25b8','rotate(90deg)']);
ck('  naming who logged it and when',
   TXT(rowOf(0)).map(x=>String(x).replace(/\u202f/g,' '))
     .filter(x=>/^Alex Melkonian \u00b7 /.test(x)),['Alex Melkonian \u00b7 10:00 AM ET']);
ck('  with a link out to the message itself',
   (()=>{const a=rowOf(0).children[1].children.filter(c=>c.tag==='a')[0];
     return [TXT(rowOf(0)).indexOf('open in Slack \u2197')>=0,a&&a.href,a&&a.rel];})(),
   [true,'https://slack.com/x','noopener']);
ck('  and clicking it again closes it, arrow included',
   (()=>{rowOf(0).children[0].onclick(click());
     return [rowOf(0).children[1].style.display,
             rowOf(0).children[0].children[1].style.transform];})(),['none','']);
// The clock is the one place a local zone could contradict the bucket the
// marker sits in. 20:00 ET on the 26th is the 27th in UTC and 17:00 on the
// west coast, so this fixture fails under either substitution.
ck('the detail clock reads Eastern, like the bucket',
   C._clgMkClock(at(2026,10,26,20)).replace(/\u202f/g,' '),'4:00 PM ET');
ck('  (neither UTC nor the west coast would read that hour)',
   [new Intl.DateTimeFormat('en-US',{timeZone:'America/Los_Angeles',hour:'numeric',
      minute:'2-digit'}).format(new Date(at(2026,10,26,20))).replace(/\u202f/g,' '),
    new Intl.DateTimeFormat('en-US',{timeZone:'UTC',hour:'numeric',
      minute:'2-digit'}).format(new Date(at(2026,10,26,20))).replace(/\u202f/g,' ')],
   ['1:00 PM','8:00 PM']);
// users.info can fail, and when it does authorName is a raw Slack id. Showing
// "U077TPV2PFV" is worse than showing no name at all.
ck('a raw Slack id is not passed off as an author',
   (()=>{rowOf(1).children[0].onclick(click());
     return TXT(rowOf(1)).some(x=>/U077TPV2PFV/.test(String(x)));})(),false);
ck('  though the thread marker still shows',
   TXT(rowOf(1)).some(x=>/in thread/.test(String(x))),true);
// Following the link must not also collapse the row you were reading.
ck('the Slack link does not toggle the row it sits in',
   (()=>{const det=rowOf(1).children[1];
     const a=det.children.filter(c=>c.tag==='a');
     if(a.length)return 'link should not exist here';
     return rowOf(1).children[1].style.display;})(),'block');
ck('  and where there is one, its click is stopped',
   (()=>{const det=rowOf(0).children[1];
     rowOf(0).children[0].onclick(click());          // reopen
     const a=det.children.filter(c=>c.tag==='a')[0];
     let stopped=false;
     a.onclick({stopPropagation(){stopped=true;}});
     return [stopped,rowOf(0).children[1].style.display];})(),[true,'block']);

// One element, shared with the band readout. While it is pinned nothing else
// may take it, or a mouse crossing the chart erases what the reader opened.
ck('a marker hover cannot replace a pinned panel',
   (()=>{setCache([E(2026,10,28,{headline:'Elsewhere'})]);
     C.clgMkTip(ev,mk('2026-10-28','day','Wed, Oct 28'));
     setCache(FULL);
     return tipTxt().indexOf('Elsewhere')>=0;})(),false);
ck('  nor can a band hover',
   (()=>{C.clnWkTip(ev,mkEl({'data-wk':'Mon, Oct 26','data-grain':'Day','data-r1':'Demos|12||#72A4BF|'}));
     return tipTxt().indexOf('Demos')>=0;})(),false);
ck('  nor can a mouseout hide it',
   (()=>{C.clnWkTipHide();return byId['cln-wk-tip'].style.opacity;})(),'1');

// Closing. Every route has to unbind as well as hide, or the next pin stacks a
// second outside-click handler and the panel closes on its own opening click.
ck('a click inside the panel leaves it open',
   (()=>{fire('mousedown',{target:body()});return byId['cln-wk-tip'].style.opacity;})(),'1');
ck('a click outside closes it',
   (()=>{fire('mousedown',{target:new El('div')});
     return [byId['cln-wk-tip'].style.opacity,byId['cln-wk-tip'].style.pointerEvents];})(),
   ['0','none']);
ck('  and takes its listeners with it',
   [bound('mousedown'),bound('keydown'),bound('scroll')],[0,0,0]);
ck('Escape closes it too',
   (()=>{C.clgMkPin(click(),mk('2026-10-26','day'));
     fire('keydown',{key:'a'});
     const still=byId['cln-wk-tip'].style.opacity;
     fire('keydown',{key:'Escape'});
     return [still,byId['cln-wk-tip'].style.opacity,bound('keydown')];})(),['1','0',0]);
// position:fixed, on a page that scrolls under it.
ck('scrolling the page closes it, scrolling inside it does not',
   (()=>{C.clgMkPin(click(),mk('2026-10-26','day'));
     fire('scroll',{target:body()});
     const inner=byId['cln-wk-tip'].style.opacity;
     fire('scroll',{target:document});
     return [inner,byId['cln-wk-tip'].style.opacity];})(),['1','0']);
ck('a second click on the same marker closes it',
   (()=>{const el=mk('2026-10-26','day');
     C.clgMkPin(click(),el);const open=byId['cln-wk-tip'].style.opacity;
     C.clgMkPin(click(),el);
     return [open,byId['cln-wk-tip'].style.opacity,bound('mousedown')];})(),['1','0',0]);
// A second set of outside-click listeners survives the first unpin and closes
// the NEXT panel on the very click that opens it.
ck('clicking a different marker moves the pin rather than stacking one',
   (()=>{C.clgMkPin(click(),mk('2026-10-26','day','Mon, Oct 26'));
     setCache([E(2026,10,28,{headline:'Elsewhere'})]);
     C.clgMkPin(click(),mk('2026-10-28','day','Wed, Oct 28'));
     const t=tipTxt(),n=bound('mousedown');
     C.clgMkUnpin();setCache(FULL);
     return [t.indexOf('Wed, Oct 28')>=0,t.indexOf('Mon, Oct 26')>=0,n];})(),
   [true,false,1]);
// THE REGRESSION: the chart re-renders on every grain change and every redraw,
// which destroys the rect the panel is anchored to. A panel left pinned would
// float over the new chart, anchored to an element nobody can click again.
ck('redrawing a chart releases the pin',
   (()=>{C.clgMkPin(click(),mk('2026-10-26','day'));
     const open=byId['cln-wk-tip'].style.opacity;
     C._clgMkSvg(PTS,'day',xAt,padT,padL);
     return [open,byId['cln-wk-tip'].style.opacity,bound('mousedown')];})(),['1','0',0]);
ck('  even when the redraw has nothing to draw',
   (()=>{C.clgMkPin(click(),mk('2026-10-26','day'));
     C._clgMkSvg([{wk:'2025-01-06'}],'day',xAt,padT,padL);
     return byId['cln-wk-tip'].style.opacity;})(),'0');
ck('unpinning twice is harmless',
   (()=>{C.clgMkUnpin();C.clgMkUnpin();return bound('mousedown');})(),0);
// The panel grows as rows open. Pinned low on a tall chart it would otherwise
// push its own detail off the bottom of the window.
ck('a panel too tall to sit above the click is clamped to the window',
   (()=>{setCache(FULL);OH=800;
     C.clgMkPin({clientX:400,clientY:100,stopPropagation(){}},mk('2026-10-26','day'));
     const t=parseInt(byId['cln-wk-tip'].style.top,10);
     OH=120;C.clgMkUnpin();
     return [t,t+800<=win.innerHeight];})(),[96,true]);
ck('  and re-clamped when a row opens, not left where it was pinned',
   (()=>{setCache(FULL);OH=200;
     C.clgMkPin({clientX:400,clientY:600,stopPropagation(){}},mk('2026-10-26','day'));
     const before=parseInt(byId['cln-wk-tip'].style.top,10);
     OH=820;                                  // the row just expanded
     rowOf(0).children[0].onclick(click());
     const after=parseInt(byId['cln-wk-tip'].style.top,10);
     OH=120;C.clgMkUnpin();
     return [before,after!==before,after+820<=win.innerHeight];})(),[390,true,true]);

// .href assignment EXECUTES a javascript: scheme, and an attribute escaper does
// not stop one either. Slack is the only writer of this field today, which is
// exactly the assumption worth not building on.
ck('a permalink that is not a web URL is dropped, not turned into a link',
   (()=>{setCache([E(2026,10,26,{headline:'Hostile',permalink:'javascript:alert(1)'})]);
     C.clgMkPin(click(),mk('2026-10-26','day'));
     rowOf(0).children[0].onclick(click());
     const det=rowOf(0).children[1];
     const r=[det.children.filter(c=>c.tag==='a').length,
              TXT(rowOf(0)).indexOf('open in Slack \u2197')>=0];
     C.clgMkUnpin();setCache(FULL);
     return r;})(),[0,false]);
ck('  while an http(s) one is kept, whatever its case',
   [C._clgSafeUrl('https://a.slack.com/x'),C._clgSafeUrl('HTTP://a/x'),
    C._clgSafeUrl('javascript:alert(1)'),C._clgSafeUrl(' javascript:alert(1)'),
    C._clgSafeUrl('data:text/html,x'),C._clgSafeUrl(null)],
   ['https://a.slack.com/x','HTTP://a/x','','','','']);
// The Change Log page writes the same field into the same kind of href.
ck('  and the Change Log page checks the same field the same way',
   (()=>{const b=fn(src,'_clgRow');
     return /_clgSafeUrl\(e\.permalink\)/.test(b)
         &&!/href="'\+_clgEsc\(e\.permalink\)/.test(b);})(),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
