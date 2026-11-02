// Loading banner on the Clinician and Pilot pages (2026-11-02), and the Pilot
// page re-pulling on every nav click. The ticker and both fetchers are RUN in a
// vm with a fake clock, DOM and fetch.
const {load,fn}=require('./slice.js');const vm=require('vm');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// A tiny DOM: elements by id, each holding innerHTML; the banner's three parts
// are parsed back out of that HTML so "what the reader sees" is the string.
function mkEnv(){
  const els={};
  const node=id=>els[id]||(els[id]={id,innerHTML:'',classList:{add(){},remove(){}},
    querySelector(sel){const self=this;const cls=sel.replace('.','');
      if(this.innerHTML.indexOf('class="'+cls+'"')<0)return null;
      return {set textContent(v){self.innerHTML=self.innerHTML.replace(new RegExp('(class="'+cls+'"[^>]*>)[^<]*'),'$1'+v);},
              style:{set width(v){self.innerHTML=self.innerHTML.replace(/(class="su-skel-banner-fill" style="width:)[^%]*%/,'$1'+v);}}};}});
  let timers=[],now=0;
  const env={els,node,
    clock:{tick(ms){now+=ms;for(const t of timers.slice())if(!t.dead&&t.every){while(t.at<=now){t.at+=t.every;t.f();}}else if(!t.dead&&t.at<=now){t.dead=true;t.f();}}},
    ctx:{console:{log(){},warn(){},error(){}},
      setInterval:(f,ms)=>{const t={f,every:ms,at:now+ms};timers.push(t);return t;},
      clearInterval:t=>{if(t)t.dead=true;},
      setTimeout:(f,ms)=>{const t={f,at:now+(ms||0)};timers.push(t);return t;},
      document:{getElementById:node,querySelectorAll:()=>[]},window:{}}};
  vm.createContext(env.ctx);
  return env;
}
const banner=h=>{const m=/su-skel-banner-step">([^<]*)<[\s\S]*?width:([\d.]+)%[\s\S]*?su-skel-banner-pct">(\d+)%/.exec(h);return m?[m[1],+m[2],+m[3]]:null;};

// ── 1. The ticker ──────────────────────────────────────────────────────────
{
  const E=mkEnv();
  vm.runInContext(fn(cli,'_loadBannerHTML')+fn(cli,'_loadProgress')+';globalThis._lb=_loadBannerHTML;globalThis._lp=_loadProgress;',E.ctx);
  const st=E.ctx._lp('x',['one','two','three'],1000);
  E.node('x').innerHTML=E.ctx._lb('Loading X',st);
  ck('it starts on the first step at 6%',banner(E.node('x').innerHTML),['one',6,6]);
  E.clock.tick(1000);
  ck('each tick advances a step',banner(E.node('x').innerHTML),['two',16,16]);
  E.clock.tick(30000);
  ck('it never claims more than 92% before the data lands',banner(E.node('x').innerHTML),['three',92,92]);
  // A re-rendered skeleton (navigate away and back) paints from the live state.
  E.node('x').innerHTML=E.ctx._lb('Loading X',st);
  ck('a re-drawn banner keeps the current progress',banner(E.node('x').innerHTML),['three',92,92]);
  st.done();
  ck('done() fills it to 100%',banner(E.node('x').innerHTML),['Complete!',100,100]);
  const st2=E.ctx._lp('x',['a','b'],1000);st2.stop();E.node('x').innerHTML=E.ctx._lb('L',st2);E.clock.tick(5000);
  ck('stop() freezes it',banner(E.node('x').innerHTML),['a',6,6]);
}

// ── 2. Both skeletons carry it ─────────────────────────────────────────────
ck('the Clinician skeleton leads with the banner',/let h=_loadBannerHTML\('Loading Clinician Dashboard',S\.clinician\._prog\)/.test(fn(cli,'_clinicianSkeleton')),true);
ck('the Pilot skeleton leads with the banner',/_loadBannerHTML\('Loading Pilot Dashboard',S\.pilot\._prog\)/.test(fn(cli,'_pilotSkeleton')),true);

// ── 3. The fetchers drive it, run end to end ───────────────────────────────
(async()=>{
  for(const pg of ['pilot','clinician']){
    const E=mkEnv();let resolveFetch;const renders=[];
    E.ctx.fetch=()=>new Promise(ok=>{resolveFetch=ok;});
    const id=pg==='pilot'?'pilot-c':'clinician-c';
    vm.runInContext([
      fn(cli,'_loadBannerHTML'),fn(cli,'_loadProgress'),
      pg==='pilot'?fn(cli,'_pilotSkeleton'):fn(cli,'_clinicianSkeleton'),
      pg==='pilot'?fn(cli,'fdPilot'):fn(cli,'fdClinician'),
      "const PILOT_API='/p',CLINICIAN_API='/c',PW='pw';var AP='"+pg+"';",
      "var S={pilot:{data:null},clinician:{data:null,win:'mtd'}};",
      "function _clnPage(){return AP==='clinician';}function _syncNav(){}function _pilotEsc(v){return String(v);}",
      "function renderPage(){globalThis.renders.push(document.getElementById('"+id+"').innerHTML);}",
      "globalThis.go="+(pg==='pilot'?'fdPilot':'fdClinician')+";globalThis.S=S;",
    ].join('\n'),Object.assign(E.ctx,{renders}));
    const p=E.ctx.go();
    const h0=E.node(id).innerHTML;
    ck(pg+': the banner is up while the fetch runs',[h0.indexOf('Loading '+(pg==='pilot'?'Pilot':'Clinician')+' Dashboard')>=0,banner(h0)&&banner(h0)[1]],[true,6]);
    E.clock.tick(pg==='pilot'?700:2000);
    ck(pg+': and it ticks',banner(E.node(id).innerHTML)[1],16);
    resolveFetch({ok:true,status:200,json:async()=>({meta:{}})});
    await new Promise(r=>setImmediate(r));await new Promise(r=>setImmediate(r));
    ck(pg+': the payload fills it to 100% before the page renders',banner(E.node(id).innerHTML),['Complete!',100,100]);
    ck(pg+':   and the data is not on screen yet',E.ctx.S[pg].data==null,true);
    E.clock.tick(300);await p;
    ck(pg+': then the page renders with the data',E.ctx.S[pg].data!=null,true);
  }
  // A failed load stops the ticker rather than leaving it creeping.
  {
    const E=mkEnv();
    E.ctx.fetch=async()=>({ok:false,status:500,json:async()=>({error:'boom'})});
    vm.runInContext([fn(cli,'_loadBannerHTML'),fn(cli,'_loadProgress'),fn(cli,'_pilotSkeleton'),fn(cli,'fdPilot'),
      "const PILOT_API='/p',PW='pw';var AP='pilot';var S={pilot:{data:null}};function renderPage(){}function _syncNav(){}function _pilotEsc(v){return String(v);}",
      "globalThis.go=fdPilot;globalThis.S=S;"].join('\n'),E.ctx);
    await E.ctx.go();
    const before=E.node('pilot-c').innerHTML;E.clock.tick(10000);
    ck('a failed load shows the error and nothing keeps ticking',[/Failed to load: boom/.test(before),E.node('pilot-c').innerHTML===before],[true,true]);
  }

  // ── 4. Pilot re-pulls on every nav click ─────────────────────────────────
  const sw=fn(cli,'switchDashSub');
  ck('a nav click on Pilot always fetches',/if\(AP==='pilot'\)\{fdPilot\(\);return;\}/.test(sw),true);
  ck('  with no cached-data shortcut',/if\(AP==='pilot'\)\{\s*if\(S\.pilot\.data\)/.test(sw),false);

  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
