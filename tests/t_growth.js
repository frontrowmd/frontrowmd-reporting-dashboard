// Growth Overview (/growth, 2026-11-24): month by month from 2026-01, Qualified
// Regular Demos and Signed Brands, each exactly as the Brand Dashboard counts
// them for a one-month window.
const fs=require('fs'),vm=require('vm');
const {load}=require('./slice.js');
let pass=0,fail=0;const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

(async()=>{
  // ── Worker ──
  const src=load(process.env.WORKER_SRC||'worker.js');
  const W={console:{log(){},warn(){},error(){}},Date,JSON,Math};vm.createContext(W);
  vm.runInContext(src.slice(0,src.indexOf('\nexport default')),W);
  const run=c=>vm.runInContext(c,W);
  ck('months run Jan 2026 to this month, the last one to today',run(`growthMonths('2026-03-17')`),[{m:'2026-01',from:'2026-01-01',to:'2026-01-31',partial:false},{m:'2026-02',from:'2026-02-01',to:'2026-02-28',partial:false},{m:'2026-03',from:'2026-03-01',to:'2026-03-17',partial:true}]);
  ck('  crosses a year end',run(`growthMonths('2027-01-02').map(x=>x.m).slice(-2)`),['2026-12','2027-01']);
  // Signed: closed won (active brands only) + churned, by Eastern close month, ids deduped.
  const sb=run(`growthSignedByMonth([
    {id:1,properties:{closedate:'2026-01-31T23:30:00Z',brand_status:'Paying'}},
    {id:2,properties:{closedate:'2026-02-01T04:30:00Z',brand_status:''}},
    {id:3,properties:{closedate:'2026-02-10T12:00:00Z',brand_status:'Churned'}},
    {id:4,properties:{closedate:'',brand_status:'Signed'}},
    {id:5,properties:{closedate:'2025-12-31T12:00:00Z',brand_status:'Signed'}}],
   [{id:6,properties:{closedate:'2026-02-11T12:00:00Z'}},{id:1,properties:{closedate:'2026-01-31T23:30:00Z'}}],'2026-01-01','2026-02-28')`);
  ck('signed: a close at 04:30 UTC Feb 1 is Jan 31 in Eastern; churned-status closed-won drops; churn stage counts; no closedate drops; deduped',sb,{'2026-01':2,'2026-02':1});
  // buildGrowth with HubSpot stubbed at the fetch-helper level.
  run(`HUBSPOT=[];todayET=()=>new Date(Date.UTC(2026,1,10));
    fetchPipelineDeals=async(t,f,to)=>{HUBSPOT.push('pipe '+f+'..'+to);return f==='2026-01-01'?[
      {id:'a',properties:{date_demo_booked:'2026-01-05',demo_attendance_status:'Demo Given (originally scheduled)',brand_status:'Paying'}},
      {id:'b',properties:{date_demo_booked:'2026-01-06',demo_attendance_status:'Demo Given (rescheduled)',average_monthly_web_traffic__cloned_:'Pre-launch / just launching'}},
      {id:'c',properties:{date_demo_booked:'2026-01-07',demo_attendance_status:'No Show'}},
      {id:'d',properties:{date_demo_booked:'2026-01-08',demo_attendance_status:'Demo Given (originally scheduled)',brand_status:'Churned'}},
      {id:'e',properties:{date_demo_booked:'2025-12-20',rescheduled_meeting_date:'2026-01-09',demo_attendance_status:'Demo Given (rescheduled)'}},
      {id:'f',properties:{date_demo_booked:'2026-01-10',rescheduled_meeting_date:'2026-02-02',demo_attendance_status:'Demo Given (rescheduled)'}}]:[]};
    fetchWebinarHeldCount=async(t,f,to)=>{HUBSPOT.push('web '+f+'..'+to);return f==='2026-01-01'?3:0;};
    fetchClosedWonDeals=async()=>[{id:9,properties:{closedate:'2026-02-03T15:00:00Z',brand_status:'Signed'}}];
    fetchChurnedByCloseDate=async()=>[{id:8,properties:{closedate:'2026-01-20T15:00:00Z'}}];`);
  let g=await run(`buildGrowth({HUBSPOT_TOKEN:'t'})`);
  ck('one pipeline fetch and one webinar count per month, the current month to today',run('HUBSPOT'),['pipe 2026-01-01..2026-01-31','web 2026-01-01..2026-01-31','pipe 2026-02-01..2026-02-10','web 2026-02-01..2026-02-10']);
  ck('Jan: held demos excl. pre-launch, churned brands and no-shows, rescheduled in counts, rescheduled out does not; + 3 webinars',[g.months[0].demos,g.months[0].webinars,g.months[0].qd],[2,3,5]);
  ck('signed per month; the current month marked partial',g.months.map(r=>[r.m,r.signed,r.partial]),[['2026-01',1,false],['2026-02',1,true]]);
  ck('a clean build is ok to cache',g.ok,true);
  run(`fetchWebinarHeldCount=async()=>null;`);
  g=await run(`buildGrowth({HUBSPOT_TOKEN:'t'})`);
  ck('a failed webinar count marks the month unavailable, never demo-only, and blocks caching',[g.months[0].demoErr,g.ok],[true,false]);
  run(`fetchWebinarHeldCount=async()=>1;`);
  // Cache: a clean build is stored; a failed build serves the last good copy.
  const kv={};const env={HUBSPOT_TOKEN:'t',CONTENT_STORE:{get:async(k)=>kv[k]?JSON.parse(kv[k]):null,put:async(k,v)=>{kv[k]=v;}}};
  W.env=env;
  g=await run(`fetchGrowth(env,false)`);
  ck('a clean build is cached and kept as the last good copy',[!!kv.growth_v1,!!kv.growth_last,!!g.cached],[true,true,false]);
  ck('  the next load is served from cache',!!(await run(`fetchGrowth(env,false)`)).cached,true);
  run(`fetchPipelineDeals=async()=>{throw new Error('HubSpot down');};`);
  g=await run(`fetchGrowth(env,true)`);
  ck('Refresh while HubSpot fails: the last good copy, marked stale',[!!g.stale,/HubSpot down/.test(g.stale&&g.stale.error),g.months.length],[true,true,2]);
  // HubSpot failing does not throw: failed months take the last good copy's figures.
  run(`fetchPipelineDeals=async()=>[];fetchWebinarHeldCount=async()=>null;fetchClosedWonDeals=async()=>{noteHsError(502,'x');return [];};`);
  g=await run(`fetchGrowth(env,true)`);
  ck('a failed build fills failed months from the last good copy, marked, and does not cache',[g.months[0].qd,!!g.months[0].old,g.months[0].signed,!!(g.stale&&g.stale.partialOnly),g.ok],[3,true,1,true,false]);
  ck('  the current month is filled too only if the copy ran to the same day',g.months[1].demoErr,undefined);
  run(`HS_ERR.n=0;`);
  ck('failures are counted on a total no other request resets',/HS_ERR\.total = \(HS_ERR\.total \|\| 0\) \+ 1/.test(src)&&/const e0 = HS_ERR\.total;/.test(src)&&/const signErr = HS_ERR\.total > e1;/.test(src),true);
  ck('route: POST /api/growth is behind authed()',/url\.pathname === '\/api\/growth'\) \{\s*let body;\s*try \{ body = await request\.json\(\); \} catch \{ return jr\(\{ error: 'Invalid JSON' \}, 400\); \}\s*if \(!\(await authed\(\)\)\) return jr/.test(src),true);
  ck('route: GET /growth serves the page; build embeds it',[/url\.pathname === '\/growth'\) \{\s*return new Response\(GROWTH_HTML/.test(src),/growth-dashboard\.html/.test(load('build.js'))],[true,true]);

  // ── Page ──
  const html=load('growth-dashboard.html');
  const js=html.slice(html.lastIndexOf('<script>')+8,html.lastIndexOf('</script>'));
  const els={};const el=id=>els[id]||(els[id]={innerHTML:'',textContent:'',style:{}});
  const P={console,localStorage:{getItem(){return null;},setItem(){}},document:{getElementById:el,addEventListener(){}},window:{innerWidth:1200,innerHeight:800}};
  vm.createContext(P);vm.runInContext(js,P);
  ck('title is Growth Overview, never "investor"',[/<title>FrontrowMD Growth Overview<\/title>/.test(html),/investor/i.test(html)],[true,false]);
  ck('first tab is Demos & Signed',vm.runInContext('G_TABS.map(t=>t.label)',P),['Demos & Signed']);
  P.G.data={asOf:'2026-11-24T12:00:00Z',months:[
    {m:'2026-01',to:'2026-01-31',qd:40,demos:30,webinars:10,signed:8},
    {m:'2026-02',to:'2026-02-28',qd:0,demos:0,webinars:null,demoErr:true,signed:5},
    {m:'2026-03',to:'2026-03-12',partial:true,qd:12,demos:10,webinars:2,signed:3}]};
  let h=P.gRenderDemos();
  ck('totals skip an unavailable month and say so',[/<tr class="tot"><td>Total<\/td><td class="big">52<\/td><td class="sub">40<\/td><td class="sub">12<\/td><td class="big">16<\/td>/.test(h),/some months n\/a/.test(h),/show as n\/a/.test(h)],[true,true,true]);
  ck('the current month is marked to date',/<tr class="part"><td>Mar 2026<\/td>/.test(h)&&/runs to 2026-03-12 \(month to date\)/.test(h),true);
  ck('an unavailable month shows n/a, not 0',/<td>Feb 2026<\/td><td class="big"><span class="na">n\/a<\/span>/.test(h),true);
  ck('chart has a column per month',(h.match(/class="band"/g)||[]).length,3);
  ck('averages use complete months only',/over 2 complete months/.test(h),true);
  P.G.data.months.forEach(r=>{r.signErr=true;});
  h=P.gRenderDemos();
  ck('a column with no month read totals n/a, not 0',/<tr class="tot"><td>Total<\/td><td class="big">52<\/td><td class="sub">40<\/td><td class="sub">12<\/td><td class="big">\u2014<\/td>/.test(h),true);
  P.G.err='Growth data unavailable';
  ck('a failed Refresh keeps the numbers on screen and says so',/Refresh failed \(Growth data unavailable\)/.test(P.gRenderDemos())&&/<td>Jan 2026<\/td>/.test(P.gRenderDemos()),true);
  P.G.data=null;P.G.err='boom';
  ck('a failed load is an error band',/Couldn’t load the numbers: boom/.test(P.gRenderDemos()),true);

  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})().catch(e=>{console.error(e);process.exit(1);});
