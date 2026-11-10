// Pilot Dashboard (2026-11-02): every deal with a Pilot Waitlist Status, no
// date window, revenue = `amount` as MRR (ARR x12). The aggregation is RUN on
// fixtures; the page is rendered in a vm so what is asserted is what ships.
const {load,fn}=require('./slice.js');const vm=require('vm');
const src=load(process.env.WORKER_SRC||'worker.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

const W={console:{log(){},warn(){},error(){}}};vm.createContext(W);
vm.runInContext(src.slice(0,src.indexOf('\nexport default'))
  +'\nObject.assign(globalThis,{buildPilotDashboard,pilotDateKey,PILOT_STATUSES,PILOT_NO_STATUS});',W);
const OSR='Outreach Sent / No Response',OLR='Onboarding Link Requested',NMT='Needs More Time',NI='Not Interested';
let n=0;
const D=(status,stage,o)=>({id:String(++n),properties:Object.assign({dealname:'Deal '+n,dealstage:stage,pipeline:'default',
  pilot_waitlist_status:status,pilot_waitlist_outreach_date:'2026-10-20',createdate:'2026-09-01T15:00:00Z'},o||{})});

// ── 1. Dates ────────────────────────────────────────────────────────────────
ck('a bare date passes through',W.pilotDateKey('2026-10-20'),'2026-10-20');
ck('an ISO midnight-UTC date stays on its day (not shifted to Eastern)',W.pilotDateKey('2026-10-20T00:00:00.000Z'),'2026-10-20');
ck('  and so does epoch ms at midnight UTC',W.pilotDateKey(String(Date.UTC(2026,9,20))),'2026-10-20');
ck('blank is null',W.pilotDateKey(''),null);

// ── 2. Aggregation ──────────────────────────────────────────────────────────
const deals=[
  D(OLR,'closedwon',{amount:'2000',closedate:'2026-10-24T16:09:57Z'}),
  D(OLR,'closedwon',{amount:'3000',closedate:'2026-10-22T03:30:00Z',pilot_waitlist_outreach_date:'2026-10-22'}), // 23:30 ET on the 21st
  D(OSR,'closedwon',{amount:'4250',closedate:'2026-08-16T20:39:49Z',pilot_waitlist_outreach_date:'2026-10-29'}),
  D(OLR,'closedwon',{amount:'',closedate:'2026-10-30T17:00:00Z'}),
  D(NMT,'closedwon',{amount:'500',closedate:'2026-10-26T14:00:00Z',pilot_waitlist_outreach_date:'2026-10-26'}),   // same day: not before
  D(OSR,'1084214349',{amount:'1500',closedate:'2026-12-01T00:00:00Z'}),   // open deal: amount + forecast date
  D(OSR,'1084214349'),
  D(NMT,'3741686471',{pilot_waitlist_outreach_date:'2026-10-26'}),
  D(NI,'3453957850'),
  D(null,'appointmentscheduled',{pilot_waitlist_outreach_date:'2026-10-27'}),   // outreach date only
];
const meta={appointmentscheduled:{label:'Appointment Scheduled',order:0},'1084214349':{label:'Demo Happened',order:2},
  closedwon:{label:'Closed Won',order:7},'3453957850':{label:'No Show',order:9},'3741686471':{label:'Webinar Happened',order:12}};
const r=W.buildPilotDashboard(deals,meta);
const HIST={'5':[NMT,OLR,OSR],'8':[NMT,OSR],'7':[OSR,OSR]};   // a re-save of the same value is not a move
ck('total counts every deal',r.total,10);
ck('statuses in HubSpot order, no-status last',r.statusOrder,[OSR,OLR,NMT,NI,W.PILOT_NO_STATUS]);
ck('per-status counts',r.statusOrder.map(s=>r.byStatus[s].n),[3,3,2,1,1]);
ck('per-status Closed Won',r.statusOrder.map(s=>r.byStatus[s].won),[1,3,1,0,0]);
ck('per-status MRR counts won deals only',r.statusOrder.map(s=>r.byStatus[s].mrr),[4250,5000,500,0,0]);
ck('a blank amount is null, not $0',r.deals.find(d=>d.id==='4').mrr,null);
ck('MRR is Closed Won amounts only (an open deal\'s amount is not revenue)',r.won.mrr,9750);
ck('ARR = MRR x 12',r.won.arr,117000);
ck('Closed Won count',r.won.n,5);
ck('a blank amount is unpriced, not a $0 deal in the average',[r.won.priced,r.won.unpriced,r.won.avgMrr],[4,1,9750/4]);
ck('won before outreach is flagged and summed',[r.won.beforeOutreach.n,r.won.beforeOutreach.mrr],[2,7250]);
ck('  the Eastern close day decides it (23:30 ET on the 21st < outreach on the 22nd)',
   r.deals.filter(d=>d.wonBeforeOutreach).map(d=>d.closeDay).sort(),['2026-08-16','2026-10-21']);
ck('an open deal carries no close date',r.deals.find(d=>d.id==='6').closeDay,null);
ck('stages in pipeline order with real labels',r.stages.map(s=>s.label),['Appointment Scheduled','Demo Happened','Closed Won','No Show','Webinar Happened']);
ck('the matrix sums to each status',r.statusOrder.map(s=>Object.values(r.matrix[s]).reduce((a,b)=>a+b,0)),r.statusOrder.map(s=>r.byStatus[s].n));
ck('  and its stage columns sum to the stage totals',r.stages.map(st=>r.statusOrder.reduce((a,s)=>a+(r.matrix[s][st.id]||0),0)),r.stages.map(s=>s.n));
ck('outreach dates oldest first',r.outreach.map(o=>o.date),['2026-10-20','2026-10-22','2026-10-26','2026-10-27','2026-10-29']);
ck('  and they sum to the total',r.outreach.reduce((a,o)=>a+o.n,0),10);
ck('the outreach range',r.outreachRange,{from:'2026-10-20',to:'2026-10-29'});
// ── Waitlist funnel: reached (history) vs here now ──
{
  const f=W.buildPilotDashboard(deals,meta,HIST).funnel;
  const row=k=>{const x=f.rows.find(r=>r.key===k);return x&&[x.reached,x.here,x.fromPrevNum,x.fromPrevDen];};
  ck('funnel: the main path then the side exits',f.rows.map(r=>r.key+':'+r.kind),['outreach:path','link:path','won:path',NMT+':exit',NI+':exit']);
  ck('Outreach Sent: every deal reached it; Here now is today\'s status',row('outreach'),[10,3,null,null]);
  ck('Link Requested reached includes a deal that has since moved on',row('link'),[4,3,4,10]);
  ck('Closed Won from prior is strictly of the deals that requested a link',row('won'),[5,5,4,4]);
  ck('  and the won deal that never did is counted out loud',f.wonWithoutLink,1);
  ck('a side exit: reached from history, here from today',row(NMT),[2,2,2,10]);
  ck('a row carries the Closed Won MRR of the deals that reached it',f.rows.find(r=>r.key==='link').mrr,5500);
  ck('deals that changed status',f.moved,2);
  ck('history loaded',f.historyOk,true);
  const g=W.buildPilotDashboard(deals,meta,null).funnel;
  ck('no history: flagged, and Reached falls back to today\'s status only',[g.historyOk,g.rows.find(r=>r.key==='link').reached,g.moved],[false,3,null]);
  const ch=W.buildPilotDashboard([D(OLR,'3453925110',{amount:'900',hs_v2_date_entered_closedwon:'2026-10-25T00:00:00Z'}),D(OLR,'1084214349')],{},{}).funnel;
  ck('a churned deal reached Closed Won but is not here now',ch.rows.find(r=>r.key==='won').reached+'/'+ch.rows.find(r=>r.key==='won').here,'1/0');
  ck('  and its MRR is not counted as revenue (it churned)',[ch.rows.find(r=>r.key==='won').won,ch.rows.find(r=>r.key==='won').mrr],[0,0]);
  const ch2=W.buildPilotDashboard([D(OLR,'3453925110')],{},{}).funnel;
  ck('Churned with no stamp still reached Closed Won',ch2.rows.find(r=>r.key==='won').reached,1);
}
// A stage HubSpot's pipeline call did not return still shows, after the known ones.
const r2=W.buildPilotDashboard([D(OSR,'mystery'),D(OSR,'closedwon',{amount:'100'})],{closedwon:{label:'Closed Won',order:7}});
ck('an unmapped stage sorts last with no label',r2.stages.map(s=>[s.id,s.label]),[['closedwon','Closed Won'],['mystery',null]]);
const r3=W.buildPilotDashboard([D('Brand New Option','closedwon',{amount:'100'}),D(OLR,'closedwon')],{});
ck('a status HubSpot added later still shows, after the known ones',r3.statusOrder,[OLR,'Brand New Option']);
ck('an empty list is all zeros, not a crash',(()=>{const e=W.buildPilotDashboard([],{});return [e.total,e.won.n,e.won.avgMrr,e.outreachRange];})(),[0,0,null,null]);

// ── 3. Refusals throw; the label call never fails the page ───────────────────
(async()=>{
  const mk=fetchImpl=>{const G={console:{log(){},warn(){},error(){}},fetch:fetchImpl,setTimeout:(f)=>f()};vm.createContext(G);
    vm.runInContext(src.slice(0,src.indexOf('\nexport default'))+'\nObject.assign(globalThis,{fetchPilotDeals,fetchDealStageMeta,fetchPilotStatusHistory});',G);return G;};
  const bad=mk(async()=>({ok:false,status:500,text:async()=>'boom',json:async()=>({}),headers:{get:()=>null}}));
  let threw=false;try{await bad.fetchPilotDeals('tok');}catch(e){threw=true;}
  ck('a deal fetch HubSpot refused throws (the page says it failed, never a short list)',threw,true);
  let threw2=false;try{await bad.fetchPilotDeals('');}catch(e){threw2=true;}
  ck('  and so does a missing token',threw2,true);
  ck('a refused pipeline call is just no labels',await bad.fetchDealStageMeta('tok'),{});
  let body=null;
  const ok=mk(async(u,o)=>{body=JSON.parse(o.body);return {ok:true,status:200,json:async()=>({results:[]})};});
  await ok.fetchPilotDeals('tok');
  ck('the search ORs status and outreach date',body.filterGroups.map(g=>g.filters.map(f=>f.propertyName+' '+f.operator)),
     [['pilot_waitlist_status HAS_PROPERTY'],['pilot_waitlist_outreach_date HAS_PROPERTY']]);
  const pl=mk(async()=>({ok:true,status:200,json:async()=>({results:[{id:'default',stages:[{id:'a',label:'A',displayOrder:1}]},{id:'p2',stages:[{id:'b',label:'B',displayOrder:0}]}]})}));
  let hb=[];
  const hist=mk(async(u,o)=>{const b=JSON.parse(o.body);hb.push(b);return {ok:true,status:200,json:async()=>({results:b.inputs.map(x=>({id:x.id,propertiesWithHistory:{pilot_waitlist_status:[{value:OLR,timestamp:'2'},{value:OSR,timestamp:'1'}]}}))})};});
  const H=await hist.fetchPilotStatusHistory('tok',Array.from({length:120},(_,i)=>String(i+1)));
  ck('history is read in batches of 50',hb.map(b=>b.inputs.length),[50,50,20]);
  ck('  asking for the status history',hb[0].propertiesWithHistory,['pilot_waitlist_status']);
  ck('  and returns every value held, per deal',[Object.keys(H).length,H['7']],[120,[OLR,OSR]]);
  ck('a refused history batch returns null (the page says so), never a partial map',await bad.fetchPilotStatusHistory('tok',['1','2']),null);
  ck('stage labels carry pipeline order',await pl.fetchDealStageMeta('tok'),{a:{label:'A',order:1,pipeline:'default'},b:{label:'B',order:1000,pipeline:'p2'}});

  // ── 4. Route + nav wiring ──────────────────────────────────────────────────
  ck('the route is password-guarded',/url\.pathname === '\/api\/pilot'\) \{[\s\S]{0,200}body\.password !== env\.TEAM_PASSWORD/.test(src),true);
  const nav=cli.slice(cli.indexOf('<span>Brand Dashboard</span>'),cli.indexOf('<span>Clinician Dashboard</span>'));
  ck('the nav item sits right after Brand Dashboard',/switchDashSub\('pilot'\)[^\n]*<span>Pilot Dashboard<\/span>/.test(nav),true);
  // Hidden 2026-11-10 until Alex brings it back: the entry stays, display:none.
  ck('  and is hidden for now',/onclick="switchDashSub\('pilot'\)"[^>]*style="display:none"/.test(nav),true);
  ck('it has a page',cli.indexOf('<div class="pg" id="page-pilot"><div id="pilot-c"></div></div>')>=0,true);
  ck('switchDashSub maps it',/var apMap=\{[^}]*pilot:'pilot'/.test(cli)&&/var titleMap=\{[^}]*pilot:'Pilot Dashboard'/.test(cli),true);
  ck('renderPage routes it',/else if\(AP==='pilot'\)renderPilot\(\);/.test(cli),true);
  ck('the sidebar highlights it',/pilot:"switchDashSub\('pilot'\)"/.test(fn(cli,'_navToken')),true);
  ck('Refresh re-pulls it',/else if\(AP==='pilot'\)\{S\.pilot\.data=null;await fdPilot\(\);\}/.test(cli),true);
  ck('the filter bar has no window pills on it',/if\(AP==='pilot'\)\{[^}]*no date filter[^}]*refreshData\(\)/.test(fn(cli,'bldF')),true);
  ck('it is not inside Additional Insights',/const DASH_MENU_SUBS=\[[^\]]*'pilot'/.test(cli),false);

  // ── 5. Render ─────────────────────────────────────────────────────────────
  const pick=(a,b)=>{const i=cli.indexOf(a);if(i<0)throw new Error('missing '+a);return cli.slice(i,cli.indexOf(b,i)+b.length);};
  const el={innerHTML:''};
  const C={console:{log(){},warn(){},error(){}},document:{getElementById:()=>el,querySelectorAll:()=>[]},window:{}};
  vm.createContext(C);
  vm.runInContext([
    "const fN=v=>v==null?'—':Math.round(v).toLocaleString();",
    pick('const BRAND_STAGE_IDS=','};'),pick('const BRAND_STAGE_COLORS=','};'),
    "var STAGE_LABELS={};",fn(cli,'_kmTile'),"const _KM_LBL='',_KM_CALC='';",
    "function _clnInk(h){return h;}",
    pick('const PILOT_STATUS_COLORS=','};'),
    pick('let _pilotStageColorMap=null;','function renderPilot(){').replace(/function renderPilot\(\)\{$/,''),
    fn(cli,'renderPilot'),fn(cli,'_pilotDealCell'),fn(cli,'_pilotTable'),fn(cli,'_pilotSortD0'),pick('window.pilotSort=function','\n};'),
    "var AP='pilot';var S={pilot:{data:null,fStatus:null,fStage:null,_fetching:true}};",
    "Object.assign(globalThis,{renderPilot,S,pilotFilter:window.pilotFilter,pilotSort:window.pilotSort});",
  ].join('\n'),C);
  const evil=W.buildPilotDashboard([D(OLR,'closedwon',{dealname:'<img src=x onerror=alert(1)>',amount:'2500',closedate:'2026-10-25T15:00:00Z'})].concat(deals),meta);
  C.S.pilot.data=JSON.parse(JSON.stringify(evil));
  C.renderPilot();
  const h=el.innerHTML;
  ck('the deal name is escaped',[h.indexOf('<img src=x')<0,h.indexOf('&lt;img src=x onerror=alert(1)&gt;')>=0],[true,true]);
  ck('the headline tiles',['Waitlist Deals','Onboarding Link Requested','Closed Won','MRR','ARR'].every(t=>h.indexOf('<div class="tlbl" style="">'+t+'</div>')>=0),true);
  ck('MRR and ARR on the tiles',[h.indexOf('<div class="tval">$12,250</div>')>=0,h.indexOf('<div class="tval">$147,000</div>')>=0],[true,true]);
  ck('the before-outreach note names the count',/2 of the Closed Won \(\$7,250 MRR\) closed <b>before<\/b>/.test(h),true);
  ck('every section header',[...h.matchAll(/<div class="dsec-h dsec-in"[^>]*>([^<]+)<\/div>/g)].map(m=>m[1]),
     ['Pilot Waitlist','Waitlist Funnel','Status × Deal Stage','By Outreach Date','Closed Won Revenue','All Waitlist Deals']);
  ck('the funnel card renders its rows',['Outreach Sent','Onboarding Link Requested','Closed Won','Needs More Time','Not Interested'].every(l=>el.innerHTML.indexOf('>'+l+'</span></td><td class="n" style="font-weight:700">')>=0),true);
  ck('  and a missing history is flagged on the page',/Status history did not load/.test(h),true);
  ck('no prior-period deltas anywhere',/vs prior|vs LM|vs last month/i.test(h),false);
  // Body rows only, read off the CURRENT render (a filter re-renders el).
  const rowsIn=sec=>{const x=el.innerHTML;const i=x.indexOf('>'+sec+'</div>');const t=x.indexOf('<tbody>',i);const j=x.indexOf('</table>',t);return (x.slice(t,j).match(/<tr>/g)||[]).length;};
  ck('the deal list shows every deal',rowsIn('All Waitlist Deals'),11);
  C.pilotFilter('status',OLR);
  ck('  a status filter narrows it',rowsIn('All Waitlist Deals'),4);
  C.pilotFilter('stage','closedwon');
  ck('  and a stage filter narrows it further',rowsIn('All Waitlist Deals'),4);
  C.pilotFilter('status',OLR);C.pilotFilter('stage','');
  ck('  clicking the active status again clears it',rowsIn('All Waitlist Deals'),11);
  C.pilotFilter('stage','1084214349');
  ck('a stage filter on its own',rowsIn('All Waitlist Deals'),2);
  C.pilotFilter('stage','');
  // Review fixes (2026-11-02).
  C.pilotFilter('stage','gone-after-refresh');
  ck('a filter whose value is no longer in the data is dropped',[rowsIn('All Waitlist Deals'),C.S.pilot.fStage],[11,null]);
  ck('every heading carries a stable id, so Jump-to survives a filter re-render',
     [...el.innerHTML.matchAll(/<div class="dsec-h dsec-in"( id="dsec-pilot-[a-z]+")?>/g)].map(m=>!!m[1]),[true,true,true,true,true,true]);
  ck('a second visit during the first fetch re-shows the page',/if\(S\.pilot\._fetching\)\{[^}]*getElementById\('page-pilot'\)\.classList\.add\('active'\)/.test(fn(cli,'fdPilot')),true);
  // ── Sorting (2026-11-02) ──
  // Column texts of one table's body rows, read off the CURRENT render.
  const col=(id,ci)=>{const x=el.innerHTML;const i=x.indexOf('id="pilot-tbl-'+id+'"');const t0=x.indexOf('<tbody>',i),t1=x.indexOf('</tbody>',t0);
    return x.slice(t0,t1).split('<tr').slice(1).map(r=>{const cells=r.split('<td').slice(1);const c=cells[ci]||'';return c.replace(/<[^>]*>/g,'').replace(/^[^>]*>/,'').trim();});};
  ck('every table header is clickable',(el.innerHTML.match(/onclick="pilotSort\('(status|matrix|outreach|won|deals)',\d+\)"/g)||[]).length>=30,true);
  C.pilotSort('deals',6);
  const mrrDesc=col('deals',6);
  ck('a numeric column sorts high-to-low first, blanks last',[mrrDesc[0],mrrDesc[mrrDesc.length-1]],['$4,250','—']);
  ck('  and the header shows the arrow',/pilotSort\('deals',6\)"[^>]*>MRR ↓<\/th>/.test(el.innerHTML),true);
  C.pilotSort('deals',6);
  const mrrAsc=col('deals',6);
  ck('a second click flips it, blanks still last',[mrrAsc[0],mrrAsc[mrrAsc.length-1]],['$500','—']);
  C.pilotSort('deals',2);
  ck('deal stage sorts in pipeline order, not A-Z',[...new Set(col('deals',2))],['Appointment Scheduled','Demo Happened','Closed Won','No Show','Webinar Happened']);
  C.pilotFilter('status',NMT);
  ck('a sort survives a filter',col('deals',2),['Closed Won','Webinar Happened']);
  C.pilotFilter('status',NMT);
  C.pilotSort('matrix',6);
  ck('a summary table sorts, with Total pinned last',col('matrix',0),['Onboarding Link Requested','Outreach Sent / No Response','Needs More Time','Not Interested','(No status)','Total']);
  C.pilotSort('matrix',0);
  ck('a text column starts A-Z',col('matrix',0).slice(0,2),['(No status)','Needs More Time']);
  ck('the funnel does not sort (it reads top to bottom)',/pilotSort\('status',/.test(el.innerHTML),false);
  C.pilotSort('outreach',0);
  ck('the date column sorts by date',col('outreach',0),['Oct 20, 2026','Oct 22, 2026','Oct 26, 2026','Oct 27, 2026','Oct 29, 2026']);
  C.S.pilot.sort={};C.renderPilot();
  ck('the Closed Won table lists the won deals',rowsIn('Closed Won Revenue'),6);

  console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
