// BD Tracker: the All Deals table, and Company Segment reaching it.
//
// The invariant that matters most here is COLUMN/CELL PARITY. The header row is
// generated from a `cols` array and the body is a hand-written run of ~45 <td>
// emissions in a parallel order. Nothing links the two, so adding a column to
// one and not the other does not fail loudly — every column to the right of the
// insertion silently shifts by one, and the table still renders.
const {load,fn}=require('./slice.js');
const src=load('worker.js'), bd=load('bd-dashboard.html');

let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── The row builder, bounded by its own loop ───────────────────────────────
// The table now lives in _bdDealsTable(rows), extracted so the Churn tab can
// render the same table over a different row set. Slice the FUNCTION, so this
// keeps working wherever it moves to.
const tableFn=(function(){
  const i=bd.indexOf('  function _bdDealsTable(rows){');
  if(i<0)return '';
  let d=0,j=bd.indexOf('{', i);
  for(;;j++){ if(bd[j]==='{')d++; else if(bd[j]==='}'){d--; if(!d)break;} }
  return bd.slice(i,j+1);
})();
ck('the table was extracted into a function', tableFn.length>2000, true);
// It must return through the scroll wrapper only — the card's own closing
// </div> belongs to whichever section opened the card, and returning it here
// would close the card twice on one page and not at all on the other.
ck('and returns through the scroll wrapper, not the card',
   /return t\+'<\/tbody><\/table><\/div>';/.test(tableFn), true);
ck('its only row input is the parameter',
   /(?<![\w.])sorted(?![\w])/.test(tableFn), false);
// ── The column list, scoped to the table function ──────────────────────────
// Bracket-matched rather than sliced to the next '];': the Churn tab added
// other {k:'…'} literals to this file, and a loose slice swept them in.
const colsSrc=(function(){
  const i=tableFn.indexOf('var cols=[');
  if(i<0)return '';
  let d=0,j=tableFn.indexOf('[', i);
  for(;;j++){ if(tableFn[j]==='[')d++; else if(tableFn[j]===']'){d--; if(!d)break;} }
  return tableFn.slice(i,j+1);
})();
const cols=(colsSrc.match(/\{k:'([^']+)'/g)||[]).map(x=>x.slice(4,-1));
ck('the column list parses', cols.length>20, true);

const rowSrc=(function(){
  const k=tableFn.indexOf('+coCell+');
  const head=tableFn.lastIndexOf('for(var ri=0;ri<rows.length;ri++)', k);
  if(head<0)return '';
  const bd=tableFn;
  const b=bd.indexOf('{', head);
  let d=0,j=b;
  for(;;j++){ if(bd[j]==='{')d++; else if(bd[j]==='}'){d--; if(!d)break;} }
  return bd.slice(b,j);
})();
ck('the row builder was located', rowSrc.length>2000, true);
// Every cell: the _td() helper for hideable columns, plus bare h+='<td ...
// The extraction renamed h+= to t+=, so match either accumulator.
const cells=(rowSrc.match(/_td\('[^']+',|[ht]\+='<td/g)||[]).length;
// THE assertion. A mismatch shifts every column right of the insertion point
// and the table still renders, so nothing else catches it.
ck('one cell emitted per declared column', cells, cols.length);

// ── Company Segment ────────────────────────────────────────────────────────
// Alex asked for it "to the right of the Company column". It is placed BEFORE
// the hideable Company ID column so it stays adjacent whether or not the ID
// columns are toggled on.
ck('Company Segment sits immediately right of Company',
   cols.slice(cols.indexOf('companyName'), cols.indexOf('companyName')+2),
   ['companyName','companySegment']);
ck('and before the hideable Company ID',
   cols.indexOf('companySegment')<cols.indexOf('companyId'), true);
ck('its cell is emitted between the two',
   rowSrc.indexOf('+coCell+')<rowSrc.indexOf('row.companySegment')
   && rowSrc.indexOf('row.companySegment')<rowSrc.indexOf("_td('companyId'"), true);

// It is a COMPANY property, so it rides the company lookup, not the deal.
ck('the worker asks HubSpot for it',
   /properties: \['name','domain','website','ops_owner','hubspot_owner_id','account_manager','company_segment'\]/.test(src), true);
ck('and stores it on the cache entry',
   /companyMap\[co\.id\] = \{ name, id: co\.id, archived: !!co\.archived, ops_owner, se_owner, account_manager, company_segment, t: Date\.now\(\) \}/.test(src), true);
ck('the deal row carries it', /companySegment: company\?\.company_segment\|\|''/.test(src), true);
ck('and the client patches it from the lookup',
   /if\('company_segment' in co\)deal\.companySegment=co\.company_segment\|\|'';/.test(bd), true);

// Entries cached before the field existed must refill. Done by marking them
// stale IN PLACE rather than bumping the cache key: a bump discards every
// resolved NAME too, blanking the Company column on every row until the cache
// refills — and that cache is capped at 3000 companies per request.
ck('old cache entries are marked stale in place',
   /if \(entry\.company_segment === undefined\) return true;/.test(src), true);
ck('and the cache key was NOT bumped',
   (src.match(/bd_company_cache_v5/g)||[]).length>=3, true);
// The staleness check must come AFTER the _unresolved early return, or a
// known-unresolvable company is retried on every single load.
ck('it does not resurrect unresolvable companies',
   src.indexOf('if (entry._unresolved) return false;')<src.indexOf('if (entry.company_segment === undefined)'), true);

// ── Wired into the table's other affordances ───────────────────────────────
ck('sortable as a string', /k==='companySegment'/.test(bd), true);
ck('searchable', /\(dd\.companySegment\|\|''\)\.toLowerCase\(\)/.test(bd), true);
ck('exportable under its HubSpot name',
   /associated_company\.company_segment/.test(bd), true);

// ── The palette ────────────────────────────────────────────────────────────
// A fallback that equals a mapped value is the recurring bug in this codebase:
// two different categories then render identically.
const segSrc=(/var BD_SEGMENT_COLORS=\{[^}]+\}/.exec(bd)||[''])[0];
const segHexes=(segSrc.match(/#[0-9A-Fa-f]{6}/g)||[]).map(h=>h.toLowerCase());
ck('every HubSpot segment value has a colour',
   ['VIP','Strategic','Enterprise','Mid-Market'].every(v=>new RegExp("'"+v+"':").test(segSrc)), true);
ck('plus a fallback', /_other:/.test(segSrc), true);
ck('and no colour is reused', new Set(segHexes).size, segHexes.length);

// ══ The Churn tab ═════════════════════════════════════════════════════════
// It renders INSTEAD of the Tracker's sections, so the Tracker's filter pills
// cannot reach it. That is the point of making it a tab rather than a fourth
// anchored section, and it is the assertion most worth keeping.
ck('the tab state exists', /var _bdTab='tracker';/.test(bd), true);
ck('only one tab is written to the DOM',
   /el\.innerHTML=\(_bdTab==='churn'\)\?hChurn:h;/.test(bd), true);
ck('and each tab builds only its own deals table',
   [/h\+=\(_bdTab==='tracker'\)\?_bdDealsTable\(sorted\):'';/.test(bd),
    /\(_bdTab==='churn'\)\?_bdDealsTable\(_chSorted\):''/.test(bd)], [true,true]);
// The Tracker's section anchors point at sections that are not in the DOM on
// the Churn tab, so they are hidden with it.
ck('the section anchors are tab-scoped', /id="bd-nav-anchors"/.test(bd), true);
ck('and hidden when the Churn tab is active',
   /an\.style\.display=\(t==='tracker'\)\?'':'none';/.test(bd), true);

// ── The churn definition, used once for the whole tab ──
// brand_status cannot identify churn: it is fully populated on closed-won deals
// too and never carries a churned value while the deal is still closed-won.
// pausechurn_date cannot either — deals that are merely PAUSED have one.
ck('churn is identified by deal stage',
   /dd\.dealstage===BD_CHURN_STAGE\|\|dd\.dealstage===BD_CHURN_ONB_STAGE/.test(bd), true);
ck('both churn stages are the ones the worker fetches',
   [/var BD_CHURN_STAGE='3453925110'/.test(bd), /BD_CHURN_ONB_STAGE='3517067985'/.test(bd)], [true,true]);
ck('and those stages are in the worker’s stage filter',
   /'3453925110'/.test(src)&&/'3517067985'/.test(src), true);
// Section viii must not reuse the Tracker's filtered rows.
ck('the churned table is built from the churn set, not sharedFiltered',
   /var _chSorted=_chDeals\.slice\(\)\.sort\(_bdSortCmp\);/.test(bd), true);
ck('and both tables share one comparator',
   /function _bdSortCmp\(a,b\)\{/.test(bd)&&/sharedFiltered\.slice\(\)\.sort\(_bdSortCmp\)/.test(bd), true);

// ── Tenure: closedate is not just blank on churned deals, it is sometimes
// wrong — stamped at churn-processing time, which yields a NEGATIVE tenure.
ck('tenure starts from first entry into closed-won',
   /var _s=dd\.entered_closedwon\|\|dd\.createdate\|\|dd\.closedate, _e=dd\.pausechurn_date;/.test(bd), true);
ck('and a negative span is excluded, not counted',
   /if\(!isFinite\(_days\)\|\|_days<0\)\{_chTenSkip\+\+;return;\}/.test(bd), true);
ck('the excluded count is reported, not hidden',
   /_chTenSkip\?\('\. '\+_chTenSkip\+' excluded/.test(bd), true);

// ── Churn Type: stored values are NOT the labels ──
const grp=(/var BD_CHURN_TYPE_GROUP=\{[\s\S]*?\n\};/.exec(bd)||[''])[0];
ck('Delinquent Payment folds in with Board Defined',
   [/'Board Defined Churn':BD_CHURN_BOARD/.test(grp), /'Delinquent Payment':BD_CHURN_BOARD/.test(grp)], [true,true]);
ck('the three Never Implemented values map to their display labels',
   [/'Yes - Rejected Product\(s\)':'Never Imp - Rejected Product\(s\)'/.test(grp),
    /'Yes - Ghosted':'Never Imp - Ghosted'/.test(grp),
    /'Yes - Other Reason':'Never Imp - Other Reason'/.test(grp)], [true,true,true]);
ck('leaving four categories', (/var BD_CHURN_TYPE_ORDER=\[([^\]]+)\]/.exec(bd)||[,''])[1].split(',').length, 4);
// A per-category churn RATE is not computable — churn type is only assigned AT
// churn, so an active brand has no category and there is no denominator. What
// is shown is each category's share of the opening book, which sums to the
// overall rate.
ck('per-category % divides the same opening book as the overall rate',
   /var pct=open>0\?\(n\/open\)\*100:null;/.test(bd), true);
ck('and says so on the card', /categories sum to the overall churn rate/.test(bd), true);

// ── Opening book: _snByMonth is month-END, so "start of month" is the
// PREVIOUS key. Off by one here and every rate is wrong by a month.
ck('the monthly denominator is the previous month’s close',
   /var _chOpen=function\(m\)\{var p=_snByMonth\[_chPrev\(m\)\];return p\?p\.b:0;\};/.test(bd), true);
ck('and a quarter opens on the month before its first',
   /return _chOpen\(a\[0\]\+'-'\+\('0'\+\(\(\+a\[1\]-1\)\*3\+1\)\)\.slice\(-2\)\);/.test(bd), true);
ck('undated churns are excluded and counted',
   /if\(!_m\)\{_chUndated\+\+;return;\}/.test(bd), true);

// ── Traffic bands. RUN the real function: the 0-is-missing rule and the
// boundaries are logic, not text.
{
  const bandsSrc=(/var BD_TRAFFIC_BANDS=\[[\s\S]*?\];/.exec(bd)||[''])[0];
  const fnSrc=(/function _chBandOf\(n\)\{[\s\S]*?\n\}/.exec(bd)||[''])[0];
  const band=new Function(bandsSrc+'\n'+fnSrc+'\nreturn _chBandOf;')();
  // A stored 0 is "never measured", not "low traffic" — 24 churned deals store
  // one and almost all never launched.
  ck('zero and blank are missing, not a band', [band(0),band(null),band(undefined),band(-5)], ['','','','']);
  ck('bands are half-open on the upper bound',
     [band(9999),band(10000),band(24999),band(25000)], ['0 - 10k','10 - 25k','10 - 25k','25 - 50k']);
  ck('the top band is open-ended', [band(499999),band(500000),band(9e9)], ['200 -500k','500k+','500k+']);
  // The reported enum really is spelled with the space BEFORE the hyphen.
  // Getting it wrong drops that band silently into the fallback.
  ck('the odd band spelling is preserved verbatim', band(300000), '200 -500k');
}

// ── Sections v and vi are count tables, deliberately ──
ck('no correlation statistic is claimed',
   /No correlation statistic is shown/.test(bd), true);
ck('"Unknown" is kept apart from "not recorded"',
   /\{k:'Unknown',l:'Unknown'\},\{k:'',l:'Not recorded'\}/.test(bd), true);
// vii must sum: a deal with no company resolved is a different gap from a
// company with no segment set.
ck('the segment split has both missing-data buckets',
   /\{k:'__nosegment',l:'No segment set'\},\{k:'__nocompany',l:'No company resolved'\}/.test(bd), true);
ck('and tells them apart by whether a company resolved',
   /dd\.companySegment\?dd\.companySegment:\(dd\.companyId\?'__nosegment':'__nocompany'\)/.test(bd), true);

// ── The three fields the worker had to start fetching ──
['ab_test','web_traffic__measured','monthly_web_traffic__reported_at_time_of_churn'].forEach(f=>{
  ck('worker fetches '+f, new RegExp("'"+f+"'").test(src), true);
  ck('and projects '+f, new RegExp(f+':').test(src), true);
});

// ── Every section states its own n. Nothing here should imply 313.
['bd-sec-churn','bd-sec-churnrate','bd-sec-churntype','bd-sec-churnreason',
 'bd-sec-churnab','bd-sec-churntraffic','bd-sec-churnseg','bd-sec-churndeals'].forEach(id=>{
  ck('section '+id+' exists', bd.indexOf('id="'+id+'"')>=0, true);
});
ck('the detail charts are scoped to where the fields are populated',
   /var BD_CHURN_DETAIL_FROM='2026-01';/.test(bd), true);

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
