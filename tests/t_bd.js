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
   /an\.style\.display=\(t==='tracker'\)\?'contents':'none';/.test(bd), true);
// The Churn tab has its own section menu. display must be `contents`, not the
// browser default: these <a>s are direct flex children of the nav, and a block
// wrapper would pull them out of its layout.
ck('the Churn tab has its own anchors', /id="bd-nav-churn"/.test(bd), true);
ck('and they are toggled with it',
   /cn\.style\.display=\(t==='churn'\)\?'contents':'none';/.test(bd), true);
// Every churn section must be reachable from that menu, or the menu lies.
{
  // Bounded by the span's own close — the flex spacer that used to bound this
  // appears earlier in the document, so the slice came back empty and the
  // assertion passed vacuously on an empty menu.
  // Bounded by the LAST anchor in the group. The first '</span>' after the
  // opening tag closes the divider nested immediately inside it, which made
  // this slice ~40 chars and the assertion below pass on an empty menu.
  const navStart=bd.indexOf('id="bd-nav-churn"');
  const navZone=bd.slice(navStart, bd.indexOf('Churned Deals</a>', navStart));
  ck('the churn menu slice is not empty', navZone.length>200, true);
  const linked=(navZone.match(/data-sec="(bd-sec-churn[a-z]*)"/g)||[]).map(x=>x.slice(10,-1));
  const built=['bd-sec-churn','bd-sec-churnrate','bd-sec-churntype','bd-sec-churnreason',
               'bd-sec-churnab','bd-sec-churntraffic','bd-sec-churnseg','bd-sec-churndeals'];
  ck('every churn section is in the menu', built.filter(x=>linked.indexOf(x)<0), []);
  ck('and the menu points at nothing that is not built', linked.filter(x=>built.indexOf(x)<0), []);
}

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
   /var _chTableRows=_chDeals\.filter\(/.test(bd)&&!/sharedFiltered/.test(bd.slice(bd.indexOf('var _chTableRows='),bd.indexOf('var _chSorted='))), true);
ck('and sorted with the shared comparator',
   /var _chSorted=_chTableRows\.slice\(\)\.sort\(_bdSortCmp\);/.test(bd), true);
// ── The C8 churn-date filter ──
// It scopes the TABLE only. The charts above carry their own period axis and
// their own stated n; a filter that silently moved them would make every one of
// those n values a lie.
ck('the range filter exists', /function bdSetChurnRange\(\)\{/.test(bd), true);
ck('and both bounds are inclusive',
   /if\(_bdChurnFrom&&d<_bdChurnFrom\)return false;/.test(bd)&&/if\(_bdChurnTo&&d>_bdChurnTo\)return false;/.test(bd), true);
// An undated churn cannot be said to fall inside a range, so it drops out once
// one is set — keeping it would make the count disagree with the dates shown.
ck('an undated churn drops out when a range is set',
   /var d=\(dd\.pausechurn_date\|\|''\)\.substring\(0,10\);\s*\n\s*if\(!d\)return false;/.test(bd), true);
ck('but everything shows when no range is set',
   /if\(!_bdChurnFrom&&!_bdChurnTo\)return true;/.test(bd), true);
ck('an inverted range is refused', /End date is before the start date/.test(bd), true);
ck('the header says how many of how many', /_chTableRows\.length\+' of '\+_chDeals\.length\+' churned deals in range'/.test(bd), true);
ck('and the control says what it scopes', /filters this table only/.test(bd), true);
// The charts must NOT read the range.
{
  const chartZone=bd.slice(bd.indexOf('var _chTypeChart='), bd.indexOf('var _chTableRows='));
  ck('no chart reads the table range', /_bdChurnFrom|_bdChurnTo/.test(chartZone), false);
}
ck('and both tables share one comparator',
   /function _bdSortCmp\(a,b\)\{/.test(bd)&&/sharedFiltered\.slice\(\)\.sort\(_bdSortCmp\)/.test(bd), true);

// ── Tenure: closedate is not just blank on churned deals, it is sometimes
// wrong — stamped at churn-processing time, which yields a NEGATIVE tenure.
ck('tenure starts from first entry into closed-won',
   /var _s=dd\.entered_closedwon\|\|dd\.createdate\|\|dd\.closedate, _e=dd\.pausechurn_date;/.test(bd), true);
// The tenure calc moved into _kmTenure(list) when C1 gained its quarter split,
// so the skip counter is per-list now rather than one module-level tally.
ck('and a negative span is excluded, not counted',
   /if\(!isFinite\(_days\)\|\|_days<0\)\{skip\+\+;return;\}/.test(bd), true);
ck('  computed once, for the headline and every quarter',
   (bd.match(/var _kmTenure=function\(list\)/g)||[]).length,1);
ck('the excluded count is reported, not hidden',
   /_kmTenAll\.skip\?\('\. '\+_kmTenAll\.skip\+' excluded/.test(bd), true);

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
// The sub-label under each column printed the period's churn as a share of the
// opening book — the same number C2 already charts. Unlabelled under a column
// of counts it read as "2.54% of what?" and answered nothing.
ck('no unexplained sub-label under the columns', /subOf:function\(k,total\)/.test(bd), false);
// C3 and C4 are CLUSTERED charts, not tables and not stacks: a stack answers
// "how much churn this month", a cluster answers "which category moved".
ck('churn by type is a clustered chart', /var t=bdClusterChart\(periods,cats,counts,\{/.test(bd), true);
ck('churn by reason is too', /var t=bdClusterChart\(periods,cats,counts,\{labelOf:_chPeriodLbl,ht:330\}\);/.test(bd), true);
ck('the cluster renderer exists', /function bdClusterChart\(periods,cats,counts,opts\)\{/.test(bd), true);
ck('and nothing still calls the stacked one', /bdStackChart/.test(bd), false);
// Clustered bars sit side by side, so an axis scaled to the period TOTAL would
// leave every bar squat. It must reach the tallest single bar.
ck('the axis reaches the tallest single bar, not the period total',
   /var maxV=1;periods\.forEach\(function\(k\)\{cats\.forEach\(function\(c\)\{var n=\(counts\[c\.k\]&&counts\[c\.k\]\[k\]\)\|\|0;if\(n>maxV\)maxV=n;\}\);\}\);/.test(bd), true);
// A zero still gets a hover target, or the reader cannot tell "no churn in this
// category" from "this category is not on the chart".
ck('a zero category still renders a target',
   /var hgt=n\?Math\.max\(2,\(n\/top\)\*ph\):2;/.test(bd), true);
// Its axis must be whole units — a count axis labelled 2.5 is wrong.
ck('the count axis steps in whole units',
   /var step=Math\.max\(1,Math\.ceil\(maxV\/4\)\),top=step\*4;/.test(bd), true);
// Hover must be re-bound after every render, or it works once and then stops.
ck('the hover is re-bound on every render', /bdBindStack\(\);/.test(bd), true);
// The rolled-up tail must not borrow a real category's colour.
{
  const reasonPal=(/var BD_CHURN_REASON_COLORS=\[([^\]]+)\]/.exec(bd)||[,''])[1].match(/#[0-9A-Fa-f]{6}/g)||[];
  const otherC=(/var BD_CHURN_OTHER_C='(#[0-9A-Fa-f]{6})'/.exec(bd)||[,''])[1];
  ck('six distinct reason colours', [reasonPal.length, new Set(reasonPal.map(h=>h.toLowerCase())).size], [6,6]);
  ck('and Other is none of them', reasonPal.map(h=>h.toLowerCase()).indexOf(otherC.toLowerCase()), -1);
}

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

// ── C2 rendered ~650px tall, straight through the sections below it ────────
// buildSVGChart's default wrapper is `flex:1;min-height:180px`, which only
// resolves inside a flex-column parent with a bounded height. In a plain div
// the SVG falls back to its intrinsic viewBox ratio and grows without limit.
// aspect:true makes the wrapper self-sizing and removes the dependency.
{
  const wrapSrc=bd.slice(bd.indexOf('var _wrapStyle=opts.aspect'), bd.indexOf('var _wrapStyle=opts.aspect')+230);
  ck('the non-aspect wrapper still needs a flex parent', /flex:1;min-height:180px/.test(wrapSrc), true);
  // So every chart on the Churn tab — which uses plain divs — must pass it.
  const churnZone=bd.slice(bd.indexOf('var _chDeals=deals.filter'), bd.indexOf('bdBindSpy();'));
  const calls=churnZone.match(/buildSVGChart\([^;]*?\)/gs)||[];
  ck('the churn tab charts', calls.length>=1, true);
  ck('and every one of them is aspect-sized', calls.filter(c=>!/aspect:true/.test(c)), []);
}

// ── A rate needs a denominator big enough that one churn does not swing it ──
// The first months held a handful of brands, so a single churn printed 50% and
// flattened every later month onto the axis. That spike was an artifact of the
// denominator, not a signal.
ck('thin-book periods are not plotted',
   /if\(_o<BD_CHURN_RATE_MIN_BOOK\)\{if\(_chByMonth\[m\]\)_chSkipThin\+\+;return;\}/.test(bd), true);
ck('the floor is declared', /var BD_CHURN_RATE_MIN_BOOK=(\d+);/.test(bd), true);
ck('and is high enough that one churn moves the rate by under 5pp',
   100/(+(/var BD_CHURN_RATE_MIN_BOOK=(\d+);/.exec(bd)||[,0])[1]) < 5, true);
// Hiding months is only acceptable if the chart says how much churn it hides.
ck('the hidden churn is counted and reported',
   /That hides '\+_chSkipThin\+' early month/.test(bd), true);

// ── The page switcher ──
ck('the two tabs are one segmented control', /<span class="bd-tabs" role="tablist">/.test(bd), true);
ck('and are ruled off from the section anchors', /class="bd-navdiv"/.test(bd), true);
ck('the active tab is announced, not just coloured',
   /b\.setAttribute\('aria-selected',on\?'true':'false'\);/.test(bd), true);

// ── C2's axis and hover ───────────────────────────────────────────────────
// A percentage axis labelled 0,1,2 reads as a count, and over a 0-6% range
// rounds most gridlines onto the same integers.
ck('the rate axis carries its unit', /ySuffix:'%',yDec:1/.test(bd), true);
ck('buildSVGChart honours it',
   /\(\(opts\.yDec\?gv\.toFixed\(opts\.yDec\):Math\.round\(gv\)\)\+\(opts\.ySuffix\|\|''\)\)/.test(bd), true);
// tipPrefix was concatenated straight onto the label — "Churn rate19 churned
// ÷ 514 active" with nothing between them.
ck('a tip prefix gets a separator',
   /var _d=tipPrefix\?\(tipPrefix\+' \\u00b7 '\+_lbl\):_lbl;/.test(bd), true);
// tipNote is one string for the whole chart and cannot describe a single point.
ck('per-point notes are supported',
   /var _note=\(opts\.tipNotes&&opts\.tipNotes\[di\]!=null\)\?opts\.tipNotes\[di\]:\(opts\.tipNote\|\|''\);/.test(bd), true);
ck('and the rate chart uses them, with the period as the header',
   /tipLabels:_chRateTip,tipNotes:_chRateNote/.test(bd), true);
ck('so it no longer passes a prefix',
   /buildSVGChart\(_chRateLbl,_chRateVal,BD_CHURN_C,'%',false,'',\{/.test(bd), true);

// ── C5/C6 grid ────────────────────────────────────────────────────────────
// Left to size itself the browser gave every shaded cell a different width, so
// the fill read as floating blocks rather than a grid.
// Scoped to the matrix builder. Unscoped, `table-layout:fixed` matched the
// pre-existing .stbl CSS rule and passed whether or not this table set it.
{
  const mx=bd.slice(bd.indexOf('var _chMatrix=function('), bd.indexOf('var _chMatrix=function(')+2200);
  // Matched inside the STYLE STRING, not as a bare token: the comment directly
  // above this code says "table-layout:fixed" too, so the loose pattern passed
  // on the prose whether or not the table set it.
  ck('the matrix table sets a fixed layout',
     /border-collapse:separate;border-spacing:0;table-layout:fixed/.test(mx), true);
  ck('and sizes its label and total columns',
     /<colgroup><col style="width:168px">.*<col style="width:68px"><\/colgroup>/.test(mx), true);
  // Fixed layout without a min-width collapses every column at narrow widths.
  ck('with a floor before it starts scrolling', /min-width:620px/.test(mx), true);
}

// ── C8 ────────────────────────────────────────────────────────────────────
// The table draws its own header rule immediately below, so the filter bar's
// border made two lines a pixel apart.
{
  const filterBar=bd.slice(bd.indexOf("id=\"bd-churn-from\"")-900, bd.indexOf("id=\"bd-churn-from\""));
  ck('the filter bar draws no rule of its own', /border-bottom/.test(filterBar), false);
}

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
