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

// ── The column list ────────────────────────────────────────────────────────
const colsSrc=(function(){
  const i=bd.indexOf('  var cols=[');
  return bd.slice(i, bd.indexOf('\n  ];', i));
})();
const cols=(colsSrc.match(/\{k:'([^']+)'/g)||[]).map(x=>x.slice(4,-1));
ck('the column list parses', cols.length>20, true);

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

console.log(pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
