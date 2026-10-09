// Ad tables, clinician channel. Two changes: the Campaigns table drops
// Inquiries and CPI, and every table's CPD header reads "Cost Per Conversion"
// on the clinician channel — where the figure counts conversions, not demos.
const {load}=require('./slice.js');
const cli=load(process.env.DASH_SRC||'dashboard.html');
let pass=0,fail=0;
const ck=(n,g,w)=>{const ok=JSON.stringify(g)===JSON.stringify(w);console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));ok?pass++:fail++;};

// ── 1. CPD is renamed, per channel, in one place ──────────────────────────
ck('the label is decided once',
   /var _cpdLbl=function\(\)\{return _clnAdCols\?'Cost Per Conversion':'CPD';\};/.test(cli),true);
ck('  and every ad table uses it',(cli.match(/_irfanSortableTh\(_cpdLbl\(\)/g)||[]).length,4);
ck('  covering ad sets, campaigns, creatives and audiences',
   [...cli.matchAll(/_irfanSortableTh\(_cpdLbl\(\),'cpd',(\w+)/g)].map(m=>m[1]),
   ['sortSt','campSort','creSort','audSort']);
// Brand channels are untouched: the sort key stays 'cpd' so saved sort state
// and _irfanSortRows keep working whichever label is showing.
ck('the sort key is unchanged',(cli.match(/_cpdLbl\(\),'cpd'/g)||[]).length,4);
ck('no bare CPD header survives in the ad tables',
   /_irfanSortableTh\('CPD'/.test(cli),false);

// ── 2. Inquiries and CPI leave the Campaigns table ────────────────────────
ck('the Inquiries header is gone',/_irfanSortableTh\('Inquiries'/.test(cli),false);
ck('the CPI header is gone',/_irfanSortableTh\('CPI'/.test(cli),false);
ck('  and so are their cells',/c\.inquiries!=null\)\?c\.inquiries:null/.test(cli),false);
ck('  and their totals',/_clnTotInqCell\(campRows\)|_clnTotCpiCell\(campRows\)/.test(cli),false);
ck('  and the heat scales that fed them',/const cInq=|const cCpi=/.test(cli),false);
// These only ever fed the two removed cells.
ck('  and the dead locals went with them',/_unavail|_dashTip/.test(cli),false);

// ── 3. The column count still adds up ─────────────────────────────────────
// Header, body row and totals row must agree per channel, or every cell after
// the gap lands under the wrong heading. Cells are not all literal <td>:
// _statusCells() emits the Status Date and Status Days pair, so it counts as 2.
const campBlk=(()=>{const i=cli.indexOf("let campH='';");
  return cli.slice(i,cli.indexOf("campH+='</tbody></table></div>';",i));})();
const hdrEnd=campBlk.indexOf("campH+='</tr></thead><tbody>';");
const totStart=campBlk.indexOf('// Totals row');
const cells=l=>(l.match(/<td\b/g)||[]).length
             +(l.match(/<th\b/g)||[]).length
             +(l.match(/_irfanSortableTh\(/g)||[]).length
             +(l.match(/_statusCells\(/g)||[]).length*2;
const tally=txt=>{let a=0,c=0,b=0;
  for(const l of txt.split('\n')){
    if(/colspan=/.test(l))continue;              // the empty-state row spans, not fills
    if(/_campColN=/.test(l))continue;            // a regex literal, not markup
    const n=cells(l); if(!n)continue;
    if(/if\(_clnAdCols\)/.test(l))c+=n;
    else if(/if\(!_clnAdCols\)/.test(l)||/\(_clnAdCols\?'':'<td/.test(l))b+=n;
    else a+=n;
  }
  return {clinician:a+c, brand:a+b};};
const H=tally(campBlk.slice(0,hdrEnd));
const B=tally(campBlk.slice(hdrEnd,totStart));
const T=tally(campBlk.slice(totStart));
ck('the counter sees the Status Date / Status Days pair',
   /campH\+=_statusCells\(/.test(campBlk)&&cells('x_statusCells(y)')===2,true);
ck('header, body and totals agree on the clinician channel',
   [H.clinician,B.clinician,T.clinician],[15,15,15]);
ck('  and on the brand channels',[H.brand,B.brand,T.brand],[16,16,16]);
// what this change did.
ck('the empty-state colspan is counted off the header',
   /const _campColN=\(campH\.match\(\/<th\\b\/g\)\|\|\[\]\)\.length;/.test(cli),true);
ck('  and used instead of a hand-kept number',
   /colspan="'\+_campColN\+'"/.test(cli),true);
ck('  with no literal left behind',/_clnAdCols\?17:16/.test(cli),false);

// ── 4. Qual is still the brand-only column it was ─────────────────────────
ck('Qual stays off the clinician tables',
   /if\(!_clnAdCols\)campH\+=_irfanSortableTh\('Qual','qualified'/.test(cli),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
