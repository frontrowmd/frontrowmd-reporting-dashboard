// Copies the Billing tab's W2 Retention Waterfall code (bd-dashboard.html) into
// growth-dashboard.html between its markers. Run after changing W2 on Billing:
//   node tests/sync-growth-w2.js
// t_growth.js fails when the two drift.
const fs=require('fs');const {fn}=require('./slice.js');
const W2_FNS=['_bdEsc','_bdWfDiff','_bdWfAdd','_bdWfLbl','_bdWfChurned','_bdWfChurnMonth','_bdWfIsNI',
  '_bdWfCohorts','_bdWfFill','_bdWfPill','_bdWfSeg','_bdWfW2Section','_bdWfW2Rows','bdWfW2Csv','bdWfW2Tier','bdWfW2TierAt','bdWfW2TierClear','bdWfSet'];
const W2_VARS=['var _bdWfPil=','var _bdWfW2Tier=','var _bdWfW2TierOpts=','var _bdWfQ=','var BD_WF_PAGE=','var BD_WF_W_LBL=','var BD_WF_CPM_MAX=','var BD_WF_NEUTRAL=','var BD_WF_MN=','var BD_WF_TIER_ORD=','var BD_WF_STOPS=','var _bdWfW2Out=','var _bdWfCols='];
function line(src,p){const i=src.indexOf('\n'+p);if(i<0)throw new Error('missing '+p);return src.slice(i+1,src.indexOf('\n',i+1));}
function filters(src){const i=src.indexOf('var BD_WF_FILTERS=[');return src.slice(i,src.indexOf('\n];',i)+3);}
function block(bd){return [].concat(W2_VARS.map(p=>line(bd,p)),[filters(bd)],W2_FNS.map(n=>fn(bd,n))).join('\n');}
module.exports={block,W2_FNS,W2_VARS};
if(require.main===module){
  const bd=fs.readFileSync('bd-dashboard.html','utf8'),gp='growth-dashboard.html';let g=fs.readFileSync(gp,'utf8');
  const A='// >>> copied from bd-dashboard.html (W2) — node tests/sync-growth-w2.js\n',B='\n// <<< copied from bd-dashboard.html (W2)';
  const i=g.indexOf(A),j=g.indexOf(B);if(i<0||j<0)throw new Error('markers missing in '+gp);
  g=g.slice(0,i+A.length)+block(bd)+g.slice(j);fs.writeFileSync(gp,g);console.log('synced',W2_FNS.length,'functions');
}
