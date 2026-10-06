// The Fatigue key must stay true to fetchCreativeFatigue — a legend that drifts
// from its algorithm is worse than no legend.
const fs=require('fs');
let pass=0,fail=0;
function ck(n,g,w){const ok=JSON.stringify(g)===JSON.stringify(w);ok?pass++:fail++;
  console.log((ok?'PASS ':'FAIL ')+n+'  got='+JSON.stringify(g)+(ok?'':' want='+JSON.stringify(w)));}
const src=fs.readFileSync('worker.js','utf8');
const cli=fs.readFileSync('dashboard.html','utf8');
const algo=src.slice(src.indexOf('async function fetchCreativeFatigue'),src.indexOf('function buildAzResponse'));
const key=cli.slice(cli.indexOf('Creative Fatigue</div>')-900,cli.indexOf('Creative Fatigue</div>')+1800);

// The glyph is written as a \u{...} escape in the source, not as the character.
ck('the key exists',/Creative Fatigue<\/div>/.test(cli),true);
ck('it carries the same badge glyph the rows use',cli.includes('u{1F4C9} Creative Fatigue'),true);
ck('it is a fourth column',/grid-template-columns:repeat\(4,minmax\(0,1fr\)\);gap:14px/.test(cli),true);
// Thresholds must match the code.
ck('algorithm flags CTR decline > 15%',/decline > 0\.15/.test(algo),true);
ck('key states the same',/CTR drop &gt; 15%/.test(key),true);
ck('algorithm gates on impression drop < 50%',/imprDrop < 0\.5/.test(algo),true);
ck('key states the same',/impression drop &lt; 50%/.test(key),true);
ck('algorithm needs 6 days',/days\.length < 6/.test(algo),true);
ck('key states the same',/6 days of data/.test(key),true);
ck('algorithm needs 200 impressions per half',/firstImpr < 200 \|\| lastImpr < 200/.test(algo),true);
ck('key states the same',/200 impressions in <i>each<\/i> half/.test(key),true);
// 14-day trailing window, set inside the fetch and not from the page filter.
ck('algorithm uses a trailing 14 days',/setDate\(from\.getDate\(\)-13\)/.test(algo),true);
ck('the fetch takes no window argument',/async function fetchCreativeFatigue\(apiKey\)/.test(src),true);
ck('key says it ignores the Table Dates filter',/independent of the Table Dates filter/.test(key),true);
// It splits halves, so the key must not describe it as a single-period check.
ck('algorithm compares two halves',/const first = days\.filter/.test(algo)&&/const last = days\.filter/.test(algo),true);
ck('key says last 7 vs first 7',/last 7 days<\/b>.{0,80}first 7/s.test(key),true);
ck('key explains why the impression gate exists',/paused or outbid/.test(key),true);
ck('and that a missed gate means untagged, not healthy',/left untagged, not marked healthy/.test(key),true);

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
