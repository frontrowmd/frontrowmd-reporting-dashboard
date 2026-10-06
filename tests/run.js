// Runs every t_*.js suite with cwd pinned to the repo root — the suites read
// dashboard.html and worker.js by relative path, so this works from anywhere.
//
// These lived in a scratch directory until 2026-10-06 and were silently wiped by
// the OS twice in two days, taking ~174 assertions with them the second time.
// That is why they are version-controlled now.
const { execFileSync } = require('child_process');
const fs = require('fs'), path = require('path');

const dir = __dirname;
const root = path.resolve(dir, '..');
// Every suite asserts against the shipped source, so a wrong cwd is a silent
// ENOENT rather than a failure. Fail loudly instead.
for (const f of ['dashboard.html', 'worker.js']) {
  if (!fs.existsSync(path.join(root, f))) {
    console.error(`run.js: cannot find ${f} in ${root} — tests/ must sit beside it.`);
    process.exit(2);
  }
}

const only = process.argv[2] || '';
const suites = fs.readdirSync(dir)
  .filter(f => /^t_.*\.js$/.test(f))
  .filter(f => !only || f.includes(only))
  .sort();

let pass = 0, fail = 0, broke = 0;
const failed = [];
for (const f of suites) {
  let out = '';
  try {
    // Capture stderr too: suites log stub warnings there, and leaving it on the
    // parent buries the summary under noise.
    out = execFileSync(process.execPath, [path.join(dir, f)],
      { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (e) {
    // A suite that throws is worse than one that fails an assertion — it means
    // the code under test did not even load.
    out = (e.stdout || '') + (e.stderr || '');
    const tail = (out.trim().split('\n').slice(-1)[0] || '').trim();
    if (!/\d+ passed, \d+ failed/.test(tail)) {
      console.log(`${f.padEnd(18)} ERRORED`);
      console.log(out.trim().split('\n').slice(0, 6).map(l => '    ' + l).join('\n'));
      broke++; failed.push(f); continue;
    }
  }
  const tail = (out.trim().split('\n').slice(-1)[0] || '').trim();
  const m = /(\d+) passed, (\d+) failed/.exec(tail);
  if (!m) { console.log(`${f.padEnd(18)} NO SUMMARY`); broke++; failed.push(f); continue; }
  const p = +m[1], fl = +m[2];
  pass += p; fail += fl;
  if (fl) { failed.push(f); console.log(`${f.padEnd(18)} ${tail}`);
    out.split('\n').filter(l => l.startsWith('FAIL')).forEach(l => console.log('    ' + l)); }
  else console.log(`${f.padEnd(18)} ${String(p).padStart(3)} passed`);
}

console.log('─'.repeat(52));
console.log(`${suites.length} suites · ${pass} passed · ${fail} failed${broke ? ` · ${broke} errored` : ''}`);
if (failed.length) { console.log('failing: ' + failed.join(', ')); process.exit(1); }
