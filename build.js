const fs=require('fs');
const w=fs.readFileSync('worker.js','utf8');
const h=fs.readFileSync('dashboard.html','utf8');
const b=Buffer.from(h).toString('base64');
let out=w.replace("const _DASHBOARD_B64 = '';","const _DASHBOARD_B64 = '"+b+"';");

// Sales dashboard (optional — skip if file doesn't exist)
if(fs.existsSync('sales-dashboard.html')){
  const sh=fs.readFileSync('sales-dashboard.html','utf8');
  const sb=Buffer.from(sh).toString('base64');
  out=out.replace("const _SALES_B64 = '';","const _SALES_B64 = '"+sb+"';");
  console.log(`  sales-dashboard: ${Math.round(fs.statSync('sales-dashboard.html').size/1024)} KB`);
}

// BD dashboard (optional — skip if file doesn't exist)
if(fs.existsSync('bd-dashboard.html')){
  const bh=fs.readFileSync('bd-dashboard.html','utf8');
  const bb=Buffer.from(bh).toString('base64');
  out=out.replace("const _BD_B64 = '';","const _BD_B64 = '"+bb+"';");
  console.log(`  bd-dashboard: ${Math.round(fs.statSync('bd-dashboard.html').size/1024)} KB`);
}

fs.mkdirSync('dist',{recursive:true});
fs.writeFileSync('dist/worker.js',out);
console.log(`✅ Built dist/worker.js (${Math.round(out.length/1024)} KB) | worker: ${Math.round(fs.statSync('worker.js').size/1024)} KB | html: ${Math.round(fs.statSync('dashboard.html').size/1024)} KB`);
