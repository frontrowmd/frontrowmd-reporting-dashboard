// Pull a real function (or declaration) out of a source file so tests exercise
// the shipped code rather than a copy of it.
const fs=require('fs');
function load(f){return fs.readFileSync(f,'utf8');}
function fn(src,name){
  let start=src.indexOf('function '+name+'(');
  if(start<0)throw new Error('missing function '+name);
  // Keep a leading `async` — slicing it off turns an async function into one
  // whose `await`s are syntax errors.
  if(src.slice(Math.max(0,start-6),start)==='async ')start-=6;
  let i=src.indexOf('{',start),depth=0,j=i;
  for(;;){const c=src[j];if(c==='{')depth++;else if(c==='}'){depth--;if(!depth)break;}j++;}
  return src.slice(start,j+1);
}
function decl(src,a,b){
  const i=src.indexOf(a); if(i<0)throw new Error('missing '+a);
  const k=src.indexOf(b,i); return src.slice(i,k+b.length);
}
module.exports={load,fn,decl};
