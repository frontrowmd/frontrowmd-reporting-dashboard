// Pull a real function (or declaration) out of a source file so tests exercise
// the shipped code rather than a copy of it.
const fs=require('fs');
function load(f){return fs.readFileSync(f,'utf8');}
function fn(src,name){
  let start=src.indexOf('function '+name+'(');
  // Function EXPRESSIONS (`var f=function(){}`) are as common in these files as
  // declarations, and a test should not have to know which form a helper was
  // written in. The slice keeps the `var` so the result is a statement that can
  // be dropped straight into a vm context.
  if(start<0){
    const m=new RegExp('(?:^|[^\\w$.])((?:var|let|const)\\s+)?'+
      name.replace(/[$]/g,'\\$')+'\\s*=\\s*(?:async\\s+)?function\\s*\\(')
      .exec(src);
    if(m){
      start=m.index+(/^[\w$.]/.test(m[0])?0:1);
      const sliced=sliceBraces(src,start);
      // Without the `var` an assignment to an undeclared name throws in strict
      // mode and silently leaks a global otherwise.
      return m[1]?sliced:('var '+sliced);
    }
    throw new Error('missing function '+name);
  }
  // Keep a leading `async` — slicing it off turns an async function into one
  // whose `await`s are syntax errors.
  if(src.slice(Math.max(0,start-6),start)==='async ')start-=6;
  return sliceBraces(src,start);
}
function sliceBraces(src,start){
  let i=src.indexOf('{',start),depth=0,j=i;
  for(;;){const c=src[j];if(c==='{')depth++;else if(c==='}'){depth--;if(!depth)break;}j++;}
  return src.slice(start,j+1);
}
function decl(src,a,b){
  const i=src.indexOf(a); if(i<0)throw new Error('missing '+a);
  const k=src.indexOf(b,i); return src.slice(i,k+b.length);
}
module.exports={load,fn,decl};
