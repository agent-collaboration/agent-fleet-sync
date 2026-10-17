import {Miniflare,Log,LogLevel} from 'miniflare';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {resolve,dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import assert from 'node:assert/strict';

const fixture=resolve(process.argv[2]);
const manifest=JSON.parse(await readFile(fixture,'utf8'));
const root=resolve(import.meta.dirname);
const evidence=resolve(root,'evidence',process.argv[3]??'matrix-01');await mkdir(evidence,{recursive:true});
const options={host:'127.0.0.1',port:0,log:new Log(LogLevel.WARN),modules:true,
 modulesRules:[{type:'ESModule',include:['**/*.js','**/*.mjs']},{type:'CompiledWasm',include:['**/*.wasm']}],
 scriptPath:resolve(root,'build/shim.mjs'),compatibilityDate:'2026-07-29',
 durableObjects:{PROBE:{className:'ProbeObject',useSQLite:true}},durableObjectsPersist:resolve(evidence,'state'),cf:false};
let runtime=new Miniflare(options);const results=[];
async function call(name,body){
 const response=await runtime.dispatchFetch('http://normal.local/',{method:'POST',headers:{'Content-Type':'application/json','X-Probe-Object':name},body,signal:AbortSignal.timeout(30000)});
 const text=await response.text();
 if(response.status!==200)throw Error(`HTTP${response.status}: ${text}`);
 return JSON.parse(text);
}
function normalize(rows){return rows.map(values=>values.map(value=>JSON.parse(value)));}
async function inspect(name,checkpoint){
 const result=await call(name,JSON.stringify({action:'inspect',queries:checkpoint.queries}));
 assert.deepEqual(normalize(result.results),checkpoint.expected);
 return {name:checkpoint.name,matched:true,results:result.results};
}
try{
 await runtime.ready;
 for(const item of manifest.cases){
   const name='normal-'+item.name+'-'+randomUUID();const log=[];const checkpoints=[];
   try{
     await call(name,JSON.stringify({action:'initialize',create:item.create}));
     if(item.checkpoints?.['0'])checkpoints.push(await inspect(name,item.checkpoints['0']));
     for(const [index,path]of item.pieces.entries()){
       const raw=await readFile(resolve(dirname(fixture),path),'utf8');
       const answer=await call(name,'{"action":"apply","piece":'+raw+',"sender":'+JSON.stringify(item.sender)+'}');
       log.push(answer);
       assert.ok(['applied','staged','duplicate'].includes(answer.kind),JSON.stringify(answer));
       const checkpoint=item.checkpoints[String(index+1)];
       if(checkpoint)checkpoints.push(await inspect(name,checkpoint));
     }
     results.push({name:item.name,kind:item.targetProduced?'passed':'prefixOnly',writer:item.writerStatus,log,checkpoints});
   }catch(error){results.push({name:item.name,kind:'failed',error:String(error),log,checkpoints});}
 }
 // A fresh object is populated by the public snapshot rules, never by manual INSERT fixtures.
 for(const item of manifest.snapshots??[]){
   const name='snapshot-'+item.name+'-'+randomUUID();const log=[];
   try{
     log.push(await call(name,JSON.stringify({action:'snapshotPrepare',install:item.install,source:item.source})));
     for(const page of item.pages){
       const raw=await readFile(resolve(dirname(fixture),page.path),'utf8');
       log.push(await call(name,'{"action":"snapshotPage","install":'+JSON.stringify(item.install)+',"index":'+page.index+',"cursor":'+JSON.stringify(page.cursor)+',"page":'+raw+'}'));
     }
     log.push(await call(name,JSON.stringify({action:'snapshotFinish',install:item.install,manifest:item.manifest})));
     const checkpoint=await inspect(name,item.checkpoint);
     results.push({name:item.name,kind:'snapshotPassed',checkpoint,log});
   }catch(error){results.push({name:item.name,kind:'snapshotFailed',error:String(error),log});}
 }
 await writeFile(resolve(evidence,'results.json'),JSON.stringify(results,null,2));
 console.log(JSON.stringify(results.map(({name,kind,error,checkpoints})=>({name,kind,error,checkpoints:checkpoints?.length}))));
 if(results.some(result=>['failed','snapshotFailed'].includes(result.kind)))process.exitCode=1;
}finally{await runtime.dispose();}
