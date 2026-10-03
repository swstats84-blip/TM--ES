(function(){
 'use strict';
 if(window.__webV364HybridInstalled)return;window.__webV364HybridInstalled=true;
 const direct=window.__fbFirebaseDirectV355;
 if(!direct||!window.firebase||!firebase.database){console.warn('v3.64 transport: Firebase direct facade 준비 전');return;}
 const DB=String(firebase.app().options&&firebase.app().options.databaseURL||'').replace(/\/+$/,'');
 const DEFAULT_RELAY='https://swstats84.mooo.com';
 const CLIENT_KEY='psuRelayClientIdV364';
 let CLIENT_ID='';
 try{CLIENT_ID=String(sessionStorage.getItem(CLIENT_KEY)||'');if(!CLIENT_ID){CLIENT_ID='web-'+(crypto.randomUUID?crypto.randomUUID():Date.now().toString(36)+'-'+Math.random().toString(36).slice(2));sessionStorage.setItem(CLIENT_KEY,CLIENT_ID)}}catch(_e){CLIENT_ID='web-'+Date.now().toString(36)}
 let relayReady=false,directEnabled=true,mode='DIRECT',relayURL=DEFAULT_RELAY,generation=0,hybrid=null,controlRef=null,leaseTimer=null;
 const logicalListeners=new Set(),relayStreams=new Map();
 const clean=p=>String(p||'').replace(/^\/+|\/+$/g,''),clone=v=>v===undefined?undefined:JSON.parse(JSON.stringify(v)),obj=v=>!!v&&typeof v==='object'&&!Array.isArray(v);
 async function user(){return await direct.auth()}
 async function token(force=false){const u=await user();return await u.getIdToken(!!force)}
 function qs(q){const x=new URLSearchParams();if(q.orderBy!==undefined)x.set('orderBy',JSON.stringify(q.orderBy));if(q.startAtSet)x.set('startAt',JSON.stringify(q.startAt));if(q.endAtSet)x.set('endAt',JSON.stringify(q.endAt));if(q.equalToSet)x.set('equalTo',JSON.stringify(q.equalTo));if(q.limitFirst!==undefined)x.set('limitToFirst',String(q.limitFirst));if(q.limitLast!==undefined)x.set('limitToLast',String(q.limitLast));return x}
 async function rawFirebaseREST(path,q={},opt={}){const sp=qs(q);sp.set('auth',await token(!!opt.forceToken));const r=await fetch(DB+'/'+clean(path)+'.json?'+sp.toString(),{method:opt.method||'GET',body:opt.body===undefined?undefined:JSON.stringify(opt.body),cache:'no-store',headers:Object.assign({'Cache-Control':'no-cache'},opt.body===undefined?{}:{'Content-Type':'application/json'},opt.headers||{})});const text=await r.text();let data=null;try{data=text.trim()?JSON.parse(text):null}catch(_e){data=text}return {res:r,data,text}}
 async function relayREST(path,q={},opt={}){if(!relayReady||mode!=='RELAY'||!relayURL)throw Object.assign(new Error('중계서버 OFF'),{relayTransport:true});const sp=qs(q);sp.set('auth',await token(!!opt.forceToken));sp.set('clientId',CLIENT_ID);sp.set('clientType','web');const ac=new AbortController(),tm=setTimeout(()=>ac.abort(),5000);try{const r=await fetch(relayURL+'/firebase/'+clean(path)+'.json?'+sp.toString(),{method:opt.method||'GET',body:opt.body===undefined?undefined:JSON.stringify(opt.body),cache:'no-store',signal:ac.signal,headers:Object.assign({'Cache-Control':'no-cache','X-PSU-Client-ID':CLIENT_ID,'X-PSU-Client-Type':'web'},opt.body===undefined?{}:{'Content-Type':'application/json'},opt.headers||{})});const text=await r.text();let data=null;try{data=text.trim()?JSON.parse(text):null}catch(_e){data=text}return {res:r,data,text}}finally{clearTimeout(tm)}}
 function route(){return relayReady?'RELAY':'DIRECT'}
 function status(){return {mode,relayReady,relayURL,directEnabled:!relayReady,directActive:!relayReady,generation,label:relayReady?'중계 ON':'Firebase Direct'}}
 function emit(){window.dispatchEvent(new CustomEvent('psu-transport-mode',{detail:status()}))}
 function switchMode(next,url=''){
   next=String(next||'').toUpperCase()==='RELAY'?'RELAY':'DIRECT';url=String(url||relayURL||DEFAULT_RELAY).replace(/\/+$/,'');
   const ready=next==='RELAY';if(mode===next&&relayReady===ready&&(!ready||relayURL===url)){directEnabled=!ready;return}
   for(const s of relayStreams.values())s.close(false);relayStreams.clear();for(const l of logicalListeners)l.detach();
   relayReady=ready;directEnabled=!ready;mode=next;if(ready&&url)relayURL=url;generation++;if(hybrid)hybrid.mode=mode.toLowerCase();for(const l of logicalListeners)l.bind();emit();
 }
 function setRelayState(ready,url=''){switchMode(ready?'RELAY':'DIRECT',url||relayURL||DEFAULT_RELAY)}
 async function rest(path,q={},opt={}){
   if(mode==='DIRECT'){const x=await rawFirebaseREST(path,q,opt);if(!x.res.ok)throw new Error('Firebase HTTP '+x.res.status+': '+x.text);return x}
   const x=await relayREST(path,q,opt);if(!x.res.ok)throw Object.assign(new Error('Relay HTTP '+x.res.status+': '+x.text),{status:x.res.status});return x
 }
 class Snap{constructor(v,key=''){this._v=clone(v);this.key=key||null}val(){return clone(this._v)}exists(){return this._v!==null&&this._v!==undefined}forEach(fn){if(!obj(this._v)&&!Array.isArray(this._v))return false;for(const k of Object.keys(this._v||{})){if(fn(new Snap(this._v[k],k))===true)return true}return false}child(k){return new Snap(this._v&&typeof this._v==='object'?this._v[k]:null,String(k))}}
 function directQuery(path,q){let r=firebase.database().ref(clean(path));if(q.orderBy==='$key')r=r.orderByKey();else if(q.orderBy!==undefined)r=r.orderByChild(q.orderBy);if(q.startAtSet)r=r.startAt(q.startAt);if(q.endAtSet)r=r.endAt(q.endAt);if(q.equalToSet)r=r.equalTo(q.equalTo);if(q.limitFirst!==undefined)r=r.limitToFirst(q.limitFirst);if(q.limitLast!==undefined)r=r.limitToLast(q.limitLast);return r}
 function setAt(root,parts,val){if(!parts.length)return clone(val);let out=(obj(root)||Array.isArray(root))?clone(root):{},cur=out;for(let i=0;i<parts.length-1;i++){const k=parts[i];if(!cur[k]||typeof cur[k]!=='object')cur[k]={};cur=cur[k]}const k=parts[parts.length-1];if(val===null||val===undefined){if(Array.isArray(cur)&&/^\d+$/.test(k))cur[+k]=null;else delete cur[k]}else cur[k]=clone(val);return out}
 function apply(root,path,kind,data){const pp=clean(path).split('/').filter(Boolean);if(kind==='put')return setAt(root,pp,data);let out=(obj(root)||Array.isArray(root))?clone(root):{};if(obj(data))for(const [k,v] of Object.entries(data))out=setAt(out,pp.concat(clean(k).split('/').filter(Boolean)),v);return out}
 function streamKey(path,q){return clean(path)+'?'+qs(q).toString()}
 function childMap(v){return(v&&typeof v==='object')?v:{}}
 class RelayStream{
   constructor(path,q){this.path=clean(path);this.q=q;this.key=streamKey(path,q);this.es=null;this.model=undefined;this.listeners=new Set();this.opening=false}
   add(l){this.listeners.add(l);if(this.model!==undefined)this.initial(l);this.open()}
   del(l){this.listeners.delete(l);if(!this.listeners.size)this.close(true)}
   initial(l){if(l.event==='value')l.cb(new Snap(this.model,l.ref.key));else if(l.event==='child_added')for(const k of Object.keys(childMap(this.model)))l.cb(new Snap(this.model[k],k))}
   dispatch(before,after,initial){for(const l of [...this.listeners]){try{if(l.event==='value'){l.cb(new Snap(after,l.ref.key));continue}const a=childMap(before),b=childMap(after);if(initial){if(l.event==='child_added')for(const k of Object.keys(b))l.cb(new Snap(b[k],k));continue}if(l.event==='child_added'){for(const k of Object.keys(b))if(!(k in a))l.cb(new Snap(b[k],k))}else if(l.event==='child_removed'){for(const k of Object.keys(a))if(!(k in b))l.cb(new Snap(a[k],k))}else if(l.event==='child_changed'){for(const k of Object.keys(b))if(k in a&&JSON.stringify(a[k])!==JSON.stringify(b[k]))l.cb(new Snap(b[k],k))}}catch(e){if(l.err)l.err(e)}}}
   async open(){if(this.es||this.opening||mode!=='RELAY'||!relayReady||!relayURL||!this.listeners.size)return;this.opening=true;try{const sp=qs(this.q);sp.set('auth',await token());sp.set('clientId',CLIENT_ID);sp.set('clientType','web');if(mode!=='RELAY'||!relayReady||!this.listeners.size)return;const es=new EventSource(relayURL+'/firebase/'+this.path+'.json?'+sp.toString());this.es=es;const ev=(kind,e)=>{if(this.es!==es)return;try{const m=JSON.parse(e.data||'{}'),before=clone(this.model),first=this.model===undefined;this.model=apply(this.model,String(m.path||'/'),kind,m.data);this.dispatch(before,this.model,first)}catch(x){for(const l of this.listeners)if(l.err)l.err(x)}};es.addEventListener('put',e=>ev('put',e));es.addEventListener('patch',e=>ev('patch',e));es.onerror=()=>{};}finally{this.opening=false}}
   close(remove){if(this.es){try{this.es.close()}catch(_e){}this.es=null}if(remove)relayStreams.delete(this.key)}
 }
 function relayStream(path,q){const k=streamKey(path,q);let s=relayStreams.get(k);if(!s){s=new RelayStream(path,q);relayStreams.set(k,s)}return s}
 class Listener{
   constructor(ref,event,cb,err){this.ref=ref;this.event=event;this.cb=cb;this.err=err;this.directRef=null;this.stream=null;logicalListeners.add(this);this.bind()}
   bind(){this.detach();if(mode==='DIRECT'){this.directRef=directQuery(this.ref.path,this.ref.q);this.directRef.on(this.event,this.cb,this.err)}else if(mode==='RELAY'){this.stream=relayStream(this.ref.path,this.ref.q);this.stream.add(this)}}
   detach(){if(this.directRef){try{this.directRef.off(this.event,this.cb)}catch(_e){}this.directRef=null}if(this.stream){this.stream.del(this);this.stream=null}}
   close(){this.detach();logicalListeners.delete(this)}
 }
 class HRef{
   constructor(path,q={}){this.path=clean(path);this.q=q;this._ls=[]}
   get key(){const a=this.path.split('/');return a[a.length-1]||null}
   child(p){return new HRef([this.path,clean(p)].filter(Boolean).join('/'),Object.assign({},this.q))}
   orderByKey(){return new HRef(this.path,Object.assign({},this.q,{orderBy:'$key'}))}
   orderByChild(k){return new HRef(this.path,Object.assign({},this.q,{orderBy:String(k)}))}
   startAt(v){return new HRef(this.path,Object.assign({},this.q,{startAtSet:true,startAt:v}))}
   endAt(v){return new HRef(this.path,Object.assign({},this.q,{endAtSet:true,endAt:v}))}
   equalTo(v){return new HRef(this.path,Object.assign({},this.q,{equalToSet:true,equalTo:v}))}
   limitToFirst(n){return new HRef(this.path,Object.assign({},this.q,{limitFirst:+n}))}
   limitToLast(n){return new HRef(this.path,Object.assign({},this.q,{limitLast:+n}))}
   async once(ev){if(mode==='DIRECT')return directQuery(this.path,this.q).once(ev);if(ev!=='value')throw new Error('Relay once는 value만 지원합니다.');return new Snap((await rest(this.path,this.q,{method:'GET'})).data,this.key)}
   on(ev,cb,err){const l=new Listener(this,ev,cb,err);this._ls.push(l);return cb}
   off(ev,cb){this._ls=this._ls.filter(l=>{if((!ev||l.event===ev)&&(!cb||l.cb===cb)){l.close();return false}return true})}
   async set(v){if(mode==='DIRECT')return directQuery(this.path,{}).set(v);await rest(this.path,{}, {method:'PUT',body:v});return v}
   async update(v){if(mode==='DIRECT')return directQuery(this.path,{}).update(v);await rest(this.path,{}, {method:'PATCH',body:v});return v}
   async remove(){if(mode==='DIRECT')return directQuery(this.path,{}).remove();await rest(this.path,{}, {method:'DELETE'});return null}
   push(v){const key=firebase.database().ref(this.path).push().key,r=new HRef([this.path,key].filter(Boolean).join('/'));if(arguments.length)return r.set(v).then(()=>r);return r}
   async transaction(fn){if(mode==='DIRECT')return directQuery(this.path,{}).transaction(fn);for(let i=0;i<10;i++){let g,p;g=await relayREST(this.path,{}, {method:'GET',headers:{'X-Firebase-ETag':'true'},forceToken:i>0});if(!g.res.ok)throw new Error('Relay transaction GET '+g.res.status);const cur=g.data,etag=g.res.headers.get('ETag')||g.res.headers.get('etag'),next=fn(clone(cur));if(next===undefined)return {committed:false,snapshot:new Snap(cur,this.key)};p=await relayREST(this.path,{}, {method:'PUT',body:next,headers:{'If-Match':etag||'*'},forceToken:i>0});if(p.res.status===412)continue;if(!p.res.ok)throw new Error('Relay transaction PUT '+p.res.status);return {committed:true,snapshot:new Snap(next,this.key)}}throw new Error('transaction retry exceeded')}
 }
 function controlValid(v){if(!v||v.cacheMode!=='hard-route-v1'||v.enabled!==true||String(v.status||'').toLowerCase()!=='ready')return false;const u=String(v.currentUrl||'').replace(/\/+$/,'');if(!u)return false;const lease=Number(v.leaseUntil||0),now=Date.now();return lease>now&&lease-now<=15000}
 function applyControl(v){v=v&&typeof v==='object'?v:{};if(leaseTimer){clearTimeout(leaseTimer);leaseTimer=null}const valid=controlValid(v),u=String(v.currentUrl||relayURL||DEFAULT_RELAY).replace(/\/+$/,'');setRelayState(valid,u);if(valid){const ms=Math.max(0,Number(v.leaseUntil)-Date.now()+20);leaseTimer=setTimeout(()=>{if(Date.now()>=Number(v.leaseUntil||0))setRelayState(false,u)},ms)}}
 async function startControl(){try{await user();controlRef=firebase.database().ref('relayDiscovery/current');controlRef.on('value',s=>applyControl(s.val()),()=>setRelayState(false,relayURL));}catch(_e){setRelayState(false,relayURL)}}
 hybrid={mode:'direct',auth:(...a)=>direct.auth(...a),bump:(...a)=>direct.bump(...a),bumpScheduleDeltaV286:(...a)=>direct.bumpScheduleDeltaV286(...a),read:async p=>{await user();const s=await new HRef(p).once('value');return s.val()},readRange:async(p,start,end)=>{await user();let q=new HRef(p).orderByKey();if(start!==undefined&&start!==null&&String(start)!=='')q=q.startAt(String(start));if(end!==undefined&&end!==null&&String(end)!=='')q=q.endAt(String(end));const s=await q.once('value');return s.val()},readFresh:async p=>(await rest(p,{}, {method:'GET'})).data,set:async(p,v)=>{await user();return new HRef(p).set(v)},update:async(p,v)=>{await user();return new HRef(p).update(v)},del:async p=>{await user();return new HRef(p).remove()},b64key:(...a)=>direct.b64key(...a),isAdmin:(...a)=>direct.isAdmin(...a),isMaster:(...a)=>direct.isMaster(...a),ref:p=>new HRef(p)};
 hybrid.mode=mode.toLowerCase();
 window.__psuDataTransportV357=hybrid;window.__psuDataTransportV355=hybrid;window.__fbDirectV187=hybrid;
 window.__psuHybridTransportV360={status};window.__psuHybridTransportV364={status};
 startControl();emit();
})();
