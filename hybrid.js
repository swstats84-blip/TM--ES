(function(){
 'use strict';
 if(window.__webV362HybridInstalled)return;window.__webV362HybridInstalled=true;
 const direct=window.__fbFirebaseDirectV355;
 if(!direct||!window.firebase||!firebase.database){console.warn('v3.62 transport: Firebase direct facade 준비 전');return;}
 const DB=String(firebase.app().options&&firebase.app().options.databaseURL||'').replace(/\/+$/,'');
 const DEFAULT_RELAY='https://swstats84.mooo.com';
 const RELAY_KEY='psuRelayLastUrlV361',CLIENT_KEY='psuRelayClientIdV361';
 let CLIENT_ID='';
 try{CLIENT_ID=String(sessionStorage.getItem(CLIENT_KEY)||'');if(!CLIENT_ID){CLIENT_ID='web-'+(crypto.randomUUID?crypto.randomUUID():Date.now().toString(36)+'-'+Math.random().toString(36).slice(2));sessionStorage.setItem(CLIENT_KEY,CLIENT_ID)}}catch(_e){CLIENT_ID='web-'+Date.now().toString(36)}
 let relayReady=true,mode='RELAY',relayURL=DEFAULT_RELAY,generation=0,probeBusy=false,relayCheckBusy=null,hybrid=null;
 const logicalListeners=new Set(),relayStreams=new Map();
 const clean=p=>String(p||'').replace(/^\/+|\/+$/g,''),clone=v=>v===undefined?undefined:JSON.parse(JSON.stringify(v)),obj=v=>!!v&&typeof v==='object'&&!Array.isArray(v);
 function sdkGate(v){try{const db=firebase.database();if(v&&db&&db.goOnline)db.goOnline();else if(db&&db.goOffline)db.goOffline()}catch(_e){}}
 sdkGate(false);
 async function user(){return await direct.auth()}
 async function token(force=false){const u=await user();return await u.getIdToken(!!force)}
 function qs(q){const x=new URLSearchParams();if(q.orderBy!==undefined)x.set('orderBy',JSON.stringify(q.orderBy));if(q.startAtSet)x.set('startAt',JSON.stringify(q.startAt));if(q.endAtSet)x.set('endAt',JSON.stringify(q.endAt));if(q.equalToSet)x.set('equalTo',JSON.stringify(q.equalTo));if(q.limitFirst!==undefined)x.set('limitToFirst',String(q.limitFirst));if(q.limitLast!==undefined)x.set('limitToLast',String(q.limitLast));return x}
 async function rawFirebaseREST(path,q={},opt={}){const sp=qs(q);sp.set('auth',await token(!!opt.forceToken));const r=await fetch(DB+'/'+clean(path)+'.json?'+sp.toString(),{method:opt.method||'GET',body:opt.body===undefined?undefined:JSON.stringify(opt.body),cache:'no-store',headers:Object.assign({'Cache-Control':'no-cache'},opt.body===undefined?{}:{'Content-Type':'application/json'},opt.headers||{})});const text=await r.text();let data=null;try{data=text.trim()?JSON.parse(text):null}catch(_e){data=text}return {res:r,data,text}}
 async function relayREST(path,q={},opt={}){if(!relayURL)throw Object.assign(new Error('중계서버 주소 없음'),{relayNetwork:true});const sp=qs(q);sp.set('auth',await token(!!opt.forceToken));sp.set('clientId',CLIENT_ID);sp.set('clientType','web');let r;try{r=await fetch(relayURL+'/firebase/'+clean(path)+'.json?'+sp.toString(),{method:opt.method||'GET',body:opt.body===undefined?undefined:JSON.stringify(opt.body),cache:'no-store',headers:Object.assign({'Cache-Control':'no-cache','X-PSU-Client-ID':CLIENT_ID,'X-PSU-Client-Type':'web'},opt.body===undefined?{}:{'Content-Type':'application/json'},opt.headers||{})})}catch(e){throw Object.assign(new Error(e&&e.message?e.message:'Relay network error'),{relayNetwork:true,cause:e})}const text=await r.text();let data=null;try{data=text.trim()?JSON.parse(text):null}catch(_e){data=text}const alive=String(r.headers.get('X-PSU-Relay-Alive')||'').trim()==='1';return {res:r,data,text,relayAlive:alive}}
 function route(){return relayReady?'RELAY':'DIRECT'}
 function status(){return {mode,relayReady,relayURL,directEnabled:!relayReady,directActive:!relayReady,generation,label:relayReady?'중계 READY':'Firebase Direct'}}
 function emit(){window.dispatchEvent(new CustomEvent('psu-transport-mode',{detail:status()}))}
 function switchMode(next,url=''){
   next=String(next||'').toUpperCase()==='RELAY'?'RELAY':'DIRECT';url=next==='RELAY'?String(url||relayURL||DEFAULT_RELAY).replace(/\/+$/,''):'';
   if(mode===next&&relayURL===url){sdkGate(mode==='DIRECT');return}
   for(const st of relayStreams.values())st.close(false);relayStreams.clear();for(const l of logicalListeners)l.detach();
   mode=next;relayReady=next==='RELAY';relayURL=url;if(mode==='RELAY'&&relayURL){try{localStorage.setItem(RELAY_KEY,relayURL)}catch(_e){}}generation++;if(hybrid)hybrid.mode=mode.toLowerCase();sdkGate(mode==='DIRECT');for(const l of logicalListeners)l.bind();emit();
 }
 function setRelayState(ready,url=''){switchMode(ready?'RELAY':'DIRECT',ready?(url||relayURL||DEFAULT_RELAY):'')}
 async function directREST(path,q={},opt={}){const x=await rawFirebaseREST(path,q,opt);if(!x.res.ok)throw new Error('Firebase HTTP '+x.res.status+': '+x.text);return x}
 async function currentRelayHealth(){if(!relayURL)return null;if(relayCheckBusy)return relayCheckBusy;const u=relayURL;relayCheckBusy=health(u).finally(()=>{relayCheckBusy=null});return relayCheckBusy}
 async function prepareAction(){if(mode==='DIRECT')await probe();return mode}
 async function rest(path,q={},opt={}){
   await prepareAction();
   if(mode==='DIRECT')return directREST(path,q,opt);
   let x;
   try{x=await relayREST(path,q,opt)}catch(e){const h=await currentRelayHealth();if(h){setRelayState(true,h.url);return directREST(path,q,opt)}setRelayState(false,'');return directREST(path,q,opt)}
   if(x.res.ok){setRelayState(true,relayURL);return x}
   if(x.relayAlive)return directREST(path,q,opt);
   const h=await currentRelayHealth();if(h){setRelayState(true,h.url);return directREST(path,q,opt)}setRelayState(false,'');return directREST(path,q,opt)
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
   async recover(){if(mode!=='RELAY'||!this.listeners.size)return;const h=await currentRelayHealth();if(!h){setRelayState(false,'');return}setRelayState(true,h.url);try{const x=await rawFirebaseREST(this.path,this.q,{method:'GET'});if(x.res.ok){const before=clone(this.model),first=this.model===undefined;this.model=clone(x.data);this.dispatch(before,this.model,first)}}catch(e){for(const l of this.listeners)if(l.err)l.err(e)}if(mode==='RELAY'&&this.listeners.size)queueMicrotask(()=>this.open())}
   async open(){if(this.es||this.opening||mode!=='RELAY'||!relayReady||!relayURL||!this.listeners.size)return;this.opening=true;try{const sp=qs(this.q);sp.set('auth',await token());sp.set('clientId',CLIENT_ID);sp.set('clientType','web');if(mode!=='RELAY'||!relayReady||!this.listeners.size)return;const es=new EventSource(relayURL+'/firebase/'+this.path+'.json?'+sp.toString());this.es=es;const ev=(kind,e)=>{if(this.es!==es)return;try{const m=JSON.parse(e.data||'{}'),before=clone(this.model),first=this.model===undefined;this.model=apply(this.model,String(m.path||'/'),kind,m.data);this.dispatch(before,this.model,first)}catch(x){for(const l of this.listeners)if(l.err)l.err(x)}};es.addEventListener('put',e=>ev('put',e));es.addEventListener('patch',e=>ev('patch',e));es.onerror=()=>{if(this.es!==es)return;try{es.close()}catch(_e){}this.es=null;queueMicrotask(()=>this.recover())};}finally{this.opening=false}}
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
   async once(ev){await prepareAction();if(mode==='DIRECT')return directQuery(this.path,this.q).once(ev);if(ev!=='value')throw new Error('Relay once는 value만 지원합니다.');return new Snap((await rest(this.path,this.q,{method:'GET'})).data,this.key)}
   on(ev,cb,err){const l=new Listener(this,ev,cb,err);this._ls.push(l);if(mode==='DIRECT')queueMicrotask(()=>probe());return cb}
   off(ev,cb){this._ls=this._ls.filter(l=>{if((!ev||l.event===ev)&&(!cb||l.cb===cb)){l.close();return false}return true})}
   async set(v){await prepareAction();if(mode==='DIRECT')return directQuery(this.path,{}).set(v);await rest(this.path,{}, {method:'PUT',body:v});return v}
   async update(v){await prepareAction();if(mode==='DIRECT')return directQuery(this.path,{}).update(v);await rest(this.path,{}, {method:'PATCH',body:v});return v}
   async remove(){await prepareAction();if(mode==='DIRECT')return directQuery(this.path,{}).remove();await rest(this.path,{}, {method:'DELETE'});return null}
   push(v){const key=firebase.database().ref(this.path).push().key,r=new HRef([this.path,key].filter(Boolean).join('/'));if(arguments.length)return r.set(v).then(()=>r);return r}
   async transaction(fn){await prepareAction();if(mode==='DIRECT')return directQuery(this.path,{}).transaction(fn);for(let i=0;i<10;i++){let g,p;try{g=await relayREST(this.path,{}, {method:'GET',headers:{'X-Firebase-ETag':'true'},forceToken:i>0})}catch(e){const h=await currentRelayHealth();if(h){setRelayState(true,h.url);return directQuery(this.path,{}).transaction(fn)}setRelayState(false,'');return directQuery(this.path,{}).transaction(fn)}if(!g.res.ok){if(!g.relayAlive){const h=await currentRelayHealth();if(!h)setRelayState(false,'');else setRelayState(true,h.url)}return directQuery(this.path,{}).transaction(fn)}const cur=g.data,etag=g.res.headers.get('ETag')||g.res.headers.get('etag'),next=fn(clone(cur));if(next===undefined)return {committed:false,snapshot:new Snap(cur,this.key)};try{p=await relayREST(this.path,{}, {method:'PUT',body:next,headers:{'If-Match':etag||'*'},forceToken:i>0})}catch(e){const h=await currentRelayHealth();if(h){setRelayState(true,h.url);return directQuery(this.path,{}).transaction(fn)}setRelayState(false,'');return directQuery(this.path,{}).transaction(fn)}if(p.res.status===412)continue;if(!p.res.ok){if(!p.relayAlive){const h=await currentRelayHealth();if(!h)setRelayState(false,'');else setRelayState(true,h.url)}return directQuery(this.path,{}).transaction(fn)}return {committed:true,snapshot:new Snap(next,this.key)}}throw new Error('transaction retry exceeded')}
 }
 function externalRelayURL(v){try{v=String(v||'').replace(/\/+$/,'');if(!v)return'';const u=new URL(v);if(u.protocol!=='https:')return'';const h=String(u.hostname||'').toLowerCase();if(!h||h==='localhost'||h==='127.0.0.1'||h==='::1'||h==='[::1]'||/^10\./.test(h)||/^192\.168\./.test(h)||/^172\.(1[6-9]|2\d|3[01])\./.test(h))return'';return v}catch(_e){return''}}
 function savedRelayCandidates(){const out=[];const add=v=>{v=externalRelayURL(v);if(v&&!out.includes(v))out.push(v)};try{add(new URLSearchParams(location.search).get('psuRelay'));add(localStorage.getItem(RELAY_KEY))}catch(_e){}add(DEFAULT_RELAY);return out}
 async function health(u){u=externalRelayURL(u);if(!u)return null;const ac=new AbortController(),tm=setTimeout(()=>ac.abort(),700);try{const r=await fetch(u+'/health',{cache:'no-store',signal:ac.signal});if(!r.ok)return null;const h=await r.json(),alive=!!(h&&((h.serverOnline===true)||(h.ok&&h.usable)));if(!alive||h.mode!=='second-firebase'||(h.protocol&&h.protocol!=='firebase-delta-v1'))return null;return {url:u,health:h}}catch(_e){return null}finally{clearTimeout(tm)}}
 async function discover(){for(const x of savedRelayCandidates()){const h=await health(x);if(h){setRelayState(true,h.url);return true}}try{const d=(await rawFirebaseREST('relayDiscovery/current/currentUrl',{}, {method:'GET'})).data,x=externalRelayURL(d);if(x){const h=await health(x);if(h){setRelayState(true,h.url);return true}}}catch(_e){}setRelayState(false,'');return false}
 async function probe(){if(probeBusy)return status();probeBusy=true;try{return await discover()?status():status()}finally{probeBusy=false}}
 hybrid={mode:'relay',auth:(...a)=>direct.auth(...a),bump:(...a)=>direct.bump(...a),bumpScheduleDeltaV286:(...a)=>direct.bumpScheduleDeltaV286(...a),read:(...a)=>direct.read(...a),readRange:(...a)=>direct.readRange(...a),readFresh:async p=>(await rest(p,{}, {method:'GET'})).data,set:(...a)=>direct.set(...a),update:(...a)=>direct.update(...a),del:(...a)=>direct.del(...a),b64key:(...a)=>direct.b64key(...a),isAdmin:(...a)=>direct.isAdmin(...a),isMaster:(...a)=>direct.isMaster(...a),ref:p=>new HRef(p)};
 hybrid.mode=mode.toLowerCase();
 window.__psuDataTransportV357=hybrid;window.__psuDataTransportV355=hybrid;window.__fbDirectV187=hybrid;
 window.__psuHybridTransportV362={status,probe};window.__psuHybridTransportV361=window.__psuHybridTransportV362;window.__psuHybridTransportV360=window.__psuHybridTransportV362;
 probe();document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')probe()});window.addEventListener('focus',probe);emit();
})();
