const fs=require('fs');
const s=fs.readFileSync('hybrid.js','utf8');
for(const x of ['relayDiscovery/current/currentUrl','/alive?','ALIVE_TIMEOUT_MS=6500','REDISCOVERY_MS=60000']) if(!s.includes(x)) throw new Error('missing '+x);
if(s.includes("EventSource(DB+'/relayDiscovery/current")) throw new Error('Firebase discovery SSE must not remain');
console.log('web v3.67 address/alive route: PASS');
