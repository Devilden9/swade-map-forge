
const crypto = require("crypto");
const http = require("http");
const { WebSocketServer } = require("ws");
const path = require("path");
const fs = require("fs");

const server = http.createServer((req,res)=>{res.setHeader("Content-Type","text/html; charset=utf-8");fs.createReadStream(path.join(__dirname,"public/index.html")).pipe(res)});
const wss = new WebSocketServer({ server, path: "/ws" });

const {RoomStore}=require("./room-store");
const dataDir=process.env.ROOM_DATA_DIR||process.env.RAILWAY_VOLUME_MOUNT_PATH||path.join(__dirname,"data");
if(process.env.RAILWAY_ENVIRONMENT_ID&&!process.env.ROOM_DATA_DIR&&!process.env.RAILWAY_VOLUME_MOUNT_PATH)throw Error("Persistent volume is required for Railway deployment");
const roomStore=new RoomStore(dataDir);
const rooms=roomStore.load();
let shuttingDown=false;
console.log(`Restored ${rooms.size} room(s) from persistent storage`);
function getRoom(code){
  if(!rooms.has(code)) rooms.set(code,{code,state:null,clients:new Map(),gmId:null,turnClientId:null,deck:[],discard:[],emptySince:null});
  return rooms.get(code);
}
function visibleState(room,viewerId){
 if(viewerId===room.gmId)return room.state;
 if(!room.state)return null;
 const copy=JSON.parse(JSON.stringify(room.state));
 const hiddenIds=new Set(),hiddenNames=new Set();
 for(const m of [copy,...(copy.scenes||[]).map(s=>s.map)])for(const t of m?.items||[])if(t.type==="token"&&t.hidden){hiddenIds.add(t.id);if(t.name)hiddenNames.add(t.name)}
 function filterMap(m){
  if(!m)return;
  m.items=(m.items||[]).filter(t=>!hiddenIds.has(t.id));
  const b=m.battleSession;if(!b)return;
  const drawn=b.initDrawn||[],power=c=>c.joker?100:({"2":2,"3":3,"4":4,"5":5,"6":6,"7":7,"8":8,"9":9,"10":10,J:11,Q:12,K:13,A:14}[c.rank]*10+{"♣":1,"♠":2,"♦":3,"♥":4}[c.suit]);
  const sorted=drawn.map((e,i)=>({...e,i})).sort((a,b)=>power(b.card)-power(a.card));const active=sorted[b.initTurnIndex]?.i;
  const indexMap=new Map();b.initDrawn=drawn.filter((e,i)=>{if(hiddenIds.has(e.tokenId))return false;indexMap.set(i,indexMap.size);return true});
  for(const k of ["heldTurns","interruptedTurnStack","jokerUsed"])b[k]=(b[k]||[]).filter(i=>indexMap.has(i)).map(i=>indexMap.get(i));
  const order=b.initDrawn.map((e,i)=>({...e,i})).sort((a,b)=>power(b.card)-power(a.card));b.initTurnIndex=indexMap.has(active)?order.findIndex(e=>e.i===indexMap.get(active)):-1;
 }
 filterMap(copy);for(const scene of copy.scenes||[])filterMap(scene.map);
 copy.battleLog=(copy.battleLog||[]).filter(r=>![...hiddenNames].some(n=>String(r.text).includes(n)));
 return copy;
}
function payloadFor(room,viewerId,obj){
 if(viewerId===room.gmId)return obj;
 const out={...obj};if(Object.hasOwn(out,"state"))out.state=visibleState(room,viewerId);
 if(out.type==="players")out.players=out.players.map(p=>{const t=room.state?.items?.find(t=>t.id===p.tokenId);return t?.hidden?{...p,tokenId:null,isTurn:false}:p});
 if(out.type==="tokenMoved"&&room.state?.items?.find(t=>t.id===out.tokenId)?.hidden)return null;
 return out;
}
function send(ws,obj){
 const room=rooms.get(ws.roomCode);if(room)roomStore.save(room);
 const data=room?payloadFor(room,ws.clientId,obj):obj;if(data&&ws?.readyState===1)ws.send(JSON.stringify(data));
}
function broadcast(room,obj,except=null){
 roomStore.save(room);
 for(const [id,c] of room.clients){if(id===except||c.ws?.readyState!==1)continue;const data=payloadFor(room,id,obj);if(data)c.ws.send(JSON.stringify(data))}
}
function ensurePlayerTokens(room){
 if(!room.state)return false;
 const scene=room.state.activeSceneId||"default";let changed=false;
 for(const [id,c] of room.clients){
  if(id===room.gmId)continue;
  c.tokensByScene=c.tokensByScene||{};
  const known=c.tokensByScene[scene];
  let t=room.state.items.find(t=>t.type==="token"&&(t.id===known||t.ownerClientId===id));
  if(!t&&!known){
   const cell=room.state.cell||48;let x=0,y=0;
   outer:for(let row=0;row<100;row++)for(let col=0;col<(room.state.cols||30);col++){
    x=col*cell;y=row*cell;if(!room.state.items.some(i=>x<i.x+i.w&&x+cell>i.x&&y<i.y+i.h&&y+cell>i.y))break outer;
   }
   room.state.rows=Math.max(room.state.rows,Math.ceil((y+cell)/cell));
   t={id:crypto.randomUUID(),type:"token",name:c.name,x,y,w:cell,h:cell,r:0,bennies:0,wounds:0,shaken:false,conditions:{},ownerClientId:id};room.state.items.push(t);changed=true;
  }
  if(t){t.ownerClientId=id;c.tokensByScene[scene]=t.id;c.tokenId=t.id}else c.tokenId=null;
 }
 if(changed)room.state.roomRevision=(room.state.roomRevision||0)+1;
 return changed;
}
function syncActiveScene(room){const scene=room.state?.scenes?.find(s=>s.id===room.state.activeSceneId);if(scene?.map){scene.map.items=JSON.parse(JSON.stringify(room.state.items));scene.map.rows=room.state.rows}}
function players(room){
  return [...room.clients].map(([id,c])=>({id,name:c.name,isGM:id===room.gmId,tokenId:c.tokenId||null,isTurn:id===room.turnClientId,online:c.ws?.readyState===1}));
}
wss.on("connection", ws=>{
  let clientId=crypto.randomUUID();
  let joinedCode=null;
  ws.on("message", raw=>{
    let msg; try{msg=JSON.parse(raw)}catch{return}
    if(shuttingDown||!msg||typeof msg!=="object")return;
    try{
    if(msg.type==="join"){
      const code=String(msg.room||"").toUpperCase().replace(/[^A-Z0-9_-]/g,"").slice(0,24);
      if(!code) return send(ws,{type:"error",message:"Неверный код комнаты."});
      if(joinedCode)return;
      if(!String(msg.name||"").trim())return send(ws,{type:"error",message:"Введите имя."});
      joinedCode=code;ws.roomCode=code; const room=getRoom(code);
      const resumed=[...room.clients].find(([,c])=>typeof msg.resumeToken==="string"&&c.resumeToken===msg.resumeToken);
      let cl;
      if(resumed){clientId=resumed[0];cl=resumed[1];const old=cl.ws;cl.ws=ws;cl.lastSeen=Date.now();if(old&&old!==ws)old.close(4001,"Opened in another tab");}
      else{cl={ws,name:String(msg.name).trim().slice(0,32),tokenId:null,tokensByScene:{},resumeToken:crypto.randomBytes(32).toString("hex"),lastSeen:Date.now()};room.clients.set(clientId,cl)}
      ws.clientId=clientId;room.emptySince=null;
      if(!room.gmId)room.gmId=clientId;
      const tokensCreated=ensurePlayerTokens(room);syncActiveScene(room);
      send(ws,{type:"welcome",clientId,name:cl.name,resumeToken:cl.resumeToken,resumed:!!resumed,isGM:room.gmId===clientId,state:room.state,turnClientId:room.turnClientId});
      if(tokensCreated)broadcast(room,{type:"state",clientId:"server",state:room.state});
      broadcast(room,{type:"players",players:players(room)});
    } else if(joinedCode&&rooms.get(joinedCode)?.clients.get(clientId)?.ws!==ws){return;
    } else if(msg.type==="transferGM" && joinedCode){
      const room=getRoom(joinedCode);
      if(room.gmId!==clientId)return send(ws,{type:"error",message:"Только текущий GM может передать роль."});
      if(msg.targetId===clientId||room.clients.get(msg.targetId)?.ws?.readyState!==1)return send(ws,{type:"error",message:"Игрок уже отключился."});
      room.gmId=msg.targetId;ensurePlayerTokens(room);syncActiveScene(room);broadcast(room,{type:"state",clientId:"server",state:room.state});broadcast(room,{type:"players",players:players(room)});
      broadcast(room,{type:"gmTransferred",name:room.clients.get(msg.targetId).name});
    } else if(msg.type==="benny" && joinedCode){
      const room=getRoom(joinedCode),cl=room.clients.get(clientId),delta=msg.delta;
      if(![1,-1].includes(delta)||!room.state)return;
      if(clientId!==room.gmId&&(delta!==-1||cl.tokenId!==msg.tokenId))return send(ws,{type:"error",message:"Игрок может тратить только свои фишки."});
      const t=room.state.items?.find(t=>t.type==="token"&&t.id===msg.tokenId);if(!t)return;
      if(t.hidden&&clientId!==room.gmId)return;
      const count=Math.max(0,Math.floor(Number(t.bennies)||0));if(count+delta<0)return;
      t.bennies=count+delta;
      room.state.battleLog=room.state.battleLog||[];
      room.state.battleLog.push({time:new Date().toISOString(),round:room.state.battleSession?.combatRound||1,actor:cl.name,text:`${t.name||"Токен"}: фишки ${delta>0?"+1":"−1"} (${count} → ${t.bennies})`});
      broadcast(room,{type:"state",clientId:"server",state:room.state});
    } else if(msg.type==="state" && joinedCode){
      const room=getRoom(joinedCode);
      if(clientId!==room.gmId) return send(ws,{type:"error",message:"Только GM может редактировать карту."});
      if(!msg.state||!Array.isArray(msg.state.items))return;
      if(room.state&&(msg.state.roomRevision||0)<(room.state.roomRevision||0)){send(ws,{type:"state",clientId:"server",state:room.state});return}
      if(room.state){for(const t of msg.state.items){const oldMap=room.state.activeSceneId===msg.state.activeSceneId?room.state:room.state.scenes?.find(s=>s.id===msg.state.activeSceneId)?.map;const old=oldMap?.items?.find(i=>i.id===t.id);if(old)t.bennies=old.bennies||0}
      const byKey=new Map();for(const row of [...(room.state.battleLog||[]),...(msg.state.battleLog||[])])byKey.set(JSON.stringify(row),row);msg.state.battleLog=[...byKey.values()].sort((a,b)=>a.time.localeCompare(b.time));}
      const sceneChanged=room.state?.activeSceneId!==msg.state.activeSceneId;
      room.state=msg.state;
      if(sceneChanged){for(const c of room.clients.values())c.tokenId=c.tokensByScene?.[room.state.activeSceneId||"default"]||null;room.turnClientId=null;broadcast(room,{type:"players",players:players(room)})}
      const added=ensurePlayerTokens(room);syncActiveScene(room);broadcast(room,{type:"state",clientId:added?"server":clientId,state:room.state},added?null:clientId);broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="assignToken" && joinedCode){
      const room=getRoom(joinedCode); if(clientId!==room.gmId)return;
      const target=room.clients.get(String(msg.clientId||"")); if(!target)return;
      const tokenId=String(msg.tokenId||"");
      const assigned=room.state?.items?.find(t=>t.id===tokenId&&t.type==="token");if(tokenId&&!assigned)return;
      for(const t of room.state?.items||[])if(t.ownerClientId===String(msg.clientId||""))delete t.ownerClientId;
      if(assigned)assigned.ownerClientId=String(msg.clientId||"");
      for(const c of room.clients.values())if(c!==target&&c.tokenId===tokenId){c.tokenId=null;if(c.tokensByScene)c.tokensByScene[room.state?.activeSceneId||"default"]=""}
      target.tokenId=tokenId;target.tokensByScene=target.tokensByScene||{};target.tokensByScene[room.state?.activeSceneId||"default"]=target.tokenId; broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="setTurn" && joinedCode){
      const room=getRoom(joinedCode); if(clientId!==room.gmId)return;
      room.turnClientId=String(msg.clientId||"")||null;
      broadcast(room,{type:"turn",turnClientId:room.turnClientId});
      broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="updateToken" && joinedCode){
      const room=getRoom(joinedCode),cl=room.clients.get(clientId),t=room.state?.items?.find(t=>t.id===msg.tokenId&&t.type==="token");
      if(!t||(clientId!==room.gmId&&(cl.tokenId!==t.id||t.hidden)))return;
      const v=msg.values||{};
      const before=JSON.stringify(t),nameBefore=t.name||"Токен";
      if(typeof v.name==="string")t.name=v.name.slice(0,80);
      if(v.portrait===null)delete t.portrait;
      else if(typeof v.portrait==="string"&&v.portrait.length<300000&&/^data:image\/(png|jpeg|webp);base64,/.test(v.portrait))t.portrait=v.portrait;
      if(Number.isFinite(v.wounds))t.wounds=Math.max(0,Math.min(3,Math.floor(v.wounds)));
      if(typeof v.shaken==="boolean")t.shaken=v.shaken;
      if(v.conditions&&typeof v.conditions==="object"){t.conditions=t.conditions||{};for(const k of ["dead","unconscious","fatigued","prone","bound","distracted","vulnerable","stunned"])if(typeof v.conditions[k]==="boolean")t.conditions[k]=v.conditions[k]}
      if(JSON.stringify(t)!==before){room.state.roomRevision=(room.state.roomRevision||0)+1;room.state.battleLog=room.state.battleLog||[];room.state.battleLog.push({time:new Date().toISOString(),round:room.state.battleSession?.combatRound||1,actor:cl.name,text:`Изменён токен: ${nameBefore}${t.name!==nameBefore?" → "+t.name:""}`})}
      syncActiveScene(room);broadcast(room,{type:"state",clientId:"server",state:room.state});
    } else if(msg.type==="moveToken" && joinedCode){
      const room=getRoom(joinedCode), cl=room.clients.get(clientId);
      if(!cl||(room.turnClientId&&room.turnClientId!==clientId)||!cl.tokenId||cl.tokenId!==msg.tokenId||!room.state)return;
      const it=room.state.items?.find(x=>x.id===cl.tokenId&&x.type==="token"); if(!it||it.hidden&&clientId!==room.gmId)return;
      if(!Number.isFinite(msg.x)||!Number.isFinite(msg.y))return;
      it.x=Math.max(0,Math.min(room.state.cols*room.state.cell-it.w,msg.x));it.y=Math.max(0,Math.min(room.state.rows*room.state.cell-it.h,msg.y));syncActiveScene(room);
      broadcast(room,{type:"tokenMoved",clientId,tokenId:it.id,x:it.x,y:it.y});
    } else if(msg.type==="drawInitiative" && joinedCode){
      const room=getRoom(joinedCode);
      const suits=["♠","♥","♦","♣"], ranks=["2","3","4","5","6","7","8","9","10","J","Q","K","A"];
      if(!room.deck.length){room.deck=[];for(const s of suits)for(const r of ranks)room.deck.push(r+s);room.deck.push("JOKER 🃏","JOKER 🃏");for(let i=room.deck.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[room.deck[i],room.deck[j]]=[room.deck[j],room.deck[i]]}}
      const card=room.deck.pop(), cl=room.clients.get(clientId); room.discard.push(card);
      broadcast(room,{type:"initiative",clientId,name:cl?.name||"Игрок",card});

    } else if(msg.type==="dice" && joinedCode){
      const room=getRoom(joinedCode), c=room.clients.get(clientId);
      broadcast(room,{type:"dice",clientId,name:c?.name||"Игрок",title:msg.title,total:msg.total,detail:msg.detail,cls:msg.cls},clientId);
    }
    if(joinedCode&&rooms.has(joinedCode))roomStore.save(rooms.get(joinedCode));
    }catch(err){console.error("Room update could not be saved:",err.message);if(ws.readyState===1)ws.send(JSON.stringify({type:"error",message:"Не удалось сохранить комнату на сервере. Сохраните бой в файл и повторите позже."}))}
  });
  ws.on("close",()=>{
    if(shuttingDown||!joinedCode)return; const room=rooms.get(joinedCode); if(!room)return;
    const cl=room.clients.get(clientId);if(!cl||cl.ws!==ws)return;cl.ws=null;cl.lastSeen=Date.now();
    if(room.gmId===clientId)room.gmId=[...room.clients].find(([,c])=>c.ws?.readyState===1)?.[0]||clientId;
    if(![...room.clients.values()].some(c=>c.ws?.readyState===1))room.emptySince=Date.now();
    try{broadcast(room,{type:"players",players:players(room)})}catch(err){console.error("Could not save disconnected room:",err.message)}
  });
});
// Persistent rooms are not automatically deleted after 24 hours.
function shutdown(){
 if(shuttingDown)return;shuttingDown=true;
 try{for(const room of rooms.values())roomStore.save(room)}catch(err){console.error("Final room save failed:",err.message);process.exitCode=1}
 for(const ws of wss.clients)ws.close(1012,"Server restarting");
 server.close(()=>process.exit(process.exitCode||0));setTimeout(()=>process.exit(process.exitCode||0),5000).unref();
}
process.on("SIGTERM",shutdown);process.on("SIGINT",shutdown);
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(ws.alive===false){ws.terminate();continue}ws.alive=false;ws.ping()}},30000);heartbeat.unref();
wss.on("connection",ws=>{ws.alive=true;ws.on("pong",()=>ws.alive=true);ws.on("error",()=>{})});
const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log(`SWADE Map Forge listening on ${PORT}`));

