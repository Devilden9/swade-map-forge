
const crypto = require("crypto");
const http = require("http");
const { WebSocketServer } = require("ws");
const path = require("path");
const fs = require("fs");

const server = http.createServer((req,res)=>{res.setHeader("Content-Type","text/html; charset=utf-8");fs.createReadStream(path.join(__dirname,"public/index.html")).pipe(res)});
const wss = new WebSocketServer({ server, path: "/ws" });

const rooms = new Map();
function getRoom(code){
  if(!rooms.has(code)) rooms.set(code,{state:null,clients:new Map(),gmId:null,turnClientId:null,deck:[],discard:[],emptySince:null});
  return rooms.get(code);
}
function send(ws,obj){ if(ws?.readyState===1) ws.send(JSON.stringify(obj)); }
function broadcast(room,obj,except=null){
  const data=JSON.stringify(obj);
  for(const [id,c] of room.clients) if(id!==except && c.ws?.readyState===1) c.ws.send(data);
}
function players(room){
  return [...room.clients].map(([id,c])=>({id,name:c.name,isGM:id===room.gmId,tokenId:c.tokenId||null,isTurn:id===room.turnClientId,online:c.ws?.readyState===1}));
}
wss.on("connection", ws=>{
  let clientId=crypto.randomUUID();
  let joinedCode=null;
  ws.on("message", raw=>{
    let msg; try{msg=JSON.parse(raw)}catch{return}
    if(msg.type==="join"){
      const code=String(msg.room||"").toUpperCase().replace(/[^A-Z0-9_-]/g,"").slice(0,24);
      if(!code) return send(ws,{type:"error",message:"Неверный код комнаты."});
      if(joinedCode)return;
      if(!String(msg.name||"").trim())return send(ws,{type:"error",message:"Введите имя."});
      joinedCode=code; const room=getRoom(code);
      const resumed=[...room.clients].find(([,c])=>typeof msg.resumeToken==="string"&&c.resumeToken===msg.resumeToken);
      let cl;
      if(resumed){clientId=resumed[0];cl=resumed[1];const old=cl.ws;cl.ws=ws;cl.lastSeen=Date.now();if(old&&old!==ws)old.close(4001,"Opened in another tab");}
      else{cl={ws,name:String(msg.name).trim().slice(0,32),tokenId:null,tokensByScene:{},resumeToken:crypto.randomBytes(32).toString("hex"),lastSeen:Date.now()};room.clients.set(clientId,cl)}
      room.emptySince=null;
      if(!room.gmId||!room.clients.get(room.gmId)?.ws)room.gmId=clientId;
      send(ws,{type:"welcome",clientId,name:cl.name,resumeToken:cl.resumeToken,resumed:!!resumed,isGM:room.gmId===clientId,state:room.state,turnClientId:room.turnClientId});
      broadcast(room,{type:"players",players:players(room)});
    } else if(joinedCode&&rooms.get(joinedCode)?.clients.get(clientId)?.ws!==ws){return;
    } else if(msg.type==="transferGM" && joinedCode){
      const room=getRoom(joinedCode);
      if(room.gmId!==clientId)return send(ws,{type:"error",message:"Только текущий GM может передать роль."});
      if(msg.targetId===clientId||room.clients.get(msg.targetId)?.ws?.readyState!==1)return send(ws,{type:"error",message:"Игрок уже отключился."});
      room.gmId=msg.targetId;broadcast(room,{type:"players",players:players(room)});
      broadcast(room,{type:"gmTransferred",name:room.clients.get(msg.targetId).name});
    } else if(msg.type==="benny" && joinedCode){
      const room=getRoom(joinedCode),cl=room.clients.get(clientId),delta=msg.delta;
      if(![1,-1].includes(delta)||!room.state)return;
      if(clientId!==room.gmId&&(delta!==-1||cl.tokenId!==msg.tokenId))return send(ws,{type:"error",message:"Игрок может тратить только свои фишки."});
      const t=room.state.items?.find(t=>t.type==="token"&&t.id===msg.tokenId);if(!t)return;
      const count=Math.max(0,Math.floor(Number(t.bennies)||0));if(count+delta<0)return;
      t.bennies=count+delta;
      room.state.battleLog=room.state.battleLog||[];
      room.state.battleLog.push({time:new Date().toISOString(),round:room.state.battleSession?.combatRound||1,actor:cl.name,text:`${t.name||"Токен"}: фишки ${delta>0?"+1":"−1"} (${count} → ${t.bennies})`});
      broadcast(room,{type:"state",clientId:"server",state:room.state});
    } else if(msg.type==="state" && joinedCode){
      const room=getRoom(joinedCode);
      if(clientId!==room.gmId) return send(ws,{type:"error",message:"Только GM может редактировать карту."});
      if(!msg.state||!Array.isArray(msg.state.items))return;
      if(room.state){for(const t of msg.state.items){const oldMap=room.state.activeSceneId===msg.state.activeSceneId?room.state:room.state.scenes?.find(s=>s.id===msg.state.activeSceneId)?.map;const old=oldMap?.items?.find(i=>i.id===t.id);if(old)t.bennies=old.bennies||0}
      const byKey=new Map();for(const row of [...(room.state.battleLog||[]),...(msg.state.battleLog||[])])byKey.set(JSON.stringify(row),row);msg.state.battleLog=[...byKey.values()].sort((a,b)=>a.time.localeCompare(b.time));}
      const sceneChanged=room.state?.activeSceneId!==msg.state.activeSceneId;
      room.state=msg.state;
      if(sceneChanged){for(const c of room.clients.values())c.tokenId=c.tokensByScene?.[room.state.activeSceneId||"default"]||null;room.turnClientId=null;broadcast(room,{type:"players",players:players(room)})}
      broadcast(room,{type:"state",clientId,state:msg.state},clientId);
    } else if(msg.type==="assignToken" && joinedCode){
      const room=getRoom(joinedCode); if(clientId!==room.gmId)return;
      const target=room.clients.get(String(msg.clientId||"")); if(!target)return;
      target.tokenId=String(msg.tokenId||"");target.tokensByScene=target.tokensByScene||{};target.tokensByScene[room.state?.activeSceneId||"default"]=target.tokenId; broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="setTurn" && joinedCode){
      const room=getRoom(joinedCode); if(clientId!==room.gmId)return;
      room.turnClientId=String(msg.clientId||"")||null;
      broadcast(room,{type:"turn",turnClientId:room.turnClientId});
      broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="moveToken" && joinedCode){
      const room=getRoom(joinedCode), cl=room.clients.get(clientId);
      if(!cl||room.turnClientId!==clientId||!cl.tokenId||cl.tokenId!==msg.tokenId||!room.state)return;
      const it=room.state.items?.find(x=>x.id===cl.tokenId&&x.type==="token"); if(!it)return;
      it.x=Number(msg.x)||0;it.y=Number(msg.y)||0;
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
  });
  ws.on("close",()=>{
    if(!joinedCode)return; const room=rooms.get(joinedCode); if(!room)return;
    const cl=room.clients.get(clientId);if(!cl||cl.ws!==ws)return;cl.ws=null;cl.lastSeen=Date.now();
    if(room.gmId===clientId)room.gmId=[...room.clients].find(([,c])=>c.ws?.readyState===1)?.[0]||clientId;
    if(![...room.clients.values()].some(c=>c.ws?.readyState===1))room.emptySince=Date.now();
    broadcast(room,{type:"players",players:players(room)});
  });
});
// Keep rooms and reconnect credentials for 24 hours after the last disconnect (in memory).
setInterval(()=>{for(const [code,r] of rooms){if(r.emptySince&&Date.now()-r.emptySince>86400000)rooms.delete(code)}},60000).unref();
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(ws.alive===false){ws.terminate();continue}ws.alive=false;ws.ping()}},30000);heartbeat.unref();
wss.on("connection",ws=>{ws.alive=true;ws.on("pong",()=>ws.alive=true);ws.on("error",()=>{})});
const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log(`SWADE Map Forge listening on ${PORT}`));

