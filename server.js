
const http = require("http");
const { WebSocketServer } = require("ws");
const path = require("path");
const fs = require("fs");

const server = http.createServer((req,res)=>{res.setHeader("Content-Type","text/html; charset=utf-8");fs.createReadStream(path.join(__dirname,"public/index.html")).pipe(res)});
const wss = new WebSocketServer({ server, path: "/ws" });

const rooms = new Map();
function getRoom(code){
  if(!rooms.has(code)) rooms.set(code,{state:null,clients:new Map(),gmId:null,turnClientId:null,deck:[],discard:[]});
  return rooms.get(code);
}
function send(ws,obj){ if(ws.readyState===1) ws.send(JSON.stringify(obj)); }
function broadcast(room,obj,except=null){
  const data=JSON.stringify(obj);
  for(const [id,c] of room.clients) if(id!==except && c.ws.readyState===1) c.ws.send(data);
}
function players(room){
  return [...room.clients].map(([id,c])=>({id,name:c.name,isGM:id===room.gmId,tokenId:c.tokenId||null,isTurn:id===room.turnClientId}));
}
wss.on("connection", ws=>{
  const clientId=Math.random().toString(36).slice(2,10)+Date.now().toString(36);
  let joinedCode=null;
  ws.on("message", raw=>{
    let msg; try{msg=JSON.parse(raw)}catch{return}
    if(msg.type==="join"){
      const code=String(msg.room||"").toUpperCase().replace(/[^A-Z0-9_-]/g,"").slice(0,24);
      if(!code) return send(ws,{type:"error",message:"Неверный код комнаты."});
      if(joinedCode)return;
      if(!String(msg.name||"").trim())return send(ws,{type:"error",message:"Введите имя."});
      joinedCode=code; const room=getRoom(code);
      room.clients.set(clientId,{ws,name:String(msg.name).trim().slice(0,32),tokenId:null});
      if(!room.gmId) room.gmId=clientId;
      send(ws,{type:"welcome",clientId,isGM:room.gmId===clientId,state:room.state,turnClientId:room.turnClientId});
      broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="transferGM" && joinedCode){
      const room=getRoom(joinedCode);
      if(room.gmId!==clientId)return send(ws,{type:"error",message:"Только текущий GM может передать роль."});
      if(msg.targetId===clientId||!room.clients.has(msg.targetId))return send(ws,{type:"error",message:"Игрок уже отключился."});
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
      if(room.state){for(const t of msg.state.items){const old=room.state.items?.find(i=>i.id===t.id);if(old)t.bennies=old.bennies||0}
      const byKey=new Map();for(const row of [...(room.state.battleLog||[]),...(msg.state.battleLog||[])])byKey.set(JSON.stringify(row),row);msg.state.battleLog=[...byKey.values()].sort((a,b)=>a.time.localeCompare(b.time));}
      room.state=msg.state;
      broadcast(room,{type:"state",clientId,state:msg.state},clientId);
    } else if(msg.type==="assignToken" && joinedCode){
      const room=getRoom(joinedCode); if(clientId!==room.gmId)return;
      const target=room.clients.get(String(msg.clientId||"")); if(!target)return;
      target.tokenId=String(msg.tokenId||""); broadcast(room,{type:"players",players:players(room)});
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
    room.clients.delete(clientId);
    if(room.gmId===clientId) room.gmId=room.clients.keys().next().value||null;
    if(room.turnClientId===clientId) room.turnClientId=null;
    if(room.clients.size===0) rooms.delete(joinedCode);
    else broadcast(room,{type:"players",players:players(room)});
  });
});
const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log(`SWADE Map Forge listening on ${PORT}`));

