const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const path = require("path");
const fs = require("fs");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });
const publicDir = path.join(__dirname, "public");
app.get("/", (req,res)=>{
  try {
    const html=[0,1,2,3].map(i=>fs.readFileSync(path.join(publicDir,"index.part0"+i),"utf8")).join("");
    res.type("html").send(html);
  } catch(e) { res.status(500).send("Frontend load error"); }
});
app.use(express.static(publicDir));

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
      joinedCode=code; const room=getRoom(code);
      room.clients.set(clientId,{ws,name:String(msg.name||"Игрок").slice(0,32),tokenId:null});
      if(!room.gmId) room.gmId=clientId;
      send(ws,{type:"welcome",clientId,isGM:room.gmId===clientId,state:room.state,turnClientId:room.turnClientId});
      broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="state" && joinedCode){
      const room=getRoom(joinedCode);
      if(clientId!==room.gmId) return send(ws,{type:"error",message:"Только GM может редактировать карту."});
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
