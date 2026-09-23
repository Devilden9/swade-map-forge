const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });
app.use(express.static(path.join(__dirname, "public")));

const rooms = new Map();
function getRoom(code){
  if(!rooms.has(code)) rooms.set(code,{state:null,clients:new Map(),gmId:null});
  return rooms.get(code);
}
function send(ws,obj){ if(ws.readyState===1) ws.send(JSON.stringify(obj)); }
function broadcast(room,obj,except=null){
  const data=JSON.stringify(obj);
  for(const [id,c] of room.clients) if(id!==except && c.ws.readyState===1) c.ws.send(data);
}
function players(room){
  return [...room.clients].map(([id,c])=>({id,name:c.name,isGM:id===room.gmId}));
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
      room.clients.set(clientId,{ws,name:String(msg.name||"Игрок").slice(0,32)});
      if(!room.gmId) room.gmId=clientId;
      send(ws,{type:"welcome",clientId,isGM:room.gmId===clientId,state:room.state});
      broadcast(room,{type:"players",players:players(room)});
    } else if(msg.type==="state" && joinedCode){
      const room=getRoom(joinedCode); room.state=msg.state;
      broadcast(room,{type:"state",clientId,state:msg.state},clientId);
    } else if(msg.type==="dice" && joinedCode){
      const room=getRoom(joinedCode), c=room.clients.get(clientId);
      broadcast(room,{type:"dice",clientId,name:c?.name||"Игрок",title:msg.title,total:msg.total,detail:msg.detail,cls:msg.cls},clientId);
    }
  });
  ws.on("close",()=>{
    if(!joinedCode)return; const room=rooms.get(joinedCode); if(!room)return;
    room.clients.delete(clientId);
    if(room.gmId===clientId) room.gmId=room.clients.keys().next().value||null;
    if(room.clients.size===0) rooms.delete(joinedCode);
    else broadcast(room,{type:"players",players:players(room)});
  });
});
const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log(`SWADE Map Forge: http://localhost:${PORT}`));
