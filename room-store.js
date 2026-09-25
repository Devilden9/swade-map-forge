"use strict";
const fs=require("fs"),path=require("path");

// Private server-side state. Never serve this directory over HTTP.
class RoomStore {
 constructor(directory){this.directory=directory;this.last=new Map();this.recovered=new Set();fs.mkdirSync(directory,{recursive:true,mode:0o700})}
 file(code){if(!/^[A-Z0-9_-]{1,24}$/.test(code))throw Error("Invalid room code");return path.join(this.directory,code+".json")}
 decode(raw,code){
  const data=JSON.parse(raw);
  if(data.version!==1||data.code!==code||!Array.isArray(data.clients)||!Array.isArray(data.deck)||!Array.isArray(data.discard))throw Error("Invalid room snapshot");
  if(data.state!==null&&(!data.state||!Array.isArray(data.state.items)))throw Error("Invalid room map");
  if(!data.clients.every(x=>Array.isArray(x)&&typeof x[0]==="string"&&x[1]&&typeof x[1].name==="string"&&typeof x[1].resumeToken==="string"))throw Error("Invalid participants");
  if(data.gmId&&!data.clients.some(([id])=>id===data.gmId))throw Error("Invalid GM");
  return {code,state:data.state,gmId:data.gmId,turnClientId:data.turnClientId,deck:data.deck,discard:data.discard,emptySince:data.emptySince,clients:new Map(data.clients.map(([id,c])=>[id,{...c,ws:null}]))};
 }
 load(){
  const rooms=new Map();
  for(const name of fs.readdirSync(this.directory).filter(n=>/^[A-Z0-9_-]{1,24}\.json$/.test(n))){
   const code=name.slice(0,-5),file=this.file(code);let raw,room;
   try{raw=fs.readFileSync(file,"utf8");room=this.decode(raw,code)}
   catch{try{raw=fs.readFileSync(file+".bak","utf8");room=this.decode(raw,code);this.recovered.add(code);console.warn("Recovered room from backup:",code)}catch{throw Error("Room storage is damaged: "+code+". Refusing to overwrite it.")}}
   rooms.set(code,room);this.last.set(code,raw);
  }
  return rooms;
 }
 save(room){
  const raw=JSON.stringify({version:1,code:room.code,state:room.state,gmId:room.gmId,turnClientId:room.turnClientId,deck:room.deck,discard:room.discard,emptySince:room.emptySince,clients:[...room.clients].map(([id,c])=>[id,{name:c.name,tokenId:c.tokenId,tokensByScene:c.tokensByScene,resumeToken:c.resumeToken,lastSeen:c.lastSeen}])});
  if(this.last.get(room.code)===raw&&!this.recovered.has(room.code))return;
  const file=this.file(room.code),temp=file+".tmp";const fd=fs.openSync(temp,"w",0o600);
  try{fs.writeFileSync(fd,raw,"utf8");fs.fsyncSync(fd)}finally{fs.closeSync(fd)}
  if(fs.existsSync(file)&&!this.recovered.has(room.code))fs.copyFileSync(file,file+".bak");
  fs.renameSync(temp,file);
  if(process.platform!=="win32"){const dir=fs.openSync(this.directory,"r");try{fs.fsyncSync(dir)}finally{fs.closeSync(dir)}}
  this.last.set(room.code,raw);this.recovered.delete(room.code);
 }
}
module.exports={RoomStore};

