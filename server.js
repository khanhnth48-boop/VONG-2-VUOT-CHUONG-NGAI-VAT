import express from 'express';
import {createServer} from 'node:http';
import {Server} from 'socket.io';
const app=express(),http=createServer(app),io=new Server(http);app.use(express.static('public'));
app.get('/', (req, res) => res.sendFile('index.html', { root: process.cwd() }));
const rooms=new Map();const makeCode=()=>Math.random().toString(36).slice(2,8).toUpperCase();
const clean=s=>String(s??'').trim().slice(0,160);
function view(r){return {code:r.code,question:r.question,number:r.number,open:r.open,seconds:r.seconds,deadline:r.deadline,teams:Object.fromEntries(Object.entries(r.teams).map(([k,v])=>[k,{name:v.name,connected:!!v.sid,answer:v.answer,submitted:v.submitted,score:v.score}])),revealed:r.revealed};}
function publish(r){io.to(r.code+':host').emit('state',view(r));io.to(r.code+':players').emit('question',{number:r.number,question:r.question,open:r.open,seconds:r.seconds,deadline:r.deadline,revealed:r.revealed});}
io.on('connection',s=>{
 s.on('create',(_,cb)=>{let code=makeCode();while(rooms.has(code))code=makeCode();let r={code,host:s.id,teams:{},question:'',number:0,open:false,seconds:30,revealed:false};rooms.set(code,r);s.join(code+':host');cb({ok:true,code});publish(r)});
 s.on('hostJoin',({code},cb)=>{const r=rooms.get(clean(code).toUpperCase());if(!r||r.host!==s.id)return cb({ok:false,error:'Phòng không tồn tại hoặc phiên giáo viên đã kết thúc.'});s.join(r.code+':host');cb({ok:true});publish(r)});
 s.on('join',({code,team},cb)=>{const r=rooms.get(clean(code).toUpperCase()),id=String(team);if(!r||!['1','2','3','4','5','6'].includes(id))return cb({ok:false,error:'Mã phòng hoặc số đội không hợp lệ'});if(r.teams[id]?.sid&&r.teams[id].sid!==s.id)return cb({ok:false,error:'Đội này đã có thiết bị tham gia'});r.teams[id]??={name:'Đội '+id,answer:'',submitted:false,score:0};r.teams[id].sid=s.id;s.data={code:r.code,team:id};s.join(r.code+':players');cb({ok:true,team:id});publish(r)});
s.on('openQuestion',({question,seconds},cb)=>{
  const r=[...rooms.values()].find(x=>x.host===s.id);
  if(!r)return cb?.({ok:false,error:'Không tìm thấy phòng giáo viên'});

  const q=clean(question);
  if(!q)return cb?.({ok:false,error:'Chưa nhập câu hỏi'});

  clearTimeout(r.timer);
  r.number=(r.number||0)+1;
  r.question=q;
  r.seconds=Math.max(5,Math.min(300,Number(seconds)||20));
  r.deadline=Date.now()+r.seconds*1000;
  r.open=true;
  r.revealed=false;

  Object.values(r.teams).forEach(t=>{
    t.answer='';
    t.submitted=false;
  });

  const questionNumber=r.number;

  r.timer=setTimeout(()=>{
    if(r.number!==questionNumber||!r.open)return;
    r.open=false;
    r.deadline=null;
    publish(r);
  },r.seconds*1000);

  publish(r);
  cb?.({ok:true});
});
 s.on('submit',({answer},cb)=>{const r=rooms.get(s.data?.code),t=r?.teams[s.data?.team];if(!r||!t||t.sid!==s.id||!r.open||Date.now()>=r.deadline||t.submitted)return cb?.({ok:false,error:'Không thể gửi đáp án'});t.answer=clean(answer);t.submitted=true;cb?.({ok:true});publish(r)});
 const r=[...rooms.values()].find(x=>x.host===s.id);if(r){clearTimeout(r.timer);
r.revealed=true;
r.open=false;
r.deadline=null;
publish(r);cb?.({ok:true})}});
 s.on('score',({team,delta})=>{const r=[...rooms.values()].find(x=>x.host===s.id);if(r?.teams[String(team)]){r.teams[String(team)].score+=Math.max(-100,Math.min(100,Number(delta)||0));publish(r)}});
 s.on('disconnect',()=>{if(s.data?.code){const r=rooms.get(s.data.code),t=r?.teams[s.data.team];if(t?.sid===s.id){t.sid=null;publish(r)}}for(const [code,r] of rooms)if(r.host===s.id){io.to(code+':players').emit('ended');rooms.delete(code)}})
});
http.listen(process.env.PORT||3000,()=>console.log('Olympia running on port '+(process.env.PORT||3000)));
