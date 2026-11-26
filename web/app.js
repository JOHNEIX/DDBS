'use strict';
const $=id=>document.getElementById(id);
const state={token:'',user:null,view:'catalog',after:0,pages:[],rows:[],selected:[],busy:false,pending:null};
const messages={LOGIN_FAILED:'账号或密码错误',LOGIN_REQUIRED:'请先登录',FORBIDDEN:'没有此操作权限',INVALID_SESSION:'请刷新页面后重试',NOT_FOUND:'没有找到对应的学生或课程',ALREADY_ENROLLED:'已选修该课程',NOT_ENROLLED:'尚未选修该课程',NO_SEATS:'课程名额已满',NODE_UNAVAILABLE:'数据站点暂时离线',BLOCKED:'系统等待恢复，请联系管理员',RECOVERING:'系统正在恢复',INVALID_ARGUMENT:'请输入有效的编号',NETWORK_ERROR:'连接中断，请查询或重试原操作'};
const admin=()=>state.user?.role==='admin';
let noticeTimer;
function notice(text,error=false) { clearTimeout(noticeTimer); $('notice').textContent=text; $('notice').hidden=false; $('notice').classList.toggle('error',error); if(!error) noticeTimer=setTimeout(()=>$('notice').hidden=true,3500); }
function savePending(value) { state.pending=value; try{localStorage.setItem('ddbs.pending.'+state.user.username,JSON.stringify(value));}catch{} $('pending').hidden=!value; }
async function api(path,body) {
  let data;
  try { const response=await fetch('/api/'+path,body===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json','X-Session-Token':state.token},body:JSON.stringify(body)}); data=await response.json(); }
  catch { throw Object.assign(new Error(messages.NETWORK_ERROR),{code:'NETWORK_ERROR'}); }
  if(admin()&&data.trace) $('trace').textContent=data.trace.map(e=>JSON.stringify(e)).join('\n');
  if(!data.ok) { if(data.code==='LOGIN_REQUIRED') signedIn(null); throw Object.assign(new Error(messages[data.code]||data.message),{code:data.code}); }
  return data.data;
}
async function run(fn) { if(state.busy)return; state.busy=true; document.querySelectorAll('button,input').forEach(e=>e.disabled=true); try{await fn();}catch(e){notice(e.message,true);}finally{state.busy=false; document.querySelectorAll('button,input').forEach(e=>e.disabled=e.dataset.lock==='true');} }
function lock(el,value) { el.dataset.lock=String(value); el.disabled=state.busy||value; }
function action(text,fn,disabled=false) { const b=document.createElement('button'); b.textContent=text; b.onclick=()=>run(fn); lock(b,disabled); return b; }
function table(headers,rows) {
  $('columns').replaceChildren(); $('rows').replaceChildren();
  const tr=document.createElement('tr'); for(const name of headers){const th=document.createElement('th');th.textContent=name;tr.append(th);} $('columns').append(tr);
  for(const cells of rows){const row=document.createElement('tr');for(const value of cells){const td=document.createElement('td');value instanceof Node?td.append(value):td.textContent=value??'—';row.append(td);}$('rows').append(row);}
  if(!rows.length){const row=document.createElement('tr'),cell=document.createElement('td');cell.colSpan=headers.length;cell.className='empty';cell.textContent='暂无记录';row.append(cell);$('rows').append(row);}
}
function signedIn(user) {
  state.user=user; $('login-form').hidden=!!user; $('workspace').hidden=!user; $('account').hidden=!user;
  if(!user){$('password').value='';return;}
  $('identity').textContent=user.username+'（'+(admin()?'管理员':'学生')+'）';
  document.querySelectorAll('[data-admin]').forEach(e=>e.hidden=!admin());
  $('recover').hidden=true; $('schedule-nav').textContent=admin()?'学生课表':'我的课表';
  try{state.pending=JSON.parse(localStorage.getItem('ddbs.pending.'+user.username));}catch{state.pending=null;}
  $('pending').hidden=!state.pending;
}
async function load(view=state.view) {
  state.view=view; $('summary').hidden=true; $('recover').hidden=view!=='status'; $('pager').hidden=view!=='catalog';
  $('search-form').hidden=!['catalog','roster'].includes(view)&&!(view==='schedule'&&admin());
  $('reset-search').hidden=view!=='catalog'; $('trace-panel').hidden=!admin();
  $('search-label').textContent=view==='schedule'?'学生编号':'课程编号';
  $('title').textContent={catalog:'课程列表',schedule:admin()?'学生课表':'我的课表',roster:'课程选课名单',records:'操作记录',status:'系统管理'}[view];
  document.querySelectorAll('[data-view]').forEach(e=>e.classList.toggle('active',e.dataset.view===view));
  const value=$('search').value.trim();
  if(view==='catalog') {
    if(!admin()) state.selected=(await api('schedule')).courses;
    state.rows=await api('catalog?'+(value?'course_id='+value:'after_id='+state.after));
    table(['课程编号','课程名称','学分','剩余 / 总名额','操作'],state.rows.map(c=>{
      const selected=state.selected.some(e=>e.course_id===c.course_id);
      const button=admin()?action('名单',()=>{ $('search').value=c.course_id;return load('roster'); }):action(selected?'已选修':'选课',()=>write('enroll',c),selected||!c.remaining);
      return [c.course_id,c.course_name,c.credits,c.remaining===null?'站点离线':c.remaining+' / '+c.capacity,button];
    }));
    $('page').textContent='第 '+(state.pages.length+1)+' 页'; lock($('previous'),!state.pages.length||!!value);lock($('next'),state.rows.length<10||!!value);
  } else if(view==='schedule') {
    if(admin()&&!value){table(['学生编号','课程编号','课程名称','学分','选课时间'],[]);return;}
    const data=await api('schedule'+(admin()?'?student_id='+value:''));state.selected=data.courses;
    $('summary').hidden=false;$('summary').textContent=data.student.name+' · '+data.courses.length+' 门课程 · '+data.courses.reduce((s,c)=>s+c.credits,0)+' 学分';
    const headers=['课程编号','课程名称','学分','选课时间'];if(!admin())headers.push('操作');
    table(headers,data.courses.map(c=>{const cells=[c.course_id,c.course_name,c.credits,new Date(c.enrolled_at).toLocaleString('zh-CN')];if(!admin())cells.push(action('退课',()=>write('drop',c)));return cells;}));
  } else if(view==='roster') {
    const rows=await api('roster?course_id='+value);table(['学生编号','姓名','院系','选课时间'],rows.map(s=>[s.student_id,s.name,s.department,new Date(s.enrolled_at).toLocaleString('zh-CN')]));
  } else if(view==='records') {
    const rows=await api('records');table(['时间','操作者','学生','课程','操作','事务状态'],rows.map(t=>[new Date(t.timestamp).toLocaleString('zh-CN'),t.actor||'cli',t.student_id,t.course_id,t.action==='enroll'?'选课':'退课',t.event]));
  } else {
    const data=await api('status');$('summary').hidden=false;$('summary').textContent='系统状态：'+data.system_state+(data.last_error?' · '+data.last_error:'');
    table(['站点','状态','数据集版本'],data.nodes.map(n=>[n.node_id,n.reachable?'在线':'离线',n.dataset_version]));
    if(data.pending.length)$('summary').textContent+=' · 待恢复事务 '+data.pending.length+' 笔';
  }
}
async function write(kind,course,retry=false) {
  if(!retry&&state.pending)throw new Error('上次操作等待确认，请先查询结果或重试');
  if(kind==='drop'&&!retry&&!confirm('确认退选“'+course.course_name+'”（'+course.course_id+'）？'))return;
  const request=retry?state.pending:{action:kind,course_id:course.course_id,request_id:crypto.randomUUID?.()||'web-'+Date.now()+'-'+Math.random().toString(16).slice(2)};
  savePending(request);let result;
  try{result=await api(request.action,request);}catch(e){if(!['NETWORK_ERROR','BLOCKED','RECOVERING'].includes(e.code))savePending(null);throw e;}
  if(result.outcome!=='processing')savePending(null);
  notice(result.outcome==='success'?(request.action==='enroll'?'选课成功':'退课成功'):result.outcome==='aborted'?(messages[result.reason]||'操作已中止'):'操作处理中，请稍后查询',result.outcome!=='success');
  if(result.outcome!=='processing')await load();
}
$('login-form').onsubmit=e=>{e.preventDefault();run(async()=>{const user=await api('login',{username:$('username').value.trim(),password:$('password').value});signedIn(user);$('search').value='';await load('catalog');});};
$('logout').onclick=()=>run(async()=>{await api('logout',{});signedIn(null);$('notice').hidden=true;});
document.querySelectorAll('[data-view]').forEach(b=>b.onclick=()=>run(()=>{$('search').value='';state.after=0;state.pages=[];return load(b.dataset.view);}));
$('search-form').onsubmit=e=>{e.preventDefault();run(()=>load());};
$('reset-search').onclick=()=>run(()=>{$('search').value='';state.after=0;state.pages=[];return load();});
$('previous').onclick=()=>run(()=>{state.after=state.pages.pop();return load();});
$('next').onclick=()=>run(()=>{state.pages.push(state.after);state.after=state.rows.at(-1).course_id;return load();});
$('refresh').onclick=()=>run(()=>load());
$('recover').onclick=()=>run(async()=>{await api('recover',{});await load('status');});
$('retry').onclick=()=>run(()=>write(null,null,true));
$('check-request').onclick=()=>run(async()=>{const result=await api('request?request_id='+encodeURIComponent(state.pending.request_id));if(result.outcome!=='processing')savePending(null);notice({success:'操作已成功',aborted:'操作已中止',processing:'操作处理中'}[result.outcome]);if(result.outcome!=='processing')await load();});
run(async()=>{const session=await api('session');state.token=session.token;signedIn(session.user?.username?session.user:null);if(state.user)await load('catalog');});
