import {activeStates,label,duration,messageText,outputMessage,applyEvent,definitionText,diffLines,createSessionAuth,httpError,redactSecret,consumeSse,normalizePermissions,dashboardPageAllowed,defaultDashboardPage,operationDisposition,operationHasRetrievableResult,approvalDisposition,approvalCanBeResolved,mutationOutcomeUnknown} from './state.js';

const $ = id => document.getElementById(id);
const auth=createSessionAuth();
auth.load();
const state = {runs:[],conversations:[],run:null,detail:null,span:null,page:'executions',view:'flow',tab:'conversation',sidebar:'runs',conversation:null,messages:[],lineage:null,loading:false,pending:[],selection:0,nextOffset:null,connection:null,lastEventId:null,session:null,permissions:normalizePermissions(null),control:{operations:null,approvals:null,suggestions:null,memory:null,statistics:null,runtime:null,health:null,admin:null},controlStatus:{},controlCursor:{operations:null,approvals:null},controlFilter:{operations:'',approvals:'pending'},controlResult:null,adminWorkforces:[],adminKeys:[]};
let authGeneration=0;
let controlRefreshTimer=null,controlPollTimer=null,confirmHandler=null,activeControlAction=false;
const kinds = {request:'Client request',foreman_decision:'Foreman · decide',agent_execution:'Specialist execution',agent_call:'Specialist · work',agent_creation:'Create specialist',meta_agent_creation:'Agent-generator',workflow_creation:'Create workflow',meta_workflow_creation:'Workflow-generator',workflow_execution:'Workflow execution',workflow_stage:'Workflow stage',internal_tool:'Internal operation',evaluation_cycle:'Evaluate & improve',evaluation:'Evaluator',composition:'Foreman · compose',client_tool:'Client tool'};
Object.assign(kinds,{definition_creation:'Create or reuse definition',creation_planning:'Evaluator · plan creation',consolidation_review:'Evaluator · compare duplicates'});
Object.assign(kinds,{foreman_output_contract:'Foreman · output contract'});
function h(tag, attrs={}, ...children) {
  const el=document.createElement(tag);
  const selectValue=tag==='select'&&attrs.value!=null?String(attrs.value):null;
  for (const [key,value] of Object.entries(attrs)) {
    if (key==='class') el.className=value;
    else if (key==='text') el.textContent=value;
    else if (key.startsWith('on')) el.addEventListener(key.slice(2).toLowerCase(),value);
    else if (key==='style') el.setAttribute('style',value);
    else if (tag==='select'&&key==='value') continue;
    else if (value!==false && value!=null) el.setAttribute(key,value===true?'':String(value));
  }
  for(const child of children.flat()) if(child!=null) el.append(child instanceof Node?child:document.createTextNode(String(child)));
  if(selectValue!=null)el.value=selectValue;
  return el;
}
async function request(path, options={}) {
  const usedToken=auth.token;
  const response=await auth.fetch(fetch,path,options);
  if(!response.ok){
    let detail='',data=null;
    if(response.status!==401&&response.status!==403){try{data=await response.json();detail=typeof data.error==='string'?data.error:(typeof data.error?.message==='string'?data.error.message:'');}catch{}}
    const error=httpError(response.status,detail,usedToken);
    if(data){error.data=data;error.code=data.error?.code;error.retryable=data.error?.retryable===true;}
    if(error.code==='service_draining')state.control.adminService={phase:'draining',accepting:false};
    if(response.status===401&&auth.token===usedToken){auth.clear();stopTrace();resetDashboardData();showAuthScreen(error.message,true);}
    throw error;
  }
  return response;
}
async function api(path, options) {
  const generation=authGeneration;let response;
  try{response=await request(path,options);}catch(error){if(generation!==authGeneration&&error?.status!==401)throw Object.assign(new Error('Session changed.'),{silent:true});throw error;}
  if(response.status===204||response.status===205){if(generation!==authGeneration)throw Object.assign(new Error('Session changed.'),{silent:true});return null;}
  let value;try{value=await response.json();}catch{if(generation!==authGeneration)throw Object.assign(new Error('Session changed.'),{silent:true});throw new Error('CCM returned an unreadable response.');}
  if(generation!==authGeneration)throw Object.assign(new Error('Session changed.'),{silent:true});
  return value;
}
const post=(path,body)=>api(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
let toastTimer;
function toast(text,error=false) {clearTimeout(toastTimer);$('toast').textContent=text;$('toast').className=`toast ${error?'error':''}`;$('toast').hidden=false;toastTimer=setTimeout(()=>{$('toast').hidden=true;},6500);}
function errorText(error){if(error?.code==='service_draining')return 'CCM is draining and rejected this new-work request. Existing operation, result, approval and read controls remain available.';if(mutationOutcomeUnknown(error))return `${error?.status?`CCM returned HTTP ${error.status}.`:'Could not get a definitive response from CCM.'} If this request changed server state, its outcome may be unknown; refresh the relevant state before retrying.`;return redactSecret(error?.message||'CCM request failed.',auth.token);}
function reportError(error){if(!error?.silent&&error?.status!==401){toast(errorText(error),true);if(error?.code==='service_draining')render();}}
const safe=fn=>(...args)=>{
  try{return Promise.resolve(fn(...args)).catch(reportError);}
  catch(error){reportError(error);}
};
function resetDashboardData(){
  authGeneration++;
  if($('action-dialog').open)$('action-dialog').close();if($('token-dialog').open)$('token-dialog').close();$('issued-token').value='';
  state.selection++;state.runs=[];state.conversations=[];state.run=null;state.detail=null;state.span=null;state.conversation=null;state.messages=[];state.lineage=null;state.pending=[];state.loading=false;state.nextOffset=null;state.lastEventId=null;
  state.session=null;state.permissions=normalizePermissions(null);state.control={operations:null,approvals:null,suggestions:null,memory:null,statistics:null,runtime:null,health:null,admin:null};state.controlStatus={};state.controlCursor={operations:null,approvals:null};state.controlFilter={operations:'',approvals:'pending'};state.controlResult=null;state.adminWorkforces=[];state.adminKeys=[];state.page='executions';clearTimeout(controlRefreshTimer);clearTimeout(controlPollTimer);controlRefreshTimer=null;controlPollTimer=null;confirmHandler=null;activeControlAction=false;
  clearTimeout(refreshTimer);clearTimeout(detailTimer);clearTimeout(workforceState.timer);workforceState.timer=null;workforceState.dirty=false;workforceState.loading=false;workforceState.snapshot=null;workforceState.selected=null;workforceState.history=null;workforceState.error='';workforceState.receipt=null;
  render();
}
function showAuthScreen(message='',invalid=false){
  $('workspace').hidden=true;$('auth-view').hidden=false;$('logout').hidden=true;$('dashboard-nav').hidden=true;$('toggle-inspector').hidden=true;
  const input=$('auth-token'),status=$('auth-message');input.value='';input.setAttribute('aria-invalid',invalid?'true':'false');
  status.textContent=message;status.hidden=!message;setConnection('error','Authentication required');input.focus();
}
function showDashboard(){
  $('auth-view').hidden=true;$('workspace').hidden=false;$('logout').hidden=false;$('dashboard-nav').hidden=false;$('toggle-inspector').hidden=false;
  $('auth-token').value='';$('auth-token').removeAttribute('aria-invalid');$('auth-message').textContent='';$('auth-message').hidden=true;setConnection('connecting','Connecting…');
  $('run-title').focus();
}
function setConnection(status,message){
  const node=$('connection');node.className=`connection ${status}`;node.replaceChildren(h('i'),document.createTextNode(message));
}
function stopTrace(){const connection=state.connection;state.connection=null;connection?.abort();}
async function beginSession(token){
  const submit=$('auth-submit');submit.disabled=true;submit.textContent='Connecting…';
  auth.setToken(token,{save:false});
  try{
    const session=await api('/api/session');
    state.session=session;state.permissions=normalizePermissions(session);state.page=defaultDashboardPage(state.permissions);
    auth.persist();showDashboard();state.lastEventId=null;
    if(state.permissions.roster_read){
      const initial=await refreshRuns();connect(initial.cursor||0);await safe(refreshConversations)();
      if(state.runs.length&&state.page==='executions')await safe(()=>selectRun(state.runs[0].id))();
    }
    render();
    if(['operations','approvals','improvements','settings','system','admin'].includes(state.page))void refreshControlPage();
    if(state.page==='workforce')void safe(refreshWorkforce)();
  }catch(error){
    if(error?.status===401)auth.clear();stopTrace();showAuthScreen(errorText(error),error?.status===401||error?.status===403);
  }finally{submit.disabled=false;submit.textContent='Connect dashboard';}
}
const current=()=>state.runs.find(r=>r.id===state.run);
const selected=()=>state.detail?.spans.find(s=>s.id===state.span);
const pretty=value=>typeof value==='string'?value:JSON.stringify(value,null,2);
const short=id=>id?String(id).slice(0,8):'—';
const workforceState={snapshot:null,selected:null,history:null,loading:false,dirty:false,timer:null,error:'',receipt:null};
const dateTime=ms=>ms==null?'No recorded use':new Date(ms).toLocaleString();
function serviceAccepting(){return (state.control.adminService||state.control.runtime?.service||state.control.adminRuntime?.service)?.accepting!==false;}
function scheduleWorkforce(){if(workforceState.timer)return;workforceState.timer=setTimeout(()=>{workforceState.timer=null;safe(refreshWorkforce)();},200);}
async function refreshWorkforce(){
  if(!state.permissions.roster_read){
    workforceState.snapshot=null;workforceState.error='';
    if(state.page==='workforce'){renderWorkforce();renderWorkforceInspector();}
    return;
  }
  if(workforceState.loading){workforceState.dirty=true;return;}
  workforceState.loading=true;workforceState.error='';
  try{
    workforceState.snapshot=await api('/api/workforce');
    if(workforceState.selected)await loadWorkforceHistory(workforceState.selected);
    if(state.page==='workforce'){renderWorkforce();renderWorkforceInspector();}
  }catch(error){workforceState.error=controlError(error);if(state.page==='workforce'){renderWorkforce();renderWorkforceInspector();}throw error;
  }finally{workforceState.loading=false;if(workforceState.dirty){workforceState.dirty=false;scheduleWorkforce();}}
}
async function loadWorkforceHistory(id){const history=await api(`/api/definitions/${encodeURIComponent(id)}`);if(workforceState.selected===id)workforceState.history=history;}
async function selectWorkforce(id){workforceState.selected=id;workforceState.history=null;$('workspace').classList.remove('inspector-hidden');renderWorkforce();renderWorkforceInspector();await loadWorkforceHistory(id);if(state.page==='workforce')renderWorkforceInspector();}
function renderWorkforce(){
  const snapshot=workforceState.snapshot;
  $('workforce-note').textContent=workforceState.error?`Refresh failed${snapshot?' · showing last successful snapshot':''}: ${workforceState.error}`:snapshot?`Live generated workforce · counts since ${dateTime(snapshot.tracking_since_ms)} (older activity is not backfilled). Rolling windows as of ${dateTime(snapshot.as_of_ms)}. Failed attempts count; tool continuations do not add assignments. Workflow model calls include its stage agents.`:state.permissions.roster_read?'Loading workforce…':'This key can change definitions but cannot read or enumerate the roster. Use an exact known ID for retirement or evaluation.';
  renderWorkforceActions();
  const query=$('workforce-search').value.toLowerCase(),kind=$('workforce-kind').value,status=$('workforce-status').value;
  const entities=(snapshot?.entities||[]).filter(e=>(!kind||e.kind===kind)&&(!status||(status==='retired')===e.retired)&&`${e.name} ${e.id} ${e.definition.purpose}`.toLowerCase().includes(query));
  $('workforce-list').replaceChildren(...entities.map(e=>{
    const windows=['1h','6h','1d','7d','30d'];
    return h('article',{class:`workforce-card ${e.id===workforceState.selected?'selected':''}`},
      h('button',{onClick:safe(()=>selectWorkforce(e.id))},`${e.name} ↗`),badge(e.retired?'retired':'active'),
      h('p',{},`${e.kind} · ${e.id}`),h('p',{},e.definition.purpose),
      h('p',{},`Last assignment: ${dateTime(e.last_assignment_ms)} · Last model call: ${dateTime(e.last_model_call_ms)}`),
      e.retired_ms?h('p',{},`Retired ${dateTime(e.retired_ms)} · catalogue removal ${dateTime(e.retired_ms+30*86400000)}; history retained`):null,
      h('table',{class:'usage-table'},h('thead',{},h('tr',{},h('th',{scope:'col'},'Previous'),...windows.map(w=>h('th',{scope:'col'},w)))),
        h('tbody',{},...['assignment','model_call'].map(k=>h('tr',{},h('th',{scope:'row'},k==='assignment'?'Assignments':'Model calls'),...windows.map(w=>h('td',{},e.usage[w][k])))))));
  }));
  if(snapshot&&!entities.length)$('workforce-list').append(empty('No matching workforce','Generated agents and workflows appear here automatically.'));
}
function renderWorkforceActions(){
  const target=$('workforce-actions'),saved=Object.fromEntries([...target.querySelectorAll('[data-persist]')].map(el=>[el.dataset.persist,el.type==='checkbox'?el.checked:el.value]));target.replaceChildren();
  if(workforceState.receipt)target.append(h('section',{class:'control-card receipt-card'},h('div',{class:'card-title-row'},h('h3',{},workforceState.receipt.title),h('button',{class:'button subtle',onClick:()=>{workforceState.receipt=null;renderWorkforceActions();}},'Dismiss')),h('pre',{},pretty(workforceState.receipt.value))));
  if(state.permissions.roster_write){
    target.append(h('details',{class:'workforce-action'},h('summary',{},'＋ Create agent through the generator'),definitionCreateForm('agent')),
      h('details',{class:'workforce-action'},h('summary',{},'＋ Create workflow through the generator'),definitionCreateForm('workflow')));
    if(!state.permissions.roster_read)target.append(h('details',{class:'workforce-action'},h('summary',{},'◇ Evaluate a known definition'),definitionEvaluateByIdForm()),h('details',{class:'workforce-action'},h('summary',{},'Retire a known definition'),definitionRetireByIdForm()));
  }
  const entity=workforceState.snapshot?.entities.find(item=>item.id===workforceState.selected);
  if(entity&&!entity.retired){
    if(state.permissions.roster_use)target.append(h('details',{class:'workforce-action'},h('summary',{},`▶ Run selected ${entity.kind}`),definitionRunForm(entity)));
    if(state.permissions.roster_write){
      target.append(h('details',{class:'workforce-action'},h('summary',{},`◇ Ask Evaluator to assess ${entity.name}`),definitionEvaluateForm(entity)),
        h('button',{class:'button amber retire-definition',onClick:()=>askConfirmation({title:`Retire ${entity.kind}?`,description:'Retirement removes this definition from operational selection while retaining its history and traces.',detail:`${entity.name} · ${entity.id}`,confirmLabel:'Retire definition',run:()=>retireDefinition(entity)})},'Retire selected definition'));
    }
  }
  for(const el of target.querySelectorAll('[data-persist]'))if(Object.hasOwn(saved,el.dataset.persist)){if(el.type==='checkbox')el.checked=saved[el.dataset.persist];else el.value=saved[el.dataset.persist];}
}
function definitionCreateForm(kind){
  const form=h('form',{class:'control-form',onsubmit:safe(async event=>{
    event.preventDefault();const requirements=form.elements.namedItem('requirements').value.trim();if(!requirements)return;
    const button=form.querySelector('button[type=submit]');button.disabled=true;button.textContent='Submitting to generator…';
    let completed=false;try{
      const value=await post('/api/management/definitions',{kind,requirements});
      workforceState.receipt={title:`${kind} registration result · ${value.status||'server-confirmed'}`,value};form.reset();
      try{await refreshWorkforce();}catch{}
      const id=value.resource?.id||value.entity_id||value.id;if(id&&workforceState.snapshot?.entities.some(item=>item.id===id))await selectWorkforce(id);
      completed=true;toast(`${kind} request completed by the server; creation does not execute the new definition.`);
    }finally{button.disabled=false;button.textContent='Create and register';if(completed)renderWorkforceActions();}
  })},h('label',{},'Requirements',h('textarea',{name:'requirements',rows:'3',required:true,maxlength:'12000','data-persist':`${kind}-requirements`,placeholder:`Describe the ${kind} requirements and expected behavior.`})),h('button',{class:'button primary',type:'submit',disabled:!serviceAccepting()},'Create and register'));
  return form;
}
function definitionRunForm(entity){
  const form=h('form',{class:'control-form',onsubmit:safe(async event=>{
    event.preventDefault();const message=form.elements.namedItem('message').value.trim();if(!message)return;
    const button=form.querySelector('button[type=submit]');button.disabled=true;button.textContent='Starting selected run…';
    try{
      const value=await post(`/api/management/definitions/${encodeURIComponent(entity.kind)}/${encodeURIComponent(entity.id)}/run`,{message});
      workforceState.receipt={title:`${entity.kind} run result`,value};form.reset();toast('The selected definition was run; server response recorded.');
      const runId=value.run_id||value.run?.id||value.receipt?.run_id;if(runId&&state.permissions.roster_read)await openRun(runId);
    }finally{button.disabled=false;button.textContent='Run selected definition';}
  })},h('p',{class:'muted'},`${entity.name} · ${entity.id}`),h('label',{},'Task message',h('textarea',{name:'message',rows:'3',required:true,maxlength:'12000','data-persist':`run-message-${entity.id}`,placeholder:'Give this selected definition a specific task.'})),h('button',{class:'button primary',type:'submit',disabled:!serviceAccepting()},'Run selected definition'));
  return form;
}
function definitionEvaluateForm(entity){
  const form=h('form',{class:'control-form',onsubmit:safe(async event=>{
    event.preventDefault();const task=form.elements.namedItem('task').value.trim(),result=form.elements.namedItem('result').value.trim();if(!task||!result)return;
    askConfirmation({title:`Ask Evaluator to assess ${entity.name}?`,description:'The evaluator may recommend or apply a revision or retirement. The returned server receipt will state whether a change was applied.',detail:pretty({task,result}),confirmLabel:'Request evaluation',run:async()=>{
      const button=form.querySelector('button[type=submit]');button.disabled=true;button.textContent='Requesting evaluator…';
      try{
        const value=await post(`/api/management/definitions/${encodeURIComponent(entity.kind)}/${encodeURIComponent(entity.id)}/evaluate`,{task,result});
        workforceState.receipt={title:'Evaluator result',value};form.reset();try{await refreshWorkforce();}catch{}toast('Evaluator result received. Any definition change remains evaluator-owned.');
      }finally{button.disabled=false;button.textContent='Evaluate definition';}
    }});
  })},h('p',{class:'warning-note'},'Assessment can recommend or apply an evaluator-owned revision or retirement.'),h('label',{},'Task evidence',h('textarea',{name:'task',rows:'2',required:true,maxlength:'12000','data-persist':`eval-task-${entity.id}`})),h('label',{},'Observed result',h('textarea',{name:'result',rows:'3',required:true,maxlength:'12000','data-persist':`eval-result-${entity.id}`})),h('button',{class:'button amber',type:'submit',disabled:!serviceAccepting()},'Evaluate definition'));
  return form;
}
function definitionEvaluateByIdForm(){
  const form=h('form',{class:'control-form',onsubmit:safe(async event=>{
    event.preventDefault();const kind=form.elements.namedItem('kind').value,id=form.elements.namedItem('id').value.trim(),task=form.elements.namedItem('task').value.trim(),result=form.elements.namedItem('result').value.trim();if(!id||!task||!result)return;
    askConfirmation({title:`Evaluate ${kind} ${id}?`,description:'This key cannot inspect the roster; verify the exact ID before invoking Evaluator.',detail:pretty({task,result}),confirmLabel:'Request evaluation',run:async()=>{
      const value=await post(`/api/management/definitions/${encodeURIComponent(kind)}/${encodeURIComponent(id)}/evaluate`,{task,result});
      workforceState.receipt={title:'Evaluator result',value};form.reset();clearWorkforceActionEdits('eval-unknown-');await refreshWorkforce();toast('Evaluator response received; any applied change is identified by the server receipt.');
    }});
  })},adminFormField('Definition kind',h('select',{name:'kind','data-persist':'eval-unknown-kind'},h('option',{value:'agent'},'Agent'),h('option',{value:'workflow'},'Workflow'))),adminFormField('Exact definition ID',h('input',{name:'id',required:true,'data-persist':'eval-unknown-id'})),adminFormField('Task evidence',h('textarea',{name:'task',rows:'2',required:true,maxlength:'12000','data-persist':'eval-unknown-task'})),adminFormField('Observed result',h('textarea',{name:'result',rows:'3',required:true,maxlength:'12000','data-persist':'eval-unknown-result'})),h('button',{class:'button amber',type:'submit',disabled:!serviceAccepting()},'Evaluate known definition'));
  return form;
}
function definitionRetireByIdForm(){
  const form=h('form',{class:'admin-inline-form',onsubmit:safe(async event=>{
    event.preventDefault();const kind=form.elements.namedItem('kind').value,id=form.elements.namedItem('id').value.trim();if(!id)return;
    askConfirmation({title:`Retire this ${kind}?`,description:'Verify the exact ID. Retirement removes the active definition but retains its history and traces.',detail:id,confirmLabel:'Retire definition',run:async()=>{
      const value=await post(`/api/management/definitions/${encodeURIComponent(kind)}/${encodeURIComponent(id)}/retire`,{});
      workforceState.receipt={title:'Retirement result',value};form.reset();clearWorkforceActionEdits('retire-unknown-');
      try{await refreshWorkforce();}catch{}renderWorkforceActions();toast('Server returned the retirement result; retained history remains available.');
    }});
  })},adminFormField('Kind',h('select',{name:'kind','data-persist':'retire-unknown-kind'},h('option',{value:'agent'},'Agent'),h('option',{value:'workflow'},'Workflow'))),adminFormField('Exact definition ID',h('input',{name:'id',required:true,'data-persist':'retire-unknown-id'})),h('button',{class:'button amber',type:'submit'},'Retire known definition'));
  return form;
}
function clearWorkforceActionEdits(prefix=''){for(const el of $('workforce-actions').querySelectorAll('[data-persist]'))if(!prefix||el.dataset.persist.startsWith(prefix))el.removeAttribute('data-persist');}
async function retireDefinition(entity){
  const value=await post(`/api/management/definitions/${encodeURIComponent(entity.kind)}/${encodeURIComponent(entity.id)}/retire`,{});
  workforceState.receipt={title:'Retirement result',value};try{await refreshWorkforce();}catch{}toast('Server returned the retirement result; retained history remains available.');
}
function renderWorkforceInspector(){
  const entity=workforceState.snapshot?.entities.find(e=>e.id===workforceState.selected),history=workforceState.history;
  $('inspector-title').textContent=entity?.name||(workforceState.selected?'Historical definition':'Select an agent or workflow');
  $('inspector-status').textContent=entity?(entity.retired?'Retired':'Active'):workforceState.selected?'History':'—';
  const target=$('inspector-body'),scroll=target.scrollTop,open=new Set([...target.querySelectorAll('details[open][data-history]')].map(el=>el.dataset.history));target.replaceChildren();
  if(!workforceState.selected){target.append(empty('Workforce inspector','Select a definition to inspect its full prompt, stages, versions and evaluations.'));return;}
  if(entity){
    target.append(h('h3',{},'Current definition'),h('pre',{class:'definition-text'},pretty(entity.definition)));
    if(entity.consolidated_into)target.append(h('button',{class:'button subtle',onClick:safe(()=>selectWorkforce(entity.consolidated_into))},`Consolidated into ${entity.consolidated_into} ↗`));
    const related=entity.kind==='workflow'?(entity.definition.stages||[]).map(s=>s.agent_id):(workforceState.snapshot.entities.filter(e=>e.kind==='workflow'&&(e.definition.stages||[]).some(s=>s.agent_id===entity.id)).map(e=>e.id));
    target.append(h('div',{class:'workforce-links'},...[...new Set(related)].map(id=>h('button',{class:'button subtle',onClick:safe(()=>selectWorkforce(id))},workforceState.snapshot.entities.find(e=>e.id===id)?.name||id))));
  }else target.append(h('p',{},'This operational definition is absent. Retained history is shown below.'));
  if(!history){target.append(h('p',{},'Loading history…'));return;}
  if(history.versions.length)target.append(h('button',{class:'button subtle',onClick:safe(()=>showDiff(history.entity_id,history.versions.at(-1).id))},'Compare versions'));
  for(const [title,items] of [['Definition versions',history.versions],['Changes and evidence',history.changes],['Evaluations',history.evaluations]]){
    target.append(h('h3',{class:'workforce-history'},`${title} (${items.length})`),...items.map((item,index)=>({item,key:`${history.entity_id}:${title}:${index}`})).reverse().map(({item,key})=>h('details',{'data-history':key,open:open.has(key)},h('summary',{},`${item.action||item.id||item.request_id} · ${dateTime(item.created_ms??item.at_ms??item.created_at*1000)}`),h('pre',{},pretty(item)))));
  }
  target.scrollTop=scroll;
}
function badge(status) {return h('span',{class:`badge ${status}`},label(status));}
function empty(title,text,action) {return h('div',{class:'empty'},h('div',{class:'empty-symbol'},'⌘'),h('h2',{},title),h('p',{},text),action||null);}
function time(ms) {return new Date(ms).toLocaleTimeString([], {hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});}
function versionFor(span){return state.detail?.versions.find(v=>v.id===span?.version_id);}
function entityName(span){const version=versionFor(span);return (version?.entity_id===span?.entity_id?version?.definition?.name:null)||span?.entity_id||'Unknown';}

let refreshTimer,detailTimer,renderTimer;
function scheduleRender(){if(renderTimer)return;renderTimer=requestAnimationFrame(()=>{renderTimer=null;render();});}
function scheduleRefresh(){clearTimeout(refreshTimer);refreshTimer=setTimeout(()=>safe(refreshRuns)(),250);}
function scheduleDetail(){if(detailTimer)return;detailTimer=setTimeout(()=>{detailTimer=null;safe(refreshDetail)();},180);}
async function refreshRuns(offset=0) {
  const result=await api(`/api/runs?offset=${offset}`);
  const map=new Map(state.runs.map(r=>[r.id,r]));result.runs.forEach(r=>map.set(r.id,r));
  state.runs=[...map.values()].sort((a,b)=>b.created_ms-a.created_ms);
  state.nextOffset=result.next_offset;
  renderSidebar();renderHeader();return result;
}
async function refreshConversations(){state.conversations=await api('/api/conversations');renderSidebar();}
async function selectRun(id){
  if(!state.permissions.roster_read)return;
  if(state.page!=='executions')state.page='executions';
  state.selection++;state.run=id;state.detail=null;state.span=null;state.lineage=null;state.pending=[];state.loading=false;state.messages=[];
  const run=current();state.conversation=run?.source==='web'?run.conversation_id:null;
  render();await refreshDetail();await refreshChat();
}
async function refreshDetail(){
  if(!state.run || state.loading)return;
  state.loading=true;const selection=state.selection;const run=state.run;let refreshImprovements=false;
  state.pending=[];
  try {
    const detail=await api(`/api/runs/${encodeURIComponent(run)}`);
    if(selection!==state.selection)return;
    const oldEvents=state.detail?.events||[];
    detail.events=[...new Map([...oldEvents,...(detail.events||[])].map(e=>[e.seq,e])).values()].sort((a,b)=>a.seq-b.seq);
    refreshImprovements=state.pending.some(event=>event.kind==='improvement.changed'&&event.seq>detail.cursor);
    for(const event of state.pending) applyEvent(detail,event);
    state.detail=detail;
    if(!state.span) state.span=detail.spans.findLast(s=>!['request','workflow_execution','agent_execution'].includes(s.kind))?.id||detail.spans[0]?.id;
    render();
  } finally {if(selection===state.selection){state.loading=false;state.pending=[];if(refreshImprovements)scheduleDetail();}}
}
function handleTraceEvent(event){
  if(event.event==='trace_error'){
    try{toast(redactSecret(JSON.parse(event.data).error||'The trace stream reported an error.',auth.token),true);}catch{toast('The trace stream reported an error.',true);}
    return;
  }
  if(event.event==='trace_reset'){
    safe(async()=>{
      state.selection++;state.detail=null;state.pending=[];state.loading=false;state.runs=[];
      await refreshRuns();await refreshConversations();await refreshDetail();await refreshChat();scheduleWorkforce();
    })();
    return;
  }
  if(event.event!=='trace')return;
  const value=JSON.parse(event.data);
  if(['workforce.changed','definition.changed','system.reset'].includes(value.kind))scheduleWorkforce();
  if(['workforce.changed','definition.changed','system.reset','request.started','request.finished','async.operation','tool.waiting','tool.approval_requested','control.accepted','control.delivered','improvement.changed'].includes(value.kind))scheduleControlRefresh();
  if(value.run_id===state.run){
    if(state.loading)state.pending.push(value);
    if(applyEvent(state.detail,value)){
      if($('follow').checked && ['span.started','model.delta'].includes(value.kind) && value.span_id) state.span=value.span_id;
      if(value.kind==='request.finished'){const run=current();if(run)run.status=value.data.status;safe(refreshChat)();}
      if(value.kind==='execution.paused'){const run=current();if(run)run.status='paused';}
      if(value.kind==='execution.resumed'){const run=current();if(run)run.status='running';}
      scheduleRender();
    }
    if(['span.started','request.started','definition.changed','tool.waiting','tools.returned','request.finished','improvement.changed','creation.policy','creation.policy_changed','creation.deferred'].includes(value.kind))scheduleDetail();
    if(value.kind==='conversation.message')safe(refreshChat)();
  }
  if(value.kind==='system.reset'){scheduleDetail();toast('Workforce reset. Trace and definition history preserved.');}
  if(!['model.delta','span.input','response.delta'].includes(value.kind))scheduleRefresh();
  if(value.kind==='conversation.message')safe(refreshConversations)();
}
function connect(cursor){
  stopTrace();state.lastEventId=null;
  const controller=new AbortController();state.connection=controller;
  let retry=500,reported=false;
  const wait=ms=>new Promise(resolve=>{
    if(controller.signal.aborted){resolve();return;}
    let timer;
    const finish=()=>{clearTimeout(timer);controller.signal.removeEventListener('abort',finish);resolve();};
    timer=setTimeout(finish,ms);controller.signal.addEventListener('abort',finish,{once:true});
  });
  const run=async()=>{
    while(!controller.signal.aborted){
      try{
        const headers=new Headers({'accept':'text/event-stream'});
        if(state.lastEventId)headers.set('Last-Event-ID',state.lastEventId);
        const response=await request(`/api/events?after=${cursor||0}`,{headers,signal:controller.signal});
        if(!response.body)throw new Error('CCM did not return a live trace stream.');
        setConnection('live','Live connection');scheduleWorkforce();reported=false;retry=500;
        await consumeSse(response.body,event=>{if(event.id)state.lastEventId=event.id;handleTraceEvent(event);});
        if(controller.signal.aborted)return;
        setConnection('reconnecting','Reconnecting…');
      }catch(error){
        if(controller.signal.aborted||error?.status===401)return;
        if(error?.status===403){setConnection('error','Access denied (403)');toast(error.message,true);return;}
        setConnection('reconnecting','Connection interrupted — retrying');
        if(!reported){toast(errorText(error),true);reported=true;}
      }
      await wait(retry);retry=Math.min(retry*2,10000);
    }
  };
  void run();
}

function renderSidebar(){
  const search=$('search').value.toLowerCase();const filter=$('status-filter').value,source=$('source-filter').value;
  const list=$('run-list');
  if(state.sidebar==='conversations'){
    $('list-heading').textContent='NATIVE CONVERSATIONS';$('run-count').textContent=state.conversations.length;
    list.replaceChildren(...state.conversations.filter(c=>`${c.title} ${c.id}`.toLowerCase().includes(search)).map(c=>h('button',{class:`run-row ${state.conversation===c.id?'active':''}`,onClick:safe(async()=>{
      state.conversation=c.id;const run=state.runs.find(r=>r.conversation_id===c.id);if(run)await selectRun(run.id);else{state.run=null;state.detail=null;state.span=null;await refreshChat();}setView('chat');
    })},h('div',{class:'run-row-title'},h('span',{class:'status-dot completed'}),h('span',{},c.title)),h('div',{class:'run-row-meta'},h('span',{},'Web conversation'),h('span',{},short(c.id))))));
    $('load-more').hidden=true;return;
  }
  const runs=state.runs.filter(r=>(!source||r.source===source)&&(!filter||(filter==='active'?activeStates.has(r.status):r.status===filter))&&`${r.title} ${r.id} ${r.conversation_id||''}`.toLowerCase().includes(search));
  $('list-heading').textContent='RECENT EXECUTIONS';$('run-count').textContent=runs.length;
  list.replaceChildren(...runs.map(run=>h('button',{class:`run-row ${run.id===state.run?'active':''}`,onClick:safe(()=>selectRun(run.id))},
    h('div',{class:'run-row-title'},h('span',{class:`status-dot ${run.status}`}),h('span',{},run.title)),
    h('div',{class:'run-row-meta'},h('span',{},run.source==='web'?'Web chat':run.source==='improvement'?'Auto-improvement':'API · '+short(run.id)),h('span',{},time(run.created_ms))))));
  if(!runs.length)list.append(h('div',{class:'empty small'},'No matching executions.'));
  $('load-more').hidden=state.nextOffset==null;
}
function renderHeader(){
  if(state.page==='workforce'){
    $('run-title').textContent='Workforce';$('run-eyebrow').textContent='LIVE AGENTS & WORKFLOWS';$('run-subtitle').textContent='Definitions, usage and retained improvement history';$('export').disabled=true;$('pause-run').disabled=true;$('event-status').textContent='Workforce updates automatically · rolling counts refresh every minute';return;
  }
  if(state.page!=='executions'){
    const titles={run:'Run workforce item',operations:'Operations',approvals:'Tool approvals',improvements:'Improvements',settings:'Your settings',system:'System health & metrics',admin:'Administration',access:'Access overview'};
    $('run-title').textContent=titles[state.page]||'Control room';$('run-eyebrow').textContent='SYSTEM CONTROL';$('run-subtitle').textContent=state.session?`${state.session.workforce?.name||state.session.workforce_name||'Authenticated workspace'} · permissions are independent`:'Permission-scoped dashboard';$('export').disabled=true;$('pause-run').disabled=true;
    const status=state.controlStatus[state.page];$('event-status').textContent=status?.loading?'Refreshing from CCM…':status?.error?`Refresh failed · ${status.stale?'showing last successful data':'no current data'}`:status?.updated_ms?`Updated ${dateTime(status.updated_ms)} · server state`:'Ready';return;
  }
  const run=current(),spans=state.detail?.spans||[];
  $('run-title').textContent=run?.title||'Execution workspace';
   $('run-eyebrow').textContent=run?(run.source==='web'?'NATIVE CONVERSATION':run.source==='improvement'?'AUTOMATIC IMPROVEMENT':'EXTERNAL API EXECUTION'):'EXECUTION WORKSPACE';
  $('run-subtitle').textContent=run?`${short(run.id)} · ${new Date(run.created_ms).toLocaleString()}${run.conversation_id?' · conversation '+short(run.conversation_id):' · no client conversation ID supplied'}`:'Watch decisions unfold. Understand every handoff.';
  $('metric-state').textContent=run?label(run.status):'Ready';$('metric-state').style.color=run?.status==='error'?'var(--error)':run&&activeStates.has(run.status)?'var(--accent)':'';
  $('metric-ops').textContent=run?spans.filter(s=>s.kind!=='request').length:'—';
  $('metric-time').textContent=run?duration((activeStates.has(run.status)?Date.now():run.updated_ms)-run.created_ms):'—';
  const measured=spans.filter(s=>s.usage && (s.usage.total_tokens!=null || s.usage.prompt_tokens!=null));
  const tokens=measured.reduce((n,s)=>n+(s.usage.total_tokens??((s.usage.prompt_tokens||0)+(s.usage.completion_tokens||0))),0);
  $('metric-tokens').textContent=measured.length?tokens.toLocaleString():'—';$('metric-tokens').title='Provider-reported tokens only; missing usage is not zero.';
  $('export').disabled=!run;$('pause-run').disabled=!run||!activeStates.has(run.status);
  $('pause-run').textContent=isPaused('*')?'▷ Resume run':'Ⅱ Pause run';
  $('event-status').textContent=state.detail?`${spans.length} operations · event #${state.detail.cursor}`:'Waiting for an execution';
}
function renderFlow(){
  const target=$('flow');
  if(!state.detail){target.replaceChildren(empty(state.run?'Loading execution…':'Your control room is ready',state.run?'Retrieving the durable execution record.':'Start a conversation here, or send a request from your API client. Every decision and handoff will appear as it happens.',state.run?null:h('button',{class:'button primary',onClick:newChat},'＋ Start a conversation')));return;}
  const spans=state.detail.spans;const byId=new Map(spans.map(s=>[s.id,s]));
  const depth=span=>{let n=0,p=span.parent_id,seen=new Set();while(p&&byId.has(p)&&!seen.has(p)){seen.add(p);n++;p=byId.get(p).parent_id;}return Math.min(n,5);};
  target.replaceChildren(h('div',{class:'flow-root'},'EXECUTION TRACE · '+short(state.run)),...spans.map(span=>{
    const kind=span.kind,model=span.input?.model;let note='';
    if(kind==='foreman_decision') {try {const decision=JSON.parse(messageText(outputMessage(span)));note=decision.note||decision.requirements||decision.name||'';}catch{}}
    if(kind==='workflow_stage')note=span.input?.stage?.instruction||'';
    const icon=kind.includes('evaluation')?'◇':kind.includes('tool')?'⌁':kind.includes('workflow')?'⌘':kind==='request'?'↳':'✦';
    return h('div',{class:'node-wrap',style:`--depth:${depth(span)}`},h('button',{class:`node ${span.status} ${state.span===span.id?'selected':''}`,onClick:()=>{state.span=span.id;state.lineage=null;$('follow').checked=false;$('workspace').classList.remove('inspector-hidden');render();}},
      h('div',{class:'node-heading'},h('span',{class:`node-icon ${kind.includes('evaluation')?'evaluation':kind.includes('tool')?'tool':''}`},icon),h('div',{class:'node-main'},h('div',{class:'node-name'},kinds[kind]||label(kind)),h('div',{class:'node-entity mono'},entityName(span))),badge(span.status)),
      h('div',{class:'node-detail mono'},h('span',{},model||`span ${short(span.id)}`),h('span',{},duration((span.ended_ms||Date.now())-span.started_ms))),note?h('div',{class:'node-note'},note):null));
  }));
}
function messageView(message,name){
  const role=message.role||'assistant';const body=messageText(message);
  const box=h('div',{class:`message ${role==='assistant'?'output':role==='system'?'system':role==='operator'?'operator':''}`},h('div',{class:'message-header'},h('span',{class:'message-avatar'},role==='assistant'?'✦':role==='tool'?'⌁':role==='system'?'S':'↗'),h('span',{},name||label(role))));
  if(message.ccm_narration)box.append(h('div',{class:'message-body'},message.ccm_narration));
  const reasoning=message.reasoning_content||message.reasoning;
  if(reasoning)box.append(h('details',{},h('summary',{},'Thinking · reasoning and operational observations'),h('pre',{},reasoning)));
  if(role==='system')box.append(h('details',{},h('summary',{},'System instructions · '+body.length.toLocaleString()+' characters'),h('pre',{},body)));
  else if(body)box.append(h('div',{class:'message-body'},body));
  if(message.tool_calls?.length)box.append(h('details',{open:true},h('summary',{},`${message.tool_calls.length} tool call(s)`),h('pre',{},pretty(message.tool_calls))));
  return box;
}
function renderInspector(){
  if(state.page==='workforce'){renderWorkforceInspector();return;}
  const span=selected(),target=$('inspector-body');
  $('inspector-title').textContent=span?(kinds[span.kind]||label(span.kind)):'Select an operation';
  $('inspector-status').replaceWith(Object.assign(badge(span?.status||'idle'),{id:'inspector-status'}));
  if(!span){target.replaceChildren(h('div',{class:'empty small'},'Select a node to inspect its exact inputs, output, definition and controls.'));renderSteering();return;}
  const atBottom=target.scrollHeight-target.scrollTop-target.clientHeight<70;
  const scroll=target.scrollTop;
  if(state.tab==='raw'){
    target.replaceChildren(h('button',{class:'button subtle',onClick:safe(async()=>{await navigator.clipboard.writeText(pretty(span));toast('Operation copied.');})},'Copy operation JSON'),h('pre',{style:'margin-top:14px'},pretty(span)));
  }else if(state.tab==='definition'){
    const version=versionFor(span);
    target.replaceChildren(h('div',{class:'definition-meta'},h('strong',{},'Executed definition · immutable snapshot'),h('div',{class:'mono'},version?.id||'No definition attached to this operation'),h('div',{},version?`Captured ${new Date(version.created_ms).toLocaleString()}`:'Request and tool events retain their own input artifacts.')));
    if(version){
      target.append(h('div',{class:'definition-actions'},h('button',{class:'button subtle',onClick:safe(()=>showDiff(version.entity_id,version.id))},'⇄ Compare versions')),h('pre',{class:'definition-text'},definitionText(version.definition)));
      const changes=state.detail.changes.filter(c=>c.entity_id===version.entity_id);
      if(changes.length)target.append(h('div',{class:'eyebrow',style:'margin-top:25px'},'CHANGE EVIDENCE'),...changes.map(c=>h('div',{class:'revision'},h('strong',{},`${label(c.action)} · ${c.applied?'applied':'not applied'}`),h('p',{},c.evidence?.reason||pretty(c.evidence)),h('p',{},c.evidence?.evidence||''))));
    }
  }else{
    let messages=span.input?.messages||span.input?.input?.messages;
    target.replaceChildren();
    if(span.kind==='client_tool')target.append(messageView({role:'tool',content:pretty(span.input)},'Tool call · executed by originating client'));
    else if(messages)target.append(...messages.map(m=>messageView(m)));
    else target.append(h('details',{open:true},h('summary',{},'Operation input'),h('pre',{},pretty(span.input))));
    if(span.status==='running')target.append(h('div',{class:'streaming-label'},span.partial?'Receiving live output':'Waiting for model output'));
    const output=outputMessage(span);
    if(output)target.append(messageView(output,entityName(span)));
    else if(span.output)target.append(h('details',{open:true},h('summary',{},'Operation result'),h('pre',{},pretty(span.output))));
    if(span.kind==='client_tool'&&span.status==='waiting_tools')target.append(h('div',{class:'definition-meta'},'Waiting for the API client to execute this tool and return its result.'));
  }
  target.scrollTop=atBottom&&state.tab==='conversation'?target.scrollHeight:scroll;
  renderSteering();
}
function isPaused(target){const controls=state.detail?.controls||[];const latest=controls.findLast(c=>c.target===target&&['pause','resume','interrupt'].includes(c.action));return latest?.action==='pause';}
function renderSteering(){
  const span=selected(),run=current(),enabled=run&&activeStates.has(run.status)&&span&&!['client_tool','internal_tool','workflow_execution'].includes(span.kind);
  $('steering-target').textContent=span?entityName(span):'No target';
  for(const id of ['guide','interrupt','pause-agent'])$(id).disabled=!enabled;
  $('pause-agent').textContent=isPaused(span?.entity_id)?'▷':'Ⅱ';
  $('pause-agent').title=isPaused(span?.entity_id)?'Resume this agent':'Pause at the next boundary';
  const controls=(state.detail?.controls||[]).filter(c=>c.target==='*'||c.target===span?.entity_id).slice(-5).reverse();
  $('controls').replaceChildren(...controls.map(c=>h('div',{class:'control',title:c.message},h('span',{},label(c.action)+(c.message?' · '+c.message.slice(0,40):'')),h('span',{},label(c.status)))));
}
async function sendControl(action,wholeRun=false){
  const span=selected();const target=wholeRun?'*':span?.entity_id;if(!state.run||!target)return;
  const message=['guidance','interrupt'].includes(action)?$('guidance').value:'';
  if(['guidance','interrupt'].includes(action)&&!message.trim()){toast('Enter guidance before sending.',true);$('guidance').focus();return;}
  const result=await post(`/api/runs/${encodeURIComponent(state.run)}/control`,{target,action,message});
  if(message)$('guidance').value='';toast(`${label(action)} ${result.status}.`);await refreshDetail();
}

function renderTimeline(){
  const events=(state.detail?.events||[]).filter(e=>$('show-deltas').checked||!['model.delta','span.input'].includes(e.kind));
  $('timeline').replaceChildren(...events.map(event=>h('div',{class:'timeline-row'},h('time',{class:'mono',title:new Date(event.at_ms).toISOString()},time(event.at_ms)+'.'+String(event.at_ms%1000).padStart(3,'0')),h('div',{},h('strong',{},event.kind),h('details',{},h('summary',{class:'mono'},`#${event.seq} · ${short(event.span_id)}`),h('pre',{},pretty(event.data)))))));
  if(!events.length)$('timeline').append(h('div',{class:'empty small'},'No timeline events loaded.'));
}
async function loadHistory(){
  if(!state.run)return;const run=state.run;let after=0;const events=[];$('load-history').disabled=true;
  try{while(true){const page=await api(`/api/events/page?run=${encodeURIComponent(run)}&after=${after}`);events.push(...page.events);if(page.events.length<500)break;after=page.events.at(-1).seq;}
    if(state.run===run&&state.detail){state.detail.events=[...new Map([...events,...state.detail.events].map(e=>[e.seq,e])).values()].sort((a,b)=>a.seq-b.seq);renderTimeline();}
  }finally{$('load-history').disabled=false;}
}

async function refreshChat(){
  if(state.conversation){const id=state.conversation;const result=await api(`/api/conversations/${encodeURIComponent(id)}`);if(state.conversation===id)state.messages=result.messages;}
  else{const root=state.detail?.spans.findLast(s=>s.kind==='request');state.messages=[...(root?.input?.messages||[])];if(root?.output?.role)state.messages.push(root.output);}
  renderChat();
}
function renderChat(){
  const target=$('chat-history'),atBottom=target.scrollHeight-target.scrollTop-target.clientHeight<90;const previous=target.scrollTop;
  const run=current();const external=run&&run.source!=='web';
  target.replaceChildren(...state.messages.map(m=>messageView(m,m.role==='assistant'?'Foreman':undefined)));
  const root=state.detail?.spans.findLast(s=>s.kind==='request');
  const presentation=state.detail?.presentations?.[root?.request_id];
  if(presentation&&['running','paused'].includes(root?.status)&&(presentation.narration||presentation.reasoning)){
    target.append(messageView({role:'assistant',content:presentation.narration,reasoning_content:presentation.reasoning},'Foreman · live progress'));
  }
  const live=state.detail?.spans.findLast(s=>s.kind==='composition'&&s.status==='running');
  if(live){const message=outputMessage(live);if(message){const visible={...message};delete visible.reasoning;delete visible.reasoning_content;if(messageText(visible))target.append(messageView(visible,'Foreman · composing'));}else target.append(h('div',{class:'streaming-label'},'Foreman is composing'));}
  else if(run&&activeStates.has(run.status))target.append(h('div',{class:'streaming-label'},run.status==='waiting_tools'?'Waiting for client tools':run.status==='paused'?'Execution paused':'Your workforce is working — open Execution to follow it'));
  if(!state.messages.length&&!live&&!run)target.append(empty('Talk to Foreman','Give your workforce a task. Follow its decisions and step in whenever you need to.'));
   $('chat-input').disabled=Boolean(!state.permissions.roster_use||!serviceAccepting()||external);$('send-chat').disabled=Boolean(!state.permissions.roster_use||!serviceAccepting()||external||(run&&activeStates.has(run.status)));
   $('chat-hint').textContent=!state.permissions.roster_use?'This key can read traces but cannot submit Foreman requests.':!serviceAccepting()?'Service is draining; new Foreman requests are disabled.':external?'API client conversation · use the inspector to steer':run&&activeStates.has(run.status)?'Run active · send guidance through the inspector':'Native chat · same pipeline, full visibility';
  target.scrollTop=atBottom?target.scrollHeight:previous;
}
async function sendChat(event){
  event.preventDefault();if(!state.permissions.roster_use){toast('Roster-use permission is required to message Foreman.',true);return;}if(!serviceAccepting()){toast('CCM is draining and is not accepting new Foreman work.',true);return;}const message=$('chat-input').value;if(!message.trim())return;$('send-chat').disabled=true;
  try{const result=await post('/api/chat',{conversation_id:state.conversation,message});$('chat-input').value='';state.conversation=result.conversation_id;await refreshRuns();await refreshConversations();await selectRun(result.run_id);setView('chat');}
  finally{renderChat();}
}
function newChat(){if(!state.permissions.roster_use){toast('Roster-use permission is required to start a Foreman conversation.',true);return;}state.selection++;state.run=null;state.detail=null;state.span=null;state.conversation=null;state.messages=[];state.loading=false;state.pending=[];setView('chat');render();$('chat-input').focus();}
function syncNavigation(){
  const workforce=state.page==='workforce';
  const control=state.page!=='executions'&&!workforce;
  $('workspace').classList.toggle('control-mode',control);
  $('workspace').classList.toggle('workforce-mode',workforce);
  $('workforce-view').hidden=!workforce;
  $('control-view').hidden=!control;
  $('run-header').hidden=control;$('metrics').hidden=control;$('improvements').hidden=control;$('view-toolbar').hidden=control;
  for(const name of ['flow','timeline','chat'])$(`${name}-view`).hidden=workforce||control||name!==state.view;
  document.querySelectorAll('[data-page]').forEach(b=>{const allowed=dashboardPageAllowed(b.dataset.page,state.permissions);b.hidden=!allowed;const active=b.dataset.page===state.page;b.classList.toggle('active',active);if(active)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');});
  document.querySelectorAll('[data-view]').forEach(b=>b.classList.toggle('active',b.dataset.view===state.view));
  document.querySelector('[data-view="chat"]').hidden=!state.permissions.roster_use;$('new-chat').hidden=!state.permissions.roster_use;
  $('inspector-eyebrow').textContent=workforce?'WORKFORCE INSPECTOR':'OPERATION INSPECTOR';
}
function setPage(page){if(!dashboardPageAllowed(page,state.permissions)){toast('This key does not have access to that dashboard area.',true);return;}state.page=page;if(page==='workforce')safe(refreshWorkforce)();else if(['operations','approvals','improvements','settings','system','admin'].includes(page))void refreshControlPage();else if(state.view==='chat')safe(refreshChat)();render();}
function setView(view){state.view=view;setPage('executions');}

async function showDiff(entity,executed){
  state.lineage=await api(`/api/definitions/${encodeURIComponent(entity)}`);const versions=state.lineage.versions;if(!versions.length){toast('No retained definition versions.');return;}
  const options=()=>versions.map((v,index)=>h('option',{value:v.id},`v${index+1} · ${short(v.id)} · ${new Date(v.created_ms).toLocaleString()}`));
  $('diff-before').replaceChildren(...options());$('diff-after').replaceChildren(...options());
  const index=Math.max(0,versions.findIndex(v=>v.id===executed));$('diff-before').value=versions[Math.max(0,index-1)].id;$('diff-after').value=versions[index].id;
  $('diff-title').textContent=`${entity} · ${versions.length} retained version(s)`;renderDiff();$('diff-dialog').showModal();
}
async function downloadExport(){
  if(!state.run)return;
  const generation=authGeneration,run=state.run,response=await request(`/api/runs/${encodeURIComponent(run)}/otlp`),blob=await response.blob();
  if(generation!==authGeneration)return;
  const url=URL.createObjectURL(blob);
  const link=h('a',{href:url,download:`ccm-${short(run)}-otlp.json`});document.body.append(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
function renderDiff(){
  const versions=state.lineage?.versions||[];const before=versions.find(v=>v.id===$('diff-before').value),after=versions.find(v=>v.id===$('diff-after').value);
  const lines=diffLines(definitionText(before?.definition),definitionText(after?.definition));
  $('diff-body').replaceChildren(...lines.map(line=>h('div',{class:`diff-line ${line.type}`},h('b',{},line.type==='add'?'+':line.type==='remove'?'−':' '),h('span',{},line.text||' '))));
}
function renderImprovements(){
  const jobs=state.detail?.improvements||[],policy=state.detail?.creation_policy,target=$('improvements');target.hidden=!jobs.length&&!policy;
  target.replaceChildren(policy?h('div',{},`Creation: ${policy.policy} · ${policy.phase||'no timing selected'} · ${policy.orders?.length||0} mandatory deferred orders`):'',h('strong',{},'Automatic improvement · post-response'),...jobs.map(job=>h('div',{class:'improvement-job'},
    h('span',{},`${job.kind} · ${short(job.entity_id)}`),badge(job.status==='held'?'awaiting response':job.status),
    h('small',{},`Attempts: ${job.attempts}`),
    job.evaluation_run?h('button',{class:'button subtle',onClick:safe(async()=>{await refreshRuns();await selectRun(job.evaluation_run);})},'Inspect evaluation ↗'):null,
    job.outcome?h('details',{},h('summary',{},'Result'),h('pre',{},pretty(job.outcome))):null)));
}
function pageStatus(page){return state.controlStatus[page] ||= {loading:false,error:'',stale:false,updated_ms:null};}
function controlError(error){return errorText(error);}
function controlHeader(title,description,page){
  return h('header',{class:'control-heading'},h('div',{},h('div',{class:'eyebrow'},'CONTROL WORKSPACE'),h('h2',{},title),h('p',{},description)),
    h('div',{class:'control-heading-actions'},page?h('button',{class:'button subtle',disabled:pageStatus(page).loading,onClick:safe(()=>refreshControlPage())},pageStatus(page).loading?'Refreshing…':'↻ Refresh'):null));
}
function controlNotice(page){
  const status=pageStatus(page);
  const dataKeys={improvements:['suggestions'],settings:['overrides'],system:['memory','statistics','runtime','health'],admin:['adminWorkforces','adminKeys','adminAsync','adminSettings','adminService','adminRuntime','adminHealth']}[page]||[page];
  if(status.loading&&!dataKeys.some(key=>state.control[key]!=null))return h('div',{class:'control-notice loading',role:'status'},'Loading current server state…');
  if(status.error)return h('div',{class:`control-notice ${status.stale?'stale':'error'}`,role:'status'},status.stale?`Could not refresh. Showing the last successful server snapshot. ${status.error}`:`Could not load this area. ${status.error}`);
  if(status.stale)return h('div',{class:'control-notice stale',role:'status'},`Showing server data from ${dateTime(status.updated_ms)}; refresh failed.`);
  return null;
}
async function readHealthProbe(path){
  const response=await fetch(path,{cache:'no-store'});let data=null;
  try{data=await response.json();}catch{}
  return {ok:response.ok,status:response.status,data};
}
function scheduleControlPoll(page){
  clearTimeout(controlPollTimer);controlPollTimer=null;
  if(state.page!==page)return;
  if(!state.permissions.roster_read&&['approvals','improvements','settings','admin'].includes(page)){
    controlPollTimer=setTimeout(()=>{if(state.page===page)void refreshControlPage();},10000);return;
  }
  if(['system','admin'].includes(page)){controlPollTimer=setTimeout(()=>{if(state.page===page)void refreshControlPage();},15000);return;}
  if(!['operations','approvals'].includes(page))return;
  const rows=page==='operations'?(state.control.operations?.operations||[]):(state.control.approvals?.approvals||[]);
  const live=page==='operations'?rows.some(item=>['queued','running','cancelling','waiting_tools','waiting_approval'].includes(item.status)):rows.some(item=>item.status==='pending');
  if(live)controlPollTimer=setTimeout(()=>{if(state.page===page)void refreshControlPage();},5000);
}
function scheduleControlRefresh(){
  clearTimeout(controlRefreshTimer);controlRefreshTimer=setTimeout(()=>{controlRefreshTimer=null;if(['operations','approvals','improvements','settings','system','admin'].includes(state.page))void refreshControlPage();},250);
}
async function refreshControlPage({append=false}={}){
  const page=state.page;
  if(!['operations','approvals','improvements','settings','system','admin'].includes(page)||!dashboardPageAllowed(page,state.permissions))return;
  const status=pageStatus(page);if(status.loading){status.dirty=true;return;}
  const generation=authGeneration;status.loading=true;status.error='';render();
  const run=async(key,request)=>{try{const value=await request();state.control[key]=value;delete state.controlStatus[`${page}:${key}`];if(page==='settings'&&status.conflict&&value?.revision)status.conflictRevision=value.revision;return {ok:true};}catch(error){if(error?.status===401)throw error;state.controlStatus[`${page}:${key}`]={error:controlError(error),status:error?.status||0};return {ok:false,error};}};
  try{
    if(page==='operations'){
      const query=new URLSearchParams({limit:'50'}),filter=state.controlFilter.operations;
      if(filter)query.set('status',filter);
      const cursor=append?state.controlCursor.operations:null;if(cursor)query.set('cursor',cursor);
      const next=await api(`/api/operations?${query}`);
      if(generation!==authGeneration)return;
      const previous=append?(state.control.operations?.operations||[]):[];
      const operations=[...new Map([...previous,...(next.operations||[])].map(item=>[item.id,item])).values()];
      state.control.operations={...next,operations};state.controlCursor.operations=next.next_cursor||null;
    }else if(page==='approvals'){
      const query=new URLSearchParams({limit:'50',status:state.controlFilter.approvals||'pending'});
      const cursor=append?state.controlCursor.approvals:null;if(cursor)query.set('cursor',cursor);
      const next=await api(`/api/tool-approvals?${query}`);
      if(generation!==authGeneration)return;
      const previous=append?(state.control.approvals?.approvals||[]):[];
      const approvals=[...new Map([...previous,...(next.approvals||[])].map(item=>[item.approval?.id||item.id,item])).values()];
      state.control.approvals={...next,approvals};state.controlCursor.approvals=next.next_cursor||null;
    }else if(page==='improvements'){
      await run('suggestions',()=>api('/api/improvements'));
    }else if(page==='settings'){
      await run('overrides',()=>api('/api/settings/overrides'));
    }else if(page==='system'){
      const results=await Promise.allSettled([api('/api/memory'),api('/api/statistics'),api('/api/runtime'),readHealthProbe('/health/live'),readHealthProbe('/health/ready')]);
      if(generation!==authGeneration)return;
      for(const [index,key] of ['memory','statistics','runtime','health_live','health_ready'].entries()){
        const result=results[index];if(result.status==='fulfilled'){state.control[key]=result.value;delete state.controlStatus[`${page}:${key}`];}
        else{state.controlStatus[`${page}:${key}`]={error:controlError(result.reason),status:result.reason?.status||0};}
      }
      state.control.health={live:state.control.health_live,ready:state.control.health_ready};
    }else if(page==='admin'){
      const results=await Promise.allSettled([api('/api/admin/workforces'),api('/api/admin/keys'),api('/api/admin/async-limits'),api('/api/admin/settings'),api('/api/admin/service/status'),api('/api/runtime'),readHealthProbe('/health/live'),readHealthProbe('/health/ready')]);
      if(generation!==authGeneration)return;
      for(const [index,key] of ['adminWorkforces','adminKeys','adminAsync','adminSettings','adminService','adminRuntime','adminHealthLive','adminHealthReady'].entries()){
        const result=results[index];if(result.status==='fulfilled'){state.control[key]=result.value;delete state.controlStatus[`${page}:${key}`];}
        else state.controlStatus[`${page}:${key}`]={error:controlError(result.reason),status:result.reason?.status||0};
      }
      state.control.adminHealth={live:state.control.adminHealthLive,ready:state.control.adminHealthReady};
      const wf=state.control.adminWorkforces;state.adminWorkforces=Array.isArray(wf)?wf:(wf?.workforces||[]);
      const keys=state.control.adminKeys;state.adminKeys=Array.isArray(keys)?keys:(keys?.keys||[]);
    }
    if(generation!==authGeneration)return;
    const sectionErrors=Object.entries(state.controlStatus).filter(([key,value])=>key.startsWith(`${page}:`)&&value.error);
    status.error=sectionErrors.length===0?'':`${sectionErrors.length} section(s) could not be refreshed.`;
    const pageDataKeys={operations:['operations'],approvals:['approvals'],improvements:['suggestions'],settings:['overrides'],system:['memory','statistics','runtime','health'],admin:['adminWorkforces','adminKeys','adminAsync','adminSettings','adminService','adminRuntime','adminHealth']}[page]||[];
    status.stale=Boolean(status.error&&pageDataKeys.some(key=>state.control[key]!=null));if(!status.error)status.updated_ms=Date.now();
  }catch(error){
    if(generation!==authGeneration)return;
    status.error=controlError(error);const pageDataKey={improvements:'suggestions',settings:'overrides'}[page]||page;status.stale=state.control[pageDataKey]!=null;
  }finally{
    if(generation===authGeneration){status.loading=false;const dirty=status.dirty===true;status.dirty=false;render();if(dirty)void refreshControlPage();else scheduleControlPoll(page);}
  }
}
function apiEndpoint(url){
  const value=url;if(!value)throw new Error('The server did not provide an action URL.');
  const resolved=new URL(value,window.location.href);
  if(resolved.origin!==window.location.origin||!resolved.pathname.startsWith('/api/'))throw new Error('CCM returned an unsafe action URL; the bearer key was not sent.');
  return `${resolved.pathname}${resolved.search}`;
}
function runLink(runId){return runId&&state.permissions.roster_read?h('button',{class:'button link-button',onClick:safe(()=>openRun(runId))},`Open trace ${short(runId)} ↗`):null;}
async function openRun(runId){
  if(!state.permissions.roster_read)return;
  state.page='executions';await refreshRuns();await selectRun(runId);
}
function renderOperations(){
  const data=state.control.operations,rows=data?.operations||[],target=$('control-view');
  const filter=h('select',{value:state.controlFilter.operations,'aria-label':'Filter operations',onchange:event=>{state.controlFilter.operations=event.target.value;state.controlCursor.operations=null;state.control.operations=null;void refreshControlPage();}},
    h('option',{value:''},'All operation states'),...['queued','running','cancelling','waiting_tools','waiting_approval','completed','error','cancelled','expired','result_expired','interrupted'].map(value=>h('option',{value},label(value))));
  target.append(controlHeader('Async operations','Owner-scoped requests, truthful recovery state, results and cancellation. Results are retrieved only when you choose.', 'operations'),h('div',{class:'control-toolbar'},filter),controlNotice('operations')||'');
  if(data&&!rows.length)target.append(empty('No operations','No async operations match this filter for the current key.'));
  for(const operation of rows){
    const active=['queued','running','cancelling','waiting_tools','waiting_approval'].includes(operation.status);
    const statusText=operationDisposition(operation);
    const actions=[];
    actions.push(h('button',{class:'button subtle',onClick:safe(()=>refreshOperationStatus(operation))},'Refresh status'));
    if(operation.run_id)actions.push(runLink(operation.run_id));
    if(operationHasRetrievableResult(operation))actions.push(h('button',{class:'button subtle',onClick:safe(()=>retrieveOperationResult(operation))},'Retrieve result'));
    if(active&&state.permissions.roster_write)actions.push(h('button',{class:'button amber',onClick:()=>askConfirmation({title:'Cancel async operation?',description:'This cancels the owner-scoped operation. Completed tool effects are not undone.',detail:operation.id,confirmLabel:'Cancel operation',run:()=>cancelOperation(operation)})},'Cancel'));
    const approvals=operation.pending_approvals||[];
    target.append(h('article',{class:'control-card operation-card'},h('div',{class:'card-title-row'},h('div',{},h('h3',{},`Operation ${short(operation.id)}`),h('p',{class:'mono'},operation.id)),badge(operation.status)),
      h('p',{class:`lifecycle-note ${['interrupted','expired','result_expired','error','cancelled'].includes(operation.status)?'warning':''}`},statusText),
      h('div',{class:'card-metrics'},metric('Created',dateTime(operation.created_ms)),metric('Updated',dateTime(operation.updated_ms)),metric('Started',dateTime(operation.started_ms)),metric('Finished',dateTime(operation.finished_ms)),metric('Active time',duration(operation.active_ms)),metric('Model calls',operation.logical_model_calls)),
      h('p',{class:'mono muted'},`Root request · ${operation.root_request_id}`),
      operation.error?h('p',{class:'inline-error'},operation.error):null,
      operation.queue_expires_ms?h('p',{class:'muted'},`Queue deadline · ${dateTime(operation.queue_expires_ms)}`):null,
      operation.handoff_expires_ms?h('p',{class:'muted'},`Client-tool handoff deadline · ${dateTime(operation.handoff_expires_ms)}`):null,
      approvals.length?h('button',{class:'button subtle',onClick:()=>setPage('approvals')},`Review ${approvals.length} pending approval(s) ↗`):null,
      operation.budget?h('details',{},h('summary',{},'Accepted operation budget'),h('pre',{},pretty(operation.budget))):null,
      h('div',{class:'card-actions'},...actions)));
  }
  if(data?.next_cursor)target.append(h('button',{class:'button subtle load-next',onClick:safe(()=>refreshControlPage({append:true}))},'Load older operations ↓'));
  if(state.controlResult?.page==='operations')target.append(controlResultPanel(state.controlResult));
}
function metric(name,value){return h('div',{class:'control-metric'},h('span',{},name),h('strong',{},value==null?'—':String(value)));}
async function retrieveOperationResult(operation){
  const result=await api(apiEndpoint(operation.result_url));
  state.controlResult={page:'operations',title:`Result · ${operation.id}`,value:result};render();
}
async function refreshOperationStatus(operation){
  const snapshot=await api(apiEndpoint(operation.status_url));
  const rows=state.control.operations?.operations||[];state.control.operations={...state.control.operations,operations:rows.map(item=>item.id===operation.id?{...item,...snapshot}:item)};render();
}
async function cancelOperation(operation){
  await api(apiEndpoint(operation.cancel_url),{method:'DELETE'});
  await refreshControlPage();toast('Cancellation accepted by the server.');
}
function renderApprovals(){
  const data=state.control.approvals,rows=data?.approvals||[],target=$('control-view');
  const filter=h('select',{value:state.controlFilter.approvals,'aria-label':'Filter approvals',onchange:event=>{state.controlFilter.approvals=event.target.value;state.controlCursor.approvals=null;state.control.approvals=null;void refreshControlPage();}},h('option',{value:'pending'},'Pending only'),h('option',{value:'all'},'All approval states'));
  target.append(controlHeader('Exact-action approvals','Review the immutable tool name and arguments. Each vote resolves this one action only and is never replayed.', 'approvals'),h('div',{class:'control-toolbar'},filter),controlNotice('approvals')||'');
  if(data&&!rows.length)target.append(empty('No approval handles','No approvals are retained for this key in the selected state.'));
  for(const item of rows){
    const approval=item.approval,flow=item.flow,id=approval.id,status=item.status;
    const canResolve=approvalCanBeResolved(item)&&state.permissions.roster_use;
    const runId=approval.run_id||flow.run_id;
    const actions=[];
    if(canResolve){
      actions.push(h('button',{class:'button primary',onClick:()=>askConfirmation({title:'Approve this exact action once?',description:'The displayed tool and arguments will be approved one time for this invoker. Stored allow/ask/deny policy will not change.',detail:pretty({tool:approval.operation.tool,arguments:approval.operation.arguments}),confirmLabel:'Approve once',run:()=>resolveApproval(approval,'approve')})},'Approve once'));
      actions.push(h('button',{class:'button amber',onClick:()=>askConfirmation({title:'Deny this exact action?',description:'The operation will not run. The paused request receives a denial result; stored tool policy will not change.',detail:pretty({tool:approval.operation.tool,arguments:approval.operation.arguments}),confirmLabel:'Deny action',run:()=>resolveApproval(approval,'deny')})},'Deny'));
      actions.push(h('button',{class:'button subtle',onClick:()=>askConfirmation({title:'Cancel this approval and its paused run?',description:'This cancels only this invoker-owned approval and paused request.',detail:id,confirmLabel:'Cancel approval',run:()=>cancelApproval(approval)})},'Cancel approval'));
    }
    const canReadResult=['completed','interrupted','error','cancelled','result_expired'].includes(flow.status)||['approved','denied','cancelled','expired','interrupted'].includes(status);
    if(canReadResult)actions.push(h('button',{class:'button subtle',onClick:safe(()=>retrieveApprovalResult(approval))},'Check result'));
    actions.unshift(h('button',{class:'button subtle',onClick:safe(()=>refreshApprovalStatus(approval))},'Refresh status'));
    target.append(h('article',{class:'control-card approval-card'},h('div',{class:'card-title-row'},h('div',{},h('h3',{},`Approval ${short(id)}`),h('p',{class:'mono'},id)),badge(status)),
      h('p',{class:'lifecycle-note'},approvalDisposition(item)),
      h('div',{class:'approval-operation'},h('span',{class:'metric-label'},'EXACT TOOL OPERATION'),h('strong',{},approval.operation?.tool||'Unknown tool'),h('pre',{},pretty(approval.operation?.arguments??{}))),
      h('div',{class:'card-metrics'},metric('Agent',approval.agent_id),metric('Stage',approval.stage_id),metric('Expires',dateTime(approval.expires_ms)),metric('Decision',item.decision),metric('Execution outcome',item.execution_outcome),metric('Recovery',item.recovery_reason)),
      h('p',{class:'mono muted'},`Invoker key · ${approval.requested_by.key_id} · workforce · ${approval.requested_by.workforce_id}`),
      flow.error?h('p',{class:'inline-error'},flow.error):null,runLink(runId),h('div',{class:'card-actions'},...actions)));
  }
  if(data?.next_cursor)target.append(h('button',{class:'button subtle load-next',onClick:safe(()=>refreshControlPage({append:true}))},'Load older approvals ↓'));
  if(state.controlResult?.page==='approvals')target.append(controlResultPanel(state.controlResult));
}
async function resolveApproval(approval,decision){
  const endpoint=decision==='approve'?approval.approve_url:approval.deny_url;
  await api(apiEndpoint(endpoint),{method:'POST'});
  await refreshControlPage();toast(`Server recorded ${decision} for this exact approval.`);
}
async function refreshApprovalStatus(approval){
  const response=await api(apiEndpoint(approval.status_url));
  const refreshed={approval:response.approval,status:response.status,decision:response.approval.decision,execution_outcome:response.approval.execution_outcome,recovery_reason:response.approval.recovery_reason,flow:response.flow};
  const rows=state.control.approvals?.approvals||[];state.control.approvals={...state.control.approvals,approvals:rows.map(item=>item.approval.id===approval.id?refreshed:item)};render();
}
async function cancelApproval(approval){
  await api(apiEndpoint(approval.cancel_url),{method:'DELETE'});
  await refreshControlPage();toast('Approval cancellation recorded by the server.');
}
async function retrieveApprovalResult(approval){
  const result=await api(apiEndpoint(approval.result_url));
  state.controlResult={page:'approvals',title:`Approval result · ${approval.id}`,value:result};render();
}
function renderImprovementsPage(){
  const target=$('control-view'),data=state.control.suggestions,rows=Array.isArray(data)?data:(data?.suggestions||[]),filter=state.controlFilter.suggestions||'all';
  target.append(controlHeader('Improvement proposals','Private evidence stays private. Releasing a proposal shares its content; applying it is a separate, conflict-checked change.', 'improvements'),controlNotice('improvements')||'');
  target.append(h('div',{class:'control-toolbar'},h('select',{value:filter,'aria-label':'Filter improvement proposals',onchange:event=>{state.controlFilter.suggestions=event.target.value;render();}},h('option',{value:'all'},'All proposals'),...['pending','applied','stale','rejected'].map(value=>h('option',{value},label(value))))));
  const visible=rows.filter(item=>filter==='all'||item.status===filter);
  if(data&&!visible.length)target.append(empty('No proposals','No improvement proposals match this filter for the current key.'));
  for(const item of visible){
    const own=!item.note,proposal=item.proposal||{},replacement=proposal.replacement||item.proposed_definition||item.requirements||null;
    const canRelease=own&&state.permissions.improvement_contribute&&!item.declassified&&['revise','create'].includes(item.action)&&item.status==='pending';
    const canApply=state.permissions.roster_write&&item.status==='pending'&&!item.declassification_required;
    const actions=[];
    if(canRelease)actions.push(h('button',{class:'button amber',onClick:()=>askConfirmation({title:'Release this proposal to workforce writers?',description:'This explicitly shares generated proposal content with eligible writers in this workforce. It does not apply the proposal.',detail:`${item.kind} · ${item.entity_id}`,confirmLabel:'Release visibility',run:()=>releaseSuggestion(item)})},'Release visibility'));
    if(canApply)actions.push(h('button',{class:'button primary',onClick:()=>askConfirmation({title:'Apply this proposal?',description:'CCM will re-check the saved definition before applying. A stale revision is rejected; this UI will not retry it.',detail:pretty({target:item.entity_id,action:item.action,before:item.before||null,proposed:replacement}),confirmLabel:'Apply proposal',run:()=>applySuggestion(item)})},'Apply proposal'));
    if(item.declassification_required)actions.push(h('span',{class:'muted'},'Waiting for the source key to release proposal content.'));
    target.append(h('article',{class:'control-card suggestion-card'},h('div',{class:'card-title-row'},h('div',{},h('h3',{},`${label(item.action)} · ${item.kind||'definition'}`),h('p',{class:'mono'},`${item.entity_id||item.id} · ${item.id}`)),badge(item.status||'unknown')),
      item.note?h('p',{class:'privacy-note'},item.note):h('p',{class:'muted'},`Private source suggestion · created ${dateTime(item.created_ms)}`),
      proposal.reason?h('p',{},proposal.reason):null,
      item.declassified?h('p',{class:'privacy-note'},'Source key released proposal visibility; this is not an apply receipt.'):null,
      h('details',{},h('summary',{},'Review supplied proposal and before snapshot'),item.before?h('div',{class:'proposal-columns'},h('section',{},h('h4',{},'Before'),h('pre',{},pretty(item.before))),h('section',{},h('h4',{},'Proposed'),h('pre',{},pretty(replacement||proposal)))):h('pre',{},pretty({proposal,replacement}))),
      item.applied_ms?h('p',{class:'muted'},`Applied ${dateTime(item.applied_ms)}`):null,
      h('div',{class:'card-actions'},...actions)));
  }
  if(state.controlResult?.page==='improvements')target.append(controlResultPanel(state.controlResult));
}
async function releaseSuggestion(item){
  const value=await post(`/api/improvements/${encodeURIComponent(item.id)}/declassify`,{});
  state.controlResult={page:'improvements',title:'Visibility release receipt',value};await refreshControlPage();toast('Server recorded release of proposal visibility. No definition was applied.');
}
async function applySuggestion(item){
  try{
    const value=await post(`/api/improvements/${encodeURIComponent(item.id)}/apply`,{});
    state.controlResult={page:'improvements',title:'Apply receipt',value};await refreshControlPage();
    if(state.permissions.roster_read){try{await refreshWorkforce();}catch{}}
    toast('Server returned an improvement application result.');
  }catch(error){
    if(error?.status===409){toast('This proposal conflicts with a newer definition. The server rejected it; refresh and review the current proposal before any new action.',true);try{await refreshControlPage();}catch{}}
    throw error;
  }
}
function bytes(value){
  if(value==null||!Number.isFinite(Number(value)))return '—';
  const n=Number(value);if(n<1024)return `${n} B`;if(n<1024**2)return `${(n/1024).toFixed(1)} KiB`;return `${(n/1024**2).toFixed(1)} MiB`;
}
function memorySection(memory){
  if(!memory)return h('section',{class:'control-card'},h('h3',{},'Persistence and memory'),h('p',{class:'muted'},'No memory snapshot is available.'));
  const db=memory.database,dashboard=memory.dashboard,journal=memory.journal,delivery=memory.delivery;
  return h('section',{class:'control-card'},h('div',{class:'card-title-row'},h('h3',{},'Persistence and memory'),h('span',{class:'badge'},`Configured memory budget · ${memory.budget_mib??'—'} MiB`)),
    db?h('div',{class:'card-metrics'},metric('Memory cache',`${bytes(db.cache_bytes)} / ${bytes(db.cache_limit_bytes)}`),metric('Commit generation',db.commit_generation),metric('Persisted generation',db.persisted_generation),metric('Pending pages',db.pending_pages)):h('p',{class:'muted'},'Persistent database metrics are not exposed in this process.'),
    db?.error?h('p',{class:'inline-error'},`Persistence error · ${db.error}`):null,
    db?h('p',{class:db.pending_pages?'warning-note':'success-note'},db.error?'Persistence reports an error; inspect the server status before relying on durable state.':db.pending_pages?`${db.pending_pages} page(s) are pending disk persistence. Acknowledged in-memory writes are not necessarily durable yet.`:'No pending database pages are reported; this is a point-in-time snapshot.'):null,
    h('div',{class:'metrics-grid'},metric('Delivery memory',`${bytes(delivery?.bytes)} / ${bytes(delivery?.limit_bytes)}`),metric('Dashboard cache',`${bytes(dashboard?.bytes)} / ${bytes(dashboard?.limit_bytes)}`),metric('Dashboard cache entries',dashboard?.entries),metric('Cache hits / misses',dashboard?`${dashboard.hits} / ${dashboard.misses}`:'—'),metric('Journal queue',`${bytes(journal?.queued_bytes)} / ${bytes(journal?.limit_bytes)}`)),
    h('p',{class:'muted'},'Cache and delivery budgets are not process RSS. Generation counters are process-local and must not be compared across restarts.'));
}
function healthSection(health){
  const live=health?.live,ready=health?.ready;
  const liveOkay=live?.ok===true,readyOkay=ready?.ok===true;
  return h('section',{class:'control-card'},h('div',{class:'card-title-row'},h('h3',{},'Service readiness'),badge(liveOkay&&readyOkay?'ready':'unavailable')),
    h('p',{},`Liveness · ${live?`${liveOkay?'Live':'Unavailable'} (HTTP ${live.status})`:'not reported'}`),
    h('p',{},`Readiness · ${ready?`${readyOkay?'Ready':'Not ready'} (HTTP ${ready.status})`:'not reported'}`),
    h('p',{class:'muted'},'Readiness is provider-independent. It does not report or imply inference-provider health.'),
    live?.data?h('details',{},h('summary',{},'Liveness response'),h('pre',{},pretty(live.data))):null,
    ready?.data?h('details',{},h('summary',{},'Readiness response'),h('pre',{},pretty(ready.data))):null);
}
function runtimeSection(runtime){
  if(!runtime)return h('section',{class:'control-card'},h('h3',{},'Configured provider/admission state'),h('p',{class:'muted'},'No authorized runtime snapshot is available.'));
  const providers=runtime.providers||[];
  const asyncView=runtime.async_supervisor||null;
  return h('section',{class:'control-card'},h('div',{class:'card-title-row'},h('h3',{},'Configured provider/admission state'),h('span',{class:'muted'},`Snapshot · ${dateTime(runtime.generated_ms)}`)),
    h('p',{class:'muted'},`Snapshot scope · ${runtime.scope?.type||'not reported'}${runtime.provider_scope?` · provider visibility · ${label(runtime.provider_scope)}`:''}`),
    providers.length?h('div',{class:'provider-grid'},...providers.map(provider=>h('article',{class:'provider-card'},h('h4',{},provider.provider_id||'Configured provider'),
      h('p',{},`Models · ${(provider.configured_model_ids||[]).join(', ')||'none reported'}`),
      h('div',{class:'card-metrics'},metric('Concurrency limit',provider.max_concurrent_requests==null?'Unlimited':provider.max_concurrent_requests),metric('Active requests',provider.active_requests),metric('Waiting requests',provider.waiting_requests)),
      provider.circuit_state?h('p',{class:'muted'},`Admission circuit · ${label(provider.circuit_state)}${provider.cooldown_remaining_ms!=null?` · ${duration(provider.cooldown_remaining_ms)} remaining`:''}`):null))):h('p',{class:'muted'},'No configured provider entries are reported.'),
    asyncView?h('details',{open:true},h('summary',{},'Scoped async limits, queue and effective capacity'),h('pre',{},pretty(asyncView))):h('p',{class:'muted'},runtime.provider_scope==='bootstrap_admin_only'?'Provider and process-wide async details require bootstrap administration.':'No async supervisor projection was returned.'),
    runtime.service?h('p',{class:'lifecycle-note'},`Service lifecycle · ${label(runtime.service.phase)} · ${runtime.service.accepting?'accepting new work':'not accepting new work'}`):null,
    runtime.native_tool_sandbox?h('details',{},h('summary',{},'Effective native-tool sandbox mode'),h('pre',{},pretty(runtime.native_tool_sandbox))):null,
    h('p',{class:'muted'},'These are configured provider and admission measurements, not a provider health check. No endpoint URLs or credentials are displayed.'));
}
function statisticsSection(statistics){
  const rows=statistics?.groups||[];
  return h('section',{class:'control-card'},h('h3',{},'Scoped usage statistics'),h('p',{class:'muted'},statistics?.token_usage||'Provider-reported token counts; missing usage remains unknown.'),
    rows.length?h('div',{class:'table-scroll'},h('table',{class:'data-table'},h('thead',{},h('tr',{},...['Operation','Definition','Status','Count','Duration','Prompt tokens','Completion tokens'].map(name=>h('th',{scope:'col'},name)))),h('tbody',{},...rows.map(row=>h('tr',{},h('td',{},row.kind),h('td',{class:'mono'},row.entity_id),h('td',{},label(row.status)),h('td',{},row.count),h('td',{},duration(row.duration_ms)),h('td',{},row.prompt_tokens==null?'unknown':row.prompt_tokens),h('td',{},row.completion_tokens==null?'unknown':row.completion_tokens)))))):h('p',{class:'muted'},'No statistics are available for this scope.'));
}
function renderSystem(){
  const target=$('control-view');target.append(controlHeader('System health & metrics','Live service readiness, scoped usage, persistence pipeline and authorized runtime admission snapshot.','system'),controlNotice('system')||'');
  for(const key of ['memory','runtime','statistics','health_live','health_ready']){const status=state.controlStatus[`system:${key}`];if(status?.error)target.append(h('p',{class:'control-notice error'},`${label(key)}: ${status.error}`));}
  target.append(healthSection(state.control.health),memorySection(state.control.memory),runtimeSection(state.control.runtime),statisticsSection(state.control.statistics));
}
function controlResultPanel(result){return h('section',{class:'control-card result-panel'},h('div',{class:'card-title-row'},h('h3',{},result.title),h('button',{class:'button subtle',onClick:()=>{state.controlResult=null;render();}},'Dismiss')),h('pre',{},pretty(result.value)));}
function valueAt(object,path){return String(path||'').split('.').filter(Boolean).reduce((value,key)=>value?.[key],object);}
const creationPolicies=['AlwaysPreExecution','DefaultPreExecution','Dynamic','DefaultPostExecution','AlwaysPostExecution'];
const globalSettingFields=[
  {key:'creation_policy',label:'Creation policy',group:'Workforce policy',options:creationPolicies,description:'Server-wide creation timing for generated definitions.'},
  {key:'personality_transparency',label:'Personality transparency',group:'Workforce policy',options:['introvert','extrovert','flexible'],description:'Global Foreman response mode.'},
  ...[
    ['connect_timeout_ms','Connect timeout (ms)',1,86400000],['first_response_timeout_ms','First response timeout (ms)',1,86400000],
    ['idle_timeout_ms','Idle timeout (ms)',1,86400000],['total_timeout_ms','Total timeout (ms)',1,1800000],
    ['max_attempts','Maximum attempts',1,10],['retry_base_ms','Retry base (ms)',1,86400000],
  ].map(([key,labelText,min,max])=>({key:`upstream.${key}`,label:labelText,group:'Upstream timeouts',number:true,min,max,description:'Applied after service restart.'})),
  {key:'memory.budget_mib',label:'Memory budget (MiB)',group:'Memory budget',number:true,min:32,max:16384,description:'Persistence and runtime memory budget; restart required.'},
  {key:'sandbox.global_mode',label:'Global native-tool sandbox',group:'Sandbox policy',options:['platform_default','on','off'],description:'Platform default is an explicit tri-state option; off disables the configured sandbox.'},
];
function settingFields(snapshot,scope){
  if(scope!=='global')return [
    {key:'creation_policy',label:'Creation policy',group:'Current-key preferences',options:creationPolicies,nullable:true,description:'Clear the override to use the server default. Applies on the next request.'},
    {key:'personality_transparency',label:'Personality transparency',group:'Current-key preferences',options:['introvert','extrovert'],nullable:true,description:'Clear the override to use the flexible default. This override is ignored when the server fixes a non-flexible mode.'},
  ];
  const fields=[...globalSettingFields];
  const modes=snapshot?.saved?.sandbox?.mcp_stdio_modes||{};
  for(const id of Object.keys(modes).sort())fields.push({key:`sandbox.mcp_stdio_modes.${id}`,label:`MCP stdio sandbox · ${id}`,group:'Sandbox policy',options:['inherit','on','off'],description:'Override the global sandbox mode for this configured stdio server; inherit preserves platform/global behavior.'});
  return fields;
}
function clearControlEdits(prefix=''){
  for(const el of $('control-view').querySelectorAll('[data-persist]'))if(!prefix||el.dataset.persist.startsWith(prefix))el.removeAttribute('data-persist');
}
function settingsPanel(snapshot,{scope='global',title='Settings',path='/api/admin/settings',canEdit=false}={}){
  const section=h('section',{class:'control-card settings-card'},h('div',{class:'card-title-row'},h('div',{},h('h3',{},title),h('p',{class:'muted'},scope==='global'?'Server-wide settings · bootstrap administrator only':'Current-key preferences · applies only to this bearer identity')),h('span',{class:'badge'},scope==='global'?'Global':'Current key')));
  if(!snapshot){section.append(h('p',{class:'muted'},'Settings are not available from this server response. No controls are shown.'));return section;}
  const saved=snapshot.saved||{},effective=snapshot.effective||{},sources=snapshot.source||{},timing=snapshot.application_timing||{},fields=settingFields(snapshot,scope);
  const currentFor=key=>valueAt(saved,key);
  const effectiveFor=key=>scope==='global'&&key==='sandbox.global_mode'?effective.sandbox?.shell:scope==='global'&&key.startsWith('sandbox.mcp_stdio_modes.')?effective.sandbox?.mcp_stdio?.[key.split('.').at(-1)]:valueAt(effective,key);
  const sourceFor=key=>valueAt(sources,key);
  const timingFor=key=>scope==='global'&&key.startsWith('sandbox.')?timing.sandbox:scope==='global'&&key.startsWith('upstream.')?timing.upstream:valueAt(timing,key);
  if(canEdit){
    const form=h('form',{class:'control-form settings-form',onsubmit:safe(async event=>{
      event.preventDefault();const changes={};
      for(const element of form.querySelectorAll('[data-setting]')){
        const field=fields.find(item=>item.key===element.dataset.setting);if(!field)continue;
        const value=element.value==='__unset'?null:field.number?Number(element.value):element.value;
        if(Object.is(value,currentFor(field.key)))continue;
        const [root,...rest]=field.key.split('.');
        if(!rest.length)changes[root]=value;
        else{
          changes[root]||={};let target=changes[root];
          for(const key of rest.slice(0,-1))target=target[key]||={};
          target[rest.at(-1)]=value;
        }
      }
      if(!Object.keys(changes).length){toast('No editable settings were supplied.',true);return;}
      const revision=snapshot.revision;if(revision==null){toast('This settings response has no revision; refusing to write without a conflict guard.',true);return;}
      const button=form.querySelector('button[type=submit]');button.disabled=true;button.textContent='Validating and saving…';
      try{
        const headers={'content-type':'application/json','if-match':`"${String(revision).replace(/^"|"$/g,'')}"`};
        const result=await api(path,{method:'PUT',headers,body:JSON.stringify({changes})});
        clearControlEdits(`${scope}-setting-`);if(scope==='global')state.control.adminSettings=result;else state.control.overrides=result;state.controlResult={page:state.page,title:`${title} server receipt`,value:result};
        pageStatus(state.page).conflict=false;await refreshControlPage();toast('The server validated and saved the requested settings.');
      }catch(error){
        if(error?.status===409){pageStatus(state.page).conflict=true;pageStatus(state.page).conflictRevision=error.data?.current_revision??error.data?.revision;clearControlEdits(`${scope}-setting-`);toast('Settings changed on the server. No changes were applied; review the latest saved/effective values before another submission.',true);await refreshControlPage();}
        throw error;
      }finally{button.disabled=false;button.textContent='Validate and save';}
    })});
    let previousGroup='';
    for(const field of fields){
      const key=field.key,current=currentFor(key),controlAttrs={name:key,'data-setting':key,'data-persist':`${scope}-setting-${key}`,'data-snapshot-revision':snapshot.revision};let input;
      if(field.options){
        input=h('select',controlAttrs,...(field.nullable?[h('option',{value:'__unset'},'Use server default / clear override')]:[]),...field.options.map(value=>h('option',{value,selected:current===value},label(value))));
        input.value=field.nullable&&current==null?'__unset':String(current);
      }else{
        input=h('input',{...controlAttrs,type:'number',required:true,min:field.min,max:field.max,step:'1',value:current??''});
      }
      if(field.group!==previousGroup){previousGroup=field.group;form.append(h('h4',{class:'settings-group-heading'},field.group));}
      const effectiveValue=effectiveFor(key),source=sourceFor(key),applies=timingFor(key);
      form.append(h('label',{class:'setting-field'},h('span',{},field.label),input,field.description?h('small',{},field.description):null,
        h('small',{class:'setting-meta'},`Saved · ${current==null?'(unset)':pretty(current)} · Effective · ${effectiveValue==null?'not reported':pretty(effectiveValue)}${source!=null?` · Source · ${pretty(source)}`:''}${applies!=null?` · Applies · ${label(applies)}`:''}`)));
    }
    const status=pageStatus(state.page),conflict=status.conflict===true,canAcknowledge=conflict&&!status.error&&snapshot.revision!=null&&(status.conflictRevision==null||status.conflictRevision===snapshot.revision);
    form.append(h('p',{class:conflict?'control-notice error':'muted'},conflict?`Revision conflict. Latest server revision · ${snapshot.revision}. Review every saved/effective value; writes remain locked until you acknowledge this snapshot.`:scope==='global'?'Writes are allowlisted, validated server-side and guarded by the displayed revision. Global changes require restart.':'Writes are allowlisted, validated server-side and guarded by the displayed revision.'),h('div',{class:'card-actions'},h('button',{class:'button primary',type:'submit',disabled:conflict},'Validate and save'),conflict?h('button',{class:'button subtle',type:'button',onClick:safe(async()=>{clearControlEdits(`${scope}-setting-`);await refreshControlPage();})},'Refresh latest snapshot'):null,canAcknowledge?h('button',{class:'button amber',type:'button',onClick:()=>{status.conflict=false;status.conflictRevision=null;render();}},'I reviewed these values · unlock writes'):null));section.append(form);
  }else{
    section.append(h('p',{class:'muted'},'This key can inspect but not modify these settings.'),h('details',{},h('summary',{},'Saved and effective values'),h('pre',{},pretty({revision:snapshot.revision??null,saved,effective,source:sources,application_timing:timing}))));
  }
  return section;
}
function renderSettingsPage(){
  const target=$('control-view');target.append(controlHeader('Your settings','Scoped preferences affect this key only. They do not grant roster-write or bootstrap-administrator authority.','settings'),controlNotice('settings')||'');
  const status=state.controlStatus['settings:overrides'];if(status?.error)target.append(h('p',{class:'control-notice error'},status.error));
  target.append(settingsPanel(state.control.overrides,{scope:'key',title:'Key-scoped runtime preferences',path:'/api/settings/overrides',canEdit:state.permissions.roster_use}));
  if(state.controlResult?.page==='settings')target.append(controlResultPanel(state.controlResult));
}
function renderRunPage(){
  const target=$('control-view'),form=h('form',{class:'control-form run-by-id-form',onsubmit:safe(async event=>{
    event.preventDefault();const kind=form.elements.namedItem('kind').value,id=form.elements.namedItem('id').value.trim(),message=form.elements.namedItem('message').value.trim();if(!id||!message)return;
    const button=form.querySelector('button[type=submit]');button.disabled=true;button.textContent='Running selected definition…';
    try{
      const result=await post(`/api/management/definitions/${encodeURIComponent(kind)}/${encodeURIComponent(id)}/run`,{message});
      state.control.runReceipt={kind,id,result};form.reset();clearControlEdits('run-');render();
      if((result.status==='waiting_approval'||result.pending_approvals?.length)&&state.permissions.roster_use){state.page='approvals';void refreshControlPage();}
      else toast('Server returned the selected-run result.');
    }finally{button.disabled=false;button.textContent='Run selected definition';}
  })},h('p',{class:'muted'},'This form runs only the explicitly selected active definition. A use-only key can enter an ID it already knows but cannot enumerate or inspect the roster.'),
    adminFormField('Definition kind',h('select',{name:'kind','data-persist':'run-kind'},h('option',{value:'agent'},'Agent'),h('option',{value:'workflow'},'Workflow'))),
    adminFormField('Definition ID',h('input',{name:'id',required:true,maxlength:'128','data-persist':'run-id',placeholder:'Exact agent or workflow ID'})),
    adminFormField('Task message',h('textarea',{name:'message',required:true,maxlength:'12000',rows:'4','data-persist':'run-message',placeholder:'Describe the task for this definition.'})),
    h('button',{class:'button primary',type:'submit',disabled:!serviceAccepting()},'Run selected definition'));
  target.append(controlHeader('Run a selected workforce item','A direct, permission-checked run does not require reading run history. The request is not automatically made asynchronous.','run'),!serviceAccepting()?h('p',{class:'control-notice stale'},'Service is draining; new work is disabled. Existing operations, results and approvals remain available.'):null,h('section',{class:'control-card'},form));
  const receipt=state.control.runReceipt;
  if(receipt){
    const result=receipt.result,runId=result.run_id||result.run?.id||result.receipt?.run_id;
    target.append(h('section',{class:'control-card receipt-card'},h('div',{class:'card-title-row'},h('h3',{},'Server run receipt'),badge(result.status||'completed')),
      h('p',{},`${receipt.kind} · ${receipt.id}`),runId?h('p',{class:'mono'},`Run ${runId}`):null,runId?runLink(runId):null,
      result.pending_approvals?.length?h('button',{class:'button primary',onClick:()=>setPage('approvals')},`Review ${result.pending_approvals.length} pending approval(s)`):null,
      h('details',{open:true},h('summary',{},'Exact response'),h('pre',{},pretty(result)))));
  }
}
function adminFormField(labelText,control){return h('label',{class:'admin-field'},h('span',{},labelText),control);}
function adminPermissionsForm(){
  return h('fieldset',{class:'permission-grid'},h('legend',{},'Independent permissions · all default to denied'),...[
    ['roster_use','Use selected workforce and Foreman'],['roster_read','Read workforce and this key’s history'],['roster_write','Change shared workforce definitions'],['improvement_contribute','Contribute private improvement evidence'],...(state.permissions.bootstrap_admin?[['bootstrap_admin','Bootstrap administration']]:[]),
  ].map(([name,text])=>h('label',{class:'permission-choice'},h('input',{type:'checkbox',name:`permission_${name}`,'data-persist':`admin-key-perm-${name}`}),h('span',{},text))));
}
function renderAdminWorkforces(){
  const target=h('section',{class:'control-card'},h('h3',{},'Workforces'),h('p',{class:'muted'},'Create private or shared roster boundaries. Existing history is not migrated.'),
    h('form',{class:'admin-inline-form',onsubmit:safe(async event=>{event.preventDefault();const form=event.currentTarget,name=form.elements.namedItem('name').value.trim(),visibility=form.elements.namedItem('visibility').value;if(!name)return;const button=form.querySelector('button[type=submit]');button.disabled=true;
      try{const value=await post('/api/admin/workforces',{name,visibility});state.control.adminWorkforceReceipt=value;form.reset();clearControlEdits('admin-workforce-');await refreshControlPage();toast('Workforce created by the server.');}finally{button.disabled=false;}})},
      adminFormField('Name',h('input',{name:'name',required:true,maxlength:'120','data-persist':'admin-workforce-name'})),adminFormField('Visibility',h('select',{name:'visibility','data-persist':'admin-workforce-visibility'},h('option',{value:'private'},'Private'),h('option',{value:'shared'},'Shared'))),h('button',{class:'button primary',type:'submit'},'Create workforce')));
  if(state.control.adminWorkforceReceipt)target.append(h('details',{},h('summary',{},'Workforce creation receipt'),h('pre',{},pretty(state.control.adminWorkforceReceipt))));
  if(state.adminWorkforces.length)target.append(h('div',{class:'table-scroll'},h('table',{class:'data-table'},h('thead',{},h('tr',{},...['Name','ID','Visibility','Active keys','Created'].map(text=>h('th',{scope:'col'},text)))),h('tbody',{},...state.adminWorkforces.map(workforce=>h('tr',{},h('td',{},workforce.name),h('td',{class:'mono'},workforce.id),h('td',{},workforce.visibility),h('td',{},workforce.active_keys),h('td',{},dateTime(workforce.created_ms))))))));
  return target;
}
function renderAdminKeys(){
  const workforceOptions=state.adminWorkforces.map(workforce=>h('option',{value:workforce.id},`${workforce.name} · ${workforce.visibility}`));
  const form=h('form',{class:'control-form admin-key-form',onsubmit:safe(async event=>{
    event.preventDefault();const labelValue=form.elements.namedItem('label').value.trim(),workforceId=form.elements.namedItem('workforce_id').value,workforceName=form.elements.namedItem('workforce_name').value.trim();if(!labelValue)return;
    const permissions=Object.fromEntries([...form.querySelectorAll('[name^="permission_"]')].map(input=>[input.name.slice('permission_'.length),input.checked]));
    const body={label:labelValue,permissions};if(workforceId)body.workforce_id=workforceId;if(!workforceId&&workforceName)body.workforce_name=workforceName;
    const selected=state.adminWorkforces.find(item=>item.id===workforceId);
    askConfirmation({title:'Create a bearer key?',description:selected?.visibility==='private'?'The existing-workforce key API changes this private workforce to shared visibility. Its prior records are not migrated. Confirm only if this visibility change is intended.':'The new credential will have only the independent permissions shown. Its token is returned once and cannot be recovered.',detail:pretty({label:labelValue,workforce_id:workforceId||null,new_private_workforce_name:workforceId?null:(workforceName||labelValue),permissions}),confirmLabel:'Provision key',run:async()=>{
      const button=form.querySelector('button[type=submit]');button.disabled=true;button.textContent='Provisioning key…';
      try{
        const value=await post('/api/admin/keys',body),token=value.token;
        const receipt={...value};delete receipt.token;delete receipt.shown_once;state.control.adminKeyReceipt=receipt;form.reset();clearControlEdits('admin-key-');
        await refreshControlPage();
        if(typeof token==='string'&&token)showOneTimeToken(token);else toast('The server did not return a one-time token. Review the key list before taking further action.',true);
      }finally{button.disabled=false;button.textContent='Create key · show token once';}
    }});
  })},adminFormField('Key label',h('input',{name:'label',required:true,maxlength:'120','data-persist':'admin-key-label'})),
    adminFormField('Workforce',h('select',{name:'workforce_id','data-persist':'admin-key-workforce'},h('option',{value:''},'Create a new private workforce'),...workforceOptions)),
    adminFormField('New private workforce name (optional)',h('input',{name:'workforce_name',maxlength:'120','data-persist':'admin-key-workforce-name'})),adminPermissionsForm(),
    h('p',{class:'warning-note'},'A key is returned once. It is never shown again or saved by this dashboard. Grant bootstrap administration only when deliberately provisioning an administrator.'),
    h('button',{class:'button primary',type:'submit'},'Create key · show token once'));
  const target=h('section',{class:'control-card'},h('h3',{},'Bearer keys'),h('p',{class:'muted'},'Key records show permission metadata only; tokens are never listed.'),form);
  if(state.control.adminKeyReceipt)target.append(h('details',{},h('summary',{},'Last key creation receipt (token removed)'),h('pre',{},pretty(state.control.adminKeyReceipt))));
  if(state.adminKeys.length)target.append(h('div',{class:'table-scroll'},h('table',{class:'data-table'},h('thead',{},h('tr',{},...['Label','Key ID','Workforce','Permissions','Created','State','Action'].map(text=>h('th',{scope:'col'},text)))),h('tbody',{},...state.adminKeys.map(key=>{
     const permissions=key.permissions||{},revoked=key.revoked_ms!=null;
    const chips=['roster_use','roster_read','roster_write','improvement_contribute','bootstrap_admin'].filter(name=>permissions[name]).map(name=>h('span',{class:'permission-chip'},label(name)));
     return h('tr',{},h('td',{},key.label),h('td',{class:'mono'},key.id),h('td',{class:'mono'},key.workforce_id),h('td',{class:'permission-chips'},...chips),h('td',{},dateTime(key.created_ms)),h('td',{},revoked?`Revoked ${dateTime(key.revoked_ms)}`:'Active'),h('td',{},!revoked?h('button',{class:'button amber',onClick:()=>askConfirmation({title:`Revoke key ${key.label}?`,description:'Revocation is immediate and permanent for this credential. The server protects its configured bootstrap key.',detail:`${key.id} · ${key.workforce_id}`,confirmLabel:'Revoke key',run:()=>revokeKey(key)})},'Revoke'):null));
  })))));
  return target;
}
function showOneTimeToken(token){
  $('issued-token').value=token;$('token-dialog').showModal();$('token-copy').focus();
}
async function copyOneTimeToken(){
  const input=$('issued-token');try{await navigator.clipboard.writeText(input.value);toast('One-time key copied to clipboard.');}catch{input.focus();input.select();toast('Clipboard permission unavailable; key selected for manual copy.',true);}
}
async function revokeKey(key){
  await api(`/api/admin/keys/${encodeURIComponent(key.id)}`,{method:'DELETE'});await refreshControlPage();toast('Key revocation recorded by the server.');
}
const limitFields=[['max_root_operations','Active root operations',1,256],['max_child_workers','Child workers',1,256],['max_queued_roots','Queued roots',0,4096],['max_queue_wait_seconds','Queue wait (seconds)',1,86400]];
function renderAdminAsyncLimits(){
  const data=state.control.adminAsync,server=data?.server||null,workforces=data?.workforces||{};
  const section=h('section',{class:'control-card'},h('h3',{},'Live async capacity'),h('p',{class:'muted'},'These limits are in-memory, not saved to ccm.json. Lowering a cap blocks new admissions; it does not cancel active work.'),
    data?h('div',{class:'card-metrics'},metric('Active roots',data.active_root_operations),metric('Active children',data.active_child_workers),metric('Queued roots',data.queued_roots),metric('Open handoffs',data.outstanding_client_tool_handoffs)):h('p',{class:'muted'},'Live async controls are unavailable until the admin endpoint returns a snapshot.'));
  if(server){
    const form=h('form',{class:'admin-inline-form',onsubmit:safe(async event=>{event.preventDefault();const changes={};for(const [key] of limitFields){const value=form.elements.namedItem(key).value;if(value==='')return;changes[key]=Number(value);}const button=form.querySelector('button[type=submit]');button.disabled=true;
      try{await api('/api/admin/async-limits',{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(changes)});clearControlEdits('admin-limit-server-');await refreshControlPage();toast('Live server capacity limits updated.');}finally{button.disabled=false;}})},...limitFields.map(([key,labelText,min,max])=>adminFormField(labelText,h('input',{type:'number',name:key,min,max,step:'1',required:true,value:server[key], 'data-persist':`admin-limit-server-${key}`}))),h('button',{class:'button primary',type:'submit'},'Apply live server limits'));
    section.append(h('h4',{},'Server limits'),form);
  }
  const workforceList=state.adminWorkforces;
  if(workforceList.length){
    section.append(h('h4',{},'Per-workforce limits'),h('p',{class:'muted'},'Blank fields remove that workforce override; the effective cap is bounded by the server limit.'),
      h('div',{class:'workforce-limit-list'},...workforceList.map(workforce=>h('details',{},h('summary',{},`${workforce.name} · ${workforce.id}`),h('pre',{},pretty(workforces[workforce.id]||{}))))));
    const form=h('form',{class:'admin-inline-form',onsubmit:safe(async event=>{event.preventDefault();const workforceId=form.elements.namedItem('workforce_id').value;if(!workforceId)return;const limits={};for(const [key] of limitFields){const value=form.elements.namedItem(key).value;if(value!=='')limits[key]=Number(value);}const button=form.querySelector('button[type=submit]');button.disabled=true;
      try{await api(`/api/admin/workforces/${encodeURIComponent(workforceId)}/async-limits`,{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(limits)});clearControlEdits('admin-limit-workforce-');await refreshControlPage();toast('Workforce live limits updated.');}finally{button.disabled=false;}})},
      adminFormField('Workforce',h('select',{name:'workforce_id','data-persist':'admin-limit-workforce-id'},...workforceList.map(workforce=>h('option',{value:workforce.id},workforce.name)))),
      ...limitFields.map(([key,labelText,min,max])=>adminFormField(`${labelText} · blank clears`,h('input',{type:'number',name:key,min,max,step:'1','data-persist':`admin-limit-workforce-${key}`}))),h('button',{class:'button subtle',type:'submit'},'Save workforce limits'));
    section.append(form);
  }
  return section;
}
function serviceStatusSection(service){
  const section=h('section',{class:'control-card'},h('div',{class:'card-title-row'},h('h3',{},'Service lifecycle'),badge(service?.phase||'status unavailable')));
  if(!service){section.append(h('p',{class:'muted'},'Lifecycle status is unavailable. Drain is intentionally disabled until the server reports its current state.'));return section;}
  section.append(h('p',{},`Phase · ${label(service.phase)} · ${service.accepting?'accepting new work':'not accepting new work'}`),h('p',{class:'muted'},'Drain is idempotent. It stops new work admission, while this session can continue polling, reading results and resolving approvals. Restart is required to return a drained service to ready.'));
  section.append(h('button',{class:'button amber',disabled:service.phase!=='ready'||service.accepting!==true,onClick:()=>askConfirmation({title:'Drain CCM now?',description:'New work will be rejected with a retryable 503. Existing operations, status/result requests and approval resolution remain available; restart is required to return to ready.',detail:pretty(service),confirmLabel:'Begin drain',run:async()=>{
    const result=await api('/api/admin/service/drain',{method:'POST'});
    state.control.adminService=result.service;state.control.adminDrainReceipt=result;render();toast(result.accepted?'Service drain accepted; existing status, results and approvals remain available.':'The server returned a drain response.');
  }})},service.phase==='draining'?'Already draining':'Drain service'));
  if(state.control.adminDrainReceipt)section.append(h('details',{},h('summary',{},'Last drain response'),h('pre',{},pretty(state.control.adminDrainReceipt))));
  return section;
}
function renderAdmin(){
  const target=$('control-view');target.append(controlHeader('Administration','Bootstrap-admin-only controls. This permission is independent from roster use, read, write and improvement contribution.','admin'),controlNotice('admin')||'');
  for(const key of ['adminWorkforces','adminKeys','adminAsync','adminSettings','adminService','adminRuntime','adminHealthLive','adminHealthReady']){const status=state.controlStatus[`admin:${key}`];if(status?.error)target.append(h('p',{class:'control-notice error'},`${label(key)}: ${status.error}`));}
  target.append(serviceStatusSection(state.control.adminService),healthSection(state.control.adminHealth),runtimeSection(state.control.adminRuntime),renderAdminWorkforces(),renderAdminKeys(),renderAdminAsyncLimits(),settingsPanel(state.control.adminSettings,{scope:'global',title:'Global saved settings',path:'/api/admin/settings',canEdit:true}));
  if(state.controlResult?.page==='admin')target.append(controlResultPanel(state.controlResult));
}
function renderControlPage(){
  const target=$('control-view'),saved=Object.fromEntries([...target.querySelectorAll('[data-persist]')].map(el=>[el.dataset.persist,{value:el.type==='checkbox'?el.checked:el.value,revision:el.dataset.snapshotRevision||null}]));
  const settingSnapshots=state.page==='settings'?[['key-setting-',state.control.overrides]]:state.page==='admin'?[['global-setting-',state.control.adminSettings]]:[];
  for(const [prefix,snapshot] of settingSnapshots){
    if(snapshot?.revision==null)continue;
    if(Object.entries(saved).some(([key,previous])=>key.startsWith(prefix)&&previous.revision!=null&&previous.revision!==snapshot.revision)){
      const status=pageStatus(state.page);status.conflict=true;status.conflictRevision=snapshot.revision;
    }
  }
  target.replaceChildren();
  if(state.page==='operations')renderOperations();
  else if(state.page==='approvals')renderApprovals();
  else if(state.page==='run')renderRunPage();
  else if(state.page==='improvements')renderImprovementsPage();
  else if(state.page==='settings')renderSettingsPage();
  else if(state.page==='system')renderSystem();
  else if(state.page==='admin')renderAdmin();
  else{
     target.append(controlHeader('Access overview','This key is authenticated. Available dashboard areas reflect only its independent permissions.',null),h('section',{class:'control-card'},h('h3',{},state.session?.identity?.label||'Authenticated key'),h('p',{},`Workforce · ${state.session?.workforce?.name||'not reported'}`),h('div',{class:'permission-chips'},...Object.entries(state.permissions).map(([name,enabled])=>h('span',{class:`permission-chip ${enabled?'granted':'denied'}`},`${label(name)} · ${enabled?'granted':'not granted'}`))),h('p',{class:'muted'},'No read, use, contribution, or administration workspace is enabled for this key. Contact an administrator to provision only the permissions you need.')));
  }
  for(const el of target.querySelectorAll('[data-persist]'))if(Object.hasOwn(saved,el.dataset.persist)){
    const previous=saved[el.dataset.persist];
    if(previous.revision!=null&&el.dataset.snapshotRevision&&previous.revision!==el.dataset.snapshotRevision)continue;
    if(el.type==='checkbox')el.checked=previous.value;else el.value=previous.value;
  }
}
function askConfirmation({title,description,detail='',confirmLabel='Confirm',run}){
  confirmHandler=run;activeControlAction=false;
  $('action-title').textContent=title;$('action-description').textContent=description;
  $('action-detail').textContent=detail;$('action-detail').hidden=!detail;
  $('action-confirm').textContent=confirmLabel;$('action-confirm').disabled=false;$('action-confirm').hidden=false;$('action-cancel').textContent='Cancel';$('action-cancel').disabled=false;
  if(!$('action-dialog').open)$('action-dialog').showModal();$('action-confirm').focus();
}
async function confirmPendingAction(){
  if(activeControlAction||!confirmHandler)return;
  activeControlAction=true;const handler=confirmHandler;$('action-confirm').disabled=true;$('action-cancel').disabled=true;
  try{await handler();confirmHandler=null;$('action-dialog').close();}
  catch(error){
    const unknown=mutationOutcomeUnknown(error);
    $('action-description').textContent=error?.code==='service_draining'?'The server rejected new work because it is draining. This request was not admitted; status, results and existing approval resolution remain available.':error?.status===410?'The server reports this item or result has expired. No action was replayed; refresh the current state.':error?.status===409?'The server reports a conflict or a lifecycle state that no longer accepts this action. No automatic retry was made; refresh current state before deciding what to do next.':error?.status===403?'The server denied this action for the current key. No mutation was reported as successful.':unknown?'The request connection failed and the server outcome may be unknown. Do not repeat this action yet; refresh the relevant server state to reconcile it.':`The server rejected this action (HTTP ${error.status}). No automatic retry was made. Refresh current state before deciding what to do next.`;
    $('action-confirm').textContent='Close';$('action-confirm').disabled=false;$('action-cancel').textContent='Close';$('action-cancel').disabled=false;confirmHandler=null;reportError(error);
  }finally{activeControlAction=false;}
}
function closeActionDialog(){
  if(activeControlAction)return;
  confirmHandler=null;if($('action-dialog').open)$('action-dialog').close();
}
function render(){syncNavigation();renderHeader();if(state.page==='workforce')renderWorkforce();else if(state.page!=='executions')renderControlPage();else{renderSidebar();renderImprovements();if(state.view==='flow')renderFlow();if(state.view==='timeline')renderTimeline();if(state.view==='chat')renderChat();}renderInspector();}

$('host').textContent=location.host;
document.querySelectorAll('[data-page]').forEach(b=>b.onclick=()=>setPage(b.dataset.page));
document.querySelectorAll('[data-view]').forEach(b=>b.onclick=()=>setView(b.dataset.view));
document.querySelectorAll('[data-inspector]').forEach(b=>b.onclick=()=>{state.tab=b.dataset.inspector;document.querySelectorAll('[data-inspector]').forEach(n=>n.classList.toggle('active',n===b));renderInspector();});
document.querySelectorAll('[data-sidebar]').forEach(b=>b.onclick=()=>{state.sidebar=b.dataset.sidebar;document.querySelectorAll('[data-sidebar]').forEach(n=>n.classList.toggle('active',n===b));safe(refreshConversations)();renderSidebar();});
for(const id of ['search','status-filter','source-filter'])$(id).addEventListener('input',renderSidebar);
$('show-deltas').onchange=renderTimeline;$('new-chat').onclick=newChat;$('chat-form').onsubmit=safe(sendChat);
$('guide').onclick=safe(()=>sendControl('guidance'));$('interrupt').onclick=safe(()=>sendControl('interrupt'));
$('pause-agent').onclick=safe(()=>sendControl(isPaused(selected()?.entity_id)?'resume':'pause'));
$('pause-run').onclick=safe(()=>sendControl(isPaused('*')?'resume':'pause',true));
$('export').onclick=safe(downloadExport);
$('auth-form').onsubmit=event=>{event.preventDefault();const token=$('auth-token').value.trim();if(token)void beginSession(token);};
$('logout').onclick=()=>{auth.clear();stopTrace();if($('token-dialog').open)$('token-dialog').close();$('issued-token').value='';resetDashboardData();showAuthScreen('You have been signed out.');};
$('load-history').onclick=safe(loadHistory);$('load-more').onclick=safe(()=>refreshRuns(state.nextOffset||0));
$('close-diff').onclick=()=>$('diff-dialog').close();$('diff-before').onchange=renderDiff;$('diff-after').onchange=renderDiff;
$('action-confirm').onclick=safe(confirmPendingAction);$('action-cancel').onclick=closeActionDialog;$('action-close').onclick=closeActionDialog;
$('action-dialog').addEventListener('cancel',event=>{if(activeControlAction)event.preventDefault();else confirmHandler=null;});
$('token-copy').onclick=safe(copyOneTimeToken);$('token-close').onclick=()=>$('token-dialog').close();$('token-done').onclick=()=>$('token-dialog').close();
$('token-dialog').addEventListener('close',()=>{$('issued-token').value='';});
$('toggle-inspector').onclick=()=>$('workspace').classList.toggle('inspector-hidden');
if(innerWidth<960)$('workspace').classList.add('inspector-hidden');
try{document.documentElement.dataset.theme=localStorage.getItem('ccm-theme')||'dark';}catch{}
$('theme').onclick=()=>{const theme=document.documentElement.dataset.theme==='dark'?'light':'dark';document.documentElement.dataset.theme=theme;try{localStorage.setItem('ccm-theme',theme);}catch{}};
document.addEventListener('keydown',event=>{if(event.key.toLowerCase()==='n'&&!event.ctrlKey&&!event.metaKey&&!['INPUT','TEXTAREA','SELECT'].includes(document.activeElement?.tagName)&&!$('diff-dialog').open){event.preventDefault();newChat();}});
for(const handle of document.querySelectorAll('[data-resize]')){
  const variable=`--${handle.dataset.resize}`;
  const update=value=>document.documentElement.style.setProperty(variable,`${Math.max(handle.dataset.resize==='sidebar'?180:300,Math.min(value,handle.dataset.resize==='sidebar'?420:680))}px`);
  handle.onpointerdown=event=>{handle.setPointerCapture(event.pointerId);const start=event.clientX,size=parseFloat(getComputedStyle(document.documentElement).getPropertyValue(variable));handle.onpointermove=e=>update(size+(e.clientX-start)*(handle.dataset.resize==='sidebar'?1:-1));handle.onpointerup=()=>{handle.onpointermove=null;};};
  handle.onkeydown=event=>{if(['ArrowLeft','ArrowRight'].includes(event.key)){event.preventDefault();update(parseFloat(getComputedStyle(document.documentElement).getPropertyValue(variable))+(event.key==='ArrowRight'?20:-20));}};
}
setInterval(()=>{if(current()&&activeStates.has(current().status))renderHeader();},1000);
for(const id of ['workforce-search','workforce-kind','workforce-status'])$(id).addEventListener(id==='workforce-search'?'input':'change',renderWorkforce);
setInterval(()=>{if(state.page==='workforce')scheduleWorkforce();},60000);
render();
if(auth.token)void beginSession(auth.token);else showAuthScreen();
