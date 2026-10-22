import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('./state.js',import.meta.url),'utf8');
const {applyEvent,outputMessage,diffLines,createSessionAuth,httpError,redactSecret,consumeSse,normalizePermissions,dashboardPageAllowed,defaultDashboardPage,operationDisposition,operationHasRetrievableResult,approvalDisposition,approvalCanBeResolved,mutationOutcomeUnknown}=await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

test('browser auth stores the key in session storage and attaches it to every request',async()=>{
  const values=new Map();
  const storage={getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};
  const auth=createSessionAuth(storage);
  auth.setToken('secret-key',{save:false});
  assert.equal(values.size,0);
  let request;
  await auth.fetch(async(...args)=>{request=args;return 'ok';},'/api/runs',{headers:{authorization:'Bearer stale'}});
  assert.equal(request[1].headers.get('Authorization'),'Bearer secret-key');
  auth.persist();
  assert.deepEqual([...values.entries()],[['ccm-api-token','secret-key']]);
  const restored=createSessionAuth(storage);
  assert.equal(restored.load(),'secret-key');
  restored.clear();
  assert.equal(values.size,0);
});

test('browser auth refuses to issue API requests without a token',async()=>{
  let requested=false;
  const auth=createSessionAuth({});
  await assert.rejects(auth.fetch(async()=>{requested=true;},'/api/runs'),{status:401});
  assert.equal(requested,false);
});

test('HTTP error states are clear and never include the bearer key',()=>{
  const token='sensitive-key';
  assert.match(httpError(401,'',token).message,/incorrect or revoked/);
  assert.match(httpError(403,'',token).message,/permission/);
  assert.equal(httpError(500,`Failure echoed ${token}`,token).message,'Failure echoed [redacted]');
  assert.equal(redactSecret(`Bearer ${token}`,token),'[redacted]');
});

test('SSE parser handles chunk boundaries, comments, event IDs and multiline data',async()=>{
  const chunks=['id: epoch:4\nevent: trace\ndata: {"run', '_id":"r"}\n', 'data: {"seq":4}\n\n: ping\n\nid: epoch:5\nevent: trace_reset\ndata: {"cursor":5}'];
  const stream=new ReadableStream({start(controller){for(const chunk of chunks)controller.enqueue(new TextEncoder().encode(chunk));controller.close();}});
  const events=[];
  await consumeSse(stream,event=>events.push(event));
  assert.deepEqual(events,[
    {event:'trace',data:'{"run_id":"r"}\n{"seq":4}',id:'epoch:4'},
    {event:'trace_reset',data:'{"cursor":5}',id:'epoch:5'},
  ]);
});

test('snapshot cursor suppresses replayed deltas and keeps new tool fragments',()=>{
  const detail={run_id:'run',cursor:20,spans:[{id:'span',output:{choices:[{message:{role:'assistant',content:'Already'}}]}}],controls:[],changes:[],events:[]};
  const event={seq:20,run_id:'run',span_id:'span',kind:'model.delta',data:{choices:[{delta:{content:'Already'}}]}};
  assert.equal(applyEvent(detail,event),false);
  assert.equal(outputMessage(detail.spans[0]).content,'Already');
  assert.equal(applyEvent(detail,{...event,seq:21,data:{choices:[{delta:{content:' new',tool_calls:[{index:0,id:'tool',function:{name:'lookup',arguments:'{"q":'}}]}}]}}),true);
  applyEvent(detail,{...event,seq:22,data:{choices:[{delta:{tool_calls:[{index:0,function:{arguments:'"value"}'}}]}}]}});
  const message=outputMessage(detail.spans[0]);
  assert.equal(message.content,'Already new');
  assert.equal(message.tool_calls[0].function.arguments,'{"q":"value"}');
  assert.equal(applyEvent(detail,{...event,seq:23,run_id:'other'}),false);
});

test('interrupt preserves separate attempts and delivered guidance association',()=>{
  const detail={run_id:'run',cursor:0,spans:[],controls:[],changes:[],events:[]};
  let seq=0;
  const emit=(kind,span_id,data)=>applyEvent(detail,{run_id:'run',seq:++seq,kind,span_id,data,at_ms:seq});
  emit('span.started','old',{kind:'agent_call',entity_id:'worker'});
  emit('model.delta','old',{choices:[{delta:{content:'discarded partial'}}]});
  emit('control.accepted',null,{id:'control',action:'interrupt',status:'queued'});
  emit('span.finished','old',{status:'interrupted',output:{reason:'redirect'}});
  emit('span.started','new',{kind:'agent_call',entity_id:'worker'});
  emit('control.delivered','new',{id:'control'});
  emit('model.delta','new',{choices:[{delta:{content:'replacement'}}]});
  assert.equal(detail.spans[0].status,'interrupted');
  assert.equal(outputMessage(detail.spans[1]).content,'replacement');
  assert.equal(detail.controls[0].span_id,'new');
  assert.equal(detail.controls[0].status,'delivered');
  assert.equal(detail.events.find(e=>e.kind==='model.delta').data.choices[0].delta.content,'discarded partial');
});

test('version diff reconstructs both immutable inputs including empty lines',()=>{
  const before='identity: Writer\n\nDo not invent\nOld method';
  const after='identity: Writer\n\nUse supplied evidence\nDo not invent';
  const lines=diffLines(before,after);
  assert.equal(lines.filter(l=>l.type!=='add').map(l=>l.text).join('\n'),before);
  assert.equal(lines.filter(l=>l.type!=='remove').map(l=>l.text).join('\n'),after);
});

test('presentation replay separates thinking and narration across requests and retries',()=>{
  const detail={run_id:'run',cursor:10,spans:[],controls:[],changes:[],events:[],presentations:{r1:{narration:'Before\n\n',reasoning:'Saved',closed:false}}};
  const event={seq:10,run_id:'run',kind:'response.delta',data:{request_id:'r1',channel:'narration',text:'Before\n\n'}};
  assert.equal(applyEvent(detail,event),false);
  applyEvent(detail,{...event,seq:11,data:{request_id:'r1',channel:'reasoning',text:' thinking'}});
  applyEvent(detail,{...event,seq:12,data:{request_id:'r2',channel:'reasoning',text:'Separate'}});
  assert.equal(detail.presentations.r1.narration,'Before\n\n');
  assert.equal(detail.presentations.r1.reasoning,'Saved thinking');
  assert.equal(detail.presentations.r2.narration,'');
  applyEvent(detail,{...event,seq:13,kind:'response.reasoning.reset',data:{request_id:'r1',reasoning:'Saved'}});
  assert.equal(detail.presentations.r1.reasoning,'Saved');
  applyEvent(detail,{...event,seq:14,kind:'response.presentation.finished',data:{request_id:'r1'}});
  assert.equal(detail.presentations.r1.closed,true);
  assert.equal(detail.presentations.r2.closed,false);
});

test('dashboard permissions stay independent and each role reaches only its permitted workspace',()=>{
  const admin=normalizePermissions({permissions:{bootstrap_admin:true}});
  assert.equal(dashboardPageAllowed('admin',admin),true);
  assert.equal(dashboardPageAllowed('executions',admin),false);
  assert.equal(dashboardPageAllowed('workforce',admin),false);
  assert.equal(defaultDashboardPage(admin),'admin');

  const useOnly=normalizePermissions({permissions:{roster_use:true}});
  assert.equal(dashboardPageAllowed('approvals',useOnly),true);
  assert.equal(dashboardPageAllowed('run',useOnly),true);
  assert.equal(dashboardPageAllowed('settings',useOnly),true);
  assert.equal(dashboardPageAllowed('executions',useOnly),false);
  assert.equal(dashboardPageAllowed('operations',useOnly),false);
  assert.equal(defaultDashboardPage(useOnly),'run');

  const readOnly=normalizePermissions({permissions:{roster_read:true}});
  assert.equal(dashboardPageAllowed('operations',readOnly),true);
  assert.equal(dashboardPageAllowed('approvals',readOnly),false);
  assert.equal(defaultDashboardPage(readOnly),'executions');

  const contributor=normalizePermissions({permissions:{improvement_contribute:true}});
  assert.equal(dashboardPageAllowed('improvements',contributor),true);
  assert.equal(dashboardPageAllowed('workforce',contributor),false);
  assert.equal(dashboardPageAllowed('admin',contributor),false);
  assert.equal(defaultDashboardPage(contributor),'improvements');

  const writer=normalizePermissions({permissions:{roster_write:true}});
  assert.equal(dashboardPageAllowed('workforce',writer),true);
  assert.equal(dashboardPageAllowed('executions',writer),false);
  assert.equal(defaultDashboardPage(writer),'workforce');
});

test('permission normalization never infers roster rights or admin from another permission',()=>{
  assert.deepEqual(normalizePermissions({permissions:{roster_write:true,improvement_contribute:true}}),{
    roster_use:false,roster_read:false,roster_write:true,improvement_contribute:true,bootstrap_admin:false,
  });
  assert.deepEqual(normalizePermissions({permissions:{bootstrap_admin:true}}),{
    roster_use:false,roster_read:false,roster_write:false,improvement_contribute:false,bootstrap_admin:true,
  });
  assert.equal(dashboardPageAllowed('access',normalizePermissions({})),false);
});

test('async lifecycle keeps restart, expiry, handoff and unknown outcome distinct',()=>{
  assert.match(operationDisposition({status:'interrupted',recovery_reason:'server_restart'}),/server restart/);
  assert.match(operationDisposition({status:'interrupted'}),/not resumed or replayed/);
  assert.match(operationDisposition({status:'result_expired',result_expired_reason:'retained_memory_pressure'}),/Result expired/);
  assert.match(operationDisposition({status:'expired',recovery_reason:'handoff_ttl',handoff_expires_ms:123}),/handoff expired/);
  assert.match(operationDisposition({status:'waiting_approval'}),/one-time approval/);
  assert.match(operationDisposition({status:'waiting_tools'}),/client-tool continuation/);
  assert.equal(operationHasRetrievableResult({status:'completed',result_available:true}),true);
  assert.equal(operationHasRetrievableResult({status:'waiting_tools',result_available:true}),true);
  assert.equal(operationHasRetrievableResult({status:'interrupted',result_available:true}),false);
  assert.equal(operationHasRetrievableResult({status:'result_expired',result_available:true}),false);
});

test('approval actions are enabled only for a live server-reported pending handle',()=>{
  assert.equal(approvalCanBeResolved({status:'pending',expires_ms:Date.now()+60000}),true);
  assert.equal(approvalCanBeResolved({status:'approved'}),false);
  assert.equal(approvalCanBeResolved({status:'pending',expires_ms:Date.now()-1}),false);
  assert.match(approvalDisposition({status:'interrupted',execution_outcome:'not_executed',recovery_reason:'server_restart'}),/not executed/);
  assert.match(approvalDisposition({status:'interrupted',decision:'approved',execution_outcome:'unknown_after_restart',recovery_reason:'server_restart'}),/unknown after restart/);
  assert.equal(approvalDisposition({status:'expired'}), 'Approval expired; action was not approved.');
});

test('mutation failures distinguish rejected requests from unknown outcomes',()=>{
  assert.equal(mutationOutcomeUnknown(new TypeError('connection lost')),true);
  assert.equal(mutationOutcomeUnknown(new Error('unreadable response')),true);
  assert.equal(mutationOutcomeUnknown({status:503}),true);
  assert.equal(mutationOutcomeUnknown({status:409}),false);
  assert.equal(mutationOutcomeUnknown({status:403}),false);
  assert.equal(mutationOutcomeUnknown({status:503,code:'service_draining'}),false);
});
