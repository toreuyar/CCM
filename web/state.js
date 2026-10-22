export const activeStates = new Set(['running', 'paused', 'waiting_tools']);
export const label = value => String(value || 'unknown').replaceAll('_', ' ');
const permissionNames = ['roster_use', 'roster_read', 'roster_write', 'improvement_contribute', 'bootstrap_admin'];

export function normalizePermissions(session) {
  const permissions = session?.permissions || session?.auth?.permissions || {};
  return Object.fromEntries(permissionNames.map(name => [name, permissions[name] === true]));
}

export function dashboardPageAllowed(page, permissions) {
  switch (page) {
    case 'executions': return permissions.roster_read === true;
    case 'workforce': return permissions.roster_read === true || permissions.roster_write === true;
    case 'operations': return permissions.roster_read === true;
    case 'approvals': return permissions.roster_use === true;
    case 'run': return permissions.roster_use === true;
    case 'improvements': return permissions.roster_read === true || permissions.improvement_contribute === true;
    case 'settings': return permissions.roster_use === true;
    case 'system': return permissions.roster_read === true;
    case 'admin': return permissions.bootstrap_admin === true;
    default: return false;
  }
}

export function defaultDashboardPage(permissions) {
  for (const page of ['executions', 'workforce', 'run', 'approvals', 'settings', 'improvements', 'operations', 'system', 'admin']) {
    if (dashboardPageAllowed(page, permissions)) return page;
  }
  return 'access';
}

export function operationDisposition(operation) {
  const status = operation?.status || 'unknown';
  const reason = operation?.recovery_reason;
  if (status === 'interrupted') return `Interrupted${reason ? ` · ${label(reason)}` : ''}; this operation was not resumed or replayed.`;
  if (status === 'result_expired') return `Result expired${operation?.result_expired_reason ? ` · ${label(operation.result_expired_reason)}` : ''}; no result is available.`;
  if (status === 'expired' && (reason === 'handoff_ttl' || operation?.handoff_expires_ms != null)) return 'Client-tool handoff expired.';
  if (status === 'waiting_approval') return 'Waiting for one-time approval.';
  if (status === 'waiting_tools') return 'Waiting for client-tool continuation.';
  if (status === 'expired' && reason) return `Operation expired · ${label(reason)}.`;
  return label(status);
}

export function operationHasRetrievableResult(operation) {
  return operation?.result_available === true
    && ['completed', 'waiting_tools'].includes(operation?.status);
}

export function approvalDisposition(approval) {
  const status = approval?.status || approval?.approval?.status || 'unknown';
  const executionOutcome = approval?.execution_outcome ?? approval?.approval?.execution_outcome;
  const decision = approval?.decision ?? approval?.approval?.decision;
  const recoveryReason = approval?.recovery_reason ?? approval?.approval?.recovery_reason;
  if (executionOutcome === 'unknown_after_restart') return 'The approval was granted, but its execution outcome is unknown after restart; it will not be replayed.';
  if (status === 'interrupted' && executionOutcome === 'not_executed') return 'Interrupted by server restart before execution; this action was not executed and will not be replayed.';
  if (status === 'interrupted' && recoveryReason === 'server_restart' && String(decision).toLowerCase() === 'approved') return 'The approved action has an unknown outcome after restart; it will not be replayed.';
  if (status === 'interrupted' || recoveryReason === 'server_restart') return 'Interrupted by server restart; this action will not be replayed.';
  if (status === 'expired') return 'Approval expired; action was not approved.';
  const expires = approval?.expires_ms ?? approval?.approval?.expires_ms;
  if (status === 'pending' && expires != null && expires <= Date.now()) return 'Approval expired; refresh status before acting.';
  return label(status);
}

export function approvalCanBeResolved(approval) {
  const expires = approval?.expires_ms ?? approval?.approval?.expires_ms;
  return (approval?.status || approval?.approval?.status) === 'pending'
    && !(expires != null && expires <= Date.now());
}

export function mutationOutcomeUnknown(error) {
  if (error?.code === 'service_draining') return false;
  return error instanceof TypeError || error?.status == null || error.status >= 500;
}

export function duration(ms) {
  if (ms == null || !Number.isFinite(ms)) return '—';
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor(ms % 60000 / 1000)}s`;
}
export function messageText(message) {
  if (!message) return '';
  if (typeof message === 'string') return message;
  if (typeof message.content === 'string') return message.content;
  if (Array.isArray(message.content)) return message.content.map(part => part.text ?? JSON.stringify(part)).join('\n');
  return '';
}
export function outputMessage(span) {
  const output = span?.output;
  return output?.choices?.[0]?.message ?? (output?.report != null ? {role: 'assistant', content: output.report} : output?.role ? output : null);
}
export function applyDelta(message, chunk) {
  const next = structuredClone(message || {role: 'assistant', content: ''});
  for (const choice of chunk?.choices || []) {
    if ((choice.index || 0) !== 0) continue;
    const delta = choice.delta || {};
    if (delta.content) next.content = (next.content || '') + delta.content;
    const reasoning = delta.reasoning || delta.reasoning_content;
    if (reasoning) next.reasoning = (next.reasoning || '') + reasoning;
    for (const call of delta.tool_calls || []) {
      const index = call.index ?? 0;
      next.tool_calls ||= [];
      const target = next.tool_calls[index] ||= {id: '', type: 'function', function: {name: '', arguments: ''}};
      target.id += call.id || '';
      target.function.name += call.function?.name || '';
      target.function.arguments += call.function?.arguments || '';
    }
  }
  return next;
}
export function applyEvent(detail, event) {
  if (!detail || event.run_id !== detail.run_id || event.seq <= detail.cursor) return false;
  detail.cursor = event.seq;
  detail.events ||= [];
  detail.events.push(event);
  const data = event.data || {};
  if(event.kind.startsWith('response.')&&data.request_id){
    detail.presentations ||= {};
    const output=detail.presentations[data.request_id] ||= {narration:'',reasoning:'',closed:false};
    if(event.kind==='response.presentation')output.personality_transparency=data.personality_transparency;
    if(event.kind==='response.contract')output.chat_narration_allowed=data.chat_narration_allowed;
    if(event.kind==='response.delta')output[data.channel==='narration'?'narration':'reasoning']+=data.text||'';
    if(event.kind==='response.presentation.finished')output.closed=true;
    if(event.kind==='response.reasoning.reset')output.reasoning=data.reasoning||'';
  }
  let span = detail.spans.find(s => s.id === event.span_id);
  if (event.kind === 'span.started') {
    if (!span) detail.spans.push({...data, id: event.span_id, status: 'running', started_ms: event.at_ms, output: null});
  } else if (event.kind === 'model.delta' && span) {
    const message = applyDelta(outputMessage(span), data);
    span.output = {choices: [{message}]};
    if (data.usage) span.usage = data.usage;
    span.partial = true;
  } else if (event.kind === 'model.output' && span) {
    span.output = data;
  } else if (event.kind === 'span.input' && span) {
    span.input = data;
  } else if (event.kind === 'span.finished' && span) {
    Object.assign(span, {status: data.status, output: data.output, usage: data.usage, ended_ms: event.at_ms, partial: false});
  } else if (event.kind === 'control.accepted') {
    detail.controls.push({...data, created_ms: event.at_ms});
  } else if (event.kind === 'control.delivered') {
    const control = detail.controls.find(c => c.id === data.id);
    if (control) Object.assign(control, {status: 'delivered', span_id: event.span_id});
  } else if (event.kind === 'definition.changed') {
    detail.changes.push({...data, at_ms: event.at_ms});
  } else if (event.kind === 'request.finished') {
    detail.controls.filter(c => c.status === 'queued').forEach(c => {if (data.status !== 'waiting_tools') c.status = 'not_delivered';});
    if (span) Object.assign(span, {status: data.status, output: data.output, ended_ms: event.at_ms});
  }
  return true;
}
export function definitionText(definition) {
  if (!definition) return '';
  if (definition.system_prompt) {
    const header = Object.entries(definition).filter(([key]) => key !== 'system_prompt').map(([key,value]) => `${key}: ${typeof value === 'string' ? value : JSON.stringify(value)}`).join('\n');
    return `${header}\n\n${definition.system_prompt}`;
  }
  return JSON.stringify(definition, null, 2);
}
// Exact line diff with bounded memory; very large inputs use explicit whole-file
// replacement, never a guessed correspondence between lines.
export function diffLines(before, after) {
  const a = before.split('\n'), b = after.split('\n');
  if (a.length * b.length > 1000000) return [...a.map(text => ({type:'remove',text})), ...b.map(text => ({type:'add',text}))];
  const dp = Array.from({length:a.length + 1}, () => new Uint32Array(b.length + 1));
  for (let i=a.length-1;i>=0;i--) for (let j=b.length-1;j>=0;j--) dp[i][j] = a[i]===b[j] ? dp[i+1][j+1]+1 : Math.max(dp[i+1][j],dp[i][j+1]);
  const lines=[]; let i=0,j=0;
  while(i<a.length || j<b.length) {
    if(i<a.length && j<b.length && a[i]===b[j]) {lines.push({type:'same',text:a[i++]});j++;}
    else if(j<b.length && (i===a.length || dp[i][j+1]>=dp[i+1][j])) lines.push({type:'add',text:b[j++]});
    else lines.push({type:'remove',text:a[i++]});
  }
  return lines;
}

const SESSION_TOKEN_KEY = 'ccm-api-token';

function browserSessionStorage() {
  try { return globalThis.sessionStorage; } catch { return null; }
}

export function createSessionAuth(storage = browserSessionStorage()) {
  let token = '';
  const persist = () => {
    try {
      if (token) storage?.setItem(SESSION_TOKEN_KEY, token);
      else storage?.removeItem(SESSION_TOKEN_KEY);
    } catch { /* Keep the credential in memory if session storage is unavailable. */ }
  };
  return {
    get token() { return token; },
    load() {
      try { token = storage?.getItem(SESSION_TOKEN_KEY) || ''; }
      catch { token = ''; }
      return token;
    },
    setToken(value, {save = true} = {}) {
      token = String(value || '').trim();
      if (save) persist();
      return token;
    },
    persist,
    clear() {
      token = '';
      persist();
    },
    async fetch(fetcher, input, options = {}) {
      if (!token) throw Object.assign(new Error('Enter an access key to continue.'), {status:401});
      const headers = new Headers(options.headers || {});
      headers.set('Authorization', `Bearer ${token}`);
      return fetcher(input, {...options, headers});
    },
  };
}

export function redactSecret(message, token) {
  let safe = String(message || '');
  if (!token) return safe;
  for (const secret of new Set([`Bearer ${token}`, token, encodeURIComponent(token)])) {
    if (secret) safe = safe.replaceAll(secret, '[redacted]');
  }
  return safe;
}

export function httpError(status, detail = '', token = '') {
  let message;
  if (status === 401) message = 'Access key rejected (401). It may be incorrect or revoked.';
  else if (status === 403) message = 'Access denied (403). This key does not have permission for this action.';
  else message = redactSecret(detail, token) || `CCM request failed (HTTP ${status}).`;
  return Object.assign(new Error(message), {status});
}

export async function consumeSse(stream, onEvent) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '', eventName = '', lastEventId = '';
  let data = [];
  const dispatch = () => {
    if (data.length) onEvent({event:eventName || 'message', data:data.join('\n'), id:lastEventId});
    eventName = '';
    data = [];
  };
  const line = value => {
    if (!value) { dispatch(); return; }
    if (value.startsWith(':')) return;
    const separator = value.indexOf(':');
    const field = separator < 0 ? value : value.slice(0, separator);
    let fieldValue = separator < 0 ? '' : value.slice(separator + 1);
    if (fieldValue.startsWith(' ')) fieldValue = fieldValue.slice(1);
    if (field === 'data') data.push(fieldValue);
    else if (field === 'event') eventName = fieldValue;
    else if (field === 'id' && !fieldValue.includes('\0')) lastEventId = fieldValue;
  };
  const append = (chunk, final = false) => {
    buffer += chunk;
    while (buffer) {
      const match = /\r\n|\r|\n/.exec(buffer);
      if (!match || (!final && match[0] === '\r' && match.index === buffer.length - 1)) break;
      line(buffer.slice(0, match.index));
      buffer = buffer.slice(match.index + match[0].length);
    }
    if (final) {
      if (buffer) line(buffer);
      buffer = '';
      dispatch();
    }
  };
  try {
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      append(decoder.decode(value, {stream:true}));
    }
    append(decoder.decode(), true);
  } finally {
    reader.releaseLock();
  }
}
