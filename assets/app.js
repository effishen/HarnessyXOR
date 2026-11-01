'use strict';

(() => {
  const $ = id => document.getElementById(id);
  const S = { user: null, settings: null, workspaces: [], workspace: null, workspaceConfiguration: {}, profileUserSelected: false, projects: [], project: null, messages: [], bookmarks: new Set(), runs: [], tab: 'chat', archived: false, files: [], filePath: '', selectedFiles: new Set(), environments: [], team: null, teamEvents: [], eventSequence: 0, selectedAgentId: null, changes: null, evidence: [], projectPaused: false, teamPollBusy: false, streams: new Map(), live: new Map(), loading: 0, online: true, editor: null };
  const terminalStates = new Set(['completed', 'complete', 'done', 'failed', 'cancelled', 'canceled', 'interrupted', 'needs_input', 'completion_unconfirmed']);
  const storage = { get(key, fallback = null) { try { return localStorage.getItem(key) ?? fallback; } catch { return fallback; } }, set(key, value) { try { localStorage.setItem(key, value); } catch {} }, remove(key) { try { localStorage.removeItem(key); } catch {} } };
  const theme = storage.get('hxr-theme', 'dark');
  document.documentElement.dataset.theme = theme === 'light' ? 'light' : 'dark';

  function el(tag, attrs = {}, ...children) {
    const n = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs)) {
      if (value == null || value === false) continue;
      if (key === 'class') n.className = value;
      else if (key === 'text') n.textContent = value;
      else if (key.startsWith('on') && typeof value === 'function') n.addEventListener(key.slice(2), value);
      else if (key === 'checked' || key === 'disabled' || key === 'hidden' || key === 'selected' || key === 'required') n[key] = Boolean(value);
      else if (key === 'value') n.value = value;
      else n.setAttribute(key, String(value));
    }
    children.flat(Infinity).forEach(child => { if (child != null && child !== false) n.append(child instanceof Node ? child : document.createTextNode(String(child))); });
    return n;
  }
  const button = (text, fn, cls = 'button', attrs = {}) => el('button', { type: 'button', class: cls, onclick: fn, ...attrs }, text);
  const help = text => el('span', { class: 'form-hint' }, text);
  const field = (text, input, hint) => el('label', {}, text, input, hint ? help(hint) : null);
  const iconButton = (text, title, fn) => button(text, fn, 'icon-button', { title, 'aria-label': title });
  const formatBytes = n => !Number.isFinite(+n) ? '—' : +n < 1024 ? `${n} B` : +n < 1048576 ? `${(+n / 1024).toFixed(1)} KB` : `${(+n / 1048576).toFixed(1)} MB`;
  const formatNumber = n => Number.isFinite(+n) ? new Intl.NumberFormat().format(+n) : '—';
  const shortDate = iso => { const d = new Date(iso); return Number.isNaN(d.getTime()) ? '' : d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); };
  const errorText = error => error?.message || String(error);
  const active = run => run && !terminalStates.has(run.status);
  const architectRun = run => run && (run.agent_id ? run.agent_id === S.team?.architect?.id : !run.agent_role || ['architect','principal_architect'].includes(run.agent_role));
  const currentRun = () => S.runs.find(r => r.project_id === S.project?.id && architectRun(r) && active(r));
  const acceptingGuidance = run => !run || ['delegated', 'paused'].includes(run.status);
  const architectMessage = message => !(message.role === 'user' && (message.metadata?.controller_step || message.metadata?.origin === 'controller')) && (message.metadata?.agent_id ? message.metadata.agent_id === S.team?.architect?.id : !message.metadata?.agent_role || ['architect','principal_architect'].includes(message.metadata.agent_role));
  const projectApi = path => `/projects/${encodeURIComponent(S.project.id)}${path}`;
  const scope = suffix => `hxr-${S.user?.id || S.user?.username || 'local'}-${suffix}`;
  const composerKey = () => scope(`draft-${S.workspace?.id || ''}-${S.project?.id || ''}`);
  const bookmarks = () => S.bookmarks;

  async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (options.body != null && typeof options.body !== 'string' && !(options.body instanceof Blob) && !(options.body instanceof ArrayBuffer)) { headers.set('Content-Type', 'application/json'); options.body = JSON.stringify(options.body); }
    const response = await fetch(`/api${path}`, { credentials: 'same-origin', ...options, headers });
    if (!response.ok) {
      let message = `Request failed (${response.status})`;
      try { const body = await response.json(); message = body.error?.message || body.error || body.message || message; } catch {}
      if (response.status === 401 && !path.startsWith('/auth/')) showAuth({ configured: true, authenticated: false });
      throw new Error(typeof message === 'string' ? message : JSON.stringify(message));
    }
    if (response.status === 204) return null;
    const type = response.headers.get('content-type') || '';
    return type.includes('json') ? response.json() : response.text();
  }
  async function attempt(fn) { try { await fn(); } catch (e) { notice(errorText(e), true); } }
  function notice(text, error = false) { if (!$('notice-text')) return; $('notice-text').textContent = text; $('notice').classList.toggle('error', error); $('notice').hidden = false; }
  function toast(text, error = false) { const n = el('div', { class: `toast${error ? ' error' : ''}` }, text); $('toast-region').append(n); setTimeout(() => n.remove(), 6500); }
  function setConnected(value) { if (!$('connection-state')) return; S.online = value; $('connection-state').replaceChildren(el('span', { class: `status-dot${value ? '' : ' error'}` }), value ? 'Connected' : 'Reconnecting'); $('send-prompt').disabled = !value || !S.project || S.project.archived || S.project.locked || !acceptingGuidance(currentRun()); }
  function modal(title, body, actions = [], wide = false) {
    const d = $('modal');
    if (d.open) d.close();
    d.classList.toggle('wide', wide);
    const status = el('span', { class: 'form-status', role: 'status' });
    const footer = el('div', { class: 'modal-footer' }, status, ...actions);
    $('modal-content').replaceChildren(el('div', { class: 'modal-header' }, el('h2', {}, title), iconButton('×', 'Close dialog', () => d.close())), el('div', { class: 'modal-body' }, body), footer);
    d.showModal();
    return { close: () => d.close(), status: text => { status.textContent = text; }, dialog: d };
  }
  async function formAction(m, btn, fn) { btn.disabled = true; m.status('Working…'); try { await fn(); } catch (e) { m.status(errorText(e)); } finally { btn.disabled = false; } }
  function download(path, name) { const link = el('a', { href: `/api${path}`, download: name || '' }); document.body.append(link); link.click(); link.remove(); }
  function downloadJSON(value, name) { const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: 'application/json' })); const a = el('a', { href: url, download: name }); document.body.append(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000); }

  async function authStatus() {
    try { const result = await api('/auth/status'); if (result.authenticated) { S.user = result.user; try { await startApp(); } catch (e) { setConnected(false); notice(errorText(e), true); } } else showAuth(result); }
    catch (e) { $('auth-screen').hidden = false; $('auth-description').textContent = 'The server is unavailable. Start HarnessyXOR on this host, then refresh.'; $('setup-help').hidden = true; $('login-form').hidden = true; }
  }
  function showAuth(status) {
    for (const stream of S.streams.values()) stream.close();
    S.streams.clear();
    $('app-shell').hidden = true; $('auth-screen').hidden = false;
    $('setup-help').hidden = Boolean(status.configured); $('login-form').hidden = !status.configured;
    $('auth-description').textContent = status.configured ? 'Sign in to your workspace.' : 'Set up your first account.';
    $('login-error').textContent = '';
    if (status.configured) $('login-username').focus();
  }
  async function startApp() {
    $('auth-screen').hidden = true; $('app-shell').hidden = false;
    $('signed-in-user').textContent = S.user?.username || 'Signed in';
    $('user-avatar').textContent = (S.user?.username || 'U').slice(0, 1).toUpperCase();
    const [settings, workspaces, runs] = await Promise.all([api('/settings'), api('/workspaces'), api('/runs')]);
    S.settings = settings; S.workspaces = Array.isArray(workspaces) ? workspaces : workspaces.workspaces || []; S.runs = Array.isArray(runs) ? runs : runs.runs || [];
    renderWorkspaces(); renderModels(); renderQueue(); setConnected(true);
    const previous = storage.get(scope('workspace'));
    const selected = S.workspaces.find(w => w.id === previous) || S.workspaces[0];
    if (selected) await selectWorkspace(selected.id); else showWelcome();
    S.runs.filter(r => active(r) && architectRun(r)).forEach(connectRun);
  }
  function renderWorkspaces() {
    $('workspace-select').replaceChildren(el('option', { value: '' }, S.workspaces.length ? 'Choose a workspace' : 'No workspaces yet'), S.workspaces.map(w => el('option', { value: w.id, selected: w.id === S.workspace?.id }, w.name)));
  }
  async function selectWorkspace(id) {
    saveDraft(); S.workspace = S.workspaces.find(w => w.id === id) || null; S.project = null; S.messages = []; S.filePath = ''; S.selectedFiles.clear();
    storage.set(scope('workspace'), id); $('workspace-select').value = id; $('workspace-label').textContent = S.workspace?.name || 'Workspace';
    if (!S.workspace) { S.projects = []; renderProjects(); showWelcome(); return; }
    await loadProjects();
    const config = await api(`/workspaces/${encodeURIComponent(id)}/settings`); S.workspaceConfiguration = config.settings || {}; S.profileUserSelected = false; renderModels();
    const previous = storage.get(scope(`project-${id}`));
    const project = S.projects.find(p => p.id === previous) || S.projects.find(p => p.pinned) || S.projects[0];
    if (project) await selectProject(project.id); else showWelcome();
  }
  async function loadProjects() {
    if (!S.workspace) return;
    const workspaceId = S.workspace.id;
    const response = await api(`/workspaces/${encodeURIComponent(workspaceId)}/projects?archived=true`);
    if (S.workspace?.id !== workspaceId) return;
    S.projects = Array.isArray(response) ? response : response.projects || [];
    renderProjects();
  }
  function renderProjects() {
    const query = $('project-search').value.toLowerCase();
    const projects = S.projects.filter(p => Boolean(p.archived) === S.archived && (!query || p.title.toLowerCase().includes(query))).sort((a, b) => Number(b.pinned) - Number(a.pinned) || b.updated_at.localeCompare(a.updated_at));
    $('project-count').textContent = projects.length;
    $('active-projects').classList.toggle('selected', !S.archived); $('archived-projects').classList.toggle('selected', S.archived);
    $('project-list').replaceChildren(...(projects.length ? projects.map(p => {
      const run = S.runs.find(r => r.project_id === p.id && active(r));
      return button([el('span', { class: 'project-icon', 'aria-hidden': 'true' }, p.pinned ? '⌑' : '▤'), el('span', { class: 'project-title' }, p.title), run ? el('span', { class: 'project-status', title: run.status }, el('span', { class: run.status === 'queued' ? 'status-dot busy' : 'spinner' })) : p.locked ? el('span', { class: 'project-status', title: 'Locked' }, '⌑') : null], () => attempt(() => selectProject(p.id)), `project-row${S.project?.id === p.id ? ' active' : ''}`, { title: p.title, 'aria-current': S.project?.id === p.id ? 'page' : undefined });
    }) : [el('div', { class: 'sidebar-empty' }, query ? 'No matching projects.' : S.workspace ? S.archived ? 'No archived projects.' : 'Start a project to keep your conversation and files together.' : 'Create a workspace to get started.')]));
    $('new-project').disabled = !S.workspace;
  }
  function showWelcome() {
    ['chat-view', 'files-view', 'team-view', 'changes-view', 'knowledge-view', 'regression-view', 'environments-view', 'project-tabs'].forEach(id => $(id).hidden = true);
    $('welcome-view').hidden = false; $('project-label').textContent = S.workspace ? 'Start a project' : 'Welcome'; $('project-lock').hidden = true; $('project-menu').disabled = true; $('search-conversation').disabled = true;
    $('welcome-create').replaceChildren(S.workspace ? 'Create a project ' : 'Create a workspace ', el('span', { 'aria-hidden': 'true' }, '+'));
  }
  async function selectProject(id) {
    saveDraft(); for (const stream of S.streams.values()) stream.close(); S.streams.clear(); const token = ++S.loading; S.project = S.projects.find(p => p.id === id); if (!S.project) return;
    storage.set(scope(`project-${S.workspace.id}`), id); S.filePath = ''; S.selectedFiles.clear(); S.messages = []; S.bookmarks = new Set(); S.environments = []; S.team = null; S.changes = null; S.teamEvents = []; S.eventSequence = 0; S.selectedAgentId = null; S.projectPaused = false; S.tab = 'chat';
    $('project-label').textContent = S.project.title; $('project-lock').hidden = !S.project.locked && !S.project.archived; $('project-lock').textContent = S.project.archived ? 'Archived' : 'Locked'; $('project-menu').disabled = false; $('search-conversation').disabled = false;
    $('welcome-view').hidden = true; $('project-tabs').hidden = false; renderProjects(); restoreDraft(); renderComposer(); closeSidebar();
    const [team, savedBookmarks] = await Promise.all([api(`/projects/${encodeURIComponent(id)}/team`), api(`/projects/${encodeURIComponent(id)}/bookmarks`)]);
    if (token !== S.loading) return;
    const messages = await api(`/projects/${encodeURIComponent(id)}/agents/${encodeURIComponent(team.architect.id)}/messages`);
    if (token !== S.loading) return;
    S.team = team; mergeTeamEvents(team.events || []); S.messages = Array.isArray(messages) ? messages : messages.messages || []; S.bookmarks = new Set(Array.isArray(savedBookmarks) ? savedBookmarks : savedBookmarks.message_ids || []); renderProjectProgress();
    S.runs.filter(run => run.project_id === id && active(run) && architectRun(run)).forEach(connectRun); renderMessages(true); await switchTab(S.tab);
    loadChanges().catch(error => notice(errorText(error), true));
  }
  async function refreshMessages(scroll = false) {
    if (!S.project) return; const id = S.project.id;
    if (!S.team?.architect?.id) return;
    const messages = await api(`/projects/${encodeURIComponent(id)}/agents/${encodeURIComponent(S.team.architect.id)}/messages`);
    if (S.project?.id !== id) return;
    S.messages = Array.isArray(messages) ? messages : messages.messages || []; renderMessages(scroll);
  }
  async function switchTab(tab) {
    S.tab = tab;
    if (!S.project) return;
    for (const n of document.querySelectorAll('.tab')) { const selected = n.dataset.tab === tab; n.classList.toggle('active', selected); n.setAttribute('aria-selected', String(selected)); }
    for (const name of ['chat', 'team', 'changes', 'knowledge', 'regression', 'files', 'environments']) $(`${name}-view`).hidden = name !== tab;
    if (tab === 'files') await loadFiles();
    if (tab === 'team') { await refreshTeam(); renderTeam(); }
    if (tab === 'changes') await loadChanges();
    if (tab === 'knowledge') { await refreshTeam(); renderKnowledge(); }
    if (tab === 'regression') await loadRegression();
    if (tab === 'environments') await loadEnvironments();
    if (tab === 'chat') { renderMessages(); renderComposer(); renderProjectProgress(); }
  }
  function createWorkspace() {
    const name = el('input', { required: true, placeholder: 'A name for this workspace', maxlength: 160 });
    const path = el('input', { required: true, placeholder: '/absolute/path/to/new-workspace', spellcheck: false });
    const body = el('form', { class: 'form-grid' }, el('p', { class: 'muted small' }, 'A workspace keeps projects, conversations and files in one place. Choose a folder for this workspace’s projects and team records.'), field('Workspace name', name), field('Folder on this host', path, 'Use an absolute path to a new workspace folder.'));
    const save = button('Create workspace', () => body.requestSubmit(), 'button primary');
    const m = modal('Create a workspace', body, [button('Cancel', () => m.close()), save]);
    body.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const w = await api('/workspaces', { method: 'POST', body: { name: name.value.trim(), path: path.value.trim() } }); S.workspaces.push(w); renderWorkspaces(); m.close(); S.archived = false; await selectWorkspace(w.id); createProject(); }); };
    name.focus();
  }
  function createProject() {
    if (!S.workspace) return createWorkspace();
    const title = el('input', { required: true, placeholder: 'What are we building?', maxlength: 200 });
    const form = el('form', { class: 'form-grid' }, field('Project name', title), help('Each project starts with a Principal Architect. Teammates, tasks, Git changes and shared knowledge stay inside this project.'));
    const save = button('Create project', () => form.requestSubmit(), 'button primary');
    const m = modal('Create a project', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const project = await api(`/workspaces/${encodeURIComponent(S.workspace.id)}/projects`, { method: 'POST', body: { title: title.value.trim() } }); m.close(); S.archived = false; await loadProjects(); await selectProject(project.id); $('composer-input').focus(); }); }; title.focus();
  }
  async function patchProject(patch) { const p = await api(`/projects/${encodeURIComponent(S.project.id)}`, { method: 'PATCH', body: patch }); const at = S.projects.findIndex(old => old.id === p.id); if (at >= 0) S.projects[at] = p; S.project = p; renderProjects(); renderComposer(); $('project-label').textContent = p.title; $('project-lock').hidden = !p.locked && !p.archived; $('project-lock').textContent = p.archived ? 'Archived' : 'Locked'; }
  function projectMenu() {
    if (!S.project) return;
    const p = S.project;
    const body = el('div', { class: 'danger-menu' });
    const m = modal('Project actions', body, [button('Close', () => m.close())]);
    body.append(button('Rename project', () => { const input = el('input', { value: p.title, required: true, maxlength: 200 }); const form = el('form', { class: 'form-grid' }, field('Project name', input)); const save = button('Save name', () => form.requestSubmit(), 'button primary'); const n = modal('Rename project', form, [button('Cancel', () => n.close()), save]); form.onsubmit = e => { e.preventDefault(); formAction(n, save, async () => { await patchProject({ title: input.value.trim() }); n.close(); }); }; input.focus(); }), button(p.pinned ? 'Unpin project' : 'Pin project', () => attempt(async () => { await patchProject({ pinned: !p.pinned }); m.close(); })), button(p.locked ? 'Unlock project' : 'Lock project', () => attempt(async () => { await patchProject({ locked: !p.locked }); m.close(); })), button(p.archived ? 'Restore from archive' : 'Archive project', () => attempt(async () => { await patchProject({ archived: !p.archived }); S.archived = !p.archived; renderProjects(); m.close(); })), button('Export conversation as Markdown', () => { download(`/projects/${encodeURIComponent(p.id)}/export.md?reasoning=true&tools=true&stats=true`, `${safeFilename(p.title)}.md`); m.close(); }), button('Export conversation as JSON', () => attempt(async () => { const data = await api(`/projects/${encodeURIComponent(p.id)}/export`); downloadJSON(data, `${safeFilename(p.title)}.json`); m.close(); })));
  }
  const safeFilename = text => (text || 'project').replace(/[^a-zA-Z0-9._-]+/g, '-').slice(0, 100);

  function inlineMarkdown(text) {
    const fragment = document.createDocumentFragment();
    const parts = String(text).split(/(`[^`\n]+`|\*\*[^*\n]+\*\*)/g);
    for (const part of parts) fragment.append(part.startsWith('`') && part.endsWith('`') ? el('code', {}, part.slice(1, -1)) : part.startsWith('**') && part.endsWith('**') ? el('strong', {}, part.slice(2, -2)) : document.createTextNode(part));
    return fragment;
  }
  function renderText(text) {
    const fragment = document.createDocumentFragment();
    const chunks = String(text || '').split(/(```[^\n]*\n[\s\S]*?```|```[^\n]*\n[\s\S]*$)/g);
    for (const chunk of chunks) {
      if (!chunk) continue;
      if (chunk.startsWith('```')) { const firstNewline = chunk.indexOf('\n'); const content = chunk.slice(firstNewline + 1).replace(/```$/, ''); fragment.append(el('pre', {}, el('code', {}, content))); continue; }
      const paragraphs = chunk.split(/\n\s*\n/);
      for (const paragraph of paragraphs) {
        if (!paragraph.trim()) continue;
        if (/^#{1,3}\s/.test(paragraph) && !paragraph.includes('\n')) fragment.append(el('h3', {}, inlineMarkdown(paragraph.replace(/^#+\s/, ''))));
        else if (paragraph.split('\n').every(line => /^[-*]\s/.test(line) || !line.trim())) fragment.append(el('ul', {}, paragraph.split('\n').filter(Boolean).map(line => el('li', {}, inlineMarkdown(line.replace(/^[-*]\s/, ''))))));
        else fragment.append(el('p', {}, inlineMarkdown(paragraph)));
      }
    }
    return fragment;
  }
  async function toggleBookmark(messageId) {
    const projectId = S.project.id; const enabled = !S.bookmarks.has(messageId);
    await api(`/projects/${encodeURIComponent(projectId)}/bookmarks/${encodeURIComponent(messageId)}`, { method: 'PUT', body: { enabled } });
    if (S.project?.id !== projectId) return;
    if (enabled) S.bookmarks.add(messageId); else S.bookmarks.delete(messageId); renderMessages();
  }
  function messageNode(message, saved) {
    const role = message.role || 'assistant';
    const n = el('article', { class: `message ${role}${saved.has(message.id) ? ' bookmarked' : ''}`, id: `message-${message.id}` });
    const actions = el('span', { class: 'message-actions' }, iconButton(saved.has(message.id) ? '★' : '☆', saved.has(message.id) ? 'Remove bookmark' : 'Bookmark message', () => attempt(() => toggleBookmark(message.id))), iconButton('⧉', 'Copy message', () => copyText(message.content || '')));
    const title = role === 'user' ? 'You' : role === 'tool' ? message.metadata?.name || 'Tool result' : role === 'system' || role === 'event' ? 'Event' : S.team?.architect?.label || 'Principal Architect';
    if (role === 'tool') { const d = el('details', { class: 'tool-detail', 'data-detail-id': `tool-${message.id}` }, el('summary', {}, title), el('pre', {}, message.content || '')); n.append(d); return n; }
    n.append(el('div', { class: 'message-header' }, el('span', { class: `role-icon${role === 'user' ? ' user-icon' : ''}`, 'aria-hidden': 'true' }, role === 'user' ? 'U' : '✳'), el('span', { class: 'role-name' }, title), el('time', { class: 'message-time', datetime: message.created_at, title: new Date(message.created_at).toLocaleString() }, shortDate(message.created_at)), actions));
    const meta = message.metadata || {};
    if (meta.reasoning || meta.thinking) n.append(el('details', { class: 'thinking-detail', 'data-detail-id': `reasoning-${message.id}` }, el('summary', {}, 'Thinking'), el('pre', {}, meta.reasoning || meta.thinking)));
    n.append(el('div', { class: 'message-content' }, renderText(message.content || '')));
    if (meta.metrics || meta.usage) n.append(metricsNode(meta.metrics || meta.usage));
    return n;
  }
  function metricsNode(metrics) {
    const input = metrics.prompt_tokens ?? metrics.input_tokens ?? metrics.prompt_eval_count;
    const output = metrics.completion_tokens ?? metrics.output_tokens ?? metrics.eval_count;
    const cached = metrics.cached_tokens ?? metrics.cache_read_input_tokens ?? metrics.prompt_tokens_details?.cached_tokens;
    const speed = metrics.tokens_per_second ?? metrics.output_tokens_per_second ?? metrics.generation_tokens_per_second;
    const seconds = metrics.elapsed_seconds ?? metrics.duration_seconds ?? (metrics.total_ms != null ? metrics.total_ms / 1000 : null);
    const bits = [];
    if (input != null) bits.push(`${formatNumber(input)} prompt tok`);
    if (cached != null) bits.push(`${formatNumber(cached)} cached`);
    if (input > 0 && cached != null) bits.push(`Hit ${(cached / input * 100).toFixed(0)}%`);
    if (output != null) bits.push(`${formatNumber(output)} output tok`);
    if (seconds != null) bits.push(`${(+seconds).toFixed(1)}s`);
    if (speed != null) bits.push(`${(+speed).toFixed(1)} tok/s`);
    if (metrics.prompt_tokens_per_second != null) bits.push(`PP ${(+metrics.prompt_tokens_per_second).toFixed(1)} tok/s`);
    const detail = iconButton('ⓘ', 'View performance details', () => modal('Performance details', el('pre', { class: 'small muted' }, JSON.stringify(metrics, null, 2)), [button('Close', () => $('modal').close())]));
    return el('div', { class: 'metric-line' }, bits.join(' · '), detail);
  }
  function renderMessages(scroll = false) {
    if (!S.project) return;
    const pane = $('transcript-scroll'); const openDetails = new Set([...pane.querySelectorAll('details[open][data-detail-id]')].map(n => n.dataset.detailId)); const wasAtEnd = pane.scrollHeight - pane.scrollTop - pane.clientHeight < 140;
    const saved = bookmarks();
    const nodes = S.messages.filter(architectMessage).filter(m => m.role !== 'system' || m.content).map(m => messageNode(m, saved));
    const run = currentRun(); const live = run && S.live.get(run.id);
    const savedRun = run ? S.messages.filter(architectMessage).filter(message => message.metadata?.run_id === run.id) : [];
    const savedText = savedRun.filter(message => message.role === 'assistant').map(message => message.content || '').join('');
    const savedReasoning = savedRun.filter(message => message.role === 'assistant').map(message => message.metadata?.reasoning || '').join('');
    const pendingText = live ? live.text.startsWith(savedText) ? live.text.slice(savedText.length) : live.text : '';
    const pendingReasoning = live ? live.reasoning.startsWith(savedReasoning) ? live.reasoning.slice(savedReasoning.length) : live.reasoning : '';
    const savedTools = new Set(savedRun.filter(message => message.role === 'tool').map(message => message.metadata?.tool_call_id));
    const pendingTools = live ? live.tools.filter(tool => !savedTools.has(tool.id)) : [];
    if (live && (pendingText || pendingReasoning || pendingTools.length || live.status || active(run))) {
      const n = el('article', { class: 'message assistant', id: `live-${run.id}` }, el('div', { class: 'message-header' }, el('span', { class: 'role-icon', 'aria-hidden': 'true' }, '✳'), el('span', { class: 'role-name' }, S.team?.architect?.label || 'Principal Architect'), el('span', { class: 'spinner', 'aria-hidden': 'true' }), el('span', { class: 'message-time' }, readableStatus(run.status))));
      if (live.truncated) n.append(el('p', { class: 'small muted' }, 'Showing the latest streaming output. The saved conversation contains the full completed steps.'));
      if (pendingReasoning) n.append(el('details', { class: 'thinking-detail', 'data-detail-id': `live-reasoning-${run.id}` }, el('summary', {}, 'Thinking'), el('pre', {}, pendingReasoning)));
      for (const [index, tool] of pendingTools.entries()) n.append(el('details', { class: 'tool-detail', 'data-detail-id': `live-tool-${run.id}-${index}` }, el('summary', {}, tool.name || 'Tool'), el('pre', {}, tool.text || 'Running…')));
      if (pendingText) n.append(el('div', { class: 'message-content' }, renderText(pendingText)));
      else if (!pendingReasoning && !pendingTools.length) n.append(el('p', { class: 'muted small' }, live.status || (run.status === 'queued' ? 'Waiting for an execution slot…' : run.status === 'delegated' ? 'The architect is waiting for teammates. You can add guidance below.' : run.status === 'paused' ? 'Project paused. Guidance is saved without starting inference.' : 'Waiting for the model…')));
      if (live.metrics) n.append(metricsNode(live.metrics)); nodes.push(n);
    }
    if (!nodes.length) nodes.push(el('div', { class: 'conversation-empty' }, el('span', { 'aria-hidden': 'true' }, '✳'), el('h2', {}, 'What should this project achieve?'), el('p', {}, 'Tell the Principal Architect your goal. The architect coordinates the team and brings decisions and results back here.')));
    $('transcript').replaceChildren(...nodes);
    for (const detail of $('transcript').querySelectorAll('details[data-detail-id]')) detail.open = openDetails.has(detail.dataset.detailId);
    $('prompt-navigator').replaceChildren(...S.messages.filter(architectMessage).filter(m => m.role === 'user').map((m, i, all) => button('', () => $(`message-${m.id}`)?.scrollIntoView({ behavior: 'smooth', block: 'start' }), `nav-prompt${saved.has(m.id) ? ' bookmarked' : ''}${i === all.length - 1 ? ' latest' : ''}`, { title: `Prompt ${i + 1}: ${(m.content || '').slice(0, 100)}`, 'aria-label': `Jump to prompt ${i + 1}${saved.has(m.id) ? ', bookmarked' : ''}` })));
    if (scroll || wasAtEnd) pane.scrollTop = pane.scrollHeight;
    updateJump(); renderComposer();
  }
  async function copyText(text) { try { await navigator.clipboard.writeText(text); } catch { const area = el('textarea', { value: text, class: 'sr-only' }); document.body.append(area); area.select(); document.execCommand('copy'); area.remove(); } }
  function updateJump() { const pane = $('transcript-scroll'); $('jump-latest').hidden = pane.scrollHeight - pane.scrollTop - pane.clientHeight < 180; }
  function saveDraft() { if (S.project) storage.set(composerKey(), $('composer-input').value); }
  function restoreDraft() { $('composer-input').value = S.project ? storage.get(composerKey(), '') : ''; growComposer(); }
  function growComposer() { const area = $('composer-input'); area.style.height = 'auto'; area.style.height = `${Math.min(260, Math.max(75, area.scrollHeight))}px`; }
  function renderModels() {
    const current = S.profileUserSelected ? $('model-select').value : S.workspaceConfiguration.default_profile_id || S.settings.active_profile_id;
    $('model-select').replaceChildren(...(S.settings?.profiles || []).map(p => el('option', { value: p.id, selected: p.id === current }, p.name || p.model || p.id)));
    if (!$('model-select').value && S.settings?.profiles[0]) $('model-select').value = S.settings.profiles[0].id;
    const p = S.settings?.profiles.find(p => p.id === $('model-select').value); $('context-hint').textContent = p ? `${formatNumber(p.context_window)} context` : '';
  }
  function readableStatus(status) { return String(status || '').replaceAll('_', ' ').replace(/^./, s => s.toUpperCase()); }
  function renderComposer() {
    const readonly = !S.project || S.project.locked || S.project.archived;
    const run = currentRun();
    $('composer-input').disabled = readonly; $('send-prompt').disabled = readonly || !S.online || !acceptingGuidance(run);
    $('composer-input').placeholder = readonly ? S.project?.archived ? 'Restore this project to continue.' : 'Unlock this project to continue.' : 'Tell the Principal Architect what you want to do…';
    $('stop-run').hidden = !run; $('send-prompt').title = run && acceptingGuidance(run) ? 'Send guidance to the architect' : run ? 'Wait for this architect’s execution step to finish' : 'Send message'; $('send-prompt').setAttribute('aria-label', 'Send message');
    const liveStatus = run ? S.live.get(run.id)?.status : '';
    const lastRun = S.runs.find(r => r.project_id === S.project?.id && architectRun(r));
    const needsReview = !run && lastRun && ['failed', 'interrupted', 'completion_unconfirmed', 'needs_input'].includes(lastRun.status);
    $('composer-status').textContent = run ? liveStatus?.startsWith('Waiting for model') ? liveStatus : run.status === 'queued' ? 'Queued · waiting for a slot' : readableStatus(run.status) : needsReview ? `${readableStatus(lastRun.status)} · view Agent runs` : 'Conversations are saved on this host';
    $('composer-status').classList.toggle('needs-review', Boolean(needsReview));
    $('composer-status').title = needsReview ? lastRun.error || 'Review this run before continuing.' : '';
    $('run-state').replaceChildren(...(run ? [el('span', { class: run.status === 'queued' ? 'status-dot busy' : 'spinner' }), readableStatus(run.status)] : needsReview ? [button(readableStatus(lastRun.status), showRuns, 'text-button')] : []));
    const approval = run?.approval || S.live.get(run?.id)?.approval;
    $('approval-panel').hidden = !approval;
    if (approval) {
      const identifier = approval.id || approval.approval_id;
      const title = approval.tool_name || approval.tool || approval.name || 'Tool request';
      $('approval-panel').replaceChildren(el('strong', {}, `Permission needed · ${title}`), el('p', { class: 'small muted' }, approval.description || approval.summary || 'Review the requested tool action before allowing it.'), el('details', { class: 'tool-detail' }, el('summary', {}, 'Request details'), el('pre', {}, JSON.stringify(approval.arguments || approval, null, 2))), el('div', { class: 'approval-actions' }, button('Allow this request', () => attempt(() => decideApproval(run.id, identifier, true)), 'button primary'), button('Deny', () => attempt(() => decideApproval(run.id, identifier, false)), 'button')));
    }
  }
  async function sendPrompt(e) {
    e.preventDefault(); const prompt = $('composer-input').value.trim(); if (!prompt || !S.project) return;
    if (!acceptingGuidance(currentRun())) return notice('The architect is executing a step. Keep drafting here; guidance can be added when it delegates or pauses.');
    const btn = $('send-prompt'); btn.disabled = true;
    try {
      const projectId = S.project.id; const draftKey = composerKey();
      const run = await api('/runs', { method: 'POST', body: { project_id: projectId, prompt, profile_id: $('model-select').value } });
      if (S.project?.id === projectId && $('composer-input').value.trim() === prompt) { $('composer-input').value = ''; growComposer(); }
      storage.remove(draftKey); S.runs = [run, ...S.runs.filter(r => r.id !== run.id)]; connectRun(run); renderQueue(); renderProjects(); renderComposer(); loadChanges().catch(() => {});
      await refreshMessages(true);
    } catch (error) { notice(errorText(error), true); }
    finally { renderComposer(); }
  }
  async function decideApproval(runId, approvalId, allow) { await api(`/runs/${encodeURIComponent(runId)}/approval`, { method: 'POST', body: { approval_id: approvalId, allow } }); const run = S.runs.find(r => r.id === runId); if (run) run.approval = null; if (S.live.has(runId)) S.live.get(runId).approval = null; renderComposer(); }
  function connectRun(run) {
    if (!active(run) || !architectRun(run) || S.streams.has(run.id)) return;
    if (!S.live.has(run.id)) S.live.set(run.id, { text: '', reasoning: '', tools: [], metrics: null, approval: run.approval, seen: new Set(), snapshotSeq: 0, status: '' });
    const stream = new EventSource(`/api/runs/${encodeURIComponent(run.id)}/events`); S.streams.set(run.id, stream);
    stream.addEventListener('snapshot', event => {
      try {
        const snapshot = JSON.parse(event.data); const index = S.runs.findIndex(r => r.id === run.id); if (index >= 0) S.runs[index] = snapshot;
        const live = S.live.get(run.id);
        if (live) {
          live.approval = snapshot.approval;
          if (typeof snapshot.live_content === 'string') live.text = snapshot.live_content;
          if (typeof snapshot.live_reasoning === 'string') live.reasoning = snapshot.live_reasoning;
          if (Number.isFinite(snapshot.last_seq)) live.snapshotSeq = snapshot.last_seq;
          live.truncated = Boolean(snapshot.live_truncated);
        }
        scheduleDraw();
      } catch {}
    });
    stream.addEventListener('resync', () => {
      if (S.project?.id === run.project_id && architectRun(run)) refreshMessages().catch(() => {});
    });
    stream.addEventListener('run', event => {
      let envelope; try { envelope = JSON.parse(event.data); } catch { return; }
      handleRunEvent(run.id, envelope);
    });
    stream.onopen = () => setConnected(true);
    stream.onerror = () => { if (active(S.runs.find(r => r.id === run.id))) $('composer-status').textContent = 'Reconnecting to the run…'; };
  }
  let drawPending = false;
  function scheduleDraw() { if (drawPending) return; drawPending = true; requestAnimationFrame(() => { drawPending = false; if (S.tab === 'chat') renderMessages(); renderQueue(); renderProjects(); renderComposer(); }); }
  function handleRunEvent(runId, envelope) {
    const run = S.runs.find(r => r.id === runId); const live = S.live.get(runId); if (!run || !live) return;
    if (envelope.seq != null) { if (live.seen.has(envelope.seq)) return; live.seen.add(envelope.seq); if (live.seen.size > 12000) live.seen.delete(live.seen.values().next().value); }
    const data = envelope.data || {}; const kind = envelope.kind;
    if ((kind === 'token' || kind === 'reasoning') && envelope.seq <= (live.snapshotSeq || 0)) return;
    if (kind === 'token') { live.text += data.text || ''; if (live.status.startsWith('Waiting for model')) live.status = ''; }
    if (kind === 'reasoning') { live.reasoning += data.text || ''; if (live.status.startsWith('Waiting for model')) live.status = ''; }
    if (kind === 'status') { run.status = data.status || (typeof data === 'string' ? data : run.status); live.status = data.message || data.detail || ''; renderProjectProgress(); }
    if (kind === 'metrics') { live.metrics = data; run.metrics = data; }
    if (kind === 'approval') { live.approval = data; run.approval = data; }
    if (kind === 'tool_start') live.tools.push({ id: data.id || data.tool_call_id, name: data.name || data.tool || 'Tool', text: data.arguments ? JSON.stringify(data.arguments, null, 2) : 'Running…' });
    if (kind === 'tool_result') { const tool = live.tools.find(t => t.id && t.id === (data.id || data.tool_call_id)) || live.tools[live.tools.length - 1]; const text = data.output ?? data.result ?? data.content ?? data; if (tool) tool.text = typeof text === 'string' ? text : JSON.stringify(text, null, 2); else live.tools.push({ name: data.name || 'Tool result', text: typeof text === 'string' ? text : JSON.stringify(text, null, 2) }); }
    if (kind === 'completion_check') { live.status = data.message || data.status || 'Checking whether the task is complete…'; }
    if (kind === 'waiting') { live.status = `Waiting for model · quiet ${Math.round((data.quiet_ms || 0) / 1000)}s`; if (S.project?.id === run.project_id) $('composer-status').textContent = live.status; }
    if (kind === 'error') { run.error = data.message || data.error || 'The run failed.'; if (S.project?.id === run.project_id && architectRun(run)) notice(run.error, true); }
    if (kind === 'done' || (kind === 'status' && terminalStates.has(run.status))) {
      run.status = data.status || (terminalStates.has(run.status) ? run.status : 'completed');
      S.streams.get(runId)?.close(); S.streams.delete(runId);
      if (S.project?.id === run.project_id && architectRun(run)) refreshMessages(true).catch(e => notice(errorText(e), true));
      refreshRuns().catch(() => {});
    }
    scheduleDraw();
  }
  function renderQueue() {
    const running = S.runs.filter(r => ['running','checking','awaiting_approval'].includes(r.status)); const queued = S.runs.filter(r => r.status === 'queued');
    const label = running.length || queued.length ? `${running.length} running${queued.length ? ` · ${queued.length} queued` : ''}` : 'Queue is clear';
    $('queue-summary').replaceChildren(el('span', { class: `status-dot${running.length || queued.length ? ' busy' : ''}` }), el('span', {}, label), el('span', {}, `${S.settings?.agent_slots || 1} slot${S.settings?.agent_slots === 1 ? '' : 's'}`));
  }
  function showRuns() {
    const body = el('div', { class: 'run-list' });
    const m = modal('Agent runs', body, [button('Close', () => m.close())], true);
    const draw = () => {
      body.replaceChildren(...(S.runs.length ? S.runs.slice(0, 80).map(run => {
        const project = S.projects.find(p => p.id === run.project_id);
        const row = el('article', { class: 'run-card' }, el('div', { class: 'card-title' }, el('strong', {}, project?.title || 'Project run'), el('span', { class: `state-badge ${run.status}` }, readableStatus(run.status))), el('p', { class: 'small muted' }, `${new Date(run.created_at).toLocaleString()} · ${run.id.slice(0, 8)}`));
        if (run.error) row.append(el('p', { class: 'error-text' }, typeof run.error === 'string' ? run.error : JSON.stringify(run.error)));
        if (run.metrics && Object.keys(run.metrics).length) row.append(metricsNode(run.metrics));
        const actions = el('div', { class: 'action-row' });
        if (active(run)) actions.append(button('Stop run', () => attempt(async () => { await api(`/runs/${encodeURIComponent(run.id)}/cancel`, { method: 'POST' }); await refreshRuns(); draw(); }), 'button danger'));
        else if (architectRun(run)) actions.append(button('Continue with architect', () => attempt(async () => {
          const next = await api(`/runs/${encodeURIComponent(run.id)}/retry`, { method: 'POST' });
          S.runs = [next, ...S.runs.filter(r => r.id !== next.id)]; connectRun(next); renderQueue(); renderProjects();
          m.close(); if (project) await selectProject(project.id);
        }), 'button'));
        if (project) actions.append(button('Open project', () => { m.close(); attempt(() => selectProject(project.id)); }));
        row.append(actions); return row;
      }) : [el('div', { class: 'empty-panel' }, 'Runs appear here after you send a prompt.')]));
    };
    draw();
  }
  async function refreshRuns() {
    if (!S.user || $('app-shell').hidden) return;
    const result = await api('/runs'); const runs = Array.isArray(result) ? result : result.runs || [];
    const old = new Map(S.runs.map(r => [r.id, r])); S.runs = runs;
    for (const run of runs) {
      if (active(run) && architectRun(run)) connectRun(run);
      else { const stream = S.streams.get(run.id); if (stream) { stream.close(); S.streams.delete(run.id); } if (active(old.get(run.id)) && run.project_id === S.project?.id && architectRun(run)) await refreshMessages(); }
    }
    renderQueue(); renderProjects(); renderComposer(); renderProjectProgress(); if (S.tab === 'team') renderTeam();
  }

  function searchConversation() {
    if (!S.project) return;
    const projectId = S.project.id;
    const input = el('input', { type: 'search', required: true, placeholder: 'Search saved messages…', 'aria-label': 'Search saved messages' });
    const search = button('Search', () => form.requestSubmit(), 'button primary');
    const form = el('form', { class: 'conversation-search-form' }, input, search);
    const results = el('div', { class: 'conversation-search-results' });
    const m = modal('Search saved conversation', el('div', {}, form, el('p', { class: 'small muted' }, 'Searches the saved conversation on this host. Live text appears after it has been saved.'), results), [button('Close', () => m.close())], true);
    form.onsubmit = async event => {
      event.preventDefault(); search.disabled = true; m.status('Searching…');
      try {
        const query = input.value.trim();
        const response = await api(`/projects/${encodeURIComponent(projectId)}/search?q=${encodeURIComponent(query)}&limit=100`);
        const matches = (Array.isArray(response) ? response : response.messages || response.results || []).filter(architectMessage);
        results.replaceChildren(...matches.map(message => {
          const content = String(message.content || ''); const index = content.toLowerCase().indexOf(query.toLowerCase()); const start = Math.max(0, index - 90); const end = Math.min(content.length, Math.max(index, 0) + query.length + 180);
          const preview = `${start ? '…' : ''}${content.slice(start, end)}${end < content.length ? '…' : ''}`;
          return button([el('span', { class: 'search-result-meta' }, `${message.role === 'user' ? 'You' : message.role === 'assistant' ? 'HarnessyXOR' : readableStatus(message.role)} · ${new Date(message.created_at).toLocaleString()}`), el('span', { class: 'search-result-text' }, preview)], () => attempt(async () => { m.close(); if (S.project?.id !== projectId) await selectProject(projectId); await switchTab('chat'); $(`message-${message.id}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' }); }), 'search-result');
        }));
        if (!matches.length) results.append(el('div', { class: 'empty-panel' }, 'No saved messages matched.'));
        m.status(`${matches.length} result${matches.length === 1 ? '' : 's'}${matches.length === 100 ? ' · first 100 shown' : ''}`);
      } catch (e) { m.status(errorText(e)); }
      finally { search.disabled = false; }
    };
    input.focus();
  }

  function openSettings() {
    if (!S.settings) return;
    const draft = JSON.parse(JSON.stringify(S.settings)); let selected = draft.profiles.find(p => p.id === draft.active_profile_id)?.id || draft.profiles[0]?.id; let editorProfile = null; let editorFields = {};
    const profileSelect = el('select', { 'aria-label': 'Profile to edit' });
    const profileFields = el('div', { class: 'form-grid two' });
    const readProfile = () => {
      if (!editorProfile) return;
      const f = editorFields;
      editorProfile.name = f.name.value.trim(); editorProfile.base_url = f.base_url.value.trim(); editorProfile.model = f.model.value.trim(); editorProfile.api_key = f.api_key.value; editorProfile.api_mode = f.api_mode.value;
      editorProfile.context_window = Number(f.context_window.value); editorProfile.max_output_tokens = Number(f.max_output_tokens.value); editorProfile.temperature = f.temperature.value === '' ? null : Number(f.temperature.value); editorProfile.supports_usage_stream = f.supports_usage_stream.checked; editorProfile.send_tool_choice = f.send_tool_choice.checked;
    };
    const renderProfile = () => {
      profileSelect.replaceChildren(...draft.profiles.map(p => el('option', { value: p.id, selected: p.id === selected }, p.name || p.id)));
      editorProfile = draft.profiles.find(p => p.id === selected); profileFields.replaceChildren(); if (!editorProfile) return;
      defaultProfile.checked = selected === draft.active_profile_id;
      const p = editorProfile;
      const f = editorFields = {
        name: el('input', { value: p.name, required: true, maxlength: 120 }), base_url: el('input', { value: p.base_url, required: true, type: 'url', placeholder: 'http://127.0.0.1:8080/v1' }), model: el('input', { value: p.model, placeholder: 'Model ID from your server', maxlength: 500 }),
        api_key: el('input', { type: 'password', value: p.api_key || '', placeholder: p.api_key_set ? 'Saved key — leave unchanged to keep it' : 'Optional', autocomplete: 'off', spellcheck: false }),
        api_mode: el('select', {}, el('option', { value: 'openai', selected: p.api_mode !== 'ollama' }, 'OpenAI-compatible'), el('option', { value: 'ollama', selected: p.api_mode === 'ollama' }, 'Ollama')),
        context_window: el('input', { type: 'number', min: 1024, max: 2000000, step: 1, value: p.context_window || 262144, required: true }),
        max_output_tokens: el('input', { type: 'number', min: 1, max: 1000000, step: 1, value: p.max_output_tokens || 32768, required: true }),
        temperature: el('input', { type: 'number', min: 0, max: 5, step: 0.05, value: p.temperature ?? '', placeholder: 'Server default' }),
        supports_usage_stream: el('input', { type: 'checkbox', checked: p.supports_usage_stream }), send_tool_choice: el('input', { type: 'checkbox', checked: p.send_tool_choice })
      };
      const models = button('Fetch model IDs', () => attempt(async () => { models.disabled = true; try { const result = await api(`/models?profile_id=${encodeURIComponent(p.id)}`); const list = result.data || result.models || result; const items = Array.isArray(list) ? list.map(item => typeof item === 'string' ? item : item.id || item.name).filter(Boolean) : []; const datalist = el('datalist', { id: 'model-id-options' }, items.map(name => el('option', { value: name }))); profileFields.querySelector('datalist')?.remove(); profileFields.append(datalist); f.model.setAttribute('list', 'model-id-options'); m.status(items.length ? `${items.length} models found. Choose an ID in the Model field.` : 'The endpoint returned no model IDs.'); } finally { models.disabled = false; } }), 'button');
      const clearKey = button('Clear saved key', () => { f.api_key.value = ''; p.api_key = ''; p.api_key_set = false; clearKey.disabled = true; m.status('The saved key will be removed when you save settings.'); }, 'text-button clear-key', { disabled: !p.api_key && !p.api_key_set });
      const keyField = el('div', { class: 'form-grid key-field' }, field('API key', f.api_key, 'The saved value stays hidden. Enter a replacement or use Clear saved key. Changing the endpoint requires a new key or clearing the saved key.'), clearKey);
      profileFields.append(field('Profile name', f.name), field('API format', f.api_mode), field('Base URL', f.base_url, 'The endpoint runs on this host or your network.'), keyField, field('Model', f.model), el('div', { class: 'action-row' }, models), field('Context window', f.context_window, 'Total window, including output and safety space.'), field('Maximum output tokens', f.max_output_tokens), field('Temperature', f.temperature), el('div', { class: 'form-grid' }, el('label', { class: 'field-check' }, f.supports_usage_stream, 'Request usage in streamed replies'), el('label', { class: 'field-check' }, f.send_tool_choice, 'Send tool_choice: auto')));
    };
    profileSelect.onchange = () => { readProfile(); selected = profileSelect.value; renderProfile(); };
    const add = button('+ Add profile', () => { readProfile(); selected = `profile-${Date.now()}`; draft.profiles.push({ ...draft.profiles[0], id: selected, name: 'New model', model: '', api_key: '', api_key_set: false }); renderProfile(); });
    const remove = button('Remove', () => { if (draft.profiles.length <= 1) { m.status('Keep at least one model profile.'); return; } draft.profiles = draft.profiles.filter(p => p.id !== selected); selected = draft.profiles[0].id; if (!draft.profiles.some(p => p.id === draft.active_profile_id)) draft.active_profile_id = selected; renderProfile(); });
    const slots = el('input', { type: 'number', min: 1, max: 64, step: 1, value: draft.agent_slots, required: true });
    const steps = el('input', { type: 'number', min: 1, max: 10000, step: 1, value: draft.max_steps, required: true });
    const completion = el('input', { type: 'checkbox', checked: draft.completion_check_enabled });
    const instructions = el('textarea', { value: draft.system_instructions, spellcheck: false });
    const permissionInputs = {};
    const permissions = el('div', { class: 'permissions-grid' }, Object.entries(draft.tool_permissions || {}).map(([name, value]) => { const input = permissionInputs[name] = el('select', {}, ['on', 'ask', 'off'].map(v => el('option', { value: v, selected: v === value }, ({ on: 'Allow', ask: 'Ask', off: 'Off' })[v]))); return field(name.replaceAll('_', ' '), input); }));
    const defaultProfile = el('input', { type: 'checkbox', checked: selected === draft.active_profile_id });
    const content = el('form', {},
      el('section', { class: 'settings-section' }, el('h3', {}, 'Models'), el('div', { class: 'profile-tabs' }, profileSelect, add, remove), profileFields, el('label', { class: 'field-check profile-default' }, defaultProfile, 'Use this profile by default')),
      el('section', { class: 'settings-section' }, el('h3', {}, 'Agent'), el('div', { class: 'form-grid two' }, field('Global agent slots', slots, 'One shared limit for active project runs.'), field('Maximum steps per run', steps, 'A run stops at this guardrail.'), el('label', { class: 'field-check span-two' }, completion, 'Check completion before finishing a run'), el('div', { class: 'span-two' }, field('Global instructions', instructions)))),
      el('section', { class: 'settings-section' }, el('h3', {}, 'Tool permissions'), permissions),
      el('section', { class: 'settings-section' }, el('h3', {}, 'Diagnostics'), el('p', { class: 'small muted' }, 'Download fixed operational observations. Conversations, project files, model payloads and credentials are excluded.'), button('Download diagnostics', () => download('/diagnostics', 'HarnessyXOR-Diagnostics.json')))
    );
    const save = button('Save settings', () => content.requestSubmit(), 'button primary');
    const m = modal('Settings', content, [button('Cancel', () => m.close()), save], true);
    defaultProfile.onchange = () => { if (defaultProfile.checked) draft.active_profile_id = selected; };
    content.onsubmit = e => { e.preventDefault(); readProfile(); formAction(m, save, async () => { draft.agent_slots = Number(slots.value); draft.max_steps = Number(steps.value); draft.completion_check_enabled = completion.checked; draft.system_instructions = instructions.value; for (const [name, input] of Object.entries(permissionInputs)) draft.tool_permissions[name] = input.value; if (defaultProfile.checked) draft.active_profile_id = selected; S.settings = await api('/settings', { method: 'PUT', body: draft }); renderModels(); renderQueue(); m.close(); }); };
    renderProfile();
  }

  async function loadFiles() {
    if (!S.project) return; const id = S.project.id; const path = S.filePath;
    const response = await api(`/projects/${encodeURIComponent(id)}/files?path=${encodeURIComponent(path)}`);
    if (S.project?.id !== id || S.tab !== 'files') return;
    S.files = response.entries || []; S.filesReadonly = Boolean(response.read_only); S.selectedFiles.clear(); renderFiles(S.filesReadonly);
  }
  function renderFiles(serverReadonly = false) {
    const readonly = serverReadonly || S.project?.locked || S.project?.archived;
    const view = $('files-view');
    const pathParts = S.filePath.split('/').filter(Boolean);
    const breadcrumbs = el('div', { class: 'file-breadcrumbs' }, button('Project files', () => attempt(async () => { S.filePath = ''; await loadFiles(); }), 'text-button'), pathParts.map((part, i) => [el('span', {}, '/'), button(part, () => attempt(async () => { S.filePath = pathParts.slice(0, i + 1).join('/'); await loadFiles(); }), 'text-button')]));
    const uploadInput = el('input', { type: 'file', multiple: true, hidden: true });
    uploadInput.onchange = () => attempt(async () => {
      const files = [...uploadInput.files]; const projectId = S.project.id; const base = S.filePath;
      for (const file of files) await api(`/projects/${encodeURIComponent(projectId)}/upload?path=${encodeURIComponent([base, file.name].filter(Boolean).join('/'))}`, { method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: file });
      if (S.project?.id === projectId) await loadFiles();
    });
    const zip = button('Pack as ZIP', () => packFiles(), 'button', { disabled: readonly });
    const actions = el('div', { class: 'action-row' }, button('New file', () => newFile(), 'button', { disabled: readonly }), button('New folder', () => newDirectory(), 'button', { disabled: readonly }), button('Upload', () => uploadInput.click(), 'button', { disabled: readonly }), zip, button('Trash', () => attempt(openTrash)), iconButton('↻', 'Refresh files', () => attempt(loadFiles)));
    const list = el('div', { class: 'file-list' });
    const entries = [...S.files].sort((a, b) => Number(b.is_dir) - Number(a.is_dir) || a.name.localeCompare(b.name));
    for (const entry of entries) {
      const checkbox = el('input', { type: 'checkbox', 'aria-label': `Select ${entry.name}` });
      checkbox.onchange = () => { if (checkbox.checked) S.selectedFiles.add(entry.path); else S.selectedFiles.delete(entry.path); zip.textContent = S.selectedFiles.size ? `ZIP ${S.selectedFiles.size} selected` : 'Pack as ZIP'; };
      const open = button([el('span', { 'aria-hidden': 'true' }, entry.is_dir ? '▱' : '▤'), el('span', {}, entry.name)], () => attempt(async () => { if (entry.is_dir) { S.filePath = entry.path; await loadFiles(); } else await openFile(entry.path, readonly); }), 'file-name');
      const row = el('div', { class: 'file-row' }, checkbox, open, el('span', { class: 'file-size' }, entry.is_dir ? 'Folder' : formatBytes(entry.size)));
      if (!entry.is_dir) row.append(iconButton('↓', `Download ${entry.name}`, () => download(`/projects/${encodeURIComponent(S.project.id)}/download?path=${encodeURIComponent(entry.path)}`, entry.name)));
      if (!readonly) {
        if (!entry.is_dir && /\.zip$/i.test(entry.name)) row.append(iconButton('⇱', `Extract ${entry.name} into a new folder`, () => extractArchive(entry)));
        row.append(iconButton('✎', `Rename ${entry.name}`, () => renameFile(entry)));
        const trash = iconButton('⌫', `Move ${entry.name} to trash`, () => attempt(async () => { trash.disabled = true; try { await api(`/projects/${encodeURIComponent(S.project.id)}/file?path=${encodeURIComponent(entry.path)}`, { method: 'DELETE' }); await loadFiles(); notice(`Moved ${entry.name} to trash. Use Trash to restore it.`); } finally { trash.disabled = false; } }));
        row.append(trash);
      }
      list.append(row);
    }
    view.replaceChildren(el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, 'Project files'), el('p', {}, readonly ? S.project.locked || S.project.archived ? 'This project is read only.' : 'The initial baseline is sealed. Later source changes come through reviewed worker proposals.' : 'Prepare initial source before the first message to the Architect. The first Architect run or finalizing this baseline closes these editing controls.')), actions), breadcrumbs, uploadInput, entries.length ? list : el('div', { class: 'empty-panel' }, el('h3', {}, 'A little room to build'), 'Upload a file or create one to start working here.'));
  }
  function newDirectory() {
    const input = el('input', { required: true, placeholder: 'new-folder' }); const form = el('form', { class: 'form-grid' }, field('Folder name', input));
    const save = button('Create folder', () => form.requestSubmit(), 'button primary'); const m = modal('New folder', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(`/projects/${encodeURIComponent(S.project.id)}/directories`, { method: 'POST', body: { path: [S.filePath, input.value.trim()].filter(Boolean).join('/') } }); m.close(); await loadFiles(); }); }; input.focus();
  }
  function newFile() {
    const input = el('input', { required: true, placeholder: 'notes.txt' }); const form = el('form', { class: 'form-grid' }, field('File name', input));
    const save = button('Create file', () => form.requestSubmit(), 'button primary'); const m = modal('New file', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const path = [S.filePath, input.value.trim()].filter(Boolean).join('/'); if (S.files.some(f => f.path === path)) throw new Error('A file with this name already exists.'); await api(`/projects/${encodeURIComponent(S.project.id)}/file?path=${encodeURIComponent(path)}`, { method: 'PUT', body: { content: '' } }); m.close(); await loadFiles(); await openFile(path, false); }); }; input.focus();
  }
  function renameFile(entry) {
    const input = el('input', { required: true, value: entry.path }); const form = el('form', { class: 'form-grid' }, field('New project-relative path', input));
    const save = button('Rename', () => form.requestSubmit(), 'button primary'); const m = modal('Rename file or folder', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(`/projects/${encodeURIComponent(S.project.id)}/rename`, { method: 'POST', body: { path: entry.path, new_path: input.value.trim() } }); m.close(); await loadFiles(); }); };
  }
  async function packFiles() {
    const selected = S.selectedFiles.size ? [...S.selectedFiles] : S.files.map(f => f.path);
    if (!selected.length) return notice('Add files before creating an archive.');
    const input = el('input', { value: `Project-${Date.now()}.zip`, required: true, pattern: '.*\\.zip' });
    const form = el('form', { class: 'form-grid' }, el('p', { class: 'small muted' }, `${selected.length} selected file${selected.length === 1 ? '' : 's'} or folder${selected.length === 1 ? '' : 's'}. The completed ZIP appears in Project files.`), field('ZIP filename', input));
    const save = button('Create ZIP', () => form.requestSubmit(), 'button primary'); const m = modal('Pack as ZIP', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const result = await api(`/projects/${encodeURIComponent(S.project.id)}/zip`, { method: 'POST', body: { paths: selected, output: [S.filePath, input.value.trim()].filter(Boolean).join('/') } }); m.close(); await loadFiles(); notice(`Created ${result.path} · ${formatBytes(result.bytes)}${result.excluded_count ? ` · ${result.excluded_count} protected items excluded` : ''}. Download it from the file row.`); }); };
  }
  async function openFile(path, readonly) {
    const projectId = S.project.id;
    const result = await api(`/projects/${encodeURIComponent(projectId)}/file?path=${encodeURIComponent(path)}`);
    const editor = el('textarea', { class: 'file-editor', value: result.content || '', spellcheck: false, 'aria-label': `Edit ${path}`, readonly: readonly ? '' : null });
    if (readonly) editor.readOnly = true;
    const status = el('span', { class: 'small muted' });
    const container = el('div', { class: 'editor-container', role: 'dialog', 'aria-modal': 'true', 'aria-label': path });
    const close = () => { if (editor.value !== (result.content || '') && !confirm('Close this file without saving your changes?')) return; container.remove(); S.editor = null; };
    const save = button('Save', () => attempt(async () => { save.disabled = true; status.textContent = 'Saving…'; try { await api(`/projects/${encodeURIComponent(projectId)}/file?path=${encodeURIComponent(path)}`, { method: 'PUT', body: { content: editor.value } }); result.content = editor.value; status.textContent = 'Saved'; } catch (e) { status.textContent = errorText(e); } finally { save.disabled = false; } }), 'button primary', { disabled: readonly });
    container.append(el('div', { class: 'editor-header' }, el('strong', {}, path), el('div', { class: 'action-row' }, status, button('Download', () => download(`/projects/${encodeURIComponent(projectId)}/download?path=${encodeURIComponent(path)}`, path.split('/').pop())), save, iconButton('×', 'Close editor', close))), editor);
    editor.onkeydown = e => { if ((e.ctrlKey || e.metaKey) && e.key === 's') { e.preventDefault(); if (!readonly) save.click(); } if (e.key === 'Escape') close(); };
    S.editor?.remove(); S.editor = container; document.body.append(container); editor.focus();
  }

  async function openTrash() {
    const projectId = S.project.id;
    const response = await api(`/projects/${encodeURIComponent(projectId)}/trash`);
    const entries = response.entries || [];
    const readonly = response.read_only || S.filesReadonly || S.project.locked || S.project.archived;
    const list = el('div', { class: 'trash-list' }, ...entries.map(entry => el('div', { class: 'trash-row' }, el('div', {}, el('strong', { class: 'small' }, entry.path), el('p', { class: 'small muted' }, `${entry.is_dir ? 'Folder' : 'File'} · ${new Date(entry.deleted_at).toLocaleString()}`)), button('Restore', () => restoreTrashEntry(projectId, entry), 'button', { disabled: readonly }))));
    const m = modal('Project trash', el('div', {}, el('p', { class: 'small muted' }, 'Files moved here can be restored. Restoring never overwrites an existing file or folder.'), entries.length ? list : el('div', { class: 'empty-panel' }, 'Nothing in this project’s trash.')), [button('Close', () => m.close())], true);
  }
  function restoreTrashEntry(projectId, entry) {
    const input = el('input', { value: entry.path, required: true, spellcheck: false });
    const form = el('form', { class: 'form-grid' }, field('Restore to project-relative path', input, 'Choose a different path if the original location is occupied.'));
    const save = button('Restore', () => form.requestSubmit(), 'button primary');
    const m = modal('Restore from trash', form, [button('Cancel', () => { m.close(); attempt(openTrash); }), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(`/projects/${encodeURIComponent(projectId)}/trash/${encodeURIComponent(entry.id)}/restore`, { method: 'POST', body: { path: input.value.trim() } }); m.close(); await loadFiles(); notice(`Restored ${input.value.trim()}.`); }); };
  }
  function extractArchive(entry) {
    const stem = entry.name.replace(/\.zip$/i, '') || 'extracted';
    const input = el('input', { value: [S.filePath, stem].filter(Boolean).join('/'), required: true, spellcheck: false });
    const form = el('form', { class: 'form-grid' }, el('p', { class: 'small muted' }, `Extract ${entry.name} into a new folder. Existing files are never overwritten.`), field('New project-relative folder', input));
    const save = button('Extract ZIP', () => form.requestSubmit(), 'button primary');
    const m = modal('Extract ZIP', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(`/projects/${encodeURIComponent(S.project.id)}/extract`, { method: 'POST', body: { path: entry.path, destination: input.value.trim() } }); m.close(); await loadFiles(); notice(`Extracted ${entry.name} into ${input.value.trim()}.`); }); };
  }

  async function refreshEnvironmentList() {
    if (!S.workspace || !S.project) return; const projectId = S.project.id;
    const result = await api(`/environments?workspace_id=${encodeURIComponent(S.workspace.id)}`);
    if (S.project?.id !== projectId) return;
    S.environments = Array.isArray(result) ? result : result.environments || [];
  }
  async function loadEnvironments() {
    await refreshEnvironmentList(); if (S.tab !== 'environments') return;
    const grid = el('div', { class: 'environment-grid' });
    const header = el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, 'Environments'), el('p', {}, 'Give this project access to a registered VM.')), el('div', { class: 'action-row' }, button('Register existing Lima VM', () => environmentEditor(), 'button primary'), iconButton('↻', 'Refresh environments', () => attempt(loadEnvironments))));
    for (const env of S.environments) {
      const badge = el('span', { class: 'state-badge' }, 'Checking');
      const card = el('article', { class: `environment-card${env.enabled ? ' enabled' : ''}` });
      const hasAccess = (env.allowed_project_ids || []).includes(S.project.id);
      const actions = el('div', { class: 'action-row' }, button('Start', () => attempt(async () => { badge.textContent = 'Starting'; await api(`/environments/${encodeURIComponent(env.id)}/start`, { method: 'POST' }); await status(); })), button('Stop', () => attempt(async () => { badge.textContent = 'Stopping'; await api(`/environments/${encodeURIComponent(env.id)}/stop`, { method: 'POST' }); await status(); })), button('Settings', () => environmentEditor(env)));
      card.append(el('div', { class: 'card-title' }, el('h3', {}, env.name), badge), el('dl', {}, el('dt', {}, 'Instance'), el('dd', {}, env.instance), el('dt', {}, 'Guest'), el('dd', {}, env.guest_family), el('dt', {}, 'Model access'), el('dd', {}, env.enabled ? hasAccess ? 'Enabled for this project' : 'This project is not allowed' : 'Disabled'), el('dt', {}, 'Guest files'), el('dd', {}, env.guest_project_root || 'Not configured')), actions); grid.append(card);
      const status = async () => { try { const response = await api(`/environments/${encodeURIComponent(env.id)}/status`); badge.textContent = readableStatus(response.status); badge.className = `state-badge ${response.status}`; badge.title = response.message || response.reason || ''; } catch (e) { badge.textContent = 'Unavailable'; badge.className = 'state-badge unavailable'; badge.title = errorText(e); } };
      status();
    }
    $('environments-view').replaceChildren(header, S.environments.length ? grid : el('div', { class: 'empty-panel' }, el('h3', {}, 'Connect a place to run your tools'), 'Register a Lima VM that already exists on this host, then enable model access for this project.'), el('p', { class: 'environment-footnote' }, 'Register and control existing Lima VMs. Automatic provisioning, operating system installation, desktops, snapshots and radio setup are unavailable. Commands require an enabled environment, an explicit project grant and a verified shared folder.'));
  }
  function environmentEditor(existing = null) {
    const env = existing || { name: '', workspace_id: S.workspace.id, instance: '', lima_home: '', guest_family: 'linux', enabled: false, allowed_project_ids: [], host_project_root: '', guest_project_root: '' };
    const fields = {
      name: el('input', { value: env.name, required: true, maxlength: 120 }), instance: el('input', { value: env.instance, required: true, placeholder: 'my-lima-vm', spellcheck: false }),
      guest_family: el('select', {}, ['linux', 'openwrt', 'windows'].map(v => el('option', { value: v, selected: env.guest_family === v }, v === 'openwrt' ? 'OpenWrt' : v === 'windows' ? 'Windows' : 'Linux'))),
      lima_home: el('input', { value: env.lima_home || '', placeholder: 'Default Lima home', spellcheck: false }),
      host_project_root: el('input', { value: env.host_project_root || '', required: true, placeholder: '/absolute/path/to/shared/project', spellcheck: false }),
      guest_project_root: el('input', { value: env.guest_project_root || '', required: true, placeholder: '/absolute/path/inside/guest', spellcheck: false }),
      enabled: el('input', { type: 'checkbox', checked: env.enabled })
    };
    const grants = S.projects.filter(p => !p.archived).map(p => ({ p, input: el('input', { type: 'checkbox', checked: (env.allowed_project_ids || []).includes(p.id) }) }));
    const form = el('form', { class: 'form-grid two' }, field('Name', fields.name), field('Existing Lima instance', fields.instance), field('Guest family', fields.guest_family), field('Lima home', fields.lima_home, 'Optional absolute path. Use the default unless this instance needs a separate Lima home.'), field('Host project folder', fields.host_project_root, 'The project folder already shared with this VM.'), field('Guest project folder', fields.guest_project_root, 'The matching absolute path inside the guest.'), el('label', { class: 'field-check span-two' }, fields.enabled, 'Enable model commands in this environment'), el('div', { class: 'span-two form-grid' }, el('h3', {}, 'Allowed projects'), grants.map(({ p, input }) => el('label', { class: 'field-check' }, input, p.title))), el('p', { class: 'port-note span-two' }, 'Registration does not create or provision a VM. Start and Stop control the named instance. Windows command execution remains unavailable in this build.'));
    const save = button(existing ? 'Save environment' : 'Register VM', () => form.requestSubmit(), 'button primary');
    const m = modal(existing ? 'Environment settings' : 'Register an existing VM', form, [button('Cancel', () => m.close()), save], true);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const data = { ...env }; for (const [key, input] of Object.entries(fields)) data[key] = key === 'enabled' ? input.checked : input.value.trim(); if (!data.lima_home) data.lima_home = null; data.allowed_project_ids = grants.filter(g => g.input.checked).map(g => g.p.id); await api(existing ? `/environments/${encodeURIComponent(env.id)}` : '/environments', { method: existing ? 'PUT' : 'POST', body: data }); m.close(); await loadEnvironments(); }); };
  }
  const teamRoles = ['developer', 'tester', 'qa', 'code_reviewer', 'architecture_reviewer'];
  const roleLabel = role => ({ architect: 'Principal Architect', developer: 'Developer', tester: 'Tester', qa: 'QA', code_reviewer: 'Code reviewer', architecture_reviewer: 'Architecture reviewer' })[role] || readableStatus(role);
  const statusBadge = status => el('span', { class: `state-badge ${status || 'unknown'}` }, readableStatus(status || 'not recorded'));
  const formSelect = (values, selected) => el('select', {}, values.map(value => el('option', { value: typeof value === 'string' ? value : value.value, selected: (typeof value === 'string' ? value : value.value) === selected }, typeof value === 'string' ? readableStatus(value) : value.label)));
  function mergeTeamEvents(events) {
    const merged = new Map(S.teamEvents.map(event => [event.id || event.sequence, event]));
    for (const event of events) { merged.set(event.id || event.sequence, event); S.eventSequence = Math.max(S.eventSequence, Number(event.sequence) || 0); }
    S.teamEvents = [...merged.values()].sort((a, b) => (Number(a.sequence) || 0) - (Number(b.sequence) || 0)).slice(-200);
  }
  async function refreshTeam() {
    if (!S.project) return;
    const projectId = S.project.id; const team = await api(projectApi('/team'));
    if (S.project?.id !== projectId) return;
    S.team = team; mergeTeamEvents(team.events || []);
    if (typeof team.paused === 'boolean') S.projectPaused = team.paused;
    renderProjectProgress(); renderComposer();
  }
  async function pollTeamEvents() {
    if (!S.project || !S.team || S.teamPollBusy || $('app-shell').hidden) return;
    S.teamPollBusy = true; const projectId = S.project.id;
    try {
      const result = await api(projectApi(`/events?after=${S.eventSequence}&limit=80`));
      if (S.project?.id !== projectId) return;
      const events = Array.isArray(result) ? result : result.events || [];
      mergeTeamEvents(events);
      if (events.length) {
        await refreshTeam();
        if (S.tab === 'team') { if (S.selectedAgentId) await fetchAgentMessages(S.selectedAgentId); renderTeam(); }
        if (S.tab === 'knowledge') renderKnowledge();
        if (S.tab === 'regression') await loadRegression();
        if (S.tab === 'changes') await loadChanges();
      }
      renderProjectProgress();
    } finally { S.teamPollBusy = false; }
  }
  function eventSummary(event) {
    const data = event.data || {}; const task = S.team?.tasks?.find(task => task.id === data.task_id);
    return data.summary || data.title || task?.title || readableStatus(event.kind || 'Project activity');
  }
  function eventNode(event) {
    const agentId = event.actor_agent_id || event.data?.agent_id;
    const agent = S.team?.agents?.find(agent => agent.id === agentId);
    return button([el('span', { class: 'event-time' }, shortDate(event.created_at)), el('span', { class: 'event-body' }, el('strong', {}, agent?.label || (event.origin === 'user' ? 'You' : 'Controller')), el('span', {}, eventSummary(event))), el('span', { class: 'event-kind' }, readableStatus(event.kind || 'event'))], () => attempt(async () => { await switchTab('team'); if (agentId) await inspectAgent(agentId); }), 'team-event', { title: 'Inspect this activity in Team' });
  }
  function renderProjectProgress() {
    if (!S.project || !S.team) return;
    const runs = S.runs.filter(run => run.project_id === S.project.id && active(run));
    const executing = runs.filter(run => ['running', 'checking', 'awaiting_approval'].includes(run.status)).length;
    const waiting = runs.filter(run => ['delegated', 'blocked', 'paused'].includes(run.status)).length;
    const teammates = (S.team.agents || []).filter(agent => agent.role !== 'architect' && !agent.retired).length;
    $('architect-summary').textContent = `${teammates} teammate${teammates === 1 ? '' : 's'} · ${executing} executing${waiting ? ` · ${waiting} waiting` : ''}`;
    $('activity-count').textContent = S.teamEvents.length ? `${S.teamEvents.length} recent events` : 'No activity yet';
    $('activity-feed').replaceChildren(...(S.teamEvents.length ? S.teamEvents.slice(-5).reverse().map(eventNode) : [el('p', { class: 'small muted activity-empty' }, 'Team tasks and controller activity will appear here.')]));
    $('pause-project').hidden = S.projectPaused; $('resume-project').hidden = !S.projectPaused;
    $('pause-project').disabled = S.project.locked || S.project.archived;
    $('resume-project').disabled = S.project.locked || S.project.archived;
  }
  async function changeProjectPause(paused) {
    await api(projectApi(`/team/${paused ? 'pause' : 'resume'}`), { method: 'POST' });
    S.projectPaused = paused; await refreshRuns(); await refreshTeam(); if (S.tab === 'team') renderTeam();
    notice(paused ? 'Project paused. Waiting roles do not occupy an execution slot.' : 'Project resumed. Eligible work can use the global execution slots.');
  }
  function agentRun(agentId) { return S.runs.find(run => run.project_id === S.project?.id && run.agent_id === agentId && active(run)); }
  function agentState(agent) {
    if (agent.retired) return 'retired';
    const run = agentRun(agent.id); if (run) return run.status;
    const tasks = (S.team?.tasks || []).filter(task => task.assigned_agent_id === agent.id).sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || ''));
    return tasks.find(task => ['queued', 'blocked', 'running'].includes(task.status))?.status || 'idle';
  }
  async function fetchAgentMessages(agentId) {
    const projectId = S.project.id; const messages = await api(projectApi(`/agents/${encodeURIComponent(agentId)}/messages`));
    if (S.project?.id !== projectId) return;
    if (!S.agentMessages) S.agentMessages = new Map(); S.agentMessages.set(agentId, Array.isArray(messages) ? messages : messages.messages || []);
  }
  async function inspectAgent(agentId) {
    S.selectedAgentId = agentId; await fetchAgentMessages(agentId); if (S.tab !== 'team') await switchTab('team'); else renderTeam();
    $('agent-detail')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
  function readOnlyMessage(message, agent) {
    const title = message.role === 'tool' ? message.metadata?.name || 'Tool result' : message.role === 'user' ? 'Task / controller' : message.role === 'event' ? 'Controller event' : agent.label;
    const node = el('article', { class: `message ${message.role}` }, el('div', { class: 'message-header' }, el('span', { class: 'role-name' }, title), el('time', { class: 'message-time' }, shortDate(message.created_at))));
    if (message.metadata?.reasoning) node.append(el('details', { class: 'thinking-detail' }, el('summary', {}, 'Thinking'), el('pre', {}, message.metadata.reasoning)));
    if (message.role === 'tool') node.append(el('details', { class: 'tool-detail' }, el('summary', {}, 'Tool result'), el('pre', {}, message.content || '')));
    else node.append(el('div', { class: 'message-content' }, renderText(message.content || '')));
    return node;
  }
  function agentApproval(agent) {
    const run = agentRun(agent.id); const approval = run?.approval; if (!approval) return null;
    return el('div', { class: 'approval-panel' }, el('strong', {}, `Tool permission requested · ${approval.tool || approval.name || 'Tool'}`), el('details', { class: 'tool-detail' }, el('summary', {}, 'Exact request'), el('pre', {}, JSON.stringify(approval.arguments || approval, null, 2))), el('div', { class: 'approval-actions' }, button('Allow this request', () => attempt(async () => { await decideApproval(run.id, approval.id || approval.approval_id, true); await refreshRuns(); renderTeam(); }), 'button primary'), button('Deny', () => attempt(async () => { await decideApproval(run.id, approval.id || approval.approval_id, false); await refreshRuns(); renderTeam(); }), 'button')));
  }
  function renderTeam() {
    if (!S.team || !S.project) return;
    const detailScroll = $('agent-detail')?.querySelector('.agent-transcript')?.scrollTop || 0;
    const agents = S.team.agents || []; const tasks = S.team.tasks || []; const readonly = S.project.locked || S.project.archived;
    const cards = el('div', { class: 'team-grid' }, agents.map(agent => {
      const run = agentRun(agent.id); const assigned = tasks.filter(task => task.assigned_agent_id === agent.id);
      return el('article', { class: `team-agent-card${agent.role === 'architect' ? ' principal' : ''}${S.selectedAgentId === agent.id ? ' selected' : ''}` }, el('div', { class: 'card-title' }, el('span', { class: 'role-tag' }, roleLabel(agent.role)), statusBadge(agentState(agent))), el('h3', {}, agent.label), el('p', { class: 'small muted' }, agent.instructions || (agent.role === 'architect' ? 'Coordinates the project and speaks with you here.' : 'No additional role instructions saved.')), el('p', { class: 'small muted' }, `${assigned.length} task${assigned.length === 1 ? '' : 's'} · ${assigned.filter(task => task.status === 'completed').length} completed${agent.replaces_agent_id ? ' · replacement role' : ''}`), el('div', { class: 'action-row' }, button('Inspect activity', () => attempt(() => inspectAgent(agent.id))), run ? button('Stop this agent', () => attempt(async () => { await api(projectApi(`/agents/${encodeURIComponent(agent.id)}/stop`), { method: 'POST' }); await refreshRuns(); await refreshTeam(); renderTeam(); }), 'button danger') : null, agent.role !== 'architect' && !agent.retired ? button('Assign task', () => taskEditor(agent.id), 'button', { disabled: readonly }) : null, agent.role !== 'architect' && !agent.retired ? button('Retire', () => retireAgent(agent), 'button', { disabled: readonly }) : null));
    }));
    const taskRows = tasks.map(task => {
      const agent = agents.find(agent => agent.id === task.assigned_agent_id);
      return el('div', { class: 'task-row' }, el('div', {}, el('strong', {}, task.title), el('p', { class: 'small muted' }, `${agent?.label || 'Unassigned'} · revision ${task.revision}${task.dependencies?.length ? ` · ${task.dependencies.length} dependencies` : ''}`), task.summary ? el('p', { class: 'small' }, task.summary) : null), statusBadge(task.status), button('Details', () => taskDetails(task), 'text-button'));
    });
    const selected = agents.find(agent => agent.id === S.selectedAgentId);
    const detail = selected ? el('section', { class: 'team-detail', id: 'agent-detail' }, el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, selected.label), el('p', {}, `${roleLabel(selected.role)} · read-only activity and transcript`)), button('Refresh transcript', () => attempt(async () => { await fetchAgentMessages(selected.id); renderTeam(); }))), el('div', { class: 'readonly-note' }, selected.role === 'architect' ? 'Talk with this role in the Architect tab.' : 'Teammates receive structured tasks from the project controller. Send project requests to the Principal Architect.'), agentApproval(selected), el('div', { class: 'agent-transcript' }, ...(S.agentMessages?.get(selected.id) || []).map(message => readOnlyMessage(message, selected))), !(S.agentMessages?.get(selected.id) || []).length ? el('p', { class: 'small muted' }, 'This agent has no saved transcript yet.') : null) : null;
    $('team-view').replaceChildren(el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, 'Project team'), el('p', {}, 'One Principal Architect, with teammates working inside this project.')), el('div', { class: 'action-row' }, button('Add teammate', addAgent, 'button primary', { disabled: readonly }), button(S.projectPaused ? 'Resume project' : 'Pause project', () => attempt(() => changeProjectPause(!S.projectPaused))), iconButton('↻', 'Refresh team', () => attempt(async () => { await refreshTeam(); renderTeam(); })))), el('div', { class: 'team-policy-note' }, 'Execution slots limit model work. Delegated, blocked and paused roles retain their state without occupying a slot.'), cards, el('section', { class: 'team-section' }, el('div', { class: 'section-heading' }, el('h3', {}, 'Tasks'), button('Assign structured task', () => taskEditor(), 'button', { disabled: readonly || !agents.some(agent => agent.role !== 'architect' && !agent.retired) })), taskRows.length ? el('div', { class: 'task-list' }, taskRows) : el('div', { class: 'empty-panel' }, 'The architect can delegate tasks as the project needs them.')), detail, el('section', { class: 'team-section' }, el('h3', {}, 'Recent controller activity'), S.teamEvents.length ? el('div', { class: 'team-event-list' }, S.teamEvents.slice(-50).reverse().map(eventNode)) : el('p', { class: 'small muted' }, 'No team activity has been recorded.')));
    if ($('agent-detail')?.querySelector('.agent-transcript')) $('agent-detail').querySelector('.agent-transcript').scrollTop = detailScroll;
  }
  function addAgent() {
    const role = formSelect(teamRoles, 'developer'); const label = el('input', { required: true, maxlength: 160, placeholder: 'A clear name for this role' }); const instructions = el('textarea', { required: true, placeholder: 'Responsibilities and boundaries for this teammate.' });
    const form = el('form', { class: 'form-grid' }, field('Role', role), field('Label', label), field('Role instructions', instructions), help('This teammate shares the current project and receives structured tasks from its controller.'));
    const save = button('Add teammate', () => form.requestSubmit(), 'button primary'); const m = modal('Add a teammate', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const agent = await api(projectApi('/agents'), { method: 'POST', body: { role: role.value, label: label.value.trim(), instructions: instructions.value.trim() } }); m.close(); await refreshTeam(); S.selectedAgentId = agent.id; renderTeam(); }); };
  }
  function taskEditor(agentId = null) {
    const agents = (S.team?.agents || []).filter(agent => agent.role !== 'architect' && !agent.retired);
    const assigned = formSelect(agents.map(agent => ({ value: agent.id, label: `${agent.label} · ${roleLabel(agent.role)}` })), agentId || agents[0]?.id);
    const title = el('input', { required: true, maxlength: 200 }); const instructions = el('textarea', { required: true, placeholder: 'The concrete work and the result this task must return.' });
    const dependencies = (S.team?.tasks || []).map(task => ({ task, input: el('input', { type: 'checkbox' }) }));
    const form = el('form', { class: 'form-grid' }, field('Assign to', assigned), field('Task title', title), field('Task instructions', instructions), el('div', { class: 'form-grid' }, el('strong', { class: 'small' }, 'Dependencies'), dependencies.length ? dependencies.map(({ task, input }) => el('label', { class: 'field-check' }, input, `${task.title} (${task.status})`)) : help('No earlier tasks to depend on.')), help('The controller queues eligible work. A dependency must finish before this task can execute.'));
    const save = button('Assign task', () => form.requestSubmit(), 'button primary'); const m = modal('Assign a structured task', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(projectApi('/tasks'), { method: 'POST', body: { assigned_agent_id: assigned.value, title: title.value.trim(), instructions: instructions.value.trim(), dependencies: dependencies.filter(item => item.input.checked).map(item => item.task.id) } }); m.close(); await refreshTeam(); await refreshRuns(); renderTeam(); }); };
  }
  function taskDetails(task) {
    const body = el('div', { class: 'form-grid' }, el('div', { class: 'action-row' }, statusBadge(task.status), el('span', { class: 'small muted' }, `Revision ${task.revision}`)), el('div', { class: 'message-content' }, renderText(task.instructions || '')), task.summary ? el('div', {}, el('h3', {}, 'Recorded result'), el('p', {}, task.summary)) : null, el('details', { class: 'tool-detail' }, el('summary', {}, 'Task record'), el('pre', {}, JSON.stringify(task, null, 2))));
    const m = modal(task.title, body, [button('Close', () => m.close())], true);
  }
  function retireAgent(agent) {
    const reason = el('textarea', { required: true, placeholder: 'Why is this role being retired?' }); const replacement = el('input', { type: 'checkbox' }); const label = el('input', { placeholder: 'Replacement label' }); const instructions = el('textarea', { placeholder: 'Replacement role instructions' });
    const form = el('form', { class: 'form-grid' }, field('Reason', reason), el('label', { class: 'field-check' }, replacement, 'Create a replacement in the same role'), field('Replacement label', label), field('Replacement instructions', instructions), help('Saved transcripts and task history remain attached to the retired role.'));
    const save = button('Retire teammate', () => form.requestSubmit(), 'button danger'); const m = modal(`Retire ${agent.label}`, form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(projectApi(`/agents/${encodeURIComponent(agent.id)}/retire`), { method: 'POST', body: { reason: reason.value.trim(), replacement: replacement.checked ? { label: label.value.trim(), instructions: instructions.value.trim() } : null } }); m.close(); await refreshTeam(); renderTeam(); }); };
  }
  async function workspaceConfiguration() {
    if (!S.workspace) return createWorkspace();
    const workspaceId = S.workspace.id; const current = await api(`/workspaces/${encodeURIComponent(workspaceId)}/settings`);
    const editor = el('textarea', { class: 'configuration-editor', spellcheck: false, value: JSON.stringify(current.settings || {}, null, 2) });
    const form = el('form', { class: 'form-grid' }, el('p', { class: 'small muted' }, 'Configuration is scoped to this workspace. Model credentials and the shared execution-slot limit belong in Models & runtime.'), field('Workspace configuration JSON', editor), el('p', { class: 'form-hint' }, 'Supported overrides: instructions, default_profile_id and tool_permissions. Workspace permissions can tighten the global policy. Model credentials stay in Models & runtime.'));
    const save = button('Save configuration', () => form.requestSubmit(), 'button primary'); const m = modal(`${S.workspace.name} · configuration`, form, [button('Cancel', () => m.close()), save], true);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const value = JSON.parse(editor.value); if (!value || Array.isArray(value) || typeof value !== 'object') throw new Error('Workspace configuration must be a JSON object.'); await api(`/workspaces/${encodeURIComponent(workspaceId)}/settings`, { method: 'PUT', body: { settings: value, expected_revision: current.revision } }); S.workspaceConfiguration = value; if (!currentRun()) { S.profileUserSelected = false; renderModels(); } m.close(); }); };
  }
  function latestRecords(records, key) {
    const groups = new Map();
    for (const record of records || []) { const id = record[key] || record.id; if (!groups.has(id)) groups.set(id, []); groups.get(id).push(record); }
    return [...groups.values()].map(history => { history.sort((a, b) => Number(b.version) - Number(a.version)); return { current: history[0], history }; }).sort((a, b) => (b.current.created_at || '').localeCompare(a.current.created_at || ''));
  }
  function knowledgeCard(group, kind) {
    const record = group.current; const readonly = S.project.locked || S.project.archived;
    const actor = (S.team?.agents || []).find(agent => agent.id === record.actor_agent_id);
    const history = el('details', { class: 'knowledge-history' }, el('summary', {}, `Preserved history · ${group.history.length} version${group.history.length === 1 ? '' : 's'}`), group.history.map(version => el('div', { class: 'knowledge-version' }, el('div', { class: 'action-row' }, el('strong', { class: 'small' }, `Version ${version.version}`), statusBadge(version.status), el('time', { class: 'small muted' }, new Date(version.created_at).toLocaleString())), el('p', { class: 'small' }, version.content), el('span', { class: 'small muted' }, version.origin === 'user' ? 'Recorded by you' : 'Recorded by the project controller'))));
    return el('article', { class: 'knowledge-card' }, el('div', { class: 'card-title' }, el('h3', {}, record.title), statusBadge(record.status)), el('div', { class: 'message-content' }, renderText(record.content || '')), el('div', { class: 'record-meta' }, `${kind === 'requirement' ? 'Requirement' : readableStatus(record.kind || kind)} · v${record.version} · ${actor?.label || (record.origin === 'user' ? 'You' : 'Controller')}`), el('div', { class: 'action-row' }, button('Record revision', () => knowledgeEditor(kind, record), 'button', { disabled: readonly })), history);
  }
  function renderKnowledge() {
    if (!S.team || !S.project) return;
    const requirements = latestRecords(S.team.requirements, 'requirement_id'); const decisions = latestRecords(S.team.decisions, 'decision_id'); const readonly = S.project.locked || S.project.archived;
    const section = (title, groups, kind, empty) => el('section', { class: 'knowledge-section' }, el('h3', { class: 'section-title' }, title), groups.length ? el('div', { class: 'knowledge-grid' }, groups.map(group => knowledgeCard(group, kind))) : el('div', { class: 'empty-panel' }, empty));
    $('knowledge-view').replaceChildren(el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, 'Project knowledge'), el('p', {}, 'Objectives, requirements and decisions with their full revision history.')), el('div', { class: 'action-row' }, button('Add objective', () => knowledgeEditor('objective'), 'button', { disabled: readonly }), button('Add requirement', () => knowledgeEditor('requirement'), 'button primary', { disabled: readonly }), button('Record architecture', () => knowledgeEditor('architecture'), 'button', { disabled: readonly }), button('Record decision', () => knowledgeEditor('decision'), 'button', { disabled: readonly }))), section('Objectives', decisions.filter(group => group.current.kind === 'objective'), 'objective', 'Record what this project should achieve.'), section('Requirements', requirements, 'requirement', 'Requirements give the team and the acceptance gate a shared target.'), section('Architecture', decisions.filter(group => group.current.kind === 'architecture'), 'architecture', 'Architecture records explain the structure and its constraints.'), section('Decisions', decisions.filter(group => !['objective', 'architecture'].includes(group.current.kind)), 'decision', 'Accepted, rejected and superseded decisions remain available here.'));
  }
  function knowledgeEditor(kind, previous = null) {
    const title = el('input', { value: previous?.title || '', required: true, maxlength: 200 }); const content = el('textarea', { value: previous?.content || '', required: true });
    const statuses = kind === 'requirement' ? ['proposed', 'active', 'retired', 'superseded'] : ['proposed', 'accepted', 'rejected', 'superseded'];
    const status = formSelect(statuses, previous?.status || (kind === 'requirement' ? 'active' : 'proposed'));
    const form = el('form', { class: 'form-grid' }, field('Title', title), field('Content', content), field('Status', status), help('Saving adds a version. Earlier records remain in history. Requirement and decision changes become part of the proposal review context.'));
    const save = button(previous ? 'Save revision' : 'Record '+kind, () => form.requestSubmit(), 'button primary'); const m = modal(`${previous ? 'Revise' : 'Record'} ${kind}`, form, [button('Cancel', () => m.close()), save], true);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => {
      const body = { title: title.value.trim(), content: content.value.trim(), status: status.value, kind: kind === 'requirement' ? 'requirement' : kind };
      if (previous) { body.expected_version = previous.version; body[kind === 'requirement' ? 'requirement_id' : 'decision_id'] = previous[kind === 'requirement' ? 'requirement_id' : 'decision_id']; }
      await api(projectApi(kind === 'requirement' ? '/requirements' : '/decisions'), { method: 'POST', body }); m.close(); await refreshTeam(); renderKnowledge();
    }); };
  }
  async function loadChanges() {
    if (!S.project) return; const projectId = S.project.id; const data = await api(projectApi('/git'));
    if (S.project?.id !== projectId) return; S.changes = data; renderBaselineHint(); if (S.tab === 'changes') renderChanges();
  }
  function renderBaselineHint() { $('baseline-hint').hidden = !S.project || S.changes?.bootstrap_open !== true; }
  function renderChanges() {
    const data = S.changes; if (!data || !S.project) return; const readonly = S.project.locked || S.project.archived;
    const proposals = data.proposals || S.team?.proposals || [];
    const proposalCards = proposals.map(proposal => {
      const agent = S.team?.agents?.find(agent => agent.id === proposal.author_agent_id || agent.id === proposal.agent_id || agent.id === proposal.actor_agent_id);
      return el('article', { class: 'proposal-card' }, el('div', { class: 'card-title' }, el('h3', {}, proposal.title || 'Candidate proposal'), statusBadge(proposal.status || 'proposed')), el('p', { class: 'small muted' }, `${agent?.label || 'Recorded candidate'} · ${shortDate(proposal.created_at)}`), el('dl', { class: 'git-identities' }, el('dt', {}, 'Base'), el('dd', {}, (proposal.base_sha || 'not recorded').slice(0, 12)), el('dt', {}, 'Candidate'), el('dd', {}, (proposal.candidate_sha || proposal.integrated_sha || 'not recorded').slice(0, 12)), el('dt', {}, 'Catalog'), el('dd', {}, `Revision ${proposal.catalog_revision ?? 'not recorded'}`)), button('Inspect reviews & gate', () => attempt(() => proposalDetails(proposal.id)), 'button'));
    });
    const branches = (data.branches || []).map(branch => el('div', { class: 'branch-row' }, el('strong', {}, branch.name), el('code', {}, String(branch.sha || '').slice(0, 12))));
    const log = (data.log || []).map(commit => el('div', { class: 'commit-row' }, el('code', {}, String(commit.sha || '').slice(0, 10)), el('div', {}, el('strong', {}, commit.subject), el('p', { class: 'small muted' }, `${commit.author || ''}${commit.created_at ? ' · '+new Date(commit.created_at).toLocaleString() : ''}`))));
    $('changes-view').replaceChildren(el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, 'Project changes'), el('p', {}, 'Inspect candidate Git changes and the evidence required to accept them.')), el('div', { class: 'action-row' }, button('Current diff', () => attempt(() => showDiff()), 'button', { disabled: !data.initialized }), button('Download committed source ZIP', () => download(projectApi('/git/export.zip'), `${safeFilename(S.project.title)}-committed.zip`), 'button', { disabled: !data.initialized, title: 'Download the immutable committed source. Uncommitted owner edits are excluded.' }), button('Finalize initial baseline (unverified)', commitProject, 'button', { disabled: readonly || !data.initialized || data.bootstrap_open !== true }), button('Update worker branch', mergeAcceptedIntoWorker, 'button', { disabled: readonly }), button('Snapshot worker proposal', createProposal, 'button primary', { disabled: readonly }), iconButton('↻', 'Refresh Git state', () => attempt(loadChanges)))), el('div', { class: 'git-state-strip' }, statusBadge(data.initialized ? data.dirty ? 'uncommitted_changes' : 'clean' : 'not_initialized'), el('span', {}, data.branch || 'No accepted branch recorded'), el('code', {}, (data.head || '').slice(0, 12))), el('section', { class: 'team-section' }, el('h3', {}, 'Candidate proposals'), proposalCards.length ? el('div', { class: 'proposal-grid' }, proposalCards) : el('div', { class: 'empty-panel' }, 'Worker changes become proposals. Code review, architecture review and current-catalog checks stay attached to the exact candidate.')), el('div', { class: 'git-columns' }, el('section', { class: 'team-section' }, el('h3', {}, 'Branches'), branches.length ? el('div', { class: 'branch-list' }, branches) : el('p', { class: 'small muted' }, 'No branches recorded.')), el('section', { class: 'team-section' }, el('h3', {}, 'Accepted commit history'), log.length ? el('div', { class: 'commit-list' }, log) : el('p', { class: 'small muted' }, 'No commits recorded.'))));
  }
  async function showDiff(proposalId = null) {
    const value = await api(projectApi('/git/diff'+(proposalId ? `?proposal_id=${encodeURIComponent(proposalId)}` : '')));
    const m = modal(proposalId ? 'Candidate diff' : 'Current project diff', el('div', {}, el('div', { class: 'small muted' }, `Base ${(value.base_sha || '').slice(0, 12)} · candidate ${(value.candidate_sha || '').slice(0, 12)}`), el('pre', { class: 'diff-view' }, value.diff || 'No textual diff.')), [button('Close', () => m.close())], true);
  }
  function commitProject() {
    const message = el('input', { required: true, maxlength: 300, placeholder: 'Describe the current project-file change' });
    const form = el('form', { class: 'form-grid' }, field('Commit message', message), help('This finalizes the initial unverified source baseline and closes direct editing. It does not create review or test evidence. Later changes require a worker proposal and the acceptance gate.'));
    const save = button('Finalize initial baseline (unverified)', () => form.requestSubmit(), 'button primary'); const m = modal('Finalize the initial baseline', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(projectApi('/git/commit'), { method: 'POST', body: { message: message.value.trim() } }); m.close(); await loadChanges(); }); };
  }
  function createProposal() {
    const agents = (S.team?.agents || []).filter(agent => agent.role !== 'architect' && !agent.retired);
    const agent = formSelect(agents.map(agent => ({ value: agent.id, label: agent.label })), agents[0]?.id); const title = el('input', { required: true, maxlength: 200 });
    const form = el('form', { class: 'form-grid' }, field('Worker', agent), field('Proposal title', title), help('The controller snapshots an actual worker branch. A role without a valid Git branch cannot produce a proposal.'));
    const save = button('Snapshot candidate', () => form.requestSubmit(), 'button primary', { disabled: !agents.length }); const m = modal('Snapshot a worker proposal', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const proposal = await api(projectApi('/git/proposals'), { method: 'POST', body: { agent_id: agent.value, title: title.value.trim() } }); m.close(); await loadChanges(); if (proposal.id) await proposalDetails(proposal.id); }); };
  }
  function mergeAcceptedIntoWorker() {
    const agents = (S.team?.agents || []).filter(agent => ['developer', 'tester'].includes(agent.role) && !agent.retired);
    const agent = formSelect(agents.map(agent => ({ value: agent.id, label: agent.label })), agents[0]?.id);
    const form = el('form', { class: 'form-grid' }, field('Worker branch', agent), help('Merge the current accepted source into this worker branch. Conflicts remain visible for the assigned worker to resolve and commit. This does not accept the worker’s changes.'));
    const save = button('Merge accepted source', () => form.requestSubmit(), 'button primary', { disabled: !agents.length }); const m = modal('Update a worker branch', form, [button('Cancel', () => m.close()), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const result = await api(projectApi('/git/merge-main'), { method: 'POST', body: { agent_id: agent.value } }); const body = el('div', { class: 'form-grid' }, statusBadge(result.status), el('p', {}, result.next || (result.requires_commit ? 'The worker must resolve and commit this branch before proposing it.' : 'The worker branch matches the accepted source.')), (result.conflicts || []).length ? el('div', {}, el('h3', {}, 'Conflict paths'), el('ul', {}, result.conflicts.map(path => el('li', {}, el('code', {}, path))))) : null, el('details', { class: 'tool-detail' }, el('summary', {}, 'Controller merge record'), el('pre', {}, JSON.stringify(result, null, 2)))); const next = modal('Worker branch update', body, [button('Close', () => next.close())], true); await loadChanges(); }); };
  }
  function gateBlockers(detail) {
    const gate = detail.gate || detail.acceptance_gate || detail.verification?.gate || {};
    return gate.blockers || gate.reasons || detail.blockers || [];
  }
  function verificationNodes(detail) {
    const receipts = detail.verifications || detail.verification_receipts || detail.receipts || (detail.verification ? [detail.verification] : []);
    return receipts.map(receiptCard);
  }
  function receiptCard(receipt) {
    const tests = receipt.tests || receipt.cases || [];
    return el('article', { class: 'receipt-card' }, el('div', { class: 'card-title' }, el('strong', {}, 'Controller check receipt'), statusBadge(receipt.status || 'not_recorded')), el('p', { class: 'small muted' }, `Candidate ${(receipt.candidate_sha || '').slice(0, 12)} · catalog ${receipt.catalog_revision ?? 'not recorded'} · ${receipt.finished_at ? new Date(receipt.finished_at).toLocaleString() : 'No completion time recorded'}`), tests.map(test => el('div', { class: 'receipt-test' }, el('span', {}, test.name || test.id || test.kind), statusBadge(test.status), el('code', {}, test.reason || ''))), receipt.limitation ? el('p', { class: 'small muted' }, receipt.limitation) : null, el('details', { class: 'evidence-record' }, el('summary', {}, 'Exact receipt and candidate identity'), el('pre', {}, JSON.stringify(receipt, null, 2))));
  }
  async function proposalDetails(proposalId) {
    const response = await api(projectApi(`/git/proposals/${encodeURIComponent(proposalId)}`));
    const proposal = response.proposal || response; const gate = response.gate || response.acceptance_gate || proposal.gate || {};
    const reviews = response.reviews || proposal.reviews || []; const objections = response.objections || proposal.objections || reviews.filter(review => review.verdict === 'changes_requested');
    const discussion = response.discussion || response.comments || [];
    const blockers = gateBlockers(response); const readonly = S.project.locked || S.project.archived;
    const reviewsView = reviews.length ? el('div', { class: 'review-list' }, reviews.map(review => el('div', { class: 'review-record' }, el('div', { class: 'action-row' }, el('strong', {}, `${readableStatus(review.kind || review.review_kind)} review`), statusBadge(review.verdict || review.status)), el('p', { class: 'small muted' }, review.actor_type === 'human' || ['human', 'user'].includes(review.origin) || review.reviewer_type === 'human' || review.reviewer_user_id ? `Human review · ${review.username || review.reviewer_user_id || review.actor || review.reviewer || 'signed-in owner'}` : `Agent / controller record · ${review.agent_id || review.actor_agent_id || 'see record'}`), el('p', {}, review.notes || review.summary || ''), el('details', { class: 'tool-detail' }, el('summary', {}, 'Exact review record'), el('pre', {}, JSON.stringify(review, null, 2)))))) : el('p', { class: 'small muted' }, 'No review receipts recorded.');
    const body = el('div', { class: 'proposal-detail' }, el('div', { class: 'action-row' }, statusBadge(proposal.status || 'proposed'), button('View diff', () => attempt(() => showDiff(proposalId))), button('Prepare / revalidate candidate', () => attempt(async () => { await api(projectApi(`/git/proposals/${encodeURIComponent(proposalId)}/candidate`), { method: 'POST', body: {} }); await proposalDetails(proposalId); }), 'button', { disabled: readonly })), el('details', { class: 'identity-record' }, el('summary', {}, 'Exact candidate identity'), el('pre', {}, JSON.stringify(Object.fromEntries(['id','base_sha','candidate_sha','candidate_tree','catalog_revision','catalog_hash','requirements_revision','decision_revision','policy_revision'].map(key => [key, proposal[key] ?? response[key] ?? null])), null, 2))), el('section', {}, el('h3', {}, 'Reviews'), reviewsView, el('div', { class: 'action-row' }, button('Record human code review', () => humanReview(proposalId, 'code'), 'button', { disabled: readonly }), button('Record human architecture review', () => humanReview(proposalId, 'architecture'), 'button', { disabled: readonly }))), el('section', {}, el('h3', {}, 'Objections'), objections.length ? el('div', {}, objections.map(objection => el('pre', { class: 'objection-record' }, typeof objection === 'string' ? objection : JSON.stringify(objection, null, 2)))) : el('p', { class: 'small muted' }, 'No objection records returned.')), el('section', {}, el('h3', {}, 'Checks and acceptance gate'), el('div', { class: 'action-row' }, el('span', { class: 'small muted' }, 'Controller gate'), statusBadge(gate.ready === true ? 'ready' : 'blocked')), blockers.length ? el('ul', { class: 'gate-blockers' }, blockers.map(blocker => el('li', {}, typeof blocker === 'string' ? blocker : JSON.stringify(blocker)))) : el('p', { class: 'small muted' }, 'The controller rechecks every gate when acceptance is requested.'), ...verificationNodes(response), el('details', { class: 'tool-detail' }, el('summary', {}, 'Controller gate record'), el('pre', {}, JSON.stringify(gate, null, 2))), el('p', { class: 'small muted' }, 'Run checks executes the active catalog. Manual and hardware checks stay pending without supported evidence; no button can manufacture a passing receipt.')));
    body.append(el('section', {}, el('div', { class: 'section-heading' }, el('h3', {}, 'Proposal discussion'), button('Add comment', () => proposalComment(proposalId), 'button', { disabled: readonly })), discussion.length ? el('div', { class: 'proposal-discussion' }, discussion.map(comment => el('article', { class: 'discussion-comment' }, el('div', { class: 'record-meta' }, `${['human', 'user'].includes(comment.origin) ? 'Human' : 'Agent'} · ${S.team?.agents?.find(agent => agent.id === comment.actor_id)?.label || comment.actor_id || comment.author_id || comment.actor_user_id || comment.user_id || comment.reviewer_user_id || comment.author || 'recorded author'} · ${comment.created_at ? new Date(comment.created_at).toLocaleString() : ''}${comment.reply_to ? ' · reply' : ''}`), el('p', {}, comment.body || comment.content || ''), button('Reply', () => proposalComment(proposalId, comment.id), 'text-button', { disabled: readonly })))) : el('p', { class: 'small muted' }, 'No discussion comments. Comments are preserved as human records and do not count as verification evidence.')));
    const runChecks = button('Run current-catalog checks', () => formAction(m, runChecks, async () => { await api(projectApi(`/git/proposals/${encodeURIComponent(proposalId)}/verify`), { method: 'POST', body: {} }); await proposalDetails(proposalId); }), 'button', { disabled: readonly });
    const accept = button('Request acceptance', () => formAction(m, accept, async () => { await api(projectApi(`/git/proposals/${encodeURIComponent(proposalId)}/accept`), { method: 'POST', body: {} }); m.close(); await loadChanges(); notice('The controller accepted the verified candidate.'); }), 'button primary', { disabled: readonly || proposal.status === 'accepted' || gate.ready !== true, title: gate.ready ? 'Recheck the controller gate and accept this exact candidate' : 'Resolve the recorded gate blockers before acceptance' });
    const m = modal(proposal.title || 'Candidate proposal', body, [button('Close', () => m.close()), button('Close proposal…', () => closeProposal(proposalId), 'button danger', { disabled: readonly || ['accepted', 'rejected', 'superseded'].includes(proposal.status) }), runChecks, accept], true);
  }
  function proposalComment(proposalId, replyTo = null) {
    const content = el('textarea', { required: true, placeholder: 'Record the question, clarification or objection for this candidate.' });
    const form = el('form', { class: 'form-grid' }, el('p', { class: 'human-review-label' }, `Human discussion · ${S.user?.username || 'signed-in user'}`), field(replyTo ? 'Reply' : 'Comment', content), help('The controller records your account identity. This discussion does not create a test receipt or an agent review.'));
    const save = button('Record comment', () => form.requestSubmit(), 'button primary'); const m = modal(replyTo ? 'Reply to proposal discussion' : 'Comment on this proposal', form, [button('Cancel', () => attempt(() => proposalDetails(proposalId))), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(projectApi(`/proposals/${encodeURIComponent(proposalId)}/comments`), { method: 'POST', body: { body: content.value.trim(), reply_to: replyTo } }); await proposalDetails(proposalId); }); };
  }
  function closeProposal(proposalId) {
    const status = formSelect(['rejected', 'superseded'], 'rejected'); const reason = el('textarea', { required: true });
    const replacements = (S.changes?.proposals || S.team?.proposals || []).filter(proposal => proposal.id !== proposalId);
    const replacement = el('select', {}, el('option', { value: '' }, 'No replacement'), replacements.map(proposal => el('option', { value: proposal.id }, proposal.title || proposal.id)));
    const form = el('form', { class: 'form-grid' }, field('Disposition', status), field('Reason', reason), field('Replacement proposal', replacement), help('The candidate, discussion, objections and receipts remain in project history. This does not delete a branch or change the accepted source.'));
    const save = button('Record proposal disposition', () => form.requestSubmit(), 'button danger'); const m = modal('Close this proposal', form, [button('Cancel', () => attempt(() => proposalDetails(proposalId))), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(projectApi(`/proposals/${encodeURIComponent(proposalId)}/close`), { method: 'POST', body: { status: status.value, reason: reason.value.trim(), replacement_id: replacement.value || null } }); m.close(); await loadChanges(); }); };
  }
  function humanReview(proposalId, kind) {
    const verdict = formSelect([{ value: 'changes_requested', label: 'Changes requested' }, { value: 'approved', label: 'Approved by me' }], 'changes_requested');
    const notes = el('textarea', { required: true, placeholder: 'What did you review? Record any limitations or objections.' });
    const form = el('form', { class: 'form-grid' }, el('p', { class: 'human-review-label' }, `Human ${kind} review · ${S.user?.username || 'signed-in user'}`), field('Your verdict', verdict), field('Review notes', notes), help('This records your human review against the exact candidate. It is not an agent review or a test result.'));
    const save = button('Record my human review', () => form.requestSubmit(), 'button primary'); const m = modal('Record a human review', form, [button('Cancel', () => attempt(() => proposalDetails(proposalId))), save]);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { await api(projectApi(`/git/proposals/${encodeURIComponent(proposalId)}/reviews`), { method: 'POST', body: { kind, verdict: verdict.value, notes: notes.value.trim() } }); await proposalDetails(proposalId); }); };
  }
  async function loadRegression() {
    if (!S.project) return; await refreshTeam(); if (S.tab !== 'regression') return;
    const catalog = S.team.catalog || await api(projectApi('/catalog')); const tests = catalog.tests || catalog.entries || []; const history = S.team.catalog_history || [];
    const proposals = S.team.proposals || []; const selected = proposals.find(proposal => proposal.id === S.regressionProposalId) || proposals[0]; S.regressionProposalId = selected?.id || null;
    const evidence = selected ? await api(projectApi(`/git/proposals/${encodeURIComponent(selected.id)}`)) : null;
    const chooser = formSelect(proposals.map(proposal => ({ value: proposal.id, label: proposal.title || proposal.id })), selected?.id); chooser.onchange = () => { S.regressionProposalId = chooser.value; attempt(loadRegression); };
    const evidenceView = el('section', { class: 'team-section regression-evidence' }, el('div', { class: 'section-heading' }, el('h3', {}, 'Candidate check evidence'), proposals.length ? chooser : null), evidence ? (evidence.receipts || []).length ? (evidence.receipts || []).map(receiptCard) : el('p', { class: 'small muted' }, 'No controller test receipts for this candidate yet. Run current-catalog checks in Changes.') : el('p', { class: 'small muted' }, 'Candidate evidence appears here after a worker proposal is recorded.'));
    const rows = tests.map(test => el('div', { class: 'regression-row' }, el('div', {}, el('strong', {}, test.name || test.id), el('p', { class: 'small muted' }, `${test.kind || 'command'}${test.path ? ' · '+test.path : ''}${test.hardware ? ' · hardware evidence required' : ''}`)), statusBadge(test.active === false || test.enabled === false ? 'inactive' : test.kind === 'manual' || test.hardware ? 'manual_pending' : test.kind === 'command' || !test.kind ? 'execution_unavailable' : 'controller_check'), el('span', { class: 'small muted' }, 'All active checks required')));
    const historyNodes = history.slice().sort((a, b) => Number(b.revision) - Number(a.revision)).map(version => el('details', { class: 'catalog-history' }, el('summary', {}, `Revision ${version.revision} · ${(version.tests || version.entries || []).length} checks · ${version.created_at ? new Date(version.created_at).toLocaleString() : 'saved'}`), el('pre', {}, JSON.stringify(version, null, 2))));
    $('regression-view').replaceChildren(el('div', { class: 'view-header' }, el('div', {}, el('h2', {}, 'Regression catalog'), el('p', {}, `Active revision ${catalog.revision ?? 0}. Earlier catalog versions and check evidence remain attached to their original candidates.`)), button('Edit active catalog', () => catalogEditor(catalog), 'button primary', { disabled: S.project.locked || S.project.archived })), el('div', { class: 'catalog-identity' }, el('span', { class: 'role-tag' }, catalog.origin === 'user' ? 'USER-ACTIVATED CATALOG' : 'NO USER CATALOG ACTIVE'), el('code', {}, catalog.hash || 'No active catalog hash')), rows.length ? el('div', { class: 'regression-list' }, rows) : el('div', { class: 'empty-panel' }, 'No active regression checks. Acceptance remains blocked until a required catalog is recorded.'), el('p', { class: 'team-policy-note' }, 'Built-in checks run through the controller against a candidate. Hardware/manual checks remain pending. Arbitrary VM test execution is unavailable and fails closed. Run checks from a candidate in Changes to preserve exact-candidate evidence.'), evidenceView, el('section', { class: 'team-section' }, el('h3', {}, 'Preserved catalog history'), historyNodes.length ? historyNodes : el('p', { class: 'small muted' }, 'No earlier catalog revisions.')));
  }
  function catalogEditor(catalog) {
    const tests = JSON.parse(JSON.stringify(catalog.tests || catalog.entries || [])); const existingIds = new Set(tests.map(test => test.id)); const list = el('div', { class: 'catalog-editor-list' }); const controls = [];
    const kinds = [{ value: 'git_diff_check', label: 'Git whitespace check' }, { value: 'file_exists', label: 'File exists' }, { value: 'file_contains', label: 'File contains text' }, { value: 'json_valid', label: 'JSON file is valid' }, { value: 'manual', label: 'Manual / hardware evidence' }, { value: 'command', label: 'VM command (unavailable)' }];
    const addRow = test => {
      const name = el('input', { required: true, value: test.name || '', placeholder: 'Check name' }); const kind = formSelect(kinds, test.kind || (test.command ? 'command' : 'git_diff_check')); const path = el('input', { value: test.path || '', placeholder: 'Project-relative file path' }); const expected = el('input', { value: test.expected || '', placeholder: 'Expected text (for contains)' }); const command = el('input', { value: typeof test.command === 'string' ? test.command : test.command ? JSON.stringify(test.command) : '', placeholder: 'Stored command; isolated executor unavailable' }); const retirementReason = el('input', { value: test.retirement_reason || '', placeholder: 'Why is this check being retired?' }); const active = el('input', { type: 'checkbox', checked: test.active !== false && test.enabled !== false }); const required = el('input', { type: 'checkbox', checked: true, disabled: true }); const hardware = el('input', { type: 'checkbox', checked: Boolean(test.hardware) });
      const row = el('div', { class: 'catalog-editor-row' }, el('div', { class: 'form-grid two' }, field('Check name', name), field('Check kind', kind), field('File path', path), field('Expected text', expected), field('VM command (unavailable)', command), field('Retirement reason', retirementReason, 'Required when disabling or retiring an active check.')), el('div', { class: 'action-row' }, el('label', { class: 'field-check' }, active, 'Active'), el('label', { class: 'field-check' }, required, 'All active checks required'), el('label', { class: 'field-check' }, hardware, 'Hardware'), button(existingIds.has(test.id) ? 'Retire check' : 'Remove new check', () => { if (existingIds.has(test.id)) { active.checked = false; retirementReason.required = test.active !== false; retirementReason.focus(); row.classList.add('retiring'); } else { row.remove(); record.removed = true; } }, 'text-button')));
      const record = { test, name, kind, path, expected, command, retirementReason, active, required, hardware, removed: false }; active.onchange = () => { retirementReason.required = existingIds.has(test.id) && test.active !== false && !active.checked; }; controls.push(record); list.append(row);
    };
    tests.forEach(addRow);
    const form = el('form', { class: 'form-grid' }, el('p', { class: 'small muted' }, `Editing from revision ${catalog.revision ?? 0}. Saving activates a new user-authored catalog; historical revisions are retained.`), list, button('+ Add check', () => addRow({ id: `check-${crypto.randomUUID()}`, name: '', kind: 'git_diff_check', required: true, active: true, hardware: false })), el('p', { class: 'form-hint' }, 'Retiring or disabling an active check requires a reason and applies to a new revision. Earlier catalog versions remain unchanged. Every active check must pass, including historical checks marked required:false. Manual/hardware checks stay pending until supported evidence is available. Command checks cannot pass while VM execution is unavailable.'));
    const save = button('Activate new catalog revision', () => form.requestSubmit(), 'button primary'); const m = modal('Edit regression catalog', form, [button('Cancel', () => m.close()), save], true);
    form.onsubmit = e => { e.preventDefault(); formAction(m, save, async () => { const next = controls.filter(control => !control.removed).map(control => ({ ...control.test, id: control.test.id || `check-${Date.now()}`, name: control.name.value.trim(), kind: control.kind.value, path: control.path.value.trim(), expected: control.expected.value, ...(control.kind.value === 'command' ? { command: control.command.value.trim() } : {}), required: control.required.checked, active: control.active.checked, hardware: control.hardware.checked, retirement_reason: control.retirementReason.value.trim() })); await api(projectApi('/catalog'), { method: 'PUT', body: { tests: next, expected_revision: catalog.revision ?? 0 } }); m.close(); await loadRegression(); }); };
  }
  function closeSidebar() { $('sidebar').classList.remove('open'); $('sidebar-backdrop').hidden = true; }

  $('login-form').addEventListener('submit', async e => {
    e.preventDefault(); const btn = $('login-form').querySelector('button'); btn.disabled = true; $('login-error').textContent = '';
    try { const result = await api('/auth/login', { method: 'POST', body: { username: $('login-username').value.trim(), password: $('login-password').value } }); $('login-password').value = ''; S.user = result.user || result; await startApp(); }
    catch (err) { $('login-error').textContent = errorText(err); } finally { btn.disabled = false; }
  });
  $('auth-refresh').onclick = authStatus;
  $('logout').onclick = () => attempt(async () => { const key = composerKey(); await api('/auth/logout', { method: 'POST' }); storage.remove(key); S.user = null; S.project = null; S.workspace = null; S.live.clear(); $('composer-input').value = ''; showAuth({ configured: true }); });
  $('theme-toggle').onclick = () => { const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; document.documentElement.dataset.theme = next; storage.set('hxr-theme', next); };
  $('workspace-select').onchange = e => attempt(() => selectWorkspace(e.target.value));
  $('new-workspace').onclick = createWorkspace; $('new-project').onclick = () => createProject(); $('welcome-create').onclick = () => S.workspace ? createProject() : createWorkspace();
  $('project-search').oninput = renderProjects;
  $('active-projects').onclick = () => { S.archived = false; renderProjects(); }; $('archived-projects').onclick = () => { S.archived = true; renderProjects(); };
  $('project-menu').onclick = projectMenu; $('open-settings').onclick = openSettings; $('workspace-settings').onclick = () => attempt(workspaceConfiguration);
  $('pause-project').onclick = () => attempt(() => changeProjectPause(true)); $('resume-project').onclick = () => attempt(() => changeProjectPause(false));
  $('search-conversation').onclick = searchConversation;
  $('queue-summary').setAttribute('role', 'button'); $('queue-summary').setAttribute('tabindex', '0'); $('queue-summary').setAttribute('title', 'View runs and continue previous work'); $('queue-summary').onclick = showRuns; $('queue-summary').onkeydown = e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); showRuns(); } };
  $('dismiss-notice').onclick = () => $('notice').hidden = true;
  $('menu-toggle').onclick = () => { $('sidebar').classList.add('open'); $('sidebar-backdrop').hidden = false; }; $('sidebar-backdrop').onclick = closeSidebar;
  document.querySelectorAll('.tab').forEach(n => n.onclick = () => attempt(() => switchTab(n.dataset.tab)));
  $('baseline-files').onclick = () => attempt(() => switchTab('files'));
  $('composer-form').onsubmit = sendPrompt; $('composer-input').oninput = () => { saveDraft(); growComposer(); };
  $('composer-input').onkeydown = e => { if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); $('composer-form').requestSubmit(); } };
  $('model-select').onchange = () => { S.profileUserSelected = true; renderModels(); };
  $('composer-status').onclick = () => { if ($('composer-status').classList.contains('needs-review')) showRuns(); };
  $('stop-run').onclick = () => attempt(async () => { const run = currentRun(); if (!run) return; await api(`/runs/${encodeURIComponent(run.id)}/cancel`, { method: 'POST' }); await refreshRuns(); });
  $('transcript-scroll').onscroll = updateJump; $('jump-latest').onclick = () => { $('transcript-scroll').scrollTop = $('transcript-scroll').scrollHeight; };
  document.addEventListener('keydown', e => { if ((e.metaKey || e.ctrlKey) && e.key === ',') { e.preventDefault(); if (!$('app-shell').hidden) openSettings(); } });
  window.addEventListener('beforeunload', saveDraft);
  window.addEventListener('online', () => { setConnected(true); refreshRuns().catch(() => {}); }); window.addEventListener('offline', () => setConnected(false));
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshRuns().catch(() => {}); });
  setInterval(() => { if (!$('app-shell').hidden && !document.hidden) Promise.all([refreshRuns(), pollTeamEvents()]).then(() => setConnected(true)).catch(() => setConnected(false)); }, 4500);
  authStatus();
})();
