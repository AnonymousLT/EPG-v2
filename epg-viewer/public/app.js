const settingsBtn = document.getElementById('settingsBtn');
const homeBtn = document.getElementById('homeBtn');
const settingsSection = document.getElementById('settings');
const dashboardSection = document.getElementById('dashboard');
const loadingBar = document.getElementById('loadingBar');
// Settings controls
const settingsPlaylist = document.getElementById('settingsPlaylist');
const settingsEpgUrl = document.getElementById('settingsEpgUrl');
const usePlaylistEpg = document.getElementById('usePlaylistEpg');
const settingsHost = document.getElementById('settingsHost');
const pastDaysInput = document.getElementById('pastDays');
const futureDaysInput = document.getElementById('futureDays');
const exportGzUrlInput = document.getElementById('exportGzUrl');
const exportXmlUrlInput = document.getElementById('exportXmlUrl');
const copyGzBtn = document.getElementById('copyGz');
const copyXmlBtn = document.getElementById('copyXml');
const openGzBtn = document.getElementById('openGz');
const openXmlBtn = document.getElementById('openXml');
const fullExportChk = document.getElementById('fullExport');
const historyBackfillChk = document.getElementById('historyBackfill');
const historyRetentionInput = document.getElementById('historyRetentionDays');
const applySettingsBtn = document.getElementById('applySettings');
const closeSettingsBtn = document.getElementById('closeSettings');
const prewarmBtn = document.getElementById('prewarmBtn');
const prewarmStatus = document.getElementById('prewarmStatus');
const settingsTabs = document.getElementById('settingsTabs');
// Sources & mappings UI elements
const sourcesList = document.getElementById('sourcesList');
const newSourceUrl = document.getElementById('newSourceUrl');
const addSourceBtn = document.getElementById('addSourceBtn');
const refreshSourcesBtn = document.getElementById('refreshSources');
const mappingList = document.getElementById('mappingList');
const mapSearch = document.getElementById('mapSearch');
const mapGroup = document.getElementById('mapGroup');
const srcListEl = document.getElementById('srcList');
const tzListEl = document.getElementById('tzList');
const channelList = document.getElementById('channelList');
const searchInput = document.getElementById('search');
const groupFilter = document.getElementById('groupFilter');
const channelHeader = document.getElementById('channelHeader');
const timelineBar = document.getElementById('timelineBar');
const epgDiv = document.getElementById('epg');

let channels = [];
let epg = { channels: [], schedules: {} };
let selectedId = null;
let detectedEpgUrl = '';
let sources = [];
let sourcesChannels = new Map(); // sourceId -> [{ id, name, icon }]
let mappings = {};
let selectedDay = startOfDayLocal(new Date());
const DAY_MS = 24 * 60 * 60 * 1000;
let windowFromMs = Date.now() - 7 * DAY_MS;
let windowToMs = Date.now() + 3 * DAY_MS;

function renderChannels(filter = '') {
  const q = filter.trim().toLowerCase();
  channelList.innerHTML = '';
  const group = (groupFilter?.value || '').trim();
  let source = q ? channels.filter(c => (c.name || '').toLowerCase().includes(q) || (c.id || '').toLowerCase().includes(q)) : channels;
  if (group) source = source.filter(c => (c.group || '') === group);
  for (const c of source) {
    const li = document.createElement('li');
    li.className = c.id === selectedId ? 'selected' : '';
    li.innerHTML = `
      <div class="row">
        <div class="meta">
          ${c.logo ? `<img src="${c.logo}" alt="logo" />` : ''}
          <div>
            <div class="name">${escapeHtml(c.name || '(no name)')}</div>
            <div class="id">${escapeHtml(c.id || '(no id)')}</div>
          </div>
        </div>
        <div class="group">${escapeHtml(c.group || '')}</div>
      </div>
    `;
    li.onclick = () => {
      selectedId = c.id || null;
      renderChannels(q);
      renderEpg(selectedId);
    };
    channelList.appendChild(li);
  }
}

function renderEpg(id) {
  channelHeader.innerHTML = '';
  epgDiv.innerHTML = '';
  if (!id) return;
  const meta = channels.find(c => c.id === id);
  channelHeader.innerHTML = `
    <div class="meta">
      ${meta?.logo ? `<img src="${meta.logo}" alt="logo" />` : ''}
      <div>
        <div class="name">${escapeHtml(meta?.name || id)}</div>
        <div class="id">${escapeHtml(id)}</div>
      </div>
    </div>
  `;
  renderTimeline();
  epgDiv.innerHTML = '<div class="epg-loading">Loading…</div>';
  loadChannelDay(id, selectedDay).then(list => {
    if (!list.length) { epgDiv.innerHTML = '<div class="muted">No EPG entries for this channel.</div>'; return; }
    const now = Date.now();
    const frag = document.createDocumentFragment();
    for (const p of list) {
      const start = new Date(p.start);
      const stop = p.stop ? new Date(p.stop) : null;
      const isNow = stop ? (start.getTime() <= now && now < stop.getTime()) : (start.getTime() <= now);
      const div = document.createElement('div');
      div.className = 'programme' + (isNow ? ' now' : '');
      const timeStr = fmtTime(start) + (stop ? ' – ' + fmtTime(stop) : '');
      div.innerHTML = `
        <div class="time">${timeStr}</div>
        <div class="title">${escapeHtml(p.title || '(untitled)')}</div>
        ${p.category ? `<div class="category">${escapeHtml(p.category)}</div>` : ''}
        ${p.desc ? `<div class="desc">${escapeHtml(p.desc)}</div>` : ''}
      `;
      frag.appendChild(div);
    }
    epgDiv.innerHTML = '';
    epgDiv.appendChild(frag);
    if (isToday(selectedDay)) {
      const nowEl = epgDiv.querySelector('.programme.now');
      if (nowEl) nowEl.scrollIntoView({ block: 'center' });
    }
  }).catch(() => { epgDiv.innerHTML = '<div class="error">Failed to load EPG.</div>'; });
}

function escapeHtml(s) {
  return (s || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

function simplifyUrlLabel(u) {
  try {
    const x = new URL(u);
    return x.host;
  } catch { return u; }
}

function fmtTime(d) {
  return d.toLocaleString(undefined, {
    weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
  });
}

function showLoading(on) {
  loadingBar.classList.toggle('active', !!on);
}

function computeExportUrls() {
  const host = (settingsHost.value || window.location.host).trim();
  const proto = window.location.protocol;
  const base = `${proto}//${host}`;
  const past = parseInt(pastDaysInput.value || '7', 10) || 0;
  const future = parseInt(futureDaysInput.value || '3', 10) || 0;
  let gz, xml;
  if (fullExportChk && fullExportChk.checked) {
    gz = `${base}/epg.xml.gz`;
    xml = `${base}/epg.xml`;
  } else {
    gz = `${base}/epg.xml.gz?pastDays=${past}&futureDays=${future}`;
    xml = `${base}/epg.xml?pastDays=${past}&futureDays=${future}`;
  }
  exportGzUrlInput.value = gz;
  exportXmlUrlInput.value = xml;
}

async function loadAll() {
  const playlist = settingsPlaylist.value.trim();
  if (!playlist) return;
  selectedId = null;
  channels = [];
  epg = { channels: [], schedules: {} };
  channelList.innerHTML = '<li class="muted">Loading channels…</li>';
  epgDiv.innerHTML = '';
  channelHeader.innerHTML = '';
  settingsEpgUrl.textContent = '';

  showLoading(true);
  const chRes = await fetch(`/api/channels?playlist=${encodeURIComponent(playlist)}`);
  const chJson = await chRes.json();
  if (chJson.error) {
    channelList.innerHTML = `<li class="error">${escapeHtml(chJson.error)}</li>`;
    showLoading(false);
    return;
  }
  channels = chJson.channels || [];
  detectedEpgUrl = chJson.epgUrl || '';
  settingsEpgUrl.textContent = detectedEpgUrl || '(none)';
  populateMainGroupFilter();
  renderChannels('');

  // Auto-select first channel
  selectedId = channels[0]?.id || null;
  renderChannels(searchInput.value);
  renderEpg(selectedId);
  showLoading(false);
  // If settings open, refresh mappings view
  if (!settingsSection.classList.contains('hidden')) {
    await loadMappings();
    renderMappings();
  }
}

// Settings interactions
async function loadDefaultsIntoUI() {
  try {
    const res = await fetch('/api/settings');
    const d = await res.json();
    if (d.playlistUrl) settingsPlaylist.value = d.playlistUrl;
    if (typeof d.usePlaylistEpg === 'boolean') usePlaylistEpg.checked = !!d.usePlaylistEpg;
    if (typeof d.pastDays === 'number') pastDaysInput.value = d.pastDays;
    if (typeof d.futureDays === 'number') futureDaysInput.value = d.futureDays;
    if (typeof d.historyBackfill === 'boolean' && historyBackfillChk) historyBackfillChk.checked = !!d.historyBackfill;
    if (typeof d.historyRetentionDays === 'number' && historyRetentionInput) historyRetentionInput.value = d.historyRetentionDays;
  } catch {}
  computeExportUrls();
}

settingsBtn.onclick = () => {
  settingsSection.classList.remove('hidden');
  dashboardSection.classList.add('hidden');
  // default values
  if (!settingsPlaylist.value) settingsPlaylist.value = 'http://248on.com/x/12aba339.m3u8';
  if (!settingsHost.value) settingsHost.value = window.location.host;
  loadDefaultsIntoUI();
  loadSources();
  ensureTimezoneDatalist();
  ensureSourceDatalist();
  loadMappings().then(()=>{ populateGroupFilter(); renderMappings(); });
  initTabs();
};

// Return to EPG (dashboard) when clicking the title
homeBtn && (homeBtn.onclick = () => {
  settingsSection.classList.add('hidden');
  dashboardSection.classList.remove('hidden');
});
if (applySettingsBtn) {
  applySettingsBtn.onclick = async () => {};
}
closeSettingsBtn.onclick = () => {
  settingsSection.classList.add('hidden');
  dashboardSection.classList.remove('hidden');
};
settingsPlaylist.oninput = computeExportUrls;
settingsHost.oninput = computeExportUrls;
pastDaysInput.oninput = computeExportUrls;
futureDaysInput.oninput = computeExportUrls;
fullExportChk && (fullExportChk.onchange = computeExportUrls);
historyBackfillChk && (historyBackfillChk.onchange = () => saveDefaults({ historyBackfill: !!historyBackfillChk.checked }));
historyRetentionInput && (historyRetentionInput.onchange = () => {
  const v = parseInt(historyRetentionInput.value || '21', 10) || 21;
  saveDefaults({ historyRetentionDays: v });
});
copyGzBtn.onclick = () => { navigator.clipboard.writeText(exportGzUrlInput.value).catch(()=>{}); };
copyXmlBtn.onclick = () => { navigator.clipboard.writeText(exportXmlUrlInput.value).catch(()=>{}); };
openGzBtn && (openGzBtn.onclick = () => { const u=exportGzUrlInput.value; if (u) window.open(u, '_blank'); });
openXmlBtn && (openXmlBtn.onclick = () => { const u=exportXmlUrlInput.value; if (u) window.open(u, '_blank'); });
prewarmBtn.onclick = prewarmNow;
searchInput.oninput = () => renderChannels(searchInput.value);
groupFilter && (groupFilter.onchange = () => renderChannels(searchInput.value));

// Auto-init: set defaults and load
settingsPlaylist.value = 'http://248on.com/x/12aba339.m3u8';
settingsHost.value = window.location.host;
loadDefaultsIntoUI();
loadAll().catch(() => { showLoading(false); });

// Sources management
async function loadSources() {
  try {
    const res = await fetch('/api/sources');
    const json = await res.json();
    sources = json.sources || [];
    ensureSourceDatalist();
    renderSources();
  } catch {}
}

function renderSources() {
  if (!sourcesList) return;
  sourcesList.innerHTML = '';
  if (!sources.length) {
    sourcesList.innerHTML = '<div class="muted">No sources added.</div>';
    return;
  }
  for (const s of sources) {
    const row = document.createElement('div');
    row.className = 'source-row';
    row.innerHTML = `
      <div class="url mono" title="${escapeHtml(s.url)}">${escapeHtml(s.url)}</div>
      <div><label><input type="checkbox" ${s.enabled ? 'checked' : ''} data-role="enabled"/> Enabled</label></div>
      <div><input type="number" min="0" value="${s.priority ?? 100}" data-role="priority"/></div>
      <div class="meta">${s.channelCount ?? 0} ch • ${s.lastScanAt ? new Date(s.lastScanAt).toLocaleString() : 'never'}</div>
      <div>
        <button data-action="rescan">Rescan</button>
        <button data-action="remove">Remove</button>
      </div>
    `;
    row.querySelector('[data-role="enabled"]').onchange = async (e) => {
      const enabled = e.target.checked;
      await fetch('/api/sources', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: s.id, url: s.url, enabled, priority: s.priority }) });
      loadSources();
    };
    row.querySelector('[data-role="priority"]').onchange = async (e) => {
      const priority = parseInt(e.target.value || '100', 10) || 100;
      await fetch('/api/sources', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: s.id, url: s.url, enabled: s.enabled, priority }) });
      loadSources();
    };
    row.querySelector('[data-action="rescan"]').onclick = async () => {
      row.querySelector('[data-action="rescan"]').disabled = true;
      await fetch(`/api/sources/${s.id}/rescan`, { method: 'POST' }).catch(()=>{});
      row.querySelector('[data-action="rescan"]').disabled = false;
      sourcesChannels.delete(s.id);
      loadSources();
    };
    row.querySelector('[data-action="remove"]').onclick = async () => {
      await fetch(`/api/sources/${s.id}`, { method: 'DELETE' });
      loadSources();
    };
    sourcesList.appendChild(row);
  }
}

addSourceBtn && (addSourceBtn.onclick = async () => {
  const url = (newSourceUrl?.value || '').trim();
  if (!url) return;
  await fetch('/api/sources', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ url, enabled: true, priority: 100 }) });
  if (newSourceUrl) newSourceUrl.value = '';
  loadSources();
});
refreshSourcesBtn && (refreshSourcesBtn.onclick = loadSources);
const rescanAllBtn = document.getElementById('rescanAllSources');
rescanAllBtn && (rescanAllBtn.onclick = async () => {
  if (!sources.length) return;
  rescanAllBtn.disabled = true;
  for (const s of sources) {
    try { await fetch(`/api/sources/${s.id}/rescan`, { method: 'POST' }); } catch {}
  }
  await loadSources();
  rescanAllBtn.disabled = false;
});

async function getSourceChannels(sourceId) {
  if (sourcesChannels.has(sourceId)) return sourcesChannels.get(sourceId);
  const res = await fetch(`/api/sources/${sourceId}/channels`);
  const json = await res.json();
  const list = json.channels || [];
  sourcesChannels.set(sourceId, list);
  return list;
}

async function loadMappings() {
  try {
    const res = await fetch('/api/mappings');
    const json = await res.json();
    mappings = json.mappings || {};
  } catch { mappings = {}; }
}

function renderMappings() {
  if (!mappingList) return;
  mappingList.innerHTML = '';
  if (!channels.length) {
    mappingList.innerHTML = '<div class="muted">Load channels first (Apply & Load).</div>';
    return;
  }
  // Column header
  const head = document.createElement('div');
  head.className = 'map-header';
  head.innerHTML = `
    <div>Channel</div>
    <div>Source</div>
    <div>EPG Channel</div>
    <div>Offset</div>
    <div>Mode</div>
    <div>Zone</div>
  `;
  mappingList.appendChild(head);
  // Bulk controls bar (outside scroll list)
  const mappingBulk = document.getElementById('mappingBulk');
  if (mappingBulk) {
    mappingBulk.innerHTML = `
      <div class="map-bulk-bar">
        <div class="row">
          <label><input type="checkbox" id="mapSelectAll"/> Select all</label>
          <input id="bulkSource" type="text" class="small" list="srcList" placeholder="EPG Source (pick)" />
          <input id="bulkOffset" type="number" class="small" placeholder="Offset (min)" />
          <input id="bulkZone" type="text" class="small" placeholder="Zone (IANA)" />
          <select id="bulkMode" class="small">
            <option value="">(keep mode)</option>
            <option value="wall">Shift Wall Clock</option>
            <option value="offset">Shift Numeric Offset</option>
          </select>
          <button id="applySelected">Apply to selected</button>
          <button id="applyAll">Apply to all</button>
        </div>
      </div>
    `;
  }
  const q = (mapSearch?.value || '').trim().toLowerCase();
  const g = (mapGroup?.value || '').trim();
  let filtered = q ? channels.filter(c => (c.name||'').toLowerCase().includes(q) || (c.id||'').toLowerCase().includes(q)) : channels;
  if (g) filtered = filtered.filter(c => (c.group || '') === g);
  for (const ch of filtered) {
    const row = document.createElement('div');
    row.className = 'map-row';
    row.dataset.chId = ch.id;
    const map = mappings[ch.id] || {};
    const sourceId = map.sourceId || '';
    const epgChannelId = map.epgChannelId || '';
    row.innerHTML = `
      <div class="ch">
        <label class="sel"><input type="checkbox" data-role="select" /></label>
        ${ch.logo ? `<img src="${ch.logo}" alt="logo"/>` : ''}
        <div>
          <div class="name">${escapeHtml(ch.name || ch.id)}</div>
          <div class="id mono">${escapeHtml(ch.id)}</div>
        </div>
      </div>
      <div class="col-source">
        <input data-role="source" class="small" type="text" list="srcList" placeholder="Auto (Default)" value="${sourceId ? escapeHtml(sourceId) : ''}" />
      </div>
      <div class="col-epgid">
        <input data-role="epgid" class="small" type="text" list="dl_${escapeHtml(ch.id)}" placeholder="Pick or type…" ${sourceId ? '' : 'disabled'} value="${epgChannelId ? escapeHtml(epgChannelId) : ''}" />
        <datalist id="dl_${escapeHtml(ch.id)}"></datalist>
      </div>
      <div class="col-offset">
        <input data-role="offset" type="number" class="small" placeholder="Offset (min)" value="${Number.isFinite(map.offsetMinutes)?map.offsetMinutes:''}" />
      </div>
      <div class="col-mode">
        <select data-role="shiftmode" class="small">
          <option value="wall" ${(!map.shiftMode || map.shiftMode==='wall')?'selected':''}>Shift Wall Clock</option>
          <option value="offset" ${(map.shiftMode==='offset')?'selected':''}>Shift Numeric Offset</option>
        </select>
      </div>
      <div class="col-zone">
        <input data-role="zoneid" type="text" class="small" list="tzList" placeholder="Zone (IANA)" value="${map.zoneId ? escapeHtml(map.zoneId) : ''}" />
      </div>
      
    `;
    const srcInput = row.querySelector('[data-role="source"]');
    const epgInput = row.querySelector('[data-role="epgid"]');
    const modeSelect = row.querySelector('[data-role="shiftmode"]');

    async function populateEpgOptions(autopick = false) {
      const sid = getValidSourceId(srcInput.value);
      if (!sid) { epgInput.disabled = true; const dl = row.querySelector('datalist'); if (dl) dl.innerHTML = ''; epgInput.value = epgChannelId || ''; return; }
      epgInput.disabled = false;
      const list = await getSourceChannels(sid);
      const dl = row.querySelector('datalist');
      if (dl) {
        const frag = document.createDocumentFragment();
        // Include an empty option to allow clearing to auto
        const optEmpty = document.createElement('option'); optEmpty.value = ''; frag.appendChild(optEmpty);
        for (const it of list) {
          const opt = document.createElement('option');
          opt.value = it.id;
          opt.label = it.name ? `${it.id} — ${it.name}` : it.id;
          frag.appendChild(opt);
        }
        dl.innerHTML = '';
        dl.appendChild(frag);
      }

      // Auto-select: try by exact id (case-insensitive), else by normalized name
      if (autopick) {
        const norm = s => (s||'').toString().trim().toLowerCase();
        const normName = s => (s||'').toString().toLowerCase().replace(/[^a-z0-9]+/g, '');
        let candidate = list.find(it => norm(it.id) === norm(ch.id))?.id;
        if (!candidate && ch.name) {
          const target = normName(ch.name);
          candidate = list.find(it => normName(it.name) === target)?.id;
        }
        if (candidate) {
          epgInput.value = candidate;
        }
      }
    }

    async function saveMappingForRow() {
      const sid = getValidSourceId(srcInput.value) || undefined;
      const eid = epgInput.value || undefined;
      const offv = offsetInput.value.trim();
      const off = offv === '' ? undefined : (parseInt(offv, 10) || 0);
      const zoneInput = row.querySelector('[data-role="zoneid"]');
      const zoneId = zoneInput ? (zoneInput.value.trim() || undefined) : undefined;
      const shiftMode = modeSelect ? modeSelect.value : undefined;
      try {
        await fetch('/api/mappings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ playlistId: ch.id, sourceId: sid, epgChannelId: eid, offsetMinutes: off, zoneId, shiftMode })
        });
        await loadMappings();
        // If this row corresponds to the selected channel, refresh its EPG view automatically
        if (selectedId === ch.id) {
          renderEpg(selectedId);
        }
      } catch {}
    }

    srcInput.onchange = async () => {
      await populateEpgOptions(true);
      await saveMappingForRow();
    };
    // Filter is built-in via datalist suggestions
    const offsetInput = row.querySelector('[data-role="offset"]');
    offsetInput.onchange = saveMappingForRow;
    const zoneInputInit = document.createElement('div');
    const zoneElem = row.querySelector('[data-role="zoneid"]');
    if (zoneElem) zoneElem.onchange = saveMappingForRow;
    if (modeSelect) modeSelect.onchange = saveMappingForRow;
    epgInput.onchange = saveMappingForRow;
    if (sourceId) populateEpgOptions();
    mappingList.appendChild(row);
  }
  // Bulk handlers
  const selectAll = document.getElementById('mapSelectAll');
  const applySel = document.getElementById('applySelected');
  const applyAll = document.getElementById('applyAll');
  const bulkSource = document.getElementById('bulkSource');
  const bulkOffset = document.getElementById('bulkOffset');
  const bulkZone = document.getElementById('bulkZone');
  const bulkMode = document.getElementById('bulkMode');
  if (selectAll) selectAll.onchange = () => {
    const boxes = mappingList.querySelectorAll('.map-row input[type="checkbox"][data-role="select"]');
    boxes.forEach(b => { b.checked = selectAll.checked; });
  };
  async function doApply(target) {
    const ids = [];
    const rows = mappingList.querySelectorAll('.map-row');
    rows.forEach(r => {
      if (target === 'all') ids.push(r.dataset.chId);
      else {
        const cb = r.querySelector('input[type="checkbox"][data-role="select"]');
        if (cb && cb.checked) ids.push(r.dataset.chId);
      }
    });
    if (!ids.length) return;
    const payload = {};
    const src = bulkSource ? bulkSource.value.trim() : '';
    const off = bulkOffset.value.trim();
    const zone = bulkZone.value.trim();
    const mode = bulkMode.value;
    ids.forEach(id => {
      const m = {};
      const sid = getValidSourceId(src);
      if (sid) m.sourceId = sid;
      if (off !== '') m.offsetMinutes = parseInt(off, 10) || 0;
      if (zone !== '') m.zoneId = zone;
      if (mode) m.shiftMode = mode;
      if (Object.keys(m).length) payload[id] = m;
    });
    if (!Object.keys(payload).length) return;
    try {
      await fetch('/api/mappings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ mappings: payload }) });
      await loadMappings();
      renderMappings();
    } catch {}
  }
  applySel && (applySel.onclick = () => doApply('selected'));
  applyAll && (applyAll.onclick = () => doApply('all'));
}

// Build global IANA timezone datalist once
function ensureTimezoneDatalist() {
  if (!tzListEl) return;
  if (tzListEl.dataset.ready === '1') return;
  let zones = [];
  try {
    if (typeof Intl !== 'undefined' && typeof Intl.supportedValuesOf === 'function') {
      zones = Intl.supportedValuesOf('timeZone') || [];
    }
  } catch {}
  const preferred = [
    'Europe/Vilnius','Europe/Riga','Europe/Tallinn','Europe/London','Europe/Dublin',
    'Europe/Berlin','Europe/Paris','Europe/Madrid','Europe/Rome','Europe/Warsaw',
    'America/New_York','America/Chicago','America/Denver','America/Los_Angeles',
    'Europe/Moscow','Asia/Dubai','Asia/Kolkata','Asia/Tokyo','Australia/Sydney','Pacific/Auckland'
  ];
  const set = new Set();
  const out = [];
  for (const p of preferred) { if (!set.has(p)) { out.push(p); set.add(p); } }
  for (const z of zones.sort()) { if (!set.has(z)) { out.push(z); set.add(z); } }
  const frag = document.createDocumentFragment();
  // Include empty to allow clearing
  const empty = document.createElement('option'); empty.value = ''; frag.appendChild(empty);
  for (const z of out) {
    const opt = document.createElement('option');
    opt.value = z;
    frag.appendChild(opt);
  }
  tzListEl.innerHTML = '';
  tzListEl.appendChild(frag);
  tzListEl.dataset.ready = '1';
}

// Build global sources datalist from current sources
function ensureSourceDatalist() {
  if (!srcListEl) return;
  const frag = document.createDocumentFragment();
  // empty option to allow clearing (auto default)
  const empty = document.createElement('option'); empty.value = ''; frag.appendChild(empty);
  const items = (sources || []).map(s => ({ id: s.id, label: simplifyUrlLabel(s.url || s.id) }));
  items.sort((a,b)=> a.label.localeCompare(b.label));
  for (const it of items) {
    const opt = document.createElement('option');
    opt.value = it.id; // mapping needs the id
    opt.label = it.label; // user sees the host
    frag.appendChild(opt);
  }
  srcListEl.innerHTML = '';
  srcListEl.appendChild(frag);
}

function getValidSourceId(val) {
  const v = (val || '').trim();
  if (!v) return '';
  return (sources || []).find(s => s.id === v)?.id || '';
}
mapSearch && (mapSearch.oninput = renderMappings);
mapGroup && (mapGroup.onchange = renderMappings);

function populateGroupFilter() {
  if (!mapGroup) return;
  const current = mapGroup.value;
  const set = new Set();
  for (const c of channels) { if (c.group) set.add(c.group); }
  const groups = Array.from(set).sort((a,b)=>a.localeCompare(b));
  mapGroup.innerHTML = '<option value="">All groups</option>' + groups.map(g => `<option value="${escapeHtml(g)}" ${current===g?'selected':''}>${escapeHtml(g)}</option>`).join('');
}

// Timeline helpers
function startOfDayLocal(d) {
  const x = new Date(d);
  return new Date(x.getFullYear(), x.getMonth(), x.getDate());
}
function isToday(day) {
  const t = startOfDayLocal(new Date());
  return day.getTime() === t.getTime();
}
function overlapsDay(p, day) {
  const from = day.getTime();
  const to = from + DAY_MS;
  const s = new Date(p.start).getTime();
  const e = p.stop ? new Date(p.stop).getTime() : s + 1;
  return s < to && e > from;
}
function fmtDay(d) {
  return d.toLocaleDateString(undefined, { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric' });
}
function renderTimeline() {
  const minDay = startOfDayLocal(new Date(windowFromMs));
  const maxDay = startOfDayLocal(new Date(windowToMs - 1));
  const canPrev = selectedDay.getTime() > minDay.getTime();
  const canNext = selectedDay.getTime() < maxDay.getTime();
  timelineBar.innerHTML = `
    <div class="day">${fmtDay(selectedDay)}</div>
    <div class="buttons">
      <button id="prevDay" ${canPrev ? '' : 'disabled'}>&larr; Prev</button>
      <button id="todayBtn" class="${isToday(selectedDay) ? 'primary' : ''}">Today</button>
      <button id="nextDay" ${canNext ? '' : 'disabled'}>Next &rarr;</button>
    </div>
  `;
  document.getElementById('prevDay').onclick = () => { if (canPrev) { selectedDay = new Date(selectedDay.getTime() - DAY_MS); renderEpg(selectedId); } };
  document.getElementById('nextDay').onclick = () => { if (canNext) { selectedDay = new Date(selectedDay.getTime() + DAY_MS); renderEpg(selectedId); } };
  document.getElementById('todayBtn').onclick = () => { selectedDay = startOfDayLocal(new Date()); renderEpg(selectedId); };
}

async function loadChannelDay(id, day) {
  const d0 = startOfDayLocal(day);
  const d1 = new Date(d0.getTime() + DAY_MS);
  const playlist = settingsPlaylist.value.trim();
  const url = `/api/epg/channel?id=${encodeURIComponent(id)}&from=${encodeURIComponent(d0.toISOString())}&to=${encodeURIComponent(d1.toISOString())}&playlist=${encodeURIComponent(playlist)}`;
  try {
    const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (res.status === 304) {
      // Using cache; return what we have (if any)
      return epg.schedules[id] || [];
    }
    const json = await res.json();
    if (json && Array.isArray(json.schedules)) {
      epg.schedules[id] = json.schedules;
      return json.schedules;
    }
  } catch {}
  return [];
}

function populateMainGroupFilter() {
  if (!groupFilter) return;
  const current = groupFilter.value;
  const set = new Set();
  for (const c of channels) { if (c.group) set.add(c.group); }
  const groups = Array.from(set).sort((a,b)=>a.localeCompare(b));
  groupFilter.innerHTML = '<option value="">All groups</option>' + groups.map(g => `<option value="${escapeHtml(g)}" ${current===g?'selected':''}>${escapeHtml(g)}</option>`).join('');
}

// Prewarm export in the background and poll status
let prewarmPoll;
async function prewarmNow() {
  const past = parseInt(pastDaysInput.value || '7', 10) || 0;
  const future = parseInt(futureDaysInput.value || '3', 10) || 0;
  const playlist = settingsPlaylist.value.trim();
  prewarmBtn.disabled = true;
  prewarmStatus.textContent = 'Starting…';
  try {
    const res = await fetch('/api/export/prewarm', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pastDays: past, futureDays: future, playlist, full: fullExportChk && fullExportChk.checked })
    });
    const json = await res.json();
    if (!json || !json.key) throw new Error('No key');
    const key = json.key;
    if (prewarmPoll) clearInterval(prewarmPoll);
    prewarmPoll = setInterval(async () => {
      try {
        const sres = await fetch(`/api/export/status?key=${encodeURIComponent(key)}`);
        const stat = await sres.json();
        if (stat.status === 'done') {
          prewarmStatus.textContent = 'Ready';
          clearInterval(prewarmPoll);
          prewarmBtn.disabled = false;
        } else if (stat.status === 'error') {
          prewarmStatus.textContent = `Error: ${stat.message || ''}`;
          clearInterval(prewarmPoll);
          prewarmBtn.disabled = false;
        } else {
          const pct = (stat.percent ?? 0);
          prewarmStatus.textContent = `${pct}% — ${stat.message || ''}`;
        }
      } catch {
        prewarmStatus.textContent = 'Error';
        clearInterval(prewarmPoll);
        prewarmBtn.disabled = false;
      }
    }, 1000);
  } catch {
    prewarmStatus.textContent = 'Error starting prewarm';
    prewarmBtn.disabled = false;
  }
}

// Auto-save defaults when settings change, and reload channels if playlist/EPG behavior changed
async function saveDefaults(partial, reload = false) {
  try {
    await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(partial)
    });
  } catch {}
  computeExportUrls();
  if (reload) await loadAll();
}

settingsPlaylist.onchange = () => saveDefaults({
  playlistUrl: settingsPlaylist.value.trim(),
  epgUrl: detectedEpgUrl || undefined,
  usePlaylistEpg: !!usePlaylistEpg.checked,
  pastDays: parseInt(pastDaysInput.value || '7', 10) || 0,
  futureDays: parseInt(futureDaysInput.value || '3', 10) || 0,
}, true);

usePlaylistEpg && (usePlaylistEpg.onchange = () => saveDefaults({
  playlistUrl: settingsPlaylist.value.trim(),
  epgUrl: detectedEpgUrl || undefined,
  usePlaylistEpg: !!usePlaylistEpg.checked,
}, true));

pastDaysInput.onchange = () => saveDefaults({ pastDays: parseInt(pastDaysInput.value || '7', 10) || 0 });
futureDaysInput.onchange = () => saveDefaults({ futureDays: parseInt(futureDaysInput.value || '3', 10) || 0 });

// Tabs logic
function initTabs() {
  if (!settingsTabs) return;
  const btns = Array.from(settingsTabs.querySelectorAll('.tab-btn'));
  const cards = Array.from(document.querySelectorAll('#settings .card'));
  const last = localStorage.getItem('settingsTab') || 'general';
  const show = (tab) => {
    btns.forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    cards.forEach(c => c.classList.toggle('hidden', c.dataset.tabId !== tab));
    localStorage.setItem('settingsTab', tab);
  };
  btns.forEach(b => b.onclick = () => show(b.dataset.tab));
  // Ensure a valid tab is shown
  if (!btns.find(b => b.dataset.tab === last)) show('general'); else show(last);
}
