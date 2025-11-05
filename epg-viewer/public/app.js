const settingsBtn = document.getElementById('settingsBtn');
const settingsSection = document.getElementById('settings');
const dashboardSection = document.getElementById('dashboard');
const loadingBar = document.getElementById('loadingBar');
// Settings controls
const settingsPlaylist = document.getElementById('settingsPlaylist');
const settingsEpgUrl = document.getElementById('settingsEpgUrl');
const settingsHost = document.getElementById('settingsHost');
const pastDaysInput = document.getElementById('pastDays');
const futureDaysInput = document.getElementById('futureDays');
const exportGzUrlInput = document.getElementById('exportGzUrl');
const exportXmlUrlInput = document.getElementById('exportXmlUrl');
const copyGzBtn = document.getElementById('copyGz');
const copyXmlBtn = document.getElementById('copyXml');
const applySettingsBtn = document.getElementById('applySettings');
const closeSettingsBtn = document.getElementById('closeSettings');
const channelList = document.getElementById('channelList');
const searchInput = document.getElementById('search');
const channelHeader = document.getElementById('channelHeader');
const timelineBar = document.getElementById('timelineBar');
const epgDiv = document.getElementById('epg');

let channels = [];
let epg = { channels: [], schedules: {} };
let selectedId = null;
let detectedEpgUrl = '';
let selectedDay = startOfDayLocal(new Date());
const DAY_MS = 24 * 60 * 60 * 1000;
let windowFromMs = Date.now() - 7 * DAY_MS;
let windowToMs = Date.now() + 3 * DAY_MS;

function renderChannels(filter = '') {
  const q = filter.trim().toLowerCase();
  channelList.innerHTML = '';
  const source = q ? channels.filter(c => (c.name || '').toLowerCase().includes(q) || (c.id || '').toLowerCase().includes(q)) : channels;
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
  const list = (epg.schedules[id] || []).filter(p => overlapsDay(p, selectedDay));
  if (!list.length) {
    epgDiv.innerHTML = '<div class="muted">No EPG entries for this channel.</div>';
    return;
  }
  const now = Date.now();
  const frag = document.createDocumentFragment();
  // Build a floating day header in the timeline bar
  renderTimeline();
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
  epgDiv.appendChild(frag);
  // Scroll to the current programme if visible in this day's list
  if (isToday(selectedDay)) {
    const nowEl = epgDiv.querySelector('.programme.now');
    if (nowEl) nowEl.scrollIntoView({ block: 'center' });
  }
}

function escapeHtml(s) {
  return (s || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
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
  const gz = `${base}/epg.xml.gz?pastDays=${past}&futureDays=${future}`;
  const xml = `${base}/epg.xml?pastDays=${past}&futureDays=${future}`;
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
  renderChannels('');

  const epgRes = await fetch(`/api/epg?playlist=${encodeURIComponent(playlist)}`);
  const epgJson = await epgRes.json();
  if (epgJson.error) {
    epgDiv.innerHTML = `<div class="error">${escapeHtml(epgJson.error)}</div>`;
    showLoading(false);
    return;
  }
  epg = epgJson;
  if (epg.epgUrl) detectedEpgUrl = epg.epgUrl;
  settingsEpgUrl.textContent = detectedEpgUrl || '(none)';
  // Auto-select first channel that has EPG
  const firstWithEpg = channels.find(c => c.id && epg.schedules[c.id]?.length);
  selectedId = firstWithEpg?.id || channels[0]?.id || null;
  renderChannels(searchInput.value);
  renderEpg(selectedId);
  showLoading(false);
}

// Settings interactions
settingsBtn.onclick = () => {
  settingsSection.classList.remove('hidden');
  dashboardSection.classList.add('hidden');
  // default values
  if (!settingsPlaylist.value) settingsPlaylist.value = 'http://248on.com/x/12aba339.m3u8';
  if (!settingsHost.value) settingsHost.value = window.location.host;
  computeExportUrls();
};
applySettingsBtn.onclick = async () => {
  // Persist defaults server-side so simplified export URLs work across devices
  try {
    await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playlistUrl: settingsPlaylist.value.trim(),
        epgUrl: detectedEpgUrl || undefined,
        pastDays: parseInt(pastDaysInput.value || '7', 10) || 0,
        futureDays: parseInt(futureDaysInput.value || '3', 10) || 0,
      })
    });
  } catch {}
  computeExportUrls();
  await loadAll();
};
closeSettingsBtn.onclick = () => {
  settingsSection.classList.add('hidden');
  dashboardSection.classList.remove('hidden');
};
settingsPlaylist.oninput = computeExportUrls;
settingsHost.oninput = computeExportUrls;
pastDaysInput.oninput = computeExportUrls;
futureDaysInput.oninput = computeExportUrls;
copyGzBtn.onclick = () => { navigator.clipboard.writeText(exportGzUrlInput.value).catch(()=>{}); };
copyXmlBtn.onclick = () => { navigator.clipboard.writeText(exportXmlUrlInput.value).catch(()=>{}); };
searchInput.oninput = () => renderChannels(searchInput.value);

// Auto-init: set defaults and load
settingsPlaylist.value = 'http://248on.com/x/12aba339.m3u8';
settingsHost.value = window.location.host;
computeExportUrls();
loadAll().catch(() => { showLoading(false); });

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
