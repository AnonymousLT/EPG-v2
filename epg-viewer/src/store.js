import fs from 'node:fs';
import path from 'node:path';

const dataDir = path.join(process.cwd(), 'epg-viewer', 'data');
const settingsPath = path.join(dataDir, 'settings.json');
const sourcesCacheDir = path.join(dataDir, 'source-cache');

function ensureDirs() {
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  if (!fs.existsSync(sourcesCacheDir)) fs.mkdirSync(sourcesCacheDir, { recursive: true });
}

  const defaultSettings = {
  defaults: {
    playlistUrl: 'http://248on.com/x/12aba339.m3u8',
    epgUrl: null,
    usePlaylistEpg: true,
    pastDays: 7,
    futureDays: 3
  },
  sources: [
    // { id, url, enabled: true, priority: 100, lastScanAt: ISO, channelCount: 0 }
  ],
  mappings: {
    // playlistChannelId: { sourceId, epgChannelId }
  }
};

let cached = null;

export function loadSettings() {
  ensureDirs();
  if (!fs.existsSync(settingsPath)) {
    cached = defaultSettings;
    fs.writeFileSync(settingsPath, JSON.stringify(cached, null, 2));
    return cached;
  }
  try {
    const text = fs.readFileSync(settingsPath, 'utf8');
    const obj = JSON.parse(text);
    // merge defaults for new fields
    cached = {
      defaults: { ...defaultSettings.defaults, ...(obj.defaults || {}) },
      sources: Array.isArray(obj.sources) ? obj.sources : [],
      mappings: obj.mappings || {}
    };
  } catch {
    cached = JSON.parse(JSON.stringify(defaultSettings));
  }
  return cached;
}

export function saveSettings() {
  ensureDirs();
  if (!cached) loadSettings();
  fs.writeFileSync(settingsPath, JSON.stringify(cached, null, 2));
}

export function getDefaults() {
  if (!cached) loadSettings();
  return cached.defaults;
}

export function updateDefaults(partial) {
  if (!cached) loadSettings();
  cached.defaults = { ...cached.defaults, ...partial };
  saveSettings();
  return cached.defaults;
}

export function listSources() {
  if (!cached) loadSettings();
  return cached.sources;
}

export function upsertSource(src) {
  if (!cached) loadSettings();
  let { id, url, enabled = true, priority = 100 } = src;
  if (!id) {
    id = `src_${Math.random().toString(36).slice(2, 8)}`;
  }
  const idx = cached.sources.findIndex(s => s.id === id);
  const base = { id, url, enabled: !!enabled, priority: priority | 0 };
  if (idx >= 0) {
    cached.sources[idx] = { ...cached.sources[idx], ...base };
  } else {
    cached.sources.push(base);
  }
  saveSettings();
  return base;
}

export function deleteSource(id) {
  if (!cached) loadSettings();
  const before = cached.sources.length;
  cached.sources = cached.sources.filter(s => s.id !== id);
  // remove cache file
  try {
    const p = path.join(sourcesCacheDir, `${id}.json`);
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
  // remove mappings referencing this source
  for (const k of Object.keys(cached.mappings)) {
    if (cached.mappings[k]?.sourceId === id) delete cached.mappings[k];
  }
  saveSettings();
  return cached.sources.length !== before;
}

export function getMappings() {
  if (!cached) loadSettings();
  return cached.mappings;
}

export function setMapping(playlistId, mapping) {
  if (!cached) loadSettings();
  if (!mapping) delete cached.mappings[playlistId];
  else {
    // Only keep known fields
    const out = {};
    if (mapping.sourceId) out.sourceId = mapping.sourceId;
    if (mapping.epgChannelId) out.epgChannelId = mapping.epgChannelId;
    if (Number.isFinite(mapping.offsetMinutes)) out.offsetMinutes = mapping.offsetMinutes|0;
    if (typeof mapping.zoneId === 'string' && mapping.zoneId.trim()) out.zoneId = mapping.zoneId.trim();
    if (mapping.shiftMode === 'wall' || mapping.shiftMode === 'offset') out.shiftMode = mapping.shiftMode;
    cached.mappings[playlistId] = out;
  }
  saveSettings();
  return cached.mappings[playlistId] || null;
}

export function bulkSetMappings(m) {
  if (!cached) loadSettings();
  cached.mappings = { ...cached.mappings, ...(m || {}) };
  saveSettings();
  return cached.mappings;
}

// Channel cache per source
export function readSourceChannelCache(sourceId) {
  ensureDirs();
  const p = path.join(sourcesCacheDir, `${sourceId}.json`);
  if (!fs.existsSync(p)) return { scannedAt: null, channels: [] };
  try {
    const obj = JSON.parse(fs.readFileSync(p, 'utf8'));
    return { scannedAt: obj.scannedAt || null, channels: Array.isArray(obj.channels) ? obj.channels : [] };
  } catch {
    return { scannedAt: null, channels: [] };
  }
}

export function writeSourceChannelCache(sourceId, channels) {
  ensureDirs();
  const p = path.join(sourcesCacheDir, `${sourceId}.json`);
  const payload = { scannedAt: new Date().toISOString(), channels: channels || [] };
  fs.writeFileSync(p, JSON.stringify(payload, null, 2));
  // Update settings stats
  if (!cached) loadSettings();
  const idx = cached.sources.findIndex(s => s.id === sourceId);
  if (idx >= 0) {
    cached.sources[idx].lastScanAt = payload.scannedAt;
    cached.sources[idx].channelCount = payload.channels.length;
    saveSettings();
  }
  return payload;
}
