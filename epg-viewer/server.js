import express from 'express';
import compression from 'compression';
import fs from 'node:fs';
import zlib from 'zlib';
import { parsePlaylist } from './src/parseM3U.js';
import { findEpgUrlInHeader, parseXmlTv, xmltvTimeToIso, isoToXmltvTime } from './src/xmltv.js';
import { streamParseXmltv } from './src/streamXmltv.js';
import { mirrorFetch, listSnapshots } from './src/mirror.js';
import { getCache, setCache } from './src/cache.js';
import { stableStringify, sha1hex } from './src/hash.js';
import { DateTime } from 'luxon';
import {
  loadSettings,
  getDefaults,
  updateDefaults,
  listSources,
  upsertSource,
  deleteSource,
  getMappings,
  setMapping,
  bulkSetMappings,
  readSourceChannelCache,
  writeSourceChannelCache
} from './src/store.js';

const app = express();
const PORT = process.env.PORT || 3333;

// Simple in-memory cache with TTL
const TTL_MS = 10 * 60 * 1000; // 10 minutes
const playlistCache = new Map(); // key: playlistUrl -> { at: Date.now(), text, parsed }
const epgCache = new Map(); // key: epgUrl -> { at: Date.now(), text, parsed }
// Background export prewarm jobs
const prewarmJobs = new Map(); // key -> { status, percent, message, startedAt, finishedAt, exportUrl }

// Load persisted settings
loadSettings();

app.use(express.json());
app.use(compression());
app.use(express.static('public'));

// Normalize all numeric offsets in XMLTV timestamps to +0000.
// This helps IPTV apps that misinterpret or double-apply timezone offsets.
const FORCE_ZERO_OFFSET = true;

function normalizeXmltvOffsetZero(xmltv) {
  if (!xmltv) return xmltv;
  const m = /^(\d{14})(?:\s*(?:[+\-]\d{4}|Z))?$/.exec(xmltv);
  if (m) return `${m[1]} +0000`;
  return xmltv;
}

// Merge historical programmes from source mirror snapshots to backfill past days
async function backfillFromHistory(groups, windowFromMs, windowToMs, schedules, mappings) {
  const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
  const now = Date.now();
  const pastTo = Math.min(windowToMs, now);
  if (!(Number.isFinite(windowFromMs) && windowFromMs < pastTo)) return;
  // Build de-dup sets for existing items per channel
  const existingKeys = new Map(); // id -> Set(start)
  for (const id of Object.keys(schedules)) {
    const set = new Set();
    for (const p of schedules[id]) if (p.start) set.add(p.start);
    existingKeys.set(id, set);
  }
  for (const g of groups) {
    const snaps = listSnapshots(g.url);
    for (const s of snaps) {
      // parse with window limited to past segment only
      try {
        const { schedules: raw } = await streamParseXmltv(s.path, g.allowed || null, { windowFromMs, windowToMs: pastTo });
        if (g.allowed) {
          for (const [epgId, list] of Object.entries(raw)) {
            const plId = g.idMap.get(norm(epgId)); if (!plId) continue;
            if (!schedules[plId]) schedules[plId] = [];
            const set = existingKeys.get(plId) || new Set();
            for (const p of list) {
              if (p.start && set.has(p.start)) continue;
              schedules[plId].push(p);
              if (p.start) set.add(p.start);
            }
            existingKeys.set(plId, set);
          }
        } else {
          for (const [epgId, list] of Object.entries(raw)) {
            if (!schedules[epgId]) schedules[epgId] = [];
            const set = existingKeys.get(epgId) || new Set();
            for (const p of list) {
              if (p.start && set.has(p.start)) continue;
              schedules[epgId].push(p);
              if (p.start) set.add(p.start);
            }
            existingKeys.set(epgId, set);
          }
        }
      } catch {}
    }
  }
  // Sort after backfill
  for (const id of Object.keys(schedules)) schedules[id].sort((a,b)=>(a.start<b.start?-1:a.start>b.start?1:0));
}

async function fetchText(url) {
  const res = await fetch(url, { headers: { 'User-Agent': 'epg-viewer/0.1' } });
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.status} ${res.statusText}`);
  const contentType = res.headers.get('content-type') || '';
  const encoding = res.headers.get('content-encoding') || '';
  const buf = Buffer.from(await res.arrayBuffer());

  // If gzip encoded or looks like .gz, try to gunzip
  const looksGz = encoding.includes('gzip') || contentType.includes('gzip') || url.endsWith('.gz');
  if (looksGz) {
    try {
      const unzipped = zlib.gunzipSync(buf);
      return unzipped.toString('utf8');
    } catch {
      // fall through to return as text
    }
  }
  return buf.toString('utf8');
}

function getCached(map, key) {
  const v = map.get(key);
  if (!v) return null;
  if (Date.now() - v.at > TTL_MS) {
    map.delete(key);
    return null;
  }
  return v;
}

function applyOffset(list, offsetMinutes) {
  const delta = (offsetMinutes|0) * 60 * 1000;
  if (!delta) return list;
  return list.map(p => ({
    ...p,
    start: p.start ? new Date(new Date(p.start).getTime() + delta).toISOString() : p.start,
    stop: p.stop ? new Date(new Date(p.stop).getTime() + delta).toISOString() : p.stop
  }));
}

function formatXmltvWithZone(utcIso, originalXmltv, zoneId, offsetMinutes, shiftMode = 'wall') {
  // Helper to parse XMLTV timestamp
  const parseXmltvParts = (xml) => {
    const m = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+\-]\d{4}|Z)?$/.exec(xml || '');
    if (!m) return null;
    const [, Y, Mo, D, H, Mi, S, Z] = m;
    let offMin = 0;
    if (Z && Z !== 'Z') {
      const sign = Z.startsWith('-') ? -1 : 1;
      offMin = sign * ((parseInt(Z.slice(1,3),10)||0)*60 + (parseInt(Z.slice(3,5),10)||0));
    }
    return { Y, Mo, D, H, Mi, S, offMin };
  };
  const fmtXmltv = (Y, Mo, D, H, Mi, S, offMin) => {
    const sign = offMin >= 0 ? '+' : '-';
    const abs = Math.abs(offMin);
    const oh = String(Math.floor(abs/60)).padStart(2,'0');
    const om = String(abs%60).padStart(2,'0');
    return `${Y}${Mo}${D}${H}${Mi}${S} ${sign}${oh}${om}`;
  };

  const addOff = offsetMinutes|0;

  // Shift modes:
  // - 'wall': shift the wall-clock time by offsetMinutes in the given zone (DST-aware);
  //           write the correct numeric offset for the shifted instant. If zoneId is missing,
  //           shift the absolute time and keep the original numeric offset if available.
  // - 'offset': keep wall-clock digits and only adjust the numeric offset by offsetMinutes.

  // Helper: format from a Luxon DateTime using its current zone/offset
  const fmtFromDt = (dt) => {
    const Y = dt.toFormat('yyyy');
    const Mo = dt.toFormat('LL');
    const D = dt.toFormat('dd');
    const H = dt.toFormat('HH');
    const Mi = dt.toFormat('mm');
    const S = dt.toFormat('ss');
    const offMin = dt.offset|0; // minutes
    return fmtXmltv(Y, Mo, D, H, Mi, S, offMin);
  };

  if (shiftMode === 'offset') {
    // Keep wall time digits; adjust numeric offset
    if (zoneId) {
      // If zone is specified, derive digits from UTC in that zone (to be consistent per mapping),
      // then adjust numeric offset only.
      if (!utcIso && originalXmltv) {
        const p = parseXmltvParts(originalXmltv);
        if (!p) return originalXmltv;
        const finalOff = Math.max(-14*60, Math.min(14*60, p.offMin + addOff));
        return fmtXmltv(p.Y, p.Mo, p.D, p.H, p.Mi, p.S, finalOff);
      }
      if (!utcIso) return originalXmltv || null;
      const base = DateTime.fromISO(utcIso, { zone: 'utc' }).setZone(zoneId);
      const Y = base.toFormat('yyyy');
      const Mo = base.toFormat('LL');
      const D = base.toFormat('dd');
      const H = base.toFormat('HH');
      const Mi = base.toFormat('mm');
      const S = base.toFormat('ss');
      const finalOff = Math.max(-14*60, Math.min(14*60, (base.offset|0) + addOff));
      return fmtXmltv(Y, Mo, D, H, Mi, S, finalOff);
    }
    // No zoneId: use original xmltv digits if present
    if (originalXmltv) {
      const p = parseXmltvParts(originalXmltv);
      if (!p) return originalXmltv;
      const finalOff = Math.max(-14*60, Math.min(14*60, p.offMin + addOff));
      return fmtXmltv(p.Y, p.Mo, p.D, p.H, p.Mi, p.S, finalOff);
    }
    // Fallback: format from UTC ISO, with +0000 and no digit change relative to UTC
    const shiftedIso = utcIso ? new Date(new Date(utcIso).getTime()).toISOString() : null;
    return isoToXmltvTime(shiftedIso);
  }

  // Default: 'wall' mode
  if (zoneId) {
    // Zone-aware wall shift
    let iso = utcIso;
    if (!iso && originalXmltv) iso = xmltvTimeToIso(originalXmltv);
    if (!iso) return originalXmltv || null;
    const dt = DateTime.fromISO(iso, { zone: 'utc' }).setZone(zoneId).plus({ minutes: addOff });
    return fmtFromDt(dt);
  }
  // No zoneId: try to preserve original numeric offset and shift absolute time
  if (originalXmltv) {
    const p = parseXmltvParts(originalXmltv);
    const iso = xmltvTimeToIso(originalXmltv);
    if (!p || !iso) return originalXmltv;
    const shifted = DateTime.fromISO(iso, { zone: 'utc' }).plus({ minutes: addOff });
    // Render wall digits in the original fixed offset
    const dtLocal = shifted.setZone(`UTC${p.offMin>=0?'+':'-'}${String(Math.floor(Math.abs(p.offMin)/60)).padStart(2,'0')}:${String(Math.abs(p.offMin)%60).padStart(2,'0')}`);
    return fmtXmltv(dtLocal.toFormat('yyyy'), dtLocal.toFormat('LL'), dtLocal.toFormat('dd'), dtLocal.toFormat('HH'), dtLocal.toFormat('mm'), dtLocal.toFormat('ss'), p.offMin);
  }
  // Last resort: UTC wall shift
  const shiftedIso = utcIso ? new Date(new Date(utcIso).getTime() + addOff*60*1000).toISOString() : null;
  return isoToXmltvTime(shiftedIso);
}

// Build export in background and cache to file
async function prewarmExportJob(params) {
  const { pastDays, futureDays, playlistUrl, epgUrl, full = false } = params;
  const d = getDefaults();
  const pl = playlistUrl || d.playlistUrl || null;
  let epg = epgUrl || d.epgUrl || null;
  const DAY = 24 * 60 * 60 * 1000;
  const todayUTC = new Date(); todayUTC.setUTCHours(0,0,0,0);
  const windowFromMs = todayUTC.getTime() - pastDays * DAY;
  const windowToMs = todayUTC.getTime() + (futureDays + 1) * DAY;

  // Load playlist
  const channelIds = new Set();
  const channelMeta = new Map();
  if (pl) {
    let playlistText; let parsed;
    const cache = getCached(playlistCache, pl);
    if (cache) { playlistText = cache.text; parsed = cache.parsed; }
    else { playlistText = await fetchText(pl); parsed = parsePlaylist(playlistText); playlistCache.set(pl, { at: Date.now(), text: playlistText, parsed }); }
    if (epg == null) {
      const fromPl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
      epg = d.usePlaylistEpg === false ? null : fromPl;
    }
    parsed.channels.forEach(c => { if (c.id) { channelIds.add(c.id); channelMeta.set(c.id, { name: c.name, logo: c.logo || null }); }});
  }
  const mappings = getMappings();
  const sources = listSources().filter(s => s.enabled);
  const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
  const groups = new Map();
  const ensureGroup = (url) => { if (!groups.has(url)) groups.set(url, { allowed: new Set(), idMap: new Map(), url }); return groups.get(url); };
  if (channelIds.size) {
    for (const plId of channelIds) {
      const map = mappings[plId];
      if (map && map.sourceId) {
        const src = sources.find(s => s.id === map.sourceId);
        if (src && src.url) { const egId = map.epgChannelId || plId; const g = ensureGroup(src.url); g.allowed.add(egId); g.idMap.set(norm(egId), plId); continue; }
      }
      if (epg) { const g = ensureGroup(epg); g.allowed.add(plId); g.idMap.set(norm(plId), plId); }
    }
  } else {
    if (epg) { const g = ensureGroup(epg); g.allowed = null; }
    for (const s of sources) { const g = ensureGroup(s.url); g.allowed = null; }
  }

  // Mirror
  const job = prewarmJobs.get(params.key);
  if (job) { job.status = 'running'; job.message = 'Mirroring sources'; job.percent = 5; }
  const groupArr = Array.from(groups.values());
  const mirrors = await Promise.all(groupArr.map(g => mirrorFetch(g.url)));
  // Signatures
  const sigs = {};
  const hist = {};
  mirrors.forEach((m,i)=>{ const st = fs.existsSync(m.path)?fs.statSync(m.path):null; const url = groupArr[i].url; sigs[url] = { etag: m.etag||null, lastModified: m.lastModified||null, size: st?st.size:null, mtimeMs: st?st.mtimeMs:null }; const snaps = listSnapshots(url).slice(0, 12).map(s=>s.savedAt); hist[url] = snaps; });
  const keyObj = { v:1, type:'export-gz', full: !!full, urls:sigs, history: hist, ids: channelIds.size?Array.from(channelIds).sort():[], maps: channelIds.size?Array.from(channelIds).reduce((a,id)=>{const m=mappings[id]; if(m) a[id]={sourceId:m.sourceId||null, epgChannelId:m.epgChannelId||null, offsetMinutes:Number.isFinite(m.offsetMinutes)?(m.offsetMinutes|0):0, zoneId: m.zoneId || null, shiftMode: (m.shiftMode==='offset'?'offset':'wall')}; return a;},{}):{}, windowFromMs, windowToMs };
  const cacheKey = 'EPG_' + sha1hex(stableStringify(keyObj));
  params.key = cacheKey;
  const exportDir = process.cwd() + '/epg-viewer/data/cache/exports';
  try { fs.mkdirSync(exportDir, { recursive: true }); } catch {}
  const exportPath = exportDir + `/${cacheKey}.xml.gz`;
  if (fs.existsSync(exportPath)) {
    if (job) { job.message = 'Ready (cached)'; job.percent = 100; job.status = 'done'; job.exportUrl = `/epg.xml.gz?pastDays=${pastDays}&futureDays=${futureDays}`; job.finishedAt = new Date().toISOString(); }
    return { cacheKey, exportPath };
  }

  // Parse concurrently
  if (job) { job.message = 'Parsing sources'; job.percent = 25; }
  const parsedResults = await Promise.allSettled(groupArr.map((g,i)=> streamParseXmltv(mirrors[i].path, g.allowed || null, full ? { noWindow: true } : { windowFromMs, windowToMs })));
  if (job) { job.message = 'Merging'; job.percent = 70; }
  // Merge with offsets
  const schedules = {}; const epgChMeta = new Map();
  for (let i=0;i<parsedResults.length;i++){
    const g = groupArr[i];
    if (parsedResults[i].status !== 'fulfilled') { continue; }
    const { channels: chMap, schedules: raw } = parsedResults[i].value;
    for (const [id, meta] of chMap.entries()) epgChMeta.set(id, meta);
    if (g.allowed) {
      for (const [epgId, list] of Object.entries(raw)) {
        const plId = g.idMap.get(norm(epgId)); if (!plId) continue;
        if (!schedules[plId]) schedules[plId] = [];
        const off = Number.isFinite(mappings[plId]?.offsetMinutes) ? (mappings[plId].offsetMinutes|0) : 0;
        if (off) schedules[plId].push(...applyOffset(list, off)); else schedules[plId].push(...list);
      }
    } else {
      for (const [epgId, list] of Object.entries(raw)) { if (!schedules[epgId]) schedules[epgId] = []; schedules[epgId].push(...list); }
    }
  }
  for (const id of Object.keys(schedules)) schedules[id].sort((a,b)=>(a.start<b.start?-1:a.start>b.start?1:0));
  // Backfill from history if enabled and window includes the past
  if (d.historyBackfill !== false && !full) {
    await backfillFromHistory(groupArr, windowFromMs, windowToMs, schedules, mappings);
  }

  // Write gz to disk (same format as /epg.xml.gz)
  if (job) { job.message = 'Writing export'; job.percent = 85; }
  const { PassThrough } = await import('node:stream');
  const gzip = zlib.createGzip({ level: 6 });
  const tee = new PassThrough();
  gzip.pipe(tee);
  const ws = fs.createWriteStream(exportPath);
  tee.pipe(ws);
  const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const escAttr = (s) => esc(s).replace(/"/g, '&quot;');
  const write = (s) => gzip.write(s);
  write('<?xml version="1.0" encoding="UTF-8"?>\n');
  write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n');
  write('<tv generator-info-name="epg-viewer export">\n');
  const orderedIds = channelIds.size ? Array.from(channelIds) : Array.from(epgChMeta.keys());
  for (const id of orderedIds) {
    const fromPlaylist = channelMeta.get(id) || {};
    const epgId = (mappings[id]?.epgChannelId) || id;
    const fromEpg = epgChMeta.get(epgId) || {};
    const name = fromPlaylist.name || fromEpg.name || id;
    const icon = fromPlaylist.logo || fromEpg.icon || null;
    write(`  <channel id="${escAttr(id)}">\n`);
    write(`    <display-name>${esc(name)}</display-name>\n`);
    if (icon) write(`    <icon src="${escAttr(icon)}"/>\n`);
    write('  </channel>\n');
  }
  for (const id of orderedIds) {
    const list = schedules[id] || [];
    const map = mappings[id] || {};
    const offForId = Number.isFinite(map.offsetMinutes) ? (map.offsetMinutes|0) : 0;
    const mustRewrite = offForId !== 0 || (!!map.zoneId && (map.shiftMode || 'wall') === 'wall');
    for (const p of list) {
      let start = (!mustRewrite && p.startXmltv) ? p.startXmltv : formatXmltvWithZone(p.start, p.startXmltv, map.zoneId, offForId, map.shiftMode || 'wall');
      let stop = (!mustRewrite && p.stopXmltv) ? p.stopXmltv : formatXmltvWithZone(p.stop, p.stopXmltv, map.zoneId, offForId, map.shiftMode || 'wall');
      if (FORCE_ZERO_OFFSET) { start = normalizeXmltvOffsetZero(start); if (stop) stop = normalizeXmltvOffsetZero(stop); }
      const attrs = [`start="${escAttr(start || '')}"`, `channel="${escAttr(id)}"`];
      if (stop) attrs.push(`stop="${escAttr(stop)}"`);
      write(`  <programme ${attrs.join(' ')}>\n`);
      if (p.title) write(`    <title>${esc(p.title)}</title>\n`);
      if (p.desc) write(`    <desc>${esc(p.desc)}</desc>\n`);
      if (p.category) write(`    <category>${esc(p.category)}</category>\n`);
      if (p.icon) write(`    <icon src="${escAttr(p.icon)}"/>\n`);
      write('  </programme>\n');
    }
  }
  write('</tv>\n');
  gzip.end();
  await new Promise((resolve)=> ws.on('finish', resolve));
  if (job) { job.message = 'Ready'; job.percent = 100; job.status = 'done'; job.finishedAt = new Date().toISOString(); job.exportUrl = `/epg.xml.gz?pastDays=${pastDays}&futureDays=${futureDays}`; }
  return { cacheKey, exportPath };
}

app.get('/api/channels', async (req, res) => {
  try {
    const defaults = getDefaults();
    const playlistUrl = req.query.playlist || defaults.playlistUrl;
    if (!playlistUrl) return res.status(400).json({ error: 'Missing `playlist` (no server default set)' });

    let playlistText;
    let parsed;
    const cache = getCached(playlistCache, playlistUrl);
    if (cache) {
      playlistText = cache.text;
      parsed = cache.parsed;
    } else {
      playlistText = await fetchText(playlistUrl);
      parsed = parsePlaylist(playlistText);
      playlistCache.set(playlistUrl, { at: Date.now(), text: playlistText, parsed });
    }

    let epgUrl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
    if (defaults.usePlaylistEpg === false) epgUrl = null;

    res.json({
      playlistUrl,
      epgUrl,
      channelCount: parsed.channels.length,
      channels: parsed.channels.map(c => ({
        id: c.id,
        name: c.name,
        group: c.group || null,
        logo: c.logo || null,
        url: c.url
      }))
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/epg', async (req, res) => {
  try {
    const defaults = getDefaults();
    const playlistUrl = req.query.playlist || defaults.playlistUrl;
    let epgUrl = req.query.epg || defaults.epgUrl || null;
    const debug = req.query.debug === '1';
    // Time window: last 7 days to next 3 days (quantized to UTC midnight for better cache reuse)
    const DAY = 24 * 60 * 60 * 1000;
    const todayUTC = new Date();
    todayUTC.setUTCHours(0, 0, 0, 0);
    const windowFromMs = todayUTC.getTime() - 7 * DAY;
    const windowToMs = todayUTC.getTime() + (3 + 1) * DAY; // inclusive end-of-day

    let channelIds = new Set();
    let channelMeta = new Map(); // id -> { name, logo }

    if (playlistUrl) {
      let playlistText;
      let parsed;
      const cache = getCached(playlistCache, playlistUrl);
      if (cache) {
        playlistText = cache.text;
        parsed = cache.parsed;
      } else {
        playlistText = await fetchText(playlistUrl);
        parsed = parsePlaylist(playlistText);
        playlistCache.set(playlistUrl, { at: Date.now(), text: playlistText, parsed });
      }
      if (epgUrl == null) {
        const fromPl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
        epgUrl = defaults.usePlaylistEpg === false ? null : fromPl;
      }
      parsed.channels.forEach(c => {
        if (c.id) {
          channelIds.add(c.id);
          channelMeta.set(c.id, { name: c.name, logo: c.logo || null });
        }
      });
    }

    if (!epgUrl && listSources().length === 0) return res.status(400).json({ error: 'No EPG source. Provide ?epg=..., set default via /api/settings, add sources, or use a playlist with url-tvg.' });

    const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
    let base;
    if (channelIds.size) {
      // Build groups by source
      const mappings = getMappings();
      const sources = listSources().filter(s => s.enabled);
      const groups = new Map(); // key: url -> { url, idMap: Map(epgIdNorm -> playlistId), allowed:Set(epgId) }
      const ensureGroup = (url) => {
        if (!groups.has(url)) groups.set(url, { url, idMap: new Map(), allowed: new Set() });
        return groups.get(url);
      };
      for (const plId of channelIds) {
        const map = mappings[plId];
        if (map && map.sourceId) {
          const src = sources.find(s => s.id === map.sourceId);
          if (src && src.url) {
            const egId = map.epgChannelId || plId;
            const g = ensureGroup(src.url);
            g.allowed.add(egId);
            g.idMap.set(norm(egId), plId);
            continue;
          }
        }
        if (epgUrl) { const g = ensureGroup(epgUrl); g.allowed.add(plId); g.idMap.set(norm(plId), plId); }
      }

      // Mirror sources and build cache key
      const groupArr = Array.from(groups.values());
      const mirrors = await Promise.all(groupArr.map(g => mirrorFetch(g.url)));
      const sigs = {};
      mirrors.forEach((m, i) => {
        const st = fs.existsSync(m.path) ? fs.statSync(m.path) : null;
        sigs[groupArr[i].url] = {
          etag: m.etag || null,
          lastModified: m.lastModified || null,
          size: st ? st.size : null,
          mtimeMs: st ? st.mtimeMs : null
        };
      });
      const keyObj = {
        v: 1,
        urls: sigs,
        ids: Array.from(channelIds).sort(),
        maps: Array.from(channelIds).reduce((acc, id) => { const m = mappings[id]; if (m) acc[id] = { sourceId: m.sourceId, epgChannelId: m.epgChannelId || null }; return acc; }, {}),
        windowFromMs, windowToMs
      };
      const cacheKey = 'EPG_' + sha1hex(stableStringify(keyObj));
      const cached = getCache(cacheKey);
      if (cached) {
        const channels = [];
        for (const id of channelIds) {
          const fromPlaylist = channelMeta.get(id) || {};
          const map = mappings[id];
          const epgId = (map?.epgChannelId) || id;
          const fromEpg = (cached.epgMeta && cached.epgMeta[epgId]) || {};
          channels.push({ id, name: fromPlaylist.name || fromEpg.name || id, logo: fromPlaylist.logo || fromEpg.icon || null });
        }
        base = {
          epgUrl: epgUrl || null,
          channelCount: channels.length,
          programmeCount: cached.programmeCount || Object.values(cached.schedules).reduce((n,arr)=>n+arr.length,0),
          channels,
          schedules: cached.schedules
        };
      } else {
        // Parse groups concurrently from mirror files
        const parseResults = await Promise.all(groupArr.map((g, idx) => streamParseXmltv(mirrors[idx].path, g.allowed, { windowFromMs, windowToMs })));
        const mergedSchedules = {};
        const epgMetaById = new Map();
        for (let i = 0; i < parseResults.length; i++) {
          const g = groupArr[i];
          const { channels: epgChMap, schedules: rawSchedules } = parseResults[i];
          for (const [eid, meta] of epgChMap.entries()) epgMetaById.set(eid, meta);
          for (const [epgId, list] of Object.entries(rawSchedules)) {
            const plId = g.idMap.get(norm(epgId)); if (!plId) continue;
            if (!mergedSchedules[plId]) mergedSchedules[plId] = [];
            const off = Number.isFinite(mappings[plId]?.offsetMinutes) ? (mappings[plId].offsetMinutes|0) : 0;
            if (off) mergedSchedules[plId].push(...applyOffset(list, off)); else mergedSchedules[plId].push(...list);
          }
        }
        for (const id of Object.keys(mergedSchedules)) mergedSchedules[id].sort((a,b)=>(a.start<b.start?-1:a.start>b.start?1:0));
        const channels = [];
        for (const id of channelIds) {
          const fromPlaylist = channelMeta.get(id) || {};
          const map = mappings[id];
          const epgId = (map?.epgChannelId) || id;
          const fromEpg = epgMetaById.get(epgId) || {};
          channels.push({ id, name: fromPlaylist.name || fromEpg.name || id, logo: fromPlaylist.logo || fromEpg.icon || null });
        }
        const programmeCount = Object.values(mergedSchedules).reduce((n,arr)=>n+arr.length,0);
        // Save cache
        const epgMetaObj = {};
        for (const [id, meta] of epgMetaById.entries()) epgMetaObj[id] = meta;
        setCache(cacheKey, { schedules: mergedSchedules, epgMeta: epgMetaObj, programmeCount }, 10*60*1000);
        base = { epgUrl: epgUrl || null, channelCount: channels.length, programmeCount, channels, schedules: mergedSchedules };
      }
    } else {
      // No playlist — report channels from all enabled sources (channels-only) and default epgUrl
      const sources = listSources().filter(s => s.enabled);
      const channels = [];
      let programmeCount = 0;
      if (epgUrl) {
        const { channels: epgChMap, totalProgrammes } = await streamParseXmltv(epgUrl, null, { windowFromMs, windowToMs });
        for (const [id, meta] of epgChMap.entries()) channels.push({ id, name: meta.name || id, logo: meta.icon || null });
        programmeCount += totalProgrammes || 0;
      }
      for (const s of sources) {
        const { channels: epgChMap, totalProgrammes } = await streamParseXmltv(s.url, null, { windowFromMs, windowToMs });
        for (const [id, meta] of epgChMap.entries()) channels.push({ id, name: meta.name || id, logo: meta.icon || null });
        programmeCount += totalProgrammes || 0;
      }
      base = { epgUrl: epgUrl || null, channelCount: channels.length, programmeCount, channels, schedules: {} };
    }
    if (debug) {
      // For debug, we need EPG IDs; use a shallow pass to list EPG channels
      let epgIds = [];
      if (epgUrl) {
        const { channels: epgChMap } = await streamParseXmltv(epgUrl, null, { limitProgrammes: 0 });
        epgIds = epgIds.concat(Array.from(epgChMap.keys()));
      }
      for (const s of listSources().filter(s=>s.enabled)) {
        const { channels: epgChMap } = await streamParseXmltv(s.url, null, { limitProgrammes: 0 });
        epgIds = epgIds.concat(Array.from(epgChMap.keys()));
      }
      const plIds = Array.from(channelIds);
      const matched = plIds.filter(id => epgIds.includes(id));
      const plMissing = plIds.filter(id => !epgIds.includes(id));
      const epgNotInPl = epgIds.filter(id => !channelIds.has(id));
      base.debug = {
        playlistIds: plIds,
        epgChannelIds: epgIds.slice(0, 500),
        matchedCount: matched.length,
        playlistMissingInEpg: plMissing.slice(0, 200),
        epgIdsNotInPlaylist: epgNotInPl.slice(0, 200)
      };
    }
    res.json(base);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Diagnostic: quickly check if EPG text contains expected tags
app.get('/api/_diag/epg-stats', async (req, res) => {
  try {
    const epgUrl = req.query.epg;
    if (!epgUrl) return res.status(400).json({ error: 'Provide ?epg=URL' });
    const cache = getCached(epgCache, epgUrl);
    let text;
    if (cache) text = cache.text; else text = await fetchText(epgUrl);
    const len = text.length;
    const sample = text.slice(0, 400);
    const count = (needle) => (text.match(new RegExp(needle, 'g')) || []).length;
    const channelTagCount = count('<channel');
    const programmeTagCount = count('<programme');
    const programTagCount = count('<program');
    res.json({ epgUrl, length: len, channelTagCount, programmeTagCount, programTagCount, head: sample });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Diagnostic: fetch headers to see encoding/content-type
app.get('/api/_diag/headers', async (req, res) => {
  try {
    const url = req.query.url;
    if (!url) return res.status(400).json({ error: 'Provide ?url=...' });
    const r = await fetch(url, { method: 'GET', headers: { 'User-Agent': 'epg-viewer/0.1' } });
    const headers = {};
    r.headers.forEach((v, k) => { headers[k] = v; });
    res.json({ url, status: r.status, headers });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Settings endpoints to support simplified export URLs without exposing playlist in query
app.get('/api/settings', (req, res) => {
  const d = getDefaults();
  res.json({
    playlistUrl: d.playlistUrl,
    epgUrl: d.epgUrl,
    usePlaylistEpg: d.usePlaylistEpg !== false,
    pastDays: d.pastDays,
    futureDays: d.futureDays,
    historyBackfill: d.historyBackfill !== false,
    historyRetentionDays: d.historyRetentionDays
  });
});

app.post('/api/settings', (req, res) => {
  try {
    const { playlistUrl, epgUrl, usePlaylistEpg, pastDays, futureDays, historyBackfill, historyRetentionDays } = req.body || {};
    const updated = updateDefaults({
      ...(typeof playlistUrl === 'string' && playlistUrl.trim() ? { playlistUrl: playlistUrl.trim() } : {}),
      ...(typeof epgUrl === 'string' && epgUrl.trim() ? { epgUrl: epgUrl.trim() } : {}),
      ...(typeof usePlaylistEpg === 'boolean' ? { usePlaylistEpg } : {}),
      ...(Number.isFinite(pastDays) ? { pastDays: Math.max(0, pastDays|0) } : {}),
      ...(Number.isFinite(futureDays) ? { futureDays: Math.max(0, futureDays|0) } : {}),
      ...(typeof historyBackfill === 'boolean' ? { historyBackfill } : {}),
      ...(Number.isFinite(historyRetentionDays) ? { historyRetentionDays: Math.max(1, historyRetentionDays|0) } : {})
    });
    res.json({ ok: true, settings: updated });
  } catch (e) {
    res.status(400).json({ error: 'Invalid settings payload' });
  }
});

// Sources management
app.get('/api/sources', (req, res) => {
  res.json({ sources: listSources() });
});

app.post('/api/sources', (req, res) => {
  const body = req.body || {};
  if (!body.url) return res.status(400).json({ error: 'Missing url' });
  const src = upsertSource({ id: body.id, url: body.url, enabled: body.enabled, priority: body.priority });
  res.json({ ok: true, source: src });
});

app.delete('/api/sources/:id', (req, res) => {
  const ok = deleteSource(req.params.id);
  res.json({ ok });
});

// Rescan channels for a source
app.post('/api/sources/:id/rescan', async (req, res) => {
  const id = req.params.id;
  const src = listSources().find(s => s.id === id);
  if (!src) return res.status(404).json({ error: 'Source not found' });
  try {
    const { channels: chMap } = await streamParseXmltv(src.url, null, { limitProgrammes: 0 });
    const chList = Array.from(chMap.entries()).map(([id, meta]) => ({ id, name: meta.name || id, icon: meta.icon || null }));
    const payload = writeSourceChannelCache(id, chList);
    res.json({ ok: true, ...payload });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/sources/:id/channels', (req, res) => {
  const id = req.params.id;
  const cache = readSourceChannelCache(id);
  res.json(cache);
});

// Channel mappings
app.get('/api/mappings', (req, res) => {
  res.json({ mappings: getMappings() });
});

app.post('/api/mappings', (req, res) => {
  const body = req.body || {};
  if (body.mappings && typeof body.mappings === 'object') {
    const out = bulkSetMappings(body.mappings);
    res.json({ ok: true, mappings: out });
    return;
  }
  const { playlistId, sourceId, epgChannelId, offsetMinutes, zoneId, shiftMode } = body;
  if (!playlistId) return res.status(400).json({ error: 'Missing playlistId' });
  const hasAny = (sourceId != null && String(sourceId).trim() !== '') || (epgChannelId != null && String(epgChannelId).trim() !== '') || Number.isFinite(offsetMinutes) || (typeof zoneId === 'string' && zoneId.trim() !== '') || (shiftMode === 'wall' || shiftMode === 'offset');
  const payload = hasAny ? { sourceId, epgChannelId, offsetMinutes, zoneId, shiftMode } : null;
  const saved = setMapping(playlistId, payload);
  res.json({ ok: true, mapping: saved });
});

// Diagnostic: list first N EPG channel IDs via streaming parser
app.get('/api/_diag/epg-channels', async (req, res) => {
  try {
    const epgUrl = req.query.epg;
    const n = Math.max(1, Math.min(1000, parseInt(req.query.n || '50', 10)));
    if (!epgUrl) return res.status(400).json({ error: 'Provide ?epg=URL' });
    const { channels: epgChMap } = await streamParseXmltv(epgUrl, null, { limitProgrammes: 0 });
    const ids = Array.from(epgChMap.keys());
    res.json({ epgUrl, count: ids.length, sample: ids.slice(0, n) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Diagnostic: fallback channel extractor (regex over full text)
app.get('/api/_diag/epg-channels-simple', async (req, res) => {
  try {
    const epgUrl = req.query.epg;
    const n = Math.max(1, Math.min(1000, parseInt(req.query.n || '50', 10)));
    if (!epgUrl) return res.status(400).json({ error: 'Provide ?epg=URL' });
    const text = await fetchText(epgUrl);
    const re = /<channel[^>]*\sid\s*=\s*"([^"]+)"/gi;
    const ids = [];
    let m;
    while ((m = re.exec(text)) !== null) {
      ids.push(m[1]);
      if (ids.length >= 50000) break; // safety
    }
    res.json({ epgUrl, count: ids.length, sample: ids.slice(0, n) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Diagnostic: peek first N bytes of decompressed EPG via streaming pipeline
app.get('/api/_diag/stream-head', async (req, res) => {
  try {
    const epgUrl = req.query.epg;
    const n = Math.max(128, Math.min(8192, parseInt(req.query.n || '1024', 10)));
    if (!epgUrl) return res.status(400).json({ error: 'Provide ?epg=URL' });
    const r = await fetch(epgUrl, { headers: { 'User-Agent': 'epg-viewer/0.1' } });
    if (!r.ok) return res.status(r.status).json({ error: `Fetch failed: ${r.status} ${r.statusText}` });
    let stream = (await import('node:stream')).Readable.fromWeb(r.body);
    const contentType = r.headers.get('content-type') || '';
    const encoding = r.headers.get('content-encoding') || '';
    const looksGz = encoding.includes('gzip') || contentType.includes('gzip') || epgUrl.endsWith('.gz');
    if (looksGz) {
      stream = stream.pipe((await import('node:zlib')).default.createGunzip());
    }
    stream.setEncoding('utf8');
    let out = '';
    for await (const chunk of stream) {
      out += typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8');
      if (out.length >= n) {
        out = out.slice(0, n);
        break;
      }
    }
    res.json({ epgUrl, bytes: out.length, head: out });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Per-channel, windowed schedules (lazy-loading for UI)
app.get('/api/epg/channel', async (req, res) => {
  try {
    const defaults = getDefaults();
    const playlistUrl = req.query.playlist || defaults.playlistUrl;
    const chId = String(req.query.id || '').trim();
    if (!chId) return res.status(400).json({ error: 'Missing ?id=playlistChannelId' });
    const fromIso = req.query.from;
    const toIso = req.query.to;
    const now = Date.now();
    const DAY = 24 * 60 * 60 * 1000;
    let fromMs = fromIso ? Date.parse(fromIso) : now - (now % DAY);
    let toMs = toIso ? Date.parse(toIso) : (fromMs + DAY);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs)) return res.status(400).json({ error: 'Invalid from/to' });

    // Load playlist meta and default EPG URL if available and allowed
    let channelMeta = { name: chId, logo: null };
    let epgUrl = req.query.epg || defaults.epgUrl || null;
    if (playlistUrl) {
      let playlistText;
      let parsed;
      const cache = getCached(playlistCache, playlistUrl);
      if (cache) { playlistText = cache.text; parsed = cache.parsed; }
      else { playlistText = await fetchText(playlistUrl); parsed = parsePlaylist(playlistText); playlistCache.set(playlistUrl, { at: Date.now(), text: playlistText, parsed }); }
      const plCh = parsed.channels.find(c => c.id === chId);
      if (plCh) channelMeta = { name: plCh.name || chId, logo: plCh.logo || null };
      if (epgUrl == null) {
        const fromPl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
        epgUrl = defaults.usePlaylistEpg === false ? null : fromPl;
      }
    }

    // Resolve mapping
    const mappings = getMappings();
    const map = mappings[chId];
    const sources = listSources().filter(s => s.enabled);
    let sourceUrl = epgUrl || null;
    let epgChannelId = chId;
    if (map && map.sourceId) {
      const src = sources.find(s => s.id === map.sourceId);
      if (src && src.url) { sourceUrl = src.url; epgChannelId = map.epgChannelId || chId; }
    }
    if (!sourceUrl) return res.status(400).json({ error: 'No source EPG available for this channel. Add a source or enable playlist EPG.' });

    // Mirror source and build cache key
    const mirror = await mirrorFetch(sourceUrl);
    const st = fs.existsSync(mirror.path) ? fs.statSync(mirror.path) : null;
    const sig = { etag: mirror.etag || null, lastModified: mirror.lastModified || null, size: st?st.size:null, mtimeMs: st?st.mtimeMs:null };
    const off = Number.isFinite(map?.offsetMinutes) ? (map.offsetMinutes|0) : 0;
    const keyObj = { v:1, type:'channel', url: sourceUrl, sig, chId, epgChannelId, offsetMinutes: off, fromMs, toMs };
    const cacheKey = 'CH_' + sha1hex(stableStringify(keyObj));
    const inm = req.headers['if-none-match'];
    if (inm && inm === cacheKey) { res.status(304).end(); return; }
    const cached = getCache(cacheKey);
    if (cached) {
      res.setHeader('ETag', cacheKey);
      return res.json(cached);
    }

    // Parse from mirror for only this channel
    const { channels: chMap, schedules } = await streamParseXmltv(mirror.path, new Set([epgChannelId]), { windowFromMs: fromMs, windowToMs: toMs });
    const meta = chMap.get(epgChannelId) || {};
    const listRaw = schedules[epgChannelId] || [];
    const list = off ? applyOffset(listRaw, off) : listRaw;
    const payload = {
      id: chId,
      sourceUrl,
      epgChannelId,
      name: channelMeta.name || meta.name || chId,
      logo: channelMeta.logo || meta.icon || null,
      from: new Date(fromMs).toISOString(),
      to: new Date(toMs).toISOString(),
      count: list.length,
      schedules: list
    };
    setCache(cacheKey, payload, 10*60*1000);
    res.setHeader('ETag', cacheKey);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Export filtered EPG as XMLTV (gzipped)
app.get(['/api/export/epg.xml.gz', '/epg.xml.gz'], async (req, res) => {
  try {
    const d = getDefaults();
    const playlistUrl = req.query.playlist || d.playlistUrl || null;
    let epgUrl = req.query.epg || d.epgUrl || null;
    const pastDays = Math.max(0, parseInt(req.query.pastDays || `${d.pastDays}`, 10));
    const futureDays = Math.max(0, parseInt(req.query.futureDays || `${d.futureDays}`, 10));
    const filename = (req.query.filename && String(req.query.filename).trim()) || 'filtered_epg.xml.gz';

    if (!playlistUrl && !epgUrl && listSources().length === 0) return res.status(400).json({ error: 'No playlist/epg. Set defaults via POST /api/settings, add sources, or pass query params.' });

    // Collect allowed channel IDs from playlist (if provided)
    let channelIds = new Set();
    let channelMeta = new Map(); // id -> { name, logo }
    if (playlistUrl) {
      let playlistText;
      let parsed;
      const cache = getCached(playlistCache, playlistUrl);
      if (cache) {
        playlistText = cache.text;
        parsed = cache.parsed;
      } else {
        playlistText = await fetchText(playlistUrl);
        parsed = parsePlaylist(playlistText);
        playlistCache.set(playlistUrl, { at: Date.now(), text: playlistText, parsed });
      }
      if (epgUrl == null) {
        const fromPl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
        epgUrl = d.usePlaylistEpg === false ? null : fromPl;
      }
      parsed.channels.forEach(c => {
        if (c.id) {
          channelIds.add(c.id);
          channelMeta.set(c.id, { name: c.name, logo: c.logo || null });
        }
      });
    }

    if (!epgUrl && listSources().length === 0) return res.status(400).json({ error: 'No EPG URL found. Provide ?epg=..., set default via /api/settings, add sources, or use a playlist with url-tvg.' });

    // Time window (quantized to UTC midnight for cache reuse), allow full export without limits via ?full=1
    const hasWindowParams = Object.prototype.hasOwnProperty.call(req.query, 'pastDays') || Object.prototype.hasOwnProperty.call(req.query, 'futureDays');
    const isFull = req.query.full === '1' || req.query.full === 'true' || !hasWindowParams;
    const DAY = 24 * 60 * 60 * 1000;
    const todayUTC = new Date();
    todayUTC.setUTCHours(0, 0, 0, 0);
    const windowFromMs = isFull ? null : (todayUTC.getTime() - pastDays * DAY);
    const windowToMs = isFull ? null : (todayUTC.getTime() + (futureDays + 1) * DAY); // inclusive end-of-day

    // Build groups by source (like /api/epg)
    const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
    const mappings = getMappings();
    const sources = listSources().filter(s => s.enabled);
    const groups = new Map(); // url -> { allowed:Set, idMap: Map(normEpgId->playlistId) }
    const ensureGroup = (url) => { if (!groups.has(url)) groups.set(url, { allowed: new Set(), idMap: new Map(), url }); return groups.get(url); };
    if (channelIds.size) {
      for (const plId of channelIds) {
        const map = mappings[plId];
        if (map && map.sourceId) {
          const src = sources.find(s => s.id === map.sourceId);
          if (src && src.url) {
            const egId = map.epgChannelId || plId;
            const g = ensureGroup(src.url); g.allowed.add(egId); g.idMap.set(norm(egId), plId); continue;
          }
        }
        if (epgUrl) { const g = ensureGroup(epgUrl); g.allowed.add(plId); g.idMap.set(norm(plId), plId); }
      }
    } else {
      // No playlist: export whole default epgUrl (if any) + all enabled sources
      if (epgUrl) { const g = ensureGroup(epgUrl); g.allowed = null; }
      for (const s of sources) { const g = ensureGroup(s.url); g.allowed = null; }
    }

    // Mirror and parse concurrently, with schedule cache
    const groupArr = Array.from(groups.values());
    const mirrors = await Promise.all(groupArr.map(g => mirrorFetch(g.url)));
    const sigs = {};
    const hist = {};
    mirrors.forEach((m,i)=>{ const st = fs.existsSync(m.path)?fs.statSync(m.path):null; const url = groupArr[i].url; sigs[url] = { etag: m.etag||null, lastModified: m.lastModified||null, size: st?st.size:null, mtimeMs: st?st.mtimeMs:null }; const snaps = listSnapshots(url).slice(0, 12).map(s=>s.savedAt); hist[url] = snaps; });
    const keyObj = { v:1, type:'export-gz', full: !!isFull, urls:sigs, history: hist, ids: channelIds.size?Array.from(channelIds).sort():[], maps: channelIds.size?Array.from(channelIds).reduce((a,id)=>{const m=getMappings()[id]; if(m) a[id]={sourceId:m.sourceId||null, epgChannelId:m.epgChannelId||null, offsetMinutes:Number.isFinite(m.offsetMinutes)?(m.offsetMinutes|0):0, zoneId:m.zoneId||null, shiftMode:(m.shiftMode==='offset'?'offset':'wall')}; return a;},{}):{}, windowFromMs, windowToMs };
    const cacheKey = 'EPG_' + sha1hex(stableStringify(keyObj));
    let schedules = {};
    const epgChMeta = new Map();
    const cached = getCache(cacheKey);
    if (cached) {
      schedules = cached.schedules || {};
      const meta = cached.epgMeta || {};
      for (const id of Object.keys(meta)) epgChMeta.set(id, meta[id]);
    } else {
  const parsedResults = await Promise.allSettled(groupArr.map((g,i)=> streamParseXmltv(mirrors[i].path, g.allowed || null, isFull ? { noWindow: true } : { windowFromMs, windowToMs })));
      for (let i=0;i<parsedResults.length;i++){
        const g = groupArr[i];
        if (parsedResults[i].status !== 'fulfilled') { continue; }
        const { channels: chMap, schedules: raw } = parsedResults[i].value;
        for (const [id, meta] of chMap.entries()) epgChMeta.set(id, meta);
        if (g.allowed) {
          for (const [epgId, list] of Object.entries(raw)) {
            const plId = g.idMap.get(norm(epgId)); if (!plId) continue;
            if (!schedules[plId]) schedules[plId] = [];
            const off = Number.isFinite(mappings[plId]?.offsetMinutes) ? (mappings[plId].offsetMinutes|0) : 0;
            if (off) schedules[plId].push(...applyOffset(list, off)); else schedules[plId].push(...list);
          }
        } else {
          for (const [epgId, list] of Object.entries(raw)) { if (!schedules[epgId]) schedules[epgId] = []; schedules[epgId].push(...list); }
        }
      }
      for (const id of Object.keys(schedules)) schedules[id].sort((a,b)=>(a.start<b.start?-1:a.start>b.start?1:0));
      const d = getDefaults();
      if (d.historyBackfill !== false && !isFull) {
        await backfillFromHistory(groupArr, windowFromMs, windowToMs, schedules, getMappings());
      }
      const epgMetaObj = {}; for (const [id, meta] of epgChMeta.entries()) epgMetaObj[id] = meta;
      setCache(cacheKey, { schedules, epgMeta: epgMetaObj }, 10*60*1000);
    }

    const orderedIds = channelIds.size ? Array.from(channelIds) : Array.from(epgChMeta.keys());
    const plToEpgId = {};
    if (channelIds.size) {
      const maps = getMappings();
      for (const id of orderedIds) plToEpgId[id] = (maps[id]?.epgChannelId) || id;
    }
    

    // Prepare response headers and gzip stream (send as XML with gzip encoding for max compatibility)
    // Serve a pre-compressed GZ file; do NOT set Content-Encoding
    res.setHeader('Content-Type', 'application/gzip');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Cache-Control', 'no-cache');
    const gzip = zlib.createGzip({ level: 6 });
    // Serve from prebuilt export cache if available; else tee to cache file while streaming to client
    const exportDir = process.cwd() + '/epg-viewer/data/cache/exports';
    try { fs.mkdirSync(exportDir, { recursive: true }); } catch {}
    const exportKey = cacheKey; // reuse key signature
    const exportPath = exportDir + `/${exportKey}.xml.gz`;
    const existingGood = fs.existsSync(exportPath) && (() => { try { const st = fs.statSync(exportPath); return st.size > 100; } catch { return false; }})()
    if (existingGood) {
      // Fast path: stream file
      const rs = fs.createReadStream(exportPath);
      rs.on('error', () => res.status(500).end());
      rs.pipe(res);
      return;
    }
    const { PassThrough } = await import('node:stream');
    const tee = new PassThrough();
    gzip.pipe(tee);
    tee.pipe(res);
    const tmpPath = exportPath + '.tmp';
    try { if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath); } catch {}
    const ws = fs.createWriteStream(tmpPath);
    tee.pipe(ws);

    // XML helpers
    const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const escAttr = (s) => esc(s).replace(/"/g, '&quot;');
    const write = (s) => gzip.write(s);

    // Write XMLTV
    write('<?xml version="1.0" encoding="UTF-8"?>\n');
    write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n');
    write('<tv generator-info-name="epg-viewer export">\n');

    // Channels
    for (const id of orderedIds) {
      const fromPlaylist = channelMeta.get(id) || {};
      const epgId = plToEpgId[id] || id;
      const fromEpg = epgChMeta.get(epgId) || {};
      const name = fromPlaylist.name || fromEpg.name || id;
      const icon = fromPlaylist.logo || fromEpg.icon || null;
      write(`  <channel id="${escAttr(id)}">\n`);
      write(`    <display-name>${esc(name)}</display-name>\n`);
      if (icon) write(`    <icon src="${escAttr(icon)}"/>\n`);
      write('  </channel>\n');
    }

    // Programmes
    for (const id of orderedIds) {
      const list = schedules[id] || [];
      const map = mappings[id] || {};
      const offForId = Number.isFinite(map.offsetMinutes) ? (map.offsetMinutes|0) : 0;
      const mustRewrite = offForId !== 0 || (!!map.zoneId && (map.shiftMode || 'wall') === 'wall');
      for (const p of list) {
        const start = (!mustRewrite && p.startXmltv) ? p.startXmltv : formatXmltvWithZone(p.start, p.startXmltv, map.zoneId, offForId, map.shiftMode || 'wall');
        const stop = (!mustRewrite && p.stopXmltv) ? p.stopXmltv : formatXmltvWithZone(p.stop, p.stopXmltv, map.zoneId, offForId, map.shiftMode || 'wall');
        const attrs = [`start="${escAttr(start || '')}"`, `channel="${escAttr(id)}"`];
        if (stop) attrs.push(`stop="${escAttr(stop)}"`);
        write(`  <programme ${attrs.join(' ')}>\n`);
        if (p.title) write(`    <title>${esc(p.title)}</title>\n`);
        if (p.desc) write(`    <desc>${esc(p.desc)}</desc>\n`);
        if (p.category) write(`    <category>${esc(p.category)}</category>\n`);
        if (p.icon) write(`    <icon src="${escAttr(p.icon)}"/>\n`);
        write('  </programme>\n');
      }
    }

    write('</tv>\n');
    gzip.end();
    ws.on('finish', () => {
      try { fs.renameSync(tmpPath, exportPath); } catch {}
    });
  } catch (err) {
    // If headers not sent, return JSON error; else end the stream
    if (!res.headersSent) res.status(500).json({ error: err.message });
    else try { res.end(); } catch {}
  }
});

// Optional: uncompressed XML export for clients that don't support gzip
app.get(['/api/export/epg.xml', '/epg.xml'], async (req, res) => {
  try {
    const d = getDefaults();
    const playlistUrl = req.query.playlist || d.playlistUrl || null;
    let epgUrl = req.query.epg || d.epgUrl || null;
    const pastDays = Math.max(0, parseInt(req.query.pastDays || `${d.pastDays}`, 10));
    const futureDays = Math.max(0, parseInt(req.query.futureDays || `${d.futureDays}`, 10));

    if (!playlistUrl && !epgUrl && listSources().length === 0) return res.status(400).json({ error: 'No playlist/epg. Set defaults via POST /api/settings, add sources, or pass query params.' });

    // Collect allowed channel IDs from playlist (if provided)
    let channelIds = new Set();
    let channelMeta = new Map(); // id -> { name, logo }
    if (playlistUrl) {
      let playlistText;
      let parsed;
      const cache = getCached(playlistCache, playlistUrl);
      if (cache) {
        playlistText = cache.text;
        parsed = cache.parsed;
      } else {
        playlistText = await fetchText(playlistUrl);
        parsed = parsePlaylist(playlistText);
        playlistCache.set(playlistUrl, { at: Date.now(), text: playlistText, parsed });
      }
      if (epgUrl == null) {
        const fromPl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
        epgUrl = d.usePlaylistEpg === false ? null : fromPl;
      }
      parsed.channels.forEach(c => {
        if (c.id) {
          channelIds.add(c.id);
          channelMeta.set(c.id, { name: c.name, logo: c.logo || null });
        }
      });
    }

    if (!epgUrl && listSources().length === 0) return res.status(400).json({ error: 'No EPG URL found. Provide ?epg=..., set default via /api/settings, add sources, or use a playlist with url-tvg.' });

    // Time window (quantized to UTC midnight for cache reuse)
    const DAY = 24 * 60 * 60 * 1000;
    const todayUTC = new Date();
    todayUTC.setUTCHours(0, 0, 0, 0);
    const windowFromMs = todayUTC.getTime() - pastDays * DAY;
    const windowToMs = todayUTC.getTime() + (futureDays + 1) * DAY; // inclusive end-of-day

    // Group by source (same as gz)
    const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
    const mappings = getMappings();
    const sources = listSources().filter(s => s.enabled);
    const groups = new Map();
    const ensureGroup = (url) => { if (!groups.has(url)) groups.set(url, { allowed: new Set(), idMap: new Map(), url }); return groups.get(url); };
    if (channelIds.size) {
      for (const plId of channelIds) {
        const map = mappings[plId];
        if (map && map.sourceId) {
          const src = sources.find(s => s.id === map.sourceId);
          if (src && src.url) { const egId = map.epgChannelId || plId; const g=ensureGroup(src.url); g.allowed.add(egId); g.idMap.set(norm(egId), plId); continue; }
        }
        if (epgUrl) { const g=ensureGroup(epgUrl); g.allowed.add(plId); g.idMap.set(norm(plId), plId); }
      }
    } else {
      if (epgUrl) { const g=ensureGroup(epgUrl); g.allowed=null; }
      for (const s of sources) { const g=ensureGroup(s.url); g.allowed=null; }
    }
    const groupArr = Array.from(groups.values());
    const mirrors = await Promise.all(groupArr.map(g => mirrorFetch(g.url)));
    const sigs = {};
    const hist = {};
    mirrors.forEach((m,i)=>{ const st = fs.existsSync(m.path)?fs.statSync(m.path):null; const url = groupArr[i].url; sigs[url] = { etag: m.etag||null, lastModified: m.lastModified||null, size: st?st.size:null, mtimeMs: st?st.mtimeMs:null }; const snaps = listSnapshots(url).slice(0, 12).map(s=>s.savedAt); hist[url] = snaps; });
    const keyObj = { v:1, type:'export-xml', urls:sigs, history: hist, ids: channelIds.size?Array.from(channelIds).sort():[], maps: channelIds.size?Array.from(channelIds).reduce((a,id)=>{const m=getMappings()[id]; if(m) a[id]={sourceId:m.sourceId, epgChannelId:m.epgChannelId||null, offsetMinutes:Number.isFinite(m.offsetMinutes)?(m.offsetMinutes|0):0, zoneId:m.zoneId||null, shiftMode:(m.shiftMode==='offset'?'offset':'wall')}; return a;},{}):{}, windowFromMs, windowToMs };
    const cacheKey = 'EPG_' + sha1hex(stableStringify(keyObj));
    let schedules = {};
    const epgChMeta = new Map();
    const cached = getCache(cacheKey);
    if (cached) {
      schedules = cached.schedules || {};
      const meta = cached.epgMeta || {};
      for (const id of Object.keys(meta)) epgChMeta.set(id, meta[id]);
    } else {
      const parsedResults = await Promise.allSettled(groupArr.map((g,i)=> streamParseXmltv(mirrors[i].path, g.allowed || null, { windowFromMs, windowToMs })));
      for (let i=0;i<parsedResults.length;i++){
        const g = groupArr[i];
        if (parsedResults[i].status !== 'fulfilled') { continue; }
        const { channels: chMap, schedules: raw } = parsedResults[i].value;
        for (const [id, meta] of chMap.entries()) epgChMeta.set(id, meta);
        if (g.allowed) {
          for (const [epgId, list] of Object.entries(raw)) {
            const plId = g.idMap.get(norm(epgId)); if (!plId) continue;
            if (!schedules[plId]) schedules[plId] = []; schedules[plId].push(...list);
          }
        } else {
          for (const [epgId, list] of Object.entries(raw)) { if (!schedules[epgId]) schedules[epgId] = []; schedules[epgId].push(...list); }
        }
      }
      for (const id of Object.keys(schedules)) schedules[id].sort((a,b)=>(a.start<b.start?-1:a.start>b.start?1:0));
      const d = getDefaults();
      if (d.historyBackfill !== false) {
        await backfillFromHistory(groupArr, windowFromMs, windowToMs, schedules, getMappings());
      }
      const epgMetaObj = {}; for (const [id, meta] of epgChMeta.entries()) epgMetaObj[id] = meta;
      setCache(cacheKey, { schedules, epgMeta: epgMetaObj }, 10*60*1000);
    }
    const orderedIds = channelIds.size ? Array.from(channelIds) : Array.from(epgChMeta.keys());
    const plToEpgId = {};
    if (channelIds.size) {
      const maps = getMappings();
      for (const id of orderedIds) plToEpgId[id] = (maps[id]?.epgChannelId) || id;
    }

    // Headers
    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache');

    // XML helpers
    const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const escAttr = (s) => esc(s).replace(/\"/g, '&quot;');
    const write = (s) => res.write(s);

    // Write XMLTV
    write('<?xml version="1.0" encoding="UTF-8"?>\n');
    write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n');
    write('<tv generator-info-name="epg-viewer export">\n');

    // Channels
    for (const id of orderedIds) {
      const fromPlaylist = channelMeta.get(id) || {};
      const fromEpg = epgChMeta.get(plToEpgId[id] || id) || {};
      const name = fromPlaylist.name || fromEpg.name || id;
      const icon = fromPlaylist.logo || fromEpg.icon || null;
      write(`  <channel id=\"${escAttr(id)}\">\n`);
      write(`    <display-name>${esc(name)}</display-name>\n`);
      if (icon) write(`    <icon src=\"${escAttr(icon)}\"/>\n`);
      write('  </channel>\n');
    }

    // Programmes
    for (const id of orderedIds) {
      const list = schedules[id] || [];
      const map = mappings[id] || {};
      const offForId = Number.isFinite(map.offsetMinutes) ? (map.offsetMinutes|0) : 0;
      const mustRewrite = offForId !== 0 || (!!map.zoneId && (map.shiftMode || 'wall') === 'wall');
      for (const p of list) {
        let start = (!mustRewrite && p.startXmltv) ? p.startXmltv : formatXmltvWithZone(p.start, p.startXmltv, map.zoneId, offForId, map.shiftMode || 'wall');
        let stop = (!mustRewrite && p.stopXmltv) ? p.stopXmltv : formatXmltvWithZone(p.stop, p.stopXmltv, map.zoneId, offForId, map.shiftMode || 'wall');
        if (FORCE_ZERO_OFFSET) { start = normalizeXmltvOffsetZero(start); if (stop) stop = normalizeXmltvOffsetZero(stop); }
        const attrs = [`start=\"${escAttr(start || '')}\"`, `channel=\"${escAttr(id)}\"`];
        if (stop) attrs.push(`stop=\"${escAttr(stop)}\"`);
        write(`  <programme ${attrs.join(' ')}>\n`);
        if (p.title) write(`    <title>${esc(p.title)}</title>\n`);
        if (p.desc) write(`    <desc>${esc(p.desc)}</desc>\n`);
        if (p.category) write(`    <category>${esc(p.category)}</category>\n`);
        if (p.icon) write(`    <icon src=\"${escAttr(p.icon)}\"/>\n`);
        write('  </programme>\n');
      }
    }

    write('</tv>\n');
    res.end();
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: err.message });
    else try { res.end(); } catch {}
  }
});

// Trigger prewarm of export in background
app.post('/api/export/prewarm', async (req, res) => {
  try {
    const body = req.body || {};
    const d = getDefaults();
    const pastDays = Math.max(0, parseInt(body.pastDays ?? d.pastDays, 10));
    const futureDays = Math.max(0, parseInt(body.futureDays ?? d.futureDays, 10));
    const playlistUrl = body.playlist || d.playlistUrl || null;
    const epgUrl = body.epg || d.epgUrl || null;
    const full = !!(body.full === true || body.full === '1' || body.full === 'true');
    const job = { status: 'starting', percent: 0, message: 'Queued', startedAt: new Date().toISOString(), exportUrl: `/epg.xml.gz?pastDays=${pastDays}&futureDays=${futureDays}${full?'&full=1':''}` };
    // Temporary key until we compute the actual signature inside the job
    const tempKey = 'PRE_' + Math.random().toString(36).slice(2,8);
    prewarmJobs.set(tempKey, job);
    // Run in background, compute real key, and move job
    (async () => {
      try {
        const out = await prewarmExportJob({ pastDays, futureDays, playlistUrl, epgUrl, full, key: tempKey });
        const finalKey = out.cacheKey || out.key || tempKey;
        // Keep the temporary key alive so clients polling with tempKey still see 'done'
        // Also expose the final signature key for direct access.
        job.aliasKey = finalKey;
        prewarmJobs.set(finalKey, job);
      } catch (e) {
        job.status = 'error'; job.message = e.message || 'failed'; job.percent = 0;
      }
    })();
    res.json({ key: tempKey, started: true, exportUrl: job.exportUrl });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Report prewarm status by key; if unknown but export exists, return done
app.get('/api/export/status', async (req, res) => {
  try {
    const key = req.query.key;
    if (!key) return res.status(400).json({ error: 'Missing ?key' });
    const job = prewarmJobs.get(key);
    const exportDir = process.cwd() + '/epg-viewer/data/cache/exports';
    const exportPath = exportDir + `/${key}.xml.gz`;
    if (job) return res.json({ key, ...job });
    if (fs.existsSync(exportPath)) return res.json({ key, status: 'done', percent: 100, message: 'Ready', exportUrl: '/epg.xml.gz' });
    return res.json({ key, status: 'idle', percent: 0, message: 'No job' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`EPG Viewer listening on http://localhost:${PORT}`);
});
