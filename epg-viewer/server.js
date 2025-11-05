import express from 'express';
import zlib from 'zlib';
import { parsePlaylist } from './src/parseM3U.js';
import { findEpgUrlInHeader, parseXmlTv, xmltvTimeToIso, isoToXmltvTime } from './src/xmltv.js';
import { streamParseXmltv } from './src/streamXmltv.js';

const app = express();
const PORT = process.env.PORT || 3333;

// Simple in-memory cache with TTL
const TTL_MS = 10 * 60 * 1000; // 10 minutes
const playlistCache = new Map(); // key: playlistUrl -> { at: Date.now(), text, parsed }
const epgCache = new Map(); // key: epgUrl -> { at: Date.now(), text, parsed }

// Server-side defaults for simplified export URLs
const defaultSettings = {
  playlistUrl: 'http://248on.com/x/12aba339.m3u8',
  epgUrl: null,
  pastDays: 7,
  futureDays: 3,
};

app.use(express.json());
app.use(express.static('public'));

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

app.get('/api/channels', async (req, res) => {
  try {
    const playlistUrl = req.query.playlist || defaultSettings.playlistUrl;
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

    const epgUrl = findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;

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
    const playlistUrl = req.query.playlist || defaultSettings.playlistUrl;
    let epgUrl = req.query.epg || defaultSettings.epgUrl || null;
    const debug = req.query.debug === '1';
    // Time window: last 7 days to next 3 days
    const DAY = 24 * 60 * 60 * 1000;
    const now = Date.now();
    const windowFromMs = now - 7 * DAY;
    const windowToMs = now + 3 * DAY;

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
      epgUrl = epgUrl || findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
      parsed.channels.forEach(c => {
        if (c.id) {
          channelIds.add(c.id);
          channelMeta.set(c.id, { name: c.name, logo: c.logo || null });
        }
      });
    }

    if (!epgUrl) return res.status(400).json({ error: 'No EPG URL found. Provide ?epg=... or set default via /api/settings or use a playlist with url-tvg.' });

    let base;
    if (channelIds.size) {
      // Stream-parse and filter to just these channels to avoid loading whole EPG into memory
      const { channels: epgChMap, schedules: rawSchedules } = await streamParseXmltv(epgUrl, channelIds, { windowFromMs, windowToMs });
      const channels = [];
      for (const id of channelIds) {
        const fromEpg = epgChMap.get(id) || {};
        const fromPlaylist = channelMeta.get(id) || {};
        channels.push({
          id,
          name: fromPlaylist.name || fromEpg.name || id,
          logo: fromPlaylist.logo || fromEpg.icon || null
        });
      }
      // Normalize and re-key schedules under playlist IDs to avoid case/whitespace mismatches
      const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
      const idMap = new Map(); // norm -> canonical
      for (const id of channelIds) idMap.set(norm(id), id);
      const schedules = {};
      for (const [epgId, list] of Object.entries(rawSchedules)) {
        const canonical = idMap.get(norm(epgId));
        if (!canonical) continue;
        if (!schedules[canonical]) schedules[canonical] = [];
        schedules[canonical].push(...list);
      }
      for (const id of Object.keys(schedules)) {
        schedules[id].sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
      }
      base = {
        epgUrl,
        channelCount: channels.length,
        programmeCount: Object.values(schedules).reduce((n, arr) => n + arr.length, 0),
        channels,
        schedules
      };
    } else {
      // No playlist provided: get counts and channels without materializing all programmes
      const { channels: epgChMap, totalProgrammes } = await streamParseXmltv(epgUrl, null, { windowFromMs, windowToMs });
      const channels = [];
      for (const [id, meta] of epgChMap.entries()) {
        channels.push({ id, name: meta.name || id, logo: meta.icon || null });
      }
      base = {
        epgUrl,
        channelCount: channels.length,
        programmeCount: totalProgrammes,
        channels,
        schedules: {}
      };
    }
    if (debug) {
      // For debug, we need EPG IDs; use a shallow pass to list EPG channels
      const { channels: epgChMap } = await streamParseXmltv(epgUrl, null, { limitProgrammes: 0 });
      const epgIds = Array.from(epgChMap.keys());
      const plIds = Array.from(channelIds);
      const matched = plIds.filter(id => epgChMap.has(id));
      const plMissing = plIds.filter(id => !epgChMap.has(id));
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
  res.json({
    playlistUrl: defaultSettings.playlistUrl,
    epgUrl: defaultSettings.epgUrl,
    pastDays: defaultSettings.pastDays,
    futureDays: defaultSettings.futureDays,
  });
});

app.post('/api/settings', (req, res) => {
  try {
    const { playlistUrl, epgUrl, pastDays, futureDays } = req.body || {};
    if (typeof playlistUrl === 'string' && playlistUrl.trim()) defaultSettings.playlistUrl = playlistUrl.trim();
    if (typeof epgUrl === 'string' && epgUrl.trim()) defaultSettings.epgUrl = epgUrl.trim();
    if (Number.isFinite(pastDays)) defaultSettings.pastDays = Math.max(0, pastDays|0);
    if (Number.isFinite(futureDays)) defaultSettings.futureDays = Math.max(0, futureDays|0);
    res.json({ ok: true, settings: defaultSettings });
  } catch (e) {
    res.status(400).json({ error: 'Invalid settings payload' });
  }
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

// Export filtered EPG as XMLTV (gzipped)
app.get(['/api/export/epg.xml.gz', '/epg.xml.gz'], async (req, res) => {
  try {
    const playlistUrl = req.query.playlist || defaultSettings.playlistUrl || null;
    let epgUrl = req.query.epg || defaultSettings.epgUrl || null;
    const pastDays = Math.max(0, parseInt(req.query.pastDays || `${defaultSettings.pastDays}`, 10));
    const futureDays = Math.max(0, parseInt(req.query.futureDays || `${defaultSettings.futureDays}`, 10));
    const filename = (req.query.filename && String(req.query.filename).trim()) || 'filtered_epg.xml.gz';

    if (!playlistUrl && !epgUrl) return res.status(400).json({ error: 'No playlist/epg. Set defaults via POST /api/settings or pass query params.' });

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
      epgUrl = epgUrl || findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
      parsed.channels.forEach(c => {
        if (c.id) {
          channelIds.add(c.id);
          channelMeta.set(c.id, { name: c.name, logo: c.logo || null });
        }
      });
    }

    if (!epgUrl) return res.status(400).json({ error: 'No EPG URL found. Provide ?epg=... or set default via /api/settings or use a playlist with url-tvg.' });

    // Time window
    const DAY = 24 * 60 * 60 * 1000;
    const now = Date.now();
    const windowFromMs = now - pastDays * DAY;
    const windowToMs = now + futureDays * DAY;

    // Parse and filter EPG
    const { channels: epgChMap, schedules: rawSchedules } = await streamParseXmltv(epgUrl, channelIds, { windowFromMs, windowToMs });

    // Normalize schedules keys to playlist IDs if a playlist is provided
    const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
    const schedules = {};
    let orderedIds = [];
    if (channelIds.size) {
      const idMap = new Map(); // norm -> canonical
      for (const id of channelIds) idMap.set(norm(id), id);
      for (const [epgId, list] of Object.entries(rawSchedules)) {
        const canonical = idMap.get(norm(epgId));
        if (!canonical) continue;
        if (!schedules[canonical]) schedules[canonical] = [];
        schedules[canonical].push(...list);
      }
      orderedIds = Array.from(channelIds);
    } else {
      // No playlist: include all channels we saw in EPG, keep their schedules as-is
      for (const [epgId, list] of Object.entries(rawSchedules)) schedules[epgId] = list;
      orderedIds = Array.from(epgChMap.keys());
    }
    for (const id of Object.keys(schedules)) {
      schedules[id].sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
    }

    // Prepare response headers and gzip stream (send as XML with gzip encoding for max compatibility)
    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    res.setHeader('Content-Encoding', 'gzip');
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
    res.setHeader('Cache-Control', 'no-cache');
    const gzip = zlib.createGzip({ level: 6 });
    gzip.pipe(res);

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
      const fromEpg = epgChMap.get(id) || {};
      const fromPlaylist = channelMeta.get(id) || {};
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
      for (const p of list) {
        const start = isoToXmltvTime(p.start);
        const stop = isoToXmltvTime(p.stop);
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
  } catch (err) {
    // If headers not sent, return JSON error; else end the stream
    if (!res.headersSent) res.status(500).json({ error: err.message });
    else try { res.end(); } catch {}
  }
});

// Optional: uncompressed XML export for clients that don't support gzip
app.get(['/api/export/epg.xml', '/epg.xml'], async (req, res) => {
  try {
    const playlistUrl = req.query.playlist || defaultSettings.playlistUrl || null;
    let epgUrl = req.query.epg || defaultSettings.epgUrl || null;
    const pastDays = Math.max(0, parseInt(req.query.pastDays || `${defaultSettings.pastDays}`, 10));
    const futureDays = Math.max(0, parseInt(req.query.futureDays || `${defaultSettings.futureDays}`, 10));

    if (!playlistUrl && !epgUrl) return res.status(400).json({ error: 'No playlist/epg. Set defaults via POST /api/settings or pass query params.' });

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
      epgUrl = epgUrl || findEpgUrlInHeader(parsed.headerAttrs) || findEpgUrlInHeader(parsed.guessedHeaderAttrs) || null;
      parsed.channels.forEach(c => {
        if (c.id) {
          channelIds.add(c.id);
          channelMeta.set(c.id, { name: c.name, logo: c.logo || null });
        }
      });
    }

    if (!epgUrl) return res.status(400).json({ error: 'No EPG URL found. Provide ?epg=... or set default via /api/settings or use a playlist with url-tvg.' });

    // Time window
    const DAY = 24 * 60 * 60 * 1000;
    const now = Date.now();
    const windowFromMs = now - pastDays * DAY;
    const windowToMs = now + futureDays * DAY;

    // Parse and filter EPG
    const { channels: epgChMap, schedules: rawSchedules } = await streamParseXmltv(epgUrl, channelIds, { windowFromMs, windowToMs });

    // Normalize schedules keys to playlist IDs if a playlist is provided
    const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
    const schedules = {};
    let orderedIds = [];
    if (channelIds.size) {
      const idMap = new Map(); // norm -> canonical
      for (const id of channelIds) idMap.set(norm(id), id);
      for (const [epgId, list] of Object.entries(rawSchedules)) {
        const canonical = idMap.get(norm(epgId));
        if (!canonical) continue;
        if (!schedules[canonical]) schedules[canonical] = [];
        schedules[canonical].push(...list);
      }
      orderedIds = Array.from(channelIds);
    } else {
      // No playlist: include all channels we saw in EPG, keep their schedules as-is
      for (const [epgId, list] of Object.entries(rawSchedules)) schedules[epgId] = list;
      orderedIds = Array.from(epgChMap.keys());
    }
    for (const id of Object.keys(schedules)) {
      schedules[id].sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
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
      const fromEpg = epgChMap.get(id) || {};
      const fromPlaylist = channelMeta.get(id) || {};
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
      for (const p of list) {
        const start = isoToXmltvTime(p.start);
        const stop = isoToXmltvTime(p.stop);
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

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`EPG Viewer listening on http://localhost:${PORT}`);
});
