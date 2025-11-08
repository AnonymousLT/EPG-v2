import { Readable } from 'node:stream';
import fs from 'node:fs';
import zlib from 'node:zlib';
import sax from 'sax';
import { xmltvTimeToIso } from './xmltv.js';

// Stream-parse a potentially huge XMLTV file and filter to allowed channel IDs.
// Returns { channels: Map(id->{name,icon}), schedules: {id: [programmes]}, totalProgrammes }
export async function streamParseXmltv(epgUrlOrPath, allowedIds = null, opts = {}) {
  const norm = (s) => (s == null ? '' : String(s).trim().toLowerCase());
  const keepAll = !allowedIds || allowedIds.size === 0;
  const allowedNorm = keepAll ? null : new Set(Array.from(allowedIds).map(norm));
  const limitProgrammes = Number.isFinite(opts.limitProgrammes) ? opts.limitProgrammes : Infinity;
  const DAY = 24 * 60 * 60 * 1000;
  const defaultFrom = Date.now() - 7 * DAY;
  const defaultTo = Date.now() + 3 * DAY;
  const windowFromMs = Number.isFinite(opts.windowFromMs) ? opts.windowFromMs : defaultFrom;
  const windowToMs = Number.isFinite(opts.windowToMs) ? opts.windowToMs : defaultTo;
  const noWindow = !!opts.noWindow;

  let input;
  let looksGz = false;
  const src = String(epgUrlOrPath || '');
  if (/^https?:\/\//i.test(src)) {
    const res = await fetch(src, { headers: { 'User-Agent': 'epg-viewer/0.1' } });
    if (!res.ok) throw new Error(`Failed to fetch ${src}: ${res.status} ${res.statusText}`);
    input = Readable.fromWeb(res.body);
    const contentType = res.headers.get('content-type') || '';
    const encoding = res.headers.get('content-encoding') || '';
    looksGz = encoding.includes('gzip') || contentType.includes('gzip') || src.endsWith('.gz');
  } else {
    // treat as local file path
    looksGz = src.endsWith('.gz');
    input = fs.createReadStream(src);
  }
  input = looksGz ? input.pipe(zlib.createGunzip()) : input;
  if (input.setEncoding) input.setEncoding('utf8');

  const channels = new Map();
  const schedules = {}; // id -> [{start, stop, title, desc, category, icon}]
  let totalProgrammes = 0;

  const parser = sax.createStream(true, { lowercase: true, trim: false, normalize: false });
  let aborted = false;

  // State machines for current elements
  let curChannel = null; // { id, name, icon }
  let curProg = null; // { channel, chNorm, start, stop, title, desc, category, icon, include }
  let curTextTag = null; // 'display-name' | 'title' | 'desc' | 'category'

  function pushProgramme(p) {
    totalProgrammes++;
    if (!p.include) return;
    // Time-window filter: include only if overlaps [windowFromMs, windowToMs), unless noWindow
    const startIso = xmltvTimeToIso(p.start);
    const stopIso = xmltvTimeToIso(p.stop);
    if (!noWindow) {
      const startMs = startIso ? Date.parse(startIso) : NaN;
      const stopMs = stopIso ? Date.parse(stopIso) : NaN;
      const overlaps = (
        (Number.isFinite(startMs) && startMs < windowToMs && (!Number.isFinite(stopMs) || stopMs > windowFromMs)) ||
        (Number.isFinite(stopMs) && stopMs > windowFromMs && (!Number.isFinite(startMs) || startMs < windowToMs))
      );
      if (!overlaps) return;
    }
    if (!schedules[p.channel]) schedules[p.channel] = [];
    schedules[p.channel].push({
      start: startIso,
      stop: stopIso,
      startXmltv: p.start || null,
      stopXmltv: p.stop || null,
      title: p.title || null,
      desc: p.desc || null,
      category: p.category || null,
      icon: p.icon || null
    });
  }

  parser.on('opentag', (node) => {
    const name = String(node.name || '').toLowerCase();
    const attrs = {};
    const ra = node.attributes || {};
    for (const k in ra) {
      if (!Object.prototype.hasOwnProperty.call(ra, k)) continue;
      const a = ra[k];
      const lk = String((a && typeof a === 'object' && a.name != null ? a.name : k)).toLowerCase();
      if (typeof a === 'string') {
        attrs[lk] = a;
      } else if (a && typeof a === 'object' && a.value != null) {
        attrs[lk] = String(a.value);
      } else {
        attrs[lk] = '';
      }
    }
    switch (name) {
      case 'channel': {
        const id = attrs.id || null;
        curChannel = { id, name: null, icon: null };
        break;
      }
      case 'icon': {
        if (curChannel) {
          const src = attrs.src || null;
          if (src && !curChannel.icon) curChannel.icon = src;
        } else if (curProg) {
          const src = attrs.src || null;
          if (src && !curProg.icon) curProg.icon = src;
        }
        break;
      }
      case 'display-name':
        if (curChannel) curTextTag = 'display-name';
        break;
      case 'programme': {
        const ch = attrs.channel || null;
        const start = attrs.start || null;
        const stop = attrs.stop || null;
        const chNorm = ch ? norm(ch) : null;
        const include = keepAll ? false : (chNorm && allowedNorm.has(chNorm));
        curProg = { channel: ch, chNorm, start, stop, title: null, desc: null, category: null, icon: null, include };
        break;
      }
      case 'title':
      case 'desc':
      case 'category':
        if (curProg) curTextTag = name;
        break;
      default:
        break;
    }
  });

  parser.on('text', (txt) => {
    const t = txt?.trim?.();
    if (!t) return;
    if (curChannel && curTextTag === 'display-name') {
      if (!curChannel.name) curChannel.name = t;
    } else if (curProg && curTextTag) {
      if (curTextTag === 'title' && !curProg.title) curProg.title = t;
      else if (curTextTag === 'desc' && !curProg.desc) curProg.desc = t;
      else if (curTextTag === 'category' && !curProg.category) curProg.category = t;
    }
  });

  parser.on('closetag', (nameRaw) => {
    const name = String(nameRaw || '').toLowerCase();
    if (name === 'display-name' || name === 'title' || name === 'desc' || name === 'category') {
      curTextTag = null;
    } else if (name === 'channel') {
      if (curChannel && curChannel.id) {
        const prev = channels.get(curChannel.id) || {};
        channels.set(curChannel.id, {
          name: curChannel.name || prev.name || curChannel.id,
          icon: curChannel.icon || prev.icon || null
        });
      }
      curChannel = null;
    } else if (name === 'programme') {
      if (curProg) {
        pushProgramme(curProg);
        curProg = null;
      }
      if (totalProgrammes >= limitProgrammes) {
        // Stop early if a limit was specified
        aborted = true;
        try { parser.close(); } catch {}
      }
    }
  });

  await new Promise((resolve, reject) => {
    parser.on('error', reject);
    parser.on('end', resolve);
    input.on('error', reject);
    input.pipe(parser);
  });

  // Sort schedules per channel
  for (const id of Object.keys(schedules)) {
    schedules[id].sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
  }

  return { channels, schedules, totalProgrammes };
}
