import { XMLParser } from 'fast-xml-parser';

export function findEpgUrlInHeader(attrs = {}) {
  const keys = Object.keys(attrs || {});
  const byPriority = ['url-tvg', 'x-tvg-url', 'tvg-url', 'epg', 'epg-url'];
  for (const k of byPriority) {
    if (attrs[k]) return attrs[k];
  }
  for (const k of keys) {
    const v = attrs[k];
    if (typeof v === 'string' && /^https?:\/\//i.test(v) && v.toLowerCase().includes('xml')) return v;
  }
  return null;
}

function firstText(val) {
  if (val == null) return null;
  const arr = Array.isArray(val) ? val : [val];
  for (const v of arr) {
    if (typeof v === 'string') return v;
    if (v && typeof v === 'object') {
      if (typeof v['#text'] === 'string') return v['#text'];
      // Some feeds use _ or text keys; be defensive
      if (typeof v.text === 'string') return v.text;
    }
  }
  return null;
}

export function parseXmlTv(text) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '',
    trimValues: true,
    allowBooleanAttributes: true,
    parseTagValue: true,
    parseAttributeValue: false,
    removeNSPrefix: true,
    // Do NOT make 'tv' an array; keep channel/programme as arrays
    isArray: (tagName) => ['channel', 'programme', 'display-name', 'title', 'desc', 'category', 'icon'].includes(tagName)
  });
  const doc = parser.parse(text || '');
  const tv = doc.tv || {};

  // Channels
  const channels = {};
  const channelNodes = Array.isArray(tv.channel) ? tv.channel : (tv.channel ? [tv.channel] : []);
  for (const ch of channelNodes) {
    const id = ch.id || null;
    if (!id) continue;
    const name = firstText(ch['display-name']) || id;
    let icon = null;
    const icons = ch.icon || [];
    if (Array.isArray(icons) && icons.length) {
      const ic = icons[0];
      if (ic && typeof ic === 'object' && ic.src) icon = ic.src;
    }
    channels[id] = { name, icon };
  }

  // Programmes
  const programmes = [];
  let programmeNodes = Array.isArray(tv.programme) ? tv.programme : (tv.programme ? [tv.programme] : []);
  // Be tolerant to alternative tag naming or casing
  if (!programmeNodes.length) {
    const altKeys = Object.keys(tv).filter(k => k.toLowerCase() === 'programme' || k.toLowerCase() === 'program');
    for (const k of altKeys) {
      const v = tv[k];
      if (Array.isArray(v)) programmeNodes = programmeNodes.concat(v);
      else if (v) programmeNodes.push(v);
    }
  }
  for (const p of programmeNodes) {
    const channel = p.channel;
    const start = p.start;
    const stop = p.stop || null;
    const title = firstText(p.title);
    const desc = firstText(p.desc);
    const category = firstText(p.category);
    let icon = null;
    const iconRaw = p.icon || [];
    if (Array.isArray(iconRaw) && iconRaw.length) {
      const ic = iconRaw[0];
      if (ic && typeof ic === 'object' && ic.src) icon = ic.src;
    }
    programmes.push({ channel, start, stop, title, desc, category, icon });
  }

  return { channels, programmes };
}

export function xmltvTimeToIso(xmltv) {
  if (!xmltv) return null;
  // Formats: YYYYMMDDHHMMSS ZZZZ or YYYYMMDDHHMMSSZ or no TZ
  const m = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+\-]\d{4}|Z))?$/.exec(xmltv);
  if (!m) return null;
  const [_, Y, Mo, D, H, Mi, S, Z] = m;
  let offset = 'Z';
  if (Z && Z !== 'Z') {
    const sign = Z.startsWith('-') ? '-' : '+';
    const hh = Z.slice(1, 3);
    const mm = Z.slice(3, 5);
    offset = `${sign}${hh}:${mm}`;
  } else if (Z === 'Z') {
    offset = 'Z';
  }
  const iso = `${Y}-${Mo}-${D}T${H}:${Mi}:${S}${offset}`;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  return d.toISOString();
}

export function isoToXmltvTime(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  const Y = d.getUTCFullYear().toString().padStart(4, '0');
  const Mo = (d.getUTCMonth() + 1).toString().padStart(2, '0');
  const D = d.getUTCDate().toString().padStart(2, '0');
  const H = d.getUTCHours().toString().padStart(2, '0');
  const Mi = d.getUTCMinutes().toString().padStart(2, '0');
  const S = d.getUTCSeconds().toString().padStart(2, '0');
  return `${Y}${Mo}${D}${H}${Mi}${S} +0000`;
}
