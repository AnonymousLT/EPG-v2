// M3U/M3U8 playlist parser focused on IPTV-style EXTINF metadata

function parseAttributes(str) {
  const attrs = {};
  if (!str) return attrs;
  const re = /([A-Za-z0-9_\-]+)\s*=\s*"([^"]*)"/g;
  let m;
  while ((m = re.exec(str)) !== null) {
    attrs[m[1].toLowerCase()] = m[2];
  }
  return attrs;
}

export function parsePlaylist(text) {
  const lines = (text || '').split(/\r?\n/);
  const channels = [];
  let headerAttrs = {};

  // parse header
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i].trim();
    if (!l) continue;
    if (l.startsWith('#EXTM3U')) {
      headerAttrs = parseAttributes(l.substring('#EXTM3U'.length));
      break;
    }
    // if first non-empty not EXTM3U, stop
    break;
  }

  // guess header attrs anywhere in the text for url-tvg variants
  const guessedHeaderAttrs = {};
  const guesses = [
    /(?:url-tvg|x-tvg-url|tvg-url)\s*=\s*"([^"]+)"/i,
    /(https?:\/\/[^\s"']+\.xml(?:\.gz)?)/i
  ];
  for (const re of guesses) {
    const m = text.match(re);
    if (m && m[1]) {
      if (!guessedHeaderAttrs['url-tvg']) guessedHeaderAttrs['url-tvg'] = m[1];
      break;
    }
  }

  let pending = null; // { id, name, group, logo }
  let pendingGrp = null;
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const l = raw.trim();
    if (!l) continue;
    if (l.startsWith('#EXTINF')) {
      const after = l.substring(l.indexOf(':') + 1);
      const commaIdx = after.indexOf(',');
      const meta = commaIdx >= 0 ? after.substring(0, commaIdx) : after;
      const displayName = commaIdx >= 0 ? after.substring(commaIdx + 1).trim() : '';
      const attrs = parseAttributes(meta);
      const id = attrs['tvg-id'] || attrs['channel-id'] || attrs['id'] || null;
      const name = attrs['tvg-name'] || displayName || null;
      const logo = attrs['tvg-logo'] || attrs['logo'] || null;
      const group = attrs['group-title'] || pendingGrp || null;
      pending = { id, name, group, logo, url: null };
      pendingGrp = null;
      continue;
    }
    if (l.startsWith('#EXTGRP')) {
      const grp = l.substring('#EXTGRP'.length).trim();
      pendingGrp = grp || pendingGrp;
      continue;
    }
    if (l.startsWith('#')) continue; // comment/other tags

    // first non-comment line after EXTINF is the URL
    if (pending) {
      pending.url = l;
      channels.push(pending);
      pending = null;
    }
  }

  return { headerAttrs, guessedHeaderAttrs, channels };
}

