import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { Readable } from 'node:stream';

const mirrorDir = path.join(process.cwd(), 'epg-viewer', 'data', 'mirror');
function ensureDir() {
  if (!fs.existsSync(mirrorDir)) fs.mkdirSync(mirrorDir, { recursive: true });
}

function sha1(s) {
  return crypto.createHash('sha1').update(String(s)).digest('hex');
}

export function mirrorKeyFor(url) {
  return sha1(url);
}

export function mirrorPaths(url) {
  ensureDir();
  const key = mirrorKeyFor(url);
  const base = path.join(mirrorDir, key);
  return {
    base,
    gz: base + '.xmltv.gz',
    xml: base + '.xml',
    meta: base + '.json'
  };
}

export async function mirrorFetch(url) {
  ensureDir();
  const paths = mirrorPaths(url);
  let prev = {};
  try { prev = JSON.parse(fs.readFileSync(paths.meta, 'utf8')); } catch {}
  const headers = { 'User-Agent': 'Mozilla/5.0 (epg-viewer/0.1)' };
  if (prev.etag) headers['If-None-Match'] = prev.etag;
  if (prev.lastModified) headers['If-Modified-Since'] = prev.lastModified;
  let res = await fetch(url, { headers });
  // Retry on 5xx with a fresh, unconditional fetch
  if (!res.ok && res.status >= 500) {
    try { res.body?.cancel?.(); } catch {}
    await new Promise(r => setTimeout(r, 500));
    res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (epg-viewer/0.1)' } });
  }

  if (res.status === 304) {
    // unchanged
    const isGz = prev.isGz ?? url.endsWith('.gz');
    const file = isGz ? paths.gz : paths.xml;
    if (!fs.existsSync(file)) {
      // We rotated the previous file to a snapshot; refetch without conditionals
      const fresh = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (epg-viewer/0.1)' } });
      if (!fresh.ok) throw new Error(`Mirror fetch (fresh) failed ${url}: ${fresh.status} ${fresh.statusText}`);
      return await forceDownload(url, paths, fresh);
    }
    return { path: file, isGz, etag: prev.etag || null, lastModified: prev.lastModified || null, changed: false };
  }
  if (!res.ok) throw new Error(`Mirror fetch failed ${url}: ${res.status} ${res.statusText}`);
  return await forceDownload(url, paths, res);
}

function tsStamp(d = new Date()) {
  const pad = (n, w=2) => String(n).padStart(w, '0');
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth()+1)}${pad(d.getUTCDate())}${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
}

export function listSnapshots(url) {
  ensureDir();
  const key = mirrorKeyFor(url);
  const files = fs.readdirSync(mirrorDir).filter(f => f.startsWith(key + '.') && (f.endsWith('.xmltv.gz') || f.endsWith('.xml')));
  const out = [];
  for (const f of files) {
    const full = path.join(mirrorDir, f);
    const m = new RegExp(`^${key}\\.(\\d{14})\\.(xmltv\\.gz|xml)$`).exec(f);
    let savedAt = null;
    if (m) {
      const s = m[1];
      const iso = `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}T${s.slice(8,10)}:${s.slice(10,12)}:${s.slice(12,14)}Z`;
      savedAt = new Date(iso);
    } else {
      try { savedAt = fs.statSync(full).mtime; } catch { savedAt = new Date(0); }
    }
    out.push({ path: full, savedAt: savedAt ? savedAt.getTime() : 0, isGz: f.endsWith('.gz') });
  }
  out.sort((a,b)=> b.savedAt - a.savedAt);
  return out;
}

function pruneSnapshots(url, retentionDays = 21, keepMax = 40) {
  const snaps = listSnapshots(url);
  const cutoff = Date.now() - Math.max(1, retentionDays|0) * 24*60*60*1000;
  let kept = 0;
  for (const s of snaps) {
    kept++;
    if (s.savedAt < cutoff || kept > keepMax) {
      try { fs.unlinkSync(s.path); } catch {}
    }
  }
}

async function forceDownload(url, paths, res) {
  const contentType = res.headers.get('content-type') || '';
  const encoding = res.headers.get('content-encoding') || '';
  const isGz = url.endsWith('.gz') || contentType.includes('gzip') || encoding.includes('gzip');
  const target = isGz ? paths.gz : paths.xml;
  // rotate previous file into timestamped snapshot
  try {
    if (fs.existsSync(target)) {
      const stamp = tsStamp(new Date());
      const snap = `${paths.base}.${stamp}.${isGz ? 'xmltv.gz' : 'xml'}`;
      fs.renameSync(target, snap);
    }
  } catch {}
  // write to temp then move
  const tmp = target + '.tmp';
  await new Promise(async (resolve, reject) => {
    try {
      const out = fs.createWriteStream(tmp);
      const r = res.body;
      let src = null;
      try {
        if (r && typeof r.getReader === 'function') src = Readable.fromWeb(r);
        else if (r && typeof r.pipe === 'function') src = r;
      } catch {}
      if (src && typeof src.pipe === 'function') {
        src.pipe(out);
      } else {
        // Fallback: read full body and write once
        const buf = Buffer.from(await res.arrayBuffer());
        out.end(buf);
      }
      out.on('finish', resolve);
      out.on('error', reject);
    } catch (e) { reject(e); }
  });
  fs.renameSync(tmp, target);
  const meta = {
    url,
    savedAt: new Date().toISOString(),
    etag: res.headers.get('etag') || null,
    lastModified: res.headers.get('last-modified') || null,
    isGz
  };
  fs.writeFileSync(paths.meta, JSON.stringify(meta, null, 2));
  // prune old snapshots
  pruneSnapshots(url);
  return { path: target, isGz, etag: meta.etag, lastModified: meta.lastModified, changed: true };
}
