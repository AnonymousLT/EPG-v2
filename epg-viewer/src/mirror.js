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
  const headers = { 'User-Agent': 'epg-viewer/0.1' };
  if (prev.etag) headers['If-None-Match'] = prev.etag;
  if (prev.lastModified) headers['If-Modified-Since'] = prev.lastModified;
  const res = await fetch(url, { headers });
  if (res.status === 304) {
    // unchanged
    const isGz = prev.isGz ?? url.endsWith('.gz');
    const file = isGz ? paths.gz : paths.xml;
    if (!fs.existsSync(file)) {
      // missing file, force re-download
      return await forceDownload(url, paths, res);
    }
    return { path: file, isGz, etag: prev.etag || null, lastModified: prev.lastModified || null, changed: false };
  }
  if (!res.ok) throw new Error(`Mirror fetch failed ${url}: ${res.status} ${res.statusText}`);
  return await forceDownload(url, paths, res);
}

async function forceDownload(url, paths, res) {
  const contentType = res.headers.get('content-type') || '';
  const encoding = res.headers.get('content-encoding') || '';
  const isGz = url.endsWith('.gz') || contentType.includes('gzip') || encoding.includes('gzip');
  const target = isGz ? paths.gz : paths.xml;
  // write to temp then move
  const tmp = target + '.tmp';
  await new Promise(async (resolve, reject) => {
    try {
      const out = fs.createWriteStream(tmp);
      const r = res.body;
      const src = r && r.getReader ? Readable.fromWeb(res.body) : res.body; // fetch body is web stream
      if (src.pipe) src.pipe(out); else out.end(Buffer.from(await res.arrayBuffer()));
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
  return { path: target, isGz, etag: meta.etag, lastModified: meta.lastModified, changed: true };
}
