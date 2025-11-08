import fs from 'node:fs';
import path from 'node:path';

const cacheDir = path.join(process.cwd(), 'epg-viewer', 'data', 'cache', 'schedules');
function ensureDir() { if (!fs.existsSync(cacheDir)) fs.mkdirSync(cacheDir, { recursive: true }); }

const mem = new Map(); // key -> { at, ttl, data }

export function getCache(key) {
  const v = mem.get(key);
  if (v && Date.now() - v.at < v.ttl) return v.data;
  // disk fallback
  ensureDir();
  const p = path.join(cacheDir, `${key}.json`);
  if (!fs.existsSync(p)) return null;
  try {
    const raw = JSON.parse(fs.readFileSync(p, 'utf8'));
    if (raw && raw.at && raw.ttl && (Date.now() - raw.at < raw.ttl)) {
      mem.set(key, { at: raw.at, ttl: raw.ttl, data: raw.data });
      return raw.data;
    }
  } catch {}
  return null;
}

export function setCache(key, data, ttlMs) {
  const ttl = Math.max(1000, ttlMs|0);
  const at = Date.now();
  mem.set(key, { at, ttl, data });
  ensureDir();
  const p = path.join(cacheDir, `${key}.json`);
  try { fs.writeFileSync(p, JSON.stringify({ at, ttl, data })); } catch {}
}

