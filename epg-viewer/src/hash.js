import crypto from 'node:crypto';

export function stableStringify(obj) {
  return JSON.stringify(sortObj(obj));
}

function sortObj(o) {
  if (Array.isArray(o)) return o.map(sortObj);
  if (o && typeof o === 'object') {
    const out = {};
    for (const k of Object.keys(o).sort()) out[k] = sortObj(o[k]);
    return out;
  }
  return o;
}

export function sha1hex(s) {
  return crypto.createHash('sha1').update(String(s)).digest('hex');
}

