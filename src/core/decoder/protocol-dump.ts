import { ProtocolCapture } from './protocol-decoder';

/**
 * Parser for the `.chproto` capture dump format written by the proxy harness
 * (scripts/native-proxy.mjs). The format is:
 *
 *   magic     "CHPROTO1"   (8 bytes ASCII)
 *   metaLen   u32 LE
 *   meta      metaLen bytes (UTF-8 JSON)
 *   segments  repeated: [dir u8][len u32 LE][len bytes]
 *
 * where dir 0 = client→server, 1 = server→client. Segments of the same
 * direction are concatenated into one contiguous stream (a packet may be split
 * across TCP segments, so each direction must be decoded as one buffer).
 */
const MAGIC = 'CHPROTO1';
const DIR_C2S = 0;
const DIR_S2C = 1;
const TEXT_DECODER = new TextDecoder();

export function parseChprotoDump(buf: Uint8Array): ProtocolCapture {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const magic = TEXT_DECODER.decode(buf.subarray(0, MAGIC.length));
  if (magic !== MAGIC) {
    throw new Error('not a CHPROTO dump (bad magic)');
  }
  let pos = MAGIC.length;
  const metaLen = view.getUint32(pos, true);
  pos += 4;
  const meta = JSON.parse(TEXT_DECODER.decode(buf.subarray(pos, pos + metaLen))) as Record<string, unknown>;
  pos += metaLen;

  const c2sChunks: Uint8Array[] = [];
  const s2cChunks: Uint8Array[] = [];
  while (pos < buf.length) {
    const dir = view.getUint8(pos);
    pos += 1;
    const len = view.getUint32(pos, true);
    pos += 4;
    const chunk = buf.subarray(pos, pos + len);
    pos += len;
    if (dir === DIR_C2S) c2sChunks.push(chunk);
    else if (dir === DIR_S2C) s2cChunks.push(chunk);
    else throw new Error(`unknown segment direction ${dir}`);
  }

  return { c2s: concat(c2sChunks), s2c: concat(s2cChunks), meta };
}

function concat(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}
