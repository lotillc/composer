/**
 * Size of a value as the default payload converter writes it: UTF-8 bytes of its JSON.
 *
 * Shared by the workflow factory, which budgets what a run carries forward, and the client,
 * which limits what callers may add to it, so both count the same bytes.
 *
 * Kept in a leaf module with no imports: the workflow factory runs in Temporal's
 * deterministic sandbox and bundles whatever it imports.
 *
 * @module json-size
 */

/** UTF-8 byte length of a string, without TextEncoder or Buffer. */
export function utf8Length(text: string): number {
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i);
    if (unit < 0x80) bytes += 1;
    else if (unit < 0x800) bytes += 2;
    else if (unit >= 0xd800 && unit < 0xdc00 && i + 1 < text.length) {
      bytes += 4; // a surrogate pair is one 4-byte code point
      i++;
    } else bytes += 3;
  }
  return bytes;
}

/** UTF-8 bytes of a string once written as a JSON string, escapes included. */
export function jsonStringBytes(text: string): number {
  return utf8Length(JSON.stringify(text));
}
