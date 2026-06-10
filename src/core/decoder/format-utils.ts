/**
 * Shared formatting helpers used by the RowBinary and Native decoders.
 */

/**
 * Format eight 16-bit groups as a canonical (RFC 5952) IPv6 string: lowercase
 * hex, no leading zeros, and the longest run of consecutive zero groups (length
 * >= 2, leftmost on ties) collapsed to "::".
 */
export function formatIPv6(groups: number[]): string {
  let bestStart = -1;
  let bestLen = 0;
  let curStart = -1;
  let curLen = 0;
  for (let i = 0; i < groups.length; i++) {
    if (groups[i] === 0) {
      if (curStart === -1) curStart = i;
      curLen++;
      if (curLen > bestLen) {
        bestLen = curLen;
        bestStart = curStart;
      }
    } else {
      curStart = -1;
      curLen = 0;
    }
  }

  if (bestLen < 2) {
    return groups.map((g) => g.toString(16)).join(':');
  }

  const head = groups.slice(0, bestStart).map((g) => g.toString(16));
  const tail = groups.slice(bestStart + bestLen).map((g) => g.toString(16));
  return `${head.join(':')}::${tail.join(':')}`;
}
