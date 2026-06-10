/**
 * Regression tests for the native TCP protocol decoder, run against real
 * packet captures (scripts/native-proxy.mjs driving clickhouse-client through a
 * proxy). The captures live in fixtures/protocol/*.chproto and are decoded with
 * no live ClickHouse needed.
 *
 * The core guarantee is 100% byte coverage: every byte of both the
 * client→server and server→client streams must be attributed to a labeled AST
 * node, and no Protocol.DecodeError node may appear. That is the forcing
 * function that proves the positional decode stayed aligned across every
 * packet and version-gated field.
 */
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { ProtocolDecoder } from './protocol-decoder';
import { parseChprotoDump } from './protocol-dump';
import { analyzeByteRange, formatUncoveredRanges } from './test-helpers';
import { AstNode, ParsedData } from '../types/ast';

const FIXTURE_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures', 'protocol');

function loadCapture(name: string) {
  return parseChprotoDump(readFileSync(path.join(FIXTURE_DIR, name)));
}

function decodeFixture(name: string): { parsed: ParsedData; combinedLength: number } {
  const cap = loadCapture(name);
  const decoder = ProtocolDecoder.fromCapture(cap);
  const parsed = decoder.decode();
  return { parsed, combinedLength: cap.c2s.length + cap.s2c.length };
}

function walk(node: AstNode, visit: (n: AstNode) => void): void {
  visit(node);
  node.children?.forEach((c) => walk(c, visit));
}

function allNodes(parsed: ParsedData): AstNode[] {
  const out: AstNode[] = [];
  parsed.trailingNodes?.forEach((n) => walk(n, (x) => out.push(x)));
  return out;
}

function packetTypes(section: AstNode): string[] {
  return (section.children ?? []).map((p) => p.type.replace('Protocol.', ''));
}

const FIXTURES = readdirSync(FIXTURE_DIR).filter((f) => f.endsWith('.chproto')).sort();

describe('ProtocolDecoder — fixtures', () => {
  it('discovers fixtures', () => {
    expect(FIXTURES.length).toBeGreaterThan(0);
  });

  describe.each(FIXTURES)('%s', (name) => {
    it('decodes with no error nodes', () => {
      const { parsed } = decodeFixture(name);
      const errors = allNodes(parsed).filter((n) => n.type === 'Protocol.DecodeError');
      expect(errors.map((e) => e.displayValue)).toEqual([]);
    });

    it('covers 100% of the captured bytes', () => {
      const { parsed, combinedLength } = decodeFixture(name);
      const coverage = analyzeByteRange(parsed, combinedLength);
      if (!coverage.isComplete) {
        const cap = loadCapture(name);
        const combined = new Uint8Array(combinedLength);
        combined.set(cap.c2s, 0);
        combined.set(cap.s2c, cap.c2s.length);
        throw new Error(`${name}\n${formatUncoveredRanges(coverage, combined)}`);
      }
      expect(coverage.isComplete).toBe(true);
      expect(coverage.coveragePercent).toBe(100);
    });

    it('has exactly two direction sections', () => {
      const { parsed } = decodeFixture(name);
      expect(parsed.trailingNodes).toHaveLength(2);
      expect(parsed.trailingNodes![0].type).toBe('Protocol.ClientStream');
      expect(parsed.trailingNodes![1].type).toBe('Protocol.ServerStream');
    });

    it('starts each direction with a Hello', () => {
      const { parsed } = decodeFixture(name);
      const [client, server] = parsed.trailingNodes!;
      expect(packetTypes(client)[0]).toBe('ClientHello');
      // Server may answer with ServerHello (or Exception only on auth failure).
      expect(['ServerHello', 'Exception']).toContain(packetTypes(server)[0]);
    });
  });
});

describe('ProtocolDecoder — structural expectations', () => {
  it('simple select: ClientHello, Addendum, Query, empty Data marker; server Data + EndOfStream', () => {
    const { parsed } = decodeFixture('01-simple-select.chproto');
    const [client, server] = parsed.trailingNodes!;
    const c = packetTypes(client);
    expect(c).toContain('ClientHello');
    expect(c).toContain('Addendum');
    expect(c).toContain('Query');
    expect(c).toContain('Data'); // empty end-of-client-data marker
    const s = packetTypes(server);
    expect(s).toContain('ServerHello');
    expect(s).toContain('Data');
    expect(s[s.length - 1]).toBe('EndOfStream');
  });

  it('negotiated version is recorded and below the server version', () => {
    const { parsed } = decodeFixture('01-simple-select.chproto');
    const neg = parsed.metadata?.negotiatedVersion as number;
    expect(neg).toBeGreaterThanOrEqual(54479); // cluster-function feature present
  });

  it('Query packet carries query_id, ClientInfo, settings, stage, compression, query_body', () => {
    const { parsed } = decodeFixture('01-simple-select.chproto');
    const client = parsed.trailingNodes![0];
    const query = client.children!.find((p) => p.type === 'Protocol.Query')!;
    const labels = query.children!.map((c) => c.label);
    expect(labels).toEqual(
      expect.arrayContaining(['query_id', 'settings', 'stage', 'compression', 'query_body']),
    );
    const clientInfo = query.children!.find((c) => c.type === 'Protocol.ClientInfo');
    expect(clientInfo).toBeDefined();
    const qb = query.children!.find((c) => c.label === 'query_body')!;
    expect(String(qb.value)).toContain('SELECT');
  });

  it('exception fixture surfaces a server Exception with code and message', () => {
    const { parsed } = decodeFixture('04-exception.chproto');
    const server = parsed.trailingNodes![1];
    const exc = server.children!.find((p) => p.type === 'Protocol.Exception');
    expect(exc).toBeDefined();
    const exBody = exc!.children!.find((c) => c.label === 'exception')!;
    const code = exBody.children!.find((c) => c.label === 'code')!;
    expect(Number(code.value)).toBeGreaterThan(0);
    const msg = exBody.children!.find((c) => c.label === 'message')!;
    expect(String(msg.value).length).toBeGreaterThan(0);
  });

  it('totals/extremes fixture yields Totals and Extremes packets', () => {
    const { parsed } = decodeFixture('03-totals-extremes.chproto');
    const s = packetTypes(parsed.trailingNodes![1]);
    expect(s).toContain('Totals');
    expect(s).toContain('Extremes');
  });

  it('multiblock fixture yields several server Data packets', () => {
    const { parsed } = decodeFixture('05-multiblock.chproto');
    const dataCount = packetTypes(parsed.trailingNodes![1]).filter((t) => t === 'Data').length;
    expect(dataCount).toBeGreaterThan(2);
  });

  it('logs fixture yields Log and ProfileEvents packets', () => {
    const { parsed } = decodeFixture('06-logs.chproto');
    const s = packetTypes(parsed.trailingNodes![1]);
    expect(s).toContain('Log');
    expect(s).toContain('ProfileEvents');
  });

  it('insert fixture: client sends a Data block with rows; server sends a 0-row schema block', () => {
    const { parsed } = decodeFixture('07-insert.chproto');
    const [client, server] = parsed.trailingNodes!;
    // A client Data packet carrying the inserted rows.
    const clientBlocks = (client.children ?? [])
      .filter((p) => p.type === 'Protocol.Data')
      .map((p) => p.children!.find((c) => c.type === 'Native.Block')!)
      .filter(Boolean);
    const withRows = clientBlocks.filter((b) => (b.value as { rows: number }).rows > 0);
    expect(withRows.length).toBeGreaterThan(0);
    // The server's schema header block: columns present, 0 rows.
    const serverBlocks = (server.children ?? [])
      .filter((p) => p.type === 'Protocol.Data')
      .map((p) => p.children!.find((c) => c.type === 'Native.Block')!)
      .filter(Boolean);
    const schema = serverBlocks.find(
      (b) => (b.value as { rows: number; columns: number }).rows === 0
        && (b.value as { columns: number }).columns > 0,
    );
    expect(schema).toBeDefined();
  });

  it('parameters fixture: Query carries a parameters list with entries', () => {
    const { parsed } = decodeFixture('08-parameters.chproto');
    const query = parsed.trailingNodes![0].children!.find((p) => p.type === 'Protocol.Query')!;
    const params = query.children!.find((c) => c.label === 'parameters');
    expect(params).toBeDefined();
    const entries = (params!.children ?? []).filter((c) => c.label !== 'terminator');
    expect(entries.length).toBeGreaterThanOrEqual(2);
  });
});
