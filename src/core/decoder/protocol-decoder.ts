import { BinaryReader } from './reader';
import { decodeLEB128 } from './leb128';
import { NativeDecoder } from './native-decoder';
import { AstNode, BlockNode, ByteRange, HeaderNode, ParsedData } from '../types/ast';
import { ClickHouseFormat } from '../types/formats';

const TEXT_DECODER = new TextDecoder();

/**
 * Protocol feature gates, keyed by the protocol version that introduced them.
 * A feature is active when the negotiated version is >= its value. See
 * docs/full_native_protocol_spec.md (the feature table).
 */
const F = {
  BLOCK_INFO: 51903,
  TIMEZONE: 54058,
  QUOTA_KEY_IN_CLIENT_INFO: 54060,
  DISPLAY_NAME: 54372,
  VERSION_PATCH: 54401,
  WRITE_CLIENT_INFO: 54420,
  SETTINGS_AS_STRINGS: 54429,
  INTERSERVER_SECRET: 54441,
  OPEN_TELEMETRY: 54442,
  DISTRIBUTED_DEPTH: 54448,
  INITIAL_QUERY_START_TIME: 54449,
  PARALLEL_REPLICAS: 54453,
  ADDENDUM: 54458,
  PARAMETERS: 54459,
  SERVER_QUERY_TIME_IN_PROGRESS: 54460,
  PASSWORD_COMPLEXITY_RULES: 54461,
  INTERSERVER_SECRET_V2: 54462,
  TOTAL_BYTES_IN_PROGRESS: 54463,
  TIMEZONE_UPDATES: 54464,
  ROWS_BEFORE_AGGREGATION: 54469,
  CHUNKED_PROTOCOL: 54470,
  VERSIONED_PARALLEL_REPLICAS: 54471,
  INTERSERVER_EXTERNALLY_GRANTED_ROLES: 54472,
  SERVER_SETTINGS: 54474,
  QUERY_AND_LINE_NUMBERS: 54475,
  JWT_IN_INTERSERVER: 54476,
  QUERY_PLAN_SERIALIZATION: 54477,
  VERSIONED_CLUSTER_FUNCTION: 54479,
} as const;

/** Client → Server packet type codes. */
const ClientPacket = {
  Hello: 0,
  Query: 1,
  Data: 2,
  Cancel: 3,
  Ping: 4,
  SSHChallengeRequest: 11,
  SSHChallengeResponse: 12,
} as const;

/** Server → Client packet type codes. */
const ServerPacket = {
  Hello: 0,
  Data: 1,
  Exception: 2,
  Progress: 3,
  Pong: 4,
  EndOfStream: 5,
  ProfileInfo: 6,
  Totals: 7,
  Extremes: 8,
  Log: 10,
  TableColumns: 11,
  ProfileEvents: 14,
  TimezoneUpdate: 17,
  SSHChallenge: 18,
} as const;

/**
 * Server packet codes that can legitimately appear as the *first* packet after
 * ServerHello. Used to absorb undocumented trailing version VarUInts that some
 * server builds append to ServerHello (see decodeServerHello): a trailing
 * VarUInt whose value is not in this set cannot be a packet start, so it is a
 * hello field.
 */
const VALID_FIRST_SERVER_PACKET = new Set<number>([
  ServerPacket.Data,
  ServerPacket.Exception,
  ServerPacket.Progress,
  ServerPacket.EndOfStream,
  ServerPacket.ProfileInfo,
  ServerPacket.Totals,
  ServerPacket.Extremes,
  ServerPacket.Log,
  ServerPacket.TableColumns,
  ServerPacket.ProfileEvents,
  ServerPacket.TimezoneUpdate,
]);

export interface ProtocolCapture {
  /** Concatenated client → server byte stream. */
  c2s: Uint8Array;
  /** Concatenated server → client byte stream. */
  s2c: Uint8Array;
  meta?: Record<string, unknown>;
}

/**
 * Decoder for the ClickHouse native TCP protocol. It consumes a capture of one
 * connection's two per-direction byte streams (as produced by the proxy
 * harness), concatenates them into a single buffer for the hex viewer, and
 * produces an AstNode tree: two top-level "stream" nodes (client→server,
 * server→client) each containing one node per packet, with packet fields and —
 * for Data-family packets — the full Native Block subtree (reused from
 * NativeDecoder) nested underneath.
 *
 * Compression and TLS are out of scope: captures are expected to be plaintext,
 * uncompressed (localhost clickhouse-client disables compression by default).
 */
export class ProtocolDecoder {
  readonly format = ClickHouseFormat.NativeProtocol;
  private readonly combined: Uint8Array;
  private readonly c2sLength: number;
  private readonly total: number;
  private readonly meta?: Record<string, unknown>;
  private readonly negotiated: number;
  private readonly native: NativeDecoder;
  private readonly r: BinaryReader;
  private idCounter = 0;
  private blockIndex = 0;

  /**
   * @param combined  the concatenated [c2s][s2c] byte buffer (rawData for the hex viewer)
   * @param c2sLength byte length of the client→server portion (the split point)
   */
  constructor(combined: Uint8Array, c2sLength: number, meta?: Record<string, unknown>) {
    this.combined = combined;
    this.c2sLength = c2sLength;
    this.total = combined.length;
    this.meta = meta;
    this.negotiated = this.computeNegotiatedVersion();
    // NativeDecoder reads `combined`; we drive its reader for all framing too,
    // so packet-framing reads and block decoding share one offset cursor.
    this.native = new NativeDecoder(combined, this.negotiated);
    this.r = this.native.sharedReader;
  }

  /** Build a ProtocolDecoder from a capture object (separate c2s / s2c). */
  static fromCapture(capture: ProtocolCapture): ProtocolDecoder {
    const combined = new Uint8Array(capture.c2s.length + capture.s2c.length);
    combined.set(capture.c2s, 0);
    combined.set(capture.s2c, capture.c2s.length);
    return new ProtocolDecoder(combined, capture.c2s.length, capture.meta);
  }

  decode(): ParsedData {
    const clientPackets = this.decodeClientStream();
    const serverPackets = this.decodeServerStream();

    const clientSection: AstNode = {
      id: this.nid(),
      type: 'Protocol.ClientStream',
      byteRange: { start: 0, end: this.c2sLength },
      value: clientPackets.length,
      displayValue: `client → server · ${clientPackets.length} packet(s) · ${this.c2sLength}B`,
      label: 'client → server',
      children: clientPackets,
    };
    const serverSection: AstNode = {
      id: this.nid(),
      type: 'Protocol.ServerStream',
      byteRange: { start: this.c2sLength, end: this.total },
      value: serverPackets.length,
      displayValue: `server → client · ${serverPackets.length} packet(s) · ${this.total - this.c2sLength}B`,
      label: 'server → client',
      children: serverPackets,
    };

    return {
      format: this.format,
      header: this.emptyHeader(),
      totalBytes: this.total,
      trailingNodes: [clientSection, serverSection],
      metadata: { negotiatedVersion: this.negotiated, ...this.meta },
    };
  }

  // --- stream loops -------------------------------------------------------

  private decodeClientStream(): AstNode[] {
    const packets: AstNode[] = [];
    if (this.c2sLength === 0) return packets;

    // 1. ClientHello is always first.
    packets.push(this.guard(() => this.decodeClientHello(), 'client', packets));
    // 2. Addendum (no packet type byte), gated by the negotiated version.
    if (this.negotiated >= F.ADDENDUM && this.r.offset < this.c2sLength) {
      packets.push(this.guard(() => this.decodeAddendum(), 'client', packets));
    }
    // 3. Remaining client packets (Query, Data, Ping, Cancel, ...).
    while (this.r.offset < this.c2sLength) {
      const before = this.r.offset;
      packets.push(this.guard(() => this.decodeClientPacket(), 'client', packets));
      if (this.r.offset <= before) break; // no progress: stop to avoid a loop
    }
    return packets;
  }

  private decodeServerStream(): AstNode[] {
    const packets: AstNode[] = [];
    if (this.total - this.c2sLength === 0) return packets;
    while (this.r.offset < this.total) {
      const before = this.r.offset;
      packets.push(this.guard(() => this.decodeServerPacket(), 'server', packets));
      if (this.r.offset <= before) break;
    }
    return packets;
  }

  /**
   * Run one packet decode; on failure, emit an error node spanning the rest of
   * the current direction and stop that stream. Keeps the UI usable on a
   * partially-understood capture while letting tests assert zero error nodes.
   */
  private guard(fn: () => AstNode, dir: 'client' | 'server', _packets: AstNode[]): AstNode {
    const start = this.r.offset;
    try {
      return fn();
    } catch (err) {
      const end = dir === 'client' ? this.c2sLength : this.total;
      // Consume the remainder so the stream loop terminates.
      this.r.skip(Math.max(0, end - this.r.offset));
      return {
        id: this.nid(),
        type: 'Protocol.DecodeError',
        byteRange: { start, end },
        value: String(err instanceof Error ? err.message : err),
        displayValue: `decode error: ${err instanceof Error ? err.message : err}`,
        label: 'error',
      };
    }
  }

  // --- client packets -----------------------------------------------------

  private decodeClientHello(): AstNode {
    const start = this.r.offset;
    const children: AstNode[] = [];
    children.push(this.typeNode('ClientHello', ClientPacket.Hello));
    children.push(this.str('client_name').node);
    children.push(this.vu('version_major').node);
    children.push(this.vu('version_minor').node);
    children.push(this.vu('protocol_version').node);
    children.push(this.str('database').node);
    children.push(this.str('user').node);
    children.push(this.str('password').node);
    return this.packet('ClientHello', start, children);
  }

  private decodeAddendum(): AstNode {
    const start = this.r.offset;
    const children: AstNode[] = [];
    // The Addendum has no packet type byte — fields go raw on the wire.
    children.push(this.str('quota_key').node);
    if (this.negotiated >= F.CHUNKED_PROTOCOL) {
      children.push(this.str('proto_send_chunked').node);
      children.push(this.str('proto_recv_chunked').node);
    }
    if (this.negotiated >= F.VERSIONED_PARALLEL_REPLICAS) {
      children.push(this.vu('parallel_replicas_protocol_version').node);
    }
    if (this.negotiated >= F.VERSIONED_CLUSTER_FUNCTION) {
      children.push(this.vu('cluster_function_protocol_version').node);
    }
    return this.packet('Addendum', start, children);
  }

  private decodeClientPacket(): AstNode {
    const start = this.r.offset;
    const { value: type } = this.peekTypeOrThrow();
    switch (type) {
      case ClientPacket.Query:
        return this.decodeQuery(start);
      case ClientPacket.Data:
        return this.decodeDataPacket('Data', ClientPacket.Data, start);
      case ClientPacket.Cancel:
        return this.bodylessPacket('Cancel', ClientPacket.Cancel, start);
      case ClientPacket.Ping:
        return this.bodylessPacket('Ping', ClientPacket.Ping, start);
      case ClientPacket.SSHChallengeRequest:
        return this.bodylessPacket('SSHChallengeRequest', ClientPacket.SSHChallengeRequest, start);
      case ClientPacket.SSHChallengeResponse: {
        const children = [this.typeNode('SSHChallengeResponse', type), this.str('signature').node];
        return this.packet('SSHChallengeResponse', start, children);
      }
      default:
        throw new Error(`unsupported client packet type ${type} at offset ${start}`);
    }
  }

  private decodeQuery(start: number): AstNode {
    const children: AstNode[] = [];
    children.push(this.typeNode('Query', ClientPacket.Query));
    children.push(this.str('query_id').node);
    if (this.negotiated >= F.WRITE_CLIENT_INFO) {
      children.push(this.decodeClientInfo());
    }
    if (this.negotiated >= F.SETTINGS_AS_STRINGS) {
      children.push(this.decodeSettingsList('settings'));
    } else {
      throw new Error(
        `Query settings below v${F.SETTINGS_AS_STRINGS} (binary settings) are not supported; negotiated ${this.negotiated}`,
      );
    }
    if (this.negotiated >= F.INTERSERVER_EXTERNALLY_GRANTED_ROLES) {
      children.push(this.str('external_roles').node);
    }
    if (this.negotiated >= F.INTERSERVER_SECRET) {
      children.push(this.str('cluster_secret').node);
    }
    children.push(this.vu('stage').node);
    children.push(this.vu('compression').node);
    children.push(this.str('query_body').node);
    if (this.negotiated >= F.PARAMETERS) {
      children.push(this.decodeSettingsList('parameters'));
    }
    return this.packet('Query', start, children);
  }

  private decodeClientInfo(): AstNode {
    const start = this.r.offset;
    const children: AstNode[] = [];
    const queryKindRes = this.u8('query_kind');
    children.push(queryKindRes.node);
    const queryKind = queryKindRes.value;
    children.push(this.str('initial_user').node);
    children.push(this.str('initial_query_id').node);
    children.push(this.str('initial_address').node);
    if (this.negotiated >= F.INITIAL_QUERY_START_TIME) {
      children.push(this.i64('initial_time').node); // fixed-width 8 bytes
    }
    const ifaceRes = this.u8('query_interface');
    children.push(ifaceRes.node);
    const iface = ifaceRes.value;
    const isTcp = iface === 1;
    if (isTcp) {
      children.push(this.str('os_user').node);
      children.push(this.str('client_hostname').node);
      children.push(this.str('client_name').node);
      children.push(this.vu('client_version_major').node);
      children.push(this.vu('client_version_minor').node);
      children.push(this.vu('client_protocol_version').node);
    }
    if (this.negotiated >= F.QUOTA_KEY_IN_CLIENT_INFO) {
      children.push(this.str('quota_key').node);
    }
    if (this.negotiated >= F.DISTRIBUTED_DEPTH) {
      children.push(this.vu('distributed_depth').node);
    }
    if (this.negotiated >= F.VERSION_PATCH && isTcp) {
      children.push(this.vu('client_version_patch').node);
    }
    if (this.negotiated >= F.OPEN_TELEMETRY) {
      children.push(this.decodeOpenTelemetry());
    }
    if (this.negotiated >= F.PARALLEL_REPLICAS) {
      children.push(this.vu('collaborate_with_initiator').node);
      children.push(this.vu('count_participating_replicas').node);
      children.push(this.vu('number_of_current_replica').node);
    }
    if (this.negotiated >= F.QUERY_AND_LINE_NUMBERS) {
      children.push(this.vu('script_query_number').node);
      children.push(this.vu('script_line_number').node);
    }
    if (this.negotiated >= F.JWT_IN_INTERSERVER) {
      const jwtRes = this.u8('jwt_present');
      children.push(jwtRes.node);
      if (jwtRes.value === 1) {
        children.push(this.str('jwt').node);
      }
    }
    return this.group('ClientInfo', start, children, `query_kind=${queryKind}, interface=${iface}`);
  }

  private decodeOpenTelemetry(): AstNode {
    const start = this.r.offset;
    const children: AstNode[] = [];
    const hasTraceRes = this.u8('has_trace');
    children.push(hasTraceRes.node);
    if (hasTraceRes.value === 1) {
      children.push(this.fixedBytes(16, 'trace_id', 'UInt128'));
      children.push(this.fixedBytes(8, 'span_id', 'UInt64'));
      children.push(this.str('trace_state').node);
      children.push(this.u8('trace_flags').node);
    }
    return this.group('OpenTelemetry', start, children, hasTraceRes.value === 1 ? 'trace present' : 'no trace');
  }

  // --- server packets -----------------------------------------------------

  private decodeServerPacket(): AstNode {
    const start = this.r.offset;
    const { value: type } = this.peekTypeOrThrow();
    switch (type) {
      case ServerPacket.Hello:
        return this.decodeServerHello(start);
      case ServerPacket.Data:
        return this.decodeDataPacket('Data', ServerPacket.Data, start);
      case ServerPacket.Exception:
        return this.decodeException(start);
      case ServerPacket.Progress:
        return this.decodeProgress(start);
      case ServerPacket.Pong:
        return this.bodylessPacket('Pong', ServerPacket.Pong, start);
      case ServerPacket.EndOfStream:
        return this.bodylessPacket('EndOfStream', ServerPacket.EndOfStream, start);
      case ServerPacket.ProfileInfo:
        return this.decodeProfileInfo(start);
      case ServerPacket.Totals:
        return this.decodeDataPacket('Totals', ServerPacket.Totals, start);
      case ServerPacket.Extremes:
        return this.decodeDataPacket('Extremes', ServerPacket.Extremes, start);
      case ServerPacket.Log:
        return this.decodeDataPacket('Log', ServerPacket.Log, start);
      case ServerPacket.TableColumns:
        return this.decodeTableColumns(start);
      case ServerPacket.ProfileEvents:
        return this.decodeDataPacket('ProfileEvents', ServerPacket.ProfileEvents, start);
      case ServerPacket.TimezoneUpdate: {
        const children = [this.typeNode('TimezoneUpdate', type), this.str('timezone').node];
        return this.packet('TimezoneUpdate', start, children);
      }
      case ServerPacket.SSHChallenge: {
        const children = [this.typeNode('SSHChallenge', type), this.str('challenge').node];
        return this.packet('SSHChallenge', start, children);
      }
      default:
        throw new Error(`unsupported server packet type ${type} at offset ${start}`);
    }
  }

  private decodeServerHello(start: number): AstNode {
    const children: AstNode[] = [];
    children.push(this.typeNode('ServerHello', ServerPacket.Hello));
    children.push(this.str('server_name').node);
    children.push(this.vu('version_major').node);
    children.push(this.vu('version_minor').node);
    children.push(this.vu('protocol_version').node);
    if (this.negotiated >= F.VERSIONED_PARALLEL_REPLICAS) {
      // Wire position: immediately after protocol_version, before timezone.
      children.push(this.vu('parallel_replicas_protocol_version').node);
    }
    if (this.negotiated >= F.TIMEZONE) {
      children.push(this.str('timezone').node);
    }
    if (this.negotiated >= F.DISPLAY_NAME) {
      children.push(this.str('display_name').node);
    }
    if (this.negotiated >= F.VERSION_PATCH) {
      children.push(this.vu('version_patch').node);
    }
    if (this.negotiated >= F.CHUNKED_PROTOCOL) {
      children.push(this.str('proto_send_chunked_srv').node);
      children.push(this.str('proto_recv_chunked_srv').node);
    }
    if (this.negotiated >= F.PASSWORD_COMPLEXITY_RULES) {
      children.push(this.decodePasswordRules());
    }
    if (this.negotiated >= F.INTERSERVER_SECRET_V2) {
      children.push(this.u64('nonce').node);
    }
    if (this.negotiated >= F.SERVER_SETTINGS) {
      children.push(this.decodeSettingsList('server_settings'));
    }
    if (this.negotiated >= F.QUERY_PLAN_SERIALIZATION) {
      children.push(this.vu('query_plan_serialization_version').node);
    }
    if (this.negotiated >= F.VERSIONED_CLUSTER_FUNCTION) {
      children.push(this.vu('cluster_function_protocol_version').node);
    }
    // Forward-compat: some server builds (observed: 25.12.x, proto 54483)
    // append extra trailing version VarUInt(s) after cluster_function that are
    // absent from the public spec and source. Consume any trailing VarUInt
    // that can't begin a valid post-hello server packet so the stream stays
    // aligned. See docs/full_native_protocol_spec.md — this is a known gap.
    while (this.r.offset < this.total) {
      const peeked = this.peekVarUInt();
      if (peeked === null || VALID_FIRST_SERVER_PACKET.has(peeked)) break;
      const node = this.vu('hello_tail_extra_version').node;
      node.metadata = { specGap: true };
      children.push(node);
    }
    return this.packet('ServerHello', start, children);
  }

  private decodePasswordRules(): AstNode {
    const start = this.r.offset;
    const { value: count, node: countNode } = this.vu('rule_count');
    const children: AstNode[] = [countNode];
    for (let i = 0; i < count; i++) {
      const ruleStart = this.r.offset;
      const pattern = this.str('pattern').node;
      const message = this.str('message').node;
      children.push(this.group(`rule[${i}]`, ruleStart, [pattern, message], ''));
    }
    return this.group('password_complexity_rules', start, children, `${count} rule(s)`);
  }

  private decodeException(start: number): AstNode {
    const children: AstNode[] = [this.typeNode('Exception', ServerPacket.Exception)];
    // Chain of nested exceptions: each ends with a has_nested Bool.
    let depth = 0;
    while (true) {
      const exStart = this.r.offset;
      const code = this.i32('code').node;
      const name = this.str('name').node;
      const message = this.str('message').node;
      const stack = this.str('stack_trace').node;
      const hasNestedRes = this.u8('has_nested');
      const hasNested = hasNestedRes.value;
      const nestedFlag = hasNestedRes.node;
      const exNode = this.group(
        depth === 0 ? 'exception' : `nested_exception[${depth}]`,
        exStart,
        [code, name, message, stack, nestedFlag],
        message.displayValue,
      );
      children.push(exNode);
      if (hasNested !== 1) break;
      depth += 1;
      if (depth > 64) break; // defensive bound
    }
    return this.packet('Exception', start, children);
  }

  private decodeProgress(start: number): AstNode {
    const children: AstNode[] = [this.typeNode('Progress', ServerPacket.Progress)];
    children.push(this.vu('rows').node);
    children.push(this.vu('bytes').node);
    children.push(this.vu('total_rows').node);
    if (this.negotiated >= F.TOTAL_BYTES_IN_PROGRESS) {
      children.push(this.vu('total_bytes').node);
    }
    if (this.negotiated >= F.WRITE_CLIENT_INFO) {
      children.push(this.vu('wrote_rows').node);
      children.push(this.vu('wrote_bytes').node);
    }
    if (this.negotiated >= F.SERVER_QUERY_TIME_IN_PROGRESS) {
      children.push(this.vu('elapsed_ns').node);
    }
    return this.packet('Progress', start, children);
  }

  private decodeProfileInfo(start: number): AstNode {
    const children: AstNode[] = [this.typeNode('ProfileInfo', ServerPacket.ProfileInfo)];
    children.push(this.vu('rows').node);
    children.push(this.vu('blocks').node);
    children.push(this.vu('bytes').node);
    children.push(this.bool('applied_limit').node);
    children.push(this.vu('rows_before_limit').node);
    children.push(this.bool('calculated_rows_before_limit').node);
    if (this.negotiated >= F.ROWS_BEFORE_AGGREGATION) {
      children.push(this.bool('applied_aggregation').node);
      children.push(this.vu('rows_before_aggregation').node);
    }
    return this.packet('ProfileInfo', start, children);
  }

  private decodeTableColumns(start: number): AstNode {
    const children: AstNode[] = [this.typeNode('TableColumns', ServerPacket.TableColumns)];
    children.push(this.str('external_table').node);
    children.push(this.str('columns_description').node);
    return this.packet('TableColumns', start, children);
  }

  // --- Data-family packets (table_name + Block) ---------------------------

  private decodeDataPacket(name: string, typeCode: number, start: number): AstNode {
    const children: AstNode[] = [this.typeNode(name, typeCode)];
    children.push(this.str('table_name').node);
    const block = this.native.decodeProtocolBlock(this.blockIndex++);
    children.push(this.blockToAst(block));
    return this.packet(name, start, children);
  }

  private blockToAst(block: BlockNode): AstNode {
    const children: AstNode[] = [block.header.astNode];
    for (const col of block.columns) {
      const colChildren: AstNode[] = [col.metadataNode, ...col.dataPrefixNodes, ...col.values];
      children.push({
        id: this.nid(),
        type: col.typeString || 'Column',
        byteRange: { start: col.metadataByteRange.start, end: col.dataByteRange.end },
        value: null,
        displayValue: `${col.name}: ${col.typeString} · ${col.values.length} value(s)`,
        label: col.name,
        children: colChildren,
      });
    }
    return {
      id: this.nid(),
      type: 'Native.Block',
      byteRange: block.byteRange,
      value: { rows: block.rowCount, columns: block.columns.length },
      displayValue: `${block.rowCount} row(s) × ${block.columns.length} column(s)`,
      label: 'block',
      children,
    };
  }

  // --- settings / parameters lists ----------------------------------------

  private decodeSettingsList(label: string): AstNode {
    const start = this.r.offset;
    const children: AstNode[] = [];
    let count = 0;
    while (true) {
      const keyStart = this.r.offset;
      const { value: key, node: keyNode } = this.str('key');
      if (key === '') {
        // Empty key = terminator (a single VarUInt 0).
        keyNode.label = 'terminator';
        children.push(keyNode);
        break;
      }
      const flags = this.vu('flags').node;
      const value = this.str('value').node;
      children.push(this.group(key, keyStart, [keyNode, flags, value], value.displayValue));
      count += 1;
      if (count > 100000) throw new Error('settings list overflow');
    }
    return this.group(label, start, children, `${count} entr${count === 1 ? 'y' : 'ies'}`);
  }

  // --- version negotiation ------------------------------------------------

  private computeNegotiatedVersion(): number {
    const client = this.peekHelloVersion(this.combined.subarray(0, this.c2sLength));
    const server = this.peekHelloVersion(this.combined.subarray(this.c2sLength, this.total));
    if (client != null && server != null) return Math.min(client, server);
    return client ?? server ?? 0;
  }

  /** Read just the protocol_version out of a Hello at the start of `buf`. */
  private peekHelloVersion(buf: Uint8Array): number | null {
    if (buf.length === 0) return null;
    try {
      const rr = new BinaryReader(buf);
      const { value: type } = decodeLEB128(rr);
      if (type !== 0) return null; // not a Hello (e.g. handshake Exception)
      const { value: nameLen } = decodeLEB128(rr);
      rr.skip(nameLen);
      decodeLEB128(rr); // version_major
      decodeLEB128(rr); // version_minor
      return decodeLEB128(rr).value; // protocol_version
    } catch {
      return null;
    }
  }

  // --- primitive readers (each returns an AstNode + value) ----------------

  private leaf(type: string, start: number, value: unknown, displayValue: string, label: string): AstNode {
    return { id: this.nid(), type, byteRange: { start, end: this.r.offset }, value, displayValue, label };
  }

  private vu(label: string): { value: number; node: AstNode } {
    const start = this.r.offset;
    const { value } = decodeLEB128(this.r);
    return { value, node: this.leaf('VarUInt', start, value, String(value), label) };
  }

  private str(label: string): { value: string; node: AstNode } {
    const start = this.r.offset;
    const { value: len } = decodeLEB128(this.r);
    const { value: bytes } = this.r.readBytes(len);
    const value = TEXT_DECODER.decode(bytes);
    return { value, node: this.leaf('String', start, value, `"${value}"`, label) };
  }

  private u8(label: string): { value: number; node: AstNode } {
    const start = this.r.offset;
    const { value } = this.r.readUInt8();
    return { value, node: this.leaf('UInt8', start, value, String(value), label) };
  }

  private bool(label: string): { value: boolean; node: AstNode } {
    const start = this.r.offset;
    const { value } = this.r.readUInt8();
    return { value: value !== 0, node: this.leaf('Bool', start, value !== 0, value !== 0 ? 'true' : 'false', label) };
  }

  private i32(label: string): { value: number; node: AstNode } {
    const start = this.r.offset;
    const { value } = this.r.readInt32LE();
    return { value, node: this.leaf('Int32', start, value, String(value), label) };
  }

  private i64(label: string): { value: bigint; node: AstNode } {
    const start = this.r.offset;
    const { value } = this.r.readInt64LE();
    return { value, node: this.leaf('Int64', start, value, String(value), label) };
  }

  private u64(label: string): { value: bigint; node: AstNode } {
    const start = this.r.offset;
    const { value } = this.r.readUInt64LE();
    return { value, node: this.leaf('UInt64', start, value, String(value), label) };
  }

  private fixedBytes(n: number, label: string, typeName: string): AstNode {
    const start = this.r.offset;
    const { value } = this.r.readBytes(n);
    const hex = Array.from(value).map((b) => b.toString(16).padStart(2, '0')).join('');
    return this.leaf(typeName, start, hex, `0x${hex}`, label);
  }

  // --- node builders ------------------------------------------------------

  private typeNode(name: string, code: number): AstNode {
    // The leading VarUInt packet type code. Length is computed from the offset
    // delta so multi-byte type codes (>=128) are tracked correctly.
    const start = this.r.offset;
    decodeLEB128(this.r);
    return this.leaf('VarUInt', start, code, `${code} (${name})`, 'packet_type');
  }

  private peekTypeOrThrow(): { value: number } {
    const v = this.peekVarUInt();
    if (v === null) throw new Error(`could not read packet type at offset ${this.r.offset}`);
    return { value: v };
  }

  private peekVarUInt(): number | null {
    const bytes = this.r.peekBytes(10);
    let result = 0;
    let shift = 0;
    for (let i = 0; i < bytes.length; i++) {
      const b = bytes[i];
      result += (b & 0x7f) * 2 ** shift;
      if ((b & 0x80) === 0) return result;
      shift += 7;
      if (shift > 56) return null;
    }
    return null;
  }

  /** A packet whose only content is its type code (Ping, Pong, Cancel, EndOfStream, ...). */
  private bodylessPacket(name: string, code: number, start: number): AstNode {
    return this.packet(name, start, [this.typeNode(name, code)]);
  }

  private packet(name: string, start: number, children: AstNode[]): AstNode {
    return {
      id: this.nid(),
      type: `Protocol.${name}`,
      byteRange: { start, end: this.r.offset },
      value: name,
      displayValue: name,
      label: name,
      children,
    };
  }

  private group(name: string, start: number, children: AstNode[], display: string): AstNode {
    return {
      id: this.nid(),
      type: `Protocol.${name.replace(/[^A-Za-z0-9_]/g, '_')}`,
      byteRange: { start, end: this.r.offset },
      value: name,
      displayValue: display || name,
      label: name,
      children,
    };
  }

  private emptyHeader(): HeaderNode {
    const zero: ByteRange = { start: 0, end: 0 };
    return { byteRange: zero, columnCount: 0, columnCountRange: zero, columns: [] };
  }

  private nid(): string {
    return `proto-${this.idCounter++}`;
  }
}
