export interface NativeProtocolPreset {
  value: number;
  label: string;
  constantName: string;
  summary: string;
}

export const NATIVE_PROTOCOL_PRESETS: NativeProtocolPreset[] = [
  {
    value: 0,
    label: 'Legacy HTTP default (0)',
    constantName: 'LEGACY_HTTP_DEFAULT',
    summary: 'Omit client_protocol_version and use the legacy HTTP Native layout.',
  },
  {
    value: 54405,
    label: '54405 LowCardinality',
    constantName: 'DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE',
    summary: 'Enables LowCardinality type negotiation.',
  },
  {
    value: 54452,
    label: '54452 AggregateFunction versioning',
    constantName: 'DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING',
    summary: 'Adds AggregateFunction revision-aware state serialization.',
  },
  {
    value: 54454,
    label: '54454 Custom serialization',
    constantName: 'DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION',
    summary: 'Adds per-column serialization metadata before Native column data.',
  },
  {
    value: 54465,
    label: '54465 Sparse serialization',
    constantName: 'DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION',
    summary: 'Allows sparse column serialization kinds.',
  },
  {
    value: 54473,
    label: '54473 Dynamic/JSON v2',
    constantName: 'DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION',
    summary: 'Switches Dynamic and JSON Native serialization to v2.',
  },
  {
    value: 54480,
    label: '54480 Out-of-order buckets',
    constantName: 'DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION',
    summary: 'Adds BlockInfo field support for out-of-order aggregation buckets.',
  },
  {
    value: 54482,
    label: '54482 Replicated serialization',
    constantName: 'DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION',
    summary: 'Allows replicated serialization kinds in Native output.',
  },
  {
    value: 54483,
    label: '54483 Nullable sparse / current',
    constantName: 'DBMS_MIN_REVISION_WITH_NULLABLE_SPARSE_SERIALIZATION',
    summary: 'Current upstream protocol version with nullable sparse serialization.',
  },
];

export const DEFAULT_NATIVE_PROTOCOL_VERSION = 0;
export const CURRENT_NATIVE_PROTOCOL_VERSION = 54483;

const NATIVE_PROTOCOL_PRESET_VALUES = new Set(
  NATIVE_PROTOCOL_PRESETS.map((preset) => preset.value),
);

export function isNativeProtocolVersion(value: number): boolean {
  return NATIVE_PROTOCOL_PRESET_VALUES.has(value);
}
