# Native Protocol Versions

This document explains how ClickHouse's `client_protocol_version` request parameter changes the HTTP `FORMAT Native` wire layout, and how the explorer maps that behavior.

## Source of Truth

The behavior described here is taken from the local ClickHouse source tree in `~/Code/clickhouse`:

- `src/Server/HTTPHandler.cpp`
- `src/Formats/FormatFactory.cpp`
- `src/Processors/Formats/Impl/NativeFormat.cpp`
- `src/Formats/NativeWriter.cpp`
- `src/Core/ProtocolDefines.h`
- `src/Core/BlockInfo.cpp`
- `src/DataTypes/Serializations/SerializationInfo.cpp`

## Request Flow

For HTTP queries, `client_protocol_version` is handled as follows:

1. `HTTPHandler.cpp` reads the `client_protocol_version` query parameter.
2. The value is stored in `Context.client_protocol_version`.
3. `FormatFactory.cpp` copies that value into `FormatSettings.client_protocol_version`.
4. `NativeFormat.cpp` passes it to `NativeWriter`.
5. `NativeWriter.cpp` changes the output layout based on the selected revision.

The explorer's Native protocol selector controls this exact parameter.

## Explorer Presets

The UI exposes fixed revision presets instead of free-form input:

| Preset | Upstream constant | Meaning |
|--------|-------------------|---------|
| `0` | legacy HTTP default | Omits `client_protocol_version` and preserves the old HTTP-native layout |
| `54405` | `DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE` | LowCardinality negotiation |
| `54452` | `DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING` | AggregateFunction state versioning |
| `54454` | `DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION` | Adds per-column serialization metadata |
| `54465` | `DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION` | Allows sparse serialization kinds |
| `54473` | `DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION` | Dynamic/JSON v2 |
| `54480` | `DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION` | Adds `BlockInfo.out_of_order_buckets` |
| `54482` | `DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION` | Allows replicated serialization kinds |
| `54483` | `DBMS_MIN_REVISION_WITH_NULLABLE_SPARSE_SERIALIZATION` | Current upstream protocol version in the checked source tree |

## What Changes on the Wire

### `0`: Legacy HTTP Native

With no explicit protocol version:

- `NativeWriter` does not write `BlockInfo`
- AggregateFunction versioning is disabled
- Custom serialization metadata is not written
- Dynamic and JSON use the older v1 serialization path

This matches the explorer's historical behavior and remains the default preset.

### `> 0`: `BlockInfo` Appears Before Block Dimensions

In `NativeWriter::write()`:

- if `client_revision > 0`, `block.info.write()` runs before `NumColumns` and `NumRows`
- this is true even for HTTP `FORMAT Native`

That means HTTP Native is not always just:

```text
NumColumns, NumRows, columns...
```

With a non-zero protocol version it becomes:

```text
BlockInfo, NumColumns, NumRows, columns...
```

`BlockInfo` is field-number encoded and terminated by field `0`.

Fields currently relevant to Native output:

| Field | Name | Type | Min revision |
|-------|------|------|--------------|
| `1` | `is_overflows` | `bool` | `0` |
| `2` | `bucket_num` | `Int32` | `0` |
| `3` | `out_of_order_buckets` | `Array(Int32)` | `54480` |

## Column-Level Changes

Each Native column still writes:

1. column name
2. column type
3. optional serialization metadata
4. column data

The change point is revision `54454`.

### `54454`: Custom Serialization Metadata

Starting at `DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION`, `NativeWriter` calls `getSerializationAndColumn()` and writes:

- `UInt8 has_custom`
- if `has_custom == 1`, a binary-encoded serialization kind stack

The kind-stack encoding comes from `SerializationInfo.cpp`.

Common encodings:

| Tag | Kind stack |
|-----|------------|
| `0` | `DEFAULT` |
| `1` | `DEFAULT -> SPARSE` |
| `2` | `DEFAULT -> DETACHED` |
| `3` | `DEFAULT -> SPARSE -> DETACHED` |
| `4` | `DEFAULT -> REPLICATED` |
| `5` | arbitrary combination, encoded as count + raw kinds |

For tuple types, nested element serialization info is serialized recursively after the tuple's own kind stack.

### `54465`: Sparse Serialization

Once the client revision reaches `54465`, ClickHouse may keep sparse serialization instead of materializing the column first.

For sparse columns:

- the stream starts with sparse-offset metadata
- only non-default values are serialized in the value stream
- readers must materialize omitted rows as type defaults

The explorer now materializes both plain sparse columns and sparse `Nullable(...)` columns. If other custom serialization combinations that the app does not model are encountered, the decoder raises an explicit error that includes the kind stack and selected protocol version.

### `54482`: Replicated Serialization

Before `54482`, `NativeWriter` converts replicated columns to full columns.

At and after `54482`, ClickHouse may keep replicated serialization kinds in the output. The explorer decodes the replicated index stream, expands the shared nested values back to row-shaped AST nodes, and surfaces the metadata in the UI.

### `54483`: Nullable Sparse Serialization

At `54483`, sparse serialization can also apply to `Nullable`-based layouts. The null map is derived from sparse offsets instead of a separate explicit null-map stream, and the explorer reconstructs `NULL` vs value rows accordingly.

## Type-Specific Revision Gates

### `54405`: LowCardinality

Below `54405`, `NativeWriter` removes the `LowCardinality` wrapper before sending data. At and after `54405`, the server can keep the real `LowCardinality(T)` type and dictionary-style encoding.

### `54452`: AggregateFunction State Versioning

At `54452`, `setVersionToAggregateFunctions()` begins passing the selected client revision into aggregate-state serialization. That lets aggregate functions negotiate compatible state formats across revisions.

### `54473`: Dynamic / JSON v2

`NativeWriter::writeData()` switches these settings based on the selected revision:

- below `54473`: `dynamic_serialization_version = V1`, `object_serialization_version = V1`
- at and above `54473`: `dynamic_serialization_version = V2`, `object_serialization_version = V2`

This is the main protocol gate for modern Native `Dynamic` and `JSON` layouts.

## Current Upstream Version

In the checked local source tree, `src/Core/ProtocolDefines.h` defines:

```text
DBMS_TCP_PROTOCOL_VERSION = 54483
```

That is why the explorer's "current" preset is `54483`.

## Explorer Behavior

- The protocol selector is shown only for `Native`.
- The selected revision is used for both live HTTP queries and uploaded Native files.
- The default preset is `0` so existing behavior does not change unexpectedly.
- Share links preserve the Native protocol preset.
- The AST and hex viewer expose:
  - `BlockInfo`
  - `has_custom`
  - serialization kind stacks

## Practical Guidance

- Use `0` when you want to match the explorer's original legacy HTTP behavior.
- Use `54483` when you want the most modern layout the checked ClickHouse source supports.
- Use intermediate presets when you need to isolate when a specific protocol feature appeared on the wire.
