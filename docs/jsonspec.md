### 6.9 JSON

The `JSON` type stores semi-structured JSON data with automatic schema inference. It uses an object-oriented storage model where JSON paths are stored as separate columns, similar to how `Nested` columns work.

**Type Definition:**
```sql
JSON
JSON(max_dynamic_paths=N)
JSON(a Int64, b String)  -- with typed paths
```

**Storage Model:**

`JSON` columns store data using three categories of paths:

1. **Typed Paths**: Paths with known, fixed types (declared in type definition)
2. **Dynamic Paths**: Frequently occurring paths discovered at runtime (up to `max_dynamic_paths`)
3. **Shared Data**: Remaining paths stored in a shared `Dynamic` column

**Stream Architecture:**

Similar to `Dynamic`, `JSON` uses multiple streams:

1. **ObjectStructure** stream:
   - Serialization version (UInt64)
   - Structure metadata (list of dynamic paths, statistics, etc.)

2. **ObjectData** stream:
   - **ObjectTypedPath** substreams: One per typed path
   - **ObjectDynamicPath** substreams: One per dynamic path
   - **ObjectSharedData** substream: For overflow paths

**Serialization Versions:**

**V1 (version = 0):**
- `<max_dynamic_paths>` (VarUInt)
- `<num_dynamic_paths>` (VarUInt)
- `<sorted list of dynamic path names>` (String for each)
- Optional statistics

**V2 (version = 2):**
- Same as V1 without `max_dynamic_paths`

**V3 (version = 4):**
- Like V2 with binary type encoding and optional statistics flag

> **Note:** JSON Object version numbers are: V1=0, STRING=1, V2=2, FLATTENED=3, V3=4.

> **Important:** `Dynamic` columns (used for dynamic paths) have **different** version numbers: V1=1, V2=2, FLATTENED=3, V3=4. Don't confuse JSON V1 (=0) with Dynamic V1 (=1).

**STRING (version = 1):**
- Special mode for Native format only
- JSON stored as plain String (JSON text)
- Controlled by `output_format_native_write_json_as_string=1`

**FLATTENED (version = 3):**
- Native format only, for easier client parsing
- All paths (typed + dynamic + shared) serialized as separate columns

**Example: JSON with STRING serialization**

```bash
curl -s -XPOST "http://localhost:8123?default_format=Native&allow_experimental_json_type=1&output_format_native_write_json_as_string=1" \
  --data-binary "SELECT '{\"a\": 1}'::JSON AS col" | xxd
```

```
00000000: 0101 0363 6f6c 044a 534f 4e01 0000 0000  ...col.JSON.....
00000010: 0000 0007 7b22 6122 3a31 7d              ....{"a":1}
```

Breakdown:
```typescript
const block = new Uint8Array([
  0x01,                                    // NumColumns = 1
  0x01,                                    // NumRows = 1

  // Column 0:
  0x03, 0x63, 0x6f, 0x6c,                  // Column name = "col"
  0x04, 0x4a, 0x53, 0x4f, 0x4e,           // Column type = "JSON"

  // ObjectStructure stream:
  0x01, 0x00, 0x00, 0x00,                  // Version = 1 (STRING)
  0x00, 0x00, 0x00, 0x00,

  // ObjectData stream (String column):
  0x07,                                    // String length = 7
  0x7b, 0x22, 0x61, 0x22, 0x3a,           // '{"a":1}'
  0x31, 0x7d,
]);
```

**Example: JSON with standard serialization**

```bash
curl -s -XPOST "http://localhost:8123?default_format=Native&allow_experimental_json_type=1" \
  --data-binary "SELECT '{\"a\": 1, \"b\": \"hello\"}'::JSON AS col" | xxd
```

```
00000000: 0101 0363 6f6c 044a 534f 4e00 0000 0000  ...col.JSON.....
00000010: 0000 0002 0201 6101 6201 0000 0000 0000  ......a.b.......
00000020: 0001 0105 496e 7436 3400 0000 0000 0000  ....Int64.......
00000030: 0001 0000 0000 0000 0001 0106 5374 7269  ............Stri
00000040: 6e67 0000 0000 0000 0000 0001 0000 0000  ng..............
00000050: 0000 0001 0568 656c 6c6f 0000 0000 0000  .....hello......
00000060: 0000                                     ..
```

Breakdown (simplified structure):
```typescript
const block = new Uint8Array([
  0x01,                                    // NumColumns = 1
  0x01,                                    // NumRows = 1

  // Column metadata...
  0x03, 0x63, 0x6f, 0x6c,                  // "col"
  0x04, 0x4a, 0x53, 0x4f, 0x4e,           // "JSON"

  // ObjectStructure stream:
  0x00, 0x00, 0x00, 0x00,                  // Version = 0 (V1)
  0x00, 0x00, 0x00, 0x00,

  0x02,                                    // num_dynamic_paths = 2 (VarUInt)
  0x02,                                    // Actual count = 2

  // Sorted dynamic paths:
  0x01, 0x61,                              // Path 0: "a" (length=1)
  0x01, 0x62,                              // Path 1: "b" (length=1)

  // Shared data serialization version:
  0x01, 0x00, 0x00, 0x00,                  // Version info
  0x00, 0x00, 0x00, 0x00,

  // ObjectData stream:
  // Path "a" stored as Dynamic with inferred type Int64:
  0x01,                                    // num_types = 1
  0x05, 0x49, 0x6e, 0x74, 0x36, 0x34,     // "Int64"
  // ... Variant discriminators ...
  0x00, 0x00, 0x00, 0x00,                  // Mode
  0x00, 0x00, 0x00, 0x00,
  0x01,                                    // Discriminator
  // ... Value ...
  0x01, 0x00, 0x00, 0x00,                  // 1 (as Int64)
  0x00, 0x00, 0x00, 0x00,

  // Path "b" stored as Dynamic with type String:
  0x01,                                    // num_types = 1
  0x06, 0x53, 0x74, 0x72, 0x69,           // "String"
  0x6e, 0x67,
  // ... Variant discriminators ...
  0x00, 0x00, 0x00, 0x00,                  // Mode
  0x00, 0x00, 0x00, 0x00,
  0x01,                                    // Discriminator
  // ... Value ...
  0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,     // "hello"
]);
```

Note: Each dynamic path is stored as a `Dynamic` column, so it includes the full `Dynamic` serialization (structure + variant data).

**Example: JSON with typed and dynamic paths**

This example demonstrates a `JSON` column with both a **typed path** (declared type) and a **dynamic path** (inferred type). Typed paths are serialized directly using their native format, while dynamic paths are wrapped in `Dynamic` serialization.

```bash
curl -s -XPOST "http://localhost:8123?default_format=Native&allow_experimental_json_type=1" \
  --data-binary "SELECT '{\"a\": 42, \"b\": \"hi\"}'::JSON(a UInt32) AS col" | xxd
```

```
00000000: 0101 0363 6f6c 0e4a 534f 4e28 6120 5549  ...col.JSON(a UI
00000010: 6e74 3332 2900 0000 0000 0000 0001 0101  nt32)...........
00000020: 6201 0000 0000 0000 0001 0106 5374 7269  b...........Stri
00000030: 6e67 0000 0000 0000 0000 2a00 0000 0102  ng........*.....
00000040: 6869 0000 0000 0000 0000                 hi........
```

Full breakdown:
```typescript
const block = new Uint8Array([
  // ═══════════════════════════════════════════════════════════════════
  // BLOCK HEADER
  // ═══════════════════════════════════════════════════════════════════
  0x01,                                    // NumColumns = 1 (VarUInt)
  0x01,                                    // NumRows = 1 (VarUInt)

  // ═══════════════════════════════════════════════════════════════════
  // COLUMN HEADER
  // ═══════════════════════════════════════════════════════════════════
  0x03, 0x63, 0x6f, 0x6c,                  // Column name = "col" (len=3)
  0x0e,                                    // Type name length = 14
  0x4a, 0x53, 0x4f, 0x4e, 0x28, 0x61,     // "JSON(a UInt32)"
  0x20, 0x55, 0x49, 0x6e, 0x74, 0x33,     //   - typed path "a" with type UInt32
  0x32, 0x29,                              //   - typed paths are part of the type name

  // ═══════════════════════════════════════════════════════════════════
  // OBJECT STRUCTURE STREAM (prefix for JSON column)
  // ═══════════════════════════════════════════════════════════════════
  0x00, 0x00, 0x00, 0x00,                  // Version = 0 (V1 serialization)
  0x00, 0x00, 0x00, 0x00,                  //   (UInt64 little-endian)

  // V1 structure metadata:
  0x01,                                    // max_dynamic_paths = 1 (VarUInt)
  0x01,                                    // num_dynamic_paths = 1 (VarUInt)

  // Sorted list of dynamic path names:
  0x01, 0x62,                              // Path 0: "b" (len=1, then "b")
                                           //   - typed path "a" is NOT listed here
                                           //   - only runtime-discovered paths appear

  // NOTE: In Native format, statistics mode defaults to NONE, so no
  // statistics are written here. The next bytes are the Dynamic prefix.

  // ═══════════════════════════════════════════════════════════════════
  // DYNAMIC STRUCTURE STREAM (prefix for dynamic path "b")
  // ═══════════════════════════════════════════════════════════════════
  0x01, 0x00, 0x00, 0x00,                  // Dynamic version = 1 (V1)
  0x00, 0x00, 0x00, 0x00,                  //   (UInt64 little-endian)
                                           //   NOTE: Dynamic V1=1, V2=2, FLATTENED=3, V3=4

  // V1 Dynamic structure:
  0x01,                                    // max_dynamic_types = 1 (VarUInt)
  0x01,                                    // num_dynamic_types = 1 (VarUInt)

  // Type names (not SharedVariant, which is implicit):
  0x06, 0x53, 0x74, 0x72, 0x69,           // Type 0: "String" (len=6)
  0x6e, 0x67,

  // NOTE: Dynamic statistics also use NONE mode in Native format,
  // so no statistics written here either.

  // ═══════════════════════════════════════════════════════════════════
  // VARIANT STRUCTURE STREAM (prefix for Dynamic's Variant)
  // ═══════════════════════════════════════════════════════════════════
  0x00, 0x00, 0x00, 0x00,                  // Variant mode = 0 (COMPACT)
  0x00, 0x00, 0x00, 0x00,                  //   (UInt64 little-endian)

  // ═══════════════════════════════════════════════════════════════════
  // OBJECT DATA STREAM
  // ═══════════════════════════════════════════════════════════════════

  // --- TYPED PATH "a" (UInt32) ---
  // Serialized directly using UInt32's native format (no Dynamic wrapper!)
  0x2a, 0x00, 0x00, 0x00,                  // value = 42 (UInt32 little-endian)

  // --- DYNAMIC PATH "b" (as Dynamic -> Variant) ---
  // Variant discriminator column (1 row):
  0x01,                                    // Row 0: discriminator = 1 (String variant)
                                           //   Variants sorted alphabetically: SharedVariant=0, String=1
                                           //   (255 = NULL)

  // String variant data:
  0x02, 0x68, 0x69,                        // value = "hi" (len=2, then "hi")

  // ═══════════════════════════════════════════════════════════════════
  // SHARED DATA STREAM (overflow paths, empty in this case)
  // ═══════════════════════════════════════════════════════════════════
  // Shared data is Array(Tuple(path: String, value: String))
  // For 1 row with 0 shared paths:
  0x00, 0x00, 0x00, 0x00,                  // Array offsets: [0] (UInt64)
  0x00, 0x00, 0x00, 0x00,                  //   - row 0 has 0 elements
]);
```

**Key observations:**

1. **Typed path "a"** (UInt32):
   - Declared in type definition: `JSON(a UInt32)`
   - Type name includes the typed path specification
   - NOT listed in the dynamic paths list
   - Serialized directly as UInt32 (4 bytes, little-endian): `2a 00 00 00` = 42
   - No `Dynamic` wrapper overhead

2. **Dynamic path "b"** (inferred as String):
   - Discovered at runtime from the JSON data
   - Listed in the dynamic paths list in ObjectStructure
   - Serialized as a full `Dynamic` column:
     - DynamicStructure: version, types list, statistics
     - VariantStructure: mode
     - VariantData: discriminators + type-specific data

3. **Serialization order in ObjectData**:
   - Typed paths first (sorted alphabetically by path name)
   - Dynamic paths second (sorted alphabetically by path name)
   - Shared data last

4. **Space efficiency**: Typed paths save significant space by avoiding the `Dynamic` overhead. For path "a", we use 4 bytes (UInt32) instead of ~20+ bytes (Dynamic structure + Variant + value).

**Example: JSON with typed and dynamic paths (multiple rows)**

This example shows the same structure with 2 rows, demonstrating how column data is laid out contiguously for each path.

```bash
curl -s -XPOST "http://localhost:8123?default_format=Native&allow_experimental_json_type=1" \
  --data-binary "SELECT ('{\"a\": ' || toString(number * 10) || ', \"b\": \"row' || toString(number) || '\"}')::JSON(a UInt32) AS col FROM system.numbers LIMIT 2" | xxd
```

This produces rows: `{"a": 0, "b": "row0"}` and `{"a": 10, "b": "row1"}`.

```
00000000: 0102 0363 6f6c 0e4a 534f 4e28 6120 5549  ...col.JSON(a UI
00000010: 6e74 3332 2900 0000 0000 0000 0001 0101  nt32)...........
00000020: 6201 0000 0000 0000 0001 0106 5374 7269  b...........Stri
00000030: 6e67 0000 0000 0000 0000 0000 0000 0a00  ng..............
00000040: 0000 0101 0472 6f77 3004 726f 7731 0000  .....row0.row1..
00000050: 0000 0000 0000 0000 0000 0000 0000       ..............
```

Full breakdown:
```typescript
const block = new Uint8Array([
  // ═══════════════════════════════════════════════════════════════════
  // BLOCK HEADER
  // ═══════════════════════════════════════════════════════════════════
  0x01,                                    // NumColumns = 1 (VarUInt)
  0x02,                                    // NumRows = 2 (VarUInt) <-- 2 rows!

  // ═══════════════════════════════════════════════════════════════════
  // COLUMN HEADER
  // ═══════════════════════════════════════════════════════════════════
  0x03, 0x63, 0x6f, 0x6c,                  // Column name = "col" (len=3)
  0x0e,                                    // Type name length = 14
  0x4a, 0x53, 0x4f, 0x4e, 0x28, 0x61,     // "JSON(a UInt32)"
  0x20, 0x55, 0x49, 0x6e, 0x74, 0x33,
  0x32, 0x29,

  // ═══════════════════════════════════════════════════════════════════
  // OBJECT STRUCTURE STREAM
  // ═══════════════════════════════════════════════════════════════════
  0x00, 0x00, 0x00, 0x00,                  // Version = 0 (V1 serialization)
  0x00, 0x00, 0x00, 0x00,

  0x01,                                    // max_dynamic_paths = 1 (VarUInt)
  0x01,                                    // num_dynamic_paths = 1 (VarUInt)
  0x01, 0x62,                              // Path 0: "b" (len=1)

  // NOTE: No statistics in Native format (NONE mode is default)

  // ═══════════════════════════════════════════════════════════════════
  // DYNAMIC STRUCTURE STREAM (for path "b")
  // ═══════════════════════════════════════════════════════════════════
  0x01, 0x00, 0x00, 0x00,                  // Dynamic version = 1 (V1)
  0x00, 0x00, 0x00, 0x00,                  //   (Dynamic V1=1, V2=2, V3=4)

  0x01,                                    // max_dynamic_types = 1 (VarUInt)
  0x01,                                    // num_dynamic_types = 1 (VarUInt)
  0x06, 0x53, 0x74, 0x72, 0x69,           // Type 0: "String" (len=6)
  0x6e, 0x67,

  // NOTE: No Dynamic statistics in Native format (NONE mode)

  // ═══════════════════════════════════════════════════════════════════
  // VARIANT STRUCTURE STREAM
  // ═══════════════════════════════════════════════════════════════════
  0x00, 0x00, 0x00, 0x00,                  // Variant mode = 0 (COMPACT)
  0x00, 0x00, 0x00, 0x00,

  // ═══════════════════════════════════════════════════════════════════
  // OBJECT DATA STREAM
  // ═══════════════════════════════════════════════════════════════════

  // --- TYPED PATH "a" (UInt32) - ALL ROWS CONTIGUOUS ---
  0x00, 0x00, 0x00, 0x00,                  // Row 0: a = 0
  0x0a, 0x00, 0x00, 0x00,                  // Row 1: a = 10

  // --- DYNAMIC PATH "b" - Variant discriminators (ALL ROWS) ---
  // Variants sorted alphabetically: SharedVariant=0, String=1 (255=NULL)
  0x01,                                    // Row 0: discriminator = 1 (String)
  0x01,                                    // Row 1: discriminator = 1 (String)

  // --- DYNAMIC PATH "b" - String variant data (ALL ROWS) ---
  0x04, 0x72, 0x6f, 0x77, 0x30,           // Row 0: "row0" (len=4)
  0x04, 0x72, 0x6f, 0x77, 0x31,           // Row 1: "row1" (len=4)

  // ═══════════════════════════════════════════════════════════════════
  // SHARED DATA STREAM (Array offsets for each row)
  // ═══════════════════════════════════════════════════════════════════
  0x00, 0x00, 0x00, 0x00,                  // Row 0 offset: 0 elements
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,                  // Row 1 offset: 0 elements
  0x00, 0x00, 0x00, 0x00,
]);
```

**Key observations for multi-row data:**

1. **Columnar layout**: All values for each path are stored contiguously:
   - Typed path "a": `[0, 10]` as two consecutive UInt32 values
   - Dynamic path "b" discriminators: `[1, 1]` (both rows are String)
   - Dynamic path "b" String data: `["row0", "row1"]` stored sequentially

2. **No per-row overhead**: The structure metadata (version, paths, types) is written once in the prefix, not repeated for each row. Only the actual data values scale with row count.

3. **Variant discriminators**: For the Dynamic path, discriminators for ALL rows come first, then the actual variant data for all rows. This enables efficient columnar processing.

4. **Shared data offsets**: One UInt64 offset per row indicating how many shared path entries that row has. With 0 shared paths, all offsets are 0.

**Example: JSON with exceeded max_dynamic_paths (shared data)**

This example shows what happens when the number of paths exceeds `max_dynamic_paths`. Overflow paths are stored in the **shared data** section.

```bash
curl -s -XPOST "http://localhost:8123?default_format=Native&allow_experimental_json_type=1" \
  --data-binary "SELECT '{\"a\": 1, \"b\": 2, \"c\": 3}'::JSON(max_dynamic_paths=2) AS col" | xxd
```

With `max_dynamic_paths=2` and 3 paths in the JSON:
- Paths "a" and "b" become **dynamic paths** (stored as `Dynamic` columns)
- Path "c" overflows to **shared data**

```
00000000: 0101 0363 6f6c 194a 534f 4e28 6d61 785f  ...col.JSON(max_
00000010: 6479 6e61 6d69 635f 7061 7468 733d 3229  dynamic_paths=2)
00000020: 0000 0000 0000 0000 0202 0161 0162 0100  ...........a.b..
00000030: 0000 0000 0000 0101 0549 6e74 3634 0000  .........Int64..
00000040: 0000 0000 0000 0100 0000 0000 0000 0101  ................
00000050: 0549 6e74 3634 0000 0000 0000 0000 0001  .Int64..........
00000060: 0000 0000 0000 0000 0200 0000 0000 0000  ................
00000070: 0100 0000 0000 0000 0163 090a 0300 0000  .........c......
00000080: 0000 0000                                ....
```

Full breakdown:
```typescript
const block = new Uint8Array([
  // ═══════════════════════════════════════════════════════════════════
  // BLOCK HEADER
  // ═══════════════════════════════════════════════════════════════════
  0x01,                                    // NumColumns = 1
  0x01,                                    // NumRows = 1

  // ═══════════════════════════════════════════════════════════════════
  // COLUMN HEADER
  // ═══════════════════════════════════════════════════════════════════
  0x03, 0x63, 0x6f, 0x6c,                  // Column name = "col"
  0x19,                                    // Type name length = 25
  // "JSON(max_dynamic_paths=2)"
  0x4a, 0x53, 0x4f, 0x4e, 0x28, 0x6d, 0x61, 0x78, 0x5f,
  0x64, 0x79, 0x6e, 0x61, 0x6d, 0x69, 0x63, 0x5f,
  0x70, 0x61, 0x74, 0x68, 0x73, 0x3d, 0x32, 0x29,

  // ═══════════════════════════════════════════════════════════════════
  // OBJECT STRUCTURE STREAM
  // ═══════════════════════════════════════════════════════════════════
  0x00, 0x00, 0x00, 0x00,                  // Version = 0 (V1)
  0x00, 0x00, 0x00, 0x00,

  0x02,                                    // max_dynamic_paths = 2
  0x02,                                    // num_dynamic_paths = 2
  0x01, 0x61,                              // Path 0: "a"
  0x01, 0x62,                              // Path 1: "b"
                                           // Note: "c" is NOT here - it's in shared data

  // NOTE: No statistics in Native format (NONE mode is default)

  // ═══════════════════════════════════════════════════════════════════
  // DYNAMIC PATH "a" STRUCTURE
  // ═══════════════════════════════════════════════════════════════════
  0x01, 0x00, 0x00, 0x00,                  // Dynamic version = 1 (V1)
  0x00, 0x00, 0x00, 0x00,                  //   (Dynamic V1=1, V2=2, V3=4)
  0x01,                                    // max_dynamic_types = 1
  0x01,                                    // num_dynamic_types = 1
  0x05, 0x49, 0x6e, 0x74, 0x36, 0x34,     // Type: "Int64"
  // NOTE: No Dynamic stats (NONE mode)
  0x00, 0x00, 0x00, 0x00,                  // Variant mode = 0 (COMPACT)
  0x00, 0x00, 0x00, 0x00,

  // ═══════════════════════════════════════════════════════════════════
  // DYNAMIC PATH "b" STRUCTURE (similar to "a")
  // ═══════════════════════════════════════════════════════════════════
  0x01, 0x00, 0x00, 0x00,                  // Dynamic version = 1 (V1)
  0x00, 0x00, 0x00, 0x00,
  0x01,                                    // max_dynamic_types = 1
  0x01,                                    // num_dynamic_types = 1
  0x05, 0x49, 0x6e, 0x74, 0x36, 0x34,     // Type: "Int64"
  0x00, 0x00, 0x00, 0x00,                  // Variant mode = 0 (COMPACT)
  0x00, 0x00, 0x00, 0x00,

  // ═══════════════════════════════════════════════════════════════════
  // DYNAMIC PATH "a" DATA (Int64 value = 1)
  // ═══════════════════════════════════════════════════════════════════
  // Variants sorted alphabetically: Int64=0, SharedVariant=1 (255=NULL)
  0x00,                                    // Discriminator = 0 (Int64 variant)
  0x01, 0x00, 0x00, 0x00,                  // value = 1 (Int64 little-endian)
  0x00, 0x00, 0x00, 0x00,

  // ═══════════════════════════════════════════════════════════════════
  // DYNAMIC PATH "b" DATA (Int64 value = 2)
  // ═══════════════════════════════════════════════════════════════════
  0x00,                                    // Discriminator = 0 (Int64 variant)
  0x02, 0x00, 0x00, 0x00,                  // value = 2 (Int64 little-endian)
  0x00, 0x00, 0x00, 0x00,

  // ═══════════════════════════════════════════════════════════════════
  // SHARED DATA STREAM
  // This is where path "c" lives (overflow from max_dynamic_paths)
  // Format: Array(Tuple(path: String, value: String))
  // ═══════════════════════════════════════════════════════════════════

  // Array offsets (one UInt64 per row):
  0x01, 0x00, 0x00, 0x00,                  // Row 0: offset = 1 (has 1 element)
  0x00, 0x00, 0x00, 0x00,

  // Shared data element 0 (path "c"):
  0x01, 0x63,                              // Path name: "c" (len=1)

  // Binary-encoded Dynamic value for "c":
  0x09,                                    // Value string length = 9 bytes
  0x0a,                                    // BinaryTypeIndex = 0x0a (Int64)
  0x03, 0x00, 0x00, 0x00,                  // value = 3 (Int64 little-endian)
  0x00, 0x00, 0x00, 0x00,
]);
```

**Key observations for shared data:**

1. **Overflow mechanism**: When paths exceed `max_dynamic_paths`, extra paths go to shared data. Here "a" and "b" are dynamic paths, "c" overflows.

2. **Shared data format**: `Array(Tuple(path: String, value: String))`
   - Array offsets indicate how many shared paths each row has
   - Each element is a (path_name, binary_encoded_value) tuple

3. **Binary-encoded values in shared data**: The value is stored as a length-prefixed string containing:
   - `BinaryTypeIndex` (1 byte): Type identifier (0x0a = Int64)
   - Native value: The value in its type's binary format

4. **BinaryTypeIndex values** (common types):
   | Type | Index |
   |------|-------|
   | Nothing | 0x00 |
   | UInt8 | 0x01 |
   | UInt32 | 0x03 |
   | UInt64 | 0x04 |
   | Int64 | 0x0a |
   | String | 0x15 |
   | Array | 0x1e |
   | JSON | 0x30 |

5. **Space tradeoff**: Shared data is less efficient than dynamic paths because each value includes its path name and type encoding. Use `max_dynamic_paths` wisely for your data.

**Example: Variant discriminator ordering (multiple types)**

Variant types are sorted **alphabetically by type name** to determine discriminator values. SharedVariant is included in this sorting. Here's an example showing how discriminators are assigned:

```sql
-- Insert multiple rows with different JSON value types
INSERT INTO test_json VALUES
('{"x": true}'),      -- Bool
('{"x": 3.14}'),      -- Float64
('{"x": "hello"}'),   -- String
('{"x": [1,2,3]}'),   -- Array(Nullable(Int64))
('{"x": null}')       -- NULL
```

The Dynamic column for path "x" will have variant types sorted alphabetically:

| Index | Type | Notes |
|-------|------|-------|
| 0 | Array(Nullable(Int64)) | "A" < "B" < ... |
| 1 | Bool | |
| 2 | Float64 | |
| 3 | SharedVariant | Implicit, always present |
| 4 | String | "Sh..." < "St..." |
| 255 | (NULL) | Special NULL_DISCRIMINATOR |

The discriminator bytes in the serialized data will be:
```
0x01,  // Bool (index 1)
0x02,  // Float64 (index 2)
0x04,  // String (index 4, after SharedVariant=3)
0x00,  // Array (index 0)
0xff,  // NULL (255)
```

**Key rule**: For any type T in a Dynamic column, its discriminator = index in the alphabetically sorted list of [all_types + SharedVariant].

**Path Naming:**

JSON paths use dot notation:
- `a` - top-level key "a"
- `a.b` - nested key "b" inside "a"
- `a.b.c` - deeply nested

ClickHouse can escape dots in JSON keys when `json_type_escape_dots_in_keys=1` is set.

**Typed Paths:**

When a `JSON` type has typed paths declared:
```sql
JSON(user_id UInt64, name String)
```

These paths:
1. Always have the specified type (no inference needed)
2. Are serialized directly in their native type format (not as `Dynamic`)
3. Appear in `ObjectData/ObjectTypedPath` substreams

**Dynamic Paths:**

Dynamic paths are:
1. Discovered at insertion time
2. Limited by `max_dynamic_paths` (default 1024)
3. Stored as `Dynamic` columns with inferred types
4. Appear in `ObjectData/ObjectDynamicPath` substreams

**Shared Data:**

When more than `max_dynamic_paths` unique paths exist:
- Overflow paths are stored in a shared `Dynamic` column
- Each value in shared data includes its full path + value
- Uses binary serialization similar to `Dynamic`'s shared variant

**Format Setting: `output_format_native_write_json_as_string`:**

When set to `1`:
- Uses STRING serialization (version = 1)
- Entire JSON object serialized as text String
- Easier for clients that don't support complex `JSON` deserialization
- Default: `0` (use structured serialization)

**Format Setting: `output_format_native_use_flattened_dynamic_and_json_serialization`:**

When set to `1`:
- Uses FLATTENED serialization (version = 3)
- All paths serialized as individual columns
- No `Dynamic` wrapper, just direct column data
- Easier for some clients to parse
- Default: `0`

**Reading JSON Columns:**

To deserialize a `JSON` column in standard mode:

1. Read `ObjectStructure` to get version and path list
2. For each typed path: deserialize using its known type
3. For each dynamic path: deserialize as `Dynamic` column
4. If shared data exists: deserialize the shared `Dynamic` column
5. Reconstruct JSON objects by merging values from all paths

In STRING mode: just read the String value containing JSON text.

In FLATTENED mode: read each path as a separate column, with a NULL indicator for missing paths.
