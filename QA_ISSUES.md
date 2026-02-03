# QA Issues Found - RowBinary/Native Decoder Edge Cases

This document summarizes bugs found during QA testing of the RowBinary and Native decoders.

## Critical Issues (Parsing Failures)

### 1. Enum8 with Negative Values - Both Decoders

**Severity:** High
**Location:** `rowbinary-decoder.ts:697` and equivalent in native-decoder.ts

**Description:**
Enum8 can store values from -128 to 127 (Int8), but the decoder reads enum values as `UInt8` and looks them up in the value map. When the enum has negative values (e.g., `'neg' = -128`), the decoder reads `128` instead of `-128`, failing to find the enum name.

**Query to reproduce:**
```sql
SELECT 'neg'::Enum8('neg' = -128, 'pos' = 127) as val
```

**Expected:** `'neg'`
**Actual:** `'<unknown:128>'`

**Root Cause:**
`decodeEnum8()` uses `this.reader.readUInt8()` but should use `this.reader.readInt8()` since Enum8 values are stored as signed Int8.

The parser correctly stores enum values like -128 as Map keys (`type-parser.ts:321`). However, the decoder reads the byte 0x80 as unsigned 128 instead of signed -128, so the Map lookup fails.

```typescript
// Current (buggy):
const { value, range } = this.reader.readUInt8();  // reads 0x80 as 128

// Should be:
const { value, range } = this.reader.readInt8();   // reads 0x80 as -128
```

**Fix Location:**
- `src/core/decoder/rowbinary-decoder.ts:698` - Change `readUInt8()` to `readInt8()`
- `src/core/decoder/native-decoder.ts:763` - Change `readUInt8()` to `readInt8()`

---

### 2. JSON with Nested Array of Objects - RowBinary Decoder

**Severity:** High
**Location:** `rowbinary-decoder.ts:1110`

**Description:**
When decoding JSON that contains arrays of objects, the decoder throws `Unknown BinaryTypeIndex: 0x30`. This indicates the decoder doesn't handle all BinaryTypeIndex values used in the JSON Dynamic encoding.

**Query to reproduce:**
```sql
SELECT '{"items": [{"id": 1}, {"id": 2}]}'::JSON as val
-- Requires: allow_experimental_json_type=1
```

**Expected:** Successfully decode the nested JSON structure
**Actual:** `Error: Unknown BinaryTypeIndex: 0x30`

**Root Cause:**
The `decodeDynamicType()` switch statement is missing the case for BinaryTypeIndex `0x30` which is **JSON**. When JSON contains nested objects/arrays, those nested JSON values are encoded with type index `0x30`, but the decoder doesn't handle this recursive case.

Per the spec (`jsonspec.md:538` and `nativespec.md:2718`):
```
| JSON | 0x30 |
```

**Fix Location:**
- `src/core/decoder/rowbinary-decoder.ts:942-1111` - Add case for `0x30` (JSON) in `decodeDynamicType()`

---

### 3. JSON with Nested Array of Objects - Native Decoder

**Severity:** High
**Location:** `native-decoder.ts:2219`

**Description:**
When decoding JSON that contains arrays of objects, the Native decoder throws `RangeError: Offset is outside the bounds of the DataView`. This indicates the decoder is reading past the end of the buffer.

**Query to reproduce:**
```sql
SELECT '{"items": [{"id": 1}, {"id": 2}]}'::JSON as val
-- Requires: allow_experimental_json_type=1
```

**Expected:** Successfully decode the nested JSON structure
**Actual:** `RangeError: Offset is outside the bounds of the DataView` at `decodeJSONColumnV1`

**Root Cause:**
The JSON V1 decoder appears to miscalculate offsets or misparse the structure when dealing with arrays containing objects, leading to reading past buffer boundaries.

**Fix Location:**
- `src/core/decoder/native-decoder.ts:2144` and `native-decoder.ts:2219`

---

## Minor Issues (Cosmetic / Non-Failures)

### 4. Negative Zero Float64 Preservation

**Severity:** Low (Not a bug - correct IEEE 754 behavior)
**Location:** Both decoders

**Description:**
The decoders correctly preserve IEEE 754 negative zero (`-0.0`) rather than normalizing it to positive zero. This is actually correct behavior per the IEEE 754 specification, but may be unexpected.

**Query:**
```sql
SELECT -0.0::Float64 as val
```

**Result:** `-0` (correct IEEE 754 preservation)

**Notes:** No fix needed - this is correct behavior. Tests should account for this.

---

### 5. DateTime Timezone Not in typeString (Native)

**Severity:** Low
**Location:** Native decoder

**Description:**
When querying `DateTime('Pacific/Kiritimati')`, the Native format column's `typeString` shows just `DateTime` without the timezone parameter. This may be ClickHouse returning the type differently in Native format.

**Query:**
```sql
SELECT '2024-01-15 12:30:45'::DateTime('Pacific/Kiritimati') as val
```

**Expected:** typeString contains `Pacific/Kiritimati`
**Actual:** typeString is just `DateTime`

**Notes:** May be ClickHouse behavior rather than decoder issue. Needs investigation.

---

## Test Issues (Not Decoder Bugs)

### 6. Variant with Multiple Similar Integer Types

**Status:** Test issue, not decoder bug

ClickHouse rejects Variants with similar types (e.g., `Variant(UInt8, UInt16, UInt32)`) unless `allow_suspicious_variant_types=1` is set. Tests need to add this setting.

### 7. Nested Type Cast Syntax

**Status:** Test issue, not decoder bug

The test used incorrect syntax `[1, 2]::Nested(id UInt8)`. Nested types require table creation and INSERT to test properly.

---

---

## Dynamic Type Issues

### 4. Array inside Dynamic has extra nesting level - RowBinary

**Severity:** High
**Location:** `rowbinary-decoder.ts` - `decodeDynamic()`

**Description:**
When decoding an Array type wrapped in Dynamic, the resulting AST has an extra nesting level. The value property contains `[[1, 2, 3]]` instead of `[1, 2, 3]`.

**Query to reproduce:**
```sql
CREATE TABLE test (id UInt32, val Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3]::Array(UInt8));
SELECT val FROM test;
```

**Expected:** `children[1].value = 1` (direct array elements after length node)
**Actual:** `children[1].value = [1, 2, 3]` (array wrapped in another level)

---

### 5. DateTime64 in Dynamic shows raw timestamp - RowBinary

**Severity:** Medium
**Location:** `rowbinary-decoder.ts` - `decodeDynamic()`

**Description:**
When DateTime64 is stored in a Dynamic column and decoded, the displayValue shows the raw Unix timestamp (e.g., `1705321845.123`) instead of a formatted date string.

**Query to reproduce:**
```sql
CREATE TABLE test (id UInt32, val Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (1, toDateTime64('2024-01-15 12:30:45.123', 3));
SELECT val FROM test;
```

**Expected:** displayValue contains `2024-01-15`
**Actual:** displayValue is `1705321845.123`

---

### 6. Native decoder - Complex types in Dynamic have garbled values

**Severity:** High
**Location:** `native-decoder.ts` - Dynamic column decoding

**Description:**
When decoding Array or Tuple types inside Dynamic columns in Native format, the values are garbled or incorrectly structured. Arrays show empty, Tuples show raw bytes.

**Query to reproduce:**
```sql
SELECT [1, 2, 3]::Array(UInt32)::Dynamic as val  -- Native format
SELECT (42, 'test')::Tuple(UInt32, String)::Dynamic as val  -- Native format
```

**Expected:** Properly decoded array/tuple values
**Actual:** Empty arrays, garbled tuple values (raw bytes instead of proper types)

---

### 7. Deeply nested arrays in Dynamic fail - Both decoders

**Severity:** Medium
**Location:** Both decoders

**Description:**
Deeply nested arrays (3+ levels) inside Dynamic columns fail to decode properly. RowBinary shows incorrect nesting, Native crashes with undefined children.

**Query to reproduce:**
```sql
SELECT [[[1]]]::Array(Array(Array(UInt8)))::Dynamic as val
```

---

### 8. Named Tuple labels sorted alphabetically - RowBinary

**Severity:** Medium
**Location:** `rowbinary-decoder.ts` - Dynamic Tuple decoding

**Description:**
When decoding a Named Tuple inside Dynamic, the field labels appear to be sorted alphabetically rather than in their original definition order.

**Query to reproduce:**
```sql
SELECT CAST((1, 'x'), 'Tuple(id UInt32, name String)')::Dynamic as val
```

**Expected:** First child has label `id`, second has label `name`
**Actual:** Labels are reordered (e.g., `name` before `id` or `type` instead of `id`)

---

### 9. Tuple values misaligned in Dynamic - RowBinary

**Severity:** High
**Location:** `rowbinary-decoder.ts` - Dynamic Tuple decoding

**Description:**
When decoding a Tuple inside Dynamic, the values in children nodes don't match the expected tuple elements. The decoder appears to mix type metadata with actual values.

**Query to reproduce:**
```sql
SELECT (42, 'test')::Tuple(UInt32, String)::Dynamic as val
```

**Expected:** `children[0].value = 42`, `children[1].value = 'test'`
**Actual:** `children[0].value = 31` (or other unexpected value)

---

## Summary

| Issue | Severity | Decoder | Status |
|-------|----------|---------|--------|
| Enum8 negative values | High | Both | **Fix Required** |
| JSON array of objects | High | RowBinary | **Fix Required** |
| JSON array of objects | High | Native | **Fix Required** |
| Array in Dynamic extra nesting | High | RowBinary | **Fix Required** |
| DateTime64 in Dynamic raw timestamp | Medium | RowBinary | **Fix Required** |
| Complex types in Dynamic garbled | High | Native | **Fix Required** |
| Deeply nested arrays in Dynamic | Medium | Both | **Fix Required** |
| Named Tuple labels sorted | Medium | RowBinary | **Fix Required** |
| Tuple values misaligned in Dynamic | High | RowBinary | **Fix Required** |
| Negative zero | Low | Both | Working as intended |
| DateTime timezone | Low | Native | Investigation needed |

---

## Test Files

**Edge case tests:**
`src/core/decoder/qa-edge-cases.integration.test.ts`

Run with:
```bash
npm test -- --testNamePattern="QA Edge Case"
```

**Dynamic type exhaustive tests:**
`src/core/decoder/dynamic-exhaustive.integration.test.ts`

Run with:
```bash
npm test -- src/core/decoder/dynamic-exhaustive.integration.test.ts
```

All known-failing tests are marked with `it.fails()` to document the decoder bugs while keeping the test suite green.
