# Binary formats overview

## Introduction

This document covers basic concepts of three main binary formats used in ClickHouse:

- [RowBinary](about:blank#rowbinary): row-based format. Rows and their values are listed consecutively, without separators.
- [RowBinaryWithNamesAndTypes](about:blank#rowbinarywithnamesandtypes): similar to `RowBinary`, but with [additional metadata](about:blank#header-structure) about column names and types.
- [Native](about:blank#native): columnar format streamed as [blocks](about:blank#block-structure). Each block contains a header with metadata about the columns, followed by the column data itself.

> Do not confuse Native format with the TCP (Native) interface.
> 

## Recordings

- [The first recording of the document review](https://drive.google.com/file/d/1TY35nih3ft_ebOdWDybmlSdKV-52mSaw/view).
- *The second and the following recordings (if any) will be added here*

## RowBinary

> See also: RowBinary ClickHouse docs.
> 

### Data types wire format

> Most of the queries provided in the examples can be executed with curl with a file output.
> 
> 
> ```bash
> curl -XPOST "http://localhost:8123?default_format=RowBinary" \
>   --data-binary "SELECT 42 :: UInt32" \  > out.bin
> ```
> 
> Then, the data can be examined with a hex editor like [ImHex](https://github.com/WerWolv/ImHex).
> 

### Unsigned LEB128 (Little Endian Base 128)

An **unsigned little-endian** variable-width integer is used to hint of a length for some data types, such as `String`, `Array`, `Map`, and `Tuple`. A sample implementation can be found on the [LEB128 wiki page](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer).

### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256

All integer types are encoded with an appropriate number of bytes as **little-endian**. Most languages support extracting such integers from byte arrays, using either built-in tools, or well-known libraries.

### Bool

Boolean values are encoded as a single byte, and can be deserialized similarly to `UInt8`.

- `0` is `false`
- `1` is `true`

### Float32, Float64

**Little-endian** floating-point numbers encoded as 4 bytes for `Float32` and 8 bytes for `Float64`. Similarly to integers, most languages provide proper tools to deserialize these values.

### BFloat16

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point) is a 16-bit floating point format with the range of Float32 and reduced precision, making it useful for machine learning workloads. The wire format is essentially the top 16 bits of a Float32 value. If your language doesn't support it natively, the easiest way to handle it is to read and write as UInt16, converting to and from Float32:

To convert BFloat16 to Float32:

```tsx
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
const bfloat16Bits = readUInt16()
const float32Bits = bfloat16Bits << 16
const floatValue = reinterpretAsFloat32(float32Bits)
```

To convert Float32 to BFloat16:

```tsx
// Right-shift Float32 bits by 16 to truncate to BFloat16
const float32Bits = reinterpretAsUInt32(floatValue)
const bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

Sample underlying values for `BFloat16`:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```tsx
const data = new Uint8Array([
  0xA0, 0x3F, // 1.25 as BFloat16
])
```

### Decimal32, Decimal64, Decimal128, Decimal256

Decimal types are represented as **little-endian** integers with respective bit width.

- `Decimal32` - 4 bytes, or `Int32`.
- `Decimal64` - 8 bytes, or `Int64`.
- `Decimal128` - 16 bytes, or `Int128`.
- `Decimal256` - 32 bytes, or `Int256`.

When deserializing a Decimal value, the whole and fractional parts can be derived using the following pseudocode:

```tsx
let scale_multipler = 10 ** scale
let whole_part = ~~(value / scale_multiplier)
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

Where `~~` is the “fast” floor division operator, and `scale` is the number of digits after the decimal point. For example, for `Decimal(10, 2)` (an equivalent to `Decimal32(2)`), the scale is `2`, and the value `12345` will be represented as `(123, 45)`.

Serialization requires the reverse operation:

```tsx
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

See more details in the [Decimal types ClickHouse docs](https://clickhouse.com/docs/sql-reference/data-types/decimal).

### String

Encoded in two parts:

1. A variable-length integer (LEB128) that indicates the length of the string in bytes.
2. The string itself, encoded as a sequence of bytes.

For example, a string `foobar` will be encoded using *seven* bytes as follows:

```tsx
const data = new Uint8Array([
  0x06, // LEB128 length of the string (6)
  0x66, // 'f'
  0x6f, // 'o'
  0x6f, // 'o'
  0x62, // 'b'
  0x61, // 'a'
  0x72, // 'r'
])
```

### FixedString

Unlike `String`, `FixedString` has a fixed length, which is defined in the schema. It is encoded as a sequence of bytes, padded with zeroes if necessary.

An empty `FixedString(3)` contains only padding zeroes:

```tsx
const data = new Uint8Array([0x00, 0x00, 0x00])
```

Non-empty `FixedString(3)` containing the string `hi`:

```tsx
const data = new Uint8Array([
  0x68, // 'h'
  0x69, // 'i'
  0x00, // padding zero
])
```

Non-empty `FixedString(3)` containing the string `bar`:

```tsx
const data = new Uint8Array([
  0x62, // 'b'
  0x61, // 'a'
  0x72, // 'r'
])
```

No padding is required in the last example, since all *three* bytes are used.

### Date

Stored as `UInt16` (two bytes) representing the number of days ***since*** `1970-01-01`.

Supported range of values: `[1970-01-01, 2149-06-06]`.

Sample underlying values for `Date`:

- TODO

### Date32

Stored as `Int32` (four bytes) representing the number of days ***before or after*** `1970-01-01`.

Supported range of values: `[1900-01-01, 2299-12-31]`

Sample underlying values for `Date32`:

- TODO

### DateTime

Stored as `UInt32` (four bytes) representing the number of seconds ***since*** `1970-01-01 00:00:00 UTC`.

Syntax:

```
DateTime([timezone])
```

For example, `DateTime` or `DateTime('UTC')`.

Supported range of values: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

Sample underlying values for `DateTime`:

- TODO

### DateTime64

Stored as `Int64` (eight bytes) representing the number of **ticks** ***before or after*** `1970-01-01 00:00:00 UTC`. Tick resolution is defined by the `precision` parameter, see the syntax below:

```
DateTime64(precision, [timezone])
```

Where `precision` is an integer from `0` to `9`. Typically, only the following are used: `3` (milliseconds), `6` (microseconds),
`9` (nanoseconds).

Examples of valid DateTime64 definitions: `DateTime64(0)`, `DateTime64(3)`, `DateTime64(6, 'UTC')`, or `DateTime64(9, 'Europe/Amsterdam')`.

> [!NOTE]
Variants like DateTime64(2) or DateTime64(5, 'UTC') are also valid, while not commonly used.
> 

The underlying `Int64` value of the `DateTime64` type could be used as number of the following units before or after the UNIX epoch:

- `DateTime64(0)` - seconds.
- `DateTime64(3)` - milliseconds.
- `DateTime64(6)` - microseconds.
- `DateTime64(9)` - nanoseconds.

Supported range of values: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

Sample underlying values for `DateTime64`:

- `DateTime64(3)`: value `1546300800000` represents `2019-01-01 00:00:00 UTC`.
- TODO

> [!NOTE]
The precision of the maximum value is 8. If the maximum precision of 9 digits (nanoseconds) is used, the maximum supported value is 2262-04-11 23:47:16 in UTC.
> 

### Time

Stored as `Int32` representing a time value in seconds. Negative values are valid.

Supported range of values: `[-999:59:59, 999:59:59]` (i.e., `[-3599999, 3599999]` seconds).

> [!NOTE]
At the moment, the setting enable_time_time64_type must be set to 1 to use Time or Time64.
> 

Sample underlying values for `Time`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```tsx
const data = new Uint8Array([
  0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
])
```

### Time64

Internally stored as a `Decimal64` (which is stored as `Int64`) representing a time value with fractional seconds, with configurable precision. Negative values are valid. 

Syntax:

```
Time64(precision)
```

Where `precision` is an integer from `0` to `9`. Common values: `3` (milliseconds), `6` (microseconds), `9` (nanoseconds).

Supported range of values: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

> [!NOTE]
At the moment, the setting enable_time_time64_type must be set to 1 to use Time or Time64.
> 

The underlying `Int64` value represents fractional seconds scaled by `10^precision`.

Sample underlying values for `Time64`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```tsx
const data = new Uint8Array([
  0x40, 0x82, 0x0D, 0x06,
  0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
])
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

### Enum8, Enum16

Stored as a single byte (`Enum8` == `UInt8`) or two bytes (`Enum16` == `UInt16`) representing the index of the enum value in the enum definition.

Note that an Enum can be defined in a fairly simple way, like this:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```
   ┌─e─────┐
1. │ hello │
   └───────┘
```

The Enum8 defined above will have the following values map on the client:

```
Map<UInt8, String> {
  1: 'hello',
  2: 'world'
}
```

Or in a more complex way, like this:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

The Enum16 defined above will have the following values map on the client:

```
Map<UInt16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

As you can see, for the data type parser, the main challenge is to track the escaped symbols in the enum definition, such as `\'`, and special symbols like `=` that may appear in the quotes.

### UUID

Represented as a sequence of 16 bytes.

Sample underlying values for `UUID`:

- `61f0c404-5cb3-11e7-907b-a6006ad3dba0` is represented as:

```tsx
const data = new Uint8Array([
  0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
  0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
])
```

- The default UUID `00000000-0000-0000-0000-000000000000` is represented as 16 zero bytes:

```tsx
const data = new Uint8Array([
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
 ])
```

It can be used when a new record was inserted, but the UUID value was not specified.

### IPv4

Stored in four bytes as `UInt32`. Sample underlying values for `IPv4`:

```sql
SELECT    
    CAST('0.0.0.0',         'IPv4') AS a,
    CAST('127.0.0.1',       'IPv4') AS b,
    CAST('192.168.0.1',     'IPv4') AS c,
    CAST('255.255.255.255', 'IPv4') AS d,
    CAST('168.212.226.204', 'IPv4') AS e
```

```tsx
const data = new Uint8Array([
  0x00, 0x00, 0x00, 0x00, // 0.0.0.0
  0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
  0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
  0xff, 0xff, 0xff, 0xff, // 255.255.255.255
  0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
])
```

### IPv6

Stored in 16 bytes, *but it does not correspond to `(U)Int128`*. Sample underlying values for `IPv6`:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```tsx
const data = new Uint8Array([
  // 2a02:aa08:e000:3100::2
  0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
  // 2001:44c8:129:2632:33:0:252:2
  0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
  0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
  // 2a02:e980:1e::1
  0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 
])
```

### Nullable

A nullable data type is encoded as follows:

1. A single byte that indicates whether the value is `NULL` or not:
    - `0x00` means the value is not `NULL`.
    - `0x01` means the value is `NULL`.
2. If the value is not `NULL`, the underlying data type is encoded as usual.

For example, a `Nullable(UInt32)` value:

```sql
SELECT    CAST(42,   'Nullable(UInt32)') AS a,
    CAST(NULL, 'Nullable(UInt32)') AS b
```

```tsx
const data = new Uint8Array([
  0x00,                   // Not NULL - the value follows
  0x2A, 0x00, 0x00, 0x00, // UInt32(42)
  0x01,                   // NULL - nothing follows
])
```

### LowCardinality

Low-cardinality marker does not affect the wire format. For example, a `LowCardinality(String)` is encoded the same way as a regular `String`.

> [!NOTE]
A column can be defined as LowCardinality(Nullable(T)), but it is not possible to define it as Nullable(LowCardinality(T)) - it will always result in an error from the server.
> 

While testing, [allow_suspicious_low_cardinality_types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) can be set to `1` to allow most of the data types inside `LowCardinality` for better coverage.

### Array

An array is encoded as follows:

1. A [variable-length integer (LEB128)](about:blank#unsigned-leb128-little-endian-base-128) that indicates the number of elements in the array.
2. The elements of the array, encoded in the same way as the underlying data type.

For example, an array with `UInt32` values:

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```tsx
const data = new Uint8Array([
  0x03,                   // LEB128 - the array has 3 elements
  0x01, 0x00, 0x00, 0x00, // UInt32(1)
  0x02, 0x00, 0x00, 0x00, // UInt32(2)
  0x03, 0x00, 0x00, 0x00, // UInt32(3)
])
```

A slightly more complex example:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```tsx
const data = new Uint8Array([
  0x02,             // LEB128 - the array has 2 elements
  0x06,             // LEB128 - the first string has 6 bytes
  0x66, 0x6f, 0x6f, 
  0x62, 0x61, 0x72, // 'foobar'
  0x03,             // LEB128 - the second string has 3 bytes
  0x71, 0x61, 0x7a, // 'qaz'
])
```

> [!NOTE]
An array can contain nullable values, but the array itself cannot be nullable.
> 

The following is valid:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

And it will be encoded as follows:

```tsx
const data = new Uint8Array([
  0x02,             // LEB128  - the array has 2 elements
  0x01,             // Is NULL - nothing follows for this element
  0x00,             // Is NOT NULL - the data follows
  0x03,             // LEB128  - the string has 3 bytes
  0x66, 0x6f, 0x6f, // 'foo'
])
```

An example of dealing with multidimensional arrays can be found in the [Geo section](about:blank#geo).

### Tuple

A tuple is encoded as all elements of the tuple following each other in their corresponding wire format without any additional meta-information or delimiters.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```tsx
const data = new Uint8Array([
  0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
  0x03,                   // LEB128 - the string has 3 bytes
  0x66, 0x6f, 0x6f,       // 'foo'
  0x02,                   // LEB128 - the array has 2 elements
  0x63,                   // 99 as UInt8
  0x90,                   // 144 as UInt8
])
```

The string encoding of the tuple data type presents the similar challenges as with the [Enum type](about:blank#enum8-enum16), such as tracking the escaped symbols and special characters; now, with Tuple it is also required to track open and closing parenthesis. Additionally, note that the most complex Tuples can contain other nested Tuples, Arrays, Maps, and even enums.

For example, in the following table, the tuple contains an enum with a tick and parenthesis in the name, which can cause parsing issues if not handled properly:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),          Array(Nullable(Tuple(UInt32, String)))       ))ENGINE = Memory;
```

### Map

A map can be viewed as an `Array(Tuple(K, V))`, where `K` is the key type and `V` is the value type. The map is encoded as follows:

1. A [variable-length integer (LEB128)](about:blank#unsigned-leb128-little-endian-base-128) that indicates the number of elements in the map.
2. The elements of the map as key-value pairs, encoded as their corresponding types.

For example, a map with `String` keys and `UInt32` values:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```tsx
const data = new Uint8Array([
  0x02,                   // LEB128 - the map has 2 elements
  0x03,                   // LEB128 - the first key has 3 bytes
  0x66, 0x6f, 0x6f,       // 'foo'
  0x01, 0x00, 0x00, 0x00, // UInt32(1)
  0x03,                   // LEB128 - the second key has 3 bytes
  0x62, 0x61, 0x72,       // 'bar'
  0x02, 0x00, 0x00, 0x00, // UInt32(2)
])
```

> It is possible to have maps with deeply nested structures, such as Map(String, Map(Int32, Array(Nullable(String)))), which will be encoded similarly to what is described above.
> 

### Variant

This type represents a union of other data types. Type `Variant(T1, T2, ..., TN)` means that each row of this type has a value of either type `T1` or `T2` or … or `TN` or none of them (`NULL` value).

> [!WARNING]
While for the end user Variant(T1, T2) means exactly the same as Variant(T2, T1), the order of types in the definition matters for the wire format: the types in the definition are always sorted alphabetically, and this is important, since the exact variant is encoded by a “discriminant” - the data type index in the definition.
> 

Consider the following example:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- it does not matter what is the order of types in the user input  -- the types are always sorted alphabetically in the wire format  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES(true), ('foo'), (CAST(100, 'Int128')), (array(1,2,3));
SELECT * FROM foo;
```

```tsx
const data = new Uint8Array([
   0x01,                               // type index -> Bool
   0x01,                               // true
   0x03,                               // type index -> FixedString(6)
   0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
   0x05,                               // type index -> Float64
   0x00, 0x00, 0x00, 0x00, 
   0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
   0x06,                               // type index -> Int128
   0x64, 0x00, 0x00, 0x00, 
   0x00, 0x00, 0x00, 0x00, 
   0x00, 0x00, 0x00, 0x00, 
   0x00, 0x00, 0x00, 0x00,             // 100 as Int128
   0x00,                               // type index -> Array(Int16)
   0x03,                               // LEB128 - the array has 3 elements
   0x01, 0x00,                         // 1 as Int16
   0x02, 0x00,                         // 2 as Int16
   0x03, 0x00,                         // 3 as Int16
])
```

[allow_suspicious_variant_types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) setting can be used to allow more exhaustive testing of the `Variant` type.

### Dynamic

The `Dynamic` type can hold values of any type, determined at runtime. In RowBinary format, each value is self-describing: the first part is the type specification in [**this format](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding).** The contents then follow, with the value encoding as described in this document. So to parse you you just need to use the type index to determine the right parser and then re-use the RowBinary parsing you already have elsewhere. 

```
[BinaryTypeIndex][type-specific parameters...][value]
```

Where `BinaryTypeIndex` is a single byte identifying the type. See the reference [here](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding) for the type indices and parameters.

**Examples:**

```sql
SELECT 42::Dynamic
```

```
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

### JSON

The JSON type encodes data in two distinct categories:

1. **Typed Paths** - Paths declared with explicit types in the schema (e.g., `JSON(user_id UInt32, name String)`)
2. **Dynamic Paths/Overflow paths when dynamic path limit is exceeded** - Runtime-discovered paths stored as `Dynamic` type. The value encoding is preceded by the type definition.

The wire format and rules are different for these two categories.

| Path Category | Included in Serialization | Value Encoding | Variant/Nullable allowed |
| --- | --- | --- | --- |
| **Typed paths** | Always (even if NULL) | Type-specific binary format | Yes |
| **Dynamic paths** | Only if non-null | Dynamic | No |

Each JSON row in RowBinary format is serialized as:

```
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**Examples:**

1. Simple JSON with typed paths only:

Schema: `JSON(user_id UInt32, active Bool)`

Row: `{"user_id": 42, "active": true}`

Binary encoding (hex with annotations):

```
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

1. Simple JSON with typed and dynamic paths:

Schema: `JSON(user_id UInt32, active Bool)`

Row: `{"user_id": 42, "active": true, "name": "Alice"}`

Binary encoding (hex with annotations):

```
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

1. Null handling:

With typed nullable column you get null:

Schema: `JSON(score Nullable(Int32))`

Row: `{"score": null }`

Binary encoding (hex with annotations):

```
02                              # VarUInt: 2 paths total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

With typed non-nullable column, you get the default value:

JSON(name String)

Schema: `JSON(name String)`

Row: `{"name": null}`

Binary encoding:

```
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

With dynamic path, it is ignored:

Schema: `JSON(id UInt64)`

Row: `{"id": 100, "metadata": null}`

Binary encoding:

```
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

Note: The `metadata` path with NULL value is **not included** because dynamic paths are only serialized when non-null. This is a key difference from typed paths.

1. Nested JSON objects:

Schema: `JSON()`

Row: `{"user": {"name": "Bob", "age": 30}}`

Binary encoding (hex with annotations):

```
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

Note: Nested objects are flattened into dot-separated paths (e.g., `user.name` instead of a nested structure).

**Alternative: JSON as String Mode**

With the setting `output_format_binary_write_json_as_string=1`, JSON columns are serialized as a single JSON text string instead of the structured binary format. There is a corresponding setting for writing to JSON columns, `input_format_binary_read_json_as_string`. The choice of setting here comes down to whether you want to parse the json in the client or the server.

### Geo types

Geo is a category of data types that represent geographical data. It includes:

- `Point` - as `Tuple(Float64, Float64)`.
- `Ring` - as `Array(Point)`, or `Array(Tuple(Float64, Float64))`.
- `Polygon` - as `Array(Ring)`, or `Array(Array(Tuple(Float64, Float64)))`.
- `MultiPolygon` - as `Array(Polygon)`, or `Array(Array(Array(Tuple(Float64, Float64))))`.
- `LineString` - as `Array(Point)`, or `Array(Tuple(Float64, Float64))`.
- `MultiLineString` - as `Array(LineString)`, or `Array(Array(Tuple(Float64, Float64)))`.

The wire format of the Geo values is exactly the same as with Tuple and Array. [RowBinaryWithNamesAndTypes](about:blank#rowbinarywithnamesandtypes)
and [Native](about:blank#native) formats headers will contain the aliases for these types, e.g., `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString`, and `MultiLineString`.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```tsx
const data = new Uint8Array([
  // Point - or Tuple(Float64, Float64)
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
  // Ring - or Array(Tuple(Float64, Float64))
  0x02, // LEB128 - the "ring" array has 2 points
     // Ring - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
     // Ring - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
  // Polygon - or Array(Array(Tuple(Float64, Float64)))
  0x02, // LEB128 - the "polygon" array has 2 rings
     0x02, // LEB128 - the first ring has 2 points
        // Polygon - Ring #1 - Point #1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
        // Polygon - Ring #1 - Point #2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
    0x01, // LEB128 - the second ring has 1 point
        // Polygon - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
        0x00, 0x00, 0x00, 0x00,  0x00, 0x00, 0x28, 0x40, 
  // MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
  0x01, // LEB128 - the "multi_polygon" array has 1 polygon
     0x02, // LEB128 - the first polygon has 2 rings
        0x02, // LEB128 - the first ring has 2 points
           // MultiPolygon - Polygon #1 - Ring #1 - Point #1
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
           // MultiPolygon - Polygon #1 - Ring #1 - Point #2
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
        0x01, // LEB128 - the second ring has 1 point
          // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
   // LineString - or Array(Tuple(Float64, Float64))
   0x02, // LEB128 - the line string has 2 points
      // LineString - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
      // LineString - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
   // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
   0x02, // LEB128 - the multi line string has 2 line strings
     0x02, // LEB128 - the first line string has 2 points
       // MultiLineString - LineString #1 - Point #1
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
       // MultiLineString - LineString #1 - Point #2
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
     0x01, // LEB128 - the second line string has 1 point
       // MultiLineString - LineString #2 - Point #1 (the only one)
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40, 
])
```

### Geometry

`Geometry` is a `Variant` type that can hold any of the Geo types listed above. On the wire, it is encoded exactly like a `Variant`, with a discriminant byte indicating which geo type follows.

The discriminant indices for Geometry are:

| Index | Type |
| --- | --- |
| 0 | LineString |
| 1 | MultiLineString |
| 2 | MultiPolygon |
| 3 | Point |
| 4 | Polygon |
| 5 | Ring |

Wire format structure:

```tsx
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

Sample encoding of a `Point` as `Geometry`:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```tsx
const data = new Uint8Array([
  0x03,                                           // discriminant = 3 (Point)
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
])
```

Sample encoding of a `Ring` as `Geometry`:

```tsx
const data = new Uint8Array([
  0x05,       // discriminant = 5 (Ring)
  0x02,       // LEB128 - array has 2 points
  // Point #1
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
  // Point #2
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
])
```

### Nested

In the wire format, `Nested` is represented as a sequence of arrays, where each array corresponds to a column in the nested structure. Each array is encoded as described in the [Array](about:blank#array) section.

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested    (
        a String,
        b Int32
    )
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES (array('foo', 'bar'), array(42, 144))
```

Note that even the `DESCRIBE TABLE foo` will show the `Nested` structure as a sequence of arrays:

```
DESCRIBE TABLE foo

   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

Sample wire format for the `Nested` structure above:

```tsx
const data = new Uint8Array([
   0x02,                   // LEB128 - 2 String elements in the first array
   0x03,                   // LEB128 - the first string has 3 bytes
   0x66, 0x6F, 0x6F,       // 'foo'
   0x03,                   // LEB128 - the second string has 3 bytes
   0x62, 0x61, 0x72,       // 'bar'
   0x02,                   // LEB128 - 2 Int32 elements in the second array
   0x2A, 0x00, 0x00, 0x00, // 42 as Int32
   0x90, 0x00, 0x00, 0x00, // 144 as Int32
])
```

### SimpleAggregateFunction

TODO

### QBit

`QBit` vector type for efficient lookup with different levels of precision. Internally it’s stored in a transposed format. On the wire, QBit is simply an `Array` of the underlying element type (`Float32`, `Float64`, or `BFloat16`). The bit-transpose optimization for storage happens server-side, not in the RowBinary protocol.

Syntax:

```
QBit(element_type, dimension)
```

Where `element_type` is `Float32`, `Float64`, or `BFloat16`, and `dimension` is the fixed vector dimension.

Wire format: identical to `Array(element_type)`:

```tsx
// LEB128 length
// followed by `length` elements of `element_type`
```

Sample encoding of `QBit(Float32, 4)` containing `[1.0, 2.0, 3.0, 4.0]`:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```tsx
const data = new Uint8Array([
  0x04,                   // LEB128 - array has 4 elements
  0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
  0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
  0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
  0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
])
```

---

## RowBinaryWithNamesAndTypes

> [!TIP]
See also: RowBinaryWithNamesAndTypes ClickHouse docs.
> 

Often abbreviated as **RBWNAT** for convenience. Used as the main format for [clickhouse-java](https://github.com/ClickHouse/clickhouse-java) and [clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) (WIP) clients.

The wire format is exactly the same as `RowBinary`, but it adds a header with the following information that enables the client to ensure proper (de)serialization of the data.

### Header structure

- [LEB128](about:blank#unsigned-leb128-little-endian-base-128) as `N`: number of columns in the resulting rows.
- `N` times: [String](about:blank#string) as the name of the column.
- `N` times: [String](about:blank#string) as the type of the column.

An example of the **RBWNAT** header for a table with three columns, `id UInt32`, `name String` and `sku Array(UInt64)`:

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinaryWithNamesAndTypes" \  --data-binary "SELECT 42 :: UInt32        AS id,
               'foobar'  :: String        AS name,
                array(23) :: Array(UInt64) AS sku                
                LIMIT 0" \  > out.bin
```

```tsx
const data = new Uint8Array([
  0x03,                                     // LEB128 - rows have 3 columns 
  0x02,                                     // LEB128 - str has 2 bytes
  0x69, 0x64,                               // id
  0x04,                                     // LEB128 - str has 4 bytes
  0x6E, 0x61, 0x6D, 0x65,                   // name
  0x03,                                     // LEB128 - str has 3 bytes
  0x73, 0x6B, 0x75,                         // sku
  0x06,                                     // LEB128 - str has 6 bytes
  0x55, 0x49, 0x6E, 0x74, 0x33, 0x32,       // UInt32
  0x06,                                     // LEB128 - str has 6 bytes
  0x53, 0x74, 0x72, 0x69, 0x6E, 0x67,       // String
  0x0D,                                     // LEB128 - str has 13 bytes
  0x41, 0x72, 0x72, 0x61, 0x79, 0x28, 0x55, 
  0x49, 0x6E, 0x74, 0x36, 0x34, 0x29,       // Array(UInt64)
])
```

Currently, the client has to parse the data types AST from the strings provided in the header.

---