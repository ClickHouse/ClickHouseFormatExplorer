-- Comprehensive query selecting every ClickHouse type
-- Use with: FORMAT RowBinaryWithNamesAndTypes or FORMAT Native
-- Required settings are at the bottom

SELECT
    -- ===========================================
    -- INTEGERS
    -- ===========================================
    42::UInt8 as uint8_val,
    1234::UInt16 as uint16_val,
    123456::UInt32 as uint32_val,
    9223372036854775807::UInt64 as uint64_val,
    170141183460469231731687303715884105727::UInt128 as uint128_val,
    toUInt256(12345678901234567890) as uint256_val,
    toInt8(-42) as int8_val,
    toInt16(-1234) as int16_val,
    toInt32(-123456) as int32_val,
    toInt64(-9223372036854775807) as int64_val,
    toInt128('-123456789012345678901234567890') as int128_val,
    toInt256('-12345678901234567890123456789012345678901234567890') as int256_val,

    -- ===========================================
    -- FLOATS
    -- ===========================================
    3.14::Float32 as float32_val,
    3.141592653589793::Float64 as float64_val,
    1.5::BFloat16 as bfloat16_val,
    inf::Float32 as float32_inf,
    nan::Float64 as float64_nan,

    -- ===========================================
    -- STRINGS
    -- ===========================================
    'hello world'::String as string_val,
    'Привет мир 你好 🎉'::String as string_unicode,
    'abc'::FixedString(5) as fixedstring_val,

    -- ===========================================
    -- BOOLEAN
    -- ===========================================
    true::Bool as bool_true,
    false::Bool as bool_false,

    -- ===========================================
    -- DATE AND TIME
    -- ===========================================
    '2024-01-15'::Date as date_val,
    '1960-06-15'::Date32 as date32_val,
    '2024-01-15 12:30:45'::DateTime as datetime_val,
    '2024-01-15 12:30:45'::DateTime('UTC') as datetime_tz,
    '2024-01-15 12:30:45.123456'::DateTime64(6) as datetime64_val,
    '2024-01-15 12:30:45.123'::DateTime64(3, 'America/New_York') as datetime64_tz,
    '12:30:45'::Time as time_val,
    '12:30:45.123456'::Time64(6) as time64_val,

    -- ===========================================
    -- SPECIAL TYPES
    -- ===========================================
    '550e8400-e29b-41d4-a716-446655440000'::UUID as uuid_val,
    '192.168.1.1'::IPv4 as ipv4_val,
    '2001:db8::1'::IPv6 as ipv6_val,

    -- ===========================================
    -- DECIMALS
    -- ===========================================
    123.45::Decimal32(2) as decimal32_val,
    12345.6789::Decimal64(4) as decimal64_val,
    123456789.123456789::Decimal128(9) as decimal128_val,
    0::Decimal256(20) as decimal256_val,

    -- ===========================================
    -- ENUMS
    -- ===========================================
    'hello'::Enum8('hello' = 1, 'world' = 2) as enum8_val,
    'world'::Enum16('hello' = 1, 'world' = 1000) as enum16_val,

    -- ===========================================
    -- NULLABLE
    -- ===========================================
    42::Nullable(UInt32) as nullable_int,
    NULL::Nullable(String) as nullable_null,
    'test'::Nullable(String) as nullable_string,

    -- ===========================================
    -- ARRAYS
    -- ===========================================
    []::Array(UInt8) as array_empty,
    [1, 2, 3]::Array(UInt32) as array_int,
    ['a', 'b', 'c']::Array(String) as array_string,
    [[1, 2], [3, 4, 5]]::Array(Array(UInt32)) as array_nested,
    [[[1, 2], [3]], [[4, 5, 6]]]::Array(Array(Array(UInt8))) as array_3d,
    [1, NULL, 3]::Array(Nullable(UInt32)) as array_nullable,

    -- ===========================================
    -- TUPLES
    -- ===========================================
    (42, 'hello')::Tuple(UInt32, String) as tuple_simple,
    CAST((1, 'test', 3.14), 'Tuple(id UInt32, name String, value Float64)') as tuple_named,
    ((1, 2), ('a', 'b'))::Tuple(Tuple(UInt8, UInt8), Tuple(String, String)) as tuple_nested,
    (1, [2, 3], ('inner', 4.5))::Tuple(UInt8, Array(UInt8), Tuple(String, Float32)) as tuple_complex,

    -- ===========================================
    -- MAPS
    -- ===========================================
    map()::Map(String, UInt32) as map_empty,
    map('a', 1, 'b', 2, 'c', 3)::Map(String, UInt32) as map_string_int,
    map(1, 'one', 2, 'two')::Map(UInt32, String) as map_int_string,
    map('nested', map('inner', 42))::Map(String, Map(String, UInt32)) as map_nested,

    -- ===========================================
    -- LOWCARDINALITY
    -- ===========================================
    'hello'::LowCardinality(String) as lowcard_string,
    NULL::LowCardinality(Nullable(String)) as lowcard_nullable,

    -- ===========================================
    -- VARIANT
    -- ===========================================
    'hello'::Variant(String, UInt64) as variant_string,
    42::Variant(String, UInt64) as variant_int,
    NULL::Variant(String, UInt64) as variant_null,

    -- ===========================================
    -- DYNAMIC
    -- ===========================================
    42::Dynamic as dynamic_int,
    'hello'::Dynamic as dynamic_string,
    NULL::Dynamic as dynamic_null,
    toDateTime64('2024-01-15 12:30:00', 3)::Dynamic as dynamic_datetime,

    -- ===========================================
    -- JSON
    -- ===========================================
    '{"name": "test", "value": 42}'::JSON as json_simple,
    '{"id": 1, "nested": {"x": 10, "y": 20}}'::JSON as json_nested,

    -- ===========================================
    -- GEO TYPES
    -- ===========================================
    (1.5, 2.5)::Point as point_val,
    [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]::Ring as ring_val,
    [[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]]::Polygon as polygon_val,
    [[[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)]]]::MultiPolygon as multipolygon_val,
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]::LineString as linestring_val,
    [[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]::MultiLineString as multilinestring_val,
    ((1.0, 2.0)::Point)::Geometry as geometry_point,
    ([(0.0, 0.0), (1.0, 1.0)]::LineString)::Geometry as geometry_linestring,

    -- ===========================================
    -- QBIT (Quantized Bit Vector)
    -- ===========================================
    [1.0, 2.0, 3.0]::QBit(Float32, 3) as qbit_float32,

    -- ===========================================
    -- COMPLEX NESTED COMBINATIONS
    -- ===========================================
    -- Array of Tuples
    [(1, 'a'), (2, 'b'), (3, 'c')]::Array(Tuple(UInt8, String)) as array_of_tuples,

    -- Array of Maps
    [map('x', 1), map('y', 2, 'z', 3)]::Array(Map(String, UInt32)) as array_of_maps,

    -- Map of Arrays
    map('nums', [1, 2, 3], 'more', [4, 5])::Map(String, Array(UInt32)) as map_of_arrays,

    -- Tuple with Array and Map
    (
        42::UInt32,
        [1, 2, 3]::Array(UInt8),
        map('key', 'value')::Map(String, String)
    )::Tuple(id UInt32, numbers Array(UInt8), metadata Map(String, String)) as tuple_with_collections,

    -- Deeply nested Array (4D)
    [[[[1, 2], [3, 4]], [[5, 6]]]]::Array(Array(Array(Array(UInt8)))) as array_4d,

    -- Map with Tuple values
    map('point1', (1.0, 2.0), 'point2', (3.0, 4.0))::Map(String, Tuple(Float64, Float64)) as map_of_tuples,

    -- Tuple containing Nullable Array
    (42, [1, NULL, 3]::Array(Nullable(UInt32)))::Tuple(UInt32, Array(Nullable(UInt32))) as tuple_nullable_array,

    -- Map of Maps
    map('level1', map('level2a', 1, 'level2b', 2))::Map(String, Map(String, UInt32)) as map_of_maps,

    -- Complex real-world-like structure (user record)
    (
        'user_123'::String,
        ['admin', 'editor', 'viewer']::Array(String),
        map(
            'profile', (30, 'John Doe', true)::Tuple(age UInt8, name String, active Bool),
            'settings', (25, 'Jane Smith', false)::Tuple(age UInt8, name String, active Bool)
        )::Map(String, Tuple(age UInt8, name String, active Bool)),
        [(1.0, 2.0), (3.0, 4.0)]::Array(Point)
    )::Tuple(
        user_id String,
        roles Array(String),
        profiles Map(String, Tuple(age UInt8, name String, active Bool)),
        locations Array(Point)
    ) as complex_user_record,

    -- Tuple with Variant
    (42, 'hello'::Variant(String, UInt64))::Tuple(UInt32, Variant(String, UInt64)) as tuple_with_variant,

    -- Array of Geo Points with labels
    [((0.0, 0.0), 'origin'), ((1.0, 1.0), 'point_a')]::Array(Tuple(Point, String)) as labeled_points,

    -- Nested Maps with Arrays
    map(
        'group1', map('items', [1, 2, 3]::Array(UInt32)),
        'group2', map('items', [4, 5, 6]::Array(UInt32))
    )::Map(String, Map(String, Array(UInt32))) as deeply_nested_map

SETTINGS
    allow_experimental_variant_type = 1,
    allow_experimental_dynamic_type = 1,
    allow_experimental_json_type = 1,
    allow_suspicious_variant_types = 1,
    allow_experimental_qbit_type = 1,
    allow_suspicious_low_cardinality_types = 1
