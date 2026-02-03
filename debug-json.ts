import { NativeDecoder } from './src/core/decoder/native-decoder';

// Enable debug mode
(globalThis as any).DEBUG_NATIVE_DECODER = true;

// Hex dump helper
function hexDump(data: Uint8Array, bytesPerLine = 16): string {
  let result = '';
  for (let i = 0; i < data.length; i += bytesPerLine) {
    const hex = Array.from(data.slice(i, i + bytesPerLine))
      .map(b => b.toString(16).padStart(2, '0'))
      .join(' ');
    const offset = i.toString(16).padStart(4, '0');
    result += offset + ': ' + hex + '\n';
  }
  return result;
}

async function main() {
  // Query ClickHouse for JSON with exceeded max_dynamic_paths
  const response = await fetch('http://localhost:8123/?default_format=Native&allow_experimental_json_type=1', {
    method: 'POST',
    body: "SELECT '{\"a\": 1, \"b\": 2, \"c\": 3}'::JSON(max_dynamic_paths=2) AS col"
  });
  const data = new Uint8Array(await response.arrayBuffer());

  console.log('Data length:', data.length);
  console.log('Hex dump:');
  console.log(hexDump(data));

  // Manual trace of the bytes for path "a" Dynamic
  console.log('\n--- Manual trace ---');
  console.log('JSON version (0x20-0x27):', Array.from(data.slice(0x20, 0x28)).map(b => b.toString(16).padStart(2, '0')).join(' '));
  console.log('max_dynamic_paths (0x28):', data[0x28]);
  console.log('num_dynamic_paths (0x29):', data[0x29]);
  console.log('path "a" (0x2a-0x2b):', String.fromCharCode(data[0x2b]));
  console.log('path "b" (0x2c-0x2d):', String.fromCharCode(data[0x2d]));
  console.log('Dynamic "a" version (0x2e-0x35):', Array.from(data.slice(0x2e, 0x36)).map(b => b.toString(16).padStart(2, '0')).join(' '));
  console.log('Dynamic "a" max (0x36):', data[0x36]);
  console.log('Dynamic "a" num (0x37):', data[0x37]);
  console.log('Dynamic "a" type name len (0x38):', data[0x38]);
  console.log('Dynamic "a" type name (0x39-0x3d):', new TextDecoder().decode(data.slice(0x39, 0x3e)));
  console.log('Dynamic "a" mode (0x3e-0x45):', Array.from(data.slice(0x3e, 0x46)).map(b => b.toString(16).padStart(2, '0')).join(' '));
  console.log('');
  console.log('At offset 0x46 (should be discriminator or data):');
  console.log('  0x46-0x4d as bytes:', Array.from(data.slice(0x46, 0x4e)).map(b => b.toString(16).padStart(2, '0')).join(' '));
  console.log('  0x46-0x4d as Int64 LE:', new DataView(data.buffer, data.byteOffset + 0x46, 8).getBigInt64(0, true).toString());
  console.log('');
  console.log('At offset 0x4e onwards:');
  console.log('  0x4e-0x5d:', Array.from(data.slice(0x4e, 0x5e)).map(b => b.toString(16).padStart(2, '0')).join(' '));

  try {
    const decoder = new NativeDecoder(data);
    const result = decoder.decode();
    console.log('\nDecoded successfully!');
    console.log(JSON.stringify(result, (k, v) => typeof v === 'bigint' ? v.toString() : v, 2));
  } catch (e) {
    console.error('Decode error:', e);
  }
}

main().catch(console.error);
