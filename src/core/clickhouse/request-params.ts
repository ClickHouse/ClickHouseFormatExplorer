import { ClickHouseFormat } from '../types/formats';
import { DEFAULT_NATIVE_PROTOCOL_VERSION } from '../types/native-protocol';

export function appendClickHouseRequestParams(
  params: URLSearchParams,
  format: string,
  nativeProtocolVersion: number = DEFAULT_NATIVE_PROTOCOL_VERSION,
): void {
  params.set('default_format', format);

  if (format === ClickHouseFormat.Native && nativeProtocolVersion !== DEFAULT_NATIVE_PROTOCOL_VERSION) {
    params.set('client_protocol_version', String(nativeProtocolVersion));
  }
}
