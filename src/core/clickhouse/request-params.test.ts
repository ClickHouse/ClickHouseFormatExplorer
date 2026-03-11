import { describe, expect, it } from 'vitest';
import { appendClickHouseRequestParams } from './request-params';
import { ClickHouseFormat } from '../types/formats';

describe('appendClickHouseRequestParams', () => {
  it('always sets default_format', () => {
    const params = new URLSearchParams();

    appendClickHouseRequestParams(params, ClickHouseFormat.RowBinaryWithNamesAndTypes);

    expect(params.get('default_format')).toBe(ClickHouseFormat.RowBinaryWithNamesAndTypes);
  });

  it('omits client_protocol_version for legacy Native', () => {
    const params = new URLSearchParams();

    appendClickHouseRequestParams(params, ClickHouseFormat.Native, 0);

    expect(params.get('default_format')).toBe(ClickHouseFormat.Native);
    expect(params.has('client_protocol_version')).toBe(false);
  });

  it('adds client_protocol_version for explicit Native presets', () => {
    const params = new URLSearchParams();

    appendClickHouseRequestParams(params, ClickHouseFormat.Native, 54483);

    expect(params.get('default_format')).toBe(ClickHouseFormat.Native);
    expect(params.get('client_protocol_version')).toBe('54483');
  });
});
