// Type declarations for the JS proxy harness so the TypeScript CLI can import
// it. Runtime is scripts/native-proxy.mjs (Node built-ins only).
import type { Buffer } from 'node:buffer';

export const MAGIC: string;
export const DIR_C2S: 0;
export const DIR_S2C: 1;

export interface Segment {
  dir: 0 | 1;
  data: Buffer;
}

export interface Capture {
  c2s: Buffer;
  s2c: Buffer;
  segments: Segment[];
  meta: Record<string, unknown>;
}

export interface CaptureQueryOptions {
  query: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  clientPath?: string;
  clientArgs?: string[];
  settings?: Record<string, string>;
}

export function startProxy(opts: {
  targetHost: string;
  targetPort: number;
  listenHost?: string;
}): Promise<{ port: number; done: Promise<Segment[]>; close: () => void }>;

export interface StartCaptureProxyOptions {
  targetHost: string;
  targetPort: number;
  listenHost?: string;
  listenPort?: number;
  once?: boolean;
  onCapture?: (capture: Capture) => void;
  onError?: (err: Error) => void;
}

export function startCaptureProxy(opts: StartCaptureProxyOptions): Promise<{
  host: string;
  port: number;
  done: Promise<void>;
  close: () => void;
}>;

export function splitStreams(segments: Segment[]): { c2s: Buffer; s2c: Buffer };
export function captureQuery(opts: CaptureQueryOptions): Promise<Capture>;
export function encodeDump(capture: Capture): Buffer;
export function decodeDump(buf: Buffer): Capture;
