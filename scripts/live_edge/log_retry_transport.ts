/** CANARY ONLY: real SDK/SigV4 transport with one post-commit ACK fault.
 * The normal function import map must never point to this module.
 */
import { Client as RealClient } from 'npm:@opensearch-project/opensearch';
import sdkPackage from 'npm:@opensearch-project/opensearch/package.json' with { type: 'json' };

type Attempt = {
  backend_status: number | null;
  delivered_status: number | null;
  request_id: string | null;
  response_id: string | null;
  faulted: boolean;
};
type CapturedResult = {
  status: number | null;
  request_id: string | null;
  response_id: string | null;
};
type Receipt = {
  resolved_sdk_version: string;
  backend_attempt_count: number;
  attempts: Attempt[];
  invocation_count: number;
  results: CapturedResult[];
  verified: boolean;
};
export type Capture = {
  origin: string;
  index: string;
  document: Record<string, unknown>;
  expectedCalls: number;
  attempts: Attempt[];
  results: CapturedResult[];
  pending: Promise<unknown>[];
  clients: any[];
  faulted: boolean;
  refused: boolean;
  completion?: Promise<Receipt>;
};
let current: Capture | undefined;
export class CanaryTransportRefusal extends Error {
  constructor() {
    super('canary_transport_scope_refused');
  }
}
function uuid(value: unknown): string | null {
  return typeof value === 'string' && /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(value)
    ? value
    : null;
}
function safeStatus(value: unknown): number | null {
  return Number.isInteger(value) && Number(value) >= 100 && Number(value) <= 599
    ? Number(value)
    : null;
}
export function beginCapture(
  origin: string,
  index: string,
  document: Record<string, unknown>,
  expectedCalls: number,
): Capture {
  if (current || ![1, 2].includes(expectedCalls)) throw new CanaryTransportRefusal();
  const capture: Capture = {
    origin,
    index,
    document,
    expectedCalls,
    attempts: [],
    results: [],
    pending: [],
    clients: [],
    faulted: false,
    refused: false,
  };
  current = capture;
  return capture;
}
/** Subclass the caller's signed Connection; do not replace SigV4 with a plain connection. */
export function retryConnection(Base: any, capture: Capture): any {
  return class AckFaultConnection extends Base {
    request(params: any, callback: any) {
      let requestId: string | null = null;
      try {
        const origin = this.url.origin;
        const path = String(params.path ?? '');
        const prefix = '/' + encodeURIComponent(capture.index) + '/_doc';
        if (
          current !== capture ||
          origin !== capture.origin ||
          this.url.pathname !== '/' ||
          this.url.search ||
          this.url.hash ||
          this.url.username ||
          this.url.password ||
          !['POST', 'PUT'].includes(params.method) ||
          params.querystring ||
          capture.attempts.length >= capture.expectedCalls + 1 ||
          !(path === prefix || path.startsWith(prefix + '/')) ||
          typeof params.body !== 'string' ||
          params.body.length > 2048
        ) {
          throw new CanaryTransportRefusal();
        }
        if (path !== prefix) {
          requestId = uuid(decodeURIComponent(path.slice(prefix.length + 1)));
          if (!requestId) throw new CanaryTransportRefusal();
        }
        const body = JSON.parse(params.body);
        const expectedKeys = Object.keys(capture.document).sort();
        if (
          !body ||
          JSON.stringify(Object.keys(body).sort()) !== JSON.stringify(expectedKeys) ||
          !expectedKeys.every((key) => body[key] === capture.document[key])
        )
          throw new CanaryTransportRefusal();
      } catch (_) {
        capture.refused = true;
        callback(new CanaryTransportRefusal(), null);
        return { abort() {} };
      }
      const attempt: Attempt = {
        backend_status: null,
        delivered_status: null,
        request_id: requestId,
        response_id: null,
        faulted: false,
      };
      capture.attempts.push(attempt);
      return super.request(params, (error: any, response: any) => {
        if (error || !response) {
          callback(error, response);
          return;
        }
        attempt.backend_status = safeStatus(response.statusCode);
        attempt.delivered_status = attempt.backend_status;
        // Observe actual backend result IDs without consuming/replacing its
        // stream. No body, error reason, headers or credentials enter receipts.
        let text = '';
        response.on('data', (chunk: any) => {
          if (text.length <= 8192)
            text += typeof chunk === 'string' ? chunk : new TextDecoder().decode(chunk);
        });
        response.on('end', () => {
          try {
            attempt.response_id = uuid(JSON.parse(text)._id);
          } catch (_) {
            /* No raw diagnostics. */
          }
        });
        if (!capture.faulted && [200, 201].includes(attempt.backend_status ?? 0)) {
          capture.faulted = true;
          attempt.faulted = true;
          attempt.delivered_status = 503;
          response.statusCode = 503;
        }
        callback(error, response);
      });
    }
  };
}
/** The actual logger constructs this Client through a canary-only import alias. */
export class Client extends RealClient {
  constructor(options: any) {
    const capture = current;
    const origin = new URL(String(options.node));
    if (
      !capture ||
      typeof options.Connection !== 'function' ||
      origin.origin !== capture.origin ||
      origin.pathname !== '/' ||
      origin.search ||
      origin.hash ||
      origin.username ||
      origin.password
    )
      throw new CanaryTransportRefusal();
    super({
      ...options,
      Connection: retryConnection(options.Connection, capture),
      maxRetries: 1,
      requestTimeout: 15000,
      sniffOnStart: false,
      sniffOnConnectionFault: false,
      sniffInterval: false,
    });
    capture.clients.push(this);
    const original = this.index.bind(this);
    (this as any).index = (parameters: any) => {
      if (
        current !== capture ||
        capture.pending.length >= capture.expectedCalls ||
        parameters.index !== capture.index
      )
        throw new CanaryTransportRefusal();
      const pending = Promise.resolve(original(parameters)).then(
        (result: any) => {
          capture.results.push({
            status: safeStatus(result.statusCode),
            request_id: uuid(parameters.id),
            response_id: uuid(result.body?._id),
          });
          return result;
        },
        (error: any) => {
          const status = safeStatus(error?.meta?.statusCode ?? error?.statusCode);
          // The actual logger's catch remains untouched, but receives no raw SDK
          // object containing headers, body, target URLs or signature material.
          throw new Error(`canary_backend_write_failed_${status ?? 'transport'}`);
        },
      );
      capture.pending.push(pending);
      return pending;
    };
  }
}
export function finishCapture(capture: Capture): Promise<Receipt> {
  capture.completion ??= drainCapture(capture);
  return capture.completion;
}
async function drainCapture(capture: Capture): Promise<Receipt> {
  try {
    const outcomes = await Promise.allSettled(capture.pending);
    const succeeded = outcomes.every((result) => result.status === 'fulfilled');
    const expectedAttempts = capture.expectedCalls + 1;
    const ids = capture.results.map((result) => result.request_id);
    const stable =
      capture.results.every(
        (result) => result.request_id && result.request_id === result.response_id,
      ) &&
      capture.attempts.every(
        (attempt) => attempt.request_id && attempt.request_id === attempt.response_id,
      );
    const version =
      typeof sdkPackage.version === 'string' &&
      /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/.test(sdkPackage.version)
        ? sdkPackage.version
        : 'unavailable';
    return {
      resolved_sdk_version: version,
      backend_attempt_count: capture.attempts.length,
      attempts: capture.attempts,
      invocation_count: capture.pending.length,
      results: capture.results,
      verified:
        !capture.refused &&
        succeeded &&
        capture.faulted &&
        stable &&
        capture.pending.length === capture.expectedCalls &&
        capture.attempts.length === expectedAttempts &&
        new Set(ids).size === capture.expectedCalls &&
        capture.attempts.filter((attempt) => attempt.faulted).length === 1,
    };
  } finally {
    // Drain writes before releasing the request lease or closing signed clients.
    await Promise.allSettled(capture.clients.map((client) => client.close()));
    if (current === capture) current = undefined;
  }
}
