import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Transport from 'winston-transport';
import { httpLogger } from '@spalx/common-loggers-pkg';
import { CorrelatedMessage } from '@spalx/transport-pkg';
import { HTTPTransportAdapter } from '../dist/index.js';

const ACTION = 'reports.getCharges';
const CORRELATION_ID = 'correlation-1';
const MARKER = 'response-body-marker';

class CapturingTransport extends Transport {
  lines: string[] = [];

  log(info: { message: unknown }, next: () => void): void {
    this.lines.push(String(info.message));
    next();
  }
}

describe('HTTPTransportAdapter response logging', () => {
  const capture = new CapturingTransport();
  let server: Server;
  let port: number;
  let reply = '';

  before(async () => {
    // The http logger writes to logs/ under the working directory.
    process.chdir(mkdtempSync(join(tmpdir(), 'http-transport-adapter-')));
    httpLogger.add(capture);

    server = createServer((req, res) => {
      req.resume();
      req.on('end', () => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(reply);
      });
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    port = (server.address() as AddressInfo).port;
  });

  after(async () => {
    await new Promise<void>(resolve => server.close(() => resolve()));
    httpLogger.close();
  });

  async function sendAndGetResponseLine(): Promise<string> {
    capture.lines = [];
    const adapter = new HTTPTransportAdapter();
    const req = CorrelatedMessage.create(CORRELATION_ID, ACTION, 'http', { query: 'all' });

    await adapter.send(req, { host: '127.0.0.1', port });

    const deadline = Date.now() + 1000;
    let line: string | undefined;
    while (!(line = capture.lines.find(l => l.startsWith('Response received'))) && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.ok(line, 'no response line was logged');

    return line;
  }

  function expectedLine(): string {
    return `Response received: POST http://127.0.0.1:${port}/http-transport ${ACTION} ${CORRELATION_ID} (${Buffer.byteLength(reply)} bytes)`;
  }

  it('logs a reply by its method, URL, action, correlation id and size, not its body', async () => {
    reply = JSON.stringify(CorrelatedMessage.create(CORRELATION_ID, ACTION, 'http', { charges: [MARKER] }));

    const line = await sendAndGetResponseLine();

    assert.equal(line, expectedLine());
    assert.ok(capture.lines.every(l => !l.includes(MARKER)));
  });

  it('logs an error reply the same way', async () => {
    reply = JSON.stringify(CorrelatedMessage.create(CORRELATION_ID, ACTION, 'http', undefined, new Error(MARKER)));

    const line = await sendAndGetResponseLine();

    assert.equal(line, expectedLine());
    assert.ok(capture.lines.every(l => !l.includes(MARKER)));
  });
});
