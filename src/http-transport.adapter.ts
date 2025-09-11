import http, { createServer, IncomingMessage, ServerResponse, Server } from 'http';
import { CorrelatedMessage, TransportAdapter, transportService } from 'transport-pkg';
import { IAppPkg, AppRunPriority } from 'app-life-cycle-pkg';
import { BaseError, BadRequestError, InternalServerError } from 'rest-pkg';
import { httpLogger } from 'common-loggers-pkg';

const HTTP_TRANSPORT_ENDPOINT = '/http-transport';

class HTTPTransportAdapter extends TransportAdapter implements IAppPkg {
  private server: Server | null = null;
  private port: number;

  constructor(port: number = 0) {
    super();

    this.port = port;
  }

  async init(): Promise<void> {
    const actionHandlers: Record<string, (req: CorrelatedMessage) => Promise<object>> = transportService.getActionHandlers();
    if (!Object.keys(actionHandlers).length) {
      return;
    }

    if (!this.port) {
      throw new InternalServerError('HTTP transport needs a port in order to consume (receive) actions');
    }

    this.server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
      if (req.method !== 'POST' || req.url !== HTTP_TRANSPORT_ENDPOINT) {
        // Let other handlers deal with it
        return;
      }

      let body = '';
      req.on('data', chunk => { body += chunk; });
      req.on('end', async () => {
        httpLogger.info(`Request received: ${body}`);

        const req: CorrelatedMessage | null = body ? JSON.parse(body) : null;
        await this.handleHTTPRequest(req, res);
      });
    });

    await new Promise<void>((resolve, reject) => {
      this.server?.listen(this.port, resolve);
      this.server?.on('error', reject);
    });
  }

  async shutdown(): Promise<void> {
    if (this.server) {
      await new Promise<void>((resolve, reject) => {
        this.server?.close(err => err ? reject(err) : resolve());
      });
      this.server = null;
    }
  }

  getPriority(): number {
    return AppRunPriority.Lowest;
  }

  async send(req: CorrelatedMessage, options: Record<string, unknown>, timeout?: number): Promise<CorrelatedMessage> {
    if (!options['host'] || !options['port']) {
      throw new BadRequestError('Host and/or port missing in transport options');
    }

    return await this.sendHTTPRequest(req, options, timeout);
  }

  private getRequestUrl(options: Record<string, unknown>) {
    return `http://${options['host']}:${options['port']}${HTTP_TRANSPORT_ENDPOINT}`;
  }

  private sendHTTPRequest(req: CorrelatedMessage, options: Record<string, unknown>, timeout?: number): Promise<CorrelatedMessage> {
    return new Promise((resolve, reject) => {
      const jsonData: string = JSON.stringify(req);

      const requestOptions: http.RequestOptions = {
        hostname: options['host'] as string,
        port: options['port'] as number,
        path: HTTP_TRANSPORT_ENDPOINT,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(jsonData)
        },
      };

      httpLogger.info(`Request sent: POST ${this.getRequestUrl(options)} ${jsonData}`);

      const httpReq = http.request(requestOptions, (res) => {
        let responseData = '';

        res.on('data', (chunk) => {
          responseData += chunk;
        });

        res.on('end', () => {
          httpLogger.info(`Response received: ${responseData}`);
          resolve(JSON.parse(responseData));
        });
      });

      httpReq.on('error', (err) => reject(err));

      if (timeout) {
        httpReq.setTimeout(timeout, () => {
          httpReq.destroy(new InternalServerError(`Request timed out after ${timeout}ms`));
        });
      }

      httpReq.write(jsonData);
      httpReq.end();
    });
  }

  private async handleHTTPRequest(req: CorrelatedMessage | null, res: ServerResponse): Promise<void> {
    const actionHandlers: Record<string, (req: CorrelatedMessage) => Promise<object>> = transportService.getActionHandlers();

    try {
      if (!req || !actionHandlers[req.action]) {
        throw new BadRequestError(`Invalid action provided: ${req?.action}`);
      }

      const handler: (req: CorrelatedMessage) => Promise<object> = actionHandlers[req.action];
      if (!handler) {
        throw new InternalServerError(`Handler not defined for action: ${req.action}`);
      }

      await this.executeActionHandler(handler, req, res);
    } catch (error) {
      let status = 500;
      let errorMessage = 'Internal Server Error';

      if (error instanceof BaseError) {
        status = error.code;
        errorMessage = error.message;
      }

      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: errorMessage }));
    }
  }

  private async executeActionHandler(handler: (req: CorrelatedMessage) => Promise<object>, req: CorrelatedMessage, res: ServerResponse): Promise<void> {
    let error: unknown | undefined;
    let responseData: object | undefined;

    try {
      responseData = await handler(req);
    } catch (err) {
      error = err;
    } finally {
      res.writeHead(200, { 'Content-Type': 'application/json' });

      const response: CorrelatedMessage = CorrelatedMessage.create(
        req.correlation_id,
        req.action,
        req.transport,
        responseData,
        error
      );

      res.end(JSON.stringify(response));
    }
  }
}

export default HTTPTransportAdapter;
