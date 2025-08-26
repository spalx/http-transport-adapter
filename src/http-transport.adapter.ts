import http, { createServer, IncomingMessage, ServerResponse, Server } from 'http';
import { v4 as uuidv4 } from 'uuid';
import { ZodError } from 'zod';
import { CorrelatedRequestDTO, CorrelatedResponseDTO, TransportAdapter, transportService } from 'transport-pkg';
import { AppRunPriority } from 'app-life-cycle-pkg';
import { BaseError, BadRequestError, InternalServerError } from 'rest-pkg';
import { httpLogger } from 'common-loggers-pkg';

const HTTP_TRANSPORT_ENDPOINT = '/http-transport';

class HTTPTransportAdapter implements TransportAdapter {
  private server: Server | null = null;
  private pendingResponses: Map<string, (message: CorrelatedResponseDTO) => void> = new Map();
  private port: number;

  constructor(port: number = 0) {
    this.port = port;
  }

  async init(): Promise<void> {
    const actionsToConsume: Record<string, (data: CorrelatedRequestDTO) => Promise<void>> = transportService.getReceivableActions();
    if (!Object.keys(actionsToConsume).length) {
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

        const data: CorrelatedRequestDTO | null = body ? JSON.parse(body) : null;
        await this.handleHTTPRequest(data, res);
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

  async send(data: CorrelatedRequestDTO, options: Record<string, unknown>, timeout?: number): Promise<CorrelatedResponseDTO> {
    if (!data.request_id) {
      data.request_id = uuidv4();
    }

    const actionsToProduce: string[] = transportService.getSendableActions();
    if (!actionsToProduce.includes(data.action)) {
      throw new BadRequestError(`Invalid action provided: ${data.action}`);
    }

    if (!options['host'] || !options['port']) {
      throw new BadRequestError('Host and/or port missing in transport options');
    }

    return await this.sendHTTPRequest(data, options, timeout);
  }

  async sendResponse(data: CorrelatedResponseDTO): Promise<void> {
    if (data.request_id && this.pendingResponses.has(data.request_id)) {
      const resolve = this.pendingResponses.get(data.request_id);
      if (resolve) {
        httpLogger.info(`Response sent: ${JSON.stringify(data)}`);
        resolve(data);
        this.pendingResponses.delete(data.request_id);
      }
    }
  }

  private getRequestUrl(options: Record<string, unknown>) {
    return `http://${options['host']}:${options['port']}${HTTP_TRANSPORT_ENDPOINT}`;
  }

  private sendHTTPRequest(data: CorrelatedRequestDTO, options: Record<string, unknown>, timeout?: number): Promise<CorrelatedResponseDTO> {
    return new Promise((resolve, reject) => {
      const jsonData: string = JSON.stringify(data);

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

      const req = http.request(requestOptions, (res) => {
        let responseData = '';

        res.on('data', (chunk) => {
          responseData += chunk;
        });

        res.on('end', () => {
          httpLogger.info(`Response received: ${responseData}`);
          resolve(JSON.parse(responseData));
        });
      });

      req.on('error', (err) => reject(err));

      if (timeout) {
        req.setTimeout(timeout, () => {
          req.destroy(new InternalServerError(`Request timed out after ${timeout}ms`));
        });
      }

      req.write(jsonData);
      req.end();
    });
  }

  private async handleHTTPRequest(data: CorrelatedRequestDTO | null, res: ServerResponse) {
    const actionsToConsume: Record<string, (data: CorrelatedRequestDTO) => Promise<void>> = transportService.getReceivableActions();

    try {
      if (!data || !(data.action in actionsToConsume)) {
        throw new BadRequestError(`Invalid action provided: ${data?.action}`);
      }

      const handler = actionsToConsume[data.action];
      if (!handler) {
        throw new InternalServerError(`Handler not defined for action: ${data.action}`);
      }

      await this.executeActionHandler(handler, data, res);
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

  private async executeActionHandler(handler: (data: CorrelatedRequestDTO) => Promise<void>, data: CorrelatedRequestDTO, res: ServerResponse) {
    let status = 200;
    let errorMessage: string = '';
    let response: CorrelatedResponseDTO | null = null;

    try {
      response = await new Promise<CorrelatedResponseDTO>(async (resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new InternalServerError(`Timeout waiting for HTTP response on ${data.action}`));
        }, 30000);

        if (data.request_id) {
          this.pendingResponses.set(data.request_id, (msg) => {
            clearTimeout(timer);
            resolve(msg);
          });
        }

        try {
          await handler(data);
        } catch (err) {
          clearTimeout(timer);
          if (data.request_id) {
            this.pendingResponses.delete(data.request_id);
          }
          reject(err);
        }
      });
    } catch (error) {
      if (error instanceof ZodError) {
        status = 400;
        errorMessage = error.errors.map(e => e.message).join(', ');
      } else if (error instanceof BaseError) {
        status = error.code;
        errorMessage = error.message;
      } else if (error instanceof Error) {
        errorMessage = error.message;
      } else {
        status = 500;
        errorMessage = 'Internal Server Error';
      }
    } finally {
      res.writeHead(200, { 'Content-Type': 'application/json' });

      if (!response) {
        response = {
          action: data.action,
          correlation_id: data.correlation_id,
          data: {},
          error: errorMessage,
          status
        };
      }
      res.end(JSON.stringify(response));
    }
  }
}

export default HTTPTransportAdapter;
