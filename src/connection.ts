/**
 * Connection management for the Kevo SDK
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { ConnectionError, TimeoutError } from './errors';

// Define a type for the promisified gRPC methods we'll create
type GrpcPromiseMethod<RequestType, ResponseType> = (
  request: RequestType
) => Promise<ResponseType>;

// Extended client interface with known methods
interface GrpcServiceClient {
  // Dynamic method access
  [method: string]: unknown;
  // Specific known methods
  waitForReady: (deadline: number, callback: (error: Error | null) => void) => void;
  getChannel: () => { getConnectivityState: (tryToConnect: boolean) => grpc.connectivityState };
  close: () => void;
  
  // Strongly typed methods from the proto
  // We use uppercase first letter to match the proto definition
  Get: (request: any, callback: (error: Error | null, response: any) => void) => void;
  Put: (request: any, callback: (error: Error | null, response: any) => void) => void;
  Delete: (request: any, callback: (error: Error | null, response: any) => void) => void;
  BatchWrite: (request: any, callback: (error: Error | null, response: any) => void) => void;
  Scan: (request: any) => grpc.ClientReadableStream<unknown>;
  BeginTransaction: (request: any, callback: (error: Error | null, response: any) => void) => void;
  CommitTransaction: (request: any, callback: (error: Error | null, response: any) => void) => void;
  RollbackTransaction: (request: any, callback: (error: Error | null, response: any) => void) => void;
  TxGet: (request: any, callback: (error: Error | null, response: any) => void) => void;
  TxPut: (request: any, callback: (error: Error | null, response: any) => void) => void;
  TxDelete: (request: any, callback: (error: Error | null, response: any) => void) => void;
  TxScan: (request: any) => grpc.ClientReadableStream<unknown>;
  GetStats: (request: any, callback: (error: Error | null, response: any) => void) => void;
  Compact: (request: any, callback: (error: Error | null, response: any) => void) => void;
}

export interface ConnectionOptions {
  host: string;
  port: number;
  useTls?: boolean;
  caCert?: Buffer;
  clientCert?: Buffer;
  clientKey?: Buffer;
  connectTimeout?: number;
  requestTimeout?: number;
  maxRetries?: number;
  retryDelay?: number;
}

export class Connection {
  private client: GrpcServiceClient | null = null;
  private options: ConnectionOptions;
  private connected = false;
  
  constructor(options: ConnectionOptions) {
    this.options = {
      useTls: false,
      connectTimeout: 5000,
      requestTimeout: 10000,
      maxRetries: 3,
      retryDelay: 1000,
      ...options
    };
  }

  /**
   * Connect to the Kevo database
   */
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      const packageDefinition = await protoLoader.load(
        path.resolve(__dirname, '../proto/kevo/service.proto'),
        {
          keepCase: true,
          longs: String,
          enums: String,
          defaults: true,
          oneofs: true
        }
      );

      const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
      
      // Cast to access the KevoService property
      const serviceProto = (protoDescriptor as Record<string, Record<string, unknown>>).kevo.KevoService as unknown as { new(address: string, credentials: grpc.ChannelCredentials, options: object): GrpcServiceClient };

      const address = `${this.options.host}:${this.options.port}`;
      let credentials: grpc.ChannelCredentials;

      if (this.options.useTls) {
        if (!this.options.caCert) {
          throw new ConnectionError('CA certificate is required for TLS connections');
        }

        const secureOptions = {
          rootCerts: this.options.caCert,
          privateKey: this.options.clientKey,
          certChain: this.options.clientCert
        };

        credentials = grpc.credentials.createSsl(
          secureOptions.rootCerts, 
          secureOptions.privateKey, 
          secureOptions.certChain
        );
      } else {
        credentials = grpc.credentials.createInsecure();
      }

      this.client = new serviceProto(address, credentials, {
        'grpc.max_receive_message_length': 20 * 1024 * 1024,  // 20MB
        'grpc.max_send_message_length': 20 * 1024 * 1024      // 20MB
      });

      // Wait for the channel to be ready
      const channelState = await new Promise<grpc.connectivityState>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new TimeoutError(`Connection timeout after ${this.options.connectTimeout}ms`));
        }, this.options.connectTimeout);

        if (!this.client) {
          clearTimeout(timeout);
          reject(new ConnectionError('Client not initialized'));
          return;
        }

        this.client.waitForReady(Date.now() + this.options.connectTimeout!, (error: Error | null) => {
          clearTimeout(timeout);
          if (error) {
            reject(new ConnectionError(`Failed to connect: ${error.message}`));
          } else {
            resolve(this.client!.getChannel().getConnectivityState(true));
          }
        });
      });

      if (channelState !== grpc.connectivityState.READY) {
        throw new ConnectionError(`Channel not ready, state: ${channelState}`);
      }

      this.connected = true;
    } catch (error) {
      if (error instanceof Error) {
        throw new ConnectionError(`Failed to connect: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Disconnect from the Kevo database
   */
  disconnect(): void {
    if (this.client) {
      this.client.close();
      this.connected = false;
      this.client = null;
    }
  }

  /**
   * Check if connected to the Kevo database
   */
  isConnected(): boolean {
    return this.connected && 
      this.client?.getChannel().getConnectivityState(false) === grpc.connectivityState.READY;
  }

  /**
   * Get the gRPC client
   */
  getClient(): GrpcServiceClient {
    if (!this.connected || !this.client) {
      throw new ConnectionError('Not connected to Kevo database');
    }
    return this.client;
  }

  /**
   * Execute an RPC method with retries and timeouts
   */
  async executeWithRetry<T>(method: string, request: Record<string, unknown>): Promise<T> {
    if (!this.isConnected()) {
      await this.connect();
    }

    let lastError: Error | null = null;
    const maxRetries = this.options.maxRetries!;
    const retryDelay = this.options.retryDelay!;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await this.execute<T>(method, request);
      } catch (error) {
        if (error instanceof Error) {
          lastError = error;
          
          // Only retry on specific errors that might be transient
          const code = (error as { code?: number }).code;
          const retriableErrors = [
            grpc.status.UNAVAILABLE,
            grpc.status.INTERNAL,
            grpc.status.RESOURCE_EXHAUSTED,
            grpc.status.DEADLINE_EXCEEDED
          ];
          
          if (!code || !retriableErrors.includes(code) || attempt >= maxRetries) {
            throw error;
          }
          
          // Wait before retrying
          await new Promise(resolve => setTimeout(resolve, retryDelay * Math.pow(2, attempt)));
        } else {
          throw error;
        }
      }
    }
    
    throw lastError || new ConnectionError('Max retries exceeded');
  }

  /**
   * Execute an RPC method
   */
  private execute<T>(method: string, request: Record<string, unknown>): Promise<T> {
    return new Promise((resolve, reject) => {
      try {
        const client = this.getClient();
        
        // Get the RPC method directly from the prototype
        const methodFn = (client as any)[method];
        
        if (typeof methodFn !== 'function') {
          const proto = Object.getPrototypeOf(client);
          const methods = Object.getOwnPropertyNames(proto)
            .filter(name => typeof proto[name] === 'function');
          
          reject(new Error(`Method ${method} not found. Available methods: ${methods.join(', ')}`));
          return;
        }
        
        // Call the method with callback
        methodFn.call(client, request, (error: Error | null, response: T) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Execute a streaming RPC method
   */
  executeStream(method: string, request: Record<string, unknown>): grpc.ClientReadableStream<unknown> {
    try {
      if (!this.isConnected()) {
        throw new ConnectionError('Not connected to Kevo database');
      }

      const client = this.getClient();
      
      const methodFn = (client as any)[method];
      if (typeof methodFn !== 'function') {
        const proto = Object.getPrototypeOf(client);
        const methods = Object.getOwnPropertyNames(proto)
          .filter(name => typeof proto[name] === 'function');
        
        throw new Error(`Streaming method ${method} not found. Available methods: ${methods.join(', ')}`);
      }
      
      return methodFn.call(client, request);
    } catch (error) {
      throw error;
    }
  }
}