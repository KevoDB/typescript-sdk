/**
 * Connection management for the Kevo SDK
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { ConnectionError, TimeoutError, ReadOnlyError } from './errors';

// Generic type for requests and responses
type GrpcRequest = Record<string, unknown>;
type GrpcResponse = Record<string, unknown>;

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
  Get: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  Put: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  Delete: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  BatchWrite: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  Scan: (request: GrpcRequest) => grpc.ClientReadableStream<unknown>;
  BeginTransaction: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  CommitTransaction: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  RollbackTransaction: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  TxGet: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  TxPut: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  TxDelete: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  TxScan: (request: GrpcRequest) => grpc.ClientReadableStream<unknown>;
  GetStats: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  Compact: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
  GetNodeInfo: (request: GrpcRequest, callback: (error: Error | null, response: GrpcResponse) => void) => void;
}

export type ReplicaSelectionStrategy = 'random' | 'sequential' | 'round_robin';

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
  autoRouteReads?: boolean;
  autoRouteWrites?: boolean;
  preferReplica?: boolean;
  replicaSelectionStrategy?: ReplicaSelectionStrategy;
}

interface ReplicaConnection {
  client: GrpcServiceClient;
  address: string;
  connected: boolean;
}

export class Connection {
  private client: GrpcServiceClient | null = null;
  private primaryClient: GrpcServiceClient | null = null;
  private replicaConnections: Map<string, ReplicaConnection> = new Map();
  private options: ConnectionOptions;
  private connected = false;
  private isPrimary = true;
  private currentReplicaIndex = 0;
  
  constructor(options: ConnectionOptions) {
    this.options = {
      useTls: false,
      connectTimeout: 5000,
      requestTimeout: 10000,
      maxRetries: 3,
      retryDelay: 1000,
      autoRouteReads: true,
      autoRouteWrites: true,
      preferReplica: true,
      replicaSelectionStrategy: 'round_robin',
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
      
      // Discover topology to find the primary and replicas
      await this.discoverTopology();
    } catch (error) {
      if (error instanceof Error) {
        throw new ConnectionError(`Failed to connect: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Discover the replication topology to find primary and replicas
   */
  private async discoverTopology(): Promise<void> {
    try {
      // Get node info to determine if we're connected to a primary or replica
      const response = await this.execute<{
        role?: string;
        primary?: { host?: string; port?: number; available?: boolean };
        replicas?: Array<{ host?: string; port?: number; available?: boolean }>
      }>('GetNodeInfo', {}, this.client!);
      
      // Determine if we're connected to the primary
      const role = response.role?.toLowerCase();
      this.isPrimary = role === 'primary';
      
      if (role === 'replica' && response.primary && this.options.autoRouteWrites) {
        // If we're connected to a replica and want to auto-route writes, connect to the primary
        const { host, port, available } = response.primary;
        
        if (host && port && available) {
          try {
            const primaryClient = await this.createClient(host, port.toString());
            this.primaryClient = primaryClient;
          } catch (error) {
            // If we can't connect to the primary, log it but continue
            console.warn(`Failed to connect to primary: ${error instanceof Error ? error.message : String(error)}`);
          }
        }
      }
      
      // If we're connected to the primary and want to auto-route reads, connect to replicas
      if (this.isPrimary && response.replicas && this.options.autoRouteReads) {
        for (const replica of response.replicas) {
          const { host, port, available } = replica;
          
          if (host && port && available) {
            try {
              await this.connectToReplica(host, port.toString());
            } catch (error) {
              // If we can't connect to a replica, log it but continue
              console.warn(`Failed to connect to replica: ${error instanceof Error ? error.message : String(error)}`);
            }
          }
        }
      }
    } catch (error) {
      // If topology discovery fails, just continue with the current connection
      console.warn(`Failed to discover topology: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disconnect from the Kevo database
   */
  disconnect(): void {
    // Close all replica connections
    for (const replicaConn of this.replicaConnections.values()) {
      replicaConn.client.close();
    }
    this.replicaConnections.clear();
    
    // Close primary client if it's different from the main client
    if (this.primaryClient && this.primaryClient !== this.client) {
      this.primaryClient.close();
      this.primaryClient = null;
    }
    
    // Close main client
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
   * Check if we should route to a replica for read operations
   */
  private shouldRouteToReplica(): boolean {
    // Don't route if auto-routing is disabled
    if (!this.options.autoRouteReads) {
      return false;
    }
    
    // Don't route if there are no replicas available
    if (this.replicaConnections.size === 0) {
      return false;
    }
    
    // If we're already connected to a replica, use it
    if (!this.isPrimary) {
      return true;
    }
    
    // Otherwise, respect the preferReplica option from client config
    return !!this.options.preferReplica;
  }
  
  /**
   * Check if we should route to the primary for write operations
   */
  private shouldRouteToPrimary(): boolean {
    // Don't route if auto-routing is disabled
    if (!this.options.autoRouteWrites) {
      return false;
    }
    
    // If we're already connected to the primary, use it
    if (this.isPrimary) {
      return false;
    }
    
    // Otherwise, route to primary for write operations
    return true;
  }
  
  /**
   * Get a client for read operations
   */
  getReadClient(): GrpcServiceClient {
    if (!this.connected) {
      throw new ConnectionError('Not connected to Kevo database');
    }
    
    if (this.shouldRouteToReplica()) {
      // Select a replica based on the configured strategy
      const client = this.selectReplicaClient();
      if (client) {
        return client;
      }
    }
    
    // Fall back to the current client
    return this.client!;
  }
  
  /**
   * Get a client for write operations
   */
  getWriteClient(): GrpcServiceClient {
    if (!this.connected) {
      throw new ConnectionError('Not connected to Kevo database');
    }
    
    if (this.shouldRouteToPrimary()) {
      if (this.primaryClient) {
        return this.primaryClient;
      }
    }
    
    // If we're already on the primary or no primary is known, use the current client
    return this.client!;
  }
  
  /**
   * Select a replica client based on the configured strategy
   */
  private selectReplicaClient(): GrpcServiceClient | null {
    if (this.replicaConnections.size === 0) {
      return null;
    }
    
    // Get all available replicas
    const availableReplicas = Array.from(this.replicaConnections.values())
      .filter(conn => conn.connected);
    
    if (availableReplicas.length === 0) {
      return null;
    }
    
    const strategy = this.options.replicaSelectionStrategy;
    let selectedReplica: ReplicaConnection;
    
    switch (strategy) {
      case 'random':
        // Select a random replica
        const randomIndex = Math.floor(Math.random() * availableReplicas.length);
        selectedReplica = availableReplicas[randomIndex];
        break;
        
      case 'sequential':
        // Always use the first replica (sorted by address for consistency)
        selectedReplica = availableReplicas.sort((a, b) => a.address.localeCompare(b.address))[0];
        break;
        
      case 'round_robin':
      default:
        // Rotate through replicas in order
        this.currentReplicaIndex = this.currentReplicaIndex % availableReplicas.length;
        selectedReplica = availableReplicas[this.currentReplicaIndex];
        this.currentReplicaIndex++;
        break;
    }
    
    return selectedReplica.client;
  }
  
  /**
   * Connect to a replica
   */
  private async connectToReplica(host: string, port: number | string): Promise<void> {
    const address = `${host}:${port}`;
    
    // Skip if already connected to this replica
    if (this.replicaConnections.has(address)) {
      return;
    }
    
    try {
      const client = await this.createClient(host, port);
      
      this.replicaConnections.set(address, {
        client,
        address,
        connected: true
      });
    } catch (error) {
      if (error instanceof Error) {
        throw new ConnectionError(`Failed to connect to replica at ${address}: ${error.message}`);
      }
      throw error;
    }
  }
  
  /**
   * Create a gRPC client
   */
  private async createClient(host: string, port: number | string): Promise<GrpcServiceClient> {
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

      const address = `${host}:${port}`;
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

      const client = new serviceProto(address, credentials, {
        'grpc.max_receive_message_length': 20 * 1024 * 1024,  // 20MB
        'grpc.max_send_message_length': 20 * 1024 * 1024      // 20MB
      });

      // Wait for the channel to be ready
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new TimeoutError(`Connection timeout after ${this.options.connectTimeout}ms`));
        }, this.options.connectTimeout);

        client.waitForReady(Date.now() + this.options.connectTimeout!, (error: Error | null) => {
          clearTimeout(timeout);
          if (error) {
            reject(new ConnectionError(`Failed to connect: ${error.message}`));
          } else {
            resolve();
          }
        });
      });

      return client;
    } catch (error) {
      if (error instanceof Error) {
        throw new ConnectionError(`Failed to create client: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Execute a read operation with retries and timeouts
   */
  async executeRead<T>(method: string, request: Record<string, unknown>): Promise<T> {
    if (!this.isConnected()) {
      await this.connect();
    }

    let lastError: Error | null = null;
    const maxRetries = this.options.maxRetries!;
    const retryDelay = this.options.retryDelay!;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        // Try to execute on a read client (possibly replica)
        const client = this.getReadClient();
        return await this.execute<T>(method, request, client);
      } catch (error) {
        if (error instanceof Error) {
          lastError = error;
          
          // Get error code
          const code = (error as { code?: number }).code;
          const retriableErrors = [
            grpc.status.UNAVAILABLE,
            grpc.status.INTERNAL,
            grpc.status.RESOURCE_EXHAUSTED,
            grpc.status.DEADLINE_EXCEEDED
          ];
          
          // Check for read-only errors
          const errorMessage = error.message.toLowerCase();
          const isReadOnlyError = errorMessage.includes('read-only') || 
            errorMessage.includes('readonly') ||
            code === grpc.status.FAILED_PRECONDITION;
          
          if (isReadOnlyError && this.primaryClient) {
            // If we get a read-only error and we know the primary, try using the primary
            try {
              return await this.execute<T>(method, request, this.primaryClient);
            } catch (primaryError) {
              // If that also fails, continue with the retry loop
              if (primaryError instanceof Error) {
                lastError = primaryError;
              }
            }
          }
          
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
   * Execute a write operation with retries and timeouts
   */
  async executeWrite<T>(method: string, request: Record<string, unknown>): Promise<T> {
    if (!this.isConnected()) {
      await this.connect();
    }

    let lastError: Error | null = null;
    const maxRetries = this.options.maxRetries!;
    const retryDelay = this.options.retryDelay!;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        // Always use the write client for write operations (primary)
        const client = this.getWriteClient();
        return await this.execute<T>(method, request, client);
      } catch (error) {
        if (error instanceof Error) {
          lastError = error;
          
          // Handle read-only errors
          const errorMessage = error.message.toLowerCase();
          if (errorMessage.includes('read-only') || errorMessage.includes('readonly')) {
            throw new ReadOnlyError();
          }
          
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
   * Execute an RPC method with retries and timeouts
   * This is a compatibility method that will be routed to executeRead or executeWrite
   * based on the method name
   */
  async executeWithRetry<T>(method: string, request: Record<string, unknown>): Promise<T> {
    // Determine if this is a read or write operation based on method name
    const readMethods = ['Get', 'GetStats', 'Scan', 'GetNodeInfo'];
    const isRead = readMethods.includes(method);
    
    if (isRead) {
      return this.executeRead<T>(method, request);
    } else {
      return this.executeWrite<T>(method, request);
    }
  }

  /**
   * Execute an RPC method on a specific client
   */
  private execute<T>(method: string, request: Record<string, unknown>, client: GrpcServiceClient): Promise<T> {
    return new Promise((resolve, reject) => {
      try {
        // Get the RPC method directly from the prototype
        const methodFn = client[method] as unknown as (
          request: Record<string, unknown>,
          metadata: { deadline: number },  
          callback: (error: Error | null, response: T) => void
        ) => void;
        
        if (typeof methodFn !== 'function') {
          const proto = Object.getPrototypeOf(client);
          const methods = Object.getOwnPropertyNames(proto)
            .filter(name => typeof proto[name] === 'function');
          
          reject(new Error(`Method ${method} not found. Available methods: ${methods.join(', ')}`));
          return;
        }
        
        // Set deadline for the request
        const deadline = Date.now() + this.options.requestTimeout!;
        
        // Call the method with callback
        methodFn.call(client, request, { deadline }, (error: Error | null, response: T) => {
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
   * Execute a streaming RPC method for read operations
   */
  executeReadStream(method: string, request: Record<string, unknown>): grpc.ClientReadableStream<unknown> {
    try {
      if (!this.isConnected()) {
        throw new ConnectionError('Not connected to Kevo database');
      }

      const client = this.getReadClient();
      
      const methodFn = client[method] as unknown as (
        request: Record<string, unknown>,
        metadata?: { deadline?: number }
      ) => grpc.ClientReadableStream<unknown>;
      
      if (typeof methodFn !== 'function') {
        const proto = Object.getPrototypeOf(client);
        const methods = Object.getOwnPropertyNames(proto)
          .filter(name => typeof proto[name] === 'function');
        
        throw new Error(`Streaming method ${method} not found. Available methods: ${methods.join(', ')}`);
      }
      
      // Set deadline for the request
      const deadline = Date.now() + this.options.requestTimeout!;
      
      return methodFn.call(client, request, { deadline });
    } catch (error) {
      throw error;
    }
  }
  
  /**
   * Execute a streaming RPC method for write operations
   */
  executeWriteStream(method: string, request: Record<string, unknown>): grpc.ClientReadableStream<unknown> {
    try {
      if (!this.isConnected()) {
        throw new ConnectionError('Not connected to Kevo database');
      }

      const client = this.getWriteClient();
      
      const methodFn = client[method] as unknown as (
        request: Record<string, unknown>,
        metadata?: { deadline?: number }
      ) => grpc.ClientReadableStream<unknown>;
      
      if (typeof methodFn !== 'function') {
        const proto = Object.getPrototypeOf(client);
        const methods = Object.getOwnPropertyNames(proto)
          .filter(name => typeof proto[name] === 'function');
        
        throw new Error(`Streaming method ${method} not found. Available methods: ${methods.join(', ')}`);
      }
      
      // Set deadline for the request
      const deadline = Date.now() + this.options.requestTimeout!;
      
      return methodFn.call(client, request, { deadline });
    } catch (error) {
      throw error;
    }
  }
  
  /**
   * Execute a streaming RPC method - compatibility method
   */
  executeStream(method: string, request: Record<string, unknown>): grpc.ClientReadableStream<unknown> {
    // Determine if this is a read or write operation based on method name
    const readMethods = ['Scan'];
    const isRead = readMethods.includes(method);
    
    if (isRead) {
      return this.executeReadStream(method, request);
    } else {
      return this.executeWriteStream(method, request);
    }
  }
}