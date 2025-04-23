/**
 * Tests for Connection
 */

import { Connection } from '../src/connection';
import { ConnectionError, TimeoutError } from '../src/errors';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Mock grpc and proto-loader
jest.mock('@grpc/grpc-js');
jest.mock('@grpc/proto-loader');

describe('Connection', () => {
  let mockClient: {
    waitForReady: jest.Mock;
    getChannel: jest.Mock;
    close: jest.Mock;
    SomeMethod: jest.Mock;
  };
  let connection: Connection;
  
  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    
    // Setup mock client
    mockClient = {
      waitForReady: jest.fn().mockImplementation((deadline, callback) => callback(null)),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(grpc.connectivityState.READY),
      }),
      close: jest.fn(),
      SomeMethod: jest.fn(),
    };
    
    // Mock load package definition
    (protoLoader.load as jest.Mock).mockResolvedValue({});
    
    // Mock gRPC service class
    const mockServiceClass = jest.fn().mockImplementation(() => mockClient);
    
    (grpc.loadPackageDefinition as jest.Mock).mockReturnValue({
      kevo: {
        KevoService: mockServiceClass,
      },
    });
    
    // Create connection
    connection = new Connection({
      host: 'localhost',
      port: 50051,
    });
  });
  
  test('should connect to server', async () => {
    // Connect
    await connection.connect();
    
    // Verify connection
    expect(protoLoader.load).toHaveBeenCalled();
    expect(grpc.loadPackageDefinition).toHaveBeenCalled();
    expect(grpc.credentials.createInsecure).toHaveBeenCalled();
    expect(mockClient.waitForReady).toHaveBeenCalled();
    expect(mockClient.getChannel().getConnectivityState).toHaveBeenCalled();
    
    // Verify connected state
    expect(connection.isConnected()).toBe(true);
  });
  
  test('should connect with TLS', async () => {
    // Mock certificates
    const caCert = Buffer.from('ca-cert');
    const clientCert = Buffer.from('client-cert');
    const clientKey = Buffer.from('client-key');
    
    // Create connection with TLS
    connection = new Connection({
      host: 'localhost',
      port: 50051,
      useTls: true,
      caCert,
      clientCert,
      clientKey,
    });
    
    // Connect
    await connection.connect();
    
    // Verify SSL credentials used
    expect(grpc.credentials.createSsl).toHaveBeenCalledWith(
      caCert,
      clientKey,
      clientCert
    );
  });
  
  test('should throw error when connecting with TLS without CA cert', async () => {
    // Create connection with TLS but no caCert
    connection = new Connection({
      host: 'localhost',
      port: 50051,
      useTls: true,
      // No caCert provided
    });
    
    // Attempt to connect
    await expect(connection.connect()).rejects.toThrow(
      'CA certificate is required for TLS connections'
    );
  });
  
  test('should throw error when channel not ready', async () => {
    // Mock channel not ready
    mockClient.getChannel.mockReturnValue({
      getConnectivityState: jest.fn().mockReturnValue(grpc.connectivityState.TRANSIENT_FAILURE),
    });
    
    // Attempt to connect
    await expect(connection.connect()).rejects.toThrow(
      /Channel not ready/
    );
  });
  
  test('should throw error when waitForReady fails', async () => {
    // Mock waitForReady failure
    mockClient.waitForReady.mockImplementation((deadline, callback) => {
      callback(new Error('Connection refused'));
    });
    
    // Attempt to connect
    await expect(connection.connect()).rejects.toThrow(
      'Failed to connect: Connection refused'
    );
  });
  
  test('should throw error when connection times out', async () => {
    // Mock waitForReady to never call callback
    mockClient.waitForReady.mockImplementation(() => {
      // Do nothing, don't call callback
    });
    
    // Create connection with short timeout
    connection = new Connection({
      host: 'localhost',
      port: 50051,
      connectTimeout: 10, // 10ms timeout
    });
    
    // Attempt to connect
    await expect(connection.connect()).rejects.toThrow('Failed to connect: Connection timeout');
  });
  
  test('should disconnect from server', async () => {
    // Connect first
    await connection.connect();
    
    // Verify connected
    expect(connection.isConnected()).toBe(true);
    
    // Disconnect
    connection.disconnect();
    
    // Verify client closed
    expect(mockClient.close).toHaveBeenCalled();
    
    // Verify disconnected state
    expect(connection.isConnected()).toBe(false);
  });
  
  test('should execute RPC method', async () => {
    // Mock response
    const mockResponse = { result: 'success' };
    
    // Setup mock method
    mockClient.SomeMethod = jest.fn().mockImplementation((request, metadata, callback) => {
      callback(null, mockResponse);
    });
    
    // Connect first
    await connection.connect();
    
    // Execute method
    const result = await (connection as any).execute('SomeMethod', { key: 'value' });
    
    // Verify result
    expect(result).toEqual(mockResponse);
    
    // Verify method called
    expect(mockClient.SomeMethod).toHaveBeenCalledWith(
      { key: 'value' },
      { deadline: expect.any(Number) },
      expect.any(Function)
    );
  });
  
  test('should throw error when method not found', async () => {
    // Connect first
    await connection.connect();
    
    // Attempt to execute non-existent method
    await expect((connection as any).execute('NonExistentMethod', {})).rejects.toThrow(
      'Method NonExistentMethod not found'
    );
  });
  
  test('should throw error when method fails', async () => {
    // Setup mock method that fails
    mockClient.SomeMethod = jest.fn().mockImplementation((request, metadata, callback) => {
      callback(new Error('Operation failed'));
    });
    
    // Connect first
    await connection.connect();
    
    // Attempt to execute method
    await expect((connection as any).execute('SomeMethod', {})).rejects.toThrow(
      'Operation failed'
    );
  });
  
  test('should retry on retriable errors', async () => {
    // Setup a counter for retry attempts
    let attempts = 0;
    
    // Create a mock error with a retriable error code
    const retriableError = new Error('Service unavailable');
    (retriableError as any).code = grpc.status.UNAVAILABLE;
    
    // Setup mock method that fails on first attempt but succeeds on second
    mockClient.SomeMethod = jest.fn().mockImplementation((request, metadata, callback) => {
      attempts++;
      if (attempts === 1) {
        callback(retriableError);
      } else {
        callback(null, { result: 'success after retry' });
      }
    });
    
    // Connect first
    await connection.connect();
    
    // Execute method with retry
    const result = await connection.executeWithRetry('SomeMethod', {});
    
    // Verify method called twice
    expect(mockClient.SomeMethod).toHaveBeenCalledTimes(2);
    
    // Verify result from second attempt
    expect(result).toEqual({ result: 'success after retry' });
  });
  
  test('should not retry on non-retriable errors', async () => {
    // Create a mock error with a non-retriable error code
    const nonRetriableError = new Error('Permission denied');
    (nonRetriableError as any).code = grpc.status.PERMISSION_DENIED;
    
    // Setup mock method that fails
    mockClient.SomeMethod = jest.fn().mockImplementation((request, metadata, callback) => {
      callback(nonRetriableError);
    });
    
    // Connect first
    await connection.connect();
    
    // Attempt to execute method
    await expect(connection.executeWithRetry('SomeMethod', {})).rejects.toThrow(
      'Permission denied'
    );
    
    // Verify method called only once
    expect(mockClient.SomeMethod).toHaveBeenCalledTimes(1);
  });
  
  test('should auto-connect when executing method', async () => {
    // Setup mock method
    mockClient.SomeMethod = jest.fn().mockImplementation((request, metadata, callback) => {
      callback(null, { result: 'success' });
    });
    
    // Don't connect explicitly
    
    // Execute method (should auto-connect)
    await connection.executeWithRetry('SomeMethod', {});
    
    // Verify connection was established
    expect(connection.isConnected()).toBe(true);
    expect(protoLoader.load).toHaveBeenCalled();
  });
});