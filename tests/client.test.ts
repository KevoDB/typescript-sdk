/**
 * Tests for KevoClient
 */

import { KevoClient } from '../src';
import { KeyNotFoundError } from '../src/errors';

// Mock the gRPC client
jest.mock('@grpc/grpc-js', () => {
  const connectivityState = {
    IDLE: 0,
    CONNECTING: 1,
    READY: 2,
    TRANSIENT_FAILURE: 3,
    FATAL_FAILURE: 4,
    SHUTDOWN: 5,
  };

  const status = {
    OK: 0,
    CANCELLED: 1,
    UNKNOWN: 2,
    INVALID_ARGUMENT: 3,
    DEADLINE_EXCEEDED: 4,
    NOT_FOUND: 5,
    ALREADY_EXISTS: 6,
    PERMISSION_DENIED: 7,
    RESOURCE_EXHAUSTED: 8,
    FAILED_PRECONDITION: 9,
    ABORTED: 10,
    OUT_OF_RANGE: 11,
    UNIMPLEMENTED: 12,
    INTERNAL: 13,
    UNAVAILABLE: 14,
    DATA_LOSS: 15,
    UNAUTHENTICATED: 16,
  };

  return {
    connectivityState,
    status,
    credentials: {
      createInsecure: jest.fn().mockReturnValue({}),
      createSsl: jest.fn().mockReturnValue({}),
    },
    loadPackageDefinition: jest.fn().mockImplementation(() => ({
      kevo: {
        KevoService: jest.fn().mockImplementation(() => ({
          waitForReady: jest.fn().mockImplementation((deadline, callback) => {
            callback(null);
          }),
          getChannel: jest.fn().mockReturnValue({
            getConnectivityState: jest.fn().mockReturnValue(connectivityState.READY),
          }),
          close: jest.fn(),
          Get: jest.fn(),
          Put: jest.fn(),
          Delete: jest.fn(),
          BatchWrite: jest.fn(),
          Scan: jest.fn(),
          BeginTransaction: jest.fn(),
          CommitTransaction: jest.fn(),
          RollbackTransaction: jest.fn(),
          TxGet: jest.fn(),
          TxPut: jest.fn(),
          TxDelete: jest.fn(),
          TxScan: jest.fn(),
          GetStats: jest.fn(),
          Compact: jest.fn(),
        })),
      },
    })),
  };
});

jest.mock('@grpc/proto-loader', () => ({
  load: jest.fn().mockResolvedValue({}),
}));

describe('KevoClient', () => {
  let client: KevoClient;
  
  beforeEach(() => {
    client = new KevoClient({
      host: 'localhost',
      port: 50051,
    });
  });
  
  afterEach(() => {
    jest.clearAllMocks();
  });
  
  test('should connect to the database', async () => {
    await client.connect();
    expect(client.isConnected()).toBe(true);
  });
  
  test('should disconnect from the database', async () => {
    await client.connect();
    client.disconnect();
    expect(client.isConnected()).toBe(false);
  });
  
  test('should get a value', async () => {
    const mockResponse = {
      exists: true,
      value: Buffer.from('test-value'),
    };
    
    // @ts-ignore - Mocking internal client
    client['connection']['client'] = {
      Get: jest.fn().mockImplementation((request, metadata, callback) => {
        callback(null, mockResponse);
      }),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(2), // READY
      }),
    };
    
    // @ts-ignore - Mock connected state
    client['connection']['connected'] = true;
    
    const value = await client.get('test-key');
    expect(value.toString()).toBe('test-value');
  });
  
  test('should throw KeyNotFoundError when key does not exist', async () => {
    const mockResponse = {
      exists: false,
    };
    
    // @ts-ignore - Mocking internal client
    client['connection']['client'] = {
      Get: jest.fn().mockImplementation((request, metadata, callback) => {
        callback(null, mockResponse);
      }),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(2), // READY
      }),
    };
    
    // @ts-ignore - Mock connected state
    client['connection']['connected'] = true;
    
    await expect(client.get('non-existent-key')).rejects.toThrow(KeyNotFoundError);
  });
  
  test('should put a value', async () => {
    const mockResponse = {};
    
    // @ts-ignore - Mocking internal client
    client['connection']['client'] = {
      Put: jest.fn().mockImplementation((request, metadata, callback) => {
        callback(null, mockResponse);
      }),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(2), // READY
      }),
    };
    
    // @ts-ignore - Mock connected state
    client['connection']['connected'] = true;
    
    await expect(client.put('test-key', 'test-value')).resolves.not.toThrow();
  });
  
  test('should delete a value', async () => {
    const mockResponse = {};
    
    // @ts-ignore - Mocking internal client
    client['connection']['client'] = {
      Delete: jest.fn().mockImplementation((request, metadata, callback) => {
        callback(null, mockResponse);
      }),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(2), // READY
      }),
    };
    
    // @ts-ignore - Mock connected state
    client['connection']['connected'] = true;
    
    await expect(client.delete('test-key')).resolves.not.toThrow();
  });
  
  test('should get database stats', async () => {
    const mockResponse = {
      total_keys: '100',
      disk_usage_bytes: '1024',
      memory_usage_bytes: '512',
      last_compaction_time: '2023-01-01T00:00:00Z',
      uptime: '1d 2h 3m',
      version: '1.0.0',
    };
    
    // @ts-ignore - Mocking internal client
    client['connection']['client'] = {
      GetStats: jest.fn().mockImplementation((request, metadata, callback) => {
        callback(null, mockResponse);
      }),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(2), // READY
      }),
    };
    
    // @ts-ignore - Mock connected state
    client['connection']['connected'] = true;
    
    const stats = await client.getStats();
    expect(stats).toEqual({
      totalKeys: 100,
      diskUsageBytes: 1024,
      memoryUsageBytes: 512,
      lastCompactionTime: '2023-01-01T00:00:00Z',
      uptime: '1d 2h 3m',
      version: '1.0.0',
    });
  });
});