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
          GetNodeInfo: jest.fn().mockImplementation((request, metadata, callback) => {
            callback(null, { role: 'primary', replicas: [] });
          }),
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
    // Mock executeRead method on the connection
    (client as any).connection.executeRead = jest.fn().mockResolvedValue({
      found: true,
      value: Buffer.from('test-value'),
    });
    
    // Set the connected state
    (client as any).connection.connected = true;
    
    const value = await client.get('test-key');
    expect(value.toString()).toBe('test-value');
  });
  
  test('should throw KeyNotFoundError when key does not exist', async () => {
    // Mock executeRead method on the connection
    (client as any).connection.executeRead = jest.fn().mockResolvedValue({
      found: false,
    });
    
    // Set the connected state
    (client as any).connection.connected = true;
    
    await expect(client.get('non-existent-key')).rejects.toThrow(KeyNotFoundError);
  });
  
  test('should put a value', async () => {
    // Mock executeWrite method on the connection
    (client as any).connection.executeWrite = jest.fn().mockResolvedValue({});
    
    // Set the connected state
    (client as any).connection.connected = true;
    
    await expect(client.put('test-key', 'test-value')).resolves.not.toThrow();
  });
  
  test('should delete a value', async () => {
    // Mock executeWrite method on the connection
    (client as any).connection.executeWrite = jest.fn().mockResolvedValue({});
    
    // Set the connected state
    (client as any).connection.connected = true;
    
    await expect(client.delete('test-key')).resolves.not.toThrow();
  });
  
  test('should get database stats', async () => {
    // Mock executeRead method on the connection to match the expected stats format
    (client as any).connection.executeRead = jest.fn().mockResolvedValue({
      key_count: 100,
      storage_size: 1024,
      memtable_count: 1,
      sstable_count: 5,
      write_amplification: 1.5,
      read_amplification: 1.2,
      operation_counts: { get: 1000, put: 500 },
      latency_stats: {
        get: { count: 1000, avg_ns: 100000, min_ns: 50000, max_ns: 500000 },
        put: { count: 500, avg_ns: 200000, min_ns: 100000, max_ns: 1000000 }
      },
      error_counts: { timeout: 5 },
      total_bytes_read: 10240,
      total_bytes_written: 5120,
      flush_count: 10,
      compaction_count: 2,
      recovery_stats: {
        wal_files_recovered: 1,
        wal_entries_recovered: 100,
        wal_corrupted_entries: 0,
        wal_recovery_duration_ms: 500
      }
    });
    
    // Set the connected state
    (client as any).connection.connected = true;
    
    const stats = await client.getStats();
    
    // Check that the stats object has the expected properties (as defined in the Stats interface)
    expect(stats).toHaveProperty('keyCount');
    expect(stats).toHaveProperty('storageSize');
    expect(stats).toHaveProperty('memtableCount');
    expect(stats).toHaveProperty('latencyStats');
    expect(stats).toHaveProperty('recoveryStats');
    
    // Verify specific values
    expect(stats.keyCount).toBe(100);
    expect(stats.storageSize).toBe(1024);
    expect(stats.operationCounts).toEqual({ get: 1000, put: 500 });
  });
});