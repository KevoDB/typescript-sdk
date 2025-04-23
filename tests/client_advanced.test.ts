/**
 * Additional tests for KevoClient advanced functionality
 */

import { KevoClient } from '../src';
import { Transaction } from '../src/transaction';
import { BatchWriter } from '../src/batch';
import { Scanner } from '../src/scanner';
import { Readable } from 'stream';
import * as grpc from '@grpc/grpc-js';

// Create a mock stream that emits data
function createMockStream(data: { key: Buffer; value: Buffer }[]): Readable {
  // Create a readable stream
  const stream = new Readable({
    objectMode: true,
    read() {}
  });

  // Push data to the stream
  for (const item of data) {
    stream.push({ key: item.key, value: item.value });
  }
  
  // End the stream
  setTimeout(() => {
    stream.push(null);
  }, 10);

  return stream;
}

describe('KevoClient Advanced Features', () => {
  let client: KevoClient;
  let mockClient: {
    GetStats: jest.Mock;
    Compact: jest.Mock;
    BeginTransaction: jest.Mock;
    Scan: jest.Mock;
    getChannel: jest.Mock;
  };
  
  beforeEach(() => {
    // Create mock client
    mockClient = {
      GetStats: jest.fn(),
      Compact: jest.fn(),
      BeginTransaction: jest.fn(),
      Scan: jest.fn(),
      getChannel: jest.fn().mockReturnValue({
        getConnectivityState: jest.fn().mockReturnValue(2), // READY
      }),
    };
    
    // Create client
    client = new KevoClient({
      host: 'localhost',
      port: 50051,
    });
    
    // Mock the internal connection
    // @ts-ignore - Accessing private property for testing
    client['connection']['client'] = mockClient;
    // @ts-ignore - Mocking connected state
    client['connection']['connected'] = true;
  });
  
  test('should trigger database compaction', async () => {
    // Setup mock
    mockClient.Compact.mockImplementation((request, metadata, callback) => {
      callback(null, {});
    });
    
    // Trigger compaction
    await client.compact();
    
    // Verify compact was called
    expect(mockClient.Compact).toHaveBeenCalled();
  });
  
  test('should handle compact error', async () => {
    // Setup mock to throw error
    mockClient.Compact.mockImplementation((request, metadata, callback) => {
      callback(new Error('Compaction failed'));
    });
    
    // Attempt to compact
    await expect(client.compact()).rejects.toThrow('Failed to trigger compaction: Compaction failed');
  });
  
  test('should begin a transaction', async () => {
    // Mock transaction ID
    const mockTxId = 'test-tx-id';
    
    // Setup mock
    mockClient.BeginTransaction.mockImplementation((request, metadata, callback) => {
      callback(null, { transaction_id: mockTxId });
    });
    
    // Begin transaction
    const tx = await client.beginTransaction();
    
    // Verify transaction was created
    expect(tx).toBeInstanceOf(Transaction);
    expect(tx.getId()).toBe(mockTxId);
    
    // Verify BeginTransaction was called
    expect(mockClient.BeginTransaction).toHaveBeenCalledWith(
      { readonly: false, timeout_ms: 30000 },
      expect.any(Object),
      expect.any(Function)
    );
  });
  
  test('should begin a readonly transaction', async () => {
    // Mock transaction ID
    const mockTxId = 'readonly-tx-id';
    
    // Setup mock
    mockClient.BeginTransaction.mockImplementation((request, metadata, callback) => {
      callback(null, { transaction_id: mockTxId });
    });
    
    // Begin readonly transaction
    const tx = await client.beginTransaction({ readOnly: true, timeoutMs: 60000 });
    
    // Verify BeginTransaction was called with correct options
    expect(mockClient.BeginTransaction).toHaveBeenCalledWith(
      { readonly: true, timeout_ms: 60000 },
      expect.any(Object),
      expect.any(Function)
    );
  });
  
  test('should create a batch writer', () => {
    // Create batch
    const batch = client.batch();
    
    // Verify batch was created
    expect(batch).toBeInstanceOf(BatchWriter);
  });
  
  test('should scan with options', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('key1'), value: Buffer.from('value1') },
      { key: Buffer.from('key2'), value: Buffer.from('value2') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockClient.Scan.mockReturnValue(mockStream);
    
    // Scan and collect results
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of client.scan({ 
      prefix: 'key',
      limit: 10,
      reverse: true 
    })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(2);
    expect(results[0].key.toString()).toBe('key1');
    expect(results[0].value.toString()).toBe('value1');
    
    // Verify Scan was called with correct options
    expect(mockClient.Scan).toHaveBeenCalledWith(
      {
        prefix: expect.any(Buffer),
        limit: 10,
        reverse: true,
      },
      expect.any(Object)
    );
  });
  
  test('should scan with prefix', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('prefix-key1'), value: Buffer.from('value1') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockClient.Scan.mockReturnValue(mockStream);
    
    // Scan and collect results
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of client.scanPrefix('prefix-', { limit: 5 })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(1);
    
    // Verify Scan was called with correct options
    expect(mockClient.Scan).toHaveBeenCalledWith(
      {
        prefix: expect.any(Buffer),
        limit: 5,
        reverse: false,
      },
      expect.any(Object)
    );
  });
  
  test('should scan with range', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('key2'), value: Buffer.from('value2') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockClient.Scan.mockReturnValue(mockStream);
    
    // Scan and collect results
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of client.scanRange('key1', 'key3', { reverse: true })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(1);
    
    // Verify Scan was called with correct options
    expect(mockClient.Scan).toHaveBeenCalledWith(
      {
        start: expect.any(Buffer),
        end: expect.any(Buffer),
        limit: 0,
        reverse: true,
      },
      expect.any(Object)
    );
  });
});