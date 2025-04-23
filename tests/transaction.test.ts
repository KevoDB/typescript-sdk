/**
 * Tests for Transaction
 */

import { Transaction } from '../src/transaction';
import { Connection } from '../src/connection';
import { KeyNotFoundError, TransactionError } from '../src/errors';
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

describe('Transaction', () => {
  let mockConnection: { 
    executeWithRetry: jest.Mock; 
    executeStream: jest.Mock; 
  };
  let transaction: Transaction;
  
  beforeEach(() => {
    // Create a mock connection
    mockConnection = {
      executeWithRetry: jest.fn(),
      executeStream: jest.fn(),
    };
    
    // Create a transaction with the mock connection
    transaction = new Transaction(mockConnection as unknown as Connection);
  });
  
  test('should begin a transaction', async () => {
    // Mock response
    const mockResponse = {
      transaction_id: 'test-tx-id',
    };
    
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue(mockResponse);
    
    // Begin transaction
    await transaction.begin();
    
    // Verify connection call
    expect(mockConnection.executeWithRetry).toHaveBeenCalledWith('BeginTransaction', {
      readonly: false,
      timeout_ms: 30000,
    });
    
    // Verify transaction ID was set
    expect(transaction.getId()).toBe('test-tx-id');
  });
  
  test('should commit a transaction', async () => {
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue({});
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Commit transaction
    await transaction.commit();
    
    // Verify connection call
    expect(mockConnection.executeWithRetry).toHaveBeenCalledWith('CommitTransaction', {
      transaction_id: 'test-tx-id',
    });
    
    // Verify transaction is marked as committed
    expect(transaction.isCommitted()).toBe(true);
  });
  
  test('should rollback a transaction', async () => {
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue({});
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Rollback transaction
    await transaction.rollback();
    
    // Verify connection call
    expect(mockConnection.executeWithRetry).toHaveBeenCalledWith('RollbackTransaction', {
      transaction_id: 'test-tx-id',
    });
    
    // Verify transaction is marked as rolled back
    expect(transaction.isRolledBack()).toBe(true);
  });
  
  test('should get a value from the transaction', async () => {
    // Mock response
    const mockResponse = {
      exists: true,
      value: Buffer.from('test-value'),
    };
    
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue(mockResponse);
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Get value
    const value = await transaction.get('test-key');
    
    // Verify connection call
    expect(mockConnection.executeWithRetry).toHaveBeenCalledWith('TxGet', {
      transaction_id: 'test-tx-id',
      key: expect.any(Buffer),
    });
    
    // Verify returned value
    expect(value.toString()).toBe('test-value');
  });
  
  test('should throw KeyNotFoundError when key does not exist', async () => {
    // Mock response
    const mockResponse = {
      exists: false,
    };
    
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue(mockResponse);
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Attempt to get value that doesn't exist
    await expect(transaction.get('non-existent-key')).rejects.toThrow(KeyNotFoundError);
  });
  
  test('should put a value in the transaction', async () => {
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue({});
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Put value
    await transaction.put('test-key', 'test-value');
    
    // Verify connection call
    expect(mockConnection.executeWithRetry).toHaveBeenCalledWith('TxPut', {
      transaction_id: 'test-tx-id',
      key: expect.any(Buffer),
      value: expect.any(Buffer),
    });
  });
  
  test('should not allow put in readonly transaction', async () => {
    // Create readonly transaction
    const readonlyTx = new Transaction(mockConnection as unknown as Connection, { readOnly: true });
    
    // Set transaction ID directly for testing
    (readonlyTx as any).id = 'readonly-tx-id';
    
    // Attempt to put value
    await expect(readonlyTx.put('test-key', 'test-value')).rejects.toThrow(
      'Cannot write in a read-only transaction'
    );
    
    // Verify connection not called
    expect(mockConnection.executeWithRetry).not.toHaveBeenCalled();
  });
  
  test('should delete a value in the transaction', async () => {
    // Setup mock
    mockConnection.executeWithRetry.mockResolvedValue({});
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Delete value
    await transaction.delete('test-key');
    
    // Verify connection call
    expect(mockConnection.executeWithRetry).toHaveBeenCalledWith('TxDelete', {
      transaction_id: 'test-tx-id',
      key: expect.any(Buffer),
    });
  });
  
  test('should not allow delete in readonly transaction', async () => {
    // Create readonly transaction
    const readonlyTx = new Transaction(mockConnection as unknown as Connection, { readOnly: true });
    
    // Set transaction ID directly for testing
    (readonlyTx as any).id = 'readonly-tx-id';
    
    // Attempt to delete value
    await expect(readonlyTx.delete('test-key')).rejects.toThrow(
      'Cannot delete in a read-only transaction'
    );
    
    // Verify connection not called
    expect(mockConnection.executeWithRetry).not.toHaveBeenCalled();
  });
  
  test('should scan values in the transaction', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('key1'), value: Buffer.from('value1') },
      { key: Buffer.from('key2'), value: Buffer.from('value2') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockConnection.executeStream.mockReturnValue(mockStream as unknown as grpc.ClientReadableStream<unknown>);
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Scan and collect results
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of transaction.scan({ prefix: 'key' })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(2);
    expect(results[0].key.toString()).toBe('key1');
    expect(results[0].value.toString()).toBe('value1');
    expect(results[1].key.toString()).toBe('key2');
    expect(results[1].value.toString()).toBe('value2');
    
    // Verify connection call
    expect(mockConnection.executeStream).toHaveBeenCalledWith('TxScan', {
      transaction_id: 'test-tx-id',
      limit: 0,
      reverse: false,
      prefix: expect.any(Buffer),
    });
  });
  
  test('should not allow operations after commit', async () => {
    // Setup mock for commit
    mockConnection.executeWithRetry.mockResolvedValue({});
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Commit transaction
    await transaction.commit();
    
    // Verify transaction is committed
    expect(transaction.isCommitted()).toBe(true);
    
    // Attempt operations after commit
    await expect(transaction.get('test-key')).rejects.toThrow('Transaction already committed');
    await expect(transaction.put('test-key', 'test-value')).rejects.toThrow('Transaction already committed');
    await expect(transaction.delete('test-key')).rejects.toThrow('Transaction already committed');
    
    // Attempt to scan after commit
    await expect(async () => {
      for await (const _ of transaction.scan()) {
        // Shouldn't get here
      }
    }).rejects.toThrow('Transaction already committed');
    
    // Attempt to commit again
    await expect(transaction.commit()).rejects.toThrow('Transaction already committed');
    
    // Attempt to rollback after commit
    await expect(transaction.rollback()).rejects.toThrow('Transaction already committed');
  });
  
  test('should not allow operations after rollback', async () => {
    // Setup mock for rollback
    mockConnection.executeWithRetry.mockResolvedValue({});
    
    // Set transaction ID directly for testing
    (transaction as any).id = 'test-tx-id';
    
    // Rollback transaction
    await transaction.rollback();
    
    // Verify transaction is rolled back
    expect(transaction.isRolledBack()).toBe(true);
    
    // Attempt operations after rollback
    await expect(transaction.get('test-key')).rejects.toThrow('Transaction already rolled back');
    await expect(transaction.put('test-key', 'test-value')).rejects.toThrow('Transaction already rolled back');
    await expect(transaction.delete('test-key')).rejects.toThrow('Transaction already rolled back');
    
    // Attempt to scan after rollback
    await expect(async () => {
      for await (const _ of transaction.scan()) {
        // Shouldn't get here
      }
    }).rejects.toThrow('Transaction already rolled back');
    
    // Attempt to commit after rollback
    await expect(transaction.commit()).rejects.toThrow('Transaction already rolled back');
    
    // Attempt to rollback again
    await expect(transaction.rollback()).rejects.toThrow('Transaction already rolled back');
  });
  
  test('should handle transaction begin error', async () => {
    // Setup mock to throw error
    const mockError = new Error('Connection failed');
    mockConnection.executeWithRetry.mockRejectedValue(mockError);
    
    // Attempt to begin transaction
    await expect(transaction.begin()).rejects.toThrow(
      'Failed to begin transaction: Connection failed'
    );
  });
});