/**
 * Tests for BatchWriter
 */

import { BatchWriter, OperationType } from '../src/batch';
import { Connection } from '../src/connection';

describe('BatchWriter', () => {
  let mockConnection: { executeWrite: jest.Mock };
  let batchWriter: BatchWriter;
  
  beforeEach(() => {
    // Create a mock connection
    mockConnection = {
      executeWrite: jest.fn(),
    };
    
    // Create a batch writer with the mock connection
    batchWriter = new BatchWriter(mockConnection as unknown as Connection);
  });
  
  test('should add put operations to batch', () => {
    // Add operations
    batchWriter.put('key1', 'value1');
    batchWriter.put('key2', 'value2');
    
    // Verify batch size
    expect(batchWriter.size()).toBe(2);
  });
  
  test('should add delete operations to batch', () => {
    // Add operations
    batchWriter.delete('key1');
    batchWriter.delete('key2');
    
    // Verify batch size
    expect(batchWriter.size()).toBe(2);
  });
  
  test('should clear batch operations', () => {
    // Add operations
    batchWriter.put('key1', 'value1');
    batchWriter.delete('key2');
    
    // Verify initial size
    expect(batchWriter.size()).toBe(2);
    
    // Clear batch
    batchWriter.clear();
    
    // Verify size after clear
    expect(batchWriter.size()).toBe(0);
  });
  
  test('should execute batch operations', async () => {
    // Setup mock
    mockConnection.executeWrite.mockResolvedValue({});
    
    // Add operations
    batchWriter.put('key1', 'value1');
    batchWriter.delete('key2');
    
    // Execute batch
    await batchWriter.execute();
    
    // Verify connection call
    expect(mockConnection.executeWrite).toHaveBeenCalledWith('BatchWrite', {
      operations: [
        {
          type: OperationType.PUT,
          key: expect.any(Buffer),
          value: expect.any(Buffer),
        },
        {
          type: OperationType.DELETE,
          key: expect.any(Buffer),
        },
      ],
    });
    
    // Verify batch is cleared after execution
    expect(batchWriter.size()).toBe(0);
  });
  
  test('should not execute an empty batch', async () => {
    // Execute empty batch
    await batchWriter.execute();
    
    // Verify connection not called
    expect(mockConnection.executeWrite).not.toHaveBeenCalled();
  });
  
  test('should throw error for put operation without value', async () => {
    // Create a batch with invalid PUT operation
    const batch = batchWriter;
    
    // Add invalid operation (using internal property to bypass type checking)
    const operations = (batch as any).operations;
    operations.push({
      type: OperationType.PUT,
      key: 'key-without-value',
      // No value provided
    });
    
    // Attempt to execute batch
    await expect(batch.execute()).rejects.toThrow('Value is required for PUT operations');
  });
  
  test('should handle batch execution error', async () => {
    // Setup mock to throw error
    const mockError = new Error('Connection failed');
    mockConnection.executeWrite.mockRejectedValue(mockError);
    
    // Add operation
    batchWriter.put('key1', 'value1');
    
    // Attempt to execute batch
    await expect(batchWriter.execute()).rejects.toThrow('Batch execution failed: Connection failed');
    
    // Verify batch is not cleared on error
    expect(batchWriter.size()).toBe(1);
  });
  
  test('should support method chaining', () => {
    // Use method chaining
    const result = batchWriter
      .put('key1', 'value1')
      .put('key2', 'value2')
      .delete('key3')
      .clear()
      .put('key4', 'value4');
    
    // Verify result is the batch writer
    expect(result).toBe(batchWriter);
    
    // Verify final state
    expect(batchWriter.size()).toBe(1);
  });
});