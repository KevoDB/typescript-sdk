/**
 * Tests for Scanner
 */

import { Scanner } from '../src/scanner';
import { Connection } from '../src/connection';
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

describe('Scanner', () => {
  let mockConnection: { executeStream: jest.Mock };
  let scanner: Scanner;
  
  beforeEach(() => {
    // Create a mock connection
    mockConnection = {
      executeStream: jest.fn(),
    };
    
    // Create a scanner with the mock connection
    scanner = new Scanner(mockConnection as unknown as Connection);
  });
  
  test('should scan with no options', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('key1'), value: Buffer.from('value1') },
      { key: Buffer.from('key2'), value: Buffer.from('value2') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockConnection.executeStream.mockReturnValue(mockStream as unknown as grpc.ClientReadableStream<unknown>);
    
    // Scan and collect results
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of scanner.scan()) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(2);
    expect(results[0].key.toString()).toBe('key1');
    expect(results[0].value.toString()).toBe('value1');
    expect(results[1].key.toString()).toBe('key2');
    expect(results[1].value.toString()).toBe('value2');
    
    // Verify connection calls
    expect(mockConnection.executeStream).toHaveBeenCalledWith('Scan', {
      limit: 0,
      reverse: false,
    });
  });
  
  test('should scan with prefix option', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('prefix-key1'), value: Buffer.from('value1') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockConnection.executeStream.mockReturnValue(mockStream as unknown as grpc.ClientReadableStream<unknown>);
    
    // Scan with prefix
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of scanner.scan({ prefix: 'prefix-' })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(1);
    expect(results[0].key.toString()).toBe('prefix-key1');
    
    // Verify connection calls
    expect(mockConnection.executeStream).toHaveBeenCalledWith('Scan', {
      limit: 0,
      reverse: false,
      prefix: expect.any(Buffer),
    });
  });
  
  test('should scan with range options', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('key3'), value: Buffer.from('value3') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockConnection.executeStream.mockReturnValue(mockStream as unknown as grpc.ClientReadableStream<unknown>);
    
    // Scan with range
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of scanner.scan({ 
      start: 'key2', 
      end: 'key4',
      limit: 10,
      reverse: true
    })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(1);
    expect(results[0].key.toString()).toBe('key3');
    
    // Verify connection calls
    expect(mockConnection.executeStream).toHaveBeenCalledWith('Scan', {
      start_key: expect.any(Buffer),
      end_key: expect.any(Buffer),
      limit: 10,
      reverse: true,
    });
  });
  
  test('should scan with suffix option', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('key-suffix'), value: Buffer.from('value-suffix') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockConnection.executeStream.mockReturnValue(mockStream as unknown as grpc.ClientReadableStream<unknown>);
    
    // Scan with suffix
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of scanner.scan({ suffix: 'suffix' })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(1);
    expect(results[0].key.toString()).toBe('key-suffix');
    
    // Verify connection calls
    expect(mockConnection.executeStream).toHaveBeenCalledWith('Scan', {
      limit: 0,
      reverse: false,
      suffix: expect.any(Buffer),
    });
  });

  test('should scan with both prefix and suffix', async () => {
    // Mock data
    const mockData = [
      { key: Buffer.from('prefix-key-suffix'), value: Buffer.from('value-both') },
    ];
    
    // Mock the stream
    const mockStream = createMockStream(mockData);
    mockConnection.executeStream.mockReturnValue(mockStream as unknown as grpc.ClientReadableStream<unknown>);
    
    // Scan with both prefix and suffix
    const results: { key: Buffer; value: Buffer }[] = [];
    for await (const item of scanner.scan({ 
      prefix: 'prefix-',
      suffix: 'suffix',
    })) {
      results.push(item);
    }
    
    // Verify results
    expect(results).toHaveLength(1);
    expect(results[0].key.toString()).toBe('prefix-key-suffix');
    
    // Verify connection calls
    expect(mockConnection.executeStream).toHaveBeenCalledWith('Scan', {
      limit: 0,
      reverse: false,
      prefix: expect.any(Buffer),
      suffix: expect.any(Buffer),
    });
  });

  test('should handle scan errors', async () => {
    // Mock connection to throw error
    const mockError = new Error('Test error');
    mockConnection.executeStream.mockImplementation(() => {
      throw mockError;
    });
    
    // Attempt to scan
    await expect(async () => {
      for await (const _ of scanner.scan()) {
        // Shouldn't get here
      }
    }).rejects.toThrow('Scan failed: Test error');
  });
});