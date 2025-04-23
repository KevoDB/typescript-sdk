/**
 * Kevo Client implementation
 */

import { Connection, ConnectionOptions } from './connection';
import { Transaction, TransactionOptions } from './transaction';
import { Scanner, ScanOptions } from './scanner';
import { BatchWriter } from './batch';
import { KeyNotFoundError } from './errors';
import { validateKey, validateValue } from './utils';

// Re-export ConnectionOptions as our client uses it directly
export { ConnectionOptions };

export interface Stats {
  totalKeys: number;
  diskUsageBytes: number;
  memoryUsageBytes: number;
  lastCompactionTime: string;
  uptime: string;
  version: string;
}

export class KevoClient {
  private connection: Connection;
  
  constructor(options: ConnectionOptions) {
    this.connection = new Connection(options);
  }

  /**
   * Connect to the Kevo database
   */
  async connect(): Promise<void> {
    await this.connection.connect();
  }

  /**
   * Disconnect from the Kevo database
   */
  disconnect(): void {
    this.connection.disconnect();
  }

  /**
   * Check if connected to the Kevo database
   */
  isConnected(): boolean {
    return this.connection.isConnected();
  }

  /**
   * Get a value from the database
   */
  async get(key: string | Buffer): Promise<Buffer> {
    const keyBuffer = validateKey(key);

    try {
      const request = {
        key: keyBuffer
      };
      
      const response = await this.connection.executeWithRetry<{found: boolean; value: Uint8Array}>('Get', request);
      
      if (!response.found) {
        throw new KeyNotFoundError(keyBuffer);
      }
      
      return Buffer.from(response.value);
    } catch (error) {
      if (error instanceof KeyNotFoundError) {
        throw error;
      }
      if (error instanceof Error) {
        throw new Error(`Failed to get value: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Put a value into the database
   */
  async put(key: string | Buffer, value: string | Buffer, sync: boolean = false): Promise<void> {
    const keyBuffer = validateKey(key);
    const valueBuffer = validateValue(value);

    try {
      const request = {
        key: keyBuffer,
        value: valueBuffer,
        sync: sync
      };
      
      const response = await this.connection.executeWithRetry<{success: boolean}>('Put', request);
      
      if (response && !response.success) {
        throw new Error('Put operation failed: Server returned success=false');
      }
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Failed to put value: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Delete a value from the database
   */
  async delete(key: string | Buffer, sync: boolean = false): Promise<void> {
    const keyBuffer = validateKey(key);

    try {
      const request = {
        key: keyBuffer,
        sync: sync
      };
      
      const response = await this.connection.executeWithRetry<{success: boolean}>('Delete', request);
      
      if (response && !response.success) {
        throw new Error('Delete operation failed: Server returned success=false');
      }
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Failed to delete value: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Get database statistics
   */
  async getStats(): Promise<Stats> {
    try {
      const request = {};
      const response = await this.connection.executeWithRetry<{
        key_count: string;
        storage_size: string;
        memtable_count: string;
        sstable_count: string;
        write_amplification: number;
        read_amplification: number;
      }>('GetStats', request);
      
      return {
        totalKeys: parseInt(response.key_count, 10),
        diskUsageBytes: parseInt(response.storage_size, 10),
        memoryUsageBytes: 0, // Not available in the proto
        lastCompactionTime: new Date().toISOString(), // Not available in the proto
        uptime: "0", // Not available in the proto
        version: "1.0" // Not available in the proto
      };
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Failed to get stats: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Trigger database compaction
   */
  async compact(): Promise<void> {
    try {
      const request = {};
      await this.connection.executeWithRetry<Record<string, never>>('Compact', request);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Failed to trigger compaction: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Begin a new transaction
   */
  async beginTransaction(options?: TransactionOptions): Promise<Transaction> {
    const tx = new Transaction(this.connection, options);
    await tx.begin();
    return tx;
  }

  /**
   * Create a batch writer
   */
  batch(): BatchWriter {
    return new BatchWriter(this.connection);
  }

  /**
   * Create a scanner
   */
  async* scan(options?: ScanOptions): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    const scanner = new Scanner(this.connection);
    yield* scanner.scan(options);
  }

  /**
   * Scan for keys with a prefix
   */
  async* scanPrefix(prefix: string | Buffer, options: Omit<ScanOptions, 'prefix' | 'start' | 'end'> = {}): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    yield* this.scan({ ...options, prefix });
  }

  /**
   * Scan for keys in a range
   */
  async* scanRange(start: string | Buffer, end: string | Buffer, options: Omit<ScanOptions, 'prefix' | 'start' | 'end'> = {}): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    yield* this.scan({ ...options, start, end });
  }
}