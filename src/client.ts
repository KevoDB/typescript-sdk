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
export { ConnectionOptions, ReplicaSelectionStrategy };

export interface LatencyStats {
  count: number;
  avgNs: number;
  minNs: number;
  maxNs: number;
}

export interface RecoveryStats {
  walFilesRecovered: number;
  walEntriesRecovered: number;
  walCorruptedEntries: number;
  walRecoveryDurationMs: number;
}

export interface Stats {
  keyCount: number;
  storageSize: number;
  memtableCount: number;
  sstableCount: number;
  writeAmplification: number;
  readAmplification: number;
  operationCounts: Record<string, number>;
  latencyStats: Record<string, LatencyStats>;
  errorCounts: Record<string, number>;
  totalBytesRead: number;
  totalBytesWritten: number;
  flushCount: number;
  compactionCount: number;
  recoveryStats: RecoveryStats;
}

export interface KevoClientOptions extends ConnectionOptions {
  autoRouteReads?: boolean;
  autoRouteWrites?: boolean;
  preferReplica?: boolean;
  replicaSelectionStrategy?: ReplicaSelectionStrategy;
}

export class KevoClient {
  private connection: Connection;
  
  constructor(options: KevoClientOptions) {
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
   * @param key The key to retrieve
   */
  async get(key: string | Buffer): Promise<Buffer> {
    const keyBuffer = validateKey(key);

    try {
      const request = {
        key: keyBuffer
      };
      
      const response = await this.connection.executeRead<{found: boolean; value: Uint8Array}>('Get', request);
      
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
      
      await this.connection.executeWrite<Record<string, never>>('Put', request);
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
      
      await this.connection.executeWrite<Record<string, never>>('Delete', request);
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
      const response = await this.connection.executeRead<{
        key_count?: number;
        storage_size?: number;
        memtable_count?: number;
        sstable_count?: number;
        write_amplification?: number;
        read_amplification?: number;
        operation_counts?: Record<string, number>;
        latency_stats?: Record<string, {
          count: number;
          avg_ns: number;
          min_ns: number;
          max_ns: number;
        }>;
        error_counts?: Record<string, number>;
        total_bytes_read?: number;
        total_bytes_written?: number;
        flush_count?: number;
        compaction_count?: number;
        recovery_stats?: {
          wal_files_recovered?: number;
          wal_entries_recovered?: number;
          wal_corrupted_entries?: number;
          wal_recovery_duration_ms?: number;
        };
      }>('GetStats', request, preferReplica);
      
      // Convert snake_case keys to camelCase and handle null values
      const latencyStats: Record<string, LatencyStats> = {};
      if (response.latency_stats) {
        for (const [key, value] of Object.entries(response.latency_stats)) {
          if (value) {
            latencyStats[key] = {
              count: value.count || 0,
              avgNs: value.avg_ns || 0,
              minNs: value.min_ns || 0,
              maxNs: value.max_ns || 0
            };
          }
        }
      }
      
      // Ensure all fields have default values to prevent errors
      return {
        keyCount: response.key_count || 0,
        storageSize: response.storage_size || 0,
        memtableCount: response.memtable_count || 0,
        sstableCount: response.sstable_count || 0,
        writeAmplification: response.write_amplification || 0,
        readAmplification: response.read_amplification || 0,
        operationCounts: response.operation_counts || {},
        latencyStats,
        errorCounts: response.error_counts || {},
        totalBytesRead: response.total_bytes_read || 0,
        totalBytesWritten: response.total_bytes_written || 0,
        flushCount: response.flush_count || 0,
        compactionCount: response.compaction_count || 0,
        recoveryStats: {
          walFilesRecovered: response.recovery_stats?.wal_files_recovered || 0,
          walEntriesRecovered: response.recovery_stats?.wal_entries_recovered || 0,
          walCorruptedEntries: response.recovery_stats?.wal_corrupted_entries || 0,
          walRecoveryDurationMs: response.recovery_stats?.wal_recovery_duration_ms || 0
        }
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
      // Compaction is a write operation as it modifies the database
      await this.connection.executeWrite<Record<string, never>>('Compact', request);
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
   * @param options Scan options, including preferReplica to control routing
   */
  async* scan(options?: ScanOptions): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    const scanner = new Scanner(this.connection);
    yield* scanner.scan(options);
  }

  /**
   * Scan for keys with a prefix
   * @param prefix The prefix to match
   * @param options Additional scan options, including preferReplica to control routing
   */
  async* scanPrefix(
    prefix: string | Buffer, 
    options: Omit<ScanOptions, 'prefix' | 'suffix' | 'start' | 'end'> = {}
  ): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    yield* this.scan({ ...options, prefix });
  }

  /**
   * Scan for keys in a range
   * @param start The start key of the range (inclusive)
   * @param end The end key of the range (exclusive)
   * @param options Additional scan options, including preferReplica to control routing
   */
  async* scanRange(
    start: string | Buffer, 
    end: string | Buffer, 
    options: Omit<ScanOptions, 'prefix' | 'suffix' | 'start' | 'end'> = {}
  ): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    yield* this.scan({ ...options, start, end });
  }

  /**
   * Scan for keys with a suffix
   * @param suffix The suffix to match
   * @param options Additional scan options, including preferReplica to control routing
   */
  async* scanSuffix(
    suffix: string | Buffer, 
    options: Omit<ScanOptions, 'prefix' | 'suffix' | 'start' | 'end'> = {}
  ): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    yield* this.scan({ ...options, suffix });
  }
}