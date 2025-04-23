/**
 * Transaction support for the Kevo SDK
 */

import { Connection } from './connection';
import { KeyNotFoundError, TransactionError } from './errors';
import { validateKey, validateValue, generateTransactionId } from './utils';

export interface TransactionOptions {
  readOnly?: boolean;
  timeoutMs?: number;
}

interface TransactionResponse {
  transaction_id: string;
}

interface TxGetResponse {
  exists: boolean;
  value: Uint8Array;
}

export class Transaction {
  private connection: Connection;
  private id: string;
  private committed = false;
  private rolledBack = false;
  private readonly: boolean;
  private timeout: number;

  constructor(connection: Connection, options: TransactionOptions = {}) {
    this.connection = connection;
    this.id = generateTransactionId();
    this.readonly = options.readOnly || false;
    this.timeout = options.timeoutMs || 30000; // Default to 30 seconds
  }

  /**
   * Begin the transaction
   */
  async begin(): Promise<void> {
    try {
      const request = {
        readonly: this.readonly,
        timeout_ms: this.timeout
      };
      
      const response = await this.connection.executeWithRetry<TransactionResponse>('BeginTransaction', request);
      this.id = response.transaction_id;
    } catch (error) {
      if (error instanceof Error) {
        throw new TransactionError(`Failed to begin transaction: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Commit the transaction
   */
  async commit(): Promise<void> {
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    try {
      const request = {
        transaction_id: this.id
      };
      
      await this.connection.executeWithRetry<Record<string, never>>('CommitTransaction', request);
      this.committed = true;
    } catch (error) {
      if (error instanceof Error) {
        throw new TransactionError(`Failed to commit transaction: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Rollback the transaction
   */
  async rollback(): Promise<void> {
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    try {
      const request = {
        transaction_id: this.id
      };
      
      await this.connection.executeWithRetry<Record<string, never>>('RollbackTransaction', request);
      this.rolledBack = true;
    } catch (error) {
      if (error instanceof Error) {
        throw new TransactionError(`Failed to rollback transaction: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Get a value from the database within the transaction
   */
  async get(key: string | Buffer): Promise<Buffer> {
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    const keyBuffer = validateKey(key);

    try {
      const request = {
        transaction_id: this.id,
        key: keyBuffer
      };
      
      const response = await this.connection.executeWithRetry<TxGetResponse>('TxGet', request);
      
      if (!response.exists) {
        throw new KeyNotFoundError(keyBuffer);
      }
      
      return Buffer.from(response.value);
    } catch (error) {
      if (error instanceof KeyNotFoundError) {
        throw error;
      }
      if (error instanceof Error) {
        throw new TransactionError(`Failed to get value: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Put a value into the database within the transaction
   */
  async put(key: string | Buffer, value: string | Buffer): Promise<void> {
    if (this.readonly) {
      throw new TransactionError('Cannot write in a read-only transaction');
    }
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    const keyBuffer = validateKey(key);
    const valueBuffer = validateValue(value);

    try {
      const request = {
        transaction_id: this.id,
        key: keyBuffer,
        value: valueBuffer
      };
      
      await this.connection.executeWithRetry<Record<string, never>>('TxPut', request);
    } catch (error) {
      if (error instanceof Error) {
        throw new TransactionError(`Failed to put value: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Delete a value from the database within the transaction
   */
  async delete(key: string | Buffer): Promise<void> {
    if (this.readonly) {
      throw new TransactionError('Cannot delete in a read-only transaction');
    }
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    const keyBuffer = validateKey(key);

    try {
      const request = {
        transaction_id: this.id,
        key: keyBuffer
      };
      
      await this.connection.executeWithRetry<Record<string, never>>('TxDelete', request);
    } catch (error) {
      if (error instanceof Error) {
        throw new TransactionError(`Failed to delete value: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Scan a range of key-value pairs within the transaction
   */
  async* scan(options: {
    prefix?: string | Buffer;
    suffix?: string | Buffer;
    start?: string | Buffer;
    end?: string | Buffer;
    limit?: number;
    reverse?: boolean;
  } = {}): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    try {
      const request: Record<string, unknown> = {
        transaction_id: this.id,
        limit: options.limit || 0,
        reverse: options.reverse || false
      };

      if (options.prefix) {
        request.prefix = validateKey(options.prefix);
      }
      
      if (options.suffix) {
        request.suffix = validateKey(options.suffix);
      }
      
      if (options.start || options.end) {
        if (options.start) {
          request.start_key = validateKey(options.start);
        }
        if (options.end) {
          request.end_key = validateKey(options.end);
        }
      }

      const stream = this.connection.executeStream('TxScan', request);
      
      for await (const response of stream) {
        // Cast response to expected shape
        const item = response as { key?: Uint8Array; value?: Uint8Array };
        if (item.key && item.value) {
          yield {
            key: Buffer.from(item.key),
            value: Buffer.from(item.value),
          };
        }
      }
    } catch (error) {
      if (error instanceof Error) {
        throw new TransactionError(`Scan failed: ${error.message}`);
      }
      throw error;
    }
  }

  /**
   * Get the transaction ID
   */
  getId(): string {
    return this.id;
  }

  /**
   * Check if the transaction is committed
   */
  isCommitted(): boolean {
    return this.committed;
  }

  /**
   * Check if the transaction is rolled back
   */
  isRolledBack(): boolean {
    return this.rolledBack;
  }
}