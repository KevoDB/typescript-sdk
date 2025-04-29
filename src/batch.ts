/**
 * Batch operations for the Kevo SDK
 */

import { Connection } from './connection';
import { validateKey, validateValue } from './utils';

export enum OperationType {
  PUT = 0,
  DELETE = 1,
}

export interface BatchOperation {
  type: OperationType;
  key: string | Buffer;
  value?: string | Buffer;
}

export class BatchWriter {
  private connection: Connection;
  private operations: BatchOperation[] = [];
  
  constructor(connection: Connection) {
    this.connection = connection;
  }

  /**
   * Add a put operation to the batch
   */
  put(key: string | Buffer, value: string | Buffer): this {
    this.operations.push({
      type: OperationType.PUT,
      key,
      value,
    });
    return this;
  }

  /**
   * Add a delete operation to the batch
   */
  delete(key: string | Buffer): this {
    this.operations.push({
      type: OperationType.DELETE,
      key,
    });
    return this;
  }

  /**
   * Clear all operations from the batch
   */
  clear(): this {
    this.operations = [];
    return this;
  }

  /**
   * Get the number of operations in the batch
   */
  size(): number {
    return this.operations.length;
  }

  /**
   * Execute the batch of operations
   */
  async execute(): Promise<void> {
    if (this.operations.length === 0) {
      return;
    }

    const request = {
      operations: this.operations.map(op => {
        const keyBuffer = validateKey(op.key);
        
        if (op.type === OperationType.PUT) {
          if (!op.value) {
            throw new Error('Value is required for PUT operations');
          }
          const valueBuffer = validateValue(op.value);
          
          return {
            type: op.type,
            key: keyBuffer,
            value: valueBuffer,
          };
        } else {
          return {
            type: op.type,
            key: keyBuffer,
          };
        }
      }),
    };

    try {
      // Batch write is always a write operation that must go to the primary
      await this.connection.executeWrite<Record<string, never>>('BatchWrite', request);
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Batch execution failed: ${error.message}`);
      }
      throw error;
    }

    // Clear operations after successful execution
    this.clear();
  }
}