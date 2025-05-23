/**
 * Scanner implementation for iterating over key-value pairs
 */

import { Connection } from './connection';
import { validateKey } from './utils';

export interface ScanOptions {
  prefix?: string | Buffer;
  suffix?: string | Buffer;
  start?: string | Buffer;
  end?: string | Buffer;
  limit?: number;
  reverse?: boolean;
}

export class Scanner {
  private connection: Connection;
  
  constructor(connection: Connection) {
    this.connection = connection;
  }

  /**
   * Scan a range of key-value pairs
   */
  async* scan(options: ScanOptions = {}): AsyncGenerator<{ key: Buffer; value: Buffer }, void, unknown> {
    try {
      const request: Record<string, unknown> = {
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

      // Use executeReadStream to route to a replica if possible (based on client config)
      const stream = this.connection.executeReadStream('Scan', request);

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
        throw new Error(`Scan failed: ${error.message}`);
      }
      throw error;
    }
  }
}