/**
 * Utility functions for the Kevo SDK
 */

/**
 * Convert a string or Buffer to a Buffer
 */
export function toBuffer(value: string | Buffer): Buffer {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  return Buffer.from(value);
}

/**
 * Validate key size
 */
export function validateKey(key: string | Buffer): Buffer {
  const keyBuffer = toBuffer(key);
  if (keyBuffer.length === 0) {
    throw new Error('Key cannot be empty');
  }
  if (keyBuffer.length > 1024) {
    throw new Error('Key cannot be larger than 1KB');
  }
  return keyBuffer;
}

/**
 * Validate value size
 */
export function validateValue(value: string | Buffer): Buffer {
  if (value === null || value === undefined) {
    throw new Error('Value cannot be null or undefined');
  }
  
  const valueBuffer = toBuffer(value);
  if (valueBuffer.length > 10 * 1024 * 1024) {
    throw new Error('Value cannot be larger than 10MB');
  }
  return valueBuffer;
}

/**
 * Sleep for the specified number of milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Generate a random transaction ID
 */
export function generateTransactionId(): string {
  return `tx-${Date.now()}-${Math.floor(Math.random() * 1000000)}`;
}