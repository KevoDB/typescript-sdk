/**
 * Error types for the Kevo SDK
 */

export class KevoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'KevoError';
  }
}

export class ConnectionError extends KevoError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

export class TimeoutError extends KevoError {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
  }
}

export class TransactionError extends KevoError {
  constructor(message: string) {
    super(message);
    this.name = 'TransactionError';
  }
}

export class KeyNotFoundError extends KevoError {
  constructor(key: string | Buffer) {
    const keyStr = Buffer.isBuffer(key) ? key.toString('hex') : key;
    super(`Key not found: ${keyStr}`);
    this.name = 'KeyNotFoundError';
  }
}

export class InvalidArgumentError extends KevoError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidArgumentError';
  }
}