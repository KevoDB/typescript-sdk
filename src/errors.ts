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
    // Try to show the key as a utf8 string first, then fall back to hex if it's binary data
    let keyStr = Buffer.isBuffer(key) ? key.toString('utf8') : key;
    
    // Check if the key contains non-printable characters; if so, use hex encoding
    if (Buffer.isBuffer(key) && /[\x00-\x1F\x7F-\xFF]/.test(keyStr)) {
      keyStr = key.toString('hex');
    }
    
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

export class ReadOnlyError extends KevoError {
  constructor(message: string = 'Operation not allowed on read-only connection') {
    super(message);
    this.name = 'ReadOnlyError';
  }
}