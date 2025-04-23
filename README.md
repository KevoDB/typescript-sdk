# 🔑 Kevo TypeScript SDK

[![npm version](https://img.shields.io/npm/v/kevo-sdk.svg)](https://www.npmjs.com/package/kevo-sdk)
[![Node.js Version](https://img.shields.io/node/v/kevo-sdk.svg)](https://www.npmjs.com/package/kevo-sdk)
[![License](https://img.shields.io/github/license/KevoDB/typescript-sdk.svg)](LICENSE)

High-performance TypeScript/JavaScript client for the [Kevo](https://github.com/KevoDB/kevo) key-value store.

## ✨ Features

- Simple and intuitive API for JavaScript/TypeScript developers
- Efficient binary protocol (gRPC)
- Full TypeScript type definitions
- Transaction support with ACID guarantees
- Range and prefix scans using async iterators
- Atomic batch operations
- Buffer and string interface
- TLS/SSL support
- Automatic retries with exponential backoff

## 🚀 Installation

```bash
npm install kevo-sdk
```

Or install from source:

```bash
git clone https://github.com/KevoDB/typescript-sdk.git
cd typescript-sdk
npm install
npm run build
```

## 🏁 Quick Start

### JavaScript

```javascript
const { KevoClient, KeyNotFoundError } = require('kevo-sdk');

async function main() {
  // Create a client
  const client = new KevoClient({
    host: 'localhost',
    port: 50051,
  });

  try {
    // Connect to the database
    await client.connect();
    
    // Basic operations
    await client.put('hello', 'world');
    
    try {
      const value = await client.get('hello');
      console.log(value.toString()); // Prints: world
    } catch (error) {
      if (error instanceof KeyNotFoundError) {
        console.log('Key not found');
      } else {
        throw error;
      }
    }
    
    // Scan with prefix
    for await (const { key, value } of client.scanPrefix('user:')) {
      console.log(`Key: ${key.toString()}, Value: ${value.toString()}`);
    }
    
    // Use transactions
    const tx = await client.beginTransaction();
    try {
      await tx.put('key1', 'value1');
      await tx.put('key2', 'value2');
      await tx.commit();
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  } finally {
    // Always disconnect when done
    client.disconnect();
  }
}

main().catch(console.error);
```

### TypeScript

```typescript
import { KevoClient, ConnectionOptions, KeyNotFoundError } from 'kevo-sdk';

async function main() {
  // Create a client with type-safe options
  const options: ConnectionOptions = {
    host: 'localhost',
    port: 50051,
    // Optional settings
    useTls: false,
    connectTimeout: 5000,
    requestTimeout: 10000,
    maxRetries: 3,
  };
  
  const client = new KevoClient(options);

  try {
    // Connect to the database
    await client.connect();
    
    // Basic operations (works with string or Buffer)
    await client.put('counter', '1');
    await client.put(Buffer.from('binary-key'), Buffer.from([0x01, 0x02, 0x03]));
    
    // Get values
    try {
      const value = await client.get('counter');
      console.log(`Counter: ${value.toString()}`);
      
      const binaryValue = await client.get('binary-key');
      console.log(`Binary: ${binaryValue.toString('hex')}`);
    } catch (error) {
      if (error instanceof KeyNotFoundError) {
        console.log('Key not found');
      } else {
        throw error;
      }
    }
    
    // Transaction example
    const tx = await client.beginTransaction({ readOnly: false });
    try {
      const currentValue = await tx.get('counter');
      const newValue = (parseInt(currentValue.toString()) + 1).toString();
      await tx.put('counter', newValue);
      await tx.commit();
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  } finally {
    client.disconnect();
  }
}

main().catch(console.error);
```

## 📖 API Reference

### KevoClient

```typescript
const client = new KevoClient(options);
```

#### Constructor Options

```typescript
interface ConnectionOptions {
  host: string;               // Required: The database host
  port: number;               // Required: The database port
  useTls?: boolean;           // Optional: Whether to use TLS (default: false)
  caCert?: Buffer;            // Optional: CA certificate for TLS
  clientCert?: Buffer;        // Optional: Client certificate for TLS
  clientKey?: Buffer;         // Optional: Client key for TLS
  connectTimeout?: number;    // Optional: Connection timeout in ms (default: 5000)
  requestTimeout?: number;    // Optional: Request timeout in ms (default: 10000)
  maxRetries?: number;        // Optional: Maximum number of retries (default: 3)
  retryDelay?: number;        // Optional: Base delay between retries in ms (default: 1000)
}
```

#### Core Methods

| Method | Description |
|--------|-------------|
| `connect()` | Connect to the server |
| `disconnect()` | Close the connection |
| `isConnected()` | Check if connected to the server |
| `get(key)` | Get a value by key |
| `put(key, value, sync?)` | Store a key-value pair |
| `delete(key, sync?)` | Delete a key-value pair |

#### Advanced Features

| Method | Description |
|--------|-------------|
| `batch()` | Create a new batch writer |
| `scanPrefix(prefix, options?)` | Scan for keys with a prefix |
| `scanRange(start, end, options?)` | Scan for keys in a range |
| `scan(options?)` | Low-level scan with custom options |
| `beginTransaction(options?)` | Begin a new transaction |
| `getStats()` | Get database statistics |
| `compact()` | Trigger database compaction |

### Transaction

#### Methods

| Method | Description |
|--------|-------------|
| `getId()` | Get the transaction ID |
| `commit()` | Commit the transaction |
| `rollback()` | Roll back the transaction |
| `get(key)` | Get a value within the transaction |
| `put(key, value)` | Store a key-value pair within the transaction |
| `delete(key)` | Delete a key-value pair within the transaction |
| `scan(options?)` | Scan keys within the transaction |
| `isCommitted()` | Check if transaction is committed |
| `isRolledBack()` | Check if transaction is rolled back |

### BatchWriter

#### Methods

| Method | Description |
|--------|-------------|
| `put(key, value)` | Add a put operation to the batch |
| `delete(key)` | Add a delete operation to the batch |
| `size()` | Get the number of operations in the batch |
| `clear()` | Clear all operations from the batch |
| `execute()` | Execute all operations atomically |

### Scan Operations

```typescript
// Prefix scan
for await (const { key, value } of client.scanPrefix('user:')) {
  console.log(`${key.toString()}: ${value.toString()}`);
}

// Range scan
for await (const { key, value } of client.scanRange('user:100', 'user:200')) {
  console.log(`${key.toString()}: ${value.toString()}`);
}

// Scan with options
for await (const { key, value } of client.scan({
  prefix: 'user:',
  limit: 10,
  reverse: true
})) {
  console.log(`${key.toString()}: ${value.toString()}`);
}
```

### Error Handling

The SDK provides several error classes for specific error cases:

| Error Class | Description |
|-------------|-------------|
| `KevoError` | Base error class |
| `ConnectionError` | Connection-related errors |
| `TimeoutError` | Timeout errors |
| `TransactionError` | Transaction-related errors |
| `KeyNotFoundError` | Key not found errors |
| `InvalidArgumentError` | Invalid argument errors |

## 📋 Examples

Check the `examples` directory for more detailed examples:

- `01-basic-operations.js`: Basic key-value operations
- `02-transactions.js`: Transaction support with ACID guarantees
- `03-scanning.js`: Scanning operations with prefix and range

## 🛠️ Development

### Prerequisites

- Node.js 18+
- npm

### Setup

```bash
# Install dependencies
npm install

# Build the SDK
npm run build

# Run tests
npm run test

# Run linters
npm run lint

# Check TypeScript types
npm run typecheck
```

## 📄 License

[MIT](https://opensource.org/licenses/MIT)