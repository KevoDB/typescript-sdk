/**
 * Kevo TypeScript SDK
 */

export { KevoClient, ConnectionOptions, Stats } from './client';
export { TransactionOptions } from './transaction';
export { ScanOptions } from './scanner';
export { BatchOperation, OperationType } from './batch';
export {
  KevoError,
  ConnectionError,
  TimeoutError,
  TransactionError,
  KeyNotFoundError,
  InvalidArgumentError
} from './errors';