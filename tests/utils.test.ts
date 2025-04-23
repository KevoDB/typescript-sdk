/**
 * Tests for utility functions
 */

import { toBuffer, validateKey, validateValue, sleep, generateTransactionId } from '../src/utils';

describe('Utility Functions', () => {
  describe('toBuffer', () => {
    test('should return the same Buffer if input is a Buffer', () => {
      const inputBuffer = Buffer.from('test');
      const result = toBuffer(inputBuffer);
      expect(result).toBe(inputBuffer); // Same instance
      expect(result.toString()).toBe('test');
    });
    
    test('should convert string to Buffer', () => {
      const result = toBuffer('test');
      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString()).toBe('test');
    });
  });
  
  describe('validateKey', () => {
    test('should accept valid string key', () => {
      const result = validateKey('valid-key');
      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString()).toBe('valid-key');
    });
    
    test('should accept valid Buffer key', () => {
      const inputBuffer = Buffer.from('valid-key');
      const result = validateKey(inputBuffer);
      expect(result).toBe(inputBuffer);
    });
    
    test('should throw error for empty key', () => {
      expect(() => validateKey('')).toThrow('Key cannot be empty');
      expect(() => validateKey(Buffer.from(''))).toThrow('Key cannot be empty');
    });
    
    test('should throw error for key larger than 1KB', () => {
      const largeKey = 'a'.repeat(1025); // 1KB + 1 byte
      expect(() => validateKey(largeKey)).toThrow('Key cannot be larger than 1KB');
    });
  });
  
  describe('validateValue', () => {
    test('should accept valid string value', () => {
      const result = validateValue('valid-value');
      expect(Buffer.isBuffer(result)).toBe(true);
      expect(result.toString()).toBe('valid-value');
    });
    
    test('should accept valid Buffer value', () => {
      const inputBuffer = Buffer.from('valid-value');
      const result = validateValue(inputBuffer);
      expect(result).toBe(inputBuffer);
    });
    
    test('should throw error for null or undefined value', () => {
      // @ts-ignore - Testing runtime behavior with invalid input
      expect(() => validateValue(null)).toThrow('Value cannot be null or undefined');
      // @ts-ignore - Testing runtime behavior with invalid input
      expect(() => validateValue(undefined)).toThrow('Value cannot be null or undefined');
    });
    
    test('should throw error for value larger than 10MB', () => {
      // Create a Buffer slightly larger than 10MB
      const largeValue = Buffer.alloc(10 * 1024 * 1024 + 1);
      expect(() => validateValue(largeValue)).toThrow('Value cannot be larger than 10MB');
    });
  });
  
  describe('sleep', () => {
    test('should resolve after specified time', async () => {
      const start = Date.now();
      await sleep(50); // 50ms
      const elapsed = Date.now() - start;
      
      // Allow for some timing imprecision
      expect(elapsed).toBeGreaterThanOrEqual(40);
    });
  });
  
  describe('generateTransactionId', () => {
    test('should generate a string', () => {
      const id = generateTransactionId();
      expect(typeof id).toBe('string');
    });
    
    test('should generate unique IDs', () => {
      const id1 = generateTransactionId();
      const id2 = generateTransactionId();
      expect(id1).not.toBe(id2);
    });
    
    test('should include tx- prefix', () => {
      const id = generateTransactionId();
      expect(id.startsWith('tx-')).toBe(true);
    });
  });
});