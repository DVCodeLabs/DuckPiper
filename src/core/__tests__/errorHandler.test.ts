import { ErrorHandler, ErrorSeverity } from '../errorHandler';

// Mock vscode
jest.mock('vscode');
jest.mock('../logger');

describe('ErrorHandler', () => {
  describe('extractErrorMessage', () => {
    it('should extract message from Error instance', () => {
      const error = new Error('Test error');
      const message = ErrorHandler.extractErrorMessage(error);

      expect(message).toBe('Test error');
    });

    it('should handle string errors', () => {
      const error = 'String error message';
      const message = ErrorHandler.extractErrorMessage(error);

      expect(message).toBe('String error message');
    });

    it('should extract message from error-like objects', () => {
      const error = { message: 'Object error message' };
      const message = ErrorHandler.extractErrorMessage(error);

      expect(message).toBe('Object error message');
    });

    it('should handle nested error objects', () => {
      const error = { error: { message: 'Nested error' } };
      const message = ErrorHandler.extractErrorMessage(error);

      expect(message).toBe('Nested error');
    });

    it('should use toString for objects with custom toString', () => {
      const error = {
        toString: () => 'Custom toString'
      };
      const message = ErrorHandler.extractErrorMessage(error);

      expect(message).toBe('Custom toString');
    });

    it('should handle unknown error types', () => {
      const message = ErrorHandler.extractErrorMessage(undefined);

      expect(message).toBe('An unknown error occurred');
    });

    it('should handle null errors', () => {
      const message = ErrorHandler.extractErrorMessage(null);

      expect(message).toBe('An unknown error occurred');
    });

    it('should handle objects without message property', () => {
      const error = { code: 'ERROR_CODE' };
      const message = ErrorHandler.extractErrorMessage(error);

      expect(message).toBe('An unknown error occurred');
    });
  });

  describe('wrap', () => {
    it('should wrap function and catch errors', async () => {
      const fn = jest.fn().mockRejectedValue(new Error('Test error'));
      const wrapped = ErrorHandler.wrap(fn, {
        severity: ErrorSeverity.Silent,
        context: 'test'
      });

      const result = await wrapped('arg1', 'arg2');

      expect(result).toBeUndefined();
      expect(fn).toHaveBeenCalledWith('arg1', 'arg2');
    });

    it('should return function result on success', async () => {
      const fn = jest.fn().mockResolvedValue('success');
      const wrapped = ErrorHandler.wrap(fn, {
        severity: ErrorSeverity.Silent
      });

      const result = await wrapped();

      expect(result).toBe('success');
    });

    it('should preserve function arguments', async () => {
      const fn = jest.fn().mockResolvedValue(true);
      const wrapped = ErrorHandler.wrap(fn, {
        severity: ErrorSeverity.Silent
      });

      await wrapped(1, 'test', { key: 'value' });

      expect(fn).toHaveBeenCalledWith(1, 'test', { key: 'value' });
    });
  });

  describe('handleWithRetry', () => {
    it('should retry operation on failure', async () => {
      let attempts = 0;
      const operation = jest.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 3) {
          throw new Error('Retry me');
        }
        return 'success';
      });

      const result = await ErrorHandler.handleWithRetry(operation, {
        severity: ErrorSeverity.Silent,
        maxRetries: 3,
        retryDelay: 10
      });

      expect(result).toBe('success');
      expect(operation).toHaveBeenCalledTimes(3);
    });

    it('should return undefined after all retries fail', async () => {
      const operation = jest.fn().mockRejectedValue(new Error('Always fails'));

      const result = await ErrorHandler.handleWithRetry(operation, {
        severity: ErrorSeverity.Silent,
        maxRetries: 2,
        retryDelay: 10
      });

      expect(result).toBeUndefined();
      expect(operation).toHaveBeenCalledTimes(2);
    });

    it('should use default retry values', async () => {
      const operation = jest.fn().mockResolvedValue('success');

      const result = await ErrorHandler.handleWithRetry(operation, {
        severity: ErrorSeverity.Silent
      });

      expect(result).toBe('success');
    });

    it('should succeed on first try', async () => {
      const operation = jest.fn().mockResolvedValue('success');

      const result = await ErrorHandler.handleWithRetry(operation, {
        severity: ErrorSeverity.Silent,
        maxRetries: 3
      });

      expect(result).toBe('success');
      expect(operation).toHaveBeenCalledTimes(1);
    });
  });
});
