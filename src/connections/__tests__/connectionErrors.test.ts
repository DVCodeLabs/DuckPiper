import { mapDatabaseError, formatDatabaseConnectionError } from '../connectionErrors';

describe('connectionErrors', () => {
  describe('mapDatabaseError', () => {
    it('should map ECONNREFUSED error', () => {
      const error = { code: 'ECONNREFUSED', message: 'Connection refused' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Connection refused');
      expect(result).toContain('database server running');
    });

    it('should map ETIMEDOUT error', () => {
      const error = { code: 'ETIMEDOUT', message: 'Timeout' };
      const result = mapDatabaseError(error);

      expect(result).toContain('timed out');
      expect(result).toContain('network connectivity');
    });

    it('should map ENOTFOUND error', () => {
      const error = { code: 'ENOTFOUND', message: 'Host not found' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Host not found');
      expect(result).toContain('hostname is correct');
    });

    it('should map ECONNRESET error', () => {
      const error = { code: 'ECONNRESET', message: 'Connection reset' };
      const result = mapDatabaseError(error);

      expect(result).toContain('reset by the server');
      expect(result).toContain('network stability');
    });

    it('should map EHOSTUNREACH error', () => {
      const error = { code: 'EHOSTUNREACH', message: 'Host unreachable' };
      const result = mapDatabaseError(error);

      expect(result).toContain('unreachable');
      expect(result).toContain('network connectivity');
    });

    it('should map PostgreSQL invalid password error (28P01)', () => {
      const error = { code: '28P01', message: 'password authentication failed' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Authentication failed');
      expect(result).toContain('username and password');
    });

    it('should map PostgreSQL invalid authorization error (28000)', () => {
      const error = { code: '28000', message: 'authorization failed' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Authorization failed');
      expect(result).toContain('permissions');
    });

    it('should map PostgreSQL invalid database error (3D000)', () => {
      const error = { code: '3D000', message: 'database does not exist' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Database does not exist');
      expect(result).toContain('database name');
    });

    it('should map EACCES error', () => {
      const error = { code: 'EACCES', message: 'Permission denied' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Permission denied');
      expect(result).toContain('permissions');
    });

    it('should map EADDRINUSE error', () => {
      const error = { code: 'EADDRINUSE', message: 'Address in use' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Address already in use');
      expect(result).toContain('port');
    });

    it('should return generic message for unknown error codes', () => {
      const error = { code: 'UNKNOWN', message: 'Something went wrong' };
      const result = mapDatabaseError(error);

      expect(result).toBe('Something went wrong');
    });

    it('should handle errors without code property', () => {
      const error = { message: 'Connection failed' };
      const result = mapDatabaseError(error);

      expect(result).toBe('Connection failed');
    });

    it('should handle null error', () => {
      const result = mapDatabaseError(null);

      expect(result).toBe('Connection test failed');
    });

    it('should handle undefined error', () => {
      const result = mapDatabaseError(undefined);

      expect(result).toBe('Connection test failed');
    });

    it('should handle errors without message', () => {
      const error = { code: 'UNKNOWN' };
      const result = mapDatabaseError(error);

      expect(result).toContain('Connection test failed');
    });

    it('should handle AggregateError with nested errors', () => {
      const aggregateError = {
        name: 'AggregateError',
        errors: [
          { code: 'ECONNREFUSED', message: 'Connection refused' },
          { code: 'ETIMEDOUT', message: 'Timeout' }
        ]
      };
      const result = mapDatabaseError(aggregateError);

      // Should use the first error in the array
      expect(result).toContain('Connection refused');
    });

    it('should handle AggregateError with constructor name', () => {
      const aggregateError = {
        constructor: { name: 'AggregateError' },
        errors: [
          { code: 'ETIMEDOUT', message: 'Timeout' }
        ]
      };
      const result = mapDatabaseError(aggregateError);

      expect(result).toContain('timed out');
    });

    it('should handle empty AggregateError', () => {
      const aggregateError = {
        name: 'AggregateError',
        errors: []
      };
      const result = mapDatabaseError(aggregateError);

      expect(result).toContain('Connection test failed');
    });
  });

  describe('formatDatabaseConnectionError', () => {
    it('should format database connection errors', () => {
      const error = { code: 'ECONNREFUSED', message: 'Connection refused' };
      const result = formatDatabaseConnectionError(error);

      expect(result).toBeTruthy();
      expect(result).toContain('Connection refused');
    });

    it('should handle various error types', () => {
      const errors = [
        { code: 'ECONNREFUSED', message: 'Connection refused' },
        { code: 'ETIMEDOUT', message: 'Timeout' },
        { code: '28P01', message: 'Authentication failed' }
      ];

      errors.forEach(error => {
        const result = formatDatabaseConnectionError(error);
        expect(result).toBeTruthy();
        expect(result.length).toBeGreaterThan(0);
      });
    });
  });
});
