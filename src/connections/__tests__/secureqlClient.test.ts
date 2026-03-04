import { normalizeBaseUrl, mapSecureQLError, SecureQLApiError } from '../adapters/secureqlClient';

describe('normalizeBaseUrl', () => {
    it('strips trailing slashes', () => {
        expect(normalizeBaseUrl('https://api.example.com/')).toBe('https://api.example.com');
        expect(normalizeBaseUrl('https://api.example.com///')).toBe('https://api.example.com');
    });

    it('prepends https:// for non-localhost', () => {
        expect(normalizeBaseUrl('api.example.com')).toBe('https://api.example.com');
        expect(normalizeBaseUrl('api.example.com:8443')).toBe('https://api.example.com:8443');
    });

    it('prepends http:// for localhost', () => {
        expect(normalizeBaseUrl('localhost:3000')).toBe('http://localhost:3000');
        expect(normalizeBaseUrl('127.0.0.1:3000')).toBe('http://127.0.0.1:3000');
    });

    it('preserves existing protocol', () => {
        expect(normalizeBaseUrl('http://localhost:3000')).toBe('http://localhost:3000');
        expect(normalizeBaseUrl('https://api.example.com')).toBe('https://api.example.com');
    });

    it('trims whitespace', () => {
        expect(normalizeBaseUrl('  https://api.example.com  ')).toBe('https://api.example.com');
    });
});

describe('mapSecureQLError', () => {
    it('maps 401 to invalid API key message', () => {
        const err = mapSecureQLError(401, { message: 'Unauthorized' }, 'key123');
        expect(err).toBeInstanceOf(SecureQLApiError);
        expect(err.statusCode).toBe(401);
        expect(err.userMessage).toContain('invalid or missing');
    });

    it('maps 403 with "User account is disabled"', () => {
        const err = mapSecureQLError(403, { message: 'User account is disabled' }, 'key123');
        expect(err.statusCode).toBe(403);
        expect(err.userMessage).toContain('user account is disabled');
    });

    it('maps 403 with "Membership is disabled"', () => {
        const err = mapSecureQLError(403, { message: 'Membership is disabled' }, 'key123');
        expect(err.statusCode).toBe(403);
        expect(err.userMessage).toContain('disabled by admin');
    });

    it('maps 403 with "Connection is disabled"', () => {
        const err = mapSecureQLError(403, { message: 'Connection is disabled' }, 'key123');
        expect(err.statusCode).toBe(403);
        expect(err.userMessage).toContain('disabled by admin');
    });

    it('maps 403 with "not authorized for this connection"', () => {
        const err = mapSecureQLError(403, { message: 'not authorized for this connection' }, 'key123');
        expect(err.statusCode).toBe(403);
        expect(err.userMessage).toContain('not authorized');
    });

    it('maps 403 generic', () => {
        const err = mapSecureQLError(403, { message: 'Some other 403' }, 'key123');
        expect(err.statusCode).toBe(403);
        expect(err.userMessage).toContain('Access denied');
    });

    it('maps 404', () => {
        const err = mapSecureQLError(404, { message: 'Not found' }, 'key123');
        expect(err.statusCode).toBe(404);
        expect(err.userMessage).toContain('not found');
    });

    it('redacts API key from error messages', () => {
        const err = mapSecureQLError(500, { message: 'Error with key123 exposed' }, 'key123');
        expect(err.userMessage).not.toContain('key123');
        expect(err.userMessage).toContain('[REDACTED]');
    });

    it('handles string body', () => {
        const err = mapSecureQLError(500, 'Internal Server Error', 'key123');
        expect(err.statusCode).toBe(500);
        expect(err.userMessage).toContain('Internal Server Error');
    });
});

describe('SecureQLApiError', () => {
    it('is an instance of Error', () => {
        const err = new SecureQLApiError(401, 'test message');
        expect(err).toBeInstanceOf(Error);
        expect(err.name).toBe('SecureQLApiError');
        expect(err.statusCode).toBe(401);
        expect(err.userMessage).toBe('test message');
        expect(err.message).toBe('test message');
    });
});
