import { sanitizeFilename } from '../utils';

describe('utils', () => {
  describe('sanitizeFilename', () => {
    it('should keep valid filename characters', () => {
      const result = sanitizeFilename('my-file_name.sql');
      expect(result).toBe('my-file_name.sql');
    });

    it('should replace forward slashes with underscores', () => {
      const result = sanitizeFilename('path/to/file.sql');
      expect(result).toBe('path_to_file.sql');
    });

    it('should replace backslashes with underscores', () => {
      const result = sanitizeFilename('path\\to\\file.sql');
      expect(result).toBe('path_to_file.sql');
    });

    it('should replace colons with underscores (Windows drive letters)', () => {
      const result = sanitizeFilename('C:\\file.sql');
      expect(result).toBe('C__file.sql');
    });

    it('should replace question marks with underscores', () => {
      const result = sanitizeFilename('file?.sql');
      expect(result).toBe('file_.sql');
    });

    it('should replace asterisks with underscores', () => {
      const result = sanitizeFilename('file*.sql');
      expect(result).toBe('file_.sql');
    });

    it('should replace pipes with underscores', () => {
      const result = sanitizeFilename('file|name.sql');
      expect(result).toBe('file_name.sql');
    });

    it('should replace angle brackets with underscores', () => {
      const result = sanitizeFilename('file<name>.sql');
      expect(result).toBe('file_name_.sql');
    });

    it('should replace double quotes with underscores', () => {
      const result = sanitizeFilename('file"name.sql');
      expect(result).toBe('file_name.sql');
    });

    it('should replace percent signs with underscores', () => {
      const result = sanitizeFilename('file%name.sql');
      expect(result).toBe('file_name.sql');
    });

    it('should handle all Windows invalid characters', () => {
      const result = sanitizeFilename('file<>:"/\\|?*.sql');
      expect(result).toBe('file_________.sql');
    });

    it('should prevent directory traversal attacks', () => {
      const result = sanitizeFilename('../../../etc/passwd');
      expect(result).toBe('.._.._.._etc_passwd');
    });

    it('should handle empty string', () => {
      const result = sanitizeFilename('');
      expect(result).toBe('');
    });

    it('should preserve file extensions', () => {
      const result = sanitizeFilename('my:file.sql');
      expect(result).toBe('my_file.sql');
    });

    it('should handle multiple consecutive invalid characters', () => {
      const result = sanitizeFilename('file///name.sql');
      expect(result).toBe('file___name.sql');
    });

    it('should preserve unicode characters that are valid in filenames', () => {
      const result = sanitizeFilename('file_名前.sql');
      expect(result).toBe('file_名前.sql');
    });
  });
});
