// utils/fileHelpers.js

/**
 * Comprehensive file utilities for handling various file operations
 */

export class FileHelpers {
  // Supported file types for different operations
  static FILE_TYPES = {
    EXCEL: [
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ],
    CSV: ['text/csv'],
    IMAGES: [
      'image/jpeg',
      'image/png', 
      'image/gif',
      'image/webp',
      'image/bmp',
      'image/svg+xml'
    ],
    DOCUMENTS: [
      'application/pdf',
      'application/msword',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      'text/plain',
      'text/rtf'
    ],
    PRESENTATIONS: [
      'application/vnd.ms-powerpoint',
      'application/vnd.openxmlformats-officedocument.presentationml.presentation'
    ]
  };

  // File size limits (in bytes)
  static SIZE_LIMITS = {
    IMAGE: 5 * 1024 * 1024,      // 5MB
    DOCUMENT: 10 * 1024 * 1024,  // 10MB
    EXCEL: 20 * 1024 * 1024,     // 20MB
    DEFAULT: 25 * 1024 * 1024    // 25MB
  };

  /**
   * Check if file type is supported
   */
  static isFileTypeSupported(file, allowedTypes = []) {
    if (!file || !file.type) return false;
    
    if (allowedTypes.length === 0) {
      // If no specific types provided, check against all known types
      const allTypes = Object.values(this.FILE_TYPES).flat();
      return allTypes.includes(file.type);
    }
    
    return allowedTypes.includes(file.type);
  }

  /**
   * Get file category based on MIME type
   */
  static getFileCategory(file) {
    if (!file || !file.type) return 'unknown';
    
    const mimeType = file.type.toLowerCase();
    
    if (this.FILE_TYPES.EXCEL.includes(mimeType)) return 'excel';
    if (this.FILE_TYPES.CSV.includes(mimeType)) return 'csv';
    if (this.FILE_TYPES.IMAGES.includes(mimeType)) return 'image';
    if (this.FILE_TYPES.DOCUMENTS.includes(mimeType)) return 'document';
    if (this.FILE_TYPES.PRESENTATIONS.includes(mimeType)) return 'presentation';
    
    return 'unknown';
  }

  /**
   * Get file extension from filename
   */
  static getFileExtension(filename) {
    if (!filename || typeof filename !== 'string') return '';
    
    const lastDotIndex = filename.lastIndexOf('.');
    if (lastDotIndex === -1 || lastDotIndex === filename.length - 1) return '';
    
    return filename.substring(lastDotIndex + 1).toLowerCase();
  }

  /**
   * Validate file size against limits
   */
  static validateFileSize(file, customLimit = null) {
    if (!file) return { isValid: false, error: 'No file provided' };
    
    const category = this.getFileCategory(file);
    const sizeLimit = customLimit || this.SIZE_LIMITS[category.toUpperCase()] || this.SIZE_LIMITS.DEFAULT;
    
    if (file.size > sizeLimit) {
      const limitMB = (sizeLimit / (1024 * 1024)).toFixed(1);
      const fileSizeMB = (file.size / (1024 * 1024)).toFixed(1);
      return {
        isValid: false,
        error: `File size (${fileSizeMB}MB) exceeds limit of ${limitMB}MB`,
        fileSize: file.size,
        sizeLimit: sizeLimit
      };
    }
    
    return { isValid: true, fileSize: file.size, sizeLimit: sizeLimit };
  }

  /**
   * Comprehensive file validation
   */
  static validateFile(file, options = {}) {
    const {
      allowedTypes = [],
      maxSize = null,
      allowedExtensions = [],
      requireExtension = false
    } = options;

    const result = {
      isValid: true,
      errors: [],
      warnings: [],
      metadata: {
        name: file?.name || '',
        size: file?.size || 0,
        type: file?.type || '',
        category: this.getFileCategory(file),
        extension: this.getFileExtension(file?.name || ''),
        lastModified: file?.lastModified || null
      }
    };

    // Basic file check
    if (!file) {
      result.isValid = false;
      result.errors.push('No file provided');
      return result;
    }

    // File type validation
    if (allowedTypes.length > 0 && !this.isFileTypeSupported(file, allowedTypes)) {
      result.isValid = false;
      const allowedTypeNames = allowedTypes.map(type => type.split('/')[1]).join(', ');
      result.errors.push(`File type not allowed. Allowed types: ${allowedTypeNames}`);
    }

    // Extension validation
    if (allowedExtensions.length > 0) {
      const extension = result.metadata.extension;
      if (!allowedExtensions.includes(extension)) {
        result.isValid = false;
        result.errors.push(`File extension not allowed. Allowed extensions: ${allowedExtensions.join(', ')}`);
      }
    }

    // Extension requirement
    if (requireExtension && !result.metadata.extension) {
      result.isValid = false;
      result.errors.push('File must have an extension');
    }

    // Size validation
    const sizeValidation = this.validateFileSize(file, maxSize);
    if (!sizeValidation.isValid) {
      result.isValid = false;
      result.errors.push(sizeValidation.error);
    }

    // Additional warnings
    if (file.size === 0) {
      result.warnings.push('File appears to be empty');
    }

    if (file.name && file.name.length > 255) {
      result.warnings.push('Filename is very long');
    }

    // Check for suspicious file names
    const suspiciousPatterns = ['.exe', '.bat', '.cmd', '.scr', '.vbs', '.js'];
    if (suspiciousPatterns.some(pattern => file.name?.toLowerCase().includes(pattern))) {
      result.warnings.push('File type may pose security risk');
    }

    return result;
  }

  /**
   * Format file size for display
   */
  static formatFileSize(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  }

  /**
   * Read file as text
   */
  static readFileAsText(file) {
    return new Promise((resolve, reject) => {
      if (!file) {
        reject(new Error('No file provided'));
        return;
      }

      const reader = new FileReader();
      
      reader.onload = (event) => {
        resolve(event.target.result);
      };
      
      reader.onerror = (error) => {
        reject(new Error(`Failed to read file: ${error.message}`));
      };
      
      reader.readAsText(file);
    });
  }

  /**
   * Read file as data URL (for images)
   */
  static readFileAsDataURL(file) {
    return new Promise((resolve, reject) => {
      if (!file) {
        reject(new Error('No file provided'));
        return;
      }

      const reader = new FileReader();
      
      reader.onload = (event) => {
        resolve(event.target.result);
      };
      
      reader.onerror = (error) => {
        reject(new Error(`Failed to read file: ${error.message}`));
      };
      
      reader.readAsDataURL(file);
    });
  }

  /**
   * Read file as array buffer
   */
  static readFileAsArrayBuffer(file) {
    return new Promise((resolve, reject) => {
      if (!file) {
        reject(new Error('No file provided'));
        return;
      }

      const reader = new FileReader();
      
      reader.onload = (event) => {
        resolve(event.target.result);
      };
      
      reader.onerror = (error) => {
        reject(new Error(`Failed to read file: ${error.message}`));
      };
      
      reader.readAsArrayBuffer(file);
    });
  }

  /**
   * Create a blob from content
   */
  static createBlob(content, mimeType = 'text/plain') {
    return new Blob([content], { type: mimeType });
  }

  /**
   * Download file/blob
   */
  static downloadFile(blob, filename) {
    try {
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      
      // Append to body, click, and remove
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      // Clean up the URL
      URL.revokeObjectURL(url);
      
      return true;
    } catch (error) {
      console.error('Download failed:', error);
      return false;
    }
  }

  /**
   * Convert file to different format (basic conversions)
   */
  static async convertFile(file, targetFormat) {
    const sourceCategory = this.getFileCategory(file);
    
    // CSV to JSON conversion
    if (sourceCategory === 'csv' && targetFormat === 'json') {
      const text = await this.readFileAsText(file);
      const lines = text.split('\n').filter(line => line.trim());
      
      if (lines.length === 0) {
        throw new Error('Empty CSV file');
      }
      
      const headers = lines[0].split(',').map(h => h.trim());
      const data = lines.slice(1).map(line => {
        const values = line.split(',').map(v => v.trim());
        const obj = {};
        headers.forEach((header, index) => {
          obj[header] = values[index] || '';
        });
        return obj;
      });
      
      return this.createBlob(JSON.stringify(data, null, 2), 'application/json');
    }
    
    // JSON to CSV conversion
    if (sourceCategory === 'document' && file.type === 'application/json' && targetFormat === 'csv') {
      const text = await this.readFileAsText(file);
      const data = JSON.parse(text);
      
      if (!Array.isArray(data) || data.length === 0) {
        throw new Error('JSON must be an array of objects');
      }
      
      const headers = Object.keys(data[0]);
      const csvContent = [
        headers.join(','),
        ...data.map(row => headers.map(header => row[header] || '').join(','))
      ].join('\n');
      
      return this.createBlob(csvContent, 'text/csv');
    }
    
    throw new Error(`Conversion from ${sourceCategory} to ${targetFormat} not supported`);
  }

  /**
   * Compress image file
   */
  static compressImage(file, options = {}) {
    return new Promise((resolve, reject) => {
      const {
        maxWidth = 1920,
        maxHeight = 1080,
        quality = 0.8,
        outputFormat = 'image/jpeg'
      } = options;

      if (!this.FILE_TYPES.IMAGES.includes(file.type)) {
        reject(new Error('File is not an image'));
        return;
      }

      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d');
      const img = new Image();

      img.onload = () => {
        // Calculate new dimensions
        let { width, height } = img;
        
        if (width > maxWidth) {
          height = (height * maxWidth) / width;
          width = maxWidth;
        }
        
        if (height > maxHeight) {
          width = (width * maxHeight) / height;
          height = maxHeight;
        }

        // Set canvas dimensions
        canvas.width = width;
        canvas.height = height;

        // Draw and compress
        ctx.drawImage(img, 0, 0, width, height);
        
        canvas.toBlob(
          (blob) => {
            if (blob) {
              resolve(blob);
            } else {
              reject(new Error('Image compression failed'));
            }
          },
          outputFormat,
          quality
        );
      };

      img.onerror = () => {
        reject(new Error('Failed to load image'));
      };

      img.src = URL.createObjectURL(file);
    });
  }

  /**
   * Extract metadata from file
   */
  static async extractFileMetadata(file) {
    const basic = {
      name: file.name,
      size: file.size,
      type: file.type,
      lastModified: new Date(file.lastModified),
      category: this.getFileCategory(file),
      extension: this.getFileExtension(file.name)
    };

    // For images, try to extract dimensions
    if (this.FILE_TYPES.IMAGES.includes(file.type)) {
      try {
        const dimensions = await this.getImageDimensions(file);
        basic.width = dimensions.width;
        basic.height = dimensions.height;
        basic.aspectRatio = (dimensions.width / dimensions.height).toFixed(2);
      } catch (error) {
        console.warn('Could not extract image dimensions:', error);
      }
    }

    return basic;
  }

  /**
   * Get image dimensions
   */
  static getImageDimensions(file) {
    return new Promise((resolve, reject) => {
      if (!this.FILE_TYPES.IMAGES.includes(file.type)) {
        reject(new Error('File is not an image'));
        return;
      }

      const img = new Image();
      
      img.onload = () => {
        resolve({
          width: img.naturalWidth,
          height: img.naturalHeight
        });
      };
      
      img.onerror = () => {
        reject(new Error('Failed to load image'));
      };
      
      img.src = URL.createObjectURL(file);
    });
  }

  /**
   * Generate file hash (simple hash for client-side use)
   */
  static async generateFileHash(file) {
    try {
      const arrayBuffer = await this.readFileAsArrayBuffer(file);
      const hashBuffer = await crypto.subtle.digest('SHA-256', arrayBuffer);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
      return hashHex;
    } catch (error) {
      // Fallback to simple hash based on file properties
      const str = `${file.name}-${file.size}-${file.lastModified}`;
      let hash = 0;
      for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
      }
      return Math.abs(hash).toString(16);
    }
  }

  /**
   * Check if files are duplicates
   */
  static async areFilesDuplicate(file1, file2) {
    // Quick checks first
    if (file1.name === file2.name && 
        file1.size === file2.size && 
        file1.lastModified === file2.lastModified) {
      return true;
    }

    // Size and type check
    if (file1.size !== file2.size || file1.type !== file2.type) {
      return false;
    }

    // Content hash comparison for more accuracy
    try {
      const hash1 = await this.generateFileHash(file1);
      const hash2 = await this.generateFileHash(file2);
      return hash1 === hash2;
    } catch (error) {
      console.warn('Could not generate file hashes:', error);
      return false;
    }
  }

  /**
   * Batch file operations
   */
  static async processFileBatch(files, processor) {
    const results = [];
    const errors = [];

    for (let i = 0; i < files.length; i++) {
      try {
        const result = await processor(files[i], i);
        results.push(result);
      } catch (error) {
        errors.push({
          file: files[i],
          index: i,
          error: error.message
        });
      }
    }

    return { results, errors };
  }

  /**
   * File preview URL generation
   */
  static createPreviewURL(file) {
    const category = this.getFileCategory(file);
    
    if (category === 'image') {
      return URL.createObjectURL(file);
    }
    
    // For other file types, return null
    // In a real app, you might have server-side preview generation
    return null;
  }

  /**
   * Clean up object URLs
   */
  static revokeObjectURL(url) {
    if (url && typeof url === 'string' && url.startsWith('blob:')) {
      URL.revokeObjectURL(url);
    }
  }
}

// Export individual functions for convenience
export const {
  isFileTypeSupported,
  getFileCategory,
  getFileExtension,
  validateFile,
  formatFileSize,
  readFileAsText,
  readFileAsDataURL,
  readFileAsArrayBuffer,
  downloadFile,
  compressImage,
  extractFileMetadata,
  generateFileHash
} = FileHelpers;

export default FileHelpers;