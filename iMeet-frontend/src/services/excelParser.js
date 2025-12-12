// services/excelParser.js
import * as XLSX from 'xlsx';

class ExcelParserService {
  constructor() {
    this.supportedFormats = [
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'text/csv'
    ];
  }

  /**
   * Check if file is supported
   */
  isSupported(file) {
    return this.supportedFormats.includes(file.type) || 
           file.name.toLowerCase().endsWith('.csv') ||
           file.name.toLowerCase().endsWith('.xls') ||
           file.name.toLowerCase().endsWith('.xlsx');
  }

  /**
   * Parse Excel/CSV file and extract emails
   */
  async parseFile(file) {
    if (!this.isSupported(file)) {
      throw new Error('Unsupported file format');
    }

    try {
      const arrayBuffer = await file.arrayBuffer();
      let result = {
        fileName: file.name,
        fileSize: file.size,
        sheets: [],
        emails: [],
        metadata: {
          totalCells: 0,
          totalRows: 0,
          emailsFound: 0,
          processingTime: 0
        }
      };

      const startTime = Date.now();

      if (file.name.toLowerCase().endsWith('.csv')) {
        result = await this.parseCSV(arrayBuffer, result);
      } else {
        result = await this.parseExcel(arrayBuffer, result);
      }

      result.metadata.processingTime = Date.now() - startTime;
      return result;

    } catch (error) {
      console.error('Excel parsing error:', error);
      throw new Error(`Failed to parse file: ${error.message}`);
    }
  }

  /**
   * Parse CSV file
   */
  async parseCSV(arrayBuffer, result) {
    const text = new TextDecoder().decode(arrayBuffer);
    const lines = text.split('\n').filter(line => line.trim());
    
    const csvSheet = {
      name: 'CSV',
      rowCount: lines.length,
      columnCount: 0,
      emails: [],
      preview: []
    };

    let maxColumns = 0;

    lines.forEach((line, rowIndex) => {
      const cells = this.parseCSVLine(line);
      maxColumns = Math.max(maxColumns, cells.length);
      
      // Store preview (first 5 rows)
      if (rowIndex < 5) {
        csvSheet.preview.push(cells);
      }

      // Extract emails from each cell
      cells.forEach((cell, colIndex) => {
        const emails = this.extractEmailsFromText(cell);
        emails.forEach(email => {
          const emailData = {
            email: email,
            source: `Row ${rowIndex + 1}, Column ${colIndex + 1}`,
            sheet: 'CSV',
            rowIndex: rowIndex,
            columnIndex: colIndex,
            cellValue: cell
          };
          csvSheet.emails.push(emailData);
          result.emails.push(emailData);
        });
      });
    });

    csvSheet.columnCount = maxColumns;
    result.sheets.push(csvSheet);
    result.metadata.totalRows = lines.length;
    result.metadata.totalCells = lines.length * maxColumns;
    result.metadata.emailsFound = result.emails.length;

    return result;
  }

  /**
   * Parse Excel file
   */
  async parseExcel(arrayBuffer, result) {
    const workbook = XLSX.read(arrayBuffer, {
      cellStyles: true,
      cellFormulas: true,
      cellDates: true,
      cellNF: true,
      sheetStubs: true
    });

    let totalCells = 0;
    let totalRows = 0;

    for (const sheetName of workbook.SheetNames) {
      const worksheet = workbook.Sheets[sheetName];
      const sheetData = this.processWorksheet(worksheet, sheetName);
      
      result.sheets.push(sheetData);
      result.emails.push(...sheetData.emails);
      
      totalCells += sheetData.rowCount * sheetData.columnCount;
      totalRows += sheetData.rowCount;
    }

    result.metadata.totalCells = totalCells;
    result.metadata.totalRows = totalRows;
    result.metadata.emailsFound = result.emails.length;

    return result;
  }

  /**
   * Process individual worksheet
   */
  processWorksheet(worksheet, sheetName) {
    const jsonData = XLSX.utils.sheet_to_json(worksheet, { 
      header: 1, 
      defval: '',
      raw: false 
    });

    const sheetResult = {
      name: sheetName,
      rowCount: jsonData.length,
      columnCount: 0,
      emails: [],
      preview: [],
      metadata: {
        range: worksheet['!ref'],
        hasHyperlinks: false,
        hasFormulas: false
      }
    };

    // Get maximum column count
    const maxColumns = Math.max(...jsonData.map(row => Array.isArray(row) ? row.length : 0));
    sheetResult.columnCount = maxColumns;

    // Store preview (first 10 rows)
    sheetResult.preview = jsonData.slice(0, 10);

    // Extract emails from each cell
    jsonData.forEach((row, rowIndex) => {
      if (Array.isArray(row)) {
        row.forEach((cell, colIndex) => {
          if (cell && typeof cell === 'string') {
            const emails = this.extractEmailsFromText(cell);
            emails.forEach(email => {
              const emailData = {
                email: email,
                source: `${sheetName} - Row ${rowIndex + 1}, Col ${this.columnIndexToLetter(colIndex)}`,
                sheet: sheetName,
                rowIndex: rowIndex,
                columnIndex: colIndex,
                cellValue: cell
              };
              sheetResult.emails.push(emailData);
            });
          }
        });
      }
    });

    // Check for hyperlinks and formulas in the original sheet
    this.analyzeSheetMetadata(worksheet, sheetResult);

    return sheetResult;
  }

  /**
   * Analyze sheet for additional metadata
   */
  analyzeSheetMetadata(worksheet, sheetResult) {
    for (const cellRef in worksheet) {
      if (cellRef.startsWith('!')) continue; // Skip metadata cells
      
      const cell = worksheet[cellRef];
      
      // Check for hyperlinks
      if (cell.l) {
        sheetResult.metadata.hasHyperlinks = true;
      }
      
      // Check for formulas
      if (cell.f) {
        sheetResult.metadata.hasFormulas = true;
      }
    }
  }

  /**
   * Extract emails from text using multiple patterns
   */
  extractEmailsFromText(text) {
    if (!text || typeof text !== 'string') return [];

    const emailPatterns = [
      // Standard email pattern
      /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g,
      // More permissive pattern
      /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g,
      // Pattern for emails with spaces (common in poorly formatted data)
      /[a-zA-Z0-9._%+-]+\s*@\s*[a-zA-Z0-9.-]+\s*\.\s*[a-zA-Z]{2,}/g
    ];

    const foundEmails = new Set();

    emailPatterns.forEach(pattern => {
      const matches = text.match(pattern);
      if (matches) {
        matches.forEach(email => {
          // Clean up the email (remove spaces, normalize)
          const cleanEmail = email.replace(/\s+/g, '').toLowerCase().trim();
          if (this.isValidEmail(cleanEmail)) {
            foundEmails.add(cleanEmail);
          }
        });
      }
    });

    return Array.from(foundEmails);
  }

  /**
   * Validate email format
   */
  isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  /**
   * Parse CSV line handling quoted values and commas
   */
  parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    let quoteChar = null;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if ((char === '"' || char === "'") && !inQuotes) {
        // Start of quoted string
        inQuotes = true;
        quoteChar = char;
      } else if (char === quoteChar && inQuotes) {
        // End of quoted string or escaped quote
        if (nextChar === quoteChar) {
          // Escaped quote
          current += char;
          i++; // Skip next character
        } else {
          // End of quoted string
          inQuotes = false;
          quoteChar = null;
        }
      } else if (char === ',' && !inQuotes) {
        // Field separator
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }

    // Add the last field
    result.push(current.trim());

    return result;
  }

  /**
   * Convert column index to letter (0 = A, 1 = B, etc.)
   */
  columnIndexToLetter(index) {
    let result = '';
    while (index >= 0) {
      result = String.fromCharCode(65 + (index % 26)) + result;
      index = Math.floor(index / 26) - 1;
    }
    return result;
  }

  /**
   * Validate and clean email list
   */
  validateAndCleanEmails(emails) {
    const results = {
      valid: [],
      invalid: [],
      duplicates: [],
      cleaned: []
    };

    const emailMap = new Map();

    emails.forEach(emailData => {
      const email = emailData.email.toLowerCase().trim();
      
      if (!this.isValidEmail(email)) {
        results.invalid.push({
          ...emailData,
          reason: 'Invalid format'
        });
        return;
      }

      if (emailMap.has(email)) {
        results.duplicates.push({
          ...emailData,
          originalSource: emailMap.get(email).source
        });
        return;
      }

      emailMap.set(email, emailData);
      results.valid.push(emailData);
    });

    results.cleaned = Array.from(emailMap.values());
    return results;
  }

  /**
   * Generate summary report
   */
  generateSummary(parseResult) {
    const validationResult = this.validateAndCleanEmails(parseResult.emails);
    
    return {
      file: {
        name: parseResult.fileName,
        size: this.formatFileSize(parseResult.fileSize),
        processingTime: `${parseResult.metadata.processingTime}ms`
      },
      structure: {
        sheets: parseResult.sheets.length,
        totalRows: parseResult.metadata.totalRows,
        totalCells: parseResult.metadata.totalCells
      },
      emails: {
        total: parseResult.emails.length,
        valid: validationResult.valid.length,
        invalid: validationResult.invalid.length,
        duplicates: validationResult.duplicates.length,
        unique: validationResult.cleaned.length
      },
      sheetBreakdown: parseResult.sheets.map(sheet => ({
        name: sheet.name,
        rows: sheet.rowCount,
        columns: sheet.columnCount,
        emails: sheet.emails.length
      }))
    };
  }

  /**
   * Format file size for display
   */
  formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Export validated emails to different formats
   */
  exportEmails(emails, format = 'csv') {
    switch (format.toLowerCase()) {
      case 'csv':
        return this.exportToCSV(emails);
      case 'json':
        return this.exportToJSON(emails);
      case 'txt':
        return this.exportToText(emails);
      default:
        throw new Error('Unsupported export format');
    }
  }

  exportToCSV(emails) {
    const headers = ['Email', 'Source', 'Sheet', 'Row', 'Column'];
    const csvContent = [
      headers.join(','),
      ...emails.map(email => [
        email.email,
        `"${email.source}"`,
        email.sheet,
        email.rowIndex + 1,
        email.columnIndex + 1
      ].join(','))
    ].join('\n');

    return new Blob([csvContent], { type: 'text/csv' });
  }

  exportToJSON(emails) {
    const jsonContent = JSON.stringify(emails, null, 2);
    return new Blob([jsonContent], { type: 'application/json' });
  }

  exportToText(emails) {
    const textContent = emails.map(email => email.email).join('\n');
    return new Blob([textContent], { type: 'text/plain' });
  }
}

// Create singleton instance
const excelParserService = new ExcelParserService();

export default excelParserService;