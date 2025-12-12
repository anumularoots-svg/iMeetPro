// hooks/useExcelUpload.js
import { useState, useCallback } from 'react';
import excelParserService from '../services/excelParser';

const useExcelUpload = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [parseResult, setParseResult] = useState(null);
  const [validationResult, setValidationResult] = useState(null);
  const [summary, setSummary] = useState(null);

  const resetState = useCallback(() => {
    setLoading(false);
    setError(null);
    setParseResult(null);
    setValidationResult(null);
    setSummary(null);
  }, []);

  const uploadAndParseFile = useCallback(async (file) => {
    if (!file) {
      setError('No file provided');
      return null;
    }

    setLoading(true);
    setError(null);

    try {
      // Check if file is supported
      if (!excelParserService.isSupported(file)) {
        throw new Error('Unsupported file format. Please upload .xlsx, .xls, or .csv files.');
      }

      // Parse the file
      const result = await excelParserService.parseFile(file);
      setParseResult(result);

      // Validate emails
      const validation = excelParserService.validateAndCleanEmails(result.emails);
      setValidationResult(validation);

      // Generate summary
      const summaryData = excelParserService.generateSummary(result);
      setSummary(summaryData);

      return {
        parseResult: result,
        validationResult: validation,
        summary: summaryData
      };

    } catch (err) {
      const errorMessage = err.message || 'Failed to process file';
      setError(errorMessage);
      console.error('Excel upload error:', err);
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  const getValidEmails = useCallback(() => {
    if (!validationResult) return [];
    return validationResult.valid.map(item => item.email);
  }, [validationResult]);

  const getUniqueEmails = useCallback(() => {
    if (!validationResult) return [];
    return validationResult.cleaned.map(item => item.email);
  }, [validationResult]);

  const exportValidEmails = useCallback((format = 'csv') => {
    if (!validationResult) {
      throw new Error('No validation result available');
    }

    return excelParserService.exportEmails(validationResult.valid, format);
  }, [validationResult]);

  const exportUniqueEmails = useCallback((format = 'csv') => {
    if (!validationResult) {
      throw new Error('No validation result available');
    }

    return excelParserService.exportEmails(validationResult.cleaned, format);
  }, [validationResult]);

  const downloadExport = useCallback((blob, filename) => {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }, []);

  const getEmailsBySheet = useCallback((sheetName) => {
    if (!parseResult) return [];
    const sheet = parseResult.sheets.find(s => s.name === sheetName);
    return sheet ? sheet.emails : [];
  }, [parseResult]);

  const getSheetNames = useCallback(() => {
    if (!parseResult) return [];
    return parseResult.sheets.map(sheet => sheet.name);
  }, [parseResult]);

  const getSheetPreview = useCallback((sheetName) => {
    if (!parseResult) return [];
    const sheet = parseResult.sheets.find(s => s.name === sheetName);
    return sheet ? sheet.preview : [];
  }, [parseResult]);

  const validateSingleEmail = useCallback((email) => {
    return excelParserService.isValidEmail(email);
  }, []);

  const formatFileSize = useCallback((bytes) => {
    return excelParserService.formatFileSize(bytes);
  }, []);

  // Advanced filtering functions
  const filterEmailsByPattern = useCallback((pattern) => {
    if (!validationResult) return [];
    
    const regex = new RegExp(pattern, 'i');
    return validationResult.valid.filter(item => 
      regex.test(item.email) || regex.test(item.source)
    );
  }, [validationResult]);

  const getEmailsByDomain = useCallback(() => {
    if (!validationResult) return {};
    
    const domainMap = {};
    validationResult.valid.forEach(item => {
      const domain = item.email.split('@')[1];
      if (!domainMap[domain]) {
        domainMap[domain] = [];
      }
      domainMap[domain].push(item);
    });
    
    return domainMap;
  }, [validationResult]);

  const getStatistics = useCallback(() => {
    if (!summary || !validationResult) return null;

    const domainStats = getEmailsByDomain();
    const topDomains = Object.entries(domainStats)
      .sort(([,a], [,b]) => b.length - a.length)
      .slice(0, 5);

    return {
      ...summary,
      topDomains: topDomains.map(([domain, emails]) => ({
        domain,
        count: emails.length,
        percentage: ((emails.length / validationResult.valid.length) * 100).toFixed(1)
      })),
      averageEmailsPerSheet: summary.emails.total / summary.structure.sheets,
      successRate: ((summary.emails.valid / summary.emails.total) * 100).toFixed(1)
    };
  }, [summary, validationResult, getEmailsByDomain]);

  return {
    // State
    loading,
    error,
    parseResult,
    validationResult,
    summary,

    // Actions
    uploadAndParseFile,
    resetState,

    // Email extraction
    getValidEmails,
    getUniqueEmails,
    validateSingleEmail,

    // Export functions
    exportValidEmails,
    exportUniqueEmails,
    downloadExport,

    // Sheet operations
    getEmailsBySheet,
    getSheetNames,
    getSheetPreview,

    // Filtering and analysis
    filterEmailsByPattern,
    getEmailsByDomain,
    getStatistics,

    // Utilities
    formatFileSize,

    // Computed values
    hasData: !!parseResult,
    hasValidEmails: validationResult?.valid?.length > 0,
    hasErrors: validationResult?.invalid?.length > 0,
    hasDuplicates: validationResult?.duplicates?.length > 0,
    totalEmails: parseResult?.emails?.length || 0,
    validEmailCount: validationResult?.valid?.length || 0,
    uniqueEmailCount: validationResult?.cleaned?.length || 0,
    invalidEmailCount: validationResult?.invalid?.length || 0,
    duplicateEmailCount: validationResult?.duplicates?.length || 0
  };
};

export default useExcelUpload;