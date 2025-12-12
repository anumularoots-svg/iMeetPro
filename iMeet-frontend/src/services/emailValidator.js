// services/emailValidator.js

/**
 * EmailValidator Service - Advanced email validation with API integration
 * This service provides comprehensive email validation including format validation,
 * domain verification, disposable email detection, and optional API-based validation
 */

class EmailValidatorService {
  constructor() {
    this.cache = new Map(); // Cache validation results
    this.cacheTimeout = 300000; // 5 minutes cache
    this.apiEndpoint = null; // Optional API endpoint for advanced validation
    this.rateLimitDelay = 100; // Delay between API calls (ms)
    
    // Common disposable email domains
    this.disposableDomains = new Set([
      '10minutemail.com', 'guerrillamail.com', 'mailinator.com',
      'tempmail.org', 'throwaway.email', 'temp-mail.org',
      'yopmail.com', 'maildrop.cc', 'sharklasers.com',
      'getnada.com', 'tempail.com', 'dispostable.com'
    ]);

    // Common domain typos
    this.domainCorrections = {
      'gmai.com': 'gmail.com',
      'gmial.com': 'gmail.com',
      'gmaill.com': 'gmail.com',
      'gmil.com': 'gmail.com',
      'yahooo.com': 'yahoo.com',
      'yaho.com': 'yahoo.com',
      'yahho.com': 'yahoo.com',
      'outlok.com': 'outlook.com',
      'outloook.com': 'outlook.com',
      'outlook.co': 'outlook.com',
      'hotmial.com': 'hotmail.com',
      'hotmail.co': 'hotmail.com'
    };

    // Trust score factors
    this.trustFactors = {
      COMMON_DOMAIN: 20,
      BUSINESS_DOMAIN: 15,
      PROPER_FORMAT: 10,
      NO_TYPOS: 10,
      REASONABLE_LENGTH: 5,
      DISPOSABLE_EMAIL: -30,
      SUSPICIOUS_PATTERN: -15,
      TYPO_DOMAIN: -10
    };
  }

  /**
   * Set API endpoint for advanced validation
   */
  setApiEndpoint(endpoint, apiKey = null) {
    this.apiEndpoint = endpoint;
    this.apiKey = apiKey;
  }

  /**
   * Basic email format validation
   */
  isValidFormat(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  /**
   * Advanced email format validation with detailed checks
   */
  validateFormat(email) {
    const result = {
      isValid: false,
      issues: [],
      parts: null
    };

    // Basic format check
    if (!this.isValidFormat(email)) {
      result.issues.push('Invalid email format');
      return result;
    }

    const [localPart, domain] = email.split('@');
    result.parts = { localPart, domain };

    // Length validations
    if (email.length > 254) {
      result.issues.push('Email too long (max 254 characters)');
      return result;
    }

    if (localPart.length > 64) {
      result.issues.push('Local part too long (max 64 characters)');
      return result;
    }

    if (localPart.length === 0) {
      result.issues.push('Local part cannot be empty');
      return result;
    }

    // Domain validations
    if (domain.length > 253) {
      result.issues.push('Domain too long (max 253 characters)');
      return result;
    }

    const domainParts = domain.split('.');
    if (domainParts.length < 2) {
      result.issues.push('Domain must have at least one dot');
      return result;
    }

    // Check for invalid characters
    const validLocalChars = /^[a-zA-Z0-9._%+-]+$/;
    if (!validLocalChars.test(localPart)) {
      result.issues.push('Local part contains invalid characters');
      return result;
    }

    // Check for consecutive dots
    if (localPart.includes('..') || domain.includes('..')) {
      result.issues.push('Cannot contain consecutive dots');
      return result;
    }

    // Check start/end with dots
    if (localPart.startsWith('.') || localPart.endsWith('.')) {
      result.issues.push('Local part cannot start or end with dot');
      return result;
    }

    // TLD validation
    const tld = domainParts[domainParts.length - 1];
    if (tld.length < 2) {
      result.issues.push('Top-level domain too short');
      return result;
    }

    // If no issues, mark as valid
    if (result.issues.length === 0) {
      result.isValid = true;
    }

    return result;
  }

  /**
   * Check if domain is disposable/temporary
   */
  isDisposableEmail(email) {
    const domain = email.split('@')[1]?.toLowerCase();
    return this.disposableDomains.has(domain);
  }

  /**
   * Suggest domain corrections for typos
   */
  suggestDomainCorrection(email) {
    const domain = email.split('@')[1]?.toLowerCase();
    return this.domainCorrections[domain] || null;
  }

  /**
   * Calculate trust score for email
   */
  calculateTrustScore(email, formatValidation) {
    let score = 50; // Base score

    const domain = email.split('@')[1]?.toLowerCase();
    const localPart = email.split('@')[0]?.toLowerCase();

    // Common domain bonus
    const commonDomains = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com'];
    if (commonDomains.includes(domain)) {
      score += this.trustFactors.COMMON_DOMAIN;
    }

    // Business domain detection
    if (!commonDomains.includes(domain) && !this.disposableDomains.has(domain)) {
      score += this.trustFactors.BUSINESS_DOMAIN;
    }

    // Format validation bonus
    if (formatValidation.isValid) {
      score += this.trustFactors.PROPER_FORMAT;
    }

    // Domain typo penalty
    if (this.domainCorrections[domain]) {
      score += this.trustFactors.TYPO_DOMAIN;
    } else {
      score += this.trustFactors.NO_TYPOS;
    }

    // Length reasonableness
    if (email.length >= 6 && email.length <= 50) {
      score += this.trustFactors.REASONABLE_LENGTH;
    }

    // Disposable email penalty
    if (this.isDisposableEmail(email)) {
      score += this.trustFactors.DISPOSABLE_EMAIL;
    }

    // Suspicious patterns
    if (/^\d+@/.test(email) || localPart.length < 2) {
      score += this.trustFactors.SUSPICIOUS_PATTERN;
    }

    // Ensure score is within bounds
    return Math.max(0, Math.min(100, score));
  }

  /**
   * Comprehensive email validation
   */
  async validateEmail(email, options = {}) {
    const {
      checkDisposable = true,
      suggestCorrections = true,
      useCache = true,
      apiValidation = false
    } = options;

    const normalizedEmail = email.trim().toLowerCase();
    
    // Check cache first
    if (useCache && this.cache.has(normalizedEmail)) {
      const cached = this.cache.get(normalizedEmail);
      if (Date.now() - cached.timestamp < this.cacheTimeout) {
        return cached.result;
      }
    }

    const result = {
      email: normalizedEmail,
      isValid: false,
      isFormatValid: false,
      isDisposable: false,
      trustScore: 0,
      issues: [],
      warnings: [],
      suggestions: [],
      metadata: {
        localPart: '',
        domain: '',
        tld: '',
        category: 'unknown'
      }
    };

    // Format validation
    const formatValidation = this.validateFormat(normalizedEmail);
    result.isFormatValid = formatValidation.isValid;
    result.issues.push(...formatValidation.issues);

    if (formatValidation.parts) {
      result.metadata.localPart = formatValidation.parts.localPart;
      result.metadata.domain = formatValidation.parts.domain;
      result.metadata.tld = formatValidation.parts.domain.split('.').pop();
    }

    // Only proceed with other checks if format is valid
    if (result.isFormatValid) {
      // Disposable email check
      if (checkDisposable) {
        result.isDisposable = this.isDisposableEmail(normalizedEmail);
        if (result.isDisposable) {
          result.warnings.push('This appears to be a disposable/temporary email');
        }
      }

      // Domain correction suggestions
      if (suggestCorrections) {
        const suggestion = this.suggestDomainCorrection(normalizedEmail);
        if (suggestion) {
          result.suggestions.push(`Did you mean ${normalizedEmail.split('@')[0]}@${suggestion}?`);
        }
      }

      // Calculate trust score
      result.trustScore = this.calculateTrustScore(normalizedEmail, formatValidation);

      // Determine category
      result.metadata.category = this.categorizeEmail(normalizedEmail);

      // API validation if enabled
      if (apiValidation && this.apiEndpoint) {
        try {
          const apiResult = await this.performApiValidation(normalizedEmail);
          result.apiValidation = apiResult;
          
          // Adjust trust score based on API result
          if (apiResult.deliverable === false) {
            result.trustScore = Math.max(0, result.trustScore - 30);
            result.warnings.push('Email may not be deliverable');
          }
        } catch (error) {
          result.warnings.push('Could not perform API validation');
        }
      }

      // Final validation decision
      result.isValid = result.isFormatValid && 
                      !result.isDisposable && 
                      result.trustScore >= 30 &&
                      result.issues.length === 0;
    }

    // Cache the result
    if (useCache) {
      this.cache.set(normalizedEmail, {
        result: { ...result },
        timestamp: Date.now()
      });
    }

    return result;
  }

  /**
   * Categorize email type
   */
  categorizeEmail(email) {
    const domain = email.split('@')[1]?.toLowerCase();
    
    const personalDomains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com'];
    if (personalDomains.includes(domain)) {
      return 'personal';
    }

    if (this.disposableDomains.has(domain)) {
      return 'disposable';
    }

    const eduDomains = ['.edu', '.ac.'];
    if (eduDomains.some(edu => domain.includes(edu))) {
      return 'educational';
    }

    const govDomains = ['.gov', '.mil'];
    if (govDomains.some(gov => domain.includes(gov))) {
      return 'government';
    }

    return 'business';
  }

  /**
   * API validation (placeholder for external service integration)
   */
  async performApiValidation(email) {
    if (!this.apiEndpoint) {
      throw new Error('API endpoint not configured');
    }

    // Add rate limiting
    await this.sleep(this.rateLimitDelay);

    const headers = {
      'Content-Type': 'application/json'
    };

    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    const response = await fetch(this.apiEndpoint, {
      method: 'POST',
      headers,
      body: JSON.stringify({ email })
    });

    if (!response.ok) {
      throw new Error(`API validation failed: ${response.statusText}`);
    }

    return await response.json();
  }

  /**
   * Batch email validation
   */
  async validateEmailBatch(emails, options = {}) {
    const {
      batchSize = 10,
      delay = 100,
      stopOnFirstError = false
    } = options;

    const results = [];
    const errors = [];

    // Process in batches to avoid overwhelming the system
    for (let i = 0; i < emails.length; i += batchSize) {
      const batch = emails.slice(i, i + batchSize);
      const batchPromises = batch.map(async (email, index) => {
        try {
          const result = await this.validateEmail(email, options);
          return { email, result, index: i + index };
        } catch (error) {
          const errorResult = { 
            email, 
            error: error.message, 
            index: i + index 
          };
          
          if (stopOnFirstError) {
            throw errorResult;
          }
          
          errors.push(errorResult);
          return null;
        }
      });

      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults.filter(r => r !== null));

      // Add delay between batches
      if (i + batchSize < emails.length && delay > 0) {
        await this.sleep(delay);
      }
    }

    return {
      results,
      errors,
      summary: {
        total: emails.length,
        processed: results.length,
        errors: errors.length,
        valid: results.filter(r => r.result.isValid).length,
        invalid: results.filter(r => !r.result.isValid).length
      }
    };
  }

  /**
   * Generate validation report
   */
  generateValidationReport(validationResults) {
    const valid = validationResults.filter(r => r.result.isValid);
    const invalid = validationResults.filter(r => !r.result.isValid);
    const disposable = validationResults.filter(r => r.result.isDisposable);
    
    const categories = validationResults.reduce((acc, r) => {
      const category = r.result.metadata.category;
      acc[category] = (acc[category] || 0) + 1;
      return acc;
    }, {});

    const domains = validationResults.reduce((acc, r) => {
      const domain = r.result.metadata.domain;
      acc[domain] = (acc[domain] || 0) + 1;
      return acc;
    }, {});

    const topDomains = Object.entries(domains)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 10)
      .map(([domain, count]) => ({ domain, count }));

    return {
      summary: {
        total: validationResults.length,
        valid: valid.length,
        invalid: invalid.length,
        disposable: disposable.length,
        validationRate: ((valid.length / validationResults.length) * 100).toFixed(1)
      },
      categories,
      topDomains,
      issues: {
        formatErrors: invalid.filter(r => !r.result.isFormatValid).length,
        disposableEmails: disposable.length,
        lowTrustScore: validationResults.filter(r => r.result.trustScore < 50).length
      },
      averageTrustScore: (
        validationResults.reduce((sum, r) => sum + r.result.trustScore, 0) / 
        validationResults.length
      ).toFixed(1)
    };
  }

  /**
   * Clear validation cache
   */
  clearCache() {
    this.cache.clear();
  }

  /**
   * Sleep utility for rate limiting
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Update disposable domains list
   */
  updateDisposableDomains(domains) {
    domains.forEach(domain => this.disposableDomains.add(domain.toLowerCase()));
  }

  /**
   * Export validation results
   */
  exportValidationResults(results, format = 'csv') {
    switch (format.toLowerCase()) {
      case 'csv':
        return this.exportToCsv(results);
      case 'json':
        return this.exportToJson(results);
      default:
        throw new Error('Unsupported export format');
    }
  }

  /**
   * Export to CSV
   */
  exportToCsv(results) {
    const headers = ['Email', 'Valid', 'Trust Score', 'Category', 'Issues', 'Warnings'];
    const rows = results.map(r => [
      r.email,
      r.result.isValid ? 'Yes' : 'No',
      r.result.trustScore,
      r.result.metadata.category,
      r.result.issues.join('; '),
      r.result.warnings.join('; ')
    ]);

    const csvContent = [headers, ...rows]
      .map(row => row.map(field => `"${field}"`).join(','))
      .join('\n');

    return new Blob([csvContent], { type: 'text/csv' });
  }

  /**
   * Export to JSON
   */
  exportToJson(results) {
    const jsonContent = JSON.stringify(results, null, 2);
    return new Blob([jsonContent], { type: 'application/json' });
  }
}

// Create singleton instance
const emailValidatorService = new EmailValidatorService();

export default emailValidatorService;