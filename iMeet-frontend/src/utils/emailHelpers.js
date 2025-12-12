// utils/emailHelpers.js

/**
 * Comprehensive email utilities for validation, formatting, and processing
 */

export class EmailHelpers {
  // Common email domains for validation and suggestions
  static COMMON_DOMAINS = [
    'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'aol.com',
    'icloud.com', 'protonmail.com', 'zoho.com', 'mail.com', 'yandex.com'
  ];

  // Common domain typos and their corrections
  static DOMAIN_CORRECTIONS = {
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
    'hotmail.co': 'hotmail.com',
    'hotmailcom': 'hotmail.com',
    'gmailcom': 'gmail.com',
    'yahoocom': 'yahoo.com'
  };

  // Business email domains (partial list)
  static BUSINESS_DOMAINS = [
    'company.com', 'corp.com', 'inc.com', 'ltd.com', 'org',
    'edu', 'gov', 'mil', 'int'
  ];

  /**
   * Basic email format validation
   */
  static isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  /**
   * Enhanced email validation with detailed analysis
   */
  static validateEmailDetailed(email) {
    const result = {
      isValid: false,
      email: email.trim().toLowerCase(),
      parts: { local: '', domain: '', tld: '' },
      issues: [],
      suggestions: [],
      type: 'unknown', // personal, business, temporary, etc.
      score: 0 // 0-100 quality score
    };

    // Basic format check
    if (!this.isValidEmail(email)) {
      result.issues.push('Invalid email format');
      return result;
    }

    const [local, domain] = email.split('@');
    result.parts = {
      local: local.toLowerCase(),
      domain: domain.toLowerCase(),
      tld: domain.split('.').pop().toLowerCase()
    };

    // Length validations
    if (email.length > 254) {
      result.issues.push('Email too long (max 254 characters)');
      return result;
    }

    if (local.length > 64) {
      result.issues.push('Local part too long (max 64 characters)');
      return result;
    }

    if (local.length < 1) {
      result.issues.push('Local part cannot be empty');
      return result;
    }

    // Domain validations
    if (domain.length > 253) {
      result.issues.push('Domain too long (max 253 characters)');
      return result;
    }

    // Check for domain typos
    if (this.DOMAIN_CORRECTIONS[domain]) {
      result.suggestions.push(`Did you mean ${this.DOMAIN_CORRECTIONS[domain]}?`);
    }

    // Determine email type
    result.type = this.determineEmailType(domain);

    // Calculate quality score
    result.score = this.calculateEmailScore(result.parts, result.type);

    // Additional validations and warnings
    this.addAdvancedValidations(result);

    result.isValid = result.issues.length === 0;
    return result;
  }

  /**
   * Determine if email is personal, business, temporary, etc.
   */
  static determineEmailType(domain) {
    if (this.COMMON_DOMAINS.includes(domain)) {
      return 'personal';
    }

    if (this.BUSINESS_DOMAINS.some(bd => domain.includes(bd))) {
      return 'business';
    }

    // Temporary email services (partial list)
    const tempDomains = [
      '10minutemail.com', 'guerrillamail.com', 'mailinator.com',
      'tempmail.org', 'throwaway.email'
    ];
    
    if (tempDomains.includes(domain)) {
      return 'temporary';
    }

    // If it's not a common personal domain, likely business
    const tld = domain.split('.').pop();
    if (['com', 'org', 'net', 'edu', 'gov'].includes(tld)) {
      return 'business';
    }

    return 'unknown';
  }

  /**
   * Calculate email quality score (0-100)
   */
  static calculateEmailScore(parts, type) {
    let score = 50; // Base score

    // Domain reputation
    if (this.COMMON_DOMAINS.includes(parts.domain)) {
      score += 20;
    } else if (type === 'business') {
      score += 15;
    } else if (type === 'temporary') {
      score -= 30;
    }

    // Local part quality
    if (parts.local.length >= 3 && parts.local.length <= 20) {
      score += 10;
    } else if (parts.local.length < 3) {
      score -= 10;
    }

    // Check for common patterns
    if (/^[a-z]+\.[a-z]+$/.test(parts.local)) {
      score += 15; // firstname.lastname pattern
    } else if (/^\d+$/.test(parts.local)) {
      score -= 15; // all numbers
    }

    // TLD quality
    const goodTlds = ['com', 'org', 'net', 'edu', 'gov'];
    if (goodTlds.includes(parts.tld)) {
      score += 10;
    }

    // Ensure score is within bounds
    return Math.max(0, Math.min(100, score));
  }

  /**
   * Add advanced validations and warnings
   */
  static addAdvancedValidations(result) {
    const { local, domain } = result.parts;

    // Check for suspicious patterns
    if (local.includes('..')) {
      result.issues.push('Contains consecutive dots');
    }

    if (local.startsWith('.') || local.endsWith('.')) {
      result.issues.push('Cannot start or end with dot');
    }

    if (local.includes('+')) {
      result.suggestions.push('Contains plus symbol (email alias)');
    }

    // Check for very short or long parts
    if (local.length < 3) {
      result.suggestions.push('Very short email address');
    }

    if (local.length > 30) {
      result.suggestions.push('Unusually long local part');
    }

    // Domain specific checks
    if (domain.split('.').length > 4) {
      result.suggestions.push('Unusually deep subdomain');
    }
  }

  /**
   * Extract emails from text using multiple patterns
   */
  static extractEmailsFromText(text) {
    const patterns = [
      /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g,
      /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g,
      /[a-zA-Z0-9._%+-]+\s*@\s*[a-zA-Z0-9.-]+\s*\.\s*[a-zA-Z]{2,}/g
    ];

    const foundEmails = new Set();

    patterns.forEach(pattern => {
      const matches = text.match(pattern);
      if (matches) {
        matches.forEach(email => {
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
   * Clean and normalize email address
   */
  static normalizeEmail(email) {
    if (!email || typeof email !== 'string') return '';
    
    return email
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ''); // Remove all whitespace
  }

  /**
   * Remove duplicates from email list
   */
  static removeDuplicates(emails) {
    const seen = new Set();
    return emails.filter(email => {
      const normalized = this.normalizeEmail(email);
      if (seen.has(normalized)) {
        return false;
      }
      seen.add(normalized);
      return true;
    });
  }

  /**
   * Group emails by domain
   */
  static groupByDomain(emails) {
    const groups = {};
    
    emails.forEach(email => {
      const normalized = this.normalizeEmail(email);
      if (this.isValidEmail(normalized)) {
        const domain = normalized.split('@')[1];
        if (!groups[domain]) {
          groups[domain] = [];
        }
        groups[domain].push(email);
      }
    });

    return groups;
  }

  /**
   * Filter emails by type (personal, business, etc.)
   */
  static filterByType(emails, type) {
    return emails.filter(email => {
      const normalized = this.normalizeEmail(email);
      if (!this.isValidEmail(normalized)) return false;
      
      const domain = normalized.split('@')[1];
      return this.determineEmailType(domain) === type;
    });
  }

  /**
   * Get email domain statistics
   */
  static getDomainStatistics(emails) {
    const stats = {
      total: emails.length,
      domains: {},
      types: {
        personal: 0,
        business: 0,
        temporary: 0,
        unknown: 0
      },
      topDomains: []
    };

    emails.forEach(email => {
      const normalized = this.normalizeEmail(email);
      if (this.isValidEmail(normalized)) {
        const domain = normalized.split('@')[1];
        
        // Count domains
        stats.domains[domain] = (stats.domains[domain] || 0) + 1;
        
        // Count types
        const type = this.determineEmailType(domain);
        stats.types[type]++;
      }
    });

    // Sort domains by count
    stats.topDomains = Object.entries(stats.domains)
      .sort(([,a], [,b]) => b - a)
      .map(([domain, count]) => ({
        domain,
        count,
        percentage: ((count / stats.total) * 100).toFixed(1)
      }));

    return stats;
  }

  /**
   * Suggest corrections for misspelled domains
   */
  static suggestDomainCorrections(email) {
    const normalized = this.normalizeEmail(email);
    if (!normalized.includes('@')) return [];

    const domain = normalized.split('@')[1];
    const suggestions = [];

    // Direct corrections
    if (this.DOMAIN_CORRECTIONS[domain]) {
      suggestions.push(this.DOMAIN_CORRECTIONS[domain]);
    }

    // Fuzzy matching for similar domains
    this.COMMON_DOMAINS.forEach(commonDomain => {
      const similarity = this.calculateStringSimilarity(domain, commonDomain);
      if (similarity > 0.7 && similarity < 1.0) {
        suggestions.push(commonDomain);
      }
    });

    return [...new Set(suggestions)];
  }

  /**
   * Calculate string similarity (Jaro-Winkler-like algorithm)
   */
  static calculateStringSimilarity(str1, str2) {
    if (str1 === str2) return 1.0;
    
    const maxLength = Math.max(str1.length, str2.length);
    const minLength = Math.min(str1.length, str2.length);
    
    if (maxLength === 0) return 1.0;
    
    let matches = 0;
    for (let i = 0; i < minLength; i++) {
      if (str1[i] === str2[i]) {
        matches++;
      }
    }
    
    return matches / maxLength;
  }

  /**
   * Validate email list and return detailed results
   */
  static validateEmailList(emails) {
    const results = {
      total: emails.length,
      valid: [],
      invalid: [],
      duplicates: [],
      suggestions: [],
      statistics: null
    };

    const seen = new Set();
    const validEmails = [];

    emails.forEach((email, index) => {
      const normalized = this.normalizeEmail(email);
      const validation = this.validateEmailDetailed(email);
      
      if (validation.isValid) {
        if (seen.has(normalized)) {
          results.duplicates.push({
            email: email,
            index: index,
            normalizedEmail: normalized
          });
        } else {
          seen.add(normalized);
          results.valid.push({
            email: email,
            index: index,
            validation: validation
          });
          validEmails.push(email);
        }
      } else {
        results.invalid.push({
          email: email,
          index: index,
          validation: validation
        });
      }

      // Add suggestions
      if (validation.suggestions.length > 0) {
        results.suggestions.push({
          email: email,
          suggestions: validation.suggestions
        });
      }
    });

    results.statistics = this.getDomainStatistics(validEmails);
    return results;
  }

  /**
   * Format email for display (mask sensitive parts if needed)
   */
  static formatEmailForDisplay(email, maskLevel = 'none') {
    const normalized = this.normalizeEmail(email);
    if (!this.isValidEmail(normalized)) return email;

    const [local, domain] = normalized.split('@');

    switch (maskLevel) {
      case 'partial':
        if (local.length <= 2) return email;
        return `${local[0]}${'*'.repeat(local.length - 2)}${local[local.length - 1]}@${domain}`;
      
      case 'domain':
        return `${local}@***`;
      
      case 'full':
        return '***@***';
      
      default:
        return email;
    }
  }

  /**
   * Export emails to different formats
   */
  static exportEmails(emails, format = 'csv') {
    const validEmails = emails.filter(email => this.isValidEmail(this.normalizeEmail(email)));
    
    switch (format.toLowerCase()) {
      case 'csv':
        return validEmails.join('\n');
      
      case 'json':
        return JSON.stringify(validEmails, null, 2);
      
      case 'txt':
        return validEmails.join('\n');
      
      case 'html':
        return `<ul>${validEmails.map(email => `<li>${email}</li>`).join('')}</ul>`;
      
      default:
        throw new Error('Unsupported export format');
    }
  }

  /**
   * Generate email validation report
   */
  static generateValidationReport(emails) {
    const validation = this.validateEmailList(emails);
    const stats = validation.statistics;
    
    return {
      summary: {
        total: validation.total,
        valid: validation.valid.length,
        invalid: validation.invalid.length,
        duplicates: validation.duplicates.length,
        validationRate: ((validation.valid.length / validation.total) * 100).toFixed(1)
      },
      domainBreakdown: stats.topDomains.slice(0, 10),
      typeBreakdown: stats.types,
      issues: {
        duplicates: validation.duplicates.length,
        invalidFormat: validation.invalid.filter(item => 
          item.validation.issues.includes('Invalid email format')).length,
        domainIssues: validation.invalid.filter(item => 
          item.validation.issues.some(issue => issue.includes('domain'))).length
      },
      suggestions: validation.suggestions.slice(0, 20) // Top 20 suggestions
    };
  }
}

// Export individual functions for convenience
export const {
  isValidEmail,
  validateEmailDetailed,
  normalizeEmail,
  removeDuplicates,
  extractEmailsFromText,
  groupByDomain,
  filterByType,
  getDomainStatistics,
  validateEmailList,
  generateValidationReport
} = EmailHelpers;

export default EmailHelpers;