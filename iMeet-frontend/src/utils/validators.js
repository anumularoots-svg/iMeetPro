// utils/validators.js
export const validators = {
  // Email validation
  email: (email) => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  },

  // Password validation
  password: (password) => {
    if (password.length < 8) {
      return { isValid: false, message: 'Password must be at least 8 characters long' };
    }
    if (!/(?=.*[a-z])/.test(password)) {
      return { isValid: false, message: 'Password must contain at least one lowercase letter' };
    }
    if (!/(?=.*[A-Z])/.test(password)) {
      return { isValid: false, message: 'Password must contain at least one uppercase letter' };
    }
    if (!/(?=.*\d)/.test(password)) {
      return { isValid: false, message: 'Password must contain at least one number' };
    }
    return { isValid: true, message: 'Password is valid' };
  },

  // Phone number validation
  phone: (phone) => {
    const phoneRegex = /^\+?[\d\s\-\(\)]{10,}$/;
    return phoneRegex.test(phone);
  },

  // Meeting name validation
  meetingName: (name) => {
    if (!name || name.trim().length === 0) {
      return { isValid: false, message: 'Meeting name is required' };
    }
    if (name.length > 100) {
      return { isValid: false, message: 'Meeting name must be less than 100 characters' };
    }
    return { isValid: true, message: 'Meeting name is valid' };
  },

  // Meeting duration validation
  duration: (duration) => {
    if (!duration || duration <= 0) {
      return { isValid: false, message: 'Duration must be greater than 0' };
    }
    if (duration > 480) { // 8 hours max
      return { isValid: false, message: 'Duration cannot exceed 8 hours' };
    }
    return { isValid: true, message: 'Duration is valid' };
  },

  // URL validation
  url: (url) => {
    try {
      new URL(url);
      return true;
    } catch {
      return false;
    }
  },

  // Meeting ID validation
  meetingId: (id) => {
    const meetingIdRegex = /^[a-zA-Z0-9\-]{8,}$/;
    return meetingIdRegex.test(id);
  },

  // File validation
  file: (file, options = {}) => {
    const {
      maxSize = 10 * 1024 * 1024, // 10MB default
      allowedTypes = ['image/*', 'video/*', 'audio/*', 'application/pdf'],
    } = options;

    if (file.size > maxSize) {
      return { 
        isValid: false, 
        message: `File size must be less than ${Math.round(maxSize / 1024 / 1024)}MB` 
      };
    }

    const fileType = file.type;
    const isAllowed = allowedTypes.some(type => {
      if (type.endsWith('/*')) {
        return fileType.startsWith(type.slice(0, -1));
      }
      return fileType === type;
    });

    if (!isAllowed) {
      return { 
        isValid: false, 
        message: `File type ${fileType} is not allowed` 
      };
    }

    return { isValid: true, message: 'File is valid' };
  },

  // Date validation
  date: (date, options = {}) => {
    const { 
      minDate = new Date(), 
      maxDate = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000) // 1 year from now
    } = options;

    const inputDate = new Date(date);
    
    if (isNaN(inputDate.getTime())) {
      return { isValid: false, message: 'Invalid date format' };
    }

    if (inputDate < minDate) {
      return { isValid: false, message: 'Date cannot be in the past' };
    }

    if (inputDate > maxDate) {
      return { isValid: false, message: 'Date is too far in the future' };
    }

    return { isValid: true, message: 'Date is valid' };
  },

  // Time validation
  time: (time) => {
    const timeRegex = /^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$/;
    return timeRegex.test(time);
  },

  // Required field validation
  required: (value, fieldName = 'Field') => {
    if (!value || (typeof value === 'string' && value.trim().length === 0)) {
      return { isValid: false, message: `${fieldName} is required` };
    }
    return { isValid: true, message: `${fieldName} is valid` };
  },
};