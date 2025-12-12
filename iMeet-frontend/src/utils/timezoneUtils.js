// utils/timezoneUtils.js
export const timezoneUtils = {
  // Get user's timezone
  getUserTimezone: () => {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  },

  // Convert date to specific timezone
  convertToTimezone: (date, timezone) => {
    return new Date(date.toLocaleString('en-US', { timeZone: timezone }));
  },

  // Format date with timezone
  formatWithTimezone: (date, timezone, options = {}) => {
    return new Date(date).toLocaleString('en-US', {
      timeZone: timezone,
      ...options,
    });
  },

  // Get timezone offset
  getTimezoneOffset: (timezone) => {
    const now = new Date();
    const utc = new Date(now.getTime() + (now.getTimezoneOffset() * 60000));
    const targetTime = new Date(utc.toLocaleString('en-US', { timeZone: timezone }));
    return (targetTime.getTime() - utc.getTime()) / (1000 * 60 * 60);
  },

  // Common timezones
  commonTimezones: [
    { value: 'UTC', label: 'UTC (Coordinated Universal Time)', offset: 0 },
    { value: 'America/New_York', label: 'Eastern Time (US & Canada)', offset: -5 },
    { value: 'America/Chicago', label: 'Central Time (US & Canada)', offset: -6 },
    { value: 'America/Denver', label: 'Mountain Time (US & Canada)', offset: -7 },
    { value: 'America/Los_Angeles', label: 'Pacific Time (US & Canada)', offset: -8 },
    { value: 'Europe/London', label: 'London (GMT)', offset: 0 },
    { value: 'Europe/Paris', label: 'Paris (CET)', offset: 1 },
    { value: 'Asia/Tokyo', label: 'Tokyo (JST)', offset: 9 },
    { value: 'Asia/Shanghai', label: 'Shanghai (CST)', offset: 8 },
    { value: 'Asia/Kolkata', label: 'India (IST)', offset: 5.5 },
    { value: 'Australia/Sydney', label: 'Sydney (AEDT)', offset: 11 },
  ],
};