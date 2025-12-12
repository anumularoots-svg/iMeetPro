// utils/muiHelpers.js
export const muiHelpers = {
  // Breakpoint helpers
  breakpoints: {
    xs: 0,
    sm: 600,
    md: 900,
    lg: 1200,
    xl: 1536,
  },

  // Common spacing values
  spacing: {
    xs: 4,
    sm: 8,
    md: 16,
    lg: 24,
    xl: 32,
  },

  // Create responsive value
  responsive: (values) => {
    return {
      xs: values.xs || values.mobile || values.default,
      sm: values.sm || values.tablet || values.default,
      md: values.md || values.desktop || values.default,
      lg: values.lg || values.desktop || values.default,
      xl: values.xl || values.desktop || values.default,
    };
  },

  // Box shadow presets
  shadows: {
    light: '0 2px 8px rgba(0,0,0,0.1)',
    medium: '0 4px 16px rgba(0,0,0,0.15)',
    heavy: '0 8px 32px rgba(0,0,0,0.2)',
    card: '0 2px 12px rgba(0,0,0,0.08)',
    float: '0 6px 20px rgba(0,0,0,0.12)',
  },

  // Color utilities
  colors: {
    primary: '#1976d2',
    secondary: '#dc004e',
    success: '#2e7d32',
    error: '#d32f2f',
    warning: '#ed6c02',
    info: '#0288d1',
    grey: {
      50: '#fafafa',
      100: '#f5f5f5',
      200: '#eeeeee',
      300: '#e0e0e0',
      400: '#bdbdbd',
      500: '#9e9e9e',
      600: '#757575',
      700: '#616161',
      800: '#424242',
      900: '#212121',
    },
  },

  // Animation presets
  animations: {
    fadeIn: {
      '@keyframes fadeIn': {
        '0%': { opacity: 0 },
        '100%': { opacity: 1 },
      },
      animation: 'fadeIn 0.3s ease-in-out',
    },
    slideUp: {
      '@keyframes slideUp': {
        '0%': { transform: 'translateY(20px)', opacity: 0 },
        '100%': { transform: 'translateY(0)', opacity: 1 },
      },
      animation: 'slideUp 0.3s ease-out',
    },
    pulse: {
      '@keyframes pulse': {
        '0%': { transform: 'scale(1)' },
        '50%': { transform: 'scale(1.05)' },
        '100%': { transform: 'scale(1)' },
      },
      animation: 'pulse 2s infinite',
    },
  },
};