// Professional Typography Configuration
export const typography = {
  fontFamily: [
    'Inter',
    'Roboto',
    '-apple-system',
    'BlinkMacSystemFont',
    '"Segoe UI"',
    'Arial',
    'sans-serif'
  ].join(','),
  
  // Display styles - for hero sections
  h1: {
    fontSize: '2.5rem',
    fontWeight: 700,
    lineHeight: 1.2,
    letterSpacing: '-0.02em'
  },
  
  // Page titles
  h2: {
    fontSize: '2rem',
    fontWeight: 600,
    lineHeight: 1.3,
    letterSpacing: '-0.01em'
  },
  
  // Section headers
  h3: {
    fontSize: '1.5rem',
    fontWeight: 600,
    lineHeight: 1.4
  },
  
  // Card titles
  h4: {
    fontSize: '1.25rem',
    fontWeight: 600,
    lineHeight: 1.4
  },
  
  // Component headers
  h5: {
    fontSize: '1.125rem',
    fontWeight: 600,
    lineHeight: 1.5
  },
  
  // Small headers
  h6: {
    fontSize: '1rem',
    fontWeight: 600,
    lineHeight: 1.5
  },
  
  // Body text
  body1: {
    fontSize: '1rem',
    fontWeight: 400,
    lineHeight: 1.6
  },
  
  // Secondary body text
  body2: {
    fontSize: '0.875rem',
    fontWeight: 400,
    lineHeight: 1.5
  },
  
  // Button text
  button: {
    fontSize: '0.875rem',
    fontWeight: 600,
    lineHeight: 1.4,
    textTransform: 'none', // Don't uppercase buttons
    letterSpacing: '0.02em'
  },
  
  // Captions
  caption: {
    fontSize: '0.75rem',
    fontWeight: 400,
    lineHeight: 1.4
  },
  
  // Overlines and labels
  overline: {
    fontSize: '0.75rem',
    fontWeight: 600,
    lineHeight: 1.4,
    textTransform: 'uppercase',
    letterSpacing: '0.08em'
  },
  
  // Meeting-specific typography
  meetingTitle: {
    fontSize: '1.125rem',
    fontWeight: 600,
    lineHeight: 1.4
  },
  
  participantName: {
    fontSize: '0.875rem',
    fontWeight: 500,
    lineHeight: 1.4
  },
  
  chatMessage: {
    fontSize: '0.875rem',
    fontWeight: 400,
    lineHeight: 1.5
  },
  
  timestamp: {
    fontSize: '0.75rem',
    fontWeight: 400,
    lineHeight: 1.4,
    color: 'text.secondary'
  },
  
  controlLabel: {
    fontSize: '0.75rem',
    fontWeight: 500,
    lineHeight: 1.4
  }
};

// Font weights
export const fontWeights = {
  light: 300,
  regular: 400,
  medium: 500,
  semiBold: 600,
  bold: 700,
  extraBold: 800
};

// Letter spacing
export const letterSpacing = {
  tight: '-0.02em',
  normal: '0em',
  wide: '0.02em',
  wider: '0.05em',
  widest: '0.08em'
};