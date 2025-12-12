// Modern Professional Color Palette for Video Meeting App
export const lightPalette = {
  mode: 'light',
  primary: {
    main: '#1976d2',        // Professional blue
    light: '#42a5f5',       // Light blue
    dark: '#1565c0',        // Dark blue
    contrastText: '#ffffff'
  },
  secondary: {
    main: '#dc004e',        // Meeting red (for leave/end)
    light: '#ff5983',       // Light red
    dark: '#9a0036',        // Dark red
    contrastText: '#ffffff'
  },
  success: {
    main: '#2e7d32',        // Green for success states
    light: '#4caf50',
    dark: '#1b5e20',
    contrastText: '#ffffff'
  },
  warning: {
    main: '#ed6c02',        // Orange for warnings
    light: '#ff9800',
    dark: '#e65100',
    contrastText: '#ffffff'
  },
  error: {
    main: '#d32f2f',        // Red for errors
    light: '#ef5350',
    dark: '#c62828',
    contrastText: '#ffffff'
  },
  info: {
    main: '#0288d1',        // Info blue
    light: '#03a9f4',
    dark: '#01579b',
    contrastText: '#ffffff'
  },
  background: {
    default: '#fafafa',     // Light grey background
    paper: '#ffffff',       // White paper
    meeting: '#f5f5f5',     // Meeting room background
    sidebar: '#ffffff'      // Sidebar background
  },
  text: {
    primary: '#212121',     // Primary text
    secondary: '#757575',   // Secondary text
    disabled: '#bdbdbd'     // Disabled text
  },
  divider: '#e0e0e0',      // Divider color
  meeting: {
    videoBackground: '#000000',    // Video background
    controlsBackground: 'rgba(0, 0, 0, 0.7)', // Controls overlay
    chatBackground: '#ffffff',     // Chat background
    participantCard: '#f8f9fa',    // Participant card
    reactionOverlay: 'rgba(255, 255, 255, 0.9)' // Reaction overlay
  }
};

export const darkPalette = {
  mode: 'dark',
  primary: {
    main: '#90caf9',        // Light blue for dark mode
    light: '#bbdefb',
    dark: '#42a5f5',
    contrastText: '#000000'
  },
  secondary: {
    main: '#f48fb1',        // Light pink
    light: '#f8bbd9',
    dark: '#ad2d5a',
    contrastText: '#000000'
  },
  success: {
    main: '#66bb6a',
    light: '#81c784',
    dark: '#388e3c',
    contrastText: '#000000'
  },
  warning: {
    main: '#ffa726',
    light: '#ffb74d',
    dark: '#f57c00',
    contrastText: '#000000'
  },
  error: {
    main: '#f44336',
    light: '#e57373',
    dark: '#d32f2f',
    contrastText: '#ffffff'
  },
  info: {
    main: '#29b6f6',
    light: '#4fc3f7',
    dark: '#0277bd',
    contrastText: '#000000'
  },
  background: {
    default: '#121212',     // Dark background
    paper: '#1e1e1e',       // Dark paper
    meeting: '#0a0a0a',     // Dark meeting room
    sidebar: '#1e1e1e'      // Dark sidebar
  },
  text: {
    primary: '#ffffff',
    secondary: '#b3b3b3',
    disabled: '#666666'
  },
  divider: '#333333',
  meeting: {
    videoBackground: '#000000',
    controlsBackground: 'rgba(0, 0, 0, 0.8)',
    chatBackground: '#1e1e1e',
    participantCard: '#2a2a2a',
    reactionOverlay: 'rgba(0, 0, 0, 0.9)'
  }
};

// Meeting-specific colors (theme independent)
export const meetingColors = {
  online: '#4caf50',        // Online status
  offline: '#757575',       // Offline status
  away: '#ff9800',          // Away status
  busy: '#f44336',          // Busy status
  recording: '#dc004e',     // Recording indicator
  muted: '#757575',         // Muted state
  unmuted: '#4caf50',       // Unmuted state
  handsRaised: '#ff9800',   // Hand raised
  screenShare: '#2196f3',   // Screen sharing
  chat: '#9c27b0'           // Chat notifications
};