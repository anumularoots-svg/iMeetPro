// Responsive breakpoints for video meeting app
export const breakpoints = {
  values: {
    xs: 0,      // Mobile phones
    sm: 600,    // Large phones / small tablets
    md: 960,    // Tablets
    lg: 1280,   // Desktop
    xl: 1920    // Large desktop
  }
};

// Meeting-specific breakpoints
export const meetingBreakpoints = {
  // Video grid layouts
  videoGrid: {
    mobile: 480,     // 1 video per row
    tablet: 768,     // 2 videos per row
    desktop: 1024,   // 3-4 videos per row
    large: 1440      // 6+ videos per row
  },
  
  // Chat panel
  chat: {
    collapse: 768,   // Collapse chat on tablet and below
    overlay: 960     // Show chat as overlay
  },
  
  // Controls
  controls: {
    compact: 480,    // Compact controls on mobile
    standard: 768    // Standard controls
  },
  
  // Participants list
  participants: {
    sidebar: 1024,   // Show as sidebar
    drawer: 768      // Show as drawer
  }
};

// Utility functions for responsive design
export const getVideoGridColumns = (width) => {
  if (width < meetingBreakpoints.videoGrid.mobile) return 1;
  if (width < meetingBreakpoints.videoGrid.tablet) return 1;
  if (width < meetingBreakpoints.videoGrid.desktop) return 2;
  if (width < meetingBreakpoints.videoGrid.large) return 3;
  return 4;
};

export const shouldCollapseSidebar = (width) => {
  return width < meetingBreakpoints.participants.sidebar;
};

export const shouldShowChatOverlay = (width) => {
  return width < meetingBreakpoints.chat.overlay;
};

export const shouldUseCompactControls = (width) => {
  return width < meetingBreakpoints.controls.compact;
};