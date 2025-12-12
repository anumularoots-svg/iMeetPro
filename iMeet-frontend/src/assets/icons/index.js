// Icons index file - exports all custom icons used in the app
// This file provides a centralized way to import and use custom SVG icons

// Meeting & Conference Icons
export { default as MeetingIcon } from './meeting.svg';
export { default as CalendarIcon } from './calendar.svg';
export { default as VideoIcon } from './video.svg';
export { default as AudioIcon } from './audio.svg';
export { default as ChatIcon } from './chat.svg';
export { default as ScreenShareIcon } from './screen-share.svg';
export { default as ParticipantsIcon } from './participants.svg';
export { default as RecordingIcon } from './recording.svg';
export { default as HandRaiseIcon } from './hand-raise.svg';
export { default as ReactionsIcon } from './reactions.svg';
export { default as SettingsIcon } from './settings.svg';
export { default as LogoIcon } from './logo.svg';

// Usage example:
// import { MeetingIcon, VideoIcon } from '@assets/icons';
// 
// function MyComponent() {
//   return (
//     <div>
//       <img src={MeetingIcon} alt="Meeting" />
//       <img src={VideoIcon} alt="Video" />
//     </div>
//   );
// }

// For Material-UI integration, you can create icon components:
import React from 'react';
import SvgIcon from '@mui/material/SvgIcon';

// Example of creating a Material-UI compatible icon component
export const CustomMeetingIcon = (props) => (
  <SvgIcon {...props}>
    <svg viewBox="0 0 24 24" fill="currentColor">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
    </svg>
  </SvgIcon>
);

export const CustomVideoIcon = (props) => (
  <SvgIcon {...props}>
    <svg viewBox="0 0 24 24" fill="currentColor">
      <path d="M17 10.5V7c0-.55-.45-1-1-1H4c-.55 0-1 .45-1 1v10c0 .55.45 1 1 1h12c.55 0 1-.45 1-1v-3.5l4 4v-11l-4 4z"/>
    </svg>
  </SvgIcon>
);

// Icon size constants for consistency
export const ICON_SIZES = {
  small: 16,
  medium: 24,
  large: 32,
  xlarge: 48,
};

// Icon color constants
export const ICON_COLORS = {
  primary: '#1976d2',
  secondary: '#dc004e',
  success: '#4caf50',
  warning: '#ff9800',
  error: '#f44336',
  info: '#2196f3',
  disabled: '#9e9e9e',
};