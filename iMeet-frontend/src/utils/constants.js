// ENHANCED: src/utils/constants.js - Complete LiveKit Integration

// FIXED: API Configuration - Base URL without /api suffix to avoid double /api
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'https://192.168.48.201:8111';
export const WEBSOCKET_URL = import.meta.env.VITE_WEBSOCKET_URL || 'wss://192.168.48.201:8111/wss';

// NEW: LiveKit Configuration
export const LIVEKIT_CONFIG = {
  url: import.meta.env.VITE_LIVEKIT_URL || 'wss://192.168.48.201:8921',
  apiKey: import.meta.env.VITE_LIVEKIT_API_KEY || 'jyothi_1a4ef83a56464a698fa56a116feafc48',
  apiSecret: import.meta.env.VITE_LIVEKIT_API_SECRET || '340aef08f9ae6d32debd384b692b4c85828f9cdc034a342eb170cf9b9294676d',
  ttl: 3600, // Token TTL in seconds
  maxParticipants: 200,
  emptyTimeout: 300, // 5 minutes
  departureTimeout: 30 // 30 seconds
};
 
// Authentication
export const TOKEN_KEY = 'meeting_app_token';
export const REFRESH_TOKEN_KEY = 'meeting_app_refresh_token';
export const USER_KEY = 'meeting_app_user';

// Meeting Configuration
export const MEETING_TYPES = {
  INSTANT: 'InstantMeeting',
  SCHEDULED: 'ScheduleMeeting',
  CALENDAR: 'CalendarMeeting'
};

export const PARTICIPANT_ROLES = {
  HOST: 'host',
  CO_HOST: 'co-host',
  PARTICIPANT: 'participant'
};

export const MEETING_STATUS = {
  WAITING: 'waiting',
  ACTIVE: 'active',
  SCHEDULED: 'scheduled',
  ENDED: 'ended',
  CANCELLED: 'cancelled'
};

// NEW: Participant Status (Enhanced for LiveKit)
export const PARTICIPANT_STATUS = {
  LIVE: 'live',        // Connected to LiveKit
  ONLINE: 'online',    // In database but not LiveKit
  OFFLINE: 'offline'   // Left the meeting
};

// User Status
export const USER_STATUS = {
  ONLINE: 'online',
  OFFLINE: 'offline',
  AWAY: 'away',
  BUSY: 'busy'
};

// Message Types
export const MESSAGE_TYPES = {
  TEXT: 'text',
  FILE: 'file',
  SYSTEM: 'system',
  REACTION: 'reaction'
};

// WebRTC Configuration (Enhanced for LiveKit)
export const RTC_CONFIG = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    ...(import.meta.env.VITE_TURN_SERVER ? [{
      urls: import.meta.env.VITE_TURN_SERVER,
      username: import.meta.env.VITE_TURN_USERNAME,
      credential: import.meta.env.VITE_TURN_CREDENTIAL
    }] : [])
  ]
};

// NEW: LiveKit Connection Configuration
export const LIVEKIT_CONNECTION_CONFIG = {
  autoSubscribe: true,
  adaptiveStream: true,
  dynacast: true,
  publishDefaults: {
    videoSimulcast: true,
    audioPreset: 'music',
    videoCodec: 'vp8'
  },
  websocketTimeout: 15000,
  roomConnectTimeout: 15000,
  resumeConnection: 'auto'
};

// WebSocket Events - Updated to match Django consumer + LiveKit
export const SOCKET_EVENTS = {
  // Connection
  CONNECT: 'connect',
  DISCONNECT: 'disconnect',
  CONNECTED: 'connected',
  
  // Meeting
  JOIN_MEETING: 'join-meeting',
  LEAVE_MEETING: 'leave-meeting',
  USER_JOINED: 'user_joined',
  USER_LEFT: 'user_left',
  USER_WAITING: 'user_waiting',
  USER_ALLOWED: 'user_allowed',
  
  // Meeting Status
  STATUS_UPDATE: 'status_update',
  MEETING_STATUS_UPDATE: 'meeting_status_update',
  
  // Host Controls
  ALLOW_USER: 'allow_user',
  
  // Media
  TOGGLE_AUDIO: 'toggle-audio',
  TOGGLE_VIDEO: 'toggle-video',
  START_SCREEN_SHARE: 'start-screen-share',
  STOP_SCREEN_SHARE: 'stop-screen-share',
  
  // Chat
  SEND_MESSAGE: 'send-message',
  MESSAGE_RECEIVED: 'message-received',
  CHAT_MESSAGE: 'chat-message',
  TYPING_START: 'typing-start',
  TYPING_STOP: 'typing-stop',
  
  // Reactions
  SEND_REACTION: 'send_reaction',
  REACTION_RECEIVED: 'reaction_received',
  
  // Interactions
  RAISE_HAND: 'raise-hand',
  LOWER_HAND: 'lower-hand',
  
  // Participant Controls
  MUTE_PARTICIPANT: 'mute-participant',
  REMOVE_PARTICIPANT: 'remove-participant',
  PROMOTE_PARTICIPANT: 'promote-participant',
  
  // Recording
  START_RECORDING: 'start-recording',
  STOP_RECORDING: 'stop-recording',
  RECORDING_STATUS: 'recording-status',
  
  // Error handling
  ERROR: 'error',
  WAITING_ROOM: 'waiting_room',
  
  // NEW: LiveKit Events
  LIVEKIT_CONNECTED: 'livekit-connected',
  LIVEKIT_DISCONNECTED: 'livekit-disconnected',
  LIVEKIT_RECONNECTING: 'livekit-reconnecting',
  LIVEKIT_RECONNECTED: 'livekit-reconnected',
  TRACK_SUBSCRIBED: 'track-subscribed',
  TRACK_UNSUBSCRIBED: 'track-unsubscribed',
  PARTICIPANTS_SYNCED: 'participants_synced',
  PARTICIPANTS_UPDATE: 'participants_update'
};

// ENHANCED: Django API Endpoints - Complete LiveKit Integration
export const API_ENDPOINTS = {
  // Authentication endpoints
  REGISTER: '/api/auth/register',
  LOGIN: '/api/auth/login',
  LOGOUT: '/api/auth/logout',
  FORGOT_PASSWORD: '/api/auth/forgot-password',
  RESET_PASSWORD: '/api/auth/reset-password',
  VERIFY_EMAIL: '/api/auth/verify-email',
  CHANGE_PASSWORD: '/api/auth/change-password',
  UPDATE_PROFILE: '/api/auth/update-profile',
  UPLOAD_PROFILE_PICTURE: '/api/auth/upload-profile-picture',
  VALIDATE_TOKEN: '/api/auth/validate-token',
  GET_USER_BY_ID: '/api/user/list',
  
  // Meeting Management - Updated to match backend endpoints
  CREATE_MEETING: '/api/meetings/create',
  CREATE_INSTANT_MEETING: '/api/meetings/instant-meeting',
  CREATE_SCHEDULED_MEETING: '/api/meetings/schedule-meeting',
  CREATE_CALENDAR_MEETING: '/api/meetings/calendar-meeting',
  GET_MEETING: '/api/meetings/get',
  UPDATE_MEETING: '/api/meetings/update',
  DELETE_MEETING: '/api/meetings/delete',
  LIST_MEETINGS: '/api/meetings/list',
  GET_USER_SCHEDULED_MEETINGS: '/api/meetings/user-schedule-meetings',
  GET_USER_CALENDAR_MEETINGS: '/api/meetings/user-calendar-meetings',
  
  // NEW: LiveKit Integration Endpoints
  LIVEKIT_JOIN_MEETING: '/api/livekit/join-meeting/',
  LIVEKIT_LEAVE_MEETING: '/api/livekit/leave-meeting/',
  LIVEKIT_GET_PARTICIPANTS: '/api/livekit/participants',
  LIVEKIT_CONNECTION_INFO: '/api/livekit/connection-info',
  LIVEKIT_SEND_REACTION: '/api/livekit/send-reaction/',
  LIVEKIT_SEND_CHAT: '/api/livekit/send-chat/',
  LIVEKIT_CHAT_HISTORY: '/api/livekit/chat-history',
  LIVEKIT_UPDATE_STATUS: '/api/livekit/update-status/',
  LIVEKIT_RECORD_EVENT: '/api/livekit/record-event/',
  LIVEKIT_GET_EVENTS: '/api/livekit/events',
  
  // NEW: Bulk Invitation Endpoints
  BULK_SEND_INVITATIONS: '/api/invitations/bulk-send',
  
  // ENHANCED: Participant Management - LiveKit Integration
  PARTICIPANTS_JOIN: '/api/participants/join/',
  PARTICIPANTS_LEAVE: '/api/participants/leave',
  PARTICIPANTS_LIST: '/api/participants/list',
  PARTICIPANTS_GET: '/api/participants/get',
  // PARTICIPANTS_UPDATE: '/api/participants/update',
  PARTICIPANTS_REMOVE: '/api/participants/delete',
  PARTICIPANTS_PROMOTE: '/api/participants/promote',
  PARTICIPANTS_MUTE: '/api/participants/mute',
  PARTICIPANTS_UNMUTE: '/api/participants/unmute',
  
  // NEW: LiveKit Participant Endpoints
  PARTICIPANTS_RECORD_JOIN: '/api/participants/record-join/',
  PARTICIPANTS_RECORD_LEAVE: '/api/participants/record-leave/',
  PARTICIPANTS_GET_LIVE: '/api/participants/live',
  PARTICIPANTS_SYNC_LIVEKIT: '/api/participants/sync',
  // PARTICIPANTS_STATISTICS: '/api/participants/statistics',
  // PARTICIPANTS_BULK_UPDATE: '/api/participants/bulk-update',
  
  // Meeting Data
  GET_MEETING_HISTORY: '/api/Get_Meeting_History',
  MEETING_HISTORY: '/api/meetings/history',
  
  // Settings & Feedback
  UPDATE_MEETING_SETTINGS: '/api/Update_Meeting_Settings',
  SUBMIT_FEEDBACK: '/api/Submit_Feedback'
};

// Reactions
export const REACTIONS = {
  THUMBS_UP: '👍',
  THUMBS_DOWN: '👎',
  CLAP: '👏',
  HEART: '❤️',
  LAUGH: '😀',
  SURPRISED: '😮',
  RAISE_HAND: '✋'
};

// File Upload
export const FILE_UPLOAD = {
  MAX_SIZE: 50 * 1024 * 1024, // 50MB
  ALLOWED_TYPES: [
    'image/jpeg',
    'image/png',
    'image/gif',
    'application/pdf',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'text/plain',
    'text/csv',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
  ]
};

// Video Configuration (Enhanced for LiveKit)
export const VIDEO_CONFIG = {
  DEFAULT_CONSTRAINTS: {
    video: {
      width: { ideal: 1280 },
      height: { ideal: 720 },
      frameRate: { ideal: 30 }
    },
    audio: {
      echoCancellation: true,
      noiseSuppression: true,
      sampleRate: 44100
    }
  },
  SCREEN_SHARE_CONSTRAINTS: {
    video: {
      cursor: 'always'
    },
    audio: false
  },
  // NEW: LiveKit specific video configuration
  LIVEKIT_VIDEO_CONFIG: {
    simulcast: true,
    codec: 'vp8',
    maxBitrate: 2500000, // 2.5 Mbps
    maxFramerate: 30,
    resolution: {
      width: 1280,
      height: 720
    }
  },
  LIVEKIT_AUDIO_CONFIG: {
    preset: 'music',
    echoCancellation: true,
    noiseSuppression: true,
    autoGainControl: true
  }
};

// Meeting Limits
export const MEETING_LIMITS = {
  MAX_PARTICIPANTS: parseInt(import.meta.env.VITE_MAX_PARTICIPANTS) || 200,
  MAX_DURATION: 24 * 60 * 60 * 1000, // 24 hours in milliseconds
  MAX_RECORDING_SIZE: 5 * 1024 * 1024 * 1024, // 5GB
  RECENT_MEETINGS: 5,
  UPCOMING_MEETINGS: 4,
  // NEW: LiveKit specific limits
  LIVEKIT_MAX_PARTICIPANTS: 200,
  LIVEKIT_MAX_CONCURRENT_ROOMS: 100,
  LIVEKIT_TOKEN_TTL: 3600 // 1 hour
};

// Notification Types
export const NOTIFICATION_TYPES = {
  SUCCESS: 'success',
  ERROR: 'error',
  WARNING: 'warning',
  INFO: 'info'
};

// Routes
export const ROUTES = {
  HOME: '/',
  LOGIN: '/login',
  REGISTER: '/register',
  FORGOT_PASSWORD: '/forgot-password',
  EMAIL_VERIFICATION: '/verify-email',
  DASHBOARD: '/dashboard',
  MEETING: '/meeting/:id',
  JOIN_MEETING: '/join/:meetingId',
  INSTANT_MEETING: '/instant-meeting',
  SCHEDULE_MEETING: '/schedule-meeting',
  CALENDAR_MEETING: '/calendar-meeting',
  RECORDINGS: '/recordings',
  SCHEDULE: '/schedule',
  CALENDAR: '/calendar',
  PROFILE: '/profile',
  SETTINGS: '/settings',
  ANALYTICS: '/analytics'
};

// Local Storage Keys
export const STORAGE_KEYS = {
  THEME: 'meeting_app_theme',
  LANGUAGE: 'meeting_app_language',
  AUDIO_DEVICE: 'meeting_app_audio_device',
  VIDEO_DEVICE: 'meeting_app_video_device',
  MEETING_SETTINGS: 'meeting_app_meeting_settings',
  USER_PREFERENCES: 'meeting_user_preferences',
  RECENT_MEETING_IDS: 'recent_meeting_ids',
  DEVICE_SETTINGS: 'device_settings',
  // NEW: LiveKit storage keys
  LIVEKIT_SETTINGS: 'livekit_settings',
  LIVEKIT_DEVICES: 'livekit_devices',
  LIVEKIT_LAST_ROOM: 'livekit_last_room'
};

// Date Formats
export const DATE_FORMATS = {
  DISPLAY: 'MMM dd, yyyy',
  FULL: 'MMMM dd, yyyy HH:mm',
  TIME: 'HH:mm',
  ISO: "yyyy-MM-dd'T'HH:mm:ss",
  DISPLAY_WITH_TIME: 'MMM DD, YYYY HH:mm',
  TIME_ONLY: 'HH:mm'
};

// Animation Durations
export const ANIMATION_DURATION = {
  SHORT: 200,
  MEDIUM: 300,
  LONG: 500
};

// Browser Support
export const BROWSER_SUPPORT = {
  CHROME: 'chrome',
  FIREFOX: 'firefox',
  SAFARI: 'safari',
  EDGE: 'edge',
  SUPPORTED_BROWSERS: ['chrome', 'firefox', 'safari', 'edge'],
  FEATURES_BY_BROWSER: {
    webrtc: ['chrome', 'firefox', 'safari', 'edge'],
    screen_share: ['chrome', 'firefox', 'edge'],
    media_devices: ['chrome', 'firefox', 'safari', 'edge']
  }
};

// Accessibility
export const ACCESSIBILITY = {
  KEYBOARD_SHORTCUTS: {
    TOGGLE_AUDIO: 'Ctrl+D',
    TOGGLE_VIDEO: 'Ctrl+E',
    TOGGLE_CHAT: 'Ctrl+Shift+C',
    LEAVE_MEETING: 'Ctrl+Shift+L',
    RAISE_HAND: 'Ctrl+Shift+R'
  },
  ARIA_LABELS: {
    MUTE_BUTTON: 'Mute microphone',
    VIDEO_BUTTON: 'Turn off camera',
    SCREEN_SHARE: 'Share screen',
    LEAVE_MEETING: 'Leave meeting',
    PARTICIPANT_LIST: 'Participant list'
  }
};

// Internationalization
export const I18N = {
  DEFAULT_LOCALE: 'en-US',
  SUPPORTED_LOCALES: ['en-US', 'es-ES', 'fr-FR', 'de-DE', 'zh-CN', 'ja-JP'],
  RTL_LOCALES: ['ar-SA', 'he-IL'],
  DATE_FORMATS: {
    'en-US': 'MM/DD/YYYY',
    'es-ES': 'DD/MM/YYYY',
    'fr-FR': 'DD/MM/YYYY',
    'de-DE': 'DD.MM.YYYY',
    'zh-CN': 'YYYY/MM/DD',
    'ja-JP': 'YYYY/MM/DD'
  }
};

// Features
export const FEATURES = {
  RECORDING: true,
  SCREEN_SHARE: true,
  CHAT: true,
  REACTIONS: true,
  WAITING_ROOM: true,
  BREAKOUT_ROOMS: false,
  WHITEBOARD: false,
  FILE_SHARING: true,
  POLLS: false,
  VIRTUAL_BACKGROUNDS: false,
  BULK_INVITE: true,
  // NEW: LiveKit specific features
  LIVEKIT_ENABLED: true,
  SIMULCAST: true,
  ADAPTIVE_BITRATE: true,
  DYNACAST: true
};

// Rate Limits
export const RATE_LIMITS = {
  API_REQUESTS: {
    PER_MINUTE: 60,
    PER_HOUR: 1000
  },
  CHAT_MESSAGES: {
    PER_MINUTE: 30,
    PER_HOUR: 500
  },
  REACTIONS: {
    PER_MINUTE: 20,
    PER_HOUR: 200
  },
  BULK_INVITES: {
    PER_MINUTE: 5,
    PER_HOUR: 50
  }
};

// ENHANCED: Error Messages - LiveKit Integration
export const ERROR_MESSAGES = {
  NETWORK_ERROR: 'Network error. Please check your connection.',
  UNAUTHORIZED: 'Please log in to continue.',
  FORBIDDEN: 'You do not have permission to perform this action.',
  NOT_FOUND: 'The requested resource was not found.',
  SERVER_ERROR: 'Server error. Please try again later.',
  VALIDATION_ERROR: 'Please check your input and try again.',
  CAMERA_ERROR: 'Unable to access camera. Please check permissions.',
  MICROPHONE_ERROR: 'Unable to access microphone. Please check permissions.',
  MEETING_FULL: 'This meeting is full.',
  MEETING_ENDED: 'This meeting has ended.',
  INVALID_MEETING_ID: 'Invalid meeting ID.',
  MEETING_NOT_FOUND: 'Meeting not found or has ended.',
  HOST_ID_REQUIRED: 'Host_ID is required',
  MEETING_ID_REQUIRED: 'Meeting_ID or meetingId is required',
  USER_ID_REQUIRED: 'User_ID or userId is required',
  MEETING_NOT_AVAILABLE: 'Meeting is not available',
  NOT_HOST: 'Only the host can perform this action',
  USER_NOT_AUTHENTICATED: 'User not authenticated',
  PARTICIPANT_NOT_FOUND: 'Participant not found',
  PARTICIPANT_ALREADY_LEFT: 'Participant has already left',
  INVALID_ROLE: 'Invalid role specified',
  GENERIC_ERROR: 'An unexpected error occurred. Please try again.',
  
  // NEW: LiveKit specific errors
  LIVEKIT_CONNECTION_FAILED: 'Failed to connect to video service',
  LIVEKIT_TOKEN_EXPIRED: 'Video session expired. Please rejoin.',
  LIVEKIT_ROOM_NOT_FOUND: 'Video room not found',
  LIVEKIT_PARTICIPANT_LIMIT: 'Meeting has reached maximum participants',
  LIVEKIT_MEDIA_ERROR: 'Media device error. Please check your camera and microphone.',
  LIVEKIT_NETWORK_ERROR: 'Network connection lost. Attempting to reconnect...',
  LIVEKIT_PERMISSION_DENIED: 'Camera/microphone permission denied',
  LIVEKIT_SCREEN_SHARE_FAILED: 'Screen sharing failed. Please try again.',
  LIVEKIT_RECORDING_FAILED: 'Recording failed to start',
  SCREEN_SHARE_NOT_SUPPORTED: 'Screen sharing is not supported in this browser',
  
  // NEW: Bulk invite errors
  BULK_INVITE_FAILED: 'Failed to send bulk invitations',
  BULK_INVITE_INVALID_EMAILS: 'Some email addresses are invalid',
  BULK_INVITE_NO_EMAILS: 'Please add some email addresses',
  BULK_INVITE_FILE_ERROR: 'Failed to process uploaded file',
  BULK_INVITE_FILE_FORMAT: 'Unsupported file format. Please use CSV, Excel, or TXT files.'
};

// Success Messages
export const SUCCESS_MESSAGES = {
  MEETING_CREATED: 'Meeting created successfully!',
  MEETING_JOINED: 'Successfully joined the meeting!',
  MEETING_LEFT: 'Left the meeting successfully.',
  MEETING_ENDED: 'Meeting ended successfully.',
  PARTICIPANT_JOINED: 'Participant joined successfully!',
  PARTICIPANT_LEFT: 'Participant left successfully.',
  PARTICIPANT_PROMOTED: 'Participant promoted successfully!',
  PARTICIPANT_MUTED: 'Participant muted successfully.',
  PARTICIPANT_UNMUTED: 'Participant unmuted successfully.',
  PARTICIPANT_REMOVED: 'Participant removed successfully.',
  SETTINGS_UPDATED: 'Meeting settings updated!',
  FEEDBACK_SUBMITTED: 'Thank you for your feedback!',
  
  // NEW: LiveKit specific success messages
  LIVEKIT_CONNECTED: 'Connected to video service successfully!',
  LIVEKIT_RECONNECTED: 'Reconnected to video service!',
  SCREEN_SHARE_STARTED: 'Screen sharing started successfully!',
  SCREEN_SHARE_STOPPED: 'Screen sharing stopped.',
  RECORDING_STARTED: 'Recording started successfully!',
  RECORDING_STOPPED: 'Recording stopped and saved.',
  CHAT_MESSAGE_SENT: 'Message sent successfully!',
  REACTION_SENT: 'Reaction sent!',
  PARTICIPANT_SYNCED: 'Participants synchronized successfully!',
  
  // NEW: Bulk invite success messages
  BULK_INVITE_SENT: 'Bulk invitations sent successfully!',
  BULK_INVITE_PROCESSED: 'Bulk invitations processed successfully!',
  BULK_INVITE_FILE_UPLOADED: 'File uploaded and processed successfully!'
};

// Default Meeting Settings
export const DEFAULT_MEETING_SETTINGS = {
  waitingRoomEnabled: false,
  recordingEnabled: false,
  chatEnabled: true,
  screenShareEnabled: true,
  reactionsEnabled: true,
  // NEW: LiveKit specific settings
  livekitEnabled: true,
  autoJoinAudio: true,
  autoJoinVideo: true,
  adaptiveStream: true,
  simulcast: true,
  echoCancellation: true,
  noiseSuppression: true
};

// Theme Colors
export const THEME_COLORS = {
  primary: 'from-blue-500 to-purple-600',
  secondary: 'from-indigo-500 to-blue-600', 
  success: 'from-green-500 to-emerald-600',
  warning: 'from-yellow-500 to-orange-600',
  danger: 'from-red-500 to-pink-600',
  // NEW: LiveKit status colors
  live: '#4ade80', // green-400
  online: '#3b82f6', // blue-500
  offline: '#6b7280', // gray-500
  connecting: '#f59e0b', // amber-500
  error: '#ef4444' // red-500
};

// Validation Rules
export const VALIDATION = {
  MEETING_NAME: {
    MIN_LENGTH: 1,
    MAX_LENGTH: 100
  },
  PARTICIPANT_NAME: {
    MIN_LENGTH: 1,
    MAX_LENGTH: 50
  },
  MEETING_ID: {
    PATTERN: /^[a-zA-Z0-9-_]+$/
  },
  FULL_NAME: {
    MIN_LENGTH: 1,
    MAX_LENGTH: 100
  },
  ENGAGEMENT_SCORE: {
    MIN: 0,
    MAX: 999.99
  },
  ATTENDANCE_PERCENTAGE: {
    MIN: 0,
    MAX: 999.99
  },
  // NEW: LiveKit validation rules
  LIVEKIT_ROOM_NAME: {
    MIN_LENGTH: 1,
    MAX_LENGTH: 100,
    PATTERN: /^[a-zA-Z0-9_-]+$/
  },
  LIVEKIT_PARTICIPANT_IDENTITY: {
    MIN_LENGTH: 1,
    MAX_LENGTH: 100,
    PATTERN: /^[a-zA-Z0-9_-]+$/
  },
  // NEW: Bulk invite validation
  BULK_INVITE_EMAILS: {
    MAX_COUNT: 100,
    MIN_COUNT: 1
  }
};

// WebSocket Close Codes - for Django Channels + LiveKit
export const WSS_CLOSE_CODES = {
  NORMAL_CLOSURE: 1000,
  GOING_AWAY: 1001,
  PROTOCOL_ERROR: 1002,
  UNSUPPORTED_DATA: 1003,
  MEETING_NOT_FOUND: 4004,
  MEETING_NOT_ACTIVE: 4003,
  UNAUTHORIZED: 4001,
  // NEW: LiveKit specific codes
  LIVEKIT_TOKEN_EXPIRED: 4010,
  LIVEKIT_ROOM_FULL: 4011,
  LIVEKIT_PERMISSION_DENIED: 4012
};

// HTTP Status Codes
export const HTTPS_STATUS = {
  SUCCESS: 200,
  CREATED: 201,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  INTERNAL_SERVER_ERROR: 500
};

// NEW: LiveKit Connection States
export const LIVEKIT_CONNECTION_STATE = {
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  CONNECTED: 'connected',
  RECONNECTING: 'reconnecting',
  FAILED: 'failed'
};

// NEW: LiveKit Track Types
export const LIVEKIT_TRACK_TYPE = {
  CAMERA: 'camera',
  MICROPHONE: 'microphone',
  SCREEN_SHARE: 'screen_share',
  SCREEN_SHARE_AUDIO: 'screen_share_audio'
};

// NEW: LiveKit Participant Permissions
export const LIVEKIT_PERMISSIONS = {
  CAN_PUBLISH: 'canPublish',
  CAN_SUBSCRIBE: 'canSubscribe',
  CAN_PUBLISH_DATA: 'canPublishData',
  CAN_UPDATE_METADATA: 'canUpdateOwnMetadata',
  ROOM_JOIN: 'roomJoin',
  HIDDEN: 'hidden',
  RECORDER: 'recorder'
};

// ENHANCED: API Response Field Mappings - LiveKit Integration
export const FIELD_MAPPINGS = {
  // Meeting fields
  MEETING: {
    frontend: {
      meetingId: 'Meeting_ID',
      name: 'Meeting_Name',
      hostId: 'Host_ID',
      type: 'Meeting_Type',
      status: 'Status',
      recordingEnabled: 'Is_Recording_Enabled',
      waitingRoomEnabled: 'Waiting_Room_Enabled',
      startTime: 'Started_At',
      endTime: 'Ended_At',
      // NEW: LiveKit fields
      livekitRoomName: 'LiveKit_Room_Name',
      livekitRoomSid: 'LiveKit_Room_SID',
      livekitUrl: 'LiveKit_URL'
    },
    backend: {
      Meeting_ID: 'meetingId',
      Meeting_Name: 'name',
      Host_ID: 'hostId',
      Meeting_Type: 'type',
      Status: 'status',
      Is_Recording_Enabled: 'recordingEnabled',
      Waiting_Room_Enabled: 'waitingRoomEnabled',
      Started_At: 'startTime',
      Ended_At: 'endTime',
      // NEW: LiveKit fields
      LiveKit_Room_Name: 'livekitRoomName',
      LiveKit_Room_SID: 'livekitRoomSid',
      LiveKit_URL: 'livekitUrl'
    }
  },
  // Participant fields (Enhanced)
  PARTICIPANT: {
    frontend: {
      meetingId: 'Meeting_ID',
      userId: 'User_ID',
      name: 'Full_Name',
      participantName: 'Full_Name',
      joinTime: 'Join_Time',
      leaveTime: 'Leave_Time',
      role: 'Role',
      duration: 'Duration',
      engagementScore: 'Engagement_Score',
      attendancePercentage: 'Attendance_Percentage',
      // NEW: LiveKit fields
      participantId: 'Participant_ID',
      connectionId: 'Connection_ID',
      livekitConnected: 'LiveKit_Connected',
      audioEnabled: 'Audio_Enabled',
      videoEnabled: 'Video_Enabled',
      status: 'Status'
    },
    backend: {
      Meeting_ID: 'meetingId',
      User_ID: 'userId',
      Full_Name: 'name',
      Join_Time: 'joinTime',
      Leave_Time: 'leaveTime',
      Role: 'role',
      Duration: 'duration',
      Engagement_Score: 'engagementScore',
      Attendance_Percentage: 'attendancePercentage',
      // NEW: LiveKit fields
      Participant_ID: 'participantId',
      Connection_ID: 'connectionId',
      LiveKit_Connected: 'livekitConnected',
      Audio_Enabled: 'audioEnabled',
      Video_Enabled: 'videoEnabled',
      Status: 'status'
    }
  }
};

// ENHANCED: Request/Response Transformers - LiveKit Integration
export const TRANSFORMERS = {
  // Transform frontend participant data to backend format
  participantToBackend: (frontendData) => ({
    meetingId: frontendData.meetingId || frontendData.Meeting_ID,
    userId: frontendData.userId || frontendData.User_ID,
    name: frontendData.name || frontendData.participant_name || frontendData.Full_Name,
    Meeting_ID: frontendData.meetingId,
    User_ID: frontendData.userId,
    Full_Name: frontendData.name || frontendData.participant_name,
    // NEW: LiveKit fields
    participant_identity: frontendData.participantIdentity,
    connection_id: frontendData.connectionId,
    is_host: frontendData.isHost || false,
    livekit_connected: frontendData.livekitConnected || false
  }),
  
  // Transform backend participant data to frontend format
  participantToFrontend: (backendData) => ({
    id: backendData.ID,
    meetingId: backendData.Meeting_ID,
    userId: backendData.User_ID,
    name: backendData.Full_Name,
    joinTime: backendData.Join_Time,
    leaveTime: backendData.Leave_Time,
    role: backendData.Role,
    duration: backendData.Duration,
    engagementScore: backendData.Engagement_Score,
    attendancePercentage: backendData.Attendance_Percentage,
    // NEW: LiveKit fields
    participantId: backendData.Participant_ID,
    connectionId: backendData.Connection_ID,
    livekitConnected: backendData.LiveKit_Connected,
    audioEnabled: backendData.Audio_Enabled,
    videoEnabled: backendData.Video_Enabled,
    status: backendData.Status
  }),
  
  // Transform frontend meeting data to backend format (Enhanced)
  meetingToBackend: (frontendData) => ({
    Meeting_Name: frontendData.name || frontendData.Meeting_Name,
    Host_ID: frontendData.hostId || frontendData.Host_ID,
    Meeting_Type: frontendData.type || frontendData.Meeting_Type,
    Status: frontendData.status || 'active',
    Is_Recording_Enabled: frontendData.recordingEnabled || false,
    Waiting_Room_Enabled: frontendData.waitingRoomEnabled || false,
    Started_At: frontendData.startTime || frontendData.Started_At,
    Ended_At: frontendData.endTime || frontendData.Ended_At,
    // NEW: LiveKit fields
    LiveKit_Room_Name: frontendData.livekitRoomName,
    LiveKit_Enabled: frontendData.livekitEnabled || true
  }),
  
  // Transform backend meeting data to frontend format (Enhanced)
  meetingToFrontend: (backendData) => ({
    id: backendData.ID,
    meetingId: backendData.Meeting_ID || backendData.ID,
    name: backendData.Meeting_Name,
    hostId: backendData.Host_ID,
    type: backendData.Meeting_Type,
    status: backendData.Status,
    recordingEnabled: backendData.Is_Recording_Enabled,
    waitingRoomEnabled: backendData.Waiting_Room_Enabled,
    startTime: backendData.Started_At,
    endTime: backendData.Ended_At,
    createdAt: backendData.Created_At,
    updatedAt: backendData.Updated_At,
    // NEW: LiveKit fields
    livekitRoomName: backendData.LiveKit_Room_Name,
    livekitRoomSid: backendData.LiveKit_Room_SID,
    livekitUrl: backendData.LiveKit_URL,
    livekitEnabled: backendData.LiveKit_Enabled
  }),
  
  // NEW: LiveKit specific transformers
  livekitParticipantToFrontend: (livekitData) => ({
    id: livekitData.identity,
    participantId: livekitData.sid,
    connectionId: livekitData.sid,
    name: livekitData.name || livekitData.identity,
    audioEnabled: livekitData.isMicrophoneEnabled,
    videoEnabled: livekitData.isCameraEnabled,
    speaking: livekitData.isSpeaking,
    connectionQuality: livekitData.connectionQuality,
    joinTime: livekitData.joinedAt,
    metadata: livekitData.metadata ? JSON.parse(livekitData.metadata) : {},
    tracks: livekitData.tracks || [],
    permissions: livekitData.permissions || {},
    livekitConnected: true,
    status: 'live'
  })
};

// Device Management Constants
export const DEVICE_TYPES = {
  CAMERA: 'videoinput',
  MICROPHONE: 'audioinput',
  SPEAKER: 'audiooutput'
};

export const DEVICE_STATES = {
  ENABLED: 'enabled',
  DISABLED: 'disabled',
  UNAVAILABLE: 'unavailable',
  PERMISSION_DENIED: 'permission_denied'
};

// Meeting Quality Settings (Enhanced for LiveKit)
export const QUALITY_SETTINGS = {
  VIDEO: {
    LOW: { width: 320, height: 240, frameRate: 15, bitrate: 300000 },
    MEDIUM: { width: 640, height: 480, frameRate: 24, bitrate: 1000000 },
    HIGH: { width: 1280, height: 720, frameRate: 30, bitrate: 2500000 },
    HD: { width: 1920, height: 1080, frameRate: 30, bitrate: 5000000 }
  },
  AUDIO: {
    LOW: { sampleRate: 16000, bitrate: 32000 },
    MEDIUM: { sampleRate: 22050, bitrate: 64000 },
    HIGH: { sampleRate: 44100, bitrate: 128000 }
  }
};

// Screen Share Settings
export const SCREEN_SHARE = {
  CONSTRAINTS: {
    video: {
      cursor: 'always',
      displaySurface: 'monitor'
    },
    audio: {
      echoCancellation: true,
      noiseSuppression: true
    }
  },
  OPTIONS: {
    preferCurrentTab: false,
    selfBrowserSurface: 'exclude',
    systemAudio: 'include'
  }
};

// Recording Settings (Enhanced for LiveKit)
export const RECORDING_SETTINGS = {
  FORMATS: {
    WEBM: 'video/webm',
    MP4: 'video/mp4',
    AUDIO_ONLY: 'audio/webm'
  },
  QUALITY: {
    LOW: { videoBitsPerSecond: 1000000 },
    MEDIUM: { videoBitsPerSecond: 2500000 },
    HIGH: { videoBitsPerSecond: 5000000 },
    ULTRA: { videoBitsPerSecond: 10000000 }
  },
  CHUNK_SIZE: 1024 * 1024, // 1MB chunks
  MAX_DURATION: 3 * 60 * 60 * 1000, // 3 hours
  // NEW: LiveKit recording settings
  LIVEKIT_RECORDING: {
    video: {
      codec: 'vp8',
      bitrate: 2500000,
      fps: 30
    },
    audio: {
      codec: 'opus',
      bitrate: 128000,
      sampleRate: 44100
    }
  }
};

// NEW: LiveKit Utility Functions
export const LIVEKIT_UTILS = {
  // Generate room name from meeting ID
  generateRoomName: (meetingId) => `meeting_${meetingId}`,
  
  // Generate participant identity
  generateParticipantIdentity: (userId, timestamp = Date.now()) => `user_${userId}_${timestamp}`,
  
  // Parse participant identity to get user ID
  parseParticipantIdentity: (identity) => {
    const match = identity.match(/^user_(\d+)_\d+$/);
    return match ? match[1] : null;
  },
  
  // Check if LiveKit is supported
  isLiveKitSupported: () => {
    return typeof window !== 'undefined' && 
           window.navigator && 
           window.navigator.mediaDevices &&
           typeof window.navigator.mediaDevices.getUserMedia === 'function';
  },
  
  // Get connection quality string
  getConnectionQualityString: (quality) => {
    const qualityMap = {
      0: 'poor',
      1: 'poor', 
      2: 'good',
      3: 'excellent'
    };
    return qualityMap[quality] || 'unknown';
  },
  
  // Format LiveKit error
  formatLiveKitError: (error) => {
    if (error.name === 'NotAllowedError') {
      return ERROR_MESSAGES.LIVEKIT_PERMISSION_DENIED;
    } else if (error.name === 'NotFoundError') {
      return ERROR_MESSAGES.CAMERA_ERROR;
    } else if (error.message?.includes('token')) {
      return ERROR_MESSAGES.LIVEKIT_TOKEN_EXPIRED;
    } else if (error.message?.includes('room')) {
      return ERROR_MESSAGES.LIVEKIT_ROOM_NOT_FOUND;
    }
    return ERROR_MESSAGES.LIVEKIT_CONNECTION_FAILED;
  }
};

// Utility Functions for Constants (Enhanced)
export const UTILS = {
  // Get device type display name
  getDeviceTypeName: (type) => {
    const names = {
      [DEVICE_TYPES.CAMERA]: 'Camera',
      [DEVICE_TYPES.MICROPHONE]: 'Microphone',
      [DEVICE_TYPES.SPEAKER]: 'Speaker'
    };
    return names[type] || 'Unknown Device';
  },

  // Get meeting status display name  
  getMeetingStatusName: (status) => {
    const names = {
      [MEETING_STATUS.WAITING]: 'Waiting',
      [MEETING_STATUS.ACTIVE]: 'Active',
      [MEETING_STATUS.SCHEDULED]: 'Scheduled',
      [MEETING_STATUS.ENDED]: 'Ended',
      [MEETING_STATUS.CANCELLED]: 'Cancelled'
    };
    return names[status] || 'Unknown';
  },

  // Get participant role display name
  getParticipantRoleName: (role) => {
    const names = {
      [PARTICIPANT_ROLES.HOST]: 'Host',
      [PARTICIPANT_ROLES.CO_HOST]: 'Co-Host',
      [PARTICIPANT_ROLES.PARTICIPANT]: 'Participant'
    };
    return names[role] || 'Participant';
  },

  // NEW: Get participant status display name
  getParticipantStatusName: (status) => {
    const names = {
      [PARTICIPANT_STATUS.LIVE]: 'Live',
      [PARTICIPANT_STATUS.ONLINE]: 'Online',
      [PARTICIPANT_STATUS.OFFLINE]: 'Offline'
    };
    return names[status] || 'Unknown';
  },

  // Format file size
  formatFileSize: (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  },

  // Format duration in seconds to readable time
  formatDuration: (seconds) => {
    if (!seconds || seconds <= 0) return '0:00';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    if (hours > 0) {
      return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    return `${minutes}:${secs.toString().padStart(2, '0')}`;
  },

  // Check if browser supports feature
  isBrowserSupported: (feature) => {
    const userAgent = navigator.userAgent.toLowerCase();
    const browsers = BROWSER_SUPPORT.FEATURES_BY_BROWSER[feature] || [];
    
    for (const browser of browsers) {
      if (userAgent.includes(browser)) {
        return true;
      }
    }
    return false;
  },

  // Get quality setting by name
  getQualitySetting: (type, quality) => {
    return QUALITY_SETTINGS[type.toUpperCase()]?.[quality.toUpperCase()] || null;
  },

  // Transform data between frontend and backend formats
  transformData: (data, direction, type) => {
    const transformer = TRANSFORMERS[`${type}To${direction === 'backend' ? 'Backend' : 'Frontend'}`];
    return transformer ? transformer(data) : data;
  },

  // NEW: LiveKit specific utilities
  ...LIVEKIT_UTILS,

  // Validate meeting name
  validateMeetingName: (name) => {
    if (!name || typeof name !== 'string') return false;
    return name.length >= VALIDATION.MEETING_NAME.MIN_LENGTH && 
           name.length <= VALIDATION.MEETING_NAME.MAX_LENGTH;
  },

  // Validate participant name
  validateParticipantName: (name) => {
    if (!name || typeof name !== 'string') return false;
    return name.length >= VALIDATION.PARTICIPANT_NAME.MIN_LENGTH && 
           name.length <= VALIDATION.PARTICIPANT_NAME.MAX_LENGTH;
  },

  // Check if file type is allowed
  isFileTypeAllowed: (fileType, allowedTypes = FILE_UPLOAD.ALLOWED_TYPES) => {
    return allowedTypes.some(type => {
      if (type.endsWith('/*')) {
        return fileType.startsWith(type.slice(0, -1));
      }
      return fileType === type;
    });
  },

  // NEW: Validate email format
  isValidEmail: (email) => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  },

  // NEW: Validate bulk invite emails
  validateBulkEmails: (emails) => {
    if (!Array.isArray(emails)) return false;
    if (emails.length < VALIDATION.BULK_INVITE_EMAILS.MIN_COUNT) return false;
    if (emails.length > VALIDATION.BULK_INVITE_EMAILS.MAX_COUNT) return false;
    return emails.every(email => UTILS.isValidEmail(email));
  },

  // Get error message by key
  getErrorMessage: (key, defaultMessage = ERROR_MESSAGES.GENERIC_ERROR) => {
    return ERROR_MESSAGES[key] || defaultMessage;
  },

  // Get success message by key  
  getSuccessMessage: (key, defaultMessage = 'Operation completed successfully') => {
    return SUCCESS_MESSAGES[key] || defaultMessage;
  },

  // Check if feature is enabled
  isFeatureEnabled: (feature) => {
    return FEATURES[feature] === true;
  },

  // Get keyboard shortcut by action
  getKeyboardShortcut: (action) => {
    return ACCESSIBILITY.KEYBOARD_SHORTCUTS[action] || null;
  },

  // Format date according to locale
  formatDate: (date, locale = I18N.DEFAULT_LOCALE) => {
    const format = I18N.DATE_FORMATS[locale] || I18N.DATE_FORMATS[I18N.DEFAULT_LOCALE];
    return new Date(date).toLocaleDateString(locale, { 
      year: 'numeric', 
      month: '2-digit', 
      day: '2-digit' 
    });
  },

  // Check if locale is RTL
  isRTLLocale: (locale) => {
    return I18N.RTL_LOCALES.includes(locale);
  },

  // Get theme color class
  getThemeColor: (type) => {
    return THEME_COLORS[type] || THEME_COLORS.primary;
  },

  // Check rate limit
  checkRateLimit: (type, count) => {
    const limits = RATE_LIMITS[type];
    if (!limits) return true;
    
    return count < limits.PER_MINUTE;
  },

  // Generate meeting link
  generateMeetingLink: (meetingId, baseUrl = window.location.origin) => {
    return `${baseUrl}/meeting/${meetingId}`;
  },

  // Parse meeting ID from link
  parseMeetingIdFromLink: (link) => {
    const match = link.match(/\/meeting\/([a-zA-Z0-9-_]+)/);
    return match ? match[1] : null;
  },

  // Get current timestamp
  getCurrentTimestamp: () => {
    return new Date().toISOString();
  },

  // Check if meeting is active
  isMeetingActive: (status) => {
    return status === MEETING_STATUS.ACTIVE;
  },

  // Check if user is host
  isHost: (role) => {
    return role === PARTICIPANT_ROLES.HOST;
  },

  // Check if user can control meeting
  canControlMeeting: (role) => {
    return role === PARTICIPANT_ROLES.HOST || role === PARTICIPANT_ROLES.CO_HOST;
  },

  // NEW: Check if participant is live
  isParticipantLive: (status) => {
    return status === PARTICIPANT_STATUS.LIVE;
  },

  // Get notification icon by type
  getNotificationIcon: (type) => {
    const icons = {
      [NOTIFICATION_TYPES.SUCCESS]: '✅',
      [NOTIFICATION_TYPES.ERROR]: '❌',
      [NOTIFICATION_TYPES.WARNING]: '⚠️',
      [NOTIFICATION_TYPES.INFO]: 'ℹ️'
    };
    return icons[type] || icons[NOTIFICATION_TYPES.INFO];
  },

  // Generate random meeting ID
  generateMeetingId: () => {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
  },

  // Calculate meeting duration
  calculateMeetingDuration: (startTime, endTime) => {
    if (!startTime) return 0;
    const start = new Date(startTime);
    const end = endTime ? new Date(endTime) : new Date();
    return Math.floor((end - start) / 1000); // Duration in seconds
  },

  // Check if time is in the past
  isTimeInPast: (timestamp) => {
    return new Date(timestamp) < new Date();
  },

  // Check if time is in the future
  isTimeInFuture: (timestamp) => {
    return new Date(timestamp) > new Date();
  },

  // Format time remaining
  formatTimeRemaining: (timestamp) => {
    const now = new Date();
    const target = new Date(timestamp);
    const diff = target - now;
    
    if (diff <= 0) return 'Started';
    
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
    
    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    return `${minutes}m`;
  },

  // Debounce function
  debounce: (func, wait) => {
    let timeout;
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  },

  // Throttle function
  throttle: (func, limit) => {
    let inThrottle;
    return function() {
      const args = arguments;
      const context = this;
      if (!inThrottle) {
        func.apply(context, args);
        inThrottle = true;
        setTimeout(() => inThrottle = false, limit);
      }
    };
  }
};