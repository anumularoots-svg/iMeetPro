// src/utils/performanceConfig.js - Enhanced Configuration for 40-50 participants with Screen Share

export const PERFORMANCE_CONFIG = {
  // Participant limits
  MAX_VIDEO_PARTICIPANTS: 25,        // Maximum participants with video at once
  MAX_TOTAL_PARTICIPANTS: 50,        // Maximum total participants
  PARTICIPANTS_PER_PAGE: 25,         // Participants per page in grid
  
  // Video quality settings
  VIDEO_QUALITY: {
    LOCAL: {
      width: { ideal: 640, max: 960 },
      height: { ideal: 360, max: 540 },
      frameRate: { ideal: 24, max: 30 }
    },
    // ENHANCED: Better screen share quality settings
    SCREEN_SHARE: {
      width: { min: 1280, ideal: 1920, max: 3840 },
      height: { min: 720, ideal: 1080, max: 2160 },
      frameRate: { min: 15, ideal: 30, max: 60 },
      aspectRatio: { ideal: 16/9 }
    },
    // High quality mode for screen share
    SCREEN_SHARE_HIGH: {
      width: 1920,
      height: 1080,
      frameRate: 30,
      bitrate: 4000000  // 4 Mbps
    }
  },
  
  // Throttling and debouncing
  THROTTLE_DELAYS: {
    PARTICIPANT_UPDATE: 500,      // ms
    STATE_UPDATE: 300,            // ms
    STREAM_UPDATE: 100,           // ms
    CONNECTION_CHECK: 1000,       // ms
    PARTICIPANT_SYNC: 5000        // ms
  },
  
  // Connection settings
  CONNECTION: {
    MAX_RETRIES: 3,
    RETRY_DELAY: 1000,           // ms
    CONNECTION_TIMEOUT: 20000,    // ms
    HEARTBEAT_INTERVAL: 30000,   // ms
    RECONNECT_DELAY: 5000        // ms
  },
  
  // Memory management
  MEMORY: {
    MAX_CACHED_STREAMS: 30,
    CACHE_CLEANUP_INTERVAL: 60000,  // ms
    MAX_MESSAGES: 100,               // Chat messages
    MAX_REACTIONS: 10                // Active reactions
  },
  
  // Performance thresholds
  PERFORMANCE: {
    MIN_FPS: 20,
    MAX_MEMORY_USAGE: 80,           // percentage
    RENDER_THROTTLE: 200            // ms
  },
  
  // LiveKit optimized settings
  LIVEKIT_CONFIG: {
    adaptiveStream: true,
    dynacast: true,
    publishDefaults: {
      videoSimulcast: true,
      audioPreset: 'speech',
      // Use H264 for better compatibility and quality
      videoCodec: 'h264',
      stopMicTrackOnMute: false,
      stopVideoTrackOnMute: false,
      // Regular video layers for participants
      videoSimulcastLayers: [
        { 
          width: 160, 
          height: 90, 
          bitrate: 60_000,
          scalabilityMode: 'L1T3'
        },
        { 
          width: 320, 
          height: 180, 
          bitrate: 150_000,
          scalabilityMode: 'L1T3'
        },
        { 
          width: 640, 
          height: 360, 
          bitrate: 400_000,
          scalabilityMode: 'L1T3'
        }
      ],
      // ENHANCED: NO simulcast for screen share to maintain quality
      screenShareSimulcast: false,
      // High bitrate encoding for screen share
      screenShareEncoding: {
        maxBitrate: 4_000_000,  // 4 Mbps for crisp text
        maxFramerate: 30,
        priority: 'high'
      }
    },
    connectOptions: {
      autoSubscribe: true,
      maxRetries: 3,
      peerConnectionTimeout: 15000,
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' }
      ]
    },
    // Screen capture specific settings
    screenCaptureDefaults: {
      resolution: {
        width: 1920,
        height: 1080
      },
      frameRate: 30,
      displaySurface: 'monitor',
      logicalSurface: true,
      cursor: 'always'
    }
  },
  
  // ENHANCED: Screen share specific configuration
  SCREEN_SHARE: {
    // WebRTC encodings for screen share (SVC - Scalable Video Coding)
    ENCODINGS: [
      {
        rid: 'q',  // Quarter quality
        maxBitrate: 500_000,
        maxFramerate: 5,
        scaleResolutionDownBy: 4
      },
      {
        rid: 'h',  // Half quality
        maxBitrate: 1_500_000,
        maxFramerate: 15,
        scaleResolutionDownBy: 2
      },
      {
        rid: 'f',  // Full quality
        maxBitrate: 4_000_000,
        maxFramerate: 30,
        scaleResolutionDownBy: 1
      }
    ],
    // Display media options for getDisplayMedia
    DISPLAY_MEDIA_OPTIONS: {
      video: {
        width: { min: 1280, ideal: 1920, max: 3840 },
        height: { min: 720, ideal: 1080, max: 2160 },
        frameRate: { min: 15, ideal: 30, max: 60 },
        displaySurface: 'monitor',
        logicalSurface: true,
        cursor: 'always',
        resizeMode: 'none'
      },
      audio: {
        echoCancellation: false,
        noiseSuppression: false,
        autoGainControl: false,
        sampleRate: 48000,
        sampleSize: 16,
        channelCount: 2
      },
      preferCurrentTab: false,
      selfBrowserSurface: 'exclude',
      systemAudio: 'include',
      surfaceSwitching: 'include',
      monitorTypeSurfaces: 'include'
    },
    // Constraints for applying to track after capture
    TRACK_CONSTRAINTS: {
      width: { min: 1280, ideal: 1920, max: 3840 },
      height: { min: 720, ideal: 1080, max: 2160 },
      frameRate: { min: 15, ideal: 30, max: 60 },
      aspectRatio: { ideal: 16/9 }
    },
    // Publish options for LiveKit
    PUBLISH_OPTIONS: {
      simulcast: false,  // Disable simulcast for quality
      videoEncoding: {
        maxBitrate: 4_000_000,
        maxFramerate: 30,
        priority: 'high'
      },
      degradationPreference: 'maintain-resolution',
      scalabilityMode: 'L3T3'  // 3 spatial, 3 temporal layers
    }
  },
  
  // Grid layout configurations
  GRID_LAYOUTS: {
    COMPACT: { cols: 7, rows: 7, max: 49 },
    COMFORTABLE: { cols: 5, rows: 5, max: 25 },
    FOCUS: { cols: 4, rows: 4, max: 16 },
    MINIMAL: { cols: 3, rows: 3, max: 9 }
  },
  
  // Auto quality adjustment thresholds
  AUTO_QUALITY: {
    HIGH_THRESHOLD: 15,    // participants
    MEDIUM_THRESHOLD: 25,  // participants
    LOW_THRESHOLD: 35      // participants
  },
  
  // Network quality thresholds
  NETWORK_QUALITY: {
    EXCELLENT: { minBandwidth: 5000, maxLatency: 50, maxPacketLoss: 0.5 },
    GOOD: { minBandwidth: 2500, maxLatency: 100, maxPacketLoss: 1 },
    MEDIUM: { minBandwidth: 1000, maxLatency: 200, maxPacketLoss: 2 },
    POOR: { minBandwidth: 500, maxLatency: 400, maxPacketLoss: 5 }
  }
};

// Helper functions for performance optimization

export const getOptimalLayout = (participantCount) => {
  if (participantCount <= 9) return PERFORMANCE_CONFIG.GRID_LAYOUTS.MINIMAL;
  if (participantCount <= 16) return PERFORMANCE_CONFIG.GRID_LAYOUTS.FOCUS;
  if (participantCount <= 25) return PERFORMANCE_CONFIG.GRID_LAYOUTS.COMFORTABLE;
  return PERFORMANCE_CONFIG.GRID_LAYOUTS.COMPACT;
};

export const getVideoQuality = (participantCount, isScreenShare = false) => {
  // Always use high quality for screen share
  if (isScreenShare) {
    return PERFORMANCE_CONFIG.VIDEO_QUALITY.SCREEN_SHARE_HIGH;
  }
  
  // Adaptive quality for regular video based on participant count
  if (participantCount <= PERFORMANCE_CONFIG.AUTO_QUALITY.HIGH_THRESHOLD) {
    return {
      width: { ideal: 1280, max: 1920 },
      height: { ideal: 720, max: 1080 },
      frameRate: { ideal: 30, max: 30 }
    };
  }
  if (participantCount <= PERFORMANCE_CONFIG.AUTO_QUALITY.MEDIUM_THRESHOLD) {
    return PERFORMANCE_CONFIG.VIDEO_QUALITY.LOCAL;
  }
  return {
    width: { ideal: 320, max: 640 },
    height: { ideal: 180, max: 360 },
    frameRate: { ideal: 15, max: 24 }
  };
};

export const shouldPaginate = (participantCount) => {
  return participantCount > PERFORMANCE_CONFIG.MAX_VIDEO_PARTICIPANTS;
};

export const calculatePagesNeeded = (participantCount) => {
  return Math.ceil(participantCount / PERFORMANCE_CONFIG.PARTICIPANTS_PER_PAGE);
};

// Get optimal screen share settings based on network quality
export const getScreenShareSettings = (networkQuality = 'good') => {
  const baseSettings = { ...PERFORMANCE_CONFIG.SCREEN_SHARE.PUBLISH_OPTIONS };
  
  switch (networkQuality) {
    case 'excellent':
      baseSettings.videoEncoding.maxBitrate = 6_000_000;  // 6 Mbps
      baseSettings.videoEncoding.maxFramerate = 60;
      break;
    case 'good':
      baseSettings.videoEncoding.maxBitrate = 4_000_000;  // 4 Mbps
      baseSettings.videoEncoding.maxFramerate = 30;
      break;
    case 'medium':
      baseSettings.videoEncoding.maxBitrate = 2_000_000;  // 2 Mbps
      baseSettings.videoEncoding.maxFramerate = 24;
      break;
    case 'poor':
      baseSettings.videoEncoding.maxBitrate = 1_000_000;  // 1 Mbps
      baseSettings.videoEncoding.maxFramerate = 15;
      break;
    default:
      break;
  }
  
  return baseSettings;
};

// Memory cleanup utility
export const cleanupMemory = () => {
  if (global.gc) {
    global.gc();
  }
  
  // Clear image caches
  const images = document.getElementsByTagName('img');
  for (let img of images) {
    if (!img.isConnected) {
      img.src = '';
    }
  }
  
  // Clear video elements not in use
  const videos = document.getElementsByTagName('video');
  for (let video of videos) {
    if (!video.isConnected && video.srcObject) {
      video.srcObject.getTracks().forEach(track => track.stop());
      video.srcObject = null;
    }
  }
};

// Performance monitoring utility
export const monitorPerformance = (callback) => {
  let frameCount = 0;
  let lastCheck = Date.now();
  
  const checkPerformance = () => {
    frameCount++;
    const now = Date.now();
    const elapsed = now - lastCheck;
    
    if (elapsed >= 1000) {
      const fps = frameCount;
      frameCount = 0;
      lastCheck = now;
      
      const metrics = {
        fps,
        timestamp: now,
        memory: null
      };
      
      if (performance.memory) {
        metrics.memory = {
          used: performance.memory.usedJSHeapSize,
          limit: performance.memory.jsHeapSizeLimit,
          percentage: (performance.memory.usedJSHeapSize / performance.memory.jsHeapSizeLimit) * 100
        };
      }
      
      callback(metrics);
    }
    
    requestAnimationFrame(checkPerformance);
  };
  
  const animationId = requestAnimationFrame(checkPerformance);
  
  return () => cancelAnimationFrame(animationId);
};

// Network quality monitoring
export const monitorNetworkQuality = async () => {
  if (!navigator.connection) {
    return 'unknown';
  }
  
  const connection = navigator.connection;
  const downlink = connection.downlink || 0;  // Mbps
  const rtt = connection.rtt || 0;  // ms
  
  if (downlink >= 5 && rtt <= 50) return 'excellent';
  if (downlink >= 2.5 && rtt <= 100) return 'good';
  if (downlink >= 1 && rtt <= 200) return 'medium';
  return 'poor';
};

// Optimize media constraints based on device capabilities
export const getOptimizedMediaConstraints = async (isScreenShare = false) => {
  if (isScreenShare) {
    return PERFORMANCE_CONFIG.SCREEN_SHARE.DISPLAY_MEDIA_OPTIONS;
  }
  
  // Check device capabilities
  const devices = await navigator.mediaDevices.enumerateDevices();
  const videoDevices = devices.filter(d => d.kind === 'videoinput');
  
  // Use lower quality if multiple video devices (likely mobile)
  if (videoDevices.length > 1) {
    return {
      video: {
        width: { ideal: 640 },
        height: { ideal: 360 },
        frameRate: { ideal: 24 }
      },
      audio: true
    };
  }
  
  return {
    video: PERFORMANCE_CONFIG.VIDEO_QUALITY.LOCAL,
    audio: true
  };
};

export default PERFORMANCE_CONFIG;