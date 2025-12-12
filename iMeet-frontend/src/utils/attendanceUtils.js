// src/utils/attendanceUtils.js
/**
 * Utility functions for AI attendance tracking
 * Handles face detection, frame processing, and attendance calculations
 */

// Attendance configuration constants
export const ATTENDANCE_CONFIG = {
  // Frame processing
  FRAME_INTERVAL: 2000, // 2 seconds between frames
  MAX_FRAME_SIZE: 5 * 1024 * 1024, // 5MB limit
  OPTIMAL_WIDTH: 640,
  OPTIMAL_HEIGHT: 480,
  JPEG_QUALITY: 0.8,
  
  // Violation thresholds (matching backend)
  EAR_THRESHOLD: 0.22,
  HEAD_YAW_THRESHOLD: 25,
  HAND_FACE_DISTANCE: 0.12,
  FACE_MOVEMENT_THRESHOLD: 0.015,
  
  // Timing thresholds
  INACTIVITY_WARNING_TIME: 300, // 5 minutes
  INACTIVITY_VIOLATION_TIME: 420, // 7 minutes
  VIOLATION_POPUP_TIME: 20, // 20 seconds
  VIOLATION_PENALTY_TIME: 240, // 4 minutes
  BREAK_DURATION: 300, // 5 minutes
  
  // Scoring
  MAX_POPUPS: 4,
  BREAK_PENALTY: 1.0,
  VIOLATION_PENALTY: 1.0,
  INACTIVITY_PENALTY: 1.0
};

// Violation severity levels
export const VIOLATION_SEVERITY = {
  LOW: 1,
  MEDIUM: 2,
  HIGH: 3,
  CRITICAL: 4
};

// Violation types and their severity
export const VIOLATION_TYPES = {
  "Eyes closed": VIOLATION_SEVERITY.MEDIUM,
  "Head turned": VIOLATION_SEVERITY.LOW,
  "Hand near face": VIOLATION_SEVERITY.MEDIUM,
  "Face not visible": VIOLATION_SEVERITY.HIGH,
  "Multiple faces detected": VIOLATION_SEVERITY.CRITICAL,
  "Lying down": VIOLATION_SEVERITY.HIGH,
  "Inactivity detected": VIOLATION_SEVERITY.HIGH
};

/**
 * Video and Canvas Processing Utils
 */

/**
 * Initialize video stream for face detection
 * @param {Object} constraints - Media constraints
 * @returns {Promise<MediaStream>} Video stream
 */
export const initializeVideoStream = async (constraints = {}) => {
  const defaultConstraints = {
    video: {
      width: { ideal: ATTENDANCE_CONFIG.OPTIMAL_WIDTH },
      height: { ideal: ATTENDANCE_CONFIG.OPTIMAL_HEIGHT },
      frameRate: { ideal: 15, max: 30 },
      facingMode: 'user'
    },
    audio: false
  };

  try {
    const mergedConstraints = {
      ...defaultConstraints,
      ...constraints
    };

    const stream = await navigator.mediaDevices.getUserMedia(mergedConstraints);
    
    if (!stream || stream.getVideoTracks().length === 0) {
      throw new Error('No video tracks available');
    }

    return stream;
  } catch (error) {
    console.error('Failed to initialize video stream:', error);
    
    if (error.name === 'NotAllowedError') {
      throw new Error('Camera permission denied. Please allow camera access for attendance tracking.');
    } else if (error.name === 'NotFoundError') {
      throw new Error('No camera found. Please connect a camera for attendance tracking.');
    } else if (error.name === 'NotReadableError') {
      throw new Error('Camera is already in use by another application.');
    }
    
    throw new Error(`Camera initialization failed: ${error.message}`);
  }
};

/**
 * Capture frame from video element
 * @param {HTMLVideoElement} videoElement - Video element
 * @param {HTMLCanvasElement} canvasElement - Canvas element
 * @returns {string|null} Base64 encoded frame
 */
export const captureVideoFrame = (videoElement, canvasElement) => {
  try {
    if (!videoElement || !canvasElement) {
      throw new Error('Video or canvas element not provided');
    }

    // Check if video is ready
    if (videoElement.videoWidth === 0 || videoElement.videoHeight === 0) {
      return null;
    }

    const context = canvasElement.getContext('2d');
    if (!context) {
      throw new Error('Failed to get canvas context');
    }

    // Resize canvas to match video dimensions (optimized)
    const { width, height } = getOptimalDimensions(
      videoElement.videoWidth,
      videoElement.videoHeight
    );

    canvasElement.width = width;
    canvasElement.height = height;

    // Draw video frame to canvas
    context.drawImage(videoElement, 0, 0, width, height);

    // Convert to optimized base64
    return canvasElement.toDataURL('image/jpeg', ATTENDANCE_CONFIG.JPEG_QUALITY);
  } catch (error) {
    console.error('Failed to capture video frame:', error);
    return null;
  }
};

/**
 * Get optimal dimensions maintaining aspect ratio
 * @param {number} originalWidth - Original width
 * @param {number} originalHeight - Original height
 * @returns {Object} Optimal dimensions
 */
export const getOptimalDimensions = (originalWidth, originalHeight) => {
  const maxWidth = ATTENDANCE_CONFIG.OPTIMAL_WIDTH;
  const maxHeight = ATTENDANCE_CONFIG.OPTIMAL_HEIGHT;

  let width = originalWidth;
  let height = originalHeight;

  // Scale down if too large
  if (width > maxWidth) {
    height = (height * maxWidth) / width;
    width = maxWidth;
  }

  if (height > maxHeight) {
    width = (width * maxHeight) / height;
    height = maxHeight;
  }

  return {
    width: Math.round(width),
    height: Math.round(height)
  };
};

/**
 * Validate base64 frame data
 * @param {string} frameData - Base64 frame data
 * @returns {boolean} Is valid
 */
export const validateFrameData = (frameData) => {
  if (!frameData || typeof frameData !== 'string') {
    return false;
  }

  // Check base64 image format
  const base64Regex = /^data:image\/(png|jpeg|jpg);base64,/;
  if (!base64Regex.test(frameData)) {
    return false;
  }

  // Check file size
  const sizeInBytes = getBase64Size(frameData);
  if (sizeInBytes > ATTENDANCE_CONFIG.MAX_FRAME_SIZE) {
    console.warn('Frame too large:', sizeInBytes, 'bytes');
    return false;
  }

  return true;
};

/**
 * Calculate base64 string size in bytes
 * @param {string} base64String - Base64 string
 * @returns {number} Size in bytes
 */
export const getBase64Size = (base64String) => {
  if (!base64String) return 0;
  
  // Remove data URL prefix
  const base64Data = base64String.split(',')[1] || base64String;
  
  // Calculate size: 4 base64 chars = 3 bytes
  const padding = (base64Data.match(/=/g) || []).length;
  return Math.floor((base64Data.length * 3) / 4) - padding;
};

/**
 * Attendance Calculation Utils
 */

/**
 * Calculate attendance percentage based on violations and time
 * @param {Object} sessionData - Session data
 * @returns {number} Attendance percentage
 */
export const calculateAttendancePercentage = (sessionData) => {
  const {
    totalPenalties = 0,
    breakUsed = false,
    sessionDuration = 0,
    activeDuration = 0
  } = sessionData;

  let baseScore = 100;

  // Apply penalties
  baseScore -= totalPenalties;

  // Break penalty
  if (breakUsed) {
    baseScore -= ATTENDANCE_CONFIG.BREAK_PENALTY;
  }

  // Activity ratio penalty
  if (sessionDuration > 0) {
    const activityRatio = activeDuration / sessionDuration;
    if (activityRatio < 0.8) {
      baseScore -= (1 - activityRatio) * 20; // Up to 20% penalty for low activity
    }
  }

  return Math.max(0, Math.min(100, baseScore));
};

/**
 * Calculate engagement score based on interactions and focus
 * @param {Object} engagementData - Engagement data
 * @returns {number} Engagement score
 */
export const calculateEngagementScore = (engagementData) => {
  const {
    violationCount = 0,
    violationSeverityScore = 0,
    activityRatio = 1,
    breakUsed = false
  } = engagementData;

  let baseScore = 100;

  // Violation penalties
  baseScore -= Math.min(violationSeverityScore * 5, 40);

  // Activity penalty
  baseScore -= (1 - activityRatio) * 30;

  // Break penalty
  if (breakUsed) {
    baseScore -= 5;
  }

  return Math.max(0, Math.min(100, baseScore));
};

/**
 * Calculate focus score based on attention and violations
 * @param {Object} focusData - Focus data
 * @returns {number} Focus score
 */
export const calculateFocusScore = (focusData) => {
  const {
    eyesClosedTime = 0,
    headTurnedTime = 0,
    faceNotVisibleTime = 0,
    totalTime = 1,
    handNearFaceCount = 0
  } = focusData;

  let baseScore = 100;

  // Time-based penalties
  baseScore -= (eyesClosedTime / totalTime) * 30;
  baseScore -= (headTurnedTime / totalTime) * 20;
  baseScore -= (faceNotVisibleTime / totalTime) * 40;

  // Count-based penalties
  baseScore -= Math.min(handNearFaceCount * 2, 20);

  return Math.max(0, Math.min(100, baseScore));
};

/**
 * Violation Processing Utils
 */

/**
 * Get violation severity
 * @param {string} violationType - Type of violation
 * @returns {number} Severity level
 */
export const getViolationSeverity = (violationType) => {
  return VIOLATION_TYPES[violationType] || VIOLATION_SEVERITY.LOW;
};

/**
 * Get violation message for display
 * @param {string} violationType - Type of violation
 * @returns {string} User-friendly message
 */
export const getViolationMessage = (violationType) => {
  const messages = {
    "Eyes closed": "Please keep your eyes open and look at the screen",
    "Head turned": "Please face the camera directly",
    "Hand near face": "Please keep your hands away from your face",
    "Face not visible": "Please ensure your face is clearly visible to the camera",
    "Multiple faces detected": "Only you should be visible on camera",
    "Lying down": "Please sit up straight for the session",
    "Inactivity detected": "Please show some movement to indicate you are active"
  };

  return messages[violationType] || "Please correct your position";
};

/**
 * Get helpful tips for violation types
 * @param {string} violationType - Type of violation
 * @returns {Array<string>} Array of tips
 */
export const getViolationTips = (violationType) => {
  const tips = {
    "Eyes closed": [
      "Take a short break if you're feeling tired",
      "Adjust lighting to reduce eye strain",
      "Blink naturally but avoid closing eyes for extended periods"
    ],
    "Head turned": [
      "Position yourself directly in front of the camera",
      "Use a swivel chair to face the screen comfortably",
      "Avoid looking at secondary monitors for extended periods"
    ],
    "Hand near face": [
      "Rest your hands on the desk or in your lap",
      "Use a notepad instead of touching your face when thinking",
      "Be mindful of habitual face-touching gestures"
    ],
    "Face not visible": [
      "Check camera positioning and angle",
      "Ensure adequate lighting on your face",
      "Avoid moving too far from the camera"
    ],
    "Multiple faces detected": [
      "Ensure you're alone in the camera frame",
      "Check for reflections or images in the background",
      "Position camera to show only yourself"
    ],
    "Lying down": [
      "Sit upright in a proper chair",
      "Use cushions for back support if needed",
      "Take regular posture breaks"
    ],
    "Inactivity detected": [
      "Make small movements periodically",
      "Nod or gesture naturally during content",
      "Take notes or interact with the material"
    ]
  };

  return tips[violationType] || ["Maintain proper positioning and engagement"];
};

/**
 * Time and Duration Utils
 */

/**
 * Format duration in seconds to readable string
 * @param {number} seconds - Duration in seconds
 * @returns {string} Formatted duration
 */
export const formatDuration = (seconds) => {
  if (!seconds || seconds <= 0) return '0:00';

  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = Math.floor(seconds % 60);

  if (hours > 0) {
    return `${hours}:${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}`;
  }

  return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
};

/**
 * Get time remaining for break
 * @param {number} breakStartTime - Break start timestamp
 * @returns {number} Seconds remaining
 */
export const getBreakTimeRemaining = (breakStartTime) => {
  if (!breakStartTime) return 0;

  const elapsed = (Date.now() - breakStartTime) / 1000;
  const remaining = ATTENDANCE_CONFIG.BREAK_DURATION - elapsed;

  return Math.max(0, Math.floor(remaining));
};

/**
 * Check if break is over
 * @param {number} breakStartTime - Break start timestamp
 * @returns {boolean} Is break over
 */
export const isBreakOver = (breakStartTime) => {
  return getBreakTimeRemaining(breakStartTime) <= 0;
};

/**
 * Status and Display Utils
 */

/**
 * Get attendance status level
 * @param {number} attendancePercentage - Attendance percentage
 * @param {Array} violations - Current violations
 * @returns {string} Status level
 */
export const getAttendanceStatus = (attendancePercentage, violations = []) => {
  if (violations.length > 0) return 'violation';
  if (attendancePercentage >= 90) return 'excellent';
  if (attendancePercentage >= 80) return 'good';
  if (attendancePercentage >= 60) return 'warning';
  return 'poor';
};

/**
 * Get status color for UI
 * @param {string} status - Status level
 * @returns {string} Color code
 */
export const getStatusColor = (status) => {
  const colors = {
    excellent: '#4caf50',
    good: '#8bc34a',
    warning: '#ff9800',
    poor: '#f44336',
    violation: '#f44336',
    ended: '#666666'
  };

  return colors[status] || colors.good;
};

/**
 * Get status display text
 * @param {string} status - Status level
 * @returns {string} Display text
 */
export const getStatusText = (status) => {
  const texts = {
    excellent: 'Excellent',
    good: 'Good',
    warning: 'Needs Attention',
    poor: 'Critical',
    violation: 'Violation Active',
    ended: 'Session Ended'
  };

  return texts[status] || 'Good';
};

/**
 * Browser and Device Utils
 */

/**
 * Check if browser supports required features
 * @returns {Object} Support status
 */
export const checkBrowserSupport = () => {
  const support = {
    mediaDevices: !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia),
    canvas: !!(document.createElement('canvas').getContext),
    webgl: !!document.createElement('canvas').getContext('webgl'),
    requestAnimationFrame: !!window.requestAnimationFrame
  };

  support.allSupported = Object.values(support).every(Boolean);

  return support;
};

/**
 * Get optimal frame rate based on device performance
 * @returns {number} Recommended frame rate
 */
export const getOptimalFrameRate = () => {
  // Simple performance detection
  if (navigator.hardwareConcurrency >= 8 && navigator.deviceMemory >= 8) {
    return 30; // High-end device
  } else if (navigator.hardwareConcurrency >= 4 && navigator.deviceMemory >= 4) {
    return 20; // Mid-range device
  }
  
  return 15; // Lower-end device
};

/**
 * Debug and Development Utils
 */

/**
 * Log attendance event for debugging
 * @param {string} event - Event type
 * @param {Object} data - Event data
 */
export const logAttendanceEvent = (event, data = {}) => {
  if (process.env.NODE_ENV === 'development') {
    console.log(`[Attendance] ${event}:`, {
      timestamp: new Date().toISOString(),
      ...data
    });
  }
};

/**
 * Create debug overlay for development
 * @param {Object} debugData - Debug information
 * @returns {string} Debug HTML
 */
export const createDebugOverlay = (debugData) => {
  if (process.env.NODE_ENV !== 'development') {
    return '';
  }

  return `
    <div style="
      position: fixed;
      top: 10px;
      right: 10px;
      background: rgba(0,0,0,0.8);
      color: white;
      padding: 10px;
      font-family: monospace;
      font-size: 10px;
      z-index: 9999;
      max-width: 300px;
    ">
      <div>Frame Rate: ${debugData.frameRate || 0} fps</div>
      <div>Processing Time: ${debugData.processingTime || 0} ms</div>
      <div>Violations: ${debugData.violationCount || 0}</div>
      <div>Attendance: ${debugData.attendancePercentage || 0}%</div>
      <div>Last Update: ${new Date(debugData.lastUpdate || Date.now()).toLocaleTimeString()}</div>
    </div>
  `;
};

/**
 * Default export with commonly used functions
 */
export default {
  // Core functions
  initializeVideoStream,
  captureVideoFrame,
  validateFrameData,
  
  // Calculations
  calculateAttendancePercentage,
  calculateEngagementScore,
  calculateFocusScore,
  
  // Violations
  getViolationSeverity,
  getViolationMessage,
  getViolationTips,
  
  // Display
  getAttendanceStatus,
  getStatusColor,
  getStatusText,
  formatDuration,
  
  // Utils
  checkBrowserSupport,
  logAttendanceEvent,
  
  // Constants
  ATTENDANCE_CONFIG,
  VIOLATION_SEVERITY,
  VIOLATION_TYPES
};