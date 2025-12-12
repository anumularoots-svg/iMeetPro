// attendanceAPI.js - UPDATED with Role-Agnostic Continuous Tracking
import axios from 'axios';
import { API_BASE_URL } from '../utils/constants';

// Create attendance-specific axios instance
const attendanceApi = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for authentication
attendanceApi.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
attendanceApi.interceptors.response.use(
  (response) => {
    return response.data;
  },
  (error) => {
    console.error('Attendance API Error:', error);
    
    // Handle specific error cases
    if (error.response?.status === 429) {
      throw new Error('Too many requests. Please slow down.');
    } else if (error.response?.status === 413) {
      throw new Error('Image too large. Please reduce camera quality.');
    } else if (error.response?.status === 503) {
      throw new Error('AI service temporarily unavailable.');
    }
    
    return Promise.reject(error);
  }
);

// Attendance API functions
export const attendanceAPI = {
  /**
   * UPDATED: Start attendance tracking for all roles with role-aware behavior
   * @param {Object} data - Meeting and user data
   * @returns {Promise<Object>} Response from backend
   */
  startTracking: async (data) => {
    try {
      console.log('API: Starting role-agnostic tracking for:', {
        userId: data.userId,
        userRole: data.userRole,
        currentTrackingMode: data.currentTrackingMode,
        isHost: data.isHost,
        isCoHost: data.isCoHost
      });
      
      const response = await attendanceApi.post('/api/attendance/start/', {
        meeting_id: data.meetingId,
        user_id: data.userId,
        user_name: data.userName || `User ${data.userId}`,
        user_role: data.userRole,
        current_tracking_mode: data.currentTrackingMode,
        is_host: data.isHost || false,
        is_cohost: data.isCoHost || false,
        participant_identity: data.participantIdentity,
        camera_enabled: data.cameraEnabled !== false,
        role_history: data.roleHistory || [],
        session_start_time: data.sessionStartTime || Date.now(),
        should_detect_violations: data.shouldDetectViolations !== false
      });
      
      console.log('API: Tracking started successfully for', data.currentTrackingMode, ':', response);
      return response;
    } catch (error) {
      console.error('API: Failed to start attendance tracking:', error);
      throw new Error(error.response?.data?.error || error.message || 'Failed to start tracking');
    }
  },

  /**
   * Stop attendance tracking for any role
   * @param {Object} data - Meeting and user data
   * @returns {Promise<Object>} Response from backend
   */
  stopTracking: async (data) => {
    try {
      console.log('API: Stopping attendance tracking:', data);
      
      const response = await attendanceApi.post('/api/attendance/stop/', {
        meeting_id: data.meetingId,
        user_id: data.userId,
        tracking_mode: data.trackingMode,
        role_history: data.roleHistory || []
      });
      
      console.log('API: Attendance tracking stopped:', response);
      return response;
    } catch (error) {
      console.error('API: Failed to stop attendance tracking:', error);
      throw new Error(error.response?.data?.error || error.message || 'Failed to stop tracking');
    }
  },

  /**
   * UPDATED: Enhanced violation detection with role-aware processing
   * @param {Object} data - Frame data and user info
   * @returns {Promise<Object>} Analysis response
   */
  detectViolations: async (data) => {
    try {
      // Validate essential data
      if (!data.meeting_id || !data.user_id) {
        throw new Error('Missing required meeting_id or user_id for violation detection');
      }

      console.log('API: Processing detection request for:', {
        meeting_id: data.meeting_id,
        user_id: data.user_id,
        has_frame: !!data.frame,
        camera_enabled: data.camera_enabled,
        current_tracking_mode: data.current_tracking_mode,
        should_detect_violations: data.should_detect_violations,
        is_on_break: data.is_on_break,
        is_break_paused: data.is_break_paused
      });

      // UPDATED: Enhanced request data with role and tracking mode info
      const requestData = {
        meeting_id: data.meeting_id,
        user_id: data.user_id,
        frame: data.frame || null,
        camera_enabled: data.camera_enabled || false,
        user_role: data.user_role,
        current_tracking_mode: data.current_tracking_mode,
        is_host: data.is_host || false,
        is_cohost: data.is_cohost || false,
        is_on_break: data.is_on_break || false,
        is_break_paused: data.is_break_paused || false,
        should_detect_violations: data.should_detect_violations !== false,
        role_history: data.role_history || [],
        
        // Warning system state for participants
        warning_count: data.warning_count || 0,
        warnings_exhausted: data.warnings_exhausted || false,
        
        // Detection control flags
        detection_active: data.camera_enabled && (!data.is_on_break || data.is_break_paused),
        skip_violations: data.should_detect_violations === false || (data.is_on_break && !data.is_break_paused),
        
        // Additional context
        frame_info: data.frame_info || (data.frame ? 'captured' : 'camera-disabled'),
        timestamp: Date.now(),
        client_version: '1.0.0'
      };

      // Skip API call for hosts/co-hosts when camera is disabled (save bandwidth)
      if (data.current_tracking_mode !== 'participant' && !data.camera_enabled) {
        console.log('API: Skipping detection - host/co-host with camera disabled');
        return {
          status: 'presence_tracking',
          violations: [],
          attendance_percentage: data.last_attendance_percentage || 100,
          engagement_score: data.last_engagement_score || 100,
          popup: null,
          session_active: true,
          detection_skipped: true,
          tracking_mode: data.current_tracking_mode,
          message: `Presence tracking active for ${data.current_tracking_mode}`
        };
      }

      // Skip API call if participant on break and not paused (save bandwidth)
      if (data.current_tracking_mode === 'participant' && data.is_on_break && !data.is_break_paused) {
        console.log('API: Skipping detection - participant on break and not paused');
        return {
          status: 'on_break',
          violations: [],
          attendance_percentage: data.last_attendance_percentage || 100,
          engagement_score: data.last_engagement_score || 100,
          popup: null,
          session_active: true,
          is_on_break: true,
          detection_skipped: true,
          message: 'Detection paused during break'
        };
      }

      const startTime = Date.now();
      const response = await attendanceApi.post('/api/attendance/detect/', requestData);
      const processingTime = Date.now() - startTime;
      
      console.log(`API: Detection response received in ${processingTime}ms for ${data.current_tracking_mode}:`, {
        status: response.status,
        has_violations: response.violations?.length > 0,
        violations: response.violations,
        attendance_percentage: response.attendance_percentage,
        popup: response.popup,
        tracking_mode: response.tracking_mode
      });
      
      // UPDATED: Enhanced response processing with role awareness
      const processedResponse = {
        ...response,
        // Ensure violations array exists and is appropriate for role
        violations: data.should_detect_violations ? (response.violations || []) : [],
        // Ensure attendance percentage is valid
        attendance_percentage: response.attendance_percentage ?? 100,
        engagement_score: response.engagement_score ?? 100,
        // Add processing metadata
        processing_time_ms: processingTime,
        camera_status: data.camera_enabled ? 'enabled' : 'disabled',
        detection_active: requestData.detection_active,
        tracking_mode: data.current_tracking_mode,
        
        // Continuous violation time tracking (participants only)
        continuous_violation_time: data.should_detect_violations ? (response.continuous_violation_time || 0) : 0,
        
        // Warning system updates (participants only)
        warning_count: data.should_detect_violations ? (response.warning_count || 0) : 0,
        warnings_exhausted: data.should_detect_violations ? (response.warnings_exhausted || false) : false,
        
        // Session status
        session_active: response.session_active !== false,
        
        // Enhanced break information
        is_on_break: data.is_on_break || response.is_on_break || false,
        is_break_paused: data.is_break_paused || response.is_break_paused || false,
        total_break_time_used: response.total_break_time_used || 0,
        break_time_remaining: response.break_time_remaining || 300,
        
        // Detection statistics
        popup_count: response.popup_count || 0,
        total_detections: response.total_detections || 0,
        max_popups: response.max_popups || 4,
        
        // Presence tracking
        total_presence_time: response.total_presence_time || 0,
        session_duration: response.session_duration || 0,
        
        // Role transition information
        role_history: data.role_history || [],
        role_transitions: response.role_transitions || 0,
        
        // Frame analysis info
        frame_processing_count: response.frame_count || 0,
        baseline_established: response.baseline_established || false,
        
        // Real-time status
        timestamp: Date.now(),
        success: true,
        user_role: data.user_role
      };

      // UPDATED: Enhanced logging for different tracking modes
      if (data.current_tracking_mode === 'participant') {
        if (data.is_on_break) {
          if (data.is_break_paused) {
            console.log('API: Participant break paused - detection active:', {
              violations: processedResponse.violations,
              camera_enabled: data.camera_enabled
            });
          } else {
            console.log('API: Participant on break - detection skipped');
          }
        } else if (processedResponse.violations.length > 0) {
          console.log('API: PARTICIPANT VIOLATIONS DETECTED:', {
            violations: processedResponse.violations,
            continuous_time: processedResponse.continuous_violation_time,
            warning_count: processedResponse.warning_count,
            warnings_exhausted: processedResponse.warnings_exhausted,
            attendance_percentage: processedResponse.attendance_percentage
          });
        } else if (data.camera_enabled) {
          console.log('API: Participant - no violations detected, all clear');
        } else {
          console.log('API: Participant - camera disabled, no violation detection');
        }
      } else {
        // Host/Co-host logging
        console.log(`API: ${data.current_tracking_mode.toUpperCase()} presence tracking:`, {
          attendance_percentage: processedResponse.attendance_percentage,
          total_presence_time: processedResponse.total_presence_time,
          camera_enabled: data.camera_enabled,
          violations_disabled: true
        });
      }
      
      return processedResponse;
      
    } catch (error) {
      console.error('API: Violation detection failed:', error);
      
      // Handle specific backend errors properly
      if (error.response?.data) {
        const errorData = error.response.data;
        
        // Handle session closed error (participants only)
        if (errorData.error?.includes('session_closed') || errorData.status === 'session_closed') {
          console.log('API: Session closed by backend due to violations');
          return {
            status: 'session_closed',
            popup: errorData.error || errorData.popup || 'Session ended due to violations',
            penalty: errorData.penalty || 0,
            reason: errorData.reason || 'violation',
            violations: [],
            attendance_percentage: 0,
            session_active: false,
            success: false
          };
        }
        
        // Handle other API errors
        if (errorData.message || errorData.error) {
          throw new Error(errorData.message || errorData.error);
        }
      }
      
      throw new Error(error.response?.data?.error || error.message || 'Violation detection failed');
    }
  },

  /**
   * UPDATED: Enhanced break management with role validation
   * @param {Object} data - Break action data
   * @returns {Promise<Object>} Break response
   */
  pauseResumeBreak: async (data) => {
    try {
      // Block break actions for hosts and co-hosts
      if (data.isHost || data.isCoHost || data.userRole === 'host' || data.userRole === 'co-host' || data.userRole === 'cohost') {
        console.log('API: Blocking break action for host/co-host role');
        return {
          success: false,
          status: 'role_excluded',
          message: 'Break functionality not available for hosts and co-hosts',
          role: data.userRole || (data.isHost ? 'host' : 'co-host')
        };
      }

      console.log(`API: ${data.action === 'pause' ? 'Starting' : 'Ending'} break for participant:`, data);
      
      const response = await attendanceApi.post('/api/attendance/pause-resume/', {
        meeting_id: data.meetingId,
        user_id: data.userId,
        action: data.action,
        user_role: 'participant', // Ensure only participants can use breaks
        tracking_mode: data.trackingMode || 'participant',
        role_history: data.roleHistory || []
      });
      
      console.log(`API: Break ${data.action} successful:`, response);
      
      // Enhanced response with explicit camera control instructions
      const enhancedResponse = {
        ...response,
        // Camera control metadata
        camera_action: data.action === 'pause' ? 'disable' : 'enable',
        should_disable_camera: data.action === 'pause',
        should_enable_camera: data.action === 'resume',
        
        // Break state
        break_active: data.action === 'pause' ? true : false,
        detection_active: data.action === 'pause' ? false : true,
        
        // Time information
        break_duration: response.break_duration || 300,
        break_time_remaining: response.break_time_remaining || 300,
        total_break_time_used: response.total_break_time_used || 0,
        
        // Status
        success: true,
        timestamp: Date.now()
      };
      
      return enhancedResponse;
    } catch (error) {
      console.error(`API: Break ${data.action} failed:`, error);
      
      // Handle specific break errors
      if (error.response?.data?.error?.includes('Break time limit exceeded')) {
        throw new Error('Break time limit exceeded (5 minutes total)');
      } else if (error.response?.data?.error?.includes('Already on break')) {
        throw new Error('Already on break');
      } else if (error.response?.data?.error?.includes('Not currently on break')) {
        throw new Error('Not currently on break');
      }
      
      throw new Error(error.response?.data?.error || error.message || `Failed to ${data.action} break`);
    }
  },

  /**
   * UPDATED: Enhanced status with comprehensive role and transition information
   * @param {string} meetingId - Meeting ID
   * @param {string} userId - User ID
   * @returns {Promise<Object>} Current status with role details
   */
  getStatus: async (meetingId, userId) => {
    try {
      const response = await attendanceApi.get('/api/attendance/status/', {
        params: {
          meeting_id: meetingId,
          user_id: userId
        }
      });
      
      // UPDATED: Enhance response with role-aware camera control suggestions
      const enhancedResponse = {
        ...response,
        // Camera control suggestions based on role and break state
        suggested_camera_state: response.is_on_break && !response.is_break_paused ? 'disabled' : 'enabled',
        suggested_detection_state: response.is_on_break && !response.is_break_paused ? 'paused' : 'active',
        
        // Break eligibility and control (participants only)
        can_take_break: response.current_tracking_mode === 'participant' && 
                       (response.break_time_remaining || 0) > 0 && 
                       !response.is_on_break,
        can_pause_break: response.current_tracking_mode === 'participant' && 
                        response.is_on_break && 
                        !response.is_break_paused,
        can_resume_break: response.current_tracking_mode === 'participant' && 
                         response.is_on_break && 
                         response.is_break_paused,
        can_end_break: response.current_tracking_mode === 'participant' && 
                      response.is_on_break,
        
        // Detection readiness based on role
        detection_ready: response.current_tracking_mode === 'participant' ? 
                        (!response.is_on_break || response.is_break_paused) : 
                        true, // Always ready for presence tracking
        should_capture_frames: !response.is_on_break || response.is_break_paused || 
                              response.current_tracking_mode !== 'participant',
        
        // Time calculations
        break_time_used: 300 - (response.break_time_remaining || 300),
        break_percentage_used: ((300 - (response.break_time_remaining || 300)) / 300) * 100,
        
        // Enhanced break information
        break_status: response.is_on_break ? 
          (response.is_break_paused ? 'paused' : 'active') : 
          'not_on_break',
        
        // Role transition information
        current_tracking_mode: response.current_tracking_mode || 'participant',
        role_history: response.role_history || [],
        role_transitions_count: response.role_transitions_count || 0,
        
        // Warning system status (participants only)
        warning_count: response.current_tracking_mode === 'participant' ? 
                      (response.warning_count || 0) : 0,
        warnings_exhausted: response.current_tracking_mode === 'participant' ? 
                           (response.warnings_exhausted || false) : false,
        max_warnings: response.max_warnings || 4,
        
        // Presence tracking information
        total_presence_time: response.total_presence_time || 0,
        session_duration: response.session_duration || 0,
        presence_percentage: response.presence_percentage || 100,
        
        timestamp: Date.now()
      };
      
      console.log('API: Enhanced attendance status retrieved:', {
        user_id: userId,
        current_tracking_mode: enhancedResponse.current_tracking_mode,
        session_active: enhancedResponse.session_active,
        attendance_percentage: enhancedResponse.attendance_percentage,
        is_on_break: enhancedResponse.is_on_break,
        is_break_paused: enhancedResponse.is_break_paused,
        break_time_remaining: enhancedResponse.break_time_remaining,
        detection_ready: enhancedResponse.detection_ready,
        suggested_camera_state: enhancedResponse.suggested_camera_state,
        role_transitions_count: enhancedResponse.role_transitions_count,
        total_presence_time: enhancedResponse.total_presence_time
      });
      
      return enhancedResponse;
    } catch (error) {
      console.error('API: Failed to get attendance status:', error);
      
      // Handle 404 and other errors gracefully
      if (error.response?.status === 404) {
        return {
          status: 'not_started',
          session_active: false,
          attendance_percentage: 100,
          engagement_score: 100,
          violations: [],
          is_on_break: false,
          is_break_paused: false,
          break_time_remaining: 300,
          total_break_time_used: 0,
          suggested_camera_state: 'enabled',
          suggested_detection_state: 'active',
          detection_ready: true,
          can_take_break: true,
          can_pause_break: false,
          can_resume_break: false,
          can_end_break: false,
          break_status: 'not_on_break',
          current_tracking_mode: 'participant',
          role_history: [],
          role_transitions_count: 0,
          warning_count: 0,
          warnings_exhausted: false,
          max_warnings: 4,
          total_presence_time: 0,
          session_duration: 0,
          presence_percentage: 100,
          message: 'Attendance tracking not started'
        };
      }
      
      throw new Error(error.response?.data?.message || error.message || 'Failed to get status');
    }
  },

  /**
   * Break pause detection (client-side operation)
   * @param {Object} data - Meeting and user data
   * @returns {Promise<Object>} Break pause response
   */
  pauseBreakDetection: async (data) => {
    try {
      console.log('API: Pausing break (enabling detection):', data);
      
      // This is a client-side operation for UI coordination
      return {
        success: true,
        action: 'break_paused',
        message: 'Break paused - detection enabled',
        camera_action: 'enable',
        should_enable_camera: true,
        detection_active: true,
        break_paused: true,
        is_break_paused: true,
        timestamp: Date.now()
      };
    } catch (error) {
      console.error('API: Failed to pause break:', error);
      throw new Error('Failed to pause break detection');
    }
  },

  /**
   * Break resume detection (client-side operation)
   * @param {Object} data - Meeting and user data
   * @returns {Promise<Object>} Break resume response
   */
  resumeBreakDetection: async (data) => {
    try {
      console.log('API: Resuming break (disabling detection):', data);
      
      // This is a client-side operation for UI coordination
      return {
        success: true,
        action: 'break_resumed',
        message: 'Break resumed - detection disabled',
        camera_action: 'disable',
        should_disable_camera: true,
        detection_active: false,
        break_paused: false,
        is_break_paused: false,
        timestamp: Date.now()
      };
    } catch (error) {
      console.error('API: Failed to resume break:', error);
      throw new Error('Failed to resume break detection');
    }
  },

  /**
   * UPDATED: Check comprehensive break eligibility with role awareness
   * @param {string} meetingId - Meeting ID
   * @param {string} userId - User ID
   * @returns {Promise<Object>} Break eligibility info
   */
  checkBreakEligibility: async (meetingId, userId) => {
    try {
      const status = await attendanceAPI.getStatus(meetingId, userId);
      
      return {
        can_take_break: status.current_tracking_mode === 'participant' && 
                       status.break_time_remaining > 0 && 
                       !status.is_on_break,
        can_pause_break: status.current_tracking_mode === 'participant' && 
                        status.is_on_break && 
                        !status.is_break_paused,
        can_resume_break: status.current_tracking_mode === 'participant' && 
                         status.is_on_break && 
                         status.is_break_paused,
        can_end_break: status.current_tracking_mode === 'participant' && 
                      status.is_on_break,
        break_time_remaining: status.break_time_remaining || 0,
        total_break_time_used: status.total_break_time_used || 0,
        max_break_time_allowed: status.max_break_time_allowed || 300,
        is_currently_on_break: status.is_on_break || false,
        is_break_paused: status.is_break_paused || false,
        break_count: status.break_count || 0,
        recommended_camera_action: status.suggested_camera_state || 'enable',
        detection_ready: status.detection_ready || false,
        break_status: status.break_status || 'not_on_break',
        current_tracking_mode: status.current_tracking_mode || 'participant',
        tracking_mode_allows_breaks: status.current_tracking_mode === 'participant'
      };
    } catch (error) {
      console.error('API: Failed to check break eligibility:', error);
      
      // Return safe defaults
      return {
        can_take_break: false, // Default to false for safety
        can_pause_break: false,
        can_resume_break: false,
        can_end_break: false,
        break_time_remaining: 300,
        total_break_time_used: 0,
        max_break_time_allowed: 300,
        is_currently_on_break: false,
        is_break_paused: false,
        break_count: 0,
        recommended_camera_action: 'enable',
        detection_ready: true,
        break_status: 'not_on_break',
        current_tracking_mode: 'participant',
        tracking_mode_allows_breaks: true
      };
    }
  },

  /**
   * UPDATED: Smart break manager with complete camera coordination and role awareness
   */
  smartBreakManager: {
    /**
     * Start a break with automatic camera control (participants only)
     * @param {Object} data - Break request data
     * @param {Function} cameraController - Camera control function
     * @returns {Promise<Object>} Break start result
     */
    startBreak: async (data, cameraController = null) => {
      try {
        console.log('API: Smart break start initiated');
        
        // Step 1: Check eligibility and role
        const eligibility = await attendanceAPI.checkBreakEligibility(data.meetingId, data.userId);
        if (!eligibility.tracking_mode_allows_breaks) {
          throw new Error('Break functionality not available for hosts and co-hosts');
        }
        if (!eligibility.can_take_break) {
          throw new Error('Break not available - time limit exceeded or already on break');
        }
        
        // Step 2: Start break via backend
        const breakResponse = await attendanceAPI.pauseResumeBreak({
          ...data,
          action: 'pause'
        });
        
        // Step 3: Execute camera control if provided
        if (cameraController && breakResponse.should_disable_camera) {
          console.log('API: Automatically disabling camera for break');
          try {
            await cameraController(false);
            breakResponse.camera_disabled = true;
          } catch (cameraError) {
            console.warn('API: Camera disable failed:', cameraError);
            breakResponse.camera_disabled = false;
            breakResponse.camera_error = cameraError.message;
          }
        }
        
        return {
          ...breakResponse,
          smart_break_active: true,
          instructions: {
            message: 'Break started - camera disabled automatically',
            camera_state: 'disabled',
            detection_state: 'paused',
            break_duration: breakResponse.break_time_remaining,
            grace_period: 2000
          }
        };
        
      } catch (error) {
        console.error('API: Smart break start failed:', error);
        throw error;
      }
    },

    /**
     * End a break with automatic camera control (participants only)
     * @param {Object} data - Break end data
     * @param {Function} cameraController - Camera control function
     * @returns {Promise<Object>} Break end result
     */
    endBreak: async (data, cameraController = null) => {
      try {
        console.log('API: Smart break end initiated');
        
        // Step 1: End break via backend
        const breakResponse = await attendanceAPI.pauseResumeBreak({
          ...data,
          action: 'resume'
        });
        
        // Step 2: Execute camera control if provided
        if (cameraController && breakResponse.should_enable_camera) {
          console.log('API: Automatically enabling camera after break');
          try {
            await cameraController(true);
            breakResponse.camera_enabled = true;
          } catch (cameraError) {
            console.warn('API: Camera enable failed:', cameraError);
            breakResponse.camera_enabled = false;
            breakResponse.camera_error = cameraError.message;
          }
        }
        
        return {
          ...breakResponse,
          smart_break_active: false,
          instructions: {
            message: 'Break ended - camera enabled automatically',
            camera_state: 'enabled',
            detection_state: 'active',
            grace_period: '2 seconds before monitoring resumes'
          }
        };
        
      } catch (error) {
        console.error('API: Smart break end failed:', error);
        throw error;
      }
    },

    /**
     * Pause break detection (enable camera during break)
     * @param {Object} data - Break pause data
     * @param {Function} cameraController - Camera control function
     * @returns {Promise<Object>} Break pause result
     */
    pauseBreak: async (data, cameraController = null) => {
      try {
        console.log('API: Smart break pause initiated');
        
        const pauseResponse = await attendanceAPI.pauseBreakDetection(data);
        
        // Enable camera for detection during break pause
        if (cameraController && pauseResponse.should_enable_camera) {
          console.log('API: Enabling camera for break pause');
          try {
            await cameraController(true);
            pauseResponse.camera_enabled = true;
          } catch (cameraError) {
            console.warn('API: Camera enable failed:', cameraError);
            pauseResponse.camera_enabled = false;
            pauseResponse.camera_error = cameraError.message;
          }
        }
        
        return pauseResponse;
        
      } catch (error) {
        console.error('API: Smart break pause failed:', error);
        throw error;
      }
    },

    /**
     * Resume break (disable camera during break)
     * @param {Object} data - Break resume data
     * @param {Function} cameraController - Camera control function
     * @returns {Promise<Object>} Break resume result
     */
    resumeBreak: async (data, cameraController = null) => {
      try {
        console.log('API: Smart break resume initiated');
        
        const resumeResponse = await attendanceAPI.resumeBreakDetection(data);
        
        // Disable camera during break resume
        if (cameraController && resumeResponse.should_disable_camera) {
          console.log('API: Disabling camera for break resume');
          try {
            await cameraController(false);
            resumeResponse.camera_disabled = true;
          } catch (cameraError) {
            console.warn('API: Camera disable failed:', cameraError);
            resumeResponse.camera_disabled = false;
            resumeResponse.camera_error = cameraError.message;
          }
        }
        
        return resumeResponse;
        
      } catch (error) {
        console.error('API: Smart break resume failed:', error);
        throw error;
      }
    }
  },

  /**
   * Get meeting attendance report
   * @param {string} meetingId - Meeting ID
   * @returns {Promise<Object>} Meeting report
   */
  getMeetingReport: async (meetingId) => {
    try {
      console.log('API: Getting meeting attendance report:', meetingId);
      
      const response = await attendanceApi.get(`/api/attendance/report/${meetingId}/`);
      
      console.log('API: Meeting report retrieved');
      return response;
    } catch (error) {
      console.error('API: Failed to get meeting report:', error);
      throw new Error(error.response?.data?.message || error.message || 'Failed to get meeting report');
    }
  },

  /**
   * Health check for attendance service
   * @returns {Promise<Object>} Service status
   */
  healthCheck: async () => {
    try {
      const response = await attendanceApi.get('/api/attendance/health/');
      console.log('API: Attendance service health check passed');
      return response;
    } catch (error) {
      console.error('API: Attendance service health check failed:', error);
      return { status: 'error', message: 'Service unavailable' };
    }
  },

  /**
   * UPDATED: Role-aware utility function to determine tracking mode
   * @param {string} userRole - User role
   * @param {boolean} isHost - Is user a host
   * @param {boolean} isCoHost - Is user a co-host
   * @returns {string} Tracking mode
   */
  determineTrackingMode: (userRole, isHost, isCoHost) => {
    if (isHost || userRole === 'host') return 'host';
    if (isCoHost || userRole === 'co-host' || userRole === 'cohost') return 'cohost';
    return 'participant';
  },

  /**
   * UPDATED: Check if violation detection should be enabled for role
   * @param {string} trackingMode - Current tracking mode
   * @returns {boolean} Should detect violations
   */
  shouldDetectViolations: (trackingMode) => {
    return trackingMode === 'participant';
  },

  /**
   * Get user role information from participant data
   * @param {Object} participant - Participant object
   * @returns {Object} Role information
   */
  getUserRole: (participant) => {
    const metadata = participant.metadata || {};
    const userRole = metadata.role || 'participant';
    const isHost = metadata.role === 'host' || metadata.isHost === true;
    const isCoHost = metadata.role === 'co-host' || metadata.isCoHost === true;
    const trackingMode = attendanceAPI.determineTrackingMode(userRole, isHost, isCoHost);
    
    return {
      userRole,
      isHost,
      isCoHost,
      trackingMode,
      shouldDetectViolations: attendanceAPI.shouldDetectViolations(trackingMode)
    };
  },

  /**
   * Utility: Validate frame data before sending
   * @param {string} frameData - Base64 frame data
   * @returns {boolean} Is valid frame
   */
  validateFrame: (frameData) => {
    if (!frameData || typeof frameData !== 'string') {
      return false;
    }
    
    // Check if it's a valid base64 image
    const base64Regex = /^data:image\/(png|jpeg|jpg);base64,/;
    if (!base64Regex.test(frameData)) {
      return false;
    }
    
    // Check size (should be reasonable for processing)
    const sizeInBytes = (frameData.length * 3) / 4;
    const maxSize = 5 * 1024 * 1024; // 5MB limit
    
    if (sizeInBytes > maxSize) {
      console.warn('Frame too large:', sizeInBytes, 'bytes');
      return false;
    }
    
    return true;
  },

  /**
   * Utility: Convert canvas to optimized base64
   * @param {HTMLCanvasElement} canvas - Canvas element
   * @param {number} quality - JPEG quality (0.1 - 1.0)
   * @returns {string} Optimized base64 string
   */
  optimizeFrame: (canvas, quality = 0.8) => {
    if (!canvas || !canvas.getContext) {
      throw new Error('Invalid canvas element');
    }
    
    // Convert to JPEG with specified quality
    return canvas.toDataURL('image/jpeg', quality);
  },

  /**
   * Utility: Resize canvas for optimal processing
   * @param {HTMLVideoElement} video - Video element
   * @param {HTMLCanvasElement} canvas - Canvas element
   * @param {number} maxWidth - Maximum width
   * @param {number} maxHeight - Maximum height
   */
  resizeFrame: (video, canvas, maxWidth = 640, maxHeight = 480) => {
    const context = canvas.getContext('2d');
    
    let { width, height } = video;
    
    // Calculate new dimensions maintaining aspect ratio
    if (width > maxWidth) {
      height = (height * maxWidth) / width;
      width = maxWidth;
    }
    
    if (height > maxHeight) {
      width = (width * maxHeight) / height;
      height = maxHeight;
    }
    
    canvas.width = width;
    canvas.height = height;
    
    // Draw resized frame
    context.drawImage(video, 0, 0, width, height);
  }
};

export default attendanceAPI;