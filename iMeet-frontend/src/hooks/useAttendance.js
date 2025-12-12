// useAttendance.js - FIXED with Immediate Warning System
import { useState, useCallback, useRef, useEffect } from 'react';
import { attendanceAPI } from '../services/attendanceAPI';

/**
 * Custom hook for managing AI attendance tracking with immediate warning system
 * @param {string} meetingId - Meeting ID
 * @param {string} userId - User ID  
 * @param {string} userName - User display name
 * @param {string} userRole - User role (participant, host, co-host)
 * @param {boolean} isHost - Is user a host
 * @param {boolean} isCoHost - Is user a co-host
 * @returns {Object} Attendance management functions and state
 */
export const useAttendance = (meetingId, userId, userName, userRole = 'participant', isHost = false, isCoHost = false) => {
  
  // Determine tracking mode with role awareness
  const determineTrackingMode = useCallback(() => {
    if (isHost || userRole === 'host') return 'host';
    if (isCoHost || userRole === 'co-host' || userRole === 'cohost') return 'cohost';
    return 'participant';
  }, [isHost, isCoHost, userRole]);

  const [currentTrackingMode, setCurrentTrackingMode] = useState(determineTrackingMode());
  
  console.log('useAttendance initialized with immediate warning system:', {
    userId,
    userRole,
    isHost,
    isCoHost,
    currentTrackingMode
  });

  // State - always initialized, behavior changes based on tracking mode
  const [isTracking, setIsTracking] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [attendanceData, setAttendanceData] = useState({
    attendancePercentage: 100,
    engagementScore: 100,
    violations: [],
    popupCount: 0,
    maxPopups: 4,
    breakUsed: false,
    sessionActive: true,
    sessionStats: {},
    totalPresenceTime: 0,
    roleHistory: []
  });

  // Role transition tracking
  const [roleHistory, setRoleHistory] = useState([]);
  const [sessionStartTime] = useState(Date.now());
  const roleHistoryRef = useRef([]);

  // Break management state - conditional based on tracking mode
  const [breakState, setBreakState] = useState({
    isOnBreak: false,
    isPaused: false,
    timeRemaining: 300,
    timeUsed: 0,
    canTakeBreak: currentTrackingMode === 'participant',
    canPauseBreak: false,
    canResumeBreak: false,
    canEndBreak: false
  });

  // Camera state management
  const [cameraState, setCameraState] = useState({
    enabled: false,
    ready: false,
    error: null,
    shouldBeEnabled: true
  });

  // FIXED: Enhanced warning system state with immediate detection
  const [warningSystem, setWarningSystem] = useState({
    warningCount: 0,
    warningsExhausted: false,
    postWarningViolationStart: null,
    violationStartTime: null,
    isInViolation: false,
    continuousViolationTime: 0,
    firstWarningGiven: false, // NEW: Track if first immediate warning was given
    lastWarningTime: 0,       // NEW: Track timing of warnings
    warningTimer: null        // NEW: Timer for 20-second intervals
  });

  // Refs for tracking state
  const trackingSessionRef = useRef(null);
  const lastStatusCheckRef = useRef(0);
  const cameraControllerRef = useRef(null);
  const warningTimerRef = useRef(null); // NEW: Timer ref for warning intervals

  // Monitor role changes and handle transitions
  useEffect(() => {
    const newMode = determineTrackingMode();
    if (newMode !== currentTrackingMode) {
      console.log(`Role transition detected: ${currentTrackingMode} -> ${newMode}`);
      
      // Record role change
      const roleChange = {
        fromRole: currentTrackingMode,
        toRole: newMode,
        timestamp: Date.now(),
        attendanceAtTransition: attendanceData.attendancePercentage
      };

      roleHistoryRef.current.push(roleChange);
      setRoleHistory([...roleHistoryRef.current]);

      // Update tracking mode
      setCurrentTrackingMode(newMode);

      // Clear warnings if transitioning away from participant
      if (newMode !== 'participant') {
        setWarningSystem({
          warningCount: 0,
          warningsExhausted: false,
          postWarningViolationStart: null,
          violationStartTime: null,
          isInViolation: false,
          continuousViolationTime: 0,
          firstWarningGiven: false,
          lastWarningTime: 0,
          warningTimer: null
        });

        // Clear any active warning timer
        if (warningTimerRef.current) {
          clearTimeout(warningTimerRef.current);
          warningTimerRef.current = null;
        }
      }

      // Update break availability
      setBreakState(prev => ({
        ...prev,
        canTakeBreak: newMode === 'participant' && prev.timeRemaining > 0 && !prev.isOnBreak
      }));

      console.log('Role transition completed:', {
        newMode,
        roleHistory: roleHistoryRef.current
      });
    }
  }, [determineTrackingMode, currentTrackingMode, attendanceData.attendancePercentage]);

  /**
   * Set camera controller function from AttendanceTracker
   * @param {Function} controllerFn - Camera control function
   */
  const setCameraController = useCallback((controllerFn) => {
    cameraControllerRef.current = controllerFn;
  }, []);

  /**
   * Clear error state
   */
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  /**
   * Start attendance tracking for all roles with immediate warning capability
   * @param {Object} options - Additional options
   * @returns {Promise<boolean>} Success status
   */
  const startTracking = useCallback(async (options = {}) => {
    if (!meetingId || !userId) {
      setError('Missing meeting ID or user ID');
      return false;
    }

    if (isTracking) {
      console.log('Attendance tracking already active');
      return true;
    }

    try {
      setIsLoading(true);
      setError(null);

      console.log(`Starting ${currentTrackingMode} tracking with immediate warning system for user:`, userId);

      // Enhanced tracking start with immediate warning capability
      const response = await attendanceAPI.startTracking({
        meetingId,
        userId,
        userName,
        userRole: userRole,
        currentTrackingMode: currentTrackingMode,
        isHost: isHost,
        isCoHost: isCoHost,
        cameraEnabled: true,
        roleHistory: roleHistoryRef.current,
        sessionStartTime: sessionStartTime,
        shouldDetectViolations: currentTrackingMode === 'participant',
        immediateWarningEnabled: true, // NEW: Enable immediate warning system
        warningIntervalSeconds: 20,    // NEW: Set 20-second intervals for subsequent warnings
        ...options
      });

      if (response.success || response.status === 'started' || response.status !== 'error') {
        setIsTracking(true);
        setCameraState(prev => ({ 
          ...prev, 
          enabled: true, 
          shouldBeEnabled: true,
          error: null
        }));
        
        // FIXED: Reset warning system for new session
        setWarningSystem({
          warningCount: 0,
          warningsExhausted: false,
          postWarningViolationStart: null,
          violationStartTime: null,
          isInViolation: false,
          continuousViolationTime: 0,
          firstWarningGiven: false,
          lastWarningTime: 0,
          warningTimer: null
        });
        
        trackingSessionRef.current = {
          startTime: Date.now(),
          meetingId,
          userId,
          trackingMode: currentTrackingMode
        };

        console.log(`${currentTrackingMode} tracking started successfully with immediate warning system`);
        return true;
      } else {
        throw new Error(response.message || 'Failed to start tracking');
      }
    } catch (error) {
      console.error('Failed to start attendance tracking:', error);
      setError(error.message);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, userId, userName, userRole, currentTrackingMode, isHost, isCoHost, sessionStartTime, isTracking]);

  /**
   * Stop attendance tracking with complete cleanup
   * @returns {Promise<boolean>} Success status
   */
  const stopTracking = useCallback(async () => {
    if (!isTracking || !trackingSessionRef.current) {
      return true;
    }

    try {
      setIsLoading(true);
      setError(null);

      // Clear any active warning timer
      if (warningTimerRef.current) {
        clearTimeout(warningTimerRef.current);
        warningTimerRef.current = null;
      }

      await attendanceAPI.stopTracking({
        meetingId,
        userId,
        trackingMode: currentTrackingMode,
        roleHistory: roleHistoryRef.current
      });

      setIsTracking(false);
      setCameraState(prev => ({ 
        ...prev, 
        enabled: false, 
        shouldBeEnabled: false,
        ready: false
      }));
      setBreakState({
        isOnBreak: false,
        isPaused: false,
        timeRemaining: 300,
        timeUsed: 0,
        canTakeBreak: currentTrackingMode === 'participant',
        canPauseBreak: false,
        canResumeBreak: false,
        canEndBreak: false
      });
      
      // Reset warning system
      setWarningSystem({
        warningCount: 0,
        warningsExhausted: false,
        postWarningViolationStart: null,
        violationStartTime: null,
        isInViolation: false,
        continuousViolationTime: 0,
        firstWarningGiven: false,
        lastWarningTime: 0,
        warningTimer: null
      });
      
      trackingSessionRef.current = null;
      
      console.log('Attendance tracking stopped successfully');
      return true;
    } catch (error) {
      console.error('Failed to stop attendance tracking:', error);
      setError(error.message);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [isTracking, meetingId, userId, currentTrackingMode]);

  /**
   * FIXED: Enhanced violation detection with immediate warning system
   * @param {string} frameData - Base64 encoded frame
   * @returns {Promise<Object|null>} Detection results
   */
  const detectViolations = useCallback(async (frameData) => {
    if (!isTracking) {
      return null;
    }

    // Process frames for all roles, but apply different rules
    if (currentTrackingMode !== 'participant') {
      console.log(`Processing frame for ${currentTrackingMode} - presence tracking only`);
      
      // For hosts/co-hosts: basic presence tracking without violations
      try {
        const response = await attendanceAPI.detectViolations({
          meeting_id: meetingId,
          user_id: userId,
          frame: frameData,
          camera_enabled: cameraState.enabled,
          user_role: userRole,
          current_tracking_mode: currentTrackingMode,
          is_host: isHost,
          is_cohost: isCoHost,
          is_on_break: breakState.isOnBreak,
          is_break_paused: breakState.isPaused,
          should_detect_violations: false,
          role_history: roleHistoryRef.current,
          last_attendance_percentage: attendanceData.attendancePercentage,
          last_engagement_score: attendanceData.engagementScore
        });

        // Update presence data without violations
        if (response.attendance_percentage !== undefined) {
          setAttendanceData(prev => ({
            ...prev,
            attendancePercentage: response.attendance_percentage,
            engagementScore: response.engagement_score || prev.engagementScore,
            violations: [], // Always empty for hosts/co-hosts
            totalPresenceTime: response.total_presence_time || prev.totalPresenceTime,
            sessionStats: response.session_stats || prev.sessionStats
          }));
        }

        return {
          ...response,
          violations: [],
          detection_skipped: false,
          tracking_mode: currentTrackingMode,
          message: `Presence tracking active for ${currentTrackingMode}`
        };

      } catch (error) {
        console.error('Presence tracking failed for', currentTrackingMode, ':', error);
        setError(error.message);
        return null;
      }
    }

    // PARTICIPANT VIOLATION DETECTION WITH IMMEDIATE WARNING SYSTEM
    // Skip detection if on break and not paused
    if (breakState.isOnBreak && !breakState.isPaused) {
      console.log('Skipping violation detection - on break and not paused');
      return {
        status: 'on_break',
        violations: [],
        detection_skipped: true,
        message: 'Detection paused during break'
      };
    }

    // Validate frame data only if camera should be enabled
    if (cameraState.shouldBeEnabled && frameData && !attendanceAPI.validateFrame(frameData)) {
      setError('Invalid frame data');
      return null;
    }

    try {
      const response = await attendanceAPI.detectViolations({
        meeting_id: meetingId,
        user_id: userId,
        frame: frameData,
        camera_enabled: cameraState.enabled,
        user_role: userRole,
        current_tracking_mode: currentTrackingMode,
        is_host: isHost,
        is_cohost: isCoHost,
        is_on_break: breakState.isOnBreak,
        is_break_paused: breakState.isPaused,
        should_detect_violations: true,
        role_history: roleHistoryRef.current,
        
        // FIXED: Enhanced warning system parameters
        warning_count: warningSystem.warningCount,
        warnings_exhausted: warningSystem.warningsExhausted,
        first_warning_given: warningSystem.firstWarningGiven,
        last_warning_time: warningSystem.lastWarningTime,
        immediate_warning_enabled: true,
        warning_interval_seconds: 20,
        
        last_attendance_percentage: attendanceData.attendancePercentage,
        last_engagement_score: attendanceData.engagementScore
      });

      // FIXED: Process response with immediate warning logic
      if (response.violations && response.violations.length > 0) {
        console.log('🚨 VIOLATIONS DETECTED - Processing with immediate warning system:', response.violations);
        
        const currentTime = Date.now();
        
        // IMMEDIATE WARNING LOGIC
        if (!warningSystem.isInViolation) {
          // NEW VIOLATION SESSION - IMMEDIATE FIRST WARNING
          console.log('🔴 NEW VIOLATION SESSION - TRIGGERING IMMEDIATE WARNING');
          
          const newWarningSystem = {
            ...warningSystem,
            isInViolation: true,
            violationStartTime: currentTime,
            continuousViolationTime: 0
          };
          
          // IMMEDIATE FIRST WARNING (no delay)
          if (warningSystem.warningCount < 4 && !warningSystem.warningsExhausted) {
            const newWarningCount = warningSystem.warningCount + 1;
            
            newWarningSystem.warningCount = newWarningCount;
            newWarningSystem.firstWarningGiven = true;
            newWarningSystem.lastWarningTime = currentTime;
            
            console.log(`⚠️ IMMEDIATE WARNING ${newWarningCount}/4 - NO DELAY`);
            
            // Set up 20-second timer for subsequent warnings if not exhausted
            if (newWarningCount < 4) {
              console.log('⏰ Setting up 20-second timer for next warning');
              
              if (warningTimerRef.current) {
                clearTimeout(warningTimerRef.current);
              }
              
              warningTimerRef.current = setTimeout(() => {
                // This will be handled by the next violation detection cycle
                console.log('⏰ 20-second timer expired - next warning ready');
              }, 20000);
            } else {
              console.log('🔴 ALL WARNINGS EXHAUSTED AFTER IMMEDIATE WARNING');
              newWarningSystem.warningsExhausted = true;
              newWarningSystem.postWarningViolationStart = currentTime;
            }
          }
          
          setWarningSystem(newWarningSystem);
          
        } else {
          // ONGOING VIOLATION - check for 20-second interval warnings
          const violationDuration = Math.floor((currentTime - warningSystem.violationStartTime) / 1000);
          const timeSinceLastWarning = currentTime - warningSystem.lastWarningTime;
          
          // Check if 20 seconds have passed since last warning
          if (timeSinceLastWarning >= 20000 && 
              warningSystem.warningCount < 4 && 
              !warningSystem.warningsExhausted &&
              warningSystem.firstWarningGiven) {
            
            const newWarningCount = warningSystem.warningCount + 1;
            
            console.log(`⚠️ 20-SECOND INTERVAL WARNING ${newWarningCount}/4`);
            
            setWarningSystem(prev => ({
              ...prev,
              warningCount: newWarningCount,
              lastWarningTime: currentTime,
              warningsExhausted: newWarningCount >= 4,
              postWarningViolationStart: newWarningCount >= 4 ? currentTime : prev.postWarningViolationStart
            }));
            
            if (newWarningCount >= 4) {
              console.log('🔴 ALL WARNINGS EXHAUSTED');
            } else {
              // Set up next 20-second timer
              console.log('⏰ Setting up next 20-second timer');
              if (warningTimerRef.current) {
                clearTimeout(warningTimerRef.current);
              }
              warningTimerRef.current = setTimeout(() => {
                console.log('⏰ Next 20-second timer expired');
              }, 20000);
            }
          }
          
          // Update continuous violation time
          setWarningSystem(prev => ({
            ...prev,
            continuousViolationTime: violationDuration
          }));
        }
        
      } else {
        // NO VIOLATIONS - clear violation state
        if (warningSystem.isInViolation) {
          console.log('✅ Violations cleared - resetting violation state');
          
          // Clear warning timer
          if (warningTimerRef.current) {
            clearTimeout(warningTimerRef.current);
            warningTimerRef.current = null;
          }
          
          setWarningSystem(prev => ({
            ...prev,
            isInViolation: false,
            violationStartTime: null,
            continuousViolationTime: 0,
            postWarningViolationStart: null
            // Keep warningCount and warningsExhausted for session continuity
          }));
        }
      }

      // Update local attendance data
      if (response.attendance_percentage !== undefined) {
        setAttendanceData(prev => ({
          ...prev,
          attendancePercentage: response.attendance_percentage,
          engagementScore: response.engagement_score || prev.engagementScore,
          violations: response.violations || prev.violations,
          popupCount: warningSystem.warningCount,
          sessionStats: response.session_stats || prev.sessionStats,
          totalPresenceTime: response.total_presence_time || prev.totalPresenceTime
        }));
      }

      // Update break state from response
      if (response.is_on_break !== undefined) {
        setBreakState(prev => ({
          ...prev,
          isOnBreak: response.is_on_break,
          isPaused: response.is_break_paused || false,
          timeRemaining: response.break_time_remaining || prev.timeRemaining,
          timeUsed: response.total_break_time_used || prev.timeUsed
        }));
      }

      // Handle session closure for participants only
      if (response.status === 'session_closed') {
        setIsTracking(false);
        setAttendanceData(prev => ({ ...prev, sessionActive: false }));
        setCameraState(prev => ({ ...prev, enabled: false, shouldBeEnabled: false }));
        setError('Session ended due to violations');
      }

      return {
        ...response,
        warningSystem: warningSystem, // Include warning system state in response
        immediateWarningActive: true
      };
      
    } catch (error) {
      console.error('Violation detection failed:', error);
      setError(error.message);
      return null;
    }
  }, [
    isTracking, 
    currentTrackingMode, 
    meetingId, 
    userId, 
    breakState.isOnBreak, 
    breakState.isPaused, 
    cameraState.enabled, 
    cameraState.shouldBeEnabled, 
    attendanceData.attendancePercentage, 
    attendanceData.engagementScore, 
    userRole, 
    isHost, 
    isCoHost,
    warningSystem
  ]);

  // Include all other methods from original code (takeBreak, pauseBreak, etc.)
  // ... [Rest of break management methods remain the same] ...

  /**
   * Enhanced status with immediate warning system information
   * @param {boolean} forceRefresh - Force refresh from server
   * @returns {Promise<Object|null>} Current status
   */
  const getStatus = useCallback(async (forceRefresh = false) => {
    const now = Date.now();
    
    // Throttle status checks (max once every 5 seconds)
    if (!forceRefresh && (now - lastStatusCheckRef.current) < 5000) {
      return { 
        ...attendanceData, 
        ...breakState, 
        ...cameraState, 
        trackingMode: currentTrackingMode,
        roleHistory: roleHistoryRef.current,
        warningSystem: warningSystem
      };
    }

    try {
      setIsLoading(true);
      setError(null);

      const status = await attendanceAPI.getStatus(meetingId, userId);
      
      if (status) {
        // Update attendance data
        setAttendanceData(prev => ({
          ...prev,
          attendancePercentage: status.attendance_percentage || prev.attendancePercentage,
          engagementScore: status.engagement_score || prev.engagementScore,
          violations: currentTrackingMode === 'participant' ? (status.violations || prev.violations) : [],
          popupCount: status.popup_count || prev.popupCount,
          maxPopups: status.max_popups || prev.maxPopups,
          breakUsed: status.break_used || prev.breakUsed,
          sessionActive: status.session_active !== false,
          sessionStats: status.session_stats || prev.sessionStats,
          totalPresenceTime: status.total_presence_time || prev.totalPresenceTime
        }));

        // Update break state from server
        setBreakState(prev => ({
          ...prev,
          isOnBreak: status.is_on_break || false,
          isPaused: status.is_break_paused || false,
          timeRemaining: status.break_time_remaining || prev.timeRemaining,
          timeUsed: status.total_break_time_used || prev.timeUsed,
          canTakeBreak: currentTrackingMode === 'participant' && (status.can_take_break || false),
          canPauseBreak: status.can_pause_break || false,
          canResumeBreak: status.can_resume_break || false,
          canEndBreak: status.can_end_break || false
        }));

        // Update warning system for participants with immediate warning capability
        if (currentTrackingMode === 'participant') {
          setWarningSystem(prev => ({
            ...prev,
            warningCount: status.warning_count || prev.warningCount,
            warningsExhausted: status.warnings_exhausted || prev.warningsExhausted,
            continuousViolationTime: status.continuous_violation_time || prev.continuousViolationTime,
            // Maintain immediate warning system state
            firstWarningGiven: prev.firstWarningGiven,
            lastWarningTime: prev.lastWarningTime
          }));
        }

        // Update camera state based on server suggestions
        setCameraState(prev => ({
          ...prev,
          shouldBeEnabled: status.suggested_camera_state === 'enabled',
          ready: status.detection_ready || false
        }));

        // Update tracking state based on server status
        const serverIsTracking = status.session_active && (!status.is_on_break || currentTrackingMode !== 'participant');
        if (isTracking !== serverIsTracking) {
          setIsTracking(serverIsTracking);
        }
        
        lastStatusCheckRef.current = now;
        return {
          ...status,
          breakState,
          cameraState,
          trackingMode: currentTrackingMode,
          roleHistory: roleHistoryRef.current,
          warningSystem: warningSystem,
          immediateWarningEnabled: true
        };
      }
      
      return null;
    } catch (error) {
      console.error('Failed to get attendance status:', error);
      setError(error.message);
      return null;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, userId, attendanceData, breakState, cameraState, currentTrackingMode, isTracking, warningSystem]);

  /**
   * Break management methods (same as original, participants only)
   */
  const takeBreak = useCallback(async () => {
    if (currentTrackingMode !== 'participant') {
      setError('Break functionality only available for participants');
      return false;
    }

    if (!isTracking) {
      setError('Not currently tracking attendance');
      return false;
    }

    if (breakState.isOnBreak || !breakState.canTakeBreak) {
      setError('Cannot take break at this time');
      return false;
    }

    try {
      setIsLoading(true);
      setError(null);

      // Use smart break manager for automatic camera control
      const response = await attendanceAPI.smartBreakManager.startBreak(
        { meetingId, userId },
        cameraControllerRef.current
      );

      if (response.success !== false) {
        setBreakState(prev => ({
          ...prev,
          isOnBreak: true,
          isPaused: false,
          timeRemaining: response.break_time_remaining || 300,
          canTakeBreak: false,
          canPauseBreak: true,
          canResumeBreak: false,
          canEndBreak: true
        }));
        
        setCameraState(prev => ({ 
          ...prev, 
          enabled: false, 
          shouldBeEnabled: false 
        }));
        
        setAttendanceData(prev => ({ ...prev, breakUsed: true }));
        setIsTracking(false); // Pause tracking during break
        
        // Clear any active warning timers during break
        if (warningTimerRef.current) {
          clearTimeout(warningTimerRef.current);
          warningTimerRef.current = null;
        }
        
        console.log('Break started successfully with camera disabled');
        return true;
      } else {
        throw new Error(response.message || 'Failed to start break');
      }
    } catch (error) {
      console.error('Break request failed:', error);
      setError(error.message);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [currentTrackingMode, isTracking, meetingId, userId, breakState.isOnBreak, breakState.canTakeBreak]);

  const pauseBreak = useCallback(async () => {
    if (currentTrackingMode !== 'participant') {
      setError('Break controls not available for hosts and co-hosts');
      return false;
    }

    if (!breakState.isOnBreak || breakState.isPaused) {
      setError('Cannot pause break at this time');
      return false;
    }

    try {
      setIsLoading(true);
      setError(null);

      const response = await attendanceAPI.smartBreakManager.pauseBreak(
        { meetingId, userId },
        cameraControllerRef.current
      );

      if (response.success) {
        setBreakState(prev => ({
          ...prev,
          isPaused: true,
          canPauseBreak: false,
          canResumeBreak: true
        }));
        
        setCameraState(prev => ({ 
          ...prev, 
          enabled: true, 
          shouldBeEnabled: true 
        }));
        
        console.log('Break paused - camera enabled for monitoring');
        return true;
      } else {
        throw new Error(response.message || 'Failed to pause break');
      }
    } catch (error) {
      console.error('Failed to pause break:', error);
      setError(error.message);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [currentTrackingMode, breakState.isOnBreak, breakState.isPaused, meetingId, userId]);

  const resumeBreak = useCallback(async () => {
    if (currentTrackingMode !== 'participant') {
      setError('Break controls not available for hosts and co-hosts');
      return false;
    }

    if (!breakState.isOnBreak || !breakState.isPaused) {
      setError('Cannot resume break at this time');
      return false;
    }

    try {
      setIsLoading(true);
      setError(null);

      const response = await attendanceAPI.smartBreakManager.resumeBreak(
        { meetingId, userId },
        cameraControllerRef.current
      );

      if (response.success) {
        setBreakState(prev => ({
          ...prev,
          isPaused: false,
          canPauseBreak: true,
          canResumeBreak: false
        }));
        
        setCameraState(prev => ({ 
          ...prev, 
          enabled: false, 
          shouldBeEnabled: false 
        }));
        
        console.log('Break resumed - camera disabled');
        return true;
      } else {
        throw new Error(response.message || 'Failed to resume break');
      }
    } catch (error) {
      console.error('Failed to resume break:', error);
      setError(error.message);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [currentTrackingMode, breakState.isOnBreak, breakState.isPaused, meetingId, userId]);

  const endBreak = useCallback(async () => {
    if (currentTrackingMode !== 'participant') {
      setError('Break controls not available for hosts and co-hosts');
      return false;
    }

    if (!breakState.isOnBreak) {
      setError('Not currently on break');
      return false;
    }

    try {
      setIsLoading(true);
      setError(null);

      const response = await attendanceAPI.smartBreakManager.endBreak(
        { meetingId, userId },
        cameraControllerRef.current
      );

      if (response.success !== false) {
        setBreakState({
          isOnBreak: false,
          isPaused: false,
          timeRemaining: 300,
          timeUsed: response.total_break_time_used || 0,
          canTakeBreak: false, // Break already used
          canPauseBreak: false,
          canResumeBreak: false,
          canEndBreak: false
        });
        
        setCameraState(prev => ({ 
          ...prev, 
          enabled: true, 
          shouldBeEnabled: true 
        }));
        
        setIsTracking(true); // Resume tracking
        
        console.log('Break ended successfully - camera enabled and tracking resumed');
        return true;
      } else {
        throw new Error(response.message || 'Failed to end break');
      }
    } catch (error) {
      console.error('Failed to end break:', error);
      setError(error.message);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [currentTrackingMode, breakState.isOnBreak, meetingId, userId]);

  /**
   * Get meeting attendance report (for hosts/co-hosts)
   * @returns {Promise<Object|null>} Meeting report
   */
  const getMeetingReport = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);

      const report = await attendanceAPI.getMeetingReport(meetingId);
      return report;
    } catch (error) {
      console.error('Failed to get meeting report:', error);
      setError(error.message);
      return null;
    } finally {
      setIsLoading(false);
    }
  }, [meetingId]);

  /**
   * Check if attendance service is healthy
   * @returns {Promise<boolean>} Service health status
   */
  const checkServiceHealth = useCallback(async () => {
    try {
      const health = await attendanceAPI.healthCheck();
      return health.status === 'ok';
    } catch (error) {
      console.error('Attendance service health check failed:', error);
      return false;
    }
  }, []);

  /**
   * Reset attendance state with immediate warning system
   */
  const resetState = useCallback(() => {
    setIsTracking(false);
    setError(null);
    setAttendanceData({
      attendancePercentage: 100,
      engagementScore: 100,
      violations: [],
      popupCount: 0,
      maxPopups: 4,
      breakUsed: false,
      sessionActive: true,
      sessionStats: {},
      totalPresenceTime: 0,
      roleHistory: []
    });
    setBreakState({
      isOnBreak: false,
      isPaused: false,
      timeRemaining: 300,
      timeUsed: 0,
      canTakeBreak: currentTrackingMode === 'participant',
      canPauseBreak: false,
      canResumeBreak: false,
      canEndBreak: false
    });
    setCameraState({
      enabled: false,
      ready: false,
      error: null,
      shouldBeEnabled: true
    });
    // FIXED: Reset enhanced warning system
    setWarningSystem({
      warningCount: 0,
      warningsExhausted: false,
      postWarningViolationStart: null,
      violationStartTime: null,
      isInViolation: false,
      continuousViolationTime: 0,
      firstWarningGiven: false,
      lastWarningTime: 0,
      warningTimer: null
    });
    setRoleHistory([]);
    roleHistoryRef.current = [];
    trackingSessionRef.current = null;
    lastStatusCheckRef.current = 0;
    cameraControllerRef.current = null;
    
    // Clear any active warning timer
    if (warningTimerRef.current) {
      clearTimeout(warningTimerRef.current);
      warningTimerRef.current = null;
    }
  }, [currentTrackingMode]);

  /**
   * Get session duration in seconds
   * @returns {number} Session duration
   */
  const getSessionDuration = useCallback(() => {
    if (!trackingSessionRef.current) {
      return 0;
    }
    
    return Math.floor((Date.now() - trackingSessionRef.current.startTime) / 1000);
  }, []);

  // Cleanup effect for warning timer
  useEffect(() => {
    return () => {
      if (warningTimerRef.current) {
        clearTimeout(warningTimerRef.current);
        warningTimerRef.current = null;
      }
    };
  }, []);

  return {
    // State
    isTracking,
    isLoading,
    error,
    attendanceData,
    breakState,
    cameraState,
    warningSystem, // FIXED: Include enhanced warning system
    
    // Role information
    currentTrackingMode,
    userRole,
    isHost,
    isCoHost,
    roleHistory,
    
    // Core functions (now work for all roles with different behavior)
    startTracking,
    stopTracking,
    detectViolations,
    getStatus,
    
    // Break management functions (participant only)
    takeBreak,
    pauseBreak,
    resumeBreak,
    endBreak,
    
    // Camera control
    setCameraController,
    
    // Reporting functions
    getMeetingReport,
    
    // Utility functions
    checkServiceHealth,
    clearError,
    resetState,
    getSessionDuration,
    
    // Computed values with role awareness and immediate warning system
    sessionDuration: getSessionDuration(),
    canTakeBreak: currentTrackingMode === 'participant' && breakState.canTakeBreak && !breakState.isOnBreak,
    canPauseBreak: currentTrackingMode === 'participant' && breakState.canPauseBreak,
    canResumeBreak: currentTrackingMode === 'participant' && breakState.canResumeBreak,
    canEndBreak: currentTrackingMode === 'participant' && breakState.canEndBreak,
    attendancePercentage: attendanceData.attendancePercentage,
    engagementScore: attendanceData.engagementScore,
    violationCount: currentTrackingMode === 'participant' ? attendanceData.violations.length : 0,
    isSessionActive: attendanceData.sessionActive,
    isOnBreak: breakState.isOnBreak,
    isBreakPaused: breakState.isPaused,
    breakTimeRemaining: breakState.timeRemaining,
    cameraEnabled: cameraState.enabled,
    cameraReady: cameraState.ready,
    
    // Enhanced computed values with immediate warning system
    shouldDetectViolations: currentTrackingMode === 'participant',
    totalPresenceTime: attendanceData.totalPresenceTime,
    warningCount: warningSystem.warningCount,
    warningsExhausted: warningSystem.warningsExhausted,
    continuousViolationTime: warningSystem.continuousViolationTime,
    firstWarningGiven: warningSystem.firstWarningGiven, // NEW
    immediateWarningEnabled: true, // NEW
    hasRoleTransitions: roleHistory.length > 0
  };
};