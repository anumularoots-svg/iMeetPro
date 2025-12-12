import React, {
  useState,
  useEffect,
  useRef,
  useCallback,
  useMemo,
} from "react";
import {
  Box,
  Typography,
  IconButton,
  Chip,
  Card,
  CardContent,
  LinearProgress,
  Tooltip,
  Button,
  Alert,
  CircularProgress,
  Snackbar,
  Dialog,
  DialogContent,
  DialogActions,
  DialogTitle,
} from "@mui/material";
import {
  Visibility,
  VisibilityOff,
  Coffee,
  Warning,
  CheckCircle,
  Error as ErrorIcon,
  Videocam,
  VideocamOff,
  Stop,
  SupervisorAccount,
  AdminPanelSettings,
  PersonOff,
  Close,
  VideocamOutlined,
} from "@mui/icons-material";
import { styled } from "@mui/material/styles";

import AttendancePopup from "./AttendancePopup";
import { faceAuthAPI } from '../../services/faceAuthAPI';

// ==================== STYLED COMPONENTS ====================
const AttendanceContainer = styled(Box)(({ theme }) => ({
  position: "fixed",
  top: theme.spacing(0.5),
  right: theme.spacing(5),
  zIndex: 1000,
}));

const AttendanceIndicator = styled(Card)(({ theme }) => ({
  backgroundColor: "rgba(0,0,0,0.9)",
  backdropFilter: "blur(20px)",
  border: "1px solid rgba(255,255,255,0.1)",
  color: "white",
  boxShadow: "0 8px 32px rgba(0,0,0,0.4)",
}));

const StatusChip = styled(Chip)(({ status }) => ({
  height: 22,
  fontSize: "0.7rem",
  fontWeight: 600,
  color: "white",
  backgroundColor:
    status === "active"
      ? "rgba(76,175,80,0.8)"
      : status === "warning"
        ? "rgba(255,152,0,0.8)"
        : status === "violation"
          ? "rgba(244,67,54,0.8)"
          : status === "break"
            ? "rgba(33,150,243,0.8)"
            : status === "excluded"
              ? "rgba(158,158,158,0.8)"
              : status === "host_tracking"
                ? "rgba(76,175,80,0.6)"
                : status === "terminated"
                  ? "rgba(244,67,54,0.9)"
                  : status === "initializing"
                    ? "rgba(255,193,7,0.8)"
                    : status === "unauthorized"
                      ? "rgba(156,39,176,0.8)"
                      : "rgba(158,158,158,0.8)",
}));

const CameraPromptDialog = styled(Dialog)(({ theme }) => ({
  '& .MuiDialog-paper': {
    backgroundColor: '#FFFFFF',
    border: '3px solid #FF9800',
    borderRadius: theme.spacing(2),
    minWidth: 420,
    maxWidth: 550,
    boxShadow: '0 8px 32px rgba(255, 152, 0, 0.3)',
  }
}));

const AttendanceConfig = {
  MAX_WARNING_MESSAGES: 4,
  IDENTITY_MAX_WARNINGS: 3,
  VIOLATION_AUTO_REMOVAL_TIME: 120,
  BREAK_DURATION: 300,
  MAX_TOTAL_BREAK_TIME: 300,
  DETECTION_INTERVAL: 20,
  GRACE_PERIOD_DURATION: 2,
  CAMERA_VERIFICATION_TIMEOUT: 5,
  CAMERA_PROMPT_DELAY: 3000, // 3 seconds delay before showing camera prompt
};

// ==================== MAIN COMPONENT ====================
const AttendanceTracker = ({
  meetingId,
  userId,
  userName,
  isActive = true,
  cameraEnabled: propCameraEnabled = false,
  onViolation,
  onStatusChange,
  minimized = false,
  onToggleMinimized,
  onCameraToggle,
  onSessionTerminated,
  isHost = false,
  isCoHost = false,
  effectiveRole = "participant",
}) => {
  
  // ==================== REFS ====================
  const videoRef = useRef(null);
  const canvasRef = useRef(null);
  const intervalRef = useRef(null);
  const streamRef = useRef(null);
  const mountedRef = useRef(true);
  const breakTimerRef = useRef(null);
  const roleHistoryRef = useRef([]);
  const breakProcessingRef = useRef(false);
  const autoResumeInProgressRef = useRef(false);
  const cameraVerificationTokenRef = useRef(null);
  const cameraWasEnabledBeforeBreakRef = useRef(false);
  const gracePeriodRef = useRef({ active: false, until: 0 });
  const cameraPromptTimerRef = useRef(null);

  // ==================== FACE AUTHENTICATION REFS ====================
  const lastAuthCheckTimeRef = useRef(0);
  const authCheckIntervalRef = useRef(null);
  const isAuthBlockedRef = useRef(false);

  // ==================== SESSION STATE ====================
  const [isSessionTerminated, setIsSessionTerminated] = useState(false);
  const [isSessionPermanentlyEnded, setIsSessionPermanentlyEnded] = useState(false);
  const [terminationReason, setTerminationReason] = useState("");
  const [terminationCountdown, setTerminationCountdown] = useState(null);
  const [isTerminating, setIsTerminating] = useState(false);

  // ==================== ROLE TRACKING STATE ====================
  const [currentTrackingMode, setCurrentTrackingMode] = useState("participant");
  const [roleTransitionInProgress, setRoleTransitionInProgress] = useState(false);
  const [sessionStartTime, setSessionStartTime] = useState(Date.now());

  // ==================== TRACKING STATE ====================
  const [isTracking, setIsTracking] = useState(false);
  const [attendanceData, setAttendanceData] = useState({
    attendancePercentage: 100,
    engagementScore: 100,
    popupCount: 0,
    maxPopups: 4,
    breakUsed: false,
    violations: [],
    currentViolations: [],
    frameCount: 0,
    lastDetectionTime: null,
    totalPresenceTime: 0,
    roleHistory: [],
  });

  // ==================== VIOLATION STATE ====================
  const [currentViolations, setCurrentViolations] = useState([]);
  
  // ✅ MODAL STATE (Only for Termination/Blocking)
  const [showTerminationPopup, setShowTerminationPopup] = useState(false);
  const [terminationMessage, setTerminationMessage] = useState("");

  // ✅ CAMERA ENABLE PROMPT STATE
  const [showCameraEnablePrompt, setShowCameraEnablePrompt] = useState(false);
  const [cameraPromptDismissed, setCameraPromptDismissed] = useState(false);

  // ✅ TOAST STATE (For Warnings/Info/Success)
  const [toastOpen, setToastOpen] = useState(false);
  const [toastMessage, setToastMessage] = useState("");
  const [toastSeverity, setToastSeverity] = useState("info");
  const [toastVariant, setToastVariant] = useState("standard");

  const [sessionActive, setSessionActive] = useState(true);
  const [videoReady, setVideoReady] = useState(false);
  const [cameraError, setCameraError] = useState(null);
  const [processingFrame, setProcessingFrame] = useState(false);
  const [gracePeriodActive, setGracePeriodActive] = useState(false);

  // ==================== WARNING SYSTEM STATE ====================
  const [warningCount, setWarningCount] = useState(0);
  const [warningsExhausted, setWarningsExhausted] = useState(false);
  
  // Identity States
  const [authWarningCount, setAuthWarningCount] = useState(0);
  const [isAuthBlocked, setIsAuthBlocked] = useState(false);

  // ==================== BREAK MANAGEMENT STATE ====================
  const [isOnBreak, setIsOnBreak] = useState(false);
  const [breakTimeLeft, setBreakTimeLeft] = useState(0);
  const [totalBreakTimeUsed, setTotalBreakTimeUsed] = useState(0);
  const [breakTimeRemaining, setBreakTimeRemaining] = useState(300);
  const [showSuccessPopup, setShowSuccessPopup] = useState(true);
  const [breakStartedAt, setBreakStartedAt] = useState(null);
  const [breakCount, setBreakCount] = useState(0);
  const [isProcessingBreak, setIsProcessingBreak] = useState(false);

  // ==================== CAMERA STATE ====================
  const [cameraEnabled, setCameraEnabled] = useState(propCameraEnabled);
  const [cameraInitStatus, setCameraInitStatus] = useState("idle");
  const [cameraInitError, setCameraInitError] = useState("");

  // ==================== FACE AUTHENTICATION STATE ====================
  const [faceAuthStatus, setFaceAuthStatus] = useState("verified"); 

  // ==================== ROLE-BASED TRACKING MODE ====================
  const determineTrackingMode = useCallback(() => {
    if (isHost || effectiveRole === "host") return "host";
    if (isCoHost || effectiveRole === "co-host" || effectiveRole === "cohost")
      return "cohost";
    return "participant";
  }, [isHost, isCoHost, effectiveRole]);

  // ==================== VIOLATION DETECTION CHECK ====================
  const shouldDetectViolations = useMemo(() => {
    return (
      currentTrackingMode === "participant" &&
      sessionActive &&
      !isSessionTerminated &&
      !isOnBreak
    );
  }, [currentTrackingMode, sessionActive, isSessionTerminated, isOnBreak]);

  // ==================== BREAK AVAILABILITY CHECK ====================
  const canTakeBreak = useMemo(() => {
    return (
      currentTrackingMode === "participant" &&
      breakTimeRemaining >= 5 &&
      sessionActive &&
      !isOnBreak &&
      !isSessionTerminated &&
      !isProcessingBreak &&
      totalBreakTimeUsed < 300 &&
      !isAuthBlocked
    );
  }, [
    currentTrackingMode,
    breakTimeRemaining,
    sessionActive,
    isOnBreak,
    isSessionTerminated,
    isProcessingBreak,
    totalBreakTimeUsed,
    isAuthBlocked,
  ]);

  // ==================== TOAST HELPER ====================
  const triggerToast = useCallback((message, severity = "info") => {
    if (!message) return;
    setToastMessage(message);
    setToastSeverity(severity);
    // Use filled variant for high severity/warnings to stand out
    setToastVariant(severity === 'error' || severity === 'warning' ? 'filled' : 'standard');
    setToastOpen(true);
  }, []);

  const handleToastClose = (event, reason) => {
    if (reason === 'clickaway') return;
    setToastOpen(false);
  };

  // ==================== DISPLAY NOTIFICATION SYSTEM ====================
  const showViolationPopup = useCallback(
    (message, type = "warning", force = false) => {
      // Host check - ignore non-critical messages
      if (
        currentTrackingMode !== "participant" &&
        type !== "info" &&
        type !== "success"
      ) {
        return;
      }

      // ✅ TERMINATION CHECK: Only Modal for Removal/Blocking
      const isTermination = 
        type === "error" && 
        (message.toLowerCase().includes("terminated") || 
         message.toLowerCase().includes("blocked") || 
         message.toLowerCase().includes("removed"));

      if (isTermination) {
        setTerminationMessage(message);
        setShowTerminationPopup(true);
      } else {
        // ✅ EVERYTHING ELSE -> TOAST
        if (type === "success" && !force) {
           if (!showSuccessPopup) return;
           setShowSuccessPopup(false);
        }
        
        // Map notification types to toast severity
        let toastType = type;
        if(type === "violation") toastType = "warning";

        triggerToast(message, toastType);
      }

      // Call onViolation callback
      if (onViolation) {
        onViolation({ message, type, timestamp: Date.now() });
      }
    },
    [currentTrackingMode, onViolation, showSuccessPopup, triggerToast]
  );

  // ==================== API CALL FUNCTION ====================
  const apiCall = useCallback(
    async (endpoint, method = "GET", data = null) => {
      const url = `/api/attendance${endpoint}`;
      const options = {
        method,
        headers: {
          "Content-Type": "application/json",
        },
      };

      if (data) {
        options.body = JSON.stringify({
          ...data,
          current_tracking_mode: currentTrackingMode,
          role_history: roleHistoryRef.current,
          session_start_time: sessionStartTime,
        });
      }

      const response = await fetch(url, options);
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }
      return await response.json();
    },
    [currentTrackingMode, sessionStartTime]
  );

  // ==================== SESSION TERMINATION HANDLER ====================
  const handleSessionTermination = useCallback(
    async (
      reason = "violations",
      message = "Session ended due to violations"
    ) => {
      if (currentTrackingMode !== "participant") return;

      setIsSessionTerminated(true);
      setIsSessionPermanentlyEnded(true);
      setTerminationReason(reason);
      setSessionActive(false);
      setIsTracking(false);
      setIsTerminating(true);

      // Close camera prompt if open
      setShowCameraEnablePrompt(false);

      try {
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = null;
        }

        if (breakTimerRef.current) {
          clearInterval(breakTimerRef.current);
          breakTimerRef.current = null;
        }

        if (authCheckIntervalRef.current) {
          clearInterval(authCheckIntervalRef.current);
          authCheckIntervalRef.current = null;
        }

        if (cameraPromptTimerRef.current) {
          clearTimeout(cameraPromptTimerRef.current);
          cameraPromptTimerRef.current = null;
        }

        if (streamRef.current) {
          streamRef.current.getTracks().forEach((track) => {
            track.stop();
          });
          streamRef.current = null;
        }

        if (videoRef.current) {
          videoRef.current.srcObject = null;
        }

        setVideoReady(false);

        if (meetingId && userId) {
          try {
            await apiCall("/stop/", "POST", {
              meeting_id: meetingId,
              user_id: userId,
              reason: "session_terminated",
              termination_reason: reason,
            });
          } catch (backendError) {
            console.warn("Failed to notify backend:", backendError.message);
          }
        }
      } catch (cleanupError) {
        console.error("Termination cleanup error:", cleanupError);
      }

      // ✅ SHOW TERMINATION MODAL (The only allowed popup)
      showViolationPopup(message, "error");

      setTerminationCountdown(3);
      const countdownInterval = setInterval(() => {
        setTerminationCountdown((prev) => {
          if (prev <= 1) {
            clearInterval(countdownInterval);

            if (onSessionTerminated) {
              onSessionTerminated({
                userId,
                userName,
                reason: reason,
                message: message,
                timestamp: Date.now(),
                participantSpecific: true,
                permanent: true,
              });
            }
            return 0;
          }
          return prev - 1;
        });
      }, 1000);
    },
    [
      currentTrackingMode,
      meetingId,
      userId,
      userName,
      onSessionTerminated,
      showViolationPopup,
      apiCall,
    ]
  );

  // ==================== CAMERA INITIALIZATION ====================
  const initializeCamera = useCallback(async () => {
    if (isSessionTerminated || isSessionPermanentlyEnded) {
      return false;
    }

    try {
      setCameraError(null);
      setCameraInitStatus("initializing");
      setCameraInitError("");

      const constraints = [
        { video: { width: 640, height: 480, frameRate: 15 }, audio: false },
        { video: { width: 320, height: 240 }, audio: false },
        { video: true, audio: false },
      ];

      let stream = null;
      for (let i = 0; i < constraints.length; i++) {
        try {
          stream = await navigator.mediaDevices.getUserMedia(constraints[i]);
          break;
        } catch (err) {
          if (i === constraints.length - 1) throw err;
        }
      }

      if (!stream) {
        throw new Error("Failed to get camera stream");
      }

      streamRef.current = stream;

      if (videoRef.current && mountedRef.current && !isSessionTerminated) {
        videoRef.current.srcObject = stream;

        await new Promise((resolve, reject) => {
          const video = videoRef.current;
          if (!video) {
            reject(new Error("Video ref lost"));
            return;
          }

          const timeout = setTimeout(() => {
            reject(new Error("Video setup timeout"));
          }, 10000);

          const handleLoad = () => {
            clearTimeout(timeout);
            video.removeEventListener("loadedmetadata", handleLoad);
            video.removeEventListener("error", handleError);
            resolve();
          };

          const handleError = (err) => {
            clearTimeout(timeout);
            video.removeEventListener("loadedmetadata", handleLoad);
            video.removeEventListener("error", handleError);
            reject(err);
          };

          video.addEventListener("loadedmetadata", handleLoad);
          video.addEventListener("error", handleError);

          video.play().catch(reject);
        });
      }

      if (mountedRef.current && !isSessionTerminated) {
        setVideoReady(true);
        setCameraInitStatus("ready");
        setCameraEnabled(true);
      }
      return true;
    } catch (error) {
      console.error("Camera init failed:", error);
      setCameraError(error.message);
      setCameraInitError(error.message);
      setVideoReady(false);
      setCameraInitStatus("failed");
      return false;
    }
  }, [isSessionTerminated, isSessionPermanentlyEnded]);

  // ==================== VERIFY CAMERA HARDWARE IS READY ====================
  const verifyCameraReady = useCallback(
    async (maxRetries = 5, retryDelay = 500) => {
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        if (!mountedRef.current) return false;

        if (!videoRef.current || !streamRef.current) {
          if (attempt < maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, retryDelay));
            continue;
          }
          return false;
        }

        const videoTracks = streamRef.current.getVideoTracks();
        if (videoTracks.length === 0) {
          if (attempt < maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, retryDelay));
            continue;
          }
          return false;
        }

        const track = videoTracks[0];

        if (!track.enabled) {
          track.enabled = true;
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        if (track.readyState !== "live") {
          if (attempt < maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, retryDelay));
            continue;
          }
          return false;
        }

        if (videoRef.current.paused || videoRef.current.ended) {
          try {
            await videoRef.current.play();
            await new Promise((resolve) => setTimeout(resolve, 500));
          } catch (playError) {
            if (attempt < maxRetries) {
              await new Promise((resolve) => setTimeout(resolve, retryDelay));
              continue;
            }
          }
        }

        if (
          videoRef.current.videoWidth === 0 ||
          videoRef.current.videoHeight === 0
        ) {
          if (attempt < maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, retryDelay));
            continue;
          }
          return false;
        }

        return true;
      }
      return false;
    },
    []
  );

  // ==================== VERIFY WITH BACKEND ====================
  const verifyWithBackend = useCallback(
    async (confirmationToken) => {
      if (!confirmationToken) return false;

      try {
        setCameraInitStatus("verifying");

        const response = await apiCall("/verify-camera/", "POST", {
          meeting_id: meetingId,
          user_id: userId,
          confirmation_token: confirmationToken,
          camera_active: true,
        });

        if (response.success) {
          return true;
        } else {
          return false;
        }
      } catch (error) {
        return true;
      }
    },
    [meetingId, userId, apiCall]
  );

  // ==================== FRAME CAPTURE AND ANALYSIS ====================
  const captureAndAnalyze = useCallback(async () => {
    if (!mountedRef.current) return;

    if (isSessionTerminated || isSessionPermanentlyEnded) return;

    if (!cameraEnabled) return;

    if (isOnBreak) return;

    if (!sessionActive || !videoReady || !isTracking) return;

    if (!videoRef.current || !canvasRef.current) return;

    const video = videoRef.current;
    const canvas = canvasRef.current;

    if (video.videoWidth === 0 || video.videoHeight === 0) return;

    if (video.paused || video.ended) return;

    if (!streamRef.current || streamRef.current.getTracks().length === 0) return;

    const videoTracks = streamRef.current.getVideoTracks();
    if (videoTracks.length === 0 || !videoTracks[0].enabled) return;

    if (processingFrame) return;

    try {
      setProcessingFrame(true);

      const context = canvas.getContext("2d");
      const maxWidth = 640;
      const maxHeight = 480;
      let { videoWidth: width, videoHeight: height } = video;

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
      context.drawImage(video, 0, 0, width, height);

      const frameData = canvas.toDataURL("image/jpeg", 0.8);

      const analysisState = {
        meeting_id: meetingId,
        user_id: userId,
        frame: frameData,
        user_role: effectiveRole,
        current_tracking_mode: currentTrackingMode,
        is_host: isHost,
        is_cohost: isCoHost,
        is_on_break: isOnBreak,
        should_detect_violations: shouldDetectViolations,
        role_history: roleHistoryRef.current,
        session_start_time: sessionStartTime,
      };

      // Backend handles BOTH behavior (20s check) and identity (1s check)
      const response = await apiCall("/detect/", "POST", analysisState);

      if (mountedRef.current && !isSessionTerminated) {
        handleAnalysisResponse(response);
      }
    } catch (error) {
      console.error("Frame analysis error:", error);

      if (
        error.message.includes("session_closed") ||
        error.message.includes("session_terminated")
      ) {
        if (currentTrackingMode === "participant") {
          handleSessionTermination("session_violations_error", "Session ended");
        }
        return;
      }
    } finally {
      if (mountedRef.current) {
        setProcessingFrame(false);
      }
    }
  }, [
    sessionActive,
    videoReady,
    meetingId,
    userId,
    processingFrame,
    isOnBreak,
    cameraEnabled,
    isTracking,
    effectiveRole,
    currentTrackingMode,
    isHost,
    isCoHost,
    shouldDetectViolations,
    sessionStartTime,
    apiCall,
    isSessionTerminated,
    isSessionPermanentlyEnded,
  ]);

  const handleAnalysisResponse = useCallback(
    (response) => {
      if (!response || !mountedRef.current) return;

      if (isSessionTerminated || isSessionPermanentlyEnded) return;

      // ============================================================
      // 1. IDENTITY VERIFICATION
      // ============================================================
      
      // Sync state (Backend source: 508)
      if (response.identity_warning_count !== undefined) {
        setAuthWarningCount(response.identity_warning_count);
        
        // Update visual status based on backend counts
        if (response.identity_warning_count > 0) {
           setFaceAuthStatus("unauthorized");
        } else if (response.identity_verified) {
           setFaceAuthStatus("verified");
        }
      }

      // ✅ CHECK 1: Identity Toast (Backend Source: 213, 220)
      // The backend ONLY sends 'identity_popup' when 5s threshold is reached.
      if (response.identity_popup && response.identity_popup.trim() !== "") {
         triggerToast(response.identity_popup, "error");
      }

      // ✅ CHECK 2: Identity Removal (Backend Source: 198)
      // Strict check for identity_is_removed flag
      if (response.identity_is_removed === true || (response.status === "removed_from_meeting" && response.removal_type === "identity_verification")) {
         setIsAuthBlocked(true);
         handleSessionTermination(
            "identity_verification_failure",
            response.message || "🚫 Session terminated: Identity verification failed 3 times."
         );
         return;
      }

      // ============================================================
      // 2. BEHAVIORAL ANALYSIS 
      // ============================================================

      // ✅ CHECK 3: Behavior Toast (Backend Source: 551, 570)
      // The backend ONLY sends 'popup' when the 20-sec cooldown is met.
      if (response.popup && response.popup.trim() !== "") {
        let popupType = "warning";
        
        if (response.popup.includes("Detection")) {
             popupType = "error"; // Red for detections (penalty phase)
        } else if (response.popup.includes("Warning")) {
             popupType = "warning"; // Orange for warnings
        } else if (response.popup.includes("Inactivity")) {
             popupType = "info";
        }
        
        triggerToast(response.popup, popupType);
      }

      // ✅ CHECK 4: Continuous Removal (Backend Source: 584)
      // Strict check for continuous removal flag
      if (response.status === "participant_removed" || response.removal_type === "continuous_violations") {
        handleSessionTermination(
          "continuous_violations",
          response.message || `🚫 Session terminated: Continuous violations for 2 minutes.`
        );
        return;
      }

      // Grace Period Check
      if (response.grace_period_active) {
        const gracePeriodTimeRemaining = (response.grace_period_expires_in || 0) * 1000;
        setGracePeriodActive(true);
        gracePeriodRef.current.active = true;
        gracePeriodRef.current.until = Date.now() + gracePeriodTimeRemaining;

        if (response.message) {
          triggerToast(response.message, "info");
        }
        return;
      } else if (
        gracePeriodRef.current.active &&
        Date.now() >= gracePeriodRef.current.until
      ) {
        setGracePeriodActive(false);
        gracePeriodRef.current.active = false;
        gracePeriodRef.current.until = 0;
      }

      // Update Attendance Data
      if (response.attendance_percentage !== undefined) {
        setAttendanceData((prev) => ({
          ...prev,
          attendancePercentage: response.attendance_percentage,
          engagementScore: response.engagement_score || response.attendance_percentage,
          popupCount: response.popup_count || 0,
        }));
      }

      // Update Violation List
      if (response.violations && Array.isArray(response.violations)) {
        setCurrentViolations(response.violations);
      }

      // Update Warning Counts
      if (response.warning_count !== undefined) {
        setWarningCount(response.warning_count);
      }

      if (response.warning_phase_complete !== undefined) {
        setWarningsExhausted(response.warning_phase_complete);
      }

      // Call Parent Callback
      if (onStatusChange) {
        onStatusChange({
          status: response.status,
          attendancePercentage: response.attendance_percentage,
          violations: response.violations || [],
          warningCount: response.warning_count || 0,
          detectionCount: response.detection_counts || 0,
        });
      }
    },
    [
      isSessionTerminated,
      isSessionPermanentlyEnded,
      handleSessionTermination,
      triggerToast,
      currentTrackingMode,
      onStatusChange,
    ]
  );

  // ==================== END BREAK HANDLER ====================
  const handleEndBreak = useCallback(async () => {
    if (currentTrackingMode !== "participant") return;

    const cameraWasEnabled = cameraWasEnabledBeforeBreakRef.current;

    if (breakProcessingRef.current || autoResumeInProgressRef.current) return;

    const isAutoExpire = breakTimeLeft === 0;

    breakProcessingRef.current = true;
    autoResumeInProgressRef.current = true;
    setIsProcessingBreak(true);

    let backendResponse = null;

    try {
      const statusResponse = await apiCall(
        `/status/?meeting_id=${meetingId}&user_id=${userId}`,
        "GET"
      );

      if (!statusResponse.is_on_break) {
        setIsOnBreak(false);
        setBreakTimeLeft(0);
        setBreakStartedAt(null);
        if (breakTimerRef.current) {
          clearInterval(breakTimerRef.current);
          breakTimerRef.current = null;
        }
      } else {
        backendResponse = await apiCall("/pause-resume/", "POST", {
          meeting_id: meetingId,
          user_id: userId,
          action: "resume",
        });

        if (backendResponse.success !== false) {
          setIsOnBreak(false);
          setBreakStartedAt(null);
          setCurrentViolations([]);
          setTotalBreakTimeUsed(backendResponse.total_break_time_used || 0);
          setBreakTimeRemaining(backendResponse.break_time_remaining || 0);
          setBreakCount(backendResponse.break_count || 0);

          if (breakTimerRef.current) {
            clearInterval(breakTimerRef.current);
            breakTimerRef.current = null;
          }
          setBreakTimeLeft(0);

          if (backendResponse.camera_confirmation_token) {
            cameraVerificationTokenRef.current =
              backendResponse.camera_confirmation_token;
          }
        } else {
          throw new Error(backendResponse.error || "Failed to end break");
        }
      }
    } catch (error) {
      showViolationPopup(`Failed to end break: ${error.message}`, "error");
      breakProcessingRef.current = false;
      autoResumeInProgressRef.current = false;
      setIsProcessingBreak(false);
      return;
    }

    if (isSessionTerminated || isSessionPermanentlyEnded) {
      breakProcessingRef.current = false;
      autoResumeInProgressRef.current = false;
      setIsProcessingBreak(false);
      return;
    }

    if (cameraWasEnabled) {
      if (!onCameraToggle) {
        showViolationPopup("Error: Cannot control camera", "error");
        breakProcessingRef.current = false;
        autoResumeInProgressRef.current = false;
        setIsProcessingBreak(false);
        return;
      }

      if (isAutoExpire) {
        setCameraEnabled(true);
        await new Promise((resolve) => setTimeout(resolve, 250));
      }

      showViolationPopup("Enabling camera after break...", "info");

      setCameraInitStatus("initializing");
      setCameraEnabled(true);

      await new Promise((resolve) => setTimeout(resolve, 150));

      try {
        onCameraToggle(true);
      } catch (toggleError) {
        setCameraInitStatus("failed");
        showViolationPopup("Failed to enable camera", "error");
        breakProcessingRef.current = false;
        autoResumeInProgressRef.current = false;
        setIsProcessingBreak(false);
        return;
      }

      const initialWaitTime = isAutoExpire ? 4000 : 2000;
      await new Promise((resolve) => setTimeout(resolve, initialWaitTime));

      const maxRetries = isAutoExpire ? 20 : 5;
      const retryDelay = isAutoExpire ? 1500 : 500;

      let cameraReady = false;

      for (let attempt = 1; attempt <= 3 && !cameraReady; attempt++) {
        if (attempt > 1) {
          try {
            onCameraToggle(true);
            await new Promise((resolve) => setTimeout(resolve, 2000));
          } catch (retryError) {
            console.error(`Retry ${attempt} failed:`, retryError);
          }
        }

        cameraReady = await verifyCameraReady(maxRetries, retryDelay);

        if (cameraReady) {
          break;
        } else if (attempt < 3) {
          await new Promise((resolve) => setTimeout(resolve, 1500));
        }
      }

      if (!cameraReady) {
        setCameraInitStatus("failed");
        showViolationPopup(
          "Camera failed to start - please enable manually",
          "error"
        );
        breakProcessingRef.current = false;
        autoResumeInProgressRef.current = false;
        setIsProcessingBreak(false);
        return;
      }

      setCameraInitStatus("ready");

      if (
        backendResponse?.camera_verification_required &&
        cameraVerificationTokenRef.current &&
        !isAutoExpire
      ) {
        await verifyWithBackend(cameraVerificationTokenRef.current);
      }

      if (!mountedRef.current || !sessionActive) {
        breakProcessingRef.current = false;
        autoResumeInProgressRef.current = false;
        setIsProcessingBreak(false);
        return;
      }

      setIsTracking(true);

      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }

      intervalRef.current = setInterval(() => {
        if (mountedRef.current && !isSessionTerminated && !isOnBreak) {
          captureAndAnalyze();
        }
      }, 3000);

      setTimeout(() => {
        if (
          mountedRef.current &&
          !isSessionTerminated &&
          !isOnBreak &&
          cameraEnabled
        ) {
          captureAndAnalyze();
        }
      }, 1000);

      const endType = isAutoExpire ? "auto-completed (300s)" : "manually ended";
      showViolationPopup(
        `Break ${endType} - Camera enabled, detection active`,
        "success"
      );
    } else {
      if (mountedRef.current && sessionActive && !isSessionTerminated) {
        setIsTracking(true);
      }
      showViolationPopup("Break ended - Camera remains off", "info");
    }

    breakProcessingRef.current = false;
    autoResumeInProgressRef.current = false;
    setIsProcessingBreak(false);
  }, [
    currentTrackingMode,
    isOnBreak,
    breakTimeLeft,
    isSessionTerminated,
    isSessionPermanentlyEnded,
    meetingId,
    userId,
    onCameraToggle,
    sessionActive,
    showViolationPopup,
    apiCall,
    verifyCameraReady,
    verifyWithBackend,
    captureAndAnalyze,
    cameraEnabled,
  ]);

  // ==================== RETRY CAMERA ====================
  const handleRetryCamera = useCallback(async () => {
    if (!onCameraToggle) {
      showViolationPopup("Camera control not available", "error");
      return;
    }

    setCameraInitStatus("initializing");
    showViolationPopup("Retrying camera initialization...", "info");

    try {
      onCameraToggle(true);
      setCameraEnabled(true);

      await new Promise((resolve) => setTimeout(resolve, 2000));

      const cameraReady = await verifyCameraReady(5, 500);

      if (!cameraReady) {
        throw new Error("Camera verification failed after retry");
      }

      setCameraInitStatus("ready");
      showViolationPopup("Camera enabled successfully", "success");

      if (!isTracking && sessionActive && !isSessionTerminated && !isOnBreak) {
        setIsTracking(true);

        if (intervalRef.current) {
          clearInterval(intervalRef.current);
        }

        intervalRef.current = setInterval(() => {
          if (mountedRef.current && !isSessionTerminated && !isOnBreak) {
            captureAndAnalyze();
          }
        }, 3000);

        setTimeout(() => {
          if (mountedRef.current && !isSessionTerminated && !isOnBreak) {
            captureAndAnalyze();
          }
        }, 1000);
      }
    } catch (error) {
      setCameraInitStatus("failed");
      setCameraInitError(error.message);
      showViolationPopup(`Camera retry failed: ${error.message}`, "error");
    }
  }, [
    onCameraToggle,
    showViolationPopup,
    verifyCameraReady,
    isTracking,
    sessionActive,
    isSessionTerminated,
    isOnBreak,
    captureAndAnalyze,
  ]);

  // ==================== HANDLE ENABLE CAMERA FROM PROMPT ====================
  const handleEnableCameraFromPrompt = useCallback(async () => {
    if (!onCameraToggle) {
      triggerToast("Camera control not available", "error");
      setShowCameraEnablePrompt(false);
      return;
    }

    try {
      setShowCameraEnablePrompt(false);
      setCameraPromptDismissed(false);
      
      triggerToast("Enabling camera...", "info");
      
      onCameraToggle(true);
      setCameraEnabled(true);

      await new Promise((resolve) => setTimeout(resolve, 2000));

      const cameraReady = await verifyCameraReady(5, 500);

      if (!cameraReady) {
        throw new Error("Camera verification failed");
      }

      setCameraInitStatus("ready");
      triggerToast("Camera enabled - tracking resumed", "success");

      if (!isTracking && sessionActive && !isSessionTerminated && !isOnBreak) {
        setIsTracking(true);

        if (intervalRef.current) {
          clearInterval(intervalRef.current);
        }

        intervalRef.current = setInterval(() => {
          if (mountedRef.current && !isSessionTerminated && !isOnBreak) {
            captureAndAnalyze();
          }
        }, 3000);

        setTimeout(() => {
          if (mountedRef.current && !isSessionTerminated && !isOnBreak) {
            captureAndAnalyze();
          }
        }, 1000);
      }
    } catch (error) {
      setCameraInitStatus("failed");
      setCameraInitError(error.message);
      triggerToast(`Failed to enable camera: ${error.message}`, "error");
    }
  }, [
    onCameraToggle,
    triggerToast,
    verifyCameraReady,
    isTracking,
    sessionActive,
    isSessionTerminated,
    isOnBreak,
    captureAndAnalyze,
  ]);

  // ==================== BREAK TIMER ====================
  const startBreakTimer = useCallback(
    (duration) => {
      setBreakStartedAt(Date.now());

      if (breakTimerRef.current) {
        clearInterval(breakTimerRef.current);
        breakTimerRef.current = null;
      }

      setBreakTimeLeft(duration);

      breakTimerRef.current = setInterval(() => {
        setBreakTimeLeft((prev) => {
          const newTime = prev - 1;

          if (newTime <= 0) {
            clearInterval(breakTimerRef.current);
            breakTimerRef.current = null;

            if (
              mountedRef.current &&
              !isSessionTerminated &&
              !breakProcessingRef.current &&
              !autoResumeInProgressRef.current
            ) {
              handleEndBreak();
            }

            return 0;
          }

          if (newTime === 10) {
            showViolationPopup("10 seconds remaining on break", "info");
          }

          return newTime;
        });
      }, 1000);
    },
    [isSessionTerminated, showViolationPopup, handleEndBreak]
  );

  // ==================== BACKEND SYNC ====================
  const syncWithBackend = useCallback(async () => {
    if (!meetingId || !userId || isSessionTerminated) return;

    try {
      const response = await apiCall(
        `/status/?meeting_id=${meetingId}&user_id=${userId}`,
        "GET"
      );

      const backendOnBreak = response.is_on_break || false;
      const backendBreakTimeRemaining = response.break_time_remaining || 0;
      const backendTotalUsed = response.total_break_time_used || 0;
      const backendBreakCount = response.break_count || 0;

      if (backendBreakCount !== breakCount) {
        setBreakCount(backendBreakCount);
      }

      if (Math.abs(breakTimeRemaining - backendBreakTimeRemaining) > 5) {
        setBreakTimeRemaining(backendBreakTimeRemaining);
      }

      if (Math.abs(totalBreakTimeUsed - backendTotalUsed) > 2) {
        setTotalBreakTimeUsed(backendTotalUsed);
      }

      if (isOnBreak !== backendOnBreak) {
        setIsOnBreak(backendOnBreak);

        if (!backendOnBreak && breakTimerRef.current) {
          clearInterval(breakTimerRef.current);
          breakTimerRef.current = null;
          setBreakTimeLeft(0);
          setBreakStartedAt(null);
          breakProcessingRef.current = false;
          autoResumeInProgressRef.current = false;
          setIsProcessingBreak(false);

          if (cameraWasEnabledBeforeBreakRef.current && onCameraToggle) {
            onCameraToggle(true);
            setCameraEnabled(true);
          }
        } else if (
          backendOnBreak &&
          !breakTimerRef.current &&
          !breakProcessingRef.current
        ) {
          const remainingTime = Math.max(0, backendBreakTimeRemaining);
          if (remainingTime > 0) {
            setBreakTimeLeft(remainingTime);
            startBreakTimer(remainingTime);
          }
        }
      }
    } catch (error) {
      console.warn("Backend sync failed:", error.message);
    }
  }, [
    meetingId,
    userId,
    isSessionTerminated,
    isOnBreak,
    breakTimeRemaining,
    totalBreakTimeUsed,
    breakCount,
    onCameraToggle,
    startBreakTimer,
    apiCall,
  ]);

  // ==================== TAKE BREAK ====================
  const handleTakeBreak = useCallback(async () => {
    if (currentTrackingMode !== "participant") {
      showViolationPopup("Break only available for participants", "warning");
      return;
    }

    if (
      breakProcessingRef.current ||
      isSessionTerminated ||
      isSessionPermanentlyEnded ||
      isOnBreak ||
      !canTakeBreak
    ) {
      return;
    }

    breakProcessingRef.current = true;
    setIsProcessingBreak(true);

    try {
      cameraWasEnabledBeforeBreakRef.current = cameraEnabled;

      const response = await apiCall("/pause-resume/", "POST", {
        meeting_id: meetingId,
        user_id: userId,
        action: "pause",
      });

      if (response.success) {
        setIsOnBreak(true);
        setCurrentViolations([]);
        setTotalBreakTimeUsed(response.total_break_time_used || 0);
        setBreakTimeRemaining(response.break_time_remaining || 0);
        setBreakCount(response.break_count || 0);

        if (cameraEnabled && onCameraToggle) {
          onCameraToggle(false);
          setCameraEnabled(false);
        }

        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = null;
        }

        // Close camera prompt if it was open
        setShowCameraEnablePrompt(false);
        setCameraPromptDismissed(false);
        if (cameraPromptTimerRef.current) {
          clearTimeout(cameraPromptTimerRef.current);
          cameraPromptTimerRef.current = null;
        }

        const breakDuration =
          response.break_duration || response.break_time_remaining || 300;

        setBreakTimeLeft(breakDuration);
        startBreakTimer(breakDuration);

        const displayMinutes = Math.floor(breakDuration / 60);
        const displaySeconds = breakDuration % 60;

        showViolationPopup(
          `Break #${response.break_count
          } started - camera disabled. ${displayMinutes}:${displaySeconds
            .toString()
            .padStart(2, "0")} available.`,
          "success"
        );
      } else {
        throw new Error(response.error || "Failed to start break");
      }
    } catch (error) {
      showViolationPopup(`Break failed: ${error.message}`, "error");
    } finally {
      breakProcessingRef.current = false;
      setIsProcessingBreak(false);
    }
  }, [
    currentTrackingMode,
    isOnBreak,
    isSessionTerminated,
    isSessionPermanentlyEnded,
    canTakeBreak,
    cameraEnabled,
    onCameraToggle,
    meetingId,
    userId,
    showViolationPopup,
    apiCall,
    startBreakTimer,
  ]);

  // ==================== ROLE TRANSITION ====================
  const handleRoleTransition = useCallback(
    (newMode) => {
      if (newMode === currentTrackingMode) return;

      setRoleTransitionInProgress(true);

      const roleChange = {
        fromRole: currentTrackingMode,
        toRole: newMode,
        timestamp: Date.now(),
        attendanceAtTransition: attendanceData.attendancePercentage,
      };

      roleHistoryRef.current.push(roleChange);

      setAttendanceData((prev) => ({
        ...prev,
        roleHistory: [...roleHistoryRef.current],
      }));

      if (newMode !== "participant") {
        setCurrentViolations([]);
        setWarningsExhausted(false);
      }

      setCurrentTrackingMode(newMode);
      setRoleTransitionInProgress(false);
    },
    [currentTrackingMode, attendanceData.attendancePercentage]
  );

  // ==================== START TRACKING ====================
  const startTracking = useCallback(async () => {
    if (!meetingId || !userId || !mountedRef.current) {
      return false;
    }

    if (isSessionTerminated || isSessionPermanentlyEnded) {
      return false;
    }

    try {
      if (!videoReady && !isOnBreak) {
        const cameraReady = await initializeCamera();
        if (!cameraReady || !mountedRef.current || isSessionTerminated) {
          showViolationPopup("Camera initialization failed", "error");
          return false;
        }
      }

      const response = await apiCall("/start/", "POST", {
        meeting_id: meetingId,
        user_id: userId,
        user_name: userName,
        user_role: effectiveRole,
        current_tracking_mode: currentTrackingMode,
        is_host: isHost,
        is_cohost: isCoHost,
        should_detect_violations: shouldDetectViolations,
      });

      if (!mountedRef.current || isSessionTerminated) {
        return false;
      }

      setSessionActive(true);
      setIsTracking(true);
      if (currentTrackingMode === "participant") {
        setWarningCount(0);
        setWarningsExhausted(false);
        setFaceAuthStatus("verified");
        setIsAuthBlocked(false);
      }

      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }

      intervalRef.current = setInterval(() => {
        if (mountedRef.current && !isSessionTerminated) {
          captureAndAnalyze();
        } else {
          if (intervalRef.current) {
            clearInterval(intervalRef.current);
            intervalRef.current = null;
          }
        }
      }, 3000);

      if (!isOnBreak) {
        setTimeout(() => {
          if (mountedRef.current && !isOnBreak && !isSessionTerminated) {
            captureAndAnalyze();
          }
        }, 500);
      }

      if (showSuccessPopup) {
        const message =
          currentTrackingMode === "participant"
            ? "AI attendance monitoring started"
            : `Presence tracking started for ${currentTrackingMode}`;
        showViolationPopup(message, "success");
      }

      return true;
    } catch (error) {
      console.error("Failed to start:", error);
      showViolationPopup(`Failed to start: ${error.message}`, "error");
      return false;
    }
  }, [
    meetingId,
    userId,
    userName,
    currentTrackingMode,
    videoReady,
    initializeCamera,
    captureAndAnalyze,
    showViolationPopup,
    showSuccessPopup,
    effectiveRole,
    isHost,
    isCoHost,
    shouldDetectViolations,
    isOnBreak,
    apiCall,
    isSessionTerminated,
    isSessionPermanentlyEnded,
  ]);

  // ==================== STOP TRACKING ====================
  const stopTracking = useCallback(async () => {
    try {
      setIsTracking(false);

      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }

      if (breakTimerRef.current) {
        clearInterval(breakTimerRef.current);
        breakTimerRef.current = null;
      }

      if (cameraPromptTimerRef.current) {
        clearTimeout(cameraPromptTimerRef.current);
        cameraPromptTimerRef.current = null;
      }

      if (meetingId && userId && !isSessionTerminated) {
        await apiCall("/stop/", "POST", {
          meeting_id: meetingId,
          user_id: userId,
          reason: "manual_stop",
        });
      }

      if (streamRef.current) {
        streamRef.current.getTracks().forEach((track) => track.stop());
        setVideoReady(false);
      }
    } catch (error) {
      console.error("Failed to stop:", error);
    }
  }, [meetingId, userId, isSessionTerminated, apiCall]);

  // ==================== EFFECTS ====================
  useEffect(() => {
    const newMode = determineTrackingMode();
    if (newMode !== currentTrackingMode) {
      handleRoleTransition(newMode);
    }
  }, [determineTrackingMode, currentTrackingMode, handleRoleTransition]);

  // ==================== CAMERA ENABLE PROMPT EFFECT ====================
  useEffect(() => {
    // Clear any existing timer
    if (cameraPromptTimerRef.current) {
      clearTimeout(cameraPromptTimerRef.current);
      cameraPromptTimerRef.current = null;
    }

    // Close prompt if conditions no longer met
    if (
      isOnBreak ||
      isSessionTerminated ||
      isSessionPermanentlyEnded ||
      currentTrackingMode !== "participant" ||
      !sessionActive ||
      propCameraEnabled
    ) {
      setShowCameraEnablePrompt(false);
      setCameraPromptDismissed(false);
      return;
    }

    // Show prompt if camera is disabled (not during break)
    if (
      !propCameraEnabled &&
      currentTrackingMode === "participant" &&
      sessionActive &&
      !isSessionTerminated &&
      !isSessionPermanentlyEnded &&
      !isOnBreak &&
      !cameraPromptDismissed
    ) {
      cameraPromptTimerRef.current = setTimeout(() => {
        if (
          mountedRef.current &&
          !propCameraEnabled &&
          !isOnBreak &&
          !isSessionTerminated &&
          !cameraPromptDismissed
        ) {
          setShowCameraEnablePrompt(true);
        }
      }, AttendanceConfig.CAMERA_PROMPT_DELAY);
    }

    return () => {
      if (cameraPromptTimerRef.current) {
        clearTimeout(cameraPromptTimerRef.current);
        cameraPromptTimerRef.current = null;
      }
    };
  }, [
    propCameraEnabled,
    isOnBreak,
    isSessionTerminated,
    isSessionPermanentlyEnded,
    currentTrackingMode,
    sessionActive,
    cameraPromptDismissed,
  ]);

  useEffect(() => {
    const prevEnabled = cameraEnabled;

    setCameraEnabled(propCameraEnabled);

    if (prevEnabled !== propCameraEnabled) {
      if (!propCameraEnabled) {
        setCurrentViolations([]);

        if (isTracking && !isOnBreak && currentTrackingMode === "participant") {
          setIsTracking(false);
          if (intervalRef.current) {
            clearInterval(intervalRef.current);
            intervalRef.current = null;
          }
          showViolationPopup("Tracking paused - camera disabled", "warning");
        }
      } else {
        // Close the camera prompt when camera is enabled
        setShowCameraEnablePrompt(false);
        setCameraPromptDismissed(false);
        
        if (
          !isTracking &&
          !isOnBreak &&
          sessionActive &&
          !isSessionTerminated
        ) {
          const message =
            currentTrackingMode === "participant"
              ? "Camera enabled - resuming detection"
              : "Camera enabled - presence tracking active";
          showViolationPopup(message, "info");

          setIsTracking(true);

          if (intervalRef.current) {
            clearInterval(intervalRef.current);
            intervalRef.current = null;
          }

          intervalRef.current = setInterval(() => {
            if (mountedRef.current && !isSessionTerminated) {
              captureAndAnalyze();
            }
          }, 3000);

          setTimeout(() => {
            if (mountedRef.current && !isSessionTerminated && !isOnBreak) {
              captureAndAnalyze();
            }
          }, 1000);
        }
      }
    }
  }, [
    propCameraEnabled,
    isTracking,
    isOnBreak,
    sessionActive,
    showViolationPopup,
    isSessionTerminated,
    currentTrackingMode,
    cameraEnabled,
    captureAndAnalyze,
  ]);

  useEffect(() => {
    if (!meetingId || !userId || isSessionTerminated) return;

    const syncInterval = setInterval(syncWithBackend, isOnBreak ? 5000 : 10000);

    return () => clearInterval(syncInterval);
  }, [meetingId, userId, isSessionTerminated, isOnBreak, syncWithBackend]);

  useEffect(() => {
    mountedRef.current = true;

    if (isSessionTerminated || isSessionPermanentlyEnded) {
      return;
    }

    if (meetingId && userId) {
      const initialize = async () => {
        try {
          await syncWithBackend();

          const success = await startTracking();
        } catch (error) {
          console.error("Init error:", error);
        }
      };

      const timeoutId = setTimeout(initialize, 500);

      return () => {
        clearTimeout(timeoutId);
      };
    }
  }, [
    meetingId,
    userId,
    currentTrackingMode,
    isSessionTerminated,
    isSessionPermanentlyEnded,
    cameraEnabled,
    syncWithBackend,
    startTracking,
  ]);

  useEffect(() => {
    return () => {
      mountedRef.current = false;

      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
      if (breakTimerRef.current) {
        clearInterval(breakTimerRef.current);
        breakTimerRef.current = null;
      }
      if (cameraPromptTimerRef.current) {
        clearTimeout(cameraPromptTimerRef.current);
        cameraPromptTimerRef.current = null;
      }
      if (streamRef.current) {
        streamRef.current.getTracks().forEach((track) => track.stop());
        streamRef.current = null;
      }

      setIsTracking(false);
      setSessionActive(false);
      setVideoReady(false);
    };
  }, []);

  // ==================== STATUS HELPERS ====================
  const getStatus = () => {
    if (isAuthBlocked) return "unauthorized";
    if (isSessionTerminated) return "terminated";
    if (!sessionActive) return "ended";
    if (isOnBreak) return "break";
    if (authWarningCount > 0 && faceAuthStatus === "unauthorized") return "unauthorized";
    if (currentViolations.length > 0) return "violation";
    if (attendanceData.attendancePercentage < 80) return "warning";
    return "active";
  };

  const getStatusIcon = () => {
    const status = getStatus();
    const iconProps = { sx: { fontSize: 16 } };
    switch (status) {
      case "active":
        return <CheckCircle {...iconProps} sx={{ ...iconProps.sx, color: "#4caf50" }} />;
      case "warning":
        return <Warning {...iconProps} sx={{ ...iconProps.sx, color: "#ff9800" }} />;
      case "violation":
        return <ErrorIcon {...iconProps} sx={{ ...iconProps.sx, color: "#f44336" }} />;
      case "break":
        return <Coffee {...iconProps} sx={{ ...iconProps.sx, color: "#2196f3" }} />;
      case "initializing":
        return <CircularProgress size={16} sx={{ color: "#ffc107" }} />;
      case "host_tracking":
        return <CheckCircle {...iconProps} sx={{ ...iconProps.sx, color: "#4caf50" }} />;
      case "ended":
        return <ErrorIcon {...iconProps} sx={{ ...iconProps.sx, color: "#666" }} />;
      case "terminated":
        return <ErrorIcon {...iconProps} sx={{ ...iconProps.sx, color: "#f44336" }} />;
      case "unauthorized":
        return <PersonOff {...iconProps} sx={{ ...iconProps.sx, color: "#9c27b0" }} />;
      default:
        return <CheckCircle {...iconProps} sx={{ ...iconProps.sx, color: "#4caf50" }} />;
    }
  };

  // ==================== RENDER ====================
  if (!meetingId || !userId) return null;

  return (
    <>
      <AttendanceContainer>
        <AttendanceIndicator>
          <CardContent sx={{ p: 2 }}>
            {!minimized ? (
              <>
                {/* Header */}
                <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    {currentTrackingMode === "host" ? (
                      <AdminPanelSettings sx={{ fontSize: 16, color: "#ff9800" }} />
                    ) : currentTrackingMode === "cohost" ? (
                      <SupervisorAccount sx={{ fontSize: 16, color: "#ff5722" }} />
                    ) : (
                      getStatusIcon()
                    )}
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, fontSize: "0.85rem" }}>
                      {currentTrackingMode === "host" ? "Meeting Host" : currentTrackingMode === "cohost" ? "Meeting Co-Host" : "AI Monitor"}
                    </Typography>
                  </Box>
                  <Box sx={{ display: "flex", gap: 1 }}>
                    {!isSessionTerminated && (cameraEnabled ? (
                        <Tooltip title="Camera Active">
                          <Videocam sx={{ fontSize: 16, color: "#4caf50" }} />
                        </Tooltip>
                      ) : (
                        <Tooltip title="Camera Disabled">
                          <VideocamOff sx={{ fontSize: 16, color: "#f44336" }} />
                        </Tooltip>
                      ))}
                     {onToggleMinimized && (
                      <IconButton size="small" onClick={onToggleMinimized} sx={{ color: "white" }}>
                        <VisibilityOff sx={{ fontSize: 16 }} />
                      </IconButton>
                    )}
                  </Box>
                </Box>

                {currentTrackingMode === "host" || currentTrackingMode === "cohost" ? (
                  <>
                     <StatusChip label={currentTrackingMode === "host" ? "HOST PRIVILEGES" : "CO-HOST PRIVILEGES"} status="excluded" size="small" sx={{ mb: 1.5 }} />
                     <Alert severity="info" sx={{ mb: 1.5, fontSize: "0.75rem" }}>
                        <Typography variant="caption" sx={{ fontWeight: 600, display: "block", color: "#2196f3" }}>
                           {currentTrackingMode === "host" ? "HOST PRIVILEGES:" : "CO-HOST PRIVILEGES:"}
                        </Typography>
                        <Typography variant="caption" sx={{ fontSize: "0.7rem", color: "#90caf9", display: "block" }}>
                           Presence tracking active
                        </Typography>
                     </Alert>
                  </>
                ) : (
                  <>
                    {/* Status Chip */}
                     <StatusChip
                        label={
                        isOnBreak ? "ON BREAK" :
                        isSessionTerminated ? "TERMINATED" :
                        isAuthBlocked ? "BLOCKED" :
                        authWarningCount > 0 ? `AUTH WARN ${authWarningCount}/3` :
                        `Active • ${Math.round(attendanceData.attendancePercentage)}%`
                        }
                        status={getStatus()}
                        size="small"
                        sx={{ mb: 1.5 }}
                    />

                    {/* Warnings Progress Bars */}
                    {!isSessionTerminated && currentTrackingMode === "participant" && (
                      <>
                         {/* Behavioral */}
                         <Box sx={{ mb: 1 }}>
                            <Typography variant="caption" sx={{ color: "#ff9800", fontSize: "0.7rem" }}>
                               Behavioral: {warningCount}/{AttendanceConfig.MAX_WARNING_MESSAGES}
                            </Typography>
                            <LinearProgress variant="determinate" value={(warningCount/AttendanceConfig.MAX_WARNING_MESSAGES)*100} color="warning" sx={{height: 4}} />
                         </Box>
                         {/* Identity */}
                         <Box sx={{ mb: 1 }}>
                            <Typography variant="caption" sx={{ color: "#9c27b0", fontSize: "0.7rem" }}>
                               Identity: {authWarningCount}/3
                            </Typography>
                            <LinearProgress variant="determinate" value={(authWarningCount/3)*100} color="secondary" sx={{height: 4}} />
                         </Box>
                      </>
                    )}

                    {/* Violations Text List (Passive) */}
                    {currentViolations.length > 0 && (
                         <Alert severity="warning" sx={{ mt: 1, py: 0, fontSize: '0.75rem' }}>
                            {currentViolations[0]}
                         </Alert>
                    )}

                    <Box sx={{ display: "flex", gap: 0.5, justifyContent: "center", mt: 1 }}>
                        {!isOnBreak && canTakeBreak && (
                            <Tooltip title={`Take break (${Math.floor(breakTimeRemaining)}s available)`}>
                              <IconButton
                                size="small"
                                onClick={handleTakeBreak}
                                disabled={isProcessingBreak}
                                sx={{
                                  color: "#2196f3",
                                  backgroundColor: "rgba(33,150,243,0.1)",
                                  "&:hover": { backgroundColor: "rgba(33,150,243,0.2)" },
                                  "&:disabled": { color: "#666", backgroundColor: "rgba(0,0,0,0.1)" },
                                }}
                              >
                                <Coffee sx={{ fontSize: 27 }} />
                              </IconButton>
                            </Tooltip>
                        )}
                        {isOnBreak && (
                            <Button
                                size="small"
                                variant="outlined"
                                onClick={handleEndBreak}
                                disabled={isProcessingBreak || autoResumeInProgressRef.current}
                                startIcon={<Stop />}
                                sx={{
                                  fontSize: "0.65rem",
                                  color: "#f44336",
                                  borderColor: "#f44336",
                                  "&:hover": { backgroundColor: "rgba(244,67,54,0.1)" },
                                }}
                              >
                                End Break
                            </Button>
                        )}
                    </Box>
                  </>
                )}
              </>
            ) : (
               <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                 {getStatusIcon()}
                 {onToggleMinimized && <IconButton size="small" onClick={onToggleMinimized} sx={{ color: "white" }}><Visibility sx={{ fontSize: 16 }} /></IconButton>}
               </Box>
            )}
          </CardContent>
        </AttendanceIndicator>
      </AttendanceContainer>

      {/* Hidden Video Elements */}
      {!isSessionTerminated && (
        <>
          <video ref={videoRef} autoPlay muted playsInline style={{ display: "none" }} />
          <canvas ref={canvasRef} style={{ display: "none" }} />
        </>
      )}

      {/* ✅ TOAST NOTIFICATIONS (Snackbar) */}
      <Snackbar
        open={toastOpen}
        autoHideDuration={5000}
        onClose={handleToastClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert 
            onClose={handleToastClose} 
            severity={toastSeverity === "violation" ? "warning" : toastSeverity} 
            variant={toastVariant}
            sx={{ width: '100%', boxShadow: 3, fontWeight: 600 }}
            icon={
                toastMessage.includes("Identity") ? <PersonOff /> : 
                toastSeverity === "error" ? <ErrorIcon /> :
                toastSeverity === "warning" ? <Warning /> : undefined
            }
        >
          {toastMessage}
        </Alert>
      </Snackbar>

      {/* ✅ CAMERA ENABLE PROMPT (Separate from Termination) */}
      <CameraPromptDialog
        open={showCameraEnablePrompt}
        disableEscapeKeyDown={false}
        onClose={() => {
          setShowCameraEnablePrompt(false);
          setCameraPromptDismissed(true);
        }}
      >

        <DialogTitle sx={{ textAlign: 'center', pb: 1, pt: 3 }}>
          <Box sx={{ 
            display: 'flex', 
            alignItems: 'center', 
            justifyContent: 'center',
            mb: 2
          }}>
              <IconButton
    aria-label="close"
    onClick={() => {
      setShowCameraEnablePrompt(false);
      setCameraPromptDismissed(true);
    }}
    sx={{
      position: 'absolute',
      right: 8,
      top: 8,
      color: '#888',
      '&:hover': { color: '#555' }
    }}
  >
    <Close />
  </IconButton>
            <Box sx={{
              width: 64,
              height: 64,
              borderRadius: '50%',
              backgroundColor: 'rgba(255, 152, 0, 0.1)',
              border: '2px solid #FF9800',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}>
              <VideocamOutlined sx={{ fontSize: 36, color: '#FF9800' }} />
            </Box>
          </Box>
          <Typography variant="h6" sx={{ fontWeight: 700, color: '#FF9800' }}>
            Camera Disabled
          </Typography>
        </DialogTitle>
        <DialogContent sx={{ textAlign: 'center', px: 4, pb: 2 }}>
          <Alert severity="warning" variant="outlined" sx={{ mb: 2 }}>
            Attendance tracking is paused because your camera is disabled.
          </Alert>
          <Typography variant="body2" sx={{ color: '#555', mb: 2 }}>
            To resume AI-powered attendance monitoring and tracking, please enable your camera.
          </Typography>
          <Typography variant="caption" sx={{ color: '#999', display: 'block' }}>
            Your attendance percentage: <strong>{Math.round(attendanceData.attendancePercentage)}%</strong>
          </Typography>
        </DialogContent>
        <DialogActions sx={{ justifyContent: 'center', pb: 3, px: 3, gap: 1 }}>
          <Button
            variant="contained"
            onClick={handleEnableCameraFromPrompt}
            startIcon={<Videocam />}
            sx={{ 
              backgroundColor: '#FF9800',
              color: 'white',
              fontWeight: 600,
              '&:hover': { backgroundColor: '#F57C00' }
            }}
          >
            Enable Camera
          </Button>
        </DialogActions>
      </CameraPromptDialog>

      {/* ✅ TERMINATION POPUP (Only for Termination/Blocking) */}
      <AttendancePopup
        open={showTerminationPopup}
        message={terminationMessage}
        type="error"
        onClose={() => {}}
        hideCloseButton={true}
        attendanceData={attendanceData}
        faceAuthStatus={faceAuthStatus}
      />
    </>
  );
};

export default AttendanceTracker;