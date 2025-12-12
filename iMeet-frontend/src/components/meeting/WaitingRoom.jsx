// src/components/meeting/WaitingRoom.jsx - Fixed Camera/Mic Toggle Logic & Face Auth API
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  TextField,
  IconButton,
  Avatar,
  Card,
  CardContent,
  Alert,
  useTheme,
  useMediaQuery,
  CircularProgress,
  Stack,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Tooltip,
  Menu,
  MenuItem,
  Divider
} from '@mui/material';

import {
  Videocam,
  VideocamOff,
  Mic,
  MicOff,
  ContentCopy,
  CheckCircle,
  PhotoCamera,
  CameraAlt,
  Security,
  FaceRetouchingNatural,
  Verified,
  Brightness6,
  PersonOutline,
  MoreVert,
  Refresh,
  Settings,
  Help,
  Feedback
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { useAuth } from '../../hooks/useAuth';
import { useCameraAccess } from '../../hooks/useCameraAccess';

// ==================== STYLED COMPONENTS ====================

const MainContainer = styled(Box)(({ theme }) => ({
  Height: '100vh',
  backgroundColor: '#ffffff',
  padding: theme.spacing(5),
  overflow:"scroll",
  
  [theme.breakpoints.down('lg')]: {
    padding: theme.spacing(2),
  },
  
  [theme.breakpoints.down('md')]: {
    alignItems: 'flex-start',
    padding: theme.spacing(2, 1),
  }
}));

const ContentWrapper = styled(Box)(({ theme }) => ({
  display: 'grid',
  gridTemplateColumns: '1.5fr 1fr',
  gap: theme.spacing(4),
  maxWidth: 1280,
  width: '100%',
  
  [theme.breakpoints.down('lg')]: {
    gridTemplateColumns: '1.3fr 1fr',
    gap: theme.spacing(3),
  },
  
  [theme.breakpoints.down('md')]: {
    gridTemplateColumns: '1fr',
    gap: theme.spacing(2),
  }
}));

const LeftSection = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(3),
  
  [theme.breakpoints.down('md')]: {
    gap: theme.spacing(2),
  }
}));

const RightSection = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(4),
  
  [theme.breakpoints.down('md')]: {
    gap: theme.spacing(2),
  }
}));

const VideoContainer = styled(Box)(({ theme }) => ({
  position: 'relative',
  width: '100%',
  aspectRatio: '16 / 9',
  borderRadius: 16,
  overflow: 'hidden',
  backgroundColor: '#000000',
  border: '3px solid #E0E7FF',
  boxShadow: '0 2px 8px rgba(0, 0, 0, 0.05)',
  
  [theme.breakpoints.down('md')]: {
    borderRadius: 12,
    border: '2px solid #E0E7FF',
  }
}));

const FaceVerifiedBadge = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: 16,
  left: 16,
  backgroundColor: '#E6F4EA',
  color: '#137333',
  padding: '6px 12px',
  borderRadius: 20,
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  fontSize: '0.875rem',
  fontWeight: 500,
  zIndex: 10,
  boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
  
  [theme.breakpoints.down('sm')]: {
    top: 12,
    left: 12,
    fontSize: '0.75rem',
    padding: '4px 10px',
  }
}));

const MoreOptionsButton = styled(IconButton)(({ theme }) => ({
  position: 'absolute',
  top: 16,
  right: 16,
  backgroundColor: 'rgba(0, 0, 0, 0.5)',
  color: 'white',
  width: 40,
  height: 40,
  zIndex: 10,
  
  '&:hover': {
    backgroundColor: 'rgba(0, 0, 0, 0.7)',
  },
  
  [theme.breakpoints.down('sm')]: {
    top: 12,
    right: 12,
    width: 36,
    height: 36,
  }
}));

const VideoElement = styled('video')({
  width: '100%',
  height: '100%',
  objectFit: 'cover',
});

const VideoPlaceholder = styled(Box)(({ theme }) => ({
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  backgroundColor: '#1a1a1a',
  color: 'white',
  gap: theme.spacing(2),
}));

const ResponsiveAvatar = styled(Avatar)(({ theme }) => ({
  width: 100,
  height: 100,
  fontSize: '2.5rem',
  fontWeight: 500,
  backgroundColor: '#5F6368',
  
  [theme.breakpoints.down('md')]: {
    width: 80,
    height: 80,
    fontSize: '2rem',
  }
}));

const ControlsOverlay = styled(Box)(({ theme }) => ({
  position: 'absolute',
  bottom: 16,
  left: '50%',
  transform: 'translateX(-50%)',
  display: 'flex',
  gap: theme.spacing(1.5),
  zIndex: 10,
  
  [theme.breakpoints.down('sm')]: {
    bottom: 12,
    gap: theme.spacing(1),
  }
}));

const ControlButton = styled(IconButton)(({ theme }) => ({
  width: 48,
  height: 48,
  backgroundColor: '#ffffff',
  color: '#5F6368',
  boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
  transition: 'all 0.2s ease',
  
  '&:hover': {
    backgroundColor: '#f1f3f4',
    transform: 'scale(1.05)',
  },
  
  '&.active': {
    backgroundColor: '#E8F0FE',
    color: '#1A73E8',
  },
  
  '&.danger': {
    backgroundColor: '#f44336',
    color: 'white',
    
    '&:hover': {
      backgroundColor: '#d32f2f',
    }
  },
  
  '&:disabled': {
    opacity: 0.5,
    cursor: 'not-allowed',
    transform: 'none',
  },
  
  [theme.breakpoints.down('sm')]: {
    width: 44,
    height: 44,
  }
}));

const StatusCard = styled(Card)(({ theme }) => ({
  backgroundColor: '#ffffff',
  borderRadius: 12,
  border: '1px solid #E0E0E0',
  boxShadow: '0 2px 8px rgba(0, 0, 0, 0.05)',
  
  '& .MuiCardContent-root': {
    padding: theme.spacing(2.5),
    '&:last-child': {
      paddingBottom: theme.spacing(2.5),
    }
  },
  
  [theme.breakpoints.down('sm')]: {
    borderRadius: 8,
    '& .MuiCardContent-root': {
      padding: theme.spacing(2),
      '&:last-child': {
        paddingBottom: theme.spacing(2),
      }
    }
  }
}));

const InfoCard = styled(Card)(({ theme }) => ({
  backgroundColor: '#ffffff',
  borderRadius: 12,
  border: '1px solid #E0E0E0',
  boxShadow: '0 2px 8px rgba(0, 0, 0, 0.05)',
  
  '& .MuiCardContent-root': {
    padding: theme.spacing(3),
    '&:last-child': {
      paddingBottom: theme.spacing(3),
    }
  },
  
  [theme.breakpoints.down('sm')]: {
    borderRadius: 8,
    '& .MuiCardContent-root': {
      padding: theme.spacing(2),
      '&:last-child': {
        paddingBottom: theme.spacing(2),
      }
    }
  }
}));

const NameInput = styled(TextField)(({ theme }) => ({
  '& .MuiOutlinedInput-root': {
    borderRadius: 8,
    backgroundColor: '#ffffff',
    
    '& fieldset': {
      borderColor: '#E0E0E0',
    },
    
    '&:hover fieldset': {
      borderColor: '#5F6368',
    },
    
    '&.Mui-focused fieldset': {
      borderColor: '#1A73E8',
      borderWidth: 2,
    }
  },
  
  '& .MuiInputLabel-root': {
    color: '#5F6368',
    
    '&.Mui-focused': {
      color: '#1A73E8',
    }
  }
}));

const JoinButton = styled(Button)(({ theme }) => ({
  borderRadius: 8,
  padding: theme.spacing(1.5),
  fontSize: '1rem',
  fontWeight: 500,
  textTransform: 'none',
  backgroundColor: '#1A73E8',
  color: 'white',
  boxShadow: 'none',
  
  '&:hover': {
    backgroundColor: '#1669C1',
    boxShadow: '0 1px 3px rgba(0, 0, 0, 0.2)',
  },
  
  '&:disabled': {
    backgroundColor: '#E0E0E0',
    color: '#9E9E9E',
  },
  
  [theme.breakpoints.down('sm')]: {
    fontSize: '0.9rem',
    padding: theme.spacing(1.2),
  }
}));

const CopyLinkButton = styled(Button)(({ theme }) => ({
  borderRadius: 8,
  padding: theme.spacing(1.5),
  fontSize: '0.875rem',
  fontWeight: 500,
  textTransform: 'none',
  color: '#1A73E8',
  borderColor: '#1A73E8',
  
  '&:hover': {
    backgroundColor: '#E8F0FE',
    borderColor: '#1A73E8',
  },
  
  [theme.breakpoints.down('sm')]: {
    fontSize: '0.8rem',
    padding: theme.spacing(1.2),
  }
}));

const VerifyButton = styled(Button)(({ theme }) => ({
  borderRadius: 8,
  padding: theme.spacing(1),
  fontSize: '0.875rem',
  fontWeight: 500,
  textTransform: 'none',
  color: '#5F6368',
  borderColor: '#DADCE0',
  
  '&:hover': {
    backgroundColor: '#F8F9FA',
    borderColor: '#DADCE0',
  },
  
  [theme.breakpoints.down('sm')]: {
    fontSize: '0.8rem',
  }
}));

const SuccessBox = styled(Box)(({ theme }) => ({
  backgroundColor: '#E6F4EA',
  color: '#137333',
  padding: theme.spacing(1.5),
  borderRadius: 8,
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  fontSize: '0.875rem',
  fontWeight: 500,
  
  [theme.breakpoints.down('sm')]: {
    padding: theme.spacing(1),
    fontSize: '0.8rem',
  }
}));

const StatusItem = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  padding: theme.spacing(0.5, 0),
  color: '#202124',
  fontSize: '0.875rem',
  
  [theme.breakpoints.down('sm')]: {
    fontSize: '0.8rem',
  }
}));

// ==================== MAIN COMPONENT ====================

const WaitingRoom = ({ 
  meetingId, 
  onJoin, 
  isHost = false, 
  meetingData = null,
  inWaitingRoom = false,
  onAllowUser = null,
  errors = [],
  isConnecting = false,
  connectionState = 'disconnected',
  realMeetingId = null,
  getMeetingIdForSharing = null,
  isConnected = false,
  onWebSocketMessage = null
}) => {
  const theme = useTheme();
  const { user } = useAuth();
  const videoRef = useRef(null);
  
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  const isTablet = useMediaQuery(theme.breakpoints.down('md'));
  
// ==================== CAMERA ACCESS WITH AUTO-MANAGEMENT ====================
const {
  stream: cameraStream,
  isLoading: cameraLoading,
  error: cameraError,
  hasPermission: hasCameraPermission,
  releaseCamera
} = useCameraAccess(true); // true = we're in meeting area

// Combine camera stream with local state
const [stream, setStream] = useState(null);
const [audioStream, setAudioStream] = useState(null);
const [videoEnabled, setVideoEnabled] = useState(true);
const [audioEnabled, setAudioEnabled] = useState(true);
const [audioOutputEnabled, setAudioOutputEnabled] = useState(true);


  
  // UI states
  const [displayName, setDisplayName] = useState(user?.full_name || user?.name || '');
  const [isJoining, setIsJoining] = useState(false);
  const [deviceError, setDeviceError] = useState('');
  const [copied, setCopied] = useState(false);
  const [menuAnchorEl, setMenuAnchorEl] = useState(null);
  const [showTroubleshooting, setShowTroubleshooting] = useState(false);
  const [mediaPermissionGranted, setMediaPermissionGranted] = useState(false);
  const [isInitializing, setIsInitializing] = useState(true);
  
  // Face Authentication States
  const [faceAuthStep, setFaceAuthStep] = useState(0);
  const [isFaceVerifying, setIsFaceVerifying] = useState(false);
  const [faceVerified, setFaceVerified] = useState(false);
  const [faceAuthError, setFaceAuthError] = useState('');
  const [faceAuthData, setFaceAuthData] = useState(null);
  const [showFaceAuthDialog, setShowFaceAuthDialog] = useState(false);
  const [autoVerifyAttempted, setAutoVerifyAttempted] = useState(false);
  
  // Device states
  const [mediaInitializationAttempts, setMediaInitializationAttempts] = useState(0);
  const [deviceStatus, setDeviceStatus] = useState({
    camera: 'checking',
    microphone: 'checking',
    permissions: { camera: 'unknown', microphone: 'unknown' },
    quality: 'unknown',
    bandwidth: 'unknown'
  });
  
  // Meeting data
  const meeting = meetingData || {
    title: meetingId === 'instant' ? 'Instant Meeting' : 'Professional Video Meeting',
    host_name: user?.full_name || user?.name || 'Meeting Host',
    scheduled_time: new Date(),
    participants_count: 0,
    participants: []
  };

  // ✅ FIXED: Correct API endpoint matching backend
  const FACE_AUTH_API_URL = 'https://192.168.48.201:8111/api/user/verify-face';

  // ==================== MEDIA FUNCTIONS - FIXED ====================

  const initializeMediaWithRetry = useCallback(async (attempt = 1) => {
    const maxAttempts = 3;
    setMediaInitializationAttempts(attempt);
    setIsInitializing(true);
    
    try {
      console.log(`🎥 Media initialization attempt ${attempt}/${maxAttempts}...`);
      
      await checkDeviceAvailability();
      
      const constraints = {
        video: {
          width: { ideal: 1280, min: 640, max: 1920 },
          height: { ideal: 720, min: 480, max: 1080 },
          frameRate: { ideal: 30, min: 15, max: 60 },
          facingMode: 'user'
        },
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
          sampleRate: { ideal: 48000 }
        }
      };

      console.log('📷 Requesting media with constraints:', constraints);

      const mediaStream = await navigator.mediaDevices.getUserMedia(constraints);
      
      if (mediaStream) {
        console.log('✅ Media stream obtained:', {
          videoTracks: mediaStream.getVideoTracks().length,
          audioTracks: mediaStream.getAudioTracks().length
        });
        
        setStream(mediaStream);
        setDeviceError('');
        setMediaPermissionGranted(true);
        setIsInitializing(false);
        
        // Set initial track states
        const videoTrack = mediaStream.getVideoTracks()[0];
        const audioTrack = mediaStream.getAudioTracks()[0];
        
        if (videoTrack) {
          videoTrack.enabled = videoEnabled;
          console.log('📹 Video track enabled:', videoTrack.enabled);
        }
        if (audioTrack) {
          audioTrack.enabled = audioEnabled;
          console.log('🎤 Audio track enabled:', audioTrack.enabled);
        }
        
        setDeviceStatus(prev => ({
          ...prev,
          camera: mediaStream.getVideoTracks().length > 0 ? 'available' : 'unavailable',
          microphone: mediaStream.getAudioTracks().length > 0 ? 'available' : 'unavailable',
          quality: 'good',
          permissions: { camera: 'granted', microphone: 'granted' }
        }));
        
        console.log('✅ Media initialization successful');
        return;
      }
    } catch (error) {
      console.error(`❌ Media initialization attempt ${attempt} failed:`, error);
      
      setIsInitializing(false);
      const errorAnalysis = analyzeMediaError(error);
      setDeviceError(errorAnalysis.message);
      setDeviceStatus(prev => ({ ...prev, ...errorAnalysis.deviceStatus }));
      
      if (attempt < maxAttempts && (error.name !== 'NotAllowedError')) {
        const delay = attempt * 2000;
        console.log(`⏳ Retrying in ${delay}ms...`);
        setTimeout(() => initializeMediaWithRetry(attempt + 1), delay);
      } else {
        console.error('❌ All media initialization attempts failed');
        if (error.name === 'NotAllowedError') {
          setDeviceError('Camera and microphone access denied. Please allow access and refresh.');
        } else {
          setDeviceError('Failed to access camera/microphone. Please check your devices.');
        }
      }
    }
  }, []);

  const checkDeviceAvailability = async () => {
    try {
      const devices = await navigator.mediaDevices.enumerateDevices();
      const videoDevices = devices.filter(d => d.kind === 'videoinput' && d.deviceId);
      const audioDevices = devices.filter(d => d.kind === 'audioinput' && d.deviceId);
      
      console.log('📱 Devices found:', {
        video: videoDevices.length,
        audio: audioDevices.length
      });
      
      if (videoDevices.length === 0) {
        setDeviceStatus(prev => ({ ...prev, camera: 'unavailable' }));
      }
      
      if (audioDevices.length === 0) {
        setDeviceStatus(prev => ({ ...prev, microphone: 'unavailable' }));
      }
    } catch (error) {
      console.error('❌ Device check failed:', error);
    }
  };

  const analyzeMediaError = (error) => {
    let message = 'Camera and microphone access required';
    let deviceStatus = {};
    
    if (error.name === 'NotAllowedError') {
      message = 'Camera and microphone access denied. Please allow access and refresh.';
      deviceStatus = { camera: 'permission_denied', microphone: 'permission_denied' };
    } else if (error.name === 'NotFoundError') {
      message = 'No camera or microphone found. Please connect your devices.';
      deviceStatus = { camera: 'unavailable', microphone: 'unavailable' };
    } else if (error.name === 'NotReadableError') {
      message = 'Camera is in use by another application. Please close other apps.';
      deviceStatus = { camera: 'in_use' };
    }
    
    return { message, deviceStatus };
  };

// ==================== TOGGLE VIDEO ====================
const toggleVideo = useCallback(() => {
  console.log('🎬 Toggle video clicked');
  
  if (!stream) {
    console.warn('⚠️ No stream available');
    setDeviceError('No camera stream available. Please refresh.');
    return;
  }

  const videoTrack = stream.getVideoTracks()[0];
  
  if (!videoTrack) {
    console.warn('⚠️ No video track found');
    setDeviceError('No video track available.');
    return;
  }

  setVideoEnabled(prevState => {
    const newState = !prevState;
    videoTrack.enabled = newState;
    
    console.log('📹 Video toggled:', {
      previousState: prevState,
      newState,
      trackEnabled: videoTrack.enabled
    });
    
    return newState;
  });
}, [stream]);

  // Add this new effect to sync video element with track state
  useEffect(() => {
    if (stream && videoRef.current && videoEnabled) {
      const videoTrack = stream.getVideoTracks()[0];
      
      if (videoTrack && videoTrack.enabled && videoTrack.readyState === 'live') {
        console.log('🔄 Syncing video element with active track');
        
        // Ensure video element is connected and playing
        if (videoRef.current.srcObject !== stream) {
          videoRef.current.srcObject = stream;
        }
        
        if (videoRef.current.paused) {
          videoRef.current.play().catch(err => {
            console.warn('⚠️ Video play error:', err);
          });
        }
      }
    }
  }, [stream, videoEnabled]);

 // ==================== TOGGLE AUDIO ====================
const toggleAudio = useCallback(() => {
  console.log('🎤 Toggle audio clicked');
  
  if (!audioStream) {
    console.warn('⚠️ No audio stream available');
    setDeviceError('No microphone stream available. Please refresh.');
    return;
  }

  const audioTrack = audioStream.getAudioTracks()[0];
  
  if (!audioTrack) {
    console.warn('⚠️ No audio track found');
    setDeviceError('No audio track available.');
    return;
  }

  setAudioEnabled(prevState => {
    const newState = !prevState;
    audioTrack.enabled = newState;
    
    console.log('🎤 Audio toggled:', {
      previousState: prevState,
      newState,
      trackEnabled: audioTrack.enabled
    });
    
    return newState;
  });
}, [audioStream]);

// ==================== REFRESH DEVICES ====================
const refreshDevices = async () => {
  console.log('🔄 Refreshing devices...');
  
  setIsInitializing(true);
  setDeviceError('');
  setFaceVerified(false);
  setAutoVerifyAttempted(false);
  
  // Release camera via hook
  releaseCamera();
  
  // Stop audio tracks
  if (audioStream) {
    audioStream.getTracks().forEach(track => {
      track.stop();
      console.log('🛑 Stopped audio track:', track.kind);
    });
    setAudioStream(null);
  }
  
  // Reset states
  setVideoEnabled(true);
  setAudioEnabled(true);
  setDeviceStatus({
    camera: 'checking',
    microphone: 'checking',
    permissions: { camera: 'unknown', microphone: 'unknown' },
    quality: 'unknown',
    bandwidth: 'unknown'
  });
  
  // Force re-mount by navigating away and back
  setTimeout(() => {
    window.location.reload();
  }, 500);
};
  // ==================== FACE AUTH FUNCTIONS - FIXED ====================

  const captureVideoFrame = useCallback(() => {
    if (!videoRef.current || !stream) {
      throw new Error('No video stream available');
    }

    const video = videoRef.current;
    
    // Check if video has loaded
    if (video.readyState < 2) {
      throw new Error('Video not ready for capture');
    }

    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth || 640;
    canvas.height = video.videoHeight || 480;
    
    const ctx = canvas.getContext('2d');
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
    
    return new Promise((resolve, reject) => {
      canvas.toBlob((blob) => {
        if (blob) {
          console.log('📸 Frame captured:', blob.size, 'bytes');
          resolve(blob);
        } else {
          reject(new Error('Failed to capture frame'));
        }
      }, 'image/jpeg', 0.95);
    });
  }, [stream]);

// Key fix in verifyFace function around line 810-950

const verifyFace = async () => {
    if (!user?.id) {
      setFaceAuthError('User ID not found. Please login again.');
      return false;
    }

    setIsFaceVerifying(true);
    setFaceAuthError('');
    
    try {
      console.log('🔐 Starting face verification...');
      console.log('👤 User ID:', user.id);
      
      // Step 1: Capture video frame as blob
      const imageBlob = await captureVideoFrame();
      console.log('📸 Image captured:', imageBlob.size, 'bytes');
      
      // Step 2: Convert blob to base64 STRING
      console.log('🔄 Converting to base64...');
      const base64String = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onloadend = () => {
          resolve(reader.result);
        };
        reader.onerror = reject;
        reader.readAsDataURL(imageBlob);
      });
      
      console.log('✅ Base64 string created, length:', base64String.length);
      
      const apiUrl = `${FACE_AUTH_API_URL}/${user.id}/`;
      console.log('📡 API URL:', apiUrl);
      
      const controller = new AbortController();
      const timeoutId = setTimeout(() => {
        console.warn('⏱️ Request timeout after 30 seconds');
        controller.abort();
      }, 30000);
      
      // Step 3: Send as JSON with base64 STRING
      const payload = {
        face_image: base64String,
        threshold: 0.6
      };
      
      console.log('📤 Sending payload with face_image as base64 STRING');
      console.log('📦 Payload size:', JSON.stringify(payload).length, 'characters');
      
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      
      clearTimeout(timeoutId);
      
      console.log('📬 Response Status:', response.status);
      
      const contentType = response.headers.get('content-type');
      let data;
      
      if (contentType && contentType.includes('application/json')) {
        data = await response.json();
      } else {
        const text = await response.text();
        console.error('❌ Non-JSON response:', text.substring(0, 200));
        throw new Error('Server returned non-JSON response');
      }
      
      console.log('📥 Verification response:', data);
      
      // ✅ CRITICAL FIX: Check verification result BEFORE checking response.ok
      // This is because backend returns 200 even for failed verifications
      const isVerified = data.verified === true || data.allowed === true;
      
      console.log('🔍 Verification check:', {
        verified: data.verified,
        allowed: data.allowed,
        success: data.success,
        isVerified: isVerified,
        confidence: data.confidence,
        max_similarity: data.max_similarity,
        threshold: data.threshold,
        distance: data.distance
      });
      
      // Handle HTTP error responses (4xx, 5xx)
      if (!response.ok) {
        console.error('❌ Server error:', response.status);
        
        let errorMsg = data.error || data.Error || 'Face verification failed';
        const errorCode = data.error_code || 'UNKNOWN_ERROR';
        
        console.error('❌ Error Code:', errorCode);
        console.error('❌ Error Message:', errorMsg);
        
        // Map error codes to user-friendly messages
        if (errorCode === 'USER_NOT_REGISTERED') {
          errorMsg = 'Your face is not registered. Please complete registration first.';
        } else if (errorCode === 'NO_FACE_DETECTED') {
          errorMsg = 'No face detected. Ensure good lighting and clear visibility.';
        } else if (errorCode === 'NOT_VERIFIED') {
          errorMsg = `Face verification failed. Confidence: ${data.confidence?.toFixed(1)}%`;
        } else if (errorCode === 'MISSING_IMAGE') {
          errorMsg = 'Failed to capture image. Please try again.';
        } else if (errorCode === 'INVALID_IMAGE_TYPE') {
          errorMsg = 'Invalid image format. Please try again.';
        } else if (errorCode === 'INVALID_IMAGE_DATA') {
          errorMsg = 'Invalid image data. Please try again.';
        } else if (errorCode === 'IMAGE_TOO_LARGE') {
          errorMsg = 'Image too large. Please try again.';
        } else if (errorCode === 'SERVICE_UNAVAILABLE') {
          errorMsg = 'Face verification service temporarily unavailable.';
        } else if (errorCode === 'COMPARISON_FAILED') {
          errorMsg = 'Failed to compare faces. Please try again.';
        } else if (response.status === 500) {
          errorMsg = 'Server error during face verification. Please contact support.';
        }
        
        setFaceAuthError(errorMsg);
        setFaceVerified(false);
        return false;
      }
      
      // ✅ CRITICAL: Now check if face actually matched (for 200 responses)
      if (isVerified) {
        setFaceVerified(true);
        setFaceAuthData(data);
        setFaceAuthError('');
        
        console.log('✅ Face verified successfully!');
        console.log('📊 Verification Details:', {
          confidence: data.confidence,
          distance: data.distance,
          similarity: data.similarity || data.max_similarity,
          match_quality: data.match_quality,
          threshold: data.threshold
        });
        
        return true;
      } else {
        // ❌ Face did NOT match (200 response but verification failed)
        setFaceVerified(false);
        
        const confidence = data.confidence || (data.max_similarity * 100) || 0;
        const threshold = data.threshold || 0.6;
        const similarity = data.max_similarity || data.similarity || 0;
        const distance = data.distance || 0;
        
        let errorMsg = data.error || data.Error || 
          `Face does not match. Similarity: ${(similarity * 100).toFixed(1)}% (Required: ${(threshold * 100).toFixed(0)}%)`;
        
        setFaceAuthError(errorMsg);
        
        console.error('❌ Verification failed - Face mismatch');
        console.error('📊 Failed verification details:', {
          verified: data.verified,
          allowed: data.allowed,
          confidence: confidence.toFixed(2),
          similarity: (similarity * 100).toFixed(2) + '%',
          threshold: (threshold * 100).toFixed(0) + '%',
          distance: distance.toFixed(4),
          status: data.status,
          message: data.message
        });
        
        return false;
      }
      
    } catch (error) {
      console.error('❌ Face verification error:', error);
      console.error('❌ Error stack:', error.stack);
      setFaceVerified(false);
      
      let errorMsg = 'Failed to verify face. ';
      
      if (error.name === 'AbortError') {
        errorMsg += 'Request timed out. Please try again.';
      } else if (error.message?.includes('fetch') || error.message?.includes('network')) {
        errorMsg += 'Cannot connect to server. Check your connection.';
      } else if (error.message?.includes('not ready')) {
        errorMsg += 'Camera not ready. Please wait a moment.';
      } else if (error.message?.includes('non-JSON')) {
        errorMsg += 'Server returned invalid response. Please contact support.';
      } else {
        errorMsg += error.message;
      }
      
      setFaceAuthError(errorMsg);
      return false;
    } finally {
      setIsFaceVerifying(false);
    }
  };

  // Auto-verify when camera is ready
  useEffect(() => {
    if (stream && videoEnabled && !autoVerifyAttempted && !faceVerified && mediaPermissionGranted && videoRef.current?.readyState >= 2) {
      const timer = setTimeout(() => {
        setAutoVerifyAttempted(true);
        console.log('🤖 Auto-verifying face...');
        verifyFace().catch(err => {
          console.warn('⚠️ Auto-verification failed:', err);
        });
      }, 2500);
      
      return () => clearTimeout(timer);
    }
  }, [stream, videoEnabled, autoVerifyAttempted, faceVerified, mediaPermissionGranted]);

  const handleManualFaceAuth = async () => {
    if (!stream || !videoEnabled) {
      setFaceAuthError('Please turn on your camera first');
      return;
    }
    
    if (videoRef.current?.readyState < 2) {
      setFaceAuthError('Camera is still loading. Please wait a moment.');
      return;
    }
    
    await verifyFace();
  };

  // ==================== JOIN MEETING ====================

  const handleJoinMeeting = async () => {
    if (!displayName.trim()) {
      setDeviceError('Please enter your name');
      return;
    }

    if (!faceVerified) {
      setDeviceError('Face verification required to join');
      setShowFaceAuthDialog(true);
      return;
    }

    setIsJoining(true);
    setDeviceError('');
    
    try {
      const participantData = {
        name: displayName.trim(),
        full_name: displayName.trim(),
        videoEnabled: videoEnabled && !!stream,
        audioEnabled: audioEnabled && !!stream,
        audioOutputEnabled,
        isHost,
        stream,
        user_id: user?.id || 1,
        email: user?.email || '',
        role: isHost ? 'host' : 'participant',
        Meeting_ID: realMeetingId || meetingId,
        timestamp: new Date().toISOString(),
        deviceStatus,
        mediaSettings: {
          video: videoEnabled && !!stream,
          audio: audioEnabled && !!stream,
          audioOutput: audioOutputEnabled,
          deviceReady: mediaPermissionGranted && !!stream
        },
        faceAuthentication: {
          verified: faceVerified,
          confidence: faceAuthData?.confidence || null,
          distance: faceAuthData?.distance || null,
          threshold: faceAuthData?.threshold || null,
          similarity: faceAuthData?.similarity || null,
          match_quality: faceAuthData?.match_quality || null,
          timestamp: new Date().toISOString(),
          verificationId: user?.id,
          status: 'VERIFIED'
        }
      };
      
      console.log('🚀 Joining with data:', participantData);
      await onJoin(participantData);
      
    } catch (error) {
      console.error('❌ Failed to join:', error);
      setDeviceError('Failed to join meeting. Please try again.');
      setIsJoining(false);
    }
  };

  const copyMeetingLink = () => {
    const linkMeetingId = meetingId === 'instant' 
      ? (realMeetingId || `instant-${Date.now()}`)
      : meetingId;
    
    const meetingLink = `${window.location.origin}/meeting/${linkMeetingId}`;
    
    navigator.clipboard.writeText(meetingLink).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 3000);
    });
  };

  // ==================== EFFECTS ====================
// ==================== SYNC CAMERA STREAM FROM HOOK ====================
useEffect(() => {
  if (cameraStream) {
    console.log('✅ Camera stream from hook:', cameraStream);
    setStream(cameraStream);
    setMediaPermissionGranted(true);
    setIsInitializing(false);
    setDeviceError('');
    
    setDeviceStatus(prev => ({
      ...prev,
      camera: 'available',
      quality: 'good',
      permissions: { ...prev.permissions, camera: 'granted' }
    }));
  } else if (cameraError) {
    console.error('❌ Camera error from hook:', cameraError);
    setDeviceError(cameraError);
    setIsInitializing(false);
    setDeviceStatus(prev => ({
      ...prev,
      camera: 'unavailable',
      permissions: { ...prev.permissions, camera: 'denied' }
    }));
  } else if (cameraLoading) {
    console.log('⏳ Camera loading...');
    setIsInitializing(true);
  }
}, [cameraStream, cameraError, cameraLoading]);

// ==================== REQUEST AUDIO SEPARATELY ====================
useEffect(() => {
  let mounted = true;

  const requestAudio = async () => {
    try {
      console.log('🎤 Requesting audio access...');
      const audioMediaStream = await navigator.mediaDevices.getUserMedia({ 
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
          sampleRate: { ideal: 48000 }
        },
        video: false 
      });
      
      if (mounted) {
        setAudioStream(audioMediaStream);
        console.log('✅ Audio access granted');
        
        setDeviceStatus(prev => ({
          ...prev,
          microphone: 'available',
          permissions: { ...prev.permissions, microphone: 'granted' }
        }));
      }
    } catch (err) {
      console.error('❌ Audio access error:', err);
      if (mounted) {
        setDeviceStatus(prev => ({
          ...prev,
          microphone: 'unavailable',
          permissions: { ...prev.permissions, microphone: 'denied' }
        }));
      }
    }
  };

  requestAudio();

  return () => {
    mounted = false;
    console.log('🧹 Cleaning up audio stream');
    if (audioStream) {
      audioStream.getTracks().forEach(track => track.stop());
    }
  };
}, []); // Run only once on mount

// ==================== CLEANUP ON UNMOUNT ====================
useEffect(() => {
  return () => {
    console.log('🧹 WaitingRoom unmounting - releasing camera via hook');
    releaseCamera();
    
    if (audioStream) {
      audioStream.getTracks().forEach(track => track.stop());
    }
  };
}, [releaseCamera, audioStream]);

// ==================== SET VIDEO SOURCE ====================
useEffect(() => {
  if (stream && videoRef.current) {
    console.log('📺 Setting video srcObject');
    videoRef.current.srcObject = stream;
    
    const handleLoadedData = () => {
      console.log('✅ Video loaded and ready');
      setIsInitializing(false);
    };
    
    videoRef.current.addEventListener('loadeddata', handleLoadedData);
    
    return () => {
      if (videoRef.current) {
        videoRef.current.removeEventListener('loadeddata', handleLoadedData);
      }
    };
  }
}, [stream]);
  // ==================== MENU HANDLERS ====================

  const handleMenuOpen = (event) => {
    setMenuAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuAnchorEl(null);
  };

  const handleMenuItemClick = (action) => {
    handleMenuClose();
    
    switch(action) {
      case 'settings':
        setShowTroubleshooting(true);
        break;
      case 'refresh':
        refreshDevices();
        break;
      case 'verify':
        handleManualFaceAuth();
        break;
      default:
        break;
    }
  };

  // ==================== RENDER ====================

  return (
    <MainContainer>
      <ContentWrapper>
        {/* LEFT SECTION - Video and Device Status */}
        <LeftSection>
          {/* Video Container */}
          <VideoContainer>
            {faceVerified && (
              <FaceVerifiedBadge>
                <CheckCircle sx={{ fontSize: 18 }} />
                Face Verified
              </FaceVerifiedBadge>
            )}

            <MoreOptionsButton onClick={handleMenuOpen}>
              <MoreVert />
            </MoreOptionsButton>

            <Menu
              anchorEl={menuAnchorEl}
              open={Boolean(menuAnchorEl)}
              onClose={handleMenuClose}
              anchorOrigin={{
                vertical: 'bottom',
                horizontal: 'right',
              }}
              transformOrigin={{
                vertical: 'top',
                horizontal: 'right',
              }}
              PaperProps={{
                sx: {
                  borderRadius: 2,
                  minWidth: 200,
                  boxShadow: '0 4px 12px rgba(0,0,0,0.15)'
                }
              }}
            >
              <MenuItem onClick={() => handleMenuItemClick('settings')}>
                <Settings sx={{ mr: 1.5, fontSize: 20 }} />
                Settings
              </MenuItem>
              <MenuItem onClick={() => handleMenuItemClick('refresh')}>
                <Refresh sx={{ mr: 1.5, fontSize: 20 }} />
                Refresh Devices
              </MenuItem>
            </Menu>

{videoEnabled && (stream || cameraStream) && !isInitializing && !cameraLoading ? (              <VideoElement
                ref={videoRef}
                autoPlay
                muted
                playsInline
              />
            ) : (
              <VideoPlaceholder>
{(isInitializing || cameraLoading) ? (                  <>
                   <Box sx={{ textAlign: 'center' }}>
  {/* Custom Loader */}
  <Box
    sx={{
      width: '60px',
      height: '60px',
      border: '24px solid',
      borderColor: 'rgba(255, 255, 255, 0.15) rgba(255, 255, 255, 0.25) rgba(255, 255, 255, 0.35) rgba(255, 255, 255, 0.5)',
      borderRadius: '50%',
      display: 'inline-block',
      boxSizing: 'border-box',
      animation: 'animloader 1s linear infinite',
      '@keyframes animloader': {
        '0%': {
          borderColor: 'rgba(255, 255, 255, 0.15) rgba(255, 255, 255, 0.25) rgba(255, 255, 255, 0.35) rgba(255, 255, 255, 0.75)'
        },
        '33%': {
          borderColor: 'rgba(255, 255, 255, 0.75) rgba(255, 255, 255, 0.15) rgba(255, 255, 255, 0.25) rgba(255, 255, 255, 0.35)'
        },
        '66%': {
          borderColor: 'rgba(255, 255, 255, 0.35) rgba(255, 255, 255, 0.75) rgba(255, 255, 255, 0.15) rgba(255, 255, 255, 0.25)'
        },
        '100%': {
          borderColor: 'rgba(255, 255, 255, 0.25) rgba(255, 255, 255, 0.35) rgba(255, 255, 255, 0.75) rgba(255, 255, 255, 0.15)'
        }
      }
    }}
  />
  
  <Typography 
    variant="h6" 
    sx={{ 
      mt: 2,
      fontWeight: 500,
      color: '#fff' // Added for better visibility
    }}
  >
    Initializing Camera...
  </Typography>
</Box>
                    <Typography variant="caption" sx={{ opacity: 0.7 }}>
                      Please allow camera and microphone access
                    </Typography>
                  </>
                ) : (
                  <>
                    <ResponsiveAvatar>
                      {displayName?.charAt(0)?.toUpperCase() || 
                       user?.full_name?.charAt(0)?.toUpperCase() || 'U'}
                    </ResponsiveAvatar>
                    
                    <Typography variant={isMobile ? "body1" : "h6"} sx={{ fontWeight: 500 }}>
                      {displayName || user?.full_name || 'Your Video'}
                    </Typography>
                    
                    <Typography variant="caption" sx={{ opacity: 0.7 }}>
                      Camera is {videoEnabled ? 'off' : 'disabled'}
                    </Typography>
                  </>
                )}
              </VideoPlaceholder>
            )}

            <ControlsOverlay>
              <Tooltip title={audioEnabled ? "Mute microphone" : "Unmute microphone"}>
                <ControlButton
                  onClick={toggleAudio}
                  disabled={!stream || isInitializing}
                  className={!audioEnabled ? 'danger' : 'active'}
                >
                  {audioEnabled ? <Mic /> : <MicOff />}
                </ControlButton>
              </Tooltip>
              
              <Tooltip title={videoEnabled ? "Turn off camera" : "Turn on camera"}>
                <ControlButton
                  onClick={toggleVideo}
                  disabled={!stream || isInitializing}
                  className={!videoEnabled ? 'danger' : 'active'}
                >
                  {videoEnabled ? <Videocam /> : <VideocamOff />}
                </ControlButton>
              </Tooltip>
            </ControlsOverlay>
          </VideoContainer>


          {/* Face Verification Card */}
          <StatusCard>
            <CardContent>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, color: '#202124', mb: 1.5 }}>
                Face Verification
              </Typography>
              
              {faceVerified ? (
                <SuccessBox>
                  <CheckCircle sx={{ fontSize: 20 }} />
                  <Box sx={{ flex: 1 }}>
                    <Typography variant="body2" sx={{ fontWeight: 500 }}>
                      Identity Verified
                    </Typography>
                    {faceAuthData && (
                      <Typography variant="caption" sx={{ display: 'block', mt: 0.5, opacity: 0.8 }}>
                        Confidence: {faceAuthData.confidence?.toFixed(1)}% • {faceAuthData.match_quality}
                      </Typography>
                    )}
                  </Box>
                </SuccessBox>
              ) : (
                <Alert severity={isFaceVerifying ? "info" : "warning"} sx={{ borderRadius: 1 }}>
                  <Typography variant="body2">
                    {isFaceVerifying ? 'Verifying your face...' : 'Face verification required'}
                  </Typography>
                </Alert>
              )}

              {faceAuthError && (
                <Alert severity="error" sx={{ mt: 1.5, borderRadius: 1 }}>
                  <Typography variant="caption">{faceAuthError}</Typography>
                </Alert>
              )}

            <VerifyButton
  fullWidth
  variant="outlined"
  onClick={handleManualFaceAuth}
  disabled={!stream || !videoEnabled || isFaceVerifying || isInitializing}
  startIcon={
    isFaceVerifying ? (
      <Box
        sx={{
          display: 'inline-block',
          transform: 'translateZ(1px)',
          '&::after': {
            content: '""',
            display: 'inline-block',
            width: '16px',
            height: '16px',
            margin: '0',
            borderRadius: '50%',
            background: '#000',
            animation: 'coin-flip 2.4s cubic-bezier(0, 0.2, 0.8, 1) infinite',
          },
          '@keyframes coin-flip': {
            '0%, 100%': {
              animationTimingFunction: 'cubic-bezier(0.5, 0, 1, 0.5)',
            },
            '0%': {
              transform: 'rotateY(0deg)',
            },
            '50%': {
              transform: 'rotateY(1800deg)',
              animationTimingFunction: 'cubic-bezier(0, 0.5, 0.5, 1)',
            },
            '100%': {
              transform: 'rotateY(3600deg)',
            },
          },
        }}
      />
    ) : (
      <Refresh />
    )
  }
  sx={{ mt: 1.5 }}
>
  {isFaceVerifying ? 'Verifying...' : 'Verify'}
</VerifyButton>
            </CardContent>
          </StatusCard>
        </LeftSection>

        {/* RIGHT SECTION - Meeting Info and Join */}
        <RightSection>
          {/* Meeting Info Card */}
          <InfoCard>
            <CardContent>
              <Typography variant={isMobile ? "h6" : "h5"} sx={{ fontWeight: 600, color: '#202124', mb: 1 }}>
                {meeting.title}
              </Typography>
              
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <Avatar sx={{ width: 32, height: 32, bgcolor: '#1A73E8', fontSize: '0.875rem' }}>
                  {meeting.host_name?.charAt(0)?.toUpperCase() || 'MH'}
                </Avatar>
                <Box>
                  <Typography variant="caption" sx={{ color: '#5F6368', display: 'block', lineHeight: 1.2 }}>
                    Hosted by
                  </Typography>
                  <Typography variant="body2" sx={{ color: '#202124', fontWeight: 500, lineHeight: 1.2 }}>
                    {meeting.host_name}
                  </Typography>
                  <Typography variant="caption" sx={{ color: '#5F6368', display: 'block', lineHeight: 1.2 }}>
                    {user?.email || 'host@example.com'}
                  </Typography>
                </Box>
              </Box>

              {isHost && (
                <>
                  <CopyLinkButton
                    fullWidth
                    variant="outlined"
                    startIcon={copied ? <CheckCircle /> : <ContentCopy />}
                    onClick={copyMeetingLink}
                  >
                    {copied ? 'Link Copied' : 'Copy Meeting Link'}
                  </CopyLinkButton>
                  
                  <Typography variant="caption" sx={{ color: '#5F6368', display: 'block', textAlign: 'center', mt: 1 }}>
                    Meeting ID: {realMeetingId || meetingId || '3497430'}
                  </Typography>
                </>
              )}
            </CardContent>
          </InfoCard>

          {/* Name Input */}
          <Box>
            <Typography variant="subtitle2" sx={{ fontWeight: 500, color: '#202124', mb: 1 }}>
              Your Name
            </Typography>
            
            <NameInput
              fullWidth
              placeholder="Enter your name"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              disabled={isJoining}
              error={!displayName.trim() && deviceError.includes('name')}
              size={isMobile ? "small" : "medium"}
            />
          </Box>


            {/* Device Status Card */}
          <StatusCard>
            <CardContent>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, color: '#202124', mb: 1.5 }}>
                Device Status
              </Typography>
              
              <Stack spacing={1}>
                <StatusItem>
                  <CheckCircle sx={{ 
                    color: deviceStatus.camera === 'available' ? '#34A853' : '#EA4335', 
                    fontSize: 20 
                  }} />
                  <Typography variant="body2">
                    Camera: {deviceStatus.camera === 'available' ? 'Available' : 'Checking...'}
                  </Typography>
                </StatusItem>
                
                <StatusItem>
                  <CheckCircle sx={{ 
                    color: deviceStatus.microphone === 'available' ? '#34A853' : '#EA4335', 
                    fontSize: 20 
                  }} />
                  <Typography variant="body2">
                    Microphone: {deviceStatus.microphone === 'available' ? 'Available' : 'Checking...'}
                  </Typography>
                </StatusItem>
              </Stack>
            </CardContent>
          </StatusCard>

          {/* Error Display */}
          {deviceError && !deviceError.includes('Face verification') && (
            <Alert severity="error" sx={{ borderRadius: 2 }}>
              <Typography variant="body2">{deviceError}</Typography>
            </Alert>
          )}

          {/* Join Button */}
          <JoinButton
            fullWidth
            onClick={handleJoinMeeting}
            disabled={isJoining || isConnecting || !displayName.trim() || isInitializing || !faceVerified}
            startIcon={
              (isJoining || isInitializing) && <CircularProgress size={20} color="inherit" />
            }
          >
            {isInitializing
              ? 'Setting up...'
              : isJoining
                ? 'Joining...' 
                : !faceVerified
                  ? 'Verify Face to Join'
                  : 'Join Meeting Now'
            }
          </JoinButton>

          <Typography variant="caption" sx={{ color: '#5F6368', textAlign: 'center', display: 'block' }}>
            By joining, you agree to the meeting terms and privacy policy
          </Typography>
        </RightSection>
      </ContentWrapper>

      {/* Troubleshooting Dialog */}
      <Dialog
        open={showTroubleshooting}
        onClose={() => setShowTroubleshooting(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: 3 } }}
      >
        <DialogTitle>
          <Typography variant="h6" fontWeight="600">Troubleshooting</Typography>
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mb: 2, color: '#5F6368' }}>
            Having issues with your camera or microphone?
          </Typography>

          <List>
            <ListItem>
              <ListItemAvatar>
                <Avatar sx={{ bgcolor: '#E8F0FE', color: '#1A73E8' }}>
                  <Security />
                </Avatar>
              </ListItemAvatar>
              <ListItemText
                primary="Check browser permissions"
                secondary="Allow camera and microphone access"
                primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
                secondaryTypographyProps={{ variant: 'caption' }}
              />
            </ListItem>
            
            <ListItem>
              <ListItemAvatar>
                <Avatar sx={{ bgcolor: '#E8F0FE', color: '#1A73E8' }}>
                  <CameraAlt />
                </Avatar>
              </ListItemAvatar>
              <ListItemText
                primary="Close other applications"
                secondary="Exit Zoom, Teams, or other video apps"
                primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
                secondaryTypographyProps={{ variant: 'caption' }}
              />
            </ListItem>
            
            <ListItem>
              <ListItemAvatar>
                <Avatar sx={{ bgcolor: '#E8F0FE', color: '#1A73E8' }}>
                  <Refresh />
                </Avatar>
              </ListItemAvatar>
              <ListItemText
                primary="Refresh devices"
                secondary="Click refresh to reconnect camera and microphone"
                primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
                secondaryTypographyProps={{ variant: 'caption' }}
              />
            </ListItem>
          </List>

          <Box sx={{ mt: 2, p: 2, bgcolor: '#F8F9FA', borderRadius: 2 }}>
            <Typography variant="caption" fontWeight="600" display="block" sx={{ mb: 0.5 }}>
              Current Status
            </Typography>
            <Typography variant="caption" display="block" color="#5F6368">
              Camera: {deviceStatus.camera}
            </Typography>
            <Typography variant="caption" display="block" color="#5F6368">
              Microphone: {deviceStatus.microphone}
            </Typography>
            <Typography variant="caption" display="block" color="#5F6368">
              Video: {videoEnabled ? 'On' : 'Off'}
            </Typography>
            <Typography variant="caption" display="block" color="#5F6368">
              Audio: {audioEnabled ? 'On' : 'Off'}
            </Typography>
            <Typography variant="caption" display="block" color="#5F6368">
              Face Auth: {faceVerified ? '✅ Verified' : '❌ Not Verified'}
            </Typography>
          </Box>
        </DialogContent>
        <DialogActions sx={{ p: 2 }}>
          <Button onClick={() => setShowTroubleshooting(false)}>Close</Button>
          <Button 
            variant="contained" 
            onClick={() => { 
              setShowTroubleshooting(false); 
              refreshDevices(); 
            }}
            disabled={isInitializing}
          >
            Refresh Devices
          </Button>
        </DialogActions>
      </Dialog>

      {/* Face Auth Dialog */}
      <Dialog
        open={showFaceAuthDialog}
        onClose={() => setShowFaceAuthDialog(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { borderRadius: 3 } }}
      >
        <DialogTitle>
          <Typography variant="h6" fontWeight="600">Face Verification Required</Typography>
        </DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 2, borderRadius: 2 }}>
            Face verification ensures secure access and accurate attendance tracking.
          </Alert>

          <Typography variant="subtitle2" fontWeight="600" sx={{ mb: 1 }}>
            Tips for successful verification:
          </Typography>

          <List dense>
            <ListItem>
              <ListItemAvatar>
                <Avatar sx={{ width: 32, height: 32, bgcolor: '#E8F0FE', color: '#1A73E8' }}>
                  <Brightness6 fontSize="small" />
                </Avatar>
              </ListItemAvatar>
              <ListItemText
                primary="Good lighting"
                secondary="Ensure your face is well-lit"
                primaryTypographyProps={{ variant: 'body2' }}
                secondaryTypographyProps={{ variant: 'caption' }}
              />
            </ListItem>
            
            <ListItem>
              <ListItemAvatar>
                <Avatar sx={{ width: 32, height: 32, bgcolor: '#E8F0FE', color: '#1A73E8' }}>
                  <CameraAlt fontSize="small" />
                </Avatar>
              </ListItemAvatar>
              <ListItemText
                primary="Look at camera"
                secondary="Face the camera directly"
                primaryTypographyProps={{ variant: 'body2' }}
                secondaryTypographyProps={{ variant: 'caption' }}
              />
            </ListItem>
            
            <ListItem>
              <ListItemAvatar>
                <Avatar sx={{ width: 32, height: 32, bgcolor: '#E8F0FE', color: '#1A73E8' }}>
                  <PersonOutline fontSize="small" />
                </Avatar>
              </ListItemAvatar>
              <ListItemText
                primary="Remove obstructions"
                secondary="No sunglasses or masks"
                primaryTypographyProps={{ variant: 'body2' }}
                secondaryTypographyProps={{ variant: 'caption' }}
              />
            </ListItem>
          </List>

          {faceAuthError && (
            <Alert severity="error" sx={{ mt: 2, borderRadius: 2 }}>
              <Typography variant="caption">{faceAuthError}</Typography>
            </Alert>
          )}

          {faceVerified && (
            <Alert severity="success" sx={{ mt: 2, borderRadius: 2 }}>
              Face verified successfully! You can now join.
            </Alert>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2 }}>
          <Button onClick={() => setShowFaceAuthDialog(false)}>
            {faceVerified ? 'Close' : 'Cancel'}
          </Button>
          {!faceVerified && (
            <Button 
              variant="contained"
              onClick={handleManualFaceAuth}
              disabled={!stream || !videoEnabled || isFaceVerifying || isInitializing}
              startIcon={isFaceVerifying ? <CircularProgress size={16} /> : <PhotoCamera />}
            >
              {isFaceVerifying ? 'Verifying...' : 'Verify Face Now'}
            </Button>
          )}
        </DialogActions>
      </Dialog>
    </MainContainer>
  );
};

export default WaitingRoom;