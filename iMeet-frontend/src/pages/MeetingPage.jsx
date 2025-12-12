// src/pages/MeetingPage.jsx - Enhanced Professional UI Version with Feedback Support
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Box,
  useTheme,
  alpha,
  Fade,
  Typography,
  Card,
  CardContent,
  CircularProgress,
  Button,
  Alert,
  Backdrop
} from '@mui/material';
import { styled } from '@mui/material/styles';
import { useParams, useNavigate } from 'react-router-dom';
import {
  VideoCall,
  ErrorOutline,
  Refresh,
  ArrowBack,
  ScreenShare,
  CheckCircle
} from '@mui/icons-material';

import WaitingRoom from '../components/meeting/WaitingRoom';
import MeetingRoom from '../components/meeting/MeetingRoom';
import LoadingSpinner from '../components/common/LoadingSpinner';
import { useMeeting } from '../hooks/useMeeting';
import { useAuth } from '../hooks/useAuth';
import useWebRTC from '../hooks/useWebRTC';
import { useLiveKit } from '../hooks/useLiveKit';
import { API_BASE_URL } from '../utils/constants';

// Professional Styled Components matching the UI design
const MeetingPageContainer = styled(Box)(({ theme }) => ({
  height: "100vh",
  width: "100vw",
  display: "flex",
  flexDirection: "column",
  background: "linear-gradient(135deg, #0a0f1c 0%, #1a1f2e 50%, #0f172a 100%)",
  color: "white",
  overflow: "hidden",
  position: "relative",
  fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
}));

const LoadingContainer = styled(Box)(({ theme }) => ({
  height: '100vh',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  background: "#1976d2",
  color: 'white',
  textAlign: 'center',
  padding: theme.spacing(4),
  position: 'relative',

  "&::before": {
    content: '""',
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: "radial-gradient(circle at 50% 50%, rgba(59, 130, 246, 0.1) 0%, transparent 50%)",
    pointerEvents: "none",
  }
}));

const ErrorContainer = styled(Box)(({ theme }) => ({
  height: '100vh',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  background: "linear-gradient(135deg, #5689ffff 0%, #79a4eaff 50%, #5a86edff 100%)",
  color: 'white',
  textAlign: 'center',
  padding: theme.spacing(4),
  position: 'relative',

  "&::before": {
    content: '""',
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: "radial-gradient(circle at 50% 50%, rgba(239, 68, 68, 0.1) 0%, transparent 50%)",
    pointerEvents: "none",
  }
}));



const ErrorCard = styled(Card)(({ theme }) => ({
  background: "rgba(15, 23, 42, 0.9)",
  backdropFilter: "blur(30px)",
  WebkitBackdropFilter: "blur(30px)",
  borderRadius: "24px",
  border: "1px solid rgba(239, 68, 68, 0.3)",
  boxShadow: "0 25px 50px rgba(239, 68, 68, 0.2)",
  color: "white",
  minWidth: 450,
  maxWidth: 600,
  position: 'relative',
  overflow: 'hidden',

  "&::before": {
    content: '""',
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: "linear-gradient(145deg, rgba(239, 68, 68, 0.05) 0%, rgba(239, 68, 68, 0.02) 100%)",
    pointerEvents: "none",
  }
}));

const StatusIndicator = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: theme.spacing(2),
  marginTop: theme.spacing(3),
  padding: theme.spacing(2),
  background: "rgba(255, 255, 255, 0.05)",
  borderRadius: "16px",
  border: "1px solid rgba(255, 255, 255, 0.1)",
  fontSize: "0.9rem",
  fontWeight: 500,
}));

const ActionButton = styled(Button)(({ theme, variant: buttonVariant }) => ({
  minWidth: 120,
  height: 48,
  borderRadius: "12px",
  fontWeight: 600,
  fontSize: "0.9rem",
  textTransform: "none",
  marginTop: theme.spacing(3),
  background: buttonVariant === 'primary'
    ? "linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)"
    : "rgba(255, 255, 255, 0.1)",
  border: buttonVariant === 'primary'
    ? "1px solid rgba(59, 130, 246, 0.3)"
    : "1px solid rgba(255, 255, 255, 0.2)",
  color: "white",
  backdropFilter: "blur(10px)",
  WebkitBackdropFilter: "blur(10px)",
  boxShadow: buttonVariant === 'primary'
    ? "0 8px 32px rgba(59, 130, 246, 0.3)"
    : "0 4px 16px rgba(0, 0, 0, 0.1)",
  transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",

  "&:hover": {
    background: buttonVariant === 'primary'
      ? "linear-gradient(135deg, #60a5fa 0%, #3b82f6 100%)"
      : "rgba(255, 255, 255, 0.15)",
    transform: "translateY(-2px)",
    boxShadow: buttonVariant === 'primary'
      ? "0 12px 40px rgba(59, 130, 246, 0.4)"
      : "0 8px 24px rgba(0, 0, 0, 0.15)",
  },

  "&:disabled": {
    opacity: 0.5,
    cursor: "not-allowed",
    transform: "none",
    background: "rgba(255, 255, 255, 0.05)",
  },
}));

const AnimatedLoader = styled(Box)(({ theme }) => ({
  width: '84px',
  height: '84px',
  position: 'relative',
  overflow: 'hidden',
  
  '&::before, &::after': {
    content: '""',
    position: 'absolute',
    left: '50%',
    bottom: 0,
    width: '64px',
    height: '64px',
    borderRadius: '50%',
    background: '#FFF',
    transform: 'translate(-50%, 100%) scale(0)',
    animation: 'push 2s infinite ease-in',
  },
  
  '&::after': {
    animationDelay: '1s',
  },
  
  '@keyframes push': {
    '0%': {
      transform: 'translate(-50%, 100%) scale(1)',
    },
    '15%, 25%': {
      transform: 'translate(-50%, 50%) scale(1)',
    },
    '50%, 75%': {
      transform: 'translate(-50%, -30%) scale(0.5)',
    },
    '80%, 100%': {
      transform: 'translate(-50%, -50%) scale(0)',
    },
  },
}));

const NotificationOverlay = styled(Box)(({ theme }) => ({
  position: 'fixed',
  top: 20,
  left: '50%',
  transform: 'translateX(-50%)',
  zIndex: 9999,
  minWidth: 300,
  maxWidth: '80%',
}));

const NotificationCard = styled(Card, {
  shouldForwardProp: (prop) => !['severity'].includes(prop)
})(({ theme, severity }) => ({
  background: severity === 'error'
    ? "rgba(239, 68, 68, 0.9)"
    : severity === 'success'
      ? "rgba(34, 197, 94, 0.9)"
      : "rgba(59, 130, 246, 0.9)",
  backdropFilter: "blur(20px)",
  WebkitBackdropFilter: "blur(20px)",
  borderRadius: "16px",
  border: `1px solid ${severity === 'error'
      ? "rgba(239, 68, 68, 0.3)"
      : severity === 'success'
        ? "rgba(34, 197, 94, 0.3)"
        : "rgba(59, 130, 246, 0.3)"
    }`,
  boxShadow: `0 12px 40px ${severity === 'error'
      ? "rgba(239, 68, 68, 0.3)"
      : severity === 'success'
        ? "rgba(34, 197, 94, 0.3)"
        : "rgba(59, 130, 246, 0.3)"
    }`,
  color: "white",
  animation: "slideInDown 0.5s ease",

  "@keyframes slideInDown": {
    "0%": {
      transform: "translateY(-100px)",
      opacity: 0
    },
    "100%": {
      transform: "translateY(0)",
      opacity: 1
    }
  }
}));

const ProcessingOverlay = styled(Backdrop)(({ theme }) => ({
  backgroundColor: 'rgba(15, 23, 42, 0.8)',
  backdropFilter: 'blur(10px)',
  WebkitBackdropFilter: 'blur(10px)',
  zIndex: 9999,
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(2),
}));

const ProcessingCard = styled(Card)(({ theme }) => ({
  background: "rgba(15, 23, 42, 0.95)",
  backdropFilter: "blur(30px)",
  WebkitBackdropFilter: "blur(30px)",
  borderRadius: "20px",
  border: "1px solid rgba(255, 255, 255, 0.1)",
  boxShadow: "0 20px 40px rgba(0, 0, 0, 0.3)",
  color: "white",
  padding: theme.spacing(4),
  textAlign: 'center',
  minWidth: 300,
}));

const MeetingPage = () => {
  const theme = useTheme();
  const { meetingId } = useParams();
  const navigate = useNavigate();
  const { user } = useAuth();
  const {
    meeting,
    joinMeeting,
    leaveMeeting,
    loading: meetingLoading,
    error: meetingError
  } = useMeeting();

  const [showWaitingRoom, setShowWaitingRoom] = useState(true);
  const [isHost, setIsHost] = useState(false);
  const [webrtcInitialized, setWebrtcInitialized] = useState(false);
  const [joinError, setJoinError] = useState(null);
  const [isJoining, setIsJoining] = useState(false);
  const [hasTriedJoin, setHasTriedJoin] = useState(false);
  const [actualMeeting, setActualMeeting] = useState(null);
  const [realMeetingIdState, setRealMeetingIdState] = useState(null);
  const [isCreatingMeeting, setIsCreatingMeeting] = useState(false);
  const [screenShareError, setScreenShareError] = useState(null);
  const [isProcessingScreenShare, setIsProcessingScreenShare] = useState(false);
  const [meetingEnded, setMeetingEnded] = useState(false);

  const actualMeetingId = actualMeeting?.id || realMeetingIdState || meetingId;

  // LiveKit integration
  const {
    connected: livekitConnected,
    connecting: livekitConnecting,
    connectionError: livekitError,
    connectToRoom: livekitConnect,
    disconnectFromRoom: livekitDisconnect,
    localParticipant,
    remoteParticipants,
    participantCount,
    room,
    isAudioEnabled: livekitAudioEnabled,
    isVideoEnabled: livekitVideoEnabled,
    isScreenSharing: livekitScreenSharing,
    toggleAudio: livekitToggleAudio,
    toggleVideo: livekitToggleVideo,
    startScreenShare: livekitStartScreenShare,
    stopScreenShare: livekitStopScreenShare,
    sendReaction: livekitSendReaction
  } = useLiveKit();

  // Use WebRTC hook for compatibility (now backed by LiveKit)
  const {
    localStream,
    remoteStreams,
    participants,
    isConnected,
    isVideoEnabled,
    isAudioEnabled,
    isScreenSharing,
    screenShareStream,
    screenSharer,
    isRecording,
    connectionQuality,
    errors: webrtcErrors,
    meetingStatus,
    connectionState,
    localVideoRef,
    joinMeeting: joinWebRTCMeeting,
    leaveMeeting: leaveWebRTCMeeting,
    toggleVideo,
    toggleAudio,
    startScreenShare,
    stopScreenShare,
    startRecording,
    stopRecording,
    sendDataChannelMessage,
    cleanup,
    realMeetingId,
    participantId,
    registerParticipantInDatabase,
    unregisterParticipantFromDatabase,
    syncParticipantsFromDatabase
  } = useWebRTC(actualMeetingId, user?.id, isHost);

  // Screen share state tracking
  const screenShareStateRef = useRef({
    isSharing: false,
    isProcessing: false
  });

  // Debug user information
  useEffect(() => {
    console.log('🔐 MeetingPage User Debug:', {
      user: user,
      userKeys: user ? Object.keys(user) : 'null',
      name: user?.name,
      full_name: user?.full_name,
      displayName: user?.displayName,
      id: user?.id
    });
  }, [user]);

  // Track screen share state changes
  useEffect(() => {
    screenShareStateRef.current.isSharing = isScreenSharing;
    console.log('📺 Screen share state updated:', {
      isScreenSharing,
      screenShareStream: !!screenShareStream,
      screenSharer,
      isProcessing: screenShareStateRef.current.isProcessing
    });
  }, [isScreenSharing, screenShareStream, screenSharer]);

  // Create instant meeting when meetingId is 'instant'
  useEffect(() => {
    if (meetingId === 'instant' && !actualMeeting && !isCreatingMeeting) {
      createInstantMeeting();
    }
  }, [meetingId, actualMeeting, isCreatingMeeting]);

  // Check if user is host
  useEffect(() => {
    if (meetingId === 'instant') {
      setIsHost(true);
      console.log('🎯 Instant meeting: User set as host');
    } else if (user) {
      // First check if we can get host status from meeting data
      if (meeting && (meeting.host_id === user.id || meeting.Host_ID === user.id)) {
        setIsHost(true);
        console.log('🎯 Host status determined from meeting data:', true);
      } else {
        // Fallback: fetch meeting details to check host status
        const checkHostStatus = async () => {
          try {
            const response = await fetch(`${API_BASE_URL}/api/meetings/get/${meetingId}`);
            const meetingDetails = await response.json();
            const userIsHost = meetingDetails.Host_ID === user.id;
            setIsHost(userIsHost);
            console.log('🎯 Host status determined from API:', {
              userIsHost,
              meetingHostId: meetingDetails.Host_ID,
              userId: user.id
            });
          } catch (error) {
            console.warn('⚠️ Could not determine host status:', error);
            setIsHost(false);
          }
        };

        checkHostStatus();
      }
    }
  }, [meeting, user, meetingId]);

  // Listen for changes in realMeetingId and update state
  useEffect(() => {
    if (realMeetingId) {
      console.log('📋 Real meeting ID updated in MeetingPage:', realMeetingId);
      setRealMeetingIdState(realMeetingId);

      if (typeof window !== 'undefined') {
        window.currentRealMeetingId = realMeetingId;
        console.log('🌍 Stored in window.currentRealMeetingId:', realMeetingId);
      }
    }
  }, [realMeetingId]);

  // Enhanced transition logic using only LiveKit state
  useEffect(() => {
    // Check LiveKit connection status
    const isLiveKitFullyConnected = livekitConnected &&
      room &&
      room.state === 'connected' &&
      localParticipant;

    console.log('📊 Connection Status Update:', {
      // Only track LiveKit states for decision making
      livekitConnected,
      livekitConnecting,
      hasRoom: !!room,
      roomState: room?.state,
      hasLocalParticipant: !!localParticipant,
      isLiveKitFullyConnected,
      hasTriedJoin,
      isJoining,
      showWaitingRoom,
      // Keep these for debugging but don't use for decisions
      webrtcConnected: isConnected,
      webrtcInitialized,
      actualMeetingId,
      realMeetingId: realMeetingIdState || realMeetingId,
      participantId,
      participantCount: participants.length
    });

    // Only transition to MeetingRoom when LiveKit is fully ready
    if (isLiveKitFullyConnected && hasTriedJoin && !isJoining && !livekitConnecting) {
      console.log('✅ LiveKit fully connected - transitioning to MeetingRoom');
      console.log('🎯 LiveKit ready conditions:', {
        livekitConnected: !!livekitConnected,
        hasRoom: !!room,
        roomState: room?.state,
        hasLocalParticipant: !!localParticipant,
        hasTriedJoin: !!hasTriedJoin,
        isJoining: !!isJoining,
        livekitConnecting: !!livekitConnecting
      });

      if (showWaitingRoom) {
        console.log('🚪 Transitioning from WaitingRoom to MeetingRoom...');
        setShowWaitingRoom(false);
        setWebrtcInitialized(true);

        // Small delay to ensure state updates properly
        setTimeout(() => {
          console.log('✅ Successfully transitioned to MeetingRoom');
        }, 100);
      }
    }
  }, [
    // Only depend on LiveKit states
    livekitConnected,
    livekitConnecting,
    room?.state,
    localParticipant,
    hasTriedJoin,
    isJoining,
    showWaitingRoom,
    // Keep these for logging but they don't affect transition
    isConnected,
    actualMeetingId,
    realMeetingIdState,
    realMeetingId,
    participantId,
    participants.length
  ]);

  // Handle meeting status changes
  useEffect(() => {
    if (meetingStatus === 'ended') {
      console.log('🏁 Meeting has ended');
      setTimeout(() => {
        navigate('/dashboard');
      }, 2000);
    }
  }, [meetingStatus, navigate]);

  // Clear errors after 10 seconds
  useEffect(() => {
    if (webrtcErrors.length > 0) {
      const timer = setTimeout(() => {
        const criticalErrors = webrtcErrors.filter(error =>
          error.type === 'meeting_not_found' ||
          error.type === 'meeting_inactive' ||
          error.type === 'invalid_meeting_id'
        );
        if (criticalErrors.length === 0) {
          console.log('🧹 Clearing non-critical WebRTC errors');
        }
      }, 10000);
      return () => clearTimeout(timer);
    }
  }, [webrtcErrors]);

  // Clear screen share error after 5 seconds
  useEffect(() => {
    if (screenShareError) {
      const timer = setTimeout(() => {
        setScreenShareError(null);
      }, 5000);
      return () => clearTimeout(timer);
    }
  }, [screenShareError]);

  // Function to get meeting ID for sharing
  const getMeetingIdForSharing = useCallback(() => {
    console.log('🔍 getMeetingIdForSharing called:', {
      meetingId,
      actualMeetingId: actualMeeting?.id,
      realMeetingIdState,
      realMeetingId
    });

    if (meetingId === 'instant') {
      const availableId = actualMeeting?.id || realMeetingIdState || realMeetingId;
      console.log('📋 For instant meeting, using ID:', availableId);
      return availableId;
    }
    return meetingId;
  }, [meetingId, actualMeeting, realMeetingIdState, realMeetingId]);

  // Get proper participant name from user object
  const getParticipantName = useCallback(() => {
    if (!user) {
      console.warn('⚠️ No user object available for participant name');
      return 'Unknown User';
    }

    const name = user.full_name ||
      user.name ||
      user.displayName ||
      user.username ||
      user.firstName ||
      'Unknown User';

    console.log('👤 Resolved participant name:', name, 'from user:', user);
    return name;
  }, [user]);

  // Enhanced join meeting handler with proper parameter passing
  const handleJoinFromWaitingRoom = useCallback(async (participantData) => {
    console.log('🚀 MeetingPage: Starting LiveKit join process...', participantData);
    setJoinError(null);
    setIsJoining(true);
    setHasTriedJoin(true);

    try {
      // Get the proper participant name
      const properParticipantName = getParticipantName();
      console.log('👤 Using participant name for registration:', properParticipantName);

      // Step 1: Handle instant meeting creation if needed
      if (meetingId === 'instant') {
        console.log('⚡ Instant meeting detected - using actual meeting ID:', actualMeetingId);
        if (!actualMeeting) {
          throw new Error('Instant meeting not created yet');
        }
      } else {
        // Step 2: For regular meetings, try to join through backend API first
        try {
          if (joinMeeting && !meeting) {
            console.log('🔗 Joining meeting via Django backend...');
            const joinResult = await joinMeeting(meetingId, properParticipantName);
            if (!joinResult.success) {
              console.warn('⚠️ Backend join failed:', joinResult.message);
            } else {
              console.log('✅ Backend join successful');
            }
          }
        } catch (backendError) {
          console.warn('⚠️ Backend join failed, continuing with LiveKit only:', backendError.message);
        }
      }

      // Step 3: Connect via LiveKit with FIXED parameters
      console.log('🌐 Establishing LiveKit connection with name:', properParticipantName);

      // Pass parameters correctly to LiveKit
      const meetingIdForConnection = actualMeetingId;
      const userIdForConnection = user?.id?.toString() || 'anonymous';
      const displayNameForConnection = properParticipantName;

      console.log('🔧 LiveKit connection parameters:', {
        meetingId: meetingIdForConnection,
        userId: userIdForConnection,
        displayName: displayNameForConnection,
        isHost: isHost
      });

      // Validate parameters before calling LiveKit
      if (!meetingIdForConnection || !userIdForConnection || !displayNameForConnection) {
        throw new Error(`Missing LiveKit parameters: meetingId=${meetingIdForConnection}, userId=${userIdForConnection}, displayName=${displayNameForConnection}`);
      }

      // Connect to LiveKit room with correct parameter order
      await livekitConnect(
        meetingIdForConnection,
        userIdForConnection,
        displayNameForConnection,
        {
          isHost: isHost,
          enableAudio: participantData.audioEnabled !== false,
          enableVideo: participantData.videoEnabled !== false
        }
      );

      // Step 4: Also connect WebRTC layer for compatibility
      const webrtcSuccess = await joinWebRTCMeeting(properParticipantName);

      if (webrtcSuccess) {
        console.log('✅ Both LiveKit and WebRTC connection completed successfully');

        // Step 5: Verify participant registration
        setTimeout(async () => {
          try {
            console.log('🔍 Verifying participant registration...');
            const participants = await syncParticipantsFromDatabase();
            console.log('✅ Participant verification complete:', participants.length, 'participants found');
          } catch (verificationError) {
            console.warn('⚠️ Participant verification failed:', verificationError);
          }
        }, 2000);

      } else {
        throw new Error('Failed to establish connections');
      }

    } catch (error) {
      console.error('❌ Failed to join meeting:', error);
      setJoinError(error);
      setHasTriedJoin(false);
      throw error;
    } finally {
      setIsJoining(false);
    }
  }, [joinMeeting, meetingId, joinWebRTCMeeting, meeting, actualMeeting, actualMeetingId, getParticipantName, user, isHost, livekitConnect, syncParticipantsFromDatabase]);

  // ✅ UPDATED: Enhanced leave meeting handler with feedback support
  const handleLeaveMeeting = useCallback(async () => {
    console.log("👋 MeetingPage: Leaving meeting check...");
    setMeetingEnded(true);

    // ✅ CRITICAL: Check if meeting ended and feedback is NOT submitted
    const feedbackActive = sessionStorage.getItem('feedbackActive');
    const meetingEndedAt = sessionStorage.getItem('meetingEndedAt');

    if (meetingEndedAt && feedbackActive === 'true') {
      console.log("⛔ BLOCKED: Feedback dialog is active - cannot leave yet");
      return; // STOP - don't navigate until feedback is submitted
    }

    console.log("✅ Proceeding with meeting cleanup...");

    try {
      // Step 1: Disconnect from LiveKit
      await livekitDisconnect();

      // Step 2: Clean up WebRTC layer
      leaveWebRTCMeeting();

      // Step 3: Clean up backend meeting if not instant
      if (meetingId !== 'instant' && leaveMeeting) {
        try {
          await leaveMeeting();
        } catch (error) {
          console.warn('⚠️ Backend leave call failed (continuing):', error.message);
        }
      }

      // Step 4: Reset all states
      setWebrtcInitialized(false);
      setShowWaitingRoom(true);
      setHasTriedJoin(false);
      setRealMeetingIdState(null);
      setActualMeeting(null);

      // Step 5: Clear session storage
      sessionStorage.removeItem('meetingEndedAt');
      sessionStorage.removeItem('feedbackActive');
      sessionStorage.removeItem('blockAutoRefresh');
      sessionStorage.removeItem('currentMeetingId');

      // Step 6: Navigate back to dashboard
      setTimeout(() => {
        navigate('/dashboard');
      }, 100);

    } catch (error) {
      console.error('❌ Failed to leave meeting:', error);
      cleanup();
      setWebrtcInitialized(false);
      setHasTriedJoin(false);
      setRealMeetingIdState(null);
      setActualMeeting(null);
      navigate('/dashboard');
    }
  }, [livekitDisconnect, leaveWebRTCMeeting, leaveMeeting, meetingId, cleanup, navigate]);

  // Media control handlers
  const handleToggleAudio = useCallback(() => {
    const result = toggleAudio();
    console.log('🎤 Audio toggled:', result);
    return result;
  }, [toggleAudio]);

  const handleToggleVideo = useCallback(() => {
    const result = toggleVideo();
    console.log('📹 Video toggled:', result);
    return result;
  }, [toggleVideo]);

  // Enhanced screen share handler with proper LiveKit validation
  const handleToggleScreenShare = useCallback(async () => {
    // Prevent multiple simultaneous toggles
    if (screenShareStateRef.current.isProcessing || isProcessingScreenShare) {
      console.warn('⚠️ Screen share toggle already in progress');
      return;
    }

    screenShareStateRef.current.isProcessing = true;
    setIsProcessingScreenShare(true);

    try {
      console.log('🖥️ Toggle screen share requested', {
        isScreenSharing: screenShareStateRef.current.isSharing,
        livekitConnected,
        hasRoom: !!room,
        roomState: room?.state,
        hasLocalParticipant: !!localParticipant,
        participants: participants.length
      });

      // Clear any previous errors
      setScreenShareError(null);

      // Check actual LiveKit connection state
      const isLiveKitReady = livekitConnected &&
        room &&
        room.state === 'connected' &&
        localParticipant;

      if (!isLiveKitReady) {
        console.error('❌ LiveKit not ready for screen share:', {
          livekitConnected,
          hasRoom: !!room,
          roomState: room?.state,
          hasLocalParticipant: !!localParticipant
        });

        const error = new Error('Not connected to meeting. Please wait for connection to establish.');
        setScreenShareError(error.message);
        throw error;
      }

      if (screenShareStateRef.current.isSharing) {
        console.log('🛑 Stopping screen share via LiveKit...');
        const result = await livekitStopScreenShare();
        console.log('✅ Screen share stopped successfully');

        // Wait for state to update
        await new Promise(resolve => setTimeout(resolve, 500));

        return result;
      } else {
        console.log('▶️ Starting screen share via LiveKit...');

        try {
          const result = await livekitStartScreenShare();
          console.log('✅ Screen share started successfully');

          // Wait for state to propagate
          await new Promise(resolve => setTimeout(resolve, 500));

          // Verify screen share is working
          setTimeout(() => {
            console.log('🔍 Verifying screen share status:', {
              isScreenSharing: screenShareStateRef.current.isSharing,
              screenShareStream: !!screenShareStream,
              participants: participants.length
            });
          }, 1000);

          return result;
        } catch (shareError) {
          console.error('🔥 LiveKit screen share error:', shareError);

          // Try browser fallback if LiveKit fails
          console.warn('⚠️ LiveKit screen share failed, trying browser fallback...');

          try {
            console.log('🔄 Attempting browser screen share fallback...');

            const stream = await navigator.mediaDevices.getDisplayMedia({
              video: {
                width: { ideal: 1920, max: 1920 },
                height: { ideal: 1080, max: 1080 },
                frameRate: { ideal: 30, max: 30 }
              },
              audio: false
            });

            if (stream && stream.getVideoTracks().length > 0) {
              console.log('✅ Browser screen share fallback successful');

              // Handle stream end
              stream.getVideoTracks()[0].onended = () => {
                console.log('🖥️ Screen share ended by user');
                setScreenShareError(null);
              };

              // Note: This won't integrate with LiveKit, but provides basic functionality
              console.log('📺 Browser screen share active (not integrated with LiveKit)');
              return true;
            }
          } catch (browserError) {
            console.error('❌ Browser screen share also failed:', browserError);
          }

          // Handle specific screen share errors
          if (shareError.name === 'NotAllowedError' || shareError.message?.includes('Permission denied')) {
            const message = 'Screen share permission denied. Please allow screen sharing and try again.';
            setScreenShareError(message);
            throw new Error(message);
          } else if (shareError.name === 'NotFoundError') {
            const message = 'No screen sources available to share.';
            setScreenShareError(message);
            throw new Error(message);
          } else if (shareError.name === 'NotReadableError') {
            const message = 'Screen source is already being used by another application.';
            setScreenShareError(message);
            throw new Error(message);
          } else if (shareError.message?.includes('User cancelled')) {
            const message = 'Screen share cancelled.';
            setScreenShareError(message);
            throw new Error(message);
          } else if (shareError.message?.includes('No room connection')) {
            const message = 'Meeting connection not ready. Please wait and try again.';
            setScreenShareError(message);
            throw new Error(message);
          } else {
            const message = shareError.message || 'Failed to start screen sharing';
            setScreenShareError(message);
            throw shareError;
          }
        }
      }
    } catch (error) {
      console.error('❌ Screen share error:', error);
      setScreenShareError(error.message || 'Failed to toggle screen share');

      // Ensure we're in a clean state
      if (screenShareStateRef.current.isSharing && livekitStopScreenShare) {
        try {
          await livekitStopScreenShare();
        } catch (cleanupError) {
          console.error('❌ Failed to cleanup screen share:', cleanupError);
        }
      }

      throw error; // Re-throw to be handled by MeetingRoom
    } finally {
      // Reset processing state
      screenShareStateRef.current.isProcessing = false;
      setIsProcessingScreenShare(false);
    }
  }, [
    livekitStopScreenShare,
    livekitStartScreenShare,
    livekitConnected,
    room,
    localParticipant,
    participants,
    screenShareStream
  ]);

  // Recording toggle handler with backend API
  const handleToggleRecording = useCallback(async () => {
    console.log("🔴 Toggle recording requested, current state:", isRecording);

    if (!isHost) {
      console.warn('⚠️ Only hosts can control recording');
      return;
    }

    if (!livekitConnected) {
      console.warn('⚠️ Not connected to meeting');
      return;
    }

    const meetingIdForRecording = actualMeeting?.id || realMeetingIdState || realMeetingId;
    if (!meetingIdForRecording) {
      console.error('❌ No meeting ID available for recording');
      return;
    }

    try {
      if (isRecording) {
        // Stop recording via API
        console.log("⏹️ Stopping recording via API...");

        const response = await fetch(`${API_BASE_URL}/api/meetings/${meetingIdForRecording}/stop-recording`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${localStorage.getItem('token') || ''}`
          }
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.Error || 'Failed to stop recording');
        }

        const result = await response.json();
        console.log("✅ Recording stopped via API:", result);

      } else {
        // Start recording via API
        console.log("▶️ Starting recording via API...");

        const response = await fetch(`${API_BASE_URL}/api/meetings/${meetingIdForRecording}/start-recording`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${localStorage.getItem('token') || ''}`
          }
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.Error || 'Failed to start recording');
        }

        const result = await response.json();
        console.log("✅ Recording started via API:", result);
      }

    } catch (error) {
      console.error("❌ Recording API error:", error);
      // Let the useWebRTC hook handle the state
    }
  }, [isHost, livekitConnected, isRecording, actualMeeting, realMeetingIdState, realMeetingId]);

  const handleSendReaction = useCallback((emoji) => {
    const properParticipantName = getParticipantName();

    // Send via LiveKit
    const success = livekitSendReaction(emoji.emoji || emoji);

    if (success) {
      console.log('😀 Reaction sent via LiveKit:', emoji);
    } else {
      console.error('❌ Failed to send reaction via LiveKit');
    }

    return success;
  }, [livekitSendReaction, getParticipantName]);

  // Create instant meeting via API
  const createInstantMeeting = async () => {
    setIsCreatingMeeting(true);
    try {
      console.log('🚀 Creating instant meeting via API...');

      const response = await fetch(`${API_BASE_URL}/api/meetings/instant-meeting`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          Meeting_Name: 'Instant Meeting',
          Host_ID: user?.id || 'default-host',
          Meeting_Type: 'InstantMeeting',
          Status: 'active',
          Is_Recording_Enabled: false,
          Waiting_Room_Enabled: false
        })
      });

      console.log('✅ Instant meeting created:', response.status);

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.Error || 'Failed to create meeting');
      }

      const data = await response.json();
      const meetingId = data.Meeting_ID;
      const meetingLink = data.Meeting_Link;

      if (!meetingId) {
        throw new Error('No meeting ID returned from server');
      }

      console.log('🔑 Meeting created with UUID:', meetingId);

      // Update state with the created meeting
      setActualMeeting({
        id: meetingId,
        title: 'Instant Meeting',
        link: meetingLink,
        meeting_name: 'Instant Meeting',
        host_id: user?.id
      });

      setRealMeetingIdState(meetingId);
      setIsHost(true);

      console.log('✅ Meeting state updated with UUID:', meetingId);

    } catch (error) {
      console.error('❌ Failed to create instant meeting:', error);

      let errorMessage = 'Failed to create instant meeting';

      if (error.response?.status === 500) {
        errorMessage = 'Server error - please check database configuration';
      } else if (error.response?.status === 404) {
        errorMessage = 'Meeting API endpoint not found';
      } else if (error.message) {
        errorMessage = error.message;
      }

      setJoinError(new Error(errorMessage));
    } finally {
      setIsCreatingMeeting(false);
    }
  };


  useEffect(() => {
    return () => {
      // ✅ CRITICAL: Only cleanup if actually leaving
      const isActuallyLeaving =
        !window.location.pathname.includes('/meeting/') ||
        meetingEnded;

      if (isActuallyLeaving) {
        console.log("🧹 MeetingPage: Cleanup on actual unmount");
        // cleanup code
      } else {
        console.log("⏸️ MeetingPage: Skipping cleanup - still in meeting");
      }
    };
  }, [meetingEnded]);
  // Show loading state
  if (meetingLoading || isJoining || isCreatingMeeting) {
    return (
      <LoadingContainer>
          <CardContent>
            <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
            <AnimatedLoader />

              <Typography variant="h5" sx={{ fontWeight: 600, color: 'white' }}>
                {isCreatingMeeting ? "Creating Meeting" :
                  isJoining ? "Joining Meeting" :
                    "Loading Meeting"}
              </Typography>

              <Typography variant="body1" sx={{ color: 'rgba(255, 255, 255, 0.7)', textAlign: 'center' }}>
                {isCreatingMeeting ? "Setting up your instant meeting room..." :
                  isJoining ? "Establishing secure connection via LiveKit..." :
                    "Preparing your meeting experience..."}
              </Typography>

              <StatusIndicator>
                <Box sx={{
                  width: 8,
                  height: 8,
                  borderRadius: '50%',
                  backgroundColor: '#3b82f6',
                  animation: 'pulse 2s infinite'
                }} />
                <Typography variant="body2">
                  {livekitConnecting ? 'Connecting to LiveKit...' :
                    livekitConnected ? 'Connected' : 'Initializing...'}
                </Typography>
              </StatusIndicator>
            </Box>
          </CardContent>
      </LoadingContainer>
    );
  }

  // Show error state only for critical errors
  const criticalError = joinError && !joinError.message?.includes('Database error');
  if (criticalError) {
    return (
      <ErrorContainer>
        <ErrorCard>
          <CardContent sx={{ p: 4 }}>
            <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
              <ErrorOutline sx={{ fontSize: 64, color: '#ef4444' }} />

              <Typography variant="h4" sx={{ fontWeight: 600, color: 'white', textAlign: 'center' }}>
                Unable to Join Meeting
              </Typography>

              <Typography variant="body1" sx={{ color: 'rgba(255, 255, 255, 0.8)', textAlign: 'center', maxWidth: 400 }}>
                {joinError.message || joinError || 'There was an error joining the meeting'}
              </Typography>

              <Box sx={{ display: 'flex', gap: 2, mt: 2 }}>
                <ActionButton
                  variant="outlined"
                  startIcon={<ArrowBack />}
                  onClick={() => navigate('/dashboard')}
                >
                  Go Back
                </ActionButton>

                <ActionButton
                  variant="primary"
                  startIcon={<Refresh />}
                  onClick={() => window.location.reload()}
                >
                  Try Again
                </ActionButton>
              </Box>

              <Typography variant="body2" sx={{ color: 'rgba(255, 255, 255, 0.5)', textAlign: 'center', mt: 2 }}>
                If the problem persists, please contact support
              </Typography>
            </Box>
          </CardContent>
        </ErrorCard>
      </ErrorContainer>
    );
  }

  // Use only LiveKit state for waiting room decision
  const shouldShowWaitingRoom = showWaitingRoom ||
    isJoining ||
    livekitConnecting ||
    !livekitConnected ||
    !hasTriedJoin ||
    !room ||
    room.state !== 'connected' ||
    !localParticipant;

  console.log('🎭 Render decision:', {
    shouldShowWaitingRoom,
    showWaitingRoom,
    isJoining,
    livekitConnecting,
    livekitConnected,
    hasTriedJoin,
    hasRoom: !!room,
    roomState: room?.state,
    hasLocalParticipant: !!localParticipant,
    webrtcInitialized
  });

  if (shouldShowWaitingRoom) {
    return (
      <MeetingPageContainer>
        <WaitingRoom
          meetingId={meetingId}
          onJoin={handleJoinFromWaitingRoom}
          isHost={isHost}
          meetingData={actualMeeting || meeting || {
            id: actualMeetingId,
            title: meetingId === 'instant' ? 'Instant Meeting' : 'Video Meeting'
          }}
          inWaitingRoom={false} // LiveKit doesn't use traditional waiting rooms
          onAllowUser={() => { }} // Not needed with LiveKit
          errors={webrtcErrors}
          isConnecting={isJoining || livekitConnecting}
          connectionState={livekitConnected ? 'connected' : livekitConnecting ? 'connecting' : 'disconnected'}
          realMeetingId={actualMeeting?.id || realMeetingIdState || realMeetingId}
          getMeetingIdForSharing={getMeetingIdForSharing}
          isConnected={livekitConnected} // Use LiveKit connection state
          currentUser={user}
        />
      </MeetingPageContainer>
    );
  }

  // Show actual meeting room
  return (
    <MeetingPageContainer>
      <Fade in={true} timeout={500}>
        <Box sx={{ height: '100%', width: '100%' }}>
          <MeetingRoom
            meetingData={actualMeeting || meeting || {
              title: meetingId === 'instant' ? 'Instant Meeting' : 'Video Meeting',
              id: getMeetingIdForSharing() || actualMeetingId
            }}
            participants={participants}
            currentUser={user}
            localStream={localStream}
            remoteStreams={remoteStreams}
            screenShareStream={screenShareStream}
            screenSharer={screenSharer}
            onLeaveMeeting={handleLeaveMeeting}
            onToggleAudio={handleToggleAudio}
            onToggleVideo={handleToggleVideo}
            onToggleScreenShare={handleToggleScreenShare}
            onSendReaction={handleSendReaction}
            onToggleRecording={handleToggleRecording}
            isConnected={livekitConnected} // Use LiveKit connection state
            isVideoEnabled={isVideoEnabled}
            isAudioEnabled={isAudioEnabled}
            isScreenSharing={isScreenSharing}
            isRecording={isRecording}
            connectionQuality={connectionQuality}
            webrtcErrors={webrtcErrors}
            meetingStatus={meetingStatus}
            isHost={isHost}
            connectionState={livekitConnected ? 'connected' : 'disconnected'}
            realMeetingId={actualMeeting?.id || realMeetingIdState || realMeetingId}
            isWebSocketConnected={() => livekitConnected} // Use LiveKit connection
            participantId={participantId}
          />
        </Box>
      </Fade>

      {/* Screen Share Error Notification */}
      {screenShareError && (
        <NotificationOverlay>
          <NotificationCard severity="error">
            <CardContent sx={{ p: 2 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                <ScreenShare sx={{ color: 'white' }} />
                <Box>
                  <Typography variant="body1" sx={{ fontWeight: 600, color: 'white' }}>
                    Screen Share Error
                  </Typography>
                  <Typography variant="body2" sx={{ color: 'rgba(255, 255, 255, 0.9)' }}>
                    {screenShareError}
                  </Typography>
                </Box>
              </Box>
            </CardContent>
          </NotificationCard>
        </NotificationOverlay>
      )}

      {/* Processing Screen Share Indicator */}
      {isProcessingScreenShare && (
        <ProcessingOverlay open={isProcessingScreenShare}>
          <ProcessingCard>
            <CardContent>
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
              <AnimatedLoader sx={{ width: '60px', height: '60px' }} />
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  {isScreenSharing ? 'Stopping Screen Share' : 'Starting Screen Share'}
                </Typography>
                <Typography variant="body2" sx={{ color: 'rgba(255, 255, 255, 0.7)' }}>
                  Please wait while we process your request...
                </Typography>
              </Box>
            </CardContent>
          </ProcessingCard>
        </ProcessingOverlay>
      )}
    </MeetingPageContainer>
  );
};

export default MeetingPage;