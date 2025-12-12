// src/components/video/VideoPlayer.jsx - FIXED VERSION WITH TRUE FULLSCREEN SCREEN SHARE
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Typography,
  Slider,
  Fade,
  Alert,
  Avatar,
  Card,
  CardContent,
  CircularProgress,
} from '@mui/material';
import {
  VolumeUp,
  VolumeOff,
  Fullscreen,
  FullscreenExit,
  Mic,
  MicOff,
  Videocam,
  VideocamOff,
  SignalWifi4Bar,
  SignalWifi3Bar,
  SignalWifi2Bar,
  SignalWifi1Bar,
  SignalWifiOff,
  Monitor,
  Person,
  Refresh,
  Warning,
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

// FIXED: VideoContainer with proper fullscreen screen share handling
const VideoContainer = styled(Box, {
  shouldForwardProp: (prop) => !['isScreenShare', 'isLocal'].includes(prop)
})(({ theme, isScreenShare, isLocal }) => ({
  position: 'relative',
  width: '100%',
  height: '100%',
  borderRadius: isScreenShare ? 0 : theme.spacing(1),
  overflow: 'hidden',
  backgroundColor: '#000', // Always black background for screen share
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  minHeight: '100%',
  maxHeight: '100%',
  // FIXED: Remove all padding/margin for screen share to eliminate gaps
  ...(isScreenShare && {
    padding: 0,
    margin: 0,
    borderRadius: 0,
  }),
}));

// FIXED: VideoElement with proper screen share scaling options
const VideoElement = styled('video', {
  shouldForwardProp: (prop) => !['isScreenShare', 'isLocal', 'fillMode'].includes(prop)
})(({ theme, isScreenShare, isLocal, fillMode = 'contain' }) => ({
  width: '100%',
  height: '100%',
  backgroundColor: '#000',
  borderRadius: isScreenShare ? 0 : theme.spacing(1),
  transform: isLocal && !isScreenShare ? 'scaleX(-1)' : 'none',
  maxWidth: '100%',
  maxHeight: '100%',
  display: 'block',
  // FIXED: Dynamic object-fit based on screen share and fill mode
  objectFit: isScreenShare ? 
    (fillMode === 'fill' ? 'cover' : fillMode === 'stretch' ? 'fill' : 'contain') : 
    'cover',
  objectPosition: 'center',
  // FIXED: Ensure no gaps for screen share
  ...(isScreenShare && {
    minHeight: '100%',
    minWidth: '100%',
  }),
}));

const ControlsOverlay = styled(Box, {
  shouldForwardProp: (prop) => prop !== 'compact'
})(({ theme, compact }) => ({
  position: 'absolute',
  bottom: theme.spacing(1),
  right: theme.spacing(1),
  display: 'flex',
  gap: theme.spacing(0.5),
  opacity: 0,
  transition: 'opacity 0.3s ease',
  zIndex: 10,
  backgroundColor: 'rgba(0,0,0,0.7)',
  borderRadius: theme.spacing(0.5),
  padding: theme.spacing(0.5),
  
  '&.visible': {
    opacity: 1,
  },
}));

const ScreenShareControls = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: theme.spacing(1),
  left: theme.spacing(1),
  display: 'flex',
  gap: theme.spacing(0.5),
  zIndex: 15,
}));

const FillModeButton = styled(IconButton)(({ theme }) => ({
  backgroundColor: 'rgba(0,0,0,0.7)',
  color: 'white',
  width: 32,
  height: 32,
  '&:hover': {
    backgroundColor: 'rgba(0,0,0,0.9)',
  },
  '& svg': {
    fontSize: 16,
  },
}));

const ConnectionIndicator = styled(Box, {
  shouldForwardProp: (prop) => prop !== 'quality'
})(({ theme, quality }) => ({
  position: 'absolute',
  top: theme.spacing(1),
  left: theme.spacing(1),
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(0.5),
  backgroundColor: 'rgba(0,0,0,0.7)',
  borderRadius: theme.spacing(0.5),
  padding: theme.spacing(0.25, 0.75),
  zIndex: 10,
}));

const NoVideoOverlay = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: 0,
  left: 0,
  right: 0,
  bottom: 0,
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  backgroundColor: 'rgba(26,26,26,0.9)',
  color: 'white',
  zIndex: 5,
}));

const VolumeControl = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: '50%',
  right: theme.spacing(5),
  transform: 'translateY(-50%)',
  backgroundColor: 'rgba(0,0,0,0.8)',
  borderRadius: theme.spacing(2),
  padding: theme.spacing(1, 2),
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  minWidth: '150px',
  zIndex: 15,
}));

const LoadingOverlay = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: 0,
  left: 0,
  right: 0,
  bottom: 0,
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  backgroundColor: 'rgba(0,0,0,0.8)',
  color: 'white',
  zIndex: 20,
}));

const ScreenShareIndicator = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: theme.spacing(0.5),
  right: theme.spacing(0.5),
  backgroundColor: 'rgba(33,150,243,0.7)',
  color: 'white',
  padding: theme.spacing(0.25, 0.75),
  borderRadius: theme.spacing(1),
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(0.25),
  zIndex: 15,
  fontSize: '0.65rem',
}));

const SystemAudioIndicator = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: theme.spacing(2.5),
  right: theme.spacing(0.5),
  backgroundColor: 'rgba(76, 175, 80, 0.8)',
  color: 'white',
  padding: theme.spacing(0.25, 0.75),
  borderRadius: theme.spacing(1),
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(0.25),
  zIndex: 15,
  fontSize: '0.6rem',
  animation: 'pulse 2s infinite',
  '@keyframes pulse': {
    '0%': { opacity: 1 },
    '50%': { opacity: 0.7 },
    '100%': { opacity: 1 },
  },
}));

function VideoPlayer({
  stream,
  participant,
  isLocal = false,
  isMuted = false,
  isVideoEnabled = true,
  participantName = '',
  participantId = '',
  participantConnectionId = '',
  quality = 'good',
  volume = 1.0,
  showControls = true,
  compact = false,
  isScreenShare = false,
  onVolumeChange,
  onToggleMute,
  onToggleVideo,
  onFullscreen,
}) {
  // State management
  const [localVolume, setLocalVolume] = useState(volume);
  const [showVolumeSlider, setShowVolumeSlider] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);
  const [hasVideo, setHasVideo] = useState(false);
  const [hasAudio, setHasAudio] = useState(false);
  const [hasSystemAudio, setHasSystemAudio] = useState(false);
  const [streamError, setStreamError] = useState(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [showControlsVisible, setShowControlsVisible] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [audioLevels, setAudioLevels] = useState(0);
  
  // ADDED: Screen share specific states
  const [screenShareFillMode, setScreenShareFillMode] = useState('contain'); // 'contain', 'fill', 'stretch'

  // Refs
  const videoRef = useRef(null);
  const containerRef = useRef(null);
  const controlsTimeoutRef = useRef(null);
  const audioContextRef = useRef(null);
  const analyserRef = useRef(null);
  const audioCheckIntervalRef = useRef(null);

  // Validate and convert stream to proper MediaStream
  const validStream = useMemo(() => {
    if (!stream) return null;
    
    // Check if it's already a MediaStream
    if (stream instanceof MediaStream) {
      return stream;
    }
    
    // Check if it's a LiveKit RemoteTrack or LocalTrack
    if (stream && stream.mediaStreamTrack instanceof MediaStreamTrack) {
      return new MediaStream([stream.mediaStreamTrack]);
    }
    
    // Check if it's a LiveKit Track with attach() method
    if (stream && typeof stream.attach === 'function') {
      try {
        const element = stream.attach();
        if (element && element.srcObject instanceof MediaStream) {
          return element.srcObject;
        }
        if (stream.mediaStreamTrack) {
          return new MediaStream([stream.mediaStreamTrack]);
        }
      } catch (e) {
        console.warn('Error attaching stream:', e);
      }
    }
    
    // Check if it has getTracks method
    if (stream && typeof stream.getTracks === 'function') {
      try {
        const tracks = stream.getTracks();
        if (tracks && tracks.length > 0) {
          return stream;
        }
      } catch (e) {
        console.warn('Stream has getTracks but might not be valid:', e);
      }
    }
    
    // Check if it's a MediaStreamTrack directly
    if (stream instanceof MediaStreamTrack) {
      return new MediaStream([stream]);
    }
    
    // Check if it's an HTMLMediaElement (video/audio element)
    if (stream instanceof HTMLMediaElement && stream.srcObject) {
      return stream.srcObject;
    }
    
    // Special handling for LiveKit track objects
    if (stream && typeof stream === 'object') {
      if (stream.track instanceof MediaStreamTrack) {
        return new MediaStream([stream.track]);
      }
      if (stream.mediaStream instanceof MediaStream) {
        return stream.mediaStream;
      }
      if (stream.videoTrack?.mediaStreamTrack) {
        const tracks = [];
        if (stream.videoTrack.mediaStreamTrack) {
          tracks.push(stream.videoTrack.mediaStreamTrack);
        }
        if (stream.audioTrack?.mediaStreamTrack) {
          tracks.push(stream.audioTrack.mediaStreamTrack);
        }
        if (tracks.length > 0) {
          return new MediaStream(tracks);
        }
      }
    }
    
    console.warn('Invalid stream type:', typeof stream, stream);
    return null;
  }, [stream]);

  // Use validStream to get tracks safely
  const videoTracks = useMemo(() => {
    try {
      return validStream?.getVideoTracks?.() || [];
    } catch (e) {
      console.warn('Error getting video tracks:', e);
      return [];
    }
  }, [validStream]);

  const audioTracks = useMemo(() => {
    try {
      return validStream?.getAudioTracks?.() || [];
    } catch (e) {
      console.warn('Error getting audio tracks:', e);
      return [];
    }
  }, [validStream]);

  console.log(`VideoPlayer rendering:`, {
    participantName,
    isScreenShare,
    hasStream: !!validStream,
    streamId: validStream?.id,
    videoTracks: videoTracks.length,
    audioTracks: audioTracks.length,
    isLocal,
    isVideoEnabled,
    screenShareFillMode
  });

  // Enhanced system audio detection for screen share
  const detectSystemAudio = useCallback((stream) => {
    if (!stream || !isScreenShare) return;

    try {
      const audioTracks = stream.getAudioTracks();
      console.log('Screen share audio tracks:', audioTracks.map(t => ({ 
        label: t.label, 
        enabled: t.enabled, 
        settings: t.getSettings() 
      })));

      if (audioTracks.length === 0) {
        console.warn('No audio tracks in screen share stream');
        return;
      }

      audioTracks.forEach(track => {
        console.log('System audio track details:', {
          label: track.label,
          kind: track.kind,
          enabled: track.enabled,
          muted: track.muted,
          readyState: track.readyState,
          settings: track.getSettings()
        });

        if (track.label.toLowerCase().includes('tab') || 
            track.label.toLowerCase().includes('system') ||
            track.label.toLowerCase().includes('display')) {
          setHasSystemAudio(true);
          console.log('✅ System audio detected and enabled');
        }
      });

      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const source = audioContext.createMediaStreamSource(stream);
      const analyser = audioContext.createAnalyser();
      
      analyser.fftSize = 64;
      source.connect(analyser);
      
      const bufferLength = analyser.frequencyBinCount;
      const dataArray = new Uint8Array(bufferLength);
      
      let audioDetected = false;
      
      const checkAudio = setInterval(() => {
        if (!analyser) return;
        
        analyser.getByteFrequencyData(dataArray);
        const average = dataArray.reduce((a, b) => a + b) / bufferLength;
        
        if (average > 0.5) {
          if (!audioDetected) {
            console.log('✅ SYSTEM AUDIO CONFIRMED - Audio flowing!');
            setHasSystemAudio(true);
            audioDetected = true;
          }
        }
      }, 1000);

      setTimeout(() => {
        clearInterval(checkAudio);
        audioContext.close();
      }, 30000);
      
    } catch (error) {
      console.error('System audio detection error:', error);
    }
  }, [isScreenShare, hasSystemAudio]);

  // ADDED: Screen share fill mode cycle function
  const cycleFillMode = useCallback(() => {
    setScreenShareFillMode(prev => {
      switch(prev) {
        case 'contain': return 'fill'; // Cover/crop to fill
        case 'fill': return 'stretch'; // Stretch to fit exactly
        case 'stretch': return 'contain'; // Maintain aspect ratio
        default: return 'contain';
      }
    });
  }, []);

  // ADDED: Get fill mode tooltip text
  const getFillModeTooltip = () => {
    switch(screenShareFillMode) {
      case 'contain': return 'Fit screen (may have gaps) → Click for Fill';
      case 'fill': return 'Fill screen (may crop edges) → Click for Stretch';
      case 'stretch': return 'Stretch to fit → Click for Fit';
      default: return 'Change screen fit mode';
    }
  };

  // Stream assignment with proper screen share handling
  useEffect(() => {
    const videoElement = videoRef.current;
    if (!videoElement) return;

    setIsLoading(true);
    setStreamError(null);

    console.log(`Setting up stream for ${participantName}:`, {
      hasStream: !!validStream,
      streamId: validStream?.id,
      isScreenShare,
      videoTracks: videoTracks.length,
      audioTracks: audioTracks.length,
      fillMode: screenShareFillMode
    });

    if (validStream && validStream instanceof MediaStream) {
      try {
        console.log(`Stream analysis for ${participantName}:`, {
          streamActive: validStream.active,
          videoTracks: videoTracks.length,
          audioTracks: audioTracks.length,
          videoTrackStates: videoTracks.map(t => ({ 
            id: t.id, 
            enabled: t.enabled, 
            readyState: t.readyState,
            label: t.label 
          })),
          audioTrackStates: audioTracks.map(t => ({ 
            id: t.id, 
            enabled: t.enabled, 
            readyState: t.readyState,
            label: t.label 
          }))
        });

        setHasVideo(videoTracks.length > 0);
        setHasAudio(audioTracks.length > 0);

        // Set the stream to video element
        videoElement.srcObject = validStream;

        // FIXED: Enhanced video element configuration for screen share
        if (isScreenShare) {
          // Apply the selected fill mode
          switch(screenShareFillMode) {
            case 'contain':
              videoElement.style.objectFit = 'contain';
              break;
            case 'fill':
              videoElement.style.objectFit = 'cover';
              break;
            case 'stretch':
              videoElement.style.objectFit = 'fill';
              break;
          }
          
          videoElement.style.backgroundColor = '#000';
          videoElement.style.width = '100%';
          videoElement.style.height = '100%';
          videoElement.style.maxWidth = '100%';
          videoElement.style.maxHeight = '100%';
          videoElement.style.minHeight = '100%';
          videoElement.style.minWidth = '100%';
          videoElement.style.display = 'block';
          videoElement.style.margin = '0';
          videoElement.style.padding = '0';
          videoElement.style.borderRadius = '0';
          
          // Audio settings for screen share
          videoElement.muted = isLocal;
          videoElement.volume = isLocal ? 0 : 1;
          
          // Ensure high quality for screen share
          if (videoTracks[0]) {
            videoTracks[0].applyConstraints({
              width: { ideal: 1920, max: 3840 },
              height: { ideal: 1080, max: 2160 },
              frameRate: { ideal: 30, max: 60 }
            }).catch(err => console.warn('Could not apply screen share constraints:', err));
          }
          
          // Setup system audio detection
          if (audioTracks.length > 0) {
            detectSystemAudio(validStream);
          }
          
          console.log(`Screen share configured with fill mode: ${screenShareFillMode}`);
        } else {
          // Regular video settings
          videoElement.style.objectFit = 'cover';
          videoElement.style.backgroundColor = '#1a1a1a';
          videoElement.style.width = '100%';
          videoElement.style.height = '100%';
          videoElement.muted = isLocal || isMuted;
          videoElement.volume = isLocal ? 0 : localVolume;
        }

        // Configure video element properties
        videoElement.autoplay = true;
        videoElement.playsInline = true;
        videoElement.controls = false;

        const handleLoadStart = () => {
          console.log(`Video load start for ${participantName}`);
          setIsLoading(true);
        };

        const handleLoadedMetadata = () => {
          console.log(`Video metadata loaded for ${participantName}:`, {
            videoWidth: videoElement.videoWidth,
            videoHeight: videoElement.videoHeight,
            duration: videoElement.duration
          });
          
          if (isScreenShare && videoElement.videoWidth && videoElement.videoHeight) {
            const aspectRatio = videoElement.videoWidth / videoElement.videoHeight;
            console.log(`Screen share aspect ratio: ${aspectRatio}, fillMode: ${screenShareFillMode}`);
          }
        };

        const handleCanPlay = () => {
          console.log(`Video can play for ${participantName}`);
          setIsLoading(false);
          setIsPlaying(true);
          setStreamError(null);
        };

        const handlePlaying = () => {
          console.log(`Video playing for ${participantName}`);
          setIsLoading(false);
          setIsPlaying(true);
          setStreamError(null);
        };

        const handleError = (error) => {
          console.error(`Video error for ${participantName}:`, error);
          setStreamError(error.target?.error || 'Video playback error');
          setIsLoading(false);
          setIsPlaying(false);
        };

        const handleStalled = () => {
          console.warn(`Video stalled for ${participantName}`);
        };

        const handleWaiting = () => {
          console.warn(`Video waiting for data for ${participantName}`);
        };

        const handleEnded = () => {
          console.log(`Video ended for ${participantName}`);
          setIsPlaying(false);
        };

        // Add event listeners
        videoElement.addEventListener('loadstart', handleLoadStart);
        videoElement.addEventListener('loadedmetadata', handleLoadedMetadata);
        videoElement.addEventListener('canplay', handleCanPlay);
        videoElement.addEventListener('playing', handlePlaying);
        videoElement.addEventListener('error', handleError);
        videoElement.addEventListener('stalled', handleStalled);
        videoElement.addEventListener('waiting', handleWaiting);
        videoElement.addEventListener('ended', handleEnded);

        // Attempt to play
        const playPromise = videoElement.play();
        if (playPromise !== undefined) {
          playPromise
            .then(() => {
              console.log(`Video play promise resolved for ${participantName}`);
              setIsPlaying(true);
              setIsLoading(false);
            })
            .catch((playError) => {
              console.warn(`Video play promise rejected for ${participantName}:`, playError);
              if (playError.name !== 'NotAllowedError') {
                setStreamError(`Playback failed: ${playError.message}`);
              }
              setIsLoading(false);
            });
        }

        // Cleanup function
        return () => {
          videoElement.removeEventListener('loadstart', handleLoadStart);
          videoElement.removeEventListener('loadedmetadata', handleLoadedMetadata);
          videoElement.removeEventListener('canplay', handleCanPlay);
          videoElement.removeEventListener('playing', handlePlaying);
          videoElement.removeEventListener('error', handleError);
          videoElement.removeEventListener('stalled', handleStalled);
          videoElement.removeEventListener('waiting', handleWaiting);
          videoElement.removeEventListener('ended', handleEnded);

          if (audioCheckIntervalRef.current) {
            clearInterval(audioCheckIntervalRef.current);
            audioCheckIntervalRef.current = null;
          }
          if (audioContextRef.current) {
            audioContextRef.current.close();
            audioContextRef.current = null;
          }
        };

      } catch (error) {
        console.error(`Error setting up video for ${participantName}:`, error);
        setStreamError(`Setup error: ${error.message}`);
        setIsLoading(false);
      }
    } else {
      console.log(`No valid stream available for ${participantName}`);
      videoElement.srcObject = null;
      setHasVideo(false);
      setHasAudio(false);
      setIsPlaying(false);
      setIsLoading(false);
      setStreamError(null);
      setHasSystemAudio(false);
    }
  }, [validStream, participantName, isLocal, localVolume, isScreenShare, isMuted, videoTracks, audioTracks, detectSystemAudio, screenShareFillMode]);

  // Volume control
  const handleVolumeChange = useCallback((event, newValue) => {
    setLocalVolume(newValue);
    if (videoRef.current && !isLocal) {
      videoRef.current.volume = newValue;
    }
    if (onVolumeChange) {
      onVolumeChange(newValue);
    }
  }, [isLocal, onVolumeChange]);

  // Controls visibility management
  const showControlsTemporarily = useCallback(() => {
    if (!showControls || compact) return;

    setShowControlsVisible(true);
    
    if (controlsTimeoutRef.current) {
      clearTimeout(controlsTimeoutRef.current);
    }
    
    controlsTimeoutRef.current = setTimeout(() => {
      setShowControlsVisible(false);
    }, 3000);
  }, [showControls, compact]);

  // Mouse movement handler
  const handleMouseMove = useCallback(() => {
    showControlsTemporarily();
  }, [showControlsTemporarily]);

  // Fullscreen handlers
  const handleFullscreenToggle = useCallback(() => {
    if (!containerRef.current) return;

    if (!isFullscreen) {
      if (containerRef.current.requestFullscreen) {
        containerRef.current.requestFullscreen();
      }
    } else {
      if (document.exitFullscreen) {
        document.exitFullscreen();
      }
    }
  }, [isFullscreen]);

  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };

    document.addEventListener('fullscreenchange', handleFullscreenChange);
    return () => document.removeEventListener('fullscreenchange', handleFullscreenChange);
  }, []);

  // Retry stream connection
  const retryConnection = useCallback(() => {
    console.log(`Retrying video connection for ${participantName}`);
    setStreamError(null);
    setIsLoading(true);
    
    if (videoRef.current && validStream) {
      videoRef.current.load();
    }
  }, [participantName, validStream]);

  // Get connection quality icon
  const getQualityIcon = () => {
    switch (quality) {
      case 'excellent': return <SignalWifi4Bar />;
      case 'good': return <SignalWifi3Bar />;
      case 'medium': return <SignalWifi2Bar />;
      case 'poor': return <SignalWifi1Bar />;
      default: return <SignalWifiOff />;
    }
  };

  // Get quality color
  const getQualityColor = () => {
    switch (quality) {
      case 'excellent': return '#4caf50';
      case 'good': return '#8bc34a';
      case 'medium': return '#ff9800';
      case 'poor': return '#f44336';
      default: return '#666';
    }
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (audioCheckIntervalRef.current) {
        clearInterval(audioCheckIntervalRef.current);
      }
      if (audioContextRef.current) {
        audioContextRef.current.close();
      }
    };
  }, []);

  return (
    <VideoContainer
      ref={containerRef}
      isScreenShare={isScreenShare}
      isLocal={isLocal}
      onMouseMove={handleMouseMove}
      onMouseEnter={showControlsTemporarily}
    >
      {/* Main Video Element */}
      <VideoElement
        ref={videoRef}
        isScreenShare={isScreenShare}
        isLocal={isLocal}
        fillMode={screenShareFillMode}
        muted={isLocal && !isScreenShare}
      />

      {/* Screen Share Fill Mode Controls - Only show for screen share */}
      {isScreenShare && (
        <ScreenShareControls>
          <Tooltip title={getFillModeTooltip()}>
            <FillModeButton onClick={cycleFillMode}>
              {screenShareFillMode === 'contain' && <Fullscreen />}
              {screenShareFillMode === 'fill' && <FullscreenExit />}
              {screenShareFillMode === 'stretch' && <Monitor />}
            </FillModeButton>
          </Tooltip>
        </ScreenShareControls>
      )}

      {/* Screen share indicator */}
      {isScreenShare && (
        <ScreenShareIndicator>
          <Monitor sx={{ fontSize: 12 }} />
          <Typography variant="caption" sx={{ fontSize: '0.65rem', fontWeight: 'medium' }}>
            Screen Share ({screenShareFillMode})
          </Typography>
        </ScreenShareIndicator>
      )}

      {/* System Audio Indicator */}
      {isScreenShare && hasSystemAudio && (
        <SystemAudioIndicator>
          <VolumeUp sx={{ fontSize: 10 }} />
          <Typography variant="caption" sx={{ fontSize: '0.6rem', fontWeight: 'medium' }}>
            System Audio
          </Typography>
        </SystemAudioIndicator>
      )}

      {/* Connection Quality Indicator */}
      {showControls && !compact && !isScreenShare && (
        <ConnectionIndicator quality={quality}>
          <Box sx={{ color: getQualityColor(), display: 'flex', alignItems: 'center' }}>
            {getQualityIcon()}
          </Box>
          <Typography variant="caption" sx={{ fontSize: '0.7rem', color: 'white' }}>
            {quality}
          </Typography>
        </ConnectionIndicator>
      )}

      {/* No Video Overlay - not shown for screen share */}
      {(!hasVideo || !isVideoEnabled) && !isScreenShare && (
        <NoVideoOverlay>
          <Avatar
            sx={{
              width: compact ? 40 : 80,
              height: compact ? 40 : 80,
              fontSize: compact ? '1rem' : '2rem',
              backgroundColor: isLocal ? '#1976d2' : '#666',
              mb: 2,
            }}
          >
            {participantName?.charAt(0)?.toUpperCase() || 'U'}
          </Avatar>
          <Typography variant={compact ? 'caption' : 'body2'} color="grey.300">
            {participantName || 'Unknown User'}
          </Typography>
          {!isVideoEnabled && (
            <Typography variant="caption" color="grey.500" sx={{ mt: 1 }}>
              Camera is off
            </Typography>
          )}
        </NoVideoOverlay>
      )}

      {/* Loading Overlay */}
      {isLoading && validStream && (
        <LoadingOverlay>
          <CircularProgress size={compact ? 24 : 40} color="primary" />
          <Typography variant="caption" sx={{ mt: 1 }}>
            {isScreenShare ? 'Loading screen share...' : 'Connecting video...'}
          </Typography>
        </LoadingOverlay>
      )}

      {/* Error Overlay */}
      {streamError && !isScreenShare && (
        <LoadingOverlay>
          <Warning sx={{ fontSize: compact ? 24 : 40, color: '#f44336', mb: 1 }} />
          <Typography variant="caption" color="error" align="center" sx={{ mb: 2, px: 1 }}>
            {streamError}
          </Typography>
          <IconButton size="small" onClick={retryConnection} sx={{ color: 'white' }}>
            <Refresh />
          </IconButton>
        </LoadingOverlay>
      )}

      {/* Controls Overlay - hidden for screen share unless hovered */}
      {showControls && !compact && (!isScreenShare || showControlsVisible) && (
        <ControlsOverlay className={showControlsVisible ? 'visible' : ''} compact={compact}>
          {/* Fullscreen Toggle */}
          <Tooltip title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}>
            <IconButton size="small" onClick={handleFullscreenToggle}>
              {isFullscreen ? <FullscreenExit /> : <Fullscreen />}
            </IconButton>
          </Tooltip>

          {/* Volume Control for Remote Participants */}
          {!isLocal && hasAudio && (
            <>
              <Tooltip title={localVolume === 0 ? 'Unmute' : 'Mute'}>
                <IconButton
                  size="small"
                  onClick={() => setShowVolumeSlider(!showVolumeSlider)}
                >
                  {localVolume === 0 ? <VolumeOff /> : <VolumeUp />}
                </IconButton>
              </Tooltip>
            </>
          )}

          {/* Local Participant Controls */}
          {isLocal && !isScreenShare && (
            <>
              <Tooltip title={isMuted ? 'Unmute' : 'Mute'}>
                <IconButton size="small" onClick={onToggleMute}>
                  {isMuted ? <MicOff /> : <Mic />}
                </IconButton>
              </Tooltip>

              <Tooltip title={isVideoEnabled ? 'Turn off camera' : 'Turn on camera'}>
                <IconButton size="small" onClick={onToggleVideo}>
                  {isVideoEnabled ? <Videocam /> : <VideocamOff />}
                </IconButton>
              </Tooltip>
            </>
          )}
        </ControlsOverlay>
      )}

      {/* Volume Slider */}
      {showVolumeSlider && !isLocal && hasAudio && (
        <VolumeControl>
          <VolumeUp sx={{ color: 'white' }} />
          <Slider
            value={localVolume}
            onChange={handleVolumeChange}
            min={0}
            max={1}
            step={0.1}
            sx={{
              color: 'white',
              '& .MuiSlider-thumb': {
                backgroundColor: 'white',
              },
              '& .MuiSlider-track': {
                backgroundColor: 'white',
              },
              '& .MuiSlider-rail': {
                backgroundColor: 'rgba(255,255,255,0.3)',
              },
            }}
          />
          <Typography variant="caption" color="white">
            {Math.round(localVolume * 100)}%
          </Typography>
        </VolumeControl>
      )}

    </VideoContainer>
  );
}

export default VideoPlayer;