import React, { useState, useRef, useEffect } from 'react';
import {
  Card,
  CardContent,
  Avatar,
  Typography,
  Box,
  IconButton,
  Badge,
  Tooltip,
  Chip,
  useTheme,
  alpha,
  Fade,
  Zoom
} from '@mui/material';
import {
  Mic,
  MicOff,
  Videocam,
  VideocamOff,
  VolumeUp,
  VolumeOff,
  PanTool,
  AdminPanelSettings,
  SupervisedUserCircle,
  SignalWifi4Bar,
  SignalWifi3Bar,
  SignalWifi2Bar,
  SignalWifi1Bar,
  SignalWifiOff
} from '@mui/icons-material';
import { styled, keyframes } from '@mui/material/styles';

const pulseAnimation = keyframes`
  0% {
    transform: scale(1);
    opacity: 1;
  }
  50% {
    transform: scale(1.1);
    opacity: 0.7;
  }
  100% {
    transform: scale(1);
    opacity: 1;
  }
`;

const speakingAnimation = keyframes`
  0% { box-shadow: 0 0 0 0 rgba(76, 175, 80, 0.7); }
  70% { box-shadow: 0 0 0 10px rgba(76, 175, 80, 0); }
  100% { box-shadow: 0 0 0 0 rgba(76, 175, 80, 0); }
`;

const StyledCard = styled(Card)(({ theme, isCurrentUser, isSpeaking, isHost }) => ({
  position: 'relative',
  borderRadius: theme.spacing(2),
  overflow: 'hidden',
  transition: 'all 0.3s ease',
  border: `2px solid ${
    isCurrentUser 
      ? theme.palette.primary.main
      : isHost
      ? theme.palette.error.main
      : 'transparent'
  }`,
  background: `linear-gradient(135deg, ${theme.palette.background.paper} 0%, ${alpha(theme.palette.primary.main, 0.05)} 100%)`,
  cursor: 'pointer',
  height: '100%',
  '&:hover': {
    transform: 'translateY(-4px)',
    boxShadow: theme.shadows[8],
    border: `2px solid ${theme.palette.primary.main}`,
  },
  ...(isSpeaking && {
    animation: `${speakingAnimation} 1.5s infinite`,
    border: `3px solid ${theme.palette.success.main}`,
  }),
}));

const VideoContainer = styled(Box)(({ theme }) => ({
  position: 'relative',
  width: '100%',
  height: 200,
  backgroundColor: theme.palette.grey[900],
  borderRadius: theme.spacing(1),
  overflow: 'hidden',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  background: `linear-gradient(135deg, ${theme.palette.grey[800]} 0%, ${theme.palette.grey[900]} 100%)`,
}));

const VideoElement = styled('video')({
  width: '100%',
  height: '100%',
  objectFit: 'cover',
  borderRadius: 'inherit',
});

const ControlsOverlay = styled(Box)(({ theme }) => ({
  position: 'absolute',
  bottom: theme.spacing(1),
  left: theme.spacing(1),
  right: theme.spacing(1),
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  background: alpha(theme.palette.background.paper, 0.9),
  backdropFilter: 'blur(10px)',
  borderRadius: theme.spacing(1),
  padding: theme.spacing(0.5),
}));

const StatusOverlay = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: theme.spacing(1),
  right: theme.spacing(1),
  display: 'flex',
  gap: theme.spacing(0.5),
  alignItems: 'center',
}));

const ReactionOverlay = styled(Box)(({ theme }) => ({
  position: 'absolute',
  top: '50%',
  left: '50%',
  transform: 'translate(-50%, -50%)',
  fontSize: '3rem',
  animation: `${pulseAnimation} 2s ease-in-out`,
  pointerEvents: 'none',
  zIndex: 10,
}));

const ParticipantCard = ({
  participant,
  isCurrentUser = false,
  videoStream = null,
  audioLevel = 0,
  connectionQuality = 'good',
  onMuteToggle,
  onVideoToggle,
  onVolumeChange,
  showControls = true,
  size = 'medium' // small, medium, large
}) => {
  const theme = useTheme();
  const videoRef = useRef(null);
  const [isHovered, setIsHovered] = useState(false);
  const [showReaction, setShowReaction] = useState(false);
  const [currentReaction, setCurrentReaction] = useState('');

  const sizes = {
    small: { width: 150, height: 120, videoHeight: 80 },
    medium: { width: 200, height: 160, videoHeight: 120 },
    large: { width: 280, height: 220, videoHeight: 180 }
  };

  const cardSize = sizes[size];

  useEffect(() => {
    if (videoRef.current && videoStream) {
      videoRef.current.srcObject = videoStream;
    }
  }, [videoStream]);

  useEffect(() => {
    if (participant.current_reaction) {
      setCurrentReaction(participant.current_reaction);
      setShowReaction(true);
      const timer = setTimeout(() => {
        setShowReaction(false);
      }, 3000);
      return () => clearTimeout(timer);
    }
  }, [participant.current_reaction]);

  const getConnectionIcon = () => {
    switch (connectionQuality) {
      case 'excellent': return SignalWifi4Bar;
      case 'good': return SignalWifi3Bar;
      case 'fair': return SignalWifi2Bar;
      case 'poor': return SignalWifi1Bar;
      default: return SignalWifiOff;
    }
  };

  const getConnectionColor = () => {
    switch (connectionQuality) {
      case 'excellent': return theme.palette.success.main;
      case 'good': return theme.palette.info.main;
      case 'fair': return theme.palette.warning.main;
      case 'poor': return theme.palette.error.main;
      default: return theme.palette.grey[500];
    }
  };

  const ConnectionIcon = getConnectionIcon();

  const isSpeaking = audioLevel > 0.1;
  // const isHost = participant.role === 'host' ;
  // const isCoHost = participant.role === 'co-host';
const isHost = participant.role === 'host' || participant.isHost;
const isCoHost = participant.role === 'co-host' || participant.isCoHost;
  return (
    <StyledCard
      isCurrentUser={isCurrentUser}
      isSpeaking={isSpeaking}
      isHost={isHost}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      sx={{ width: cardSize.width, height: cardSize.height }}
    >
      {/* Video Container */}
      <VideoContainer sx={{ height: cardSize.videoHeight }}>
        {participant.video_enabled && videoStream ? (
          <VideoElement
            ref={videoRef}
            autoPlay
            muted={isCurrentUser}
            playsInline
          />
        ) : (
          <Box
            display="flex"
            flexDirection="column"
            alignItems="center"
            justifyContent="center"
            height="100%"
          >
            <Avatar
              src={participant.profile_picture}
              sx={{
                width: cardSize.videoHeight * 0.4,
                height: cardSize.videoHeight * 0.4,
                fontSize: cardSize.videoHeight * 0.15,
                fontWeight: 600,
                background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                border: `3px solid ${theme.palette.background.paper}`,
              }}
            >
              {participant.full_name?.charAt(0)}
            </Avatar>
          </Box>
        )}

        {/* Status Overlay */}
        <StatusOverlay>
          {/* Connection Quality */}
          <Tooltip title={`Connection: ${connectionQuality}`}>
            <ConnectionIcon 
              sx={{ 
                fontSize: 16, 
                color: getConnectionColor(),
                filter: 'drop-shadow(0 1px 2px rgba(0,0,0,0.3))'
              }} 
            />
          </Tooltip>

          {/* Role Badge */}
          {(isHost || isCoHost) && (
            <Tooltip title={isHost ? 'Host' : 'Co-Host'}>
              <Box
                sx={{
                  backgroundColor: alpha(
                    isHost ? theme.palette.error.main : theme.palette.warning.main, 
                    0.9
                  ),
                  borderRadius: '50%',
                  p: 0.5,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                }}
              >
                {isHost ? (
                  <AdminPanelSettings sx={{ fontSize: 14, color: 'white' }} />
                ) : (
                  <SupervisedUserCircle sx={{ fontSize: 14, color: 'white' }} />
                )}
              </Box>
            </Tooltip>
          )}

          {/* Hand Raised */}
          {participant.hand_raised && (
            <Zoom in={participant.hand_raised}>
              <Box
                sx={{
                  backgroundColor: alpha(theme.palette.warning.main, 0.9),
                  borderRadius: '50%',
                  p: 0.5,
                  animation: `${pulseAnimation} 1.5s infinite`,
                }}
              >
                <PanTool sx={{ fontSize: 14, color: 'white' }} />
              </Box>
            </Zoom>
          )}
        </StatusOverlay>

        {/* Reaction Overlay */}
        <Fade in={showReaction}>
          <ReactionOverlay>
            {currentReaction}
          </ReactionOverlay>
        </Fade>

        {/* Controls Overlay */}
        {showControls && (isCurrentUser || isHovered) && (
          <Fade in={isCurrentUser || isHovered}>
            <ControlsOverlay>
              <Box display="flex" gap={0.5}>
                {/* Audio Control */}
                <Tooltip title={participant.audio_enabled ? 'Mute' : 'Unmute'}>
                  <IconButton
                    size="small"
                    onClick={() => onMuteToggle && onMuteToggle(participant.id)}
                    sx={{
                      backgroundColor: participant.audio_enabled 
                        ? alpha(theme.palette.success.main, 0.2)
                        : alpha(theme.palette.error.main, 0.2),
                      color: participant.audio_enabled 
                        ? theme.palette.success.main 
                        : theme.palette.error.main,
                      '&:hover': {
                        backgroundColor: participant.audio_enabled 
                          ? alpha(theme.palette.success.main, 0.3)
                          : alpha(theme.palette.error.main, 0.3),
                      }
                    }}
                  >
                    {participant.audio_enabled ? <Mic /> : <MicOff />}
                  </IconButton>
                </Tooltip>

                {/* Video Control */}
                <Tooltip title={participant.video_enabled ? 'Turn off camera' : 'Turn on camera'}>
                  <IconButton
                    size="small"
                    onClick={() => onVideoToggle && onVideoToggle(participant.id)}
                    sx={{
                      backgroundColor: participant.video_enabled 
                        ? alpha(theme.palette.success.main, 0.2)
                        : alpha(theme.palette.error.main, 0.2),
                      color: participant.video_enabled 
                        ? theme.palette.success.main 
                        : theme.palette.error.main,
                      '&:hover': {
                        backgroundColor: participant.video_enabled 
                          ? alpha(theme.palette.success.main, 0.3)
                          : alpha(theme.palette.error.main, 0.3),
                      }
                    }}
                  >
                    {participant.video_enabled ? <Videocam /> : <VideocamOff />}
                  </IconButton>
                </Tooltip>
              </Box>

              {/* Volume Indicator */}
              {participant.audio_enabled && (
                <Box display="flex" alignItems="center" gap={0.5}>
                  <VolumeUp sx={{ fontSize: 16, color: theme.palette.text.secondary }} />
                  <Box
                    sx={{
                      width: 30,
                      height: 4,
                      backgroundColor: alpha(theme.palette.grey[500], 0.3),
                      borderRadius: 2,
                      overflow: 'hidden',
                    }}
                  >
                    <Box
                      sx={{
                        width: `${audioLevel * 100}%`,
                        height: '100%',
                        backgroundColor: isSpeaking 
                          ? theme.palette.success.main 
                          : theme.palette.info.main,
                        transition: 'width 0.1s ease',
                      }}
                    />
                  </Box>
                </Box>
              )}
            </ControlsOverlay>
          </Fade>
        )}
      </VideoContainer>

      {/* Participant Info */}
      <CardContent sx={{ p: 1.5, paddingBottom: '12px !important' }}>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Box flex={1} minWidth={0}>
            <Typography 
              variant="subtitle2" 
              fontWeight={600}
              noWrap
              sx={{ 
                color: isCurrentUser ? theme.palette.primary.main : theme.palette.text.primary 
              }}
            >
              {participant.full_name} {isCurrentUser && '(You)'}
            </Typography>
            
            <Typography 
              variant="caption" 
              color="text.secondary"
              noWrap
              sx={{ display: 'block', mt: 0.5 }}
            >
              {participant.email}
            </Typography>
          </Box>

          {/* Speaking Indicator */}
          {isSpeaking && (
            <Chip
              label="Speaking"
              size="small"
              sx={{
                backgroundColor: alpha(theme.palette.success.main, 0.1),
                color: theme.palette.success.main,
                fontSize: '0.7rem',
                height: 20,
                animation: `${pulseAnimation} 1.5s infinite`,
              }}
            />
          )}
        </Box>

        {/* Connection Status */}
        <Box display="flex" alignItems="center" justifyContent="space-between" mt={1}>
          <Chip
            label={connectionQuality}
            size="small"
            sx={{
              backgroundColor: alpha(getConnectionColor(), 0.1),
              color: getConnectionColor(),
              fontSize: '0.7rem',
              height: 18,
              textTransform: 'capitalize',
            }}
          />
          
          {/* Join Time */}
          <Typography variant="caption" color="text.secondary">
            {participant.join_time && new Date(participant.join_time).toLocaleTimeString([], {
              hour: '2-digit',
              minute: '2-digit'
            })}
          </Typography>
        </Box>
      </CardContent>
    </StyledCard>
  );
};

export default ParticipantCard;