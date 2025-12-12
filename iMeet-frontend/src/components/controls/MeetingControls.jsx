// src/components/controls/MeetingControls.jsx
import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Paper,
  Stack,
  Badge,
  Chip,
  Fade,
  useTheme
} from '@mui/material';
import {
  Mic,
  MicOff,
  Videocam,
  VideocamOff,
  ScreenShare,
  StopScreenShare,
  Chat,
  People,
  MoreVert,
  CallEnd,
  EmojiEmotions,
  PanTool,
  FiberManualRecord,
  Stop
} from '@mui/icons-material';

const MeetingControls = ({ 
  isMuted, 
  isVideoOff, 
  isScreenSharing, 
  isChatOpen,
  isParticipantsOpen,
  isRecording,
  isHost,
  onToggleMute,
  onToggleVideo,
  onToggleScreenShare,
  onToggleChat,
  onToggleParticipants,
  onToggleReactions,
  onRaiseHand,
  onToggleRecording,
  onLeaveMeeting,
  chatNotifications = 0,
  handRaised = false
}) => {
  const theme = useTheme();
  const [showMoreOptions, setShowMoreOptions] = useState(false);

  const ControlButton = ({ 
    icon, 
    onClick, 
    active = false, 
    danger = false, 
    tooltip, 
    badge = 0,
    disabled = false 
  }) => (
    <Tooltip title={tooltip} arrow>
      <span>
        <IconButton
          onClick={onClick}
          disabled={disabled}
          sx={{
            width: 56,
            height: 56,
            bgcolor: active 
              ? (danger ? 'error.main' : 'primary.main')
              : 'background.paper',
            color: active 
              ? 'white' 
              : (danger ? 'error.main' : 'text.primary'),
            border: `2px solid ${active 
              ? (danger ? theme.palette.error.main : theme.palette.primary.main)
              : 'transparent'}`,
            boxShadow: theme.shadows[3],
            transition: 'all 0.3s ease',
            '&:hover': {
              bgcolor: active 
                ? (danger ? 'error.dark' : 'primary.dark')
                : 'action.hover',
              transform: 'scale(1.05)',
              boxShadow: theme.shadows[6]
            },
            '&:disabled': {
              opacity: 0.5,
              cursor: 'not-allowed'
            }
          }}
        >
          <Badge badgeContent={badge} color="error" max={99}>
            {icon}
          </Badge>
        </IconButton>
      </span>
    </Tooltip>
  );

  return (
    <Fade in timeout={500}>
      <Paper
        elevation={8}
        sx={{
          position: 'fixed',
          bottom: 32,
          left: '50%',
          transform: 'translateX(-50%)',
          bgcolor: 'background.paper',
          backdropFilter: 'blur(20px)',
          borderRadius: 4,
          p: 2,
          zIndex: 1000,
          border: `1px solid ${theme.palette.divider}`,
          background: `linear-gradient(145deg, ${theme.palette.background.paper}95, ${theme.palette.background.default}95)`
        }}
      >
        <Stack direction="row" spacing={2} alignItems="center">
          {/* Audio Control */}
          <ControlButton
            icon={isMuted ? <MicOff /> : <Mic />}
            onClick={onToggleMute}
            active={!isMuted}
            danger={isMuted}
            tooltip={isMuted ? 'Unmute' : 'Mute'}
          />

          {/* Video Control */}
          <ControlButton
            icon={isVideoOff ? <VideocamOff /> : <Videocam />}
            onClick={onToggleVideo}
            active={!isVideoOff}
            danger={isVideoOff}
            tooltip={isVideoOff ? 'Turn on camera' : 'Turn off camera'}
          />

          {/* Screen Share */}
          <ControlButton
            icon={isScreenSharing ? <StopScreenShare /> : <ScreenShare />}
            onClick={onToggleScreenShare}
            active={isScreenSharing}
            tooltip={isScreenSharing ? 'Stop sharing' : 'Share screen'}
          />

          {/* Reactions */}
          <ControlButton
            icon={<EmojiEmotions />}
            onClick={onToggleReactions}
            tooltip="Reactions"
          />

          {/* Raise Hand */}
          <ControlButton
            icon={<PanTool />}
            onClick={onRaiseHand}
            active={handRaised}
            tooltip={handRaised ? 'Lower hand' : 'Raise hand'}
          />

          {/* Chat */}
          <ControlButton
            icon={<Chat />}
            onClick={onToggleChat}
            active={isChatOpen}
            badge={chatNotifications}
            tooltip="Chat"
          />

          {/* Participants */}
          <ControlButton
            icon={<People />}
            onClick={onToggleParticipants}
            active={isParticipantsOpen}
            tooltip="Participants"
          />

          {/* Recording (Host only) */}
          {isHost && (
            <ControlButton
              icon={isRecording ? <Stop /> : <FiberManualRecord />}
              onClick={onToggleRecording}
              active={isRecording}
              danger={isRecording}
              tooltip={isRecording ? 'Stop recording' : 'Start recording'}
            />
          )}

          {/* Recording Indicator */}
          {isRecording && (
            <Chip
              icon={<FiberManualRecord sx={{ color: 'red', animation: 'pulse 2s infinite' }} />}
              label="REC"
              size="small"
              color="error"
              variant="outlined"
              sx={{ mx: 1 }}
            />
          )}

          {/* Leave Meeting */}
          <ControlButton
            icon={<CallEnd />}
            onClick={onLeaveMeeting}
            danger
            tooltip="Leave meeting"
          />
        </Stack>

        <style jsx>{`
          @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
          }
        `}</style>
      </Paper>
    </Fade>
  );
};

export default MeetingControls;
