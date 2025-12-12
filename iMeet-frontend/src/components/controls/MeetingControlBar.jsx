// src/components/controls/MeetingControlBar.jsx - COMPLETE CONTROL BAR
import React from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Badge,
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
  Visibility,
  VisibilityOff,
} from '@mui/icons-material';

const MeetingControlBar = ({
  // Audio/Video states
  audioEnabled,
  videoEnabled,
  screenSharing,
  isScreenSharing,
  enhancedScreenShareData, // ✅ ADD THIS
  currentUser,              // ✅ ADD THIS
  // hasHostPrivileges,        // (should already exist below)
  // Connection state
  isConnected,

  // Panel states
  chatOpen,
  participantsOpen,
  reactionsOpen,
  handRaiseOpen,
  showToggleMenu,
  attendanceMinimized,

  // Handlers
  onToggleAudio,
  onToggleVideo,
  onToggleScreenShare,
  onToggleChat,
  onToggleParticipants,
  onToggleReactions,
  onToggleHandRaise,
  onToggleMenu,
  onToggleAttendance,
  onLeaveMeeting,

  // Settings
  meetingSettings,

  // Counts
  participantCount,
  chatUnreadCount,
  pendingHandsCount,

  // User state
  isHandRaised,
  hasHostPrivileges,
  attendanceEnabled,
  currentAttendanceData,
}) => {
  return (
    <Box
      sx={{
        position: 'absolute',
        bottom: 0,
        left: 0,
        right: 0,
        padding: { xs: '12px 16px 20px', md: '16px 24px 24px' },
        zIndex: 1000,
        marginRight: chatOpen || participantsOpen ? { xs: 0, lg: '428px' } : 0,
        transition: 'margin-right 0.7s cubic-bezier(0.22, 0.61, 0.36, 1)',
        willChange: 'margin-right',
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: { xs: 1, sm: 1 },
          maxWidth: 800,
          height: "90px",
          margin: '0 auto',
          padding: { xs: '16px 20px', sm: '18px 24px', md: '20px 28px' },
          background: 'rgba(32, 33, 36, 0.8)',
          backdropFilter: 'blur(10px)',
          borderRadius: '29px',
          overflow: 'visible',
          position: 'relative',
          transition: 'all 0.5s cubic-bezier(0.22, 0.61, 0.36, 1)',
        }}
      >
        {/* Audio Control */}
        <Tooltip title={audioEnabled ? "Mute" : "Unmute"}>
          <IconButton
            onClick={onToggleAudio}
            disabled={!isConnected}
            sx={{
              width: { xs: 44, sm: 48, md: 52 },
              height: { xs: 44, sm: 48, md: 52 },
              background: !audioEnabled ? '#ea4335' : '#3a3b3c',
              color: '#ffffff',
              borderRadius: '50%',
              transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
              border: 'none',
              flexShrink: 0,
              position: 'relative',
              zIndex: 1,

              '&:hover': {
                background: !audioEnabled ? '#f28b82' : '#444648',
                transform: 'scale(1.08)',
                zIndex: 2,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
              },

              '&:disabled': {
                opacity: 0.5,
                transition: 'opacity 0.3s ease',
              },
            }}
          >
            {audioEnabled ? <Mic /> : <MicOff />}
          </IconButton>
        </Tooltip>

        {/* Video Control */}
        <Tooltip title={videoEnabled ? "Turn off camera" : "Turn on camera"}>
          <IconButton
            onClick={onToggleVideo}
            disabled={!isConnected}
            sx={{
              width: { xs: 44, sm: 48, md: 52 },
              height: { xs: 44, sm: 48, md: 52 },
              background: !videoEnabled ? '#ea4335' : '#3a3b3c',
              color: '#ffffff',
              borderRadius: '50%',
              transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
              border: 'none',
              flexShrink: 0,
              position: 'relative',
              zIndex: 1,

              '&:hover': {
                background: !videoEnabled ? '#f28b82' : '#444648',
                transform: 'scale(1.08)',
                zIndex: 2,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
              },

              '&:disabled': {
                opacity: 0.5,
                transition: 'opacity 0.3s ease',
              },
            }}
          >
            {videoEnabled ? <Videocam /> : <VideocamOff />}
          </IconButton>
        </Tooltip>

        {/* Screen Share Control */}
        {/* Screen Share Control */}
        <Tooltip
          title={
            (() => {
              const { stream: activeScreenShare, sharer: activeSharer } = enhancedScreenShareData || {};

              if (activeScreenShare && activeSharer) {
                const isCurrentUserSharing = activeSharer.isLocal ||
                  activeSharer.user_id?.toString() === currentUser?.id?.toString();

                if (isCurrentUserSharing) {
                  return "Stop sharing";
                } else if (hasHostPrivileges) {
                  return `Stop ${activeSharer.name || 'participant'}'s screen share`;
                } else {
                  return `${activeSharer.name || 'Someone'} is sharing`;
                }
              }

              return "Share screen";
            })()
          }
        >
          <IconButton
            onClick={onToggleScreenShare}
            disabled={
              !isConnected ||
              (() => {
                const { stream: activeScreenShare, sharer: activeSharer } = enhancedScreenShareData || {};

                // ✅ Disable button if someone else is sharing AND user is not host/co-host
                if (activeScreenShare && activeSharer) {
                  const isCurrentUserSharing = activeSharer.isLocal ||
                    activeSharer.user_id?.toString() === currentUser?.id?.toString();

                  // Disable if someone else is sharing and user has no host privileges
                  return !isCurrentUserSharing && !hasHostPrivileges;
                }

                return false;
              })()
            }
            sx={{
              width: { xs: 44, sm: 48, md: 52 },
              height: { xs: 44, sm: 48, md: 52 },
              background: (screenSharing || isScreenSharing) ? '#8ab4f8' : '#3a3b3c',
              color: (screenSharing || isScreenSharing) ? '#202124' : '#ffffff',
              borderRadius: '50%',
              transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
              border: 'none',
              flexShrink: 0,
              position: 'relative',
              zIndex: 1,

              '&:hover': {
                background: (screenSharing || isScreenSharing) ? '#a8c7fa' : '#444648',
                transform: 'scale(1.08)',
                zIndex: 2,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
              },

              '&:disabled': {
                opacity: 0.35, // ✅ Clearer visual feedback when disabled
                cursor: 'not-allowed',
                transition: 'opacity 0.3s ease',
              },
            }}
          >
            {screenSharing || isScreenSharing ? <StopScreenShare /> : <ScreenShare />}
          </IconButton>
        </Tooltip>

        {/* Separator */}
        <Box sx={{
          width: '1px',
          height: 32,
          backgroundColor: 'rgba(255, 255, 255, 0.2)',
          margin: { xs: '0 4px', sm: '0 8px' },
          display: { xs: 'none', sm: 'block' },
          flexShrink: 0,
          transition: 'all 0.3s ease',
        }} />

        {/* Reactions */}
        {meetingSettings?.reactionsEnabled && (
          <Tooltip title="Send reaction">
            <IconButton
              onClick={onToggleReactions}
              disabled={!isConnected}
              sx={{
                width: { xs: 40, sm: 44, md: 48 },
                height: { xs: 40, sm: 44, md: 48 },
                background: reactionsOpen ? '#8ab4f8' : '#3a3b3c',
                color: reactionsOpen ? '#202124' : '#e8eaed',
                borderRadius: '50%',
                transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
                border: 'none',
                flexShrink: 0,
                position: 'relative',
                zIndex: 1,
                '&:hover': {
                  background: reactionsOpen ? '#a8c7fa' : '#444648',
                  transform: 'scale(1.08)',
                  zIndex: 2,
                  boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
                },
                '&:disabled': {
                  opacity: 0.5,
                  transition: 'opacity 0.3s ease',
                },
              }}
            >
              <EmojiEmotions />
            </IconButton>
          </Tooltip>
        )}

        {/* Hand Raise */}
        {meetingSettings?.handRaiseEnabled && (
          <Tooltip
            title={
              hasHostPrivileges
                ? `View raised hands${pendingHandsCount > 0 ? ` (${pendingHandsCount} pending)` : ""}`
                : isHandRaised
                  ? "Lower hand"
                  : "Raise hand"
            }
          >
            <IconButton
              onClick={onToggleHandRaise}
              disabled={!isConnected}
              sx={{
                width: { xs: 40, sm: 44, md: 48 },
                height: { xs: 40, sm: 44, md: 48 },
                background: (isHandRaised || handRaiseOpen) ? '#8ab4f8' : '#3a3b3c',
                color: (isHandRaised || handRaiseOpen) ? '#202124' : '#e8eaed',
                borderRadius: '50%',
                transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
                border: 'none',
                flexShrink: 0,
                position: 'relative',
                zIndex: 1,

                '&:hover': {
                  background: (isHandRaised || handRaiseOpen) ? '#a8c7fa' : '#444648',
                  transform: 'scale(1.08)',
                  zIndex: 2,
                  boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
                },

                '&:disabled': {
                  opacity: 0.5,
                  transition: 'opacity 0.3s ease',
                },
              }}
            >
              <Badge
                badgeContent={
                  hasHostPrivileges
                    ? pendingHandsCount > 0
                      ? pendingHandsCount
                      : 0
                    : isHandRaised
                      ? 1
                      : 0
                }
                color={hasHostPrivileges ? "warning" : "primary"}
                sx={{
                  '& .MuiBadge-badge': {
                    backgroundColor: hasHostPrivileges ? '#f59e0b' : '#8ab4f8',
                    color: 'white',
                    fontSize: '0.65rem',
                    minWidth: 16,
                    height: 16,
                    transition: 'all 0.3s cubic-bezier(0.22, 0.61, 0.36, 1)',
                  }
                }}
              >
                <PanTool />
              </Badge>

              {hasHostPrivileges && pendingHandsCount > 0 && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: -2,
                    right: -2,
                    width: 10,
                    height: 10,
                    borderRadius: '50%',
                    backgroundColor: '#f59e0b',
                    animation: 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                    '@keyframes pulse': {
                      '0%': { opacity: 1, transform: 'scale(1)' },
                      '50%': { opacity: 0.5, transform: 'scale(1.2)' },
                      '100%': { opacity: 1, transform: 'scale(1)' },
                    },
                  }}
                />
              )}
            </IconButton>
          </Tooltip>
        )}

        {/* Chat */}
        {meetingSettings?.chatEnabled && (
          <Tooltip title={`Toggle chat${chatUnreadCount > 0 ? ` (${chatUnreadCount} unread)` : ""}`}>
            <IconButton
              onClick={onToggleChat}
              sx={{
                width: { xs: 40, sm: 44, md: 48 },
                height: { xs: 40, sm: 44, md: 48 },
                background: chatOpen ? '#8ab4f8' : '#3a3b3c',
                color: chatOpen ? '#202124' : '#e8eaed',
                borderRadius: '50%',
                transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
                border: 'none',
                flexShrink: 0,
                position: 'relative',
                zIndex: 1,

                '&:hover': {
                  background: chatOpen ? '#a8c7fa' : '#444648',
                  transform: 'scale(1.08)',
                  zIndex: 2,
                  boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
                },
              }}
            >
              <Badge
                badgeContent={chatOpen ? 0 : chatUnreadCount}
                color="error"
                sx={{
                  '& .MuiBadge-badge': {
                    backgroundColor: '#ea4335',
                    fontSize: '0.65rem',
                    minWidth: 16,
                    height: 16,
                    transition: 'all 0.3s cubic-bezier(0.22, 0.61, 0.36, 1)',
                  }
                }}
              >
                <Chat />
              </Badge>
            </IconButton>
          </Tooltip>
        )}

        {/* Attendance Toggle */}
        {attendanceEnabled && (
          <Tooltip title={attendanceMinimized ? "Expand attendance tracker" : "Minimize attendance tracker"}>
            <IconButton
              onClick={onToggleAttendance}
              sx={{
                width: { xs: 40, sm: 44, md: 48 },
                height: { xs: 40, sm: 44, md: 48 },
                background: attendanceMinimized ? '#3a3b3c' : '#34a853',
                color: attendanceMinimized ? '#e8eaed' : '#202124',
                borderRadius: '50%',
                transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
                border: 'none',
                flexShrink: 0,
                position: 'relative',
                zIndex: 1,

                '&:hover': {
                  background: attendanceMinimized ? '#444648' : '#5bb974',
                  transform: 'scale(1.08)',
                  zIndex: 2,
                  boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
                },
              }}
            >
              {attendanceMinimized ? <VisibilityOff /> : <Visibility />}

              {/* Status indicator dot */}
              {attendanceEnabled && !attendanceMinimized && currentAttendanceData?.sessionActive && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: 2,
                    right: 2,
                    width: 10,
                    height: 10,
                    borderRadius: '50%',
                    backgroundColor: currentAttendanceData.attendancePercentage > 80 ? '#22c55e' :
                      currentAttendanceData.attendancePercentage > 60 ? '#f59e0b' : '#ef4444',
                    border: '2px solid rgba(32, 33, 36, 0.95)',
                    animation: currentAttendanceData.violations?.length > 0 ? 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite' : 'none',
                    transition: 'background-color 0.5s cubic-bezier(0.22, 0.61, 0.36, 1)',
                  }}
                />
              )}
            </IconButton>
          </Tooltip>
        )}

        {/* Participants */}
        <Tooltip title={`${participantsOpen ? "Close" : "Open"} participants`}>
          <IconButton
            onClick={onToggleParticipants}
            sx={{
              width: { xs: 40, sm: 44, md: 48 },
              height: { xs: 40, sm: 44, md: 48 },
              background: participantsOpen ? '#8ab4f8' : '#3a3b3c',
              color: participantsOpen ? '#202124' : '#e8eaed',
              borderRadius: '50%',
              transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
              border: 'none',
              flexShrink: 0,
              position: 'relative',
              zIndex: 1,

              '&:hover': {
                background: participantsOpen ? '#a8c7fa' : '#444648',
                transform: 'scale(1.08)',
                zIndex: 2,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
              },
            }}
          >
            <Badge
              badgeContent={participantCount}
              color="primary"
              sx={{
                '& .MuiBadge-badge': {
                  backgroundColor: '#8ab4f8',
                  color: '#202124',
                  fontSize: '0.65rem',
                  minWidth: 16,
                  height: 16,
                  fontWeight: 600,
                  transition: 'all 0.3s cubic-bezier(0.22, 0.61, 0.36, 1)',
                }
              }}
            >
              <People />
            </Badge>
          </IconButton>
        </Tooltip>

        {/* More Options */}
        <Tooltip title="More options">
          <IconButton
            onClick={onToggleMenu}
            sx={{
              width: { xs: 40, sm: 44, md: 48 },
              height: { xs: 40, sm: 44, md: 48 },
              background: showToggleMenu ? '#8ab4f8' : '#3a3b3c',
              color: showToggleMenu ? '#202124' : '#e8eaed',
              borderRadius: '50%',
              transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
              border: 'none',
              flexShrink: 0,
              position: 'relative',
              zIndex: 1,

              '&:hover': {
                background: showToggleMenu ? '#a8c7fa' : '#444648',
                transform: 'scale(1.08)',
                zIndex: 2,
                boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
              },
            }}
          >
            <MoreVert />
          </IconButton>
        </Tooltip>

        {/* Separator */}
        <Box sx={{
          width: '1px',
          height: 32,
          backgroundColor: 'rgba(255, 255, 255, 0.2)',
          margin: { xs: '0 4px', sm: '0 8px' },
          display: { xs: 'none', sm: 'block' },
          flexShrink: 0,
          transition: 'all 0.3s ease',
        }} />

        {/* Leave Meeting */}
        <Tooltip title="Leave meeting">
          <IconButton
            onClick={onLeaveMeeting}
            sx={{
              width: { xs: 48, sm: 52, md: 56 },
              height: { xs: 48, sm: 52, md: 56 },
              background: '#ea4335',
              color: '#ffffff',
              borderRadius: '50%',
              transition: 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)',
              border: 'none',
              marginLeft: { xs: 0.5, sm: 1 },
              flexShrink: 0,
              position: 'relative',
              zIndex: 1,

              '&:hover': {
                background: '#f28b82',
                transform: 'scale(1.08)',
                zIndex: 2,
                boxShadow: '0 12px 32px rgba(234, 67, 53, 0.5)',
              },
            }}
          >
            <CallEnd />
          </IconButton>
        </Tooltip>
      </Box>
    </Box>
  );
};

export default MeetingControlBar;