import React, { useState, useEffect } from 'react';
import {
  Snackbar,
  Alert,
  Avatar,
  Box,
  Typography,
  IconButton,
  Slide,
  Fade,
  Stack,
  Chip,
  Card,
  CardContent
} from '@mui/material';
import {
  Close as CloseIcon,
  PanTool as HandIcon,
  Chat as ChatIcon,
  Videocam as VideoIcon,
  Mic as MicIcon,
  ScreenShare as ScreenShareIcon,
  PersonAdd as JoinIcon,
  ExitToApp as LeaveIcon,
  EmojiEmotions as ReactionIcon,
  RecordVoiceOver as UnmuteIcon,
  MicOff as MuteIcon
} from '@mui/icons-material';

const InteractionNotifications = ({ 
  notifications = [], 
  onDismiss,
  position = { vertical: 'top', horizontal: 'right' },
  autoHideDuration = 5000 
}) => {
  const [visibleNotifications, setVisibleNotifications] = useState([]);

  useEffect(() => {
    setVisibleNotifications(notifications.slice(-3)); // Show max 3 notifications
  }, [notifications]);

  const getNotificationIcon = (type) => {
    const iconProps = { fontSize: 'small', sx: { mr: 1 } };
    
    switch (type) {
      case 'hand_raised': return <HandIcon {...iconProps} color="warning" />;
      case 'hand_accepted': return <HandIcon {...iconProps} color="success" />;
      case 'hand_denied': return <HandIcon {...iconProps} color="error" />;
      case 'user_joined': return <JoinIcon {...iconProps} color="success" />;
      case 'user_left': return <LeaveIcon {...iconProps} color="info" />;
      case 'user_muted': return <MuteIcon {...iconProps} color="warning" />;
      case 'user_unmuted': return <UnmuteIcon {...iconProps} color="success" />;
      case 'screen_share_started': return <ScreenShareIcon {...iconProps} color="primary" />;
      case 'screen_share_stopped': return <ScreenShareIcon {...iconProps} color="info" />;
      case 'reaction_sent': return <ReactionIcon {...iconProps} color="primary" />;
      case 'chat_message': return <ChatIcon {...iconProps} color="info" />;
      case 'recording_started': return <VideoIcon {...iconProps} color="error" />;
      case 'recording_stopped': return <VideoIcon {...iconProps} color="info" />;
      default: return <ChatIcon {...iconProps} />;
    }
  };

  const getNotificationSeverity = (type) => {
    switch (type) {
      case 'user_joined':
      case 'hand_accepted':
      case 'user_unmuted':
        return 'success';
      case 'hand_raised':
      case 'user_muted':
      case 'recording_started':
        return 'warning';
      case 'hand_denied':
      case 'user_left':
        return 'error';
      default:
        return 'info';
    }
  };

  const formatNotificationMessage = (notification) => {
    const { type, user, data } = notification;
    const userName = user?.full_name || 'Someone';

    switch (type) {
      case 'hand_raised':
        return `${userName} raised their hand`;
      case 'hand_accepted':
        return `${userName}'s hand was accepted`;
      case 'hand_denied':
        return `${userName}'s hand was denied`;
      case 'user_joined':
        return `${userName} joined the meeting`;
      case 'user_left':
        return `${userName} left the meeting`;
      case 'user_muted':
        return `${userName} was muted`;
      case 'user_unmuted':
        return `${userName} was unmuted`;
      case 'screen_share_started':
        return `${userName} started sharing screen`;
      case 'screen_share_stopped':
        return `${userName} stopped sharing screen`;
      case 'reaction_sent':
        return `${userName} reacted with ${data?.reaction || '👍'}`;
      case 'chat_message':
        return `${userName}: ${data?.message?.substring(0, 30)}${data?.message?.length > 30 ? '...' : ''}`;
      case 'recording_started':
        return `Recording started by ${userName}`;
      case 'recording_stopped':
        return `Recording stopped by ${userName}`;
      default:
        return `${userName} performed an action`;
    }
  };

  const handleClose = (notificationId) => {
    onDismiss?.(notificationId);
  };

  const SlideTransition = (props) => {
    return <Slide {...props} direction="left" />;
  };

  return (
    <Box
      sx={{
        position: 'fixed',
        top: 24,
        right: 24,
        zIndex: 9999,
        width: 320,
        maxHeight: '80vh',
        overflow: 'hidden'
      }}
    >
      <Stack spacing={1}>
        {visibleNotifications.map((notification, index) => (
          <Fade
            in={true}
            timeout={300}
            style={{ transitionDelay: `${index * 100}ms` }}
            key={notification.id}
          >
            <Card
              elevation={8}
              sx={{
                background: `linear-gradient(135deg, 
                  ${getNotificationSeverity(notification.type) === 'success' ? '#4caf50, #66bb6a' :
                  getNotificationSeverity(notification.type) === 'warning' ? '#ff9800, #ffb74d' :
                  getNotificationSeverity(notification.type) === 'error' ? '#f44336, #ef5350' :
                  '#2196f3, #42a5f5'})`,
                color: 'white',
                borderRadius: 2,
                position: 'relative',
                overflow: 'hidden',
                animation: 'slideInRight 0.3s ease-out',
                '@keyframes slideInRight': {
                  from: { transform: 'translateX(100%)', opacity: 0 },
                  to: { transform: 'translateX(0)', opacity: 1 }
                },
                '&:hover': {
                  transform: 'translateY(-2px)',
                  transition: 'transform 0.2s ease'
                }
              }}
            >
              <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
                  {notification.user?.profile_picture ? (
                    <Avatar
                      src={notification.user.profile_picture}
                      sx={{ width: 32, height: 32, border: '2px solid rgba(255,255,255,0.3)' }}
                    />
                  ) : (
                    <Avatar sx={{ 
                      width: 32, 
                      height: 32, 
                      bgcolor: 'rgba(255,255,255,0.2)',
                      border: '2px solid rgba(255,255,255,0.3)'
                    }}>
                      {notification.user?.full_name?.charAt(0) || '?'}
                    </Avatar>
                  )}

                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 0.5 }}>
                      {getNotificationIcon(notification.type)}
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        {formatNotificationMessage(notification)}
                      </Typography>
                    </Box>

                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography variant="caption" sx={{ opacity: 0.8 }}>
                        {new Date(notification.timestamp).toLocaleTimeString([], {
                          hour: '2-digit',
                          minute: '2-digit'
                        })}
                      </Typography>
                      
                      {notification.data?.priority && (
                        <Chip
                          size="small"
                          label={notification.data.priority}
                          variant="outlined"
                          sx={{
                            height: 16,
                            fontSize: '0.6rem',
                            color: 'white',
                            borderColor: 'rgba(255,255,255,0.5)'
                          }}
                        />
                      )}
                    </Box>
                  </Box>

                  <IconButton
                    size="small"
                    onClick={() => handleClose(notification.id)}
                    sx={{
                      color: 'rgba(255,255,255,0.8)',
                      '&:hover': {
                        bgcolor: 'rgba(255,255,255,0.1)',
                        color: 'white'
                      }
                    }}
                  >
                    <CloseIcon fontSize="small" />
                  </IconButton>
                </Box>

                {/* Progress bar for auto-dismiss */}
                <Box
                  sx={{
                    position: 'absolute',
                    bottom: 0,
                    left: 0,
                    height: 2,
                    bgcolor: 'rgba(255,255,255,0.3)',
                    width: '100%',
                    '&::after': {
                      content: '""',
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      height: '100%',
                      bgcolor: 'rgba(255,255,255,0.8)',
                      animation: `progress ${autoHideDuration}ms linear`,
                      width: 0
                    },
                    '@keyframes progress': {
                      from: { width: '100%' },
                      to: { width: 0 }
                    }
                  }}
                />
              </CardContent>
            </Card>
          </Fade>
        ))}
      </Stack>

      {/* Toast-style notifications for quick actions */}
      {visibleNotifications.map((notification) => (
        <Snackbar
          key={`toast-${notification.id}`}
          open={notification.type === 'hand_raised' && notification.showToast}
          autoHideDuration={autoHideDuration}
          onClose={() => handleClose(notification.id)}
          TransitionComponent={SlideTransition}
          anchorOrigin={position}
        >
          <Alert
            onClose={() => handleClose(notification.id)}
            severity={getNotificationSeverity(notification.type)}
            variant="filled"
            sx={{
              boxShadow: 4,
              '& .MuiAlert-icon': {
                alignItems: 'center'
              }
            }}
            action={
              notification.type === 'hand_raised' && (
                <Box sx={{ display: 'flex', gap: 0.5 }}>
                  <IconButton
                    size="small"
                    aria-label="accept"
                    color="inherit"
                    onClick={() => {
                      // Handle accept action
                      handleClose(notification.id);
                    }}
                  >
                    ✓
                  </IconButton>
                  <IconButton
                    size="small"
                    aria-label="deny"
                    color="inherit"
                    onClick={() => {
                      // Handle deny action
                      handleClose(notification.id);
                    }}
                  >
                    ✗
                  </IconButton>
                </Box>
              )
            }
          >
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              {notification.user && (
                <Avatar
                  src={notification.user.profile_picture}
                  sx={{ width: 20, height: 20, mr: 1 }}
                >
                  {notification.user.full_name?.charAt(0)}
                </Avatar>
              )}
              {formatNotificationMessage(notification)}
            </Box>
          </Alert>
        </Snackbar>
      ))}
    </Box>
  );
};

export default InteractionNotifications;