import React, { useState, useEffect } from 'react';
import {
  Snackbar,
  Alert,
  AlertTitle,
  Slide,
  IconButton,
  Box,
  Typography,
  Avatar,
  Stack,
  Fade,
  Grow
} from '@mui/material';
import {
  Close,
  CheckCircle,
  Error,
  Warning,
  Info,
  VideoCall,
  Message,
  Person,
  Event,
  Notifications
} from '@mui/icons-material';

// Main Toast Notification Component
const NotificationToast = ({
  open,
  onClose,
  type = 'info',
  title,
  message,
  duration = 6000,
  position = { vertical: 'top', horizontal: 'right' },
  showIcon = true,
  showCloseButton = true,
  variant = 'filled',
  action,
  avatar,
  timestamp,
  transition = 'slide'
}) => {
  const [isOpen, setIsOpen] = useState(open);

  useEffect(() => {
    setIsOpen(open);
  }, [open]);

  const handleClose = (event, reason) => {
    if (reason === 'clickaway') {
      return;
    }
    setIsOpen(false);
    onClose && onClose();
  };

  const getTypeIcon = () => {
    switch (type) {
      case 'success':
        return <CheckCircle />;
      case 'error':
        return <Error />;
      case 'warning':
        return <Warning />;
      case 'info':
        return <Info />;
      case 'meeting':
        return <VideoCall />;
      case 'message':
        return <Message />;
      case 'user':
        return <Person />;
      case 'event':
        return <Event />;
      default:
        return <Notifications />;
    }
  };

  const getSeverity = () => {
    if (['success', 'error', 'warning', 'info'].includes(type)) {
      return type;
    }
    return 'info';
  };

  const TransitionComponent = (props) => {
    switch (transition) {
      case 'fade':
        return <Fade {...props} />;
      case 'grow':
        return <Grow {...props} />;
      default:
        return <Slide {...props} direction="left" />;
    }
  };

  const renderCustomContent = () => {
    if (['meeting', 'message', 'user', 'event'].includes(type)) {
      return (
        <Alert
          severity="info"
          variant={variant}
          icon={showIcon ? getTypeIcon() : false}
          action={
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              {action}
              {showCloseButton && (
                <IconButton
                  size="small"
                  color="inherit"
                  onClick={handleClose}
                >
                  <Close fontSize="small" />
                </IconButton>
              )}
            </Box>
          }
          sx={{
            minWidth: 350,
            maxWidth: 500,
            '& .MuiAlert-message': {
              width: '100%'
            }
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 2 }}>
            {avatar && (
              <Avatar
                src={avatar}
                sx={{ width: 40, height: 40, mt: 0.5 }}
              >
                {title?.charAt(0)}
              </Avatar>
            )}
            
            <Box sx={{ flex: 1 }}>
              {title && (
                <AlertTitle sx={{ mb: 0.5, fontWeight: 600 }}>
                  {title}
                </AlertTitle>
              )}
              
              <Typography variant="body2" sx={{ mb: timestamp ? 1 : 0 }}>
                {message}
              </Typography>
              
              {timestamp && (
                <Typography variant="caption" color="text.secondary">
                  {new Date(timestamp).toLocaleTimeString()}
                </Typography>
              )}
            </Box>
          </Box>
        </Alert>
      );
    }

    return (
      <Alert
        severity={getSeverity()}
        variant={variant}
        icon={showIcon ? getTypeIcon() : false}
        action={
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {action}
            {showCloseButton && (
              <IconButton
                size="small"
                color="inherit"
                onClick={handleClose}
              >
                <Close fontSize="small" />
              </IconButton>
            )}
          </Box>
        }
        sx={{
          minWidth: 300,
          maxWidth: 500
        }}
      >
        {title && <AlertTitle>{title}</AlertTitle>}
        {message}
      </Alert>
    );
  };

  return (
    <Snackbar
      open={isOpen}
      autoHideDuration={duration}
      onClose={handleClose}
      anchorOrigin={position}
      TransitionComponent={TransitionComponent}
      sx={{
        '& .MuiSnackbar-root': {
          top: position.vertical === 'top' ? 88 : undefined, // Account for header
        }
      }}
    >
      {renderCustomContent()}
    </Snackbar>
  );
};

// Toast Container for Multiple Notifications
export const ToastContainer = ({ toasts = [], onRemove }) => {
  return (
    <Stack
      spacing={1}
      sx={{
        position: 'fixed',
        top: 88,
        right: 16,
        zIndex: 9999,
        maxWidth: 500,
        pointerEvents: 'none',
        '& > *': {
          pointerEvents: 'auto'
        }
      }}
    >
      {toasts.map((toast) => (
        <NotificationToast
          key={toast.id}
          open={true}
          onClose={() => onRemove(toast.id)}
          {...toast}
        />
      ))}
    </Stack>
  );
};

// Meeting-specific Toast
export const MeetingToast = ({
  open,
  onClose,
  type = 'userJoined',
  userName,
  userAvatar,
  meetingName,
  action
}) => {
  const getTitle = () => {
    switch (type) {
      case 'userJoined':
        return `${userName} joined`;
      case 'userLeft':
        return `${userName} left`;
      case 'meetingStarted':
        return 'Meeting started';
      case 'meetingEnded':
        return 'Meeting ended';
      case 'recordingStarted':
        return 'Recording started';
      case 'recordingStopped':
        return 'Recording stopped';
      default:
        return 'Meeting notification';
    }
  };

  const getMessage = () => {
    switch (type) {
      case 'userJoined':
        return `${userName} has joined the meeting`;
      case 'userLeft':
        return `${userName} has left the meeting`;
      case 'meetingStarted':
        return `"${meetingName}" is now live`;
      case 'meetingEnded':
        return `"${meetingName}" has ended`;
      case 'recordingStarted':
        return 'This meeting is now being recorded';
      case 'recordingStopped':
        return 'Recording has been stopped';
      default:
        return 'Meeting update';
    }
  };

  return (
    <NotificationToast
      open={open}
      onClose={onClose}
      type="meeting"
      title={getTitle()}
      message={getMessage()}
      avatar={userAvatar}
      action={action}
      duration={4000}
      transition="slide"
    />
  );
};

// Chat Message Toast
export const ChatToast = ({
  open,
  onClose,
  senderName,
  senderAvatar,
  message,
  timestamp,
  onClick
}) => {
  return (
    <NotificationToast
      open={open}
      onClose={onClose}
      type="message"
      title={`New message from ${senderName}`}
      message={message}
      avatar={senderAvatar}
      timestamp={timestamp}
      action={
        onClick && (
          <IconButton size="small" onClick={onClick}>
            <Message fontSize="small" />
          </IconButton>
        )
      }
      duration={5000}
    />
  );
};

// System Notification Toast
export const SystemToast = ({
  open,
  onClose,
  type = 'info',
  title,
  message,
  persistent = false,
  action
}) => {
  return (
    <NotificationToast
      open={open}
      onClose={onClose}
      type={type}
      title={title}
      message={message}
      duration={persistent ? null : 6000}
      action={action}
      variant="filled"
    />
  );
};

// Toast Hook for Easy Usage
export const useToast = () => {
  const [toasts, setToasts] = useState([]);

  const addToast = (toast) => {
    const id = Date.now() + Math.random();
    setToasts(prev => [...prev, { ...toast, id }]);
  };

  const removeToast = (id) => {
    setToasts(prev => prev.filter(toast => toast.id !== id));
  };

  const showSuccess = (message, title = 'Success') => {
    addToast({ type: 'success', title, message });
  };

  const showError = (message, title = 'Error') => {
    addToast({ type: 'error', title, message });
  };

  const showWarning = (message, title = 'Warning') => {
    addToast({ type: 'warning', title, message });
  };

  const showInfo = (message, title = 'Info') => {
    addToast({ type: 'info', title, message });
  };

  const showMeeting = (type, userName, options = {}) => {
    addToast({
      type: 'meeting',
      meetingType: type,
      userName,
      ...options
    });
  };

  const showChat = (senderName, message, options = {}) => {
    addToast({
      type: 'message',
      senderName,
      message,
      ...options
    });
  };

  return {
    toasts,
    removeToast,
    showSuccess,
    showError,
    showWarning,
    showInfo,
    showMeeting,
    showChat,
    addToast
  };
};

export default NotificationToast;