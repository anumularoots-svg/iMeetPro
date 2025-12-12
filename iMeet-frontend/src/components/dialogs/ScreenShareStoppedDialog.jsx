// src/components/dialogs/ScreenShareStoppedDialog.jsx
import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Alert,
  Avatar,
} from '@mui/material';
import {
  StopScreenShare as StopIcon,
  Person as PersonIcon,
  Warning as WarningIcon,
} from '@mui/icons-material';

/**
 * Dialog shown when host stops a participant's screen share
 * Shows different content for host vs participant
 */
const ScreenShareStoppedDialog = ({
  open,
  onClose,
  stoppedBy,
  stoppedParticipant,
  isCurrentUser,
  reason = null,
}) => {
  const handleClose = () => {
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 3,
          background: 'linear-gradient(135deg, #1a1f2e 0%, #2d3748 100%)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
        },
      }}
    >
      {/* Dialog Title */}
      <DialogTitle
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          pb: 2,
          borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
        }}
      >
        <Box
          sx={{
            width: 48,
            height: 48,
            borderRadius: '50%',
            background: isCurrentUser
              ? 'linear-gradient(135deg, #f44336 0%, #d32f2f 100%)'
              : 'linear-gradient(135deg, #ff9800 0%, #f57c00 100%)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: isCurrentUser
              ? '0 4px 12px rgba(244, 67, 54, 0.4)'
              : '0 4px 12px rgba(255, 152, 0, 0.4)',
          }}
        >
          <StopIcon sx={{ color: 'white', fontSize: 28 }} />
        </Box>
        <Box sx={{ flex: 1 }}>
          <Typography variant="h6" sx={{ color: 'white', fontWeight: 600 }}>
            {isCurrentUser ? 'Screen Sharing Stopped' : 'Screen Share Stopped'}
          </Typography>
          <Typography variant="caption" sx={{ color: 'rgba(255, 255, 255, 0.6)' }}>
            {isCurrentUser ? 'By host' : 'Action taken'}
          </Typography>
        </Box>
      </DialogTitle>

      {/* Dialog Content */}
      <DialogContent sx={{ pt: 3, pb: 2 }}>
        {isCurrentUser ? (
          // Message for the participant whose screen share was stopped
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
            <Alert
              severity="warning"
              icon={<WarningIcon />}
              sx={{
                background: 'rgba(255, 152, 0, 0.1)',
                border: '1px solid rgba(255, 152, 0, 0.3)',
                color: 'white',
                '& .MuiAlert-icon': {
                  color: '#ff9800',
                },
              }}
            >
              Your screen sharing has been stopped by the host
            </Alert>

            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 2,
                p: 2,
                borderRadius: 2,
                background: 'rgba(255, 255, 255, 0.05)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
              }}
            >
              <Avatar
                sx={{
                  width: 40,
                  height: 40,
                  background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                }}
              >
                <PersonIcon />
              </Avatar>
              <Box sx={{ flex: 1 }}>
                <Typography
                  variant="body2"
                  sx={{ color: 'rgba(255, 255, 255, 0.6)', fontSize: '0.75rem' }}
                >
                  Stopped by
                </Typography>
                <Typography variant="body1" sx={{ color: 'white', fontWeight: 500 }}>
                  {stoppedBy?.name || stoppedBy?.full_name || 'Host'}
                </Typography>
              </Box>
            </Box>

            {reason && (
              <Box
                sx={{
                  p: 2,
                  borderRadius: 2,
                  background: 'rgba(255, 255, 255, 0.03)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                }}
              >
                <Typography
                  variant="caption"
                  sx={{ color: 'rgba(255, 255, 255, 0.6)', display: 'block', mb: 0.5 }}
                >
                  Reason:
                </Typography>
                <Typography variant="body2" sx={{ color: 'white' }}>
                  {reason}
                </Typography>
              </Box>
            )}

            <Typography
              variant="body2"
              sx={{
                color: 'rgba(255, 255, 255, 0.7)',
                textAlign: 'center',
                mt: 1,
              }}
            >
              You can request to share your screen again if needed.
            </Typography>
          </Box>
        ) : (
          // Message for the host who stopped the screen share
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
            <Alert
              severity="info"
              sx={{
                background: 'rgba(33, 150, 243, 0.1)',
                border: '1px solid rgba(33, 150, 243, 0.3)',
                color: 'white',
                '& .MuiAlert-icon': {
                  color: '#2196f3',
                },
              }}
            >
              You have stopped screen sharing
            </Alert>

            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 2,
                p: 2,
                borderRadius: 2,
                background: 'rgba(255, 255, 255, 0.05)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
              }}
            >
              <Avatar
                sx={{
                  width: 40,
                  height: 40,
                  background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                }}
              >
                <PersonIcon />
              </Avatar>
              <Box sx={{ flex: 1 }}>
                <Typography
                  variant="body2"
                  sx={{ color: 'rgba(255, 255, 255, 0.6)', fontSize: '0.75rem' }}
                >
                  Participant
                </Typography>
                <Typography variant="body1" sx={{ color: 'white', fontWeight: 500 }}>
                  {stoppedParticipant?.name ||
                    stoppedParticipant?.full_name ||
                    stoppedParticipant?.displayName ||
                    'Participant'}
                </Typography>
              </Box>
            </Box>

            <Typography
              variant="body2"
              sx={{
                color: 'rgba(255, 255, 255, 0.7)',
                textAlign: 'center',
                mt: 1,
              }}
            >
              The participant has been notified and their screen share has ended.
            </Typography>
          </Box>
        )}
      </DialogContent>

      {/* Dialog Actions */}
      <DialogActions
        sx={{
          px: 3,
          pb: 3,
          pt: 2,
          borderTop: '1px solid rgba(255, 255, 255, 0.1)',
        }}
      >
        <Button
          onClick={handleClose}
          variant="contained"
          fullWidth
          sx={{
            py: 1.5,
            borderRadius: 2,
            background: isCurrentUser
              ? 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
              : 'linear-gradient(135deg, #2196f3 0%, #1976d2 100%)',
            fontWeight: 600,
            textTransform: 'none',
            fontSize: '1rem',
            boxShadow: isCurrentUser
              ? '0 4px 12px rgba(102, 126, 234, 0.4)'
              : '0 4px 12px rgba(33, 150, 243, 0.4)',
            '&:hover': {
              background: isCurrentUser
                ? 'linear-gradient(135deg, #5568d3 0%, #6a4293 100%)'
                : 'linear-gradient(135deg, #1976d2 0%, #1565c0 100%)',
              transform: 'translateY(-1px)',
              boxShadow: isCurrentUser
                ? '0 6px 16px rgba(102, 126, 234, 0.5)'
                : '0 6px 16px rgba(33, 150, 243, 0.5)',
            },
            transition: 'all 0.2s ease',
          }}
        >
          {isCurrentUser ? 'Understood' : 'Close'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ScreenShareStoppedDialog;