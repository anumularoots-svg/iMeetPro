// src/components/dialogs/ForceStopScreenShareDialog.jsx
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
  Warning as WarningIcon,
  Person as PersonIcon,
} from '@mui/icons-material';

/**
 * Confirmation dialog shown to host/co-host before force-stopping a participant's screen share
 */
const ForceStopScreenShareDialog = ({
  open,
  onClose,
  onConfirm,
  participantName,
  participantData,
}) => {
  const handleConfirm = () => {
    onConfirm();
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
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
            background: 'linear-gradient(135deg, #ff9800 0%, #f57c00 100%)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 4px 12px rgba(255, 152, 0, 0.4)',
          }}
        >
          <WarningIcon sx={{ color: 'white', fontSize: 28 }} />
        </Box>
        <Box sx={{ flex: 1 }}>
          <Typography variant="h6" sx={{ color: 'white', fontWeight: 600 }}>
            Stop Screen Share?
          </Typography>
          <Typography variant="caption" sx={{ color: 'rgba(255, 255, 255, 0.6)' }}>
            Confirm action
          </Typography>
        </Box>
      </DialogTitle>

      {/* Dialog Content */}
      <DialogContent sx={{ pt: 3, pb: 2 }}>
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
            You are about to stop screen sharing for this participant
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
                {participantName || 'Unknown User'}
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
            The participant will be notified that their screen share was stopped by you.
          </Typography>
        </Box>
      </DialogContent>

      {/* Dialog Actions */}
      <DialogActions
        sx={{
          px: 3,
          pb: 3,
          pt: 2,
          gap: 2,
          borderTop: '1px solid rgba(255, 255, 255, 0.1)',
        }}
      >
        <Button
          onClick={onClose}
          variant="outlined"
          fullWidth
          sx={{
            py: 1.5,
            borderRadius: 2,
            borderColor: 'rgba(255, 255, 255, 0.3)',
            color: 'white',
            fontWeight: 600,
            textTransform: 'none',
            fontSize: '1rem',
            '&:hover': {
              borderColor: 'rgba(255, 255, 255, 0.5)',
              background: 'rgba(255, 255, 255, 0.05)',
            },
            transition: 'all 0.2s ease',
          }}
        >
          Cancel
        </Button>
        <Button
          onClick={handleConfirm}
          variant="contained"
          fullWidth
          sx={{
            py: 1.5,
            borderRadius: 2,
            background: 'linear-gradient(135deg, #f44336 0%, #d32f2f 100%)',
            fontWeight: 600,
            textTransform: 'none',
            fontSize: '1rem',
            boxShadow: '0 4px 12px rgba(244, 67, 54, 0.4)',
            '&:hover': {
              background: 'linear-gradient(135deg, #d32f2f 0%, #c62828 100%)',
              transform: 'translateY(-1px)',
              boxShadow: '0 6px 16px rgba(244, 67, 54, 0.5)',
            },
            transition: 'all 0.2s ease',
          }}
        >
          Stop Screen Share
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ForceStopScreenShareDialog;