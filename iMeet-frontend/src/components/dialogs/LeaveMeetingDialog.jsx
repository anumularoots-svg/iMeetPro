// src/components/dialogs/LeaveMeetingDialog.jsx
import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Alert,
  Box,
} from '@mui/material';
import { ExitToApp } from '@mui/icons-material';

const LeaveMeetingDialog = ({
  open,
  onClose,
  onConfirm,
  isHost,
  isCoHost,
  coHostPrivilegesActive,
  queueStatus,
}) => {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          background: 'rgba(45, 55, 72, 0.98)',
          backdropFilter: 'blur(20px)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          color: 'white',
          borderRadius: 2,
        }
      }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <ExitToApp sx={{ color: '#ef4444' }} />
          Leave Meeting
        </Box>
      </DialogTitle>
      
      <DialogContent>
        <Typography sx={{ mb: 2 }}>
          Are you sure you want to leave this meeting?
        </Typography>

        {/* Show different messages based on user role */}
        {isHost && (
          <Alert 
            severity="info" 
            sx={{ 
              mb: 2, 
              backgroundColor: 'rgba(59, 130, 246, 0.1)', 
              color: 'white' 
            }}
          >
            As the host, the meeting will continue for other participants.
          </Alert>
        )}

        {(isCoHost || coHostPrivilegesActive) && !isHost && (
          <Alert 
            severity="info" 
            sx={{ 
              mb: 2, 
              backgroundColor: 'rgba(249, 115, 22, 0.1)', 
              color: 'white' 
            }}
          >
            Your co-host privileges will be temporarily suspended.
          </Alert>
        )}

        {queueStatus?.status === "queued" && (
          <Alert 
            severity="warning" 
            sx={{ 
              mb: 2, 
              backgroundColor: 'rgba(245, 158, 11, 0.1)', 
              color: 'white' 
            }}
          >
            You are currently in the connection queue.
          </Alert>
        )}

        <Typography variant="body2" sx={{ color: 'rgba(255, 255, 255, 0.7)' }}>
          • Your attendance data will be saved<br />
          • Any ongoing recordings will continue<br />
          • You can rejoin using the same meeting link
        </Typography>
      </DialogContent>
      
      <DialogActions sx={{ p: 3 }}>
        <Button
          onClick={onClose}
          sx={{ color: 'white' }}
        >
          Cancel
        </Button>
        <Button
          onClick={onConfirm}
          variant="contained"
          startIcon={<ExitToApp />}
          sx={{
            background: 'linear-gradient(135deg, #ef4444 0%, #dc2626 100%)',
            '&:hover': {
              background: 'linear-gradient(135deg, #f87171 0%, #ef4444 100%)'
            }
          }}
        >
          Leave Meeting
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default LeaveMeetingDialog;