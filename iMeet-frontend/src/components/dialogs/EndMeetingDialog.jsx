// src/components/dialogs/EndMeetingDialog.jsx
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
import { MeetingRoom as MeetingRoomIcon } from '@mui/icons-material';

const EndMeetingDialog = ({
  open,
  onClose,
  onConfirm,
  coHosts = [],
  attendanceEnabled = false,
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
          <MeetingRoomIcon sx={{ color: '#ef4444' }} />
          End Meeting for Everyone
        </Box>
      </DialogTitle>
      
      <DialogContent>
        <Typography sx={{ mb: 2 }}>
          Are you sure you want to end this meeting for all participants? This action cannot be undone.
        </Typography>
        
        <Alert
          severity="warning"
          sx={{
            backgroundColor: 'rgba(245, 158, 11, 0.1)',
            border: '1px solid rgba(245, 158, 11, 0.3)',
            color: 'white'
          }}
        >
          <Typography variant="body2">• All participants will be immediately disconnected</Typography>
          <Typography variant="body2">• The meeting will be permanently closed</Typography>
          <Typography variant="body2">• Any ongoing recordings will be stopped and saved</Typography>
          {coHosts.length > 0 && (
            <Typography variant="body2">• All co-host privileges will be revoked</Typography>
          )}
          {attendanceEnabled && (
            <Typography variant="body2">• AI attendance tracking will be terminated and saved</Typography>
          )}
        </Alert>
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
          startIcon={<MeetingRoomIcon />}
          sx={{
            background: 'linear-gradient(135deg, #ef4444 0%, #dc2626 100%)',
            '&:hover': {
              background: 'linear-gradient(135deg, #f87171 0%, #ef4444 100%)'
            }
          }}
        >
          End Meeting
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default EndMeetingDialog;