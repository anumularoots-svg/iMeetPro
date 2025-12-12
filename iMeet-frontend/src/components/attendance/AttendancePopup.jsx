import React from 'react';
import {
  Dialog,
  DialogContent,
  Box,
  Typography,
  Button,
  Alert,
  Slide,
} from '@mui/material';
import {
  Error as ErrorIcon,
  Block,
  PersonOff,
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

// ============================================================
// TERMINATION PALETTE (Red/Severe Theme)
// ============================================================
const colors = {
  primary: '#D32F2F',      // Red - Termination
  background: '#FFEBEE',   // Light Red Background
  white: '#FFFFFF',
  text: '#C62828',
};

// ============================================================
// STYLED COMPONENTS
// ============================================================
const StyledDialog = styled(Dialog)(({ theme }) => ({
  '& .MuiDialog-paper': {
    backgroundColor: colors.white,
    border: `3px solid ${colors.primary}`,
    borderRadius: theme.spacing(2),
    minWidth: 400,
    maxWidth: 600,
    boxShadow: `0 8px 32px rgba(211, 47, 47, 0.4)`,
    zIndex: 99999, // Ensure it sits above everything
  }
}));

const IconContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  width: 80,
  height: 80,
  borderRadius: '50%',
  margin: theme.spacing(0, 'auto', 2),
  backgroundColor: colors.background,
  border: `2px solid ${colors.primary}`,
}));

// ============================================================
// MAIN COMPONENT - ONLY FOR TERMINATION/BLOCKING
// ============================================================
const AttendancePopup = ({
  open,
  message,
  onClose, // often ignored in termination to force user exit
  attendanceData = {},
  faceAuthStatus = 'verified',
}) => {
  
  // Determine specific removal type
  const isIdentityBlock = message.toLowerCase().includes('identity') || message.toLowerCase().includes('unauthorized') || faceAuthStatus === 'blocked';
  
  return (
    <StyledDialog
      open={open}
      // Disable backdrop click or escape key closing
      disableEscapeKeyDown
      onClose={(event, reason) => {
        if (reason !== 'backdropClick') {
          onClose && onClose();
        }
      }}
      TransitionComponent={Slide}
      TransitionProps={{ direction: 'up' }}
    >
      <DialogContent sx={{ textAlign: 'center', p: 4 }}>
        
        <IconContainer>
          {isIdentityBlock ? (
            <Block sx={{ fontSize: 48, color: colors.primary }} />
          ) : (
            <ErrorIcon sx={{ fontSize: 48, color: colors.primary }} />
          )}
        </IconContainer>

        <Typography variant="h5" sx={{ mb: 2, fontWeight: 700, color: colors.text }}>
          {isIdentityBlock ? 'SESSION BLOCKED' : 'SESSION TERMINATED'}
        </Typography>

        <Alert 
          severity="error" 
          variant="filled"
          sx={{ 
            mb: 3, 
            justifyContent: 'center',
            backgroundColor: colors.primary 
          }}
        >
          {message || "You have been removed from this meeting."}
        </Alert>

        <Typography variant="body1" sx={{ mb: 1, color: '#555' }}>
          Current Attendance: <strong>{Math.round(attendanceData.attendancePercentage || 0)}%</strong>
        </Typography>

        <Box sx={{ 
          mt: 3, 
          p: 2, 
          backgroundColor: '#FAFAFA', 
          borderRadius: 2, 
          border: '1px solid #EEE' 
        }}>
          <Typography variant="caption" display="block" sx={{ color: '#777' }}>
            This action is permanent for this session.
            <br />
            Please contact your administrator or instructor.
          </Typography>
        </Box>

        <Button
          variant="contained"
          color="error"
          fullWidth
          size="large"
          sx={{ mt: 3, fontWeight: 600 }}
          onClick={() => window.location.href = "/"} // Force navigate away or close window
        >
          Return to Dashboard
        </Button>

      </DialogContent>
    </StyledDialog>
  );
};

export default AttendancePopup;