import React, { useState, useEffect } from 'react';
import { 
  Box, 
  AppBar, 
  Toolbar, 
  Typography, 
  IconButton,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Tooltip
} from '@mui/material';
import { 
  ExitToApp, 
  Settings, 
  FiberManualRecord,
  AccessTime,
  People
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

const MeetingLayout = ({ children }) => {
  const [leaveDialog, setLeaveDialog] = useState(false);
  const [hooks, setHooks] = useState({
    meeting: null,
    leaveMeeting: null,
    duration: 0,
    participantCount: 0,
    isRecording: false
  });
  const navigate = useNavigate();

  // ✅ FIXED: Simplified hook loading with proper error handling
  useEffect(() => {
    const loadHooks = async () => {
      try {
        // For now, we'll use static values since dynamic hook loading 
        // in React has limitations and requires proper setup
        setHooks({
          meeting: { meeting_name: 'Meeting Room', title: 'Meeting Room' },
          leaveMeeting: () => navigate('/dashboard'),
          duration: 0,
          participantCount: 0,
          isRecording: false
        });

        console.log('✅ Meeting layout hooks initialized with defaults');
      } catch (error) {
        console.error('❌ Failed to load hooks:', error);
        // Set safe defaults
        setHooks({
          meeting: { meeting_name: 'Meeting Room', title: 'Meeting Room' },
          leaveMeeting: () => navigate('/dashboard'),
          duration: 0,
          participantCount: 0,
          isRecording: false
        });
      }
    };

    loadHooks();
  }, [navigate]);

  const handleLeaveMeeting = () => {
    setLeaveDialog(false);
    
    // Try to use the leaveMeeting function if available
    if (hooks.leaveMeeting && typeof hooks.leaveMeeting === 'function') {
      try {
        hooks.leaveMeeting();
      } catch (error) {
        console.warn('leaveMeeting function failed:', error);
      }
    }
    
    // Always navigate back to dashboard as fallback
    setTimeout(() => {
      navigate('/dashboard');
    }, 100);
  };

  const formatDuration = (seconds) => {
    // ✅ FIXED: Added proper input validation
    if (typeof seconds !== 'number' || seconds < 0 || isNaN(seconds)) {
      return '0:00';
    }
    
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    return hrs > 0 
      ? `${hrs}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`
      : `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  // ✅ FIXED: Added safe property access
  const getMeetingTitle = () => {
    return hooks.meeting?.meeting_name || 
           hooks.meeting?.title || 
           'Meeting Room';
  };

  const getParticipantCount = () => {
    const count = hooks.participantCount;
    return typeof count === 'number' && count > 0 ? count : 0;
  };

  const getDuration = () => {
    const duration = hooks.duration;
    return typeof duration === 'number' && duration > 0 ? duration : 0;
  };

  const getIsRecording = () => {
    return Boolean(hooks.isRecording);
  };

  return (
    <Box sx={{ 
      height: '100vh', 
      display: 'flex', 
      flexDirection: 'column', 
      bgcolor: '#0f0f0f',
      overflow: 'hidden'
    }}>
      <AppBar 
        position="static" 
        sx={{ 
          bgcolor: 'rgba(0,0,0,0.9)', 
          backdropFilter: 'blur(15px)',
          borderBottom: '1px solid rgba(255,255,255,0.1)'
        }}
      >
        <Toolbar sx={{ justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography 
              variant="h6" 
              sx={{ 
                color: 'white',
                fontWeight: 600,
                maxWidth: 300,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap'
              }}
            >
              {getMeetingTitle()}
            </Typography>
            
            {getIsRecording() && (
              <Chip
                icon={<FiberManualRecord />}
                label="REC"
                size="small"
                sx={{
                  bgcolor: 'error.main',
                  color: 'white',
                  animation: 'pulse 2s infinite',
                  '@keyframes pulse': {
                    '0%': { opacity: 1 },
                    '50%': { opacity: 0.7 },
                    '100%': { opacity: 1 }
                  }
                }}
              />
            )}
          </Box>
          
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            {/* Only show duration if it's greater than 0 */}
            {getDuration() > 0 && (
              <Chip
                icon={<AccessTime />}
                label={formatDuration(getDuration())}
                size="small"
                sx={{ bgcolor: 'rgba(255,255,255,0.1)', color: 'white' }}
              />
            )}
            
            {/* Only show participant count if it's greater than 0 */}
            {getParticipantCount() > 0 && (
              <Chip
                icon={<People />}
                label={getParticipantCount()}
                size="small"
                sx={{ bgcolor: 'rgba(255,255,255,0.1)', color: 'white' }}
              />
            )}
            
            <Tooltip title="Meeting Settings">
              <IconButton color="inherit" sx={{ color: 'white' }}>
                <Settings />
              </IconButton>
            </Tooltip>
            
            <Tooltip title="Leave Meeting">
              <IconButton 
                color="error" 
                onClick={() => setLeaveDialog(true)}
                sx={{
                  position:"relative",
                  bgcolor: 'rgba(244, 67, 54, 0.2)',
                  '&:hover': { bgcolor: 'rgba(244, 67, 54, 0.3)' }
                }}
              >
                <ExitToApp />
              </IconButton>
            </Tooltip>
          </Box>
        </Toolbar>
      </AppBar>
      
      {/* MAIN CONTENT - Always render children */}
      <Box sx={{ 
        flexGrow: 1, 
        overflow: 'hidden',
        position: 'relative',
        background: 'linear-gradient(135deg, #1a1a1a 0%, #2d2d30 100%)'
      }}>
        {children}
      </Box>

      {/* Leave Meeting Dialog */}
      <Dialog
        open={leaveDialog}
        onClose={() => setLeaveDialog(false)}
        PaperProps={{
          sx: {
            borderRadius: 3,
            bgcolor: 'background.paper',
            minWidth: 400
          }
        }}
      >
        <DialogTitle sx={{ textAlign: 'center', pb: 1 }}>
          <ExitToApp sx={{ fontSize: 48, color: 'error.main', mb: 1 }} />
          <Typography variant="h6">Leave Meeting?</Typography>
        </DialogTitle>
        
        <DialogContent sx={{ textAlign: 'center', pb: 2 }}>
          <Typography variant="body1" color="text.secondary">
            Are you sure you want to leave this meeting?
          </Typography>
        </DialogContent>
        
        <DialogActions sx={{ justifyContent: 'center', gap: 2, pb: 3 }}>
          <Button 
            onClick={() => setLeaveDialog(false)}
            variant="outlined"
            sx={{ minWidth: 100 }}
          >
            Cancel
          </Button>
          <Button 
            onClick={handleLeaveMeeting}
            variant="contained"
            color="error"
            sx={{ minWidth: 100 }}
          >
            Leave
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default MeetingLayout;