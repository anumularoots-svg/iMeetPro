// src/components/video/ScreenShare.jsx - FIXED VERSION
import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  IconButton,
  Typography,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Paper,
  Alert,
  Chip,
  Fab,
  Zoom,
  Snackbar
} from '@mui/material';
import {
  ScreenShare as ScreenShareIcon,
  StopScreenShare as StopScreenShareIcon,
  Computer as ComputerIcon,
  Tab as TabIcon,
  Window as WindowIcon,
  Settings as SettingsIcon,
  FullscreenExit as FullscreenExitIcon,
  Fullscreen as FullscreenIcon,
  VolumeUp as VolumeUpIcon,
  VolumeOff as VolumeOffIcon,
  Error as ErrorIcon,
  Info as InfoIcon
} from '@mui/icons-material';

const ScreenShare = ({ 
  isSharing, 
  onStartSharing, 
  onStopSharing, 
  onShareAudio,
  currentStream,
  disabled = false,
  isConnected = true 
}) => {
  const [showSourceDialog, setShowSourceDialog] = useState(false);
  const [showPermissionDialog, setShowPermissionDialog] = useState(false);
  const [availableSources, setAvailableSources] = useState([]);
  const [selectedSource, setSelectedSource] = useState(null);
  const [shareAudio, setShareAudio] = useState(false);
  const [error, setError] = useState('');
  const [showError, setShowError] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const videoRef = useRef(null);

  useEffect(() => {
    if (videoRef.current && currentStream) {
      videoRef.current.srcObject = currentStream;
    }
  }, [currentStream]);

  const getScreenSources = async () => {
    try {
      const sources = [
        {
          id: 'screen',
          name: 'Entire Screen',
          type: 'screen',
          icon: <ComputerIcon />
        },
        {
          id: 'window',
          name: 'Application Window',
          type: 'window',
          icon: <WindowIcon />
        },
        {
          id: 'tab',
          name: 'Browser Tab',
          type: 'browser',
          icon: <TabIcon />
        }
      ];
      
      setAvailableSources(sources);
    } catch (err) {
      setError('Failed to get screen sources');
      console.error('Error getting screen sources:', err);
    }
  };

  const handleStartScreenShare = async () => {
    try {
      // Check if WebSocket is connected
      if (!isConnected) {
        setError('Not connected to meeting. Please check your connection.');
        setShowError(true);
        return;
      }

      // Check browser compatibility
      if (!navigator.mediaDevices || !navigator.mediaDevices.getDisplayMedia) {
        setError('Screen sharing is not supported in this browser. Please use Chrome, Firefox, or Edge.');
        setShowError(true);
        return;
      }

      await getScreenSources();
      setShowSourceDialog(true);
      setError('');
    } catch (err) {
      setError('Screen sharing is not available');
      setShowError(true);
    }
  };

  const handleSourceSelect = async (source) => {
    try {
      setShowSourceDialog(false);
      
      let constraints = {
        video: true,
        audio: shareAudio
      };

      // Configure constraints based on source type
      switch (source.type) {
        case 'screen':
          constraints.video = {
            displaySurface: 'monitor',
            width: { ideal: 1920 },
            height: { ideal: 1080 },
            frameRate: { ideal: 30 }
          };
          break;
        case 'window':
          constraints.video = {
            displaySurface: 'window',
            width: { ideal: 1920 },
            height: { ideal: 1080 }
          };
          break;
        case 'browser':
          constraints.video = {
            displaySurface: 'browser',
            width: { ideal: 1920 },
            height: { ideal: 1080 }
          };
          break;
      }

      // Request screen share with better error handling
      try {
        const stream = await navigator.mediaDevices.getDisplayMedia(constraints);
        
        // Handle stream end event
        stream.getVideoTracks()[0].addEventListener('ended', () => {
          handleStopScreenShare();
        });

        onStartSharing(stream);
        setSelectedSource(source);
        
      } catch (err) {
        console.error('Screen share error:', err);
        
        if (err.name === 'NotAllowedError') {
          // User denied permission
          setShowPermissionDialog(true);
        } else if (err.name === 'NotFoundError') {
          setError('No screen sources available to share');
          setShowError(true);
        } else if (err.name === 'NotReadableError') {
          setError('Screen source is already being used by another application');
          setShowError(true);
        } else {
          setError(`Failed to start screen sharing: ${err.message}`);
          setShowError(true);
        }
      }
      
    } catch (err) {
      setError('Failed to start screen sharing: ' + err.message);
      setShowError(true);
      console.error('Screen share error:', err);
    }
  };

  const handleStopScreenShare = () => {
    if (currentStream) {
      currentStream.getTracks().forEach(track => track.stop());
    }
    onStopSharing();
    setSelectedSource(null);
    setIsFullscreen(false);
  };

  const toggleFullscreen = () => {
    if (videoRef.current) {
      if (!isFullscreen) {
        videoRef.current.requestFullscreen();
        setIsFullscreen(true);
      } else {
        document.exitFullscreen();
        setIsFullscreen(false);
      }
    }
  };

  const toggleAudioShare = () => {
    setShareAudio(!shareAudio);
    onShareAudio && onShareAudio(!shareAudio);
  };

  return (
    <>
      {/* Screen Share Button */}
      <Tooltip 
        title={
          !isConnected 
            ? 'Connect to meeting first' 
            : isSharing 
              ? 'Stop Screen Share' 
              : 'Share Screen'
        }
      >
        <span>
          <IconButton
            onClick={isSharing ? handleStopScreenShare : handleStartScreenShare}
            disabled={disabled || !isConnected}
            sx={{
              background: isSharing 
                ? 'linear-gradient(45deg, #f44336, #d32f2f)' 
                : 'linear-gradient(45deg, #4caf50, #388e3c)',
              color: 'white',
              width: 56,
              height: 56,
              '&:hover': {
                background: isSharing 
                  ? 'linear-gradient(45deg, #d32f2f, #f44336)' 
                  : 'linear-gradient(45deg, #388e3c, #4caf50)',
                transform: 'scale(1.05)'
              },
              '&:disabled': {
                background: 'rgba(255,255,255,0.1)',
                color: 'rgba(255,255,255,0.3)'
              },
              transition: 'all 0.3s ease'
            }}
          >
            {isSharing ? <StopScreenShareIcon /> : <ScreenShareIcon />}
          </IconButton>
        </span>
      </Tooltip>

      {/* Screen Share Preview */}
      {isSharing && currentStream && (
        <Paper
          elevation={8}
          sx={{
            position: 'fixed',
            bottom: 20,
            right: 20,
            width: 300,
            height: 200,
            borderRadius: 3,
            overflow: 'hidden',
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            zIndex: 1000
          }}
        >
          <Box sx={{ position: 'relative', height: '100%' }}>
            <video
              ref={videoRef}
              autoPlay
              muted
              style={{
                width: '100%',
                height: '100%',
                objectFit: 'cover'
              }}
            />
            
            {/* Preview Controls */}
            <Box
              sx={{
                position: 'absolute',
                top: 8,
                right: 8,
                display: 'flex',
                gap: 1
              }}
            >
              <Tooltip title="Fullscreen">
                <IconButton
                  size="small"
                  onClick={toggleFullscreen}
                  sx={{
                    background: 'rgba(0,0,0,0.5)',
                    color: 'white',
                    '&:hover': { background: 'rgba(0,0,0,0.7)' }
                  }}
                >
                  <FullscreenIcon fontSize="small" />
                </IconButton>
              </Tooltip>
              
              <Tooltip title="Stop Sharing">
                <IconButton
                  size="small"
                  onClick={handleStopScreenShare}
                  sx={{
                    background: 'rgba(244,67,54,0.8)',
                    color: 'white',
                    '&:hover': { background: 'rgba(244,67,54,1)' }
                  }}
                >
                  <StopScreenShareIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>

            {/* Sharing Status */}
            <Box
              sx={{
                position: 'absolute',
                bottom: 8,
                left: 8,
                right: 8
              }}
            >
              <Chip
                label={`Sharing: ${selectedSource?.name || 'Screen'}`}
                size="small"
                sx={{
                  background: 'rgba(76,175,80,0.9)',
                  color: 'white',
                  fontWeight: 'bold'
                }}
              />
            </Box>
          </Box>
        </Paper>
      )}

      {/* Source Selection Dialog */}
      <Dialog
        open={showSourceDialog}
        onClose={() => setShowSourceDialog(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 3,
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            color: 'white'
          }
        }}
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          gap: 2,
          background: 'rgba(255,255,255,0.1)'
        }}>
          <ScreenShareIcon />
          Select Screen Share Source
        </DialogTitle>

        <DialogContent sx={{ p: 3 }}>
          {error && (
            <Alert severity="error" sx={{ mb: 2, borderRadius: 2 }}>
              {error}
            </Alert>
          )}

          <Typography variant="body2" sx={{ mb: 3, opacity: 0.8 }}>
            Choose what you want to share with meeting participants:
          </Typography>

          <List sx={{ p: 0 }}>
            {availableSources.map((source) => (
              <ListItem
                key={source.id}
                button
                onClick={() => handleSourceSelect(source)}
                sx={{
                  borderRadius: 2,
                  mb: 1,
                  background: 'rgba(255,255,255,0.1)',
                  '&:hover': {
                    background: 'rgba(255,255,255,0.2)',
                    transform: 'translateX(8px)'
                  },
                  transition: 'all 0.3s ease'
                }}
              >
                <ListItemIcon sx={{ color: 'white' }}>
                  {source.icon}
                </ListItemIcon>
                <ListItemText
                  primary={source.name}
                  secondary={
                    source.type === 'screen' ? 'Share your entire screen' :
                    source.type === 'window' ? 'Share a specific application window' :
                    'Share a browser tab'
                  }
                  secondaryTypographyProps={{
                    sx: { color: 'rgba(255,255,255,0.7)' }
                  }}
                />
              </ListItem>
            ))}
          </List>

          {/* Audio Sharing Option */}
          <Box
            sx={{
              mt: 3,
              p: 2,
              borderRadius: 2,
              background: 'rgba(255,255,255,0.1)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between'
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <VolumeUpIcon />
              <Typography variant="body2">
                Share computer audio
              </Typography>
            </Box>
            <Button
              variant={shareAudio ? 'contained' : 'outlined'}
              size="small"
              onClick={toggleAudioShare}
              sx={{
                background: shareAudio ? 'linear-gradient(45deg, #4caf50, #388e3c)' : 'transparent',
                borderColor: 'rgba(255,255,255,0.3)',
                color: 'white',
                '&:hover': {
                  background: shareAudio 
                    ? 'linear-gradient(45deg, #388e3c, #4caf50)' 
                    : 'rgba(255,255,255,0.1)'
                }
              }}
            >
              {shareAudio ? 'ON' : 'OFF'}
            </Button>
          </Box>
          {/* YouTube/Spotify Audio Warning */}
<Alert 
  severity="warning" 
  sx={{ 
    mt: 2, 
    background: 'rgba(255, 152, 0, 0.1)',
    color: 'white',
    border: '1px solid rgba(255, 152, 0, 0.3)',
    '& .MuiAlert-icon': { color: '#ff9800' }
  }}
>
  <Typography variant="subtitle2" sx={{ fontWeight: 'bold', mb: 1 }}>
    ⚠️ Important for YouTube/Spotify Audio:
  </Typography>
  <Typography variant="caption" component="div" sx={{ lineHeight: 1.6 }}>
    • <strong>Window/Screen sharing:</strong> ❌ Audio will be BLOCKED<br/>
    • <strong>Chrome Tab sharing:</strong> ✅ Audio will work (check "Share tab audio")<br/>
    <br/>
    <strong>For music/video apps: You MUST select "Chrome Tab"</strong>
  </Typography>
</Alert>
<Alert 
  severity="error" 
  sx={{ 
    mt: 2, 
    background: 'rgba(244, 67, 54, 0.1)',
    color: 'white',
    border: '2px solid rgba(244, 67, 54, 0.5)',
  }}
>
  <Typography variant="subtitle2" sx={{ fontWeight: 'bold', mb: 1 }}>
    ⚠️ YouTube Desktop App - Video Player Shows BLACK
  </Typography>
  
  <Typography variant="body2" component="div" sx={{ lineHeight: 1.8 }}>
    <strong>What you'll see:</strong><br/>
    ✅ YouTube logo, buttons, title, comments = VISIBLE<br/>
    ❌ Video player area = BLACK/BLANK (DRM protected)<br/>
    <br/>
    <strong>Why this happens:</strong><br/>
    YouTube Desktop App protects the video stream from being captured
    while allowing the UI to be visible.<br/>
    <br/>
    <strong>✅ SOLUTION - Use Chrome Browser:</strong><br/>
    1. Close YouTube Desktop App<br/>
    2. Open <strong>Chrome Browser</strong><br/>
    3. Go to <strong>youtube.com</strong><br/>
    4. Play your video<br/>
    5. Click "Share Screen" → Select "<strong>Chrome Tab</strong>"<br/>
    6. ✅ Check "<strong>Share tab audio</strong>"<br/>
    7. ✅ Video + Audio will work perfectly!<br/>
    <br/>
    <strong>🔄 Alternative - Share Entire Screen:</strong><br/>
    • Select "Entire Screen" (not Application Window)<br/>
    • Video will show ✅ but audio will NOT work ❌<br/>
  </Typography>
</Alert>
{/* Permission Info */}
<Alert 
  severity="info" 
  sx={{ 
    mt: 2, 
    background: 'rgba(33, 150, 243, 0.1)',
    color: 'white',
    '& .MuiAlert-icon': { color: '#2196f3' }
  }}
>
  <Typography variant="caption">
    Note: You'll see a browser permission dialog after selecting a source. 
    Make sure to allow screen access.
  </Typography>
</Alert>
        </DialogContent>

        <DialogActions sx={{ p: 3, background: 'rgba(255,255,255,0.1)' }}>
          <Button
            onClick={() => setShowSourceDialog(false)}
            sx={{
              color: 'white',
              borderColor: 'rgba(255,255,255,0.3)',
              '&:hover': {
                borderColor: 'white',
                background: 'rgba(255,255,255,0.1)'
              }
            }}
            variant="outlined"
          >
            Cancel
          </Button>
        </DialogActions>
      </Dialog>

      {/* Permission Denied Dialog */}
      <Dialog
        open={showPermissionDialog}
        onClose={() => setShowPermissionDialog(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 3,
            background: '#fff'
          }
        }}
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'error.main' }}>
          <ErrorIcon />
          Screen Share Permission Denied
        </DialogTitle>
        <DialogContent>
          <Typography variant="body1" paragraph>
            You need to allow screen sharing permission to share your screen.
          </Typography>
          <Typography variant="body2" color="text.secondary" paragraph>
            To enable screen sharing:
          </Typography>
          <List>
            <ListItem>
              <ListItemText 
                primary="1. Click the share button again"
                secondary="When prompted, select what you want to share"
              />
            </ListItem>
            <ListItem>
              <ListItemText 
                primary="2. Click 'Allow' or 'Share' in the browser dialog"
                secondary="Don't click 'Block' or 'Cancel'"
              />
            </ListItem>
            <ListItem>
              <ListItemText 
                primary="3. Select your screen, window, or tab"
                secondary="Then click 'Share' to start sharing"
              />
            </ListItem>
          </List>
          <Alert severity="info" sx={{ mt: 2 }}>
            If you previously blocked screen sharing, you may need to:
            <ul>
              <li>Click the camera/lock icon in your browser's address bar</li>
              <li>Change screen sharing permission to "Allow"</li>
              <li>Refresh the page and try again</li>
            </ul>
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowPermissionDialog(false)} variant="contained">
            Got it
          </Button>
        </DialogActions>
      </Dialog>

      {/* Error Snackbar */}
      <Snackbar
        open={showError}
        autoHideDuration={6000}
        onClose={() => setShowError(false)}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert 
          onClose={() => setShowError(false)} 
          severity="error" 
          sx={{ width: '100%' }}
        >
          {error}
        </Alert>
      </Snackbar>
    </>
  );
};

export default ScreenShare;