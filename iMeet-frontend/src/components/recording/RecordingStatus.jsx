import React, { useState, useEffect } from 'react';
import {
  Box,
  Chip,
  Typography,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  LinearProgress,
  Alert,
  IconButton,
  Tooltip,
  Card,
  CardContent,
  Stack,
  Divider
} from '@mui/material';
import {
  FiberManualRecord as RecordIcon,
  Stop as StopIcon,
  Pause as PauseIcon,
  PlayArrow as PlayIcon,
  Settings as SettingsIcon,
  Storage as StorageIcon,
  Timer as TimerIcon,
  HighQuality as QualityIcon
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';

const RecordingStatus = ({ 
  isRecording = false, 
  isPaused = false, 
  duration = 0, 
  quality = 'HD',
  storageUsed = 0,
  maxStorage = 100,
  onStop,
  onPause,
  onResume,
  onSettings
}) => {
  const theme = useTheme();
  const [showDetails, setShowDetails] = useState(false);
  const [recordingSize, setRecordingSize] = useState(0);

  // Format duration to MM:SS
  const formatDuration = (seconds) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  // Calculate estimated file size (rough estimate: 1MB per minute for HD)
  useEffect(() => {
    const sizeMultiplier = quality === 'HD' ? 1 : quality === 'FHD' ? 2 : 0.5;
    setRecordingSize((duration / 60) * sizeMultiplier);
  }, [duration, quality]);

  const RecordingIndicator = () => (
    <Box
      sx={{
        position: 'fixed',
        top: 16,
        right: 16,
        zIndex: 9999,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        borderRadius: 2,
        p: 1,
        backdropFilter: 'blur(10px)',
        border: `1px solid ${theme.palette.divider}`,
      }}
    >
      <Stack direction="row" alignItems="center" spacing={1}>
        <Box
          sx={{
            width: 12,
            height: 12,
            borderRadius: '50%',
            backgroundColor: isRecording && !isPaused ? '#ff1744' : '#ffa726',
            animation: isRecording && !isPaused ? 'pulse 1.5s infinite' : 'none',
            '@keyframes pulse': {
              '0%': { opacity: 1, transform: 'scale(1)' },
              '50%': { opacity: 0.5, transform: 'scale(1.1)' },
              '100%': { opacity: 1, transform: 'scale(1)' }
            }
          }}
        />
        <Typography 
          variant="caption" 
          sx={{ 
            color: 'white', 
            fontWeight: 'medium',
            fontFamily: 'monospace'
          }}
        >
          {isPaused ? 'PAUSED' : 'REC'} {formatDuration(duration)}
        </Typography>
        <Tooltip title="Recording Details">
          <IconButton
            size="small"
            onClick={() => setShowDetails(true)}
            sx={{ color: 'white', p: 0.5 }}
          >
            <SettingsIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Stack>
    </Box>
  );

  const RecordingDetailsDialog = () => (
    <Dialog 
      open={showDetails} 
      onClose={() => setShowDetails(false)}
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
      <DialogTitle sx={{ color: 'white', pb: 1 }}>
        <Stack direction="row" alignItems="center" spacing={1}>
          <RecordIcon sx={{ color: '#ff1744' }} />
          <Typography variant="h6">Recording Details</Typography>
        </Stack>
      </DialogTitle>
      
      <DialogContent>
        <Stack spacing={3}>
          {/* Recording Status Card */}
          <Card sx={{ backgroundColor: 'rgba(255, 255, 255, 0.1)', color: 'white' }}>
            <CardContent>
              <Stack spacing={2}>
                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Typography variant="subtitle1">Status</Typography>
                  <Chip 
                    label={isPaused ? 'PAUSED' : 'RECORDING'} 
                    color={isPaused ? 'warning' : 'error'}
                    size="small"
                    sx={{ fontWeight: 'bold' }}
                  />
                </Box>
                
                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <TimerIcon fontSize="small" />
                    Duration
                  </Typography>
                  <Typography variant="h6" sx={{ fontFamily: 'monospace' }}>
                    {formatDuration(duration)}
                  </Typography>
                </Box>

                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <QualityIcon fontSize="small" />
                    Quality
                  </Typography>
                  <Chip label={quality} color="primary" size="small" />
                </Box>

                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Typography variant="body2">Estimated Size</Typography>
                  <Typography variant="body1">
                    {recordingSize.toFixed(1)} MB
                  </Typography>
                </Box>
              </Stack>
            </CardContent>
          </Card>

          {/* Storage Usage */}
          <Card sx={{ backgroundColor: 'rgba(255, 255, 255, 0.1)', color: 'white' }}>
            <CardContent>
              <Stack spacing={2}>
                <Box display="flex" alignItems="center" spacing={1}>
                  <StorageIcon fontSize="small" />
                  <Typography variant="subtitle1">Storage Usage</Typography>
                </Box>
                
                <Box>
                  <Box display="flex" justifyContent="space-between" mb={1}>
                    <Typography variant="body2">
                      {storageUsed.toFixed(1)} / {maxStorage} GB
                    </Typography>
                    <Typography variant="body2">
                      {((storageUsed / maxStorage) * 100).toFixed(1)}%
                    </Typography>
                  </Box>
                  <LinearProgress 
                    variant="determinate" 
                    value={(storageUsed / maxStorage) * 100}
                    sx={{
                      height: 8,
                      borderRadius: 4,
                      backgroundColor: 'rgba(255, 255, 255, 0.2)',
                      '& .MuiLinearProgress-bar': {
                        borderRadius: 4,
                        backgroundColor: storageUsed / maxStorage > 0.8 ? '#ff5722' : '#4caf50'
                      }
                    }}
                  />
                </Box>

                {storageUsed / maxStorage > 0.9 && (
                  <Alert 
                    severity="warning" 
                    sx={{ 
                      backgroundColor: 'rgba(255, 152, 0, 0.2)',
                      color: 'white',
                      '& .MuiAlert-icon': { color: '#ffa726' }
                    }}
                  >
                    Storage almost full! Consider stopping the recording.
                  </Alert>
                )}
              </Stack>
            </CardContent>
          </Card>

          {/* Recording Controls */}
          <Stack direction="row" spacing={2} justifyContent="center">
            {isRecording && (
              <>
                <Button
                  variant="contained"
                  color={isPaused ? "success" : "warning"}
                  startIcon={isPaused ? <PlayIcon /> : <PauseIcon />}
                  onClick={isPaused ? onResume : onPause}
                  sx={{ minWidth: 120 }}
                >
                  {isPaused ? 'Resume' : 'Pause'}
                </Button>
                
                <Button
                  variant="contained"
                  color="error"
                  startIcon={<StopIcon />}
                  onClick={() => {
                    setShowDetails(false);
                    onStop?.();
                  }}
                  sx={{ minWidth: 120 }}
                >
                  Stop
                </Button>
              </>
            )}
          </Stack>
        </Stack>
      </DialogContent>

      <DialogActions>
        <Button 
          onClick={() => setShowDetails(false)} 
          sx={{ color: 'white' }}
        >
          Close
        </Button>
        <Button 
          onClick={onSettings}
          startIcon={<SettingsIcon />}
          sx={{ color: 'white' }}
        >
          Settings
        </Button>
      </DialogActions>
    </Dialog>
  );

  if (!isRecording) return null;

  return (
    <>
      <RecordingIndicator />
      <RecordingDetailsDialog />
    </>
  );
};

export default RecordingStatus;