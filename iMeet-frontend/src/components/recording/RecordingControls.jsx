// FIXED: RecordingControls.jsx - Only show delete controls for hosts
import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  IconButton,
  Typography,
  Card,
  CardContent,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Checkbox,
  Chip,
  LinearProgress,
  Avatar,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Tooltip,
  Alert,
  Fade,
  Zoom,
  Stack,
  Divider
} from '@mui/material';
import {
  Videocam as RecordIcon,
  Stop as StopIcon,
  Pause as PauseIcon,
  PlayArrow as ResumeIcon,
  Settings as SettingsIcon,
  RadioButtonChecked as RecordingIcon,
  Schedule as TimerIcon,
  Storage as StorageIcon,
  HighQuality as QualityIcon,
  CloudUpload as CloudIcon,
  GetApp as DownloadIcon,
  Share as ShareIcon,
  Warning as WarningIcon,
  Delete as DeleteIcon,
  Security as SecurityIcon
} from '@mui/icons-material';
import { useAuth } from '../hooks/useAuth'; // ADDED: Import useAuth

const RecordingControls = ({ 
  isRecording = false,
  isPaused = false,
  recordingDuration = 0,
  onStartRecording,
  onStopRecording,
  onPauseRecording,
  onResumeRecording,
  onDeleteRecording, // ADDED: Delete callback
  isHost = false,
  currentRecording = null, // ADDED: Current recording data
  meetingParticipants = [],
  storageUsed = 0,
  storageLimit = 10000 // MB
}) => {
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [deleteDialog, setDeleteDialog] = useState(false); // ADDED: Delete dialog state
  const [recordingSettings, setRecordingSettings] = useState({
    quality: 'high', // low, medium, high, ultra
    includeChat: true,
    includeScreenShare: true,
    autoUpload: true,
    fileName: '',
    saveLocation: 'cloud' // local, cloud
  });
  const [confirmDialog, setConfirmDialog] = useState({ open: false, action: null });

  // ADDED: Get current user data
  const { user: authUser } = useAuth();
  const currentUser = authUser || {
    email: localStorage.getItem('user_email') || '',
    id: localStorage.getItem('user_id') || '',
    name: localStorage.getItem('user_name') || 'User'
  };

  // ADDED: Check if current user can delete the recording
  const canDeleteRecording = () => {
    if (!currentRecording) return false;
    if (!isHost) return false;
    
    // Check if current user is the host/owner of the recording
    const recordingUserId = String(currentRecording.user_id || currentRecording.host_id || '');
    const currentUserId = String(currentUser.id || '');
    
    return recordingUserId === currentUserId;
  };

  const formatDuration = (seconds) => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    if (hours > 0) {
      return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    return `${minutes}:${secs.toString().padStart(2, '0')}`;
  };

  const getQualityDetails = (quality) => {
    const qualities = {
      low: { label: '480p', size: '~50MB/hr', bitrate: '500kbps' },
      medium: { label: '720p', size: '~150MB/hr', bitrate: '1.5Mbps' },
      high: { label: '1080p', size: '~300MB/hr', bitrate: '3Mbps' },
      ultra: { label: '4K', size: '~800MB/hr', bitrate: '8Mbps' }
    };
    return qualities[quality] || qualities.high;
  };

  const getStoragePercentage = () => {
    return Math.min((storageUsed / storageLimit) * 100, 100);
  };

  const handleStartRecording = () => {
    if (!recordingSettings.fileName.trim()) {
      setRecordingSettings(prev => ({
        ...prev,
        fileName: `Meeting_${new Date().toISOString().split('T')[0]}_${new Date().getTime()}`
      }));
    }
    onStartRecording?.(recordingSettings);
  };

  const handleStopRecording = () => {
    setConfirmDialog({ open: true, action: 'stop' });
  };

  // ADDED: Handle delete recording
  const handleDeleteRecording = async () => {
    try {
      if (!currentRecording) {
        console.warn('No recording to delete');
        return;
      }

      console.log('🗑️ Deleting recording:', currentRecording);
      
      // Call the delete callback with user credentials
      await onDeleteRecording?.(currentRecording, {
        user_id: currentUser.id,
        email: currentUser.email
      });
      
      console.log('✅ Recording deleted successfully');
    } catch (error) {
      console.error('❌ Delete failed:', error);
      alert('Failed to delete recording: ' + error.message);
    } finally {
      setDeleteDialog(false);
    }
  };

  const confirmAction = () => {
    if (confirmDialog.action === 'stop') {
      onStopRecording?.();
    }
    setConfirmDialog({ open: false, action: null });
  };

  const RecordingStatus = () => {
    if (!isRecording) return null;

    return (
      <Fade in={isRecording}>
        <Card 
          elevation={3}
          sx={{ 
            background: isPaused 
              ? 'linear-gradient(135deg, #ff9800 0%, #ffb74d 100%)'
              : 'linear-gradient(135deg, #f44336 0%, #ef5350 100%)',
            color: 'white',
            mb: 2
          }}
        >
          <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Zoom in={!isPaused}>
                <Box sx={{ 
                  display: 'flex', 
                  alignItems: 'center',
                  animation: !isPaused ? 'pulse 2s infinite' : 'none',
                  '@keyframes pulse': {
                    '0%': { opacity: 1 },
                    '50%': { opacity: 0.5 },
                    '100%': { opacity: 1 }
                  }
                }}>
                  <RecordingIcon sx={{ fontSize: 24, mr: 1 }} />
                </Box>
              </Zoom>
              
              <Box sx={{ flex: 1 }}>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  {isPaused ? 'Recording Paused' : 'Recording in Progress'}
                </Typography>
                <Typography variant="body2" sx={{ opacity: 0.9 }}>
                  Duration: {formatDuration(recordingDuration)}
                </Typography>
              </Box>

              <Box sx={{ display: 'flex', gap: 1 }}>
                {isPaused ? (
                  <Tooltip title="Resume Recording">
                    <IconButton
                      onClick={onResumeRecording}
                      sx={{ 
                        bgcolor: 'rgba(255,255,255,0.2)',
                        color: 'white',
                        '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' }
                      }}
                    >
                      <ResumeIcon />
                    </IconButton>
                  </Tooltip>
                ) : (
                  <Tooltip title="Pause Recording">
                    <IconButton
                      onClick={onPauseRecording}
                      sx={{ 
                        bgcolor: 'rgba(255,255,255,0.2)',
                        color: 'white',
                        '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' }
                      }}
                    >
                      <PauseIcon />
                    </IconButton>
                  </Tooltip>
                )}
                
                <Tooltip title="Stop Recording">
                  <IconButton
                    onClick={handleStopRecording}
                    sx={{ 
                      bgcolor: 'rgba(255,255,255,0.2)',
                      color: 'white',
                      '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' }
                    }}
                  >
                    <StopIcon />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>

            {/* Quality and settings info */}
            <Box sx={{ display: 'flex', gap: 1, mt: 1, flexWrap: 'wrap' }}>
              <Chip
                size="small"
                label={`Quality: ${getQualityDetails(recordingSettings.quality).label}`}
                variant="outlined"
                sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)' }}
              />
              <Chip
                size="small"
                label={recordingSettings.saveLocation === 'cloud' ? 'Cloud Save' : 'Local Save'}
                variant="outlined"
                sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)' }}
              />
              {recordingSettings.includeChat && (
                <Chip
                  size="small"
                  label="Chat Included"
                  variant="outlined"
                  sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)' }}
                />
              )}
              {isHost && (
                <Chip
                  size="small"
                  label="Host"
                  icon={<SecurityIcon />}
                  variant="outlined"
                  sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)' }}
                />
              )}
            </Box>
          </CardContent>
        </Card>
      </Fade>
    );
  };

  if (!isHost) {
    return (
      <Box sx={{ textAlign: 'center', py: 2 }}>
        <RecordingStatus />
        {isRecording && (
          <Alert severity="info" sx={{ mt: 1 }}>
            This meeting is being recorded by the host
          </Alert>
        )}
      </Box>
    );
  }

  return (
    <Box>
      <RecordingStatus />
      
      {/* Storage Usage */}
      <Card elevation={1} sx={{ mb: 2 }}>
        <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
            <StorageIcon color="primary" />
            <Typography variant="subtitle2">
              Storage Usage: {storageUsed}MB / {storageLimit}MB
            </Typography>
          </Box>
          <LinearProgress
            variant="determinate"
            value={getStoragePercentage()}
            sx={{
              height: 8,
              borderRadius: 4,
              bgcolor: 'grey.200',
              '& .MuiLinearProgress-bar': {
                borderRadius: 4,
                background: getStoragePercentage() > 80 
                  ? 'linear-gradient(90deg, #f44336 0%, #ef5350 100%)'
                  : getStoragePercentage() > 60
                  ? 'linear-gradient(90deg, #ff9800 0%, #ffb74d 100%)'
                  : 'linear-gradient(90deg, #4caf50 0%, #66bb6a 100%)'
              }
            }}
          />
          {getStoragePercentage() > 90 && (
            <Alert severity="warning" sx={{ mt: 1 }}>
              <WarningIcon sx={{ mr: 1 }} />
              Storage almost full! Consider upgrading your plan.
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Recording Controls */}
      <Card elevation={2}>
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <RecordIcon color="primary" />
              Recording Controls
            </Typography>
            
            <IconButton
              onClick={() => setSettingsOpen(true)}
              disabled={isRecording}
              sx={{ 
                bgcolor: 'primary.light',
                color: 'primary.contrastText',
                '&:hover': { bgcolor: 'primary.main' }
              }}
            >
              <SettingsIcon />
            </IconButton>
          </Box>

          <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
            {!isRecording ? (
              <Button
                variant="contained"
                size="large"
                startIcon={<RecordIcon />}
                onClick={handleStartRecording}
                sx={{
                  background: 'linear-gradient(135deg, #f44336 0%, #d32f2f 100%)',
                  '&:hover': {
                    background: 'linear-gradient(135deg, #d32f2f 0%, #c62828 100%)'
                  }
                }}
              >
                Start Recording
              </Button>
            ) : (
              <Button
                variant="contained"
                size="large"
                startIcon={<StopIcon />}
                onClick={handleStopRecording}
                color="error"
              >
                Stop Recording
              </Button>
            )}

            {/* ADDED: Delete recording button - Only show for hosts with existing recordings */}
            {currentRecording && canDeleteRecording() && (
              <Button
                variant="outlined"
                size="large"
                startIcon={<DeleteIcon />}
                onClick={() => setDeleteDialog(true)}
                color="error"
                sx={{ ml: 2 }}
              >
                Delete Recording
              </Button>
            )}
          </Box>

          {/* Quick Settings Preview */}
          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
            <Chip
              icon={<QualityIcon />}
              label={`${getQualityDetails(recordingSettings.quality).label} Quality`}
              color="primary"
              variant="outlined"
            />
            <Chip
              icon={recordingSettings.saveLocation === 'cloud' ? <CloudIcon /> : <DownloadIcon />}
              label={recordingSettings.saveLocation === 'cloud' ? 'Cloud Storage' : 'Local Storage'}
              color="primary"
              variant="outlined"
            />
            <Chip
              label={`${getQualityDetails(recordingSettings.quality).size}`}
              color="info"
              variant="outlined"
            />
            {isHost && (
              <Chip
                icon={<SecurityIcon />}
                label="Host Controls"
                color="success"
                variant="outlined"
              />
            )}
          </Box>
        </CardContent>
      </Card>

      {/* Recording Settings Dialog */}
      <Dialog
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Typography variant="h6" component="div" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <SettingsIcon color="primary" />
            Recording Settings
          </Typography>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, pt: 1 }}>
            {/* File Name */}
            <TextField
              label="Recording File Name"
              fullWidth
              value={recordingSettings.fileName}
              onChange={(e) => setRecordingSettings(prev => ({ ...prev, fileName: e.target.value }))}
              placeholder="Enter file name..."
            />

            {/* Quality Settings */}
            <FormControl component="fieldset">
              <FormLabel component="legend">Video Quality</FormLabel>
              <RadioGroup
                value={recordingSettings.quality}
                onChange={(e) => setRecordingSettings(prev => ({ ...prev, quality: e.target.value }))}
              >
                {Object.entries(['low', 'medium', 'high', 'ultra']).map(([index, key]) => {
                  const details = getQualityDetails(key);
                  return (
                    <FormControlLabel
                      key={key}
                      value={key}
                      control={<Radio />}
                      label={
                        <Box>
                          <Typography variant="body1">
                            {details.label} - {details.bitrate}
                          </Typography>
                          <Typography variant="caption" color="text.secondary">
                            Estimated size: {details.size}
                          </Typography>
                        </Box>
                      }
                    />
                  );
                })}
              </RadioGroup>
            </FormControl>

            {/* Save Location */}
            <FormControl component="fieldset">
              <FormLabel component="legend">Save Location</FormLabel>
              <RadioGroup
                value={recordingSettings.saveLocation}
                onChange={(e) => setRecordingSettings(prev => ({ ...prev, saveLocation: e.target.value }))}
              >
                <FormControlLabel
                  value="cloud"
                  control={<Radio />}
                  label={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <CloudIcon />
                      <Box>
                        <Typography>Cloud Storage</Typography>
                        <Typography variant="caption" color="text.secondary">
                          Automatic backup and sharing
                        </Typography>
                      </Box>
                    </Box>
                  }
                />
                <FormControlLabel
                  value="local"
                  control={<Radio />}
                  label={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <DownloadIcon />
                      <Box>
                        <Typography>Local Download</Typography>
                        <Typography variant="caption" color="text.secondary">
                          Download to your device
                        </Typography>
                      </Box>
                    </Box>
                  }
                />
              </RadioGroup>
            </FormControl>

            {/* Additional Options */}
            <Box>
              <FormLabel component="legend">Include in Recording</FormLabel>
              <Box sx={{ mt: 1 }}>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={recordingSettings.includeChat}
                      onChange={(e) => setRecordingSettings(prev => ({ ...prev, includeChat: e.target.checked }))}
                    />
                  }
                  label="Chat Messages"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={recordingSettings.includeScreenShare}
                      onChange={(e) => setRecordingSettings(prev => ({ ...prev, includeScreenShare: e.target.checked }))}
                    />
                  }
                  label="Screen Sharing"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={recordingSettings.autoUpload}
                      onChange={(e) => setRecordingSettings(prev => ({ ...prev, autoUpload: e.target.checked }))}
                    />
                  }
                  label="Auto-upload after meeting"
                />
              </Box>
            </Box>

            {/* Host Notice */}
            {isHost && (
              <Alert severity="success" icon={<SecurityIcon />}>
                <Typography variant="body2">
                  As the meeting host, you have full control over recording settings and can delete recordings.
                </Typography>
              </Alert>
            )}

            {/* Participants Notice */}
            <Alert severity="info">
              All participants will be notified when recording starts. 
              Make sure you have consent from all attendees.
            </Alert>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setSettingsOpen(false)}>
            Cancel
          </Button>
          <Button 
            onClick={() => setSettingsOpen(false)}
            variant="contained"
          >
            Save Settings
          </Button>
        </DialogActions>
      </Dialog>

      {/* ADDED: Delete Recording Confirmation Dialog */}
      <Dialog
        open={deleteDialog}
        onClose={() => setDeleteDialog(false)}
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'error.main' }}>
          <DeleteIcon />
          Delete Recording
        </DialogTitle>
        <DialogContent>
          <Stack spacing={2}>
            <Alert severity="warning">
              <Typography variant="body2">
                This action cannot be undone. The recording and all associated files will be permanently deleted.
              </Typography>
            </Alert>
            
            {currentRecording && (
              <Box>
                <Typography variant="subtitle2" gutterBottom>
                  Recording Details:
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  • File: {currentRecording.fileName || currentRecording.meeting_name || 'Unknown'}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  • Duration: {formatDuration(recordingDuration)}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  • Size: {currentRecording.fileSize || currentRecording.file_size || 'Unknown'}
                </Typography>
              </Box>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialog(false)}>
            Cancel
          </Button>
          <Button 
            onClick={handleDeleteRecording}
            variant="contained" 
            color="error"
            startIcon={<DeleteIcon />}
          >
            Delete Permanently
          </Button>
        </DialogActions>
      </Dialog>

      {/* Stop Recording Confirmation Dialog */}
      <Dialog
        open={confirmDialog.open}
        onClose={() => setConfirmDialog({ open: false, action: null })}
      >
        <DialogTitle>Stop Recording?</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to stop the recording? 
            The recording duration is {formatDuration(recordingDuration)}.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmDialog({ open: false, action: null })}>
            Cancel
          </Button>
          <Button onClick={confirmAction} variant="contained" color="error">
            Stop Recording
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RecordingControls;