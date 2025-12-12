// components/meeting/MeetingSettings.jsx
import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Switch,
  FormControlLabel,
  TextField,
  Slider,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Paper,
  Grid,
  Avatar,
  Chip,
  Divider,
  Alert
} from '@mui/material';
import {
  Settings,
  Security,
  RecordVoiceOver,
  Videocam,
  People,
  Lock,
  Timer,
  VolumeUp
} from '@mui/icons-material';

const MeetingSettings = ({ open, onClose, meetingData, onSave, currentSettings, isHost }) => {
  const [settings, setSettings] = useState({
    waitingRoom: true,
    recording: false,
    autoRecord: false,
    transcription: false,
    muteOnEntry: true,
    videoOnEntry: true,
    chatEnabled: true,
    screenShareEnabled: true,
    reactionsEnabled: true,
    handRaiseEnabled: true,
    maxParticipants: 100,
    meetingPassword: '',
    recordingQuality: 'hd',
    audioQuality: 'high',
    autoEndMeeting: 120,
    allowGuestAccess: false,
    hostOnlyScreenShare: false,
    hostOnlyMute: false
  });

  // Initialize settings from props
  useEffect(() => {
    if (currentSettings) {
      setSettings(prev => ({
        ...prev,
        ...currentSettings
      }));
    }
  }, [currentSettings]);

  const handleSettingChange = (setting, value) => {
    setSettings(prev => ({
      ...prev,
      [setting]: value
    }));
  };

  const handleSave = () => {
    onSave(settings);
    onClose();
  };

  const qualityOptions = [
    { value: 'sd', label: 'Standard (720p)' },
    { value: 'hd', label: 'High (1080p)' },
    { value: 'uhd', label: 'Ultra HD (4K)' }
  ];

  const audioQualityOptions = [
    { value: 'low', label: 'Low' },
    { value: 'medium', label: 'Medium' },
    { value: 'high', label: 'High' },
    { value: 'studio', label: 'Studio Quality' }
  ];

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="md" 
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 3,
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        }
      }}
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" gap={2} color="white">
          <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
            <Settings />
          </Avatar>
          <Typography variant="h5" fontWeight="bold">
            Meeting Settings
          </Typography>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ bgcolor: 'rgba(255,255,255,0.95)', m: 2, borderRadius: 2 }}>
        {!isHost && (
          <Alert severity="info" sx={{ mb: 2 }}>
            Some settings can only be changed by the meeting host.
          </Alert>
        )}
        
        <Grid container spacing={3}>
          {/* Security Settings */}
          <Grid item xs={12}>
            <Paper sx={{ p: 3, borderRadius: 2, bgcolor: '#f8f9fa' }}>
              <Box display="flex" alignItems="center" gap={2} mb={2}>
                <Security color="primary" />
                <Typography variant="h6" color="primary">
                  Security & Access
                </Typography>
              </Box>
              
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.waitingRoom}
                        onChange={(e) => handleSettingChange('waitingRoom', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Enable Waiting Room"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.allowGuestAccess}
                        onChange={(e) => handleSettingChange('allowGuestAccess', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Allow Guest Access"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Meeting Password"
                    type="password"
                    value={settings.meetingPassword}
                    onChange={(e) => handleSettingChange('meetingPassword', e.target.value)}
                    variant="outlined"
                    size="small"
                    disabled={!isHost}
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Max Participants"
                    type="number"
                    value={settings.maxParticipants}
                    onChange={(e) => handleSettingChange('maxParticipants', parseInt(e.target.value))}
                    variant="outlined"
                    size="small"
                    inputProps={{ min: 2, max: 1000 }}
                    disabled={!isHost}
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Audio/Video Settings */}
          <Grid item xs={12}>
            <Paper sx={{ p: 3, borderRadius: 2, bgcolor: '#f8f9fa' }}>
              <Box display="flex" alignItems="center" gap={2} mb={2}>
                <Videocam color="primary" />
                <Typography variant="h6" color="primary">
                  Audio & Video
                </Typography>
              </Box>
              
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.muteOnEntry}
                        onChange={(e) => handleSettingChange('muteOnEntry', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Mute participants on entry"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.videoOnEntry}
                        onChange={(e) => handleSettingChange('videoOnEntry', e.target.checked)}
                        color="primary"
                      />
                    }
                    label="Enable video on entry"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Audio Quality</InputLabel>
                    <Select
                      value={settings.audioQuality}
                      onChange={(e) => handleSettingChange('audioQuality', e.target.value)}
                      label="Audio Quality"
                    >
                      {audioQualityOptions.map(option => (
                        <MenuItem key={option.value} value={option.value}>
                          {option.label}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.hostOnlyMute}
                        onChange={(e) => handleSettingChange('hostOnlyMute', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Host-only mute control"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Recording Settings */}
          <Grid item xs={12}>
            <Paper sx={{ p: 3, borderRadius: 2, bgcolor: '#f8f9fa' }}>
              <Box display="flex" alignItems="center" gap={2} mb={2}>
                <RecordVoiceOver color="primary" />
                <Typography variant="h6" color="primary">
                  Recording & Transcription
                </Typography>
              </Box>
              
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.recording}
                        onChange={(e) => handleSettingChange('recording', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Enable recording"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.autoRecord}
                        onChange={(e) => handleSettingChange('autoRecord', e.target.checked)}
                        color="primary"
                        disabled={!settings.recording || !isHost}
                      />
                    }
                    label="Auto start recording"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth size="small" disabled={!settings.recording || !isHost}>
                    <InputLabel>Recording Quality</InputLabel>
                    <Select
                      value={settings.recordingQuality}
                      onChange={(e) => handleSettingChange('recordingQuality', e.target.value)}
                      label="Recording Quality"
                    >
                      {qualityOptions.map(option => (
                        <MenuItem key={option.value} value={option.value}>
                          {option.label}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.transcription}
                        onChange={(e) => handleSettingChange('transcription', e.target.checked)}
                        color="primary"
                        disabled={!settings.recording || !isHost}
                      />
                    }
                    label="Enable transcription"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Participant Features */}
          <Grid item xs={12}>
            <Paper sx={{ p: 3, borderRadius: 2, bgcolor: '#f8f9fa' }}>
              <Box display="flex" alignItems="center" gap={2} mb={2}>
                <People color="primary" />
                <Typography variant="h6" color="primary">
                  Participant Features
                </Typography>
              </Box>
              
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.chatEnabled}
                        onChange={(e) => handleSettingChange('chatEnabled', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Enable chat"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.screenShareEnabled}
                        onChange={(e) => handleSettingChange('screenShareEnabled', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Enable screen sharing"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.reactionsEnabled}
                        onChange={(e) => handleSettingChange('reactionsEnabled', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Enable reactions"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.handRaiseEnabled}
                        onChange={(e) => handleSettingChange('handRaiseEnabled', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Enable hand raising"
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={settings.hostOnlyScreenShare}
                        onChange={(e) => handleSettingChange('hostOnlyScreenShare', e.target.checked)}
                        color="primary"
                        disabled={!isHost}
                      />
                    }
                    label="Host-only screen share"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Advanced Settings */}
          {isHost && (
            <Grid item xs={12}>
              <Paper sx={{ p: 3, borderRadius: 2, bgcolor: '#f8f9fa' }}>
                <Box display="flex" alignItems="center" gap={2} mb={2}>
                  <Timer color="primary" />
                  <Typography variant="h6" color="primary">
                    Advanced Settings
                  </Typography>
                </Box>
                
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <Typography gutterBottom>
                      Auto-end meeting after (minutes):
                    </Typography>
                    <Slider
                      value={settings.autoEndMeeting}
                      onChange={(e, value) => handleSettingChange('autoEndMeeting', value)}
                      min={30}
                      max={480}
                      step={30}
                      marks={[
                        { value: 30, label: '30m' },
                        { value: 120, label: '2h' },
                        { value: 240, label: '4h' },
                        { value: 480, label: '8h' }
                      ]}
                      valueLabelDisplay="auto"
                      sx={{ mt: 2 }}
                      disabled={!isHost}
                    />
                  </Grid>
                </Grid>
              </Paper>
            </Grid>
          )}
        </Grid>
      </DialogContent>

      <DialogActions sx={{ p: 3, bgcolor: 'rgba(255,255,255,0.1)' }}>
        <Button onClick={onClose} color="inherit" sx={{ color: 'white' }}>
          Cancel
        </Button>
        <Button
          variant="contained"
          onClick={handleSave}
          sx={{ 
            borderRadius: 2,
            bgcolor: 'white',
            color: 'primary.main',
            '&:hover': { bgcolor: 'rgba(255,255,255,0.9)' }
          }}
        >
          Save Settings
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default MeetingSettings;