// src/components/controls/AudioControls.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  IconButton,
  Slider,
  Popover,
  Paper,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Switch,
  FormControlLabel,
  Chip
} from '@mui/material';
import {
  Mic,
  MicOff,
  VolumeUp,
  VolumeDown,
  Settings,
  GraphicEq,
  Tune
} from '@mui/icons-material';

const AudioControls = ({ 
  isMuted, 
  onToggleMute, 
  volume = 50,
  onVolumeChange,
  audioDevices = [],
  selectedDevice,
  onDeviceChange,
  noiseReduction = true,
  onNoiseReductionToggle,
  echoCancellation = true,
  onEchoCancellationToggle
}) => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [audioLevel, setAudioLevel] = useState(0);

  const handleSettingsClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  // Simulate audio level meter
  useEffect(() => {
    if (!isMuted) {
      const interval = setInterval(() => {
        setAudioLevel(Math.random() * 100);
      }, 100);
      return () => clearInterval(interval);
    } else {
      setAudioLevel(0);
    }
  }, [isMuted]);

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
      {/* Main Mute Button */}
      <IconButton
        onClick={onToggleMute}
        sx={{
          bgcolor: isMuted ? 'error.main' : 'primary.main',
          color: 'white',
          '&:hover': {
            bgcolor: isMuted ? 'error.dark' : 'primary.dark'
          }
        }}
      >
        {isMuted ? <MicOff /> : <Mic />}
      </IconButton>

      {/* Audio Level Indicator */}
      <Box
        sx={{
          width: 4,
          height: 24,
          bgcolor: 'grey.300',
          borderRadius: 2,
          position: 'relative',
          overflow: 'hidden'
        }}
      >
        <Box
          sx={{
            position: 'absolute',
            bottom: 0,
            width: '100%',
            height: `${audioLevel}%`,
            bgcolor: audioLevel > 80 ? 'error.main' : audioLevel > 50 ? 'warning.main' : 'success.main',
            transition: 'height 0.1s ease'
          }}
        />
      </Box>

      {/* Audio Settings */}
      <IconButton onClick={handleSettingsClick} size="small">
        <Settings />
      </IconButton>

      <Popover
        open={Boolean(anchorEl)}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
      >
        <Paper sx={{ p: 3, minWidth: 300 }}>
          <Typography variant="h6" gutterBottom>
            Audio Settings
          </Typography>

          {/* Volume Control */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="body2" gutterBottom>
              Microphone Volume
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <VolumeDown />
              <Slider
                value={volume}
                onChange={(e, value) => onVolumeChange(value)}
                sx={{ flex: 1 }}
              />
              <VolumeUp />
            </Box>
          </Box>

          {/* Audio Devices */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="body2" gutterBottom>
              Microphone Device
            </Typography>
            <List dense>
              {audioDevices.map((device) => (
                <ListItem
                  key={device.deviceId}
                  button
                  selected={device.deviceId === selectedDevice}
                  onClick={() => onDeviceChange(device.deviceId)}
                >
                  <ListItemIcon>
                    <Mic />
                  </ListItemIcon>
                  <ListItemText primary={device.label || 'Default Microphone'} />
                </ListItem>
              ))}
            </List>
          </Box>

          {/* Audio Enhancements */}
          <Box>
            <Typography variant="body2" gutterBottom>
              Audio Enhancements
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={noiseReduction}
                  onChange={onNoiseReductionToggle}
                />
              }
              label="Noise Reduction"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={echoCancellation}
                  onChange={onEchoCancellationToggle}
                />
              }
              label="Echo Cancellation"
            />
          </Box>

          {/* Status Chips */}
          <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
            {noiseReduction && (
              <Chip
                icon={<GraphicEq />}
                label="Noise Reduction"
                size="small"
                color="primary"
              />
            )}
            {echoCancellation && (
              <Chip
                icon={<Tune />}
                label="Echo Cancellation"
                size="small"
                color="primary"
              />
            )}
          </Box>
        </Paper>
      </Popover>
    </Box>
  );
};

export default AudioControls;