// src/components/controls/VideoControls.jsx
import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Popover,
  Paper,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Slider,
  FormControlLabel,
  Switch,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Chip
} from '@mui/material';
import {
  Videocam,
  VideocamOff,
  Settings,
  Brightness4,
  Contrast,
  Tune,
  Hd,
  VideoSettings
} from '@mui/icons-material';

const VideoControls = ({
  isVideoOff,
  onToggleVideo,
  videoDevices = [],
  selectedDevice,
  onDeviceChange,
  brightness = 50,
  onBrightnessChange,
  contrast = 50,
  onContrastChange,
  videoQuality = 'HD',
  onQualityChange,
  flipVideo = false,
  onFlipVideoToggle,
  backgroundBlur = false,
  onBackgroundBlurToggle
}) => {
  const [anchorEl, setAnchorEl] = useState(null);

  const handleSettingsClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const qualityOptions = [
    { value: 'SD', label: 'SD (480p)', icon: <VideoSettings /> },
    { value: 'HD', label: 'HD (720p)', icon: <Hd /> },
    { value: 'FHD', label: 'Full HD (1080p)', icon: <Hd /> }
  ];

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
      {/* Main Video Button */}
      <IconButton
        onClick={onToggleVideo}
        sx={{
          bgcolor: isVideoOff ? 'error.main' : 'primary.main',
          color: 'white',
          '&:hover': {
            bgcolor: isVideoOff ? 'error.dark' : 'primary.dark'
          }
        }}
      >
        {isVideoOff ? <VideocamOff /> : <Videocam />}
      </IconButton>

      {/* Video Quality Indicator */}
      <Chip
        label={videoQuality}
        size="small"
        color="primary"
        variant="outlined"
        sx={{ minWidth: 50 }}
      />

      {/* Video Settings */}
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
        <Paper sx={{ p: 3, minWidth: 320 }}>
          <Typography variant="h6" gutterBottom>
            Video Settings
          </Typography>

          {/* Video Devices */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="body2" gutterBottom>
              Camera Device
            </Typography>
            <List dense>
              {videoDevices.map((device) => (
                <ListItem
                  key={device.deviceId}
                  button
                  selected={device.deviceId === selectedDevice}
                  onClick={() => onDeviceChange(device.deviceId)}
                >
                  <ListItemIcon>
                    <Videocam />
                  </ListItemIcon>
                  <ListItemText primary={device.label || 'Default Camera'} />
                </ListItem>
              ))}
            </List>
          </Box>

          {/* Video Quality */}
          <Box sx={{ mb: 3 }}>
            <FormControl fullWidth>
              <InputLabel>Video Quality</InputLabel>
              <Select
                value={videoQuality}
                onChange={(e) => onQualityChange(e.target.value)}
                label="Video Quality"
              >
                {qualityOptions.map((option) => (
                  <MenuItem key={option.value} value={option.value}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {option.icon}
                      {option.label}
                    </Box>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>

          {/* Brightness Control */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="body2" gutterBottom>
              Brightness
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Brightness4 />
              <Slider
                value={brightness}
                onChange={(e, value) => onBrightnessChange(value)}
                sx={{ flex: 1 }}
                min={0}
                max={100}
              />
            </Box>
          </Box>

          {/* Contrast Control */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="body2" gutterBottom>
              Contrast
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Contrast />
              <Slider
                value={contrast}
                onChange={(e, value) => onContrastChange(value)}
                sx={{ flex: 1 }}
                min={0}
                max={100}
              />
            </Box>
          </Box>

          {/* Video Effects */}
          <Box>
            <Typography variant="body2" gutterBottom>
              Video Effects
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={flipVideo}
                  onChange={onFlipVideoToggle}
                />
              }
              label="Flip Video Horizontally"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={backgroundBlur}
                  onChange={onBackgroundBlurToggle}
                />
              }
              label="Background Blur"
            />
          </Box>

          {/* Active Effects */}
          <Box sx={{ mt: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
            {flipVideo && (
              <Chip
                label="Flipped"
                size="small"
                color="primary"
              />
            )}
            {backgroundBlur && (
              <Chip
                label="Background Blur"
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

export default VideoControls;