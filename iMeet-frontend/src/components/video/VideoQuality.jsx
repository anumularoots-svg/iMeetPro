// components/video/VideoQuality.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  IconButton,
  Menu,
  MenuItem,
  Typography,
  Tooltip,
  Slider,
  Paper,
  Chip,
  ListItemIcon,
  ListItemText,
  Divider,
  Switch,
  FormControlLabel,
  Alert
} from '@mui/material';
import {
  Settings as SettingsIcon,
  HighQuality as HighQualityIcon,
  Hd as HdIcon,
  Sd as SdIcon,
  SignalWifi4Bar as SignalWifi4BarIcon,
  SignalWifi3Bar as SignalWifi3BarIcon,
  SignalWifi2Bar as SignalWifi2BarIcon,
  SignalWifi1Bar as SignalWifi1BarIcon,
  SignalWifiOff as SignalWifiOffIcon,
  Tune as TuneIcon,
  Speed as SpeedIcon,
  Visibility as VisibilityIcon,
  GraphicEq as GraphicEqIcon
} from '@mui/icons-material';

const VideoQuality = ({ 
  currentStream, 
  onQualityChange, 
  onFrameRateChange,
  currentQuality = 'auto',
  connectionQuality = 'good',
  bandwidth = 0 
}) => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [selectedQuality, setSelectedQuality] = useState(currentQuality);
  const [frameRate, setFrameRate] = useState(30);
  const [adaptiveQuality, setAdaptiveQuality] = useState(true);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [bitrate, setBitrate] = useState(1000);
  const [stats, setStats] = useState({
    resolution: '1280x720',
    fps: 30,
    bitrate: 1000,
    packetLoss: 0
  });

  const qualityPresets = [
    {
      id: 'auto',
      label: 'Auto (Recommended)',
      description: 'Automatically adjust based on connection',
      icon: <TuneIcon />,
      resolution: 'adaptive',
      bitrate: 'adaptive'
    },
    {
      id: 'hd',
      label: 'HD (1080p)',
      description: '1920x1080, 30fps',
      icon: <HighQualityIcon />,
      resolution: '1920x1080',
      bitrate: 2500
    },
    {
      id: 'hd720',
      label: 'HD (720p)',
      description: '1280x720, 30fps',
      icon: <HdIcon />,
      resolution: '1280x720',
      bitrate: 1500
    },
    {
      id: 'sd',
      label: 'SD (480p)',
      description: '854x480, 30fps',
      icon: <SdIcon />,
      resolution: '854x480',
      bitrate: 800
    },
    {
      id: 'low',
      label: 'Low (360p)',
      description: '640x360, 24fps',
      icon: <SdIcon />,
      resolution: '640x360',
      bitrate: 400
    }
  ];

  useEffect(() => {
    // Simulate getting real-time stats
    const interval = setInterval(() => {
      if (currentStream) {
        // In a real implementation, you'd get actual WebRTC stats
        setStats(prev => ({
          ...prev,
          packetLoss: Math.random() * 2, // 0-2% packet loss
          bitrate: bitrate + (Math.random() - 0.5) * 200
        }));
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [currentStream, bitrate]);

  const handleMenuOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
  };

  const handleQualitySelect = async (quality) => {
    setSelectedQuality(quality.id);
    
    if (quality.id === 'auto') {
      setAdaptiveQuality(true);
    } else {
      setAdaptiveQuality(false);
      // Apply quality settings
      await applyQualitySettings(quality);
    }
    
    onQualityChange && onQualityChange(quality);
    handleMenuClose();
  };

  const applyQualitySettings = async (quality) => {
    if (!currentStream) return;

    try {
      const videoTrack = currentStream.getVideoTracks()[0];
      if (!videoTrack) return;

      const constraints = {
        width: { ideal: parseInt(quality.resolution.split('x')[0]) },
        height: { ideal: parseInt(quality.resolution.split('x')[1]) },
        frameRate: { ideal: frameRate }
      };

      await videoTrack.applyConstraints(constraints);
      setBitrate(quality.bitrate);
      
    } catch (error) {
      console.error('Failed to apply quality settings:', error);
    }
  };

  const handleFrameRateChange = (event, newValue) => {
    setFrameRate(newValue);
    onFrameRateChange && onFrameRateChange(newValue);
  };

  const getConnectionIcon = () => {
    switch (connectionQuality) {
      case 'excellent': return <SignalWifi4BarIcon sx={{ color: '#4caf50' }} />;
      case 'good': return <SignalWifi3BarIcon sx={{ color: '#8bc34a' }} />;
      case 'fair': return <SignalWifi2BarIcon sx={{ color: '#ff9800' }} />;
      case 'poor': return <SignalWifi1BarIcon sx={{ color: '#f44336' }} />;
      default: return <SignalWifiOffIcon sx={{ color: '#9e9e9e' }} />;
    }
  };

  const getQualityColor = () => {
    switch (connectionQuality) {
      case 'excellent': return '#4caf50';
      case 'good': return '#8bc34a';
      case 'fair': return '#ff9800';
      case 'poor': return '#f44336';
      default: return '#9e9e9e';
    }
  };

  const formatBandwidth = (bw) => {
    if (bw < 1000) return `${bw} kbps`;
    return `${(bw / 1000).toFixed(1)} Mbps`;
  };

  return (
    <>
      {/* Quality Control Button */}
      <Tooltip title="Video Quality Settings">
        <IconButton
          onClick={handleMenuOpen}
          sx={{
            background: 'rgba(255,255,255,0.1)',
            color: 'white',
            backdropFilter: 'blur(10px)',
            '&:hover': {
              background: 'rgba(255,255,255,0.2)',
              transform: 'scale(1.05)'
            },
            transition: 'all 0.3s ease'
          }}
        >
          <SettingsIcon />
        </IconButton>
      </Tooltip>

      {/* Connection Quality Indicator */}
      <Chip
        icon={getConnectionIcon()}
        label={`${connectionQuality.toUpperCase()} (${formatBandwidth(bandwidth)})`}
        size="small"
        sx={{
          background: 'rgba(255,255,255,0.1)',
          color: 'white',
          backdropFilter: 'blur(10px)',
          ml: 1,
          '& .MuiChip-icon': {
            color: getQualityColor()
          }
        }}
      />

      {/* Quality Settings Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        PaperProps={{
          sx: {
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            color: 'white',
            borderRadius: 3,
            minWidth: 350,
            maxHeight: 600,
            overflow: 'auto'
          }
        }}
      >
        {/* Header */}
        <Box sx={{ p: 2, borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <SettingsIcon />
            Video Quality Settings
          </Typography>
        </Box>

        {/* Current Stats */}
        <Box sx={{ p: 2, background: 'rgba(255,255,255,0.1)' }}>
          <Typography variant="subtitle2" gutterBottom>
            Current Status
          </Typography>
          <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1, fontSize: '0.875rem' }}>
            <Typography variant="body2">Resolution: {stats.resolution}</Typography>
            <Typography variant="body2">FPS: {stats.fps}</Typography>
            <Typography variant="body2">Bitrate: {Math.round(stats.bitrate)} kbps</Typography>
            <Typography variant="body2">Packet Loss: {stats.packetLoss.toFixed(1)}%</Typography>
          </Box>
        </Box>

        {/* Adaptive Quality Toggle */}
        <Box sx={{ p: 2 }}>
          <FormControlLabel
            control={
              <Switch
                checked={adaptiveQuality}
                onChange={(e) => setAdaptiveQuality(e.target.checked)}
                sx={{
                  '& .MuiSwitch-switchBase.Mui-checked': {
                    color: '#4caf50',
                  },
                  '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
                    backgroundColor: '#4caf50',
                  }
                }}
              />
            }
            label="Adaptive Quality (Recommended)"
            sx={{ color: 'white' }}
          />
          <Typography variant="body2" sx={{ opacity: 0.7, ml: 4 }}>
            Automatically adjusts quality based on network conditions
          </Typography>
        </Box>

        <Divider sx={{ borderColor: 'rgba(255,255,255,0.1)' }} />

        {/* Quality Presets */}
        <Box sx={{ p: 1 }}>
          <Typography variant="subtitle2" sx={{ p: 1, opacity: 0.8 }}>
            Quality Presets
          </Typography>
          {qualityPresets.map((quality) => (
            <MenuItem
              key={quality.id}
              onClick={() => handleQualitySelect(quality)}
              disabled={adaptiveQuality && quality.id !== 'auto'}
              sx={{
                borderRadius: 2,
                m: 1,
                background: selectedQuality === quality.id ? 'rgba(255,255,255,0.2)' : 'transparent',
                '&:hover': {
                  background: 'rgba(255,255,255,0.1)',
                  transform: 'translateX(4px)'
                },
                '&.Mui-disabled': {
                  opacity: 0.5
                },
                transition: 'all 0.3s ease'
              }}
            >
              <ListItemIcon sx={{ color: 'white' }}>
                {quality.icon}
              </ListItemIcon>
              <ListItemText
                primary={quality.label}
                secondary={quality.description}
                secondaryTypographyProps={{
                  sx: { color: 'rgba(255,255,255,0.7)' }
                }}
              />
              {selectedQuality === quality.id && (
                <Chip
                  label="Active"
                  size="small"
                  sx={{
                    background: 'linear-gradient(45deg, #4caf50, #388e3c)',
                    color: 'white',
                    fontWeight: 'bold'
                  }}
                />
              )}
            </MenuItem>
          ))}
        </Box>

        {/* Advanced Settings */}
        {!adaptiveQuality && (
          <>
            <Divider sx={{ borderColor: 'rgba(255,255,255,0.1)' }} />
            
            <Box sx={{ p: 2 }}>
              <Typography variant="subtitle2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <TuneIcon fontSize="small" />
                Advanced Settings
              </Typography>

              {/* Frame Rate Slider */}
              <Box sx={{ mt: 2 }}>
                <Typography variant="body2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <SpeedIcon fontSize="small" />
                  Frame Rate: {frameRate} FPS
                </Typography>
                <Slider
                  value={frameRate}
                  onChange={handleFrameRateChange}
                  min={15}
                  max={60}
                  step={5}
                  marks={[
                    { value: 15, label: '15' },
                    { value: 30, label: '30' },
                    { value: 60, label: '60' }
                  ]}
                  sx={{
                    color: '#4caf50',
                    '& .MuiSlider-track': {
                      background: 'linear-gradient(90deg, #4caf50, #8bc34a)'
                    },
                    '& .MuiSlider-thumb': {
                      background: 'linear-gradient(45deg, #4caf50, #388e3c)'
                    }
                  }}
                />
              </Box>

              {/* Bitrate Slider */}
              <Box sx={{ mt: 3 }}>
                <Typography variant="body2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <GraphicEqIcon fontSize="small" />
                  Target Bitrate: {bitrate} kbps
                </Typography>
                <Slider
                  value={bitrate}
                  onChange={(e, newValue) => setBitrate(newValue)}
                  min={200}
                  max={5000}
                  step={100}
                  marks={[
                    { value: 200, label: '200k' },
                    { value: 1000, label: '1M' },
                    { value: 2500, label: '2.5M' },
                    { value: 5000, label: '5M' }
                  ]}
                  sx={{
                    color: '#2196f3',
                    '& .MuiSlider-track': {
                      background: 'linear-gradient(90deg, #2196f3, #03a9f4)'
                    },
                    '& .MuiSlider-thumb': {
                      background: 'linear-gradient(45deg, #2196f3, #1976d2)'
                    }
                  }}
                />
              </Box>
            </Box>
          </>
        )}

        {/* Network Recommendations */}
        {connectionQuality === 'poor' && (
          <Box sx={{ p: 2 }}>
            <Alert 
              severity="warning" 
              sx={{ 
                borderRadius: 2,
                background: 'rgba(255,152,0,0.1)',
                border: '1px solid rgba(255,152,0,0.3)',
                color: '#ff9800'
              }}
            >
              <Typography variant="body2">
                <strong>Poor Connection Detected</strong><br />
                Consider switching to a lower quality setting or enabling adaptive quality for better performance.
              </Typography>
            </Alert>
          </Box>
        )}

        {/* Action Buttons */}
        <Box sx={{ p: 2, display: 'flex', gap: 1, borderTop: '1px solid rgba(255,255,255,0.1)' }}>
          <IconButton
            onClick={() => setShowAdvanced(!showAdvanced)}
            sx={{
              background: 'rgba(255,255,255,0.1)',
              color: 'white',
              '&:hover': { background: 'rgba(255,255,255,0.2)' }
            }}
          >
            <TuneIcon />
          </IconButton>
          
          <IconButton
            onClick={handleMenuClose}
            sx={{
              background: 'linear-gradient(45deg, #4caf50, #388e3c)',
              color: 'white',
              '&:hover': { background: 'linear-gradient(45deg, #388e3c, #4caf50)' }
            }}
          >
            <VisibilityIcon />
          </IconButton>
        </Box>
      </Menu>
    </>
  );
};

export default VideoQuality;