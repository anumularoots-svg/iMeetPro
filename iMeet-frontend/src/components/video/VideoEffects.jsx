// components/video/VideoEffects.jsx
import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  IconButton,
  Menu,
  MenuItem,
  Typography,
  Tooltip,
  Slider,
  Paper,
  Grid,
  Switch,
  FormControlLabel,
  Divider,
  Button,
  ListItemIcon,
  ListItemText,
  Chip,
  Alert
} from '@mui/material';
import {
  Face as FaceIcon,
  Blur as BlurIcon,
  Brightness6 as BrightnessIcon,
  Contrast as ContrastIcon,
  AutoFixHigh as AutoFixHighIcon,
  FilterVintage as FilterVintageIcon,
  Palette as PaletteIcon,
  CameraAlt as CameraAltIcon,
  Flare as FlareIcon,
  Transform as TransformIcon,
  PhotoFilter as PhotoFilterIcon,
  Tune as TuneIcon
} from '@mui/icons-material';

const VideoEffects = ({ 
  currentStream, 
  onEffectChange, 
  isEnabled = true 
}) => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [backgroundBlur, setBackgroundBlur] = useState(false);
  const [virtualBackground, setVirtualBackground] = useState(null);
  const [brightness, setBrightness] = useState(100);
  const [contrast, setContrast] = useState(100);
  const [saturation, setSaturation] = useState(100);
  const [beauty, setBeauty] = useState(0);
  const [activeFilter, setActiveFilter] = useState(null);
  const [loading, setLoading] = useState(false);
  const canvasRef = useRef(null);
  const videoRef = useRef(null);

  const backgroundOptions = [
    {
      id: 'none',
      name: 'No Background',
      type: 'none',
      preview: null
    },
    {
      id: 'blur',
      name: 'Blur Background',
      type: 'blur',
      preview: null
    },
    {
      id: 'office',
      name: 'Modern Office',
      type: 'image',
      preview: '/backgrounds/office.jpg'
    },
    {
      id: 'home',
      name: 'Home Library',
      type: 'image',
      preview: '/backgrounds/library.jpg'
    },
    {
      id: 'nature',
      name: 'Nature Scene',
      type: 'image',
      preview: '/backgrounds/nature.jpg'
    },
    {
      id: 'abstract',
      name: 'Abstract Gradient',
      type: 'gradient',
      preview: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
    }
  ];

  const filterOptions = [
    {
      id: 'none',
      name: 'No Filter',
      icon: <PhotoFilterIcon />,
      css: 'none'
    },
    {
      id: 'warm',
      name: 'Warm',
      icon: <FlareIcon />,
      css: 'sepia(0.3) saturate(1.4) hue-rotate(10deg)'
    },
    {
      id: 'cool',
      name: 'Cool',
      icon: <FilterVintageIcon />,
      css: 'saturate(1.2) hue-rotate(180deg) brightness(1.1)'
    },
    {
      id: 'vintage',
      name: 'Vintage',
      icon: <CameraAltIcon />,
      css: 'sepia(0.5) contrast(1.2) brightness(1.1)'
    },
    {
      id: 'bw',
      name: 'Black & White',
      icon: <ContrastIcon />,
      css: 'grayscale(1) contrast(1.2)'
    }
  ];

  useEffect(() => {
    if (currentStream && videoRef.current) {
      videoRef.current.srcObject = currentStream;
    }
  }, [currentStream]);

  const handleMenuOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
  };

  const applyVideoEffects = () => {
    if (!canvasRef.current || !videoRef.current) return;

    const canvas = canvasRef.current;
    const video = videoRef.current;
    const ctx = canvas.getContext('2d');

    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;

    // Apply filters
    let filterString = '';
    
    if (brightness !== 100) {
      filterString += `brightness(${brightness}%) `;
    }
    if (contrast !== 100) {
      filterString += `contrast(${contrast}%) `;
    }
    if (saturation !== 100) {
      filterString += `saturate(${saturation}%) `;
    }
    if (activeFilter && activeFilter.css !== 'none') {
      filterString += activeFilter.css;
    }

    ctx.filter = filterString;
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);

    // Apply beauty effect (simple skin smoothing simulation)
    if (beauty > 0) {
      const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
      // Simple beauty filter implementation would go here
      ctx.putImageData(imageData, 0, 0);
    }

    // Get new stream from canvas
    const newStream = canvas.captureStream(30);
    onEffectChange && onEffectChange(newStream);
  };

  const handleBackgroundChange = async (background) => {
    setLoading(true);
    try {
      if (background.type === 'blur') {
        setBackgroundBlur(true);
        setVirtualBackground(null);
      } else if (background.type === 'image' || background.type === 'gradient') {
        setBackgroundBlur(false);
        setVirtualBackground(background);
      } else {
        setBackgroundBlur(false);
        setVirtualBackground(null);
      }
      
      // Apply background effect
      await applyBackgroundEffect(background);
      
    } catch (error) {
      console.error('Failed to apply background:', error);
    } finally {
      setLoading(false);
    }
  };

  const applyBackgroundEffect = async (background) => {
    // In a real implementation, this would use libraries like:
    // - MediaPipe for background segmentation
    // - TensorFlow.js for person detection
    // - Canvas API for background replacement
    
    console.log('Applying background effect:', background);
    // Placeholder for background effect implementation
  };

  const handleFilterChange = (filter) => {
    setActiveFilter(filter);
    applyVideoEffects();
  };

  const resetAllEffects = () => {
    setBrightness(100);
    setContrast(100);
    setSaturation(100);
    setBeauty(0);
    setActiveFilter(null);
    setBackgroundBlur(false);
    setVirtualBackground(null);
    onEffectChange && onEffectChange(currentStream);
  };

  return (
    <>
      {/* Effects Button */}
      <Tooltip title="Video Effects">
        <IconButton
          onClick={handleMenuOpen}
          disabled={!isEnabled}
          sx={{
            background: 'linear-gradient(45deg, #9c27b0, #673ab7)',
            color: 'white',
            width: 48,
            height: 48,
            '&:hover': {
              background: 'linear-gradient(45deg, #673ab7, #9c27b0)',
              transform: 'scale(1.05)'
            },
            '&:disabled': {
              background: 'rgba(255,255,255,0.1)',
              color: 'rgba(255,255,255,0.3)'
            },
            transition: 'all 0.3s ease'
          }}
        >
          <AutoFixHighIcon />
        </IconButton>
      </Tooltip>

      {/* Hidden video and canvas for processing */}
      <video
        ref={videoRef}
        style={{ display: 'none' }}
        autoPlay
        muted
      />
      <canvas
        ref={canvasRef}
        style={{ display: 'none' }}
      />

      {/* Effects Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        PaperProps={{
          sx: {
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            color: 'white',
            borderRadius: 3,
            minWidth: 400,
            maxHeight: 700,
            overflow: 'auto'
          }
        }}
      >
        {/* Header */}
        <Box sx={{ p: 2, borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <AutoFixHighIcon />
            Video Effects
          </Typography>
          {loading && (
            <Typography variant="body2" sx={{ opacity: 0.7, mt: 1 }}>
              Applying effects...
            </Typography>
          )}
        </Box>

        {/* Background Effects */}
        <Box sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <BlurIcon />
            Background
          </Typography>
          
          <Grid container spacing={1}>
            {backgroundOptions.map((bg) => (
              <Grid item xs={4} key={bg.id}>
                <Paper
                  onClick={() => handleBackgroundChange(bg)}
                  sx={{
                    p: 1,
                    textAlign: 'center',
                    cursor: 'pointer',
                    background: bg.type === 'gradient' ? bg.preview : 'rgba(255,255,255,0.1)',
                    border: (virtualBackground?.id === bg.id || (bg.id === 'blur' && backgroundBlur)) 
                      ? '2px solid #4caf50' : '1px solid rgba(255,255,255,0.2)',
                    borderRadius: 2,
                    height: 60,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    '&:hover': {
                      background: bg.type === 'gradient' ? bg.preview : 'rgba(255,255,255,0.2)',
                      transform: 'scale(1.02)'
                    },
                    transition: 'all 0.3s ease'
                  }}
                >
                  {bg.type === 'image' && bg.preview && (
                    <img 
                      src={bg.preview} 
                      alt={bg.name}
                      style={{ 
                        width: '100%', 
                        height: '100%', 
                        objectFit: 'cover',
                        borderRadius: 4
                      }}
                    />
                  )}
                  {bg.type === 'blur' && <BlurIcon />}
                  {bg.type === 'none' && <Typography variant="caption">None</Typography>}
                  <Typography 
                    variant="caption" 
                    sx={{ 
                      position: 'absolute',
                      bottom: 2,
                      fontSize: '0.7rem',
                      background: 'rgba(0,0,0,0.7)',
                      px: 0.5,
                      borderRadius: 1
                    }}
                  >
                    {bg.name}
                  </Typography>
                </Paper>
              </Grid>
            ))}
          </Grid>
        </Box>

        <Divider sx={{ borderColor: 'rgba(255,255,255,0.1)' }} />

        {/* Color Filters */}
        <Box sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <PaletteIcon />
            Color Filters
          </Typography>
          
          <Grid container spacing={1}>
            {filterOptions.map((filter) => (
              <Grid item xs={6} key={filter.id}>
                <MenuItem
                  onClick={() => handleFilterChange(filter)}
                  sx={{
                    borderRadius: 2,
                    background: activeFilter?.id === filter.id ? 'rgba(255,255,255,0.2)' : 'transparent',
                    '&:hover': { background: 'rgba(255,255,255,0.1)' }
                  }}
                >
                  <ListItemIcon sx={{ color: 'white' }}>
                    {filter.icon}
                  </ListItemIcon>
                  <ListItemText primary={filter.name} />
                  {activeFilter?.id === filter.id && (
                    <Chip label="Active" size="small" sx={{ backgroundColor: '#4caf50', color: 'white' }} />
                  )}
                </MenuItem>
              </Grid>
            ))}
          </Grid>
        </Box>

        <Divider sx={{ borderColor: 'rgba(255,255,255,0.1)' }} />

        {/* Adjustment Controls */}
        <Box sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <TuneIcon />
            Adjustments
          </Typography>

          {/* Brightness */}
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <BrightnessIcon fontSize="small" />
              Brightness: {brightness}%
            </Typography>
            <Slider
              value={brightness}
              onChange={(e, value) => {
                setBrightness(value);
                applyVideoEffects();
              }}
              min={50}
              max={150}
              sx={{
                color: '#ffeb3b',
                '& .MuiSlider-track': { background: 'linear-gradient(90deg, #f57f17, #ffeb3b)' }
              }}
            />
          </Box>

          {/* Contrast */}
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <ContrastIcon fontSize="small" />
              Contrast: {contrast}%
            </Typography>
            <Slider
              value={contrast}
              onChange={(e, value) => {
                setContrast(value);
                applyVideoEffects();
              }}
              min={50}
              max={150}
              sx={{
                color: '#607d8b',
                '& .MuiSlider-track': { background: 'linear-gradient(90deg, #37474f, #607d8b)' }
              }}
            />
          </Box>

          {/* Saturation */}
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <PaletteIcon fontSize="small" />
              Saturation: {saturation}%
            </Typography>
            <Slider
              value={saturation}
              onChange={(e, value) => {
                setSaturation(value);
                applyVideoEffects();
              }}
              min={0}
              max={200}
              sx={{
                color: '#e91e63',
                '& .MuiSlider-track': { background: 'linear-gradient(90deg, #ad1457, #e91e63)' }
              }}
            />
          </Box>

          {/* Beauty Effect */}
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <FaceIcon fontSize="small" />
              Beauty Effect: {beauty}%
            </Typography>
            <Slider
              value={beauty}
              onChange={(e, value) => {
                setBeauty(value);
                applyVideoEffects();
              }}
              min={0}
              max={100}
              sx={{
                color: '#ff9800',
                '& .MuiSlider-track': { background: 'linear-gradient(90deg, #f57c00, #ff9800)' }
              }}
            />
          </Box>
        </Box>

        {/* Action Buttons */}
        <Box sx={{ p: 2, display: 'flex', gap: 1, borderTop: '1px solid rgba(255,255,255,0.1)' }}>
          <Button
            variant="outlined"
            onClick={resetAllEffects}
            sx={{
              color: 'white',
              borderColor: 'rgba(255,255,255,0.3)',
              '&:hover': { borderColor: 'white', background: 'rgba(255,255,255,0.1)' }
            }}
          >
            Reset All
          </Button>
          
          <Button
            variant="contained"
            onClick={handleMenuClose}
            sx={{
              background: 'linear-gradient(45deg, #4caf50, #388e3c)',
              '&:hover': { background: 'linear-gradient(45deg, #388e3c, #4caf50)' }
            }}
          >
            Apply
          </Button>
        </Box>

        {/* Warning for performance */}
        <Alert 
          severity="info" 
          sx={{ 
            m: 2,
            borderRadius: 2,
            background: 'rgba(33,150,243,0.1)',
            border: '1px solid rgba(33,150,243,0.3)',
            color: '#2196f3'
          }}
        >
          <Typography variant="body2">
            Video effects may impact performance on slower devices. Disable effects if you experience issues.
          </Typography>
        </Alert>
      </Menu>
    </>
  );
};

export default VideoEffects;