import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Button,
  Avatar,
  IconButton,
  Typography,
  LinearProgress,
  Alert,
  Stack,
  useTheme,
  alpha,
  Fade,
  Backdrop,
  CircularProgress,
  Tooltip
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  PhotoCamera as CameraIcon,
  Delete as DeleteIcon,
  Edit as EditIcon,
  Close as CloseIcon,
  Image as ImageIcon
} from '@mui/icons-material';

const ImageUpload = ({
  value, // Can be base64 string or URL
  onChange,
  onError,
  variant = "avatar", // "avatar" | "banner" | "square"
  size = "large", // "small" | "medium" | "large"
  maxSize = 5 * 1024 * 1024, // 5MB
  acceptedFormats = ['image/jpeg', 'image/png', 'image/webp', 'image/jpg', 'image/gif'],
  showProgress = true,
  disabled = false,
  placeholder = "Upload Image",
  helperText = "",
  required = false,
  editable = true, // New prop to control if user can edit
  userName = "User", // For avatar fallback
  allowDelete = true // ✅ NEW: Control if delete button is shown
})  => {
  const theme = useTheme();
  const fileInputRef = useRef(null);
  const [preview, setPreview] = useState(value || null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState('');
  const [dragActive, setDragActive] = useState(false);

  // Update preview when value prop changes (from backend)
  useEffect(() => {
    if (value) {
      console.log('📸 ImageUpload received value:', value?.substring(0, 100));
      setPreview(value);
    }
  }, [value]);

  const getAvatarSize = () => {
    switch (size) {
      case 'small': return 64;
      case 'medium': return 96;
      case 'large': return 128;
      default: return 96;
    }
  };

  const getBannerHeight = () => {
    switch (size) {
      case 'small': return 120;
      case 'medium': return 160;
      case 'large': return 200;
      default: return 160;
    }
  };

  const validateFile = (file) => {
    if (!file) return "No file selected";
    
    if (file.size > maxSize) {
      return `File size must be less than ${(maxSize / (1024 * 1024)).toFixed(1)}MB`;
    }
    
    if (!acceptedFormats.includes(file.type)) {
      return `Please upload ${acceptedFormats.map(f => f.split('/')[1]).join(', ')} files only`;
    }
    
    return null;
  };

  const handleFileSelect = async (file) => {
    const validationError = validateFile(file);
    if (validationError) {
      setError(validationError);
      if (onError) onError(validationError);
      return;
    }

    setError('');
    setUploading(true);
    setProgress(0);

    try {
      // Create preview and convert to base64
      const reader = new FileReader();
      
      reader.onprogress = (e) => {
        if (e.lengthComputable) {
          const percentComplete = (e.loaded / e.total) * 100;
          setProgress(percentComplete);
        }
      };

      reader.onload = (e) => {
        const base64String = e.target.result;
        console.log('📸 Image converted to base64:', base64String?.substring(0, 100));
        setPreview(base64String);
        setProgress(100);
        
        // Call onChange with base64 string
        if (onChange) {
          onChange(base64String);
        }

        setTimeout(() => {
          setUploading(false);
          setProgress(0);
        }, 500);
      };

      reader.onerror = (error) => {
        console.error('❌ FileReader error:', error);
        setError('Failed to read file');
        if (onError) onError('Failed to read file');
        setUploading(false);
        setProgress(0);
      };

      reader.readAsDataURL(file);

    } catch (error) {
      console.error('❌ Image upload error:', error);
      setError(error.message || 'Upload failed');
      if (onError) onError(error.message);
      setUploading(false);
      setProgress(0);
    }
  };

  const handleInputChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      handleFileSelect(file);
    }
  };

  const handleDragEnter = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (!disabled && editable) {
      setDragActive(true);
    }
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (disabled || !editable) return;

    const files = e.dataTransfer.files;
    if (files && files[0]) {
      handleFileSelect(files[0]);
    }
  };

  const handleRemove = () => {
    setPreview(null);
    setError('');
    if (onChange) onChange(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const openFileDialog = () => {
    if (!disabled && editable && fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const renderAvatarUpload = () => (
    <Box position="relative" display="inline-block">
      <Tooltip title={editable && !disabled ? "Click to change photo" : ""} arrow>
        <Avatar
          src={preview}
          sx={{
            width: getAvatarSize(),
            height: getAvatarSize(),
            bgcolor: preview ? 'transparent' : alpha(theme.palette.primary.main, 0.1),
            border: dragActive 
              ? `3px dashed ${theme.palette.primary.main}` 
              : preview 
                ? `4px solid white`
                : `2px dashed ${alpha(theme.palette.divider, 0.5)}`,
            cursor: (disabled || !editable) ? 'default' : 'pointer',
            transition: 'all 0.3s ease',
            boxShadow: preview ? theme.shadows[4] : 'none',
            '&:hover': (!disabled && editable) ? {
              transform: 'scale(1.05)',
              boxShadow: theme.shadows[8],
              ...(dragActive && {
                borderColor: theme.palette.primary.main,
              })
            } : {},
            ...(dragActive && {
              '&::before': {
                content: '""',
                position: 'absolute',
                top: 0,
                left: 0,
                right: 0,
                bottom: 0,
                backgroundColor: alpha(theme.palette.primary.main, 0.2),
                borderRadius: '50%',
                zIndex: 1,
              }
            })
          }}
          onClick={openFileDialog}
          onDragEnter={handleDragEnter}
          onDragLeave={handleDragLeave}
          onDragOver={handleDragOver}
          onDrop={handleDrop}
        >
          {!preview && (
            <CameraIcon sx={{ fontSize: getAvatarSize() * 0.4, color: 'text.secondary' }} />
          )}
          {!preview && userName && (
            <Typography variant="h4" sx={{ color: 'white', fontWeight: 700 }}>
              {userName.charAt(0).toUpperCase()}
            </Typography>
          )}
        </Avatar>
      </Tooltip>

      {preview && !disabled && editable && (
        <>
          {/* Edit Button */}
          <Tooltip title="Change photo" arrow>
            <IconButton
              size="small"
              onClick={(e) => {
                e.stopPropagation();
                openFileDialog();
              }}
              sx={{
                position: 'absolute',
                bottom: 0,
                right: 0,
                bgcolor: theme.palette.primary.main,
                color: 'white',
                width: 32,
                height: 32,
                '&:hover': {
                  bgcolor: theme.palette.primary.dark,
                  transform: 'scale(1.1)',
                }
              }}
            >
              <EditIcon fontSize="small" />
            </IconButton>
          </Tooltip>

          {/* Remove Button */}
         {/* Remove Button - Only show if allowDelete is true */}
          {allowDelete && (
            <Tooltip title="Remove photo" arrow>
              <IconButton
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  handleRemove();
                }}
                sx={{
                  position: 'absolute',
                  top: -8,
                  right: -8,
                  bgcolor: theme.palette.error.main,
                  color: 'white',
                  width: 28,
                  height: 28,
                  '&:hover': {
                    bgcolor: theme.palette.error.dark,
                    transform: 'scale(1.1)',
                  }
                }}
              >
                <CloseIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          )}
        </>
      )}
    </Box>
  );

  const renderBannerUpload = () => (
    <Box
      sx={{
        width: '100%',
        height: getBannerHeight(),
        border: dragActive 
          ? `3px dashed ${theme.palette.primary.main}`
          : `2px dashed ${alpha(theme.palette.divider, 0.5)}`,
        borderRadius: 2,
        position: 'relative',
        overflow: 'hidden',
        cursor: (disabled || !editable) ? 'default' : 'pointer',
        bgcolor: preview ? 'transparent' : alpha(theme.palette.background.paper, 0.5),
        backgroundImage: preview ? `url(${preview})` : 'none',
        backgroundSize: 'cover',
        backgroundPosition: 'center',
        transition: 'all 0.3s ease',
        '&:hover': (!disabled && editable) ? {
          borderColor: theme.palette.primary.main,
          bgcolor: alpha(theme.palette.primary.main, 0.02),
        } : {},
      }}
      onClick={openFileDialog}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {!preview && (
        <Stack
          alignItems="center"
          justifyContent="center"
          height="100%"
          spacing={1}
          sx={{
            color: theme.palette.text.secondary,
          }}
        >
          <UploadIcon sx={{ fontSize: 48, opacity: 0.7 }} />
          <Typography variant="body2" textAlign="center" fontWeight={500}>
            {placeholder}
          </Typography>
          <Typography variant="caption" color="text.disabled">
            Drag & drop or click to upload
          </Typography>
        </Stack>
      )}

      {preview && !disabled && editable && (
        <Stack
          direction="row"
          spacing={1}
          sx={{
            position: 'absolute',
            top: 8,
            right: 8,
          }}
        >
          <Tooltip title="Change image" arrow>
            <IconButton
              onClick={(e) => {
                e.stopPropagation();
                openFileDialog();
              }}
              sx={{
                bgcolor: alpha(theme.palette.background.paper, 0.9),
                '&:hover': {
                  bgcolor: theme.palette.background.paper,
                }
              }}
            >
              <EditIcon />
            </IconButton>
          </Tooltip>
          
          {allowDelete && (
            <Tooltip title="Remove image" arrow>
              <IconButton
                onClick={(e) => {
                  e.stopPropagation();
                  handleRemove();
                }}
                sx={{
                  bgcolor: alpha(theme.palette.background.paper, 0.9),
                  '&:hover': {
                    bgcolor: theme.palette.error.main,
                    color: 'white',
                  }
                }}
              >
                <DeleteIcon />
              </IconButton>
            </Tooltip>
          )}
        </Stack>
      )}
    </Box>
  );

  return (
    <Box>
      <input
        ref={fileInputRef}
        type="file"
        accept={acceptedFormats.join(',')}
        onChange={handleInputChange}
        style={{ display: 'none' }}
        disabled={disabled || !editable}
      />

      {variant === 'avatar' ? renderAvatarUpload() : renderBannerUpload()}

      {/* Upload hint for editable mode */}
      {editable && !disabled && !preview && (
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{
            display: 'block',
            textAlign: 'center',
            mt: 1,
            fontWeight: 500,
          }}
        >
          Click or drag image to upload
        </Typography>
      )}

      {/* Progress Bar */}
      {showProgress && uploading && (
        <Box mt={2}>
          <LinearProgress
            variant="determinate"
            value={progress}
            sx={{
              borderRadius: 1,
              height: 6,
              bgcolor: alpha(theme.palette.primary.main, 0.1),
              '& .MuiLinearProgress-bar': {
                borderRadius: 1,
                background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
              }
            }}
          />
          <Typography variant="caption" color="text.secondary" mt={0.5} display="block" textAlign="center">
            Uploading... {Math.round(progress)}%
          </Typography>
        </Box>
      )}

      {/* Helper Text */}
      {helperText && !error && (
        <Typography variant="caption" color="text.secondary" mt={1} display="block" textAlign="center">
          {helperText}
        </Typography>
      )}

      {/* Error Message */}
      {error && (
        <Fade in={!!error}>
          <Alert 
            severity="error" 
            sx={{ mt: 1 }}
            onClose={() => setError('')}
          >
            {error}
          </Alert>
        </Fade>
      )}

      {/* Loading Backdrop */}
      <Backdrop
        sx={{ 
          color: '#fff', 
          zIndex: theme.zIndex.drawer + 1,
          bgcolor: alpha(theme.palette.background.default, 0.8)
        }}
        open={uploading}
      >
        <Stack alignItems="center" spacing={2}>
          <CircularProgress color="inherit" />
          <Typography variant="h6">Uploading...</Typography>
        </Stack>
      </Backdrop>
    </Box>
  );
};

export default ImageUpload;