// src/components/auth/CameraCapture.jsx
import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  Typography,
  Stack,
  Alert,
  alpha,
  useTheme,
  Fade,
  Zoom
} from '@mui/material';
import {
  CameraAlt,
  Close,
  Refresh,
  CheckCircle,
  PhotoCamera,
  Videocam,
  VideocamOff
} from '@mui/icons-material';

const CameraCapture = ({ open, onClose, onCapture, currentPhoto }) => {
  const theme = useTheme();
  const videoRef = useRef(null);
  const canvasRef = useRef(null);
  const [stream, setStream] = useState(null);
  const [capturedImage, setCapturedImage] = useState(currentPhoto || null);
  const [error, setError] = useState('');
  const [isCameraActive, setIsCameraActive] = useState(false);
  const [isCapturing, setIsCapturing] = useState(false);

  useEffect(() => {
    if (open && !capturedImage) {
      startCamera();
    }
    
    return () => {
      stopCamera();
    };
  }, [open]);

  useEffect(() => {
    // Update captured image when currentPhoto changes
    if (currentPhoto) {
      setCapturedImage(currentPhoto);
    }
  }, [currentPhoto]);

  const startCamera = async () => {
    try {
      setError('');
      setIsCameraActive(false);
      
      // Check if getUserMedia is supported
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        setError('Camera access is not supported in this browser. Please use a modern browser like Chrome, Firefox, or Safari.');
        return;
      }

      const mediaStream = await navigator.mediaDevices.getUserMedia({
        video: {
          width: { ideal: 1280 },
          height: { ideal: 720 },
          facingMode: 'user'
        },
        audio: false
      });
      
      if (videoRef.current) {
        videoRef.current.srcObject = mediaStream;
        
        // Wait for video to be ready
        videoRef.current.onloadedmetadata = () => {
          videoRef.current.play();
          setStream(mediaStream);
          setIsCameraActive(true);
        };
      }
    } catch (err) {
      console.error('Camera access error:', err);
      
      if (err.name === 'NotAllowedError' || err.name === 'PermissionDeniedError') {
        setError('Camera access denied. Please allow camera permissions in your browser settings and try again.');
      } else if (err.name === 'NotFoundError' || err.name === 'DevicesNotFoundError') {
        setError('No camera found. Please connect a camera and try again.');
      } else if (err.name === 'NotReadableError' || err.name === 'TrackStartError') {
        setError('Camera is already in use by another application. Please close other applications and try again.');
      } else {
        setError('Unable to access camera. Please check your camera permissions and try again.');
      }
    }
  };

  const stopCamera = () => {
    if (stream) {
      stream.getTracks().forEach(track => {
        track.stop();
      });
      setStream(null);
      setIsCameraActive(false);
    }
    
    if (videoRef.current) {
      videoRef.current.srcObject = null;
    }
  };

  const capturePhoto = () => {
    if (!videoRef.current || !canvasRef.current) {
      setError('Camera not ready. Please wait a moment and try again.');
      return;
    }

    try {
      setIsCapturing(true);
      
      const video = videoRef.current;
      const canvas = canvasRef.current;
      
      // Set canvas dimensions to match video
      canvas.width = video.videoWidth;
      canvas.height = video.videoHeight;
      
      // Draw the current video frame to canvas
      const context = canvas.getContext('2d');
      
      // Flip horizontally to match mirror view
      context.translate(canvas.width, 0);
      context.scale(-1, 1);
      context.drawImage(video, 0, 0, canvas.width, canvas.height);
      
      // Convert canvas to base64 image
      const imageData = canvas.toDataURL('image/jpeg', 0.8);
      
      // Small delay for capture animation
      setTimeout(() => {
        setCapturedImage(imageData);
        stopCamera();
        setIsCapturing(false);
      }, 300);
      
    } catch (err) {
      console.error('Error capturing photo:', err);
      setError('Failed to capture photo. Please try again.');
      setIsCapturing(false);
    }
  };

  const retakePhoto = () => {
    setCapturedImage(null);
    setError('');
    startCamera();
  };

  const handleConfirm = () => {
    if (capturedImage) {
      onCapture(capturedImage);
      handleClose();
    }
  };

  const handleClose = () => {
    stopCamera();
    setCapturedImage(null);
    setError('');
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="md"
      fullWidth
      TransitionComponent={Zoom}
      TransitionProps={{ timeout: 300 }}
      PaperProps={{
        sx: {
          borderRadius: 3,
          overflow: 'hidden',
        }
      }}
    >

      <DialogContent 
        sx={{ 
          p: 3, 
          backgroundColor: alpha(theme.palette.background.default, 0.5)
        }}
      >
        {error && (
          <Fade in>
            <Alert 
              severity="error" 
              sx={{ 
                mb: 2, 
                borderRadius: 2,
                '& .MuiAlert-message': {
                  width: '100%'
                }
              }}
              action={
                !capturedImage && !isCameraActive && (
                  <Button 
                    color="inherit" 
                    size="small" 
                    onClick={startCamera}
                    sx={{ fontWeight: 600 }}
                  >
                    Retry
                  </Button>
                )
              }
            >
              {error}
            </Alert>
          </Fade>
        )}

        <Box 
          sx={{ 
            paddingTop: '60%', 
            borderRadius: 2, 
          }}
        >
          {!capturedImage ? (
            <>
              <video
                ref={videoRef}
                autoPlay
                playsInline
                muted
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: '100%',
                  objectFit: 'cover',
                  transform: 'scaleX(-1)', // Mirror effect
                  display: isCameraActive ? 'block' : 'none'
                }}
              />
              
              {/* Loading/Camera Status */}
              {!isCameraActive && !error && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: '100%',
                    height: '100%',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    backgroundColor: 'rgba(0,0,0,0.8)',
                    color: 'white'
                  }}
                >
                  <Videocam sx={{ fontSize: 60, mb: 2, opacity: 0.5 }} />
                  <Typography variant="body1">
                    Starting camera...
                  </Typography>
                </Box>
              )}
              
              {/* Camera Inactive with Error */}
              {!isCameraActive && error && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: '100%',
                    height: '100%',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    backgroundColor: 'rgba(0,0,0,0.9)',
                    color: 'white'
                  }}
                >
                  <VideocamOff sx={{ fontSize: 60, mb: 2, opacity: 0.5 }} />
                  <Typography variant="body1">
                    Camera unavailable
                  </Typography>
                </Box>
              )}
              
              {/* Face Guide Overlay */}
              {isCameraActive && !isCapturing && (
                <Fade in timeout={500}>
                  <Box
                    sx={{
                      position: 'absolute',
                      top: '50%',
                      left: '50%',
                      transform: 'translate(-50%, -50%)',
                      width: '60%',
                      height: '80%',
                      border: `3px solid ${alpha(theme.palette.common.white, 0.5)}`,
                      borderRadius: '50%',
                      pointerEvents: 'none',
                      boxShadow: `inset 0 0 60px rgba(0,0,0,0.5), 0 0 20px ${alpha(theme.palette.primary.main, 0.3)}`
                    }}
                  />
                </Fade>
              )}
              
              {/* Capture Flash Effect */}
              {isCapturing && (
                <Box
                  sx={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: '100%',
                    height: '100%',
                    backgroundColor: 'white',
                    animation: 'flash 0.3s ease-out',
                    '@keyframes flash': {
                      '0%': { opacity: 0 },
                      '50%': { opacity: 0.8 },
                      '100%': { opacity: 0 }
                    }
                  }}
                />
              )}
            </>
          ) : (
            <Fade in>
              <img
                src={capturedImage}
                alt="Captured"
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: '100%',
                  objectFit: 'cover'
                }}
              />
            </Fade>
          )}
        </Box>

        <canvas ref={canvasRef} style={{ display: 'none' }} />

        {/* Instructions */}
        {!capturedImage && isCameraActive && (
          <Fade in>
            <Typography 
              variant="body2" 
              color="text.secondary" 
              textAlign="center" 
              mt={2}
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 1
              }}
            >
              <Videocam sx={{ fontSize: 18 }} />
              Position your face within the circle and click capture
            </Typography>
          </Fade>
        )}
        
        {capturedImage && (
          <Fade in>
            <Typography 
              variant="body2" 
              color="success.main" 
              textAlign="center" 
              mt={2}
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 1,
                fontWeight: 600
              }}
            >
              <CheckCircle sx={{ fontSize: 18 }} />
              Photo captured successfully! Confirm to use this photo.
            </Typography>
          </Fade>
        )}
      </DialogContent>

      <DialogActions 
        sx={{ 
          p: 3, 
          gap: 2,
          backgroundColor: alpha(theme.palette.background.default, 0.3)
        }}
      >
        {!capturedImage ? (
          <>
            <Button
              onClick={handleClose}
              variant="outlined"
              sx={{ 
                borderRadius: 2, 
                px: 3,
                py: 1.2,
                textTransform: 'none',
                fontWeight: 600,
                borderColor: alpha(theme.palette.primary.main, 0.3),
                '&:hover': {
                  borderColor: theme.palette.primary.main,
                  backgroundColor: alpha(theme.palette.primary.main, 0.05)
                }
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={capturePhoto}
              variant="contained"
              disabled={!isCameraActive || isCapturing}
              startIcon={<CameraAlt />}
              sx={{
                borderRadius: 2,
                px: 4,
                py: 1.2,
                textTransform: 'none',
                fontWeight: 600,
                background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.secondary.main})`,
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                '&:hover': {
                  boxShadow: '0 6px 16px rgba(0,0,0,0.25)',
                  transform: 'translateY(-1px)'
                },
                '&:disabled': {
                  background: theme.palette.grey[300],
                  color: theme.palette.grey[500]
                }
              }}
            >
              {isCapturing ? 'Capturing...' : 'Capture Photo'}
            </Button>
          </>
        ) : (
          <>
            <Button
              onClick={retakePhoto}
              variant="outlined"
              startIcon={<Refresh />}
              sx={{ 
                borderRadius: 2, 
                px: 3,
                py: 1.2,
                textTransform: 'none',
                fontWeight: 600,
                borderColor: alpha(theme.palette.primary.main, 0.3),
                '&:hover': {
                  borderColor: theme.palette.primary.main,
                  backgroundColor: alpha(theme.palette.primary.main, 0.05)
                }
              }}
            >
              Retake Photo
            </Button>
            <Button
              onClick={handleConfirm}
              variant="contained"
              startIcon={<CheckCircle />}
              sx={{
                borderRadius: 2,
                px: 4,
                py: 1.2,
                textTransform: 'none',
                fontWeight: 600,
                background: `linear-gradient(135deg, ${theme.palette.success.main}, ${theme.palette.success.dark})`,
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                '&:hover': {
                  boxShadow: '0 6px 16px rgba(0,0,0,0.25)',
                  transform: 'translateY(-1px)'
                }
              }}
            >
              Use This Photo
            </Button>
          </>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default CameraCapture;