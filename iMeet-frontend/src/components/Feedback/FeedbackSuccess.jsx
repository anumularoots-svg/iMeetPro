// src/components/feedback/FeedbackSuccess.jsx
import React, { useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Fade,
  Slide,
  Zoom,
  Stack,
  Chip
} from '@mui/material';
import {
  CheckCircle as CheckCircleIcon,
  ThumbUp as ThumbUpIcon,
  Home as HomeIcon,
  History as HistoryIcon,
  Star as StarIcon
} from '@mui/icons-material';

const FeedbackSuccess = ({ 
  feedbackId,
  rating,
  meetingTitle,
  onClose,
  autoCloseDelay = 3000 // Auto-close after 3 seconds
}) => {
  const [countdown, setCountdown] = useState(Math.floor(autoCloseDelay / 1000));
  const [showContent, setShowContent] = useState(false);

  useEffect(() => {
    // Trigger animation after mount
    const timer = setTimeout(() => setShowContent(true), 100);
    return () => clearTimeout(timer);
  }, []);

  useEffect(() => {
    // Countdown timer
    if (countdown > 0) {
      const timer = setTimeout(() => setCountdown(countdown - 1), 1000);
      return () => clearTimeout(timer);
    }
  }, [countdown]);

  useEffect(() => {
    // Auto-close
    if (autoCloseDelay > 0) {
      const timer = setTimeout(() => {
        onClose?.();
      }, autoCloseDelay);
      
      return () => clearTimeout(timer);
    }
  }, [autoCloseDelay, onClose]);

  const handleClose = () => {
    onClose?.();
  };

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: 400,
        p: 4,
        textAlign: 'center'
      }}
    >
      {/* Success Icon with Animation */}
      <Zoom in={showContent} timeout={500}>
        <Box
          sx={{
            position: 'relative',
            mb: 3
          }}
        >
          {/* Pulsing Background Circle */}
          <Box
            sx={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: 140,
              height: 140,
              borderRadius: '50%',
              bgcolor: 'success.lighter',
              animation: 'pulse 2s infinite',
              '@keyframes pulse': {
                '0%': {
                  transform: 'translate(-50%, -50%) scale(1)',
                  opacity: 0.5
                },
                '50%': {
                  transform: 'translate(-50%, -50%) scale(1.1)',
                  opacity: 0.3
                },
                '100%': {
                  transform: 'translate(-50%, -50%) scale(1)',
                  opacity: 0.5
                }
              }
            }}
          />
          
          {/* Success Icon */}
          <CheckCircleIcon
            sx={{
              fontSize: 100,
              color: 'success.main',
              position: 'relative',
              zIndex: 1,
              filter: 'drop-shadow(0 4px 8px rgba(0,0,0,0.1))'
            }}
          />
        </Box>
      </Zoom>

      {/* Success Message */}
      <Fade in={showContent} timeout={800}>
        <Box>
          <Typography 
            variant="h4" 
            fontWeight={700}
            color="success.main"
            gutterBottom
            sx={{ mb: 1 }}
          >
            Thank You!
          </Typography>
          
          <Typography 
            variant="h6" 
            color="text.secondary"
            gutterBottom
            sx={{ mb: 3 }}
          >
            Your feedback has been submitted successfully
          </Typography>

          {/* Feedback Details Card */}
          <Paper
            elevation={0}
            sx={{
              p: 3,
              mb: 3,
              bgcolor: 'background.default',
              border: 1,
              borderColor: 'divider',
              borderRadius: 2,
              maxWidth: 400,
              mx: 'auto'
            }}
          >
            <Stack spacing={2}>
              {/* Meeting Title */}
              <Box>
                <Typography variant="caption" color="text.secondary" display="block">
                  Meeting
                </Typography>
                <Typography variant="body1" fontWeight={600}>
                  {meetingTitle}
                </Typography>
              </Box>

              {/* Rating */}
              {rating && (
                <Box>
                  <Typography variant="caption" color="text.secondary" display="block" mb={0.5}>
                    Your Rating
                  </Typography>
                  <Box display="flex" alignItems="center" justifyContent="center" gap={0.5}>
                    {[...Array(5)].map((_, index) => (
                      <StarIcon
                        key={index}
                        sx={{
                          fontSize: 24,
                          color: index < rating ? 'warning.main' : 'action.disabled'
                        }}
                      />
                    ))}
                  </Box>
                </Box>
              )}

              {/* Feedback ID */}
              {feedbackId && (
                <Box>
                  <Typography variant="caption" color="text.secondary" display="block">
                    Reference ID
                  </Typography>
                  <Chip
                    label={`#${feedbackId}`}
                    size="small"
                    color="primary"
                    variant="outlined"
                  />
                </Box>
              )}
            </Stack>
          </Paper>

          {/* Appreciation Message */}
          <Paper
            elevation={0}
            sx={{
              p: 2,
              mb: 3,
              bgcolor: 'primary.lighter',
              border: 1,
              borderColor: 'primary.light',
              borderRadius: 2,
              maxWidth: 400,
              mx: 'auto'
            }}
          >
            <Box display="flex" alignItems="center" gap={1} justifyContent="center">
              <ThumbUpIcon color="primary" fontSize="small" />
              <Typography variant="body2" color="primary.dark">
                Your input helps us create better meeting experiences!
              </Typography>
            </Box>
          </Paper>

          {/* Auto-close Countdown */}
          {countdown > 0 && (
            <Fade in timeout={300}>
              <Typography variant="caption" color="text.secondary">
                Redirecting to dashboard in {countdown} second{countdown !== 1 ? 's' : ''}...
              </Typography>
            </Fade>
          )}
        </Box>
      </Fade>

      {/* Action Buttons */}
      <Slide in={showContent} direction="up" timeout={1000}>
        <Stack 
          direction={{ xs: 'column', sm: 'row' }} 
          spacing={2} 
          sx={{ mt: 4 }}
        >
          <Button
            variant="outlined"
            startIcon={<HistoryIcon />}
            onClick={handleClose}
            size="large"
          >
            View Meeting History
          </Button>
          <Button
            variant="contained"
            startIcon={<HomeIcon />}
            onClick={handleClose}
            size="large"
          >
            Go to Dashboard
          </Button>
        </Stack>
      </Slide>

      {/* Decorative Elements */}
      <Box
        sx={{
          position: 'absolute',
          top: 20,
          right: 20,
          opacity: 0.1,
          pointerEvents: 'none'
        }}
      >
        <CheckCircleIcon sx={{ fontSize: 200, color: 'success.main' }} />
      </Box>
      <Box
        sx={{
          position: 'absolute',
          bottom: 20,
          left: 20,
          opacity: 0.1,
          pointerEvents: 'none'
        }}
      >
        <StarIcon sx={{ fontSize: 150, color: 'warning.main' }} />
      </Box>
    </Box>
  );
};

export default FeedbackSuccess;