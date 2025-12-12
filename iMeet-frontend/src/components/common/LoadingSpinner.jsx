import React, { useState, useEffect } from 'react';
import { 
  Box, 
  CircularProgress, 
  Typography, 
  Backdrop,
  LinearProgress,
  Skeleton,
  Paper,
  Alert,
  Button,
  Fade,
  Grow,
  Stack,
  Chip
} from '@mui/material';
import { 
  VideoCall, 
  Wifi, 
  CloudDownload, 
  CloudUpload,
  Warning,
  Refresh,
  CheckCircle,
  Error as ErrorIcon,
  SignalWifiOff,
  Router,
  Storage
} from '@mui/icons-material';

// Enhanced Main Loading Spinner Component
const LoadingSpinner = ({ 
  size = 40, 
  message = 'Loading...', 
  variant = 'circular',
  fullScreen = false,
  color = 'primary',
  showLogo = false,
  progress = null,
  status = 'loading',
  onRetry = null,
  timeout = false
}) => {
  const [dots, setDots] = useState('');
  
  useEffect(() => {
    const interval = setInterval(() => {
      setDots(prev => prev.length >= 3 ? '' : prev + '.');
    }, 500);
    return () => clearInterval(interval);
  }, []);

  const getStatusColor = () => {
    switch (status) {
      case 'success': return 'success';
      case 'error': return 'error';
      case 'warning': return 'warning';
      default: return color;
    }
  };

  const SpinnerContent = () => {
    switch (variant) {
      case 'linear':
        return (
          <Box sx={{ width: '100%', maxWidth: 500, px: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <Box sx={{ 
                width: 48, 
                height: 48, 
                borderRadius: '12px',
                bgcolor: `${getStatusColor()}.main`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                mr: 2,
                boxShadow: '0 8px 32px rgba(0,0,0,0.1)'
              }}>
                {showLogo ? <VideoCall sx={{ color: 'white', fontSize: 24 }} /> : null}
              </Box>
              <Box sx={{ flex: 1 }}>
                <Typography variant="h6" sx={{ fontWeight: 600, mb: 0.5 }}>
                  {message}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Please wait while we process your request{dots}
                </Typography>
              </Box>
            </Box>
            <LinearProgress 
              variant={progress !== null ? 'determinate' : 'indeterminate'}
              value={progress}
              color={getStatusColor()}
              sx={{ 
                height: 8, 
                borderRadius: 4,
                bgcolor: 'rgba(0,0,0,0.06)',
                '& .MuiLinearProgress-bar': {
                  borderRadius: 4,
                  boxShadow: '0 2px 8px rgba(0,0,0,0.2)'
                }
              }} 
            />
            {progress !== null && (
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                {Math.round(progress)}% complete
              </Typography>
            )}
          </Box>
        );
        
      case 'professional':
        return (
          <Paper 
            elevation={0}
            sx={{ 
              p: 4, 
              borderRadius: 3, 
              bgcolor: 'rgba(255,255,255,0.95)',
              backdropFilter: 'blur(20px)',
              border: '1px solid rgba(0,0,0,0.08)',
              boxShadow: '0 20px 60px rgba(0,0,0,0.1)',
              textAlign: 'center',
              maxWidth: 400
            }}
          >
            <Box sx={{ position: 'relative', display: 'inline-block', mb: 3 }}>
              <CircularProgress 
                size={64} 
                thickness={3}
                color={getStatusColor()}
                sx={{
                  '& .MuiCircularProgress-circle': {
                    strokeLinecap: 'round',
                  }
                }}
              />
              <Box sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}>
                {showLogo && <VideoCall sx={{ fontSize: 28, color: `${getStatusColor()}.main` }} />}
              </Box>
            </Box>
            
            <Typography variant="h6" sx={{ fontWeight: 600, mb: 1, color: 'text.primary' }}>
              {message}
            </Typography>
            
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              We're setting up everything for you{dots}
            </Typography>

            {timeout && onRetry && (
              <Button 
                variant="outlined" 
                size="small" 
                onClick={onRetry}
                startIcon={<Refresh />}
                sx={{ mt: 2 }}
              >
                Retry Connection
              </Button>
            )}
          </Paper>
        );
        
      case 'dots':
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Box sx={{ display: 'flex', gap: 1 }}>
              {[0, 1, 2].map((i) => (
                <Box
                  key={i}
                  sx={{
                    width: 12,
                    height: 12,
                    borderRadius: '50%',
                    bgcolor: `${getStatusColor()}.main`,
                    animation: `bounce 1.4s infinite ease-in-out both`,
                    animationDelay: `${i * 0.16}s`,
                    boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
                    '@keyframes bounce': {
                      '0%, 80%, 100%': { 
                        transform: 'scale(0.7)',
                        opacity: 0.5
                      },
                      '40%': { 
                        transform: 'scale(1)',
                        opacity: 1
                      }
                    }
                  }}
                />
              ))}
            </Box>
            {message && (
              <Typography variant="body1" color="text.primary" sx={{ fontWeight: 500 }}>
                {message}
              </Typography>
            )}
          </Box>
        );
        
      default: // circular
        return (
          <Box sx={{ textAlign: 'center' }}>
            <Box sx={{ position: 'relative', display: 'inline-block', mb: 2 }}>
              {showLogo && (
                <VideoCall 
                  sx={{ 
                    fontSize: size + 24, 
                    color: `${getStatusColor()}.main`, 
                    mb: 2,
                    animation: 'rotate 3s linear infinite',
                    filter: 'drop-shadow(0 4px 8px rgba(0,0,0,0.2))',
                    '@keyframes rotate': {
                      '0%': { transform: 'rotate(0deg)' },
                      '100%': { transform: 'rotate(360deg)' }
                    }
                  }} 
                />
              )}
              <CircularProgress 
                size={size} 
                color={getStatusColor()}
                thickness={3}
                sx={{
                  '& .MuiCircularProgress-circle': {
                    strokeLinecap: 'round',
                    filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
                  }
                }}
              />
            </Box>
            {message && (
              <Typography 
                variant="body1" 
                color="text.primary"
                sx={{ fontWeight: 500, maxWidth: 250 }}
              >
                {message}
              </Typography>
            )}
          </Box>
        );
    }
  };

  if (fullScreen) {
    return (
      <Backdrop
        sx={{
          color: '#fff',
          zIndex: 9999,
          bgcolor: 'rgba(15, 23, 42, 0.85)',
          backdropFilter: 'blur(10px)'
        }}
        open={true}
      >
        <Fade in={true} timeout={500}>
          <Box sx={{ textAlign: 'center' }}>
            <SpinnerContent />
          </Box>
        </Fade>
      </Backdrop>
    );
  }

  return (
    <Box 
      sx={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center',
        p: 3,
        minHeight: 120
      }}
    >
      <SpinnerContent />
    </Box>
  );
};

// Enhanced Meeting Loading Component
export const MeetingLoadingSpinner = ({ 
  message = 'Joining meeting...', 
  status = 'connecting',
  participantCount = 0,
  onRetry = null 
}) => {
  const [connectionSteps, setConnectionSteps] = useState(0);
  
  useEffect(() => {
    const steps = ['Initializing', 'Connecting to server', 'Joining room', 'Setting up audio/video'];
    const interval = setInterval(() => {
      setConnectionSteps(prev => (prev + 1) % steps.length);
    }, 1500);
    return () => clearInterval(interval);
  }, []);

  const steps = ['Initializing', 'Connecting to server', 'Joining room', 'Setting up audio/video'];

  return (
    <Box
      sx={{
        height: '100vh',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        alignItems: 'center',
        bgcolor: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)',
        background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)',
        color: 'white',
        position: 'relative',
        overflow: 'hidden'
      }}
    >
      {/* Background Pattern */}
      <Box sx={{
        position: 'absolute',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        opacity: 0.1,
        backgroundImage: 'radial-gradient(circle at 25% 25%, #3b82f6 0%, transparent 50%), radial-gradient(circle at 75% 75%, #8b5cf6 0%, transparent 50%)'
      }} />

      <Paper
        elevation={0}
        sx={{
          p: 6,
          borderRadius: 4,
          bgcolor: 'rgba(255, 255, 255, 0.05)',
          backdropFilter: 'blur(20px)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          textAlign: 'center',
          position: 'relative',
          zIndex: 1,
          maxWidth: 480
        }}
      >
        <Box sx={{ position: 'relative', mb: 4 }}>
          <VideoCall 
            sx={{ 
              fontSize: 96, 
              color: 'primary.main', 
              mb: 3,
              animation: 'pulse 2s infinite',
              filter: 'drop-shadow(0 8px 16px rgba(59, 130, 246, 0.3))',
              '@keyframes pulse': {
                '0%': { opacity: 1, transform: 'scale(1)' },
                '50%': { opacity: 0.8, transform: 'scale(1.05)' },
                '100%': { opacity: 1, transform: 'scale(1)' }
              }
            }} 
          />
          
          <CircularProgress 
            size={80} 
            color="primary"
            thickness={2}
            sx={{ 
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              mt: -1.5
            }}
          />
        </Box>

        <Typography variant="h4" sx={{ mb: 2, fontWeight: 700, color: 'white' }}>
          {message}
        </Typography>
        
        <Typography variant="body1" color="rgba(255,255,255,0.7)" sx={{ mb: 4 }}>
          {steps[connectionSteps]}...
        </Typography>

        <Stack spacing={2} sx={{ mb: 4 }}>
          {steps.map((step, index) => (
            <Box 
              key={step}
              sx={{ 
                display: 'flex', 
                alignItems: 'center', 
                gap: 2,
                opacity: index <= connectionSteps ? 1 : 0.4,
                transition: 'opacity 0.3s ease'
              }}
            >
              {index < connectionSteps ? (
                <CheckCircle sx={{ color: 'success.main', fontSize: 20 }} />
              ) : index === connectionSteps ? (
                <CircularProgress size={20} thickness={4} />
              ) : (
                <Box sx={{ width: 20, height: 20, borderRadius: '50%', bgcolor: 'rgba(255,255,255,0.2)' }} />
              )}
              <Typography variant="body2" color="rgba(255,255,255,0.8)">
                {step}
              </Typography>
            </Box>
          ))}
        </Stack>

        {participantCount > 0 && (
          <Chip 
            label={`${participantCount} participants waiting`}
            color="primary"
            variant="filled"
            sx={{ mb: 2 }}
          />
        )}

        {status === 'error' && onRetry && (
          <Button 
            variant="contained" 
            onClick={onRetry}
            startIcon={<Refresh />}
            sx={{ mt: 2 }}
          >
            Retry Connection
          </Button>
        )}
      </Paper>
    </Box>
  );
};

// Enhanced Connection Loading Component with Error Handling
export const ConnectionLoadingSpinner = ({ 
  status = 'connecting', 
  onRetry = null,
  timeout = false,
  errorMessage = null 
}) => {
  const getStatusConfig = () => {
    switch (status) {
      case 'connecting':
        return {
          message: 'Establishing secure connection...',
          icon: <Router sx={{ fontSize: 28 }} />,
          color: 'primary'
        };
      case 'reconnecting':
        return {
          message: 'Connection lost. Reconnecting...',
          icon: <SignalWifiOff sx={{ fontSize: 28 }} />,
          color: 'warning'
        };
      case 'downloading':
        return {
          message: 'Downloading content securely...',
          icon: <CloudDownload sx={{ fontSize: 28 }} />,
          color: 'info'
        };
      case 'uploading':
        return {
          message: 'Uploading files securely...',
          icon: <CloudUpload sx={{ fontSize: 28 }} />,
          color: 'info'
        };
      case 'error':
        return {
          message: errorMessage || 'Connection failed',
          icon: <ErrorIcon sx={{ fontSize: 28 }} />,
          color: 'error'
        };
      case 'success':
        return {
          message: 'Connected successfully!',
          icon: <CheckCircle sx={{ fontSize: 28 }} />,
          color: 'success'
        };
      default:
        return {
          message: 'Loading...',
          icon: <Storage sx={{ fontSize: 28 }} />,
          color: 'primary'
        };
    }
  };

  const config = getStatusConfig();

  return (
    <Paper
      elevation={0}
      sx={{
        p: 4,
        borderRadius: 3,
        bgcolor: 'rgba(255, 255, 255, 0.98)',
        backdropFilter: 'blur(20px)',
        border: '1px solid rgba(0,0,0,0.08)',
        boxShadow: '0 20px 60px rgba(0,0,0,0.08)',
        textAlign: 'center',
        maxWidth: 400,
        mx: 'auto'
      }}
    >
      <Box sx={{ 
        position: 'relative',
        display: 'inline-block',
        mb: 3
      }}>
        {status !== 'error' && status !== 'success' && (
          <CircularProgress 
            size={80} 
            thickness={3}
            color={config.color}
            sx={{
              '& .MuiCircularProgress-circle': {
                strokeLinecap: 'round',
              }
            }}
          />
        )}
        <Box sx={{
          position: status === 'error' || status === 'success' ? 'static' : 'absolute',
          top: status === 'error' || status === 'success' ? 'auto' : '50%',
          left: status === 'error' || status === 'success' ? 'auto' : '50%',
          transform: status === 'error' || status === 'success' ? 'none' : 'translate(-50%, -50%)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: `${config.color}.main`
        }}>
          {config.icon}
        </Box>
      </Box>
      
      <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
        {config.message}
      </Typography>
      
      {status === 'error' && (
        <Alert 
          severity="error" 
          sx={{ mb: 2, textAlign: 'left' }}
        >
          {errorMessage || 'Unable to establish connection. Please check your internet connection and try again.'}
        </Alert>
      )}

      {(timeout || status === 'error') && onRetry && (
        <Button 
          variant="contained" 
          onClick={onRetry}
          startIcon={<Refresh />}
          color={config.color}
          sx={{ mt: 1 }}
        >
          Try Again
        </Button>
      )}

      {status !== 'error' && status !== 'success' && (
        <Typography variant="body2" color="text.secondary">
          This may take a few moments. Please don't close this window.
        </Typography>
      )}
    </Paper>
  );
};

// Enhanced Page Loading Skeleton
export const PageLoadingSkeleton = ({ variant = 'dashboard' }) => {
  if (variant === 'dashboard') {
    return (
      <Box sx={{ p: 4 }}>
        <Skeleton 
          variant="text" 
          width="40%" 
          height={48} 
          sx={{ 
            mb: 4,
            borderRadius: 2,
            transform: 'none'
          }} 
        />
        <Box sx={{ 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', 
          gap: 3, 
          mb: 4 
        }}>
          {[1, 2, 3].map((i) => (
            <Paper key={i} sx={{ p: 3, borderRadius: 3 }} elevation={0}>
              <Skeleton 
                variant="rectangular" 
                height={140} 
                sx={{ mb: 2, borderRadius: 2 }} 
              />
              <Skeleton variant="text" width="80%" height={24} sx={{ mb: 1 }} />
              <Skeleton variant="text" width="60%" height={20} />
            </Paper>
          ))}
        </Box>
        <Paper sx={{ p: 3, borderRadius: 3 }} elevation={0}>
          <Skeleton 
            variant="rectangular" 
            height={350} 
            sx={{ borderRadius: 2 }}
          />
        </Paper>
      </Box>
    );
  }

  if (variant === 'meeting') {
    return (
      <Box sx={{ p: 2 }}>
        <Box sx={{ 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', 
          gap: 2 
        }}>
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <Skeleton 
              key={i} 
              variant="rectangular" 
              height={180} 
              sx={{ 
                borderRadius: 3,
                animation: 'pulse 2s ease-in-out infinite',
                animationDelay: `${i * 0.1}s`
              }} 
            />
          ))}
        </Box>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 4 }}>
      <Skeleton variant="text" width="60%" height={40} sx={{ mb: 3, borderRadius: 2 }} />
      <Skeleton variant="rectangular" height={250} sx={{ mb: 3, borderRadius: 2 }} />
      <Skeleton variant="text" width="80%" height={24} sx={{ mb: 1 }} />
      <Skeleton variant="text" width="70%" height={24} sx={{ mb: 1 }} />
      <Skeleton variant="text" width="90%" height={24} />
    </Box>
  );
};

// Enhanced Inline Loading Component
export const InlineLoadingSpinner = ({ 
  size = 20, 
  message,
  variant = 'circular' 
}) => (
  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
    {variant === 'dots' ? (
      <Box sx={{ display: 'flex', gap: 0.5 }}>
        {[0, 1, 2].map((i) => (
          <Box
            key={i}
            sx={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              bgcolor: 'primary.main',
              animation: `bounce 1.4s infinite ease-in-out both`,
              animationDelay: `${i * 0.16}s`,
              '@keyframes bounce': {
                '0%, 80%, 100%': { transform: 'scale(0.7)' },
                '40%': { transform: 'scale(1)' }
              }
            }}
          />
        ))}
      </Box>
    ) : (
      <CircularProgress 
        size={size} 
        thickness={4}
        sx={{
          '& .MuiCircularProgress-circle': {
            strokeLinecap: 'round',
          }
        }}
      />
    )}
    {message && (
      <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 500 }}>
        {message}
      </Typography>
    )}
  </Box>
);

// Enhanced Button Loading Component
export const ButtonLoadingSpinner = ({ size = 20, variant = 'circular' }) => {
  if (variant === 'dots') {
    return (
      <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center' }}>
        {[0, 1, 2].map((i) => (
          <Box
            key={i}
            sx={{
              width: 4,
              height: 4,
              borderRadius: '50%',
              bgcolor: 'currentColor',
              animation: `bounce 1.4s infinite ease-in-out both`,
              animationDelay: `${i * 0.16}s`,
              '@keyframes bounce': {
                '0%, 80%, 100%': { transform: 'scale(0.7)' },
                '40%': { transform: 'scale(1)' }
              }
            }}
          />
        ))}
      </Box>
    );
  }

  return (
    <CircularProgress 
      size={size} 
      thickness={4}
      sx={{ 
        color: 'inherit',
        '& .MuiCircularProgress-circle': {
          strokeLinecap: 'round',
        }
      }}
    />
  );
};

export default LoadingSpinner;