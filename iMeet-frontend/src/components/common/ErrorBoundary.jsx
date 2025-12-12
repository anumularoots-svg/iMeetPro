import React from 'react';
import {
  Box,
  Container,
  Typography,
  Button,
  Paper,
  Alert,
  AlertTitle,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  IconButton,
  Tooltip
} from '@mui/material';
import {
  Error,
  Refresh,
  Home,
  BugReport,
  ExpandMore,
  ContentCopy,
  Warning
} from '@mui/icons-material';
// Fixed import - removed the failing import
// import { ErrorIllustration } from '../assets/images';

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
      errorId: null
    };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI
    return { 
      hasError: true,
      errorId: Date.now().toString()
    };
  }

  componentDidCatch(error, errorInfo) {
    // Log error details
    console.error('ErrorBoundary caught an error:', error, errorInfo);
    
    this.setState({
      error: error,
      errorInfo: errorInfo
    });

    // Report error to monitoring service
    this.reportError(error, errorInfo);
  }

  reportError = (error, errorInfo) => {
    // In a real app, you would send this to your error reporting service
    const errorReport = {
      message: error.message,
      stack: error.stack,
      componentStack: errorInfo.componentStack,
      timestamp: new Date().toISOString(),
      userAgent: navigator.userAgent,
      url: window.location.href,
      userId: this.props.userId || 'anonymous'
    };

    // Example: Send to error tracking service
    // errorTrackingService.captureException(error, errorReport);
    
    console.log('Error Report:', errorReport);
  };

  handleRetry = () => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
      errorId: null
    });
  };

  handleGoHome = () => {
    window.location.href = '/dashboard';
  };

  handleCopyError = () => {
    const errorText = `
Error ID: ${this.state.errorId}
Message: ${this.state.error?.message}
Stack: ${this.state.error?.stack}
Component Stack: ${this.state.errorInfo?.componentStack}
Timestamp: ${new Date().toISOString()}
URL: ${window.location.href}
    `.trim();

    navigator.clipboard.writeText(errorText).then(() => {
      // You could show a toast notification here
      console.log('Error details copied to clipboard');
    });
  };

  render() {
    if (this.state.hasError) {
      const isDevelopment = process.env.NODE_ENV === 'development';
      
      return (
        <Box
          sx={{
            minHeight: '100vh',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            bgcolor: 'background.default',
            p: 2
          }}
        >
          <Container maxWidth="md">
            <Paper
              elevation={8}
              sx={{
                p: 4,
                textAlign: 'center',
                borderRadius: 3,
                bgcolor: 'background.paper',
                border: '1px solid',
                borderColor: 'error.light'
              }}
            >
              {/* Error Icon */}
              <Error 
                sx={{ 
                  fontSize: 80, 
                  color: 'error.main', 
                  mb: 2,
                  animation: 'pulse 2s infinite',
                  '@keyframes pulse': {
                    '0%': { opacity: 1 },
                    '50%': { opacity: 0.7 },
                    '100%': { opacity: 1 }
                  }
                }} 
              />

              {/* Error Title */}
              <Typography 
                variant="h3" 
                component="h1" 
                gutterBottom
                sx={{ 
                  fontWeight: 'bold',
                  color: 'error.main',
                  mb: 2
                }}
              >
                Oops! Something went wrong
              </Typography>

              {/* Error Description */}
              <Typography 
                variant="h6" 
                color="text.secondary" 
                sx={{ mb: 3, lineHeight: 1.6 }}
              >
                We're sorry, but something unexpected happened. Our team has been notified and we're working to fix this issue.
              </Typography>

              {/* Error ID */}
              <Alert 
                severity="error" 
                sx={{ 
                  mb: 3, 
                  textAlign: 'left',
                  '& .MuiAlert-message': {
                    width: '100%'
                  }
                }}
              >
                <AlertTitle>Error Reference</AlertTitle>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Typography variant="body2" fontFamily="monospace">
                    ID: {this.state.errorId}
                  </Typography>
                  <Tooltip title="Copy error details">
                    <IconButton size="small" onClick={this.handleCopyError}>
                      <ContentCopy fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
              </Alert>

              {/* Action Buttons */}
              <Box sx={{ display: 'flex', gap: 2, justifyContent: 'center', mb: 3 }}>
                <Button
                  variant="contained"
                  size="large"
                  startIcon={<Refresh />}
                  onClick={this.handleRetry}
                  sx={{
                    minWidth: 140,
                    height: 48,
                    borderRadius: 2,
                    textTransform: 'none',
                    fontSize: '1rem',
                    fontWeight: 600
                  }}
                >
                  Try Again
                </Button>
                
                <Button
                  variant="outlined"
                  size="large"
                  startIcon={<Home />}
                  onClick={this.handleGoHome}
                  sx={{
                    minWidth: 140,
                    height: 48,
                    borderRadius: 2,
                    textTransform: 'none',
                    fontSize: '1rem',
                    fontWeight: 600
                  }}
                >
                  Go Home
                </Button>
              </Box>

              {/* Development Error Details */}
              {isDevelopment && this.state.error && (
                <Accordion sx={{ mt: 3, textAlign: 'left' }}>
                  <AccordionSummary
                    expandIcon={<ExpandMore />}
                    sx={{ bgcolor: 'action.hover' }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                      <BugReport sx={{ mr: 1, color: 'warning.main' }} />
                      <Typography variant="subtitle1" fontWeight="bold">
                        Development Error Details
                      </Typography>
                    </Box>
                  </AccordionSummary>
                  
                  <AccordionDetails sx={{ bgcolor: 'background.default' }}>
                    <Typography variant="subtitle2" gutterBottom>
                      Error Message:
                    </Typography>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        p: 2, 
                        bgcolor: 'error.dark', 
                        color: 'error.contrastText',
                        borderRadius: 1,
                        fontFamily: 'monospace',
                        mb: 2,
                        overflow: 'auto'
                      }}
                    >
                      {this.state.error.message}
                    </Typography>

                    <Typography variant="subtitle2" gutterBottom>
                      Stack Trace:
                    </Typography>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        p: 2, 
                        bgcolor: 'background.paper', 
                        border: '1px solid',
                        borderColor: 'divider',
                        borderRadius: 1,
                        fontFamily: 'monospace',
                        fontSize: '0.75rem',
                        overflow: 'auto',
                        maxHeight: 200,
                        mb: 2
                      }}
                    >
                      {this.state.error.stack}
                    </Typography>

                    {this.state.errorInfo && (
                      <>
                        <Typography variant="subtitle2" gutterBottom>
                          Component Stack:
                        </Typography>
                        <Typography 
                          variant="body2" 
                          sx={{ 
                            p: 2, 
                            bgcolor: 'background.paper', 
                            border: '1px solid',
                            borderColor: 'divider',
                            borderRadius: 1,
                            fontFamily: 'monospace',
                            fontSize: '0.75rem',
                            overflow: 'auto',
                            maxHeight: 200
                          }}
                        >
                          {this.state.errorInfo.componentStack}
                        </Typography>
                      </>
                    )}
                  </AccordionDetails>
                </Accordion>
              )}

              {/* Help Text */}
              <Box sx={{ mt: 3, p: 2, bgcolor: 'action.hover', borderRadius: 2 }}>
                <Typography variant="body2" color="text.secondary">
                  If this problem persists, please contact our support team with the error reference ID above.
                </Typography>
              </Box>
            </Paper>
          </Container>
        </Box>
      );
    }

    return this.props.children;
  }
}

// Fallback Error Component for smaller errors
export const ErrorFallback = ({ 
  error, 
  resetError, 
  title = 'Something went wrong',
  showDetails = false 
}) => {
  return (
    <Box
      sx={{
        p: 4,
        textAlign: 'center',
        bgcolor: 'background.paper',
        borderRadius: 2,
        border: '1px solid',
        borderColor: 'error.light'
      }}
    >
      <Warning sx={{ fontSize: 48, color: 'error.main', mb: 2 }} />
      
      <Typography variant="h6" gutterBottom color="error.main">
        {title}
      </Typography>
      
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        {error?.message || 'An unexpected error occurred'}
      </Typography>

      {showDetails && error && (
        <Accordion sx={{ mb: 2, textAlign: 'left' }}>
          <AccordionSummary expandIcon={<ExpandMore />}>
            <Typography variant="body2">Error Details</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Typography 
              variant="caption" 
              sx={{ 
                fontFamily: 'monospace',
                overflow: 'auto',
                wordBreak: 'break-all'
              }}
            >
              {error.stack}
            </Typography>
          </AccordionDetails>
        </Accordion>
      )}
      
      <Button
        variant="contained"
        startIcon={<Refresh />}
        onClick={resetError}
        sx={{ textTransform: 'none' }}
      >
        Try Again
      </Button>
    </Box>
  );
};

// Hook for error handling
export const useErrorHandler = () => {
  const [error, setError] = React.useState(null);

  const resetError = () => setError(null);
  
  const captureError = (error) => {
    console.error('Captured error:', error);
    setError(error);
  };

  return {
    error,
    resetError,
    captureError
  };
};

export default ErrorBoundary;