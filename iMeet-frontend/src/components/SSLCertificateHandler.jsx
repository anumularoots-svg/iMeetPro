// src/components/SSLCertificateHandler.jsx
import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Alert,
  Box,
  Link,
  CircularProgress
} from '@mui/material';
import { Security, Warning, OpenInNew } from '@mui/icons-material';
import { checkSSLCertificate } from '../services/api';
import { API_BASE_URL } from '../utils/constants';

const SSLCertificateHandler = () => {
  const [open, setOpen] = useState(false);
  const [checking, setChecking] = useState(true);
  const [sslAccepted, setSslAccepted] = useState(false);

  useEffect(() => {
    const checkSSL = async () => {
      // Only check in development
      if (process.env.NODE_ENV !== 'development' && import.meta.env.MODE !== 'development') {
        setChecking(false);
        return;
      }

      try {
        const isOk = await checkSSLCertificate();
        if (!isOk) {
          setOpen(true);
        } else {
          setSslAccepted(true);
        }
      } catch (error) {
        console.error('SSL check failed:', error);
        setOpen(true);
      } finally {
        setChecking(false);
      }
    };

    // Check SSL after a short delay to ensure app is loaded
    const timer = setTimeout(checkSSL, 1500);
    return () => clearTimeout(timer);
  }, []);

  const handleAcceptCertificate = () => {
    // Open the API URL in a new tab
    window.open(API_BASE_URL, '_blank');
    
    // Show instructions to come back
    setTimeout(() => {
      if (window.confirm('Have you accepted the certificate? Click OK to refresh the page.')) {
        window.location.reload();
      }
    }, 3000);
  };

  const handleIgnore = () => {
    setOpen(false);
    // Store in session storage to not show again during this session
    sessionStorage.setItem('ssl_warning_ignored', 'true');
  };

  // Don't show if already ignored in this session
  useEffect(() => {
    if (sessionStorage.getItem('ssl_warning_ignored') === 'true') {
      setOpen(false);
    }
  }, []);

  if (checking) {
    return null; // Don't show anything while checking
  }

  return (
    <Dialog 
      open={open} 
      onClose={handleIgnore}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" gap={1}>
          <Warning color="warning" />
          <Typography variant="h6">SSL Certificate Required</Typography>
        </Box>
      </DialogTitle>
      
      <DialogContent>
        <Alert severity="warning" sx={{ mb: 2 }}>
          Your browser is blocking the connection to the backend server due to a self-signed SSL certificate.
        </Alert>
        
        <Typography variant="body1" paragraph>
          To use this application, you need to accept the backend server's SSL certificate.
        </Typography>
        
        <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Follow these steps:
          </Typography>
          <ol style={{ margin: 0, paddingLeft: 20 }}>
            <li>Click "Accept Certificate" below</li>
            <li>A new tab will open - you'll see a security warning</li>
            <li>Click "Advanced" or "Show Details"</li>
            <li>Click "Proceed to {new URL(API_BASE_URL).hostname} (unsafe)"</li>
            <li>Come back to this tab and refresh</li>
          </ol>
        </Box>
        
        <Alert severity="info" variant="outlined">
          <Typography variant="caption">
            This is only required for development. Production environments should use valid SSL certificates.
          </Typography>
        </Alert>
      </DialogContent>
      
      <DialogActions>
        <Button onClick={handleIgnore} color="inherit">
          Ignore (Not Recommended)
        </Button>
        <Button 
          onClick={handleAcceptCertificate} 
          variant="contained" 
          color="warning"
          startIcon={<Security />}
          endIcon={<OpenInNew />}
        >
          Accept Certificate
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SSLCertificateHandler;