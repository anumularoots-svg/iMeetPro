// src/components/overlays/ScreenShareWaitingOverlay.jsx
import React from 'react';
import { Box, Card, CardContent, Typography, Button } from '@mui/material';
import { HourglassEmpty } from '@mui/icons-material';

const ScreenShareWaitingOverlay = ({ 
  showWaiting, 
  onCancel 
}) => {
  if (!showWaiting) return null;

  return (
    <Box
      sx={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: 'rgba(0, 0, 0, 0.8)',
        backdropFilter: 'blur(8px)',
        WebkitBackdropFilter: 'blur(8px)',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 15000,
        textAlign: 'center',
      }}
    >
      <Card
        sx={{
          background: 'rgba(59, 130, 246, 0.1)',
          backdropFilter: 'blur(20px)',
          WebkitBackdropFilter: 'blur(20px)',
          border: '1px solid rgba(59, 130, 246, 0.3)',
          borderRadius: 2,
          color: 'white',
          minWidth: 300,
          margin: 1,
        }}
      >
        <CardContent sx={{ textAlign: 'center', p: 3 }}>
          <HourglassEmpty sx={{ fontSize: 64, mb: 2, color: '#2196f3' }} />
          <Typography variant="h5" sx={{ mb: 2, fontWeight: 600 }}>
            Waiting for Host/Co-Host Approval
          </Typography>
          <Typography variant="body1" sx={{ mb: 2, color: 'grey.300' }}>
            Your screen share request has been sent to the hosts.
          </Typography>
          <Typography variant="body2" sx={{ color: 'grey.400' }}>
            Please wait for a host or co-host to approve your request...
          </Typography>
          <Button
            variant="outlined"
            onClick={onCancel}
            sx={{ mt: 3, color: 'white', borderColor: 'white' }}
          >
            Cancel Request
          </Button>
        </CardContent>
      </Card>
    </Box>
  );
};

export default ScreenShareWaitingOverlay;