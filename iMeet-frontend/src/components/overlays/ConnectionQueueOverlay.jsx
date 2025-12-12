// src/components/overlays/ConnectionQueueOverlay.jsx
import React from 'react';
import { Box, Card, CardContent, Typography, LinearProgress } from '@mui/material';
import { Queue } from '@mui/icons-material';

const ConnectionQueueOverlay = ({ 
  showQueue, 
  queuePosition, 
  estimatedWaitTime 
}) => {
  if (!showQueue) return null;

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
          maxWidth: 400,
          margin: 1,
        }}
      >
        <CardContent sx={{ textAlign: 'center', p: 3 }}>
          <Queue sx={{ fontSize: 64, mb: 2, color: '#2196f3' }} />
          <Typography variant="h5" sx={{ mb: 2, fontWeight: 600 }}>
            You're in the Queue
          </Typography>
          <Typography variant="body1" sx={{ mb: 2, color: 'grey.300' }}>
            Position: #{queuePosition}
          </Typography>
          <Typography variant="body2" sx={{ mb: 3, color: 'grey.400' }}>
            Estimated wait time: {estimatedWaitTime}s
          </Typography>
          <LinearProgress 
            sx={{ 
              height: 8, 
              borderRadius: 4,
              backgroundColor: 'rgba(255,255,255,0.2)',
              '& .MuiLinearProgress-bar': {
                backgroundColor: '#2196f3',
              }
            }} 
          />
          <Typography variant="caption" sx={{ mt: 2, display: 'block', color: 'grey.500' }}>
            The meeting will start automatically when it's your turn
          </Typography>
        </CardContent>
      </Card>
    </Box>
  );
};

export default ConnectionQueueOverlay;