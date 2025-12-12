// src/components/status/UploadProgressBar.jsx
import React from 'react';
import { Box, Typography, LinearProgress } from '@mui/material';

const UploadProgressBar = ({ 
  uploading, 
  uploadProgress 
}) => {
  if (!uploading || uploadProgress <= 0) return null;

  return (
    <Box
      sx={{
        position: 'fixed',
        top: 64,
        left: '50%',
        transform: 'translateX(-50%)',
        zIndex: 10001,
        minWidth: 300,
        background: 'rgba(59, 130, 246, 0.9)',
        backdropFilter: 'blur(16px)',
        color: 'white',
        padding: 2,
        borderRadius: 2,
      }}
    >
      <Typography variant="body2" sx={{ mb: 1 }}>
        Uploading Recording... {uploadProgress}%
      </Typography>
      <LinearProgress
        variant="determinate"
        value={uploadProgress}
        sx={{
          height: 8,
          borderRadius: 4,
          backgroundColor: 'rgba(255,255,255,0.3)',
          '& .MuiLinearProgress-bar': {
            backgroundColor: '#4caf50',
          },
        }}
      />
    </Box>
  );
};

export default UploadProgressBar;