// src/components/status/HandRaiseNotification.jsx
import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import { PanTool } from '@mui/icons-material';

const HandRaiseNotification = ({ 
  hasHostPrivileges, 
  pendingHandsCount, 
  handRaiseOpen,
  onClick 
}) => {
  if (!hasHostPrivileges || pendingHandsCount <= 0 || handRaiseOpen) return null;

  return (
    <Card
      sx={{
        background: 'rgba(245, 158, 11, 0.95)',
        backdropFilter: 'blur(16px)',
        border: '1px solid rgba(245, 158, 11, 0.3)',
        borderRadius: 1.5,
        color: 'white',
        cursor: 'pointer',
        transition: 'all 0.3s ease',
        '&:hover': {
          background: 'rgba(245, 158, 11, 1)',
          transform: 'translateX(-5px)',
        }
      }}
      onClick={onClick}
    >
      <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <PanTool sx={{ fontSize: 16 }} />
          <Typography variant="caption" sx={{ fontSize: '0.75rem', fontWeight: 600 }}>
            {pendingHandsCount} Hand{pendingHandsCount > 1 ? 's' : ''} Raised
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );
};

export default HandRaiseNotification;