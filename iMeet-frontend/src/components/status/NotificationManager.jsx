// src/components/status/NotificationManager.jsx
import React from 'react';
import { Snackbar, Alert } from '@mui/material';

const NotificationManager = ({ 
  notification, 
  showNotification, 
  onClose 
}) => {
  return (
    <Snackbar
      open={showNotification}
      autoHideDuration={4000}
      onClose={onClose}
      anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
    >
      <Alert
        onClose={onClose}
        severity={notification?.severity || 'info'}
        sx={{
          width: '100%',
          background: 'rgba(45, 55, 72, 0.98)',
          backdropFilter: 'blur(16px)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          borderRadius: 2,
          color: 'white',
        }}
      >
        {notification?.message}
      </Alert>
    </Snackbar>
  );
};

export default NotificationManager;