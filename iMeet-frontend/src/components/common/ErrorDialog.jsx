// ErrorDialog.jsx - VERSION WITHOUT ALPHA FUNCTION
import React from 'react';
import {
  Dialog,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  IconButton,
} from '@mui/material';
import {
  Close as CloseIcon,
  CheckCircle as SuccessIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  Info as InfoIcon,
} from '@mui/icons-material';

const ErrorDialog = ({
  open,
  onClose,
  title,
  message,
  severity = 'error',
  details = null,
}) => {
  // Get correct icon and color based on severity
  const getIconConfig = () => {
    switch (severity) {
      case 'success':
        return {
          Icon: SuccessIcon,
          color: '#4caf50', // Green
          bgColor: '#e8f5e9', // Light green
        };
      case 'warning':
        return {
          Icon: WarningIcon,
          color: '#ff9800', // Orange
          bgColor: '#fff3e0', // Light orange
        };
      case 'info':
        return {
          Icon: InfoIcon,
          color: '#2196f3', // Blue
          bgColor: '#e3f2fd', // Light blue
        };
      case 'error':
      default:
        return {
          Icon: ErrorIcon,
          color: '#f44336', // Red
          bgColor: '#ffebee', // Light red
        };
    }
  };

  const { Icon, color, bgColor } = getIconConfig();

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 3,
          boxShadow: 3,
        },
      }}
    >
      {/* Close button */}
      <IconButton
        onClick={onClose}
        sx={{
          position: 'absolute',
          right: 8,
          top: 8,
          color: 'grey.500',
          zIndex: 1,
        }}
      >
        <CloseIcon />
      </IconButton>

      {/* Content */}
      <DialogContent sx={{ textAlign: 'center', pt: 5, pb: 3 }}>
        {/* Icon Circle */}
        <Box
          sx={{
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 80,
            height: 80,
            borderRadius: '50%',
            bgcolor: bgColor,
            mb: 3,
          }}
        >
          <Icon sx={{ fontSize: 48, color }} />
        </Box>

        {/* Title */}
        <Typography variant="h5" fontWeight={600} gutterBottom>
          {title}
        </Typography>

        {/* Message */}
        <Typography variant="body1" color="text.secondary" sx={{ mt: 2 }}>
          {message}
        </Typography>

        {/* Details (if any) */}
        {details && (
          <Box
            sx={{
              mt: 3,
              p: 2,
              bgcolor: '#f5f5f5',
              borderRadius: 1,
              maxHeight: 150,
              overflow: 'auto',
              textAlign: 'left',
            }}
          >
            <Typography variant="caption" color="text.secondary" fontWeight={600}>
              Details:
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, whiteSpace: 'pre-wrap' }}>
              {details}
            </Typography>
          </Box>
        )}
      </DialogContent>

      {/* Actions */}
      <DialogActions sx={{ p: 3, pt: 0 }}>
        <Button
          onClick={onClose}
          variant="contained"
          fullWidth
          sx={{
            py: 1.5,
            fontWeight: 600,
            textTransform: 'none',
            bgcolor: severity === 'success' ? '#4caf50' : '#1976d2',
            '&:hover': {
              bgcolor: severity === 'success' ? '#45a049' : '#1565c0',
            },
          }}
        >
          OK
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ErrorDialog;