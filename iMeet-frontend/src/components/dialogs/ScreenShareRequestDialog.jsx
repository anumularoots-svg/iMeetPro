// src/components/dialogs/ScreenShareRequestDialog.jsx
import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Alert,
  Box,
  Avatar,
  Divider,
} from '@mui/material';
import { ScreenShare, Security } from '@mui/icons-material';

const ScreenShareRequestDialog = ({
  open,
  onClose,
  onApprove,
  onDeny,
  currentScreenShareRequest,
  hasHostPrivileges,
}) => {
  if (!open || !currentScreenShareRequest || !hasHostPrivileges) {
    return null;
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          borderRadius: 3,
          background: 'rgba(255, 255, 255, 0.98)',
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.15)',
          minWidth: 400,
        }
      }}
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <ScreenShare sx={{ color: '#1a73e8', fontSize: 28 }} />
          <Box>
            <Typography variant="h6" sx={{ color: '#1f2937', fontWeight: 500, fontSize: '1.125rem' }}>
              Screen Share Request
            </Typography>
            <Typography variant="caption" sx={{ color: '#5f6368' }}>
              Participant wants to share their screen
            </Typography>
          </Box>
        </Box>
      </DialogTitle>

      <DialogContent>
        <Box sx={{
          p: 2.5,
          backgroundColor: '#f8f9fa',
          borderRadius: 2,
          border: '1px solid #e8eaed',
          mb: 2
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1.5 }}>
            <Avatar
              sx={{
                width: 48,
                height: 48,
                backgroundColor: '#1a73e8',
                fontSize: '1.25rem',
                fontWeight: 600,
              }}
            >
              {currentScreenShareRequest?.user_name?.charAt(0).toUpperCase() || 'U'}
            </Avatar>
            <Box sx={{ flex: 1 }}>
            <Typography
              variant="body1"
              sx={{
                color: '#1f2937',
                fontWeight: 600,
                fontSize: '1rem',
                mb: 0.3
              }}
            >
              {/* ✅ FIX: Multiple fallbacks for display name */}
              {currentScreenShareRequest?.user_full_name || 
              currentScreenShareRequest?.user_name || 
              `User ${currentScreenShareRequest?.user_id}` || 
              'Unknown User'}
            </Typography>
              <Typography
                variant="caption"
                sx={{
                  color: '#6b7280',
                  fontSize: '0.75rem',
                  display: 'block'
                }}
              >
                User ID: {currentScreenShareRequest?.user_id || 'N/A'}
              </Typography>
            </Box>
          </Box>

          <Divider sx={{ my: 1.5 }} />

          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
            <Security sx={{ fontSize: 18, color: '#1967d2', mt: 0.2 }} />
            <Typography variant="caption" sx={{ color: '#5f6368', lineHeight: 1.4 }}>
              Requesting screen share permission
            </Typography>
          </Box>
        </Box>

        <Alert
          severity="info"
          sx={{
            backgroundColor: '#e8f0fe',
            border: '1px solid #d2e3fc',
            '& .MuiAlert-icon': { color: '#1967d2' }
          }}
        >
          <Typography variant="body2" sx={{ color: '#1967d2' }}>
            This participant will be able to share their screen with all meeting participants.
          </Typography>
        </Alert>
      </DialogContent>

      <DialogActions sx={{ p: 3, pt: 2 }}>
        <Button
          onClick={onDeny}
          variant="outlined"
          sx={{
            textTransform: 'none',
            borderColor: '#e8eaed',
            color: '#5f6368',
            px: 3,
            '&:hover': {
              backgroundColor: 'rgba(0, 0, 0, 0.04)',
              borderColor: '#d2d3d4',
            }
          }}
        >
          Deny
        </Button>
        <Button
          onClick={onApprove}
          variant="contained"
          sx={{
            textTransform: 'none',
            background: '#1a73e8',
            color: 'white',
            px: 3,
            boxShadow: 'none',
            '&:hover': {
              background: '#1557b0',
              boxShadow: '0 1px 3px rgba(0,0,0,0.12)',
            }
          }}
        >
          Approve
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ScreenShareRequestDialog;