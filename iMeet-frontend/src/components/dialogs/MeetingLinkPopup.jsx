// src/components/dialogs/MeetingLinkPopup.jsx
import React from 'react';
import {
  Card,
  CardContent,
  Typography,
  IconButton,
  Box,
  Tooltip,
  Fade,
} from '@mui/material';
import {
  Close,
  ContentCopy,
  Security,
  MeetingRoom as MeetingRoomIcon,
} from '@mui/icons-material';

const MeetingLinkPopup = ({
  open,
  minimized,
  meetingLink,
  currentUser,
  onClose,
  onCopy,
  onMinimize,
  onRestore,
  getParticipantDisplayName,
}) => {
  // Minimized button view
  if (minimized) {
    return (
      <Tooltip title="Meeting details">
        <IconButton
          onClick={onRestore}
          sx={{
            position: 'fixed',
            bottom: 24,
            left: 24,
            width: 48,
            height: 48,
            background: 'white',
            color: '#5f6368',
            border: '1px solid #e8eaed',
            borderRadius: '50%',
            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.12)',
            zIndex: 10001,
            transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',

            '&:hover': {
              background: '#f8f9fa',
              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.16)',
              color: '#1967d2',
            },
          }}
        >
          <MeetingRoomIcon />
        </IconButton>
      </Tooltip>
    );
  }

  // Full popup view
  if (!open) return null;

  return (
    <Fade in={open}>
      <Card
        sx={{
          position: 'fixed',
          top: '50%',
          left: 24,
          transform: 'translateY(-50%)',
          background: 'rgba(255, 255, 255, 0.98)',
          backdropFilter: 'blur(20px)',
          WebkitBackdropFilter: 'blur(20px)',
          borderRadius: 1.5,
          border: '1px solid rgba(0, 0, 0, 0.08)',
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.12)',
          color: '#1f2937',
          width: 360,
          maxWidth: 'calc(100vw - 48px)',
          zIndex: 10001,
          transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        }}
      >
        <CardContent sx={{ p: 3 }}>
          {/* Header */}
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'flex-start',
              mb: 2,
            }}
          >
            <Typography
              variant="h6"
              sx={{
                color: '#1f2937',
                fontWeight: 500,
                fontSize: '1.125rem',
              }}
            >
              Your meeting's ready
            </Typography>
            <IconButton
              size="small"
              onClick={onClose}
              sx={{
                color: '#6b7280',
                mt: -0.5,
                '&:hover': {
                  backgroundColor: 'rgba(0, 0, 0, 0.04)'
                }
              }}
            >
              <Close sx={{ fontSize: 20 }} />
            </IconButton>
          </Box>

          {/* Description */}
          <Typography
            variant="body2"
            sx={{
              color: '#5f6368',
              fontSize: '0.875rem',
              mb: 2,
              lineHeight: 1.5,
            }}
          >
            Or share this meeting link with others that you want in the meeting
          </Typography>

          {/* Meeting Link Box */}
          <Box
            sx={{
              backgroundColor: '#f8f9fa',
              borderRadius: 1.5,
              p: 1.5,
              mb: 2,
              border: '1px solid #e8eaed',
              display: 'flex',
              alignItems: 'center',
              gap: 1,
            }}
          >
            <Typography
              variant="body2"
              sx={{
                color: '#1f2937',
                fontSize: '0.875rem',
                wordBreak: 'break-all',
                flex: 1,
              }}
            >
              {meetingLink}
            </Typography>
            <Tooltip title="Copy meeting link">
              <IconButton
                size="small"
                onClick={onCopy}
                sx={{
                  color: '#5f6368',
                  flexShrink: 0,
                  '&:hover': {
                    backgroundColor: 'rgba(0, 0, 0, 0.04)',
                    color: '#1a73e8',
                  }
                }}
              >
                <ContentCopy sx={{ fontSize: 18 }} />
              </IconButton>
            </Tooltip>
          </Box>

          {/* Security Notice */}
          <Box
            sx={{
              display: 'flex',
              alignItems: 'flex-start',
              gap: 1,
              p: 1.5,
              backgroundColor: '#e8f0fe',
              borderRadius: 1.5,
              border: '1px solid #d2e3fc',
            }}
          >
            <Security
              sx={{
                fontSize: 20,
                color: '#1967d2',
                mt: 0.2,
                flexShrink: 0,
              }}
            />
            <Typography
              variant="caption"
              sx={{
                color: '#1967d2',
                fontSize: '0.75rem',
                lineHeight: 1.4,
              }}
            >
              People who use this meeting link must get your permission before they can join.
            </Typography>
          </Box>

          {/* User Info */}
          <Typography
            variant="caption"
            sx={{
              color: '#5f6368',
              fontSize: '0.75rem',
              mt: 2,
              display: 'block',
            }}
          >
            Joined as {currentUser?.email || getParticipantDisplayName?.(currentUser) || 'Guest'}
          </Typography>
        </CardContent>
      </Card>
    </Fade>
  );
};

export default MeetingLinkPopup;