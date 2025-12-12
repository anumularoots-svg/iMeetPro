// src/components/panels/HandRaisePanelWrapper.jsx
import React from 'react';
import {
  Box,
  Typography,
  IconButton,
  Chip,
  Tooltip,
  CircularProgress,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  ListItemSecondaryAction,
  Avatar,
} from '@mui/material';
import { PanTool, Clear, Check } from '@mui/icons-material';

const HandRaisePanelWrapper = ({
  isOpen,
  onClose,
  hasHostPrivileges,
  raisedHands,
  totalHandsCount,
  pendingHandsCount,
  handRaiseLoading,
  handRaiseStats,
  onAcknowledgeHand,
  onDenyHand,
  onClearAllHands,
}) => {
  if (!isOpen || !hasHostPrivileges) return null;

  return (
    <>
      {/* Backdrop for mobile */}
      <Box
        sx={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.3)',
          backdropFilter: 'blur(4px)',
          WebkitBackdropFilter: 'blur(4px)',
          zIndex: 1400,
          display: { xs: 'block', md: 'none' }
        }}
        onClick={onClose}
      />

      <Box
        className="hand-raise-panel-container"
        sx={{
          position: 'fixed',
          top: { xs: '10%', md: 80 },
          right: { xs: '5%', md: 24 },
          left: { xs: '5%', md: 'auto' },
          width: { xs: 'auto', md: 380 },
          maxWidth: { xs: '90vw', md: '380px' },
          maxHeight: { xs: '70vh', md: '60vh' },
          background: 'rgba(26, 32, 44, 0.98)',
          backdropFilter: 'blur(24px)',
          WebkitBackdropFilter: 'blur(24px)',
          borderRadius: 2,
          border: '1px solid rgba(255, 255, 255, 0.1)',
          boxShadow: '0 20px 60px rgba(0, 0, 0, 0.5)',
          zIndex: 1500,
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* Hand Raise Panel Header */}
        <Box
          sx={{
            p: 2,
            borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            background: 'rgba(59, 130, 246, 0.1)',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <PanTool sx={{ color: '#60a5fa', fontSize: 20 }} />
            <Typography variant="h6" sx={{ color: 'white', fontWeight: 600 }}>
              Raised Hands
            </Typography>
            {totalHandsCount > 0 && (
              <Chip
                label={totalHandsCount}
                size="small"
                sx={{
                  backgroundColor: '#f59e0b',
                  color: 'white',
                  height: 24,
                  fontSize: '0.75rem',
                }}
              />
            )}
          </Box>
          <Box>
            {totalHandsCount > 0 && (
              <Tooltip title="Clear all hands">
                <IconButton
                  onClick={onClearAllHands}
                  size="small"
                  sx={{
                    color: 'rgba(239, 68, 68, 0.8)',
                    mr: 1,
                    '&:hover': {
                      color: '#ef4444',
                      backgroundColor: 'rgba(239, 68, 68, 0.1)'
                    }
                  }}
                >
                  <Clear />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        </Box>

        {/* Hand Raise List Content */}
        <Box sx={{ flex: 1, overflow: 'auto', p: 1 }}>
          {handRaiseLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
              <CircularProgress size={32} sx={{ color: '#60a5fa' }} />
            </Box>
          ) : raisedHands.length > 0 ? (
            <List dense>
              {raisedHands.map((hand) => (
                <ListItem
                  key={hand.id}
                  sx={{
                    backgroundColor: 'rgba(255, 255, 255, 0.05)',
                    borderRadius: 2,
                    mb: 1,
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                  }}
                >
                  <ListItemAvatar>
                    <Avatar
                      sx={{
                        backgroundColor: '#f59e0b',
                        width: 36,
                        height: 36,
                      }}
                    >
                      <PanTool sx={{ fontSize: 18, color: 'white' }} />
                    </Avatar>
                  </ListItemAvatar>
                  <ListItemText
                    primary={
                      <Typography sx={{ color: 'white', fontWeight: 500 }}>
                        {hand.user?.full_name || hand.user?.name || `User ${hand.user_id}`}
                      </Typography>
                    }
                    secondary={
                      <Typography sx={{ color: 'rgba(255, 255, 255, 0.6)', fontSize: '0.75rem' }}>
                        {hand.created_at ? new Date(hand.created_at).toLocaleTimeString() : 'Just now'}
                      </Typography>
                    }
                  />
                  <ListItemSecondaryAction>
                    <Box sx={{ display: 'flex', gap: 0.5 }}>
                      <Tooltip title="Acknowledge hand">
                        <IconButton
                          size="small"
                          onClick={() => onAcknowledgeHand(hand.id)}
                          sx={{
                            color: '#4caf50',
                            '&:hover': {
                              backgroundColor: 'rgba(76, 175, 80, 0.1)',
                            },
                          }}
                        >
                          <Check sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Deny hand">
                        <IconButton
                          size="small"
                          onClick={() => onDenyHand(hand.id)}
                          sx={{
                            color: '#f44336',
                            '&:hover': {
                              backgroundColor: 'rgba(244, 67, 54, 0.1)',
                            },
                          }}
                        >
                          <Clear sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </ListItemSecondaryAction>
                </ListItem>
              ))}
            </List>
          ) : (
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                p: 4,
                textAlign: 'center',
              }}
            >
              <PanTool sx={{ fontSize: 48, color: 'rgba(255, 255, 255, 0.3)', mb: 2 }} />
              <Typography variant="h6" sx={{ color: 'rgba(255, 255, 255, 0.7)', mb: 1 }}>
                No hands raised
              </Typography>
              <Typography variant="body2" sx={{ color: 'rgba(255, 255, 255, 0.5)' }}>
                Students can raise their hands to ask questions
              </Typography>
            </Box>
          )}
        </Box>

        {/* Hand Raise Stats Footer */}
        {(totalHandsCount > 0 || handRaiseStats) && (
          <Box
            sx={{
              p: 2,
              borderTop: '1px solid rgba(255, 255, 255, 0.1)',
              background: 'rgba(0, 0, 0, 0.2)',
            }}
          >
            <Typography variant="caption" sx={{ color: 'rgba(255, 255, 255, 0.6)' }}>
              Total: {totalHandsCount} • Pending: {pendingHandsCount}
              {handRaiseStats?.acknowledged_today && ` • Acknowledged today: ${handRaiseStats.acknowledged_today}`}
            </Typography>
          </Box>
        )}
      </Box>
    </>
  );
};

export default HandRaisePanelWrapper;