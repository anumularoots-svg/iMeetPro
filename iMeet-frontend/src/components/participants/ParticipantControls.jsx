// src/components/participants/ParticipantControls.jsx - COMPLETE WORKING VERSION
import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Typography,
  Tooltip,
  useTheme,
  alpha,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Slider,
} from '@mui/material';
import {
  MoreVert,
  Mic,
  MicOff,
  Videocam,
  VideocamOff,
  SupervisedUserCircle,
  ExitToApp,
  Visibility,
  VisibilityOff,
  VolumeUp,
  PushPin,
  PushPinOutlined,
  RemoveCircle,
} from '@mui/icons-material';

const ParticipantControls = ({
  participant,
  currentUserRole = 'participant',
  isCurrentUser = false,
  onMuteParticipant,
  onUnmuteParticipant,
  onMuteVideo,
  onUnmuteVideo,
  onRemoveParticipant,
  onPromoteToCoHost,
  onDemoteFromCoHost,
  onSetVolume,
  onSpotlight,
  onPin,
}) => {
  const theme = useTheme();
  const [anchorEl, setAnchorEl] = useState(null);
  const [confirmDialog, setConfirmDialog] = useState({ 
    open: false, 
    action: null, 
    title: '', 
    content: '' 
  });
  const [volumeDialog, setVolumeDialog] = useState(false);
  const [volume, setVolume] = useState(participant.volume || 100);

  const isHost = currentUserRole === 'host';
  const isCoHost = currentUserRole === 'co-host';
  const hasHostPrivileges = isHost || isCoHost;
  const participantIsHost = participant.role === 'host';
  const participantIsCoHost = participant.role === 'co-host';

  const handleMenuOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
  };

  const handleConfirmAction = (action, title, content) => {
    setConfirmDialog({ open: true, action, title, content });
    handleMenuClose();
  };

  const executeAction = async () => {
    const { action } = confirmDialog;
    
    try {
      const participantId = participant.id || participant.user_id || participant.User_ID;
      
      switch (action) {
        case 'mute_audio':
          if (onMuteParticipant) {
            await onMuteParticipant(participantId);
          }
          break;
          
        case 'unmute_audio':
          if (onUnmuteParticipant) {
            await onUnmuteParticipant(participantId);
          }
          break;
          
        case 'mute_video':
          if (onMuteVideo) {
            await onMuteVideo(participantId);
          }
          break;
          
        case 'unmute_video':
          if (onUnmuteVideo) {
            await onUnmuteVideo(participantId);
          }
          break;
          
        case 'spotlight':
          if (onSpotlight) {
            await onSpotlight(participantId, !participant.spotlighted);
          }
          break;
          
        case 'pin':
          if (onPin) {
            await onPin(participantId, !participant.pinned);
          }
          break;
          
        case 'promote':
          if (onPromoteToCoHost) {
            await onPromoteToCoHost({
              userId: participantId,
              participant: participant
            });
          }
          break;
          
        case 'demote':
          if (onDemoteFromCoHost) {
            await onDemoteFromCoHost(
              participantId,
              participant.full_name || participant.name || `User ${participantId}`
            );
          }
          break;
          
        case 'remove':
          if (onRemoveParticipant) {
            await onRemoveParticipant({
              userId: participantId,
              participant: participant
            });
          }
          break;
          
        default:
          break;
      }
    } catch (error) {
      console.error('❌ Action execution failed:', error);
    }
    
    setConfirmDialog({ open: false, action: null, title: '', content: '' });
  };

  const handleVolumeChange = (event, newValue) => {
    setVolume(newValue);
  };

  const handleVolumeApply = async () => {
    try {
      const participantId = participant.id || participant.user_id || participant.User_ID;
      if (onSetVolume) {
        await onSetVolume(participantId, volume);
      }
      setVolumeDialog(false);
    } catch (error) {
      console.error('❌ Volume adjustment failed:', error);
    }
  };

  const handleDirectAction = async (action) => {
    handleMenuClose();
    const participantId = participant.id || participant.user_id || participant.User_ID;
    
    try {
      switch (action) {
        case 'spotlight':
          if (onSpotlight) {
            await onSpotlight(participantId, !participant.spotlighted);
          }
          break;
          
        case 'pin':
          if (onPin) {
            await onPin(participantId, !participant.pinned);
          }
          break;
          
        case 'volume':
          setVolumeDialog(true);
          break;
          
        default:
          break;
      }
    } catch (error) {
      console.error('❌ Direct action failed:', error);
    }
  };

  // Don't show controls for current user
  if (isCurrentUser) {
    return null;
  }

  const getParticipantName = () => {
    return participant.full_name || 
           participant.Full_Name || 
           participant.name || 
           participant.displayName || 
           `User ${participant.id || participant.user_id}`;
  };

  return (
    <>
      <Tooltip title="Participant controls">
        <IconButton
          size="small"
          onClick={handleMenuOpen}
          sx={{
            color: theme.palette.text.secondary,
            '&:hover': {
              backgroundColor: alpha(theme.palette.primary.main, 0.1),
              color: theme.palette.primary.main,
            }
          }}
        >
          <MoreVert />
        </IconButton>
      </Tooltip>

      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        PaperProps={{
          sx: {
            borderRadius: 1,
            minWidth: 220,
            boxShadow: theme.shadows[8],
            '& .MuiMenuItem-root': {
              borderRadius: 1,
              mx: 1,
              my: 0.5,
            }
          }
        }}
      >
        {/* Audio Controls - Host/Co-host only */}
        {hasHostPrivileges && !isCurrentUser && (
          <>
            <MenuItem
              onClick={() => {
                if (participant.audio_enabled || participant.isAudioEnabled) {
                  handleConfirmAction(
                    'mute_audio',
                    'Mute Microphone',
                    `Mute ${getParticipantName()}'s microphone?`
                  );
                } else {
                  handleConfirmAction(
                    'unmute_audio',
                    'Allow Unmute',
                    `Allow ${getParticipantName()} to unmute their microphone?`
                  );
                }
              }}
            >
              <ListItemIcon>
                {(participant.audio_enabled || participant.isAudioEnabled) ? (
                  <MicOff sx={{ color: theme.palette.error.main }} />
                ) : (
                  <Mic sx={{ color: theme.palette.success.main }} />
                )}
              </ListItemIcon>
              <ListItemText>
                {(participant.audio_enabled || participant.isAudioEnabled) ? 'Mute Microphone' : 'Allow Unmute'}
              </ListItemText>
            </MenuItem>

            <MenuItem
              onClick={() => {
                if (participant.video_enabled || participant.isVideoEnabled) {
                  handleConfirmAction(
                    'mute_video',
                    'Turn Off Camera',
                    `Turn off ${getParticipantName()}'s camera?`
                  );
                } else {
                  handleConfirmAction(
                    'unmute_video',
                    'Allow Camera',
                    `Allow ${getParticipantName()} to turn on their camera?`
                  );
                }
              }}
            >
              <ListItemIcon>
                {(participant.video_enabled || participant.isVideoEnabled) ? (
                  <VideocamOff sx={{ color: theme.palette.error.main }} />
                ) : (
                  <Videocam sx={{ color: theme.palette.success.main }} />
                )}
              </ListItemIcon>
              <ListItemText>
                {(participant.video_enabled || participant.isVideoEnabled) ? 'Turn Off Camera' : 'Allow Camera'}
              </ListItemText>
            </MenuItem>

          </>
        )}


        {/* Role Management - Host only */}
        {isHost && !isCurrentUser && !participantIsHost && (
          <>
            <Divider />
            <MenuItem
              onClick={() => {
                if (participantIsCoHost) {
                  handleConfirmAction(
                    'demote',
                    'Remove Co-Host',
                    `Remove co-host privileges from ${getParticipantName()}?`
                  );
                } else {
                  handleConfirmAction(
                    'promote',
                    'Make Co-Host',
                    `Give co-host privileges to ${getParticipantName()}?`
                  );
                }
              }}
            >
              <ListItemIcon>
                <SupervisedUserCircle sx={{ color: theme.palette.warning.main }} />
              </ListItemIcon>
              <ListItemText>
                {participantIsCoHost ? 'Remove Co-Host' : 'Make Co-Host'}
              </ListItemText>
            </MenuItem>
          </>
        )}

        {/* Remove Participant - Host/Co-host only */}
        {hasHostPrivileges && !isCurrentUser && !participantIsHost && (
          <>
            <Divider />
            <MenuItem
              onClick={() => handleConfirmAction(
                'remove',
                'Remove Participant',
                `Remove ${getParticipantName()} from the meeting? They will be disconnected immediately and cannot rejoin.`
              )}
              sx={{ color: theme.palette.error.main }}
            >
              <ListItemIcon>
                <ExitToApp sx={{ color: theme.palette.error.main }} />
              </ListItemIcon>
              <ListItemText>Remove from Meeting</ListItemText>
            </MenuItem>
          </>
        )}
      </Menu>

      {/* Confirmation Dialog */}
      <Dialog
        open={confirmDialog.open}
        onClose={() => setConfirmDialog({ open: false, action: null, title: '', content: '' })}
        PaperProps={{
          sx: { 
            borderRadius: 1,
            background: 'rgba(255, 255, 255, 0.98)',
            backdropFilter: 'blur(20px)',
          }
        }}
      >
        <DialogTitle sx={{ pb: 1, fontWeight: 600 }}>
          {confirmDialog.title}
        </DialogTitle>
        <DialogContent>
          <Typography sx={{ color: 'text.secondary' }}>
            {confirmDialog.content}
          </Typography>
        </DialogContent>
        <DialogActions sx={{ p: 2, gap: 1 }}>
          <Button 
            onClick={() => setConfirmDialog({ open: false, action: null, title: '', content: '' })}
            variant="outlined"
            sx={{ textTransform: 'none' }}
          >
            Cancel
          </Button>
          <Button 
            onClick={executeAction}
            variant="contained"
            color={confirmDialog.action === 'remove' ? 'error' : 'primary'}
            sx={{ textTransform: 'none' }}
          >
            Confirm
          </Button>
        </DialogActions>
      </Dialog>

  
    </>
  );
};

export default ParticipantControls;