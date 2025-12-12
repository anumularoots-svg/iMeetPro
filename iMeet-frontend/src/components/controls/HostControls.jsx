// src/components/controls/HostControls.jsx
import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Switch,
  Button,
  IconButton,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction, 
  Divider,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  FormControlLabel,
  Alert,
  useTheme,
  alpha
} from '@mui/material';
import {
  Security,
  MicOff,
  VideocamOff,
  RemoveCircle,
  Star,
  Block,
  Settings,
  Lock,
  LockOpen,
  RecordVoiceOver,
  VoiceOverOff,
  People,
  Chat,
  ScreenShare,
  Close
} from '@mui/icons-material';

const HostControls = ({
  participants = [],
  meetingSettings = {},
  onUpdateMeetingSettings,
  onMuteParticipant,
  onUnmuteParticipant,
  onMuteAll,
  onUnmuteAll,
  onRemoveParticipant,
  onPromoteToCoHost,
  onDemoteFromCoHost,
  onToggleWaitingRoom,
  onLockMeeting,
  onUnlockMeeting,
  onEndMeetingForAll,
  isRecording,
  onToggleRecording
}) => {
  const theme = useTheme();
  const [openDialog, setOpenDialog] = useState(null);
  const [selectedParticipant, setSelectedParticipant] = useState(null);

  const handleDialog = (dialogType, participant = null) => {
    setOpenDialog(dialogType);
    setSelectedParticipant(participant);
  };

  const closeDialog = () => {
    setOpenDialog(null);
    setSelectedParticipant(null);
  };

  const handleConfirmAction = () => {
    switch (openDialog) {
      case 'remove':
        onRemoveParticipant(selectedParticipant.id);
        break;
      case 'endMeeting':
        onEndMeetingForAll();
        break;
      case 'promote':
        onPromoteToCoHost(selectedParticipant.id);
        break;
      case 'demote':
        onDemoteFromCoHost(selectedParticipant.id);
        break;
      default:
        break;
    }
    closeDialog();
  };

  const ParticipantActions = ({ participant }) => (
    <Box sx={{ display: 'flex', gap: 1 }}>
      {/* Mute/Unmute */}
      <IconButton
        size="small"
        onClick={() => participant.isMuted ? onUnmuteParticipant(participant.id) : onMuteParticipant(participant.id)}
        color={participant.isMuted ? 'error' : 'default'}
      >
        {participant.isMuted ? <MicOff /> : <RecordVoiceOver />}
      </IconButton>

      {/* Video Control */}
      <IconButton
        size="small"
        onClick={() => participant.videoOff ? 'enable video' : 'disable video'}
        color={participant.videoOff ? 'error' : 'default'}
      >
        {participant.videoOff ? <VideocamOff /> : <Settings />}
      </IconButton>

      {/* Promote/Demote */}
      {participant.role === 'participant' ? (
        <IconButton
          size="small"
          onClick={() => handleDialog('promote', participant)}
          color="primary"
        >
          <Star />
        </IconButton>
      ) : participant.role === 'co-host' ? (
        <IconButton
          size="small"
          onClick={() => handleDialog('demote', participant)}
          color="warning"
        >
          <Star />
        </IconButton>
      ) : null}

      {/* Remove Participant */}
      <IconButton
        size="small"
        onClick={() => handleDialog('remove', participant)}
        color="error"
      >
        <RemoveCircle />
      </IconButton>
    </Box>
  );

  return (
    <Box sx={{ width: 350, maxHeight: '80vh', overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
      {/* Host Control Header */}
      <Card elevation={3} sx={{ mb: 2 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <Security color="primary" />
            <Typography variant="h6" color="primary">
              Host Controls
            </Typography>
            <Chip
              label="HOST"
              size="small"
              color="primary"
              sx={{ ml: 'auto' }}
            />
          </Box>

          {/* Quick Actions */}
          <Box sx={{ display: 'flex', gap: 1, mb: 2, flexWrap: 'wrap' }}>
            <Button
              size="small"
              variant="outlined"
              color="error"
              startIcon={<MicOff />}
              onClick={onMuteAll}
            >
              Mute All
            </Button>
            <Button
              size="small"
              variant="outlined"
              color="success"
              startIcon={<RecordVoiceOver />}
              onClick={onUnmuteAll}
            >
              Unmute All
            </Button>
            <Button
              size="small"
              variant="outlined"
              color={meetingSettings.isLocked ? 'error' : 'primary'}
              startIcon={meetingSettings.isLocked ? <Lock /> : <LockOpen />}
              onClick={meetingSettings.isLocked ? onUnlockMeeting : onLockMeeting}
            >
              {meetingSettings.isLocked ? 'Unlock' : 'Lock'}
            </Button>
          </Box>

          {/* Meeting Settings */}
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={meetingSettings.waitingRoomEnabled}
                  onChange={onToggleWaitingRoom}
                  size="small"
                />
              }
              label="Waiting Room"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={meetingSettings.chatEnabled}
                  onChange={(e) => onUpdateMeetingSettings({ chatEnabled: e.target.checked })}
                  size="small"
                />
              }
              label="Allow Chat"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={meetingSettings.screenShareEnabled}
                  onChange={(e) => onUpdateMeetingSettings({ screenShareEnabled: e.target.checked })}
                  size="small"
                />
              }
              label="Allow Screen Share"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={isRecording}
                  onChange={onToggleRecording}
                  size="small"
                />
              }
              label={`Recording ${isRecording ? '(Active)' : ''}`}
            />
          </Box>
        </CardContent>
      </Card>

      {/* Participants Management */}
      <Card elevation={3} sx={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        <CardContent sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <People color="primary" />
            <Typography variant="h6">
              Participants ({participants.length})
            </Typography>
          </Box>
        </CardContent>

        <Box sx={{ flex: 1, overflow: 'auto', px: 2 }}>
          <List dense>
            {participants.map((participant) => (
              <React.Fragment key={participant.id}>
                <ListItem sx={{ px: 0 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                    {/* Participant Avatar */}
                    <Box
                      sx={{
                        width: 40,
                        height: 40,
                        borderRadius: '50%',
                        bgcolor: participant.role === 'host' 
                          ? 'primary.main' 
                          : participant.role === 'co-host' 
                          ? 'warning.main' 
                          : 'grey.400',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        color: 'white',
                        fontSize: '1rem',
                        fontWeight: 'bold'
                      }}
                    >
                      {participant.name?.[0]?.toUpperCase() || 'U'}
                    </Box>

                    {/* Participant Info */}
                    <Box sx={{ flex: 1, minWidth: 0 }}>
                      <Typography variant="body2" noWrap>
                        {participant.name}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center' }}>
                        <Chip
                          label={participant.role.toUpperCase()}
                          size="small"
                          color={participant.role === 'host' ? 'primary' : participant.role === 'co-host' ? 'warning' : 'default'}
                          sx={{ fontSize: '0.6rem', height: 16 }}
                        />
                        {participant.isMuted && (
                          <Chip
                            icon={<MicOff sx={{ fontSize: '0.7rem' }} />}
                            label="Muted"
                            size="small"
                            color="error"
                            sx={{ fontSize: '0.6rem', height: 16 }}
                          />
                        )}
                        {participant.videoOff && (
                          <Chip
                            icon={<VideocamOff sx={{ fontSize: '0.7rem' }} />}
                            label="Video Off"
                            size="small"
                            color="error"
                            sx={{ fontSize: '0.6rem', height: 16 }}
                          />
                        )}
                        {participant.handRaised && (
                          <Chip
                            label="✋"
                            size="small"
                            color="warning"
                            sx={{ fontSize: '0.6rem', height: 16 }}
                          />
                        )}
                      </Box>
                    </Box>

                    {/* Actions */}
                    {participant.role !== 'host' && (
                      <ParticipantActions participant={participant} />
                    )}
                  </Box>
                </ListItem>
                <Divider />
              </React.Fragment>
            ))}
          </List>
        </Box>

        {/* Danger Zone */}
        <CardContent sx={{ pt: 1, bgcolor: alpha(theme.palette.error.main, 0.05) }}>
          <Button
            fullWidth
            variant="outlined"
            color="error"
            startIcon={<Close />}
            onClick={() => handleDialog('endMeeting')}
          >
            End Meeting for All
          </Button>
        </CardContent>
      </Card>

      {/* Confirmation Dialogs */}
      <Dialog open={openDialog === 'remove'} onClose={closeDialog}>
        <DialogTitle>Remove Participant</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to remove {selectedParticipant?.name} from the meeting?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeDialog}>Cancel</Button>
          <Button onClick={handleConfirmAction} color="error">
            Remove
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={openDialog === 'promote'} onClose={closeDialog}>
        <DialogTitle>Promote to Co-Host</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to promote {selectedParticipant?.name} to co-host?
            They will have additional permissions in the meeting.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeDialog}>Cancel</Button>
          <Button onClick={handleConfirmAction} color="primary">
            Promote
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={openDialog === 'demote'} onClose={closeDialog}>
        <DialogTitle>Demote from Co-Host</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to demote {selectedParticipant?.name} from co-host to participant?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeDialog}>Cancel</Button>
          <Button onClick={handleConfirmAction} color="warning">
            Demote
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={openDialog === 'endMeeting'} onClose={closeDialog}>
        <DialogTitle color="error.main">End Meeting for All</DialogTitle>
        <DialogContent>
          <Alert severity="warning" sx={{ mb: 2 }}>
            This action cannot be undone!
          </Alert>
          <Typography>
            Are you sure you want to end the meeting for all participants?
            Everyone will be removed from the meeting immediately.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeDialog}>Cancel</Button>
          <Button onClick={handleConfirmAction} color="error" variant="contained">
            End Meeting
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default HostControls;
