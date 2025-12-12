// src/components/controls/ParticipantControls.jsx
import React from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Chip,
  IconButton,
  Tooltip,
  alpha,
  useTheme
} from '@mui/material';
import {
  People,
  MicOff,
  VideocamOff,
  Star,
  PanTool,
  Message,
  ScreenShare,
  VolumeUp,
  VolumeOff
} from '@mui/icons-material';

const ParticipantControls = ({
  participants = [],
  currentUserId,
  onToggleParticipantAudio,
  showHandRaised = true
}) => {
  const theme = useTheme();

  const sortedParticipants = participants.sort((a, b) => {
    // Sort by role: host, co-host, then participants
    const roleOrder = { host: 0, 'co-host': 1, participant: 2 };
    if (roleOrder[a.role] !== roleOrder[b.role]) {
      return roleOrder[a.role] - roleOrder[b.role];
    }
    // Then by hand raised status
    if (a.handRaised !== b.handRaised) {
      return b.handRaised ? 1 : -1;
    }
    // Finally by name
    return a.name.localeCompare(b.name);
  });

  const getRoleColor = (role) => {
    switch (role) {
      case 'host':
        return 'primary';
      case 'co-host':
        return 'warning';
      default:
        return 'default';
    }
  };

  const getRoleIcon = (role) => {
    switch (role) {
      case 'host':
      case 'co-host':
        return <Star sx={{ fontSize: '0.8rem' }} />;
      default:
        return null;
    }
  };

  return (
    <Box sx={{ width: 320, maxHeight: '70vh', overflow: 'hidden' }}>
      <Card elevation={3} sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
        <CardContent sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <People color="primary" />
            <Typography variant="h6">
              Participants ({participants.length})
            </Typography>
          </Box>

          {/* Statistics */}
          <Box sx={{ display: 'flex', gap: 1, mb: 2, flexWrap: 'wrap' }}>
            <Chip
              icon={<MicOff />}
              label={`${participants.filter(p => p.isMuted).length} Muted`}
              size="small"
              color="error"
              variant="outlined"
            />
            <Chip
              icon={<VideocamOff />}
              label={`${participants.filter(p => p.videoOff).length} Video Off`}
              size="small"
              color="error"
              variant="outlined"
            />
            {showHandRaised && (
              <Chip
                icon={<PanTool />}
                label={`${participants.filter(p => p.handRaised).length} Hands Raised`}
                size="small"
                color="warning"
                variant="outlined"
              />
            )}
          </Box>
        </CardContent>

        {/* Participants List */}
        <Box sx={{ flex: 1, overflow: 'auto', px: 2 }}>
          <List dense>
            {sortedParticipants.map((participant) => (
              <ListItem
                key={participant.id}
                sx={{
                  bgcolor: participant.id === currentUserId 
                    ? alpha(theme.palette.primary.main, 0.1) 
                    : 'transparent',
                  borderRadius: 1,
                  mb: 0.5,
                  border: participant.handRaised 
                    ? `2px solid ${theme.palette.warning.main}` 
                    : 'none'
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>
                  <Box
                    sx={{
                      width: 32,
                      height: 32,
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
                      fontSize: '0.9rem',
                      fontWeight: 'bold',
                      position: 'relative'
                    }}
                  >
                    {participant.name?.[0]?.toUpperCase() || 'U'}
                    
                    {/* Speaking Indicator */}
                    {participant.isSpeaking && (
                      <Box
                        sx={{
                          position: 'absolute',
                          top: -2,
                          right: -2,
                          width: 12,
                          height: 12,
                          borderRadius: '50%',
                          bgcolor: 'success.main',
                          border: '2px solid white',
                          animation: 'pulse 1s infinite'
                        }}
                      />
                    )}
                  </Box>
                </ListItemIcon>

                <ListItemText
                  primary={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography variant="body2" noWrap>
                        {participant.name}
                        {participant.id === currentUserId && ' (You)'}
                      </Typography>
                      {getRoleIcon(participant.role)}
                    </Box>
                  }
                  secondary={
                    <Box sx={{ display: 'flex', gap: 0.5, mt: 0.5, flexWrap: 'wrap' }}>
                      <Chip
                        label={participant.role.toUpperCase()}
                        size="small"
                        color={getRoleColor(participant.role)}
                        sx={{ fontSize: '0.6rem', height: 16 }}
                      />
                      
                      {participant.isMuted && (
                        <Chip
                          icon={<MicOff sx={{ fontSize: '0.6rem' }} />}
                          label="Muted"
                          size="small"
                          color="error"
                          sx={{ fontSize: '0.6rem', height: 16 }}
                        />
                      )}
                      
                      {participant.videoOff && (
                        <Chip
                          icon={<VideocamOff sx={{ fontSize: '0.6rem' }} />}
                          label="Video Off"
                          size="small"
                          color="error"
                          sx={{ fontSize: '0.6rem', height: 16 }}
                        />
                      )}
                      
                      {participant.isScreenSharing && (
                        <Chip
                          icon={<ScreenShare sx={{ fontSize: '0.6rem' }} />}
                          label="Sharing"
                          size="small"
                          color="info"
                          sx={{ fontSize: '0.6rem', height: 16 }}
                        />
                      )}
                      
                      {participant.handRaised && (
                        <Chip
                          label="✋ Hand Raised"
                          size="small"
                          color="warning"
                          sx={{ 
                            fontSize: '0.6rem', 
                            height: 16,
                            animation: 'pulse 2s infinite'
                          }}
                        />
                      )}
                    </Box>
                  }
                />

                {/* Participant Actions */}
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  {/* Audio Control for Others */}
                  {participant.id !== currentUserId && (
                    <Tooltip title={participant.audioMuted ? 'Unmute for yourself' : 'Mute for yourself'}>
                      <IconButton
                        size="small"
                        onClick={() => onToggleParticipantAudio?.(participant.id, !participant.audioMuted)}
                        sx={{ 
                          color: participant.audioMuted ? 'error.main' : 'text.secondary',
                          '&:hover': {
                            bgcolor: participant.audioMuted ? 'error.light' : 'action.hover'
                          }
                        }}
                      >
                        {participant.audioMuted ? <VolumeOff /> : <VolumeUp />}
                      </IconButton>
                    </Tooltip>
                  )}

                  {/* Message Button */}
                  <Tooltip title="Send private message">
                    <IconButton
                      size="small"
                      sx={{
                        color: 'text.secondary',
                        '&:hover': {
                          bgcolor: 'action.hover',
                          color: 'primary.main'
                        }
                      }}
                    >
                      <Message />
                    </IconButton>
                  </Tooltip>
                </Box>
              </ListItem>
            ))}
          </List>
        </Box>

        {/* Footer Info */}
        <CardContent sx={{ pt: 1, bgcolor: alpha(theme.palette.grey[500], 0.05) }}>
          <Typography variant="caption" color="text.secondary" align="center" display="block">
            {participants.filter(p => p.isSpeaking).length > 0 
              ? `${participants.filter(p => p.isSpeaking).length} people speaking`
              : 'No one is speaking'
            }
          </Typography>
        </CardContent>
      </Card>

      <style jsx>{`
        @keyframes pulse {
          0% { opacity: 1; transform: scale(1); }
          50% { opacity: 0.7; transform: scale(1.05); }
          100% { opacity: 1; transform: scale(1); }
        }
      `}</style>
    </Box>
  );
};

export default ParticipantControls;