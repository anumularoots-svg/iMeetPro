// src/components/tabs/WhiteboardTabContent.jsx
import React from 'react';
import { Box, Typography, Button } from '@mui/material';
import { Gesture as WhiteboardIcon } from '@mui/icons-material';
import Whiteboard from '../whiteboard/Whiteboard';

const WhiteboardTabContent = ({
  meetingId,
  currentUser,
  allParticipants,
  hasHostPrivileges,
  room,
  onClose,
  onError,
  onSuccess,
}) => {
  if (!hasHostPrivileges) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100%',
          gap: 2,
          p: 4,
        }}
      >
        <WhiteboardIcon sx={{ fontSize: 64, color: 'rgba(0, 0, 0, 0.3)' }} />
        <Typography variant="h5" sx={{ color: 'rgba(0, 0, 0, 0.7)', textAlign: 'center' }}>
          Whiteboard Access Restricted
        </Typography>
        <Typography variant="body1" sx={{ color: 'rgba(0, 0, 0, 0.5)', textAlign: 'center', maxWidth: 400 }}>
          Only hosts and co-hosts can access the whiteboard. Please contact the meeting host if you need whiteboard access.
        </Typography>
        <Button
          variant="outlined"
          onClick={onClose}
          sx={{ mt: 2 }}
        >
          Close Whiteboard
        </Button>
      </Box>
    );
  }

  return (
    <Box sx={{ flex: 1, height: '100%', overflow: 'hidden', background: '#f8f9fa' }}>
      <Whiteboard
        meetingId={meetingId}
        currentUser={currentUser}
        participants={allParticipants}
        isHost={hasHostPrivileges}
        socket={room}
        onClose={onClose}
        isOpen={true}
        onError={onError}
        onSuccess={onSuccess}
        sx={{
          height: '100%',
          width: '100%',
          '& .MuiPaper-root': {
            borderRadius: 0,
            height: '100%',
          },
        }}
      />
    </Box>
  );
};

export default WhiteboardTabContent;