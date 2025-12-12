// src/components/panels/ParticipantsPanelWrapper.jsx
import React from 'react';
import { Box } from '@mui/material';
import ParticipantsList from '../participants/ParticipantsList';

const ParticipantsPanelWrapper = ({
  isOpen,
  onClose,
  participants,
  currentUser,
  isHost,
  isCoHost,
  coHosts,
  hasHostPrivileges,
  onMuteParticipant,
  onUnmuteParticipant,
  onMuteVideo,
  onUnmuteVideo,
  onRemoveParticipant,
  onPromoteToCoHost,
  onRemoveCoHost,
  onParticipantsUpdated,
}) => {
  if (!isOpen) return null;

  return (
    <>
      {/* Mobile Overlay */}
      <Box
        sx={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.5)',
          backdropFilter: 'blur(8px)',
          WebkitBackdropFilter: 'blur(8px)',
          zIndex: 1500,
          display: { xs: 'block', lg: 'none' },
        }}
        onClick={onClose}
      />

      {/* Desktop Side Panel */}
      <Box
        className="participants-panel-container"
        sx={{
          width: { lg: 400 },
          height: '100%',
          minHeight: 0,
          background: 'rgba(26, 32, 44, 0.98)',
          backdropFilter: 'blur(20px)',
          WebkitBackdropFilter: 'blur(20px)',
          borderLeft: '1px solid rgba(255, 255, 255, 0.08)',
          borderRadius: '12px',
          margin: 0,
          overflow: 'hidden',
          flexShrink: 0,
          display: { xs: 'none', lg: 'flex' },
          flexDirection: 'column',

          '@media (max-width: 1279px)': {
            position: 'fixed',
            top: '10%',
            left: '5%',
            right: '5%',
            width: 'auto',
            height: '80vh',
            maxHeight: '600px',
            borderRadius: 2,
            border: '1px solid rgba(255, 255, 255, 0.08)',
            margin: 0,
            zIndex: 1600,
            display: 'flex',
          }
        }}
      >
        <ParticipantsList
          participants={participants}
          currentUser={currentUser}
          isHost={isHost}
          isCoHost={isCoHost}
          coHosts={coHosts}
          onMuteParticipant={onMuteParticipant}
          onUnmuteParticipant={onUnmuteParticipant}
          onMuteVideo={onMuteVideo}
          onUnmuteVideo={onUnmuteVideo}
          onRemoveParticipant={onRemoveParticipant}
          onPromoteToCoHost={onPromoteToCoHost}
          onRemoveCoHost={onRemoveCoHost}
          hasHostPrivileges={hasHostPrivileges}
          onParticipantsUpdated={onParticipantsUpdated}
          currentUserId={currentUser?.id}
          onPanelClose={onClose}
        />
      </Box>
    </>
  );
};

export default ParticipantsPanelWrapper;