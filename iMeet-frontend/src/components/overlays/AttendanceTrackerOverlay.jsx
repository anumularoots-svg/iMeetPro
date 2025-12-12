// src/components/overlays/AttendanceTrackerOverlay.jsx
import React from 'react';
import { Box } from '@mui/material';
import AttendanceTracker from '../attendance/AttendanceTracker';

const AttendanceTrackerOverlay = ({
  enabled,
  minimized,
  meetingId,
  userId,
  userName,
  isActive,
  cameraEnabled,
  onViolation,
  onStatusChange,
  onSessionTerminated,
  onToggleMinimized,
  isHost,
  isCoHost,
  effectiveRole,
  onCameraToggle,
  chatOpen,
  participantsOpen,
}) => {
  if (!enabled || !meetingId || !userId) return null;

  return (
    <Box
      sx={{
        position: 'absolute',
        top: 16,
        right: chatOpen || participantsOpen ? 428 : 16,
        width: minimized ? 'auto' : 400,
        maxHeight: minimized ? 'auto' : 'calc(100vh - 200px)',
        zIndex: 1100,
        transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
      }}
    >
      <AttendanceTracker
        meetingId={meetingId}
        userId={userId}
        userName={userName}
        isActive={isActive}
        cameraEnabled={cameraEnabled}
        onViolation={onViolation}
        onStatusChange={onStatusChange}
        onSessionTerminated={onSessionTerminated}
        minimized={minimized}
        onToggleMinimized={onToggleMinimized}
        isHost={isHost}
        isCoHost={isCoHost}
        effectiveRole={effectiveRole}
        onCameraToggle={onCameraToggle}
      />
    </Box>
  );
};

export default AttendanceTrackerOverlay;