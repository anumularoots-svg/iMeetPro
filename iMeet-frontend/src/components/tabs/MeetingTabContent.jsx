// src/components/tabs/MeetingTabContent.jsx
import React from 'react';
import { Box, Typography, CircularProgress, Button } from '@mui/material';
import { VideoCall } from '@mui/icons-material';
import VideoGrid from '../video/VideoGrid';

const MeetingTabContent = ({
  // Connection state
  actualIsConnected,
  isConnecting,
  connectionAttemptRef,
  
  // Participants
  allParticipants,
  
  // Streams
  localStream,
  combinedStreams,
  enhancedScreenShareData,
  
  // User info
  currentUser,
  hasHostPrivileges,
  
  // Handlers
  onRemoveParticipant,
  onPromoteToCoHost,
  onRemoveCoHost,
  handleParticipantsUpdated,
  establishLiveKitConnection,
  
  // Settings
  viewMode,
  currentPerformanceMode,
  currentMaxParticipants,
  
  // Co-hosts
  coHosts,
  
  // Attendance
  currentAttendanceData,
}) => {
  return (
    <Box sx={{ 
      flex: 1, 
      display: 'flex', 
      overflow: 'hidden', 
      minHeight: 0,
      height: '100%',
      position: 'relative',
      transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
      borderRadius: '12px',
      margin: 0,
    }}>
      {actualIsConnected || allParticipants.length > 0 ? (
        <VideoGrid
          participants={allParticipants}
          localStream={localStream}
          currentUser={currentUser}
          screenShareStream={enhancedScreenShareData?.stream}
          isScreenSharing={!!enhancedScreenShareData?.stream && !!enhancedScreenShareData?.sharer}
          screenSharer={enhancedScreenShareData?.sharer}
          remoteStreams={combinedStreams}
          onMuteParticipant={() => {}}
          onRemoveParticipant={onRemoveParticipant}
          // onRemoveParticipant={handleRemoveParticipant}
          onPromoteToHost={onPromoteToCoHost}
          onRemoveCoHost={onRemoveCoHost}
          onParticipantRemoved={(removedUserId) => {
            setTimeout(() => {
              handleParticipantsUpdated();
            }, 500);
          }}
          viewMode={viewMode}
          containerHeight="100%"
          containerWidth="100%"
          performanceMode={currentPerformanceMode}
          maxParticipants={currentMaxParticipants}
          isHost={hasHostPrivileges}
          coHosts={coHosts}
          attendanceData={currentAttendanceData}
        />
      ) : (
        <Box
          sx={{
            width: '100%',
            height: '100%',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'linear-gradient(135deg, rgba(0,0,0,0.4) 0%, rgba(0,0,0,0.2) 100%)',
            borderRadius: 2,
            border: '1px solid rgba(255, 255, 255, 0.05)',
            gap: 2,
          }}
        >
          {isConnecting || connectionAttemptRef.current ? (
            <>
              <CircularProgress size={64} sx={{ color: '#3b82f6' }} />
              <Typography variant="h6" sx={{ color: 'rgba(255,255,255,0.8)', fontWeight: 500 }}>
                Connecting to meeting...
              </Typography>
              <Typography variant="body2" sx={{ color: 'rgba(255,255,255,0.6)' }}>
                Please wait while we establish the connection
              </Typography>
            </>
          ) : (
            <>
              <VideoCall sx={{ fontSize: 64, color: 'rgba(255,255,255,0.5)', mb: 2 }} />
              <Typography variant="h6" sx={{ color: 'rgba(255,255,255,0.8)', textAlign: 'center' }}>
                {actualIsConnected
                  ? `${allParticipants.length} participants in meeting`
                  : "Connection failed"}
              </Typography>
              <Typography variant="body2" sx={{ color: 'rgba(255,255,255,0.6)', textAlign: 'center' }}>
                {actualIsConnected
                  ? hasHostPrivileges
                    ? "Students will appear here when they join"
                    : "You will see yourself and the host when connected"
                  : "Please check your connection and try again"}
              </Typography>
              {!actualIsConnected && (
                <Button
                  variant="outlined"
                  onClick={establishLiveKitConnection}
                  sx={{
                    mt: 2,
                    color: 'white',
                    borderColor: 'rgba(255,255,255,0.3)',
                    '&:hover': {
                      borderColor: 'rgba(255,255,255,0.5)',
                      backgroundColor: 'rgba(255,255,255,0.05)'
                    }
                  }}
                >
                  Retry Connection
                </Button>
              )}
            </>
          )}
        </Box>
      )}
    </Box>
  );
};

export default MeetingTabContent;