// src/components/controls/MeetingActionsMenu.jsx
import React from 'react';
import {
  Card,
  List,
  ListItem,
  ListItemText,
  Box,
  Chip,
  Fade,
} from '@mui/material';
import {
  RadioButtonChecked,
  Gesture as WhiteboardIcon,
  Share,
  MeetingRoom as MeetingRoomIcon,
  Fullscreen,
  FullscreenExit,
} from '@mui/icons-material';

const MeetingActionsMenu = ({
  open,
  onClose,
  chatOpen,
  participantsOpen,
  recordingState,
  hasHostPrivileges,
  meetingSettings,
  attendanceEnabled,
  currentAttendanceData,
  isFullscreen,
  toggleMenuItems,
  onItemClick,
}) => {
  if (!open) return null;

  const getMenuItemColor = (label) => {
    if (label.includes('Recording')) return '#ef4444';
    if (label.includes('Whiteboard')) return '#8b5cf6';
    if (label.includes('Attendance')) return '#22c55e';
    if (label.includes('Copy') || label.includes('Share')) return '#3b82f6';
    if (label.includes('End Meeting')) return '#dc2626';
    if (label.includes('Fullscreen')) return '#6b7280';
    return '#6b7280';
  };

  return (
    <Fade in={open}>
      <Card
        className="toggle-menu-container"
        sx={{
          position: 'fixed',
          bottom: { xs: 120, md: 140 },
          right: { xs: '50%', md: chatOpen || participantsOpen ? 660 : 240 },
          transform: { xs: 'translateX(50%)', md: 'none' },
          background: 'rgba(255, 255, 255, 0.98)',
          backdropFilter: 'blur(20px)',
          WebkitBackdropFilter: 'blur(20px)',
          borderRadius: 1.5,
          border: '1px solid rgba(0, 0, 0, 0.08)',
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.15)',
          color: '#1f2937',
          width: 280,
          maxHeight: '50vh',
          overflow: 'auto',
          zIndex: 10000,
          transition: 'right 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        }}
      >
        <List sx={{ p: 1 }}>
          {toggleMenuItems
            .filter((item) => item.show)
            .map((item, index) => (
              <ListItem
                key={index}
                button
                onClick={() => {
                  onItemClick(item.action);
                  onClose();
                }}
                sx={{
                  borderRadius: 2,
                  mb: 0.5,
                  py: 1.5,
                  px: 2,
                  transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
                  '&:hover': {
                    backgroundColor: 'rgba(0, 0, 0, 0.04)',
                    transform: 'translateX(4px)',
                  },
                }}
              >
                {/* Icon */}
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    width: 40,
                    height: 40,
                    borderRadius: '50%',
                    backgroundColor: getMenuItemColor(item.label),
                    color: 'white',
                    mr: 2,
                    flexShrink: 0,
                  }}
                >
                  {item.icon}
                </Box>

                {/* Label */}
                <ListItemText
                  primary={item.label}
                  primaryTypographyProps={{
                    color: '#1f2937',
                    fontSize: '0.875rem',
                    fontWeight: 500,
                  }}
                />

                {/* Badge for recording */}
                {item.label.includes('Recording') && recordingState?.isRecording && (
                  <Chip
                    label="LIVE"
                    size="small"
                    sx={{
                      height: 20,
                      backgroundColor: '#ef4444',
                      color: 'white',
                      fontSize: '0.65rem',
                      fontWeight: 700,
                      animation: 'pulse 2s infinite',
                    }}
                  />
                )}

                {/* Badge for attendance */}
                {item.label.includes('Attendance') && attendanceEnabled && (
                  <Box
                    sx={{
                      width: 8,
                      height: 8,
                      borderRadius: '50%',
                      backgroundColor: currentAttendanceData?.attendancePercentage > 80
                        ? '#22c55e'
                        : currentAttendanceData?.attendancePercentage > 60
                          ? '#f59e0b'
                          : '#ef4444',
                    }}
                  />
                )}
              </ListItem>
            ))}
        </List>
      </Card>
    </Fade>
  );
};

export default MeetingActionsMenu;