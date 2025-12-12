import React, { useEffect, useRef, useState, useCallback } from 'react';
import {
  Box,
  Avatar,
  Chip,
  Typography,
  Tooltip,
  Badge,
  Fade,
} from '@mui/material';
import { Edit, Visibility } from '@mui/icons-material';

const WhiteboardCollaboration = ({ 
  meetingId, 
  participants, 
  currentUser, 
  onParticipantUpdate,
  socket 
}) => {
  const [activeParticipants, setActiveParticipants] = useState(new Map());
  const [cursors, setCursors] = useState(new Map());
  const [drawingIndicators, setDrawingIndicators] = useState(new Map());
  const cursorUpdateTimeoutRef = useRef(new Map());

  // Listen for participant activities
  useEffect(() => {
    if (!socket) return;

    const handleParticipantJoined = (participant) => {
      setActiveParticipants(prev => new Map(prev.set(participant.id, {
        ...participant,
        joinedAt: Date.now(),
        isDrawing: false,
        lastActivity: Date.now(),
      })));
    };

    const handleParticipantLeft = (participantId) => {
      setActiveParticipants(prev => {
        const newMap = new Map(prev);
        newMap.delete(participantId);
        return newMap;
      });
      
      setCursors(prev => {
        const newMap = new Map(prev);
        newMap.delete(participantId);
        return newMap;
      });
      
      setDrawingIndicators(prev => {
        const newMap = new Map(prev);
        newMap.delete(participantId);
        return newMap;
      });
    };

    const handleCursorMove = (data) => {
      const { participantId, position, participantInfo } = data;
      
      if (participantId === currentUser.id) return; // Don't show own cursor
      
      setCursors(prev => new Map(prev.set(participantId, {
        position,
        participant: participantInfo,
        timestamp: Date.now(),
      })));

      // Clear cursor after inactivity
      const timeoutId = cursorUpdateTimeoutRef.current.get(participantId);
      if (timeoutId) {
        clearTimeout(timeoutId);
      }

      const newTimeoutId = setTimeout(() => {
        setCursors(prev => {
          const newMap = new Map(prev);
          newMap.delete(participantId);
          return newMap;
        });
        cursorUpdateTimeoutRef.current.delete(participantId);
      }, 3000);

      cursorUpdateTimeoutRef.current.set(participantId, newTimeoutId);
    };

    const handleDrawingStart = (data) => {
      const { participantId, participantInfo } = data;
      
      setDrawingIndicators(prev => new Map(prev.set(participantId, {
        participant: participantInfo,
        isDrawing: true,
        startTime: Date.now(),
      })));

      setActiveParticipants(prev => {
        const newMap = new Map(prev);
        const participant = newMap.get(participantId);
        if (participant) {
          newMap.set(participantId, {
            ...participant,
            isDrawing: true,
            lastActivity: Date.now(),
          });
        }
        return newMap;
      });
    };

    const handleDrawingEnd = (data) => {
      const { participantId } = data;
      
      setDrawingIndicators(prev => {
        const newMap = new Map(prev);
        newMap.delete(participantId);
        return newMap;
      });

      setActiveParticipants(prev => {
        const newMap = new Map(prev);
        const participant = newMap.get(participantId);
        if (participant) {
          newMap.set(participantId, {
            ...participant,
            isDrawing: false,
            lastActivity: Date.now(),
          });
        }
        return newMap;
      });
    };

    const handleParticipantActivity = (data) => {
      const { participantId, activity, participantInfo } = data;
      
      setActiveParticipants(prev => {
        const newMap = new Map(prev);
        const existing = newMap.get(participantId) || {};
        newMap.set(participantId, {
          ...existing,
          ...participantInfo,
          lastActivity: Date.now(),
          currentActivity: activity,
        });
        return newMap;
      });
    };

    // Socket event listeners
    socket.on('participant-joined-whiteboard', handleParticipantJoined);
    socket.on('participant-left-whiteboard', handleParticipantLeft);
    socket.on('cursor-move', handleCursorMove);
    socket.on('drawing-start', handleDrawingStart);
    socket.on('drawing-end', handleDrawingEnd);
    socket.on('participant-activity', handleParticipantActivity);

    return () => {
      socket.off('participant-joined-whiteboard', handleParticipantJoined);
      socket.off('participant-left-whiteboard', handleParticipantLeft);
      socket.off('cursor-move', handleCursorMove);
      socket.off('drawing-start', handleDrawingStart);
      socket.off('drawing-end', handleDrawingEnd);
      socket.off('participant-activity', handleParticipantActivity);
      
      // Clear all timeouts
      cursorUpdateTimeoutRef.current.forEach(timeoutId => {
        clearTimeout(timeoutId);
      });
      cursorUpdateTimeoutRef.current.clear();
    };
  }, [socket, currentUser.id]);

  // Broadcast cursor movement
  const handleMouseMove = useCallback((event) => {
    if (!socket) return;

    const canvas = event.target.closest('canvas');
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;

    const position = {
      x: (event.clientX - rect.left) * scaleX,
      y: (event.clientY - rect.top) * scaleY,
    };

    socket.emit('cursor-move', {
      meetingId,
      participantId: currentUser.id,
      participantInfo: {
        name: currentUser.name,
        avatar: currentUser.avatar,
        color: currentUser.color || '#2196f3',
      },
      position,
    });
  }, [socket, meetingId, currentUser]);

  // Broadcast drawing events
  const broadcastDrawingStart = useCallback(() => {
    if (!socket) return;

    socket.emit('drawing-start', {
      meetingId,
      participantId: currentUser.id,
      participantInfo: {
        name: currentUser.name,
        avatar: currentUser.avatar,
        color: currentUser.color || '#2196f3',
      },
    });
  }, [socket, meetingId, currentUser]);

  const broadcastDrawingEnd = useCallback(() => {
    if (!socket) return;

    socket.emit('drawing-end', {
      meetingId,
      participantId: currentUser.id,
    });
  }, [socket, meetingId, currentUser.id]);

  // Generate participant color based on ID
  const getParticipantColor = (participantId) => {
    const colors = [
      '#2196f3', '#4caf50', '#ff9800', '#9c27b0',
      '#f44336', '#00bcd4', '#ffeb3b', '#795548',
      '#607d8b', '#3f51b5', '#e91e63', '#009688',
    ];
    const index = participantId.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
    return colors[index % colors.length];
  };

  // Render participant cursors
  const renderCursors = () => {
    return Array.from(cursors.entries()).map(([participantId, cursorData]) => (
      <div
        key={participantId}
        style={{
          position: 'absolute',
          left: cursorData.position.x,
          top: cursorData.position.y,
          pointerEvents: 'none',
          zIndex: 1000,
          transform: 'translate(-2px, -2px)',
        }}
      >
        <svg width="20" height="20" viewBox="0 0 20 20">
          <path
            d="M0,0 L0,16 L6,12 L9,14 L12,8 L6,8 L0,0 Z"
            fill={getParticipantColor(participantId)}
            stroke="white"
            strokeWidth="1"
          />
        </svg>
        <div
          style={{
            marginTop: 4,
            padding: '2px 6px',
            backgroundColor: getParticipantColor(participantId),
            color: 'white',
            borderRadius: 4,
            fontSize: '10px',
            fontWeight: 'bold',
            whiteSpace: 'nowrap',
          }}
        >
          {cursorData.participant.name}
        </div>
      </div>
    ));
  };

  // Render active participants list
  const renderActiveParticipants = () => {
    const activeList = Array.from(activeParticipants.values());
    const drawingCount = activeList.filter(p => p.isDrawing).length;
    const viewingCount = activeList.filter(p => !p.isDrawing).length;

    return (
      <Box
        sx={{
          position: 'absolute',
          top: 8,
          left: 8,
          zIndex: 1001,
          display: 'flex',
          flexDirection: 'column',
          gap: 1,
        }}
      >
        {/* Stats */}
        <Box sx={{ display: 'flex', gap: 1 }}>
          {drawingCount > 0 && (
            <Chip
              icon={<Edit />}
              label={`${drawingCount} drawing`}
              size="small"
              color="primary"
              variant="filled"
            />
          )}
          {viewingCount > 0 && (
            <Chip
              icon={<Visibility />}
              label={`${viewingCount} viewing`}
              size="small"
              color="default"
              variant="outlined"
            />
          )}
        </Box>

        {/* Participant avatars */}
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
          {activeList.map((participant) => (
            <Tooltip
              key={participant.id}
              title={`${participant.name} ${participant.isDrawing ? '(drawing)' : '(viewing)'}`}
            >
              <Badge
                overlap="circular"
                anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                badgeContent={
                  participant.isDrawing ? (
                    <Box
                      sx={{
                        width: 8,
                        height: 8,
                        borderRadius: '50%',
                        backgroundColor: 'primary.main',
                        border: '1px solid white',
                      }}
                    />
                  ) : null
                }
              >
                <Avatar
                  src={participant.avatar}
                  sx={{
                    width: 24,
                    height: 24,
                    fontSize: '10px',
                    bgcolor: getParticipantColor(participant.id),
                    border: participant.isDrawing ? '2px solid' : '1px solid',
                    borderColor: participant.isDrawing ? 'primary.main' : 'divider',
                  }}
                >
                  {participant.name?.charAt(0).toUpperCase()}
                </Avatar>
              </Badge>
            </Tooltip>
          ))}
        </Box>
      </Box>
    );
  };

  // Render drawing indicators
  const renderDrawingIndicators = () => {
    return Array.from(drawingIndicators.entries()).map(([participantId, indicator]) => (
      <Fade key={participantId} in={indicator.isDrawing}>
        <Box
          sx={{
            position: 'absolute',
            top: 50,
            right: 8,
            zIndex: 1001,
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            p: 1,
            bgcolor: 'background.paper',
            borderRadius: 1,
            boxShadow: 2,
            border: '2px solid',
            borderColor: getParticipantColor(participantId),
          }}
        >
          <Avatar
            src={indicator.participant.avatar}
            sx={{
              width: 20,
              height: 20,
              fontSize: '10px',
              bgcolor: getParticipantColor(participantId),
            }}
          >
            {indicator.participant.name?.charAt(0).toUpperCase()}
          </Avatar>
          <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
            {indicator.participant.name} is drawing...
          </Typography>
          <Box
            sx={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              bgcolor: getParticipantColor(participantId),
              animation: 'pulse 1s infinite',
            }}
          />
        </Box>
      </Fade>
    ));
  };

  return (
    <Box
      onMouseMove={handleMouseMove}
      onMouseDown={broadcastDrawingStart}
      onMouseUp={broadcastDrawingEnd}
      sx={{ position: 'relative', width: '100%', height: '100%' }}
    >
      {renderActiveParticipants()}
      {renderCursors()}
      {renderDrawingIndicators()}
      
      <style jsx>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
      `}</style>
    </Box>
  );
};

export default WhiteboardCollaboration;