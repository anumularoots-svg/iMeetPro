// src/components/reactions/ReactionOverlay.jsx
import React, { useState, useEffect } from 'react';
import { Box, Fade, Typography } from '@mui/material';

const getParticipantName = (reaction) => {
  return (
    reaction.participantName || 
    reaction.userName || 
    reaction.participant_name || 
    reaction.user_name ||
    reaction.full_name ||
    reaction.name ||
    reaction.displayName ||
    (reaction.participantId === 'self' || reaction.isLocal ? 'You' : null) ||
    'Someone'
  );
};

export const ReactionOverlay = ({ 
  reactions = [], 
  participantId,
  showParticipantNames = true,
  duration = 3000 
}) => {
  const [displayReactions, setDisplayReactions] = useState([]);

  useEffect(() => {
    if (reactions.length > 0) {
      const latestReaction = reactions[reactions.length - 1];
      const reactionId = `${Date.now()}-${Math.random()}`;
      
      const reactionToShow = {
        ...latestReaction,
        id: reactionId,
        displayName: getParticipantName(latestReaction),
        x: Math.random() * 60 + 20, // Random position 20-80%
        y: Math.random() * 60 + 20
      };
      
      setDisplayReactions(prev => [...prev, reactionToShow]);

      // Remove reaction after duration
      setTimeout(() => {
        setDisplayReactions(prev => prev.filter(r => r.id !== reactionId));
      }, duration);
    }
  }, [reactions, duration]);

  // Limit to last 5 reactions to prevent overlay clutter
  const reactionsToShow = displayReactions.slice(-5);

  return (
    <Box
      sx={{
        position: 'absolute',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        pointerEvents: 'none',
        zIndex: 15,
        overflow: 'hidden'
      }}
    >
      {reactionsToShow.map((reaction, index) => (
        <Fade key={reaction.id} in timeout={{ enter: 300, exit: 500 }}>
          <Box
            sx={{
              position: 'absolute',
              top: `${15 + index * 15}%`,
              left: '50%',
              transform: 'translateX(-50%)',
              animation: 'reactionFloat 3s ease-out forwards',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              pointerEvents: 'none',
              zIndex: 16
            }}
          >
            <Box
              sx={{
                fontSize: { xs: '2.5rem', sm: '3.5rem', md: '4rem' },
                textShadow: '2px 2px 8px rgba(0,0,0,0.7)',
                filter: 'drop-shadow(0 0 10px rgba(255,255,255,0.3))'
              }}
            >
              {reaction.reaction}
            </Box>
            
            {showParticipantNames && (
              <Typography
                variant="caption"
                sx={{
                  color: '#fff',
                  backgroundColor: 'rgba(0, 0, 0, 0.8)',
                  borderRadius: '12px',
                  padding: '4px 8px',
                  marginTop: '6px',
                  fontWeight: 'bold',
                  fontSize: { xs: '0.7rem', sm: '0.8rem' },
                  textShadow: '1px 1px 2px rgba(0,0,0,0.9)',
                  border: '1px solid rgba(255,255,255,0.2)'
                }}
              >
                {reaction.displayName}
              </Typography>
            )}
          </Box>
        </Fade>
      ))}

      <style jsx>{`
        @keyframes reactionFloat {
          0% {
            opacity: 0;
            transform: translate(-50%, -50%) scale(0.3) rotate(-10deg);
          }
          15% {
            opacity: 1;
            transform: translate(-50%, -50%) scale(1.3) rotate(5deg);
          }
          30% {
            opacity: 1;
            transform: translate(-50%, -55%) scale(1.1) rotate(-2deg);
          }
          70% {
            opacity: 0.9;
            transform: translate(-50%, -75%) scale(0.9) rotate(3deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, -90%) scale(0.6) rotate(0deg);
          }
        }
      `}</style>
    </Box>
  );
};