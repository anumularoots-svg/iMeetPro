import React, { useState, useEffect } from 'react';
import {
  Box,
  Chip,
  Fade,
  Zoom,
  useTheme,
  alpha,
  Typography
} from '@mui/material';
import { styled, keyframes } from '@mui/material/styles';

const floatAnimation = keyframes`
  0% {
    transform: translateY(0px) scale(1);
    opacity: 1;
  }
  50% {
    transform: translateY(-10px) scale(1.1);
    opacity: 0.8;
  }
  100% {
    transform: translateY(-20px) scale(0.8);
    opacity: 0;
  }
`;

const pulseAnimation = keyframes`
  0% {
    transform: scale(1);
    opacity: 1;
  }
  50% {
    transform: scale(1.2);
    opacity: 0.7;
  }
  100% {
    transform: scale(1);
    opacity: 1;
  }
`;

const bounceAnimation = keyframes`
  0%, 20%, 50%, 80%, 100% {
    transform: translateY(0);
  }
  40% {
    transform: translateY(-10px);
  }
  60% {
    transform: translateY(-5px);
  }
`;

const ReactionContainer = styled(Box)(({ theme, position = 'corner' }) => ({
  position: 'absolute',
  zIndex: 10,
  pointerEvents: 'none',
  ...(position === 'corner' && {
    top: theme.spacing(1),
    left: theme.spacing(1),
  }),
  ...(position === 'center' && {
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
  }),
  ...(position === 'overlay' && {
    top: theme.spacing(0.5),
    right: theme.spacing(0.5),
  }),
}));

const FloatingReaction = styled(Box)(({ theme, animationType = 'float' }) => ({
  fontSize: '2rem',
  userSelect: 'none',
  ...(animationType === 'float' && {
    animation: `${floatAnimation} 3s ease-out forwards`,
  }),
  ...(animationType === 'pulse' && {
    animation: `${pulseAnimation} 2s ease-in-out`,
  }),
  ...(animationType === 'bounce' && {
    animation: `${bounceAnimation} 1s ease-in-out`,
  }),
}));

const ReactionChip = styled(Chip)(({ theme, reactionType }) => ({
  height: 24,
  fontSize: '0.75rem',
  fontWeight: 600,
  borderRadius: 12,
  backgroundColor: alpha(theme.palette.background.paper, 0.9),
  backdropFilter: 'blur(10px)',
  border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`,
  color: theme.palette.text.primary,
  '& .MuiChip-label': {
    padding: '0 8px',
    display: 'flex',
    alignItems: 'center',
    gap: 4,
  },
  ...(reactionType === 'hand_raised' && {
    backgroundColor: alpha(theme.palette.warning.main, 0.9),
    color: 'white',
    border: `1px solid ${theme.palette.warning.main}`,
    animation: `${pulseAnimation} 1.5s infinite`,
  }),
}));

const ParticipantReaction = ({
  participant,
  reactions = [],
  position = 'corner', // corner, center, overlay
  showPersistentReactions = true,
  showFloatingReactions = true,
  animationType = 'float', // float, pulse, bounce
  onReactionEnd
}) => {
  const theme = useTheme();
  const [activeReactions, setActiveReactions] = useState([]);
  const [persistentReaction, setPersistentReaction] = useState(null);

  // Reaction mapping with metadata
  const reactionConfig = {
    '👍': { name: 'Thumbs Up', duration: 3000, color: theme.palette.success.main },
    '👎': { name: 'Thumbs Down', duration: 3000, color: theme.palette.error.main },
    '😀': { name: 'Happy', duration: 3000, color: theme.palette.info.main },
    '😮': { name: 'Surprised', duration: 3000, color: theme.palette.warning.main },
    '❤️': { name: 'Love', duration: 4000, color: theme.palette.error.main },
    '👏': { name: 'Applause', duration: 4000, color: theme.palette.success.main },
    '✋': { name: 'Hand Raised', duration: 0, color: theme.palette.warning.main }, // Persistent
    '🎉': { name: 'Celebration', duration: 5000, color: theme.palette.secondary.main },
    '🤔': { name: 'Thinking', duration: 3000, color: theme.palette.info.main },
    '😂': { name: 'Laughing', duration: 3000, color: theme.palette.success.main },
    '👌': { name: 'OK', duration: 3000, color: theme.palette.primary.main },
    '🙋': { name: 'Question', duration: 0, color: theme.palette.info.main }, // Persistent
  };

  // Handle new reactions
  useEffect(() => {
    if (reactions && reactions.length > 0) {
      reactions.forEach(reaction => {
        const config = reactionConfig[reaction.emoji] || { duration: 3000 };
        
        if (config.duration === 0) {
          // Persistent reaction (hand raised, question)
          setPersistentReaction(reaction);
        } else {
          // Floating reaction
          const reactionId = Date.now() + Math.random();
          const newReaction = {
            ...reaction,
            id: reactionId,
            config
          };
          
          setActiveReactions(prev => [...prev, newReaction]);
          
          // Remove reaction after duration
          setTimeout(() => {
            setActiveReactions(prev => prev.filter(r => r.id !== reactionId));
            onReactionEnd && onReactionEnd(reaction);
          }, config.duration);
        }
      });
    }
  }, [reactions]);

  // Handle hand raised state
  useEffect(() => {
    if (participant.hand_raised) {
      setPersistentReaction({
        emoji: '✋',
        type: 'hand_raised',
        timestamp: Date.now()
      });
    } else {
      setPersistentReaction(prev => 
        prev && prev.type === 'hand_raised' ? null : prev
      );
    }
  }, [participant.hand_raised]);

  // Handle current reaction from participant
  useEffect(() => {
    if (participant.current_reaction) {
      const config = reactionConfig[participant.current_reaction] || { duration: 3000 };
      
      if (config.duration === 0) {
        setPersistentReaction({
          emoji: participant.current_reaction,
          type: 'current',
          timestamp: Date.now()
        });
      } else {
        const reactionId = Date.now();
        setActiveReactions(prev => [...prev, {
          id: reactionId,
          emoji: participant.current_reaction,
          type: 'current',
          config
        }]);
        
        setTimeout(() => {
          setActiveReactions(prev => prev.filter(r => r.id !== reactionId));
        }, config.duration);
      }
    }
  }, [participant.current_reaction]);

  return (
    <>
      {/* Persistent Reactions (Hand Raised, Questions) */}
      {showPersistentReactions && persistentReaction && (
        <ReactionContainer position={position}>
          <Zoom in={true}>
            <ReactionChip
              label={
                <Box display="flex" alignItems="center" gap={0.5}>
                  <span>{persistentReaction.emoji}</span>
                  <Typography variant="caption" fontWeight={600}>
                    {persistentReaction.type === 'hand_raised' 
                      ? 'Raised Hand' 
                      : reactionConfig[persistentReaction.emoji]?.name || 'Reaction'
                    }
                  </Typography>
                </Box>
              }
              reactionType={persistentReaction.type}
            />
          </Zoom>
        </ReactionContainer>
      )}

      {/* Floating Reactions */}
      {showFloatingReactions && activeReactions.map((reaction) => (
        <ReactionContainer key={reaction.id} position="center">
          <FloatingReaction animationType={animationType}>
            {reaction.emoji}
          </FloatingReaction>
        </ReactionContainer>
      ))}

      {/* Multiple Reactions Stack (for overlay position) */}
      {position === 'overlay' && activeReactions.length > 1 && (
        <ReactionContainer position="overlay">
          <Fade in={true}>
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                gap: 0.5,
                alignItems: 'flex-end',
              }}
            >
              {activeReactions.slice(-3).map((reaction, index) => (
                <Box
                  key={reaction.id}
                  sx={{
                    fontSize: `${1.5 - index * 0.2}rem`,
                    opacity: 1 - index * 0.3,
                    animation: `${pulseAnimation} 1s ease-in-out`,
                  }}
                >
                  {reaction.emoji}
                </Box>
              ))}
              {activeReactions.length > 3 && (
                <Typography
                  variant="caption"
                  sx={{
                    backgroundColor: alpha(theme.palette.background.paper, 0.9),
                    borderRadius: 1,
                    px: 0.5,
                    fontSize: '0.7rem',
                  }}
                >
                  +{activeReactions.length - 3} more
                </Typography>
              )}
            </Box>
          </Fade>
        </ReactionContainer>
      )}

      {/* Reaction Trail (for center position with multiple reactions) */}
      {position === 'center' && activeReactions.length > 1 && (
        <>
          {activeReactions.slice(0, -1).map((reaction, index) => (
            <ReactionContainer key={`trail-${reaction.id}`} position="center">
              <Box
                sx={{
                  fontSize: '1.5rem',
                  opacity: 0.6 - index * 0.2,
                  transform: `translate(${(index + 1) * 20}px, ${(index + 1) * -15}px) scale(${0.8 - index * 0.1})`,
                  animation: `${floatAnimation} 2s ease-out forwards`,
                  animationDelay: `${index * 0.2}s`,
                }}
              >
                {reaction.emoji}
              </Box>
            </ReactionContainer>
          ))}
        </>
      )}
    </>
  );
};

export default ParticipantReaction;