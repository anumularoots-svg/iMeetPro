// src/components/reactions/ReactionAnimation.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  useTheme
} from '@mui/material';

const ReactionAnimation = ({ 
  reaction,
  startPosition = { x: 50, y: 50 },
  onComplete,
  animationType = 'float' // 'float', 'burst', 'trail'
}) => {
  const theme = useTheme();
  const [isVisible, setIsVisible] = useState(true);

  useEffect(() => {
    const timer = setTimeout(() => {
      setIsVisible(false);
      onComplete?.();
    }, 3000);

    return () => clearTimeout(timer);
  }, [onComplete]);

  const getAnimationStyle = () => {
    switch (animationType) {
      case 'burst':
        return {
          animation: 'reactionBurst 2s ease-out forwards'
        };
      case 'trail':
        return {
          animation: 'reactionTrail 3s linear forwards'
        };
      default:
        return {
          animation: 'reactionFloat 3s ease-out forwards'
        };
    }
  };

  if (!isVisible) return null;

  return (
    <Box
      sx={{
        position: 'fixed',
        left: `${startPosition.x}%`,
        top: `${startPosition.y}%`,
        fontSize: '3rem',
        zIndex: 9999,
        pointerEvents: 'none',
        textShadow: '2px 2px 8px rgba(0,0,0,0.3)',
        ...getAnimationStyle()
      }}
    >
      {reaction.emoji}

      <style jsx>{`
        @keyframes reactionFloat {
          0% {
            opacity: 0;
            transform: translate(-50%, -50%) scale(0.3) rotate(0deg);
          }
          20% {
            opacity: 1;
            transform: translate(-50%, -50%) scale(1.5) rotate(10deg);
          }
          80% {
            opacity: 1;
            transform: translate(-50%, -100%) scale(1) rotate(-5deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, -150%) scale(0.5) rotate(0deg);
          }
        }

        @keyframes reactionBurst {
          0% {
            opacity: 0;
            transform: translate(-50%, -50%) scale(0.1);
          }
          30% {
            opacity: 1;
            transform: translate(-50%, -50%) scale(2);
          }
          60% {
            opacity: 1;
            transform: translate(-50%, -50%) scale(1.5);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, -50%) scale(0.8);
          }
        }

        @keyframes reactionTrail {
          0% {
            opacity: 0;
            transform: translate(-50%, -50%) scale(0.5);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, -50%) scale(1.2);
          }
          90% {
            opacity: 0.8;
            transform: translate(-50%, -200%) scale(0.8) rotate(360deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, -250%) scale(0.3) rotate(360deg);
          }
        }
      `}</style>
    </Box>
  );
};

export default ReactionAnimation;