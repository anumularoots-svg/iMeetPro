// src/components/reactions/ProfessionalReactionsPanel.jsx - SMOOTH VERSION
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { 
  Box, 
  IconButton, 
  Typography, 
  Fade, 
  Zoom, 
  Tooltip,
  Slide,
  Grow,
  Chip,
  alpha
} from '@mui/material';
import { 
  Close, 
  EmojiEmotions
} from '@mui/icons-material';
// import { enhancedSoundManager } from '../../utils/enhancedReactionSounds';
const ProfessionalReactionsPanel = ({ 
  isOpen, 
  onClose, 
  onReaction, 
  disabled = false,
  style = {}
}) => {
  const [hoveredReaction, setHoveredReaction] = useState(null);
  const [selectedReaction, setSelectedReaction] = useState(null);
  const [recentReactions, setRecentReactions] = useState([]);
  const [pulseStates, setPulseStates] = useState({});
  const panelRef = useRef(null);
  const animationFrameRef = useRef();

  const reactions = [
    { 
      emoji: '👍', 
      label: 'Like', 
      color: '#4CAF50',
      shortcut: '1',
      description: 'Great!'
    },
    { 
      emoji: '👎', 
      label: 'Dislike', 
      color: '#FF5722',
      shortcut: '2',
      description: 'Not good'
    },
    { 
      emoji: '❤️', 
      label: 'Love', 
      color: '#E91E63',
      shortcut: '3',
      description: 'Love it!'
    },
    { 
      emoji: '👏', 
      label: 'Applause', 
      color: '#FF9800',
      shortcut: '4',
      description: 'Well done!'
    },
    { 
      emoji: '🎉', 
      label: 'Celebrate', 
      color: '#9C27B0',
      shortcut: '5',
      description: 'Amazing!'
    },
    { 
      emoji: '🔥', 
      label: 'Fire', 
      color: '#FF4444',
      shortcut: '6',
      description: 'Hot!'
    },
    { 
      emoji: '🤔', 
      label: 'Think', 
      color: '#607D8B',
      shortcut: '7',
      description: 'Hmm...'
    }
  ];

  // Smooth pulse animation for recent reactions using RAF
  useEffect(() => {
    if (recentReactions.length === 0) return;

    const startTime = Date.now();
    
    const animate = () => {
      const elapsed = Date.now() - startTime;
      const progress = (elapsed % 1500) / 1500; // 1.5 second cycle
      
      // Smooth sine wave for gentle pulsing
      const pulseScale = 1 + Math.sin(progress * Math.PI * 2) * 0.1;
      const pulseOpacity = 0.7 + Math.sin(progress * Math.PI * 2) * 0.3;
      
      setPulseStates({
        scale: pulseScale,
        opacity: pulseOpacity
      });
      
      animationFrameRef.current = requestAnimationFrame(animate);
    };
    
    animationFrameRef.current = requestAnimationFrame(animate);
    
    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
      }
    };
  }, [recentReactions.length]);

 const handleReactionClick = useCallback((reaction) => {
 
  
  if (disabled) {
    return;
  }
  
  if (typeof onReaction !== 'function') {
    return;
  }
  
  // Visual feedback
  setSelectedReaction(reaction.emoji);
  
  // Add to recent reactions for feedback
  setRecentReactions(prev => [
    { ...reaction, timestamp: Date.now(), id: Math.random() },
    ...prev.slice(0, 2)
  ]);
  
  // Call the reaction handler
  try {
    onReaction(reaction.emoji);
  } catch (error) {
    console.error('Error in onReaction handler:', error);}
  
  // Close with delay
  setTimeout(() => {
    setSelectedReaction(null);
    onClose();
  }, 250);
}, [onReaction, onClose, disabled])

  // Keyboard shortcuts
  useEffect(() => {
    if (!isOpen) return;

    const handleKeyPress = (e) => {
      const reactionIndex = parseInt(e.key) - 1;
      if (reactionIndex >= 0 && reactionIndex < reactions.length) {
        handleReactionClick(reactions[reactionIndex]);
      } else if (e.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('keydown', handleKeyPress);
    return () => document.removeEventListener('keydown', handleKeyPress);
  }, [isOpen, handleReactionClick, onClose]);

  // Click outside to close
  useEffect(() => {
    if (!isOpen) return;

    const handleClickOutside = (event) => {
      if (panelRef.current && !panelRef.current.contains(event.target)) {
        onClose();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isOpen, onClose]);

  // Auto-clear recent reactions
  useEffect(() => {
    if (recentReactions.length === 0) return;
    
    const timer = setTimeout(() => {
      setRecentReactions(prev => prev.slice(0, -1));
    }, 1800);
    
    return () => clearTimeout(timer);
  }, [recentReactions]);



  if (!isOpen) return null;

  return (
    <>
      {/* Simple backdrop without animation */}
      <Box
        sx={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 80,
          backgroundColor: 'rgba(0, 0, 0, 0.08)',
          backdropFilter: 'blur(2px)',
          WebkitBackdropFilter: 'blur(2px)',
          zIndex: 9998,
          opacity: isOpen ? 1 : 0,
          transition: 'opacity 0.2s ease',
        }}
      />

      {/* Main Panel - No entrance animations */}
      <Box
        ref={panelRef}
        sx={{
          position: 'fixed',
          bottom: '120px',
          left: '50%',
          transform: 'translateX(-50%)',
          zIndex: 9999,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 1.5,
          opacity: isOpen ? 1 : 0,
          transition: 'opacity 0.3s ease',
          ...style
        }}
      >
  

        {/* Reactions Container */}
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 0.8,
            padding: '16px 20px',
            backgroundColor: 'rgba(32, 33, 36, 0.95)',
            backdropFilter: 'blur(8px)',
            WebkitBackdropFilter: 'blur(8px)',
            borderRadius: '24px',
            border: '1px solid rgba(255, 255, 255, 0.08)',
            boxShadow: `
              0 2px 8px rgba(0, 0, 0, 0.15),
              0 1px 3px rgba(0, 0, 0, 0.12)
            `,
            position: 'relative',
            overflow: 'visible',
          }}
        >
          {reactions.map((reaction, index) => (
            <Tooltip
              key={reaction.emoji}
              arrow
              placement="top"
              componentsProps={{
                tooltip: {
                  sx: {
                    backgroundColor: 'rgba(32, 33, 36, 0.95)',
                    backdropFilter: 'blur(8px)',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                    borderRadius: 2,
                    fontSize: '0.75rem',
                    fontFamily: "'Google Sans', 'Roboto', sans-serif",
                    boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
                  }
                },
                arrow: {
                  sx: {
                    color: 'rgba(32, 33, 36, 0.95)',
                  }
                }
              }}
            >
              <Box sx={{ position: 'relative' }}>
                <IconButton
                  onClick={() => handleReactionClick(reaction)}
                  onMouseEnter={() => setHoveredReaction(index)}
                  onMouseLeave={() => setHoveredReaction(null)}
                  disabled={disabled}
                  sx={{
                    width: 52,
                    height: 52,
                    fontSize: '1.8rem',
                    transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
                    borderRadius: '50%',
                    position: 'relative',
                    
                    backgroundColor: hoveredReaction === index 
                      ? 'rgba(255, 255, 255, 0.08)'
                      : 'transparent',
                    transform: hoveredReaction === index 
                      ? 'scale(1.15)' 
                      : selectedReaction === reaction.emoji
                      ? 'scale(0.95)'
                      : 'scale(1)',
                    
                    '&:hover': {
                      backgroundColor: 'rgba(255, 255, 255, 0.08)',
                    },
                    '&:active': {
                      transform: 'scale(0.92)',
                      transition: 'transform 0.1s ease',
                    },
                    '&:disabled': {
                      opacity: 0.4,
                    },
                    
                    boxShadow: hoveredReaction === index 
                      ? `0 0 0 2px ${alpha(reaction.color, 0.3)}`
                      : 'none',
                  }}
                >
                  <span
                    style={{
                      filter: hoveredReaction === index ? 'brightness(1.1)' : 'none',
                      transition: 'filter 0.2s ease',
                      position: 'relative',
                      zIndex: 1,
                    }}
                  >
                    {reaction.emoji}
                  </span>
                </IconButton>

              </Box>
            </Tooltip>
          ))}

          {/* Divider */}
          <Box 
            sx={{ 
              width: '1px', 
              height: '32px', 
              backgroundColor: 'rgba(255, 255, 255, 0.12)', 
              mx: 1.5,
            }} 
          />
          
          {/* Close button */}
          <Tooltip title="Close reactions (ESC)" arrow>
            <IconButton
              onClick={onClose}
              sx={{
                width: 40,
                height: 40,
                color: 'rgba(255, 255, 255, 0.7)',
                backgroundColor: 'transparent',
                transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
                
                '&:hover': {
                  backgroundColor: 'rgba(255, 255, 255, 0.08)',
                  color: 'white',
                  transform: 'scale(1.05)',
                },
                '&:active': {
                  transform: 'scale(0.95)',
                },
              }}
            >
              <Close sx={{ fontSize: '1.2rem' }} />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Recent reactions feedback with smooth pulsing */}
        {recentReactions.length > 0 && (
          <Box
            sx={{
              display: 'flex',
              gap: 0.4,
              alignItems: 'center',
              justifyContent: 'center',
              mt: 0.5,
            }}
          >
            {recentReactions.slice(0, 3).map((reaction) => (
              <Box
                key={reaction.id}
                sx={{
                  width: 24,
                  height: 24,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: '0.9rem',
                  backgroundColor: 'rgba(32, 33, 36, 0.8)',
                  borderRadius: '50%',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                  boxShadow: '0 1px 3px rgba(0,0,0,0.12)',
                  
                  // Smooth pulsing using RAF-controlled values
                  transform: `scale(${pulseStates.scale || 1})`,
                  opacity: pulseStates.opacity || 0.9,
                  transition: 'none', // No CSS transitions
                }}
              >
                {reaction.emoji}
              </Box>
            ))}
          </Box>
        )}
      </Box>
    </>
  );
};

export default ProfessionalReactionsPanel;