// UPDATED: src/components/reactions/ReactionButton.jsx - Cache-Only Backend Integration
import React, { useState } from 'react';
import {
  IconButton,
  Box,
  Typography,
  useTheme,
  alpha,
  Zoom,
  Fade,
  Badge
} from '@mui/material';

const ReactionButton = ({ 
  reaction, 
  onClick, 
  size = 'medium',
  showLabel = false,
  disabled = false,
  count = 0,
  isActive = false,
  recentUsers = []
}) => {
  const theme = useTheme();
  const [isHovered, setIsHovered] = useState(false);

  const getSizeProps = () => {
    switch (size) {
      case 'small':
        return { width: 40, height: 40, fontSize: '1rem' };
      case 'large':
        return { width: 72, height: 72, fontSize: '2rem' };
      default:
        return { width: 56, height: 56, fontSize: '1.5rem' };
    }
  };

  const sizeProps = getSizeProps();

  const handleClick = () => {
    if (!disabled && onClick) {
      onClick(reaction);
    }
  };

  return (
    <Box
      sx={{
        position: 'relative',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: 0.5
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Zoom in timeout={300}>
        <IconButton
          onClick={handleClick}
          disabled={disabled}
          sx={{
            width: sizeProps.width,
            height: sizeProps.height,
            fontSize: sizeProps.fontSize,
            bgcolor: isActive 
              ? alpha(theme.palette.primary.main, 0.2)
              : count > 0
              ? alpha(theme.palette.success.main, 0.1)
              : alpha(theme.palette.grey[500], 0.1),
            border: isActive 
              ? `2px solid ${theme.palette.primary.main}`
              : count > 0
              ? `2px solid ${alpha(theme.palette.success.main, 0.5)}`
              : `1px solid ${alpha(theme.palette.grey[500], 0.2)}`,
            transition: 'all 0.2s ease',
            position: 'relative',
            '&:hover': {
              bgcolor: alpha(theme.palette.primary.main, 0.15),
              transform: 'scale(1.1)',
              boxShadow: theme.shadows[4]
            },
            '&:disabled': {
              opacity: 0.5,
              cursor: 'not-allowed'
            }
          }}
        >
          {reaction.emoji}
          
          {/* Count Badge */}
          {count > 0 && (
            <Badge
              badgeContent={count > 99 ? '99+' : count}
              color="primary"
              sx={{
                position: 'absolute',
                top: -8,
                right: -8,
                '& .MuiBadge-badge': {
                  backgroundColor: theme.palette.success.main,
                  color: 'white',
                  fontWeight: 'bold',
                  fontSize: '0.7rem',
                  minWidth: 20,
                  height: 20,
                  border: '2px solid white',
                  boxShadow: theme.shadows[2]
                }
              }}
            />
          )}
        </IconButton>
      </Zoom>

      {/* Label */}
      {(showLabel || isHovered) && (
        <Fade in timeout={200}>
          <Typography
            variant="caption"
            sx={{
              color: 'text.secondary',
              fontSize: '0.7rem',
              textAlign: 'center',
              maxWidth: 80,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap'
            }}
          >
            {reaction.label || reaction.name}
          </Typography>
        </Fade>
      )}

      {/* Recent Users Tooltip on Hover */}
      {isHovered && recentUsers.length > 0 && (
        <Fade in timeout={200}>
          <Box
            sx={{
              position: 'absolute',
              bottom: '100%',
              left: '50%',
              transform: 'translateX(-50%)',
              mb: 1,
              backgroundColor: 'rgba(0,0,0,0.8)',
              color: 'white',
              padding: theme.spacing(0.5, 1),
              borderRadius: 1,
              fontSize: '0.6rem',
              whiteSpace: 'nowrap',
              zIndex: 1000,
              maxWidth: 200,
              overflow: 'hidden',
              textOverflow: 'ellipsis'
            }}
          >
            {recentUsers.slice(0, 3).join(', ')}
            {recentUsers.length > 3 && ` +${recentUsers.length - 3} more`}
          </Box>
        </Fade>
      )}
    </Box>
  );
};

export default ReactionButton;