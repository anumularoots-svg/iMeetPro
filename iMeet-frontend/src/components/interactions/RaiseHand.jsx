import React, { useState, useEffect } from 'react';
import {
  IconButton,
  Tooltip,
  Badge,
  Box,
  Typography,
  useTheme,
  alpha,
  Zoom,
  Grow
} from '@mui/material';
import {
  PanTool,
  PanToolAlt,
  Check,
  Close
} from '@mui/icons-material';
import { styled, keyframes } from '@mui/material/styles';

const pulseAnimation = keyframes`
  0% {
    transform: scale(1);
    box-shadow: 0 0 0 0 rgba(255, 193, 7, 0.7);
  }
  70% {
    transform: scale(1.05);
    box-shadow: 0 0 0 10px rgba(255, 193, 7, 0);
  }
  100% {
    transform: scale(1);
    box-shadow: 0 0 0 0 rgba(255, 193, 7, 0);
  }
`;

const shakeAnimation = keyframes`
  0%, 100% { transform: rotate(0deg); }
  10% { transform: rotate(-5deg); }
  20% { transform: rotate(5deg); }
  30% { transform: rotate(-5deg); }
  40% { transform: rotate(5deg); }
  50% { transform: rotate(0deg); }
`;

const StyledIconButton = styled(IconButton)(({ theme, isRaised, isHost }) => ({
  position: 'relative',
  transition: 'all 0.3s ease',
  backgroundColor: isRaised 
    ? alpha(theme.palette.warning.main, 0.2)
    : alpha(theme.palette.background.paper, 0.1),
  color: isRaised 
    ? theme.palette.warning.main 
    : theme.palette.text.secondary,
  border: `2px solid ${isRaised 
    ? theme.palette.warning.main 
    : alpha(theme.palette.grey[300], 0.5)}`,
  backdropFilter: 'blur(10px)',
  '&:hover': {
    backgroundColor: isRaised 
      ? alpha(theme.palette.warning.main, 0.3)
      : alpha(theme.palette.warning.main, 0.1),
    color: theme.palette.warning.main,
    border: `2px solid ${theme.palette.warning.main}`,
    transform: 'translateY(-2px)',
  },
  ...(isRaised && {
    animation: `${pulseAnimation} 2s infinite`,
  }),
  ...(isHost && isRaised && {
    '&::before': {
      content: '""',
      position: 'absolute',
      top: -2,
      left: -2,
      right: -2,
      bottom: -2,
      background: `linear-gradient(45deg, ${theme.palette.warning.main}, ${theme.palette.error.main})`,
      borderRadius: 'inherit',
      zIndex: -1,
    },
  }),
}));

const NotificationBadge = styled(Badge)(({ theme }) => ({
  '& .MuiBadge-badge': {
    backgroundColor: theme.palette.error.main,
    color: 'white',
    fontWeight: 600,
    fontSize: '0.7rem',
    minWidth: 20,
    height: 20,
    animation: `${pulseAnimation} 1.5s infinite`,
  },
}));

const HandIcon = styled(PanTool)(({ theme, isRaised }) => ({
  fontSize: 28,
  transition: 'all 0.3s ease',
  ...(isRaised && {
    animation: `${shakeAnimation} 0.5s ease-in-out`,
  }),
}));

const StatusText = styled(Typography)(({ theme, isRaised }) => ({
  fontWeight: 600,
  fontSize: '0.8rem',
  textTransform: 'uppercase',
  letterSpacing: 1,
  color: isRaised ? theme.palette.warning.main : theme.palette.text.secondary,
  transition: 'color 0.3s ease',
}));

const RaiseHand = ({
  isHandRaised = false,
  pendingHandsCount = 0,
  isHost = false,
  disabled = false,
  onToggleHand,
  onAcknowledgeHand,
  onLowerHand,
  showText = false,
  size = 'medium', // small, medium, large
  variant = 'icon' // icon, button, extended
}) => {
  const theme = useTheme();
  const [isAnimating, setIsAnimating] = useState(false);
  const [showSuccess, setShowSuccess] = useState(false);

  const sizes = {
    small: { iconSize: 20, buttonSize: 40 },
    medium: { iconSize: 28, buttonSize: 50 },
    large: { iconSize: 36, buttonSize: 60 }
  };

  const currentSize = sizes[size];

  useEffect(() => {
    if (isHandRaised) {
      setIsAnimating(true);
      const timer = setTimeout(() => setIsAnimating(false), 500);
      return () => clearTimeout(timer);
    }
  }, [isHandRaised]);

  const handleToggleHand = () => {
    if (disabled) return;
    
    onToggleHand && onToggleHand(!isHandRaised);
    
    if (!isHandRaised) {
      setIsAnimating(true);
      setTimeout(() => setIsAnimating(false), 500);
    }
  };

  const handleAcknowledge = (e) => {
    e.stopPropagation();
    onAcknowledgeHand && onAcknowledgeHand();
    setShowSuccess(true);
    setTimeout(() => setShowSuccess(false), 2000);
  };

  const handleLowerHand = (e) => {
    e.stopPropagation();
    onLowerHand && onLowerHand();
  };

  if (variant === 'extended') {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 1,
          p: 2,
          borderRadius: 2,
          backgroundColor: alpha(theme.palette.background.paper, 0.8),
          backdropFilter: 'blur(10px)',
          border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`,
        }}
      >
        <Box display="flex" alignItems="center" gap={2}>
          <StyledIconButton
            onClick={handleToggleHand}
            disabled={disabled}
            isRaised={isHandRaised}
            isHost={isHost}
            size={size}
            sx={{ width: currentSize.buttonSize, height: currentSize.buttonSize }}
          >
            <NotificationBadge
              badgeContent={isHost ? pendingHandsCount : 0}
              invisible={!isHost || pendingHandsCount === 0}
            >
              <HandIcon
                isRaised={isHandRaised || isAnimating}
                sx={{ fontSize: currentSize.iconSize }}
              />
            </NotificationBadge>
          </StyledIconButton>

          {isHost && isHandRaised && (
            <Grow in={true}>
              <Box display="flex" gap={1}>
                <Tooltip title="Acknowledge hand">
                  <IconButton
                    size="small"
                    onClick={handleAcknowledge}
                    sx={{
                      backgroundColor: alpha(theme.palette.success.main, 0.1),
                      color: theme.palette.success.main,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.success.main, 0.2),
                      },
                    }}
                  >
                    <Check />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Lower hand">
                  <IconButton
                    size="small"
                    onClick={handleLowerHand}
                    sx={{
                      backgroundColor: alpha(theme.palette.error.main, 0.1),
                      color: theme.palette.error.main,
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.error.main, 0.2),
                      },
                    }}
                  >
                    <Close />
                  </IconButton>
                </Tooltip>
              </Box>
            </Grow>
          )}
        </Box>

        {showText && (
          <StatusText isRaised={isHandRaised}>
            {isHandRaised ? 'Hand Raised' : 'Raise Hand'}
          </StatusText>
        )}

        {isHost && pendingHandsCount > 0 && (
          <Typography variant="caption" color="text.secondary">
            {pendingHandsCount} participant{pendingHandsCount > 1 ? 's' : ''} waiting
          </Typography>
        )}

        {showSuccess && (
          <Zoom in={showSuccess}>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                backgroundColor: alpha(theme.palette.success.main, 0.1),
                color: theme.palette.success.main,
                px: 2,
                py: 1,
                borderRadius: 2,
                border: `1px solid ${alpha(theme.palette.success.main, 0.3)}`,
              }}
            >
              <Check sx={{ fontSize: 16 }} />
              <Typography variant="caption" fontWeight={600}>
                Hand acknowledged
              </Typography>
            </Box>
          </Zoom>
        )}
      </Box>
    );
  }

  if (variant === 'button') {
    return (
      <Box display="flex" alignItems="center" gap={1}>
        <StyledIconButton
          onClick={handleToggleHand}
          disabled={disabled}
          isRaised={isHandRaised}
          isHost={isHost}
          size={size}
          sx={{ width: currentSize.buttonSize, height: currentSize.buttonSize }}
        >
          <NotificationBadge
            badgeContent={isHost ? pendingHandsCount : 0}
            invisible={!isHost || pendingHandsCount === 0}
          >
            <HandIcon
              isRaised={isHandRaised || isAnimating}
              sx={{ fontSize: currentSize.iconSize }}
            />
          </NotificationBadge>
        </StyledIconButton>

        {showText && (
          <StatusText isRaised={isHandRaised}>
            {isHandRaised ? 'Hand Raised' : 'Raise Hand'}
          </StatusText>
        )}
      </Box>
    );
  }

  // Default icon variant
  return (
    <Tooltip 
      title={
        isHandRaised 
          ? 'Lower hand' 
          : isHost && pendingHandsCount > 0
          ? `${pendingHandsCount} hands raised`
          : 'Raise hand'
      }
    >
      <span>
        <StyledIconButton
          onClick={handleToggleHand}
          disabled={disabled}
          isRaised={isHandRaised}
          isHost={isHost}
          size={size}
          sx={{ width: currentSize.buttonSize, height: currentSize.buttonSize }}
        >
          <NotificationBadge
            badgeContent={isHost ? pendingHandsCount : 0}
            invisible={!isHost || pendingHandsCount === 0}
          >
            <HandIcon
              isRaised={isHandRaised || isAnimating}
              sx={{ fontSize: currentSize.iconSize }}
            />
          </NotificationBadge>
        </StyledIconButton>
      </span>
    </Tooltip>
  );
};

export default RaiseHand;