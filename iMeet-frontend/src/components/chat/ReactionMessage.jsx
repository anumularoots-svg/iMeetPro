// src/components/chat/ReactionMessage.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Avatar,
  Chip,
  Fade,
  Zoom,
  useTheme,
  IconButton,
  Tooltip
} from '@mui/material';
import {
  ThumbUp as ThumbUpIcon,
  ThumbDown as ThumbDownIcon,
  Favorite as HeartIcon,
  EmojiEmotions as LaughIcon,
  Surprised as SurprisedIcon,
  PanTool as HandIcon
} from '@mui/icons-material';
import { styled, keyframes } from '@mui/material/styles';

const bounce = keyframes`
  0%, 20%, 53%, 80%, 100% {
    transform: translate3d(0,0,0);
  }
  40%, 43% {
    transform: translate3d(0, -15px, 0);
  }
  70% {
    transform: translate3d(0, -7px, 0);
  }
  90% {
    transform: translate3d(0, -2px, 0);
  }
`;

const pulse = keyframes`
  0% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.1);
  }
  100% {
    transform: scale(1);
  }
`;

const ReactionContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(1),
  padding: theme.spacing(1.5),
  background: `linear-gradient(135deg, ${theme.palette.background.paper} 0%, ${theme.palette.grey[50]} 100%)`,
  borderRadius: theme.spacing(2),
  border: `1px solid ${theme.palette.divider}`,
  position: 'relative',
  overflow: 'hidden',
  '&::before': {
    content: '""',
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    height: 3,
    background: `linear-gradient(90deg, ${theme.palette.primary.main} 0%, ${theme.palette.secondary.main} 100%)`
  }
}));

const ReactionHeader = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  marginBottom: theme.spacing(0.5)
}));

const ReactionEmoji = styled(Box)(({ theme, isNew }) => ({
  fontSize: '2rem',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  animation: isNew ? `${bounce} 1s ease-in-out` : 'none',
  '&:hover': {
    animation: `${pulse} 0.5s ease-in-out`
  }
}));

const ReactionStats = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexWrap: 'wrap',
  gap: theme.spacing(0.5),
  marginTop: theme.spacing(1)
}));

const ReactionChip = styled(Chip)(({ theme, reactionType }) => {
  const getReactionColor = (type) => {
    switch (type) {
      case '👍': return theme.palette.success;
      case '👎': return theme.palette.error;
      case '❤️': return theme.palette.error;
      case '😂': return theme.palette.warning;
      case '😮': return theme.palette.info;
      case '👏': return theme.palette.primary;
      default: return theme.palette.grey;
    }
  };

  const colors = getReactionColor(reactionType);
  
  return {
    background: `linear-gradient(45deg, ${colors.light} 30%, ${colors.main} 90%)`,
    color: colors.contrastText,
    fontWeight: 600,
    fontSize: '0.75rem',
    height: 24,
    '& .MuiChip-label': {
      paddingLeft: theme.spacing(0.5),
      paddingRight: theme.spacing(0.5)
    },
    '&:hover': {
      transform: 'scale(1.05)',
      boxShadow: theme.shadows[4]
    },
    transition: 'all 0.3s ease'
  };
});

const UserAvatar = styled(Avatar)(({ theme }) => ({
  width: 24,
  height: 24,
  fontSize: '0.75rem',
  background: `linear-gradient(45deg, ${theme.palette.primary.main} 30%, ${theme.palette.primary.light} 90%)`
}));

const ReactionMessage = ({ 
  reaction, 
  participants = [],
  onReactionClick,
  showDetails = true,
  isNew = false 
}) => {
  const [reactionCounts, setReactionCounts] = useState({});
  const [totalReactions, setTotalReactions] = useState(0);
  const theme = useTheme();

  // Mock reaction data - replace with real data from props
  useEffect(() => {
    const mockReactions = {
      '👍': { count: 5, users: ['John Doe', 'Jane Smith', 'Alice Johnson', 'Bob Wilson', 'Carol Brown'] },
      '❤️': { count: 3, users: ['Jane Smith', 'Alice Johnson', 'David Lee'] },
      '😂': { count: 2, users: ['Bob Wilson', 'Carol Brown'] },
      '👏': { count: 4, users: ['John Doe', 'Jane Smith', 'David Lee', 'Emily Chen'] },
      '😮': { count: 1, users: ['Alice Johnson'] },
      '👎': { count: 1, users: ['Anonymous'] }
    };

    setReactionCounts(mockReactions);
    setTotalReactions(Object.values(mockReactions).reduce((sum, r) => sum + r.count, 0));
  }, [reaction]);

  const getReactionIcon = (emoji) => {
    switch (emoji) {
      case '👍': return <ThumbUpIcon />;
      case '👎': return <ThumbDownIcon />;
      case '❤️': return <HeartIcon />;
      case '😂': return <LaughIcon />;
      case '😮': return <SurprisedIcon />;
      case '👏': return <HandIcon />;
      default: return null;
    }
  };

  const formatReactionText = (emoji, count, users) => {
    if (count === 1) {
      return `${users[0]} reacted with ${emoji}`;
    } else if (count === 2) {
      return `${users[0]} and ${users[1]} reacted with ${emoji}`;
    } else if (count === 3) {
      return `${users[0]}, ${users[1]} and 1 other reacted with ${emoji}`;
    } else {
      return `${users[0]}, ${users[1]} and ${count - 2} others reacted with ${emoji}`;
    }
  };

  const handleReactionClick = (emoji) => {
    if (onReactionClick) {
      onReactionClick(emoji);
    }
  };

  if (totalReactions === 0) {
    return null;
  }

  return (
    <Fade in timeout={300}>
      <ReactionContainer>
        {/* Header */}
        <ReactionHeader>
          <ReactionEmoji isNew={isNew}>
            {reaction || '🎉'}
          </ReactionEmoji>
          <Box>
            <Typography variant="subtitle2" fontWeight="bold" color="primary">
              {totalReactions} {totalReactions === 1 ? 'Reaction' : 'Reactions'}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              from meeting participants
            </Typography>
          </Box>
        </ReactionHeader>

        {/* Reaction Statistics */}
        <ReactionStats>
          {Object.entries(reactionCounts)
            .filter(([_, data]) => data.count > 0)
            .sort(([_, a], [__, b]) => b.count - a.count)
            .map(([emoji, data]) => (
              <Tooltip
                key={emoji}
                title={formatReactionText(emoji, data.count, data.users)}
                arrow
                placement="top"
              >
                <ReactionChip
                  icon={
                    <Box display="flex" alignItems="center" gap={0.5}>
                      <span style={{ fontSize: '1rem' }}>{emoji}</span>
                    </Box>
                  }
                  label={data.count}
                  size="small"
                  clickable
                  onClick={() => handleReactionClick(emoji)}
                  reactionType={emoji}
                />
              </Tooltip>
            ))}
        </ReactionStats>

        {/* Detailed Breakdown */}
        {showDetails && (
          <Box mt={1}>
            {Object.entries(reactionCounts)
              .filter(([_, data]) => data.count > 0)
              .slice(0, 3) // Show top 3 reactions
              .map(([emoji, data]) => (
                <Box key={emoji} mb={1}>
                  <Box display="flex" alignItems="center" gap={1} mb={0.5}>
                    <span style={{ fontSize: '1.2rem' }}>{emoji}</span>
                    <Typography variant="body2" fontWeight="bold">
                      {data.count} {data.count === 1 ? 'person' : 'people'}
                    </Typography>
                  </Box>
                  
                  <Box display="flex" flexWrap="wrap" gap={0.5} ml={2}>
                    {data.users.slice(0, 4).map((userName, index) => (
                      <Zoom in key={userName} timeout={200 + index * 100}>
                        <Box display="flex" alignItems="center" gap={0.5}>
                          <UserAvatar>
                            {userName.charAt(0).toUpperCase()}
                          </UserAvatar>
                          <Typography variant="caption" color="text.secondary">
                            {userName}
                          </Typography>
                        </Box>
                      </Zoom>
                    ))}
                    
                    {data.users.length > 4 && (
                      <Typography variant="caption" color="text.secondary">
                        +{data.users.length - 4} more
                      </Typography>
                    )}
                  </Box>
                </Box>
              ))}
          </Box>
        )}

        {/* Recent Reaction Activity */}
        <Box 
          mt={1} 
          p={1} 
          bgcolor="action.hover" 
          borderRadius={1}
          display="flex"
          alignItems="center"
          gap={1}
        >
          <Typography variant="caption" color="text.secondary">
            💬 Latest: <strong>John Doe</strong> reacted with 👍
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ opacity: 0.7 }}>
            2 seconds ago
          </Typography>
        </Box>
      </ReactionContainer>
    </Fade>
  );
};

export default ReactionMessage;