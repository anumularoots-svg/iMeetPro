import React, { useState, useEffect } from 'react';
import {
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Avatar,
  Typography,
  Box,
  Paper,
  IconButton,
  Badge,
  Tooltip,
  Chip,
  Fade,
  Zoom
} from '@mui/material';
import {
  PanTool as HandIcon,
  CheckCircle as AcceptIcon,
  Cancel as DenyIcon,
  Schedule as ClockIcon
} from '@mui/icons-material';
import { formatDistanceToNow } from 'date-fns';

const HandRaisedList = ({ 
  raisedHands = [], 
  onAcceptHand, 
  onDenyHand, 
  isHost = false 
}) => {
  const [animatingHands, setAnimatingHands] = useState(new Set());

  useEffect(() => {
    // Animate new raised hands
    raisedHands.forEach(hand => {
      if (!animatingHands.has(hand.id)) {
        setAnimatingHands(prev => new Set([...prev, hand.id]));
        setTimeout(() => {
          setAnimatingHands(prev => {
            const newSet = new Set(prev);
            newSet.delete(hand.id);
            return newSet;
          });
        }, 1000);
      }
    });
  }, [raisedHands]);


  const getHandDuration = (timestamp) => {
    return formatDistanceToNow(new Date(timestamp), { addSuffix: true });
  };


  return (
    <Paper 
      elevation={3} 
      sx={{ 
        borderRadius: 2,
        overflow: 'hidden',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white'
      }}
    >
 

      <List sx={{ p: 0, maxHeight: 300, overflow: 'auto' }}>
        {raisedHands.map((hand, index) => (
          <Fade in={true} timeout={500} key={hand.id}>
            <ListItem
              sx={{
                bgcolor: index % 2 === 0 ? 'rgba(255,255,255,0.1)' : 'rgba(255,255,255,0.05)',
                borderLeft: animatingHands.has(hand.id) ? '4px solid #ffd700' : '4px solid transparent',
                transition: 'all 0.3s ease',
                '&:hover': {
                  bgcolor: 'rgba(255,255,255,0.15)',
                  // transform: 'translateX(4px)'
                }
              }}
            >
              <ListItemAvatar>
                <Zoom in={animatingHands.has(hand.id)} timeout={300}>
                  <Avatar
                    src={hand.user?.profile_picture}
                    sx={{
                      bgcolor: 'primary.main',
                      border: '2px solid #ffd700',
                      
                    }}
                  >
                    {hand.user?.full_name?.charAt(0) || '?'}
                  </Avatar>
                </Zoom>
              </ListItemAvatar>

              <ListItemText
                primary={
                  <Typography variant="subtitle1" sx={{ fontWeight: 500 }}>
                    {hand.user?.full_name || 'Unknown User'}
                  </Typography>
                }
                secondary={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 0.5 }}>
                    <ClockIcon sx={{ fontSize: 14, color: 'rgba(255,255,255,0.7)' }} />
                    <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.7)' }}>
                      {getHandDuration(hand.timestamp)}
                    </Typography>
                  </Box>
                }
              />

            </ListItem>
          </Fade>
        ))}
      </List>
    </Paper>
  );
};

export default HandRaisedList;