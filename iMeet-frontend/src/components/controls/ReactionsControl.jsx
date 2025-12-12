// src/components/controls/ReactionsControl.jsx
import React, { useState } from 'react';
import {
  IconButton,
  Popover,
  Box,
  Grid,
  Typography,
  useTheme,
  alpha
} from '@mui/material';
import { EmojiEmotions } from '@mui/icons-material';

const ReactionsControl = ({ onReactionSend, recentReactions = [] }) => {
  const [anchorEl, setAnchorEl] = useState(null);
  const theme = useTheme();

  const reactions = [
    { emoji: '👍', label: 'Thumbs Up' },
    { emoji: '👎', label: 'Thumbs Down' },
    { emoji: '😀', label: 'Happy' },
    { emoji: '😮', label: 'Surprised' },
    { emoji: '❤️', label: 'Love' },
    { emoji: '👏', label: 'Applause' },
    { emoji: '🎉', label: 'Celebration' },
    { emoji: '🤔', label: 'Thinking' },
    { emoji: '👌', label: 'OK' },
    { emoji: '🔥', label: 'Fire' }
  ];

  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleReactionClick = (reaction) => {
    onReactionSend(reaction);
    handleClose();
  };

  const open = Boolean(anchorEl);

  return (
    <>
      <IconButton
        onClick={handleClick}
        sx={{
          bgcolor: open ? 'primary.main' : 'background.paper',
          color: open ? 'white' : 'text.primary',
          '&:hover': {
            bgcolor: open ? 'primary.dark' : 'action.hover'
          }
        }}
      >
        <EmojiEmotions />
      </IconButton>

      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        sx={{
          '& .MuiPopover-paper': {
            borderRadius: 3,
            boxShadow: theme.shadows[8],
            border: `1px solid ${theme.palette.divider}`
          }
        }}
      >
        <Box sx={{ p: 2, minWidth: 280 }}>
          <Typography variant="h6" gutterBottom>
            Quick Reactions
          </Typography>
          
          <Grid container spacing={1}>
            {reactions.map((reaction) => (
              <Grid item xs={2.4} key={reaction.emoji}>
                <IconButton
                  onClick={() => handleReactionClick(reaction)}
                  sx={{
                    width: 48,
                    height: 48,
                    fontSize: '1.5rem',
                    bgcolor: alpha(theme.palette.primary.main, 0.1),
                    '&:hover': {
                      bgcolor: alpha(theme.palette.primary.main, 0.2),
                      transform: 'scale(1.1)'
                    },
                    transition: 'all 0.2s ease'
                  }}
                >
                  {reaction.emoji}
                </IconButton>
              </Grid>
            ))}
          </Grid>

          {recentReactions.length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Recent Reactions
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                {recentReactions.slice(0, 5).map((reaction, index) => (
                  <Box
                    key={index}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 0.5,
                      px: 1,
                      py: 0.5,
                      bgcolor: alpha(theme.palette.grey[500], 0.1),
                      borderRadius: 2,
                      fontSize: '0.75rem'
                    }}
                  >
                    <span>{reaction.emoji}</span>
                    <span>{reaction.count}</span>
                  </Box>
                ))}
              </Box>
            </Box>
          )}
        </Box>
      </Popover>
    </>
  );
};

export default ReactionsControl;  