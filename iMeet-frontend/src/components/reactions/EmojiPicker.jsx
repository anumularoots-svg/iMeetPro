// src/components/reactions/EmojiPicker.jsx
import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Grid,
  IconButton,
  Tabs,
  Tab,
  InputAdornment,
  useTheme,
  alpha
} from '@mui/material';
import { Search, Close } from '@mui/icons-material';

const EmojiPicker = ({ onEmojiSelect, onClose, isOpen = false }) => {
  const theme = useTheme();
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedCategory, setSelectedCategory] = useState(0);

  const emojiCategories = [
    {
      name: 'Smileys & People',
      icon: '😀',
      emojis: [
        '😀', '😃', '😄', '😁', '😆', '😅', '😂', '🤣', '😊', '😇',
        '🙂', '🙃', '😉', '😌', '😍', '🥰', '😘', '😗', '😙', '😚',
        '😋', '😛', '😝', '😜', '🤪', '🤨', '🧐', '🤓', '😎', '🤩',
        '🥳', '😏', '😒', '😞', '😔', '😟', '😕', '🙁', '☹️', '😣',
        '😖', '😫', '😩', '🥺', '😢', '😭', '😤', '😠', '😡', '🤬'
      ]
    },
    {
      name: 'Gestures',
      icon: '👍',
      emojis: [
        '👍', '👎', '👌', '🤌', '🤏', '✌️', '🤞', '🤟', '🤘', '🤙',
        '👈', '👉', '👆', '🖕', '👇', '☝️', '👋', '🤚', '🖐️', '✋',
        '🖖', '👏', '🙌', '🤝', '🙏', '✍️', '💪', '🦵', '🦶', '👂'
      ]
    },
    {
      name: 'Objects',
      icon: '🎉',
      emojis: [
        '🎉', '🎊', '🎈', '🎁', '🏆', '🥇', '🥈', '🥉', '⭐', '🌟',
        '💫', '✨', '🔥', '💯', '💫', '💥', '💢', '💨', '💦', '💧',
        '🌈', '☀️', '🌙', '⭐', '🌟', '✨', '⚡', '🔥', '💥', '❄️'
      ]
    },
    {
      name: 'Hearts',
      icon: '❤️',
      emojis: [
        '❤️', '🧡', '💛', '💚', '💙', '💜', '🖤', '🤍', '🤎', '💔',
        '❣️', '💕', '💞', '💓', '💗', '💖', '💘', '💝', '💟', '♥️'
      ]
    }
  ];

  const filteredEmojis = emojiCategories[selectedCategory].emojis.filter(emoji =>
    searchTerm === '' || 
    emojiCategories[selectedCategory].name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleEmojiClick = (emoji) => {
    onEmojiSelect({ 
      emoji, 
      label: `${emoji} emoji`,
      category: emojiCategories[selectedCategory].name 
    });
    onClose();
  };

  if (!isOpen) return null;

  return (
    <Paper
      elevation={12}
      sx={{
        position: 'absolute',
        bottom: 80,
        right: 20,
        width: 350,
        height: 400,
        borderRadius: 3,
        overflow: 'hidden',
        bgcolor: 'background.paper',
        border: `1px solid ${theme.palette.divider}`,
        zIndex: 9999
      }}
    >
      {/* Header */}
      <Box
        sx={{
          p: 2,
          bgcolor: alpha(theme.palette.primary.main, 0.05),
          borderBottom: `1px solid ${theme.palette.divider}`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}
      >
        <Typography variant="h6">
          Emoji Picker
        </Typography>
        <IconButton size="small" onClick={onClose}>
          <Close />
        </IconButton>
      </Box>

      {/* Search */}
      <Box sx={{ p: 2, pb: 1 }}>
        <TextField
          fullWidth
          size="small"
          placeholder="Search emojis..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Search />
              </InputAdornment>
            )
          }}
        />
      </Box>

      {/* Categories */}
      <Tabs
        value={selectedCategory}
        onChange={(e, value) => setSelectedCategory(value)}
        variant="scrollable"
        scrollButtons="auto"
        sx={{ px: 2 }}
      >
        {emojiCategories.map((category, index) => (
          <Tab
            key={index}
            label={category.icon}
            sx={{ minWidth: 50, fontSize: '1.2rem' }}
          />
        ))}
      </Tabs>

      {/* Emojis Grid */}
      <Box sx={{ p: 2, height: 280, overflow: 'auto' }}>
        <Grid container spacing={0.5}>
          {filteredEmojis.map((emoji, index) => (
            <Grid item xs={2} key={index}>
              <IconButton
                onClick={() => handleEmojiClick(emoji)}
                sx={{
                  width: '100%',
                  height: 40,
                  fontSize: '1.5rem',
                  borderRadius: 1,
                  '&:hover': {
                    bgcolor: alpha(theme.palette.primary.main, 0.1),
                    transform: 'scale(1.2)'
                  },
                  transition: 'all 0.1s ease'
                }}
              >
                {emoji}
              </IconButton>
            </Grid>
          ))}
        </Grid>
      </Box>
    </Paper>
  );
};

export default EmojiPicker;