// src/components/chat/MessageInput.jsx
import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Typography,
  Chip,
  Menu,
  MenuItem,
  Tooltip,
  Fade,
  Paper,
  useTheme
} from '@mui/material';
import {
  Send as SendIcon,
  EmojiEmotions as EmojiIcon,
  AttachFile as AttachFileIcon,
  Search as SearchIcon,   
  Lock as PrivateIcon,
  Public as PublicIcon,
  Close as CloseIcon
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import EmojiPicker from 'emoji-picker-react';

const InputContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(1),
  background: theme.palette.background.paper,
  borderRadius: theme.spacing(1),
  padding: theme.spacing(1),
  border: `1px solid ${theme.palette.divider}`,
  transition: 'all 0.3s ease',
  '&:focus-within': {
    border: `2px solid ${theme.palette.primary.main}`,
    boxShadow: `0 0 0 3px ${theme.palette.primary.main}22`
  }
}));

const StyledTextField = styled(TextField)(({ theme }) => ({
  '& .MuiOutlinedInput-root': {
    background: 'transparent',
    '& fieldset': {
      border: 'none'
    },
    '&:hover fieldset': {
      border: 'none'
    },
    '&.Mui-focused fieldset': {
      border: 'none'
    }
  },
  '& .MuiInputBase-input': {
    fontSize: '0.9rem',
    padding: theme.spacing(1, 0),
    '&::placeholder': {
      color: theme.palette.text.secondary,
      opacity: 0.7
    }
  }
}));

const ActionButton = styled(IconButton)(({ theme, variant = 'default' }) => ({
  borderRadius: theme.spacing(1),
  transition: 'all 0.3s ease',
  ...(variant === 'send' && {
    background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
    color: theme.palette.primary.contrastText,
    '&:hover': {
      background: `linear-gradient(135deg, ${theme.palette.primary.dark} 0%, ${theme.palette.primary.main} 100%)`,
      transform: 'scale(1.05)'
    },
    '&:disabled': {
      background: theme.palette.action.disabledBackground,
      color: theme.palette.action.disabled
    }
  }),
  ...(variant === 'secondary' && {
    color: theme.palette.text.secondary,
    '&:hover': {
      background: theme.palette.action.hover,
      color: theme.palette.primary.main
    }
  })
}));

const EmojiPickerContainer = styled(Paper)(({ theme }) => ({
  position: 'absolute',
  bottom: '100%',
  right: 0,
  marginBottom: theme.spacing(1),
  zIndex: 1400,
  borderRadius: theme.spacing(1),
  overflow: 'hidden',
  boxShadow: theme.shadows[8]
}));

const ReplyContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1),
  padding: theme.spacing(1),
  background: `linear-gradient(135deg, ${theme.palette.info.light} 0%, ${theme.palette.info.main} 100%)`,
  borderRadius: theme.spacing(1),
  marginBottom: theme.spacing(1),
  color: theme.palette.info.contrastText
}));

const MessageInput = ({
  onSendMessage,
  onToggleSearch,
  onToggleFileUpload,
  canUploadFiles = true,
  isHost = false,
  replyingTo = null,
  onCancelReply,
  placeholder = "Type a message...",
  disabled = false
}) => {
  const [message, setMessage] = useState('');
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [isPrivateMessage, setIsPrivateMessage] = useState(false);
  const [anchorEl, setAnchorEl] = useState(null);
  const inputRef = useRef(null);
  const theme = useTheme();

  const handleSend = () => {
    if (message.trim() && !disabled) {
      onSendMessage({
        text: message.trim(),
        isPrivate: isPrivateMessage,
        replyTo: replyingTo?.id
      });
      setMessage('');
      setIsPrivateMessage(false);
      if (onCancelReply) {
        onCancelReply();
      }
    }
  };

  const handleKeyPress = (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      handleSend();
    }
  };

  const handleEmojiClick = (emojiData) => {
    const emoji = emojiData.emoji;
    const cursorPosition = inputRef.current?.selectionStart || message.length;
    const newMessage = message.slice(0, cursorPosition) + emoji + message.slice(cursorPosition);
    setMessage(newMessage);
    setShowEmojiPicker(false);
    
    // Focus back to input and set cursor position
    setTimeout(() => {
      inputRef.current?.focus();
      inputRef.current?.setSelectionRange(cursorPosition + emoji.length, cursorPosition + emoji.length);
    }, 0);
  };

  const handlePrivacyMenuOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handlePrivacyMenuClose = () => {
    setAnchorEl(null);
  };

  const togglePrivateMessage = () => {
    setIsPrivateMessage(!isPrivateMessage);
    handlePrivacyMenuClose();
  };

  useEffect(() => {
    if (replyingTo) {
      inputRef.current?.focus();
    }
  }, [replyingTo]);

  return (
    <Box position="relative">
      {/* Reply Preview */}
      {replyingTo && (
        <Fade in>
          <ReplyContainer>
            <Box flex={1}>
              <Typography variant="caption" sx={{ opacity: 0.8 }}>
                Replying to {replyingTo.userName}
              </Typography>
              <Typography variant="body2" sx={{ 
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                maxWidth: 200
              }}>
                {replyingTo.message}
              </Typography>
            </Box>
            <IconButton 
              size="small" 
              onClick={onCancelReply}
              sx={{ color: 'inherit' }}
            >
              <CloseIcon fontSize="small" />
            </IconButton>
          </ReplyContainer>
        </Fade>
      )}

      <InputContainer>
        {/* Privacy and Status Row */}
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Box display="flex" alignItems="center" gap={1}>
            {/* Privacy Toggle */}
            <Tooltip title={isPrivateMessage ? "Private message" : "Public message"}>
              <Chip
                icon={isPrivateMessage ? <PrivateIcon /> : <PublicIcon />}
                label={isPrivateMessage ? "Private" : "Public"}
                size="small"
                clickable
                onClick={handlePrivacyMenuOpen}
                sx={{
                  background: isPrivateMessage 
                    ? `linear-gradient(45deg, ${theme.palette.warning.main} 30%, ${theme.palette.warning.light} 90%)`
                    : `linear-gradient(45deg, ${theme.palette.success.main} 30%, ${theme.palette.success.light} 90%)`,
                  color: isPrivateMessage ? theme.palette.warning.contrastText : theme.palette.success.contrastText,
                  fontWeight: 600,
                  '&:hover': {
                    transform: 'scale(1.02)'
                  }
                }}
              />
            </Tooltip>

            {/* Typing Indicator */}
            <Typography variant="caption" color="text.secondary">
              {message.length > 0 && `${message.length} characters`}
            </Typography>
          </Box>

          {/* Action Buttons */}
          <Box display="flex" alignItems="center" gap={0.5}>
            <Tooltip title="Search messages">
              <ActionButton 
                variant="secondary"
                size="small"
                onClick={onToggleSearch}
              >
                <SearchIcon fontSize="small" />
              </ActionButton>
            </Tooltip>

            {canUploadFiles && (
              <Tooltip title="Attach file">
                <ActionButton 
                  variant="secondary"
                  size="small"
                  onClick={onToggleFileUpload}
                >
                  <AttachFileIcon fontSize="small" />
                </ActionButton>
              </Tooltip>
            )}

            <Tooltip title="Add emoji">
              <ActionButton 
                variant="secondary"
                size="small"
                onClick={() => setShowEmojiPicker(!showEmojiPicker)}
              >
                <EmojiIcon fontSize="small" />
              </ActionButton>
            </Tooltip>
          </Box>
        </Box>

        {/* Input Row */}
        <Box display="flex" alignItems="flex-end" gap={1}>
          <StyledTextField
            ref={inputRef}
            fullWidth
            multiline
            maxRows={4}
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder={disabled ? "Chat is disabled" : placeholder}
            disabled={disabled}
            autoComplete="off"
            sx={{
              '& .MuiInputBase-root': {
                minHeight: 40
              }
            }}
          />

          <Tooltip title={message.trim() ? "Send message" : "Type a message"}>
            <span>
              <ActionButton
                variant="send"
                onClick={handleSend}
                disabled={!message.trim() || disabled}
                sx={{ minWidth: 40, height: 40 }}
              >
                <SendIcon />
              </ActionButton>
            </span>
          </Tooltip>
        </Box>
      </InputContainer>

      {/* Emoji Picker */}
      {showEmojiPicker && (
        <EmojiPickerContainer>
          <EmojiPicker
            onEmojiClick={handleEmojiClick}
            width={300}
            height={350}
            theme={theme.palette.mode === 'dark' ? 'dark' : 'light'}
            searchDisabled={false}
            skinTonesDisabled={false}
            previewConfig={{
              defaultEmoji: '1f60a',
              defaultCaption: 'Choose an emoji'
            }}
          />
        </EmojiPickerContainer>
      )}

      {/* Privacy Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handlePrivacyMenuClose}
        PaperProps={{
          sx: {
            background: `linear-gradient(135deg, ${theme.palette.background.paper} 0%, ${theme.palette.grey[50]} 100%)`,
            borderRadius: 2,
            minWidth: 180
          }
        }}
      >
        <MenuItem 
          onClick={togglePrivateMessage}
          selected={!isPrivateMessage}
          sx={{ gap: 1 }}
        >
          <PublicIcon color="success" />
          <Box>
            <Typography variant="body2" fontWeight="bold">
              Public Message
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Visible to all participants
            </Typography>
          </Box>
        </MenuItem>
        
        <MenuItem 
          onClick={togglePrivateMessage}
          selected={isPrivateMessage}
          sx={{ gap: 1 }}
          disabled={!isHost}
        >
          <PrivateIcon color="warning" />
          <Box>
            <Typography variant="body2" fontWeight="bold">
              Private Message
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {isHost ? "Only hosts can see" : "Host only feature"}
            </Typography>
          </Box>
        </MenuItem>
      </Menu>

      {/* Click outside to close emoji picker */}
      {showEmojiPicker && (
        <Box
          position="fixed"
          top={0}
          left={0}
          right={0}
          bottom={0}
          zIndex={1300}
          onClick={() => setShowEmojiPicker(false)}
        />
      )}
    </Box>
  );
};

export default MessageInput;