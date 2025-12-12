// src/components/chat/MessageList.jsx - WITH FILE SUPPORT
import React, { useRef, useEffect } from 'react';
import {
  Box,
  Typography,
  Avatar,
  Paper,
  Chip,
  IconButton,
  useTheme
} from '@mui/material';
import {
  Lock as PrivateIcon,
  Chat,
  InsertDriveFile as FileIcon,
  Image as ImageIcon,
  PictureAsPdf as PdfIcon,
  Description as DocIcon,
  Download as DownloadIcon
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

const MessagesContainer = styled(Box)(({ theme }) => ({
  flex: 1,
  overflowY: 'auto',
  padding: theme.spacing(2),
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(1),
  background: theme.palette.background.default,
  '&::-webkit-scrollbar': {
    width: '8px',
  },
  '&::-webkit-scrollbar-track': {
    background: theme.palette.action.hover,
    borderRadius: '4px',
  },
  '&::-webkit-scrollbar-thumb': {
    background: theme.palette.action.selected,
    borderRadius: '4px',
    '&:hover': {
      background: theme.palette.action.disabled,
    },
  },
}));

const MessageBubble = styled(Paper)(({ theme, isOwn, isSystem }) => ({
  maxWidth: '70%',
  padding: theme.spacing(1.5, 2),
  borderRadius: theme.spacing(2),
  position: 'relative',
  wordBreak: 'break-word',
  alignSelf: isOwn ? 'flex-end' : 'flex-start',
  background: isSystem 
    ? theme.palette.action.hover
    : isOwn 
      ? theme.palette.primary.main
      : theme.palette.background.paper,
  color: isSystem 
    ? theme.palette.text.secondary
    : isOwn 
      ? theme.palette.primary.contrastText 
      : theme.palette.text.primary,
  boxShadow: theme.shadows[1],
}));

const SystemMessage = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: theme.spacing(1),
  padding: theme.spacing(1, 2),
  margin: theme.spacing(1, 0),
  color: theme.palette.text.secondary,
  fontSize: '0.875rem',
  fontStyle: 'italic',
}));

const FileMessage = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(1.5),
  padding: theme.spacing(1),
  borderRadius: theme.spacing(1),
  backgroundColor: theme.palette.action.hover,
  cursor: 'pointer',
  transition: 'all 0.3s ease',
  '&:hover': {
    backgroundColor: theme.palette.action.selected,
  }
}));

const MessageList = ({ 
  messages = [], 
  currentUser,
  onReaction
}) => {
  const messagesEndRef = useRef(null);
  const theme = useTheme();

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const formatTime = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour: 'numeric', 
      minute: '2-digit',
      hour12: true 
    });
  };

  const formatFileSize = (bytes) => {
    if (!bytes) return '';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const getFileIcon = (fileType, fileName) => {
    if (!fileType && !fileName) return <FileIcon />;
    
    const type = fileType || '';
    const name = fileName?.toLowerCase() || '';
    
    if (type.startsWith('image/') || /\.(jpg|jpeg|png|gif|bmp|svg)$/.test(name)) {
      return <ImageIcon color="primary" />;
    } else if (type === 'application/pdf' || name.endsWith('.pdf')) {
      return <PdfIcon color="error" />;
    } else if (/\.(doc|docx|txt)$/.test(name)) {
      return <DocIcon color="info" />;
    }
    return <FileIcon color="action" />;
  };

  const handleFileDownload = (fileUrl, fileName) => {
    if (!fileUrl) return;
    
    try {
      // Create a temporary link element
      const link = document.createElement('a');
      link.href = fileUrl;
      link.download = fileName || 'download';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (error) {
      console.error('Error downloading file:', error);
    }
  };

  if (messages.length === 0) {
    return (
      <MessagesContainer>
        <Box
          sx={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            height: '100%',
            gap: 2,
            opacity: 0.5,
          }}
        >
          <Chat sx={{ fontSize: 64, color: 'text.secondary' }} />
          <Typography variant="body1" color="text.secondary">
            No messages yet. Start the conversation!
          </Typography>
        </Box>
      </MessagesContainer>
    );
  }

  return (
    <MessagesContainer>
      {messages.map((message) => {
        const isOwn = message.userId === currentUser?.id;
        const isSystem = message.messageType === 'system';

        if (isSystem) {
          return (
            <SystemMessage key={message.id}>
              <Chat sx={{ fontSize: 16 }} />
              <Typography variant="caption">{message.message}</Typography>
            </SystemMessage>
          );
        }

        return (
          <Box
            key={message.id}
            sx={{
              display: 'flex',
              flexDirection: isOwn ? 'row-reverse' : 'row',
              alignItems: 'flex-end',
              gap: 1,
            }}
          >
            {!isOwn && (
              <Avatar 
                src={message.userAvatar} 
                sx={{ 
                  width: 32, 
                  height: 32,
                  bgcolor: theme.palette.primary.main 
                }}
              >
                {message.userName?.charAt(0).toUpperCase() || '?'}
              </Avatar>
            )}

            <MessageBubble isOwn={isOwn} isSystem={false} elevation={1}>
              {!isOwn && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                  <Typography 
                    variant="caption" 
                    fontWeight="bold"
                    sx={{ color: theme.palette.primary.main }}
                  >
                    {message.userName || 'Unknown User'}
                  </Typography>
                  {message.isPrivate && (
                    <Chip 
                      icon={<PrivateIcon />} 
                      label="Private" 
                      size="small"
                      sx={{ height: 20, fontSize: '0.7rem' }}
                    />
                  )}
                </Box>
              )}

              {message.messageType === 'file' ? (
                <FileMessage 
                  onClick={() => handleFileDownload(message.fileUrl, message.message)}
                >
                  {getFileIcon(message.fileType, message.message)}
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Typography variant="body2" noWrap>
                      {message.message}
                    </Typography>
                    {message.fileSize && (
                      <Typography variant="caption" color="text.secondary">
                        {formatFileSize(message.fileSize)}
                      </Typography>
                    )}
                  </Box>
                  <IconButton size="small" color={isOwn ? "inherit" : "primary"}>
                    <DownloadIcon fontSize="small" />
                  </IconButton>
                </FileMessage>
              ) : (
                <Typography variant="body2">{message.message}</Typography>
              )}

              <Typography 
                variant="caption" 
                sx={{ 
                  display: 'block',
                  mt: 0.5,
                  opacity: 0.7,
                  fontSize: '0.7rem' 
                }}
              >
                {formatTime(message.timestamp)}
              </Typography>
            </MessageBubble>
          </Box>
        );
      })}

      <div ref={messagesEndRef} />
    </MessagesContainer>
  );
};

export default MessageList;