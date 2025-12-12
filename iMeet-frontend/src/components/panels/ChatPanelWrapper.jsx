// src/components/panels/ChatPanelWrapper.jsx
import React from 'react';
import { Box } from '@mui/material';
import ChatPanel from '../chat/ChatPanel';

const ChatPanelWrapper = ({
  isOpen,
  onClose,
  meetingId,
  currentUser,
  participants,
  hasHostPrivileges,
  chatPermissions,
  onUnreadCountChange,
  onTotalMessagesChange,
  onMessageReceived,
  onChatOpened,
}) => {
  if (!isOpen) return null;

  return (
   <Box
  className="chat-panel-container"
  sx={{
    width: 450,
    height: 'calc(100vh - 80px)', // Account for control buttons
    minHeight: 0,
    background: 'rgba(26, 32, 44, 0.98)',
    backdropFilter: 'blur(20px)',
    WebkitBackdropFilter: 'blur(20px)',
    borderLeft: '1px solid rgba(255, 255, 255, 0.08)',
    borderRadius: '12px',
    margin: 0,
    overflow: 'hidden',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
    position: 'relative', // Add this for better positioning
    zIndex: 1250, // Lower than chat panel but higher than controls
  }}
>
      <ChatPanel
        isOpen={isOpen}
        isChatOpen={isOpen}
        onClose={onClose}
        meetingId={meetingId}
        currentUser={currentUser}
        participants={participants}
        isHost={hasHostPrivileges}
        chatPermissions={chatPermissions}
        onUnreadCountChange={onUnreadCountChange}
        onTotalMessagesChange={onTotalMessagesChange}
        onMessageReceived={onMessageReceived}
        onChatOpened={onChatOpened}
      />
    </Box>
  );
};

export default ChatPanelWrapper;