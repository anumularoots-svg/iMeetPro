// src/services/livekit-chat.js - LiveKit Chat Service

import { API_BASE_URL } from '../utils/constants';

class LiveKitChatService {
  constructor() {
    this.apiBaseUrl = API_BASE_URL || 'http://localhost:8000';
  }

  /**
   * Send a chat message through LiveKit
   */
  async sendChatMessage(messageData) {
    try {
      console.log('📤 Sending chat message via LiveKit:', messageData);
      
      const payload = {
        message: messageData.message,
        meeting_id: messageData.meetingId,
        user_id: messageData.userId,
        user_name: messageData.userName,
        message_type: messageData.messageType || 'public',
        timestamp: messageData.timestamp || new Date().toISOString()
      };

      const response = await fetch(`${this.apiBaseUrl}/api/livekit/send-chat/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Network error' }));
        throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();
      console.log('✅ Chat message sent successfully:', result);
      
      return {
        success: true,
        data: result,
        messageId: result.message_id || Date.now()
      };

    } catch (error) {
      console.error('❌ Failed to send chat message:', error);
      return {
        success: false,
        error: error.message,
        code: 'SEND_MESSAGE_FAILED'
      };
    }
  }

  /**
   * Get chat history for a meeting
   */
  async getChatHistory(meetingId, limit = 50) {
    try {
      console.log('📥 Fetching chat history for meeting:', meetingId);
      
      const url = new URL(`${this.apiBaseUrl}/api/livekit/chat-history/${meetingId}/`);
      url.searchParams.append('limit', limit);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Network error' }));
        throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();
      console.log('✅ Chat history fetched successfully:', result);
      
      return {
        success: true,
        messages: result.messages || [],
        totalCount: result.total_count || 0
      };

    } catch (error) {
      console.error('❌ Failed to fetch chat history:', error);
      return {
        success: false,
        error: error.message,
        messages: []
      };
    }
  }

  /**
   * Format message for display
   */
  formatMessage(rawMessage) {
    return {
      id: rawMessage.id || rawMessage.message_id || `${rawMessage.timestamp}_${Math.random()}`,
      message: rawMessage.message || rawMessage.text || '',
      userName: rawMessage.user_name || rawMessage.userName || rawMessage.sender_name || 'Unknown',
      userId: rawMessage.user_id || rawMessage.userId || rawMessage.sender_id || 'anonymous',
      timestamp: rawMessage.timestamp || new Date().toISOString(),
      messageType: rawMessage.message_type || rawMessage.type || 'public',
      isPrivate: rawMessage.message_type === 'private' || rawMessage.isPrivate || false,
      meetingId: rawMessage.meeting_id || rawMessage.meetingId
    };
  }

  /**
   * Validate message before sending
   */
  validateMessage(messageData) {
    const errors = [];

    if (!messageData.message || messageData.message.trim().length === 0) {
      errors.push('Message content is required');
    }

    if (messageData.message && messageData.message.length > 1000) {
      errors.push('Message content cannot exceed 1000 characters');
    }

    if (!messageData.meetingId) {
      errors.push('Meeting ID is required');
    }

    if (!messageData.userId) {
      errors.push('User ID is required');
    }

    if (!messageData.userName || messageData.userName.trim().length === 0) {
      errors.push('User name is required');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  /**
   * Clean message content (remove harmful content, trim, etc.)
   */
  cleanMessage(message) {
    if (!message || typeof message !== 'string') {
      return '';
    }

    return message
      .trim()
      .replace(/\s+/g, ' ') // Replace multiple spaces with single space
      .substring(0, 1000); // Limit to 1000 characters
  }

  /**
   * Send typing indicator
   */
  async sendTypingIndicator(meetingId, userId, userName, isTyping = true) {
    try {
      const payload = {
        meeting_id: meetingId,
        user_id: userId,
        user_name: userName,
        is_typing: isTyping
      };

      const response = await fetch(`${this.apiBaseUrl}/api/livekit/typing-indicator/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      return response.ok;
    } catch (error) {
      console.warn('Failed to send typing indicator:', error);
      return false;
    }
  }

  /**
   * Upload file for chat
   */
  async uploadChatFile(file, meetingId, userId) {
    try {
      console.log('📎 Uploading chat file:', file.name);

      const formData = new FormData();
      formData.append('file', file);
      formData.append('meeting_id', meetingId);
      formData.append('user_id', userId);

      const response = await fetch(`${this.apiBaseUrl}/api/livekit/upload-chat-file/`, {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Upload failed' }));
        throw new Error(errorData.error || 'File upload failed');
      }

      const result = await response.json();
      console.log('✅ File uploaded successfully:', result);

      return {
        success: true,
        fileUrl: result.file_url,
        fileName: result.file_name,
        fileSize: result.file_size
      };

    } catch (error) {
      console.error('❌ File upload failed:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }
}

// Create and export singleton instance
const livekitChatService = new LiveKitChatService();

export default livekitChatService;

// Also export the class for testing/custom instances
export { LiveKitChatService };