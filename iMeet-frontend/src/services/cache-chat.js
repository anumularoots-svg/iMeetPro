// src/services/cache-chat.js - Fixed with private file upload support

import { API_BASE_URL } from '../utils/constants';

class CacheChatService {
  constructor() {
    this.apiBaseUrl = API_BASE_URL || 'http://localhost:8000';
    this.activeMeetings = new Set();
    this.messageCache = new Map();
    this.lastSyncTimestamp = new Map();
    this.pollIntervals = new Map();
    this.lastMessageCount = new Map();
    this.pendingSync = new Map();
     this.userContext = new Map();
  }
async startMeetingChat(meetingId, userId = null, isHost = false) {
  try {
    console.log('🎬 Starting enhanced cache-only chat for meeting:', meetingId);
    
    this.activeMeetings.add(meetingId);
    this.messageCache.set(meetingId, []);
    this.lastSyncTimestamp.set(meetingId, Date.now());
    this.lastMessageCount.set(meetingId, 0);
    this.pendingSync.set(meetingId, false);
    
    // NEW: Store user context for private message filtering
    this.userContext.set(meetingId, { userId, isHost });
    
    this.initializeInBackground(meetingId);
    
    console.log('✅ Enhanced meeting chat started (cache-only):', meetingId, { userId, isHost });
    return { success: true };
    
  } catch (error) {
    console.error('❌ Failed to start meeting chat:', error);
    return {
      success: false,
      error: error.message
    };
  }
}
// NEW: Method to update user context after initialization
setUserContext(meetingId, userId, isHost) {
  this.userContext.set(meetingId, { userId, isHost });
  console.log('👤 User context updated for meeting:', meetingId, { userId, isHost });
}
  async initializeInBackground(meetingId) {
    try {
      const response = await fetch(`${this.apiBaseUrl}/api/cache-chat/start/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meeting_id: meetingId
        })
      });

      if (response.ok) {
        console.log('✅ Background initialization complete for:', meetingId);
        this.startEnhancedPolling(meetingId);
        this.syncMessages(meetingId, true);
      } else {
        console.warn('⚠️ Background initialization failed, but chat still functional');
        this.startEnhancedPolling(meetingId);
      }
      
    } catch (error) {
      console.warn('⚠️ Background initialization error:', error);
      this.startEnhancedPolling(meetingId);
    }
  }

  startEnhancedPolling(meetingId) {
    if (this.pollIntervals.has(meetingId)) {
      clearInterval(this.pollIntervals.get(meetingId));
    }

    const pollInterval = setInterval(async () => {
      try {
        if (this.pendingSync.get(meetingId)) {
          return;
        }
        
        await this.syncMessages(meetingId);
      } catch (error) {
        console.error('Polling error:', error);
      }
    }, 3000);

    this.pollIntervals.set(meetingId, pollInterval);
  }

 async syncMessages(meetingId, force = false) {
  try {
    if (this.pendingSync.get(meetingId) && !force) {
      return;
    }

    this.pendingSync.set(meetingId, true);
    
    // Get stored user context for private message filtering
    const userCtx = this.userContext.get(meetingId) || {};
    const response = await this.getChatHistory(meetingId, 100, 0, userCtx.userId, userCtx.isHost);

    if (response.success && response.messages) {
      // FORMAT MESSAGES BEFORE CACHING - THIS IS THE KEY FIX
      const formattedMessages = response.messages.map(msg => this.formatMessage(msg));
      
      const currentCount = formattedMessages.length;
      const lastCount = this.lastMessageCount.get(meetingId) || 0;
      const lastMessages = this.messageCache.get(meetingId) || [];

      // Compare using formatted message IDs
      const hasNewMessages = force || 
        currentCount !== lastCount ||
        (formattedMessages.length > 0 && lastMessages.length > 0 && 
         formattedMessages[formattedMessages.length - 1]?.id !== lastMessages[lastMessages.length - 1]?.id);

      if (hasNewMessages) {
        console.log(`📥 Updating messages for ${meetingId}: ${currentCount} total (was ${lastCount})`);
        this.lastMessageCount.set(meetingId, currentCount);
        this.messageCache.set(meetingId, formattedMessages);  // Store FORMATTED messages
        this.lastSyncTimestamp.set(meetingId, Date.now());
        
        this.triggerMessageUpdate(meetingId, formattedMessages);  // Send FORMATTED messages
      }
    }
  } catch (error) {
    console.warn('⚠️ Sync failed for meeting', meetingId, ':', error);
  } finally {
    this.pendingSync.set(meetingId, false);
  }
}

  triggerMessageUpdate(meetingId, messages) {
    const event = new CustomEvent('cacheMessagesUpdated', {
      detail: { meetingId, messages }
    });
    window.dispatchEvent(event);
  }

  async sendMessage(messageData) {
    try {
      console.log('📤 Sending message:', messageData);
      
      if (!messageData.meetingId || !messageData.userId || !messageData.message) {
        throw new Error('Missing required fields: meetingId, userId, message');
      }
          
      const tempMessage = {
        id: `temp_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        message: messageData.message,
        userName: messageData.userName || 'You',
        userId: messageData.userId,
        timestamp: new Date().toISOString(),
        messageType: messageData.messageType || 'text',
        isPrivate: messageData.isPrivate || false,
        recipients: messageData.recipients || [],
        senderIsHost: messageData.senderIsHost || false,
        fileData: messageData.fileData,
        isTemp: true
      };

      const cachedMessages = this.messageCache.get(messageData.meetingId) || [];
      const updatedCache = [...cachedMessages, tempMessage];
      this.messageCache.set(messageData.meetingId, updatedCache);
      
      this.triggerMessageUpdate(messageData.meetingId, updatedCache);

      const payload = {
        meeting_id: messageData.meetingId,
        user_id: messageData.userId,
        user_name: messageData.userName || 'Anonymous',
        message: messageData.message.trim(),
        message_type: messageData.messageType || 'text',
        is_private: messageData.isPrivate || false,
        recipients: messageData.recipients || [],
        sender_is_host: messageData.senderIsHost || false,
        file_data: messageData.fileData
      };

      const response = await fetch(`${this.apiBaseUrl}/api/cache-chat/send/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const filteredMessages = cachedMessages.filter(msg => msg.id !== tempMessage.id);
        this.messageCache.set(messageData.meetingId, filteredMessages);
        this.triggerMessageUpdate(messageData.meetingId, filteredMessages);

        const errorData = await response.json().catch(() => ({ error: 'Failed to send message' }));
        throw new Error(errorData.error || `HTTP ${response.status}`);
      }

      const result = await response.json();
      
      if (result.success) {
        console.log('✅ Message sent to cache:', result);
        
        const finalMessages = updatedCache.map(msg => 
          msg.id === tempMessage.id 
            ? { ...msg, id: result.message_id, isTemp: false, timestamp: result.timestamp }
            : msg
        );
        this.messageCache.set(messageData.meetingId, finalMessages);
        this.triggerMessageUpdate(messageData.meetingId, finalMessages);
        
        return {
          success: true,
          messageId: result.message_id,
          timestamp: result.timestamp,
          storageType: 'cache_only'
        };
      } else {
        throw new Error(result.error || 'Failed to send message');
      }

    } catch (error) {
      console.error('❌ Failed to send message:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  async uploadFile(meetingId, file, userId, userName, onProgress = null, isPrivate = false, recipients = []) {
    try {
      console.log('📎 Starting enhanced file upload:', file.name);
      console.log('   - Is Private:', isPrivate);
      console.log('   - Recipients:', recipients);

      if (!file || !file.name) {
        throw new Error('Invalid file');
      }

      if (file.size > 50 * 1024 * 1024) {
        throw new Error('File too large (max 50MB)');
      }

      const formData = new FormData();
      formData.append('file', file);
      formData.append('meeting_id', meetingId);
      formData.append('user_id', userId);
      formData.append('user_name', userName);
      formData.append('is_private', isPrivate ? 'true' : 'false');
      formData.append('recipients', JSON.stringify(recipients));

      console.log('📤 Uploading file to backend with privacy settings...');

      const xhr = new XMLHttpRequest();
      
      return new Promise((resolve, reject) => {
        xhr.upload.addEventListener('progress', (event) => {
          if (event.lengthComputable && onProgress) {
            const progress = Math.round((event.loaded / event.total) * 100);
            onProgress(progress);
          }
        });

        xhr.addEventListener('load', async () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            try {
              const result = JSON.parse(xhr.responseText);
              
              if (result.success) {
                console.log('✅ File uploaded successfully:', result.file_id);
                console.log('   - Is Private:', result.is_private);
                console.log('   - Recipients:', result.recipients);
                
                setTimeout(() => this.syncMessages(meetingId, true), 100);
                setTimeout(() => this.syncMessages(meetingId, true), 500);
                
                resolve({
                  success: true,
                  fileId: result.file_id,
                  fileName: result.filename,
                  fileSize: result.size,
                  contentType: result.content_type,
                  downloadUrl: result.download_url,
                  isPrivate: result.is_private,
                  recipients: result.recipients
                });
              } else {
                reject(new Error(result.error || 'File upload failed'));
              }
            } catch (parseError) {
              reject(new Error('Invalid response from server'));
            }
          } else {
            reject(new Error(`Upload failed with status ${xhr.status}`));
          }
        });

        xhr.addEventListener('error', () => {
          reject(new Error('Network error during upload'));
        });

        xhr.open('POST', `${this.apiBaseUrl}/api/cache-chat/upload/`);
        xhr.send(formData);
      });

    } catch (error) {
      console.error('❌ File upload failed:', error);
      throw error;
    }
  }

  async getChatHistory(meetingId, limit = 100, offset = 0, userId = null, isHost = false) {
    try {
      console.log('📥 Fetching chat history with filters:', {
        meetingId,
        userId,
        isHost,
        limit
      });
      
      const url = new URL(`${this.apiBaseUrl}/api/cache-chat/history/${meetingId}/`);
      url.searchParams.append('limit', limit);
      url.searchParams.append('offset', offset);
      url.searchParams.append('_t', Date.now().toString());
      
      if (userId) {
        url.searchParams.append('user_id', userId.toString());
      }
      if (isHost !== undefined) {
        url.searchParams.append('is_host', isHost ? 'true' : 'false');
      }

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache'
        }
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Failed to get history' }));
        throw new Error(errorData.error || `HTTP ${response.status}`);
      }

      const result = await response.json();
      
      if (result.success) {
        const messages = result.messages || [];
        
        console.log('✅ Messages received:', {
          total: messages.length,
          userId,
          isHost
        });
        
        return {
          success: true,
          messages: messages,
          count: messages.length,
          totalCount: result.total_count || messages.length,
          privateChatEnabled: result.private_chat_enabled || false,
          storageType: 'cache_only'
        };
      } else {
        console.warn('⚠ Failed to get chat history:', result.error);
        
        const cachedMessages = this.messageCache.get(meetingId) || [];
        return {
          success: true,
          messages: cachedMessages,
          count: cachedMessages.length,
          totalCount: cachedMessages.length,
          isLocalFallback: true
        };
      }

    } catch (error) {
      console.error('❌ Failed to get chat history:', error);
      
      const cachedMessages = this.messageCache.get(meetingId) || [];
      return {
        success: false,
        error: error.message,
        messages: cachedMessages,
        count: cachedMessages.length,
        totalCount: cachedMessages.length,
        isLocalFallback: true
      };
    }
  }
  
  async forceRefresh(meetingId) {
    try {
      console.log('🔄 Force refreshing messages for meeting:', meetingId);
      
      this.pendingSync.set(meetingId, false);
      
      const response = await this.syncMessages(meetingId, true);
      
      setTimeout(async () => {
        const historyResponse = await this.getChatHistory(meetingId, 100, 0);
        if (historyResponse.success) {
          this.messageCache.set(meetingId, historyResponse.messages);
          this.triggerMessageUpdate(meetingId, historyResponse.messages);
        }
      }, 100);
      
      return { success: true };
    } catch (error) {
      console.error('❌ Force refresh failed:', error);
      return { success: false, error: error.message };
    }
  }

  createFileDownloadUrl(fileId) {
    return `${this.apiBaseUrl}/api/cache-chat/files/${fileId}/`;
  }

  async updateTypingStatus(meetingId, userId, userName, isTyping = true) {
    try {
      const payload = {
        meeting_id: meetingId,
        user_id: userId,
        user_name: userName,
        is_typing: isTyping
      };

      const response = await fetch(`${this.apiBaseUrl}/api/cache-chat/typing/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });

      return response.ok;
    } catch (error) {
      console.warn('Failed to update typing indicator:', error);
      return false;
    }
  }

  async getTypingUsers(meetingId) {
    try {
      const response = await fetch(`${this.apiBaseUrl}/api/cache-chat/typing-users/${meetingId}/`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Cache-Control': 'no-cache'
        }
      });

      if (!response.ok) {
        return { success: false, typingUsers: [] };
      }

      const result = await response.json();
      return {
        success: true,
        typingUsers: result.typing_users || [],
        count: result.count || 0
      };

    } catch (error) {
      console.warn('Failed to get typing users:', error);
      return { success: false, typingUsers: [] };
    }
  }

  stopPolling(meetingId) {
    if (this.pollIntervals.has(meetingId)) {
      clearInterval(this.pollIntervals.get(meetingId));
      this.pollIntervals.delete(meetingId);
      console.log('⏹️ Stopped polling for meeting:', meetingId);
    }
  }

  getCachedMessages(meetingId) {
    return this.messageCache.get(meetingId) || [];
  }

  formatMessage(rawMessage) {
   const formatted = {
    id: rawMessage.id || `${Date.now()}_${Math.random()}`,
    message: rawMessage.message || '',
    userName: rawMessage.user_name || rawMessage.userName || 'Anonymous',
    userId: rawMessage.user_id || rawMessage.userId || 'anonymous',
    timestamp: rawMessage.timestamp || new Date().toISOString(),
    messageType: rawMessage.message_type || rawMessage.messageType || 'text',
    isPrivate: rawMessage.is_private || rawMessage.isPrivate || false,
    recipients: rawMessage.recipients || [],
    senderIsHost: rawMessage.sender_is_host || rawMessage.senderIsHost || false,
    storageType: 'cache_only'
  };

    if (rawMessage.file_data || rawMessage.fileData) {
      try {
        const fileData = typeof rawMessage.file_data === 'string' 
          ? JSON.parse(rawMessage.file_data)
          : rawMessage.file_data || rawMessage.fileData;
        
        formatted.fileData = fileData;
        formatted.messageType = 'file';
      } catch (error) {
        console.warn('Failed to parse file data:', error);
      }
    }

    return formatted;
  }

 clearLocalTracking() {
  console.log('🧹 Cleaning up enhanced chat service');
  
  for (const [meetingId, interval] of this.pollIntervals) {
    clearInterval(interval);
  }
  
  this.activeMeetings.clear();
  this.messageCache.clear();
  this.lastSyncTimestamp.clear();
  this.pollIntervals.clear();
  this.lastMessageCount.clear();
  this.pendingSync.clear();
  this.userContext.clear(); // NEW: Clear user context
}
}

const cacheChatService = new CacheChatService();
export default cacheChatService;
export { CacheChatService };