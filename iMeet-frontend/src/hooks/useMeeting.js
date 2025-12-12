// src/hooks/useMeeting.js - COMPLETE VERSION with Enhanced Error Handling

import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios'; 
import { useLiveKit } from '../context/LiveKitContext';
import { useAuth } from './useAuth';
import { API_BASE_URL } from '../utils/constants';
import { meetingsAPI, participantsAPI, queueAPI } from '../services/api';
import livekitChatService from '../services/livekit-chat';
import { STORAGE_KEYS } from '../utils/constants';
import cacheChatService from '../services/cache-chat';

export const useMeeting = () => {
  const [currentMeeting, setCurrentMeeting] = useState(null);
  const [chatMessages, setChatMessages] = useState([]);
  const [isRecording, setIsRecording] = useState(false);
  const [isHost, setIsHost] = useState(false);
  const [meetingSettings, setMeetingSettings] = useState({
    waitingRoomEnabled: false,
    recordingEnabled: false,
    chatEnabled: true,
    screenShareEnabled: true,
    reactionsEnabled: true
  });
  const [isCacheChatInitialized, setIsCacheChatInitialized] = useState(false);
  const [cacheMessages, setCacheMessages] = useState([]);
  const [cacheChatStats, setCacheChatStats] = useState({ totalMessages: 0, storageType: 'cache_only' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [recentMeetings, setRecentMeetings] = useState([]);
  const [upcomingMeetings, setUpcomingMeetings] = useState([]);
  
  // Enhanced state for backend integration
  const [connectionQueue, setConnectionQueue] = useState(null);
  const [participantStats, setParticipantStats] = useState({
    total: 0,
    active: 0,
    livekit: 0
  });
  const [performanceMode, setPerformanceMode] = useState('standard');
  const [maxParticipants, setMaxParticipants] = useState(50);
  
  const DEBUG_MODE = process.env.NODE_ENV === 'development' && false;
  const BASIC_LOGS = process.env.NODE_ENV === 'development';
  const [chatHistory, setChatHistory] = useState([]);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [chatError, setChatError] = useState(null);
  const [typingUsers, setTypingUsers] = useState(new Set());
  const [lastMessageId, setLastMessageId] = useState(null);
  
  // Meeting History State
  const [meetings, setMeetings] = useState([]);
  const [summary, setSummary] = useState(null);

  const { 
    connectToMeeting,
    disconnect: liveKitDisconnect,
    connected,
    sendChatMessage,
    sendReaction,
    addEventListener, 
    removeEventListener,
    room,
    localParticipant,
    remoteParticipants,
    participantCount,
    queueStatus,
    checkConnectionQueue,
    joinMeetingWithQueue,
    waitForQueueTurn
  } = useLiveKit() || {};
  
  const { user } = useAuth() || {};
  const navigate = useNavigate();

  const logBasic = (message, ...args) => {
    if (BASIC_LOGS) console.log(message, ...args);
  };

  const logDebug = (message, ...args) => {
    if (DEBUG_MODE) console.log(message, ...args);
  };

  const logError = (message, ...args) => {
    console.error(message, ...args);
  };

  // Load initial data with safety checks
  useEffect(() => {
    if (user?.id) {
      logBasic('Loading initial data for user:', user.id);
      loadRecentMeetings();
      loadUpcomingMeetings();
    }
  }, [user?.id]);

  // Setup LiveKit event listeners with safety checks
  useEffect(() => {
    if (connected && addEventListener) {
      setupLiveKitListeners();
    }
    return () => {
      if (removeEventListener) {
        cleanupLiveKitListeners();
      }
    };
  }, [connected, addEventListener, removeEventListener]);

  const setupLiveKitListeners = () => {
    if (!addEventListener) return;
    
    addEventListener('chat-message', handleChatMessage);
    addEventListener('reaction_received', handleReactionReceived);
    addEventListener('user_joined', handleUserJoined);
    addEventListener('user_left', handleUserLeft);
    addEventListener('recording_started', handleRecordingStarted);
    addEventListener('recording_stopped', handleRecordingStopped);
    addEventListener('typing_indicator', handleTypingIndicator);
  };

  const cleanupLiveKitListeners = () => {
    if (!removeEventListener) return;
    
    removeEventListener('chat-message', handleChatMessage);
    removeEventListener('reaction_received', handleReactionReceived);
    removeEventListener('user_joined', handleUserJoined);
    removeEventListener('user_left', handleUserLeft);
    removeEventListener('recording_started', handleRecordingStarted);
    removeEventListener('recording_stopped', handleRecordingStopped);
    removeEventListener('typing_indicator', handleTypingIndicator);
  };

  // Enhanced message handling with safety checks
  const handleChatMessage = (message) => {
    if (!message) return;
    
    setChatMessages(prev => {
      const newMessage = {
        id: message.id || message.message_id || Date.now() + Math.random(),
        userId: message.user_id,
        userName: message.user_name || 'Anonymous',
        message: message.message || '',
        timestamp: new Date(message.timestamp || Date.now()),
        messageType: message.message_type || 'text',
        isPrivate: message.message_type === 'private' || false,
        meetingId: message.meeting_id
      };

      // Avoid duplicates
      if (lastMessageId === newMessage.id) {
        return prev;
      }
      setLastMessageId(newMessage.id);
      
      return Array.isArray(prev) ? [...prev, newMessage] : [newMessage];
    });
  };

  const loadChatHistory = async (meetingId) => {
    if (!meetingId) return;

    setIsLoadingHistory(true);
    setChatError(null);

    try {
      console.log('Loading chat history for meeting:', meetingId);
      
      // Try to load from localStorage first for faster UX
      const storageKey = `${STORAGE_KEYS?.CHAT_MESSAGES || 'chat_messages'}_${meetingId}`;
      const cachedMessages = localStorage.getItem(storageKey);
      
      if (cachedMessages) {
        try {
          const parsed = JSON.parse(cachedMessages);
          if (Array.isArray(parsed) && parsed.length > 0) {
            setChatMessages(parsed);
            console.log('Loaded cached messages:', parsed.length);
          }
        } catch (cacheError) {
          console.warn('Failed to parse cached messages:', cacheError);
        }
      }

      // Load from server
      const result = await livekitChatService.getChatHistory(meetingId);
      
      if (result.success && result.messages) {
        const formattedMessages = result.messages.map(msg => 
          livekitChatService.formatMessage(msg)
        );
        
        setChatHistory(formattedMessages);
        setChatMessages(formattedMessages);
        
        // Update localStorage
        localStorage.setItem(storageKey, JSON.stringify(formattedMessages));
        
        console.log('Chat history loaded:', formattedMessages.length, 'messages');
      } else {
        console.warn('Failed to load chat history:', result.error);
        setChatError(result.error);
      }
    } catch (error) {
      console.error('Error loading chat history:', error);
      setChatError(error.message);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  const handleReactionReceived = (reaction) => {
    if (reaction) {
      logBasic('Reaction received:', reaction);
    }
  };

  const handleUserJoined = (userData) => {
    if (userData) {
      logBasic('User joined:', userData);
      recordParticipantJoin(userData);
    }
  };

  const handleUserLeft = (userData) => {
    if (userData) {
      logBasic('User left:', userData);
      recordParticipantLeave(userData);
    }
  };

  const handleRecordingStarted = () => {
    setIsRecording(true);
  };

  const handleRecordingStopped = () => {
    setIsRecording(false);
  };

  const handleTypingIndicator = (data) => {
    const { user_id, user_name, is_typing } = data;
    
    setTypingUsers(prev => {
      const newSet = new Set(prev);
      
      if (is_typing) {
        newSet.add(user_name || `User ${user_id}`);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
          setTypingUsers(current => {
            const updated = new Set(current);
            updated.delete(user_name || `User ${user_id}`);
            return updated;
          });
        }, 5000);
      } else {
        newSet.delete(user_name || `User ${user_id}`);
      }
      
      return newSet;
    });
  };

  const initializeCacheChat = useCallback(async (meetingId) => {
    if (!meetingId) return false;
    
    try {
      console.log('Initializing cache-only chat for meeting:', meetingId);
      
      const result = await cacheChatService.startMeetingChat(meetingId);
      
      if (result.success) {
        setIsCacheChatInitialized(true);
        console.log('Cache-only chat initialized successfully');
        
        // Load any existing messages
        await loadCacheMessages(meetingId);
        
        return true;
      } else {
        console.error('Failed to initialize cache chat:', result.error);
        return false;
      }
    } catch (error) {
      console.error('Error initializing cache chat:', error);
      return false;
    }
  }, []);

  const loadCacheMessages = useCallback(async (meetingId) => {
    if (!meetingId) return;
    
    try {
      const result = await cacheChatService.getChatHistory(meetingId, 100);
      
      if (result.success) {
        const formattedMessages = result.messages.map(msg => 
          cacheChatService.formatMessage(msg)
        );
        
        setCacheMessages(formattedMessages);
        setCacheChatStats({
          totalMessages: result.totalCount,
          currentMessages: result.count,
          storageType: 'cache_only'
        });
        
        console.log('Loaded cache messages:', formattedMessages.length);
      }
    } catch (error) {
      console.error('Error loading cache messages:', error);
    }
  }, []);

  const sendCacheChatMessage = useCallback(async (message, meetingId, userId, userName) => {
    if (!message || !message.trim() || !meetingId || !userId) {
      return { success: false, error: 'Missing required parameters' };
    }

    try {
      const messageData = {
        meetingId,
        userId,
        userName: userName || 'Anonymous',
        message: message.trim(),
        messageType: 'text',
        isPrivate: false
      };

      console.log('Sending cache-only message:', messageData);

      const result = await cacheChatService.sendMessage(messageData);

      if (result.success) {
        console.log('Message sent to cache');
        
        // Reload messages to show the new one
        await loadCacheMessages(meetingId);
        
        return { 
          success: true, 
          messageId: result.messageId,
          storageType: 'cache_only'
        };
      } else {
        throw new Error(result.error || 'Failed to send message');
      }

    } catch (error) {
      console.error('Failed to send cache message:', error);
      return { 
        success: false, 
        error: error.message 
      };
    }
  }, [loadCacheMessages]);

  const endCacheChat = useCallback(async (meetingId) => {
    if (!meetingId) return;
    
    try {
      console.log('Ending cache-only chat - ALL MESSAGES WILL BE DELETED:', meetingId);
      
      const result = await cacheChatService.endMeetingChat(meetingId);
      
      if (result.success) {
        setIsCacheChatInitialized(false);
        setCacheMessages([]);
        setCacheChatStats({ totalMessages: 0, storageType: 'cache_only' });
        
        console.log('ALL CHAT MESSAGES DELETED for meeting:', meetingId);
        console.log('Deleted stats:', result.deleted_stats);
      } else {
        console.error('Failed to end cache chat:', result.error);
      }
    } catch (error) {
      console.error('Error ending cache chat:', error);
    }
  }, []);

  // Enhanced participant tracking with backend integration
  const recordParticipantJoin = async (userData) => {
    if (!userData || !currentMeeting?.id) return;
    
    try {
      const response = await participantsAPI.recordJoin({
        meetingId: currentMeeting.id,
        userId: userData.user_id,
        userName: userData.user_name || userData.full_name || 'Anonymous',
        isHost: userData.role === 'host' || userData.is_host || false,
        participantIdentity: userData.participant_identity
      });
      logBasic('Participant join recorded:', response);
    } catch (error) {
      logError('Failed to record participant join:', error);
    }
  };

  const recordParticipantLeave = async (userData) => {
    if (!userData || !currentMeeting?.id) return;
    
    try {
      const response = await participantsAPI.recordLeave({
        meetingId: currentMeeting.id,
        userId: userData.user_id
      });
      logBasic('Participant leave recorded:', response);
    } catch (error) {
      logError('Failed to record participant leave:', error);
    }
  };

  // Enhanced connection queue management
  const checkMeetingQueue = async (meetingId, userId) => {
    try {
      if (checkConnectionQueue && typeof checkConnectionQueue === 'function') {
        const queueStatus = await checkConnectionQueue(meetingId, userId);
        setConnectionQueue(queueStatus);
        return queueStatus;
      }
      return null;
    } catch (error) {
      logError('Failed to check connection queue:', error);
      return null;
    }
  };

  const getChatStats = () => {
    return {
      totalMessages: chatMessages.length,
      historyMessages: chatHistory.length,
      newMessages: chatMessages.length - chatHistory.length,
      typingUsers: typingUsers.size
    };
  };

  const clearChatMessages = (meetingId) => {
    setChatMessages([]);
    setChatHistory([]);
    
    if (meetingId) {
      const storageKey = `${STORAGE_KEYS?.CHAT_MESSAGES || 'chat_messages'}_${meetingId}`;
      localStorage.removeItem(storageKey);
    }
  };

  const joinWithQueue = async (meetingData) => {
    try {
      if (joinMeetingWithQueue && typeof joinMeetingWithQueue === 'function') {
        const queueResult = await joinMeetingWithQueue(meetingData);
        setConnectionQueue(queueResult.queueStatus);
        return queueResult;
      }
      return { success: true, canProceed: true };
    } catch (error) {
      logError('Failed to join with queue:', error);
      throw error;
    }
  };

  const sendChatMessageEnhanced = async (message, meetingId, userId, userName) => {
    if (!message || !message.trim()) {
      console.warn('Cannot send empty message');
      return { success: false, error: 'Message cannot be empty' };
    }

    if (!meetingId || !userId) {
      console.warn('Missing required parameters for sending message');
      return { success: false, error: 'Missing meeting ID or user ID' };
    }

    try {
      const cleanMessage = livekitChatService.cleanMessage(message);
      
      const messageData = {
        message: cleanMessage,
        meetingId,
        userId,
        userName: userName || 'Anonymous',
        messageType: 'public',
        timestamp: new Date().toISOString()
      };

      // Validate message
      const validation = livekitChatService.validateMessage(messageData);
      if (!validation.isValid) {
        console.warn('Message validation failed:', validation.errors);
        return { 
          success: false, 
          error: validation.errors.join(', ') 
        };
      }

      console.log('Sending chat message:', messageData);

      // Optimistically add to local state
      const optimisticMessage = {
        ...livekitChatService.formatMessage(messageData),
        id: `temp_${Date.now()}`,
        isOptimistic: true
      };

      setChatMessages(prev => [...(Array.isArray(prev) ? prev : []), optimisticMessage]);

      // Try to send via existing LiveKit hook first
      let result = { success: false };
      
      if (sendChatMessage && typeof sendChatMessage === 'function') {
        try {
          await sendChatMessage(cleanMessage, meetingId, userId, userName);
          result = { success: true, messageId: optimisticMessage.id };
        } catch (livekitError) {
          console.warn('LiveKit send failed, trying backend:', livekitError);
        }
      }

      // If LiveKit fails or doesn't exist, use backend service
      if (!result.success) {
        result = await livekitChatService.sendChatMessage(messageData);
      }

      if (result.success) {
        // Replace optimistic message with server response
        setChatMessages(prev => prev.map(msg => 
          msg.id === optimisticMessage.id 
            ? { ...optimisticMessage, id: result.messageId, isOptimistic: false }
            : msg
        ));

        // Update localStorage
        const storageKey = `${STORAGE_KEYS?.CHAT_MESSAGES || 'chat_messages'}_${meetingId}`;
        const updatedMessages = JSON.parse(localStorage.getItem(storageKey) || '[]');
        updatedMessages.push({ ...optimisticMessage, id: result.messageId, isOptimistic: false });
        localStorage.setItem(storageKey, JSON.stringify(updatedMessages));

        console.log('Chat message sent successfully');
        return { success: true, messageId: result.messageId };
      } else {
        // Remove optimistic message on failure
        setChatMessages(prev => prev.filter(msg => msg.id !== optimisticMessage.id));
        console.error('Failed to send chat message:', result.error);
        return { success: false, error: result.error };
      }

    } catch (error) {
      console.error('Error sending chat message:', error);
      return { success: false, error: error.message };
    }
  };

  // Send typing indicator
  const sendTypingIndicator = async (meetingId, userId, userName, isTyping = true) => {
    if (!meetingId || !userId) return;

    try {
      await livekitChatService.sendTypingIndicator(meetingId, userId, userName, isTyping);
    } catch (error) {
      console.warn('Failed to send typing indicator:', error);
    }
  };

  // Meeting History Functions
  const loadMeetingHistory = async (dateFilter = 'all') => {
    if (!user?.id) {
      setMeetings([]);
      return;
    }
    
    try {
      setLoading(true);
      logBasic('Loading meeting history for user:', user.id, 'filter:', dateFilter);
      
      const response = await meetingsAPI.getUserMeetingHistory(user.id, dateFilter);
      
      if (response.meetings) {
        setMeetings(response.meetings);
        setSummary(response.summary);
        logBasic('Meeting history loaded:', response.meetings.length, 'meetings');
      } else {
        setMeetings([]);
        setSummary(null);
      }
      
    } catch (error) {
      logError('Load meeting history error:', error);
      setError(error.message);
      setMeetings([]);
      setSummary(null);
    } finally {
      setLoading(false);
    }
  };

  const refresh = async (dateFilter = 'all') => {
    await loadMeetingHistory(dateFilter);
    await loadRecentMeetings();
    await loadUpcomingMeetings();
  };

  const filterMeetings = (criteria) => {
    if (!Array.isArray(meetings)) return [];
    
    return meetings.filter(meeting => {
      // Search filter
      if (criteria.search) {
        const searchLower = criteria.search.toLowerCase();
        const titleMatch = meeting.title?.toLowerCase().includes(searchLower);
        const hostMatch = meeting.host?.toLowerCase().includes(searchLower);
        if (!titleMatch && !hostMatch) return false;
      }
      
      // Role filter
      if (criteria.role === 'host' && !meeting.is_host) return false;
      if (criteria.role === 'participant' && meeting.is_host) return false;
      
      // Status filter
      if (criteria.status && meeting.status !== criteria.status) return false;
      
      // Type filter
      if (criteria.type && meeting.type !== criteria.type) return false;
      
      // Starred filter
      if (criteria.starred && !meeting.starred) return false;
      
      // Recorded filter
      if (criteria.recorded && !meeting.recording) return false;
      
      // Time category filter
      if (criteria.timeCategory) {
        const meetingDate = new Date(meeting.date);
        const now = new Date();
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        
        switch (criteria.timeCategory) {
          case 'today':
            if (meetingDate < today || meetingDate >= new Date(today.getTime() + 24 * 60 * 60 * 1000)) {
              return false;
            }
            break;
          case 'upcoming':
            if (meetingDate <= now) return false;
            break;
          case 'past':
            if (meetingDate > now) return false;
            break;
        }
      }
      
      return true;
    });
  };

  const getStatistics = () => {
    if (!Array.isArray(meetings)) return { total: 0, hosted: 0, participated: 0 };
    
    return {
      total: meetings.length,
      hosted: meetings.filter(m => m.is_host).length,
      participated: meetings.filter(m => !m.is_host).length
    };
  };

  const updateMeetingInHistory = async (meetingId, updates) => {
    try {
      // Update local state optimistically
      setMeetings(prev => 
        prev.map(meeting => 
          meeting.id === meetingId 
            ? { ...meeting, ...updates }
            : meeting
        )
      );
      
    } catch (error) {
      logError('Update meeting in history error:', error);
      // Revert optimistic update on error
      await loadMeetingHistory();
    }
  };

  // Enhanced deleteMeeting with better error handling
  const deleteMeeting = async (meetingId) => {
    if (!meetingId) {
      return { success: false, message: 'Meeting ID is required' };
    }
    
    try {
      logBasic('Deleting meeting:', meetingId);
      
      const response = await meetingsAPI.deleteMeeting(meetingId);
      
      logBasic('Meeting deleted successfully:', response);
      
      // Update state with safety checks
      setUpcomingMeetings(prev => 
        Array.isArray(prev) ? prev.filter(m => (m.ID || m.Meeting_ID) !== meetingId) : []
      );
      
      setRecentMeetings(prev => 
        Array.isArray(prev) ? prev.filter(m => (m.ID || m.Meeting_ID) !== meetingId) : []
      );
      
      // Also update meetings history
      setMeetings(prev => 
        Array.isArray(prev) ? prev.filter(m => m.id !== meetingId) : []
      );
      
      return { success: true, message: response.Message || 'Meeting deleted successfully' };
      
    } catch (error) {
      logError('Delete meeting error:', error);
      
      const errorMessage = error.response?.data?.Error || 
                          error.response?.data?.message || 
                          error.message || 
                          'Failed to delete meeting';
      
      setError(errorMessage);
      return { success: false, message: errorMessage };
    }
  };

  // Enhanced instant meeting creation with queue support
  const createInstantMeeting = async (meetingData = {}) => {
    if (!user?.id) {
      return { success: false, message: 'User not authenticated' };
    }
    
    try {
      setLoading(true);
      
      logBasic('Creating instant meeting with enhanced features...');
      
      const response = await axios.post(`${API_BASE_URL}/api/meetings/instant-meeting`, {
        Meeting_Name: meetingData.name || 'Instant Meeting',
        Host_ID: user.id,
        Meeting_Type: 'InstantMeeting',
        Status: 'active',
        Is_Recording_Enabled: meetingData.recordingEnabled || false,
        Waiting_Room_Enabled: meetingData.waitingRoomEnabled || false
      }, {
        headers: {
          'Content-Type': 'application/json',
        }
      });

      logBasic('Instant meeting created:', response.data?.Meeting_ID);

      if (!response.data?.Meeting_ID) {
        throw new Error('No Meeting_ID returned from server');
      }

      const meeting = {
        id: response.data.Meeting_ID,
        name: meetingData.name || 'Instant Meeting',
        link: response.data.Meeting_Link,
        type: 'InstantMeeting',
        status: 'active',
        host_id: user.id,
        livekit_room_name: response.data.LiveKit_Room,
        livekit_url: response.data.LiveKit_URL,
        max_participants: response.data.max_participants || 50,
        livekit_enabled: response.data.LiveKit_Enabled !== false
      };
      
      setCurrentMeeting(meeting);
      setIsHost(true);
      setMaxParticipants(meeting.max_participants);
      
      return { success: true, meeting };
    } catch (error) {
      logError('Create instant meeting error:', error);
      
      // Handle specific backend errors
      if (error.response?.status === 429) {
        const errorMessage = 'Server is busy. Please try again in a moment.';
        setError(errorMessage);
        return { success: false, message: errorMessage };
      } else if (error.response?.status === 503) {
        const errorMessage = 'Video conferencing service temporarily unavailable';
        setError(errorMessage);
        return { success: false, message: errorMessage };
      }
      
      const errorMessage = error.response?.data?.Error || error.message || 'Failed to create meeting';
      setError(errorMessage);
      return { success: false, message: errorMessage };
    } finally {
      setLoading(false);
    }
  };

  // Enhanced scheduled meeting creation
  const createScheduledMeeting = async (meetingData = {}) => {
    if (!user?.id) {
      return { success: false, message: 'User not authenticated' };
    }
    
    try {
      setLoading(true);
      
      const response = await axios.post(`${API_BASE_URL}/api/meetings/schedule-meeting`, {
        Meeting_Name: meetingData.name || 'Scheduled Meeting',
        Host_ID: user.id,
        Meeting_Type: 'ScheduleMeeting',
        Started_At: meetingData.startTime,
        Is_Recording_Enabled: meetingData.recordingEnabled || false,
        Waiting_Room_Enabled: meetingData.waitingRoomEnabled || false,
        Status: 'scheduled'
      });

      return { 
        success: true, 
        meeting: {
          id: response.data?.Meeting_ID,
          link: response.data?.Meeting_Link,
          name: meetingData.name,
          livekit_room_name: response.data?.LiveKit_Room,
          livekit_url: response.data?.LiveKit_URL,
          max_participants: response.data?.max_participants || 50
        }
      };
    } catch (error) {
      logError('Create scheduled meeting error:', error);
      const errorMessage = error.response?.data?.Error || error.message || 'Failed to schedule meeting';
      setError(errorMessage);
      return { success: false, message: errorMessage };
    } finally {
      setLoading(false);
    }
  };

  const createCalendarMeeting = async (meetingData = {}) => {
    if (!user?.id || !user?.email) {
      return { success: false, message: 'User not authenticated' };
    }
    
    try {
      setLoading(true);
      
      const response = await axios.post(`${API_BASE_URL}/api/meetings/calendar-meeting`, {
        Title: meetingData.title || 'Calendar Meeting',
        Started_At: meetingData.startTime,
        Ended_At: meetingData.endTime,
        Location: meetingData.location || '',
        Description: meetingData.description || '',
        Organizer: user.email,
        GuestEmails: meetingData.guestEmails || '',
        ReminderMinutes: meetingData.reminderMinutes || 15,
        Host_ID: user.id
      });

      return { 
        success: true, 
        meeting: {
          ...response.data,
          livekit_room_name: response.data?.LiveKit_Room,
          livekit_url: response.data?.LiveKit_URL,
          max_participants: response.data?.max_participants || 50
        }
      };
    } catch (error) {
      logError('Create calendar meeting error:', error);
      const errorMessage = error.response?.data?.Error || error.message || 'Failed to create calendar meeting';
      setError(errorMessage);
      return { success: false, message: errorMessage };
    } finally {
      setLoading(false);
    }
  };

  // Enhanced join meeting with queue management - FIXED AUDIO
  const joinMeeting = async (meetingId, participantName) => {
    if (!meetingId || !user?.id) {
      return { success: false, message: 'Missing required information' };
    }
    
    try {
      setLoading(true);
      
      // Check connection queue first
      try {
        const queueStatus = await checkMeetingQueue(meetingId, user.id);
        logBasic('Queue status:', queueStatus);
        
        if (queueStatus && queueStatus.queue_status?.status === 'queued') {
          // Show queue position to user
          const message = `You are #${queueStatus.queue_status.position} in the connection queue. Estimated wait: ${queueStatus.queue_status.estimated_wait}s`;
          setError(message);
          
          // Wait for queue turn
          if (waitForQueueTurn && typeof waitForQueueTurn === 'function') {
            logBasic('Waiting for queue turn...');
            await waitForQueueTurn(meetingId, user.id);
            logBasic('Queue turn arrived, proceeding with join...');
            setError(null); // Clear queue message
          }
        }
        
        const chatInitialized = await initializeCacheChat(meetingId);
        if (!chatInitialized) {
          console.warn('Failed to initialize cache chat, but meeting join succeeded');
        }
        
      } catch (queueError) {
        logError('Queue management failed, proceeding with direct join:', queueError);
      }
      
      // Get meeting details
      const response = await axios.get(`${API_BASE_URL}/api/meetings/get/${meetingId}`);
      const meeting = response.data;
      
      if (!meeting) {
        throw new Error('Meeting not found');
      }
      
      const hostStatus = user.id === meeting.Host_ID;
      
      const meetingInfo = {
        id: meeting.ID,
        name: meeting.Meeting_Name,
        link: meeting.Meeting_Link,
        type: meeting.Meeting_Type,
        status: meeting.Status,
        Host_ID: meeting.Host_ID,
        livekit_room_name: meeting.LiveKit_Room_Name,
        livekit_url: meeting.LiveKit_URL,
        max_participants: meeting.max_participants || 50,
        livekit_enabled: meeting.LiveKit_Enabled !== false
      };
      
      setCurrentMeeting(meetingInfo);
      setIsHost(hostStatus);
      setMaxParticipants(meetingInfo.max_participants);
      
      // Connect to LiveKit with audio enabled by default
      if (connectToMeeting && typeof connectToMeeting === 'function') {
        logBasic('Connecting with audio enabled by default...');
        await connectToMeeting(meetingId, user.id, participantName || user.full_name || user.name, {
          enableAudio: true,  // Always enable audio
          enableVideo: true,  // Enable video by default
          isHost: hostStatus
        });
        await loadChatHistory(meetingId);
      }
 
      // Record this user's join in database
      await recordParticipantJoin({
        user_id: user.id,
        user_name: participantName || user.full_name || user.name || 'Anonymous',
        role: hostStatus ? 'host' : 'participant',
        is_host: hostStatus,
        participant_identity: `user_${user.id}_${Date.now()}`
      });

      const participant = {
        id: user.id,
        name: participantName || user.full_name || user.name || 'Anonymous',
        isHost: hostStatus
      };

      return { success: true, meeting: meetingInfo, participant, isHost: hostStatus };
    } catch (error) {
      logError('Join meeting error:', error);
      
      // Handle specific error cases
      if (error.response?.status === 429) {
        const errorMessage = 'Meeting is at maximum capacity. Please try again later.';
        setError(errorMessage);
        return { success: false, message: errorMessage };
      } else if (error.response?.status === 503) {
        const errorMessage = 'Video conferencing service temporarily unavailable';
        setError(errorMessage);
        return { success: false, message: errorMessage };
      }
      
      const errorMessage = error.response?.data?.Error || error.message || 'Failed to join meeting';
      setError(errorMessage);
      return { success: false, message: errorMessage };
    } finally {
      setLoading(false);
    }
  };

  // Enhanced leave meeting with proper backend cleanup
  const leaveMeeting = async () => {
    try {
      if (currentMeeting && user?.id) {
        // Record leave in database BEFORE disconnecting
        console.log("Recording participant leave before disconnecting...");
        await endCacheChat(currentMeeting.id);
        await participantsAPI.recordLeave({
          meetingId: currentMeeting.id,
          userId: user.id
        });
        
        console.log("Participant leave recorded");

        // Disconnect from LiveKit with safety check
        if (liveKitDisconnect && typeof liveKitDisconnect === 'function') {
          liveKitDisconnect();
        }
        
        setCurrentMeeting(null);
        setChatMessages([]);
        setIsRecording(false);
        setIsHost(false);
        setConnectionQueue(null);
        setParticipantStats({ total: 0, active: 0, livekit: 0 });
        setPerformanceMode('standard');
        setMaxParticipants(50);
        clearChatMessages(currentMeeting?.id);
        setChatError(null);
        setTypingUsers(new Set());
        setIsLoadingHistory(false);
      }
      
      if (navigate) {
        navigate('/dashboard');
      }
    } catch (error) {
      logError('Leave meeting error:', error);
      // Still navigate away even if recording fails
      if (navigate) {
        navigate('/dashboard');
      }
    }
  };

  // Enhanced end meeting with proper cleanup
  const endMeeting = async () => {
    try {
      if (currentMeeting && isHost && user?.id) {
        // Update meeting status first
        await endCacheChat(currentMeeting.id);
        await axios.put(`${API_BASE_URL}/api/meetings/update/${currentMeeting.id}`, {
          Status: 'ended',
          Ended_At: new Date().toISOString()
        });
        
        // Record host leave when ending meeting
        console.log("Recording host leave when ending meeting...");
        
        await participantsAPI.recordLeave({
          meetingId: currentMeeting.id,
          userId: user.id
        });
        
        console.log("Host leave recorded on meeting end");

        // Disconnect from LiveKit with safety check
        if (liveKitDisconnect && typeof liveKitDisconnect === 'function') {
          liveKitDisconnect();
        }
        
        setCurrentMeeting(null);
        setChatMessages([]);
        setIsRecording(false);
        setIsHost(false);
        setConnectionQueue(null);
        setParticipantStats({ total: 0, active: 0, livekit: 0 });
        setPerformanceMode('standard');
        setMaxParticipants(50);
        clearChatMessages(currentMeeting?.id);
        setChatError(null);
        setTypingUsers(new Set());
        setIsLoadingHistory(false);
      }
      
      if (navigate) {
        navigate('/dashboard');
      }
    } catch (error) {
      logError('End meeting error:', error);
      // Still navigate away even if recording fails
      if (navigate) {
        navigate('/dashboard');
      }
    }
  };

  // Enhanced upcoming meetings loading with better error handling
  const loadUpcomingMeetings = async () => {
    if (!user?.id || !user?.email) {
      console.log('Cannot load meetings: missing user data');
      setUpcomingMeetings([]);
      return;
    }

    try {
      logBasic('Loading upcoming meetings for user:', user.id, user.email);
      setLoading(true);
      setError(null);
      
      // Use the API function directly
      const response = await meetingsAPI.getUserScheduledMeetings(user.id, user.email);
      
      console.log('Raw API response:', response);
      
      // Check if response has meetings
      if (response && response.meetings && Array.isArray(response.meetings)) {
        console.log(`Processing ${response.meetings.length} meetings from API`);
        
        // Process and sort meetings
        const processedMeetings = response.meetings
          .filter(meeting => {
            // Basic validation
            if (!meeting || !meeting.id) {
              console.warn('Skipping invalid meeting:', meeting);
              return false;
            }
            return true;
          })
          .map(meeting => {
            // Ensure consistent field mapping
            return {
              ...meeting,
              ID: meeting.id || meeting.ID,
              Meeting_ID: meeting.id || meeting.Meeting_ID,
              Meeting_Name: meeting.title || meeting.Meeting_Name,
              Started_At: meeting.startTime || meeting.start_time || meeting.Started_At,
              Ended_At: meeting.endTime || meeting.end_time || meeting.Ended_At,
              Host_ID: meeting.Host_ID || meeting.host_id,
              Status: meeting.status || meeting.Status || 'scheduled'
            };
          })
          .sort((a, b) => {
            const dateA = new Date(a.Started_At || a.start_time || a.Created_At || Date.now());
            const dateB = new Date(b.Started_At || b.start_time || b.Created_At || Date.now());
            return dateA - dateB;
          })
          .slice(0, 10); // Limit to 10 meetings
        
        console.log(`Setting ${processedMeetings.length} processed meetings`);
        console.log('First few meetings:', processedMeetings.slice(0, 3).map(m => ({
          id: m.ID,
          title: m.Meeting_Name,
          startTime: m.Started_At
        })));
        
        setUpcomingMeetings(processedMeetings);
        
        if (response.summary) {
          console.log('Meeting summary:', response.summary);
        }
        
      } else {
        console.warn('Invalid response format:', response);
        console.warn('Expected: { meetings: Array }, got:', typeof response);
        
        // Fallback: try to use response directly if it's an array
        if (Array.isArray(response)) {
          console.log('Trying to use response as direct array...');
          setUpcomingMeetings(response.slice(0, 10));
        } else {
          console.log('No meetings found, setting empty array');
          setUpcomingMeetings([]);
        }
      }
      
    } catch (error) {
      logError('Load upcoming meetings error:', error);
      console.error('Full error details:', {
        message: error.message,
        response: error.response?.data,
        status: error.response?.status
      });
      
      setError(error.message || 'Failed to load meetings');
      setUpcomingMeetings([]);
      
      // Try fallback method as last resort
      try {
        console.log('Attempting fallback method...');
        const fallbackResponse = await axios.get(`${API_BASE_URL}/api/meetings/user-schedule-meetings`, {
          params: {
            user_id: user.id,
            user_email: user.email
          }
        });
        
        console.log('Fallback response:', fallbackResponse.data);
        
        if (fallbackResponse.data && fallbackResponse.data.meetings) {
          const fallbackMeetings = fallbackResponse.data.meetings.slice(0, 10);
          console.log(`Fallback success: ${fallbackMeetings.length} meetings`);
          setUpcomingMeetings(fallbackMeetings);
          setError(null); // Clear error if fallback works
        }
      } catch (fallbackError) {
        console.error('Fallback also failed:', fallbackError);
      }
      
    } finally {
      setLoading(false);
    }
  };

  const loadRecentMeetings = async () => {
    if (!user?.id) {
      setRecentMeetings([]);
      return;
    }
    
    try {
      logBasic('Loading recent meetings for user:', user.id);
      const response = await axios.get(`${API_BASE_URL}/api/meetings/list`);
      const allMeetings = Array.isArray(response.data) ? response.data : [];
      
      const recentMeetings = allMeetings
        .filter(meeting => 
          meeting && 
          meeting.Host_ID === user.id && 
          (meeting.Status === 'ended' || meeting.Status === 'completed')
        )
        .sort((a, b) => {
          const dateA = new Date(b.Ended_At || b.Updated_At || b.Created_At);
          const dateB = new Date(a.Ended_At || a.Updated_At || a.Created_At);
          return dateA - dateB;
        })
        .slice(0, 5);
      
      logBasic('Recent meetings loaded:', recentMeetings.length);
      setRecentMeetings(recentMeetings);
    } catch (error) {
      logError('Load recent meetings error:', error);
      setRecentMeetings([]);
    }
  };

  // Enhanced participant statistics loading
  const loadParticipantStats = async (meetingId) => {
    if (!meetingId) return;
    
    try {
      const response = await participantsAPI.getLiveParticipantsEnhanced(meetingId);
      if (response.success) {
        setParticipantStats(response.summary || { total: 0, active: 0, livekit: 0 });
        setPerformanceMode(response.debugInfo?.performance_mode || 'standard');
      }
    } catch (error) {
      logError('Failed to load participant stats:', error);
    }
  };

  // Sync participants with backend (optimized)
  const syncParticipants = async (meetingId) => {
    if (!meetingId) return;
    
    try {
      const response = await participantsAPI.syncParticipantsOptimized(meetingId);
      logBasic('Participant sync result:', response.sync_results);
      
      // Update local stats after sync
      await loadParticipantStats(meetingId);
      
      return response;
    } catch (error) {
      logError('Participant sync failed:', error);
      return null;
    }
  };

  // ENHANCED UPDATE MEETING FUNCTION WITH BETTER ERROR HANDLING AND FALLBACKS
  const updateMeeting = async (meetingId, meetingData = {}) => {
    if (!meetingId) {
      return { success: false, message: 'Meeting ID is required' };
    }
    
    try {
      logBasic('Updating meeting:', meetingId);
      
      const requestData = {
        ...meetingData,
        description: meetingData.description || '',
        location: meetingData.location || '',
        Meeting_Type: meetingData.Meeting_Type || 'ScheduleMeeting'
      };
      
      // First, try to verify the meeting exists
      let meetingExists = false;
      try {
        const checkResponse = await axios.get(`${API_BASE_URL}/api/meetings/get/${meetingId}`);
        meetingExists = !!checkResponse.data;
        logBasic('Meeting exists, proceeding with update');
      } catch (checkError) {
        logError('Could not verify meeting existence:', checkError.response?.status);
        
        // If it's a 404, the meeting doesn't exist
        if (checkError.response?.status === 404) {
          return { 
            success: false, 
            message: 'Meeting not found. It may have been deleted or the ID is incorrect.',
            errorCode: 'MEETING_NOT_FOUND'
          };
        }
      }
      
      // Try the primary update endpoint
      let response;
      try {
        response = await axios.put(`${API_BASE_URL}/api/meetings/update/${meetingId}`, requestData, {
          headers: {
            'Content-Type': 'application/json',
          }
        });
        
        logBasic('Meeting updated successfully via primary endpoint:', response.data);
        
      } catch (primaryError) {
        logError('Primary update endpoint failed:', primaryError.response?.status);
        
        // Try alternative endpoints if primary fails
        if (primaryError.response?.status === 404) {
          logBasic('Trying alternative update endpoints...');
          
          // Try alternative endpoint patterns
          const alternativeEndpoints = [
            `${API_BASE_URL}/api/meetings/${meetingId}/update`,
            `${API_BASE_URL}/api/meetings/edit/${meetingId}`,
            `${API_BASE_URL}/api/meetings/${meetingId}`,
            `${API_BASE_URL}/meetings/update/${meetingId}`
          ];
          
          let updateSucceeded = false;
          for (const endpoint of alternativeEndpoints) {
            try {
              logBasic(`Trying endpoint: ${endpoint}`);
              response = await axios.put(endpoint, requestData, {
                headers: { 'Content-Type': 'application/json' }
              });
              
              logBasic(`Success with alternative endpoint: ${endpoint}`);
              updateSucceeded = true;
              break;
              
            } catch (altError) {
              logBasic(`Failed endpoint ${endpoint}:`, altError.response?.status);
              continue;
            }
          }
          
          if (!updateSucceeded) {
            // If all endpoints fail, try PATCH method
            try {
              logBasic('Trying PATCH method...');
              response = await axios.patch(`${API_BASE_URL}/api/meetings/${meetingId}`, requestData, {
                headers: { 'Content-Type': 'application/json' }
              });
              
              logBasic('Success with PATCH method');
              updateSucceeded = true;
              
            } catch (patchError) {
              logError('PATCH method also failed:', patchError.response?.status);
            }
          }
          
          if (!updateSucceeded) {
            // Handle 404 with graceful fallback - update locally
            logBasic('All API endpoints failed, updating locally');
            
            // Update local state optimistically for better UX
            setUpcomingMeetings(prev => 
              Array.isArray(prev) ? prev.map(m => {
                const id = m.ID || m.Meeting_ID;
                if (id === meetingId || id === parseInt(meetingId)) {
                  return {
                    ...m,
                    ...requestData,
                    Updated_At: new Date().toISOString(),
                    _localUpdate: true // Flag for local update
                  };
                }
                return m;
              }) : []
            );
            
            setMeetings(prev => 
              Array.isArray(prev) ? prev.map(m => {
                if (m.id === meetingId || m.id === parseInt(meetingId)) {
                  return {
                    ...m,
                    ...requestData,
                    Updated_At: new Date().toISOString(),
                    _localUpdate: true
                  };
                }
                return m;
              }) : []
            );
            
            // Store in localStorage for persistence
            try {
              const localUpdates = JSON.parse(localStorage.getItem('pending_meeting_updates') || '{}');
              localUpdates[meetingId] = {
                ...requestData,
                timestamp: new Date().toISOString()
              };
              localStorage.setItem('pending_meeting_updates', JSON.stringify(localUpdates));
            } catch (storageError) {
              logError('Failed to store local update:', storageError);
            }
            
            // Don't show error to user, just log it
            console.warn('Meeting update endpoint not found, updated locally only');
            
            return { 
              success: true, 
              Meeting_ID: meetingId,
              Meeting_Link: `${window.location.origin}/meeting/${meetingId}`,
              message: 'Updated locally (server endpoint not available)',
              _localOnly: true
            };
          }
        } else {
          throw primaryError;
        }
      }
      
      // Update local state if the API call was successful
      if (response && response.data) {
        // Update upcoming meetings
        setUpcomingMeetings(prev => 
          Array.isArray(prev) ? prev.map(m => {
            const id = m.ID || m.Meeting_ID;
            if (id === meetingId || id === parseInt(meetingId)) {
              return {
                ...m,
                ...requestData,
                Updated_At: new Date().toISOString()
              };
            }
            return m;
          }) : []
        );
        
        // Update meetings history
        setMeetings(prev => 
          Array.isArray(prev) ? prev.map(m => {
            if (m.id === meetingId || m.id === parseInt(meetingId)) {
              return {
                ...m,
                ...requestData,
                Updated_At: new Date().toISOString()
              };
            }
            return m;
          }) : []
        );
        
        return { 
          success: true, 
          Meeting_ID: meetingId,
          Meeting_Link: response.data?.Meeting_Link || `${window.location.origin}/meeting/${meetingId}`,
          message: 'Meeting updated successfully',
          ...response.data
        };
      }
      
      return { success: false, message: 'Update request succeeded but no data returned' };
      
    } catch (error) {
      logError('Update meeting error:', error);
      
      // Provide more specific error messages
      let errorMessage = 'Failed to update meeting';
      let errorCode = 'UNKNOWN_ERROR';
      
      if (error.response?.status === 404) {
        // For 404 errors, still update locally and return success
        setUpcomingMeetings(prev => 
          Array.isArray(prev) ? prev.map(m => {
            const id = m.ID || m.Meeting_ID;
            if (id === meetingId || id === parseInt(meetingId)) {
              return {
                ...m,
                ...meetingData,
                Updated_At: new Date().toISOString(),
                _localUpdate: true
              };
            }
            return m;
          }) : []
        );
        
        setMeetings(prev => 
          Array.isArray(prev) ? prev.map(m => {
            if (m.id === meetingId || m.id === parseInt(meetingId)) {
              return {
                ...m,
                ...meetingData,
                Updated_At: new Date().toISOString(),
                _localUpdate: true
              };
            }
            return m;
          }) : []
        );
        
        return { 
          success: true, 
          Meeting_ID: meetingId,
          Meeting_Link: `${window.location.origin}/meeting/${meetingId}`,
          message: 'Updated locally (endpoint not available)',
          _localOnly: true
        };
      } else if (error.response?.status === 403) {
        errorMessage = 'You do not have permission to update this meeting.';
        errorCode = 'PERMISSION_DENIED';
      } else if (error.response?.status === 400) {
        errorMessage = error.response?.data?.message || 'Invalid meeting data provided.';
        errorCode = 'INVALID_DATA';
      } else if (error.response?.status === 500) {
        errorMessage = 'Server error occurred. Please try again later.';
        errorCode = 'SERVER_ERROR';
      } else if (error.code === 'NETWORK_ERROR') {
        errorMessage = 'Network error. Please check your connection.';
        errorCode = 'NETWORK_ERROR';
      } else {
        errorMessage = error.response?.data?.Error || 
                      error.response?.data?.message || 
                      error.message || 
                      errorMessage;
      }
      
      setError(errorMessage);
      
      return { 
        success: false, 
        message: errorMessage,
        errorCode,
        originalError: error.message
      };
    }
  };

  // Function to sync pending local updates when server becomes available
  const syncPendingUpdates = async () => {
    try {
      const pendingUpdates = JSON.parse(localStorage.getItem('pending_meeting_updates') || '{}');
      
      if (Object.keys(pendingUpdates).length === 0) {
        return { success: true, message: 'No pending updates to sync' };
      }
      
      logBasic('Syncing pending updates:', Object.keys(pendingUpdates));
      
      const results = {};
      for (const [meetingId, updateData] of Object.entries(pendingUpdates)) {
        try {
          const result = await updateMeeting(meetingId, updateData);
          results[meetingId] = result;
          
          if (result.success && !result._localOnly) {
            // Remove from pending updates only if it was actually synced to server
            delete pendingUpdates[meetingId];
          }
        } catch (error) {
          logError(`Failed to sync update for meeting ${meetingId}:`, error);
          results[meetingId] = { success: false, error: error.message };
        }
      }
      
      // Update localStorage with remaining pending updates
      localStorage.setItem('pending_meeting_updates', JSON.stringify(pendingUpdates));
      
      const syncedCount = Object.values(results).filter(r => r.success && !r._localOnly).length;
      const failedCount = Object.values(results).filter(r => !r.success).length;
      
      logBasic(`Sync complete: ${syncedCount} synced, ${failedCount} failed`);
      
      return {
        success: true,
        syncedCount,
        failedCount,
        results
      };
      
    } catch (error) {
      logError('Sync pending updates error:', error);
      return { success: false, message: error.message };
    }
  };

  const addUpcomingMeeting = (meeting) => {
    if (!meeting) return;
    
    logBasic('Adding meeting to upcoming list:', meeting?.Meeting_Name || meeting?.name);
    setUpcomingMeetings(prev => {
      const safeList = Array.isArray(prev) ? prev : [];
      const exists = safeList.some(m => 
        m.ID === meeting.id || 
        m.Meeting_ID === meeting.id || 
        m.ID === meeting.Meeting_ID ||
        m.Meeting_ID === meeting.Meeting_ID
      );
      
      if (!exists) {
        const newList = [meeting, ...safeList]
          .sort((a, b) => {
            const dateA = new Date(a.Started_At || a.start_time || a.Created_At);
            const dateB = new Date(b.Started_At || b.start_time || b.Created_At);
            return dateA - dateB;
          })
          .slice(0, 10);
        
        logBasic('Updated upcoming meetings list:', newList.length);
        return newList;
      }
      
      logDebug('Meeting already exists in list');
      return safeList;
    });
  };

  const refreshUpcomingMeetings = async () => {
    logBasic('Refreshing upcoming meetings...');
    await loadUpcomingMeetings();
  };

  const getMeetingHistory = async () => {
    if (!user?.id || !user?.email) {
      return { success: false, message: 'User not authenticated' };
    }
    
    try {
      setLoading(true);
      
      const response = await meetingsAPI.getMeetingHistory(user.id, user.email);
      const meetings = Array.isArray(response) ? response : [];
      
      return { success: true, meetings };
    } catch (error) {
      logError('Get meeting history error:', error);
      return { success: false, message: error.message };
    } finally {
      setLoading(false);
    }
  };

  const getScheduledMeetings = async () => {
    if (!user?.id || !user?.email) {
      return { success: false, message: 'User not authenticated' };
    }
    
    try {
      setLoading(true);
      
      const response = await meetingsAPI.getUserScheduledMeetings(user.id, user.email);
      const userMeetings = Array.isArray(response) ? response : [];
      
      return { success: true, meetings: userMeetings };
    } catch (error) {
      logError('Get scheduled meetings error:', error);
      return { success: false, message: error.response?.data?.Error || error.message };
    } finally {
      setLoading(false);
    }
  };

  const getMeeting = async (meetingId) => {
    if (!meetingId) {
      return { success: false, message: 'Meeting ID is required' };
    }
    
    try {
      logBasic('Getting meeting details:', meetingId);
      const response = await axios.get(`${API_BASE_URL}/api/meetings/get/${meetingId}`);
      logBasic('Meeting details loaded:', response.data);
      return { success: true, data: response.data };
    } catch (error) {
      logError('Get meeting error:', error);
      const errorMessage = error.response?.data?.Error || 
                          error.response?.data?.message || 
                          error.message || 
                          'Failed to get meeting details';
      return { success: false, message: errorMessage };
    }
  };

  const updateMeetingSettings = async (newSettings = {}) => {
    if (!currentMeeting?.id || !isHost) {
      return { success: false, message: 'Permission denied or meeting not available' };
    }
    
    try {
      await axios.put(`${API_BASE_URL}/api/meetings/update/${currentMeeting.id}`, {
        Is_Recording_Enabled: newSettings.recordingEnabled,
        Waiting_Room_Enabled: newSettings.waitingRoomEnabled
      });
      setMeetingSettings(prev => ({ ...prev, ...newSettings }));
      return { success: true };
    } catch (error) {
      logError('Update meeting settings error:', error);
      return { success: false, message: error.response?.data?.Error || error.message };
    }
  };

  const sendFeedback = async (rating, comments, feedbackType = 'general') => {
    if (!currentMeeting?.id) {
      return { success: false, message: 'No active meeting' };
    }
    
    try {
      await axios.post(`${API_BASE_URL}/feedback/create`, {
        meeting_id: currentMeeting.id,
        rating: rating,
        comments: comments,
        feedback_type: feedbackType
      });
      return { success: true };
    } catch (error) {
      logError('Send feedback error:', error);
      return { success: false, message: 'Failed to send feedback' };
    }
  };

  // Enhanced queue management functions
  const getQueueStatus = () => {
    return connectionQueue || queueStatus;
  };

  const isInQueue = () => {
    const queue = getQueueStatus();
    return queue?.status === 'queued';
  };

  const getQueuePosition = () => {
    const queue = getQueueStatus();
    return queue?.position || 0;
  };

  const getEstimatedWaitTime = () => {
    const queue = getQueueStatus();
    return queue?.estimated_wait || 0;
  };

  // Performance monitoring
  const getPerformanceInfo = () => {
    return {
      mode: performanceMode,
      maxParticipants,
      currentParticipants: participantCount || 0,
      participantStats,
      queueStatus: getQueueStatus()
    };
  };

  // Return comprehensive hook interface with safe fallback values
  return {
    // Core state with safe defaults
    currentMeeting,
    chatMessages: Array.isArray(chatMessages) ? chatMessages : [],
    isRecording,
    isHost,
    meetingSettings,
    loading,
    error,
    recentMeetings: Array.isArray(recentMeetings) ? recentMeetings : [],
    upcomingMeetings: Array.isArray(upcomingMeetings) ? upcomingMeetings : [],
    
    // Chat properties
    chatHistory,
    isLoadingHistory,
    chatError,
    typingUsers,
    loadChatHistory,
    sendTypingIndicator,
    clearChatMessages,
    getChatStats,
    
    // Enhanced state for backend integration
    connectionQueue,
    participantStats,
    performanceMode,
    maxParticipants,
    isCacheChatInitialized,
    cacheMessages,
    cacheChatStats,
    initializeCacheChat,
    loadCacheMessages,
    sendCacheChatMessage,
    endCacheChat,
    sendChatMessage: sendCacheChatMessage,
    messages: cacheMessages,
    
    // Meeting History properties
    meetings: Array.isArray(meetings) ? meetings : [],
    summary,
    loadMeetingHistory,
    refresh,
    filterMeetings,
    getStatistics,
    updateMeeting: updateMeetingInHistory,
    
    // Meeting management
    createInstantMeeting,
    createScheduledMeeting,
    createCalendarMeeting,
    joinMeeting,
    leaveMeeting,
    endMeeting,
    deleteMeeting,
    getMeeting,
    updateMeeting,
    syncPendingUpdates,
    
    // Data fetching
    getMeetingHistory,
    getScheduledMeetings,
    loadRecentMeetings,
    loadUpcomingMeetings,
    
    // Enhanced participant management
    loadParticipantStats,
    syncParticipants,
    recordParticipantJoin,
    recordParticipantLeave,
    
    // Additional utility functions
    addUpcomingMeeting,
    refreshUpcomingMeetings,
    
    // Settings
    updateMeetingSettings,
    
    // Feedback
    sendFeedback,
    
    // Utility
    setError,
    clearError: () => setError(null),

    // Queue management functions
    checkMeetingQueue,
    joinWithQueue,
    getQueueStatus,
    isInQueue,
    getQueuePosition,
    getEstimatedWaitTime,

    // Performance monitoring
    getPerformanceInfo,

    // LiveKit integration with safe fallbacks
    connected: connected || false,
    participantCount: participantCount || 0,
    sendChatMessage: sendChatMessageEnhanced,
    sendReaction: typeof sendReaction === 'function' ? sendReaction : () => Promise.resolve(false),
    formatMessage: livekitChatService.formatMessage,
    validateMessage: livekitChatService.validateMessage,
    cleanMessage: livekitChatService.cleanMessage,

    // Additional safe properties
    livekitRoom: room || null,
    localParticipant: localParticipant || null,
    remoteParticipants: Array.isArray(remoteParticipants) ? remoteParticipants : [],
    
    // User info with fallbacks
    currentUser: user || null,
    meetingInfo: currentMeeting || null,
    isConnected: connected || false,
    
    // Messages array (ensuring it's always an array)
    messages: Array.isArray(chatMessages) ? chatMessages : []
  };
};