// src/services/handRaiseAPI.js - Frontend Hand Raise API Integration
import axios from 'axios';
import { API_BASE_URL, TOKEN_KEY } from '../utils/constants';

// Create axios instance for hand raise APIs
const handRaiseAPI = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  }
});

// Request interceptor
handRaiseAPI.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    console.log(`🖐️ Hand Raise API: ${config.method?.toUpperCase()} ${config.url}`);
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor
handRaiseAPI.interceptors.response.use(
  (response) => {
    console.log(`✅ Hand Raise Response: ${response.config.method?.toUpperCase()} ${response.config.url}`);
    return response.data;
  },
  (error) => {
    console.error(`❌ Hand Raise Error: ${error.config?.method?.toUpperCase()} ${error.config?.url}`, error);
    return Promise.reject(error);
  }
);

export const handRaiseService = {

startMeetingHandRaise: async (meetingId) => {
  try {
    console.log('🚀 Starting hand raise system for meeting:', meetingId);
    const response = await handRaiseAPI.post('/api/cache-hand-raise/start/', {
      meeting_id: meetingId
    });
    console.log('✅ Hand raise system started:', response);
    return response;
  } catch (error) {
    console.log('ℹ️ Hand raise start request failed:', error.response?.status);
    
    // Check if it's an "already exists" error or server error
    if (error.response?.status === 409) {
      console.log('ℹ️ Hand raise system already exists');
      return { success: true, message: 'Already started' };
    } else if (error.response?.status === 500) {
      console.log('ℹ️ Server error starting hand raise - will try later');
      return { success: false, message: 'Server not ready', error: error.message };
    }
    
    throw error; // Re-throw other errors
  }
},

  // Raise or lower hand
  toggleHand: async (meetingId, userId, userName, participantIdentity, action = 'raise') => {
    try {
      console.log(`🖐️ ${action} hand:`, { meetingId, userId, userName, action });
      const response = await handRaiseAPI.post('/api/cache-hand-raise/raise/', {
        meeting_id: meetingId,
        user_id: userId,
        user_name: userName,
        participant_identity: participantIdentity,
        action: action // 'raise' or 'lower'
      });
      console.log(`✅ Hand ${action}d successfully:`, response);
      return response;
    } catch (error) {
      console.error(`❌ Failed to ${action} hand:`, error);
      throw new Error(error.response?.data?.error || `Failed to ${action} hand`);
    }
  },

  // Host acknowledges or denies a hand
  acknowledgeHand: async (meetingId, hostUserId, participantUserId, participantName, action = 'acknowledge') => {
    try {
      console.log('👨‍💼 Host acknowledging hand:', { meetingId, hostUserId, participantUserId, action });
      const response = await handRaiseAPI.post('/api/cache-hand-raise/acknowledge/', {
        meeting_id: meetingId,
        host_user_id: hostUserId,
        participant_user_id: participantUserId,
        participant_name: participantName,
        action: action // 'acknowledge' or 'deny'
      });
      console.log('✅ Hand acknowledged successfully:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to acknowledge hand:', error);
      throw new Error(error.response?.data?.error || 'Failed to acknowledge hand');
    }
  },

  // Host clears all raised hands
  clearAllHands: async (meetingId, hostUserId) => {
    try {
      console.log('🧹 Clearing all hands:', { meetingId, hostUserId });
      const response = await handRaiseAPI.post('/api/cache-hand-raise/clear-all/', {
        meeting_id: meetingId,
        host_user_id: hostUserId
      });
      console.log('✅ All hands cleared:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to clear all hands:', error);
      throw new Error(error.response?.data?.error || 'Failed to clear all hands');
    }
  },

  // Get current raised hands
  getRaisedHands: async (meetingId) => {
    try {
      console.log('📋 Getting raised hands for meeting:', meetingId);
      const response = await handRaiseAPI.get(`/api/cache-hand-raise/hands/${meetingId}/`);
      console.log('✅ Raised hands retrieved:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to get raised hands:', error);
      
      // Handle specific error cases
      if (error.response?.status === 404) {
        console.log('ℹ️ Meeting not found or not initialized yet');
        return { 
          success: false, 
          raised_hands: [], 
          note: 'Meeting not found or hand raise system not started yet'
        };
      }
      
      // For other errors, still return a safe response
      return { 
        success: false, 
        raised_hands: [], 
        error: error.message 
      };
    }
  },

  // Sync hand raise state for new participants
  syncHandRaiseState: async (meetingId, userId) => {
    try {
      console.log('🔄 Syncing hand raise state:', { meetingId, userId });
      const response = await handRaiseAPI.post('/api/cache-hand-raise/sync/', {
        meeting_id: meetingId,
        user_id: userId
      });
      console.log('✅ Hand raise state synced:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to sync hand raise state:', error);
      return { success: false, error: error.message };
    }
  },

  // Check if user has hand raised
  checkHandStatus: async (meetingId, userId) => {
    try {
      console.log('❓ Checking hand status:', { meetingId, userId });
      const response = await handRaiseAPI.get(`/api/cache-hand-raise/check/${meetingId}/${userId}/`);
      console.log('✅ Hand status checked:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to check hand status:', error);
      return { success: true, hand_raised: false, note: 'Could not check status' };
    }
  },

  // End meeting hand raise (cleanup)
  endMeetingHandRaise: async (meetingId) => {
    try {
      console.log('🔚 Ending hand raise system for meeting:', meetingId);
      const response = await handRaiseAPI.post('/api/cache-hand-raise/end/', {
        meeting_id: meetingId
      });
      console.log('✅ Hand raise system ended:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to end hand raise system:', error);
      // Don't throw error - meeting ending shouldn't fail because of this
      return { success: false, error: error.message };
    }
  },

  // Get meeting hand raise statistics
  getHandRaiseStats: async (meetingId) => {
    try {
      console.log('📊 Getting hand raise stats:', meetingId);
      const response = await handRaiseAPI.get(`/api/cache-hand-raise/stats/${meetingId}/`);
      console.log('✅ Hand raise stats retrieved:', response);
      return response;
    } catch (error) {
      console.error('❌ Failed to get hand raise stats:', error);
      return { success: false, stats: null };
    }
  }
};

export default handRaiseService;