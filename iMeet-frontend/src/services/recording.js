// src/services/api.js - FIXED: Authentication and endpoint issues
import axios from 'axios';
import { API_BASE_URL, TOKEN_KEY, REFRESH_TOKEN_KEY } from '../utils/constants';

// Development mode check
const isDevelopment = process.env.NODE_ENV === 'development' || import.meta.env.MODE === 'development';

// Create axios instance
const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 300000,
  headers: {
    'Content-Type': 'application/json',
  },
  ...(isDevelopment && {
    validateStatus: function (status) {
      return status >= 200 && status < 600;
    }
  })
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    
    if (isDevelopment) {
      console.log(`📤 API Request: ${config.method?.toUpperCase()} ${config.url}`);
    }
    
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor with enhanced error handling
api.interceptors.response.use(
  (response) => {
    if (isDevelopment) {
      console.log(`📥 API Response: ${response.config.method?.toUpperCase()} ${response.config.url} - Status: ${response.status}`);
    }
    return response.data;
  },
  async (error) => {
    const originalRequest = error.config;
    
    // Handle 401 Unauthorized with token refresh
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        const refreshToken = localStorage.getItem(REFRESH_TOKEN_KEY);
        if (refreshToken) {
          const response = await api.post('/auth/refresh/', {
            refresh: refreshToken
          });
          
          const { access } = response.data;
          localStorage.setItem(TOKEN_KEY, access);
          
          return api(originalRequest);
        }
      } catch (refreshError) {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(REFRESH_TOKEN_KEY);
        window.location.href = '/login';
        return Promise.reject(refreshError);
      }
    }

    if (isDevelopment && error.response) {
      console.error(`❌ API Error: ${error.config?.method?.toUpperCase()} ${error.config?.url}`);
      console.error(`Status: ${error.response.status}`);
      console.error('Response:', error.response.data);
    }

    return Promise.reject(error);
  }
);

// Recording APIs - FIXED to match your Django backend exactly
export const recordingsAPI = {
  // Upload video
  uploadVideo: async (formData, options = {}) => {
    try {
      const config = {
        headers: { 
          'Content-Type': 'multipart/form-data' 
        },
        timeout: 600000,
        ...options
      };

      const response = await api.post('/api/videos/upload', formData, config);
      return response;
    } catch (error) {
      console.error('❌ Failed to upload video:', error);
      throw new Error(error.response?.data?.Error || error.message || 'Failed to upload video');
    }
  },
  
  // Get all recordings - UPDATED to match your Django endpoint
  getRecordings: async (params = {}) => {
    try {
      console.log('🔍 API: Getting recordings with params:', params);
      
      const queryParams = {
        page: params.page || 1,
        limit: params.limit || 50,
        ...params,
        email: params.user_email || params.email || '',
        user_id: params.user_id || '',
      };
      
      // UPDATED: Use your Django endpoint
      const response = await api.get('/api/videos/lists', { 
        params: queryParams 
      });
      
      console.log('✅ API: Recordings fetched successfully:', response);
      
      return {
        videos: response.videos || response.data || response,
        total: response.total || 0,
        page: response.page || 1,
        pages: response.pages || 1
      };
    } catch (error) {
      console.error('❌ API: Failed to get recordings:', error);
      throw new Error(error.response?.data?.Error || error.message || 'Failed to fetch recordings');
    }
  },

  // FIXED: Get single recording with proper auth parameters
  getRecording: async (id) => {
    try {
      console.log('🔍 API: Getting recording:', id);
      
      // FIXED: Add user credentials as query parameters (required by Django backend)
      const userData = JSON.parse(localStorage.getItem('user')) || {};
      const userId = userData.id || localStorage.getItem('user_id') || '';
      const userEmail = userData.email || localStorage.getItem('user_email') || '';
      
      if (!userId || !userEmail) {
        throw new Error('Missing user authentication data');
      }
      
      const response = await api.get(`/api/videos/${id}`, {
        params: {
          email: userEmail,
          user_id: userId
        }
      });
      
      console.log('✅ API: Recording fetched:', response);
      return response;
    } catch (error) {
      console.error(`❌ API: Failed to get recording ${id}:`, error);
      throw new Error(error.response?.data?.Error || error.message || 'Failed to fetch recording');
    }
  },

  // Get video stream URL
  getVideoStreamUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn('⚠️ Missing required video ID or user info for stream URL');
      return '';
    }
    return `${API_BASE_URL}/api/videos/stream/${id}?email=${encodeURIComponent(email)}&user_id=${userId}`;
  },

  // Get thumbnail URL
  getThumbnailUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn('⚠️ Missing thumbnail ID or user info');
      return '';
    }
    return `${API_BASE_URL}/api/recordings/${id}/thumbnail?email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
  },

  // FIXED: Check if subtitles are available - with proper auth
  checkSubtitlesAvailable: async (recordingId) => {
    try {
      console.log('🔍 API: Checking subtitles availability for:', recordingId);
      
      // Get the video document to check subtitles field
      const response = await recordingsAPI.getRecording(recordingId);
      const video = response.data || response;
      
      // Check if subtitles exist in the document
      const subtitles = video.subtitles || {};
      const hasSubtitles = Object.keys(subtitles).length > 0 && 
                          Object.values(subtitles).some(url => url && url.trim() !== '');
      
      console.log('✅ API: Subtitles availability checked:', hasSubtitles);
      console.log('📝 Available subtitles:', subtitles);
      
      return hasSubtitles;
    } catch (error) {
      console.error('❌ API: Failed to check subtitles availability:', error);
      return false;
    }
  },

  // FIXED: Get subtitles - using your Django endpoint with auth
  getSubtitles: async (recordingId, language = 'en') => {
    try {
      console.log('🎬 API: Getting subtitles for recording:', recordingId, 'language:', language);
      
      // FIXED: Add user credentials as query parameters
      const userData = JSON.parse(localStorage.getItem('user')) || {};
      const userId = userData.id || localStorage.getItem('user_id') || '';
      const userEmail = userData.email || localStorage.getItem('user_email') || '';
      
      if (!userId || !userEmail) {
        throw new Error('Missing user authentication data');
      }
      
      // Use your Django subtitle endpoint: /api/videos/<id>/subtitles/<lang>
      const response = await api.get(`/api/videos/${recordingId}/subtitles/${language}`, {
        params: {
          email: userEmail,
          user_id: userId
        },
        responseType: 'text' // Get as text content
      });
      
      console.log('✅ API: Subtitles retrieved successfully');
      
      return {
        subtitles: response, // This will be the SRT/WebVTT content
        language: language,
        format: 'srt' // Your backend provides SRT format
      };
    } catch (error) {
      console.error('❌ API: Failed to get subtitles:', error);
      throw new Error(error.response?.data?.Error || error.message || 'Failed to get subtitles');
    }
  },

  // FIXED: Download subtitles - using your Django endpoint with auth
  downloadSubtitles: async (recordingId, format = 'srt', fileName, language = 'en') => {
    try {
      console.log('📥 API: Downloading subtitles for:', recordingId);
      
      // FIXED: Add user credentials as query parameters
      const userData = JSON.parse(localStorage.getItem('user')) || {};
      const userId = userData.id || localStorage.getItem('user_id') || '';
      const userEmail = userData.email || localStorage.getItem('user_email') || '';
      
      if (!userId || !userEmail) {
        throw new Error('Missing user authentication data');
      }
      
      // Use your Django subtitle endpoint with download
      const response = await api.get(`/api/videos/${recordingId}/subtitles/${language}`, {
        params: {
          email: userEmail,
          user_id: userId
        },
        responseType: 'blob'
      });
      
      // Create download
      const url = window.URL.createObjectURL(new Blob([response]));
      const link = document.createElement('a');
      link.href = url;
      link.download = fileName || `subtitles-${recordingId}-${language}.${format}`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      console.log('✅ API: Subtitles download initiated');
      
      return response;
    } catch (error) {
      console.error('❌ API: Failed to download subtitles:', error);
      throw new Error(error.response?.data?.Error || error.message || 'Failed to download subtitles');
    }
  },

  // Generate subtitles - this would trigger your backend processing
  generateSubtitles: async (recordingId, options = {}) => {
    try {
      console.log('🎬 API: Generating subtitles for recording:', recordingId);
      
      // Your backend automatically generates subtitles during video processing
      // This might trigger re-processing or return existing subtitles
      const response = await api.post(`/api/videos/${recordingId}/generate-subtitles`, {
        language: options.language || 'en',
        format: options.format || 'srt',
        accuracy: options.accuracy || 'high',
        ...options
      });
      
      console.log('✅ API: Subtitles generated successfully');
      return response;
    } catch (error) {
      console.error('❌ API: Failed to generate subtitles:', error);
      throw new Error(error.response?.data?.Error || error.message || 'Failed to generate subtitles');
    }
  },

  // Update recording
  updateRecording: async (id, updateData) => {
    try {
      const response = await api.put(`/api/videos/update/${id}`, updateData);
      if (response.Message && response.data) {
        return response.data;
      }
      return response;
    } catch (error) {
      console.error(`❌ Failed to update recording ${id}:`, error);
      throw new Error(error.response?.data?.Error || 'Failed to update recording');
    }
  },

  // Delete recording with host permission check
  deleteRecording: async (id, userCredentials = {}) => {
    try {
      console.log('🗑️ API: Deleting recording:', id);
      console.log('🔍 API: User credentials:', userCredentials);
      
      const params = new URLSearchParams();
      
      if (userCredentials.user_id) {
        params.append('user_id', userCredentials.user_id);
      }
      
      if (userCredentials.email) {
        params.append('email', userCredentials.email);
      }
      
      const deleteUrl = `/api/videos/remove/${id}${params.toString() ? `?${params.toString()}` : ''}`;
      
      console.log('📤 API: Delete URL:', deleteUrl);
      
      const response = await api.delete(deleteUrl);
      
      console.log('✅ API: Recording deleted successfully:', response);
      
      return response;
    } catch (error) {
      console.error(`❌ API: Failed to delete recording ${id}:`, error);
      
      if (error.response?.status === 403) {
        throw new Error('Permission denied: Only the meeting host can delete this recording');
      } else if (error.response?.status === 404) {
        throw new Error('Recording not found');
      } else if (error.response?.status === 400) {
        throw new Error('Bad request: ' + (error.response?.data?.Error || 'Invalid request'));
      } else {
        throw new Error(error.response?.data?.Error || error.message || 'Failed to delete recording');
      }
    }
  },

  // Get document (transcript, summary, etc.)
  getDocument: async (id, docType, email, userId, action = 'download') => {
    try {
      if (!id || !email || !userId) {
        throw new Error('Missing required parameters: id, email, or userId');
      }

      const url = `/api/videos/doc/${id}/${docType}?action=${action}&email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
      
      const response = await axios.get(`${API_BASE_URL}${url}`, {
        responseType: action === 'download' ? 'blob' : 'text',
        headers: {
          'Authorization': `Bearer ${localStorage.getItem(TOKEN_KEY)}`
        }
      });
      return response.data;
    } catch (error) {
      console.error(`❌ Failed to get ${docType} document for ${id}:`, error);
      throw new Error(error.response?.data?.Error || `Failed to get ${docType} document`);
    }
  },

  // Download transcript
  downloadTranscript: async (id, email, userId, fileName) => {
    try {
      if (!id || !email || !userId) {
        throw new Error('Missing required parameters: id, email, or userId');
      }

      const url = `/api/videos/doc/${id}/transcript?action=download&email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
      
      const response = await axios.get(`${API_BASE_URL}${url}`, {
        responseType: 'blob',
        headers: {
          'Authorization': `Bearer ${localStorage.getItem(TOKEN_KEY)}`
        }
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = fileName || `transcript_${id}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return response.data;
    } catch (error) {
      console.error(`❌ Failed to download transcript for ${id}:`, error);
      throw new Error(error.response?.data?.Error || 'Failed to download transcript');
    }
  },

  // Download summary
  downloadSummary: async (id, email, userId, fileName) => {
    try {
      if (!id || !email || !userId) {
        throw new Error('Missing required parameters: id, email, or userId');
      }

      const url = `/api/videos/doc/${id}/summary?action=download&email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
      
      const response = await axios.get(`${API_BASE_URL}${url}`, {
        responseType: 'blob',
        headers: {
          'Authorization': `Bearer ${localStorage.getItem(TOKEN_KEY)}`
        }
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = fileName || `summary_${id}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return response.data;
    } catch (error) {
      console.error(`❌ Failed to download summary for ${id}:`, error);
      throw new Error(error.response?.data?.Error || 'Failed to download summary');
    }
  },

  // View transcript URL
  viewTranscript: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn('⚠️ Missing required parameters for transcript view');
      return '';
    }
    return `${API_BASE_URL}/api/videos/doc/${id}/transcript?action=view&email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
  },

  // View summary URL
  viewSummary: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn('⚠️ Missing required parameters for summary view');
      return '';
    }
    return `${API_BASE_URL}/api/videos/doc/${id}/summary?action=view&email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
  },

  // Get mindmap URL
  getMindmapUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn('⚠️ Missing required parameters for mindmap view');
      return '';
    }
    return `${API_BASE_URL}/api/videos/${id}/mindmap?email=${encodeURIComponent(email)}&user_id=${encodeURIComponent(userId)}`;
  }
};

// FIXED: Get available subtitle languages from your Django backend
export const getAvailableSubtitleLanguages = async (recordingId) => {
  try {
    console.log('🌐 Getting available subtitle languages for:', recordingId);
    
    const recording = await recordingsAPI.getRecording(recordingId);
    const video = recording.data || recording;
    const subtitles = video.subtitles || {};
    
    // Your backend stores subtitles as: { "en": "url", "hi": "url", "te": "url" }
    const availableLanguages = Object.keys(subtitles).filter(lang => 
      subtitles[lang] && subtitles[lang].trim() !== ''
    );
    
    const languageMap = {
      'en': 'English',
      'hi': 'Hindi', 
      'te': 'Telugu'
    };
    
    const result = availableLanguages.map(lang => ({
      code: lang,
      name: languageMap[lang] || lang.toUpperCase(),
      url: subtitles[lang]
    }));
    
    console.log('✅ Available subtitle languages:', result);
    return result;
  } catch (error) {
    console.error('❌ Failed to get available subtitle languages:', error);
    return [];
  }
};

// Authentication APIs
export const authAPI = {
  login: (credentials) => api.post('/api/auth/login', credentials),
  register: (userData) => api.post('/api/auth/register', userData),
  logout: () => api.post('/api/user/logout'),
  forgotPassword: (email) => api.post('/api/auth/forgot-password', { email }),
  resetPassword: (token, password) => api.post('/auth/reset-password/', { token, password }),
  verifyEmail: (token) => api.post('/api/user/validate', { token }),
  refreshToken: (refresh) => api.post('/auth/refresh/', { refresh }),
  getProfile: () => api.get('/auth/profile/'),
  updateProfile: (data) => api.patch('/auth/profile/', data),
  changePassword: (data) => api.post('/auth/change-password/', data)
};

// Meetings APIs
export const meetingsAPI = {
  checkConnectionQueue: async (meetingId, userId) => {
    try {
      console.log('🚦 Checking connection queue:', { meetingId, userId });
      const response = await api.get(`/api/meetings/check-queue/${meetingId}/`, {
        params: { user_id: userId }
      });
      console.log('✅ Queue status:', response);
      return response;
    } catch (error) {
      console.error('❌ Queue check failed:', error);
      throw new Error(error.response?.data?.error || error.message || 'Failed to check connection queue');
    }
  },

  joinMeetingWithQueue: async (meetingData) => {
    try {
      console.log('🚀 Joining meeting with queue management:', meetingData);
      const response = await api.post('/api/meetings/join-with-queue/', {
        meeting_id: meetingData.meetingId || meetingData.meeting_id,
        user_id: meetingData.userId || meetingData.user_id,
        meetingId: meetingData.meetingId || meetingData.meeting_id,
        userId: meetingData.userId || meetingData.user_id
      });
      console.log('✅ Queue join response:', response);
      return response;
    } catch (error) {
      console.error('❌ Queue join failed:', error);
      throw new Error(error.response?.data?.error || error.message || 'Failed to join meeting queue');
    }
  },
// Fixed MeetingsAPI Recording Functions for api.js

// Replace the existing startMeetingRecording function in meetingsAPI:
startMeetingRecording: async (meetingId, settings = {}) => {
  try {
    console.log('🔴 API: Starting recording for meeting:', meetingId);
    console.log('🔴 API: Recording settings:', settings);
    
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    // Ensure required fields are present
    const requestData = {
      recording_type: 'server',
      quality: 'hd',
      include_audio: true,
      include_video: true,
      layout: 'grid',
      ...settings
    };

    console.log('🔴 API: Sending request to:', `/api/meetings/${meetingId}/start-recording`);
    console.log('🔴 API: Request data:', requestData);

    const response = await api.post(`/api/meetings/${meetingId}/start-recording`, requestData);
    
    console.log('✅ API: Start recording response:', response);

    // Your Django backend might return different response structures
    // Handle various success response formats
    if (response) {
      // Check for explicit error first
      if (response.error || response.Error) {
        throw new Error(response.error || response.Error);
      }

      // Check for explicit success
      if (response.success === true) {
        return {
          success: true,
          recording_id: response.recording_id || response.id,
          message: response.message || 'Recording started successfully',
          ...response
        };
      }

      // Check for recording_id or similar success indicators
      if (response.recording_id || response.id || response.message) {
        return {
          success: true,
          recording_id: response.recording_id || response.id,
          message: response.message || 'Recording started successfully',
          ...response
        };
      }

      // If no clear error but also no clear success, assume success
      console.log('✅ API: Assuming success based on non-error response');
      return {
        success: true,
        message: 'Recording started successfully',
        ...response
      };
    }

    throw new Error('Empty response from recording service');
    
  } catch (error) {
    console.error('❌ API: Start recording failed:', error);
    
    // Enhanced error handling for different HTTP status codes
    if (error.response) {
      const status = error.response.status;
      const data = error.response.data;
      
      console.error(`❌ API: HTTP ${status} error:`, data);
      
      let errorMessage = 'Failed to start recording';
      
      switch (status) {
        case 400:
          errorMessage = data?.Error || data?.error || 'Invalid recording settings';
          break;
        case 401:
          errorMessage = 'Authentication required - please log in again';
          break;
        case 403:
          errorMessage = 'Permission denied - only hosts can start recording';
          break;
        case 404:
          errorMessage = 'Meeting not found';
          break;
        case 409:
          errorMessage = 'Recording already in progress';
          break;
        case 500:
          errorMessage = 'Recording service unavailable - please try again';
          break;
        default:
          errorMessage = data?.Error || data?.error || data?.message || 'Recording service error';
      }
      
      throw new Error(errorMessage);
    } else if (error.request) {
      throw new Error('Network error - please check your connection');
    } else {
      throw new Error(error.message || 'Failed to start recording');
    }
  }
},

// Replace the existing stopMeetingRecording function in meetingsAPI:
stopMeetingRecording: async (meetingId) => {
  try {
    console.log('⏹️ API: Stopping recording for meeting:', meetingId);
    
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    console.log('⏹️ API: Sending request to:', `/api/meetings/${meetingId}/stop-recording`);

    const response = await api.post(`/api/meetings/${meetingId}/stop-recording`);
    
    console.log('✅ API: Stop recording response:', response);

    // Handle various response formats
    if (response) {
      // Check for explicit error first
      if (response.error || response.Error) {
        throw new Error(response.error || response.Error);
      }

      // Check for explicit success
      if (response.success === true) {
        return {
          success: true,
          video_url: response.video_url,
          recording_id: response.recording_id || response.id,
          file_size: response.file_size,
          duration: response.duration,
          message: response.message || 'Recording stopped successfully',
          ...response
        };
      }

      // Check for video_url or other success indicators
      if (response.video_url || response.recording_id || response.id || response.message) {
        return {
          success: true,
          video_url: response.video_url,
          recording_id: response.recording_id || response.id,
          file_size: response.file_size,
          duration: response.duration,
          message: response.message || 'Recording stopped successfully',
          ...response
        };
      }

      // If no clear error but also no clear success, assume success
      console.log('✅ API: Assuming success based on non-error response');
      return {
        success: true,
        message: 'Recording stopped successfully',
        ...response
      };
    }

    throw new Error('Empty response from recording service');
    
  } catch (error) {
    console.error('❌ API: Stop recording failed:', error);
    
    // Enhanced error handling
    if (error.response) {
      const status = error.response.status;
      const data = error.response.data;
      
      console.error(`❌ API: HTTP ${status} error:`, data);
      
      let errorMessage = 'Failed to stop recording';
      
      switch (status) {
        case 400:
          errorMessage = data?.Error || data?.error || 'No active recording found';
          break;
        case 401:
          errorMessage = 'Authentication required - please log in again';
          break;
        case 403:
          errorMessage = 'Permission denied - only hosts can stop recording';
          break;
        case 404:
          errorMessage = 'Meeting or recording not found';
          break;
        case 500:
          errorMessage = 'Recording service error - recording may have stopped';
          break;
        default:
          errorMessage = data?.Error || data?.error || data?.message || 'Recording service error';
      }
      
      throw new Error(errorMessage);
    } else if (error.request) {
      throw new Error('Network error - please check your connection');
    } else {
      throw new Error(error.message || 'Failed to stop recording');
    }
  }
},

// Add a new function to check recording status:
getRecordingStatus: async (meetingId) => {
  try {
    console.log('🔍 API: Checking recording status for meeting:', meetingId);
    
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    const response = await api.get(`/api/meetings/${meetingId}/recording-status`);
    
    console.log('📊 API: Recording status response:', response);
    
    return {
      success: true,
      is_recording: response.is_recording || false,
      recording_method: response.recording_method || null,
      start_time: response.start_time || null,
      duration: response.duration || 0,
      recording_id: response.recording_id || null,
      ...response
    };
    
  } catch (error) {
    console.error('❌ API: Failed to check recording status:', error);
    
    // Don't throw error for status check - return default values
    return {
      success: false,
      is_recording: false,
      recording_method: null,
      start_time: null,
      duration: 0,
      recording_id: null,
      error: error.message
    };
  }
},

// Add a function to get meeting recordings:
getMeetingRecordings: async (meetingId) => {
  try {
    console.log('📋 API: Getting recordings for meeting:', meetingId);
    
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    const response = await api.get(`/api/meetings/${meetingId}/recordings`);
    
    console.log('✅ API: Meeting recordings response:', response);
    
    return {
      success: true,
      recordings: response.recordings || response.videos || [],
      total: response.total || 0,
      ...response
    };
    
  } catch (error) {
    console.error('❌ API: Failed to get meeting recordings:', error);
    throw new Error(error.response?.data?.Error || error.message || 'Failed to get meeting recordings');
  }
},

  createMeeting: (data) => api.post('/api/meetings/create', {
    Meeting_Name: data.name || data.Meeting_Name,
    Host_ID: data.host_id || data.Host_ID,
    Meeting_Type: data.type || data.Meeting_Type,
    Status: data.status || 'active',
    Is_Recording_Enabled: data.recordingEnabled || false,
    Waiting_Room_Enabled: data.waitingRoomEnabled || false,
    Started_At: data.startTime || data.Started_At,
    Ended_At: data.endTime || data.Ended_At
  }),
  
  getMeeting: (id) => {
    console.log('🔍 API: Getting meeting:', id);
    return api.get(`/api/meetings/get/${id}`);
  },
  
  updateMeeting: (id, data) => {
    console.log('🔄 API: Updating meeting:', id);
    const requestData = {
      ...data,
      description: data.description || '',
      location: data.location || '',
      Meeting_Type: data.Meeting_Type || 'ScheduleMeeting'
    };
    
    return api.put(`/api/meetings/update/${id}`, requestData);
  },
  
  deleteMeeting: (id) => {
    console.log('🗑️ API: Deleting meeting:', id);
    return api.delete(`/api/meetings/delete/${id}`);
  },
  
  getMeetings: (params) => api.get('/api/meetings/list', { params }),
  
  getUserScheduledMeetings: (userId, userEmail) => {
    console.log('🔍 API: Getting user scheduled meetings for:', { userId, userEmail });
    return api.get('/api/meetings/user-schedule-meetings', {
      params: {
        user_id: userId,
        user_email: userEmail
      }
    });
  },
  
  createInstantMeeting: (data) => api.post('/api/meetings/instant-meeting', {
    Meeting_Name: data.name || data.Meeting_Name || 'Instant Meeting',
    Host_ID: data.host_id || data.Host_ID,
    Meeting_Type: 'InstantMeeting',
    Status: data.status || 'active',
    Is_Recording_Enabled: data.recordingEnabled || false,
    Waiting_Room_Enabled: data.waitingRoomEnabled || false
  }),
  
  createScheduledMeeting: (data) => {
    console.log('📤 API: Sending scheduled meeting data:', data);
    if (data.participants && Array.isArray(data.participants)) {
      const emails = data.participants.map(p => p.email).filter(e => e);
      data.email = emails.join(',');
    }
    return api.post('/api/meetings/schedule-meeting', data);
  },
  
  getScheduledMeetings: () => {
    console.log('🔍 API: Getting scheduled meetings');
    return api.get('/api/meetings/schedule-meetings');
  },
  
  createCalendarMeeting: (data) => api.post('/api/meetings/calendar-meeting', {
    Meeting_Name: data.title || data.Meeting_Name,
    Host_ID: data.host_id || data.Host_ID,
    Meeting_Type: 'CalendarMeeting',
    Status: 'scheduled',
    Started_At: data.startTime || data.Started_At,
    Ended_At: data.endTime || data.Ended_At,
    Is_Recording_Enabled: data.recordingEnabled || false,
    Waiting_Room_Enabled: data.waitingRoomEnabled || false,
    Title: data.title,
    Location: data.location,
    Description: data.description,
    Organizer: data.organizer,
    GuestEmails: data.guestEmails,
    ReminderMinutes: data.reminderMinutes
  }),
  
  joinMeeting: (id, data) => api.post(`/meetings/${id}/join/`, data),
  leaveMeeting: (id) => api.post(`/meetings/${id}/leave/`),
  endMeeting: (id) => api.post(`/meetings/${id}/end/`),
  
  startRecording: (id, settings) => api.post(`/api/meetings/${id}/start-recording`, settings || {}),
  stopRecording: (id) => api.post(`/api/meetings/${id}/stop-recording`),
  
  allowFromWaitingRoom: (id, data) => api.post(`/api/meetings/${id}/allow-from-waiting-room`, data || {}),
  
  updateMeetingSettings: (id, settings) => api.patch(`/meetings/${id}/settings/`, settings),
  toggleWaitingRoom: (id, enabled) => api.patch(`/meetings/${id}/waiting-room/`, { enabled }),
  toggleRecording: (id, enabled) => api.patch(`/meetings/${id}/recording/`, { enabled })
};

// Error handling utility
export const handleAPIError = (error) => {
  console.error('API Error:', error);
  
  if (error.response) {
    const { status, data } = error.response;
    
    switch (status) {
      case 400:
        return `Bad Request: ${data?.error || data?.Error || data?.message || 'Invalid request'}`;
      case 401:
        return 'Unauthorized: Please log in again';
      case 403:
        return 'Forbidden: You do not have permission to perform this action';
      case 404:
        return 'Not Found: The requested resource was not found';
      case 409:
        return `Conflict: ${data?.error || data?.Error || 'Resource already exists'}`;
      case 429:
        return `Rate Limited: ${data?.error || data?.Error || 'Too many requests, please try again later'}`;
      case 500:
        return `Server Error: ${data?.error || data?.Error || 'Please try again later'}`;
      case 503:
        return `Service Unavailable: ${data?.error || data?.Error || 'Service temporarily unavailable'}`;
      default:
        return `Error ${status}: ${data?.error || data?.Error || data?.message || 'Something went wrong'}`;
    }
  } else if (error.request) {
    return 'Network Error: Please check your internet connection';
  } else {
    return `Error: ${error.message || 'Something went wrong'}`;
  }
};

export default api;