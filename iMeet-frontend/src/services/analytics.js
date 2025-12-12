// src/services/analyticsAPI.js - STANDALONE Analytics API Service
import axios from 'axios';

// Get base URL from environment or use default
const ANALYTICS_BASE_URL = import.meta.env.VITE_API_BASE_URL || 
                           import.meta.env.REACT_APP_API_BASE_URL || 
                           'https://192.168.48.201:8111';

// Create a dedicated axios instance for analytics
const analyticsClient = axios.create({
  baseURL: ANALYTICS_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 30000, // 30 seconds timeout
});

// Request interceptor for adding auth tokens
analyticsClient.interceptors.request.use(
  (config) => {
    // Try to get token from localStorage (adjust key as needed)
    const token = localStorage.getItem('token') || 
                  localStorage.getItem('authToken') ||
                  localStorage.getItem('access_token');
    
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    
    // Log request for debugging
    console.log('📤 Analytics API Request:', {
      method: config.method?.toUpperCase(),
      url: config.url,
      params: config.params,
      baseURL: config.baseURL
    });
    
    return config;
  },
  (error) => {
    console.error('❌ Analytics API Request Error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for handling errors
analyticsClient.interceptors.response.use(
  (response) => {
    console.log('✅ Analytics API Response:', {
      url: response.config.url,
      status: response.status,
      dataKeys: Object.keys(response.data || {})
    });
    return response;
  },
  (error) => {
    console.error('❌ Analytics API Response Error:', {
      url: error.config?.url,
      status: error.response?.status,
      message: error.message,
      data: error.response?.data
    });
    
    // Handle specific error cases
    if (error.response?.status === 401) {
      console.warn('🔒 Unauthorized - Token may be expired');
    } else if (error.response?.status === 404) {
      console.warn('🔍 Endpoint not found:', error.config?.url);
    }
    
    return Promise.reject(error);
  }
);

// Analytics API endpoints
const analyticsAPI = {
  // ==================== COMPREHENSIVE ANALYTICS ====================
  
  /**
   * Get comprehensive analytics (participant, host, meeting data)
   */
  getComprehensiveAnalytics: (params = {}) => {
    return analyticsClient.get('/api/analytics/comprehensive', { params });
  },

  /**
   * Get participant meeting duration analytics
   */
  getParticipantDurationAnalytics: (params = {}) => {
    return analyticsClient.get('/api/analytics/participant/duration', { params });
  },

  /**
   * Get participant attendance analytics
   */
  getParticipantAttendanceAnalytics: (params = {}) => {
    return analyticsClient.get('/api/analytics/participant/attendance', { params });
  },

  /**
   * Get host meeting count analytics
   */
  getHostMeetingCountAnalytics: (params = {}) => {
    return analyticsClient.get('/api/analytics/host/meeting-counts', { params });
  },

  // ==================== HOST ANALYTICS ====================

  /**
   * Get host dashboard overview
   */
  getHostDashboardOverview: (params = {}) => {
    return analyticsClient.get('/api/analytics/host/overview', { params });
  },

  /**
   * Get host meeting reports (uses comprehensive analytics)
   */
  getHostMeetingReports: (params = {}) => {
    return analyticsClient.get('/api/analytics/comprehensive', {
      params: {
        ...params,
        analytics_type: 'meeting'
      }
    });
  },

  /**
   * Get host engagement distribution (uses comprehensive analytics)
   */
  getHostEngagementDistribution: (params = {}) => {
    return analyticsClient.get('/api/analytics/comprehensive', {
      params: {
        ...params,
        analytics_type: 'host'
      }
    });
  },

  /**
   * Get host meeting trends (uses comprehensive analytics)
   */
  getHostMeetingTrends: (params = {}) => {
    return analyticsClient.get('/api/analytics/host/meeting-counts', { params });
  },

  // ==================== PARTICIPANT ANALYTICS ====================

  /**
   * Get participant personal report
   */
  getParticipantPersonalReport: (userId, params = {}) => {
    return analyticsClient.get('/api/analytics/comprehensive', {
      params: {
        user_id: userId,
        analytics_type: 'participant',
        ...params
      }
    });
  },

  /**
   * Get participant attendance records
   */
  getParticipantAttendance: (userId, params = {}) => {
    return analyticsClient.get('/api/analytics/participant/attendance', {
      params: {
        user_id: userId,
        ...params
      }
    });
  },

  /**
   * Get participant engagement metrics
   */
  getParticipantEngagement: (userId, params = {}) => {
    return analyticsClient.get('/api/analytics/participant/duration', {
      params: {
        user_id: userId,
        ...params
      }
    });
  },

  // ==================== REPORT GENERATION ====================

  /**
   * Generate participant PDF report
   */
  generateParticipantReportPDF: (params = {}) => {
    return analyticsClient.get('/api/reports/participant/pdf', {
      params,
      responseType: 'blob'
    });
  },

  /**
   * Generate host PDF report
   */
  generateHostReportPDF: (params = {}) => {
    return analyticsClient.get('/api/reports/host/pdf', {
      params,
      responseType: 'blob'
    });
  },

  /**
   * Get participant report preview (JSON data)
   */
  getParticipantReportPreview: (params = {}) => {
    return analyticsClient.get('/api/reports/participant/preview', { params });
  },

  /**
   * Get host report preview (JSON data)
   */
  getHostReportPreview: (params = {}) => {
    return analyticsClient.get('/api/reports/host/preview', { params });
  },

  // ==================== UTILITY FUNCTIONS ====================

  /**
   * Download PDF report
   */
  downloadPDFReport: (blob, filename) => {
    try {
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      console.log('✅ PDF downloaded:', filename);
    } catch (error) {
      console.error('❌ Error downloading PDF:', error);
      throw error;
    }
  },

  /**
   * Get user profile (uses comprehensive analytics)
   */
  getUserProfile: () => {
    return analyticsClient.get('/api/analytics/comprehensive', {
      params: {
        analytics_type: 'all',
        timeframe: '1year'
      }
    });
  },

  /**
   * Get user stats (uses comprehensive analytics)
   */
  getUserStats: (params = {}) => {
    return analyticsClient.get('/api/analytics/comprehensive', {
      params: {
        ...params,
        analytics_type: 'all'
      }
    });
  }
};

// Export the analytics API
export { analyticsAPI, analyticsClient };

// Export as default for convenience
export default analyticsAPI;