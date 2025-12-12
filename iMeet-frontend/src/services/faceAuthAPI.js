// src/services/faceAuthAPI.js - Face Authentication API Service
import axios from 'axios';
import { API_BASE_URL } from '../utils/constants';

const faceAuthApi = axios.create({
  baseURL: API_BASE_URL,
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
faceAuthApi.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor
faceAuthApi.interceptors.response.use(
  (response) => response.data,
  (error) => {
    console.error('Face Auth API Error:', error);
    return Promise.reject(error);
  }
);
  
export const faceAuthAPI = {
  /**
   * Verify user face against stored embeddings
   * @param {string} userId - User ID
   * @param {string} imageBase64 - Base64 encoded image
   * @returns {Promise<Object>} Verification result
   */
  verifyFace: async (userId, imageBase64) => {
    try {
      console.log(`🔐 Verifying face for user ${userId}...`);
      
      const response = await faceAuthApi.post('/api/face/verify', {
        user_id: userId,
        image: imageBase64, // Send as base64 string
      });

      console.log('🔐 Face verification result:', {
        allowed: response.allowed,
        distance: response.distance,
        confidence: response.confidence,
        status: response.status,
      });

      return {
        allowed: response.allowed,
        distance: response.distance,
        confidence: response.confidence,
        status: response.status,
        message: response.message || '',
      };
    } catch (error) {
      console.error('❌ Face verification error:', error);
      
      // Return failure result instead of throwing
      return {
        allowed: false,
        distance: 999,
        confidence: 0,
        status: 'ERROR',
        message: error.message || 'Verification failed',
      };
    }
  },

  /**
   * Check if face authentication service is available
   * @returns {Promise<boolean>}
   */
  healthCheck: async () => {
    try {
      const response = await faceAuthApi.get('/health');
      return response.status === 'ok';
    } catch (error) {
      console.error('Face auth service health check failed:', error);
      return false;
    }
  },
};

export default faceAuthAPI;