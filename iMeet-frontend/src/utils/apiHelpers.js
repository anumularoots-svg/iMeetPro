// src/utils/apiHelpers.js
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'https://192.168.48.201:8111/api';

// Create axios instance
export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for auth token
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('accessToken');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('accessToken');
      window.location.href = '/auth';
    }
    return Promise.reject(error);
  }
);

// API helper functions
export const apiRequest = {
  get: (url, params = {}) => apiClient.get(url, { params }),
  post: (url, data) => apiClient.post(url, data),
  put: (url, data) => apiClient.put(url, data),
  delete: (url) => apiClient.delete(url),
  patch: (url, data) => apiClient.patch(url, data),
};

// User API calls
export const userAPI = {
  register: (userData) => apiRequest.post('/users/register/', userData),
  login: (credentials) => apiRequest.post('/users/login/', credentials),
  getProfile: () => apiRequest.get('/users/profile/'),
  updateProfile: (data) => apiRequest.put('/users/profile/', data),
  forgotPassword: (email) => apiRequest.post('/users/forgot-password/', { email }),
  resetPassword: (data) => apiRequest.post('/users/reset-password/', data),
};

// Meeting API calls
export const meetingAPI = {
  createInstant: (data) => apiRequest.post('/meetings/instant-meeting/', data),
  createScheduled: (data) => apiRequest.post('/meetings/schedule-meeting/', data),
  createCalendar: (data) => apiRequest.post('/meetings/calendar-meeting/', data),
  getMeeting: (id) => apiRequest.get(`/meetings/${id}/`),
  joinMeeting: (id, data) => apiRequest.post(`/meetings/${id}/join/`, data),
  getMeetings: (params) => apiRequest.get('/meetings/', params),
  deleteMeeting: (id) => apiRequest.delete(`/meetings/${id}/`),
};