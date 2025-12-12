// src/services/api.js - UPDATED with AI Attendance Integration
import axios from "axios";
import { API_BASE_URL, TOKEN_KEY, REFRESH_TOKEN_KEY } from "../utils/constants";
import { attendanceAPI } from "./attendanceAPI";

// Development mode check
const isDevelopment =
  process.env.NODE_ENV === "development" ||
  import.meta.env.MODE === "development";

// Create axios instance
const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 300000,
  headers: {
    "Content-Type": "application/json",
  },
  ...(isDevelopment && {
    validateStatus: function (status) {
      return status >= 200 && status < 600;
    },
  }),
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }

    if (isDevelopment) {
      console.log(
        `📤 API Request: ${config.method?.toUpperCase()} ${config.url}`
      );
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
      console.log(
        `📥 API Response: ${response.config.method?.toUpperCase()} ${
          response.config.url
        } - Status: ${response.status}`
      );
    }
    return response.data;
  },
  async (error) => {
    const originalRequest = error.config;

    // Special handling for SSL certificate errors
    if (
      error.code === "ERR_CERT_AUTHORITY_INVALID" ||
      error.code === "UNABLE_TO_VERIFY_LEAF_SIGNATURE" ||
      error.code === "ERR_TLS_CERT_ALTNAME_INVALID"
    ) {
      console.error("🔒 SSL Certificate Error Detected!");
      console.error("Please follow these steps:");
      console.error("1. Open a new browser tab");
      console.error(`2. Navigate to: ${API_BASE_URL}`);
      console.error(
        '3. Click "Advanced" and proceed to accept the certificate'
      );
      console.error("4. Come back and refresh this page");

      if (isDevelopment) {
        error.message =
          "SSL Certificate Error - Please accept the certificate in your browser (see console for instructions)";
      }
    }

    // Handle network errors
    if (error.code === "ERR_NETWORK") {
      console.error("🌐 Network Error:", error.message);

      if (API_BASE_URL.startsWith("https://")) {
        console.warn(
          "💡 This might be an SSL certificate issue. Try the following:"
        );
        console.warn(`1. Open ${API_BASE_URL} directly in your browser`);
        console.warn("2. Accept any security warnings");
        console.warn("3. Refresh this application");
      }
    }

    // Handle 401 Unauthorized with token refresh
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        const refreshToken = localStorage.getItem(REFRESH_TOKEN_KEY);
        if (refreshToken) {
          const response = await api.post("/auth/refresh/", {
            refresh: refreshToken,
          });

          const { access } = response.data;
          localStorage.setItem(TOKEN_KEY, access);

          return api(originalRequest);
        }
      } catch (refreshError) {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(REFRESH_TOKEN_KEY);
        window.location.href = "/login";
        return Promise.reject(refreshError);
      }
    }

    if (isDevelopment && error.response) {
      console.error(
        `❌ API Error: ${error.config?.method?.toUpperCase()} ${
          error.config?.url
        }`
      );
      console.error(`Status: ${error.response.status}`);
      console.error("Response:", error.response.data);
    }

    return Promise.reject(error);
  }
);

// Authentication APIs
export const authAPI = {
  login: (credentials) => api.post("/api/auth/login", credentials),
  register: (userData) => api.post("/api/auth/register", userData),
  logout: () => api.post("/api/user/logout"),
  forgotPassword: (email) => api.post("/api/auth/forgot-password", { email }),
  resetPassword: (token, password) =>
    api.post("/auth/reset-password/", { token, password }),
  verifyEmail: (token) => api.post("/api/user/validate", { token }),
  refreshToken: (refresh) => api.post("/auth/refresh/", { refresh }),
  getProfile: () => api.get("/auth/profile/"),
  updateProfile: (data) => api.patch("/auth/profile/", data),
  changePassword: (data) => api.post("/auth/change-password/", data),
};

// UPDATED: Meetings APIs with enhanced scalability features and co-host management
// COMPLETE UPDATED meetingsAPI functions to replace in your api.js

export const meetingsAPI = {
  getUserMeetingHistory: async (userId, dateFilter = "all") => {
    try {
      console.log("🔍 API: Getting user meeting history:", {
        userId,
        dateFilter,
      });

      if (!userId) {
        throw new Error("User ID is required to fetch meeting history");
      }

      const response = await api.get("/api/meetings/user-meeting-history", {
        params: {
          user_id: userId,
          date_filter: dateFilter,
        },
      });

      console.log("✅ API: User meeting history retrieved:", response);

      // Validate response structure
      if (!response) {
        console.warn("⚠️ Empty response from user meeting history API");
        return { meetings: [], summary: null };
      }

      // Handle different response formats
      if (response.meetings && Array.isArray(response.meetings)) {
        return {
          meetings: response.meetings,
          summary: response.summary || null,
          filter_applied: response.filter_applied || dateFilter,
        };
      } else if (Array.isArray(response)) {
        return {
          meetings: response,
          summary: null,
          filter_applied: dateFilter,
        };
      } else {
        console.warn("⚠️ Unexpected response format:", response);
        return { meetings: [], summary: null };
      }
    } catch (error) {
      console.error("❌ API: Failed to get user meeting history:", error);

      // Handle specific error cases
      if (error.response?.status === 404) {
        console.log("ℹ️ No meetings found for this user");
        return { meetings: [], summary: null };
      } else if (error.response?.status === 403) {
        throw new Error("Permission denied: Cannot access meeting history");
      } else if (error.response?.status === 401) {
        throw new Error("Authentication required: Please log in again");
      }

      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to get user meeting history"
      );
    }
  },
  // Get user meetings by specific date range
  // Get user meetings by specific date range
  getUserMeetingsByDate: async (userId, startDate, endDate) => {
    try {
      console.log("🔍 API: Getting user meetings by date:", {
        userId,
        startDate,
        endDate,
      });

      if (!userId) {
        throw new Error("User ID is required");
      }

      const response = await api.get("/api/meetings/user-meetings-by-date", {
        params: {
          user_id: userId,
          start_date: startDate,
          end_date: endDate,
        },
      });

      console.log("✅ API: User meetings by date retrieved:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to get user meetings by date:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to get meetings by date"
      );
    }
  },
  // Get today's meetings for user
  getUserTodayMeetings: async (userId) => {
    try {
      console.log("🔍 API: Getting user today meetings:", { userId });

      if (!userId) {
        throw new Error("User ID is required");
      }

      const response = await api.get("/api/meetings/user-today-meetings", {
        params: {
          user_id: userId,
        },
      });

      console.log("✅ API: User today meetings retrieved:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to get user today meetings:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to get today meetings"
      );
    }
  },
  getUserMeetingHistory: async (userId, dateFilter = "all") => {
    try {
      console.log("🔍 API: Getting user meeting history:", {
        userId,
        dateFilter,
      });

      if (!userId) {
        throw new Error("User ID is required to fetch meeting history");
      }

      const response = await api.get("/api/meetings/user-meeting-history", {
        params: {
          user_id: userId,
          date_filter: dateFilter,
        },
      });

      console.log("✅ API: User meeting history retrieved:", response);

      // Validate response structure
      if (!response) {
        console.warn("⚠️ Empty response from user meeting history API");
        return { meetings: [], summary: null };
      }

      // Handle different response formats
      if (response.meetings && Array.isArray(response.meetings)) {
        return {
          meetings: response.meetings,
          summary: response.summary || null,
          filter_applied: response.filter_applied || dateFilter,
        };
      } else if (Array.isArray(response)) {
        return {
          meetings: response,
          summary: null,
          filter_applied: dateFilter,
        };
      } else {
        console.warn("⚠️ Unexpected response format:", response);
        return { meetings: [], summary: null };
      }
    } catch (error) {
      console.error("❌ API: Failed to get user meeting history:", error);

      // Handle specific error cases
      if (error.response?.status === 404) {
        console.log("ℹ️ No meetings found for this user");
        return { meetings: [], summary: null };
      } else if (error.response?.status === 403) {
        throw new Error("Permission denied: Cannot access meeting history");
      } else if (error.response?.status === 401) {
        throw new Error("Authentication required: Please log in again");
      }

      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to get user meeting history"
      );
    }
  },
  // NEW: Co-host management APIs
  assignCoHost: async (meetingId, userId, assignedBy, userName) => {
    try {
      console.log("👑 API: Assigning co-host:", {
        meetingId,
        userId,
        assignedBy,
      });
      const response = await api.post("/api/meetings/assign-cohost/", {
        meeting_id: meetingId,
        user_id: userId,
        assigned_by: assignedBy,
        user_name: userName,
      });
      console.log("✅ API: Co-host assigned successfully:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to assign co-host:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to assign co-host"
      );
    }
  },

  removeCoHost: async (meetingId, userId, removedBy) => {
    try {
      console.log("👑 API: Removing co-host:", {
        meetingId,
        userId,
        removedBy,
      });
      const response = await api.post("/api/meetings/remove-cohost/", {
        meeting_id: meetingId,
        user_id: userId,
        removed_by: removedBy,
      });
      console.log("✅ API: Co-host removed successfully:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to remove co-host:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to remove co-host"
      );
    }
  },

  getCoHosts: async (meetingId) => {
    try {
      console.log("👑 API: Getting co-hosts for meeting:", meetingId);
      const response = await api.get(`/api/meetings/cohosts/${meetingId}/`);
      console.log("✅ API: Co-hosts retrieved:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to get co-hosts:", error);
      throw new Error(
        error.response?.data?.error || error.message || "Failed to get co-hosts"
      );
    }
  },

  checkCoHostStatus: async (meetingId, userId) => {
    try {
      console.log("👑 API: Checking co-host status:", { meetingId, userId });
      const response = await api.get(
        `/api/meetings/check-cohost/${meetingId}/${userId}/`
      );
      console.log("✅ API: Co-host status checked:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to check co-host status:", error);
      return { is_cohost: false, error: error.message };
    }
  },

  // NEW: Participant removal API
  removeParticipantFromMeeting: async (
    meetingId,
    userId,
    removedBy,
    reason = "removed_by_host_or_cohost"
  ) => {
    try {
      console.log("API: Removing participant from meeting:", {
        meetingId,
        userId,
        removedBy,
        reason,
      });

      const response = await api.post("/api/meetings/remove-participant/", {
        meeting_id: meetingId,
        user_id: userId,
        removed_by: removedBy,
        reason: reason,
        force_disconnect: true,
        disconnect_from_livekit: true,
        notify_participant: true,
      });

      console.log("API: Raw backend response:", response);

      if (response.success) {
        console.log("API: Participant removal successful:", response.message);
        return {
          success: true,
          message: response.message,
          participant_info: response.participant_info,
          livekit_info: response.livekit_info,
          actions_performed: response.actions_performed,
        };
      } else if (
        response.error &&
        (response.error.includes("already left") ||
          response.error.includes("not found"))
      ) {
        return {
          success: true,
          message: "Participant has already left the meeting",
          already_left: true,
          participant_info: {
            user_id: userId,
            status: "already_left",
          },
        };
      } else {
        throw new Error(response.error || "Unknown error from backend");
      }
    } catch (error) {
      console.error("API: Failed to remove participant:", error);

      if (
        error.response?.data?.error?.includes("already left") ||
        error.response?.data?.error?.includes("not found")
      ) {
        return {
          success: true,
          message: "Participant has already left the meeting",
          already_left: true,
          participant_info: {
            user_id: userId,
            status: "already_left",
          },
        };
      }

      if (error.response?.status === 403) {
        throw new Error(
          "Permission denied: Only hosts and co-hosts can remove participants"
        );
      } else if (error.response?.status === 404) {
        throw new Error("Meeting not found");
      } else if (error.response?.status === 400) {
        throw new Error(error.response?.data?.error || "Invalid request data");
      } else {
        throw new Error(
          error.response?.data?.error ||
            error.message ||
            "Failed to remove participant"
        );
      }
    }
  },

  startMeetingRecording: async (meetingId, settings = {}) => {
    try {
      console.log(
        "🔴 API: Starting puppeteer recording for meeting:",
        meetingId
      );

      // Call your new puppeteer recording endpoint
      const response = await api.post(`/api/puppeteer/start/${meetingId}`, {
        recording_type: settings.recording_type || "server",
        user_id: settings.user_id || localStorage.getItem("user_id"),
        quality: settings.quality || "hd",
        include_audio: settings.include_audio !== false,
        include_video: settings.include_video !== false,
        duration_minutes: settings.duration_minutes || null,
        ...settings,
      });

      console.log(
        "✅ API: Puppeteer recording started successfully:",
        response
      );

      return {
        success: response.success || true,
        message:
          response.Message || response.message || "Server recording started",
        recording_id: response.recording_id,
        recording_type: "server",
        meeting_id: meetingId,
        user_interaction_required: false,
        screen_share_dialog: false,
        bot_joining: response.bot_joining || false,
        settings: settings,
      };
    } catch (error) {
      console.error("❌ API: Failed to start puppeteer recording:", error);

      if (
        error.response?.status === 400 &&
        error.response?.data?.Error?.includes("already active")
      ) {
        return {
          success: true,
          message: "Recording is already active",
          already_recording: true,
          recording_type: "server",
        };
      }

      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to start server recording"
      );
    }
  },

  // UPDATED: Stop recording using puppeteer backend
  stopMeetingRecording: async (meetingId) => {
    try {
      console.log(
        "⏹️ API: Stopping puppeteer recording for meeting:",
        meetingId
      );

      // Call your new puppeteer stop endpoint
      const response = await api.post(`/api/puppeteer/stop/${meetingId}`, {
        user_id: localStorage.getItem("user_id"),
      });

      console.log(
        "✅ API: Puppeteer recording stopped successfully:",
        response
      );

      return {
        success: response.success || true,
        message:
          response.Message ||
          response.message ||
          "Server recording stopped and processed",
        meeting_id: meetingId,
        recording_type: "server",
        processing_completed: response.processing_completed || false,

        // Processed file URLs from backend
        video_url: response.video_url,
        transcript_url: response.transcript_url,
        summary_url: response.summary_url,
        subtitle_urls: response.subtitle_urls || {},
        image_url: response.image_url,

        // File metadata
        file_size: response.file_size || 0,
        duration: response.duration || 0,
        transcription_available: !!response.transcript_url,
        summary_available: !!response.summary_url,
      };
    } catch (error) {
      console.error("❌ API: Failed to stop puppeteer recording:", error);

      if (error.response?.status === 404) {
        return {
          success: true,
          message: "Recording was not active",
          recording_type: "server",
          is_recording: false,
        };
      }

      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to stop server recording"
      );
    }
  },

  // NEW: Get puppeteer recording status
  getPuppeteerRecordingStatus: async (meetingId) => {
    try {
      console.log(
        "📊 API: Getting puppeteer recording status for meeting:",
        meetingId
      );

      const response = await api.get(`/api/puppeteer/status/${meetingId}`);

      console.log("✅ API: Recording status retrieved:", response);

      return {
        meeting_id: meetingId,
        is_active: response.is_active || false,
        recording_type: "server",
        status: response.status || {},
      };
    } catch (error) {
      console.error("❌ API: Failed to get recording status:", error);
      return {
        meeting_id: meetingId,
        is_active: false,
        error: error.message,
      };
    }
  },

  // NEW: List all active puppeteer recordings
  getActivePuppeteerRecordings: async () => {
    try {
      console.log("📋 API: Getting active puppeteer recordings");

      const response = await api.get("/api/puppeteer/list-active");

      console.log("✅ API: Active recordings retrieved:", response);

      return {
        active_recordings: response.active_recordings || [],
        total_count: response.total_count || 0,
      };
    } catch (error) {
      console.error("❌ API: Failed to get active recordings:", error);
      return {
        active_recordings: [],
        total_count: 0,
        error: error.message,
      };
    }
  },

  // NEW: Emergency stop all puppeteer recordings
  stopAllPuppeteerRecordings: async () => {
    try {
      console.log("🛑 API: Emergency stopping all puppeteer recordings");

      const response = await api.post("/api/puppeteer/stop-all");

      console.log("✅ API: All recordings stopped:", response);

      return {
        message: response.Message || "All recordings stopped",
        stopped_recordings: response.stopped_recordings || [],
        total_stopped: response.total_stopped || 0,
      };
    } catch (error) {
      console.error("❌ API: Failed to stop all recordings:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to stop all recordings"
      );
    }
  },

  createMeeting: (data) =>
    api.post("/api/meetings/create", {
      Meeting_Name: data.name || data.Meeting_Name,
      Host_ID: data.host_id || data.Host_ID,
      Meeting_Type: data.type || data.Meeting_Type,
      Status: data.status || "active",
      Is_Recording_Enabled: data.recordingEnabled || false,
      Waiting_Room_Enabled: data.waitingRoomEnabled || false,
      Started_At: data.startTime || data.Started_At,
      Ended_At: data.endTime || data.Ended_At,
    }),

  // NEW: Co-host management APIs
  assignCoHost: async (meetingId, userId, assignedBy, userName) => {
    try {
      console.log("👑 API: Assigning co-host:", {
        meetingId,
        userId,
        assignedBy,
      });
      const response = await api.post("/api/meetings/assign-cohost/", {
        meeting_id: meetingId,
        user_id: userId,
        assigned_by: assignedBy,
        user_name: userName,
      });
      console.log("✅ API: Co-host assigned successfully:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to assign co-host:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to assign co-host"
      );
    }
  },

  removeCoHost: async (meetingId, userId, removedBy) => {
    try {
      console.log("👑 API: Removing co-host:", {
        meetingId,
        userId,
        removedBy,
      });
      const response = await api.post("/api/meetings/remove-cohost/", {
        meeting_id: meetingId,
        user_id: userId,
        removed_by: removedBy,
      });
      console.log("✅ API: Co-host removed successfully:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to remove co-host:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to remove co-host"
      );
    }
  },

  getCoHosts: async (meetingId) => {
    try {
      console.log("👑 API: Getting co-hosts for meeting:", meetingId);
      const response = await api.get(`/api/meetings/cohosts/${meetingId}/`);
      console.log("✅ API: Co-hosts retrieved:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to get co-hosts:", error);
      throw new Error(
        error.response?.data?.error || error.message || "Failed to get co-hosts"
      );
    }
  },

  checkCoHostStatus: async (meetingId, userId) => {
    try {
      console.log("👑 API: Checking co-host status:", { meetingId, userId });
      const response = await api.get(
        `/api/meetings/check-cohost/${meetingId}/${userId}/`
      );
      console.log("✅ API: Co-host status checked:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to check co-host status:", error);
      return { is_cohost: false, error: error.message };
    }
  },

  // NEW: Participant removal API
  removeParticipantFromMeeting: async (
    meetingId,
    userId,
    removedBy,
    reason = "removed_by_host_or_cohost"
  ) => {
    try {
      console.log("API: Removing participant from meeting:", {
        meetingId,
        userId,
        removedBy,
        reason,
      });

      const response = await api.post("/api/meetings/remove-participant/", {
        meeting_id: meetingId,
        user_id: userId,
        removed_by: removedBy,
        reason: reason,
        force_disconnect: true,
        disconnect_from_livekit: true,
        notify_participant: true,
      });

      console.log("API: Raw backend response:", response);

      if (response.success) {
        console.log("API: Participant removal successful:", response.message);
        return {
          success: true,
          message: response.message,
          participant_info: response.participant_info,
          livekit_info: response.livekit_info,
          actions_performed: response.actions_performed,
        };
      } else if (
        response.error &&
        (response.error.includes("already left") ||
          response.error.includes("not found"))
      ) {
        return {
          success: true,
          message: "Participant has already left the meeting",
          already_left: true,
          participant_info: {
            user_id: userId,
            status: "already_left",
          },
        };
      } else {
        throw new Error(response.error || "Unknown error from backend");
      }
    } catch (error) {
      console.error("API: Failed to remove participant:", error);

      if (
        error.response?.data?.error?.includes("already left") ||
        error.response?.data?.error?.includes("not found")
      ) {
        return {
          success: true,
          message: "Participant has already left the meeting",
          already_left: true,
          participant_info: {
            user_id: userId,
            status: "already_left",
          },
        };
      }

      if (error.response?.status === 403) {
        throw new Error(
          "Permission denied: Only hosts and co-hosts can remove participants"
        );
      } else if (error.response?.status === 404) {
        throw new Error("Meeting not found");
      } else if (error.response?.status === 400) {
        throw new Error(error.response?.data?.error || "Invalid request data");
      } else {
        throw new Error(
          error.response?.data?.error ||
            error.message ||
            "Failed to remove participant"
        );
      }
    }
  },

  startMeetingRecording: async (meetingId, settings = {}) => {
    try {
      console.log("🔴 API: Starting recording for meeting:", meetingId);
      const response = await api.post(
        `/api/meetings/${meetingId}/start-recording`,
        settings
      );
      console.log("✅ API: Recording started successfully:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to start recording:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to start recording"
      );
    }
  },

  stopMeetingRecording: async (meetingId) => {
    try {
      console.log("⏹️ API: Stopping recording for meeting:", meetingId);
      const response = await api.post(
        `/api/meetings/${meetingId}/stop-recording`
      );
      console.log("✅ API: Recording stopped successfully:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to stop recording:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to stop recording"
      );
    }
  },

  createMeeting: (data) =>
    api.post("/api/meetings/create", {
      Meeting_Name: data.name || data.Meeting_Name,
      Host_ID: data.host_id || data.Host_ID,
      Meeting_Type: data.type || data.Meeting_Type,
      Status: data.status || "active",
      Is_Recording_Enabled: data.recordingEnabled || false,
      Waiting_Room_Enabled: data.waitingRoomEnabled || false,
      Started_At: data.startTime || data.Started_At,
      Ended_At: data.endTime || data.Ended_At,
    }),

  // UPDATED: Create scheduled meeting with full recurring support
  createScheduledMeeting: (data) => {
    console.log(
      "📤 API: Creating scheduled meeting with recurring support:",
      data
    );

    // Enhanced data preparation for recurring meetings
    const apiData = {
      // Basic meeting info
      Meeting_Name: data.title || data.Meeting_Name || "Scheduled Meeting",
      Host_ID: data.host_id || data.Host_ID,
      Meeting_Type: "ScheduleMeeting",
      Status: data.status || "scheduled",
      description: data.description || "",
      location: data.location || "",

      // Meeting times
      Started_At: data.startTime || data.Started_At,
      Ended_At: data.endTime || data.Ended_At,
      start_time: data.start_time || data.startTime || data.Started_At,
      end_time: data.end_time || data.endTime || data.Ended_At,

      // CRITICAL: New visibility window fields for recurring meetings
      start_date: data.start_date || data.startDate,
      end_date: data.end_date || data.endDate,

      // Meeting configuration
      timezone: data.timezone || "Asia/Kolkata",
      duration_minutes: parseInt(data.duration_minutes || data.duration || 60),

      // Settings
      Is_Recording_Enabled:
        data.recordingEnabled || data.Is_Recording_Enabled || false,
      Waiting_Room_Enabled:
        data.waitingRoomEnabled || data.Waiting_Room_Enabled || false,
      settings_waiting_room: data.settings?.waitingRoom ? 1 : 0,
      settings_recording: data.settings?.recording ? 1 : 0,
      settings_allow_chat: data.settings?.allowChat !== false ? 1 : 0,
      settings_allow_screen_share:
        data.settings?.allowScreenShare !== false ? 1 : 0,
      settings_mute_participants: data.settings?.muteParticipants ? 1 : 0,
      settings_require_password: data.settings?.requirePassword ? 1 : 0,
      settings_password: data.settings?.password || null,

      // Participants
      email:
        data.email ||
        (data.participants
          ? data.participants.map((p) => p.email).join(",")
          : ""),
      participants: data.participants
        ? JSON.stringify(data.participants)
        : null,

      // ENHANCED: Recurring meeting data structure
      recurrence: {
        enabled: Boolean(data.recurrence?.enabled),
        type: data.recurrence?.enabled ? data.recurrence?.type : null,
        interval: data.recurrence?.enabled
          ? data.recurrence?.interval || 1
          : null,
        endDate: data.recurrence?.enabled ? data.recurrence?.endDate : null,
        occurrences: data.recurrence?.enabled
          ? data.recurrence?.occurrences
          : null,
        selectedDays:
          data.recurrence?.enabled && data.recurrence?.type === "weekly"
            ? data.recurrence?.selectedDays
            : [],
        selectedMonthDates:
          data.recurrence?.enabled && data.recurrence?.type === "monthly"
            ? data.recurrence?.selectedMonthDates
            : [],
        monthlyPattern:
          data.recurrence?.enabled && data.recurrence?.type === "monthly"
            ? data.recurrence?.monthlyPattern || "same-date"
            : null,
      },

      // Legacy recurrence fields (for backend compatibility)
      is_recurring: data.recurrence?.enabled ? 1 : 0,
      recurrence_type: data.recurrence?.enabled ? data.recurrence?.type : null,
      recurrence_interval: data.recurrence?.enabled
        ? data.recurrence?.interval || 1
        : null,
      recurrence_end_date: data.recurrence?.enabled
        ? data.recurrence?.endDate
        : null,
      recurrence_occurrences: data.recurrence?.enabled
        ? data.recurrence?.occurrences
        : null,
      selected_days:
        data.recurrence?.enabled && data.recurrence?.type === "weekly"
          ? JSON.stringify(data.recurrence?.selectedDays || [])
          : null,
      selected_month_dates:
        data.recurrence?.enabled && data.recurrence?.type === "monthly"
          ? JSON.stringify(data.recurrence?.selectedMonthDates || [])
          : null,
      monthly_pattern:
        data.recurrence?.enabled && data.recurrence?.type === "monthly"
          ? data.recurrence?.monthlyPattern || "same-date"
          : null,

      // Reminders
      reminders_email: data.reminders?.email !== false ? 1 : 0,
      reminders_browser: data.reminders?.browser !== false ? 1 : 0,
      reminders_times: JSON.stringify(data.reminders?.reminderTimes || [15, 5]),
    };

    console.log("📋 Final API data being sent:", {
      ...apiData,
      recurrence_info: {
        enabled: apiData.recurrence.enabled,
        type: apiData.recurrence.type,
        interval: apiData.recurrence.interval,
        endDate: apiData.recurrence.endDate,
      },
    });

    return api.post("/api/meetings/schedule-meeting", apiData);
  },

  // UPDATED: Get user scheduled meetings with recurring support
  getUserScheduledMeetings: async (userId, userEmail) => {
    try {
      console.log("API: Getting user scheduled meetings:", {
        userId,
        userEmail,
      });

      if (!userId && !userEmail) {
        throw new Error("Either userId or userEmail is required");
      }

      const response = await api.get("/api/meetings/user-schedule-meetings", {
        params: {
          user_id: userId,
          user_email: userEmail,
        },
      });

      console.log("API: Response received:", response);

      if (response.meetings && Array.isArray(response.meetings)) {
        const processedMeetings = response.meetings.map((meeting) => ({
          ...meeting,
          id: meeting.ID || meeting.Meeting_ID || meeting.id,
          meetingId: meeting.ID || meeting.Meeting_ID || meeting.id,
          title: meeting.title || meeting.Meeting_Name,
          startTime: meeting.start_time || meeting.Started_At,
          endTime: meeting.end_time || meeting.Ended_At,
          isRecurring: Boolean(meeting.is_recurring),
          selectedDays: Array.isArray(meeting.selected_days)
            ? meeting.selected_days
            : meeting.selected_days
            ? JSON.parse(meeting.selected_days)
            : [],
          selectedMonthDates: Array.isArray(meeting.selected_month_dates)
            ? meeting.selected_month_dates
            : meeting.selected_month_dates
            ? JSON.parse(meeting.selected_month_dates)
            : [],
          participants: meeting.participants || [],
          participantEmails: meeting.email
            ? meeting.email.split(",").map((e) => e.trim())
            : [],
          hostName: meeting.host_name || meeting.Host_Name,
          meetingLink: meeting.Meeting_Link,
          status: meeting.Status || meeting.status,
        }));

        return {
          meetings: processedMeetings,
          summary: response.summary || {},
          totalMeetings: processedMeetings.length,
        };
      }

      return response;
    } catch (error) {
      console.error("Failed to get user scheduled meetings:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to get scheduled meetings"
      );
    }
  },

  // UPDATED: Update meeting with recurring support
  updateMeeting: (id, data) => {
    console.log("🔄 API: Updating meeting with recurring support:", id, data);

    const requestData = {
      ...data,
      Meeting_Type: data.Meeting_Type || "ScheduleMeeting",
      description: data.description || "",
      location: data.location || "",

      // Enhanced recurring data for updates
      recurrence: {
        enabled: Boolean(data.recurrence?.enabled),
        type: data.recurrence?.enabled ? data.recurrence?.type : null,
        interval: data.recurrence?.enabled
          ? data.recurrence?.interval || 1
          : null,
        endDate: data.recurrence?.enabled ? data.recurrence?.endDate : null,
        occurrences: data.recurrence?.enabled
          ? data.recurrence?.occurrences
          : null,
        selectedDays:
          data.recurrence?.enabled && data.recurrence?.type === "weekly"
            ? data.recurrence?.selectedDays
            : [],
        selectedMonthDates:
          data.recurrence?.enabled && data.recurrence?.type === "monthly"
            ? data.recurrence?.selectedMonthDates
            : [],
        monthlyPattern:
          data.recurrence?.enabled && data.recurrence?.type === "monthly"
            ? data.recurrence?.monthlyPattern || "same-date"
            : null,
      },

      // Legacy fields for backend compatibility
      is_recurring: data.recurrence?.enabled ? 1 : 0,
      recurrence_type: data.recurrence?.enabled ? data.recurrence?.type : null,
      recurrence_interval: data.recurrence?.enabled
        ? data.recurrence?.interval || 1
        : null,
      recurrence_end_date: data.recurrence?.enabled
        ? data.recurrence?.endDate
        : null,
      recurrence_occurrences: data.recurrence?.enabled
        ? data.recurrence?.occurrences
        : null,
      selected_days:
        data.recurrence?.enabled && data.recurrence?.type === "weekly"
          ? JSON.stringify(data.recurrence?.selectedDays || [])
          : null,
      selected_month_dates:
        data.recurrence?.enabled && data.recurrence?.type === "monthly"
          ? JSON.stringify(data.recurrence?.selectedMonthDates || [])
          : null,
      monthly_pattern:
        data.recurrence?.enabled && data.recurrence?.type === "monthly"
          ? data.recurrence?.monthlyPattern || "same-date"
          : null,

      // Visibility window updates
      start_date: data.start_date || data.startDate,
      end_date: data.end_date || data.endDate,
    };

    console.log("📋 Update data being sent:", requestData);
    return api.put(`/api/meetings/update/${id}`, requestData);
  },

  // Keep all other existing meetingsAPI methods unchanged
  createMeeting: (data) =>
    api.post("/api/meetings/create", {
      Meeting_Name: data.name || data.Meeting_Name,
      Host_ID: data.host_id || data.Host_ID,
      Meeting_Type: data.type || data.Meeting_Type,
      Status: data.status || "active",
      Is_Recording_Enabled: data.recordingEnabled || false,
      Waiting_Room_Enabled: data.waitingRoomEnabled || false,
      Started_At: data.startTime || data.Started_At,
      Ended_At: data.endTime || data.Ended_At,
    }),

  getMeeting: (id) => {
    console.log("🔍 API: Getting meeting:", id);
    return api.get(`/api/meetings/get/${id}`);
  },

  deleteMeeting: (id) => {
    console.log("🗑️ API: Deleting meeting:", id);
    return api.delete(`/api/meetings/delete/${id}`);
  },

  getMeetings: (params) => api.get("/api/meetings/list", { params }),

  createInstantMeeting: (data) =>
    api.post("/api/meetings/instant-meeting", {
      Meeting_Name: data.name || data.Meeting_Name || "Instant Meeting",
      Host_ID: data.host_id || data.Host_ID,
      Meeting_Type: "InstantMeeting",
      Status: data.status || "active",
      Is_Recording_Enabled: data.recordingEnabled || false,
      Waiting_Room_Enabled: data.waitingRoomEnabled || false,
    }),

  getScheduledMeetings: () => {
    console.log("🔍 API: Getting scheduled meetings");
    return api.get("/api/meetings/schedule-meetings");
  },

  createCalendarMeeting: (data) =>
    api.post("/api/meetings/calendar-meeting", {
      Meeting_Name: data.title || data.Meeting_Name,
      Host_ID: data.host_id || data.Host_ID,
      Meeting_Type: "CalendarMeeting",
      Status: "scheduled",
      Started_At: data.startTime || data.Started_At,
      Ended_At: data.endTime || data.Ended_At,
      Is_Recording_Enabled: data.recordingEnabled || false,
      Waiting_Room_Enabled: data.waitingRoomEnabled || false,
      Title: data.title,
      Location: data.location,
      Description: data.description,
      Organizer: data.organizer,
      GuestEmails: data.guestEmails,
      ReminderMinutes: data.reminderMinutes,
    }),

  // Meeting control methods
  joinMeeting: (id, data) => api.post(`/meetings/${id}/join/`, data),
  leaveMeeting: (id) => api.post(`/meetings/${id}/leave/`),
  endMeeting: (id) => api.post(`/meetings/${id}/end/`),

  // Recording methods
  startRecording: (id, settings) =>
    api.post(`/api/meetings/${id}/start-recording`, settings || {}),
  stopRecording: (id) => api.post(`/api/meetings/${id}/stop-recording`),

  // Waiting room methods
  allowFromWaitingRoom: (id, data) =>
    api.post(`/api/meetings/${id}/allow-from-waiting-room`, data || {}),

  // Settings methods
  updateMeetingSettings: (id, settings) =>
    api.patch(`/meetings/${id}/settings/`, settings),
  toggleWaitingRoom: (id, enabled) =>
    api.patch(`/meetings/${id}/waiting-room/`, { enabled }),
  toggleRecording: (id, enabled) =>
    api.patch(`/meetings/${id}/recording/`, { enabled }),

  // Enhanced methods for scalability (if you're using them)
  checkConnectionQueue: async (meetingId, userId) => {
    try {
      console.log("🚦 Checking connection queue:", { meetingId, userId });
      const response = await api.get(
        `/api/meetings/check-queue/${meetingId}/`,
        {
          params: { user_id: userId },
        }
      );
      console.log("✅ Queue status:", response);
      return response;
    } catch (error) {
      console.error("❌ Queue check failed:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to check connection queue"
      );
    }
  },

  joinMeetingWithQueue: async (meetingData) => {
    try {
      console.log("🚀 Joining meeting with queue management:", meetingData);
      const response = await api.post("/api/meetings/join-with-queue/", {
        meeting_id: meetingData.meetingId || meetingData.meeting_id,
        user_id: meetingData.userId || meetingData.user_id,
        meetingId: meetingData.meetingId || meetingData.meeting_id,
        userId: meetingData.userId || meetingData.user_id,
      });
      console.log("✅ Queue join response:", response);
      return response;
    } catch (error) {
      console.error("❌ Queue join failed:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to join meeting queue"
      );
    }
  },
};

// ==================== ANALYTICS API - COMPLETE FIXED VERSION ====================
// Replace the analyticsAPI export in your api.js with this code

export const analyticsAPI = {
  // ==================== COMPREHENSIVE ANALYTICS ====================
  getComprehensiveAnalytics: (params = {}) => {
    console.log(
      "📊 Analytics API: Getting comprehensive analytics with params:",
      params
    );
    return api.get("/api/analytics/comprehensive", { params });
  },

  // ✅ ADD THIS NEW METHOD HERE ✅
  getAvailableMeetingTimes: async (params = {}) => {
    try {
      console.log("📅 Analytics API: Getting available meeting times:", params);
      
      const response = await api.get("/api/analytics/meeting-times", { params });
      
      console.log("✅ Analytics API: Meeting times retrieved:", response);
      
      // Handle different response formats
      if (response && response.data) {
        return {
          success: true,
          data: response.data,
          count: response.count || response.data.length
        };
      }
      
      return response;
    } catch (error) {
      console.error("❌ Analytics API: Failed to get meeting times:", error);
      
      // Return empty array instead of throwing error
      if (error.response?.status === 404) {
        console.log("ℹ No meeting times found");
        return { success: true, data: [], count: 0 };
      }
      
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to get available meeting times"
      );
    }
  },

  getParticipantDurationAnalytics: (params = {}) => {
    console.log("📊 Analytics API: Getting participant duration analytics");
    return api.get("/api/analytics/participant/duration", { params });
  },
  getComprehensiveAnalytics: (params = {}) => {
    console.log(
      "📊 Analytics API: Getting comprehensive analytics with params:",
      params
    );
    return api.get("/api/analytics/comprehensive", { params });
  },

  getParticipantDurationAnalytics: (params = {}) => {
    console.log("📊 Analytics API: Getting participant duration analytics");
    return api.get("/api/analytics/participant/duration", { params });
  },

  getParticipantAttendanceAnalytics: (params = {}) => {
    console.log("📊 Analytics API: Getting participant attendance analytics");
    return api.get("/api/analytics/participant/attendance", { params });
  },

  getHostMeetingCountAnalytics: (params = {}) => {
    console.log("📊 Analytics API: Getting host meeting count analytics");
    return api.get("/api/analytics/host/meeting-counts", { params });
  },

  // ==================== HOST ANALYTICS ====================

  getHostDashboardOverview: (params = {}) => {
    console.log("📊 Analytics API: Getting host dashboard overview");
    return api.get("/api/analytics/host/overview", { params });
  },

  getHostMeetingReports: (params = {}) => {
    console.log("📊 Analytics API: Getting host meeting reports");
    return api.get("/api/analytics/comprehensive", {
      params: {
        ...params,
        analytics_type: "meeting",
      },
    });
  },

  getHostEngagementDistribution: (params = {}) => {
    console.log("📊 Analytics API: Getting host engagement distribution");
    return api.get("/api/analytics/comprehensive", {
      params: {
        ...params,
        analytics_type: "host",
      },
    });
  },

  getHostMeetingTrends: (params = {}) => {
    console.log("📊 Analytics API: Getting host meeting trends");
    return api.get("/api/analytics/host/meeting-counts", { params });
  },

  // ==================== PARTICIPANT ANALYTICS ====================

  getParticipantPersonalReport: (userId, params = {}) => {
    console.log(
      "📊 Analytics API: Getting participant personal report for user:",
      userId
    );
    return api.get("/api/analytics/comprehensive", {
      params: {
        user_id: userId,
        analytics_type: "participant",
        ...params,
      },
    });
  },

  getParticipantAttendance: (userId, params = {}) => {
    console.log(
      "📊 Analytics API: Getting participant attendance for user:",
      userId
    );
    return api.get("/api/analytics/participant/attendance", {
      params: {
        user_id: userId,
        ...params,
      },
    });
  },

  getParticipantEngagement: (userId, params = {}) => {
    console.log(
      "📊 Analytics API: Getting participant engagement for user:",
      userId
    );
    return api.get("/api/analytics/participant/duration", {
      params: {
        user_id: userId,
        ...params,
      },
    });
  },

  // ==================== REPORT GENERATION - CRITICAL FIX ====================

  /**
   * Generate participant PDF report
   * CRITICAL FIX: Enhanced token retrieval and validation
   */
  generateParticipantReportPDF: async (params = {}) => {
    try {
      console.log(
        "📊 Analytics API: Generating participant PDF report with params:",
        params
      );

      // CRITICAL FIX: Try multiple token sources
      let token = localStorage.getItem(TOKEN_KEY);

      if (!token) {
        // Try alternative token keys
        token =
          localStorage.getItem("token") ||
          localStorage.getItem("access_token") ||
          localStorage.getItem("authToken");
      }

      if (!token) {
        // Try to get from user data
        const userData = localStorage.getItem("user");
        if (userData) {
          try {
            const user = JSON.parse(userData);
            token = user.token || user.access_token || user.accessToken;
          } catch (e) {
            console.error("Failed to parse user data:", e);
          }
        }
      }

      if (!token) {
        console.error(
          "❌ Token not found. Available localStorage keys:",
          Object.keys(localStorage)
        );
        throw new Error("Authentication token not found. Please log in again.");
      }

      console.log(
        "🔐 Using auth token (first 20 chars):",
        token.substring(0, 20) + "..."
      );

      // CRITICAL: Use axios directly instead of api instance
      // This bypasses the response interceptor that unwraps response.data
      const response = await axios.get(
        `${API_BASE_URL}/api/reports/participant/pdf`,
        {
          params,
          responseType: "blob",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          timeout: 60000, // 60 second timeout
          validateStatus: function (status) {
            return status >= 200 && status < 300;
          },
        }
      );

      console.log("✅ Analytics API: PDF response received:", {
        status: response.status,
        statusText: response.statusText,
        dataType: typeof response.data,
        dataSize: response.data?.size,
        dataConstructor: response.data?.constructor?.name,
        contentType: response.headers?.["content-type"],
      });

      // Validate blob
      if (!response.data) {
        throw new Error("No data received from server");
      }

      if (!(response.data instanceof Blob)) {
        console.error("❌ Response is not a Blob:", response.data);
        throw new Error(`Expected Blob, got ${typeof response.data}`);
      }

      if (response.data.size === 0) {
        throw new Error("Received empty PDF file (0 bytes)");
      }

      console.log("✅ Analytics API: Valid PDF blob confirmed:", {
        size: response.data.size,
        type: response.data.type,
      });

      return response.data; // Return the blob directly
    } catch (error) {
      console.error(
        "❌ Analytics API: Failed to generate participant PDF:",
        error
      );

      // Enhanced error reporting
      if (error.response) {
        console.error("Server response error:", {
          status: error.response.status,
          statusText: error.response.statusText,
          data: error.response.data,
        });

        // Try to read error message from blob if present
        if (error.response.data instanceof Blob) {
          try {
            const text = await error.response.data.text();
            console.error("Error blob content:", text);
            throw new Error(`Server error: ${text}`);
          } catch (e) {
            throw new Error(
              `Server error: ${error.response.status} ${error.response.statusText}`
            );
          }
        }

        throw new Error(
          error.response.data?.Error ||
            error.response.data?.error ||
            `Server error: ${error.response.status}`
        );
      } else if (error.request) {
        console.error("No response received:", error.request);
        throw new Error(
          "No response from server. Please check your connection."
        );
      } else {
        throw new Error(error.message || "Failed to generate PDF report");
      }
    }
  },

  /**
   * Generate host PDF report
   * CRITICAL FIX: Enhanced token retrieval and validation
   */
  generateHostReportPDF: async (params = {}) => {
    try {
      console.log(
        "📊 Analytics API: Generating host PDF report with params:",
        params
      );

      // CRITICAL FIX: Try multiple token sources
      let token = localStorage.getItem(TOKEN_KEY);

      if (!token) {
        // Try alternative token keys
        token =
          localStorage.getItem("token") ||
          localStorage.getItem("access_token") ||
          localStorage.getItem("authToken");
      }

      if (!token) {
        // Try to get from user data
        const userData = localStorage.getItem("user");
        if (userData) {
          try {
            const user = JSON.parse(userData);
            token = user.token || user.access_token || user.accessToken;
          } catch (e) {
            console.error("Failed to parse user data:", e);
          }
        }
      }

      if (!token) {
        console.error(
          "❌ Token not found. Available localStorage keys:",
          Object.keys(localStorage)
        );
        throw new Error("Authentication token not found. Please log in again.");
      }

      console.log(
        "🔐 Using auth token (first 20 chars):",
        token.substring(0, 20) + "..."
      );

      // CRITICAL: Use axios directly instead of api instance
      const response = await axios.get(`${API_BASE_URL}/api/reports/host/pdf`, {
        params,
        responseType: "blob",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        timeout: 60000,
        validateStatus: function (status) {
          return status >= 200 && status < 300;
        },
      });

      console.log("✅ Analytics API: PDF response received:", {
        status: response.status,
        statusText: response.statusText,
        dataType: typeof response.data,
        dataSize: response.data?.size,
        dataConstructor: response.data?.constructor?.name,
        contentType: response.headers?.["content-type"],
      });

      // Validate blob
      if (!response.data) {
        throw new Error("No data received from server");
      }

      if (!(response.data instanceof Blob)) {
        console.error("❌ Response is not a Blob:", response.data);
        throw new Error(`Expected Blob, got ${typeof response.data}`);
      }

      if (response.data.size === 0) {
        throw new Error("Received empty PDF file (0 bytes)");
      }

      console.log("✅ Analytics API: Valid PDF blob confirmed:", {
        size: response.data.size,
        type: response.data.type,
      });

      return response.data; // Return the blob directly
    } catch (error) {
      console.error("❌ Analytics API: Failed to generate host PDF:", error);

      // Enhanced error reporting
      if (error.response) {
        console.error("Server response error:", {
          status: error.response.status,
          statusText: error.response.statusText,
          data: error.response.data,
        });

        // Try to read error message from blob if present
        if (error.response.data instanceof Blob) {
          try {
            const text = await error.response.data.text();
            console.error("Error blob content:", text);
            throw new Error(`Server error: ${text}`);
          } catch (e) {
            throw new Error(
              `Server error: ${error.response.status} ${error.response.statusText}`
            );
          }
        }

        throw new Error(
          error.response.data?.Error ||
            error.response.data?.error ||
            `Server error: ${error.response.status}`
        );
      } else if (error.request) {
        console.error("No response received:", error.request);
        throw new Error(
          "No response from server. Please check your connection."
        );
      } else {
        throw new Error(error.message || "Failed to generate PDF report");
      }
    }
  },

  /**
   * Get participant report preview (JSON data)
   */
  getParticipantReportPreview: (params = {}) => {
    console.log("📊 Analytics API: Getting participant report preview");
    return api.get("/api/reports/participant/preview", { params });
  },

  /**
   * Get host report preview (JSON data)
   */
  getHostReportPreview: (params = {}) => {
    console.log("📊 Analytics API: Getting host report preview");
    return api.get("/api/reports/host/preview", { params });
  },

  // ==================== UTILITY FUNCTIONS - ENHANCED ====================

  /**
   * Download PDF report
   * CRITICAL FIX: Enhanced validation and error handling
   */
  downloadPDFReport: (blob, filename) => {
    try {
      console.log("📊 Analytics API: downloadPDFReport called with:", {
        blobType: typeof blob,
        blobSize: blob?.size,
        blobConstructor: blob?.constructor?.name,
        isBlob: blob instanceof Blob,
        filename,
      });

      // CRITICAL: Validate blob thoroughly
      if (!blob) {
        throw new Error("PDF blob is null or undefined");
      }

      if (!(blob instanceof Blob)) {
        console.error("❌ Invalid blob type. Expected Blob, got:", {
          type: typeof blob,
          value: blob,
          constructor: blob?.constructor?.name,
        });
        throw new Error(
          `Invalid blob data - expected Blob, got ${typeof blob}`
        );
      }

      if (blob.size === 0) {
        throw new Error("PDF blob is empty (size: 0 bytes)");
      }

      console.log("✅ Blob validation passed, creating download URL");

      // Create object URL for the blob
      const url = window.URL.createObjectURL(blob);
      console.log("✅ Blob URL created:", url);

      // Create temporary download link
      const link = document.createElement("a");
      link.href = url;
      link.download = filename || "analytics_report.pdf";
      link.style.display = "none";

      // Add to DOM, click, and cleanup
      document.body.appendChild(link);
      console.log("✅ Download link created and added to DOM");

      link.click();
      console.log("✅ Download triggered");

      // Cleanup after a short delay
      setTimeout(() => {
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
        console.log("✅ Analytics API: PDF downloaded successfully:", filename);
      }, 100);

      return true;
    } catch (error) {
      console.error("❌ Analytics API: Error downloading PDF:", error);
      console.error("Error details:", {
        message: error.message,
        stack: error.stack,
      });
      throw error;
    }
  },

  getUserProfile: () => {
    return api.get("/api/user/profile");
  },

  getUserStats: (params = {}) => {
    console.log("📊 Analytics API: Getting user stats");
    return api.get("/api/analytics/comprehensive", {
      params: {
        ...params,
        analytics_type: "all",
      },
    });
  },
};

// ENHANCED: LiveKit Integration APIs with scalability features
export const livekitAPI = {
  // Enhanced join meeting with queue support
  joinMeeting: async (meetingData) => {
    try {
      console.log(
        "🚀 LiveKit: Joining meeting with scalability features:",
        meetingData
      );

      const requestData = {
        meeting_id: meetingData.meetingId || meetingData.meeting_id,
        user_id: meetingData.userId || meetingData.user_id,
        displayName:
          meetingData.displayName || meetingData.user_name || meetingData.name,
        isHost: meetingData.isHost || false,
        realMeetingId: meetingData.realMeetingId || meetingData.meetingId,
        // Backend expects these specific field names
        meeting_id: meetingData.meetingId || meetingData.meeting_id,
        user_id: meetingData.userId || meetingData.user_id,
        user_name:
          meetingData.displayName || meetingData.user_name || meetingData.name,
        is_host: meetingData.isHost || false,
      };

      const response = await api.post(
        "/api/livekit/join-meeting/",
        requestData
      );

      if (response.success && response.access_token) {
        console.log(
          "✅ LiveKit: Successfully joined meeting with enhanced features"
        );
        return {
          success: true,
          accessToken: response.access_token,
          roomName: response.room_name,
          participantIdentity: response.participant_identity,
          livekitUrl: response.livekit_url,
          meetingInfo: response.meeting_info,
          participants: response.participants || [],
          config: response.config || {},
          // NEW: Enhanced response data
          participantCount: response.participant_count || 0,
          debugInfo: response.debug_info || {},
          performanceMode: response.debug_info?.performance_mode || "standard",
          maxParticipants: response.meeting_info?.max_participants || 50,
        };
      } else {
        throw new Error("Invalid response from LiveKit join API");
      }
    } catch (error) {
      console.error("❌ LiveKit: Failed to join meeting:", error);

      // Handle specific error cases from backend
      if (error.response?.status === 429) {
        throw new Error(
          "Meeting is at maximum capacity. Please try again later."
        );
      } else if (error.response?.status === 503) {
        throw new Error("Video conferencing service temporarily unavailable");
      } else if (error.response?.data?.error?.includes("queue")) {
        throw new Error(`Connection queue: ${error.response.data.error}`);
      }

      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to join LiveKit meeting"
      );
    }
  },

  // Leave LiveKit meeting
  leaveMeeting: async (meetingData) => {
    try {
      console.log("🚪 LiveKit: Leaving meeting:", meetingData);

      const response = await api.post("/api/livekit/leave-meeting/", {
        meeting_id: meetingData.meetingId || meetingData.meeting_id,
        user_id: meetingData.userId || meetingData.user_id,
        participant_identity: meetingData.participant_identity,
      });

      console.log("✅ LiveKit: Successfully left meeting");
      return response;
    } catch (error) {
      console.error("❌ LiveKit: Failed to leave meeting:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to leave meeting"
      );
    }
  },

  // Get current participants in meeting
  getParticipants: async (meetingId) => {
    try {
      console.log("👥 LiveKit: Getting participants for meeting:", meetingId);

      const response = await api.get(`/api/livekit/participants/${meetingId}/`);

      console.log("✅ LiveKit: Retrieved participants:", response);
      return {
        participants: response.participants || [],
        totalCount: response.total_count || 0,
        meetingId: response.meeting_id,
      };
    } catch (error) {
      console.error("❌ LiveKit: Failed to get participants:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to get participants"
      );
    }
  },

  // Get LiveKit connection info
  getConnectionInfo: async (meetingId) => {
    try {
      console.log(
        "🔗 LiveKit: Getting connection info for meeting:",
        meetingId
      );

      const response = await api.get(
        `/api/livekit/connection-info/${meetingId}/`
      );

      console.log("✅ LiveKit: Retrieved connection info");
      return response;
    } catch (error) {
      console.error("❌ LiveKit: Failed to get connection info:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to get connection info"
      );
    }
  },

  // Send reaction
  sendReaction: async (reactionData) => {
    try {
      console.log("👍 LiveKit: Sending reaction:", reactionData);

      const response = await api.post("/api/livekit/send-reaction/", {
        meeting_id: reactionData.meetingId || reactionData.meeting_id,
        user_id: reactionData.userId || reactionData.user_id,
        emoji: reactionData.emoji,
        user_name: reactionData.userName || reactionData.user_name,
      });

      console.log("✅ LiveKit: Reaction sent");
      return response;
    } catch (error) {
      console.error("❌ LiveKit: Failed to send reaction:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to send reaction"
      );
    }
  },

  // Send chat message
  sendChatMessage: async (messageData) => {
    try {
      console.log("💬 LiveKit: Sending chat message:", messageData);

      const response = await api.post("/api/livekit/send-chat/", {
        meeting_id: messageData.meetingId || messageData.meeting_id,
        user_id: messageData.userId || messageData.user_id,
        message: messageData.message,
        user_name: messageData.userName || messageData.user_name,
      });

      console.log("✅ LiveKit: Chat message sent");
      return response;
    } catch (error) {
      console.error("❌ LiveKit: Failed to send chat message:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to send chat message"
      );
    }
  },

  // Get chat history
  getChatHistory: async (meetingId) => {
    try {
      console.log("💬 LiveKit: Getting chat history for meeting:", meetingId);

      const response = await api.get(`/api/livekit/chat-history/${meetingId}/`);

      console.log("✅ LiveKit: Retrieved chat history");
      return {
        success: true,
        messages: response.messages || [],
        totalCount: response.total_count || 0,
      };
    } catch (error) {
      console.error("❌ LiveKit: Failed to get chat history:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to get chat history"
      );
    }
  },

  // Update participant status (audio/video)
  updateParticipantStatus: async (statusData) => {
    try {
      console.log("🔄 LiveKit: Updating participant status:", statusData);

      const response = await api.post("/api/livekit/update-status/", {
        meeting_id: statusData.meetingId || statusData.meeting_id,
        user_id: statusData.userId || statusData.user_id,
        status_type: statusData.statusType || statusData.status_type,
        is_enabled:
          statusData.isEnabled !== undefined
            ? statusData.isEnabled
            : statusData.is_enabled,
      });

      console.log("✅ LiveKit: Participant status updated");
      return response;
    } catch (error) {
      console.error("❌ LiveKit: Failed to update participant status:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to update participant status"
      );
    }
  },

  // Record meeting event
  recordMeetingEvent: async (eventData) => {
    try {
      console.log("📅 LiveKit: Recording meeting event:", eventData);

      const response = await api.post("/api/livekit/record-event/", {
        meeting_id: eventData.meetingId || eventData.meeting_id,
        user_id: eventData.userId || eventData.user_id,
        event_type: eventData.eventType || eventData.event_type,
        event_data: eventData.eventData || eventData.event_data || {},
      });

      console.log("✅ LiveKit: Meeting event recorded");
      return response;
    } catch (error) {
      console.error("❌ LiveKit: Failed to record meeting event:", error);
      return { success: false, error: error.message };
    }
  },
};

// UPDATED: Enhanced Participants APIs with optimized syncing
export const participantsAPI = {
  // NEW: Enhanced live participants endpoint that uses optimized backend
  getLiveParticipantsEnhanced: async (meetingId) => {
    try {
      console.log(
        "📊 API: Getting enhanced live participants for meeting:",
        meetingId
      );

      const response = await api.get(
        `/api/participants/live-enhanced/${meetingId}/`
      );

      console.log("✅ API: Enhanced participants retrieved:", response);
      return {
        success: true,
        meetingId: response.meeting_id,
        summary: response.summary || {},
        participants: response.participants || [],
        livekitRaw: response.livekit_raw || [],
        livekitEnabled: response.livekit_enabled || false,
        debugInfo: response.debug_info || {},
      };
    } catch (error) {
      console.error("❌ API: Failed to get enhanced live participants:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to get enhanced live participants"
      );
    }
  },

  // Updated to use enhanced endpoint
  getLiveParticipants: async (meetingId) => {
    return participantsAPI.getLiveParticipantsEnhanced(meetingId);
  },

  // UPDATED: Record participant join with enhanced error handling
  recordJoin: async (participantData) => {
    try {
      console.log("📝 API: Recording participant join:", participantData);

      const response = await api.post("/api/participants/record-join/", {
        meeting_id: participantData.meetingId || participantData.meeting_id,
        user_id: participantData.userId || participantData.user_id,
        user_name:
          participantData.userName ||
          participantData.user_name ||
          participantData.displayName,
        is_host: participantData.isHost || participantData.is_host || false,
        participant_identity: participantData.participant_identity,
        // Backend expects these specific field names
        Meeting_ID: participantData.meetingId || participantData.meeting_id,
        User_ID: participantData.userId || participantData.user_id,
        Full_Name:
          participantData.userName ||
          participantData.user_name ||
          participantData.displayName,
        IsHost: participantData.isHost || participantData.is_host || false,
      });

      console.log("✅ API: Participant join recorded");
      return response;
    } catch (error) {
      console.error("❌ API: Failed to record participant join:", error);

      // Handle specific backend error cases
      if (error.response?.status === 409) {
        console.warn(
          "⚠️ Participant already exists - this is expected behavior"
        );
        return {
          success: true,
          status: "already_exists",
          message: "Participant already recorded",
        };
      }

      return { success: false, error: error.message };
    }
  },

  // UPDATED: Record participant leave with enhanced error handling
  recordLeave: async (participantData) => {
    try {
      console.log(
        "📝 API: Recording MANUAL participant leave:",
        participantData
      );

      const response = await api.post("/api/participants/record-leave/", {
        meeting_id: participantData.meetingId || participantData.meeting_id,
        user_id: participantData.userId || participantData.user_id,
        participant_id: participantData.participant_id,
        // CRITICAL: Manual leave flags
        manual_leave: true,
        reason: "manual",
        leave_type: "user_action",
        // Backend expects these specific field names
        Meeting_ID: participantData.meetingId || participantData.meeting_id,
        User_ID: participantData.userId || participantData.user_id,
      });

      console.log("✅ API: Manual participant leave recorded successfully");
      return response;
    } catch (error) {
      console.error("⚠ API: Failed to record participant leave:", error);

      // Handle backend responses for manual leave blocking
      if (error.response?.status === 400) {
        const errorData = error.response.data;

        if (errorData.blocked_reason) {
          console.warn(
            "⚠ Leave blocked - this indicates an issue with manual leave detection"
          );
          return {
            success: false,
            status: "blocked_auto_leave",
            message: "Leave was blocked - check manual leave flags",
            blocked_reason: errorData.blocked_reason,
          };
        }

        if (errorData.status === "already_left") {
          console.warn("⚠ Participant already left - this is normal");
          return {
            success: true,
            status: "already_left",
            message: "Participant already recorded as left",
            participant_id: errorData.participant_id,
            leave_time: errorData.leave_time,
          };
        }

        if (errorData.status === "not_found") {
          console.warn(
            "⚠ Participant never joined - this can happen with quick disconnects"
          );
          return {
            success: true,
            status: "never_joined",
            message: "Participant never joined meeting",
            meeting_id: errorData.meeting_id,
            user_id: errorData.user_id,
          };
        }

        console.error("❌ Bad request error:", errorData.error);
        return {
          success: false,
          error: errorData.error || "Invalid request data",
          details: errorData.details || "Check request parameters",
        };
      }

      // Handle other errors...
      if (error.response?.status === 500) {
        console.error(
          "❌ Server error while recording leave:",
          error.response.data
        );
        return {
          success: false,
          error: "Server error while recording participant leave",
          details: error.response.data?.details || "Internal server error",
        };
      }

      if (error.code === "ERR_NETWORK") {
        console.error("❌ Network error while recording leave");
        return {
          success: false,
          error: "Network error - could not connect to server",
          isNetworkError: true,
        };
      }

      console.error("❌ Unexpected error while recording leave:", error);
      return {
        success: false,
        error:
          error.response?.data?.error ||
          error.message ||
          "Failed to record participant leave",
        status: error.response?.status || "unknown",
      };
    }
  },

  // NEW: Optimized participant sync
  syncParticipantsOptimized: async (meetingId) => {
    try {
      console.log("🔄 API: Optimized participant sync for meeting:", meetingId);

      const response = await api.post(
        `/api/participants/sync-optimized/${meetingId}/`
      );

      console.log("✅ API: Optimized participants sync completed:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to sync participants optimized:", error);
      throw new Error(
        error.response?.data?.error ||
          error.message ||
          "Failed to sync participants"
      );
    }
  },

  // Legacy sync method - kept for backward compatibility
  syncParticipants: async (meetingId) => {
    return participantsAPI.syncParticipantsOptimized(meetingId);
  },

  // Legacy methods - kept for backward compatibility
  getParticipants: (meetingId) =>
    participantsAPI.getLiveParticipantsEnhanced(meetingId),
  joinParticipant: (data) => participantsAPI.recordJoin(data),
  leaveParticipant: (participantId) =>
    participantsAPI.recordLeave({ participant_id: participantId }),
  getParticipant: (participantId) =>
    api.get(`/participants/get/${participantId}`),
  removeParticipant: (participantId) =>
    api.delete(`/participants/delete/${participantId}`),
  updateParticipant: (meetingId, participantId, data) =>
    api.patch(`/meetings/${meetingId}/participants/${participantId}/`, data),
  promoteParticipant: (meetingId, participantId, role) =>
    api.patch(`/meetings/${meetingId}/participants/${participantId}/promote/`, {
      role,
    }),
  muteParticipant: (meetingId, participantId) =>
    api.post(`/meetings/${meetingId}/participants/${participantId}/mute/`),
  unmuteParticipant: (meetingId, participantId) =>
    api.post(`/meetings/${meetingId}/participants/${participantId}/unmute/`),
};

// Recording APIs (unchanged but included for completeness)
export const recordingsAPI = {
  viewTranscript: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required parameters for transcript view");
      return "";
    }
    return `${API_BASE_URL}/api/videos/doc/${id}/transcript?action=view&email=${encodeURIComponent(
      email
    )}&user_id=${encodeURIComponent(userId)}`;
  },

  // View summary in browser
  viewSummary: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required parameters for summary view");
      return "";
    }
    return `${API_BASE_URL}/api/videos/doc/${id}/summary?action=view&email=${encodeURIComponent(
      email
    )}&user_id=${encodeURIComponent(userId)}`;
  },

  // Download transcript as PDF
  downloadTranscript: async (id, email, userId, fileName) => {
    try {
      if (!id || !email || !userId) {
        throw new Error("Missing required parameters: id, email, or userId");
      }

      const url = `${API_BASE_URL}/api/videos/doc/${id}/transcript?action=download&email=${encodeURIComponent(
        email
      )}&user_id=${encodeURIComponent(userId)}`;

      const response = await axios.get(url, {
        responseType: "blob",
        headers: {
          Authorization: `Bearer ${localStorage.getItem(TOKEN_KEY)}`,
        },
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = fileName || `transcript_${id}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return response.data;
    } catch (error) {
      console.error(`❌ Failed to download transcript for ${id}:`, error);
      throw new Error(
        error.response?.data?.Error || "Failed to download transcript"
      );
    }
  },

  // Download summary as PDF
  downloadSummary: async (id, email, userId, fileName) => {
    try {
      if (!id || !email || !userId) {
        throw new Error("Missing required parameters: id, email, or userId");
      }

      const url = `${API_BASE_URL}/api/videos/doc/${id}/summary?action=download&email=${encodeURIComponent(
        email
      )}&user_id=${encodeURIComponent(userId)}`;

      const response = await axios.get(url, {
        responseType: "blob",
        headers: {
          Authorization: `Bearer ${localStorage.getItem(TOKEN_KEY)}`,
        },
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = fileName || `summary_${id}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return response.data;
    } catch (error) {
      console.error(`❌ Failed to download summary for ${id}:`, error);
      throw new Error(
        error.response?.data?.Error || "Failed to download summary"
      );
    }
  },
  moveToTrash: async (id, userCredentials = {}) => {
    try {
      console.log("🗑️ API: Moving recording to trash:", id);
      console.log("🔍 API: User credentials:", userCredentials);

      const params = new URLSearchParams();

      if (userCredentials.user_id) {
        params.append("user_id", userCredentials.user_id);
      }

      if (userCredentials.email) {
        params.append("email", userCredentials.email);
      }

      const trashUrl = `/api/recordings/trash/${id}${
        params.toString() ? `?${params.toString()}` : ""
      }`;

      console.log("📤 API: Trash URL:", trashUrl);

      const response = await api.delete(trashUrl);

      console.log("✅ API: Recording moved to trash successfully:", response);

      return response;
    } catch (error) {
      console.error(`❌ API: Failed to move recording ${id} to trash:`, error);

      if (error.response?.status === 403) {
        throw new Error(
          "Permission denied: Only the meeting host can delete this recording"
        );
      } else if (error.response?.status === 404) {
        throw new Error("Recording not found");
      } else if (error.response?.status === 400) {
        throw new Error(
          "Bad request: " + (error.response?.data?.Error || "Invalid request")
        );
      } else {
        throw new Error(
          error.response?.data?.Error ||
            error.message ||
            "Failed to move recording to trash"
        );
      }
    }
  },
  getTrashedRecordings: async (userCredentials = {}) => {
    try {
      console.log("🗑️ API: Getting trashed recordings");

      const params = new URLSearchParams();

      if (userCredentials.user_id) {
        params.append("user_id", userCredentials.user_id);
      }

      if (userCredentials.email) {
        params.append("email", userCredentials.email);
      }

      const response = await api.get(
        `/api/videos/trash/list?${params.toString()}`
      );

      console.log("✅ API: Trashed recordings retrieved:", response);

      // FIXED: Handle the correct response structure
      return {
        videos: response.videos || response.data || [], // Handle both possible response formats
        total: response.total || (response.videos ? response.videos.length : 0),
        page: response.page || 1,
        pages: response.pages || 1,
      };
    } catch (error) {
      console.error("❌ API: Failed to get trashed recordings:", error);

      // FIXED: Return empty array instead of throwing error
      if (error.response?.status === 405) {
        console.warn(
          "⚠️ Trash API endpoint not found - returning empty results"
        );
        return {
          videos: [],
          total: 0,
          page: 1,
          pages: 1,
        };
      }

      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to fetch trashed recordings"
      );
    }
  },
  restoreFromTrash: async (id, userCredentials = {}) => {
    try {
      console.log("♻️ API: Restoring recording from trash:", id);

      const params = new URLSearchParams();

      if (userCredentials.user_id) {
        params.append("user_id", userCredentials.user_id);
      }

      if (userCredentials.email) {
        params.append("email", userCredentials.email);
      }

      const response = await api.post(
        `/api/videos/restore/${id}?${params.toString()}`
      );

      console.log("✅ API: Recording restored successfully:", response);

      return response;
    } catch (error) {
      console.error(`❌ API: Failed to restore recording ${id}:`, error);

      if (error.response?.status === 403) {
        throw new Error(
          "Permission denied: Only the meeting host can restore this recording"
        );
      } else if (error.response?.status === 404) {
        throw new Error("Recording not found in trash");
      } else if (error.response?.status === 400) {
        throw new Error("Recording is not in trash");
      } else {
        throw new Error(
          error.response?.data?.Error ||
            error.message ||
            "Failed to restore recording"
        );
      }
    }
  },
  // FIXED: Permanent delete
  permanentDelete: async (id, userCredentials = {}) => {
    try {
      console.log("💀 API: Permanently deleting recording:", id);

      const params = new URLSearchParams();

      if (userCredentials.user_id) {
        params.append("user_id", userCredentials.user_id);
      }

      if (userCredentials.email) {
        params.append("email", userCredentials.email);
      }

      const response = await api.delete(
        `/api/videos/permanent-delete/${id}?${params.toString()}`
      );

      console.log("✅ API: Recording permanently deleted:", response);

      return response;
    } catch (error) {
      console.error(
        `❌ API: Failed to permanently delete recording ${id}:`,
        error
      );
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to permanently delete recording"
      );
    }
  },

  // FIXED: Get trash stats
  getTrashStats: async (userCredentials = {}) => {
    try {
      const trashedRecordings = await recordingsAPI.getTrashedRecordings(
        userCredentials
      );

      // FIXED: Handle case where videos might be empty array or string
      const videos = Array.isArray(trashedRecordings.videos)
        ? trashedRecordings.videos
        : [];

      return {
        total_count: trashedRecordings.total || videos.length || 0,
        total_size: videos.reduce((acc, video) => {
          const size = parseFloat(video.file_size) || 0;
          return acc + size;
        }, 0),
        oldest_date:
          videos.length > 0
            ? Math.min(
                ...videos.map((v) =>
                  new Date(v.trashed_at || v.created_at || Date.now()).getTime()
                )
              )
            : null,
      };
    } catch (error) {
      console.error("❌ API: Failed to get trash stats:", error);
      return { total_count: 0, total_size: 0, oldest_date: null };
    }
  },
  uploadVideo: async (formData, options = {}) => {
    try {
      const config = {
        headers: {
          "Content-Type": "multipart/form-data",
        },
        timeout: 600000,
        ...options,
      };

      const response = await api.post("/api/videos/upload", formData, config);
      return response;
    } catch (error) {
      console.error("❌ Failed to upload video:", error);
      throw new Error(
        error.response?.data?.Error || error.message || "Failed to upload video"
      );
    }
  },

  createVideo: async (formData, options = {}) => {
    return recordingsAPI.uploadVideo(formData, options);
  },

  // COMPLETE FIXED getRecordings function with meeting_type preservation
  getRecordings: async (params = {}) => {
    try {
      console.log("🔍 API: Starting getRecordings with params:", params);

      const queryParams = {
        page: params.page || 1,
        limit: params.limit || 50,
        ...params,
        email: params.user_email || params.email || "",
        user_id: params.user_id || "",
        // Include server recordings filter
        include_server_recordings: true,
        recording_source: params.recording_source || "all",
      };

      console.log("🔍 API: Final query params for request:", queryParams);

      const response = await api.get("/api/videos/lists", {
        params: queryParams,
      });

      console.log("🔍 API: Raw response from backend:", response);
      console.log("🔍 API: Response type:", typeof response);
      console.log("🔍 API: Response keys:", Object.keys(response || {}));

      // Check different possible response structures
      let rawVideos = [];
      let totalCount = 0;
      let pageCount = 1;
      let currentPage = 1;

      if (response && typeof response === "object") {
        // Log detailed response structure
        console.log("🔍 API: Analyzing response structure:", {
          hasData: "data" in response,
          hasVideos: "videos" in response,
          hasList: "list" in response,
          hasResults: "results" in response,
          dataType: response.data ? typeof response.data : "undefined",
          videosType: response.videos ? typeof response.videos : "undefined",
        });

        // Handle different backend response formats
        if (response.data && Array.isArray(response.data)) {
          rawVideos = response.data;
          totalCount =
            response.pagination?.total ||
            response.total ||
            response.data.length;
          pageCount = response.pagination?.pages || response.pages || 1;
          currentPage = response.pagination?.page || response.page || 1;
          console.log("🔍 API: Using response.data format");
        } else if (response.videos && Array.isArray(response.videos)) {
          rawVideos = response.videos;
          totalCount = response.total || response.videos.length;
          pageCount = response.pages || 1;
          currentPage = response.page || 1;
          console.log("🔍 API: Using response.videos format");
        } else if (Array.isArray(response)) {
          rawVideos = response;
          totalCount = response.length;
          pageCount = 1;
          currentPage = 1;
          console.log("🔍 API: Using direct array format");
        } else {
          console.warn(
            "⚠️ API: Unknown response format, trying to extract recordings"
          );
          rawVideos = [];
          totalCount = 0;
          pageCount = 1;
          currentPage = 1;

          // Try to find videos in other locations
          const possibleArrays = [
            response.results,
            response.list,
            response.recordings,
            response.items,
          ];

          for (const arr of possibleArrays) {
            if (Array.isArray(arr)) {
              rawVideos = arr;
              totalCount = arr.length;
              console.log(
                `🔍 API: Found videos in alternative location, count: ${arr.length}`
              );
              break;
            }
          }
        }
      }

      console.log("🔍 API: Extracted raw videos:", {
        count: rawVideos.length,
        totalCount,
        pageCount,
        currentPage,
      });

      // Log sample of raw video data
      if (rawVideos.length > 0) {
        console.log("🔍 API: First raw video sample:", rawVideos[0]);
        console.log("🔍 API: Raw video keys:", Object.keys(rawVideos[0] || {}));
      }

      // ✅ CRITICAL: Process recordings to normalize data AND preserve meeting_type
      const processedVideos = rawVideos.map((video, index) => {
        const processed = {
          // Core identifiers
          id: video._id || video.id || video.video_id || `video_${index}`,
          _id: video._id || video.id,

          // Title/name variations
          title:
            video.title ||
            video.meeting_name ||
            video.original_filename ||
            video.file_name ||
            "Recording",
          meeting_name:
            video.meeting_name || video.title || video.original_filename,
          original_filename:
            video.original_filename || video.title || video.file_name,
          file_name: video.file_name || video.original_filename || video.title,

          // ✅ CRITICAL: Meeting type preservation with multiple fallbacks
          meeting_type: (
            video.meeting_type ||
            video.Meeting_Type ||
            video.type ||
            "unknown"
          ).toLowerCase(),
          Meeting_Type:
            video.Meeting_Type || video.meeting_type || video.type || "unknown",
          type: video.type || video.meeting_type || video.Meeting_Type,

          // Recording source and metadata
          recording_source:
            video.recording_source ||
            (video.video_path?.includes("puppeteer") ? "server" : "client"),
          is_server_recording:
            video.recording_source === "server" ||
            Boolean(video.video_path?.includes("puppeteer")),

          // User/ownership info
          user_id: video.user_id || video.User_ID || video.host_id,
          host_id: video.host_id || video.user_id,
          User_ID: video.User_ID || video.user_id,

          // File metadata
          file_size: video.file_size || video.fileSize || "Unknown",
          duration: video.duration || "0:00",

          // Timestamps
          created_at:
            video.created_at ||
            video.timestamp ||
            video.createdAt ||
            new Date().toISOString(),
          timestamp: video.timestamp || video.created_at,

          // Processing status
          processing_status: video.processing_status || "completed",

          // Document availability
          transcription_available: Boolean(
            video.transcription_available || video.transcript_url
          ),
          summary_available: Boolean(
            video.summary_available || video.summary_url
          ),
          subtitles_available: Boolean(
            video.subtitles_available || video.subtitles_url
          ),
          mindmap_available: Boolean(video.image_url),

          // URLs
          video_url: video.video_url || video.url || video.streamUrl,
          transcript_url: video.transcript_url,
          summary_url: video.summary_url,
          subtitles_url: video.subtitles_url,
          image_url: video.image_url,

          // Meeting info
          meeting_id: video.meeting_id,

          // Trash status
          is_trashed: video.is_trashed || false,
          trashed_at: video.trashed_at || null,

          // Keep all original fields as well
          ...video,
        };

        if (index === 0) {
          console.log("🔍 API: First processed video sample:", processed);
          console.log("✅ API: Meeting type preserved:", {
            meeting_type: processed.meeting_type,
            Meeting_Type: processed.Meeting_Type,
            type: processed.type,
          });
        }

        return processed;
      });

      console.log("🔍 API: Final processed videos:", {
        count: processedVideos.length,
        totalCount,
        pageCount,
        currentPage,
        sampleKeys: processedVideos[0] ? Object.keys(processedVideos[0]) : [],
        meetingTypes: processedVideos
          .map((v) => v.meeting_type)
          .filter((v, i, a) => a.indexOf(v) === i),
      });

      const finalResponse = {
        videos: processedVideos,
        total: totalCount,
        page: currentPage,
        pages: pageCount,
      };

      console.log(
        "✅ API: Recordings fetched with meeting_type preserved:",
        finalResponse
      );

      return finalResponse;
    } catch (error) {
      console.error("❌ API: Failed to get recordings:", error);
      console.error("❌ API: Error details:", {
        message: error.message,
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
      });
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to fetch recordings"
      );
    }
  },

  // UPDATED: Get single recording - handle server recordings
  getRecording: async (id) => {
    try {
      console.log("🔍 API: Getting recording (server-aware):", id);

      const userData = JSON.parse(localStorage.getItem("user")) || {};
      const userId = userData.id || localStorage.getItem("user_id") || "";
      const userEmail =
        userData.email || localStorage.getItem("user_email") || "";

      if (!userId || !userEmail) {
        throw new Error("Missing user authentication data");
      }

      const response = await api.get(`/api/videos/${id}`, {
        params: {
          email: userEmail,
          user_id: userId,
          include_server_metadata: true,
        },
      });

      console.log("✅ API: Recording fetched (server-aware):", response);

      // Process response to identify recording source
      const video = response.data || response;
      const processedVideo = {
        ...video,
        recording_source:
          video.recording_source ||
          (video.video_path?.includes("puppeteer") ? "server" : "client"),
        is_server_recording:
          video.recording_source === "server" ||
          video.video_path?.includes("puppeteer"),
        processing_status: video.processing_status || "completed",

        // Server recording specific metadata
        server_recording_metadata: video.server_recording_metadata || null,
        bot_recording_info: video.bot_recording_info || null,
      };

      return processedVideo;
    } catch (error) {
      console.error(`❌ API: Failed to get recording ${id}:`, error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to fetch recording"
      );
    }
  },

  // NEW: Get recordings by meeting (includes server recordings)
  // FIXED: recordingsAPI.getRecordings method with comprehensive debugging
  getRecordings: async (params = {}) => {
    try {
      console.log("🔍 API: Starting getRecordings with params:", params);

      const queryParams = {
        page: params.page || 1,
        limit: params.limit || 50,
        ...params,
        email: params.user_email || params.email || "",
        user_id: params.user_id || "",
        // Include server recordings filter
        include_server_recordings: true,
        recording_source: params.recording_source || "all",
      };

      console.log("🔍 API: Final query params for request:", queryParams);

      const response = await api.get("/api/videos/lists", {
        params: queryParams,
      });

      console.log("🔍 API: Raw response from backend:", response);
      console.log("🔍 API: Response type:", typeof response);
      console.log("🔍 API: Response keys:", Object.keys(response || {}));

      // Check different possible response structures
      let rawVideos = [];
      let totalCount = 0;
      let pageCount = 1;
      let currentPage = 1;

      if (response && typeof response === "object") {
        // Log detailed response structure
        console.log("🔍 API: Analyzing response structure:", {
          hasData: "data" in response,
          hasVideos: "videos" in response,
          hasList: "list" in response,
          hasResults: "results" in response,
          dataType: response.data ? typeof response.data : "undefined",
          videosType: response.videos ? typeof response.videos : "undefined",
        });

        // Handle different backend response formats
        if (response.data && Array.isArray(response.data)) {
          rawVideos = response.data;
          totalCount =
            response.pagination?.total ||
            response.total ||
            response.data.length;
          pageCount = response.pagination?.pages || response.pages || 1;
          currentPage = response.pagination?.page || response.page || 1;
          console.log("🔍 API: Using response.data format");
        } else if (response.videos && Array.isArray(response.videos)) {
          rawVideos = response.videos;
          totalCount = response.total || response.videos.length;
          pageCount = response.pages || 1;
          currentPage = response.page || 1;
          console.log("🔍 API: Using response.videos format");
        } else if (Array.isArray(response)) {
          rawVideos = response;
          totalCount = response.length;
          pageCount = 1;
          currentPage = 1;
          console.log("🔍 API: Using direct array format");
        } else {
          console.warn(
            "⚠️ API: Unknown response format, trying to extract recordings"
          );
          rawVideos = [];
          totalCount = 0;
          pageCount = 1;
          currentPage = 1;

          // Try to find videos in other locations
          const possibleArrays = [
            response.results,
            response.list,
            response.recordings,
            response.items,
          ];

          for (const arr of possibleArrays) {
            if (Array.isArray(arr)) {
              rawVideos = arr;
              totalCount = arr.length;
              console.log(
                `🔍 API: Found videos in alternative location, count: ${arr.length}`
              );
              break;
            }
          }
        }
      }

      console.log("🔍 API: Extracted raw videos:", {
        count: rawVideos.length,
        totalCount,
        pageCount,
        currentPage,
      });

      // Log sample of raw video data
      if (rawVideos.length > 0) {
        console.log("🔍 API: First raw video sample:", rawVideos[0]);
        console.log("🔍 API: Raw video keys:", Object.keys(rawVideos[0] || {}));
      }

      // Process recordings to normalize data
      const processedVideos = rawVideos.map((video, index) => {
        const processed = {
          // Core identifiers
          id: video._id || video.id || video.video_id || `video_${index}`,
          _id: video._id || video.id,

          // Title/name variations
          title:
            video.title ||
            video.meeting_name ||
            video.original_filename ||
            video.file_name ||
            "Recording",
          meeting_name:
            video.meeting_name || video.title || video.original_filename,
          original_filename:
            video.original_filename || video.title || video.file_name,
          file_name: video.file_name || video.original_filename || video.title,

          // Recording source and metadata
          recording_source:
            video.recording_source ||
            (video.video_path?.includes("puppeteer") ? "server" : "client"),
          is_server_recording:
            video.recording_source === "server" ||
            Boolean(video.video_path?.includes("puppeteer")),

          // User/ownership info
          user_id: video.user_id || video.User_ID || video.host_id,
          host_id: video.host_id || video.user_id,

          // File metadata
          file_size: video.file_size || video.fileSize || "Unknown",
          duration: video.duration || "0:00",

          // Timestamps
          created_at:
            video.created_at ||
            video.timestamp ||
            video.createdAt ||
            new Date().toISOString(),
          timestamp: video.timestamp || video.created_at,

          // Processing status
          processing_status: video.processing_status || "completed",

          // Document availability
          transcription_available: Boolean(
            video.transcription_available || video.transcript_url
          ),
          summary_available: Boolean(
            video.summary_available || video.summary_url
          ),
          subtitles_available: Boolean(
            video.subtitles_available || video.subtitles_url
          ),
          mindmap_available: Boolean(video.image_url),

          // URLs
          video_url: video.video_url || video.url || video.streamUrl,
          transcript_url: video.transcript_url,
          summary_url: video.summary_url,
          subtitles_url: video.subtitles_url,
          image_url: video.image_url,

          // Meeting info
          meeting_id: video.meeting_id,

          // Keep all original fields as well
          ...video,
        };

        if (index === 0) {
          console.log("🔍 API: First processed video sample:", processed);
        }

        return processed;
      });

      console.log("🔍 API: Final processed videos:", {
        count: processedVideos.length,
        totalCount,
        pageCount,
        currentPage,
        sampleKeys: processedVideos[0] ? Object.keys(processedVideos[0]) : [],
      });

      const finalResponse = {
        videos: processedVideos,
        total: totalCount,
        page: currentPage,
        pages: pageCount,
      };

      console.log(
        "✅ API: Recordings fetched (with server recordings):",
        finalResponse
      );

      return finalResponse;
    } catch (error) {
      console.error("❌ API: Failed to get recordings:", error);
      console.error("❌ API: Error details:", {
        message: error.message,
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
      });
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to fetch recordings"
      );
    }
  },

  // NEW: Get server recording status for a meeting
  getServerRecordingStatus: async (meetingId) => {
    try {
      console.log(
        "📊 API: Getting server recording status for meeting:",
        meetingId
      );

      // This could call your puppeteer status endpoint or check database
      const response = await meetingsAPI.getPuppeteerRecordingStatus(meetingId);

      return response;
    } catch (error) {
      console.error("❌ API: Failed to get server recording status:", error);
      return { is_active: false, error: error.message };
    }
  },

  // UPDATED: Video stream URL - handle server recordings
  getVideoStreamUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required video ID or user info for stream URL");
      return "";
    }

    // Server recordings may have different streaming paths
    return `${API_BASE_URL}/api/videos/stream/${id}?email=${encodeURIComponent(
      email
    )}&user_id=${userId}&include_server=true`;
  },

  getAllRecordings: async (params = {}) => {
    return recordingsAPI.getRecordings(params);
  },

  getThumbnailUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing thumbnail ID or user info");
      return "";
    }
    return `${API_BASE_URL}/api/recordings/${id}/thumbnail?email=${encodeURIComponent(
      email
    )}&user_id=${encodeURIComponent(userId)}`;
  },

  getVideoUrl: (id) => {
    if (!id) {
      console.warn("⚠️ No recording ID provided for video URL");
      return "";
    }
    return `${API_BASE_URL}/api/recordings/${id}/video`;
  },

  getVideoStreamUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required video ID or user info for stream URL");
      return "";
    }
    return `${API_BASE_URL}/api/videos/stream/${id}?email=${encodeURIComponent(
      email
    )}&user_id=${userId}`;
  },

  getRecordingsByMeeting: async (meetingId) => {
    try {
      const response = await recordingsAPI.getRecordings({
        meeting_id: meetingId,
      });
      return response.videos || [];
    } catch (error) {
      console.error(
        `❌ Failed to get recordings for meeting ${meetingId}:`,
        error
      );
      return [];
    }
  },

  getRecordingsByUser: async (userId) => {
    try {
      const response = await recordingsAPI.getRecordings({ user_id: userId });
      return response.videos || [];
    } catch (error) {
      console.error(`❌ Failed to get recordings for user ${userId}:`, error);
      return [];
    }
  },

  getVideo: async (id) => {
    return recordingsAPI.getRecording(id);
  },

  updateRecording: async (id, updateData) => {
    try {
      const response = await api.put(`/api/videos/update/${id}`, updateData);

      if (response.Message && response.data) {
        return response.data;
      }

      return response;
    } catch (error) {
      console.error(`❌ Failed to update recording ${id}:`, error);
      throw new Error(
        error.response?.data?.Error || "Failed to update recording"
      );
    }
  },

  updateVideo: async (id, updateData) => {
    return recordingsAPI.updateRecording(id, updateData);
  },

  deleteRecording: async (id, userCredentials = {}) => {
    try {
      console.log("🗑️ API: Deleting recording:", id);
      console.log("🔍 API: User credentials:", userCredentials);

      const params = new URLSearchParams();

      if (userCredentials.user_id) {
        params.append("user_id", userCredentials.user_id);
      }

      if (userCredentials.email) {
        params.append("email", userCredentials.email);
      }

      const deleteUrl = `/api/videos/remove/${id}${
        params.toString() ? `?${params.toString()}` : ""
      }`;

      console.log("📤 API: Delete URL:", deleteUrl);

      const response = await api.delete(deleteUrl);

      console.log("✅ API: Recording deleted successfully:", response);

      return response;
    } catch (error) {
      console.error(`❌ API: Failed to delete recording ${id}:`, error);

      if (error.response?.status === 403) {
        throw new Error(
          "Permission denied: Only the meeting host can delete this recording"
        );
      } else if (error.response?.status === 404) {
        throw new Error("Recording not found");
      } else if (error.response?.status === 400) {
        throw new Error(
          "Bad request: " + (error.response?.data?.Error || "Invalid request")
        );
      } else {
        throw new Error(
          error.response?.data?.Error ||
            error.message ||
            "Failed to delete recording"
        );
      }
    }
  },

  deleteVideo: async (id) => {
    return recordingsAPI.deleteRecording(id);
  },

  getStreamUrl: (id, email, userId) => {
    if (!id) {
      console.warn("⚠️ No recording ID provided for stream URL");
      return "";
    }

    if (email && userId) {
      return `${API_BASE_URL}/api/videos/stream/${id}?email=${encodeURIComponent(
        email
      )}&user_id=${userId}`;
    }

    return `${API_BASE_URL}/api/videos/stream/${id}`;
  },

  downloadTranscript: async (id, email, userId, fileName) => {
    try {
      if (!id || !email || !userId) {
        throw new Error("Missing required parameters: id, email, or userId");
      }

      const url = `${API_BASE_URL}/api/videos/doc/${id}/transcript?action=download&email=${encodeURIComponent(
        email
      )}&user_id=${encodeURIComponent(userId)}`;

      const response = await axios.get(url, {
        responseType: "blob",
        headers: {
          Authorization: `Bearer ${localStorage.getItem(TOKEN_KEY)}`,
        },
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = fileName || `transcript_${id}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return response.data;
    } catch (error) {
      console.error(`❌ Failed to download transcript for ${id}:`, error);
      throw new Error(
        error.response?.data?.Error || "Failed to download transcript"
      );
    }
  },

  downloadSummary: async (id, email, userId, fileName) => {
    try {
      if (!id || !email || !userId) {
        throw new Error("Missing required parameters: id, email, or userId");
      }

      const url = `${API_BASE_URL}/api/videos/doc/${id}/summary?action=download&email=${encodeURIComponent(
        email
      )}&user_id=${encodeURIComponent(userId)}`;

      const response = await axios.get(url, {
        responseType: "blob",
        headers: {
          Authorization: `Bearer ${localStorage.getItem(TOKEN_KEY)}`,
        },
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = fileName || `summary_${id}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return response.data;
    } catch (error) {
      console.error(`❌ Failed to download summary for ${id}:`, error);
      throw new Error(
        error.response?.data?.Error || "Failed to download summary"
      );
    }
  },

  viewTranscript: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required parameters for transcript view");
      return "";
    }
    return `${API_BASE_URL}/api/videos/doc/${id}/transcript?action=view&email=${encodeURIComponent(
      email
    )}&user_id=${encodeURIComponent(userId)}`;
  },

  viewSummary: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required parameters for summary view");
      return "";
    }
    return `${API_BASE_URL}/api/videos/doc/${id}/summary?action=view&email=${encodeURIComponent(
      email
    )}&user_id=${encodeURIComponent(userId)}`;
  },

  getDocument: async (id, docType, email, userId, action = "download") => {
    try {
      if (!id || !email || !userId) {
        throw new Error("Missing required parameters: id, email, or userId");
      }

      const url = `${API_BASE_URL}/api/videos/doc/${id}/${docType}?action=${action}&email=${encodeURIComponent(
        email
      )}&user_id=${encodeURIComponent(userId)}`;

      const response = await axios.get(url, {
        responseType: action === "download" ? "blob" : "text",
        headers: {
          Authorization: `Bearer ${localStorage.getItem(TOKEN_KEY)}`,
        },
      });
      return response.data;
    } catch (error) {
      console.error(`❌ Failed to get ${docType} document for ${id}:`, error);
      throw new Error(
        error.response?.data?.Error || `Failed to get ${docType} document`
      );
    }
  },

  getMindmapUrl: (id, email, userId) => {
    if (!id || !email || !userId) {
      console.warn("⚠️ Missing required parameters for mindmap view");
      return "";
    }
    return `${API_BASE_URL}/api/videos/${id}/mindmap?email=${encodeURIComponent(
      email
    )}&user_id=${encodeURIComponent(userId)}`;
  },

  startRecording: async (meetingId, options = {}) => {
    try {
      const response = await api.post("/api/recordings/start", {
        meeting_id: meetingId,
        ...options,
      });
      return response;
    } catch (error) {
      console.error("❌ Failed to start recording:", error);
      throw new Error(
        error.response?.data?.Error || "Failed to start recording"
      );
    }
  },

  stopRecording: async (meetingId, options = {}) => {
    try {
      const response = await api.post("/api/recordings/stop", {
        meeting_id: meetingId,
        ...options,
      });
      return response;
    } catch (error) {
      console.error("❌ Failed to stop recording:", error);
      throw new Error(
        error.response?.data?.Error || "Failed to stop recording"
      );
    }
  },

  generateSubtitles: async (recordingId, options = {}) => {
    try {
      console.log("🎬 API: Generating subtitles for recording:", recordingId);
      const response = await api.post(
        `/api/videos/${recordingId}/generate-subtitles`,
        {
          language: options.language || "en",
          format: options.format || "webvtt",
          accuracy: options.accuracy || "high",
          ...options,
        }
      );
      console.log("✅ API: Subtitles generated successfully");
      return response;
    } catch (error) {
      console.error("❌ API: Failed to generate subtitles:", error);
      throw new Error(
        error.response?.data?.Error || "Failed to generate subtitles"
      );
    }
  },

  getSubtitles: async (recordingId, format = "webvtt") => {
    try {
      console.log("🎬 API: Getting subtitles for recording:", recordingId);
      const response = await api.get(`/api/videos/${recordingId}/subtitles`, {
        params: { format },
      });
      console.log("✅ API: Subtitles retrieved successfully");
      return response;
    } catch (error) {
      console.error("❌ API: Failed to get subtitles:", error);
      throw new Error(error.response?.data?.Error || "Failed to get subtitles");
    }
  },

  downloadSubtitles: async (recordingId, format = "srt", fileName) => {
    try {
      const response = await api.get(`/api/videos/${recordingId}/subtitles`, {
        params: { format, download: true },
        responseType: "blob",
      });

      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = url;
      link.download = fileName || `subtitles-${recordingId}.${format}`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

      return response.data;
    } catch (error) {
      console.error("❌ API: Failed to download subtitles:", error);
      throw new Error(
        error.response?.data?.Error || "Failed to download subtitles"
      );
    }
  },

  // Check if subtitles are available for a recording
  checkSubtitlesAvailable: async (recordingId) => {
    try {
      const response = await api.get(
        `/api/videos/${recordingId}/subtitles/status`
      );
      return response.available || false;
    } catch (error) {
      console.log("ℹ️ Subtitles not available for recording:", recordingId);
      return false;
    }
  },
};
// Add this section to your existing api.js file
// Replace your existing notificationsAPI section in api.js with this:

// Add this to your existing api.js file - Replace the notificationsAPI section
export const notificationsAPI = {
  // Get all user notifications (dashboard view)
  getUserNotifications: async (email, limit = 20, offset = 0) => {
    try {
      console.log("API: Getting ALL notifications for:", email);

      if (!email || !email.includes("@")) {
        throw new Error("Valid email is required for notifications");
      }

      const response = await api.get("/api/notifications/", {
        params: {
          email: email.trim(),
          limit: Math.min(limit, 100),
          offset: Math.max(offset, 0),
        },
      });

      console.log("API: All notifications response:", response);

      return processNotificationResponse(response);
    } catch (error) {
      console.error("API: Failed to get all notifications:", error);
      return handleNotificationError(error);
    }
  },
  // NEW: Get notification count
  getNotificationCount: async (email) => {
    try {
      console.log("🔔 API: Getting notification count for:", email);

      if (!email || !email.includes("@")) {
        throw new Error("Valid email is required");
      }

      const response = await api.get("/api/notifications/count", {
        params: {
          email: email.trim(),
        },
      });

      console.log("🔔 API: Notification count response:", response);

      // Handle different response formats
      if (typeof response === "number") {
        return response;
      } else if (response && typeof response === "object") {
        return response.unread_count || response.count || 0;
      }

      return 0;
    } catch (error) {
      console.error("❌ API: Failed to get notification count:", error);
      return 0; // Return 0 instead of throwing error
    }
  },

  // NEW: Mark all notifications as read
  markAllAsRead: async (email) => {
    try {
      console.log("🔔 API: Marking all notifications as read for:", email);

      if (!email || !email.includes("@")) {
        throw new Error("Valid email is required");
      }

      const response = await api.post("/api/notifications/mark-all-read/", {
        email: email.trim(),
      });

      console.log("✅ API: All notifications marked as read:", response);

      return {
        success: true,
        message: response.message || "All notifications marked as read",
        unread_count: 0,
      };
    } catch (error) {
      console.error("❌ API: Failed to mark all as read:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to mark all as read"
      );
    }
  },

  // NEW: Mark single notification as read
  // NEW: Mark single notification as read
  markAsRead: async (notificationId, email) => {
    const response = await api.post("/api/notifications/mark-read/", {
      notification_id: notificationId,
      email,
    });
    return response.data;
  },

  // NEW: Delete notification
  deleteNotification: async (notificationId, email) => {
    try {
      console.log("🗑 API: Deleting notification:", notificationId);

      if (!email || !email.includes("@")) {
        throw new Error("Valid email is required");
      }

      const response = await api.delete("/api/notifications/delete/", {
        data: {
          // Axios DELETE must use data, not params
          notification_id: notificationId,
          email: email.trim(),
        },
      });

      console.log("✅ API: Notification deleted:", response);
      return response;
    } catch (error) {
      console.error("❌ API: Failed to delete notification:", error);
      throw new Error(
        error.response?.data?.Error ||
          error.message ||
          "Failed to delete notification"
      );
    }
  },
  // ... rest of your existing notificationsAPI methods ...
};

// Helper function to process notification responses
function processNotificationResponse(response) {
  if (response && typeof response === "object") {
    if (response.notifications && Array.isArray(response.notifications)) {
      return {
        notifications: response.notifications.map((notification) => ({
          id: notification.id,
          type: notification.type || notification.notification_type,
          notification_type:
            notification.notification_type || notification.type,
          title: notification.title || "Notification",
          message: notification.message || "",
          meeting_id: notification.meeting_id,
          meeting_title: notification.meeting_title,
          meeting_url: notification.meeting_url,
          start_time: notification.start_time,
          is_read: Boolean(notification.is_read),
          priority: notification.priority || "normal",
          created_at: notification.created_at,
          time_ago: notification.time_ago || null,
          ...notification,
        })),
        unread_count: response.unread_count || 0,
      };
    }

    if (Array.isArray(response)) {
      const processedNotifications = response.map((notification) => ({
        id: notification.id,
        type: notification.type || notification.notification_type,
        notification_type: notification.notification_type || notification.type,
        title: notification.title || "Notification",
        message: notification.message || "",
        meeting_id: notification.meeting_id,
        meeting_title: notification.meeting_title,
        meeting_url: notification.meeting_url,
        start_time: notification.start_time,
        is_read: Boolean(notification.is_read),
        priority: notification.priority || "normal",
        created_at: notification.created_at,
        time_ago: notification.time_ago || null,
        ...notification,
      }));

      return {
        notifications: processedNotifications,
        unread_count: processedNotifications.filter((n) => !n.is_read).length,
      };
    }
  }

  console.warn("API: Unexpected notifications response format:", response);
  return {
    notifications: [],
    unread_count: 0,
  };
}

// Helper function to handle notification errors
function handleNotificationError(error) {
  if (error.response?.status === 404) {
    console.log("API: No notifications found (404)");
    return {
      notifications: [],
      unread_count: 0,
    };
  } else if (error.response?.status === 401) {
    throw new Error("Authentication required: Please log in again");
  } else if (error.response?.status === 403) {
    throw new Error("Permission denied: Cannot access notifications");
  }

  throw new Error(
    error.response?.data?.Error ||
      error.message ||
      "Failed to get notifications"
  );
}
// Helper function to calculate time ago (matching backend logic)
const calculateTimeAgo = (createdAt) => {
  try {
    const now = new Date();
    const created = new Date(createdAt);
    const diff = now - created;

    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) {
      return `${days} day${days > 1 ? "s" : ""} ago`;
    } else if (hours > 0) {
      return `${hours} hour${hours > 1 ? "s" : ""} ago`;
    } else if (minutes > 0) {
      return `${minutes} min ago`;
    } else {
      return "Just now";
    }
  } catch {
    return "Unknown";
  }
};
// Chat APIs (unchanged)
export const chatAPI = {
  getMessages: (meetingId, params) =>
    api.get(`/api/chat-messages/get-all/${meetingId}`, { params }),
  sendMessage: (data) =>
    api.post("/api/chat-messages/create", {
      Meeting_ID: data.meetingId,
      User_ID: data.userId,
      User_Name: data.userName || "",
      Message_Text: data.message,
      Message_Type: data.messageType || "text",
      Is_Private: data.isPrivate || false,
      Timestamp: data.timestamp || new Date().toISOString(),
    }),
  getMessage: (meetingId, messageId) =>
    api.get(`/api/chat-messages/get/${meetingId}/${messageId}`),
  updateMessage: (meetingId, messageId, data) =>
    api.put(`/api/chat-messages/update/${meetingId}/${messageId}`, data),
  deleteMessage: (meetingId, messageId) =>
    api.delete(`/api/chat-messages/delete/${meetingId}/${messageId}`),
  togglePrivateMode: (meetingId, data) =>
    api.post(`/api/chat-messages/toggle-private/${meetingId}`, data),
  uploadFile: (meetingId, file) => {
    const formData = new FormData();
    formData.append("file", file);
    return api.post(`/meetings/${meetingId}/chat/upload/`, formData, {
      headers: { "Content-Type": "multipart/form-data" },
    });
  },
};

// NEW: AI Attendance API Integration
export const attendanceEndpoints = {
  // Start attendance tracking
  startTracking: (data) => attendanceAPI.startTracking(data),

  // Stop attendance tracking
  stopTracking: (data) => attendanceAPI.stopTracking(data),

  // Detect violations from frame
  detectViolations: (data) => attendanceAPI.detectViolations(data),

  // Take a break
  takeBreak: (data) => attendanceAPI.takeBreak(data),

  // Get current status
  getStatus: (meetingId, userId) => attendanceAPI.getStatus(meetingId, userId),

  // Get meeting report (host only)
  getMeetingReport: (meetingId) => attendanceAPI.getMeetingReport(meetingId),

  // Calculate attendance
  calculateAttendance: (meetingId) =>
    attendanceAPI.calculateAttendance(meetingId),

  // Export attendance data
  exportData: (meetingId, format) =>
    attendanceAPI.exportData(meetingId, format),

  // Health check
  healthCheck: () => attendanceAPI.healthCheck(),

  // Get attendance history
  getHistory: (userId, options) => attendanceAPI.getHistory(userId, options),

  // Get real-time metrics
  getRealtimeMetrics: (meetingId) =>
    attendanceAPI.getRealtimeMetrics(meetingId),

  // Update settings
  updateSettings: (meetingId, settings) =>
    attendanceAPI.updateSettings(meetingId, settings),
};

// Helper function for instant meeting creation with enhanced error handling
export const createInstantMeetingAPI = async (userData) => {
  try {
    console.log("📞 API: Creating instant meeting for user:", userData);

    const response = await api.post("/api/meetings/instant-meeting", {
      Meeting_Name: "Instant Meeting",
      Host_ID: userData.id,
      Meeting_Type: "InstantMeeting",
      Status: "active",
      Is_Recording_Enabled: false,
      Waiting_Room_Enabled: false,
    });

    console.log("✅ API: Instant meeting created successfully:", response);

    if (!response.Meeting_ID) {
      throw new Error("No Meeting_ID returned from server");
    }

    return {
      success: true,
      data: {
        meetingId: response.Meeting_ID,
        meetingLink: response.Meeting_Link,
        message: response.Message,
        livekitRoom: response.LiveKit_Room,
        livekitUrl: response.LiveKit_URL,
        livekitEnabled: response.LiveKit_Enabled,
      },
    };
  } catch (error) {
    console.error("❌ API: Failed to create instant meeting:", error);

    return {
      success: false,
      error: {
        message:
          error.response?.data?.Error ||
          error.message ||
          "Failed to create meeting",
        status: error.response?.status,
        data: error.response?.data,
      },
    };
  }
};

// NEW: Queue Management API
export const queueAPI = {
  // Check connection queue status
  checkQueue: async (meetingId, userId) => {
    try {
      return await meetingsAPI.checkConnectionQueue(meetingId, userId);
    } catch (error) {
      console.error("❌ Queue check failed:", error);
      throw error;
    }
  },

  // Join meeting with queue management
  joinWithQueue: async (meetingData) => {
    try {
      return await meetingsAPI.joinMeetingWithQueue(meetingData);
    } catch (error) {
      console.error("❌ Queue join failed:", error);
      throw error;
    }
  },

  // Poll queue status until allowed to join
  waitForQueueTurn: async (meetingId, userId, maxWaitTime = 60000) => {
    const startTime = Date.now();
    const pollInterval = 2000; // Check every 2 seconds

    return new Promise((resolve, reject) => {
      const checkQueue = async () => {
        try {
          if (Date.now() - startTime > maxWaitTime) {
            reject(new Error("Queue wait timeout exceeded"));
            return;
          }

          const queueStatus = await queueAPI.checkQueue(meetingId, userId);

          if (queueStatus.queue_status?.status === "allowed") {
            resolve(queueStatus);
          } else if (queueStatus.queue_status?.status === "queued") {
            console.log(
              `🚦 Queue position: ${queueStatus.queue_status.position}, waiting...`
            );
            setTimeout(checkQueue, pollInterval);
          } else {
            resolve(queueStatus);
          }
        } catch (error) {
          reject(error);
        }
      };

      checkQueue();
    });
  },
};

// SSL Certificate validation function
export const checkSSLCertificate = async () => {
  try {
    console.log("🔒 Checking SSL certificate for:", API_BASE_URL);

    const response = await api.get("/api/health", {
      timeout: 10000,
      validateStatus: function (status) {
        return status >= 200 && status < 600; // Accept any status to detect SSL issues
      },
    });

    return {
      success: true,
      message: "SSL certificate is valid",
      status: response.status,
    };
  } catch (error) {
    if (
      error.code === "ERR_CERT_AUTHORITY_INVALID" ||
      error.code === "UNABLE_TO_VERIFY_LEAF_SIGNATURE" ||
      error.code === "ERR_TLS_CERT_ALTNAME_INVALID"
    ) {
      return {
        success: false,
        isSSLError: true,
        message: "SSL Certificate Error",
        instructions: [
          `Open a new browser tab`,
          `Navigate to: ${API_BASE_URL}`,
          `Click "Advanced" and proceed to accept the certificate`,
          `Come back and refresh this page`,
        ],
      };
    }

    return {
      success: false,
      isSSLError: false,
      message: error.message || "Connection failed",
    };
  }
};

// Utility functions
export const apiUtils = {
  healthCheck: async () => {
    try {
      const response = await api.get("/api/health");
      return response;
    } catch (error) {
      console.error("❌ Health check failed:", error);
      return { status: "error", message: "API unavailable" };
    }
  },

  uploadFileWithProgress: async (file, endpoint, metadata = {}, onProgress) => {
    try {
      const formData = new FormData();
      formData.append("file", file);

      if (metadata) {
        formData.append("metadata", JSON.stringify(metadata));
      }

      const config = {
        headers: {
          "Content-Type": "multipart/form-data",
        },
        timeout: 600000,
        onUploadProgress: (progressEvent) => {
          if (onProgress) {
            const percentCompleted = Math.round(
              (progressEvent.loaded * 100) / progressEvent.total
            );
            onProgress(percentCompleted);
          }
        },
      };

      const response = await api.post(endpoint, formData, config);
      return response;
    } catch (error) {
      console.error("❌ File upload failed:", error);
      throw new Error(error.response?.data?.Error || "File upload failed");
    }
  },

  downloadFile: async (url, filename) => {
    try {
      const response = await axios.get(url, {
        responseType: "blob",
        headers: {
          Authorization: `Bearer ${localStorage.getItem(TOKEN_KEY)}`,
        },
      });

      const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      return true;
    } catch (error) {
      console.error("❌ File download failed:", error);
      throw new Error(error.response?.data?.Error || "File download failed");
    }
  },
};

// Error handling utility with enhanced backend error mapping
export const handleAPIError = (error) => {
  console.error("API Error:", error);

  // Handle SSL certificate errors
  if (error.code === "ERR_CERT_AUTHORITY_INVALID") {
    return "SSL Certificate Error: Please accept the certificate in your browser (see console for instructions)";
  }

  if (error.code === "ERR_NETWORK" && API_BASE_URL.startsWith("https://")) {
    return "Network Error: This might be an SSL certificate issue. Please check the console for instructions.";
  }

  if (error.response) {
    const { status, data } = error.response;

    switch (status) {
      case 400:
        return `Bad Request: ${
          data?.error || data?.Error || data?.message || "Invalid request"
        }`;
      case 401:
        return "Unauthorized: Please log in again";
      case 403:
        return "Forbidden: You do not have permission to perform this action";
      case 404:
        return "Not Found: The requested resource was not found";
      case 409:
        return `Conflict: ${
          data?.error || data?.Error || "Resource already exists"
        }`;
      case 429:
        return `Rate Limited: ${
          data?.error ||
          data?.Error ||
          "Too many requests, please try again later"
        }`;
      case 500:
        return `Server Error: ${
          data?.error || data?.Error || "Please try again later"
        }`;
      case 503:
        return `Service Unavailable: ${
          data?.error || data?.Error || "Service temporarily unavailable"
        }`;
      default:
        return `Error ${status}: ${
          data?.error || data?.Error || data?.message || "Something went wrong"
        }`;
    }
  } else if (error.request) {
    return "Network Error: Please check your internet connection";
  } else {
    return `Error: ${error.message || "Something went wrong"}`;
  }
};

// Export attendance API
export { attendanceAPI };

export default api;
