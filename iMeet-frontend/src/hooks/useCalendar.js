// ULTIMATE SOLUTION: Enhanced useCalendar.js with participant chunking for large lists

import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useAuth } from "./useAuth";

const createAxiosInstance = () => {
  const instance = axios.create({
    timeout: 120000, // 2 minutes
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-cache",
    },
  });

  instance.interceptors.request.use(
    (config) => {
      console.log(
        `📤 API Request: ${config.method?.toUpperCase()} ${config.url}` 
      );
      return config;
    },
    (error) => Promise.reject(error)
  );

  instance.interceptors.response.use(
    (response) => {
      console.log(
        `✅ API Response: ${response.config.method?.toUpperCase()} ${
          response.config.url
        } - Status: ${response.status}`
      );
      return response;
    },
    (error) => {
      console.error(
        `❌ API Error: ${error.config?.method?.toUpperCase()} ${
          error.config?.url
        } - ${error.message}`
      );
      return Promise.reject(error);
    }
  );

  return instance;
};

export const useCalendar = () => {
  const [events, setEvents] = useState([]);
  const [selectedDate, setSelectedDate] = useState(new Date());
  const [viewMode, setViewMode] = useState("month");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [calendarSettings, setCalendarSettings] = useState({
    defaultView: "month",
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    workingHours: { start: "09:00", end: "17:00" },
    workingDays: [1, 2, 3, 4, 5],
    reminderMinutes: [15, 30],
    allowOverlapping: false,
  });
  const [connectedCalendars, setConnectedCalendars] = useState([]);
  const [availabilitySlots, setAvailabilitySlots] = useState([]);

  const { user } = useAuth();
  const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL || "https://192.168.48.201:8111";

  const apiClient = createAxiosInstance();

  // ENHANCED: Participant chunking for large lists
  const PARTICIPANT_CHUNK_SIZE = 10; // Process 10 participants at a time
  const MAX_INITIAL_PARTICIPANTS = 20; // Include max 20 in initial creation

  const retryRequest = async (requestFn, maxRetries = 3, delay = 2000) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        console.log(`🔄 Attempt ${attempt}/${maxRetries}`);
        const result = await requestFn();
        return result;
      } catch (error) {
        console.error(`❌ Attempt ${attempt} failed:`, error.message);

        if (attempt === maxRetries) {
          throw error;
        }

        if (
          error.response?.status === 400 ||
          error.response?.status === 401 ||
          error.response?.status === 403
        ) {
          throw error;
        }

        await new Promise((resolve) => setTimeout(resolve, delay * attempt));
      }
    }
  };

  const backendHealthCheck = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/api/health`, {
        timeout: 5000,
      });
      console.log("✅ Backend health check passed");
      return true;
    } catch (error) {
      console.warn("⚠️ Backend health check failed:", error.message);
      return false;
    }
  };

  // ENHANCED: Smart participant processing
  const processParticipants = (participants) => {
    if (!participants || !Array.isArray(participants)) {
      return { initial: [], deferred: [] };
    }

    // If participants <= MAX_INITIAL_PARTICIPANTS, include all in initial creation
    if (participants.length <= MAX_INITIAL_PARTICIPANTS) {
      return {
        initial: participants,
        deferred: [],
      };
    }

    // Otherwise, split them
    return {
      initial: participants.slice(0, MAX_INITIAL_PARTICIPANTS),
      deferred: participants.slice(MAX_INITIAL_PARTICIPANTS),
    };
  };

  // ENHANCED: Add participants to existing meeting
  const addParticipantsToMeeting = async (
    meetingId,
    participants,
    progressCallback
  ) => {
    if (!participants || participants.length === 0) {
      return { success: true, added: 0 };
    }

    console.log(
      `📧 Adding ${participants.length} participants to meeting ${meetingId}`
    );

    const chunks = [];
    for (let i = 0; i < participants.length; i += PARTICIPANT_CHUNK_SIZE) {
      chunks.push(participants.slice(i, i + PARTICIPANT_CHUNK_SIZE));
    }

    let totalAdded = 0;
    let errors = [];

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const progress = Math.round(((i + 1) / chunks.length) * 100);

      if (progressCallback) {
        progressCallback(
          progress,
          `Adding participants ${i * PARTICIPANT_CHUNK_SIZE + 1}-${Math.min(
            (i + 1) * PARTICIPANT_CHUNK_SIZE,
            participants.length
          )}`
        );
      }

      try {
        console.log(`📧 Processing chunk ${i + 1}/${chunks.length}:`, chunk);

        const response = await apiClient.post(
          `${API_BASE_URL}/api/meetings/${meetingId}/participants`,
          {
            participants: chunk.map((email) => ({
              email,
              name: email.split("@")[0],
            })),
          },
          {
            timeout: 30000, // 30 seconds per chunk
          }
        );

        totalAdded += chunk.length;
        console.log(`✅ Chunk ${i + 1} added successfully`);

        // Small delay between chunks to avoid overwhelming the server
        if (i < chunks.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      } catch (error) {
        console.error(`❌ Error adding chunk ${i + 1}:`, error);
        errors.push(`Chunk ${i + 1}: ${error.message}`);

        // Continue with next chunks even if one fails
      }
    }

    return {
      success: totalAdded > 0,
      added: totalAdded,
      total: participants.length,
      errors: errors,
    };
  };

  useEffect(() => {
    if (user) {
      loadCalendarData();
    }
  }, [user, selectedDate, viewMode]);

  const loadCalendarData = async () => {
    try {
      if (events.length === 0) {
        setLoading(true);
      } else {
        console.log("🕓 Refreshing calendar silently (no UI flicker)");
      }
      setLoading(true);
      setError(null);

      const startDate = new Date(getViewStartDate());
      const endDate = new Date(getViewEndDate());
      startDate.setDate(startDate.getDate() - 1);
      endDate.setDate(endDate.getDate() + 1);
      console.log("📅 Loading calendar data for user:", user?.id, user?.email);

      const endpoint = `/api/meetings/user-calendar-meetings`;

      const response = await retryRequest(async () => {
        return await apiClient.get(`${API_BASE_URL}${endpoint}`, {
          params: {
            start_date: startDate.toISOString(),
            end_date: endDate.toISOString(),
            user_id: user?.id,
            user_email: user?.email,
          },
          timeout: 30000,
        });
      });

      console.log(
        "✅ Calendar response received:",
        response.data?.length || 0,
        "meetings"
      );

      const transformedEvents = (response.data || []).map((meeting) => ({
        id:
          meeting.ID || meeting.id || meeting.Meeting_ID || meeting.meeting_id,
        meeting_id:
          meeting.ID || meeting.id || meeting.Meeting_ID || meeting.meeting_id,
        title:
          meeting.title ||
          meeting.Meeting_Name ||
          meeting.meeting_name ||
          "Untitled Meeting",
        start_time:
          meeting.start_time || meeting.startTime || meeting.Started_At,
        end_time: meeting.end_time || meeting.endTime || meeting.Ended_At,
        meeting_url:
          meeting.meetingUrl || meeting.meeting_url || meeting.Meeting_Link,
        location: meeting.location || "",
        host: meeting.Host_ID || meeting.host_id || meeting.host,
        host_id: meeting.Host_ID || meeting.host_id || meeting.host,
        attendees: meeting.attendee_emails || meeting.attendees || [],
        guest_emails: meeting.guest_emails || meeting.guestEmails || [],
        duration: meeting.duration || meeting.Duration_Minutes || 60,
        status: meeting.Status || meeting.status || "scheduled",
        type: "calendar",
        is_host: meeting.is_host || meeting.Is_Host || false,
        user_role: meeting.user_role || "participant",
        provider: meeting.provider || "internal",
      }));

      setEvents(transformedEvents);
    } catch (error) {
      console.error("❌ Load calendar data error:", error);
      setError(`Failed to load calendar: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const getViewStartDate = () => {
    const date = new Date(selectedDate);
    switch (viewMode) {
      case "month":
        date.setDate(1);
        date.setDate(date.getDate() - date.getDay());
        break;
      case "week":
        date.setDate(date.getDate() - date.getDay());
        break;
      case "day":
        break;
    }
    date.setHours(0, 0, 0, 0);
    return date;
  };

  const getViewEndDate = () => {
    const date = new Date(selectedDate);
    switch (viewMode) {
      case "month":
        date.setMonth(date.getMonth() + 1, 0);
        date.setDate(date.getDate() + (6 - date.getDay()));
        break;
      case "week":
        date.setDate(date.getDate() - date.getDay() + 6);
        break;
      case "day":
        break;
    }
    date.setHours(23, 59, 59, 999);
    return date;
  };

  // ENHANCED: Create event with intelligent participant handling
  // CRITICAL FIXES for useCalendar.js
// Replace these functions in your existing useCalendar.js

// ============================================
// REPLACE YOUR createEvent FUNCTION WITH THIS VERSION
// ============================================
const createEvent = async (eventData, progressCallback) => {
  try {
    setLoading(true);
    setError(null);

    console.log("📅 Creating calendar event with data:", eventData);

    // Step 1: Health check
    const isHealthy = await backendHealthCheck();
    if (!isHealthy) {
      console.warn("⚠️ Backend health check failed, proceeding anyway...");
    }

    // Step 2: Process participants intelligently
    const participantEmails =
      eventData.guestEmails || eventData.participants || [];
    const { initial, deferred } = processParticipants(participantEmails);

    console.log(
      `📊 Participant distribution: ${initial.length} initial, ${deferred.length} deferred`
    );

    if (progressCallback) {
      progressCallback(10, "Processing participant list...");
    }

    // Step 3: Create meeting with initial participants only
    const payload = {
      Meeting_Name:
        eventData.title || eventData.meetingTitle || "Calendar Meeting",
      title: eventData.title || eventData.meetingTitle || "Calendar Meeting",
      Started_At:
        eventData.startTime || eventData.start_time || eventData.start,
      startTime:
        eventData.startTime || eventData.start_time || eventData.start,
      Ended_At: eventData.endTime || eventData.end_time || eventData.end,
      endTime: eventData.endTime || eventData.end_time || eventData.end,
      Host_ID: user?.id || eventData.host_id,
      hostId: user?.id || eventData.host_id,
      email: user?.email || eventData.organizer || eventData.email,
      organizer: user?.email || eventData.organizer || eventData.email,

      // OPTIMIZED: Only include initial participants
      guestEmails: initial,
      GuestEmails: initial,
      participants: initial,
      Participants: initial.map((email) => ({
        email: email,
        name: email.split("@")[0],
      })),

      location: eventData.location || "",
      Location: eventData.location || "",
      duration: eventData.duration || eventData.meetingDuration || 60,
      Duration_Minutes: eventData.duration || eventData.meetingDuration || 60,
      ReminderMinutes: eventData.reminderMinutes || [15, 30],
      reminderMinutes: eventData.reminderMinutes || [15, 30],
      Settings: {
        createCalendarEvent:
          eventData.createCalendarEvent !== undefined
            ? eventData.createCalendarEvent
            : true,
        sendInvitations:
          eventData.sendInvitations !== undefined
            ? eventData.sendInvitations
            : false,
        setReminders:
          eventData.setReminders !== undefined
            ? eventData.setReminders
            : false,
        addMeetingLink:
          eventData.addMeetingLink !== undefined
            ? eventData.addMeetingLink
            : true,
      },
      Status: "scheduled",
      Meeting_Type: "CalendarMeeting",
      Is_Recording_Enabled: eventData.recordingEnabled || false,
      Waiting_Room_Enabled: eventData.waitingRoomEnabled || false,
      provider: eventData.provider || "internal",
    };

    console.log(
      "📅 Sending payload to backend (with limited participants):",
      {
        ...payload,
        participantCount: initial.length,
      }
    );

    if (progressCallback) {
      progressCallback(30, "Creating meeting...");
    }

    // Step 4: Create the meeting - ENHANCED ERROR HANDLING
    const response = await retryRequest(async () => {
      return await apiClient.post(
        `${API_BASE_URL}/api/meetings/calendar-meeting`,
        payload,
        {
          timeout: 60000,
        }
      );
    }, 2);

    console.log("✅ Meeting created successfully:", response.data);

    if (progressCallback) {
      progressCallback(
        60,
        "Meeting created! Adding remaining participants..."
      );
    }

    const meetingId = response.data.Meeting_ID;
    let participantResult = {
      success: true,
      added: initial.length,
      total: initial.length,
      errors: [],
    };

    // Step 5: Add deferred participants if any
    if (deferred.length > 0) {
      console.log(`📧 Adding ${deferred.length} deferred participants...`);

      participantResult = await addParticipantsToMeeting(
        meetingId,
        deferred,
        (progress, message) => {
          const overallProgress = 60 + progress * 0.4;
          if (progressCallback) {
            progressCallback(overallProgress, message);
          }
        }
      );
    }

    if (progressCallback) {
      progressCallback(100, "Complete!");
    }

    const newEvent = {
      id: meetingId,
      meeting_id: meetingId,
      title: payload.Meeting_Name,
      start_time: payload.startTime,
      end_time: payload.endTime,
      meeting_url: response.data.Meeting_Link,
      location: payload.location,
      host: payload.Host_ID,
      host_id: payload.Host_ID,
      attendees: participantEmails,
      guest_emails: participantEmails,
      duration: payload.duration,
      status: payload.Status,
      type: "calendar",
      is_host: true,
      user_role: "host",
    };

    setEvents((prev) => [...prev, newEvent]);

    // Return comprehensive result WITHOUT alert()
    const result = {
      success: true,
      event: newEvent,
      meetingLink: response.data.Meeting_Link,
      meetingId: meetingId,
      participantSummary: {
        total: participantEmails.length,
        added: participantResult.added,
        failed: participantResult.total - participantResult.added,
        errors: participantResult.errors,
      },
    };

    // Add warning if not all participants were added
    if (participantResult.errors.length > 0) {
      result.warning = `Meeting created successfully, but ${participantResult.errors.length} participant batches failed to add. You can try adding them manually.`;
    }

    return result;
  }catch (error) {
  console.error("❌ Create event error:", error);

  // CRITICAL: Enhanced error object instead of string message
  let errorResult = {
    success: false,
    error: {
      title: "Error Creating Meeting",
      message: "Failed to create meeting",
      severity: "error",
      details: null,
      code: error.code,
      status: error.response?.status
    }
  };

  if (error.code === "ECONNABORTED") {
    errorResult.error.message =
      "The request timed out. However, the meeting might have been created. Please check your calendar and refresh the page.";
  } else if (error.response?.status === 400) {
    // ✅ CRITICAL FIX: Extract the ACTUAL error message from backend
    const serverMessage = error.response?.data?.Error || 
                         error.response?.data?.error || 
                         error.response?.data?.message;
    
    console.log("🔍 Backend error message:", serverMessage);
    
    if (serverMessage?.includes("already have a scheduled meeting")) {
      errorResult.error.title = "Time Conflict";
      errorResult.error.message = "You already have a meeting scheduled at this time. Please choose a different time slot.";
      errorResult.error.severity = "warning";
      errorResult.error.details = serverMessage;
    } else {
      errorResult.error.title = "Invalid Data";
      errorResult.error.message = serverMessage || "Invalid meeting data. Please check your input.";
      errorResult.error.severity = "warning";
    }
  } else if (error.response?.status === 404) {
    errorResult.error.message =
      "Meeting service endpoint not found. Please contact support.";
  } else if (error.response?.status === 500) {
    errorResult.error.message =
      "Server error occurred. Please try again in a few minutes.";
    errorResult.error.details = error.response?.data?.Error || "Internal server error";
  } else if (error.message.includes("Network Error")) {
    errorResult.error.message =
      "Network connection issue. Please check your internet connection.";
  } else if (error.response?.data?.Error || error.response?.data?.message) {
    errorResult.error.message = error.response.data.Error || error.response.data.message;
  }

  setError(errorResult.error.message);
  
  // Return structured error instead of simple message
  return errorResult;
} 
  finally {
    setLoading(false);
  }
};

  // Rest of the functions remain similar but with retry logic where appropriate...
  const updateEvent = async (eventId, eventData, progressCallback) => {
    try {
      setLoading(true);
      setError(null);

      console.log("📝 Updating meeting:", eventId, eventData);

      if (!eventId) {
        throw new Error("Meeting ID is required for update");
      }

      const participantEmails = Array.isArray(eventData.participants)
        ? eventData.participants
            .filter((p) => {
              if (typeof p === "string") return p.includes("@");
              return p?.email?.includes("@");
            })
            .map((p) => (typeof p === "string" ? p.trim() : p.email.trim()))
        : [];

      const { initial, deferred } = processParticipants(participantEmails);

      if (progressCallback) {
        progressCallback(10, "Preparing update...");
      }

      const payload = {
        Meeting_Name: eventData.title || eventData.meetingTitle,
        Started_At:
          eventData.startTime ||
          eventData.start_time ||
          new Date().toISOString(),
        Ended_At:
          eventData.endTime ||
          eventData.end_time ||
          new Date(Date.now() + 3600000).toISOString(),
        location: eventData.location || "",
        duration: eventData.duration || 60,
        Status: eventData.status || "scheduled",
        guest_emails: initial.length > 0 ? initial : participantEmails,
        guestEmails:
          initial.length > 0 ? initial.join(",") : participantEmails.join(","),
      };

      if (progressCallback) {
        progressCallback(20, "Sending update to server...");
      }

      const response = await retryRequest(async () => {
        return await apiClient.put(
          `${API_BASE_URL}/api/meetings/update/${eventId}`,
          payload,
          {
            timeout: 60000,
            headers: { "Content-Type": "application/json" },
          }
        );
      }, 2);

      console.log("✅ Meeting updated successfully:", response.data);

      if (progressCallback) {
        progressCallback(60, "Meeting updated! Refreshing calendar...");
      }

      let participantResult = {
        success: true,
        added: initial.length,
        total: participantEmails.length,
        errors: [],
      };

      if (deferred.length > 0) {
        console.log(`📧 Adding ${deferred.length} additional participants...`);

        participantResult = await addParticipantsToMeeting(
          eventId,
          deferred,
          (progress, message) => {
            const overallProgress = 60 + progress * 0.4;
            if (progressCallback) {
              progressCallback(overallProgress, message);
            }
          }
        );
      }

      if (progressCallback) {
        progressCallback(100, "Complete!");
      }

      const updatedEvent = {
        id: eventId,
        ID: eventId,
        meeting_id: eventId,
        Meeting_ID: eventId,
        title: payload.Meeting_Name || eventData.title || "Untitled Meeting",
        Meeting_Name:
          payload.Meeting_Name || eventData.title || "Untitled Meeting",
        start_time:
          payload.Started_At || eventData.start_time || eventData.startTime,
        Started_At:
          payload.Started_At || eventData.start_time || eventData.startTime,
        startTime:
          payload.Started_At || eventData.start_time || eventData.startTime,
        end_time: payload.Ended_At || eventData.end_time || eventData.endTime,
        Ended_At: payload.Ended_At || eventData.end_time || eventData.endTime,
        endTime: payload.Ended_At || eventData.end_time || eventData.endTime,
        meeting_url: eventData.meeting_url || eventData.meetingUrl || "",
        location: payload.location || eventData.location || "",
        duration: payload.duration || eventData.duration || 60,
        Duration_Minutes: payload.duration || eventData.duration || 60,
        status: payload.Status || eventData.status || "scheduled",
        Status: payload.Status || eventData.status || "scheduled",
        attendees: participantEmails,
        guestEmails: participantEmails,
        guest_emails: participantEmails,
        participantEmails: participantEmails,
        Participants: participantEmails,
        type: "calendar",
        is_host: true,
        user_role: "host",
        provider: eventData.provider || "internal",
        ...eventData,
      };

      console.log(
        "🔄 CRITICAL: Updating events array in hook state immediately..."
      );
      setEvents((prev) => {
        const updatedList = prev.map((ev) => {
          const isSameEvent =
            String(ev.id || ev.ID || ev.meeting_id || ev.Meeting_ID).trim() ===
              String(eventId).trim() ||
            String(ev.meeting_id).trim() === String(eventId).trim() ||
            String(ev.Meeting_ID).trim() === String(eventId).trim();

          if (isSameEvent) {
            console.log("✅ Found and updating event in array:", eventId);
            return updatedEvent;
          }
          return ev;
        });

        if (
          !updatedList.some(
            (ev) =>
              String(
                ev.id || ev.ID || ev.meeting_id || ev.Meeting_ID
              ).trim() === String(eventId).trim()
          )
        ) {
          console.log("⚠️ Event not found in array, adding it:", eventId);
          updatedList.push(updatedEvent);
        }

        console.log("📊 Events array updated. New count:", updatedList.length);
        return updatedList;
      });

      console.log("✅ Meeting updated and events array refreshed:", eventId);

      return {
        success: true,
        event: updatedEvent,
        participantSummary: {
          total: participantEmails.length,
          added: participantResult.added,
          failed: participantResult.total - participantResult.added,
          errors: participantResult.errors,
        },
      };
    } catch (error) {
      console.error("❌ Update event error:", error);

      let errorMessage = "Failed to update meeting";

      if (error.code === "ECONNABORTED") {
        errorMessage =
          "Request timeout. The meeting might have been updated. Please refresh the page.";
      } else if (error.response?.status === 404) {
        errorMessage = "Meeting not found. It may have been deleted.";
      } else if (error.response?.status === 500) {
        errorMessage = "Server error. Please try again in a few minutes.";
      } else if (error.response?.status === 400) {
        errorMessage = error.response?.data?.Error || "Invalid meeting data.";
      } else if (error.response?.data?.Error || error.response?.data?.message) {
        errorMessage = error.response.data.Error || error.response.data.message;
      }

      setError(errorMessage);
      return { success: false, message: errorMessage };
    } finally {
      setLoading(false);
    }
  };

  const deleteEvent = async (eventId) => {
    try {
      setLoading(true);
      setError(null);

      await retryRequest(async () => {
        return await apiClient.delete(
          `${API_BASE_URL}/api/meetings/delete/${eventId}`
        );
      });

      setEvents((prev) =>
        prev.filter((e) => e.id !== eventId && e.meeting_id !== eventId)
      );

      return { success: true };
    } catch (error) {
      console.error("❌ Delete event error:", error);
      const errorMessage =
        error.response?.data?.Error ||
        error.message ||
        "Failed to delete event";
      setError(errorMessage);
      return { success: false, message: errorMessage };
    } finally {
      setLoading(false);
    }
  };

  // Navigation functions
  const navigateToDate = useCallback((date) => {
    setSelectedDate(new Date(date));
  }, []);

  const navigateToToday = useCallback(() => {
    setSelectedDate(new Date());
  }, []);

  const navigatePrevious = useCallback(() => {
    const newDate = new Date(selectedDate);
    switch (viewMode) {
      case "month":
        newDate.setMonth(newDate.getMonth() - 1);
        break;
      case "week":
        newDate.setDate(newDate.getDate() - 7);
        break;
      case "day":
        newDate.setDate(newDate.getDate() - 1);
        break;
    }
    setSelectedDate(newDate);
  }, [selectedDate, viewMode]);

  const navigateNext = useCallback(() => {
    const newDate = new Date(selectedDate);
    switch (viewMode) {
      case "month":
        newDate.setMonth(newDate.getMonth() + 1);
        break;
      case "week":
        newDate.setDate(newDate.getDate() + 7);
        break;
      case "day":
        newDate.setDate(newDate.getDate() + 1);
        break;
    }
    setSelectedDate(newDate);
  }, [selectedDate, viewMode]);

  return {
    // State
    events,
    selectedDate,
    viewMode,
    loading,
    error,
    calendarSettings,
    connectedCalendars,
    availabilitySlots,

    // Event management
    createEvent,
    updateEvent,
    deleteEvent,

    // Navigation
    navigateToDate,
    navigateToToday,
    navigatePrevious,
    navigateNext,
    setViewMode,

    // Data refresh
    loadCalendarData,

    // Utility
    setError,
    clearError: () => setError(null),

    // Health check
    backendHealthCheck,

    // Participant management
    addParticipantsToMeeting,
  };
};