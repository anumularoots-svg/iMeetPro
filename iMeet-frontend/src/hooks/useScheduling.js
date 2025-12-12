
import { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import { useAuth } from './useAuth';

export const useScheduling = () => {
  const [scheduledMeetings, setScheduledMeetings] = useState([]);
  const [invitations, setInvitations] = useState([]);
  const [timeZones, setTimeZones] = useState([]);
  const [recurrencePatterns, setRecurrencePatterns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [schedulingSettings, setSchedulingSettings] = useState({
    defaultDuration: 60,
    defaultTimeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    defaultReminderMinutes: [15],
    bufferTime: 15,
    maxBookingDays: 60,
    allowWeekends: false,
    workingHours: {
      start: '09:00',
      end: '17:00'
    },
    workingDays: [1, 2, 3, 4, 5] // Monday to Friday
  });

  const { user } = useAuth();
  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'https://192.168.48.201:8111/api';

  useEffect(() => {
    if (user) {
      loadScheduledMeetings();
      loadInvitations();
      loadTimeZones();
      loadRecurrencePatterns();
      loadSchedulingSettings();
    }
  }, [user]);

  const loadScheduledMeetings = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE_URL}/meetings/scheduled`);
      setScheduledMeetings(response.data);
    } catch (error) {
      console.error('Load scheduled meetings error:', error);
      setError('Failed to load scheduled meetings');
    } finally {
      setLoading(false);
    }
  };

  const loadInvitations = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/invitations`);
      setInvitations(response.data);
    } catch (error) {
      console.error('Load invitations error:', error);
    }
  };

  const loadTimeZones = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/scheduling/timezones`);
      setTimeZones(response.data);
    } catch (error) {
      console.error('Load timezones error:', error);
      // Fallback to basic timezone list
      setTimeZones([
        { value: 'UTC', label: 'UTC' },
        { value: 'America/New_York', label: 'Eastern Time' },
        { value: 'America/Chicago', label: 'Central Time' },
        { value: 'America/Denver', label: 'Mountain Time' },
        { value: 'America/Los_Angeles', label: 'Pacific Time' },
        { value: 'Europe/London', label: 'Greenwich Mean Time' },
        { value: 'Europe/Paris', label: 'Central European Time' },
        { value: 'Asia/Tokyo', label: 'Japan Standard Time' },
        { value: 'Asia/Kolkata', label: 'India Standard Time' }
      ]);
    }
  };

  const loadRecurrencePatterns = () => {
    setRecurrencePatterns([
      { value: 'none', label: 'No Recurrence' },
      { value: 'daily', label: 'Daily' },
      { value: 'weekly', label: 'Weekly' },
      { value: 'biweekly', label: 'Every 2 weeks' },
      { value: 'monthly', label: 'Monthly' },
      { value: 'quarterly', label: 'Quarterly' },
      { value: 'yearly', label: 'Yearly' },
      { value: 'custom', label: 'Custom' }
    ]);
  };

  const loadSchedulingSettings = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/scheduling/settings`);
      setSchedulingSettings(prev => ({ ...prev, ...response.data }));
    } catch (error) {
      console.error('Load scheduling settings error:', error);
    }
  };

  const scheduleMeeting = async (meetingData) => {
    try {
      setLoading(true);
      const response = await axios.post(`${API_BASE_URL}/meetings/schedule`, {
        meeting_name: meetingData.title,
        host_id: user.id,
        scheduled_start_time: meetingData.startTime,
        timezone: meetingData.timeZone || schedulingSettings.defaultTimeZone,
        duration: meetingData.duration || schedulingSettings.defaultDuration,
        description: meetingData.description,
        is_recurring: meetingData.recurrence?.type !== 'none',
        recurrence_pattern: meetingData.recurrence,
        is_recording_enabled: meetingData.recordingEnabled || false,
        waiting_room_enabled: meetingData.waitingRoomEnabled || false,
        max_participants: meetingData.maxParticipants,
        guest_emails: meetingData.guestEmails || [],
        reminder_minutes: meetingData.reminderMinutes || schedulingSettings.defaultReminderMinutes
      });

      const newMeeting = response.data;
      setScheduledMeetings(prev => [...prev, newMeeting]);

      // Send invitations if guest emails provided
      if (meetingData.guestEmails && meetingData.guestEmails.length > 0) {
        await sendInvitations(newMeeting.id, meetingData.guestEmails);
      }

      return { success: true, meeting: newMeeting };
    } catch (error) {
      console.error('Schedule meeting error:', error);
      setError(error.response?.data?.message || 'Failed to schedule meeting');
      return { success: false, message: error.response?.data?.message };
    } finally {
      setLoading(false);
    }
  };

  const rescheduleMeeting = async (meetingId, newData) => {
    try {
      setLoading(true);
      const response = await axios.put(`${API_BASE_URL}/meetings/${meetingId}/reschedule`, {
        scheduled_start_time: newData.startTime,
        timezone: newData.timeZone,
        duration: newData.duration,
        send_notifications: newData.sendNotifications !== false
      });

      const updatedMeeting = response.data;
      setScheduledMeetings(prev => 
        prev.map(m => m.id === meetingId ? updatedMeeting : m)
      );

      return { success: true, meeting: updatedMeeting };
    } catch (error) {
      console.error('Reschedule meeting error:', error);
      setError(error.response?.data?.message || 'Failed to reschedule meeting');
      return { success: false, message: error.response?.data?.message };
    } finally {
      setLoading(false);
    }
  };

  const cancelMeeting = async (meetingId, reason = '') => {
    try {
      setLoading(true);
      await axios.delete(`${API_BASE_URL}/meetings/${meetingId}/cancel`, {
        data: { reason, send_notifications: true }
      });

      setScheduledMeetings(prev => prev.filter(m => m.id !== meetingId));

      return { success: true };
    } catch (error) {
      console.error('Cancel meeting error:', error);
      setError(error.response?.data?.message || 'Failed to cancel meeting');
      return { success: false, message: error.response?.data?.message };
    } finally {
      setLoading(false);
    }
  };

  const sendInvitations = async (meetingId, emailList, customMessage = '') => {
    try {
      const response = await axios.post(`${API_BASE_URL}/invitations/send`, {
        meeting_id: meetingId,
        emails: emailList,
        custom_message: customMessage
      });

      const newInvitations = response.data;
      setInvitations(prev => [...prev, ...newInvitations]);

      return { success: true, invitations: newInvitations };
    } catch (error) {
      console.error('Send invitations error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  const resendInvitation = async (invitationId) => {
    try {
      await axios.post(`${API_BASE_URL}/invitations/${invitationId}/resend`);
      return { success: true };
    } catch (error) {
      console.error('Resend invitation error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  const respondToInvitation = async (invitationId, response, message = '') => {
    try {
      const result = await axios.post(`${API_BASE_URL}/invitations/${invitationId}/respond`, {
        rsvp_status: response, // 'accepted', 'declined', 'maybe'
        response_message: message
      });

      setInvitations(prev => 
        prev.map(inv => 
          inv.id === invitationId 
            ? { ...inv, rsvp_status: response, responded_at: new Date().toISOString() }
            : inv
        )
      );

      return { success: true, data: result.data };
    } catch (error) {
      console.error('Respond to invitation error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  const getAvailableTimeSlots = async (date, duration, timeZone) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/scheduling/availability`, {
        params: {
          date: date.toISOString().split('T')[0],
          duration: duration || schedulingSettings.defaultDuration,
          timezone: timeZone || schedulingSettings.defaultTimeZone,
          buffer_time: schedulingSettings.bufferTime
        }
      });

      return { success: true, slots: response.data };
    } catch (error) {
      console.error('Get available time slots error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  const checkAvailability = async (startTime, endTime, participants = []) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/scheduling/check-availability`, {
        start_time: startTime,
        end_time: endTime,
        participants: participants
      });

      return { success: true, availability: response.data };
    } catch (error) {
      console.error('Check availability error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  const suggestMeetingTimes = async (participants, duration, preferences = {}) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/scheduling/suggest-times`, {
        participants: participants,
        duration: duration,
        preferences: {
          preferred_days: preferences.preferredDays || schedulingSettings.workingDays,
          preferred_hours: preferences.preferredHours || schedulingSettings.workingHours,
          timezone: preferences.timeZone || schedulingSettings.defaultTimeZone,
          max_suggestions: preferences.maxSuggestions || 5
        }
      });

      return { success: true, suggestions: response.data };
    } catch (error) {
      console.error('Suggest meeting times error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  const createRecurringMeeting = async (meetingData, recurrencePattern) => {
    try {
      setLoading(true);
      const response = await axios.post(`${API_BASE_URL}/meetings/recurring`, {
        meeting_name: meetingData.title,
        host_id: user.id,
        first_occurrence: meetingData.startTime,
        duration: meetingData.duration,
        timezone: meetingData.timeZone,
        recurrence: {
          type: recurrencePattern.type, // 'daily', 'weekly', 'monthly'
          interval: recurrencePattern.interval || 1,
          days_of_week: recurrencePattern.daysOfWeek, // for weekly
          day_of_month: recurrencePattern.dayOfMonth, // for monthly
          end_date: recurrencePattern.endDate,
          max_occurrences: recurrencePattern.maxOccurrences
        },
        description: meetingData.description,
        guest_emails: meetingData.guestEmails || [],
        is_recording_enabled: meetingData.recordingEnabled || false,
        waiting_room_enabled: meetingData.waitingRoomEnabled || false
      });

      const recurringMeetings = response.data;
      setScheduledMeetings(prev => [...prev, ...recurringMeetings]);

      return { success: true, meetings: recurringMeetings };
    } catch (error) {
      console.error('Create recurring meeting error:', error);
      setError(error.response?.data?.message || 'Failed to create recurring meeting');
      return { success: false, message: error.response?.data?.message };
    } finally {
      setLoading(false);
    }
  };

  const updateRecurringSeries = async (seriesId, updateData, updateType = 'all') => {
    try {
      setLoading(true);
      const response = await axios.put(`${API_BASE_URL}/meetings/recurring/${seriesId}`, {
        update_data: updateData,
        update_type: updateType // 'this', 'following', 'all'
      });

      const updatedMeetings = response.data;
      setScheduledMeetings(prev => {
        const updated = [...prev];
        updatedMeetings.forEach(updatedMeeting => {
          const index = updated.findIndex(m => m.id === updatedMeeting.id);
          if (index >= 0) {
            updated[index] = updatedMeeting;
          }
        });
        return updated;
      });

      return { success: true, meetings: updatedMeetings };
    } catch (error) {
      console.error('Update recurring series error:', error);
      setError(error.response?.data?.message || 'Failed to update recurring series');
      return { success: false, message: error.response?.data?.message };
    } finally {
      setLoading(false);
    }
  };

  const updateSchedulingSettings = async (newSettings) => {
    try {
      const response = await axios.put(`${API_BASE_URL}/scheduling/settings`, newSettings);
      setSchedulingSettings(prev => ({ ...prev, ...newSettings }));
      return { success: true, settings: response.data };
    } catch (error) {
      console.error('Update scheduling settings error:', error);
      return { success: false, message: error.response?.data?.message };
    }
  };

  // Utility functions
  const getUpcomingMeetings = useCallback((days = 7) => {
    const now = new Date();
    const futureDate = new Date(now.getTime() + days * 24 * 60 * 60 * 1000);
    
    return scheduledMeetings
      .filter(meeting => {
        const meetingDate = new Date(meeting.scheduled_start_time);
        return meetingDate >= now && meetingDate <= futureDate;
      })
      .sort((a, b) => new Date(a.scheduled_start_time) - new Date(b.scheduled_start_time));
  }, [scheduledMeetings]);

  const getMeetingsForDate = useCallback((date) => {
    const targetDate = new Date(date);
    targetDate.setHours(0, 0, 0, 0);
    const nextDate = new Date(targetDate);
    nextDate.setDate(nextDate.getDate() + 1);
    
    return scheduledMeetings.filter(meeting => {
      const meetingDate = new Date(meeting.scheduled_start_time);
      return meetingDate >= targetDate && meetingDate < nextDate;
    });
  }, [scheduledMeetings]);

  const getPendingInvitations = useCallback(() => {
    return invitations.filter(inv => inv.rsvp_status === 'pending');
  }, [invitations]);

  const getInvitationsByMeeting = useCallback((meetingId) => {
    return invitations.filter(inv => inv.meeting_id === meetingId);
  }, [invitations]);

  const isTimeSlotAvailable = useCallback((startTime, endTime) => {
    const start = new Date(startTime);
    const end = new Date(endTime);
    
    return !scheduledMeetings.some(meeting => {
      const meetingStart = new Date(meeting.scheduled_start_time);
      const meetingEnd = new Date(meetingStart.getTime() + meeting.duration * 60000);
      
      return (start < meetingEnd && end > meetingStart);
    });
  }, [scheduledMeetings]);

  const formatTimeZone = (timeZone) => {
    try {
      const now = new Date();
      const formatter = new Intl.DateTimeFormat('en', {
        timeZone: timeZone,
        timeZoneName: 'short'
      });
      return formatter.formatToParts(now).find(part => part.type === 'timeZoneName')?.value || timeZone;
    } catch (error) {
      return timeZone;
    }
  };

  const convertTimeZone = (dateTime, fromTimeZone, toTimeZone) => {
    try {
      const date = new Date(dateTime);
      return new Intl.DateTimeFormat('en-CA', {
        timeZone: toTimeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      }).format(date);
    } catch (error) {
      return dateTime;
    }
  };

  return {
    // State
    scheduledMeetings,
    invitations,
    timeZones,
    recurrencePatterns,
    loading,
    error,
    schedulingSettings,
    
    // Meeting scheduling
    scheduleMeeting,
    rescheduleMeeting,
    cancelMeeting,
    createRecurringMeeting,
    updateRecurringSeries,
    
    // Invitations
    sendInvitations,
    resendInvitation,
    respondToInvitation,
    
    // Availability
    getAvailableTimeSlots,
    checkAvailability,
    suggestMeetingTimes,
    
    // Data utilities
    getUpcomingMeetings,
    getMeetingsForDate,
    getPendingInvitations,
    getInvitationsByMeeting,
    isTimeSlotAvailable,
    
    // Settings
    updateSchedulingSettings,
    
    // Utility functions
    formatTimeZone,
    convertTimeZone,
    
    // Data refresh
    loadScheduledMeetings,
    loadInvitations,
    
    // Error handling
    setError,
    clearError: () => setError(null)
  };
};