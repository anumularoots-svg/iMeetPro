// src/services/calendar.js
import apiService from './api';

class CalendarService {
  constructor() {
    this.providers = {
      GOOGLE: 'google',
      OUTLOOK: 'outlook',
      APPLE: 'apple'
    };
    this.syncedCalendars = new Map();
    this.eventCache = new Map();
    this.lastSyncTime = null;
  }

  // Get supported calendar providers
  getSupportedProviders() {
    return [
      {
        id: this.providers.GOOGLE,
        name: 'Google Calendar',
        icon: 'google',
        color: '#4285f4'
      },
      {
        id: this.providers.OUTLOOK,
        name: 'Microsoft Outlook',
        icon: 'microsoft',
        color: '#0078d4'
      },
      {
        id: this.providers.APPLE,
        name: 'Apple Calendar',
        icon: 'apple',
        color: '#000000'
      }
    ];
  }

  // Authenticate with calendar provider
  async authenticateProvider(provider) {
    try {
      const response = await apiService.post('/calendar/auth', {
        provider
      });
      
      if (response.data.authUrl) {
        // Open popup for OAuth
        const popup = window.open(
          response.data.authUrl,
          'calendar-auth',
          'width=600,height=600,scrollbars=yes,resizable=yes'
        );

        return new Promise((resolve, reject) => {
          const checkAuth = setInterval(() => {
            try {
              if (popup.closed) {
                clearInterval(checkAuth);
                this.checkAuthStatus(provider).then(resolve).catch(reject);
              }
            } catch (error) {
              clearInterval(checkAuth);
              reject(error);
            }
          }, 1000);
        });
      }
      
      return response.data;
    } catch (error) {
      console.error('Calendar authentication error:', error);
      throw error;
    }
  }

  // Check authentication status
  async checkAuthStatus(provider) {
    try {
      const response = await apiService.get(`/calendar/auth-status/${provider}`);
      return response.data;
    } catch (error) {
      console.error('Auth status check error:', error);
      throw error;
    }
  }

  // Get user calendars
  async getUserCalendars(provider) {
    try {
      const response = await apiService.get(`/calendar/calendars/${provider}`);
      this.syncedCalendars.set(provider, response.data.calendars);
      return response.data.calendars;
    } catch (error) {
      console.error('Error fetching calendars:', error);
      throw error;
    }
  }

  // Sync calendar events
  async syncCalendarEvents(provider, calendarId, startDate, endDate) {
    try {
      const response = await apiService.post('/calendar/sync', {
        provider,
        calendarId,
        startDate: startDate.toISOString(),
        endDate: endDate.toISOString()
      });

      const cacheKey = `${provider}-${calendarId}`;
      this.eventCache.set(cacheKey, response.data.events);
      this.lastSyncTime = new Date();

      return response.data.events;
    } catch (error) {
      console.error('Calendar sync error:', error);
      throw error;
    }
  }

  // Create calendar event
  async createCalendarEvent(provider, calendarId, eventData) {
    try {
      const response = await apiService.post('/calendar/events', {
        provider,
        calendarId,
        event: {
          title: eventData.title,
          description: eventData.description,
          startTime: eventData.startTime.toISOString(),
          endTime: eventData.endTime.toISOString(),
          location: eventData.location,
          attendees: eventData.attendees || [],
          reminders: eventData.reminders || [],
          meetingUrl: eventData.meetingUrl
        }
      });

      // Update cache
      const cacheKey = `${provider}-${calendarId}`;
      if (this.eventCache.has(cacheKey)) {
        const events = this.eventCache.get(cacheKey);
        events.push(response.data.event);
        this.eventCache.set(cacheKey, events);
      }

      return response.data.event;
    } catch (error) {
      console.error('Error creating calendar event:', error);
      throw error;
    }
  }

  // Update calendar event
  async updateCalendarEvent(provider, calendarId, eventId, eventData) {
    try {
      const response = await apiService.put(`/calendar/events/${eventId}`, {
        provider,
        calendarId,
        event: eventData
      });

      // Update cache
      const cacheKey = `${provider}-${calendarId}`;
      if (this.eventCache.has(cacheKey)) {
        const events = this.eventCache.get(cacheKey);
        const index = events.findIndex(e => e.id === eventId);
        if (index !== -1) {
          events[index] = response.data.event;
          this.eventCache.set(cacheKey, events);
        }
      }

      return response.data.event;
    } catch (error) {
      console.error('Error updating calendar event:', error);
      throw error;
    }
  }

  // Delete calendar event
  async deleteCalendarEvent(provider, calendarId, eventId) {
    try {
      await apiService.delete(`/calendar/events/${eventId}`, {
        data: { provider, calendarId }
      });

      // Update cache
      const cacheKey = `${provider}-${calendarId}`;
      if (this.eventCache.has(cacheKey)) {
        const events = this.eventCache.get(cacheKey);
        const filteredEvents = events.filter(e => e.id !== eventId);
        this.eventCache.set(cacheKey, filteredEvents);
      }

      return true;
    } catch (error) {
      console.error('Error deleting calendar event:', error);
      throw error;
    }
  }

  // Get available time slots
  async getAvailableTimeSlots(provider, calendarId, date, duration = 60) {
    try {
      const response = await apiService.post('/calendar/available-slots', {
        provider,
        calendarId,
        date: date.toISOString(),
        duration
      });

      return response.data.availableSlots;
    } catch (error) {
      console.error('Error getting available slots:', error);
      throw error;
    }
  }

  // Find meeting time across multiple calendars
  async findMeetingTime(attendees, duration, preferredTimes, dateRange) {
    try {
      const response = await apiService.post('/calendar/find-meeting-time', {
        attendees,
        duration,
        preferredTimes,
        startDate: dateRange.start.toISOString(),
        endDate: dateRange.end.toISOString()
      });

      return response.data.suggestedTimes;
    } catch (error) {
      console.error('Error finding meeting time:', error);
      throw error;
    }
  }

  // Get events for date range
  getEventsForDateRange(provider, calendarId, startDate, endDate) {
    const cacheKey = `${provider}-${calendarId}`;
    const cachedEvents = this.eventCache.get(cacheKey) || [];
    
    return cachedEvents.filter(event => {
      const eventStart = new Date(event.startTime);
      const eventEnd = new Date(event.endTime);
      return eventStart >= startDate && eventEnd <= endDate;
    });
  }

  // Get events for specific date
  getEventsForDate(provider, calendarId, date) {
    const startOfDay = new Date(date);
    startOfDay.setHours(0, 0, 0, 0);
    
    const endOfDay = new Date(date);
    endOfDay.setHours(23, 59, 59, 999);
    
    return this.getEventsForDateRange(provider, calendarId, startOfDay, endOfDay);
  }

  // Check for conflicts
  checkForConflicts(newEvent, existingEvents) {
    const newStart = new Date(newEvent.startTime);
    const newEnd = new Date(newEvent.endTime);
    
    return existingEvents.filter(event => {
      const eventStart = new Date(event.startTime);
      const eventEnd = new Date(event.endTime);
      
      return (newStart < eventEnd && newEnd > eventStart);
    });
  }

  // Format event for display
  formatEventForDisplay(event) {
    return {
      id: event.id,
      title: event.title || 'Untitled Event',
      description: event.description || '',
      start: new Date(event.startTime),
      end: new Date(event.endTime),
      location: event.location || '',
      attendees: event.attendees || [],
      allDay: event.allDay || false,
      color: event.color || '#1976d2',
      meetingUrl: event.meetingUrl || null
    };
  }

  // Get calendar color
  getCalendarColor(provider) {
    const colors = {
      google: '#4285f4',
      outlook: '#0078d4',
      apple: '#007aff'
    };
    return colors[provider] || '#1976d2';
  }

  // Generate iCal format
  generateICalEvent(event) {
    const formatDate = (date) => {
      return date.toISOString().replace(/[-:]/g, '').split('.')[0] + 'Z';
    };

    const ical = [
      'BEGIN:VEVENT',
      `UID:${event.id}@meeting-app.com`,
      `DTSTART:${formatDate(new Date(event.startTime))}`,
      `DTEND:${formatDate(new Date(event.endTime))}`,
      `SUMMARY:${event.title}`,
      `DESCRIPTION:${event.description}`,
      event.location ? `LOCATION:${event.location}` : '',
      event.meetingUrl ? `URL:${event.meetingUrl}` : '',
      'END:VEVENT'
    ].filter(line => line !== '').join('\r\n');

    return ical;
  }

  // Get timezone information
  getTimezoneInfo() {
    return {
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      offset: new Date().getTimezoneOffset(),
      name: new Intl.DateTimeFormat('en', { timeZoneName: 'long' })
        .formatToParts(new Date())
        .find(part => part.type === 'timeZoneName')?.value
    };
  }

  // Convert time between timezones
  convertTimezone(date, fromTimezone, toTimezone) {
    const fromDate = new Date(date.toLocaleString('en-US', { timeZone: fromTimezone }));
    const toDate = new Date(date.toLocaleString('en-US', { timeZone: toTimezone }));
    const offset = toDate.getTime() - fromDate.getTime();
    return new Date(date.getTime() + offset);
  }

  // Clear cache
  clearCache() {
    this.eventCache.clear();
    this.syncedCalendars.clear();
    this.lastSyncTime = null;
  }

  // Disconnect calendar provider
  async disconnectProvider(provider) {
    try {
      await apiService.delete(`/calendar/auth/${provider}`);
      this.syncedCalendars.delete(provider);
      
      // Clear related cache
      const keysToDelete = [];
      this.eventCache.forEach((value, key) => {
        if (key.startsWith(provider)) {
          keysToDelete.push(key);
        }
      });
      keysToDelete.forEach(key => this.eventCache.delete(key));
      
      return true;
    } catch (error) {
      console.error('Error disconnecting calendar:', error);
      throw error;
    }
  }
}

export default new CalendarService();