// context/CalendarContext.jsx
import React, { createContext, useContext, useState, useCallback } from 'react';
import axios from 'axios';

const CalendarContext = createContext();

export const useCalendar = () => {
  const context = useContext(CalendarContext);
  if (!context) {
    throw new Error('useCalendar must be used within a CalendarProvider');
  }
  return context;
};

export const CalendarProvider = ({ children }) => {
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedDate, setSelectedDate] = useState(new Date());
  const [viewMode, setViewMode] = useState('month'); // month, week, day
  const [connectedCalendars, setConnectedCalendars] = useState([]);

  // Fetch calendar events
  const fetchEvents = useCallback(async (startDate, endDate) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await axios.get('/api/calendar/events', {
        params: {
          start_date: startDate.toISOString(),
          end_date: endDate.toISOString(),
        },
      });
      
      setEvents(response.data);
      return response.data;
    } catch (err) {
      setError('Failed to fetch calendar events');
      console.error('Calendar fetch error:', err);
      return [];
    } finally {
      setLoading(false);
    }
  }, []);

  // Create calendar event
  const createEvent = useCallback(async (eventData) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await axios.post('/api/calendar/events', eventData);
      const newEvent = response.data;
      
      setEvents(prev => [...prev, newEvent]);
      return newEvent;
    } catch (err) {
      setError('Failed to create calendar event');
      console.error('Calendar create error:', err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  // Update calendar event
  const updateEvent = useCallback(async (eventId, eventData) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await axios.put(`/api/calendar/events/${eventId}`, eventData);
      const updatedEvent = response.data;
      
      setEvents(prev => prev.map(event => 
        event.id === eventId ? updatedEvent : event
      ));
      
      return updatedEvent;
    } catch (err) {
      setError('Failed to update calendar event');
      console.error('Calendar update error:', err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  // Delete calendar event
  const deleteEvent = useCallback(async (eventId) => {
    try {
      setLoading(true);
      setError(null);
      
      await axios.delete(`/api/calendar/events/${eventId}`);
      
      setEvents(prev => prev.filter(event => event.id !== eventId));
    } catch (err) {
      setError('Failed to delete calendar event');
      console.error('Calendar delete error:', err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  // Sync with external calendar
  const syncExternalCalendar = useCallback(async (provider, credentials) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await axios.post('/api/calendar/sync', {
        provider,
        credentials,
      });
      
      const syncedEvents = response.data.events;
      setEvents(prev => [...prev, ...syncedEvents]);
      
      // Update connected calendars
      setConnectedCalendars(prev => [
        ...prev.filter(cal => cal.provider !== provider),
        response.data.calendar,
      ]);
      
      return response.data;
    } catch (err) {
      setError('Failed to sync external calendar');
      console.error('Calendar sync error:', err);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  // Get available time slots
  const getAvailableSlots = useCallback((date, duration = 60) => {
    const dayEvents = events.filter(event => {
      const eventDate = new Date(event.start_time);
      return eventDate.toDateString() === date.toDateString();
    });

    const slots = [];
    const startHour = 9; // 9 AM
    const endHour = 17; // 5 PM

    for (let hour = startHour; hour < endHour; hour++) {
      const slotStart = new Date(date);
      slotStart.setHours(hour, 0, 0, 0);
      
      const slotEnd = new Date(slotStart);
      slotEnd.setMinutes(slotEnd.getMinutes() + duration);

      // Check if slot conflicts with existing events
      const hasConflict = dayEvents.some(event => {
        const eventStart = new Date(event.start_time);
        const eventEnd = new Date(event.end_time);
        
        return (slotStart < eventEnd && slotEnd > eventStart);
      });

      if (!hasConflict) {
        slots.push({
          start: slotStart,
          end: slotEnd,
          available: true,
        });
      }
    }

    return slots;
  }, [events]);

  // Change view mode
  const changeViewMode = useCallback((mode) => {
    setViewMode(mode);
  }, []);

  // Navigate calendar
  const navigateCalendar = useCallback((direction) => {
    setSelectedDate(prev => {
      const newDate = new Date(prev);
      
      switch (viewMode) {
        case 'month':
          newDate.setMonth(newDate.getMonth() + (direction === 'next' ? 1 : -1));
          break;
        case 'week':
          newDate.setDate(newDate.getDate() + (direction === 'next' ? 7 : -7));
          break;
        case 'day':
          newDate.setDate(newDate.getDate() + (direction === 'next' ? 1 : -1));
          break;
        default:
          break;
      }
      
      return newDate;
    });
  }, [viewMode]);

  const value = {
    events,
    loading,
    error,
    selectedDate,
    viewMode,
    connectedCalendars,
    fetchEvents,
    createEvent,
    updateEvent,
    deleteEvent,
    syncExternalCalendar,
    getAvailableSlots,
    changeViewMode,
    navigateCalendar,
    setSelectedDate,
  };

  return (
    <CalendarContext.Provider value={value}>
      {children}
    </CalendarContext.Provider>
  );
};