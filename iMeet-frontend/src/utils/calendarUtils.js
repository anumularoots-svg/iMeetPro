// utils/calendarUtils.js
import { 
  format, 
  startOfMonth, 
  endOfMonth, 
  startOfWeek, 
  endOfWeek, 
  eachDayOfInterval,
  isSameDay,
  isToday,
  addDays,
  addWeeks,
  addMonths,
  subDays,
  subWeeks,
  subMonths
} from 'date-fns';

export const calendarUtils = {
  // Get days in month
  getDaysInMonth: (year, month) => {
    return new Date(year, month + 1, 0).getDate();
  },

  // Get first day of month (0 = Sunday)
  getFirstDayOfMonth: (year, month) => {
    return new Date(year, month, 1).getDay();
  },

  // Generate calendar grid for month view
  generateCalendarGrid: (year, month) => {
    const daysInMonth = calendarUtils.getDaysInMonth(year, month);
    const firstDay = calendarUtils.getFirstDayOfMonth(year, month);
    const grid = [];

    // Previous month's trailing days
    const prevMonth = month === 0 ? 11 : month - 1;
    const prevYear = month === 0 ? year - 1 : year;
    const daysInPrevMonth = calendarUtils.getDaysInMonth(prevYear, prevMonth);

    for (let i = firstDay - 1; i >= 0; i--) {
      grid.push({
        date: new Date(prevYear, prevMonth, daysInPrevMonth - i),
        isCurrentMonth: false,
        isPreviousMonth: true,
      });
    }

    // Current month days
    for (let day = 1; day <= daysInMonth; day++) {
      grid.push({
        date: new Date(year, month, day),
        isCurrentMonth: true,
        isPreviousMonth: false,
        isNextMonth: false,
      });
    }

    // Next month's leading days
    const totalCells = 42; // 6 weeks * 7 days
    const remainingCells = totalCells - grid.length;
    const nextMonth = month === 11 ? 0 : month + 1;
    const nextYear = month === 11 ? year + 1 : year;

    for (let day = 1; day <= remainingCells; day++) {
      grid.push({
        date: new Date(nextYear, nextMonth, day),
        isCurrentMonth: false,
        isNextMonth: true,
      });
    }

    return grid;
  },

  // Get week dates
  getWeekDates: (date) => {
    const startOfWeekDate = startOfWeek(date);
    const weekDates = [];
    
    for (let i = 0; i < 7; i++) {
      const weekDate = addDays(startOfWeekDate, i);
      weekDates.push(weekDate);
    }

    return weekDates;
  },

  // Get month dates for calendar display
  getMonthDates: (date) => {
    const monthStart = startOfMonth(date);
    const monthEnd = endOfMonth(date);
    const calendarStart = startOfWeek(monthStart);
    const calendarEnd = endOfWeek(monthEnd);
    
    return eachDayOfInterval({ start: calendarStart, end: calendarEnd });
  },

  // Check if two dates are same day
  isSameDay: (date1, date2) => {
    return isSameDay(new Date(date1), new Date(date2));
  },

  // Check if date is today
  isToday: (date) => {
    return isToday(new Date(date));
  },

  // Get events for specific date
  getEventsForDate: (events, date) => {
    if (!events || !Array.isArray(events)) return [];
    
    return events.filter(event => {
      if (!event.start_time && !event.startTime) return false;
      
      try {
        const eventDate = new Date(event.start_time || event.startTime);
        return calendarUtils.isSameDay(eventDate, date);
      } catch (error) {
        console.error('Error parsing event date:', event);
        return false;
      }
    });
  },

  // Check if date has events
  hasEvents: (events, date) => {
    return calendarUtils.getEventsForDate(events, date).length > 0;
  },

  // Format time for display
  formatTime: (timeString, format12Hour = true) => {
    try {
      const date = new Date(timeString);
      return format(date, format12Hour ? 'h:mm a' : 'HH:mm');
    } catch (error) {
      console.error('Error formatting time:', timeString);
      return format12Hour ? '12:00 AM' : '00:00';
    }
  },

  // Format date for display
  formatDate: (dateString, formatString = 'MMM dd, yyyy') => {
    try {
      const date = new Date(dateString);
      return format(date, formatString);
    } catch (error) {
      console.error('Error formatting date:', dateString);
      return 'Invalid Date';
    }
  },

  // Get time slots for a day
  getTimeSlots: (startHour = 6, endHour = 22, intervalMinutes = 60) => {
    const slots = [];
    
    for (let hour = startHour; hour < endHour; hour += intervalMinutes / 60) {
      const time = new Date();
      time.setHours(Math.floor(hour), (hour % 1) * 60, 0, 0);
      
      slots.push({
        time: time,
        hour: Math.floor(hour),
        minute: (hour % 1) * 60,
        displayTime: format(time, 'h:mm a'),
        value: format(time, 'HH:mm')
      });
    }
    
    return slots;
  },

  // Navigate calendar dates
  navigateDate: (currentDate, direction, viewMode) => {
    const date = new Date(currentDate);
    
    switch (viewMode) {
      case 'month':
        return direction === 'next' ? addMonths(date, 1) : subMonths(date, 1);
      case 'week':
        return direction === 'next' ? addWeeks(date, 1) : subWeeks(date, 1);
      case 'day':
        return direction === 'next' ? addDays(date, 1) : subDays(date, 1);
      default:
        return date;
    }
  },

  // Get view title
  getViewTitle: (date, viewMode) => {
    switch (viewMode) {
      case 'month':
        return format(date, 'MMMM yyyy');
      case 'week':
        const weekStart = startOfWeek(date);
        const weekEnd = endOfWeek(date);
        return `${format(weekStart, 'MMM dd')} - ${format(weekEnd, 'MMM dd, yyyy')}`;
      case 'day':
        return format(date, 'EEEE, MMMM dd, yyyy');
      default:
        return '';
    }
  },

  // Check for event conflicts
  checkEventConflicts: (newEvent, existingEvents) => {
    const newStart = new Date(newEvent.startTime || newEvent.start_time);
    const newEnd = new Date(newEvent.endTime || newEvent.end_time);
    
    return existingEvents.filter(event => {
      const eventStart = new Date(event.startTime || event.start_time);
      const eventEnd = new Date(event.endTime || event.end_time);
      
      return (newStart < eventEnd && newEnd > eventStart);
    });
  },

  // Get available time slots
  getAvailableSlots: (date, events, workingHours = { start: 9, end: 17 }, slotDuration = 60) => {
    const slots = [];
    const dayEvents = calendarUtils.getEventsForDate(events, date);
    
    for (let hour = workingHours.start; hour < workingHours.end; hour++) {
      const slotStart = new Date(date);
      slotStart.setHours(hour, 0, 0, 0);
      
      const slotEnd = new Date(slotStart);
      slotEnd.setMinutes(slotEnd.getMinutes() + slotDuration);
      
      // Check if slot conflicts with existing events
      const hasConflict = dayEvents.some(event => {
        const eventStart = new Date(event.startTime || event.start_time);
        const eventEnd = new Date(event.endTime || event.end_time);
        
        return (slotStart < eventEnd && slotEnd > eventStart);
      });
      
      if (!hasConflict) {
        slots.push({
          start: slotStart,
          end: slotEnd,
          available: true,
          displayTime: format(slotStart, 'h:mm a')
        });
      }
    }
    
    return slots;
  },

  // Transform meeting data for calendar display
  transformMeetingData: (meetings) => {
    if (!meetings || !Array.isArray(meetings)) return [];
    
    return meetings.map(meeting => ({
      id: meeting.id || meeting.Meeting_ID,
      title: meeting.title || meeting.Meeting_Name || 'Untitled Meeting',
      startTime: meeting.startTime || meeting.start_time || meeting.Started_At,
      endTime: meeting.endTime || meeting.end_time || meeting.Ended_At,
      organizer: meeting.organizer || meeting.host || meeting.Host_ID || 'Unknown',
      participants: meeting.participants || meeting.attendees?.length || 0,
      location: meeting.location || meeting.Location || '',
      description: meeting.description || meeting.Description || '',
      meetingUrl: meeting.meetingUrl || meeting.meeting_url || meeting.Meeting_Link,
      status: meeting.status || meeting.Status || 'scheduled',
      type: meeting.type || meeting.Meeting_Type || 'meeting',
      color: meeting.color || '#1976d2'
    }));
  },

  // Generate calendar event colors
  getEventColor: (eventType) => {
    const colors = {
      meeting: '#1976d2',
      calendar: '#2e7d32',
      instant: '#ed6c02',
      scheduled: '#9c27b0',
      recurring: '#d32f2f'
    };
    
    return colors[eventType] || colors.meeting;
  },

  // Validate date range
  isValidDateRange: (startDate, endDate) => {
    try {
      const start = new Date(startDate);
      const end = new Date(endDate);
      return start <= end && !isNaN(start.getTime()) && !isNaN(end.getTime());
    } catch (error) {
      return false;
    }
  },

  // Get business days between dates
  getBusinessDays: (startDate, endDate) => {
    const businessDays = [];
    let currentDate = new Date(startDate);
    
    while (currentDate <= endDate) {
      const dayOfWeek = currentDate.getDay();
      if (dayOfWeek !== 0 && dayOfWeek !== 6) { // Not Sunday or Saturday
        businessDays.push(new Date(currentDate));
      }
      currentDate = addDays(currentDate, 1);
    }
    
    return businessDays;
  },

  // Calculate duration between times
  calculateDuration: (startTime, endTime) => {
    try {
      const start = new Date(startTime);
      const end = new Date(endTime);
      const durationMs = end - start;
      const durationMinutes = Math.floor(durationMs / (1000 * 60));
      
      if (durationMinutes < 60) {
        return `${durationMinutes}m`;
      } else {
        const hours = Math.floor(durationMinutes / 60);
        const minutes = durationMinutes % 60;
        return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
      }
    } catch (error) {
      console.error('Error calculating duration:', error);
      return '0m';
    }
  },

  // Get timezone offset
  getTimezoneOffset: (date = new Date()) => {
    return date.getTimezoneOffset();
  },

  // Convert to user's timezone
  toUserTimezone: (date) => {
    return new Date(date);
  },

  // Get relative time (e.g., "in 2 hours", "3 days ago")
  getRelativeTime: (date) => {
    const now = new Date();
    const targetDate = new Date(date);
    const diffMs = targetDate - now;
    const diffMinutes = Math.floor(Math.abs(diffMs) / (1000 * 60));
    const diffHours = Math.floor(diffMinutes / 60);
    const diffDays = Math.floor(diffHours / 24);
    
    if (diffMs < 0) {
      // Past
      if (diffMinutes < 60) return `${diffMinutes}m ago`;
      if (diffHours < 24) return `${diffHours}h ago`;
      return `${diffDays}d ago`;
    } else {
      // Future
      if (diffMinutes < 60) return `in ${diffMinutes}m`;
      if (diffHours < 24) return `in ${diffHours}h`;
      return `in ${diffDays}d`;
    }
  }
};

export default calendarUtils;