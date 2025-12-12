// src/hooks/useNotifications.js - COMPLETE FIXED VERSION
import { useState, useEffect, useCallback } from 'react';
import { useSnackbar } from 'notistack';
import { notificationsAPI } from '../services/api';
import { useAuth } from './useAuth';

// Helper function to format relative time
const formatRelativeTime = (dateString) => {
  if (!dateString) return 'Unknown';
  
  try {
    const date = new Date(dateString);
    const now = new Date();
    const diff = now - date;
    
    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) {
      return `${days} day${days > 1 ? 's' : ''} ago`;
    } else if (hours > 0) {
      return `${hours} hour${hours > 1 ? 's' : ''} ago`;
    } else if (minutes > 0) {
      return `${minutes} min ago`;
    } else {
      return 'Just now';
    }
  } catch (error) {
    console.error('Error formatting time:', error);
    return 'Unknown';
  }
};

export const useNotifications = () => {
  const [notifications, setNotifications] = useState([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const { enqueueSnackbar } = useSnackbar();
  const { user } = useAuth();

  // ENHANCED: Fetch notifications with comprehensive error handling
  const fetchNotifications = useCallback(async () => {
    if (!user?.email) {
      setNotifications([]);
      setUnreadCount(0);
      return;
    }
    
    setLoading(true);
    try {
      
      const response = await notificationsAPI.getUserNotifications(user.email, 20, 0);
      
      
      // Handle different response structures
      let notificationsData = [];
      let unreadCountData = 0;
      
      if (response && typeof response === 'object') {
        if (response.notifications && Array.isArray(response.notifications)) {
          notificationsData = response.notifications;
          unreadCountData = response.unread_count || 0;
        } else if (Array.isArray(response)) {
          notificationsData = response;
          unreadCountData = response.filter(n => !n.is_read).length;
        } else if (response.data && Array.isArray(response.data)) {
          notificationsData = response.data;
          unreadCountData = response.unread_count || response.data.filter(n => !n.is_read).length;
        } else {
          console.warn('🔔 Unexpected response format:', response);
          notificationsData = [];
          unreadCountData = 0;
        }
      }
       
      // ENHANCED: Process notifications with proper field mapping
      const processedNotifications = notificationsData.map(notification => {
        const processed = {
          // Core fields
          id: notification.id,
          
          // Map both type and notification_type for compatibility
          type: notification.type || notification.notification_type,
          notification_type: notification.notification_type || notification.type,
          
          // Content fields
          title: notification.title || 'Notification',
          message: notification.message || '',
          
          // Meeting-specific fields
          meeting_id: notification.meeting_id,
          meeting_title: notification.meeting_title,
          meeting_url: notification.meeting_url,
          start_time: notification.start_time,
          
          // Status fields
          is_read: Boolean(notification.is_read),
          priority: notification.priority || 'normal',
          
          // Timestamps
          created_at: notification.created_at,
          
          // Use backend-provided time_ago or calculate it
          time_ago: notification.time_ago || formatRelativeTime(notification.created_at),
          
          // Keep all original fields for debugging
          ...notification
        };
        
        return processed;
      });      
      setNotifications(processedNotifications);
      setUnreadCount(unreadCountData);
      
    } catch (error) {
      console.error('❌ Failed to fetch notifications:', error);
      
      // Better error handling
      if (error.response?.status === 404) {
        setNotifications([]);
        setUnreadCount(0);
      } else if (error.response?.status === 401) {
        enqueueSnackbar('Please log in to view notifications', { variant: 'warning' });
        setNotifications([]);
        setUnreadCount(0);
      } else {
        enqueueSnackbar('Failed to load notifications', { variant: 'error' });
        setNotifications([]);
        setUnreadCount(0);
      }
    } finally {
      setLoading(false);
    }
  }, [user?.email, enqueueSnackbar]);

  // Fetch notification count
  const fetchNotificationCount = useCallback(async () => {
    if (!user?.email) return;
    
    try {
      const response = await notificationsAPI.getNotificationCount(user.email);
      
      
      // Handle different response formats
      let count = 0;
      if (typeof response === 'number') {
        count = response;
      } else if (response && typeof response === 'object') {
        count = response.unread_count || response.count || 0;
      }
      
      setUnreadCount(count);      
    } catch (error) {
      console.error('❌ Failed to fetch notification count:', error);
      // Don't show error to user for count fetch failures
    }
  }, [user?.email]);

  // Mark notification as read with optimistic updates
  const markAsRead = useCallback(async (notificationId) => {
    if (!user?.email || !notificationId) {
      console.warn('🔔 Missing email or notification ID for mark as read');
      return;
    }

    
    // Optimistic update
    const previousNotifications = notifications;
    const previousCount = unreadCount;
    
    setNotifications(prev => 
      prev.map(notif => 
        notif.id === notificationId 
          ? { ...notif, is_read: true }
          : notif
      )
    );
    
    // Only decrement if notification was actually unread
    const notification = notifications.find(n => n.id === notificationId);
    if (notification && !notification.is_read) {
      setUnreadCount(prev => Math.max(0, prev - 1));
    }

    try {
      const response = await notificationsAPI.markAsRead(notificationId, user.email);
      
      // Update count from response if provided
      if (response && typeof response === 'object' && response.unread_count !== undefined) {
        setUnreadCount(response.unread_count);
      }
      
    } catch (error) {
      console.error('❌ Failed to mark notification as read:', error);
      
      // Revert optimistic updates on error
      setNotifications(previousNotifications);
      setUnreadCount(previousCount);
      
      enqueueSnackbar('Failed to mark notification as read', { variant: 'error' });
    }
  }, [user?.email, notifications, unreadCount, enqueueSnackbar]);

  // Mark all notifications as read
  const markAllAsRead = useCallback(async () => {
    if (!user?.email) return;

    
    // Optimistic update
    const previousNotifications = notifications;
    const previousCount = unreadCount;
    
    setNotifications(prev => 
      prev.map(notif => ({ ...notif, is_read: true }))
    );
    setUnreadCount(0);

    try {
      const response = await notificationsAPI.markAllAsRead(user.email);
      
    } catch (error) {
      
      // Revert optimistic updates on error
      setNotifications(previousNotifications);
      setUnreadCount(previousCount);
      
      enqueueSnackbar('Failed to mark all notifications as read', { variant: 'error' });
    }
  }, [user?.email, notifications, unreadCount, enqueueSnackbar]);

  // Delete notification with optimistic updates
  const deleteNotification = useCallback(async (notificationId) => {
    if (!user?.email || !notificationId) return;
    
    // Find the notification to delete
    const notificationToDelete = notifications.find(n => n.id === notificationId);
    
    // Optimistic update
    const previousNotifications = notifications;
    const previousCount = unreadCount;
    
    setNotifications(prev => 
      prev.filter(notif => notif.id !== notificationId)
    );
    
    // Update unread count if the deleted notification was unread
    if (notificationToDelete && !notificationToDelete.is_read) {
      setUnreadCount(prev => Math.max(0, prev - 1));
    }

    try {
      const response = await notificationsAPI.deleteNotification(notificationId, user.email);
      
      // Update count from response if provided
      if (response && response.unread_count !== undefined) {
        setUnreadCount(response.unread_count);
      }
      
    } catch (error) {
      console.error('❌ Failed to delete notification:', error);
      
      // Revert optimistic updates on error
      setNotifications(previousNotifications);
      setUnreadCount(previousCount);
      
      enqueueSnackbar('Failed to delete notification', { variant: 'error' });
    }
  }, [user?.email, notifications, unreadCount, enqueueSnackbar]);

  // Request notification permission
  const requestPermission = useCallback(async () => {
    if ('Notification' in window) {
      const permission = await Notification.requestPermission();
      return permission === 'granted';
    }
    return false;
  }, []);

  // Show browser notification
  const showBrowserNotification = useCallback((title, options = {}) => {
    if (Notification.permission === 'granted') {
      const notification = new Notification(title, {
        icon: '/logo192.png',
        badge: '/logo192.png',
        tag: 'meeting-app',
        renotify: true,
        ...options,
      });

      notification.onclick = () => {
        window.focus();
        notification.close();
      };

      setTimeout(() => notification.close(), 5000);
      return notification;
    }
    return null;
  }, []);
// Add these new methods to your useNotifications hook:

// NEW: Fetch schedule-specific notifications
const fetchScheduleNotifications = useCallback(async () => {
  if (!user?.email) {
    return;
  }
  
  setLoading(true);
  try {
    
    const response = await notificationsAPI.getScheduleNotifications(user.email, 50, 0);
    
    
    setNotifications(response.notifications || []);
    setUnreadCount(response.unread_count || 0);
    
  } catch (error) {    console.log('🔔 Fetching SCHEDULE notifications for user:', user.email);
    console.error('❌ Failed to fetch schedule notifications:', error);
    enqueueSnackbar('Failed to load schedule notifications', { variant: 'error' });
  } finally {
    setLoading(false);
  }
}, [user?.email, enqueueSnackbar]);

// NEW: Fetch calendar-specific notifications
const fetchCalendarNotifications = useCallback(async () => {
  if (!user?.email) {
    return;
  }
  
  setLoading(true);
  try {
    
    const response = await notificationsAPI.getCalendarNotifications(user.email, 50, 0);
    
    
    setNotifications(response.notifications || []);
    setUnreadCount(response.unread_count || 0);
    
  } catch (error) {
    console.error('❌ Failed to fetch calendar notifications:', error);
    enqueueSnackbar('Failed to load calendar notifications', { variant: 'error' });
  } finally {
    setLoading(false);
  }
}, [user?.email, enqueueSnackbar]);

// NEW: Fetch recording-specific notifications
const fetchRecordingNotifications = useCallback(async () => {
  if (!user?.email) {
    return;
  }
  
  setLoading(true);
  try {
    
    const response = await notificationsAPI.getRecordingNotifications(user.email, 50, 0);
    
    
    setNotifications(response.notifications || []);
    setUnreadCount(response.unread_count || 0);
    
  } catch (error) {
    console.error('❌ Failed to fetch recording notifications:', error);
    enqueueSnackbar('Failed to load recording notifications', { variant: 'error' });
  } finally {
    setLoading(false);
  }
}, [user?.email, enqueueSnackbar]);

// Return the new methods at the end of your hook:

  // Show in-app notification (toast)
  const showNotification = useCallback((message, variant = 'info', options = {}) => {
    enqueueSnackbar(message, {
      variant,
      autoHideDuration: 5000,
      ...options,
    });
  }, [enqueueSnackbar]);

  // NEW: Meeting invitation notification
  const notifyMeetingInvitation = useCallback((meetingTitle, hostName, startTime) => {
    showNotification(
      `${hostName} invited you to "${meetingTitle}"`,
      'info',
      { autoHideDuration: 8000 }
    );
    showBrowserNotification('Meeting Invitation', {
      body: `${hostName} invited you to "${meetingTitle}"`,
    });
  }, [showNotification, showBrowserNotification]);

  // NEW: Meeting created notification (for host)
  const notifyMeetingCreated = useCallback((meetingTitle) => {
    showNotification(
      `Meeting "${meetingTitle}" created successfully`,
      'success'
    );
  }, [showNotification]);

  // NEW: Recording completion notification
  const notifyRecordingCompleted = useCallback((meetingTitle) => {
    showNotification(
      `Recording for "${meetingTitle}" is now available`,
      'success',
      { autoHideDuration: 8000 }
    );
    showBrowserNotification('Recording Available', {
      body: `The recording for "${meetingTitle}" has been processed`,
    });
  }, [showNotification, showBrowserNotification]);

  // Meeting specific notifications (existing)
  const notifyMeetingStarted = useCallback((meetingName) => {
    showNotification(`Meeting "${meetingName}" has started`, 'info');
    showBrowserNotification(`Meeting Started`, {
      body: `"${meetingName}" is now live`,
    });
  }, [showNotification, showBrowserNotification]);

  const notifyParticipantJoined = useCallback((participantName) => {
    showNotification(`${participantName} joined the meeting`, 'success');
  }, [showNotification]);

  const notifyParticipantLeft = useCallback((participantName) => {
    showNotification(`${participantName} left the meeting`, 'warning');
  }, [showNotification]);

  const notifyMeetingReminder = useCallback((meetingName, minutesBefore) => {
    showNotification(
      `Meeting "${meetingName}" starts in ${minutesBefore} minutes`, 
      'info',
      { autoHideDuration: 10000 }
    );
    showBrowserNotification(`Meeting Reminder`, {
      body: `"${meetingName}" starts in ${minutesBefore} minutes`,
    });
  }, [showNotification, showBrowserNotification]);

  const notifyRecordingStarted = useCallback(() => {
    showNotification('Recording started', 'info');
  }, [showNotification]);

  const notifyRecordingStopped = useCallback(() => {
    showNotification('Recording stopped', 'info');
  }, [showNotification]);

  const notifyHandRaised = useCallback((participantName) => {
    showNotification(`${participantName} raised their hand`, 'info');
  }, [showNotification]);

  const notifyNewMessage = useCallback((senderName) => {
    showNotification(`New message from ${senderName}`, 'info');
  }, [showNotification]);

  // Handle notification click - navigate to meeting or mark as read
  const handleNotificationClick = useCallback((notification) => {
    
    markAsRead(notification.id);
    
    // Navigate to meeting if it has a meeting_url
    if (notification.meeting_url) {
      window.open(notification.meeting_url, '_blank');
    }
  }, [markAsRead]);

  // Polling setup with better error handling
  useEffect(() => {
  if (user?.email) {
    // Polling every 30 seconds to fetch new notifications
    const interval = setInterval(() => {
      fetchNotifications(); // Call to update notifications regularly
    }, 30000); // Adjust as needed

    return () => clearInterval(interval); // Cleanup on component unmount
  } else {
    // Clear notifications if no user
    setNotifications([]);
    setUnreadCount(0);
  }
}, [user?.email, fetchNotifications]); // Ensure this effect runs whenever the user email changes

  // Request permission on mount
  useEffect(() => {
    requestPermission();
  }, [requestPermission]);

  return {
    notifications,
    unreadCount,
    loading,
    fetchNotifications,
    fetchScheduleNotifications,  // NEW
  fetchCalendarNotifications,  // NEW
  fetchRecordingNotifications, // NEW
    markAsRead,
    markAllAsRead,
    deleteNotification,
    handleNotificationClick,
    showNotification,
    showBrowserNotification,
    requestPermission,
    // Meeting specific notifications
    notifyMeetingStarted,
    notifyParticipantJoined,
    notifyParticipantLeft,
    notifyMeetingReminder,
    notifyRecordingStarted,
    notifyRecordingStopped,
    notifyHandRaised,
    notifyNewMessage,
    // NEW: Additional notification types
    notifyMeetingInvitation,
    notifyMeetingCreated,
    notifyRecordingCompleted,
  };
};