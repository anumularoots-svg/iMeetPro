// src/context/NotificationContext.jsx - COMPLETE FIXED VERSION
import React, { createContext, useContext, useState, useCallback, useEffect } from 'react';
import { useNotifications } from '../hooks/useNotifications';

const NotificationContext = createContext();

export const useNotificationContext = () => {
  const context = useContext(NotificationContext);
  if (!context) {
    throw new Error('useNotificationContext must be used within a NotificationProvider');
  }
  return context;
};

export const NotificationProvider = ({ children }) => {
  const [preferences, setPreferences] = useState({
    email: true,
    browser: true,
    sound: true,
    meetingReminders: true,
    participantUpdates: true,
    chatMessages: false,
    reactions: false,
    recordingNotifications: true, // NEW: Recording completion notifications
  });

  const notifications = useNotifications();

  // Load preferences from localStorage on mount
  useEffect(() => {
    try {
      const saved = localStorage.getItem('notificationPreferences');
      if (saved) {
        const parsed = JSON.parse(saved);
        console.log('🔔 Context - Loaded notification preferences:', parsed);
        setPreferences(prev => ({ ...prev, ...parsed }));
      }
    } catch (error) {
      console.error('🔔 Context - Failed to load notification preferences:', error);
    }
  }, []);

  // Update notification preferences
  const updatePreferences = useCallback((newPreferences) => {
    console.log('🔔 Context - Updating preferences:', newPreferences);
    
    const updatedPreferences = { ...preferences, ...newPreferences };
    setPreferences(updatedPreferences);
    
    try {
      localStorage.setItem('notificationPreferences', JSON.stringify(updatedPreferences));
      console.log('🔔 Context - Preferences saved to localStorage');
    } catch (error) {
      console.error('🔔 Context - Failed to save preferences:', error);
    }
  }, [preferences]);

  // Check if notification type is enabled
  const isEnabled = useCallback((type) => {
    const enabled = preferences[type] !== false;
    console.log(`🔔 Context - Checking if ${type} is enabled:`, enabled);
    return enabled;
  }, [preferences]);

  // ENHANCED: Conditional notification with better type mapping
  const notifyIfEnabled = useCallback((type, ...args) => {
    console.log(`🔔 Context - Attempting to notify ${type} with args:`, args);
    
    if (isEnabled(type)) {
      console.log(`🔔 Context - ${type} notifications are enabled, proceeding`);
      
      switch (type) {
        case 'meetingStarted':
          notifications.notifyMeetingStarted(...args);
          break;
        
        case 'participantJoined':
          if (isEnabled('participantUpdates')) {
            notifications.notifyParticipantJoined(...args);
          }
          break;
        
        case 'participantLeft':
          if (isEnabled('participantUpdates')) {
            notifications.notifyParticipantLeft(...args);
          }
          break;
        
        case 'newMessage':
          if (isEnabled('chatMessages')) {
            notifications.notifyNewMessage(...args);
          }
          break;
        
        case 'reaction':
          if (isEnabled('reactions')) {
            notifications.showNotification(...args);
          }
          break;
        
        case 'handRaised':
          notifications.notifyHandRaised(...args);
          break;
        
        case 'recordingStarted':
          notifications.notifyRecordingStarted(...args);
          break;
        
        case 'recordingStopped':
          notifications.notifyRecordingStopped(...args);
          break;
        
        case 'meetingReminder':
          if (isEnabled('meetingReminders')) {
            notifications.notifyMeetingReminder(...args);
          }
          break;
        
        // NEW: Recording completion notification
        case 'recordingCompleted':
        case 'recording_completed':
          if (isEnabled('recordingNotifications')) {
            notifications.notifyRecordingCompleted(...args);
          }
          break;
        
        // NEW: Meeting invitation notification
        case 'meetingInvitation':
        case 'meeting_invitation':
          if (isEnabled('meetingReminders')) {
            notifications.notifyMeetingInvitation(...args);
          }
          break;
        
        // NEW: Meeting created notification (for host)
        case 'meetingCreated':
        case 'meeting_created':
          notifications.notifyMeetingCreated(...args);
          break;
        
        default:
          console.log(`🔔 Context - Unknown notification type: ${type}`);
          notifications.showNotification(...args);
      }
    } else {
      console.log(`🔔 Context - ${type} notifications are disabled, skipping`);
    }
  }, [isEnabled, notifications]);

  // Debug current state
  useEffect(() => {
    console.log('🔔 Context - Current state:', {
      preferences,
      notificationCount: notifications.notifications?.length || 0,
      unreadCount: notifications.unreadCount || 0,
      loading: notifications.loading
    });
  }, [preferences, notifications.notifications, notifications.unreadCount, notifications.loading]);

  const value = {
    preferences,
    updatePreferences,
    isEnabled,
    notifyIfEnabled,
    ...notifications,
  };

  return (
    <NotificationContext.Provider value={value}>
      {children}
    </NotificationContext.Provider>
  );
};