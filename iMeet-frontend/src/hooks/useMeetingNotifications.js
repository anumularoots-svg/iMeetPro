// src/hooks/useMeetingNotifications.js - Complete Notification Management Hook
import { useState, useCallback } from 'react';

export const useMeetingNotifications = () => {
  // Notification state
  const [notification, setNotification] = useState({
    message: '',
    severity: 'info'
  });
  const [showNotification, setShowNotification] = useState(false);

  // Show notification with message and severity
  const showNotificationMessage = useCallback((message, severity = 'info') => {
    setNotification({ message, severity });
    setShowNotification(true);
  }, []);

  // Hide notification
  const hideNotification = useCallback(() => {
    setShowNotification(false);
  }, []);

  // Auto-hide notification after delay
  const showTimedNotification = useCallback((message, severity = 'info', duration = 4000) => {
    setNotification({ message, severity });
    setShowNotification(true);
    
    setTimeout(() => {
      setShowNotification(false);
    }, duration);
  }, []);

  // Show success notification
  const showSuccess = useCallback((message) => {
    showNotificationMessage(message, 'success');
  }, [showNotificationMessage]);

  // Show error notification
  const showError = useCallback((message) => {
    showNotificationMessage(message, 'error');
  }, [showNotificationMessage]);

  // Show warning notification
  const showWarning = useCallback((message) => {
    showNotificationMessage(message, 'warning');
  }, [showNotificationMessage]);

  // Show info notification
  const showInfo = useCallback((message) => {
    showNotificationMessage(message, 'info');
  }, [showNotificationMessage]);

  return {
    // State
    notification,
    showNotification,
    
    // Setters
    setNotification,
    setShowNotification,
    
    // Handlers
    showNotificationMessage,
    hideNotification,
    showTimedNotification,
    showSuccess,
    showError,
    showWarning,
    showInfo,
  };
};