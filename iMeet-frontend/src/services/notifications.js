// src/services/notifications.js
import { NotificationSound, MessageSound, HandRaiseSound } from '../assets/sounds';

class NotificationsService {
  constructor() {
    this.permission = 'default';
    this.soundEnabled = true;
    this.browserNotificationsEnabled = true;
    this.activeNotifications = new Map();
    this.notificationQueue = [];
    this.maxNotifications = 5;
    this.notificationTypes = {
      MEETING_INVITE: 'meeting_invite',
      MEETING_START: 'meeting_start',
      MEETING_END: 'meeting_end',
      USER_JOINED: 'user_joined',
      USER_LEFT: 'user_left',
      CHAT_MESSAGE: 'chat_message',
      HAND_RAISED: 'hand_raised',
      RECORDING_STARTED: 'recording_started',
      RECORDING_STOPPED: 'recording_stopped',
      MEETING_REMINDER: 'meeting_reminder',
      PERMISSION_REQUEST: 'permission_request'
    };
  }

  // Initialize notifications
  async initialize() {
    try {
      this.permission = await this.requestPermission();
      return this.permission === 'granted';
    } catch (error) {
      console.error('Failed to initialize notifications:', error);
      return false;
    }
  }

  // Request notification permission
  async requestPermission() {
    if (!('Notification' in window)) {
      console.warn('Browser does not support notifications');
      return 'denied';
    }

    if (Notification.permission === 'granted') {
      return 'granted';
    }

    if (Notification.permission === 'denied') {
      return 'denied';
    }

    const permission = await Notification.requestPermission();
    this.permission = permission;
    return permission;
  }

  // Show browser notification
  showBrowserNotification(title, options = {}) {
    if (!this.browserNotificationsEnabled || this.permission !== 'granted') {
      return null;
    }

    const defaultOptions = {
      icon: '/logo192.png',
      badge: '/logo192.png',
      requireInteraction: false,
      silent: !this.soundEnabled,
      tag: `meeting-app-${Date.now()}`,
      ...options
    };

    try {
      const notification = new Notification(title, defaultOptions);
      
      // Auto close after 5 seconds if not requiring interaction
      if (!defaultOptions.requireInteraction) {
        setTimeout(() => {
          notification.close();
        }, 5000);
      }

      // Track active notifications
      if (defaultOptions.tag) {
        this.activeNotifications.set(defaultOptions.tag, notification);
      }

      return notification;
    } catch (error) {
      console.error('Failed to show notification:', error);
      return null;
    }
  }

  // Show in-app notification (toast)
  showInAppNotification(type, title, message, options = {}) {
    const notification = {
      id: `notification-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      type,
      title,
      message,
      timestamp: new Date(),
      duration: options.duration || 5000,
      actions: options.actions || [],
      priority: options.priority || 'normal',
      persistent: options.persistent || false
    };

    // Add to queue
    this.notificationQueue.push(notification);

    // Limit queue size
    if (this.notificationQueue.length > this.maxNotifications) {
      this.notificationQueue.shift();
    }

    // Play sound if enabled
    if (this.soundEnabled && options.sound !== false) {
      this.playNotificationSound(type);
    }

    // Dispatch custom event for UI components to listen
    window.dispatchEvent(new CustomEvent('appNotification', {
      detail: notification
    }));

    return notification;
  }

  // Meeting specific notifications
  notifyMeetingInvite(meetingTitle, hostName, startTime) {
    const title = 'Meeting Invitation';
    const message = `${hostName} invited you to "${meetingTitle}"`;
    
    this.showBrowserNotification(title, {
      body: message,
      tag: 'meeting-invite',
      actions: [
        { action: 'accept', title: 'Accept' },
        { action: 'decline', title: 'Decline' }
      ]
    });

    this.showInAppNotification(
      this.notificationTypes.MEETING_INVITE,
      title,
      message,
      {
        actions: [
          { label: 'Accept', action: 'accept', primary: true },
          { label: 'Decline', action: 'decline' }
        ]
      }
    );
  }

  notifyMeetingStarting(meetingTitle, minutesUntil) {
    const title = 'Meeting Starting Soon';
    const message = `"${meetingTitle}" starts in ${minutesUntil} minute${minutesUntil !== 1 ? 's' : ''}`;
    
    this.showBrowserNotification(title, {
      body: message,
      tag: 'meeting-reminder',
      requireInteraction: true
    });

    this.showInAppNotification(
      this.notificationTypes.MEETING_REMINDER,
      title,
      message,
      {
        actions: [
          { label: 'Join Now', action: 'join', primary: true },
          { label: 'Dismiss', action: 'dismiss' }
        ],
        persistent: true
      }
    );
  }

  notifyUserJoined(userName) {
    const message = `${userName} joined the meeting`;
    
    this.showInAppNotification(
      this.notificationTypes.USER_JOINED,
      'Participant Joined',
      message,
      { duration: 3000 }
    );
  }

  notifyUserLeft(userName) {
    const message = `${userName} left the meeting`;
    
    this.showInAppNotification(
      this.notificationTypes.USER_LEFT,
      'Participant Left',
      message,
      { duration: 3000 }
    );
  }

  notifyChatMessage(senderName, messagePreview) {
    const title = 'New Chat Message';
    const message = `${senderName}: ${messagePreview}`;
    
    this.showInAppNotification(
      this.notificationTypes.CHAT_MESSAGE,
      title,
      message,
      {
        duration: 4000,
        actions: [
          { label: 'View', action: 'view-chat', primary: true }
        ]
      }
    );
  }

  notifyHandRaised(userName) {
    const title = 'Hand Raised';
    const message = `${userName} raised their hand`;
    
    this.showBrowserNotification(title, {
      body: message,
      tag: 'hand-raised'
    });

    this.showInAppNotification(
      this.notificationTypes.HAND_RAISED,
      title,
      message,
      {
        actions: [
          { label: 'View Participants', action: 'view-participants', primary: true }
        ]
      }
    );
  }

  notifyRecordingStarted() {
    const title = 'Recording Started';
    const message = 'This meeting is now being recorded';
    
    this.showInAppNotification(
      this.notificationTypes.RECORDING_STARTED,
      title,
      message,
      { duration: 4000, priority: 'high' }
    );
  }

  notifyRecordingStopped() {
    const title = 'Recording Stopped';
    const message = 'Meeting recording has been stopped';
    
    this.showInAppNotification(
      this.notificationTypes.RECORDING_STOPPED,
      title,
      message,
      { duration: 4000 }
    );
  }

  notifyPermissionRequest(requesterName, permission) {
    const title = 'Permission Request';
    const message = `${requesterName} requests ${permission} permission`;
    
    this.showInAppNotification(
      this.notificationTypes.PERMISSION_REQUEST,
      title,
      message,
      {
        actions: [
          { label: 'Grant', action: 'grant', primary: true },
          { label: 'Deny', action: 'deny' }
        ],
        persistent: true
      }
    );
  }

  // Play notification sound
  playNotificationSound(type) {
    if (!this.soundEnabled) return;

    let soundFile;
    switch (type) {
      case this.notificationTypes.CHAT_MESSAGE:
        soundFile = MessageSound;
        break;
      case this.notificationTypes.HAND_RAISED:
        soundFile = HandRaiseSound;
        break;
      default:
        soundFile = NotificationSound;
    }

    if (soundFile) {
      try {
        const audio = new Audio(soundFile);
        audio.volume = 0.5;
        audio.play().catch(e => console.log('Could not play notification sound:', e));
      } catch (error) {
        console.log('Notification sound not available:', error);
      }
    }
  }

  // Get notification history
  getNotificationHistory() {
    return [...this.notificationQueue];
  }

  // Clear specific notification
  clearNotification(notificationId) {
    this.notificationQueue = this.notificationQueue.filter(n => n.id !== notificationId);
    
    window.dispatchEvent(new CustomEvent('clearNotification', {
      detail: { notificationId }
    }));
  }

  // Clear all notifications
  clearAllNotifications() {
    this.notificationQueue = [];
    
    // Close all browser notifications
    this.activeNotifications.forEach(notification => {
      notification.close();
    });
    this.activeNotifications.clear();

    window.dispatchEvent(new CustomEvent('clearAllNotifications'));
  }

  // Toggle sound
  toggleSound() {
    this.soundEnabled = !this.soundEnabled;
    localStorage.setItem('notificationSoundEnabled', this.soundEnabled.toString());
    return this.soundEnabled;
  }

  // Toggle browser notifications
  toggleBrowserNotifications() {
    this.browserNotificationsEnabled = !this.browserNotificationsEnabled;
    localStorage.setItem('browserNotificationsEnabled', this.browserNotificationsEnabled.toString());
    return this.browserNotificationsEnabled;
  }

  // Load settings from localStorage
  loadSettings() {
    const soundEnabled = localStorage.getItem('notificationSoundEnabled');
    const browserEnabled = localStorage.getItem('browserNotificationsEnabled');

    if (soundEnabled !== null) {
      this.soundEnabled = soundEnabled === 'true';
    }

    if (browserEnabled !== null) {
      this.browserNotificationsEnabled = browserEnabled === 'true';
    }
  }

  // Get notification settings
  getSettings() {
    return {
      soundEnabled: this.soundEnabled,
      browserNotificationsEnabled: this.browserNotificationsEnabled,
      permission: this.permission
    };
  }

  // Update notification settings
  updateSettings(settings) {
    if (settings.soundEnabled !== undefined) {
      this.soundEnabled = settings.soundEnabled;
      localStorage.setItem('notificationSoundEnabled', this.soundEnabled.toString());
    }

    if (settings.browserNotificationsEnabled !== undefined) {
      this.browserNotificationsEnabled = settings.browserNotificationsEnabled;
      localStorage.setItem('browserNotificationsEnabled', this.browserNotificationsEnabled.toString());
    }
  }

  // Schedule notification
  scheduleNotification(title, message, scheduledTime, options = {}) {
    const now = new Date().getTime();
    const delay = scheduledTime.getTime() - now;

    if (delay <= 0) {
      this.showBrowserNotification(title, { body: message, ...options });
      return null;
    }

    const timeoutId = setTimeout(() => {
      this.showBrowserNotification(title, { body: message, ...options });
    }, delay);

    return timeoutId;
  }

  // Cancel scheduled notification
  cancelScheduledNotification(timeoutId) {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }

  // Cleanup
  cleanup() {
    this.clearAllNotifications();
    this.notificationQueue = [];
  }
}

export default new NotificationsService();