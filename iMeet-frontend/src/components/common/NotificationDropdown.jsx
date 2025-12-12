// src/components/common/NotificationDropdown.jsx - UPDATED
import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  IconButton,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Avatar,
  Badge,
  Divider,
  Button,
  CircularProgress,
  Fade,
  Skeleton,
  Tooltip
} from '@mui/material';
import {
  VideoCall,
  RecordVoiceOver,
  Person,
  Event,
  Close,
  MarkEmailRead,
  Delete,
  Refresh
} from '@mui/icons-material';
import { formatRelativeTime } from '../../utils/helpers';
import { useNotificationContext } from '../../context/NotificationContext';

const NotificationDropdown = ({ open, onClose, anchorEl, filterType = 'all' }) => {
  const {
    notifications = [],
    unreadCount = 0,
    loading = false,
    markAsRead,
    markAllAsRead,
    deleteNotification,
    fetchNotifications,
    fetchScheduleNotifications,
    fetchCalendarNotifications,
    fetchRecordingNotifications
  } = useNotificationContext();

  const [localLoading, setLocalLoading] = useState(false);
   useEffect(() => {
  if (open) {
    // Fetch notifications when the dropdown is opened
    fetchNotifications();
  }
}, [open, fetchNotifications]); // Run whenever the dropdown is opened
  // CRITICAL FIX: Filter notifications based on page type
 const filteredNotifications = notifications;
  // Calculate filtered unread count
  // ✅ AFTER (keep this, just update the reference):
const filteredUnreadCount = React.useMemo(() => {
  return filteredNotifications.filter(n => !n.is_read).length;
}, [filteredNotifications]);

  // Get notification icon based on type
  const getNotificationIcon = (type) => {
    const iconProps = { 
      sx: { 
        fontSize: 20, 
        color: 'primary.main' 
      } 
    };

    switch (type) {
      case 'meeting_reminder':
      case 'meeting_started':
      case 'meeting_ended':
      case 'meeting_invitation':
      case 'meeting_created':
        return <VideoCall {...iconProps} />;
      case 'recording_ready':
      case 'recording_started':
      case 'recording_stopped':
      case 'recording_completed':
        return <RecordVoiceOver {...iconProps} />;
      case 'participant_joined':
      case 'participant_left':
      case 'new_participant':
        return <Person {...iconProps} />;
      case 'meeting_scheduled':
      case 'meeting_updated':
      case 'calendar_meeting':
        return <Event {...iconProps} />;
      default:
        return <VideoCall {...iconProps} />;
    }
  };

  const handleNotificationClick = async (notification) => {
    console.log('Notification clicked:', notification);
    
    if (!notification.is_read) {
      try {
        await markAsRead(notification.id);
      } catch (error) {
        console.error('Error marking notification as read:', error);
      }
    }

    if (notification.meeting_url && notification.type === 'meeting_reminder') {
      window.open(notification.meeting_url, '_blank');
    }
  };

  const handleDeleteNotification = async (notificationId, event) => {
    event.stopPropagation();
    
    try {
      setLocalLoading(true);
      await deleteNotification(notificationId);
    } catch (error) {
      console.error('Error deleting notification:', error);
    } finally {
      setLocalLoading(false);
    }
  };

  const handleMarkAllAsRead = async () => {
    if (filteredUnreadCount === 0) return;
    
    try {
      setLocalLoading(true);
      await markAllAsRead();
    } catch (error) {
      console.error('Error marking all as read:', error);
    } finally {
      setLocalLoading(false);
    }
  };

  const handleRefresh = async () => {
  try {
    setLocalLoading(true);
    await fetchNotifications(); // Only fetch all notifications
  } catch (error) {
    console.error('Error refreshing notifications:', error);
  } finally {
    setLocalLoading(false);
  }
};

  const renderSkeleton = () => (
    <Box sx={{ p: 2 }}>
      {[...Array(3)].map((_, index) => (
        <Box key={index} sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <Skeleton variant="circular" width={40} height={40} sx={{ mr: 2 }} />
          <Box sx={{ flex: 1 }}>
            <Skeleton variant="text" width="80%" height={20} />
            <Skeleton variant="text" width="60%" height={16} />
            <Skeleton variant="text" width="40%" height={14} />
          </Box>
        </Box>
      ))}
    </Box>
  );

  if (!open) return null;

  return (
    <Fade in={open}>
      <Box
        sx={{
          position: 'fixed',
          top: 88,
          right: 16,
          width: 400,
          maxWidth: 'calc(100vw - 32px)',
          maxHeight: 'calc(100vh - 120px)',
          bgcolor: 'background.paper',
          borderRadius: 3,
          boxShadow: '0 20px 40px rgba(0,0,0,0.15)',
          border: '1px solid rgba(0,0,0,0.08)',
          overflow: 'hidden',
          zIndex: 9999,
        }}
      >
        {/* Header */}
        <Box
          sx={{
            p: 2,
            bgcolor: 'primary.main',
            color: 'white',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center'
          }}
        >
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 600, mb: 0.5 }}>
  All Notifications
</Typography>

            <Typography variant="body2" sx={{ opacity: 0.9 }}>
              {filteredUnreadCount} unread notification{filteredUnreadCount !== 1 ? 's' : ''}
            </Typography>
          </Box>
          
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Tooltip title="Refresh">
              <IconButton 
                size="small" 
                sx={{ color: 'white' }}
                onClick={handleRefresh}
                disabled={localLoading}
              >
                <Refresh sx={{ fontSize: 20 }} />
              </IconButton>
            </Tooltip>
            
            <Tooltip title="Close">
              <IconButton 
                size="small" 
                sx={{ color: 'white' }}
                onClick={onClose}
              >
                <Close sx={{ fontSize: 20 }} />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>

        {/* Actions Bar */}
        {filteredUnreadCount > 0 && (
          <Box sx={{ p: 2, bgcolor: 'grey.50', borderBottom: '1px solid rgba(0,0,0,0.05)' }}>
            <Button
              size="small"
              startIcon={<MarkEmailRead />}
              onClick={handleMarkAllAsRead}
              disabled={localLoading}
              sx={{ fontSize: '0.75rem' }}
            >
              Mark all as read
            </Button>
          </Box>
        )}

        {/* Notifications List */}
        <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
          {loading ? (
            renderSkeleton()
          ) : filteredNotifications.length === 0 ? (
            <Box sx={{ p: 4, textAlign: 'center' }}>
<Typography variant="body2" color="text.secondary">
  No notifications yet
</Typography>
            </Box>
          ) : (
            <List sx={{ p: 0 }}>
              {filteredNotifications.map((notification, index) => (
                <React.Fragment key={notification.id}>
                  <ListItem
                    sx={{
                      cursor: 'pointer',
                      bgcolor: notification.is_read ? 'transparent' : 'rgba(25, 118, 210, 0.04)',
                      '&:hover': {
                        bgcolor: 'rgba(0,0,0,0.04)'
                      },
                      py: 2,
                      px: 2,
                      position: 'relative'
                    }}
                    onClick={() => handleNotificationClick(notification)}
                  >
                    <ListItemAvatar>
                      <Avatar
                        sx={{
                          bgcolor: notification.is_read ? 'grey.100' : 'primary.light',
                          width: 40,
                          height: 40
                        }}
                      >
                        {getNotificationIcon(notification.type || notification.notification_type)}
                      </Avatar>
                    </ListItemAvatar>
                    
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                          <Typography
                            component="span"
                            variant="subtitle2"
                            sx={{
                              fontWeight: notification.is_read ? 500 : 600,
                              color: notification.is_read ? 'text.primary' : 'primary.main',
                              flex: 1,
                              mr: 1
                            }}
                          >
                            {notification.title}
                          </Typography>
                          
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            {!notification.is_read && (
                              <Box
                                sx={{
                                  width: 8,
                                  height: 8,
                                  borderRadius: '50%',
                                  bgcolor: 'primary.main'
                                }}
                              />
                            )}
                            
                            <Tooltip title="Delete">
                              <IconButton
                                size="small"
                                onClick={(e) => handleDeleteNotification(notification.id, e)}
                                sx={{
                                  opacity: 0,
                                  '.MuiListItem-root:hover &': {
                                    opacity: 1
                                  },
                                  color: 'error.main'
                                }}
                              >
                                <Delete sx={{ fontSize: 16 }} />
                              </IconButton>
                            </Tooltip>
                          </Box>
                        </Box>
                      }
                      secondary={
                        <Box component="span">
                          <Typography
                            component="span"
                            variant="body2"
                            color="text.secondary"
                            sx={{ mb: 0.5, display: 'block' }}
                          >
                            {notification.message}
                          </Typography>
                          
                          <Typography
                            component="span"
                            variant="caption"
                            color="text.secondary"
                            sx={{ fontSize: '0.7rem' }}
                          >
                            {notification.time_ago || formatRelativeTime(notification.created_at)}
                          </Typography>
                        </Box>
                      }
                    />
                  </ListItem>
                  
                  {index < filteredNotifications.length - 1 && (
                    <Divider variant="middle" />
                  )}
                </React.Fragment>
              ))}
            </List>
          )}
        </Box>

        {/* Loading overlay */}
        {localLoading && (
          <Box
            sx={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              bgcolor: 'rgba(255,255,255,0.8)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 1
            }}
          >
            <CircularProgress size={24} />
          </Box>
        )}
      </Box>
    </Fade>
  );
};

export default NotificationDropdown;