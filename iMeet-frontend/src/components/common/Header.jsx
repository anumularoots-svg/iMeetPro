// src/components/common/Header.jsx - FIXED VERSION
import React, { useState, useEffect } from 'react';
import { 
  AppBar, 
  Toolbar, 
  Typography, 
  IconButton, 
  Badge, 
  Box,
  Menu,
  MenuItem,
  Avatar,
  Divider,
  ListItemIcon,
  Tooltip,
  Switch,
  FormControlLabel,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Paper,
  Skeleton,
  Chip,
  Button
} from '@mui/material';
import { 
  Notifications, 
  Settings, 
  VideoCall,
  AccountCircle,
  LightMode,
  DarkMode,
  NotificationsActive,
  Message,
  Event,
  VideoLibrary,
  MarkEmailRead,
  Close,
  Schedule,
  Person,
  Refresh
} from '@mui/icons-material';
import { useAuth } from '../../hooks/useAuth';
import { useNotifications } from '../../hooks/useNotifications';
import { formatRelativeTime } from '../../utils/helpers';

const Header = ({ toggleTheme, isDarkMode }) => {
  const [notificationAnchor, setNotificationAnchor] = useState(null);
  const [settingsAnchor, setSettingsAnchor] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  
  const { user } = useAuth();
  const { 
    notifications, 
    markAsRead, 
    markAllAsRead,
    deleteNotification,
    unreadCount, 
    loading,
    fetchNotifications 
  } = useNotifications();

  // Debug logging
  useEffect(() => {
    console.log('🔔 Header - Notifications state:', {
      count: notifications.length,
      unreadCount,
      loading,
      sampleNotification: notifications[0]
    });
  }, [notifications, unreadCount, loading]);

  const handleNotificationClick = (event) => {
    setNotificationAnchor(event.currentTarget);
  };

  const handleSettingsClick = (event) => {
    setSettingsAnchor(event.currentTarget);
  };

  const handleNotificationClose = () => {
    setNotificationAnchor(null);
  };

  const handleSettingsClose = () => {
    setSettingsAnchor(null);
  };

  const handleNotificationItemClick = async (notification) => {
    console.log('🔔 Header - Notification clicked:', notification);
    
    // Mark as read if not already read
    if (!notification.is_read) {
      await markAsRead(notification.id);
    }
    
    // Handle navigation based on notification type
    if (notification.meeting_url) {
      window.open(notification.meeting_url, '_blank');
    }
    
    handleNotificationClose();
  };

  const handleMarkAllAsRead = async () => {
    console.log('🔔 Header - Mark all as read clicked');
    await markAllAsRead();
  };

  const handleRefreshNotifications = async () => {
    console.log('🔔 Header - Refresh notifications clicked');
    setRefreshing(true);
    try {
      await fetchNotifications();
    } finally {
      setRefreshing(false);
    }
  };

  // FIXED: Get notification icon with proper type handling
  const getNotificationIcon = (notification) => {
    const type = notification.type || notification.notification_type || 'default';
    
    console.log('🔔 Header - Getting icon for type:', type);
    
    switch (type) {
      case 'meeting_invitation':
      case 'meeting_reminder':
      case 'meeting_created':
        return <VideoCall fontSize="small" color="primary" />;
      case 'message':
      case 'chat_message':
        return <Message fontSize="small" color="info" />;
      case 'event':
        return <Event fontSize="small" color="secondary" />;
      case 'recording':
      case 'recording_started':
      case 'recording_stopped':
        return <VideoLibrary fontSize="small" color="success" />;
      case 'participant_joined':
      case 'participant_left':
      case 'user_joined':
      case 'user_left':
        return <Person fontSize="small" color="warning" />;
      default:
        return <NotificationsActive fontSize="small" color="action" />;
    }
  };

  const getNotificationTitle = (notification) => {
    return notification.title || 'Notification';
  };

  const getNotificationMessage = (notification) => {
    return notification.message || '';
  };

  const getPriorityColor = (priority) => {
    switch (priority) {
      case 'high': return 'error';
      case 'medium': return 'warning';
      case 'low': return 'info';
      default: return 'default';
    }
  };

  const formatTimeAgo = (notification) => {
    // Use backend-provided time_ago or calculate from created_at
    if (notification.time_ago) {
      return notification.time_ago;
    }
    
    if (notification.created_at) {
      return formatRelativeTime(notification.created_at);
    }
    
    return 'Unknown time';
  };

  return (
    <AppBar 
      position="fixed" 
      sx={{ 
        zIndex: 1300, 
        bgcolor: 'primary.main',
        boxShadow: '0 2px 20px rgba(0,0,0,0.1)',
        backdropFilter: 'blur(10px)'
      }}
    >
      <Toolbar>
        <VideoCall sx={{ mr: 2, fontSize: 32 }} />
        
        <Typography 
          variant="h6" 
          component="div" 
          sx={{ 
            flexGrow: 1, 
            fontWeight: 'bold',
            background: 'linear-gradient(45deg, #fff 30%, #e3f2fd 90%)',
            backgroundClip: 'text',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
            fontSize: '1.5rem'
          }}
        >
          MeetPro
        </Typography>
        
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          {/* Enhanced Notifications */}
          <Tooltip title={`${unreadCount} unread notifications`}>
            <IconButton 
              color="inherit" 
              onClick={handleNotificationClick}
              sx={{
                '&:hover': {
                  bgcolor: 'rgba(255,255,255,0.1)',
                  transform: 'scale(1.05)'
                }
              }}
            >
              <Badge 
                badgeContent={unreadCount} 
                color="error"
                sx={{
                  '& .MuiBadge-badge': {
                    animation: unreadCount > 0 ? 'pulse 2s infinite' : 'none',
                    '@keyframes pulse': {
                      '0%': { transform: 'scale(1)' },
                      '50%': { transform: 'scale(1.2)' },
                      '100%': { transform: 'scale(1)' }
                    }
                  }
                }}
              >
                <Notifications />
              </Badge>
            </IconButton>
          </Tooltip>
          
          {/* Settings */}
          <Tooltip title="Settings">
            <IconButton 
              color="inherit"
              onClick={handleSettingsClick}
              sx={{
                '&:hover': {
                  bgcolor: 'rgba(255,255,255,0.1)',
                  transform: 'scale(1.05)'
                }
              }}
            >
              <Settings />
            </IconButton>
          </Tooltip>

          {/* User Avatar */}
          <Tooltip title={user?.full_name || user?.name || 'User'}>
            <Avatar 
              src={user?.profile_picture} 
              sx={{ 
                width: 36, 
                height: 36,
                border: '2px solid rgba(255,255,255,0.3)',
                cursor: 'pointer',
                '&:hover': {
                  transform: 'scale(1.1)',
                  boxShadow: '0 4px 15px rgba(255,255,255,0.3)'
                }
              }}
            >
              {(user?.full_name || user?.name || 'U').charAt(0).toUpperCase()}
            </Avatar>
          </Tooltip>
        </Box>

        {/* Enhanced Notifications Menu */}
        <Menu
          anchorEl={notificationAnchor}
          open={Boolean(notificationAnchor)}
          onClose={handleNotificationClose}
          PaperProps={{
            sx: {
              mt: 1,
              borderRadius: 2,
              minWidth: 400,
              maxWidth: 450,
              maxHeight: 600,
              boxShadow: '0 8px 32px rgba(0,0,0,0.12)'
            }
          }}
        >
          {/* Header */}
          <Box sx={{ p: 2, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
              <Typography variant="h6" fontWeight="bold">
                Notifications
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                <Chip 
                  label={unreadCount}
                  size="small"
                  color="primary"
                  sx={{ minWidth: 24 }}
                />
                <Tooltip title="Refresh">
                  <IconButton 
                    size="small" 
                    onClick={handleRefreshNotifications}
                    disabled={refreshing}
                  >
                    <Refresh sx={{ 
                      fontSize: 18,
                      animation: refreshing ? 'spin 1s linear infinite' : 'none',
                      '@keyframes spin': {
                        '0%': { transform: 'rotate(0deg)' },
                        '100%': { transform: 'rotate(360deg)' }
                      }
                    }} />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>
            
            {/* Action buttons */}
            {unreadCount > 0 && (
              <Button 
                size="small" 
                onClick={handleMarkAllAsRead}
                startIcon={<MarkEmailRead />}
              >
                Mark all read
              </Button>
            )}
          </Box>
          
          {/* Loading state */}
          {loading ? (
            <Box sx={{ p: 2 }}>
              {[1, 2, 3].map((item) => (
                <Box key={item} sx={{ display: 'flex', gap: 2, mb: 2 }}>
                  <Skeleton variant="circular" width={40} height={40} />
                  <Box sx={{ flex: 1 }}>
                    <Skeleton variant="text" width="80%" />
                    <Skeleton variant="text" width="60%" />
                  </Box>
                </Box>
              ))}
            </Box>
          ) : notifications.length === 0 ? (
            // Empty state
            <Box sx={{ p: 4, textAlign: 'center' }}>
              <Notifications sx={{ fontSize: 48, color: 'text.disabled', mb: 1 }} />
              <Typography color="text.secondary" variant="body2">
                No notifications yet
              </Typography>
              <Button 
                size="small" 
                onClick={handleRefreshNotifications}
                sx={{ mt: 1 }}
              >
                Refresh
              </Button>
            </Box>
          ) : (
            // Notifications list
            <List sx={{ p: 0, maxHeight: 400, overflowY: 'auto' }}>
              {notifications.slice(0, 10).map((notification) => {
                console.log('🔔 Header - Rendering notification:', {
                  id: notification.id,
                  type: notification.type,
                  title: notification.title,
                  time_ago: notification.time_ago
                });
                
                return (
                  <ListItem
                    key={notification.id}
                    button
                    onClick={() => handleNotificationItemClick(notification)}
                    sx={{
                      py: 2,
                      px: 2,
                      borderBottom: '1px solid',
                      borderColor: 'divider',
                      bgcolor: notification.is_read ? 'transparent' : 'action.hover',
                      '&:hover': {
                        bgcolor: 'action.selected'
                      }
                    }}
                  >
                    <ListItemAvatar sx={{ minWidth: 48 }}>
                      <Box sx={{ 
                        position: 'relative',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center'
                      }}>
                        {getNotificationIcon(notification)}
                        {!notification.is_read && (
                          <Box
                            sx={{
                              position: 'absolute',
                              top: -2,
                              right: -2,
                              width: 8,
                              height: 8,
                              borderRadius: '50%',
                              bgcolor: 'primary.main'
                            }}
                          />
                        )}
                      </Box>
                    </ListItemAvatar>
                    
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                          <Typography 
                            variant="body2" 
                            fontWeight={notification.is_read ? 400 : 600}
                            sx={{ flex: 1 }}
                          >
                            {getNotificationTitle(notification)}
                          </Typography>
                          {notification.priority && notification.priority !== 'normal' && (
                            <Chip 
                              size="small" 
                              label={notification.priority}
                              color={getPriorityColor(notification.priority)}
                              sx={{ height: 16, fontSize: '0.625rem' }}
                            />
                          )}
                        </Box>
                      }
                      secondary={
                        <Box>
                          <Typography 
                            variant="body2" 
                            color="text.secondary" 
                            sx={{ mb: 0.5, lineHeight: 1.3 }}
                          >
                            {getNotificationMessage(notification)}
                          </Typography>
                          <Typography variant="caption" color="text.disabled">
                            {formatTimeAgo(notification)}
                            {notification.meeting_title && ` • ${notification.meeting_title}`}
                          </Typography>
                        </Box>
                      }
                    />
                  </ListItem>
                );
              })}
            </List>
          )}
          
          {/* Footer */}
          {notifications.length > 10 && (
            <Box sx={{ p: 1, textAlign: 'center', borderTop: '1px solid', borderColor: 'divider' }}>
              <Typography variant="body2" color="primary" sx={{ cursor: 'pointer' }}>
                View all {notifications.length} notifications
              </Typography>
            </Box>
          )}
        </Menu>

        {/* Settings Menu */}
        <Menu
          anchorEl={settingsAnchor}
          open={Boolean(settingsAnchor)}
          onClose={handleSettingsClose}
          PaperProps={{
            sx: {
              mt: 1,
              borderRadius: 2,
              minWidth: 250,
              boxShadow: '0 8px 32px rgba(0,0,0,0.12)'
            }
          }}
        >
          <Box sx={{ p: 2, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Typography variant="h6" fontWeight="bold">
              Quick Settings
            </Typography>
          </Box>
          
          <MenuItem sx={{ py: 2 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={isDarkMode}
                  onChange={toggleTheme}
                  icon={<LightMode />}
                  checkedIcon={<DarkMode />}
                />
              }
              label={isDarkMode ? 'Dark Mode' : 'Light Mode'}
              sx={{ margin: 0 }}
            />
          </MenuItem>
          
          <Divider />
          
          <MenuItem onClick={handleSettingsClose}>
            <ListItemIcon>
              <Settings fontSize="small" />
            </ListItemIcon>
            All Settings
          </MenuItem>
          
          <MenuItem onClick={handleSettingsClose}>
            <ListItemIcon>
              <AccountCircle fontSize="small" />
            </ListItemIcon>
            Profile Settings
          </MenuItem>
        </Menu>
      </Toolbar>
    </AppBar>
  );
};

export default Header;