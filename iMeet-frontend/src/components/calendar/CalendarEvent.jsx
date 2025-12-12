import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Avatar,
  Chip,
  IconButton,
  Stack,
  Button,
  Divider,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Menu,
  MenuItem,
  ListItemIcon,
  useTheme,
  alpha,
  Tooltip,
  Badge
} from '@mui/material';
import {
  VideoCall as VideoIcon,
  AccessTime as TimeIcon,
  People as PeopleIcon,
  LocationOn as LocationIcon,
  MoreVert as MoreIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  Launch as JoinIcon,
  Share as ShareIcon,
  NotificationsActive as ReminderIcon,
  Repeat as RepeatIcon,
  Description as DescriptionIcon,
  Link as LinkIcon,
  EventBusy as CancelIcon,
  Event as EventIcon,
  Schedule as ScheduleIcon
} from '@mui/icons-material';

const CalendarEvent = ({ 
  event,
  variant = 'card', // 'card', 'list', 'compact'
  onJoin,
  onEdit,
  onDelete,
  onShare,
  showActions = true
}) => {
  const theme = useTheme();
  const [menuAnchor, setMenuAnchor] = useState(null);

  const formatTime = (timeString) => {
    return new Date(`2000-01-01T${timeString}`).toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      weekday: 'short',
      month: 'short',
      day: 'numeric'
    });
  };

  const getEventTypeColor = (type) => {
    switch (type) {
      case 'instant': return 'primary';
      case 'scheduled': return 'success';
      case 'calendar': return 'info';
      case 'recurring': return 'warning';
      default: return 'default';
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'scheduled': return 'success';
      case 'in-progress': return 'warning';
      case 'completed': return 'info';
      case 'cancelled': return 'error';
      default: return 'default';
    }
  };

  const getEventIcon = (type) => {
    switch (type) {
      case 'instant': return <VideoIcon />;
      case 'scheduled': return <ScheduleIcon />;
      case 'calendar': return <EventIcon />;
      case 'recurring': return <RepeatIcon />;
      default: return <VideoIcon />;
    }
  };

  const handleMenuClick = (e) => {
    e.stopPropagation();
    setMenuAnchor(e.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuAnchor(null);
  };

  const isUpcoming = () => {
    const eventDate = new Date(`${event.date}T${event.startTime}`);
    return eventDate > new Date();
  };

  const isNow = () => {
    const now = new Date();
    const eventStart = new Date(`${event.date}T${event.startTime}`);
    const eventEnd = new Date(`${event.date}T${event.endTime}`);
    return now >= eventStart && now <= eventEnd;
  };

  const getTimeUntil = () => {
    const eventDate = new Date(`${event.date}T${event.startTime}`);
    const now = new Date();
    const diffMs = eventDate - now;
    
    if (diffMs < 0) return null;
    
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
    const diffMinutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
    
    if (diffHours > 24) {
      const diffDays = Math.floor(diffHours / 24);
      return `in ${diffDays} day${diffDays > 1 ? 's' : ''}`;
    } else if (diffHours > 0) {
      return `in ${diffHours}h ${diffMinutes}m`;
    } else {
      return `in ${diffMinutes}m`;
    }
  };

  // Compact variant for calendar grid
  if (variant === 'compact') {
    return (
      <Box
        sx={{
          bgcolor: alpha(theme.palette[getEventTypeColor(event.type)]?.main || theme.palette.primary.main, 0.2),
          border: `1px solid ${theme.palette[getEventTypeColor(event.type)]?.main || theme.palette.primary.main}`,
          borderRadius: 1,
          p: 0.5,
          cursor: 'pointer',
          '&:hover': {
            bgcolor: alpha(theme.palette[getEventTypeColor(event.type)]?.main || theme.palette.primary.main, 0.3)
          }
        }}
        onClick={() => onJoin?.(event)}
      >
        <Typography
          variant="caption"
          sx={{
            display: 'block',
            fontWeight: 'medium',
            color: theme.palette[getEventTypeColor(event.type)]?.dark || theme.palette.primary.dark,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap'
          }}
        >
          {formatTime(event.startTime)} {event.title}
        </Typography>
        {isNow() && (
          <Chip
            label="Live"
            color="error"
            size="small"
            sx={{ fontSize: '0.6rem', height: 16, mt: 0.25 }}
          />
        )}
      </Box>
    );
  }

  // List variant for agenda view
  if (variant === 'list') {
    return (
      <ListItem
        sx={{
          border: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
          borderRadius: 2,
          mb: 1,
          bgcolor: isNow() ? alpha(theme.palette.success.main, 0.1) : 'background.paper',
          '&:hover': {
            bgcolor: alpha(theme.palette.primary.main, 0.05)
          }
        }}
      >
        <ListItemAvatar>
          <Badge
            color="error"
            variant="dot"
            invisible={!isNow()}
          >
            <Avatar
              sx={{
                bgcolor: theme.palette[getEventTypeColor(event.type)]?.main,
                width: 40,
                height: 40
              }}
            >
              {getEventIcon(event.type)}
            </Avatar>
          </Badge>
        </ListItemAvatar>
        <ListItemText
          primary={
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography variant="subtitle2" fontWeight="bold">
                {event.title}
              </Typography>
              <Chip
                label={event.type}
                color={getEventTypeColor(event.type)}
                size="small"
                variant="outlined"
              />
              {isNow() && (
                <Chip
                  label="Live Now"
                  color="error"
                  size="small"
                  sx={{ animation: 'pulse 2s infinite' }}
                />
              )}
            </Box>
          }
          secondary={
            <Box sx={{ mt: 0.5 }}>
              <Typography variant="caption" color="text.secondary">
                {formatTime(event.startTime)} - {formatTime(event.endTime)} • {event.participants} participants
              </Typography>
              {getTimeUntil() && (
                <Typography variant="caption" color="primary" sx={{ ml: 1 }}>
                  {getTimeUntil()}
                </Typography>
              )}
            </Box>
          }
        />
        {showActions && (
          <Stack direction="row" spacing={1}>
            {isUpcoming() && (
              <Button
                variant="contained"
                size="small"
                startIcon={<JoinIcon />}
                onClick={() => onJoin?.(event)}
                sx={{ borderRadius: 2 }}
              >
                {isNow() ? 'Join Now' : 'Join'}
              </Button>
            )}
            <IconButton onClick={handleMenuClick} size="small">
              <MoreIcon />
            </IconButton>
          </Stack>
        )}
      </ListItem>
    );
  }

  // Card variant (default)
  return (
    <>
      <Card
        elevation={isNow() ? 8 : 2}
        sx={{
          position: 'relative',
          overflow: 'visible',
          border: isNow() ? `2px solid ${theme.palette.success.main}` : `1px solid ${alpha(theme.palette.divider, 0.2)}`,
          borderRadius: 3,
          background: isNow() 
            ? `linear-gradient(135deg, ${alpha(theme.palette.success.main, 0.1)} 0%, ${alpha(theme.palette.success.light, 0.05)} 100%)`
            : 'background.paper',
          '&:hover': {
            transform: 'translateY(-2px)',
            boxShadow: theme.shadows[6]
          },
          transition: 'all 0.3s ease'
        }}
      >
        {/* Live indicator */}
        {isNow() && (
          <Box
            sx={{
              position: 'absolute',
              top: -8,
              right: 16,
              bgcolor: theme.palette.error.main,
              color: 'white',
              px: 1.5,
              py: 0.5,
              borderRadius: 2,
              fontSize: '0.75rem',
              fontWeight: 'bold',
              animation: 'pulse 2s infinite',
              zIndex: 1
            }}
          >
            LIVE
          </Box>
        )}

        <CardContent sx={{ p: 3 }}>
          {/* Header */}
          <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', flexGrow: 1 }}>
              <Avatar
                sx={{
                  bgcolor: theme.palette[getEventTypeColor(event.type)]?.main,
                  mr: 2,
                  width: 48,
                  height: 48,
                  background: `linear-gradient(135deg, ${theme.palette[getEventTypeColor(event.type)]?.main}, ${theme.palette[getEventTypeColor(event.type)]?.dark})`
                }}
              >
                {getEventIcon(event.type)}
              </Avatar>
              <Box sx={{ flexGrow: 1 }}>
                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  {event.title}
                </Typography>
                <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                  <Chip
                    label={event.type}
                    color={getEventTypeColor(event.type)}
                    size="small"
                    variant="outlined"
                  />
                  <Chip
                    label={event.status}
                    color={getStatusColor(event.status)}
                    size="small"
                    variant="outlined"
                  />
                  {event.isRecurring && (
                    <Chip
                      icon={<RepeatIcon />}
                      label="Recurring"
                      color="info"
                      size="small"
                      variant="outlined"
                    />
                  )}
                </Stack>
              </Box>
            </Box>
            {showActions && (
              <IconButton onClick={handleMenuClick}>
                <MoreIcon />
              </IconButton>
            )}
          </Box>

          <Divider sx={{ mb: 2 }} />

          {/* Event Details */}
          <Stack spacing={1.5}>
            {/* Date and Time */}
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <TimeIcon sx={{ color: 'text.secondary', mr: 1 }} />
              <Typography variant="body2">
                {formatDate(event.date)} • {formatTime(event.startTime)} - {formatTime(event.endTime)}
              </Typography>
              {getTimeUntil() && (
                <Chip
                  label={getTimeUntil()}
                  color="primary"
                  size="small"
                  sx={{ ml: 1, fontSize: '0.7rem' }}
                />
              )}
            </Box>

            {/* Participants */}
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <PeopleIcon sx={{ color: 'text.secondary', mr: 1 }} />
              <Typography variant="body2">
                {event.participants} participant{event.participants !== 1 ? 's' : ''}
              </Typography>
            </Box>

            {/* Location/Link */}
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <LinkIcon sx={{ color: 'text.secondary', mr: 1 }} />
              <Typography 
                variant="body2" 
                sx={{ 
                  fontFamily: 'monospace',
                  bgcolor: alpha(theme.palette.background.default, 0.8),
                  px: 1,
                  py: 0.25,
                  borderRadius: 1,
                  fontSize: '0.75rem'
                }}
              >
                meet.app.com/join/{event.meetingId || event.id}
              </Typography>
            </Box>

            {/* Description */}
            {event.description && (
              <Box sx={{ display: 'flex', alignItems: 'flex-start' }}>
                <DescriptionIcon sx={{ color: 'text.secondary', mr: 1, mt: 0.25 }} />
                <Typography variant="body2" color="text.secondary" fontStyle="italic">
                  {event.description}
                </Typography>
              </Box>
            )}

            {/* Reminder */}
            {event.reminder && (
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <ReminderIcon sx={{ color: 'text.secondary', mr: 1 }} />
                <Typography variant="body2" color="text.secondary">
                  Reminder: {event.reminder} minutes before
                </Typography>
              </Box>
            )}
          </Stack>

          {/* Action Buttons */}
          {showActions && (
            <Box sx={{ mt: 3, display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
              {isUpcoming() && (
                <Button
                  variant="contained"
                  startIcon={<JoinIcon />}
                  onClick={() => onJoin?.(event)}
                  sx={{
                    borderRadius: 2,
                    background: isNow() 
                      ? `linear-gradient(135deg, ${theme.palette.success.main}, ${theme.palette.success.dark})`
                      : `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
                    boxShadow: theme.shadows[4],
                    ...(isNow() && {
                      animation: 'pulse 2s infinite'
                    })
                  }}
                >
                  {isNow() ? 'Join Now' : 'Join Meeting'}
                </Button>
              )}
              <Button
                variant="outlined"
                startIcon={<ShareIcon />}
                onClick={() => onShare?.(event)}
                sx={{ borderRadius: 2 }}
              >
                Share
              </Button>
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Context Menu */}
      <Menu
        anchorEl={menuAnchor}
        open={Boolean(menuAnchor)}
        onClose={handleMenuClose}
        transformOrigin={{ horizontal: 'right', vertical: 'top' }}
        anchorOrigin={{ horizontal: 'right', vertical: 'bottom' }}
      >
        {isUpcoming() && (
          <MenuItem onClick={() => { onJoin?.(event); handleMenuClose(); }}>
            <ListItemIcon>
              <JoinIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText>{isNow() ? 'Join Now' : 'Join Meeting'}</ListItemText>
          </MenuItem>
        )}
        <MenuItem onClick={() => { onEdit?.(event); handleMenuClose(); }}>
          <ListItemIcon>
            <EditIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Edit Meeting</ListItemText>
        </MenuItem>
        <MenuItem onClick={() => { onShare?.(event); handleMenuClose(); }}>
          <ListItemIcon>
            <ShareIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Share Meeting</ListItemText>
        </MenuItem>
        <Divider />
        {event.status !== 'cancelled' && (
          <MenuItem 
            onClick={() => { handleMenuClose(); }}
            sx={{ color: 'warning.main' }}
          >
            <ListItemIcon>
              <CancelIcon fontSize="small" color="warning" />
            </ListItemIcon>
            <ListItemText>Cancel Meeting</ListItemText>
          </MenuItem>
        )}
        <MenuItem 
          onClick={() => { onDelete?.(event); handleMenuClose(); }}
          sx={{ color: 'error.main' }}
        >
          <ListItemIcon>
            <DeleteIcon fontSize="small" color="error" />
          </ListItemIcon>
          <ListItemText>Delete Meeting</ListItemText>
        </MenuItem>
      </Menu>

      {/* Keyframes for pulse animation */}
      <style>
        {`
          @keyframes pulse {
            0% {
              opacity: 1;
              transform: scale(1);
            }
            50% {
              opacity: 0.7;
              transform: scale(1.05);
            }
            100% {
              opacity: 1;
              transform: scale(1);
            }
          }
        `}
      </style>
    </>
  );
};

export default CalendarEvent;