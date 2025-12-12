import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Button,
  IconButton,
  Stack,
  Chip,
  Avatar,
  Badge,
  Tooltip,
  useTheme,
  alpha,
  Fab,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText
} from '@mui/material';
import {
  ChevronLeft as PrevIcon,
  ChevronRight as NextIcon,
  Today as TodayIcon,
  Add as AddIcon,
  Event as EventIcon,
  AccessTime as TimeIcon,
  People as PeopleIcon,
  VideoCall as VideoIcon,
  ViewWeek as WeekIcon,
  ViewDay as DayIcon,
  ViewModule as MonthIcon,
  MoreVert as MoreIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  Launch as JoinIcon
} from '@mui/icons-material';

const CalendarView = ({ 
  events = [],
  onEventClick,
  onDateClick,
  onCreateMeeting,
  viewMode = 'month' // 'month', 'week', 'day'
}) => {
  const theme = useTheme();
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedEvent, setSelectedEvent] = useState(null);
  const [eventDetailOpen, setEventDetailOpen] = useState(false);
  const [viewModeState, setViewModeState] = useState(viewMode);
  const [eventMenuAnchor, setEventMenuAnchor] = useState(null);

  // Mock events for demonstration
  const mockEvents = [
    {
      id: 1,
      title: 'Team Standup',
      startTime: '09:00',
      endTime: '09:30',
      date: new Date().toISOString().split('T')[0],
      type: 'instant',
      participants: 5,
      status: 'scheduled',
      color: 'primary'
    },
    {
      id: 2,
      title: 'Client Presentation',
      startTime: '14:00',
      endTime: '15:00',
      date: new Date(Date.now() + 86400000).toISOString().split('T')[0],
      type: 'scheduled',
      participants: 8,
      status: 'scheduled',
      color: 'success'
    },
    {
      id: 3,
      title: 'Project Review',
      startTime: '11:00',
      endTime: '12:00',
      date: new Date(Date.now() + 172800000).toISOString().split('T')[0],
      type: 'calendar',
      participants: 12,
      status: 'in-progress',
      color: 'warning'
    }
  ];

  const allEvents = [...events, ...mockEvents];

  const getDaysInMonth = (date) => {
    const year = date.getFullYear();
    const month = date.getMonth();
    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    const daysInMonth = lastDay.getDate();
    const startingDayOfWeek = firstDay.getDay();

    const days = [];
    
    // Add empty cells for days before the first day of the month
    for (let i = 0; i < startingDayOfWeek; i++) {
      days.push(null);
    }
    
    // Add all days of the month
    for (let day = 1; day <= daysInMonth; day++) {
      days.push(new Date(year, month, day));
    }
    
    return days;
  };

  const getEventsForDate = (date) => {
    if (!date) return [];
    const dateString = date.toISOString().split('T')[0];
    return allEvents.filter(event => event.date === dateString);
  };

  const formatTime = (timeString) => {
    return new Date(`2000-01-01T${timeString}`).toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  };

  const navigateMonth = (direction) => {
    setCurrentDate(prev => {
      const newDate = new Date(prev);
      newDate.setMonth(prev.getMonth() + direction);
      return newDate;
    });
  };

  const goToToday = () => {
    setCurrentDate(new Date());
  };

  const handleEventClick = (event, clickEvent) => {
    clickEvent.stopPropagation();
    setSelectedEvent(event);
    setEventDetailOpen(true);
    onEventClick?.(event);
  };

  const handleEventMenu = (event, clickEvent) => {
    clickEvent.stopPropagation();
    setSelectedEvent(event);
    setEventMenuAnchor(clickEvent.currentTarget);
  };

  const monthNames = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

  return (
    <Box>
      {/* Calendar Header */}
      <Card 
        elevation={3}
        sx={{
          mb: 3,
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.1)} 0%, ${alpha(theme.palette.secondary.main, 0.05)} 100%)`,
          border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`
        }}
      >
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Avatar
                sx={{
                  bgcolor: theme.palette.primary.main,
                  mr: 2,
                  background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
                }}
              >
                <EventIcon />
              </Avatar>
              <Box>
                <Typography variant="h5" fontWeight="bold">
                  {monthNames[currentDate.getMonth()]} {currentDate.getFullYear()}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {allEvents.length} meetings scheduled
                </Typography>
              </Box>
            </Box>

            {/* View Mode Toggles */}
            <Stack direction="row" spacing={1}>
              <Button
                variant={viewModeState === 'month' ? 'contained' : 'outlined'}
                startIcon={<MonthIcon />}
                onClick={() => setViewModeState('month')}
                size="small"
              >
                Month
              </Button>
              <Button
                variant={viewModeState === 'week' ? 'contained' : 'outlined'}
                startIcon={<WeekIcon />}
                onClick={() => setViewModeState('week')}
                size="small"
              >
                Week
              </Button>
              <Button
                variant={viewModeState === 'day' ? 'contained' : 'outlined'}
                startIcon={<DayIcon />}
                onClick={() => setViewModeState('day')}
                size="small"
              >
                Day
              </Button>
            </Stack>
          </Box>

          {/* Navigation */}
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Stack direction="row" spacing={1}>
              <IconButton onClick={() => navigateMonth(-1)} color="primary">
                <PrevIcon />
              </IconButton>
              <Button
                variant="outlined"
                startIcon={<TodayIcon />}
                onClick={goToToday}
                sx={{ borderRadius: 2 }}
              >
                Today
              </Button>
              <IconButton onClick={() => navigateMonth(1)} color="primary">
                <NextIcon />
              </IconButton>
            </Stack>

            <Chip
              label={`${allEvents.filter(e => e.status === 'scheduled').length} Scheduled`}
              color="primary"
              variant="outlined"
            />
          </Box>
        </CardContent>
      </Card>

      {/* Calendar Grid */}
      <Card elevation={2}>
        <CardContent sx={{ p: 0 }}>
          {/* Day Headers */}
          <Grid container sx={{ bgcolor: alpha(theme.palette.primary.main, 0.1) }}>
            {dayNames.map((day) => (
              <Grid item xs key={day} sx={{ p: 2, textAlign: 'center' }}>
                <Typography variant="subtitle2" fontWeight="bold" color="primary">
                  {day}
                </Typography>
              </Grid>
            ))}
          </Grid>

          {/* Calendar Days */}
          <Grid container>
            {getDaysInMonth(currentDate).map((date, index) => {
              const dayEvents = getEventsForDate(date);
              const isToday = date && date.toDateString() === new Date().toDateString();
              const isCurrentMonth = date && date.getMonth() === currentDate.getMonth();

              return (
                <Grid 
                  item 
                  xs 
                  key={index}
                  sx={{
                    minHeight: 120,
                    border: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
                    cursor: date ? 'pointer' : 'default',
                    '&:hover': date ? {
                      bgcolor: alpha(theme.palette.primary.main, 0.05)
                    } : {},
                    bgcolor: isToday ? alpha(theme.palette.primary.main, 0.1) : 'background.paper'
                  }}
                  onClick={() => date && onDateClick?.(date)}
                >
                  <Box sx={{ p: 1, height: '100%', display: 'flex', flexDirection: 'column' }}>
                    {date && (
                      <>
                        {/* Day Number */}
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 0.5 }}>
                          <Typography
                            variant="body2"
                            fontWeight={isToday ? 'bold' : 'normal'}
                            color={isCurrentMonth ? 'text.primary' : 'text.disabled'}
                            sx={{
                              ...(isToday && {
                                bgcolor: theme.palette.primary.main,
                                color: 'white',
                                borderRadius: '50%',
                                width: 24,
                                height: 24,
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center'
                              })
                            }}
                          >
                            {date.getDate()}
                          </Typography>
                          {dayEvents.length > 0 && (
                            <Badge
                              badgeContent={dayEvents.length}
                              color="primary"
                              max={9}
                              sx={{ '& .MuiBadge-badge': { fontSize: '0.6rem', minWidth: 16, height: 16 } }}
                            />
                          )}
                        </Box>

                        {/* Events */}
                        <Box sx={{ flexGrow: 1, overflowY: 'auto' }}>
                          {dayEvents.slice(0, 3).map((event) => (
                            <Tooltip
                              key={event.id}
                              title={`${event.title} • ${formatTime(event.startTime)} - ${formatTime(event.endTime)}`}
                              arrow
                            >
                              <Box
                                sx={{
                                  bgcolor: alpha(theme.palette[event.color]?.main || theme.palette.primary.main, 0.2),
                                  border: `1px solid ${theme.palette[event.color]?.main || theme.palette.primary.main}`,
                                  borderRadius: 1,
                                  p: 0.5,
                                  mb: 0.5,
                                  cursor: 'pointer',
                                  '&:hover': {
                                    bgcolor: alpha(theme.palette[event.color]?.main || theme.palette.primary.main, 0.3)
                                  }
                                }}
                                onClick={(e) => handleEventClick(event, e)}
                              >
                                <Typography
                                  variant="caption"
                                  sx={{
                                    display: 'block',
                                    fontWeight: 'medium',
                                    color: theme.palette[event.color]?.dark || theme.palette.primary.dark,
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap'
                                  }}
                                >
                                  {formatTime(event.startTime)} {event.title}
                                </Typography>
                              </Box>
                            </Tooltip>
                          ))}
                          {dayEvents.length > 3 && (
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              sx={{ fontSize: '0.7rem' }}
                            >
                              +{dayEvents.length - 3} more
                            </Typography>
                          )}
                        </Box>
                      </>
                    )}
                  </Box>
                </Grid>
              );
            })}
          </Grid>
        </CardContent>
      </Card>

      {/* Floating Action Button */}
      <Fab
        color="primary"
        sx={{
          position: 'fixed',
          bottom: 24,
          right: 24,
          background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
          boxShadow: theme.shadows[8]
        }}
        onClick={onCreateMeeting}
      >
        <AddIcon />
      </Fab>

      {/* Event Detail Dialog */}
      <Dialog
        open={eventDetailOpen}
        onClose={() => setEventDetailOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        {selectedEvent && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Avatar
                    sx={{
                      bgcolor: theme.palette[selectedEvent.color]?.main || theme.palette.primary.main,
                      mr: 2
                    }}
                  >
                    <VideoIcon />
                  </Avatar>
                  <Box>
                    <Typography variant="h6" fontWeight="bold">
                      {selectedEvent.title}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {new Date(selectedEvent.date).toLocaleDateString('en-US', {
                        weekday: 'long',
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric'
                      })}
                    </Typography>
                  </Box>
                </Box>
                <IconButton onClick={(e) => handleEventMenu(selectedEvent, e)}>
                  <MoreIcon />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent>
              <Stack spacing={2}>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <TimeIcon sx={{ mr: 1, color: 'text.secondary' }} />
                  <Typography>
                    {formatTime(selectedEvent.startTime)} - {formatTime(selectedEvent.endTime)}
                  </Typography>
                </Box>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <PeopleIcon sx={{ mr: 1, color: 'text.secondary' }} />
                  <Typography>
                    {selectedEvent.participants} participants
                  </Typography>
                </Box>
                <Box>
                  <Chip
                    label={selectedEvent.type}
                    color={selectedEvent.color}
                    variant="outlined"
                    sx={{ mr: 1 }}
                  />
                  <Chip
                    label={selectedEvent.status}
                    color={selectedEvent.status === 'scheduled' ? 'success' : 'warning'}
                    variant="outlined"
                  />
                </Box>
              </Stack>
            </DialogContent>
            <DialogActions>
              <Button
                startIcon={<JoinIcon />}
                variant="contained"
                sx={{
                  background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
                }}
              >
                Join Meeting
              </Button>
              <Button onClick={() => setEventDetailOpen(false)}>
                Close
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {/* Event Menu */}
      <Menu
        anchorEl={eventMenuAnchor}
        open={Boolean(eventMenuAnchor)}
        onClose={() => setEventMenuAnchor(null)}
      >
        <MenuItem onClick={() => setEventMenuAnchor(null)}>
          <ListItemIcon>
            <JoinIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Join Meeting</ListItemText>
        </MenuItem>
        <MenuItem onClick={() => setEventMenuAnchor(null)}>
          <ListItemIcon>
            <EditIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Edit Meeting</ListItemText>
        </MenuItem>
        <MenuItem onClick={() => setEventMenuAnchor(null)}>
          <ListItemIcon>
            <DeleteIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Delete Meeting</ListItemText>
        </MenuItem>
      </Menu>
    </Box>
  );
};

export default CalendarView;