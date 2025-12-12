// Complete MonthWeekDayView.jsx with All Views and Proper Styling - Fixed

import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  Card,
  CardContent,
  Chip,
  useTheme,
  alpha,
  Stack,
  Button
} from '@mui/material';
import {
  VideoCall,
  Schedule,
  Person,
  AccessTime,
  Add as AddIcon
} from '@mui/icons-material';
import { 
  format, 
  startOfMonth, 
  endOfMonth, 
  eachDayOfInterval, 
  isSameMonth, 
  isSameDay, 
  startOfWeek, 
  endOfWeek,
  addDays,
  isToday
} from 'date-fns';

const MonthWeekDayView = ({ viewMode, currentDate, meetings = [], onDateClick, onMeetingClick }) => {
  const theme = useTheme();

  // Get meetings for a specific date (only future meetings that are actually scheduled)
  const getMeetingsForDate = (date) => {
    if (!meetings || !Array.isArray(meetings)) return [];
    
    const now = new Date();
    
    return meetings.filter(meeting => {
      if (!meeting.startTime) return false;
      
      try {
        const meetingStartTime = new Date(meeting.startTime);
        
        // Only show meetings that:
        // 1. Are on the same day as the requested date
        // 2. Haven't started yet (future meetings only)
        // 3. Are actual scheduled meetings (have required fields like title, organizer, etc.)
        return isSameDay(meetingStartTime, date) && 
               meetingStartTime > now &&
               meeting.title && 
               meeting.title.trim() !== '' &&
               !meeting.isDummy; // Exclude any meetings marked as dummy/test data
      } catch (error) {
        console.error('Error parsing meeting date:', meeting.startTime, error);
        return false;
      }
    });
  };

  // Format time properly
  const formatTime = (timeString) => {
    try {
      const date = new Date(timeString);
      return format(date, 'HH:mm');
    } catch (error) {
      console.error('Error formatting time:', timeString);
      return '00:00';
    }
  };

  const renderMonthView = () => {
    const monthStart = startOfMonth(currentDate);
    const monthEnd = endOfMonth(currentDate);
    const calendarStart = startOfWeek(monthStart);
    const calendarEnd = endOfWeek(monthEnd);
    const calendarDays = eachDayOfInterval({ start: calendarStart, end: calendarEnd });

    const weekDays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    return (
      <Paper elevation={3} sx={{ borderRadius: 3, overflow: 'hidden' }}>
        {/* Month Header */}
        <Box
          sx={{
            background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
            color: 'white',
            p: 2,
            textAlign: 'center'
          }}
        >
          <Typography variant="h5" fontWeight="bold">
            {format(currentDate, 'MMMM yyyy')}
          </Typography>
        </Box>

        {/* Week Headers */}
        <Grid container spacing={0}>
          {weekDays.map((day) => (
            <Grid item xs key={day}>
              <Typography 
                variant="subtitle2" 
                sx={{ 
                  textAlign: 'center', 
                  fontWeight: 'bold',
                  color: theme.palette.text.secondary,
                  py: 2,
                  bgcolor: alpha(theme.palette.background.default, 0.5),
                  borderBottom: `2px solid ${alpha(theme.palette.divider, 0.1)}`
                }}
              >
                {day}
              </Typography>
            </Grid>
          ))}
        </Grid>

        {/* Calendar Days */}
        <Grid container spacing={0}>
          {calendarDays.map((day, index) => {
            const dayMeetings = getMeetingsForDate(day);
            const isCurrentMonth = isSameMonth(day, currentDate);
            const isTodayDate = isToday(day);

            return (
              <Grid item xs key={index}>
                <Box
                  onClick={() => onDateClick(day)}
                  sx={{
                    minHeight: 120,
                    cursor: 'pointer',
                    border: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
                    backgroundColor: isTodayDate 
                      ? alpha(theme.palette.primary.main, 0.1)
                      : isCurrentMonth ? theme.palette.background.paper : alpha(theme.palette.grey[100], 0.5),
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.primary.main, 0.05),
                      borderColor: theme.palette.primary.main
                    },
                    transition: 'all 0.2s ease-in-out',
                    p: 1,
                    display: 'flex',
                    flexDirection: 'column'
                  }}
                >
                  <Typography
                    variant="body2"
                    sx={{
                      fontWeight: isTodayDate ? 'bold' : 'normal',
                      color: isCurrentMonth ? theme.palette.text.primary : theme.palette.text.disabled,
                      mb: 1,
                      textAlign: 'center',
                      ...(isTodayDate && {
                        bgcolor: theme.palette.primary.main,
                        color: theme.palette.primary.contrastText,
                        borderRadius: '50%',
                        width: 28,
                        height: 28,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        margin: '0 auto 8px',
                        fontSize: '0.875rem'
                      })
                    }}
                  >
                    {format(day, 'd')}
                  </Typography>
                  
                  <Box sx={{ flexGrow: 1, overflow: 'hidden' }}>
                    {dayMeetings.slice(0, 3).map((meeting, idx) => (
                      <Chip
                        key={idx}
                        label={`${formatTime(meeting.startTime)} ${meeting.title}`}
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          onMeetingClick(meeting);
                        }}
                        sx={{
                          fontSize: '10px',
                          height: 20,
                          mb: 0.5,
                          width: '100%',
                          justifyContent: 'flex-start',
                          backgroundColor: alpha(theme.palette.primary.main, 0.2),
                          color: theme.palette.primary.dark,
                          border: `1px solid ${alpha(theme.palette.primary.main, 0.3)}`,
                          '&:hover': {
                            backgroundColor: alpha(theme.palette.primary.main, 0.3)
                          },
                          '& .MuiChip-label': {
                            whiteSpace: 'nowrap',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            maxWidth: '100%',
                            px: 0.5
                          }
                        }}
                      />
                    ))}
                    
                    {dayMeetings.length > 3 && (
                      <Typography variant="caption" color="text.secondary" sx={{ fontSize: '10px', textAlign: 'center', display: 'block' }}>
                        +{dayMeetings.length - 3} more
                      </Typography>
                    )}
                  </Box>
                </Box>
              </Grid>
            );
          })}
        </Grid>
      </Paper>
    );
  };

  const renderWeekView = () => {
    const weekStart = startOfWeek(currentDate);
    const weekDays = eachDayOfInterval({ 
      start: weekStart, 
      end: addDays(weekStart, 6) 
    });

    const hours = Array.from({ length: 12 }, (_, i) => i + 8); // 8 AM to 8 PM

    return (
      <Paper elevation={3} sx={{ borderRadius: 3, overflow: 'hidden' }}>
        {/* Week Header */}
        <Box
          sx={{
            background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
            color: 'white',
            p: 2,
            textAlign: 'center'
          }}
        >
          <Typography variant="h5" fontWeight="bold">
            {format(weekStart, 'MMM dd')} - {format(addDays(weekStart, 6), 'MMM dd, yyyy')}
          </Typography>
        </Box>

        <Grid container spacing={0}>
          {/* Time Column */}
          <Grid item xs={1}>
            <Box sx={{ bgcolor: alpha(theme.palette.background.default, 0.5), pt: 8 }}>
              {hours.map((hour) => (
                <Box key={hour} sx={{ height: 60, display: 'flex', alignItems: 'center', justifyContent: 'center', borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}` }}>
                  <Typography variant="caption" color="text.secondary" fontWeight="medium">
                    {format(new Date().setHours(hour, 0, 0, 0), 'ha')}
                  </Typography>
                </Box>
              ))}
            </Box>
          </Grid>

          {/* Days Columns */}
          {weekDays.map((day) => {
            const dayMeetings = getMeetingsForDate(day);
            const isTodayDate = isToday(day);

            return (
              <Grid item xs key={day.toString()}>
                <Box 
                  sx={{ 
                    textAlign: 'center', 
                    p: 2, 
                    cursor: 'pointer',
                    bgcolor: isTodayDate ? alpha(theme.palette.primary.main, 0.1) : 'transparent',
                    borderBottom: `2px solid ${alpha(theme.palette.divider, 0.1)}`,
                    '&:hover': {
                      bgcolor: alpha(theme.palette.primary.main, 0.05)
                    }
                  }}
                  onClick={() => onDateClick(day)}
                >
                  <Typography
                    variant="subtitle2"
                    sx={{
                      fontWeight: isTodayDate ? 'bold' : 'normal',
                      color: isTodayDate ? theme.palette.primary.main : theme.palette.text.primary
                    }}
                  >
                    {format(day, 'EEE')}
                  </Typography>
                  <Typography
                    variant="h6"
                    sx={{
                      fontWeight: isTodayDate ? 'bold' : 'normal',
                      color: isTodayDate ? theme.palette.primary.main : theme.palette.text.primary,
                      ...(isTodayDate && {
                        bgcolor: theme.palette.primary.main,
                        color: theme.palette.primary.contrastText,
                        borderRadius: '50%',
                        width: 32,
                        height: 32,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        margin: '4px auto 0'
                      })
                    }}
                  >
                    {format(day, 'd')}
                  </Typography>
                </Box>

                <Box sx={{ position: 'relative', minHeight: 720 }}>
                  {/* Hour Grid Lines */}
                  {hours.map((hour) => (
                    <Box
                      key={hour}
                      sx={{
                        position: 'absolute',
                        top: (hour - 8) * 60,
                        width: '100%',
                        height: 60,
                        borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
                        '&:hover': {
                          backgroundColor: alpha(theme.palette.primary.main, 0.03)
                        }
                      }}
                    />
                  ))}

                  {/* Meetings */}
                  {dayMeetings.map((meeting, idx) => {
                    const startTime = new Date(meeting.startTime);
                    const endTime = new Date(meeting.endTime);
                    const startHour = startTime.getHours();
                    const startMinute = startTime.getMinutes();
                    const endHour = endTime.getHours();
                    const endMinute = endTime.getMinutes();
                    
                    const top = (startHour - 8) * 60 + (startMinute / 60) * 60;
                    const height = Math.max(30, ((endHour - startHour) * 60 + (endMinute - startMinute)) * (60/60));

                    return (
                      <Card
                        key={idx}
                        elevation={4}
                        onClick={() => onMeetingClick(meeting)}
                        sx={{
                          position: 'absolute',
                          top: top,
                          left: 4,
                          right: 4,
                          height: height,
                          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.9)}, ${theme.palette.primary.dark})`,
                          color: 'white',
                          cursor: 'pointer',
                          '&:hover': {
                            backgroundColor: theme.palette.primary.main,
                            transform: 'scale(1.02)'
                          },
                          transition: 'all 0.2s ease-in-out',
                          zIndex: 1
                        }}
                      >
                        <CardContent sx={{ p: 1 }}>
                          <Typography variant="caption" sx={{ fontWeight: 'bold', fontSize: '11px', display: 'block' }}>
                            {meeting.title}
                          </Typography>
                          <Typography variant="caption" sx={{ fontSize: '10px', opacity: 0.9 }}>
                            {formatTime(meeting.startTime)}
                          </Typography>
                        </CardContent>
                      </Card>
                    );
                  })}
                </Box>
              </Grid>
            );
          })}
        </Grid>
      </Paper>
    );
  };

  const renderDayView = () => {
    const hours = Array.from({ length: 16 }, (_, i) => i + 6); // 6 AM to 10 PM
    const dayMeetings = getMeetingsForDate(currentDate);

    return (
      <Paper 
        elevation={3} 
        sx={{ 
          borderRadius: 3,
          overflow: 'hidden',
          border: `1px solid ${alpha(theme.palette.divider, 0.2)}`
        }}
      >
        {/* Day Header */}
        <Box
          sx={{
            background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
            color: 'white',
            p: 3,
            textAlign: 'center'
          }}
        >
          <Typography variant="h4" fontWeight="bold" gutterBottom>
            {format(currentDate, 'EEEE')}
          </Typography>
          <Typography variant="h6" sx={{ opacity: 0.9 }}>
            {format(currentDate, 'MMMM dd, yyyy')}
          </Typography>
          {dayMeetings.length > 0 && (
            <Chip
              label={`${dayMeetings.length} meeting${dayMeetings.length !== 1 ? 's' : ''}`}
              sx={{
                mt: 2,
                bgcolor: alpha(theme.palette.common.white, 0.2),
                color: 'white',
                fontWeight: 'bold'
              }}
            />
          )}
        </Box>

        {/* Time Grid Container */}
        <Box sx={{ display: 'flex', height: '600px', overflow: 'hidden' }}>
          {/* Time Column */}
          <Box
            sx={{
              width: 120,
              bgcolor: alpha(theme.palette.background.default, 0.5),
              borderRight: `2px solid ${alpha(theme.palette.divider, 0.1)}`
            }}
          >
            {hours.map((hour) => (
              <Box
                key={hour}
                sx={{
                  height: '37.5px', // 600px / 16 hours
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
                  bgcolor: hour % 2 === 0 ? alpha(theme.palette.primary.main, 0.02) : 'transparent'
                }}
              >
                <Typography 
                  variant="body2" 
                  color="text.secondary"
                  fontWeight="medium"
                  sx={{ fontSize: '0.875rem' }}
                >
                  {format(new Date().setHours(hour, 0, 0, 0), 'h:mm a')}
                </Typography>
              </Box>
            ))}
          </Box>

          {/* Events Column */}
          <Box sx={{ flex: 1, position: 'relative', bgcolor: 'background.paper' }}>
            {/* Hour Grid Lines */}
            {hours.map((hour, index) => (
              <Box
                key={hour}
                sx={{
                  position: 'absolute',
                  top: index * 37.5,
                  left: 0,
                  right: 0,
                  height: '37.5px',
                  borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
                  cursor: 'pointer',
                  transition: 'background-color 0.2s ease',
                  '&:hover': {
                    bgcolor: alpha(theme.palette.primary.main, 0.03)
                  }
                }}
                onClick={() => {
                  const clickDate = new Date(currentDate);
                  clickDate.setHours(hour, 0, 0, 0);
                  onDateClick(clickDate);
                }}
              />
            ))}

            {/* Current Time Indicator */}
            {isToday(currentDate) && (() => {
              const now = new Date();
              const currentHour = now.getHours();
              const currentMinute = now.getMinutes();
              
              if (currentHour >= 6 && currentHour <= 21) {
                const topPosition = ((currentHour - 6) * 37.5) + (currentMinute / 60 * 37.5);
                
                return (
                  <Box
                    sx={{
                      position: 'absolute',
                      top: topPosition,
                      left: 0,
                      right: 0,
                      height: 2,
                      bgcolor: theme.palette.error.main,
                      zIndex: 1000,
                      '&::before': {
                        content: '""',
                        position: 'absolute',
                        left: -6,
                        top: -4,
                        width: 12,
                        height: 12,
                        borderRadius: '50%',
                        bgcolor: theme.palette.error.main
                      }
                    }}
                  />
                );
              }
              return null;
            })()}

            {/* Meetings */}
            {dayMeetings.map((meeting, idx) => {
              const startTime = new Date(meeting.startTime);
              const endTime = new Date(meeting.endTime);
              const startHour = startTime.getHours();
              const startMinute = startTime.getMinutes();
              const endHour = endTime.getHours();
              const endMinute = endTime.getMinutes();
              
              // Calculate position and height
              const topPosition = ((startHour - 6) * 37.5) + (startMinute / 60 * 37.5);
              const duration = ((endHour - startHour) * 60) + (endMinute - startMinute);
              const height = Math.max(30, (duration / 60) * 37.5);

              return (
                <Card
                  key={idx}
                  elevation={4}
                  onClick={() => onMeetingClick(meeting)}
                  sx={{
                    position: 'absolute',
                    top: topPosition,
                    left: 12,
                    right: 12,
                    height: height,
                    background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.9)}, ${theme.palette.primary.dark})`,
                    color: 'white',
                    cursor: 'pointer',
                    zIndex: 10,
                    border: `2px solid ${theme.palette.primary.light}`,
                    borderRadius: 2,
                    overflow: 'hidden',
                    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                    '&:hover': {
                      transform: 'scale(1.02) translateX(4px)',
                      boxShadow: theme.shadows[12],
                      background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
                    }
                  }}
                >
                  <CardContent sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
                    {/* Meeting Header */}
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                      <VideoCall sx={{ mr: 1, fontSize: 20 }} />
                      <Typography 
                        variant="subtitle1" 
                        sx={{ 
                          fontWeight: 'bold',
                          fontSize: '0.95rem',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          flex: 1
                        }}
                      >
                        {meeting.title}
                      </Typography>
                    </Box>
                    
                    {/* Time */}
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                      <AccessTime sx={{ mr: 1, fontSize: 16, opacity: 0.9 }} />
                      <Typography variant="body2" sx={{ fontSize: '0.8rem', opacity: 0.9 }}>
                        {formatTime(meeting.startTime)} - {formatTime(meeting.endTime)}
                      </Typography>
                    </Box>

                    {/* Organizer */}
                    <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                      <Person sx={{ mr: 1, fontSize: 16, opacity: 0.9 }} />
                      <Typography 
                        variant="body2" 
                        sx={{ 
                          fontSize: '0.8rem', 
                          opacity: 0.9,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap'
                        }}
                      >
                        {meeting.organizer}
                      </Typography>
                    </Box>

                    {/* Description */}
                    {meeting.description && height > 80 && (
                      <Typography 
                        variant="caption" 
                        sx={{ 
                          fontSize: '0.75rem',
                          opacity: 0.8,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          display: '-webkit-box',
                          WebkitLineClamp: 2,
                          WebkitBoxOrient: 'vertical',
                          mt: 'auto'
                        }}
                      >
                        {meeting.description}
                      </Typography>
                    )}

                    {/* Status Badge */}
                    <Box sx={{ mt: 'auto', pt: 1 }}>
                      <Chip
                        label={meeting.status || 'Scheduled'}
                        size="small"
                        sx={{
                          bgcolor: alpha(theme.palette.common.white, 0.2),
                          color: 'white',
                          fontSize: '0.7rem',
                          height: 20
                        }}
                      />
                    </Box>
                  </CardContent>
                </Card>
              );
            })}

            {/* No meetings state */}
            {dayMeetings.length === 0 && (
              <Box 
                sx={{ 
                  position: 'absolute',
                  top: '50%',
                  left: '50%',
                  transform: 'translate(-50%, -50%)',
                  textAlign: 'center',
                  color: theme.palette.text.secondary
                }}
              >
                <Schedule sx={{ fontSize: 80, mb: 2, opacity: 0.3 }} />
                <Typography variant="h5" gutterBottom sx={{ fontWeight: 'bold' }}>
                  No meetings today
                </Typography>
                <Typography variant="body1" sx={{ mb: 3 }}>
                  Your day is completely free!
                </Typography>
                <Button
                  variant="contained"
                  startIcon={<AddIcon />}
                  onClick={() => onDateClick(currentDate)}
                  sx={{
                    borderRadius: 3,
                    px: 4,
                    py: 1.5,
                    background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
                  }}
                >
                  Schedule Meeting
                </Button>
              </Box>
            )}
          </Box>
        </Box>

        {/* Footer with Quick Actions */}
        <Box
          sx={{
            p: 2,
            bgcolor: alpha(theme.palette.background.default, 0.5),
            borderTop: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center'
          }}
        >
          <Typography variant="body2" color="text.secondary">
            {isToday(currentDate) ? 'Today' : format(currentDate, 'MMM dd, yyyy')} • {dayMeetings.length} meeting{dayMeetings.length !== 1 ? 's' : ''}
          </Typography>
          
          <Stack direction="row" spacing={1}>
            <Button
              variant="outlined"
              size="small"
              startIcon={<AddIcon />}
              onClick={() => onDateClick(currentDate)}
              sx={{ borderRadius: 2 }}
            >
              Add Meeting
            </Button>
            {isToday(currentDate) && (
              <Button
                variant="contained"
                size="small"
                onClick={() => {
                  const now = new Date();
                  const element = document.querySelector(`[data-hour="${now.getHours()}"]`);
                  element?.scrollIntoView({ behavior: 'smooth', block: 'center' });
                }}
                sx={{ borderRadius: 2 }}
              >
                Go to Now
              </Button>
            )}
          </Stack>
        </Box>
      </Paper>
    );
  };  

  // Render the correct view based on viewMode
  return (
    <Box>
      {viewMode === 'month' && renderMonthView()}
      {viewMode === 'week' && renderWeekView()}
      {viewMode === 'day' && renderDayView()}
    </Box>
  );
};
export default MonthWeekDayView;