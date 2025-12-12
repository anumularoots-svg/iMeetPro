import React, { useState, useEffect, useMemo } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Button,
  Chip,
  Switch,
  FormControlLabel,
  Stack,
  Divider,
  Alert,
  useTheme,
  alpha,
  CircularProgress,
  IconButton,
  Tooltip
} from '@mui/material';
import {
  AccessTime as ClockIcon,
  CalendarToday as CalendarIcon,
  Refresh as RefreshIcon,
  Check as CheckIcon,
  Schedule as ScheduleIcon
} from '@mui/icons-material';
import { format, addMinutes, isSameDay, isAfter, isBefore } from 'date-fns';

const AvailabilitySlots = ({
  selectedDate,
  participants = [],
  onSlotSelect,
  selectedSlot,
  meetingDuration = 60,
  workingHours = { start: '09:00', end: '17:00' },
  timeZone = 'Asia/Kolkata',
  showAllSlots = false,
  businessHoursOnly = true
}) => {
  const theme = useTheme();
  const [loading, setLoading] = useState(false);
  const [availableSlots, setAvailableSlots] = useState([]);
  const [occupiedSlots, setOccupiedSlots] = useState([]);
  const [showAll, setShowAll] = useState(showAllSlots);
  const [businessOnly, setBusinessOnly] = useState(businessHoursOnly);
  const [timeFormat24, setTimeFormat24] = useState(false);

  // Generate time slots based on duration and working hours
  const generateTimeSlots = useMemo(() => {
    if (!selectedDate) return [];

    const slots = [];
    const startHour = parseInt(workingHours.start.split(':')[0]);
    const startMinute = parseInt(workingHours.start.split(':')[1]);
    const endHour = parseInt(workingHours.end.split(':')[0]);
    const endMinute = parseInt(workingHours.end.split(':')[1]);

    // Create slots for the entire day (24 hours) or business hours
    const dayStartHour = businessOnly ? startHour : 0;
    const dayEndHour = businessOnly ? endHour : 24;

    for (let hour = dayStartHour; hour < dayEndHour; hour++) {
      for (let minute = 0; minute < 60; minute += meetingDuration >= 60 ? 60 : 30) {
        const slotStart = new Date(selectedDate);
        slotStart.setHours(hour, minute, 0, 0);
        
        const slotEnd = addMinutes(slotStart, meetingDuration);
        
        // Skip if slot end goes beyond working hours (for business hours mode)
        if (businessOnly && slotEnd.getHours() > endHour) {
          continue;
        }

        // Skip past slots
        const now = new Date();
        if (isBefore(slotStart, now)) {
          continue;
        }

        const timeString = format(slotStart, 'HH:mm');
        const endTimeString = format(slotEnd, 'HH:mm');
        
        slots.push({
          id: `${format(selectedDate, 'yyyy-MM-dd')}-${timeString}`,
          startTime: timeString,
          endTime: endTimeString,
          fullStartTime: slotStart,
          fullEndTime: slotEnd,
          available: true, // Will be updated based on conflicts
          displayTime: format(slotStart, timeFormat24 ? 'HH:mm' : 'h:mm a'),
          isBusinessHours: hour >= startHour && hour < endHour,
          isAfterHours: hour < startHour || hour >= endHour
        });
      }
    }

    return slots;
  }, [selectedDate, meetingDuration, workingHours, businessOnly, timeFormat24]);

  // Simulate checking availability (replace with real API call)
  useEffect(() => {
    if (!selectedDate || generateTimeSlots.length === 0) return;

    setLoading(true);
    
    // Simulate API delay
    setTimeout(() => {
      // Mock some occupied slots for demonstration
      const mockOccupied = generateTimeSlots.slice(2, 4).concat(generateTimeSlots.slice(8, 10));
      setOccupiedSlots(mockOccupied.map(slot => slot.id));
      
      // Update availability
      const updatedSlots = generateTimeSlots.map(slot => ({
        ...slot,
        available: !mockOccupied.some(occupied => occupied.id === slot.id)
      }));
      
      setAvailableSlots(updatedSlots);
      setLoading(false);
    }, 500);
  }, [generateTimeSlots, selectedDate, participants]);

  // Get slots to display
  const slotsToDisplay = useMemo(() => {
    if (showAll) {
      return availableSlots;
    }
    return availableSlots.filter(slot => slot.available);
  }, [availableSlots, showAll]);

  // Get statistics
  const stats = useMemo(() => {
    const total = availableSlots.length;
    const available = availableSlots.filter(slot => slot.available).length;
    const occupied = total - available;
    
    return { total, available, occupied };
  }, [availableSlots]);

  const handleSlotClick = (slot) => {
    if (!slot.available) return;
    onSlotSelect?.(slot);
  };

  const handleRefresh = () => {
    setLoading(true);
    // Trigger re-fetch
    setTimeout(() => setLoading(false), 1000);
  };

  if (!selectedDate) {
    return (
      <Card sx={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Box textAlign="center">
          <CalendarIcon sx={{ fontSize: 64, color: 'text.disabled', mb: 2 }} />
          <Typography variant="h6" color="text.secondary">
            Please select a date
          </Typography>
        </Box>
      </Card>
    );
  }

  return (
    <Card sx={{ height: '100%', overflow: 'hidden' }}>
      <CardContent sx={{ p: 0, height: '100%', display: 'flex', flexDirection: 'column' }}>
        {/* Header */}
        <Box sx={{ p: 3, pb: 2, bgcolor: alpha(theme.palette.primary.main, 0.05) }}>
          <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
            <Box display="flex" alignItems="center" gap={1}>
              <ScheduleIcon color="primary" />
              <Typography variant="h6" fontWeight={600}>
                Available Time Slots - {timeZone}
              </Typography>
            </Box>
            <Tooltip title="Refresh availability">
              <IconButton onClick={handleRefresh} disabled={loading}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
          </Box>
          
          <Typography variant="body2" color="text.secondary" gutterBottom>
            {format(selectedDate, 'EEEE, MMMM dd, yyyy')} • All 24 Hours • {timeZone}
          </Typography>

          {/* Controls */}
          <Stack direction="row" spacing={2} alignItems="center" mt={2}>
            <FormControlLabel
              control={
                <Switch
                  checked={timeFormat24}
                  onChange={(e) => setTimeFormat24(e.target.checked)}
                  size="small"
                />
              }
              label="24-hour format"
            />
            
            <FormControlLabel
              control={
                <Switch
                  checked={showAll}
                  onChange={(e) => setShowAll(e.target.checked)}
                  size="small"
                />
              }
              label="Show all slots"
            />
            
            <FormControlLabel
              control={
                <Switch
                  checked={businessOnly}
                  onChange={(e) => setBusinessOnly(e.target.checked)}
                  size="small"
                />
              }
              label={`Business hours only (${workingHours.start} - ${workingHours.end} ${timeZone})`}
            />
          </Stack>

          {/* Statistics */}
          <Stack direction="row" spacing={1} mt={2}>
            <Chip
              label={`${stats.available} available`}
              color="success"
              size="small"
              icon={<CheckIcon />}
            />
            <Chip
              label={`${stats.occupied} occupied`}
              color="error"
              size="small"
            />
            <Chip
              label={`${meetingDuration} min slots`}
              color="info"
              size="small"
            />
            <Chip
              label={`${timeZone} Timezone`}
              color="primary"
              size="small"
            />
          </Stack>

          {/* Time format preview */}
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            Time format preview ({timeZone}): {format(new Date(), timeFormat24 ? 'HH:mm' : 'h:mm a')} - {format(addMinutes(new Date(), meetingDuration), timeFormat24 ? 'HH:mm' : 'h:mm a')}
          </Typography>
        </Box>

        <Divider />

        {/* Loading state */}
        {loading && (
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', p: 4 }}>
            <CircularProgress size={24} sx={{ mr: 2 }} />
            <Typography variant="body2" color="text.secondary">
              Loading available slots...
            </Typography>
          </Box>
        )}

        {/* Slots Grid */}
        {!loading && (
          <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
            {slotsToDisplay.length === 0 ? (
              <Alert severity="info" sx={{ m: 2 }}>
                <Typography variant="body2">
                  No available slots found for the selected criteria. Try adjusting your filters or selecting a different date.
                </Typography>
              </Alert>
            ) : (
              <Grid container spacing={1}>
                {slotsToDisplay.map((slot) => (
                  <Grid item xs={6} sm={4} md={3} key={slot.id}>
                    <Button
                      fullWidth
                      variant={selectedSlot?.id === slot.id ? "contained" : "outlined"}
                      onClick={() => handleSlotClick(slot)}
                      disabled={!slot.available}
                      sx={{
                        py: 1.5,
                        px: 2,
                        height: 'auto',
                        minHeight: 56,
                        borderRadius: 2,
                        textTransform: 'none',
                        fontWeight: selectedSlot?.id === slot.id ? 600 : 400,
                        position: 'relative',
                        ...(slot.available ? {
                          borderColor: selectedSlot?.id === slot.id ? 'primary.main' : alpha(theme.palette.success.main, 0.5),
                          backgroundColor: selectedSlot?.id === slot.id 
                            ? 'primary.main' 
                            : alpha(theme.palette.success.main, 0.05),
                          '&:hover': {
                            backgroundColor: selectedSlot?.id === slot.id 
                              ? 'primary.dark' 
                              : alpha(theme.palette.success.main, 0.1),
                            borderColor: selectedSlot?.id === slot.id ? 'primary.dark' : 'success.main'
                          }
                        } : {
                          backgroundColor: alpha(theme.palette.error.main, 0.05),
                          borderColor: alpha(theme.palette.error.main, 0.3),
                          color: alpha(theme.palette.text.disabled, 0.6)
                        }),
                        ...(slot.isAfterHours && {
                          '&::after': {
                            content: '"After Hours"',
                            position: 'absolute',
                            top: 4,
                            right: 4,
                            fontSize: '0.6rem',
                            color: theme.palette.warning.main,
                            backgroundColor: alpha(theme.palette.warning.main, 0.1),
                            borderRadius: 1,
                            px: 0.5,
                            py: 0.25
                          }
                        })
                      }}
                    >
                      <Box textAlign="center">
                        <Typography variant="body1" fontWeight="inherit">
                          {slot.displayTime}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {slot.startTime} - {slot.endTime} ({meetingDuration} minutes)
                        </Typography>
                        {slot.isAfterHours && (
                          <Typography variant="caption" color="warning.main" display="block">
                            After Hours
                          </Typography>
                        )}
                        {selectedSlot?.id === slot.id && (
                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', mt: 0.5 }}>
                            <CheckIcon sx={{ fontSize: 16, mr: 0.5 }} />
                            <Typography variant="caption">
                              Available
                            </Typography>
                          </Box>
                        )}
                      </Box>
                    </Button>
                  </Grid>
                ))}
              </Grid>
            )}
          </Box>
        )}

        {/* Selected slot info */}
        {selectedSlot && (
          <Box sx={{ p: 2, bgcolor: alpha(theme.palette.success.main, 0.05), borderTop: `1px solid ${alpha(theme.palette.divider, 0.1)}` }}>
            <Alert severity="success" sx={{ '& .MuiAlert-message': { width: '100%' } }}>
              <Box display="flex" alignItems="center" justifyContent="space-between">
                <Box>
                  <Typography variant="body2" fontWeight={600}>
                    Selected Time Slot
                  </Typography>
                  <Typography variant="body2">
                    {format(selectedDate, 'EEEE, MMMM dd, yyyy')} at {selectedSlot.displayTime}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {selectedSlot.startTime} - {selectedSlot.endTime} ({meetingDuration} minutes) • {timeZone}
                  </Typography>
                </Box>
                <ClockIcon color="success" />
              </Box>
            </Alert>
          </Box>
        )}
      </CardContent>
    </Card>
  );
};

export default AvailabilitySlots;