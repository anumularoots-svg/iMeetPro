import React, { useState } from 'react';
import {
  Box,
  Grid,
  Typography,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Card,
  CardContent,
  Chip,
  Stack,
  IconButton,
  Paper,
  Divider
} from '@mui/material';
import {
  DatePicker,
  TimePicker,
  LocalizationProvider
} from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import {
  Schedule as ScheduleIcon,
  Today as TodayIcon,
  AccessTime as TimeIcon,
  Repeat as RepeatIcon,
  Add as AddIcon,
  Close as CloseIcon
} from '@mui/icons-material';
import { format, addDays, addWeeks, addMonths } from 'date-fns';

const DateTimePicker = ({ 
  onDateTimeChange, 
  initialDateTime = new Date(),
  showRecurring = false,
  timezone = 'Asia/Kolkata'
}) => {
  const [selectedDate, setSelectedDate] = useState(initialDateTime);
  const [selectedTime, setSelectedTime] = useState(initialDateTime);
  const [duration, setDuration] = useState(60);
  const [isRecurring, setIsRecurring] = useState(false);
  const [recurringType, setRecurringType] = useState('weekly');
  const [customTimes, setCustomTimes] = useState([]);

  const quickDateOptions = [
    { label: 'Today', value: new Date(), icon: <TodayIcon /> },
    { label: 'Tomorrow', value: addDays(new Date(), 1), icon: <TodayIcon /> },
    { label: 'Next Week', value: addWeeks(new Date(), 1), icon: <ScheduleIcon /> },
    { label: 'Next Month', value: addMonths(new Date(), 1), icon: <ScheduleIcon /> }
  ];

  const quickTimeOptions = [
    '09:00', '10:00', '11:00', '14:00', '15:00', '16:00', '17:00', '18:00'
  ];

  const durationOptions = [
    { label: '15 min', value: 15 },
    { label: '30 min', value: 30 },
    { label: '45 min', value: 45 },
    { label: '1 hour', value: 60 },
    { label: '1.5 hours', value: 90 },
    { label: '2 hours', value: 120 },
    { label: 'Custom', value: 'custom' }
  ];

  const recurringOptions = [
    { label: 'Daily', value: 'daily' },
    { label: 'Weekly', value: 'weekly' },
    { label: 'Monthly', value: 'monthly' },
    { label: 'Custom', value: 'custom' }
  ];

  const handleDateChange = (newDate) => {
    setSelectedDate(newDate);
    updateDateTime(newDate, selectedTime);
  };

  const handleTimeChange = (newTime) => {
    setSelectedTime(newTime);
    updateDateTime(selectedDate, newTime);
  };

  const updateDateTime = (date, time) => {
    const combinedDateTime = new Date(date);
    combinedDateTime.setHours(time.getHours());
    combinedDateTime.setMinutes(time.getMinutes());
    
    if (onDateTimeChange) {
      onDateTimeChange({
        dateTime: combinedDateTime,
        duration,
        isRecurring,
        recurringType,
        timezone
      });
    }
  };

  const handleQuickDate = (date) => {
    setSelectedDate(date);
    updateDateTime(date, selectedTime);
  };

  const handleQuickTime = (timeString) => {
    const [hours, minutes] = timeString.split(':');
    const newTime = new Date();
    newTime.setHours(parseInt(hours), parseInt(minutes), 0, 0);
    setSelectedTime(newTime);
    updateDateTime(selectedDate, newTime);
  };

  const addCustomTime = () => {
    const newTime = new Date();
    setCustomTimes([...customTimes, newTime]);
  };

  const removeCustomTime = (index) => {
    setCustomTimes(customTimes.filter((_, i) => i !== index));
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <Card sx={{ p: 3, borderRadius: 3, boxShadow: 3 }}>
        <CardContent>
          <Typography variant="h5" gutterBottom sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <ScheduleIcon sx={{ mr: 1, color: 'primary.main' }} />
            Schedule Meeting
          </Typography>

          <Grid container spacing={3}>
            {/* Quick Date Selection */}
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                <TodayIcon sx={{ mr: 1 }} />
                Quick Date Selection
              </Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                {quickDateOptions.map((option, index) => (
                  <Chip
                    key={index}
                    icon={option.icon}
                    label={option.label}
                    onClick={() => handleQuickDate(option.value)}
                    color={format(selectedDate, 'yyyy-MM-dd') === format(option.value, 'yyyy-MM-dd') ? 'primary' : 'default'}
                    variant={format(selectedDate, 'yyyy-MM-dd') === format(option.value, 'yyyy-MM-dd') ? 'filled' : 'outlined'}
                    sx={{ cursor: 'pointer' }}
                  />
                ))}
              </Stack>
            </Grid>

            {/* Date Picker */}
            <Grid item xs={12} md={6}>
              <DatePicker
                label="Select Date"
                value={selectedDate}
                onChange={handleDateChange}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    fullWidth
                    variant="outlined"
                    sx={{
                      '& .MuiOutlinedInput-root': {
                        borderRadius: 2,
                      }
                    }}
                  />
                )}
                minDate={new Date()}
              />
            </Grid>

            {/* Time Picker */}
            <Grid item xs={12} md={6}>
              <TimePicker
                label="Select Time"
                value={selectedTime}
                onChange={handleTimeChange}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    fullWidth
                    variant="outlined"
                    sx={{
                      '& .MuiOutlinedInput-root': {
                        borderRadius: 2,
                      }
                    }}
                  />
                )}
              />
            </Grid>

            {/* Quick Time Selection */}
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                <TimeIcon sx={{ mr: 1 }} />
                Quick Time Selection
              </Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                {quickTimeOptions.map((time, index) => (
                  <Chip
                    key={index}
                    label={time}
                    onClick={() => handleQuickTime(time)}
                    color={format(selectedTime, 'HH:mm') === time ? 'primary' : 'default'}
                    variant={format(selectedTime, 'HH:mm') === time ? 'filled' : 'outlined'}
                    sx={{ cursor: 'pointer' }}
                  />
                ))}
              </Stack>
            </Grid>

            {/* Duration Selection */}
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom>
                Meeting Duration
              </Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                {durationOptions.map((option, index) => (
                  <Chip
                    key={index}
                    label={option.label}
                    onClick={() => setDuration(option.value)}
                    color={duration === option.value ? 'primary' : 'default'}
                    variant={duration === option.value ? 'filled' : 'outlined'}
                    sx={{ cursor: 'pointer' }}
                  />
                ))}
              </Stack>
              {duration === 'custom' && (
                <TextField
                  label="Custom Duration (minutes)"
                  type="number"
                  value={duration === 'custom' ? '' : duration}
                  onChange={(e) => setDuration(parseInt(e.target.value))}
                  sx={{ mt: 2, maxWidth: 200 }}
                  variant="outlined"
                />
              )}
            </Grid>

            {/* Recurring Options */}
            {showRecurring && (
              <>
                <Grid item xs={12}>
                  <Divider sx={{ my: 2 }} />
                  <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                    <RepeatIcon sx={{ mr: 1 }} />
                    Recurring Meeting
                  </Typography>
                  <Button
                    variant={isRecurring ? 'contained' : 'outlined'}
                    onClick={() => setIsRecurring(!isRecurring)}
                    sx={{ mb: 2 }}
                  >
                    {isRecurring ? 'Recurring Enabled' : 'Enable Recurring'}
                  </Button>
                </Grid>

                {isRecurring && (
                  <Grid item xs={12}>
                    <FormControl fullWidth sx={{ mb: 2 }}>
                      <InputLabel>Repeat Pattern</InputLabel>
                      <Select
                        value={recurringType}
                        onChange={(e) => setRecurringType(e.target.value)}
                        label="Repeat Pattern"
                      >
                        {recurringOptions.map((option) => (
                          <MenuItem key={option.value} value={option.value}>
                            {option.label}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  </Grid>
                )}
              </>
            )}

            {/* Custom Times */}
            <Grid item xs={12}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">
                  Additional Time Slots
                </Typography>
                <IconButton onClick={addCustomTime} color="primary">
                  <AddIcon />
                </IconButton>
              </Box>
              
              {customTimes.map((time, index) => (
                <Paper key={index} sx={{ p: 2, mb: 1, display: 'flex', alignItems: 'center' }}>
                  <TimePicker
                    value={time}
                    onChange={(newTime) => {
                      const updatedTimes = [...customTimes];
                      updatedTimes[index] = newTime;
                      setCustomTimes(updatedTimes);
                    }}
                    renderInput={(params) => (
                      <TextField {...params} size="small" />
                    )}
                  />
                  <IconButton onClick={() => removeCustomTime(index)} color="error">
                    <CloseIcon />
                  </IconButton>
                </Paper>
              ))}
            </Grid>

            {/* Summary */}
            <Grid item xs={12}>
              <Paper sx={{ p: 3, bgcolor: 'primary.50', borderRadius: 2 }}>
                <Typography variant="h6" gutterBottom color="primary.main">
                  Meeting Summary
                </Typography>
                <Typography variant="body1">
                  <strong>Date:</strong> {format(selectedDate, 'EEEE, MMMM d, yyyy')}
                </Typography>
                <Typography variant="body1">
                  <strong>Time:</strong> {format(selectedTime, 'h:mm a')} ({timezone})
                </Typography>
                <Typography variant="body1">
                  <strong>Duration:</strong> {duration} minutes
                </Typography>
                <Typography variant="body1">
                  <strong>End Time:</strong> {format(new Date(selectedTime.getTime() + duration * 60000), 'h:mm a')}
                </Typography>
                {isRecurring && (
                  <Typography variant="body1">
                    <strong>Recurring:</strong> {recurringType}
                  </Typography>
                )}
              </Paper>
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </LocalizationProvider>
  );
};

export default DateTimePicker;