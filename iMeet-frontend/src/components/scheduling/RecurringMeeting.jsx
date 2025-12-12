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
  Paper,
  Switch,
  FormControlLabel,
  Divider,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  IconButton
} from '@mui/material';
import {
  Repeat as RepeatIcon,
  Event as EventIcon,
  Today as TodayIcon,
  CalendarMonth as CalendarIcon,
  Preview as PreviewIcon,
  Schedule as ScheduleIcon,
  AccessTime as TimeIcon,
  Close as CloseIcon
} from '@mui/icons-material';
import { DatePicker, LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { format, addDays, addWeeks, addMonths } from 'date-fns';

const RecurringMeeting = ({ onRecurringChange, initialData = {} }) => {
  const [isRecurring, setIsRecurring] = useState(initialData.isRecurring || false);
  const [recurringType, setRecurringType] = useState(initialData.recurringType || 'weekly');
  const [interval, setInterval] = useState(initialData.interval || 1);
  const [endDate, setEndDate] = useState(initialData.endDate || null);
  const [endType, setEndType] = useState(initialData.endType || 'never');
  const [occurrences, setOccurrences] = useState(initialData.occurrences || 10);
  const [selectedDays, setSelectedDays] = useState(initialData.selectedDays || []);
  const [customPattern, setCustomPattern] = useState(initialData.customPattern || '');
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewDates, setPreviewDates] = useState([]);
  const [selectedMonthDates, setSelectedMonthDates] = useState(initialData.selectedMonthDates || []);
  const [monthlyPattern, setMonthlyPattern] = useState(initialData.monthlyPattern || 'same-date');

  const recurringTypes = [
    { value: 'daily', label: 'Daily', icon: <TodayIcon />, description: 'Every day' },
    { value: 'weekly', label: 'Weekly', icon: <EventIcon />, description: 'Specific days of the week' },
    { value: 'monthly', label: 'Monthly', icon: <CalendarIcon />, description: 'Same date each month' },
    { value: 'custom', label: 'Custom', icon: <ScheduleIcon />, description: 'Custom pattern' }
  ];

  const weekDays = [
    { value: 0, label: 'Sun', fullLabel: 'Sunday', color: '#f44336' },
    { value: 1, label: 'Mon', fullLabel: 'Monday', color: '#2196f3' },
    { value: 2, label: 'Tue', fullLabel: 'Tuesday', color: '#4caf50' },
    { value: 3, label: 'Wed', fullLabel: 'Wednesday', color: '#ff9800' },
    { value: 4, label: 'Thu', fullLabel: 'Thursday', color: '#9c27b0' },
    { value: 5, label: 'Fri', fullLabel: 'Friday', color: '#3f51b5' },
    { value: 6, label: 'Sat', fullLabel: 'Saturday', color: '#795548' }
  ];

  const endTypes = [
    { value: 'never', label: 'Never ends', description: 'Continue indefinitely' },
    { value: 'date', label: 'End by date', description: 'Stop on specific date' },
    { value: 'occurrences', label: 'After number of meetings', description: 'Stop after X meetings' }
  ];

  const quickIntervals = {
    daily: [1, 2, 3, 7],
    weekly: [1, 2, 3, 4],
    monthly: [1, 2, 3, 6, 12]
  };

  const handleRecurringToggle = (event) => {
    const enabled = event.target.checked;
    setIsRecurring(enabled);
    updateRecurringData({ isRecurring: enabled });
  };

  const handleTypeChange = (type) => {
    setRecurringType(type);
    // Reset interval to 1 when changing type
    setInterval(1);
    // Set default days for weekly
    if (type === 'weekly' && selectedDays.length === 0) {
      const today = new Date().getDay();
      setSelectedDays([today]);
    }
    updateRecurringData({ recurringType: type, interval: 1 });
  };

  const handleDayToggle = (dayValue) => {
    const newSelectedDays = selectedDays.includes(dayValue)
      ? selectedDays.filter(day => day !== dayValue)
      : [...selectedDays, dayValue].sort();
    
    setSelectedDays(newSelectedDays);
    updateRecurringData({ selectedDays: newSelectedDays });
  };

  const updateRecurringData = (updates) => {
    const data = {
      isRecurring,
      recurringType,
      interval,
      endDate,
      endType,
      occurrences,
      selectedDays,
      customPattern,
      selectedMonthDates,
    monthlyPattern,
      ...updates
    };

    if (onRecurringChange) {
      onRecurringChange(data);
    }
  };

  const generatePreviewDates = () => {
    if (!isRecurring) return [];

    const dates = [];
    let currentDate = new Date();
    const maxDates = endType === 'occurrences' ? occurrences : 20;

    for (let i = 0; i < maxDates && dates.length < 20; i++) {
      if (endType === 'date' && endDate && currentDate > endDate) break;

      switch (recurringType) {
        case 'daily':
          dates.push(new Date(currentDate));
          currentDate = addDays(currentDate, interval);
          break;
        
        case 'weekly':
          if (selectedDays.length === 0) break;
          
          // For the first iteration, find the next occurrence of selected days
          if (i === 0) {
            for (const day of selectedDays.sort()) {
              const nextDate = new Date(currentDate);
              let daysToAdd = (day - nextDate.getDay() + 7) % 7;
              if (daysToAdd === 0 && nextDate.getTime() < new Date().getTime()) {
                daysToAdd = 7; // If it's today but in the past, move to next week
              }
              nextDate.setDate(nextDate.getDate() + daysToAdd);
              
              if (endType !== 'date' || !endDate || nextDate <= endDate) {
                dates.push(new Date(nextDate));
              }
            }
          } else {
            // For subsequent iterations, add interval weeks
            const baseDate = dates[dates.length - selectedDays.length] || currentDate;
            const nextWeek = addWeeks(baseDate, interval);
            
            for (const day of selectedDays.sort()) {
              const nextDate = new Date(nextWeek);
              const daysToAdd = (day - nextDate.getDay() + 7) % 7;
              nextDate.setDate(nextDate.getDate() + daysToAdd);
              
              if (endType !== 'date' || !endDate || nextDate <= endDate) {
                dates.push(new Date(nextDate));
              }
            }
          }
          break;
        
        case 'monthly':
          dates.push(new Date(currentDate));
          currentDate = addMonths(currentDate, interval);
          break;
        
        default:
          break;
      }
    }

    // Sort dates and limit to 20
    return dates.sort((a, b) => a - b).slice(0, 20);
  };

  const handlePreview = () => {
    const dates = generatePreviewDates();
    setPreviewDates(dates);
    setPreviewOpen(true);
  };
const handleMonthlyPatternChange = (pattern) => {
  setMonthlyPattern(pattern);
  updateRecurringData({ monthlyPattern: pattern });
};

// CRITICAL FIX: Add this function to handle month date selection
const handleMonthDateToggle = (date) => {
  const newSelectedDates = selectedMonthDates.includes(date)
    ? selectedMonthDates.filter(d => d !== date)
    : [...selectedMonthDates, date].sort((a, b) => a - b);
  
  setSelectedMonthDates(newSelectedDates);
  updateRecurringData({ selectedMonthDates: newSelectedDates });
};
  const getPatternDescription = () => {
    if (!isRecurring) return '';
    
    let description = '';
    
    switch (recurringType) {
      case 'daily':
        description = interval === 1 ? 'Every day' : `Every ${interval} days`;
        break;
      case 'weekly':
        const dayNames = selectedDays.map(day => weekDays.find(d => d.value === day)?.label).join(', ');
        description = interval === 1 
          ? `Every week on ${dayNames}` 
          : `Every ${interval} weeks on ${dayNames}`;
        break;
      case 'monthly':
        description = interval === 1 ? 'Every month' : `Every ${interval} months`;
        break;
      case 'custom':
        description = customPattern || 'Custom pattern';
        break;
    }
    
    return description;
  };

  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <Card sx={{ borderRadius: 3, boxShadow: 3, overflow: 'visible' }}>
        <CardContent sx={{ p: 4 }}>
          {/* Header */}
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 4 }}>
            <Box sx={{ 
              p: 2, 
              borderRadius: 2, 
              bgcolor: 'primary.50', 
              display: 'flex', 
              alignItems: 'center',
              mr: 2 
            }}>
              <RepeatIcon sx={{ color: 'primary.main', fontSize: 32 }} />
            </Box>
            <Box>
              <Typography variant="h4" color="primary.main" fontWeight="bold">
                Recurring Meeting
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Set up automatic recurring patterns for your meetings
              </Typography>
            </Box>
          </Box>

          {/* Enable Recurring Toggle */}
          <Card sx={{ mb: 4, bgcolor: 'grey.50', border: '2px dashed', borderColor: 'primary.200' }}>
            <CardContent sx={{ p: 3 }}>
              <FormControlLabel
                control={
                  <Switch
                    checked={isRecurring}
                    onChange={handleRecurringToggle}
                    color="primary"
                    size="large"
                    sx={{ 
                      '& .MuiSwitch-thumb': {
                        width: 28,
                        height: 28,
                      },
                      '& .MuiSwitch-track': {
                        borderRadius: 15,
                      }
                    }}
                  />
                }
                label={
                  <Box>
                    <Typography variant="h5" fontWeight="bold">
                      {isRecurring ? '🔄 Recurring Enabled' : '⚡ Enable Recurring'}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                      {isRecurring 
                        ? 'This meeting will repeat automatically based on your settings below'
                        : 'Turn on to schedule this meeting to repeat automatically'
                      }
                    </Typography>
                  </Box>
                }
                sx={{ m: 0 }}
              />
            </CardContent>
          </Card>

          {isRecurring && (
            <>
              {/* Recurring Type Selection */}
              <Box sx={{ mb: 4 }}>
                <Typography variant="h5" gutterBottom sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
                  <TimeIcon sx={{ mr: 1 }} />
                  Choose Repeat Pattern
                </Typography>
                
                <Grid container spacing={2}>
                  {recurringTypes.map((type) => (
                    <Grid item xs={12} sm={6} md={3} key={type.value}>
                      <Card
                        onClick={() => handleTypeChange(type.value)}
                        sx={{
                          cursor: 'pointer',
                          transition: 'all 0.3s ease',
                          border: recurringType === type.value ? '2px solid' : '1px solid',
                          borderColor: recurringType === type.value ? 'primary.main' : 'grey.300',
                          bgcolor: recurringType === type.value ? 'primary.50' : 'background.paper',
                          '&:hover': {
                            transform: 'translateY(-4px)',
                            boxShadow: 4,
                            borderColor: 'primary.main'
                          }
                        }}
                      >
                        <CardContent sx={{ textAlign: 'center', p: 3 }}>
                          <Box sx={{ 
                            color: recurringType === type.value ? 'primary.main' : 'text.secondary',
                            mb: 2 
                          }}>
                            {React.cloneElement(type.icon, { sx: { fontSize: 40 } })}
                          </Box>
                          <Typography variant="h6" fontWeight="bold" gutterBottom>
                            {type.label}
                          </Typography>
                          <Typography variant="body2" color="text.secondary">
                            {type.description}
                          </Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              </Box>

              {/* Interval Settings */}
              <Box sx={{ mb: 4 }}>
                <Typography variant="h6" gutterBottom>
                  Frequency
                </Typography>
                
                <Grid container spacing={3} alignItems="center">
                  <Grid item xs={12} md={6}>
                    <TextField
                      label={`Repeat every ${
                        recurringType === 'daily' ? 'day(s)' : 
                        recurringType === 'weekly' ? 'week(s)' : 
                        recurringType === 'monthly' ? 'month(s)' : 'interval'
                      }`}
                      type="number"
                      value={interval}
                      onChange={(e) => {
                        const val = Math.max(1, parseInt(e.target.value) || 1);
                        setInterval(val);
                        updateRecurringData({ interval: val });
                      }}
                      inputProps={{ min: 1, max: 30 }}
                      fullWidth
                      variant="outlined"
                      size="large"
                    />
                  </Grid>
                  
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                      Quick select:
                    </Typography>
                    <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                      {quickIntervals[recurringType]?.map((quickInterval) => (
                        <Chip
                          key={quickInterval}
                          label={quickInterval}
                          onClick={() => {
                            setInterval(quickInterval);
                            updateRecurringData({ interval: quickInterval });
                          }}
                          color={interval === quickInterval ? 'primary' : 'default'}
                          variant={interval === quickInterval ? 'filled' : 'outlined'}
                          sx={{ cursor: 'pointer' }}
                        />
                      ))}
                    </Stack>
                  </Grid>
                </Grid>
              </Box>

              {/* Weekly Days Selection */}
              {/* Weekly Days Selection */}
              {recurringType === 'weekly' && (
                <Box sx={{ mb: 4 }}>
                  <Typography variant="h6" gutterBottom>
                    Select Days of the Week
                  </Typography>
                  
                  {/* Quick Select Options */}
                  <Box sx={{ mb: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => {
                        setSelectedDays([1, 2, 3, 4, 5]); // Monday to Friday
                        updateRecurringData({ selectedDays: [1, 2, 3, 4, 5] });
                      }}
                      sx={{ borderRadius: 3 }}
                    >
                      Weekdays Only
                    </Button>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => {
                        setSelectedDays([0, 6]); // Sunday and Saturday
                        updateRecurringData({ selectedDays: [0, 6] });
                      }}
                      sx={{ borderRadius: 3 }}
                    >
                      Weekends Only
                    </Button>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => {
                        setSelectedDays([0, 1, 2, 3, 4, 5, 6]); // All days
                        updateRecurringData({ selectedDays: [0, 1, 2, 3, 4, 5, 6] });
                      }}
                      sx={{ borderRadius: 3 }}
                    >
                      Every Day
                    </Button>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => {
                        setSelectedDays([]);
                        updateRecurringData({ selectedDays: [] });
                      }}
                      sx={{ borderRadius: 3 }}
                    >
                      Clear All
                    </Button>
                  </Box>
                  
                  <Paper sx={{ p: 3, bgcolor: 'grey.50' }}>
                    <Grid container spacing={1}>
                      {weekDays.map((day) => (
                        <Grid item xs={12/7} key={day.value}>
                          <Button
                            onClick={() => handleDayToggle(day.value)}
                            variant={selectedDays.includes(day.value) ? 'contained' : 'outlined'}
                            sx={{
                              width: '100%',
                              aspectRatio: '1',
                              minWidth: 'auto',
                              borderRadius: 2,
                              bgcolor: selectedDays.includes(day.value) ? day.color : 'transparent',
                              borderColor: day.color,
                              color: selectedDays.includes(day.value) ? 'white' : day.color,
                              '&:hover': {
                                bgcolor: selectedDays.includes(day.value) ? day.color : `${day.color}20`,
                              }
                            }}
                          >
                            <Box sx={{ textAlign: 'center' }}>
                              <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
                                {day.label}
                              </Typography>
                            </Box>
                          </Button>
                        </Grid>
                      ))}
                    </Grid>
                    
                    {selectedDays.length === 0 && (
                      <Alert severity="warning" sx={{ mt: 2 }}>
                        Please select at least one day for weekly recurring meetings
                      </Alert>
                    )}
                    
                    {selectedDays.length > 0 && (
                      <Box sx={{ mt: 2 }}>
                        <Typography variant="body2" color="text.secondary">
                          Selected: {selectedDays.map(day => weekDays.find(d => d.value === day)?.fullLabel).join(', ')}
                        </Typography>
                      </Box>
                    )}
                  </Paper>
                </Box>
              )}

              {/* Monthly Date Selection */}
              {recurringType === 'monthly' && (
                <Box sx={{ mb: 4 }}>
                  <Typography variant="h6" gutterBottom>
                    Select Monthly Recurrence
                  </Typography>
                  
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <FormControl fullWidth>
                        <InputLabel>Monthly Pattern</InputLabel>
                        <Select
                          value={monthlyPattern}
                          onChange={(e) => {
                            setMonthlyPattern(e.target.value);
                            updateRecurringData({ monthlyPattern: e.target.value });
                          }}
                          label="Monthly Pattern"
                        >
                          <MenuItem value="same-date">Same date each month</MenuItem>
                          <MenuItem value="specific-dates">Specific dates</MenuItem>
                          <MenuItem value="day-of-week">Same day of week</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    
                    {monthlyPattern === 'specific-dates' && (
                      <Grid item xs={12}>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                          Select specific dates of the month (1-31):
                        </Typography>
                        <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                          <Grid container spacing={1}>
                            {Array.from({ length: 31 }, (_, i) => i + 1).map((date) => (
                              <Grid item key={date}>
                                <Button
                                  size="small"
                                  variant={selectedMonthDates.includes(date) ? 'contained' : 'outlined'}
                                  onClick={() => {
                                    const newDates = selectedMonthDates.includes(date)
                                      ? selectedMonthDates.filter(d => d !== date)
                                      : [...selectedMonthDates, date].sort((a, b) => a - b);
                                    setSelectedMonthDates(newDates);
                                    updateRecurringData({ selectedMonthDates: newDates });
                                  }}
                                  sx={{ minWidth: 40, aspectRatio: '1' }}
                                >
                                  {date}
                                </Button>
                              </Grid>
                            ))}
                          </Grid>
                          
                          {selectedMonthDates.length > 0 && (
                            <Box sx={{ mt: 2 }}>
                              <Typography variant="body2" color="text.secondary">
                                Selected dates: {selectedMonthDates.join(', ')}
                              </Typography>
                            </Box>
                          )}
                        </Paper>
                      </Grid>
                    )}
                  </Grid>
                </Box>
              )}

              {/* Monthly Date Selection */}
              {recurringType === 'monthly' && (
                <Box sx={{ mb: 4 }}>
                  <Typography variant="h6" gutterBottom>
                    Select Monthly Recurrence
                  </Typography>
                  
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <FormControl fullWidth>
                        <InputLabel>Monthly Pattern</InputLabel>
                        <Select
                          value={monthlyPattern}
                          onChange={(e) => {
                            setMonthlyPattern(e.target.value);
                            updateRecurringData({ monthlyPattern: e.target.value });
                          }}
                          label="Monthly Pattern"
                        >
                          <MenuItem value="same-date">Same date each month</MenuItem>
                          <MenuItem value="specific-dates">Specific dates</MenuItem>
                          <MenuItem value="day-of-week">Same day of week</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    
                    {monthlyPattern === 'specific-dates' && (
                      <Grid item xs={12}>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                          Select specific dates of the month (1-31):
                        </Typography>
                        <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                          <Grid container spacing={1}>
                            {Array.from({ length: 31 }, (_, i) => i + 1).map((date) => (
                              <Grid item key={date}>
                                <Button
                                  size="small"
                                  variant={selectedMonthDates.includes(date) ? 'contained' : 'outlined'}
                                  onClick={() => {
                                    const newDates = selectedMonthDates.includes(date)
                                      ? selectedMonthDates.filter(d => d !== date)
                                      : [...selectedMonthDates, date].sort((a, b) => a - b);
                                    setSelectedMonthDates(newDates);
                                    updateRecurringData({ selectedMonthDates: newDates });
                                  }}
                                  sx={{ minWidth: 40, aspectRatio: '1' }}
                                >
                                  {date}
                                </Button>
                              </Grid>
                            ))}
                          </Grid>
                          
                          {selectedMonthDates.length > 0 && (
                            <Box sx={{ mt: 2 }}>
                              <Typography variant="body2" color="text.secondary">
                                Selected dates: {selectedMonthDates.join(', ')}
                              </Typography>
                            </Box>
                          )}
                        </Paper>
                      </Grid>
                    )}
                  </Grid>
                </Box>
              )}

              {/* Custom Pattern */}
              {recurringType === 'custom' && (
                <Box sx={{ mb: 4 }}>
                  <Typography variant="h6" gutterBottom>
                    Custom Pattern
                  </Typography>
                  <TextField
                    label="Describe your custom recurring pattern"
                    value={customPattern}
                    onChange={(e) => {
                      setCustomPattern(e.target.value);
                      updateRecurringData({ customPattern: e.target.value });
                    }}
                    fullWidth
                    multiline
                    rows={3}
                    variant="outlined"
                    placeholder="e.g., First Monday of every month, Last Friday of each quarter, etc."
                    helperText="Describe when you want the meeting to repeat"
                  />
                </Box>
              )}

              <Divider sx={{ my: 4 }} />

              {/* End Conditions */}
              <Box sx={{ mb: 4 }}>
                <Typography variant="h5" gutterBottom>
                  When to Stop
                </Typography>
                
                <Grid container spacing={3}>
                  <Grid item xs={12}>
                    <FormControl fullWidth size="large">
                      <InputLabel>End Condition</InputLabel>
                      <Select
                        value={endType}
                        onChange={(e) => {
                          setEndType(e.target.value);
                          updateRecurringData({ endType: e.target.value });
                        }}
                        label="End Condition"
                      >
                        {endTypes.map((type) => (
                          <MenuItem key={type.value} value={type.value}>
                            <Box>
                              <Typography variant="body1">{type.label}</Typography>
                              <Typography variant="caption" color="text.secondary">
                                {type.description}
                              </Typography>
                            </Box>
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  </Grid>

                  {endType === 'date' && (
                    <Grid item xs={12} md={6}>
                      <DatePicker
                        label="End Date"
                        value={endDate}
                        onChange={(date) => {
                          setEndDate(date);
                          updateRecurringData({ endDate: date });
                        }}
                        renderInput={(params) => (
                          <TextField {...params} fullWidth variant="outlined" size="large" />
                        )}
                        minDate={new Date()}
                      />
                    </Grid>
                  )}

                  {endType === 'occurrences' && (
                    <Grid item xs={12} md={6}>
                      <TextField
                        label="Number of meetings"
                        type="number"
                        value={occurrences}
                        onChange={(e) => {
                          const val = Math.max(1, parseInt(e.target.value) || 1);
                          setOccurrences(val);
                          updateRecurringData({ occurrences: val });
                        }}
                        inputProps={{ min: 1, max: 100 }}
                        fullWidth
                        variant="outlined"
                        size="large"
                        helperText="Maximum 100 meetings"
                      />
                    </Grid>
                  )}
                </Grid>
              </Box>

              {/* Action Buttons */}
              <Box sx={{ display: 'flex', gap: 2, mb: 4 }}>
                <Button
                  variant="outlined"
                  startIcon={<PreviewIcon />}
                  onClick={handlePreview}
                  size="large"
                  sx={{ borderRadius: 2 }}
                >
                  Preview Schedule
                </Button>
              </Box>

              {/* Summary Card */}
              <Card sx={{ bgcolor: 'success.50', border: '1px solid', borderColor: 'success.200' }}>
                <CardContent sx={{ p: 3 }}>
                  <Typography variant="h6" color="success.main" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                    <EventIcon sx={{ mr: 1 }} />
                    Recurring Schedule Summary
                  </Typography>
                  
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body1" sx={{ mb: 1 }}>
                        <strong>Pattern:</strong> {getPatternDescription()}
                      </Typography>
                      <Typography variant="body1">
                        <strong>Ends:</strong> {' '}
                        {endType === 'never' && 'Never (continues indefinitely)'}
                        {endType === 'date' && endDate && `On ${format(endDate, 'MMMM d, yyyy')}`}
                        {endType === 'occurrences' && `After ${occurrences} meetings`}
                      </Typography>
                    </Grid>
                    
                    <Grid item xs={12} md={6}>
                      {recurringType === 'weekly' && selectedDays.length > 0 && (
                        <Box>
                          <Typography variant="body2" color="text.secondary" gutterBottom>
                            Meeting days:
                          </Typography>
                          <Stack direction="row" spacing={0.5} flexWrap="wrap" gap={0.5}>
                            {selectedDays.map(day => {
                              const dayInfo = weekDays.find(d => d.value === day);
                              return (
                                <Chip
                                  key={day}
                                  label={dayInfo?.label}
                                  size="small"
                                  sx={{ 
                                    bgcolor: dayInfo?.color,
                                    color: 'white',
                                    fontWeight: 'bold'
                                  }}
                                />
                              );
                            })}
                          </Stack>
                        </Box>
                      )}
                    </Grid>
                  </Grid>
                </CardContent>
              </Card>
            </>
          )}
        </CardContent>
      </Card>

      {/* Preview Dialog */}
      <Dialog 
        open={previewOpen} 
        onClose={() => setPreviewOpen(false)} 
        maxWidth="md" 
        fullWidth
        PaperProps={{
          sx: { borderRadius: 3 }
        }}
      >
        <DialogTitle sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <CalendarIcon sx={{ mr: 1, color: 'primary.main' }} />
              <Box>
                <Typography variant="h5">Meeting Schedule Preview</Typography>
                <Typography variant="body2" color="text.secondary">
                  Next {Math.min(previewDates.length, 20)} scheduled meetings
                </Typography>
              </Box>
            </Box>
            <IconButton onClick={() => setPreviewOpen(false)}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        
        <DialogContent sx={{ pt: 2 }}>
          {previewDates.length > 0 ? (
            <List sx={{ maxHeight: 400, overflow: 'auto' }}>
              {previewDates.map((date, index) => (
                <ListItem 
                  key={index} 
                  divider={index < previewDates.length - 1}
                  sx={{ 
                    borderRadius: 1,
                    mb: 0.5,
                    '&:hover': { bgcolor: 'grey.50' }
                  }}
                >
                  <ListItemText
                    primary={
                      <Typography variant="h6">
                        Meeting #{index + 1}
                      </Typography>
                    }
                    secondary={
                      <Box>
                        <Typography variant="body1" color="primary.main">
                          {format(date, 'EEEE, MMMM d, yyyy')}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          {format(date, 'h:mm a')}
                        </Typography>
                      </Box>
                    }
                  />
                  <ListItemSecondaryAction>
                    <Chip
                      label={format(date, 'MMM d')}
                      color="primary"
                      variant="outlined"
                      sx={{ fontWeight: 'bold' }}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
              ))}
            </List>
          ) : (
            <Alert severity="info" sx={{ borderRadius: 2 }}>
              No meetings scheduled. Please check your recurring settings and try again.
            </Alert>
          )}
        </DialogContent>
        
        <DialogActions sx={{ p: 3, pt: 1 }}>
          <Button 
            onClick={() => setPreviewOpen(false)}
            variant="contained"
            size="large"
            sx={{ borderRadius: 2 }}
          >
            Close Preview
          </Button>
        </DialogActions>
      </Dialog>
    </LocalizationProvider>
  );
};

export default RecurringMeeting;