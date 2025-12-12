import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  TextField,
  Switch,
  FormControlLabel,
  FormGroup,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Chip,
  IconButton,
  Alert,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  Grid,
  Paper,
  Divider,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tooltip,
  Avatar,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  ListItemSecondaryAction
} from '@mui/material';
import {
  Schedule,
  Add,
  Delete,
  Edit,
  Email,
  Person,
  Settings,
  Security,
  Repeat,
  Notifications,
  CalendarToday,
  AccessTime,
  LocationOn,
  Description,
  People,
  VideoCall,
  ExpandMore,
  Save,
  Send,
  Preview,
  Close
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { useNavigate } from 'react-router-dom';
import { meetingsAPI } from '../../services/api'; // Fixed import path

const StyledContainer = styled(Box)(({ theme }) => ({
  minHeight: '100vh',
  background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
  padding: theme.spacing(2),
}));

const ScheduleCard = styled(Card)(({ theme }) => ({
  maxWidth: 800,
  margin: '0 auto',
  borderRadius: theme.spacing(3),
  boxShadow: '0 20px 40px rgba(0,0,0,0.1)',
  background: 'rgba(255,255,255,0.95)',
  backdropFilter: 'blur(10px)',
}));

const StepCard = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(3),
  borderRadius: theme.spacing(2),
  marginBottom: theme.spacing(2),
}));

const ParticipantChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.5),
  backgroundColor: theme.palette.primary.light,
  color: 'white',
}));

const RecurrenceOption = styled(Paper)(({ theme, selected }) => ({
  padding: theme.spacing(2),
  textAlign: 'center',
  cursor: 'pointer',
  border: selected ? `2px solid ${theme.palette.primary.main}` : '2px solid transparent',
  borderRadius: theme.spacing(1),
  transition: 'all 0.3s ease',
  '&:hover': {
    borderColor: theme.palette.primary.main,
    backgroundColor: theme.palette.primary.light + '10',
  },
}));

function ScheduleMeeting({ userProfile, onMeetingScheduled }) {
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState(0);
  
  // FIXED: Initialize with tomorrow's date to avoid past date issues
  const getInitialDate = () => {
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(9, 0, 0, 0); // Set to 9 AM
    return tomorrow;
  };

  // Get initial timezone and normalize it
  const getInitialTimezone = () => {
    let timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    // Normalize old timezone names
    if (timezone === 'Asia/Calcutta') {
      timezone = 'Asia/Kolkata';
    }
    return timezone;
  };

  const [meetingData, setMeetingData] = useState({
    title: '',
    description: '',
    startDate: getInitialDate(),
    startTime: getInitialDate(),
    duration: 60,
    timezone: getInitialTimezone(),
    location: '',
    participants: [],
    settings: {
      waitingRoom: true,
      recording: false,
      allowChat: true,
      allowScreenShare: true,
      muteParticipants: false,
      requirePassword: false,
      password: '',
    },
    recurrence: {
      enabled: false,
      type: 'none', // daily, weekly, monthly
      interval: 1,
      endDate: null,
      occurrences: null,
    },
    reminders: {
      email: true,
      browser: true,
      reminderTimes: [15, 5], // minutes before meeting
    },
  });
  const [participantEmail, setParticipantEmail] = useState('');
  const [showPreview, setShowPreview] = useState(false);
  const [isScheduling, setIsScheduling] = useState(false);
  const [validationErrors, setValidationErrors] = useState({});

  const steps = [
    'Basic Information',
    'Date & Time',
    'Participants',
    'Settings & Recurrence',
    'Review & Schedule'
  ];

  const timezones = [
    'America/New_York',
    'America/Los_Angeles',
    'Europe/London',
    'Europe/Paris',
    'Asia/Tokyo',
    'Asia/Shanghai',
    'Asia/Kolkata',
    'Australia/Sydney',
  ];

  const durationOptions = [
    { value: 15, label: '15 minutes' },
    { value: 30, label: '30 minutes' },
    { value: 45, label: '45 minutes' },
    { value: 60, label: '1 hour' },
    { value: 90, label: '1.5 hours' },
    { value: 120, label: '2 hours' },
    { value: 180, label: '3 hours' },
    { value: 240, label: '4 hours' },
  ];

  const handleInputChange = (field, value) => {
    setMeetingData(prev => ({
      ...prev,
      [field]: value
    }));
    
    // Clear validation error for this field
    if (validationErrors[field]) {
      setValidationErrors(prev => ({
        ...prev,
        [field]: null
      }));
    }
  };

  const handleSettingsChange = (setting, value) => {
    setMeetingData(prev => ({
      ...prev,
      settings: {
        ...prev.settings,
        [setting]: value
      }
    }));
  };

  const handleRecurrenceChange = (field, value) => {
    setMeetingData(prev => ({
      ...prev,
      recurrence: {
        ...prev.recurrence,
        [field]: value
      }
    }));
  };

  const handleReminderChange = (field, value) => {
    setMeetingData(prev => ({
      ...prev,
      reminders: {
        ...prev.reminders,
        [field]: value
      }
    }));
  };

  const addParticipant = () => {
    if (participantEmail && isValidEmail(participantEmail)) {
      const newParticipant = {
        id: Date.now(),
        email: participantEmail,
        name: participantEmail.split('@')[0],
        status: 'pending'
      };
      
      setMeetingData(prev => ({
        ...prev,
        participants: [...prev.participants, newParticipant]
      }));
      
      setParticipantEmail('');
    }
  };

  const removeParticipant = (participantId) => {
    setMeetingData(prev => ({
      ...prev,
      participants: prev.participants.filter(p => p.id !== participantId)
    }));
  };

  const isValidEmail = (email) => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  };

  // FIXED: Improved validation logic
  const validateStep = (step) => {
    const errors = {};
    
    switch (step) {
      case 0: // Basic Information
        if (!meetingData.title.trim()) {
          errors.title = 'Title is required';
        }
        break;
        
      case 1: // Date & Time
        // FIXED: More robust date validation
        const now = new Date();
        const selectedDateTime = new Date(meetingData.startDate);
        selectedDateTime.setHours(meetingData.startTime.getHours(), meetingData.startTime.getMinutes(), 0, 0);
        
        // Check if the selected date/time is at least 1 minute in the future
        if (selectedDateTime.getTime() <= now.getTime() + 60000) { // 1 minute buffer
          errors.startDate = 'Meeting date and time must be in the future';
        }
        break;
        
      case 2: // Participants
        // Optional validation - participants can be empty
        break;
        
      case 3: // Settings & Recurrence
        if (meetingData.settings.requirePassword && !meetingData.settings.password) {
          errors.password = 'Password is required when password protection is enabled';
        }
        break;
    }
    
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleNext = () => {
    console.log('🔄 Validating step:', activeStep);
    console.log('📅 Current meeting data:', meetingData);
    
    if (validateStep(activeStep)) {
      console.log('✅ Validation passed, moving to next step');
      setActiveStep(prev => prev + 1);
    } else {
      console.log('❌ Validation failed:', validationErrors);
    }
  };

  const handleBack = () => {
    setActiveStep(prev => prev - 1);
  };

  // FIXED: Updated to use the API service
  const handleScheduleMeeting = async () => {
    setIsScheduling(true);
    
    try {
      // Combine date and time
      const startDateTime = new Date(meetingData.startDate);
      startDateTime.setHours(meetingData.startTime.getHours());
      startDateTime.setMinutes(meetingData.startTime.getMinutes());
      
      const endDateTime = new Date(startDateTime.getTime() + meetingData.duration * 60000);
      
      // Prepare the data for API - matching backend expectations
      const apiData = {
        // Main meeting data
        Meeting_Name: meetingData.title,
        Host_ID: userProfile?.id || 'default-host',
        Meeting_Type: 'ScheduleMeeting',
        Started_At: startDateTime.toISOString(),
        Ended_At: endDateTime.toISOString(),
        Status: 'scheduled',
        Is_Recording_Enabled: meetingData.settings.recording,
        Waiting_Room_Enabled: meetingData.settings.waitingRoom,
        
        // ScheduledMeetings table fields
        title: meetingData.title,
        description: meetingData.description,
        location: meetingData.location,
        start_time: startDateTime.toISOString(),
        end_time: endDateTime.toISOString(),
        timezone: meetingData.timezone,
        duration_minutes: meetingData.duration,
        
        // Recurrence settings
        is_recurring: meetingData.recurrence.enabled ? 1 : 0,
        recurrence_type: meetingData.recurrence.enabled ? meetingData.recurrence.type : null,
        recurrence_interval: meetingData.recurrence.enabled ? meetingData.recurrence.interval : null,
        recurrence_occurrences: meetingData.recurrence.enabled ? meetingData.recurrence.occurrences : null,
        recurrence_end_date: meetingData.recurrence.enabled ? meetingData.recurrence.endDate : null,
        
        // Settings
        settings_waiting_room: meetingData.settings.waitingRoom ? 1 : 0,
        settings_recording: meetingData.settings.recording ? 1 : 0,
        settings_allow_chat: meetingData.settings.allowChat ? 1 : 0,
        settings_allow_screen_share: meetingData.settings.allowScreenShare ? 1 : 0,
        settings_mute_participants: meetingData.settings.muteParticipants ? 1 : 0,
        settings_require_password: meetingData.settings.requirePassword ? 1 : 0,
        settings_password: meetingData.settings.requirePassword ? meetingData.settings.password : null,
        
        // Reminders
        reminders_email: meetingData.reminders.email ? 1 : 0,
        reminders_browser: meetingData.reminders.browser ? 1 : 0,
        reminders_times: JSON.stringify(meetingData.reminders.reminderTimes),
        
        // Additional metadata for reference
        participants: meetingData.participants
      };

      // Validate required data before sending
      if (!apiData.Meeting_Name || !apiData.Host_ID) {
        throw new Error('Meeting name and host ID are required');
      }

      console.log('📤 Sending scheduled meeting data:', apiData);

      // Call the API using the fixed API service
      const response = await meetingsAPI.createScheduledMeeting(apiData);
      
      console.log('✅ Meeting scheduled successfully:', response);
      
      const scheduledMeeting = {
        ...meetingData,
        id: response.Meeting_ID || Date.now(),
        hostId: userProfile?.id,
        meetingType: 'ScheduleMeeting',
        startDateTime: startDateTime.toISOString(),
        endDateTime: endDateTime.toISOString(),
        meetingLink: response.Meeting_Link || `${window.location.origin}/meeting/${response.Meeting_ID}`,
        status: 'scheduled',
        createdAt: new Date().toISOString(),
      };

      onMeetingScheduled?.(scheduledMeeting);
      navigate('/dashboard', { 
        state: { 
          message: 'Meeting scheduled successfully! Invitations will be sent to participants.',
          meetingData: scheduledMeeting
        } 
      });
      
    } catch (error) {
      console.error('❌ Error scheduling meeting:', error);
      console.error('❌ Error response:', error.response?.data);
      console.error('❌ Error status:', error.response?.status);
      console.error('❌ Error headers:', error.response?.headers);
      
      let errorMessage = 'Failed to schedule meeting. Please try again.';
      
      if (error.response?.data) {
        if (typeof error.response.data === 'string') {
          errorMessage = error.response.data;
        } else if (error.response.data.Error) {
          errorMessage = error.response.data.Error;
        } else if (error.response.data.message) {
          errorMessage = error.response.data.message;
        }
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      setValidationErrors({
        api: errorMessage
      });
    } finally {
      setIsScheduling(false);
    }
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0: // Basic Information
        return (
          <StepCard elevation={2}>
            <Typography variant="h6" gutterBottom>
              Meeting Details
            </Typography>
            
            {validationErrors.api && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {validationErrors.api}
              </Alert>
            )}
            
            <TextField
              fullWidth
              label="Meeting Title *"
              value={meetingData.title}
              onChange={(e) => handleInputChange('title', e.target.value)}
              error={!!validationErrors.title}
              helperText={validationErrors.title}
              sx={{ mb: 3 }}
              placeholder="Enter meeting title..."
            />
            
            <TextField
              fullWidth
              label="Description"
              value={meetingData.description}
              onChange={(e) => handleInputChange('description', e.target.value)}
              multiline
              rows={3}
              sx={{ mb: 3 }}
              placeholder="Add meeting description or agenda..."
            />
            
            <TextField
              fullWidth
              label="Location (Optional)"
              value={meetingData.location}
              onChange={(e) => handleInputChange('location', e.target.value)}
              placeholder="Meeting room, address, or virtual location"
              InputProps={{
                startAdornment: <LocationOn sx={{ mr: 1, color: 'text.secondary' }} />
              }}
            />
          </StepCard>
        );

      case 1: // Date & Time
        return (
          <StepCard elevation={2}>
            <Typography variant="h6" gutterBottom>
              When is your meeting?
            </Typography>
            
            {validationErrors.startDate && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {validationErrors.startDate}
              </Alert>
            )}
            
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Meeting Date *"
                  type="date"
                  value={meetingData.startDate ? meetingData.startDate.toISOString().split('T')[0] : ''}
                  onChange={(e) => handleInputChange('startDate', new Date(e.target.value))}
                  error={!!validationErrors.startDate}
                  helperText={validationErrors.startDate}
                  InputLabelProps={{
                    shrink: true,
                  }}
                  inputProps={{
                    min: new Date().toISOString().split('T')[0] // Prevent past dates
                  }}
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Start Time"
                  type="time"
                  value={meetingData.startTime ? 
                    `${String(meetingData.startTime.getHours()).padStart(2, '0')}:${String(meetingData.startTime.getMinutes()).padStart(2, '0')}` 
                    : '09:00'
                  }
                  onChange={(e) => {
                    const [hours, minutes] = e.target.value.split(':');
                    const newTime = new Date();
                    newTime.setHours(parseInt(hours), parseInt(minutes), 0, 0);
                    handleInputChange('startTime', newTime);
                  }}
                  InputLabelProps={{
                    shrink: true,
                  }}
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Duration</InputLabel>
                  <Select
                    value={meetingData.duration}
                    onChange={(e) => handleInputChange('duration', e.target.value)}
                    label="Duration"
                  >
                    {durationOptions.map(option => (
                      <MenuItem key={option.value} value={option.value}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Timezone</InputLabel>
                  <Select
                    value={meetingData.timezone}
                    onChange={(e) => handleInputChange('timezone', e.target.value)}
                    label="Timezone"
                  >
                    {timezones.map(tz => (
                      <MenuItem key={tz} value={tz}>
                        {tz.replace('_', ' ')}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </StepCard>
        );

      case 2: // Participants
        return (
          <StepCard elevation={2}>
            <Typography variant="h6" gutterBottom>
              Who's joining?
            </Typography>
            
            <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
              <TextField
                fullWidth
                label="Email Address"
                value={participantEmail}
                onChange={(e) => setParticipantEmail(e.target.value)}
                placeholder="Enter participant email..."
                onKeyPress={(e) => e.key === 'Enter' && addParticipant()}
                InputProps={{
                  startAdornment: <Email sx={{ mr: 1, color: 'text.secondary' }} />
                }}
              />
              <Button
                variant="contained"
                onClick={addParticipant}
                disabled={!participantEmail || !isValidEmail(participantEmail)}
                startIcon={<Add />}
                sx={{ minWidth: 120 }}
              >
                Add
              </Button>
            </Box>
            
            {meetingData.participants.length > 0 && (
              <Box>
                <Typography variant="subtitle1" gutterBottom>
                  Participants ({meetingData.participants.length})
                </Typography>
                <List>
                  {meetingData.participants.map((participant) => (
                    <ListItem key={participant.id} divider>
                      <ListItemAvatar>
                        <Avatar sx={{ bgcolor: 'primary.main' }}>
                          {participant.name.charAt(0).toUpperCase()}
                        </Avatar>
                      </ListItemAvatar>
                      <ListItemText
                        primary={participant.name}
                        secondary={participant.email}
                      />
                      <ListItemSecondaryAction>
                        <IconButton
                          edge="end"
                          onClick={() => removeParticipant(participant.id)}
                          color="error"
                        >
                          <Delete />
                        </IconButton>
                      </ListItemSecondaryAction>
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
            
            <Alert severity="info" sx={{ mt: 2 }}>
              Participants will receive email invitations with the meeting details and join link.
            </Alert>
          </StepCard>
        );

      case 3: // Settings & Recurrence
        return (
          <Box>
            <StepCard elevation={2}>
              <Typography variant="h6" gutterBottom>
                Meeting Settings
              </Typography>
              
              <FormGroup>
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.settings.waitingRoom}
                      onChange={(e) => handleSettingsChange('waitingRoom', e.target.checked)}
                    />
                  }
                  label={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Security fontSize="small" />
                      Enable Waiting Room
                    </Box>
                  }
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.settings.recording}
                      onChange={(e) => handleSettingsChange('recording', e.target.checked)}
                    />
                  }
                  label="Auto-start Recording"
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.settings.allowChat}
                      onChange={(e) => handleSettingsChange('allowChat', e.target.checked)}
                    />
                  }
                  label="Allow Chat"
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.settings.allowScreenShare}
                      onChange={(e) => handleSettingsChange('allowScreenShare', e.target.checked)}
                    />
                  }
                  label="Allow Screen Sharing"
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.settings.muteParticipants}
                      onChange={(e) => handleSettingsChange('muteParticipants', e.target.checked)}
                    />
                  }
                  label="Mute Participants on Join"
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.settings.requirePassword}
                      onChange={(e) => handleSettingsChange('requirePassword', e.target.checked)}
                    />
                  }
                  label="Require Meeting Password"
                />
              </FormGroup>
              
              {meetingData.settings.requirePassword && (
                <TextField
                  fullWidth
                  label="Meeting Password *"
                  value={meetingData.settings.password}
                  onChange={(e) => handleSettingsChange('password', e.target.value)}
                  error={!!validationErrors.password}
                  helperText={validationErrors.password}
                  sx={{ mt: 2 }}
                  type="password"
                />
              )}
            </StepCard>

            <StepCard elevation={2}>
              <Typography variant="h6" gutterBottom>
                Recurrence
              </Typography>
              
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.recurrence.enabled}
                    onChange={(e) => handleRecurrenceChange('enabled', e.target.checked)}
                  />
                }
                label="Make this a recurring meeting"
                sx={{ mb: 2 }}
              />
              
              {meetingData.recurrence.enabled && (
                <Box>
                  <Typography variant="subtitle2" gutterBottom>
                    Repeat every:
                  </Typography>
                  <Grid container spacing={2} sx={{ mb: 3 }}>
                    <Grid item xs={4}>
                      <RecurrenceOption
                        selected={meetingData.recurrence.type === 'daily'}
                        onClick={() => handleRecurrenceChange('type', 'daily')}
                      >
                        <Typography variant="body2" fontWeight="bold">
                          Daily
                        </Typography>
                      </RecurrenceOption>
                    </Grid>
                    <Grid item xs={4}>
                      <RecurrenceOption
                        selected={meetingData.recurrence.type === 'weekly'}
                        onClick={() => handleRecurrenceChange('type', 'weekly')}
                      >
                        <Typography variant="body2" fontWeight="bold">
                          Weekly
                        </Typography>
                      </RecurrenceOption>
                    </Grid>
                    <Grid item xs={4}>
                      <RecurrenceOption
                        selected={meetingData.recurrence.type === 'monthly'}
                        onClick={() => handleRecurrenceChange('type', 'monthly')}
                      >
                        <Typography variant="body2" fontWeight="bold">
                          Monthly
                        </Typography>
                      </RecurrenceOption>
                    </Grid>
                  </Grid>
                  
                  <TextField
                    fullWidth
                    label="Number of Occurrences"
                    type="number"
                    value={meetingData.recurrence.occurrences || ''}
                    onChange={(e) => handleRecurrenceChange('occurrences', parseInt(e.target.value))}
                    placeholder="Leave empty for no end date"
                    inputProps={{ min: 1, max: 365 }}
                  />
                </Box>
              )}
            </StepCard>

            <StepCard elevation={2}>
              <Typography variant="h6" gutterBottom>
                Reminders
              </Typography>
              
              <FormGroup>
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.reminders.email}
                      onChange={(e) => handleReminderChange('email', e.target.checked)}
                    />
                  }
                  label="Email Reminders"
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.reminders.browser}
                      onChange={(e) => handleReminderChange('browser', e.target.checked)}
                    />
                  }
                  label="Browser Notifications"
                />
              </FormGroup>
            </StepCard>
          </Box>
        );

      case 4: // Review & Schedule
        return (
          <StepCard elevation={2}>
            <Typography variant="h6" gutterBottom>
              Review Meeting Details
            </Typography>
            
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                  <Typography variant="subtitle2" color="primary" gutterBottom>
                    MEETING INFO
                  </Typography>
                  <Typography variant="h6" gutterBottom>
                    {meetingData.title}
                  </Typography>
                  {meetingData.description && (
                    <Typography variant="body2" color="text.secondary" gutterBottom>
                      {meetingData.description}
                    </Typography>
                  )}
                  {meetingData.location && (
                    <Typography variant="body2" color="text.secondary">
                      📍 {meetingData.location}
                    </Typography>
                  )}
                </Paper>
              </Grid>
              
              <Grid item xs={12} md={6}>
                <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                  <Typography variant="subtitle2" color="primary" gutterBottom>
                    DATE & TIME
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    📅 {meetingData.startDate?.toLocaleDateString()}
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    🕐 {meetingData.startTime?.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </Typography>
                  <Typography variant="body1">
                    ⏱️ {meetingData.duration} minutes
                  </Typography>
                </Paper>
              </Grid>
              
              <Grid item xs={12} md={6}>
                <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                  <Typography variant="subtitle2" color="primary" gutterBottom>
                    PARTICIPANTS ({meetingData.participants.length})
                  </Typography>
                  {meetingData.participants.length > 0 ? (
                    meetingData.participants.slice(0, 3).map((participant, index) => (
                      <Typography key={index} variant="body2" gutterBottom>
                        👤 {participant.name}
                      </Typography>
                    ))
                  ) : (
                    <Typography variant="body2" color="text.secondary">
                      No participants added
                    </Typography>
                  )}
                  {meetingData.participants.length > 3 && (
                    <Typography variant="body2" color="text.secondary">
                      +{meetingData.participants.length - 3} more
                    </Typography>
                  )}
                </Paper>
              </Grid>
              
              <Grid item xs={12} md={6}>
                <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                  <Typography variant="subtitle2" color="primary" gutterBottom>
                    SETTINGS
                  </Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {meetingData.settings.waitingRoom && <Chip label="Waiting Room" size="small" />}
                    {meetingData.settings.recording && <Chip label="Recording" size="small" />}
                    {meetingData.settings.allowChat && <Chip label="Chat" size="small" />}
                    {meetingData.settings.allowScreenShare && <Chip label="Screen Share" size="small" />}
                    {meetingData.recurrence.enabled && <Chip label="Recurring" size="small" />}
                  </Box>
                </Paper>
              </Grid>
            </Grid>
            
            <Box sx={{ mt: 3, display: 'flex', gap: 2, justifyContent: 'center' }}>
              <Button
                variant="outlined"
                startIcon={<Preview />}
                onClick={() => setShowPreview(true)}
              >
                Preview Invitation
              </Button>
            </Box>
          </StepCard>
        );

      default:
        return null;
    }
  };

  return (
    <StyledContainer>
      <ScheduleCard>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ textAlign: 'center', mb: 4 }}>
            <Schedule sx={{ fontSize: 80, color: 'primary.main', mb: 2 }} />
            <Typography variant="h4" fontWeight="bold" gutterBottom>
              Schedule Meeting
            </Typography>
            <Typography variant="body1" color="text.secondary">
              Plan your meeting for the future and invite participants
            </Typography>
          </Box>

          <Stepper activeStep={activeStep} orientation="vertical">
            {steps.map((label, index) => (
              <Step key={label}>
                <StepLabel>
                  <Typography variant="h6">{label}</Typography>
                </StepLabel>
                <StepContent>
                  {renderStepContent(index)}
                  
                  <Box sx={{ mt: 3, display: 'flex', gap: 2 }}>
                    {activeStep > 0 && (
                      <Button onClick={handleBack} variant="outlined">
                        Back
                      </Button>
                    )}
                    
                    {activeStep < steps.length - 1 ? (
                      <Button
                        variant="contained"
                        onClick={handleNext}
                        disabled={Object.keys(validationErrors).length > 0}
                      >
                        Next
                      </Button>
                    ) : (
                      <Button
                        variant="contained"
                        onClick={handleScheduleMeeting}
                        disabled={isScheduling}
                        startIcon={isScheduling ? <Notifications /> : <Schedule />}
                        sx={{
                          background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
                        }}
                      >
                        {isScheduling ? 'Scheduling...' : 'Schedule Meeting'}
                      </Button>
                    )}
                    
                    <Button
                      variant="text"
                      onClick={() => navigate('/dashboard')}
                      color="inherit"
                    >
                      Cancel
                    </Button>
                  </Box>
                </StepContent>
              </Step>
            ))}
          </Stepper>
        </CardContent>
      </ScheduleCard>

      {/* Preview Dialog */}
      <Dialog
        open={showPreview}
        onClose={() => setShowPreview(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 3 }
        }}
      >
        <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          Email Invitation Preview
          <IconButton onClick={() => setShowPreview(false)}>
            <Close />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Paper sx={{ p: 3, bgcolor: 'grey.50' }}>
            <Typography variant="h6" gutterBottom>
              You're invited to: {meetingData.title}
            </Typography>
            <Typography variant="body1" gutterBottom>
              📅 {meetingData.startDate?.toLocaleDateString()} at {meetingData.startTime?.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </Typography>
            <Typography variant="body1" gutterBottom>
              ⏱️ Duration: {meetingData.duration} minutes
            </Typography>
            {meetingData.description && (
              <Typography variant="body2" sx={{ mt: 2 }}>
                {meetingData.description}
              </Typography>
            )}
            <Button
              variant="contained"
              fullWidth
              sx={{ mt: 3 }}
              startIcon={<VideoCall />}
            >
              Join Meeting
            </Button>
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowPreview(false)}>
            Close Preview
          </Button>
        </DialogActions>
      </Dialog>
    </StyledContainer>
  );
}

export default ScheduleMeeting;