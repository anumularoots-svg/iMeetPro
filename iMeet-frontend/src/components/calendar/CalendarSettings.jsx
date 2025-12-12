import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Switch,
  FormControlLabel,
  TextField,
  Button,
  Grid,
  Divider,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Alert,
  AlertTitle,
  useTheme,
  alpha,
  Tooltip,
  Avatar,
  Badge
} from '@mui/material';
import {
  Settings as SettingsIcon,
  Google as GoogleIcon,
  Microsoft as MicrosoftIcon,
  Apple as AppleIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  Sync as SyncIcon,
  ExpandMore as ExpandMoreIcon,
  Schedule as ScheduleIcon,
  Notifications as NotificationsIcon,
  Security as SecurityIcon,
  Palette as PaletteIcon,
  Language as LanguageIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  AccessTime as AccessTimeIcon,
  Business as BusinessIcon,
  Public as PublicIcon,
  Lock as LockIcon,
  Save as SaveIcon
} from '@mui/icons-material';

const CalendarSettings = () => {
  const theme = useTheme();
  const [settings, setSettings] = useState({
    // General Settings
    defaultMeetingDuration: 60,
    defaultTimeZone: 'America/New_York',
    workingHours: {
      start: '09:00',
      end: '17:00',
      days: ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
    },
    
    // Notification Settings
    emailNotifications: true,
    browserNotifications: true,
    reminderTimes: [15, 5],
    dailyAgenda: true,
    weeklyDigest: true,
    meetingStartAlert: true,
    
    // Calendar Integration
    syncEnabled: true,
    autoCreateMeeting: true,
    defaultCalendar: 'primary',
    syncFrequency: 15, // minutes
    
    // Privacy Settings
    showAvailability: true,
    allowGuestInvites: true,
    requireApproval: false,
    defaultMeetingPrivacy: 'private',
    
    // Appearance
    calendarView: 'month',
    theme: 'light',
    firstDayOfWeek: 'sunday',
    timeFormat: '12',
    dateFormat: 'MM/DD/YYYY'
  });

  const [connectedCalendars, setConnectedCalendars] = useState([
    {
      id: 'google-primary',
      provider: 'google',
      name: 'Primary Calendar',
      email: 'user@gmail.com',
      status: 'connected',
      lastSync: new Date().toISOString(),
      isDefault: true,
      eventsCount: 45,
      color: '#4285f4'
    },
    {
      id: 'outlook-work',
      provider: 'microsoft',
      name: 'Work Calendar',
      email: 'user@company.com',
      status: 'error',
      lastSync: new Date(Date.now() - 86400000).toISOString(),
      isDefault: false,
      eventsCount: 23,
      color: '#0078d4'
    }
  ]);

  const [addCalendarOpen, setAddCalendarOpen] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState('');
  const [syncing, setSyncing] = useState(false);
  const [saveLoading, setSaveLoading] = useState(false);

  const handleSettingChange = (key, value) => {
    setSettings(prev => ({
      ...prev,
      [key]: value
    }));
  };

  const handleWorkingHoursChange = (key, value) => {
    setSettings(prev => ({
      ...prev,
      workingHours: {
        ...prev.workingHours,
        [key]: value
      }
    }));
  };

  const handleWorkingDaysChange = (day) => {
    setSettings(prev => ({
      ...prev,
      workingHours: {
        ...prev.workingHours,
        days: prev.workingHours.days.includes(day)
          ? prev.workingHours.days.filter(d => d !== day)
          : [...prev.workingHours.days, day]
      }
    }));
  };

  const handleSaveSettings = async () => {
    setSaveLoading(true);
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1500));
      console.log('Settings saved:', settings);
    } catch (error) {
      console.error('Error saving settings:', error);
    } finally {
      setSaveLoading(false);
    }
  };

  const handleConnectCalendar = async (provider) => {
    try {
      // Simulate OAuth flow
      const newCalendar = {
        id: `${provider}-${Date.now()}`,
        provider,
        name: `${provider.charAt(0).toUpperCase() + provider.slice(1)} Calendar`,
        email: `user@${provider}.com`,
        status: 'connected',
        lastSync: new Date().toISOString(),
        isDefault: false,
        eventsCount: Math.floor(Math.random() * 50),
        color: provider === 'google' ? '#4285f4' : provider === 'microsoft' ? '#0078d4' : '#000'
      };
      
      setConnectedCalendars(prev => [...prev, newCalendar]);
      setAddCalendarOpen(false);
      setSelectedProvider('');
    } catch (error) {
      console.error('Error connecting calendar:', error);
    }
  };

  const handleDisconnectCalendar = (calendarId) => {
    setConnectedCalendars(prev => 
      prev.filter(cal => cal.id !== calendarId)
    );
  };

  const handleSyncCalendar = async (calendarId) => {
    setSyncing(true);
    try {
      // Simulate sync
      await new Promise(resolve => setTimeout(resolve, 2000));
      setConnectedCalendars(prev =>
        prev.map(cal =>
          cal.id === calendarId
            ? { ...cal, lastSync: new Date().toISOString(), status: 'connected' }
            : cal
        )
      );
    } catch (error) {
      console.error('Error syncing calendar:', error);
    } finally {
      setSyncing(false);
    }
  };

  const handleSetDefaultCalendar = (calendarId) => {
    setConnectedCalendars(prev =>
      prev.map(cal => ({
        ...cal,
        isDefault: cal.id === calendarId
      }))
    );
  };

  const getProviderIcon = (provider) => {
    switch (provider) {
      case 'google': return <GoogleIcon sx={{ color: '#4285f4' }} />;
      case 'microsoft': return <MicrosoftIcon sx={{ color: '#0078d4' }} />;
      case 'apple': return <AppleIcon sx={{ color: '#000' }} />;
      default: return <ScheduleIcon />;
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'connected': return theme.palette.success.main;
      case 'error': return theme.palette.error.main;
      case 'syncing': return theme.palette.warning.main;
      default: return theme.palette.grey[500];
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'connected': return <CheckCircleIcon sx={{ color: getStatusColor(status) }} />;
      case 'error': return <ErrorIcon sx={{ color: getStatusColor(status) }} />;
      case 'syncing': return <SyncIcon sx={{ color: getStatusColor(status) }} />;
      default: return <WarningIcon sx={{ color: getStatusColor(status) }} />;
    }
  };

  const weekDays = [
    { id: 'monday', label: 'Mon' },
    { id: 'tuesday', label: 'Tue' },
    { id: 'wednesday', label: 'Wed' },
    { id: 'thursday', label: 'Thu' },
    { id: 'friday', label: 'Fri' },
    { id: 'saturday', label: 'Sat' },
    { id: 'sunday', label: 'Sun' }
  ];

  return (
    <Box sx={{ maxWidth: 1200, mx: 'auto', p: 3 }}>
      {/* Header */}
      <Box mb={4}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Box>
            <Typography variant="h4" fontWeight={700} color="text.primary" mb={1}>
              Calendar Settings
            </Typography>
            <Typography variant="body1" color="text.secondary">
              Manage your calendar integrations, notifications, and preferences
            </Typography>
          </Box>
          <Button
            variant="contained"
            startIcon={<SaveIcon />}
            onClick={handleSaveSettings}
            loading={saveLoading}
            sx={{
              borderRadius: 3,
              textTransform: 'none',
              fontWeight: 600,
              px: 3,
              py: 1.5,
              background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
              boxShadow: theme.shadows[8]
            }}
          >
            {saveLoading ? 'Saving...' : 'Save Changes'}
          </Button>
        </Box>
      </Box>

      <Grid container spacing={4}>
        {/* Left Column - Main Settings */}
        <Grid item xs={12} lg={8}>
          {/* Calendar Integrations */}
          <Card sx={{ mb: 4, overflow: 'visible' }}>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
                <Box display="flex" alignItems="center" gap={2}>
                  <Avatar sx={{ 
                    bgcolor: alpha(theme.palette.primary.main, 0.1),
                    color: theme.palette.primary.main 
                  }}>
                    <ScheduleIcon />
                  </Avatar>
                  <Box>
                    <Typography variant="h6" fontWeight={600}>
                      Connected Calendars
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Sync your external calendars
                    </Typography>
                  </Box>
                </Box>
                <Button
                  variant="contained"
                  startIcon={<AddIcon />}
                  onClick={() => setAddCalendarOpen(true)}
                  sx={{
                    borderRadius: 2,
                    textTransform: 'none',
                    fontWeight: 600
                  }}
                >
                  Add Calendar
                </Button>
              </Box>

              <List sx={{ bgcolor: alpha(theme.palette.primary.main, 0.02), borderRadius: 2 }}>
                {connectedCalendars.map((calendar, index) => (
                  <React.Fragment key={calendar.id}>
                    <ListItem sx={{ 
                      borderRadius: 2,
                      mb: 1,
                      bgcolor: 'background.paper',
                      boxShadow: theme.shadows[1],
                      '&:hover': { 
                        bgcolor: alpha(theme.palette.primary.main, 0.05),
                        boxShadow: theme.shadows[4]
                      }
                    }}>
                      <ListItemIcon>
                        <Badge
                          overlap="circular"
                          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                          badgeContent={
                            calendar.isDefault ? (
                              <Avatar sx={{ 
                                width: 20, 
                                height: 20, 
                                bgcolor: theme.palette.success.main,
                                fontSize: '0.6rem'
                              }}>
                                ✓
                              </Avatar>
                            ) : null
                          }
                        >
                          {getProviderIcon(calendar.provider)}
                        </Badge>
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Box display="flex" alignItems="center" gap={1}>
                            <Typography variant="body1" fontWeight={600}>
                              {calendar.name}
                            </Typography>
                            {calendar.isDefault && (
                              <Chip 
                                label="Default" 
                                size="small" 
                                color="success"
                                sx={{ height: 20, fontSize: '0.7rem' }}
                              />
                            )}
                            <Chip 
                              label={`${calendar.eventsCount} events`} 
                              size="small" 
                              variant="outlined"
                              sx={{ height: 20, fontSize: '0.7rem' }}
                            />
                          </Box>
                        }
                        secondary={
                          <Box>
                            <Typography variant="body2" color="text.secondary">
                              {calendar.email}
                            </Typography>
                            <Box display="flex" alignItems="center" gap={1} mt={0.5}>
                              {getStatusIcon(calendar.status)}
                              <Typography variant="caption" color="text.secondary">
                                Last synced: {new Date(calendar.lastSync).toLocaleString()}
                              </Typography>
                            </Box>
                          </Box>
                        }
                      />
                      <ListItemSecondaryAction>
                        <Box display="flex" gap={1}>
                          {!calendar.isDefault && (
                            <Tooltip title="Set as default">
                              <IconButton
                                onClick={() => handleSetDefaultCalendar(calendar.id)}
                                size="small"
                                sx={{ 
                                  bgcolor: alpha(theme.palette.success.main, 0.1),
                                  '&:hover': { bgcolor: alpha(theme.palette.success.main, 0.2) }
                                }}
                              >
                                <CheckCircleIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          <Tooltip title="Sync calendar">
                            <IconButton
                              onClick={() => handleSyncCalendar(calendar.id)}
                              disabled={syncing}
                              size="small"
                              sx={{ 
                                bgcolor: alpha(theme.palette.info.main, 0.1),
                                '&:hover': { bgcolor: alpha(theme.palette.info.main, 0.2) }
                              }}
                            >
                              <SyncIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Remove calendar">
                            <IconButton
                              onClick={() => handleDisconnectCalendar(calendar.id)}
                              size="small"
                              sx={{ 
                                bgcolor: alpha(theme.palette.error.main, 0.1),
                                '&:hover': { bgcolor: alpha(theme.palette.error.main, 0.2) }
                              }}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </Box>
                      </ListItemSecondaryAction>
                    </ListItem>
                  </React.Fragment>
                ))}
              </List>

              {connectedCalendars.length === 0 && (
                <Box 
                  display="flex" 
                  flexDirection="column" 
                  alignItems="center" 
                  py={6}
                  sx={{
                    bgcolor: alpha(theme.palette.primary.main, 0.02),
                    borderRadius: 2,
                    border: `2px dashed ${alpha(theme.palette.primary.main, 0.2)}`
                  }}
                >
                  <ScheduleIcon sx={{ fontSize: '4rem', color: 'text.disabled', mb: 2 }} />
                  <Typography variant="h6" color="text.secondary" mb={1}>
                    No calendars connected
                  </Typography>
                  <Typography variant="body2" color="text.secondary" textAlign="center" mb={3}>
                    Connect your calendar to sync meetings and events automatically
                  </Typography>
                  <Button
                    variant="contained"
                    startIcon={<AddIcon />}
                    onClick={() => setAddCalendarOpen(true)}
                    sx={{
                      borderRadius: 2,
                      textTransform: 'none',
                      fontWeight: 600
                    }}
                  >
                    Connect Your First Calendar
                  </Button>
                </Box>
              )}
            </CardContent>
          </Card>

          {/* General Settings */}
          <Accordion defaultExpanded sx={{ mb: 2, borderRadius: 2, overflow: 'hidden' }}>
            <AccordionSummary 
              expandIcon={<ExpandMoreIcon />}
              sx={{ 
                bgcolor: alpha(theme.palette.primary.main, 0.05),
                '&:hover': { bgcolor: alpha(theme.palette.primary.main, 0.1) }
              }}
            >
              <Box display="flex" alignItems="center" gap={2}>
                <SettingsIcon color="primary" />
                <Box>
                  <Typography variant="h6" fontWeight={600}>
                    General Settings
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Configure default meeting preferences
                  </Typography>
                </Box>
              </Box>
            </AccordionSummary>
            <AccordionDetails sx={{ p: 3 }}>
              <Grid container spacing={3}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Default Meeting Duration"
                    type="number"
                    value={settings.defaultMeetingDuration}
                    onChange={(e) => handleSettingChange('defaultMeetingDuration', parseInt(e.target.value))}
                    InputProps={{ 
                      inputProps: { min: 15, max: 480 },
                      endAdornment: <Typography variant="body2" color="text.secondary">minutes</Typography>
                    }}
                    helperText="Duration for new meetings (15-480 minutes)"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <FormControl fullWidth>
                    <InputLabel>Default Time Zone</InputLabel>
                    <Select
                      value={settings.defaultTimeZone}
                      label="Default Time Zone"
                      onChange={(e) => handleSettingChange('defaultTimeZone', e.target.value)}
                    >
                      <MenuItem value="America/New_York">Eastern Time (ET)</MenuItem>
                      <MenuItem value="America/Chicago">Central Time (CT)</MenuItem>
                      <MenuItem value="America/Denver">Mountain Time (MT)</MenuItem>
                      <MenuItem value="America/Los_Angeles">Pacific Time (PT)</MenuItem>
                      <MenuItem value="UTC">Coordinated Universal Time (UTC)</MenuItem>
                      <MenuItem value="Europe/London">Greenwich Mean Time (GMT)</MenuItem>
                      <MenuItem value="Europe/Paris">Central European Time (CET)</MenuItem>
                      <MenuItem value="Asia/Tokyo">Japan Standard Time (JST)</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>

                <Grid item xs={12}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Working Hours
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={4}>
                      <TextField
                        fullWidth
                        label="Start Time"
                        type="time"
                        value={settings.workingHours.start}
                        onChange={(e) => handleWorkingHoursChange('start', e.target.value)}
                        InputLabelProps={{ shrink: true }}
                      />
                    </Grid>
                    <Grid item xs={12} sm={4}>
                      <TextField
                        fullWidth
                        label="End Time"
                        type="time"
                        value={settings.workingHours.end}
                        onChange={(e) => handleWorkingHoursChange('end', e.target.value)}
                        InputLabelProps={{ shrink: true }}
                      />
                    </Grid>
                    <Grid item xs={12} sm={4}>
                      <FormControl fullWidth>
                        <InputLabel>Sync Frequency</InputLabel>
                        <Select
                          value={settings.syncFrequency}
                          label="Sync Frequency"
                          onChange={(e) => handleSettingChange('syncFrequency', e.target.value)}
                        >
                          <MenuItem value={5}>Every 5 minutes</MenuItem>
                          <MenuItem value={15}>Every 15 minutes</MenuItem>
                          <MenuItem value={30}>Every 30 minutes</MenuItem>
                          <MenuItem value={60}>Every hour</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                  </Grid>
                </Grid>

                <Grid item xs={12}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Working Days
                  </Typography>
                  <Box display="flex" gap={1} flexWrap="wrap">
                    {weekDays.map((day) => (
                      <Chip
                        key={day.id}
                        label={day.label}
                        onClick={() => handleWorkingDaysChange(day.id)}
                        color={settings.workingHours.days.includes(day.id) ? "primary" : "default"}
                        variant={settings.workingHours.days.includes(day.id) ? "filled" : "outlined"}
                        sx={{ 
                          cursor: 'pointer',
                          minWidth: 60,
                          fontWeight: 600
                        }}
                      />
                    ))}
                  </Box>
                </Grid>

                <Grid item xs={12}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Display Preferences
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={3}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Default View</InputLabel>
                        <Select
                          value={settings.calendarView}
                          label="Default View"
                          onChange={(e) => handleSettingChange('calendarView', e.target.value)}
                        >
                          <MenuItem value="month">Month</MenuItem>
                          <MenuItem value="week">Week</MenuItem>
                          <MenuItem value="day">Day</MenuItem>
                          <MenuItem value="agenda">Agenda</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={3}>
                      <FormControl fullWidth size="small">
                        <InputLabel>First Day of Week</InputLabel>
                        <Select
                          value={settings.firstDayOfWeek}
                          label="First Day of Week"
                          onChange={(e) => handleSettingChange('firstDayOfWeek', e.target.value)}
                        >
                          <MenuItem value="sunday">Sunday</MenuItem>
                          <MenuItem value="monday">Monday</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={3}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Time Format</InputLabel>
                        <Select
                          value={settings.timeFormat}
                          label="Time Format"
                          onChange={(e) => handleSettingChange('timeFormat', e.target.value)}
                        >
                          <MenuItem value="12">12-hour (AM/PM)</MenuItem>
                          <MenuItem value="24">24-hour</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={3}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Date Format</InputLabel>
                        <Select
                          value={settings.dateFormat}
                          label="Date Format"
                          onChange={(e) => handleSettingChange('dateFormat', e.target.value)}
                        >
                          <MenuItem value="MM/DD/YYYY">MM/DD/YYYY</MenuItem>
                          <MenuItem value="DD/MM/YYYY">DD/MM/YYYY</MenuItem>
                          <MenuItem value="YYYY-MM-DD">YYYY-MM-DD</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                  </Grid>
                </Grid>
              </Grid>
            </AccordionDetails>
          </Accordion>

          {/* Notification Settings */}
          <Accordion sx={{ mb: 2, borderRadius: 2, overflow: 'hidden' }}>
            <AccordionSummary 
              expandIcon={<ExpandMoreIcon />}
              sx={{ 
                bgcolor: alpha(theme.palette.warning.main, 0.05),
                '&:hover': { bgcolor: alpha(theme.palette.warning.main, 0.1) }
              }}
            >
              <Box display="flex" alignItems="center" gap={2}>
                <NotificationsIcon sx={{ color: theme.palette.warning.main }} />
                <Box>
                  <Typography variant="h6" fontWeight={600}>
                    Notification Settings
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Manage how you receive meeting alerts
                  </Typography>
                </Box>
              </Box>
            </AccordionSummary>
            <AccordionDetails sx={{ p: 3 }}>
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Notification Types
                  </Typography>
                  <Box display="flex" flexDirection="column" gap={2}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.emailNotifications}
                          onChange={(e) => handleSettingChange('emailNotifications', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Email Notifications</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Receive meeting reminders via email
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.browserNotifications}
                          onChange={(e) => handleSettingChange('browserNotifications', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Browser Notifications</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Show desktop notifications
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.meetingStartAlert}
                          onChange={(e) => handleSettingChange('meetingStartAlert', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Meeting Start Alerts</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Alert when meetings begin
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.dailyAgenda}
                          onChange={(e) => handleSettingChange('dailyAgenda', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Daily Agenda Email</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Daily summary of upcoming meetings
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.weeklyDigest}
                          onChange={(e) => handleSettingChange('weeklyDigest', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Weekly Meeting Digest</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Weekly meeting statistics and insights
                          </Typography>
                        </Box>
                      }
                    />
                  </Box>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Reminder Times
                  </Typography>
                  <Typography variant="body2" color="text.secondary" mb={2}>
                    Select when to receive meeting reminders (minutes before meeting)
                  </Typography>
                  <Box display="flex" gap={1} flexWrap="wrap">
                    {[1, 5, 10, 15, 30, 60, 120].map((time) => (
                      <Chip
                        key={time}
                        label={time < 60 ? `${time}m` : `${time/60}h`}
                        onClick={() => {
                          const newTimes = settings.reminderTimes.includes(time)
                            ? settings.reminderTimes.filter(t => t !== time)
                            : [...settings.reminderTimes, time];
                          handleSettingChange('reminderTimes', newTimes);
                        }}
                        color={settings.reminderTimes.includes(time) ? "primary" : "default"}
                        variant={settings.reminderTimes.includes(time) ? "filled" : "outlined"}
                        sx={{ 
                          cursor: 'pointer',
                          fontWeight: 600,
                          transition: 'all 0.2s ease'
                        }}
                      />
                    ))}
                  </Box>
                </Grid>
              </Grid>
            </AccordionDetails>
          </Accordion>

          {/* Privacy & Security */}
          <Accordion sx={{ mb: 2, borderRadius: 2, overflow: 'hidden' }}>
            <AccordionSummary 
              expandIcon={<ExpandMoreIcon />}
              sx={{ 
                bgcolor: alpha(theme.palette.error.main, 0.05),
                '&:hover': { bgcolor: alpha(theme.palette.error.main, 0.1) }
              }}
            >
              <Box display="flex" alignItems="center" gap={2}>
                <SecurityIcon sx={{ color: theme.palette.error.main }} />
                <Box>
                  <Typography variant="h6" fontWeight={600}>
                    Privacy & Security
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Control meeting access and privacy settings
                  </Typography>
                </Box>
              </Box>
            </AccordionSummary>
            <AccordionDetails sx={{ p: 3 }}>
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Meeting Privacy
                  </Typography>
                  <Box display="flex" flexDirection="column" gap={2}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.showAvailability}
                          onChange={(e) => handleSettingChange('showAvailability', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Show Availability</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Let others see when you're free or busy
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.allowGuestInvites}
                          onChange={(e) => handleSettingChange('allowGuestInvites', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Allow Guest Invites</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Guests can invite others to meetings
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.requireApproval}
                          onChange={(e) => handleSettingChange('requireApproval', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Require Host Approval</Typography>
                          <Typography variant="caption" color="text.secondary">
                            New participants need approval to join
                          </Typography>
                        </Box>
                      }
                    />
                    <FormControlLabel
                      control={
                        <Switch
                          checked={settings.autoCreateMeeting}
                          onChange={(e) => handleSettingChange('autoCreateMeeting', e.target.checked)}
                          color="primary"
                        />
                      }
                      label={
                        <Box>
                          <Typography variant="body2" fontWeight={500}>Auto-Create Meeting Links</Typography>
                          <Typography variant="caption" color="text.secondary">
                            Automatically add meeting links to calendar events
                          </Typography>
                        </Box>
                      }
                    />
                  </Box>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle1" fontWeight={600} mb={2}>
                    Default Meeting Settings
                  </Typography>
                  <Box display="flex" flexDirection="column" gap={2}>
                    <FormControl fullWidth>
                      <InputLabel>Default Meeting Privacy</InputLabel>
                      <Select
                        value={settings.defaultMeetingPrivacy}
                        label="Default Meeting Privacy"
                        onChange={(e) => handleSettingChange('defaultMeetingPrivacy', e.target.value)}
                      >
                        <MenuItem value="public">
                          <Box display="flex" alignItems="center" gap={1}>
                            <PublicIcon fontSize="small" />
                            Public - Anyone with link can join
                          </Box>
                        </MenuItem>
                        <MenuItem value="private">
                          <Box display="flex" alignItems="center" gap={1}>
                            <LockIcon fontSize="small" />
                            Private - Only invited participants
                          </Box>
                        </MenuItem>
                        <MenuItem value="organization">
                          <Box display="flex" alignItems="center" gap={1}>
                            <BusinessIcon fontSize="small" />
                            Organization - Only team members
                          </Box>
                        </MenuItem>
                      </Select>
                    </FormControl>

                    <Alert severity="info" sx={{ mt: 2 }}>
                      <Typography variant="body2">
                        <strong>Security Tip:</strong> Enable "Require Host Approval" for sensitive meetings to control who can join.
                      </Typography>
                    </Alert>
                  </Box>
                </Grid>
              </Grid>
            </AccordionDetails>
          </Accordion>
        </Grid>

        {/* Right Column - Quick Actions & Status */}
        <Grid item xs={12} lg={4}>
          {/* Quick Actions */}
          <Card sx={{ mb: 3, overflow: 'visible' }}>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" alignItems="center" gap={2} mb={3}>
                <Avatar sx={{ 
                  bgcolor: alpha(theme.palette.success.main, 0.1),
                  color: theme.palette.success.main 
                }}>
                  <SyncIcon />
                </Avatar>
                <Typography variant="h6" fontWeight={600}>
                  Quick Actions
                </Typography>
              </Box>
              <Box display="flex" flexDirection="column" gap={2}>
                <Button
                  variant="contained"
                  fullWidth
                  startIcon={<SyncIcon />}
                  onClick={() => connectedCalendars.forEach(cal => handleSyncCalendar(cal.id))}
                  disabled={syncing}
                  sx={{
                    borderRadius: 2,
                    textTransform: 'none',
                    fontWeight: 600,
                    py: 1.5,
                    background: `linear-gradient(135deg, ${theme.palette.success.main} 0%, ${theme.palette.success.dark} 100%)`,
                    boxShadow: theme.shadows[4]
                  }}
                >
                  {syncing ? 'Syncing All...' : 'Sync All Calendars'}
                </Button>
                <Button
                  variant="outlined"
                  fullWidth
                  startIcon={<AddIcon />}
                  onClick={() => setAddCalendarOpen(true)}
                  sx={{
                    borderRadius: 2,
                    textTransform: 'none',
                    fontWeight: 600,
                    py: 1.5,
                    borderWidth: 2,
                    '&:hover': { borderWidth: 2 }
                  }}
                >
                  Add New Calendar
                </Button>
                <Button
                  variant="outlined"
                  fullWidth
                  startIcon={<SettingsIcon />}
                  sx={{
                    borderRadius: 2,
                    textTransform: 'none',
                    fontWeight: 600,
                    py: 1.5,
                    borderWidth: 2,
                    '&:hover': { borderWidth: 2 }
                  }}
                >
                  Advanced Settings
                </Button>
              </Box>
            </CardContent>
          </Card>

          {/* Sync Status */}
          <Card sx={{ mb: 3 }}>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" alignItems="center" gap={2} mb={3}>
                <Avatar sx={{ 
                  bgcolor: alpha(theme.palette.info.main, 0.1),
                  color: theme.palette.info.main 
                }}>
                  <CheckCircleIcon />
                </Avatar>
                <Typography variant="h6" fontWeight={600}>
                  Sync Status
                </Typography>
              </Box>
              <Box display="flex" flexDirection="column" gap={2}>
                {connectedCalendars.map((calendar) => (
                  <Box
                    key={calendar.id}
                    sx={{
                      p: 2,
                      borderRadius: 2,
                      bgcolor: alpha(getStatusColor(calendar.status), 0.05),
                      border: `1px solid ${alpha(getStatusColor(calendar.status), 0.2)}`,
                      transition: 'all 0.2s ease'
                    }}
                  >
                    <Box display="flex" alignItems="center" gap={2} mb={1}>
                      {getProviderIcon(calendar.provider)}
                      <Typography variant="body2" fontWeight={600} noWrap>
                        {calendar.name}
                      </Typography>
                    </Box>
                    <Box display="flex" alignItems="center" justifyContent="space-between">
                      <Box display="flex" alignItems="center" gap={1}>
                        {getStatusIcon(calendar.status)}
                        <Typography variant="caption" color="text.secondary">
                          {calendar.status === 'connected' ? 'Connected' : 
                           calendar.status === 'error' ? 'Error' : 'Syncing...'}
                        </Typography>
                      </Box>
                      <Chip 
                        label={`${calendar.eventsCount} events`}
                        size="small"
                        sx={{ 
                          height: 20,
                          fontSize: '0.7rem',
                          bgcolor: alpha(calendar.color, 0.1),
                          color: calendar.color,
                          fontWeight: 600
                        }}
                      />
                    </Box>
                    <Typography variant="caption" color="text.secondary" display="block" mt={1}>
                      Last sync: {new Date(calendar.lastSync).toLocaleString()}
                    </Typography>
                  </Box>
                ))}
              </Box>

              {connectedCalendars.length === 0 && (
                <Box textAlign="center" py={3}>
                  <Typography variant="body2" color="text.secondary">
                    No calendars to sync
                  </Typography>
                </Box>
              )}
            </CardContent>
          </Card>

          {/* Settings Summary */}
          <Card>
            <CardContent sx={{ p: 3 }}>
              <Box display="flex" alignItems="center" gap={2} mb={3}>
                <Avatar sx={{ 
                  bgcolor: alpha(theme.palette.primary.main, 0.1),
                  color: theme.palette.primary.main 
                }}>
                  <SettingsIcon />
                </Avatar>
                <Typography variant="h6" fontWeight={600}>
                  Settings Summary
                </Typography>
              </Box>
              <Box display="flex" flexDirection="column" gap={2}>
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Default Meeting Duration
                  </Typography>
                  <Typography variant="body1" fontWeight={600}>
                    {settings.defaultMeetingDuration} minutes
                  </Typography>
                </Box>
                <Divider />
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Active Notifications
                  </Typography>
                  <Typography variant="body1" fontWeight={600}>
                    {[
                      settings.emailNotifications && 'Email',
                      settings.browserNotifications && 'Browser',
                      settings.dailyAgenda && 'Daily Agenda'
                    ].filter(Boolean).join(', ') || 'None'}
                  </Typography>
                </Box>
                <Divider />
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Reminder Times
                  </Typography>
                  <Typography variant="body1" fontWeight={600}>
                    {settings.reminderTimes.map(t => `${t}m`).join(', ')}
                  </Typography>
                </Box>
                <Divider />
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    Privacy Level
                  </Typography>
                  <Chip 
                    label={settings.defaultMeetingPrivacy.charAt(0).toUpperCase() + settings.defaultMeetingPrivacy.slice(1)}
                    size="small"
                    color={settings.defaultMeetingPrivacy === 'private' ? 'error' : settings.defaultMeetingPrivacy === 'organization' ? 'warning' : 'success'}
                    sx={{ fontWeight: 600 }}
                  />
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Add Calendar Dialog */}
      <Dialog 
        open={addCalendarOpen} 
        onClose={() => setAddCalendarOpen(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 3,
            overflow: 'hidden'
          }
        }}
      >
        <DialogTitle sx={{ 
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.1)} 0%, ${alpha(theme.palette.primary.main, 0.05)} 100%)`,
          borderBottom: `1px solid ${theme.palette.divider}`
        }}>
          <Box display="flex" alignItems="center" gap={2}>
            <AddIcon color="primary" />
            <Typography variant="h6" fontWeight={600}>
              Connect New Calendar
            </Typography>
          </Box>
        </DialogTitle>
        <DialogContent sx={{ p: 3 }}>
          <Typography variant="body1" color="text.secondary" mb={3}>
            Choose a calendar provider to sync with your meetings
          </Typography>
          
          <Grid container spacing={2}>
            {[
              { 
                id: 'google', 
                name: 'Google Calendar', 
                icon: <GoogleIcon sx={{ color: '#4285f4', fontSize: '2rem' }} />,
                description: 'Sync with your Google account',
                popular: true
              },
              { 
                id: 'microsoft', 
                name: 'Outlook Calendar', 
                icon: <MicrosoftIcon sx={{ color: '#0078d4', fontSize: '2rem' }} />,
                description: 'Connect Microsoft Outlook/Office 365',
                popular: false
              },
              { 
                id: 'apple', 
                name: 'Apple Calendar', 
                icon: <AppleIcon sx={{ color: '#000', fontSize: '2rem' }} />,
                description: 'Sync with iCloud calendar',
                popular: false
              }
            ].map((provider) => (
              <Grid item xs={12} key={provider.id}>
                <Card
                  sx={{
                    cursor: 'pointer',
                    border: selectedProvider === provider.id ? 
                      `2px solid ${theme.palette.primary.main}` : 
                      `1px solid ${theme.palette.divider}`,
                    position: 'relative',
                    transition: 'all 0.2s ease',
                    '&:hover': {
                      boxShadow: theme.shadows[8],
                      borderColor: theme.palette.primary.main,
                      transform: 'translateY(-2px)'
                    }
                  }}
                  onClick={() => setSelectedProvider(provider.id)}
                >
                  {provider.popular && (
                    <Chip 
                      label="Popular" 
                      size="small" 
                      color="primary"
                      sx={{ 
                        position: 'absolute',
                        top: 8,
                        right: 8,
                        fontSize: '0.7rem',
                        fontWeight: 600
                      }}
                    />
                  )}
                  <CardContent sx={{ p: 3 }}>
                    <Box display="flex" alignItems="center" gap={3}>
                      <Avatar sx={{ 
                        width: 60, 
                        height: 60,
                        bgcolor: alpha(theme.palette.primary.main, 0.05) 
                      }}>
                        {provider.icon}
                      </Avatar>
                      <Box flex={1}>
                        <Typography variant="h6" fontWeight={600} mb={0.5}>
                          {provider.name}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          {provider.description}
                        </Typography>
                      </Box>
                      {selectedProvider === provider.id && (
                        <CheckCircleIcon color="primary" sx={{ fontSize: '1.5rem' }} />
                      )}
                    </Box>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>

          {selectedProvider && (
            <Alert severity="info" sx={{ mt: 3 }}>
              <AlertTitle>Authorization Required</AlertTitle>
              You'll be redirected to {selectedProvider.charAt(0).toUpperCase() + selectedProvider.slice(1)} to authorize calendar access. This is secure and your credentials are never stored.
            </Alert>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 3, borderTop: `1px solid ${theme.palette.divider}` }}>
          <Button 
            onClick={() => setAddCalendarOpen(false)}
            sx={{ 
              textTransform: 'none',
              fontWeight: 600,
              borderRadius: 2
            }}
          >
            Cancel
          </Button>
          <Button
            variant="contained"
            onClick={() => handleConnectCalendar(selectedProvider)}
            disabled={!selectedProvider}
            sx={{
              textTransform: 'none',
              fontWeight: 600,
              borderRadius: 2,
              px: 3
            }}
          >
            Connect Calendar
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default CalendarSettings;