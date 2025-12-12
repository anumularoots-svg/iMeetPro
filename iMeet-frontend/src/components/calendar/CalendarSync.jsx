import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Switch,
  FormControlLabel,
  Avatar,
  Stack,
  Chip,
  Alert,
  Divider,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  useTheme,
  alpha,
  CircularProgress,
  LinearProgress
} from '@mui/material';
import {
  Google as GoogleIcon,
  Microsoft as OutlookIcon,
  Apple as AppleIcon,
  CalendarToday as CalendarIcon,
  Sync as SyncIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  Edit as EditIcon,
  CheckCircle as SuccessIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  Refresh as RefreshIcon,
  Settings as SettingsIcon,
  CloudSync as CloudSyncIcon,
  Schedule as ScheduleIcon
} from '@mui/icons-material';

const CalendarSync = ({ onSyncComplete, userCalendars = [] }) => {
  const theme = useTheme();
  const [connectedCalendars, setConnectedCalendars] = useState([]);
  const [syncInProgress, setSyncInProgress] = useState(false);
  const [addCalendarOpen, setAddCalendarOpen] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState('');
  const [syncSettings, setSyncSettings] = useState({
    autoSync: true,
    syncFrequency: 15, // minutes
    twoWaySync: false,
    syncPastEvents: false
  });
  const [lastSyncTime, setLastSyncTime] = useState(null);

  const calendarProviders = [
    {
      id: 'google',
      name: 'Google Calendar',
      icon: GoogleIcon,
      color: '#4285f4',
      description: 'Sync with your Google Calendar'
    },
    {
      id: 'outlook',
      name: 'Microsoft Outlook',
      icon: OutlookIcon,
      color: '#0078d4',
      description: 'Sync with Outlook/Office 365'
    },
    {
      id: 'apple',
      name: 'Apple Calendar',
      icon: AppleIcon,
      color: '#000000',
      description: 'Sync with iCloud Calendar'
    }
  ];

  // Mock connected calendars
  useEffect(() => {
    setConnectedCalendars([
      {
        id: 1,
        provider: 'google',
        name: 'Work Calendar',
        email: 'user@company.com',
        status: 'connected',
        lastSync: new Date().toISOString(),
        eventsCount: 25,
        enabled: true
      },
      {
        id: 2,
        provider: 'outlook',
        name: 'Personal Calendar',
        email: 'user@outlook.com',
        status: 'error',
        lastSync: new Date(Date.now() - 86400000).toISOString(),
        eventsCount: 12,
        enabled: false,
        error: 'Authentication expired'
      }
    ]);
    setLastSyncTime(new Date());
  }, []);

  const handleConnectCalendar = async (providerId) => {
    setSyncInProgress(true);
    
    // Simulate OAuth flow
    setTimeout(() => {
      const provider = calendarProviders.find(p => p.id === providerId);
      const newCalendar = {
        id: Date.now(),
        provider: providerId,
        name: `${provider.name}`,
        email: `user@${providerId}.com`,
        status: 'connected',
        lastSync: new Date().toISOString(),
        eventsCount: Math.floor(Math.random() * 50),
        enabled: true
      };
      
      setConnectedCalendars(prev => [...prev, newCalendar]);
      setAddCalendarOpen(false);
      setSyncInProgress(false);
      setSelectedProvider('');
    }, 2000);
  };

  const handleDisconnectCalendar = (calendarId) => {
    setConnectedCalendars(prev => prev.filter(cal => cal.id !== calendarId));
  };

  const handleToggleCalendar = (calendarId, enabled) => {
    setConnectedCalendars(prev =>
      prev.map(cal =>
        cal.id === calendarId ? { ...cal, enabled } : cal
      )
    );
  };

  const handleSyncNow = async () => {
    setSyncInProgress(true);
    
    // Simulate sync process
    setTimeout(() => {
      setConnectedCalendars(prev =>
        prev.map(cal => ({
          ...cal,
          lastSync: new Date().toISOString(),
          status: 'connected',
          error: null
        }))
      );
      setLastSyncTime(new Date());
      setSyncInProgress(false);
      onSyncComplete?.();
    }, 3000);
  };

  const getProviderIcon = (providerId) => {
    const provider = calendarProviders.find(p => p.id === providerId);
    return provider ? provider.icon : CalendarIcon;
  };

  const getProviderColor = (providerId) => {
    const provider = calendarProviders.find(p => p.id === providerId);
    return provider ? provider.color : theme.palette.primary.main;
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'connected': return 'success';
      case 'error': return 'error';
      case 'syncing': return 'warning';
      default: return 'default';
    }
  };

  const formatLastSync = (dateString) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins} minutes ago`;
    
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours} hours ago`;
    
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays} days ago`;
  };

  return (
    <Box>
      {/* Header */}
      <Card 
        elevation={3}
        sx={{
          mb: 3,
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.1)} 0%, ${alpha(theme.palette.secondary.main, 0.05)} 100%)`,
          border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`
        }}
      >
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Avatar
                sx={{
                  bgcolor: theme.palette.primary.main,
                  mr: 2,
                  background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
                }}
              >
                <CloudSyncIcon />
              </Avatar>
              <Box>
                <Typography variant="h6" fontWeight="bold">
                  Calendar Sync
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Connect and sync external calendars
                </Typography>
              </Box>
            </Box>
            <Stack direction="row" spacing={1}>
              <Button
                variant="outlined"
                startIcon={<AddIcon />}
                onClick={() => setAddCalendarOpen(true)}
                sx={{ borderRadius: 2 }}
              >
                Add Calendar
              </Button>
              <Button
                variant="contained"
                startIcon={syncInProgress ? <CircularProgress size={16} color="inherit" /> : <SyncIcon />}
                onClick={handleSyncNow}
                disabled={syncInProgress || connectedCalendars.length === 0}
                sx={{
                  borderRadius: 2,
                  background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
                }}
              >
                {syncInProgress ? 'Syncing...' : 'Sync Now'}
              </Button>
            </Stack>
          </Box>

          {/* Sync Progress */}
          {syncInProgress && (
            <Box sx={{ mt: 2 }}>
              <LinearProgress sx={{ borderRadius: 1 }} />
              <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                Syncing calendar events...
              </Typography>
            </Box>
          )}

          {/* Last Sync Info */}
          {lastSyncTime && !syncInProgress && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="caption" color="text.secondary">
                Last sync: {formatLastSync(lastSyncTime.toISOString())}
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Sync Settings */}
      <Card elevation={2} sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
            <SettingsIcon sx={{ color: theme.palette.primary.main, mr: 1 }} />
            <Typography variant="subtitle1" fontWeight="bold">
              Sync Settings
            </Typography>
          </Box>

          <Stack spacing={2}>
            <FormControlLabel
              control={
                <Switch
                  checked={syncSettings.autoSync}
                  onChange={(e) => setSyncSettings(prev => ({ ...prev, autoSync: e.target.checked }))}
                  color="primary"
                />
              }
              label="Automatic sync"
            />

            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <FormControl size="small" sx={{ minWidth: 150 }}>
                <InputLabel>Sync Frequency</InputLabel>
                <Select
                  value={syncSettings.syncFrequency}
                  label="Sync Frequency"
                  onChange={(e) => setSyncSettings(prev => ({ ...prev, syncFrequency: e.target.value }))}
                >
                  <MenuItem value={5}>Every 5 minutes</MenuItem>
                  <MenuItem value={15}>Every 15 minutes</MenuItem>
                  <MenuItem value={30}>Every 30 minutes</MenuItem>
                  <MenuItem value={60}>Every hour</MenuItem>
                </Select>
              </FormControl>
              <Typography variant="body2" color="text.secondary">
                How often to sync calendar events
              </Typography>
            </Box>

            <FormControlLabel
              control={
                <Switch
                  checked={syncSettings.twoWaySync}
                  onChange={(e) => setSyncSettings(prev => ({ ...prev, twoWaySync: e.target.checked }))}
                  color="primary"
                />
              }
              label="Two-way sync"
            />

            <FormControlLabel
              control={
                <Switch
                  checked={syncSettings.syncPastEvents}
                  onChange={(e) => setSyncSettings(prev => ({ ...prev, syncPastEvents: e.target.checked }))}
                  color="primary"
                />
              }
              label="Sync past events"
            />
          </Stack>
        </CardContent>
      </Card>

      {/* Connected Calendars */}
      <Card elevation={2}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Typography variant="subtitle1" fontWeight="bold">
              Connected Calendars ({connectedCalendars.length})
            </Typography>
            {connectedCalendars.length === 0 && (
              <Button
                variant="text"
                startIcon={<AddIcon />}
                onClick={() => setAddCalendarOpen(true)}
                size="small"
              >
                Add your first calendar
              </Button>
            )}
          </Box>

          {connectedCalendars.length === 0 ? (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <Avatar
                sx={{
                  bgcolor: alpha(theme.palette.primary.main, 0.1),
                  width: 64,
                  height: 64,
                  mx: 'auto',
                  mb: 2
                }}
              >
                <CalendarIcon sx={{ fontSize: 32, color: 'primary.main' }} />
              </Avatar>
              <Typography variant="h6" color="text.secondary" gutterBottom>
                No calendars connected
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Connect your external calendars to sync events automatically
              </Typography>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={() => setAddCalendarOpen(true)}
                sx={{ borderRadius: 2 }}
              >
                Connect Calendar
              </Button>
            </Box>
          ) : (
            <List>
              {connectedCalendars.map((calendar, index) => {
                const ProviderIcon = getProviderIcon(calendar.provider);
                
                return (
                  <ListItem
                    key={calendar.id}
                    sx={{
                      border: `1px solid ${alpha(theme.palette.divider, 0.2)}`,
                      borderRadius: 2,
                      mb: index < connectedCalendars.length - 1 ? 1 : 0,
                      bgcolor: calendar.enabled ? 'background.paper' : alpha(theme.palette.action.disabled, 0.1)
                    }}
                  >
                    <ListItemAvatar>
                      <Avatar
                        sx={{
                          bgcolor: getProviderColor(calendar.provider),
                          width: 40,
                          height: 40
                        }}
                      >
                        <ProviderIcon sx={{ color: 'white' }} />
                      </Avatar>
                    </ListItemAvatar>
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography variant="subtitle2" fontWeight="bold">
                            {calendar.name}
                          </Typography>
                          <Chip
                            label={calendar.status}
                            color={getStatusColor(calendar.status)}
                            size="small"
                            variant="outlined"
                          />
                        </Box>
                      }
                      secondary={
                        <Box>
                          <Typography variant="caption" color="text.secondary">
                            {calendar.email} • {calendar.eventsCount} events
                          </Typography>
                          <br />
                          <Typography variant="caption" color="text.secondary">
                            Last sync: {formatLastSync(calendar.lastSync)}
                          </Typography>
                          {calendar.error && (
                            <Alert severity="error" sx={{ mt: 1, fontSize: '0.75rem' }}>
                              {calendar.error}
                            </Alert>
                          )}
                        </Box>
                      }
                    />
                    <ListItemSecondaryAction>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Switch
                          checked={calendar.enabled}
                          onChange={(e) => handleToggleCalendar(calendar.id, e.target.checked)}
                          color="primary"
                          size="small"
                        />
                        <IconButton
                          onClick={() => handleDisconnectCalendar(calendar.id)}
                          color="error"
                          size="small"
                        >
                          <DeleteIcon />
                        </IconButton>
                      </Stack>
                    </ListItemSecondaryAction>
                  </ListItem>
                );
              })}
            </List>
          )}
        </CardContent>
      </Card>

      {/* Add Calendar Dialog */}
      <Dialog
        open={addCalendarOpen}
        onClose={() => setAddCalendarOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Avatar sx={{ bgcolor: theme.palette.primary.main, mr: 2 }}>
              <AddIcon />
            </Avatar>
            <Box>
              <Typography variant="h6" fontWeight="bold">
                Connect Calendar
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Choose a calendar provider to sync with
              </Typography>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Stack spacing={2}>
            {calendarProviders.map((provider) => {
              const ProviderIcon = provider.icon;
              
              return (
                <Card
                  key={provider.id}
                  elevation={selectedProvider === provider.id ? 4 : 1}
                  sx={{
                    cursor: 'pointer',
                    border: selectedProvider === provider.id 
                      ? `2px solid ${theme.palette.primary.main}`
                      : `1px solid ${alpha(theme.palette.divider, 0.2)}`,
                    '&:hover': {
                      boxShadow: theme.shadows[4]
                    },
                    transition: 'all 0.3s ease'
                  }}
                  onClick={() => setSelectedProvider(provider.id)}
                >
                  <CardContent sx={{ p: 2 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                      <Avatar
                        sx={{
                          bgcolor: provider.color,
                          mr: 2,
                          width: 48,
                          height: 48
                        }}
                      >
                        <ProviderIcon sx={{ color: 'white' }} />
                      </Avatar>
                      <Box sx={{ flexGrow: 1 }}>
                        <Typography variant="subtitle1" fontWeight="bold">
                          {provider.name}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          {provider.description}
                        </Typography>
                      </Box>
                      {selectedProvider === provider.id && (
                        <SuccessIcon color="primary" />
                      )}
                    </Box>
                  </CardContent>
                </Card>
              );
            })}
          </Stack>
        </DialogContent>
        <DialogActions sx={{ p: 3 }}>
          <Button
            onClick={() => setAddCalendarOpen(false)}
            sx={{ borderRadius: 2 }}
          >
            Cancel
          </Button>
          <Button
            variant="contained"
            onClick={() => handleConnectCalendar(selectedProvider)}
            disabled={!selectedProvider || syncInProgress}
            startIcon={syncInProgress ? <CircularProgress size={16} color="inherit" /> : <SyncIcon />}
            sx={{
              borderRadius: 2,
              background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`
            }}
          >
            {syncInProgress ? 'Connecting...' : 'Connect'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default CalendarSync;