import React, { useState } from 'react';
import {
  Box,
  Grid,
  Typography,
  Card,
  CardContent,
  Switch,
  FormControlLabel,
  Chip,
  Stack,
  Button,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  Divider,
  Avatar
} from '@mui/material';
import {
  NotificationsActive as NotificationIcon,
  Email as EmailIcon,
  Sms as SmsIcon,
  PhoneAndroid as PushIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  Edit as EditIcon,
  Schedule as ScheduleIcon,
  AccessTime as TimeIcon,
  Close as CloseIcon,
  Settings as SettingsIcon,
  VolumeUp as SoundIcon,
  Vibration as VibrationIcon
} from '@mui/icons-material';

const MeetingReminder = ({ onReminderChange, initialReminders = [] }) => {
  const [reminders, setReminders] = useState(initialReminders);
  const [isEnabled, setIsEnabled] = useState(initialReminders.length > 0);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingReminder, setEditingReminder] = useState(null);
  const [newReminder, setNewReminder] = useState({
    time: 15,
    timeUnit: 'minutes',
    type: 'email',
    message: '',
    enabled: true
  });

  const reminderTypes = [
    { 
      value: 'email', 
      label: 'Email', 
      icon: <EmailIcon />, 
      color: 'primary',
      description: 'Send email notification'
    },
    { 
      value: 'sms', 
      label: 'SMS', 
      icon: <SmsIcon />, 
      color: 'success',
      description: 'Send text message'
    },
    { 
      value: 'push', 
      label: 'Push Notification', 
      icon: <PushIcon />, 
      color: 'info',
      description: 'Browser/app notification'
    },
    { 
      value: 'popup', 
      label: 'Popup Alert', 
      icon: <NotificationIcon />, 
      color: 'warning',
      description: 'Screen popup alert'
    }
  ];

  const timeUnits = [
    { value: 'minutes', label: 'Minutes' },
    { value: 'hours', label: 'Hours' },
    { value: 'days', label: 'Days' },
    { value: 'weeks', label: 'Weeks' }
  ];

  const quickReminderTimes = [
    { time: 5, unit: 'minutes', label: '5 minutes before' },
    { time: 15, unit: 'minutes', label: '15 minutes before' },
    { time: 30, unit: 'minutes', label: '30 minutes before' },
    { time: 1, unit: 'hours', label: '1 hour before' },
    { time: 2, unit: 'hours', label: '2 hours before' },
    { time: 1, unit: 'days', label: '1 day before' }
  ];

  const defaultMessages = {
    email: 'Your meeting "{meetingTitle}" starts in {time} {timeUnit}. Join here: {meetingLink}',
    sms: 'Meeting reminder: "{meetingTitle}" starts in {time} {timeUnit}. Link: {meetingLink}',
    push: 'Meeting starting soon: {meetingTitle}',
    popup: 'Your meeting "{meetingTitle}" is about to start!'
  };

  const handleToggleReminders = (event) => {
    const enabled = event.target.checked;
    setIsEnabled(enabled);
    
    if (!enabled) {
      setReminders([]);
      updateParent([]);
    } else if (reminders.length === 0) {
      // Add default reminder
      const defaultReminder = {
        id: Date.now(),
        time: 15,
        timeUnit: 'minutes',
        type: 'email',
        message: defaultMessages.email,
        enabled: true
      };
      setReminders([defaultReminder]);
      updateParent([defaultReminder]);
    }
  };

  const updateParent = (updatedReminders) => {
    if (onReminderChange) {
      onReminderChange(updatedReminders);
    }
  };

  const addQuickReminder = (quickReminder) => {
    const reminder = {
      id: Date.now(),
      time: quickReminder.time,
      timeUnit: quickReminder.unit,
      type: 'email',
      message: defaultMessages.email,
      enabled: true
    };
    
    const updatedReminders = [...reminders, reminder];
    setReminders(updatedReminders);
    updateParent(updatedReminders);
  };

  const openAddDialog = () => {
    setEditingReminder(null);
    setNewReminder({
      time: 15,
      timeUnit: 'minutes',
      type: 'email',
      message: defaultMessages.email,
      enabled: true
    });
    setDialogOpen(true);
  };

  const openEditDialog = (reminder) => {
    setEditingReminder(reminder);
    setNewReminder({ ...reminder });
    setDialogOpen(true);
  };

  const handleSaveReminder = () => {
    const reminder = {
      ...newReminder,
      id: editingReminder ? editingReminder.id : Date.now()
    };

    let updatedReminders;
    if (editingReminder) {
      updatedReminders = reminders.map(r => r.id === editingReminder.id ? reminder : r);
    } else {
      updatedReminders = [...reminders, reminder];
    }

    setReminders(updatedReminders);
    updateParent(updatedReminders);
    setDialogOpen(false);
  };

  const deleteReminder = (id) => {
    const updatedReminders = reminders.filter(r => r.id !== id);
    setReminders(updatedReminders);
    updateParent(updatedReminders);
  };

  const toggleReminderEnabled = (id) => {
    const updatedReminders = reminders.map(r => 
      r.id === id ? { ...r, enabled: !r.enabled } : r
    );
    setReminders(updatedReminders);
    updateParent(updatedReminders);
  };

  const getTimeDisplay = (time, timeUnit) => {
    return `${time} ${timeUnit === 'minutes' && time === 1 ? 'minute' : 
                     timeUnit === 'hours' && time === 1 ? 'hour' : 
                     timeUnit === 'days' && time === 1 ? 'day' : 
                     timeUnit === 'weeks' && time === 1 ? 'week' : timeUnit}`;
  };

  const getReminderIcon = (type) => {
    const reminderType = reminderTypes.find(rt => rt.value === type);
    return reminderType ? reminderType.icon : <NotificationIcon />;
  };

  const getReminderColor = (type) => {
    const reminderType = reminderTypes.find(rt => rt.value === type);
    return reminderType ? reminderType.color : 'default';
  };

  return (
    <Card sx={{ borderRadius: 3, boxShadow: 3 }}>
      <CardContent sx={{ p: 4 }}>
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 4 }}>
          <Box sx={{ 
            p: 2, 
            borderRadius: 2, 
            bgcolor: 'warning.50', 
            display: 'flex', 
            alignItems: 'center',
            mr: 2 
          }}>
            <NotificationIcon sx={{ color: 'warning.main', fontSize: 32 }} />
          </Box>
          <Box>
            <Typography variant="h4" color="warning.main" fontWeight="bold">
              Meeting Reminders
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Set up automatic notifications before your meeting starts
            </Typography>
          </Box>
        </Box>

        {/* Enable Toggle */}
        <Card sx={{ mb: 4, bgcolor: 'grey.50', border: '2px dashed', borderColor: 'warning.200' }}>
          <CardContent sx={{ p: 3 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={isEnabled}
                  onChange={handleToggleReminders}
                  color="warning"
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
                    {isEnabled ? '🔔 Reminders Enabled' : '🔕 Enable Reminders'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    {isEnabled 
                      ? 'You will receive notifications based on your settings below'
                      : 'Turn on to get notified before your meetings start'
                    }
                  </Typography>
                </Box>
              }
              sx={{ m: 0 }}
            />
          </CardContent>
        </Card>

        {isEnabled && (
          <>
            {/* Quick Reminders */}
            <Box sx={{ mb: 4 }}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                <TimeIcon sx={{ mr: 1 }} />
                Quick Add Reminders
              </Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap" gap={1}>
                {quickReminderTimes.map((quick, index) => (
                  <Chip
                    key={index}
                    label={quick.label}
                    onClick={() => addQuickReminder(quick)}
                    color="warning"
                    variant="outlined"
                    sx={{ 
                      cursor: 'pointer',
                      '&:hover': {
                        bgcolor: 'warning.50'
                      }
                    }}
                    disabled={reminders.some(r => r.time === quick.time && r.timeUnit === quick.unit)}
                  />
                ))}
              </Stack>
            </Box>

            {/* Current Reminders */}
            <Box sx={{ mb: 4 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 3 }}>
                <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center' }}>
                  <ScheduleIcon sx={{ mr: 1 }} />
                  Active Reminders ({reminders.length})
                </Typography>
                <Button
                  startIcon={<AddIcon />}
                  onClick={openAddDialog}
                  variant="contained"
                  color="warning"
                  sx={{ borderRadius: 2 }}
                >
                  Add Custom
                </Button>
              </Box>

              {reminders.length === 0 ? (
                <Paper sx={{ p: 4, textAlign: 'center', bgcolor: 'grey.50' }}>
                  <NotificationIcon sx={{ fontSize: 48, color: 'text.disabled', mb: 2 }} />
                  <Typography variant="h6" color="text.secondary" gutterBottom>
                    No reminders set
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    Add reminders to get notified before your meeting starts
                  </Typography>
                  <Button
                    variant="outlined"
                    startIcon={<AddIcon />}
                    onClick={openAddDialog}
                    color="warning"
                  >
                    Add First Reminder
                  </Button>
                </Paper>
              ) : (
                <List sx={{ bgcolor: 'background.paper', borderRadius: 2 }}>
                  {reminders.map((reminder, index) => (
                    <React.Fragment key={reminder.id}>
                      <ListItem
                        sx={{
                          bgcolor: reminder.enabled ? 'transparent' : 'grey.100',
                          borderRadius: 1,
                          mb: 1,
                          border: '1px solid',
                          borderColor: reminder.enabled ? 'grey.300' : 'grey.400'
                        }}
                      >
                        <Avatar
                          sx={{
                            bgcolor: `${getReminderColor(reminder.type)}.main`,
                            mr: 2,
                            width: 40,
                            height: 40
                          }}
                        >
                          {getReminderIcon(reminder.type)}
                        </Avatar>
                        
                        <ListItemText
                          primary={
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="h6">
                                {getTimeDisplay(reminder.time, reminder.timeUnit)} before
                              </Typography>
                              <Chip
                                label={reminderTypes.find(rt => rt.value === reminder.type)?.label}
                                size="small"
                                color={getReminderColor(reminder.type)}
                                variant="outlined"
                              />
                              {!reminder.enabled && (
                                <Chip
                                  label="Disabled"
                                  size="small"
                                  color="default"
                                  variant="filled"
                                />
                              )}
                            </Box>
                          }
                          secondary={
                            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                              {reminder.message.length > 80 
                                ? `${reminder.message.substring(0, 80)}...` 
                                : reminder.message}
                            </Typography>
                          }
                        />
                        
                        <ListItemSecondaryAction>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Switch
                              checked={reminder.enabled}
                              onChange={() => toggleReminderEnabled(reminder.id)}
                              color="warning"
                              size="small"
                            />
                            <IconButton
                              onClick={() => openEditDialog(reminder)}
                              color="primary"
                              size="small"
                            >
                              <EditIcon />
                            </IconButton>
                            <IconButton
                              onClick={() => deleteReminder(reminder.id)}
                              color="error"
                              size="small"
                            >
                              <DeleteIcon />
                            </IconButton>
                          </Box>
                        </ListItemSecondaryAction>
                      </ListItem>
                      {index < reminders.length - 1 && <Divider />}
                    </React.Fragment>
                  ))}
                </List>
              )}
            </Box>

            {/* Summary */}
            {reminders.length > 0 && (
              <Card sx={{ bgcolor: 'warning.50', border: '1px solid', borderColor: 'warning.200' }}>
                <CardContent sx={{ p: 3 }}>
                  <Typography variant="h6" color="warning.main" gutterBottom>
                    Reminder Summary
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" sx={{ mb: 1 }}>
                        <strong>Total Reminders:</strong> {reminders.length}
                      </Typography>
                      <Typography variant="body2" sx={{ mb: 1 }}>
                        <strong>Active:</strong> {reminders.filter(r => r.enabled).length}
                      </Typography>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" sx={{ mb: 1 }}>
                        <strong>Notification Types:</strong>
                      </Typography>
                      <Stack direction="row" spacing={0.5} flexWrap="wrap" gap={0.5}>
                        {[...new Set(reminders.filter(r => r.enabled).map(r => r.type))].map(type => (
                          <Chip
                            key={type}
                            label={reminderTypes.find(rt => rt.value === type)?.label}
                            size="small"
                            color={getReminderColor(type)}
                            variant="filled"
                          />
                        ))}
                      </Stack>
                    </Grid>
                  </Grid>
                </CardContent>
              </Card>
            )}
          </>
        )}
      </CardContent>

      {/* Add/Edit Dialog */}
      <Dialog 
        open={dialogOpen} 
        onClose={() => setDialogOpen(false)}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 3 }
        }}
      >
        <DialogTitle sx={{ pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <SettingsIcon sx={{ mr: 1, color: 'warning.main' }} />
              <Typography variant="h5">
                {editingReminder ? 'Edit Reminder' : 'Add New Reminder'}
              </Typography>
            </Box>
            <IconButton onClick={() => setDialogOpen(false)}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>

        <DialogContent sx={{ pt: 2 }}>
          <Grid container spacing={3}>
            {/* Time Settings */}
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom>
                When to Remind
              </Typography>
            </Grid>
            
            <Grid item xs={6}>
              <TextField
                label="Time"
                type="number"
                value={newReminder.time}
                onChange={(e) => setNewReminder({ 
                  ...newReminder, 
                  time: Math.max(1, parseInt(e.target.value) || 1) 
                })}
                inputProps={{ min: 1, max: 999 }}
                fullWidth
                variant="outlined"
              />
            </Grid>
            
            <Grid item xs={6}>
              <FormControl fullWidth>
                <InputLabel>Time Unit</InputLabel>
                <Select
                  value={newReminder.timeUnit}
                  onChange={(e) => setNewReminder({ ...newReminder, timeUnit: e.target.value })}
                  label="Time Unit"
                >
                  {timeUnits.map((unit) => (
                    <MenuItem key={unit.value} value={unit.value}>
                      {unit.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>

            {/* Notification Type */}
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
                Notification Type
              </Typography>
              <Grid container spacing={2}>
                {reminderTypes.map((type) => (
                  <Grid item xs={6} sm={3} key={type.value}>
                    <Card
                      onClick={() => setNewReminder({ 
                        ...newReminder, 
                        type: type.value,
                        message: newReminder.message || defaultMessages[type.value]
                      })}
                      sx={{
                        cursor: 'pointer',
                        border: newReminder.type === type.value ? '2px solid' : '1px solid',
                        borderColor: newReminder.type === type.value ? `${type.color}.main` : 'grey.300',
                        bgcolor: newReminder.type === type.value ? `${type.color}.50` : 'background.paper',
                        '&:hover': {
                          transform: 'translateY(-2px)',
                          boxShadow: 2
                        },
                        transition: 'all 0.2s ease'
                      }}
                    >
                      <CardContent sx={{ textAlign: 'center', p: 2 }}>
                        <Avatar
                          sx={{
                            bgcolor: `${type.color}.main`,
                            mx: 'auto',
                            mb: 1,
                            width: 40,
                            height: 40
                          }}
                        >
                          {type.icon}
                        </Avatar>
                        <Typography variant="body2" fontWeight="bold">
                          {type.label}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {type.description}
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                ))}
              </Grid>
            </Grid>

            {/* Custom Message */}
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
                Notification Message
              </Typography>
              <TextField
                label="Custom message"
                value={newReminder.message}
                onChange={(e) => setNewReminder({ ...newReminder, message: e.target.value })}
                fullWidth
                multiline
                rows={3}
                variant="outlined"
                placeholder={defaultMessages[newReminder.type]}
                helperText="Use {meetingTitle}, {time}, {timeUnit}, {meetingLink} as placeholders"
              />
            </Grid>

            {/* Preview */}
            <Grid item xs={12}>
              <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  Preview:
                </Typography>
                <Typography variant="body1">
                  {newReminder.message || defaultMessages[newReminder.type]}
                </Typography>
              </Paper>
            </Grid>
          </Grid>
        </DialogContent>

        <DialogActions sx={{ p: 3, pt: 1 }}>
          <Button onClick={() => setDialogOpen(false)}>
            Cancel
          </Button>
          <Button 
            onClick={handleSaveReminder}
            variant="contained"
            color="warning"
            size="large"
            disabled={!newReminder.time || !newReminder.type}
          >
            {editingReminder ? 'Update Reminder' : 'Add Reminder'}
          </Button>
        </DialogActions>
      </Dialog>
    </Card>
  );
};

export default MeetingReminder;