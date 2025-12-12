import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Box,
  Container,
  Typography,
  Card,
  CardContent,
  Grid,
  Switch,
  Button,
  Divider,
  Avatar,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Chip,
  Alert,
  Tabs,
  Tab,
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
  useTheme,
  alpha,
  Stack,
  Badge,
  Tooltip,
  LinearProgress,
  FormControlLabel,
  InputAdornment,
  Breadcrumbs,
  Link,
  createTheme,
  ThemeProvider,
  CircularProgress,
  Snackbar,
} from '@mui/material';
import {
  Person,
  VideoCall,
  Notifications,
  Security,
  Language,
  Edit,
  Save,
  Camera,
  Shield,
  Settings as SettingsIcon,
  CloudSync,
  Email,
  ArrowBack as ArrowBackIcon,
  CheckCircle,
  Warning,
  Info,
  Phone,
  LocationOn,
  Public,
  Visibility,
  VisibilityOff,
  Home,
  NavigateNext,
  Business,
  VpnKey,
  AdminPanelSettings,
  Group,
  MoreVert,
  Refresh,
  Download,
  Upload,
  Help,
  ContactSupport,
  PowerSettingsNew,
  AccountCircle,
  WorkOutline,
  School,
  Place,
  Schedule,
  PhotoCamera,
  CloudUpload,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';

const professionalTheme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
      light: '#42a5f5',
      dark: '#1565c0',
      contrastText: '#ffffff',
    },
    secondary: {
      main: '#757575',
      light: '#a4a4a4',
      dark: '#494949',
    },
    background: {
      default: '#f8fafc',
      paper: '#ffffff',
    },
    text: {
      primary: '#263238',
      secondary: '#546e7a',
    },
    divider: '#e1e5e9',
    success: {
      main: '#2e7d32',
      light: '#4caf50',
      dark: '#1b5e20',
    },
    warning: {
      main: '#ed6c02',
      light: '#ff9800',
      dark: '#e65100',
    },
    error: {
      main: '#d32f2f',
      light: '#ef5350',
      dark: '#c62828',
    },
  },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
    h4: {
      fontWeight: 700,
      fontSize: '2rem',
    },
    h5: {
      fontWeight: 600,
      fontSize: '1.5rem',
    },
    h6: {
      fontWeight: 600,
      fontSize: '1.25rem',
    },
    body1: {
      fontSize: '1rem',
      lineHeight: 1.6,
    },
    body2: {
      fontSize: '0.875rem',
      lineHeight: 1.5,
    },
  },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
          borderRadius: 12,
          border: '1px solid #e1e5e9',
        }
      }
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontWeight: 600,
          borderRadius: 8,
          padding: '10px 24px',
        },
        contained: {
          boxShadow: '0 2px 4px rgba(25, 118, 210, 0.25)',
          '&:hover': {
            boxShadow: '0 4px 12px rgba(25, 118, 210, 0.4)',
            transform: 'translateY(-1px)',
          },
        },
      }
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            borderRadius: 8,
            '&:hover .MuiOutlinedInput-notchedOutline': {
              borderColor: '#1976d2',
            },
            '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
              borderWidth: 2,
            },
          }
        }
      }
    },
    MuiTab: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontWeight: 600,
          fontSize: '0.95rem',
          minHeight: 64,
          '&.Mui-selected': {
            color: '#1976d2',
          }
        }
      }
    }
  }
});

function TabPanel({ children, value, index, ...other }) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`settings-tabpanel-${index}`}
      aria-labelledby={`settings-tab-${index}`}
      {...other}
    >
      {value === index && <Box>{children}</Box>}
    </div>
  );
}

// ✅ FIXED: ProfileTab component moved OUTSIDE
const ProfileTab = ({ 
  userProfile, 
  isDragging, 
  handleProfileUpdate, 
  handleDragOver, 
  handleDragLeave, 
  handleDrop, 
  triggerFileUpload, 
  fileInputRef,
  handleProfilePictureChange,
  theme 
}) => (
  <Grid container spacing={3}>
    <Grid item xs={12}>
      <Card>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 4 }}>
            <AccountCircle sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
            <Box>
              <Typography variant="h5" fontWeight={700}>
                Profile Information
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Update your personal details and contact information
              </Typography>
            </Box>
          </Box>
          
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 4 }}>
            <Box position="relative">
              <input
                type="file"
                accept="image/*"
                style={{ display: 'none' }}
                ref={fileInputRef}
                onChange={(e) => {
                  if (e.target.files && e.target.files[0]) {
                    handleProfilePictureChange(e.target.files[0]);
                  }
                }}
              />
              
              <Badge
                overlap="circular"
                anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                badgeContent={
                  <Tooltip title="Change Profile Picture">
                    <IconButton
                      size="small"
                      onClick={triggerFileUpload}
                      sx={{
                        bgcolor: 'primary.main',
                        color: 'white',
                        width: 36,
                        height: 36,
                        '&:hover': { 
                          bgcolor: 'primary.dark',
                          transform: 'scale(1.1)'
                        },
                        transition: 'all 0.2s ease'
                      }}
                    >
                      <PhotoCamera fontSize="small" />
                    </IconButton>
                  </Tooltip>
                }
              >
                <Avatar
                  src={userProfile.profilePicture}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  onDrop={handleDrop}
                  sx={{ 
                    width: 120, 
                    height: 120,
                    bgcolor: 'primary.main',
                    fontSize: '2.5rem',
                    fontWeight: 600,
                    cursor: 'pointer',
                    border: `4px solid ${isDragging ? theme.palette.primary.light : 'white'}`,
                    boxShadow: isDragging ? theme.shadows[16] : theme.shadows[8],
                    transition: 'all 0.3s ease',
                    transform: isDragging ? 'scale(1.02)' : 'scale(1)',
                    '&:hover': {
                      transform: 'scale(1.02)',
                      boxShadow: theme.shadows[12],
                    },
                  }}
                  onClick={triggerFileUpload}
                >
                  {!userProfile.profilePicture && 
                    `${userProfile.firstName?.charAt(0) || 'U'}${userProfile.lastName?.charAt(0) || ''}`
                  }
                </Avatar>
              </Badge>
            </Box>
            
            <Box sx={{ ml: 3 }}>
              <Typography variant="h6" fontWeight={600}>
                Profile Picture
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                Upload a professional photo. Recommended size: 400x400px
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Click the camera icon or drag & drop an image
              </Typography>
            </Box>
          </Box>

          <Grid container spacing={3}>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="First Name"
                value={userProfile.firstName || ''}
                onChange={(e) => handleProfileUpdate('firstName', e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Person color="action" />
                    </InputAdornment>
                  ),
                }}
                sx={{
                  '& .MuiOutlinedInput-root': {
                    '&:hover fieldset': {
                      borderColor: 'primary.main',
                    },
                  },
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Last Name"
                value={userProfile.lastName || ''}
                onChange={(e) => handleProfileUpdate('lastName', e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Person color="action" />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Email Address"
                type="email"
                value={userProfile.email || ''}
                onChange={(e) => handleProfileUpdate('email', e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Email color="action" />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Phone Number"
                value={userProfile.phoneNumber || ''}
                onChange={(e) => {
                  const value = e.target.value.replace(/[^\d\+\-\(\)\s]/g, '');
                  handleProfileUpdate('phoneNumber', value);
                }}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Phone color="action" />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Job Title"
                value={userProfile.jobTitle || ''}
                onChange={(e) => handleProfileUpdate('jobTitle', e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <WorkOutline color="action" />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Department"
                value={userProfile.department || ''}
                onChange={(e) => handleProfileUpdate('department', e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Business color="action" />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Organization"
                value={userProfile.organization || ''}
                onChange={(e) => handleProfileUpdate('organization', e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <School color="action" />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <FormControl fullWidth>
                <InputLabel>Timezone</InputLabel>
                <Select
                  value={userProfile.timezone || ''}
                  label="Timezone"
                  onChange={(e) => handleProfileUpdate('timezone', e.target.value)}
                >
                  <MenuItem value="UTC-8">Pacific Time (UTC-8)</MenuItem>
                  <MenuItem value="UTC-5">Eastern Time (UTC-5)</MenuItem>
                  <MenuItem value="UTC+0">Greenwich Mean Time (UTC+0)</MenuItem>
                  <MenuItem value="UTC+1">Central European Time (UTC+1)</MenuItem>
                  <MenuItem value="UTC+5:30">India Standard Time (UTC+5:30)</MenuItem>
                  <MenuItem value="UTC+9">Japan Standard Time (UTC+9)</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Location"
                value={userProfile.location || ''}
                onChange={(e) => handleProfileUpdate('location', e.target.value)}
                multiline
                rows={2}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <LocationOn color="action" />
                    </InputAdornment>
                  ),
                }}
                placeholder="Enter your city, state, country"
              />
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </Grid>
  </Grid>
);

// ✅ FIXED: MeetingTab component moved OUTSIDE
const MeetingTab = ({ meetingSettings, handleMeetingSettingUpdate }) => (
  <Grid container spacing={3}>
    <Grid item xs={12} lg={6}>
      <Card>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <VideoCall sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
            <Box>
              <Typography variant="h6" fontWeight={600}>
                Default Meeting Settings
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Configure your default meeting preferences
              </Typography>
            </Box>
          </Box>
          
          <Stack spacing={3}>
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.defaultCameraState}
                    onChange={(e) => handleMeetingSettingUpdate('defaultCameraState', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Camera On by Default</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Start meetings with camera enabled
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.defaultMicrophoneState}
                    onChange={(e) => handleMeetingSettingUpdate('defaultMicrophoneState', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Microphone On by Default</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Start meetings with microphone enabled
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.autoJoinAudio}
                    onChange={(e) => handleMeetingSettingUpdate('autoJoinAudio', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Auto-join Audio</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Automatically connect to audio when joining
                    </Typography>
                  </Box>
                }
              />
            </Box>
          </Stack>

          <Divider sx={{ my: 3 }} />

          <Grid container spacing={2}>
            <Grid item xs={6}>
              <FormControl fullWidth size="small">
                <InputLabel>Video Quality</InputLabel>
                <Select
                  value={meetingSettings.videoQuality}
                  label="Video Quality"
                  onChange={(e) => handleMeetingSettingUpdate('videoQuality', e.target.value)}
                >
                  <MenuItem value="SD">Standard (480p)</MenuItem>
                  <MenuItem value="HD">High Definition (720p)</MenuItem>
                  <MenuItem value="FHD">Full HD (1080p)</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={6}>
              <FormControl fullWidth size="small">
                <InputLabel>Audio Quality</InputLabel>
                <Select
                  value={meetingSettings.audioQuality}
                  label="Audio Quality"
                  onChange={(e) => handleMeetingSettingUpdate('audioQuality', e.target.value)}
                >
                  <MenuItem value="Low">Low</MenuItem>
                  <MenuItem value="Medium">Medium</MenuItem>
                  <MenuItem value="High">High</MenuItem>
                </Select>
              </FormControl>
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </Grid>

    <Grid item xs={12} lg={6}>
      <Card>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <Security sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
            <Box>
              <Typography variant="h6" fontWeight={600}>
                Security & Privacy
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Meeting security and privacy options
              </Typography>
            </Box>
          </Box>
          
          <Stack spacing={3}>
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.enableWaitingRoom}
                    onChange={(e) => handleMeetingSettingUpdate('enableWaitingRoom', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Enable Waiting Room</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Participants wait for host approval
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.requireMeetingPassword}
                    onChange={(e) => handleMeetingSettingUpdate('requireMeetingPassword', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Require Meeting Password</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Password required to join meetings
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.allowParticipantScreenShare}
                    onChange={(e) => handleMeetingSettingUpdate('allowParticipantScreenShare', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Allow Screen Sharing</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Participants can share their screen
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingSettings.recordMeetingsByDefault}
                    onChange={(e) => handleMeetingSettingUpdate('recordMeetingsByDefault', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Auto-record Meetings</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Automatically start recording
                    </Typography>
                  </Box>
                }
              />
            </Box>
          </Stack>
        </CardContent>
      </Card>
    </Grid>
  </Grid>
);

// ✅ FIXED: NotificationTab component moved OUTSIDE
const NotificationTab = ({ notificationSettings, handleNotificationUpdate }) => (
  <Grid container spacing={3}>
    <Grid item xs={12} lg={6}>
      <Card>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <Notifications sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
            <Box>
              <Typography variant="h6" fontWeight={600}>
                Notification Preferences
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Choose how you want to receive notifications
              </Typography>
            </Box>
          </Box>
          
          <Stack spacing={3}>
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={notificationSettings.emailNotifications}
                    onChange={(e) => handleNotificationUpdate('emailNotifications', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Email Notifications</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Receive notifications via email
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={notificationSettings.meetingReminders}
                    onChange={(e) => handleNotificationUpdate('meetingReminders', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Meeting Reminders</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Reminders before scheduled meetings
                    </Typography>
                  </Box>
                }
              />
            </Box>
            
            <Box>
              <FormControlLabel
                control={
                  <Switch
                    checked={notificationSettings.securityAlerts}
                    onChange={(e) => handleNotificationUpdate('securityAlerts', e.target.checked)}
                    color="primary"
                  />
                }
                label={
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Security Alerts</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Important security notifications
                    </Typography>
                  </Box>
                }
              />
            </Box>
          </Stack>

          <Divider sx={{ my: 3 }} />

          <Box>
            <Typography variant="body1" fontWeight={500} gutterBottom>
              Meeting Reminder Timing
            </Typography>
            <FormControl fullWidth size="small">
              <InputLabel>Reminder Time</InputLabel>
              <Select
                value={notificationSettings.reminderTiming}
                label="Reminder Time"
                onChange={(e) => handleNotificationUpdate('reminderTiming', e.target.value)}
              >
                <MenuItem value={5}>5 minutes before</MenuItem>
                <MenuItem value={10}>10 minutes before</MenuItem>
                <MenuItem value={15}>15 minutes before</MenuItem>
                <MenuItem value={30}>30 minutes before</MenuItem>
                <MenuItem value={60}>1 hour before</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </CardContent>
      </Card>
    </Grid>

    <Grid item xs={12} lg={6}>
      <Card>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <Schedule sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
            <Box>
              <Typography variant="h6" fontWeight={600}>
                Quiet Hours
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Set times when notifications are disabled
              </Typography>
            </Box>
          </Box>
          
          <Box sx={{ mb: 3 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={notificationSettings.quietHoursEnabled}
                  onChange={(e) => handleNotificationUpdate('quietHoursEnabled', e.target.checked)}
                  color="primary"
                />
              }
              label={
                <Box>
                  <Typography variant="body1" fontWeight={500}>Enable Quiet Hours</Typography>
                  <Typography variant="body2" color="text.secondary">
                    Disable notifications during specified hours
                  </Typography>
                </Box>
              }
            />
          </Box>

          {notificationSettings.quietHoursEnabled && (
            <Grid container spacing={2}>
              <Grid item xs={6}>
                <TextField
                  fullWidth
                  label="Start Time"
                  type="time"
                  value={notificationSettings.quietHoursStart}
                  onChange={(e) => handleNotificationUpdate('quietHoursStart', e.target.value)}
                  InputLabelProps={{ shrink: true }}
                  size="small"
                />
              </Grid>
              <Grid item xs={6}>
                <TextField
                  fullWidth
                  label="End Time"
                  type="time"
                  value={notificationSettings.quietHoursEnd}
                  onChange={(e) => handleNotificationUpdate('quietHoursEnd', e.target.value)}
                  InputLabelProps={{ shrink: true }}
                  size="small"
                />
              </Grid>
            </Grid>
          )}
        </CardContent>
      </Card>
    </Grid>
  </Grid>
);

// ✅ FIXED: SecurityTab component moved OUTSIDE
const SecurityTab = ({ securitySettings, handleSecurityUpdate, calculateSecurityScore }) => {
  const securityScore = calculateSecurityScore();
  
  return (
    <Grid container spacing={3}>
      <Grid item xs={12}>
        <Card sx={{ 
          background: `linear-gradient(135deg, ${alpha('#1976d2', 0.08)} 0%, ${alpha('#1976d2', 0.03)} 100%)`,
          border: `1px solid ${alpha('#1976d2', 0.2)}`
        }}>
          <CardContent sx={{ p: 4 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <Shield sx={{ fontSize: 36, mr: 2, color: 'primary.main' }} />
                <Box>
                  <Typography variant="h5" fontWeight={700}>
                    Security Overview
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Monitor and manage your account security
                  </Typography>
                </Box>
              </Box>
              <Chip 
                label={`${securityScore}% Secure`}
                color={securityScore >= 80 ? 'success' : securityScore >= 60 ? 'warning' : 'error'}
                sx={{ fontWeight: 600, px: 2, py: 1, fontSize: '0.875rem' }}
              />
            </Box>
            
            <Box sx={{ mb: 2 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
                <Typography variant="body2" color="text.secondary">Security Score</Typography>
                <Typography variant="body2" fontWeight={600}>{securityScore}/100</Typography>
              </Box>
              <LinearProgress 
                variant="determinate" 
                value={securityScore} 
                sx={{ 
                  height: 8, 
                  borderRadius: 4,
                  bgcolor: alpha('#1976d2', 0.1),
                  '& .MuiLinearProgress-bar': {
                    borderRadius: 4,
                    bgcolor: securityScore >= 80 ? '#4caf50' : securityScore >= 60 ? '#ff9800' : '#f44336'
                  }
                }}
              />
            </Box>
            
            <Typography variant="body2" color="text.secondary">
              {securityScore >= 80 
                ? 'Excellent security posture. Your account is well protected.' 
                : securityScore >= 60 
                ? 'Good security. Consider enabling additional protection features.' 
                : 'Your account security could be improved. Enable recommended features below.'
              }
            </Typography>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} lg={6}>
        <Card>
          <CardContent sx={{ p: 4 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <VpnKey sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
              <Box>
                <Typography variant="h6" fontWeight={600}>
                  Authentication
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Secure your account with additional protection
                </Typography>
              </Box>
            </Box>
            
            <Stack spacing={3}>
              <Box>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Box>
                    <Typography variant="body1" fontWeight={500}>Two-Factor Authentication</Typography>
                    <Typography variant="body2" color="text.secondary">
                      Add extra security with 2FA
                    </Typography>
                    {!securitySettings.twoFactorAuthentication && (
                      <Chip label="Recommended" size="small" color="warning" variant="outlined" sx={{ mt: 1 }} />
                    )}
                  </Box>
                  <Switch
                    checked={securitySettings.twoFactorAuthentication}
                    onChange={(e) => handleSecurityUpdate('twoFactorAuthentication', e.target.checked)}
                    color="primary"
                  />
                </Box>
              </Box>
              
              <Divider />
              
              <Box>
                <Typography variant="body1" fontWeight={500} gutterBottom>
                  Session Timeout
                </Typography>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  Automatically sign out after inactivity
                </Typography>
                <FormControl fullWidth size="small">
                  <InputLabel>Timeout Duration</InputLabel>
                  <Select
                    value={securitySettings.sessionTimeout}
                    label="Timeout Duration"
                    onChange={(e) => handleSecurityUpdate('sessionTimeout', e.target.value)}
                  >
                    <MenuItem value={60}>1 hour</MenuItem>
                    <MenuItem value={240}>4 hours</MenuItem>
                    <MenuItem value={480}>8 hours</MenuItem>
                    <MenuItem value={1440}>24 hours</MenuItem>
                    <MenuItem value={0}>Never</MenuItem>
                  </Select>
                </FormControl>
              </Box>
            </Stack>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} lg={6}>
        <Card>
          <CardContent sx={{ p: 4 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <AdminPanelSettings sx={{ fontSize: 32, mr: 2, color: 'primary.main' }} />
              <Box>
                <Typography variant="h6" fontWeight={600}>
                  Security Monitoring
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Monitor and track security events
                </Typography>
              </Box>
            </Box>
            
            <Stack spacing={3}>
              <Box>
                <FormControlLabel
                  control={
                    <Switch
                      checked={securitySettings.loginNotifications}
                      onChange={(e) => handleSecurityUpdate('loginNotifications', e.target.checked)}
                      color="primary"
                    />
                  }
                  label={
                    <Box>
                      <Typography variant="body1" fontWeight={500}>Login Notifications</Typography>
                      <Typography variant="body2" color="text.secondary">
                        Get notified of new sign-ins
                      </Typography>
                    </Box>
                  }
                />
              </Box>
              
              <Box>
                <FormControlLabel
                  control={
                    <Switch
                      checked={securitySettings.suspiciousActivityAlerts}
                      onChange={(e) => handleSecurityUpdate('suspiciousActivityAlerts', e.target.checked)}
                      color="primary"
                    />
                  }
                  label={
                    <Box>
                      <Typography variant="body1" fontWeight={500}>Suspicious Activity Alerts</Typography>
                      <Typography variant="body2" color="text.secondary">
                        Alerts for unusual account activity
                      </Typography>
                    </Box>
                  }
                />
              </Box>
              
              <Box>
                <FormControlLabel
                  control={
                    <Switch
                      checked={securitySettings.auditLogging}
                      onChange={(e) => handleSecurityUpdate('auditLogging', e.target.checked)}
                      color="primary"
                    />
                  }
                  label={
                    <Box>
                      <Typography variant="body1" fontWeight={500}>Audit Logging</Typography>
                      <Typography variant="body2" color="text.secondary">
                        Log security events and access
                      </Typography>
                    </Box>
                  }
                />
              </Box>
            </Stack>
          </CardContent>
        </Card>
      </Grid>
    </Grid>
  );
};

const ProfessionalSettingsPage = () => {
  const navigate = useNavigate();
  const { user, updateProfile } = useAuth();
  const theme = useTheme();
  
  const [tabValue, setTabValue] = useState(0);
  const [loading, setLoading] = useState(false);
  const fileInputRef = useRef(null);
  const [isDragging, setIsDragging] = useState(false);
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: '',
    severity: 'success'
  });

  const getUserValue = (field, fallback = '') => {
    return user?.[field] ?? fallback;
  };

  const [userProfile, setUserProfile] = useState({
    firstName: '',
    lastName: '',
    email: '',
    phoneNumber: '',
    jobTitle: '',
    department: '',
    organization: '',
    location: '',
    timezone: '',
    profilePicture: '',
  });

useEffect(() => {
  if (user) {
    // ✅ Split full_name into firstName and lastName
    const fullName = getUserValue('full_name') || getUserValue('name') || '';
    const nameParts = fullName.split(' ');
    const firstName = nameParts[0] || '';
    const lastName = nameParts.slice(1).join(' ') || '';

    console.log('👤 User data in settings:', {
      full_name: fullName,
      firstName,
      lastName,
      email: user.email
    });

    setUserProfile({
      firstName: firstName,
      lastName: lastName,
      email: getUserValue('email') || '',
      phoneNumber: getUserValue('phone_number') || getUserValue('phoneNumber') || '',
      jobTitle: getUserValue('job_title') || getUserValue('jobTitle') || '',
      department: getUserValue('department') || '',
      organization: getUserValue('organization') || '',
      location: getUserValue('location') || getUserValue('address') || '',
      timezone: getUserValue('timezone') || 'UTC-5',
      profilePicture: getUserValue('profile_picture') || getUserValue('profilePicture') || '',
    });
  }
}, [user]);

  const [meetingSettings, setMeetingSettings] = useState({
    defaultCameraState: getUserValue('auto_join_video', true),
    defaultMicrophoneState: false,
    autoJoinAudio: getUserValue('auto_join_audio', true),
    videoQuality: 'HD',
    audioQuality: 'High',
    enableWaitingRoom: true,
    requireMeetingPassword: false,
    allowParticipantScreenShare: true,
    recordMeetingsByDefault: false,
    showParticipantNames: true,
  });

  const [notificationSettings, setNotificationSettings] = useState({
    emailNotifications: getUserValue('email_notifications', true),
    browserNotifications: true,
    mobileNotifications: true,
    meetingReminders: getUserValue('meeting_reminders', true),
    meetingInvitations: true,
    chatMessages: false,
    systemUpdates: true,
    securityAlerts: true,
    reminderTiming: 15,
    quietHoursEnabled: false,
    quietHoursStart: '22:00',
    quietHoursEnd: '08:00',
  });

  const [securitySettings, setSecuritySettings] = useState({
    twoFactorAuthentication: false,
    sessionTimeout: 480,
    loginNotifications: true,
    suspiciousActivityAlerts: true,
    deviceManagement: true,
    apiAccessEnabled: false,
    dataExportEnabled: true,
    auditLogging: true,
  });

  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  const handleProfileUpdate = useCallback((field, value) => {
    setUserProfile(prev => ({
      ...prev,
      [field]: value
    }));
  }, []);

  const handleMeetingSettingUpdate = useCallback((field, value) => {
    setMeetingSettings(prev => ({
      ...prev,
      [field]: value
    }));
  }, []);

  const handleNotificationUpdate = useCallback((field, value) => {
    setNotificationSettings(prev => ({
      ...prev,
      [field]: value
    }));
  }, []);

  const handleSecurityUpdate = useCallback((field, value) => {
    setSecuritySettings(prev => ({
      ...prev,
      [field]: value
    }));
  }, []);

  const handleProfilePictureChange = useCallback((file) => {
    if (file) {
      const validTypes = ['image/jpeg', 'image/jpg', 'image/png', 'image/gif', 'image/webp'];
      if (!validTypes.includes(file.type)) {
        setSnackbar({
          open: true,
          message: "Please select a valid image file (JPEG, PNG, GIF, or WebP)",
          severity: "error",
        });
        return;
      }

      const maxSize = 5 * 1024 * 1024;
      if (file.size > maxSize) {
        setSnackbar({
          open: true,
          message: "Image size should be less than 5MB",
          severity: "error",
        });
        return;
      }

      const imageUrl = URL.createObjectURL(file);
      handleProfileUpdate('profilePicture', imageUrl);
      
      setSnackbar({
        open: true,
        message: "Profile picture updated! Don't forget to save changes.",
        severity: "success",
      });
    }
  }, [handleProfileUpdate]);

  const triggerFileUpload = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    
    const files = e.dataTransfer.files;
    if (files && files[0]) {
      handleProfilePictureChange(files[0]);
    }
  };

const handleSaveSettings = async () => {
  setLoading(true);
  try {
    // ✅ Combine first_name and last_name into full_name
    const fullName = `${userProfile.firstName?.trim() || ''} ${userProfile.lastName?.trim() || ''}`.trim();
    
    if (!fullName) {
      setSnackbar({
        open: true,
        message: 'Please enter at least a first name',
        severity: 'error'
      });
      setLoading(false);
      return;
    }

    console.log('💾 Saving settings with full_name:', fullName);

    const settingsData = {
      full_name: fullName,  // ✅ Send as full_name
      email: userProfile.email,
      phone_number: userProfile.phoneNumber || null,
      address: userProfile.location || null,  // Using location as address
      country: null,  // You don't have country in settings page
      
      // ✅ Convert languages to comma-separated string (if you add it later)
      languages: 'English',  // Default for now
      
      // Meeting settings
      auto_join_video: meetingSettings.defaultCameraState,
      auto_join_audio: meetingSettings.autoJoinAudio,
      
      // Notification settings
      email_notifications: notificationSettings.emailNotifications,
      meeting_reminders: notificationSettings.meetingReminders,
      recording_notifications: meetingSettings.recordMeetingsByDefault,
    };

    console.log('📤 Sending settings data:', settingsData);

    const result = await updateProfile(settingsData);
    
    if (result?.ok) {
      setSnackbar({
        open: true,
        message: 'Settings saved successfully!',
        severity: 'success'
      });
    } else {
      throw new Error(result?.error || 'Update failed');
    }
  } catch (error) {
    console.error('❌ Settings update error:', error);
    setSnackbar({
      open: true,
      message: error.message || 'Failed to save settings. Please try again.',
      severity: 'error'
    });
  } finally {
    setLoading(false);
  }
};

  const calculateSecurityScore = () => {
    let score = 0;
    const weights = {
      twoFactorAuthentication: 30,
      sessionTimeout: securitySettings.sessionTimeout <= 240 ? 20 : 10,
      loginNotifications: 15,
      suspiciousActivityAlerts: 15,
      auditLogging: 20,
    };
    
    Object.entries(weights).forEach(([key, weight]) => {
      if (securitySettings[key] === true || (key === 'sessionTimeout' && securitySettings[key] <= 240)) {
        score += weight;
      }
    });
    
    return Math.min(score, 100);
  };

  return (
    <ThemeProvider theme={professionalTheme}>
      <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
        <Container maxWidth="lg" sx={{ py: 4 }}>
          <Paper 
            elevation={0} 
            sx={{ 
              p: 4, 
              mb: 4, 
              background: 'linear-gradient(135deg, #1976d2 0%, #1565c0 100%)',
              color: 'white',
              borderRadius: 3,
              position: 'relative',
              overflow: 'hidden',
              '&::before': {
                content: '""',
                position: 'absolute',
                top: 0,
                left: 0,
                right: 0,
                bottom: 0,
                background: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="0.05"%3E%3Cpath d="M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
              }
            }}
          >
            <Box sx={{ position: 'relative', zIndex: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <Tooltip title="Back to Dashboard">
                  <IconButton 
                    sx={{ 
                      mr: 2, 
                      color: 'white',
                      bgcolor: alpha('#fff', 0.15),
                      '&:hover': { 
                        bgcolor: alpha('#fff', 0.25),
                        transform: 'translateX(-2px)'
                      },
                      transition: 'all 0.2s ease'
                    }}
                    onClick={() => navigate('/dashboard')}
                  >
                    <ArrowBackIcon />
                  </IconButton>
                </Tooltip>
                <SettingsIcon sx={{ fontSize: 40, mr: 2 }} />
                <Box>
                  <Typography variant="h4" fontWeight={700} sx={{ mb: 0.5 }}>
                    Account Settings
                  </Typography>
                  <Breadcrumbs 
                    sx={{ 
                      color: alpha('#fff', 0.8),
                      '& .MuiLink-root': { 
                        color: alpha('#fff', 0.8),
                        textDecoration: 'none',
                        '&:hover': { 
                          color: 'white',
                          cursor: 'pointer'
                        }
                      }
                    }}
                  >
                    <Link 
                      color="inherit" 
                      onClick={() => navigate('/dashboard')} 
                      sx={{ display: 'flex', alignItems: 'center' }}
                    >
                      <Home sx={{ mr: 0.5, fontSize: 18 }} />
                      Dashboard
                    </Link>
                    <Typography color="white" sx={{ fontWeight: 600 }}>Settings</Typography>
                  </Breadcrumbs>
                </Box>
              </Box>
              <Typography variant="body1" sx={{ opacity: 0.9, fontSize: '1.1rem' }}>
                Manage your profile, preferences, and security settings
              </Typography>
            </Box>
          </Paper>

          <Paper elevation={0} sx={{ borderRadius: 3, overflow: 'hidden', border: '1px solid #e1e5e9' }}>
            <Tabs
              value={tabValue}
              onChange={handleTabChange}
              variant="scrollable"
              scrollButtons="auto"
              sx={{ 
                borderBottom: 1, 
                borderColor: 'divider',
                bgcolor: alpha('#1976d2', 0.02),
                '& .MuiTab-root': {
                  fontWeight: 600,
                  py: 3,
                  minHeight: 72,
                  '&.Mui-selected': {
                    color: 'primary.main',
                    bgcolor: alpha('#1976d2', 0.08)
                  },
                  '&:hover': {
                    bgcolor: alpha('#1976d2', 0.04)
                  }
                }
              }}
            >
              <Tab icon={<Person sx={{ fontSize: 24 }} />} label="Profile" iconPosition="start" />
              <Tab icon={<VideoCall sx={{ fontSize: 24 }} />} label="Meetings" iconPosition="start" />
              <Tab icon={<Notifications sx={{ fontSize: 24 }} />} label="Notifications" iconPosition="start" />
              <Tab icon={<Security sx={{ fontSize: 24 }} />} label="Security" iconPosition="start" />
            </Tabs>

            <Box sx={{ p: 4 }}>
              <TabPanel value={tabValue} index={0}>
                <ProfileTab 
                  userProfile={userProfile}
                  isDragging={isDragging}
                  handleProfileUpdate={handleProfileUpdate}
                  handleDragOver={handleDragOver}
                  handleDragLeave={handleDragLeave}
                  handleDrop={handleDrop}
                  triggerFileUpload={triggerFileUpload}
                  fileInputRef={fileInputRef}
                  handleProfilePictureChange={handleProfilePictureChange}
                  theme={theme}
                />
              </TabPanel>
              <TabPanel value={tabValue} index={1}>
                <MeetingTab 
                  meetingSettings={meetingSettings}
                  handleMeetingSettingUpdate={handleMeetingSettingUpdate}
                />
              </TabPanel>
              <TabPanel value={tabValue} index={2}>
                <NotificationTab 
                  notificationSettings={notificationSettings}
                  handleNotificationUpdate={handleNotificationUpdate}
                />
              </TabPanel>
              <TabPanel value={tabValue} index={3}>
                <SecurityTab 
                  securitySettings={securitySettings}
                  handleSecurityUpdate={handleSecurityUpdate}
                  calculateSecurityScore={calculateSecurityScore}
                />
              </TabPanel>
            </Box>

            <Box 
              sx={{ 
                p: 4, 
                borderTop: 1, 
                borderColor: 'divider',
                bgcolor: alpha('#f8fafc', 0.8),
                display: 'flex', 
                justifyContent: 'space-between',
                alignItems: 'center'
              }}
            >
              <Button 
                variant="outlined" 
                sx={{ 
                  fontWeight: 600,
                  borderColor: alpha('#1976d2', 0.3),
                  '&:hover': {
                    borderColor: 'primary.main',
                    bgcolor: alpha('#1976d2', 0.04)
                  }
                }}
              >
                Reset to Defaults
              </Button>
              <Box sx={{ display: 'flex', gap: 2 }}>
                <Button 
                  variant="outlined" 
                  sx={{ 
                    fontWeight: 600,
                    borderColor: alpha('#1976d2', 0.3),
                    '&:hover': {
                      borderColor: 'primary.main',
                      bgcolor: alpha('#1976d2', 0.04)
                    }
                  }}
                >
                  Cancel
                </Button>
                <Button 
                  variant="contained" 
                  startIcon={loading ? <CircularProgress size={18} color="inherit" /> : <Save />}
                  onClick={handleSaveSettings}
                  disabled={loading}
                  sx={{ 
                    fontWeight: 700,
                    minWidth: 160,
                    py: 1.5,
                    px: 3,
                    boxShadow: '0 4px 12px rgba(25, 118, 210, 0.3)',
                    '&:hover': {
                      boxShadow: '0 6px 20px rgba(25, 118, 210, 0.4)',
                      transform: 'translateY(-1px)'
                    },
                    '&:disabled': {
                      bgcolor: alpha('#1976d2', 0.3)
                    }
                  }}
                >
                  {loading ? 'Saving Changes...' : 'Save Changes'}
                </Button>
              </Box>
            </Box>
          </Paper>

          <Snackbar
            open={snackbar.open}
            autoHideDuration={6000}
            onClose={() => setSnackbar({ ...snackbar, open: false })}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
          >
            <Alert
              onClose={() => setSnackbar({ ...snackbar, open: false })}
              severity={snackbar.severity}
              variant="filled"
              sx={{ 
                width: "100%",
                borderRadius: 2,
                boxShadow: theme.shadows[12],
                fontWeight: 600
              }}
            >
              {snackbar.message}
            </Alert>
          </Snackbar>
        </Container>
      </Box>
    </ThemeProvider>
  );
};

export default ProfessionalSettingsPage;