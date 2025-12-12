

import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Grid,
  IconButton,
  Chip,
  Divider,
  useTheme,
  alpha,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  InputAdornment,
  Alert,
  Paper,
  Stack
} from '@mui/material';
import {
  VideoCall,
  Schedule,
  CalendarMonth,
  Launch,
  ContentCopy,
  Settings,
  PersonAdd,
  AccessTime,
  Groups,
  Close,
  Link as LinkIcon,
  Tag,
  ArrowForward,
  CheckCircle
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

const MeetingOptions = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  
  // Modal state
  const [joinModalOpen, setJoinModalOpen] = useState(false);
  const [meetingInput, setMeetingInput] = useState('');
  const [inputError, setInputError] = useState('');

  const meetingTypes = [
    {
      id: 'instant',
      title: 'Instant Meeting',
      description: 'Start a meeting right now',
      icon: <VideoCall sx={{ fontSize: 28 }} />,
      color: '#4CAF50',
      features: ['Start immediately', 'Share link', 'No scheduling'],
      action: 'Start Now',
      route: '/meeting/instant'
    },
    {
      id: 'schedule',
      title: 'Schedule Meeting',
      description: 'Plan a meeting for later',
      icon: <Schedule sx={{ fontSize: 28 }} />,
      color: '#2196F3',
      features: ['Set date & time', 'Send invitations', 'Email reminders'],
      action: 'Schedule',
      route: '/schedule'
    },
    {
      id: 'calendar',
      title: 'Calendar Meeting',
      description: 'Integrate with your calendar',
      icon: <CalendarMonth sx={{ fontSize: 28 }} />,
      color: '#FF9800',
      features: ['Calendar sync', 'Auto-invites', 'Availability check'],
      action: 'Open Calendar',
      route: '/calendar'
    }
  ];

  const quickActions = [
    {
      title: 'Join Meeting',
      description: 'Enter meeting ID or link',
      icon: <Launch />,
      color: '#2196F3',
      action: () => setJoinModalOpen(true)
    },
    {
      title: 'Meeting Settings',
      description: 'Configure default settings',
      icon: <Settings />,
      color: '#607D8B',
      action: () => navigate('/settings')
    }
  ];

  // Handle button click with event propagation stop
  const handleButtonClick = (route, event) => {
    event.stopPropagation();
    event.preventDefault();
    console.log('Button clicked, navigating to:', route);
    navigate(route);
  };

  // Handle join meeting modal
  const handleJoinMeeting = () => {
    if (!meetingInput.trim()) {
      setInputError('Please enter a meeting ID or link');
      return;
    }

    setInputError('');

    let meetingId = meetingInput.trim();
    
    if (meetingInput.includes('https') || meetingInput.includes('meeting/')) {
      const urlParts = meetingInput.split('/');
      const lastPart = urlParts[urlParts.length - 1];
      meetingId = lastPart.split('?')[0];
    }

    if (meetingId.length < 3) {
      setInputError('Meeting ID must be at least 3 characters long');
      return;
    }

    setJoinModalOpen(false);
    setMeetingInput('');
    navigate(`/meeting/${meetingId}`);
  };

  const handleCloseModal = () => {
    setJoinModalOpen(false);
    setMeetingInput('');
    setInputError('');
  };

  const isUrl = meetingInput.includes('https') || meetingInput.includes('meeting/');

  return (
    <Box>
      {/* Primary Meeting Options */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        {meetingTypes.map((type) => (
          <Grid item xs={12} md={4} key={type.id}>
            <Paper
              elevation={0}
              sx={{
                height: '100%',
                cursor: 'pointer',
                transition: 'all 0.2s ease-in-out',
                border: '1px solid',
                borderColor: theme.palette.divider,
                borderRadius: 2,
                backgroundColor: 'white',
                '&:hover': {
                  borderColor: type.color,
                  transform: 'translateY(-4px)',
                  boxShadow: `0 12px 24px ${type.color}15`,
                  '& .meeting-icon': {
                    backgroundColor: type.color,
                    color: 'white',
                    transform: 'scale(1.1)',
                  },
                  '& .action-button': {
                    backgroundColor: type.color,
                    color: 'white',
                  }
                }
              }}
              onClick={() => {
                console.log('Card clicked, navigating to:', type.route);
                navigate(type.route);
              }}
            >
              <CardContent sx={{ p: 3, height: '100%', display: 'flex', flexDirection: 'column' }}>
                {/* Icon */}
                <Box
                  className="meeting-icon"
                  sx={{
                    width: 56,
                    height: 56,
                    borderRadius: 2,
                    backgroundColor: `${type.color}15`,
                    color: type.color,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    transition: 'all 0.2s ease-in-out',
                    mb: 3,
                    alignSelf: 'flex-start'
                  }}
                >
                  {type.icon}
                </Box>

                {/* Content */}
                <Box sx={{ flex: 1 }}>
                  <Typography 
                    variant="h6" 
                    sx={{ 
                      fontWeight: 600, 
                      mb: 1,
                      color: theme.palette.text.primary
                    }}
                  >
                    {type.title}
                  </Typography>
                  <Typography 
                    variant="body2" 
                    color="text.secondary" 
                    sx={{ mb: 3, lineHeight: 1.5 }}
                  >
                    {type.description}
                  </Typography>

                  {/* Features */}
                  <Stack direction="row" spacing={1} flexWrap="wrap" sx={{ mb: 3 }}>
                    {type.features.map((feature, index) => (
                      <Chip
                        key={index}
                        label={feature}
                        size="small"
                        sx={{
                          backgroundColor: theme.palette.grey[100],
                          color: theme.palette.text.secondary,
                          fontWeight: 500,
                          fontSize: '0.75rem',
                          border: 'none',
                          mb: 0.5
                        }}
                      />
                    ))}
                  </Stack>
                </Box>

                {/* Action Button */}
                <Button
                  className="action-button"
                  variant="outlined"
                  fullWidth
                  endIcon={<ArrowForward sx={{ fontSize: 16 }} />}
                  onClick={(event) => handleButtonClick(type.route, event)}
                  sx={{
                    borderColor: theme.palette.divider,
                    fontWeight: 500,
                      backgroundColor: type.color,
                      color: 'white',
                    py: 1.2,
                    textTransform: 'none',
                    transition: 'all 0.2s ease-in-out',
                    '&:hover': {
                      borderColor: type.color,
                      backgroundColor: type.color,
                     
                    }
                  }}
                >
                  {type.action}
                </Button>
              </CardContent>
            </Paper>
          </Grid>
        ))}
      </Grid>

      {/* Quick Actions */}
      <Box sx={{ mb: 4 }}>
        <Typography 
          variant="h6" 
          sx={{ 
            fontWeight: 600, 
            mb: 2,
            color: theme.palette.text.primary
          }}
        >
          Quick Actions
        </Typography>
        <Grid container spacing={2}>
          {quickActions.map((action, index) => (
            <Grid item xs={12} sm={6} key={index}>
              <Paper
                elevation={0}
                sx={{
                  cursor: 'pointer',
                  transition: 'all 0.2s ease-in-out',
                  border: '1px solid',
                  borderColor: theme.palette.divider,
                  borderRadius: 2,
                  '&:hover': {
                    borderColor: action.color,
                    backgroundColor: `${action.color}05`,
                    transform: 'translateY(-2px)',
                    boxShadow: `0 8px 16px ${action.color}15`,
                  }
                }}
                onClick={action.action}
              >
                <CardContent sx={{ p: 2.5 }}>
                  <Stack direction="row" spacing={2} alignItems="center">
                    <Box
                      sx={{
                        width: 40,
                        height: 40,
                        borderRadius: 1.5,
                        backgroundColor: `${action.color}15`,
                        color: action.color,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center'
                      }}
                    >
                      {action.icon}
                    </Box>
                    <Box sx={{ flex: 1 }}>
                      <Typography 
                        variant="subtitle1" 
                        sx={{ 
                          fontWeight: 600,
                          color: theme.palette.text.primary
                        }}
                      >
                        {action.title}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        {action.description}
                      </Typography>
                    </Box>
                    <ArrowForward sx={{ color: theme.palette.text.secondary, fontSize: 20 }} />
                  </Stack>
                </CardContent>
              </Paper>
            </Grid>
          ))}
        </Grid>
      </Box>

      {/* Platform Stats */}
      <Paper
        elevation={0}
        sx={{
          p: 3,
          backgroundColor: theme.palette.grey[50],
          border: '1px solid',
          borderColor: theme.palette.divider,
          borderRadius: 2
        }}
      >
        <Grid container spacing={3} alignItems="center">
          <Grid item xs={12} md={8}>
            <Typography 
              variant="h6" 
              sx={{ 
                fontWeight: 600, 
                mb: 1,
                color: theme.palette.text.primary
              }}
            >
              Trusted Platform
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Join millions of users who rely on our platform for seamless video meetings
            </Typography>
          </Grid>
          <Grid item xs={12} md={4}>
            <Stack direction="row" spacing={4} justifyContent={{ xs: 'flex-start', md: 'flex-end' }}>
              <Box sx={{ textAlign: 'center' }}>
                <Typography 
                  variant="h5" 
                  sx={{ 
                    fontWeight: 700,
                    color: theme.palette.success.main
                  }}
                >
                  99.9%
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Uptime
                </Typography>
              </Box>
              <Box sx={{ textAlign: 'center' }}>
                <Stack direction="row" alignItems="center" justifyContent="center" spacing={0.5}>
                  <Groups sx={{ fontSize: 20, color: theme.palette.primary.main }} />
                  <Typography 
                    variant="h5" 
                    sx={{ 
                      fontWeight: 700,
                      color: theme.palette.primary.main
                    }}
                  >
                    50+
                  </Typography>
                </Stack>
                <Typography variant="caption" color="text.secondary">
                  Participants
                </Typography>
              </Box>
            </Stack>
          </Grid>
        </Grid>
      </Paper>

      {/* Join Meeting Modal */}
      <Dialog 
        open={joinModalOpen} 
        onClose={handleCloseModal}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 2,
            boxShadow: '0 20px 40px rgba(0,0,0,0.1)'
          }
        }}
        slotProps={{
          backdrop: {
            sx: {
              backgroundColor: 'rgba(0, 0, 0, 0.5)',
              backdropFilter: 'blur(8px)',
              WebkitBackdropFilter: 'blur(8px)',
            }
          }
        }}
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          pb: 1,
          borderBottom: '1px solid',
          borderColor: 'divider'
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Box
              sx={{
                width: 40,
                height: 40,
                borderRadius: 1,
                backgroundColor: `${theme.palette.primary.main}15`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            >
              <Launch sx={{ color: theme.palette.primary.main, fontSize: 20 }} />
            </Box>
            <Typography variant="h6" fontWeight={600}>
              Join Meeting
            </Typography>
          </Box>
          <IconButton 
            onClick={handleCloseModal} 
            size="small"
            sx={{ 
              color: theme.palette.text.secondary,
              '&:hover': { backgroundColor: theme.palette.action.hover }
            }}
          >
            <Close />
          </IconButton>
        </DialogTitle>

        <DialogContent sx={{ pt: 3 }}>
          <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
            Enter a meeting ID or paste a meeting link to join the conversation
          </Typography>

          <TextField
            fullWidth
            autoFocus
            label="Meeting ID or Link"
            placeholder="e.g., 123-456-789 or https://example.com/meeting/123-456-789"
            value={meetingInput}
            onChange={(e) => {
              setMeetingInput(e.target.value);
              if (inputError) setInputError('');
            }}
            error={!!inputError}
            helperText={inputError}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  {isUrl ? <LinkIcon sx={{ color: theme.palette.primary.main }} /> : <Tag sx={{ color: theme.palette.primary.main }} />}
                </InputAdornment>
              ),
            }}
            sx={{ 
              mb: 2,
              '& .MuiOutlinedInput-root': {
                '&:hover fieldset': {
                  borderColor: theme.palette.primary.main,
                },
                '&.Mui-focused fieldset': {
                  borderColor: theme.palette.primary.main,
                }
              },
              '& .MuiInputLabel-root.Mui-focused': {
                color: theme.palette.primary.main,
              }
            }}
            onKeyPress={(e) => {
              if (e.key === 'Enter') {
                handleJoinMeeting();
              }
            }}
          />

          {meetingInput && !inputError && (
            <Alert 
              severity="success" 
              variant="outlined"
              icon={<CheckCircle />}
              sx={{ 
                backgroundColor: `${theme.palette.success.main}08`,
                borderColor: `${theme.palette.success.main}30`,
                color: theme.palette.success.main,
                '& .MuiAlert-icon': {
                  color: theme.palette.success.main
                },
                borderRadius: 1,
                mb: 1
              }}
            >
              {isUrl ? 'Meeting link detected - Ready to join!' : 'Meeting ID entered - Ready to join!'}
            </Alert>
          )}
        </DialogContent>

        <DialogActions sx={{ 
          p: 3, 
          pt: 1, 
          gap: 1,
          borderTop: '1px solid',
          borderColor: 'divider'
        }}>
          <Button 
            onClick={handleCloseModal}
            variant="outlined"
            sx={{ 
              textTransform: 'none',
              fontWeight: 500,
              px: 3,
              borderColor: theme.palette.divider,
              color: theme.palette.text.secondary,
              '&:hover': {
                backgroundColor: theme.palette.action.hover,
                borderColor: theme.palette.text.secondary
              }
            }}
          >
            Cancel
          </Button>
          <Button
            variant="contained"
            onClick={handleJoinMeeting}
            disabled={!meetingInput.trim()}
            sx={{
              textTransform: 'none',
              fontWeight: 600,
              px: 4,
              backgroundColor: theme.palette.primary.main,
              '&:hover': {
                backgroundColor: theme.palette.primary.dark,
              },
              '&:disabled': {
                backgroundColor: theme.palette.action.disabledBackground,
              }
            }}
          >
            Join Meeting
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default MeetingOptions;