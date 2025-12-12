import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Button,
  Chip,
  IconButton,
  Grid,
  Card,
  CardContent,
  Avatar,
  Divider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Switch,
  FormControlLabel,
  useTheme,
  alpha,
  Alert,
  CircularProgress
} from '@mui/material';
import {
  Add,
  Delete,
  PersonAdd,
  Email,
  Link,
  Send,
  ContentCopy,
  Group,
  CheckCircle,
  AccessTime,
  Warning,
  GroupAdd,
  CloudUpload
} from '@mui/icons-material';
import { format } from 'date-fns';
import BulkInvite from './BulkInvite';

const InviteParticipants = ({ meetingId, meetingData, onInvitesSent }) => {
  const theme = useTheme();
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteeName, setInviteeName] = useState('');
  const [inviteList, setInviteList] = useState([]);
  const [inviteMessage, setInviteMessage] = useState('');
  const [reminderTime, setReminderTime] = useState(15);
  const [sendCalendarInvite, setSendCalendarInvite] = useState(true);
  const [loading, setLoading] = useState(false);
  const [emailError, setEmailError] = useState('');
  const [nameError, setNameError] = useState('');
  const [bulkInviteOpen, setBulkInviteOpen] = useState(false);

  // Email validation
  const validateEmail = (email) => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  };

  // Based on Meeting_Invitations table structure from backend
  const handleAddInvite = () => {
    // Reset errors
    setEmailError('');
    setNameError('');

    // Validation
    if (!inviteeName.trim()) {
      setNameError('Full name is required');
      return;
    }
    if (!inviteEmail.trim()) {
      setEmailError('Email address is required');
      return;
    }
    if (!validateEmail(inviteEmail)) {
      setEmailError('Please enter a valid email address');
      return;
    }

    // Check if email already exists
    if (inviteList.some(invite => invite.email.toLowerCase() === inviteEmail.toLowerCase())) {
      setEmailError('This email is already in the invite list');
      return;
    }

    const newInvite = {
      id: Date.now(),
      meetingId: meetingId,
      email: inviteEmail.trim(),
      fullName: inviteeName.trim(),
      invitationStatus: 'pending', // 'sent', 'delivered', 'opened', 'bounced'
      rsvpStatus: 'pending', // 'accepted', 'declined', 'maybe'
      inviteToken: `token_${Date.now()}`,
      sentAt: null,
      openedAt: null,
      respondedAt: null,
      createdAt: new Date()
    };
    
    setInviteList([...inviteList, newInvite]);
    setInviteEmail('');
    setInviteeName('');
  };

  const handleRemoveInvite = (id) => {
    setInviteList(inviteList.filter(invite => invite.id !== id));
  };

  const handleSendInvites = async () => {
    if (inviteList.length === 0) return;

    setLoading(true);
    try {
      // API call to send invitations matching backend Meeting_Invitations table
      const inviteData = {
        meetingId,
        invitations: inviteList.map(invite => ({
          meetingId: invite.meetingId,
          userId: null, // Will be set by backend if user exists
          email: invite.email,
          fullName: invite.fullName,
          invitationStatus: 'sent',
          rsvpStatus: 'pending',
          inviteToken: invite.inviteToken,
          customMessage: inviteMessage,
          reminderMinutes: reminderTime,
          sendCalendarInvite,
          sentAt: new Date(),
          createdAt: new Date()
        }))
      };
      
      // Backend API call - POST /api/meeting-invitations/send
      // const response = await fetch('/api/meeting-invitations/send', {
      //   method: 'POST',
      //   headers: { 'Content-Type': 'application/json' },
      //   body: JSON.stringify(inviteData)
      // });
      
      // Update invitation status to 'sent'
      const updatedInvites = inviteList.map(invite => ({
        ...invite,
        invitationStatus: 'sent',
        sentAt: new Date()
      }));
      
      if (onInvitesSent) {
        onInvitesSent(updatedInvites);
      }

      // Clear the form
      setInviteList([]);
      setInviteMessage('');
      
    } catch (error) {
      console.error('Error sending invites:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e, field) => {
    if (e.key === 'Enter') {
      if (field === 'email' && inviteEmail && inviteeName) {
        handleAddInvite();
      }
    }
  };

  const handleBulkInvitesSent = (bulkEmails) => {
    // Convert bulk emails to invite format
    const bulkInvites = bulkEmails.map(email => ({
      id: Date.now() + Math.random(),
      meetingId: meetingId,
      email: email,
      fullName: email.split('@')[0], // Use email prefix as temporary name
      invitationStatus: 'sent',
      rsvpStatus: 'pending',
      inviteToken: `token_${Date.now()}_${Math.random()}`,
      sentAt: new Date(),
      openedAt: null,
      respondedAt: null,
      createdAt: new Date()
    }));

    if (onInvitesSent) {
      onInvitesSent(bulkInvites);
    }
  };

  return (
    <>
      <Paper 
        elevation={4} 
        sx={{ 
          p: 4, 
          borderRadius: 4,
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.02)}, ${alpha(theme.palette.secondary.main, 0.02)})`,
          border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}`
        }}
      >
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Avatar sx={{ 
              mr: 3, 
              bgcolor: theme.palette.primary.main,
              width: 56,
              height: 56
            }}>
              <Group sx={{ fontSize: 28 }} />
            </Avatar>
            <Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.primary.main }}>
                Invite Participants
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Add participants to join your meeting
              </Typography>
            </Box>
          </Box>
          
          {/* Bulk Invite Button */}
          <Button
            variant="outlined"
            startIcon={<GroupAdd />}
            onClick={() => setBulkInviteOpen(true)}
            sx={{
              borderColor: theme.palette.secondary.main,
              color: theme.palette.secondary.main,
              '&:hover': {
                borderColor: theme.palette.secondary.dark,
                backgroundColor: alpha(theme.palette.secondary.main, 0.1)
              }
            }}
          >
            Bulk Invite
          </Button>
        </Box>

        {/* Meeting Info */}
        {meetingData && (
          <Card sx={{ 
            mb: 4, 
            border: `1px solid ${alpha(theme.palette.info.main, 0.2)}`,
            bgcolor: alpha(theme.palette.info.main, 0.05)
          }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2, fontWeight: 'bold' }}>
                Meeting: {meetingData.title || meetingData.meetingName}
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <AccessTime sx={{ mr: 1, color: theme.palette.primary.main }} />
                    <Typography variant="body2">
                      {meetingData.startTime ? format(new Date(meetingData.startTime), 'PPp') : 'Instant Meeting'}
                    </Typography>
                  </Box>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <PersonAdd sx={{ mr: 1, color: theme.palette.primary.main }} />
                    <Typography variant="body2">
                      Host: {meetingData.organizer || 'You'}
                    </Typography>
                  </Box>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        )}

        {/* Add Participant Form */}
        <Card sx={{ 
          mb: 4, 
          border: `2px solid ${theme.palette.primary.main}`,
          borderRadius: 3,
          overflow: 'hidden'
        }}>
          <Box sx={{ 
            bgcolor: theme.palette.primary.main, 
            color: 'white', 
            p: 2 
          }}>
            <Typography variant="h6" sx={{ fontWeight: 'bold' }}>
              Add New Participant
            </Typography>
          </Box>
          <CardContent sx={{ p: 3 }}>
            <Grid container spacing={3}>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Full Name *"
                  value={inviteeName}
                  onChange={(e) => {
                    setInviteeName(e.target.value);
                    setNameError('');
                  }}
                  onKeyPress={(e) => handleKeyPress(e, 'name')}
                  variant="outlined"
                  error={!!nameError}
                  helperText={nameError}
                  InputProps={{
                    startAdornment: <PersonAdd sx={{ mr: 1, color: theme.palette.primary.main }} />
                  }}
                  sx={{
                    '& .MuiOutlinedInput-root': {
                      '&:hover fieldset': {
                        borderColor: theme.palette.primary.main,
                      }
                    }
                  }}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Email Address *"
                  type="email"
                  value={inviteEmail}
                  onChange={(e) => {
                    setInviteEmail(e.target.value);
                    setEmailError('');
                  }}
                  onKeyPress={(e) => handleKeyPress(e, 'email')}
                  variant="outlined"
                  error={!!emailError}
                  helperText={emailError}
                  InputProps={{
                    startAdornment: <Email sx={{ mr: 1, color: theme.palette.primary.main }} />
                  }}
                  sx={{
                    '& .MuiOutlinedInput-root': {
                      '&:hover fieldset': {
                        borderColor: theme.palette.primary.main,
                      }
                    }
                  }}
                />
              </Grid>
              <Grid item xs={12} md={2}>
                <Button
                  fullWidth
                  variant="contained"
                  onClick={handleAddInvite}
                  disabled={!inviteEmail || !inviteeName}
                  sx={{ 
                    height: 56,
                    borderRadius: 2,
                    fontWeight: 'bold',
                    '&:hover': {
                      transform: 'translateY(-2px)',
                      boxShadow: theme.shadows[8]
                    },
                    transition: 'all 0.3s ease'
                  }}
                  startIcon={<Add />}
                >
                  Add
                </Button>
              </Grid>
            </Grid>
          </CardContent>
        </Card>

        {/* Invite List */}
        {inviteList.length > 0 && (
          <Box sx={{ mb: 4 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
              <Typography variant="h6" sx={{ fontWeight: 'bold', color: theme.palette.primary.main }}>
                Participants to Invite
              </Typography>
              <Chip 
                label={inviteList.length} 
                color="primary" 
                size="small" 
                sx={{ ml: 2 }}
              />
            </Box>
            <Grid container spacing={3}>
              {inviteList.map((invite) => (
                <Grid item xs={12} md={6} lg={4} key={invite.id}>
                  <Card sx={{ 
                    border: `1px solid ${theme.palette.divider}`,
                    borderRadius: 3,
                    '&:hover': { 
                      boxShadow: theme.shadows[8],
                      transform: 'translateY(-4px)',
                      borderColor: theme.palette.primary.main
                    },
                    transition: 'all 0.3s ease-in-out'
                  }}>
                    <CardContent>
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
                        <Avatar sx={{ 
                          bgcolor: theme.palette.secondary.main,
                          width: 48,
                          height: 48,
                          fontSize: '1.2rem',
                          fontWeight: 'bold'
                        }}>
                          {invite.fullName.charAt(0).toUpperCase()}
                        </Avatar>
                        <IconButton 
                          onClick={() => handleRemoveInvite(invite.id)}
                          sx={{ 
                            color: theme.palette.error.main,
                            '&:hover': {
                              bgcolor: alpha(theme.palette.error.main, 0.1),
                              transform: 'scale(1.1)'
                            }
                          }}
                          size="small"
                        >
                          <Delete />
                        </IconButton>
                      </Box>
                      <Typography variant="subtitle1" sx={{ fontWeight: 'bold', mb: 1 }}>
                        {invite.fullName}
                      </Typography>
                      <Typography 
                        variant="body2" 
                        color="text.secondary"
                        sx={{ 
                          wordBreak: 'break-word',
                          fontSize: '0.85rem'
                        }}
                      >
                        {invite.email}
                      </Typography>
                      <Chip 
                        label={invite.rsvpStatus} 
                        size="small" 
                        color="default"
                        sx={{ mt: 1 }}
                      />
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          </Box>
        )}

        {/* Invitation Settings */}
        <Card sx={{ 
          mb: 4, 
          border: `1px solid ${alpha(theme.palette.secondary.main, 0.3)}`,
          borderRadius: 3
        }}>
          <Box sx={{ 
            bgcolor: alpha(theme.palette.secondary.main, 0.1), 
            p: 2,
            borderBottom: `1px solid ${alpha(theme.palette.secondary.main, 0.2)}`
          }}>
            <Typography variant="h6" sx={{ fontWeight: 'bold', color: theme.palette.secondary.main }}>
              Invitation Settings
            </Typography>
          </Box>
          <CardContent sx={{ p: 3 }}>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  label="Custom Message (Optional)"
                  value={inviteMessage}
                  onChange={(e) => setInviteMessage(e.target.value)}
                  placeholder="Add a personal message to your invitation..."
                  variant="outlined"
                  sx={{
                    '& .MuiOutlinedInput-root': {
                      borderRadius: 2
                    }
                  }}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Reminder Time</InputLabel>
                  <Select
                    value={reminderTime}
                    onChange={(e) => setReminderTime(e.target.value)}
                    label="Reminder Time"
                    sx={{ borderRadius: 2 }}
                  >
                    <MenuItem value={5}>5 minutes before</MenuItem>
                    <MenuItem value={10}>10 minutes before</MenuItem>
                    <MenuItem value={15}>15 minutes before</MenuItem>
                    <MenuItem value={30}>30 minutes before</MenuItem>
                    <MenuItem value={60}>1 hour before</MenuItem>
                    <MenuItem value={120}>2 hours before</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={6}>
                <Box sx={{ pt: 2 }}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={sendCalendarInvite}
                        onChange={(e) => setSendCalendarInvite(e.target.checked)}
                        color="primary"
                      />
                    }
                    label="Send Calendar Invite"
                    sx={{ 
                      '& .MuiFormControlLabel-label': {
                        fontWeight: 'bold'
                      }
                    }}
                  />
                </Box>
              </Grid>
            </Grid>
          </CardContent>
        </Card>

        {/* Send Invites Button */}
        <Box sx={{ textAlign: 'center' }}>
          {inviteList.length > 0 ? (
            <Button
              variant="contained"
              size="large"
              onClick={handleSendInvites}
              disabled={loading}
              startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <Send />}
              sx={{
                px: 6,
                py: 2,
                borderRadius: 4,
                fontWeight: 'bold',
                fontSize: '1.1rem',
                background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
                boxShadow: theme.shadows[6],
                '&:hover': {
                  background: `linear-gradient(135deg, ${theme.palette.primary.dark}, ${theme.palette.primary.main})`,
                  transform: 'translateY(-3px)',
                  boxShadow: theme.shadows[12]
                },
                '&:disabled': {
                  background: theme.palette.grey[400]
                },
                transition: 'all 0.3s ease-in-out'
              }}
            >
              {loading ? 'Sending...' : `Send ${inviteList.length} Invitation${inviteList.length > 1 ? 's' : ''}`}
            </Button>
          ) : (
            <Alert 
              severity="info" 
              sx={{ 
                maxWidth: 400, 
                mx: 'auto',
                borderRadius: 3
              }}
            >
              Add participants above to send invitations or use bulk invite for Excel/CSV files
            </Alert>
          )}
        </Box>
      </Paper>

      {/* Bulk Invite Dialog */}
      <BulkInvite
        open={bulkInviteOpen}
        onClose={() => setBulkInviteOpen(false)}
        meetingId={meetingId}
        onInvitesSent={handleBulkInvitesSent}
      />
    </>
  );
};

export default InviteParticipants;