// components/meeting/MeetingInvite.jsx
import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Box,
  Typography,
  Chip,
  Avatar,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Paper,
  Divider,
  Switch,
  FormControlLabel,
  Grid,
  Alert,
  Snackbar
} from '@mui/material';
import {
  Email,
  Link as LinkIcon,
  ContentCopy,
  Delete,
  Send,
  PersonAdd,
  Check,
  Close,
  Share
} from '@mui/icons-material';

const MeetingInvite = ({ open, onClose, meetingData }) => {
  const [inviteMethod, setInviteMethod] = useState('email');
  const [emails, setEmails] = useState([]);
  const [emailInput, setEmailInput] = useState('');
  const [message, setMessage] = useState('');
  const [sendReminder, setSendReminder] = useState(true);
  const [copySuccess, setCopySuccess] = useState(false);
  const [invitedParticipants, setInvitedParticipants] = useState([]);

  const meetingLink = `${window.location.origin}/join/${meetingData?.id}`;

  const handleAddEmail = () => {
    if (emailInput && !emails.includes(emailInput)) {
      setEmails([...emails, emailInput]);
      setEmailInput('');
    }
  };

  const handleRemoveEmail = (emailToRemove) => {
    setEmails(emails.filter(email => email !== emailToRemove));
  };

  const handleCopyLink = async () => {
    try {
      await navigator.clipboard.writeText(meetingLink);
      setCopySuccess(true);
    } catch (err) {
      console.error('Failed to copy link');
    }
  };

  const handleSendInvites = () => {
    // API call to send invites
    console.log('Sending invites to:', emails);
    // Add to invited participants
    const newParticipants = emails.map(email => ({
      email,
      status: 'sent',
      sentAt: new Date()
    }));
    setInvitedParticipants([...invitedParticipants, ...newParticipants]);
    setEmails([]);
  };

  return (
    <Dialog 
      open={open} 
      onClose={onClose} 
      maxWidth="md" 
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 3,
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          color: 'white'
        }
      }}
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" gap={2}>
          <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
            <PersonAdd />
          </Avatar>
          <Typography variant="h5" fontWeight="bold">
            Invite Participants
          </Typography>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ bgcolor: 'rgba(255,255,255,0.95)', color: 'black', m: 2, borderRadius: 2 }}>
        <Grid container spacing={3}>
          {/* Meeting Details */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2, bgcolor: '#f8f9fa', borderRadius: 2 }}>
              <Typography variant="h6" gutterBottom color="primary">
                {meetingData?.title || 'Video Meeting'}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {meetingData?.scheduledTime && `Scheduled: ${new Date(meetingData.scheduledTime).toLocaleString()}`}
              </Typography>
            </Paper>
          </Grid>

          {/* Invite Methods */}
          <Grid item xs={12}>
            <Box display="flex" gap={2} mb={3}>
              <Button
                variant={inviteMethod === 'email' ? 'contained' : 'outlined'}
                startIcon={<Email />}
                onClick={() => setInviteMethod('email')}
                sx={{ borderRadius: 2 }}
              >
                Email Invite
              </Button>
              <Button
                variant={inviteMethod === 'link' ? 'contained' : 'outlined'}
                startIcon={<LinkIcon />}
                onClick={() => setInviteMethod('link')}
                sx={{ borderRadius: 2 }}
              >
                Share Link
              </Button>
            </Box>
          </Grid>

          {/* Email Invite Section */}
          {inviteMethod === 'email' && (
            <Grid item xs={12}>
              <Box display="flex" gap={2} mb={2}>
                <TextField
                  fullWidth
                  label="Enter email addresses"
                  value={emailInput}
                  onChange={(e) => setEmailInput(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && handleAddEmail()}
                  variant="outlined"
                  size="small"
                  sx={{ borderRadius: 2 }}
                />
                <Button
                  variant="contained"
                  onClick={handleAddEmail}
                  sx={{ borderRadius: 2 }}
                >
                  Add
                </Button>
              </Box>

              {/* Email Chips */}
              <Box display="flex" flexWrap="wrap" gap={1} mb={2}>
                {emails.map((email, index) => (
                  <Chip
                    key={index}
                    label={email}
                    onDelete={() => handleRemoveEmail(email)}
                    deleteIcon={<Close />}
                    color="primary"
                    variant="outlined"
                  />
                ))}
              </Box>

              {/* Custom Message */}
              <TextField
                fullWidth
                multiline
                rows={3}
                label="Custom message (optional)"
                value={message}
                onChange={(e) => setMessage(e.target.value)}
                variant="outlined"
                sx={{ mb: 2 }}
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={sendReminder}
                    onChange={(e) => setSendReminder(e.target.checked)}
                  />
                }
                label="Send reminder 15 minutes before meeting"
              />
            </Grid>
          )}

          {/* Link Share Section */}
          {inviteMethod === 'link' && (
            <Grid item xs={12}>
              <Paper sx={{ p: 2, bgcolor: '#f8f9fa', borderRadius: 2 }}>
                <Typography variant="subtitle2" gutterBottom>
                  Meeting Link
                </Typography>
                <Box display="flex" gap={2} alignItems="center">
                  <TextField
                    fullWidth
                    value={meetingLink}
                    variant="outlined"
                    size="small"
                    InputProps={{ readOnly: true }}
                  />
                  <Button
                    variant="contained"
                    startIcon={<ContentCopy />}
                    onClick={handleCopyLink}
                    sx={{ borderRadius: 2 }}
                  >
                    Copy
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<Share />}
                    sx={{ borderRadius: 2 }}
                  >
                    Share
                  </Button>
                </Box>
              </Paper>
            </Grid>
          )}

          {/* Invited Participants List */}
          {invitedParticipants.length > 0 && (
            <Grid item xs={12}>
              <Typography variant="h6" gutterBottom>
                Invited Participants
              </Typography>
              <List>
                {invitedParticipants.map((participant, index) => (
                  <ListItem key={index} divider>
                    <ListItemAvatar>
                      <Avatar sx={{ bgcolor: 'primary.main' }}>
                        {participant.email.charAt(0).toUpperCase()}
                      </Avatar>
                    </ListItemAvatar>
                    <ListItemText
                      primary={participant.email}
                      secondary={`Sent: ${participant.sentAt.toLocaleString()}`}
                    />
                    <ListItemSecondaryAction>
                      <Chip
                        label={participant.status}
                        color={participant.status === 'sent' ? 'warning' : 'success'}
                        size="small"
                      />
                    </ListItemSecondaryAction>
                  </ListItem>
                ))}
              </List>
            </Grid>
          )}
        </Grid>
      </DialogContent>

      <DialogActions sx={{ p: 3 }}>
        <Button onClick={onClose} color="inherit">
          Cancel
        </Button>
        {inviteMethod === 'email' && emails.length > 0 && (
          <Button
            variant="contained"
            startIcon={<Send />}
            onClick={handleSendInvites}
            sx={{ borderRadius: 2 }}
          >
            Send Invites
          </Button>
        )}
      </DialogActions>

      <Snackbar
        open={copySuccess}
        autoHideDuration={3000}
        onClose={() => setCopySuccess(false)}
      >
        <Alert severity="success" onClose={() => setCopySuccess(false)}>
          Meeting link copied to clipboard!
        </Alert>
      </Snackbar>
    </Dialog>
  );
};

export default MeetingInvite;