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
  Divider,
  Alert,
  Snackbar,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  Chip,
  Tooltip,
  CircularProgress,
  Grid,
  Paper,
  InputAdornment
} from '@mui/material';
import {
  VideoCall,
  ContentCopy,
  Share,
  Settings,
  Security,
  Schedule,
  People,
  VolumeUp,
  Videocam,
  Close,
  Launch,
  Link as LinkIcon,
  QrCode,
  Email,
  WhatsApp,
  Telegram,
  Facebook
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { useNavigate } from 'react-router-dom';

const StyledContainer = styled(Box)(({ theme }) => ({
  minHeight: '100vh',
  background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  padding: theme.spacing(2),
}));

const MeetingCard = styled(Card)(({ theme }) => ({
  maxWidth: 600,
  width: '100%',
  borderRadius: theme.spacing(3),
  boxShadow: '0 20px 40px rgba(0,0,0,0.1)',
  background: 'rgba(255,255,255,0.95)',
  backdropFilter: 'blur(10px)',
}));

const FeatureChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.5),
  backgroundColor: theme.palette.primary.light,
  color: 'white',
}));

const ShareButton = styled(Button)(({ theme }) => ({
  borderRadius: theme.spacing(3),
  textTransform: 'none',
  padding: theme.spacing(1, 3),
  margin: theme.spacing(1),
}));

const MeetingLinkBox = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
  backgroundColor: '#f5f5f5',
  borderRadius: theme.spacing(1),
  border: '2px dashed #ddd',
  marginTop: theme.spacing(2),
}));

function InstantMeeting({ onMeetingCreated, userProfile }) {
  const navigate = useNavigate();
  const [meetingTitle, setMeetingTitle] = useState('');
  const [meetingSettings, setMeetingSettings] = useState({
    waitingRoom: true,
    recording: false,
    audioOnly: false,
    maxParticipants: 100,
    allowChat: true,
    allowScreenShare: true,
    hostVideoOn: true,
    hostAudioOn: true,
  });
  const [meetingLink, setMeetingLink] = useState('');
  const [meetingId, setMeetingId] = useState('');
  const [isCreating, setIsCreating] = useState(false);
  const [showSuccess, setShowSuccess] = useState(false);
  const [showShareDialog, setShowShareDialog] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);
  const [showAdvancedSettings, setShowAdvancedSettings] = useState(false);

  useEffect(() => {
    // Set default meeting title
    const now = new Date();
    const defaultTitle = `${userProfile?.fullName || 'User'}'s Meeting - ${now.toLocaleDateString()} ${now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
    setMeetingTitle(defaultTitle);
  }, [userProfile]);

  const generateMeetingId = () => {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
  };

  const handleCreateMeeting = async () => {
    setIsCreating(true);
    
    try {
      // Simulate API call to create meeting
      const newMeetingId = generateMeetingId();
      const newMeetingLink = `${window.location.origin}/meeting/${newMeetingId}`;
      
      // Simulate backend API call
      const meetingData = {
        id: newMeetingId,
        title: meetingTitle,
        hostId: userProfile?.id,
        meetingType: 'InstantMeeting',
        meetingLink: newMeetingLink,
        status: 'active',
        createdAt: new Date().toISOString(),
        settings: meetingSettings,
      };

      // Simulate delay
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      setMeetingId(newMeetingId);
      setMeetingLink(newMeetingLink);
      setShowSuccess(true);
      
      onMeetingCreated?.(meetingData);
      
    } catch (error) {
      console.error('Error creating meeting:', error);
    } finally {
      setIsCreating(false);
    }
  };

  const handleCopyLink = async () => {
    try {
      await navigator.clipboard.writeText(meetingLink);
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 2000);
    } catch (error) {
      console.error('Failed to copy link:', error);
    }
  };

  const handleShareLink = (platform) => {
    const shareText = `Join my video meeting: ${meetingTitle}`;
    const shareUrl = meetingLink;
    
    const shareUrls = {
      email: `mailto:?subject=${encodeURIComponent(shareText)}&body=${encodeURIComponent(`${shareText}\n\n${shareUrl}`)}`,
      whatsapp: `https://wa.me/?text=${encodeURIComponent(`${shareText}\n${shareUrl}`)}`,
      telegram: `https://t.me/share/url?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(shareText)}`,
      facebook: `https://www.facebook.com/sharer/sharer.php?u=${encodeURIComponent(shareUrl)}`,
    };

    if (shareUrls[platform]) {
      window.open(shareUrls[platform], '_blank');
    }
  };

  const handleJoinNow = () => {
    navigate(`/meeting/${meetingId}`);
  };

  const handleSettingChange = (setting, value) => {
    setMeetingSettings(prev => ({
      ...prev,
      [setting]: value
    }));
  };

  if (showSuccess && meetingLink) {
    return (
      <StyledContainer>
        <MeetingCard>
          <CardContent sx={{ p: 4, textAlign: 'center' }}>
            <VideoCall sx={{ fontSize: 80, color: 'success.main', mb: 2 }} />
            
            <Typography variant="h4" fontWeight="bold" gutterBottom>
              Meeting Created Successfully!
            </Typography>
            
            <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
              Your instant meeting is ready. Share the link below to invite participants.
            </Typography>

            <MeetingLinkBox elevation={0}>
              <Typography variant="h6" gutterBottom>
                Meeting Link
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <TextField
                  fullWidth
                  value={meetingLink}
                  variant="outlined"
                  size="small"
                  InputProps={{
                    readOnly: true,
                    startAdornment: (
                      <InputAdornment position="start">
                        <LinkIcon />
                      </InputAdornment>
                    ),
                  }}
                />
                <Tooltip title={copySuccess ? 'Copied!' : 'Copy link'}>
                  <IconButton onClick={handleCopyLink} color="primary">
                    <ContentCopy />
                  </IconButton>
                </Tooltip>
              </Box>
              
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Meeting ID: <strong>{meetingId}</strong>
              </Typography>
            </MeetingLinkBox>

            <Box sx={{ mt: 3, display: 'flex', gap: 2, justifyContent: 'center', flexWrap: 'wrap' }}>
              <Button
                variant="contained"
                size="large"
                startIcon={<Launch />}
                onClick={handleJoinNow}
                sx={{
                  borderRadius: 3,
                  px: 4,
                  background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
                }}
              >
                Join Now
              </Button>
              
              <Button
                variant="outlined"
                size="large"
                startIcon={<Share />}
                onClick={() => setShowShareDialog(true)}
                sx={{ borderRadius: 3, px: 4 }}
              >
                Share Meeting
              </Button>
            </Box>

            <Box sx={{ mt: 3 }}>
              <Typography variant="h6" gutterBottom>
                Meeting Settings
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 1 }}>
                {meetingSettings.waitingRoom && <FeatureChip label="Waiting Room" icon={<Security />} />}
                {meetingSettings.recording && <FeatureChip label="Recording Enabled" />}
                {meetingSettings.allowChat && <FeatureChip label="Chat Enabled" />}
                {meetingSettings.allowScreenShare && <FeatureChip label="Screen Share" />}
                <FeatureChip label={`Max ${meetingSettings.maxParticipants} participants`} icon={<People />} />
              </Box>
            </Box>
          </CardContent>
        </MeetingCard>

        {/* Share Dialog */}
        <Dialog
          open={showShareDialog}
          onClose={() => setShowShareDialog(false)}
          maxWidth="sm"
          fullWidth
        >
          <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            Share Meeting
            <IconButton onClick={() => setShowShareDialog(false)}>
              <Close />
            </IconButton>
          </DialogTitle>
          <DialogContent>
            <Typography variant="body1" gutterBottom>
              Share this meeting with others:
            </Typography>
            
            <Grid container spacing={2} sx={{ mt: 2 }}>
              <Grid item xs={6}>
                <ShareButton
                  fullWidth
                  variant="outlined"
                  startIcon={<Email />}
                  onClick={() => handleShareLink('email')}
                >
                  Email
                </ShareButton>
              </Grid>
              <Grid item xs={6}>
                <ShareButton
                  fullWidth
                  variant="outlined"
                  startIcon={<WhatsApp />}
                  sx={{ color: '#25D366', borderColor: '#25D366' }}
                  onClick={() => handleShareLink('whatsapp')}
                >
                  WhatsApp
                </ShareButton>
              </Grid>
              <Grid item xs={6}>
                <ShareButton
                  fullWidth
                  variant="outlined"
                  startIcon={<Telegram />}
                  sx={{ color: '#0088cc', borderColor: '#0088cc' }}
                  onClick={() => handleShareLink('telegram')}
                >
                  Telegram
                </ShareButton>
              </Grid>
              <Grid item xs={6}>
                <ShareButton
                  fullWidth
                  variant="outlined"
                  startIcon={<Facebook />}
                  sx={{ color: '#1877f2', borderColor: '#1877f2' }}
                  onClick={() => handleShareLink('facebook')}
                >
                  Facebook
                </ShareButton>
              </Grid>
            </Grid>
          </DialogContent>
        </Dialog>

        <Snackbar
          open={copySuccess}
          autoHideDuration={2000}
          message="Meeting link copied to clipboard!"
          anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        />
      </StyledContainer>
    );
  }

  return (
    <StyledContainer>
      <MeetingCard>
        <CardContent sx={{ p: 4 }}>
          <Box sx={{ textAlign: 'center', mb: 4 }}>
            <VideoCall sx={{ fontSize: 80, color: 'primary.main', mb: 2 }} />
            <Typography variant="h4" fontWeight="bold" gutterBottom>
              Create Instant Meeting
            </Typography>
            <Typography variant="body1" color="text.secondary">
              Start a meeting right now and invite participants instantly
            </Typography>
          </Box>

          {/* Meeting Title */}
          <TextField
            fullWidth
            label="Meeting Title"
            value={meetingTitle}
            onChange={(e) => setMeetingTitle(e.target.value)}
            variant="outlined"
            sx={{ mb: 3 }}
            placeholder="Enter meeting title..."
          />

          {/* Basic Settings */}
          <Typography variant="h6" gutterBottom>
            Meeting Settings
          </Typography>
          
          <FormGroup sx={{ mb: 3 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={meetingSettings.waitingRoom}
                  onChange={(e) => handleSettingChange('waitingRoom', e.target.checked)}
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
                  checked={meetingSettings.allowChat}
                  onChange={(e) => handleSettingChange('allowChat', e.target.checked)}
                />
              }
              label="Allow Chat"
            />
            
            <FormControlLabel
              control={
                <Switch
                  checked={meetingSettings.allowScreenShare}
                  onChange={(e) => handleSettingChange('allowScreenShare', e.target.checked)}
                />
              }
              label="Allow Screen Sharing"
            />
          </FormGroup>

          {/* Advanced Settings Toggle */}
          <Button
            variant="text"
            startIcon={<Settings />}
            onClick={() => setShowAdvancedSettings(!showAdvancedSettings)}
            sx={{ mb: 2 }}
          >
            {showAdvancedSettings ? 'Hide' : 'Show'} Advanced Settings
          </Button>

          {/* Advanced Settings */}
          {showAdvancedSettings && (
            <Box sx={{ mb: 3 }}>
              <Divider sx={{ mb: 2 }} />
              
              <FormGroup>
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingSettings.recording}
                      onChange={(e) => handleSettingChange('recording', e.target.checked)}
                    />
                  }
                  label="Enable Recording"
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingSettings.hostVideoOn}
                      onChange={(e) => handleSettingChange('hostVideoOn', e.target.checked)}
                    />
                  }
                  label={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Videocam fontSize="small" />
                      Start with video on
                    </Box>
                  }
                />
                
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingSettings.hostAudioOn}
                      onChange={(e) => handleSettingChange('hostAudioOn', e.target.checked)}
                    />
                  }
                  label={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <VolumeUp fontSize="small" />
                      Start with audio on
                    </Box>
                  }
                />
              </FormGroup>

              <TextField
                fullWidth
                label="Maximum Participants"
                type="number"
                value={meetingSettings.maxParticipants}
                onChange={(e) => handleSettingChange('maxParticipants', parseInt(e.target.value) || 100)}
                variant="outlined"
                sx={{ mt: 2 }}
                inputProps={{ min: 2, max: 1000 }}
              />
            </Box>
          )}

          {/* Action Buttons */}
          <Box sx={{ display: 'flex', gap: 2, justifyContent: 'center' }}>
            <Button
              variant="contained"
              size="large"
              onClick={handleCreateMeeting}
              disabled={isCreating || !meetingTitle.trim()}
              startIcon={isCreating ? <CircularProgress size={20} /> : <VideoCall />}
              sx={{
                borderRadius: 3,
                px: 4,
                py: 1.5,
                background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
                '&:hover': {
                  background: 'linear-gradient(45deg, #1976D2 30%, #0097A7 90%)',
                },
              }}
            >
              {isCreating ? 'Creating Meeting...' : 'Create Meeting'}
            </Button>
            
            <Button
              variant="outlined"
              size="large"
              onClick={() => navigate('/dashboard')}
              sx={{ borderRadius: 3, px: 4, py: 1.5 }}
            >
              Cancel
            </Button>
          </Box>

          {/* Info Alert */}
          <Alert severity="info" sx={{ mt: 3 }}>
            <Typography variant="body2">
              <strong>Instant Meeting:</strong> Your meeting will be created immediately and you can start inviting participants right away. The meeting will remain active until the last participant leaves.
            </Typography>
          </Alert>
        </CardContent>
      </MeetingCard>
    </StyledContainer>
  );
}

export default InstantMeeting;