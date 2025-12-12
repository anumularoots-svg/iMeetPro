import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Switch,
  FormControlLabel,
  Stepper,
  Step,
  StepLabel,
  Grid,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  useTheme,
  alpha,
  Divider
} from '@mui/material';
import {
  VideoCall,
  Schedule,
  CalendarMonth,
  Settings,
  Security,
  Groups,
  RecordVoiceOver,
  Share,
  ContentCopy,
  ArrowBack,
  ArrowForward,
  Check,
  Close
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

const CreateMeeting = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState(0);
  const [meetingData, setMeetingData] = useState({
    type: 'instant',
    title: '',
    description: '',
    duration: 60,
    waitingRoom: true,
    recording: false,
    chatEnabled: true,
    screenSharing: true,
    maxParticipants: 100,
    password: '',
    autoRecord: false,
    muteOnEntry: false,
    allowReactions: true
  });
  const [showPreview, setShowPreview] = useState(false);
  const [generatedLink, setGeneratedLink] = useState('');

  const steps = ['Meeting Type', 'Basic Settings', 'Security & Privacy', 'Review & Create'];

  const meetingTypes = [
    {
      id: 'instant',
      title: 'Instant Meeting',
      description: 'Start immediately',
      icon: <VideoCall />,
      color: '#4CAF50'
    },
    {
      id: 'schedule',
      title: 'Scheduled Meeting',
      description: 'Plan for later',
      icon: <Schedule />,
      color: '#2196F3'
    },
    {
      id: 'calendar',
      title: 'Calendar Meeting',
      description: 'Sync with calendar',
      icon: <CalendarMonth />,
      color: '#FF9800'
    }
  ];

  const durationOptions = [
    { value: 15, label: '15 minutes' },
    { value: 30, label: '30 minutes' },
    { value: 60, label: '1 hour' },
    { value: 90, label: '1.5 hours' },
    { value: 120, label: '2 hours' },
    { value: 180, label: '3 hours' },
    { value: 0, label: 'No limit' }
  ];

  const participantLimits = [
    { value: 10, label: '10 participants' },
    { value: 25, label: '25 participants' },
    { value: 50, label: '50 participants' },
    { value: 100, label: '100 participants' },
    { value: 250, label: '250 participants' },
    { value: 500, label: '500 participants' }
  ];

  const handleNext = () => {
    if (activeStep < steps.length - 1) {
      setActiveStep(activeStep + 1);
    } else {
      handleCreateMeeting();
    }
  };

  const handleBack = () => {
    setActiveStep(activeStep - 1);
  };

  const handleCreateMeeting = () => {
    // Generate meeting ID and link
    const meetingId = Math.random().toString(36).substring(2, 15);
    const link = `${window.location.origin}/meeting/${meetingId}`;
    setGeneratedLink(link);
    setShowPreview(true);
  };

  const handleInputChange = (field, value) => {
    setMeetingData({ ...meetingData, [field]: value });
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
              Choose Meeting Type
            </Typography>
            <Grid container spacing={3}>
              {meetingTypes.map((type) => (
                <Grid item xs={12} md={4} key={type.id}>
                  <Card
                    sx={{
                      cursor: 'pointer',
                      transition: 'all 0.3s ease',
                      border: '2px solid',
                      borderColor: meetingData.type === type.id ? type.color : 'divider',
                      backgroundColor: meetingData.type === type.id 
                        ? alpha(type.color, 0.05) 
                        : 'background.paper',
                      '&:hover': {
                        borderColor: type.color,
                        transform: 'translateY(-2px)',
                        boxShadow: `0 8px 25px ${alpha(type.color, 0.2)}`
                      }
                    }}
                    onClick={() => handleInputChange('type', type.id)}
                  >
                    <CardContent sx={{ textAlign: 'center', py: 3 }}>
                      <Box
                        sx={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          width: 60,
                          height: 60,
                          borderRadius: '50%',
                          backgroundColor: alpha(type.color, 0.1),
                          color: type.color,
                          mb: 2,
                          fontSize: 24
                        }}
                      >
                        {type.icon}
                      </Box>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
                        {type.title}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        {type.description}
                      </Typography>
                      {meetingData.type === type.id && (
                        <Check 
                          sx={{ 
                            color: type.color, 
                            mt: 1,
                            fontSize: 24
                          }} 
                        />
                      )}
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          </Box>
        );

      case 1:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
              Basic Meeting Settings
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Meeting Title"
                  value={meetingData.title}
                  onChange={(e) => handleInputChange('title', e.target.value)}
                  placeholder="Enter meeting title"
                />
              </Grid>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Description (Optional)"
                  value={meetingData.description}
                  onChange={(e) => handleInputChange('description', e.target.value)}
                  placeholder="Meeting agenda or description"
                  multiline
                  rows={3}
                />
              </Grid>
              <Grid item xs={12} sm={6}>
                <FormControl fullWidth>
                  <InputLabel>Duration</InputLabel>
                  <Select
                    value={meetingData.duration}
                    onChange={(e) => handleInputChange('duration', e.target.value)}
                  >
                    {durationOptions.map((option) => (
                      <MenuItem key={option.value} value={option.value}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} sm={6}>
                <FormControl fullWidth>
                  <InputLabel>Max Participants</InputLabel>
                  <Select
                    value={meetingData.maxParticipants}
                    onChange={(e) => handleInputChange('maxParticipants', e.target.value)}
                  >
                    {participantLimits.map((option) => (
                      <MenuItem key={option.value} value={option.value}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Box>
        );

      case 2:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
              Security & Privacy Settings
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <Card sx={{ p: 2, backgroundColor: alpha(theme.palette.primary.main, 0.05) }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                    <Security color="primary" />
                    <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                      Security Options
                    </Typography>
                  </Box>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.waitingRoom}
                            onChange={(e) => handleInputChange('waitingRoom', e.target.checked)}
                          />
                        }
                        label="Enable Waiting Room"
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.muteOnEntry}
                            onChange={(e) => handleInputChange('muteOnEntry', e.target.checked)}
                          />
                        }
                        label="Mute Participants on Entry"
                      />
                    </Grid>
                  </Grid>
                  <TextField
                    fullWidth
                    label="Meeting Password (Optional)"
                    value={meetingData.password}
                    onChange={(e) => handleInputChange('password', e.target.value)}
                    placeholder="Set a password for extra security"
                    sx={{ mt: 2 }}
                  />
                </Card>
              </Grid>
              
              <Grid item xs={12}>
                <Card sx={{ p: 2, backgroundColor: alpha(theme.palette.success.main, 0.05) }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                    <Settings color="success" />
                    <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                      Feature Settings
                    </Typography>
                  </Box>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.recording}
                            onChange={(e) => handleInputChange('recording', e.target.checked)}
                          />
                        }
                        label="Enable Recording"
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.autoRecord}
                            onChange={(e) => handleInputChange('autoRecord', e.target.checked)}
                            disabled={!meetingData.recording}
                          />
                        }
                        label="Auto-start Recording"
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.chatEnabled}
                            onChange={(e) => handleInputChange('chatEnabled', e.target.checked)}
                          />
                        }
                        label="Enable Chat"
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.screenSharing}
                            onChange={(e) => handleInputChange('screenSharing', e.target.checked)}
                          />
                        }
                        label="Allow Screen Sharing"
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={meetingData.allowReactions}
                            onChange={(e) => handleInputChange('allowReactions', e.target.checked)}
                          />
                        }
                        label="Allow Reactions"
                      />
                    </Grid>
                  </Grid>
                </Card>
              </Grid>
            </Grid>
          </Box>
        );

      case 3:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 3, fontWeight: 600 }}>
              Review Your Meeting Settings
            </Typography>
            <Card sx={{ p: 3 }}>
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                    Meeting Type
                  </Typography>
                  <Chip
                    icon={meetingTypes.find(t => t.id === meetingData.type)?.icon}
                    label={meetingTypes.find(t => t.id === meetingData.type)?.title}
                    sx={{
                      backgroundColor: alpha(meetingTypes.find(t => t.id === meetingData.type)?.color || '#000', 0.1),
                      color: meetingTypes.find(t => t.id === meetingData.type)?.color,
                      mb: 2
                    }}
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                    Duration
                  </Typography>
                  <Typography variant="body1" sx={{ mb: 2 }}>
                    {meetingData.duration === 0 ? 'No limit' : `${meetingData.duration} minutes`}
                  </Typography>
                </Grid>
                <Grid item xs={12}>
                  <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                    Meeting Title
                  </Typography>
                  <Typography variant="body1" sx={{ mb: 2 }}>
                    {meetingData.title || 'Untitled Meeting'}
                  </Typography>
                </Grid>
                {meetingData.description && (
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                      Description
                    </Typography>
                    <Typography variant="body1" sx={{ mb: 2 }}>
                      {meetingData.description}
                    </Typography>
                  </Grid>
                )}
                <Grid item xs={12}>
                  <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                    Enabled Features
                  </Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                    {meetingData.waitingRoom && <Chip label="Waiting Room" size="small" color="primary" />}
                    {meetingData.recording && <Chip label="Recording" size="small" color="success" />}
                    {meetingData.chatEnabled && <Chip label="Chat" size="small" color="info" />}
                    {meetingData.screenSharing && <Chip label="Screen Sharing" size="small" color="warning" />}
                    {meetingData.allowReactions && <Chip label="Reactions" size="small" color="secondary" />}
                    {meetingData.password && <Chip label="Password Protected" size="small" color="error" />}
                  </Box>
                </Grid>
              </Grid>
            </Card>
          </Box>
        );

      default:
        return null;
    }
  };

  return (
    <Box sx={{ p: 3, maxWidth: 800, mx: 'auto' }}>
      {/* Header */}
      <Box sx={{ mb: 4, textAlign: 'center' }}>
        <Typography 
          variant="h4" 
          sx={{ 
            fontWeight: 700,
            mb: 1,
            background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
            backgroundClip: 'text',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent'
          }}
        >
          Create New Meeting
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Set up your meeting with custom preferences
        </Typography>
      </Box>

      {/* Stepper */}
      <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
        {steps.map((label) => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>

      {/* Step Content */}
      <Card sx={{ mb: 4 }}>
        <CardContent sx={{ p: 4 }}>
          {renderStepContent(activeStep)}
        </CardContent>
      </Card>

      {/* Navigation Buttons */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
        <Button
          onClick={handleBack}
          disabled={activeStep === 0}
          startIcon={<ArrowBack />}
          variant="outlined"
        >
          Back
        </Button>
        <Button
          onClick={handleNext}
          endIcon={activeStep === steps.length - 1 ? <VideoCall /> : <ArrowForward />}
          variant="contained"
          disabled={activeStep === 0 && !meetingData.type}
        >
          {activeStep === steps.length - 1 ? 'Create Meeting' : 'Next'}
        </Button>
      </Box>

      {/* Meeting Created Dialog */}
      <Dialog open={showPreview} onClose={() => setShowPreview(false)} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ textAlign: 'center', pb: 1 }}>
          <VideoCall color="success" sx={{ fontSize: 48, mb: 1 }} />
          <Typography variant="h5" sx={{ fontWeight: 600 }}>
            Meeting Created Successfully!
          </Typography>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ textAlign: 'center', mb: 3 }}>
            <Typography variant="body1" color="text.secondary">
              Your meeting is ready. Share the link below with participants:
            </Typography>
          </Box>
          
          <TextField
            fullWidth
            value={generatedLink}
            InputProps={{
              readOnly: true,
              endAdornment: (
                <IconButton onClick={() => navigator.clipboard.writeText(generatedLink)}>
                  <ContentCopy />
                </IconButton>
              )
            }}
            sx={{ mb: 2 }}
          />

          <Box sx={{ display: 'flex', gap: 1, justifyContent: 'center', flexWrap: 'wrap' }}>
            <Chip
              icon={<ContentCopy />}
              label="Copy Link"
              onClick={() => navigator.clipboard.writeText(generatedLink)}
              clickable
              color="primary"
            />
            <Chip
              icon={<Share />}
              label="Share"
              clickable
              color="primary"
              variant="outlined"
            />
          </Box>
        </DialogContent>
        <DialogActions sx={{ justifyContent: 'center', pb: 3 }}>
          <Button onClick={() => setShowPreview(false)} variant="outlined">
            Close
          </Button>
          <Button 
            variant="contained" 
            onClick={() => {
              navigate(`/meeting/${generatedLink.split('/meeting/')[1]}`);
            }}
            startIcon={<VideoCall />}
          >
            Start Meeting Now
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default CreateMeeting;