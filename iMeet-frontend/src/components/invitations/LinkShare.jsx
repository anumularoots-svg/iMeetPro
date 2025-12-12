import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Button,
  Card,
  CardContent,
  Grid,
  IconButton,
  useTheme,
  alpha,
  Snackbar,
  Alert,
  Divider,
  Avatar,
  Tooltip,
  Switch,
  FormControlLabel,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  QRCode as QRCodeIcon,
  Chip
} from '@mui/material';
import {
  Link,
  ContentCopy,
  Share,
  WhatsApp,
  Telegram,
  Twitter,
  Facebook,
  LinkedIn,
  Email,
  QrCode,
  Settings,
  Security,
  AccessTime,
  Group,
  CheckCircle,
  Launch
} from '@mui/icons-material';

const LinkShare = ({ meetingData, onLinkGenerated, onSettingsChange }) => {
  const theme = useTheme();
  const [meetingLink, setMeetingLink] = useState('');
  const [shortLink, setShortLink] = useState('');
  const [copySuccess, setCopySuccess] = useState(false);
  const [shareDialogOpen, setShareDialogOpen] = useState(false);
  const [qrDialogOpen, setQrDialogOpen] = useState(false);
  const [linkSettings, setLinkSettings] = useState({
    requirePassword: false,
    password: '',
    expiresAt: null,
    maxParticipants: 100,
    waitingRoom: true,
    allowAnonymous: true
  });
  const [isGenerating, setIsGenerating] = useState(false);

  // Generate meeting link based on backend Meetings table structure
  useEffect(() => {
    if (meetingData) {
      generateMeetingLink();
    }
  }, [meetingData]);

  const generateMeetingLink = async () => {
    setIsGenerating(true);
    try {
      // Generate meeting link based on meeting type and ID
      const baseUrl = window.location.origin;
      const linkId = meetingData.id || meetingData.meetingId;
      const fullLink = `${baseUrl}/join/${linkId}`;
      
      // Generate short link
      const shortId = linkId.toString().substr(-8);
      const short = `${baseUrl}/j/${shortId}`;
      
      setMeetingLink(fullLink);
      setShortLink(short);
      
      // Backend API call to save meeting link
      // const linkData = {
      //   meetingId: linkId,
      //   meetingLink: fullLink,
      //   shortLink: short,
      //   settings: linkSettings,
      //   createdAt: new Date()
      // };
      // await fetch('/api/meetings/generate-link', {
      //   method: 'POST',
      //   headers: { 'Content-Type': 'application/json' },
      //   body: JSON.stringify(linkData)
      // });
      
      if (onLinkGenerated) {
        onLinkGenerated({ fullLink, shortLink: short });
      }
    } catch (error) {
      console.error('Error generating meeting link:', error);
    } finally {
      setIsGenerating(false);
    }
  };

  const handleCopyLink = async (link = meetingLink) => {
    try {
      await navigator.clipboard.writeText(link);
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 3000);
    } catch (error) {
      console.error('Failed to copy link:', error);
    }
  };

  const handleSocialShare = (platform) => {
    const title = encodeURIComponent(`Join my meeting: ${meetingData?.title || meetingData?.meetingName || 'Video Meeting'}`);
    const url = encodeURIComponent(shortLink || meetingLink);
    const text = encodeURIComponent(`You're invited to join my video meeting. Click the link to join: ${shortLink || meetingLink}`);

    const shareUrls = {
      whatsapp: `https://wa.me/?text=${text}`,
      telegram: `https://t.me/share/url?url=${url}&text=${title}`,
      twitter: `https://twitter.com/intent/tweet?text=${title}&url=${url}`,
      facebook: `https://www.facebook.com/sharer/sharer.php?u=${url}`,
      linkedin: `https://www.linkedin.com/sharing/share-offsite/?url=${url}`,
      email: `mailto:?subject=${title}&body=${text}`
    };

    if (shareUrls[platform]) {
      window.open(shareUrls[platform], '_blank', 'width=600,height=400');
    }
  };

  const handleSettingsUpdate = async (newSettings) => {
    setLinkSettings(newSettings);
    
    // Backend API call to update meeting settings
    // await fetch(`/api/meetings/${meetingData.id}/settings`, {
    //   method: 'PUT',
    //   headers: { 'Content-Type': 'application/json' },
    //   body: JSON.stringify(newSettings)
    // });
    
    if (onSettingsChange) {
      onSettingsChange(newSettings);
    }
  };

  const renderQRCode = () => {
    // In a real implementation, you'd generate an actual QR code
    // For now, we'll show a placeholder
    return (
      <Box sx={{ 
        width: 200, 
        height: 200, 
        border: `2px solid ${theme.palette.primary.main}`,
        borderRadius: 2,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        bgcolor: alpha(theme.palette.primary.main, 0.05),
        mx: 'auto',
        mb: 2
      }}>
        <QrCode sx={{ fontSize: 100, color: theme.palette.primary.main }} />
      </Box>
    );
  };

  return (
    <Paper elevation={4} sx={{ 
      p: 4, 
      borderRadius: 4,
      background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.02)}, ${alpha(theme.palette.secondary.main, 0.02)})`,
      border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}`
    }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 4 }}>
        <Avatar sx={{ 
          mr: 3, 
          bgcolor: theme.palette.success.main,
          width: 56,
          height: 56
        }}>
          <Link sx={{ fontSize: 28 }} />
        </Avatar>
        <Box>
          <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.primary.main }}>
            Share Meeting Link
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Share your meeting with participants
          </Typography>
        </Box>
      </Box>

      {/* Meeting Info */}
      {meetingData && (
        <Card sx={{ 
          mb: 4, 
          border: `1px solid ${alpha(theme.palette.info.main, 0.2)}`,
          bgcolor: alpha(theme.palette.info.main, 0.05),
          borderRadius: 3
        }}>
          <CardContent>
            <Grid container spacing={2} alignItems="center">
              <Grid item xs={12} md={8}>
                <Typography variant="h6" sx={{ fontWeight: 'bold', mb: 1 }}>
                  {meetingData.title || meetingData.meetingName || 'Video Meeting'}
                </Typography>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
                  <Chip 
                    icon={<AccessTime />}
                    label={meetingData.startTime ? new Date(meetingData.startTime).toLocaleString() : 'Instant Meeting'}
                    size="small"
                    color="primary"
                    variant="outlined"
                  />
                  <Chip 
                    icon={<Group />}
                    label={`Host: ${meetingData.organizer || 'You'}`}
                    size="small"
                    color="secondary"
                    variant="outlined"
                  />
                </Box>
              </Grid>
              <Grid item xs={12} md={4} sx={{ textAlign: { xs: 'left', md: 'right' } }}>
                <Typography variant="caption" color="text.secondary">Meeting ID</Typography>
                <Typography variant="h6" sx={{ fontWeight: 'bold', fontFamily: 'monospace' }}>
                  {meetingData.id || 'XXX-XXX-XXX'}
                </Typography>
              </Grid>
            </Grid>
          </CardContent>
        </Card>
      )}

      {/* Meeting Links */}
      <Card sx={{ 
        mb: 4, 
        border: `2px solid ${theme.palette.success.main}`,
        borderRadius: 3,
        overflow: 'hidden'
      }}>
        <Box sx={{ 
          bgcolor: theme.palette.success.main, 
          color: 'white', 
          p: 2 
        }}>
          <Typography variant="h6" sx={{ fontWeight: 'bold' }}>
            Meeting Links
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Grid container spacing={3}>
            {/* Full Link */}
            <Grid item xs={12}>
              <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 'bold' }}>
                Full Meeting Link
              </Typography>
              <Box sx={{ display: 'flex', gap: 1 }}>
                <TextField
                  fullWidth
                  value={meetingLink}
                  variant="outlined"
                  size="small"
                  InputProps={{
                    readOnly: true,
                    sx: { fontFamily: 'monospace', fontSize: '0.9rem' }
                  }}
                />
                <Tooltip title="Copy Link">
                  <IconButton 
                    onClick={() => handleCopyLink(meetingLink)}
                    sx={{ 
                      bgcolor: theme.palette.success.main,
                      color: 'white',
                      '&:hover': { bgcolor: theme.palette.success.dark }
                    }}
                  >
                    <ContentCopy />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Open Link">
                  <IconButton 
                    onClick={() => window.open(meetingLink, '_blank')}
                    sx={{ 
                      bgcolor: theme.palette.primary.main,
                      color: 'white',
                      '&:hover': { bgcolor: theme.palette.primary.dark }
                    }}
                  >
                    <Launch />
                  </IconButton>
                </Tooltip>
              </Box>
            </Grid>

            {/* Short Link */}
            <Grid item xs={12}>
              <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 'bold' }}>
                Short Link (Easier to Share)
              </Typography>
              <Box sx={{ display: 'flex', gap: 1 }}>
                <TextField
                  fullWidth
                  value={shortLink}
                  variant="outlined"
                  size="small"
                  InputProps={{
                    readOnly: true,
                    sx: { fontFamily: 'monospace', fontSize: '1rem', fontWeight: 'bold' }
                  }}
                />
                <Tooltip title="Copy Short Link">
                  <IconButton 
                    onClick={() => handleCopyLink(shortLink)}
                    sx={{ 
                      bgcolor: theme.palette.secondary.main,
                      color: 'white',
                      '&:hover': { bgcolor: theme.palette.secondary.dark }
                    }}
                  >
                    <ContentCopy />
                  </IconButton>
                </Tooltip>
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Share Options */}
      <Card sx={{ 
        mb: 4, 
        border: `1px solid ${alpha(theme.palette.warning.main, 0.3)}`,
        borderRadius: 3
      }}>
        <Box sx={{ 
          bgcolor: alpha(theme.palette.warning.main, 0.1), 
          p: 2,
          borderBottom: `1px solid ${alpha(theme.palette.warning.main, 0.2)}`
        }}>
          <Typography variant="h6" sx={{ fontWeight: 'bold', color: theme.palette.warning.main }}>
            Share Options
          </Typography>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Grid container spacing={2}>
            {/* Quick Actions */}
            <Grid item xs={12} md={6}>
              <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 'bold' }}>
                Quick Actions
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                <Button
                  variant="outlined"
                  startIcon={<QrCode />}
                  onClick={() => setQrDialogOpen(true)}
                  size="small"
                  sx={{ borderRadius: 2 }}
                >
                  QR Code
                </Button>
                <Button
                  variant="outlined"
                  startIcon={<Share />}
                  onClick={() => setShareDialogOpen(true)}
                  size="small"
                  sx={{ borderRadius: 2 }}
                >
                  More Options
                </Button>
                <Button
                  variant="outlined"
                  startIcon={<Settings />}
                  onClick={() => {/* Open settings dialog */}}
                  size="small"
                  sx={{ borderRadius: 2 }}
                >
                  Settings
                </Button>
              </Box>
            </Grid>

            {/* Social Share */}
            <Grid item xs={12} md={6}>
              <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 'bold' }}>
                Share via Social Media
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                {[
                  { platform: 'whatsapp', icon: WhatsApp, color: '#25D366', label: 'WhatsApp' },
                  { platform: 'telegram', icon: Telegram, color: '#0088cc', label: 'Telegram' },
                  { platform: 'twitter', icon: Twitter, color: '#1DA1F2', label: 'Twitter' },
                  { platform: 'email', icon: Email, color: '#EA4335', label: 'Email' }
                ].map(({ platform, icon: Icon, color, label }) => (
                  <Tooltip key={platform} title={`Share via ${label}`}>
                    <IconButton
                      onClick={() => handleSocialShare(platform)}
                      sx={{
                        bgcolor: color,
                        color: 'white',
                        width: 40,
                        height: 40,
                        '&:hover': {
                          bgcolor: color,
                          opacity: 0.8,
                          transform: 'scale(1.1)'
                        },
                        transition: 'all 0.2s ease'
                      }}
                    >
                      <Icon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                ))}
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Link Security Settings */}
      <Card sx={{ 
        border: `1px solid ${alpha(theme.palette.error.main, 0.3)}`,
        borderRadius: 3
      }}>
        <Box sx={{ 
          bgcolor: alpha(theme.palette.error.main, 0.1), 
          p: 2,
          borderBottom: `1px solid ${alpha(theme.palette.error.main, 0.2)}`
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Security sx={{ mr: 1, color: theme.palette.error.main }} />
            <Typography variant="h6" sx={{ fontWeight: 'bold', color: theme.palette.error.main }}>
              Security Settings
            </Typography>
          </Box>
        </Box>
        <CardContent sx={{ p: 3 }}>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={linkSettings.requirePassword}
                    onChange={(e) => handleSettingsUpdate({
                      ...linkSettings,
                      requirePassword: e.target.checked
                    })}
                    color="primary"
                  />
                }
                label="Require Password"
              />
              {linkSettings.requirePassword && (
                <TextField
                  fullWidth
                  size="small"
                  label="Meeting Password"
                  value={linkSettings.password}
                  onChange={(e) => handleSettingsUpdate({
                    ...linkSettings,
                    password: e.target.value
                  })}
                  sx={{ mt: 1 }}
                />
              )}
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={linkSettings.waitingRoom}
                    onChange={(e) => handleSettingsUpdate({
                      ...linkSettings,
                      waitingRoom: e.target.checked
                    })}
                    color="primary"
                  />
                }
                label="Enable Waiting Room"
              />
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* QR Code Dialog */}
      <Dialog 
        open={qrDialogOpen} 
        onClose={() => setQrDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ textAlign: 'center', fontWeight: 'bold' }}>
          QR Code for Meeting
        </DialogTitle>
        <DialogContent sx={{ textAlign: 'center', py: 3 }}>
          {renderQRCode()}
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Scan this QR code to join the meeting
          </Typography>
          <Typography variant="caption" sx={{ fontFamily: 'monospace', wordBreak: 'break-all' }}>
            {shortLink || meetingLink}
          </Typography>
        </DialogContent>
        <DialogActions sx={{ justifyContent: 'center', pb: 3 }}>
          <Button onClick={() => setQrDialogOpen(false)} variant="outlined">
            Close
          </Button>
          <Button 
            onClick={() => handleCopyLink(shortLink || meetingLink)} 
            variant="contained"
            startIcon={<ContentCopy />}
          >
            Copy Link
          </Button>
        </DialogActions>
      </Dialog>

      {/* Success Snackbar */}
      <Snackbar
        open={copySuccess}
        autoHideDuration={3000}
        onClose={() => setCopySuccess(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert 
          onClose={() => setCopySuccess(false)} 
          severity="success" 
          sx={{ width: '100%', borderRadius: 2 }}
          icon={<CheckCircle />}
        >
          Meeting link copied to clipboard!
        </Alert>
      </Snackbar>
    </Paper>
  );
};

export default LinkShare;