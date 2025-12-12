import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Grid,
  Chip,
  Avatar,
  AvatarGroup,
  IconButton,
  Divider,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  TextField,
  Card,
  CardContent,
  useTheme,
  alpha,
  Tooltip,
  Badge
} from '@mui/material';
import {
  Close as CloseIcon,
  VideoCall as VideoCallIcon,
  Schedule as ScheduleIcon,
  LocationOn as LocationIcon,
  People as PeopleIcon,
  Description as DescriptionIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  ContentCopy as CopyIcon,
  Notifications as NotificationsIcon,
  Person as PersonIcon,
  CheckCircle as CheckCircleIcon,
  Cancel as CancelIcon,
  Help as HelpIcon,
  AccessTime as AccessTimeIcon
} from '@mui/icons-material';
import { format, formatDuration, intervalToDuration } from 'date-fns';

const EventDetails = ({ 
  open, 
  onClose, 
  event, 
  onEdit, 
  onDelete, 
  onJoin,
  onCopyLink,
  currentUser,
  isHost = false 
}) => {
  const theme = useTheme();
  const [participants, setParticipants] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (event && open) {
      loadParticipants();
    }
  }, [event, open]);

  const loadParticipants = async () => {
    setLoading(true);
    try {
      // Simulate API call to get participant details
      const mockParticipants = [
        {
          id: 1,
          name: event?.organizer || 'Host User',
          email: 'host@example.com',
          status: 'accepted',
          role: 'host',
          avatar: null
        },
        ...event?.guestEmails?.map((email, index) => ({
          id: index + 2,
          name: email.split('@')[0],
          email: email,
          status: ['accepted', 'pending', 'declined', 'maybe'][Math.floor(Math.random() * 4)],
          role: 'participant',
          avatar: null
        })) || []
      ];
      setParticipants(mockParticipants);
    } catch (error) {
      console.error('Error loading participants:', error);
    } finally {
      setLoading(false);
    }
  };

  const getEventDuration = () => {
    if (!event?.startTime || !event?.endTime) return 'Unknown duration';
    const start = new Date(event.startTime);
    const end = new Date(event.endTime);
    const duration = intervalToDuration({ start, end });
    
    if (duration.hours > 0) {
      return `${duration.hours}h ${duration.minutes || 0}m`;
    }
    return `${duration.minutes || 0}m`;
  };

  const getEventStatus = () => {
    if (!event?.startTime || !event?.endTime) return 'scheduled';
    const now = new Date();
    const start = new Date(event.startTime);
    const end = new Date(event.endTime);

    if (now < start) return 'upcoming';
    if (now >= start && now <= end) return 'live';
    return 'ended';
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'live': return theme.palette.success.main;
      case 'upcoming': return theme.palette.info.main;
      case 'ended': return theme.palette.grey[500];
      case 'accepted': return theme.palette.success.main;
      case 'pending': return theme.palette.warning.main;
      case 'declined': return theme.palette.error.main;
      case 'maybe': return theme.palette.info.main;
      default: return theme.palette.primary.main;
    }
  };

  const getRSVPIcon = (status) => {
    switch (status) {
      case 'accepted': return <CheckCircleIcon fontSize="small" />;
      case 'declined': return <CancelIcon fontSize="small" />;
      case 'maybe': return <HelpIcon fontSize="small" />;
      default: return <AccessTimeIcon fontSize="small" />;
    }
  };

  const handleCopyLink = () => {
    navigator.clipboard.writeText(event?.meetingURL || '');
    onCopyLink?.(event?.meetingURL);
  };

  if (!event) return null;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 3,
          bgcolor: 'background.paper'
        }
      }}
    >
      <DialogTitle sx={{ 
        pb: 1, 
        borderBottom: `1px solid ${theme.palette.divider}`,
        background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.1)} 0%, ${alpha(theme.palette.primary.main, 0.05)} 100%)`
      }}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Box display="flex" alignItems="center" gap={2}>
            <VideoCallIcon 
              sx={{ 
                color: theme.palette.primary.main,
                fontSize: '2rem'
              }} 
            />
            <Box>
              <Typography variant="h5" fontWeight={700} color="text.primary">
                {event.title}
              </Typography>
              <Chip
                label={getEventStatus().toUpperCase()}
                size="small"
                sx={{
                  mt: 0.5,
                  backgroundColor: alpha(getStatusColor(getEventStatus()), 0.1),
                  color: getStatusColor(getEventStatus()),
                  fontWeight: 600,
                  fontSize: '0.75rem'
                }}
              />
            </Box>
          </Box>
          <IconButton 
            onClick={onClose}
            sx={{ 
              color: 'text.secondary',
              '&:hover': { color: 'text.primary' }
            }}
          >
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ p: 0 }}>
        <Grid container>
          {/* Left Column - Event Details */}
          <Grid item xs={12} md={8} sx={{ p: 3 }}>
            <Box mb={3}>
              <Typography variant="h6" fontWeight={600} mb={2} color="text.primary">
                Meeting Details
              </Typography>
              
              <List disablePadding>
                <ListItem disableGutters sx={{ py: 1 }}>
                  <ListItemIcon sx={{ minWidth: 40 }}>
                    <ScheduleIcon color="primary" />
                  </ListItemIcon>
                  <ListItemText
                    primary={
                      <Typography variant="body1" fontWeight={500}>
                        {format(new Date(event.startTime), 'EEEE, MMMM dd, yyyy')}
                      </Typography>
                    }
                    secondary={
                      <Typography variant="body2" color="text.secondary">
                        {format(new Date(event.startTime), 'h:mm a')} - {format(new Date(event.endTime), 'h:mm a')} ({getEventDuration()})
                      </Typography>
                    }
                  />
                </ListItem>

                {event.location && (
                  <ListItem disableGutters sx={{ py: 1 }}>
                    <ListItemIcon sx={{ minWidth: 40 }}>
                      <LocationIcon color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary={
                        <Typography variant="body1" fontWeight={500}>
                          {event.location}
                        </Typography>
                      }
                    />
                  </ListItem>
                )}

                <ListItem disableGutters sx={{ py: 1 }}>
                  <ListItemIcon sx={{ minWidth: 40 }}>
                    <PersonIcon color="primary" />
                  </ListItemIcon>
                  <ListItemText
                    primary={
                      <Typography variant="body1" fontWeight={500}>
                        Organizer
                      </Typography>
                    }
                    secondary={
                      <Typography variant="body2" color="text.secondary">
                        {event.organizer}
                      </Typography>
                    }
                  />
                </ListItem>

                {event.reminderMinutes && (
                  <ListItem disableGutters sx={{ py: 1 }}>
                    <ListItemIcon sx={{ minWidth: 40 }}>
                      <NotificationsIcon color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary={
                        <Typography variant="body1" fontWeight={500}>
                          Reminder
                        </Typography>
                      }
                      secondary={
                        <Typography variant="body2" color="text.secondary">
                          {event.reminderMinutes} minutes before
                        </Typography>
                      }
                    />
                  </ListItem>
                )}
              </List>
            </Box>

            {event.description && (
              <Box mb={3}>
                <Typography variant="h6" fontWeight={600} mb={1} color="text.primary">
                  Description
                </Typography>
                <Card sx={{ 
                  bgcolor: alpha(theme.palette.primary.main, 0.02),
                  border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}` 
                }}>
                  <CardContent>
                    <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: 'pre-wrap' }}>
                      {event.description}
                    </Typography>
                  </CardContent>
                </Card>
              </Box>
            )}

            {/* Meeting Link */}
            {event.meetingURL && (
              <Box mb={3}>
                <Typography variant="h6" fontWeight={600} mb={1} color="text.primary">
                  Meeting Link
                </Typography>
                <Card sx={{ 
                  bgcolor: alpha(theme.palette.success.main, 0.02),
                  border: `1px solid ${alpha(theme.palette.success.main, 0.1)}` 
                }}>
                  <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                    <Box display="flex" alignItems="center" gap={2}>
                      <TextField
                        fullWidth
                        value={event.meetingURL}
                        InputProps={{
                          readOnly: true,
                          sx: { 
                            fontSize: '0.875rem',
                            '& .MuiInputBase-root': {
                              bgcolor: 'background.paper'
                            }
                          }
                        }}
                        size="small"
                      />
                      <Tooltip title="Copy Link">
                        <IconButton 
                          onClick={handleCopyLink}
                          color="primary"
                          sx={{ 
                            bgcolor: alpha(theme.palette.primary.main, 0.1),
                            '&:hover': { bgcolor: alpha(theme.palette.primary.main, 0.2) }
                          }}
                        >
                          <CopyIcon />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </CardContent>
                </Card>
              </Box>
            )}
          </Grid>

          {/* Right Column - Participants */}
          <Grid item xs={12} md={4} sx={{ 
            borderLeft: { md: `1px solid ${theme.palette.divider}` },
            bgcolor: alpha(theme.palette.primary.main, 0.02),
            p: 3
          }}>
            <Typography variant="h6" fontWeight={600} mb={2} color="text.primary">
              Participants ({participants.length})
            </Typography>

            <List disablePadding>
              {participants.map((participant) => (
                <ListItem
                  key={participant.id}
                  disableGutters
                  sx={{ 
                    py: 1,
                    borderRadius: 1,
                    mb: 1,
                    '&:hover': { bgcolor: alpha(theme.palette.primary.main, 0.05) }
                  }}
                >
                  <ListItemIcon sx={{ minWidth: 48 }}>
                    <Badge
                      overlap="circular"
                      anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                      badgeContent={
                        participant.role === 'host' ? (
                          <Avatar sx={{ 
                            width: 16, 
                            height: 16, 
                            bgcolor: theme.palette.warning.main,
                            fontSize: '0.5rem'
                          }}>
                            H
                          </Avatar>
                        ) : null
                      }
                    >
                      <Avatar 
                        src={participant.avatar}
                        sx={{ 
                          width: 40, 
                          height: 40,
                          bgcolor: theme.palette.primary.main
                        }}
                      >
                        {participant.name.charAt(0).toUpperCase()}
                      </Avatar>
                    </Badge>
                  </ListItemIcon>
                  <ListItemText
                    primary={
                      <Box display="flex" alignItems="center" gap={1}>
                        <Typography variant="body2" fontWeight={500} noWrap>
                          {participant.name}
                        </Typography>
                        {participant.role === 'host' && (
                          <Chip 
                            label="Host" 
                            size="small" 
                            sx={{ 
                              height: 20,
                              fontSize: '0.7rem',
                              bgcolor: alpha(theme.palette.warning.main, 0.1),
                              color: theme.palette.warning.main
                            }} 
                          />
                        )}
                      </Box>
                    }
                    secondary={
                      <Box display="flex" alignItems="center" gap={0.5} mt={0.5}>
                        {getRSVPIcon(participant.status)}
                        <Typography 
                          variant="caption" 
                          sx={{ color: getStatusColor(participant.status) }}
                        >
                          {participant.status.charAt(0).toUpperCase() + participant.status.slice(1)}
                        </Typography>
                      </Box>
                    }
                  />
                </ListItem>
              ))}
            </List>

            {participants.length === 0 && (
              <Box 
                display="flex" 
                flexDirection="column" 
                alignItems="center" 
                justifyContent="center"
                py={4}
              >
                <PeopleIcon sx={{ fontSize: '3rem', color: 'text.disabled', mb: 1 }} />
                <Typography variant="body2" color="text.secondary" textAlign="center">
                  No participants yet
                </Typography>
              </Box>
            )}
          </Grid>
        </Grid>
      </DialogContent>

      <DialogActions sx={{ 
        p: 3, 
        borderTop: `1px solid ${theme.palette.divider}`,
        gap: 1
      }}>
        {getEventStatus() === 'live' && (
          <Button
            variant="contained"
            color="success"
            startIcon={<VideoCallIcon />}
            onClick={() => onJoin?.(event.meetingURL)}
            sx={{
              fontWeight: 600,
              textTransform: 'none',
              borderRadius: 2,
              px: 3
            }}
          >
            Join Meeting
          </Button>
        )}

        {getEventStatus() === 'upcoming' && (
          <Button
            variant="outlined"
            color="primary"
            startIcon={<VideoCallIcon />}
            onClick={() => onJoin?.(event.meetingURL)}
            sx={{
              fontWeight: 600,
              textTransform: 'none',
              borderRadius: 2,
              px: 3
            }}
          >
            Join Meeting
          </Button>
        )}

        {isHost && (
          <>
            <Button
              variant="outlined"
              startIcon={<EditIcon />}
              onClick={() => onEdit?.(event)}
              sx={{
                fontWeight: 600,
                textTransform: 'none',
                borderRadius: 2
              }}
            >
              Edit
            </Button>
            <Button
              variant="outlined"
              color="error"
              startIcon={<DeleteIcon />}
              onClick={() => onDelete?.(event.id)}
              sx={{
                fontWeight: 600,
                textTransform: 'none',
                borderRadius: 2
              }}
            >
              Delete
            </Button>
          </>
        )}

        <Button
          onClick={onClose}
          sx={{
            fontWeight: 600,
            textTransform: 'none',
            borderRadius: 2
          }}
        >
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default EventDetails;