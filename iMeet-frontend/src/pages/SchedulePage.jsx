// UPDATED: SchedulePage.jsx - Complete with transparent modal support
// ALL 1620+ LINES PRESERVED - Just updated modal opening logic

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import {
  Box,
  Container,
  Typography,
  Grid,
  Card,
  CardContent,
  CardActions,
  Button,
  IconButton,
  Chip,
  Stack,
  Avatar,
  Divider,
  useTheme,
  alpha,
  Fab,
  Badge,
  Menu,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  Skeleton,
  Snackbar,
  Tooltip,
  AvatarGroup
} from '@mui/material';
import {
  Add as AddIcon,
  Schedule as ScheduleIcon,
  VideoCall as VideoIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  MoreVert as MoreIcon,
  Person as PersonIcon,
  AccessTime as TimeIcon,
  Today as TodayIcon,
  EventAvailable as EventIcon,
  Notifications as NotificationIcon,
  Refresh as RefreshIcon,
  People as PeopleIcon,
  LocationOn as LocationIcon,
  Lock as LockIcon,
  Email as EmailIcon,
  Close as CloseIcon,
  Event as EventDateIcon,
  PlayArrow as StartIcon,
  Stop as EndIcon
} from '@mui/icons-material';
import { format, isAfter, isBefore, addHours, startOfDay, endOfDay } from 'date-fns';
import { useNavigate, useLocation } from 'react-router-dom';
import DashboardLayout from '../layouts/DashboardLayout';
import SearchInput from '../components/common/SearchInput';
import { useMeeting } from '../hooks/useMeeting';
import { useAuth } from '../hooks/useAuth';
import BackButton from '../components/common/BackButton';
import { useNotifications } from '../hooks/useNotifications';
import ScheduleMeeting from '../components/meeting/ScheduleMeeting';

const SchedulePage = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuth();
  
  const { 
    upcomingMeetings, 
    loading, 
    refreshUpcomingMeetings,
    addUpcomingMeeting,
    deleteMeeting
  } = useMeeting();

  const { notifications, fetchScheduleNotifications } = useNotifications();
  
  const [searchQuery, setSearchQuery] = useState('');
  const [filterBy, setFilterBy] = useState('upcoming');
  const [menuAnchor, setMenuAnchor] = useState(null);
  const [selectedMeetingId, setSelectedMeetingId] = useState(null);
  const [showSuccessMessage, setShowSuccessMessage] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [meetingToDelete, setMeetingToDelete] = useState(null);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' });
  const [viewDialogOpen, setViewDialogOpen] = useState(false);
  const [selectedMeetingForView, setSelectedMeetingForView] = useState(null);
  
  // NEW: State for schedule meeting modal
  const [scheduleDialogOpen, setScheduleDialogOpen] = useState(false);
  const [editingMeeting, setEditingMeeting] = useState(null);
  
  const hasInitialized = useRef(false);
  const hasHandledNavigation = useRef(false);
  const refreshTimeoutRef = useRef(null);
  const navigationTimeoutRef = useRef(null);

  // OPTIMIZED: Initial load with better error handling
  useEffect(() => {
    if (!hasInitialized.current && user?.id) {
      console.log('📅 SchedulePage: Initial load of scheduled meetings...');
      const initLoad = async () => {
        try {
          await refreshUpcomingMeetings();
          hasInitialized.current = true;
        } catch (error) {
          console.error('Failed to load meetings:', error);
          setSnackbar({
            open: true,
            message: 'Failed to load meetings. Please refresh.',
            severity: 'error'
          });
        }
      };
      initLoad();
    }
  }, [user?.id, refreshUpcomingMeetings]);

  // OPTIMIZED: Faster navigation handling with immediate UI feedback
  useEffect(() => {
    if (
      location.state?.refreshMeetings && 
      hasInitialized.current && 
      !hasHandledNavigation.current
    ) {
      console.log('🔄 SchedulePage: Handling navigation refresh - OPTIMIZED...');
      
      hasHandledNavigation.current = true;
      
      // Show success message immediately
      setShowSuccessMessage(true);
      
      // Clear navigation state immediately to prevent re-triggers
      window.history.replaceState({}, document.title);
      
      // OPTIMIZED: Add new meeting to local state immediately if provided
      if (location.state?.newMeeting) {
        console.log('➕ Adding new meeting to local state immediately');
        addUpcomingMeeting(location.state.newMeeting);
      }
      
      // OPTIMIZED: Reduced refresh delay from 500ms to 100ms
      refreshTimeoutRef.current = setTimeout(() => {
        refreshUpcomingMeetings();
      }, 100);
    }

    return () => {
      if (refreshTimeoutRef.current) {
        clearTimeout(refreshTimeoutRef.current);
      }
    };
  }, [location.state?.refreshMeetings, location.state?.newMeeting, refreshUpcomingMeetings, addUpcomingMeeting]);

  // OPTIMIZED: Reset navigation handler on route change
  useEffect(() => {
    hasHandledNavigation.current = false;
    
    // Clear any pending timeouts when route changes
    if (navigationTimeoutRef.current) {
      clearTimeout(navigationTimeoutRef.current);
    }
  }, [location.pathname]);

  const now = new Date();
  
  // Helper function to parse participants from meeting data
  const parseParticipants = useCallback((meeting) => {
    let participantEmails = [];
    let participantDetails = [];
    
    // Parse from email field (comma-separated)
    if (meeting?.email && typeof meeting.email === 'string') {
      participantEmails = meeting.email
        .split(',')
        .map(e => e.trim())
        .filter(e => e && e.length > 0 && e.includes('@'));
    }
    
    // Parse from participants field if available
    if (meeting?.participants) {
      if (Array.isArray(meeting.participants)) {
        meeting.participants.forEach(p => {
          if (typeof p === 'string') {
            if (p.includes('@')) {
              participantEmails.push(p);
              participantDetails.push({ email: p, name: p.split('@')[0] });
            }
          } else if (p && typeof p === 'object') {
            participantDetails.push({
              email: p.email,
              name: p.name || (p.email ? p.email.split('@')[0] : 'Unknown')
            });
            if (p.email && p.email.length > 0 && !participantEmails.includes(p.email)) {
              participantEmails.push(p.email);
            }
          }
        });
      } else if (typeof meeting.participants === 'string') {
        try {
          const parsed = JSON.parse(meeting.participants);
          if (Array.isArray(parsed)) {
            parsed.forEach(p => {
              if (typeof p === 'string' && p.includes('@')) {
                participantEmails.push(p);
                participantDetails.push({ email: p, name: p.split('@')[0] });
              } else if (p && typeof p === 'object') {
                participantDetails.push({
                  email: p.email,
                  name: p.name || (p.email ? p.email.split('@')[0] : 'Unknown')
                });
                if (p.email && p.email.length > 0 && !participantEmails.includes(p.email)) {
                  participantEmails.push(p.email);
                }
              }
            });
          }
        } catch (e) {
          console.debug('Failed to parse participants JSON:', e);
        }
      }
    }
    
    // Remove duplicates
    participantEmails = [...new Set(participantEmails)];
    
    return {
      emails: participantEmails,
      details: participantDetails,
      count: participantEmails.length
    };
  }, []);

  // Function to format date and time
  const formatDateTime = useCallback((dateTime) => {
    if (!dateTime) return 'Invalid Date';
    const date = new Date(dateTime);
    if (isNaN(date.getTime())) return 'Invalid Date';
    return format(date, 'MMM dd, yyyy • HH:mm');
  }, []);

  // Function to get start and end times including recurrence end date
  const getMeetingTimes = useCallback((meeting) => {
    console.log('getMeetingTimes called with:', meeting);
    console.log('Available time fields:', {
      Started_At: meeting?.Started_At,
      start_time: meeting?.start_time,
      Ended_At: meeting?.Ended_At,
      end_time: meeting?.end_time,
      start_date: meeting?.start_date,
      end_date: meeting?.end_date,
      recurrence_end_date: meeting?.recurrence_end_date,
      is_recurring: meeting?.is_recurring
    });
    
    const startTime = meeting?.Started_At || meeting?.start_time || meeting?.startTime;
    const endTime = meeting?.Ended_At || meeting?.end_time || meeting?.endTime;
    const recurrenceEndDate = meeting?.recurrence_end_date;
    
    const finalEndDate = meeting?.end_date || recurrenceEndDate;
    
    console.log('Parsed times:', { 
      startTime, 
      endTime, 
      finalEndDate, 
      recurrenceEndDate,
      end_date: meeting?.end_date 
    });
    
    const actualEndDate = finalEndDate ? 
      new Date(finalEndDate) : 
      (endTime ? new Date(endTime) : null);
    
    const result = {
      start: startTime ? formatDateTime(startTime) : 'Not set',
      end: endTime ? formatDateTime(endTime) : 'Not set',
      seriesEndDate: actualEndDate ? formatDateTime(actualEndDate) : null,
      isRecurring: Boolean(meeting?.is_recurring),
      startDate: startTime ? new Date(startTime) : null,
      endDate: endTime ? new Date(endTime) : null,
      actualEndDate: actualEndDate,
      finalEndDate: finalEndDate
    };
    
    console.log('getMeetingTimes result:', result);
    return result;
  }, [formatDateTime]);
  
  // OPTIMIZED: Memoized filtering with better performance
  const filteredMeetings = useMemo(() => {
    if (!upcomingMeetings || upcomingMeetings.length === 0) {
      return [];
    }
    
    const startFilterTime = performance.now();
    
    const filtered = upcomingMeetings.filter(meeting => {
      const meetingType = meeting?.Meeting_Type || meeting?.type || '';
      if (meetingType.toLowerCase() !== 'schedulemeeting' && meetingType.toLowerCase() !== 'scheduled') {
        return false;
      }

      const meetingName = meeting?.Meeting_Name || meeting?.title || '';
      const hostName = user?.full_name || user?.name || '';
      const description = meeting?.description || '';
      
      const { emails: participantEmails } = parseParticipants(meeting);
      const isParticipant = participantEmails.includes(user?.email);

      const matchesSearch =
        searchQuery === '' ||
        meetingName.toLowerCase().includes(searchQuery.toLowerCase()) ||
        hostName.toLowerCase().includes(searchQuery.toLowerCase()) ||
        description.toLowerCase().includes(searchQuery.toLowerCase()) ||
        participantEmails.some(email => email.toLowerCase().includes(searchQuery.toLowerCase()));

      if (!matchesSearch) return false;

      const meetingStartTime = meeting?.Started_At || meeting?.start_time;
      if (!meetingStartTime) return false;

      const meetingTime = new Date(meetingStartTime);
      if (isNaN(meetingTime.getTime())) return false;

      switch (filterBy) {
        case 'all':
          return true;
        case 'upcoming': {
          const meetingEndTime =
            new Date(meeting?.Ended_At || meeting?.end_time || (meetingTime.getTime() + 60 * 60 * 1000));
          const timeSinceEnd = now.getTime() - meetingEndTime.getTime();
          if (timeSinceEnd > 0) return false;
          return meeting?.Status !== 'completed';
        }
        case 'today': {
          const start = startOfDay(now);
          const end = endOfDay(now);
          return isAfter(meetingTime, start) && isBefore(meetingTime, end);
        }
        case 'past': {
          const endTime = new Date(
            meeting?.Ended_At || meeting?.end_time || (meetingTime.getTime() + 60 * 60 * 1000)
          );
          return isBefore(endTime, now) || meeting?.Status === 'completed';
        }
        case 'hosting':
          return meeting?.Host_ID === user?.id || meeting?.is_host;
        case 'invited':
          return isParticipant && meeting?.Host_ID !== user?.id;
        default:
          return true;
      }
    });
    
    const filterTime = performance.now() - startFilterTime;
    if (filterTime > 50) {
      console.warn(`🐌 Filtering took ${filterTime.toFixed(2)}ms for ${upcomingMeetings.length} meetings`);
    }
    
    return filtered;
  }, [upcomingMeetings, searchQuery, filterBy, now, user, parseParticipants]);

  // OPTIMIZED: Memoized meeting counts calculation
  const meetingCounts = useMemo(() => {
    const startTime = performance.now();
    
    if (!upcomingMeetings || upcomingMeetings.length === 0) {
      return { upcoming: 0, today: 0, startingSoon: 0, total: 0, hosting: 0, invited: 0 };
    }

    const counts = {
      upcoming: 0,
      today: 0,
      startingSoon: 0,
      total: upcomingMeetings.length,
      hosting: 0,
      invited: 0
    };

    const start = startOfDay(now);
    const end = endOfDay(now);

    upcomingMeetings.forEach(meeting => {
      const isHost = meeting?.Host_ID === user?.id || meeting?.is_host;
      const { emails: participantEmails } = parseParticipants(meeting);
      const isParticipant = participantEmails.includes(user?.email);
      
      if (isHost) counts.hosting++;
      if (isParticipant && !isHost) counts.invited++;
      
      const meetingStartTime = meeting?.Started_At || meeting?.start_time;
      if (!meetingStartTime) return;
      
      const meetingTime = new Date(meetingStartTime);
      if (isNaN(meetingTime.getTime())) return;

      if (meeting?.Status !== 'completed') {
        const meetingEndTime = new Date(meeting?.Ended_At || meeting?.end_time || meetingTime.getTime() + 60 * 60 * 1000);
        const timeSinceEnd = now.getTime() - meetingEndTime.getTime();
        
        if (timeSinceEnd <= 30 * 60 * 1000) {
          counts.upcoming++;
          const timeDiff = meetingTime.getTime() - now.getTime();
          if (timeDiff < 60 * 60 * 1000 && timeDiff > 0) {
            counts.startingSoon++;
          }
        }
      }
      if (isAfter(meetingTime, start) && isBefore(meetingTime, end)) {
        counts.today++;
      }
    });

    const countTime = performance.now() - startTime;
    if (countTime > 20) {
      console.warn(`🐌 Meeting count calculation took ${countTime.toFixed(2)}ms`);
    }

    return counts;
  }, [upcomingMeetings, now, user, parseParticipants]);

  const handleMenuOpen = useCallback((event, meetingId) => {
    setMenuAnchor(event.currentTarget);
    setSelectedMeetingId(meetingId);
  }, []);

  const handleMenuClose = useCallback(() => {
    setMenuAnchor(null);
    setSelectedMeetingId(null);
  }, []);

  const handleEdit = useCallback((meeting) => {
    console.log('🔧 Opening edit dialog for meeting:', meeting);
    setEditingMeeting(meeting);
    setScheduleDialogOpen(true);
    handleMenuClose();
  }, [handleMenuClose]);

  const handleDelete = useCallback((meetingId) => {
    const meeting = upcomingMeetings.find(m => (m?.ID || m?.Meeting_ID) === meetingId);
    if (meeting) {
      setMeetingToDelete(meeting);
      setDeleteDialogOpen(true);
    }
    handleMenuClose();
  }, [upcomingMeetings, handleMenuClose]);

  // OPTIMIZED: Faster delete with optimistic updates
  const confirmDelete = useCallback(async () => {
    if (!meetingToDelete) return;
    
    const meetingId = meetingToDelete?.ID || meetingToDelete?.Meeting_ID;
    if (!meetingId) {
      console.error('No meeting ID found for deletion');
      setSnackbar({
        open: true,
        message: 'Error: No meeting ID found',
        severity: 'error'
      });
      return;
    }

    try {
      setDeleteLoading(true);
      console.log('🗑️ Deleting meeting:', meetingToDelete);
      
      setDeleteDialogOpen(false);
      setMeetingToDelete(null);
      
      const result = await deleteMeeting(meetingId);
      
      if (result.success) {
        console.log('✅ Meeting deleted successfully');
        
        setSnackbar({
          open: true,
          message: `Meeting "${meetingToDelete.Meeting_Name || meetingToDelete.title}" deleted successfully!`,
          severity: 'success'
        });
        
        refreshUpcomingMeetings();
        
      } else {
        throw new Error(result.message || 'Failed to delete meeting');
      }
      
    } catch (error) {
      console.error('❌ Error deleting meeting:', error);
      
      setSnackbar({
        open: true,
        message: `Failed to delete meeting: ${error.message || 'Unknown error'}`,
        severity: 'error'
      });
    } finally {
      setDeleteLoading(false);
    }
  }, [meetingToDelete, deleteMeeting, refreshUpcomingMeetings]);

  const handleJoinMeeting = useCallback((meeting) => {
    const meetingId = meeting?.ID || meeting?.Meeting_ID;
    if (meetingId) {
      navigate(`/meeting/${meetingId}`);
    }
  }, [navigate]);

  // OPTIMIZED: Debounced refresh to prevent multiple rapid calls
  const debouncedRefresh = useCallback(() => {
    if (refreshTimeoutRef.current) {
      clearTimeout(refreshTimeoutRef.current);
    }
    
    refreshTimeoutRef.current = setTimeout(() => {
      console.log('🔄 Debounced refresh triggered');
      refreshUpcomingMeetings();
    }, 300);
  }, [refreshUpcomingMeetings]);

  const handleRefresh = useCallback(() => {
    console.log('🔄 Manual refresh triggered');
    debouncedRefresh();
  }, [debouncedRefresh]);

  const handleScheduleNew = useCallback(() => {
    console.log('➕ Opening new meeting dialog');
    setEditingMeeting(null);
    setScheduleDialogOpen(true);
  }, []);

  const getMeetingStatus = useCallback((meeting) => {
    const meetingTime = new Date(meeting?.Started_At || meeting?.start_time);
    if (isNaN(meetingTime.getTime())) {
      return { color: 'default', text: 'Invalid Date' };
    }

    const timeDiff = meetingTime.getTime() - now.getTime();
    const hoursUntil = timeDiff / (1000 * 60 * 60);
    
    if (meeting?.Status === 'completed' || meeting?.status === 'completed') {
      return { color: 'default', text: 'Completed' };
    }
    if (hoursUntil < -24) {
      return { color: 'error', text: 'Overdue' };
    }
    if (hoursUntil < 0 && hoursUntil >= -2) {
      return { color: 'info', text: 'In Progress' };
    }
    if (hoursUntil < 1 && hoursUntil >= 0) {
      return { color: 'warning', text: 'Starting Soon' };
    }
    return { color: 'success', text: 'Scheduled' };
  }, [now]);

  const getMeetingDuration = useCallback((meeting) => {
    if (meeting?.duration_minutes) return meeting.duration_minutes;
    if (meeting?.duration) return meeting.duration;
    
    const startTime = new Date(meeting?.Started_At || meeting?.start_time);
    const endTime = new Date(meeting?.Ended_At || meeting?.end_time);
    
    if (!isNaN(startTime.getTime()) && !isNaN(endTime.getTime()) && endTime > startTime) {
      return Math.round((endTime - startTime) / (1000 * 60));
    }
    
    return 60;
  }, []);

  const handleCloseSuccessMessage = useCallback(() => {
    setShowSuccessMessage(false);
  }, []);

  useEffect(() => {
    if (showSuccessMessage) {
      const timer = setTimeout(() => {
        setShowSuccessMessage(false);
      }, 3000);
      
      return () => clearTimeout(timer);
    }
  }, [showSuccessMessage]);

  useEffect(() => {
    console.log('📅 Schedule Page: Fetching SCHEDULE notifications only');
    fetchScheduleNotifications();
  }, [fetchScheduleNotifications]);

  const handleCloseSnackbar = useCallback(() => {
    setSnackbar(prev => ({ ...prev, open: false }));
  }, []);

  const handleView = useCallback((meeting) => {
    console.log('Original meeting data:', meeting);
    
    const { emails: participantEmails, details: participantDetails } = parseParticipants(meeting);
    
    const actualHostId = meeting?.Host_ID || meeting?.host_id;
    const actualHostName = meeting?.host_full_name || 
                          meeting?.host_username || 
                          meeting?.host_name || 
                          meeting?.organizer || 
                          'Unknown Host';
    const actualHostEmail = meeting?.host_email || '';
    
    const isCurrentUserHost = actualHostId === user?.id || meeting?.is_host === true;
    
    console.log('Raw meeting for time parsing:', meeting);
    
    const transformedMeeting = {
      ...meeting,
      id: meeting?.ID || meeting?.Meeting_ID,
      title: meeting?.Meeting_Name || meeting?.title || 'Meeting',
      startTime: meeting?.Started_At || meeting?.start_time,
      endTime: meeting?.Ended_At || meeting?.end_time,
      location: meeting?.location || '',
      description: meeting?.description || '',
      
      organizer: actualHostName,
      organizerEmail: actualHostEmail,
      hostId: actualHostId,
      hostName: actualHostName,
      
      meetingURL: meeting?.Meeting_Link || `${window.location.origin}/meeting/${meeting?.ID || meeting?.Meeting_ID}`,
      reminderMinutes: (() => {
        try {
          return meeting?.reminders_times ? JSON.parse(meeting.reminders_times)[0] : 15;
        } catch {
          return 15;
        }
      })(),
      guestEmails: participantEmails,
      
      duration: meeting?.duration_minutes || 60,
      timezone: meeting?.timezone || 'Asia/Kolkata',
      settings: {
        waitingRoom: meeting?.Waiting_Room_Enabled || meeting?.settings_waiting_room,
        recording: meeting?.Is_Recording_Enabled || meeting?.settings_recording,
        allowChat: meeting?.settings_allow_chat,
        allowScreenShare: meeting?.settings_allow_screen_share,
        muteParticipants: meeting?.settings_mute_participants,
        requirePassword: meeting?.settings_require_password
      },
      recurrence: {
        enabled: meeting?.is_recurring,
        type: meeting?.recurrence_type,
        interval: meeting?.recurrence_interval
      },
      reminders: {
        email: meeting?.reminders_email,
        browser: meeting?.reminders_browser
      },
      
      participants: participantDetails
        .filter(p => p.email !== actualHostEmail)
        .map(p => ({
          name: p.name || p.email?.split('@')[0] || 'Unknown',
          email: p.email,
          isHost: false
        }))
    };
    
    console.log('Transformed meeting data:', transformedMeeting);
    
    setSelectedMeetingForView(transformedMeeting);
    setViewDialogOpen(true);
    handleMenuClose();
  }, [user, parseParticipants, handleMenuClose]);

  const getFirstChar = useCallback((str) => {
    if (!str || typeof str !== 'string' || str.length === 0) return '?';
    return str.charAt(0).toUpperCase();
  }, []);

  // NEW: Handle meeting save from dialog
  const handleMeetingSave = useCallback((savedMeeting) => {
    console.log('✅ Meeting saved successfully:', savedMeeting);
    setScheduleDialogOpen(false);
    setEditingMeeting(null);
    
    setSnackbar({
      open: true,
      message: editingMeeting ? 'Meeting updated successfully!' : 'Meeting created successfully!',
      severity: 'success'
    });
    
    // Refresh meetings list
    refreshUpcomingMeetings();
  }, [editingMeeting, refreshUpcomingMeetings]);

  // NEW: Handle dialog close
  const handleDialogClose = useCallback(() => {
    console.log('❌ Closing schedule dialog');
    setScheduleDialogOpen(false);
    setEditingMeeting(null);
  }, []);

  return (
    <DashboardLayout>
      <Box
        sx={{
          flexGrow: 1,
          background: `linear-gradient(135deg, ${alpha(theme.palette.background.default, 0.9)} 0%, ${alpha(theme.palette.primary.main, 0.02)} 100%)`,
          minHeight: '100vh',
          pt: 2
        }}
      >
        <Container maxWidth="xl" sx={{ py: 3 }}>
          {(location.state?.message || showSuccessMessage) && (
            <Alert 
              severity="success" 
              sx={{ mb: 3, animation: 'slideInDown 0.3s ease-out' }}
              onClose={handleCloseSuccessMessage}
            >
              {location.state?.message || 'Meeting scheduled successfully!'}
            </Alert>
          )}

          {/* Header */}
          <Box mb={4}>
            <Stack direction="row" alignItems="center" justifyContent="space-between" mb={3}>
              <Stack direction="row" alignItems="center" spacing={2}>
                <ScheduleIcon sx={{ fontSize: 32, color: theme.palette.primary.main }} />
                <Typography variant="h4" fontWeight={700} color="text.primary">
                  Scheduled Meetings
                </Typography>
                <Badge badgeContent={meetingCounts.upcoming} color="primary">
                  <Chip 
                    label="Upcoming" 
                    size="small" 
                    color="primary" 
                    variant="outlined" 
                  />
                </Badge>
              </Stack>
              <Stack direction="row" spacing={2}>
                <Button
                  variant="outlined"
                  startIcon={<RefreshIcon />}
                  onClick={handleRefresh}
                  disabled={loading}
                  sx={{
                    borderRadius: 2,
                    textTransform: 'none',
                    px: 2,
                    py: 1,
                  }}
                >
                  {loading ? 'Refreshing...' : 'Refresh'}
                </Button>
                <Button
                  variant="contained"
                  startIcon={<AddIcon />}
                  onClick={handleScheduleNew}
                  sx={{
                    borderRadius: 2,
                    textTransform: 'none',
                    px: 3,
                    py: 1.5,
                    fontWeight: 600,
                  }}
                >
                  Schedule Meeting
                </Button>
              </Stack>
            </Stack>

            {/* Search and Filters */}
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems="center">
              <SearchInput
                placeholder="Search meetings..."
                onSearch={setSearchQuery}
                size="medium"
                sx={{ minWidth: 300 }}
              />
              
              <Stack direction="row" spacing={1}>
                {[
                  { key: 'upcoming', label: 'Upcoming', count: meetingCounts.upcoming },
                  { key: 'today', label: 'Today', count: meetingCounts.today },
                  { key: 'hosting', label: 'Hosting', count: meetingCounts.hosting },
                  { key: 'invited', label: 'Invited', count: meetingCounts.invited },
                  { key: 'past', label: 'Past', count: null },
                  { key: 'all', label: 'All', count: null }
                ].map((filter) => (
                  <Chip
                    key={filter.key}
                    label={
                      <Stack direction="row" alignItems="center" spacing={0.5}>
                        <span>{filter.label}</span>
                        {filter.count !== null && filter.count > 0 && (
                          <Badge 
                            badgeContent={filter.count} 
                            color="primary" 
                            sx={{ '& .MuiBadge-badge': { fontSize: '0.6rem', height: 16, minWidth: 16 } }}
                          />
                        )}
                      </Stack>
                    }
                    variant={filterBy === filter.key ? "filled" : "outlined"}
                    color={filterBy === filter.key ? "primary" : "default"}
                    onClick={() => setFilterBy(filter.key)}
                    sx={{ textTransform: 'capitalize' }}
                  />
                ))}
              </Stack>
            </Stack>
          </Box>

          {/* Statistics Cards */}
          <Grid container spacing={3} mb={4}>
            <Grid item xs={12} sm={6} md={3}>
              <Card sx={{ 
                background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.1)} 0%, ${alpha(theme.palette.primary.main, 0.05)} 100%)`,
                border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`
              }}>
                <CardContent>
                  <Stack direction="row" alignItems="center" spacing={2}>
                    <Avatar sx={{ bgcolor: theme.palette.primary.main }}>
                      <TodayIcon />
                    </Avatar>
                    <Box>
                      <Typography variant="h4" fontWeight={700} color="primary">
                        {loading ? <Skeleton width={30} /> : meetingCounts.today}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Today's Meetings
                      </Typography>
                    </Box>
                  </Stack>
                </CardContent>
              </Card>
            </Grid>
            
            <Grid item xs={12} sm={6} md={3}>
              <Card sx={{ 
                background: `linear-gradient(135deg, ${alpha(theme.palette.success.main, 0.1)} 0%, ${alpha(theme.palette.success.main, 0.05)} 100%)`,
                border: `1px solid ${alpha(theme.palette.success.main, 0.2)}`
              }}>
                <CardContent>
                  <Stack direction="row" alignItems="center" spacing={2}>
                    <Avatar sx={{ bgcolor: theme.palette.success.main }}>
                      <EventIcon />
                    </Avatar>
                    <Box>
                      <Typography variant="h4" fontWeight={700} color="success.main">
                        {loading ? <Skeleton width={30} /> : meetingCounts.upcoming}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Upcoming
                      </Typography>
                    </Box>
                  </Stack>
                </CardContent>
              </Card>
            </Grid>
            
            <Grid item xs={12} sm={6} md={3}>
              <Card sx={{ 
                background: `linear-gradient(135deg, ${alpha(theme.palette.info.main, 0.1)} 0%, ${alpha(theme.palette.info.main, 0.05)} 100%)`,
                border: `1px solid ${alpha(theme.palette.info.main, 0.2)}`
              }}>
                <CardContent>
                  <Stack direction="row" alignItems="center" spacing={2}>
                    <Avatar sx={{ bgcolor: theme.palette.info.main }}>
                      <PersonIcon />
                    </Avatar>
                    <Box>
                      <Typography variant="h4" fontWeight={700} color="info.main">
                        {loading ? <Skeleton width={30} /> : meetingCounts.hosting}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Hosting
                      </Typography>
                    </Box>
                  </Stack>
                </CardContent>
              </Card>
            </Grid>
            
            <Grid item xs={12} sm={6} md={3}>
              <Card sx={{ 
                background: `linear-gradient(135deg, ${alpha(theme.palette.warning.main, 0.1)} 0%, ${alpha(theme.palette.warning.main, 0.05)} 100%)`,
                border: `1px solid ${alpha(theme.palette.warning.main, 0.2)}`
              }}>
                <CardContent>
                  <Stack direction="row" alignItems="center" spacing={2}>
                    <Avatar sx={{ bgcolor: theme.palette.warning.main }}>
                      <NotificationIcon />
                    </Avatar>
                    <Box>
                      <Typography variant="h4" fontWeight={700} color="warning.main">
                        {loading ? <Skeleton width={30} /> : meetingCounts.startingSoon}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Starting Soon
                      </Typography>
                    </Box>
                  </Stack>
                </CardContent>
              </Card>
            </Grid>
          </Grid>

          {/* Meetings List */}
          {loading ? (
            <Grid container spacing={3}>
              {[1, 2, 3, 4].map((i) => (
                <Grid item xs={12} lg={6} key={i}>
                  <Card sx={{ height: 250 }}>
                    <CardContent>
                      <Skeleton variant="text" width="80%" height={32} />
                      <Skeleton variant="text" width="60%" height={24} />
                      <Skeleton variant="text" width="40%" height={20} />
                      <Box sx={{ mt: 2 }}>
                        <Skeleton variant="rectangular" height={80} />
                      </Box>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          ) : (
            <Grid container spacing={3}>
              {filteredMeetings.map((meeting) => {
                const meetingId = meeting?.ID || meeting?.Meeting_ID;
                const meetingName = meeting?.Meeting_Name || meeting?.title || 'Untitled Meeting';
                const duration = getMeetingDuration(meeting);
                const status = getMeetingStatus(meeting);
                const meetingTimes = getMeetingTimes(meeting);
                
                const { emails: participantEmails, details: participantDetails, count: participantCount } = parseParticipants(meeting);
                
                const isHost = meeting?.Host_ID === user?.id || meeting?.is_host;
                const isParticipant = meeting?.is_participant || participantEmails.includes(user?.email);
                
                return (
                  <Grid item xs={12} lg={6} key={meetingId}>
                    <Card
                      sx={{
                        height: '100%',
                        transition: 'all 0.3s ease',
                        '&:hover': {
                          transform: 'translateY(-2px)',
                          boxShadow: theme.shadows[8],
                        },
                        border: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
                        borderRadius: 2,
                        borderLeft: `4px solid ${theme.palette[status.color].main}`,
                      }}
                    >
                      <CardContent>
                        <Stack spacing={2}>
                          {/* Header */}
                          <Stack direction="row" alignItems="flex-start" justifyContent="space-between">
                            <Box flex={1}>
                              <Typography variant="h6" fontWeight={600} mb={0.5}>
                                {meetingName}
                              </Typography>
                              <Stack direction="row" spacing={1}>
                                <Chip
                                  label={status.text}
                                  size="small"
                                  color={status.color}
                                  sx={{ borderRadius: 1 }}
                                />
                                <Chip
                                  label={isHost ? 'Host' : 'Participant'}
                                  size="small"
                                  color={isHost ? 'primary' : 'default'}
                                  variant={isHost ? 'filled' : 'outlined'}
                                  icon={isHost ? <PersonIcon /> : <PeopleIcon />}
                                />
                              </Stack>
                            </Box>
                            
                            {(isHost || isParticipant) && (
                              <IconButton
                                size="small"
                                onClick={(e) => handleMenuOpen(e, meetingId)}
                              >
                                <MoreIcon />
                              </IconButton>
                            )}
                          </Stack>

                          {/* Time Display */}
                          <Stack spacing={1}>
                            <Stack direction="row" alignItems="center" spacing={0.5}>
                              <StartIcon sx={{ fontSize: 16, color: 'success.main' }} />
                              <Typography variant="body2" color="text.secondary">
                                <strong>Start:</strong> {meetingTimes.start}
                              </Typography>
                            </Stack>
                            <Stack direction="row" alignItems="center" spacing={0.5}>
                              <EndIcon sx={{ fontSize: 16, color: 'warning.main' }} />
                              <Typography variant="body2" color="text.secondary">
                                <strong>Session End:</strong> {meetingTimes.end}
                              </Typography>
                            </Stack>
                            {meetingTimes.isRecurring && meetingTimes.seriesEndDate && (
                              <Stack direction="row" alignItems="center" spacing={0.5}>
                                <EventDateIcon sx={{ fontSize: 16, color: 'error.main' }} />
                                <Typography variant="body2" color="text.secondary">
                                  <strong>Series Ends:</strong> {meetingTimes.seriesEndDate}
                                </Typography>
                              </Stack>
                            )}
                            <Stack direction="row" alignItems="center" spacing={0.5}>
                              <TimeIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
                              <Typography variant="body2" color="text.secondary">
                                Duration: {duration} minutes
                              </Typography>
                            </Stack>
                          </Stack>

                          {/* Location */}
                          {meeting?.location && (
                            <Stack direction="row" alignItems="center" spacing={0.5}>
                              <LocationIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
                              <Typography variant="body2" color="text.secondary">
                                {meeting.location}
                              </Typography>
                            </Stack>
                          )}

                          {/* Participants count */}
                          <Stack direction="row" alignItems="center" spacing={1}>
                            <Typography variant="body2" color="text.disabled">
                              • {participantCount} participant{participantCount !== 1 ? 's' : ''}
                            </Typography>
                          </Stack>

                          {/* Description */}
                          {meeting?.description && (
                            <Typography variant="body2" color="text.secondary" sx={{ 
                              display: '-webkit-box',
                              WebkitLineClamp: 2,
                              WebkitBoxOrient: 'vertical',
                              overflow: 'hidden',
                            }}>
                              {meeting.description}
                            </Typography>
                          )}

                          {/* Participants List */}
                          {participantEmails.length > 0 && (
                            <Box>
                              <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                                <EmailIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
                                <Typography variant="caption" color="text.secondary">
                                  Invited participants:
                                </Typography>
                              </Stack>
                              <Box sx={{ pl: 3 }}>
                                <Typography variant="caption" color="text.secondary">
                                  {participantEmails.slice(0, 2).join(', ')}
                                  {participantEmails.length > 2 && ` +${participantEmails.length - 2} more`}
                                </Typography>
                              </Box>
                            </Box>
                          )}

                          {/* Participants Avatars */}
                          {participantEmails.length > 0 && (
                            <Stack direction="row" alignItems="center" spacing={2}>
                              <AvatarGroup max={4} sx={{ '& .MuiAvatar-root': { width: 28, height: 28, fontSize: '0.75rem' } }}>
                                {participantDetails.length > 0 
                                  ? participantDetails.slice(0, 4).map((participant, index) => {
                                      const displayName = participant?.name || participant?.email || 'Unknown';
                                      return (
                                        <Tooltip key={index} title={displayName}>
                                          <Avatar>{getFirstChar(displayName)}</Avatar>
                                        </Tooltip>
                                      );
                                    })
                                  : participantEmails.slice(0, 4).map((email, index) => {
                                      const displayEmail = email || 'Unknown';
                                      return (
                                        <Tooltip key={index} title={displayEmail}>
                                          <Avatar>{getFirstChar(displayEmail)}</Avatar>
                                        </Tooltip>
                                      );
                                    })
                                }
                              </AvatarGroup>
                            </Stack>
                          )}

                          {/* Features */}
                          <Stack direction="row" spacing={1} flexWrap="wrap">
                            {(meeting?.Waiting_Room_Enabled || meeting?.settings_waiting_room) && (
                              <Chip label="Waiting Room" size="small" variant="outlined" />
                            )}
                            {(meeting?.Is_Recording_Enabled || meeting?.settings_recording) && (
                              <Chip label="Recording" size="small" variant="outlined" color="error" />
                            )}
                            {(meeting?.settings_require_password) && (
                              <Chip 
                                label="Password Protected" 
                                size="small" 
                                variant="outlined" 
                                color="warning"
                                icon={<LockIcon />}
                              />
                            )}
                            <Chip 
                              label={`ID: ${meetingId}`} 
                              size="small" 
                              variant="outlined" 
                              sx={{ fontSize: '0.7rem' }}
                            />
                          </Stack>
                        </Stack>
                      </CardContent>

                      <Divider />

                      <CardActions sx={{ justifyContent: 'space-between', px: 2 }}>
                        <Button
                          size="small"
                          startIcon={<VideoIcon />}
                          onClick={() => handleJoinMeeting(meeting)}
                          disabled={status.text === 'Completed'}
                          sx={{ textTransform: 'none' }}
                        >
                          {status.text === 'Completed' ? 'Completed' : 'Join Meeting'}
                        </Button>

                        {isHost && (
                          <Button
                            size="small"
                            startIcon={<EditIcon />}
                            onClick={() => handleEdit(meeting)}
                            sx={{ textTransform: 'none' }}
                          >
                            Edit
                          </Button>
                        )}
                      </CardActions>
                    </Card>
                  </Grid>
                );
              })}
            </Grid>
          )}

          {/* Empty State */}
          {filteredMeetings.length === 0 && !loading && (
            <Box
              sx={{
                textAlign: 'center',
                py: 8,
              }}
            >
              <ScheduleIcon sx={{ fontSize: 64, color: 'text.disabled', mb: 2 }} />
              <Typography variant="h6" color="text.secondary" mb={1}>
                No scheduled meetings found
              </Typography>
              <Typography variant="body2" color="text.disabled" mb={3}>
                {searchQuery ? 'Try adjusting your search criteria' : 
                 filterBy === 'hosting' ? 'You are not hosting any meetings' :
                 filterBy === 'invited' ? 'You have not been invited to any meetings' :
                 'Schedule your first meeting to get started'}
              </Typography>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={handleScheduleNew}
              >
                Schedule Meeting
              </Button>
            </Box>
          )}

          {/* Floating Action Button */}
          <Fab
            color="primary"
            sx={{
              position: 'fixed',
              bottom: 24,
              right: 24,
              zIndex: 1000,
            }}
            onClick={handleScheduleNew}
          >
            <AddIcon />
          </Fab>
        </Container>

        {/* Context Menu */}
        <Menu
          anchorEl={menuAnchor}
          open={Boolean(menuAnchor)}
          onClose={handleMenuClose}
        >
          <MenuItem onClick={() => {
            const meeting = upcomingMeetings.find(m => (m?.ID || m?.Meeting_ID) === selectedMeetingId);
            if (meeting) handleView(meeting);
          }}>
            <EventIcon sx={{ mr: 1 }} />
            View Details
          </MenuItem>
          
          {(() => {
            const meeting = upcomingMeetings.find(m => (m?.ID || m?.Meeting_ID) === selectedMeetingId);
            const isCurrentUserHost = meeting?.Host_ID === user?.id || meeting?.is_host;
            
            if (!isCurrentUserHost) return null;
            
            return (
              <>
                <MenuItem onClick={() => {
                  if (meeting) handleEdit(meeting);
                }}>
                  <EditIcon sx={{ mr: 1 }} />
                  Edit Meeting
                </MenuItem>
                <MenuItem 
                  onClick={() => handleDelete(selectedMeetingId)}
                  sx={{ color: 'error.main' }}
                >
                  <DeleteIcon sx={{ mr: 1 }} />
                  Delete Meeting
                </MenuItem>
              </>
            );
          })()}
        </Menu>

        {/* Delete Confirmation Dialog */}
        <Dialog 
          open={deleteDialogOpen} 
          onClose={() => !deleteLoading && setDeleteDialogOpen(false)}
          maxWidth="sm"
          fullWidth
        >
          <DialogTitle>
            <Stack direction="row" alignItems="center" spacing={1}>
              <DeleteIcon color="error" />
              <Typography variant="h6">Delete Meeting</Typography>
            </Stack>
          </DialogTitle>
          <DialogContent>
            <Typography>
              Are you sure you want to delete <strong>"{meetingToDelete?.Meeting_Name || meetingToDelete?.title}"</strong>?
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              This action cannot be undone and will permanently remove the meeting from your schedule.
            </Typography>
          </DialogContent>
          <DialogActions>
            <Button 
              onClick={() => setDeleteDialogOpen(false)}
              disabled={deleteLoading}
            >
              Cancel
            </Button>
            <Button 
              onClick={confirmDelete} 
              color="error" 
              variant="contained"
              disabled={deleteLoading}
              startIcon={deleteLoading ? <RefreshIcon sx={{ animation: 'spin 1s linear infinite' }} /> : <DeleteIcon />}
            >
              {deleteLoading ? 'Deleting...' : 'Delete Meeting'}
            </Button>
          </DialogActions>
        </Dialog>

        {/* Snackbar */}
        <Snackbar
          open={snackbar.open}
          autoHideDuration={6000}
          onClose={handleCloseSnackbar}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        >
          <Alert 
            onClose={handleCloseSnackbar} 
            severity={snackbar.severity}
            variant="filled"
            sx={{ width: '100%' }}
          >
            {snackbar.message}
          </Alert>
        </Snackbar>

        {/* Meeting Details Dialog */}
        <Dialog
          open={viewDialogOpen}
          onClose={() => {
            setViewDialogOpen(false);
            setSelectedMeetingForView(null);
          }}
          maxWidth="md"
          fullWidth
          PaperProps={{
            sx: { borderRadius: 3, maxHeight: '90vh' }
          }}
        >
          <DialogTitle
            sx={{
              background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
              color: 'white',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center'
            }}
          >
            <Stack direction="row" alignItems="center" spacing={2}>
              <EventDateIcon />
              <Typography variant="h6" fontWeight={600}>
                Meeting Details
              </Typography>
            </Stack>
            <IconButton 
              onClick={() => {
                setViewDialogOpen(false);
                setSelectedMeetingForView(null);
              }}
              sx={{ color: 'white' }}
            >
              <CloseIcon />
            </IconButton>
          </DialogTitle>

          <DialogContent sx={{ p: 0 }}>
            {selectedMeetingForView && (
              <Box>
                {/* Meeting Header */}
                <Box sx={{ p: 3, bgcolor: 'grey.50' }}>
                  <Typography variant="h5" fontWeight={700} gutterBottom>
                    {selectedMeetingForView.title}
                  </Typography>
                  
                  <Grid container spacing={2} sx={{ mb: 2 }}>
                    <Grid item xs={12} md={4}>
                      <Card sx={{ p: 2, bgcolor: 'success.50', border: '1px solid', borderColor: 'success.200' }}>
                        <Stack direction="row" alignItems="center" spacing={1}>
                          <StartIcon sx={{ color: 'success.main' }} />
                          <Box>
                            <Typography variant="subtitle2" color="success.dark" fontWeight={600}>
                              Meeting Start
                            </Typography>
                            <Typography variant="body1" fontWeight={500}>
                              {getMeetingTimes(selectedMeetingForView).start}
                            </Typography>
                          </Box>
                        </Stack>
                      </Card>
                    </Grid>
                    
                    <Grid item xs={12} md={4}>
                      <Card sx={{ p: 2, bgcolor: 'warning.50', border: '1px solid', borderColor: 'warning.200' }}>
                        <Stack direction="row" alignItems="center" spacing={1}>
                          <EndIcon sx={{ color: 'warning.main' }} />
                          <Box>
                            <Typography variant="subtitle2" color="warning.dark" fontWeight={600}>
                              Session End
                            </Typography>
                            <Typography variant="body1" fontWeight={500}>
                              {getMeetingTimes(selectedMeetingForView).end}
                            </Typography>
                          </Box>
                        </Stack>
                      </Card>
                    </Grid>

                    <Grid item xs={12} md={4}>
                      <Card sx={{ p: 2, bgcolor: 'error.50', border: '1px solid', borderColor: 'error.200' }}>
                        <Stack direction="row" alignItems="center" spacing={1}>
                          <EventDateIcon sx={{ color: 'error.main' }} />
                          <Box>
                            <Typography variant="subtitle2" color="error.dark" fontWeight={600}>
                              Final End Date
                            </Typography>
                            <Typography variant="body1" fontWeight={500}>
                              {(() => {
                                const meetingTimes = getMeetingTimes(selectedMeetingForView);
                                if (meetingTimes.seriesEndDate && meetingTimes.finalEndDate) {
                                  return meetingTimes.seriesEndDate;
                                } else if (selectedMeetingForView?.end_date) {
                                  return formatDateTime(selectedMeetingForView.end_date);
                                } else if (selectedMeetingForView?.recurrence_end_date) {
                                  return formatDateTime(selectedMeetingForView.recurrence_end_date);
                                } else {
                                  return meetingTimes.end;
                                }
                              })()}
                            </Typography>
                            {getMeetingTimes(selectedMeetingForView).isRecurring && (
                              <Typography variant="caption" color="error.main">
                                (Recurring Series)
                              </Typography>
                            )}
                          </Box>
                        </Stack>
                      </Card>
                    </Grid>
                  </Grid>

                  <Stack direction="row" spacing={1} flexWrap="wrap">
                    <Chip 
                      label={`${selectedMeetingForView.duration || 60} minutes`} 
                      size="small" 
                      color="primary"
                      variant="outlined"
                    />
                    <Chip 
                      label={selectedMeetingForView.timezone || 'Asia/Kolkata'} 
                      size="small" 
                      variant="outlined"
                    />
                  </Stack>
                </Box>

                <Divider />

                {/* Meeting Info */}
                <Box sx={{ p: 3 }}>
                  <Grid container spacing={3}>
                    {/* Left Column */}
                    <Grid item xs={12} md={6}>
                      {/* Organizer */}
                      <Box sx={{ mb: 3 }}>
                        <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                          <PersonIcon sx={{ mr: 1 }} />
                          Organizer
                        </Typography>
                        <Card sx={{ p: 2, bgcolor: 'grey.50' }}>
                          <Stack direction="row" alignItems="center" spacing={2}>
                            <Avatar sx={{ bgcolor: theme.palette.primary.main }}>
                              {getFirstChar(selectedMeetingForView.organizer)}
                            </Avatar>
                            <Box>
                              <Typography variant="subtitle1" fontWeight={600}>
                                {selectedMeetingForView.organizer}
                              </Typography>
                              {selectedMeetingForView.organizerEmail && (
                                <Typography variant="body2" color="text.secondary">
                                  {selectedMeetingForView.organizerEmail}
                                </Typography>
                              )}
                            </Box>
                          </Stack>
                        </Card>
                      </Box>

                      {/* Location */}
                      {selectedMeetingForView.location && (
                        <Box sx={{ mb: 3 }}>
                          <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                            <LocationIcon sx={{ mr: 1 }} />
                            Location
                          </Typography>
                          <Card sx={{ p: 2, bgcolor: 'grey.50' }}>
                            <Typography variant="body1">
                              {selectedMeetingForView.location}
                            </Typography>
                          </Card>
                        </Box>
                      )}

                      {/* Description */}
                      {selectedMeetingForView.description && (
                        <Box sx={{ mb: 3 }}>
                          <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                            <EventDateIcon sx={{ mr: 1 }} />
                            Description
                          </Typography>
                          <Card sx={{ p: 2, bgcolor: 'grey.50' }}>
                            <Typography variant="body1">
                              {selectedMeetingForView.description}
                            </Typography>
                          </Card>
                        </Box>
                      )}
                    </Grid>

                    {/* Right Column */}
                    <Grid item xs={12} md={6}>
                      {/* Participants */}
                      <Box sx={{ mb: 3 }}>
                        <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                          <PeopleIcon sx={{ mr: 1 }} />
                          Participants ({selectedMeetingForView.participants?.length || 0})
                        </Typography>
                        
                        {selectedMeetingForView.participants && selectedMeetingForView.participants.length > 0 ? (
                          <Card sx={{ bgcolor: 'grey.50' }}>
                            <Box sx={{ maxHeight: 300, overflow: 'auto' }}>
                              {selectedMeetingForView.participants.map((participant, index) => (
                                <Box key={index}>
                                  <Box sx={{ p: 2, display: 'flex', alignItems: 'center', gap: 2 }}>
                                    <Avatar sx={{ width: 32, height: 32, bgcolor: 'primary.main' }}>
                                      {getFirstChar(participant.name)}
                                    </Avatar>
                                    <Box flex={1}>
                                      <Typography variant="subtitle2" fontWeight={600}>
                                        {participant.name}
                                      </Typography>
                                      <Typography variant="body2" color="text.secondary">
                                        {participant.email}
                                      </Typography>
                                    </Box>
                                  </Box>
                                  {index < selectedMeetingForView.participants.length - 1 && <Divider />}
                                </Box>
                              ))}
                            </Box>
                          </Card>
                        ) : (
                          <Card sx={{ p: 3, bgcolor: 'grey.50', textAlign: 'center' }}>
                            <PeopleIcon sx={{ fontSize: 48, color: 'text.disabled', mb: 1 }} />
                            <Typography variant="body2" color="text.secondary">
                              No participants invited
                            </Typography>
                          </Card>
                        )}
                      </Box>

                      {/* Meeting Settings */}
                      <Box>
                        <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
                          <ScheduleIcon sx={{ mr: 1 }} />
                          Meeting Settings
                        </Typography>
                        <Stack spacing={1}>
                          {selectedMeetingForView.settings?.waitingRoom && (
                            <Chip label="Waiting Room Enabled" size="small" color="primary" variant="outlined" />
                          )}
                          {selectedMeetingForView.settings?.recording && (
                            <Chip label="Recording Enabled" size="small" color="error" variant="outlined" />
                          )}
                          {selectedMeetingForView.settings?.allowChat && (
                            <Chip label="Chat Allowed" size="small" variant="outlined" />
                          )}
                          {selectedMeetingForView.settings?.allowScreenShare && (
                            <Chip label="Screen Share Allowed" size="small" variant="outlined" />
                          )}
                          {selectedMeetingForView.settings?.requirePassword && (
                            <Chip label="Password Protected" size="small" color="warning" variant="outlined" icon={<LockIcon />} />
                          )}
                        </Stack>
                      </Box>
                    </Grid>
                  </Grid>
                </Box>
              </Box>
            )}
          </DialogContent>

          <DialogActions sx={{ p: 3, bgcolor: 'grey.50' }}>
            <Button
              onClick={() => {
                if (selectedMeetingForView?.meetingURL) {
                  navigator.clipboard.writeText(selectedMeetingForView.meetingURL);
                  setSnackbar({
                    open: true,
                    message: 'Meeting link copied to clipboard!',
                    severity: 'success'
                  });
                }
              }}
              startIcon={<EmailIcon />}
            >
              Copy Link
            </Button>
            {selectedMeetingForView?.hostId === user?.id && (
              <>
                <Button
                  startIcon={<EditIcon />}
                  onClick={() => {
                    setViewDialogOpen(false);
                    const originalMeeting = upcomingMeetings.find(m => 
                      (m?.ID || m?.Meeting_ID) === selectedMeetingForView?.id
                    );
                    if (originalMeeting) {
                      handleEdit(originalMeeting);
                    }
                  }}
                  color="primary"
                >
                  Edit
                </Button>
                
                <Button
                  startIcon={<DeleteIcon />}
                  onClick={() => {
                    setViewDialogOpen(false);
                    handleDelete(selectedMeetingForView?.id);
                  }}
                  color="error"
                >
                  Delete
                </Button>
              </>
            )}

            <Button onClick={() => {
              setViewDialogOpen(false);
              setSelectedMeetingForView(null);
            }}>
              Close
            </Button>
          </DialogActions>
        </Dialog>

        {/* NEW: Schedule Meeting Dialog */}
        {scheduleDialogOpen && (
          <ScheduleMeeting
            meeting={editingMeeting}
            onClose={handleDialogClose}
            onSave={handleMeetingSave}
          />
        )}
      </Box>
    </DashboardLayout>
  );
};

export default SchedulePage;