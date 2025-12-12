// Enhanced RecordingsList.jsx with trash functionality + Meeting Type Filter
import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Chip,
  IconButton,
  Menu,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Stack,
  Avatar,
  Tooltip,
  Alert,
  LinearProgress,
  Divider,
  InputAdornment,
  FormControl,
  InputLabel,
  Select,
  ListItemIcon,
  ListItemText,
  Tabs,
  Tab,
  Badge
} from '@mui/material';
import {
  PlayArrow as PlayIcon,
  Download as DownloadIcon,
  Share as ShareIcon,
  Delete as DeleteIcon,
  MoreVert as MoreIcon,
  Search as SearchIcon,
  FilterList as FilterIcon,
  VideoFile as VideoIcon,
  Schedule as ScheduleIcon,
  People as PeopleIcon,
  Storage as StorageIcon,
  Edit as EditIcon,
  Visibility as ViewIcon,
  CloudDownload as CloudIcon,
  Subtitles as SubtitlesIcon,
  Block as BlockIcon,
  Security as SecurityIcon,
  SubtitlesOff as SubtitlesOffIcon,
  ClosedCaption as ClosedCaptionIcon,
  AutoFixHigh as GenerateIcon,
  GetApp as GetAppIcon,
  // NEW: Trash-related icons
  DeleteOutlined as TrashIcon,
  RestoreFromTrash as RestoreIcon,
  DeleteForever as PermanentDeleteIcon,
  CleaningServices as EmptyTrashIcon,
  Info as InfoIcon
} from '@mui/icons-material';
import { useTheme, alpha } from '@mui/material/styles';
import { useAuth } from '../hooks/useAuth';
import { recordingsAPI } from '../services/api';

const RecordingsList = ({ 
  recordings = [], 
  trashedRecordings = [],
  trashStats = { total_count: 0, total_size: 0 },
  onPlay, 
  onDelete, 
  onShare, 
  onDownload,
  onMoveToTrash,
  onRestoreFromTrash,
  onPermanentDelete,
  onEmptyTrash,
  loading = false,
  showTrash = false
}) => {
  const theme = useTheme();
  
  const { user: authUser } = useAuth();
  
  // Get user data with fallback to localStorage
  const currentUser = authUser || {
    email: localStorage.getItem('user_email') || '',
    id: localStorage.getItem('user_id') || '',
    name: localStorage.getItem('user_name') || 'User'
  };

  // NEW: Tab state for Active/Trash views
  const [currentTab, setCurrentTab] = useState(0); // 0 = Active, 1 = Trash

  const [filteredRecordings, setFilteredRecordings] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [sortBy, setSortBy] = useState('newest');
  const [selectedRecording, setSelectedRecording] = useState(null);
  const [anchorEl, setAnchorEl] = useState(null);
  const [shareDialog, setShareDialog] = useState(false);
  const [deleteDialog, setDeleteDialog] = useState(false);
  const [restoreDialog, setRestoreDialog] = useState(false);
  const [permanentDeleteDialog, setPermanentDeleteDialog] = useState(false);
  const [emptyTrashDialog, setEmptyTrashDialog] = useState(false);
  const [editDialog, setEditDialog] = useState(false);
  const [meetingTypeFilter, setMeetingTypeFilter] = useState('all'); // ✅ NEW: Meeting type filter
 console.log('🎬 RecordingsList rendered with:', {
  recordingsCount: recordings.length,
  trashedCount: trashedRecordings.length,
  loading
});
  // Helper function to check if current user is host of a recording
  const isUserHostOfRecording = (recording) => {
    const recordingUserId = String(recording.user_id || recording.host_id || '');
    const currentUserId = String(currentUser.id || '');
    
    console.log('Host check in RecordingsList:', {
      recordingUserId,
      currentUserId,
      isHost: recordingUserId === currentUserId
    });
    
    return recordingUserId === currentUserId;
  };

  // Get current recordings list based on active tab
  const getCurrentRecordings = () => {
    return currentTab === 0 ? recordings : trashedRecordings;
  };

  // Transform recordings data and add host check
  const transformedRecordings = getCurrentRecordings().map(recording => ({
    ...recording,
    isUserHost: isUserHostOfRecording(recording),
    is_trashed: currentTab === 1 // Mark as trashed if in trash tab
  }));

  // Filter and search functionality
  useEffect(() => {
    let filtered = transformedRecordings;

    // Apply search filter
    if (searchTerm) {
      filtered = filtered.filter(recording => 
        recording.fileName?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        recording.meetingName?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        recording.meeting_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        recording.file_name?.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }

    // Apply type filter
    if (filterType !== 'all') {
      if (filterType === 'myRecordings') {
        filtered = filtered.filter(recording => recording.isUserHost);
      } else {
        filtered = filtered.filter(recording => recording.meetingType === filterType);
      }
    }

    // ✅ NEW: Apply meeting type filter
    if (meetingTypeFilter !== 'all') {
      filtered = filtered.filter(recording => 
        recording.meeting_type === meetingTypeFilter
      );
    }

    // Apply sorting
    filtered.sort((a, b) => {
      switch (sortBy) {
        case 'newest':
          return new Date(b.created_at || b.createdAt || b.trashed_at) - new Date(a.created_at || a.createdAt || a.trashed_at);
        case 'oldest':
          return new Date(a.created_at || a.createdAt || a.trashed_at) - new Date(b.created_at || b.createdAt || b.trashed_at);
        case 'duration':
          return (b.duration || '').localeCompare(a.duration || '');
        case 'size':
          return parseFloat(b.file_size || b.fileSize || '0') - parseFloat(a.file_size || a.fileSize || '0');
        default:
          return 0;
      }
    });

    setFilteredRecordings(filtered);
  }, [transformedRecordings, searchTerm, filterType, sortBy, meetingTypeFilter]); // ✅ Added meetingTypeFilter

  // Tab change handler
  const handleTabChange = (event, newValue) => {
    setCurrentTab(newValue);
    setSearchTerm(''); // Clear search when switching tabs
    setFilterType('all'); // Reset filter when switching tabs
    setMeetingTypeFilter('all'); // ✅ NEW: Reset meeting type filter when switching tabs
  };

  const handleMenuClick = (event, recording) => {
    event.stopPropagation();
    setAnchorEl(event.currentTarget);
    setSelectedRecording(recording);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
    setSelectedRecording(null);
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'processed': return 'success';
      case 'processing': return 'warning';
      case 'failed': return 'error';
      case 'trashed': return 'error';
      default: return 'default';
    }
  };

  const getTypeColor = (type) => {
    switch (type) {
      case 'instant': return 'primary';
      case 'scheduled': return 'secondary';
      case 'calendar': return 'info';
      default: return 'default';
    }
  };

  // Handle delete/move to trash
  const handleDelete = async () => {
    if (!selectedRecording) return;
    
    try {
      console.log('Moving recording to trash:', selectedRecording);
      
      if (!selectedRecording.isUserHost) {
        alert('Permission denied: Only the meeting host can delete this recording');
        handleMenuClose();
        return;
      }

      await onMoveToTrash?.(selectedRecording, {
        user_id: currentUser.id,
        email: currentUser.email
      });
      
      console.log('Recording moved to trash successfully');
      alert('Recording moved to trash successfully!');
      
    } catch (error) {
      console.error('Move to trash failed:', error);
      
      if (error.response?.status === 403) {
        alert('Permission denied: Only the meeting host can delete this recording');
      } else if (error.response?.status === 404) {
        alert('Recording not found');
      } else {
        alert('Failed to move recording to trash: ' + (error.message || 'Unknown error'));
      }
    } finally {
      setDeleteDialog(false);
      handleMenuClose();
    }
  };

  // Handle restore from trash
  const handleRestore = async () => {
    if (!selectedRecording) return;
    
    try {
      console.log('Restoring recording from trash:', selectedRecording);
      
      await onRestoreFromTrash?.(selectedRecording, {
        user_id: currentUser.id,
        email: currentUser.email
      });
      
      console.log('Recording restored successfully');
      alert('Recording restored successfully!');
      
    } catch (error) {
      console.error('Restore failed:', error);
      alert('Failed to restore recording: ' + (error.message || 'Unknown error'));
    } finally {
      setRestoreDialog(false);
      handleMenuClose();
    }
  };

  // Handle permanent delete
  const handlePermanentDelete = async () => {
    if (!selectedRecording) return;
    
    try {
      console.log('Permanently deleting recording:', selectedRecording);
      
      await onPermanentDelete?.(selectedRecording, {
        user_id: currentUser.id,
        email: currentUser.email
      });
      
      console.log('Recording permanently deleted');
      alert('Recording permanently deleted!');
      
    } catch (error) {
      console.error('Permanent delete failed:', error);
      alert('Failed to permanently delete recording: ' + (error.message || 'Unknown error'));
    } finally {
      setPermanentDeleteDialog(false);
      handleMenuClose();
    }
  };

  // Handle empty trash
  const handleEmptyTrash = async () => {
    try {
      await onEmptyTrash?.();
      alert('Trash emptied successfully!');
      setEmptyTrashDialog(false);
    } catch (error) {
      console.error('Empty trash failed:', error);
      alert('Failed to empty trash: ' + (error.message || 'Unknown error'));
    }
  };

  // Generate subtitle handlers
  const handleGenerateSubtitles = async (recording) => {
    try {
      console.log('Generating subtitles for:', recording.meeting_name);
      
      await recordingsAPI.generateSubtitles(recording.id, {
        language: 'en',
        format: 'webvtt',
        accuracy: 'high'
      });
      
      console.log('Subtitle generation started');
      alert('Subtitle generation started! This may take a few minutes.');
      
    } catch (err) {
      console.error('Subtitle generation failed:', err);
      alert('Failed to generate subtitles: ' + err.message);
    }
    handleMenuClose();
  };

  const handleDownloadSubtitles = async (recording, format = 'srt') => {
    try {
      console.log('Downloading subtitles for:', recording.meeting_name);
      
      const fileName = `${recording.meeting_name || recording.file_name}_subtitles.${format}`;
      await recordingsAPI.downloadSubtitles(recording.id, format, fileName);
      
      console.log('Subtitle download initiated');
      
    } catch (err) {
      console.error('Subtitle download failed:', err);
      alert('Failed to download subtitles: ' + err.message);
    }
    handleMenuClose();
  };

  // ✅ NEW: Handle meeting type filter change
  const handleMeetingTypeChange = (e) => {
    setMeetingTypeFilter(e.target.value);
  };

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography variant="h5" gutterBottom>Loading recordings...</Typography>
        <LinearProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3, maxWidth: 1200, mx: 'auto' }}>
      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', color: theme.palette.primary.main }}>
          Meeting Recordings
        </Typography>
        <Typography variant="subtitle1" color="text.secondary">
          Manage and view your recorded meetings
        </Typography>
      </Box>

      {/* NEW: Tabs for Active/Trash views */}
      {showTrash && (
        <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 3 }}>
          <Tabs value={currentTab} onChange={handleTabChange}>
            <Tab 
              label={
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <VideoIcon />
                  Active Recordings ({recordings.length})
                </Box>
              } 
            />
            <Tab 
              label={
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Badge badgeContent={trashStats.total_count} color="error">
                    <TrashIcon />
                  </Badge>
                  Trash ({trashStats.total_count})
                </Box>
              } 
            />
          </Tabs>
        </Box>
      )}

      {/* NEW: Trash statistics and actions */}
      {currentTab === 1 && trashStats.total_count > 0 && (
        <Alert 
          severity="info" 
          sx={{ mb: 2 }}
          action={
            <Button 
              size="small" 
              startIcon={<EmptyTrashIcon />}
              onClick={() => setEmptyTrashDialog(true)}
              color="error"
              variant="outlined"
            >
              Empty Trash
            </Button>
          }
        >
          <InfoIcon sx={{ mr: 1 }} />
          {trashStats.total_count} recordings in trash • 
          Total size: {(trashStats.total_size / (1024 * 1024)).toFixed(2)} MB •
          Recordings will be permanently deleted after 15 days
        </Alert>
      )}

      {/* Filters and Search */}
      <Card sx={{ mb: 3, background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)' }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                placeholder={`Search ${currentTab === 0 ? 'active' : 'trashed'} recordings...`}
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon />
                    </InputAdornment>
                  ),
                }}
                sx={{ backgroundColor: 'white', borderRadius: 1 }}
              />
            </Grid>
            
            <Grid item xs={12} md={2}>
              <FormControl fullWidth sx={{ backgroundColor: 'white', borderRadius: 1 }}>
                <InputLabel>Filter Type</InputLabel>
                <Select
                  value={filterType}
                  label="Filter Type"
                  onChange={(e) => setFilterType(e.target.value)}
                >
                  <MenuItem value="all">All</MenuItem>
                  <MenuItem value="myRecordings">My Recordings</MenuItem>
                </Select>
              </FormControl>
            </Grid>

            {/* ✅ NEW: Meeting Type Filter */}
            <Grid item xs={12} md={2}>
              <FormControl fullWidth sx={{ backgroundColor: 'white', borderRadius: 1 }}>
                <InputLabel>Meeting Type</InputLabel>
                <Select
                  value={meetingTypeFilter}
                  label="Meeting Type"
                  onChange={handleMeetingTypeChange}
                >
                  <MenuItem value="all">All Types</MenuItem>
                  <MenuItem value="CalendarMeeting">Calendar</MenuItem>
                  <MenuItem value="ScheduleMeeting">Scheduled</MenuItem>
                  <MenuItem value="InstantMeeting">Instant</MenuItem>
                </Select>
              </FormControl>
            </Grid>

            <Grid item xs={12} md={2}>
              <FormControl fullWidth sx={{ backgroundColor: 'white', borderRadius: 1 }}>
                <InputLabel>Sort By</InputLabel>
                <Select
                  value={sortBy}
                  label="Sort By"
                  onChange={(e) => setSortBy(e.target.value)}
                >
                  <MenuItem value="newest">Newest</MenuItem>
                  <MenuItem value="oldest">Oldest</MenuItem>
                  <MenuItem value="duration">Duration</MenuItem>
                  <MenuItem value="size">Size</MenuItem>
                </Select>
              </FormControl>
            </Grid>
          </Grid>

          {/* ✅ NEW: Recording count with meeting type info */}
          <Box sx={{ mt: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="body2" color="text.secondary">
              {filteredRecordings.length} recording(s) found
              {meetingTypeFilter !== 'all' && (
                <Chip 
                  label={meetingTypeFilter} 
                  size="small" 
                  color="primary" 
                  sx={{ ml: 1 }}
                  onDelete={() => setMeetingTypeFilter('all')}
                />
              )}
            </Typography>
          </Box>
        </CardContent>
      </Card>

      {/* Recordings Grid */}
      {filteredRecordings.length === 0 ? (
        <Card sx={{ textAlign: 'center', py: 8 }}>
          <CardContent>
            {currentTab === 0 ? (
              <>
                <VideoIcon sx={{ fontSize: 80, color: 'text.secondary', mb: 2 }} />
                <Typography variant="h6" color="text.secondary" gutterBottom>
                  No recordings found
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {searchTerm || filterType !== 'all' || meetingTypeFilter !== 'all'
                    ? 'Try adjusting your search or filter criteria' 
                    : 'Start recording meetings to see them here'}
                </Typography>
              </>
            ) : (
              <>
                <TrashIcon sx={{ fontSize: 80, color: 'text.secondary', mb: 2 }} />
                <Typography variant="h6" color="text.secondary" gutterBottom>
                  Trash is empty
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Deleted recordings will appear here
                </Typography>
              </>
            )}
          </CardContent>
        </Card>
      ) : (
        <Grid container spacing={3}>
          {filteredRecordings.map((recording) => (
            <Grid item xs={12} md={6} lg={4} key={recording.id || recording._id}>
              <Card 
                sx={{ 
                  height: '100%',
                  display: 'flex',
                  flexDirection: 'column',
                  cursor: recording.is_trashed ? 'default' : 'pointer',
                  opacity: recording.is_trashed ? 0.7 : 1,
                  transition: 'all 0.3s ease',
                  '&:hover': {
                    transform: recording.is_trashed ? 'none' : 'translateY(-4px)',
                    boxShadow: recording.is_trashed ? theme.shadows[1] : theme.shadows[8],
                  },
                  border: recording.is_trashed 
                    ? `1px solid ${theme.palette.error.light}` 
                    : `1px solid ${alpha(theme.palette.divider, 0.1)}`,
                }}
                onClick={() => !recording.is_trashed && onPlay?.(recording)}
              >
                {/* Thumbnail/Video Preview */}
                <Box 
                  sx={{ 
                    height: 160,
                    background: recording.is_trashed 
                      ? `linear-gradient(135deg, ${alpha(theme.palette.error.main, 0.1)} 0%, ${alpha(theme.palette.error.main, 0.2)} 100%)`
                      : 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    position: 'relative'
                  }}
                >
                  <Avatar 
                    sx={{ 
                      width: 60, 
                      height: 60, 
                      backgroundColor: recording.is_trashed 
                        ? 'rgba(244, 67, 54, 0.2)'
                        : 'rgba(255, 255, 255, 0.2)',
                      backdropFilter: 'blur(10px)'
                    }}
                  >
                    <PlayIcon sx={{ fontSize: 30, color: 'white' }} />
                  </Avatar>
                  
                  {/* Status Badge */}
                  <Chip
                    label={recording.is_trashed ? 'TRASHED' : (recording.status || 'processed')}
                    color={getStatusColor(recording.is_trashed ? 'trashed' : recording.status)}
                    size="small"
                    sx={{
                      position: 'absolute',
                      top: 8,
                      left: 8,
                      fontWeight: 'bold'
                    }}
                  />

                  {/* Host badge */}
                  {recording.isUserHost && (
                    <Chip
                      label="Host"
                      color="primary"
                      size="small"
                      icon={<SecurityIcon />}
                      sx={{
                        position: 'absolute',
                        top: 8,
                        right: 50,
                        fontWeight: 'bold',
                        fontSize: '0.7rem'
                      }}
                    />
                  )}

                  {/* ✅ NEW: Meeting Type Badge */}
                  {recording.meeting_type && (
                    <Chip
                      label={recording.meeting_type === 'CalendarMeeting' ? '📅 Calendar' : 
                             recording.meeting_type === 'ScheduleMeeting' ? '🗓️ Scheduled' : 
                             '⚡ Instant'}
                      size="small"
                      sx={{
                        position: 'absolute',
                        bottom: 40,
                        left: 8,
                        backgroundColor: 'rgba(255, 255, 255, 0.9)',
                        fontWeight: '600',
                        fontSize: '0.7rem'
                      }}
                    />
                  )}

                  {/* Duration Badge */}
                  <Chip
                    label={recording.duration || '0:00'}
                    sx={{
                      position: 'absolute',
                      bottom: 8,
                      right: 8,
                      backgroundColor: 'rgba(0, 0, 0, 0.7)',
                      color: 'white',
                      fontFamily: 'monospace'
                    }}
                  />

                  {/* NEW: Trash date overlay */}
                  {recording.is_trashed && recording.trashed_at && (
                    <Box
                      sx={{
                        position: 'absolute',
                        bottom: 0,
                        left: 0,
                        right: 0,
                        background: 'linear-gradient(transparent, rgba(244,67,54,0.8))',
                        color: 'white',
                        p: 1,
                        textAlign: 'center'
                      }}
                    >
                      <Typography variant="caption">
                        Deleted: {formatDate(recording.trashed_at)}
                      </Typography>
                    </Box>
                  )}
                </Box>

                <CardContent sx={{ flexGrow: 1, pb: 1 }}>
                  <Typography 
                    variant="h6" 
                    gutterBottom 
                    noWrap
                    sx={{ 
                      textDecoration: recording.is_trashed ? 'line-through' : 'none',
                      color: recording.is_trashed ? 'text.secondary' : 'text.primary'
                    }}
                  >
                    {recording.fileName || recording.meeting_name || recording.file_name || 'Meeting Recording'}
                  </Typography>
                  
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2, height: 40, overflow: 'hidden' }}>
                    {recording.meetingName || recording.meeting_name || 'No description available'}
                  </Typography>

                  <Stack spacing={1.5}>
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <ScheduleIcon fontSize="small" />
                        {formatDate(recording.createdAt || recording.created_at || recording.trashed_at || new Date())}
                      </Typography>
                      <Chip 
                        label={recording.meetingType || 'meeting'} 
                        color={getTypeColor(recording.meetingType)}
                        size="small"
                      />
                    </Box>

                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <PeopleIcon fontSize="small" />
                        {recording.participants || recording.participants_count || 0} participants
                      </Typography>
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <StorageIcon fontSize="small" />
                        {recording.fileSize || recording.file_size || 'Unknown size'}
                      </Typography>
                    </Box>

                    {/* Tags section with subtitle indicators */}
                    <Stack direction="row" spacing={1} flexWrap="wrap">
                      {recording.subtitles_available ? (
                        <Chip 
                          label="Subtitles" 
                          size="small" 
                          color="success" 
                          variant="outlined"
                          icon={<ClosedCaptionIcon />}
                        />
                      ) : (
                        <Chip 
                          label="No Subtitles" 
                          size="small" 
                          color="default" 
                          variant="outlined"
                          icon={<SubtitlesOffIcon />}
                        />
                      )}
                      
                      {(recording.transcriptionAvailable || recording.transcription_available) && (
                        <Chip 
                          label="Transcript" 
                          size="small" 
                          color="success" 
                          variant="outlined"
                          icon={<SubtitlesIcon />}
                        />
                      )}
                    </Stack>
                  </Stack>
                </CardContent>

                <Divider />

                {/* Action Buttons */}
                <Box sx={{ p: 1, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  {recording.is_trashed ? (
                    // Trash actions
                    <Stack direction="row" spacing={1}>
                      <Tooltip title="Restore Recording">
                        <Button
                          size="small"
                          startIcon={<RestoreIcon />}
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedRecording(recording);
                            setRestoreDialog(true);
                          }}
                          disabled={!recording.isUserHost}
                          sx={{ textTransform: 'none' }}
                        >
                          Restore
                        </Button>
                      </Tooltip>
                      
                      <Tooltip title="Delete Permanently">
                        <Button
                          size="small"
                          startIcon={<PermanentDeleteIcon />}
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedRecording(recording);
                            setPermanentDeleteDialog(true);
                          }}
                          disabled={!recording.isUserHost}
                          color="error"
                          sx={{ textTransform: 'none' }}
                        >
                          Delete Forever
                        </Button>
                      </Tooltip>
                    </Stack>
                  ) : (
                    // Active actions
                    <Stack direction="row" spacing={1}>
                      <Tooltip title="Play Recording">
                        <IconButton size="small" color="primary" onClick={(e) => {
                          e.stopPropagation();
                          onPlay?.(recording);
                        }}>
                          <PlayIcon />
                        </IconButton>
                      </Tooltip>
                      
                      <Tooltip title="Download">
                        <IconButton size="small" color="success" onClick={(e) => {
                          e.stopPropagation();
                          onDownload?.(recording);
                        }}>
                          <DownloadIcon />
                        </IconButton>
                      </Tooltip>
                      
                      <Tooltip title="Share">
                        <IconButton size="small" color="info" onClick={(e) => {
                          e.stopPropagation();
                          onShare?.(recording);
                        }}>
                          <ShareIcon />
                        </IconButton>
                      </Tooltip>
                    </Stack>
                  )}

                  <IconButton 
                    size="small"
                    onClick={(e) => handleMenuClick(e, recording)}
                  >
                    <MoreIcon />
                  </IconButton>
                </Box>
              </Card>
            </Grid>
          ))}
        </Grid>
      )}

      {/* Context Menu - Enhanced for trash support */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        transformOrigin={{ horizontal: 'right', vertical: 'top' }}
        anchorOrigin={{ horizontal: 'right', vertical: 'bottom' }}
      >
        {selectedRecording?.is_trashed ? (
          // Trash menu options
          [
            <MenuItem key="restore" onClick={() => {
              setRestoreDialog(true);
              handleMenuClose();
            }} disabled={!selectedRecording?.isUserHost}>
              <ListItemIcon><RestoreIcon fontSize="small" /></ListItemIcon>
              <ListItemText>Restore Recording</ListItemText>
            </MenuItem>,
            
            <MenuItem key="permanent-delete" onClick={() => {
              setPermanentDeleteDialog(true);
              handleMenuClose();
            }} disabled={!selectedRecording?.isUserHost} sx={{ color: 'error.main' }}>
              <ListItemIcon><PermanentDeleteIcon fontSize="small" color="error" /></ListItemIcon>
              <ListItemText>Delete Permanently</ListItemText>
            </MenuItem>
          ]
        ) : (
          // Active recording menu options
          [
            <MenuItem key="play" onClick={() => {
              onPlay?.(selectedRecording);
              handleMenuClose();
            }}>
              <ListItemIcon><PlayIcon fontSize="small" /></ListItemIcon>
              <ListItemText>Play Recording</ListItemText>
            </MenuItem>,
            
            <MenuItem key="rename" onClick={() => setEditDialog(true)}>
              <ListItemIcon><EditIcon fontSize="small" /></ListItemIcon>
              <ListItemText>Rename</ListItemText>
            </MenuItem>,
            
            <MenuItem key="share" onClick={() => setShareDialog(true)}>
              <ListItemIcon><ShareIcon fontSize="small" /></ListItemIcon>
              <ListItemText>Share Recording</ListItemText>
            </MenuItem>,
            
            <MenuItem key="download" onClick={() => {
              onDownload?.(selectedRecording);
              handleMenuClose();
            }}>
              <ListItemIcon><CloudIcon fontSize="small" /></ListItemIcon>
              <ListItemText>Download</ListItemText>
            </MenuItem>,
            
            <Divider key="divider" />,
            
            // Subtitle options
            (() => {
              if (selectedRecording?.subtitles_available) {
                return [
                  <MenuItem key="download-srt" onClick={() => handleDownloadSubtitles(selectedRecording, 'srt')}>
                    <ListItemIcon><GetAppIcon fontSize="small" /></ListItemIcon>
                    <ListItemText>Download SRT Subtitles</ListItemText>
                  </MenuItem>,
                  
                  <MenuItem key="download-webvtt" onClick={() => handleDownloadSubtitles(selectedRecording, 'webvtt')}>
                    <ListItemIcon><GetAppIcon fontSize="small" /></ListItemIcon>
                    <ListItemText>Download WebVTT Subtitles</ListItemText>
                  </MenuItem>
                ];
              } else {
                return (
                  <MenuItem key="generate-subtitles" onClick={() => handleGenerateSubtitles(selectedRecording)}>
                    <ListItemIcon><GenerateIcon fontSize="small" /></ListItemIcon>
                    <ListItemText>Generate Subtitles</ListItemText>
                  </MenuItem>
                );
              }
            })(),
            
            <Divider key="divider2" />,
            
            // Host-only delete option (now moves to trash)
            selectedRecording?.isUserHost && (
              <MenuItem key="delete" onClick={() => setDeleteDialog(true)} sx={{ color: 'error.main' }}>
                <ListItemIcon>
                  <DeleteIcon fontSize="small" color="error" />
                </ListItemIcon>
                <ListItemText>Move to Trash</ListItemText>
              </MenuItem>
            )
          ].flat().filter(Boolean)
        )}
      </Menu>

      {/* Dialog Components */}
      
      {/* Share Dialog */}
      <Dialog open={shareDialog} onClose={() => setShareDialog(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Share Recording</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Share Link"
            value="https://meetingapp.com/recording/abc123"
            InputProps={{
              readOnly: true,
              endAdornment: (
                <Button variant="contained" size="small">
                  Copy
                </Button>
              )
            }}
            sx={{ mb: 2 }}
          />
          <Alert severity="info">
            Anyone with this link can view the recording. Make sure to share it only with trusted individuals.
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShareDialog(false)}>Cancel</Button>
          <Button variant="contained">Share</Button>
        </DialogActions>
      </Dialog>

      {/* Move to Trash Confirmation Dialog */}
      <Dialog open={deleteDialog} onClose={() => setDeleteDialog(false)}>
        <DialogTitle>Move Recording to Trash?</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to move "{selectedRecording?.fileName || selectedRecording?.meeting_name}" to trash? 
            You can restore it later if needed.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialog(false)}>Cancel</Button>
          <Button color="error" variant="contained" onClick={handleDelete}>
            Move to Trash
          </Button>
        </DialogActions>
      </Dialog>

      {/* Restore Confirmation Dialog */}
      <Dialog open={restoreDialog} onClose={() => setRestoreDialog(false)}>
        <DialogTitle>Restore Recording?</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to restore "{selectedRecording?.fileName || selectedRecording?.meeting_name}"? 
            It will be moved back to your active recordings.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRestoreDialog(false)}>Cancel</Button>
          <Button color="success" variant="contained" onClick={handleRestore}>
            Restore
          </Button>
        </DialogActions>
      </Dialog>

      {/* Permanent Delete Confirmation Dialog */}
      <Dialog open={permanentDeleteDialog} onClose={() => setPermanentDeleteDialog(false)}>
        <DialogTitle sx={{ color: 'error.main' }}>Permanently Delete Recording?</DialogTitle>
        <DialogContent>
          <Alert severity="error" sx={{ mb: 2 }}>
            This action cannot be undone! The recording and all associated files will be permanently deleted.
          </Alert>
          <Typography>
            Are you sure you want to permanently delete "{selectedRecording?.fileName || selectedRecording?.meeting_name}"?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPermanentDeleteDialog(false)}>Cancel</Button>
          <Button color="error" variant="contained" onClick={handlePermanentDelete}>
            Delete Forever
          </Button>
        </DialogActions>
      </Dialog>

      {/* Empty Trash Confirmation Dialog */}
      <Dialog open={emptyTrashDialog} onClose={() => setEmptyTrashDialog(false)}>
        <DialogTitle sx={{ color: 'error.main' }}>Empty Trash?</DialogTitle>
        <DialogContent>
          <Alert severity="error" sx={{ mb: 2 }}>
            This will permanently delete ALL recordings in trash. This action cannot be undone!
          </Alert>
          <Typography>
            Are you sure you want to permanently delete all {trashStats.total_count} recordings in trash?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEmptyTrashDialog(false)}>Cancel</Button>
          <Button color="error" variant="contained" onClick={handleEmptyTrash}>
            Empty Trash
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RecordingsList;