// src/components/feedback/FeedbackHistory.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Card,
  CardContent,
  Rating,
  Chip,
  Stack,
  TextField,
  InputAdornment,
  IconButton,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
  CircularProgress,
  Alert,
  Divider,
  Button,
  Tooltip,
  Grid
} from '@mui/material';
import {
  Search as SearchIcon,
  FilterList as FilterIcon,
  Star as StarIcon,
  CalendarToday as CalendarIcon,
  Comment as CommentIcon,
  Category as CategoryIcon,
  Refresh as RefreshIcon,
  Sort as SortIcon
} from '@mui/icons-material';
import { getAllFeedback, getFeedbackById } from '../../services/feedbackAPI';
import { format } from 'date-fns';

const FEEDBACK_TYPE_COLORS = {
  General: 'primary',
  Technical: 'error',
  Content: 'success',
  Other: 'warning'
};

const FEEDBACK_TYPE_ICONS = {
  General: '💬',
  Technical: '🔧',
  Content: '📚',
  Other: '📝'
};

const FeedbackHistory = ({ userId, showAllFeedback = false }) => {
  const [feedbackList, setFeedbackList] = useState([]);
  const [filteredFeedback, setFilteredFeedback] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [filterType, setFilterType] = useState('All');
  const [filterRating, setFilterRating] = useState('All');
  const [sortBy, setSortBy] = useState('date_desc'); // date_desc, date_asc, rating_desc, rating_asc

  // Fetch feedback on component mount
  useEffect(() => {
    fetchFeedback();
  }, [userId, showAllFeedback]);

  // Apply filters whenever feedback or filters change
  useEffect(() => {
    applyFilters();
  }, [feedbackList, searchQuery, filterType, filterRating, sortBy]);

  const fetchFeedback = async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await getAllFeedback();
      
      if (result.success) {
        let feedback = result.data;

        // Filter by user if not showing all feedback
        if (!showAllFeedback && userId) {
          feedback = feedback.filter(fb => fb.User_ID === userId);
        }

        setFeedbackList(feedback);
      } else {
        setError(result.error || 'Failed to load feedback');
      }
    } catch (err) {
      setError('An error occurred while loading feedback');
    } finally {
      setLoading(false);
    }
  };

  const applyFilters = () => {
    let filtered = [...feedbackList];

    // Search filter
    if (searchQuery) {
      filtered = filtered.filter(fb =>
        fb.Comments?.toLowerCase().includes(searchQuery.toLowerCase()) ||
        fb.Meeting_ID?.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    // Type filter
    if (filterType !== 'All') {
      filtered = filtered.filter(fb => fb.Feedback_Type === filterType);
    }

    // Rating filter
    if (filterRating !== 'All') {
      filtered = filtered.filter(fb => fb.Rating === parseInt(filterRating));
    }

    // Sort
    filtered.sort((a, b) => {
      switch (sortBy) {
        case 'date_desc':
          return new Date(b.Submitted_At) - new Date(a.Submitted_At);
        case 'date_asc':
          return new Date(a.Submitted_At) - new Date(b.Submitted_At);
        case 'rating_desc':
          return b.Rating - a.Rating;
        case 'rating_asc':
          return a.Rating - b.Rating;
        default:
          return 0;
      }
    });

    setFilteredFeedback(filtered);
  };

  const handleRefresh = () => {
    fetchFeedback();
  };

  const handleResetFilters = () => {
    setSearchQuery('');
    setFilterType('All');
    setFilterRating('All');
    setSortBy('date_desc');
  };

  const formatDate = (dateString) => {
    try {
      return format(new Date(dateString), 'MMM dd, yyyy • hh:mm a');
    } catch {
      return dateString;
    }
  };

  const getRatingColor = (rating) => {
    if (rating >= 4) return 'success';
    if (rating === 3) return 'info';
    if (rating === 2) return 'warning';
    return 'error';
  };

  const getAverageRating = () => {
    if (filteredFeedback.length === 0) return 0;
    const sum = filteredFeedback.reduce((acc, fb) => acc + fb.Rating, 0);
    return (sum / filteredFeedback.length).toFixed(1);
  };

  const getRatingDistribution = () => {
    const distribution = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    filteredFeedback.forEach(fb => {
      distribution[fb.Rating] = (distribution[fb.Rating] || 0) + 1;
    });
    return distribution;
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight={400}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box p={3}>
        <Alert 
          severity="error" 
          action={
            <Button color="inherit" size="small" onClick={handleRefresh}>
              Retry
            </Button>
          }
        >
          {error}
        </Alert>
      </Box>
    );
  }

  const ratingDistribution = getRatingDistribution();
  const averageRating = getAverageRating();

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Box>
          <Typography variant="h5" fontWeight={700} gutterBottom>
            Feedback History
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {filteredFeedback.length} feedback submission{filteredFeedback.length !== 1 ? 's' : ''}
          </Typography>
        </Box>
        <Tooltip title="Refresh">
          <IconButton onClick={handleRefresh} color="primary">
            <RefreshIcon />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Summary Statistics */}
      {filteredFeedback.length > 0 && (
        <Grid container spacing={2} sx={{ mb: 3 }}>
          <Grid item xs={12} md={3}>
            <Paper elevation={0} sx={{ p: 2, border: 1, borderColor: 'divider' }}>
              <Typography variant="caption" color="text.secondary">
                Average Rating
              </Typography>
              <Box display="flex" alignItems="center" gap={1} mt={1}>
                <StarIcon color="warning" />
                <Typography variant="h4" fontWeight={700}>
                  {averageRating}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  / 5.0
                </Typography>
              </Box>
            </Paper>
          </Grid>

          {[5, 4, 3, 2, 1].map(rating => (
            <Grid item xs={6} sm={4} md={1.8} key={rating}>
              <Paper elevation={0} sx={{ p: 2, border: 1, borderColor: 'divider' }}>
                <Box display="flex" alignItems="center" gap={0.5} mb={1}>
                  <StarIcon sx={{ fontSize: 16, color: 'warning.main' }} />
                  <Typography variant="body2" fontWeight={600}>
                    {rating}
                  </Typography>
                </Box>
                <Typography variant="h6" fontWeight={700}>
                  {ratingDistribution[rating]}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {filteredFeedback.length > 0 
                    ? Math.round((ratingDistribution[rating] / filteredFeedback.length) * 100)
                    : 0}%
                </Typography>
              </Paper>
            </Grid>
          ))}
        </Grid>
      )}

      {/* Filters and Search */}
      <Paper elevation={0} sx={{ p: 2, mb: 3, border: 1, borderColor: 'divider' }}>
        <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
          {/* Search */}
          <TextField
            fullWidth
            size="small"
            placeholder="Search by comments or meeting ID..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              )
            }}
          />

          {/* Filter by Type */}
          <FormControl size="small" sx={{ minWidth: 180 }}>
            <InputLabel>Feedback Type</InputLabel>
            <Select
              value={filterType}
              onChange={(e) => setFilterType(e.target.value)}
              label="Feedback Type"
            >
              <MenuItem value="All">All Types</MenuItem>
              <MenuItem value="General">General</MenuItem>
              <MenuItem value="Technical">Technical</MenuItem>
              <MenuItem value="Content">Content</MenuItem>
              <MenuItem value="Other">Other</MenuItem>
            </Select>
          </FormControl>

          {/* Filter by Rating */}
          <FormControl size="small" sx={{ minWidth: 150 }}>
            <InputLabel>Rating</InputLabel>
            <Select
              value={filterRating}
              onChange={(e) => setFilterRating(e.target.value)}
              label="Rating"
            >
              <MenuItem value="All">All Ratings</MenuItem>
              {[5, 4, 3, 2, 1].map(rating => (
                <MenuItem key={rating} value={rating}>{rating} Stars</MenuItem>
              ))}
            </Select>
          </FormControl>

          {/* Sort */}
          <FormControl size="small" sx={{ minWidth: 150 }}>
            <InputLabel>Sort By</InputLabel>
            <Select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value)}
              label="Sort By"
            >
              <MenuItem value="date_desc">Newest First</MenuItem>
              <MenuItem value="date_asc">Oldest First</MenuItem>
              <MenuItem value="rating_desc">Highest Rating</MenuItem>
              <MenuItem value="rating_asc">Lowest Rating</MenuItem>
            </Select>
          </FormControl>

          {/* Reset Filters */}
          <Button
            variant="outlined"
            onClick={handleResetFilters}
            sx={{ minWidth: 100 }}
          >
            Reset
          </Button>
        </Stack>
      </Paper>

      {/* Feedback List */}
      {filteredFeedback.length === 0 ? (
        <Paper elevation={0} sx={{ p: 4, textAlign: 'center', border: 1, borderColor: 'divider' }}>
          <CommentIcon sx={{ fontSize: 64, color: 'text.disabled', mb: 2 }} />
          <Typography variant="h6" color="text.secondary" gutterBottom>
            No feedback found
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {feedbackList.length === 0 
              ? 'Submit feedback after meetings to see them here'
              : 'Try adjusting your filters'}
          </Typography>
        </Paper>
      ) : (
        <Stack spacing={2}>
          {filteredFeedback.map((feedback) => (
            <Card 
              key={feedback.ID} 
              elevation={0}
              sx={{ 
                border: 1, 
                borderColor: 'divider',
                '&:hover': {
                  boxShadow: 2,
                  borderColor: 'primary.main'
                },
                transition: 'all 0.2s'
              }}
            >
              <CardContent>
                <Grid container spacing={2}>
                  {/* Left Section - Rating and Type */}
                  <Grid item xs={12} md={3}>
                    <Box>
                      {/* Rating */}
                      <Box display="flex" alignItems="center" gap={1} mb={2}>
                        <Rating
                          value={feedback.Rating}
                          readOnly
                          size="small"
                        />
                        <Chip
                          label={feedback.Rating}
                          size="small"
                          color={getRatingColor(feedback.Rating)}
                        />
                      </Box>

                      {/* Feedback Type */}
                      <Chip
                        icon={<CategoryIcon />}
                        label={
                          <Box display="flex" alignItems="center" gap={0.5}>
                            <span>{FEEDBACK_TYPE_ICONS[feedback.Feedback_Type]}</span>
                            <span>{feedback.Feedback_Type}</span>
                          </Box>
                        }
                        color={FEEDBACK_TYPE_COLORS[feedback.Feedback_Type]}
                        variant="outlined"
                        size="small"
                      />
                    </Box>
                  </Grid>

                  {/* Right Section - Details */}
                  <Grid item xs={12} md={9}>
                    {/* Date */}
                    <Box display="flex" alignItems="center" gap={1} mb={1}>
                      <CalendarIcon fontSize="small" color="action" />
                      <Typography variant="caption" color="text.secondary">
                        {formatDate(feedback.Submitted_At)}
                      </Typography>
                    </Box>

                    {/* Meeting ID */}
                    <Typography variant="body2" color="text.secondary" gutterBottom>
                      Meeting: <strong>{feedback.Meeting_ID}</strong>
                    </Typography>

                    {/* Comments */}
                    {feedback.Comments && (
                      <Box mt={2}>
                        <Divider sx={{ mb: 1 }} />
                        <Box display="flex" gap={1}>
                          <CommentIcon fontSize="small" color="action" />
                          <Typography variant="body2" color="text.primary">
                            {feedback.Comments}
                          </Typography>
                        </Box>
                      </Box>
                    )}

                    {/* Feedback ID */}
                    <Box mt={2}>
                      <Chip
                        label={`ID: ${feedback.ID}`}
                        size="small"
                        variant="outlined"
                      />
                    </Box>
                  </Grid>
                </Grid>
              </CardContent>
            </Card>
          ))}
        </Stack>
      )}
    </Box>
  );
};

export default FeedbackHistory;