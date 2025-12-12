// ============================================================================
// MeetingEndedOverlay.jsx - DIRECT SKIP REDIRECT VERSION
// ============================================================================

import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Rating,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Paper,
  Alert,
  CircularProgress,
  Snackbar,
  Slide
} from '@mui/material';
import {
  MeetingRoom as MeetingRoomIcon,
  ExitToApp,
  Send as SendIcon,
  Close as CloseIcon,
  CheckCircle as CheckCircleIcon
} from '@mui/icons-material';

// ✅ Import your existing feedback system
import { useFeedback } from '../../hooks/useFeedback';

// Transition component for Snackbar
const SlideTransition = (props) => {
  return <Slide {...props} direction="down" />;
};

const MeetingEndedOverlay = ({
  meetingEnded,
  onLeaveMeeting,
  meetingId,
  userId,
  meetingTitle,
  currentUser
}) => {
  const [showFeedback, setShowFeedback] = useState(true);
  const [rating, setRating] = useState(0);
  const [feedbackType, setFeedbackType] = useState('General');
  const [comments, setComments] = useState('');
  const [submitError, setSubmitError] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // ✅ Snackbar notification state
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: '',
    severity: 'info'
  });

  // ✅ Rating validation notification
  const [showRatingError, setShowRatingError] = useState(false);

  // ✅ Use your existing feedback hook
  const {
    submitFeedback,
    loading: hookLoading,
    error: feedbackError
  } = useFeedback();

  // ✅ Set feedback active flag when overlay mounts
  useEffect(() => {
    if (meetingEnded && showFeedback) {
      console.log("🔒 Feedback dialog active - blocking navigation");
      sessionStorage.setItem('feedbackActive', 'true');
      window.blockAutoRefresh = true;
    }
  }, [meetingEnded, showFeedback]);

  // ✅ Clear feedback flag when showing Thank You screen
  useEffect(() => {
    if (!showFeedback) {
      console.log("✅ Thank You screen showing - clearing feedback flag");
      sessionStorage.removeItem('feedbackActive');
      window.blockAutoRefresh = false;
    }
  }, [showFeedback]);

  // ✅ Update error display when feedbackError changes
  useEffect(() => {
    if (feedbackError) {
      setSubmitError(feedbackError);
      setIsSubmitting(false);
      showSnackbar(feedbackError, 'error');
    }
  }, [feedbackError]);

  console.log("🔍 MeetingEndedOverlay render:", {
    meetingEnded,
    showFeedback,
    meetingId,
    userId: userId || currentUser?.id,
    meetingTitle,
    rating,
    isSubmitting,
    timestamp: new Date().toISOString()
  });

  if (!meetingEnded) {
    console.log("❌ Meeting not ended - hiding overlay");
    return null;
  }

  // ✅ Snackbar helper function
  const showSnackbar = (message, severity = 'info') => {
    console.log(`📢 Snackbar: [${severity}] ${message}`);
    setSnackbar({
      open: true,
      message,
      severity
    });
  };

  const handleCloseSnackbar = () => {
    setSnackbar(prev => ({ ...prev, open: false }));
  };

  // ✅ Navigate to dashboard helper
  const handleNavigateToDashboard = () => {
    console.log("🚪 Navigating to dashboard...");

    // Clear all flags
    sessionStorage.removeItem('feedbackActive');
    sessionStorage.removeItem('meetingEndedAt');
    sessionStorage.removeItem('blockAutoRefresh');
    sessionStorage.removeItem('currentMeetingId');
    window.blockAutoRefresh = false;

    // Call cleanup if exists
    if (onLeaveMeeting) {
      try {
        console.log("🧹 Calling onLeaveMeeting cleanup...");
        onLeaveMeeting();
      } catch (error) {
        console.warn("⚠️ Cleanup error:", error);
      }
    }

    // Force refresh and navigate
    console.log("🔄 Forcing page refresh to /dashboard");
    window.location.href = '/dashboard';
  };

  const handleSubmit = async () => {
    console.log("📝 Submitting feedback:", { rating, feedbackType, comments });

    if (rating === 0) {
      setShowRatingError(true);
      showSnackbar("Please provide a rating before submitting!", 'warning');
      return;
    }

    setIsSubmitting(true);
    setSubmitError(null);
    setShowRatingError(false);

    try {
      // ✅ Use your existing submitFeedback function
      const feedbackData = {
        meetingId: meetingId,
        userId: userId || currentUser?.id,
        rating: rating,
        comments: comments,
        feedbackType: feedbackType
      };

      console.log("📤 Sending feedback via useFeedback hook:", feedbackData);

      const result = await submitFeedback(feedbackData);

      // ✅ Check if it's a database error specifically
      const isDatabaseError = result?.error?.includes('Database error') ||
        result?.error?.includes('SQL syntax');

      if (result && result.success) {
        console.log("✅ Feedback submitted successfully:", result);

        // Show success message
        showSnackbar("Feedback submitted successfully! Thank you!", 'success');

        // Wait a moment then show thank you screen
        setTimeout(() => {
          console.log("✅ Transitioning to Thank You screen after successful submit");
          setShowFeedback(false);
        }, 500);
      } else if (isDatabaseError) {
        // ✅ SPECIAL HANDLING: Database error - save locally and proceed silently
        console.warn("⚠️ Database error detected - saving locally and proceeding");

        // Save to localStorage
        saveFeedbackLocally(feedbackData, result?.error || 'Database error');

        // ✅ HIDE the technical error from user - just show generic success
        showSnackbar("Feedback recorded! Thank you for your input.", 'success');

        // Show thank you screen
        setTimeout(() => {
          console.log("✅ Transitioning to Thank You screen after database error");
          setShowFeedback(false);
        }, 500);
      } else {
        // Handle other submission failures
        const errorMessage = result?.error || 'Failed to submit feedback';
        console.error("❌ Feedback submission failed:", errorMessage);

        // Save to localStorage as backup
        saveFeedbackLocally(feedbackData, errorMessage);

        const userMessage = "Unable to submit feedback right now. Your feedback has been saved.";
        setSubmitError(userMessage);
        showSnackbar(userMessage, 'info');

        // ✅ Re-enable form so user can edit or skip
        setIsSubmitting(false);

        // Auto-proceed after 3 seconds
        setTimeout(() => {
          console.log("⏱️ Auto-proceeding to Thank You after error (3s elapsed)");
          setShowFeedback(false);
        }, 3000);
      }
    } catch (error) {
      console.error("❌ Feedback submission exception:", error);

      // ✅ Save to localStorage as backup
      const feedbackData = {
        meetingId: meetingId,
        userId: userId || currentUser?.id,
        rating: rating,
        comments: comments,
        feedbackType: feedbackType
      };

      saveFeedbackLocally(feedbackData, error.message);

      // ✅ Show friendly message and proceed
      showSnackbar("Feedback recorded! Thank you for your input.", 'success');

      // Show thank you screen
      setTimeout(() => {
        console.log("✅ Transitioning to Thank You screen after exception");
        setShowFeedback(false);
      }, 500);
    }
  };

  const saveFeedbackLocally = (feedbackData, errorMessage) => {
    try {
      const localFeedback = {
        ...feedbackData,
        meeting_title: meetingTitle || 'Meeting',
        submitted_at: new Date().toISOString(),
        status: 'pending_sync',
        error: errorMessage
      };

      const existingFeedback = JSON.parse(localStorage.getItem('pendingFeedback') || '[]');
      existingFeedback.push(localFeedback);
      localStorage.setItem('pendingFeedback', JSON.stringify(existingFeedback));

      console.log("💾 Feedback saved locally for later sync:", localFeedback);
    } catch (storageError) {
      console.error("❌ Failed to save to localStorage:", storageError);
    }
  };

  // ✅ FIXED: Handle skip button - DIRECT REDIRECT TO DASHBOARD
  const handleSkipClick = () => {
    console.log("⏭️ Skip button clicked - redirecting directly to dashboard");

    // Clear all feedback flags immediately
    sessionStorage.removeItem('feedbackActive');
    sessionStorage.removeItem('meetingEndedAt');
    sessionStorage.removeItem('blockAutoRefresh');
    sessionStorage.removeItem('currentMeetingId');
    window.blockAutoRefresh = false;

    // Show brief notification
    showSnackbar("Redirecting to dashboard...", 'info');

    // ✅ Navigate directly to dashboard after brief delay for notification
    setTimeout(() => {
      console.log("🚪 Executing direct navigation to dashboard");
      handleNavigateToDashboard();
    }, 500);
  };

  return (
    <>
      <Box
        sx={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.95)',
          backdropFilter: 'blur(20px)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 20000,
          padding: 2,
          '@keyframes fadeIn': {
            from: { opacity: 0, transform: 'translateY(10px)' },
            to: { opacity: 1, transform: 'translateY(0)' }
          }
        }}
      >
        {showFeedback ? (
          // ✅ PHASE 1: FEEDBACK FORM
          <Paper
            elevation={24}
            sx={{
              backgroundColor: 'white',
              borderRadius: 4,
              p: 4,
              maxWidth: 600,
              width: '100%',
              position: 'relative'
            }}
          >
            {/* Meeting Icon */}
            <Box sx={{ textAlign: 'center', mb: 3 }}>
              <MeetingRoomIcon sx={{ fontSize: 60, color: '#667eea' }} />
              <Typography variant="h4" sx={{ mt: 2, fontWeight: 600, color: '#333' }}>
                Meeting Ended
              </Typography>
              <Typography variant="body1" sx={{ mt: 1, color: '#666' }}>
                Please share your feedback about this meeting
              </Typography>
              {meetingTitle && (
                <Typography variant="body2" sx={{ mt: 1, color: '#999', fontStyle: 'italic' }}>
                  {meetingTitle}
                </Typography>
              )}
            </Box>

            {/* Error Alert */}
            {submitError && (
              <Alert
                severity="warning"
                sx={{ mb: 3 }}
                onClose={() => setSubmitError(null)}
              >
                {submitError}
                {!isSubmitting && (
                  <Typography variant="caption" sx={{ display: 'block', mt: 1 }}>
                    You can edit your feedback or click Skip to continue.
                  </Typography>
                )}
              </Alert>
            )}

            {/* Rating */}
            <Box sx={{ mb: 3, textAlign: 'center' }}>
              <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 500, color: '#333' }}>
                How would you rate this meeting? *
              </Typography>
              <Rating
                value={rating}
                onChange={(event, newValue) => {
                  console.log("⭐ Rating changed:", newValue);
                  setRating(newValue);
                  setSubmitError(null);
                  setShowRatingError(false);
                }}
                size="large"
                disabled={isSubmitting}
                sx={{
                  '& .MuiRating-iconFilled': {
                    color: '#ffd700',
                  },
                  '& .MuiRating-iconHover': {
                    color: '#ffd700',
                  },
                  '& .MuiRating-iconEmpty': {
                    opacity: isSubmitting ? 0.3 : 1
                  }
                }}
              />
              {rating > 0 && (
                <Typography variant="caption" sx={{ display: 'block', mt: 1, color: '#666' }}>
                  {rating} out of 5 stars
                </Typography>
              )}
              {showRatingError && rating === 0 && (
                <Typography variant="caption" sx={{ display: 'block', mt: 1, color: '#d32f2f' }}>
                  Please select a rating to submit
                </Typography>
              )}
            </Box>

            {/* Feedback Type */}
            <FormControl fullWidth sx={{ mb: 3 }}>
              <InputLabel id="feedback-type-label">Feedback Type</InputLabel>
              <Select
                labelId="feedback-type-label"
                id="feedback-type-select"
                value={feedbackType}
                onChange={(e) => {
                  console.log("📋 Feedback type changed to:", e.target.value);
                  setFeedbackType(e.target.value);
                }}
                label="Feedback Type"
                disabled={isSubmitting}
                inputProps={{
                  'aria-label': 'Feedback Type'
                }}
              >
                <MenuItem value="General">General Feedback</MenuItem>
                <MenuItem value="Technical">Technical Issues</MenuItem>
                <MenuItem value="Content">Content Quality</MenuItem>
                <MenuItem value="Other">Other</MenuItem>
              </Select>
            </FormControl>

            {/* Comments */}
            <TextField
              fullWidth
              multiline
              rows={4}
              label="Comments (Optional)"
              placeholder="Share your thoughts about the meeting..."
              value={comments}
              onChange={(e) => {
                const newValue = e.target.value;
                console.log("💬 Comments changed:", newValue.length, "chars");
                setComments(newValue);
              }}
              disabled={isSubmitting}
              sx={{
                mb: 3,
                '& .MuiInputBase-root': {
                  backgroundColor: isSubmitting ? '#f5f5f5' : 'white'
                }
              }}
              inputProps={{
                maxLength: 500,
                'aria-label': 'Comments'
              }}
              helperText={`${comments.length}/500 characters`}
            />

            {/* Action Buttons */}
            <Box sx={{ display: 'flex', gap: 2 }}>
              <Button
                fullWidth
                variant="outlined"
                onClick={handleSkipClick}
                disabled={isSubmitting}
                startIcon={<CloseIcon />}
                sx={{
                  borderColor: '#999',
                  color: '#666',
                  '&:hover': {
                    borderColor: '#666',
                    backgroundColor: 'rgba(0,0,0,0.05)'
                  },
                  '&:disabled': {
                    borderColor: '#ddd',
                    color: '#bbb'
                  }
                }}
              >
                Skip
              </Button>
              <Button
                fullWidth
                variant="contained"
                onClick={handleSubmit}
                disabled={rating === 0 || isSubmitting}
                startIcon={isSubmitting ? <CircularProgress size={20} color="inherit" /> : <SendIcon />}
                sx={{
                  background: rating === 0 || isSubmitting
                    ? '#ccc'
                    : 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                  '&:hover': {
                    background: rating === 0 || isSubmitting
                      ? '#ccc'
                      : 'linear-gradient(135deg, #764ba2 0%, #667eea 100%)'
                  },
                  '&:disabled': {
                    background: '#ccc',
                    color: 'rgba(0, 0, 0, 0.26)'
                  }
                }}
              >
                {isSubmitting ? 'Submitting...' : 'Submit Feedback'}
              </Button>
            </Box>

            {/* Helper Text */}
            <Typography variant="caption" sx={{ display: 'block', textAlign: 'center', mt: 2, color: '#999' }}>
              Your feedback helps us improve future meetings
            </Typography>
          </Paper>
        ) : (
          // ✅ PHASE 2: THANK YOU MESSAGE - NO AUTO-REDIRECT
          <Box sx={{ textAlign: 'center' }}>
            <CheckCircleIcon
              sx={{
                fontSize: 120,
                color: '#4caf50',
                mb: 3,
                animation: 'fadeIn 0.5s ease'
              }}
            />

            <Typography
              variant="h3"
              sx={{
                mb: 2,
                fontWeight: 600,
                color: 'white',
                animation: 'fadeIn 0.7s ease'
              }}
            >
              Thank You!
            </Typography>

            <Typography
              variant="h6"
              sx={{
                mb: 1,
                color: '#ccc',
                animation: 'fadeIn 0.9s ease'
              }}
            >
              {rating > 0 ? 'Your feedback has been recorded' : 'We appreciate your time'}
            </Typography>

            <Typography
              variant="body1"
              sx={{
                mb: 4,
                color: '#999',
                animation: 'fadeIn 1.1s ease'
              }}
            >
              Click the button below to return to the dashboard
            </Typography>

            {/* ✅ ONLY Manual Navigation - No Auto-Redirect */}
            <Button
              variant="contained"
              size="large"
              onClick={handleNavigateToDashboard}
              sx={{
                mt: 2,
                background: 'linear-gradient(135deg, #4caf50 0%, #45a049 100%)',
                '&:hover': {
                  background: 'linear-gradient(135deg, #66bb6a 0%, #4caf50 100%)',
                  transform: 'translateY(-2px)',
                  boxShadow: '0 8px 16px rgba(76, 175, 80, 0.3)',
                },
                px: 8,
                py: 2.5,
                borderRadius: 2,
                fontSize: '1.2rem',
                fontWeight: 600,
                transition: 'all 0.3s ease',
                boxShadow: '0 4px 12px rgba(76, 175, 80, 0.2)',
                animation: 'fadeIn 1.3s ease',
              }}
              startIcon={<ExitToApp />}
            >
              Go to Dashboard
            </Button>

            {/* Optional: Add a subtle hint */}
            <Typography
              variant="caption"
              sx={{
                display: 'block',
                mt: 3,
                color: '#666',
                fontStyle: 'italic'
              }}
            >
              You can safely close this window anytime
            </Typography>
          </Box>
        )}
      </Box>

      {/* ✅ Snackbar Notification */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={handleCloseSnackbar}
        TransitionComponent={SlideTransition}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert
          onClose={handleCloseSnackbar}
          severity={snackbar.severity}
          variant="filled"
          sx={{
            width: '100%',
            boxShadow: 3
          }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </>
  );
};

export default MeetingEndedOverlay;