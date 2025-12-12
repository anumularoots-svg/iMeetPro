// src/components/feedback/FeedbackDialog.jsx
import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  Box,
  Typography,
  IconButton,
  Slide
} from '@mui/material';
import {
  Close as CloseIcon,
  ThumbUp as ThumbUpIcon
} from '@mui/icons-material';
import FeedbackForm from './FeedbackForm';
import FeedbackSuccess from './FeedbackSuccess';

const Transition = React.forwardRef(function Transition(props, ref) {
  return <Slide direction="up" ref={ref} {...props} />;
});

const FeedbackDialog = ({ 
  open, 
  onClose, 
  meetingId, 
  userId,
  meetingTitle = "Recent Meeting" 
}) => {
  const [showSuccess, setShowSuccess] = useState(false);
  const [submittedFeedback, setSubmittedFeedback] = useState(null);

  const handleSubmitSuccess = (feedbackData) => {
    // Store submitted feedback data for success screen
    setSubmittedFeedback(feedbackData);
    setShowSuccess(true);
  };

  const handleClose = () => {
    // Reset state
    setShowSuccess(false);
    setSubmittedFeedback(null);
    onClose();
  };

  const handleSkip = () => {
    // Allow users to skip feedback
    handleClose();
  };

  return (
   <Dialog
  open={open}
  onClose={(event, reason) => {
    // CRITICAL: Prevent ANY auto-closing when feedback form is active
    if (reason === 'backdropClick' || reason === 'escapeKeyDown') {
      console.log('⛔ Prevented dialog close:', reason);
      return; // Block all backdrop/escape closes
    }
    handleClose();
  }}
  disableEscapeKeyDown={true} // ✅ ADD THIS
  BackdropProps={{
    sx: {
      backgroundColor: 'rgba(0, 0, 0, 0.9)', // Darker backdrop
      backdropFilter: 'blur(10px)',
    },
    onClick: (e) => {
      e.stopPropagation(); // ✅ ADD THIS - Stop backdrop clicks
      console.log('⛔ Backdrop click blocked');
    }
  }}
  maxWidth="md"
  fullWidth
  PaperProps={{
    sx: {
      borderRadius: 3,
      boxShadow: '0 8px 32px rgba(0,0,0,0.12)',
      maxHeight: '90vh',
      zIndex: 26000, // ✅ ADD THIS - Ensure dialog paper is on top
    }
  }}
>
      {!showSuccess && (
        <DialogTitle sx={{ pb: 1 }}>
          <Box display="flex" alignItems="center" justifyContent="space-between">
            <Box display="flex" alignItems="center" gap={1}>
              <ThumbUpIcon color="primary" sx={{ fontSize: 28 }} />
              <Typography variant="h5" fontWeight={700}>
                Share Your Feedback
              </Typography>
            </Box>
            <IconButton onClick={handleSkip} size="small">
              <CloseIcon />
            </IconButton>
          </Box>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5, ml: 5 }}>
            Help us improve meeting experiences
          </Typography>
        </DialogTitle>
      )}

      <DialogContent sx={{ px: 4, py: 3 }}>
        {showSuccess ? (
          // Success Screen
          <FeedbackSuccess
            feedbackId={submittedFeedback?.feedbackId}
            rating={submittedFeedback?.rating}
            meetingTitle={meetingTitle}
            onClose={handleClose}
            autoCloseDelay={3000}
          />
        ) : (
          // Feedback Form
          <FeedbackForm
            meetingId={meetingId}
            userId={userId}
            meetingTitle={meetingTitle}
            onSubmitSuccess={(feedbackId) => {
              handleSubmitSuccess({
                feedbackId,
                rating: null // Will be passed from form if needed
              });
            }}
            onCancel={handleSkip}
          />
        )}
      </DialogContent>
    </Dialog>
  );
};

export default FeedbackDialog;