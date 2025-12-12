// src/hooks/useFeedback.js
import { useState, useCallback } from 'react';
import { 
  createFeedback, 
  validateFeedback, 
  checkFeedbackExists 
} from '../services/feedbackAPI';
import { useAuth } from './useAuth';

/**
 * Custom hook for managing feedback operations
 */
export const useFeedback = () => {
  const { user } = useAuth();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [validationErrors, setValidationErrors] = useState({});
  const [feedbackSubmitted, setFeedbackSubmitted] = useState(false);

  // Submit feedback
  const submitFeedback = useCallback(async (feedbackData) => {
    setLoading(true);
    setError(null);

    try {
      // Prepare data with user ID
      const dataToSubmit = {
        meetingId: feedbackData.meetingId,
        userId: user?.id || feedbackData.userId,
        rating: parseInt(feedbackData.rating),
        comments: feedbackData.comments || '',
        feedbackType: feedbackData.feedbackType || 'General'
      };

      // Submit to backend
      const result = await createFeedback(dataToSubmit);

      if (result.success) {
        setFeedbackSubmitted(true);
        return { success: true, feedbackId: result.data.Feedback_ID };
      } else {
        setError(result.error);
        return { success: false, error: result.error };
      }
    } catch (err) {
      const errorMessage = err.message || 'Failed to submit feedback';
      setError(errorMessage);
      return { success: false, error: errorMessage };
    } finally {
      setLoading(false);
    }
  }, [user]);

  // Validate feedback in real-time
  const validateFeedbackData = useCallback(async (feedbackData) => {
    try {
      const result = await validateFeedback({
        meetingId: feedbackData.meetingId,
        userId: user?.id || feedbackData.userId,
        rating: feedbackData.rating,
        comments: feedbackData.comments,
        feedbackType: feedbackData.feedbackType
      });

      if (result.success) {
        // Parse validation results
        const errors = {};
        Object.keys(result.data).forEach(field => {
          if (!result.data[field].is_valid) {
            errors[field] = result.data[field].message;
          }
        });
        setValidationErrors(errors);
        return { valid: Object.keys(errors).length === 0, errors };
      }
      return { valid: false, errors: {} };
    } catch (err) {
      console.error('Validation error:', err);
      return { valid: false, errors: {} };
    }
  }, [user]);

  // Check if feedback already exists
  const checkExistingFeedback = useCallback(async (meetingId) => {
    if (!user?.id) return { exists: false };

    try {
      const result = await checkFeedbackExists(meetingId, user.id);
      return result;
    } catch (err) {
      console.error('Error checking feedback:', err);
      return { exists: false };
    }
  }, [user]);

  // Reset feedback state
  const resetFeedback = useCallback(() => {
    setFeedbackSubmitted(false);
    setError(null);
    setValidationErrors({});
    setLoading(false);
  }, []);

  return {
    submitFeedback,
    validateFeedbackData,
    checkExistingFeedback,
    resetFeedback,
    loading,
    error,
    validationErrors,
    feedbackSubmitted
  };
};

export default useFeedback;