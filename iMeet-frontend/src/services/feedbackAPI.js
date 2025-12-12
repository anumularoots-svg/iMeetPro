// src/services/feedbackAPI.js
import axios from 'axios';

const FEEDBACK_BASE_URL = 'https://192.168.48.201:8111/api/feedback'; // Adjust port as needed

/**
 * Feedback API Service
 * Communicates with Django backend for feedback operations
 */

// Create new feedback
export const createFeedback = async (feedbackData) => {
  try {
    const response = await axios.post(`${FEEDBACK_BASE_URL}/create`, {
      Meeting_ID: feedbackData.meetingId,
      User_ID: feedbackData.userId,
      Rating: feedbackData.rating,
      Comments: feedbackData.comments || '',
      Feedback_Type: feedbackData.feedbackType || 'General'
    });
    return { success: true, data: response.data };
  } catch (error) {
    console.error('Error creating feedback:', error);
    return { 
      success: false, 
      error: error.response?.data?.Error || 'Failed to submit feedback' 
    };
  }
};

// Get all feedback (admin use)
export const getAllFeedback = async () => {
  try {
    const response = await axios.get(`${FEEDBACK_BASE_URL}/feedbacks`);
    return { success: true, data: response.data };
  } catch (error) {
    console.error('Error fetching feedback:', error);
    return { success: false, error: error.message };
  }
};

// Get specific feedback by ID
export const getFeedbackById = async (feedbackId) => {
  try {
    const response = await axios.get(`${FEEDBACK_BASE_URL}/feedback/${feedbackId}`);
    return { success: true, data: response.data };
  } catch (error) {
    console.error('Error fetching feedback:', error);
    return { success: false, error: error.message };
  }
};

// Validate feedback data before submission
export const validateFeedback = async (feedbackData) => {
  try {
    const response = await axios.post(`${FEEDBACK_BASE_URL}/validate`, {
      Meeting_ID: feedbackData.meetingId,
      User_ID: feedbackData.userId,
      Rating: feedbackData.rating,
      Comments: feedbackData.comments,
      Feedback_Type: feedbackData.feedbackType
    });
    return { success: true, data: response.data };
  } catch (error) {
    console.error('Error validating feedback:', error);
    return { success: false, error: error.message };
  }
};

// Update existing feedback
export const updateFeedback = async (feedbackId, feedbackData) => {
  try {
    const response = await axios.put(`${FEEDBACK_BASE_URL}/update/${feedbackId}`, {
      Meeting_ID: feedbackData.meetingId,
      User_ID: feedbackData.userId,
      Rating: feedbackData.rating,
      Comments: feedbackData.comments,
      Feedback_Type: feedbackData.feedbackType
    });
    return { success: true, data: response.data };
  } catch (error) {
    console.error('Error updating feedback:', error);
    return { success: false, error: error.message };
  }
};

// Delete feedback
export const deleteFeedback = async (feedbackId) => {
  try {
    const response = await axios.delete(`${FEEDBACK_BASE_URL}/delete/${feedbackId}`);
    return { success: true, data: response.data };
  } catch (error) {
    console.error('Error deleting feedback:', error);
    return { success: false, error: error.message };
  }
};

// Check if user has submitted feedback for a meeting
export const checkFeedbackExists = async (meetingId, userId) => {
  try {
    const response = await axios.get(`${FEEDBACK_BASE_URL}/feedbacks`);
    const feedbacks = response.data;
    
    const existingFeedback = feedbacks.find(
      fb => fb.Meeting_ID === meetingId && fb.User_ID === userId
    );
    
    return { 
      success: true, 
      exists: !!existingFeedback,
      feedback: existingFeedback 
    };
  } catch (error) {
    console.error('Error checking feedback:', error);
    return { success: false, error: error.message };
  }
};

export default {
  createFeedback,
  getAllFeedback,
  getFeedbackById,
  validateFeedback,
  updateFeedback,
  deleteFeedback,
  checkFeedbackExists
};