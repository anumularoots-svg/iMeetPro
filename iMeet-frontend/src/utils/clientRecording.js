// src/utils/clientRecording.js - Client-Side Recording Utilities

/**
 * Check if browser supports client-side recording
 */
export const checkRecordingSupport = () => {
  const hasMediaRecorder = typeof MediaRecorder !== 'undefined';
  const hasGetUserMedia = navigator.mediaDevices && navigator.mediaDevices.getUserMedia;
  const hasGetDisplayMedia = navigator.mediaDevices && navigator.mediaDevices.getDisplayMedia;
  
  // Check supported formats
  const supportedFormats = [];
  if (hasMediaRecorder) {
    const formats = ['video/webm', 'video/mp4', 'video/webm;codecs=vp9', 'video/webm;codecs=vp8'];
    formats.forEach(format => {
      if (MediaRecorder.isTypeSupported(format)) {
        supportedFormats.push(format);
      }
    });
  }
  
  return {
    supported: hasMediaRecorder && hasGetUserMedia,
    mediaRecorder: hasMediaRecorder,
    getUserMedia: hasGetUserMedia,
    getDisplayMedia: hasGetDisplayMedia,
    supportedFormats,
    preferredFormat: supportedFormats[0] || 'video/webm'
  };
};

/**
 * Create a combined stream from multiple video/audio streams
 */
export const createRecordingStream = async (streams = [], options = {}) => {
  const {
    includeAudio = true,
    includeVideo = true,
    audioOnly = false
  } = options;

  try {
    // Create a combined stream
    const combinedStream = new MediaStream();
    
    // Add tracks from all provided streams
    streams.forEach(stream => {
      if (stream && stream.getTracks) {
        stream.getTracks().forEach(track => {
          if (includeAudio && track.kind === 'audio') {
            combinedStream.addTrack(track);
          }
          if (includeVideo && track.kind === 'video' && !audioOnly) {
            combinedStream.addTrack(track);
          }
        });
      }
    });
    
    // If no streams provided, try to get user media
    if (streams.length === 0) {
      const constraints = {
        audio: includeAudio,
        video: includeVideo && !audioOnly
      };
      
      const userStream = await navigator.mediaDevices.getUserMedia(constraints);
      return userStream;
    }
    
    return combinedStream;
  } catch (error) {
    console.error('Failed to create recording stream:', error);
    throw new Error(`Failed to create recording stream: ${error.message}`);
  }
};

/**
 * Initialize MediaRecorder with appropriate settings
 */
export const createMediaRecorder = (stream, options = {}) => {
  const support = checkRecordingSupport();
  
  if (!support.supported) {
    throw new Error('MediaRecorder not supported in this browser');
  }
  
  const {
    mimeType = support.preferredFormat,
    videoBitsPerSecond = 2500000, // 2.5 Mbps
    audioBitsPerSecond = 128000   // 128 kbps
  } = options;
  
  const recorderOptions = {
    mimeType: MediaRecorder.isTypeSupported(mimeType) ? mimeType : support.preferredFormat,
    videoBitsPerSecond,
    audioBitsPerSecond
  };
  
  try {
    const recorder = new MediaRecorder(stream, recorderOptions);
    
    console.log('MediaRecorder created with options:', recorderOptions);
    console.log('Stream tracks:', stream.getTracks().map(t => ({ kind: t.kind, enabled: t.enabled })));
    
    return recorder;
  } catch (error) {
    console.error('Failed to create MediaRecorder:', error);
    throw new Error(`Failed to create MediaRecorder: ${error.message}`);
  }
};

/**
 * Process recorded chunks into a blob
 */
export const processRecordingChunks = (chunks, mimeType = 'video/webm') => {
  try {
    if (!chunks || chunks.length === 0) {
      throw new Error('No recording chunks to process');
    }
    
    const blob = new Blob(chunks, { type: mimeType });
    
    const metadata = {
      size: blob.size,
      type: blob.type,
      chunks: chunks.length,
      duration: null // Will be calculated separately if needed
    };
    
    console.log('Processed recording chunks:', metadata);
    
    return { blob, metadata };
  } catch (error) {
    console.error('Failed to process recording chunks:', error);
    throw new Error(`Failed to process recording: ${error.message}`);
  }
};

/**
 * Estimate file size during recording
 */
export const estimateFileSize = (durationMs, bitrate = 2500000) => {
  const durationSeconds = durationMs / 1000;
  const estimatedBits = durationSeconds * bitrate;
  const estimatedBytes = estimatedBits / 8;
  
  return {
    bytes: Math.round(estimatedBytes),
    mb: Math.round(estimatedBytes / (1024 * 1024) * 100) / 100,
    formatted: formatFileSize(estimatedBytes)
  };
};

/**
 * Format file size for display
 */
export const formatFileSize = (bytes) => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/**
 * Get video duration from blob
 */
export const getVideoDuration = (blob) => {
  return new Promise((resolve, reject) => {
    try {
      const video = document.createElement('video');
      video.preload = 'metadata';
      
      video.onloadedmetadata = () => {
        window.URL.revokeObjectURL(video.src);
        resolve(video.duration);
      };
      
      video.onerror = () => {
        window.URL.revokeObjectURL(video.src);
        reject(new Error('Failed to load video metadata'));
      };
      
      video.src = URL.createObjectURL(blob);
    } catch (error) {
      reject(error);
    }
  });
};

/**
 * Create a preview thumbnail from video blob
 */
export const createVideoThumbnail = (blob, timeOffset = 1) => {
  return new Promise((resolve, reject) => {
    try {
      const video = document.createElement('video');
      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d');
      
      video.onloadedmetadata = () => {
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        video.currentTime = Math.min(timeOffset, video.duration / 2);
      };
      
      video.onseeked = () => {
        ctx.drawImage(video, 0, 0);
        canvas.toBlob((thumbnailBlob) => {
          window.URL.revokeObjectURL(video.src);
          resolve(thumbnailBlob);
        }, 'image/jpeg', 0.8);
      };
      
      video.onerror = () => {
        window.URL.revokeObjectURL(video.src);
        reject(new Error('Failed to create thumbnail'));
      };
      
      video.src = URL.createObjectURL(blob);
    } catch (error) {
      reject(error);
    }
  });
};

/**
 * Download recording blob as file
 */
export const downloadRecordingBlob = (blob, fileName = 'recording.webm') => {
  try {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    
    link.href = url;
    link.download = fileName;
    link.style.display = 'none';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Clean up
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    
    console.log('Recording download initiated:', fileName);
  } catch (error) {
    console.error('Failed to download recording:', error);
    throw new Error(`Failed to download recording: ${error.message}`);
  }
};

/**
 * Validate recording blob
 */
export const validateRecordingBlob = async (blob, minDuration = 1) => {
  try {
    if (!blob || blob.size === 0) {
      throw new Error('Recording blob is empty');
    }
    
    if (blob.size < 1000) { // Less than 1KB
      throw new Error('Recording file is too small');
    }
    
    // Check if we can get duration
    try {
      const duration = await getVideoDuration(blob);
      if (duration < minDuration) {
        throw new Error(`Recording too short: ${duration}s (minimum: ${minDuration}s)`);
      }
      
      return {
        valid: true,
        duration,
        size: blob.size,
        type: blob.type
      };
    } catch (durationError) {
      console.warn('Could not validate duration:', durationError);
      
      // If we can't get duration but file seems valid, allow it
      return {
        valid: true,
        duration: null,
        size: blob.size,
        type: blob.type,
        warning: 'Could not validate duration'
      };
    }
  } catch (error) {
    return {
      valid: false,
      error: error.message,
      size: blob?.size || 0,
      type: blob?.type || 'unknown'
    };
  }
};

/**
 * Convert video blob to different format (basic)
 */
export const convertVideoFormat = (blob, targetType = 'video/mp4') => {
  return new Promise((resolve, reject) => {
    if (blob.type === targetType) {
      resolve(blob);
      return;
    }
    
    // Simple re-wrapping for supported formats
    try {
      const newBlob = new Blob([blob], { type: targetType });
      resolve(newBlob);
    } catch (error) {
      reject(new Error(`Format conversion not supported: ${blob.type} to ${targetType}`));
    }
  });
};

/**
 * Cleanup stream tracks and objects
 */
export const cleanupRecordingResources = (stream, recorder) => {
  try {
    // Stop recorder if still recording
    if (recorder && recorder.state !== 'inactive') {
      recorder.stop();
    }
    
    // Stop all stream tracks
    if (stream && stream.getTracks) {
      stream.getTracks().forEach(track => {
        if (track.readyState === 'live') {
          track.stop();
        }
      });
    }
    
    console.log('Recording resources cleaned up');
  } catch (error) {
    console.error('Error cleaning up recording resources:', error);
  }
};

/**
 * Create recording session metadata
 */
export const createRecordingMetadata = (options = {}) => {
  const {
    meetingId,
    userId,
    userName,
    startTime = new Date(),
    settings = {}
  } = options;
  
  return {
    recordingId: `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    meetingId,
    userId,
    userName,
    startTime: startTime.toISOString(),
    recordingType: 'client',
    browserInfo: {
      userAgent: navigator.userAgent,
      platform: navigator.platform,
      language: navigator.language
    },
    settings: {
      mimeType: settings.mimeType || 'video/webm',
      videoBitsPerSecond: settings.videoBitsPerSecond || 2500000,
      audioBitsPerSecond: settings.audioBitsPerSecond || 128000,
      ...settings
    },
    capabilities: checkRecordingSupport()
  };
};

export default {
  checkRecordingSupport,
  createRecordingStream,
  createMediaRecorder,
  processRecordingChunks,
  estimateFileSize,
  formatFileSize,
  getVideoDuration,
  createVideoThumbnail,
  downloadRecordingBlob,
  validateRecordingBlob,
  convertVideoFormat,
  cleanupRecordingResources,
  createRecordingMetadata
};