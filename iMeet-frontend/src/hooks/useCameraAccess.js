// src/hooks/useCameraAccess.js
// FIXED VERSION - Better error handling and debugging

import { useState, useEffect, useCallback, useRef } from 'react';

export const useCameraAccess = (isInMeetingArea = false) => {
  const [stream, setStream] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hasPermission, setHasPermission] = useState(false);
  const streamRef = useRef(null);
  const isRequestingRef = useRef(false);
  const mountedRef = useRef(true);

  // Request BOTH camera and microphone
  const requestCamera = useCallback(async () => {
    // Prevent duplicate requests
    if (streamRef.current || isRequestingRef.current) {
      console.log('📷 Media already active or request in progress');
      return streamRef.current;
    }

    isRequestingRef.current = true;
    setIsLoading(true);
    setError(null);

    try {
      console.log('🎥🎤 Requesting CAMERA + MICROPHONE access...');
      console.log('📍 isInMeetingArea:', isInMeetingArea);
      
      // Request both video and audio
      const mediaStream = await navigator.mediaDevices.getUserMedia({
        video: {
          width: { ideal: 1280 },
          height: { ideal: 720 },
          facingMode: 'user'
        },
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        }
      });

      if (!mountedRef.current) {
        console.log('⚠️ Component unmounted, stopping tracks');
        mediaStream.getTracks().forEach(track => track.stop());
        return null;
      }

      // Verify we got both tracks
      const videoTracks = mediaStream.getVideoTracks();
      const audioTracks = mediaStream.getAudioTracks();
      
      console.log('✅✅✅ SUCCESS - Got both tracks:', {
        '📷 Video': videoTracks.length > 0 ? `✅ ${videoTracks[0].label}` : '❌ MISSING',
        '🎤 Audio': audioTracks.length > 0 ? `✅ ${audioTracks[0].label}` : '❌ MISSING',
        videoEnabled: videoTracks[0]?.enabled,
        audioEnabled: audioTracks[0]?.enabled,
        videoState: videoTracks[0]?.readyState,
        audioState: audioTracks[0]?.readyState
      });

      if (videoTracks.length === 0) {
        throw new Error('No video track received - camera access failed');
      }

      if (audioTracks.length === 0) {
        console.warn('⚠️ No audio track received - microphone access failed');
      }

      streamRef.current = mediaStream;
      setStream(mediaStream);
      setHasPermission(true);
      setIsLoading(false);
      isRequestingRef.current = false;
      
      console.log('✅ Camera + Microphone ready!');
      return mediaStream;
    } catch (err) {
      console.error('❌❌❌ FAILED to get camera/microphone:', err);
      console.error('Error name:', err.name);
      console.error('Error message:', err.message);
      
      if (mountedRef.current) {
        setError(err.message || 'Failed to access camera/microphone');
        setHasPermission(false);
        setIsLoading(false);
      }
      
      isRequestingRef.current = false;
      throw err;
    }
  }, [isInMeetingArea]);

  // Release both camera and microphone
  const releaseCamera = useCallback(() => {
    if (streamRef.current) {
      console.log('🛑 Releasing camera + microphone...');
      
      streamRef.current.getTracks().forEach(track => {
        track.stop();
        console.log(`🛑 Stopped ${track.kind}:`, track.label);
      });
      
      streamRef.current = null;
      setStream(null);
      setHasPermission(false);
      isRequestingRef.current = false;
      
      console.log('✅ All media released');
    }
  }, []);

 useEffect(() => {
  mountedRef.current = true;

  console.log('🔄 useCameraAccess effect triggered:', {
    isInMeetingArea,
    hasStream: !!streamRef.current,
    isRequesting: isRequestingRef.current
  });

  // ✅ CRITICAL: Don't re-request if already have stream
  if (streamRef.current) {
    console.log('⏸️ Already have media stream - skipping request');
    return;
  }

  if (isInMeetingArea && !isRequestingRef.current) {
    console.log('🚪 ENTERED meeting area - requesting camera + mic NOW');
    
    // Small delay to ensure component is fully mounted
    const timer = setTimeout(() => {
      if (!streamRef.current && !isRequestingRef.current) {
        requestCamera().catch(err => {
          console.error('❌ Failed to request media:', err);
        });
      }
    }, 100);

    return () => clearTimeout(timer);
  } else if (!isInMeetingArea) {
    console.log('🚪 LEFT meeting area - releasing camera + mic');
    releaseCamera();
  }

  return () => {
    console.log('🧹 Cleanup - releasing all media');
    mountedRef.current = false;
    // Only release if actually leaving (not during re-renders)
    if (!isInMeetingArea) {
      releaseCamera();
    }
  };
}, [isInMeetingArea]); // ✅ CRITICAL: Remove requestCamera and releaseCamera from dependencies

  const toggleCamera = useCallback(async () => {
    if (stream) {
      releaseCamera();
      return false;
    } else {
      await requestCamera();
      return true;
    }
  }, [stream, requestCamera, releaseCamera]);

  return {
    stream,
    isLoading,
    error,
    hasPermission,
    requestCamera,
    releaseCamera,
    toggleCamera
  };
};

export default useCameraAccess;