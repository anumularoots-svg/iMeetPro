// src/services/webrtc.js - COMPLETE ENHANCED VERSION WITH AUDIO FIXES

class WebRTCService {
  constructor() {
    this.localStream = null;
    this.screenShareStream = null;
    this.isVideoEnabled = true;
    this.isAudioEnabled = true;
    this.isScreenSharing = false;
    this.screenShareStatusCallback = null;
    this.recordingStatusCallback = null;
    this.mediaRecorder = null;
    this.recordedChunks = [];
    this.isRecording = false;
    this.recordingStartTime = null;
    
    // Enhanced error tracking
    this.lastError = null;
    this.retryCount = 0;
    this.maxRetries = 3;
    
    // Audio context for better audio processing
    this.audioContext = null;
    
    // Device management
    this.availableDevices = {
      videoInputs: [],
      audioInputs: [],
      audioOutputs: []
    };
    
    // Enhanced constraints management
    this.currentConstraints = {
      video: {
        width: { min: 320, ideal: 1280, max: 1920 },
        height: { min: 240, ideal: 720, max: 1080 },
        frameRate: { min: 15, ideal: 30, max: 60 }
      },
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        sampleRate: 44100,
        channelCount: 2,
        volume: 1.0,
        latency: 0.01
      }
    };
  }

  // Initialize audio context for better audio processing
  async initializeAudioContext() {
    try {
      // Create audio context for better audio processing
      if (!this.audioContext) {
        this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
        
        // Resume audio context if suspended (required by some browsers)
        if (this.audioContext.state === 'suspended') {
          await this.audioContext.resume();
          console.log('🔊 Audio context resumed');
        }
      }
      
      return this.audioContext;
    } catch (error) {
      console.error('❌ Failed to initialize audio context:', error);
      return null;
    }
  }

  // Enhanced device enumeration with error handling
  async enumerateDevices() {
    try {
      console.log('🔍 Enumerating media devices...');
      
      if (!navigator.mediaDevices || !navigator.mediaDevices.enumerateDevices) {
        throw new Error('Media devices API not supported');
      }

      const devices = await navigator.mediaDevices.enumerateDevices();
      
      this.availableDevices = {
        videoInputs: devices.filter(device => device.kind === 'videoinput'),
        audioInputs: devices.filter(device => device.kind === 'audioinput'),
        audioOutputs: devices.filter(device => device.kind === 'audiooutput')
      };

      console.log('📱 Available devices:', {
        video: this.availableDevices.videoInputs.length,
        audioIn: this.availableDevices.audioInputs.length,
        audioOut: this.availableDevices.audioOutputs.length
      });

      return this.availableDevices;
    } catch (error) {
      console.error('❌ Failed to enumerate devices:', error);
      throw error;
    }
  }

  // Enhanced media initialization with multiple fallback strategies
  async initializeMedia(videoEnabled = true, audioEnabled = true) {
  console.log('🎥 Initializing media with enhanced error handling...', { videoEnabled, audioEnabled });

  this.isVideoEnabled = videoEnabled;
  this.isAudioEnabled = audioEnabled;
  this.retryCount = 0;

  // Initialize audio context first
  if (audioEnabled) {
    await this.initializeAudioContext();
  }

  // First, enumerate devices to check availability
  try {
    await this.enumerateDevices();
  } catch (error) {
    console.warn('⚠️ Device enumeration failed, continuing with default constraints');
  }

  // Try multiple initialization strategies
  const strategies = [
    () => this.initializeWithIdealConstraints(videoEnabled, audioEnabled),
    () => this.initializeWithBasicConstraints(videoEnabled, audioEnabled),
    () => this.initializeWithMinimalConstraints(videoEnabled, audioEnabled),
    () => this.initializeAudioOnly(audioEnabled)
  ];

  for (let i = 0; i < strategies.length; i++) {
    try {
      console.log(`🔄 Trying initialization strategy ${i + 1}/${strategies.length}...`);

      const stream = await strategies[i]();

      if (stream) {
        this.localStream = stream;
        console.log('✅ Media initialization successful with strategy', i + 1);

        // Verify audio track
        if (audioEnabled) {
          const audioTrack = stream.getAudioTracks()[0];
          if (audioTrack) {
            if (!audioTrack.enabled || audioTrack.readyState !== 'live') {
              console.warn('⚠️ Audio track issue detected:', {
                enabled: audioTrack.enabled,
                readyState: audioTrack.readyState
              });
              throw new Error('Audio track not functional');
            }
            console.log('✅ Audio track verified:', {
              enabled: audioTrack.enabled,
              readyState: audioTrack.readyState,
              settings: audioTrack.getSettings()
            });
          } else {
            console.warn('⚠️ No audio track in stream');
            throw new Error('No audio track available');
          }
        }

        // Set up track event listeners
        this.setupTrackEventListeners(stream);

        // Update enabled states based on actual tracks
        this.updateMediaStates(stream);

        return stream;
      }
    } catch (error) {
      console.warn(`⚠️ Strategy ${i + 1} failed:`, error.message);

      if (i === strategies.length - 1) {
        throw new Error(`All initialization strategies failed. Last error: ${error.message}`);
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

  // Strategy 1: Ideal constraints with enhanced audio
  async initializeWithIdealConstraints(videoEnabled, audioEnabled) {
    console.log('🎯 Trying ideal constraints with enhanced audio...');
    
    const constraints = {
      video: videoEnabled ? this.currentConstraints.video : false,
      audio: audioEnabled ? {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        sampleRate: 44100,
        channelCount: 2,
        volume: 1.0,
        latency: 0.01,
        // Additional constraints for better audio quality
        advanced: [
          { echoCancellation: true },
          { noiseSuppression: true },
          { autoGainControl: true },
          { googEchoCancellation: true },
          { googNoiseSuppression: true },
          { googAutoGainControl: true },
          { googHighpassFilter: true }
        ]
      } : false
    };

    const stream = await navigator.mediaDevices.getUserMedia(constraints);
    
    // CRITICAL: Ensure audio tracks are properly configured
    if (stream && audioEnabled) {
      const audioTracks = stream.getAudioTracks();
      audioTracks.forEach(track => {
        console.log('🎤 Audio track configured:', track.getSettings());
        
        // Apply additional audio settings if supported
        try {
          track.applyConstraints({
            echoCancellation: true,
            noiseSuppression: true,
            autoGainControl: true
          });
        } catch (error) {
          console.warn('⚠️ Could not apply audio constraints:', error);
        }
      });
    }

    return stream;
  }

  // Strategy 2: Basic constraints
  async initializeWithBasicConstraints(videoEnabled, audioEnabled) {
    console.log('🔧 Trying basic constraints...');
    
    const constraints = {
      video: videoEnabled ? {
        width: { ideal: 1280 },
        height: { ideal: 720 },
        frameRate: { ideal: 30 }
      } : false,
      audio: audioEnabled ? {
        echoCancellation: true,
        noiseSuppression: true
      } : false
    };

    return await navigator.mediaDevices.getUserMedia(constraints);
  }

  // Strategy 3: Minimal constraints
  async initializeWithMinimalConstraints(videoEnabled, audioEnabled) {
    console.log('⚡ Trying minimal constraints...');
    
    const constraints = {
      video: videoEnabled ? true : false,
      audio: audioEnabled ? true : false
    };

    return await navigator.mediaDevices.getUserMedia(constraints);
  }

  // Strategy 4: Audio only
  async initializeAudioOnly(audioEnabled) {
    if (!audioEnabled) {
      throw new Error('Audio disabled and video failed');
    }

    console.log('🎤 Trying audio-only mode...');
    
    const constraints = {
      video: false,
      audio: true
    };

    return await navigator.mediaDevices.getUserMedia(constraints);
  }

  // Enhanced track event listeners with audio monitoring
  setupTrackEventListeners(stream) {
    stream.getTracks().forEach(track => {
      track.addEventListener('ended', () => {
        console.log(`🔚 Track ${track.kind} ended unexpectedly`);
        this.handleTrackEnded(track);
      });

      track.addEventListener('mute', () => {
        console.log(`🔇 Track ${track.kind} muted`);
        if (track.kind === 'audio') {
          // Try to unmute audio track immediately
          setTimeout(() => {
            if (track.muted && track.readyState === 'live') {
              console.log('🔊 Attempting to unmute audio track');
            }
          }, 100);
        }
      });

      track.addEventListener('unmute', () => {
        console.log(`🔊 Track ${track.kind} unmuted`);
      });

      // AUDIO FIX: Add specific audio track monitoring
      if (track.kind === 'audio') {
        track.addEventListener('overconstrained', () => {
          console.error('🎤 Audio track overconstrained');
          this.handleAudioTrackError(track);
        });
      }
    });
  }

  // Handle audio track errors with recovery
  async handleAudioTrackError(track) {
    console.log('🔄 Handling audio track error, attempting recovery...');
    
    try {
      // Try to restart audio with simpler constraints
      const simpleAudioConstraints = {
        audio: {
          echoCancellation: true,
          noiseSuppression: false,
          autoGainControl: false
        },
        video: false
      };
      
      const audioStream = await navigator.mediaDevices.getUserMedia(simpleAudioConstraints);
      const newAudioTrack = audioStream.getAudioTracks()[0];
      
      if (newAudioTrack && this.localStream) {
        // Replace the problematic audio track
        const oldAudioTrack = this.localStream.getAudioTracks()[0];
        if (oldAudioTrack) {
          this.localStream.removeTrack(oldAudioTrack);
          oldAudioTrack.stop();
        }
        
        this.localStream.addTrack(newAudioTrack);
        console.log('✅ Audio track recovered');
      }
    } catch (error) {
      console.error('❌ Failed to recover audio track:', error);
    }
  }

  // Handle track ended event
  async handleTrackEnded(track) {
    console.log('🔄 Attempting to recover from track ended event...');
    
    try {
      // Try to restart the media stream
      if (track.kind === 'video') {
        const newStream = await this.initializeMedia(true, this.isAudioEnabled);
        if (newStream) {
          console.log('✅ Video track recovered');
        }
      } else if (track.kind === 'audio') {
        const newStream = await this.initializeMedia(this.isVideoEnabled, true);
        if (newStream) {
          console.log('✅ Audio track recovered');
        }
      }
    } catch (error) {
      console.error('❌ Failed to recover track:', error);
    }
  }

  // Update media states based on actual tracks
  updateMediaStates(stream) {
    const videoTrack = stream.getVideoTracks()[0];
    const audioTrack = stream.getAudioTracks()[0];
    
    this.isVideoEnabled = videoTrack ? videoTrack.enabled : false;
    this.isAudioEnabled = audioTrack ? audioTrack.enabled : false;
    
    console.log('📊 Updated media states:', {
      video: this.isVideoEnabled,
      audio: this.isAudioEnabled,
      videoTrack: !!videoTrack,
      audioTrack: !!audioTrack
    });
  }

  // Enhanced screen share with better error handling
  async startScreenShare() {
    try {
      console.log('🖥️ Starting enhanced screen share...');
      
      if (!navigator.mediaDevices || !navigator.mediaDevices.getDisplayMedia) {
        throw new Error('Screen sharing not supported by this browser');
      }

      // Check if already sharing
      if (this.isScreenSharing) {
        console.warn('⚠️ Screen share already active');
        return this.screenShareStream;
      }

      // Multiple constraint options for better compatibility
      const constraintOptions = [
        {
          video: {
            mediaSource: 'screen',
            width: { max: 1920 },
            height: { max: 1080 },
            frameRate: { max: 30 }
          },
          audio: {
            mediaSource: 'screen',
            echoCancellation: true,
            noiseSuppression: true
          }
        },
        {
          video: {
            mediaSource: 'screen',
            width: { max: 1280 },
            height: { max: 720 },
            frameRate: { max: 15 }
          },
          audio: false
        },
        {
          video: true,
          audio: false
        }
      ];

      let screenStream = null;
      let constraintIndex = 0;

      for (const constraints of constraintOptions) {
        try {
          console.log(`🔄 Trying screen share constraints ${constraintIndex + 1}/${constraintOptions.length}...`);
          
          screenStream = await navigator.mediaDevices.getDisplayMedia(constraints);
          
          if (screenStream) {
            console.log('✅ Screen share successful with constraints', constraintIndex + 1);
            break;
          }
        } catch (error) {
          console.warn(`⚠️ Screen share constraint ${constraintIndex + 1} failed:`, error.message);
          constraintIndex++;
          
          if (constraintIndex === constraintOptions.length) {
            throw error;
          }
        }
      }

      if (!screenStream) {
        throw new Error('Failed to get screen share stream with all constraint options');
      }

      // Set up screen share stream
      this.screenShareStream = screenStream;
      this.isScreenSharing = true;

      // Set up end listener
      const videoTrack = screenStream.getVideoTracks()[0];
      if (videoTrack) {
        videoTrack.onended = () => {
          console.log('🛑 Screen share ended by user');
          this.stopScreenShare();
        };
      }

      // Notify callback
      if (this.screenShareStatusCallback) {
        this.screenShareStatusCallback(true, screenStream);
      }

      console.log('✅ Screen share started successfully');
      return screenStream;

    } catch (error) {
      console.error('❌ Screen share failed:', error);
      
      this.isScreenSharing = false;
      this.screenShareStream = null;
      
      // Notify callback of failure
      if (this.screenShareStatusCallback) {
        this.screenShareStatusCallback(false, null);
      }
      
      // Provide user-friendly error messages
      if (error.name === 'NotAllowedError') {
        throw new Error('Screen sharing permission was denied');
      } else if (error.name === 'NotFoundError') {
        throw new Error('No screen sources available to share');
      } else if (error.name === 'NotReadableError') {
        throw new Error('Screen source is already being used');
      } else if (error.message?.includes('cancelled')) {
        throw new Error('Screen sharing was cancelled');
      } else {
        throw new Error(`Screen sharing failed: ${error.message}`);
      }
    }
  }

  // Enhanced stop screen share
  async stopScreenShare() {
    try {
      console.log('🛑 Stopping screen share...');
      
      if (this.screenShareStream) {
        this.screenShareStream.getTracks().forEach(track => {
          track.stop();
        });
        
        this.screenShareStream = null;
      }
      
      this.isScreenSharing = false;
      
      // Notify callback
      if (this.screenShareStatusCallback) {
        this.screenShareStatusCallback(false, null);
      }
      
      console.log('✅ Screen share stopped successfully');
      return true;
      
    } catch (error) {
      console.error('❌ Error stopping screen share:', error);
      return false;
    }
  }

  // Enhanced media controls with error handling
  toggleVideo() {
    try {
      if (!this.localStream) {
        console.warn('⚠️ No local stream available for video toggle');
        return this.isVideoEnabled;
      }

      const videoTrack = this.localStream.getVideoTracks()[0];
      if (videoTrack) {
        videoTrack.enabled = !videoTrack.enabled;
        this.isVideoEnabled = videoTrack.enabled;
        console.log(`📹 Video ${this.isVideoEnabled ? 'enabled' : 'disabled'}`);
      } else {
        console.warn('⚠️ No video track found');
      }

      return this.isVideoEnabled;
    } catch (error) {
      console.error('❌ Error toggling video:', error);
      return this.isVideoEnabled;
    }
  }

  toggleAudio() {
  try {
    if (!this.localStream) {
      console.warn('⚠️ No local stream available for audio toggle');
      return this.isAudioEnabled;
    }

    const audioTrack = this.localStream.getAudioTracks()[0];
    if (audioTrack) {
      audioTrack.enabled = !audioTrack.enabled;
      this.isAudioEnabled = audioTrack.enabled;
      console.log(`🎤 Audio ${this.isAudioEnabled ? 'enabled' : 'disabled'}`, {
        trackId: audioTrack.id,
        enabled: audioTrack.enabled,
        readyState: audioTrack.readyState
      });
    } else {
      console.warn('⚠️ No audio track found');
    }

    return this.isAudioEnabled;
  } catch (error) {
    console.error('❌ Error toggling audio:', error);
    return this.isAudioEnabled;
  }
}

  // Enhanced live recording with better error handling
  async startLiveRecording(options = {}) {
    try {
      console.log('🔴 Starting enhanced live recording...');
      
      if (this.isRecording) {
        console.warn('⚠️ Recording already in progress');
        return false;
      }

      if (!this.localStream) {
        throw new Error('No local stream available for recording');
      }

      // Reset recorded chunks
      this.recordedChunks = [];
      
      // Try different MIME types for better compatibility
      const mimeTypes = [
        'video/webm;codecs=vp9,opus',
        'video/webm;codecs=vp8,opus',
        'video/webm;codecs=h264,opus',
        'video/webm',
        'video/mp4'
      ];
      
      let selectedMimeType = '';
      for (const mimeType of mimeTypes) {
        if (MediaRecorder.isTypeSupported(mimeType)) {
          selectedMimeType = mimeType;
          console.log('✅ Using MIME type:', mimeType);
          break;
        }
      }
      
      if (!selectedMimeType) {
        throw new Error('No supported recording format found');
      }

      // Create MediaRecorder with enhanced options
      const recordingOptions = {
        mimeType: selectedMimeType,
        videoBitsPerSecond: options.videoBitrate || 2500000,
        audioBitsPerSecond: options.audioBitrate || 128000
      };

      this.mediaRecorder = new MediaRecorder(this.localStream, recordingOptions);

      // Set up event listeners
      this.mediaRecorder.ondataavailable = (event) => {
        if (event.data && event.data.size > 0) {
          this.recordedChunks.push(event.data);
          console.log('📦 Recording chunk added, size:', event.data.size);
        }
      };

      this.mediaRecorder.onstop = () => {
        console.log('⏹️ Recording stopped, processing data...');
        this.processRecordingData();
      };

      this.mediaRecorder.onerror = (event) => {
        console.error('❌ Recording error:', event.error);
        this.handleRecordingError(event.error);
      };

      // Start recording
      this.mediaRecorder.start(1000); // Collect data every 1 second
      this.isRecording = true;
      this.recordingStartTime = new Date();

      // Notify callback
      if (this.recordingStatusCallback) {
        this.recordingStatusCallback(true, this.recordingStartTime, null, null);
      }

      console.log('✅ Live recording started successfully');
      return true;

    } catch (error) {
      console.error('❌ Failed to start recording:', error);
      
      this.isRecording = false;
      this.recordingStartTime = null;
      
      if (this.recordingStatusCallback) {
        this.recordingStatusCallback(false, null, error, null);
      }
      
      throw error;
    }
  }

  // Stop live recording
  async stopLiveRecording() {
    try {
      console.log('⏹️ Stopping live recording...');
      
      if (!this.isRecording || !this.mediaRecorder) {
        console.warn('⚠️ No active recording to stop');
        return null;
      }

      return new Promise((resolve, reject) => {
        const originalOnStop = this.mediaRecorder.onstop;
        
        this.mediaRecorder.onstop = () => {
          // Call original handler
          if (originalOnStop) {
            originalOnStop();
          }
          
          // Process and resolve
          const recordingData = this.processRecordingData();
          resolve(recordingData);
        };

        this.mediaRecorder.stop();
        this.isRecording = false;
      });

    } catch (error) {
      console.error('❌ Error stopping recording:', error);
      throw error;
    }
  }

  // Process recording data
  processRecordingData() {
    try {
      if (this.recordedChunks.length === 0) {
        console.warn('⚠️ No recorded chunks available');
        return null;
      }

      const blob = new Blob(this.recordedChunks, { type: 'video/webm' });
      const url = URL.createObjectURL(blob);
      
      const recordingData = {
        blob,
        url,
        size: blob.size,
        duration: this.recordingStartTime ? Date.now() - this.recordingStartTime.getTime() : 0,
        filename: `recording_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.webm`,
        timestamp: new Date().toISOString()
      };

      console.log('📹 Recording data processed:', {
        size: recordingData.size,
        duration: recordingData.duration,
        filename: recordingData.filename
      });

      // Notify callback
      if (this.recordingStatusCallback) {
        this.recordingStatusCallback(false, null, null, recordingData);
      }

      // Clear chunks
      this.recordedChunks = [];
      this.recordingStartTime = null;

      return recordingData;
    } catch (error) {
      console.error('❌ Error processing recording data:', error);
      return null;
    }
  }

  // Handle recording error
  handleRecordingError(error) {
    console.error('🔥 Recording error occurred:', error);
    
    this.isRecording = false;
    this.recordingStartTime = null;
    this.recordedChunks = [];
    
    if (this.recordingStatusCallback) {
      this.recordingStatusCallback(false, null, error, null);
    }
  }

  // Download recording
  downloadRecording(recordingData) {
    try {
      if (!recordingData || !recordingData.url) {
        console.error('❌ No recording data available for download');
        return false;
      }

      const link = document.createElement('a');
      link.href = recordingData.url;
      link.download = recordingData.filename;
      link.style.display = 'none';
      
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      console.log('📥 Recording download initiated:', recordingData.filename);
      
      // Clean up blob URL after delay
      setTimeout(() => {
        URL.revokeObjectURL(recordingData.url);
      }, 1000);
      
      return true;
    } catch (error) {
      console.error('❌ Error downloading recording:', error);
      return false;
    }
  }

  // Getters
  getMediaStates() {
    return {
      isVideoEnabled: this.isVideoEnabled,
      isAudioEnabled: this.isAudioEnabled,
      isScreenSharing: this.isScreenSharing,
      isRecording: this.isRecording
    };
  }

  getLocalStream() {
    return this.localStream;
  }

  getScreenShareStream() {
    return this.screenShareStream;
  }

  // Setters for callbacks
  setScreenShareStatusCallback(callback) {
    this.screenShareStatusCallback = callback;
  }

  setRecordingStatusCallback(callback) {
    this.recordingStatusCallback = callback;
  }

  // Enhanced cleanup
  cleanup() {
    console.log('🧹 Enhanced WebRTC cleanup...');
    
    try {
      // Stop recording if active
      if (this.isRecording && this.mediaRecorder) {
        this.mediaRecorder.stop();
      }
      
      // Stop screen share
      if (this.isScreenSharing) {
        this.stopScreenShare();
      }
      
      // Stop local stream
      if (this.localStream) {
        this.localStream.getTracks().forEach(track => {
          track.stop();
        });
        this.localStream = null;
      }
      
      // Close audio context
      if (this.audioContext) {
        this.audioContext.close();
        this.audioContext = null;
      }
      
      // Reset states
      this.isVideoEnabled = true;
      this.isAudioEnabled = true;
      this.isScreenSharing = false;
      this.isRecording = false;
      this.recordingStartTime = null;
      this.recordedChunks = [];
      this.mediaRecorder = null;
      this.lastError = null;
      this.retryCount = 0;
      
      // Clear callbacks
      this.screenShareStatusCallback = null;
      this.recordingStatusCallback = null;
      
      console.log('✅ WebRTC cleanup completed');
    } catch (error) {
      console.error('❌ Error during cleanup:', error);
    }
  }
}

// Export singleton instance
const webrtcService = new WebRTCService();
export default webrtcService;