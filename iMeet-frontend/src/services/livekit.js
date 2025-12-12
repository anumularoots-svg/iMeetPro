// src/services/livekit.js - Enhanced Version with Immediate Audio Support
import { 
  Room, 
  RoomEvent, 
  Track, 
  RemoteTrack, 
  RemoteParticipant,
  LocalParticipant,
  DataPacket_Kind,
  ConnectionQuality,
  ConnectionState,
  createLocalTracks,
  createLocalAudioTrack,
  createLocalVideoTrack,
  LocalVideoTrack,
  LocalAudioTrack
} from 'livekit-client';

class LiveKitService {
  constructor() {
    this.room = null;
    this.token = null;
    this.serverUrl = import.meta.env.VITE_LIVEKIT_URL || 'ws://localhost:7880';
    this.localAudioTrack = null;
    this.localVideoTrack = null;
    this.isAudioEnabled = false;
    this.isVideoEnabled = false;
  }

  async connect(roomName, participantName, token, options = {}) {
    try {
      // Create room with optimized settings
      this.room = new Room({
        adaptiveStream: true,
        dynacast: true,
        publishDefaults: {
          videoSimulcast: true,
          audioPreset: 'speech',
          videoCodec: 'h264',
          stopMicTrackOnMute: false,
          stopVideoTrackOnMute: false
        },
        connectOptions: {
          autoSubscribe: true,
          maxRetries: 3,
          peerConnectionTimeout: 15000
        }
      });
      
      // Set up event listeners before connecting
      this.setupEventListeners();
      
      // Connect to room
      await this.room.connect(this.serverUrl, token, {
        autoSubscribe: true
      });
      
      console.log('✅ Connected to LiveKit room:', roomName);
      
      // CRITICAL: Initialize audio immediately after connection
      if (options.enableAudio !== false) {
        await this.enableAudioImmediately();
      }
      
      // Initialize video if requested
      if (options.enableVideo) {
        await this.enableVideo();
      }
      
      return this.room;
    } catch (error) {
      console.error('❌ LiveKit connection failed:', error);
      throw error;
    }
  }

  // CRITICAL FIX: Immediate audio initialization
  async enableAudioImmediately() {
    try {
      if (!this.room?.localParticipant) {
        console.warn('⚠️ No room or local participant for audio');
        return false;
      }

      // Check if audio is already published
      const existingAudioPub = this.room.localParticipant.getTrackPublication(Track.Source.Microphone);
      if (existingAudioPub?.track) {
        console.log('🎤 Audio already published, ensuring it is unmuted...');
        await existingAudioPub.track.unmute();
        this.localAudioTrack = existingAudioPub.track;
        this.isAudioEnabled = true;
        return true;
      }

      console.log('🎤 Creating and publishing audio track immediately...');
      
      // Create audio track with optimized settings
      const audioTrack = await createLocalAudioTrack({
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        channelCount: 2,
        sampleRate: 48000
      });

      this.localAudioTrack = audioTrack;

      // Publish audio track immediately with high priority
      const publication = await this.room.localParticipant.publishTrack(audioTrack, {
        name: 'microphone',
        source: Track.Source.Microphone,
        dtx: true,
        red: true,
        simulcast: false,
        priority: 'high'
      });
      
      console.log('✅ Audio track published and unmuted successfully');
      
      // Ensure track is unmuted
      await audioTrack.unmute();
      
      this.isAudioEnabled = true;
      
      return true;
    } catch (error) {
      console.error('❌ Audio initialization failed:', error);
      
      // Try simpler audio constraints as fallback
      try {
        console.log('🎤 Trying fallback audio initialization...');
        const audioTrack = await createLocalAudioTrack({
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        });
        
        this.localAudioTrack = audioTrack;
        
        await this.room.localParticipant.publishTrack(audioTrack, {
          source: Track.Source.Microphone,
          priority: 'high'
        });
        
        await audioTrack.unmute();
        this.isAudioEnabled = true;
        
        console.log('✅ Fallback audio initialization successful');
        return true;
      } catch (fallbackError) {
        console.error('❌ Fallback audio initialization also failed:', fallbackError);
        return false;
      }
    }
  }

  async enableVideo() {
    try {
      if (!this.room?.localParticipant) {
        console.warn('⚠️ No room or local participant for video');
        return false;
      }

      // Check if video is already published
      const existingVideoPub = this.room.localParticipant.getTrackPublication(Track.Source.Camera);
      if (existingVideoPub?.track) {
        console.log('📹 Video already published, ensuring it is unmuted...');
        await existingVideoPub.track.unmute();
        this.localVideoTrack = existingVideoPub.track;
        this.isVideoEnabled = true;
        return true;
      }

      console.log('📹 Creating and publishing video track...');
      
      const videoTrack = await createLocalVideoTrack({
        resolution: {
          width: { ideal: 640 },
          height: { ideal: 480 },
          frameRate: { ideal: 15 }
        },
        facingMode: 'user'
      });

      this.localVideoTrack = videoTrack;

      const publication = await this.room.localParticipant.publishTrack(videoTrack, {
        name: 'camera',
        source: Track.Source.Camera,
        simulcast: true
      });
      
      console.log('✅ Video track published successfully');
      
      this.isVideoEnabled = true;
      
      return true;
    } catch (error) {
      console.error('❌ Video enable failed:', error);
      return false;
    }
  }

  async toggleAudio() {
    try {
      if (!this.room?.localParticipant) return false;

      const audioPublication = this.room.localParticipant.getTrackPublication(Track.Source.Microphone);
      
      if (!audioPublication?.track) {
        // No audio track, enable it
        return await this.enableAudioImmediately();
      }

      if (audioPublication.track.isMuted) {
        await audioPublication.track.unmute();
        this.isAudioEnabled = true;
        console.log('🎤 Audio unmuted');
      } else {
        await audioPublication.track.mute();
        this.isAudioEnabled = false;
        console.log('🔇 Audio muted');
      }
      
      return this.isAudioEnabled;
    } catch (error) {
      console.error('❌ Audio toggle failed:', error);
      return false;
    }
  }

  async toggleVideo() {
    try {
      if (!this.room?.localParticipant) return false;

      const videoPublication = this.room.localParticipant.getTrackPublication(Track.Source.Camera);
      
      if (!videoPublication?.track) {
        // No video track, enable it
        return await this.enableVideo();
      }

      if (videoPublication.track.isMuted) {
        await videoPublication.track.unmute();
        this.isVideoEnabled = true;
        console.log('📹 Video unmuted');
      } else {
        await videoPublication.track.mute();
        this.isVideoEnabled = false;
        console.log('📹 Video muted');
      }
      
      return this.isVideoEnabled;
    } catch (error) {
      console.error('❌ Video toggle failed:', error);
      return false;
    }
  }

  setupEventListeners() {
    this.room.on(RoomEvent.Connected, () => {
      console.log('🔗 LiveKit room connected');
    });

    this.room.on(RoomEvent.ParticipantConnected, (participant) => {
      console.log('👤 Participant joined:', participant.identity);
    });

    this.room.on(RoomEvent.ParticipantDisconnected, (participant) => {
      console.log('👋 Participant left:', participant.identity);
    });

    this.room.on(RoomEvent.TrackSubscribed, (track, publication, participant) => {
      console.log('📹 Track subscribed:', track.kind, participant.identity);
    });

    this.room.on(RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
      console.log('📹 Track unsubscribed:', track.kind, participant.identity);
    });

    this.room.on(RoomEvent.DataReceived, (payload, participant) => {
      try {
        const message = JSON.parse(new TextDecoder().decode(payload));
        console.log('📨 Data received:', message);
      } catch (error) {
        console.error('Failed to parse data message:', error);
      }
    });

    this.room.on(RoomEvent.Disconnected, (reason) => {
      console.log('🔌 Disconnected:', reason);
      this.cleanup();
    });

    this.room.on(RoomEvent.ConnectionStateChanged, (state) => {
      console.log('🔄 Connection state:', state);
    });
  }

  // Send data messages (replaces WebSocket messages)
  sendData(message) {
    if (this.room && this.room.localParticipant) {
      try {
        const data = JSON.stringify(message);
        this.room.localParticipant.publishData(
          new TextEncoder().encode(data),
          DataPacket_Kind.RELIABLE
        );
        return true;
      } catch (error) {
        console.error('Failed to send data:', error);
        return false;
      }
    }
    return false;
  }

  // Send chat message
  sendChatMessage(message, userName = 'Anonymous') {
    return this.sendData({
      type: 'chat-message',
      message: message,
      user_name: userName,
      timestamp: Date.now()
    });
  }

  // Send reaction
  sendReaction(emoji, userName = 'Anonymous') {
    return this.sendData({
      type: 'reaction',
      emoji: emoji,
      user_name: userName,
      timestamp: Date.now()
    });
  }

  // Get participants
  getParticipants() {
    if (!this.room) return [];
    
    const participants = [];
    
    // Add local participant
    if (this.room.localParticipant) {
      participants.push({
        identity: this.room.localParticipant.identity,
        name: this.room.localParticipant.name,
        isLocal: true,
        audioEnabled: this.isAudioEnabled,
        videoEnabled: this.isVideoEnabled
      });
    }
    
    // Add remote participants
    this.room.remoteParticipants.forEach(participant => {
      participants.push({
        identity: participant.identity,
        name: participant.name,
        isLocal: false,
        audioEnabled: participant.isMicrophoneEnabled,
        videoEnabled: participant.isCameraEnabled
      });
    });
    
    return participants;
  }

  // Clean up tracks
  cleanup() {
    if (this.localAudioTrack) {
      this.localAudioTrack.stop();
      this.localAudioTrack = null;
    }
    
    if (this.localVideoTrack) {
      this.localVideoTrack.stop();
      this.localVideoTrack = null;
    }
    
    this.isAudioEnabled = false;
    this.isVideoEnabled = false;
  }

  disconnect() {
    if (this.room) {
      console.log('🔌 Disconnecting from LiveKit...');
      
      // Clean up tracks
      this.cleanup();
      
      // Disconnect from room
      this.room.disconnect();
      this.room = null;
    }
  }

  // Get room state
  getRoomState() {
    if (!this.room) return null;
    
    return {
      name: this.room.name,
      state: this.room.state,
      participantCount: this.room.remoteParticipants.size + 1,
      isConnected: this.room.state === ConnectionState.Connected,
      audioEnabled: this.isAudioEnabled,
      videoEnabled: this.isVideoEnabled
    };
  }
}

export default new LiveKitService();