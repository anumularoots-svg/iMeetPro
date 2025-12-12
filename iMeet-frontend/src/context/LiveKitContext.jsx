// src/context/LiveKitContext.jsx - FIXED Size Property Issue
import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react';
import { 
  Room, 
  RoomEvent, 
  Track, 
  RemoteTrack, 
  RemoteParticipant,
  LocalParticipant,
  DataPacket_Kind,
  ConnectionQuality,
  DisconnectReason,
  ParticipantEvent,
  TrackEvent,
  LocalTrackPublication,
  RemoteTrackPublication
} from 'livekit-client';
import { API_BASE_URL } from '../utils/constants';

const LiveKitContext = createContext();

// Export the context for use in other files
export { LiveKitContext };

export const useLiveKit = () => {
  const context = useContext(LiveKitContext);
  if (!context) {
    throw new Error('useLiveKit must be used within a LiveKitProvider');
  }
  return context;
};

export const LiveKitProvider = ({ children }) => {
  // Core LiveKit state
  const [room, setRoom] = useState(null);
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [connectionError, setConnectionError] = useState(null);
  const [currentMeetingId, setCurrentMeetingId] = useState(null);
  
  // Participants and media state
  const [localParticipant, setLocalParticipant] = useState(null);
  const [remoteParticipants, setRemoteParticipants] = useState(new Map());
  const [localTracks, setLocalTracks] = useState(new Map());
  const [remoteTracks, setRemoteTracks] = useState(new Map());
  
  // Meeting state
  const [isAudioEnabled, setIsAudioEnabled] = useState(true);
  const [isVideoEnabled, setIsVideoEnabled] = useState(true);
  const [isScreenSharing, setIsScreenSharing] = useState(false);
  const [connectionQuality, setConnectionQuality] = useState('good');
  const [participantCount, setParticipantCount] = useState(0);
  
  // Event listeners
  const eventListeners = useRef(new Map());
  const roomRef = useRef(null);
  const currentUserRef = useRef(null);
  
  // Event listener management
  const addEventListener = useCallback((event, callback) => {
    if (!eventListeners.current.has(event)) {
      eventListeners.current.set(event, new Set());
    }
    eventListeners.current.get(event).add(callback);
  }, []);

  const removeEventListener = useCallback((event, callback) => {
    if (eventListeners.current.has(event)) {
      eventListeners.current.get(event).delete(callback);
    }
  }, []);

  const emitEvent = useCallback((event, data) => {
    if (eventListeners.current.has(event)) {
      eventListeners.current.get(event).forEach(callback => {
        try {
          callback(data);
        } catch (error) {
          console.error(`Error in event listener for ${event}:`, error);
        }
      });
    }
  }, []);

  // Setup room event listeners
  const setupRoomEventListeners = useCallback((room) => {
    // Connection events
    room.on(RoomEvent.Connected, () => {
      console.log('✅ LiveKit room connected');
      setConnected(true);
      setConnecting(false);
      setConnectionError(null);
      setLocalParticipant(room.localParticipant);
      
      emitEvent('connected', { 
        room: room.name,
        meeting_id: currentMeetingId,
        connection_id: room.localParticipant.sid
      });
    });

    room.on(RoomEvent.Disconnected, (reason) => {
      console.log('❌ LiveKit room disconnected:', reason);
      setConnected(false);
      setConnecting(false);
      setLocalParticipant(null);
      
      if (reason !== DisconnectReason.CLIENT_INITIATED) {
        setConnectionError(`Disconnected: ${reason}`);
      }
      
      emitEvent('disconnected', { reason });
    });

    room.on(RoomEvent.Reconnecting, () => {
      console.log('🔄 LiveKit reconnecting...');
      setConnecting(true);
      emitEvent('reconnecting', {});
    });

    room.on(RoomEvent.Reconnected, () => {
      console.log('✅ LiveKit reconnected');
      setConnected(true);
      setConnecting(false);
      setConnectionError(null);
      emitEvent('reconnected', {});
    });

    // Participant events
    room.on(RoomEvent.ParticipantConnected, (participant) => {
      console.log('👤 Participant joined:', participant.identity);
      
      setRemoteParticipants(prev => {
        const newMap = new Map(prev);
        newMap.set(participant.sid, participant);
        return newMap;
      });

      setParticipantCount(prev => prev + 1);
      
      // Setup participant event listeners
      setupParticipantEventListeners(participant);
      
      emitEvent('user_joined', {
        participant_id: participant.sid,
        connection_id: participant.sid,
        user_id: participant.identity,
        full_name: participant.name || participant.identity,
        name: participant.name || participant.identity,
        is_video_enabled: participant.isCameraEnabled,
        is_audio_enabled: participant.isMicrophoneEnabled,
        role: participant.metadata ? JSON.parse(participant.metadata).role : 'participant'
      });
    });

    room.on(RoomEvent.ParticipantDisconnected, (participant) => {
      console.log('👋 Participant left:', participant.identity);
      
      setRemoteParticipants(prev => {
        const newMap = new Map(prev);
        newMap.delete(participant.sid);
        return newMap;
      });

      setParticipantCount(prev => Math.max(0, prev - 1));

      emitEvent('user_left', {
        participant_id: participant.sid,
        connection_id: participant.sid,
        user_id: participant.identity
      });
    });

    // Track events
    room.on(RoomEvent.TrackSubscribed, (track, publication, participant) => {
      console.log('📹 Track subscribed:', track.kind, participant.identity);
      
      setRemoteTracks(prev => {
        const newMap = new Map(prev);
        const key = `${participant.sid}-${track.kind}`;
        newMap.set(key, { track, publication, participant });
        return newMap;
      });

      // Handle screen share detection
      if (track.source === Track.Source.ScreenShare) {
        setIsScreenSharing(true);
        emitEvent('screen_share_started', {
          user_id: participant.identity,
          connection_id: participant.sid,
          user_name: participant.name || participant.identity
        });
      }

      emitEvent('track-subscribed', { track, publication, participant });
    });

    room.on(RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
      console.log('📹 Track unsubscribed:', track.kind, participant.identity);
      
      setRemoteTracks(prev => {
        const newMap = new Map(prev);
        const key = `${participant.sid}-${track.kind}`;
        newMap.delete(key);
        return newMap;
      });

      // Handle screen share stop
      if (track.source === Track.Source.ScreenShare) {
        setIsScreenSharing(false);
        emitEvent('screen_share_stopped', {
          user_id: participant.identity,
          connection_id: participant.sid
        });
      }

      emitEvent('track-unsubscribed', { track, publication, participant });
    });

    // Data events - handle chat and reactions
    room.on(RoomEvent.DataReceived, (payload, participant) => {
      try {
        const message = JSON.parse(new TextDecoder().decode(payload));
        console.log('📨 Data received from', participant?.identity, ':', message);
        
        // Route message based on type
        switch (message.type) {
          case 'chat-message':
            emitEvent('chat-message', {
              ...message,
              user_id: participant?.identity,
              user_name: participant?.name || participant?.identity,
              connection_id: participant?.sid,
              timestamp: message.timestamp || Date.now()
            });
            break;
          case 'reaction':
            emitEvent('reaction_received', {
              emoji: message.emoji,
              user_name: participant?.name || participant?.identity,
              user_id: participant?.identity,
              participant_id: participant?.sid,
              timestamp: message.timestamp || Date.now()
            });
            break;
          case 'participants_synced':
            emitEvent('participants_synced', message);
            break;
          default:
            emitEvent(message.type, message);
        }
      } catch (error) {
        console.error('❌ Error parsing data message:', error);
      }
    });

    // Connection quality events
    room.on(RoomEvent.ConnectionQualityChanged, (quality, participant) => {
      const qualityString = quality === ConnectionQuality.Excellent ? 'excellent' :
                           quality === ConnectionQuality.Good ? 'good' :
                           quality === ConnectionQuality.Poor ? 'poor' : 'unknown';
      
      if (!participant) {
        // Local participant quality
        setConnectionQuality(qualityString);
      }
      
      emitEvent('connection-quality-changed', { quality: qualityString, participant });
    });

    // Track muted/unmuted events
    room.on(RoomEvent.TrackMuted, (publication, participant) => {
      console.log(`🔇 Track muted: ${publication.kind} from ${participant.identity}`);
      emitEvent(`${publication.kind}_toggle`, {
        user_id: participant.identity,
        connection_id: participant.sid,
        is_enabled: false
      });
    });

    room.on(RoomEvent.TrackUnmuted, (publication, participant) => {
      console.log(`🔊 Track unmuted: ${publication.kind} from ${participant.identity}`);
      emitEvent(`${publication.kind}_toggle`, {
        user_id: participant.identity,
        connection_id: participant.sid,
        is_enabled: true
      });
    });

  }, [emitEvent, currentMeetingId]);

  // Setup participant event listeners
  const setupParticipantEventListeners = useCallback((participant) => {
    participant.on(ParticipantEvent.TrackMuted, (publication) => {
      console.log(`🔇 ${participant.identity} muted ${publication.kind}`);
      emitEvent(`${publication.kind === Track.Kind.Audio ? 'audio' : 'video'}_toggle`, {
        user_id: participant.identity,
        connection_id: participant.sid,
        is_enabled: false
      });
    });

    participant.on(ParticipantEvent.TrackUnmuted, (publication) => {
      console.log(`🔊 ${participant.identity} unmuted ${publication.kind}`);
      emitEvent(`${publication.kind === Track.Kind.Audio ? 'audio' : 'video'}_toggle`, {
        user_id: participant.identity,
        connection_id: participant.sid,
        is_enabled: true
      });
    });
  }, [emitEvent]);

  // Connect to meeting via LiveKit
  const connectToMeeting = useCallback(async (meetingId, currentUser) => {
    if (roomRef.current) {
      console.log('⚠️ Already connected to a room, disconnecting first');
      await disconnect();
    }

    try {
      console.log('🔗 Connecting to LiveKit room for meeting:', meetingId);
      setConnecting(true);
      setConnectionError(null);
      setCurrentMeetingId(meetingId);
      currentUserRef.current = currentUser;

      // Get access token from backend
      const response = await fetch(`${API_BASE_URL}/api/livekit/join-meeting/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meetingId: meetingId,
          userId: currentUser?.id,
          displayName: currentUser?.full_name || currentUser?.name || 'Participant',
          isHost: currentUser?.isHost || false
        })
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to get access token');
      }

      const { access_token, room_name, livekit_url } = await response.json();
      
      console.log('🎫 Received access token for room:', room_name);

      // Create and connect to room
      const newRoom = new Room({
        adaptiveStream: true,
        dynacast: true,
        publishDefaults: {
          videoSimulcast: true,
          audioPreset: 'music',
          videoCodec: 'vp8'
        }
      });

      roomRef.current = newRoom;
      setRoom(newRoom);

      // Setup event listeners before connecting
      setupRoomEventListeners(newRoom);

      // Connect to LiveKit room
      await newRoom.connect(livekit_url, access_token);
      
      // Enable camera and microphone by default
      await newRoom.localParticipant.enableCameraAndMicrophone();
      
      // 🔧 FIXED: Handle participant count correctly
      const participantsMap = newRoom.remoteParticipants;
      let participantCountValue = 1; // Local participant
      
      if (participantsMap) {
        if (typeof participantsMap.size === 'number') {
          // participantsMap is a Map
          participantCountValue += participantsMap.size;
        } else if (Array.isArray(participantsMap)) {
          // participantsMap is an Array
          participantCountValue += participantsMap.length;
        } else {
          // participantsMap is an object, get keys length
          participantCountValue += Object.keys(participantsMap).length;
        }
      }
      
      setParticipantCount(participantCountValue);
      
      console.log('✅ Successfully connected to LiveKit room');
      return true;

    } catch (error) {
      console.error('❌ Failed to connect to LiveKit room:', error);
      setConnecting(false);
      setConnectionError(error.message);
      throw error;
    }
  }, [setupRoomEventListeners]);

  // Disconnect from meeting
  const disconnect = useCallback(async () => {
    try {
      if (roomRef.current) {
        console.log('🔌 Disconnecting from LiveKit room');
        
        // Leave meeting on backend
        if (currentMeetingId && currentUserRef.current) {
          try {
            await fetch(`${API_BASE_URL}/api/livekit/leave-meeting/`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify({
                meeting_id: currentMeetingId,
                user_id: currentUserRef.current.id,
                participant_identity: roomRef.current.localParticipant.identity
              })
            });
          } catch (error) {
            console.warn('Failed to notify backend of leave:', error);
          }
        }
        
        roomRef.current.disconnect();
        roomRef.current = null;
      }
      
      // Reset state
      setRoom(null);
      setConnected(false);
      setConnecting(false);
      setConnectionError(null);
      setCurrentMeetingId(null);
      setLocalParticipant(null);
      setRemoteParticipants(new Map());
      setLocalTracks(new Map());
      setRemoteTracks(new Map());
      setIsAudioEnabled(true);
      setIsVideoEnabled(true);
      setIsScreenSharing(false);
      setParticipantCount(0);
      currentUserRef.current = null;
      
    } catch (error) {
      console.error('❌ Error disconnecting:', error);
    }
  }, [currentMeetingId]);

  // Send data message
  const sendMessage = useCallback((type, data) => {
    if (!roomRef.current || !connected) {
      console.warn('⚠️ Cannot send message: not connected to room');
      return false;
    }

    try {
      const message = {
        type,
        ...data,
        timestamp: Date.now(),
        sender_id: roomRef.current.localParticipant.sid,
        user_id: currentUserRef.current?.id,
        user_name: currentUserRef.current?.full_name || currentUserRef.current?.name
      };

      const encoder = new TextEncoder();
      const data_packet = encoder.encode(JSON.stringify(message));
      
      roomRef.current.localParticipant.publishData(data_packet, DataPacket_Kind.RELIABLE);
      
      console.log('✅ Data message sent:', type);
      return true;
    } catch (error) {
      console.error('❌ Failed to send data message:', error);
      return false;
    }
  }, [connected]);

  // Send chat message
  const sendChatMessage = useCallback((message) => {
    return sendMessage('chat-message', { message });
  }, [sendMessage]);

  // Send reaction
  const sendReaction = useCallback((emoji) => {
    return sendMessage('reaction', { emoji });
  }, [sendMessage]);

  // Toggle audio
  const toggleAudio = useCallback(async () => {
    if (!roomRef.current) return false;

    try {
      const enabled = roomRef.current.localParticipant.isMicrophoneEnabled;
      await roomRef.current.localParticipant.setMicrophoneEnabled(!enabled);
      setIsAudioEnabled(!enabled);
      
      // Notify backend
      if (currentMeetingId) {
        fetch(`${API_BASE_URL}/api/livekit/update-status/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            meeting_id: currentMeetingId,
            user_id: currentUserRef.current?.id,
            status_type: 'audio',
            is_enabled: !enabled
          })
        }).catch(console.warn);
      }
      
      return !enabled;
    } catch (error) {
      console.error('❌ Failed to toggle audio:', error);
      return isAudioEnabled;
    }
  }, [currentMeetingId, isAudioEnabled]);

  // Toggle video
  const toggleVideo = useCallback(async () => {
    if (!roomRef.current) return false;

    try {
      const enabled = roomRef.current.localParticipant.isCameraEnabled;
      await roomRef.current.localParticipant.setCameraEnabled(!enabled);
      setIsVideoEnabled(!enabled);
      
      // Notify backend
      if (currentMeetingId) {
        fetch(`${API_BASE_URL}/api/livekit/update-status/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            meeting_id: currentMeetingId,
            user_id: currentUserRef.current?.id,
            status_type: 'video',
            is_enabled: !enabled
          })
        }).catch(console.warn);
      }
      
      return !enabled;
    } catch (error) {
      console.error('❌ Failed to toggle video:', error);
      return isVideoEnabled;
    }
  }, [currentMeetingId, isVideoEnabled]);

  // Start screen share
  const startScreenShare = useCallback(async () => {
    if (!roomRef.current) throw new Error('Not connected to room');

    try {
      await roomRef.current.localParticipant.setScreenShareEnabled(true);
      setIsScreenSharing(true);
      
      // Notify backend
      if (currentMeetingId) {
        fetch(`${API_BASE_URL}/api/livekit/record-event/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            meeting_id: currentMeetingId,
            user_id: currentUserRef.current?.id,
            event_type: 'screen_share_started',
            event_data: { timestamp: Date.now() }
          })
        }).catch(console.warn);
      }
      
      return true;
    } catch (error) {
      console.error('❌ Failed to start screen share:', error);
      throw error;
    }
  }, [currentMeetingId]);

  // Stop screen share
  const stopScreenShare = useCallback(async () => {
    if (!roomRef.current) return false;

    try {
      await roomRef.current.localParticipant.setScreenShareEnabled(false);
      setIsScreenSharing(false);
      
      // Notify backend
      if (currentMeetingId) {
        fetch(`${API_BASE_URL}/api/livekit/record-event/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            meeting_id: currentMeetingId,
            user_id: currentUserRef.current?.id,
            event_type: 'screen_share_stopped',
            event_data: { timestamp: Date.now() }
          })
        }).catch(console.warn);
      }
      
      return true;
    } catch (error) {
      console.error('❌ Failed to stop screen share:', error);
      return false;
    }
  }, [currentMeetingId]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (roomRef.current) {
        roomRef.current.disconnect();
      }
    };
  }, []);

  const contextValue = {
    // Connection state
    room,
    connected,
    connecting,
    connectionError,
    currentMeetingId,
    
    // Participants
    localParticipant,
    remoteParticipants,
    participantCount,
    
    // Media state
    isAudioEnabled,
    isVideoEnabled,
    isScreenSharing,
    connectionQuality,
    localTracks,
    remoteTracks,
    
    // Methods
    connectToMeeting,
    disconnect,
    sendMessage,
    sendChatMessage,
    sendReaction,
    toggleAudio,
    toggleVideo,
    startScreenShare,
    stopScreenShare,
    
    // Event management
    addEventListener,
    removeEventListener
  };

  return (
    <LiveKitContext.Provider value={contextValue}>
      {children}
    </LiveKitContext.Provider>
  );
};