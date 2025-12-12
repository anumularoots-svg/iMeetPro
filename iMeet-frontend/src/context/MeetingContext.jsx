// ENHANCED: src/context/MeetingContext.jsx - Fixed Audio Initialization
import React, { createContext, useContext, useReducer, useCallback, useEffect } from 'react';
import { useLiveKit } from "./LiveKitContext";
import { participantsAPI } from '../services/api';

const MeetingContext = createContext();

const initialState = {
  currentMeeting: null,
  participants: [],
  liveParticipants: [], // NEW: LiveKit + Database participants
  participantStats: { // NEW: Participant statistics
    total: 0,
    active: 0,
    livekit: 0
  },
  localStream: null,
  remoteStreams: {},
  screenShareStream: null,
  screenSharer: null,
  isHost: false,  
  isRecording: false,
  recordingStartTime: null,
  recordingDuration: 0,
  currentRecording: null,
  savedRecordings: [],
  isScreenSharing: false,
  audioEnabled: true,
  videoEnabled: true,
  chatMessages: [],
  reactions: [],
  handRaised: [],
  meetingSettings: {
    waitingRoomEnabled: false,
    recordingEnabled: false,
    chatEnabled: true,
    reactionsEnabled: true,
  },
  connectionStatus: 'disconnected', // disconnected, connecting, connected, error
  isLoading: false,
  error: null,
  // NEW: LiveKit specific state
  livekitConnected: false,
  livekitRoom: null,
  livekitParticipantCount: 0,
};

const meetingReducer = (state, action) => {
  switch (action.type) {
    case 'SET_LOADING':
      return { ...state, isLoading: action.payload };
    
    case 'SET_ERROR':
      return { ...state, error: action.payload, isLoading: false };
    
    case 'CLEAR_ERROR':
      return { ...state, error: null };
    
    case 'JOIN_MEETING_SUCCESS':
      return {
        ...state,
        currentMeeting: action.payload.meeting,
        isHost: action.payload.isHost,
        meetingSettings: action.payload.settings,
        connectionStatus: 'connected',
        isLoading: false,
        error: null,
      };
    
    case 'LEAVE_MEETING':
      return {
        ...initialState,
        connectionStatus: 'disconnected',
      };
    
    // NEW: LiveKit connection management
    case 'LIVEKIT_CONNECTED':
      return {
        ...state,
        livekitConnected: true,
        livekitRoom: action.payload.room,
        connectionStatus: 'connected',
      };
    
    case 'LIVEKIT_DISCONNECTED':
      return {
        ...state,
        livekitConnected: false,
        livekitRoom: null,
        livekitParticipantCount: 0,
      };
    
    case 'UPDATE_PARTICIPANTS':
      return {
        ...state,
        participants: action.payload,
      };
    
    // NEW: LiveKit participants management
    case 'UPDATE_LIVE_PARTICIPANTS':
      return {
        ...state,
        liveParticipants: action.payload.participants,
        participantStats: action.payload.stats,
      };
    
    case 'ADD_PARTICIPANT':
      return {
        ...state,
        participants: [...state.participants, action.payload],
      };
    
    case 'REMOVE_PARTICIPANT':
      return {
        ...state,
        participants: state.participants.filter(p => p.id !== action.payload),
      };
    
    case 'UPDATE_PARTICIPANT':
      return {
        ...state,
        participants: state.participants.map(p =>
          p.id === action.payload.id ? { ...p, ...action.payload.updates } : p
        ),
      };
    
    // NEW: LiveKit participant count
    case 'UPDATE_LIVEKIT_PARTICIPANT_COUNT':
      return {
        ...state,
        livekitParticipantCount: action.payload,
      };
    
    case 'SET_LOCAL_STREAM':
      return {
        ...state,
        localStream: action.payload,
      };
    
    case 'ADD_REMOTE_STREAM':
      return {
        ...state,
        remoteStreams: {
          ...state.remoteStreams,
          [action.payload.participantId]: action.payload.stream,
        },
      };
    
    case 'REMOVE_REMOTE_STREAM':
      return {
        ...state,
        remoteStreams: Object.fromEntries(
          Object.entries(state.remoteStreams).filter(
            ([key]) => key !== action.payload
          )
        ),
      };
    
    case 'TOGGLE_AUDIO':
      return {
        ...state,
        audioEnabled: !state.audioEnabled,
      };
    
    case 'TOGGLE_VIDEO':
      return {
        ...state,
        videoEnabled: !state.videoEnabled,
      };
    
    case 'SET_RECORDING_STATUS':
      return {
        ...state,
        isRecording: action.payload,
      };
    
    case 'SET_SCREEN_SHARING':
      return {
        ...state,
        isScreenSharing: action.payload.isSharing,
        screenShareStream: action.payload.stream,
        screenSharer: action.payload.sharer,
      };
    
    case 'SET_SCREEN_SHARE_STREAM':
      return {
        ...state,
        screenShareStream: action.payload,
      };
    
    case 'SET_SCREEN_SHARER':
      return {
        ...state,
        screenSharer: action.payload,
      };
    
    case 'ADD_CHAT_MESSAGE':
      return {
        ...state,
        chatMessages: [...state.chatMessages, action.payload],
      };
    
    case 'SET_CHAT_MESSAGES':
      return {
        ...state,
        chatMessages: action.payload,
      };
    
    case 'ADD_REACTION':
      return {
        ...state,
        reactions: [...state.reactions, action.payload],
      };
    
    case 'CLEAR_REACTIONS':
      return {
        ...state,
        reactions: state.reactions.filter(
          r => Date.now() - r.timestamp < 5000
        ),
      };
    
    case 'RAISE_HAND':
      return {
        ...state,
        handRaised: [...state.handRaised, action.payload],
      };
    
    case 'LOWER_HAND':
      return {
        ...state,
        handRaised: state.handRaised.filter(h => h.participantId !== action.payload),
      };
    
    case 'UPDATE_MEETING_SETTINGS':
      return {
        ...state,
        meetingSettings: { ...state.meetingSettings, ...action.payload },
      };
    
    case 'SET_CONNECTION_STATUS':
      return {
        ...state,
        connectionStatus: action.payload,
      };
      
    case 'START_LIVE_RECORDING':
      return {
        ...state,
        isRecording: true,
        recordingStartTime: action.payload.startTime,
        recordingDuration: 0,
      };
    
    case 'STOP_LIVE_RECORDING':
      return {
        ...state,
        isRecording: false,
        recordingStartTime: null,
        recordingDuration: 0,
        currentRecording: action.payload.recordingData,
      };
      
    case 'UPDATE_RECORDING_DURATION':
      return {
        ...state,
        recordingDuration: action.payload,
      };
    
    case 'SAVE_RECORDING':
      return {
        ...state,
        savedRecordings: [...state.savedRecordings, action.payload],
        currentRecording: null,
      };
    
    case 'CLEAR_CURRENT_RECORDING':
      return {
        ...state,
        currentRecording: null,
      };
    
    default:
      return state;
  }
};

export const MeetingProvider = ({ children }) => {
  const [state, dispatch] = useReducer(meetingReducer, initialState);
  const { 
    addEventListener, 
    removeEventListener, 
    connectToMeeting,
    disconnect: livekitDisconnect,
    connected: livekitConnected,
    participantCount,
    room
  } = useLiveKit();

  // NEW: LiveKit connection monitoring
  useEffect(() => {
    if (livekitConnected && room) {
      dispatch({
        type: 'LIVEKIT_CONNECTED',
        payload: { room }
      });
    } else {
      dispatch({
        type: 'LIVEKIT_DISCONNECTED'
      });
    }
  }, [livekitConnected, room]);

  // NEW: LiveKit participant count monitoring
  useEffect(() => {
    if (participantCount !== undefined) {
      dispatch({
        type: 'UPDATE_LIVEKIT_PARTICIPANT_COUNT',
        payload: participantCount
      });
    }
  }, [participantCount]);

  // NEW: Load live participants from API
  const loadLiveParticipants = useCallback(async (meetingId) => {
    if (!meetingId) return;
    
    try {
      console.log('📊 MeetingContext: Loading live participants for meeting:', meetingId);
      const response = await participantsAPI.getLiveParticipants(meetingId);
      
      if (response.success) {
        dispatch({
          type: 'UPDATE_LIVE_PARTICIPANTS',
          payload: {
            participants: response.database_participants || [],
            stats: response.summary || { total: 0, active: 0, livekit: 0 }
          }
        });
        
        console.log('✅ MeetingContext: Live participants loaded:', response.summary);
      }
    } catch (error) {
      console.error('❌ MeetingContext: Failed to load live participants:', error);
    }
  }, []);

  // NEW: Record participant join
  const recordParticipantJoin = useCallback(async (meetingId, participantData) => {
    try {
      console.log('📝 MeetingContext: Recording participant join:', participantData);
      const response = await participantsAPI.recordParticipantJoin({
        meeting_id: meetingId,
        user_id: participantData.user_id,
        user_name: participantData.user_name,
        is_host: participantData.is_host
      });
      
      if (response.Message) {
        console.log('✅ MeetingContext: Participant join recorded:', response);
        // Refresh live participants
        await loadLiveParticipants(meetingId);
      }
    } catch (error) {
      console.error('❌ MeetingContext: Failed to record participant join:', error);
    }
  }, [loadLiveParticipants]);

  // NEW: Record participant leave
  const recordParticipantLeave = useCallback(async (meetingId, participantData) => {
    try {
      console.log('📝 MeetingContext: Recording participant leave:', participantData);
      const response = await participantsAPI.recordParticipantLeave({
        meeting_id: meetingId,
        user_id: participantData.user_id
      });
      
      if (response.Message) {
        console.log('✅ MeetingContext: Participant leave recorded:', response);
        // Refresh live participants
        await loadLiveParticipants(meetingId);
      }
    } catch (error) {
      console.error('❌ MeetingContext: Failed to record participant leave:', error);
    }
  }, [loadLiveParticipants]);

  // NEW: Sync LiveKit participants with database
  const syncLiveKitParticipants = useCallback(async (meetingId) => {
    try {
      console.log('🔄 MeetingContext: Syncing LiveKit participants...');
      const response = await participantsAPI.syncLiveKitParticipants(meetingId);
      
      if (response.success) {
        console.log('✅ MeetingContext: Participants synced:', response.sync_results);
        // Refresh live participants after sync
        await loadLiveParticipants(meetingId);
      }
    } catch (error) {
      console.error('❌ MeetingContext: Failed to sync participants:', error);
    }
  }, [loadLiveParticipants]);

  // Enhanced screen share event handling
  useEffect(() => {
    const handleScreenShareStarted = (data) => {
      console.log('📺 Screen share started event in MeetingContext:', data);
      
      dispatch({
        type: 'UPDATE_PARTICIPANT',
        payload: {
          id: data.connection_id || data.user_id,
          updates: { isScreenSharing: true }
        }
      });
      
      dispatch({
        type: 'SET_SCREEN_SHARER',
        payload: {
          connection_id: data.connection_id,
          user_id: data.user_id,
          user_name: data.user_name || 'Unknown User'
        }
      });
    };

    const handleScreenShareStopped = (data) => {
      console.log('📺 Screen share stopped event in MeetingContext:', data);
      
      dispatch({
        type: 'UPDATE_PARTICIPANT',
        payload: {
          id: data.connection_id || data.user_id,
          updates: { isScreenSharing: false }
        }
      });
      
      dispatch({
        type: 'SET_SCREEN_SHARING',
        payload: {
          isSharing: false,
          stream: null,
          sharer: null
        }
      });
    };

    // NEW: Enhanced participant event handling
    const handleUserJoined = (data) => {
      console.log('👤 User joined event in MeetingContext:', data);
      
      // Record in database if meeting is active
      if (state.currentMeeting?.id) {
        recordParticipantJoin(state.currentMeeting.id, {
          user_id: data.user_id,
          user_name: data.user_name || data.full_name,
          is_host: data.role === 'host' || data.is_host
        });
      }
    };

    const handleUserLeft = (data) => {
      console.log('👋 User left event in MeetingContext:', data);
      
      // Record in database if meeting is active
      if (state.currentMeeting?.id) {
        recordParticipantLeave(state.currentMeeting.id, {
          user_id: data.user_id
        });
      }
    };

    const handleChatMessage = (data) => {
      console.log('💬 Chat message event in MeetingContext:', data);
      
      dispatch({
        type: 'ADD_CHAT_MESSAGE',
        payload: {
          id: Date.now() + Math.random(),
          senderId: data.user_id,
          senderName: data.user_name,
          message: data.message,
          timestamp: new Date(data.timestamp),
          type: 'text'
        }
      });
    };

    const handleReactionReceived = (data) => {
      console.log('😀 Reaction received in MeetingContext:', data);
      
      dispatch({
        type: 'ADD_REACTION',
        payload: {
          id: Date.now() + Math.random(),
          participantId: data.user_id,
          participantName: data.user_name,
          emoji: data.emoji,
          timestamp: Date.now()
        }
      });
      
      // Auto-clear reaction after 5 seconds
      setTimeout(() => {
        dispatch({ type: 'CLEAR_REACTIONS' });
      }, 5000);
    };

    // Register event listeners
    addEventListener('screen-share-started', handleScreenShareStarted);
    addEventListener('screen-share-stopped', handleScreenShareStopped);
    addEventListener('user_joined', handleUserJoined);
    addEventListener('user_left', handleUserLeft);
    addEventListener('chat-message', handleChatMessage);
    addEventListener('reaction_received', handleReactionReceived);

    // Cleanup
    return () => {
      removeEventListener('screen-share-started', handleScreenShareStarted);
      removeEventListener('screen-share-stopped', handleScreenShareStopped);
      removeEventListener('user_joined', handleUserJoined);
      removeEventListener('user_left', handleUserLeft);
      removeEventListener('chat-message', handleChatMessage);
      removeEventListener('reaction_received', handleReactionReceived);
    };
  }, [addEventListener, removeEventListener, state.currentMeeting?.id, recordParticipantJoin, recordParticipantLeave]);

  // Meeting actions - FIXED: Enable audio by default
  const joinMeeting = useCallback(async (meetingId, participantData) => {
    dispatch({ type: 'SET_LOADING', payload: true });
    dispatch({ type: 'SET_CONNECTION_STATUS', payload: 'connecting' });
    
    try {
      console.log('🚀 MeetingContext: Joining meeting:', meetingId);
      console.log('🎤 MeetingContext: Audio will be enabled by default');
      
      // CRITICAL FIX: Connect to LiveKit with audio enabled
      await connectToMeeting(meetingId, participantData.user_id, participantData.user_name || participantData.userName, {
        enableAudio: true,  // Always enable audio
        enableVideo: true,  // Enable video by default
        isHost: participantData.role === 'host' || participantData.isHost
      });
      
      // Load live participants
      await loadLiveParticipants(meetingId);
      
      const meetingData = {
        meeting: { 
          id: meetingId, 
          name: participantData.meetingName || 'Meeting',
          livekit_enabled: true
        },
        isHost: participantData.role === 'host' || participantData.isHost,
        settings: {
          waitingRoomEnabled: true,
          recordingEnabled: false,
          chatEnabled: true,
          reactionsEnabled: true,
        },
      };
      
      dispatch({ type: 'JOIN_MEETING_SUCCESS', payload: meetingData });
      
      console.log('✅ MeetingContext: Meeting joined successfully');
      
    } catch (error) {
      console.error('❌ MeetingContext: Failed to join meeting:', error);
      dispatch({ type: 'SET_ERROR', payload: error.message });
      dispatch({ type: 'SET_CONNECTION_STATUS', payload: 'error' });
    }
  }, [connectToMeeting, loadLiveParticipants]);

  const leaveMeeting = useCallback(async () => {
    try {
      console.log('👋 MeetingContext: Leaving meeting...');
      
      // Record participant leave if we have current meeting and user info
      if (state.currentMeeting?.id && state.currentMeeting?.currentUser) {
        await recordParticipantLeave(state.currentMeeting.id, {
          user_id: state.currentMeeting.currentUser.id
        });
      }
      
      // Clean up streams
      if (state.localStream) {
        state.localStream.getTracks().forEach(track => track.stop());
      }
      
      Object.values(state.remoteStreams).forEach(stream => {
        stream.getTracks().forEach(track => track.stop());
      });
      
      if (state.screenShareStream) {
        state.screenShareStream.getTracks().forEach(track => track.stop());
      }
      
      // Disconnect from LiveKit
      if (livekitDisconnect) {
        livekitDisconnect();
      }
      
      dispatch({ type: 'LEAVE_MEETING' });
      
      console.log('✅ MeetingContext: Left meeting successfully');
      
    } catch (error) {
      console.error('❌ MeetingContext: Error leaving meeting:', error);
    }
  }, [state.currentMeeting, state.localStream, state.remoteStreams, state.screenShareStream, livekitDisconnect, recordParticipantLeave]);

  const toggleAudio = useCallback(() => {
    if (state.localStream) {
      const audioTrack = state.localStream.getAudioTracks()[0];
      if (audioTrack) {
        audioTrack.enabled = !state.audioEnabled;
      }
    }
    dispatch({ type: 'TOGGLE_AUDIO' });
  }, [state.localStream, state.audioEnabled]);

  const toggleVideo = useCallback(() => {
    if (state.localStream) {
      const videoTrack = state.localStream.getVideoTracks()[0];
      if (videoTrack) {
        videoTrack.enabled = !state.videoEnabled;
      }
    }
    dispatch({ type: 'TOGGLE_VIDEO' });
  }, [state.localStream, state.videoEnabled]);

  const sendChatMessage = useCallback((message) => {
    const chatMessage = {
      id: Date.now() + Math.random(),
      senderId: 'current-user',
      senderName: 'Current User',
      message,
      timestamp: new Date(),
      type: 'text',
    };
    dispatch({ type: 'ADD_CHAT_MESSAGE', payload: chatMessage });
  }, []);

  const sendReaction = useCallback((emoji) => {
    const reaction = {
      id: Date.now() + Math.random(),
      participantId: 'current-user',
      participantName: 'Current User',
      emoji,
      timestamp: Date.now(),
    };
    dispatch({ type: 'ADD_REACTION', payload: reaction });
    
    // Auto-clear reaction after 5 seconds
    setTimeout(() => {
      dispatch({ type: 'CLEAR_REACTIONS' });
    }, 5000);
  }, []);

  const raiseHand = useCallback(() => {
    const handRaise = {
      participantId: 'current-user',
      participantName: 'Current User',
      timestamp: Date.now(),
    };
    dispatch({ type: 'RAISE_HAND', payload: handRaise });
  }, []);

  const lowerHand = useCallback((participantId = 'current-user') => {
    dispatch({ type: 'LOWER_HAND', payload: participantId });
  }, []);

  const startRecording = useCallback(() => {
    dispatch({ type: 'SET_RECORDING_STATUS', payload: true });
  }, []);

  const stopRecording = useCallback(() => {
    dispatch({ type: 'SET_RECORDING_STATUS', payload: false });
  }, []);

  const startScreenShare = useCallback(async () => {
    try {
      const screenStream = await navigator.mediaDevices.getDisplayMedia({
        video: true,
        audio: true,
      });
      
      dispatch({ 
        type: 'SET_SCREEN_SHARING', 
        payload: { 
          isSharing: true, 
          stream: screenStream,
          sharer: { id: 'current-user', name: 'You' }
        } 
      });
      
      return screenStream;
    } catch (error) {
      throw new Error('Failed to start screen sharing: ' + error.message);
    }
  }, []);

  const stopScreenShare = useCallback(() => {
    if (state.screenShareStream) {
      state.screenShareStream.getTracks().forEach(track => track.stop());
    }
    
    dispatch({ 
      type: 'SET_SCREEN_SHARING', 
      payload: { 
        isSharing: false, 
        stream: null,
        sharer: null
      } 
    });
  }, [state.screenShareStream]);

  const updateRemoteScreenShare = useCallback((participantId, stream) => {
    dispatch({
      type: 'SET_SCREEN_SHARE_STREAM',
      payload: stream
    });
  }, []);

  const updateMeetingSettings = useCallback((settings) => {
    dispatch({ type: 'UPDATE_MEETING_SETTINGS', payload: settings });
  }, []);

  const clearError = useCallback(() => {
    dispatch({ type: 'CLEAR_ERROR' });
  }, []);

  // Recording management
  const startLiveRecording = useCallback(async (options = {}) => {
    try {
      console.log('🔴 MeetingContext: Starting live recording...');
      
      if (state.isRecording) {
        throw new Error('Recording already in progress');
      }

      const startTime = new Date();
      
      dispatch({
        type: 'START_LIVE_RECORDING',
        payload: { startTime }
      });

      console.log('✅ MeetingContext: Live recording started successfully');
      return true;
    } catch (error) {
      console.error('❌ MeetingContext: Error starting live recording:', error);
      dispatch({ type: 'SET_ERROR', payload: error.message });
      throw error;
    }
  }, [state.isRecording]);

  const stopLiveRecording = useCallback(async () => {
    try {
      console.log('⏹️ MeetingContext: Stopping live recording...');
      
      if (!state.isRecording) {
        console.warn('MeetingContext: No recording in progress');
        return null;
      }

      const recordingData = {
        id: Date.now(),
        filename: `meeting_recording_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.webm`,
        duration: state.recordingDuration,
        timestamp: state.recordingStartTime,
        size: 0,
        url: null
      };

      dispatch({
        type: 'STOP_LIVE_RECORDING',
        payload: { recordingData }
      });

      console.log('✅ MeetingContext: Live recording stopped successfully');
      return recordingData;
    } catch (error) {
      console.error('❌ MeetingContext: Error stopping live recording:', error);
      dispatch({ type: 'SET_ERROR', payload: error.message });
      throw error;
    }
  }, [state.isRecording, state.recordingDuration, state.recordingStartTime]);

  const toggleLiveRecording = useCallback(async () => {
    if (state.isRecording) {
      return await stopLiveRecording();
    } else {
      return await startLiveRecording();
    }
  }, [state.isRecording, startLiveRecording, stopLiveRecording]);

  const saveRecording = useCallback((recordingData, customName = null) => {
    if (!recordingData) {
      console.error('MeetingContext: No recording data to save');
      return;
    }

    const savedRecording = {
      ...recordingData,
      id: recordingData.id || Date.now(),
      savedAt: new Date(),
      customName: customName || recordingData.filename,
    };

    dispatch({
      type: 'SAVE_RECORDING',
      payload: savedRecording
    });

    console.log('💾 MeetingContext: Recording saved:', savedRecording);
  }, []);

  const clearCurrentRecording = useCallback(() => {
    dispatch({ type: 'CLEAR_CURRENT_RECORDING' });
  }, []);

  const updateRecordingDuration = useCallback((duration) => {
    dispatch({
      type: 'UPDATE_RECORDING_DURATION',
      payload: duration
    });
  }, []);

  // NEW: Get participant statistics
  const getParticipantStatistics = useCallback((meetingId) => {
    if (!meetingId) return null;
    
    return {
      total: state.participantStats.total,
      active: state.participantStats.active,
      livekit: state.participantStats.livekit,
      participants: state.liveParticipants
    };
  }, [state.participantStats, state.liveParticipants]);

  // NEW: Refresh participant data
  const refreshParticipants = useCallback(async (meetingId) => {
    if (meetingId) {
      await loadLiveParticipants(meetingId);
      await syncLiveKitParticipants(meetingId);
    }
  }, [loadLiveParticipants, syncLiveKitParticipants]);

  const value = {
    ...state,
    
    // Actions
    joinMeeting,
    leaveMeeting,
    toggleAudio,
    toggleVideo,
    sendChatMessage,
    sendReaction,
    raiseHand,
    lowerHand,
    startRecording,
    stopRecording,
    startScreenShare,
    stopScreenShare,
    updateRemoteScreenShare,
    updateMeetingSettings,
    clearError,
    
    // Recording
    startLiveRecording,
    stopLiveRecording,
    toggleLiveRecording,
    saveRecording,
    clearCurrentRecording,
    updateRecordingDuration,
    
    // NEW: LiveKit integration
    livekitConnected: state.livekitConnected,
    livekitRoom: state.livekitRoom,
    livekitParticipantCount: state.livekitParticipantCount,
    
    // NEW: Enhanced participant management
    liveParticipants: state.liveParticipants,
    participantStats: state.participantStats,
    loadLiveParticipants,
    recordParticipantJoin,
    recordParticipantLeave,
    syncLiveKitParticipants,
    getParticipantStatistics,
    refreshParticipants,
    
    // Dispatch for advanced usage
    dispatch,
  };

  return (
    <MeetingContext.Provider value={value}>
      {children}
    </MeetingContext.Provider>
  );
};

export const useMeetingContext = () => {
  const context = useContext(MeetingContext);
  if (!context) {
    throw new Error('useMeetingContext must be used within a MeetingProvider');
  }
  return context;
};