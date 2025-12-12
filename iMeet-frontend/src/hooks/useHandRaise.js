// src/hooks/useHandRaise.js - Hand Raise Hook (FIXED VERSION)
import { useState, useEffect, useCallback, useRef } from 'react';
import { handRaiseService } from '../services/handRaiseAPI';

export const useHandRaise = (meetingId, currentUser, isHost = false, livekitRoom = null, hasHostPrivileges = false) => {  const [raisedHands, setRaisedHands] = useState([]);
  const [isHandRaised, setIsHandRaised] = useState(false);
  const [handRaiseStats, setHandRaiseStats] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  
  const pollIntervalRef = useRef(null);
  const initializedRef = useRef(false);
  const cleanupPerformedRef = useRef(false);

  
const loadRaisedHands = useCallback(async () => {
  if (!meetingId) {
    console.log('❌ No meetingId for loading hands');
    return;
  }
  
  try {
    console.log('📋 Loading raised hands for meeting:', meetingId);
    
    // First, ensure the system is started if we're a host
    if (currentUser && (isHost || hasHostPrivileges)) {
      try {
        await handRaiseService.startMeetingHandRaise(meetingId);
        console.log('✅ Ensured hand raise system is started');
      } catch (startError) {
        // Ignore if already exists
        if (!startError.message?.includes('already') && startError.response?.status !== 409) {
          console.warn('⚠️ Could not start hand raise system:', startError.message);
        }
      }
    }
    
    const response = await handRaiseService.getRaisedHands(meetingId);
    
    console.log('📋 Raised hands API response:', response);
    
    if (response.success) {
      const hands = response.raised_hands || [];
      console.log('✅ Raised hands loaded:', hands.length, 'hands found');
      setRaisedHands(hands);
    } else {
      console.log('ℹ️ No hands or meeting not active:', response.note || response.message);
      setRaisedHands([]);
    }
  } catch (error) {
    console.log('ℹ️ Could not load raised hands:', error.message);
    setRaisedHands([]);
  }
}, [meetingId, currentUser, isHost, hasHostPrivileges]);

const initializeHandRaise = useCallback(async () => {
  if (!meetingId || !currentUser || initializedRef.current) return;
  
  try {
    console.log('🚀 Lazy initializing hand raise system for meeting:', meetingId);
    
    // Just mark as initialized - don't start the system until needed
    initializedRef.current = true;
    
    // Check current hand status only
    try {
      const handStatus = await handRaiseService.checkHandStatus(meetingId, currentUser.id);
      setIsHandRaised(handStatus.hand_raised || false);
      console.log('🖐️ Current hand status:', handStatus.hand_raised);
    } catch (statusError) {
      console.log('ℹ️ Hand status check failed - system probably not started yet');
      setIsHandRaised(false);
    }
    
    console.log('✅ Hand raise lazy initialization complete');
    
  } catch (error) {
    console.error('❌ Hand raise lazy initialization failed:', error);
    setError(error.message);
  }
}, [meetingId, currentUser]);

// src/hooks/useHandRaise.js - FIXED polling system
const startPolling = useCallback(() => {
  if (pollIntervalRef.current || !initializedRef.current) {
    console.log('⏸️ Polling not started:', { 
      alreadyPolling: !!pollIntervalRef.current, 
      initialized: initializedRef.current 
    });
    return;
  }
  
  console.log('🔄 Starting hand raise polling...');
  
  pollIntervalRef.current = setInterval(async () => {
    try {
      console.log('🔄 Polling for hand raise updates...');
      await loadRaisedHands();
      
      // Also check our own hand status
      if (currentUser) {
        const handStatus = await handRaiseService.checkHandStatus(meetingId, currentUser.id);
        console.log('❓ Own hand status check result:', handStatus);
        setIsHandRaised(handStatus.hand_raised || false);
      }
    } catch (error) {
      console.error('❌ Polling error:', error);
      // If polling fails consistently, the meeting might have ended
      if (error.response?.status === 404) {
        console.log('🔚 Meeting appears to have ended, stopping polling');
        stopPolling();
      }
    }
  }, 2000); // Poll every 2 seconds for better responsiveness
  
  console.log('✅ Hand raise polling started');
}, [meetingId, currentUser, loadRaisedHands]);

  // Stop polling
  const stopPolling = useCallback(() => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = null;
      console.log('⏹️ Stopped hand raise polling');
    }
  }, []);

  // Toggle hand raise for current user
  const toggleHandRaise = useCallback(async () => {
    if (!meetingId || !currentUser || isLoading) return;
    
    try {
      setIsLoading(true);
      setError(null);
      
      const action = isHandRaised ? 'lower' : 'raise';
      const response = await handRaiseService.toggleHand(
        meetingId,
        currentUser.id,
        currentUser.full_name || currentUser.name || 'User',
        `user_${currentUser.id}`,
        action
      );
      
      if (response.success) {
        setIsHandRaised(!isHandRaised);
        
        // Broadcast via LiveKit if available
        if (livekitRoom && response.send_via_livekit && response.data) {
          try {
            const dataToSend = JSON.stringify({
              type: 'hand_raise_update',
              ...response.data
            });
            
            console.log('📡 Broadcasting hand raise data:', dataToSend);
            
            livekitRoom.localParticipant.publishData(
              dataToSend,
              'reliable'
            );
            console.log('📡 Hand raise broadcasted via LiveKit');
          } catch (broadcastError) {
            console.warn('⚠️ Failed to broadcast via LiveKit:', broadcastError);
          }
        }
        
        // Immediately refresh the list
        await loadRaisedHands();
      }
    } catch (error) {
      console.error('❌ Failed to toggle hand raise:', error);
      setError(error.message);
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser, isHandRaised, isLoading, livekitRoom, loadRaisedHands]);

  // Host acknowledges a hand
  const acknowledgeHand = useCallback(async (participantUserId, action = 'acknowledge') => {
    if (!meetingId || !currentUser || !isHost || isLoading) return;
    
    try {
      setIsLoading(true);
      setError(null);
      
      const participant = raisedHands.find(hand => hand.user_id === participantUserId);
      const participantName = participant?.user?.full_name || 'Participant';
      
      const response = await handRaiseService.acknowledgeHand(
        meetingId,
        currentUser.id,
        participantUserId,
        participantName,
        action
      );
      
      if (response.success) {
        // Broadcast via LiveKit if available
        if (livekitRoom && response.send_via_livekit && response.data) {
          try {
            const dataToSend = JSON.stringify({
              type: 'hand_acknowledgment',
              ...response.data
            });
            
            console.log('📡 Broadcasting acknowledgment data:', dataToSend);
            
            livekitRoom.localParticipant.publishData(
              dataToSend,
              'reliable'
            );
            console.log('📡 Hand acknowledgment broadcasted via LiveKit');
          } catch (broadcastError) {
            console.warn('⚠️ Failed to broadcast acknowledgment via LiveKit:', broadcastError);
          }
        }
        
        // Immediately refresh the list
        await loadRaisedHands();
      }
    } catch (error) {
      console.error('❌ Failed to acknowledge hand:', error);
      setError(error.message);
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser, isHost, isLoading, raisedHands, livekitRoom, loadRaisedHands]);

  // Host clears all hands
  const clearAllHands = useCallback(async () => {
    if (!meetingId || !currentUser || !isHost || isLoading) return;
    
    try {
      setIsLoading(true);
      setError(null);
      
      const response = await handRaiseService.clearAllHands(meetingId, currentUser.id);
      
      if (response.success) {
        // Broadcast via LiveKit if available
        if (livekitRoom && response.send_via_livekit && response.data) {
          try {
            const dataToSend = JSON.stringify({
              type: 'clear_all_hands',
              ...response.data
            });
            
            console.log('📡 Broadcasting clear all data:', dataToSend);
            
            livekitRoom.localParticipant.publishData(
              dataToSend,
              'reliable'
            );
            console.log('📡 Clear all hands broadcasted via LiveKit');
          } catch (broadcastError) {
            console.warn('⚠️ Failed to broadcast clear all via LiveKit:', broadcastError);
          }
        }
        
        // Clear local state
        setRaisedHands([]);
        setIsHandRaised(false);
      }
    } catch (error) {
      console.error('❌ Failed to clear all hands:', error);
      setError(error.message);
    } finally {
      setIsLoading(false);
    }
  }, [meetingId, currentUser, isHost, isLoading, livekitRoom]);

// src/hooks/useHandRaise.js - FIXED handleLivekitDataMessage function
const handleLivekitDataMessage = useCallback((data, participant, kind, topic) => {
  try {
    console.log('📡 Raw LiveKit data received:', { data, participant: participant?.identity, kind, topic });
    
    // Convert data to string properly
    let dataString;
    
    if (data instanceof Uint8Array) {
      dataString = new TextDecoder('utf-8').decode(data);
    } else if (data instanceof ArrayBuffer) {
      dataString = new TextDecoder('utf-8').decode(new Uint8Array(data));
    } else if (typeof data === 'string') {
      dataString = data;
    } else {
      console.log('📡 Non-parseable data type:', typeof data);
      return;
    }
    
    console.log('📡 Decoded data string:', dataString);
    
    // Validate JSON format
    const trimmedData = dataString.trim();
    if (!trimmedData || (!trimmedData.startsWith('{') && !trimmedData.startsWith('['))) {
      console.log('📡 Not JSON data, ignoring:', trimmedData.substring(0, 50));
      return;
    }
    
    const message = JSON.parse(trimmedData);
    console.log('📡 Parsed message:', message);
    
    // CRITICAL FIX: Only process hand raise messages
    const handRaiseTypes = [
      'hand_raise_update', 
      'hand_acknowledgment', 
      'clear_all_hands',
      'hand_state_sync'
    ];
    
    if (!handRaiseTypes.includes(message.type)) {
      console.log('📡 Not a hand raise message, ignoring type:', message.type);
      return;
    }
    
    console.log('✅ Processing hand raise message:', message.type);
    
    // Process hand raise messages
    if (message.type === 'hand_raise_update') {
      console.log('🖐️ Hand raise update received:', message);
      
      // Update own hand status if it's for current user
      if (message.user_id?.toString() === currentUser?.id?.toString()) {
        console.log('🖐️ Updating own hand status:', message.action === 'raise');
        setIsHandRaised(message.action === 'raise');
      }
    } else if (message.type === 'clear_all_hands') {
      console.log('🧹 Clear all hands received');
      setIsHandRaised(false);
    }
    
    // CRITICAL: Always refresh hands list when any hand raise message arrives
    console.log('🔄 Refreshing hands list due to LiveKit message');
    loadRaisedHands();
    
  } catch (error) {
    console.error('❌ Failed to parse LiveKit hand raise message:', error);
    console.error('❌ Raw data:', data);
    console.error('❌ Data type:', typeof data);
  }
}, [loadRaisedHands, currentUser]);

  // Cleanup when meeting ends
  const cleanup = useCallback(async () => {
    if (cleanupPerformedRef.current) return;
    
    console.log('🧹 Cleaning up hand raise system...');
    
    stopPolling();
    
    // End the hand raise system for this meeting
    if (meetingId) {
      try {
        await handRaiseService.endMeetingHandRaise(meetingId);
        console.log('✅ Hand raise system cleanup completed');
      } catch (error) {
        console.warn('⚠️ Hand raise cleanup failed:', error);
      }
    }
    
    // Reset state
    setRaisedHands([]);
    setIsHandRaised(false);
    setHandRaiseStats(null);
    setError(null);
    initializedRef.current = false;
    cleanupPerformedRef.current = true;
  }, [meetingId, stopPolling]);

  // Get statistics
  const getStats = useCallback(async () => {
    if (!meetingId) return;
    
    try {
      const response = await handRaiseService.getHandRaiseStats(meetingId);
      if (response.success) {
        setHandRaiseStats(response.stats);
      }
    } catch (error) {
      console.error('❌ Failed to get hand raise stats:', error);
    }
  }, [meetingId]);


  // src/hooks/useHandRaise.js - REPLACE the initialization useEffect
useEffect(() => {
  if (meetingId && currentUser && !initializedRef.current) {
    console.log('🚀 Hand raise hook: Starting initialization...');
    
    // Don't initialize immediately - wait for meeting to be connected
    const delayedInit = setTimeout(() => {
      if (!initializedRef.current) {
        initializeHandRaise().then(() => {
          console.log('✅ Hand raise initialization complete, starting polling...');
          startPolling();
        }).catch((error) => {
          console.error('❌ Hand raise initialization failed:', error);
          setError(`Initialization failed: ${error.message}`);
          
          // Try to start polling anyway for fallback
          setTimeout(() => {
            if (!pollIntervalRef.current) {
              console.log('🔄 Starting polling as fallback after init failure');
              startPolling();
            }
          }, 5000);
        });
      }
    }, 3000); // Wait 3 seconds for meeting to fully connect
    
    return () => {
      clearTimeout(delayedInit);
      stopPolling();
    };
  }
}, [meetingId, currentUser, initializeHandRaise, startPolling, stopPolling]);

  // Setup LiveKit listener - IMPROVED
  useEffect(() => {
    if (livekitRoom) {
      console.log('📡 Setting up LiveKit data listener');
      
      // Use the correct event name and handler signature
      const dataReceivedHandler = (payload, participant, kind, topic) => {
        console.log('📡 LiveKit dataReceived event fired');
        console.log('📡 Payload type:', typeof payload);
        console.log('📡 Participant:', participant?.identity);
        console.log('📡 Kind:', kind);
        console.log('📡 Topic:', topic);
        
        handleLivekitDataMessage(payload, participant, kind, topic);
      };
      
      livekitRoom.on('dataReceived', dataReceivedHandler);
      
      return () => {
        console.log('📡 Removing LiveKit data listener');
        livekitRoom.off('dataReceived', dataReceivedHandler);
      };
    }
  }, [livekitRoom, handleLivekitDataMessage]);


  // src/hooks/useHandRaise.js - FIXED LiveKit listener setup
useEffect(() => {
  if (livekitRoom && livekitRoom.state === 'connected') {
    console.log('📡 Setting up LiveKit hand raise listener for room:', livekitRoom.name);
    
    const dataReceivedHandler = (payload, participant, kind, topic) => {
      console.log('📡 LiveKit dataReceived event fired for hand raise system');
      handleLivekitDataMessage(payload, participant, kind, topic);
    };
    
    // Remove any existing listener first
    livekitRoom.off('dataReceived', dataReceivedHandler);
    // Add the new listener
    livekitRoom.on('dataReceived', dataReceivedHandler);
    
    console.log('✅ LiveKit hand raise listener attached');
    
    return () => {
      console.log('📡 Removing LiveKit hand raise listener');
      livekitRoom.off('dataReceived', dataReceivedHandler);
    };
  } else {
    console.log('⚠️ LiveKit room not ready for hand raise listener:', {
      hasRoom: !!livekitRoom,
      roomState: livekitRoom?.state
    });
  }
}, [livekitRoom, livekitRoom?.state, handleLivekitDataMessage]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      cleanup();
    };
  }, [cleanup]);

  return {
    // State
    raisedHands,
    isHandRaised,
    handRaiseStats,
    isLoading,
    error,
    
    // Actions
    toggleHandRaise,
    acknowledgeHand,
    clearAllHands,
    loadRaisedHands,
    getStats,
    cleanup,
    
    // Utilities
    pendingHandsCount: raisedHands.filter(hand => hand.status === 'waiting').length,
    totalHandsCount: raisedHands.length,
    isInitialized: initializedRef.current
  };
};