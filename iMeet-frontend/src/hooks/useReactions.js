// src/hooks/useReactions.js - COMPLETE WITH SOUNDS
import { useState, useEffect, useCallback } from 'react';
import { DataPacket_Kind } from 'livekit-client';
import { useLiveKit } from './useLiveKit';
import { useAuth } from './useAuth';
import { API_BASE_URL } from '../utils/constants';

export const useReactions = (meetingId, room) => {
  const [activeReactions, setActiveReactions] = useState([]);
  const [reactionHistory, setReactionHistory] = useState([]);
  const [reactionCounts, setReactionCounts] = useState({});
  const [participantReactions, setParticipantReactions] = useState(new Map());
  const [soundEnabled, setSoundEnabled] = useState(true);
  const [meetingInitialized, setMeetingInitialized] = useState(false);
  
  const { connected, isConnected } = useLiveKit();
  const { user } = useAuth();

  // Available reaction types
  const reactionTypes = {
    THUMBS_UP: '👍',
    THUMBS_DOWN: '👎', 
    HEART: '❤️',
    CLAP: '👏',
    CELEBRATION: '🎉',
    FIRE: '🔥',
    THINKING: '🤔'
  };

  // ✅ ENHANCED: Professional reaction sounds using Web Audio API
  const playReactionTone = useCallback((frequencies, duration = 200, type = 'default') => {
    if (!soundEnabled) {
      console.log("🔇 Sound disabled - skipping playback");
      return;
    }

    try {
      console.log("🔊 Playing sound:", type, "frequencies:", frequencies);
      
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const masterGain = audioContext.createGain();
      masterGain.connect(audioContext.destination);
      
      // Volume control - increase to 0.25 for better audibility
      masterGain.gain.setValueAtTime(0.25, audioContext.currentTime);
      
      const freqArray = Array.isArray(frequencies) ? frequencies : [frequencies];
      
      freqArray.forEach((freq, index) => {
        const oscillator = audioContext.createOscillator();
        const gain = audioContext.createGain();
        
        oscillator.connect(gain);
        gain.connect(masterGain);
        
        // Different wave types for different reactions
        switch (type) {
          case 'positive':
            oscillator.type = 'sine';
            break;
          case 'negative':
            oscillator.type = 'sawtooth';
            break;
          case 'love':
            oscillator.type = 'sine';
            break;
          case 'clap':
            oscillator.type = 'square';
            break;
          case 'celebration':
            oscillator.type = 'sine';
            break;
          case 'fire':
            oscillator.type = 'triangle';
            break;
          case 'thinking':
            oscillator.type = 'sine';
            break;
          default:
            oscillator.type = 'sine';
        }
        
        oscillator.frequency.setValueAtTime(freq, audioContext.currentTime);
        
        const startTime = audioContext.currentTime + (index * 0.05);
        const endTime = startTime + (duration / 1000);
        
        gain.gain.setValueAtTime(0, startTime);
        gain.gain.linearRampToValueAtTime(0.4 / freqArray.length, startTime + 0.01);
        gain.gain.exponentialRampToValueAtTime(0.001, endTime);
        
        oscillator.start(startTime);
        oscillator.stop(endTime);
      });
      
      setTimeout(() => {
        if (audioContext.state !== 'closed') {
          audioContext.close();
        }
      }, duration + 100);
      
      console.log("✅ Sound played successfully");
      
    } catch (error) {
      console.warn("⚠️ Web Audio API failed:", error);
      
      // Fallback: Simple beep
      try {
        const audioContext = new AudioContext();
        const oscillator = audioContext.createOscillator();
        const gain = audioContext.createGain();
        
        oscillator.connect(gain);
        gain.connect(audioContext.destination);
        
        oscillator.frequency.value = 800;
        oscillator.type = 'sine';
        
        gain.gain.setValueAtTime(0, audioContext.currentTime);
        gain.gain.linearRampToValueAtTime(0.2, audioContext.currentTime + 0.01);
        gain.gain.exponentialRampToValueAtTime(0.001, audioContext.currentTime + 0.15);
        
        oscillator.start();
        oscillator.stop(audioContext.currentTime + 0.15);
        
        setTimeout(() => audioContext.close(), 200);
        console.log("✅ Fallback sound played");
      } catch (fallbackError) {
        console.error("❌ All sound playback failed:", fallbackError);
      }
    }
  }, [soundEnabled]);

  // ✅ ENHANCED: Reaction sounds mapped to Web Audio tones
  const reactionSounds = {
    '👍': () => playReactionTone(800, 150, 'positive'),
    '👎': () => playReactionTone(400, 200, 'negative'), 
    '❤️': () => playReactionTone([523, 659, 784], 300, 'love'),
    '👏': () => playReactionTone([220, 330], 100, 'clap'),
    '🎉': () => playReactionTone([523, 659, 784, 1047], 250, 'celebration'),
    '🔥': () => playReactionTone([440, 554, 659], 200, 'fire'),
    '🤔': () => playReactionTone([349, 415], 300, 'thinking')
  };

  // ✅ FIXED: Handle incoming reactions with sound
  const handleIncomingReaction = useCallback((data) => {
    console.log("⚡ INSTANT RECEIVE:", {
      emoji: data.emoji,
      from: data.user_name,
      userId: data.user_id,
      timestamp: Date.now(),
      soundEnabled: soundEnabled,
    });
    
    // Skip own reactions
    if (data.user_id?.toString() === user?.id?.toString()) {
      console.log("⏭️ Skipping own reaction (no sound for sender)");
      return;
    }

    const timestamp = data.timestamp || Date.now();
    const displayDuration = 5;

    const reactionData = {
      id: `${data.user_id}-${timestamp}`,
      emoji: data.emoji,
      userName: data.user_name,
      user_name: data.user_name,
      userId: data.user_id,
      timestamp: timestamp,
      participantId: data.participant_identity,
      expiresAt: Date.now() + (displayDuration * 1000)
    };

    // Update state immediately
    setReactionHistory(prev => [reactionData, ...prev.slice(0, 49)]);
    
    // Update participant reactions map
    setParticipantReactions(prev => {
      const newMap = new Map(prev);
      newMap.set(data.user_id?.toString(), {
        emoji: data.emoji,
        userName: data.user_name,
        user_name: data.user_name,
        timestamp: timestamp,
        expiresAt: reactionData.expiresAt
      });
      console.log("✅ participantReactions Map updated");
      return newMap;
    });

    // ✅ PLAY SOUND for received reaction
    console.log("🔊 Attempting to play sound for:", data.emoji);
    if (soundEnabled && reactionSounds[data.emoji]) {
      console.log("🎵 Playing sound for received reaction:", data.emoji);
      try {
        reactionSounds[data.emoji]();
      } catch (soundError) {
        console.warn("⚠️ Sound playback failed:", soundError);
      }
    } else {
      console.log("🔇 Sound not played - soundEnabled:", soundEnabled, "hasSound:", !!reactionSounds[data.emoji]);
    }

    // Auto-clear after duration
    setTimeout(() => {
      setParticipantReactions(prev => {
        const newMap = new Map(prev);
        newMap.delete(data.user_id?.toString());
        return newMap;
      });
    }, displayDuration * 1000);

  }, [user?.id, soundEnabled, reactionSounds]);

  // ✅ FIXED: Setup LiveKit data listener
  useEffect(() => {
    if (!room) {
      console.warn("⚠️ useReactions: No room - listener not active");
      return;
    }

    console.log("✅ useReactions: Setting up dataReceived listener");

    const handleDataReceived = (payload, participant) => {
      try {
        if (!payload || payload.length === 0) {
          return;
        }

        const decoder = new TextDecoder();
        const messageStr = decoder.decode(payload);
        const message = JSON.parse(messageStr);

        // Handle reaction notifications
        if (message.type === 'reaction_notification') {
          console.log("📥 useReactions received reaction:", message.emoji, "from", message.user_name);
          
          if (!message.emoji || !message.user_id) {
            console.warn("⚠️ Invalid reaction data:", message);
            return;
          }
          
          handleIncomingReaction(message);
        } 
        else if (message.type === 'clear_all_reactions') {
          setActiveReactions([]);
          setReactionHistory([]);
          setReactionCounts({});
          setParticipantReactions(new Map());
        }
        
      } catch (error) {
        console.error("❌ useReactions dataReceived error:", error);
      }
    };

    room.on('dataReceived', handleDataReceived);
    console.log("✅ useReactions: dataReceived listener added");
    
    return () => {
      console.log("🧹 useReactions: Removing dataReceived listener");
      room.off('dataReceived', handleDataReceived);
    };
  }, [room, handleIncomingReaction]);

  // Initialize meeting reactions
  const initializeMeetingReactions = useCallback(async () => {
    if (!meetingId || meetingInitialized) {
      return false;
    }

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/start/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meeting_id: meetingId
        })
      });

      const result = await response.json();
      
      if (result.success) {
        setMeetingInitialized(true);
        loadReactionCounts();
        loadActiveReactions();
        return true;
      } else {
        return false;
      }
    } catch (error) {
      return false;
    }
  }, [meetingId, meetingInitialized]);

  // Load reaction counts from backend
  const loadReactionCounts = useCallback(async () => {
    if (!meetingId) return;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/counts/${meetingId}/`);
      const result = await response.json();

      if (result.success) {
        setReactionCounts(result.reaction_counts || {});
      } else if (result.error && result.error.includes('not found')) {
        setReactionCounts({});
        setActiveReactions([]);
        setReactionHistory([]);
      }
    } catch (error) {
      console.error("Failed to load reaction counts:", error);
    }
  }, [meetingId]);

  // Load active reactions from backend  
  const loadActiveReactions = useCallback(async () => {
    if (!meetingId) return;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/active/${meetingId}/`);
      const result = await response.json();

      if (result.success) {
        const reactions = result.active_reactions || [];
        setActiveReactions(reactions);
        
        const participantMap = new Map();
        reactions.forEach(reaction => {
          if (reaction.time_remaining > 0) {
            participantMap.set(reaction.user_id?.toString(), {
              emoji: reaction.emoji,
              userName: reaction.user_name,
              user_name: reaction.user_name,
              timestamp: reaction.timestamp,
              expiresAt: reaction.expires_at
            });
          }
        });
        setParticipantReactions(participantMap);
        
      } else if (result.error && result.error.includes('not found')) {
        setActiveReactions([]);
        setParticipantReactions(new Map());
      }
    } catch (error) {
      console.error("Failed to load active reactions:", error);
    }
  }, [meetingId]);

  // ✅ ENHANCED: Send reaction with sound feedback
  const sendReactionToMeeting = useCallback(async (emojiOrData) => {
    let emoji, finalMeetingId, finalUser;
    
    if (typeof emojiOrData === 'string') {
      emoji = emojiOrData;
      finalMeetingId = meetingId;
      finalUser = user;
    } else if (emojiOrData && typeof emojiOrData === 'object') {
      emoji = emojiOrData.emoji;
      finalMeetingId = emojiOrData.meetingId || meetingId;
      finalUser = emojiOrData.user || user;
    } else {
      console.error("❌ Invalid reaction data format");
      return false;
    }

    if (!finalMeetingId || !finalUser?.id || !emoji) {
      console.error("❌ Missing required data");
      return false;
    }

    if (!Object.values(reactionTypes).includes(emoji)) {
      console.error("❌ Invalid emoji:", emoji);
      return false;
    }

    if (!room?.localParticipant) {
      console.error("❌ Room not connected");
      return false;
    }

    try {
      const timestamp = Date.now();
      const userName = finalUser.full_name || finalUser.name || finalUser.username || `User ${finalUser.id}`;
      
      console.log("🚀 INSTANT SEND:", emoji, "at", timestamp);

      // STEP 1: LiveKit broadcast
      const broadcastData = {
        type: 'reaction_notification',
        emoji: emoji,
        user_id: finalUser.id.toString(),
        user_name: userName,
        participant_identity: finalUser.participant_identity || `user_${finalUser.id}`,
        timestamp: timestamp,
        created_at: new Date().toISOString(),
        meeting_id: finalMeetingId,
        display_duration: 5,
      };

      const encoder = new TextEncoder();
      const data = encoder.encode(JSON.stringify(broadcastData));

      await room.localParticipant.publishData(data, DataPacket_Kind.RELIABLE);
      console.log("✅ BROADCAST SENT");

      // STEP 2: Play sound for SENDER immediately
      console.log("🔊 Playing sound for SENDER:", emoji);
      if (soundEnabled && reactionSounds[emoji]) {
        try {
          reactionSounds[emoji]();
          console.log("✅ Sender sound played");
        } catch (soundError) {
          console.warn("⚠️ Sender sound failed:", soundError);
        }
      }

      // STEP 3: Backend call in background
      fetch(`${API_BASE_URL}/api/cache-reactions/add/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          meeting_id: finalMeetingId,
          user_id: finalUser.id,
          user_name: userName,
          emoji: emoji,
          participant_identity: finalUser.participant_identity || `user_${finalUser.id}`
        })
      })
      .then(response => response.json())
      .then(result => {
        if (result.success) {
          console.log("✅ Backend confirmed");
          setTimeout(() => loadReactionCounts(), 100);
        }
      })
      .catch(error => {
        console.error("❌ Backend error:", error);
      });

      return true;

    } catch (error) {
      console.error("❌ SEND FAILED:", error);
      return false;
    }
  }, [meetingId, user, room, soundEnabled, reactionSounds, loadReactionCounts, reactionTypes]);

  // Clear all reactions
  const clearAllReactions = useCallback(async (hostUserId) => {
    if (!meetingId || !hostUserId) return false;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/clear-all/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meeting_id: meetingId,
          host_user_id: hostUserId
        })
      });

      const result = await response.json();

      if (result.success) {
        if (room) {
          try {
            const encoder = new TextEncoder();
            const data = encoder.encode(JSON.stringify({
              type: 'clear_all_reactions',
              host_user_id: hostUserId,
              timestamp: Date.now(),
              meeting_id: meetingId
            }));
            
            await room.localParticipant.publishData(data, DataPacket_Kind.RELIABLE);
          } catch (broadcastError) {
            console.error("Broadcast error:", broadcastError);
          }
        }

        setActiveReactions([]);
        setReactionHistory([]);
        setReactionCounts({});
        setParticipantReactions(new Map());

        return true;
      }
      return false;
    } catch (error) {
      return false;
    }
  }, [meetingId, room]);

  // End meeting reactions
  const endMeetingReactions = useCallback(async () => {
    if (!meetingId) return false;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/end/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meeting_id: meetingId
        })
      });

      const result = await response.json();
      
      if (result.success) {
        setMeetingInitialized(false);
        setActiveReactions([]);
        setReactionHistory([]);
        setReactionCounts({});
        setParticipantReactions(new Map());
        return true;
      }
    } catch (error) {
      console.error("End meeting reactions error:", error);
    }
    return false;
  }, [meetingId]);

  // Initialize meeting when connected
  useEffect(() => {
    if (meetingId && (connected || isConnected) && !meetingInitialized) {
      initializeMeetingReactions();
    }
  }, [meetingId, connected, isConnected, meetingInitialized, initializeMeetingReactions]);

  // Cleanup expired reactions (faster polling)
  useEffect(() => {
    const cleanupInterval = setInterval(() => {
      const now = Date.now();
      setParticipantReactions(prev => {
        const newMap = new Map();
        let hasChanges = false;
        
        prev.forEach((reaction, participantId) => {
          if (reaction.expiresAt > now) {
            newMap.set(participantId, reaction);
          } else {
            hasChanges = true;
          }
        });
        
        return hasChanges ? newMap : prev;
      });
    }, 500);

    return () => clearInterval(cleanupInterval);
  }, []);

  // Get participant reaction
  const getParticipantReaction = useCallback((participantId) => {
    return participantReactions.get(participantId?.toString());
  }, [participantReactions]);

  // Get all active reactions
  const getAllActiveReactions = useCallback(() => {
    return Array.from(participantReactions.entries()).map(([participantId, reaction]) => ({
      participantId,
      ...reaction
    }));
  }, [participantReactions]);

  // Get reaction count
  const getReactionCount = useCallback((emoji) => {
    return reactionCounts[emoji]?.count || 0;
  }, [reactionCounts]);

  // Get top reactions
  const getTopReactions = useCallback((limit = 3) => {
    return Object.entries(reactionCounts)
      .map(([emoji, data]) => ({ emoji, count: data.count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, limit);
  }, [reactionCounts]);

  // ✅ Toggle sound
  const toggleSound = useCallback(() => {
    setSoundEnabled(prev => {
      const newState = !prev;
      console.log("🔊 Sound toggled:", newState ? "ENABLED" : "DISABLED");
      return newState;
    });
  }, []);

  // Quick reaction functions
  const sendThumbsUp = useCallback(() => sendReactionToMeeting(reactionTypes.THUMBS_UP), [sendReactionToMeeting]);
  const sendThumbsDown = useCallback(() => sendReactionToMeeting(reactionTypes.THUMBS_DOWN), [sendReactionToMeeting]);
  const sendHeart = useCallback(() => sendReactionToMeeting(reactionTypes.HEART), [sendReactionToMeeting]);
  const sendClap = useCallback(() => sendReactionToMeeting(reactionTypes.CLAP), [sendReactionToMeeting]);
  const sendCelebration = useCallback(() => sendReactionToMeeting(reactionTypes.CELEBRATION), [sendReactionToMeeting]);
  const sendFire = useCallback(() => sendReactionToMeeting(reactionTypes.FIRE), [sendReactionToMeeting]);
  const sendThinking = useCallback(() => sendReactionToMeeting(reactionTypes.THINKING), [sendReactionToMeeting]);

  return {
    // State
    activeReactions,
    reactionHistory,
    reactionCounts,
    participantReactions,
    soundEnabled,
    reactionTypes,
    meetingInitialized,
    
    // Actions
    sendReactionToMeeting,
    sendThumbsUp,
    sendThumbsDown,
    sendHeart,
    sendClap,
    sendCelebration,
    sendFire,
    sendThinking,
    clearAllReactions,
    initializeMeetingReactions,
    endMeetingReactions,
    
    // Utilities
    getParticipantReaction,
    getAllActiveReactions,
    getReactionCount,
    getTopReactions,
    toggleSound,
    loadReactionCounts,
    loadActiveReactions,
    
    // Settings
    setSoundEnabled
  };
};