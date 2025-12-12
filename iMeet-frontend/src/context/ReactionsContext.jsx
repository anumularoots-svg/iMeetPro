// UPDATED: src/context/ReactionsContext.jsx - Cache-Only Backend Integration
import React, { createContext, useContext, useState, useCallback, useEffect } from 'react';
import { useLiveKit } from './LiveKitContext';
import { API_BASE_URL } from '../utils/constants';

const ReactionsContext = createContext();

export const useReactions = () => {
  const context = useContext(ReactionsContext);
  if (!context) {
    throw new Error('useReactions must be used within a ReactionsProvider');
  }
  return context;
};

export const ReactionsProvider = ({ children }) => {
  const [activeReactions, setActiveReactions] = useState([]);
  const [reactionHistory, setReactionHistory] = useState([]);
  const [reactionCounts, setReactionCounts] = useState({});
  const [soundEnabled, setSoundEnabled] = useState(true);
  const [currentMeetingId, setCurrentMeetingId] = useState(null);
  const { sendMessage, room, connected, isConnected } = useLiveKit();

  // Available reactions - FIXED: Match backend exactly
  const availableReactions = [
    { emoji: '👍', name: 'thumbs_up', label: 'Thumbs Up', sound: '/sounds/reaction.mp3' },
    { emoji: '👎', name: 'thumbs_down', label: 'Thumbs Down', sound: '/sounds/reaction.mp3' },
    { emoji: '❤', name: 'heart', label: 'Heart', sound: '/sounds/reaction.mp3' },
    { emoji: '👏', name: 'clap', label: 'Clap', sound: '/sounds/clap.mp3' },
    { emoji: '🎉', name: 'celebration', label: 'Celebration', sound: '/sounds/celebration.mp3' },
    { emoji: '🔥', name: 'fire', label: 'Fire', sound: '/sounds/reaction.mp3' },
    { emoji: '🤔', name: 'thinking', label: 'Thinking', sound: '/sounds/reaction.mp3' },
  ];

  // Initialize meeting reactions
  const initializeMeetingReactions = useCallback(async (meetingId) => {
    if (!meetingId) return false;

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
        setCurrentMeetingId(meetingId);
        console.log('✅ Meeting reactions initialized');
        return true;
      } else {
        console.error('Failed to initialize reactions:', result.error);
        return false;
      }
    } catch (error) {
      console.error('Failed to initialize meeting reactions:', error);
      return false;
    }
  }, []);

  // End meeting reactions
  const endMeetingReactions = useCallback(async (meetingId) => {
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
        console.log('✅ Meeting reactions ended, all data deleted');
        setCurrentMeetingId(null);
        setActiveReactions([]);
        setReactionHistory([]);
        setReactionCounts({});
        return true;
      }
    } catch (error) {
      console.error('Failed to end meeting reactions:', error);
    }
    return false;
  }, []);

  // Send reaction to cache-only backend
  const handleSendReaction = useCallback(async (meetingId, emoji, userId, userName) => {
    if (!meetingId || !emoji || !userId) {
      console.error('Missing required reaction parameters');
      return null;
    }

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/add/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meeting_id: meetingId,
          user_id: userId,
          user_name: userName || 'Anonymous',
          emoji: emoji,
          participant_identity: `user_${userId}`
        })
      });

      const result = await response.json();

      if (result.success) {
        const reaction = {
          id: Date.now(),
          emoji: emoji,
          userName: userName || 'You',
          userId: userId,
          timestamp: new Date().toISOString()
        };

        // Add to local history
        setReactionHistory(prev => [reaction, ...prev.slice(0, 49)]);

        // Send via LiveKit for real-time broadcasting
        if (result.send_via_livekit && result.data && sendMessage) {
          sendMessage('reaction', result.data);
        }

        // Play sound if enabled
        const reactionData = availableReactions.find(r => r.emoji === emoji);
        if (soundEnabled && reactionData?.sound) {
          const audio = new Audio(reactionData.sound);
          audio.volume = 0.3;
          audio.play().catch(console.error);
        }

        // Update counts
        loadReactionCounts(meetingId);

        return reaction.id;
      } else {
        console.error('Failed to send reaction:', result.error);
        return null;
      }
    } catch (error) {
      console.error('Failed to send reaction:', error);
      return null;
    }
  }, [sendMessage, soundEnabled, availableReactions]);

  // Load reaction counts
  const loadReactionCounts = useCallback(async (meetingId) => {
    if (!meetingId) return;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/counts/${meetingId}/`);
      const result = await response.json();

      if (result.success) {
        setReactionCounts(result.reaction_counts || {});
      }
    } catch (error) {
      console.error('Failed to load reaction counts:', error);
    }
  }, []);

  // Load active reactions
  const loadActiveReactions = useCallback(async (meetingId) => {
    if (!meetingId) return;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/active/${meetingId}/`);
      const result = await response.json();

      if (result.success) {
        const reactions = result.active_reactions || [];
        setActiveReactions(reactions.map(r => ({
          id: r.id,
          emoji: r.emoji,
          userName: r.user.full_name,
          userId: r.user.user_id,
          timestamp: r.timestamp,
          expiresAt: r.expires_at,
          timeRemaining: r.time_remaining
        })));
      }
    } catch (error) {
      console.error('Failed to load active reactions:', error);
    }
  }, []);

  // Clear all reactions (host only)
  const clearAllReactions = useCallback(async (meetingId, hostUserId) => {
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
        // Send via LiveKit for real-time broadcasting
        if (result.send_via_livekit && result.data && sendMessage) {
          sendMessage('clear_all_reactions', result.data);
        }

        // Clear local state
        setActiveReactions([]);
        setReactionHistory([]);
        setReactionCounts({});

        console.log(`✅ All reactions cleared (${result.cleared_count} reactions)`);
        return true;
      } else {
        console.error('Failed to clear reactions:', result.error);
        return false;
      }
    } catch (error) {
      console.error('Failed to clear all reactions:', error);
      return false;
    }
  }, [sendMessage]);

  // Handle incoming reactions from LiveKit
  useEffect(() => {
    if (!room) return;

    const handleDataReceived = (payload, participant) => {
      try {
        const decoder = new TextDecoder();
        const data = JSON.parse(decoder.decode(payload));
        
        if (data.type === 'reaction') {
          const receivedReaction = {
            id: data.timestamp || Date.now(),
            emoji: data.emoji,
            userName: data.user_name,
            userId: data.user_id,
            timestamp: data.timestamp
          };

          setReactionHistory(prev => [receivedReaction, ...prev.slice(0, 49)]);

          // Play sound for received reactions
          if (soundEnabled) {
            const reactionData = availableReactions.find(r => r.emoji === data.emoji);
            if (reactionData?.sound) {
              const audio = new Audio(reactionData.sound);
              audio.volume = 0.2;
              audio.play().catch(console.error);
            }
          }

          // Update counts
          if (currentMeetingId) {
            loadReactionCounts(currentMeetingId);
          }
        } else if (data.type === 'clear_all_reactions') {
          // Handle clear all reactions
          setActiveReactions([]);
          setReactionHistory([]);
          setReactionCounts({});
          console.log('🧹 All reactions cleared by host');
        }
      } catch (error) {
        console.error('Failed to parse reaction data:', error);
      }
    };

    room.on('dataReceived', handleDataReceived);

    return () => {
      room.off('dataReceived', handleDataReceived);
    };
  }, [room, soundEnabled, availableReactions, currentMeetingId, loadReactionCounts]);

  // Auto-load reactions when meeting changes
  useEffect(() => {
    if (currentMeetingId && (connected || isConnected)) {
      loadReactionCounts(currentMeetingId);
      loadActiveReactions(currentMeetingId);
    }
  }, [currentMeetingId, connected, isConnected, loadReactionCounts, loadActiveReactions]);

  // Get reaction by name
  const getReactionByName = useCallback((name) => {
    return availableReactions.find(r => r.name === name);
  }, [availableReactions]);

  // Get reaction by emoji
  const getReactionByEmoji = useCallback((emoji) => {
    return availableReactions.find(r => r.emoji === emoji);
  }, [availableReactions]);

  // Toggle sound
  const toggleSound = useCallback(() => {
    setSoundEnabled(prev => !prev);
  }, []);

  // Get reactions count by type
  const getReactionCount = useCallback((emoji) => {
    return reactionCounts[emoji]?.count || 0;
  }, [reactionCounts]);

  // Get total reactions count
  const getTotalReactionsCount = useCallback(() => {
    return Object.values(reactionCounts).reduce((total, reactionData) => {
      return total + (reactionData.count || 0);
    }, 0);
  }, [reactionCounts]);

  const value = {
    // State
    activeReactions,
    reactionHistory,
    reactionCounts,
    availableReactions,
    soundEnabled,
    currentMeetingId,

    // Actions
    sendReaction: handleSendReaction,
    clearAllReactions,
    initializeMeetingReactions,
    endMeetingReactions,
    loadReactionCounts,
    loadActiveReactions,
    toggleSound,

    // Utilities
    getReactionByName,
    getReactionByEmoji,
    getReactionCount,
    getTotalReactionsCount,
  };

  return (
    <ReactionsContext.Provider value={value}>
      {children}
    </ReactionsContext.Provider>
  );
}