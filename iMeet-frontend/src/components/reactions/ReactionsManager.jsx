// src/components/reactions/ReactionsManager.jsx - COMPLETE IMPLEMENTATION
import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { Box, Typography, Snackbar, Alert, IconButton, Tooltip } from '@mui/material';
import { VolumeUp, VolumeOff } from '@mui/icons-material';
import { useReactions } from '../../hooks/useReactions';
import ProfessionalReactionsPanel from './ProfessionalReactionsPanel';
import FloatingReactionsOverlay from './FloatingReactionsOverlay';
import { DataPacket_Kind } from 'livekit-client';

/**
 * Complete Reactions Management System
 * Handles all reaction logic, display, synchronization, and backend connections
 * 
 * Features:
 * - Send/receive reactions via LiveKit
 * - Backend synchronization with Redis cache
 * - Real-time reaction animations
 * - Sound effects for reactions
 * - Participant reaction tracking
 * - Reaction statistics
 * - Error handling and notifications
 */
const ReactionsManager = ({
  // ===== Required Props =====
  meetingId,              // Meeting ID for backend sync
  currentUser,            // Current user object with id, name, etc.
  room,                   // LiveKit room instance
  
  // ===== Participant Data =====
  allParticipants = [],   // List of all meeting participants
  
  // ===== UI State =====
  reactionsOpen,          // Boolean: is reactions panel open
  onReactionsToggle,      // Function: toggle reactions panel
  
  // ===== Meeting Settings =====
  reactionsEnabled = true,          // Boolean: are reactions enabled
  isConnected = false,               // Boolean: is connected to meeting
  isHost = false,                    // Boolean: is user the host
  isCoHost = false,                  // Boolean: is user a co-host
  
  // ===== Callbacks =====
  onNotification,         // Function: show notification (message, severity)
  onError,                // Function: handle errors
  
  // ===== Optional Customization =====
  soundEnabled: propSoundEnabled,    // Override sound state
  onSoundToggle,                     // Custom sound toggle handler
  showSoundControl = true,           // Show sound control button
  showDebugInfo = false,             // Show debug information
  autoHideReactions = true,          // Auto-hide reactions after duration
  reactionDisplayDuration = 5000,    // How long to show reactions (ms)
  maxVisibleReactions = 10,          // Max reactions to show at once
  enableReactionHistory = true,      // Track reaction history
  enableReactionStats = true,        // Track reaction statistics
}) => {
  // ===== Hooks =====
  const {
    sendReactionToMeeting,
    activeReactions,
    reactionCounts,
    participantReactions,
    getParticipantReaction,
    initializeMeetingReactions,
    endMeetingReactions,
    clearAllReactions,
    soundEnabled: hookSoundEnabled,
    toggleSound: hookToggleSound,
    loadReactionCounts,
    loadActiveReactions,
  } = useReactions(meetingId, room);

  // ===== Local State =====
  const [initialized, setInitialized] = useState(false);
  const [error, setError] = useState(null);
  const [reactionHistory, setReactionHistory] = useState([]);
  const [localReactionCounts, setLocalReactionCounts] = useState({});
  const [showNotification, setShowNotification] = useState(false);
  const [notificationMessage, setNotificationMessage] = useState('');
  const [notificationSeverity, setNotificationSeverity] = useState('info');
  const [isProcessing, setIsProcessing] = useState(false);
  const [lastReactionTime, setLastReactionTime] = useState(0);

  // ===== Refs =====
  const initializationAttempted = useRef(false);
  const cleanupTriggered = useRef(false);
  const reactionQueueRef = useRef([]);
  const processingQueueRef = useRef(false);
  const statsUpdateTimerRef = useRef(null);
  const historyCleanupTimerRef = useRef(null);
  const recentReactionKeysRef = useRef(new Set());

  // ===== Computed Values =====
  // Determine sound state (prop takes precedence over hook)
  const soundEnabled = useMemo(() => {
    return propSoundEnabled !== undefined ? propSoundEnabled : hookSoundEnabled;
  }, [propSoundEnabled, hookSoundEnabled]);

  // Sound toggle handler (prop takes precedence)
  const toggleSound = useMemo(() => {
    return onSoundToggle || hookToggleSound;
  }, [onSoundToggle, hookToggleSound]);

  // Check if user has host privileges
  const hasHostPrivileges = useMemo(() => {
    return isHost || isCoHost;
  }, [isHost, isCoHost]);

  // Get current reaction statistics
  const reactionStats = useMemo(() => {
    if (!enableReactionStats) return null;

    const stats = {
      total: 0,
      byType: {},
      topReaction: null,
      recentCount: 0,
    };

    Object.entries(reactionCounts).forEach(([emoji, data]) => {
      const count = data?.count || 0;
      stats.total += count;
      stats.byType[emoji] = count;

      if (!stats.topReaction || count > stats.byType[stats.topReaction]) {
        stats.topReaction = emoji;
      }
    });

    // Count reactions in last 30 seconds
    const now = Date.now();
    stats.recentCount = reactionHistory.filter(
      r => now - r.timestamp < 30000
    ).length;

    return stats;
  }, [reactionCounts, reactionHistory, enableReactionStats]);

  // ===== Notification Helper =====
  const showMessage = useCallback((message, severity = 'info') => {
    setNotificationMessage(message);
    setNotificationSeverity(severity);
    setShowNotification(true);
    
    // Also call parent notification handler if provided
    if (onNotification) {
      onNotification(message, severity);
    }
  }, [onNotification]);

  // ===== Error Handler =====
  const handleError = useCallback((error, context = '') => {
    console.error(`❌ ReactionsManager Error [${context}]:`, error);
    
    const errorMessage = error?.message || 'An error occurred';
    setError(errorMessage);
    
    if (onError) {
      onError(error, context);
    }
    
    showMessage(`Reaction error: ${errorMessage}`, 'error');
  }, [onError, showMessage]);

useEffect(() => {
  if (!meetingId || !room || !isConnected) {
    console.log('⏳ ReactionsManager: Waiting for prerequisites', {
      meetingId: !!meetingId,
      room: !!room,
      isConnected,
    });
    return;
  }

  if (initializationAttempted.current) {
    return;
  }

  initializationAttempted.current = true;

  const initializeSystem = async () => {
    try {
      console.log('🎭 ReactionsManager: Initializing...', {
        meetingId,
        userId: currentUser?.id,
        roomConnected: !!room,
      });

      // Initialize via hook
      if (typeof initializeMeetingReactions === 'function') {
        const success = await initializeMeetingReactions();
        
        if (success) {
          setInitialized(true);
          console.log('✅ ReactionsManager: Initialized successfully');
          
          // Load initial data
          if (typeof loadReactionCounts === 'function') {
            await loadReactionCounts();
          }
          
          if (typeof loadActiveReactions === 'function') {
            await loadActiveReactions();
          }
          
          showMessage('Reactions system ready', 'success');
        } else {
          // DON'T throw error - just log it and handle gracefully
          console.warn('⚠️ ReactionsManager: Initialization returned false, but continuing anyway');
          setInitialized(true); // Still set initialized to allow basic functionality
          showMessage('Reactions system initialized with limitations', 'warning');
        }
      } else {
        // If hook doesn't have initialization, mark as initialized
        setInitialized(true);
        console.log('✅ ReactionsManager: Initialized (no explicit init needed)');
      }
    } catch (error) {
      console.error('❌ ReactionsManager initialization error:', error);
      handleError(error, 'initialization');
      // Set initialized to true anyway to allow basic functionality
      setInitialized(true);
      showMessage('Reactions system started with errors', 'error');
    }
  };

  initializeSystem();
}, [
  meetingId,
  room,
  isConnected,
  currentUser,
  initializeMeetingReactions,
  loadReactionCounts,
  loadActiveReactions,
  handleError,
  showMessage,
]);

  // ===== Cleanup on Unmount =====
  useEffect(() => {
    return () => {
      if (cleanupTriggered.current) return;
      cleanupTriggered.current = true;

      console.log('🧹 ReactionsManager: Cleaning up...');

      // Clear timers
      if (statsUpdateTimerRef.current) {
        clearInterval(statsUpdateTimerRef.current);
      }
      if (historyCleanupTimerRef.current) {
        clearInterval(historyCleanupTimerRef.current);
      }

      // End reactions if function exists
      if (typeof endMeetingReactions === 'function' && meetingId) {
        endMeetingReactions().catch(err => {
          console.warn('Cleanup error:', err);
        });
      }
    };
  }, [meetingId, endMeetingReactions]);

  // ===== Reaction History Management =====
  useEffect(() => {
    if (!enableReactionHistory) return;

    // Add active reactions to history
    activeReactions.forEach(reaction => {
      setReactionHistory(prev => {
        // Check if already in history
        const exists = prev.some(r => 
          r.id === reaction.id || 
          (r.userId === reaction.userId && r.timestamp === reaction.timestamp)
        );

        if (exists) return prev;

        const newHistory = [
          {
            id: reaction.id || Date.now(),
            emoji: reaction.emoji,
            userId: reaction.userId,
            userName: reaction.userName || reaction.user_name,
            timestamp: reaction.timestamp || Date.now(),
          },
          ...prev,
        ].slice(0, 100); // Keep last 100

        return newHistory;
      });
    });
  }, [activeReactions, enableReactionHistory]);

  // ===== Cleanup Old History =====
  useEffect(() => {
    if (!enableReactionHistory) return;

    historyCleanupTimerRef.current = setInterval(() => {
      const cutoffTime = Date.now() - 300000; // 5 minutes
      setReactionHistory(prev => 
        prev.filter(r => r.timestamp > cutoffTime)
      );
    }, 60000); // Clean every minute

    return () => {
      if (historyCleanupTimerRef.current) {
        clearInterval(historyCleanupTimerRef.current);
      }
    };
  }, [enableReactionHistory]);

  // ===== Update Local Reaction Counts =====
  useEffect(() => {
    if (!enableReactionStats) return;

    setLocalReactionCounts(reactionCounts);

    // Periodically sync with backend
    statsUpdateTimerRef.current = setInterval(() => {
      if (typeof loadReactionCounts === 'function') {
        loadReactionCounts().catch(err => {
          console.warn('Stats sync error:', err);
        });
      }
    }, 30000); // Every 30 seconds

    return () => {
      if (statsUpdateTimerRef.current) {
        clearInterval(statsUpdateTimerRef.current);
      }
    };
  }, [reactionCounts, enableReactionStats, loadReactionCounts]);

  // ===== Process Reaction Queue =====
  const processReactionQueue = useCallback(async () => {
    if (processingQueueRef.current || reactionQueueRef.current.length === 0) {
      return;
    }

    processingQueueRef.current = true;

    try {
      while (reactionQueueRef.current.length > 0) {
        const reaction = reactionQueueRef.current.shift();
        
        try {
          await sendReactionToMeeting(reaction);
          await new Promise(resolve => setTimeout(resolve, 100)); // Rate limiting
        } catch (err) {
          console.error('Queue processing error:', err);
          // Don't stop queue processing for individual errors
        }
      }
    } finally {
      processingQueueRef.current = false;
    }
  }, [sendReactionToMeeting]);

  // ===== Send Reaction Handler =====
  const handleSendReaction = useCallback(
    async (emojiOrData) => {
      // Validation
      if (!isConnected) {
        showMessage("Not connected to meeting", "warning");
        return false;
      }

      if (!reactionsEnabled) {
        showMessage("Reactions are disabled", "warning");
        return false;
      }

      if (!meetingId || !currentUser?.id) {
        showMessage("User information not available", "error");
        return false;
      }

      if (isProcessing) {
        showMessage("Please wait, processing previous reaction", "info");
        return false;
      }

      // Rate limiting - prevent spam (500ms between reactions)
      const now = Date.now();
      if (now - lastReactionTime < 500) {
        showMessage("Please wait before sending another reaction", "warning");
        return false;
      }

      try {
        setIsProcessing(true);
        setLastReactionTime(now);

        // Extract emoji from data or use directly
        const emoji = typeof emojiOrData === 'string' 
          ? emojiOrData 
          : emojiOrData?.emoji || emojiOrData;

        console.log('📤 ReactionsManager: Sending reaction:', {
          emoji,
          userId: currentUser.id,
          userName: currentUser.full_name || currentUser.name,
          meetingId,
        });

        // Prepare reaction data
        const reactionData = {
          emoji,
          meetingId,
          user: {
            id: currentUser.id,
            full_name: currentUser.full_name || currentUser.name,
            name: currentUser.name || currentUser.full_name,
            username: currentUser.username,
            participant_identity: `user_${currentUser.id}`,
          },
        };

        // Send through the hook (handles LiveKit + backend)
        const success = await sendReactionToMeeting(reactionData);

        if (success) {
          console.log('✅ ReactionsManager: Reaction sent successfully');
          
          // Update local state immediately for instant feedback
          setReactionHistory(prev => [
            {
              id: Date.now(),
              emoji,
              userId: currentUser.id,
              userName: currentUser.full_name || currentUser.name,
              timestamp: Date.now(),
            },
            ...prev,
          ].slice(0, 100));

          // Close reactions panel after sending
          if (onReactionsToggle) {
            setTimeout(() => {
              onReactionsToggle(false);
            }, 250);
          }

          showMessage(`Sent ${emoji} reaction`, "success");
          return true;
        } else {
          throw new Error('Send reaction returned false');
        }
      } catch (error) {
        handleError(error, 'send_reaction');
        return false;
      } finally {
        setIsProcessing(false);
      }
    },
    [
      isConnected,
      reactionsEnabled,
      meetingId,
      currentUser,
      isProcessing,
      lastReactionTime,
      sendReactionToMeeting,
      onReactionsToggle,
      showMessage,
      handleError,
    ]
  );

  // ===== Clear All Reactions (Host Only) =====
  const handleClearAllReactions = useCallback(async () => {
    if (!hasHostPrivileges) {
      showMessage('Only hosts can clear all reactions', 'warning');
      return false;
    }

    if (!meetingId || !currentUser?.id) {
      showMessage('Cannot clear reactions - missing information', 'error');
      return false;
    }

    try {
      console.log('🧹 ReactionsManager: Clearing all reactions...');

      if (typeof clearAllReactions === 'function') {
        const success = await clearAllReactions(currentUser.id);
        
        if (success) {
          // Clear local state
          setReactionHistory([]);
          setLocalReactionCounts({});
          
          showMessage('All reactions cleared', 'success');
          return true;
        }
      }

      throw new Error('Clear all reactions failed');
    } catch (error) {
      handleError(error, 'clear_all');
      return false;
    }
  }, [
    hasHostPrivileges,
    meetingId,
    currentUser,
    clearAllReactions,
    showMessage,
    handleError,
  ]);

  // ===== Listen for Incoming Reactions via LiveKit =====
  useEffect(() => {
    if (!room) return;

    const handleDataReceived = (payload, participant) => {
      try {
        const decoder = new TextDecoder();
        const data = JSON.parse(decoder.decode(payload));

        // Only handle reaction notifications
        if (data.type === 'reaction_notification') {
          console.log('📨 ReactionsManager: Received reaction:', {
            emoji: data.emoji,
            from: data.user_name,
            userId: data.user_id,
          });

          // Skip own reactions
          if (data.user_id?.toString() === currentUser?.id?.toString()) {
            return;
          }

          // Deduplication
          const reactionKey = `${data.user_id}-${data.emoji}-${data.timestamp}`;
          if (recentReactionKeysRef.current.has(reactionKey)) {
            return;
          }

          recentReactionKeysRef.current.add(reactionKey);
          setTimeout(() => {
            recentReactionKeysRef.current.delete(reactionKey);
          }, 5000);

          // Add to history
          if (enableReactionHistory) {
            setReactionHistory(prev => [
              {
                id: `remote_${Date.now()}`,
                emoji: data.emoji,
                userId: data.user_id,
                userName: data.user_name,
                timestamp: data.timestamp || Date.now(),
              },
              ...prev,
            ].slice(0, 100));
          }
        }
      } catch (error) {
        console.warn('ReactionsManager: Data parse error:', error);
      }
    };

    room.on('dataReceived', handleDataReceived);

    return () => {
      room.off('dataReceived', handleDataReceived);
    };
  }, [room, currentUser, enableReactionHistory]);

  // ===== Don't Render if Disabled =====
  if (!reactionsEnabled) {
    return null;
  }

  // ===== Don't Render if Not Initialized =====
  if (!initialized && isConnected) {
    return (
      <Box
        sx={{
          position: 'fixed',
          bottom: 200,
          right: 50,
          background: 'rgba(0, 0, 0, 0.8)',
          color: 'white',
          p: 2,
          borderRadius: 2,
          zIndex: 9999,
        }}
      >
        <Typography variant="caption">
          Initializing reactions...
        </Typography>
      </Box>
    );
  }

  // ===== Render =====
  return (
    <>
      {/* ===== Floating Reactions Display Overlay ===== */}
      <FloatingReactionsOverlay
        participantReactions={participantReactions}
        allParticipants={allParticipants}
        currentUser={currentUser}
      />

      {/* ===== Reactions Panel ===== */}
      <ProfessionalReactionsPanel
        isOpen={reactionsOpen && reactionsEnabled && isConnected}
        onClose={() => onReactionsToggle?.(false)}
        onReaction={handleSendReaction}
        disabled={!isConnected || !reactionsEnabled || isProcessing}
      />

      {/* ===== Error Display ===== */}
      {error && (
        <Box
          sx={{
            position: 'fixed',
            top: 80,
            left: '50%',
            transform: 'translateX(-50%)',
            background: 'rgba(244, 67, 54, 0.95)',
            color: 'white',
            p: 2,
            borderRadius: 2,
            zIndex: 10001,
            maxWidth: 400,
          }}
        >
          <Typography variant="body2">
            {error}
          </Typography>
          <IconButton
            size="small"
            onClick={() => setError(null)}
            sx={{ position: 'absolute', top: 4, right: 4, color: 'white' }}
          >
            ×
          </IconButton>
        </Box>
      )}

      {/* ===== Notification Snackbar ===== */}
      <Snackbar
        open={showNotification}
        autoHideDuration={3000}
        onClose={() => setShowNotification(false)}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert
          onClose={() => setShowNotification(false)}
          severity={notificationSeverity}
          sx={{
            background: 'rgba(45, 55, 72, 0.98)',
            backdropFilter: 'blur(16px)',
            border: '1px solid rgba(255, 255, 255, 0.1)',
            borderRadius: 2,
            color: 'white',
          }}
        >
          {notificationMessage}
        </Alert>
      </Snackbar>

    </>
  );
};

// ===== Display Name for React DevTools =====
ReactionsManager.displayName = 'ReactionsManager';

// ===== Memoize to Prevent Unnecessary Re-renders =====
export default React.memo(ReactionsManager);

// ===== Named Exports for Testing/Advanced Usage =====
export { ReactionsManager };