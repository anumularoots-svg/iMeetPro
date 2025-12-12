// UPDATED: src/components/reactions/ReactionsPanel.jsx - Cache-Only Backend Integration

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Heart, ThumbsUp, ThumbsDown, Smile, Hand, Zap, Star, Coffee } from 'lucide-react';
import { useLiveKit } from '../../hooks/useLiveKit';
import { API_BASE_URL } from '../../utils/constants';

const ReactionsPanel = ({ className = '', meetingId, currentUser }) => {
  const { 
    room,
    sendMessage,
    connected,
    isConnected
  } = useLiveKit();

  const [isOpen, setIsOpen] = useState(false);
  const [activeReactions, setActiveReactions] = useState([]);
  const [reactionHistory, setReactionHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  const [reactionCounts, setReactionCounts] = useState({});
  
  const panelRef = useRef(null);
  const animationContainerRef = useRef(null);

  // 🎯 FIXED: Use same emojis as backend allows
  const quickReactions = [
    { emoji: '👍', label: 'Thumbs Up', icon: ThumbsUp, color: 'text-green-500', type: 'thumbs_up' },
    { emoji: '👎', label: 'Thumbs Down', icon: ThumbsDown, color: 'text-red-500', type: 'thumbs_down' },
    { emoji: '❤', label: 'Heart', icon: Heart, color: 'text-red-500', type: 'heart' },
    { emoji: '👏', label: 'Clap', icon: null, color: 'text-blue-500', type: 'clap' },
    { emoji: '🎉', label: 'Celebration', icon: Star, color: 'text-purple-500', type: 'celebration' },
    { emoji: '🔥', label: 'Fire', icon: Zap, color: 'text-orange-500', type: 'fire' },
    { emoji: '🤔', label: 'Thinking', icon: null, color: 'text-yellow-500', type: 'thinking' }
  ];

  // 🎭 Send reaction to cache-only backend
  const handleSendReaction = useCallback(async (reactionData) => {
    if (!connected && !isConnected) {
      console.warn('Not connected to meeting');
      return;
    }

    if (!meetingId || !currentUser) {
      console.error('Missing meetingId or currentUser');
      return;
    }

    try {
      // Send to cache-only backend
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/add/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          meeting_id: meetingId,
          user_id: currentUser.id,
          user_name: currentUser.full_name || currentUser.name || 'You',
          emoji: reactionData.emoji,
          participant_identity: currentUser.participant_identity || `user_${currentUser.id}`
        })
      });

      const result = await response.json();

      if (result.success) {
        // Send via LiveKit for real-time broadcasting
        if (result.send_via_livekit && result.data) {
          sendMessage('reaction', result.data);
        }

        // Add to local reaction history
        const newReaction = {
          id: Date.now(),
          emoji: reactionData.emoji,
          label: reactionData.label,
          userName: currentUser.full_name || currentUser.name || 'You',
          userId: currentUser.id,
          timestamp: new Date().toISOString()
        };

        setReactionHistory(prev => [newReaction, ...prev.slice(0, 49)]);
        
        // Create floating animation
        createFloatingReaction(reactionData.emoji);
        
        // Show success feedback
        showReactionFeedback(reactionData);

        // Update reaction counts
        loadReactionCounts();
        
        console.log('✅ Reaction sent successfully');
      } else {
        console.error('Failed to send reaction:', result.error);
      }
    } catch (error) {
      console.error('Failed to send reaction:', error);
    }
  }, [connected, isConnected, meetingId, currentUser, sendMessage]);

  // 🎨 Create floating reaction animation
  const createFloatingReaction = useCallback((emoji) => {
    if (!animationContainerRef.current) return;

    const reactionElement = document.createElement('div');
    reactionElement.textContent = emoji;
    reactionElement.className = 'absolute text-4xl pointer-events-none z-50 animate-bounce';
    reactionElement.style.left = Math.random() * 300 + 'px';
    reactionElement.style.bottom = '0px';
    reactionElement.style.animation = 'float-up 3s ease-out forwards';

    animationContainerRef.current.appendChild(reactionElement);

    // Remove after animation
    setTimeout(() => {
      if (reactionElement.parentNode) {
        reactionElement.parentNode.removeChild(reactionElement);
      }
    }, 3000);
  }, []);

  // ✨ Show reaction feedback
  const showReactionFeedback = useCallback((reactionData) => {
    const feedbackReaction = {
      id: Date.now(),
      ...reactionData,
      timestamp: Date.now()
    };

    setActiveReactions(prev => [...prev, feedbackReaction]);

    // Remove after 2 seconds
    setTimeout(() => {
      setActiveReactions(prev => prev.filter(r => r.id !== feedbackReaction.id));
    }, 2000);
  }, []);

  // 📥 Load reaction counts from backend
  const loadReactionCounts = useCallback(async () => {
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
  }, [meetingId]);

  // 📥 Load active reactions from backend
  const loadActiveReactions = useCallback(async () => {
    if (!meetingId) return;

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/active/${meetingId}/`);
      const result = await response.json();

      if (result.success) {
        const reactions = result.active_reactions || [];
        setReactionHistory(reactions.map(r => ({
          id: r.id,
          emoji: r.emoji,
          userName: r.user.full_name,
          userId: r.user.user_id,
          timestamp: r.timestamp
        })));
      }
    } catch (error) {
      console.error('Failed to load active reactions:', error);
    }
  }, [meetingId]);

  // 📥 Handle received reactions from LiveKit
  useEffect(() => {
    if (!room) return;

    const handleDataReceived = (payload, participant) => {
      try {
        const decoder = new TextDecoder();
        const data = JSON.parse(decoder.decode(payload));
        
        if (data.type === 'reaction' && data.user_id !== currentUser?.id) {
          // Add to reaction history
          const receivedReaction = {
            id: Date.now(),
            emoji: data.emoji,
            userName: data.user_name,
            userId: data.user_id,
            timestamp: data.timestamp
          };

          setReactionHistory(prev => [receivedReaction, ...prev.slice(0, 49)]);
          
          // Create floating animation for received reaction
          createFloatingReaction(data.emoji);

          // Update counts
          loadReactionCounts();
        }
      } catch (error) {
        console.error('Failed to parse reaction data:', error);
      }
    };

    room.on('dataReceived', handleDataReceived);
    
    return () => {
      room.off('dataReceived', handleDataReceived);
    };
  }, [room, currentUser?.id, createFloatingReaction, loadReactionCounts]);

  // 🎯 Handle click outside to close
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (panelRef.current && !panelRef.current.contains(event.target)) {
        setIsOpen(false);
        setShowHistory(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // ⌨️ Keyboard shortcuts
  useEffect(() => {
    const handleKeyPress = (event) => {
      if (!connected && !isConnected) return;

      // Number keys 1-7 for quick reactions
      const keyNum = parseInt(event.key);
      if (keyNum >= 1 && keyNum <= 7 && !event.ctrlKey && !event.altKey) {
        const reaction = quickReactions[keyNum - 1];
        if (reaction && !event.target.matches('input, textarea')) {
          event.preventDefault();
          handleSendReaction(reaction);
        }
      }
    };

    document.addEventListener('keydown', handleKeyPress);
    return () => document.removeEventListener('keydown', handleKeyPress);
  }, [connected, isConnected, handleSendReaction]);

  // Load initial data
  useEffect(() => {
    if (meetingId && (connected || isConnected)) {
      loadReactionCounts();
      loadActiveReactions();
    }
  }, [meetingId, connected, isConnected, loadReactionCounts, loadActiveReactions]);

  // 🕒 Format timestamp
  const formatTimestamp = useCallback((timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }, []);

  // 📊 Get reaction count by emoji
  const getReactionCount = useCallback((emoji) => {
    return reactionCounts[emoji]?.count || 0;
  }, [reactionCounts]);

  // 🎨 Render reaction button
  const renderReactionButton = useCallback((reaction, index) => {
    const IconComponent = reaction.icon;
    const count = getReactionCount(reaction.emoji);
    
    return (
      <button
        key={reaction.emoji}
        onClick={() => handleSendReaction(reaction)}
        disabled={!connected && !isConnected}
        className={`
          relative group flex flex-col items-center p-3 rounded-lg border-2 border-transparent
          hover:border-gray-200 hover:bg-gray-50 transition-all duration-200 
          disabled:opacity-50 disabled:cursor-not-allowed
          ${count > 0 ? 'bg-blue-50 border-blue-200' : ''}
        `}
        title={`${reaction.label} (${index + 1})`}
      >
        <div className="relative">
          {IconComponent ? (
            <IconComponent className={`w-6 h-6 ${reaction.color}`} />
          ) : (
            <span className="text-2xl">{reaction.emoji}</span>
          )}
          
          {count > 0 && (
            <span className="absolute -top-2 -right-2 bg-blue-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center">
              {count > 99 ? '99+' : count}
            </span>
          )}
        </div>
        
        <span className="text-xs text-gray-600 mt-1 group-hover:text-gray-800">
          {reaction.label}
        </span>
      </button>
    );
  }, [handleSendReaction, connected, isConnected, getReactionCount]);

  return (
    <div ref={panelRef} className={`relative ${className}`}>
      {/* Animation Container */}
      <div 
        ref={animationContainerRef} 
        className="fixed inset-0 pointer-events-none z-40"
        style={{
          background: 'transparent'
        }}
      />

      {/* CSS for floating animation */}
      <style jsx>{`
        @keyframes float-up {
          0% {
            transform: translateY(0) scale(1);
            opacity: 1;
          }
          50% {
            transform: translateY(-100px) scale(1.2);
            opacity: 0.8;
          }
          100% {
            transform: translateY(-200px) scale(0.8);
            opacity: 0;
          }
        }
      `}</style>

      {/* Toggle Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        disabled={!connected && !isConnected}
        className={`
          p-3 rounded-full transition-all duration-200 
          ${isOpen ? 'bg-blue-500 text-white' : 'bg-white text-gray-600 hover:bg-gray-100'}
          ${(!connected && !isConnected) ? 'opacity-50 cursor-not-allowed' : 'hover:shadow-lg'}
          border border-gray-200 relative
        `}
        title="Reactions"
      >
        <Smile className="w-5 h-5" />
        
        {activeReactions.length > 0 && (
          <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full w-4 h-4 flex items-center justify-center">
            {activeReactions.length}
          </span>
        )}
      </button>

      {/* Reactions Panel */}
      {isOpen && (
        <div className="absolute bottom-full right-0 mb-2 bg-white rounded-lg shadow-xl border border-gray-200 p-4 w-80 z-50">
          {/* Header */}
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-semibold text-gray-900">Quick Reactions</h3>
            <button
              onClick={() => setShowHistory(!showHistory)}
              className="text-sm text-blue-600 hover:text-blue-800"
            >
              {showHistory ? 'Hide History' : 'Show History'}
            </button>
          </div>

          {(!connected && !isConnected) && (
            <div className="mb-4 p-2 bg-orange-100 text-orange-700 rounded text-sm text-center">
              Reactions unavailable - not connected to meeting
            </div>
          )}

          {/* Quick Reactions Grid */}
          <div className="grid grid-cols-4 gap-2 mb-4">
            {quickReactions.map((reaction, index) => 
              renderReactionButton(reaction, index)
            )}
          </div>

          {/* Keyboard Shortcut Hint */}
          <div className="text-xs text-gray-500 text-center mb-4">
            Press 1-7 for quick reactions
          </div>

          {/* Recent Reactions History */}
          {showHistory && (
            <div className="border-t pt-4">
              <h4 className="font-medium text-gray-900 mb-2">Recent Reactions</h4>
              
              {reactionHistory.length === 0 ? (
                <div className="text-center text-gray-500 py-4">
                  <Smile className="w-8 h-8 mx-auto mb-2 opacity-50" />
                  <div className="text-sm">No reactions yet</div>
                  <div className="text-xs">Be the first to react!</div>
                </div>
              ) : (
                <div className="max-h-40 overflow-y-auto space-y-2">
                  {reactionHistory.slice(0, 20).map((reaction) => (
                    <div 
                      key={reaction.id}
                      className="flex items-center justify-between p-2 bg-gray-50 rounded"
                    >
                      <div className="flex items-center space-x-2">
                        <span className="text-lg">{reaction.emoji}</span>
                        <span className="text-sm text-gray-600">
                          {reaction.userName}
                        </span>
                      </div>
                      <span className="text-xs text-gray-400">
                        {formatTimestamp(reaction.timestamp)}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Active Reactions Display */}
          {activeReactions.length > 0 && (
            <div className="border-t pt-4">
              <div className="flex flex-wrap gap-1">
                {activeReactions.map((reaction) => (
                  <div
                    key={reaction.id}
                    className="animate-pulse bg-blue-100 text-blue-800 px-2 py-1 rounded-full text-sm"
                  >
                    {reaction.emoji} Sent!
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Floating Active Reactions */}
      {reactionHistory.slice(0, 5).map((reaction) => (
        <div
          key={reaction.id}
          className="absolute bottom-full right-0 mb-16 animate-bounce bg-white shadow-lg rounded-full p-2 border border-gray-200"
          style={{
            right: `${Math.random() * 200}px`,
            animationDelay: `${Math.random() * 1000}ms`,
            animationDuration: '3s'
          }}
        >
          <span className="text-2xl">{reaction.emoji}</span>
        </div>
      ))}
    </div>
  );
};

export default ReactionsPanel;