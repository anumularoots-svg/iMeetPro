// UPDATED: src/services/reactions.js - Cache-Only Backend Integration
import { API_BASE_URL } from '../utils/constants';

class ReactionsService {
  constructor() {
    this.activeReactions = new Map();
    this.reactionCounts = {};
    this.soundEnabled = true;
    this.currentMeetingId = null;
  }

  // FIXED: Standard reaction emojis matching backend
  static REACTIONS = {
    THUMBS_UP: '👍',
    THUMBS_DOWN: '👎',
    HEART: '❤',
    CLAP: '👏',
    CELEBRATION: '🎉',
    FIRE: '🔥',
    THINKING: '🤔'
  };

  // Initialize meeting reactions on backend
  async initializeMeeting(meetingId) {
    if (!meetingId) {
      throw new Error('Meeting ID is required');
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
        this.currentMeetingId = meetingId;
        console.log('✅ Meeting reactions initialized:', result.message);
        return result;
      } else {
        throw new Error(result.error || 'Failed to initialize meeting reactions');
      }
    } catch (error) {
      console.error('Failed to initialize meeting reactions:', error);
      throw error;
    }
  }

  // Send reaction to cache-only backend
  async sendReaction(meetingId, userId, userName, emoji, participantIdentity = null) {
    if (!meetingId || !userId || !userName || !emoji) {
      throw new Error('Missing required parameters');
    }

    // Validate emoji
    if (!Object.values(ReactionsService.REACTIONS).includes(emoji)) {
      throw new Error(`Invalid reaction emoji: ${emoji}. Allowed: ${Object.values(ReactionsService.REACTIONS).join(', ')}`);
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
          user_name: userName,
          emoji: emoji,
          participant_identity: participantIdentity || `user_${userId}`
        })
      });

      const result = await response.json();
      
      if (result.success) {
        console.log('✅ Reaction sent successfully');
        
        // Update local state
        this.setActiveReaction(userId, {
          id: Date.now(),
          emoji: emoji,
          userName: userName,
          userId: userId,
          timestamp: new Date().toISOString(),
          displayDuration: result.display_duration_seconds || 5
        });

        // Play sound effect
        this.playReactionSound(emoji);

        return result;
      } else {
        throw new Error(result.error || 'Failed to send reaction');
      }
    } catch (error) {
      console.error('Failed to send reaction:', error);
      throw error;
    }
  }

  // Get active reactions from backend
  async getActiveReactions(meetingId) {
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/active/${meetingId}/`);
      const result = await response.json();
      
      if (result.success) {
        return result.active_reactions || [];
      } else {
        if (result.error && result.error.includes('not found')) {
          // Meeting not active
          return [];
        }
        throw new Error(result.error || 'Failed to get active reactions');
      }
    } catch (error) {
      console.error('Failed to get active reactions:', error);
      throw error;
    }
  }

  // Get reaction counts from backend
  async getReactionCounts(meetingId) {
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/counts/${meetingId}/`);
      const result = await response.json();
      
      if (result.success) {
        this.reactionCounts = result.reaction_counts || {};
        return this.reactionCounts;
      } else {
        if (result.error && result.error.includes('not found')) {
          // Meeting not active
          this.reactionCounts = {};
          return {};
        }
        throw new Error(result.error || 'Failed to get reaction counts');
      }
    } catch (error) {
      console.error('Failed to get reaction counts:', error);
      throw error;
    }
  }

  // Clear all reactions (host only)
  async clearAllReactions(meetingId, hostUserId) {
    if (!meetingId || !hostUserId) {
      throw new Error('Meeting ID and host user ID are required');
    }

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
        // Clear local state
        this.activeReactions.clear();
        this.reactionCounts = {};
        
        console.log(`✅ All reactions cleared (${result.cleared_count} reactions)`);
        return result;
      } else {
        throw new Error(result.error || 'Failed to clear all reactions');
      }
    } catch (error) {
      console.error('Failed to clear all reactions:', error);
      throw error;
    }
  }

  // End meeting reactions (deletes all data)
  async endMeetingReactions(meetingId) {
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

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
        // Clear local state
        this.activeReactions.clear();
        this.reactionCounts = {};
        this.currentMeetingId = null;
        
        console.log('✅ Meeting reactions ended, all data deleted');
        return result;
      } else {
        throw new Error(result.error || 'Failed to end meeting reactions');
      }
    } catch (error) {
      console.error('Failed to end meeting reactions:', error);
      throw error;
    }
  }

  // Get meeting reactions statistics
  async getMeetingStats(meetingId) {
    if (!meetingId) {
      throw new Error('Meeting ID is required');
    }

    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/stats/${meetingId}/`);
      const result = await response.json();
      
      if (result.success) {
        return result.stats;
      } else {
        if (result.error && result.error.includes('not found')) {
          // Meeting not active
          return null;
        }
        throw new Error(result.error || 'Failed to get meeting stats');
      }
    } catch (error) {
      console.error('Failed to get meeting stats:', error);
      throw error;
    }
  }

  // Get allowed reactions
  async getAllowedReactions() {
    try {
      const response = await fetch(`${API_BASE_URL}/api/cache-reactions/allowed/`);
      const result = await response.json();
      
      if (result.success) {
        return result.allowed_reactions;
      } else {
        throw new Error(result.error || 'Failed to get allowed reactions');
      }
    } catch (error) {
      console.error('Failed to get allowed reactions:', error);
      // Return default reactions if API fails
      return Object.entries(ReactionsService.REACTIONS).map(([type, emoji]) => ({
        emoji,
        reaction_type: type.toLowerCase(),
        description: type.replace('_', ' ').toLowerCase()
      }));
    }
  }

  // Receive reaction from other participants (via LiveKit)
  receiveReaction(reactionData) {
    if (!reactionData || !reactionData.emoji || !reactionData.user_id) {
      console.warn('Invalid reaction data received');
      return;
    }

    // Add to local active reactions
    this.setActiveReaction(reactionData.user_id, {
      id: reactionData.timestamp || Date.now(),
      emoji: reactionData.emoji,
      userName: reactionData.user_name || 'Unknown',
      userId: reactionData.user_id,
      timestamp: reactionData.timestamp || new Date().toISOString(),
      displayDuration: reactionData.display_duration || 5
    });

    // Play sound effect
    this.playReactionSound(reactionData.emoji);

    console.log('📥 Reaction received:', reactionData.emoji, 'from', reactionData.user_name);
  }

  // Set active reaction for participant
  setActiveReaction(participantId, reactionData) {
    // Clear existing reaction for this participant
    if (this.activeReactions.has(participantId)) {
      this.clearActiveReaction(participantId);
    }

    // Set new active reaction
    this.activeReactions.set(participantId, reactionData);

    // Set timer to clear reaction
    setTimeout(() => {
      this.clearActiveReaction(participantId);
    }, (reactionData.displayDuration || 5) * 1000);
  }

  // Clear active reaction for participant
  clearActiveReaction(participantId) {
    this.activeReactions.delete(participantId);
  }

  // Get active reaction for participant
  getActiveReaction(participantId) {
    return this.activeReactions.get(participantId);
  }

  // Get all active reactions
  getAllActiveReactions() {
    return Array.from(this.activeReactions.entries()).map(([participantId, reaction]) => ({
      participantId,
      ...reaction
    }));
  }

  // Play reaction sound
  playReactionSound(emoji) {
    if (!this.soundEnabled) return;

    const soundMap = {
      '👍': '/sounds/reaction.mp3',
      '👎': '/sounds/reaction.mp3',
      '❤': '/sounds/reaction.mp3',
      '👏': '/sounds/clap.mp3',
      '🎉': '/sounds/celebration.mp3',
      '🔥': '/sounds/reaction.mp3',
      '🤔': '/sounds/reaction.mp3'
    };

    const soundFile = soundMap[emoji] || '/sounds/reaction.mp3';
    
    try {
      const audio = new Audio(soundFile);
      audio.volume = 0.3;
      audio.play().catch(e => console.log('Could not play reaction sound:', e));
    } catch (error) {
      console.log('Reaction sound not available:', error);
    }
  }

  // Toggle sound
  toggleSound() {
    this.soundEnabled = !this.soundEnabled;
    return this.soundEnabled;
  }

  // Get reaction display name
  getReactionDisplayName(emoji) {
    const names = {
      '👍': 'Thumbs Up',
      '👎': 'Thumbs Down',
      '❤': 'Heart',
      '👏': 'Clap',
      '🎉': 'Celebration',
      '🔥': 'Fire',
      '🤔': 'Thinking'
    };
    return names[emoji] || 'Reaction';
  }

  // Validate reaction emoji
  isValidReaction(emoji) {
    return Object.values(ReactionsService.REACTIONS).includes(emoji);
  }

  // Get reaction statistics
  getReactionStats() {
    const stats = {};
    let total = 0;

    Object.entries(this.reactionCounts).forEach(([emoji, data]) => {
      const count = data.count || 0;
      stats[emoji] = count;
      total += count;
    });

    const mostUsed = Object.entries(stats).sort((a, b) => b[1] - a[1])[0];

    return {
      total,
      breakdown: stats,
      mostUsed: mostUsed ? { emoji: mostUsed[0], count: mostUsed[1] } : null
    };
  }

  // Clean up service
  cleanup() {
    this.activeReactions.clear();
    this.reactionCounts = {};
    this.currentMeetingId = null;
  }
}

export default new ReactionsService();