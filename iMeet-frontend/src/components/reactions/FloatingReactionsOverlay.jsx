// src/components/reactions/FloatingReactionsOverlay.jsx - SLOW, CONTINUOUS FLOAT
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Box, Typography } from '@mui/material';

const FloatingReactionsOverlay = ({ 
  participantReactions, 
  allParticipants = [], 
  currentUser 
}) => {
  const [reactions, setReactions] = useState([]);
  const activeReactionsRef = useRef([]);
  const lastFrameTimeRef = useRef(null);
  const animationFrameRef = useRef(null);
  const processedReactionsRef = useRef(new Set());
  const processedReactionIds = useRef(new Set());

  // Get emoji unicode for Noto Emoji
  const getEmojiUnicode = useCallback((emoji) => {
    try {
      const codePoints = Array.from(emoji)
        .map(char => {
          const code = char.codePointAt(0);
          return code.toString(16).toLowerCase();
        })
        .join('_');
      return codePoints;
    } catch (error) {
      return '1f44d';
    }
  }, []);

  // ✅ FIXED: Add reaction with BOTTOM-RIGHT positioning
  const addReaction = useCallback((reactionData) => {
    const reactionKey = `${reactionData.participantId}-${reactionData.emoji}-${Date.now()}`;
    
    if (processedReactionsRef.current.has(reactionKey)) {
      return;
    }
    
    processedReactionsRef.current.add(reactionKey);
    
    setTimeout(() => {
      processedReactionsRef.current.delete(reactionKey);
    }, 2000);

    const reactionId = `reaction-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    console.log("✅ ADDING REACTION TO OVERLAY:", {
      id: reactionId,
      emoji: reactionData.emoji,
      participant: reactionData.participantName,
    });

    setReactions((prev) => {
      const filtered = prev.filter(
        (r) =>
          r.participantId !== reactionData.participantId ||
          Date.now() - r.startTime > 500
      );

      // ✅ Start from BOTTOM-RIGHT (75-85% from left)
      const rightPosition = 75 + Math.random() * 10; // 75% to 85% from left

      const newReaction = {
        id: reactionId,
        emoji: reactionData.emoji,
        participantName: reactionData.participantName,
        participantId: reactionData.participantId,
        startTime: Date.now(),
        x: rightPosition, // ✅ Bottom-right positioning
        y: 100, // ✅ Start from BOTTOM (100%)
        drift: (Math.random() - 0.5) * 3, // Very minimal horizontal drift
      };

      console.log("🎭 Reaction added to state:", newReaction);

      return [...filtered, newReaction];
    });
  }, []);

  // Process participantReactions with deduplication
  useEffect(() => {
    if (!participantReactions || participantReactions.size === 0) {
      return;
    }

    console.log("🎭 FloatingReactionsOverlay: Processing reactions from hook", {
      totalReactions: participantReactions.size,
    });

    Array.from(participantReactions.entries()).forEach(
      ([participantId, reaction]) => {
        const reactionId = `${participantId}-${reaction.emoji}-${reaction.timestamp}`;
        
        if (processedReactionIds.current.has(reactionId)) {
          return;
        }
        
        processedReactionIds.current.add(reactionId);
        
        setTimeout(() => {
          processedReactionIds.current.delete(reactionId);
        }, 10000);

        let participantName = reaction.userName || 
                             reaction.user_name || 
                             reaction.name || 
                             "Unknown";

        if (participantName === "Unknown" || !participantName) {
          const participant = allParticipants?.find(
            (p) => p.user_id?.toString() === participantId?.toString() ||
                   p.id?.toString() === participantId?.toString()
          );
          participantName =
            participant?.full_name || 
            participant?.name || 
            participant?.displayName ||
            "Someone";
        }

        if (participantId?.toString() === currentUser?.id?.toString()) {
          participantName = "You";
        }

        console.log("➕ Adding NEW reaction to overlay:", {
          emoji: reaction.emoji,
          participantName,
          participantId,
          reactionId,
        });

        addReaction({
          emoji: reaction.emoji || reaction.reaction,
          participantName,
          participantId,
          timestamp: reaction.timestamp || Date.now(),
        });
      }
    );
  }, [participantReactions, allParticipants, currentUser, addReaction]);

  // ✅ FIXED: SLOW, CONTINUOUS ANIMATION - Float from bottom to top without stopping
  useEffect(() => {
    if (reactions.length === 0) {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
        animationFrameRef.current = null;
      }
      return;
    }

    console.log("🎬 Floating reactions animation started for", reactions.length);
    activeReactionsRef.current = reactions.map((r) => ({
      ...r,
      x: r.x ?? 80,
      drift: r.drift ?? (Math.random() * 3 - 1.5),
      startTime: r.startTime ?? Date.now(),
    }));

    const animate = (timestamp) => {
      if (!lastFrameTimeRef.current) lastFrameTimeRef.current = timestamp;
      lastFrameTimeRef.current = timestamp;

      if (activeReactionsRef.current.length === 0) {
        animationFrameRef.current = null;
        return;
      }

      // ✅ SLOW DURATION: 8 seconds (was 4 seconds)
      const duration = 8000; // ✅ SLOWER animation
      const now = Date.now();

      const updatedReactions = activeReactionsRef.current.map((reaction) => {
        const elapsed = now - reaction.startTime;
        const progress = Math.min(elapsed / duration, 1);

        // ✅ LINEAR movement - smooth, continuous, no stopping
        const linearProgress = progress; // No easing - constant speed
        
        // ✅ FLOAT BEYOND TOP: 110% travel (goes off-screen at top)
        const translateY = -linearProgress * 110; // Float 110% (beyond viewport)
        
        // ✅ NO OPACITY FADE - Always fully visible
        const opacity = 1; // Always solid, no fade
        
        // ✅ MINIMAL SCALE - stays consistent size
        const scale = 1 + 0.05 * (1 - progress); // Very subtle scale

        // Apply transform
        const el = document.getElementById(reaction.id);
        if (el) {
          // ✅ Continuous upward movement from bottom
          el.style.left = `${reaction.x}%`;
          el.style.bottom = `${-translateY}%`; // Moves from bottom to beyond top
          el.style.transform = `translateX(-50%) scale(${scale})`;
          el.style.opacity = opacity; // ✅ Always 1 (no fade)
        }

        // ✅ Remove only when completely off-screen (110% traveled)
        if (progress < 1) {
          return reaction;
        }
        return null;
      }).filter(Boolean);

      activeReactionsRef.current = updatedReactions;

      if (updatedReactions.length > 0) {
        animationFrameRef.current = requestAnimationFrame(animate);
      } else {
        animationFrameRef.current = null;
      }
    };

    animationFrameRef.current = requestAnimationFrame(animate);

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
        animationFrameRef.current = null;
      }
    };
  }, [reactions]);

  // ✅ EXTENDED cleanup time (match animation duration)
  useEffect(() => {
    const cleanupInterval = setInterval(() => {
      const now = Date.now();
      setReactions((prev) => {
        const filtered = prev.filter((r) => now - r.startTime < 10000); // 10 seconds
        if (filtered.length !== prev.length) {
          console.log("🧹 Cleaned up", prev.length - filtered.length, "old reactions");
        }
        return filtered;
      });
    }, 1000);

    return () => clearInterval(cleanupInterval);
  }, []);

  if (reactions.length === 0) {
    return null;
  }

  console.log("🎨 RENDERING", reactions.length, "reactions in overlay");

  return (
    <Box
      sx={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        pointerEvents: 'none',
        zIndex: 99999,
        overflow: 'hidden', // ✅ Hide reactions when they go off-screen
      }}
    >
      {reactions.map((reaction) => (
        <Box
          key={reaction.id}
          id={reaction.id}
          sx={{
            position: 'absolute',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 100000,
            pointerEvents: 'none',
            willChange: 'transform, bottom',
            // ✅ Initial position: BOTTOM-RIGHT
            bottom: '5%', // Start from bottom
            left: `${reaction.x}%`, // Right side (75-85%)
          }}
        >
          {/* Noto Emoji */}
          <Box
            sx={{
              width: '40px',
              height: '40px',
              marginBottom: '10px',
              filter: 'drop-shadow(0 6px 12px rgba(0,0,0,0.7))',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <img
              src={`https://fonts.gstatic.com/s/e/notoemoji/latest/${getEmojiUnicode(
                reaction.emoji
              )}/512.gif`}
              alt={reaction.emoji}
              style={{
                width: '100%',
                height: '100%',
                objectFit: 'contain',
              }}
              onError={(e) => {
                console.warn('Noto Emoji failed, using text fallback for:', reaction.emoji);
                e.target.style.display = 'none';
                e.target.parentElement.innerHTML = `<span style="font-size: 3.5rem;">${reaction.emoji}</span>`;
              }}
            />
          </Box>

          {/* ✅ DARK Participant Name - NO opacity */}
          <Box
          >
            {reaction.participantName}
          </Box>
        </Box>
      ))}
    </Box>
  );
};

FloatingReactionsOverlay.displayName = "FloatingReactionsOverlay";

export default React.memo(FloatingReactionsOverlay);