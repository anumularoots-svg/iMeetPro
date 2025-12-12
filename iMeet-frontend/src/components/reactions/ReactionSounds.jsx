// src/components/reactions/ReactionSounds.jsx
import React, { useEffect, useRef } from 'react';

const ReactionSounds = ({ 
  reaction, 
  volume = 0.3, 
  enabled = true 
}) => {
  const audioRef = useRef(null);

  const soundMap = {
    '👍': '/sounds/thumbs-up.mp3',
    '👏': '/sounds/applause.mp3',
    '❤️': '/sounds/heart.mp3',
    '😂': '/sounds/laugh.mp3',
    '🎉': '/sounds/celebration.mp3',
    '🔥': '/sounds/fire.mp3',
    'default': '/sounds/reaction.mp3'
  };

  useEffect(() => {
    if (reaction && enabled) {
      const soundFile = soundMap[reaction.emoji] || soundMap.default;
      
      if (audioRef.current) {
        audioRef.current.src = soundFile;
        audioRef.current.volume = volume;
        audioRef.current.play().catch(console.error);
      }
    }
  }, [reaction, volume, enabled]);

  return (
    <audio
      ref={audioRef}
      preload="none"
      style={{ display: 'none' }}
    />
  );
};

export default ReactionSounds;