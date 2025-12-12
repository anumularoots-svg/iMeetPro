
// Sounds index file - exports all audio files used in the app
// This file provides a centralized way to import and use audio files

// Meeting Event Sounds
export { default as JoinSound } from './join.mp3';
export { default as LeaveSound } from './leave.mp3';
export { default as NotificationSound } from './notification.mp3';
export { default as MessageSound } from './message.mp3';
export { default as HandRaiseSound } from './hand-raise.mp3';
export { default as ReactionSound } from './reaction.mp3';
export { default as RecordingStartSound } from './recording-start.mp3';
export { default as RecordingStopSound } from './recording-stop.mp3';

// Usage example:
// import { JoinSound, MessageSound } from '@assets/sounds';
// 
// function playJoinSound() {
//   const audio = new Audio(JoinSound);
//   audio.play();
// }

// Audio management class
class AudioManager {
  constructor() {
    this.sounds = new Map();
    this.isMuted = false;
    this.volume = 0.7;
    this.loadSounds();
  }

  loadSounds() {
    const soundFiles = {
      join: JoinSound,
      leave: LeaveSound,
      notification: NotificationSound,
      message: MessageSound,
      handRaise: HandRaiseSound,
      reaction: ReactionSound,
      recordingStart: RecordingStartSound,
      recordingStop: RecordingStopSound,
    };

    Object.entries(soundFiles).forEach(([name, src]) => {
      const audio = new Audio(src);
      audio.preload = 'auto';
      audio.volume = this.volume;
      this.sounds.set(name, audio);
    });
  }

  play(soundName) {
    if (this.isMuted) return;
    
    const sound = this.sounds.get(soundName);
    if (sound) {
      // Reset the audio to the beginning
      sound.currentTime = 0;
      sound.play().catch(error => {
        console.warn(`Could not play sound ${soundName}:`, error);
      });
    }
  }

  setVolume(volume) {
    this.volume = Math.max(0, Math.min(1, volume));
    this.sounds.forEach(sound => {
      sound.volume = this.volume;
    });
  }

  mute() {
    this.isMuted = true;
  }

  unmute() {
    this.isMuted = false;
  }

  toggleMute() {
    this.isMuted = !this.isMuted;
    return this.isMuted;
  }

  // Preload all sounds for better performance
  preloadAll() {
    this.sounds.forEach(sound => {
      sound.load();
    });
  }
}

// Create a singleton instance
export const audioManager = new AudioManager();

// Convenient wrapper functions
export const playJoinSound = () => audioManager.play('join');
export const playLeaveSound = () => audioManager.play('leave');
export const playNotificationSound = () => audioManager.play('notification');
export const playMessageSound = () => audioManager.play('message');
export const playHandRaiseSound = () => audioManager.play('handRaise');
export const playReactionSound = () => audioManager.play('reaction');
export const playRecordingStartSound = () => audioManager.play('recordingStart');
export const playRecordingStopSound = () => audioManager.play('recordingStop');

// Sound settings hook for React components
export const useSoundSettings = () => {
  const [volume, setVolumeState] = React.useState(audioManager.volume);
  const [isMuted, setIsMutedState] = React.useState(audioManager.isMuted);

  const setVolume = (newVolume) => {
    audioManager.setVolume(newVolume);
    setVolumeState(newVolume);
  };

  const toggleMute = () => {
    const muted = audioManager.toggleMute();
    setIsMutedState(muted);
    return muted;
  };

  return {
    volume,
    isMuted,
    setVolume,
    toggleMute,
    mute: () => {
      audioManager.mute();
      setIsMutedState(true);
    },
    unmute: () => {
      audioManager.unmute();
      setIsMutedState(false);
    },
  };
};

// Audio constants
export const AUDIO_FORMATS = {
  MP3: 'audio/mpeg',
  WAV: 'audio/wav',
  OGG: 'audio/ogg',
  AAC: 'audio/aac',
};

export const SOUND_EVENTS = {
  JOIN: 'join',
  LEAVE: 'leave',
  NOTIFICATION: 'notification',
  MESSAGE: 'message',
  HAND_RAISE: 'handRaise',
  REACTION: 'reaction',
  RECORDING_START: 'recordingStart',
  RECORDING_STOP: 'recordingStop',
};

// Web Audio API integration for advanced sound processing
export class AdvancedAudioManager extends AudioManager {
  constructor() {
    super();
    this.audioContext = null;
    this.initializeAudioContext();
  }

  initializeAudioContext() {
    if ('AudioContext' in window || 'webkitAudioContext' in window) {
      this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
    }
  }

  // Play sound with 3D positioning (for spatial audio in meetings)
  play3D(soundName, x = 0, y = 0, z = 0) {
    if (!this.audioContext || this.isMuted) return;

    const sound = this.sounds.get(soundName);
    if (!sound) return;

    const source = this.audioContext.createMediaElementSource(sound);
    const panner = this.audioContext.createPanner();
    
    panner.setPosition(x, y, z);
    source.connect(panner);
    panner.connect(this.audioContext.destination);
    
    sound.currentTime = 0;
    sound.play().catch(error => {
      console.warn(`Could not play 3D sound ${soundName}:`, error);
    });
  }

  // Add reverb effect for meeting room acoustics
  addReverb(soundName, roomSize = 0.5) {
    if (!this.audioContext) return this.play(soundName);

    const sound = this.sounds.get(soundName);
    if (!sound) return;

    const source = this.audioContext.createMediaElementSource(sound);
    const convolver = this.audioContext.createConvolver();
    const gainNode = this.audioContext.createGain();

    // Create impulse response for reverb
    const impulseBuffer = this.createImpulseResponse(roomSize);
    convolver.buffer = impulseBuffer;

    source.connect(convolver);
    convolver.connect(gainNode);
    gainNode.connect(this.audioContext.destination);
    gainNode.gain.setValueAtTime(this.volume, this.audioContext.currentTime);

    sound.currentTime = 0;
    sound.play().catch(error => {
      console.warn(`Could not play reverb sound ${soundName}:`, error);
    });
  }

  createImpulseResponse(roomSize) {
    const length = this.audioContext.sampleRate * roomSize;
    const impulse = this.audioContext.createBuffer(2, length, this.audioContext.sampleRate);

    for (let channel = 0; channel < 2; channel++) {
      const channelData = impulse.getChannelData(channel);
      for (let i = 0; i < length; i++) {
        channelData[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / length, 2);
      }
    }

    return impulse;
  }
}

// Create advanced audio manager instance
export const advancedAudioManager = new AdvancedAudioManager();

// React Hook for advanced audio features
import React from 'react';

export const useAdvancedAudio = () => {
  const [audioContext, setAudioContext] = React.useState(null);
  const [isSupported, setIsSupported] = React.useState(false);

  React.useEffect(() => {
    const checkSupport = () => {
      const supported = !!(window.AudioContext || window.webkitAudioContext);
      setIsSupported(supported);
      
      if (supported) {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        setAudioContext(ctx);
      }
    };

    checkSupport();

    return () => {
      if (audioContext && audioContext.state !== 'closed') {
        audioContext.close();
      }
    };
  }, []);

  return {
    audioContext,
    isSupported,
    play3D: advancedAudioManager.play3D.bind(advancedAudioManager),
    addReverb: advancedAudioManager.addReverb.bind(advancedAudioManager),
  };
};