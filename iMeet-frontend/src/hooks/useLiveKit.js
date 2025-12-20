// src/hooks/useLiveKit.js - COMPLETE FIXED VERSION - ALL LINES PRESERVED + SCREEN SHARE DEBUGGING + FULL HD QUALITY
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import {
  Room,
  RoomEvent,
  Track,
  TrackPublication,
  ParticipantEvent,
  ConnectionState,
  LocalParticipant,
  RemoteParticipant,
  createLocalTracks,
  createLocalVideoTrack,
  createLocalAudioTrack,
  LocalVideoTrack,
  LocalAudioTrack,
  DataPacket_Kind,
  VideoPresets,
  ScreenSharePresets,
  RemoteTrackPublication,
} from "livekit-client";
import { throttle, debounce } from "lodash";
import { API_BASE_URL } from "../utils/constants";
import { participantsAPI, queueAPI } from "../services/api";
import { meetingsAPI } from "../services/api";

// ✅ ENHANCED PERFORMANCE CONFIGURATION FOR 50+ PARTICIPANTS WITH FULL HD SCREEN SHARE
const PERFORMANCE_CONFIG = {
  MAX_VIDEO_PARTICIPANTS: 300,
  THROTTLE_DELAYS: {
    PARTICIPANT_UPDATE: 200,
    STATE_UPDATE: 100,
    TRACK_UPDATE: 150,
  },
  CONNECTION: {
    CONNECTION_TIMEOUT: 30000,
    MAX_RETRIES: 3,
    RETRY_DELAY: 2000,
    HEARTBEAT_INTERVAL: 30000,
    RECONNECT_DELAY: 5000,
    STABILITY_CHECK_INTERVAL: 100,
    STABILITY_CHECK_COUNT: 10,
  },
  MEMORY: {
    MAX_CACHED_STREAMS: 50,
    MAX_MESSAGES: 100,
    MAX_REACTIONS: 10,
  },
  PARTICIPANT_SYNC_INTERVAL: 10000,
  VIDEO_QUALITY: {
    LOCAL: {
      width: { ideal: 640 },
      height: { ideal: 480 },
      frameRate: 15,
    },
    // ✅ FULL HD (1080p) REMOTE VIDEO FOR VIEWERS
    REMOTE: {
      width: { ideal: 1920 }, // Full HD 1080p
      height: { ideal: 1080 }, // Full HD 1080p
      frameRate: 60, // 60 FPS for smooth viewing
    },
    // ✅ FULL HD SCREEN SHARE SPECIFIC QUALITY (1920x1080 @ 60fps)
    SCREEN_SHARE: {
      width: { ideal: 1920 }, // Full HD 1920px width
      height: { ideal: 1080 }, // Full HD 1080px height
      frameRate: 60, // 60 FPS for smooth motion
      codec: "h264", // H264 for better compatibility
    },
  },
};

// ✅ CRITICAL FIX: Ensure this is a proper React hook with stable structure
const useLiveKit = (meetingEndedProp = false) => {
  // FIXED: Initialize all state with proper default values to prevent queue issues
  const [room, setRoom] = useState(null);
  const [connectionState, setConnectionState] = useState("disconnected");
  const [participants, setParticipants] = useState([]);
  const [localParticipant, setLocalParticipant] = useState(null);
  const [remoteParticipants, setRemoteParticipants] = useState(new Map());

  // Connection state
  const [isConnected, setIsConnected] = useState(false);
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState(null);

  // Media State - Start with audio/video OFF
  const [localTracks, setLocalTracks] = useState({
    audio: null,
    video: null,
    screenShare: null,
  });
  const [isAudioEnabled, setIsAudioEnabled] = useState(false);
  const [isVideoEnabled, setIsVideoEnabled] = useState(false);
  const [isScreenSharing, setIsScreenSharing] = useState(false);
  const [isSpeaking, setIsSpeaking] = useState(false);
  const [isSpeakerMuted, setIsSpeakerMuted] = useState(false);

  // Screen share tracking
  const [screenSharingParticipant, setScreenSharingParticipant] =
    useState(null);
  const [localIsScreenSharing, setLocalIsScreenSharing] = useState(false);
  const [screenShareTrack, setScreenShareTrack] = useState(null);

  // Screen Share Permission System
  const [screenSharePermissions, setScreenSharePermissions] = useState({
    requiresHostApproval: true,
    hasPermission: false,
    pendingRequest: false,
    requestId: null,
    hostUserId: null,
  });
  const [screenShareRequests, setScreenShareRequests] = useState([]);
  const [currentScreenShareRequest, setCurrentScreenShareRequest] =
    useState(null);

  // Chat and Data
  const [messages, setMessages] = useState([]);
  const [reactions, setReactions] = useState([]);

  // ==========================================================================
  // STATE FOR REMOTE PARTICIPANTS TRACKING
  // ==========================================================================
  const [remoteParticipantsState, setRemoteParticipantsState] = useState(
    new Map()
  );
  const remoteParticipantsRef = useRef(new Map());

  // Meeting Info
  const [meetingInfo, setMeetingInfo] = useState(null);
  const [roomMetadata, setRoomMetadata] = useState({});

  // Queue management state
  const [queueStatus, setQueueStatus] = useState(null);
  const [participantCount, setParticipantCount] = useState(0);
  const [maxParticipants, setMaxParticipants] = useState(50);
  const [performanceMode, setPerformanceMode] = useState("standard");

  // Meeting control state
  const [meetingEnded, setMeetingEnded] = useState(false);

  // ✅ SCREEN SHARE STATE TRACKING - CRITICAL FOR LATE JOINER FIX
  const [screenShareCheckComplete, setScreenShareCheckComplete] =
    useState(false);
  const [lastScreenShareCheckTime, setLastScreenShareCheckTime] = useState(0);

  // FIXED: Initialize all refs properly to prevent undefined errors
  const roomRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const heartbeatIntervalRef = useRef(null);
  const connectionReadyRef = useRef(false);
  const isConnectedRef = useRef(false);
  const connectionAttemptRef = useRef(false);
  const connectionTimeoutRef = useRef(null);
  const mediaInitializedRef = useRef(false);
  const eventHandlersRef = useRef(new Map());
  const streamCacheRef = useRef(new Map());
  const lastUpdateTimeRef = useRef(0);
  const userIdRef = useRef(null);
  const screenShareStreamRef = useRef(null);
  const queuePollIntervalRef = useRef(null);
  const audioTrackRef = useRef(null);
  const videoTrackRef = useRef(null);
  const remoteAudioElementsRef = useRef(new Map());
  const remoteAudioTracksRef = useRef(new Map());
  const localParticipantIdentityRef = useRef(null);
  const localParticipantSidRef = useRef(null);
  const currentMeetingIdRef = useRef(null);
  const isCoHostRef = useRef(false);
  // Connection lock to prevent multiple simultaneous attempts
  const connectionLockRef = useRef(false);
  const activeConnectionRef = useRef(null);
  const [remoteVideoStreams, setRemoteVideoStreams] = useState(new Map());

  // ✅ SCREEN SHARE CHECK LOCK - PREVENT DUPLICATE CHECKS FOR LATE JOINERS
  const screenShareCheckLockRef = useRef(false);
  const screenShareCheckTimeRef = useRef(0);

  // Track mute states - START MUTED
  const audioMutedRef = useRef(true);
  const videoMutedRef = useRef(true);
  const speakerMutedRef = useRef(false);

  // Track muted participants to not play their audio
  const mutedParticipantsRef = useRef(new Set());

  // Screen share state tracking ref
  const screenShareStateRef = useRef({
    isPublishing: false,
    videoTrackPublished: false,
    audioTrackPublished: false,
    publishingPromises: new Map(),
  });

  // Permission system refs
  const isHostRef = useRef(false);
  const screenShareCallbacksRef = useRef(new Map());

  // ✅ CRITICAL FIX: Add screen share event tracking for debugging
  const screenShareDebugRef = useRef({
    publishedEvents: [],
    subscribedEvents: [],
    stateChanges: [],
    lastRemoteScreenShare: null,
    lateJoinerChecks: [],
    screenShareParticipants: new Map(),
  });

  const remoteVideoStreamsRef = useRef(new Map()); // Store remote video streams
  const audioDeviceChangeHandlerRef = useRef(null);
  const currentAudioDeviceIdRef = useRef(null);
  const isHandlingDeviceChangeRef = useRef(false);
  const lastAudioTrackIdRef = useRef(null);
  const audioTrackEndedHandlerRef = useRef(null);

  const handleAudioDeviceChange = useCallback(async () => {
    // Prevent multiple simultaneous device change handlers
    if (isHandlingDeviceChangeRef.current) {
      console.log("⏳ Already handling device change, skipping...");
      return;
    }

    const activeRoom = roomRef.current;
    if (!activeRoom?.localParticipant) {
      console.log("📱 Device change detected but no active room");
      return;
    }

    isHandlingDeviceChangeRef.current = true;
    console.log("🎧🎧🎧 AUDIO DEVICE CHANGE DETECTED - Handling...");

    const wasMutedBefore = audioMutedRef.current;
    console.log("📝 Current mute state:", wasMutedBefore ? "MUTED" : "UNMUTED");

    try {
      const devices = await navigator.mediaDevices.enumerateDevices();
      const audioInputDevices = devices.filter(d => d.kind === 'audioinput');
      
      console.log("🎤 Available audio input devices:", audioInputDevices.length);

      const defaultDevice = audioInputDevices.find(d => d.deviceId === 'default') || audioInputDevices[0];
      
      if (!defaultDevice) {
        console.warn("⚠️ No audio input devices available");
        isHandlingDeviceChangeRef.current = false;
        return;
      }

      console.log("🎧 Selected device:", defaultDevice.label || "Unknown Device");

      const currentPublication = activeRoom.localParticipant.getTrackPublication(Track.Source.Microphone);
      const currentTrack = currentPublication?.track;
      
      let needsNewTrack = false;
      let oldTrackSid = null;

      if (currentTrack?.mediaStreamTrack) {
        const mediaTrack = currentTrack.mediaStreamTrack;
        const currentSettings = mediaTrack.getSettings();
        
        console.log("🔍 Current track status:", {
          readyState: mediaTrack.readyState,
          enabled: mediaTrack.enabled,
          deviceId: currentSettings.deviceId?.substring(0, 8) + "...",
        });
        
        // Check if track needs replacement
        if (mediaTrack.readyState === 'ended') {
          console.log("🚨 Track ENDED - device was disconnected!");
          needsNewTrack = true;
          oldTrackSid = currentTrack.sid;
        } else if (currentSettings.deviceId !== currentAudioDeviceIdRef.current && currentAudioDeviceIdRef.current !== null) {
          console.log("🔄 Device ID changed - need new track");
          needsNewTrack = true;
          oldTrackSid = currentTrack.sid;
        }
      } else {
        console.log("📢 No current audio track - need to create one");
        needsNewTrack = true;
      }

      if (!needsNewTrack) {
        console.log("✅ Audio track is healthy, no action needed");
        isHandlingDeviceChangeRef.current = false;
        return;
      }

      // ========== STEP 1: UNPUBLISH OLD TRACK ==========
      console.log("🔴 STEP 1: Unpublishing old audio track...");
      
      if (currentPublication?.track) {
        try {
          // Remove ended event listener
          if (currentTrack?.mediaStreamTrack && audioTrackEndedHandlerRef.current) {
            currentTrack.mediaStreamTrack.removeEventListener('ended', audioTrackEndedHandlerRef.current);
            audioTrackEndedHandlerRef.current = null;
          }
          
          // Unpublish from LiveKit server
          await activeRoom.localParticipant.unpublishTrack(currentPublication.track);
          console.log("✅ Old track unpublished from LiveKit");
          
          // Stop the track
          currentPublication.track.stop();
          console.log("✅ Old track stopped");
          
        } catch (unpubError) {
          console.warn("⚠️ Error unpublishing old track:", unpubError.message);
        }
      }

      // ✅ CRITICAL: Wait for LiveKit to process the unpublish
      console.log("⏳ Waiting for LiveKit to process unpublish...");
      await new Promise(resolve => setTimeout(resolve, 800));

      // ========== STEP 2: CREATE NEW TRACK ==========
      console.log("🟢 STEP 2: Creating new audio track...");
      
      let newAudioTrack;
      try {
        newAudioTrack = await createLocalAudioTrack({
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
          channelCount: 2,
          sampleRate: 48000,
        });
      } catch (createError) {
        console.error("❌ Failed to create audio track:", createError);
        isHandlingDeviceChangeRef.current = false;
        return;
      }

      const newSettings = newAudioTrack.mediaStreamTrack.getSettings();
      currentAudioDeviceIdRef.current = newSettings.deviceId;
      
      console.log("✅ New audio track created:", {
        deviceId: newSettings.deviceId?.substring(0, 8) + "...",
        sampleRate: newSettings.sampleRate,
      });

      // ========== STEP 3: SET UP ENDED HANDLER ==========
      const endedHandler = () => {
        console.log("🚨 New audio track ENDED event!");
        setTimeout(() => {
          isHandlingDeviceChangeRef.current = false;
          handleAudioDeviceChange();
        }, 500);
      };
      audioTrackEndedHandlerRef.current = endedHandler;
      newAudioTrack.mediaStreamTrack.addEventListener('ended', endedHandler);

      // ========== STEP 4: PUBLISH NEW TRACK ==========
      console.log("🟡 STEP 3: Publishing new audio track to LiveKit...");
      
      try {
        await activeRoom.localParticipant.publishTrack(newAudioTrack, {
          name: "microphone",
          source: Track.Source.Microphone,
          dtx: false,           // ✅ CRITICAL: No DTX for recording
          red: true,
          simulcast: false,
          priority: "high",
          stopTrackOnMute: false,  // ✅ CRITICAL: Keep track for recording
        });
        
        console.log("✅ New audio track PUBLISHED to LiveKit");
        console.log("📢 Recording bot should now receive new track!");
        
      } catch (publishError) {
        console.error("❌ Failed to publish new audio track:", publishError);
        newAudioTrack.stop();
        isHandlingDeviceChangeRef.current = false;
        return;
      }

      // Store reference
      audioTrackRef.current = newAudioTrack;
      lastAudioTrackIdRef.current = newAudioTrack.sid;

      // ========== STEP 5: RESTORE MUTE STATE ==========
      console.log("🔵 STEP 4: Restoring mute state...");
      
      if (wasMutedBefore) {
        // User was muted - keep them muted
        console.log("🔇 Restoring MUTED state...");
        await newAudioTrack.mute();
        if (newAudioTrack.mediaStreamTrack) {
          newAudioTrack.mediaStreamTrack.enabled = false;
        }
        audioMutedRef.current = true;
        setIsAudioEnabled(false);
        console.log("✅ Track published but MUTED (user preference preserved)");
      } else {
        // User was unmuted - enable audio
        console.log("🔊 Restoring UNMUTED state...");
        if (newAudioTrack.isMuted) {
          await newAudioTrack.unmute();
        }
        if (newAudioTrack.mediaStreamTrack) {
          newAudioTrack.mediaStreamTrack.enabled = true;
        }
        audioMutedRef.current = false;
        setIsAudioEnabled(true);
        console.log("✅ Track published and UNMUTED - TRANSMITTING");
      }

      // ========== STEP 6: BROADCAST CHANGE ==========
      try {
        const encoder = new TextEncoder();
        const deviceChangeData = encoder.encode(
          JSON.stringify({
            type: "audio_device_changed",
            user_id: userIdRef.current,
            participant_identity: activeRoom.localParticipant.identity,
            participant_sid: activeRoom.localParticipant.sid,
            old_track_sid: oldTrackSid,
            new_track_sid: newAudioTrack.sid,
            device_label: defaultDevice.label || "Unknown",
            is_muted: wasMutedBefore,
            timestamp: Date.now(),
          })
        );

        await activeRoom.localParticipant.publishData(deviceChangeData, DataPacket_Kind.RELIABLE);
        console.log("📢 Broadcasted device change to all participants");
      } catch (broadcastErr) {
        console.warn("⚠️ Broadcast failed:", broadcastErr.message);
      }

      // ========== STEP 7: VERIFY ==========
      setTimeout(() => {
        const verifyPub = activeRoom.localParticipant?.getTrackPublication(Track.Source.Microphone);
        console.log("🔍 VERIFICATION after 1s:", {
          hasPublication: !!verifyPub,
          hasTrack: !!verifyPub?.track,
          trackSid: verifyPub?.track?.sid,
          isMuted: verifyPub?.track?.isMuted,
          mediaTrackState: verifyPub?.track?.mediaStreamTrack?.readyState,
          mediaTrackEnabled: verifyPub?.track?.mediaStreamTrack?.enabled,
        });
        
        if (!verifyPub?.track || verifyPub.track.mediaStreamTrack?.readyState !== 'live') {
          console.error("❌ VERIFICATION FAILED - Track not properly published!");
        } else {
          console.log("✅ VERIFICATION PASSED - Track is live and published");
        }
      }, 1000);

      if (window.showNotificationMessage) {
        window.showNotificationMessage(
          `Audio switched to: ${defaultDevice.label || "New Device"}`,
          "info"
        );
      }

      console.log("✅✅✅ AUDIO DEVICE CHANGE COMPLETE ✅✅✅");

    } catch (error) {
      console.error("❌ Audio device change failed:", error);
      
      if (window.showNotificationMessage) {
        window.showNotificationMessage("Audio device change failed", "warning");
      }
    } finally {
      isHandlingDeviceChangeRef.current = false;
    }
  }, []);

const setupAudioDeviceListeners = useCallback(() => {
  if (audioDeviceChangeHandlerRef.current) {
    try {
      navigator.mediaDevices.removeEventListener('devicechange', audioDeviceChangeHandlerRef.current);
    } catch (e) {}
  }

  let debounceTimer = null;
  const debouncedHandler = () => {
    console.log("📱 Device change event detected...");
    
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }
    
    debounceTimer = setTimeout(() => {
      console.log("📱 Device change event (debounced) - triggering handler");
      handleAudioDeviceChange();
    }, 500);
  };

  audioDeviceChangeHandlerRef.current = debouncedHandler;

  if (navigator.mediaDevices) {
    navigator.mediaDevices.addEventListener('devicechange', debouncedHandler);
    console.log("✅ Audio device change listener registered");
  }

  return () => {
    if (audioDeviceChangeHandlerRef.current) {
      try {
        navigator.mediaDevices.removeEventListener('devicechange', audioDeviceChangeHandlerRef.current);
      } catch (e) {}
      audioDeviceChangeHandlerRef.current = null;
    }
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }
  };
}, [handleAudioDeviceChange]);

// 3. THIRD - monitorAudioTrackHealth (depends on handleAudioDeviceChange)
const monitorAudioTrackHealth = useCallback(() => {
  const activeRoom = roomRef.current;
  if (!activeRoom?.localParticipant) return;

  if (audioMutedRef.current) return;

  const audioPublication = activeRoom.localParticipant.getTrackPublication(
    Track.Source.Microphone
  );

  if (!audioPublication?.track) {
    console.warn("⚠️ Audio health check: No audio track found but audio should be enabled");
    handleAudioDeviceChange();
    return;
  }

  const mediaTrack = audioPublication.track.mediaStreamTrack;
  if (!mediaTrack) return;

  if (mediaTrack.readyState === 'ended') {
    console.warn("🚨 Audio health check: Track has ENDED (device disconnected)");
    handleAudioDeviceChange();
    return;
  }

  if (mediaTrack.muted && !audioMutedRef.current) {
    console.warn("⚠️ Audio health check: Track muted at source but should be unmuted");
    handleAudioDeviceChange();
  }
}, [handleAudioDeviceChange]);


// 4. FOURTH - refreshAudioTrack (depends on handleAudioDeviceChange)
const refreshAudioTrack = useCallback(async () => {
  console.log("🔄 Manual audio track refresh requested");
  
  try {
    isHandlingDeviceChangeRef.current = false;
    currentAudioDeviceIdRef.current = null;
    
    await handleAudioDeviceChange();
    
    console.log("✅ Audio track refreshed successfully");
    return true;
  } catch (error) {
    console.error("❌ Audio track refresh failed:", error);
    return false;
  }
}, [handleAudioDeviceChange]);
 const cleanupAllAudioElements = useCallback(() => {
  console.log(
    "🧹 Cleaning up ALL audio elements (including screen share audio)..."
  );

  remoteAudioElementsRef.current.forEach((audioElement, key) => {
    if (audioElement) {
      try {
        audioElement.pause();
        audioElement.srcObject = null;
        audioElement.src = "";

        if (audioElement.parentNode) {
          audioElement.remove();
        }

        console.log(`🧹 Cleaned up audio element: ${key}`);
      } catch (e) {
        console.warn(`Error cleaning up audio element ${key}:`, e);
      }
    }
  });

  remoteAudioElementsRef.current.clear();
  remoteAudioTracksRef.current.clear();
  mutedParticipantsRef.current.clear();

  console.log("✅ All audio elements cleaned up");
}, []);

    const cleanupExistingConnection = useCallback(async () => {
    try {
      cleanupAllAudioElements();

      if (roomRef.current) {
        eventHandlersRef.current.forEach((handler, event) => {
          roomRef.current.off(event, handler);
        });
        eventHandlersRef.current.clear();

        if (roomRef.current.localParticipant) {
          const tracks = roomRef.current.localParticipant.tracks;
          if (tracks) {
            tracks.forEach((publication) => {
              if (publication.track) {
                publication.track.stop();
              }
            });
          }
        }

        if (roomRef.current.state !== ConnectionState.Disconnected) {
          await roomRef.current.disconnect(true);
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        roomRef.current = null;
        setRoom(null);
      }

      localParticipantIdentityRef.current = null;
      localParticipantSidRef.current = null;
      currentMeetingIdRef.current = null;

      if (audioTrackRef.current) {
        audioTrackRef.current.stop();
        audioTrackRef.current = null;
      }
      if (videoTrackRef.current) {
        videoTrackRef.current.stop();
        videoTrackRef.current = null;
      }

      streamCacheRef.current.clear();
      screenShareStreamRef.current = null;

      // Reset screen share state
      screenShareStateRef.current.isPublishing = false;
      screenShareStateRef.current.videoTrackPublished = false;
      screenShareStateRef.current.audioTrackPublished = false;
      screenShareStateRef.current.publishingPromises.clear();

      // Reset permission system
      screenShareCallbacksRef.current.clear();
      setScreenSharePermissions({
        requiresHostApproval: true,
        hasPermission: false,
        pendingRequest: false,
        requestId: null,
        hostUserId: null,
      });
      setScreenShareRequests([]);
      setCurrentScreenShareRequest(null);

      if (connectionTimeoutRef.current) {
        clearTimeout(connectionTimeoutRef.current);
        connectionTimeoutRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }

      // ✅ NEW: Clean up audio device listeners
      if (audioDeviceChangeHandlerRef.current) {
        try {
          navigator.mediaDevices.removeEventListener(
            "devicechange",
            audioDeviceChangeHandlerRef.current
          );
        } catch (e) {}
        audioDeviceChangeHandlerRef.current = null;
      }

      if (audioTrackEndedHandlerRef.current) {
        audioTrackEndedHandlerRef.current = null;
      }

      currentAudioDeviceIdRef.current = null;
      isHandlingDeviceChangeRef.current = false;
      lastAudioTrackIdRef.current = null;
    } catch (error) {
      console.warn("Cleanup error:", error);
    }
  }, [cleanupAllAudioElements]);


  useEffect(() => {
    if (!isConnected || !roomRef.current) return;

    console.log("🎧 Setting up audio device change monitoring...");

    // Setup device change listeners when connected
    const cleanup = setupAudioDeviceListeners();

    // Setup periodic audio health monitoring
    const healthCheckInterval = setInterval(() => {
      monitorAudioTrackHealth();
    }, 5000); // Check every 5 seconds

    return () => {
      console.log("🧹 Cleaning up audio device listeners...");
      cleanup();
      clearInterval(healthCheckInterval);
    };
  }, [isConnected, setupAudioDeviceListeners, monitorAudioTrackHealth]);


  // CRITICAL FIX: Create throttled function with useMemo to prevent recreation
  const throttledParticipantUpdate = useMemo(
    () =>
      throttle(
        (updateFn) => {
          const now = Date.now();
          if (now - lastUpdateTimeRef.current > 50) {
            lastUpdateTimeRef.current = now;
            setRemoteParticipants((prev) => {
              const newMap = new Map(prev);
              updateFn(newMap);
              return newMap;
            });
          }
        },
        PERFORMANCE_CONFIG.THROTTLE_DELAYS.PARTICIPANT_UPDATE,
        { leading: true, trailing: true }
      ),
    []
  );

  // Check if participant is local
  const isLocalParticipant = useCallback((participant) => {
    if (!participant) return false;

    const roomLocalParticipant = roomRef.current?.localParticipant;
    if (!roomLocalParticipant) return false;

    if (participant.sid === roomLocalParticipant.sid) {
      return true;
    }

    if (participant.identity === roomLocalParticipant.identity) {
      return true;
    }

    const currentUserId = userIdRef.current?.toString();
    const participantUserId = participant.identity?.split("_")[1];

    if (
      currentUserId &&
      participantUserId &&
      currentUserId === participantUserId
    ) {
      return true;
    }

    return false;
  }, []);

  // Attach remote audio track
  // FIXED: Better audio element attachment
  const attachRemoteAudioTrack = useCallback(
    (track, participant) => {
      if (isLocalParticipant(participant)) {
        return null;
      }

      // Clean up existing element first
      const existingElement = remoteAudioElementsRef.current.get(
        participant.sid
      );
      if (existingElement) {
        existingElement.pause();
        existingElement.srcObject = null;
        if (existingElement.parentNode) {
          existingElement.remove();
        }
      }

      const audioElement = track.attach();
      if (audioElement) {
        // FIXED: Ensure audio element is properly configured
        audioElement.autoplay = true;
        audioElement.playsInline = true;
        audioElement.controls = false;
        audioElement.style.display = "none";
        audioElement.volume = speakerMutedRef.current ? 0 : 1;

        // Add data attributes for identification
        audioElement.setAttribute("data-participant-sid", participant.sid);
        audioElement.setAttribute(
          "data-participant-identity",
          participant.identity
        );
        audioElement.setAttribute("data-livekit-audio", "true");

        remoteAudioElementsRef.current.set(participant.sid, audioElement);
        document.body.appendChild(audioElement);

        // FIXED: Better audio playback handling
        const playPromise = audioElement.play();
        if (playPromise !== undefined) {
          playPromise
            .then(() => {})
            .catch((err) => {
              console.warn("Audio autoplay blocked:", err);
              // Try to play with user gesture
              document.addEventListener(
                "click",
                () => {
                  audioElement.play().catch(console.warn);
                },
                { once: true }
              );
            });
        }

        return audioElement;
      }

      return null;
    },
    [isLocalParticipant]
  );

  // Remove audio for participant
  const removeParticipantAudio = useCallback((participant) => {
    const audioElement = remoteAudioElementsRef.current.get(participant.sid);
    if (audioElement) {
      audioElement.pause();
      audioElement.srcObject = null;
      if (audioElement.parentNode) {
        audioElement.remove();
      }
    }
    remoteAudioElementsRef.current.delete(participant.sid);
  }, []);

  // ==========================================================================
  // FUNCTION TO SYNC REMOTE PARTICIPANTS FROM LIVEKIT ROOM
  // ==========================================================================
  const syncRemoteParticipants = useCallback(() => {
    const room = roomRef.current;
    if (!room) {
      console.log("📹 syncRemoteParticipants: No room available");
      return;
    }

    const remoteParticipantsMap = new Map();

    // Get all remote participants from LiveKit room
    room.remoteParticipants.forEach((participant, identity) => {
      console.log("📹 Found remote participant:", {
        identity,
        name: participant.name,
        sid: participant.sid,
        isCameraEnabled: participant.isCameraEnabled,
        isMicrophoneEnabled: participant.isMicrophoneEnabled,
      });

      remoteParticipantsMap.set(identity, participant);
    });

    console.log(
      "📹 Total remote participants synced:",
      remoteParticipantsMap.size
    );

    // Update state only if changed
    if (remoteParticipantsMap.size !== remoteParticipantsRef.current.size) {
      remoteParticipantsRef.current = remoteParticipantsMap;
      setRemoteParticipantsState(new Map(remoteParticipantsMap));
    }
  }, []);
  // Handle reactions
  const handleReaction = useCallback((data) => {
    const reactionId = Date.now();
    setReactions((prev) => {
      const newReactions = [
        ...prev.slice(-PERFORMANCE_CONFIG.MEMORY.MAX_REACTIONS + 1),
        {
          id: reactionId,
          emoji: data.emoji,
          userName: data.user_name,
          userId: data.user_id,
          timestamp: data.timestamp,
        },
      ];

      setTimeout(() => {
        setReactions((current) => current.filter((r) => r.id !== reactionId));
      }, 3000);

      return newReactions;
    });
  }, []);

  const handleMeetingEnded = useCallback((data) => {
    console.log("🛑 Meeting ended - setting flag only (NO auto-refresh)");
    setMeetingEnded(true);

    // ✅ CRITICAL: Block ALL auto-refresh attempts
    window.blockAutoRefresh = true;
    sessionStorage.setItem("blockAutoRefresh", "true");
    sessionStorage.setItem("meetingEndedAt", Date.now().toString());

    console.log("🔒 Auto-refresh BLOCKED - waiting for user feedback");

    if (window.showNotificationMessage) {
      window.showNotificationMessage(
        data.message || "Meeting has been ended by the host",
        "warning"
      );
    }

    console.log("⏸️ Auto-refresh DISABLED - waiting for user feedback");
  }, []);

  const requestScreenSharePermission = useCallback(async (userId, userName) => {
    if (!roomRef.current || !isConnectedRef.current) {
      throw new Error("Not connected to room");
    }

    const requestId = `ss_req_${Date.now()}_${userId}`;

    try {
      // ✅ ENHANCED: Get the FULL user name from multiple sources
      let fullUserName = userName;

      // Try to get better name from local participant
      if (roomRef.current.localParticipant) {
        const localName = roomRef.current.localParticipant.name;
        if (
          localName &&
          !localName.includes("user_") &&
          localName !== userName
        ) {
          fullUserName = localName;
          console.log("✅ Using localParticipant name:", fullUserName);
        }
      }

      // Try to get from window.currentUser
      if (window.currentUser) {
        const currentUserName =
          window.currentUser.full_name ||
          window.currentUser.Full_Name ||
          window.currentUser.name ||
          window.currentUser.displayName;

        if (currentUserName && !currentUserName.includes("user_")) {
          fullUserName = currentUserName;
          console.log("✅ Using window.currentUser name:", fullUserName);
        }
      }

      // Try to get from allParticipants in window scope
      if (window.allParticipants) {
        const participant = window.allParticipants.find((p) => {
          const pId = (p.id || p.user_id || p.User_ID)?.toString();
          return pId === userId?.toString();
        });

        if (participant) {
          const participantName =
            participant.full_name ||
            participant.Full_Name ||
            participant.displayName ||
            participant.name;

          if (participantName && !participantName.includes("user_")) {
            fullUserName = participantName;
            console.log("✅ Using allParticipants name:", fullUserName);
          }
        }
      }

      console.log("📤 Sending screen share request with:", {
        requestId,
        userId,
        userName: fullUserName,
        originalUserName: userName,
      });

      const encoder = new TextEncoder();
      const requestData = encoder.encode(
        JSON.stringify({
          type: "screen_share_request",
          request_id: requestId,
          user_id: userId,
          user_name: fullUserName,
          user_full_name: fullUserName,
          displayName: fullUserName,
          sender_name: fullUserName,
          timestamp: Date.now(),
          sender_id: roomRef.current.localParticipant.sid,
          sender_identity: roomRef.current.localParticipant.identity,
        })
      );

      roomRef.current.localParticipant.publishData(
        requestData,
        DataPacket_Kind.RELIABLE
      );

      setScreenSharePermissions((prev) => ({
        ...prev,
        pendingRequest: true,
        requestId: requestId,
      }));

      return requestId;
    } catch (error) {
      console.error("Failed to request screen share permission:", error);
      throw error;
    }
  }, []);

  const approveScreenShareRequest = useCallback(async (requestId, userId) => {
    if (!isHostRef.current) {
      throw new Error("Only hosts can approve screen share requests");
    }

    try {
      const encoder = new TextEncoder();
      const approvalData = encoder.encode(
        JSON.stringify({
          type: "screen_share_approval",
          request_id: requestId,
          user_id: userId,
          approved: true,
          timestamp: Date.now(),
          sender_id: roomRef.current.localParticipant.sid,
        })
      );

      roomRef.current.localParticipant.publishData(
        approvalData,
        DataPacket_Kind.RELIABLE
      );

      setScreenShareRequests((prev) =>
        prev.filter((req) => req.request_id !== requestId)
      );

      return true;
    } catch (error) {
      console.error("Failed to approve screen share request:", error);
      throw error;
    }
  }, []);

  const denyScreenShareRequest = useCallback(async (requestId, userId) => {
    if (!isHostRef.current) {
      throw new Error("Only hosts can deny screen share requests");
    }

    try {
      const encoder = new TextEncoder();
      const denialData = encoder.encode(
        JSON.stringify({
          type: "screen_share_approval",
          request_id: requestId,
          user_id: userId,
          approved: false,
          timestamp: Date.now(),
          sender_id: roomRef.current.localParticipant.sid,
        })
      );

      roomRef.current.localParticipant.publishData(
        denialData,
        DataPacket_Kind.RELIABLE
      );

      setScreenShareRequests((prev) =>
        prev.filter((req) => req.request_id !== requestId)
      );

      return true;
    } catch (error) {
      console.error("Failed to deny screen share request:", error);
      throw error;
    }
  }, []);

  const handleScreenShareRequest = useCallback((data) => {
    if (isHostRef.current) {
      console.log("📥 Raw screen share request data:", data);

      // ✅ ENHANCED: Multiple fallback strategies for getting the user's name
      let cleanUserName =
        data.user_name || data.user_full_name || data.sender_name || null;

      // Strategy 1: Check if name contains LiveKit identity format
      if (cleanUserName && cleanUserName.includes("user_")) {
        cleanUserName = null; // Reset to try other methods
      }

      // Strategy 2: Look up from remoteParticipants by sender_id or identity
      if (!cleanUserName || cleanUserName === `User ${data.user_id}`) {
        const participant = Array.from(
          roomRef.current?.remoteParticipants || []
        ).find(([_, p]) => {
          const pIdentity = p.identity;
          const pUserId = pIdentity?.includes("user_")
            ? pIdentity.split("_")[1]
            : pIdentity;

          return (
            p.identity === data.sender_id ||
            p.sid === data.sender_id ||
            pUserId === data.user_id?.toString()
          );
        })?.[1];

        if (participant?.name && !participant.name.includes("user_")) {
          cleanUserName = participant.name;
          console.log("✅ Found name from remoteParticipants:", cleanUserName);
        }
      }

      // Strategy 3: Look up from liveParticipants array (from backend)
      if (!cleanUserName || cleanUserName === `User ${data.user_id}`) {
        // Access liveParticipants from window scope or parent component
        const liveParticipants = window.liveParticipants || [];
        const participant = liveParticipants.find((p) => {
          const pUserId = (p.User_ID || p.user_id || p.ID)?.toString();
          return pUserId === data.user_id?.toString();
        });

        if (participant) {
          cleanUserName =
            participant.Full_Name || participant.full_name || participant.name;
          console.log("✅ Found name from liveParticipants:", cleanUserName);
        }
      }

      // Final fallback
      if (!cleanUserName) {
        cleanUserName = `User ${data.user_id}`;
      }

      console.log("📝 Final screen share request details:", {
        receivedUserName: data.user_name,
        receivedFullName: data.user_full_name,
        cleanUserName: cleanUserName,
        userId: data.user_id,
        senderId: data.sender_id,
      });

      const requestObject = {
        request_id: data.request_id,
        user_id: data.user_id,
        user_name: cleanUserName,
        user_full_name: cleanUserName,
        displayName: cleanUserName,
        timestamp: data.timestamp,
        sender_id: data.sender_id,
        sender_name: cleanUserName,
      };

      setScreenShareRequests((prev) => [...prev, requestObject]);
      setCurrentScreenShareRequest(requestObject);
    }
  }, []);

  const handleScreenShareApproval = useCallback((data) => {
    const currentUserId = userIdRef.current?.toString();
    if (data.user_id?.toString() === currentUserId) {
      if (data.approved) {
        setScreenSharePermissions((prev) => ({
          ...prev,
          hasPermission: true,
          pendingRequest: false,
          requestId: null,
        }));

        const callback = screenShareCallbacksRef.current.get(data.request_id);
        if (callback) {
          callback.resolve(true);
          screenShareCallbacksRef.current.delete(data.request_id);
        }
      } else {
        setScreenSharePermissions((prev) => ({
          ...prev,
          hasPermission: false,
          pendingRequest: false,
          requestId: null,
        }));

        const callback = screenShareCallbacksRef.current.get(data.request_id);
        if (callback) {
          callback.reject(new Error("Screen share request was denied by host"));
          screenShareCallbacksRef.current.delete(data.request_id);
        }
      }
    }

    setScreenShareRequests((prev) =>
      prev.filter((req) => req.request_id !== data.request_id)
    );
    setCurrentScreenShareRequest(null);
  }, []);

  // Participant removal handler
  const handleParticipantRemoved = useCallback(
    (data) => {
      const currentUserId = userIdRef.current?.toString();

      if (data.target_user_id?.toString() === currentUserId) {
        if (window.showNotificationMessage) {
          window.showNotificationMessage(
            data.message ||
              "You have been removed from the meeting by the host",
            "error"
          );
        }

        // Set meeting ended state
        setMeetingEnded(true);

        // AUTO-REFRESH AFTER REMOVAL
        setTimeout(async () => {
          try {
            // Clean up everything
            cleanupAllAudioElements();

            // Disconnect from room
            if (roomRef.current) {
              await roomRef.current.disconnect();
            }

            // REFRESH THE ENTIRE APPLICATION
            window.location.reload();
          } catch (error) {
            console.error("Error during forced disconnect:", error);
            // Force refresh even if cleanup fails
            window.location.reload();
          }
        }, 2000);
      }
    },
    [cleanupAllAudioElements]
  );

  // CRITICAL FIX: Handle disconnection properly
  const handleDisconnection = useCallback(
    (reason) => {
      // Check if this is a forced disconnection (user was removed)
      if (connectionAttemptRef.current || connectionLockRef.current) {
        console.log("⏳ Ignoring disconnect - connection in progress");
        return;
      }
      const isForcedDisconnect =
        reason === "KICKED" ||
        reason === "REMOVED" ||
        reason === 2 ||
        meetingEnded;

      if (isForcedDisconnect) {
        // Don't attempt reconnection for forced disconnects
        setMeetingEnded(true);
      }

      setIsConnected(false);
      isConnectedRef.current = false;
      connectionReadyRef.current = false;
      setConnectionState("disconnected");

      // Clear media states
      setIsScreenSharing(false);
      setLocalIsScreenSharing(false);
      setScreenSharingParticipant(null);
      setScreenShareTrack(null);
      screenShareStreamRef.current = null;

      // Clear track references
      audioTrackRef.current = null;
      videoTrackRef.current = null;
      localParticipantSidRef.current = null;

      // Clear connection lock
      connectionLockRef.current = false;
      activeConnectionRef.current = null;

      // Clear screen share state
      screenShareStateRef.current.isPublishing = false;
      screenShareStateRef.current.videoTrackPublished = false;
      screenShareStateRef.current.audioTrackPublished = false;
      screenShareStateRef.current.publishingPromises.clear();

      // Reset screen share permissions
      setScreenSharePermissions({
        requiresHostApproval: true,
        hasPermission: false,
        pendingRequest: false,
        requestId: null,
        hostUserId: null,
      });

      // Clean up all audio elements
      cleanupAllAudioElements();

      // Enhanced reconnection logic - but NOT for forced disconnects
      // Enhanced reconnection logic - but NOT for forced disconnects
      if (
        !isForcedDisconnect &&
        reason !== "LEAVE" &&
        reason !== "USER_DISCONNECT" &&
        !meetingEnded
      ) {
        if (reconnectTimeoutRef.current === null) {
          reconnectTimeoutRef.current = setTimeout(async () => {
            reconnectTimeoutRef.current = null;

            if (!isConnectedRef.current && roomRef.current && !meetingEnded) {
              try {
                // ✅ FIX: LiveKit Room doesn't have reconnect() method
                // Instead, the room will auto-reconnect if configured properly
                // We just need to wait for the connection state to change
                console.log("🔄 Waiting for auto-reconnection...");

                // Check if room is already reconnecting
                if (roomRef.current.state === ConnectionState.Reconnecting) {
                  console.log("🔄 Room is already reconnecting...");
                  return;
                }

                // If room is disconnected, we need to reconnect manually
                if (roomRef.current.state === ConnectionState.Disconnected) {
                  console.log(
                    "🔄 Room disconnected, manual reconnection needed"
                  );
                  // Don't try to reconnect here - let the user rejoin
                  // This prevents infinite reconnection loops
                  if (window.showNotificationMessage) {
                    window.showNotificationMessage(
                      "Connection lost. Please rejoin the meeting.",
                      "warning"
                    );
                  }
                  return;
                }

                // If room reconnected successfully
                if (roomRef.current.state === ConnectionState.Connected) {
                  setIsConnected(true);
                  isConnectedRef.current = true;
                  connectionReadyRef.current = true;
                  setConnectionState("connected");
                  console.log("✅ Reconnection successful");
                }
              } catch (reconnectError) {
                console.error("Reconnection failed:", reconnectError);
                connectionAttemptRef.current = false;
              }
            }
          }, 5000);
        }
      }
    },
    [cleanupAllAudioElements, meetingEnded]
  );
// }, [cleanupAllAudioElements, meetingEnded]);

  // ✅ CRITICAL FIX: enableAudio function - MUST be defined BEFORE toggleAudio
  const enableAudio = useCallback(async () => {
    try {
      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) {
        console.warn("No active room for audio publishing");
        return false;
      }

      console.log("🎤 enableAudio called - will broadcast to ALL participants");

      const existingAudioPub = activeRoom.localParticipant.getTrackPublication(
        Track.Source.Microphone
      );

      if (existingAudioPub?.track) {
        console.log("🎤 Existing audio track found, enabling...");

        // Step 1: Update ref
        audioMutedRef.current = false;

        // Step 2: Enable MediaStreamTrack FIRST
        const mediaTrack = existingAudioPub.track.mediaStreamTrack;
        if (mediaTrack) {
          mediaTrack.enabled = true;
          console.log("✅ MediaStreamTrack ENABLED");
        }

        // Step 3: Unmute LiveKit track - BROADCASTS TO ALL
        if (existingAudioPub.track.isMuted) {
          await existingAudioPub.track.unmute();
          console.log("✅ LiveKit track UNMUTED - Broadcast to all participants");
        }

        audioTrackRef.current = existingAudioPub.track;
        setIsAudioEnabled(true);

        console.log("✅ Existing audio track enabled for ALL participants");
        return true;
      }

      // No existing track, create new one
      console.log("🎤 Creating new audio track for ALL participants...");

      const audioTrack = await createLocalAudioTrack({
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        channelCount: 2,
        sampleRate: 48000,
      });

      audioTrackRef.current = audioTrack;

      // Publish track to LiveKit
      await activeRoom.localParticipant.publishTrack(audioTrack, {
        name: "microphone",
        source: Track.Source.Microphone,
        dtx: false,
        red: true,
        simulcast: false,
        priority: "high",
        stopTrackOnMute: false,
      });

      console.log("✅ Audio track PUBLISHED to LiveKit server");

      // Ensure MediaStreamTrack is ENABLED
      const mediaTrack = audioTrack.mediaStreamTrack;
      if (mediaTrack) {
        mediaTrack.enabled = true;
      }

      // Ensure LiveKit track is NOT muted
      if (audioTrack.isMuted) {
        await audioTrack.unmute();
        console.log("✅ New audio track UNMUTED - Broadcasting to all");
      }

      setLocalTracks((prev) => ({ ...prev, audio: audioTrack }));
      setIsAudioEnabled(true);
      audioMutedRef.current = false;

      console.log("✅ New audio track created and enabled for ALL participants");
      return true;
    } catch (error) {
      console.error("❌ Audio enable/publish failed:", error);
      setError(`Audio error: ${error.message}`);
      return false;
    }
  }, []);

  // ✅ CRITICAL FIX: Log all screen share events for debugging
  // ✅ CRITICAL FIX: Log all screen share events for debugging
  const logScreenShareEvent = useCallback((eventType, details) => {
    const timestamp = new Date().toLocaleTimeString();
    console.log(`[${timestamp}] 📺 SCREEN SHARE EVENT:`, eventType, details);

    // Store in debug ref for later analysis
    if (eventType === "published") {
      screenShareDebugRef.current.publishedEvents.push({
        timestamp,
        participant: details.participant?.identity,
        source: details.source,
        isSubscribed: details.isSubscribed,
        track: !!details.track,
      });
    } else if (eventType === "subscribed") {
      screenShareDebugRef.current.subscribedEvents.push({
        timestamp,
        participant: details.participant?.identity,
        track: !!details.track,
      });
    } else if (eventType === "state_change") {
      screenShareDebugRef.current.stateChanges.push({
        timestamp,
        from: details.from,
        to: details.to,
        participant: details.participant?.identity,
      });
    }
  }, []);

  const createEnhancedStreamMapping = useMemo(() => {
    const streamMap = new Map();

    if (
      streamCacheRef.current.size > 0 &&
      streamCacheRef.current._timestamp &&
      Date.now() - streamCacheRef.current._timestamp < 100
    ) {
      return streamCacheRef.current;
    }

    try {
      const currentUserId = userIdRef.current;

      // ✅ CRITICAL FIX 1: Process LOCAL participant (HOST) streams FIRST
      if (localParticipant && room) {
        if (typeof localParticipant.getTrackPublication === "function") {
          const localVideoTrack = localParticipant.getTrackPublication(
            Track.Source.Camera
          );
          const localAudioTrack = localParticipant.getTrackPublication(
            Track.Source.Microphone
          );

          // ✅ Create stream from LOCAL tracks (HOST VIDEO)
          if (
            localVideoTrack?.track?.mediaStreamTrack ||
            localAudioTrack?.track?.mediaStreamTrack
          ) {
            const stream = new MediaStream();

            if (localVideoTrack?.track?.mediaStreamTrack) {
              stream.addTrack(localVideoTrack.track.mediaStreamTrack);
              console.log("✅ Added LOCAL (HOST) video track to stream");
            }

            if (localAudioTrack?.track?.mediaStreamTrack) {
              stream.addTrack(localAudioTrack.track.mediaStreamTrack);
              console.log("✅ Added LOCAL (HOST) audio track to stream");
            }

            const userId = currentUserId || "local";

            // ✅ CRITICAL: Store HOST stream with ALL possible keys
            const hostKeys = [
              userId.toString(),
              `user_${userId}`,
              `participant_${userId}`,
              "local",
              "host",
              localParticipant.sid,
              localParticipant.identity,
              currentUserId?.toString(),
            ];

            hostKeys.forEach((key) => {
              if (key) {
                streamMap.set(key, stream);
              }
            });

            console.log("✅ HOST stream stored with keys:", hostKeys);
          }
        }
      }

      // ✅ CRITICAL FIX 2: Process REMOTE participants (STUDENTS) streams
      if (remoteParticipants?.size > 0) {
        remoteParticipants.forEach((participant, participantSid) => {
          try {
            if (typeof participant.getTrackPublication !== "function") {
              return;
            }

            const videoTrack = participant.getTrackPublication(
              Track.Source.Camera
            );
            const audioTrack = participant.getTrackPublication(
              Track.Source.Microphone
            );

            let userId = participant.identity;
            if (participant.identity?.includes("user_")) {
              userId = participant.identity.split("_")[1];
            }

            // Skip if this is the current user (host)
            if (userId === currentUserId?.toString()) return;

            // ✅ Create stream if ANY track exists
            if (
              videoTrack?.track?.mediaStreamTrack ||
              audioTrack?.track?.mediaStreamTrack
            ) {
              const stream = new MediaStream();

              if (videoTrack?.track?.mediaStreamTrack) {
                stream.addTrack(videoTrack.track.mediaStreamTrack);
                console.log(
                  `✅ Added REMOTE video track for: ${
                    participant.name || userId
                  }`
                );
              }

              if (audioTrack?.track?.mediaStreamTrack) {
                stream.addTrack(audioTrack.track.mediaStreamTrack);
                console.log(
                  `✅ Added REMOTE audio track for: ${
                    participant.name || userId
                  }`
                );
              }

              // ✅ Store PARTICIPANT stream with MULTIPLE keys
              const participantKeys = [
                userId.toString(),
                participantSid,
                `user_${userId}`,
                `participant_${userId}`,
                participant.identity,
              ];

              participantKeys.forEach((key) => {
                if (key) {
                  streamMap.set(key, stream);
                }
              });

              console.log(
                `✅ Stored stream for ${participant.name || userId} with keys:`,
                participantKeys
              );
            }
          } catch (error) {
            console.error("Error processing participant stream:", error);
          }
        });
      }

      streamCacheRef.current = streamMap;
      streamCacheRef.current._timestamp = Date.now();

      console.log(
        "🎥 Final streamMap size:",
        streamMap.size,
        "keys:",
        Array.from(streamMap.keys())
      );
    } catch (error) {
      console.error("Stream mapping error:", error);
    }

    return streamMap;
  }, [localParticipant, remoteParticipants, room]);

  // CRITICAL FIX: Setup event listeners with stable callbacks
  const setupOptimizedEventListeners = useCallback(
    (room) => {
      if (!room) return;

      // FIXED: Use proper throttle with stable references
      const handleConnectionStateChanged = throttle((state) => {
        setConnectionState(state);
        setIsConnected(state === ConnectionState.Connected);
        isConnectedRef.current = state === ConnectionState.Connected;
      }, 1000);

      // ============================================================================
      // HELPER FUNCTION - GET PARTICIPANT DISPLAY NAME (Regular function, not a hook)
      // ============================================================================
      const getParticipantDisplayName = (participant) => {
        if (!participant) return "Unknown User";

        // Extract user ID from identity
        let userId = participant.identity;
        if (participant.identity?.includes("user_")) {
          userId = participant.identity.split("_")[1];
        }

        // Try to get name from participant object itself
        let displayName = participant.name;

        // If name looks like "user_123" or is just an ID, look up the real name
        if (
          !displayName ||
          displayName.includes("user_") ||
          displayName === userId
        ) {
          // Try to get from window.allParticipants (populated by MeetingRoom)
          if (window.allParticipants && Array.isArray(window.allParticipants)) {
            const matchedParticipant = window.allParticipants.find((p) => {
              const pUserId = (p.id || p.user_id || p.User_ID)?.toString();
              return pUserId === userId?.toString();
            });

            if (matchedParticipant) {
              displayName =
                matchedParticipant.full_name ||
                matchedParticipant.Full_Name ||
                matchedParticipant.displayName ||
                matchedParticipant.name ||
                matchedParticipant.username ||
                matchedParticipant.user_name;
            }
          }

          // Try to get from window.liveParticipants as fallback
          if (
            (!displayName || displayName.includes("user_")) &&
            window.liveParticipants &&
            Array.isArray(window.liveParticipants)
          ) {
            const matchedParticipant = window.liveParticipants.find((p) => {
              const pUserId = (p.User_ID || p.user_id || p.ID)?.toString();
              return pUserId === userId?.toString();
            });

            if (matchedParticipant) {
              displayName =
                matchedParticipant.Full_Name ||
                matchedParticipant.full_name ||
                matchedParticipant.name ||
                matchedParticipant.displayName;
            }
          }
        }

        // Final fallback
        return displayName || participant.name || `User ${userId}`;
      };

      const handleTrackPublished = (publication, participant) => {
        if (isLocalParticipant(participant)) {
          return;
        }

        console.log("🎬 Track published:", {
          participant: participant.identity,
          participantName: participant.name,
          source: publication.source,
          kind: publication.kind,
          isSubscribed: publication.isSubscribed,
          isEnabled: publication.isEnabled,
        });

        // ✅ CRITICAL: Log all screen share published events for debugging
        if (publication.source === Track.Source.ScreenShare) {
          logScreenShareEvent("published", {
            participant,
            source: publication.source,
            isSubscribed: publication.isSubscribed,
            track: !!publication.track,
          });

          console.log(
            "📺 REMOTE screen share VIDEO published - updating state immediately"
          );

          // Set screen sharing participant IMMEDIATELY when published
          setScreenSharingParticipant(participant);
          setIsScreenSharing(true);

          // Auto-subscribe to screen share video
          if (!publication.isSubscribed && publication.isEnabled) {
            console.log("📺 Auto-subscribing to screen share VIDEO track");
            publication.setSubscribed(true);
          }
        }

        // ✅ CRITICAL FIX: Handle screen share AUDIO - AUTO SUBSCRIBE
        if (publication.source === Track.Source.ScreenShareAudio) {
          console.log(
            "🔊 REMOTE screen share AUDIO published from:",
            participant.name || participant.identity
          );

          // Auto-subscribe to screen share audio
          if (!publication.isSubscribed) {
            console.log("🔊 Auto-subscribing to screen share AUDIO track");
            publication.setSubscribed(true);
          }
        }

        // Subscribe to other tracks as well
        if (
          publication.source !== Track.Source.ScreenShare &&
          publication.source !== Track.Source.ScreenShareAudio
        ) {
          if (!publication.isSubscribed && publication.isEnabled) {
            publication.setSubscribed(true);
          }
        }
      };

      const handleDataReceived = throttle((payload, participant) => {
        try {
          const decoder = new TextDecoder();
          const data = JSON.parse(decoder.decode(payload));

          if (data.type === "chat_message") {
            setMessages((prev) => [
              ...prev.slice(-PERFORMANCE_CONFIG.MEMORY.MAX_MESSAGES + 1),
              {
                id: Date.now(),
                message: data.message,
                userName: data.user_name,
                userId: data.user_id,
                timestamp: data.timestamp,
              },
            ]);
          } else if (data.type === "reaction") {
            handleReaction(data);
          } else if (data.type === "meeting_ended") {
            handleMeetingEnded(data);
          } else if (data.type === "screen_share_request") {
            handleScreenShareRequest(data);
          } else if (data.type === "screen_share_approval") {
            handleScreenShareApproval(data);
          } else if (data.type === "participant_removed") {
            handleParticipantRemoved(data);
          }
          // ✅ EXISTING: Handle force stop for TARGET participant
          else if (data.type === "force_stop_screen_share") {
            const currentIdentity = roomRef.current?.localParticipant?.identity;

            if (
              data.target_identity === currentIdentity ||
              data.target_sid === roomRef.current?.localParticipant?.sid
            ) {
              console.log(
                "🛑 Received force stop screen share command from host/co-host"
              );

              if (window.showNotificationMessage) {
                window.showNotificationMessage(
                  `Your screen share was stopped by ${
                    data.stopped_by_name || "host"
                  }`,
                  "warning"
                );
              }

              // Force stop own screen share
              stopScreenShareInternal();

              // Also update UI state
              setIsScreenSharing(false);
              setLocalIsScreenSharing(false);
              setScreenShareTrack(null);
              setScreenSharingParticipant(null);
            }
          } else if (data.type === "audio_device_changed") {
            console.log("🎧 Remote participant changed audio device:", {
              from: data.participant_name || data.participant_identity,
              oldTrack: data.old_track_id,
              newTrack: data.new_track_id,
              device: data.device_label,
            });

            // Dispatch event for UI notification (optional)
            window.dispatchEvent(
              new CustomEvent("remoteAudioDeviceChanged", {
                detail: {
                  userId: data.user_id,
                  participantIdentity: data.participant_identity,
                  participantName: data.participant_name,
                  oldTrackId: data.old_track_id,
                  newTrackId: data.new_track_id,
                  deviceLabel: data.device_label,
                  timestamp: data.timestamp,
                },
              })
            );
          }
          // ✅ NEW: Handle global screen share stop broadcast (for ALL participants)
          else if (data.type === "screen_share_stopped_global") {
            console.log("🌐 GLOBAL screen share stop broadcast received:", {
              stoppedParticipant: data.stopped_participant_identity,
              stoppedBy: data.stopped_by_name,
              isForced: data.is_forced,
            });

            const currentIdentity = roomRef.current?.localParticipant?.identity;
            const currentUserId = userIdRef.current?.toString();

            // ✅ CRITICAL: Clear screen share state for ALL participants
            // Check if the stopped participant matches current screen sharer
            const isStoppedParticipant =
              data.stopped_participant_identity === currentIdentity ||
              data.stopped_participant_user_id?.toString() === currentUserId;

            // Update UI state for everyone
            setIsScreenSharing(false);
            setLocalIsScreenSharing(false);
            setScreenShareTrack(null);
            setScreenSharingParticipant(null);
            screenShareStreamRef.current = null;

            console.log(
              "✅ Screen share state cleared globally for all participants"
            );

            // Show notification (except for the person who stopped it - likely the host)
            if (!isStoppedParticipant && data.stopped_by !== currentIdentity) {
              if (window.showNotificationMessage) {
                window.showNotificationMessage(
                  `Screen sharing stopped by ${data.stopped_by_name || "host"}`,
                  "info"
                );
              }
            }
          }

          // ✅ Handle video state change broadcast from other participants
          else if (data.type === "participant_video_state_changed") {
            console.log("📹 Received video state change:", {
              from: data.participant_name || data.user_id,
              isVideoEnabled: data.is_video_enabled,
            });

            // Find the participant
            const participantIdentity = data.participant_identity;
            const participant =
              remoteParticipantsRef.current.get(participantIdentity);

            if (participant) {
              // Update participant in map
              remoteParticipantsRef.current.set(
                participantIdentity,
                participant
              );
              setRemoteParticipantsState(
                new Map(remoteParticipantsRef.current)
              );

              // If video enabled, try to get/create stream
              if (data.is_video_enabled) {
                const cameraPublication = participant.getTrackPublication?.(
                  Track.Source.Camera
                );
                if (cameraPublication?.track?.mediaStreamTrack) {
                  const videoStream = new MediaStream([
                    cameraPublication.track.mediaStreamTrack,
                  ]);

                  const streamKeys = [
                    participant.sid,
                    participantIdentity,
                    data.user_id,
                    `user_${data.user_id}`,
                    `participant_${data.user_id}`,
                    data.user_id?.toString(),
                  ];

                  streamKeys.forEach((key) => {
                    if (key) {
                      remoteVideoStreamsRef.current.set(key, videoStream);
                    }
                  });

                  setRemoteVideoStreams(new Map(remoteVideoStreamsRef.current));
                  console.log(
                    "✅ Updated video stream from data channel message"
                  );
                }
              }
            }

            // Dispatch event for UI
            window.dispatchEvent(
              new CustomEvent("remoteParticipantVideoStateChanged", {
                detail: {
                  userId: data.user_id,
                  participantIdentity: data.participant_identity,
                  participantSid: data.participant_sid,
                  participantName: data.participant_name,
                  isVideoEnabled: data.is_video_enabled,
                  timestamp: data.timestamp,
                },
              })
            );

            // Dispatch specific enable/disable event
            if (data.is_video_enabled) {
              window.dispatchEvent(
                new CustomEvent("remoteParticipantVideoEnabled", {
                  detail: {
                    userId: data.user_id,
                    participantIdentity: data.participant_identity,
                    participantSid: data.participant_sid,
                    participantName: data.participant_name,
                    isCameraEnabled: true,
                    timestamp: data.timestamp,
                  },
                })
              );
            } else {
              window.dispatchEvent(
                new CustomEvent("remoteParticipantVideoDisabled", {
                  detail: {
                    userId: data.user_id,
                    participantIdentity: data.participant_identity,
                    participantSid: data.participant_sid,
                    participantName: data.participant_name,
                    isCameraEnabled: false,
                    timestamp: data.timestamp,
                  },
                })
              );
            }
          }
        } catch (error) {
          console.error("Data parse error:", error);
        }
      }, 50);

      // ==========================================================================
      // PARTICIPANT CONNECTED - When a new participant joins
      // ==========================================================================
      const handleParticipantConnected = (participant) => {
        console.log("👤 Participant CONNECTED:", {
          identity: participant.identity,
          name: participant.name,
          sid: participant.sid,
        });

        // Add to remote participants map
        remoteParticipantsRef.current.set(participant.identity, participant);
        setRemoteParticipantsState(new Map(remoteParticipantsRef.current));

        // Dispatch event for MeetingRoom
        window.dispatchEvent(
          new CustomEvent("participantConnected", {
            detail: {
              identity: participant.identity,
              name: participant.name,
              sid: participant.sid,
            },
          })
        );
      };

      // ==========================================================================
      // PARTICIPANT DISCONNECTED - When a participant leaves
      // ==========================================================================
      const handleParticipantDisconnected = (participant) => {
        console.log("👤 Participant DISCONNECTED:", {
          identity: participant.identity,
          name: participant.name,
          sid: participant.sid,
        });

        // Remove from remote participants map
        remoteParticipantsRef.current.delete(participant.identity);
        setRemoteParticipantsState(new Map(remoteParticipantsRef.current));

        // Dispatch event for MeetingRoom
        window.dispatchEvent(
          new CustomEvent("participantDisconnected", {
            detail: {
              identity: participant.identity,
              name: participant.name,
              sid: participant.sid,
            },
          })
        );
      };

      // ==========================================================================
      // TRACK SUBSCRIBED - When we receive a remote track (CRITICAL FOR VIDEO)
      // ==========================================================================

      const handleTrackSubscribed = (track, publication, participant) => {
        // Skip local participant
        if (participant.identity === localParticipantIdentityRef.current)
          return;
        if (participant.sid === roomRef.current?.localParticipant?.sid) return;

        console.log("📹 Track SUBSCRIBED:", {
          participant: participant.name || participant.identity,
          kind: track.kind,
          source: publication.source,
          sid: participant.sid,
          isCameraEnabled: participant.isCameraEnabled,
        });

        // ✅ CRITICAL: Update remote participants map
        remoteParticipantsRef.current.set(participant.identity, participant);
        setRemoteParticipantsState(new Map(remoteParticipantsRef.current));

        // Extract user ID
        let userId = participant.identity;
        if (participant.identity?.includes("user_")) {
          userId = participant.identity.split("_")[1];
        }

        // ========== Handle VIDEO tracks (Camera) ==========
        if (
          track.kind === Track.Kind.Video &&
          publication.source === Track.Source.Camera
        ) {
          console.log(
            "📹✅ VIDEO track subscribed for:",
            participant.name || participant.identity
          );

          // ✅ CRITICAL: Create MediaStream from the track
          if (track.mediaStreamTrack) {
            const videoStream = new MediaStream([track.mediaStreamTrack]);

            console.log("✅ Created video stream for participant:", {
              name: participant.name,
              identity: participant.identity,
              userId,
              streamId: videoStream.id,
              trackState: track.mediaStreamTrack.readyState,
              trackEnabled: track.mediaStreamTrack.enabled,
            });

            // ✅ CRITICAL: Store stream with MULTIPLE keys for reliable lookup
            const streamKeys = [
              participant.sid,
              participant.identity,
              userId,
              `user_${userId}`,
              `participant_${userId}`,
              userId?.toString(),
            ];

            streamKeys.forEach((key) => {
              if (key) {
                remoteVideoStreamsRef.current.set(key, videoStream);
              }
            });

            // ✅ Update state to trigger re-render
            setRemoteVideoStreams(new Map(remoteVideoStreamsRef.current));

            console.log("✅ Stored remote video stream with keys:", streamKeys);
            console.log(
              "📹 Total remote video streams:",
              remoteVideoStreamsRef.current.size
            );

            // ✅ Dispatch event for UI update
            window.dispatchEvent(
              new CustomEvent("remoteVideoTrackSubscribed", {
                detail: {
                  participantIdentity: participant.identity,
                  participantSid: participant.sid,
                  participantName: participant.name,
                  userId,
                  track: track,
                  stream: videoStream,
                  hasVideo: true,
                  isCameraEnabled: participant.isCameraEnabled,
                  timestamp: Date.now(),
                },
              })
            );
          } else {
            console.warn(
              "⚠️ Video track has no mediaStreamTrack:",
              participant.name
            );
          }
        }

        // ========== Handle MICROPHONE AUDIO tracks ==========
        if (
          track.kind === Track.Kind.Audio &&
          publication.source === Track.Source.Microphone
        ) {
          console.log(
            "🔊 MICROPHONE AUDIO track subscribed for:",
            participant.name || participant.identity
          );

          remoteAudioTracksRef.current.set(participant.sid, track);

          if (!mutedParticipantsRef.current.has(participant.sid)) {
            attachRemoteAudioTrack(track, participant);
          }
        }

        // ========== Handle SCREEN SHARE VIDEO tracks ==========
        if (
          publication.source === Track.Source.ScreenShare &&
          track.kind === Track.Kind.Video
        ) {
          console.log(
            "📺 SCREEN SHARE VIDEO track subscribed for:",
            participant.name || participant.identity
          );

          setScreenSharingParticipant(participant);
          setIsScreenSharing(true);

          if (track.mediaStreamTrack) {
            const screenStream = new MediaStream([track.mediaStreamTrack]);
            screenShareStreamRef.current = screenStream;
            setScreenShareTrack(track);

            console.log("✅ Screen share video stream created:", {
              streamId: screenStream.id,
              trackState: track.mediaStreamTrack.readyState,
            });
          }
        }

        // ========== Handle SCREEN SHARE AUDIO tracks - CRITICAL FOR TAB/SYSTEM AUDIO ==========
        if (publication.source === Track.Source.ScreenShareAudio) {
          console.log(
            "🔊🔊🔊 SCREEN SHARE AUDIO track subscribed for:",
            participant.name || participant.identity
          );
          console.log("🔊 Track details:", {
            kind: track.kind,
            mediaStreamTrack: !!track.mediaStreamTrack,
            trackState: track.mediaStreamTrack?.readyState,
            trackEnabled: track.mediaStreamTrack?.enabled,
          });

          // ✅ CRITICAL: Attach audio track to play system/tab audio
          try {
            // Method 1: Use LiveKit's attach() method
            const audioElement = track.attach();

            if (audioElement) {
              // Configure audio element
              audioElement.autoplay = true;
              audioElement.playsInline = true;
              audioElement.controls = false;
              audioElement.muted = false; // ✅ CRITICAL: Make sure it's NOT muted
              audioElement.volume = speakerMutedRef.current ? 0 : 1.0;
              audioElement.style.display = "none";

              // Add data attributes for identification
              audioElement.setAttribute(
                "data-participant-sid",
                participant.sid
              );
              audioElement.setAttribute(
                "data-track-source",
                "screen-share-audio"
              );
              audioElement.setAttribute("data-livekit-screen-audio", "true");

              // Store reference for cleanup and volume control
              const screenAudioKey = `${participant.sid}_screen_audio`;

              // Remove any existing screen share audio element for this participant
              const existingElement =
                remoteAudioElementsRef.current.get(screenAudioKey);
              if (existingElement) {
                existingElement.pause();
                existingElement.srcObject = null;
                if (existingElement.parentNode) {
                  existingElement.remove();
                }
              }

              remoteAudioElementsRef.current.set(screenAudioKey, audioElement);

              // Append to body
              document.body.appendChild(audioElement);

              console.log(
                "🔊 Screen share audio element created and appended to DOM"
              );

              // Play the audio
              const playPromise = audioElement.play();
              if (playPromise !== undefined) {
                playPromise
                  .then(() => {
                    console.log(
                      "✅✅✅ Screen share audio PLAYING successfully for:",
                      participant.name || participant.identity
                    );
                    console.log("🔊 Audio element state:", {
                      paused: audioElement.paused,
                      muted: audioElement.muted,
                      volume: audioElement.volume,
                      readyState: audioElement.readyState,
                      currentTime: audioElement.currentTime,
                    });
                  })
                  .catch((err) => {
                    console.error(
                      "❌ Screen share audio autoplay blocked:",
                      err
                    );
                    console.log(
                      "🔊 Attempting to play on next user interaction..."
                    );

                    // Try to play with user gesture
                    const playOnInteraction = () => {
                      audioElement
                        .play()
                        .then(() => {
                          console.log(
                            "✅ Screen share audio started after user interaction"
                          );
                        })
                        .catch((e) => {
                          console.error(
                            "❌ Still cannot play screen share audio:",
                            e
                          );
                        });
                    };

                    document.addEventListener("click", playOnInteraction, {
                      once: true,
                    });
                    document.addEventListener("keydown", playOnInteraction, {
                      once: true,
                    });

                    // Also try unmuting and playing
                    audioElement.muted = false;
                    audioElement.volume = 1.0;
                  });
              }

              console.log(
                "✅ Screen share audio element attached and configured"
              );

              // Dispatch event for UI notification
              window.dispatchEvent(
                new CustomEvent("screenShareAudioStarted", {
                  detail: {
                    participantIdentity: participant.identity,
                    participantName: participant.name,
                    timestamp: Date.now(),
                  },
                })
              );
            } else {
              console.error(
                "❌ track.attach() returned null for screen share audio"
              );

              // Method 2: Fallback - create audio element manually
              if (track.mediaStreamTrack) {
                console.log(
                  "🔊 Trying fallback method with mediaStreamTrack..."
                );

                const fallbackAudioElement = document.createElement("audio");
                fallbackAudioElement.autoplay = true;
                fallbackAudioElement.playsInline = true;
                fallbackAudioElement.muted = false;
                fallbackAudioElement.volume = 1.0;
                fallbackAudioElement.style.display = "none";

                const audioStream = new MediaStream([track.mediaStreamTrack]);
                fallbackAudioElement.srcObject = audioStream;

                document.body.appendChild(fallbackAudioElement);

                const screenAudioKey = `${participant.sid}_screen_audio`;
                remoteAudioElementsRef.current.set(
                  screenAudioKey,
                  fallbackAudioElement
                );

                fallbackAudioElement
                  .play()
                  .then(() => {
                    console.log("✅ Fallback screen share audio PLAYING");
                  })
                  .catch((err) => {
                    console.error("❌ Fallback audio play failed:", err);
                  });
              }
            }
          } catch (error) {
            console.error("❌ Failed to attach screen share audio:", error);

            // Last resort fallback
            try {
              if (track.mediaStreamTrack) {
                const emergencyAudio = document.createElement("audio");
                emergencyAudio.srcObject = new MediaStream([
                  track.mediaStreamTrack,
                ]);
                emergencyAudio.autoplay = true;
                emergencyAudio.muted = false;
                emergencyAudio.volume = 1.0;
                document.body.appendChild(emergencyAudio);

                const screenAudioKey = `${participant.sid}_screen_audio`;
                remoteAudioElementsRef.current.set(
                  screenAudioKey,
                  emergencyAudio
                );

                emergencyAudio.play().catch(console.error);
                console.log("🔊 Emergency audio element created");
              }
            } catch (e) {
              console.error("❌ Emergency audio creation failed:", e);
            }
          }
        }
      };

      // ==========================================================================
      // TRACK UNSUBSCRIBED - When a remote track is removed
      // ==========================================================================
      const handleTrackUnsubscribed = (track, publication, participant) => {
        if (participant.identity === localParticipantIdentityRef.current)
          return;

        console.log("📹 Track UNSUBSCRIBED:", {
          participant: participant.name || participant.identity,
          kind: track.kind,
          source: publication.source,
        });

        // ✅ Update remote participants map
        remoteParticipantsRef.current.set(participant.identity, participant);
        setRemoteParticipantsState(new Map(remoteParticipantsRef.current));

        // Handle MICROPHONE AUDIO track removal
        if (
          track.kind === Track.Kind.Audio &&
          publication.source === Track.Source.Microphone
        ) {
          removeParticipantAudio(participant);
          remoteAudioTracksRef.current.delete(participant.sid);
        }

        // Handle VIDEO track removal
        if (
          track.kind === Track.Kind.Video &&
          publication.source === Track.Source.Camera
        ) {
          let userId = participant.identity;
          if (participant.identity?.includes("user_")) {
            userId = participant.identity.split("_")[1];
          }

          // Remove from video streams
          const keysToRemove = [
            participant.sid,
            participant.identity,
            userId,
            `user_${userId}`,
            `participant_${userId}`,
            userId?.toString(),
          ];

          keysToRemove.forEach((key) => {
            if (key) {
              remoteVideoStreamsRef.current.delete(key);
            }
          });

          setRemoteVideoStreams(new Map(remoteVideoStreamsRef.current));

          window.dispatchEvent(
            new CustomEvent("remoteVideoTrackUnsubscribed", {
              detail: {
                participantIdentity: participant.identity,
                participantSid: participant.sid,
                userId,
                timestamp: Date.now(),
              },
            })
          );
        }

        // ========== Handle SCREEN SHARE VIDEO track removal ==========
        if (publication.source === Track.Source.ScreenShare) {
          console.log(
            "📺 Screen share VIDEO track unsubscribed for:",
            participant.name || participant.identity
          );

          // Clear screen share state
          setScreenSharingParticipant(null);
          setIsScreenSharing(false);
          setScreenShareTrack(null);
          screenShareStreamRef.current = null;
        }

        // ========== Handle SCREEN SHARE AUDIO track removal ==========
        if (publication.source === Track.Source.ScreenShareAudio) {
          console.log(
            "🔊 Screen share AUDIO track unsubscribed for:",
            participant.name || participant.identity
          );

          const screenAudioKey = `${participant.sid}_screen_audio`;
          const audioElement =
            remoteAudioElementsRef.current.get(screenAudioKey);

          if (audioElement) {
            console.log("🔊 Cleaning up screen share audio element...");

            try {
              audioElement.pause();
              audioElement.srcObject = null;
              audioElement.src = "";

              if (audioElement.parentNode) {
                audioElement.remove();
              }

              // Detach from LiveKit track
              if (track && typeof track.detach === "function") {
                track.detach(audioElement);
              }
            } catch (e) {
              console.warn("Error cleaning up screen share audio:", e);
            }

            remoteAudioElementsRef.current.delete(screenAudioKey);
            console.log(
              "✅ Screen share audio cleaned up for:",
              participant.name || participant.identity
            );
          }

          // Dispatch event
          window.dispatchEvent(
            new CustomEvent("screenShareAudioStopped", {
              detail: {
                participantIdentity: participant.identity,
                participantName: participant.name,
                timestamp: Date.now(),
              },
            })
          );
        }
      };

      // ==========================================================================
      // TRACK MUTED
      // ==========================================================================
      const handleTrackMuted = (publication, participant) => {
        if (participant.identity === localParticipantIdentityRef.current)
          return;
        if (participant.sid === roomRef.current?.localParticipant?.sid) return;

        const trackKind = publication.kind;

        console.log("🔇 Remote Track MUTED:", {
          participant: participant.name || participant.identity,
          kind: trackKind,
          source: publication.source,
        });

        // ✅ Update remote participants map with new state
        remoteParticipantsRef.current.set(participant.identity, participant);
        setRemoteParticipantsState(new Map(remoteParticipantsRef.current));

        // Handle microphone audio mute
        if (
          trackKind === Track.Kind.Audio &&
          publication.source === Track.Source.Microphone
        ) {
          mutedParticipantsRef.current.add(participant.sid);
          removeParticipantAudio(participant);
        }

        // Handle screen share audio mute
        if (publication.source === Track.Source.ScreenShareAudio) {
          console.log(
            "🔇 Screen share audio MUTED for:",
            participant.name || participant.identity
          );

          const screenAudioKey = `${participant.sid}_screen_audio`;
          const audioElement =
            remoteAudioElementsRef.current.get(screenAudioKey);

          if (audioElement) {
            audioElement.volume = 0;
            audioElement.muted = true;
          }
        }

        // Dispatch event
        let userId = participant.identity;
        if (participant.identity?.includes("user_")) {
          userId = participant.identity.split("_")[1];
        }

        window.dispatchEvent(
          new CustomEvent("remoteTrackMuted", {
            detail: {
              participantIdentity: participant.identity,
              participantSid: participant.sid,
              userId,
              trackKind: trackKind === Track.Kind.Audio ? "audio" : "video",
              source: publication.source,
              participantName: participant.name,
              timestamp: Date.now(),
            },
          })
        );
      };

      // ==========================================================================
      // TRACK UNMUTED
      // ==========================================================================

      const handleTrackUnmuted = (publication, participant) => {
        if (participant.identity === localParticipantIdentityRef.current)
          return;
        if (participant.sid === roomRef.current?.localParticipant?.sid) return;

        const trackKind = publication.kind;

        console.log("🔊 Remote Track UNMUTED:", {
          participant: participant.name || participant.identity,
          kind: trackKind,
          source: publication.source,
        });

        // ✅ Update remote participants map with new state
        remoteParticipantsRef.current.set(participant.identity, participant);
        setRemoteParticipantsState(new Map(remoteParticipantsRef.current));

        // Handle microphone audio unmute
        if (
          trackKind === Track.Kind.Audio &&
          publication.source === Track.Source.Microphone
        ) {
          mutedParticipantsRef.current.delete(participant.sid);
          const track = remoteAudioTracksRef.current.get(participant.sid);
          if (track) {
            attachRemoteAudioTrack(track, participant);
          }
        }

        // Handle screen share audio unmute
        if (publication.source === Track.Source.ScreenShareAudio) {
          console.log(
            "🔊 Screen share audio UNMUTED for:",
            participant.name || participant.identity
          );

          const screenAudioKey = `${participant.sid}_screen_audio`;
          const audioElement =
            remoteAudioElementsRef.current.get(screenAudioKey);

          if (audioElement) {
            audioElement.muted = false;
            audioElement.volume = speakerMutedRef.current ? 0 : 1.0;

            // Try to play if paused
            if (audioElement.paused) {
              audioElement.play().catch(console.warn);
            }
          }
        }

        // Dispatch event
        let userId = participant.identity;
        if (participant.identity?.includes("user_")) {
          userId = participant.identity.split("_")[1];
        }

        window.dispatchEvent(
          new CustomEvent("remoteTrackUnmuted", {
            detail: {
              participantIdentity: participant.identity,
              participantSid: participant.sid,
              userId,
              trackKind: trackKind === Track.Kind.Audio ? "audio" : "video",
              source: publication.source,
              participantName: participant.name,
              timestamp: Date.now(),
            },
          })
        );
      };
      const handleDisconnected = (reason) => {
        handleDisconnection(reason);
      };

      // Store handlers in ref for cleanup
      eventHandlersRef.current.clear();
      eventHandlersRef.current.set(
        RoomEvent.ConnectionStateChanged,
        handleConnectionStateChanged
      );
      eventHandlersRef.current.set(
        RoomEvent.ParticipantConnected,
        handleParticipantConnected
      );
      eventHandlersRef.current.set(
        RoomEvent.ParticipantDisconnected,
        handleParticipantDisconnected
      );
      eventHandlersRef.current.set(
        RoomEvent.TrackSubscribed,
        handleTrackSubscribed
      );
      eventHandlersRef.current.set(
        RoomEvent.TrackUnsubscribed,
        handleTrackUnsubscribed
      );
      eventHandlersRef.current.set(
        RoomEvent.TrackPublished,
        handleTrackPublished
      );
      eventHandlersRef.current.set(RoomEvent.TrackMuted, handleTrackMuted);
      eventHandlersRef.current.set(RoomEvent.TrackUnmuted, handleTrackUnmuted);
      eventHandlersRef.current.set(RoomEvent.DataReceived, handleDataReceived);
      eventHandlersRef.current.set(RoomEvent.Disconnected, handleDisconnected);

      // Add event listeners
      room.on(RoomEvent.ConnectionStateChanged, handleConnectionStateChanged);
      room.on(RoomEvent.ParticipantConnected, handleParticipantConnected);
      room.on(RoomEvent.ParticipantDisconnected, handleParticipantDisconnected);
      room.on(RoomEvent.TrackSubscribed, handleTrackSubscribed);
      room.on(RoomEvent.TrackUnsubscribed, handleTrackUnsubscribed);
      room.on(RoomEvent.TrackPublished, handleTrackPublished);
      room.on(RoomEvent.TrackMuted, handleTrackMuted);
      room.on(RoomEvent.TrackUnmuted, handleTrackUnmuted);
      room.on(RoomEvent.DataReceived, handleDataReceived);
      room.on(RoomEvent.Disconnected, handleDisconnected);
    },
    [
      throttledParticipantUpdate,
      handleDisconnection,
      handleReaction,
      handleMeetingEnded,
      attachRemoteAudioTrack,
      removeParticipantAudio,
      isLocalParticipant,
      handleScreenShareRequest,
      handleScreenShareApproval,
      handleParticipantRemoved,
      logScreenShareEvent,
    ]
  );
  const checkExistingScreenShares = useCallback((room) => {
    if (!room) {
      console.error("❌ No room provided to checkExistingScreenShares");
      return;
    }

    // ✅ PREVENT DUPLICATE CHECKS - use lock and time check
    if (screenShareCheckLockRef.current) {
      console.log("⏸️ Screen share check already in progress");
      return;
    }

    const now = Date.now();
    if (now - screenShareCheckTimeRef.current < 2000) {
      console.log("⏸️ Screen share check throttled - ran too recently");
      return;
    }

    screenShareCheckLockRef.current = true;
    screenShareCheckTimeRef.current = now;

    try {
      console.log(
        "🔍 [COMPREHENSIVE CHECK] Checking for existing screen shares and audio..."
      );

      // Check all remote participants for screen share tracks
      room.remoteParticipants.forEach((participant) => {
        console.log(`📋 [CHECK] Participant ${participant.identity}:`, {
          trackCount: participant.trackPublications.size,
          tracks: Array.from(participant.trackPublications.values()).map(
            (pub) => ({
              source: pub.source,
              kind: pub.kind,
              isSubscribed: pub.isSubscribed,
              track: !!pub.track,
              isEnabled: pub.isEnabled,
            })
          ),
        });

        participant.trackPublications.forEach((publication) => {
          // ✅ Handle screen share VIDEO
          if (publication.source === Track.Source.ScreenShare) {
            console.log(
              "✅ [FOUND] Existing screen share VIDEO from:",
              participant.identity
            );

            // IMMEDIATELY update state for late joiner
            setScreenSharingParticipant(participant);
            setIsScreenSharing(true);
            setScreenShareCheckComplete(true);
            setLastScreenShareCheckTime(Date.now());

            // Subscribe to the track if not already subscribed
            if (!publication.isSubscribed) {
              console.log(
                "📺 [ACTION] Subscribing to screen share VIDEO track for late joiner..."
              );
              publication.setSubscribed(true);
            }

            // If track is already available, set it immediately
            if (publication.track && publication.track.mediaStreamTrack) {
              const screenStream = new MediaStream([
                publication.track.mediaStreamTrack,
              ]);
              screenShareStreamRef.current = screenStream;
              setScreenShareTrack(publication.track);
              console.log(
                "✅ [STATE] Screen share video state updated for late joiner"
              );
            }
          }

          // ✅ Handle screen share AUDIO - CRITICAL FOR LATE JOINERS
          if (publication.source === Track.Source.ScreenShareAudio) {
            console.log(
              "🔊 [FOUND] Existing screen share AUDIO from:",
              participant.identity
            );

            if (!publication.isSubscribed) {
              console.log(
                "🔊 [ACTION] Subscribing to screen share AUDIO track for late joiner..."
              );
              publication.setSubscribed(true);
            }

            // If track is already available, attach audio element
            if (publication.track) {
              console.log(
                "🔊 [ACTION] Attaching existing screen share audio for late joiner..."
              );

              try {
                const audioElement = publication.track.attach();

                if (audioElement) {
                  audioElement.autoplay = true;
                  audioElement.muted = false;
                  audioElement.volume = speakerMutedRef.current ? 0 : 1.0;
                  audioElement.style.display = "none";

                  const screenAudioKey = `${participant.sid}_screen_audio`;
                  remoteAudioElementsRef.current.set(
                    screenAudioKey,
                    audioElement
                  );

                  document.body.appendChild(audioElement);

                  audioElement
                    .play()
                    .then(() => {
                      console.log(
                        "✅ [LATE JOINER] Screen share audio PLAYING"
                      );
                    })
                    .catch((err) => {
                      console.warn(
                        "⚠️ [LATE JOINER] Screen share audio autoplay blocked:",
                        err
                      );
                    });
                }
              } catch (error) {
                console.error(
                  "❌ [LATE JOINER] Failed to attach screen share audio:",
                  error
                );
              }
            }
          }
        });
      });

      console.log("✅ [COMPLETE] Screen share check finished");
    } catch (error) {
      console.error("❌ [ERROR] Screen share check failed:", error);
    } finally {
      screenShareCheckLockRef.current = false;
    }
  }, []);
  // ✅ COMPLETE FIX: Force stop any participant's screen share (host/co-host only)
  const forceStopParticipantScreenShare = useCallback(
    async (targetParticipant) => {
      try {
        console.log(
          "🛡️ Force stopping participant's screen share:",
          targetParticipant
        );

        const activeRoom = roomRef.current;
        if (!activeRoom?.localParticipant) {
          console.error("❌ No active room");
          return false;
        }

        // Verify host/co-host permissions
        if (!isHostRef.current && !isCoHostRef.current) {
          console.error(
            "❌ PERMISSION DENIED: Only hosts/co-hosts can force stop screen shares"
          );
          if (window.showNotificationMessage) {
            window.showNotificationMessage(
              "Only hosts and co-hosts can stop other participants' screen shares",
              "error"
            );
          }
          return false;
        }

        // Find the remote participant
        let remoteParticipant = null;
        let targetIdentity = null;
        let targetUserId = null;

        // Try to find by different identifiers
        for (const [sid, participant] of activeRoom.remoteParticipants) {
          if (
            participant.sid === targetParticipant ||
            participant.identity === targetParticipant ||
            sid === targetParticipant
          ) {
            remoteParticipant = participant;
            targetIdentity = participant.identity;

            // Extract user ID from identity
            if (participant.identity?.includes("user_")) {
              targetUserId = participant.identity.split("_")[1];
            }
            break;
          }
        }

        console.log(`📺 Target participant info:`, {
          found: !!remoteParticipant,
          targetIdentity,
          targetUserId,
        });

        if (!remoteParticipant) {
          console.error("❌ Participant not found:", targetParticipant);
        }

        // ✅ CRITICAL: Send command to TARGET participant to stop their share
        const encoder = new TextEncoder();
        const stopCommand = encoder.encode(
          JSON.stringify({
            type: "force_stop_screen_share",
            target_identity: targetIdentity || targetParticipant,
            target_sid: remoteParticipant?.sid || targetParticipant,
            target_user_id: targetUserId,
            stopped_by: activeRoom.localParticipant.identity,
            stopped_by_name: activeRoom.localParticipant.name || "Host",
            is_forced: true,
            timestamp: Date.now(),
          })
        );

        await activeRoom.localParticipant.publishData(
          stopCommand,
          DataPacket_Kind.RELIABLE
        );

        console.log("✅ Force stop command sent to target participant");

        // ✅ CRITICAL FIX: Broadcast to ALL participants that screen share has stopped
        await new Promise((resolve) => setTimeout(resolve, 100)); // Small delay to ensure target receives command first

        const broadcastStopCommand = encoder.encode(
          JSON.stringify({
            type: "screen_share_stopped_global",
            stopped_participant_identity: targetIdentity || targetParticipant,
            stopped_participant_user_id: targetUserId,
            stopped_by: activeRoom.localParticipant.identity,
            stopped_by_name: activeRoom.localParticipant.name || "Host",
            is_forced: true,
            timestamp: Date.now(),
            broadcast: true, // Flag to indicate this is a global broadcast
          })
        );

        await activeRoom.localParticipant.publishData(
          broadcastStopCommand,
          DataPacket_Kind.RELIABLE
        );

        console.log(
          "✅ Global screen share stop broadcast sent to ALL participants"
        );

        // ✅ Update local UI state immediately (for host)
        setScreenSharingParticipant(null);
        setIsScreenSharing(false);
        setScreenShareTrack(null);
        screenShareStreamRef.current = null;

        if (window.showNotificationMessage) {
          window.showNotificationMessage(
            `Stopped screen share for ${
              remoteParticipant?.name || "participant"
            }`,
            "success"
          );
        }

        return true;
      } catch (error) {
        console.error("❌ Force stop screen share failed:", error);

        if (window.showNotificationMessage) {
          window.showNotificationMessage(
            `Failed to stop screen share: ${error.message}`,
            "error"
          );
        }

        return false;
      }
    },
    []
  );

  // CRITICAL FIX: Get access token without hooks in nested functions
  const getAccessTokenWithRetry = useCallback(
    async (meetingId, userId, displayName, isHost) => {
      let retryCount = 0;
      const maxRetries = PERFORMANCE_CONFIG.CONNECTION.MAX_RETRIES;

      while (retryCount < maxRetries) {
        try {
          const response = await fetch(
            `${API_BASE_URL}/api/livekit/join-meeting/`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${localStorage.getItem("token") || ""}`,
              },
              body: JSON.stringify({
                meeting_id: meetingId,
                user_id: userId,
                user_name: displayName,
                is_host: isHost || false,
                meetingId,
                userId,
                displayName,
                isHost: isHost || false,
              }),
            }
          );

          if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || `HTTP ${response.status}`);
          }

          const data = await response.json();

          if (data.success && data.access_token) {
            return {
              success: true,
              accessToken: data.access_token,
              access_token: data.access_token,
              livekit_url: data.livekit_url,
              livekitUrl: data.livekit_url,
              meeting_info: data.meeting_info,
              meetingInfo: data.meeting_info,
              participant_identity: data.participant_identity,
              participantIdentity: data.participant_identity,
              room_name: data.room_name,
            };
          } else {
            throw new Error("Invalid token response");
          }
        } catch (error) {
          retryCount++;
          console.warn(
            `Token retry ${retryCount}/${maxRetries}:`,
            error.message
          );

          if (retryCount >= maxRetries) {
            throw error;
          }

          await new Promise((resolve) =>
            setTimeout(
              resolve,
              PERFORMANCE_CONFIG.CONNECTION.RETRY_DELAY * retryCount
            )
          );
        }
      }
    },
    []
  );

  // Wait for stable connection
  const waitForStableConnection = useCallback(async (room, timeout = 15000) => {
    const startTime = Date.now();

    return new Promise((resolve, reject) => {
      const checkInterval =
        PERFORMANCE_CONFIG.CONNECTION.STABILITY_CHECK_INTERVAL;
      let lastState = room.state;
      let stableCount = 0;
      const requiredStableChecks =
        PERFORMANCE_CONFIG.CONNECTION.STABILITY_CHECK_COUNT;

      const checkConnection = () => {
        const elapsed = Date.now() - startTime;

        if (elapsed > timeout) {
          reject(new Error("Connection stability timeout"));
          return;
        }

        const currentState = room.state;

        if (
          currentState === ConnectionState.Connected &&
          room.localParticipant
        ) {
          if (currentState === lastState) {
            stableCount++;
            if (stableCount >= requiredStableChecks) {
              if (
                room.engine &&
                room.engine.client &&
                room.localParticipant.sid
              ) {
                resolve(true);
                return;
              }
            }
          } else {
            stableCount = 0;
          }
        } else {
          stableCount = 0;
        }

        lastState = currentState;
        setTimeout(checkConnection, checkInterval);
      };

      checkConnection();
    });
  }, []);

  // Participant sync
  const startParticipantSync = useCallback((meetingId) => {
    if (!meetingId) return;

    currentMeetingIdRef.current = meetingId;

    const syncParticipants = async () => {
      try {
        const response = await participantsAPI.syncParticipantsOptimized(
          meetingId
        );
      } catch (error) {
        console.warn("Participant sync failed:", error);
      }
    };

    syncParticipants();

    const syncInterval = setInterval(
      syncParticipants,
      PERFORMANCE_CONFIG.PARTICIPANT_SYNC_INTERVAL
    );

    heartbeatIntervalRef.current = syncInterval;
  }, []);

  // Heartbeat
  const startHeartbeat = useCallback(() => {
    if (heartbeatIntervalRef.current) return;

    heartbeatIntervalRef.current = setInterval(() => {
      if (roomRef.current && isConnectedRef.current) {
        try {
          const encoder = new TextEncoder();
          const data = encoder.encode(
            JSON.stringify({
              type: "heartbeat",
              timestamp: Date.now(),
            })
          );
          roomRef.current.localParticipant.publishData(
            data,
            DataPacket_Kind.RELIABLE
          );
        } catch (error) {
          console.warn("Heartbeat failed:", error);
        }
      }
    }, PERFORMANCE_CONFIG.CONNECTION.HEARTBEAT_INTERVAL);
  }, []);

  // Recording functions
  // UPDATED: Backend recording functions - remove all client-side recording logic
  const startRecording = useCallback(async (meetingId, settings = {}) => {
    try {
      if (!roomRef.current || !isConnectedRef.current) {
        throw new Error("Not connected to room");
      }

      // Call backend recording API instead of browser recording
      const response = await meetingsAPI.startMeetingRecording(meetingId, {
        recording_type: "server",
        quality: settings.quality || "hd",
        audio: settings.audio !== false,
        video: settings.video !== false,
        layout: settings.layout || "grid",
        include_system_audio: true,
        ...settings,
      });

      return { success: true, data: response };
    } catch (error) {
      console.error("Start recording failed:", error);
      throw error;
    }
  }, []);

  const stopRecording = useCallback(async (meetingId) => {
    try {
      // Call backend stop recording API
      const response = await meetingsAPI.stopMeetingRecording(meetingId);

      return { success: true, data: response };
    } catch (error) {
      console.error("Stop recording failed:", error);
      throw error;
    }
  }, []);

  // Queue management functions
  const checkConnectionQueue = useCallback(async (meetingId, userId) => {
    try {
      const queueResponse = await queueAPI.checkQueue(meetingId, userId);
      setQueueStatus(queueResponse.queue_status);
      return queueResponse;
    } catch (error) {
      console.error("Queue check failed:", error);
      throw error;
    }
  }, []);

  const joinMeetingWithQueue = useCallback(async (meetingData) => {
    try {
      const queueResponse = await queueAPI.joinWithQueue(meetingData);

      setQueueStatus(queueResponse.queue_status);

      if (queueResponse.can_proceed) {
        return { success: true, queueStatus: queueResponse.queue_status };
      } else {
        return {
          success: false,
          queueStatus: queueResponse.queue_status,
          needsToWait: true,
        };
      }
    } catch (error) {
      console.error("Queue join failed:", error);
      throw error;
    }
  }, []);

  const waitForQueueTurn = useCallback(async (meetingId, userId) => {
    try {
      const queueResponse = await queueAPI.waitForQueueTurn(meetingId, userId);
      setQueueStatus(queueResponse.queue_status);
      return queueResponse;
    } catch (error) {
      console.error("Queue wait failed:", error);
      throw error;
    }
  }, []);

  // End meeting for everyone (host only)
  const endMeetingForEveryone = useCallback(async (meetingId) => {
    try {
      if (roomRef.current && isConnectedRef.current) {
        const encoder = new TextEncoder();
        const endMeetingData = encoder.encode(
          JSON.stringify({
            type: "meeting_ended",
            message: "Meeting has been ended by the host",
            timestamp: Date.now(),
            sender_id: roomRef.current.localParticipant.sid,
          })
        );

        roomRef.current.localParticipant.publishData(
          endMeetingData,
          DataPacket_Kind.RELIABLE
        );
      }

      const response = await fetch(
        `${API_BASE_URL}/api/meetings/${meetingId}/end`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${localStorage.getItem("token") || ""}`,
          },
          body: JSON.stringify({
            reason: "host_ended",
            force_end: true,
          }),
        }
      );

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || "Failed to end meeting");
      }

      const result = await response.json();

      setMeetingEnded(true);
      return { success: true, message: "Meeting ended successfully" };
    } catch (error) {
      console.error("End meeting failed:", error);
      throw error;
    }
  }, []);

  // CRITICAL FIX: Connection establishment without hook violations
  // ==========================================================================
  // CRITICAL FIX: Connection establishment with REMOTE PARTICIPANTS SYNC
  // ==========================================================================
  const connectToRoom = useCallback(
    async (meetingId, userId, displayName, options = {}) => {
      try {
        // ✅ FIX: Check connection lock FIRST
        if (connectionLockRef.current) {
          console.log("⏳ Connection already in progress...");
          if (activeConnectionRef.current) {
            return await activeConnectionRef.current;
          }
          return { success: false, error: "Connection in progress" };
        }

        // ✅ Check if already connected to SAME meeting
        if (
          isConnectedRef.current &&
          roomRef.current?.state === ConnectionState.Connected &&
          currentMeetingIdRef.current === meetingId
        ) {
          console.log("✅ Already connected to this meeting");
          return { success: true, room: roomRef.current };
        }

        // ✅ NOW acquire lock BEFORE disconnect
        connectionLockRef.current = true;

        // ✅ NOW safe to cleanup old connection
        if (roomRef.current) {
          try {
            console.log("🔄 Cleaning up previous connection...");
            await roomRef.current.disconnect();
          } catch (e) {}
          roomRef.current = null;
          cleanupAllAudioElements();
          localParticipantSidRef.current = null;
          localParticipantIdentityRef.current = null;
          remoteParticipantsRef.current = new Map();
          setRemoteParticipantsState(new Map());
          await new Promise((resolve) => setTimeout(resolve, 300));
        }

        const connectionPromise = (async () => {
          try {
            connectionAttemptRef.current = true;
            setIsConnecting(true);
            setError(null);
            setMeetingEnded(false);
            
            if (!meetingId || !userId || !displayName) {
              throw new Error("Missing required parameters");
            }

            userIdRef.current = userId;
            currentMeetingIdRef.current = meetingId;
            isHostRef.current = options.isHost || false;
            isCoHostRef.current = options.isCoHost || false;

            // Set up permission system based on host status
            if (isHostRef.current) {
              setScreenSharePermissions((prev) => ({
                ...prev,
                requiresHostApproval: false,
                hasPermission: true,
                hostUserId: userId,
              }));
            } else {
              setScreenSharePermissions((prev) => ({
                ...prev,
                requiresHostApproval: true,
                hasPermission: false,
                hostUserId: null,
              }));
            }

            const tokenData = await getAccessTokenWithRetry(
              meetingId,
              userId,
              displayName,
              options.isHost
            );

            const newRoom = new Room({
              adaptiveStream: true,
              dynacast: true,
              publishDefaults: {
                audioPreset: "speech",
                videoCodec: "h264",
                stopMicTrackOnMute: false,
                stopVideoTrackOnMute: false,
                dtx: false,
                red: true,
                simulcast: true,
                screenShareSimulcast: true,
              },
              audioCaptureDefaults: {
                echoCancellation: true,
                noiseSuppression: true,
                autoGainControl: true,
                channelCount: 2,
                sampleRate: 48000,
              },
              videoCaptureDefaults: {
                resolution: PERFORMANCE_CONFIG.VIDEO_QUALITY.LOCAL,
              },
            });

            roomRef.current = newRoom;
            setRoom(newRoom);

            setupOptimizedEventListeners(newRoom);

            let connectionSuccess = false;
            const connectionTimeout = setTimeout(() => {
              if (!connectionSuccess) {
                console.error("Connection timeout");
                newRoom.disconnect();
                throw new Error("Connection timeout");
              }
            }, 30000);

            try {
              await newRoom.connect(
                tokenData.livekit_url || tokenData.livekitUrl,
                tokenData.access_token || tokenData.accessToken,
                {
                  autoSubscribe: true,
                  publishOnly: false,
                  reconnect: true,
                  reconnectPolicy: {
                    maxRetries: 5,
                    nextRetryDelayInMs: (retryCount) => {
                      return Math.min(5000 * Math.pow(2, retryCount), 30000);
                    },
                  },
                }
              );

              connectionSuccess = true;
              clearTimeout(connectionTimeout);

              if (newRoom.localParticipant) {
                localParticipantIdentityRef.current =
                  newRoom.localParticipant.identity;
                localParticipantSidRef.current = newRoom.localParticipant.sid;
              }

              // ==========================================================================
              // ✅ CRITICAL FIX: SYNC EXISTING REMOTE PARTICIPANTS IMMEDIATELY
              // ==========================================================================
              console.log("📹 Checking for existing remote participants...");

              if (
                newRoom.remoteParticipants &&
                newRoom.remoteParticipants.size > 0
              ) {
                console.log(
                  "📹 Found existing remote participants:",
                  newRoom.remoteParticipants.size
                );

                const initialRemoteParticipants = new Map();

                newRoom.remoteParticipants.forEach((participant, identity) => {
                  console.log("📹 Syncing remote participant:", {
                    identity,
                    name: participant.name,
                    sid: participant.sid,
                    isCameraEnabled: participant.isCameraEnabled,
                    isMicrophoneEnabled: participant.isMicrophoneEnabled,
                  });

                  initialRemoteParticipants.set(identity, participant);

                  // ✅ Check for existing video tracks and dispatch event
                  const cameraPublication = participant.getTrackPublication?.(
                    Track.Source.Camera
                  );
                  if (cameraPublication?.track && participant.isCameraEnabled) {
                    let participantUserId = identity;
                    if (identity?.includes("user_")) {
                      participantUserId = identity.split("_")[1];
                    }

                    console.log(
                      "📹 Found existing video track for:",
                      participant.name
                    );

                    // Dispatch event for video track
                    window.dispatchEvent(
                      new CustomEvent("remoteVideoTrackSubscribed", {
                        detail: {
                          participantIdentity: identity,
                          participantSid: participant.sid,
                          userId: participantUserId,
                          participantName: participant.name,
                          track: cameraPublication.track,
                          timestamp: Date.now(),
                        },
                      })
                    );
                  }

                  // ✅ Check for existing audio tracks
                  const audioPublication = participant.getTrackPublication?.(
                    Track.Source.Microphone
                  );
                  if (audioPublication?.track) {
                    console.log(
                      "🔊 Found existing audio track for:",
                      participant.name
                    );

                    // Store audio track reference
                    remoteAudioTracksRef.current.set(
                      participant.sid,
                      audioPublication.track
                    );

                    // Attach audio if not muted
                    if (
                      participant.isMicrophoneEnabled &&
                      !mutedParticipantsRef.current.has(participant.sid)
                    ) {
                      attachRemoteAudioTrack(
                        audioPublication.track,
                        participant
                      );
                    }
                  }
                });

                // ✅ Update remote participants state
                remoteParticipantsRef.current = initialRemoteParticipants;
                setRemoteParticipantsState(new Map(initialRemoteParticipants));

                console.log(
                  "✅ Remote participants synced:",
                  initialRemoteParticipants.size
                );
              } else {
                console.log("📹 No existing remote participants found");
              }
              // ==========================================================================

              await waitForStableConnection(newRoom, 15000);

              if (newRoom.state !== ConnectionState.Connected) {
                throw new Error("Connection not stable");
              }

              connectionReadyRef.current = true;
              isConnectedRef.current = true;
              setIsConnected(true);
              setConnectionState("connected");
              setMeetingInfo(tokenData.meeting_info || tokenData.meetingInfo);
              setLocalParticipant(newRoom.localParticipant);

              // CRITICAL FIX: IMMEDIATELY publish tracks for recording bot detection

              audioMutedRef.current = true;
              videoMutedRef.current = true;
              setIsAudioEnabled(false);
              setIsVideoEnabled(false);
              mediaInitializedRef.current = false;

              setTimeout(async () => {
                try {
                  // 🔥 STEP 1: Create video track (do NOT mute yet - needs dimensions)
                  console.log("📹 Creating video track...");
                  const videoTrack = await createLocalVideoTrack({
                    resolution: {
                      width: 640,
                      height: 480,
                      frameRate: 15,
                    },
                    facingMode: "user",
                  });

                  // ✅ CRITICAL: Wait for track to have dimensions before publishing
                  const videoMediaTrack = videoTrack.mediaStreamTrack;

                  // Wait for track to be ready with dimensions
                  if (videoMediaTrack) {
                    const settings = videoMediaTrack.getSettings();
                    console.log("📹 Video track settings:", settings);

                    // If no dimensions yet, wait briefly
                    if (!settings.width || !settings.height) {
                      console.log("⏳ Waiting for video track dimensions...");
                      await new Promise((resolve) => setTimeout(resolve, 500));
                    }
                  }

                  // 🔥 STEP 2: Publish the track FIRST (while still active)
                  console.log("📹 Publishing video track...");
                  await newRoom.localParticipant.publishTrack(videoTrack, {
                    name: "camera",
                    source: Track.Source.Camera,
                    simulcast: false,
                    videoCodec: "h264",
                    stopTrackOnMute: false,
                  });
                  console.log("✅ Video track published");

                  // 🔥 STEP 3: NOW mute it immediately after publishing
                  console.log("📹 Muting video track after publish...");
                  await videoTrack.mute();

                  if (videoMediaTrack) {
                    videoMediaTrack.enabled = false;
                  }

                  videoMutedRef.current = true;
                  setIsVideoEnabled(false);
                  videoTrackRef.current = videoTrack;

                  console.log("✅ Video track published and muted:", {
                    isMuted: videoTrack.isMuted,
                    mediaTrackEnabled: videoMediaTrack?.enabled,
                  });

                  // Check for screen shares
                  setTimeout(() => {
                    checkExistingScreenShares(newRoom);
                  }, 1000);

                  // 🔥 STEP 4: Create audio track
                  console.log("🎤 Creating audio track...");
                  const audioTrack = await createLocalAudioTrack({
                    echoCancellation: true,
                    noiseSuppression: true,
                    autoGainControl: true,
                    channelCount: 2,
                    sampleRate: 48000,
                  });

                  const audioMediaTrack = audioTrack.mediaStreamTrack;

                  // 🔥 STEP 5: Publish audio track FIRST
                  console.log("🎤 Publishing audio track...");
                  await newRoom.localParticipant.publishTrack(audioTrack, {
                    name: "microphone",
                    source: Track.Source.Microphone,
                    dtx: false,
                    red: true,
                    simulcast: false,
                    priority: "high",
                    stopTrackOnMute: false,
                  });
                  console.log("✅ Audio track published");

                  // 🔥 STEP 6: NOW mute audio immediately after publishing
                  console.log("🎤 Muting audio track after publish...");
                  await audioTrack.mute();

                  if (audioMediaTrack) {
                    audioMediaTrack.enabled = false;
                  }

                  audioMutedRef.current = true;
                  setIsAudioEnabled(false);
                  audioTrackRef.current = audioTrack;

                  console.log("✅ Audio track published and muted:", {
                    isMuted: audioTrack.isMuted,
                    mediaTrackEnabled: audioMediaTrack?.enabled,
                  });

                  // 🔥 STEP 7: Verify mute state after a short delay
                  setTimeout(async () => {
                    const videoPublication =
                      newRoom.localParticipant.getTrackPublication(
                        Track.Source.Camera
                      );
                    const audioPublication =
                      newRoom.localParticipant.getTrackPublication(
                        Track.Source.Microphone
                      );

                    console.log("🔍 Verifying track states after publish...");

                    // Force mute video if still unmuted
                    if (videoPublication?.track) {
                      if (!videoPublication.track.isMuted) {
                        console.warn("⚠️ Video still unmuted, forcing mute...");
                        await videoPublication.track.mute();
                      }
                      if (videoPublication.track.mediaStreamTrack?.enabled) {
                        console.warn(
                          "⚠️ Video MediaStreamTrack still enabled, forcing disable..."
                        );
                        videoPublication.track.mediaStreamTrack.enabled = false;
                      }
                      videoMutedRef.current = true;
                      setIsVideoEnabled(false);
                    }

                    // Force mute audio if still unmuted
                    if (audioPublication?.track) {
                      if (!audioPublication.track.isMuted) {
                        console.warn("⚠️ Audio still unmuted, forcing mute...");
                        await audioPublication.track.mute();
                      }
                      if (audioPublication.track.mediaStreamTrack?.enabled) {
                        console.warn(
                          "⚠️ Audio MediaStreamTrack still enabled, forcing disable..."
                        );
                        audioPublication.track.mediaStreamTrack.enabled = false;
                      }
                      audioMutedRef.current = true;
                      setIsAudioEnabled(false);
                    }

                    console.log("✅ Track state verification complete:", {
                      videoMuted: videoPublication?.track?.isMuted,
                      videoMediaEnabled:
                        videoPublication?.track?.mediaStreamTrack?.enabled,
                      audioMuted: audioPublication?.track?.isMuted,
                      audioMediaEnabled:
                        audioPublication?.track?.mediaStreamTrack?.enabled,
                    });

                    if (window.showNotificationMessage) {
                      window.showNotificationMessage(
                        "Connected - mic/camera OFF (tracks ready for recording)",
                        "success"
                      );
                    }
                  }, 500);

                  // 🔥 STEP 8: Additional verification at 2 seconds
                  setTimeout(async () => {
                    const videoPublication =
                      newRoom.localParticipant.getTrackPublication(
                        Track.Source.Camera
                      );
                    const audioPublication =
                      newRoom.localParticipant.getTrackPublication(
                        Track.Source.Microphone
                      );

                    console.log("🔍 2-second verification:", {
                      videoMuted: videoPublication?.track?.isMuted,
                      videoMediaEnabled:
                        videoPublication?.track?.mediaStreamTrack?.enabled,
                      audioMuted: audioPublication?.track?.isMuted,
                      audioMediaEnabled:
                        audioPublication?.track?.mediaStreamTrack?.enabled,
                      videoMutedRef: videoMutedRef.current,
                      audioMutedRef: audioMutedRef.current,
                    });

                    // Final force mute if needed
                    if (videoMutedRef.current && videoPublication?.track) {
                      if (!videoPublication.track.isMuted) {
                        await videoPublication.track.mute();
                      }
                      if (videoPublication.track.mediaStreamTrack?.enabled) {
                        videoPublication.track.mediaStreamTrack.enabled = false;
                      }
                    }

                    if (audioMutedRef.current && audioPublication?.track) {
                      if (!audioPublication.track.isMuted) {
                        await audioPublication.track.mute();
                      }
                      if (audioPublication.track.mediaStreamTrack?.enabled) {
                        audioPublication.track.mediaStreamTrack.enabled = false;
                      }
                    }
                  }, 2000);

                  // Sync remote participants after local tracks published
                  setTimeout(() => {
                    if (
                      newRoom.remoteParticipants &&
                      newRoom.remoteParticipants.size > 0
                    ) {
                      console.log(
                        "📹 Re-syncing remote participants after track publish..."
                      );

                      const updatedRemoteParticipants = new Map();

                      newRoom.remoteParticipants.forEach(
                        (participant, identity) => {
                          updatedRemoteParticipants.set(identity, participant);

                          // Check for video tracks
                          const cameraPublication =
                            participant.getTrackPublication?.(
                              Track.Source.Camera
                            );
                          if (
                            cameraPublication?.track &&
                            participant.isCameraEnabled
                          ) {
                            let participantUserId = identity;
                            if (identity?.includes("user_")) {
                              participantUserId = identity.split("_")[1];
                            }

                            console.log(
                              "📹 Re-syncing video track for:",
                              participant.name
                            );

                            window.dispatchEvent(
                              new CustomEvent("remoteVideoTrackSubscribed", {
                                detail: {
                                  participantIdentity: identity,
                                  participantSid: participant.sid,
                                  userId: participantUserId,
                                  participantName: participant.name,
                                  track: cameraPublication.track,
                                  timestamp: Date.now(),
                                },
                              })
                            );
                          }
                        }
                      );

                      remoteParticipantsRef.current = updatedRemoteParticipants;
                      setRemoteParticipantsState(
                        new Map(updatedRemoteParticipants)
                      );

                      console.log(
                        "✅ Remote participants re-synced:",
                        updatedRemoteParticipants.size
                      );
                    }
                  }, 1000);
                } catch (publishError) {
                  console.error(
                    "❌ CRITICAL: Failed to publish tracks:",
                    publishError
                  );

                  // Don't block connection - tracks can be published later
                  if (window.showNotificationMessage) {
                    window.showNotificationMessage(
                      "Connected but media setup had issues - try turning on camera/mic manually",
                      "warning"
                    );
                  }
                }
              }, 500);

              startHeartbeat();
              startParticipantSync(meetingId);

              // ==========================================================================
              // ✅ CRITICAL: Set up periodic remote participant sync
              // ==========================================================================
              const remoteParticipantSyncInterval = setInterval(() => {
                if (
                  newRoom.state === ConnectionState.Connected &&
                  newRoom.remoteParticipants
                ) {
                  const currentRemoteParticipants = new Map();

                  newRoom.remoteParticipants.forEach(
                    (participant, identity) => {
                      currentRemoteParticipants.set(identity, participant);
                    }
                  );

                  // Only update if size changed
                  if (
                    currentRemoteParticipants.size !==
                    remoteParticipantsRef.current.size
                  ) {
                    console.log("📹 Remote participants count changed:", {
                      previous: remoteParticipantsRef.current.size,
                      current: currentRemoteParticipants.size,
                    });

                    remoteParticipantsRef.current = currentRemoteParticipants;
                    setRemoteParticipantsState(
                      new Map(currentRemoteParticipants)
                    );
                  }
                }
              }, 3000); // Sync every 3 seconds

              // Store interval for cleanup
              if (!window.livekitIntervals) {
                window.livekitIntervals = [];
              }
              window.livekitIntervals.push(remoteParticipantSyncInterval);
              // ==========================================================================

              return {
                success: true,
                room: newRoom,
                meetingInfo: tokenData.meeting_info || tokenData.meetingInfo,
                participantIdentity:
                  tokenData.participant_identity ||
                  tokenData.participantIdentity,
                tracksPublished: true,
              };
            } catch (connectError) {
              clearTimeout(connectionTimeout);
              throw connectError;
            }
          } catch (error) {
            console.error("Connection failed:", error);
            await cleanupExistingConnection();

            connectionAttemptRef.current = false;
            setIsConnecting(false);
            isConnectedRef.current = false;
            setIsConnected(false);

            throw error;
          } finally {
            connectionAttemptRef.current = false;
            setIsConnecting(false);
            connectionLockRef.current = false;
            activeConnectionRef.current = null;
          }
        })();

        activeConnectionRef.current = connectionPromise;
        return await connectionPromise;
      } catch (error) {
        console.error("Connection wrapper failed:", error);
        connectionLockRef.current = false;
        activeConnectionRef.current = null;
        throw error;
      }
    },
    [
      cleanupAllAudioElements,
      setupOptimizedEventListeners,
      cleanupExistingConnection,
      startHeartbeat,
      startParticipantSync,
      getAccessTokenWithRetry,
      waitForStableConnection,
      checkExistingScreenShares,
      attachRemoteAudioTrack, // ✅ Added dependency
    ]
  );


  const disableAudio = useCallback(async () => {
    try {
      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) return false;

      console.log("🎤 disableAudio called...");

      const audioPublication = activeRoom.localParticipant.getTrackPublication(
        Track.Source.Microphone
      );

      if (audioPublication?.track) {
        // Step 1: Update ref FIRST (source of truth)
        audioMutedRef.current = true;

        // Step 2: Mute LiveKit track
        await audioPublication.track.mute();
        console.log("✅ LiveKit track MUTED");

        // Step 3: Disable MediaStreamTrack (CRITICAL - prevents leak)
        const mediaTrack = audioPublication.track.mediaStreamTrack;
        if (mediaTrack) {
          mediaTrack.enabled = false;
          console.log("✅ MediaStreamTrack DISABLED - NO AUDIO LEAK");
        }

        // Step 4: Update state
        setIsAudioEnabled(false);

        console.log("✅ Audio disabled successfully");
        return true;
      }

      return false;
    } catch (error) {
      console.error("Audio disable failed:", error);
      // Force disable on error
      audioMutedRef.current = true;
      setIsAudioEnabled(false);
      return false;
    }
  }, []);
  const toggleAudio = useCallback(async () => {
    try {
      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) {
        console.error("❌ No active room for audio toggle");
        return false;
      }

      const audioPublication = activeRoom.localParticipant.getTrackPublication(
        Track.Source.Microphone
      );

      // If no audio track exists, create one
      if (!audioPublication?.track) {
        console.log("📢 No audio track exists, creating new one...");
        return await enableAudio();
      }

      const currentlyMuted = audioPublication.track.isMuted;
      const mediaTrack = audioPublication.track.mediaStreamTrack;

      console.log("🎤 Toggle Audio - Current State:", {
        livekitMuted: currentlyMuted,
        mediaTrackEnabled: mediaTrack?.enabled,
        refSaysMuted: audioMutedRef.current,
        stateSaysEnabled: isAudioEnabled,
        willBecome: currentlyMuted ? "UNMUTED (audio ON)" : "MUTED (audio OFF)",
      });

      if (currentlyMuted) {
        // ========== UNMUTE (Enable Audio Transmission) ==========
        console.log("🎤 UNMUTING audio...");

        // Step 1: Update ref FIRST (source of truth)
        audioMutedRef.current = false;

        // Step 2: Enable MediaStreamTrack (allows audio to flow)
        if (mediaTrack) {
          mediaTrack.enabled = true;
          console.log("✅ Step 1: MediaStreamTrack ENABLED");
        }

        // Step 3: Unmute LiveKit track (broadcasts to all participants)
        await audioPublication.track.unmute();
        console.log("✅ Step 2: LiveKit track UNMUTED");

        // Step 4: Update React state
        setIsAudioEnabled(true);
        console.log("✅ Step 3: State updated - isAudioEnabled = true");

        // Step 5: Verify
        console.log("✅ Audio UNMUTED - Transmitting to all participants", {
          livekitMuted: audioPublication.track.isMuted,
          mediaTrackEnabled: mediaTrack?.enabled,
        });

        return true; // Audio is now ON
      } else {
        // ========== MUTE (Stop Audio Transmission) ==========
        console.log("🎤 MUTING audio...");

        // Step 1: Update ref FIRST (source of truth) - CRITICAL for preventing leaks
        audioMutedRef.current = true;

        // Step 2: Mute LiveKit track IMMEDIATELY (stops broadcast)
        await audioPublication.track.mute();
        console.log("✅ Step 1: LiveKit track MUTED");

        // Step 3: Disable MediaStreamTrack (ensures ZERO audio - prevents leak)
        if (mediaTrack) {
          mediaTrack.enabled = false;
          console.log("✅ Step 2: MediaStreamTrack DISABLED - NO AUDIO LEAK");
        }

        // Step 4: Update React state
        setIsAudioEnabled(false);
        console.log("✅ Step 3: State updated - isAudioEnabled = false");

        // Step 5: TRIPLE VERIFY no audio leak
        setTimeout(() => {
          const verifyPub = activeRoom.localParticipant.getTrackPublication(
            Track.Source.Microphone
          );
          if (verifyPub?.track) {
            const verifyMediaTrack = verifyPub.track.mediaStreamTrack;

            if (!verifyPub.track.isMuted) {
              console.error(
                "🚨 AUDIO LEAK: LiveKit track not muted! Force muting..."
              );
              verifyPub.track.mute();
            }

            if (verifyMediaTrack?.enabled) {
              console.error(
                "🚨 AUDIO LEAK: MediaStreamTrack enabled! Force disabling..."
              );
              verifyMediaTrack.enabled = false;
            }

            console.log("✅ Audio mute verification:", {
              livekitMuted: verifyPub.track.isMuted,
              mediaTrackEnabled: verifyMediaTrack?.enabled,
              LEAK_STATUS: verifyMediaTrack?.enabled
                ? "🚨 LEAKING!"
                : "✅ NO LEAK",
            });
          }
        }, 100);

        console.log("✅ Audio MUTED - No transmission");
        return false; // Audio is now OFF
      }
    } catch (error) {
      console.error("❌ Audio toggle failed:", error);

      // On error, force mute to prevent leak
      try {
        const pub = roomRef.current?.localParticipant?.getTrackPublication(
          Track.Source.Microphone
        );
        if (pub?.track) {
          await pub.track.mute();
          if (pub.track.mediaStreamTrack) {
            pub.track.mediaStreamTrack.enabled = false;
          }
        }
        audioMutedRef.current = true;
        setIsAudioEnabled(false);
      } catch (e) {
        console.error("Failed to force mute on error:", e);
      }

      return false;
    }
  }, [enableAudio, isAudioEnabled]);

  // ADD this function to useLiveKit.js after other functions
  const verifyTracksPublished = useCallback(() => {
    if (!roomRef.current?.localParticipant) {
      return false;
    }

    const participant = roomRef.current.localParticipant;
    const videoTrack = participant.getTrackPublication(Track.Source.Camera);
    const audioTrack = participant.getTrackPublication(Track.Source.Microphone);

    const hasPublishedTracks = !!(videoTrack?.track || audioTrack?.track);

    if (hasPublishedTracks) {
      if (window.showNotificationMessage) {
        window.showNotificationMessage(
          "Media tracks published - recording bot can detect content",
          "success"
        );
      }
    } else {
      console.error(
        "❌ RECORDING BOT: NO TRACKS PUBLISHED - recording will fail"
      );
      if (window.showNotificationMessage) {
        window.showNotificationMessage(
          "WARNING: No media tracks published - recording may fail",
          "error"
        );
      }
    }

    return hasPublishedTracks;
  }, []);

  // ADD this useEffect to useLiveKit.js to auto-verify tracks
  useEffect(() => {
    if (isConnected && roomRef.current) {
      // Verify tracks are published 5 seconds after connection
      const verificationTimer = setTimeout(() => {
        verifyTracksPublished();
      }, 5000);

      // Also verify after 10 seconds as backup
      const backupTimer = setTimeout(() => {
        verifyTracksPublished();
      }, 10000);

      return () => {
        clearTimeout(verificationTimer);
        clearTimeout(backupTimer);
      };
    }
  }, [isConnected, verifyTracksPublished]);

  // Speaker mute functions

  const muteSpeaker = useCallback(() => {
    speakerMutedRef.current = true;
    setIsSpeakerMuted(true);

    remoteAudioElementsRef.current.forEach((audioElement, key) => {
      if (audioElement) {
        audioElement.volume = 0;
        console.log(`🔇 Muted audio for: ${key}`);
      }
    });

    console.log("🔇 All speakers muted (including screen share audio)");
  }, []);

  // Unmute all speakers including screen share audio
  const unmuteSpeaker = useCallback(() => {
    speakerMutedRef.current = false;
    setIsSpeakerMuted(false);

    remoteAudioElementsRef.current.forEach((audioElement, key) => {
      if (audioElement) {
        audioElement.volume = 1.0;
        audioElement.muted = false;

        // Try to play if paused
        if (audioElement.paused) {
          audioElement.play().catch(console.warn);
        }

        console.log(`🔊 Unmuted audio for: ${key}`);
      }
    });

    console.log("🔊 All speakers unmuted (including screen share audio)");
  }, []);

  const toggleSpeaker = useCallback(() => {
    if (speakerMutedRef.current) {
      unmuteSpeaker();
    } else {
      muteSpeaker();
    }
    return !speakerMutedRef.current;
  }, [muteSpeaker, unmuteSpeaker]);

  const enableVideo = useCallback(async () => {
    try {
      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) {
        console.warn("No active room for video publishing");
        return false;
      }

      console.log("📹 enableVideo called - will broadcast to ALL participants");

      const existingVideoPub = activeRoom.localParticipant.getTrackPublication(
        Track.Source.Camera
      );

      if (existingVideoPub?.track) {
        console.log(
          "📹 Existing video track found, enabling for ALL participants..."
        );

        // Step 1: Update ref
        videoMutedRef.current = false;

        // Step 2: Enable MediaStreamTrack FIRST
        const mediaTrack = existingVideoPub.track.mediaStreamTrack;
        if (mediaTrack) {
          mediaTrack.enabled = true;
          console.log("✅ MediaStreamTrack ENABLED");
        }

        // Step 3: Unmute LiveKit track - BROADCASTS TO ALL
        if (existingVideoPub.track.isMuted) {
          await existingVideoPub.track.unmute();
          console.log(
            "✅ LiveKit track UNMUTED - Broadcast to all participants"
          );
        }

        // ✅ Wait for track to be fully active
        await new Promise((resolve) => setTimeout(resolve, 100));

        videoTrackRef.current = existingVideoPub.track;
        setIsVideoEnabled(true);

        // ✅ CRITICAL: Dispatch events for UI update
        window.dispatchEvent(
          new CustomEvent("localVideoStateChanged", {
            detail: {
              userId: userIdRef.current,
              isVideoEnabled: true,
              hasStream: true,
              timestamp: Date.now(),
            },
          })
        );

        window.dispatchEvent(
          new CustomEvent("localVideoReady", {
            detail: {
              userId: userIdRef.current,
              isVideoEnabled: true,
              timestamp: Date.now(),
            },
          })
        );

        // Broadcast via data channel
        try {
          const encoder = new TextEncoder();
          const videoStateData = encoder.encode(
            JSON.stringify({
              type: "participant_video_state_changed",
              user_id: userIdRef.current,
              participant_identity: activeRoom.localParticipant.identity,
              participant_sid: activeRoom.localParticipant.sid,
              participant_name: activeRoom.localParticipant.name,
              is_video_enabled: true,
              timestamp: Date.now(),
            })
          );

          await activeRoom.localParticipant.publishData(
            videoStateData,
            DataPacket_Kind.RELIABLE
          );
          console.log("✅ Broadcast video ENABLED to all participants");
        } catch (broadcastErr) {
          console.warn("Could not broadcast video state:", broadcastErr);
        }

        console.log("✅ Existing video track enabled for ALL participants");
        return true;
      }

      // No existing track, create new one
      console.log("📹 Creating new video track for ALL participants...");

      const videoTrack = await createLocalVideoTrack({
        resolution: { width: 640, height: 480, frameRate: 15 },
        facingMode: "user",
      });

      videoTrackRef.current = videoTrack;

      // Publish track to LiveKit
      await activeRoom.localParticipant.publishTrack(videoTrack, {
        name: "camera",
        source: Track.Source.Camera,
        simulcast: false,
        videoCodec: "h264",
        stopTrackOnMute: false,
      });

      console.log("✅ Video track PUBLISHED to LiveKit server");

      // Ensure MediaStreamTrack is ENABLED
      const mediaTrack = videoTrack.mediaStreamTrack;
      if (mediaTrack) {
        mediaTrack.enabled = true;
      }

      // Ensure LiveKit track is NOT muted
      if (videoTrack.isMuted) {
        await videoTrack.unmute();
        console.log("✅ New video track UNMUTED - Broadcasting to all");
      }

      // ✅ Wait for track to be fully active
      await new Promise((resolve) => setTimeout(resolve, 100));

      setLocalTracks((prev) => ({ ...prev, video: videoTrack }));
      setIsVideoEnabled(true);
      videoMutedRef.current = false;

      // ✅ CRITICAL: Dispatch events for UI update
      window.dispatchEvent(
        new CustomEvent("localVideoStateChanged", {
          detail: {
            userId: userIdRef.current,
            isVideoEnabled: true,
            hasStream: true,
            timestamp: Date.now(),
          },
        })
      );

      window.dispatchEvent(
        new CustomEvent("localVideoReady", {
          detail: {
            userId: userIdRef.current,
            isVideoEnabled: true,
            timestamp: Date.now(),
          },
        })
      );

      // Broadcast via data channel
      try {
        const encoder = new TextEncoder();
        const videoStateData = encoder.encode(
          JSON.stringify({
            type: "participant_video_state_changed",
            user_id: userIdRef.current,
            participant_identity: activeRoom.localParticipant.identity,
            participant_sid: activeRoom.localParticipant.sid,
            participant_name: activeRoom.localParticipant.name,
            is_video_enabled: true,
            timestamp: Date.now(),
          })
        );

        await activeRoom.localParticipant.publishData(
          videoStateData,
          DataPacket_Kind.RELIABLE
        );
        console.log("✅ Broadcast new video track to all participants");
      } catch (broadcastErr) {
        console.warn("Could not broadcast video state:", broadcastErr);
      }

      console.log(
        "✅ New video track created and enabled for ALL participants"
      );
      return true;
    } catch (error) {
      console.error("❌ Video enable/publish failed:", error);
      setError(`Video error: ${error.message}`);
      return false;
    }
  }, []);

  const toggleVideo = useCallback(async () => {
    try {
      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) {
        console.error("❌ No active room for video toggle");
        return false;
      }

      const videoPublication = activeRoom.localParticipant.getTrackPublication(
        Track.Source.Camera
      );

      // If no video track exists, create one (this enables video)
      if (!videoPublication?.track) {
        console.log("📹 No video track exists, creating new one...");
        const result = await enableVideo();

        // ✅ CRITICAL: Dispatch event immediately after enabling
        if (result) {
          window.dispatchEvent(
            new CustomEvent("localVideoStateChanged", {
              detail: {
                userId: userIdRef.current,
                isVideoEnabled: true,
                hasStream: true,
                timestamp: Date.now(),
              },
            })
          );

          // ✅ CRITICAL: Also dispatch localVideoReady event
          setTimeout(() => {
            window.dispatchEvent(
              new CustomEvent("localVideoReady", {
                detail: {
                  userId: userIdRef.current,
                  isVideoEnabled: true,
                  timestamp: Date.now(),
                },
              })
            );
          }, 100);
        }

        return result;
      }

      const currentlyMuted = videoPublication.track.isMuted;
      const mediaTrack = videoPublication.track.mediaStreamTrack;

      console.log("📹 Toggle Video - Current State:", {
        isMuted: currentlyMuted,
        mediaTrackEnabled: mediaTrack?.enabled,
        videoMutedRef: videoMutedRef.current,
        isVideoEnabled: isVideoEnabled,
        willBecome: currentlyMuted
          ? "ENABLED (showing video)"
          : "DISABLED (hiding video)",
      });

      if (currentlyMuted) {
        // ========== ENABLE VIDEO ==========
        console.log("📹 ENABLING video...");

        // Step 1: Update ref FIRST
        videoMutedRef.current = false;

        // Step 2: Enable MediaStreamTrack FIRST (before unmuting LiveKit)
        if (mediaTrack) {
          mediaTrack.enabled = true;
          console.log("✅ MediaStreamTrack ENABLED");
        }

        // Step 3: Unmute LiveKit track
        await videoPublication.track.unmute();
        console.log("✅ LiveKit track UNMUTED");

        // ✅ CRITICAL: Wait for track to be fully active
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Step 4: Update state
        setIsVideoEnabled(true);

        // ✅ CRITICAL: Dispatch event IMMEDIATELY
        window.dispatchEvent(
          new CustomEvent("localVideoStateChanged", {
            detail: {
              userId: userIdRef.current,
              isVideoEnabled: true,
              hasStream: true,
              timestamp: Date.now(),
            },
          })
        );

        // ✅ CRITICAL: Dispatch localVideoReady for UI update
        window.dispatchEvent(
          new CustomEvent("localVideoReady", {
            detail: {
              userId: userIdRef.current,
              isVideoEnabled: true,
              timestamp: Date.now(),
            },
          })
        );

        // Step 5: Broadcast via data channel
        try {
          const encoder = new TextEncoder();
          const videoStateData = encoder.encode(
            JSON.stringify({
              type: "participant_video_state_changed",
              user_id: userIdRef.current,
              participant_identity: activeRoom.localParticipant.identity,
              participant_sid: activeRoom.localParticipant.sid,
              participant_name: activeRoom.localParticipant.name,
              is_video_enabled: true,
              timestamp: Date.now(),
            })
          );
          await activeRoom.localParticipant.publishData(
            videoStateData,
            DataPacket_Kind.RELIABLE
          );
          console.log("✅ Broadcast video ENABLED to all participants");
        } catch (broadcastErr) {
          console.warn("Broadcast failed:", broadcastErr);
        }

        console.log("✅ VIDEO ENABLED");
        return true;
      } else {
        // ========== DISABLE VIDEO ==========
        console.log("📹 DISABLING video...");

        // Step 1: Update ref FIRST (source of truth)
        videoMutedRef.current = true;

        // Step 2: Mute LiveKit track IMMEDIATELY
        await videoPublication.track.mute();
        console.log("✅ LiveKit track MUTED");

        // Step 3: Disable MediaStreamTrack IMMEDIATELY
        if (mediaTrack) {
          mediaTrack.enabled = false;
          console.log("✅ MediaStreamTrack DISABLED");
        }

        // Step 4: Update state
        setIsVideoEnabled(false);

        // ✅ Dispatch event
        window.dispatchEvent(
          new CustomEvent("localVideoStateChanged", {
            detail: {
              userId: userIdRef.current,
              isVideoEnabled: false,
              hasStream: false,
              timestamp: Date.now(),
            },
          })
        );

        // Step 5: Broadcast
        try {
          const encoder = new TextEncoder();
          const videoStateData = encoder.encode(
            JSON.stringify({
              type: "participant_video_state_changed",
              user_id: userIdRef.current,
              participant_identity: activeRoom.localParticipant.identity,
              participant_sid: activeRoom.localParticipant.sid,
              participant_name: activeRoom.localParticipant.name,
              is_video_enabled: false,
              timestamp: Date.now(),
            })
          );
          await activeRoom.localParticipant.publishData(
            videoStateData,
            DataPacket_Kind.RELIABLE
          );
        } catch (e) {
          console.warn("Broadcast failed:", e);
        }

        console.log("✅ VIDEO DISABLED");
        return false;
      }
    } catch (error) {
      console.error("❌ Video toggle failed:", error);

      // On error, force the expected state
      try {
        const pub = roomRef.current?.localParticipant?.getTrackPublication(
          Track.Source.Camera
        );
        if (pub?.track && videoMutedRef.current) {
          await pub.track.mute();
          if (pub.track.mediaStreamTrack) {
            pub.track.mediaStreamTrack.enabled = false;
          }
        }
      } catch (e) {}

      return isVideoEnabled;
    }
  }, [enableVideo, isVideoEnabled]);

  // Helper function to detect sharing mode
  const detectSharingMode = useCallback((videoLabel) => {
    const label = videoLabel.toLowerCase();

    if (label.includes("tab") || label.includes("chrome")) {
      return "chrome-tab";
    } else if (label.includes("window") || label.includes("application")) {
      return "window";
    } else if (
      label.includes("screen") ||
      label.includes("monitor") ||
      label.includes("display")
    ) {
      return "entire-screen";
    }

    return "unknown";
  }, []);

  const updateCoHostStatus = useCallback((isCoHost) => {
    isCoHostRef.current = isCoHost;

    // CRITICAL: Update screen share permissions for co-hosts
    if (isCoHost && !isHostRef.current) {
      setScreenSharePermissions((prev) => ({
        ...prev,
        requiresHostApproval: false,
        hasPermission: true,
        hostUserId: userIdRef.current,
      }));

      // Notify parent about role change
      if (window.attendanceTracker?.updateRole) {
        window.attendanceTracker.updateRole("co-host");
      }
    } else if (!isCoHost && !isHostRef.current) {
      setScreenSharePermissions((prev) => ({
        ...prev,
        requiresHostApproval: true,
        hasPermission: false,
        hostUserId: null,
      }));
      // Notify parent about role change
      if (window.attendanceTracker?.updateRole) {
        window.attendanceTracker.updateRole("participant");
      }
    }
  }, []);

  // Internal stop function for cleanup without state updates
  const stopScreenShareInternal = useCallback(async () => {
    const activeRoom = roomRef.current;
    if (!activeRoom?.localParticipant) return;

    try {
      // Stop video track
      const screenPub = activeRoom.localParticipant.getTrackPublication(
        Track.Source.ScreenShare
      );
      if (screenPub?.track) {
        await activeRoom.localParticipant.unpublishTrack(screenPub.track);
        screenPub.track.stop();
      }

      // Stop system audio track
      const screenAudioPub = activeRoom.localParticipant.getTrackPublication(
        Track.Source.ScreenShareAudio
      );
      if (screenAudioPub?.track) {
        await activeRoom.localParticipant.unpublishTrack(screenAudioPub.track);
        screenAudioPub.track.stop();
      }

      // Clean up stream
      if (screenShareStreamRef.current) {
        if (screenShareStreamRef.current instanceof MediaStream) {
          screenShareStreamRef.current
            .getTracks()
            .forEach((track) => track.stop());
        }
        screenShareStreamRef.current = null;
      }

      // Reset internal state
      screenShareStateRef.current.videoTrackPublished = false;
      screenShareStateRef.current.audioTrackPublished = false;
      screenShareStateRef.current.publishingPromises.clear();

      // Reset permission for next time (unless host)
      if (!isHostRef.current) {
        setScreenSharePermissions((prev) => ({
          ...prev,
          hasPermission: false,
        }));
      }
    } catch (error) {
      console.error("Screen share cleanup error:", error);
    }
  }, []);

  // Enhanced monitoring with lower threshold
  const monitorSystemAudio = useCallback((audioTrack) => {
    if (!audioTrack || audioTrack.readyState !== "live") {
      return;
    }

    try {
      const audioContext = new (window.AudioContext ||
        window.webkitAudioContext)();
      const source = audioContext.createMediaStreamSource(
        new MediaStream([audioTrack])
      );
      const analyser = audioContext.createAnalyser();

      // Minimal processing for stability
      analyser.fftSize = 32;
      source.connect(analyser);

      const bufferLength = analyser.frequencyBinCount;
      const dataArray = new Uint8Array(bufferLength);

      let audioDetected = false;
      let checkCount = 0;
      const maxChecks = 10;

      const checkAudio = setInterval(() => {
        if (!analyser) {
          clearInterval(checkAudio);
          return;
        }

        analyser.getByteFrequencyData(dataArray);
        const average = dataArray.reduce((a, b) => a + b) / bufferLength;

        checkCount++;

        // Very low threshold for better detection
        if (average > 0.5) {
          if (!audioDetected) {
            audioDetected = true;

            if (window.showNotificationMessage) {
              window.showNotificationMessage(
                "System audio confirmed!",
                "success"
              );
            }

            clearInterval(checkAudio);
            audioContext.close();
            return;
          }
        }

        if (checkCount >= maxChecks) {
          if (!audioDetected) {
            if (window.showNotificationMessage) {
              window.showNotificationMessage(
                "System audio not detected. Ensure 'Share tab audio' is checked.",
                "warning"
              );
            }
          }
          clearInterval(checkAudio);
          audioContext.close();
        }
      }, 2000);
    } catch (error) {
      console.error("Audio monitoring failed:", error);
    }
  }, []);

  // More reliable feedback system
  const provideFeedback = useCallback(
    (sharingMode, audioStrategy, hasSystemAudio) => {
      const messages = {
        "chrome-tab": {
          system_audio: {
            message: "Chrome Tab with system audio!",
            severity: "success",
          },
          system_audio_failed: {
            message:
              'Chrome Tab shared but audio failed. Check "Share tab audio" was selected.',
            severity: "warning",
          },
          no_audio_fallback: {
            message:
              'Chrome Tab without audio. Make sure "Share tab audio" is checked.',
            severity: "warning",
          },
          audio_not_ready: {
            message:
              "Audio track not ready. Try sharing again and ensure tab audio is selected.",
            severity: "warning",
          },
        },
        window: {
          no_audio_fallback: {
            message: "Window sharing blocks system audio.",
            severity: "error",
          },
        },
        "entire-screen": {
          no_audio_fallback: {
            message: "Screen sharing blocks system audio.",
            severity: "error",
          },
        },
      };

      if (window.showNotificationMessage) {
        const feedback = messages[sharingMode]?.[audioStrategy] || {
          message: "Screen sharing started",
          severity: "info",
        };

        window.showNotificationMessage(feedback.message, feedback.severity);

        // Additional specific guidance
        if (sharingMode === "chrome-tab" && audioStrategy !== "system_audio") {
          setTimeout(() => {
            window.showNotificationMessage(
              'Select "Chrome Tab" → Check "Share tab audio" → Click Share',
              "info"
            );
          }, 3000);
        }
      }
    },
    []
  );

  // Handle screen share errors with specific guidance
  const handleScreenShareError = useCallback((error) => {
    const errorMessages = {
      NotAllowedError: "Screen sharing permission denied.",
      NotSupportedError:
        "Screen sharing not supported. Use Chrome, Edge, or Firefox.",
      NotFoundError: "No screen available to share.",
      NotReadableError: "Screen sharing blocked by system settings.",
      AbortError: "Screen sharing cancelled by user.",
    };

    const message =
      errorMessages[error.name] || `Screen share error: ${error.message}`;

    if (window.showNotificationMessage) {
      window.showNotificationMessage(message, "error");

      // Additional YouTube-specific guidance
      if (error.name === "NotAllowedError") {
        setTimeout(() => {
          window.showNotificationMessage(
            "Select 'Chrome Tab' and check 'Share tab audio' to include sound"
          );
        }, 2000);
      }
    }
  }, []);

  // Screen share with permission system - UPDATED for co-host direct access
  const startScreenShare = useCallback(async () => {
    // Prevent multiple simultaneous attempts
    if (screenShareStateRef.current.isPublishing) {
      return { success: false, error: "Already publishing" };
    }

    try {
      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) {
        throw new Error("Not connected to room");
      }

      screenShareStateRef.current.isPublishing = true;

      // Check permissions for non-host/co-host users
      if (
        !isHostRef.current &&
        !isCoHostRef.current &&
        screenSharePermissions.requiresHostApproval
      ) {
        if (!screenSharePermissions.hasPermission) {
          if (screenSharePermissions.pendingRequest) {
            return {
              success: false,
              error: "Permission request already pending",
              needsPermission: true,
              pending: true,
            };
          }

          const displayName =
            localParticipant?.name ||
            localParticipant?.identity ||
            `User ${userIdRef.current}`;

          try {
            const requestId = await requestScreenSharePermission(
              userIdRef.current,
              displayName
            );

            return new Promise((resolve, reject) => {
              screenShareCallbacksRef.current.set(requestId, {
                resolve,
                reject,
              });

              setTimeout(() => {
                const callback = screenShareCallbacksRef.current.get(requestId);
                if (callback) {
                  screenShareCallbacksRef.current.delete(requestId);
                  setScreenSharePermissions((prev) => ({
                    ...prev,
                    pendingRequest: false,
                    requestId: null,
                  }));
                  reject(new Error("Permission request timed out"));
                }
              }, 30000);
            });
          } catch (error) {
            console.error("Permission request failed:", error);
            return {
              success: false,
              error: error.message,
              needsPermission: true,
            };
          }
        }
      }

      // Stop any existing screen share first
      await stopScreenShareInternal();
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Request screen capture with audio settings
      let screenStream = null;
      let hasSystemAudio = false;
      let audioStrategy = "none";

      try {
        // ✅ CRITICAL: Request screen share WITH AUDIO
        console.log("🔊 Requesting screen share with audio...");

        screenStream = await navigator.mediaDevices.getDisplayMedia({
          video: {
            mediaSource: "screen",
            width: { ideal: 1920, max: 1920 },
            height: { ideal: 1080, max: 1080 },
            frameRate: { ideal: 60, max: 60 },
            cursor: "always",
          },
          audio: {
            echoCancellation: false,
            noiseSuppression: false,
            autoGainControl: false,
            suppressLocalAudioPlayback: false,
            sampleRate: 48000,
            channelCount: 2,
            latency: 0.01,
            volume: 1.0,
          },
          preferCurrentTab: false,
          systemAudio: "include",
          surfaceSwitching: "include",
        });

        const videoTrack = screenStream.getVideoTracks()[0];
        const systemAudioTrack = screenStream.getAudioTracks()[0];

        if (!videoTrack) {
          throw new Error("No video track captured");
        }

        hasSystemAudio = !!systemAudioTrack;

        console.log("🔊 Screen share captured:", {
          hasVideo: !!videoTrack,
          hasAudio: hasSystemAudio,
          videoLabel: videoTrack.label,
          audioLabel: systemAudioTrack?.label || "none",
          audioState: systemAudioTrack?.readyState || "none",
          audioEnabled: systemAudioTrack?.enabled,
        });

        // Detect sharing mode
        const sharingMode = detectSharingMode(videoTrack.label);

        // ========== PUBLISH VIDEO TRACK ==========
        const liveKitScreenTrack = new LocalVideoTrack(videoTrack, {
          name: "screen_share",
          source: Track.Source.ScreenShare,
        });

        console.log("📺 Publishing screen share VIDEO track...");

        const videoPublishPromise = activeRoom.localParticipant.publishTrack(
          liveKitScreenTrack,
          {
            name: "screen_share",
            source: Track.Source.ScreenShare,
            simulcast: false,
            screenShareSimulcast: false,
            videoCodec: "h264",
            priority: "high",
            videoBitrate: 8000000,
            stopVideoTrackOnMute: false,
            stopMicTrackOnMute: false,
          }
        );

        screenShareStateRef.current.publishingPromises.set(
          "video",
          videoPublishPromise
        );

        await videoPublishPromise;
        screenShareStateRef.current.videoTrackPublished = true;
        console.log("✅ Screen share VIDEO track published");

        await new Promise((resolve) => setTimeout(resolve, 200));

        // ========== PUBLISH AUDIO TRACK - CRITICAL ==========
        if (
          hasSystemAudio &&
          systemAudioTrack &&
          systemAudioTrack.readyState === "live"
        ) {
          try {
            console.log("🔊 Publishing screen share AUDIO track...");
            console.log("🔊 Audio track details BEFORE publish:", {
              kind: systemAudioTrack.kind,
              label: systemAudioTrack.label,
              readyState: systemAudioTrack.readyState,
              enabled: systemAudioTrack.enabled,
              muted: systemAudioTrack.muted,
              id: systemAudioTrack.id,
            });

            const liveKitSystemAudioTrack = new LocalAudioTrack(
              systemAudioTrack,
              {
                name: "system_audio",
                source: Track.Source.ScreenShareAudio,
              }
            );

            const audioPublishPromise =
              activeRoom.localParticipant.publishTrack(
                liveKitSystemAudioTrack,
                {
                  name: "system_audio",
                  source: Track.Source.ScreenShareAudio,
                  dtx: false,
                  red: false,
                  simulcast: false,
                  priority: "high",
                  audioCodec: "opus",
                  bitrate: 128000,
                  stereo: true,
                  echoCancellation: false,
                  noiseSuppression: false,
                  autoGainControl: false,
                  stopMicTrackOnMute: false,
                }
              );

            screenShareStateRef.current.publishingPromises.set(
              "audio",
              audioPublishPromise
            );

            await audioPublishPromise;
            screenShareStateRef.current.audioTrackPublished = true;
            audioStrategy = "system_audio";

            console.log(
              "✅✅✅ Screen share AUDIO track PUBLISHED successfully!"
            );

            // Verify audio track publication
            const audioPublication =
              activeRoom.localParticipant.getTrackPublication(
                Track.Source.ScreenShareAudio
              );

            console.log("🔊 Audio publication verification:", {
              hasPublication: !!audioPublication,
              hasTrack: !!audioPublication?.track,
              trackKind: audioPublication?.track?.kind,
              isMuted: audioPublication?.track?.isMuted,
              isEnabled: audioPublication?.isEnabled,
            });

            // Monitor system audio
            setTimeout(() => monitorSystemAudio(systemAudioTrack), 1000);
          } catch (audioError) {
            console.error("❌ System audio publishing failed:", audioError);
            audioStrategy = "system_audio_failed";
            screenShareStateRef.current.audioTrackPublished = false;
          }
        } else if (!hasSystemAudio) {
          console.warn("⚠️ No system audio track available");
          audioStrategy = "no_audio_fallback";

          if (window.showNotificationMessage) {
            window.showNotificationMessage(
              "Screen sharing started WITHOUT audio. To include audio, select 'Chrome Tab' and check 'Share tab audio'",
              "warning"
            );
          }
        } else if (systemAudioTrack?.readyState !== "live") {
          console.warn(
            "⚠️ System audio track not live:",
            systemAudioTrack?.readyState
          );
          audioStrategy = "audio_not_ready";
        }

        // Verify tracks are published
        const videoPublication =
          activeRoom.localParticipant.getTrackPublication(
            Track.Source.ScreenShare
          );
        const audioPublication =
          activeRoom.localParticipant.getTrackPublication(
            Track.Source.ScreenShareAudio
          );

        console.log("🔍 Final publication status:", {
          videoPublished: !!videoPublication?.track,
          audioPublished: !!audioPublication?.track,
          audioStrategy,
        });

        if (videoPublication?.track) {
          screenShareStreamRef.current = screenStream;
          setIsScreenSharing(true);
          setLocalIsScreenSharing(true);

          // Broadcast screen share START
          try {
            const encoder = new TextEncoder();
            const screenShareStartData = encoder.encode(
              JSON.stringify({
                type: "screen_share_started",
                user_id: userIdRef.current,
                user_name: activeRoom.localParticipant.name || "User",
                user_identity: activeRoom.localParticipant.identity,
                has_audio: hasSystemAudio && audioStrategy === "system_audio",
                timestamp: Date.now(),
                sender_id: activeRoom.localParticipant.sid,
              })
            );

            activeRoom.localParticipant.publishData(
              screenShareStartData,
              DataPacket_Kind.RELIABLE
            );

            console.log(
              "📢 Broadcasted screen share start event with audio:",
              hasSystemAudio && audioStrategy === "system_audio"
            );
          } catch (broadcastError) {
            console.error(
              "❌ Failed to broadcast screen share start:",
              broadcastError
            );
          }

          // Handle track ending
          videoTrack.onended = () => {
            stopScreenShare();
          };

          if (systemAudioTrack) {
            systemAudioTrack.onended = () => {
              console.log("🔊 System audio track ended");
            };
          }

          // Provide feedback
          provideFeedback(sharingMode, audioStrategy, hasSystemAudio);

          return {
            success: true,
            audioStrategy,
            sharingMode,
            hasSystemAudio: hasSystemAudio && audioStrategy === "system_audio",
            videoTrack: videoTrack.label,
            audioTrack: systemAudioTrack?.label || "none",
            approved: true,
            userRole: isHostRef.current
              ? "host"
              : isCoHostRef.current
              ? "co-host"
              : "participant",
          };
        } else {
          throw new Error("Video track publishing verification failed");
        }
      } catch (error) {
        console.error("Screen share request failed:", error);

        if (screenStream) {
          screenStream.getTracks().forEach((track) => track.stop());
        }

        handleScreenShareError(error);
        throw error;
      }
    } catch (error) {
      console.error("Screen share wrapper failed:", error);

      screenShareStateRef.current.videoTrackPublished = false;
      screenShareStateRef.current.audioTrackPublished = false;
      setIsScreenSharing(false);
      setLocalIsScreenSharing(false);

      throw error;
    } finally {
      screenShareStateRef.current.isPublishing = false;
      screenShareStateRef.current.publishingPromises.clear();
    }
  }, [
    stopScreenShareInternal,
    monitorSystemAudio,
    provideFeedback,
    handleScreenShareError,
    detectSharingMode,
    requestScreenSharePermission,
    screenSharePermissions,
    localParticipant,
  ]);

  const stopScreenShare = useCallback(async (participantIdentity = null) => {
    try {
      console.log("🛑 Stopping screen share...", {
        participantIdentity,
        isHost: isHostRef.current,
        isCoHost: isCoHostRef.current,
      });

      const activeRoom = roomRef.current;
      if (!activeRoom?.localParticipant) {
        console.log("❌ No active room for stopping screen share");
        return false;
      }

      // ✅ NEW: Check if trying to stop someone else's screen share
      if (
        participantIdentity &&
        participantIdentity !== activeRoom.localParticipant.identity
      ) {
        // Trying to stop someone else's screen share
        // Only hosts/co-hosts can do this
        if (!isHostRef.current && !isCoHostRef.current) {
          console.error(
            "❌ PERMISSION DENIED: Only hosts/co-hosts can stop other participants' screen shares"
          );
          if (window.showNotificationMessage) {
            window.showNotificationMessage(
              "Only hosts and co-hosts can stop other participants' screen shares",
              "error"
            );
          }
          return false;
        }

        console.log(
          `🛡️ Host/Co-host stopping ${participantIdentity}'s screen share`
        );

        // Send command to target participant to stop their screen share
        const encoder = new TextEncoder();
        const stopCommand = encoder.encode(
          JSON.stringify({
            type: "force_stop_screen_share",
            target_identity: participantIdentity,
            stopped_by: activeRoom.localParticipant.identity,
            stopped_by_name: activeRoom.localParticipant.name || "Host",
            timestamp: Date.now(),
          })
        );

        await activeRoom.localParticipant.publishData(
          stopCommand,
          DataPacket_Kind.RELIABLE
        );

        if (window.showNotificationMessage) {
          window.showNotificationMessage(
            `Stopped screen share for participant`,
            "success"
          );
        }

        return true;
      }

      // ✅ EXISTING CODE: Stop own screen share
      console.log("🛑 Stopping own screen share...");

      // CRITICAL: Get publications BEFORE any state changes
      const screenPub = activeRoom.localParticipant.getTrackPublication(
        Track.Source.ScreenShare
      );
      const screenAudioPub = activeRoom.localParticipant.getTrackPublication(
        Track.Source.ScreenShareAudio
      );

      console.log("📋 Current screen share publications:", {
        video: !!screenPub?.track,
        audio: !!screenAudioPub?.track,
      });

      // IMMEDIATE unpublish - don't use internal function
      if (screenPub?.track) {
        console.log("🎬 Unpublishing video track...");
        await activeRoom.localParticipant.unpublishTrack(screenPub.track);
        screenPub.track.stop();
      }

      if (screenAudioPub?.track) {
        console.log("🔊 Unpublishing audio track...");
        await activeRoom.localParticipant.unpublishTrack(screenAudioPub.track);
        screenAudioPub.track.stop();
      }

      // Clean up stream
      if (screenShareStreamRef.current) {
        if (screenShareStreamRef.current instanceof MediaStream) {
          screenShareStreamRef.current.getTracks().forEach((track) => {
            track.stop();
            console.log("⏹️ Stopped track:", track.kind);
          });
        }
        screenShareStreamRef.current = null;
      }

      // Reset all screen share state IMMEDIATELY
      screenShareStateRef.current.isPublishing = false;
      screenShareStateRef.current.videoTrackPublished = false;
      screenShareStateRef.current.audioTrackPublished = false;
      screenShareStateRef.current.publishingPromises.clear();

      // Update UI state
      setLocalTracks((prev) => ({ ...prev, screenShare: null }));
      setScreenShareTrack(null);
      setIsScreenSharing(false);
      setLocalIsScreenSharing(false);
      setScreenSharingParticipant(null);

      // Reset permission for next time (unless host/co-host)
      if (!isHostRef.current && !isCoHostRef.current) {
        setScreenSharePermissions((prev) => ({
          ...prev,
          hasPermission: false,
        }));
      }
      console.log("✅ Screen share stopped successfully");

      // Broadcast screen share stop to all participants
      if (activeRoom && activeRoom.localParticipant) {
        try {
          const encoder = new TextEncoder();
          const screenShareStopData = encoder.encode(
            JSON.stringify({
              type: "screen_share_stopped",
              user_id: userIdRef.current,
              user_name: activeRoom.localParticipant.name || "User",
              timestamp: Date.now(),
              sender_id: activeRoom.localParticipant.sid,
            })
          );

          activeRoom.localParticipant.publishData(
            screenShareStopData,
            DataPacket_Kind.RELIABLE
          );
        } catch (err) {
          console.warn("Failed to broadcast screen share stop:", err);
        }
      }

      // Verify tracks are unpublished
      setTimeout(() => {
        const verifyScreenPub = activeRoom.localParticipant.getTrackPublication(
          Track.Source.ScreenShare
        );
        const verifyScreenAudioPub =
          activeRoom.localParticipant.getTrackPublication(
            Track.Source.ScreenShareAudio
          );

        if (!verifyScreenPub?.track && !verifyScreenAudioPub?.track) {
          console.log("✅ Verified: Screen share tracks fully unpublished");
        } else {
          console.error("❌ WARNING: Screen share tracks still published!", {
            video: !!verifyScreenPub?.track,
            audio: !!verifyScreenAudioPub?.track,
          });
        }
      }, 500);

      return true;
    } catch (error) {
      console.error("❌ Stop screen share failed:", error);

      // Force state reset even on error
      setIsScreenSharing(false);
      setLocalIsScreenSharing(false);
      setScreenShareTrack(null);
      setScreenSharingParticipant(null);

      return false;
    }
  }, []);

  // Toggle screen share
  const toggleScreenShare = useCallback(async () => {
    if (isScreenSharing || localIsScreenSharing) {
      return await stopScreenShare();
    } else {
      try {
        const result = await startScreenShare();
        return result?.success || false;
      } catch (error) {
        console.error("Toggle screen share failed:", error);
        return false;
      }
    }
  }, [
    isScreenSharing,
    localIsScreenSharing,
    startScreenShare,
    stopScreenShare,
  ]);

  // Add state verification function
  const verifyScreenShareState = useCallback(() => {
    if (!roomRef.current?.localParticipant) return false;

    const videoPublication =
      roomRef.current.localParticipant.getTrackPublication(
        Track.Source.ScreenShare
      );
    const audioPublication =
      roomRef.current.localParticipant.getTrackPublication(
        Track.Source.ScreenShareAudio
      );

    const actualVideoState = !!videoPublication?.track?.enabled;
    const actualAudioState = !!audioPublication?.track?.enabled;

    // Fix state if out of sync
    if (isScreenSharing !== actualVideoState) {
      setIsScreenSharing(actualVideoState);
      setLocalIsScreenSharing(actualVideoState);
    }

    return actualVideoState;
  }, [isScreenSharing]);

  const getScreenShareStream = useCallback(() => {
    const ref = screenShareStreamRef.current;

    if (ref instanceof MediaStream) {
      return ref;
    }

    if (ref && ref.srcObject instanceof MediaStream) {
      return ref.srcObject;
    }

    if (ref && ref.mediaStreamTrack instanceof MediaStreamTrack) {
      return new MediaStream([ref.mediaStreamTrack]);
    }

    if (ref && typeof ref.attach === "function") {
      try {
        const element = ref.attach();
        if (element && element.srcObject instanceof MediaStream) {
          return element.srcObject;
        }
      } catch (err) {
        console.warn("Error getting screen share stream:", err);
      }
    }

    if (screenShareTrack && screenShareTrack.mediaStreamTrack) {
      return new MediaStream([screenShareTrack.mediaStreamTrack]);
    }

    return ref;
  }, [screenShareTrack]);

  // Disconnect
  const disconnectFromRoom = useCallback(async () => {
    // ✅ CRITICAL: Prevent disconnect during active screen share operations
    if (screenShareStateRef.current.isPublishing) {
      console.warn(
        "⚠️ Disconnect blocked - screen share operation in progress"
      );
      return;
    }

    // ✅ CRITICAL: Prevent disconnect if meeting is active and not ended
    if (isConnectedRef.current && !meetingEnded) {
      console.warn("⚠️ Disconnect blocked - meeting still active");
      return;
    }

    console.log("🚪 Proceeding with disconnect from room");
    try {
      connectionLockRef.current = false;
      activeConnectionRef.current = null;

      cleanupAllAudioElements();

      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }

      if (isScreenSharing || localIsScreenSharing) {
        await stopScreenShare();
      }

      await cleanupExistingConnection();

      // Reset all states
      setRoom(null);
      setIsConnected(false);
      setConnectionState("disconnected");
      setParticipants([]);
      setLocalParticipant(null);
      setRemoteParticipants(new Map());
      setLocalTracks({ audio: null, video: null, screenShare: null });
      setIsAudioEnabled(false);
      setIsVideoEnabled(false);
      setIsScreenSharing(false);
      setLocalIsScreenSharing(false);
      setScreenSharingParticipant(null);
      setScreenShareTrack(null);
      setMeetingInfo(null);
      setError(null);
      setMeetingEnded(false);

      // Reset permission system
      setScreenSharePermissions({
        requiresHostApproval: true,
        hasPermission: false,
        pendingRequest: false,
        requestId: null,
        hostUserId: null,
      });
      setScreenShareRequests([]);
      setCurrentScreenShareRequest(null);

      connectionReadyRef.current = false;
      isConnectedRef.current = false;
      mediaInitializedRef.current = false;
      currentMeetingIdRef.current = null;
      isHostRef.current = false;
    } catch (error) {
      console.error("Disconnect error:", error);
    }
  }, [
    cleanupAllAudioElements,
    cleanupExistingConnection,
    isScreenSharing,
    localIsScreenSharing,
    stopScreenShare,
  ]);

  // Send message
  const sendMessage = useCallback((type, data) => {
    if (!roomRef.current || !isConnectedRef.current) return false;

    try {
      const encoder = new TextEncoder();
      const packet = encoder.encode(
        JSON.stringify({
          type,
          ...data,
          timestamp: Date.now(),
          sender_id: roomRef.current.localParticipant.sid,
        })
      );

      roomRef.current.localParticipant.publishData(
        packet,
        DataPacket_Kind.RELIABLE
      );
      return true;
    } catch (error) {
      console.error("Send failed:", error);
      return false;
    }
  }, []);

  const sendChatMessage = useCallback(
    (message) => {
      return sendMessage("chat-message", { message });
    },
    [sendMessage]
  );

  const sendReaction = useCallback(
    (emoji) => {
      return sendMessage("reaction", { emoji });
    },
    [sendMessage]
  );

  // Get participants list
  const getParticipantsList = useCallback(() => {
    if (!roomRef.current) return [];

    try {
      const localPart = roomRef.current.localParticipant;
      const remoteParts = Array.from(
        roomRef.current.remoteParticipants.values()
      );

      const allParticipants = localPart
        ? [localPart, ...remoteParts]
        : remoteParts;

      return allParticipants.map((participant) => ({
        identity: participant.identity,
        name: participant.name || participant.identity,
        isLocal: participant.sid === localParticipantSidRef.current,
        connectionQuality: participant.connectionQuality || "unknown",
        isSpeaking: participant.isSpeaking || false,
        audioEnabled: participant.isMicrophoneEnabled || false,
        videoEnabled: participant.isCameraEnabled || false,
        isScreenSharing: !!participant.getTrackPublication?.(
          Track.Source.ScreenShare
        )?.track,
      }));
    } catch (error) {
      console.error("Get participants error:", error);
      return [];
    }
  }, []);

  // ✅ DEBUG EXPORT FUNCTION: Get screen share event logs for debugging
  const getScreenShareDebugInfo = useCallback(() => {
    return {
      currentState: {
        isScreenSharing,
        localIsScreenSharing,
        screenSharingParticipant: screenSharingParticipant?.identity,
        screenShareTrack: !!screenShareTrack,
      },
      publishedEvents: screenShareDebugRef.current.publishedEvents.slice(-10),
      subscribedEvents: screenShareDebugRef.current.subscribedEvents.slice(-10),
      stateChanges: screenShareDebugRef.current.stateChanges.slice(-10),
      lastRemoteScreenShare: screenShareDebugRef.current.lastRemoteScreenShare,
      lateJoinerChecks: screenShareDebugRef.current.lateJoinerChecks.slice(-10),
      screenShareParticipants: Array.from(
        screenShareDebugRef.current.screenShareParticipants.entries()
      ),
    };
  }, [
    isScreenSharing,
    localIsScreenSharing,
    screenSharingParticipant,
    screenShareTrack,
  ]);

  // FIXED: Add proper useEffect for cleanup on unmount
  useEffect(() => {
    return () => {
      if (connectionAttemptRef.current || connectionLockRef.current) {
        console.log("⏳ Skipping cleanup - connection in progress");
        return;
      }
        cleanupAllAudioElements();
    };
  }, [cleanupAllAudioElements]);

  // 🔥 CRITICAL FIX: AGGRESSIVE audio state monitoring and enforcement
  useEffect(() => {
    if (!roomRef.current?.localParticipant) return;

    const monitorInterval = setInterval(() => {
      const audioPublication =
        roomRef.current.localParticipant.getTrackPublication(
          Track.Source.Microphone
        );

      if (!audioPublication?.track) return;

      const mediaTrack = audioPublication.track.mediaStreamTrack;
      if (!mediaTrack) return;

      const livekitMuted = audioPublication.track.isMuted;
      const mediaTrackEnabled = mediaTrack.enabled;
      const refSaysMuted = audioMutedRef.current;
      const stateSaysEnabled = isAudioEnabled;

      // 🔥 CRITICAL: Detect ANY desync or audio leak
      const hasDesync =
        livekitMuted !== refSaysMuted ||
        stateSaysEnabled === refSaysMuted ||
        (refSaysMuted && mediaTrackEnabled); // AUDIO LEAK!

      if (hasDesync) {
        console.error("🚨 AUDIO STATE DESYNC/LEAK DETECTED!", {
          livekitMuted,
          mediaTrackEnabled,
          refSaysMuted,
          stateSaysEnabled,
          LEAK: refSaysMuted && mediaTrackEnabled,
        });

        // 🔥 FORCE CORRECT STATE based on ref (source of truth)
        if (refSaysMuted) {
          // Should be MUTED
          if (!livekitMuted) {
            console.log("🔧 FORCE: Muting LiveKit track");
            audioPublication.track.mute();
          }

          if (mediaTrackEnabled) {
            console.log("🔧 FORCE: Disabling MediaStreamTrack (STOPPING LEAK)");
            mediaTrack.enabled = false;
          }

          if (stateSaysEnabled) {
            console.log("🔧 FORCE: Syncing state to muted");
            setIsAudioEnabled(false);
          }
        } else {
          // Should be UNMUTED
          if (livekitMuted) {
            console.log("🔧 FORCE: Unmuting LiveKit track");
            audioPublication.track.unmute();
          }

          if (!mediaTrackEnabled) {
            console.log("🔧 FORCE: Enabling MediaStreamTrack");
            mediaTrack.enabled = true;
          }

          if (!stateSaysEnabled) {
            console.log("🔧 FORCE: Syncing state to unmuted");
            setIsAudioEnabled(true);
          }
        }
      }
    }, 500); // Check every 500ms for aggressive monitoring

    return () => clearInterval(monitorInterval);
  }, [isAudioEnabled]);

  // 🔥 CRITICAL FIX: Monitor and enforce mute state
  useEffect(() => {
    if (!roomRef.current?.localParticipant) return;

    const interval = setInterval(() => {
      const audioPublication =
        roomRef.current.localParticipant.getTrackPublication(
          Track.Source.Microphone
        );

      if (audioPublication?.track) {
        const trackMuted = audioPublication.track.isMuted;
        const refMuted = audioMutedRef.current;
        const stateEnabled = isAudioEnabled;
        const mediaTrackEnabled =
          audioPublication.track.mediaStreamTrack?.enabled;

        // 🔥 CRITICAL: Detect state desync
        if (
          trackMuted !== refMuted ||
          stateEnabled === refMuted ||
          (refMuted && mediaTrackEnabled)
        ) {
          console.error("⚠️ AUDIO STATE DESYNC DETECTED!", {
            trackMuted,
            refMuted,
            stateEnabled,
            mediaTrackEnabled,
            shouldBeMuted: refMuted,
          });

          // 🔥 FORCE CORRECT STATE
          if (refMuted && !trackMuted) {
            console.log("🔧 Forcing audio mute...");
            audioPublication.track.mute();
          }

          if (refMuted && mediaTrackEnabled) {
            console.log("🔧 Forcing MediaStreamTrack disable...");
            audioPublication.track.mediaStreamTrack.enabled = false;
          }

          if (!refMuted && trackMuted) {
            console.log("🔧 Forcing audio unmute...");
            audioPublication.track.unmute();
          }

          // 🔥 Sync state
          setIsAudioEnabled(!refMuted);
        }
      }
    }, 1000); // Check every second

    return () => clearInterval(interval);
  }, [isAudioEnabled]);

  // ✅ CRITICAL: Listen for stream update events
  useEffect(() => {
    const handleLocalStreamUpdate = () => {
      streamCacheRef.current._timestamp = 0;
      setLocalTracks((prev) => ({ ...prev }));
    };

    const handleRemoteStreamUpdate = (event) => {
      const { participantIdentity } = event.detail;
      streamCacheRef.current._timestamp = 0;

      throttledParticipantUpdate((map) => {
        const p = map.get(participantIdentity);
        if (p) {
          map.set(participantIdentity, { ...p });
        }
      });
    };

    window.addEventListener("localStreamUpdated", handleLocalStreamUpdate);
    window.addEventListener("remoteStreamUpdated", handleRemoteStreamUpdate);

    return () => {
      window.removeEventListener("localStreamUpdated", handleLocalStreamUpdate);
      window.removeEventListener(
        "remoteStreamUpdated",
        handleRemoteStreamUpdate
      );
    };
  }, [throttledParticipantUpdate]);

  // 🔥 CRITICAL FIX: Monitor and enforce VIDEO state (similar to audio monitoring)
  useEffect(() => {
    if (!roomRef.current?.localParticipant) return;

    const monitorInterval = setInterval(() => {
      const videoPublication =
        roomRef.current.localParticipant.getTrackPublication(
          Track.Source.Camera
        );

      if (!videoPublication?.track) return;

      const mediaTrack = videoPublication.track.mediaStreamTrack;
      if (!mediaTrack) return;

      const livekitMuted = videoPublication.track.isMuted;
      const mediaTrackEnabled = mediaTrack.enabled;
      const refSaysMuted = videoMutedRef.current;
      const stateSaysEnabled = isVideoEnabled;

      // Detect desync
      const expectedMuted = refSaysMuted;
      const expectedMediaEnabled = !refSaysMuted;

      const hasDesync =
        livekitMuted !== expectedMuted ||
        mediaTrackEnabled !== expectedMediaEnabled ||
        stateSaysEnabled === expectedMuted;

      if (hasDesync) {
        console.warn("⚠️ VIDEO STATE DESYNC DETECTED!", {
          livekitMuted,
          mediaTrackEnabled,
          refSaysMuted,
          stateSaysEnabled,
          expected: {
            livekitMuted: expectedMuted,
            mediaTrackEnabled: expectedMediaEnabled,
            stateSaysEnabled: !expectedMuted,
          },
        });

        // FORCE CORRECT STATE based on ref (source of truth)
        if (refSaysMuted) {
          // Should be MUTED/DISABLED
          if (!livekitMuted) {
            console.log("🔧 FORCE: Muting LiveKit video track");
            videoPublication.track.mute();
          }
          if (mediaTrackEnabled) {
            console.log("🔧 FORCE: Disabling video MediaStreamTrack");
            mediaTrack.enabled = false;
          }
          if (stateSaysEnabled) {
            console.log("🔧 FORCE: Syncing video state to disabled");
            setIsVideoEnabled(false);
          }
        } else {
          // Should be UNMUTED/ENABLED
          if (livekitMuted) {
            console.log("🔧 FORCE: Unmuting LiveKit video track");
            videoPublication.track.unmute();
          }
          if (!mediaTrackEnabled) {
            console.log("🔧 FORCE: Enabling video MediaStreamTrack");
            mediaTrack.enabled = true;
          }
          if (!stateSaysEnabled) {
            console.log("🔧 FORCE: Syncing video state to enabled");
            setIsVideoEnabled(true);
          }
        }
      }
    }, 1000); // Check every second

    return () => clearInterval(monitorInterval);
  }, [isVideoEnabled]);
  // 🔥 CRITICAL: AGGRESSIVE video state monitoring and enforcement
  useEffect(() => {
    // ✅ CRITICAL: Check isConnected state, not just roomRef
    if (!isConnected) return;

    const checkAndFixVideoState = () => {
      // ✅ SAFE null checks
      const room = roomRef.current;
      if (!room || !room.localParticipant) return;

      const videoPublication = room.localParticipant.getTrackPublication(
        Track.Source.Camera
      );

      if (!videoPublication?.track) return;

      const mediaTrack = videoPublication.track.mediaStreamTrack;
      if (!mediaTrack) return;

      const livekitMuted = videoPublication.track.isMuted;
      const mediaTrackEnabled = mediaTrack.enabled;
      const refSaysMuted = videoMutedRef.current;
      const stateSaysEnabled = isVideoEnabled;

      // ========== LEAK DETECTION ==========
      const isLeaking = refSaysMuted && (mediaTrackEnabled || !livekitMuted);

      const isDesynced =
        livekitMuted !== refSaysMuted || stateSaysEnabled === refSaysMuted;

      if (isLeaking) {
        console.error("🚨 VIDEO LEAK DETECTED!", {
          livekitMuted,
          mediaTrackEnabled,
          refSaysMuted,
          stateSaysEnabled,
        });

        // IMMEDIATE FIX
        if (!livekitMuted) {
          console.log("🔧 EMERGENCY: Force muting LiveKit video track");
          videoPublication.track.mute();
        }

        if (mediaTrackEnabled) {
          console.log("🔧 EMERGENCY: Force disabling video MediaStreamTrack");
          mediaTrack.enabled = false;
        }

        if (stateSaysEnabled) {
          console.log("🔧 EMERGENCY: Force setting video state to disabled");
          setIsVideoEnabled(false);
        }

        console.log("✅ Video leak FIXED");
        return;
      }

      if (isDesynced) {
        console.warn("⚠️ Video state desync detected");

        // Fix based on ref (source of truth)
        if (refSaysMuted) {
          if (!livekitMuted) videoPublication.track.mute();
          if (mediaTrackEnabled) mediaTrack.enabled = false;
          if (stateSaysEnabled) setIsVideoEnabled(false);
        } else {
          if (livekitMuted) videoPublication.track.unmute();
          if (!mediaTrackEnabled) mediaTrack.enabled = true;
          if (!stateSaysEnabled) setIsVideoEnabled(true);
        }
      }
    };

    // Run every 300ms for aggressive monitoring
    const monitorInterval = setInterval(checkAndFixVideoState, 300);

    // Also check on tab visibility change
    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        setTimeout(checkAndFixVideoState, 100);
      }
    };
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      clearInterval(monitorInterval);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [isConnected, isVideoEnabled]); // ✅ Depend on isConnected

  // 🔥 CRITICAL: Enforce muted state immediately after connection
  useEffect(() => {
    // ✅ CRITICAL: Check isConnected first
    if (!isConnected) return;

    const enforceInitialMuteState = async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // ✅ SAFE null check
      const room = roomRef.current;
      if (!room || !room.localParticipant) return;

      const videoPublication = room.localParticipant.getTrackPublication(
        Track.Source.Camera
      );
      const audioPublication = room.localParticipant.getTrackPublication(
        Track.Source.Microphone
      );

      console.log("🔒 Enforcing initial mute state for all tracks...");

      // Enforce VIDEO mute
      if (videoPublication?.track && videoMutedRef.current) {
        const videoMediaTrack = videoPublication.track.mediaStreamTrack;

        if (!videoPublication.track.isMuted) {
          await videoPublication.track.mute();
          console.log("✅ Initial: Video LiveKit track force muted");
        }

        if (videoMediaTrack && videoMediaTrack.enabled) {
          videoMediaTrack.enabled = false;
          console.log("✅ Initial: Video MediaStreamTrack force disabled");
        }

        setIsVideoEnabled(false);
      }

      // Enforce AUDIO mute
      if (audioPublication?.track && audioMutedRef.current) {
        const audioMediaTrack = audioPublication.track.mediaStreamTrack;

        if (!audioPublication.track.isMuted) {
          await audioPublication.track.mute();
          console.log("✅ Initial: Audio LiveKit track force muted");
        }

        if (audioMediaTrack && audioMediaTrack.enabled) {
          audioMediaTrack.enabled = false;
          console.log("✅ Initial: Audio MediaStreamTrack force disabled");
        }

        setIsAudioEnabled(false);
      }

      console.log("✅ Initial mute state enforced for all tracks");
    };

    enforceInitialMuteState();

    // Backup enforcement at 3 seconds
    const backupTimeout = setTimeout(enforceInitialMuteState, 3000);

    return () => clearTimeout(backupTimeout);
  }, [isConnected]); // ✅ Depend on isConnected

  // 🔥 CRITICAL: Enforce muted state immediately after connection
  useEffect(() => {
    if (!isConnected || !roomRef.current?.localParticipant) return;

    // Wait for tracks to be published, then enforce mute
    const enforceInitialMute = async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000));

      const audioPublication =
        roomRef.current?.localParticipant?.getTrackPublication(
          Track.Source.Microphone
        );

      if (audioPublication?.track) {
        const mediaTrack = audioPublication.track.mediaStreamTrack;

        console.log("🔒 Enforcing initial audio mute state...");

        // Force mute if ref says muted
        if (audioMutedRef.current) {
          if (!audioPublication.track.isMuted) {
            await audioPublication.track.mute();
            console.log("✅ Initial: LiveKit track force muted");
          }

          if (mediaTrack && mediaTrack.enabled) {
            mediaTrack.enabled = false;
            console.log("✅ Initial: MediaStreamTrack force disabled");
          }

          setIsAudioEnabled(false);
        }

        console.log("✅ Initial audio state enforced:", {
          livekitMuted: audioPublication.track.isMuted,
          mediaTrackEnabled: mediaTrack?.enabled,
          refMuted: audioMutedRef.current,
          LEAK_CHECK:
            audioMutedRef.current && mediaTrack?.enabled
              ? "🚨 LEAK!"
              : "✅ NO LEAK",
        });
      }
    };

    enforceInitialMute();

    // Also run after 3 seconds as backup
    const backupTimeout = setTimeout(enforceInitialMute, 3000);

    return () => clearTimeout(backupTimeout);
  }, [isConnected]);
  useEffect(() => {
    if (!room) return;

    const debugScreenShareAudio = () => {
      console.log("========== SCREEN SHARE AUDIO DEBUG ==========");
      console.log("Remote participants:", room.remoteParticipants.size);
      console.log("isScreenSharing:", isScreenSharing);
      console.log(
        "screenSharingParticipant:",
        screenSharingParticipant?.identity
      );

      // Check all remote participants for screen share audio
      room.remoteParticipants.forEach((participant) => {
        participant.trackPublications.forEach((pub) => {
          if (pub.source === Track.Source.ScreenShareAudio) {
            console.log("🔊 Screen Share Audio Publication found:", {
              participant: participant.identity,
              participantName: participant.name,
              isSubscribed: pub.isSubscribed,
              isEnabled: pub.isEnabled,
              hasTrack: !!pub.track,
              trackKind: pub.track?.kind,
              trackMuted: pub.track?.isMuted,
              mediaStreamTrack: !!pub.track?.mediaStreamTrack,
              mediaTrackState: pub.track?.mediaStreamTrack?.readyState,
              mediaTrackEnabled: pub.track?.mediaStreamTrack?.enabled,
            });
          }
        });
      });

      // Check local participant for screen share audio (publishing side)
      if (room.localParticipant) {
        const localScreenAudio = room.localParticipant.getTrackPublication(
          Track.Source.ScreenShareAudio
        );
        if (localScreenAudio) {
          console.log("🔊 LOCAL Screen Share Audio Publication:", {
            hasPublication: true,
            hasTrack: !!localScreenAudio.track,
            trackKind: localScreenAudio.track?.kind,
            isMuted: localScreenAudio.track?.isMuted,
            isEnabled: localScreenAudio.isEnabled,
          });
        } else {
          console.log("🔊 LOCAL Screen Share Audio: NOT PUBLISHING");
        }
      }

      // Check audio elements in DOM
      console.log(
        "Audio elements in ref:",
        remoteAudioElementsRef.current.size
      );
      remoteAudioElementsRef.current.forEach((el, key) => {
        console.log(`Audio element [${key}]:`, {
          paused: el.paused,
          muted: el.muted,
          volume: el.volume,
          readyState: el.readyState,
          currentTime: el.currentTime,
          srcObject: !!el.srcObject,
          inDOM: !!el.parentNode,
        });
      });

      // Also check DOM directly for any screen share audio elements
      const screenAudioElements = document.querySelectorAll(
        '[data-livekit-screen-audio="true"]'
      );
      console.log(
        "Screen share audio elements in DOM:",
        screenAudioElements.length
      );
      screenAudioElements.forEach((el, index) => {
        console.log(`DOM Audio Element ${index}:`, {
          paused: el.paused,
          muted: el.muted,
          volume: el.volume,
          readyState: el.readyState,
        });
      });

      console.log("==============================================");
    };

    // Run debug every 5 seconds when screen sharing is active
    const debugInterval = setInterval(() => {
      if (isScreenSharing || screenSharingParticipant) {
        debugScreenShareAudio();
      }
    }, 5000);

    // Also run once immediately when screen sharing starts
    if (isScreenSharing || screenSharingParticipant) {
      setTimeout(debugScreenShareAudio, 1000);
    }

    return () => clearInterval(debugInterval);
  }, [room, isScreenSharing, screenSharingParticipant]);
  // REPLACE the return statement at the end of useLiveKit.js hook
  return {
    // Connection
    connectToRoom,
    connectToMeeting: connectToRoom,
    disconnectFromRoom,
    disconnect: disconnectFromRoom,
    isConnected,
    connected: isConnected,
    isConnecting,
    connectionState,
    error,
    room,

    // Queue
    queueStatus,
    checkConnectionQueue,
    joinMeetingWithQueue,
    waitForQueueTurn,

    // Meeting Control
    endMeetingForEveryone,
    meetingEnded,

    startRecording,
    stopRecording,

    // Participants
    participants,
    localParticipant,
    remoteParticipants,
    getParticipantsList,
    participantCount,
    maxParticipants,

    // Performance
    performanceMode,
    meetingEnded,
    // Media Controls
    enableAudio,
    disableAudio,
    toggleAudio,
    enableVideo,
    toggleVideo,
    startScreenShare,
    stopScreenShare,
    toggleScreenShare,

    // Speaker Controls
    muteSpeaker,
    unmuteSpeaker,
    toggleSpeaker,
    isSpeakerMuted,

    // Screen Share Permission System
    screenSharePermissions,
    screenShareRequests,
    currentScreenShareRequest,
    requestScreenSharePermission,
    approveScreenShareRequest,
    denyScreenShareRequest,

    // Media State
    localTracks,
    isAudioEnabled,
    isVideoEnabled,
    isScreenSharing,
    isSpeaking,
    screenShareTrack,

    // Screen Share State
    screenSharingParticipant,
    localIsScreenSharing,
    getScreenShareStream,
    verifyScreenShareState,
    updateCoHostStatus,

    // Communication
    sendChatMessage,
    sendReaction,
    sendMessage,
    checkExistingScreenShares,
    messages,
    reactions,

    handleAudioDeviceChange,
    refreshAudioTrack,
    monitorAudioTrackHealth,
    // Meeting Info
    meetingInfo,
    roomMetadata,

    // CRITICAL: Add verification function for recording
    verifyTracksPublished,

    // ✅ DEBUG: Screen share event logging
    getScreenShareDebugInfo,
    logScreenShareEvent,
    forceStopParticipantScreenShare,
    // Screen share state tracking for UI
    screenShareCheckComplete,
    lastScreenShareCheckTime,

    // Legacy compatibility
    connecting: isConnecting,
    connectionError: error,
    addEventListener: () => {},
    removeEventListener: () => {},

    remoteVideoStreams,
    remoteVideoStreamsRef: remoteVideoStreamsRef.current,

    // Helper to get video stream
    getRemoteVideoStream: useCallback((participantId) => {
      if (!participantId) return null;

      const keysToTry = [
        participantId,
        participantId?.toString(),
        `user_${participantId}`,
        `participant_${participantId}`,
      ];

      for (const key of keysToTry) {
        const stream = remoteVideoStreamsRef.current.get(key);
        if (stream) return stream;
      }

      return null;
    }, []),

    remoteParticipants: remoteParticipantsState,

    // Helper function to get remote participant by ID
    getRemoteParticipant: useCallback((participantId) => {
      return (
        remoteParticipantsRef.current.get(participantId) ||
        remoteParticipantsRef.current.get(`user_${participantId}`)
      );
    }, []),

    // Sync function (optional, for manual refresh)
    syncRemoteParticipants,
  };
};

export { useLiveKit };
export default useLiveKit;
