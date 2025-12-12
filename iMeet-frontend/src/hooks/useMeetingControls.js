// src/hooks/useMeetingControls.js - Complete Meeting Controls Hook
import { useState, useCallback, useRef, useMemo } from "react";
import { throttle } from "lodash";
import { DataPacket_Kind } from "livekit-client";
import { Track } from "livekit-client";

const PERFORMANCE_CONFIG = {
  THROTTLE_DELAY: 200,
  INITIAL_MEDIA_DELAY: 100,
};
export const useMeetingControls = ({
  livekitToggleAudio,
  livekitToggleVideo,
  livekitStartScreenShare,
  livekitStopScreenShare,
  livekitLocalIsScreenSharing,
  enableAudio,
  enableVideo,
  isConnectionReady,
  onToggleAudio: propOnToggleAudio,
  onToggleVideo: propOnToggleVideo,
  showNotificationMessage,
  canShareScreenDirectly,
  hasHostPrivileges,
  meetingSettings,
  screenSharePermissions,
  room,
  forceStopParticipantScreenShare, // ✅ ADD THIS
  isHost, // ✅ ADD THIS
  isCoHost, // ✅ ADD THIS
  coHostPrivilegesActive, // ✅ ADD THIS
  currentUser, // ✅ ADD THIS
  enhancedScreenShareData, // ✅ ADD THIS
}) => {
  // State
  const [audioEnabled, setAudioEnabled] = useState(false);
  const [videoEnabled, setVideoEnabled] = useState(false);
  const [screenSharing, setScreenSharing] = useState(false);
  const [showScreenShareWaiting, setShowScreenShareWaiting] = useState(false);
  const [screenShareWaitingTimeout, setScreenShareWaitingTimeout] =
    useState(null);
  const [audioInitStatus, setAudioInitStatus] = useState("");
  const [showAudioStatus, setShowAudioStatus] = useState(false);
  const [showScreenShareStopped, setShowScreenShareStopped] = useState(false);
  const [screenShareStoppedBy, setScreenShareStoppedBy] = useState(null);

  // Refs
  const audioInitializedRef = useRef(false);
  const videoInitializedRef = useRef(false);

  // Show audio init status
  const showAudioInitStatus = useCallback((status) => {
    setAudioInitStatus(status);
    setShowAudioStatus(true);
    setTimeout(() => setShowAudioStatus(false), 3000);
  }, []);

  const handleToggleAudio = useMemo(
    () =>
      throttle(async () => {
        try {
          if (!isConnectionReady) {
            showNotificationMessage(
              "Please wait for connection to establish",
              "warning"
            );
            return;
          }

          if (
            !audioEnabled &&
            enableAudio &&
            typeof enableAudio === "function"
          ) {
            showAudioInitStatus("Enabling microphone...");
            const result = await enableAudio();
            if (result) {
              setAudioEnabled(true);
              audioInitializedRef.current = true;
              showAudioInitStatus("Microphone enabled");
              showNotificationMessage("Microphone unmuted");

              // 🔥 CRITICAL FIX: Broadcast state change immediately
              if (room?.localParticipant && currentUser) {
                const encoder = new TextEncoder();
                const stateData = encoder.encode(
                  JSON.stringify({
                    type: "track_state_update",
                    user_id: currentUser.id,
                    track_kind: Track.Kind.Audio,
                    enabled: true,
                    timestamp: Date.now(),
                  })
                );

                room.localParticipant.publishData(
                  stateData,
                  DataPacket_Kind.RELIABLE
                );
              }

              return;
            }
          }

          if (livekitToggleAudio && typeof livekitToggleAudio === "function") {
            showAudioInitStatus(audioEnabled ? "Muting..." : "Unmuting...");

            console.log("🎤 MeetingControls: Toggling audio - Before:", {
              audioEnabled,
              audioInitialized: audioInitializedRef.current,
            });

            const newState = await livekitToggleAudio();

            console.log("🎤 MeetingControls: Toggling audio - After:", {
              newState,
              audioEnabled: newState,
            });

            setAudioEnabled(newState);
            audioInitializedRef.current = true;
            showAudioInitStatus(
              newState ? "Microphone unmuted" : "Microphone muted"
            );
            showNotificationMessage(
              newState ? "Microphone unmuted" : "Microphone muted"
            );

            // 🔥 CRITICAL FIX: Broadcast state change immediately
            if (room?.localParticipant && currentUser) {
              const encoder = new TextEncoder();
              const stateData = encoder.encode(
                JSON.stringify({
                  type: "track_state_update",
                  user_id: currentUser.id,
                  track_kind: Track.Kind.Audio,
                  enabled: newState,
                  muted: !newState,
                  timestamp: Date.now(),
                })
              );

              try {
                await room.localParticipant.publishData(
                  stateData,
                  DataPacket_Kind.RELIABLE
                );
                console.log("✅ Audio state broadcasted:", {
                  enabled: newState,
                  muted: !newState,
                });
              } catch (err) {
                console.error("❌ Failed to broadcast audio state:", err);
              }
            }

            return;
          }
        } catch (error) {
          console.error("Audio toggle error:", error);
          showAudioInitStatus("Audio error");
          showNotificationMessage(
            `Audio toggle failed: ${error.message}`,
            "error"
          );
        }

        if (propOnToggleAudio) {
          const newState = propOnToggleAudio();
          setAudioEnabled(
            typeof newState === "boolean" ? newState : !audioEnabled
          );
        } else {
          setAudioEnabled(!audioEnabled);
        }
      }, PERFORMANCE_CONFIG.THROTTLE_DELAY),
    [
      livekitToggleAudio,
      enableAudio,
      isConnectionReady,
      propOnToggleAudio,
      audioEnabled,
      showNotificationMessage,
      showAudioInitStatus,
    ]
  );

  // Video toggle with track creation only when turning on
  const handleToggleVideo = useMemo(
    () =>
      throttle(async () => {
        try {
          if (!isConnectionReady) {
            showNotificationMessage(
              "Please wait for connection to establish",
              "warning"
            );
            return;
          }

          if (
            !videoEnabled &&
            enableVideo &&
            typeof enableVideo === "function"
          ) {
            const result = await enableVideo();
            if (result) {
              setVideoEnabled(true);
              videoInitializedRef.current = true;
              showNotificationMessage("Camera turned on");
              return;
            }
          }

          if (livekitToggleVideo) {
            const newState = await livekitToggleVideo();
            setVideoEnabled(newState);
            videoInitializedRef.current = true;
            showNotificationMessage(
              newState ? "Camera turned on" : "Camera turned off"
            );

            // 🔥 CRITICAL FIX: Broadcast video state change immediately
            if (room?.localParticipant && currentUser) {
              const encoder = new TextEncoder();
              const stateData = encoder.encode(
                JSON.stringify({
                  type: "track_state_update",
                  user_id: currentUser.id,
                  track_kind: Track.Kind.Video,
                  enabled: newState,
                  timestamp: Date.now(),
                })
              );

              room.localParticipant.publishData(
                stateData,
                DataPacket_Kind.RELIABLE
              );
            }

            return;
          }
        } catch (error) {
          console.error("Video toggle error:", error);
          showNotificationMessage(
            `Video toggle failed: ${error.message}`,
            "error"
          );
        }

        if (propOnToggleVideo) {
          const newState = propOnToggleVideo();
          setVideoEnabled(
            typeof newState === "boolean" ? newState : !videoEnabled
          );
        } else {
          setVideoEnabled(!videoEnabled);
        }
      }, PERFORMANCE_CONFIG.THROTTLE_DELAY),
    [
      livekitToggleVideo,
      enableVideo,
      isConnectionReady,
      propOnToggleVideo,
      videoEnabled,
      showNotificationMessage,
    ]
  );

  const handleForceStopScreenShare = async (participant) => {
    if (!room || !forceStopParticipantScreenShare) return;

    try {
      // Stop the screen share
      await forceStopParticipantScreenShare(participant);

      // Send data channel message to notify participants
      if (room.localParticipant) {
        const encoder = new TextEncoder();
        const stopData = encoder.encode(
          JSON.stringify({
            type: "force_stop_screen_share",
            target_user_id: participant.user_id || participant.id,
            target_user_name:
              participant.name || participant.displayName || "Participant",
            stopped_by_id: currentUser.id,
            stopped_by_name:
              currentUser.name || currentUser.full_name || "Host",
            reason: "Stopped by host",
            timestamp: Date.now(),
          })
        );

        await room.localParticipant.publishData(
          stopData,
          DataPacket_Kind.RELIABLE
        );
      }

      // Trigger the callback to show dialog for host
      if (onScreenShareStopped) {
        onScreenShareStopped({
          stoppedBy: currentUser,
          stoppedParticipant: participant,
          isCurrentUser: false,
          reason: "Stopped by host",
        });
      }

      showNotificationMessage(
        `Stopped screen sharing for ${
          participant.name || participant.displayName
        }`,
        "success"
      );
    } catch (error) {
      console.error("Error stopping screen share:", error);
      showNotificationMessage("Failed to stop screen sharing", "error");
    }
  };

  const handleToggleScreenShare = useMemo(
    () =>
      throttle(async () => {
        if (!isConnectionReady) {
          showNotificationMessage(
            "Not connected to meeting. Please wait for connection to establish.",
            "error"
          );
          return;
        }

        // ✅ CRITICAL: Check if someone else is already sharing
        const { stream: activeScreenShare, sharer: activeSharer } =
          enhancedScreenShareData || {};

        if (activeScreenShare && activeSharer) {
          const isCurrentUserSharing =
            activeSharer.isLocal ||
            activeSharer.user_id?.toString() === currentUser?.id?.toString();

          // === CASE 1: Someone else is sharing and current user is NOT host/co-host ===
          if (!isCurrentUserSharing && !hasHostPrivileges) {
            showNotificationMessage(
              `${
                activeSharer.name || "Someone"
              } is already sharing their screen`,
              "warning"
            );
            return; // ✅ EXIT - button should already be disabled in UI
          }

          // === CASE 2: Host/Co-host wants to stop someone else's screen share ===
          if (!isCurrentUserSharing && hasHostPrivileges) {
            console.log("🛡️ Host/Co-host stopping participant's screen share");

            const targetIdentity =
              activeSharer.connection_id ||
              activeSharer.participant_id ||
              `user_${activeSharer.user_id}`;

            try {
              await forceStopParticipantScreenShare(targetIdentity);

              // Send broadcast message
              if (room?.localParticipant) {
                const encoder = new TextEncoder();
                const stopData = encoder.encode(
                  JSON.stringify({
                    type: "force_stop_screen_share",
                    target_identity: targetIdentity,
                    target_user_id: activeSharer.user_id,
                    stopped_by_id: currentUser?.id,
                    stopped_by_name:
                      currentUser?.full_name || currentUser?.name || "Host",
                    timestamp: Date.now(),
                  })
                );

                await room.localParticipant.publishData(
                  stopData,
                  DataPacket_Kind.RELIABLE
                );
              }

              setScreenSharing(false);
              setShowScreenShareStopped(true);
              setScreenShareStoppedBy({
                name: currentUser?.full_name || currentUser?.name || "Host",
                user_id: currentUser?.id,
              });

              showNotificationMessage(
                `Stopped ${activeSharer.name || "participant"}'s screen share`,
                "success"
              );
            } catch (error) {
              console.error("❌ Failed to stop screen share:", error);
              showNotificationMessage("Failed to stop screen share", "error");
            }

            return;
          }

          // === CASE 3: Current user is sharing - stop their own share ===
          if (isCurrentUserSharing) {
            console.log("🛑 Current user stopping their own screen share");

            try {
              await livekitStopScreenShare();
              setScreenSharing(false);
              setShowScreenShareWaiting(false);
              showNotificationMessage("Screen sharing stopped", "info");
            } catch (error) {
              console.error("❌ Failed to stop own screen share:", error);
              showNotificationMessage("Failed to stop screen sharing", "error");
            }

            return;
          }
        }

        // === CASE 4: No one is sharing - start new screen share ===
        console.log("▶️ Starting new screen share...");

        try {
          // Check if needs permission
          if (
            !canShareScreenDirectly &&
            meetingSettings.screenShareRequiresApproval
          ) {
            setShowScreenShareWaiting(true);
            showNotificationMessage(
              "Requesting screen share permission from host...",
              "info"
            );
          }

          // Show audio guidance
          showNotificationMessage(
            '💡 TIP: For YouTube/Spotify audio, select "Chrome Tab" (not Window/Screen)',
            "info"
          );

          const result = await livekitStartScreenShare();

          if (result?.success) {
            setScreenSharing(true);
            setShowScreenShareWaiting(false);

            const roleMessage = isHost ? "Host" : isCoHost ? "Co-Host" : "Your";
            showNotificationMessage(
              result.hasSystemAudio
                ? `${roleMessage} screen sharing with audio started!`
                : `${roleMessage} screen sharing started`,
              "success"
            );
          } else if (result?.needsPermission) {
            setShowScreenShareWaiting(true);
          } else {
            setShowScreenShareWaiting(false);
            setScreenSharing(false);
          }
        } catch (error) {
          console.error("❌ Screen share start error:", error);
          setShowScreenShareWaiting(false);
          setScreenSharing(false);
          showNotificationMessage(
            `Screen share failed: ${error.message}`,
            "error"
          );
        }
      }, PERFORMANCE_CONFIG.THROTTLE_DELAY),
    [
      isConnectionReady,
      livekitLocalIsScreenSharing,
      screenSharing,
      enhancedScreenShareData,
      currentUser,
      hasHostPrivileges,
      canShareScreenDirectly,
      isHost,
      isCoHost,
      meetingSettings?.screenShareRequiresApproval,
      screenSharePermissions,
      room,
      livekitStopScreenShare,
      livekitStartScreenShare,
      forceStopParticipantScreenShare,
      showNotificationMessage,
      setScreenSharing,
      setShowScreenShareWaiting,
      setShowScreenShareStopped,
      setScreenShareStoppedBy,
    ]
  );
  // Camera toggle for attendance
  const handleCameraToggle = useCallback(
    async (enabled) => {
      try {
        if (enabled) {
          // Enable camera
          if (enableVideo && typeof enableVideo === "function") {
            const result = await enableVideo();
            if (result) {
              setVideoEnabled(true);
              videoInitializedRef.current = true;
              showNotificationMessage("Camera enabled for attendance tracking");
              return Promise.resolve();
            }
          }

          if (livekitToggleVideo) {
            const newState = await livekitToggleVideo();
            if (newState) {
              setVideoEnabled(true);
              videoInitializedRef.current = true;
              showNotificationMessage("Camera enabled for attendance tracking");
              return Promise.resolve();
            }
          }

          setVideoEnabled(true);
          return Promise.resolve();
        } else {
          // Disable camera
          if (livekitToggleVideo) {
            const newState = await livekitToggleVideo();
            setVideoEnabled(newState);
            showNotificationMessage("Camera disabled for attendance break");
            return Promise.resolve();
          }

          setVideoEnabled(false);
          return Promise.resolve();
        }
      } catch (error) {
        console.error("❌ Camera toggle failed:", error);
        showNotificationMessage(
          `Camera toggle failed: ${error.message}`,
          "error"
        );
        throw error;
      }
    },
    [livekitToggleVideo, enableVideo, showNotificationMessage]
  );

  return {
    // State
    audioEnabled,
    videoEnabled,
    screenSharing,
    showScreenShareWaiting,
    screenShareWaitingTimeout,
    audioInitStatus,
    showAudioStatus,
    showScreenShareStopped, // ✅ ADD THIS
    screenShareStoppedBy, // ✅ ADD THIS

    // Setters
    setAudioEnabled,
    setVideoEnabled,
    setScreenSharing,
    setShowScreenShareWaiting,
    setScreenShareWaitingTimeout,
    setShowScreenShareStopped, // ✅ ADD THIS
    setScreenShareStoppedBy, // ✅ ADD THIS

    // Refs
    audioInitializedRef,
    videoInitializedRef,

    // Handlers
    handleToggleAudio,
    handleToggleVideo,
    handleToggleScreenShare,
    handleCameraToggle,
    showAudioInitStatus,
  };
};
