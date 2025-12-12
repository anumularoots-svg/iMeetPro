// src/hooks/useParticipantActions.js - Complete Participant Control Actions Hook
import { useCallback } from 'react';
import { DataPacket_Kind } from 'livekit-client';

export const useParticipantActions = ({
  room,
  currentUser,
  hasHostPrivileges,
  allParticipants,
  getParticipantDisplayName,
  showNotificationMessage,
  loadLiveParticipants,
  setLiveParticipants,
  setParticipantStats,
}) => {
  // Mute participant audio
  const handleMuteParticipant = useCallback(
    async (participantId) => {
      if (!hasHostPrivileges) {
        showNotificationMessage("Only hosts and co-hosts can mute participants", "error");
        return { success: false };
      }

      try {
        console.log("🔇 Muting participant:", participantId);

        const participant = allParticipants.find(
          p => (p.id || p.user_id || p.User_ID)?.toString() === participantId?.toString()
        );

        if (!participant) {
          throw new Error("Participant not found");
        }

        const userName = getParticipantDisplayName(participant);

        // INSTANT UI UPDATE - Optimistic update
        setLiveParticipants(prev =>
          prev.map(p => {
            const pId = (p.id || p.user_id || p.User_ID)?.toString();
            if (pId === participantId?.toString()) {
              return {
                ...p,
                audio_enabled: false,
                isAudioEnabled: false
              };
            }
            return p;
          })
        );

        // Send mute command via LiveKit
        if (room && room.localParticipant) {
          const encoder = new TextEncoder();
          const muteData = encoder.encode(
            JSON.stringify({
              type: "force_mute_audio",
              target_user_id: participantId,
              target_user_name: userName,
              muted_by: currentUser.id,
              muted_by_name: getParticipantDisplayName(currentUser),
              timestamp: Date.now(),
            })
          );

          await room.localParticipant.publishData(muteData, DataPacket_Kind.RELIABLE);
          showNotificationMessage(`Muted ${userName}'s microphone`, "success");

          return { success: true };
        }

        throw new Error("Room not available");
      } catch (error) {
        console.error("❌ Mute failed:", error);
        showNotificationMessage(`Failed to mute: ${error.message}`, "error");

        // Revert optimistic update on error
        await loadLiveParticipants(true);
        return { success: false };
      }
    },
    [hasHostPrivileges, room, currentUser, allParticipants, getParticipantDisplayName, showNotificationMessage, loadLiveParticipants, setLiveParticipants]
  );

  // Unmute participant audio
  const handleUnmuteParticipant = useCallback(
    async (participantId) => {
      if (!hasHostPrivileges) {
        showNotificationMessage("Only hosts and co-hosts can unmute participants", "error");
        return { success: false };
      }

      try {
        console.log("🔊 Allowing unmute for participant:", participantId);

        const participant = allParticipants.find(
          p => (p.id || p.user_id || p.User_ID)?.toString() === participantId?.toString()
        );

        if (!participant) {
          throw new Error("Participant not found");
        }

        const userName = getParticipantDisplayName(participant);

        // Send unmute permission via LiveKit
        if (room && room.localParticipant) {
          const encoder = new TextEncoder();
          const unmuteData = encoder.encode(
            JSON.stringify({
              type: "allow_unmute_audio",
              target_user_id: participantId,
              target_user_name: userName,
              unmuted_by: currentUser.id,
              unmuted_by_name: getParticipantDisplayName(currentUser),
              timestamp: Date.now(),
            })
          );

          await room.localParticipant.publishData(unmuteData, DataPacket_Kind.RELIABLE);
          showNotificationMessage(`Allowed ${userName} to unmute`, "success");

          return { success: true };
        }

        throw new Error("Room not available");
      } catch (error) {
        console.error("❌ Unmute permission failed:", error);
        showNotificationMessage(`Failed to allow unmute: ${error.message}`, "error");
        return { success: false };
      }
    },
    [hasHostPrivileges, room, currentUser, allParticipants, getParticipantDisplayName, showNotificationMessage]
  );

  // Mute participant video
  const handleMuteVideo = useCallback(
    async (participantId) => {
      if (!hasHostPrivileges) {
        showNotificationMessage("Only hosts and co-hosts can control video", "error");
        return { success: false };
      }

      try {
        console.log("📹 Turning off participant video:", participantId);

        const participant = allParticipants.find(
          p => (p.id || p.user_id || p.User_ID)?.toString() === participantId?.toString()
        );

        if (!participant) {
          throw new Error("Participant not found");
        }

        const userName = getParticipantDisplayName(participant);

        // INSTANT UI UPDATE
        setLiveParticipants(prev =>
          prev.map(p => {
            const pId = (p.id || p.user_id || p.User_ID)?.toString();
            if (pId === participantId?.toString()) {
              return {
                ...p,
                video_enabled: false,
                isVideoEnabled: false
              };
            }
            return p;
          })
        );

        // Send video mute command via LiveKit
        if (room && room.localParticipant) {
          const encoder = new TextEncoder();
          const muteVideoData = encoder.encode(
            JSON.stringify({
              type: "force_mute_video",
              target_user_id: participantId,
              target_user_name: userName,
              muted_by: currentUser.id,
              muted_by_name: getParticipantDisplayName(currentUser),
              timestamp: Date.now(),
            })
          );

          await room.localParticipant.publishData(muteVideoData, DataPacket_Kind.RELIABLE);
          showNotificationMessage(`Turned off ${userName}'s camera`, "success");

          return { success: true };
        }

        throw new Error("Room not available");
      } catch (error) {
        console.error("❌ Video mute failed:", error);
        showNotificationMessage(`Failed to turn off video: ${error.message}`, "error");

        // Revert optimistic update
        await loadLiveParticipants(true);
        return { success: false };
      }
    },
    [hasHostPrivileges, room, currentUser, allParticipants, getParticipantDisplayName, showNotificationMessage, loadLiveParticipants, setLiveParticipants]
  );

  // Unmute participant video
  const handleUnmuteVideo = useCallback(
    async (participantId) => {
      if (!hasHostPrivileges) {
        showNotificationMessage("Only hosts and co-hosts can control video", "error");
        return { success: false };
      }

      try {
        console.log("📹 Allowing camera for participant:", participantId);

        const participant = allParticipants.find(
          p => (p.id || p.user_id || p.User_ID)?.toString() === participantId?.toString()
        );

        if (!participant) {
          throw new Error("Participant not found");
        }

        const userName = getParticipantDisplayName(participant);

        // Send video permission via LiveKit
        if (room && room.localParticipant) {
          const encoder = new TextEncoder();
          const unmuteVideoData = encoder.encode(
            JSON.stringify({
              type: "allow_unmute_video",
              target_user_id: participantId,
              target_user_name: userName,
              unmuted_by: currentUser.id,
              unmuted_by_name: getParticipantDisplayName(currentUser),
              timestamp: Date.now(),
            })
          );

          await room.localParticipant.publishData(unmuteVideoData, DataPacket_Kind.RELIABLE);
          showNotificationMessage(`Allowed ${userName} to turn on camera`, "success");

          return { success: true };
        }

        throw new Error("Room not available");
      } catch (error) {
        console.error("❌ Video permission failed:", error);
        showNotificationMessage(`Failed to allow camera: ${error.message}`, "error");
        return { success: false };
      }
    },
    [hasHostPrivileges, room, currentUser, allParticipants, getParticipantDisplayName, showNotificationMessage]
  );

  // Remove participant from meeting
  const handleRemoveParticipant = useCallback(
    async (participantData) => {
      if (!hasHostPrivileges) {
        showNotificationMessage(
          "Only hosts and co-hosts can remove participants",
          "error"
        );
        return { success: false, error: "Insufficient permissions" };
      }

      try {
        const userId =
          participantData.userId ||
          participantData.user_id ||
          participantData.participantId ||
          participantData.id ||
          (participantData.participant &&
            (participantData.participant.user_id ||
              participantData.participant.User_ID ||
              participantData.participant.id));

        const participant = participantData.participant || participantData;
        const userName =
          participant?.displayName ||
          participant?.name ||
          participant?.full_name ||
          participant?.Full_Name ||
          `User ${userId}`;

        if (userId?.toString() === currentUser?.id?.toString()) {
          showNotificationMessage(
            "You cannot remove yourself from the meeting",
            "error"
          );
          return { success: false, error: "Cannot remove self" };
        }

        if (participant.role === "host" || participant.isHost) {
          showNotificationMessage(
            "Cannot remove another host from the meeting",
            "error"
          );
          return { success: false, error: "Cannot remove host" };
        }

        // STEP 1: IMMEDIATE UI UPDATE
        setLiveParticipants((prev) => {
          const updated = prev.filter((p) => {
            const pUserId = p.User_ID || p.user_id || p.ID;
            return pUserId?.toString() !== userId?.toString();
          });
          return updated;
        });

        setParticipantStats((prev) => ({
          ...prev,
          total: Math.max(0, prev.total - 1),
          active: Math.max(0, prev.active - 1),
        }));

        // STEP 2: GLOBAL EVENT DISPATCH
        window.dispatchEvent(
          new CustomEvent("participantRemoved", {
            detail: {
              removedUserId: userId,
              removedUserName: userName,
              removedBy: currentUser.id,
              removedByName: getParticipantDisplayName(currentUser),
              timestamp: Date.now(),
            },
          })
        );

        // STEP 3: Show feedback
        showNotificationMessage(
          `Removing ${userName} from the meeting...`,
          "info"
        );

        // STEP 4: Send LiveKit disconnection signal
        if (room && room.localParticipant) {
          try {
            const encoder = new TextEncoder();
            const removalData = encoder.encode(
              JSON.stringify({
                type: "participant_removed",
                target_user_id: userId,
                target_user_name: userName,
                removed_by: currentUser.id,
                removed_by_name: getParticipantDisplayName(currentUser),
                reason: "removed_by_host_or_cohost",
                timestamp: Date.now(),
                force_disconnect: true,
              })
            );

            room.localParticipant.publishData(
              removalData,
              DataPacket_Kind.RELIABLE
            );
          } catch (signalError) {
            console.error("Failed to send removal signal:", signalError);
          }
        }

        showNotificationMessage(
          `${userName} has been removed from the meeting`,
          "success"
        );

        // STEP 5: Force refresh after delay
        setTimeout(async () => {
          await loadLiveParticipants(true);
        }, 1000);

        return { success: true };
      } catch (error) {
        console.error("Failed to remove participant:", error);
        await loadLiveParticipants(true);

        const errorMessage =
          error.response?.data?.error ||
          error.message ||
          "Failed to remove participant";
        showNotificationMessage(
          `Failed to remove participant: ${errorMessage}`,
          "error"
        );
        return { success: false, error: errorMessage };
      }
    },
    [
      hasHostPrivileges,
      currentUser?.id,
      showNotificationMessage,
      loadLiveParticipants,
      room,
      getParticipantDisplayName,
      setLiveParticipants,
      setParticipantStats,
    ]
  );

  return {
    handleMuteParticipant,
    handleUnmuteParticipant,
    handleMuteVideo,
    handleUnmuteVideo,
    handleRemoveParticipant,
  };
};