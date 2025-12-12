// src/hooks/useRecording.js
// MERGED & CONSOLIDATED: Trash + Hybrid Recording + Upload Progress + Subtitles + Pagination
// Sources merged: Enhanced trash version + corrected API/endpoint version

import { useState, useCallback } from "react";
import { recordingsAPI, meetingsAPI } from "../services/api"; // requires endpoints listed below
import { useAuth } from "./useAuth";

// If your api layer exposes getVideoStreamUrl; if not, we fall back to env URL builder below inside getRecordingStreamUrl

export const useRecording = () => {
  // Core state
  const [recordings, setRecordings] = useState([]);
  const [trashedRecordings, setTrashedRecordings] = useState([]);
  const [trashStats, setTrashStats] = useState({
    total_count: 0,
    total_size: 0,
    oldest_date: null,
  });

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Pagination
  const [pagination, setPagination] = useState({
    page: 1,
    pages: 1,
    total: 0,
    total_pages: 1,
  });

  // Upload & recording method
  const [uploadProgress, setUploadProgress] = useState(0);
  const [recordingMethod, setRecordingMethod] = useState(null); // 'server' | 'client' | null
  const [clientRecording, setClientRecording] = useState(false);

  // Auth/current user
  const { user: authUser } = useAuth();
  const currentUser = authUser || {
    email: localStorage.getItem("user_email") || "",
    id: localStorage.getItem("user_id") || "",
    name: localStorage.getItem("user_name") || "User",
  };

  // Document helpers exposed from API layer (mindmap/transcript/summary)
  // Keep these in case your UI uses them
  const {
    downloadTranscript,
    downloadSummary,
    viewTranscript,
    viewSummary,
    getMindmapUrl,
  } = recordingsAPI;

  // ---- Utilities ----
  const clearError = useCallback(() => setError(null), []);

  const resetState = useCallback(() => {
    setRecordings([]);
    setTrashedRecordings([]);
    setTrashStats({ total_count: 0, total_size: 0, oldest_date: null });
    setLoading(false);
    setError(null);
    setUploadProgress(0);
    setRecordingMethod(null);
    setClientRecording(false);
    setPagination({ page: 1, pages: 1, total: 0, total_pages: 1 });
  }, []);

  const formatDuration = useCallback((duration) => {
    if (!duration) return "0:00";
    const seconds =
      typeof duration === "number" ? duration : Number(duration) || 0;
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    return h > 0
      ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
      : `${m}:${String(s).padStart(2, "0")}`;
  }, []);

  const checkRecordingSupport = useCallback(() => {
    const hasMediaRecorder = typeof MediaRecorder !== "undefined";
    const hasGetUserMedia =
      typeof navigator !== "undefined" &&
      navigator.mediaDevices &&
      navigator.mediaDevices.getUserMedia;
    const hasGetDisplayMedia =
      typeof navigator !== "undefined" &&
      navigator.mediaDevices &&
      navigator.mediaDevices.getDisplayMedia;
    return {
      client: hasMediaRecorder && hasGetUserMedia,
      server: true, // runtime availability will be checked via API
      screenCapture: !!hasGetDisplayMedia,
    };
  }, []);

  // Build a stream URL (prefers API helper if present)
  const getRecordingStreamUrl = useCallback(
    (recordingId, email, userId) => {
      const userEmail = email || currentUser.email;
      const userIdParam = userId || currentUser.id;

      // Prefer API helper if it exists
      if (typeof recordingsAPI.getVideoStreamUrl === "function") {
        return recordingsAPI.getVideoStreamUrl(
          recordingId,
          userEmail,
          userIdParam
        );
      }

      // Fallback to env-based URL (for Vite/CRA)
      const baseUrl =
        (import.meta && import.meta.env && import.meta.env.VITE_API_URL) ||
        (import.meta && import.meta.env && import.meta.env.REACT_APP_API_URL) ||
        process.env.REACT_APP_API_URL ||
        "http://localhost:3001";

      const params = new URLSearchParams({
        email: userEmail,
        user_id: userIdParam,
      }).toString();

      return `${baseUrl}/api/recordings/${recordingId}/stream?${params}`;
    },
    [currentUser.email, currentUser.id]
  );

  // ---- Fetch: Active recordings ----
  const fetchAllRecordings = useCallback(
    async (params = {}) => {
      try {
        setLoading(true);
        setError(null);

        const requestParams = {
          page: 1,
          limit: 50,
          email: currentUser.email,
          user_id: currentUser.id,
          ...params,
        };

        const response = await recordingsAPI.getRecordings(requestParams);

        const active = (response.videos || []).filter((r) => !r.is_trashed);
        setRecordings(active);
        setPagination({
          page: response.page || 1,
          pages: response.pages || 1,
          total: active.length,
          total_pages: response.pages || 1,
        });

        return response;
      } catch (err) {
        setError(err.message || "Failed to fetch recordings");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [currentUser.email, currentUser.id]
  );

  // ---- Pagination (active) ----
  const loadMoreRecordings = useCallback(async () => {
    if (loading || pagination.page >= pagination.total_pages) return;
    try {
      setLoading(true);
      const nextPage = pagination.page + 1;
      const response = await recordingsAPI.getRecordings({
        page: nextPage,
        limit: 50,
        email: currentUser.email,
        user_id: currentUser.id,
        include_server_recordings: true,
      });

      const active = (response.videos || []).filter((r) => !r.is_trashed);
      setRecordings((prev) => [...prev, ...active]);
      setPagination((prev) => ({
        ...prev,
        page: response.page || nextPage,
        pages: response.pages || prev.pages,
        total: prev.total + active.length,
        total_pages: response.pages || prev.total_pages,
      }));

      return response;
    } catch (err) {
      setError(err.message || "Failed to load more recordings");
      throw err;
    } finally {
      setLoading(false);
    }
  }, [loading, pagination, currentUser.email, currentUser.id]);

  // ---- Fetch: Trash ----
  const fetchTrashedRecordings = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await recordingsAPI.getTrashedRecordings({
        user_id: currentUser.id,
        email: currentUser.email,
      });

      setTrashedRecordings(response.videos || []);
      return response;
    } catch (err) {
      // if empty results -> not an error UX-wise
      if (err.response?.status !== 404) {
        setError(err.message || "Failed to fetch trashed recordings");
      }
      setTrashedRecordings([]);
      return { videos: [] };
    } finally {
      setLoading(false);
    }
  }, [currentUser.id, currentUser.email]);

  const getTrashStats = useCallback(async () => {
    try {
      const stats = await recordingsAPI.getTrashStats({
        user_id: currentUser.id,
        email: currentUser.email,
      });
      setTrashStats(stats);
      return stats;
    } catch (err) {
      // silent fallback
      return { total_count: 0, total_size: 0, oldest_date: null };
    }
  }, [currentUser.id, currentUser.email]);

  const moveToTrash = useCallback(
    async (recordingId, userCredentials = null) => {
      try {
        setLoading(true);
        setError(null);

        const creds = userCredentials || {
          user_id: currentUser.id,
          email: currentUser.email,
        };

        const response = await recordingsAPI.moveToTrash(recordingId, creds);

        // remove from active
        setRecordings((prev) =>
          prev.filter((r) => (r._id || r.id) !== recordingId)
        );

        // refresh trash
        await fetchTrashedRecordings();
        return response;
      } catch (err) {
        setError(err.message || "Failed to move recording to trash");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [currentUser.id, currentUser.email, fetchTrashedRecordings]
  );

  const restoreFromTrash = useCallback(
    async (recordingId, userCredentials = null) => {
      try {
        setLoading(true);
        setError(null);

        const creds = userCredentials || {
          user_id: currentUser.id,
          email: currentUser.email,
        };

        const response = await recordingsAPI.restoreFromTrash(
          recordingId,
          creds
        );

        // remove from trash
        setTrashedRecordings((prev) =>
          prev.filter((r) => (r._id || r.id) !== recordingId)
        );

        // refresh active
        await fetchAllRecordings({
          page: 1,
          limit: 50,
          email: currentUser.email,
          user_id: currentUser.id,
        });

        return response;
      } catch (err) {
        setError(err.message || "Failed to restore recording from trash");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [currentUser.id, currentUser.email, fetchAllRecordings]
  );

  const permanentDelete = useCallback(
    async (recordingId, userCredentials = null) => {
      try {
        setLoading(true);
        setError(null);

        const creds = userCredentials || {
          user_id: currentUser.id,
          email: currentUser.email,
        };

        const response = await recordingsAPI.permanentDelete(
          recordingId,
          creds
        );

        // remove from trash list
        setTrashedRecordings((prev) =>
          prev.filter((r) => (r._id || r.id) !== recordingId)
        );

        return response;
      } catch (err) {
        setError(err.message || "Failed to permanently delete recording");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [currentUser.id, currentUser.email]
  );

  const emptyTrash = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      const promises = trashedRecordings.map((r) =>
        recordingsAPI.permanentDelete(r._id || r.id, {
          user_id: currentUser.id,
          email: currentUser.email,
        })
      );

      await Promise.allSettled(promises);
      setTrashedRecordings([]);
      setTrashStats({ total_count: 0, total_size: 0, oldest_date: null });
    } catch (err) {
      setError(err.message || "Failed to empty trash");
      throw err;
    } finally {
      setLoading(false);
    }
  }, [trashedRecordings, currentUser.id, currentUser.email]);

  // Make deleteRecording a soft-delete
  const deleteRecording = useCallback(
    async (recordingId, userCredentials = null) =>
      moveToTrash(recordingId, userCredentials),
    [moveToTrash]
  );

  // ---- Upload (client) ----
  const uploadRecording = useCallback(
    async (file, metadata = {}) => {
      try {
        setLoading(true);
        setError(null);
        setUploadProgress(0);

        if (!file || file.size === 0) throw new Error("Invalid or empty file");
        if (file.size > 500 * 1024 * 1024)
          throw new Error("File too large. Maximum size is 500MB");

        if (!currentUser?.id || !currentUser?.email)
          throw new Error("User authentication required for upload");

        const formData = new FormData();
        formData.append("file", file);
        formData.append("user_id", currentUser.id);
        formData.append("user_email", currentUser.email);
        formData.append("recording_source", "client");

        const validated = {
          meeting_id: metadata.meeting_id || "",
          title:
            metadata.title ||
            `Meeting Recording - ${new Date().toLocaleDateString()}`,
          duration: metadata.duration || 0,
          fileSize: file.size,
          recordingType: "client",
          ...metadata,
        };

        Object.keys(validated).forEach((k) => {
          if (validated[k] !== undefined && validated[k] !== null) {
            formData.append(k, String(validated[k]));
          }
        });

        const response = await recordingsAPI.uploadVideo(formData, {
          onUploadProgress: (evt) => {
            if (evt.lengthComputable) {
              const pct = Math.round((evt.loaded * 100) / evt.total);
              setUploadProgress(pct);
              if (window.setRecordingState) {
                window.setRecordingState((prev) => ({
                  ...prev,
                  uploadProgress: pct,
                }));
              }
            }
          },
        });

        setUploadProgress(100);

        // delayed refresh after server-side processing
        setTimeout(async () => {
          try {
            await fetchAllRecordings();
          } catch (_) {}
        }, 5000);

        return {
          success: true,
          recording: response.video || response.data || response,
          message: "Recording uploaded and processed successfully",
          file_size: file.size,
          upload_time: new Date().toISOString(),
        };
      } catch (err) {
        let msg = err.message || "Failed to upload recording";
        if (err.response?.status === 413) msg = "File too large for upload";
        else if (err.response?.status === 415) msg = "Unsupported file format";
        else if (err.response?.status === 401) msg = "Authentication failed";
        else if (err.response?.status === 403) msg = "Permission denied";
        else if (err.response?.status === 500)
          msg = "Server error during upload";
        else if (err.response?.data?.Error) msg = err.response.data.Error;

        setError(msg);
        throw new Error(msg);
      } finally {
        setLoading(false);
        setTimeout(() => setUploadProgress(0), 2000);
      }
    },
    [currentUser.email, currentUser.id, fetchAllRecordings]
  );

  // ---- Server recording (start/stop) ----
  const startMeetingRecording = useCallback(
    async (meetingId, settings = {}) => {
      try {
        if (!meetingId) throw new Error("Meeting ID is required");
        if (!currentUser?.id) throw new Error("User authentication required");

        const request = {
          user_id: currentUser.id,
          user_email: currentUser.email,
          recording_type: "server",
          quality: "hd",
          include_audio: true,
          include_video: true,
          layout: "grid",
          ...settings,
        };

        const response = await meetingsAPI.startMeetingRecording(
          meetingId,
          request
        );

        const ok =
          response &&
          response.success !== false &&
          response.status !== "error" &&
          !response.error;

        if (!ok) {
          const msg =
            response?.error || response?.message || "Unknown recording error";
          throw new Error(msg);
        }

        return {
          success: true,
          method: "server",
          message: response.message || "Server recording started successfully",
          recording_id: response.recording_id || response.id,
          ...response,
        };
      } catch (err) {
        let msg = err.message || "Failed to start recording";
        if (err.response?.status === 400)
          msg = "Invalid recording settings or meeting not found";
        else if (err.response?.status === 403)
          msg = "Permission denied: Only hosts can start recording";
        else if (err.response?.status === 409)
          msg = "Recording already in progress";
        else if (err.response?.status === 500)
          msg = "Recording service unavailable, please try again";
        else if (err.response?.data?.Error) msg = err.response.data.Error;
        else if (err.response?.data?.error) msg = err.response.data.error;
        throw new Error(msg);
      }
    },
    [currentUser.id, currentUser.email]
  );

  const stopMeetingRecording = useCallback(
    async (meetingId) => {
      try {
        if (!meetingId) throw new Error("Meeting ID is required");
        if (!currentUser?.id) throw new Error("User authentication required");

        const response = await meetingsAPI.stopMeetingRecording(meetingId);

        const ok =
          response &&
          response.success !== false &&
          response.status !== "error" &&
          !response.error;

        if (!ok) {
          const msg =
            response?.error ||
            response?.message ||
            "Unknown stop recording error";
          throw new Error(msg);
        }

        // delayed refresh
        setTimeout(async () => {
          try {
            await fetchAllRecordings();
          } catch (_) {}
        }, 3000);

        return {
          success: true,
          method: "server",
          message: response.message || "Server recording stopped successfully",
          video_url: response.video_url,
          recording_id: response.recording_id || response.id,
          file_size: response.file_size,
          duration: response.duration,
          ...response,
        };
      } catch (err) {
        let msg = err.message || "Failed to stop recording";
        if (err.response?.status === 400)
          msg = "No active recording found for this meeting";
        else if (err.response?.status === 403)
          msg = "Permission denied: Only hosts can stop recording";
        else if (err.response?.status === 404)
          msg = "Recording not found or already stopped";
        else if (err.response?.status === 500)
          msg = "Recording service error, recording may have stopped";
        else if (err.response?.data?.Error) msg = err.response.data.Error;
        else if (err.response?.data?.error) msg = err.response.data.error;
        throw new Error(msg);
      }
    },
    [currentUser.id, fetchAllRecordings]
  );

  // ---- Hybrid recording controller ----
  const startRecording = useCallback(
    async (meetingId, options = {}) => {
      try {
        setLoading(true);
        setError(null);

        const { method = "auto", fallback = true, ...settings } = options;

        // Decide best method
        let chosen = method;
        if (method === "auto") {
          const support = checkRecordingSupport();
          chosen = support.server ? "server" : support.client ? "client" : null;
          if (!chosen) throw new Error("No recording method available");
        }

        setRecordingMethod(chosen);

        if (chosen === "server" || chosen === "auto") {
          try {
            const resp = await startMeetingRecording(meetingId, {
              ...settings,
              recording_type: "server",
            });
            if (resp?.success) {
              setRecordingMethod("server");
              return {
                success: true,
                method: "server",
                message: "Server recording started successfully",
                ...resp,
              };
            }
          } catch (serverErr) {
            if (!fallback || method === "server") throw serverErr;
            // Fall back to client
            setRecordingMethod("client");
          }
        }

        // Client recording path
        if (chosen === "client" || (fallback && chosen !== "server")) {
          const support = checkRecordingSupport();
          if (!support.client)
            throw new Error("Client recording not supported in this browser");
          setClientRecording(true);
          setRecordingMethod("client");
          return {
            success: true,
            method: "client",
            message:
              "Ready to start client recording – call startClientRecording() from your component",
            requiresUserAction: true,
            meetingId,
          };
        }

        throw new Error("No recording method could be started");
      } catch (err) {
        setError(err.message || "Failed to start recording");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [checkRecordingSupport, startMeetingRecording]
  );

  const stopRecording = useCallback(
    async (meetingId, clientRecordingData = null) => {
      try {
        setLoading(true);
        setError(null);

        if (recordingMethod === "server") {
          const resp = await stopMeetingRecording(meetingId);
          setRecordingMethod(null);
          return resp;
        }

        if (recordingMethod === "client" && clientRecordingData) {
          setClientRecording(false);

          const { blob, metadata = {} } = clientRecordingData;
          const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
          const fileName = `meeting-${meetingId}-${timestamp}.webm`;
          const file = new File([blob], fileName, { type: blob.type });

          const result = await uploadRecording(file, {
            meeting_id: meetingId,
            title: `Meeting Recording - ${new Date().toLocaleDateString()}`,
            ...metadata,
          });

          setRecordingMethod(null);

          return {
            success: true,
            method: "client",
            message: "Client recording processed successfully",
            recording: result.recording,
            file_size: file.size,
            duration: metadata.duration || "Unknown",
          };
        }

        throw new Error("No active recording to stop");
      } catch (err) {
        setError(err.message || "Failed to stop recording");
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [recordingMethod, stopMeetingRecording, uploadRecording]
  );

  // ---- Recording CRUD & utilities ----
  const updateRecording = useCallback(async (recordingId, updates) => {
    try {
      setLoading(true);
      setError(null);

      const response = await recordingsAPI.updateRecording(
        recordingId,
        updates
      );

      setRecordings((prev) =>
        prev.map((r) =>
          (r._id || r.id) === recordingId ? { ...r, ...updates } : r
        )
      );

      return response;
    } catch (err) {
      setError(err.message || "Failed to update recording");
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const getRecording = useCallback(async (recordingId) => {
    try {
      setLoading(true);
      setError(null);
      return await recordingsAPI.getRecording(recordingId);
    } catch (err) {
      setError(err.message || "Failed to get recording");
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const downloadRecording = useCallback(
    async (recordingId) => {
      try {
        const url = getRecordingStreamUrl(
          recordingId,
          currentUser.email,
          currentUser.id
        );
        const link = document.createElement("a");
        link.href = url;
        link.download = `recording-${recordingId}.mp4`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      } catch (err) {
        setError(err.message || "Failed to download recording");
        throw err;
      }
    },
    [currentUser.email, currentUser.id, getRecordingStreamUrl]
  );

  const shareRecording = useCallback(
    async (recordingId) => {
      try {
        const url = getRecordingStreamUrl(
          recordingId,
          currentUser.email,
          currentUser.id
        );
        if (navigator.share) {
          await navigator.share({
            title: "Meeting Recording",
            text: "Check out this meeting recording",
            url,
          });
        } else {
          await navigator.clipboard.writeText(url);
          return { message: "Link copied to clipboard!" };
        }
      } catch (err) {
        setError(err.message || "Failed to share recording");
        throw err;
      }
    },
    [currentUser.email, currentUser.id, getRecordingStreamUrl]
  );

  // ---- Subtitles ----
  const generateSubtitles = useCallback(async (recordingId) => {
    try {
      setLoading(true);
      setError(null);
      const response = await recordingsAPI.generateSubtitles(recordingId);
      setRecordings((prev) =>
        prev.map((r) =>
          (r._id || r.id) === recordingId
            ? { ...r, subtitles_available: true }
            : r
        )
      );
      return response;
    } catch (err) {
      setError(err.message || "Failed to generate subtitles");
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const getSubtitles = useCallback(async (recordingId) => {
    try {
      return await recordingsAPI.getSubtitles(recordingId);
    } catch (err) {
      setError(err.message || "Failed to get subtitles");
      throw err;
    }
  }, []);

  const downloadSubtitles = useCallback(async (recordingId) => {
    try {
      return await recordingsAPI.downloadSubtitles(recordingId);
    } catch (err) {
      setError(err.message || "Failed to download subtitles");
      throw err;
    }
  }, []);

  const checkSubtitlesAvailable = useCallback(async (recordingId) => {
    try {
      return await recordingsAPI.checkSubtitlesAvailable(recordingId);
    } catch (_) {
      return false;
    }
  }, []);

  // ---- Return API ----
  return {
    // state
    recordings,
    trashedRecordings,
    trashStats,
    loading,
    error,
    pagination,
    uploadProgress,
    recordingMethod,
    clientRecording,

    // record (hybrid) control
    startRecording,
    stopRecording,
    startMeetingRecording,
    stopMeetingRecording,
    checkRecordingSupport,

    // CRUD & lists
    fetchAllRecordings,
    loadMoreRecordings,
    updateRecording,
    deleteRecording, // soft delete -> trash
    getRecording,
    downloadRecording,
    shareRecording,

    // trash ops
    fetchTrashedRecordings,
    moveToTrash,
    restoreFromTrash,
    permanentDelete,
    emptyTrash,
    getTrashStats,

    // subtitles
    generateSubtitles,
    getSubtitles,
    downloadSubtitles,
    checkSubtitlesAvailable,

    // utils
    getRecordingStreamUrl,
    formatDuration,
    clearError,
    resetState,

    // doc helpers from API (optional usage in UI)
    downloadTranscript,
    downloadSummary,
    viewTranscript,
    viewSummary,
    getMindmapUrl,

    // combined flags/info
    isUploading: loading && uploadProgress > 0,
    recordingSupport: checkRecordingSupport(),
    currentUser,
  };
};
