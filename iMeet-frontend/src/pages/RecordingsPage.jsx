// Enhanced RecordingsPage.jsx with folder structure for scheduled meetings
import React, { useState, useEffect, useMemo } from "react";
import {
  Box,
  Container,
  Typography,
  Grid,
  Card,
  CardContent,
  CardActions,
  Button,
  IconButton,
  Chip,
  Stack,
  Avatar,
  Divider,
  useTheme,
  alpha,
  TextField,
  InputAdornment,
  Menu,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  CircularProgress,
  Alert,
  LinearProgress,
  ListItemIcon,
  ListItemText,
  Badge,
  Tabs,
  Tab,
  Tooltip,
  FormControl,
  InputLabel,
  Select,
  Collapse,
} from "@mui/material";
import {
  PlayArrow as PlayIcon,
  Download as DownloadIcon,
  Share as ShareIcon,
  MoreVert as MoreIcon,
  Search as SearchIcon,
  VideoLibrary as VideoIcon,
  AccessTime as TimeIcon,
  Person as PersonIcon,
  Delete as DeleteIcon,
  Edit as EditIcon,
  ErrorOutline as ErrorIcon,
  Upload as UploadIcon,
  Description as DocumentIcon,
  Visibility as ViewIcon,
  PictureAsPdf as PdfIcon,
  Assessment as SummaryIcon,
  Subtitles as TranscriptIcon,
  Close as CloseIcon,
  Timeline as TimelineIcon,
  Block as BlockIcon,
  SubtitlesOff as SubtitlesOffIcon,
  ClosedCaption as ClosedCaptionIcon,
  AutoFixHigh as GenerateIcon,
  GetApp as GetAppIcon,
  Folder as FolderIcon,
  FolderOpen as FolderOpenIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  // Trash-related icons
  DeleteOutlined as TrashIcon,
  RestoreFromTrash as RestoreIcon,
  DeleteForever as PermanentDeleteIcon,
  CleaningServices as EmptyTrashIcon,
  Info as InfoIcon,
} from "@mui/icons-material";
import { format } from "date-fns";
import DashboardLayout from "../layouts/DashboardLayout";
import RecordingPlayer from "../components/recording/RecordingPlayer";
import { useRecording } from "../hooks/useRecording";
import { recordingsAPI } from "../services/api";
import { useAuth } from "../hooks/useAuth";
import { useNotifications } from "../hooks/useNotifications";

const RecordingsPage = () => {
  const theme = useTheme();

  const { user: authUser } = useAuth();

  const getCurrentUser = () => {
    const sources = [
      authUser,
      JSON.parse(localStorage.getItem("user") || "{}"),
      {
        email: localStorage.getItem("user_email"),
        id: localStorage.getItem("user_id"),
        name: localStorage.getItem("user_name"),
      },
    ];

    for (const source of sources) {
      if (source && (source.id || source.email)) {
        return {
          email: source.email || "",
          id: source.id || source.Id || "",
          name: source.name || source.Name || source.full_name || "User",
        };
      }
    }

    return { email: "", id: "", name: "User" };
  };

  const currentUser = getCurrentUser();

  const {
    recordings,
    trashedRecordings,
    trashStats,
    loading,
    error,
    pagination,
    fetchAllRecordings,
    fetchTrashedRecordings,
    moveToTrash,
    restoreFromTrash,
    permanentDelete,
    getTrashStats,
    emptyTrash,
    getRecordingStreamUrl,
    formatDuration,
    loadMoreRecordings,
    documentMethods,
  } = useRecording();
  
  const { notifications, fetchRecordingNotifications } = useNotifications();
  
  const {
    downloadTranscript,
    downloadSummary,
    viewTranscript,
    viewSummary,
    getMindmapUrl,
  } = documentMethods || recordingsAPI;

  const [currentTab, setCurrentTab] = useState(0);
  const [searchQuery, setSearchQuery] = useState("");
  const [filterBy, setFilterBy] = useState("all");
  const [selectedRecording, setSelectedRecording] = useState(null);
  const [playerOpen, setPlayerOpen] = useState(false);
  const [menuAnchor, setMenuAnchor] = useState(null);
  const [selectedRecordingId, setSelectedRecordingId] = useState(null);
  const [apiError, setApiError] = useState(null);
  const [isInitialLoading, setIsInitialLoading] = useState(true);
  const [meetingTypeFilter, setMeetingTypeFilter] = useState("all");
  const [expandedFolders, setExpandedFolders] = useState({}); // Track folder expand/collapse

  const [deleteConfirmDialog, setDeleteConfirmDialog] = useState(false);
  const [restoreConfirmDialog, setRestoreConfirmDialog] = useState(false);
  const [permanentDeleteDialog, setPermanentDeleteDialog] = useState(false);
  const [emptyTrashDialog, setEmptyTrashDialog] = useState(false);

  const [userCanAccessTrash, setUserCanAccessTrash] = useState(false);

  const isUserHostOfRecording = (recording) => {
    if (!recording || !currentUser.id) {
      return false;
    }

    const possibleHostFields = [
      "user_id",
      "host_id",
      "Host_ID",
      "uploaded_by",
      "owner_id",
      "creator_id",
      "author_id",
    ];

    const currentUserId = String(currentUser.id).trim();

    for (const field of possibleHostFields) {
      const recordingUserId = recording[field];

      if (recordingUserId != null) {
        const recordingUserIdStr = String(recordingUserId).trim();

        if (recordingUserIdStr === currentUserId) {
          return true;
        }
      }
    }

    if (currentUser.email) {
      const emailFields = [
        "email",
        "user_email",
        "host_email",
        "uploaded_by_email",
      ];
      const currentEmail = currentUser.email.toLowerCase().trim();

      for (const field of emailFields) {
        const recordingEmail = recording[field];
        if (
          recordingEmail &&
          String(recordingEmail).toLowerCase().trim() === currentEmail
        ) {
          return true;
        }
      }
    }

    return false;
  };

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const typeParam = params.get("type");
    
    console.log("📊 RecordingsPage: URL type parameter:", typeParam);
    
    if (typeParam && ["all", "instant", "scheduled", "calendar"].includes(typeParam)) {
      setMeetingTypeFilter(typeParam);
      console.log("✅ RecordingsPage: Meeting type filter set to:", typeParam);
    } else {
      setMeetingTypeFilter("all");
    }
  }, [window.location.search]);

  const getMeetingTypeForAPI = (filterType) => {
    const typeMap = {
      instant: "InstantMeeting",
      scheduled: "ScheduleMeeting",
      calendar: "CalendarMeeting",
      all: null,
    };
    return typeMap[filterType] || null;
  };
  
  useEffect(() => {
    const loadRecordings = async () => {
      try {
        setApiError(null);

        if (!currentUser.id) {
          setApiError("Missing user credentials. Please log in again.");
          setIsInitialLoading(false);
          return;
        }

        const meetingTypeParam = getMeetingTypeForAPI(meetingTypeFilter);
        const fetchParams = {
          page: 1,
          limit: 50,
          user_id: currentUser.id,
          email: currentUser.email,
        };

        if (meetingTypeParam) {
          fetchParams.meeting_type = meetingTypeParam;
        }

        console.log("📊 RecordingsPage: Initial fetch with params:", fetchParams);

        const activeRecordingsResponse = await fetchAllRecordings(fetchParams);

        const isHostOfAnyRecordings = (
          activeRecordingsResponse?.videos || []
        ).some((recording) => isUserHostOfRecording(recording));

        setUserCanAccessTrash(isHostOfAnyRecordings);

        if (isHostOfAnyRecordings) {
          await Promise.all([fetchTrashedRecordings(), getTrashStats()]);
        }
      } catch (err) {
        console.error("Failed to load recordings:", err);
        setApiError(err.message || "Failed to load recordings");
      } finally {
        setIsInitialLoading(false);
      }
    };

    loadRecordings();
  }, [
    currentUser.id,
    currentUser.email,
    fetchAllRecordings,
    fetchTrashedRecordings,
    getTrashStats,
  ]);

  useEffect(() => {
    if (!currentUser.id || isInitialLoading) return;

    const loadFilteredRecordings = async () => {
      try {
        const meetingTypeParam = getMeetingTypeForAPI(meetingTypeFilter);
        const fetchParams = {
          page: 1,
          limit: 50,
          user_id: currentUser.id,
          email: currentUser.email,
        };

        if (meetingTypeParam) {
          fetchParams.meeting_type = meetingTypeParam;
        }

        console.log("📊 RecordingsPage: Reloading with filter:", meetingTypeFilter, "API param:", meetingTypeParam);
        await fetchAllRecordings(fetchParams);
      } catch (err) {
        console.error("Failed to reload recordings:", err);
        setApiError(err.message || "Failed to reload recordings");
      }
    };

    loadFilteredRecordings();
  }, [meetingTypeFilter, currentUser.id, currentUser.email]);
  
  useEffect(() => {
    console.log("🎥 Recordings Page: Fetching RECORDING notifications only");
    fetchRecordingNotifications();
  }, [fetchRecordingNotifications]);

  const getCurrentRecordings = () => {
    if (currentTab === 0) {
      return recordings;
    } else if (currentTab === 1 && userCanAccessTrash) {
      return trashedRecordings;
    }
    return [];
  };

  const transformRecordings = (recordingsList) => {
    return recordingsList.map((recording) => {
      const isHost = isUserHostOfRecording(recording);
      const meetingType = (
        recording.meeting_type ||
        recording.Meeting_Type ||
        recording.type ||
        "unknown"
      ).toLowerCase();

      return {
        id: recording._id || recording.id,
        meeting_name:
          recording.original_filename || recording.title || "Meeting Recording",
        meeting_type: meetingType,
        Meeting_Type: recording.Meeting_Type || recording.meeting_type,
        file_name: recording.original_filename || `recording_${recording._id}`,
        duration: recording.duration
          ? formatDuration(recording.duration)
          : "0:00",
        file_size: recording.file_size || "Unknown",
        created_at: recording.timestamp || recording.created_at || new Date(),
        trashed_at: recording.trashed_at,
        host_name: `User ${recording.user_id || "Unknown"}`,
        participants_count: 0,
        thumbnail:
          recording.image_url || `/api/recordings/${recording._id}/thumbnail`,
        transcription_available:
          recording.transcription_available || !!recording.transcript_url,
        summary_available:
          recording.summary_available || !!recording.summary_url,
        mindmap_available: !!recording.image_url,
        subtitles_available:
          recording.subtitles_available || !!recording.subtitles_url || false,
        quality: "HD",
        status: recording.is_trashed ? "trashed" : "processed",
        streamUrl:
          recording.streamUrl ||
          getRecordingStreamUrl(
            recording._id,
            currentUser.email,
            currentUser.id
          ),
        transcript_url: recording.transcript_url,
        summary_url: recording.summary_url,
        image_url: recording.image_url,
        subtitles_url: recording.subtitles_url,
        _id: recording._id,
        meeting_id: recording.meeting_id,
        user_id: recording.user_id,
        isUserHost: isHost,
        is_trashed: recording.is_trashed || false,
        // NEW: Schedule metadata
        schedule_id: recording.schedule_id,
        schedule_title: recording.schedule_title,
        schedule_folder: recording.schedule_folder,
      };
    });
  };
  
  const displayRecordings = transformRecordings(getCurrentRecordings());
  
  // NEW: Group recordings by schedule for ScheduleMeetings
  const groupedRecordings = useMemo(() => {
    const groups = {
      instant: [],
      calendar: [],
      schedules: {} // { schedule_id: { title, recordings: [] } }
    };

    displayRecordings.forEach(recording => {
      const type = recording.meeting_type?.toLowerCase();
      
      if (type === 'schedulemeeting') {
        const scheduleId = recording.schedule_id || 'unknown';
        const scheduleTitle = recording.schedule_title || 'Unnamed Schedule';
        
        if (!groups.schedules[scheduleId]) {
          groups.schedules[scheduleId] = {
            title: scheduleTitle,
            folder_path: recording.schedule_folder,
            recordings: []
          };
        }
        groups.schedules[scheduleId].recordings.push(recording);
      } else if (type === 'instantmeeting') {
        groups.instant.push(recording);
      } else if (type === 'calendarmeeting') {
        groups.calendar.push(recording);
      }
    });

    return groups;
  }, [displayRecordings]);
  
  const filteredRecordings = displayRecordings.filter((recording) => {
    const matchesSearch =
      (recording.meeting_name || "")
        .toLowerCase()
        .includes(searchQuery.toLowerCase()) ||
      (recording.host_name || "")
        .toLowerCase()
        .includes(searchQuery.toLowerCase()) ||
      (recording.file_name || "")
        .toLowerCase()
        .includes(searchQuery.toLowerCase());

    if (filterBy === "all") return matchesSearch;
    if (filterBy === "transcribed")
      return matchesSearch && recording.transcription_available;
    if (filterBy === "recent") {
      const weekAgo = new Date();
      weekAgo.setDate(weekAgo.getDate() - 7);
      return matchesSearch && new Date(recording.created_at) > weekAgo;
    }

    return matchesSearch;
  });

  useEffect(() => {
    if (displayRecordings.length > 0) {
      console.log("📊 RecordingsPage: Current filter state:", {
        meetingTypeFilter,
        totalRecordings: displayRecordings.length,
        filteredCount: filteredRecordings.length,
        scheduledFolders: Object.keys(groupedRecordings.schedules).length,
        sampleMeetingTypes: displayRecordings
          .slice(0, 3)
          .map((r) => ({
            name: r.meeting_name,
            type: r.meeting_type,
            Type: r.Meeting_Type,
          })),
      });
    }
  }, [meetingTypeFilter, displayRecordings.length, filteredRecordings.length]);

  const handleTabChange = (event, newValue) => {
    if (newValue === 1 && !userCanAccessTrash) {
      console.warn("User does not have recordings they can move to trash");
      return;
    }

    setCurrentTab(newValue);
    setSearchQuery("");
    setFilterBy("all");
  };

  const handleMenuOpen = (event, recordingId) => {
    setSelectedRecordingId(recordingId);
    setMenuAnchor(event.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuAnchor(null);
    setSelectedRecordingId(null);
  };

  const handlePlay = (recording) => {
    if (recording.is_trashed) {
      alert("Cannot play trashed recordings. Please restore first.");
      return;
    }

    setSelectedRecording({
      id: recording.id,
      title: recording.meeting_name || recording.file_name,
      duration: recording.duration,
      videoUrl:
        recording.streamUrl ||
        getRecordingStreamUrl(recording.id, currentUser.email, currentUser.id),
      thumbnailUrl: recording.thumbnail,
      quality: recording.quality || "HD",
      fileSize: recording.file_size,
      recordedAt: recording.created_at,
      participants: [],
      transcription: [],
      chatMessages: [],
      currentUser: currentUser,
    });
    setPlayerOpen(true);
  };

  const handleMoveToTrash = async (recording) => {
    try {
      await moveToTrash(recording.id, {
        user_id: currentUser.id,
        email: currentUser.email,
      });

      const remainingRecordings = recordings.filter(
        (r) => r.id !== recording.id
      );
      const stillHasRecordingsToTrash = remainingRecordings.some((r) =>
        isUserHostOfRecording(r)
      );
      setUserCanAccessTrash(stillHasRecordingsToTrash);

      if (!stillHasRecordingsToTrash && currentTab === 1) {
        setCurrentTab(0);
      }

      alert("Recording moved to trash successfully!");
    } catch (err) {
      console.error("Move to trash failed:", err);

      if (err.response?.status === 403) {
        setApiError(
          "Permission denied: Only the meeting host can delete this recording"
        );
      } else {
        setApiError("Failed to move recording to trash: " + err.message);
      }
    }
    handleMenuClose();
  };

  const handleRestoreFromTrash = async (recording) => {
    try {
      await restoreFromTrash(recording.id, {
        user_id: currentUser.id,
        email: currentUser.email,
      });

      setUserCanAccessTrash(true);
      alert("Recording restored successfully!");
    } catch (err) {
      console.error("Restore failed:", err);
      setApiError("Failed to restore recording: " + err.message);
    }
    handleMenuClose();
  };

  const handlePermanentDelete = async (recording) => {
    try {
      await permanentDelete(recording.id, {
        user_id: currentUser.id,
        email: currentUser.email,
      });

      alert("Recording permanently deleted!");
    } catch (err) {
      console.error("Permanent delete failed:", err);
      setApiError("Failed to permanently delete recording: " + err.message);
    }
    handleMenuClose();
  };

  const handleEmptyTrash = async () => {
    try {
      await emptyTrash();
      alert("Trash emptied successfully!");
      setEmptyTrashDialog(false);
    } catch (err) {
      console.error("Empty trash failed:", err);
      setApiError("Failed to empty trash: " + err.message);
    }
  };

  const handleViewTranscript = (recording) => {
    if (recording.is_trashed) {
      alert(
        "Cannot view transcript of trashed recordings. Please restore first."
      );
      return;
    }

    if (!recording.transcription_available) {
      alert("Transcript not available for this recording.");
      return;
    }

    try {
      const transcriptUrl = viewTranscript(
        recording.id,
        currentUser.email,
        currentUser.id
      );

      if (transcriptUrl) {
        window.open(transcriptUrl, "_blank");
      } else {
        alert("Transcript URL not available.");
      }
    } catch (err) {
      console.error("Failed to open transcript:", err);
      alert("Failed to open transcript: " + err.message);
    }
  };

  const handleDownloadTranscript = async (recording) => {
    if (recording.is_trashed) {
      alert(
        "Cannot download transcript of trashed recordings. Please restore first."
      );
      return;
    }

    if (!recording.transcription_available) {
      alert("Transcript not available for this recording.");
      return;
    }

    try {
      await downloadTranscript(
        recording.id,
        currentUser.email,
        currentUser.id,
        `transcript_${recording.meeting_name || recording.id}.pdf`
      );

      alert("Transcript download started!");
    } catch (err) {
      console.error("Failed to download transcript:", err);
      alert("Failed to download transcript: " + err.message);
    }
  };

  const handleViewSummary = (recording) => {
    if (recording.is_trashed) {
      alert("Cannot view summary of trashed recordings. Please restore first.");
      return;
    }

    if (!recording.summary_available) {
      alert("Summary not available for this recording.");
      return;
    }

    try {
      const summaryUrl = viewSummary(
        recording.id,
        currentUser.email,
        currentUser.id
      );

      if (summaryUrl) {
        window.open(summaryUrl, "_blank");
      } else {
        alert("Summary URL not available.");
      }
    } catch (err) {
      console.error("Failed to open summary:", err);
      alert("Failed to open summary: " + err.message);
    }
  };

  const handleDownloadSummary = async (recording) => {
    if (recording.is_trashed) {
      alert(
        "Cannot download summary of trashed recordings. Please restore first."
      );
      return;
    }

    if (!recording.summary_available) {
      alert("Summary not available for this recording.");
      return;
    }

    try {
      await downloadSummary(
        recording.id,
        currentUser.email,
        currentUser.id,
        `summary_${recording.meeting_name || recording.id}.pdf`
      );

      alert("Summary download started!");
    } catch (err) {
      console.error("Failed to download summary:", err);
      alert("Failed to download summary: " + err.message);
    }
  };

  const handleDownload = (recording) => {
    if (recording.is_trashed) {
      alert("Cannot download trashed recordings. Please restore first.");
      return;
    }

    try {
      const streamUrl =
        recording.streamUrl || getRecordingStreamUrl(recording.id);
      const link = document.createElement("a");
      link.href = streamUrl;
      link.download = recording.file_name || `recording-${recording.id}.mp4`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (err) {
      setApiError("Failed to download recording");
    }

    handleMenuClose();
  };

  // NEW: Toggle folder expand/collapse
  const toggleFolder = (scheduleId) => {
    setExpandedFolders(prev => ({
      ...prev,
      [scheduleId]: !prev[scheduleId]
    }));
  };

  // Recording Card Component (extracted for reuse)
  const RecordingCard = ({ recording }) => (
    <Card
      sx={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        cursor: recording.is_trashed ? "default" : "pointer",
        opacity: recording.is_trashed ? 0.7 : 1,
        transition: "all 0.3s ease",
        "&:hover": {
          transform: recording.is_trashed
            ? "none"
            : "translateY(-4px)",
          boxShadow: recording.is_trashed
            ? theme.shadows[1]
            : theme.shadows[8],
        },
        border: recording.is_trashed
          ? `1px solid ${theme.palette.error.light}`
          : `1px solid ${alpha(theme.palette.divider, 0.1)}`,
        borderRadius: 2,
      }}
    >
      {/* Thumbnail */}
      <Box
        sx={{
          height: 180,
          background: recording.is_trashed
            ? `linear-gradient(135deg, ${alpha(
                theme.palette.error.main,
                0.1
              )} 0%, ${alpha(
                theme.palette.error.main,
                0.2
              )} 100%)`
            : `linear-gradient(135deg, ${alpha(
                theme.palette.primary.main,
                0.1
              )} 0%, ${alpha(
                theme.palette.secondary.main,
                0.1
              )} 100%)`,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          position: "relative",
          borderRadius: "8px 8px 0 0",
        }}
      >
        <IconButton
          onClick={() => handlePlay(recording)}
          disabled={recording.is_trashed}
          sx={{
            bgcolor: recording.is_trashed
              ? alpha(theme.palette.error.main, 0.5)
              : alpha(theme.palette.primary.main, 0.9),
            color: "white",
            width: 64,
            height: 64,
            "&:hover": {
              bgcolor: recording.is_trashed
                ? alpha(theme.palette.error.main, 0.5)
                : theme.palette.primary.dark,
              transform: recording.is_trashed
                ? "none"
                : "scale(1.1)",
            },
            "&:disabled": {
              bgcolor: alpha(theme.palette.error.main, 0.3),
              color: "rgba(255,255,255,0.5)",
            },
          }}
        >
          <PlayIcon sx={{ fontSize: 32 }} />
        </IconButton>

        {/* Duration Badge */}
        <Chip
          label={recording.duration || "0:00"}
          size="small"
          sx={{
            position: "absolute",
            bottom: 8,
            right: 8,
            bgcolor: alpha(theme.palette.background.paper, 0.9),
            backdropFilter: "blur(4px)",
          }}
        />

        {/* Status Badge */}
        <Chip
          label={
            recording.is_trashed ? "TRASHED" : recording.status
          }
          size="small"
          color={
            recording.is_trashed
              ? "error"
              : recording.status === "processed"
              ? "success"
              : "warning"
          }
          sx={{
            position: "absolute",
            top: 8,
            left: 8,
          }}
        />

        {/* Host badge */}
        {recording.isUserHost && (
          <Chip
            label="You're the Host"
            size="small"
            color="primary"
            sx={{
              position: "absolute",
              top: 8,
              right: 8,
              fontSize: "0.7rem",
            }}
          />
        )}

        {/* Trash date overlay */}
        {recording.is_trashed && recording.trashed_at && (
          <Box
            sx={{
              position: "absolute",
              bottom: 0,
              left: 0,
              right: 0,
              background:
                "linear-gradient(transparent, rgba(244,67,54,0.8))",
              color: "white",
              p: 1,
              textAlign: "center",
            }}
          >
            <Typography variant="caption">
              Deleted:{" "}
              {format(
                new Date(recording.trashed_at),
                "MMM dd, yyyy"
              )}
            </Typography>
          </Box>
        )}
      </Box>

      <CardContent sx={{ flexGrow: 1, pb: 1 }}>
        <Stack spacing={2}>
          {/* Title */}
          <Typography
            variant="h6"
            fontWeight={600}
            noWrap
            sx={{
              textDecoration: recording.is_trashed
                ? "line-through"
                : "none",
              color: recording.is_trashed
                ? "text.secondary"
                : "text.primary",
            }}
          >
            {recording.meeting_name || recording.file_name}
          </Typography>

          {/* Host Info */}
          <Stack direction="row" alignItems="center" spacing={1}>
            <Avatar
              sx={{ width: 24, height: 24, fontSize: "0.75rem" }}
            >
              {(recording.host_name || "U").charAt(0)}
            </Avatar>
            <Typography variant="body2" color="text.secondary">
              {recording.host_name || `User ${recording.user_id}`}
              {recording.isUserHost && (
                <Typography
                  component="span"
                  variant="caption"
                  color="primary"
                  sx={{ ml: 1 }}
                >
                  (You)
                </Typography>
              )}
            </Typography>
          </Stack>

          {/* Details */}
          <Stack spacing={1}>
            <Stack
              direction="row"
              alignItems="center"
              spacing={1}
            >
              <TimeIcon
                sx={{ fontSize: 16, color: "text.secondary" }}
              />
              <Typography
                variant="caption"
                color="text.secondary"
              >
                {recording.created_at
                  ? format(
                      new Date(recording.created_at),
                      "MMM dd, yyyy • HH:mm"
                    )
                  : "Unknown date"}
              </Typography>
            </Stack>

            {recording.meeting_id && (
              <Stack
                direction="row"
                alignItems="center"
                spacing={1}
              >
                <PersonIcon
                  sx={{ fontSize: 16, color: "text.secondary" }}
                />
                <Typography
                  variant="caption"
                  color="text.secondary"
                >
                  Meeting: {recording.meeting_id}
                </Typography>
              </Stack>
            )}
          </Stack>

          {/* Tags */}
          <Stack direction="row" spacing={1} flexWrap="wrap">
            <Chip
              label={recording.file_size || "Unknown"}
              size="small"
              variant="outlined"
            />

            {recording.subtitles_available ? (
              <Chip
                label="Subtitles"
                size="small"
                color="success"
                variant="outlined"
                icon={<ClosedCaptionIcon />}
              />
            ) : (
              <Chip
                label="No Subtitles"
                size="small"
                color="default"
                variant="outlined"
                icon={<SubtitlesOffIcon />}
              />
            )}

            {recording.transcription_available && (
              <Chip
                label="Transcript"
                size="small"
                color="success"
                variant="outlined"
                icon={<TranscriptIcon />}
              />
            )}
            {recording.summary_available && (
              <Chip
                label="Summary"
                size="small"
                color="info"
                variant="outlined"
                icon={<SummaryIcon />}
              />
            )}
            {recording.mindmap_available && (
              <Chip
                label="Mind Map"
                size="small"
                color="secondary"
                variant="outlined"
                icon={<TimelineIcon />}
              />
            )}
            {recording.quality && (
              <Chip
                label={recording.quality}
                size="small"
                color="primary"
                variant="outlined"
              />
            )}
          </Stack>
        </Stack>
      </CardContent>

      <Divider />

      <CardActions
        sx={{ justifyContent: "space-between", px: 2 }}
      >
        {recording.is_trashed ? (
          <Stack direction="row" spacing={1}>
            <Tooltip title="Restore Recording">
              <Button
                size="small"
                startIcon={<RestoreIcon />}
                onClick={() => {
                  setSelectedRecording(recording);
                  setRestoreConfirmDialog(true);
                }}
                disabled={!recording.isUserHost}
                sx={{ textTransform: "none" }}
              >
                Restore
              </Button>
            </Tooltip>

            <Tooltip title="Delete Permanently">
              <Button
                size="small"
                startIcon={<PermanentDeleteIcon />}
                onClick={() => {
                  setSelectedRecording(recording);
                  setPermanentDeleteDialog(true);
                }}
                disabled={!recording.isUserHost}
                color="error"
                sx={{ textTransform: "none" }}
              >
                Delete Forever
              </Button>
            </Tooltip>
          </Stack>
        ) : (
          <Stack direction="row" spacing={1}>
            <Tooltip title="Play Recording">
              <Button
                size="small"
                startIcon={<PlayIcon />}
                onClick={() => handlePlay(recording)}
                sx={{ textTransform: "none" }}
              >
                Play
              </Button>
            </Tooltip>

            {recording.transcription_available && (
              <Tooltip title="View Transcript">
                <Button
                  size="small"
                  startIcon={<ViewIcon />}
                  onClick={() => handleViewTranscript(recording)}
                  sx={{ textTransform: "none" }}
                >
                  Transcript
                </Button>
              </Tooltip>
            )}
          </Stack>
        )}

        <IconButton
          size="small"
          onClick={(e) => handleMenuOpen(e, recording.id)}
        >
          <MoreIcon />
        </IconButton>
      </CardActions>
    </Card>
  );

  if (isInitialLoading || (loading && displayRecordings.length === 0)) {
    return (
      <DashboardLayout>
        <Box
          sx={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            minHeight: "50vh",
          }}
        >
          <Stack alignItems="center" spacing={2}>
  {/* Custom Loader */}
  <Box
    sx={{
      border: '2px solid',
      borderColor: 'transparent #1976d2',
      width: '60px',
      height: '60px',
      borderRadius: '50%',
      display: 'inline-block',
      position: 'relative',
      boxSizing: 'border-box',
      animation: 'rotation 2s linear infinite',
      '&::after': {
        content: '""',
        boxSizing: 'border-box',
        position: 'absolute',
        left: '50%',
        top: '50%',
        border: '30px solid',
        borderColor: 'transparent rgba(111, 161, 231, 0.67)',
        borderRadius: '50%',
        transform: 'translate(-50%, -50%)',
      },
      '@keyframes rotation': {
        '0%': {
          transform: 'rotate(0deg)',
        },
        '100%': {
          transform: 'rotate(360deg)',
        },
      },
    }}
  />
  
  <Typography variant="h6">Loading recordings...</Typography>
</Stack>
        </Box>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <Box
        sx={{
          flexGrow: 1,
          background: `linear-gradient(135deg, ${alpha(
            theme.palette.background.default,
            0.9
          )} 0%, ${alpha(theme.palette.primary.main, 0.02)} 100%)`,
          minHeight: "100vh",
          pt: 2,
        }}
      >
        <Container maxWidth="xl" sx={{ py: 3 }}>
          {/* Header */}
          <Box mb={4}>
            <Stack
              direction="row"
              alignItems="center"
              justifyContent="space-between"
              mb={3}
            >
              <Stack direction="row" alignItems="center" spacing={2}>
                <VideoIcon
                  sx={{ fontSize: 32, color: theme.palette.primary.main }}
                />
                <Typography variant="h4" fontWeight={700} color="text.primary">
                  Meeting Recordings
                </Typography>
              </Stack>
            </Stack>

            {/* Tabs */}
            <Box sx={{ borderBottom: 1, borderColor: "divider", mb: 3 }}>
              <Tabs value={currentTab} onChange={handleTabChange}>
                <Tab
                  label={
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                      <VideoIcon />
                      Active Recordings ({recordings.length})
                    </Box>
                  }
                />

                {userCanAccessTrash && (
                  <Tab
                    label={
                      <Box
                        sx={{ display: "flex", alignItems: "center", gap: 1 }}
                      >
                        <Badge
                          badgeContent={trashStats.total_count}
                          color="error"
                        >
                          <TrashIcon />
                        </Badge>
                        Trash ({trashStats.total_count})
                      </Box>
                    }
                  />
                )}
              </Tabs>
            </Box>

            {/* Error Alert */}
            {(error || apiError) && (
              <Alert
                severity="error"
                sx={{ mb: 2 }}
                onClose={() => setApiError(null)}
              >
                <ErrorIcon sx={{ mr: 1 }} />
                {error || apiError}
                <Button
                  size="small"
                  onClick={() => window.location.reload()}
                  sx={{ ml: 2 }}
                >
                  Retry
                </Button>
              </Alert>
            )}

            {/* Trash statistics */}
            {userCanAccessTrash &&
              currentTab === 1 &&
              trashStats.total_count > 0 && (
                <Alert
                  severity="info"
                  sx={{ mb: 2 }}
                  action={
                    <Stack direction="row" spacing={1}>
                      <Button
                        size="small"
                        startIcon={<EmptyTrashIcon />}
                        onClick={() => setEmptyTrashDialog(true)}
                        color="error"
                        variant="outlined"
                      >
                        Empty Trash
                      </Button>
                    </Stack>
                  }
                >
                  <InfoIcon sx={{ mr: 1 }} />
                  {trashStats.total_count} recordings in trash • Total size:{" "}
                  {(trashStats.total_size / (1024 * 1024)).toFixed(2)} MB •
                  Recordings will be permanently deleted after 15 days
                </Alert>
              )}

            {/* Search and Filters */}
            <Stack
              direction={{ xs: "column", md: "row" }}
              spacing={2}
              alignItems="center"
            >
              <TextField
                placeholder={`Search ${
                  currentTab === 0 ? "active" : "trashed"
                } recordings...`}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon />
                    </InputAdornment>
                  ),
                }}
                sx={{ minWidth: 300 }}
              />

              <Stack direction="row" spacing={1}>
                {["all", "recent", "transcribed"].map((filter) => (
                  <Chip
                    key={filter}
                    label={filter.charAt(0).toUpperCase() + filter.slice(1)}
                    variant={filterBy === filter ? "filled" : "outlined"}
                    color={filterBy === filter ? "primary" : "default"}
                    onClick={() => setFilterBy(filter)}
                    sx={{ textTransform: "capitalize" }}
                  />
                ))}
              </Stack>

              <Divider orientation="vertical" flexItem sx={{ mx: 2 }} />

              {/* Meeting Type Filter */}
              <FormControl sx={{ minWidth: 180 }} size="small">
                <InputLabel>Meeting Type</InputLabel>
                <Select
                  value={meetingTypeFilter}
                  label="Meeting Type"
                  onChange={(e) => {
                    console.log("📊 RecordingsPage: Meeting type changed to:", e.target.value);
                    setMeetingTypeFilter(e.target.value);
                  }}
                >
                  <MenuItem value="all">All Types</MenuItem>
                  <MenuItem value="instant">Instant Meeting</MenuItem>
                  <MenuItem value="scheduled">Scheduled Meeting</MenuItem>
                  <MenuItem value="calendar">Calendar Meeting</MenuItem>
                </Select>
              </FormControl>

              <Typography variant="body2" color="text.secondary">
                {meetingTypeFilter === 'scheduled' 
                  ? `${Object.keys(groupedRecordings.schedules).length} schedule(s) with ${filteredRecordings.length} recording(s)`
                  : `${filteredRecordings.length} recording(s) found`}
                {meetingTypeFilter !== "all" && ` (${meetingTypeFilter})`}
              </Typography>
            </Stack>
          </Box>

          {/* Recordings Grid with Folder Structure */}
          <Grid container spacing={3}>
            {filteredRecordings.length === 0 ? (
              <Grid item xs={12}>
                <Card sx={{ textAlign: "center", py: 8 }}>
                  <CardContent>
                    {currentTab === 0 ? (
                      <>
                        <VideoIcon
                          sx={{ fontSize: 64, color: "text.disabled", mb: 2 }}
                        />
                        <Typography variant="h6" color="text.secondary" mb={1}>
                          No active recordings found
                        </Typography>
                        <Typography
                          variant="body2"
                          color="text.disabled"
                          mb={3}
                        >
                          {searchQuery || filterBy !== "all" || meetingTypeFilter !== "all"
                            ? "Try adjusting your search criteria"
                            : "No recordings available in your database"}
                        </Typography>
                      </>
                    ) : userCanAccessTrash ? (
                      <>
                        <TrashIcon
                          sx={{ fontSize: 64, color: "text.disabled", mb: 2 }}
                        />
                        <Typography variant="h6" color="text.secondary" mb={1}>
                          Trash is empty
                        </Typography>
                        <Typography
                          variant="body2"
                          color="text.disabled"
                          mb={3}
                        >
                          Deleted recordings will appear here
                        </Typography>
                      </>
                    ) : (
                      <>
                        <BlockIcon
                          sx={{ fontSize: 64, color: "text.disabled", mb: 2 }}
                        />
                        <Typography variant="h6" color="text.secondary" mb={1}>
                          No Trash Access
                        </Typography>
                        <Typography
                          variant="body2"
                          color="text.disabled"
                          mb={3}
                        >
                          You need to be the host of recordings to access trash
                          functionality
                        </Typography>
                      </>
                    )}
                  </CardContent>
                </Card>
              </Grid>
            ) : (
              <>
                {/* Show folder structure for scheduled meetings */}
                {meetingTypeFilter === 'scheduled' ? (
                  Object.entries(groupedRecordings.schedules).length > 0 ? (
                    Object.entries(groupedRecordings.schedules).map(([scheduleId, schedule]) => (
                      <Grid item xs={12} key={scheduleId}>
                        {/* Folder Header */}
                        <Card 
                          sx={{ 
                            mb: 2, 
                            bgcolor: alpha(theme.palette.primary.main, 0.05),
                            border: `2px solid ${alpha(theme.palette.primary.main, 0.2)}`,
                            cursor: 'pointer',
                            transition: 'all 0.3s ease',
                            '&:hover': {
                              bgcolor: alpha(theme.palette.primary.main, 0.08),
                              transform: 'translateX(4px)',
                            }
                          }}
                          onClick={() => toggleFolder(scheduleId)}
                        >
                          <CardContent>
                            <Stack direction="row" alignItems="center" justifyContent="space-between">
                              <Stack direction="row" alignItems="center" spacing={2}>
                                {expandedFolders[scheduleId] ? (
                                  <FolderOpenIcon sx={{ fontSize: 48, color: theme.palette.primary.main }} />
                                ) : (
                                  <FolderIcon sx={{ fontSize: 48, color: theme.palette.primary.main }} />
                                )}
                                <Box>
                                  <Typography variant="h6" fontWeight={600} color="primary">
                                    📁 {schedule.title}
                                  </Typography>
                                  <Typography variant="body2" color="text.secondary">
                                    {schedule.recordings.length} recording(s) • Schedule ID: {scheduleId}
                                  </Typography>
                                  {schedule.folder_path && (
                                    <Typography variant="caption" color="text.disabled">
                                      S3 Path: {schedule.folder_path}
                                    </Typography>
                                  )}
                                </Box>
                              </Stack>
                              <IconButton>
                                {expandedFolders[scheduleId] ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                              </IconButton>
                            </Stack>
                          </CardContent>
                        </Card>
                        
                        {/* Recordings inside this folder */}
                        <Collapse in={expandedFolders[scheduleId]} timeout="auto" unmountOnExit>
                          <Grid container spacing={3} sx={{ pl: 4, mb: 4 }}>
                            {schedule.recordings.map(recording => (
                              <Grid item xs={12} md={6} lg={4} key={recording.id}>
                                <RecordingCard recording={recording} />
                              </Grid>
                            ))}
                          </Grid>
                        </Collapse>
                      </Grid>
                    ))
                  ) : (
                    <Grid item xs={12}>
                      <Card sx={{ textAlign: "center", py: 8 }}>
                        <CardContent>
                          <FolderIcon sx={{ fontSize: 64, color: "text.disabled", mb: 2 }} />
                          <Typography variant="h6" color="text.secondary" mb={1}>
                            No scheduled meeting recordings found
                          </Typography>
                          <Typography variant="body2" color="text.disabled">
                            Schedule meetings will be organized into folders automatically
                          </Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                  )
                ) : (
                  // Show flat list for instant/calendar meetings or "all"
                  filteredRecordings.map(recording => (
                    <Grid item xs={12} md={6} lg={4} key={recording.id}>
                      <RecordingCard recording={recording} />
                    </Grid>
                  ))
                )}
              </>
            )}
          </Grid>

          {/* Loading indicator */}
          {loading && displayRecordings.length > 0 && (
            <Box sx={{ display: "flex", justifyContent: "center", mt: 4 }}>
              <CircularProgress />
            </Box>
          )}
        </Container>

        {/* Context Menu */}
        <Menu
          anchorEl={menuAnchor}
          open={Boolean(menuAnchor)}
          onClose={handleMenuClose}
        >
          {(() => {
            const recording = filteredRecordings.find(
              (r) => r.id === selectedRecordingId
            );

            if (!recording) return null;

            if (recording.is_trashed) {
              return [
                <MenuItem
                  key="restore"
                  onClick={() => {
                    setSelectedRecording(recording);
                    setRestoreConfirmDialog(true);
                    handleMenuClose();
                  }}
                  disabled={!recording.isUserHost}
                >
                  <ListItemIcon>
                    <RestoreIcon fontSize="small" />
                  </ListItemIcon>
                  <ListItemText>Restore Recording</ListItemText>
                </MenuItem>,

                <MenuItem
                  key="permanent-delete"
                  onClick={() => {
                    setSelectedRecording(recording);
                    setPermanentDeleteDialog(true);
                    handleMenuClose();
                  }}
                  disabled={!recording.isUserHost}
                  sx={{ color: "error.main" }}
                >
                  <ListItemIcon>
                    <PermanentDeleteIcon fontSize="small" color="error" />
                  </ListItemIcon>
                  <ListItemText>Delete Permanently</ListItemText>
                </MenuItem>,
              ];
            } else {
              return [
                <MenuItem
                  key="play"
                  onClick={() => {
                    handlePlay(recording);
                    handleMenuClose();
                  }}
                >
                  <ListItemIcon>
                    <PlayIcon fontSize="small" />
                  </ListItemIcon>
                  <ListItemText>Play Video</ListItemText>
                </MenuItem>,

                <MenuItem
                  key="download-video"
                  onClick={() => {
                    handleDownload(recording);
                    handleMenuClose();
                  }}
                >
                  <ListItemIcon>
                    <DownloadIcon fontSize="small" />
                  </ListItemIcon>
                  <ListItemText>Download Video</ListItemText>
                </MenuItem>,

                <Divider key="divider1" />,

                ...(recording.transcription_available
                  ? [
                      <MenuItem
                        key="view-transcript"
                        onClick={() => {
                          handleViewTranscript(recording);
                          handleMenuClose();
                        }}
                      >
                        <ListItemIcon>
                          <ViewIcon fontSize="small" />
                        </ListItemIcon>
                        <ListItemText>View Transcript (PDF)</ListItemText>
                      </MenuItem>,

                      <MenuItem
                        key="download-transcript"
                        onClick={() => {
                          handleDownloadTranscript(recording);
                          handleMenuClose();
                        }}
                      >
                        <ListItemIcon>
                          <PdfIcon fontSize="small" />
                        </ListItemIcon>
                        <ListItemText>Download Transcript</ListItemText>
                      </MenuItem>,
                    ]
                  : []),

                ...(recording.summary_available
                  ? [
                      <MenuItem
                        key="view-summary"
                        onClick={() => {
                          handleViewSummary(recording);
                          handleMenuClose();
                        }}
                      >
                        <ListItemIcon>
                          <ViewIcon fontSize="small" />
                        </ListItemIcon>
                        <ListItemText>View Summary (PDF)</ListItemText>
                      </MenuItem>,

                      <MenuItem
                        key="download-summary"
                        onClick={() => {
                          handleDownloadSummary(recording);
                          handleMenuClose();
                        }}
                      >
                        <ListItemIcon>
                          <SummaryIcon fontSize="small" />
                        </ListItemIcon>
                        <ListItemText>Download Summary</ListItemText>
                      </MenuItem>,
                    ]
                  : []),

                ...(recording.mindmap_available
                  ? [
                      <MenuItem
                        key="view-mindmap"
                        onClick={() => {
                          const mindmapUrl = getMindmapUrl(
                            recording.id,
                            currentUser.email,
                            currentUser.id
                          );
                          window.open(mindmapUrl, "_blank");
                          handleMenuClose();
                        }}
                      >
                        <ListItemIcon>
                          <TimelineIcon fontSize="small" />
                        </ListItemIcon>
                        <ListItemText>View Mind Map</ListItemText>
                      </MenuItem>,
                    ]
                  : []),

                <Divider key="divider2" />,

                ...(recording.isUserHost
                  ? [
                      <MenuItem
                        key="delete"
                        onClick={() => {
                          setSelectedRecording(recording);
                          setDeleteConfirmDialog(true);
                          handleMenuClose();
                        }}
                        sx={{ color: "error.main" }}
                      >
                        <ListItemIcon>
                          <DeleteIcon fontSize="small" color="error" />
                        </ListItemIcon>
                        <ListItemText>Move to Trash</ListItemText>
                      </MenuItem>,
                    ]
                  : []),
              ]
                .flat()
                .filter(Boolean);
            }
          })()}
        </Menu>

        {/* Dialogs */}
        <Dialog
          open={deleteConfirmDialog}
          onClose={() => setDeleteConfirmDialog(false)}
        >
          <DialogTitle>Move Recording to Trash?</DialogTitle>
          <DialogContent>
            <Typography>
              Are you sure you want to move "
              {selectedRecording?.meeting_name || selectedRecording?.file_name}"
              to trash? You can restore it later if needed.
            </Typography>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setDeleteConfirmDialog(false)}>
              Cancel
            </Button>
            <Button
              color="error"
              variant="contained"
              onClick={() => {
                if (selectedRecording) handleMoveToTrash(selectedRecording);
                setDeleteConfirmDialog(false);
              }}
            >
              Move to Trash
            </Button>
          </DialogActions>
        </Dialog>

        <Dialog
          open={restoreConfirmDialog}
          onClose={() => setRestoreConfirmDialog(false)}
        >
          <DialogTitle>Restore Recording?</DialogTitle>
          <DialogContent>
            <Typography>
              Are you sure you want to restore "
              {selectedRecording?.meeting_name || selectedRecording?.file_name}
              "? It will be moved back to your active recordings.
            </Typography>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setRestoreConfirmDialog(false)}>
              Cancel
            </Button>
            <Button
              color="success"
              variant="contained"
              onClick={() => {
                if (selectedRecording)
                  handleRestoreFromTrash(selectedRecording);
                setRestoreConfirmDialog(false);
              }}
            >
              Restore
            </Button>
          </DialogActions>
        </Dialog>

        <Dialog
          open={permanentDeleteDialog}
          onClose={() => setPermanentDeleteDialog(false)}
        >
          <DialogTitle sx={{ color: "error.main" }}>
            Permanently Delete Recording?
          </DialogTitle>
          <DialogContent>
            <Alert severity="error" sx={{ mb: 2 }}>
              This action cannot be undone! The recording and all associated
              files will be permanently deleted.
            </Alert>
            <Typography>
              Are you sure you want to permanently delete "
              {selectedRecording?.meeting_name || selectedRecording?.file_name}
              "?
            </Typography>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setPermanentDeleteDialog(false)}>
              Cancel
            </Button>
            <Button
              color="error"
              variant="contained"
              onClick={() => {
                if (selectedRecording) handlePermanentDelete(selectedRecording);
                setPermanentDeleteDialog(false);
              }}
            >
              Delete Forever
            </Button>
          </DialogActions>
        </Dialog>

        <Dialog
          open={emptyTrashDialog}
          onClose={() => setEmptyTrashDialog(false)}
        >
          <DialogTitle sx={{ color: "error.main" }}>Empty Trash?</DialogTitle>
          <DialogContent>
            <Alert severity="error" sx={{ mb: 2 }}>
              This will permanently delete ALL recordings in trash. This action
              cannot be undone!
            </Alert>
            <Typography>
              Are you sure you want to permanently delete all{" "}
              {trashStats.total_count} recordings in trash?
            </Typography>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setEmptyTrashDialog(false)}>Cancel</Button>
            <Button
              color="error"
              variant="contained"
              onClick={handleEmptyTrash}
            >
              Empty Trash
            </Button>
          </DialogActions>
        </Dialog>

        {/* Recording Player Dialog */}
        <Dialog
          open={playerOpen}
          onClose={() => setPlayerOpen(false)}
          maxWidth="lg"
          fullWidth
          sx={{
            "& .MuiDialog-paper": {
              minHeight: "80vh",
              maxHeight: "90vh",
            },
          }}
        >
          <DialogTitle>
            <Stack
              direction="row"
              alignItems="center"
              justifyContent="space-between"
            >
              <Typography variant="h6">
                {selectedRecording?.title || "Recording Player"}
              </Typography>
              <IconButton onClick={() => setPlayerOpen(false)}>
                <CloseIcon />
              </IconButton>
            </Stack>
          </DialogTitle>
          <DialogContent sx={{ p: 0 }}>
            {selectedRecording && (
              <RecordingPlayer recordingData={selectedRecording} />
            )}
          </DialogContent>
        </Dialog>
      </Box>
    </DashboardLayout>
  );
};

export default RecordingsPage;