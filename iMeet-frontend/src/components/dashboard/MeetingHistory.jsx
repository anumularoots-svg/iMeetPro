import React, { useState, useMemo, useEffect } from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  Avatar,
  Chip,
  IconButton,
  Button,
  Grid,
  Divider,
  TextField,
  InputAdornment,
  Menu,
  MenuItem,
  Tooltip,
  useTheme,
  alpha,
  Skeleton,
  Alert,
  Paper,
  Stack,
  Fade,
  Slide,
  Grow,
  Dialog,
  DialogContent,
  AppBar,
  Toolbar,
  List,
  ListItem,
  ListItemAvatar,
  useMediaQuery,
  Pagination,
  Tab,
  Tabs,
} from "@mui/material";
import {
  Search,
  FilterList,
  MoreVert,
  PlayArrow,
  Download,
  Share,
  Delete,
  VideoCall,
  Schedule,
  CalendarMonth,
  AccessTime,
  Groups,
  Star,
  StarBorder,
  Refresh,
  PersonOutline,
  History,
  EventNote,
  RecordVoiceOver,
  ExpandMore,
  ExpandLess,
  Close,
  Person,
} from "@mui/icons-material";
import {
  formatDistanceToNow,
  format,
  isToday,
  isTomorrow,
  isYesterday,
} from "date-fns";
import { useAuth } from "../../hooks/useAuth";
import { useMeeting } from "../../hooks/useMeeting";

const MeetingHistory = () => {
  const theme = useTheme();
  const fullScreen = useMediaQuery(theme.breakpoints.down("md"));
  const { user } = useAuth();
  const {
    meetings,
    summary,
    loading,
    error,
    loadMeetingHistory,
    refresh,
    setError,
    updateMeeting,
    filterMeetings,
    getStatistics,
  } = useMeeting();

  const [searchTerm, setSearchTerm] = useState("");
  const [selectedTab, setSelectedTab] = useState(0); // 0: All, 1: Hosted, 2: Participated
  const [actionAnchor, setActionAnchor] = useState(null);
  const [selectedMeeting, setSelectedMeeting] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [filterAnchor, setFilterAnchor] = useState(null);
  const [selectedFilter, setSelectedFilter] = useState("all");
  const meetingsPerPage = 10;

  const clearError = () => setError(null);

  useEffect(() => {
    if (user?.id && !loading && meetings.length === 0) {
      loadMeetingHistory();
    }
  }, [user?.id]);

  // Get filtered meetings based on tab and search
  const filteredMeetings = useMemo(() => {
    const criteria = {
      search: searchTerm,
    };

    // Apply filter dropdown filters
    switch (selectedFilter) {
      case "hosted":
        criteria.role = "host";
        break;
      case "participated":
        criteria.role = "participant";
        break;
      case "starred":
        criteria.starred = true;
        break;
      case "recorded":
        criteria.recorded = true;
        break;
      case "instant":
        criteria.type = "instant";
        break;
      case "schedule":
        criteria.type = "schedule";
        break;
      case "calendar":
        criteria.type = "calendar";
        break;
      default: // all
        break;
    }

    return filterMeetings(criteria);
  }, [meetings, searchTerm, selectedFilter, filterMeetings]);

  // Calculate pagination
  const totalPages = Math.ceil(filteredMeetings.length / meetingsPerPage);
  const startIndex = (currentPage - 1) * meetingsPerPage;
  const endIndex = startIndex + meetingsPerPage;
  const currentMeetings = filteredMeetings.slice(startIndex, endIndex);

  // Reset to page 1 when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, selectedFilter]);

  const handleTabChange = (event, newValue) => {
    setSelectedTab(newValue);
  };

  const handleFilterClick = (filter) => {
    setSelectedFilter(filter);
    setFilterAnchor(null);
  };

  const handlePageChange = (event, value) => {
    setCurrentPage(value);
  };

  const formatMeetingDate = (date) => {
    if (!date) return "Unknown time";

    const meetingDate = new Date(date);

    if (isToday(meetingDate)) {
      return `Today at ${format(meetingDate, "HH:mm")}`;
    } else if (isTomorrow(meetingDate)) {
      return `Tomorrow at ${format(meetingDate, "HH:mm")}`;
    } else if (isYesterday(meetingDate)) {
      return `Yesterday at ${format(meetingDate, "HH:mm")}`;
    } else {
      return (
        format(meetingDate, "MMM dd") + ` at ${format(meetingDate, "HH:mm")}`
      );
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await refresh();
    } finally {
      setRefreshing(false);
    }
  };

  const handleStarToggle = (meetingId) => {
    updateMeeting(meetingId, {
      starred: !meetings.find((m) => m.id === meetingId)?.starred,
    });
  };
  const formatDurationDisplay = (meeting) => {
  const isHost = meeting.is_host || meeting.user_role === 'host';
  const meetingDuration = meeting.meeting_duration || meeting.duration || '00:00';
  const participationDuration = meeting.participation_duration || meeting.duration || '00:00';
  
  if (isHost) {
    return `Duration: ${participationDuration}`;
  }
  
  if (participationDuration !== meetingDuration && participationDuration !== '00:00') {
    return (
      <>
        Duration: {meetingDuration}{' '}
        <span style={{ color: '#9CA3AF', fontSize: '0.875rem' }}>
          (You: {participationDuration})
        </span>
      </>
    );
  }
  
  return ` Duration: ${meetingDuration}`;
};

  if (!user) {
    return (
      <Box sx={{ p: 4 }}>
        <Alert
          severity="warning"
          sx={{
            mb: 2,
            borderRadius: 2,
          }}
        >
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Authentication Required
          </Typography>
          <Typography>Please log in to view your meeting history</Typography>
        </Alert>
      </Box>
    );
  }

  if (loading) {
    return (
      <Box sx={{ p: 4 }}>
        <Skeleton variant="text" sx={{ fontSize: "2rem", mb: 2 }} />
        <Skeleton
          variant="rectangular"
          width="100%"
          height={60}
          sx={{ mb: 3, borderRadius: 2 }}
        />
        {[...Array(5)].map((_, index) => (
          <Skeleton
            key={index}
            variant="rectangular"
            width="100%"
            height={80}
            sx={{ mb: 2, borderRadius: 2 }}
          />
        ))}
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3, backgroundColor: "#F8FAFC", minHeight: "100vh" }}>
      {/* Header */}
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 3,
        }}
      >
        <Typography variant="h4" sx={{ fontWeight: 600, color: "#1F2937" }}>
          Meeting History
        </Typography>
        <Typography variant="body2" sx={{ color: "#6B7280" }}>
          {user.name || user.full_name || user.email}
        </Typography>
      </Box>

      {/* Error Alert */}
      {error && (
        <Alert
          severity="error"
          sx={{ mb: 3, borderRadius: 2 }}
          onClose={clearError}
        >
          {error}
        </Alert>
      )}

      {/* Search Bar with Filter */}
      <Paper
        elevation={0}
        sx={{
          p: 2,
          mb: 3,
          borderRadius: 2,
          backgroundColor: "white",
          border: "1px solid #E5E7EB",
        }}
      >
        <Box sx={{ display: "flex", gap: 2, alignItems: "center" }}>
          <TextField
            fullWidth
            placeholder="Search meetings by title, host, or participants..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search sx={{ color: "#9CA3AF" }} />
                </InputAdornment>
              ),
            }}
            sx={{
              "& .MuiOutlinedInput-root": {
                borderRadius: 2,
                "& fieldset": {
                  border: "none",
                },
              },
            }}
          />

          <Button
            variant="outlined"
            onClick={(e) => setFilterAnchor(e.currentTarget)}
            endIcon={<ExpandMore />}
            sx={{
              minWidth: 180,
              borderRadius: 2,
              textTransform: "none",
              fontWeight: 500,
              borderColor: "#D1D5DB",
              color: "#374151",
              "&:hover": {
                borderColor: "#9CA3AF",
                backgroundColor: "#F9FAFB",
              },
            }}
          >
            {selectedFilter === "all"
              ? "All Meetings"
              : selectedFilter === "hosted"
                ? "Hosted by Me"
                : selectedFilter === "participated"
                  ? "Participated In"
                  : selectedFilter === "starred"
                    ? "Starred"
                    : selectedFilter === "recorded"
                      ? "Recorded"
                      : selectedFilter === "instant"
                        ? "Instant"
                        : selectedFilter === "schedule"
                          ? "Scheduled"
                          : selectedFilter === "calendar"
                            ? "Calendar"
                            : "Filter"}
          </Button>
        </Box>
      </Paper>

      {/* Results Info */}
      <Typography variant="body2" sx={{ color: "#6B7280", mb: 2 }}>
        Showing {startIndex + 1}-{Math.min(endIndex, filteredMeetings.length)}{" "}
        of {filteredMeetings.length} meetings
      </Typography>

      {/* Meetings List */}
      <Paper
        elevation={0}
        sx={{
          borderRadius: 2,
          backgroundColor: "white",
          border: "1px solid #E5E7EB",
          overflow: "hidden",
        }}
      >
        {currentMeetings.length === 0 ? (
          <Box sx={{ p: 8, textAlign: "center" }}>
            <VideoCall sx={{ fontSize: 48, color: "#9CA3AF", mb: 2 }} />
            <Typography
              variant="h6"
              sx={{ fontWeight: 600, color: "#4B5563", mb: 1 }}
            >
              No meetings found
            </Typography>
            <Typography variant="body2" sx={{ color: "#6B7280" }}>
              {searchTerm
                ? "Try adjusting your search terms"
                : "Your meeting history will appear here"}
            </Typography>
          </Box>
        ) : (
          <List sx={{ p: 3 }}>
            {currentMeetings.map((meeting, index) => (
              <React.Fragment key={meeting.id}>
                <ListItem
                  sx={{
                    px: 3,
                    py: 4,
                    cursor: "pointer",
                    "&:hover": {
                      "& .meeting-title": {
                        color: "#3B82F6",
                      },
                    },
                  }}
                >
                  <Box
                    sx={{
                      display: "flex",
                      alignItems: "center",
                      width: "100%",
                      gap: 2,
                    }}
                  >
                    {/* Meeting Icon */}
                    <Avatar
                      sx={{
                        width: 40,
                        height: 40,
                        backgroundColor: "#3B82F6",
                        color: "white",
                      }}
                    >
                      <VideoCall />
                    </Avatar>

                    {/* Meeting Info */}
                    <Box sx={{ flexGrow: 2, minWidth: 0 }}>
                      <Box
                        sx={{
                          display: "flex",
                          alignItems: "center",
                          gap: 2,
                          mb: 0.5,
                        }}
                      >
                        <Typography
                          variant="subtitle1"
                          className="meeting-title"
                          sx={{
                            fontWeight: 600,
                            color: "#1F2937",
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                            flexShrink: 1,
                            transition: "color 0.2s ease",
                          }}
                        >
                          {meeting.title}
                        </Typography>

                        {meeting.is_host ? (
                          <Chip
                            icon={<Person sx={{ fontSize: 14 }} />}
                            label="Hosted"
                            size="small"
                            sx={{
                              backgroundColor: "#DBEAFE",
                              color: "#1D4ED8",
                              fontSize: "0.75rem",
                              fontWeight: 600,
                              "& .MuiChip-icon": {
                                color: "#1D4ED8",
                              },
                            }}
                          />
                        ) : (
                          <Chip
                            icon={<Groups sx={{ fontSize: 14 }} />}
                            label="Participated"
                            size="small"
                            sx={{
                              backgroundColor: "#FEF3C7",
                              color: "#D97706",
                              fontSize: "0.75rem",
                              fontWeight: 600,
                              "& .MuiChip-icon": {
                                color: "#D97706",
                              },
                            }}
                          />
                        )}

                        {meeting.status === "ended" && (
                          <Chip
                            icon={<AccessTime sx={{ fontSize: 14 }} />}
                            label="Ended"
                            size="small"
                            sx={{
                              backgroundColor: "#FEE2E2",
                              color: "#e83434ff",
                              fontSize: "0.75rem",
                              fontWeight: 600,
                              "& .MuiChip-icon": {
                                color: "#DC2626",
                              },
                            }}
                          />
                        )}
                      </Box>

                      <Box
                        sx={{
                          display: "flex",
                          alignItems: "center",
                          gap: 3,
                          flexWrap: "wrap",
                        }}
                      >
                        <Box
                          sx={{
                            display: "flex",
                            alignItems: "center",
                            gap: 0.5,
                          }}
                        >
                          <AccessTime sx={{ fontSize: 16, color: "#6B7280" }} />
                          <Typography variant="body2" sx={{ color: "#6B7280" }}>
                            {formatMeetingDate(meeting.date)}
                          </Typography>
                        </Box>

                        <Box
                          sx={{
                            display: "flex",
                            alignItems: "center",
                            gap: 0.5,
                          }}
                        >
                          <Groups sx={{ fontSize: 16, color: "#6B7280" }} />
                          <Typography variant="body2" sx={{ color: "#6B7280" }}>
                            {meeting.participants} participant
                            {meeting.participants !== 1 ? "s" : ""}
                          </Typography>
                        </Box>

                        <Typography variant="body2" sx={{ color: "#6B7280" }}>
                          {formatDurationDisplay(meeting)}
                        </Typography>

                        <Typography variant="body2" sx={{ color: "#6B7280" }}>
                          {meeting.is_host
                            ? "Hosted by me"
                            : `Hosted by ${meeting.host}`}
                        </Typography>
                      </Box>
                    </Box>

                    {/* Actions */}
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleStarToggle(meeting.id);
                        }}
                        sx={{
                          color: meeting.starred ? "#F59E0B" : "#9CA3AF",
                          "&:hover": { color: "#F59E0B" },
                        }}
                      >
                        {meeting.starred ? <Star /> : <StarBorder />}
                      </IconButton>
                    </Box>
                  </Box>
                </ListItem>
                {index < currentMeetings.length - 1 && <Divider />}
              </React.Fragment>
            ))}
          </List>
        )}
      </Paper>

      {/* Pagination */}
      {totalPages > 1 && (
        <Box sx={{ display: "flex", justifyContent: "center", mt: 3 }}>
          <Pagination
            count={totalPages}
            page={currentPage}
            onChange={handlePageChange}
            color="primary"
            sx={{
              "& .MuiPaginationItem-root": {
                borderRadius: 2,
              },
            }}
          />
        </Box>
      )}

      {/* Filter Menu */}
      <Menu
        anchorEl={filterAnchor}
        open={Boolean(filterAnchor)}
        onClose={() => setFilterAnchor(null)}
        PaperProps={{
          sx: {
            borderRadius: 2,
            boxShadow: "0 4px 20px rgba(0,0,0,0.1)",
            mt: 1,
          },
        }}
      >
        <MenuItem
          onClick={() => handleFilterClick("all")}
          selected={selectedFilter === "all"}
        >
          All Meetings
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("hosted")}
          selected={selectedFilter === "hosted"}
        >
          Hosted by Me
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("participated")}
          selected={selectedFilter === "participated"}
        >
          Participated In
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("starred")}
          selected={selectedFilter === "starred"}
        >
          Starred
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("recorded")}
          selected={selectedFilter === "recorded"}
        >
          Recorded
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("instant")}
          selected={selectedFilter === "instant"}
        >
          Instant
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("schedule")}
          selected={selectedFilter === "schedule"}
        >
          Scheduled
        </MenuItem>
        <MenuItem
          onClick={() => handleFilterClick("calendar")}
          selected={selectedFilter === "calendar"}
        >
          Calendar
        </MenuItem>
      </Menu>

      {/* Action Menu */}
      <Menu
        anchorEl={actionAnchor}
        open={Boolean(actionAnchor)}
        onClose={() => setActionAnchor(null)}
        PaperProps={{
          sx: {
            borderRadius: 2,
            boxShadow: "0 4px 20px rgba(0,0,0,0.1)",
          },
        }}
      >
        {selectedMeeting?.recording && (
          <MenuItem onClick={() => setActionAnchor(null)}>
            <PlayArrow sx={{ mr: 2 }} />
            Play Recording
          </MenuItem>
        )}
      </Menu>
    </Box>
  );
};

export default MeetingHistory;
