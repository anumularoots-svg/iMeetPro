import React from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  Avatar,
  Chip,
  Divider,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Button,
  Stack,
  useTheme,
  alpha,
  Paper,
} from "@mui/material";
import {
  Event as EventIcon,
  AccessTime as TimeIcon,
  LocationOn as LocationIcon,
  People as PeopleIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  VideocamOff as VideoIcon,
  Mic as MicIcon,
  RecordVoiceOver as RecordIcon,
  Security as SecurityIcon,
  Link as LinkIcon,
  CalendarToday as CalendarIcon,
  Repeat as RepeatIcon,
  DateRange as DateRangeIcon,
} from "@mui/icons-material";

const SchedulePreview = ({
  meetingData,
  invitedParticipants = [],
  currentUser,
  onEdit,
  onDelete,
  onSchedule,
}) => {
  const theme = useTheme();

  const formatDate = (date) => {
    if (!date) return "Invalid Date";

    try {
      const dateObj = date instanceof Date ? date : new Date(date);
      if (isNaN(dateObj.getTime())) return "Invalid Date";

      return dateObj.toLocaleDateString("en-US", {
        weekday: "long",
        year: "numeric",
        month: "long",
        day: "numeric",
      });
    } catch (error) {
      console.error("Date formatting error:", error);
      return "Invalid Date";
    }
  };

  const formatTime = (timeInput) => {
    if (!timeInput) return "Invalid Time";

    try {
      let timeObj;

      if (timeInput instanceof Date) {
        timeObj = timeInput;
      } else if (typeof timeInput === "string") {
        // Handle HH:MM format
        if (timeInput.match(/^\d{2}:\d{2}$/)) {
          timeObj = new Date(`2000-01-01T${timeInput}:00`);
        } else {
          timeObj = new Date(timeInput);
        }
      } else {
        return "Invalid Time";
      }

      if (isNaN(timeObj.getTime())) return "Invalid Time";

      return timeObj.toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      });
    } catch (error) {
      console.error("Time formatting error:", error);
      return "Invalid Time";
    }
  };

  const getDuration = () => {
    if (meetingData?.duration) {
      const duration = meetingData.duration;
      const hours = Math.floor(duration / 60);
      const minutes = duration % 60;

      if (hours > 0) {
        return `${hours}h ${minutes > 0 ? `${minutes}m` : ""}`;
      }
      return `${duration}m`;
    }

    if (meetingData?.startTime && meetingData?.endTime) {
      try {
        const start = new Date(`2000-01-01T${meetingData.startTime}`);
        const end = new Date(`2000-01-01T${meetingData.endTime}`);
        const diffMs = end - start;
        const diffMinutes = Math.floor(diffMs / (1000 * 60));
        const hours = Math.floor(diffMinutes / 60);
        const minutes = diffMinutes % 60;

        if (hours > 0) {
          return `${hours}h ${minutes > 0 ? `${minutes}m` : ""}`;
        }
        return `${diffMinutes}m`;
      } catch (error) {
        console.error("Duration calculation error:", error);
        return "1h";
      }
    }

    return "60m";
  };

  // CRITICAL FIX: Handle recurring meeting display
  const getRecurrenceInfo = () => {
    const recurrence = meetingData?.recurrence;
    if (!recurrence?.enabled) return null;

    const { type, interval, selectedDays, selectedMonthDates, endDate } =
      recurrence;

    let recurrenceText = "";
    let daysText = "";

    switch (type) {
      case "weekly":
        recurrenceText = interval === 1 ? "Weekly" : `Every ${interval} weeks`;
        if (selectedDays && selectedDays.length > 0) {
          const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
          daysText = selectedDays.map((day) => dayNames[day]).join(", ");
        }
        break;
      case "monthly":
        recurrenceText =
          interval === 1 ? "Monthly" : `Every ${interval} months`;
        if (selectedMonthDates && selectedMonthDates.length > 0) {
          daysText = `on ${selectedMonthDates.join(", ")}`;
        }
        break;
      default:
        recurrenceText = "Custom recurrence";
    }

    return {
      text: recurrenceText,
      days: daysText,
      endDate: endDate,
    };
  };

  const recurrenceInfo = getRecurrenceInfo();

  // Get the host name from currentUser or meetingData
  const getHostName = () => {
    if (currentUser?.full_name) return currentUser.full_name;
    if (currentUser?.name) return currentUser.name;
    if (meetingData?.hostName) return meetingData.hostName;
    return "Meeting Host";
  };

  return (
    <Card
      elevation={4}
      sx={{
        maxWidth: 650,
        mx: "auto",
        background: `linear-gradient(135deg, ${alpha(
          theme.palette.primary.main,
          0.1
        )} 0%, ${alpha(theme.palette.secondary.main, 0.05)} 100%)`,
        border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`,
        borderRadius: 3,
      }}
    >
      <CardContent sx={{ p: 3 }}>
        {/* Header Section */}
        <Box sx={{ display: "flex", alignItems: "flex-start", mb: 3 }}>
          <Avatar
            sx={{
              bgcolor: theme.palette.primary.main,
              width: 56,
              height: 56,
              mr: 2,
              background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
            }}
          >
            <EventIcon sx={{ fontSize: 28 }} />
          </Avatar>
          <Box sx={{ flexGrow: 1 }}>
            <Typography
              variant="h5"
              fontWeight="bold"
              color="primary"
              gutterBottom
            >
              {meetingData?.title || "Team Meeting"}
            </Typography>
            <Stack direction="row" spacing={1} sx={{ mb: 1 }}>
              <Chip
                label="Scheduled Meeting"
                color="primary"
                variant="outlined"
                size="small"
                sx={{ borderRadius: 2 }}
              />
              {recurrenceInfo && (
                <Chip
                  icon={<RepeatIcon />}
                  label="Recurring"
                  color="secondary"
                  variant="outlined"
                  size="small"
                  sx={{ borderRadius: 2 }}
                />
              )}
            </Stack>
          </Box>
          <Stack direction="row" spacing={1}>
            <IconButton
              onClick={onEdit}
              sx={{
                bgcolor: alpha(theme.palette.warning.main, 0.1),
                "&:hover": { bgcolor: alpha(theme.palette.warning.main, 0.2) },
              }}
            >
              <EditIcon color="warning" />
            </IconButton>
            <IconButton
              onClick={onDelete}
              sx={{
                bgcolor: alpha(theme.palette.error.main, 0.1),
                "&:hover": { bgcolor: alpha(theme.palette.error.main, 0.2) },
              }}
            >
              <DeleteIcon color="error" />
            </IconButton>
          </Stack>
        </Box>

        <Divider sx={{ mb: 3 }} />

        {/* Meeting Details */}
        <Stack spacing={2.5}>
          {/* Meeting Start Information */}
          <Paper
            elevation={1}
            sx={{
              p: 2,
              bgcolor: alpha(theme.palette.success.main, 0.05),
              border: `1px solid ${alpha(theme.palette.success.main, 0.2)}`,
              borderRadius: 2,
            }}
          >
            <Box sx={{ display: "flex", alignItems: "center", mb: 1 }}>
              <CalendarIcon sx={{ color: theme.palette.success.main, mr: 2 }} />
              <Typography variant="h6" fontWeight="bold" color="success.main">
                Meeting Start
              </Typography>
            </Box>
            <Typography variant="body1" fontWeight="medium">
              {formatDate(meetingData?.date || meetingData?.startDate)}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {formatTime(meetingData?.startTime)} -{" "}
              {formatTime(meetingData?.endTime)}
              <Chip
                label={getDuration()}
                size="small"
                sx={{ ml: 1, fontSize: "0.75rem" }}
                color="success"
                variant="outlined"
              />
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              {meetingData?.timezone || "Asia/Kolkata"}
            </Typography>
          </Paper>

          {/* CRITICAL FIX: Recurring Meeting Information */}
          {recurrenceInfo && (
            <Paper
              elevation={1}
              sx={{
                p: 2,
                bgcolor: alpha(theme.palette.secondary.main, 0.05),
                border: `1px solid ${alpha(theme.palette.secondary.main, 0.2)}`,
                borderRadius: 2,
              }}
            >
              <Box sx={{ display: "flex", alignItems: "center", mb: 1 }}>
                <RepeatIcon
                  sx={{ color: theme.palette.secondary.main, mr: 2 }}
                />
                <Typography
                  variant="h6"
                  fontWeight="bold"
                  color="secondary.main"
                >
                  Recurrence Pattern
                </Typography>
              </Box>
              <Typography variant="body1" fontWeight="medium">
                {recurrenceInfo.text}
                {recurrenceInfo.days && (
                  <Typography
                    component="span"
                    variant="body2"
                    color="text.secondary"
                  >
                    {" "}
                    ({recurrenceInfo.days})
                  </Typography>
                )}
              </Typography>
              {recurrenceInfo.endDate && (
                <Box sx={{ mt: 1 }}>
                  <Typography variant="body2" color="text.secondary">
                    Ends on: {formatDate(recurrenceInfo.endDate)}
                  </Typography>
                </Box>
              )}
            </Paper>
          )}

          {/* CRITICAL FIX: Series End Date for Recurring Meetings */}
          {recurrenceInfo && recurrenceInfo.endDate && (
            <Paper
              elevation={1}
              sx={{
                p: 2,
                bgcolor: alpha(theme.palette.warning.main, 0.05),
                border: `1px solid ${alpha(theme.palette.warning.main, 0.2)}`,
                borderRadius: 2,
              }}
            >
              <Box sx={{ display: "flex", alignItems: "center", mb: 1 }}>
                <DateRangeIcon
                  sx={{ color: theme.palette.warning.main, mr: 2 }}
                />
                <Typography variant="h6" fontWeight="bold" color="warning.main">
                  Final End Date
                </Typography>
                <Chip
                  label="(Recurring Series)"
                  size="small"
                  color="warning"
                  variant="outlined"
                  sx={{ ml: 1 }}
                />
              </Box>
              <Typography variant="body1" fontWeight="medium">
                {formatDate(recurrenceInfo.endDate)}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Last meeting in the recurring series
              </Typography>
            </Paper>
          )}

          {/* Description */}
          {meetingData?.description && (
            <Box>
              <Typography
                variant="body2"
                color="text.secondary"
                fontStyle="italic"
                sx={{
                  p: 2,
                  bgcolor: alpha(theme.palette.background.paper, 0.6),
                  borderRadius: 1,
                  border: `1px dashed ${alpha(
                    theme.palette.text.secondary,
                    0.3
                  )}`,
                }}
              >
                "{meetingData.description}"
              </Typography>
            </Box>
          )}

          {/* Location */}
          {meetingData?.location && (
            <Box sx={{ display: "flex", alignItems: "center" }}>
              <LocationIcon sx={{ color: theme.palette.primary.main, mr: 2 }} />
              <Typography variant="body1">{meetingData.location}</Typography>
            </Box>
          )}

          {/* Meeting Settings */}
          <Box>
            <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
              Meeting Settings
            </Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              <Chip
                icon={<VideoIcon />}
                label={meetingData?.videoEnabled ? "Video On" : "Video Off"}
                color={meetingData?.videoEnabled ? "success" : "default"}
                size="small"
                variant="outlined"
              />
              <Chip
                icon={<MicIcon />}
                label={meetingData?.audioEnabled ? "Audio On" : "Audio Off"}
                color={meetingData?.audioEnabled ? "success" : "default"}
                size="small"
                variant="outlined"
              />
              <Chip
                icon={<RecordIcon />}
                label={
                  meetingData?.recordingEnabled
                    ? "Recording On"
                    : "Recording Off"
                }
                color={meetingData?.recordingEnabled ? "warning" : "default"}
                size="small"
                variant="outlined"
              />
              <Chip
                icon={<SecurityIcon />}
                label={
                  meetingData?.waitingRoomEnabled
                    ? "Waiting Room"
                    : "Direct Join"
                }
                color={meetingData?.waitingRoomEnabled ? "info" : "default"}
                size="small"
                variant="outlined"
              />
            </Stack>
          </Box>

          {/* Participants */}
          <Box>
            <Box sx={{ display: "flex", alignItems: "center", mb: 1 }}>
              <PeopleIcon sx={{ color: theme.palette.primary.main, mr: 1 }} />
              <Typography variant="subtitle2" fontWeight="bold">
                Participants ({invitedParticipants.length + 1})
              </Typography>
            </Box>

            <List
              dense
              sx={{
                bgcolor: alpha(theme.palette.background.paper, 0.6),
                borderRadius: 2,
              }}
            >
              {/* Host */}
              <ListItem>
                <ListItemAvatar>
                  <Avatar
                    sx={{
                      bgcolor: theme.palette.primary.main,
                      width: 32,
                      height: 32,
                    }}
                  >
                    {getHostName().charAt(0).toUpperCase()}
                  </Avatar>
                </ListItemAvatar>
                <ListItemText
                  primary={`${getHostName()} (Host)`}
                  secondary="Meeting Organizer"
                />
                <Chip label="Host" color="primary" size="small" />
              </ListItem>

              {/* Invited Participants */}
              {invitedParticipants.slice(0, 3).map((participant, index) => (
                <ListItem key={participant.id || index}>
                  <ListItemAvatar>
                    <Avatar sx={{ width: 32, height: 32 }}>
                      {(
                        participant.name?.charAt(0) ||
                        participant.email?.charAt(0) ||
                        "?"
                      ).toUpperCase()}
                    </Avatar>
                  </ListItemAvatar>
                  <ListItemText
                    primary={participant.name || participant.email}
                    secondary={participant.email}
                  />
                  <Chip
                    label={
                      participant.rsvpStatus || participant.status || "Pending"
                    }
                    color={
                      participant.rsvpStatus === "accepted" ||
                      participant.status === "accepted"
                        ? "success"
                        : participant.rsvpStatus === "declined" ||
                          participant.status === "declined"
                        ? "error"
                        : "default"
                    }
                    size="small"
                    variant="outlined"
                  />
                </ListItem>
              ))}

              {invitedParticipants.length > 3 && (
                <ListItem>
                  <ListItemText
                    primary={`+${
                      invitedParticipants.length - 3
                    } more participants`}
                    sx={{ textAlign: "center", color: "text.secondary" }}
                  />
                </ListItem>
              )}
            </List>
          </Box>
        </Stack>
      </CardContent>
    </Card>
  );
};

export default SchedulePreview;