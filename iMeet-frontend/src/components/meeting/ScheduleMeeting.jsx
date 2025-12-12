// COMPLETE UPDATED: src/components/meeting/ScheduleMeeting.jsx
// TRANSPARENT BACKGROUND MODAL DESIGN - ALL 2366 LINES PRESERVED

import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  TextField,
  Switch,
  FormControlLabel,
  FormGroup,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Chip,
  IconButton,
  Alert,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  Grid,
  Paper,
  Divider,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Avatar,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  ListItemSecondaryAction,
  Container,
  CircularProgress,
  Tooltip,
  InputAdornment,
  FormHelperText,
} from "@mui/material";
import {
  Schedule,
  Add,
  Delete,
  Edit,
  Email,
  Person,
  Settings,
  Security,
  Repeat,
  Notifications,
  CalendarToday,
  AccessTime,
  LocationOn,
  Description,
  People,
  VideoCall,
  ExpandMore,
  Save,
  Send,
  Preview,
  Close,
  ArrowBack,
  Lock,
  LockOpen,
  Warning,
  CheckCircle,
  GroupAdd,
  CloudUpload,
  Error as ErrorIcon,
  ArrowForward,
} from "@mui/icons-material";
import { styled } from "@mui/material/styles";
import { useNavigate, useLocation, useSearchParams } from "react-router-dom";
import { meetingsAPI } from "../../services/api";
import { useMeeting } from "../../hooks/useMeeting";
import { useAuth } from "../../hooks/useAuth";
import axios from "axios";
import SchedulePreview from '../scheduling/SchedulePreview';
import BulkInvite from "../invitations/BulkInvite";

// FIXED: Consistent API configuration
const API_BASE_URL = "https://192.168.48.201:8111";

// UPDATED: Transparent container with backdrop blur
const StyledContainer = styled(Box)(({ theme }) => ({
  minHeight: "100vh",
  background: "transparent",
  padding: theme.spacing(2),
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  [theme.breakpoints.down('sm')]: {
    padding: theme.spacing(1),
  },
}));

// UPDATED: Modal-style card with white background and transparency
const ScheduleCard = styled(Card)(({ theme }) => ({
  maxWidth: 1100,
  width: "100%",
  margin: "0 auto",
  borderRadius: theme.spacing(2),
  boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
  background: "rgba(255,255,255,0.98)",
  backdropFilter: "blur(20px)",
  border: "1px solid rgba(255,255,255,0.3)",
  [theme.breakpoints.down('md')]: {
    maxWidth: '95%',
    borderRadius: theme.spacing(1.5),
  },
  [theme.breakpoints.down('sm')]: {
    maxWidth: '98%',
    borderRadius: theme.spacing(1),
    boxShadow: "0 4px 16px rgba(0,0,0,0.1)",
  },
}));

// UPDATED: Responsive step card
const StepCard = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(3),
  borderRadius: theme.spacing(2),
  marginBottom: theme.spacing(2),
  background: "rgba(249, 250, 251, 0.8)",
  border: "1px solid rgba(229, 231, 235, 0.8)",
  [theme.breakpoints.down('md')]: {
    padding: theme.spacing(2.5),
  },
  [theme.breakpoints.down('sm')]: {
    padding: theme.spacing(2),
    borderRadius: theme.spacing(1.5),
  },
}));

const RecurrenceOption = styled(Paper)(({ theme, selected }) => ({
  padding: theme.spacing(2),
  textAlign: "center",
  cursor: "pointer",
  border: selected
    ? `2px solid ${theme.palette.primary.main}`
    : "2px solid transparent",
  borderRadius: theme.spacing(1),
  transition: "all 0.3s ease",
  "&:hover": {
    borderColor: theme.palette.primary.main,
    backgroundColor: theme.palette.primary.light + "10",
  },
  [theme.breakpoints.down('sm')]: {
    padding: theme.spacing(1.5),
  },
}));

const ParticipantChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.5),
  backgroundColor: theme.palette.primary.light,
  color: theme.palette.primary.contrastText,
  "& .MuiChip-deleteIcon": {
    color: theme.palette.primary.contrastText,
    "&:hover": {
      color: theme.palette.error.main,
    },
  },
}));

// UPDATED: Step indicator component matching the design
const StepIndicator = styled(Box)(({ theme, active, completed }) => ({
  width: 40,
  height: 40,
  borderRadius: '50%',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  fontWeight: 600,
  fontSize: '16px',
  background: completed 
    ? theme.palette.primary.main 
    : active 
    ? theme.palette.primary.main 
    : '#E5E7EB',
  color: active || completed ? '#fff' : '#9CA3AF',
  transition: 'all 0.3s ease',
}));

// UPDATED: Step connector line
const StepConnector = styled(Box)(({ theme }) => ({
  flex: 1,
  height: 2,
  background: '#E5E7EB',
  margin: '0 8px',
}));

function ScheduleMeeting({ meeting, onClose, onSave }) {
  const navigate = useNavigate();
  const location = useLocation();
  const [searchParams] = useSearchParams();
  const { user } = useAuth();
  const { refreshUpcomingMeetings, addUpcomingMeeting, upcomingMeetings } = useMeeting();
  const [activeStep, setActiveStep] = useState(0);

  // Bulk invite state
  const [bulkInviteOpen, setBulkInviteOpen] = useState(false);

  // Detect edit mode and get meeting ID
  const isDialogMode = Boolean(onClose);
  const editMeetingId = searchParams.get("edit") || (meeting && (meeting.ID || meeting.Meeting_ID));
  const isEditMode = Boolean(editMeetingId);
  const [isLoadingMeeting, setIsLoadingMeeting] = useState(false);
  const [existingMeeting, setExistingMeeting] = useState(null);

  // Use useRef to track if data has been loaded to prevent re-renders
  const dataLoadedRef = useRef(false);
  const loadingRef = useRef(false);

  console.log("🔧 ScheduleMeeting Debug:", {
    isEditMode,
    editMeetingId,
    isDialogMode,
    meeting,
    searchParams: Object.fromEntries(searchParams.entries()),
    dataLoaded: dataLoadedRef.current,
  });

  // FIXED: Better initial date/time functions with timezone handling
  const getInitialDate = useCallback(() => {
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(9, 0, 0, 0);
    return tomorrow;
  }, []);

  const getInitialTimezone = useCallback(() => {
    let timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    
    // Handle legacy timezone names
    if (timezone === "Asia/Calcutta") {
      timezone = "Asia/Kolkata";
    }
    
    // FIXED: Available timezones in the select component
    const availableTimezones = [
      "America/New_York",
      "America/Los_Angeles",
      "Europe/London", 
      "Europe/Paris",
      "Asia/Tokyo",
      "Asia/Shanghai",
      "Asia/Kolkata",
      "Australia/Sydney"
    ];
    
    // If detected timezone is not in available options, default to Asia/Kolkata
    if (!availableTimezones.includes(timezone)) {
      console.log(`🌍 Timezone ${timezone} not in available options, defaulting to Asia/Kolkata`);
      timezone = "Asia/Kolkata";
    }
    
    return timezone;
  }, []);

const getInitialMeetingData = useCallback(
  () => ({
    title: "",
    description: "",
    startDate: null,
    startTime: null,
    duration: 60,
    timezone: getInitialTimezone(),
    location: "",
    participants: [],
    settings: {
      waitingRoom: true,
      recording: false,
      allowChat: true,
      allowScreenShare: true,
      muteParticipants: false,
      requirePassword: false,
      password: "",
    },
    recurrence: {
      enabled: false,
      type: null,
      interval: 1,
      endDate: null,
      occurrences: null,
      selectedDays: [],
      selectedMonthDates: [],
      monthlyPattern: null
    },
    reminders: {
      email: true,
      browser: true,
      reminderTimes: [15, 5],
    },
  }),
  []
);

  const [meetingData, setMeetingData] = useState(getInitialMeetingData);
  const [participantEmail, setParticipantEmail] = useState("");
  const [participantName, setParticipantName] = useState("");
  const [showPreview, setShowPreview] = useState(false);
  const [isScheduling, setIsScheduling] = useState(false);
  const [validationErrors, setValidationErrors] = useState({});
  const [emailValidationError, setEmailValidationError] = useState("");

const loadExistingMeeting = useCallback(
  async (meetingId) => {
    if (loadingRef.current) {
      console.log("⚠️ Already loading, skipping...");
      return;
    }

    try {
      loadingRef.current = true;
      setIsLoadingMeeting(true);
      setValidationErrors({});
      console.log("📥 Loading existing meeting:", meetingId);

      let foundMeeting = null;

      foundMeeting = upcomingMeetings.find(
        (m) => String(m.ID || m.Meeting_ID || m.id) === String(meetingId)
      );

      console.log("🔍 Looking for meeting in local state:", { 
        meetingId, 
        upcomingMeetingsCount: upcomingMeetings.length,
        foundInLocal: !!foundMeeting,
        localMeetings: upcomingMeetings.map(m => ({ 
          id: m.ID || m.Meeting_ID || m.id, 
          title: m.title || m.Meeting_Name 
        }))
      });

      if (!foundMeeting && user?.id && user?.email) {
        console.log("🔍 Fetching fresh data from user-schedule-meetings API...");
        try {
          const response = await meetingsAPI.getUserScheduledMeetings(user.id, user.email);
          const allMeetings = response?.meetings || [];
          console.log("📥 Got meetings from user-schedule-meetings:", allMeetings.length, allMeetings);
          
          foundMeeting = allMeetings.find(
            (m) => String(m.ID || m.id) === String(meetingId)
          );

          if (foundMeeting) {
            console.log("✅ Found meeting via user-schedule-meetings endpoint");
          } else {
            console.log("❌ Meeting not found in user-schedule-meetings response");
          }
        } catch (apiError) {
          console.error("❌ Failed to fetch from user-schedule-meetings:", apiError);
        }
      } else if (!foundMeeting) {
        console.warn("⚠️ Cannot fetch meetings: missing user ID or email");
      } else {
        console.log("✅ Found meeting in local state");
      }

      if (foundMeeting) {
        console.log("✅ Processing meeting data:", foundMeeting);
        setExistingMeeting(foundMeeting);

        let startDateTime, endDateTime;
        try {
          const startTimeSource = foundMeeting.original_start_time || foundMeeting.Started_At || foundMeeting.start_time;
          const endTimeSource = foundMeeting.original_end_time || foundMeeting.Ended_At || foundMeeting.end_time;
          
          console.log("🕐 Time source analysis:", {
            original_start_time: foundMeeting.original_start_time,
            Started_At: foundMeeting.Started_At,
            start_time: foundMeeting.start_time,
            selectedSource: startTimeSource
          });
          
          startDateTime = new Date(startTimeSource);
          endDateTime = new Date(endTimeSource);

          if (isNaN(startDateTime.getTime())) {
            console.warn("⚠️ Invalid start date, using current date");
            startDateTime = new Date();
            startDateTime.setHours(9, 0, 0, 0);
          }

          if (isNaN(endDateTime.getTime())) {
            console.warn("⚠️ Invalid end date, calculating from start + duration");
            const duration = foundMeeting.duration_minutes || 60;
            endDateTime = new Date(startDateTime.getTime() + duration * 60 * 1000);
          }
          
          console.log("🕒 Parsed date times:", {
            startDateTime: startDateTime.toISOString(),
            endDateTime: endDateTime.toISOString(),
            startHours: startDateTime.getHours(),
            startMinutes: startDateTime.getMinutes()
          });
          
        } catch (dateError) {
          console.error("❌ Date parsing error:", dateError);
          const now = new Date();
          startDateTime = new Date(now.getTime() + 24 * 60 * 60 * 1000);
          startDateTime.setHours(9, 0, 0, 0);
          endDateTime = new Date(startDateTime.getTime() + 60 * 60 * 1000);
        }

        const duration =
          foundMeeting.duration_minutes ||
          Math.round((endDateTime - startDateTime) / (1000 * 60)) ||
          60;

        let participants = [];
        try {
          console.log("🔍 Parsing participants from email field...");

          if (foundMeeting.email && typeof foundMeeting.email === "string") {
            const emails = foundMeeting.email
              .split(",")
              .map((email) => email.trim())
              .filter((email) => email && email.includes("@"));

            participants = emails.map((email, index) => ({
              id: Date.now() + index,
              email: email,
              name: email.split("@")[0],
              status: "pending",
            }));

            console.log("✅ Parsed participants from email field:", participants);
          }

          if (participants.length === 0 && foundMeeting.participants) {
            if (Array.isArray(foundMeeting.participants)) {
              participants = foundMeeting.participants.map((p, index) => ({
                id: p.id || Date.now() + index,
                email: p.email || p,
                name:
                  p.name ||
                  (typeof p === "string"
                    ? p.split("@")[0]
                    : p.email?.split("@")[0]) ||
                  "Unknown",
                status: p.status || "pending",
              }));
            } else if (typeof foundMeeting.participants === "string") {
              try {
                const parsed = JSON.parse(foundMeeting.participants);
                if (Array.isArray(parsed)) {
                  participants = parsed.map((p, index) => ({
                    id: p.id || Date.now() + index,
                    email: p.email || p,
                    name:
                      p.name ||
                      (typeof p === "string"
                        ? p.split("@")[0]
                        : p.email?.split("@")[0]) ||
                      "Unknown",
                    status: p.status || "pending",
                  }));
                }
              } catch (parseError) {
                console.warn("⚠️ Failed to parse participants JSON:", parseError);
              }
            }
          }

          console.log("✅ Final parsed participants:", participants);
        } catch (participantError) {
          console.error("❌ Error parsing participants:", participantError);
          participants = [];
        }

        let recurrenceData = {
          enabled: Boolean(foundMeeting.is_recurring),
          type: foundMeeting.recurrence_type || null,
          interval: foundMeeting.recurrence_interval || 1,
          endDate: null,
          occurrences: foundMeeting.recurrence_occurrences || null,
          selectedDays: [],
          selectedMonthDates: [],
          monthlyPattern: foundMeeting.monthly_pattern || 'same-date'
        };

        console.log("🔍 Parsing recurrence end dates:", {
          recurrence_end_date: foundMeeting.recurrence_end_date,
          end_date: foundMeeting.end_date,
          is_recurring: foundMeeting.is_recurring
        });

        if (foundMeeting.recurrence_end_date) {
          try {
            const recurrenceEndDate = new Date(foundMeeting.recurrence_end_date);
            if (!isNaN(recurrenceEndDate.getTime())) {
              recurrenceData.endDate = recurrenceEndDate;
              console.log("✅ Using recurrence_end_date:", recurrenceEndDate.toISOString());
            }
          } catch (endDateError) {
            console.error("❌ Failed to parse recurrence_end_date:", endDateError);
          }
        }
        else if (foundMeeting.is_recurring && foundMeeting.end_date) {
          try {
            const seriesEndDate = new Date(foundMeeting.end_date);
            const meetingEndDate = new Date(endDateTime);
            
            const timeDiff = seriesEndDate.getTime() - meetingEndDate.getTime();
            const daysDiff = timeDiff / (1000 * 60 * 60 * 24);
            
            console.log("📅 Analyzing series end date:", {
              seriesEndDate: seriesEndDate.toISOString(),
              meetingEndDate: meetingEndDate.toISOString(),
              daysDiff: daysDiff
            });
            
            if (daysDiff > 1) {
              recurrenceData.endDate = seriesEndDate;
              console.log("✅ Using end_date as recurrence end date (series end):", seriesEndDate.toISOString());
            } else {
              console.log("ℹ️ end_date appears to be meeting end time, not series end");
            }
          } catch (endDateError) {
            console.error("❌ Failed to parse end_date:", endDateError);
          }
        }

        if (foundMeeting.selected_days) {
          try {
            recurrenceData.selectedDays = typeof foundMeeting.selected_days === 'string' 
              ? JSON.parse(foundMeeting.selected_days)
              : foundMeeting.selected_days;
            console.log("✅ Parsed selected days:", recurrenceData.selectedDays);
          } catch (e) {
            console.warn("Could not parse selected_days:", e);
            recurrenceData.selectedDays = [];
          }
        }

        if (foundMeeting.selected_month_dates) {
          try {
            recurrenceData.selectedMonthDates = typeof foundMeeting.selected_month_dates === 'string'
              ? JSON.parse(foundMeeting.selected_month_dates)
              : foundMeeting.selected_month_dates;
            console.log("✅ Parsed selected month dates:", recurrenceData.selectedMonthDates);
          } catch (e) {
            console.warn("Could not parse selected_month_dates:", e);
            recurrenceData.selectedMonthDates = [];
          }
        }

        console.log("🔄 Final parsed recurrence data:", recurrenceData);

        const title = foundMeeting.title || foundMeeting.Meeting_Name || "";
        const description = foundMeeting.description || "";
        const location = foundMeeting.location || "";

        const formStartDate = new Date(startDateTime.getTime());
        const formStartTime = new Date();
        formStartTime.setHours(startDateTime.getHours(), startDateTime.getMinutes(), 0, 0);

        console.log("🕒 CRITICAL TIME DEBUG:", {
          originalStartTime: startDateTime.toISOString(),
          originalHours: startDateTime.getHours(),
          originalMinutes: startDateTime.getMinutes(),
          formStartDateCreated: formStartDate.toISOString(),
          formStartTimeCreated: formStartTime.toISOString(),
          formStartTimeHours: formStartTime.getHours(),
          formStartTimeMinutes: formStartTime.getMinutes(),
          timeFor24HourFormat: `${String(formStartTime.getHours()).padStart(2, '0')}:${String(formStartTime.getMinutes()).padStart(2, '0')}`,
          recurrenceEndDate: recurrenceData.endDate?.toISOString() || "No end date"
        });

        const newMeetingData = {
          title: title,
          description: description,
          location: location,
          startDate: formStartDate,
          startTime: formStartTime,
          duration: duration,
          timezone: foundMeeting.timezone || getInitialTimezone(),
          participants: participants,
          settings: {
            waitingRoom: Boolean(
              foundMeeting.Waiting_Room_Enabled ||
                foundMeeting.settings_waiting_room
            ),
            recording: Boolean(
              foundMeeting.Is_Recording_Enabled ||
                foundMeeting.settings_recording
            ),
            allowChat: foundMeeting.settings_allow_chat !== false,
            allowScreenShare:
              foundMeeting.settings_allow_screen_share !== false,
            muteParticipants: Boolean(
              foundMeeting.settings_mute_participants
            ),
            requirePassword: Boolean(foundMeeting.settings_require_password),
            password: foundMeeting.settings_password || "",
          },
          recurrence: recurrenceData,
          reminders: {
            email: foundMeeting.reminders_email !== false,
            browser: foundMeeting.reminders_browser !== false,
            reminderTimes: foundMeeting.reminders_times
              ? Array.isArray(foundMeeting.reminders_times)
                ? foundMeeting.reminders_times
                : JSON.parse(foundMeeting.reminders_times || "[15, 5]")
              : [15, 5],
          },
        };

        console.log("✅ FINAL MEETING DATA SET:", {
          title: newMeetingData.title,
          startDate: newMeetingData.startDate.toISOString(),
          startTime: newMeetingData.startTime.toISOString(),
          startTimeForInput: `${String(newMeetingData.startTime.getHours()).padStart(2, '0')}:${String(newMeetingData.startTime.getMinutes()).padStart(2, '0')}`,
          duration: newMeetingData.duration,
          recurrenceEndDate: newMeetingData.recurrence.endDate?.toISOString() || "No recurrence end date",
          recurrenceEnabled: newMeetingData.recurrence.enabled,
          selectedDays: newMeetingData.recurrence.selectedDays
        });

        setMeetingData(newMeetingData);
        dataLoadedRef.current = true;
      } else {
        console.warn("⚠️ No meeting found with ID:", meetingId);
        setValidationErrors({
          api: "Meeting not found. It may have been deleted or you may not have permission to edit it."
        });
        dataLoadedRef.current = true;
      }
    } catch (error) {
      console.error("❌ Error loading meeting for edit:", error);
      setValidationErrors({
        api: "Warning: Some meeting data could not be loaded. You can still edit the meeting.",
      });
      dataLoadedRef.current = true;
    } finally {
      setIsLoadingMeeting(false);
      loadingRef.current = false;
    }
  },
  [upcomingMeetings, getInitialTimezone, user?.id, user?.email, meetingsAPI]
);

useEffect(() => {
  console.log("🔧 Initial component mount setup");
  
  if (!dataLoadedRef.current) {
    if (!isEditMode) {
      console.log("🆕 Setting up for new meeting");
      const initialData = getInitialMeetingData();
      setMeetingData({
        ...initialData,
        startDate: getInitialDate(),
        startTime: getInitialDate(),
      });
      dataLoadedRef.current = true;
    }
  }
}, []);

useEffect(() => {
  console.log("🔄 Edit mode detection triggered:", { isEditMode, editMeetingId, dataLoaded: dataLoadedRef.current });
  
  if (isEditMode && editMeetingId && !dataLoadedRef.current) {
    console.log("📥 Loading meeting data for edit mode");
    loadExistingMeeting(editMeetingId);
  }
}, [isEditMode, editMeetingId]);

useEffect(() => {
  console.log("🔄 Meeting ID changed, resetting state:", editMeetingId);

  dataLoadedRef.current = false;
  loadingRef.current = false;

  setExistingMeeting(null);
  setValidationErrors({});
  setActiveStep(0);

  if (!editMeetingId) {
    const initialData = getInitialMeetingData();
    setMeetingData({
      ...initialData,
      startDate: getInitialDate(),
      startTime: getInitialDate(),
    });
    dataLoadedRef.current = true;
  }
}, [editMeetingId]);

  const steps = [
    { label: "SELECT TIME", shortLabel: "Date & Time" },
    { label: "MEETING DETAILS", shortLabel: "Details" },
    { label: "REVIEW & CREATE", shortLabel: "Review" },
  ];

  const timezones = [
    "America/New_York",
    "America/Los_Angeles",
    "Europe/London",
    "Europe/Paris",
    "Asia/Tokyo",
    "Asia/Shanghai",
    "Asia/Kolkata",
    "Australia/Sydney",
  ];

  const durationOptions = [
    { value: 15, label: "15 minutes" },
    { value: 30, label: "30 minutes" },
    { value: 45, label: "45 minutes" },
    { value: 60, label: "1 hour" },
    { value: 90, label: "1.5 hours" },
    { value: 120, label: "2 hours" },
    { value: 180, label: "3 hours" },
    { value: 240, label: "4 hours" },
  ];

  const handleInputChange = (field, value) => {
    console.log(`🔧 Field changed: ${field} = `, value);
    setMeetingData((prev) => ({
      ...prev,
      [field]: value,
    }));

    if (validationErrors[field]) {
      setValidationErrors((prev) => ({
        ...prev,
        [field]: null,
      }));
    }
  };

  const handleSettingsChange = (setting, value) => {
    setMeetingData((prev) => ({
      ...prev,
      settings: {
        ...prev.settings,
        [setting]: value,
      },
    }));
  };

  const handleRecurrenceChange = (field, value) => {
    setMeetingData((prev) => ({
      ...prev,
      recurrence: {
        ...prev.recurrence,
        [field]: value,
      },
    }));
  };

  const handleReminderChange = (field, value) => {
    setMeetingData((prev) => ({
      ...prev,
      reminders: {
        ...prev.reminders,
        [field]: value,
      },
    }));
  };

  const addParticipant = () => {
  setEmailValidationError("");

  if (!participantEmail) {
    setEmailValidationError("Please enter at least one email address");
    return;
  }

  const emailList = participantEmail
    .split(",")
    .map((email) => email.trim())
    .filter((email) => email.length > 0);

  if (emailList.length === 0) {
    setEmailValidationError("Please enter at least one valid email address");
    return;
  }

  const invalidEmails = emailList.filter((email) => !isValidEmail(email));
  if (invalidEmails.length > 0) {
    setEmailValidationError(
      `Invalid email addresses: ${invalidEmails.join(", ")}`
    );
    return;
  }

  const existingEmails = emailList.filter((email) =>
    meetingData.participants.some(
      (p) => p.email.toLowerCase() === email.toLowerCase()
    )
  );

  if (existingEmails.length > 0) {
    setEmailValidationError(
      `These emails are already in the participant list: ${existingEmails.join(
        ", "
      )}`
    );
    return;
  }

  const newParticipants = emailList.map((email, index) => ({
    id: Date.now() + index,
    email: email,
    name: participantName || email.split("@")[0],
    status: "pending",
  }));

  setMeetingData((prev) => ({
    ...prev,
    participants: [...prev.participants, ...newParticipants],
  }));

  // CLEAR THE PARTICIPANTS VALIDATION ERROR AFTER ADDING
  if (validationErrors.participants) {
    setValidationErrors((prev) => ({
      ...prev,
      participants: null,
    }));
  }

  setParticipantEmail("");
  setParticipantName("");
};

  const removeParticipant = (participantId) => {
    setMeetingData((prev) => ({
      ...prev,
      participants: prev.participants.filter((p) => p.id !== participantId),
    }));
  };

  const isValidEmail = (email) => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  };

  const handleBulkInvitesSent = (bulkEmails) => {
    console.log("📧 Adding bulk participants to meeting:", bulkEmails);
    
    const newParticipants = bulkEmails.map((email, index) => ({
      id: Date.now() + index + Math.random(),
      email: email,
      name: email.split("@")[0],
      status: "pending",
    }));

    setMeetingData((prev) => {
      const existingEmails = new Set(prev.participants.map(p => p.email.toLowerCase()));
      const uniqueNewParticipants = newParticipants.filter(
        p => !existingEmails.has(p.email.toLowerCase())
      );

      console.log(`✅ Added ${uniqueNewParticipants.length} new participants to meeting`);

      return {
        ...prev,
        participants: [...prev.participants, ...uniqueNewParticipants],
      };
    });

    setBulkInviteOpen(false);
    
    if (newParticipants.length > 0) {
      console.log(`✅ Successfully added ${newParticipants.length} participants. They will receive invitations when you schedule the meeting.`);
    }
  };

  const validateStep = (step) => {
    const errors = {};

    switch (step) {
      case 0:
        const now = new Date();
        const selectedDateTime = new Date(meetingData.startDate);
        selectedDateTime.setHours(
          meetingData.startTime.getHours(),
          meetingData.startTime.getMinutes(),
          0,
          0
        );

        if (
          !isEditMode &&
          selectedDateTime.getTime() <= now.getTime() + 60000
        ) {
          errors.startDate = "Meeting date and time must be in the future";
        }
        break;

      case 1:
        if (!meetingData.title.trim()) {
          errors.title = "Title is required";
        }
         if (!meetingData.participants || meetingData.participants.length === 0) {
        errors.participants = "Email cannot be empty. Please enter a valid email.";
      }
      
        if (
          meetingData.settings.requirePassword &&
          !meetingData.settings.password
        ) {
          errors.password =
            "Password is required when password protection is enabled";
        }
        break;
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleNext = () => {
    if (validateStep(activeStep)) {
      setActiveStep((prev) => prev + 1);
    }
  };

  const handleBack = () => {
    setActiveStep((prev) => prev - 1);
  };

const handleScheduleMeeting = async () => {
  setIsScheduling(true);
  setValidationErrors({});
  
  try {
    console.log('🚀 Starting meeting scheduling process...');
    
    if (!user?.id) {
      throw new Error('User authentication required. Please log in again.');
    }

    const startDate = new Date(meetingData.startDate);
    const startTime = meetingData.startTime;
    
    console.log('🕒 TIMEZONE DEBUG - Input data:', {
      startDate: startDate.toISOString(),
      startTime: startTime.toISOString ? startTime.toISOString() : startTime,
      timezone: meetingData.timezone,
      startTimeHours: startTime.getHours ? startTime.getHours() : 'N/A',
      startTimeMinutes: startTime.getMinutes ? startTime.getMinutes() : 'N/A'
    });

    const startDateTime = new Date(startDate);
    
    if (startTime instanceof Date) {
      startDateTime.setHours(
        startTime.getHours(),
        startTime.getMinutes(),
        0,
        0
      );
    } else if (typeof startTime === 'string') {
      const [hours, minutes] = startTime.split(':').map(Number);
      startDateTime.setHours(hours || 0, minutes || 0, 0, 0);
    } else {
      console.warn('⚠️ Invalid startTime format, defaulting to 9:00 AM');
      startDateTime.setHours(9, 0, 0, 0);
    }

    const endDateTime = new Date(
      startDateTime.getTime() + meetingData.duration * 60000
    );

    console.log('🕒 TIMEZONE DEBUG - Calculated times:', {
      startDateTime: startDateTime.toISOString(),
      endDateTime: endDateTime.toISOString(),
      startDateTimeLocal: startDateTime.toString(),
      endDateTimeLocal: endDateTime.toString(),
      duration: meetingData.duration
    });

    if (isNaN(startDateTime.getTime()) || isNaN(endDateTime.getTime())) {
      throw new Error('Invalid date or time selected');
    }

    const participantEmails = meetingData.participants
      .map((p) => p.email)
      .filter((email) => email && isValidEmail(email));

    console.log('📧 Validated participant emails:', participantEmails);

    const recurrenceData = meetingData.recurrence || {};
    console.log('📊 Processing recurrence data:', recurrenceData);
    
    let seriesStartDate, seriesEndDate;
    
    if (recurrenceData.enabled) {
      seriesStartDate = startDateTime.toISOString();
      
      if (recurrenceData.endDate) {
        const recurrenceEndDate = new Date(recurrenceData.endDate);
        recurrenceEndDate.setHours(23, 59, 59, 999);
        seriesEndDate = recurrenceEndDate.toISOString();
      } else if (recurrenceData.occurrences) {
        const calculatedEndDate = new Date(startDateTime);
        
        switch (recurrenceData.type) {
          case 'daily':
            calculatedEndDate.setDate(calculatedEndDate.getDate() + (recurrenceData.occurrences * recurrenceData.interval));
            break;
          case 'weekly':
            calculatedEndDate.setDate(calculatedEndDate.getDate() + (recurrenceData.occurrences * 7 * recurrenceData.interval));
            break;
          case 'monthly':
            calculatedEndDate.setMonth(calculatedEndDate.getMonth() + (recurrenceData.occurrences * recurrenceData.interval));
            break;
          default:
            calculatedEndDate.setDate(calculatedEndDate.getDate() + 365);
        }
        
        seriesEndDate = calculatedEndDate.toISOString();
      } else {
        const defaultEndDate = new Date(startDateTime);
        defaultEndDate.setFullYear(defaultEndDate.getFullYear() + 1);
        seriesEndDate = defaultEndDate.toISOString();
      }
    } else {
      seriesStartDate = startDateTime.toISOString();
      seriesEndDate = endDateTime.toISOString();
    }

    console.log('📅 Calculated visibility dates:', {
      seriesStartDate,
      seriesEndDate,
      isRecurring: recurrenceData.enabled
    });
    
    const apiData = {
      title: meetingData.title?.trim(),
      Meeting_Name: meetingData.title?.trim(),
      description: meetingData.description?.trim() || '',
      location: meetingData.location?.trim() || '',
      Host_ID: user.id,
      Meeting_Type: 'ScheduleMeeting',
      
      Started_At: startDateTime.toISOString(),
      Ended_At: endDateTime.toISOString(),
      start_time: startDateTime.toISOString(),
      end_time: endDateTime.toISOString(),
      
      start_date: seriesStartDate,
      end_date: seriesEndDate,
      
      Status: 'scheduled',
      timezone: meetingData.timezone || 'Asia/Kolkata',
      duration_minutes: parseInt(meetingData.duration) || 60,
      
      Is_Recording_Enabled: Boolean(meetingData.settings.recording),
      Waiting_Room_Enabled: Boolean(meetingData.settings.waitingRoom),
      
      email: participantEmails.join(','),
      participants: JSON.stringify(meetingData.participants),
      
      recurrence: {
        enabled: Boolean(recurrenceData.enabled),
        type: recurrenceData.enabled ? recurrenceData.type : null,
        interval: recurrenceData.enabled ? (recurrenceData.interval || 1) : null,
        endDate: recurrenceData.enabled ? recurrenceData.endDate : null,
        occurrences: recurrenceData.enabled ? recurrenceData.occurrences : null,
        selectedDays: recurrenceData.enabled && recurrenceData.type === 'weekly' 
          ? recurrenceData.selectedDays : [],
        selectedMonthDates: recurrenceData.enabled && recurrenceData.type === 'monthly' 
          ? recurrenceData.selectedMonthDates : [],
        monthlyPattern: recurrenceData.enabled && recurrenceData.type === 'monthly' 
          ? (recurrenceData.monthlyPattern || 'same-date') : null
      },
      
      settings: {
        waitingRoom: Boolean(meetingData.settings.waitingRoom),
        recording: Boolean(meetingData.settings.recording),
        allowChat: Boolean(meetingData.settings.allowChat),
        allowScreenShare: Boolean(meetingData.settings.allowScreenShare),
        muteParticipants: Boolean(meetingData.settings.muteParticipants),
        requirePassword: Boolean(meetingData.settings.requirePassword),
        password: meetingData.settings.requirePassword ? meetingData.settings.password : null
      },
      
      reminders: {
        email: Boolean(meetingData.reminders.email),
        browser: Boolean(meetingData.reminders.browser),
        reminderTimes: meetingData.reminders.reminderTimes || [15, 5]
      }
    };

    if (!apiData.title) {
      throw new Error('Meeting title is required');
    }

    console.log('📤 FINAL API DATA being sent:', {
      title: apiData.title,
      Started_At: apiData.Started_At,
      start_time: apiData.start_time,
      end_time: apiData.end_time,
      timezone: apiData.timezone,
      duration_minutes: apiData.duration_minutes
    });

    let response, resultMeeting;

    if (isEditMode && editMeetingId) {
      console.log('🔄 Updating existing meeting...');
      
      try {
        response = await meetingsAPI.updateMeeting(editMeetingId, apiData);
        console.log('✅ Update response:', response);
        
        if (!response || response.error || response.Error) {
          throw new Error(response?.error || response?.Error || 'Failed to update meeting');
        }
        
        await refreshUpcomingMeetings();

        resultMeeting = {
          ID: editMeetingId,
          Meeting_ID: editMeetingId,
          Meeting_Name: meetingData.title,
          ...apiData,
          meetingLink: response.Meeting_Link || `${window.location.origin}/meeting/${editMeetingId}`,
          updatedAt: new Date().toISOString(),
        };

        console.log('✅ Meeting updated successfully:', resultMeeting);

        setTimeout(() => {
          if (isDialogMode) {
            onSave?.(resultMeeting);
            onClose?.();
          } else {
            navigate('/schedule', {
              state: {
                message: 'Meeting updated successfully!',
                meetingData: resultMeeting,
                refreshMeetings: true,
              },
            });
          }
        }, 500);
        
      } catch (updateError) {
        console.error('❌ Meeting update failed:', updateError);
        const errorData = updateError.response?.data;
let errorMsg = updateError.message || 'Failed to update meeting';

// Check for time conflict specifically
if (errorData?.Error?.includes('already have another scheduled meeting') || 
    errorData?.error?.includes('already have another scheduled meeting')) {
  errorMsg = '⚠️ Time Conflict: You already have a meeting scheduled at this time. Please select a different date or time slot.';
} else if (errorData?.Error || errorData?.error) {
  errorMsg = errorData.Error || errorData.error;
}
        throw new Error(
          updateError.response?.data?.Error || 
          updateError.response?.data?.error || 
          updateError.message || 
          'Failed to update meeting'
        );
      }
      
    } else {
      console.log('🆕 Creating new meeting...');
      
      try {
        response = await meetingsAPI.createScheduledMeeting(apiData);
        console.log('📥 Create response:', response);
        
        if (!response) {
          throw new Error('No response received from server');
        }
        
        if (response.error || response.Error) {
          throw new Error(response.error || response.Error);
        }
        
        if (!response.Meeting_ID) {
          throw new Error('Server did not return a Meeting_ID. Meeting may not have been created.');
        }
        
        console.log('✅ Meeting created successfully with ID:', response.Meeting_ID);
        
        const createdMeeting = {
          ID: response.Meeting_ID,
          Meeting_ID: response.Meeting_ID,
          Meeting_Name: meetingData.title,
          ...apiData,
          meetingLink: response.Meeting_Link || `${window.location.origin}/meeting/${response.Meeting_ID}`,
          createdAt: new Date().toISOString(),
        };
        
        addUpcomingMeeting(createdMeeting);
        console.log('✅ Meeting added to local state');
        
        setTimeout(() => {
          if (isDialogMode) {
            onSave?.(createdMeeting);
            onClose?.();
          } else {
            navigate('/schedule', {
              state: {
                message: 'Meeting scheduled successfully! Participants will receive email invitations.',
                meetingData: createdMeeting,
                refreshMeetings: true,
              },
            });
          }
        }, 500);
        
      } catch (createError) {
        console.error('❌ Meeting creation failed:', createError);
        
        let errorMessage = 'Failed to create meeting';
        
        if (createError.response) {
          const { status, data } = createError.response;
          console.error('❌ Server error details:', { status, data });
          
          switch (status) {
            case 400:
              errorMessage = `Invalid data: ${data?.Error || data?.error || 'Please check your input'}`;
              if (data?.Error?.includes('already have another scheduled meeting') || 
      data?.error?.includes('already have another scheduled meeting') ||
      data?.Error?.includes('time conflict') || 
      data?.error?.includes('time conflict')) {
    errorMessage = `⚠️ Time Conflict: You already have a meeting scheduled at this time. Please select a different date or time slot.`;
  } else {
    errorMessage = `Invalid data: ${data?.Error || data?.error || 'Please check your input'}`;
  }
              break;
            case 401:
              errorMessage = 'Authentication failed: Please log in again';
              break;
            case 403:
              errorMessage = 'Permission denied: You don\'t have access to create meetings';
              break;
            case 404:
              errorMessage = 'API endpoint not found: Please contact support';
              break;
            case 500:
              errorMessage = `Server error: ${data?.Error || data?.error || 'Internal server error. Please try again.'}`;
              break;
            case 503:
              errorMessage = 'Service temporarily unavailable: Please try again later';
              break;
            default:
              errorMessage = `Server error (${status}): ${data?.Error || data?.error || createError.message}`;
          }
        } else if (createError.code === 'ERR_NETWORK') {
          errorMessage = 'Network error: Could not connect to server. Please check your internet connection.';
        } else if (createError.code === 'ERR_CERT_AUTHORITY_INVALID') {
          errorMessage = 'SSL Certificate error: Please accept the certificate and try again';
        } else {
          errorMessage = createError.message || 'An unexpected error occurred';
        }
        
        throw new Error(errorMessage);
      }
    }
    
  } catch (error) {
    console.error('❌ Meeting scheduling failed:', error);
    
    setValidationErrors({ 
      api: error.message || 'Failed to schedule meeting. Please try again.' 
    });
    
    if (activeStep !== 0) {
      setActiveStep(0);
    }
    
  } finally {
    setIsScheduling(false);
  }
};  

  const handleRecurringDataChange = (recurringData) => {
    console.log('📊 Received recurring data from component:', recurringData);
    
    setMeetingData(prev => ({
      ...prev,
      recurrence: {
        enabled: Boolean(recurringData.isRecurring),
        type: recurringData.isRecurring ? recurringData.recurringType : null,
        interval: recurringData.isRecurring ? (recurringData.interval || 1) : 1,
        endDate: recurringData.isRecurring ? recurringData.endDate : null,
        occurrences: recurringData.isRecurring ? recurringData.occurrences : null,
        selectedDays: recurringData.isRecurring && recurringData.recurringType === 'weekly' 
          ? (recurringData.selectedDays || []) : [],
        selectedMonthDates: recurringData.isRecurring && recurringData.recurringType === 'monthly' 
          ? (recurringData.selectedMonthDates || []) : [],
        monthlyPattern: recurringData.isRecurring && recurringData.recurringType === 'monthly' 
          ? (recurringData.monthlyPattern || 'same-date') : null
      }
    }));
  };

  // UPDATED: Render modern horizontal stepper
  const renderStepIndicator = () => {
    return (
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center',
        mb: 4,
        px: 2
      }}>
        {steps.map((step, index) => (
          <React.Fragment key={index}>
            <Box sx={{ textAlign: 'center' }}>
              <StepIndicator 
                active={activeStep === index} 
                completed={activeStep > index}
              >
                {index + 1}
              </StepIndicator>
              <Typography 
                variant="caption" 
                sx={{ 
                  display: 'block',
                  mt: 1,
                  color: activeStep >= index ? 'primary.main' : 'text.secondary',
                  fontWeight: activeStep === index ? 600 : 400,
                  fontSize: { xs: '0.7rem', sm: '0.75rem' }
                }}
              >
                {step.label}
              </Typography>
            </Box>
            {index < steps.length - 1 && (
              <StepConnector sx={{ mx: { xs: 1, sm: 2 } }} />
            )}
          </React.Fragment>
        ))}
      </Box>
    );
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        // Date & Time Step
        return (
          <Box>
            <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ mb: 3 }}>
              Date & Time
            </Typography>

           {validationErrors.api && (
        <Alert 
          severity="error" 
          sx={{ 
            mb: 3,
            '& .MuiAlert-message': {
              width: '100%'
            }
          }}
          icon={<ErrorIcon />}
          onClose={() => setValidationErrors(prev => ({ ...prev, api: null }))}
        >
          <Typography variant="subtitle2" sx={{ fontWeight: 'bold', mb: 1 }}>
            Unable to Schedule Meeting
          </Typography>
          <Typography variant="body2">
            {validationErrors.api}
          </Typography>
          <Typography variant="caption" sx={{ display: 'block', mt: 1, fontStyle: 'italic' }}>
            💡 Tip: Check your existing meetings on the Schedule page to find an available time slot.
          </Typography>
        </Alert>
      )}

      {validationErrors.startDate && !validationErrors.api && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {validationErrors.startDate}
        </Alert>
      )}

            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Meeting Date *"
                  type="date"
                  value={
                    meetingData.startDate
                      ? meetingData.startDate.toISOString().split("T")[0]
                      : ""
                  }
                  onChange={(e) =>
                    handleInputChange("startDate", new Date(e.target.value))
                  }
                  error={!!validationErrors.startDate}
                  helperText={validationErrors.startDate}
                  InputLabelProps={{
                    shrink: true,
                  }}
                  inputProps={{
                    min: isEditMode
                      ? undefined
                      : new Date().toISOString().split("T")[0],
                  }}
                  sx={{
                    '& .MuiOutlinedInput-root': {
                      borderRadius: 2,
                    }
                  }}
                />
              </Grid>

              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Start Time"
                  type="time"
                  value={
                    meetingData.startTime
                      ? `${String(meetingData.startTime.getHours()).padStart(
                          2,
                          "0"
                        )}:${String(
                          meetingData.startTime.getMinutes()
                        ).padStart(2, "0")}`
                      : "09:00"
                  }
                  onChange={(e) => {
                    const [hours, minutes] = e.target.value.split(":");
                    const newTime = new Date();
                    newTime.setHours(parseInt(hours), parseInt(minutes), 0, 0);
                    handleInputChange("startTime", newTime);
                  }}
                  InputLabelProps={{
                    shrink: true,
                  }}
                  sx={{
                    '& .MuiOutlinedInput-root': {
                      borderRadius: 2,
                    }
                  }}
                />
              </Grid>

              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Duration</InputLabel>
                  <Select
                    value={meetingData.duration || 60}
                    onChange={(e) =>
                      handleInputChange("duration", e.target.value)
                    }
                    label="Duration"
                    sx={{
                      borderRadius: 2,
                    }}
                  >
                    {durationOptions.map((option) => (
                      <MenuItem key={option.value} value={option.value}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>

              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Timezone</InputLabel>
                  <Select
                    value={meetingData.timezone || getInitialTimezone()}
                    onChange={(e) =>
                      handleInputChange("timezone", e.target.value)
                    }
                    label="Timezone"
                    sx={{
                      borderRadius: 2,
                    }}
                  >
                    {timezones.map((tz) => (
                      <MenuItem key={tz} value={tz}>
                        {tz.replace("_", " ")}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
            </Grid>

            <Divider sx={{ my: 4 }} />
            
            <Card sx={{ bgcolor: 'grey.50', border: '2px dashed', borderColor: 'primary.200', borderRadius: 2 }}>
              <CardContent sx={{ p: { xs: 2, sm: 3 } }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={meetingData.recurrence?.enabled || false}
                      onChange={(e) =>
                        handleRecurrenceChange("enabled", e.target.checked)
                      }
                      color="primary"
                      size="large"
                      sx={{ 
                        '& .MuiSwitch-thumb': {
                          width: 28,
                          height: 28,
                        },
                        '& .MuiSwitch-track': {
                          borderRadius: 15,
                        }
                      }}
                    />
                  }
                  label={
                    <Box>
                      <Typography variant="h6" fontWeight="bold">
                        {meetingData.recurrence?.enabled ? '🔄 Recurring Enabled' : '⚡ Enable Recurring'}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                        {meetingData.recurrence?.enabled 
                          ? 'This meeting will repeat automatically based on your settings below'
                          : 'Turn on to schedule this meeting to repeat automatically'
                        }
                      </Typography>
                    </Box>
                  }
                  sx={{ m: 0, alignItems: 'flex-start' }}
                />
              </CardContent>
            </Card>

            {meetingData.recurrence?.enabled && (
              <Box sx={{ mt: 4 }}>
                <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
                  <Repeat sx={{ mr: 1 }} />
                  Choose Repeat Pattern
                </Typography>
                
                <Grid container spacing={2} sx={{ mb: 3 }}>
                  <Grid item xs={12} sm={6}>
                    <Card
                      onClick={() => handleRecurrenceChange('type', 'weekly')}
                      sx={{
                        cursor: 'pointer',
                        transition: 'all 0.3s ease',
                        border: meetingData.recurrence.type === 'weekly' ? '2px solid' : '1px solid',
                        borderColor: meetingData.recurrence.type === 'weekly' ? 'primary.main' : 'grey.300',
                        bgcolor: meetingData.recurrence.type === 'weekly' ? 'primary.50' : 'background.paper',
                        '&:hover': {
                          transform: 'translateY(-4px)',
                          boxShadow: 4,
                          borderColor: 'primary.main'
                        },
                        borderRadius: 2
                      }}
                    >
                      <CardContent sx={{ textAlign: 'center', p: 3 }}>
                        <CalendarToday sx={{ 
                          fontSize: 40, 
                          color: meetingData.recurrence.type === 'weekly' ? 'primary.main' : 'text.secondary',
                          mb: 2 
                        }} />
                        <Typography variant="h6" fontWeight="bold" gutterBottom>
                          Weekly
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Specific days of the week
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>

                  <Grid item xs={12} sm={6}>
                    <Card
                      onClick={() => handleRecurrenceChange('type', 'monthly')}
                      sx={{
                        cursor: 'pointer',
                        transition: 'all 0.3s ease',
                        border: meetingData.recurrence.type === 'monthly' ? '2px solid' : '1px solid',
                        borderColor: meetingData.recurrence.type === 'monthly' ? 'primary.main' : 'grey.300',
                        bgcolor: meetingData.recurrence.type === 'monthly' ? 'primary.50' : 'background.paper',
                        '&:hover': {
                          transform: 'translateY(-4px)',
                          boxShadow: 4,
                          borderColor: 'primary.main'
                        },
                        borderRadius: 2
                      }}
                    >
                      <CardContent sx={{ textAlign: 'center', p: 3 }}>
                        <Schedule sx={{ 
                          fontSize: 40, 
                          color: meetingData.recurrence.type === 'monthly' ? 'primary.main' : 'text.secondary',
                          mb: 2 
                        }} />
                        <Typography variant="h6" fontWeight="bold" gutterBottom>
                          Monthly
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Specific dates each month
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                </Grid>

                {meetingData.recurrence.type === 'weekly' && (
                  <Box sx={{ mb: 4 }}>
                    <Typography variant="h6" gutterBottom>
                      Select Days of the Week
                    </Typography>
                    
                    <Box sx={{ mb: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                      <Button
                        variant="outlined"
                        size="small"
                        onClick={() => {
                          handleRecurrenceChange('selectedDays', [1, 2, 3, 4, 5]);
                        }}
                        sx={{ borderRadius: 3 }}
                      >
                        Weekdays Only
                      </Button>
                      <Button
                        variant="outlined"
                        size="small"
                        onClick={() => {
                          handleRecurrenceChange('selectedDays', [0, 6]);
                        }}
                        sx={{ borderRadius: 3 }}
                      >
                        Weekends Only
                      </Button>
                    </Box>
                    
                    <Paper sx={{ p: 3, bgcolor: 'grey.50', borderRadius: 2 }}>
                      <Grid container spacing={1}>
                        {[
                          { value: 0, label: 'Sun', color: '#f44336' },
                          { value: 1, label: 'Mon', color: '#2196f3' },
                          { value: 2, label: 'Tue', color: '#4caf50' },
                          { value: 3, label: 'Wed', color: '#ff9800' },
                          { value: 4, label: 'Thu', color: '#9c27b0' },
                          { value: 5, label: 'Fri', color: '#3f51b5' },
                          { value: 6, label: 'Sat', color: '#795548' }
                        ].map((day) => (
                          <Grid item xs={12/7} key={day.value}>
                            <Button
                              onClick={() => {
                                const selectedDays = meetingData.recurrence.selectedDays || [];
                                const newSelectedDays = selectedDays.includes(day.value)
                                  ? selectedDays.filter(d => d !== day.value)
                                  : [...selectedDays, day.value].sort();
                                handleRecurrenceChange('selectedDays', newSelectedDays);
                              }}
                              variant={(meetingData.recurrence.selectedDays || []).includes(day.value) ? 'contained' : 'outlined'}
                              sx={{
                                width: '100%',
                                aspectRatio: '1',
                                minWidth: 'auto',
                                borderRadius: 2,
                                bgcolor: (meetingData.recurrence.selectedDays || []).includes(day.value) ? day.color : 'transparent',
                                borderColor: day.color,
                                color: (meetingData.recurrence.selectedDays || []).includes(day.value) ? 'white' : day.color,
                                '&:hover': {
                                  bgcolor: (meetingData.recurrence.selectedDays || []).includes(day.value) ? day.color : `${day.color}20`,
                                },
                                p: 1
                              }}
                            >
                              <Typography variant="caption" sx={{ fontWeight: 'bold' }}>
                                {day.label}
                              </Typography>
                            </Button>
                          </Grid>
                        ))}
                      </Grid>
                      
                      {(!meetingData.recurrence.selectedDays || meetingData.recurrence.selectedDays.length === 0) && (
                        <Alert severity="warning" sx={{ mt: 2 }}>
                          Please select at least one day for weekly recurring meetings
                        </Alert>
                      )}
                    </Paper>
                  </Box>
                )}

                {meetingData.recurrence.type === 'monthly' && (
                  <Box sx={{ mb: 4 }}>
                    <Typography variant="h6" gutterBottom>
                      Select Monthly Recurrence
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                      Select specific dates of the month (1-31):
                    </Typography>
                    <Paper sx={{ p: 2, bgcolor: 'grey.50', borderRadius: 2 }}>
                      <Grid container spacing={1}>
                        {Array.from({ length: 31 }, (_, i) => i + 1).map((date) => (
                          <Grid item key={date}>
                            <Button
                              size="small"
                              variant={(meetingData.recurrence.selectedMonthDates || []).includes(date) ? 'contained' : 'outlined'}
                              onClick={() => {
                                const selectedDates = meetingData.recurrence.selectedMonthDates || [];
                                const newDates = selectedDates.includes(date)
                                  ? selectedDates.filter(d => d !== date)
                                  : [...selectedDates, date].sort((a, b) => a - b);
                                handleRecurrenceChange('selectedMonthDates', newDates);
                              }}
                              sx={{ minWidth: 40, aspectRatio: '1' }}
                            >
                              {date}
                            </Button>
                          </Grid>
                        ))}
                      </Grid>
                      
                      {(meetingData.recurrence.selectedMonthDates || []).length > 0 && (
                        <Box sx={{ mt: 2 }}>
                          <Typography variant="body2" color="text.secondary">
                            Selected dates: {(meetingData.recurrence.selectedMonthDates || []).join(', ')}
                          </Typography>
                        </Box>
                      )}
                    </Paper>
                  </Box>
                )}

                <Box sx={{ mb: 3 }}>
                  <Typography variant="h6" gutterBottom>
                    When to Stop
                  </Typography>
                  
                  <TextField
                    fullWidth
                    label="End Date (Optional)"
                    type="date"
                    value={meetingData.recurrence.endDate ? meetingData.recurrence.endDate.toISOString().split('T')[0] : ''}
                    onChange={(e) => handleRecurrenceChange('endDate', e.target.value ? new Date(e.target.value) : null)}
                    InputLabelProps={{
                      shrink: true,
                    }}
                    inputProps={{
                      min: meetingData.startDate ? meetingData.startDate.toISOString().split('T')[0] : new Date().toISOString().split('T')[0]
                    }}
                    helperText="Leave empty to continue indefinitely"
                    sx={{
                      '& .MuiOutlinedInput-root': {
                        borderRadius: 2,
                      }
                    }}
                  />
                </Box>
              </Box>
            )}
          </Box>
        );

      case 1:
        // Meeting Details Step
        return (
          <Box>
            <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ mb: 3 }}>
              Meeting Details
            </Typography>

            {validationErrors.api && (
              <Alert 
                severity="error" 
                sx={{ mb: 2 }}
                icon={<ErrorIcon />}
                action={
                  <IconButton
                    aria-label="close"
                    color="inherit"
                    size="small"
                    onClick={() => setValidationErrors(prev => ({ ...prev, api: null }))}
                  >
                    <Close fontSize="inherit" />
                  </IconButton>
                }
              >
                <Typography variant="subtitle2" sx={{ fontWeight: 'bold' }}>
                  Meeting Creation Failed
                </Typography>
                <Typography variant="body2" sx={{ mt: 1 }}>
                  {validationErrors.api}
                </Typography>
              </Alert>
            )}

            <TextField
              fullWidth
              label="Meeting Title *"
              value={meetingData.title || ""}
              onChange={(e) => handleInputChange("title", e.target.value)}
              error={!!validationErrors.title}
              helperText={validationErrors.title}
              sx={{ 
                mb: 3,
                '& .MuiOutlinedInput-root': {
                  borderRadius: 2,
                }
              }}
              placeholder="Enter meeting title..."
            />

            <TextField
              fullWidth
              label="Description"
              value={meetingData.description || ""}
              onChange={(e) => handleInputChange("description", e.target.value)}
              multiline
              rows={3}
              sx={{ 
                mb: 3,
                '& .MuiOutlinedInput-root': {
                  borderRadius: 2,
                }
              }}
              placeholder="Add meeting description or agenda..."
            />

            <TextField
              fullWidth
              label="Location (Optional)"
              value={meetingData.location || ""}
              onChange={(e) => handleInputChange("location", e.target.value)}
              placeholder="Meeting room, address, or virtual location"
              InputProps={{
                startAdornment: (
                  <LocationOn sx={{ mr: 1, color: "text.secondary" }} />
                ),
              }}
              sx={{
                mb: 3,
                '& .MuiOutlinedInput-root': {
                  borderRadius: 2,
                }
              }}
            />

            <Divider sx={{ my: 3 }} />

            <Box sx={{ mb: 3 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2, flexWrap: 'wrap', gap: 2 }}>
                <Typography variant="h6">
                  Participants
                </Typography>
                <Button
                  variant="outlined"
                  startIcon={<GroupAdd />}
                  onClick={() => setBulkInviteOpen(true)}
                  size="small"
                  sx={{
                    borderColor: 'secondary.main',
                    color: 'secondary.main',
                    '&:hover': {
                      borderColor: 'secondary.dark',
                      backgroundColor: 'rgba(156, 39, 176, 0.04)'
                    }
                  }}
                >
                  Bulk Invite
                </Button>
              </Box>
                {/* ADD THIS ERROR ALERT */}
  {validationErrors.participants && (
    <Alert severity="error" sx={{ mb: 2 }}>
      {validationErrors.participants}
    </Alert>
  )}
              <Box sx={{ mb: 3 }}>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={10}>
                    <TextField
                      fullWidth
                      label="Email Address *"
                      value={participantEmail}
                      onChange={(e) => {
                        setParticipantEmail(e.target.value);
                        setEmailValidationError("");
                        if (validationErrors.participants) {
              setValidationErrors((prev) => ({
                ...prev,
                participants: null,
              }));
            }
          }}
                      
                      placeholder="user1@example.com, user2@example.com"
                      onKeyPress={(e) => e.key === "Enter" && addParticipant()}
                      error={!!emailValidationError}
                      helperText={
                        emailValidationError ||
                        validationErrors.participants ||
                        "Enter multiple emails separated by commas"
                      }
                      InputProps={{
                        startAdornment: (
                          <Email sx={{ mr: 1, color: "text.secondary" }} />
                        ),
                      }}
                      sx={{
                        '& .MuiOutlinedInput-root': {
                          borderRadius: 2,
                        }
                      }}
                    />
                  </Grid>
                  <Grid item xs={12} md={2}>
                    <Button
                      fullWidth
                      variant="contained"
                      onClick={addParticipant}
                      disabled={!participantEmail}
                      startIcon={<Add />}
                      sx={{ height: 56, borderRadius: 2 }}
                    >
                      {participantEmail?.includes(",") ? "Add All" : "Add"}
                    </Button>
                  </Grid>
                </Grid>
              </Box>

              {meetingData.participants &&
                meetingData.participants.length > 0 && (
                  <Box>
                    <Typography variant="subtitle1" gutterBottom>
                      Participants ({meetingData.participants.length})
                    </Typography>
                    <Paper sx={{ p: 2, bgcolor: "grey.50", borderRadius: 2 }}>
                      <List>
                        {meetingData.participants.map((participant) => (
                          <ListItem key={participant.id} divider>
                            <ListItemAvatar>
                              <Avatar sx={{ bgcolor: "primary.main" }}>
                                {participant.name
                                  ? participant.name.charAt(0).toUpperCase()
                                  : participant.email.charAt(0).toUpperCase()}
                              </Avatar>
                            </ListItemAvatar>
                            <ListItemText
                              primary={participant.name || participant.email}
                              secondary={participant.email}
                            />
                            <ListItemSecondaryAction>
                              <Tooltip title="Remove participant">
                                <IconButton
                                  edge="end"
                                  onClick={() =>
                                    removeParticipant(participant.id)
                                  }
                                  color="error"
                                  size="small"
                                >
                                  <Delete fontSize="small" />
                                </IconButton>
                              </Tooltip>
                            </ListItemSecondaryAction>
                          </ListItem>
                        ))}
                      </List>
                    </Paper>
                  </Box>
                )}
            </Box>

            <Divider sx={{ my: 3 }} />

            <Typography variant="h6" gutterBottom sx={{ mb: 2 }}>
              Meeting Settings
            </Typography>

            <FormGroup>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.settings?.waitingRoom || false}
                    onChange={(e) =>
                      handleSettingsChange("waitingRoom", e.target.checked)
                    }
                  />
                }
                label={
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <Security fontSize="small" />
                    <Typography>Enable Waiting Room</Typography>
                  </Box>
                }
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.settings?.recording || false}
                    onChange={(e) =>
                      handleSettingsChange("recording", e.target.checked)
                    }
                  />
                }
                label={<Typography>Auto-start Recording</Typography>}
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.settings?.allowChat !== false}
                    onChange={(e) =>
                      handleSettingsChange("allowChat", e.target.checked)
                    }
                  />
                }
                label={<Typography>Allow Chat</Typography>}
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.settings?.allowScreenShare !== false}
                    onChange={(e) =>
                      handleSettingsChange(
                        "allowScreenShare",
                        e.target.checked
                      )
                    }
                  />
                }
                label={<Typography>Allow Screen Sharing</Typography>}
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.settings?.muteParticipants || false}
                    onChange={(e) =>
                      handleSettingsChange(
                        "muteParticipants",
                        e.target.checked
                      )
                    }
                  />
                }
                label={<Typography>Mute Participants on Join</Typography>}
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.settings?.requirePassword || false}
                    onChange={(e) =>
                      handleSettingsChange(
                        "requirePassword",
                        e.target.checked
                      )
                    }
                  />
                }
                label={<Typography>Require Meeting Password</Typography>}
              />
            </FormGroup>

            {meetingData.settings?.requirePassword && (
              <TextField
                fullWidth
                label="Meeting Password *"
                value={meetingData.settings?.password || ""}
                onChange={(e) =>
                  handleSettingsChange("password", e.target.value)
                }
                error={!!validationErrors.password}
                helperText={validationErrors.password}
                sx={{ 
                  mt: 2,
                  '& .MuiOutlinedInput-root': {
                    borderRadius: 2,
                  }
                }}
                type="password"
                InputProps={{
                  startAdornment: (
                    <Lock sx={{ mr: 1, color: "text.secondary" }} />
                  ),
                }}
              />
            )}

            <Divider sx={{ my: 3 }} />

            <Typography variant="h6" gutterBottom sx={{ mb: 2 }}>
              Reminders
            </Typography>

            <FormGroup>
              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.reminders?.email !== false}
                    onChange={(e) =>
                      handleReminderChange("email", e.target.checked)
                    }
                  />
                }
                label={<Typography>Email Reminders</Typography>}
              />

              <FormControlLabel
                control={
                  <Switch
                    checked={meetingData.reminders?.browser !== false}
                    onChange={(e) =>
                      handleReminderChange("browser", e.target.checked)
                    }
                  />
                }
                label={<Typography>Browser Notifications</Typography>}
              />
            </FormGroup>
          </Box>
        );

      case 2:
        // Review & Create Step
        return (
          <Box>
            <Typography variant="h5" fontWeight="bold" gutterBottom sx={{ mb: 3 }}>
              Meeting Summary
            </Typography>
            
            <Paper sx={{ p: 3, bgcolor: 'grey.50', borderRadius: 2, mb: 2 }}>
              <Box sx={{ display: 'flex', alignItems: 'flex-start', mb: 2 }}>
                <CalendarToday sx={{ color: 'primary.main', mr: 2, mt: 0.5 }} />
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    {meetingData.startDate?.toLocaleDateString('en-US', { 
                      weekday: 'long', 
                      year: 'numeric', 
                      month: 'long', 
                      day: 'numeric' 
                    })}
                  </Typography>
                </Box>
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'flex-start', mb: 2 }}>
                <AccessTime sx={{ color: 'primary.main', mr: 2, mt: 0.5 }} />
                <Box>
                  <Typography variant="body2" color="text.secondary">
                    {meetingData.startTime?.toLocaleTimeString([], {
                      hour: "2-digit",
                      minute: "2-digit",
                    }) || "TBD"} - {(() => {
                      if (meetingData.startTime && meetingData.duration) {
                        const endTime = new Date(meetingData.startTime);
                        endTime.setMinutes(endTime.getMinutes() + meetingData.duration);
                        return endTime.toLocaleTimeString([], {
                          hour: "2-digit",
                          minute: "2-digit",
                        });
                      }
                      return "TBD";
                    })()}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {meetingData.duration} minutes duration
                  </Typography>
                </Box>
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'flex-start', mb: 2 }}>
                <People sx={{ color: 'primary.main', mr: 2, mt: 0.5 }} />
                <Box>
                  <Typography variant="body2" fontWeight="bold">
                    {meetingData.participants?.length || 0} Participants
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {meetingData.participants?.length > 0 
                      ? "Participants will receive invitations"
                      : "No participants added"}
                  </Typography>
                </Box>
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'flex-start' }}>
                <LocationOn sx={{ color: 'primary.main', mr: 2, mt: 0.5 }} />
                <Box>
                  <Typography variant="body2" fontWeight="bold">
                    {meetingData.timezone?.replace('_', ' ') || 'Asia/Kolkata'}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Timezone
                  </Typography>
                </Box>
              </Box>
            </Paper>

            {meetingData.title && (
              <Alert severity="success" sx={{ mb: 2, borderRadius: 2 }} icon={<CheckCircle />}>
                <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                  Meeting scheduled for:
                </Typography>
                <Typography variant="body2">
                  {meetingData.startDate?.toLocaleDateString('en-US', { 
                    weekday: 'long',
                    month: 'long',
                    day: 'numeric',
                    year: 'numeric'
                  })} at {meetingData.startTime?.toLocaleTimeString([], {
                    hour: "2-digit",
                    minute: "2-digit",
                  })} ({meetingData.duration} minutes)
                </Typography>
              </Alert>
            )}
          </Box>
        );

      default:
        return null;
    }
  };

  if (isLoadingMeeting) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          minHeight: "400px",
          flexDirection: 'column',
          gap: 2
        }}
      >
        <CircularProgress />
        <Typography variant="h6">
          Loading meeting data...
        </Typography>
      </Box>
    );
  }

  // UPDATED: Main render with transparent modal design
  return (
    <>
      <Dialog
        open={true}
        onClose={onClose || (() => navigate("/schedule"))}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 2,
            background: "rgba(255,255,255,0.98)",
            backdropFilter: "blur(20px)",
            boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
          }
        }}
        BackdropProps={{
          sx: {
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
            backdropFilter: 'blur(8px)',
          }
        }}
      >
        <DialogContent sx={{ p: { xs: 2, sm: 4 } }}>
          {/* Header with close button */}
          <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 2 }}>
            <IconButton 
              onClick={onClose || (() => navigate("/schedule"))}
              sx={{ 
                color: 'text.secondary',
                '&:hover': { bgcolor: 'rgba(0,0,0,0.04)' }
              }}
            >
              <Close />
            </IconButton>
          </Box>
          
          {/* Step Indicator */}
          {renderStepIndicator()}

          {/* Step Content */}
          <Box sx={{ mt: 3 }}>
            {renderStepContent(activeStep)}
          </Box>

          {/* Navigation Buttons */}
          <Box sx={{ 
            mt: 4, 
            pt: 3,
            borderTop: '1px solid',
            borderColor: 'divider',
            display: "flex", 
            justifyContent: "space-between",
            gap: 2,
            flexWrap: 'wrap'
          }}>
            <Button 
              onClick={handleBack} 
              disabled={activeStep === 0}
              variant="outlined"
              sx={{ 
                minWidth: 100,
                borderRadius: 2,
                textTransform: 'none',
                fontWeight: 500
              }}
            >
              Back
            </Button>

            <Box sx={{ display: 'flex', gap: 2 }}>
              <Button
                variant="text"
                onClick={onClose || (() => navigate("/schedule"))}
                sx={{ 
                  minWidth: 100,
                  borderRadius: 2,
                  textTransform: 'none',
                  color: 'text.secondary'
                }}
              >
                Cancel
              </Button>

              {activeStep < steps.length - 1 ? (
                <Button
                  variant="contained"
                  onClick={handleNext}
                  disabled={
                    (activeStep === 0 && !meetingData.startDate) ||
                    (activeStep === 1 && !meetingData.title.trim())
                  }
                  endIcon={<ArrowForward />}
                  sx={{ 
                    minWidth: 100,
                    borderRadius: 2,
                    textTransform: 'none',
                    fontWeight: 600,
                    background: 'linear-gradient(45deg, #667eea 30%, #764ba2 90%)',
                    boxShadow: '0 3px 5px 2px rgba(102, 126, 234, .3)',
                  }}
                >
                  Next
                </Button>
              ) : (
                <Button
                  variant="contained"
                  onClick={handleScheduleMeeting}
                  disabled={isScheduling || !meetingData.title.trim()}
                  startIcon={
                    isScheduling ? (
                      <CircularProgress size={20} color="inherit" />
                    ) : isEditMode ? (
                      <Save />
                    ) : (
                      <CheckCircle />
                    )
                  }
                  sx={{ 
                    minWidth: 140,
                    borderRadius: 2,
                    textTransform: 'none',
                    fontWeight: 600,
                    background: 'linear-gradient(45deg, #667eea 30%, #764ba2 90%)',
                    boxShadow: '0 3px 5px 2px rgba(102, 126, 234, .3)',
                  }}
                >
                  {isScheduling
                    ? isEditMode
                      ? "Updating..."
                      : "Creating..."
                    : isEditMode
                    ? "Update Meeting"
                    : "Create Meeting"}
                </Button>
              )}
            </Box>
          </Box>
        </DialogContent>
      </Dialog>

      {/* Bulk Invite Dialog */}
      <BulkInvite
        open={bulkInviteOpen}
        onClose={() => setBulkInviteOpen(false)}
        meetingId={meetingData?.id || editMeetingId || 'new-meeting'}
        meetingTitle={meetingData?.title || 'New Meeting'}
        onInvitesSent={handleBulkInvitesSent}
      />
    </>
  );
}

export default ScheduleMeeting;