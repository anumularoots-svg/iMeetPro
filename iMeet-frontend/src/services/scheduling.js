// COMPLETE FIXED: src/components/meeting/ScheduleMeeting.jsx

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
} from "@mui/icons-material";
import { styled } from "@mui/material/styles";
import { useNavigate, useLocation, useSearchParams } from "react-router-dom";
import { meetingsAPI } from "../../services/api";
import { useMeeting } from "../../hooks/useMeeting";
import { useAuth } from "../../hooks/useAuth";
import axios from "axios";
 // Add this import at the top with other imports
import SchedulePreview from '../scheduling/SchedulePreview';
// Import bulk invite component
import BulkInvite from "../invitations/BulkInvite";

// FIXED: Consistent API configuration
const API_BASE_URL = "https://192.168.48.201:8111";

const StyledContainer = styled(Box)(({ theme }) => ({
  minHeight: "100vh",
  background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
  padding: theme.spacing(2),
}));

const ScheduleCard = styled(Card)(({ theme }) => ({
  maxWidth: 800,
  margin: "0 auto",
  borderRadius: theme.spacing(3),
  boxShadow: "0 20px 40px rgba(0,0,0,0.1)",
  background: "rgba(255,255,255,0.95)",
  backdropFilter: "blur(10px)",
}));

const StepCard = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(3),
  borderRadius: theme.spacing(2),
  marginBottom: theme.spacing(2),
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

// Initialize state with empty values for edit mode
  // FIND this in your ScheduleMeeting.jsx and UPDATE the recurrence section:

// REPLACE: Your getInitialMeetingData function with this memoized version
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
    // FIXED: Standardized recurrence structure
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
  [] // No dependencies - this should be stable
);

  const [meetingData, setMeetingData] = useState(getInitialMeetingData);
  const [participantEmail, setParticipantEmail] = useState("");
  const [participantName, setParticipantName] = useState("");
  const [showPreview, setShowPreview] = useState(false);
  const [isScheduling, setIsScheduling] = useState(false);
  const [validationErrors, setValidationErrors] = useState({});
  const [emailValidationError, setEmailValidationError] = useState("");

  // Load existing meeting data for edit mode
  // COMPLETE REPLACEMENT for your loadExistingMeeting function in ScheduleMeeting.jsx
const loadExistingMeeting = useCallback(
  async (meetingId) => {
    // Prevent concurrent loads
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

      // First, try to find in local state (upcomingMeetings)
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

      // If not found locally, fetch from user-schedule-meetings API
      if (!foundMeeting && user?.id && user?.email) {
        console.log("🔍 Fetching fresh data from user-schedule-meetings API...");
        try {
          const response = await meetingsAPI.getUserScheduledMeetings(user.id, user.email);
          
          // FIX: Properly extract meetings array from response
          const allMeetings = response?.meetings || []; // Extract meetings array
          
          console.log("📥 Got meetings from user-schedule-meetings:", allMeetings.length, allMeetings);
          
          foundMeeting = allMeetings.find(
            (m) => String(m.ID || m.id) === String(meetingId)
          );

          if (foundMeeting) {
            console.log("✅ Found meeting via user-schedule-meetings endpoint");
          } else {
            console.log("❌ Meeting not found in user-schedule-meetings response");
            console.log("Available meetings:", allMeetings.map(m => ({ 
              id: m.ID || m.id, 
              title: m.title || m.Meeting_Name 
            })));
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

        // FIXED: Better date parsing with timezone handling
        let startDateTime, endDateTime;
        try {
          // Use original_start_time and original_end_time if available, otherwise use start_time/end_time
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

        // Parse participants from email field
        let participants = [];
        try {
          console.log("🔍 Parsing participants from email field...");

          // Primary method: Parse from email field (comma-separated)
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

          // Fallback: Parse from participants field if exists
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

        // FIXED: Enhanced recurrence data parsing with proper date handling
        let recurrenceData = {
          enabled: Boolean(foundMeeting.is_recurring),
          type: foundMeeting.recurrence_type || null,
          interval: foundMeeting.recurrence_interval || 1,
          endDate: null, // Initialize as null
          occurrences: foundMeeting.recurrence_occurrences || null,
          selectedDays: [],
          selectedMonthDates: [],
          monthlyPattern: foundMeeting.monthly_pattern || 'same-date'
        };

        // FIXED: Proper end date parsing - this was the main issue
        if (foundMeeting.recurrence_end_date) {
          try {
            const endDate = new Date(foundMeeting.recurrence_end_date);
            if (!isNaN(endDate.getTime())) {
              recurrenceData.endDate = endDate;
              console.log("✅ Parsed recurrence end date:", endDate.toISOString());
            } else {
              console.warn("⚠️ Invalid recurrence end date:", foundMeeting.recurrence_end_date);
            }
          } catch (endDateError) {
            console.error("❌ Failed to parse recurrence end date:", endDateError);
          }
        }

        // Parse selected days if it's a weekly recurring meeting
        if (foundMeeting.selected_days) {
          try {
            recurrenceData.selectedDays = typeof foundMeeting.selected_days === 'string' 
              ? JSON.parse(foundMeeting.selected_days)
              : foundMeeting.selected_days;
          } catch (e) {
            console.warn("Could not parse selected_days:", e);
            recurrenceData.selectedDays = [];
          }
        }

        // Parse selected month dates if it's a monthly recurring meeting
        if (foundMeeting.selected_month_dates) {
          try {
            recurrenceData.selectedMonthDates = typeof foundMeeting.selected_month_dates === 'string'
              ? JSON.parse(foundMeeting.selected_month_dates)
              : foundMeeting.selected_month_dates;
          } catch (e) {
            console.warn("Could not parse selected_month_dates:", e);
            recurrenceData.selectedMonthDates = [];
          }
        }

        console.log("🔄 Final parsed recurrence data:", recurrenceData);

        // Extract form data with proper fallbacks
        const title = foundMeeting.title || foundMeeting.Meeting_Name || "";
        const description = foundMeeting.description || "";
        const location = foundMeeting.location || "";

        // CRITICAL FIX: Create time objects that work with Material-UI time picker
        const formStartDate = new Date(startDateTime.getTime());
        
        // FIXED: Create a new Date object for time picker with today's date but original time
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
          timeFor24HourFormat: `${String(formStartTime.getHours()).padStart(2, '0')}:${String(formStartTime.getMinutes()).padStart(2, '0')}`
        });

        // Set meeting data with complete structure
        const newMeetingData = {
          title: title,
          description: description,
          location: location,
          startDate: formStartDate, // Date object for date picker
          startTime: formStartTime, // Time object for time picker with preserved time
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
          recurrence: recurrenceData, // Use the properly parsed recurrence data
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
          recurrenceEndDate: newMeetingData.recurrence.endDate?.toISOString() || null,
          recurrenceEnabled: newMeetingData.recurrence.enabled
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

  
// Fixed: Stable initial setup - no dependencies to prevent loop
useEffect(() => {
  console.log("🔧 Initial component mount setup");
  
  // Only run initial setup once
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
}, []); // Empty dependency array - runs only once

// Fixed: Separate useEffect for edit mode detection
useEffect(() => {
  console.log("🔄 Edit mode detection triggered:", { isEditMode, editMeetingId, dataLoaded: dataLoadedRef.current });
  
  if (isEditMode && editMeetingId && !dataLoadedRef.current) {
    console.log("📥 Loading meeting data for edit mode");
    loadExistingMeeting(editMeetingId);
  }
}, [isEditMode, editMeetingId]); // Only watch these specific values

// Fixed: Reset state when editMeetingId changes
useEffect(() => {
  console.log("🔄 Meeting ID changed, resetting state:", editMeetingId);

  // Reset refs when meeting ID changes
  dataLoadedRef.current = false;
  loadingRef.current = false;

  setExistingMeeting(null);
  setValidationErrors({});
  setActiveStep(0);

  // Reset form to initial state when switching from edit to new
  if (!editMeetingId) {
    const initialData = getInitialMeetingData();
    setMeetingData({
      ...initialData,
      startDate: getInitialDate(),
      startTime: getInitialDate(),
    });
    dataLoadedRef.current = true;
  }
}, [editMeetingId]); // Only watch editMeetingId changes

  const steps = [
    "Basic Information",
    "Date & Time",
    "Participants",
    "Settings & Recurrence",
    "Review & Schedule",
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

    // Parse comma-separated emails
    const emailList = participantEmail
      .split(",")
      .map((email) => email.trim())
      .filter((email) => email.length > 0);

    if (emailList.length === 0) {
      setEmailValidationError("Please enter at least one valid email address");
      return;
    }

    // Validate each email
    const invalidEmails = emailList.filter((email) => !isValidEmail(email));
    if (invalidEmails.length > 0) {
      setEmailValidationError(
        `Invalid email addresses: ${invalidEmails.join(", ")}`
      );
      return;
    }

    // Check for existing emails
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

    // Create new participants for all valid emails
    const newParticipants = emailList.map((email, index) => ({
      id: Date.now() + index, // Ensure unique IDs
      email: email,
      name: participantName || email.split("@")[0],
      status: "pending",
    }));

    // Add all participants at once
    setMeetingData((prev) => ({
      ...prev,
      participants: [...prev.participants, ...newParticipants],
    }));

    // Clear the form
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

  // FIXED: Bulk invite handler
  const handleBulkInvitesSent = (bulkEmails) => {
    console.log("📧 Adding bulk participants to meeting:", bulkEmails);
    
    // Convert bulk emails to participant format
    const newParticipants = bulkEmails.map((email, index) => ({
      id: Date.now() + index + Math.random(), // Ensure unique IDs
      email: email,
      name: email.split("@")[0], // Use email prefix as name
      status: "pending",
    }));

    // Add to existing participants, avoiding duplicates
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

    // Close bulk invite dialog
    setBulkInviteOpen(false);
    
    // Show success message
    if (newParticipants.length > 0) {
      // You can add a toast notification here if you have one
      console.log(`✅ Successfully added ${newParticipants.length} participants. They will receive invitations when you schedule the meeting.`);
    }
  };

  const validateStep = (step) => {
    const errors = {};

    switch (step) {
      case 0:
        if (!meetingData.title.trim()) {
          errors.title = "Title is required";
        }
        break;

      case 1:
        const now = new Date();
        const selectedDateTime = new Date(meetingData.startDate);
        selectedDateTime.setHours(
          meetingData.startTime.getHours(),
          meetingData.startTime.getMinutes(),
          0,
          0
        );

        // For edit mode, allow past dates if they're not being changed
        if (
          !isEditMode &&
          selectedDateTime.getTime() <= now.getTime() + 60000
        ) {
          errors.startDate = "Meeting date and time must be in the future";
        }
        break;

      case 3:
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

  // REPLACE: Your entire handleScheduleMeeting function
  // REPLACE your handleScheduleMeeting function in ScheduleMeeting.jsx with this updated version

const handleScheduleMeeting = async () => {
  setIsScheduling(true);
  setValidationErrors({});
  
  try {
    console.log('🚀 Starting meeting scheduling process...');
    
    // Validate user authentication
    if (!user?.id) {
      throw new Error('User authentication required. Please log in again.');
    }

    const startDateTime = new Date(meetingData.startDate);
    startDateTime.setHours(meetingData.startTime.getHours());
    startDateTime.setMinutes(meetingData.startTime.getMinutes());

    const endDateTime = new Date(
      startDateTime.getTime() + meetingData.duration * 60000
    );

    // Validate dates
    if (isNaN(startDateTime.getTime()) || isNaN(endDateTime.getTime())) {
      throw new Error('Invalid date or time selected');
    }

    // Extract participant emails for storage
    const participantEmails = meetingData.participants
      .map((p) => p.email)
      .filter((email) => email && isValidEmail(email));

    console.log('📧 Validated participant emails:', participantEmails);

    // UPDATED: Prepare recurrence data correctly for new backend
    const recurrenceData = meetingData.recurrence || {};
    console.log('📊 Processing recurrence data:', recurrenceData);
    
    // UPDATED: Calculate proper start_date and end_date for visibility window
    let seriesStartDate, seriesEndDate;
    
    if (recurrenceData.enabled) {
      // For recurring meetings:
      // start_date = when the series starts (first meeting date)  
      // end_date = when the series ends (recurrence end date or calculated)
      
      seriesStartDate = startDateTime.toISOString();
      
      if (recurrenceData.endDate) {
        // User specified an end date for the recurrence
        const recurrenceEndDate = new Date(recurrenceData.endDate);
        // Set to end of day for the recurrence end date
        recurrenceEndDate.setHours(23, 59, 59, 999);
        seriesEndDate = recurrenceEndDate.toISOString();
      } else if (recurrenceData.occurrences) {
        // Calculate end date based on number of occurrences
        const calculatedEndDate = new Date(startDateTime);
        
        // Simple calculation - can be made more sophisticated
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
            calculatedEndDate.setDate(calculatedEndDate.getDate() + 365); // Default 1 year
        }
        
        seriesEndDate = calculatedEndDate.toISOString();
      } else {
        // No end specified, default to 1 year from start
        const defaultEndDate = new Date(startDateTime);
        defaultEndDate.setFullYear(defaultEndDate.getFullYear() + 1);
        seriesEndDate = defaultEndDate.toISOString();
      }
    } else {
      // For non-recurring meetings:
      // start_date = meeting start time
      // end_date = meeting end time  
      seriesStartDate = startDateTime.toISOString();
      seriesEndDate = endDateTime.toISOString();
    }

    console.log('📅 Calculated visibility dates:', {
      seriesStartDate,
      seriesEndDate,
      isRecurring: recurrenceData.enabled
    });
    
    // UPDATED: Prepare API data with proper structure for new backend
    const apiData = {
      // Basic meeting information
      title: meetingData.title?.trim(),
      Meeting_Name: meetingData.title?.trim(),
      description: meetingData.description?.trim() || '',
      location: meetingData.location?.trim() || '',
      Host_ID: user.id,
      Meeting_Type: 'ScheduleMeeting',
      
      // Meeting timing
      Started_At: startDateTime.toISOString(),
      Ended_At: endDateTime.toISOString(),
      start_time: startDateTime.toISOString(),
      end_time: endDateTime.toISOString(),
      
      // CRITICAL: Visibility window for recurring meetings  
      start_date: seriesStartDate,
      end_date: seriesEndDate,
      
      // Meeting configuration
      Status: 'scheduled',
      timezone: meetingData.timezone || 'Asia/Kolkata',
      duration_minutes: parseInt(meetingData.duration) || 60,
      
      // Basic settings
      Is_Recording_Enabled: Boolean(meetingData.settings.recording),
      Waiting_Room_Enabled: Boolean(meetingData.settings.waitingRoom),
      
      // Participants
      email: participantEmails.join(','),
      participants: JSON.stringify(meetingData.participants),
      
      // UPDATED: Enhanced recurrence data structure
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
      
      // Settings as detailed object structure
      settings: {
        waitingRoom: Boolean(meetingData.settings.waitingRoom),
        recording: Boolean(meetingData.settings.recording),
        allowChat: Boolean(meetingData.settings.allowChat),
        allowScreenShare: Boolean(meetingData.settings.allowScreenShare),
        muteParticipants: Boolean(meetingData.settings.muteParticipants),
        requirePassword: Boolean(meetingData.settings.requirePassword),
        password: meetingData.settings.requirePassword ? meetingData.settings.password : null
      },
      
      // Reminder settings
      reminders: {
        email: Boolean(meetingData.reminders.email),
        browser: Boolean(meetingData.reminders.browser),
        reminderTimes: meetingData.reminders.reminderTimes || [15, 5]
      }
    };

    // Final validation
    if (!apiData.title) {
      throw new Error('Meeting title is required');
    }

    console.log('📤 Sending meeting data to API:', apiData);
    console.log('🔄 Recurrence data being sent:', apiData.recurrence);

    let response, resultMeeting;

    if (isEditMode && editMeetingId) {
      // UPDATE EXISTING MEETING
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
        throw new Error(
          updateError.response?.data?.Error || 
          updateError.response?.data?.error || 
          updateError.message || 
          'Failed to update meeting'
        );
      }
      
    } else {
      // CREATE NEW MEETING
      console.log('🆕 Creating new meeting...');
      
      try {
        response = await meetingsAPI.createScheduledMeeting(apiData);
        console.log('📥 Create response:', response);
        
        // CRITICAL: Comprehensive success validation
        if (!response) {
          throw new Error('No response received from server');
        }
        
        if (response.error || response.Error) {
          throw new Error(response.error || response.Error);
        }
        
        if (!response.Meeting_ID) {
          throw new Error('Server did not return a Meeting_ID. Meeting may not have been created.');
        }
        
        // SUCCESS: Meeting was created successfully
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
  // REPLACE: Your handleRecurringDataChange function with this fixed version
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

  const renderStepContent = (step) => {
    switch (step) {
      case 0: // Basic Information
        return (
          <StepCard elevation={2}>
            <Typography variant="h6" gutterBottom>
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
              sx={{ mb: 3 }}
              placeholder="Enter meeting title..."
            />

            <TextField
              fullWidth
              label="Description"
              value={meetingData.description || ""}
              onChange={(e) => handleInputChange("description", e.target.value)}
              multiline
              rows={3}
              sx={{ mb: 3 }}
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
            />
          </StepCard>
        );

     case 1: // Date & Time
  return (
    <StepCard elevation={2}>
      <Typography variant="h6" gutterBottom>
        When is your meeting?
      </Typography>

      {validationErrors.startDate && (
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

      {/* RECURRENCE TOGGLE SECTION */}
      <Divider sx={{ my: 4 }} />
      
      <Card sx={{ bgcolor: 'grey.50', border: '2px dashed', borderColor: 'primary.200' }}>
        <CardContent sx={{ p: 3 }}>
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
                <Typography variant="h5" fontWeight="bold">
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
            sx={{ m: 0 }}
          />
        </CardContent>
      </Card>

      {/* RECURRENCE OPTIONS */}
     // REPLACE your existing recurrence handling in step 1 with this:

{/* RECURRENCE OPTIONS */}
{meetingData.recurrence?.enabled && (
  <Box sx={{ mt: 4 }}>
    <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
      <Repeat sx={{ mr: 1 }} />
      Choose Repeat Pattern
    </Typography>
    
    <Grid container spacing={2} sx={{ mb: 3 }}>
      <Grid item xs={12} sm={6} md={6}>
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
            }
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

      <Grid item xs={12} sm={6} md={6}>
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
            }
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

    {/* WEEKLY OPTIONS */}
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
              handleRecurrenceChange('selectedDays', [1, 2, 3, 4, 5]); // Monday to Friday
            }}
            sx={{ borderRadius: 3 }}
          >
            Weekdays Only
          </Button>
          <Button
            variant="outlined"
            size="small"
            onClick={() => {
              handleRecurrenceChange('selectedDays', [0, 6]); // Sunday and Saturday
            }}
            sx={{ borderRadius: 3 }}
          >
            Weekends Only
          </Button>
        </Box>
        
        <Paper sx={{ p: 3, bgcolor: 'grey.50' }}>
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
                    }
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

    {/* MONTHLY OPTIONS */}
    {meetingData.recurrence.type === 'monthly' && (
      <Box sx={{ mb: 4 }}>
        <Typography variant="h6" gutterBottom>
          Select Monthly Recurrence
        </Typography>
        
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Select specific dates of the month (1-31):
        </Typography>
        <Paper sx={{ p: 2, bgcolor: 'grey.50' }}>
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

    {/* END DATE */}
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
      />
    </Box>
  </Box>
)}
    </StepCard>
  );
      case 2: // Participants - ENHANCED with Bulk Invite
        return (
          <StepCard elevation={2}>
            {/* ENHANCED: Header with Bulk Invite Button */}
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
              <Typography variant="h6">
                Who's joining?
              </Typography>
              <Button
                variant="outlined"
                startIcon={<GroupAdd />}
                onClick={() => setBulkInviteOpen(true)}
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

            <Box sx={{ mb: 3 }}>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Email Address *"
                    value={participantEmail}
                    onChange={(e) => {
                      setParticipantEmail(e.target.value);
                      setEmailValidationError("");
                    }}
                    placeholder="user1@example.com, user2@example.com"
                    onKeyPress={(e) => e.key === "Enter" && addParticipant()}
                    error={!!emailValidationError}
                    helperText={
                      emailValidationError ||
                      "Enter multiple emails separated by commas"
                    }
                    InputProps={{
                      startAdornment: (
                        <Email sx={{ mr: 1, color: "text.secondary" }} />
                      ),
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
                    sx={{ height: 56 }}
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
                  <Paper sx={{ p: 2, bgcolor: "grey.50" }}>
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
                              >
                                <Delete />
                              </IconButton>
                            </Tooltip>
                          </ListItemSecondaryAction>
                        </ListItem>
                      ))}
                    </List>
                  </Paper>
                </Box>
              )}

            <Alert severity="info" sx={{ mt: 2 }}>
              <Typography variant="body2">
                <strong>Important:</strong> Only invited participants and the
                host can see this meeting in their schedule.
              </Typography>
              <Typography variant="body2" sx={{ mt: 1 }}>
                Participants will receive email invitations with the meeting
                details and join link.
              </Typography>
              <Typography variant="body2" sx={{ mt: 1 }}>
                <strong>Tip:</strong> You can add multiple participants at once
                by entering email addresses separated by commas, or use the Bulk Invite button to upload from Excel/CSV files.
              </Typography>
            </Alert>
          </StepCard>
        );

      case 3: // Settings & Recurrence
        return (
          <Box>
            <StepCard elevation={2}>
              <Typography variant="h6" gutterBottom>
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
                      Enable Waiting Room
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
                  label="Auto-start Recording"
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
                  label="Allow Chat"
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
                  label="Allow Screen Sharing"
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
                  label="Mute Participants on Join"
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
                  label="Require Meeting Password"
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
                  sx={{ mt: 2 }}
                  type="password"
                  InputProps={{
                    startAdornment: (
                      <Lock sx={{ mr: 1, color: "text.secondary" }} />
                    ),
                  }}
                />
              )}
            </StepCard>


            <StepCard elevation={2}>
              <Typography variant="h6" gutterBottom>
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
                  label="Email Reminders"
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
                  label="Browser Notifications"
                />
              </FormGroup>
            </StepCard>
          </Box>
        );

     case 4: // Review & Schedule - FIXED to show correct host
  return (
    <StepCard elevation={2}>
      <Typography variant="h6" gutterBottom>
        Preview Your Meeting
      </Typography>
      
      {/* Use SchedulePreview component with correct host info */}
      <SchedulePreview
        meetingData={{
          ...meetingData,
          date: meetingData.startDate,
          startTime: meetingData.startTime?.toTimeString?.() || 
                    `${String(meetingData.startTime?.getHours() || 9).padStart(2, '0')}:${String(meetingData.startTime?.getMinutes() || 0).padStart(2, '0')}`,
          endTime: (() => {
            if (meetingData.startTime && meetingData.duration) {
              const endTime = new Date(meetingData.startTime);
              endTime.setMinutes(endTime.getMinutes() + meetingData.duration);
              return `${String(endTime.getHours()).padStart(2, '0')}:${String(endTime.getMinutes()).padStart(2, '0')}`;
            }
            return "10:00";
          })(),
          meetingId: editMeetingId || 'new-meeting',
          // CRITICAL FIX: Pass actual host information
          hostName: user?.full_name || user?.name || 'Host',
          host_id: user?.id,
          videoEnabled: true,
          audioEnabled: true,
          recordingEnabled: meetingData.settings?.recording,
          waitingRoomEnabled: meetingData.settings?.waitingRoom
        }}
        invitedParticipants={meetingData.participants || []}
        currentUser={user} // Pass current user to component
        onEdit={() => setActiveStep(0)} // Go back to first step to edit
        onDelete={() => {
          if (isDialogMode) {
            onClose?.();
          } else {
            navigate('/schedule');
          }
        }}
        onSchedule={() => {}} // Preview mode, no action needed
      />
      
      <Box sx={{ mt: 3, display: "flex", gap: 2, justifyContent: "center" }}>
        <Button
          variant="outlined"
          startIcon={<Preview />}
          onClick={() => setShowPreview(true)}
        >
          Preview Email Invitation
        </Button>
      </Box>
    </StepCard>
  );
      default:
        return null;
    }
  };

  // Show loading state when loading meeting data
  if (isLoadingMeeting) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          minHeight: "400px",
        }}
      >
        <CircularProgress />
        <Typography variant="h6" sx={{ ml: 2 }}>
          Loading meeting data...
        </Typography>
      </Box>
    );
  }

  // Render differently based on dialog mode
  if (isDialogMode) {
    return (
      <Box sx={{ p: 2 }}>
        <DialogTitle
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            p: 0,
            mb: 2,
          }}
        >
          <Typography variant="h6">
            {isEditMode ? "Edit Meeting" : "Schedule New Meeting"}
          </Typography>
          <IconButton onClick={onClose}>
            <Close />
          </IconButton>
        </DialogTitle>

        <Stepper activeStep={activeStep} orientation="vertical">
          {steps.map((label, index) => (
            <Step key={label}>
              <StepLabel>
                <Typography variant="h6">{label}</Typography>
              </StepLabel>
              <StepContent>
                {renderStepContent(index)}

                <Box sx={{ mt: 3, display: "flex", gap: 2 }}>
                  {activeStep > 0 && (
                    <Button onClick={handleBack} variant="outlined">
                      Back
                    </Button>
                  )}

                  {activeStep < steps.length - 1 ? (
                    <Button
                      variant="contained"
                      onClick={handleNext}
                      disabled={!meetingData.title.trim() && activeStep === 0}
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
                          <CircularProgress size={20} />
                        ) : isEditMode ? (
                          <Save />
                        ) : (
                          <Schedule />
                        )
                      }
                    >
                      {isScheduling
                        ? isEditMode
                          ? "Updating..."
                          : "Scheduling..."
                        : isEditMode
                        ? "Update Meeting"
                        : "Schedule Meeting"}
                    </Button>
                  )}

                  <Button variant="text" onClick={onClose} color="inherit">
                    Cancel
                  </Button>
                </Box>
              </StepContent>
            </Step>
          ))}
        </Stepper>

        {/* Preview Dialog */}
        <Dialog
          open={showPreview}
          onClose={() => setShowPreview(false)}
          maxWidth="sm"
          fullWidth
        >
          <DialogTitle>Email Invitation Preview</DialogTitle>
          <DialogContent>
            <Paper sx={{ p: 3, bgcolor: "grey.50" }}>
              <Typography variant="h6" gutterBottom>
                You're invited to: {meetingData.title || "Untitled Meeting"}
              </Typography>
              <Typography variant="body1" gutterBottom>
                📅 {meetingData.startDate?.toLocaleDateString() || "TBD"} at{" "}
                {meetingData.startTime?.toLocaleTimeString([], {
                  hour: "2-digit",
                  minute: "2-digit",
                }) || "TBD"}
              </Typography>
              <Typography variant="body1" gutterBottom>
                ⏱️ Duration: {meetingData.duration || 60} minutes
              </Typography>
              {meetingData.description && (
                <Typography variant="body2" sx={{ mt: 2 }}>
                  {meetingData.description}
                </Typography>
              )}
              <Button
                variant="contained"
                fullWidth
                sx={{ mt: 3 }}
                startIcon={<VideoCall />}
              >
                Join Meeting
              </Button>
            </Paper>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setShowPreview(false)}>Close Preview</Button>
          </DialogActions>
        </Dialog>

        {/* Bulk Invite Dialog */}
        <BulkInvite
          open={bulkInviteOpen}
          onClose={() => setBulkInviteOpen(false)}
          meetingId={meetingData?.id || editMeetingId || 'new-meeting'}
          meetingTitle={meetingData?.title || 'New Meeting'}
          onInvitesSent={handleBulkInvitesSent}
        />
      </Box>
    );
  }

  // Standalone mode
  return (
    <StyledContainer>
      <ScheduleCard>
        <CardContent sx={{ p: 4 }}>
          {/* Back button for standalone mode */}
          <Box sx={{ display: "flex", alignItems: "center", mb: 3 }}>
            <IconButton onClick={() => navigate("/schedule")} sx={{ mr: 2 }}>
              <ArrowBack />
            </IconButton>
            <Typography variant="h6" color="text.secondary">
              Back to Schedule
            </Typography>
          </Box>

          <Box sx={{ textAlign: "center", mb: 4 }}>
            <Schedule sx={{ fontSize: 80, color: "primary.main", mb: 2 }} />
            <Typography variant="h4" fontWeight="bold" gutterBottom>
              {isEditMode ? "Edit Meeting" : "Schedule Meeting"}
            </Typography>
            <Typography variant="body1" color="text.secondary">
              {isEditMode
                ? "Update your meeting details"
                : "Plan your meeting for the future and invite participants"}
            </Typography>
          </Box>

          <Stepper activeStep={activeStep} orientation="vertical">
            {steps.map((label, index) => (
              <Step key={label}>
                <StepLabel>
                  <Typography variant="h6">{label}</Typography>
                </StepLabel>
                <StepContent>
                  {renderStepContent(index)}

                  <Box sx={{ mt: 3, display: "flex", gap: 2 }}>
                    {activeStep > 0 && (
                      <Button onClick={handleBack} variant="outlined">
                        Back
                      </Button>
                    )}

                    {activeStep < steps.length - 1 ? (
                      <Button
                        variant="contained"
                        onClick={handleNext}
                        disabled={!meetingData.title.trim() && activeStep === 0}
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
                            <CircularProgress size={20} />
                          ) : isEditMode ? (
                            <Save />
                          ) : (
                            <Schedule />
                          )
                        }
                        sx={{
                          background:
                            "linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)",
                        }}
                      >
                        {isScheduling
                          ? isEditMode
                            ? "Updating..."
                            : "Scheduling..."
                          : isEditMode
                          ? "Update Meeting"
                          : "Schedule Meeting"}
                      </Button>
                    )}

                    <Button
                      variant="text"
                      onClick={() => navigate("/schedule")}
                      color="inherit"
                    >
                      Cancel
                    </Button>
                  </Box>
                </StepContent>
              </Step>
            ))}
          </Stepper>
        </CardContent>
      </ScheduleCard>

      {/* Preview Dialog */}
      <Dialog
        open={showPreview}
        onClose={() => setShowPreview(false)}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: { borderRadius: 3 },
        }}
      >
        <DialogTitle
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          Email Invitation Preview
          <IconButton onClick={() => setShowPreview(false)}>
            <Close />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Paper sx={{ p: 3, bgcolor: "grey.50" }}>
            <Typography variant="h6" gutterBottom>
              You're invited to: {meetingData.title || "Untitled Meeting"}
            </Typography>
            <Typography variant="body1" gutterBottom>
              📅 {meetingData.startDate?.toLocaleDateString() || "TBD"} at{" "}
              {meetingData.startTime?.toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
              }) || "TBD"}
            </Typography>
            <Typography variant="body1" gutterBottom>
              ⏱️ Duration: {meetingData.duration || 60} minutes
            </Typography>
            {meetingData.location && (
              <Typography variant="body1" gutterBottom>
                📍 Location: {meetingData.location}
              </Typography>
            )}
            {meetingData.description && (
              <Box
                sx={{
                  mt: 2,
                  p: 2,
                  bgcolor: "background.paper",
                  borderRadius: 1,
                }}
              >
                <Typography variant="body2">
                  {meetingData.description}
                </Typography>
              </Box>
            )}
            {meetingData.settings?.requirePassword && (
              <Alert severity="warning" sx={{ mt: 2 }}>
                This meeting is password protected. The password will be shared
                separately.
              </Alert>
            )}
            <Button
              variant="contained"
              fullWidth
              sx={{ mt: 3 }}
              startIcon={<VideoCall />}
            >
              Join Meeting
            </Button>
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowPreview(false)}>Close Preview</Button>
        </DialogActions>
      </Dialog>

      {/* Bulk Invite Dialog */}
      <BulkInvite
        open={bulkInviteOpen}
        onClose={() => setBulkInviteOpen(false)}
        meetingId={meetingData?.id || editMeetingId || 'new-meeting'}
        meetingTitle={meetingData?.title || 'New Meeting'}
        onInvitesSent={handleBulkInvitesSent}
      />
    </StyledContainer>
  );
}

export default ScheduleMeeting;