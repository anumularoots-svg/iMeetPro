import React, { useState, useEffect } from "react";
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Avatar,
  FormControlLabel,
  Switch,
  Divider,
  Alert,
  CircularProgress,
  Grid,
  IconButton,
  Tooltip,
  useTheme,
  alpha,
} from "@mui/material";
import {
  VideoCall,
  Videocam,
  VideocamOff,
  Mic,
  MicOff,
  Settings,
  ContentPaste,
  QrCodeScanner,
  Person,
  Link,
  Security,
  CheckCircle,
  ErrorOutline,
} from "@mui/icons-material";
import { useNavigate, useParams } from "react-router-dom";

const JoinMeeting = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const { meetingId: urlMeetingId } = useParams();

  const [meetingData, setMeetingData] = useState({
    meetingId: urlMeetingId || "",
    displayName: "",
    email: "",
    password: "",
  });

  const [deviceSettings, setDeviceSettings] = useState({
    camera: true,
    microphone: true,
    speaker: true,
  });

  const [meetingInfo, setMeetingInfo] = useState(null);
  const [loading, setLoading] = useState(false);
  const [validating, setValidating] = useState(false);
  const [error, setError] = useState("");
  const [step, setStep] = useState("enter"); // 'enter', 'validate', 'join'

  useEffect(() => {
    // Auto-validate if meeting ID is provided in URL
    if (urlMeetingId) {
      validateMeeting(urlMeetingId);
    }
  }, [urlMeetingId]);

  const validateMeeting = async (id) => {
    setValidating(true);
    setError("");

    try {
      // Simulate API call to validate meeting
      await new Promise((resolve) => setTimeout(resolve, 1500));

      // Mock meeting data - replace with actual API call
      const mockMeetingInfo = {
        id: id,
        title: "Weekly Team Standup",
        host: "John Doe",
        scheduledTime: new Date(),
        participants: 5,
        requiresPassword: Math.random() > 0.5,
        waitingRoomEnabled: true,
        isActive: true,
      };

      setMeetingInfo(mockMeetingInfo);
      setStep("validate");
    } catch (err) {
      setError("Meeting not found. Please check the meeting ID and try again.");
    } finally {
      setValidating(false);
    }
  };

  const handleMeetingIdSubmit = () => {
    if (!meetingData.meetingId.trim()) {
      setError("Please enter a meeting ID or paste a meeting link");
      return;
    }

    // Extract meeting ID from URL if full link is provided
    let id = meetingData.meetingId.trim();
    if (id.includes("/meeting/")) {
      id = id.split("/meeting/")[1];
    }

    validateMeeting(id);
  };

  const handleJoinMeeting = () => {
    if (!meetingData.displayName.trim()) {
      setError("Please enter your display name");
      return;
    }

    if (meetingInfo?.requiresPassword && !meetingData.password.trim()) {
      setError("This meeting requires a password");
      return;
    }

    setLoading(true);

    // Navigate with host status information
    setTimeout(() => {
      navigate(`/meeting/${meetingInfo.id}`, {
        state: {
          displayName: meetingData.displayName,
          email: meetingData.email,
          deviceSettings,
          isHost: meetingInfo.isHost, // ← Add this line
          meetingData: meetingInfo, // ← Add this line
        },
      });
    }, 1000);
  };

  const handlePasteFromClipboard = async () => {
    try {
      const text = await navigator.clipboard.readText();
      setMeetingData({ ...meetingData, meetingId: text });

      // Auto-validate if it looks like a meeting link
      if (text.includes("/meeting/")) {
        const id = text.split("/meeting/")[1];
        validateMeeting(id);
      }
    } catch (err) {
      console.error("Failed to read clipboard:", err);
    }
  };

  const toggleDevice = (device) => {
    setDeviceSettings({
      ...deviceSettings,
      [device]: !deviceSettings[device],
    });
  };

  if (loading) {
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "80vh",
          textAlign: "center",
        }}
      >
        <CircularProgress size={60} sx={{ mb: 3 }} />
        <Typography variant="h6" sx={{ mb: 1 }}>
          Joining Meeting...
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Setting up your audio and video
        </Typography>
      </Box>
    );
  }

  return (
    <Box
      sx={{
        minHeight: "100vh",
        background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        p: 2,
      }}
    >
      <Card
        sx={{
          maxWidth: 500,
          width: "100%",
          boxShadow: "0 20px 40px rgba(0,0,0,0.1)",
          borderRadius: 3,
        }}
      >
        <CardContent sx={{ p: 4 }}>
          {/* Header */}
          <Box sx={{ textAlign: "center", mb: 4 }}>
            <Avatar
              sx={{
                width: 80,
                height: 80,
                mx: "auto",
                mb: 2,
                background: "linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)",
              }}
            >
              <VideoCall sx={{ fontSize: 40 }} />
            </Avatar>
            <Typography variant="h4" sx={{ fontWeight: 700, mb: 1 }}>
              Join Meeting
            </Typography>
            <Typography variant="body1" color="text.secondary">
              Enter meeting details to join the conference
            </Typography>
          </Box>

          {error && (
            <Alert severity="error" sx={{ mb: 3 }}>
              {error}
            </Alert>
          )}

          {step === "enter" && (
            <Box>
              <TextField
                fullWidth
                label="Meeting ID or Link"
                placeholder="Enter meeting ID or paste meeting link"
                value={meetingData.meetingId}
                onChange={(e) =>
                  setMeetingData({ ...meetingData, meetingId: e.target.value })
                }
                InputProps={{
                  startAdornment: (
                    <Link sx={{ mr: 1, color: "action.active" }} />
                  ),
                  endAdornment: (
                    <Tooltip title="Paste from clipboard">
                      <IconButton onClick={handlePasteFromClipboard} edge="end">
                        <ContentPaste />
                      </IconButton>
                    </Tooltip>
                  ),
                }}
                sx={{ mb: 3 }}
                autoFocus
              />

              <Button
                fullWidth
                variant="contained"
                size="large"
                onClick={handleMeetingIdSubmit}
                disabled={validating || !meetingData.meetingId.trim()}
                sx={{
                  py: 1.5,
                  background:
                    "linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)",
                  boxShadow: "0 8px 20px rgba(33, 150, 243, 0.3)",
                }}
              >
                {validating ? (
                  <>
                    <CircularProgress size={20} sx={{ mr: 1 }} />
                    Validating...
                  </>
                ) : (
                  "Continue"
                )}
              </Button>

              <Divider sx={{ my: 3 }}>
                <Typography variant="body2" color="text.secondary">
                  or
                </Typography>
              </Divider>

              <Button
                fullWidth
                variant="outlined"
                startIcon={<QrCodeScanner />}
                sx={{ py: 1.5 }}
              >
                Scan QR Code
              </Button>
            </Box>
          )}

          {step === "validate" && meetingInfo && (
            <Box>
              {/* Meeting Info */}
              <Card
                sx={{
                  mb: 3,
                  backgroundColor: alpha(theme.palette.success.main, 0.05),
                }}
              >
                <CardContent sx={{ p: 2 }}>
                  <Box sx={{ display: "flex", alignItems: "center", mb: 2 }}>
                    <CheckCircle color="success" sx={{ mr: 1 }} />
                    <Typography variant="h6" sx={{ fontWeight: 600 }}>
                      Meeting Found
                    </Typography>
                  </Box>
                  <Typography variant="body1" sx={{ fontWeight: 500, mb: 1 }}>
                    {meetingInfo.title}
                  </Typography>
                  {/* <Typography variant="body2" color="text.secondary">
                    Hosted by {meetingInfo.host} • {meetingInfo.participants} participants
                  </Typography> */}
                  <Typography variant="body2" color="text.secondary">
                    Hosted by {meetingInfo.host} • {meetingInfo.participants}{" "}
                    participants
                    {meetingInfo.isHost && (
                      <Chip
                        label="You are the host"
                        size="small"
                        color="primary"
                        sx={{ ml: 1 }}
                      />
                    )}
                  </Typography>
                </CardContent>
              </Card>

              {/* User Details Form */}
              <TextField
                fullWidth
                label="Your Name"
                placeholder="Enter your display name"
                value={meetingData.displayName}
                onChange={(e) =>
                  setMeetingData({
                    ...meetingData,
                    displayName: e.target.value,
                  })
                }
                InputProps={{
                  startAdornment: (
                    <Person sx={{ mr: 1, color: "action.active" }} />
                  ),
                }}
                sx={{ mb: 2 }}
                required
              />

              <TextField
                fullWidth
                label="Email (Optional)"
                placeholder="your.email@example.com"
                value={meetingData.email}
                onChange={(e) =>
                  setMeetingData({ ...meetingData, email: e.target.value })
                }
                type="email"
                sx={{ mb: 2 }}
              />

              {meetingInfo.requiresPassword && (
                <TextField
                  fullWidth
                  label="Meeting Password"
                  placeholder="Enter meeting password"
                  value={meetingData.password}
                  onChange={(e) =>
                    setMeetingData({ ...meetingData, password: e.target.value })
                  }
                  type="password"
                  InputProps={{
                    startAdornment: (
                      <Security sx={{ mr: 1, color: "action.active" }} />
                    ),
                  }}
                  sx={{ mb: 2 }}
                  required
                />
              )}

              {/* Device Settings */}
              <Card
                sx={{
                  mb: 3,
                  backgroundColor: alpha(theme.palette.info.main, 0.05),
                }}
              >
                <CardContent sx={{ p: 2 }}>
                  <Typography
                    variant="subtitle1"
                    sx={{ fontWeight: 600, mb: 2 }}
                  >
                    Audio & Video Settings
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12}>
                      <Box
                        sx={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                        }}
                      >
                        <Box sx={{ display: "flex", alignItems: "center" }}>
                          {deviceSettings.camera ? (
                            <Videocam color="success" />
                          ) : (
                            <VideocamOff color="error" />
                          )}
                          <Typography sx={{ ml: 1 }}>Camera</Typography>
                        </Box>
                        <Switch
                          checked={deviceSettings.camera}
                          onChange={() => toggleDevice("camera")}
                          color="primary"
                        />
                      </Box>
                    </Grid>
                    <Grid item xs={12}>
                      <Box
                        sx={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                        }}
                      >
                        <Box sx={{ display: "flex", alignItems: "center" }}>
                          {deviceSettings.microphone ? (
                            <Mic color="success" />
                          ) : (
                            <MicOff color="error" />
                          )}
                          <Typography sx={{ ml: 1 }}>Microphone</Typography>
                        </Box>
                        <Switch
                          checked={deviceSettings.microphone}
                          onChange={() => toggleDevice("microphone")}
                          color="primary"
                        />
                      </Box>
                    </Grid>
                  </Grid>
                </CardContent>
              </Card>

              {meetingInfo.waitingRoomEnabled && (
                <Alert severity="info" sx={{ mb: 3 }}>
                  This meeting has a waiting room enabled. You'll need to wait
                  for the host to admit you.
                </Alert>
              )}

              <Button
                fullWidth
                variant="contained"
                size="large"
                onClick={handleJoinMeeting}
                disabled={!meetingData.displayName.trim()}
                sx={{
                  py: 1.5,
                  background:
                    "linear-gradient(45deg, #4CAF50 30%, #45a049 90%)",
                  boxShadow: "0 8px 20px rgba(76, 175, 80, 0.3)",
                }}
              >
                Join Meeting
              </Button>

              <Button
                fullWidth
                variant="text"
                onClick={() => setStep("enter")}
                sx={{ mt: 1 }}
              >
                Back to Meeting ID
              </Button>
            </Box>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default JoinMeeting;
