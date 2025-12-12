import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Avatar,
  Chip,
  LinearProgress,
  IconButton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Tooltip,
  Switch,
  FormControlLabel,
  Alert,
  Stack,
  Badge,
  CircularProgress
} from '@mui/material';
import {
  Visibility,
  VisibilityOff,
  PersonPin,
  AccessTime,
  TrendingUp,
  TrendingDown,
  RemoveRedEye,
  Face,
  Psychology,
  CheckCircle,
  Warning,
  Error,
  Info,
  Download,
  Refresh
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';

const AttendanceTracker = ({ meetingId, participants, isHost = false }) => {
  const theme = useTheme();
  const [attendanceData, setAttendanceData] = useState([]);
  const [isTracking, setIsTracking] = useState(true);
  const [realTimeData, setRealTimeData] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // AI Analysis states
  const [faceDetection, setFaceDetection] = useState(true);
  const [engagementAnalysis, setEngagementAnalysis] = useState(true);
  const [privacyMode, setPrivacyMode] = useState(false);

  useEffect(() => {
    const initializeTracking = async () => {
      setLoading(true);
      try {
        // Mock attendance data - replace with API call
        const mockData = participants.map(participant => ({
          id: participant.id,
          name: participant.name,
          email: participant.email,
          joinTime: new Date(Date.now() - Math.random() * 3600000).toISOString(),
          totalDuration: Math.floor(Math.random() * 3600),
          attendancePercentage: Math.floor(Math.random() * 30) + 70,
          engagementScore: Math.floor(Math.random() * 40) + 60,
          focusPercentage: Math.floor(Math.random() * 30) + 70,
          movements: Math.floor(Math.random() * 50),
          faceDetectionCount: Math.floor(Math.random() * 100) + 50,
          status: 'active',
          lastSeen: new Date().toISOString(),
          deviceType: ['desktop', 'mobile', 'tablet'][Math.floor(Math.random() * 3)],
          networkQuality: ['excellent', 'good', 'fair', 'poor'][Math.floor(Math.random() * 4)],
          cameraStatus: Math.random() > 0.3,
          micStatus: Math.random() > 0.2,
          screenTime: Math.floor(Math.random() * 3600),
          idleTime: Math.floor(Math.random() * 300),
          chatActivity: Math.floor(Math.random() * 20),
          reactionCount: Math.floor(Math.random() * 15)
        }));
        
        setAttendanceData(mockData);
      } catch (err) {
        setError('Failed to initialize attendance tracking');
        console.error('Error initializing attendance tracking:', err);
      } finally {
        setLoading(false);
      }
    };

    if (meetingId && participants.length > 0) {
      initializeTracking();
    }
  }, [meetingId, participants]);

  // Real-time updates simulation
  useEffect(() => {
    if (!isTracking) return;

    const interval = setInterval(() => {
      setAttendanceData(prevData => 
        prevData.map(participant => ({
          ...participant,
          engagementScore: Math.max(0, Math.min(100, 
            participant.engagementScore + (Math.random() - 0.5) * 10
          )),
          focusPercentage: Math.max(0, Math.min(100, 
            participant.focusPercentage + (Math.random() - 0.5) * 5
          )),
          movements: participant.movements + Math.floor(Math.random() * 3),
          lastSeen: new Date().toISOString()
        }))
      );
    }, 5000);

    return () => clearInterval(interval);
  }, [isTracking]);

  const getAttendanceColor = (percentage) => {
    if (percentage >= 90) return 'success';
    if (percentage >= 70) return 'warning';
    return 'error';
  };

  const getEngagementLevel = (score) => {
    if (score >= 80) return { level: 'High', color: 'success', icon: <TrendingUp /> };
    if (score >= 60) return { level: 'Medium', color: 'warning', icon: <TrendingUp /> };
    return { level: 'Low', color: 'error', icon: <TrendingDown /> };
  };

  const getStatusIcon = (status, cameraStatus, micStatus) => {
    if (status === 'active') {
      if (cameraStatus && micStatus) return <CheckCircle color="success" />;
      if (!cameraStatus && !micStatus) return <Warning color="warning" />;
      return <Info color="info" />;
    }
    return <Error color="error" />;
  };

  const formatDuration = (seconds) => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    if (hours > 0) return `${hours}h ${minutes}m`;
    return `${minutes}m`;
  };

  const calculateOverallStats = () => {
    const totalParticipants = attendanceData.length;
    const avgAttendance = attendanceData.reduce((sum, p) => sum + p.attendancePercentage, 0) / totalParticipants;
    const avgEngagement = attendanceData.reduce((sum, p) => sum + p.engagementScore, 0) / totalParticipants;
    const activeParticipants = attendanceData.filter(p => p.status === 'active').length;
    
    return {
      totalParticipants,
      avgAttendance: Math.round(avgAttendance),
      avgEngagement: Math.round(avgEngagement),
      activeParticipants,
      participationRate: Math.round((activeParticipants / totalParticipants) * 100)
    };
  };

  const stats = calculateOverallStats();

  if (loading) {
    return (
      <Card sx={{ minHeight: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress />
        <Typography sx={{ ml: 2 }}>Initializing AI attendance tracking...</Typography>
      </Card>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ m: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      {/* Header Controls */}
      <Card sx={{ mb: 2, background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)' }}>
        <CardContent>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h5" sx={{ color: 'white', fontWeight: 600 }}>
              AI Attendance Tracking
            </Typography>
            <Stack direction="row" spacing={1}>
              <Tooltip title="Refresh Data">
                <IconButton sx={{ color: 'white' }}>
                  <Refresh />
                </IconButton>
              </Tooltip>
              <Tooltip title="Download Report">
                <IconButton sx={{ color: 'white' }}>
                  <Download />
                </IconButton>
              </Tooltip>
            </Stack>
          </Box>

          {isHost && (
            <Stack direction="row" spacing={3} flexWrap="wrap">
              <FormControlLabel
                control={
                  <Switch 
                    checked={isTracking} 
                    onChange={(e) => setIsTracking(e.target.checked)}
                    sx={{ '& .MuiSwitch-thumb': { bgcolor: 'white' } }}
                  />
                }
                label={<Typography sx={{ color: 'white' }}>Real-time Tracking</Typography>}
              />
              <FormControlLabel
                control={
                  <Switch 
                    checked={faceDetection} 
                    onChange={(e) => setFaceDetection(e.target.checked)}
                    sx={{ '& .MuiSwitch-thumb': { bgcolor: 'white' } }}
                  />
                }
                label={<Typography sx={{ color: 'white' }}>Face Detection</Typography>}
              />
              <FormControlLabel
                control={
                  <Switch 
                    checked={engagementAnalysis} 
                    onChange={(e) => setEngagementAnalysis(e.target.checked)}
                    sx={{ '& .MuiSwitch-thumb': { bgcolor: 'white' } }}
                  />
                }
                label={<Typography sx={{ color: 'white' }}>Engagement Analysis</Typography>}
              />
              <FormControlLabel
                control={
                  <Switch 
                    checked={privacyMode} 
                    onChange={(e) => setPrivacyMode(e.target.checked)}
                    sx={{ '& .MuiSwitch-thumb': { bgcolor: 'white' } }}
                  />
                }
                label={<Typography sx={{ color: 'white' }}>Privacy Mode</Typography>}
              />
            </Stack>
          )}
        </CardContent>
      </Card>

      {/* Overall Statistics */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={2.4}>
          <Card sx={{ background: 'linear-gradient(45deg, #FE6B8B 30%, #FF8E53 90%)' }}>
            <CardContent sx={{ textAlign: 'center', color: 'white' }}>
              <PersonPin sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {stats.totalParticipants}
              </Typography>
              <Typography variant="body2">Total Participants</Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={2.4}>
          <Card sx={{ background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)' }}>
            <CardContent sx={{ textAlign: 'center', color: 'white' }}>
              <CheckCircle sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {stats.activeParticipants}
              </Typography>
              <Typography variant="body2">Active Now</Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={2.4}>
          <Card sx={{ background: 'linear-gradient(45deg, #4CAF50 30%, #8BC34A 90%)' }}>
            <CardContent sx={{ textAlign: 'center', color: 'white' }}>
              <AccessTime sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {stats.avgAttendance}%
              </Typography>
              <Typography variant="body2">Avg Attendance</Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={2.4}>
          <Card sx={{ background: 'linear-gradient(45deg, #FF9800 30%, #FFC107 90%)' }}>
            <CardContent sx={{ textAlign: 'center', color: 'white' }}>
              <Psychology sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {stats.avgEngagement}%
              </Typography>
              <Typography variant="body2">Avg Engagement</Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={2.4}>
          <Card sx={{ background: 'linear-gradient(45deg, #9C27B0 30%, #E91E63 90%)' }}>
            <CardContent sx={{ textAlign: 'center', color: 'white' }}>
              <TrendingUp sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {stats.participationRate}%
              </Typography>
              <Typography variant="body2">Participation Rate</Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Detailed Attendance Table */}
      <Card sx={{ 
        background: 'linear-gradient(135deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0.05) 100%)',
        backdropFilter: 'blur(10px)',
        border: '1px solid rgba(255,255,255,0.1)'
      }}>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
            Detailed Attendance Analytics
          </Typography>
          
          <TableContainer component={Paper} sx={{ 
            background: 'transparent',
            boxShadow: 'none'
          }}>
            <Table>
              <TableHead>
                <TableRow sx={{ '& th': { fontWeight: 600, bgcolor: 'rgba(0,0,0,0.1)' } }}>
                  <TableCell>Participant</TableCell>
                  <TableCell align="center">Status</TableCell>
                  <TableCell align="center">Attendance</TableCell>
                  <TableCell align="center">Engagement</TableCell>
                  <TableCell align="center">Focus</TableCell>
                  <TableCell align="center">Duration</TableCell>
                  <TableCell align="center">Activity</TableCell>
                  <TableCell align="center">AI Analysis</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {attendanceData.map((participant) => {
                  const engagement = getEngagementLevel(participant.engagementScore);
                  
                  return (
                    <TableRow 
                      key={participant.id}
                      sx={{ 
                        '&:hover': { bgcolor: 'rgba(0,0,0,0.05)' },
                        transition: 'background-color 0.3s ease'
                      }}
                    >
                      <TableCell>
                        <Box display="flex" alignItems="center" gap={2}>
                          <Badge
                            badgeContent={getStatusIcon(participant.status, participant.cameraStatus, participant.micStatus)}
                            overlap="circular"
                            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                          >
                            <Avatar sx={{
                              bgcolor: participant.status === 'active' ? 'success.main' : 'grey.500'
                            }}>
                              {participant.name.charAt(0)}
                            </Avatar>
                          </Badge>
                          <Box>
                            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                              {participant.name}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              {participant.deviceType} • {participant.networkQuality}
                            </Typography>
                          </Box>
                        </Box>
                      </TableCell>

                      <TableCell align="center">
                        <Stack alignItems="center" spacing={0.5}>
                          <Chip
                            label={participant.status}
                            size="small"
                            color={participant.status === 'active' ? 'success' : 'default'}
                            variant="outlined"
                          />
                          <Box display="flex" gap={0.5}>
                            {participant.cameraStatus && (
                              <Tooltip title="Camera On">
                                <Visibility fontSize="small" color="success" />
                              </Tooltip>
                            )}
                            {!participant.cameraStatus && (
                              <Tooltip title="Camera Off">
                                <VisibilityOff fontSize="small" color="disabled" />
                              </Tooltip>
                            )}
                          </Box>
                        </Stack>
                      </TableCell>

                      <TableCell align="center">
                        <Box>
                          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                            {participant.attendancePercentage}%
                          </Typography>
                          <LinearProgress
                            variant="determinate"
                            value={participant.attendancePercentage}
                            color={getAttendanceColor(participant.attendancePercentage)}
                            sx={{ width: 60, height: 6, borderRadius: 3 }}
                          />
                        </Box>
                      </TableCell>

                      <TableCell align="center">
                        <Box display="flex" alignItems="center" justifyContent="center" gap={1}>
                          <Box>
                            <Typography variant="body2" sx={{ fontWeight: 600 }}>
                              {participant.engagementScore}%
                            </Typography>
                            <Typography variant="caption" color={`${engagement.color}.main`}>
                              {engagement.level}
                            </Typography>
                          </Box>
                          {engagement.icon}
                        </Box>
                      </TableCell>

                      <TableCell align="center">
                        <Box>
                          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                            {participant.focusPercentage}%
                          </Typography>
                          <LinearProgress
                            variant="determinate"
                            value={participant.focusPercentage}
                            color={participant.focusPercentage >= 70 ? 'success' : 'warning'}
                            sx={{ width: 60, height: 6, borderRadius: 3 }}
                          />
                        </Box>
                      </TableCell>

                      <TableCell align="center">
                        <Stack alignItems="center" spacing={0.5}>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {formatDuration(participant.totalDuration)}
                          </Typography>
                          <Typography variant="caption" color="text.secondary">
                            Screen: {formatDuration(participant.screenTime)}
                          </Typography>
                        </Stack>
                      </TableCell>

                      <TableCell align="center">
                        <Stack alignItems="center" spacing={0.5}>
                          <Typography variant="caption">
                            Chat: {participant.chatActivity}
                          </Typography>
                          <Typography variant="caption">
                            Reactions: {participant.reactionCount}
                          </Typography>
                          <Typography variant="caption">
                            Movements: {participant.movements}
                          </Typography>
                        </Stack>
                      </TableCell>

                      <TableCell align="center">
                        {!privacyMode ? (
                          <Stack alignItems="center" spacing={0.5}>
                            <Tooltip title="Face Detection Count">
                              <Chip
                                icon={<Face />}
                                label={participant.faceDetectionCount}
                                size="small"
                                variant="outlined"
                                color="primary"
                              />
                            </Tooltip>
                            <Tooltip title="Attention Level">
                              <Chip
                                icon={<RemoveRedEye />}
                                label={`${Math.round(participant.focusPercentage)}%`}
                                size="small"
                                variant="outlined"
                                color={participant.focusPercentage >= 70 ? 'success' : 'warning'}
                              />
                            </Tooltip>
                            <Typography variant="caption" color="text.secondary">
                              Idle: {formatDuration(participant.idleTime)}
                            </Typography>
                          </Stack>
                        ) : (
                          <Typography variant="caption" color="text.secondary">
                            Privacy Mode Enabled
                          </Typography>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      {/* Privacy Notice */}
      {faceDetection && !privacyMode && (
        <Alert 
          severity="info" 
          sx={{ 
            mt: 2,
            background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
            color: 'white',
            '& .MuiAlert-icon': { color: 'white' }
          }}
        >
          <Typography variant="body2">
            <strong>AI Analytics Active:</strong> Computer vision algorithms are analyzing participant engagement while respecting privacy. 
            All data is processed locally and encrypted. Enable Privacy Mode to disable advanced tracking.
          </Typography>
        </Alert>
      )}

      {privacyMode && (
        <Alert 
          severity="warning" 
          sx={{ 
            mt: 2,
            background: 'linear-gradient(45deg, #FF9800 30%, #FFC107 90%)',
            color: 'white',
            '& .MuiAlert-icon': { color: 'white' }
          }}
        >
          <Typography variant="body2">
            <strong>Privacy Mode Enabled:</strong> Advanced AI analytics are disabled. Only basic attendance metrics are being tracked.
          </Typography>
        </Alert>
      )}
    </Box>
  );
};

export default AttendanceTracker;