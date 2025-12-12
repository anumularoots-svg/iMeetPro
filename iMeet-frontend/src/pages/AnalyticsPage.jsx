// src/pages/AnalyticsPage.jsx - Professional Analytics Dashboard with AI Integration
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  Box,
  Container,
  Typography,
  Grid,
  Card,
  CardContent,
  Tab,
  Tabs,
  Paper,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  ListSubheader,  // ← ADDED
  LinearProgress,
  useTheme,
  alpha,
  IconButton,
  Alert,
  CircularProgress,
  TextField,
  Stack,
  Chip,
  Avatar,
  Divider,
  Tooltip,
  Badge,
  Switch,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  ListItemAvatar
} from '@mui/material';
import {
  TrendingUp as TrendingUpIcon,
  Group as GroupIcon,
  AccessTime as AccessTimeIcon,
  VideoCall as VideoIcon,
  Assessment as AssessmentIcon,
  Download as DownloadIcon,
  FilterList as FilterIcon,
  ArrowBack as ArrowBackIcon,
  Person as PersonIcon,
  SupervisorAccount as SupervisorAccountIcon,
  Refresh as RefreshIcon,
  Psychology,
  Visibility,
  Face,
  Timeline,
  Speed,
  Insights,
  Settings,
  Share,
  ExpandMore,
  CheckCircle,
  Warning,
  Error as ErrorIcon,
  Info,
  Lightbulb,
  AutoGraph,
  DataUsage,
  ModelTraining,
  RemoveRedEye,
  ChatBubble,
  VoiceChat,
  // MousePointer,
  VisibilityOff,
  PersonPin,
  ThumbUp,
  BarChart,
  PieChart as PieChartIcon,
  Print,
  Email
} from '@mui/icons-material';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  Legend,
  ResponsiveContainer,
  BarChart as RechartsBarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  Area,
  AreaChart,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar
} from 'recharts';
import { useAnalytics } from '../hooks/useAnalytics';
import { useAuth } from '../hooks/useAuth';
import { useNavigate } from 'react-router-dom';
// import { analyticsAPI } from ';
import { analyticsAPI } from "../services/api";
const AnalyticsPage = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const { user } = useAuth();
  const {
    analyticsData,
    loading,
    error,
    fetchHostOverview,
    fetchHostMeetingReports,
    fetchHostEngagementDistribution,
    fetchHostTrends,
    fetchParticipantPersonalReport,
    fetchParticipantAttendance,
    fetchParticipantEngagement,
    getUserStats,
    generateParticipantReportPDF,
    generateHostReportPDF,
    clearError
  } = useAnalytics();

  const [tabValue, setTabValue] = useState(0);
  const [timeFilter, setTimeFilter] = useState('30days');
  const [meetingFilter, setMeetingFilter] = useState('all');
  const [dateRange, setDateRange] = useState({
    start: '',
    end: ''
  });
  const [userRole, setUserRole] = useState('participant');
  const [personalStats, setPersonalStats] = useState(null);
  const [isLoadingStats, setIsLoadingStats] = useState(false);
  const [meetingTimeFilter, setMeetingTimeFilter] = useState('');
  const [availableMeetingTimes, setAvailableMeetingTimes] = useState([]);
  const [isLoadingMeetingTimes, setIsLoadingMeetingTimes] = useState(false);
  // AI Analytics states
  const [aiModules, setAiModules] = useState({
    faceDetection: true,
    emotionAnalysis: true,
    speechAnalysis: true,
    gestureRecognition: true,
    attentionTracking: true,
    sentimentAnalysis: true,
    behaviorPrediction: false,
    engagementPrediction: true
  });
  const [privacyMode, setPrivacyMode] = useState(false);
  const [isTracking, setIsTracking] = useState(true);

  // Color schemes for different chart types
  const chartColors = {
    primary: '#1E3A8A',      // Deep blue (main accent)
    secondary: '#3B82F6',    // Bright blue
    success: '#60A5FA',      // Soft blue for positive visuals
    warning: '#93C5FD',      // Light sky blue (subtle attention)
    error: '#000000',        // Black (for text or strong contrast)
    info: '#E0F2FE',         // Pale blue-white (background or info)
    gradient1: ['#0F172A', '#3B82F6'],   // Dark navy to vivid blue
    gradient2: ['#FFFFFF', '#93C5FD'],   // White to light blue
    gradient3: ['#1E3A8A', '#000000'],   // Blue to black
    gradient4: ['#3B82F6', '#E0F2FE']    // Bright blue to pale white
  };


  // Determine user role based on meeting history
  useEffect(() => {
    const determineUserRole = async () => {
      if (!user?.id) return;

      try {
        setIsLoadingStats(true);
        const userStats = await getUserStats();

        if (userStats && userStats.totalHostedMeetings > 0) {
          setUserRole('host');
        } else {
          setUserRole('participant');
        }
        setPersonalStats(userStats);
      } catch (error) {
        console.error('Error determining user role:', error);
        setUserRole('participant');
      } finally {
        setIsLoadingStats(false);
      }
    };

    if (user) {
      determineUserRole();
    }
  }, [user, getUserStats]);
// const fetchAvailableMeetingTimes = useCallback(async () => {
//   if (!user?.id || !dateRange.start) {
//     setAvailableMeetingTimes([]);
//     return;
//   }

//   try {
//     setIsLoadingMeetingTimes(true);
    
//     console.log(`📅 Fetching ${userRole} meetings for user ${user.id} on ${dateRange.start}`);

//     const params = {
//       user_id: user.id,
//       start_date: dateRange.start,
//       role_type: userRole  // 'participant' or 'host'
//     };

//     // ✅ Use the analyticsAPI from your imports
//     const response = await analyticsAPI.getAvailableMeetingTimes(params);
    
//     console.log('📅 API Response:', response);

//     // Handle response
//     const times = response?.data || response || [];
//     console.log(`✅ Found ${times.length} ${userRole} meetings`);

//     setAvailableMeetingTimes(times);
    
//   } catch (error) {
//     console.error('❌ Error fetching meeting times:', error);
//     setAvailableMeetingTimes([]);
//   } finally {
//     setIsLoadingMeetingTimes(false);
//   }
// }, [user?.id, dateRange.start, userRole]);

// // FIND THIS FUNCTION in AnalyticsPage.jsx and REPLACE with this:
// const fetchAvailableMeetingTimes = useCallback(async () => {
//   // FIXED: Check for EITHER start OR end date (not just start)
//   if (!user?.id || (!dateRange.start && !dateRange.end)) {
//     setAvailableMeetingTimes([]);
//     return;
//   }

//   try {
//     setIsLoadingMeetingTimes(true);
    
//     console.log(`📅 Fetching ${userRole} meetings for user ${user.id}`);
//     console.log(`📅 Date range: ${dateRange.start} to ${dateRange.end}`);

//     const params = {
//       user_id: user.id,
//       start_date: dateRange.start,
//       end_date: dateRange.end,  // ✅ CRITICAL FIX: Add end_date!
//       role_type: userRole
//     };

//     // Remove undefined values
//     Object.keys(params).forEach(key => {
//       if (params[key] === undefined || params[key] === '') {
//         delete params[key];
//       }
//     });

//     console.log('📅 API params:', params);

//     const response = await analyticsAPI.getAvailableMeetingTimes(params);
    
//     console.log('📅 API Response:', response);

//     // Handle different response structures
//     const times = response?.data?.data || response?.data || response || [];
//     console.log(`✅ Found ${times.length} ${userRole} meetings`);

//     setAvailableMeetingTimes(times);
    
//   } catch (error) {
//     console.error('❌ Error fetching meeting times:', error);
//     setAvailableMeetingTimes([]);
//   } finally {
//     setIsLoadingMeetingTimes(false);
//   }
// }, [user?.id, dateRange.start, dateRange.end, userRole]); // ✅ Add dateRange.end to dependencies

// In AnalyticsPage.jsx - Update fetchAvailableMeetingTimes

const fetchAvailableMeetingTimes = useCallback(async () => {
  // Check for EITHER start OR end date
  if (!user?.id || (!dateRange.start && !dateRange.end)) {
    setAvailableMeetingTimes([]);
    return;
  }

  try {
    setIsLoadingMeetingTimes(true);
    
    console.log(`📅 Fetching ${userRole} meetings for user ${user.id}`);
    console.log(`📅 Date range: ${dateRange.start} to ${dateRange.end}`);

    const params = {
      user_id: user.id,
      start_date: dateRange.start,
      end_date: dateRange.end,
      role_type: userRole
    };

    // Remove empty/undefined values
    Object.keys(params).forEach(key => {
      if (params[key] === undefined || params[key] === '' || params[key] === null) {
        delete params[key];
      }
    });

    console.log('📅 API params being sent:', params);

    const response = await analyticsAPI.getAvailableMeetingTimes(params);
    
    console.log('📅 API Response:', response);

    const times = response?.data?.data || response?.data || response || [];
    const dateSummary = response?.data?.date_summary || response?.date_summary || [];
    
    console.log(`✅ Found ${Array.isArray(times) ? times.length : 0} ${userRole} meetings`);
    console.log(`✅ Date summary:`, dateSummary);

    setAvailableMeetingTimes(Array.isArray(times) ? times : []);
    
    // Optionally store date summary for display
    if (dateSummary.length > 0) {
      console.log('📊 Meetings by date:', dateSummary);
    }
    
  } catch (error) {
    console.error('❌ Error fetching meeting times:', error);
    setAvailableMeetingTimes([]);
  } finally {
    setIsLoadingMeetingTimes(false);
  }
}, [user?.id, dateRange.start, dateRange.end, userRole]);
  useEffect(() => {
    fetchAvailableMeetingTimes();
  }, [fetchAvailableMeetingTimes]);
  // Fetch analytics data based on role and filters
  useEffect(() => {
    const fetchAnalyticsData = async () => {
      if (!user?.id) return;

      try {
        clearError();

        const filters = {
          period: timeFilter,
          meetingType: meetingFilter,
          user_id: user.id,
          userId: user.id,
          host_id: user.id,
          timeframe: timeFilter,
          start_date: dateRange.start,
          end_date: dateRange.end,
          dateRange: dateRange,
          meeting_time: meetingTimeFilter || undefined  // ADD THIS LINE
        };

        if (userRole === 'host') {
          await Promise.allSettled([
            fetchHostOverview(filters),
            fetchHostMeetingReports(filters),
            fetchHostEngagementDistribution(filters),
            fetchHostTrends(filters)
          ]);
        } else {
          await Promise.allSettled([
            fetchParticipantPersonalReport(filters),
            fetchParticipantAttendance(filters),
            fetchParticipantEngagement(filters)
          ]);
        }
      } catch (error) {
        console.error('Error fetching analytics data:', error);
      }
    };

    if (user && !isLoadingStats) {
      fetchAnalyticsData();
    }
  }, [timeFilter, meetingFilter, dateRange, userRole, user, isLoadingStats]);

  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  const handleBackToDashboard = () => {
    navigate('/dashboard');
  };

  const handleRoleSwitch = (role) => {
    setUserRole(role);
    setTabValue(0);
  };

  const handleExportReport = async () => {
    try {
      if (userRole === 'host') {
        await generateHostReportPDF({
          host_id: user?.id,
          start_date: dateRange.start,
          end_date: dateRange.end,
          meeting_time: meetingTimeFilter || undefined  // ADD THIS LINE
        });
      } else {
        await generateParticipantReportPDF({
          user_id: user?.id,
          start_date: dateRange.start,
          end_date: dateRange.end,
          meeting_time: meetingTimeFilter || undefined
        });
      }
    } catch (error) {
      console.error('Error exporting report:', error);
    }
  };

  const handleModuleToggle = (module) => {
    setAiModules(prev => ({
      ...prev,
      [module]: !prev[module]
    }));
  };

  // Safe data access with default values
  const safeAnalyticsData = useMemo(() => {
    const comprehensiveData = analyticsData.comprehensiveData || {};
    const overallSummary = comprehensiveData.overall_summary || {};

    return {
      hostOverview: analyticsData.hostOverview || overallSummary,
      hostMeetings: analyticsData.hostMeetings || comprehensiveData.meeting_analytics || [],
      hostEngagement: analyticsData.hostEngagement || { distribution: comprehensiveData.host_analytics || [] },
      hostTrends: analyticsData.hostTrends || { trends: [] },
      participantReport: analyticsData.participantReport || {},
      participantAttendance: analyticsData.participantAttendance || [],
      participantEngagement: analyticsData.participantEngagement || { engagementRecords: [], summary: {} }
    };
  }, [analyticsData]);

  // Process attendance data for charts
  const attendanceChartData = useMemo(() => {
    const attendance = safeAnalyticsData.participantAttendance || [];
    if (!Array.isArray(attendance)) return [];

    return attendance.slice(-7).map(item => ({
      date: item.meeting_name || item.meeting_info?.meeting_name || 'Meeting',
      attendance: Math.round(item.participant_attendance_metrics?.overall_attendance || 0),
      engagement: Math.round(item.attendance_session?.engagement_score || 0),
      focus: Math.round(item.attendance_session?.focus_score || 0)
    }));
  }, [safeAnalyticsData.participantAttendance]);

  // Enhanced engagement pie data
  const engagementPieData = useMemo(() => {
    const engagement = safeAnalyticsData.participantEngagement?.engagementRecords || [];
    if (!Array.isArray(engagement) || engagement.length === 0) {
      return [
        { name: 'No Data Available', value: 100, color: '#e0e0e0', duration: 0 }
      ];
    }

    const latestRecord = engagement[engagement.length - 1] || engagement[0];

    if (!latestRecord) {
      return [
        { name: 'No Data Available', value: 100, color: '#e0e0e0', duration: 0 }
      ];
    }

    const durationAnalysis = latestRecord.duration_analysis || {};
    const attendanceSession = latestRecord.attendance_session || {};

    const totalDuration = durationAnalysis.total_duration_minutes || 60;
    const engagementScore = attendanceSession.engagement_score || 0;
    const focusScore = attendanceSession.focus_score || 0;
    const breakTime = attendanceSession.total_break_time_used || 0;

    const engagedTime = (totalDuration * engagementScore) / 100;
    const focusedTime = (engagedTime * focusScore) / 100;
    const unfocusedTime = engagedTime - focusedTime;
    const breakTimeMinutes = breakTime / 60;
    const awayTime = totalDuration - engagedTime - breakTimeMinutes;

    const activities = [
      {
        name: 'Focused Time',
        value: Math.round((focusedTime / totalDuration) * 100),
        color: chartColors.success,
        duration: Math.round(focusedTime),
        description: 'Time actively engaged and focused'
      },
      {
        name: 'Unfocused Time',
        value: Math.round((unfocusedTime / totalDuration) * 100),
        color: chartColors.warning,
        duration: Math.round(unfocusedTime),
        description: 'Time present but not fully focused'
      },
      {
        name: 'Break Time',
        value: Math.round((breakTimeMinutes / totalDuration) * 100),
        color: chartColors.info,
        duration: Math.round(breakTimeMinutes),
        description: 'Planned break periods'
      },
      {
        name: 'Away Time',
        value: Math.round((awayTime / totalDuration) * 100),
        color: chartColors.error,
        duration: Math.round(awayTime),
        description: 'Time away from the session'
      }
    ].filter(item => item.value > 0);

    return activities.length > 0 ? activities : [
      { name: 'No Activity Data', value: 100, color: '#e0e0e0', duration: 0, description: 'No engagement data available' }
    ];
  }, [safeAnalyticsData.participantEngagement]);

  // Process host trends data
  const hostTrendsData = useMemo(() => {
    const trends = safeAnalyticsData.hostTrends?.trends || [];
    if (!Array.isArray(trends)) return [];

    return trends.map(item => ({
      date: item.meeting_type || 'Meeting',
      meetings: item.meeting_counts?.total_meetings_hosted || 0,
      participants: item.meeting_counts?.total_participants || 0
    }));
  }, [safeAnalyticsData.hostTrends]);

  // Process host meetings data
  const hostMeetingsData = useMemo(() => {
    const meetings = safeAnalyticsData.hostMeetings || [];
    if (!Array.isArray(meetings)) return [];

    return meetings.slice(-7).map(item => ({
      date: item.meeting_name || 'Meeting',
      participants: item.participant_analytics?.total_participants || 0,
      attendance: Math.round(item.participant_analytics?.avg_participant_attendance || 0),
      duration: Math.round(item.duration_analytics?.average_duration_minutes || 0)
    }));
  }, [safeAnalyticsData.hostMeetings]);

  // AI Insights mock data (enhance with real data when available)
  const aiInsights = useMemo(() => {
    const engagement = safeAnalyticsData.participantEngagement?.engagementRecords || [];
    const latestEngagement = engagement[engagement.length - 1] || {};
    const engagementScore = latestEngagement.attendance_session?.engagement_score || 0;

    return [
      {
        id: 1,
        type: 'engagement',
        title: engagementScore > 80 ? 'High Engagement Detected' : 'Engagement Opportunity',
        description: `Current engagement level is ${Math.round(engagementScore)}%. ${engagementScore > 80 ? 'Participants are highly engaged' : 'Consider using more interactive content'}`,
        confidence: 92,
        impact: engagementScore > 80 ? 'high' : 'medium',
        recommendation: engagementScore > 80 ? 'Maintain current engagement strategies' : 'Use more visual aids and interactive content',
        timestamp: new Date().toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit' })
      }
    ];
  }, [safeAnalyticsData.participantEngagement]);

  // Custom tooltip for engagement pie chart
  const EngagementTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <Box
          sx={{
            backgroundColor: 'rgba(255, 255, 255, 0.98)',
            border: `2px solid ${data.color}`,
            borderRadius: '12px',
            padding: '16px',
            boxShadow: '0 8px 24px rgba(0,0,0,0.15)',
            maxWidth: '280px'
          }}
        >
          <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1, color: data.color }}>
            {data.name}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
            {data.description}
          </Typography>
          <Box sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            backgroundColor: alpha(data.color, 0.1),
            padding: '8px 12px',
            borderRadius: '8px'
          }}>
            <Typography variant="h6" sx={{ fontWeight: 700, color: data.color }}>
              {data.value}%
            </Typography>
            <Typography variant="body2" sx={{ fontWeight: 500 }}>
              {Math.floor(data.duration / 60)}h {data.duration % 60}m
            </Typography>
          </Box>
        </Box>
      );
    }
    return null;
  };

  // Gradient Card Component
  const GradientCard = ({ gradient, icon: IconComponent, value, label, trend }) => (
    <Card sx={{
      background: `linear-gradient(135deg, ${gradient[0]} 0%, ${gradient[1]} 100%)`,
      color: 'white',
      height: '100%',
      position: 'relative',
      overflow: 'hidden',
      transition: 'transform 0.3s ease, box-shadow 0.3s ease',
      '&:hover': {
        transform: 'translateY(-4px)',
        boxShadow: '0 12px 24px rgba(0,0,0,0.2)'
      }
    }}>
      <CardContent sx={{ position: 'relative', zIndex: 1 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
          <IconComponent sx={{ fontSize: 48, opacity: 0.9 }} />
          {trend && (
            <Chip
              label={trend}
              size="small"
              sx={{
                backgroundColor: 'rgba(255,255,255,0.25)',
                color: 'white',
                fontWeight: 600
              }}
            />
          )}
        </Box>
        <Typography variant="h3" sx={{ fontWeight: 700, mb: 1 }}>
          {value}
        </Typography>
        <Typography variant="body1" sx={{ opacity: 0.95, fontWeight: 500 }}>
          {label}
        </Typography>
      </CardContent>
      <Box sx={{
        position: 'absolute',
        right: -20,
        bottom: -20,
        width: 120,
        height: 120,
        borderRadius: '50%',
        backgroundColor: 'rgba(255,255,255,0.1)',
        zIndex: 0
      }} />
    </Card>
  );

  // Advanced Chart Component
  const AdvancedChart = ({ title, data, chartType = 'bar', height = 350, showLegend = true }) => (
    <Card sx={{
      mb: 3,
      borderRadius: '16px',
      boxShadow: '0 4px 20px rgba(0,0,0,0.08)',
      transition: 'box-shadow 0.3s ease',
      '&:hover': {
        boxShadow: '0 8px 30px rgba(0,0,0,0.12)'
      }
    }}>
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
          <Typography variant="h6" sx={{ fontWeight: 700, color: chartColors.primary }}>
            {title}
          </Typography>
          <Stack direction="row" spacing={1}>
            <Tooltip title="Download Chart">
              <IconButton size="small">
                <DownloadIcon />
              </IconButton>
            </Tooltip>
            <Tooltip title="Share Chart">
              <IconButton size="small">
                <Share />
              </IconButton>
            </Tooltip>
          </Stack>
        </Box>
        <ResponsiveContainer width="100%" height={height}>
          {chartType === 'bar' ? (
            <RechartsBarChart
              data={data}
              margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
            >
              <defs>
                <linearGradient id="barGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={chartColors.primary} stopOpacity={1} />
                  <stop offset="100%" stopColor={chartColors.secondary} stopOpacity={0.8} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12, fill: '#666' }}
                axisLine={{ stroke: '#e0e0e0' }}
              />
              <YAxis
                tick={{ fontSize: 12, fill: '#666' }}
                axisLine={{ stroke: '#e0e0e0' }}
              />
              <RechartsTooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.98)',
                  border: '2px solid #e0e0e0',
                  borderRadius: '12px',
                  boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                  padding: '12px'
                }}
                cursor={{ fill: 'rgba(102, 126, 234, 0.1)' }}
              />
              {showLegend && <Legend />}
              <Bar
                dataKey="participants"
                fill="url(#barGradient)"
                radius={[8, 8, 0, 0]}
                name="Participants"
                maxBarSize={60}
              />
              {data[0]?.attendance !== undefined && (
                <Bar
                  dataKey="attendance"
                  fill={chartColors.success}
                  radius={[8, 8, 0, 0]}
                  name="Attendance %"
                  maxBarSize={60}
                />
              )}
            </RechartsBarChart>
          ) : chartType === 'area' ? (
            <AreaChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
              <defs>
                <linearGradient id="areaGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={chartColors.primary} stopOpacity={0.8} />
                  <stop offset="95%" stopColor={chartColors.primary} stopOpacity={0.1} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12, fill: '#666' }}
                axisLine={{ stroke: '#e0e0e0' }}
              />
              <YAxis
                tick={{ fontSize: 12, fill: '#666' }}
                axisLine={{ stroke: '#e0e0e0' }}
              />
              <RechartsTooltip />
              <Area
                type="monotone"
                dataKey="attendance"
                stroke={chartColors.primary}
                strokeWidth={3}
                fill="url(#areaGradient)"
                name="Attendance"
              />
              <Area
                type="monotone"
                dataKey="engagement"
                stroke={chartColors.success}
                strokeWidth={3}
                fill={chartColors.success}
                fillOpacity={0.2}
                name="Engagement"
              />
            </AreaChart>
          ) : (
            <PieChart>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, value }) => `${name}: ${value}%`}
                outerRadius={height / 3}
                fill="#8884d8"
                dataKey="value"
              >
                {data.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <RechartsTooltip content={<EngagementTooltip />} />
              {showLegend && (
                <Legend
                  verticalAlign="bottom"
                  height={36}
                  wrapperStyle={{ paddingTop: '20px' }}
                  formatter={(value, entry) => (
                    <span style={{ color: entry.color, fontWeight: 600 }}>{value}</span>
                  )}
                />
              )}
            </PieChart>
          )}
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );

  // Participant Metrics Component
  const ParticipantMetrics = () => {
    const stats = personalStats || {};
    const totalMinutes = stats.totalMinutes || 0;
    const hours = Math.floor(totalMinutes / 60);
    const minutes = Math.floor(totalMinutes % 60);

    return (
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12}>
          <Alert
            severity="info"
            icon={<PersonIcon />}
            sx={{
              mb: 3,
              borderRadius: '12px',
              backgroundColor: alpha(chartColors.info, 0.1),
              border: `2px solid ${alpha(chartColors.info, 0.3)}`,
              '& .MuiAlert-icon': {
                color: chartColors.info
              }
            }}
          >
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Personal Analytics Dashboard
            </Typography>
            <Typography variant="body2">
              Comprehensive view of your meeting attendance and participation metrics
            </Typography>
          </Alert>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient1}
            icon={VideoIcon}
            value={stats.totalMeetings || 0}
            label="Meetings Attended"
            trend={stats.totalMeetings > 10 ? "Active" : "Getting Started"}
          />
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient2}
            icon={AccessTimeIcon}
            value={`${hours}h ${minutes}m`}
            label="Total Meeting Time"
            trend={hours > 20 ? "High Engagement" : "Building"}
          />
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient3}
            icon={TrendingUpIcon}
            value={`${Math.round(stats.averageAttendance || 0)}%`}
            label="Average Attendance"
            trend={stats.averageAttendance > 90 ? "Excellent" : "Good"}
          />
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient4}
            icon={AssessmentIcon}
            value={stats.upcomingCount || 0}
            label="Upcoming Meetings"
            trend="Scheduled"
          />
        </Grid>
      </Grid>
    );
  };

  // Host Metrics Component
  const HostMetrics = () => {
    const overview = safeAnalyticsData.hostOverview || {};
    const avgDurationMins = Math.round(overview.average_duration_minutes || 0);

    return (
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12}>
          <Alert
            severity="success"
            icon={<SupervisorAccountIcon />}
            sx={{
              mb: 3,
              borderRadius: '12px',
              backgroundColor: alpha(chartColors.success, 0.1),
              border: `2px solid ${alpha(chartColors.success, 0.3)}`,
              '& .MuiAlert-icon': {
                color: chartColors.success
              }
            }}
          >
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Host Analytics Dashboard
            </Typography>
            <Typography variant="body2">
              Complete analytics for all meetings you've hosted with AI-powered insights
            </Typography>
          </Alert>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient1}
            icon={VideoIcon}
            value={overview.total_meetings || 0}
            label="Total Meetings Hosted"
            trend="Host"
          />
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient2}
            icon={GroupIcon}
            value={overview.total_participants || 0}
            label="Total Participants"
            trend="Growing"
          />
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient3}
            icon={AccessTimeIcon}
            value={avgDurationMins > 0 ? `${avgDurationMins}m` : '0m'}
            label="Avg. Meeting Duration"
            trend="Optimal"
          />
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <GradientCard
            gradient={chartColors.gradient4}
            icon={TrendingUpIcon}
            value={`${Math.round(overview.avg_participant_attendance || 0)}%`}
            label="Avg. Attendance Rate"
            trend={overview.avg_participant_attendance > 85 ? "Excellent" : "Good"}
          />
        </Grid>
      </Grid>
    );
  };

  // AI Analytics Section Component
  const AIAnalyticsSection = () => (
    <Grid container spacing={3}>
      {/* AI System Status */}
      <Grid item xs={12}>
        <Card sx={{
          background: `linear-gradient(135deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`,
          color: 'white',
          borderRadius: '16px'
        }}>
          <CardContent>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
              <Box display="flex" alignItems="center" gap={2}>
                <Psychology sx={{ fontSize: 40 }} />
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 700 }}>
                    AI-Powered Analytics Engine
                  </Typography>
                  <Typography variant="body2" sx={{ opacity: 0.9 }}>
                    Real-time insights powered by computer vision and machine learning
                  </Typography>
                </Box>
              </Box>
              <Stack direction="row" spacing={1}>
                <Tooltip title="Refresh AI Analysis">
                  <IconButton sx={{ color: 'white' }}>
                    <RefreshIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="AI Settings">
                  <IconButton sx={{ color: 'white' }}>
                    <Settings />
                  </IconButton>
                </Tooltip>
              </Stack>
            </Box>

            {/* AI Module Toggles */}
            <Grid container spacing={2} sx={{ mt: 2 }}>
              {Object.entries(aiModules).slice(0, 4).map(([module, enabled]) => (
                <Grid item xs={6} sm={3} key={module}>
                  <Paper sx={{
                    p: 2,
                    textAlign: 'center',
                    backgroundColor: 'rgba(255,255,255,0.15)',
                    backdropFilter: 'blur(10px)'
                  }}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={enabled}
                          onChange={() => handleModuleToggle(module)}
                          sx={{
                            '& .MuiSwitch-thumb': { bgcolor: 'white' },
                            '& .MuiSwitch-track': { bgcolor: 'rgba(255,255,255,0.3)' }
                          }}
                        />
                      }
                      label={
                        <Typography variant="body2" sx={{ color: 'white', fontWeight: 600 }}>
                          {module.replace(/([A-Z])/g, ' $1').trim()}
                        </Typography>
                      }
                    />
                  </Paper>
                </Grid>
              ))}
            </Grid>
          </CardContent>
        </Card>
      </Grid>

      {/* AI Insights */}
      <Grid item xs={12}>
        <Card sx={{ borderRadius: '16px' }}>
          <CardContent>
            <Box display="flex" alignItems="center" gap={2} mb={3}>
              <Lightbulb sx={{ fontSize: 32, color: chartColors.warning }} />
              <Typography variant="h6" sx={{ fontWeight: 700 }}>
                AI-Generated Insights
              </Typography>
            </Box>

            <Grid container spacing={2}>
              {aiInsights.map((insight) => (
                <Grid item xs={12} key={insight.id}>
                  <Paper sx={{
                    p: 3,
                    border: `2px solid ${insight.impact === 'high' ? chartColors.error :
                        insight.impact === 'medium' ? chartColors.warning :
                          chartColors.success
                      }`,
                    borderRadius: '12px',
                    transition: 'transform 0.2s ease, box-shadow 0.2s ease',
                    '&:hover': {
                      transform: 'translateY(-2px)',
                      boxShadow: '0 8px 24px rgba(0,0,0,0.12)'
                    }
                  }}>
                    <Box display="flex" alignItems="flex-start" gap={2}>
                      <Box sx={{
                        p: 1.5,
                        borderRadius: '12px',
                        backgroundColor: alpha(
                          insight.impact === 'high' ? chartColors.error :
                            insight.impact === 'medium' ? chartColors.warning :
                              chartColors.success,
                          0.1
                        )
                      }}>
                        <Psychology sx={{
                          fontSize: 32,
                          color: insight.impact === 'high' ? chartColors.error :
                            insight.impact === 'medium' ? chartColors.warning :
                              chartColors.success
                        }} />
                      </Box>
                      <Box flex={1}>
                        <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                          {insight.title}
                        </Typography>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                          {insight.description}
                        </Typography>
                        <Stack direction="row" spacing={1} flexWrap="wrap" sx={{ mb: 2 }}>
                          <Chip
                            label={`${insight.confidence}% confidence`}
                            size="small"
                            color="info"
                            variant="outlined"
                          />
                          <Chip
                            label={`${insight.impact} impact`}
                            size="small"
                            color={
                              insight.impact === 'high' ? 'error' :
                                insight.impact === 'medium' ? 'warning' :
                                  'success'
                            }
                          />
                          <Chip
                            label={insight.timestamp}
                            size="small"
                            variant="outlined"
                          />
                        </Stack>
                        <Paper sx={{
                          p: 2,
                          backgroundColor: alpha(chartColors.success, 0.05),
                          borderLeft: `4px solid ${chartColors.success}`
                        }}>
                          <Typography variant="body2" sx={{ fontWeight: 600, display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Lightbulb sx={{ fontSize: 20, color: chartColors.warning }} />
                            Recommendation: {insight.recommendation}
                          </Typography>
                        </Paper>
                      </Box>
                    </Box>
                  </Paper>
                </Grid>
              ))}
            </Grid>
          </CardContent>
        </Card>
      </Grid>
    </Grid>
  );

  // Attendance Tracking Section
  const AttendanceTrackingSection = () => (
    <Grid container spacing={3}>
      <Grid item xs={12}>
        <Card sx={{ borderRadius: '16px' }}>
          <CardContent>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
              <Box display="flex" alignItems="center" gap={2}>
                <PersonPin sx={{ fontSize: 32, color: chartColors.primary }} />
                <Typography variant="h6" sx={{ fontWeight: 700 }}>
                  Attendance Analytics
                </Typography>
              </Box>
              <Stack direction="row" spacing={1}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={isTracking}
                      onChange={(e) => setIsTracking(e.target.checked)}
                    />
                  }
                  label="Real-time Tracking"
                />
                <FormControlLabel
                  control={
                    <Switch
                      checked={privacyMode}
                      onChange={(e) => setPrivacyMode(e.target.checked)}
                    />
                  }
                  label="Privacy Mode"
                />
              </Stack>
            </Box>

            <AdvancedChart
              title="Attendance & Engagement Trends"
              data={attendanceChartData}
              chartType="area"
              height={300}
            />
          </CardContent>
        </Card>
      </Grid>

      {/* Privacy Notice */}
      {!privacyMode && (
        <Grid item xs={12}>
          <Alert
            severity="info"
            icon={<RemoveRedEye />}
            sx={{
              borderRadius: '12px',
              backgroundColor: alpha(chartColors.info, 0.1),
              border: `2px solid ${alpha(chartColors.info, 0.3)}`
            }}
          >
            <Typography variant="body2">
              <strong>AI Analytics Active:</strong> Computer vision algorithms are analyzing participant engagement while respecting privacy.
              All data is processed locally and encrypted. Enable Privacy Mode to disable advanced tracking.
            </Typography>
          </Alert>
        </Grid>
      )}
    </Grid>
  );

  // Overview Tab Content
  const OverviewTab = () => (
    <Grid container spacing={3}>
      <Grid item xs={12}>
        {userRole === 'participant' ? <ParticipantMetrics /> : <HostMetrics />}
      </Grid>

      <Grid item xs={12} md={8}>
        <AdvancedChart
          title={userRole === 'participant' ? "Attendance Analytics" : "Meeting Analytics"}
          data={userRole === 'participant' ? attendanceChartData : hostMeetingsData}
          chartType="bar"
        />
      </Grid>

      <Grid item xs={12} md={4}>
        <AdvancedChart
          title="Session Activity Distribution"
          data={engagementPieData}
          chartType="pie"
          height={350}
          showLegend={false}
        />
      </Grid>

      {/* Quick Stats Cards */}
      <Grid item xs={12}>
        <Typography variant="h6" sx={{ fontWeight: 700, mb: 2, color: chartColors.primary }}>
          Quick Statistics
        </Typography>
        <Grid container spacing={2}>
          {[
            { icon: VideoIcon, label: 'Total Sessions', value: personalStats?.totalMeetings || 0, color: chartColors.gradient1 },
            { icon: AccessTimeIcon, label: 'Total Hours', value: `${Math.floor((personalStats?.totalMinutes || 0) / 60)}h`, color: chartColors.gradient2 },
            { icon: TrendingUpIcon, label: 'Avg Engagement', value: `${Math.round(personalStats?.averageAttendance || 0)}%`, color: chartColors.gradient3 },
            { icon: GroupIcon, label: 'Participants', value: safeAnalyticsData.hostOverview?.total_participants || 0, color: chartColors.gradient4 }
          ].map((stat, index) => (
            <Grid item xs={6} sm={3} key={index}>
              <Paper sx={{
                p: 2.5,
                textAlign: 'center',
                borderRadius: '12px',
                background: `linear-gradient(135deg, ${stat.color[0]} 0%, ${stat.color[1]} 100%)`,
                color: 'white',
                transition: 'transform 0.2s ease',
                '&:hover': {
                  transform: 'scale(1.05)'
                }
              }}>
                <stat.icon sx={{ fontSize: 36, mb: 1 }} />
                <Typography variant="h5" sx={{ fontWeight: 700, mb: 0.5 }}>
                  {stat.value}
                </Typography>
                <Typography variant="body2" sx={{ opacity: 0.95 }}>
                  {stat.label}
                </Typography>
              </Paper>
            </Grid>
          ))}
        </Grid>
      </Grid>
    </Grid>
  );

  // Main Loading State
  if (isLoadingStats) {
    return (
      <Container maxWidth="xl"
        sx={{
          py: 4,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          minHeight: '100vh',
          backgroundColor: '#1976d2'
        }}
      >
        <Box sx={{ textAlign: 'center' }}>
          {/* Custom Loader */}
          <Box
            sx={{
              width: '64px',
              height: '64px',
              position: 'relative',
              borderRadius: '50%',
              boxShadow: '-10px 8px 0 18px inset #fff',
              animation: 'rotate 2s ease-in infinite alternate',
              margin: '0 auto',
              '&::before': {
                content: '""',
                position: 'absolute',
                left: '14px',
                bottom: '16px',
                background: '#fcffffff',
                width: '16px',
                height: '16px',
                borderRadius: '50%',
                animation: 'scale 1s ease-in infinite alternate',
              },
              '@keyframes rotate': {
                '100%': { transform: 'rotate(750deg)' }
              },
              '@keyframes scale': {
                '100%': { transform: 'scale(0.5) translateY(5px)' }
              }
            }}
          />

          <Typography
            variant="h6"
            sx={{
              mt: 3,
              color: '#fff',
              fontWeight: 600
            }}
          >
            Initializing Analytics Engine...
          </Typography>
          <Typography
            variant="body2"
            sx={{
              mt: 1,
              color: 'rgba(255, 255, 255, 0.8)'
            }}
          >
            Loading AI-powered insights and data visualization
          </Typography>
        </Box>
      </Container>
    );
  }

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      {/* Professional Header */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <IconButton
              onClick={handleBackToDashboard}
              sx={{
                mr: 2,
                color: chartColors.primary,
                backgroundColor: alpha(chartColors.primary, 0.1),
                '&:hover': {
                  backgroundColor: alpha(chartColors.primary, 0.2),
                }
              }}
            >
              <ArrowBackIcon />
            </IconButton>
            <Box>
              <Typography variant="h4" sx={{ fontWeight: 800, background: `linear-gradient(135deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`, WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                Analytics Intelligence Center
              </Typography>
              <Typography variant="body1" color="text.secondary" sx={{ mt: 0.5, fontWeight: 500 }}>
                AI-powered insights and comprehensive meeting performance analytics
              </Typography>
            </Box>
          </Box>

          {/* Role Selector */}
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Button
              variant={userRole === 'participant' ? 'contained' : 'outlined'}
              onClick={() => handleRoleSwitch('participant')}
              startIcon={<PersonIcon />}
              sx={{
                borderRadius: '12px',
                textTransform: 'none',
                fontWeight: 600,
                ...(userRole === 'participant' && {
                  background: `linear-gradient(135deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`,
                })
              }}
            >
              Participant View
            </Button>
            {personalStats && personalStats.totalHostedMeetings > 0 && (
              <Button
                variant={userRole === 'host' ? 'contained' : 'outlined'}
                onClick={() => handleRoleSwitch('host')}
                startIcon={<SupervisorAccountIcon />}
                sx={{
                  borderRadius: '12px',
                  textTransform: 'none',
                  fontWeight: 600,
                  ...(userRole === 'host' && {
                    background: `linear-gradient(135deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`,
                  })
                }}
              >
                Host View
              </Button>
            )}
          </Box>
        </Box>
      </Box>

      {/* Advanced Filters */}
      <Paper sx={{ p: 3, mb: 4, borderRadius: '16px', boxShadow: '0 4px 20px rgba(0,0,0,0.08)' }}>
        <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
          <FilterIcon /> Advanced Filters & Controls
        </Typography>
        <Grid container spacing={3} alignItems="center">
          <Grid item xs={12} sm={6} md={2}>
            <FormControl fullWidth size="small">
              <InputLabel>Time Period</InputLabel>
              <Select
                value={timeFilter}
                label="Time Period"
                onChange={(e) => setTimeFilter(e.target.value)}
              >
                <MenuItem value="7days">Last 7 Days</MenuItem>
                <MenuItem value="30days">Last 30 Days</MenuItem>
                <MenuItem value="90days">Last 3 Months</MenuItem>
                <MenuItem value="1year">Last Year</MenuItem>
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} sm={6} md={2}>
            <FormControl fullWidth size="small">
              <InputLabel>Meeting Type</InputLabel>
              <Select
                value={meetingFilter}
                label="Meeting Type"
                onChange={(e) => setMeetingFilter(e.target.value)}
              >
                <MenuItem value="all">All Meetings</MenuItem>
                <MenuItem value="InstantMeeting">Instant</MenuItem>
                <MenuItem value="ScheduleMeeting">Scheduled</MenuItem>
                <MenuItem value="CalendarMeeting">Calendar</MenuItem>
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} sm={6} md={2}>
            <TextField
              fullWidth
              size="small"
              type="date"
              label="Start Date"
              value={dateRange.start}
              onChange={(e) => {
                setDateRange(prev => ({ ...prev, start: e.target.value }));
                setMeetingTimeFilter('');
              }}
              InputLabelProps={{ shrink: true }}
            />
          </Grid>

          <Grid item xs={12} sm={6} md={2}>
            <TextField
              fullWidth
              size="small"
              type="date"
              label="End Date"
              value={dateRange.end}
              onChange={(e) => {
                setDateRange(prev => ({ ...prev, end: e.target.value }));
                setMeetingTimeFilter('');
              }}
              InputLabelProps={{ shrink: true }}
            />
          </Grid>

          {/* NEW: Meeting Time Selector
          <Grid item xs={12} sm={6} md={2}>
            <FormControl fullWidth size="small" disabled={isLoadingMeetingTimes || availableMeetingTimes.length === 0}>
              <InputLabel>Specific Meeting</InputLabel>
              <Select
                value={meetingTimeFilter}
                label="Specific Meeting"
                onChange={(e) => setMeetingTimeFilter(e.target.value)}
              >
                <MenuItem value="">All Meetings</MenuItem>
                {availableMeetingTimes.map((meeting, index) => (
                  <MenuItem key={index} value={meeting.time}>
                    {meeting.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid> */}

          {/* Specific Meeting Dropdown - ENHANCED WITH DATE GROUPING */}
<Grid item xs={12} sm={6} md={3}>
  <FormControl fullWidth size="small" disabled={isLoadingMeetingTimes}>
    <InputLabel>Specific Meeting</InputLabel>
    <Select
      value={meetingTimeFilter}
      onChange={(e) => setMeetingTimeFilter(e.target.value)}
      label="Specific Meeting"
      MenuProps={{
        PaperProps: {
          style: {
            maxHeight: 400,
            width: 420
          }
        }
      }}
    >
      <MenuItem value="">
        <em>All Meetings</em>
      </MenuItem>
      
      {/* Group meetings by date */}
      {availableMeetingTimes.length > 0 ? (
        (() => {
          // Group meetings by date
          const groupedByDate = {};
          availableMeetingTimes.forEach(meeting => {
            const dateKey = meeting.date || 'Unknown';
            if (!groupedByDate[dateKey]) {
              groupedByDate[dateKey] = [];
            }
            groupedByDate[dateKey].push(meeting);
          });

          // Sort dates in descending order
          const sortedDates = Object.keys(groupedByDate).sort((a, b) => 
            new Date(b) - new Date(a)
          );

          return sortedDates.map((dateKey) => {
            const meetings = groupedByDate[dateKey];
            
            // Format full date for header
            let fullDateDisplay = dateKey;
            try {
              const dateObj = new Date(dateKey + 'T00:00:00');
              fullDateDisplay = dateObj.toLocaleDateString('en-US', {
                weekday: 'short',
                month: 'short',
                day: 'numeric',
                year: 'numeric'
              });
            } catch (e) {
              fullDateDisplay = dateKey;
            }

            return [
              // Date Header (non-selectable)
              <ListSubheader 
                key={`header-${dateKey}`}
                sx={{
                  backgroundColor: '#f5f5f5',
                  color: '#1976d2',
                  fontWeight: 'bold',
                  fontSize: '0.85rem',
                  lineHeight: '36px',
                  borderBottom: '1px solid #e0e0e0',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  position: 'sticky',
                  top: 0,
                  zIndex: 1
                }}
              >
                <span>📅 {fullDateDisplay}</span>
                <Chip 
                  label={`${meetings.length} meeting${meetings.length > 1 ? 's' : ''}`}
                  size="small"
                  sx={{ 
                    height: 20, 
                    fontSize: '0.7rem',
                    backgroundColor: '#1976d2',
                    color: 'white'
                  }}
                />
              </ListSubheader>,
              
              // Meetings for this date
              ...meetings.map((meeting, idx) => (
                <MenuItem 
                  key={meeting.meeting_id || `${dateKey}-${idx}`}
                  value={meeting.datetime_for_filter}
                  sx={{
                    fontSize: '0.85rem',
                    py: 1.5,
                    pl: 3,
                    '&:hover': {
                      backgroundColor: '#e3f2fd'
                    }
                  }}
                >
                  <Box sx={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <AccessTimeIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        {meeting.display_time}
                      </Typography>
                      <Chip 
                        label={
                          meeting.meeting_type === 'InstantMeeting' ? 'Instant' : 
                          meeting.meeting_type === 'ScheduleMeeting' ? 'Scheduled' : 
                          meeting.meeting_type === 'CalendarMeeting' ? 'Calendar' : 
                          meeting.meeting_type
                        }
                        size="small"
                        sx={{ 
                          height: 18, 
                          fontSize: '0.65rem',
                          backgroundColor: 
                            meeting.meeting_type === 'InstantMeeting' ? '#e3f2fd' :
                            meeting.meeting_type === 'ScheduleMeeting' ? '#fff3e0' :
                            meeting.meeting_type === 'CalendarMeeting' ? '#e8f5e9' : '#f5f5f5'
                        }}
                      />
                    </Box>
                    <Typography 
                      variant="caption" 
                      sx={{ 
                        color: 'text.secondary',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        maxWidth: 320,
                        mt: 0.5
                      }}
                    >
                      {meeting.meeting_name}
                      {meeting.participant_count !== undefined && ` • ${meeting.participant_count} participants`}
                      {meeting.role === 'participant' && meeting.label?.includes(' - ') && 
                        ` • ${meeting.label.split(' - ').pop()}`
                      }
                    </Typography>
                  </Box>
                </MenuItem>
              ))
            ];
          }).flat();
        })()
      ) : (
        <MenuItem disabled>
          <em>{isLoadingMeetingTimes ? 'Loading meetings...' : 'No meetings found in date range'}</em>
        </MenuItem>
      )}
    </Select>
  </FormControl>
</Grid>

          <Grid item xs={12} sm={6} md={2}>
            <Button
              variant="contained"
              startIcon={<DownloadIcon />}
              fullWidth
              onClick={handleExportReport}
              disabled={loading}
              sx={{
                background: `linear-gradient(135deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`,
                borderRadius: '8px',
                textTransform: 'none',
                fontWeight: 600,
                height: '40px'
              }}
            >
              Export PDF Report
            </Button>
          </Grid>
        </Grid>

        {/* NEW: Meeting Time Filter Indicator */}
        {meetingTimeFilter && (
          <Alert
            severity="info"
            sx={{ mt: 2 }}
            onClose={() => setMeetingTimeFilter('')}
          >
            <Typography variant="body2">
              <strong>Filtered by specific meeting:</strong> {availableMeetingTimes.find(m => m.time === meetingTimeFilter)?.label || meetingTimeFilter}
            </Typography>
          </Alert>
        )}
      </Paper>

      {/* Enhanced Tabs */}
      <Box sx={{ mb: 3 }}>
        <Tabs
          value={tabValue}
          onChange={handleTabChange}
          sx={{
            '& .MuiTabs-indicator': {
              height: 4,
              borderRadius: '4px 4px 0 0',
              background: `linear-gradient(90deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`,
            },
            '& .MuiTab-root': {
              textTransform: 'none',
              fontWeight: 600,
              fontSize: '1rem',
              minHeight: 56,
              '&.Mui-selected': {
                color: chartColors.primary,
              }
            }
          }}
        >
          <Tab icon={<AssessmentIcon />} iconPosition="start" label="Overview & Insights" />
          <Tab icon={<PersonPin />} iconPosition="start" label="Attendance Tracking" />
          <Tab icon={<Psychology />} iconPosition="start" label="AI Analytics" />
          {userRole === 'host' && <Tab icon={<Timeline />} iconPosition="start" label="Trends & Reports" />}
        </Tabs>
      </Box>

      {/* Loading and Error States */}
      {loading && (
        <Box sx={{ mb: 2 }}>
          <LinearProgress sx={{
            height: 6,
            borderRadius: 3,
            backgroundColor: alpha(chartColors.primary, 0.1),
            '& .MuiLinearProgress-bar': {
              background: `linear-gradient(90deg, ${chartColors.primary} 0%, ${chartColors.secondary} 100%)`,
            }
          }} />
        </Box>
      )}

      {error && (
        <Alert
          severity="error"
          sx={{ mb: 2, borderRadius: '12px' }}
          onClose={clearError}
          action={
            <IconButton size="small" onClick={clearError}>
              <RefreshIcon />
            </IconButton>
          }
        >
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            {error}
          </Typography>
        </Alert>
      )}

      {/* Tab Content */}
      <Box>
        {tabValue === 0 && <OverviewTab />}
        {tabValue === 1 && <AttendanceTrackingSection />}
        {tabValue === 2 && <AIAnalyticsSection />}
        {tabValue === 3 && userRole === 'host' && (
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <AdvancedChart
                title="Meeting Trends Over Time"
                data={hostTrendsData}
                chartType="bar"
              />
            </Grid>
          </Grid>
        )}
      </Box>
    </Container>
  );
};

export default AnalyticsPage;