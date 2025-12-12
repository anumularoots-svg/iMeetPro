// src/components/dashboard/Dashboard.jsx
import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  Button,
  Avatar,
  Chip,
  LinearProgress,
  useTheme,
  Container,
  Fade,
  Slide,
  IconButton,
  Menu,
  MenuItem,
  Divider,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  ListItemButton,
  Badge,
  Skeleton,
  Paper,
  Stack,
  Tooltip
} from '@mui/material';
import {
  VideoCall,
  Schedule,
  CalendarToday,
  History,
  Person,
  MoreVert,
  Add,
  CallMerge,
  PlayArrow,
  AccessTime,
  Group,
  TrendingUp,
  RecordVoiceOver,
  CloudDownload,
  Star,
  Analytics,
  ArrowForward
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../hooks/useAuth';
import { useMeeting } from '../../hooks/useMeeting';
import { useAnalytics } from '../../hooks/useAnalytics';
import { useNotifications } from '../../hooks/useNotifications';
import MeetingOptions from './MeetingOptions';
import MeetingHistory from './MeetingHistory';

const TypingAnimation = ({ text, speed = 80, delay = 1500 }) => {
  const [displayedText, setDisplayedText] = useState('');
  const [currentIndex, setCurrentIndex] = useState(0);
  const [showCursor, setShowCursor] = useState(true);
  const [isTyping, setIsTyping] = useState(false);
  const [animationKey, setAnimationKey] = useState(0);

  // Reset animation whenever text changes or component mounts
  useEffect(() => {
    setDisplayedText('');
    setCurrentIndex(0);
    setIsTyping(false);
    setShowCursor(true);
    setAnimationKey(prev => prev + 1);
  }, [text]);

  useEffect(() => {
    if (!text) return;

    // Start typing after delay
    if (currentIndex === 0 && animationKey > 0) {
      const startTimer = setTimeout(() => {
        setIsTyping(true);
        setCurrentIndex(1);
        setDisplayedText(text.charAt(0));
      }, delay);
      return () => clearTimeout(startTimer);
    }

    // Continue typing
    if (currentIndex > 0 && currentIndex < text.length) {
      const timer = setTimeout(() => {
        setDisplayedText(text.substring(0, currentIndex + 1));
        setCurrentIndex(currentIndex + 1);
      }, speed);
      return () => clearTimeout(timer);
    }

    // Finished typing - restart after a pause
    if (currentIndex >= text.length && text.length > 0) {
      setIsTyping(false);
      const restartTimer = setTimeout(() => {
        setDisplayedText('');
        setCurrentIndex(0);
        setShowCursor(true);
        setAnimationKey(prev => prev + 1);
      }, 3000); // Wait 3 seconds before restarting
      return () => clearTimeout(restartTimer);
    }
  }, [text, currentIndex, speed, delay, animationKey]);

  // Cursor blinking effect
  useEffect(() => {
    const cursorTimer = setInterval(() => {
      setShowCursor(prev => !prev);
    }, 530);
    return () => clearInterval(cursorTimer);
  }, []);

  return (
    <Box 
      component="span" 
      sx={{ 
        position: 'relative',
        '@keyframes blink': {
          '0%, 50%': { opacity: 1 },
          '51%, 100%': { opacity: 0 }
        }
      }}
    >
      {displayedText}
      <Box
        component="span"
        sx={{
          display: 'inline-block',
          width: '2px',
          height: '1.2em',
          backgroundColor: 'currentColor',
          marginLeft: '2px',
          verticalAlign: 'text-top',
          opacity: showCursor ? 1 : 0,
          animation: 'blink 1.06s infinite',
        }}
      />
    </Box>
  );
};

const Dashboard = () => {
  const theme = useTheme();
  const navigate = useNavigate();
  const { user } = useAuth();
  const { 
    recentMeetings, 
    upcomingMeetings, 
    joinMeeting, 
    loading: meetingLoading 
  } = useMeeting();
  const { 
    getUserStats, 
    loading: analyticsLoading 
  } = useAnalytics();
  const { fetchNotifications } = useNotifications();

  const [anchorEl, setAnchorEl] = useState(null);
  const [userStats, setUserStats] = useState({
    totalMeetings: 0,
    totalMinutes: 0,
    averageAttendance: 0,
    upcomingCount: 0
  });
  const [quickJoinCode, setQuickJoinCode] = useState('');
  const [isWaving, setIsWaving] = useState(false);

  // Fetch notifications when Dashboard mounts
  useEffect(() => {
    console.log('🏠 Dashboard: Fetching ALL notifications');
    fetchNotifications();
  }, [fetchNotifications]);

  useEffect(() => {
    loadUserStats();
  }, []);

  const loadUserStats = async () => {
    try {
      const stats = await getUserStats();
      setUserStats(stats);
    } catch (error) {
      console.error('Failed to load user stats:', error);
    }
  };

  const handleMenuOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
  };

  const handleQuickJoin = async () => {
    if (quickJoinCode.trim()) {
      try {
        await joinMeeting(quickJoinCode.trim());
        navigate(`/meeting/${quickJoinCode.trim()}`);
      } catch (error) {
        console.error('Failed to join meeting:', error);
      }
    }
  };

  const formatDuration = (minutes) => {
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return hours > 0 ? `${hours}h ${mins}m` : `${mins}m`;
  };

  const truncateText = (text, maxLength = 10) => {
    if (!text) return text;
    const str = text.toString();
    if (str.length <= maxLength) return str;
    return str.substring(0, maxLength) + '...';
  };

  const getGreeting = () => {
    const hour = new Date().getHours();
    if (hour < 12) return 'Good morning';
    if (hour < 17) return 'Good afternoon';
    return 'Good evening';
  };

  const getUserName = () => {
    return user?.full_name?.split(' ')[0] || 'User';
  };

const statsData = [
  {
    title: 'Total Meetings',
    value: analyticsLoading ? null : userStats.totalMeetings,
    fullValue: analyticsLoading ? null : userStats.totalMeetings,
    icon: VideoCall,
    color: theme.palette.primary.main,
    bgColor: `${theme.palette.primary.main}08`
  },
  {
    title: 'Total Time',
    // CRITICAL: This should use the local formatDuration function
    value: analyticsLoading ? null : formatDuration(userStats.totalMinutes),
    fullValue: analyticsLoading ? null : formatDuration(userStats.totalMinutes),
    icon: AccessTime,
    color: theme.palette.secondary.main,
    bgColor: `${theme.palette.secondary.main}08`
  },
  {
    title: 'Avg Attendance',
    // CRITICAL: Format percentage to 2 decimal places
    value: analyticsLoading ? null : `${userStats.averageAttendance.toFixed(2)}%`,
    fullValue: analyticsLoading ? null : `${userStats.averageAttendance.toFixed(2)}%`,
    icon: TrendingUp,
    color: theme.palette.success.main,
    bgColor: `${theme.palette.success.main}08`
  },
  {
    title: 'Upcoming',
    value: analyticsLoading ? null : userStats.upcomingCount,
    fullValue: analyticsLoading ? null : userStats.upcomingCount,
    icon: Schedule,
    color: theme.palette.warning.main,
    bgColor: `${theme.palette.warning.main}08`
  }
];

  return (
    <Box sx={{ 
      minHeight: '100vh',
      backgroundColor: '#fafafa',
      pt: 3,
      pb: 6
    }}>
      <Container maxWidth="xl">
        {/* Header Section */}
        <Fade in timeout={800}>
          <Box mb={5}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <Typography 
                variant="h4" 
                sx={{ 
                  fontWeight: 600,
                  color: theme.palette.text.primary,
                  fontSize: { xs: '1.75rem', md: '2.125rem' }
                }}
              >
                {getGreeting()}, <TypingAnimation text={getUserName()} speed={120} delay={800} />
              </Typography>
              <Box
                sx={{
                  fontSize: { xs: '1.75rem', md: '2.125rem' },
                  animation: 'wave 1s ease-in-out infinite',
                  animationDelay: '2s',
                  transformOrigin: '70% 70%',
                  display: 'inline-block',
                  '@keyframes wave': {
                    '0%': { transform: 'rotate(0deg)' },
                    '10%': { transform: 'rotate(14deg)' },
                    '20%': { transform: 'rotate(-8deg)' },
                    '30%': { transform: 'rotate(14deg)' },
                    '40%': { transform: 'rotate(-4deg)' },
                    '50%': { transform: 'rotate(10deg)' },
                    '60%': { transform: 'rotate(0deg)' },
                    '100%': { transform: 'rotate(0deg)' }
                  }
                }}
              >
                👋
              </Box>
            </Box>
            <Typography 
              variant="body1" 
              sx={{ 
                color: theme.palette.text.secondary,
                fontSize: '1rem',
                maxWidth: '600px'
              }}
            >
              Manage your meetings efficiently and stay connected with your team.
            </Typography>
          </Box>
        </Fade>

        <Grid container spacing={3}>
          {/* Stats Section */}
          <Grid item xs={12}>
            <Slide direction="up" in timeout={1000}>
              <Grid container spacing={2.5}>
                {statsData.map((stat, index) => (
                  <Grid item xs={6} md={3} key={index}>
                    <Tooltip 
                      title={stat.fullValue ? `${stat.title}: ${stat.fullValue}` : ''} 
                      placement="top"
                      arrow
                    >
                      <Paper
                        elevation={0}
                        sx={{
                          p: 3,
                          borderRadius: 2,
                          border: '1px solid',
                          borderColor: theme.palette.divider,
                          backgroundColor: 'white',
                          transition: 'all 0.2s ease-in-out',
                          cursor: 'pointer',
                          height: '120px',
                          display: 'flex',
                          alignItems: 'center',
                          '&:hover': {
                            borderColor: stat.color,
                            transform: 'translateY(-2px)',
                            boxShadow: `0 8px 25px ${stat.color}15`
                          }
                        }}
                      >
                        <Stack 
                          direction="row" 
                          spacing={2} 
                          alignItems="center"
                          sx={{ width: '100%', minWidth: 0 }}
                        >
                          <Box
                            sx={{
                              width: 48,
                              height: 48,
                              borderRadius: 1.5,
                              backgroundColor: stat.bgColor,
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                              flexShrink: 0
                            }}
                          >
                            <stat.icon sx={{ color: stat.color, fontSize: 24 }} />
                          </Box>
                          <Box 
                            sx={{ 
                              minWidth: 0, 
                              flex: 1,
                              overflow: 'hidden'
                            }}
                          >
                            <Typography 
                              variant="h5" 
                              sx={{ 
                                fontWeight: 700,
                                color: theme.palette.text.primary,
                                mb: 0.5,
                                fontSize: { xs: '1.1rem', md: '1.3rem' },
                                lineHeight: 1.2,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                maxWidth: '100%'
                              }}
                            >
                              {stat.value === null ? (
                                <Skeleton width="80%" height={28} />
                              ) : (
                                <Box 
                                  component="span"
                                  sx={{
                                    display: 'block',
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap'
                                  }}
                                >
                                  {truncateText(stat.value, 12)}
                                </Box>
                              )}
                            </Typography>
                            <Typography 
                              variant="body2" 
                              sx={{ 
                                color: theme.palette.text.secondary,
                                fontSize: '0.8rem',
                                fontWeight: 500,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                maxWidth: '100%',
                                lineHeight: 1.2
                              }}
                            >
                              {stat.title}
                            </Typography>
                          </Box>
                        </Stack>
                      </Paper>
                    </Tooltip>
                  </Grid>
                ))}
              </Grid>
            </Slide>
          </Grid>

          {/* Main Meeting Actions */}
          <Grid item xs={12}>
            <Slide direction="up" in timeout={1200}>
              <Paper
                elevation={0}
                sx={{
                  borderRadius: 2,
                  border: '1px solid',
                  borderColor: theme.palette.divider,
                  backgroundColor: 'white',
                  overflow: 'hidden'
                }}
              >
                <Box sx={{ p: 4 }}>
                  <Typography 
                    variant="h6" 
                    sx={{ 
                      fontWeight: 600,
                      color: theme.palette.text.primary,
                      mb: 3
                    }}
                  >
                    Meeting Actions
                  </Typography>
                  
                  <MeetingOptions />
                </Box>
              </Paper>
            </Slide>
          </Grid>

          {/* Recent Meetings */}
          <Grid item xs={12}>
            <MeetingHistory 
              meetings={recentMeetings} 
              loading={meetingLoading}
              limit={5}
            />
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
};

export default Dashboard;