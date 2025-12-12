import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  LinearProgress,
  CircularProgress,
  Chip,
  Avatar,
  Stack,
  Divider,
  IconButton,
  Tooltip,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Badge,
  Alert
} from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  Psychology,
  Visibility,
  Chat,
  ThumbUp,
  AccessTime,
  Speed,
  Timeline,
  BarChart,
  PieChart,
  Refresh,
  Download,
  Share,
  Face,
  VoiceChat,
  MousePointer,
  RemoveRedEye
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';

const EngagementMetrics = ({ meetingId, participants, realTimeData = {} }) => {
  const theme = useTheme();
  const [metrics, setMetrics] = useState({});
  const [loading, setLoading] = useState(true);
  const [timeSeriesData, setTimeSeriesData] = useState([]);
  const [selectedTimeframe, setSelectedTimeframe] = useState('5min');

  useEffect(() => {
    const generateMetrics = () => {
      // Mock engagement metrics - replace with actual AI analysis
      const engagementData = {
        overall: {
          averageEngagement: 78,
          peakEngagement: 94,
          lowestEngagement: 45,
          trend: 'increasing',
          trendPercentage: 12
        },
        individual: participants.map(participant => ({
          id: participant.id,
          name: participant.name,
          avatar: participant.avatar,
          engagement: {
            overall: Math.floor(Math.random() * 40) + 60,
            attention: Math.floor(Math.random() * 30) + 70,
            participation: Math.floor(Math.random() * 50) + 50,
            interaction: Math.floor(Math.random() * 60) + 40
          },
          metrics: {
            speakingTime: Math.floor(Math.random() * 300) + 60,
            chatMessages: Math.floor(Math.random() * 20),
            reactions: Math.floor(Math.random() * 15),
            questionsAsked: Math.floor(Math.random() * 5),
            facialExpression: ['positive', 'neutral', 'engaged'][Math.floor(Math.random() * 3)],
            eyeMovement: Math.floor(Math.random() * 100) + 200,
            handGestures: Math.floor(Math.random() * 30) + 10,
            posturalChanges: Math.floor(Math.random() * 15) + 5,
            deviceInteraction: Math.floor(Math.random() * 50) + 30,
            attentionSpan: Math.floor(Math.random() * 20) + 80
          },
          timeline: Array.from({ length: 12 }, (_, i) => ({
            time: new Date(Date.now() - (11 - i) * 5 * 60 * 1000).toLocaleTimeString('en-US', { hour12: false }),
            value: Math.floor(Math.random() * 40) + 60
          }))
        })),
        behavioral: {
          attentionPatterns: {
            focused: 65,
            distracted: 20,
            multitasking: 15
          },
          participationTypes: {
            active: 40,
            passive: 35,
            observer: 25
          },
          engagementTriggers: [
            { trigger: 'Screen Sharing', impact: '+15%' },
            { trigger: 'Q&A Session', impact: '+22%' },
            { trigger: 'Polls', impact: '+18%' },
            { trigger: 'Breakout Rooms', impact: '+25%' }
          ]
        },
        realTime: {
          currentSpeaker: participants[Math.floor(Math.random() * participants.length)]?.name || 'Host',
          activeListeners: Math.floor(participants.length * 0.8),
          currentEngagement: Math.floor(Math.random() * 20) + 70,
          peakMoment: '14:32 - Screen sharing started',
          lowMoment: '14:15 - Long presentation section'
        }
      };

      setMetrics(engagementData);
      setLoading(false);
    };

    if (participants.length > 0) {
      generateMetrics();
    }
  }, [participants]);

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      setMetrics(prev => ({
        ...prev,
        realTime: {
          ...prev.realTime,
          currentEngagement: Math.max(30, Math.min(100, 
            prev.realTime?.currentEngagement + (Math.random() - 0.5) * 10
          ))
        }
      }));
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  const getEngagementColor = (score) => {
    if (score >= 80) return 'success';
    if (score >= 60) return 'warning';
    return 'error';
  };

  const getTrendIcon = (trend) => {
    return trend === 'increasing' ? <TrendingUp color="success" /> : <TrendingDown color="error" />;
  };

  const getExpressionEmoji = (expression) => {
    const expressions = {
      positive: '😊',
      neutral: '😐',
      engaged: '🤔',
      excited: '😄',
      focused: '🧐'
    };
    return expressions[expression] || '😐';
  };

  if (loading) {
    return (
      <Card sx={{ minHeight: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress />
        <Typography sx={{ ml: 2 }}>Analyzing engagement patterns...</Typography>
      </Card>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      {/* Header */}
      <Card sx={{ mb: 3, background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)' }}>
        <CardContent>
          <Box display="flex" justifyContent="space-between" alignItems="center">
            <Typography variant="h5" sx={{ color: 'white', fontWeight: 600 }}>
              Real-time Engagement Analytics
            </Typography>
            <Stack direction="row" spacing={1}>
              <IconButton sx={{ color: 'white' }}>
                <Refresh />
              </IconButton>
              <IconButton sx={{ color: 'white' }}>
                <Download />
              </IconButton>
              <IconButton sx={{ color: 'white' }}>
                <Share />
              </IconButton>
            </Stack>
          </Box>
        </CardContent>
      </Card>

      {/* Overall Metrics */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card sx={{ 
            background: 'linear-gradient(45deg, #FF6B6B 30%, #FFE66D 90%)',
            color: 'white',
            height: '100%'
          }}>
            <CardContent sx={{ textAlign: 'center' }}>
              <Psychology sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h3" sx={{ fontWeight: 600, mb: 1 }}>
                {metrics.overall?.averageEngagement}%
              </Typography>
              <Typography variant="body1" sx={{ mb: 1 }}>
                Average Engagement
              </Typography>
              <Box display="flex" alignItems="center" justifyContent="center">
                {getTrendIcon(metrics.overall?.trend)}
                <Typography variant="body2" sx={{ ml: 0.5 }}>
                  {metrics.overall?.trendPercentage}% vs last meeting
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={3}>
          <Card sx={{ 
            background: 'linear-gradient(45deg, #4ECDC4 30%, #44A08D 90%)',
            color: 'white',
            height: '100%'
          }}>
            <CardContent sx={{ textAlign: 'center' }}>
              <TrendingUp sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h3" sx={{ fontWeight: 600, mb: 1 }}>
                {metrics.overall?.peakEngagement}%
              </Typography>
              <Typography variant="body1" sx={{ mb: 1 }}>
                Peak Engagement
              </Typography>
              <Typography variant="body2">
                {metrics.realTime?.peakMoment}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={3}>
          <Card sx={{ 
            background: 'linear-gradient(45deg, #A8E6CF 30%, #88D8A3 90%)',
            color: 'white',
            height: '100%'
          }}>
            <CardContent sx={{ textAlign: 'center' }}>
              <RemoveRedEye sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h3" sx={{ fontWeight: 600, mb: 1 }}>
                {metrics.realTime?.activeListeners}
              </Typography>
              <Typography variant="body1" sx={{ mb: 1 }}>
                Active Listeners
              </Typography>
              <Typography variant="body2">
                {Math.round((metrics.realTime?.activeListeners / participants.length) * 100)}% of participants
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={3}>
          <Card sx={{ 
            background: 'linear-gradient(45deg, #FFD93D 30%, #FF6B6B 90%)',
            color: 'white',
            height: '100%'
          }}>
            <CardContent sx={{ textAlign: 'center' }}>
              <Speed sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h3" sx={{ fontWeight: 600, mb: 1 }}>
                {metrics.realTime?.currentEngagement}%
              </Typography>
              <Typography variant="body1" sx={{ mb: 1 }}>
                Current Level
              </Typography>
              <Typography variant="body2">
                Speaking: {metrics.realTime?.currentSpeaker}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Behavioral Patterns */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                Attention Patterns
              </Typography>
              <Grid container spacing={2}>
                {Object.entries(metrics.behavioral?.attentionPatterns || {}).map(([pattern, percentage]) => (
                  <Grid item xs={4} key={pattern}>
                    <Box textAlign="center">
                      <CircularProgress
                        variant="determinate"
                        value={percentage}
                        size={80}
                        thickness={6}
                        sx={{
                          color: pattern === 'focused' ? 'success.main' : 
                                 pattern === 'distracted' ? 'error.main' : 'warning.main'
                        }}
                      />
                      <Typography variant="h6" sx={{ mt: 1, fontWeight: 600 }}>
                        {percentage}%
                      </Typography>
                      <Typography variant="caption" sx={{ textTransform: 'capitalize' }}>
                        {pattern}
                      </Typography>
                    </Box>
                  </Grid>
                ))}
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                Engagement Triggers
              </Typography>
              <List>
                {metrics.behavioral?.engagementTriggers?.map((trigger, index) => (
                  <ListItem key={index} sx={{ py: 1 }}>
                    <ListItemAvatar>
                      <Avatar sx={{ 
                        bgcolor: 'primary.main',
                        width: 40,
                        height: 40
                      }}>
                        <TrendingUp />
                      </Avatar>
                    </ListItemAvatar>
                    <ListItemText
                      primary={trigger.trigger}
                      secondary={`Impact: ${trigger.impact}`}
                    />
                    <Chip 
                      label={trigger.impact} 
                      color="success" 
                      size="small"
                      variant="outlined"
                    />
                  </ListItem>
                ))}
              </List>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Individual Participant Metrics */}
      <Card>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
            Individual Engagement Analysis
          </Typography>
          
          <Grid container spacing={2}>
            {metrics.individual?.map((participant) => (
              <Grid item xs={12} md={6} lg={4} key={participant.id}>
                <Paper sx={{ 
                  p: 2, 
                  background: 'linear-gradient(135deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0.05) 100%)',
                  backdropFilter: 'blur(10px)',
                  border: '1px solid rgba(0,0,0,0.1)'
                }}>
                  <Box display="flex" alignItems="center" mb={2}>
                    <Avatar sx={{ mr: 2, bgcolor: 'primary.main' }}>
                      {participant.name.charAt(0)}
                    </Avatar>
                    <Box flex={1}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                        {participant.name}
                      </Typography>
                      <Box display="flex" alignItems="center" gap={1}>
                        <Typography variant="body2">
                          {getExpressionEmoji(participant.metrics.facialExpression)}
                        </Typography>
                        <Chip 
                          label={`${participant.engagement.overall}%`}
                          color={getEngagementColor(participant.engagement.overall)}
                          size="small"
                        />
                      </Box>
                    </Box>
                  </Box>

                  <Stack spacing={2}>
                    <Box>
                      <Box display="flex" justifyContent="space-between" mb={0.5}>
                        <Typography variant="caption">Attention</Typography>
                        <Typography variant="caption">{participant.engagement.attention}%</Typography>
                      </Box>
                      <LinearProgress 
                        variant="determinate" 
                        value={participant.engagement.attention}
                        color={getEngagementColor(participant.engagement.attention)}
                        sx={{ height: 6, borderRadius: 3 }}
                      />
                    </Box>

                    <Box>
                      <Box display="flex" justifyContent="space-between" mb={0.5}>
                        <Typography variant="caption">Participation</Typography>
                        <Typography variant="caption">{participant.engagement.participation}%</Typography>
                      </Box>
                      <LinearProgress 
                        variant="determinate" 
                        value={participant.engagement.participation}
                        color={getEngagementColor(participant.engagement.participation)}
                        sx={{ height: 6, borderRadius: 3 }}
                      />
                    </Box>

                    <Box>
                      <Box display="flex" justifyContent="space-between" mb={0.5}>
                        <Typography variant="caption">Interaction</Typography>
                        <Typography variant="caption">{participant.engagement.interaction}%</Typography>
                      </Box>
                      <LinearProgress 
                        variant="determinate" 
                        value={participant.engagement.interaction}
                        color={getEngagementColor(participant.engagement.interaction)}
                        sx={{ height: 6, borderRadius: 3 }}
                      />
                    </Box>

                    <Divider />

                    <Grid container spacing={1}>
                      <Grid item xs={6}>
                        <Box textAlign="center">
                          <Typography variant="h6" color="primary">
                            {participant.metrics.chatMessages}
                          </Typography>
                          <Typography variant="caption">Messages</Typography>
                        </Box>
                      </Grid>
                      <Grid item xs={6}>
                        <Box textAlign="center">
                          <Typography variant="h6" color="secondary">
                            {participant.metrics.reactions}
                          </Typography>
                          <Typography variant="caption">Reactions</Typography>
                        </Box>
                      </Grid>
                      <Grid item xs={6}>
                        <Box textAlign="center">
                          <Typography variant="h6" color="success.main">
                            {Math.floor(participant.metrics.speakingTime / 60)}m
                          </Typography>
                          <Typography variant="caption">Speaking</Typography>
                        </Box>
                      </Grid>
                      <Grid item xs={6}>
                        <Box textAlign="center">
                          <Typography variant="h6" color="warning.main">
                            {participant.metrics.questionsAsked}
                          </Typography>
                          <Typography variant="caption">Questions</Typography>
                        </Box>
                      </Grid>
                    </Grid>

                    <Box>
                      <Typography variant="caption" color="text.secondary">
                        AI Insights: {participant.metrics.eyeMovement} eye movements, 
                        {participant.metrics.handGestures} gestures, 
                        {participant.metrics.attentionSpan}% attention span
                      </Typography>
                    </Box>
                  </Stack>
                </Paper>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {/* Privacy Notice */}
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
          <strong>AI-Powered Analytics:</strong> Engagement metrics are calculated using computer vision, 
          natural language processing, and behavioral analysis. All data is processed with privacy protection 
          and used only for meeting insights.
        </Typography>
      </Alert>
    </Box>
  );
};

export default EngagementMetrics;