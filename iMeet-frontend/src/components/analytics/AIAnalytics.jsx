import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Chip,
  Avatar,
  Stack,
  Divider,
  IconButton,
  Tooltip,
  Paper,
  LinearProgress,
  CircularProgress,
  Alert,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  ListItemAvatar,
  Badge,
  Switch,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import {
  Psychology,
  Visibility,
  Face,
  TrendingUp,
  TrendingDown,
  Timeline,
  Analytics,
  SmartToy,
  Computer,
  Speed,
  Assessment,
  Insights,
  CloudUpload,
  Settings,
  Refresh,
  Download,
  Share,
  ExpandMore,
  CheckCircle,
  Warning,
  Error,
  Info,
  Lightbulb,
  AutoGraph,
  DataUsage,
  ModelTraining,
  Transform,
  Memory,
  BrokenImage,
  Hearing,
  RecordVoiceOver,
  Gesture,
  RemoveRedEye,
  PersonPin,
  ChatBubble,
  ThumbUp,
  AccessTime
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';

const AIAnalytics = ({ meetingId, participants, realTimeMode = true }) => {
  const theme = useTheme();
  const [aiData, setAiData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [processingStatus, setProcessingStatus] = useState('idle');
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
  const [privacySettings, setPrivacySettings] = useState({
    anonymizeData: true,
    localProcessing: true,
    dataRetention: '30days',
    shareAnalytics: false
  });
  const [insightsDialog, setInsightsDialog] = useState(false);
  const [selectedInsight, setSelectedInsight] = useState(null);

  useEffect(() => {
    const initializeAI = async () => {
      setLoading(true);
      setProcessingStatus('initializing');
      
      try {
        // Simulate AI initialization and data processing
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        const mockAIData = {
          overview: {
            totalAnalysisPoints: 15420,
            confidenceScore: 94.2,
            processingTime: '2.3s',
            dataPoints: {
              facial: 8750,
              audio: 4200,
              behavioral: 2470
            },
            accuracy: {
              faceDetection: 97.8,
              emotionRecognition: 89.4,
              speechAnalysis: 92.1,
              gestureRecognition: 85.7
            }
          },
          realTimeMetrics: {
            currentEngagement: 82,
            attentionLevel: 76,
            emotionalState: 'positive',
            speakingClarity: 91,
            backgroundNoise: 15,
            visualQuality: 88
          },
          participantAnalysis: participants.map(participant => ({
            id: participant.id,
            name: participant.name,
            aiMetrics: {
              engagementScore: Math.floor(Math.random() * 30) + 70,
              attentionSpan: Math.floor(Math.random() * 20) + 80,
              emotionalState: ['positive', 'neutral', 'engaged', 'focused'][Math.floor(Math.random() * 4)],
              speakingTime: Math.floor(Math.random() * 300) + 60,
              facialExpressions: {
                smile: Math.floor(Math.random() * 40) + 30,
                focused: Math.floor(Math.random() * 50) + 50,
                confused: Math.floor(Math.random() * 15),
                surprised: Math.floor(Math.random() * 20)
              },
              gestures: {
                handMovements: Math.floor(Math.random() * 50) + 20,
                nodding: Math.floor(Math.random() * 30) + 10,
                pointing: Math.floor(Math.random() * 10)
              },
              voiceAnalysis: {
                clarity: Math.floor(Math.random() * 20) + 80,
                tone: ['confident', 'uncertain', 'enthusiastic'][Math.floor(Math.random() * 3)],
                pace: Math.floor(Math.random() * 40) + 60
              },
              eyeTracking: {
                screenFocus: Math.floor(Math.random() * 30) + 70,
                speakerFocus: Math.floor(Math.random() * 40) + 60,
                distractionEvents: Math.floor(Math.random() * 5)
              },
              behaviorPatterns: {
                multitasking: Math.floor(Math.random() * 30),
                notesTaking: Math.floor(Math.random() * 50) + 20,
                deviceUsage: Math.floor(Math.random() * 40) + 30
              }
            }
          })),
          insights: [
            {
              id: 1,
              type: 'engagement',
              title: 'Peak Engagement During Visual Content',
              description: 'Engagement increased by 34% when screen sharing was active',
              confidence: 92,
              impact: 'high',
              recommendation: 'Use more visual aids and interactive content',
              timestamp: '14:23'
            },
            {
              id: 2,
              type: 'attention',
              title: 'Attention Drop After 45 Minutes',
              description: 'Collective attention decreased by 28% after the 45-minute mark',
              confidence: 88,
              impact: 'medium',
              recommendation: 'Consider shorter meeting durations or breaks',
              timestamp: '14:45'
            },
            {
              id: 3,
              type: 'participation',
              title: 'Uneven Speaking Distribution',
              description: '3 participants dominated 75% of speaking time',
              confidence: 95,
              impact: 'medium',
              recommendation: 'Implement structured turn-taking or breakout sessions',
              timestamp: 'Overall'
            },
            {
              id: 4,
              type: 'emotion',
              title: 'Positive Sentiment Spike',
              description: 'Overall sentiment improved 40% during Q&A segment',
              confidence: 87,
              impact: 'high',
              recommendation: 'Incorporate more interactive Q&A sessions',
              timestamp: '15:10'
            }
          ],
          predictions: {
            nextMeetingOptimalDuration: '52 minutes',
            recommendedBreaks: 2,
            optimalParticipantCount: '8-10 people',
            bestTimeSlot: '10:00 AM - 11:00 AM',
            engagementForecast: 'High (87%)',
            technicalIssuesProbability: 'Low (12%)'
          },
          sentimentAnalysis: {
            overall: 'positive',
            timeline: [
              { time: '10:00', sentiment: 0.6, emotion: 'neutral' },
              { time: '10:15', sentiment: 0.7, emotion: 'positive' },
              { time: '10:30', sentiment: 0.8, emotion: 'engaged' },
              { time: '10:45', sentiment: 0.9, emotion: 'enthusiastic' },
              { time: '11:00', sentiment: 0.7, emotion: 'positive' },
              { time: '11:15', sentiment: 0.6, emotion: 'neutral' }
            ],
            keywords: [
              { word: 'excellent', frequency: 12, sentiment: 0.9 },
              { word: 'challenging', frequency: 8, sentiment: 0.3 },
              { word: 'innovative', frequency: 15, sentiment: 0.8 },
              { word: 'complex', frequency: 6, sentiment: 0.4 }
            ]
          }
        };

        setAiData(mockAIData);
        setProcessingStatus('complete');
      } catch (error) {
        console.error('Error initializing AI analytics:', error);
        setProcessingStatus('error');
      } finally {
        setLoading(false);
      }
    };

    if (meetingId && participants.length > 0) {
      initializeAI();
    }
  }, [meetingId, participants]);

  const getInsightIcon = (type) => {
    switch (type) {
      case 'engagement': return <Psychology color="primary" />;
      case 'attention': return <RemoveRedEye color="secondary" />;
      case 'participation': return <ChatBubble color="success" />;
      case 'emotion': return <Face color="warning" />;
      default: return <Insights color="info" />;
    }
  };

  const getImpactColor = (impact) => {
    switch (impact) {
      case 'high': return 'error';
      case 'medium': return 'warning';
      case 'low': return 'success';
      default: return 'info';
    }
  };

  const getEmotionColor = (emotion) => {
    const colors = {
      positive: 'success',
      negative: 'error',
      neutral: 'info',
      engaged: 'primary',
      focused: 'secondary',
      enthusiastic: 'warning',
      confused: 'error'
    };
    return colors[emotion] || 'default';
  };

  const handleModuleToggle = (module) => {
    setAiModules(prev => ({
      ...prev,
      [module]: !prev[module]
    }));
  };

  if (loading) {
    return (
      <Card sx={{ minHeight: 500, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress size={60} sx={{ mb: 2 }} />
        <Typography variant="h6" sx={{ mb: 1 }}>
          Initializing AI Analytics Engine
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Processing: {processingStatus}
        </Typography>
        <LinearProgress sx={{ width: '300px', mt: 2 }} />
      </Card>
    );
  }

  if (!aiData) {
    return (
      <Alert severity="error" sx={{ m: 2 }}>
        Failed to initialize AI analytics. Please try again.
      </Alert>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      {/* Header */}
      <Card sx={{ 
        mb: 3, 
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white'
      }}>
        <CardContent>
          <Box display="flex" justifyContent="space-between" alignItems="center">
            <Box>
              <Typography variant="h5" sx={{ fontWeight: 600, mb: 1 }}>
                AI-Powered Meeting Analytics
              </Typography>
              <Typography variant="body1">
                Real-time insights powered by computer vision and machine learning
              </Typography>
            </Box>
            <Stack direction="row" spacing={1}>
              <Tooltip title="Refresh Analysis">
                <IconButton sx={{ color: 'white' }}>
                  <Refresh />
                </IconButton>
              </Tooltip>
              <Tooltip title="Download Report">
                <IconButton sx={{ color: 'white' }}>
                  <Download />
                </IconButton>
              </Tooltip>
              <Tooltip title="AI Settings">
                <IconButton sx={{ color: 'white' }}>
                  <Settings />
                </IconButton>
              </Tooltip>
            </Stack>
          </Box>
        </CardContent>
      </Card>

      {/* AI System Overview */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card sx={{ 
            background: 'linear-gradient(45deg, #FF6B6B 30%, #FFE66D 90%)',
            color: 'white',
            height: '100%'
          }}>
            <CardContent sx={{ textAlign: 'center' }}>
              <DataUsage sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {aiData.overview.totalAnalysisPoints.toLocaleString()}
              </Typography>
              <Typography variant="body1">
                Analysis Points
              </Typography>
              <Typography variant="caption">
                Processed in {aiData.overview.processingTime}
              </Typography>
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
              <ModelTraining sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {aiData.overview.confidenceScore}%
              </Typography>
              <Typography variant="body1">
                AI Confidence
              </Typography>
              <Typography variant="caption">
                Machine Learning Accuracy
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
              <Psychology sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600 }}>
                {aiData.realTimeMetrics.currentEngagement}%
              </Typography>
              <Typography variant="body1">
                Live Engagement
              </Typography>
              <Typography variant="caption">
                Real-time Analysis
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
              <Face sx={{ fontSize: 40, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 600, textTransform: 'capitalize' }}>
                {aiData.realTimeMetrics.emotionalState}
              </Typography>
              <Typography variant="body1">
                Group Sentiment
              </Typography>
              <Typography variant="caption">
                Emotion Detection
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* AI Module Status */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
            AI Modules Status
          </Typography>
          <Grid container spacing={2}>
            {Object.entries(aiModules).map(([module, enabled]) => (
              <Grid item xs={12} sm={6} md={3} key={module}>
                <Paper sx={{ p: 2, textAlign: 'center' }}>
                  <FormControlLabel
                    control={
                      <Switch 
                        checked={enabled}
                        onChange={() => handleModuleToggle(module)}
                        color="primary"
                      />
                    }
                    label={
                      <Typography variant="body2" sx={{ textTransform: 'capitalize' }}>
                        {module.replace(/([A-Z])/g, ' $1').trim()}
                      </Typography>
                    }
                  />
                  <Box mt={1}>
                    {enabled ? (
                      <Chip 
                        label="Active" 
                        color="success" 
                        size="small"
                        icon={<CheckCircle />}
                      />
                    ) : (
                      <Chip 
                        label="Disabled" 
                        color="default" 
                        size="small"
                        variant="outlined"
                      />
                    )}
                  </Box>
                </Paper>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {/* AI Insights */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              AI-Generated Insights
            </Typography>
            <Button 
              onClick={() => setInsightsDialog(true)}
              variant="outlined"
              startIcon={<Lightbulb />}
            >
              View All Insights
            </Button>
          </Box>
          
          <Grid container spacing={2}>
            {aiData.insights.slice(0, 4).map((insight) => (
              <Grid item xs={12} md={6} key={insight.id}>
                <Paper sx={{ 
                  p: 2,
                  border: `2px solid ${theme.palette[getImpactColor(insight.impact)].main}`,
                  borderRadius: 2,
                  transition: 'transform 0.2s ease',
                  '&:hover': {
                    transform: 'translateY(-2px)',
                    boxShadow: 4
                  }
                }}>
                  <Box display="flex" alignItems="flex-start" mb={1}>
                    {getInsightIcon(insight.type)}
                    <Box ml={2} flex={1}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                        {insight.title}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                        {insight.description}
                      </Typography>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Chip 
                          label={`${insight.confidence}% confidence`}
                          size="small"
                          color="info"
                          variant="outlined"
                        />
                        <Chip 
                          label={`${insight.impact} impact`}
                          size="small"
                          color={getImpactColor(insight.impact)}
                        />
                        <Typography variant="caption" color="text.secondary">
                          {insight.timestamp}
                        </Typography>
                      </Stack>
                    </Box>
                  </Box>
                  <Divider sx={{ my: 1 }} />
                  <Typography variant="body2" sx={{ fontStyle: 'italic' }}>
                    💡 {insight.recommendation}
                  </Typography>
                </Paper>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {/* Individual Participant AI Analysis */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
            Individual AI Analysis
          </Typography>
          
          <Grid container spacing={2}>
            {aiData.participantAnalysis.map((participant) => (
              <Grid item xs={12} lg={6} key={participant.id}>
                <Accordion>
                  <AccordionSummary expandIcon={<ExpandMore />}>
                    <Box display="flex" alignItems="center" width="100%">
                      <Avatar sx={{ mr: 2 }}>
                        {participant.name.charAt(0)}
                      </Avatar>
                      <Box flex={1}>
                        <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                          {participant.name}
                        </Typography>
                        <Box display="flex" gap={1}>
                          <Chip 
                            label={`${participant.aiMetrics.engagementScore}% engaged`}
                            size="small"
                            color={getEmotionColor(participant.aiMetrics.emotionalState)}
                          />
                          <Chip 
                            label={participant.aiMetrics.emotionalState}
                            size="small"
                            variant="outlined"
                          />
                        </Box>
                      </Box>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    <Grid container spacing={2}>
                      {/* Facial Expression Analysis */}
                      <Grid item xs={12} md={6}>
                        <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                          Facial Expression Analysis
                        </Typography>
                        {Object.entries(participant.aiMetrics.facialExpressions).map(([expression, percentage]) => (
                          <Box key={expression} sx={{ mb: 1 }}>
                            <Box display="flex" justifyContent="space-between">
                              <Typography variant="caption" sx={{ textTransform: 'capitalize' }}>
                                {expression}
                              </Typography>
                              <Typography variant="caption">{percentage}%</Typography>
                            </Box>
                            <LinearProgress 
                              variant="determinate" 
                              value={percentage}
                              sx={{ height: 4, borderRadius: 2 }}
                            />
                          </Box>
                        ))}
                      </Grid>

                      {/* Voice Analysis */}
                      <Grid item xs={12} md={6}>
                        <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                          Voice Analysis
                        </Typography>
                        <Stack spacing={1}>
                          <Box display="flex" justifyContent="space-between">
                            <Typography variant="caption">Clarity</Typography>
                            <Typography variant="caption">
                              {participant.aiMetrics.voiceAnalysis.clarity}%
                            </Typography>
                          </Box>
                          <Box display="flex" justifyContent="space-between">
                            <Typography variant="caption">Tone</Typography>
                            <Chip 
                              label={participant.aiMetrics.voiceAnalysis.tone}
                              size="small"
                              variant="outlined"
                            />
                          </Box>
                          <Box display="flex" justifyContent="space-between">
                            <Typography variant="caption">Speaking Pace</Typography>
                            <Typography variant="caption">
                              {participant.aiMetrics.voiceAnalysis.pace}%
                            </Typography>
                          </Box>
                        </Stack>
                      </Grid>

                      {/* Eye Tracking */}
                      <Grid item xs={12} md={6}>
                        <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                          Attention Tracking
                        </Typography>
                        <Stack spacing={1}>
                          <Box>
                            <Box display="flex" justifyContent="space-between">
                              <Typography variant="caption">Screen Focus</Typography>
                              <Typography variant="caption">
                                {participant.aiMetrics.eyeTracking.screenFocus}%
                              </Typography>
                            </Box>
                            <LinearProgress 
                              variant="determinate" 
                              value={participant.aiMetrics.eyeTracking.screenFocus}
                              color="primary"
                              sx={{ height: 4, borderRadius: 2 }}
                            />
                          </Box>
                          <Box>
                            <Box display="flex" justifyContent="space-between">
                              <Typography variant="caption">Speaker Focus</Typography>
                              <Typography variant="caption">
                                {participant.aiMetrics.eyeTracking.speakerFocus}%
                              </Typography>
                            </Box>
                            <LinearProgress 
                              variant="determinate" 
                              value={participant.aiMetrics.eyeTracking.speakerFocus}
                              color="secondary"
                              sx={{ height: 4, borderRadius: 2 }}
                            />
                          </Box>
                          <Typography variant="caption" color="text.secondary">
                            Distraction Events: {participant.aiMetrics.eyeTracking.distractionEvents}
                          </Typography>
                        </Stack>
                      </Grid>

                      {/* Gesture Recognition */}
                      <Grid item xs={12} md={6}>
                        <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                          Gesture Recognition
                        </Typography>
                        <Grid container spacing={1}>
                          {Object.entries(participant.aiMetrics.gestures).map(([gesture, count]) => (
                            <Grid item xs={4} key={gesture}>
                              <Box textAlign="center">
                                <Typography variant="h6" color="primary">
                                  {count}
                                </Typography>
                                <Typography variant="caption" sx={{ textTransform: 'capitalize' }}>
                                  {gesture.replace(/([A-Z])/g, ' $1').trim()}
                                </Typography>
                              </Box>
                            </Grid>
                          ))}
                        </Grid>
                      </Grid>
                    </Grid>
                  </AccordionDetails>
                </Accordion>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {/* AI Predictions */}
      <Card>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
            AI Predictions & Recommendations
          </Typography>
          
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 2, background: 'linear-gradient(45deg, #E3F2FD 30%, #BBDEFB 90%)' }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2 }}>
                  Next Meeting Optimization
                </Typography>
                <List>
                  <ListItem>
                    <ListItemIcon><AccessTime color="primary" /></ListItemIcon>
                    <ListItemText 
                      primary="Optimal Duration"
                      secondary={aiData.predictions.nextMeetingOptimalDuration}
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemIcon><PersonPin color="primary" /></ListItemIcon>
                    <ListItemText 
                      primary="Ideal Participant Count"
                      secondary={aiData.predictions.optimalParticipantCount}
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemIcon><Schedule color="primary" /></ListItemIcon>
                    <ListItemText 
                      primary="Best Time Slot"
                      secondary={aiData.predictions.bestTimeSlot}
                    />
                  </ListItem>
                </List>
              </Paper>
            </Grid>

            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 2, background: 'linear-gradient(45deg, #F3E5F5 30%, #E1BEE7 90%)' }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 2 }}>
                  Engagement Forecast
                </Typography>
                <List>
                  <ListItem>
                    <ListItemIcon><TrendingUp color="secondary" /></ListItemIcon>
                    <ListItemText 
                      primary="Predicted Engagement"
                      secondary={aiData.predictions.engagementForecast}
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemIcon><Warning color="secondary" /></ListItemIcon>
                    <ListItemText 
                      primary="Technical Issues Risk"
                      secondary={aiData.predictions.technicalIssuesProbability}
                    />
                  </ListItem>
                  <ListItem>
                    <ListItemIcon><Assessment color="secondary" /></ListItemIcon>
                    <ListItemText 
                      primary="Recommended Breaks"
                      secondary={`${aiData.predictions.recommendedBreaks} breaks`}
                    />
                  </ListItem>
                </List>
              </Paper>
            </Grid>
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
          <strong>AI Privacy Protection:</strong> All analysis is performed with advanced privacy safeguards. 
          Facial recognition data is processed locally and encrypted. No personal biometric data is stored permanently.
          {privacySettings.anonymizeData && ' Data is anonymized before processing.'}
        </Typography>
      </Alert>

      {/* Insights Dialog */}
      <Dialog open={insightsDialog} onClose={() => setInsightsDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>
          <Box display="flex" alignItems="center">
            <Lightbulb sx={{ mr: 1 }} />
            Complete AI Insights Report
          </Box>
        </DialogTitle>
        <DialogContent>
          <List>
            {aiData.insights.map((insight) => (
              <ListItem key={insight.id} sx={{ mb: 2 }}>
                <Paper sx={{ p: 2, width: '100%' }}>
                  <Box display="flex" alignItems="flex-start">
                    {getInsightIcon(insight.type)}
                    <Box ml={2} flex={1}>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {insight.title}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                        {insight.description}
                      </Typography>
                      <Typography variant="body2" sx={{ fontStyle: 'italic', mb: 1 }}>
                        💡 {insight.recommendation}
                      </Typography>
                      <Stack direction="row" spacing={1}>
                        <Chip 
                          label={`${insight.confidence}% confidence`}
                          size="small"
                          color="info"
                        />
                        <Chip 
                          label={`${insight.impact} impact`}
                          size="small"
                          color={getImpactColor(insight.impact)}
                        />
                      </Stack>
                    </Box>
                  </Box>
                </Paper>
              </ListItem>
            ))}
          </List>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setInsightsDialog(false)}>Close</Button>
          <Button variant="contained" startIcon={<Download />}>
            Export Report
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default AIAnalytics;