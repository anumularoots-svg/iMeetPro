import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  Button,
  Chip,
  Avatar,
  Stack,
  Divider,
  IconButton,
  Tooltip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  MenuItem,
  LinearProgress,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Fab
} from '@mui/material';
import {
  Download,
  Share,
  Print,
  Email,
  Assessment,
  TrendingUp,
  TrendingDown,
  People,
  AccessTime,
  Chat,
  Videocam,
  Psychology,
  ExpandMore,
  CheckCircle,
  Warning,
  Error,
  Info,
  Timeline,
  BarChart,
  PieChart,
  Speed,
  Visibility,
  VolumeUp,
  Schedule,
  Assignment,
  Analytics
} from '@mui/icons-material';
import { useTheme } from '@mui/material/styles';

const MeetingReports = ({ meetingId, meetingData, onClose }) => {
  const theme = useTheme();
  const [reportData, setReportData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedReport, setSelectedReport] = useState('summary');
  const [exportDialog, setExportDialog] = useState(false);
  const [exportFormat, setExportFormat] = useState('pdf');
  const [emailDialog, setEmailDialog] = useState(false);
  const [emailList, setEmailList] = useState('');

  const reportTypes = [
    { id: 'summary', label: 'Executive Summary', icon: <Assessment /> },
    { id: 'attendance', label: 'Attendance Report', icon: <People /> },
    { id: 'engagement', label: 'Engagement Analysis', icon: <Psychology /> },
    { id: 'participation', label: 'Participation Metrics', icon: <Chat /> },
    { id: 'technical', label: 'Technical Quality', icon: <Speed /> },
    { id: 'ai_insights', label: 'AI Insights', icon: <Analytics /> }
  ];

  useEffect(() => {
    const generateReport = async () => {
      setLoading(true);
      try {
        // Mock report data - replace with actual API call
        const mockReportData = {
          meeting: {
            id: meetingId,
            title: 'Weekly Team Standup',
            date: new Date().toLocaleDateString(),
            startTime: '10:00 AM',
            endTime: '11:30 AM',
            duration: '1h 30m',
            host: 'John Doe',
            participants: 12,
            type: 'ScheduleMeeting'
          },
          summary: {
            overallScore: 85,
            attendanceRate: 92,
            engagementLevel: 78,
            participationRate: 65,
            technicalQuality: 88,
            keyHighlights: [
              'High attendance rate of 92%',
              'Strong engagement during Q&A session',
              'Technical issues resolved quickly',
              'Action items clearly defined'
            ],
            concerns: [
              'Low participation in first 15 minutes',
              'Audio quality issues for 2 participants'
            ],
            recommendations: [
              'Start with icebreaker to boost early engagement',
              'Provide audio troubleshooting guide beforehand',
              'Consider shorter meeting duration'
            ]
          },
          attendance: {
            totalInvited: 15,
            totalJoined: 12,
            onTime: 10,
            late: 2,
            absent: 3,
            averageJoinTime: '10:02 AM',
            participants: [
              { name: 'John Doe', status: 'Host', joinTime: '10:00', leaveTime: '11:30', duration: '1h 30m', attendance: 100 },
              { name: 'Sarah Smith', status: 'Present', joinTime: '10:01', leaveTime: '11:30', duration: '1h 29m', attendance: 98 },
              { name: 'Mike Johnson', status: 'Present', joinTime: '10:05', leaveTime: '11:25', duration: '1h 20m', attendance: 89 },
              { name: 'Emily Davis', status: 'Present', joinTime: '10:00', leaveTime: '11:30', duration: '1h 30m', attendance: 100 },
              { name: 'David Wilson', status: 'Late', joinTime: '10:15', leaveTime: '11:30', duration: '1h 15m', attendance: 83 },
              { name: 'Lisa Brown', status: 'Absent', joinTime: '-', leaveTime: '-', duration: '0m', attendance: 0 }
            ]
          },
          engagement: {
            averageScore: 78,
            peakEngagement: 94,
            lowestEngagement: 52,
            engagementTrend: 'increasing',
            timeline: [
              { time: '10:00', value: 65, event: 'Meeting starts' },
              { time: '10:15', value: 72, event: 'Agenda review' },
              { time: '10:30', value: 85, event: 'Team updates begin' },
              { time: '10:45', value: 94, event: 'Screen sharing demo' },
              { time: '11:00', value: 78, event: 'Q&A session' },
              { time: '11:15', value: 68, event: 'Action items' },
              { time: '11:30', value: 75, event: 'Meeting ends' }
            ],
            topEngagers: [
              { name: 'Sarah Smith', score: 95, interactions: 15 },
              { name: 'Mike Johnson', score: 88, interactions: 12 },
              { name: 'Emily Davis', score: 86, interactions: 10 }
            ]
          },
          participation: {
            totalMessages: 45,
            totalReactions: 28,
            questionsAsked: 12,
            speakingDistribution: [
              { name: 'John Doe (Host)', percentage: 35, time: '31m 30s' },
              { name: 'Sarah Smith', percentage: 15, time: '13m 30s' },
              { name: 'Mike Johnson', percentage: 12, time: '10m 48s' },
              { name: 'Emily Davis', percentage: 8, time: '7m 12s' },
              { name: 'Others', percentage: 30, time: '27m' }
            ],
            chatActivity: [
              { participant: 'Sarah Smith', messages: 8, reactions: 5 },
              { participant: 'Mike Johnson', messages: 6, reactions: 4 },
              { participant: 'Emily Davis', messages: 5, reactions: 3 },
              { participant: 'David Wilson', messages: 3, reactions: 2 }
            ]
          },
          technical: {
            overallQuality: 88,
            audioQuality: 92,
            videoQuality: 85,
            networkStability: 90,
            issues: [
              { type: 'Audio', participant: 'David Wilson', time: '10:15-10:18', resolved: true },
              { type: 'Video', participant: 'Lisa Brown', time: '10:22-10:25', resolved: false },
              { type: 'Network', participant: 'Mike Johnson', time: '11:10-11:12', resolved: true }
            ],
            deviceStats: {
              desktop: 8,
              mobile: 3,
              tablet: 1
            },
            browserStats: {
              chrome: 7,
              firefox: 3,
              safari: 2
            }
          },
          aiInsights: {
            sentimentAnalysis: {
              positive: 68,
              neutral: 25,
              negative: 7
            },
            keyTopics: [
              { topic: 'Project Timeline', mentions: 15, sentiment: 'positive' },
              { topic: 'Resource Allocation', mentions: 8, sentiment: 'neutral' },
              { topic: 'Budget Concerns', mentions: 5, sentiment: 'negative' }
            ],
            actionItems: [
              { item: 'Finalize project requirements', assignee: 'Sarah Smith', dueDate: '2024-01-15' },
              { item: 'Schedule follow-up meeting', assignee: 'John Doe', dueDate: '2024-01-12' },
              { item: 'Review budget allocation', assignee: 'Mike Johnson', dueDate: '2024-01-18' }
            ],
            insights: [
              'Meeting showed high engagement during interactive segments',
              'Participants were most responsive to visual presentations',
              'Technical issues decreased overall satisfaction by 12%',
              'Late joiners had 25% lower engagement scores'
            ]
          }
        };

        setReportData(mockReportData);
      } catch (error) {
        console.error('Error generating report:', error);
      } finally {
        setLoading(false);
      }
    };

    if (meetingId) {
      generateReport();
    }
  }, [meetingId]);

  const getScoreColor = (score) => {
    if (score >= 80) return 'success';
    if (score >= 60) return 'warning';
    return 'error';
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'Present':
      case 'Host':
        return <CheckCircle color="success" />;
      case 'Late':
        return <Warning color="warning" />;
      case 'Absent':
        return <Error color="error" />;
      default:
        return <Info color="info" />;
    }
  };

  const handleExport = () => {
    // Implementation for report export
    console.log(`Exporting report in ${exportFormat} format`);
    setExportDialog(false);
  };

  const handleEmailReport = () => {
    // Implementation for emailing report
    console.log(`Emailing report to: ${emailList}`);
    setEmailDialog(false);
  };

  const renderSummaryReport = () => (
    <Grid container spacing={3}>
      {/* Key Metrics */}
      <Grid item xs={12}>
        <Card>
          <CardContent>
            <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
              Meeting Overview
            </Typography>
            <Grid container spacing={3}>
              <Grid item xs={6} md={2.4}>
                <Box textAlign="center">
                  <Typography variant="h3" color="primary" sx={{ fontWeight: 600 }}>
                    {reportData.summary.overallScore}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Overall Score
                  </Typography>
                </Box>
              </Grid>
              <Grid item xs={6} md={2.4}>
                <Box textAlign="center">
                  <Typography variant="h3" color="success.main" sx={{ fontWeight: 600 }}>
                    {reportData.summary.attendanceRate}%
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Attendance Rate
                  </Typography>
                </Box>
              </Grid>
              <Grid item xs={6} md={2.4}>
                <Box textAlign="center">
                  <Typography variant="h3" color="warning.main" sx={{ fontWeight: 600 }}>
                    {reportData.summary.engagementLevel}%
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Engagement Level
                  </Typography>
                </Box>
              </Grid>
              <Grid item xs={6} md={2.4}>
                <Box textAlign="center">
                  <Typography variant="h3" color="info.main" sx={{ fontWeight: 600 }}>
                    {reportData.summary.participationRate}%
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Participation Rate
                  </Typography>
                </Box>
              </Grid>
              <Grid item xs={6} md={2.4}>
                <Box textAlign="center">
                  <Typography variant="h3" color="secondary.main" sx={{ fontWeight: 600 }}>
                    {reportData.summary.technicalQuality}%
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Technical Quality
                  </Typography>
                </Box>
              </Grid>
            </Grid>
          </CardContent>
        </Card>
      </Grid>

      {/* Highlights & Recommendations */}
      <Grid item xs={12} md={4}>
        <Card sx={{ height: '100%', background: 'linear-gradient(45deg, #4CAF50 30%, #8BC34A 90%)', color: 'white' }}>
          <CardContent>
            <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
              Key Highlights
            </Typography>
            <List>
              {reportData.summary.keyHighlights.map((highlight, index) => (
                <ListItem key={index} sx={{ py: 0.5 }}>
                  <ListItemIcon>
                    <CheckCircle sx={{ color: 'white' }} />
                  </ListItemIcon>
                  <ListItemText primary={highlight} />
                </ListItem>
              ))}
            </List>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} md={4}>
        <Card sx={{ height: '100%', background: 'linear-gradient(45deg, #FF9800 30%, #FFC107 90%)', color: 'white' }}>
          <CardContent>
            <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
              Areas of Concern
            </Typography>
            <List>
              {reportData.summary.concerns.map((concern, index) => (
                <ListItem key={index} sx={{ py: 0.5 }}>
                  <ListItemIcon>
                    <Warning sx={{ color: 'white' }} />
                  </ListItemIcon>
                  <ListItemText primary={concern} />
                </ListItem>
              ))}
            </List>
          </CardContent>
        </Card>
      </Grid>

      <Grid item xs={12} md={4}>
        <Card sx={{ height: '100%', background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)', color: 'white' }}>
          <CardContent>
            <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
              Recommendations
            </Typography>
            <List>
              {reportData.summary.recommendations.map((recommendation, index) => (
                <ListItem key={index} sx={{ py: 0.5 }}>
                  <ListItemIcon>
                    <TrendingUp sx={{ color: 'white' }} />
                  </ListItemIcon>
                  <ListItemText primary={recommendation} />
                </ListItem>
              ))}
            </List>
          </CardContent>
        </Card>
      </Grid>
    </Grid>
  );

  const renderAttendanceReport = () => (
    <Card>
      <CardContent>
        <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
          Attendance Analysis
        </Typography>
        
        <Grid container spacing={3} sx={{ mb: 3 }}>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="h4" color="primary">
                {reportData.attendance.totalJoined}/{reportData.attendance.totalInvited}
              </Typography>
              <Typography variant="body2">Participants</Typography>
            </Box>
          </Grid>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="h4" color="success.main">
                {reportData.attendance.onTime}
              </Typography>
              <Typography variant="body2">On Time</Typography>
            </Box>
          </Grid>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="h4" color="warning.main">
                {reportData.attendance.late}
              </Typography>
              <Typography variant="body2">Late</Typography>
            </Box>
          </Grid>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="h4" color="error.main">
                {reportData.attendance.absent}
              </Typography>
              <Typography variant="body2">Absent</Typography>
            </Box>
          </Grid>
        </Grid>

        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Participant</TableCell>
                <TableCell align="center">Status</TableCell>
                <TableCell align="center">Join Time</TableCell>
                <TableCell align="center">Leave Time</TableCell>
                <TableCell align="center">Duration</TableCell>
                <TableCell align="center">Attendance %</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {reportData.attendance.participants.map((participant, index) => (
                <TableRow key={index}>
                  <TableCell>
                    <Box display="flex" alignItems="center">
                      <Avatar sx={{ mr: 2, width: 32, height: 32 }}>
                        {participant.name.charAt(0)}
                      </Avatar>
                      {participant.name}
                    </Box>
                  </TableCell>
                  <TableCell align="center">
                    <Box display="flex" alignItems="center" justifyContent="center">
                      {getStatusIcon(participant.status)}
                      <Typography sx={{ ml: 1 }}>{participant.status}</Typography>
                    </Box>
                  </TableCell>
                  <TableCell align="center">{participant.joinTime}</TableCell>
                  <TableCell align="center">{participant.leaveTime}</TableCell>
                  <TableCell align="center">{participant.duration}</TableCell>
                  <TableCell align="center">
                    <Box>
                      <Typography variant="body2" sx={{ mb: 0.5 }}>
                        {participant.attendance}%
                      </Typography>
                      <LinearProgress
                        variant="determinate"
                        value={participant.attendance}
                        color={getScoreColor(participant.attendance)}
                        sx={{ width: 60, height: 4, borderRadius: 2 }}
                      />
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </CardContent>
    </Card>
  );

  const renderContent = () => {
    switch (selectedReport) {
      case 'summary':
        return renderSummaryReport();
      case 'attendance':
        return renderAttendanceReport();
      case 'engagement':
        return (
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>Engagement Analysis</Typography>
              <Alert severity="info">
                Detailed engagement metrics and timeline analysis will be displayed here.
              </Alert>
            </CardContent>
          </Card>
        );
      case 'participation':
        return (
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>Participation Metrics</Typography>
              <Alert severity="info">
                Speaking time distribution and interaction metrics will be displayed here.
              </Alert>
            </CardContent>
          </Card>
        );
      case 'technical':
        return (
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>Technical Quality Report</Typography>
              <Alert severity="info">
                Audio/video quality metrics and technical issues will be displayed here.
              </Alert>
            </CardContent>
          </Card>
        );
      case 'ai_insights':
        return (
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>AI-Generated Insights</Typography>
              <Alert severity="info">
                AI analysis results and recommendations will be displayed here.
              </Alert>
            </CardContent>
          </Card>
        );
      default:
        return renderSummaryReport();
    }
  };

  if (loading) {
    return (
      <Card sx={{ minHeight: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Typography>Generating meeting report...</Typography>
      </Card>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      {/* Header */}
      <Card sx={{ mb: 3, background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
        <CardContent>
          <Box display="flex" justifyContent="space-between" alignItems="center">
            <Box>
              <Typography variant="h4" sx={{ fontWeight: 600, mb: 1 }}>
                Meeting Report
              </Typography>
              <Typography variant="body1">
                {reportData.meeting.title} • {reportData.meeting.date} • {reportData.meeting.duration}
              </Typography>
            </Box>
            <Stack direction="row" spacing={1}>
              <Tooltip title="Export Report">
                <IconButton onClick={() => setExportDialog(true)} sx={{ color: 'white' }}>
                  <Download />
                </IconButton>
              </Tooltip>
              <Tooltip title="Email Report">
                <IconButton onClick={() => setEmailDialog(true)} sx={{ color: 'white' }}>
                  <Email />
                </IconButton>
              </Tooltip>
              <Tooltip title="Print Report">
                <IconButton sx={{ color: 'white' }}>
                  <Print />
                </IconButton>
              </Tooltip>
              <Tooltip title="Share Report">
                <IconButton sx={{ color: 'white' }}>
                  <Share />
                </IconButton>
              </Tooltip>
            </Stack>
          </Box>
        </CardContent>
      </Card>

      {/* Report Type Selector */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Stack direction="row" spacing={1} flexWrap="wrap">
            {reportTypes.map((type) => (
              <Chip
                key={type.id}
                icon={type.icon}
                label={type.label}
                onClick={() => setSelectedReport(type.id)}
                color={selectedReport === type.id ? 'primary' : 'default'}
                variant={selectedReport === type.id ? 'filled' : 'outlined'}
                sx={{ mb: 1 }}
              />
            ))}
          </Stack>
        </CardContent>
      </Card>

      {/* Report Content */}
      {renderContent()}

      {/* Export Dialog */}
      <Dialog open={exportDialog} onClose={() => setExportDialog(false)}>
        <DialogTitle>Export Report</DialogTitle>
        <DialogContent>
          <TextField
            select
            label="Format"
            value={exportFormat}
            onChange={(e) => setExportFormat(e.target.value)}
            fullWidth
            margin="dense"
          >
            <MenuItem value="pdf">PDF</MenuItem>
            <MenuItem value="excel">Excel</MenuItem>
            <MenuItem value="word">Word Document</MenuItem>
            <MenuItem value="powerpoint">PowerPoint</MenuItem>
          </TextField>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setExportDialog(false)}>Cancel</Button>
          <Button onClick={handleExport} variant="contained">Export</Button>
        </DialogActions>
      </Dialog>

      {/* Email Dialog */}
      <Dialog open={emailDialog} onClose={() => setEmailDialog(false)}>
        <DialogTitle>Email Report</DialogTitle>
        <DialogContent>
          <TextField
            label="Email Recipients"
            value={emailList}
            onChange={(e) => setEmailList(e.target.value)}
            fullWidth
            margin="dense"
            placeholder="Enter email addresses separated by commas"
            multiline
            rows={3}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEmailDialog(false)}>Cancel</Button>
          <Button onClick={handleEmailReport} variant="contained">Send</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default MeetingReports;