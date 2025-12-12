import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  Card,
  CardContent,
  Avatar,
  Chip,
  useTheme,
  alpha,
  LinearProgress,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Divider,
  IconButton,
  Tooltip,
  Badge
} from '@mui/material';
import {
  CheckCircle,
  Cancel,
  Schedule,
  Help,
  Group,
  TrendingUp,
  Email,
  Phone,
  Refresh,
  Download,
  Visibility,
  Close,
  Person,
  AccessTime
} from '@mui/icons-material';
import { PieChart, Pie, Cell, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip } from 'recharts';

const RSVPStatus = ({ meetingId, invitations, onRefresh }) => {
  const theme = useTheme();
  const [rsvpData, setRsvpData] = useState([]);
  const [selectedStatus, setSelectedStatus] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  // RSVP status colors matching your backend Meeting_Invitations table
  const statusColors = {
    accepted: theme.palette.success.main,
    declined: theme.palette.error.main,
    pending: theme.palette.warning.main,
    maybe: theme.palette.info.main
  };

  // Process RSVP data
  useEffect(() => {
    if (invitations && invitations.length > 0) {
      processRSVPData();
    }
  }, [invitations]);

  const processRSVPData = () => {
    const statusCounts = {
      accepted: 0,
      declined: 0,
      pending: 0,
      maybe: 0
    };

    // Count RSVP statuses from Meeting_Invitations table data
    invitations.forEach(invitation => {
      if (statusCounts.hasOwnProperty(invitation.rsvpStatus)) {
        statusCounts[invitation.rsvpStatus]++;
      }
    });

    const total = invitations.length;
    const chartData = Object.keys(statusCounts).map(status => ({
      name: status.charAt(0).toUpperCase() + status.slice(1),
      value: statusCounts[status],
      percentage: total > 0 ? Math.round((statusCounts[status] / total) * 100) : 0,
      color: statusColors[status]
    }));

    setRsvpData(chartData);
  };

  const handleRefreshData = async () => {
    setLoading(true);
    try {
      // Backend API call to refresh invitation data
      // const response = await fetch(`/api/meeting-invitations/${meetingId}/rsvp-status`);
      // const updatedInvitations = await response.json();
      
      if (onRefresh) {
        await onRefresh();
      }
    } catch (error) {
      console.error('Error refreshing RSVP data:', error);
    } finally {
      setLoading(false);
    }
  };

  const getInvitationsByStatus = (status) => {
    return invitations.filter(inv => inv.rsvpStatus === status.toLowerCase());
  };

  const getTotalResponses = () => {
    return invitations.filter(inv => inv.rsvpStatus !== 'pending').length;
  };

  const getResponseRate = () => {
    const total = invitations.length;
    const responses = getTotalResponses();
    return total > 0 ? Math.round((responses / total) * 100) : 0;
  };

  const renderStatusCard = (status, count, percentage, color) => (
    <Card 
      sx={{ 
        cursor: 'pointer',
        border: `2px solid ${alpha(color, 0.3)}`,
        borderRadius: 3,
        '&:hover': {
          transform: 'translateY(-4px)',
          boxShadow: theme.shadows[8],
          borderColor: color
        },
        transition: 'all 0.3s ease'
      }}
      onClick={() => {
        setSelectedStatus(status.toLowerCase());
        setDetailsDialogOpen(true);
      }}
    >
      <CardContent sx={{ textAlign: 'center', p: 3 }}>
        <Avatar 
          sx={{ 
            bgcolor: color,
            width: 56,
            height: 56,
            mx: 'auto',
            mb: 2
          }}
        >
          {status === 'Accepted' && <CheckCircle sx={{ fontSize: 32 }} />}
          {status === 'Declined' && <Cancel sx={{ fontSize: 32 }} />}
          {status === 'Pending' && <Schedule sx={{ fontSize: 32 }} />}
          {status === 'Maybe' && <Help sx={{ fontSize: 32 }} />}
        </Avatar>
        <Typography variant="h4" sx={{ fontWeight: 'bold', color: color, mb: 1 }}>
          {count}
        </Typography>
        <Typography variant="h6" sx={{ fontWeight: 'bold', mb: 1 }}>
          {status}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {percentage}% of total
        </Typography>
        <LinearProgress 
          variant="determinate" 
          value={percentage} 
          sx={{ 
            mt: 2,
            height: 8,
            borderRadius: 4,
            '& .MuiLinearProgress-bar': {
              backgroundColor: color
            }
          }}
        />
      </CardContent>
    </Card>
  );

  const renderPieChart = () => (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie
          data={rsvpData}
          cx="50%"
          cy="50%"
          innerRadius={60}
          outerRadius={120}
          paddingAngle={5}
          dataKey="value"
        >
          {rsvpData.map((entry, index) => (
            <Cell key={`cell-${index}`} fill={entry.color} />
          ))}
        </Pie>
        <RechartsTooltip 
          formatter={(value, name) => [`${value} people`, name]}
          labelFormatter={() => ''}
        />
      </PieChart>
    </ResponsiveContainer>
  );

  const renderBarChart = () => (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={rsvpData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="name" />
        <YAxis />
        <RechartsTooltip 
          formatter={(value) => [`${value} people`, 'Count']}
        />
        <Bar dataKey="value" fill={theme.palette.primary.main} radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );

  return (
    <Paper elevation={4} sx={{ 
      p: 4, 
      borderRadius: 4,
      background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.02)}, ${alpha(theme.palette.secondary.main, 0.02)})`,
      border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}`
    }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <Avatar sx={{ 
            mr: 3, 
            bgcolor: theme.palette.success.main,
            width: 56,
            height: 56
          }}>
            <TrendingUp sx={{ fontSize: 28 }} />
          </Avatar>
          <Box>
            <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.primary.main }}>
              RSVP Status
            </Typography>
            <Typography variant="body1" color="text.secondary">
              Track participant responses to your meeting invitation
            </Typography>
          </Box>
        </Box>
        <Button
          variant="contained"
          startIcon={<Refresh />}
          onClick={handleRefreshData}
          disabled={loading}
          sx={{ borderRadius: 3 }}
        >
          Refresh
        </Button>
      </Box>

      {/* Loading */}
      {loading && <LinearProgress sx={{ mb: 3, borderRadius: 2 }} />}

      {/* Overview Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={3}>
          <Card sx={{ 
            textAlign: 'center', 
            border: `2px solid ${alpha(theme.palette.primary.main, 0.3)}`,
            borderRadius: 3
          }}>
            <CardContent>
              <Group sx={{ fontSize: 48, color: theme.palette.primary.main, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.primary.main }}>
                {invitations.length}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Total Invitations
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ 
            textAlign: 'center', 
            border: `2px solid ${alpha(theme.palette.info.main, 0.3)}`,
            borderRadius: 3
          }}>
            <CardContent>
              <CheckCircle sx={{ fontSize: 48, color: theme.palette.info.main, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.info.main }}>
                {getTotalResponses()}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Total Responses
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ 
            textAlign: 'center', 
            border: `2px solid ${alpha(theme.palette.success.main, 0.3)}`,
            borderRadius: 3
          }}>
            <CardContent>
              <TrendingUp sx={{ fontSize: 48, color: theme.palette.success.main, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.success.main }}>
                {getResponseRate()}%
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Response Rate
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ 
            textAlign: 'center', 
            border: `2px solid ${alpha(theme.palette.warning.main, 0.3)}`,
            borderRadius: 3
          }}>
            <CardContent>
              <AccessTime sx={{ fontSize: 48, color: theme.palette.warning.main, mb: 1 }} />
              <Typography variant="h4" sx={{ fontWeight: 'bold', color: theme.palette.warning.main }}>
                {invitations.filter(inv => inv.rsvpStatus === 'pending').length}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Awaiting Response
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* RSVP Status Cards */}
      <Typography variant="h5" sx={{ fontWeight: 'bold', mb: 3 }}>
        Response Breakdown
      </Typography>
      <Grid container spacing={3} sx={{ mb: 4 }}>
        {rsvpData.map((status) => (
          <Grid item xs={12} sm={6} md={3} key={status.name}>
            {renderStatusCard(status.name, status.value, status.percentage, status.color)}
          </Grid>
        ))}
      </Grid>

      {/* Charts */}
      <Grid container spacing={4}>
        <Grid item xs={12} md={6}>
          <Card sx={{ border: `1px solid ${theme.palette.divider}`, borderRadius: 3 }}>
            <CardContent>
              <Typography variant="h6" sx={{ fontWeight: 'bold', mb: 2, textAlign: 'center' }}>
                RSVP Distribution
              </Typography>
              {renderPieChart()}
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={6}>
          <Card sx={{ border: `1px solid ${theme.palette.divider}`, borderRadius: 3 }}>
            <CardContent>
              <Typography variant="h6" sx={{ fontWeight: 'bold', mb: 2, textAlign: 'center' }}>
                Response Count
              </Typography>
              {renderBarChart()}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Status Details Dialog */}
      <Dialog 
        open={detailsDialogOpen} 
        onClose={() => setDetailsDialogOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          bgcolor: selectedStatus ? statusColors[selectedStatus] : theme.palette.primary.main,
          color: 'white'
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            {selectedStatus === 'accepted' && <CheckCircle sx={{ mr: 2 }} />}
            {selectedStatus === 'declined' && <Cancel sx={{ mr: 2 }} />}
            {selectedStatus === 'pending' && <Schedule sx={{ mr: 2 }} />}
            {selectedStatus === 'maybe' && <Help sx={{ mr: 2 }} />}
            <Typography variant="h6" sx={{ fontWeight: 'bold' }}>
              {selectedStatus ? selectedStatus.charAt(0).toUpperCase() + selectedStatus.slice(1) : ''} Responses
            </Typography>
          </Box>
          <IconButton onClick={() => setDetailsDialogOpen(false)} sx={{ color: 'white' }}>
            <Close />
          </IconButton>
        </DialogTitle>
        <DialogContent sx={{ p: 0 }}>
          <List>
            {selectedStatus && getInvitationsByStatus(selectedStatus).map((invitation, index) => (
              <React.Fragment key={invitation.id}>
                <ListItem sx={{ py: 2 }}>
                  <ListItemAvatar>
                    <Avatar sx={{ 
                      bgcolor: statusColors[selectedStatus],
                      width: 48,
                      height: 48
                    }}>
                      {invitation.fullName.charAt(0).toUpperCase()}
                    </Avatar>
                  </ListItemAvatar>
                  <ListItemText
                    primary={
                      <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
                        {invitation.fullName}
                      </Typography>
                    }
                    secondary={
                      <Box>
                        <Typography variant="body2" color="text.secondary">
                          {invitation.email}
                        </Typography>
                        {invitation.respondedAt && (
                          <Typography variant="caption" color="text.secondary">
                            Responded: {new Date(invitation.respondedAt).toLocaleDateString()} at{' '}
                            {new Date(invitation.respondedAt).toLocaleTimeString()}
                          </Typography>
                        )}
                        {selectedStatus === 'pending' && invitation.sentAt && (
                          <Typography variant="caption" color="text.secondary">
                            Invited: {new Date(invitation.sentAt).toLocaleDateString()}
                          </Typography>
                        )}
                      </Box>
                    }
                  />
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <Tooltip title="Send Email">
                      <IconButton size="small" sx={{ color: theme.palette.primary.main }}>
                        <Email fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="View Details">
                      <IconButton size="small" sx={{ color: theme.palette.info.main }}>
                        <Visibility fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Box>
                </ListItem>
                {index < getInvitationsByStatus(selectedStatus).length - 1 && <Divider />}
              </React.Fragment>
            ))}
          </List>
          {selectedStatus && getInvitationsByStatus(selectedStatus).length === 0 && (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <Person sx={{ fontSize: 64, color: theme.palette.grey[400], mb: 2 }} />
              <Typography variant="h6" color="text.secondary">
                No {selectedStatus} responses yet
              </Typography>
            </Box>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 3, bgcolor: alpha(theme.palette.grey[100], 0.5) }}>
          <Button
            startIcon={<Download />}
            onClick={() => {/* Export functionality */}}
            variant="outlined"
          >
            Export List
          </Button>
          <Button onClick={() => setDetailsDialogOpen(false)} variant="contained">
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};

export default RSVPStatus;