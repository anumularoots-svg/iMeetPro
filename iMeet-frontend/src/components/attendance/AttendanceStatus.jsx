// src/components/attendance/AttendanceStatus.jsx
import React from 'react';
import {
  Box,
  Typography,
  Chip,
  LinearProgress,
  Tooltip,
  IconButton,
  Card,
  CardContent
} from '@mui/material';
import {
  Visibility,
  Warning,
  CheckCircle,
  Error as ErrorIcon,
  Coffee,
  Timer,
  TrendingUp,
  Info
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

const StatusCard = styled(Card)(({ theme }) => ({
  minWidth: 200,
  backgroundColor: 'rgba(0,0,0,0.8)',
  backdropFilter: 'blur(10px)',
  border: '1px solid rgba(255,255,255,0.1)',
  color: 'white',
}));

const StatusChip = styled(Chip)(({ theme, status }) => ({
  height: 20,
  fontSize: '0.65rem',
  fontWeight: 600,
  color: 'white',
  backgroundColor: 
    status === 'excellent' ? 'rgba(76,175,80,0.8)' :
    status === 'good' ? 'rgba(139,195,74,0.8)' :
    status === 'warning' ? 'rgba(255,152,0,0.8)' :
    status === 'poor' ? 'rgba(244,67,54,0.8)' :
    'rgba(158,158,158,0.8)',
}));

const MetricBox = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  marginBottom: theme.spacing(0.5),
}));

const AttendanceStatus = ({
  attendancePercentage = 100,
  engagementScore = 100,
  sessionDuration = 0,
  breakUsed = false,
  violations = [],
  isTracking = false,
  popupCount = 0,
  maxPopups = 4,
  onShowDetails,
  compact = false
}) => {
  // Calculate overall status
  const getOverallStatus = () => {
    if (attendancePercentage >= 90 && violations.length === 0) return 'excellent';
    if (attendancePercentage >= 80 && violations.length <= 2) return 'good';
    if (attendancePercentage >= 60 || violations.length <= 5) return 'warning';
    return 'poor';
  };

  const getStatusIcon = () => {
    const status = getOverallStatus();
    switch (status) {
      case 'excellent':
      case 'good':
        return <CheckCircle sx={{ fontSize: 16, color: '#4caf50' }} />;
      case 'warning':
        return <Warning sx={{ fontSize: 16, color: '#ff9800' }} />;
      case 'poor':
        return <ErrorIcon sx={{ fontSize: 16, color: '#f44336' }} />;
      default:
        return <CheckCircle sx={{ fontSize: 16, color: '#4caf50' }} />;
    }
  };

  const formatDuration = (seconds) => {
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    
    if (hrs > 0) {
      return `${hrs}h ${mins}m`;
    }
    return `${mins}m`;
  };

  const getStatusText = () => {
    const status = getOverallStatus();
    switch (status) {
      case 'excellent': return 'Excellent';
      case 'good': return 'Good';
      case 'warning': return 'Needs Attention';
      case 'poor': return 'Critical';
      default: return 'Good';
    }
  };

  if (compact) {
    return (
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: 1,
        backgroundColor: 'rgba(0,0,0,0.7)',
        padding: '4px 8px',
        borderRadius: 1,
        border: '1px solid rgba(255,255,255,0.1)'
      }}>
        {getStatusIcon()}
        <Typography variant="caption" sx={{ color: 'white', fontWeight: 600 }}>
          {Math.round(attendancePercentage)}%
        </Typography>
        {violations.length > 0 && (
          <Chip
            label={violations.length}
            size="small"
            sx={{
              height: 16,
              fontSize: '0.6rem',
              backgroundColor: 'rgba(255,152,0,0.8)',
              color: 'white'
            }}
          />
        )}
      </Box>
    );
  }

  return (
    <StatusCard>
      <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {getStatusIcon()}
            <Typography variant="subtitle2" sx={{ fontWeight: 600, fontSize: '0.8rem' }}>
              Attendance Status
            </Typography>
          </Box>
          {onShowDetails && (
            <IconButton size="small" onClick={onShowDetails} sx={{ color: 'white' }}>
              <Info sx={{ fontSize: 14 }} />
            </IconButton>
          )}
        </Box>

        {/* Overall Status */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
          <StatusChip
            label={getStatusText()}
            status={getOverallStatus()}
            size="small"
          />
          <Typography variant="caption" sx={{ 
            color: isTracking ? '#4caf50' : '#ff9800',
            fontWeight: 600,
            fontSize: '0.65rem'
          }}>
            {isTracking ? 'TRACKING' : 'PAUSED'}
          </Typography>
        </Box>

        {/* Main Metrics */}
        <MetricBox>
          <Typography variant="caption" sx={{ fontSize: '0.7rem' }}>
            Attendance Score
          </Typography>
          <Typography variant="caption" sx={{ fontWeight: 600, fontSize: '0.7rem' }}>
            {Math.round(attendancePercentage)}%
          </Typography>
        </MetricBox>

        <LinearProgress
          variant="determinate"
          value={attendancePercentage}
          sx={{
            height: 4,
            borderRadius: 2,
            mb: 1,
            backgroundColor: 'rgba(255,255,255,0.1)',
            '& .MuiLinearProgress-bar': {
              backgroundColor: 
                attendancePercentage >= 90 ? '#4caf50' :
                attendancePercentage >= 80 ? '#8bc34a' :
                attendancePercentage >= 60 ? '#ff9800' : '#f44336'
            }
          }}
        />

        {/* Secondary Metrics */}
        <MetricBox>
          <Typography variant="caption" sx={{ fontSize: '0.7rem' }}>
            Engagement
          </Typography>
          <Typography variant="caption" sx={{ fontWeight: 600, fontSize: '0.7rem' }}>
            {Math.round(engagementScore)}%
          </Typography>
        </MetricBox>

        <MetricBox>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Timer sx={{ fontSize: 12, color: 'grey.400' }} />
            <Typography variant="caption" sx={{ fontSize: '0.7rem' }}>
              Session Time
            </Typography>
          </Box>
          <Typography variant="caption" sx={{ fontWeight: 600, fontSize: '0.7rem' }}>
            {formatDuration(sessionDuration)}
          </Typography>
        </MetricBox>

        {/* Warning Status */}
        <MetricBox>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Warning sx={{ fontSize: 12, color: popupCount > 0 ? '#ff9800' : 'grey.400' }} />
            <Typography variant="caption" sx={{ fontSize: '0.7rem' }}>
              Warnings
            </Typography>
          </Box>
          <Typography variant="caption" sx={{ 
            fontWeight: 600, 
            fontSize: '0.7rem',
            color: popupCount >= maxPopups * 0.75 ? '#ff9800' : 'inherit'
          }}>
            {popupCount}/{maxPopups}
          </Typography>
        </MetricBox>

        {/* Break Status */}
        <MetricBox>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Coffee sx={{ fontSize: 12, color: breakUsed ? '#ff9800' : '#4caf50' }} />
            <Typography variant="caption" sx={{ fontSize: '0.7rem' }}>
              Break
            </Typography>
          </Box>
          <Typography variant="caption" sx={{ fontWeight: 600, fontSize: '0.7rem' }}>
            {breakUsed ? 'Used' : 'Available'}
          </Typography>
        </MetricBox>

        {/* Violations Summary */}
        {violations.length > 0 && (
          <Box sx={{ mt: 1, p: 1, backgroundColor: 'rgba(255,152,0,0.1)', borderRadius: 0.5 }}>
            <Typography variant="caption" sx={{ 
              fontSize: '0.65rem', 
              color: '#ffb74d',
              fontWeight: 600,
              display: 'block',
              mb: 0.5
            }}>
              Recent Issues ({violations.length}):
            </Typography>
            {violations.slice(-2).map((violation, index) => (
              <Typography key={index} variant="caption" sx={{ 
                fontSize: '0.6rem', 
                color: 'grey.300',
                display: 'block'
              }}>
                • {typeof violation === 'object' ? violation.type : violation}
              </Typography>
            ))}
          </Box>
        )}

        {/* Status Indicators */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 1 }}>
          <Box sx={{ display: 'flex', gap: 0.5 }}>
            <Tooltip title="Face Detection Active">
              <Visibility sx={{ 
                fontSize: 12, 
                color: isTracking ? '#4caf50' : '#666'
              }} />
            </Tooltip>
            <Tooltip title={`Engagement: ${getStatusText()}`}>
              <TrendingUp sx={{ 
                fontSize: 12, 
                color: 
                  getOverallStatus() === 'excellent' ? '#4caf50' :
                  getOverallStatus() === 'good' ? '#8bc34a' :
                  getOverallStatus() === 'warning' ? '#ff9800' : '#f44336'
              }} />
            </Tooltip>
          </Box>
          
          <Typography variant="caption" sx={{ 
            fontSize: '0.6rem', 
            color: 'grey.500',
            fontStyle: 'italic'
          }}>
            AI Monitored
          </Typography>
        </Box>
      </CardContent>
    </StatusCard>
  );
};

export default AttendanceStatus;