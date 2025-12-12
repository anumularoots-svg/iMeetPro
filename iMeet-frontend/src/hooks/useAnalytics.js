// src/hooks/useAnalytics.js - FIXED VERSION with proper blob handling
import { useState, useEffect, useCallback } from 'react';
import { analyticsAPI } from '../services/api';
import { useAuth } from './useAuth';

export const useAnalytics = () => {
  const { user } = useAuth();
  const [analyticsData, setAnalyticsData] = useState({
    // Comprehensive data
    comprehensiveData: null,
    
    // Host analytics data
    hostOverview: null,
    hostMeetings: [],
    hostEngagement: null,
    hostTrends: null,
    
    // Participant analytics data  
    participantReport: null,
    participantAttendance: [],
    participantEngagement: null,
    participantDuration: null,
    
    // Meeting analytics data
    meetingAnalytics: [],
    
    // Common data
    filters: null,
    preferences: null
  });
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Clear error function
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  // Error handler
  const handleError = useCallback((error, context) => {
    console.error(`${context} error:`, error);
    const errorMessage = error.response?.data?.error || 
                        error.response?.data?.Error || 
                        error.message || 
                        `Failed to ${context.toLowerCase()}`;
    setError(errorMessage);
    return null;
  }, []);

  // ==================== COMPREHENSIVE ANALYTICS ====================

  const fetchComprehensiveAnalytics = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.user_id || filters.userId || user?.id,
        meeting_id: filters.meeting_id || filters.meetingId,
        analytics_type: filters.analytics_type || 'all',
        timeframe: filters.period || filters.timeframe || '30days',
        meetingType: filters.meetingType || filters.meeting_type || 'all',
        page: filters.page || 1,
        limit: filters.limit || 100,
        start_date: filters.start_date || filters.dateRange?.start,
        end_date: filters.end_date || filters.dateRange?.end
      };
      
      // Remove undefined values
      Object.keys(params).forEach(key => {
        if (params[key] === undefined) {
          delete params[key];
        }
      });
      
      console.log('Fetching comprehensive analytics with params:', params);
      const response = await analyticsAPI.getComprehensiveAnalytics(params);
      
      const data = response.data?.data || response.data;
      
      setAnalyticsData(prev => ({
        ...prev,
        comprehensiveData: data,
        hostOverview: data.overall_summary,
        hostMeetings: data.meeting_analytics || [],
        hostEngagement: data.host_analytics || [],
        participantReport: data.participant_summary?.[0] || data.participant_details?.[0],
        participantAttendance: data.participant_details || [],
        meetingAnalytics: data.meeting_analytics || []
      }));
      
      return data;
    } catch (err) {
      return handleError(err, 'Fetch comprehensive analytics');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  // ==================== HOST ANALYTICS FUNCTIONS ====================

  const fetchHostOverview = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.user_id || filters.userId || filters.host_id || user?.id,
        timeframe: filters.period || filters.timeframe || '30days',
        meetingType: filters.meetingType || filters.meeting_type || 'all'
      };
      
      console.log('Fetching host overview with params:', params);
      const response = await analyticsAPI.getHostDashboardOverview(params);
      
      const data = response.data?.data || response.data;
      
      setAnalyticsData(prev => ({
        ...prev,
        hostOverview: data
      }));
      
      return data;
    } catch (err) {
      return handleError(err, 'Fetch host overview');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const fetchHostMeetingReports = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.user_id || filters.userId || filters.host_id || user?.id,
        analytics_type: 'meeting',
        timeframe: filters.period || filters.timeframe || '30days',
        meetingType: filters.meetingType || filters.meeting_type || 'all',
        page: filters.page || 1,
        limit: filters.limit || 50,
        start_date: filters.dateRange?.start || filters.start_date,
        end_date: filters.dateRange?.end || filters.end_date
      };
      
      // Remove undefined values
      Object.keys(params).forEach(key => {
        if (params[key] === undefined) {
          delete params[key];
        }
      });
      
      console.log('Fetching host meeting reports with params:', params);
      const response = await analyticsAPI.getHostMeetingReports(params);
      
      const data = response.data?.data || response.data;
      const meetings = data.meeting_analytics || [];
      
      setAnalyticsData(prev => ({
        ...prev,
        hostMeetings: meetings,
        meetingReports: meetings
      }));
      
      return { meetings, ...data };
    } catch (err) {
      return handleError(err, 'Fetch meeting reports');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const fetchHostEngagementDistribution = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.user_id || filters.userId || filters.host_id || user?.id,
        host_id: filters.user_id || filters.userId || filters.host_id || user?.id,
        analytics_type: 'host',
        timeframe: filters.period || filters.timeframe || '30days',
        meetingType: filters.meetingType || filters.meeting_type || 'all'
      };
      
      console.log('Fetching host engagement distribution with params:', params);
      const response = await analyticsAPI.getHostEngagementDistribution(params);
      
      const data = response.data?.data || response.data;
      const engagement = data.host_analytics || [];
      
      setAnalyticsData(prev => ({
        ...prev,
        hostEngagement: { distribution: engagement }
      }));
      
      return { distribution: engagement };
    } catch (err) {
      return handleError(err, 'Fetch engagement distribution');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const fetchHostTrends = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        host_id: filters.user_id || filters.userId || filters.host_id || user?.id,
        timeframe: filters.period || filters.timeframe || '30days',
        metric: filters.metric || 'meetings',
        meeting_type: filters.meetingType || filters.meeting_type || 'all'
      };
      
      console.log('Fetching host trends with params:', params);
      const response = await analyticsAPI.getHostMeetingTrends(params);
      
      const data = response.data?.data || response.data;
      const trends = Array.isArray(data) ? data : [];
      
      setAnalyticsData(prev => ({
        ...prev,
        hostTrends: { trends }
      }));
      
      return { trends };
    } catch (err) {
      return handleError(err, 'Fetch meeting trends');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  // ==================== PARTICIPANT ANALYTICS FUNCTIONS ====================

  const fetchParticipantPersonalReport = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        timeframe: filters.period || filters.timeframe || '30days',
        userId: filters.userId || filters.user_id || user?.id
      };
      
      console.log('Fetching participant personal report with params:', params);
      const response = await analyticsAPI.getParticipantPersonalReport(user?.id, params);
      
      const data = response.data?.data || response.data;
      const report = data.participant_summary?.[0] || data.participant_details?.[0] || {};
      
      setAnalyticsData(prev => ({
        ...prev,
        participantReport: report
      }));
      
      return report;
    } catch (err) {
      return handleError(err, 'Fetch personal report');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const fetchParticipantAttendance = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.userId || filters.user_id || user?.id,
        timeframe: filters.period || filters.timeframe || '30days',
        meeting_type: filters.meetingType || filters.meeting_type || 'all',
        start_date: filters.dateRange?.start,
        end_date: filters.dateRange?.end
      };
      
      // Remove undefined values
      Object.keys(params).forEach(key => {
        if (params[key] === undefined) {
          delete params[key];
        }
      });
      
      console.log('Fetching participant attendance with params:', params);
      const response = await analyticsAPI.getParticipantAttendance(user?.id, params);
      
      const data = response.data?.data || response.data;
      const attendanceRecords = data.attendance_details || [];
      
      setAnalyticsData(prev => ({
        ...prev,
        participantAttendance: attendanceRecords
      }));
      
      return { attendanceRecords, ...data };
    } catch (err) {
      return handleError(err, 'Fetch attendance data');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const fetchParticipantEngagement = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.userId || filters.user_id || user?.id,
        meeting_id: filters.meetingId || filters.meeting_id,
        timeframe: filters.period || filters.timeframe || '30days'
      };
      
      console.log('Fetching participant engagement with params:', params);
      const response = await analyticsAPI.getParticipantEngagement(user?.id, params);
      
      const data = response.data?.data || response.data;
      const engagementRecords = Array.isArray(data) ? data : [];
      
      setAnalyticsData(prev => ({
        ...prev,
        participantEngagement: {
          engagementRecords,
          summary: engagementRecords[0] || {}
        }
      }));
      
      return { engagementRecords };
    } catch (err) {
      return handleError(err, 'Fetch engagement metrics');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  // ==================== REPORT GENERATION FUNCTIONS - CRITICAL FIX ====================

  /**
   * Generate participant PDF report
   * CRITICAL FIX: Properly handle blob response without unwrapping
   */
  const generateParticipantReportPDF = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.userId || filters.user_id || user?.id,
        start_date: filters.start_date || filters.dateRange?.start,
        end_date: filters.end_date || filters.dateRange?.end,
          meeting_time: filters.meeting_time  
      };
      Object.keys(params).forEach(key => {
        if (params[key] === undefined) {
          delete params[key];
        }
      });
      console.log('📊 useAnalytics: Generating participant PDF report with params:', params);
      
      // CRITICAL FIX: The API already returns the blob directly, not wrapped in response.data
      const pdfBlob = await analyticsAPI.generateParticipantReportPDF(params);
      
      console.log('📊 useAnalytics: PDF blob received:', {
        type: typeof pdfBlob,
        isBlob: pdfBlob instanceof Blob,
        size: pdfBlob?.size,
        blobType: pdfBlob?.type
      });
      
      // CRITICAL: Validate blob before passing to download
      if (!pdfBlob) {
        throw new Error('No PDF data received from API');
      }
      
      if (!(pdfBlob instanceof Blob)) {
        console.error('❌ Invalid blob type received:', pdfBlob);
        throw new Error(`Expected Blob, got ${typeof pdfBlob}`);
      }
      
      if (pdfBlob.size === 0) {
        throw new Error('Received empty PDF file');
      }
      
      // Generate filename
      const filename = `participant_report_${params.user_id}_${new Date().toISOString().split('T')[0]}.pdf`;
      
      console.log('📊 useAnalytics: Downloading PDF as:', filename);
      
      // CRITICAL FIX: Pass the blob directly, not response.data
      analyticsAPI.downloadPDFReport(pdfBlob, filename);
      
      console.log('✅ useAnalytics: PDF download completed successfully');
      return { success: true };
      
    } catch (err) {
      console.error('❌ useAnalytics: Generate participant PDF error:', err);
      return handleError(err, 'Generate participant PDF report');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  /**
   * Generate host PDF report
   * CRITICAL FIX: Properly handle blob response without unwrapping
   */
  const generateHostReportPDF = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        host_id: filters.host_id || filters.userId || filters.user_id || user?.id,
        start_date: filters.start_date || filters.dateRange?.start,
        end_date: filters.end_date || filters.dateRange?.end,
        meeting_time: filters.meeting_time 
      };
       Object.keys(params).forEach(key => {
        if (params[key] === undefined) {
          delete params[key];
        }
      });
      console.log('📊 useAnalytics: Generating host PDF report with params:', params);
      
      // CRITICAL FIX: The API already returns the blob directly, not wrapped in response.data
      const pdfBlob = await analyticsAPI.generateHostReportPDF(params);
      
      console.log('📊 useAnalytics: PDF blob received:', {
        type: typeof pdfBlob,
        isBlob: pdfBlob instanceof Blob,
        size: pdfBlob?.size,
        blobType: pdfBlob?.type
      });
      
      // CRITICAL: Validate blob before passing to download
      if (!pdfBlob) {
        throw new Error('No PDF data received from API');
      }
      
      if (!(pdfBlob instanceof Blob)) {
        console.error('❌ Invalid blob type received:', pdfBlob);
        throw new Error(`Expected Blob, got ${typeof pdfBlob}`);
      }
      
      if (pdfBlob.size === 0) {
        throw new Error('Received empty PDF file');
      }
      
      // Generate filename
      const filename = `host_report_${params.host_id}_${new Date().toISOString().split('T')[0]}.pdf`;
      
      console.log('📊 useAnalytics: Downloading PDF as:', filename);
      
      // CRITICAL FIX: Pass the blob directly, not response.data
      analyticsAPI.downloadPDFReport(pdfBlob, filename);
      
      console.log('✅ useAnalytics: PDF download completed successfully');
      return { success: true };
      
    } catch (err) {
      console.error('❌ useAnalytics: Generate host PDF error:', err);
      return handleError(err, 'Generate host PDF report');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const getParticipantReportPreview = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        user_id: filters.userId || filters.user_id || user?.id,
        start_date: filters.start_date || filters.dateRange?.start,
        end_date: filters.end_date || filters.dateRange?.end
      };
      
      console.log('Getting participant report preview with params:', params);
      const response = await analyticsAPI.getParticipantReportPreview(params);
      
      return response.data?.data || response.data;
    } catch (err) {
      return handleError(err, 'Get participant report preview');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  const getHostReportPreview = useCallback(async (filters = {}) => {
    try {
      setLoading(true);
      setError(null);
      
      const params = {
        host_id: filters.host_id || filters.userId || filters.user_id || user?.id,
        start_date: filters.start_date || filters.dateRange?.start,
        end_date: filters.end_date || filters.dateRange?.end
      };
      
      console.log('Getting host report preview with params:', params);
      const response = await analyticsAPI.getHostReportPreview(params);
      
      return response.data?.data || response.data;
    } catch (err) {
      return handleError(err, 'Get host report preview');
    } finally {
      setLoading(false);
    }
  }, [user, handleError]);

  /**
   * Export report - wrapper function
   * CRITICAL FIX: Properly handle blob responses
   */
  const exportReport = useCallback(async (format = 'pdf', reportType = 'host', filters = {}) => {
    try {
      console.log('📊 useAnalytics: Starting export report:', { format, reportType, filters });
      
      if (format !== 'pdf') {
        throw new Error('Only PDF format is currently supported');
      }

      if (!user?.id) {
        throw new Error('User ID is required to export report');
      }

      // Prepare params
      const params = {
        user_id: user.id,
        start_date: filters.dateRange?.start || filters.start_date,
        end_date: filters.dateRange?.end || filters.end_date,
        timeframe: filters.timeframe,
        meeting_type: filters.meetingType
      };

      console.log('📊 useAnalytics: Export params:', params);

      // Call appropriate export function (they handle everything internally now)
      if (reportType === 'host') {
        await generateHostReportPDF({
          host_id: user.id,
          ...params
        });
      } else if (reportType === 'participant') {
        await generateParticipantReportPDF(params);
      } else {
        throw new Error(`Unknown report type: ${reportType}`);
      }

      console.log('✅ useAnalytics: Export completed successfully');
      return { success: true };

    } catch (error) {
      console.error('❌ useAnalytics: Export report error:', error);
      throw error;
    }
  }, [user, generateHostReportPDF, generateParticipantReportPDF]);

  // ==================== COMMON FUNCTIONS ====================

//   const getUserStats = useCallback(async () => {
//     try {
//       setLoading(true);
//       setError(null);
      
//       const params = {
//         user_id: user?.id,
//         userId: user?.id,
//         analytics_type: 'all',
//         timeframe: '1year'
//       };
      
//       console.log('Fetching user stats with params:', params);
      
//       const response = await analyticsAPI.getComprehensiveAnalytics(params);
//       const data = response.data?.data || response.data;
//        console.log('📊 RAW BACKEND DATA:', {
//       participantData: data.participant_summary?.[0],
//       hostData: data.host_analytics?.[0],
//       overallSummary: data.overall_summary,
//       fullResponse: data
//     });
//       const participantData = data.participant_summary?.[0] || {};
//       const hostData = data.host_analytics?.[0] || {};
//       const overallSummary = data.overall_summary || {};
//        console.log('📊 EXTRACTED VALUES:', {
//       totalMeetings: participantData.meeting_participation?.total_meetings_attended,
//       totalMinutesRaw: participantData.meeting_participation?.total_participation_time_minutes,
//       averageAttendanceRaw: participantData.meeting_participation?.avg_participant_attendance,
//       totalHostedMeetings: hostData.meeting_counts?.total_meetings_hosted
//     });
//       const userStats = {
//   totalMeetings: participantData.meeting_participation?.total_meetings_attended || 
//                  overallSummary.total_meetings || 0,
//   totalHostedMeetings: hostData.meeting_counts?.total_meetings_hosted || 0,
//   // CRITICAL FIX: Round the minutes to remove decimals
//   totalMinutes: Math.round(participantData.meeting_participation?.total_participation_time_minutes || 0),
//   // CRITICAL FIX: Round attendance percentage to 2 decimal places
//   averageAttendance: Math.round((participantData.meeting_participation?.avg_participant_attendance || 0) * 100) / 100,
//   upcomingCount: 0,
//   role: hostData.meeting_counts?.total_meetings_hosted > 0 ? 'host' : 'participant'
// };
//       console.log('📊 PROCESSED USER STATS:', userStats);
//       return userStats;
//     } catch (err) {
//       console.error('Error fetching user statistics:', err);
//       return {
//         totalMeetings: 0,
//         totalHostedMeetings: 0,
//         totalMinutes: 0,
//         averageAttendance: 0,
//         upcomingCount: 0,
//         role: 'participant'
//       };
//     } finally {
//       setLoading(false);
//     }
//   }, [user]);

const getUserStats = useCallback(async () => {
  try {
    setLoading(true);
    setError(null);
    
    if (!user?.id) {
      console.warn('⚠️ getUserStats: No user ID available');
      return {
        totalMeetings: 0,
        totalHostedMeetings: 0,
        totalMinutes: 0,
        averageAttendance: 0,
        upcomingCount: 0,
        role: 'participant'
      };
    }
    
    const params = {
      user_id: user.id,
      userId: user.id,
      analytics_type: 'all',
      timeframe: '1year'
    };
    
    console.log('📊 getUserStats: Fetching with params:', params);
    
    const response = await analyticsAPI.getComprehensiveAnalytics(params);
    
    // Handle nested response structure
    const data = response?.data?.data || response?.data || response || {};
    
    console.log('📊 getUserStats: RAW BACKEND DATA:', {
      participantSummary: data.participant_summary,
      hostAnalytics: data.host_analytics,
      overallSummary: data.overall_summary,
      participantDetails: data.participant_details,
      meetingAnalytics: data.meeting_analytics
    });
    
    // Extract participant data with multiple fallbacks
    const participantSummary = data.participant_summary?.[0] || {};
    const hostData = data.host_analytics?.[0] || {};
    const overallSummary = data.overall_summary || {};
    const participantDetails = data.participant_details || [];
    const meetingAnalytics = data.meeting_analytics || [];
    
    // Get participant meeting participation data
    const meetingParticipation = participantSummary.meeting_participation || {};
    
    // ==================== CALCULATE TOTAL MEETINGS ====================
    let totalMeetingsAttended = 0;
    
    // Try multiple sources for total meetings
    if (meetingParticipation.total_meetings_attended) {
      totalMeetingsAttended = parseInt(meetingParticipation.total_meetings_attended) || 0;
    } else if (participantDetails.length > 0) {
      // Count unique meetings from participant_details
      const uniqueMeetings = new Set(participantDetails.map(p => p.meeting_id));
      totalMeetingsAttended = uniqueMeetings.size;
    } else if (meetingAnalytics.length > 0) {
      totalMeetingsAttended = meetingAnalytics.length;
    } else if (overallSummary.total_meetings) {
      totalMeetingsAttended = parseInt(overallSummary.total_meetings) || 0;
    }
    
    // ==================== CALCULATE TOTAL DURATION ====================
    let totalDurationMinutes = 0;
    
    if (meetingParticipation.total_participation_time_minutes) {
      totalDurationMinutes = parseFloat(meetingParticipation.total_participation_time_minutes) || 0;
    } else if (participantDetails.length > 0) {
      // Sum up all durations from participant_details
      totalDurationMinutes = participantDetails.reduce((sum, p) => {
        const duration = parseFloat(
          p.duration_analysis?.total_duration_minutes || 
          p.total_duration_minutes || 
          p.duration_minutes || 
          0
        );
        return sum + duration;
      }, 0);
    } else if (meetingAnalytics.length > 0) {
      // Sum from meeting_analytics
      totalDurationMinutes = meetingAnalytics.reduce((sum, m) => {
        const duration = parseFloat(
          m.duration_analytics?.total_duration_minutes ||
          m.duration_analytics?.average_duration_minutes ||
          0
        );
        return sum + duration;
      }, 0);
    }
    
    // ==================== CALCULATE AVERAGE ATTENDANCE ====================
    let avgAttendance = 0;
    
    if (meetingParticipation.avg_overall_attendance) {
      avgAttendance = parseFloat(meetingParticipation.avg_overall_attendance) || 0;
    } else if (meetingParticipation.avg_participant_attendance) {
      avgAttendance = parseFloat(meetingParticipation.avg_participant_attendance) || 0;
    } else if (participantDetails.length > 0) {
      // Calculate average from participant_details
      const attendances = participantDetails
        .map(p => parseFloat(
          p.participant_attendance_data?.overall_attendance ||
          p.participant_attendance_metrics?.overall_attendance ||
          p.attendance_percentage ||
          0
        ))
        .filter(a => a > 0);
      
      if (attendances.length > 0) {
        avgAttendance = attendances.reduce((a, b) => a + b, 0) / attendances.length;
      }
    } else if (overallSummary.avg_overall_attendance) {
      avgAttendance = parseFloat(overallSummary.avg_overall_attendance) || 0;
    } else if (overallSummary.avg_participant_attendance) {
      avgAttendance = parseFloat(overallSummary.avg_participant_attendance) || 0;
    }
    
    // ==================== CALCULATE HOSTED MEETINGS ====================
    let totalHostedMeetings = 0;
    
    if (hostData.meeting_counts?.total_meetings_hosted) {
      totalHostedMeetings = parseInt(hostData.meeting_counts.total_meetings_hosted) || 0;
    } else if (overallSummary.total_hosted_meetings) {
      totalHostedMeetings = parseInt(overallSummary.total_hosted_meetings) || 0;
    }
    
    // ==================== BUILD FINAL STATS ====================
    const userStats = {
      totalMeetings: totalMeetingsAttended,
      totalHostedMeetings: totalHostedMeetings,
      totalMinutes: Math.round(totalDurationMinutes),
      averageAttendance: Math.round(avgAttendance * 100) / 100,
      upcomingCount: 0,
      role: totalHostedMeetings > 0 ? 'host' : 'participant'
    };
    
    console.log('📊 getUserStats: FINAL PROCESSED STATS:', userStats);
    console.log('📊 getUserStats: Calculation sources:', {
      totalMeetingsSource: meetingParticipation.total_meetings_attended ? 'meetingParticipation' : 
                          participantDetails.length > 0 ? 'participantDetails' : 
                          meetingAnalytics.length > 0 ? 'meetingAnalytics' : 'overallSummary',
      totalDurationSource: meetingParticipation.total_participation_time_minutes ? 'meetingParticipation' :
                          participantDetails.length > 0 ? 'participantDetails' : 'meetingAnalytics',
      avgAttendanceSource: meetingParticipation.avg_overall_attendance ? 'avg_overall_attendance' :
                          meetingParticipation.avg_participant_attendance ? 'avg_participant_attendance' :
                          participantDetails.length > 0 ? 'participantDetails' : 'overallSummary'
    });
    
    return userStats;
    
  } catch (err) {
    console.error('❌ getUserStats: Error fetching user statistics:', err);
    console.error('❌ Error details:', {
      message: err.message,
      response: err.response?.data
    });
    
    return {
      totalMeetings: 0,
      totalHostedMeetings: 0,
      totalMinutes: 0,
      averageAttendance: 0,
      upcomingCount: 0,
      role: 'participant'
    };
  } finally {
    setLoading(false);
  }
}, [user]);

const getUpcomingMeetingsCount = async (userId) => {
  try {
    const response = await api.get('/api/meetings/user-schedule-meetings', {
      params: { user_id: userId }
    });
    
    const now = new Date();
    const upcomingMeetings = (response.meetings || []).filter(meeting => {
      const startTime = new Date(meeting.start_time || meeting.Started_At);
      return startTime > now && meeting.Status === 'scheduled';
    });
    
    return upcomingMeetings.length;
  } catch (error) {
    console.error('❌ Failed to get upcoming meetings count:', error);
    return 0;
  }
};
  const exportAnalyticsData = useCallback(async (exportConfig) => {
    try {
      setLoading(true);
      setError(null);
      
      const isHost = exportConfig.userRole === 'host';
      
      return await exportReport('pdf', isHost ? 'host' : 'participant', exportConfig.filters);
    } catch (err) {
      return handleError(err, 'Export analytics data');
    } finally {
      setLoading(false);
    }
  }, [exportReport, handleError]);

  // ==================== LEGACY FUNCTIONS ====================

  const fetchMeetingReports = useCallback(async (filters = {}) => {
    return await fetchHostMeetingReports(filters);
  }, [fetchHostMeetingReports]);

  const fetchEngagementMetrics = useCallback(async (meetingId) => {
    if (meetingId === 'overall') {
      return await fetchHostEngagementDistribution();
    }
    return await fetchParticipantEngagement({ meetingId });
  }, [fetchHostEngagementDistribution, fetchParticipantEngagement]);

  const getMeetingAnalytics = useCallback(async (meetingId) => {
    try {
      setLoading(true);
      const response = await fetchParticipantEngagement({ meetingId });
      return { success: true, analytics: response };
    } catch (err) {
      console.error('Get meeting analytics error:', err);
      setError('Failed to load meeting analytics');
      return { success: false, message: err.message || 'Failed to load analytics' };
    } finally {
      setLoading(false);
    }
  }, [fetchParticipantEngagement]);

  const getAttendanceReport = useCallback(async (dateRange) => {
    try {
      setLoading(true);
      const response = await fetchParticipantAttendance({ dateRange });
      return { success: true, report: response.attendanceRecords || [] };
    } catch (err) {
      console.error('Get attendance report error:', err);
      setError('Failed to load attendance report');
      return { success: false, message: err.message || 'Failed to load report' };
    } finally {
      setLoading(false);
    }
  }, [fetchParticipantAttendance]);

  return {
    // Data
    analyticsData,
    loading,
    error,
    
    // Comprehensive Analytics
    fetchComprehensiveAnalytics,
    
    // Host Analytics Functions
    fetchHostOverview,
    fetchHostMeetingReports,
    fetchHostEngagementDistribution,
    fetchHostTrends,
    
    // Participant Analytics Functions
    fetchParticipantPersonalReport,
    fetchParticipantAttendance,
    fetchParticipantEngagement,
    
    // Report Generation Functions
    generateParticipantReportPDF,
    generateHostReportPDF,
    getParticipantReportPreview,
    getHostReportPreview,
    exportReport,
    
    // Common Functions
    getUserStats,
    exportAnalyticsData,
    
    // Legacy Functions
    fetchMeetingReports,
    fetchEngagementMetrics,
    getMeetingAnalytics,
    getAttendanceReport,
    
    // Utility Functions
    clearError
  };
};