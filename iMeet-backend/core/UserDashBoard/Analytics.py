from django.db import connection, transaction
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
import json
import logging
from reportlab.platypus import Preformatted
from reportlab.platypus import KeepTogether, Paragraph
from django.http import HttpResponse, JsonResponse
from datetime import datetime, timedelta
import pytz
from django.utils import timezone
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.platypus.frames import Frame
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from io import BytesIO
import os
import traceback
from textwrap import fill
from reportlab.platypus import Paragraph
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import HRFlowable
# Configure logging
logging.basicConfig(filename='analytics_debug.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

# Global status codes
SUCCESS_STATUS = 200
BAD_REQUEST_STATUS = 400
UNAUTHORIZED_STATUS = 401
FORBIDDEN_STATUS = 403
NOT_FOUND_STATUS = 404
SERVER_ERROR_STATUS = 500

class ReportGenerator:
    """Helper class for generating PDF reports"""
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.custom_styles = self._create_custom_styles()
    
    def _create_custom_styles(self):
        """Create custom styles for the reports"""
        styles = {}
        
        # Title style
        styles['ReportTitle'] = ParagraphStyle(
            'ReportTitle',
            parent=self.styles['Title'],
            fontSize=18,
            spaceAfter=20,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        )
        
        # Header style
        styles['SectionHeader'] = ParagraphStyle(
            'SectionHeader',
            parent=self.styles['Heading1'],
            fontSize=14,
            spaceAfter=12,
            textColor=colors.darkblue,
            borderWidth=1,
            borderColor=colors.darkblue,
            borderPadding=5
        )
        
        # Subheader style
        styles['SubHeader'] = ParagraphStyle(
            'SubHeader',
            parent=self.styles['Heading2'],
            fontSize=12,
            spaceAfter=8,
            textColor=colors.black
        )
        
        # Summary style
        styles['Summary'] = ParagraphStyle(
            'Summary',
            parent=self.styles['Normal'],
            fontSize=10,
            spaceAfter=6,
            backColor=colors.lightgrey,
            borderWidth=1,
            borderColor=colors.grey,
            borderPadding=5
        )
        
        # Meeting detail style
        styles['MeetingDetail'] = ParagraphStyle(
            'MeetingDetail',
            parent=self.styles['Normal'],
            fontSize=9,
            spaceAfter=4
        )
        
        return styles

    def create_header_footer(self, canvas, doc, title):
        """Properly aligned header and footer for all pages"""
        canvas.saveState()
        width, height = letter

        # Header (left-aligned title, right-aligned timestamp)
        canvas.setFont('Helvetica-Bold', 14)
        canvas.drawString(55, height - 50, title)

        canvas.setFont('Helvetica', 9)
        canvas.drawRightString(width - 55, height - 50, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

        # Header line
        canvas.setLineWidth(0.5)
        canvas.line(50, height - 60, width - 50, height - 60)

        # Footer (left-aligned app name, right-aligned page number)
        canvas.setFont('Helvetica', 8)
        canvas.drawString(55, 55, "Meeting Analytics System")
        canvas.drawRightString(width - 55, 55, f"Page {doc.page}")

        # Footer line
        canvas.line(50, 65, width - 50, 65)
        canvas.restoreState()

@require_http_methods(["GET"])
@csrf_exempt
def get_comprehensive_meeting_analytics(request):
    """
    Comprehensive analytics showing:
    - How long each participant and host stayed in each meeting (duration analysis)
    - Participant attendance data from tbl_Participants (Participant_Attendance, Overall_Attendance)
    - Attendance monitoring data from tbl_Attendance_Sessions (popup_count, detections, penalties, etc.)
    - How many meetings each participant attended
    - How many meetings each host conducted/created/completed
    - Complete participant analysis using actual table columns
    - Available meeting times for date filtering (separated by role - participant vs host)
    - Uses p.Role='participant' for participant view, m.Host_ID for host view
    """
    try:
        # Accept multiple parameter names for flexibility
        user_id = request.GET.get('user_id') or request.GET.get('userId')
        meeting_id = request.GET.get('meeting_id') or request.GET.get('meetingId')
        timeframe = request.GET.get('timeframe', '30days')
        meeting_type = request.GET.get('meetingType') or request.GET.get('meeting_type', 'all')
        analytics_type = request.GET.get('analytics_type', 'all')  # all, participant, host, meeting
        page = int(request.GET.get('page', 1))
        limit = int(request.GET.get('limit', 100))
        
        # Handle date range parameters
        date_range_start = (request.GET.get('dateRange[start]') or 
                           request.GET.get('start_date') or
                           request.GET.get('startDate'))
        date_range_end = (request.GET.get('dateRange[end]') or 
                         request.GET.get('end_date') or
                         request.GET.get('endDate'))

        logging.debug(f"Comprehensive analytics request - user_id: {user_id}, meeting_id: {meeting_id}, analytics_type: {analytics_type}")

        # Calculate date range with FIXED inclusive boundaries
        ist_timezone = pytz.timezone('Asia/Kolkata')
        
        if not date_range_end:
            end_date = timezone.now().astimezone(ist_timezone)
        else:
            end_date = datetime.strptime(date_range_end, '%Y-%m-%d').replace(tzinfo=ist_timezone)
            # Set to end of day (23:59:59)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
        if not date_range_start:
            if timeframe == '7days':
                start_date = end_date - timedelta(days=7)
            elif timeframe == '30days':
                start_date = end_date - timedelta(days=30)
            elif timeframe == '90days':
                start_date = end_date - timedelta(days=90)
            elif timeframe == '1year':
                start_date = end_date - timedelta(days=365)
            else:
                start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(date_range_start, '%Y-%m-%d').replace(tzinfo=ist_timezone)
            # Set to start of day (00:00:00)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        offset = (page - 1) * limit

        with connection.cursor() as cursor:
            
            # ==================== GET AVAILABLE MEETING TIMES ====================
            available_meeting_times = []
            
            if analytics_type in ['all', 'participant'] and user_id:
                # Get meeting times where user has Role='participant' in tbl_Participants
                # Note: Removed Started_At IS NOT NULL check to include all meetings
                logging.info(f"📅 Fetching participant meetings for user {user_id} (Role='participant')")
                
                cursor.execute("""
                    SELECT DISTINCT
                        m.ID as meeting_id,
                        m.Meeting_Name,
                        m.Meeting_Type,
                        COALESCE(
                            m.Started_At,
                            sm.start_time,
                            cm.startTime,
                            m.Created_At
                        ) as meeting_time,
                        p.Total_Duration_Minutes,
                        DATE(COALESCE(
                            m.Started_At,
                            sm.start_time,
                            cm.startTime,
                            m.Created_At
                        )) as meeting_date
                    FROM tbl_Participants p
                    JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                    LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    WHERE p.User_ID = %s
                    AND p.Role = 'participant'
                    AND COALESCE(
                        m.Started_At,
                        sm.start_time,
                        cm.startTime,
                        m.Created_At
                    ) BETWEEN %s AND %s
                    ORDER BY meeting_time DESC
                """, [user_id, start_date, end_date])
                
                participant_times = cursor.fetchall()
                logging.info(f"✅ Found {len(participant_times)} participant meetings (Role='participant')")
                
                for row in participant_times:
                    meeting_time = row[3]
                    if meeting_time:
                        type_display = {
                            'InstantMeeting': 'Instant',
                            'ScheduleMeeting': 'Scheduled',
                            'CalendarMeeting': 'Calendar'
                        }.get(row[2], row[2])
                        duration = f"{int(row[4])}m" if row[4] else "N/A"
                        
                        available_meeting_times.append({
                            'meeting_id': row[0],
                            'meeting_name': row[1],
                            'meeting_type': row[2],
                            'date': row[5].isoformat(),
                            'time': meeting_time.strftime('%H:%M'),
                            'display_time': meeting_time.strftime('%I:%M %p'),
                            'datetime_for_filter': meeting_time.strftime('%Y-%m-%d %H:%M'),
                            'label': f"{meeting_time.strftime('%I:%M %p')} - {row[1]} ({type_display}) - {duration}",
                            'role': 'participant'
                        })
            
            elif analytics_type in ['all', 'host'] and user_id:
                # Get meeting times where user was HOST
                logging.info(f"📅 Fetching host meetings for user {user_id} (Host_ID={user_id})")
                
                cursor.execute("""
                    SELECT DISTINCT
                        m.ID as meeting_id,
                        m.Meeting_Name,
                        m.Meeting_Type,
                        COALESCE(
                            m.Started_At,
                            sm.start_time,
                            cm.startTime,
                            m.Created_At
                        ) as meeting_time,
                        COUNT(DISTINCT p.User_ID) as participant_count,
                        DATE(COALESCE(
                            m.Started_At,
                            sm.start_time,
                            cm.startTime,
                            m.Created_At
                        )) as meeting_date
                    FROM tbl_Meetings m
                    LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                    WHERE m.Host_ID = %s
                    AND m.Started_At IS NOT NULL
                    AND COALESCE(
                        m.Started_At,
                        sm.start_time,
                        cm.startTime,
                        m.Created_At
                    ) BETWEEN %s AND %s
                    GROUP BY m.ID, m.Meeting_Name, m.Meeting_Type, meeting_time, meeting_date
                    ORDER BY meeting_time DESC
                """, [user_id, start_date, end_date])
                
                host_times = cursor.fetchall()
                logging.info(f"✅ Found {len(host_times)} host meetings (Host_ID={user_id})")
                
                for row in host_times:
                    meeting_time = row[3]
                    if meeting_time:
                        type_display = {
                            'InstantMeeting': 'Instant',
                            'ScheduleMeeting': 'Scheduled',
                            'CalendarMeeting': 'Calendar'
                        }.get(row[2], row[2])
                        
                        available_meeting_times.append({
                            'meeting_id': row[0],
                            'meeting_name': row[1],
                            'meeting_type': row[2],
                            'date': row[5].isoformat(),
                            'time': meeting_time.strftime('%H:%M'),
                            'display_time': meeting_time.strftime('%I:%M %p'),
                            'datetime_for_filter': meeting_time.strftime('%Y-%m-%d %H:%M'),
                            'label': f"{meeting_time.strftime('%I:%M %p')} - {row[1]} ({type_display}) - {row[4]} participants",
                            'role': 'host'
                        })
            
            # ==================== 1. PARTICIPANT DURATION AND ATTENDANCE ANALYTICS ====================
            if analytics_type in ['all', 'participant']:
                participant_analytics_query = """
                    SELECT 
                        -- tbl_Participants columns (all actual columns)
                        p.ID as participant_id,
                        p.Meeting_ID,
                        p.User_ID,
                        p.Full_Name,
                        p.Role,
                        p.Meeting_Type,
                        p.Join_Times,
                        p.Leave_Times,
                        p.Total_Duration_Minutes,
                        p.Total_Sessions,
                        p.End_Meeting_Time,
                        p.Is_Currently_Active,
                        p.Attendance_Percentagebasedon_host,
                        p.Participant_Attendance,
                        p.Overall_Attendance,
                        
                        -- tbl_Attendance_Sessions columns (requested columns)
                        ats.popup_count,
                        ats.detection_counts,
                        ats.violation_start_times as violation_start_time,
                        ats.total_detections,
                        ats.attendance_penalty,
                        ats.break_used,
                        ats.total_break_time_used,
                        ats.engagement_score,
                        ats.attendance_percentage as session_attendance_percentage,
                        
                        -- Additional attendance session details
                        ats.session_active,
                        ats.break_count,
                        ats.focus_score,
                        ats.violation_severity_score,
                        ats.active_participation_time,
                        ats.total_session_time,
                        
                        -- Meeting Info from tbl_Meetings
                        m.Meeting_Name,
                        m.Status as meeting_status,
                        m.Created_At as meeting_created_at,
                        m.Started_At,
                        m.Ended_At,
                        m.Host_ID,
                        m.Meeting_Link,
                        m.Is_Recording_Enabled,
                        m.Waiting_Room_Enabled
                        
                    FROM tbl_Participants p
                    LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                    LEFT JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                    LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    WHERE 1=1
                """
                
                params = []
                if user_id:
                    participant_analytics_query += " AND p.User_ID = %s"
                    params.append(user_id)
                if meeting_id:
                    participant_analytics_query += " AND p.Meeting_ID = %s"
                    params.append(meeting_id)
                if meeting_type != 'all':
                    participant_analytics_query += " AND p.Meeting_Type = %s"
                    params.append(meeting_type)
                
                participant_analytics_query += """ AND COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) BETWEEN %s AND %s"""
                params.extend([start_date, end_date])
                
                participant_analytics_query += " ORDER BY m.Created_At DESC LIMIT %s OFFSET %s"
                params.extend([limit, offset])
                
                cursor.execute(participant_analytics_query, params)
                participant_data = []
                for row in cursor.fetchall():
                    participant_data.append({
                        # Participant basic info
                        "participant_id": row[0],
                        "meeting_id": row[1],
                        "user_id": row[2],
                        "full_name": row[3],
                        "role": row[4],
                        "meeting_type": row[5],
                        
                        # Duration Analysis (How long they stayed)
                        "duration_analysis": {
                            "join_times": json.loads(row[6]) if row[6] else [],
                            "leave_times": json.loads(row[7]) if row[7] else [],
                            "total_duration_minutes": float(row[8] or 0),
                            "total_sessions": int(row[9] or 0),
                            "end_meeting_time": row[10].isoformat() if row[10] else None,
                            "is_currently_active": bool(row[11])
                        },
                        
                        # Participant Attendance Data (from tbl_Participants)
                        "participant_attendance_data": {
                            "attendance_percentage_based_on_host": float(row[12] or 0),
                            "participant_attendance": float(row[13] or 0),
                            "overall_attendance": float(row[14] or 0)
                        },
                        
                        # Attendance Session Data (requested columns)
                        "attendance_session": {
                            "popup_count": int(row[15] or 0),
                            "detection_counts": row[16],
                            "violation_start_time": row[17],
                            "total_detections": int(row[18] or 0),
                            "attendance_penalty": float(row[19] or 0),
                            "break_used": bool(row[20]),
                            "total_break_time_used": int(row[21] or 0),
                            "engagement_score": int(row[22] or 0),
                            "attendance_percentage": float(row[23] or 0),
                            
                            # Additional session details
                            "session_active": bool(row[24]),
                            "break_count": int(row[25] or 0),
                            "focus_score": float(row[26] or 0),
                            "violation_severity_score": float(row[27] or 0),
                            "active_participation_time": int(row[28] or 0),
                            "total_session_time": int(row[29] or 0)
                        },
                        
                        # Meeting Info
                        "meeting_info": {
                            "meeting_name": row[30],
                            "status": row[31],
                            "created_at": row[32].isoformat() if row[32] else None,
                            "started_at": row[33].isoformat() if row[33] else None,
                            "ended_at": row[34].isoformat() if row[34] else None,
                            "host_id": row[35],
                            "meeting_link": row[36],
                            "is_recording_enabled": bool(row[37]),
                            "waiting_room_enabled": bool(row[38])
                        }
                    })

            # ==================== 2. HOST ANALYTICS ====================
            if analytics_type in ['all', 'host']:
                host_analytics_query = """
                    SELECT 
                        m.Host_ID,
                        m.Meeting_Type,
                        COUNT(DISTINCT m.ID) as total_meetings_hosted,
                        COUNT(DISTINCT CASE WHEN m.Status = 'active' THEN m.ID END) as active_meetings,
                        COUNT(DISTINCT CASE WHEN m.Status = 'ended' THEN m.ID END) as ended_meetings,
                        COUNT(DISTINCT CASE WHEN m.Status = 'scheduled' THEN m.ID END) as scheduled_meetings,
                        COUNT(DISTINCT p.User_ID) as total_unique_participants,
                        AVG(p.Total_Duration_Minutes) as avg_meeting_duration_minutes,
                        AVG(p.Participant_Attendance) as avg_participant_attendance,
                        AVG(p.Overall_Attendance) as avg_overall_attendance,
                        SUM(p.Total_Duration_Minutes) as total_hosting_time_minutes,
                        MIN(m.Created_At) as first_meeting_created,
                        MAX(m.Created_At) as last_meeting_created,
                        
                        -- Attendance monitoring averages
                        AVG(ats.popup_count) as avg_popup_count,
                        AVG(ats.total_detections) as avg_total_detections,
                        AVG(ats.attendance_penalty) as avg_attendance_penalty,
                        AVG(ats.engagement_score) as avg_engagement_score,
                        COUNT(CASE WHEN ats.break_used = 1 THEN 1 END) as total_breaks_used
                        
                    FROM tbl_Meetings m
                    LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                    LEFT JOIN tbl_Attendance_Sessions ats ON m.ID = ats.Meeting_ID
                    WHERE 1=1
                """
                
                params = []
                if user_id:
                    host_analytics_query += " AND m.Host_ID = %s"
                    params.append(user_id)
                if meeting_type != 'all':
                    host_analytics_query += " AND m.Meeting_Type = %s"
                    params.append(meeting_type)
                
                host_analytics_query += """ AND COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) BETWEEN %s AND %s"""
                params.extend([start_date, end_date])
                
                host_analytics_query += " GROUP BY m.Host_ID, m.Meeting_Type ORDER BY total_meetings_hosted DESC"
                
                cursor.execute(host_analytics_query, params)
                host_data = []
                for row in cursor.fetchall():
                    host_data.append({
                        "host_id": row[0],
                        "meeting_type": row[1],
                        "meeting_counts": {
                            "total_meetings_hosted": int(row[2] or 0),
                            "active_meetings": int(row[3] or 0),
                            "ended_meetings": int(row[4] or 0),
                            "scheduled_meetings": int(row[5] or 0),
                            "completion_rate": round((int(row[4] or 0) / int(row[2] or 1) * 100), 2)
                        },
                        "participant_analytics": {
                            "total_unique_participants": int(row[6] or 0),
                            "avg_meeting_duration_minutes": round(float(row[7] or 0), 2),
                            "avg_participant_attendance": round(float(row[8] or 0), 2),
                            "avg_overall_attendance": round(float(row[9] or 0), 2),
                            "total_hosting_time_minutes": round(float(row[10] or 0), 2)
                        },
                        "activity_period": {
                            "first_meeting_created": row[11].isoformat() if row[11] else None,
                            "last_meeting_created": row[12].isoformat() if row[12] else None
                        },
                        "attendance_monitoring": {
                            "avg_popup_count": round(float(row[13] or 0), 2),
                            "avg_total_detections": round(float(row[14] or 0), 2),
                            "avg_attendance_penalty": round(float(row[15] or 0), 2),
                            "avg_engagement_score": round(float(row[16] or 0), 2),
                            "total_breaks_used": int(row[17] or 0)
                        }
                    })

            # ==================== 3. PARTICIPANT SUMMARY ANALYTICS ====================
            if analytics_type in ['all', 'participant']:
                participant_summary_query = """
                    SELECT 
                        p.User_ID,
                        p.Full_Name,
                        COUNT(DISTINCT p.Meeting_ID) as total_meetings_attended,
                        SUM(p.Total_Duration_Minutes) as total_participation_time_minutes,
                        AVG(p.Total_Duration_Minutes) as avg_meeting_duration_minutes,
                        AVG(p.Participant_Attendance) as avg_participant_attendance,
                        AVG(p.Overall_Attendance) as avg_overall_attendance,
                        COUNT(DISTINCT CASE WHEN p.Is_Currently_Active = 1 THEN p.Meeting_ID END) as active_meetings,
                        AVG(p.Total_Sessions) as avg_sessions_per_meeting,
                        p.Meeting_Type,
                        MIN(m.Created_At) as first_meeting_joined,
                        MAX(m.Created_At) as last_meeting_joined,
                        
                        -- Attendance session analytics
                        AVG(ats.popup_count) as avg_popup_count,
                        AVG(ats.total_detections) as avg_total_detections,
                        AVG(ats.attendance_penalty) as avg_attendance_penalty,
                        AVG(ats.total_break_time_used) as avg_break_time_used,
                        AVG(ats.engagement_score) as avg_engagement_score,
                        AVG(ats.focus_score) as avg_focus_score,
                        COUNT(CASE WHEN ats.break_used = 1 THEN 1 END) as total_breaks_taken
                        
                    FROM tbl_Participants p
                    LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                    LEFT JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                    LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    WHERE p.Role = 'participant'
                """
                
                params = []
                if user_id:
                    participant_summary_query += " AND p.User_ID = %s"
                    params.append(user_id)
                if meeting_type != 'all':
                    participant_summary_query += " AND p.Meeting_Type = %s"
                    params.append(meeting_type)
                
                participant_summary_query += """ AND COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) BETWEEN %s AND %s"""
                params.extend([start_date, end_date])
                
                participant_summary_query += " GROUP BY p.User_ID, p.Full_Name, p.Meeting_Type ORDER BY total_meetings_attended DESC"
                
                cursor.execute(participant_summary_query, params)
                participant_summary_data = []
                for row in cursor.fetchall():
                    participant_summary_data.append({
                        "user_id": row[0],
                        "full_name": row[1],
                        "meeting_participation": {
                            "total_meetings_attended": int(row[2] or 0),
                            "total_participation_time_minutes": round(float(row[3] or 0), 2),
                            "avg_meeting_duration_minutes": round(float(row[4] or 0), 2),
                            "avg_participant_attendance": round(float(row[5] or 0), 2),
                            "avg_overall_attendance": round(float(row[6] or 0), 2),
                            "active_meetings": int(row[7] or 0),
                            "avg_sessions_per_meeting": round(float(row[8] or 0), 2)
                        },
                        "meeting_type": row[9],
                        "activity_period": {
                            "first_meeting_joined": row[10].isoformat() if row[10] else None,
                            "last_meeting_joined": row[11].isoformat() if row[11] else None
                        },
                        "attendance_analytics": {
                            "avg_popup_count": round(float(row[12] or 0), 2),
                            "avg_total_detections": round(float(row[13] or 0), 2),
                            "avg_attendance_penalty": round(float(row[14] or 0), 2),
                            "avg_break_time_used": round(float(row[15] or 0), 2),
                            "avg_engagement_score": round(float(row[16] or 0), 2),
                            "avg_focus_score": round(float(row[17] or 0), 2),
                            "total_breaks_taken": int(row[18] or 0)
                        }
                    })

            # ==================== 4. MEETING ANALYTICS ====================
            if analytics_type in ['all', 'meeting']:
                meeting_analytics_query = """
                    SELECT 
                        m.ID as meeting_id,
                        m.Meeting_Name,
                        m.Meeting_Type,
                        m.Host_ID,
                        m.Status,
                        m.Created_At,
                        m.Started_At,
                        m.Ended_At,
                        m.Meeting_Link,
                        m.Is_Recording_Enabled,
                        m.Waiting_Room_Enabled,
                        
                        -- Participant statistics
                        COUNT(DISTINCT p.User_ID) as total_participants,
                        COUNT(DISTINCT CASE WHEN p.Is_Currently_Active = 1 THEN p.User_ID END) as currently_active_participants,
                        AVG(p.Total_Duration_Minutes) as avg_participant_duration_minutes,
                        AVG(p.Participant_Attendance) as avg_participant_attendance,
                        SUM(p.Total_Duration_Minutes) as total_meeting_duration_minutes,
                        MAX(p.Total_Duration_Minutes) as longest_participant_duration,
                        MIN(p.Total_Duration_Minutes) as shortest_participant_duration,
                        
                        -- Attendance monitoring for meeting
                        AVG(ats.popup_count) as avg_popup_count,
                        AVG(ats.total_detections) as avg_total_detections,
                        AVG(ats.attendance_penalty) as avg_attendance_penalty,
                        AVG(ats.engagement_score) as avg_engagement_score,
                        COUNT(CASE WHEN ats.break_used = 1 THEN 1 END) as total_breaks_in_meeting
                        
                    FROM tbl_Meetings m
                    LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                    LEFT JOIN tbl_Attendance_Sessions ats ON m.ID = ats.Meeting_ID
                    WHERE 1=1
                """
                
                params = []
                if meeting_id:
                    meeting_analytics_query += " AND m.ID = %s"
                    params.append(meeting_id)
                if user_id:
                    meeting_analytics_query += " AND m.Host_ID = %s"
                    params.append(user_id)
                if meeting_type != 'all':
                    meeting_analytics_query += " AND m.Meeting_Type = %s"
                    params.append(meeting_type)
                
                meeting_analytics_query += """ AND COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) BETWEEN %s AND %s"""
                params.extend([start_date, end_date])
                
                meeting_analytics_query += " GROUP BY m.ID ORDER BY m.Created_At DESC"
                
                cursor.execute(meeting_analytics_query, params)
                meeting_data = []
                for row in cursor.fetchall():
                    meeting_data.append({
                        "meeting_id": row[0],
                        "meeting_name": row[1],
                        "meeting_type": row[2],
                        "host_id": row[3],
                        "status": row[4],
                        "created_at": row[5].isoformat() if row[5] else None,
                        "started_at": row[6].isoformat() if row[6] else None,
                        "ended_at": row[7].isoformat() if row[7] else None,
                        "meeting_link": row[8],
                        "is_recording_enabled": bool(row[9]),
                        "waiting_room_enabled": bool(row[10]),
                        
                        "participant_analytics": {
                            "total_participants": int(row[11] or 0),
                            "currently_active_participants": int(row[12] or 0),
                            "avg_participant_duration_minutes": round(float(row[13] or 0), 2),
                            "avg_participant_attendance": round(float(row[14] or 0), 2),
                            "total_meeting_duration_minutes": round(float(row[15] or 0), 2),
                            "longest_participant_duration_minutes": round(float(row[16] or 0), 2),
                            "shortest_participant_duration_minutes": round(float(row[17] or 0), 2)
                        },
                        
                        "attendance_analytics": {
                            "avg_popup_count": round(float(row[18] or 0), 2),
                            "avg_total_detections": round(float(row[19] or 0), 2),
                            "avg_attendance_penalty": round(float(row[20] or 0), 2),
                            "avg_engagement_score": round(float(row[21] or 0), 2),
                            "total_breaks_in_meeting": int(row[22] or 0)
                        }
                    })

            # ==================== 5. OVERALL SUMMARY STATISTICS ====================
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT m.ID) as total_meetings,
                    COUNT(DISTINCT m.Host_ID) as total_hosts,
                    COUNT(DISTINCT p.User_ID) as total_participants,
                    AVG(p.Total_Duration_Minutes) as avg_duration_minutes,
                    AVG(p.Participant_Attendance) as avg_participant_attendance,
                    AVG(p.Overall_Attendance) as avg_overall_attendance,
                    SUM(p.Total_Duration_Minutes) as total_duration_minutes,
                    COUNT(DISTINCT CASE WHEN m.Status = 'ended' THEN m.ID END) as ended_meetings,
                    COUNT(DISTINCT CASE WHEN m.Status = 'active' THEN m.ID END) as active_meetings,
                    COUNT(DISTINCT CASE WHEN m.Status = 'scheduled' THEN m.ID END) as scheduled_meetings,
                    
                    -- Overall attendance monitoring
                    AVG(ats.popup_count) as overall_avg_popup_count,
                    AVG(ats.total_detections) as overall_avg_detections,
                    AVG(ats.attendance_penalty) as overall_avg_penalty,
                    AVG(ats.engagement_score) as overall_avg_engagement
                    
                FROM tbl_Meetings m
                LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                LEFT JOIN tbl_Attendance_Sessions ats ON m.ID = ats.Meeting_ID
                WHERE COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) BETWEEN %s AND %s
            """, [start_date, end_date])
            
            summary_row = cursor.fetchone()
            overall_summary = {
                "total_meetings": int(summary_row[0] or 0),
                "total_hosts": int(summary_row[1] or 0),
                "total_participants": int(summary_row[2] or 0),
                "avg_duration_minutes": round(float(summary_row[3] or 0), 2),
                "avg_participant_attendance": round(float(summary_row[4] or 0), 2),
                "avg_overall_attendance": round(float(summary_row[5] or 0), 2),
                "total_duration_hours": round(float(summary_row[6] or 0) / 60, 2),
                "ended_meetings": int(summary_row[7] or 0),
                "active_meetings": int(summary_row[8] or 0),
                "scheduled_meetings": int(summary_row[9] or 0),
                "attendance_monitoring_summary": {
                    "overall_avg_popup_count": round(float(summary_row[10] or 0), 2),
                    "overall_avg_detections": round(float(summary_row[11] or 0), 2),
                    "overall_avg_penalty": round(float(summary_row[12] or 0), 2),
                    "overall_avg_engagement": round(float(summary_row[13] or 0), 2)
                },
                "date_range": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
            }

        # ==================== PREPARE RESPONSE DATA ====================
        response_data = {
            "overall_summary": overall_summary,
            "available_meeting_times": available_meeting_times,  # ADDED: Meeting time filters
            "filters_applied": {
                "user_id": user_id,
                "meeting_id": meeting_id,
                "analytics_type": analytics_type,
                "meeting_type": meeting_type,
                "timeframe": timeframe,
                "date_range": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
            }
        }

        if analytics_type in ['all', 'participant']:
            response_data["participant_details"] = participant_data if 'participant_data' in locals() else []
            response_data["participant_summary"] = participant_summary_data if 'participant_summary_data' in locals() else []

        if analytics_type in ['all', 'host']:
            response_data["host_analytics"] = host_data if 'host_data' in locals() else []

        if analytics_type in ['all', 'meeting']:
            response_data["meeting_analytics"] = meeting_data if 'meeting_data' in locals() else []

        logging.debug(f"✅ Comprehensive analytics fetched - analytics_type: {analytics_type}, available_times: {len(available_meeting_times)}")
        return JsonResponse({"data": response_data}, status=SUCCESS_STATUS)

    except Exception as e:
        logging.error(f"❌ Error fetching comprehensive analytics: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({"error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)


@require_http_methods(["GET"])
@csrf_exempt
def get_participant_meeting_duration_analytics(request):
    """
    Detailed analytics for:
    1. How long each participant stayed in each meeting (duration analysis)
    2. Participant attendance data from tbl_Participants (Participant_Attendance, Overall_Attendance)
    3. Attendance monitoring from tbl_Attendance_Sessions (popup_count, detections, penalties, etc.)
    Using actual table columns
    """
    try:
        user_id = request.GET.get('user_id') or request.GET.get('userId')
        meeting_id = request.GET.get('meeting_id') or request.GET.get('meetingId')
        
        if not user_id and not meeting_id:
            return JsonResponse({"error": "Either user_id or meeting_id is required"}, status=BAD_REQUEST_STATUS)

        with connection.cursor() as cursor:
            query = """
                SELECT 
                    p.User_ID,
                    p.Full_Name,
                    p.Meeting_ID,
                    m.Meeting_Name,
                    p.Join_Times,
                    p.Leave_Times,
                    p.Total_Duration_Minutes,
                    p.Total_Sessions,
                    p.Is_Currently_Active,
                    p.Attendance_Percentagebasedon_host,
                    p.Participant_Attendance,
                    p.Overall_Attendance,
                    p.End_Meeting_Time,
                    p.Role,
                    p.Meeting_Type,
                    
                    -- Attendance Sessions Data
                    ats.popup_count,
                    ats.total_detections,
                    ats.break_used,
                    ats.total_break_time_used,
                    ats.attendance_penalty,
                    ats.engagement_score,
                    ats.attendance_percentage as session_attendance_percentage,
                    ats.focus_score,
                    ats.break_count,
                    ats.active_participation_time,
                    ats.total_session_time,
                    
                    -- Meeting details
                    m.Started_At,
                    m.Ended_At,
                    m.Host_ID,
                    m.Status as meeting_status,
                    m.Created_At as meeting_created_at,
                    
                    -- Calculate meeting total duration if available
                    CASE 
                        WHEN m.Started_At IS NOT NULL AND m.Ended_At IS NOT NULL 
                        THEN TIMESTAMPDIFF(MINUTE, m.Started_At, m.Ended_At)
                        ELSE NULL 
                    END as meeting_total_duration_minutes
                    
                FROM tbl_Participants p
                JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                WHERE 1=1
            """
            
            params = []
            if user_id:
                query += " AND p.User_ID = %s"
                params.append(user_id)
            if meeting_id:
                query += " AND p.Meeting_ID = %s"
                params.append(meeting_id)
                
            query += " ORDER BY m.Created_At DESC"
            
            cursor.execute(query, params)
            duration_analytics = []
            
            for row in cursor.fetchall():
                participant_duration = float(row[6] or 0)
                meeting_total_duration = float(row[31] or 0) if row[31] else None
                participation_percentage = (participant_duration / meeting_total_duration * 100) if meeting_total_duration and meeting_total_duration > 0 else None
                
                duration_analytics.append({
                    "user_id": row[0],
                    "full_name": row[1],
                    "meeting_id": row[2],
                    "meeting_name": row[3],
                    
                    # Duration Analysis (How long they stayed in meeting)
                    "duration_analysis": {
                        "join_times": json.loads(row[4]) if row[4] else [],
                        "leave_times": json.loads(row[5]) if row[5] else [],
                        "total_duration_minutes": participant_duration,
                        "total_sessions": int(row[7] or 0),
                        "is_currently_active": bool(row[8]),
                        "end_meeting_time": row[12].isoformat() if row[12] else None,
                        "meeting_total_duration_minutes": meeting_total_duration,
                        "participation_percentage": round(participation_percentage, 2) if participation_percentage else None
                    },
                    
                    # Participant Attendance Data (from tbl_Participants)
                    "participant_attendance_data": {
                        "attendance_percentage_based_on_host": float(row[9] or 0),
                        "participant_attendance": float(row[10] or 0),
                        "overall_attendance": float(row[11] or 0)
                    },
                    
                    "participant_info": {
                        "role": row[13],
                        "meeting_type": row[14]
                    },
                    
                    "attendance_monitoring": {
                        "popup_count": int(row[15] or 0),
                        "total_detections": int(row[16] or 0),
                        "break_used": bool(row[17]),
                        "total_break_time_used": int(row[18] or 0),
                        "attendance_penalty": float(row[19] or 0),
                        "engagement_score": int(row[20] or 0),
                        "session_attendance_percentage": float(row[21] or 0),
                        "focus_score": float(row[22] or 0),
                        "break_count": int(row[23] or 0),
                        "active_participation_time": int(row[24] or 0),
                        "total_session_time": int(row[25] or 0)
                    },
                    
                    "meeting_details": {
                        "started_at": row[26].isoformat() if row[26] else None,
                        "ended_at": row[27].isoformat() if row[27] else None,
                        "host_id": row[28],
                        "status": row[29],
                        "created_at": row[30].isoformat() if row[30] else None
                    }
                })

        return JsonResponse({"data": duration_analytics}, status=SUCCESS_STATUS)

    except Exception as e:
        logging.error(f"Error fetching participant duration analytics: {e}")
        return JsonResponse({"error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def get_host_meeting_count_analytics(request):
    """
    Analytics for how many meetings each host conducted/created/completed
    Using actual table columns
    """
    try:
        host_id = request.GET.get('host_id') or request.GET.get('user_id') or request.GET.get('userId')
        timeframe = request.GET.get('timeframe', '30days')
        meeting_type = request.GET.get('meeting_type', 'all')
        
        # Calculate date range
        ist_timezone = pytz.timezone('Asia/Kolkata')
        end_date = timezone.now().astimezone(ist_timezone)
        if timeframe == '7days':
            start_date = end_date - timedelta(days=7)
        elif timeframe == '30days':
            start_date = end_date - timedelta(days=30)
        elif timeframe == '90days':
            start_date = end_date - timedelta(days=90)
        elif timeframe == '1year':
            start_date = end_date - timedelta(days=365)
        else:
            start_date = end_date - timedelta(days=30)

        with connection.cursor() as cursor:
            query = """
                SELECT 
                    m.Host_ID,
                    m.Meeting_Type,
                    COUNT(*) as total_meetings_created,
                    COUNT(CASE WHEN m.Status = 'ended' THEN 1 END) as ended_meetings,
                    COUNT(CASE WHEN m.Status = 'active' THEN 1 END) as active_meetings,
                    COUNT(CASE WHEN m.Status = 'scheduled' THEN 1 END) as scheduled_meetings,
                    
                    -- Meeting duration analytics (if started and ended times available)
                    AVG(CASE 
                        WHEN m.Started_At IS NOT NULL AND m.Ended_At IS NOT NULL 
                        THEN TIMESTAMPDIFF(MINUTE, m.Started_At, m.Ended_At)
                        ELSE NULL 
                    END) as avg_actual_meeting_duration_minutes,
                    
                    SUM(CASE 
                        WHEN m.Started_At IS NOT NULL AND m.Ended_At IS NOT NULL 
                        THEN TIMESTAMPDIFF(MINUTE, m.Started_At, m.Ended_At)
                        ELSE 0 
                    END) as total_actual_hosted_duration_minutes,
                    
                    -- Participant analytics
                    COUNT(DISTINCT p.User_ID) as total_unique_participants,
                    AVG(p.Participant_Attendance) as avg_participant_attendance,
                    AVG(p.Overall_Attendance) as avg_overall_attendance,
                    SUM(p.Total_Duration_Minutes) as total_participant_duration_minutes,
                    
                    -- Attendance monitoring
                    AVG(ats.popup_count) as avg_popup_count,
                    AVG(ats.total_detections) as avg_total_detections,
                    AVG(ats.attendance_penalty) as avg_attendance_penalty,
                    AVG(ats.engagement_score) as avg_engagement_score,
                    COUNT(CASE WHEN ats.break_used = 1 THEN 1 END) as total_breaks_across_meetings,
                    
                    -- Activity dates
                    MIN(m.Created_At) as first_meeting_date,
                    MAX(m.Created_At) as last_meeting_date,
                    
                    -- Recording analytics
                    COUNT(CASE WHEN m.Is_Recording_Enabled = 1 THEN 1 END) as meetings_with_recording_enabled,
                    COUNT(CASE WHEN m.Waiting_Room_Enabled = 1 THEN 1 END) as meetings_with_waiting_room
                    
                FROM tbl_Meetings m
                LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                LEFT JOIN tbl_Attendance_Sessions ats ON m.ID = ats.Meeting_ID
                WHERE m.Created_At BETWEEN %s AND %s
            """
            
            params = [start_date, end_date]
            if host_id:
                query += " AND m.Host_ID = %s"
                params.append(host_id)
            if meeting_type != 'all':
                query += " AND m.Meeting_Type = %s"
                params.append(meeting_type)
                
            query += " GROUP BY m.Host_ID, m.Meeting_Type ORDER BY total_meetings_created DESC"
            
            cursor.execute(query, params)
            host_analytics = []
            
            for row in cursor.fetchall():
                total_meetings = int(row[2])
                ended_meetings = int(row[3])
                
                host_analytics.append({
                    "host_id": row[0],
                    "meeting_type": row[1],
                    
                    "meeting_counts": {
                        "total_meetings_created": total_meetings,
                        "ended_meetings": ended_meetings,
                        "active_meetings": int(row[4]),
                        "scheduled_meetings": int(row[5]),
                        "completion_rate": round((ended_meetings / total_meetings * 100), 2) if total_meetings > 0 else 0
                    },
                    
                    "duration_analytics": {
                        "avg_actual_meeting_duration_minutes": round(float(row[6] or 0), 2),
                        "total_actual_hosted_duration_minutes": round(float(row[7] or 0), 2),
                        "total_actual_hosted_duration_hours": round(float(row[7] or 0) / 60, 2)
                    },
                    
                    "participant_analytics": {
                        "total_unique_participants": int(row[8] or 0),
                        "avg_participant_attendance": round(float(row[9] or 0), 2),
                        "avg_overall_attendance": round(float(row[10] or 0), 2),
                        "total_participant_duration_minutes": round(float(row[11] or 0), 2),
                        "total_participant_duration_hours": round(float(row[11] or 0) / 60, 2)
                    },
                    
                    "attendance_monitoring": {
                        "avg_popup_count": round(float(row[12] or 0), 2),
                        "avg_total_detections": round(float(row[13] or 0), 2),
                        "avg_attendance_penalty": round(float(row[14] or 0), 2),
                        "avg_engagement_score": round(float(row[15] or 0), 2),
                        "total_breaks_across_meetings": int(row[16] or 0)
                    },
                    
                    "activity_period": {
                        "first_meeting_date": row[17].isoformat() if row[17] else None,
                        "last_meeting_date": row[18].isoformat() if row[18] else None
                    },
                    
                    "meeting_features": {
                        "meetings_with_recording_enabled": int(row[19] or 0),
                        "meetings_with_waiting_room": int(row[20] or 0),
                        "recording_enabled_percentage": round((int(row[19] or 0) / total_meetings * 100), 2) if total_meetings > 0 else 0,
                        "waiting_room_enabled_percentage": round((int(row[20] or 0) / total_meetings * 100), 2) if total_meetings > 0 else 0
                    }
                })

        return JsonResponse({"data": host_analytics}, status=SUCCESS_STATUS)

    except Exception as e:
        logging.error(f"Error fetching host meeting count analytics: {e}")
        return JsonResponse({"error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def get_participant_attendance_analytics(request):
    """
    Focused analytics on participant attendance data showing:
    1. Participant_Attendance and Overall_Attendance from tbl_Participants
    2. All attendance monitoring data from tbl_Attendance_Sessions
    3. Attendance trends and patterns
    """
    try:
        user_id = request.GET.get('user_id') or request.GET.get('userId')
        meeting_id = request.GET.get('meeting_id') or request.GET.get('meetingId')
        timeframe = request.GET.get('timeframe', '30days')
        meeting_type = request.GET.get('meeting_type', 'all')
        
        # Calculate date range
        ist_timezone = pytz.timezone('Asia/Kolkata')
        end_date = timezone.now().astimezone(ist_timezone)
        if timeframe == '7days':
            start_date = end_date - timedelta(days=7)
        elif timeframe == '30days':
            start_date = end_date - timedelta(days=30)
        elif timeframe == '90days':
            start_date = end_date - timedelta(days=90)
        elif timeframe == '1year':
            start_date = end_date - timedelta(days=365)
        else:
            start_date = end_date - timedelta(days=30)

        with connection.cursor() as cursor:
            query = """
                SELECT 
                    p.User_ID,
                    p.Full_Name,
                    p.Meeting_ID,
                    m.Meeting_Name,
                    p.Meeting_Type,
                    p.Role,
                    
                    -- Core Attendance Data from tbl_Participants
                    p.Attendance_Percentagebasedon_host,
                    p.Participant_Attendance,
                    p.Overall_Attendance,
                    p.Total_Duration_Minutes,
                    p.Total_Sessions,
                    p.Is_Currently_Active,
                    
                    -- All Attendance Sessions Data (requested columns)
                    ats.popup_count,
                    ats.detection_counts,
                    ats.violation_start_times,
                    ats.total_detections,
                    ats.attendance_penalty,
                    ats.break_used,
                    ats.total_break_time_used,
                    ats.engagement_score,
                    ats.attendance_percentage as session_attendance_percentage,
                    
                    -- Additional monitoring data
                    ats.session_active,
                    ats.break_count,
                    ats.focus_score,
                    ats.violation_severity_score,
                    ats.active_participation_time,
                    ats.total_session_time,
                    ats.last_violation_type,
                    ats.continuous_violation_time,
                    
                    -- Meeting context
                    m.Created_At,
                    m.Started_At,
                    m.Ended_At,
                    m.Host_ID,
                    m.Status
                    
                FROM tbl_Participants p
                JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                WHERE 1=1
            """
            
            params = []
            if user_id:
                query += " AND p.User_ID = %s"
                params.append(user_id)
            if meeting_id:
                query += " AND p.Meeting_ID = %s"
                params.append(meeting_id)
            if meeting_type != 'all':
                query += " AND p.Meeting_Type = %s"
                params.append(meeting_type)
                
            query += " AND m.Created_At BETWEEN %s AND %s"
            params.extend([start_date, end_date])
            query += " ORDER BY m.Created_At DESC"
            
            cursor.execute(query, params)
            attendance_analytics = []
            
            for row in cursor.fetchall():
                attendance_analytics.append({
                    "user_id": row[0],
                    "full_name": row[1],
                    "meeting_id": row[2],
                    "meeting_name": row[3],
                    "meeting_type": row[4],
                    "role": row[5],
                    
                    # Core Attendance Metrics from tbl_Participants
                    "participant_attendance_metrics": {
                        "attendance_percentage_based_on_host": float(row[6] or 0),
                        "participant_attendance": float(row[7] or 0),
                        "overall_attendance": float(row[8] or 0),
                        "total_duration_minutes": float(row[9] or 0),
                        "total_sessions": int(row[10] or 0),
                        "is_currently_active": bool(row[11])
                    },
                    
                    # Detailed Attendance Monitoring from tbl_Attendance_Sessions
                    "attendance_monitoring_details": {
                        "popup_count": int(row[12] or 0),
                        "detection_counts": row[13],
                        "violation_start_times": row[14],
                        "total_detections": int(row[15] or 0),
                        "attendance_penalty": float(row[16] or 0),
                        "break_used": bool(row[17]),
                        "total_break_time_used": int(row[18] or 0),
                        "engagement_score": int(row[19] or 0),
                        "session_attendance_percentage": float(row[20] or 0)
                    },
                    
                    # Advanced Monitoring Metrics
                    "advanced_monitoring": {
                        "session_active": bool(row[21]),
                        "break_count": int(row[22] or 0),
                        "focus_score": float(row[23] or 0),
                        "violation_severity_score": float(row[24] or 0),
                        "active_participation_time": int(row[25] or 0),
                        "total_session_time": int(row[26] or 0),
                        "last_violation_type": row[27],
                        "continuous_violation_time": int(row[28] or 0)
                    },
                    
                    # Meeting Context
                    "meeting_context": {
                        "created_at": row[29].isoformat() if row[29] else None,
                        "started_at": row[30].isoformat() if row[30] else None,
                        "ended_at": row[31].isoformat() if row[31] else None,
                        "host_id": row[32],
                        "status": row[33]
                    }
                })

            # Summary statistics for attendance
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT p.User_ID) as total_participants,
                    AVG(p.Participant_Attendance) as avg_participant_attendance,
                    AVG(p.Overall_Attendance) as avg_overall_attendance,
                    AVG(p.Attendance_Percentagebasedon_host) as avg_attendance_based_on_host,
                    AVG(ats.attendance_penalty) as avg_penalty,
                    AVG(ats.engagement_score) as avg_engagement,
                    COUNT(CASE WHEN ats.break_used = 1 THEN 1 END) as total_breaks_used,
                    AVG(ats.total_detections) as avg_violations
                FROM tbl_Participants p
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                WHERE m.Created_At BETWEEN %s AND %s
            """, [start_date, end_date])
            
            summary = cursor.fetchone()
            attendance_summary = {
                "total_participants": int(summary[0] or 0),
                "avg_participant_attendance": round(float(summary[1] or 0), 2),
                "avg_overall_attendance": round(float(summary[2] or 0), 2),
                "avg_attendance_based_on_host": round(float(summary[3] or 0), 2),
                "avg_penalty": round(float(summary[4] or 0), 2),
                "avg_engagement": round(float(summary[5] or 0), 2),
                "total_breaks_used": int(summary[6] or 0),
                "avg_violations": round(float(summary[7] or 0), 2)
            }

        return JsonResponse({
            "data": {
                "attendance_details": attendance_analytics,
                "attendance_summary": attendance_summary,
                "filters_applied": {
                    "user_id": user_id,
                    "meeting_id": meeting_id,
                    "meeting_type": meeting_type,
                    "timeframe": timeframe,
                    "date_range": {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    }
                }
            }
        }, status=SUCCESS_STATUS)

    except Exception as e:
        logging.error(f"Error fetching participant attendance analytics: {e}")
        return JsonResponse({"error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

# Enhanced host dashboard overview with actual columns
@require_http_methods(["GET"])
@csrf_exempt
def get_host_dashboard_overview(request):
    """Enhanced host dashboard overview with attendance sessions data using actual columns"""
    try:
        user_id = request.GET.get('user_id') or request.GET.get('userId') or request.GET.get('host_id')
        timeframe = request.GET.get('timeframe', '7days')
        meeting_type = request.GET.get('meetingType') or request.GET.get('meeting_type', 'all')

        if not user_id:
            return JsonResponse({"error": "user_id is required"}, status=BAD_REQUEST_STATUS)

        # Calculate timeframe
        ist_timezone = pytz.timezone('Asia/Kolkata')
        end_date = timezone.now().astimezone(ist_timezone)
        if timeframe == '7days':
            start_date = end_date - timedelta(days=7)
        elif timeframe == '30days':
            start_date = end_date - timedelta(days=30)
        elif timeframe == '90days':
            start_date = end_date - timedelta(days=90)
        elif timeframe == '1year':
            start_date = end_date - timedelta(days=365)
        else:
            return JsonResponse({"error": "Invalid timeframe"}, status=BAD_REQUEST_STATUS)

        with connection.cursor() as cursor:
            # Enhanced overview with attendance sessions data using actual columns
            query = """
                SELECT 
                    COUNT(DISTINCT m.ID) as total_meetings,
                    COUNT(DISTINCT p.User_ID) as total_participants,
                    AVG(p.Total_Duration_Minutes) as avg_duration_minutes,
                    AVG(p.Participant_Attendance) as avg_participant_attendance,
                    AVG(p.Overall_Attendance) as avg_overall_attendance,
                    
                    -- Attendance monitoring averages
                    AVG(ats.popup_count) as avg_popup_count,
                    AVG(ats.total_detections) as avg_detections,
                    AVG(ats.attendance_penalty) as avg_penalty,
                    AVG(ats.total_break_time_used) as avg_break_time,
                    AVG(ats.engagement_score) as avg_engagement_score,
                    SUM(CASE WHEN ats.break_used = 1 THEN 1 ELSE 0 END) as total_breaks_used,
                    
                    -- Meeting status breakdown
                    COUNT(CASE WHEN m.Status = 'active' THEN 1 END) as active_meetings,
                    COUNT(CASE WHEN m.Status = 'ended' THEN 1 END) as ended_meetings,
                    COUNT(CASE WHEN m.Status = 'scheduled' THEN 1 END) as scheduled_meetings
                    
                FROM tbl_Meetings m
                LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                LEFT JOIN tbl_Attendance_Sessions ats ON m.ID = ats.Meeting_ID
                WHERE m.Host_ID = %s AND m.Created_At BETWEEN %s AND %s
            """
            
            params = [user_id, start_date, end_date]
            if meeting_type != 'all':
                query += " AND m.Meeting_Type = %s"
                params.append(meeting_type)

            cursor.execute(query, params)
            result = cursor.fetchone()

        data = {
            "total_meetings": int(result[0] or 0),
            "total_participants": int(result[1] or 0),
            "average_duration_minutes": round(float(result[2] or 0), 2),
            "avg_participant_attendance": round(float(result[3] or 0), 2),
            "avg_overall_attendance": round(float(result[4] or 0), 2),
            
            "attendance_monitoring": {
                "avg_popup_count": round(float(result[5] or 0), 2),
                "avg_detections": round(float(result[6] or 0), 2),
                "avg_penalty": round(float(result[7] or 0), 2),
                "avg_break_time_minutes": round(float(result[8] or 0), 2),
                "avg_engagement_score": round(float(result[9] or 0), 2),
                "total_breaks_used": int(result[10] or 0)
            },
            
            "meeting_status_breakdown": {
                "active_meetings": int(result[11] or 0),
                "ended_meetings": int(result[12] or 0),
                "scheduled_meetings": int(result[13] or 0)
            }
        }
        
        return JsonResponse({"data": data}, status=SUCCESS_STATUS)
    except Exception as e:
        logging.error(f"Error fetching enhanced host overview: {e}")
        return JsonResponse({"error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)


def get_participant_report_data(user_id, start_date=None, end_date=None, meeting_time=None):
    """
    FIXED: Get participant report data
    - Added p.Role = 'participant' filter
    - Added host_name column
    - Using only specified tbl_Attendance_Sessions columns
    """
    try:
        if not end_date:
            end_date = timezone.now()
        if not start_date:
            start_date = end_date - timedelta(days=365)

        meeting_time_filter = ""
        params = [user_id, start_date, end_date]

        if meeting_time:
            try:
                meeting_dt = datetime.strptime(meeting_time, "%Y-%m-%d %H:%M")
                time_window_start = meeting_dt - timedelta(minutes=30)
                time_window_end = meeting_dt + timedelta(minutes=30)
                
                meeting_time_filter = """
                    AND COALESCE(
                        m.Started_At,
                        sm.start_time,
                        cm.startTime,
                        m.Created_At
                    ) BETWEEN %s AND %s
                """
                params.extend([time_window_start, time_window_end])
                
                logging.info(f"✅ Filtering participant data for meeting time: {meeting_time}, window: {time_window_start} to {time_window_end}")
            except Exception as e:
                logging.warning(f"Invalid meeting_time format: {meeting_time} ({e})")

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT p.User_ID, p.Full_Name
                FROM tbl_Participants p
                WHERE p.User_ID = %s
                LIMIT 1
            """, [user_id])
            participant_info = cursor.fetchone()
            
            if not participant_info:
                logging.warning(f"No participant found with User_ID: {user_id}")
                return None

            # FIXED QUERY - Only specified columns from tbl_Attendance_Sessions
            query = f"""
                SELECT 
                    p.Meeting_ID,                                    -- 0
                    m.Meeting_Name,                                  -- 1
                    m.Meeting_Type,                                  -- 2
                    m.Host_ID,                                       -- 3
                    p.Role,                                          -- 4
                    p.Total_Duration_Minutes,                        -- 5
                    p.Total_Sessions,                                -- 6
                    p.Attendance_Percentagebasedon_host,             -- 7
                    p.Participant_Attendance,                        -- 8
                    p.Overall_Attendance,                            -- 9
                    COALESCE(host_p.Full_Name, 'Unknown Host') as host_name,  -- 10
                    ats.detection_counts,                            -- 11
                    ats.popup_count,                                 -- 12
                    ats.attendance_penalty,                          -- 13
                    ats.break_used,                                  -- 14
                    ats.violations,                                  -- 15
                    ats.engagement_score,                            -- 16
                    ats.attendance_percentage,                       -- 17
                    ats.break_count,                                 -- 18
                    ats.break_sessions,                              -- 19
                    ats.total_break_time_used,                       -- 20
                    ats.identity_warning_count,                      -- 21
                    ats.identity_warnings,                           -- 22
                    ats.identity_removal_count,                      -- 23
                    ats.identity_total_warnings_issued,              -- 24
                    ats.behavior_removal_count,                      -- 25
                    ats.continuous_violation_removal_count           -- 26
                FROM tbl_Participants p
                JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                LEFT JOIN tbl_Participants host_p ON m.ID = host_p.Meeting_ID AND m.Host_ID = host_p.User_ID
                WHERE p.User_ID = %s 
                AND p.Role = 'participant'
                AND DATE(COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                )) BETWEEN DATE(%s) AND DATE(%s)
                {meeting_time_filter}
                ORDER BY COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) DESC
            """
            
            cursor.execute(query, params)
            meetings_data = cursor.fetchall()
            
            logging.info(f"Found {len(meetings_data)} participant meetings for user {user_id}")

            # Overall stats - only 4 metrics
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT p.Meeting_ID),
                    AVG(p.Overall_Attendance),
                    SUM(p.Total_Duration_Minutes),
                    AVG(ats.engagement_score)
                FROM tbl_Participants p
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                WHERE p.User_ID = %s 
                AND p.Role = 'participant'
                AND DATE(COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                )) BETWEEN DATE(%s) AND DATE(%s)
                {meeting_time_filter}
            """, params)
            overall_stats = cursor.fetchone()

            return {
                'participant_info': {
                    'user_id': participant_info[0], 
                    'full_name': participant_info[1]
                },
                'meetings_data': meetings_data,
                'overall_stats': overall_stats,
                'date_range': {
                    'start': start_date, 
                    'end': end_date
                },
                'meeting_time_filter': meeting_time,
                'total_meetings_found': len(meetings_data)
            }

    except Exception as e:
        logging.error(f"Error getting participant report data: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return None


# @require_http_methods(["GET"])
# @csrf_exempt
# def generate_participant_report_pdf(request):
#     """
#     FIXED: Generate PDF report for participant
#     - Proper text wrapping in cells
#     - KeepTogether to prevent orphan headers
#     - Better row heights and spacing
#     - Professional design
#     """
#     try:
#         user_id = request.GET.get('user_id') or request.GET.get('userId')
#         start_date_str = request.GET.get('start_date')
#         end_date_str = request.GET.get('end_date')
#         meeting_time_str = request.GET.get('meeting_time')
        
#         if not user_id:
#             return JsonResponse({"error": "user_id is required"}, status=BAD_REQUEST_STATUS)
        
#         start_date = None
#         end_date = None
#         if start_date_str:
#             start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
#         if end_date_str:
#             end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
        
#         data = get_participant_report_data(user_id, start_date, end_date, meeting_time_str)
#         if not data:
#             return JsonResponse({"error": "Participant not found or no data available"}, status=NOT_FOUND_STATUS)
        
#         buffer = BytesIO()
#         doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=40, rightMargin=40, topMargin=70, bottomMargin=70)
        
#         report_gen = ReportGenerator()
#         story = []
        
#         # ==========================================
#         # CELL STYLES FOR PROPER TEXT WRAPPING
#         # ==========================================
#         cell_style = ParagraphStyle(
#             name='CellStyle',
#             fontName='Helvetica',
#             fontSize=7,
#             leading=9,
#             wordWrap='CJK'
#         )
        
#         cell_style_bold = ParagraphStyle(
#             name='CellStyleBold',
#             fontName='Helvetica-Bold',
#             fontSize=7,
#             leading=9,
#             wordWrap='CJK'
#         )
        
#         header_cell_style = ParagraphStyle(
#             name='HeaderCellStyle',
#             fontName='Helvetica-Bold',
#             fontSize=7,
#             leading=9,
#             textColor=colors.white,
#             wordWrap='CJK'
#         )
        
#         # Helper function to parse JSON safely
#         def safe_json_parse(value):
#             if value is None:
#                 return None
#             if isinstance(value, (dict, list)):
#                 return value
#             if isinstance(value, str):
#                 try:
#                     return json.loads(value)
#                 except:
#                     return None
#             return None
        
#         # Helper function to format timestamp
#         def format_timestamp(ts):
#             if ts is None or ts == 0 or ts == '':
#                 return 'N/A'
#             try:
#                 if isinstance(ts, str):
#                     ts = float(ts)
#                 dt = datetime.fromtimestamp(ts)
#                 return dt.strftime('%Y-%m-%d %H:%M:%S')
#             except:
#                 return str(ts)
        
#         # Helper to wrap text in Paragraph for proper cell wrapping
#         def P(text, style=cell_style):
#             return Paragraph(str(text) if text else '', style)
        
#         def PH(text):
#             return Paragraph(str(text) if text else '', header_cell_style)
        
#         # ==========================================
#         # TITLE AND PARTICIPANT INFO
#         # ==========================================
#         title = Paragraph("Participant Attendance Report", report_gen.custom_styles['ReportTitle'])
#         story.append(title)
#         story.append(Spacer(1, 20))
        
#         participant_info = data['participant_info']
#         story.append(Paragraph("Participant Information", report_gen.custom_styles['SectionHeader']))
        
#         participant_table_data = [
#             ['Full Name:', participant_info['full_name']],
#             ['Report Period:', f"{data['date_range']['start'].strftime('%Y-%m-%d')} to {data['date_range']['end'].strftime('%Y-%m-%d')}"],
#             ['Total Meetings Attended:', str(len(data['meetings_data']))]
#         ]
        
#         participant_table = Table(participant_table_data, colWidths=[2.5*inch, 4*inch])
#         participant_table.setStyle(TableStyle([
#             ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#2C3E50')),
#             ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
#             ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
#             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#             ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
#             ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
#             ('FONTSIZE', (0, 0), (-1, -1), 10),
#             ('GRID', (0, 0), (-1, -1), 1, colors.black),
#             ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#             ('LEFTPADDING', (0, 0), (-1, -1), 8),
#             ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#             ('TOPPADDING', (0, 0), (-1, -1), 8),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
#         ]))
#         story.append(participant_table)
#         story.append(Spacer(1, 20))
        
#         # Overall Performance Summary
#         overall_stats = data['overall_stats']
#         story.append(Paragraph("Overall Performance Summary", report_gen.custom_styles['SectionHeader']))
        
#         stats_table_data = [
#             ['Performance Metric', 'Value'],
#             ['Total Meetings Attended', str(int(overall_stats[0] or 0))],
#             ['Average Overall Attendance', f"{round(float(overall_stats[1] or 0), 2)}%"],
#             ['Total Duration', f"{round(float(overall_stats[2] or 0), 2)} minutes ({round(float(overall_stats[2] or 0) / 60, 2)} hours)"],
#             ['Average Engagement Score', f"{round(float(overall_stats[3] or 0), 2)} / 100"]
#         ]
        
#         stats_table = Table(stats_table_data, colWidths=[3.5*inch, 3*inch])
#         stats_table.setStyle(TableStyle([
#             ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
#             ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#             ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#             ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
#             ('FONTSIZE', (0, 0), (-1, -1), 10),
#             ('GRID', (0, 0), (-1, -1), 1, colors.grey),
#             ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#E8F8F5')]),
#             ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#             ('LEFTPADDING', (0, 0), (-1, -1), 8),
#             ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#             ('TOPPADDING', (0, 0), (-1, -1), 8),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
#         ]))
#         story.append(stats_table)
#         story.append(Spacer(1, 20))
#         story.append(PageBreak())
        
#         # ==========================================
#         # DETAILED MEETING RECORDS
#         # ==========================================
#         story.append(Paragraph("Detailed Meeting Records", report_gen.custom_styles['SectionHeader']))
#         story.append(Spacer(1, 10))
        
#         # Section header styles
#         section_title_style = ParagraphStyle(
#             name="SectionTitle",
#             fontName="Helvetica-Bold",
#             fontSize=11,
#             textColor=colors.HexColor('#2C3E50'),
#             spaceBefore=15,
#             spaceAfter=8
#         )
        
#         sub_section_style = ParagraphStyle(
#             name="SubSection",
#             fontName="Helvetica-Bold",
#             fontSize=9,
#             textColor=colors.HexColor('#7F8C8D'),
#             spaceBefore=10,
#             spaceAfter=5
#         )
        
#         if data['meetings_data']:
#             for idx, meeting in enumerate(data['meetings_data'], 1):
#                 # Column indices remain the same
#                 meeting_elements = []
                
#                 meeting_title = f"Meeting {idx}: {meeting[1] or 'Unnamed Meeting'}"
#                 meeting_elements.append(Paragraph(meeting_title, report_gen.custom_styles['SubHeader']))
#                 meeting_elements.append(Spacer(1, 10))
                
#                 # ==========================================
#                 # HOST DETAILS
#                 # ==========================================
#                 host_section_style = ParagraphStyle(
#                     'HostSection',
#                     parent=report_gen.styles['Normal'],
#                     fontSize=10,
#                     textColor=colors.HexColor('#8E44AD'),
#                     spaceAfter=5,
#                     fontName='Helvetica-Bold'
#                 )
#                 meeting_elements.append(Paragraph("Host Details", host_section_style))
                
#                 host_details_data = [
#                     ['Meeting ID', str(meeting[0])],
#                     ['Host Name', str(meeting[10]) if meeting[10] else 'N/A'],
#                     ['Meeting Type', meeting[2] or 'N/A'],
#                 ]
                
#                 host_details_table = Table(host_details_data, colWidths=[2*inch, 4.5*inch])
#                 host_details_table.setStyle(TableStyle([
#                     ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#9B59B6')),
#                     ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
#                     ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
#                     ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                     ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#                     ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
#                     ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
#                     ('FONTSIZE', (0, 0), (-1, -1), 9),
#                     ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                     ('LEFTPADDING', (0, 0), (-1, -1), 8),
#                     ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#                     ('TOPPADDING', (0, 0), (-1, -1), 6),
#                     ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
#                 ]))
#                 meeting_elements.append(host_details_table)
#                 meeting_elements.append(Spacer(1, 12))
                
#                 # ==========================================
#                 # PARTICIPATION DETAILS
#                 # ==========================================
#                 part_section_style = ParagraphStyle(
#                     'PartSection',
#                     parent=report_gen.styles['Normal'],
#                     fontSize=10,
#                     textColor=colors.HexColor('#2980B9'),
#                     spaceAfter=5,
#                     fontName='Helvetica-Bold'
#                 )
#                 meeting_elements.append(Paragraph("Your Participation Details", part_section_style))
                
#                 participation_data = [
#                     ['Participation Metric', 'Value'],
#                     ['Duration in Meeting', f"{round(float(meeting[5] or 0), 2)} minutes"],
#                     ['Total Sessions', str(int(meeting[6] or 0))],
#                     ['Attendance % (Host-based)', f"{round(float(meeting[7] or 0), 2)}%"],
#                     ['Participant Attendance', f"{round(float(meeting[8] or 0), 2)}%"],
#                     ['Overall Attendance', f"{round(float(meeting[9] or 0), 2)}%"]
#                 ]
                
#                 participation_table = Table(participation_data, colWidths=[3.2*inch, 3.3*inch])
#                 participation_table.setStyle(TableStyle([
#                     ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
#                     ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                     ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                     ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#                     ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#                     ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
#                     ('FONTSIZE', (0, 0), (-1, -1), 9),
#                     ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                     ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EBF5FB')]),
#                     ('LEFTPADDING', (0, 0), (-1, -1), 8),
#                     ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#                     ('TOPPADDING', (0, 0), (-1, -1), 6),
#                     ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
#                 ]))
#                 meeting_elements.append(participation_table)
#                 meeting_elements.append(Spacer(1, 12))
                
#                 # ==========================================
#                 # ATTENDANCE MONITORING & BEHAVIOR
#                 # ==========================================
#                 monitoring_section_style = ParagraphStyle(
#                     'MonitoringSection',
#                     parent=report_gen.styles['Normal'],
#                     fontSize=10,
#                     textColor=colors.HexColor('#E74C3C'),
#                     spaceAfter=5,
#                     fontName='Helvetica-Bold'
#                 )
#                 meeting_elements.append(Paragraph("Attendance Monitoring & Behavior", monitoring_section_style))
                
#                 simple_monitoring_data = [
#                     ['Monitoring Metric', 'Value'],
#                     ['Popup Count', str(int(meeting[12] or 0))],
#                     ['Attendance Penalty', f"{round(float(meeting[13] or 0), 2)}%"],
#                     ['Break Used', 'Yes ✓' if meeting[14] else 'No ✗'],
#                     ['Engagement Score', f"{int(meeting[16] or 0)} / 100"],
#                     ['Attendance Percentage', f"{round(float(meeting[17] or 0), 2)}%"],
#                     ['Break Count', str(int(meeting[18] or 0))],
#                     ['Total Break Time Used', f"{int(meeting[20] or 0)} seconds"],
#                     ['Identity Warning Count', str(int(meeting[21] or 0))],
#                     ['Identity Removal Count', str(int(meeting[23] or 0))],
#                     ['Identity Total Warnings Issued', str(int(meeting[24] or 0))],
#                     ['Behavior Removal Count', str(int(meeting[25] or 0))],
#                     ['Continuous Violation Removal Count', str(int(meeting[26] or 0))]
#                 ]
                
#                 simple_monitoring_table = Table(simple_monitoring_data, colWidths=[3.2*inch, 3.3*inch])
#                 simple_monitoring_table.setStyle(TableStyle([
#                     ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
#                     ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                     ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                     ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#                     ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#                     ('FONTSIZE', (0, 0), (-1, -1), 9),
#                     ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                     ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#FADBD8')]),
#                     ('LEFTPADDING', (0, 0), (-1, -1), 8),
#                     ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#                     ('TOPPADDING', (0, 0), (-1, -1), 6),
#                     ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
#                 ]))
#                 meeting_elements.append(simple_monitoring_table)
#                 meeting_elements.append(Spacer(1, 15))
                
#                 # Add all meeting basic info together
#                 story.append(KeepTogether(meeting_elements))
                
#                 # ==========================================
#                 # DETECTION COUNTS TABLE
#                 # ==========================================
#                 detection_counts_data = safe_json_parse(meeting[11])
#                 if detection_counts_data and isinstance(detection_counts_data, dict):
#                     dc_elements = []
#                     dc_elements.append(Paragraph("✓ Detection Counts", section_title_style))
                    
#                     dc_rows = [[PH('Field'), PH('Value')]]
#                     for key, value in detection_counts_data.items():
#                         display_value = str(value) if value is not None else 'null'
#                         if key in ['last_detection_time', 'camera_verified_at'] and value:
#                             display_value = format_timestamp(value)
#                         dc_rows.append([P(str(key)), P(display_value)])
                    
#                     dc_table = Table(dc_rows, colWidths=[3.2*inch, 3.3*inch], repeatRows=1)
#                     dc_table.setStyle(TableStyle([
#                         ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
#                         ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                         ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                         ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#                         ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#                         ('FONTSIZE', (0, 0), (-1, -1), 8),
#                         ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                         ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EAECEE')]),
#                         ('LEFTPADDING', (0, 0), (-1, -1), 6),
#                         ('RIGHTPADDING', (0, 0), (-1, -1), 6),
#                         ('TOPPADDING', (0, 0), (-1, -1), 5),
#                         ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
#                     ]))
#                     dc_elements.append(dc_table)
#                     dc_elements.append(Spacer(1, 15))
#                     story.append(KeepTogether(dc_elements))
                
#                 # ==========================================
#                 # VIOLATIONS DATA
#                 # ==========================================
#                 violations_data = safe_json_parse(meeting[15])
#                 if violations_data and isinstance(violations_data, dict):
#                     story.append(Paragraph("✓ Violations Data", section_title_style))
#                     story.append(Spacer(1, 8))
                    
#                     # Warnings Table
#                     warnings = violations_data.get('warnings', [])
#                     if warnings and len(warnings) > 0:
#                         warn_elements = []
#                         warn_elements.append(Paragraph("Warnings Table", sub_section_style))
                        
#                         warn_rows = [[PH('#'), PH('Timestamp'), PH('Violation Type'), PH('Duration'), PH('Time Range'), PH('Message')]]
#                         for i, w in enumerate(warnings, 1):
#                             if isinstance(w, dict):
#                                 warn_rows.append([
#                                     P(str(i)),
#                                     P(format_timestamp(w.get('timestamp', ''))),
#                                     P(str(w.get('violation_type', 'N/A'))),
#                                     P(f"{round(float(w.get('duration', 0)), 2)}s"),
#                                     P(str(w.get('time_range', 'N/A'))),
#                                     P(str(w.get('message', 'N/A'))[:40])
#                                 ])
                        
#                         warn_table = Table(warn_rows, colWidths=[0.4*inch, 1.2*inch, 1.1*inch, 0.7*inch, 0.7*inch, 2.4*inch], repeatRows=1)
#                         warn_table.setStyle(TableStyle([
#                             ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F39C12')),
#                             ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                             ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#                             ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                             ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#FEF9E7')]),
#                             ('LEFTPADDING', (0, 0), (-1, -1), 4),
#                             ('RIGHTPADDING', (0, 0), (-1, -1), 4),
#                             ('TOPPADDING', (0, 0), (-1, -1), 5),
#                             ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
#                         ]))
#                         warn_elements.append(warn_table)
#                         warn_elements.append(Spacer(1, 12))
#                         story.append(KeepTogether(warn_elements))
                    
#                     # Detection Events Table
#                     detections = violations_data.get('detections', []) or violations_data.get('detection_events', [])
#                     if detections and len(detections) > 0:
#                         det_elements = []
#                         det_elements.append(Paragraph("Detection Events Table", sub_section_style))
                        
#                         det_rows = [[PH('#'), PH('Timestamp'), PH('Violation Type'), PH('Duration'), PH('Penalty'), PH('Message')]]
#                         for i, d in enumerate(detections, 1):
#                             if isinstance(d, dict):
#                                 det_rows.append([
#                                     P(str(i)),
#                                     P(format_timestamp(d.get('timestamp', ''))),
#                                     P(str(d.get('violation_type', 'N/A'))),
#                                     P(f"{round(float(d.get('duration', 0)), 2)}s"),
#                                     P(f"{round(float(d.get('penalty_applied', 0)), 2)}%"),
#                                     P(str(d.get('message', 'N/A'))[:40])
#                                 ])
                        
#                         det_table = Table(det_rows, colWidths=[0.4*inch, 1.2*inch, 1.1*inch, 0.7*inch, 0.7*inch, 2.4*inch], repeatRows=1)
#                         det_table.setStyle(TableStyle([
#                             ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
#                             ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                             ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#                             ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                             ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EBF5FB')]),
#                             ('LEFTPADDING', (0, 0), (-1, -1), 4),
#                             ('RIGHTPADDING', (0, 0), (-1, -1), 4),
#                             ('TOPPADDING', (0, 0), (-1, -1), 5),
#                             ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
#                         ]))
#                         det_elements.append(det_table)
#                         det_elements.append(Spacer(1, 12))
#                         story.append(KeepTogether(det_elements))
                    
#                     # Continuous Removals Table
#                     removals = violations_data.get('continuous_removals', []) or violations_data.get('removals', [])
#                     if removals and len(removals) > 0:
#                         rem_elements = []
#                         rem_elements.append(Paragraph("Continuous Removals Table", sub_section_style))
                        
#                         rem_rows = [[PH('#'), PH('Timestamp'), PH('Violation Type'), PH('Duration'), PH('Penalty'), PH('Message')]]
#                         for i, r in enumerate(removals, 1):
#                             if isinstance(r, dict):
#                                 rem_rows.append([
#                                     P(str(i)),
#                                     P(format_timestamp(r.get('timestamp', ''))),
#                                     P(str(r.get('violation_type', 'N/A'))),
#                                     P(f"{round(float(r.get('duration', 0)), 2)}s"),
#                                     P(str(round(float(r.get('penalty', 0)), 2))),
#                                     P(str(r.get('message', 'N/A'))[:40])
#                                 ])
                        
#                         rem_table = Table(rem_rows, colWidths=[0.4*inch, 1.2*inch, 1.1*inch, 0.7*inch, 0.7*inch, 2.4*inch], repeatRows=1)
#                         rem_table.setStyle(TableStyle([
#                             ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
#                             ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                             ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#                             ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                             ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#FADBD8')]),
#                             ('LEFTPADDING', (0, 0), (-1, -1), 4),
#                             ('RIGHTPADDING', (0, 0), (-1, -1), 4),
#                             ('TOPPADDING', (0, 0), (-1, -1), 5),
#                             ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
#                         ]))
#                         rem_elements.append(rem_table)
#                         rem_elements.append(Spacer(1, 12))
#                         story.append(KeepTogether(rem_elements))
                
#                 # ==========================================
#                 # BREAK SESSIONS TABLE
#                 # ==========================================
#                 break_sessions_data = safe_json_parse(meeting[19])
#                 if break_sessions_data and isinstance(break_sessions_data, list) and len(break_sessions_data) > 0:
#                     bs_elements = []
#                     bs_elements.append(Paragraph("✓ Break Sessions", section_title_style))
                    
#                     bs_rows = [[PH('Break #'), PH('Start Time'), PH('End Time'), PH('Duration (sec)')]]
#                     for i, bs in enumerate(break_sessions_data, 1):
#                         if isinstance(bs, dict):
#                             start_time = bs.get('start_time', bs.get('start', ''))
#                             end_time = bs.get('end_time', bs.get('end', ''))
#                             duration = bs.get('duration', 0)
                            
#                             bs_rows.append([
#                                 P(str(i)),
#                                 P(format_timestamp(start_time) if start_time else str(start_time)),
#                                 P(format_timestamp(end_time) if end_time else str(end_time)),
#                                 P(str(round(float(duration), 2)) if duration else '0')
#                             ])
                    
#                     bs_table = Table(bs_rows, colWidths=[0.8*inch, 2.2*inch, 2.2*inch, 1.3*inch], repeatRows=1)
#                     bs_table.setStyle(TableStyle([
#                         ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
#                         ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                         ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                         ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#                         ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                         ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#E8F8F5')]),
#                         ('LEFTPADDING', (0, 0), (-1, -1), 6),
#                         ('RIGHTPADDING', (0, 0), (-1, -1), 6),
#                         ('TOPPADDING', (0, 0), (-1, -1), 6),
#                         ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
#                     ]))
#                     bs_elements.append(bs_table)
#                     bs_elements.append(Spacer(1, 15))
#                     story.append(KeepTogether(bs_elements))
                
#                 # ==========================================
#                 # IDENTITY WARNINGS TABLE
#                 # ==========================================
#                 identity_warnings_data = safe_json_parse(meeting[22])
#                 if identity_warnings_data and isinstance(identity_warnings_data, list) and len(identity_warnings_data) > 0:
#                     iw_elements = []
#                     iw_elements.append(Paragraph("✓ Identity Warnings", section_title_style))
                    
#                     iw_rows = [[PH('#'), PH('Timestamp'), PH('Cycle #'), PH('Total #'), PH('Consec. Sec'), PH('Similarity'), PH('Unknown Sec'), PH('Cycle'), PH('ID Rem'), PH('Beh Rem')]]
#                     for i, iw in enumerate(identity_warnings_data, 1):
#                         if isinstance(iw, dict):
#                             iw_rows.append([
#                                 P(str(i)),
#                                 P(format_timestamp(iw.get('timestamp', ''))),
#                                 P(str(iw.get('cycle_warning', iw.get('cycle_warning_number', 'N/A')))),
#                                 P(str(iw.get('total_warning', iw.get('total_warning_number', 'N/A')))),
#                                 P(str(iw.get('consecutive_seconds', 'N/A'))),
#                                 P(str(round(float(iw.get('similarity_score', 0)), 2)) if iw.get('similarity_score') else 'N/A'),
#                                 P(str(iw.get('total_unknown_seconds', 'N/A'))),
#                                 P(str(iw.get('removal_cycle', 'N/A'))),
#                                 P(str(iw.get('identity_removals', 'N/A'))),
#                                 P(str(iw.get('behavior_removals', 'N/A')))
#                             ])
                    
#                     iw_table = Table(iw_rows, colWidths=[0.35*inch, 1.1*inch, 0.5*inch, 0.5*inch, 0.65*inch, 0.65*inch, 0.7*inch, 0.5*inch, 0.5*inch, 0.55*inch], repeatRows=1)
#                     iw_table.setStyle(TableStyle([
#                         ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9B59B6')),
#                         ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                         ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
#                         ('VALIGN', (0, 0), (-1, -1), 'TOP'),
#                         ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                         ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F5EEF8')]),
#                         ('LEFTPADDING', (0, 0), (-1, -1), 3),
#                         ('RIGHTPADDING', (0, 0), (-1, -1), 3),
#                         ('TOPPADDING', (0, 0), (-1, -1), 5),
#                         ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
#                     ]))
#                     iw_elements.append(iw_table)
#                     iw_elements.append(Spacer(1, 15))
#                     story.append(KeepTogether(iw_elements))
                
#                 # Page break between meetings
#                 if idx < len(data['meetings_data']):
#                     story.append(Spacer(1, 20))
#                     story.append(PageBreak())
#         else:
#             story.append(Paragraph("No meeting records found where you attended as a participant.", report_gen.styles['Normal']))
        
#         def add_page_number(canvas, doc):
#             report_gen.create_header_footer(canvas, doc, "Participant Attendance Report")
        
#         doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
        
#         buffer.seek(0)
#         response = HttpResponse(buffer.getvalue(), content_type='application/pdf')
#         response['Content-Disposition'] = f'attachment; filename="participant_report_{user_id}_{datetime.now().strftime("%Y%m%d")}.pdf"'
        
#         return response
        
#     except Exception as e:
#         logging.error(f"Error generating participant PDF report: {e}")
#         logging.error(traceback.format_exc())
#         return JsonResponse({"error": f"Failed to generate report: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def generate_participant_report_pdf(request):
    """
    FIXED: Generate PDF report for participant
    - Removed box styling from main section headers
    - Fixed table splitting across pages
    - Proper KeepTogether for all sections
    - Professional design
    """
    try:
        user_id = request.GET.get('user_id') or request.GET.get('userId')
        start_date_str = request.GET.get('start_date')
        end_date_str = request.GET.get('end_date')
        meeting_time_str = request.GET.get('meeting_time')
        
        if not user_id:
            return JsonResponse({"error": "user_id is required"}, status=BAD_REQUEST_STATUS)
        
        start_date = None
        end_date = None
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
        
        data = get_participant_report_data(user_id, start_date, end_date, meeting_time_str)
        if not data:
            return JsonResponse({"error": "Participant not found or no data available"}, status=NOT_FOUND_STATUS)
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=40, rightMargin=40, topMargin=70, bottomMargin=70)
        
        report_gen = ReportGenerator()
        story = []
        
        # ==========================================
        # CUSTOM STYLES - NO BOX FOR SECTION HEADERS
        # ==========================================
        
        # Main section header style (NO BOX - just bold text with underline effect)
        main_section_style = ParagraphStyle(
            name='MainSectionHeader',
            fontName='Helvetica-Bold',
            fontSize=14,
            textColor=colors.HexColor('#2C3E50'),
            spaceBefore=15,
            spaceAfter=10,
            borderWidth=0,
            borderColor=colors.HexColor('#2C3E50'),
            borderPadding=0,
        )
        
        # Sub-section header style (for meeting titles)
        sub_section_style = ParagraphStyle(
            name='SubSectionHeader',
            fontName='Helvetica-Bold',
            fontSize=12,
            textColor=colors.HexColor('#34495E'),
            spaceBefore=12,
            spaceAfter=8,
        )
        
        cell_style = ParagraphStyle(
            name='CellStyle',
            fontName='Helvetica',
            fontSize=7,
            leading=9,
            wordWrap='CJK'
        )
        
        cell_style_bold = ParagraphStyle(
            name='CellStyleBold',
            fontName='Helvetica-Bold',
            fontSize=7,
            leading=9,
            wordWrap='CJK'
        )
        
        header_cell_style = ParagraphStyle(
            name='HeaderCellStyle',
            fontName='Helvetica-Bold',
            fontSize=7,
            leading=9,
            textColor=colors.white,
            wordWrap='CJK'
        )
        
        # Section title style for sub-sections like "✓ Detection Counts"
        section_title_style = ParagraphStyle(
            name="SectionTitle",
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=colors.HexColor('#2C3E50'),
            spaceBefore=15,
            spaceAfter=8
        )
        
        # Sub section style for tables like "Warnings Table"
        table_sub_section_style = ParagraphStyle(
            name="TableSubSection",
            fontName="Helvetica-Bold",
            fontSize=9,
            textColor=colors.HexColor('#7F8C8D'),
            spaceBefore=10,
            spaceAfter=5
        )
        
        # Helper function to parse JSON safely
        def safe_json_parse(value):
            if value is None:
                return None
            if isinstance(value, (dict, list)):
                return value
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except:
                    return None
            return None
        
        # Helper function to format timestamp
        def format_timestamp(ts):
            if ts is None or ts == 0 or ts == '':
                return 'N/A'
            try:
                if isinstance(ts, str):
                    ts = float(ts)
                dt = datetime.fromtimestamp(ts)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return str(ts)
        
        # Helper to wrap text in Paragraph for proper cell wrapping
        def P(text, style=cell_style):
            return Paragraph(str(text) if text else '', style)
        
        def PH(text):
            return Paragraph(str(text) if text else '', header_cell_style)
        
        # ==========================================
        # TITLE
        # ==========================================
        title = Paragraph("Participant Attendance Report", report_gen.custom_styles['ReportTitle'])
        story.append(title)
        story.append(Spacer(1, 20))
        
        # ==========================================
        # PARTICIPANT INFORMATION (NO BOX)
        # ==========================================
        participant_info = data['participant_info']
        
        # Section header without box - just styled text with a line underneath
        story.append(Paragraph("Participant Information", main_section_style))
        
        # Add a horizontal line under the header
        story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#2C3E50'), spaceBefore=0, spaceAfter=10))
        
        participant_table_data = [
            ['Full Name:', participant_info['full_name']],
            ['Report Period:', f"{data['date_range']['start'].strftime('%Y-%m-%d')} to {data['date_range']['end'].strftime('%Y-%m-%d')}"],
            ['Total Meetings Attended:', str(len(data['meetings_data']))]
        ]
        
        participant_table = Table(participant_table_data, colWidths=[2.5*inch, 4*inch])
        participant_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#2C3E50')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(participant_table)
        story.append(Spacer(1, 20))
        
        # ==========================================
        # OVERALL PERFORMANCE SUMMARY (NO BOX)
        # ==========================================
        overall_stats = data['overall_stats']
        
        story.append(Paragraph("Overall Performance Summary", main_section_style))
        story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#27AE60'), spaceBefore=0, spaceAfter=10))
        
        stats_table_data = [
            ['Performance Metric', 'Value'],
            ['Total Meetings Attended', str(int(overall_stats[0] or 0))],
            ['Average Overall Attendance', f"{round(float(overall_stats[1] or 0), 2)}%"],
            ['Total Duration', f"{round(float(overall_stats[2] or 0), 2)} minutes ({round(float(overall_stats[2] or 0) / 60, 2)} hours)"],
            ['Average Engagement Score', f"{round(float(overall_stats[3] or 0), 2)} / 100"]
        ]
        
        stats_table = Table(stats_table_data, colWidths=[3.5*inch, 3*inch])
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#E8F8F5')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(stats_table)
        story.append(Spacer(1, 20))
        story.append(PageBreak())
        
        # ==========================================
        # DETAILED MEETING RECORDS (NO BOX)
        # ==========================================
        
        if data['meetings_data']:
            for idx, meeting in enumerate(data['meetings_data'], 1):
                meeting_elements = []
                
                # Add "Detailed Meeting Records" header ONLY for the first meeting
                if idx == 1:
                    meeting_elements.append(Paragraph("Detailed Meeting Records", main_section_style))
                    meeting_elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#2C3E50'), spaceBefore=0, spaceAfter=15))
                
                meeting_title = f"Meeting {idx}: {meeting[1] or 'Unnamed Meeting'}"
                meeting_elements.append(Paragraph(meeting_title, sub_section_style))
                meeting_elements.append(Spacer(1, 10))
                
                # ==========================================
                # HOST DETAILS
                # ==========================================
                host_section_style = ParagraphStyle(
                    'HostSection',
                    parent=report_gen.styles['Normal'],
                    fontSize=10,
                    textColor=colors.HexColor('#8E44AD'),
                    spaceAfter=5,
                    fontName='Helvetica-Bold'
                )
                meeting_elements.append(Paragraph("Host Details", host_section_style))
                
                host_details_data = [
                    ['Meeting ID', str(meeting[0])],
                    ['Host Name', str(meeting[10]) if meeting[10] else 'N/A'],
                    ['Meeting Type', meeting[2] or 'N/A'],
                ]
                
                host_details_table = Table(host_details_data, colWidths=[2*inch, 4.5*inch])
                host_details_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#9B59B6')),
                    ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
                    ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                    ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('LEFTPADDING', (0, 0), (-1, -1), 8),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ]))
                meeting_elements.append(host_details_table)
                meeting_elements.append(Spacer(1, 12))
                
                # ==========================================
                # PARTICIPATION DETAILS
                # ==========================================
                part_section_style = ParagraphStyle(
                    'PartSection',
                    parent=report_gen.styles['Normal'],
                    fontSize=10,
                    textColor=colors.HexColor('#2980B9'),
                    spaceAfter=5,
                    fontName='Helvetica-Bold'
                )
                meeting_elements.append(Paragraph("Your Participation Details", part_section_style))
                
                participation_data = [
                    ['Participation Metric', 'Value'],
                    ['Duration in Meeting', f"{round(float(meeting[5] or 0), 2)} minutes"],
                    ['Total Sessions', str(int(meeting[6] or 0))],
                    ['Attendance % (Host-based)', f"{round(float(meeting[7] or 0), 2)}%"],
                    ['Participant Attendance', f"{round(float(meeting[8] or 0), 2)}%"],
                    ['Overall Attendance', f"{round(float(meeting[9] or 0), 2)}%"]
                ]
                
                participation_table = Table(participation_data, colWidths=[3.2*inch, 3.3*inch])
                participation_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EBF5FB')]),
                    ('LEFTPADDING', (0, 0), (-1, -1), 8),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ]))
                meeting_elements.append(participation_table)
                meeting_elements.append(Spacer(1, 12))
                
                # Keep meeting header + host + participation together
                story.append(KeepTogether(meeting_elements))
                
                # ==========================================
                # ATTENDANCE MONITORING & BEHAVIOR (Separate KeepTogether)
                # ==========================================
                monitoring_elements = []
                monitoring_section_style = ParagraphStyle(
                    'MonitoringSection',
                    parent=report_gen.styles['Normal'],
                    fontSize=10,
                    textColor=colors.HexColor('#E74C3C'),
                    spaceAfter=5,
                    fontName='Helvetica-Bold'
                )
                monitoring_elements.append(Paragraph("Attendance Monitoring & Behavior", monitoring_section_style))
                
                simple_monitoring_data = [
                    ['Monitoring Metric', 'Value'],
                    ['Popup Count', str(int(meeting[12] or 0))],
                    ['Attendance Penalty', f"{round(float(meeting[13] or 0), 2)}%"],
                    ['Break Used', 'Yes ✓' if meeting[14] else 'No ✗'],
                    ['Engagement Score', f"{int(meeting[16] or 0)} / 100"],
                    ['Attendance Percentage', f"{round(float(meeting[17] or 0), 2)}%"],
                    ['Break Count', str(int(meeting[18] or 0))],
                    ['Total Break Time Used', f"{int(meeting[20] or 0)} seconds"],
                    ['Identity Warning Count', str(int(meeting[21] or 0))],
                    ['Identity Removal Count', str(int(meeting[23] or 0))],
                    ['Identity Total Warnings Issued', str(int(meeting[24] or 0))],
                    ['Behavior Removal Count', str(int(meeting[25] or 0))],
                    ['Continuous Violation Removal Count', str(int(meeting[26] or 0))]
                ]
                
                simple_monitoring_table = Table(simple_monitoring_data, colWidths=[3.2*inch, 3.3*inch])
                simple_monitoring_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#FADBD8')]),
                    ('LEFTPADDING', (0, 0), (-1, -1), 8),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ]))
                monitoring_elements.append(simple_monitoring_table)
                monitoring_elements.append(Spacer(1, 15))
                
                # Keep monitoring section together
                story.append(KeepTogether(monitoring_elements))
                
                # ==========================================
                # DETECTION COUNTS TABLE
                # ==========================================
                detection_counts_data = safe_json_parse(meeting[11])
                if detection_counts_data and isinstance(detection_counts_data, dict):
                    dc_elements = []
                    dc_elements.append(Paragraph("✓ Detection Counts", section_title_style))
                    
                    dc_rows = [[PH('Field'), PH('Value')]]
                    for key, value in detection_counts_data.items():
                        display_value = str(value) if value is not None else 'null'
                        if key in ['last_detection_time', 'camera_verified_at'] and value:
                            display_value = format_timestamp(value)
                        dc_rows.append([P(str(key)), P(display_value)])
                    
                    dc_table = Table(dc_rows, colWidths=[3.2*inch, 3.3*inch], repeatRows=1)
                    dc_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EAECEE')]),
                        ('LEFTPADDING', (0, 0), (-1, -1), 6),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                        ('TOPPADDING', (0, 0), (-1, -1), 5),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                    ]))
                    dc_elements.append(dc_table)
                    dc_elements.append(Spacer(1, 15))
                    story.append(KeepTogether(dc_elements))
                
                # ==========================================
                # VIOLATIONS DATA - Keep header with first table
                # ==========================================
                violations_data = safe_json_parse(meeting[15])
                if violations_data and isinstance(violations_data, dict):
                    # Check if there's any actual data
                    warnings = violations_data.get('warnings', [])
                    detections = violations_data.get('detections', []) or violations_data.get('detection_events', [])
                    removals = violations_data.get('continuous_removals', []) or violations_data.get('removals', [])
                    
                    has_warnings = warnings and len(warnings) > 0
                    has_detections = detections and len(detections) > 0
                    has_removals = removals and len(removals) > 0
                    
                    # Only add violations section if there's actual data
                    if has_warnings or has_detections or has_removals:
                        # Warnings Table - include main header with first table
                        if has_warnings:
                            warn_elements = []
                            warn_elements.append(Paragraph("✓ Violations Data", section_title_style))
                            warn_elements.append(Spacer(1, 8))
                            warn_elements.append(Paragraph("Warnings Table", table_sub_section_style))
                            
                            warn_rows = [[PH('#'), PH('Timestamp'), PH('Violation Type'), PH('Duration'), PH('Time Range'), PH('Message')]]
                            for i, w in enumerate(warnings, 1):
                                if isinstance(w, dict):
                                    warn_rows.append([
                                        P(str(i)),
                                        P(format_timestamp(w.get('timestamp', ''))),
                                        P(str(w.get('violation_type', 'N/A'))),
                                        P(f"{round(float(w.get('duration', 0)), 2)}s"),
                                        P(str(w.get('time_range', 'N/A'))),
                                        P(str(w.get('message', 'N/A'))[:40])
                                    ])
                            
                            warn_table = Table(warn_rows, colWidths=[0.4*inch, 1.2*inch, 1.1*inch, 0.7*inch, 0.7*inch, 2.4*inch], repeatRows=1)
                            warn_table.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F39C12')),
                                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#FEF9E7')]),
                                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                                ('TOPPADDING', (0, 0), (-1, -1), 5),
                                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                            ]))
                            warn_elements.append(warn_table)
                            warn_elements.append(Spacer(1, 12))
                            story.append(KeepTogether(warn_elements))
                        elif has_detections or has_removals:
                            # Add violations header before first available table
                            story.append(Paragraph("✓ Violations Data", section_title_style))
                            story.append(Spacer(1, 8))
                        
                        # Detection Events Table
                        if has_detections:
                            det_elements = []
                            det_elements.append(Paragraph("Detection Events Table", table_sub_section_style))
                            
                            det_rows = [[PH('#'), PH('Timestamp'), PH('Violation Type'), PH('Duration'), PH('Penalty'), PH('Message')]]
                            for i, d in enumerate(detections, 1):
                                if isinstance(d, dict):
                                    det_rows.append([
                                        P(str(i)),
                                        P(format_timestamp(d.get('timestamp', ''))),
                                        P(str(d.get('violation_type', 'N/A'))),
                                        P(f"{round(float(d.get('duration', 0)), 2)}s"),
                                        P(f"{round(float(d.get('penalty_applied', 0)), 2)}%"),
                                        P(str(d.get('message', 'N/A'))[:40])
                                    ])
                            
                            det_table = Table(det_rows, colWidths=[0.4*inch, 1.2*inch, 1.1*inch, 0.7*inch, 0.7*inch, 2.4*inch], repeatRows=1)
                            det_table.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
                                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EBF5FB')]),
                                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                                ('TOPPADDING', (0, 0), (-1, -1), 5),
                                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                            ]))
                            det_elements.append(det_table)
                            det_elements.append(Spacer(1, 12))
                            story.append(KeepTogether(det_elements))
                        
                        # Continuous Removals Table
                        if has_removals:
                            rem_elements = []
                            rem_elements.append(Paragraph("Continuous Removals Table", table_sub_section_style))
                            
                            rem_rows = [[PH('#'), PH('Timestamp'), PH('Violation Type'), PH('Duration'), PH('Penalty'), PH('Message')]]
                            for i, r in enumerate(removals, 1):
                                if isinstance(r, dict):
                                    rem_rows.append([
                                        P(str(i)),
                                        P(format_timestamp(r.get('timestamp', ''))),
                                        P(str(r.get('violation_type', 'N/A'))),
                                        P(f"{round(float(r.get('duration', 0)), 2)}s"),
                                        P(str(round(float(r.get('penalty', 0)), 2))),
                                        P(str(r.get('message', 'N/A'))[:40])
                                    ])
                            
                            rem_table = Table(rem_rows, colWidths=[0.4*inch, 1.2*inch, 1.1*inch, 0.7*inch, 0.7*inch, 2.4*inch], repeatRows=1)
                            rem_table.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
                                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#FADBD8')]),
                                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                                ('TOPPADDING', (0, 0), (-1, -1), 5),
                                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                            ]))
                            rem_elements.append(rem_table)
                            rem_elements.append(Spacer(1, 12))
                            story.append(KeepTogether(rem_elements))
                    else:
                        # No actual violation data - just show the header with "No data"
                        vio_elements = []
                        vio_elements.append(Paragraph("✓ Violations Data", section_title_style))
                        vio_elements.append(Spacer(1, 5))
                        vio_elements.append(Paragraph("No violations recorded.", report_gen.styles['Normal']))
                        vio_elements.append(Spacer(1, 12))
                        story.append(KeepTogether(vio_elements))
                
                # ==========================================
                # BREAK SESSIONS TABLE
                # ==========================================
                break_sessions_data = safe_json_parse(meeting[19])
                if break_sessions_data and isinstance(break_sessions_data, list) and len(break_sessions_data) > 0:
                    bs_elements = []
                    bs_elements.append(Paragraph("✓ Break Sessions", section_title_style))
                    
                    bs_rows = [[PH('Break #'), PH('Start Time'), PH('End Time'), PH('Duration (sec)')]]
                    for i, bs in enumerate(break_sessions_data, 1):
                        if isinstance(bs, dict):
                            start_time = bs.get('start_time', bs.get('start', ''))
                            end_time = bs.get('end_time', bs.get('end', ''))
                            duration = bs.get('duration', 0)
                            
                            bs_rows.append([
                                P(str(i)),
                                P(format_timestamp(start_time) if start_time else str(start_time)),
                                P(format_timestamp(end_time) if end_time else str(end_time)),
                                P(str(round(float(duration), 2)) if duration else '0')
                            ])
                    
                    bs_table = Table(bs_rows, colWidths=[0.8*inch, 2.2*inch, 2.2*inch, 1.3*inch], repeatRows=1)
                    bs_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#E8F8F5')]),
                        ('LEFTPADDING', (0, 0), (-1, -1), 6),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                        ('TOPPADDING', (0, 0), (-1, -1), 6),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ]))
                    bs_elements.append(bs_table)
                    bs_elements.append(Spacer(1, 15))
                    story.append(KeepTogether(bs_elements))
                
                # ==========================================
                # IDENTITY WARNINGS TABLE
                # ==========================================
                identity_warnings_data = safe_json_parse(meeting[22])
                if identity_warnings_data and isinstance(identity_warnings_data, list) and len(identity_warnings_data) > 0:
                    iw_elements = []
                    iw_elements.append(Paragraph("✓ Identity Warnings", section_title_style))
                    
                    iw_rows = [[PH('#'), PH('Timestamp'), PH('Cycle #'), PH('Total #'), PH('Consec. Sec'), PH('Similarity'), PH('Unknown Sec'), PH('Cycle'), PH('ID Rem'), PH('Beh Rem')]]
                    for i, iw in enumerate(identity_warnings_data, 1):
                        if isinstance(iw, dict):
                            iw_rows.append([
                                P(str(i)),
                                P(format_timestamp(iw.get('timestamp', ''))),
                                P(str(iw.get('cycle_warning', iw.get('cycle_warning_number', 'N/A')))),
                                P(str(iw.get('total_warning', iw.get('total_warning_number', 'N/A')))),
                                P(str(iw.get('consecutive_seconds', 'N/A'))),
                                P(str(round(float(iw.get('similarity_score', 0)), 2)) if iw.get('similarity_score') else 'N/A'),
                                P(str(iw.get('total_unknown_seconds', 'N/A'))),
                                P(str(iw.get('removal_cycle', 'N/A'))),
                                P(str(iw.get('identity_removals', 'N/A'))),
                                P(str(iw.get('behavior_removals', 'N/A')))
                            ])
                    
                    iw_table = Table(iw_rows, colWidths=[0.35*inch, 1.1*inch, 0.5*inch, 0.5*inch, 0.65*inch, 0.65*inch, 0.7*inch, 0.5*inch, 0.5*inch, 0.55*inch], repeatRows=1)
                    iw_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9B59B6')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F5EEF8')]),
                        ('LEFTPADDING', (0, 0), (-1, -1), 3),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                        ('TOPPADDING', (0, 0), (-1, -1), 5),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                    ]))
                    iw_elements.append(iw_table)
                    iw_elements.append(Spacer(1, 15))
                    story.append(KeepTogether(iw_elements))
                
                # Page break between meetings
                if idx < len(data['meetings_data']):
                    story.append(Spacer(1, 20))
                    story.append(PageBreak())
        else:
            story.append(Paragraph("Detailed Meeting Records", main_section_style))
            story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#2C3E50'), spaceBefore=0, spaceAfter=10))
            story.append(Paragraph("No meeting records found where you attended as a participant.", report_gen.styles['Normal']))
        
        def add_page_number(canvas, doc):
            report_gen.create_header_footer(canvas, doc, "Participant Attendance Report")
        
        doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
        
        buffer.seek(0)
        response = HttpResponse(buffer.getvalue(), content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="participant_report_{user_id}_{datetime.now().strftime("%Y%m%d")}.pdf"'
        
        return response
        
    except Exception as e:
        logging.error(f"Error generating participant PDF report: {e}")
        logging.error(traceback.format_exc())
        return JsonResponse({"error": f"Failed to generate report: {str(e)}"}, status=SERVER_ERROR_STATUS)


# def get_host_report_data(host_id, start_date=None, end_date=None, meeting_time=None):
#     """
#     Helper function to get host report data with meeting time filtering
#     FIXED: Added violations JSON column to parse actual violation counts
#     """
#     try:
#         if not end_date:
#             end_date = timezone.now()
#         if not start_date:
#             start_date = end_date - timedelta(days=365)

#         meeting_time_filter = ""
#         params = [host_id, start_date, end_date]

#         if meeting_time:
#             try:
#                 meeting_dt = datetime.strptime(meeting_time, "%Y-%m-%d %H:%M")
#                 time_window_start = meeting_dt - timedelta(minutes=30)
#                 time_window_end = meeting_dt + timedelta(minutes=30)
                
#                 meeting_time_filter = """
#                     AND COALESCE(
#                         m.Started_At,
#                         sm.start_time,
#                         cm.startTime,
#                         m.Created_At
#                     ) BETWEEN %s AND %s
#                 """
#                 params.extend([time_window_start, time_window_end])
                
#                 logging.info(f"✅ Filtering host data for meeting time: {meeting_time}")
#             except Exception as e:
#                 logging.warning(f"Invalid meeting_time format: {meeting_time} ({e})")

#         with connection.cursor() as cursor:
#             # FIXED: Added violations JSON column (index 24)
#             query = f"""
#                 SELECT 
#                     m.ID as meeting_id,                              -- 0
#                     m.Meeting_Name,                                  -- 1
#                     m.Meeting_Type,                                  -- 2
#                     m.Created_At,                                    -- 3
#                     m.Started_At,                                    -- 4
#                     m.Ended_At,                                      -- 5
#                     m.Status,                                        -- 6
                    
#                     -- Participant details from tbl_Participants
#                     p.User_ID,                                       -- 7
#                     p.Full_Name,                                     -- 8
#                     p.Role,                                          -- 9
#                     p.Total_Duration_Minutes,                        -- 10
#                     p.Total_Sessions,                                -- 11
#                     p.Attendance_Percentagebasedon_host,            -- 12
#                     p.Participant_Attendance,                        -- 13
#                     p.Overall_Attendance,                            -- 14
                    
#                     -- Attendance Sessions Data
#                     ats.popup_count,                                 -- 15
#                     ats.detection_counts,                            -- 16
#                     ats.violation_start_times,                       -- 17
#                     ats.total_detections,                            -- 18
#                     ats.attendance_penalty,                          -- 19
#                     ats.break_used,                                  -- 20
#                     ats.total_break_time_used,                       -- 21
#                     ats.engagement_score,                            -- 22
#                     ats.attendance_percentage,                       -- 23
#                     ats.violations                                   -- 24 (ADDED violations JSON!)
                    
#                 FROM tbl_Meetings m
#                 LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
#                 LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
#                 LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
#                 LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
#                 WHERE m.Host_ID = %s 
#                 AND DATE(COALESCE(
#                     m.Started_At,
#                     sm.start_time,
#                     cm.startTime,
#                     m.Created_At
#                 )) BETWEEN DATE(%s) AND DATE(%s)
#                 {meeting_time_filter}
#                 ORDER BY COALESCE(
#                     m.Started_At,
#                     sm.start_time,
#                     cm.startTime,
#                     m.Created_At
#                 ) DESC, p.Full_Name
#             """
            
#             cursor.execute(query, params)
#             meetings_data = cursor.fetchall()
            
#             logging.info(f"Found {len(meetings_data)} meeting records for host {host_id}")

#             # Host summary statistics
#             cursor.execute(f"""
#                 SELECT 
#                     COUNT(DISTINCT m.ID) as total_meetings_created,
#                     COUNT(DISTINCT CASE WHEN m.Status = 'active' THEN m.ID END) as active_meetings,
#                     COUNT(DISTINCT CASE WHEN m.Status = 'ended' THEN m.ID END) as completed_meetings,
#                     COUNT(DISTINCT CASE WHEN m.Status = 'scheduled' THEN m.ID END) as scheduled_meetings,
#                     COUNT(DISTINCT p.User_ID) as total_unique_participants,
#                     AVG(p.Participant_Attendance) as avg_participant_attendance,
#                     AVG(ats.engagement_score) as avg_engagement_score,
#                     SUM(ats.total_detections) as total_violations_across_meetings
#                 FROM tbl_Meetings m
#                 LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
#                 LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
#                 LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
#                 LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
#                 WHERE m.Host_ID = %s 
#                 AND DATE(COALESCE(
#                     m.Started_At,
#                     sm.start_time,
#                     cm.startTime,
#                     m.Created_At
#                 )) BETWEEN DATE(%s) AND DATE(%s)
#                 {meeting_time_filter}
#             """, params)

#             host_stats = cursor.fetchone()

#             return {
#                 'host_id': host_id,
#                 'meetings_data': meetings_data,
#                 'host_stats': host_stats,
#                 'date_range': {
#                     'start': start_date, 
#                     'end': end_date
#                 },
#                 'meeting_time_filter': meeting_time,
#                 'total_meeting_records': len(meetings_data)
#             }

#     except Exception as e:
#         logging.error(f"Error getting host report data: {e}")
#         import traceback
#         logging.error(traceback.format_exc())
#         return None

# @require_http_methods(["GET"])
# @csrf_exempt
# def generate_host_report_pdf(request):
#     """
#     Generate comprehensive host report PDF
#     FIXED: Parse violations JSON to show actual violation counts
#     """
#     try:
#         host_id = request.GET.get('host_id') or request.GET.get('user_id') or request.GET.get('userId')
#         start_date_str = request.GET.get('start_date')
#         end_date_str = request.GET.get('end_date')
#         meeting_time_str = request.GET.get('meeting_time')

#         if not host_id:
#             return JsonResponse({"error": "host_id is required"}, status=BAD_REQUEST_STATUS)
        
#         # Parse date range
#         start_date = None
#         end_date = None
#         if start_date_str:
#             start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
#         if end_date_str:
#             end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)

#         # Get report data
#         data = get_host_report_data(host_id, start_date, end_date, meeting_time_str)
#         if not data:
#             return JsonResponse({"error": "Host not found or no data available"}, status=NOT_FOUND_STATUS)
        
#         # Get host name
#         host_name = None
#         with connection.cursor() as cursor:
#             try:
#                 cursor.execute("SELECT Full_Name FROM tbl_Users WHERE User_ID = %s LIMIT 1", [host_id])
#                 host_info = cursor.fetchone()
#                 host_name = host_info[0] if host_info else None
#             except Exception as e:
#                 logging.warning(f"Could not fetch host name from tbl_Users: {e}")
#                 try:
#                     cursor.execute("SELECT DISTINCT Full_Name FROM tbl_Participants WHERE User_ID = %s LIMIT 1", [host_id])
#                     host_info = cursor.fetchone()
#                     host_name = host_info[0] if host_info else None
#                 except Exception as e2:
#                     logging.warning(f"Could not fetch host name: {e2}")
        
#         if not host_name:
#             host_name = f"Host {host_id}"
        
#         # Create PDF
#         buffer = BytesIO()
#         doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=40, rightMargin=40, topMargin=80, bottomMargin=80)
        
#         report_gen = ReportGenerator()
#         story = []
        
#         # Title
#         title = Paragraph("Host Meeting Report", report_gen.custom_styles['ReportTitle'])
#         story.append(title)
#         story.append(Spacer(1, 20))
        
#         # Host Information
#         story.append(Paragraph("Host Information", report_gen.custom_styles['SectionHeader']))
        
#         host_info_data = [
#             ['Host ID:', str(data['host_id'])],
#             ['Host Name:', host_name],
#             ['Report Period:', f"{data['date_range']['start'].strftime('%Y-%m-%d')} to {data['date_range']['end'].strftime('%Y-%m-%d')}"]
#         ]
        
#         host_table = Table(host_info_data, colWidths=[2*inch, 4.5*inch])
#         host_table.setStyle(TableStyle([
#             ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#8E44AD')),
#             ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
#             ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
#             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#             ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
#             ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
#             ('FONTSIZE', (0, 0), (-1, -1), 10),
#             ('GRID', (0, 0), (-1, -1), 1, colors.black),
#             ('LEFTPADDING', (0, 0), (-1, -1), 8),
#             ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#             ('TOPPADDING', (0, 0), (-1, -1), 6),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
#         ]))
#         story.append(host_table)
#         story.append(Spacer(1, 20))
        
#         # Host Summary Statistics
#         host_stats = data['host_stats']
#         story.append(Paragraph("Summary Statistics", report_gen.custom_styles['SectionHeader']))
        
#         summary_data = [
#             ['Summary Metric', 'Value'],
#             ['Total Meetings Created', str(int(host_stats[0] or 0))],
#             ['Active Meetings', str(int(host_stats[1] or 0))],
#             ['Completed Meetings', str(int(host_stats[2] or 0))],
#             ['Scheduled Meetings', str(int(host_stats[3] or 0))],
#             ['Total Unique Participants', str(int(host_stats[4] or 0))],
#             ['Average Participant Attendance', f"{round(float(host_stats[5] or 0), 2)}%"],
#             ['Average Engagement Score', f"{round(float(host_stats[6] or 0), 2)} / 100"],
#             ['Total Violations Across Meetings', str(int(host_stats[7] or 0))]
#         ]
        
#         summary_table = Table(summary_data, colWidths=[3.5*inch, 3*inch])
#         summary_table.setStyle(TableStyle([
#             ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#16A085')),
#             ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#             ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#             ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
#             ('FONTSIZE', (0, 0), (-1, -1), 10),
#             ('GRID', (0, 0), (-1, -1), 1, colors.grey),
#             ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#D5F4E6')]),
#             ('LEFTPADDING', (0, 0), (-1, -1), 8),
#             ('RIGHTPADDING', (0, 0), (-1, -1), 8),
#             ('TOPPADDING', (0, 0), (-1, -1), 6),
#             ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
#         ]))
#         story.append(summary_table)
#         story.append(Spacer(1, 20))
#         story.append(PageBreak())
        
#         # Detailed Meeting Records with Participants
#         story.append(Paragraph("Detailed Meeting Records with Participants", report_gen.custom_styles['SectionHeader']))
#         story.append(Spacer(1, 10))
        
#         if data['meetings_data']:
#             # Group meetings by meeting_id
#             meetings_dict = {}
#             for record in data['meetings_data']:
#                 meeting_id = record[0]
#                 if meeting_id not in meetings_dict:
#                     meetings_dict[meeting_id] = {
#                         'meeting_info': record[:7],
#                         'participants': []
#                     }
                
#                 if record[7]:  # user_id exists
#                     meetings_dict[meeting_id]['participants'].append(record[7:])
            
#             for idx, (meeting_id, meeting_data) in enumerate(meetings_dict.items(), 1):
#                 meeting_info = meeting_data['meeting_info']
#                 participants = meeting_data['participants']
                
#                 # Meeting Header
#                 meeting_title = f"Meeting {idx}: {meeting_info[1] or 'Unnamed Meeting'}"
#                 story.append(Paragraph(meeting_title, report_gen.custom_styles['SubHeader']))
#                 story.append(Spacer(1, 8))
                
#                 # Calculate total duration
#                 total_duration = 0
#                 if meeting_info[4] and meeting_info[5]:  # Started_At and Ended_At
#                     total_duration = (meeting_info[5] - meeting_info[4]).total_seconds() / 60
                
#                 # Simple Meeting Summary Box
#                 meeting_summary = [
#                     ['Meeting ID', str(meeting_info[0])],
#                     ['Host Name', host_name],
#                     ['Meeting Type', meeting_info[2] or 'N/A'],
#                     ['Started At', meeting_info[4].strftime('%Y-%m-%d %H:%M') if meeting_info[4] else 'Not Started'],
#                     ['Ended At', meeting_info[5].strftime('%Y-%m-%d %H:%M') if meeting_info[5] else 'Not Ended'],
#                     ['Total Duration', f"{round(total_duration, 2)} minutes" if total_duration > 0 else 'N/A'],
#                     ['Status', meeting_info[6] or 'N/A'],
#                     ['Total Participants', str(len(participants))]
#                 ]
                
#                 meeting_summary_table = Table(meeting_summary, colWidths=[2.2*inch, 4.3*inch])
#                 meeting_summary_table.setStyle(TableStyle([
#                     ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#34495E')),
#                     ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
#                     ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
#                     ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
#                     ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
#                     ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
#                     ('FONTSIZE', (0, 0), (-1, -1), 9),
#                     ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                     ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#                     ('LEFTPADDING', (0, 0), (-1, -1), 6),
#                     ('RIGHTPADDING', (0, 0), (-1, -1), 6),
#                     ('TOPPADDING', (0, 0), (-1, -1), 5),
#                     ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
#                 ]))
#                 story.append(meeting_summary_table)
#                 story.append(Spacer(1, 12))
                
#                 # Detailed Participant Information
#                 if participants:
#                     section_style = ParagraphStyle(
#                         'ParticipantHeader',
#                         parent=report_gen.styles['Normal'],
#                         fontSize=10,
#                         textColor=colors.HexColor('#2980B9'),
#                         spaceAfter=6,
#                         fontName='Helvetica-Bold'
#                     )
#                     story.append(Paragraph("Participant Details", section_style))
                    
#                     # Column structure after record[7:]
#                     # p[0] = User_ID (7)
#                     # p[1] = Full_Name (8)
#                     # p[2] = Role (9)
#                     # p[3] = Total_Duration_Minutes (10)
#                     # p[4] = Total_Sessions (11)
#                     # p[5] = Attendance_Percentagebasedon_host (12)
#                     # p[6] = Participant_Attendance (13)
#                     # p[7] = Overall_Attendance (14)
#                     # p[8] = popup_count (15)
#                     # p[9] = detection_counts (16)
#                     # p[10] = violation_start_times (17)
#                     # p[11] = total_detections (18)
#                     # p[12] = attendance_penalty (19)
#                     # p[13] = break_used (20)
#                     # p[14] = total_break_time_used (21)
#                     # p[15] = engagement_score (22)
#                     # p[16] = attendance_percentage (23)
#                     # p[17] = violations JSON (24)
                    
#                     participant_headers = [
#                         'Name', 'Role', 'Duration\n(min)', 'Sessions', 'Attendance\n%',
#                         'Engage', 'Penalty\n%', 'Violations', 'Breaks', 'Break\nTime'
#                     ]
#                     participant_rows = [participant_headers]
                    
#                     compact_style = ParagraphStyle('Compact', fontSize=7, leading=9, wordWrap='CJK')
                    
#                     for p in participants:
#                         # FIXED: Parse violations JSON to get actual count
#                         violations_count = 0
#                         try:
#                             # Find matching record in meetings_data
#                             for record in data['meetings_data']:
#                                 if record[0] == meeting_id and record[7] == p[0]:  # matching meeting_id and user_id
#                                     # record[24] is violations JSON
#                                     if record[24]:
#                                         violations_data = json.loads(record[24]) if isinstance(record[24], str) else record[24]
#                                         if isinstance(violations_data, dict):
#                                             warnings = len(violations_data.get('warnings', []))
#                                             detections = len(violations_data.get('detections', []))
#                                             removals = len(violations_data.get('continuous_removals', []))
#                                             violations_count = warnings + detections + removals
#                                     break
                            
#                             # Fallback to popup_count if violations JSON is empty
#                             if violations_count == 0:
#                                 violations_count = int(p[8] or 0)  # p[8] is popup_count
#                         except Exception as e:
#                             logging.warning(f"Error parsing violations: {e}")
#                             violations_count = int(p[8] or 0)  # fallback to popup_count
                        
#                         participant_rows.append([
#                             Paragraph(str(p[1] or 'N/A')[:25], compact_style),  # Full_Name
#                             p[2] or 'N/A',  # Role
#                             f"{round(float(p[3] or 0), 1)}",  # Total_Duration_Minutes
#                             str(int(p[4] or 0)),  # Total_Sessions
#                             f"{round(float(p[6] or 0), 1)}%",  # Participant_Attendance
#                             str(int(p[15] or 0)),  # engagement_score
#                             f"{round(float(p[12] or 0), 1)}",  # attendance_penalty
#                             str(violations_count),  # FIXED: actual violations count
#                             'Yes' if p[13] else 'No',  # break_used
#                             f"{int(p[14] or 0)}s"  # total_break_time_used
#                         ])
                    
#                     participant_table = Table(participant_rows, colWidths=[
#                         1.3*inch, 0.5*inch, 0.6*inch, 0.6*inch, 0.7*inch,
#                         0.5*inch, 0.6*inch, 0.7*inch, 0.5*inch, 0.6*inch
#                     ])
#                     participant_table.setStyle(TableStyle([
#                         ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2980B9')),
#                         ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
#                         ('ALIGN', (0, 0), (0, -1), 'LEFT'),
#                         ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
#                         ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
#                         ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
#                         ('FONTSIZE', (0, 0), (-1, 0), 7),
#                         ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
#                         ('FONTSIZE', (0, 1), (-1, -1), 7),
#                         ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
#                         ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EAF2F8')]),
#                         ('LEFTPADDING', (0, 0), (-1, -1), 3),
#                         ('RIGHTPADDING', (0, 0), (-1, -1), 3),
#                         ('TOPPADDING', (0, 0), (-1, -1), 4),
#                         ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
#                     ]))
#                     story.append(participant_table)
                    
#                     story.append(Spacer(1, 8))
#                     story.append(Paragraph("Note: Violations include warnings, detections, and removals", 
#                                          ParagraphStyle('Note', fontSize=7, textColor=colors.grey)))
#                 else:
#                     story.append(Paragraph("No participants recorded for this meeting.", report_gen.styles['Normal']))
                
#                 if idx < len(meetings_dict):
#                     story.append(Spacer(1, 15))
#                     story.append(PageBreak())
#         else:
#             story.append(Paragraph("No meeting records found for the selected period.", report_gen.styles['Normal']))
        
#         # Build PDF
#         def add_page_number(canvas, doc):
#             report_gen.create_header_footer(canvas, doc, "Host Meeting Report")
        
#         doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
        
#         # Prepare response
#         buffer.seek(0)
#         response = HttpResponse(buffer.getvalue(), content_type='application/pdf')
#         response['Content-Disposition'] = f'attachment; filename="host_report_{host_id}_{datetime.now().strftime("%Y%m%d")}.pdf"'
        
#         return response
        
#     except Exception as e:
#         logging.error(f"Error generating host PDF report: {e}")
#         import traceback
#         logging.error(traceback.format_exc())
#         return JsonResponse({"error": f"Failed to generate report: {str(e)}"}, status=SERVER_ERROR_STATUS)

def get_host_report_data(host_id, start_date=None, end_date=None, meeting_time=None):
    """
    Helper function to get host report data with meeting time filtering
    FIXED: 
    - Added violations JSON column to parse actual violation counts
    - Added Meeting Duration from tbl_Participants (host's duration)
    """
    try:
        if not end_date:
            end_date = timezone.now()
        if not start_date:
            start_date = end_date - timedelta(days=365)

        meeting_time_filter = ""
        params = [host_id, start_date, end_date]

        if meeting_time:
            try:
                meeting_dt = datetime.strptime(meeting_time, "%Y-%m-%d %H:%M")
                time_window_start = meeting_dt - timedelta(minutes=30)
                time_window_end = meeting_dt + timedelta(minutes=30)
                
                meeting_time_filter = """
                    AND COALESCE(
                        m.Started_At,
                        sm.start_time,
                        cm.startTime,
                        m.Created_At
                    ) BETWEEN %s AND %s
                """
                params.extend([time_window_start, time_window_end])
                
                logging.info(f"✅ Filtering host data for meeting time: {meeting_time}")
            except Exception as e:
                logging.warning(f"Invalid meeting_time format: {meeting_time} ({e})")

        with connection.cursor() as cursor:
            # FIXED: Added host's Total_Duration_Minutes as meeting duration (index 25)
            query = f"""
                SELECT 
                    m.ID as meeting_id,                              -- 0
                    m.Meeting_Name,                                  -- 1
                    m.Meeting_Type,                                  -- 2
                    m.Created_At,                                    -- 3
                    m.Started_At,                                    -- 4
                    m.Ended_At,                                      -- 5
                    m.Status,                                        -- 6
                    
                    -- Participant details from tbl_Participants
                    p.User_ID,                                       -- 7
                    p.Full_Name,                                     -- 8
                    p.Role,                                          -- 9
                    p.Total_Duration_Minutes,                        -- 10
                    p.Total_Sessions,                                -- 11
                    p.Attendance_Percentagebasedon_host,            -- 12
                    p.Participant_Attendance,                        -- 13
                    p.Overall_Attendance,                            -- 14
                    
                    -- Attendance Sessions Data
                    ats.popup_count,                                 -- 15
                    ats.detection_counts,                            -- 16
                    ats.violation_start_times,                       -- 17
                    ats.total_detections,                            -- 18
                    ats.attendance_penalty,                          -- 19
                    ats.break_used,                                  -- 20
                    ats.total_break_time_used,                       -- 21
                    ats.engagement_score,                            -- 22
                    ats.attendance_percentage,                       -- 23
                    ats.violations,                                  -- 24 (violations JSON)
                    
                    -- FIXED: Get host's Total_Duration_Minutes from tbl_Participants as meeting duration
                    (SELECT hp.Total_Duration_Minutes 
                     FROM tbl_Participants hp 
                     WHERE hp.Meeting_ID = m.ID 
                     AND hp.User_ID = %s 
                     LIMIT 1) as meeting_total_duration              -- 25
                    
                FROM tbl_Meetings m
                LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                WHERE m.Host_ID = %s 
                AND DATE(COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                )) BETWEEN DATE(%s) AND DATE(%s)
                {meeting_time_filter}
                ORDER BY COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                ) DESC, p.Full_Name
            """
            
            # FIXED: Added host_id parameter for the subquery
            query_params = [host_id] + params
            cursor.execute(query, query_params)
            meetings_data = cursor.fetchall()
            
            logging.info(f"Found {len(meetings_data)} meeting records for host {host_id}")

            # Host summary statistics
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT m.ID) as total_meetings_created,
                    COUNT(DISTINCT CASE WHEN m.Status = 'active' THEN m.ID END) as active_meetings,
                    COUNT(DISTINCT CASE WHEN m.Status = 'ended' THEN m.ID END) as completed_meetings,
                    COUNT(DISTINCT CASE WHEN m.Status = 'scheduled' THEN m.ID END) as scheduled_meetings,
                    COUNT(DISTINCT p.User_ID) as total_unique_participants,
                    AVG(p.Participant_Attendance) as avg_participant_attendance,
                    AVG(ats.engagement_score) as avg_engagement_score,
                    SUM(ats.total_detections) as total_violations_across_meetings
                FROM tbl_Meetings m
                LEFT JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                LEFT JOIN tbl_CalendarMeetings cm ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                LEFT JOIN tbl_Participants p ON m.ID = p.Meeting_ID
                LEFT JOIN tbl_Attendance_Sessions ats ON p.Meeting_ID = ats.Meeting_ID AND p.User_ID = ats.User_ID
                WHERE m.Host_ID = %s 
                AND DATE(COALESCE(
                    m.Started_At,
                    sm.start_time,
                    cm.startTime,
                    m.Created_At
                )) BETWEEN DATE(%s) AND DATE(%s)
                {meeting_time_filter}
            """, params)

            host_stats = cursor.fetchone()

            return {
                'host_id': host_id,
                'meetings_data': meetings_data,
                'host_stats': host_stats,
                'date_range': {
                    'start': start_date, 
                    'end': end_date
                },
                'meeting_time_filter': meeting_time,
                'total_meeting_records': len(meetings_data)
            }

    except Exception as e:
        logging.error(f"Error getting host report data: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return None

@require_http_methods(["GET"])
@csrf_exempt
def generate_host_report_pdf(request):
    """
    Generate comprehensive host report PDF
    FIXED: 
    - Removed 3 rows from Summary Statistics
    - Participant Details only shows role='participant' (not host)
    - Uses actual database values only
    """
    try:
        host_id = request.GET.get('host_id') or request.GET.get('user_id') or request.GET.get('userId')
        start_date_str = request.GET.get('start_date')
        end_date_str = request.GET.get('end_date')
        meeting_time_str = request.GET.get('meeting_time')

        if not host_id:
            return JsonResponse({"error": "host_id is required"}, status=BAD_REQUEST_STATUS)
        
        # Parse date range
        start_date = None
        end_date = None
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)

        # Get report data
        data = get_host_report_data(host_id, start_date, end_date, meeting_time_str)
        if not data:
            return JsonResponse({"error": "Host not found or no data available"}, status=NOT_FOUND_STATUS)
        
        # Get host name
        host_name = None
        with connection.cursor() as cursor:
            try:
                cursor.execute("SELECT Full_Name FROM tbl_Users WHERE User_ID = %s LIMIT 1", [host_id])
                host_info = cursor.fetchone()
                host_name = host_info[0] if host_info else None
            except Exception as e:
                logging.warning(f"Could not fetch host name from tbl_Users: {e}")
                try:
                    cursor.execute("SELECT DISTINCT Full_Name FROM tbl_Participants WHERE User_ID = %s LIMIT 1", [host_id])
                    host_info = cursor.fetchone()
                    host_name = host_info[0] if host_info else None
                except Exception as e2:
                    logging.warning(f"Could not fetch host name: {e2}")
        
        if not host_name:
            host_name = f"Host {host_id}"
        
        # Create PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=40, rightMargin=40, topMargin=80, bottomMargin=80)
        
        report_gen = ReportGenerator()
        story = []
        
        # Title
        title = Paragraph("Host Meeting Report", report_gen.custom_styles['ReportTitle'])
        story.append(title)
        story.append(Spacer(1, 20))
        
        # Host Information
        story.append(Paragraph("Host Information", report_gen.custom_styles['SectionHeader']))
        
        host_info_data = [
            ['Host ID:', str(data['host_id'])],
            ['Host Name:', host_name],
            ['Report Period:', f"{data['date_range']['start'].strftime('%Y-%m-%d')} to {data['date_range']['end'].strftime('%Y-%m-%d')}"]
        ]
        
        host_table = Table(host_info_data, colWidths=[2*inch, 4.5*inch])
        host_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#8E44AD')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(host_table)
        story.append(Spacer(1, 20))
        
        # =====================================================
        # FIXED: Summary Statistics - REMOVED 3 ROWS
        # =====================================================
        host_stats = data['host_stats']
        story.append(Paragraph("Summary Statistics", report_gen.custom_styles['SectionHeader']))
        
        # FIXED: Removed these 3 rows as requested:
        # - Average Participant Attendance
        # - Average Engagement Score
        # - Total Violations Across Meetings
        summary_data = [
            ['Summary Metric', 'Value'],
            ['Total Meetings Created', str(int(host_stats[0] or 0))],
            ['Active Meetings', str(int(host_stats[1] or 0))],
            ['Completed Meetings', str(int(host_stats[2] or 0))],
            ['Scheduled Meetings', str(int(host_stats[3] or 0))],
            ['Total Unique Participants', str(int(host_stats[4] or 0))]
        ]
        
        summary_table = Table(summary_data, colWidths=[3.5*inch, 3*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#16A085')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#D5F4E6')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 20))
        story.append(PageBreak())
        
        # Detailed Meeting Records with Participants
        story.append(Paragraph("Detailed Meeting Records with Participants", report_gen.custom_styles['SectionHeader']))
        story.append(Spacer(1, 10))
        
        if data['meetings_data']:
            # Group meetings by meeting_id
            meetings_dict = {}
            for record in data['meetings_data']:
                meeting_id = record[0]
                if meeting_id not in meetings_dict:
                    meetings_dict[meeting_id] = {
                        'meeting_info': record[:7],
                        'participants': []
                    }
                
                # =====================================================
                # FIXED: Only add if role='participant' (exclude host)
                # record[7] = user_id, record[9] = role
                # =====================================================
                if record[7] and record[9] == 'participant':
                    meetings_dict[meeting_id]['participants'].append(record[7:])
            
            # for idx, (meeting_id, meeting_data) in enumerate(meetings_dict.items(), 1):
            #     meeting_info = meeting_data['meeting_info']
            #     participants = meeting_data['participants']
                
            #     # Meeting Header
            #     meeting_title = f"Meeting {idx}: {meeting_info[1] or 'Unnamed Meeting'}"
            #     story.append(Paragraph(meeting_title, report_gen.custom_styles['SubHeader']))
            #     story.append(Spacer(1, 8))
                
            #     # Calculate total duration
            #     total_duration = 0
            #     if meeting_info[4] and meeting_info[5]:  # Started_At and Ended_At
            #         total_duration = (meeting_info[5] - meeting_info[4]).total_seconds() / 60
                
            #     # Meeting Summary Box
            #     meeting_summary = [
            #         ['Meeting ID', str(meeting_info[0])],
            #         ['Host Name', host_name],
            #         ['Meeting Type', meeting_info[2] if meeting_info[2] else 'N/A'],
            #         ['Started At', meeting_info[4].strftime('%Y-%m-%d %H:%M') if meeting_info[4] else 'Not Started'],
            #         ['Ended At', meeting_info[5].strftime('%Y-%m-%d %H:%M') if meeting_info[5] else 'Not Ended'],
            #         ['Total Duration', f"{round(total_duration, 2)} minutes" if total_duration > 0 else 'N/A'],
            #         ['Status', meeting_info[6] if meeting_info[6] else 'N/A'],
            #         ['Total Participants', str(len(participants))]
            #     ]
                
            #     meeting_summary_table = Table(meeting_summary, colWidths=[2.2*inch, 4.3*inch])
            #     meeting_summary_table.setStyle(TableStyle([
            #         ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#34495E')),
            #         ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
            #         ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
            #         ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            #         ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            #         ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            #         ('FONTSIZE', (0, 0), (-1, -1), 9),
            #         ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            #         ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            #         ('LEFTPADDING', (0, 0), (-1, -1), 6),
            #         ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            #         ('TOPPADDING', (0, 0), (-1, -1), 5),
            #         ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            #     ]))
            #     story.append(meeting_summary_table)
            #     story.append(Spacer(1, 12))

            for idx, (meeting_id, meeting_data) in enumerate(meetings_dict.items(), 1):
                meeting_info = meeting_data['meeting_info']
                participants = meeting_data['participants']
                
                # Meeting Header
                meeting_title = f"Meeting {idx}: {meeting_info[1] or 'Unnamed Meeting'}"
                story.append(Paragraph(meeting_title, report_gen.custom_styles['SubHeader']))
                story.append(Spacer(1, 8))
                
                # =====================================================
                # FIXED: Get total duration from tbl_Participants (index 25)
                # This is the host's Total_Duration_Minutes from database
                # =====================================================
                meeting_duration = None
                for record in data['meetings_data']:
                    if record[0] == meeting_id:
                        meeting_duration = record[25]  # Host's Total_Duration_Minutes from tbl_Participants
                        break
                
                # Meeting Summary Box
                meeting_summary = [
                    ['Meeting ID', str(meeting_info[0])],
                    ['Host Name', host_name],
                    ['Meeting Type', meeting_info[2] if meeting_info[2] else 'N/A'],
                    ['Started At', meeting_info[4].strftime('%Y-%m-%d %H:%M') if meeting_info[4] else 'Not Started'],
                    ['Ended At', meeting_info[5].strftime('%Y-%m-%d %H:%M') if meeting_info[5] else 'Not Ended'],
                    # FIXED: Use Total_Duration_Minutes from tbl_Participants (host's duration)
                    ['Total Duration', f"{round(float(meeting_duration), 2)} minutes" if meeting_duration is not None else 'N/A'],
                    ['Status', meeting_info[6] if meeting_info[6] else 'N/A'],
                    ['Total Participants', str(len(participants))]
                ]
                
                meeting_summary_table = Table(meeting_summary, colWidths=[2.2*inch, 4.3*inch])
                meeting_summary_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#34495E')),
                    ('TEXTCOLOR', (0, 0), (0, -1), colors.whitesmoke),
                    ('TEXTCOLOR', (1, 0), (1, -1), colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                    ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 5),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ]))
                story.append(meeting_summary_table)
                story.append(Spacer(1, 12))
                            
                # =====================================================
                # FIXED: Participant Details - Only participants, no host
                # Shows actual database values only
                # =====================================================
                if participants:
                    section_style = ParagraphStyle(
                        'ParticipantHeader',
                        parent=report_gen.styles['Normal'],
                        fontSize=10,
                        textColor=colors.HexColor('#2980B9'),
                        spaceAfter=6,
                        fontName='Helvetica-Bold'
                    )
                    story.append(Paragraph("Participant Details", section_style))
                    
                    # Column mapping after record[7:]:
                    # p[0] = User_ID (index 7)
                    # p[1] = Full_Name (index 8)
                    # p[2] = Role (index 9) - filtered for 'participant' only
                    # p[3] = Total_Duration_Minutes (index 10)
                    # p[4] = Total_Sessions (index 11)
                    # p[5] = Attendance_Percentagebasedon_host (index 12)
                    # p[6] = Participant_Attendance (index 13)
                    # p[7] = Overall_Attendance (index 14)
                    # p[8] = popup_count (index 15)
                    # p[9] = detection_counts (index 16)
                    # p[10] = violation_start_times (index 17)
                    # p[11] = total_detections (index 18)
                    # p[12] = attendance_penalty (index 19)
                    # p[13] = break_used (index 20)
                    # p[14] = total_break_time_used (index 21)
                    # p[15] = engagement_score (index 22)
                    # p[16] = attendance_percentage (index 23)
                    # p[17] = violations JSON (index 24)
                    
                    # KEPT Role column to show actual database value
                    participant_headers = [
                        'Name', 'Role', 'Duration\n(min)', 'Sessions', 'Attendance\n%',
                        'Engage', 'Penalty\n%', 'Violations', 'Breaks', 'Break\nTime'
                    ]
                    participant_rows = [participant_headers]
                    
                    compact_style = ParagraphStyle('Compact', fontSize=7, leading=9, wordWrap='CJK')
                    
                    for p in participants:
                        # Parse violations JSON to get actual count
                        violations_count = 0
                        try:
                            for record in data['meetings_data']:
                                if record[0] == meeting_id and record[7] == p[0]:
                                    if record[24]:  # violations JSON
                                        violations_data = json.loads(record[24]) if isinstance(record[24], str) else record[24]
                                        if isinstance(violations_data, dict):
                                            warnings = len(violations_data.get('warnings', []))
                                            detections = len(violations_data.get('detections', []))
                                            removals = len(violations_data.get('continuous_removals', []))
                                            violations_count = warnings + detections + removals
                                    break
                            
                            # Fallback to popup_count if violations JSON is empty
                            if violations_count == 0 and p[8] is not None:
                                violations_count = int(p[8])
                        except Exception as e:
                            logging.warning(f"Error parsing violations: {e}")
                            if p[8] is not None:
                                violations_count = int(p[8])
                        
                        # =====================================================
                        # FIXED: Use actual database values - no defaults
                        # If value is None in DB, show empty string
                        # If value exists in DB (including 0), show it
                        # =====================================================
                        participant_rows.append([
                            Paragraph(str(p[1]) if p[1] is not None else '', compact_style),  # Full_Name - actual value
                            str(p[2]) if p[2] is not None else '',  # Role - actual value from DB
                            f"{round(float(p[3]), 1)}" if p[3] is not None else '',  # Duration
                            str(int(p[4])) if p[4] is not None else '',  # Sessions
                            f"{round(float(p[6]), 1)}%" if p[6] is not None else '',  # Attendance %
                            str(int(p[15])) if p[15] is not None else '',  # Engagement
                            f"{round(float(p[12]), 1)}" if p[12] is not None else '',  # Penalty
                            str(violations_count) if violations_count > 0 else '0',  # Violations - show 0 if none
                            'Yes' if p[13] else 'No' if p[13] is not None else '',  # Breaks
                            f"{int(p[14])}s" if p[14] is not None else '0s'  # Break Time
                        ])
                    
                    # Original column widths (10 columns including Role)
                    participant_table = Table(participant_rows, colWidths=[
                        1.3*inch, 0.5*inch, 0.6*inch, 0.6*inch, 0.7*inch,
                        0.5*inch, 0.6*inch, 0.7*inch, 0.5*inch, 0.6*inch
                    ])
                    participant_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2980B9')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 7),
                        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                        ('FONTSIZE', (0, 1), (-1, -1), 7),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#EAF2F8')]),
                        ('LEFTPADDING', (0, 0), (-1, -1), 3),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                        ('TOPPADDING', (0, 0), (-1, -1), 4),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                    ]))
                    story.append(participant_table)
                    
                    story.append(Spacer(1, 8))
                    story.append(Paragraph("Note: Violations include warnings, detections, and removals", 
                                         ParagraphStyle('Note', fontSize=7, textColor=colors.grey)))
                else:
                    story.append(Paragraph("No participants recorded for this meeting.", report_gen.styles['Normal']))
                
                if idx < len(meetings_dict):
                    story.append(Spacer(1, 15))
                    story.append(PageBreak())
        else:
            story.append(Paragraph("No meeting records found for the selected period.", report_gen.styles['Normal']))
        
        # Build PDF
        def add_page_number(canvas, doc):
            report_gen.create_header_footer(canvas, doc, "Host Meeting Report")
        
        doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
        
        # Prepare response
        buffer.seek(0)
        response = HttpResponse(buffer.getvalue(), content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="host_report_{host_id}_{datetime.now().strftime("%Y%m%d")}.pdf"'
        
        return response
        
    except Exception as e:
        logging.error(f"Error generating host PDF report: {e}")
        logging.error(traceback.format_exc())
        return JsonResponse({"error": f"Failed to generate report: {str(e)}"}, status=SERVER_ERROR_STATUS)
  
@require_http_methods(["GET"])
@csrf_exempt
def get_participant_report_preview(request):
    """
    Get participant report data in JSON format for preview before generating PDF
    """
    try:
        user_id = request.GET.get('user_id') or request.GET.get('userId')
        start_date_str = request.GET.get('start_date')
        end_date_str = request.GET.get('end_date')
        meeting_time_str = request.GET.get('meeting_time')

        if not user_id:
            return JsonResponse({"error": "user_id is required"}, status=BAD_REQUEST_STATUS)
        
        # Parse date range
        start_date = None
        end_date = None
        
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)

        # Get report data
        data = get_participant_report_data(user_id, start_date, end_date)
        if not data:
            return JsonResponse({"error": "Participant not found or no data available"}, status=NOT_FOUND_STATUS)
        
        # Format response data
        response_data = {
            "participant_info": data['participant_info'],
            "overall_stats": {
                "total_meetings": int(data['overall_stats'][0] or 0),
                "avg_participant_attendance": round(float(data['overall_stats'][1] or 0), 2),
                "avg_overall_attendance": round(float(data['overall_stats'][2] or 0), 2),
                "total_duration_minutes": round(float(data['overall_stats'][3] or 0), 2),
                "avg_engagement_score": round(float(data['overall_stats'][4] or 0), 2),
                "avg_penalty": round(float(data['overall_stats'][5] or 0), 2),
                "total_break_time": round(float(data['overall_stats'][6] or 0), 2),
                "total_violations": int(data['overall_stats'][7] or 0)
            },
            "date_range": {
                "start": data['date_range']['start'].isoformat(),
                "end": data['date_range']['end'].isoformat()
            },
            "total_meetings_count": len(data['meetings_data'])
        }
        
        return JsonResponse({"data": response_data}, status=SUCCESS_STATUS)
        
    except Exception as e:
        logging.error(f"Error getting participant report preview: {e}")
        return JsonResponse({"error": f"Failed to get preview: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def get_host_report_preview(request):
    """
    Get host report data in JSON format for preview before generating PDF
    """
    try:
        host_id = request.GET.get('host_id') or request.GET.get('user_id') or request.GET.get('userId')
        start_date_str = request.GET.get('start_date')
        end_date_str = request.GET.get('end_date')
        meeting_time_str = request.GET.get('meeting_time')

        if not host_id:
            return JsonResponse({"error": "host_id is required"}, status=BAD_REQUEST_STATUS)
        
        # Parse date range
        start_date = None
        end_date = None

        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)

        # Get report data
        data = get_host_report_data(host_id, start_date, end_date, meeting_time_str)
        if not data:
            return JsonResponse({"error": "Host not found or no data available"}, status=NOT_FOUND_STATUS)
        
        # Count unique meetings
        unique_meetings = set()
        for record in data['meetings_data']:
            unique_meetings.add(record[0])  # meeting_id
        
        # Format response data
        response_data = {
            "host_id": data['host_id'],
            "host_stats": {
                "total_meetings_created": int(data['host_stats'][0] or 0),
                "active_meetings": int(data['host_stats'][1] or 0),
                "completed_meetings": int(data['host_stats'][2] or 0),
                "scheduled_meetings": int(data['host_stats'][3] or 0),
                "total_unique_participants": int(data['host_stats'][4] or 0),
                "avg_participant_attendance": round(float(data['host_stats'][5] or 0), 2),
                "avg_engagement_score": round(float(data['host_stats'][6] or 0), 2),
                "total_violations": int(data['host_stats'][7] or 0)
            },
            "date_range": {
                "start": data['date_range']['start'].isoformat(),
                "end": data['date_range']['end'].isoformat()
            },
            "total_meetings_count": len(unique_meetings),
            "total_records_count": len(data['meetings_data'])
        }
        
        return JsonResponse({"data": response_data}, status=SUCCESS_STATUS)
        
    except Exception as e:
        logging.error(f"Error getting host report preview: {e}")
        return JsonResponse({"error": f"Failed to get preview: {str(e)}"}, status=SERVER_ERROR_STATUS)

# @require_http_methods(["GET"])
# @csrf_exempt
# def get_available_meeting_times(request):
#     """
#     Fixed meeting-time dropdown:
#     - Includes start date AND end date
#     - Includes dates in between
#     - Uses real meeting start time (Instant / Scheduled / Calendar)
#     - Participant view returns only attended meetings
#     - Host view returns only hosted meetings
#     - Host view NO DUPLICATES and NO 0-participant ghost rows
#     """
#     try:
#         user_id = request.GET.get("user_id") or request.GET.get("userId")
#         start_date = request.GET.get("start_date")
#         end_date = request.GET.get("end_date")
#         role_type = request.GET.get("role_type", "participant")

#         if not user_id or not start_date:
#             return JsonResponse({"error": "user_id and start_date required"}, status=400)

#         # Convert to DATE objects only
#         start_date_only = datetime.strptime(start_date, "%Y-%m-%d").date()
#         end_date_only = (
#             datetime.strptime(end_date, "%Y-%m-%d").date()
#             if end_date
#             else start_date_only
#         )

#         meeting_times = []

#         with connection.cursor() as cursor:

#             # ======================================================================
#             # PARTICIPANT VIEW  — (KEEP EXACT SAME LOGIC)
#             # ======================================================================
#             if role_type == "participant":
#                 cursor.execute("""
#                     SELECT DISTINCT
#                         m.ID AS meeting_id,
#                         m.Meeting_Name,
#                         m.Meeting_Type,
#                         COALESCE(
#                             m.Started_At,
#                             sm.start_time,
#                             cm.startTime,
#                             m.Created_At
#                         ) AS meeting_time,
#                         p.Total_Duration_Minutes,
#                         DATE(
#                             COALESCE(
#                                 m.Started_At,
#                                 sm.start_time,
#                                 cm.startTime,
#                                 m.Created_At
#                             )
#                         ) AS meeting_date
#                     FROM tbl_Participants p
#                     JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
#                     LEFT JOIN tbl_ScheduledMeetings sm 
#                            ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
#                     LEFT JOIN tbl_CalendarMeetings cm 
#                            ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
#                     WHERE p.User_ID = %s
#                       AND p.Role = 'participant'
#                       AND DATE(
#                             COALESCE(
#                                 m.Started_At,
#                                 sm.start_time,
#                                 cm.startTime,
#                                 m.Created_At
#                             )
#                           ) BETWEEN %s AND %s
#                     ORDER BY meeting_time DESC
#                 """, [user_id, start_date_only, end_date_only])

#                 rows = cursor.fetchall()

#                 for row in rows:
#                     meeting_time = row[3]
#                     if not meeting_time:
#                         continue
#                     type_display = {
#                         "InstantMeeting": "Instant",
#                         "ScheduleMeeting": "Scheduled",
#                         "CalendarMeeting": "Calendar"
#                     }.get(row[2], row[2])

#                     duration = f"{int(row[4])}m" if row[4] else "0m"

#                     meeting_times.append({
#                         "meeting_id": row[0],
#                         "meeting_name": row[1],
#                         "meeting_type": row[2],
#                         "date": row[5].isoformat(),
#                         "time": meeting_time.strftime("%H:%M"),
#                         "display_time": meeting_time.strftime("%I:%M %p"),
#                         "datetime_for_filter": meeting_time.strftime("%Y-%m-%d %H:%M"),
#                         "label": f"{meeting_time.strftime('%I:%M %p')} - {row[1]} ({type_display}) - {duration}",
#                         "role": "participant",
#                     })

#             # ======================================================================
#             # HOST VIEW — FULLY FIXED (NO DUPLICATE MEETINGS)
#             # ======================================================================
#             else:
#                 cursor.execute("""
#                     SELECT
#                         m.ID AS meeting_id,
#                         m.Meeting_Name,
#                         m.Meeting_Type,
#                         COALESCE(
#                             m.Started_At,
#                             sm.start_time,
#                             cm.startTime,
#                             m.Created_At
#                         ) AS meeting_time,

#                         (
#                             SELECT COUNT(DISTINCT p2.User_ID)
#                             FROM tbl_Participants p2
#                             WHERE p2.Meeting_ID = m.ID
#                         ) AS participant_count,

#                         DATE(
#                             COALESCE(
#                                 m.Started_At,
#                                 sm.start_time,
#                                 cm.startTime,
#                                 m.Created_At
#                             )
#                         ) AS meeting_date

#                     FROM tbl_Meetings m
#                     LEFT JOIN tbl_ScheduledMeetings sm 
#                            ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
#                     LEFT JOIN tbl_CalendarMeetings cm 
#                            ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'

#                     WHERE m.Host_ID = %s
#                       AND DATE(
#                             COALESCE(
#                                 m.Started_At,
#                                 sm.start_time,
#                                 cm.startTime,
#                                 m.Created_At
#                             )
#                         ) BETWEEN %s AND %s

#                     ORDER BY meeting_time DESC
#                 """, [user_id, start_date_only, end_date_only])

#                 rows = cursor.fetchall()

#                 for row in rows:
#                     meeting_time = row[3]
#                     if not meeting_time:
#                         continue

#                     type_display = {
#                         "InstantMeeting": "Instant",
#                         "ScheduleMeeting": "Scheduled",
#                         "CalendarMeeting": "Calendar"
#                     }.get(row[2], row[2])

#                     meeting_times.append({
#                         "meeting_id": row[0],
#                         "meeting_name": row[1],
#                         "meeting_type": row[2],
#                         "date": row[5].isoformat(),
#                         "time": meeting_time.strftime("%H:%M"),
#                         "display_time": meeting_time.strftime("%I:%M %p"),
#                         "datetime_for_filter": meeting_time.strftime("%Y-%m-%d %H:%M"),
#                         "label": f"{meeting_time.strftime('%I:%M %p')} - {row[1]} ({type_display}) - {row[4]} participants",
#                         "role": "host",
#                     })

#         return JsonResponse({
#             "success": True,
#             "data": meeting_times,
#             "count": len(meeting_times)
#         })

#     except Exception as e:
#         logging.error(f"Error: {e}")
#         return JsonResponse({"error": str(e)}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_available_meeting_times(request):
    """
    FIXED: Meeting-time dropdown with DATE displayed
    - Shows date + time in label
    - Groups meetings by date with count
    - Participant view returns only attended meetings
    - Host view returns only hosted meetings
    """
    try:
        user_id = request.GET.get("user_id") or request.GET.get("userId")
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")
        role_type = request.GET.get("role_type", "participant")

        if not user_id or not start_date:
            return JsonResponse({"error": "user_id and start_date required"}, status=400)

        # Convert to DATE objects only
        start_date_only = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_only = (
            datetime.strptime(end_date, "%Y-%m-%d").date()
            if end_date
            else start_date_only
        )

        meeting_times = []
        date_meeting_counts = {}  # Track meetings per date

        with connection.cursor() as cursor:

            # ======================================================================
            # PARTICIPANT VIEW
            # ======================================================================
            if role_type == "participant":
                cursor.execute("""
                    SELECT DISTINCT
                        m.ID AS meeting_id,
                        m.Meeting_Name,
                        m.Meeting_Type,
                        COALESCE(
                            m.Started_At,
                            sm.start_time,
                            cm.startTime,
                            m.Created_At
                        ) AS meeting_time,
                        p.Total_Duration_Minutes,
                        DATE(
                            COALESCE(
                                m.Started_At,
                                sm.start_time,
                                cm.startTime,
                                m.Created_At
                            )
                        ) AS meeting_date
                    FROM tbl_Participants p
                    JOIN tbl_Meetings m ON p.Meeting_ID = m.ID
                    LEFT JOIN tbl_ScheduledMeetings sm 
                           ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm 
                           ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'
                    WHERE p.User_ID = %s
                      AND p.Role = 'participant'
                      AND DATE(
                            COALESCE(
                                m.Started_At,
                                sm.start_time,
                                cm.startTime,
                                m.Created_At
                            )
                          ) BETWEEN %s AND %s
                    ORDER BY meeting_time DESC
                """, [user_id, start_date_only, end_date_only])

                rows = cursor.fetchall()

                # Count meetings per date
                for row in rows:
                    meeting_date = row[5]
                    if meeting_date:
                        date_str = meeting_date.isoformat()
                        date_meeting_counts[date_str] = date_meeting_counts.get(date_str, 0) + 1

                for row in rows:
                    meeting_time = row[3]
                    meeting_date = row[5]
                    if not meeting_time:
                        continue
                    
                    type_display = {
                        "InstantMeeting": "Instant",
                        "ScheduleMeeting": "Scheduled",
                        "CalendarMeeting": "Calendar"
                    }.get(row[2], row[2])

                    duration = f"{int(row[4])}m" if row[4] else "0m"
                    
                    # FIXED: Include date in label
                    date_display = meeting_time.strftime("%b %d")  # e.g., "Nov 27"
                    time_display = meeting_time.strftime("%I:%M %p")  # e.g., "06:50 PM"
                    
                    # Count how many meetings on this date
                    date_count = date_meeting_counts.get(meeting_date.isoformat(), 1) if meeting_date else 1

                    meeting_times.append({
                        "meeting_id": row[0],
                        "meeting_name": row[1],
                        "meeting_type": row[2],
                        "date": meeting_date.isoformat() if meeting_date else None,
                        "date_display": date_display,
                        "time": meeting_time.strftime("%H:%M"),
                        "display_time": time_display,
                        "datetime_for_filter": meeting_time.strftime("%Y-%m-%d %H:%M"),
                        "full_datetime": meeting_time.isoformat(),
                        "meetings_on_date": date_count,
                        # FIXED: Label now includes date
                        "label": f"{date_display} | {time_display} - {row[1]} ({type_display}) - {duration}",
                        "role": "participant",
                    })

            # ======================================================================
            # HOST VIEW
            # ======================================================================
            else:
                cursor.execute("""
                    SELECT
                        m.ID AS meeting_id,
                        m.Meeting_Name,
                        m.Meeting_Type,
                        COALESCE(
                            m.Started_At,
                            sm.start_time,
                            cm.startTime,
                            m.Created_At
                        ) AS meeting_time,

                        (
                            SELECT COUNT(DISTINCT p2.User_ID)
                            FROM tbl_Participants p2
                            WHERE p2.Meeting_ID = m.ID
                        ) AS participant_count,

                        DATE(
                            COALESCE(
                                m.Started_At,
                                sm.start_time,
                                cm.startTime,
                                m.Created_At
                            )
                        ) AS meeting_date

                    FROM tbl_Meetings m
                    LEFT JOIN tbl_ScheduledMeetings sm 
                           ON m.ID = sm.id AND m.Meeting_Type = 'ScheduleMeeting'
                    LEFT JOIN tbl_CalendarMeetings cm 
                           ON m.ID = cm.ID AND m.Meeting_Type = 'CalendarMeeting'

                    WHERE m.Host_ID = %s
                      AND DATE(
                            COALESCE(
                                m.Started_At,
                                sm.start_time,
                                cm.startTime,
                                m.Created_At
                            )
                        ) BETWEEN %s AND %s

                    ORDER BY meeting_time DESC
                """, [user_id, start_date_only, end_date_only])

                rows = cursor.fetchall()

                # Count meetings per date for host
                for row in rows:
                    meeting_date = row[5]
                    if meeting_date:
                        date_str = meeting_date.isoformat()
                        date_meeting_counts[date_str] = date_meeting_counts.get(date_str, 0) + 1

                for row in rows:
                    meeting_time = row[3]
                    meeting_date = row[5]
                    if not meeting_time:
                        continue

                    type_display = {
                        "InstantMeeting": "Instant",
                        "ScheduleMeeting": "Scheduled",
                        "CalendarMeeting": "Calendar"
                    }.get(row[2], row[2])

                    # FIXED: Include date in label
                    date_display = meeting_time.strftime("%b %d")  # e.g., "Nov 27"
                    time_display = meeting_time.strftime("%I:%M %p")  # e.g., "06:50 PM"
                    
                    # Count how many meetings hosted on this date
                    date_count = date_meeting_counts.get(meeting_date.isoformat(), 1) if meeting_date else 1

                    meeting_times.append({
                        "meeting_id": row[0],
                        "meeting_name": row[1],
                        "meeting_type": row[2],
                        "date": meeting_date.isoformat() if meeting_date else None,
                        "date_display": date_display,
                        "time": meeting_time.strftime("%H:%M"),
                        "display_time": time_display,
                        "datetime_for_filter": meeting_time.strftime("%Y-%m-%d %H:%M"),
                        "full_datetime": meeting_time.isoformat(),
                        "meetings_on_date": date_count,
                        "participant_count": row[4],
                        # FIXED: Label now includes date
                        "label": f"{date_display} | {time_display} - {row[1]} ({type_display}) - {row[4]} participants",
                        "role": "host",
                    })

        # Calculate date summary (how many meetings per date)
        date_summary = []
        for date_str, count in sorted(date_meeting_counts.items(), reverse=True):
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                date_summary.append({
                    "date": date_str,
                    "date_display": date_obj.strftime("%b %d, %Y"),
                    "day_of_week": date_obj.strftime("%A"),
                    "meeting_count": count
                })
            except:
                pass

        return JsonResponse({
            "success": True,
            "data": meeting_times,
            "count": len(meeting_times),
            "date_summary": date_summary,  # NEW: Summary of meetings per date
            "date_range": {
                "start": start_date_only.isoformat(),
                "end": end_date_only.isoformat()
            }
        })

    except Exception as e:
        logging.error(f"Error in get_available_meeting_times: {e}")
        logging.error(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)


# URL patterns
urlpatterns = [
    # Comprehensive Analytics Endpoints
    path('api/analytics/comprehensive', get_comprehensive_meeting_analytics, name='get_comprehensive_meeting_analytics'),
    path('api/analytics/participant/duration', get_participant_meeting_duration_analytics, name='get_participant_meeting_duration_analytics'),
    path('api/analytics/participant/attendance', get_participant_attendance_analytics, name='get_participant_attendance_analytics'),
    path('api/analytics/host/meeting-counts', get_host_meeting_count_analytics, name='get_host_meeting_count_analytics'),
    
    # Enhanced Existing Endpoints
    path('api/analytics/host/overview', get_host_dashboard_overview, name='get_host_dashboard_overview'),
    path('api/reports/participant/pdf', generate_participant_report_pdf, name='generate_participant_report_pdf'),
    path('api/reports/host/pdf', generate_host_report_pdf, name='generate_host_report_pdf'),
    path('api/analytics/meeting-times', get_available_meeting_times, name='get_available_meeting_times'),
    # Report Previews (JSON data)
    path('api/reports/participant/preview', get_participant_report_preview, name='get_participant_report_preview'),
    path('api/reports/host/preview', get_host_report_preview, name='get_host_report_preview'),
]