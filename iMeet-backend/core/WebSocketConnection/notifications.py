# Complete Fixed Backend Notification System
# Replace your existing notification methods with these
import json
import logging
import uuid
from datetime import datetime, timedelta
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import connection, transaction
from django.utils import timezone
import pytz
# from .meetings import Create_Calendar_Meeting as _create_calendar_meeting
# from .meetings import Create_Schedule_Meeting as _create_schedule_meeting

def short_id():
    return uuid.uuid4().hex[:20]

def ensure_notification_tables():
    """Create notification tables with proper constraints using VARCHAR(20) IDs."""
    try:
        with connection.cursor() as cursor:

            # Check if tbl_Users exists (optional safety)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = 'tbl_Users'
            """)
            users_table_exists = cursor.fetchone()[0] > 0

            # -------------------------------
            # CREATE tbl_Notifications
            # -------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_Notifications (
                    id VARCHAR(20) NOT NULL,
                    recipient_email VARCHAR(255) NOT NULL,
                    meeting_id VARCHAR(20),
                    notification_type VARCHAR(50) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    message TEXT,
                    meeting_title VARCHAR(255),
                    start_time DATETIME,
                    meeting_url VARCHAR(500),
                    is_read BOOLEAN DEFAULT FALSE,
                    priority VARCHAR(20) DEFAULT 'normal',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    PRIMARY KEY (id),
                    INDEX idx_recipient_email (recipient_email),
                    INDEX idx_meeting_id (meeting_id),
                    INDEX idx_created_at (created_at),
                    INDEX idx_is_read (is_read)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """)

            # Add FK to Meetings if not exists
            try:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM information_schema.table_constraints
                    WHERE table_schema = DATABASE()
                    AND table_name = 'tbl_Notifications'
                    AND constraint_name = 'FK_Notifications_Meetings'
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute("""
                        ALTER TABLE tbl_Notifications
                        ADD CONSTRAINT FK_Notifications_Meetings
                        FOREIGN KEY (meeting_id) REFERENCES tbl_Meetings(ID)
                        ON DELETE CASCADE
                    """)
            except Exception as e:
                logging.warning(f"Could not add FK to tbl_Notifications: {e}")

            # -------------------------------
            # CREATE tbl_ScheduledReminders
            # -------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_ScheduledReminders (
                    id VARCHAR(20) NOT NULL,
                    meeting_id VARCHAR(20) NOT NULL,
                    recipient_email VARCHAR(255) NOT NULL,
                    reminder_time DATETIME NOT NULL,
                    notification_data TEXT,
                    is_sent BOOLEAN DEFAULT FALSE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    PRIMARY KEY (id),
                    INDEX idx_reminder_time (reminder_time),
                    INDEX idx_meeting_id (meeting_id),
                    INDEX idx_is_sent (is_sent)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """)

            logging.info("✅ Notification tables created or verified successfully")

    except Exception as e:
        logging.error(f"❌ Failed to create notification tables: {e}")
        raise

def create_meeting_notifications(meeting_id, meeting_title, participant_emails, start_time, meeting_url):
    """
    Create in-app notifications for meeting participants.
    Shows customized messages for Calendar and Schedule meetings.
    """
    if not participant_emails or not meeting_id:
        logging.warning("No participant emails or meeting ID provided for notifications")
        return {"sent": 0, "failed": 0}

    try:
        ensure_notification_tables()
    except Exception as e:
        logging.error(f"Failed to ensure notification tables: {e}")
        return {"sent": 0, "failed": len(participant_emails)}

    ist_timezone = pytz.timezone("Asia/Kolkata")
    current_time = datetime.now(ist_timezone)
    sent, failed = 0, 0

    # Fetch meeting type + host name
    meeting_type, host_name = "Meeting", None
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT m.Meeting_Type, u.Full_Name 
                FROM tbl_Meetings m
                LEFT JOIN tbl_Users u ON m.Host_ID = u.ID
                WHERE m.ID = %s LIMIT 1
            """, [meeting_id])
            row = cursor.fetchone()
            if row:
                meeting_type = (row[0] or "Meeting").strip()
                host_name = row[1]
    except Exception as e:
        logging.warning(f"Could not fetch meeting type or host name: {e}")

    # Create notifications
    for email in participant_emails:
        if not email or '@' not in email:
            failed += 1
            continue

        try:
            with connection.cursor() as cursor:
                # notification_id = str(uuid.uuid4())
                notification_id = short_id()

                # Dynamic message based on meeting type
                if meeting_type.lower() == "calendarmeeting":
                    notification_type = "calendar_meeting_invitation"
                    title = f"Calendar Meeting Invitation: {meeting_title}"
                    message = (
                        f"You’ve been invited to a calendar meeting by "
                        f"{host_name or 'your host'}. Please check your calendar for meeting details."
                    )
                elif meeting_type.lower() == "schedulemeeting":
                    notification_type = "scheduled_meeting_invitation"
                    title = f"Scheduled Meeting Invitation: {meeting_title}"
                    message = (
                        f"You’ve been invited to a scheduled meeting by "
                        f"{host_name or 'your host'}. View details on your schedule page."
                    )
                else:
                    notification_type = "meeting_invitation"
                    title = f"Meeting Invitation: {meeting_title}"
                    message = f'You have been invited to join "{meeting_title}"'

                cursor.execute("""
                    INSERT INTO tbl_Notifications (
                        id, recipient_email, meeting_id, notification_type, title, message,
                        meeting_title, start_time, meeting_url, is_read, priority, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, [
                    notification_id, email.strip(), str(meeting_id), notification_type,
                    title, message, meeting_title, start_time, meeting_url,
                    False, 'high', current_time
                ])

                if cursor.rowcount > 0:
                    sent += 1
                else:
                    failed += 1

        except Exception as e:
            logging.error(f"Failed to create participant notification for {email}: {e}")
            failed += 1

    logging.info(f"📨 Participant notifications created: {sent} sent, {failed} failed")
    return {"sent": sent, "failed": failed}


def _get_host_email_by_id(host_id):
    """Lookup host's email from tbl_Users with proper error handling"""
    if not host_id:
        return None
        
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT Email FROM tbl_Users WHERE ID=%s", [host_id])
            row = cursor.fetchone()
            return row[0].strip() if row and row[0] else None
    except Exception as e:
        logging.warning(f"Could not fetch host email for {host_id}: {e}")
        return None

def create_host_notification(meeting_id, meeting_title, host_email, start_time, meeting_url):
    """Create notification for the meeting host with custom messages for Calendar and Scheduled meetings"""
    if not host_email or '@' not in host_email:
        logging.warning("Invalid or missing host email for notification")
        return

    try:
        ensure_notification_tables()
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        # notification_id = str(uuid.uuid4())
        notification_id = short_id()

        # Determine meeting type
        meeting_type = "Meeting"
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT Meeting_Type FROM tbl_Meetings WHERE ID = %s LIMIT 1", [meeting_id])
                row = cursor.fetchone()
                if row and row[0]:
                    meeting_type = row[0]
        except Exception as e:
            logging.warning(f"Could not fetch meeting type for {meeting_id}: {e}")

        # Custom messages
        if meeting_type.lower() == "calendarmeeting":
            notification_type = "calendar_meeting_created"
            title = f"Calendar meeting created: {meeting_title}"
            message = "Calendar meeting created successfully. Invitations have been sent to all participants."
        elif meeting_type.lower() == "schedulemeeting":
            notification_type = "scheduled_meeting_created"
            title = f"Scheduled meeting created: {meeting_title}"
            message = "Scheduled meeting created successfully. Invitations sent to all participants."
        else:
            notification_type = "meeting_created"
            title = f"Meeting created: {meeting_title}"
            message = f'Your meeting "{meeting_title}" was created successfully.'

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO tbl_Notifications (
                    id, recipient_email, meeting_id, notification_type, title, message,
                    meeting_title, start_time, meeting_url, is_read, priority, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                notification_id, host_email.strip(), str(meeting_id), notification_type,
                title, message, meeting_title, start_time, meeting_url,
                False, 'normal', now
            ])

            if cursor.rowcount > 0:
                logging.info(f"✅ Created host notification ({notification_type}) for {host_email}")
            else:
                logging.error(f"⚠️ Failed to create host notification for {host_email}")

    except Exception as e:
        logging.error(f"❌ Failed to insert host notification: {e}", exc_info=True)


def schedule_meeting_reminders(meeting_id, meeting_title, participant_emails, start_time, meeting_url, reminder_minutes=[15, 5]):
    """Schedule reminder notifications with enhanced error handling"""
    if not participant_emails or not meeting_id or not start_time:
        logging.warning("Missing required data for scheduling reminders")
        return 0
    
    try:
        # Ensure notification tables exist
        ensure_notification_tables()
    except Exception as e:
        logging.error(f"Failed to ensure notification tables for reminders: {e}")
        return 0
    
    ist_timezone = pytz.timezone("Asia/Kolkata")
    
    # Parse start_time
    try:
        if isinstance(start_time, str):
            start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            start_dt = ist_timezone.localize(start_dt)
        else:
            start_dt = start_time
    except Exception as e:
        logging.error(f"Failed to parse start_time for reminders: {e}")
        return 0
    
    scheduled_count = 0
    
    for reminder_min in reminder_minutes:
        reminder_dt = start_dt - timedelta(minutes=reminder_min)
        
        # Don't schedule reminders for past times
        if reminder_dt <= datetime.now(ist_timezone):
            continue
        
        for email in participant_emails:
            if not email or '@' not in email:
                continue
                
            try:
                reminder_data = {
                    'meeting_id': str(meeting_id),
                    'recipient_email': email.strip(),
                    'meeting_title': meeting_title,
                    'start_time': start_time if isinstance(start_time, str) else start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'meeting_url': meeting_url,
                    'reminder_minutes': reminder_min
                }
                
                with connection.cursor() as cursor:
                    # reminder_id = str(uuid.uuid4())
                    reminder_id = short_id()
                    cursor.execute("""
                        INSERT INTO tbl_ScheduledReminders (
                            id, meeting_id, recipient_email, reminder_time, notification_data, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, [
                        reminder_id, str(meeting_id), email.strip(), reminder_dt, 
                        json.dumps(reminder_data), datetime.now(ist_timezone)
                    ])
                    
                    if cursor.rowcount > 0:
                        scheduled_count += 1
                
            except Exception as e:
                logging.error(f"Failed to schedule reminder for {email}: {e}")
    
    logging.info(f"Scheduled {scheduled_count} reminders for meeting {meeting_id}")
    return scheduled_count

# MAINTENANCE FUNCTIONS

def cleanup_old_notifications(days_old=30):
    """Remove old read notifications to keep database clean"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        with connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM tbl_Notifications 
                WHERE created_at < %s AND is_read = TRUE
            """, [cutoff_date])
            
            deleted_count = cursor.rowcount
            logging.info(f"Cleaned up {deleted_count} old notifications")
            
        return deleted_count
        
    except Exception as e:
        logging.error(f"Failed to cleanup old notifications: {e}")
        return 0

def process_scheduled_reminders():
    """Process scheduled reminders that are due"""
    try:
        ensure_notification_tables()
        
        ist_timezone = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist_timezone)
        
        with connection.cursor() as cursor:
            # Get due reminders
            cursor.execute("""
                SELECT id, meeting_id, recipient_email, notification_data
                FROM tbl_ScheduledReminders
                WHERE reminder_time <= %s AND is_sent = FALSE
                ORDER BY reminder_time
                LIMIT 100
            """, [current_time])
            
            reminders = cursor.fetchall()
            processed = 0
            
            for reminder in reminders:
                reminder_id, meeting_id, recipient_email, notification_data_str = reminder
                
                try:
                    notification_data = json.loads(notification_data_str)
                    # notification_id = str(uuid.uuid4())
                    notification_id = short_id()
                    # Create notification
                    cursor.execute("""
                        INSERT INTO tbl_Notifications (
                            id, recipient_email, meeting_id, notification_type, title, message,
                            meeting_title, start_time, meeting_url, is_read, priority, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        notification_id, recipient_email, meeting_id, 'meeting_reminder',
                        f'Meeting Reminder: {notification_data["meeting_title"]}',
                        f'Your meeting "{notification_data["meeting_title"]}" starts in {notification_data["reminder_minutes"]} minutes',
                        notification_data["meeting_title"],
                        notification_data["start_time"],
                        notification_data["meeting_url"],
                        False, 'high', current_time
                    ])
                    
                    # Mark reminder as sent
                    cursor.execute("""
                        UPDATE tbl_ScheduledReminders 
                        SET is_sent = TRUE 
                        WHERE id = %s
                    """, [reminder_id])
                    
                    processed += 1
                    
                except Exception as e:
                    logging.error(f"Failed to process reminder {reminder_id}: {e}")
            
            logging.info(f"Processed {processed} scheduled reminders")
            return processed
            
    except Exception as e:
        logging.error(f"Failed to process scheduled reminders: {e}")
        return 0
def calculate_time_ago(created_at):
    """Calculate time ago string for notifications"""
    try:
        now = datetime.now(pytz.timezone("Asia/Kolkata"))
        
        if isinstance(created_at, str):
            created_dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_dt = pytz.timezone("Asia/Kolkata").localize(created_dt)
        else:
            created_dt = created_at
            if created_dt.tzinfo is None:
                created_dt = pytz.timezone("Asia/Kolkata").localize(created_dt)
        
        diff = now - created_dt
        
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} min ago"
        else:
            return "Just now"
    except:
        return "Unknown"

# @require_http_methods(["GET"])
# @csrf_exempt
# def get_user_notifications(request):
#     """
#     Get notifications for a user with optional page-based filtering
#     """
#     try:
#         email = request.GET.get('email', '').strip()
#         page = request.GET.get('page', 'all').lower()
        
#         try:
#             limit = min(int(request.GET.get('limit', 20)), 100)
#             offset = max(int(request.GET.get('offset', 0)), 0)
#         except (ValueError, TypeError):
#             limit = 20
#             offset = 0
        
#         logging.info(f"📧 Getting notifications for email: {email}, page: {page}, limit: {limit}, offset: {offset}")
        
#         if not email or '@' not in email or len(email) < 5:
#             return JsonResponse({
#                 "Error": "Valid email address is required",
#                 "notifications": [],
#                 "unread_count": 0
#             }, status=400)
        
#         ensure_notification_tables()
        
#         with connection.cursor() as cursor:
#             # Build query based on page filter
#             if page == 'schedule':
#                 query = """
#                     SELECT 
#                         n.id, n.notification_type, n.title, n.message, n.meeting_id,
#                         n.meeting_title, n.meeting_url, n.start_time, n.is_read,
#                         n.priority, n.created_at,
#                         CASE 
#                             WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(DAY, n.created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
#                             WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(HOUR, n.created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
#                             WHEN TIMESTAMPDIFF(MINUTE, n.created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(MINUTE, n.created_at, NOW()), ' min ago')
#                             ELSE 'Just now'
#                         END as time_ago
#                     FROM tbl_Notifications n
#                     LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
#                     WHERE n.recipient_email = %s 
#                       AND (m.Meeting_Type = 'ScheduleMeeting' OR n.notification_type LIKE '%%schedule%%')
#                     ORDER BY n.created_at DESC 
#                     LIMIT %s OFFSET %s
#                 """
#                 count_query = """
#                     SELECT COUNT(*) 
#                     FROM tbl_Notifications n
#                     LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
#                     WHERE n.recipient_email = %s 
#                       AND n.is_read = FALSE
#                       AND (m.Meeting_Type = 'ScheduleMeeting' OR n.notification_type LIKE '%%schedule%%')
#                 """
                
#             elif page == 'calendar':
#                 query = """
#                     SELECT 
#                         n.id, n.notification_type, n.title, n.message, n.meeting_id,
#                         n.meeting_title, n.meeting_url, n.start_time, n.is_read,
#                         n.priority, n.created_at,
#                         CASE 
#                             WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(DAY, n.created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
#                             WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(HOUR, n.created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
#                             WHEN TIMESTAMPDIFF(MINUTE, n.created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(MINUTE, n.created_at, NOW()), ' min ago')
#                             ELSE 'Just now'
#                         END as time_ago
#                     FROM tbl_Notifications n
#                     LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
#                     WHERE n.recipient_email = %s 
#                       AND (m.Meeting_Type = 'CalendarMeeting' OR n.notification_type LIKE '%%calendar%%')
#                     ORDER BY n.created_at DESC 
#                     LIMIT %s OFFSET %s
#                 """
#                 count_query = """
#                     SELECT COUNT(*) 
#                     FROM tbl_Notifications n
#                     LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
#                     WHERE n.recipient_email = %s 
#                       AND n.is_read = FALSE
#                       AND (m.Meeting_Type = 'CalendarMeeting' OR n.notification_type LIKE '%%calendar%%')
#                 """
                
#             elif page == 'recording':
#                 query = """
#                     SELECT 
#                         id, notification_type, title, message, meeting_id,
#                         meeting_title, meeting_url, start_time, is_read,
#                         priority, created_at,
#                         CASE 
#                             WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(DAY, created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
#                             WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(HOUR, created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
#                             WHEN TIMESTAMPDIFF(MINUTE, created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(MINUTE, created_at, NOW()), ' min ago')
#                             ELSE 'Just now'
#                         END as time_ago
#                     FROM tbl_Notifications 
#                     WHERE recipient_email = %s 
#                       AND notification_type IN ('recording_completed', 'recording_processed', 'recording_available')
#                     ORDER BY created_at DESC 
#                     LIMIT %s OFFSET %s
#                 """
#                 count_query = """
#                     SELECT COUNT(*) FROM tbl_Notifications 
#                     WHERE recipient_email = %s 
#                       AND is_read = FALSE
#                       AND notification_type IN ('recording_completed', 'recording_processed', 'recording_available')
#                 """
                
#             else:
#                 query = """
#                     SELECT 
#                         id, notification_type, title, message, meeting_id,
#                         meeting_title, meeting_url, start_time, is_read,
#                         priority, created_at,
#                         CASE 
#                             WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(DAY, created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
#                             WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(HOUR, created_at, NOW()), 
#                                 CASE WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
#                             WHEN TIMESTAMPDIFF(MINUTE, created_at, NOW()) > 0 
#                             THEN CONCAT(TIMESTAMPDIFF(MINUTE, created_at, NOW()), ' min ago')
#                             ELSE 'Just now'
#                         END as time_ago
#                     FROM tbl_Notifications 
#                     WHERE recipient_email = %s 
#                     ORDER BY created_at DESC 
#                     LIMIT %s OFFSET %s
#                 """
#                 count_query = """
#                     SELECT COUNT(*) FROM tbl_Notifications 
#                     WHERE recipient_email = %s AND is_read = FALSE
#                 """
            
#             # Execute main query
#             cursor.execute(query, [email, limit, offset])
#             rows = cursor.fetchall()
            
#             notifications = []
#             for row in rows:
#                 try:
#                     notification = {
#                         'id': str(row[0]),
#                         'type': str(row[1]),
#                         'notification_type': str(row[1]),
#                         'title': str(row[2]) if row[2] else 'Notification',
#                         'message': str(row[3]) if row[3] else '',
#                         'meeting_id': str(row[4]) if row[4] else None,
#                         'meeting_title': str(row[5]) if row[5] else None,
#                         'meeting_url': str(row[6]) if row[6] else None,
#                         'start_time': row[7].isoformat() if row[7] else None,
#                         'is_read': bool(row[8]),
#                         'priority': str(row[9]) if row[9] else 'normal',
#                         'created_at': row[10].isoformat() if row[10] else None,
#                         'time_ago': str(row[11]) if row[11] else 'Unknown'
#                     }
#                     notifications.append(notification)
#                 except Exception as e:
#                     logging.warning(f"Error processing notification row: {e}")
#                     continue
            
#             cursor.execute(count_query, [email])
#             unread_count = cursor.fetchone()[0] or 0
            
#             logging.info(f"✅ Retrieved {len(notifications)} notifications, {unread_count} unread for {email} (page: {page})")
            
#             response_data = {
#                 "notifications": notifications,
#                 "unread_count": int(unread_count),
#                 "total_count": len(notifications),
#                 "limit": limit,
#                 "offset": offset,
#                 "page": page,
#                 "success": True
#             }
            
#             return JsonResponse(response_data, status=200)
            
#     except Exception as e:
#         logging.error(f"❌ Error in get_user_notifications: {str(e)}")
#         return JsonResponse({
#             "Error": f"Failed to retrieve notifications: {str(e)}",
#             "notifications": [],
#             "unread_count": 0,
#             "success": False
#         }, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_user_notifications(request):
    """
    Get notifications for a user with optional page-based filtering.
    Automatically deletes expired meeting notifications (Calendar + Schedule) whose end_time has passed.
    """
    try:
        email = request.GET.get('email', '').strip()
        page = request.GET.get('page', 'all').lower()
        
        try:
            limit = min(int(request.GET.get('limit', 20)), 100)
            offset = max(int(request.GET.get('offset', 0)), 0)
        except (ValueError, TypeError):
            limit = 20
            offset = 0
        
        logging.info(f"📧 Getting notifications for email: {email}, page: {page}, limit: {limit}, offset: {offset}")
        
        if not email or '@' not in email or len(email) < 5:
            return JsonResponse({
                "Error": "Valid email address is required",
                "notifications": [],
                "unread_count": 0
            }, status=400)
        
        ensure_notification_tables()

        # 🧹 Auto-clean expired meeting notifications before fetching
        # try:
        #     ist = pytz.timezone("Asia/Kolkata")
        #     now = datetime.now(ist)
        #     with connection.cursor() as cleanup_cursor:
        #         # Delete expired CalendarMeeting notifications
        #         cleanup_cursor.execute("""
        #             DELETE n FROM tbl_Notifications n
        #             INNER JOIN tbl_CalendarMeetings c ON n.meeting_id = c.ID
        #             WHERE c.endTime IS NOT NULL AND c.endTime < %s
        #         """, [now])
        #         deleted_calendar = cleanup_cursor.rowcount or 0

        #         # Delete expired ScheduleMeeting notifications
        #         cleanup_cursor.execute("""
        #             DELETE n FROM tbl_Notifications n
        #             INNER JOIN tbl_ScheduledMeetings s ON n.meeting_id = s.id
        #             WHERE s.end_time IS NOT NULL AND s.end_time < %s
        #         """, [now])
        #         deleted_schedule = cleanup_cursor.rowcount or 0

        #         # Fallback: delete if tbl_Meetings.Ended_At passed
        #         cleanup_cursor.execute("""
        #             DELETE n FROM tbl_Notifications n
        #             INNER JOIN tbl_Meetings m ON n.meeting_id = m.ID
        #             WHERE m.Ended_At IS NOT NULL AND m.Ended_At < %s
        #         """, [now])
        #         deleted_meetings = cleanup_cursor.rowcount or 0

        #         total_deleted = deleted_calendar + deleted_schedule + deleted_meetings
        #         if total_deleted > 0:
        #             logging.info(f"🧹 Auto-deleted {total_deleted} expired notifications for {email}")
        # except Exception as cleanup_err:
        #     logging.warning(f"⚠️ Failed to clean expired meeting notifications: {cleanup_err}")

                # 🧹 Auto-clean expired meeting notifications before fetching
        try:
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            with connection.cursor() as cleanup_cursor:
                # Delete notifications for ended Calendar meetings
                cleanup_cursor.execute("""
                    DELETE n FROM tbl_Notifications n
                    INNER JOIN tbl_CalendarMeetings c ON n.meeting_id = c.ID
                    WHERE c.endTime IS NOT NULL AND c.endTime < %s
                """, [now])
                deleted_calendar = cleanup_cursor.rowcount or 0

                # Delete notifications for ended Scheduled meetings
                cleanup_cursor.execute("""
                    DELETE n FROM tbl_Notifications n
                    INNER JOIN tbl_ScheduledMeetings s ON n.meeting_id = s.id
                    WHERE s.end_time IS NOT NULL AND s.end_time < %s
                """, [now])
                deleted_schedule = cleanup_cursor.rowcount or 0

                # Delete notifications for ended meetings in tbl_Meetings
                cleanup_cursor.execute("""
                    DELETE n FROM tbl_Notifications n
                    INNER JOIN tbl_Meetings m ON n.meeting_id = m.ID
                    WHERE m.Ended_At IS NOT NULL AND m.Ended_At < %s
                """, [now])
                deleted_main = cleanup_cursor.rowcount or 0

                total_deleted = deleted_calendar + deleted_schedule + deleted_main
                if total_deleted > 0:
                    logging.info(f"🧹 Cleaned up {total_deleted} expired notifications (meeting ended).")
        except Exception as cleanup_err:
            logging.warning(f"⚠️ Failed to auto-clean expired meeting notifications: {cleanup_err}")

        with connection.cursor() as cursor:
            # Build query based on page filter
            if page == 'schedule':
                query = """
                    SELECT 
                        n.id, n.notification_type, n.title, n.message, n.meeting_id,
                        n.meeting_title, n.meeting_url, n.start_time, n.is_read,
                        n.priority, n.created_at,
                        CASE 
                            WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(DAY, n.created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
                            WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(HOUR, n.created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
                            WHEN TIMESTAMPDIFF(MINUTE, n.created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(MINUTE, n.created_at, NOW()), ' min ago')
                            ELSE 'Just now'
                        END as time_ago
                    FROM tbl_Notifications n
                    LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
                    WHERE n.recipient_email = %s 
                      AND (m.Meeting_Type = 'ScheduleMeeting' OR n.notification_type LIKE '%%schedule%%')
                    ORDER BY n.created_at DESC 
                    LIMIT %s OFFSET %s
                """
                count_query = """
                    SELECT COUNT(*) 
                    FROM tbl_Notifications n
                    LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
                    WHERE n.recipient_email = %s 
                      AND n.is_read = FALSE
                      AND (m.Meeting_Type = 'ScheduleMeeting' OR n.notification_type LIKE '%%schedule%%')
                """
                
            elif page == 'calendar':
                query = """
                    SELECT 
                        n.id, n.notification_type, n.title, n.message, n.meeting_id,
                        n.meeting_title, n.meeting_url, n.start_time, n.is_read,
                        n.priority, n.created_at,
                        CASE 
                            WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(DAY, n.created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(DAY, n.created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
                            WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(HOUR, n.created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(HOUR, n.created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
                            WHEN TIMESTAMPDIFF(MINUTE, n.created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(MINUTE, n.created_at, NOW()), ' min ago')
                            ELSE 'Just now'
                        END as time_ago
                    FROM tbl_Notifications n
                    LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
                    WHERE n.recipient_email = %s 
                      AND (m.Meeting_Type = 'CalendarMeeting' OR n.notification_type LIKE '%%calendar%%')
                    ORDER BY n.created_at DESC 
                    LIMIT %s OFFSET %s
                """
                count_query = """
                    SELECT COUNT(*) 
                    FROM tbl_Notifications n
                    LEFT JOIN tbl_Meetings m ON n.meeting_id = m.ID
                    WHERE n.recipient_email = %s 
                      AND n.is_read = FALSE
                      AND (m.Meeting_Type = 'CalendarMeeting' OR n.notification_type LIKE '%%calendar%%')
                """
                
            elif page == 'recording':
                query = """
                    SELECT 
                        id, notification_type, title, message, meeting_id,
                        meeting_title, meeting_url, start_time, is_read,
                        priority, created_at,
                        CASE 
                            WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(DAY, created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
                            WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(HOUR, created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
                            WHEN TIMESTAMPDIFF(MINUTE, created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(MINUTE, created_at, NOW()), ' min ago')
                            ELSE 'Just now'
                        END as time_ago
                    FROM tbl_Notifications 
                    WHERE recipient_email = %s 
                      AND notification_type IN ('recording_completed', 'recording_processed', 'recording_available')
                    ORDER BY created_at DESC 
                    LIMIT %s OFFSET %s
                """
                count_query = """
                    SELECT COUNT(*) FROM tbl_Notifications 
                    WHERE recipient_email = %s 
                      AND is_read = FALSE
                      AND notification_type IN ('recording_completed', 'recording_processed', 'recording_available')
                """
                
            else:
                query = """
                    SELECT 
                        id, notification_type, title, message, meeting_id,
                        meeting_title, meeting_url, start_time, is_read,
                        priority, created_at,
                        CASE 
                            WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(DAY, created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(DAY, created_at, NOW()) = 1 THEN ' day ago' ELSE ' days ago' END)
                            WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(HOUR, created_at, NOW()), 
                                CASE WHEN TIMESTAMPDIFF(HOUR, created_at, NOW()) = 1 THEN ' hour ago' ELSE ' hours ago' END)
                            WHEN TIMESTAMPDIFF(MINUTE, created_at, NOW()) > 0 
                            THEN CONCAT(TIMESTAMPDIFF(MINUTE, created_at, NOW()), ' min ago')
                            ELSE 'Just now'
                        END as time_ago
                    FROM tbl_Notifications 
                    WHERE recipient_email = %s 
                    ORDER BY created_at DESC 
                    LIMIT %s OFFSET %s
                """
                count_query = """
                    SELECT COUNT(*) FROM tbl_Notifications 
                    WHERE recipient_email = %s AND is_read = FALSE
                """
            
            # Execute main query
            cursor.execute(query, [email, limit, offset])
            rows = cursor.fetchall()
            
            notifications = []
            for row in rows:
                try:
                    notification = {
                        'id': str(row[0]),
                        'type': str(row[1]),
                        'notification_type': str(row[1]),
                        'title': str(row[2]) if row[2] else 'Notification',
                        'message': str(row[3]) if row[3] else '',
                        'meeting_id': str(row[4]) if row[4] else None,
                        'meeting_title': str(row[5]) if row[5] else None,
                        'meeting_url': str(row[6]) if row[6] else None,
                        'start_time': row[7].isoformat() if row[7] else None,
                        'is_read': bool(row[8]),
                        'priority': str(row[9]) if row[9] else 'normal',
                        'created_at': row[10].isoformat() if row[10] else None,
                        'time_ago': str(row[11]) if row[11] else 'Unknown'
                    }
                    notifications.append(notification)
                except Exception as e:
                    logging.warning(f"Error processing notification row: {e}")
                    continue
            
            cursor.execute(count_query, [email])
            unread_count = cursor.fetchone()[0] or 0
            
            logging.info(f"✅ Retrieved {len(notifications)} notifications, {unread_count} unread for {email} (page: {page})")
            
            response_data = {
                "notifications": notifications,
                "unread_count": int(unread_count),
                "total_count": len(notifications),
                "limit": limit,
                "offset": offset,
                "page": page,
                "success": True
            }
            
            return JsonResponse(response_data, status=200)
            
    except Exception as e:
        logging.error(f"❌ Error in get_user_notifications: {str(e)}")
        return JsonResponse({
            "Error": f"Failed to retrieve notifications: {str(e)}",
            "notifications": [],
            "unread_count": 0,
            "success": False
        }, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_notification_count(request):
    """Get unread notification count with enhanced error handling"""
    try:
        email = request.GET.get('email', '').strip()
        
        logging.info(f"📧 Getting notification count for email: {email}")
        
        if not email or '@' not in email:
            return JsonResponse({
                "Error": "Valid email address is required",
                "unread_count": 0
            }, status=400)
        
        # Ensure notification tables exist
        ensure_notification_tables()
        
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM tbl_Notifications 
                WHERE recipient_email = %s AND is_read = FALSE
            """, [email])
            
            result = cursor.fetchone()
            unread_count = result[0] if result else 0
            
            logging.info(f"✅ Unread count for {email}: {unread_count}")
            
            return JsonResponse({
                "unread_count": int(unread_count),
                "success": True
            }, status=200)
            
    except Exception as e:
        logging.error(f"❌ Error in get_notification_count: {str(e)}")
        return JsonResponse({
            "Error": f"Failed to get notification count: {str(e)}",
            "unread_count": 0,
            "success": False
        }, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def mark_notification_as_read(request):
    """
    ✅ FINAL VERSION: Marks a single notification as read (persistent, UUID-safe, autocommit)
    Frontend sends:
    {
        "notification_id": "8216b6ce-c60c-466f-b42a-c5744cea34fb",
        "email": "user@example.com"
    }
    """
    try:
        data = json.loads(request.body.decode("utf-8")) if request.body else {}
        notification_id = str(data.get("notification_id")).strip()
        email = (data.get("email") or "").strip()

        if not notification_id or not email or "@" not in email:
            return JsonResponse({
                "success": False,
                "error": "Missing or invalid parameters (notification_id, email required)"
            }, status=400)

        ensure_notification_tables()

        # ✅ Force autocommit for single record updates
        with connection.cursor() as cursor:
            cursor.execute("SET autocommit = 1;")

            # ✅ Verify that notification exists for this user
            cursor.execute("""
                SELECT is_read FROM tbl_Notifications
                WHERE id = %s AND recipient_email = %s
                LIMIT 1
            """, [notification_id, email])
            row = cursor.fetchone()

            if not row:
                logging.warning(f"⚠️ Notification not found for {email}: {notification_id}")
                return JsonResponse({
                    "success": False,
                    "error": "Notification not found or access denied"
                }, status=404)

            was_read = bool(row[0])

            # ✅ Update only if not already read
            if not was_read:
                cursor.execute("""
                    UPDATE tbl_Notifications
                    SET is_read = TRUE
                    WHERE id = %s AND recipient_email = %s
                    LIMIT 1
                """, [notification_id, email])
                connection.commit()  # ✅ Explicit commit

                affected = cursor.rowcount
                logging.info(f"✅ Updated {affected} row(s) for notification {notification_id}")

            # ✅ Fetch updated unread count
            cursor.execute("""
                SELECT COUNT(*) FROM tbl_Notifications
                WHERE recipient_email = %s AND is_read = FALSE
            """, [email])
            unread_count = cursor.fetchone()[0] or 0

        logging.info(f"✅ Notification {notification_id} marked as read for {email}. Unread count: {unread_count}")

        return JsonResponse({
            "success": True,
            "message": "Notification marked as read successfully",
            "notification_id": notification_id,
            "unread_count": int(unread_count)
        }, status=200)

    except Exception as e:
        logging.error(f"❌ Error in mark_notification_as_read: {str(e)}", exc_info=True)
        return JsonResponse({
            "success": False,
            "error": f"Failed to mark notification as read: {str(e)}"
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def mark_all_notifications_as_read(request):
    """
    ✅ Mark all notifications as read for a user (compatible with current schema)
    Frontend sends:
    {
        "email": "user@example.com"
    }
    """
    try:
        data = json.loads(request.body.decode("utf-8")) if request.body else {}
        email = (data.get("email") or "").strip()

        if not email or "@" not in email:
            return JsonResponse({
                "success": False,
                "error": "Valid email address is required"
            }, status=400)

        ensure_notification_tables()

        with transaction.atomic():
            with connection.cursor() as cursor:
                # Count unread before
                cursor.execute("""
                    SELECT COUNT(*) FROM tbl_Notifications
                    WHERE recipient_email = %s AND is_read = FALSE
                """, [email])
                unread_before = cursor.fetchone()[0] or 0

                if unread_before == 0:
                    return JsonResponse({
                        "success": True,
                        "message": "No unread notifications to mark as read",
                        "unread_count": 0
                    }, status=200)

                # Update all unread → read
                cursor.execute("""
                    UPDATE tbl_Notifications
                    SET is_read = TRUE
                    WHERE recipient_email = %s AND is_read = FALSE
                """, [email])
                marked_count = cursor.rowcount or 0

        logging.info(f"✅ Marked {marked_count} notifications as read for {email}")

        return JsonResponse({
            "success": True,
            "message": f"Successfully marked {marked_count} notifications as read",
            "unread_count": 0
        }, status=200)

    except Exception as e:
        logging.error(f"❌ Error in mark_all_notifications_as_read: {str(e)}", exc_info=True)
        return JsonResponse({
            "success": False,
            "error": f"Failed to mark all notifications as read: {str(e)}"
        }, status=500)

@csrf_exempt
@require_http_methods(["DELETE"])
def delete_notification(request):
    """
    ✅ Delete a notification (compatible with current schema)
    Frontend sends:
    {
        "notification_id": "8216b6ce-c60c-466f-b42a-c5744cea34fb",
        "email": "user@example.com"
    }
    """
    try:
        body_unicode = request.body.decode("utf-8") if request.body else "{}"
        data = json.loads(body_unicode)
        notification_id = str(data.get("notification_id")).strip()
        email = (data.get("email") or "").strip()

        if not notification_id or not email or "@" not in email:
            return JsonResponse({
                "success": False,
                "error": "Missing or invalid parameters (notification_id, email required)"
            }, status=400)

        ensure_notification_tables()

        with transaction.atomic():
            with connection.cursor() as cursor:
                # Check existence
                cursor.execute("""
                    SELECT id, is_read FROM tbl_Notifications
                    WHERE id = %s AND recipient_email = %s
                    LIMIT 1
                """, [notification_id, email])
                row = cursor.fetchone()

                if not row:
                    return JsonResponse({
                        "success": False,
                        "error": "Notification not found or access denied"
                    }, status=404)

                cursor.execute("""
                    DELETE FROM tbl_Notifications
                    WHERE id = %s AND recipient_email = %s
                """, [notification_id, email])

                # Recount unread
                cursor.execute("""
                    SELECT COUNT(*) FROM tbl_Notifications
                    WHERE recipient_email = %s AND is_read = FALSE
                """, [email])
                unread_count = cursor.fetchone()[0] or 0

        logging.info(f"✅ Notification {notification_id} deleted for {email}")

        return JsonResponse({
            "success": True,
            "message": "Notification deleted successfully",
            "notification_id": notification_id,
            "unread_count": int(unread_count)
        }, status=200)

    except Exception as e:
        logging.error(f"❌ Error in delete_notification: {str(e)}", exc_info=True)
        return JsonResponse({
            "success": False,
            "error": f"Failed to delete notification: {str(e)}"
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def process_reminder_notifications(request):
    """Process scheduled reminders - call this via cron job every minute"""
    try:
        current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        processed_count = 0
        failed_count = 0
        
        with connection.cursor() as cursor:
            # Get reminders that should be sent now
            cursor.execute("""
                SELECT id, notification_data FROM tbl_ScheduledReminders
                WHERE reminder_time <= %s AND is_sent = FALSE
            """, [current_time.strftime('%Y-%m-%d %H:%M:%S')])
            
            reminders = cursor.fetchall()
            
            for reminder_id, notification_data_json in reminders:
                try:
                    data = json.loads(notification_data_json)
                    
                    # Create reminder notification
                    # cursor.execute("""
                    #     INSERT INTO tbl_Notifications (
                    #         recipient_email, meeting_id, notification_type,
                    #         title, message, meeting_title, start_time, meeting_url,
                    #         is_read, priority, created_at
                    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    # """, [
                    #     data['recipient_email'], 
                    #     data['meeting_id'],
                    #     'meeting_reminder', 
                    #     'Meeting reminder',
                    #     f"{data['meeting_title']} starts in {data['reminder_minutes']} minutes",
                    #     data['meeting_title'], 
                    #     data['start_time'], 
                    #     data['meeting_url'],
                    #     False,  # is_read
                    #     'high',  # priority
                    #     current_time
                    # ])

                    notification_id = short_id()
                    cursor.execute("""
                        INSERT INTO tbl_Notifications (
                            id, recipient_email, meeting_id, notification_type,
                            title, message, meeting_title, start_time, meeting_url,
                            is_read, priority, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        notification_id,
                        data['recipient_email'],
                        data['meeting_id'],
                        'meeting_reminder',
                        'Meeting reminder',
                        f"{data['meeting_title']} starts in {data['reminder_minutes']} minutes",
                        data['meeting_title'],
                        data['start_time'],
                        data['meeting_url'],
                        False,
                        'high',
                        current_time
                    ])

                    
                    # Mark reminder as sent
                    cursor.execute("""
                        UPDATE tbl_ScheduledReminders SET is_sent = TRUE WHERE id = %s
                    """, [reminder_id])
                    
                    processed_count += 1
                    logging.info(f"Processed reminder for meeting {data['meeting_id']}")
                    
                except Exception as e:
                    logging.error(f"Failed to process reminder {reminder_id}: {e}")
                    failed_count += 1
        
        return JsonResponse({
            "Message": "Reminder notifications processed",
            "processed": processed_count,
            "failed": failed_count,
            "total_reminders": len(reminders) if 'reminders' in locals() else 0
        })
        
    except Exception as e:
        logging.error(f"Failed to process reminder notifications: {e}")
        return JsonResponse({"Error": str(e)}, status=500)

# FIXED: Create test notification for debugging
@require_http_methods(["POST"])
@csrf_exempt
def debug_create_test_notification(request):
    """Debug endpoint to create test notifications with proper validation"""
    try:
        data = json.loads(request.body.decode('utf-8'))
        email = data.get('email', '').strip()
        title = data.get('title', 'Test Notification').strip()
        message = data.get('message', 'This is a test notification to verify the system works').strip()
        notification_type = data.get('type', 'test_notification').strip()
        priority = data.get('priority', 'normal').strip()
        
        if not email or '@' not in email:
            return JsonResponse({
                "Error": "Valid email address is required",
                "success": False
            }, status=400)
        
        # Ensure notification tables exist
        ensure_notification_tables()
        
        ist_timezone = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist_timezone)
        
        with connection.cursor() as cursor:
            # notification_id = str(uuid.uuid4())
            notification_id = short_id()
            cursor.execute("""
                INSERT INTO tbl_Notifications (
                    id, recipient_email, notification_type, title, message,
                    is_read, priority, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                notification_id, email, notification_type, title, message,
                False, priority, current_time
            ])
            
            if cursor.rowcount == 0:
                return JsonResponse({
                    "Error": "Failed to create test notification",
                    "success": False
                }, status=500)
            
        logging.info(f"✅ Created test notification {notification_id} for {email}")
        
        return JsonResponse({
            "message": "Test notification created successfully",
            "notification_id": notification_id,
            "email": email,
            "title": title,
            "success": True
        }, status=201)
        
    except json.JSONDecodeError as e:
        logging.error(f"❌ Invalid JSON in debug_create_test_notification: {e}")
        return JsonResponse({
            "Error": "Invalid JSON format in request body",
            "success": False
        }, status=400)
    except Exception as e:
        logging.error(f"❌ Error creating test notification: {str(e)}")
        return JsonResponse({
            "Error": f"Failed to create test notification: {str(e)}",
            "success": False
        }, status=500)