import logging
from datetime import datetime, timedelta
from django.db import connection, transaction
from core.utils.date_utils import get_current_ist_datetime, format_datetime_for_db
from core.utils.recurring_calculator import (
    calculate_next_occurrence, 
    get_todays_meetings, 
    is_recurrence_ended,
    should_send_reminder
)
from .email_scheduler import send_daily_meeting_reminders

def update_recurring_meetings():
    """
    Main function to update recurring meetings ONLY after current meetings have ended
    """
    logging.info("Starting daily recurring meetings update...")
    
    try:
        current_time = get_current_ist_datetime()
        
        # Get all active recurring meetings
        recurring_meetings = get_active_recurring_meetings()
        
        updated_count = 0
        ended_count = 0
        
        for meeting in recurring_meetings:
            try:
                # Check if recurrence has ended
                if is_recurrence_ended(meeting):
                    mark_meeting_recurrence_ended(meeting['id'])
                    ended_count += 1
                    logging.info(f"Marked recurring meeting {meeting['id']} as ended")
                    continue
                
                # CRITICAL FIX: Only update if current meeting has ended
                if should_update_to_next_occurrence(meeting, current_time):
                    next_occurrence = calculate_next_occurrence(meeting, current_time)
                    
                    if next_occurrence:
                        success = update_meeting_to_next_occurrence(meeting, next_occurrence)
                        if success:
                            updated_count += 1
                            logging.info(f"Updated meeting {meeting['id']} to next occurrence")
                
            except Exception as e:
                logging.error(f"Error processing meeting {meeting.get('id', 'unknown')}: {e}")
                continue
        
        # Send daily reminders (unchanged)
        reminder_count = send_daily_meeting_reminders()
        
        logging.info(f"""
        Daily recurring meetings update completed:
        - Meetings updated to next occurrence: {updated_count}
        - Meetings ended: {ended_count}
        - Reminders sent: {reminder_count}
        """)
        
        return {
            'success': True,
            'updated_count': updated_count,
            'ended_count': ended_count,
            'reminder_count': reminder_count
        }
        
    except Exception as e:
        logging.error(f"Error in update_recurring_meetings: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def should_update_to_next_occurrence(meeting, current_time):
    """
    FIXED: Only return True if the current meeting has completely ended
    This ensures meetings show until their actual end time, not just after start time
    """
    try:
        # Get current meeting end time
        current_end_time = parse_datetime_safely(meeting.get('end_time'))
        if not current_end_time:
            logging.warning(f"No end time for meeting {meeting.get('id')}")
            return False
        
        # CRITICAL FIX: Only update AFTER the meeting has completely ended
        # Add a small buffer (e.g., 5 minutes) to ensure meeting is truly finished
        buffer_minutes = 5
        meeting_truly_ended = current_time > (current_end_time + timedelta(minutes=buffer_minutes))
        
        if meeting_truly_ended:
            logging.info(f"Meeting {meeting.get('id')} has ended (with buffer), ready for next occurrence update")
            return True
        else:
            # Meeting is still active, in progress, or just recently ended
            time_until_end = current_end_time - current_time
            if time_until_end.total_seconds() > 0:
                logging.debug(f"Meeting {meeting.get('id')} is still active. Ends in {time_until_end}")
            else:
                logging.debug(f"Meeting {meeting.get('id')} recently ended, waiting for buffer period")
            return False
            
    except Exception as e:
        logging.error(f"Error checking if meeting should update: {e}")
        return False

def update_meeting_to_next_occurrence(meeting, next_occurrence):
    """
    FIXED: Update meeting to the calculated next occurrence
    """
    try:
        meeting_id = meeting['id']
        next_start = next_occurrence['next_start_time']
        next_end = next_occurrence['next_end_time']
        
        # Parse datetime strings
        from core.utils.date_utils import parse_datetime_safely
        start_datetime = parse_datetime_safely(next_start)
        end_datetime = parse_datetime_safely(next_end)
        
        if not start_datetime:
            logging.error(f"Failed to parse start time for meeting {meeting_id}")
            return False
        
        current_time = get_current_ist_datetime()
        
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Update both tables with next occurrence times
                cursor.execute("""
                    UPDATE tbl_Meetings 
                    SET Started_At = %s, Ended_At = %s, Status = 'scheduled'
                    WHERE ID = %s
                """, [
                    format_datetime_for_db(start_datetime),
                    format_datetime_for_db(end_datetime),
                    meeting_id
                ])
                
                cursor.execute("""
                    UPDATE tbl_ScheduledMeetings 
                    SET start_time = %s, end_time = %s
                    WHERE id = %s
                """, [
                    format_datetime_for_db(start_datetime),
                    format_datetime_for_db(end_datetime),
                    meeting_id
                ])
                
                logging.info(f"Updated meeting {meeting_id} to next occurrence: {start_datetime}")
                return True
                
    except Exception as e:
        logging.error(f"Error updating meeting {meeting.get('id')} to next occurrence: {e}")
        return False

def get_active_recurring_meetings():
    """
    Get all active recurring meetings - NO extra columns needed
    """
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT 
                sm.id, sm.host_id, sm.title, sm.description, sm.location,
                sm.start_time, sm.end_time, sm.start_date, sm.end_date, sm.timezone, sm.duration_minutes,
                sm.is_recurring, sm.recurrence_type, sm.recurrence_interval,
                sm.recurrence_occurrences, sm.recurrence_end_date,
                sm.selected_days, sm.selected_month_dates, sm.monthly_pattern,
                sm.email, sm.reminders_email, sm.reminders_times,
                m.Status, m.Meeting_Link, m.Meeting_Name
            FROM tbl_ScheduledMeetings sm
            INNER JOIN tbl_Meetings m ON sm.id = m.ID
            WHERE sm.is_recurring = 1 
              AND m.Status NOT IN ('ended', 'deleted', 'cancelled', 'recurrence_ended')
              AND (
                  sm.recurrence_end_date IS NULL 
                  OR sm.recurrence_end_date >= %s
              )
            """
            
            current_date = get_current_ist_datetime().date()
            cursor.execute(query, [current_date])
            
            columns = [desc[0] for desc in cursor.description]
            meetings = []
            
            for row in cursor.fetchall():
                meeting_dict = dict(zip(columns, row))
                meetings.append(meeting_dict)
            
            logging.info(f"Retrieved {len(meetings)} active recurring meetings")
            return meetings
            
    except Exception as e:
        logging.error(f"Error getting active recurring meetings: {e}")
        return []

def get_current_active_meetings():
    """
    NEW: Get meetings that are currently active (between start and end time)
    """
    try:
        current_time = get_current_ist_datetime()
        
        with connection.cursor() as cursor:
            query = """
            SELECT 
                sm.id, sm.title, sm.start_time, sm.end_time, sm.is_recurring,
                m.Status, m.Meeting_Name
            FROM tbl_ScheduledMeetings sm
            INNER JOIN tbl_Meetings m ON sm.id = m.ID
            WHERE sm.start_time <= %s 
              AND sm.end_time >= %s
              AND m.Status = 'scheduled'
            ORDER BY sm.start_time ASC
            """
            
            cursor.execute(query, [current_time, current_time])
            
            columns = [desc[0] for desc in cursor.description]
            meetings = []
            
            for row in cursor.fetchall():
                meeting_dict = dict(zip(columns, row))
                meetings.append(meeting_dict)
            
            logging.info(f"Found {len(meetings)} currently active meetings")
            return meetings
            
    except Exception as e:
        logging.error(f"Error getting current active meetings: {e}")
        return []

def mark_meeting_recurrence_ended(meeting_id):
    """Mark a recurring meeting as ended"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE tbl_ScheduledMeetings 
                SET is_recurring = 0
                WHERE id = %s
            """, [meeting_id])
            
            cursor.execute("""
                UPDATE tbl_Meetings 
                SET Status = 'recurrence_ended'
                WHERE ID = %s
            """, [meeting_id])
            
            logging.info(f"Marked meeting {meeting_id} recurrence as ended")
            return True
            
    except Exception as e:
        logging.error(f"Error marking meeting {meeting_id} recurrence as ended: {e}")
        return False

def get_todays_scheduled_meetings():
    """
    UPDATED: Get all meetings scheduled for today, including updated recurring ones
    """
    try:
        current_date = get_current_ist_datetime().date()
        
        with connection.cursor() as cursor:
            query = """
            SELECT 
                sm.id, sm.host_id, sm.title, sm.description, sm.location,
                sm.start_time, sm.end_time, sm.duration_minutes,
                sm.is_recurring, sm.recurrence_type, sm.email, sm.reminders_email, sm.reminders_times,
                m.Meeting_Link, m.Meeting_Name, m.Status,
                u.full_name as host_name, u.email as host_email
            FROM tbl_ScheduledMeetings sm
            INNER JOIN tbl_Meetings m ON sm.id = m.ID
            LEFT JOIN tbl_Users u ON sm.host_id = u.ID
            WHERE DATE(sm.start_time) = %s
              AND m.Status IN ('scheduled', 'active')
            ORDER BY sm.start_time ASC
            """
            
            cursor.execute(query, [current_date])
            
            columns = [desc[0] for desc in cursor.description]
            meetings = []
            
            for row in cursor.fetchall():
                meeting_dict = dict(zip(columns, row))
                meetings.append(meeting_dict)
            
            logging.info(f"Retrieved {len(meetings)} meetings scheduled for today")
            return meetings
            
    except Exception as e:
        logging.error(f"Error getting today's meetings: {e}")
        return []

def process_meeting_reminders():
    """Process and send meeting reminders for today's meetings"""
    try:
        current_time = get_current_ist_datetime()
        
        # Get today's meetings
        todays_meetings = get_todays_scheduled_meetings()
        
        meetings_needing_reminders = []
        
        for meeting in todays_meetings:
            # Get reminder times from meeting data
            reminder_times_str = meeting.get('reminders_times', '[15, 5]')
            try:
                if isinstance(reminder_times_str, str):
                    reminder_times = json.loads(reminder_times_str)
                else:
                    reminder_times = reminder_times_str if reminder_times_str else [15, 5]
            except:
                reminder_times = [15, 5]
            
            # Check each reminder time
            for reminder_minutes in reminder_times:
                if should_send_reminder(meeting, reminder_minutes):
                    meetings_needing_reminders.append({
                        'meeting': meeting,
                        'reminder_minutes': reminder_minutes
                    })
        
        # Send reminders
        sent_count = 0
        for reminder_item in meetings_needing_reminders:
            try:
                success = send_meeting_reminder_email(
                    reminder_item['meeting'], 
                    reminder_item['reminder_minutes']
                )
                if success:
                    sent_count += 1
            except Exception as e:
                logging.error(f"Error sending reminder: {e}")
        
        logging.info(f"Sent {sent_count} meeting reminders")
        return sent_count
        
    except Exception as e:
        logging.error(f"Error processing meeting reminders: {e}")
        return 0

def send_meeting_reminder_email(meeting, reminder_minutes):
    """Send reminder email for a specific meeting"""
    try:
        from core.scheduler.email_scheduler import send_meeting_reminder
        return send_meeting_reminder(meeting, reminder_minutes)
    except Exception as e:
        logging.error(f"Error sending meeting reminder email: {e}")
        return False

def cleanup_old_meetings():
    """Clean up old non-recurring meetings"""
    try:
        cutoff_date = get_current_ist_datetime() - timedelta(days=30)
        
        with connection.cursor() as cursor:
            # Mark old non-recurring meetings as archived
            cursor.execute("""
                UPDATE tbl_Meetings m
                INNER JOIN tbl_ScheduledMeetings sm ON m.ID = sm.id
                SET m.Status = 'archived'
                WHERE sm.is_recurring = 0
                  AND sm.end_time < %s
                  AND m.Status NOT IN ('archived', 'deleted')
            """, [format_datetime_for_db(cutoff_date)])
            
            archived_count = cursor.rowcount
            logging.info(f"Archived {archived_count} old meetings")
            
            return archived_count
            
    except Exception as e:
        logging.error(f"Error cleaning up old meetings: {e}")
        return 0

def parse_datetime_safely(dt_str):
    """Safely parse datetime string - moved here to avoid import issues"""
    if not dt_str:
        return None
    
    try:
        if isinstance(dt_str, str):
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        elif isinstance(dt_str, datetime):
            return dt_str
        return None
    except Exception as e:
        logging.error(f"Failed to parse datetime: {dt_str} - {e}")
        return None