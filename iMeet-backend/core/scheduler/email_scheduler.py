import logging
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from django.core.mail import EmailMessage
from django.conf import settings
from core.utils.date_utils import get_current_ist_datetime, parse_datetime_safely
from core.utils.recurring_calculator import calculate_next_occurrence, should_send_reminder

def send_daily_meeting_reminders():
    """Send daily reminders for all applicable meetings"""
    try:
        from core.scheduler.recurring_scheduler import get_active_recurring_meetings, get_todays_scheduled_meetings
        
        current_time = get_current_ist_datetime()
        reminders_sent = 0
        
        # Get today's meetings (both recurring and non-recurring)
        todays_meetings = get_todays_scheduled_meetings()
        
        # Process each meeting for reminders
        for meeting in todays_meetings:
            try:
                # Check if reminders are enabled
                if not meeting.get('reminders_email', True):
                    continue
                
                # Get reminder times
                reminder_times_str = meeting.get('reminders_times', '[15, 5]')
                try:
                    if isinstance(reminder_times_str, str):
                        reminder_times = json.loads(reminder_times_str)
                    else:
                        reminder_times = reminder_times_str if reminder_times_str else [15, 5]
                except:
                    reminder_times = [15, 5]
                
                # Send reminders for each time
                for reminder_minutes in reminder_times:
                    if should_send_meeting_reminder_now(meeting, reminder_minutes, current_time):
                        success = send_meeting_reminder(meeting, reminder_minutes)
                        if success:
                            reminders_sent += 1
                            logging.info(f"Sent {reminder_minutes}-minute reminder for meeting {meeting['id']}")
                        
            except Exception as e:
                logging.error(f"Error processing reminders for meeting {meeting.get('id', 'unknown')}: {e}")
                continue
        
        logging.info(f"Daily reminders completed: {reminders_sent} reminders sent")
        return reminders_sent
        
    except Exception as e:
        logging.error(f"Error in send_daily_meeting_reminders: {e}")
        return 0

def should_send_meeting_reminder_now(meeting, reminder_minutes, current_time):
    """Check if reminder should be sent now"""
    try:
        start_time = parse_datetime_safely(meeting.get('start_time'))
        if not start_time:
            return False
        
        # Calculate reminder time
        reminder_time = start_time - timedelta(minutes=reminder_minutes)
        
        # Check if current time is within 2 minutes of reminder time
        time_diff = abs((current_time - reminder_time).total_seconds())
        
        return time_diff <= 120  # Within 2 minutes
        
    except Exception as e:
        logging.error(f"Error checking reminder time: {e}")
        return False

def send_meeting_reminder(meeting, reminder_minutes):
    """Send a reminder email for a meeting"""
    try:
        # Get participant emails
        email_str = meeting.get('email', '')
        if not email_str:
            logging.info(f"No participants to remind for meeting {meeting['id']}")
            return True
        
        participant_emails = [email.strip() for email in email_str.split(',') if email.strip()]
        if not participant_emails:
            return True
        
        # Prepare reminder email data
        start_time = parse_datetime_safely(meeting.get('start_time'))
        if not start_time:
            logging.error(f"Invalid start time for meeting {meeting['id']}")
            return False
        
        formatted_start_time = start_time.strftime('%A, %B %d, %Y at %I:%M %p')
        
        # Create reminder email
        subject = f"Meeting Reminder: {meeting.get('title', 'Scheduled Meeting')} - Starting in {reminder_minutes} minutes"
        
        meeting_link = meeting.get('Meeting_Link', '')
        meeting_id = meeting.get('id', '')
        
        message = f"""Meeting Reminder

Your meeting "{meeting.get('title', 'Scheduled Meeting')}" is starting in {reminder_minutes} minutes.

Meeting Details:
📅 Date & Time: {formatted_start_time}
⏱  Duration: {meeting.get('duration_minutes', 60)} minutes
💻 Join Link: {meeting_link}
🆔 Meeting ID: {meeting_id}

{f"📍 Location: {meeting.get('location')}" if meeting.get('location') else ""}
{f"📝 Description: {meeting.get('description')}" if meeting.get('description') else ""}

Please join on time. Click the link above or use the Meeting ID to join.

Best regards,
Meet Pro Team"""

        # Send emails asynchronously
        email_data = {
            'subject': subject,
            'message': message,
            'meeting_title': meeting.get('title', 'Scheduled Meeting'),
            'meeting_id': meeting_id
        }
        
        success_count = send_emails_to_participants(email_data, participant_emails)
        
        logging.info(f"Sent reminder to {success_count}/{len(participant_emails)} participants for meeting {meeting_id}")
        return success_count > 0
        
    except Exception as e:
        logging.error(f"Error sending meeting reminder: {e}")
        return False

def send_emails_to_participants(email_data, participant_emails):
    """Send emails to participants using threading"""
    try:
        successful_sends = 0
        
        def send_single_reminder_email(email_address):
            try:
                email_message = EmailMessage(
                    subject=email_data['subject'],
                    body=email_data['message'],
                    from_email=settings.DEFAULT_FROM_EMAIL or 'noreply@meetpro.com',
                    to=[email_address],
                )
                
                email_message.content_subtype = "plain"
                email_message.send(fail_silently=False)
                
                logging.info(f"Successfully sent reminder to: {email_address}")
                return True
                
            except Exception as e:
                logging.error(f"Failed to send reminder email to {email_address}: {e}")
                return False
        
        # Use ThreadPoolExecutor for concurrent sending
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_email = {
                executor.submit(send_single_reminder_email, email): email 
                for email in participant_emails
            }
            
            for future in future_to_email:
                try:
                    success = future.result(timeout=30)
                    if success:
                        successful_sends += 1
                except Exception as e:
                    email = future_to_email[future]
                    logging.error(f"Reminder email sending failed for {email}: {e}")
        
        return successful_sends
        
    except Exception as e:
        logging.error(f"Error in send_emails_to_participants: {e}")
        return 0

def send_daily_invitation_emails():
    """Send invitation emails for today's recurring meetings"""
    try:
        from core.scheduler.recurring_scheduler import get_todays_scheduled_meetings
        
        todays_meetings = get_todays_scheduled_meetings()
        invitations_sent = 0
        
        for meeting in todays_meetings:
            try:
                # Only send invitations for recurring meetings
                if not meeting.get('is_recurring'):
                    continue
                
                # Check if has participants
                email_str = meeting.get('email', '')
                if not email_str:
                    continue
                
                participant_emails = [email.strip() for email in email_str.split(',') if email.strip()]
                if not participant_emails:
                    continue
                
                # Send invitation email
                success = send_daily_meeting_invitation(meeting, participant_emails)
                if success:
                    invitations_sent += 1
                    logging.info(f"Sent daily invitation for recurring meeting {meeting['id']}")
                    
            except Exception as e:
                logging.error(f"Error sending daily invitation for meeting {meeting.get('id', 'unknown')}: {e}")
                continue
        
        logging.info(f"Daily invitations completed: {invitations_sent} invitations sent")
        return invitations_sent
        
    except Exception as e:
        logging.error(f"Error in send_daily_invitation_emails: {e}")
        return 0

def send_daily_meeting_invitation(meeting, participant_emails):
    """Send daily invitation email for recurring meeting"""
    try:
        start_time = parse_datetime_safely(meeting.get('start_time'))
        if not start_time:
            return False
        
        formatted_start_time = start_time.strftime('%A, %B %d, %Y at %I:%M %p')
        date_only = start_time.strftime('%Y-%m-%d')
        time_only = start_time.strftime('%H:%M')
        
        meeting_title = meeting.get('title', 'Recurring Meeting')
        meeting_link = meeting.get('Meeting_Link', '')
        meeting_id = meeting.get('id', '')
        host_name = meeting.get('host_name', 'Meeting Host')
        duration = meeting.get('duration_minutes', 60)
        
        # Create calendar link
        calendar_link = f"https://calendar.google.com/calendar/render?action=TEMPLATE&text={meeting_title.replace(' ', '+')}&dates={date_only.replace('-', '')}T{time_only.replace(':', '')}00Z/{date_only.replace('-', '')}T{time_only.replace(':', '')}00Z&details=Join+meeting:+{meeting_link}"
        
        subject = f"Today's Meeting: {meeting_title}"
        
        message = f"""Daily Meeting Reminder

You have a recurring meeting scheduled for today:

📅 Meeting: {meeting_title}
🗓  Date & Time: {formatted_start_time}
⏱  Duration: {duration} minutes
💻 Join Link: {meeting_link}
🆔 Meeting ID: {meeting_id}
👤 Host: {host_name}

{f"📍 Location: {meeting.get('location')}" if meeting.get('location') else ""}
{f"📝 Description: {meeting.get('description')}" if meeting.get('description') else ""}

Important Reminders:
- Please join 5 minutes before the scheduled time
- Ensure your camera and microphone are working
- Have a stable internet connection

Add to Calendar: {calendar_link}

This is a recurring meeting. You will receive this reminder each day the meeting is scheduled.

Best regards,
Meet Pro Team"""

        email_data = {
            'subject': subject,
            'message': message,
            'meeting_title': meeting_title,
            'meeting_id': meeting_id
        }
        
        success_count = send_emails_to_participants(email_data, participant_emails)
        
        return success_count > 0
        
    except Exception as e:
        logging.error(f"Error sending daily meeting invitation: {e}")
        return False

def send_recurring_meeting_notifications():
    """Combined function to send both invitations and reminders"""
    try:
        current_time = get_current_ist_datetime()
        current_hour = current_time.hour
        
        # Send daily invitations in the morning (8 AM)
        invitations_sent = 0
        if current_hour == 8:
            invitations_sent = send_daily_invitation_emails()
        
        # Send reminders throughout the day
        reminders_sent = send_daily_meeting_reminders()
        
        return {
            'invitations_sent': invitations_sent,
            'reminders_sent': reminders_sent,
            'total_notifications': invitations_sent + reminders_sent
        }
        
    except Exception as e:
        logging.error(f"Error in send_recurring_meeting_notifications: {e}")
        return {
            'invitations_sent': 0,
            'reminders_sent': 0,
            'total_notifications': 0,
            'error': str(e)
        }