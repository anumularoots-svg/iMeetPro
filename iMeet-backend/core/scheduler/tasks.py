from celery import shared_task
import logging
from .recurring_scheduler import update_recurring_meetings, cleanup_old_meetings
from .email_scheduler import send_daily_invitation_emails, send_daily_meeting_reminders

@shared_task
def update_recurring_meetings_task():
    """Celery task to update recurring meetings"""
    try:
        logging.info("Starting Celery task: update_recurring_meetings")
        result = update_recurring_meetings()
        logging.info(f"Celery task completed: {result}")
        return result
    except Exception as e:
        logging.error(f"Celery task failed: {e}")
        return {'success': False, 'error': str(e)}

@shared_task
def send_daily_invitations_task():
    """Celery task to send daily invitations"""
    try:
        logging.info("Starting Celery task: send_daily_invitations")
        result = send_daily_invitation_emails()
        logging.info(f"Daily invitations sent: {result}")
        return {'invitations_sent': result}
    except Exception as e:
        logging.error(f"Daily invitations task failed: {e}")
        return {'invitations_sent': 0, 'error': str(e)}

@shared_task
def send_meeting_reminders_task():
    """Celery task to send meeting reminders"""
    try:
        logging.info("Starting Celery task: send_meeting_reminders")
        result = send_daily_meeting_reminders()
        logging.info(f"Meeting reminders sent: {result}")
        return {'reminders_sent': result}
    except Exception as e:
        logging.error(f"Meeting reminders task failed: {e}")
        return {'reminders_sent': 0, 'error': str(e)}

@shared_task
def cleanup_old_meetings_task():
    """Celery task to cleanup old meetings"""
    try:
        logging.info("Starting Celery task: cleanup_old_meetings")
        result = cleanup_old_meetings()
        logging.info(f"Old meetings cleaned up: {result}")
        return {'archived_count': result}
    except Exception as e:
        logging.error(f"Cleanup task failed: {e}")
        return {'archived_count': 0, 'error': str(e)}

@shared_task
def process_all_recurring_meetings():
    """Combined task to process all recurring meeting operations"""
    try:
        logging.info("Starting combined recurring meetings processing")
        
        # Update meetings
        update_result = update_recurring_meetings()
        
        # Send notifications
        invitations_result = send_daily_invitation_emails()
        reminders_result = send_daily_meeting_reminders()
        
        # Cleanup (only on weekends)
        from core.utils.date_utils import get_current_ist_datetime
        current_time = get_current_ist_datetime()
        cleanup_result = 0
        if current_time.weekday() == 6:  # Sunday
            cleanup_result = cleanup_old_meetings()
        
        combined_result = {
            'update_result': update_result,
            'invitations_sent': invitations_result,
            'reminders_sent': reminders_result,
            'archived_count': cleanup_result,
            'processed_at': current_time.isoformat()
        }
        
        logging.info(f"Combined processing completed: {combined_result}")
        return combined_result
        
    except Exception as e:
        logging.error(f"Combined processing failed: {e}")
        return {'success': False, 'error': str(e)}