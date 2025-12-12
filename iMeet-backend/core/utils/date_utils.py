import pytz
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import calendar
import logging

def get_ist_timezone():
    """Get IST timezone object"""
    return pytz.timezone("Asia/Kolkata")

def convert_to_ist(dt):
    """Convert datetime to IST"""
    ist_tz = get_ist_timezone()
    if dt.tzinfo is None:
        return ist_tz.localize(dt)
    return dt.astimezone(ist_tz)

def get_current_ist_datetime():
    """Get current datetime in IST"""
    return datetime.now(get_ist_timezone())

def get_current_ist_date():
    """Get current date in IST"""
    return get_current_ist_datetime().date()

def parse_datetime_safely(dt_str):
    """Safely parse datetime string"""
    if not dt_str:
        return None
    
    try:
        if isinstance(dt_str, str):
            dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            return convert_to_ist(dt)
        return dt_str
    except Exception as e:
        logging.warning(f"Failed to parse datetime {dt_str}: {e}")
        return None

def format_datetime_for_db(dt):
    """Format datetime for database storage"""
    if not dt:
        return None
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def is_same_day(dt1, dt2):
    """Check if two datetimes are on the same day"""
    if not dt1 or not dt2:
        return False
    
    dt1_date = dt1.date() if hasattr(dt1, 'date') else dt1
    dt2_date = dt2.date() if hasattr(dt2, 'date') else dt2
    
    return dt1_date == dt2_date

def days_between(start_date, end_date):
    """Calculate days between two dates"""
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    
    return (end_date - start_date).days