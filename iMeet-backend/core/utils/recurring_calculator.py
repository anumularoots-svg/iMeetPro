from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import logging
from .date_utils import get_current_ist_datetime, convert_to_ist, parse_datetime_safely

def calculate_next_occurrence(meeting_data, from_date=None):
    """
    FIXED: Calculate next occurrence, respecting active meetings
    """
    current_time = from_date or get_current_ist_datetime()
    
    start_time_str = meeting_data.get('start_time')
    end_time_str = meeting_data.get('end_time')
    
    if not start_time_str:
        return None
    
    try:
        start_time = parse_datetime_safely(start_time_str)
        end_time = parse_datetime_safely(end_time_str)
        
        if not start_time:
            return None
    except Exception as e:
        logging.error(f"Failed to parse times: {e}")
        return None
    
    # CRITICAL FIX: If meeting is currently active, return current times
    if start_time and end_time:
        if start_time <= current_time <= end_time:
            return {
                'next_start_time': start_time.isoformat(),
                'next_end_time': end_time.isoformat(),
                'is_today': start_time.date() == current_time.date(),
                'is_currently_active': True
            }
    
    # Only calculate next occurrence if meeting has completely ended
    if not meeting_data.get('is_recurring'):
        if start_time >= current_time:
            return {
                'next_start_time': start_time.isoformat(),
                'next_end_time': end_time_str,
                'is_today': start_time.date() == current_time.date(),
                'is_currently_active': False
            }
        return None
    
    # For recurring meetings, calculate next occurrence only after current one ends
    if end_time and current_time <= end_time:
        # Current occurrence is still active/valid
        return {
            'next_start_time': start_time.isoformat(),
            'next_end_time': end_time.isoformat(),
            'is_today': start_time.date() == current_time.date(),
            'is_currently_active': start_time <= current_time <= end_time
        }
    
    # Calculate actual next occurrence for recurring meetings
    recurrence_type = meeting_data.get('recurrence_type')
    interval = meeting_data.get('recurrence_interval', 1)
    
    if recurrence_type == 'daily':
        return calculate_daily_occurrence(start_time, current_time, interval)
    elif recurrence_type == 'weekly':
        return calculate_weekly_occurrence(start_time, current_time, interval, meeting_data)
    elif recurrence_type == 'monthly':
        return calculate_monthly_occurrence(start_time, current_time, interval, meeting_data)
    
    return None

def calculate_daily_occurrence(start_time, current_time, interval):
    """Calculate next daily occurrence"""
    days_diff = (current_time.date() - start_time.date()).days
    
    if days_diff < 0:
        # Meeting hasn't started yet
        next_date = start_time.date()
    else:
        # Calculate next occurrence
        cycles_passed = days_diff // interval
        next_occurrence_days = (cycles_passed + 1) * interval
        next_date = start_time.date() + timedelta(days=next_occurrence_days)
    
    # Create next datetime
    next_datetime = datetime.combine(next_date, start_time.time())
    next_datetime = convert_to_ist(next_datetime)
    
    # Calculate end time
    if start_time and hasattr(start_time, 'time'):
        duration = timedelta(hours=1)  # Default 1 hour
        next_end_datetime = next_datetime + duration
    else:
        next_end_datetime = None
    
    return {
        'next_start_time': next_datetime.isoformat(),
        'next_end_time': next_end_datetime.isoformat() if next_end_datetime else None,
        'is_today': next_date == current_time.date(),
        'is_completed_today': False
    }

def calculate_weekly_occurrence(start_time, current_time, interval, meeting_data):
    """Calculate next weekly occurrence based on selected days"""
    selected_days = meeting_data.get('selected_days', [])
    
    if isinstance(selected_days, str):
        try:
            selected_days = json.loads(selected_days)
        except:
            selected_days = []
    
    if not selected_days:
        # Default to same day of week as start_time
        selected_days = [start_time.weekday()]
    
    # Convert string day names to numbers if needed
    day_mapping = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    
    numeric_days = []
    for day in selected_days:
        if isinstance(day, str):
            numeric_days.append(day_mapping.get(day.lower(), 0))
        else:
            numeric_days.append(int(day))
    
    # Find next occurrence
    current_date = current_time.date()
    
    # Check if today is one of the selected days
    if current_date.weekday() in numeric_days:
        next_datetime = datetime.combine(current_date, start_time.time())
        next_datetime = convert_to_ist(next_datetime)
        
        if next_datetime > current_time:
            # Today's meeting hasn't happened yet
            duration = timedelta(hours=1)
            next_end_datetime = next_datetime + duration
            
            return {
                'next_start_time': next_datetime.isoformat(),
                'next_end_time': next_end_datetime.isoformat(),
                'is_today': True,
                'is_completed_today': False
            }
    
    # Find next occurrence in the coming days
    for days_ahead in range(1, 14):  # Look ahead 2 weeks
        check_date = current_date + timedelta(days=days_ahead)
        if check_date.weekday() in numeric_days:
            next_datetime = datetime.combine(check_date, start_time.time())
            next_datetime = convert_to_ist(next_datetime)
            
            duration = timedelta(hours=1)
            next_end_datetime = next_datetime + duration
            
            return {
                'next_start_time': next_datetime.isoformat(),
                'next_end_time': next_end_datetime.isoformat(),
                'is_today': False,
                'is_completed_today': False
            }
    
    return None

def calculate_monthly_occurrence(start_time, current_time, interval, meeting_data):
    """Calculate next monthly occurrence"""
    monthly_pattern = meeting_data.get('monthly_pattern', 'same-date')
    selected_month_dates = meeting_data.get('selected_month_dates', [])
    
    if isinstance(selected_month_dates, str):
        try:
            selected_month_dates = json.loads(selected_month_dates)
        except:
            selected_month_dates = []
    
    current_date = current_time.date()
    
    if monthly_pattern == 'same-date':
        # Same date each month
        target_day = start_time.day
        
        # Check current month first
        try:
            next_date = current_date.replace(day=target_day)
            if next_date >= current_date:
                next_datetime = datetime.combine(next_date, start_time.time())
                next_datetime = convert_to_ist(next_datetime)
                
                if next_datetime > current_time:
                    duration = timedelta(hours=1)
                    next_end_datetime = next_datetime + duration
                    
                    return {
                        'next_start_time': next_datetime.isoformat(),
                        'next_end_time': next_end_datetime.isoformat(),
                        'is_today': next_date == current_date,
                        'is_completed_today': False
                    }
        except ValueError:
            # Day doesn't exist in current month
            pass
        
        # Try next month
        next_month = current_date + relativedelta(months=1)
        try:
            next_date = next_month.replace(day=target_day)
            next_datetime = datetime.combine(next_date, start_time.time())
            next_datetime = convert_to_ist(next_datetime)
            
            duration = timedelta(hours=1)
            next_end_datetime = next_datetime + duration
            
            return {
                'next_start_time': next_datetime.isoformat(),
                'next_end_time': next_end_datetime.isoformat(),
                'is_today': False,
                'is_completed_today': False
            }
        except ValueError:
            # Day doesn't exist in next month either
            pass
    
    elif monthly_pattern == 'selected-dates' and selected_month_dates:
        # Specific dates each month
        for target_day in sorted(selected_month_dates):
            try:
                next_date = current_date.replace(day=target_day)
                if next_date >= current_date:
                    next_datetime = datetime.combine(next_date, start_time.time())
                    next_datetime = convert_to_ist(next_datetime)
                    
                    if next_datetime > current_time:
                        duration = timedelta(hours=1)
                        next_end_datetime = next_datetime + duration
                        
                        return {
                            'next_start_time': next_datetime.isoformat(),
                            'next_end_time': next_end_datetime.isoformat(),
                            'is_today': next_date == current_date,
                            'is_completed_today': False
                        }
            except ValueError:
                continue
        
        # Try next month
        next_month = current_date + relativedelta(months=1)
        for target_day in sorted(selected_month_dates):
            try:
                next_date = next_month.replace(day=target_day)
                next_datetime = datetime.combine(next_date, start_time.time())
                next_datetime = convert_to_ist(next_datetime)
                
                duration = timedelta(hours=1)
                next_end_datetime = next_datetime + duration
                
                return {
                    'next_start_time': next_datetime.isoformat(),
                    'next_end_time': next_end_datetime.isoformat(),
                    'is_today': False,
                    'is_completed_today': False
                }
            except ValueError:
                continue
    
    return None

def get_todays_meetings(meetings_data):
    """Get meetings that should occur today"""
    today = get_current_ist_datetime().date()
    todays_meetings = []
    
    for meeting in meetings_data:
        if not meeting.get('is_recurring'):
            # Non-recurring meeting
            start_time = parse_datetime_safely(meeting.get('start_time'))
            if start_time and start_time.date() == today:
                todays_meetings.append(meeting)
        else:
            # Recurring meeting - check if it should occur today
            next_occurrence = calculate_next_occurrence(meeting)
            if next_occurrence and next_occurrence.get('is_today'):
                todays_meetings.append(meeting)
    
    return todays_meetings

def should_send_reminder(meeting_data, reminder_time_minutes=15):
    """Check if reminder should be sent for a meeting"""
    current_time = get_current_ist_datetime()
    
    # Get next occurrence
    next_occurrence = calculate_next_occurrence(meeting_data, current_time)
    if not next_occurrence:
        return False
    
    next_start_str = next_occurrence.get('next_start_time')
    if not next_start_str:
        return False
    
    next_start = parse_datetime_safely(next_start_str)
    if not next_start:
        return False
    
    # Check if reminder should be sent
    reminder_time = next_start - timedelta(minutes=reminder_time_minutes)
    
    # Send reminder if current time is within 1 minute of reminder time
    time_diff = abs((current_time - reminder_time).total_seconds())
    
    return time_diff <= 60  # Within 1 minute

def is_recurrence_ended(meeting_data, check_date=None):
    """Check if recurrence has ended"""
    if not meeting_data.get('is_recurring'):
        return True
    
    check_date = check_date or get_current_ist_datetime()
    
    # Check end date
    recurrence_end_date = meeting_data.get('recurrence_end_date')
    if recurrence_end_date:
        end_date = parse_datetime_safely(recurrence_end_date)
        if end_date and check_date.date() > end_date.date():
            return True
    
    # Check occurrences (this would need tracking in database)
    recurrence_occurrences = meeting_data.get('recurrence_occurrences')
    if recurrence_occurrences:
        # This would need to be implemented with occurrence tracking
        # For now, just use end date logic
        pass
    
    return False