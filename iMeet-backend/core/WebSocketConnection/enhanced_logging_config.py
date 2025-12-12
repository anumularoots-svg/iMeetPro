# enhanced_logging_config.py
# Complete logging solution to capture ALL logs from meetings.py, participants.py, 
# chat_messages.py, cache_only_hand_raise.py, recording_service.py, Attendance.py, and notifications.py
# This will intercept ALL logging calls and route them to the correct files

import logging
import logging.handlers
import os
import sys
import traceback
from datetime import datetime

class FileBasedFilter(logging.Filter):
    """Filter logs based on the source file"""
    
    def __init__(self, target_files):
        super().__init__()
        self.target_files = target_files
    
    def filter(self, record):
        # Check pathname first
        if hasattr(record, 'pathname'):
            filename = os.path.basename(record.pathname)
            return filename in self.target_files
        
        # Check filename attribute
        if hasattr(record, 'filename'):
            return record.filename in self.target_files
        
        # Check stack trace to find the calling file
        try:
            # Get the stack trace
            stack = traceback.extract_stack()
            for frame in reversed(stack):
                filename = os.path.basename(frame.filename)
                if filename in self.target_files:
                    return True
        except:
            pass
        
        return False

class APICallFilter(logging.Filter):
    """Filter to identify API-related logs"""
    
    def filter(self, record):
        message = record.getMessage().lower()
        
        # API indicators
        api_indicators = [
            'http/1.1', 'post ', 'get ', 'put ', 'delete ', 'options ',
            'status:', '- - [', 'api/', '/api', 'endpoint', 'request',
            'response', 'returned', 'json', 'received json'
        ]
        
        return any(indicator in message for indicator in api_indicators)

class LogInterceptor:
    """Intercept and route all logging calls"""
    
    def __init__(self):
        self.original_methods = {}
        self.setup_interception()
    
    def setup_interception(self):
        """Replace all logging methods with intercepting versions"""
        
        # Store original methods
        self.original_methods = {
            'debug': logging.debug,
            'info': logging.info,
            'warning': logging.warning,
            'error': logging.error,
            'critical': logging.critical,
            'exception': logging.exception
        }
        
        # Replace with intercepting methods
        logging.debug = self._intercept_debug
        logging.info = self._intercept_info
        logging.warning = self._intercept_warning
        logging.error = self._intercept_error
        logging.critical = self._intercept_critical
        logging.exception = self._intercept_exception
    
    def _get_calling_file(self):
        """Get the file that made the logging call"""
        try:
            # Get the call stack
            frame = sys._getframe()
            while frame:
                filename = os.path.basename(frame.f_code.co_filename)
                target_files = [
                    'meetings.py', 'participants.py', 
                    'chat_messages.py', 'cache_only_hand_raise.py',
                    'recording_service.py', 'Attendance.py', 'notifications.py'  # ✅ ADDED
                ]
                if filename in target_files:
                    return filename
                frame = frame.f_back
        except:
            pass
        return None
    
    def _route_log(self, level, msg, *args, **kwargs):
        """Route log message to appropriate logger"""
        calling_file = self._get_calling_file()
        
        if calling_file == 'meetings.py':
            logger = logging.getLogger('meetings_module')
            getattr(logger, level)(msg, *args, **kwargs)
        elif calling_file == 'participants.py':
            logger = logging.getLogger('participants_module') 
            getattr(logger, level)(msg, *args, **kwargs)
        elif calling_file == 'chat_messages.py':
            logger = logging.getLogger('cache_chat_module')
            getattr(logger, level)(msg, *args, **kwargs)
        elif calling_file == 'cache_only_hand_raise.py':
            logger = logging.getLogger('cache_hand_raise_module')
            getattr(logger, level)(msg, *args, **kwargs)
        elif calling_file == 'recording_service.py':
            logger = logging.getLogger('recording_service_module')
            getattr(logger, level)(msg, *args, **kwargs)
        elif calling_file == 'Attendance.py':
            logger = logging.getLogger('attendance_module')
            getattr(logger, level)(msg, *args, **kwargs)
        elif calling_file == 'notifications.py':  # ✅ ADDED
            logger = logging.getLogger('notifications_module')
            getattr(logger, level)(msg, *args, **kwargs)
        else:
            # Use original method for other files
            original_method = self.original_methods.get(level)
            if original_method:
                original_method(msg, *args, **kwargs)
    
    def _intercept_debug(self, msg, *args, **kwargs):
        self._route_log('debug', msg, *args, **kwargs)
    
    def _intercept_info(self, msg, *args, **kwargs):
        self._route_log('info', msg, *args, **kwargs)
    
    def _intercept_warning(self, msg, *args, **kwargs):
        self._route_log('warning', msg, *args, **kwargs)
    
    def _intercept_error(self, msg, *args, **kwargs):
        self._route_log('error', msg, *args, **kwargs)
    
    def _intercept_critical(self, msg, *args, **kwargs):
        self._route_log('critical', msg, *args, **kwargs)
    
    def _intercept_exception(self, msg, *args, **kwargs):
        self._route_log('error', msg, *args, **kwargs)

def setup_complete_logging():
    """Setup complete logging interception and routing"""
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Clear existing handlers from root logger
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set root logger level
    root_logger.setLevel(logging.DEBUG)
    
    # ============================================
    # CONFIGURE NAMED LOGGERS FROM CACHE SYSTEMS
    # ============================================
    
    # Redirect cache_chat logger to our cache_chat_module logger
    cache_chat_named_logger = logging.getLogger('cache_chat')
    cache_chat_named_logger.handlers.clear()
    cache_chat_named_logger.propagate = False
    cache_chat_named_logger.setLevel(logging.DEBUG)
    
    # Redirect cache_hand_raise logger to our cache_hand_raise_module logger  
    cache_hand_raise_named_logger = logging.getLogger('cache_hand_raise')
    cache_hand_raise_named_logger.handlers.clear()
    cache_hand_raise_named_logger.propagate = False
    cache_hand_raise_named_logger.setLevel(logging.DEBUG)
    
    # Redirect attendance logger
    attendance_named_logger = logging.getLogger('attendance')
    attendance_named_logger.handlers.clear()
    attendance_named_logger.propagate = False
    attendance_named_logger.setLevel(logging.DEBUG)
    
    # ✅ NEW: Redirect notifications logger
    notifications_named_logger = logging.getLogger('notifications')
    notifications_named_logger.handlers.clear()
    notifications_named_logger.propagate = False
    notifications_named_logger.setLevel(logging.DEBUG)
    
    # ============================================
    # MEETINGS MODULE LOGGER
    # ============================================
    
    meetings_logger = logging.getLogger('meetings_module')
    meetings_logger.setLevel(logging.DEBUG)
    meetings_logger.propagate = False
    
    # Meetings file handler
    meetings_handler = logging.handlers.RotatingFileHandler(
        'logs/meetings_module.log',
        maxBytes=15*1024*1024,  # 15MB
        backupCount=5,
        encoding='utf-8'
    )
    meetings_handler.setLevel(logging.DEBUG)
    
    meetings_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    meetings_handler.setFormatter(meetings_formatter)
    meetings_logger.addHandler(meetings_handler)
    
    # ============================================
    # PARTICIPANTS MODULE LOGGER
    # ============================================
    
    participants_logger = logging.getLogger('participants_module')
    participants_logger.setLevel(logging.DEBUG)
    participants_logger.propagate = False
    
    # Participants file handler
    participants_handler = logging.handlers.RotatingFileHandler(
        'logs/participants_module.log',
        maxBytes=15*1024*1024,  # 15MB
        backupCount=5,
        encoding='utf-8'
    )
    participants_handler.setLevel(logging.DEBUG)
    
    participants_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    participants_handler.setFormatter(participants_formatter)
    participants_logger.addHandler(participants_handler)
    
    # ============================================
    # CACHE CHAT MODULE LOGGER
    # ============================================
    
    cache_chat_logger = logging.getLogger('cache_chat_module')
    cache_chat_logger.setLevel(logging.DEBUG)
    cache_chat_logger.propagate = False
    
    # Cache chat file handler
    cache_chat_handler = logging.handlers.RotatingFileHandler(
        'logs/cache_chat_module.log',
        maxBytes=15*1024*1024,  # 15MB
        backupCount=5,
        encoding='utf-8'
    )
    cache_chat_handler.setLevel(logging.DEBUG)
    
    cache_chat_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    cache_chat_handler.setFormatter(cache_chat_formatter)
    cache_chat_logger.addHandler(cache_chat_handler)
    
    # Connect the named logger to this module logger
    cache_chat_named_logger.addHandler(cache_chat_handler)
    
    # ============================================
    # CACHE HAND RAISE MODULE LOGGER
    # ============================================
    
    cache_hand_raise_logger = logging.getLogger('cache_hand_raise_module')
    cache_hand_raise_logger.setLevel(logging.DEBUG)
    cache_hand_raise_logger.propagate = False
    
    # Cache hand raise file handler
    cache_hand_raise_handler = logging.handlers.RotatingFileHandler(
        'logs/cache_hand_raise_module.log',
        maxBytes=15*1024*1024,  # 15MB
        backupCount=5,
        encoding='utf-8'
    )
    cache_hand_raise_handler.setLevel(logging.DEBUG)
    
    cache_hand_raise_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    cache_hand_raise_handler.setFormatter(cache_hand_raise_formatter)
    cache_hand_raise_logger.addHandler(cache_hand_raise_handler)
    
    # Connect the named logger to this module logger
    cache_hand_raise_named_logger.addHandler(cache_hand_raise_handler)
    
    # ============================================
    # RECORDING SERVICE MODULE LOGGER
    # ============================================
    
    recording_service_logger = logging.getLogger('recording_service_module')
    recording_service_logger.setLevel(logging.DEBUG)
    recording_service_logger.propagate = False
    
    # Recording service file handler
    recording_service_handler = logging.handlers.RotatingFileHandler(
        'logs/recording_service_module.log',
        maxBytes=20*1024*1024,  # 20MB (larger for video processing logs)
        backupCount=5,
        encoding='utf-8'
    )
    recording_service_handler.setLevel(logging.DEBUG)
    
    recording_service_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    recording_service_handler.setFormatter(recording_service_formatter)
    recording_service_logger.addHandler(recording_service_handler)
    
    # ============================================
    # ATTENDANCE MODULE LOGGER
    # ============================================
    
    attendance_logger = logging.getLogger('attendance_module')
    attendance_logger.setLevel(logging.DEBUG)
    attendance_logger.propagate = False
    
    # Attendance file handler
    attendance_handler = logging.handlers.RotatingFileHandler(
        'logs/attendance_module.log',
        maxBytes=20*1024*1024,  # 20MB (attendance has lots of detection logs)
        backupCount=5,
        encoding='utf-8'
    )
    attendance_handler.setLevel(logging.DEBUG)
    
    attendance_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    attendance_handler.setFormatter(attendance_formatter)
    attendance_logger.addHandler(attendance_handler)
    
    # Connect the named logger to this module logger
    attendance_named_logger.addHandler(attendance_handler)
    
    # ============================================
    # ✅ NEW: NOTIFICATIONS MODULE LOGGER
    # ============================================
    
    notifications_logger = logging.getLogger('notifications_module')
    notifications_logger.setLevel(logging.DEBUG)
    notifications_logger.propagate = False
    
    # Notifications file handler
    notifications_handler = logging.handlers.RotatingFileHandler(
        'logs/notifications_module.log',
        maxBytes=15*1024*1024,  # 15MB
        backupCount=5,
        encoding='utf-8'
    )
    notifications_handler.setLevel(logging.DEBUG)
    
    notifications_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
    )
    notifications_handler.setFormatter(notifications_formatter)
    notifications_logger.addHandler(notifications_handler)
    
    # Connect the named logger to this module logger
    notifications_named_logger.addHandler(notifications_handler)
    
    # ============================================
    # API LOGS (MAIN DEBUG FILES)
    # ============================================
    
    # Meetings API logger
    meetings_api_logger = logging.getLogger('meetings_api')
    meetings_api_logger.setLevel(logging.INFO)
    meetings_api_logger.propagate = False
    
    meetings_api_handler = logging.handlers.RotatingFileHandler(
        'meetings_debug.log',
        maxBytes=20*1024*1024,  # 20MB
        backupCount=3,
        encoding='utf-8'
    )
    meetings_api_handler.setLevel(logging.INFO)
    meetings_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [MEETINGS-API] %(message)s'
    )
    meetings_api_handler.setFormatter(meetings_api_formatter)
    meetings_api_logger.addHandler(meetings_api_handler)
    
    # Participants API logger
    participants_api_logger = logging.getLogger('participants_api')
    participants_api_logger.setLevel(logging.INFO)
    participants_api_logger.propagate = False
    
    participants_api_handler = logging.handlers.RotatingFileHandler(
        'participants_debug.log',
        maxBytes=20*1024*1024,  # 20MB
        backupCount=3,
        encoding='utf-8'
    )
    participants_api_handler.setLevel(logging.INFO)
    participants_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [PARTICIPANTS-API] %(message)s'
    )
    participants_api_handler.setFormatter(participants_api_formatter)
    participants_api_logger.addHandler(participants_api_handler)
    
    # Cache Chat API logger
    cache_chat_api_logger = logging.getLogger('cache_chat_api')
    cache_chat_api_logger.setLevel(logging.INFO)
    cache_chat_api_logger.propagate = False
    
    cache_chat_api_handler = logging.handlers.RotatingFileHandler(
        'cache_chat_debug.log',
        maxBytes=20*1024*1024,  # 20MB
        backupCount=3,
        encoding='utf-8'
    )
    cache_chat_api_handler.setLevel(logging.INFO)
    cache_chat_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [CACHE-CHAT-API] %(message)s'
    )
    cache_chat_api_handler.setFormatter(cache_chat_api_formatter)
    cache_chat_api_logger.addHandler(cache_chat_api_handler)
    
    # Cache Hand Raise API logger
    cache_hand_raise_api_logger = logging.getLogger('cache_hand_raise_api')
    cache_hand_raise_api_logger.setLevel(logging.INFO)
    cache_hand_raise_api_logger.propagate = False
    
    cache_hand_raise_api_handler = logging.handlers.RotatingFileHandler(
        'cache_hand_raise_debug.log',
        maxBytes=20*1024*1024,  # 20MB
        backupCount=3,
        encoding='utf-8'
    )
    cache_hand_raise_api_handler.setLevel(logging.INFO)
    cache_hand_raise_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [CACHE-HAND-RAISE-API] %(message)s'
    )
    cache_hand_raise_api_handler.setFormatter(cache_hand_raise_api_formatter)
    cache_hand_raise_api_logger.addHandler(cache_hand_raise_api_handler)
    
    # Recording Service API logger
    recording_service_api_logger = logging.getLogger('recording_service_api')
    recording_service_api_logger.setLevel(logging.INFO)
    recording_service_api_logger.propagate = False
    
    recording_service_api_handler = logging.handlers.RotatingFileHandler(
        'recording_service_debug.log',
        maxBytes=25*1024*1024,  # 25MB
        backupCount=3,
        encoding='utf-8'
    )
    recording_service_api_handler.setLevel(logging.INFO)
    recording_service_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [RECORDING-SERVICE-API] %(message)s'
    )
    recording_service_api_handler.setFormatter(recording_service_api_formatter)
    recording_service_api_logger.addHandler(recording_service_api_handler)
    
    # Attendance API logger
    attendance_api_logger = logging.getLogger('attendance_api')
    attendance_api_logger.setLevel(logging.INFO)
    attendance_api_logger.propagate = False
    
    attendance_api_handler = logging.handlers.RotatingFileHandler(
        'attendance_debug.log',
        maxBytes=25*1024*1024,  # 25MB (attendance has lots of API calls)
        backupCount=3,
        encoding='utf-8'
    )
    attendance_api_handler.setLevel(logging.INFO)
    attendance_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [ATTENDANCE-API] %(message)s'
    )
    attendance_api_handler.setFormatter(attendance_api_formatter)
    attendance_api_logger.addHandler(attendance_api_handler)
    
    # ✅ NEW: Notifications API logger
    notifications_api_logger = logging.getLogger('notifications_api')
    notifications_api_logger.setLevel(logging.INFO)
    notifications_api_logger.propagate = False
    
    notifications_api_handler = logging.handlers.RotatingFileHandler(
        'notifications_debug.log',
        maxBytes=20*1024*1024,  # 20MB
        backupCount=3,
        encoding='utf-8'
    )
    notifications_api_handler.setLevel(logging.INFO)
    notifications_api_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [NOTIFICATIONS-API] %(message)s'
    )
    notifications_api_handler.setFormatter(notifications_api_formatter)
    notifications_api_logger.addHandler(notifications_api_handler)
    
    # ============================================
    # CONSOLE HANDLER (ONLY CRITICAL ERRORS)
    # ============================================
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.CRITICAL)  # Only show critical errors
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # ============================================
    # SUPPRESS CONSOLE OUTPUT FOR CACHE LOGGERS
    # ============================================
    
    # Ensure cache loggers don't output to console
    cache_chat_named_logger.addHandler(logging.NullHandler())
    cache_hand_raise_named_logger.addHandler(logging.NullHandler())
    attendance_named_logger.addHandler(logging.NullHandler())
    notifications_named_logger.addHandler(logging.NullHandler())  # ✅ ADDED
    
    # ============================================
    # DJANGO/OTHER LOGS (FILTERED)
    # ============================================
    
    # Reduce noise from other loggers
    logging.getLogger('django').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    logging.getLogger('livekit').setLevel(logging.ERROR)
    logging.getLogger('livekit.rtc').setLevel(logging.ERROR)
    logging.getLogger('livekit.api').setLevel(logging.ERROR)
    logging.getLogger('livekit_ffi').setLevel(logging.ERROR)
    
    # Suppress specific attendance-related noisy loggers
    logging.getLogger('unified_face_service').setLevel(logging.WARNING)
    logging.getLogger('meeting_continuous_verification').setLevel(logging.WARNING)
    
    print("✅ Enhanced logging configuration loaded")
    print(f"📝 Meetings logs: logs/meetings_module.log")
    print(f"📝 Participants logs: logs/participants_module.log") 
    print(f"📝 Cache Chat logs: logs/cache_chat_module.log")
    print(f"📝 Cache Hand Raise logs: logs/cache_hand_raise_module.log")
    print(f"📝 Recording Service logs: logs/recording_service_module.log")
    print(f"📝 Attendance logs: logs/attendance_module.log")
    print(f"📝 Notifications logs: logs/notifications_module.log")  # ✅ ADDED
    print(f"📝 Meetings API: meetings_debug.log")
    print(f"📝 Participants API: participants_debug.log")
    print(f"📝 Cache Chat API: cache_chat_debug.log")
    print(f"📝 Cache Hand Raise API: cache_hand_raise_debug.log")
    print(f"📝 Recording Service API: recording_service_debug.log")
    print(f"📝 Attendance API: attendance_debug.log")
    print(f"📝 Notifications API: notifications_debug.log")  # ✅ ADDED
    print(f"📝 Console: Critical errors only")
    
    return True

def configure_enhanced_logging():
    """Main configuration function"""
    
    try:
        # Setup the logging system
        setup_complete_logging()
        
        # Setup log interception
        interceptor = LogInterceptor()
        
        # Store interceptor globally so it doesn't get garbage collected
        import builtins
        builtins._log_interceptor = interceptor
        
        # Test the loggers
        meetings_logger = logging.getLogger('meetings_module')
        participants_logger = logging.getLogger('participants_module')
        cache_chat_logger = logging.getLogger('cache_chat_module')
        cache_hand_raise_logger = logging.getLogger('cache_hand_raise_module')
        recording_service_logger = logging.getLogger('recording_service_module')
        attendance_logger = logging.getLogger('attendance_module')
        notifications_logger = logging.getLogger('notifications_module')  # ✅ ADDED
        
        meetings_logger.info("✅ Enhanced meetings logging active")
        participants_logger.info("✅ Enhanced participants logging active")
        cache_chat_logger.info("✅ Enhanced cache chat logging active")
        cache_hand_raise_logger.info("✅ Enhanced cache hand raise logging active")
        recording_service_logger.info("✅ Enhanced recording service logging active")
        attendance_logger.info("✅ Enhanced attendance logging active")
        notifications_logger.info("✅ Enhanced notifications logging active")  # ✅ ADDED
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to configure enhanced logging: {e}")
        return False

# ============================================
# UTILITY FUNCTIONS
# ============================================

def log_meetings_activity(message):
    """Log meetings-specific activity"""
    logger = logging.getLogger('meetings_module')
    logger.info(f"[MEETINGS] {message}")

def log_participants_activity(message):
    """Log participants-specific activity"""
    logger = logging.getLogger('participants_module')
    logger.info(f"[PARTICIPANTS] {message}")

def log_cache_chat_activity(message):
    """Log cache chat-specific activity"""
    logger = logging.getLogger('cache_chat_module')
    logger.info(f"[CACHE-CHAT] {message}")

def log_cache_hand_raise_activity(message):
    """Log cache hand raise-specific activity"""
    logger = logging.getLogger('cache_hand_raise_module')
    logger.info(f"[CACHE-HAND-RAISE] {message}")

def log_recording_service_activity(message):
    """Log recording service-specific activity"""
    logger = logging.getLogger('recording_service_module')
    logger.info(f"[RECORDING-SERVICE] {message}")

def log_attendance_activity(message):
    """Log attendance-specific activity"""
    logger = logging.getLogger('attendance_module')
    logger.info(f"[ATTENDANCE] {message}")

def log_notifications_activity(message):
    """✅ NEW: Log notifications-specific activity"""
    logger = logging.getLogger('notifications_module')
    logger.info(f"[NOTIFICATIONS] {message}")

def log_api_activity(module, endpoint, method, status_code, **kwargs):
    """Log API activity to appropriate API log"""
    logger_map = {
        'meetings': logging.getLogger('meetings_api'),
        'participants': logging.getLogger('participants_api'),
        'cache_chat': logging.getLogger('cache_chat_api'),
        'cache_hand_raise': logging.getLogger('cache_hand_raise_api'),
        'recording_service': logging.getLogger('recording_service_api'),
        'attendance': logging.getLogger('attendance_api'),
        'notifications': logging.getLogger('notifications_api')  # ✅ ADDED
    }
    
    logger = logger_map.get(module)
    if not logger:
        return
    
    message = f"{method} {endpoint} - Status: {status_code}"
    
    if 'execution_time' in kwargs:
        message += f" - Time: {kwargs['execution_time']:.2f}s"
    if 'user_id' in kwargs:
        message += f" - User: {kwargs['user_id']}"
    if 'meeting_id' in kwargs:
        message += f" - Meeting: {kwargs['meeting_id']}"
    if 'file_size' in kwargs:
        message += f" - FileSize: {kwargs['file_size']}"
    if 'redis_status' in kwargs:
        message += f" - Redis: {kwargs['redis_status']}"
    if 'video_duration' in kwargs:
        message += f" - Duration: {kwargs['video_duration']}"
    if 'frames_captured' in kwargs:
        message += f" - Frames: {kwargs['frames_captured']}"
    if 'audio_samples' in kwargs:
        message += f" - Audio: {kwargs['audio_samples']}"
    if 'recording_status' in kwargs:
        message += f" - RecStatus: {kwargs['recording_status']}"
    # Attendance-specific fields
    if 'violation_type' in kwargs:
        message += f" - Violation: {kwargs['violation_type']}"
    if 'warnings' in kwargs:
        message += f" - Warnings: {kwargs['warnings']}"
    if 'detections' in kwargs:
        message += f" - Detections: {kwargs['detections']}"
    if 'attendance_percentage' in kwargs:
        message += f" - Attendance: {kwargs['attendance_percentage']}%"
    if 'identity_verified' in kwargs:
        message += f" - Identity: {kwargs['identity_verified']}"
    # ✅ NEW: Notifications-specific fields
    if 'notification_type' in kwargs:
        message += f" - NotifType: {kwargs['notification_type']}"
    if 'recipient_email' in kwargs:
        message += f" - Recipient: {kwargs['recipient_email']}"
    if 'is_read' in kwargs:
        message += f" - Read: {kwargs['is_read']}"
    if 'unread_count' in kwargs:
        message += f" - Unread: {kwargs['unread_count']}"
    if 'notification_count' in kwargs:
        message += f" - Count: {kwargs['notification_count']}"
    if 'sent_count' in kwargs:
        message += f" - Sent: {kwargs['sent_count']}"
    if 'failed_count' in kwargs:
        message += f" - Failed: {kwargs['failed_count']}"
    
    logger.info(message)

def test_logging():
    """Test the logging configuration"""
    print("\n🧪 Testing enhanced logging configuration...")
    
    # Test meetings logger
    meetings_logger = logging.getLogger('meetings_module')
    meetings_logger.debug("Debug message from meetings")
    meetings_logger.info("Info message from meetings")
    meetings_logger.warning("Warning message from meetings")
    meetings_logger.error("Error message from meetings")
    
    # Test participants logger
    participants_logger = logging.getLogger('participants_module')
    participants_logger.debug("Debug message from participants")
    participants_logger.info("Info message from participants")
    participants_logger.warning("Warning message from participants")
    participants_logger.error("Error message from participants")
    
    # Test cache chat logger
    cache_chat_logger = logging.getLogger('cache_chat_module')
    cache_chat_logger.debug("Debug message from cache chat")
    cache_chat_logger.info("Info message from cache chat")
    cache_chat_logger.warning("Warning message from cache chat")
    cache_chat_logger.error("Error message from cache chat")
    
    # Test cache hand raise logger
    cache_hand_raise_logger = logging.getLogger('cache_hand_raise_module')
    cache_hand_raise_logger.debug("Debug message from cache hand raise")
    cache_hand_raise_logger.info("Info message from cache hand raise")
    cache_hand_raise_logger.warning("Warning message from cache hand raise")
    cache_hand_raise_logger.error("Error message from cache hand raise")
    
    # Test recording service logger
    recording_service_logger = logging.getLogger('recording_service_module')
    recording_service_logger.debug("Debug message from recording service")
    recording_service_logger.info("Info message from recording service")
    recording_service_logger.warning("Warning message from recording service")
    recording_service_logger.error("Error message from recording service")
    
    # Test attendance logger
    attendance_logger = logging.getLogger('attendance_module')
    attendance_logger.debug("Debug message from attendance")
    attendance_logger.info("Info message from attendance")
    attendance_logger.warning("Warning message from attendance")
    attendance_logger.error("Error message from attendance")
    
    # ✅ NEW: Test notifications logger
    notifications_logger = logging.getLogger('notifications_module')
    notifications_logger.debug("Debug message from notifications")
    notifications_logger.info("Info message from notifications")
    notifications_logger.warning("Warning message from notifications")
    notifications_logger.error("Error message from notifications")
    
    # Test API loggers
    log_api_activity('meetings', 'test_endpoint', 'POST', 200, execution_time=0.5)
    log_api_activity('participants', 'test_endpoint', 'GET', 200, execution_time=0.3)
    log_api_activity('cache_chat', 'send_message', 'POST', 200, execution_time=0.2, file_size='5MB')
    log_api_activity('cache_hand_raise', 'raise_hand', 'POST', 200, execution_time=0.1, redis_status='connected')
    log_api_activity('recording_service', 'start_recording', 'POST', 200, 
                    execution_time=2.5, meeting_id='test123', frames_captured=1500, 
                    audio_samples=48000, recording_status='active')
    log_api_activity('attendance', 'detect_violations', 'POST', 200,
                    execution_time=0.3, meeting_id='test123', user_id='user456',
                    violation_type='Eyes closed', warnings=2, detections=0,
                    attendance_percentage=98.5, identity_verified=True)
    # ✅ NEW: Test notifications API
    log_api_activity('notifications', 'get_user_notifications', 'GET', 200,
                    execution_time=0.2, recipient_email='user@example.com',
                    unread_count=5, notification_count=20)
    log_api_activity('notifications', 'create_meeting_notifications', 'POST', 200,
                    execution_time=0.5, meeting_id='test123',
                    notification_type='meeting_invitation', sent_count=10, failed_count=0)
    
    print("✅ Test completed - check log files!")

# ============================================
# HELPER FUNCTIONS FOR CACHE SYSTEMS
# ============================================

def setup_cache_system_loggers():
    """Helper function to setup cache system loggers if called separately"""
    
    # These are already setup in the main configure function,
    # but this can be used for testing or separate initialization
    
    cache_chat_logger = logging.getLogger('cache_chat')
    cache_hand_raise_logger = logging.getLogger('cache_hand_raise')
    attendance_logger = logging.getLogger('attendance')
    notifications_logger = logging.getLogger('notifications')  # ✅ ADDED
    
    # Set them to use the module loggers
    cache_chat_logger.addHandler(logging.getLogger('cache_chat_module').handlers[0])
    cache_hand_raise_logger.addHandler(logging.getLogger('cache_hand_raise_module').handlers[0])
    attendance_logger.addHandler(logging.getLogger('attendance_module').handlers[0])
    notifications_logger.addHandler(logging.getLogger('notifications_module').handlers[0])  # ✅ ADDED
    
    return True

def get_log_file_paths():
    """Get all log file paths for monitoring"""
    return {
        'meetings_module': 'logs/meetings_module.log',
        'participants_module': 'logs/participants_module.log',
        'cache_chat_module': 'logs/cache_chat_module.log',
        'cache_hand_raise_module': 'logs/cache_hand_raise_module.log',
        'recording_service_module': 'logs/recording_service_module.log',
        'attendance_module': 'logs/attendance_module.log',
        'notifications_module': 'logs/notifications_module.log',  # ✅ ADDED
        'meetings_api': 'meetings_debug.log',
        'participants_api': 'participants_debug.log',
        'cache_chat_api': 'cache_chat_debug.log',
        'cache_hand_raise_api': 'cache_hand_raise_debug.log',
        'recording_service_api': 'recording_service_debug.log',
        'attendance_api': 'attendance_debug.log',
        'notifications_api': 'notifications_debug.log'  # ✅ ADDED
    }

def clear_all_logs():
    """Clear all log files (for testing/debugging)"""
    log_files = get_log_file_paths()
    
    for log_type, log_path in log_files.items():
        try:
            if os.path.exists(log_path):
                with open(log_path, 'w') as f:
                    f.write('')  # Clear the file
                print(f"✅ Cleared {log_type} log: {log_path}")
        except Exception as e:
            print(f"❌ Failed to clear {log_type} log: {e}")

def monitor_log_sizes():
    """Monitor log file sizes"""
    log_files = get_log_file_paths()
    
    print("\n📊 Log File Sizes:")
    print("-" * 50)
    
    for log_type, log_path in log_files.items():
        try:
            if os.path.exists(log_path):
                size = os.path.getsize(log_path)
                size_mb = size / (1024 * 1024)
                print(f"{log_type:30}: {size_mb:6.2f} MB - {log_path}")
            else:
                print(f"{log_type:30}: Not found - {log_path}")
        except Exception as e:
            print(f"{log_type:30}: Error - {e}")

# ============================================
# AUTO-INITIALIZE
# ============================================

if __name__ == "__main__":
    configure_enhanced_logging()
    test_logging()
    print("\n" + "="*60)
    monitor_log_sizes()
else:
    # Auto-configure when imported
    configure_enhanced_logging()