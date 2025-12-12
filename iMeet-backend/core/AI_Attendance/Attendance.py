import json
import time
import threading
import cv2
import numpy as np
import base64
import io
from PIL import Image
import mediapipe as mp
from scipy.spatial.distance import euclidean
from datetime import datetime, timedelta
import uuid
from functools import wraps
from typing import Optional, Dict, List, Tuple, Any
import traceback
import asyncio
from core.WebSocketConnection import enhanced_logging_config
from django.db import models, connection, transaction
from django.utils import timezone
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.urls import path
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.cache import cache
from django.db.models import Avg, Count, Q, Max, Min
import logging
from asgiref.sync import sync_to_async

logger = logging.getLogger(__name__)

# ============================================================================
# ENHANCED LOGGING CONFIGURATION FOR VERIFICATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler('attendance_verification.log')  # File output
    ]
)

# Set specific logger levels for verification components
logging.getLogger('unified_face_service').setLevel(logging.INFO)
logging.getLogger('meeting_continuous_verification').setLevel(logging.INFO)

logger.info("="*80)
logger.info("✅ AI Attendance System with Identity Verification - STARTED")
logger.info("="*80)

# ==================== CONFIGURATION ====================

class AttendanceConfig:
    """Configuration constants for attendance system - 5 MINUTE BREAK"""
    
    # ==================== FACE DETECTION THRESHOLDS (EXISTING) ====================
    EAR_THRESHOLD = 0.22  # Eye Aspect Ratio threshold for closed eyes
    HEAD_YAW_THRESHOLD = 25  # Degrees for head turn detection
    HAND_FACE_DISTANCE = 0.12  # Distance threshold for hand near face
    FACE_MOVEMENT_THRESHOLD = 0.03  # Face movement detection threshold
    YAW_MOVEMENT_THRESHOLD = 8  # Yaw movement threshold
    POSE_VARIANCE_THRESHOLD = 0.05  # Pose variance for lying down detection
    BASELINE_FRAMES_REQUIRED = 5  # Number of frames needed to establish baseline
    
    # ==================== TIME INTERVALS (EXISTING) ====================
    INACTIVITY_WARNING_TIME = 10  # Seconds before inactivity warning
    INACTIVITY_VIOLATION_TIME = 20  # Seconds before inactivity violation
    VIOLATION_POPUP_TIME = 20  # Seconds to show violation popup
    DETECTION_INTERVAL = 20  # Seconds between detection checks
    VIOLATION_AUTO_REMOVAL_TIME = 120  # 2 minutes - continuous violation removal time
    BREAK_DURATION = 300  # 5 minutes - break duration
    POPUP_COOLDOWN = 20  # Seconds between popup displays
    
    # ==================== BREAK SYSTEM (EXISTING) ====================
    MAX_TOTAL_BREAK_TIME = 300  # 5 minutes - maximum total break time allowed
    CAMERA_VERIFICATION_TIMEOUT = 5  # Seconds to verify camera after break
    GRACE_PERIOD_DURATION = 2  # Seconds of grace period after camera resume
    
    # ==================== WARNING & PENALTY SYSTEM (EXISTING) ====================
    MAX_WARNING_MESSAGES = 4  # Maximum warnings before entering detection phase
    DETECTION_PENALTY_3 = 0.25  # 0.25% penalty per 3 detections (batch penalty)
    CONTINUOUS_2MIN_PENALTY = 1.0  # 1% penalty for 2-minute continuous violation
    BREAK_PENALTY = 1.0  # 1% penalty for break usage
    INACTIVITY_PENALTY = 1.0  # 1% penalty for inactivity
    
    # ==================== MEDIAPIPE CONFIDENCE LEVELS (EXISTING) ====================
    FACE_DETECTION_CONFIDENCE = 0.7  # Face detection confidence threshold
    FACE_TRACKING_CONFIDENCE = 0.5  # Face tracking confidence threshold
    HAND_DETECTION_CONFIDENCE = 0.5  # Hand detection confidence threshold
    POSE_DETECTION_CONFIDENCE = 0.5  # Pose detection confidence threshold
    
    # ==================== IDENTITY VERIFICATION SETTINGS (NEW) ====================
    IDENTITY_CHECK_INTERVAL = 1.0  # Check identity every 1 second
    IDENTITY_UNKNOWN_THRESHOLD = 5  # 5 consecutive seconds of unknown person = 1 warning
    IDENTITY_MAX_WARNINGS = 3  # 3 warnings = removal from meeting
    IDENTITY_FACE_THRESHOLD = 0.6  # Face recognition similarity threshold (0.0-1.0)
    
    # ==================== SYSTEM CONSTANTS (EXISTING) ====================
    MAX_FRAME_HISTORY = 30  # Maximum frames to keep in history
    BASELINE_RESET_INTERVAL = 300  # Seconds before baseline reset (5 minutes)
    SESSION_TIMEOUT = 3600  # Session timeout in seconds (1 hour)
    
    # ==================== VIOLATION TYPES (EXISTING) ====================
    VIOLATION_TYPES = [
        "Eyes closed",
        "Head turned",
        "Hand near face",
        "Face not visible",
        "Multiple faces detected",
        "Lying down",
        "Inactivity detected"
    ]
    
    # ==================== DESCRIPTION STRINGS (EXISTING) ====================
    DESCRIPTIONS = {
        "Eyes closed": "Eyes appear to be closed for extended period",
        "Head turned": "Head is turned away from camera",
        "Hand near face": "Hand is covering or near face",
        "Face not visible": "Face is not visible in camera",
        "Multiple faces detected": "More than one person detected in frame",
        "Lying down": "Person appears to be lying down",
        "Inactivity detected": "No movement detected for extended period"
    }
    
    # ==================== IDENTITY VERIFICATION DESCRIPTIONS (NEW) ====================
    IDENTITY_DESCRIPTIONS = {
        "identity_warning_1": "First identity verification failure - unknown person detected",
        "identity_warning_2": "Second identity verification failure - critical warning",
        "identity_warning_3": "Third identity verification failure - user removed",
        "identity_verified": "Identity successfully verified",
        "identity_unknown": "Unknown person detected in frame"
    }
    
    # ==================== RESPONSE MESSAGES (EXISTING) ====================
    MESSAGES = {
        "warning_1": "⚠️ Warning 1/{max}: {violation}. Please correct immediately.",
        "warning_2": "⚠️ Warning 2/{max}: {violation}. Multiple violations detected.",
        "warning_3": "⚠️ Warning 3/{max}: {violation}. This is your final warning.",
        "warning_4": "⚠️ Warning 4/{max}: {violation}. Detection phase will begin.",
        "detection": "🔴 Detection {count}: {violation}. Penalty applied: {penalty}%",
        "continuous_violation": "🚫 Continuous violation for {duration} seconds. You will be removed in {remaining} seconds.",
        "removed": "🚫 You have been removed from the meeting due to continuous violations.",
        "inactivity_warning": "⚠️ Inactivity detected. Please show movement to continue.",
        "break_started": "⏸️ Break started. You have {remaining} minutes remaining.",
        "break_ended": "▶️ Break ended. Welcome back!",
        "break_exhausted": "⚠️ Break time exhausted. No more break time available.",
        "camera_required": "📷 Camera verification required. Please enable your camera."
    }
    
    # ==================== IDENTITY VERIFICATION MESSAGES (NEW) ====================
    IDENTITY_MESSAGES = {
        "warning_1": "⚠️ Warning 1/3: Unknown person detected. Please ensure only you are visible in the camera.",
        "warning_2": "⚠️⚠️ Warning 2/3: Identity verification failed again. ONE MORE failure will remove you from the meeting!",
        "warning_3": "🚫 You have been removed from the meeting due to identity verification failure (3 warnings). Please correct the issue and rejoin.",
        "removal": "🚫 Removed due to identity verification failure. You can rejoin after correcting the issue.",
        "verified": "✅ Identity verified successfully.",
        "checking": "🔍 Verifying identity..."
    }
    
    # ==================== CONFIGURATION VALIDATION (EXISTING) ====================
    @classmethod
    def validate_config(cls):
        """
        Validate configuration settings
        EXISTING METHOD - No changes
        """
        assert cls.EAR_THRESHOLD > 0, "EAR_THRESHOLD must be positive"
        assert cls.HEAD_YAW_THRESHOLD > 0, "HEAD_YAW_THRESHOLD must be positive"
        assert cls.MAX_WARNING_MESSAGES > 0, "MAX_WARNING_MESSAGES must be positive"
        assert cls.DETECTION_PENALTY_3 >= 0, "DETECTION_PENALTY_3 must be non-negative"
        assert cls.CONTINUOUS_2MIN_PENALTY >= 0, "CONTINUOUS_2MIN_PENALTY must be non-negative"
        assert cls.MAX_TOTAL_BREAK_TIME > 0, "MAX_TOTAL_BREAK_TIME must be positive"
        assert cls.VIOLATION_AUTO_REMOVAL_TIME > 0, "VIOLATION_AUTO_REMOVAL_TIME must be positive"
        return True
    
    # ==================== IDENTITY CONFIGURATION VALIDATION (NEW) ====================
    @classmethod
    def validate_identity_config(cls):
        """
        ✅ NEW METHOD - Validate identity verification configuration settings
        
        Raises:
            AssertionError: If any configuration value is invalid
        
        Returns:
            bool: True if all validations pass
        """
        assert cls.IDENTITY_CHECK_INTERVAL > 0, "IDENTITY_CHECK_INTERVAL must be positive"
        assert cls.IDENTITY_UNKNOWN_THRESHOLD > 0, "IDENTITY_UNKNOWN_THRESHOLD must be positive"
        assert cls.IDENTITY_MAX_WARNINGS > 0, "IDENTITY_MAX_WARNINGS must be positive"
        assert 0 <= cls.IDENTITY_FACE_THRESHOLD <= 1, "IDENTITY_FACE_THRESHOLD must be between 0 and 1"
        assert cls.IDENTITY_CHECK_INTERVAL <= cls.IDENTITY_UNKNOWN_THRESHOLD, \
            "IDENTITY_CHECK_INTERVAL should be less than or equal to IDENTITY_UNKNOWN_THRESHOLD"
        return True
    
    # ==================== GET PENALTY AMOUNT (EXISTING) ====================
    @classmethod
    def get_penalty_amount(cls, penalty_type: str) -> float:
        """
        Get penalty amount for specific penalty type
        EXISTING METHOD - No changes
        
        Args:
            penalty_type: Type of penalty ('detection', 'continuous', 'break', 'inactivity')
        
        Returns:
            float: Penalty percentage
        """
        penalty_map = {
            'detection': cls.DETECTION_PENALTY_3,
            'continuous': cls.CONTINUOUS_2MIN_PENALTY,
            'break': cls.BREAK_PENALTY,
            'inactivity': cls.INACTIVITY_PENALTY
        }
        return penalty_map.get(penalty_type, 0.0)
    
    # ==================== GET VIOLATION SEVERITY (EXISTING) ====================
    @classmethod
    def get_violation_severity(cls, violation_type: str) -> int:
        """
        Get severity level for violation type
        EXISTING METHOD - No changes
        
        Args:
            violation_type: Type of violation
        
        Returns:
            int: Severity level (1-4)
        """
        severity_map = {
            "Eyes closed": 2,
            "Head turned": 1,
            "Hand near face": 2,
            "Face not visible": 3,
            "Multiple faces detected": 4,
            "Lying down": 3,
            "Inactivity detected": 3
        }
        return severity_map.get(violation_type, 1)
    
    # ==================== GET IDENTITY WARNING MESSAGE (NEW) ====================
    @classmethod
    def get_identity_warning_message(cls, warning_number: int) -> str:
        """
        ✅ NEW METHOD - Get identity warning message for specific warning number
        
        Args:
            warning_number: Warning number (1, 2, or 3)
        
        Returns:
            str: Warning message to display to user
        
        Example:
            >>> AttendanceConfig.get_identity_warning_message(1)
            "⚠️ Warning 1/3: Unknown person detected..."
        """
        if warning_number == 1:
            return cls.IDENTITY_MESSAGES['warning_1']
        elif warning_number == 2:
            return cls.IDENTITY_MESSAGES['warning_2']
        elif warning_number >= 3:
            return cls.IDENTITY_MESSAGES['warning_3']
        return ""
    
    # ==================== GET IDENTITY ACTION TYPE (NEW) ====================
    @classmethod
    def get_identity_action_type(cls, warning_number: int) -> str:
        """
        ✅ NEW METHOD - Get action type for identity warning
        
        Args:
            warning_number: Warning number (1, 2, or 3)
        
        Returns:
            str: Action type identifier
        
        Example:
            >>> AttendanceConfig.get_identity_action_type(1)
            "identity_warning_1"
        """
        if warning_number == 1:
            return "identity_warning_1"
        elif warning_number == 2:
            return "identity_warning_2"
        elif warning_number >= 3:
            return "identity_removal"
        return "identity_unknown"
    
    # ==================== IS IDENTITY THRESHOLD REACHED (NEW) ====================
    @classmethod
    def is_identity_threshold_reached(cls, consecutive_seconds: int) -> bool:
        """
        ✅ NEW METHOD - Check if identity unknown threshold has been reached
        
        Args:
            consecutive_seconds: Number of consecutive seconds with unknown person
        
        Returns:
            bool: True if threshold reached and warning should be issued
        
        Example:
            >>> AttendanceConfig.is_identity_threshold_reached(5)
            True
            >>> AttendanceConfig.is_identity_threshold_reached(4)
            False
        """
        return consecutive_seconds >= cls.IDENTITY_UNKNOWN_THRESHOLD
    
    # ==================== SHOULD CHECK IDENTITY (NEW) ====================
    @classmethod
    def should_check_identity(cls, last_check_time: float, current_time: float) -> bool:
        """
        ✅ NEW METHOD - Check if enough time has passed for identity verification
        
        Args:
            last_check_time: Timestamp of last identity check
            current_time: Current timestamp
        
        Returns:
            bool: True if identity should be checked now
        
        Example:
            >>> AttendanceConfig.should_check_identity(1000.0, 1001.5)
            True  # 1.5 seconds passed, check interval is 1.0
        """
        return (current_time - last_check_time) >= cls.IDENTITY_CHECK_INTERVAL
    
    # ==================== GET IDENTITY REMOVAL REASON (NEW) ====================
    @classmethod
    def get_identity_removal_reason(cls, warning_count: int, total_unknown_seconds: int) -> str:
        """
        ✅ NEW METHOD - Get detailed removal reason for identity verification failure
        
        Args:
            warning_count: Number of warnings issued
            total_unknown_seconds: Total seconds with unknown person
        
        Returns:
            str: Detailed removal reason
        
        Example:
            >>> AttendanceConfig.get_identity_removal_reason(3, 20)
            "Identity verification failed: 3 warnings (20 seconds unknown person detected)"
        """
        return f"Identity verification failed: {warning_count} warnings ({total_unknown_seconds} seconds unknown person detected)"
    
    # ==================== GET TIME UNTIL IDENTITY WARNING (NEW) ====================
    @classmethod
    def get_time_until_identity_warning(cls, consecutive_seconds: int) -> int:
        """
        ✅ NEW METHOD - Get seconds remaining until next identity warning
        
        Args:
            consecutive_seconds: Current consecutive seconds count
        
        Returns:
            int: Seconds remaining (0 if threshold reached)
        
        Example:
            >>> AttendanceConfig.get_time_until_identity_warning(3)
            2  # 2 more seconds until warning
        """
        return max(0, cls.IDENTITY_UNKNOWN_THRESHOLD - consecutive_seconds)
    
    # ==================== FORMAT IDENTITY STATS (NEW) ====================
    @classmethod
    def format_identity_stats(cls, warning_count: int, consecutive_seconds: int, 
                             total_unknown_seconds: int) -> dict:
        """
        ✅ NEW METHOD - Format identity verification statistics for display
        
        Args:
            warning_count: Number of warnings issued
            consecutive_seconds: Current consecutive seconds
            total_unknown_seconds: Total unknown seconds
        
        Returns:
            dict: Formatted statistics
        
        Example:
            >>> AttendanceConfig.format_identity_stats(2, 3, 15)
            {
                'warnings': '2/3',
                'consecutive': '3/5 seconds',
                'total_unknown': '15 seconds',
                'status': 'warning',
                'next_warning_in': '2 seconds'
            }
        """
        return {
            'warnings': f"{warning_count}/{cls.IDENTITY_MAX_WARNINGS}",
            'consecutive': f"{consecutive_seconds}/{cls.IDENTITY_UNKNOWN_THRESHOLD} seconds",
            'total_unknown': f"{total_unknown_seconds} seconds",
            'status': 'removed' if warning_count >= cls.IDENTITY_MAX_WARNINGS else 
                     'warning' if warning_count > 0 else 'verified',
            'next_warning_in': f"{cls.get_time_until_identity_warning(consecutive_seconds)} seconds"
        }
    
    # ==================== CONFIGURATION DISPLAY (EXISTING) ====================
    @classmethod
    def display_config(cls):
        """
        Display all configuration settings
        EXISTING METHOD - Enhanced with identity settings
        """
        print("="*80)
        print("ATTENDANCE SYSTEM CONFIGURATION")
        print("="*80)
        
        print("\n📊 DETECTION THRESHOLDS:")
        print(f"  EAR Threshold: {cls.EAR_THRESHOLD}")
        print(f"  Head Yaw Threshold: {cls.HEAD_YAW_THRESHOLD}°")
        print(f"  Hand-Face Distance: {cls.HAND_FACE_DISTANCE}")
        
        print("\n⏱️  TIME INTERVALS:")
        print(f"  Inactivity Warning: {cls.INACTIVITY_WARNING_TIME}s")
        print(f"  Detection Interval: {cls.DETECTION_INTERVAL}s")
        print(f"  Auto Removal Time: {cls.VIOLATION_AUTO_REMOVAL_TIME}s")
        print(f"  Break Duration: {cls.BREAK_DURATION}s")
        
        print("\n⚠️  WARNING SYSTEM:")
        print(f"  Max Warnings: {cls.MAX_WARNING_MESSAGES}")
        print(f"  Detection Penalty: {cls.DETECTION_PENALTY_3}%")
        print(f"  Continuous Violation Penalty: {cls.CONTINUOUS_2MIN_PENALTY}%")
        
        print("\n🔐 IDENTITY VERIFICATION:")
        print(f"  Check Interval: {cls.IDENTITY_CHECK_INTERVAL}s")
        print(f"  Unknown Threshold: {cls.IDENTITY_UNKNOWN_THRESHOLD}s")
        print(f"  Max Warnings: {cls.IDENTITY_MAX_WARNINGS}")
        print(f"  Face Threshold: {cls.IDENTITY_FACE_THRESHOLD}")
        
        print("\n⏸️  BREAK SYSTEM:")
        print(f"  Max Break Time: {cls.MAX_TOTAL_BREAK_TIME}s")
        print(f"  Camera Verification Timeout: {cls.CAMERA_VERIFICATION_TIMEOUT}s")
        print(f"  Grace Period: {cls.GRACE_PERIOD_DURATION}s")
        
        print("="*80)


# ==================== VALIDATE CONFIGURATION ON MODULE LOAD ====================
try:
    AttendanceConfig.validate_config()
    AttendanceConfig.validate_identity_config()
    logger.info("✅ Configuration validated successfully")
except AssertionError as e:
    logger.error(f"❌ Configuration validation failed: {e}")
    raise


class ViolationSeverity:
    """Violation severity levels"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4

VIOLATION_SEVERITY = {
    "Eyes closed": ViolationSeverity.MEDIUM,
    "Head turned": ViolationSeverity.LOW,
    "Hand near face": ViolationSeverity.MEDIUM,
    "Face not visible": ViolationSeverity.HIGH,
    "Multiple faces detected": ViolationSeverity.CRITICAL,
    "Lying down": ViolationSeverity.HIGH,
    "Inactivity detected": ViolationSeverity.HIGH
}

# ==================== MODELS ====================

class AttendanceSession(models.Model):
    """Enhanced attendance tracking with grace period support and identity verification and 120-second removal tracking"""
    meeting_id = models.CharField(max_length=20, db_column='Meeting_ID')
    user_id = models.CharField(max_length=100, db_column='User_ID')
    
    # ==================== BEHAVIOR VIOLATION FIELDS (EXISTING - NO CHANGES) ====================
    popup_count = models.IntegerField(default=0)
    detection_counts = models.TextField(default='0')
    violation_start_times = models.TextField(default='{}')
    total_detections = models.IntegerField(default=0)
    attendance_penalty = models.FloatField(default=0.0)
    session_active = models.BooleanField(default=False)
    break_used = models.BooleanField(default=False)
    violations = models.TextField(default='[]')
    session_start_time = models.DateTimeField(default=timezone.now)
    last_activity = models.DateTimeField(default=timezone.now)
    
    last_face_movement_time = models.FloatField(default=0.0)
    inactivity_popup_shown = models.BooleanField(default=False)
    last_popup_time = models.FloatField(default=0.0)
    
    total_session_time = models.IntegerField(default=0)
    active_participation_time = models.IntegerField(default=0)
    violation_severity_score = models.FloatField(default=0.0)
    frame_processing_count = models.IntegerField(default=0)
    last_violation_type = models.CharField(max_length=50, blank=True)
    continuous_violation_time = models.IntegerField(default=0)
    
    # ==================== BREAK MANAGEMENT FIELDS (EXISTING - NO CHANGES) ====================
    total_break_time_used = models.IntegerField(default=0)
    current_break_start_time = models.FloatField(null=True, blank=True)
    break_sessions = models.TextField(default='[]')
    max_break_time_allowed = models.IntegerField(default=300)
    is_currently_on_break = models.BooleanField(default=False)
    break_count = models.IntegerField(default=0)
    last_break_calculation = models.FloatField(default=0.0)
    
    # ==================== PERFORMANCE METRICS (EXISTING - NO CHANGES) ====================
    engagement_score = models.DecimalField(max_digits=5, decimal_places=2, default=100.00)
    attendance_percentage = models.DecimalField(max_digits=5, decimal_places=2, default=100.00)
    focus_score = models.DecimalField(max_digits=5, decimal_places=2, default=100.00)
    
    # ==================== IDENTITY VERIFICATION FIELDS (EXISTING - NO CHANGES) ====================
    identity_warning_count = models.IntegerField(default=0)  # 0-3 warnings for identity mismatch
    identity_consecutive_unknown_seconds = models.IntegerField(default=0)  # Current consecutive streak
    identity_total_unknown_seconds = models.IntegerField(default=0)  # Total unknown time across session
    identity_is_removed = models.BooleanField(default=False)  # Removed due to identity verification failure
    identity_removal_time = models.DateTimeField(null=True, blank=True)  # When removed for identity issues
    identity_can_rejoin = models.BooleanField(default=True)  # Permission to rejoin after identity removal
    identity_warnings = models.TextField(default='[]')  # JSON list of warning events with timestamps
    identity_last_check_time = models.FloatField(default=0.0)  # Last identity verification check timestamp
    
    # ==================== REMOVAL TRACKING COLUMNS ====================
    identity_removal_count = models.IntegerField(default=0)  # Number of times removed for identity issues
    identity_total_warnings_issued = models.IntegerField(default=0)  # Total warnings: 1,2,3,4,5,6... (cumulative)
    identity_current_cycle_warnings = models.IntegerField(default=0)  # Current cycle: 0-3 (resets after removal)
    behavior_removal_count = models.IntegerField(default=0)  # Number of times removed for 2-min violations
    continuous_violation_removal_count = models.IntegerField(default=0)  # ✅ NEW: Number of 120-second removals
    
    # ==================== SYSTEM FIELDS (EXISTING - NO CHANGES) ====================
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'tbl_Attendance_Sessions'
        unique_together = ['meeting_id', 'user_id']
        indexes = [
            models.Index(fields=['meeting_id']),
            models.Index(fields=['user_id']),
            models.Index(fields=['session_active']),
            models.Index(fields=['created_at']),
        ]
    
    def get_violation_list(self) -> List[Dict]:
        """
        Get list of behavior violations
        EXISTING METHOD - No changes
        """
        try:
            return json.loads(self.violations) if self.violations else []
        except json.JSONDecodeError:
            return []
    
    def get_identity_warnings(self) -> List[Dict]:
        """
        ✅ EXISTING METHOD - Get identity warning history
        
        Returns:
            List of warning dictionaries with timestamps and details
        
        Example return:
        [
            {
                'timestamp': 1234567890.0,
                'warning_number': 1,
                'consecutive_seconds': 5,
                'similarity': 0.45,
                'total_unknown_seconds': 5
            },
            ...
        ]
        """
        try:
            return json.loads(self.identity_warnings) if self.identity_warnings else []
        except json.JSONDecodeError:
            return []

        
    def get_behavior_messages(self) -> Dict:
        """
        ✅ ENHANCED METHOD - Get behavior warning, detection, and 120-second removal messages from violations column
        
        Returns:
            Dict: {
                'warnings': [
                    {
                        'timestamp': 1763356547.09,
                        'warning_number': 1,
                        'violation_type': 'Eyes closed',
                        'message': '⚠️ Warning 1/4: Eyes closed',
                        'time_range': '0-20s',
                        'duration': 21.3
                    },
                    ...
                ],
                'detections': [
                    {
                        'timestamp': 1763357233.76,
                        'detection_number': 1,
                        'violation_type': 'Hand near face',
                        'message': '🔴 Detection 1: Hand near face',
                        'time_range': '60-80s',
                        'penalty_applied': 0.0,
                        'duration': 65.1
                    },
                    ...
                ],
                'continuous_removals': [
                    {
                        'timestamp': 1763377500.123456,
                        'removal_number': 1,
                        'violation_type': 'Face not visible',
                        'duration': 125.5,
                        'time_range': '120s+',
                        'removal_type': '120_second_continuous',
                        'penalty_applied': 1.0,
                        'message': '🚫 Removed: Continuous Face not visible for 125 seconds'
                    },
                    ...
                ]
            }
        
        Example:
            >>> session = AttendanceSession.objects.get(meeting_id='123', user_id='456')
            >>> messages = session.get_behavior_messages()
            >>> print(f"Warnings: {len(messages['warnings'])}")
            >>> print(f"Detections: {len(messages['detections'])}")
            >>> print(f"120s Removals: {len(messages['continuous_removals'])}")
        """
        try:
            messages = json.loads(self.violations) if self.violations else {}
            
            # Handle legacy format (if violations was a simple list)
            if isinstance(messages, list):
                return {'warnings': [], 'detections': [], 'continuous_removals': []}
            
            # Ensure all keys exist
            if not isinstance(messages, dict):
                return {'warnings': [], 'detections': [], 'continuous_removals': []}
            
            return {
                'warnings': messages.get('warnings', []),
                'detections': messages.get('detections', []),
                'continuous_removals': messages.get('continuous_removals', [])  # ✅ NEW
            }
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse violations for {self.user_id}")
            return {'warnings': [], 'detections': [], 'continuous_removals': []}
    
    def add_behavior_warning(self, warning_number: int, violation_type: str,
                            duration: float, timestamp: float):
        """
        ✅ EXISTING METHOD - Add a behavior warning message to violations column
        
        Args:
            warning_number (int): Warning number (1, 2, 3, or 4)
            violation_type (str): Type of violation (e.g., "Eyes closed")
            duration (float): Duration of violation in seconds
            timestamp (float): Unix timestamp when warning was issued
        
        Example:
            >>> session.add_behavior_warning(
            ...     warning_number=1,
            ...     violation_type="Eyes closed",
            ...     duration=21.5,
            ...     timestamp=1763356547.09
            ... )
            >>> session.save()
        
        Notes:
            - Automatically categorizes duration into time ranges
            - Stores in violations column as JSON
            - Does NOT save to database (call .save() after)
        """
        messages = self.get_behavior_messages()
        
        # Categorize duration into time range
        time_range = self._get_time_range(duration)
        
        # Create warning event
        warning = {
            'timestamp': timestamp,
            'warning_number': warning_number,
            'violation_type': violation_type,
            'message': f"⚠️ Warning {warning_number}/{AttendanceConfig.MAX_WARNING_MESSAGES}: {violation_type}",
            'time_range': time_range,
            'duration': round(duration, 2),
            'description': AttendanceConfig.DESCRIPTIONS.get(violation_type, ''),
        }
        
        # Add to warnings list
        messages['warnings'].append(warning)
        
        # Save back to violations column
        self.violations = json.dumps(messages)
        
        logger.debug(
            f"Added warning #{warning_number} for {self.user_id}: "
            f"{violation_type} ({duration:.1f}s)"
        )
    
    def add_behavior_detection(self, detection_number: int, violation_type: str,
                               duration: float, timestamp: float, penalty: float):
        """
        ✅ EXISTING METHOD - Add a behavior detection message to violations column
        
        Args:
            detection_number (int): Detection number (1, 2, 3, ...)
            violation_type (str): Type of violation (e.g., "Hand near face")
            duration (float): Duration of violation in seconds
            timestamp (float): Unix timestamp when detection was issued
            penalty (float): Penalty percentage applied (0.0 or 0.25)
        
        Example:
            >>> session.add_behavior_detection(
            ...     detection_number=3,
            ...     violation_type="Hand near face",
            ...     duration=65.1,
            ...     timestamp=1763357233.76,
            ...     penalty=0.25
            ... )
            >>> session.save()
        
        Notes:
            - Automatically categorizes duration into time ranges
            - Stores in violations column as JSON
            - Does NOT save to database (call .save() after)
        """
        messages = self.get_behavior_messages()
        
        # Categorize duration into time range
        time_range = self._get_time_range(duration)
        
        # Create detection event
        detection = {
            'timestamp': timestamp,
            'detection_number': detection_number,
            'violation_type': violation_type,
            'message': f"🔴 Detection {detection_number}: {violation_type}. Penalty applied: {penalty:.2f}%",
            'time_range': time_range,
            'penalty_applied': round(penalty, 4),
            'duration': round(duration, 2),
            'description': AttendanceConfig.DESCRIPTIONS.get(violation_type, ''),
        }
        
        # Add to detections list
        messages['detections'].append(detection)
        
        # Save back to violations column
        self.violations = json.dumps(messages)
        
        logger.debug(
            f"Added detection #{detection_number} for {self.user_id}: "
            f"{violation_type} ({duration:.1f}s) - Penalty: {penalty:.4f}%"
        )
    
    def add_continuous_violation_removal(self, duration: float, timestamp: float, violation_type: str):
        """
        ✅ NEW METHOD - Record a 120-second continuous violation removal event
        
        Args:
            duration (float): Actual duration of continuous violation (should be 120+)
            timestamp (float): Unix timestamp when removal occurred
            violation_type (str): Type of violation that caused removal
        
        Stores in violations column under 'continuous_removals' key with actual 120+ seconds duration
        
        Example:
            >>> session.add_continuous_violation_removal(
            ...     duration=125.5,
            ...     timestamp=1763377500.123456,
            ...     violation_type="Face not visible"
            ... )
            >>> session.save()
        
        Notes:
            - Stores ACTUAL continuous duration (120+), not individual violation periods (21s)
            - Automatically sets time_range to "120s+"
            - Increments continuous_violation_removal_count automatically in calling code
            - Does NOT save to database (call .save() after)
        """
        try:
            messages = self.get_behavior_messages()
            
            # Ensure continuous_removals key exists
            if 'continuous_removals' not in messages:
                messages['continuous_removals'] = []
            
            # Create removal event
            removal_event = {
                'timestamp': timestamp,
                'removal_number': self.continuous_violation_removal_count + 1,
                'violation_type': violation_type,
                'duration': round(duration, 2),  # ✅ ACTUAL 120+ seconds
                'message': f"🚫 Removed: Continuous {violation_type} for {duration:.0f} seconds",
                'time_range': '120s+',  # ✅ Always 120s+ for continuous removals
                'removal_type': '120_second_continuous',
                'penalty_applied': 1.0,  # 1% penalty for 120-second removal
                'description': f'User removed due to continuous {violation_type} for over 2 minutes'
            }
            
            # Add to removals list
            messages['continuous_removals'].append(removal_event)
            
            # Save back to violations column
            self.violations = json.dumps(messages)
            
            logger.info(
                f"📝 120-SECOND REMOVAL EVENT RECORDED:\n"
                f"   User: {self.user_id}\n"
                f"   Removal #{self.continuous_violation_removal_count + 1}\n"
                f"   Duration: {duration:.1f}s (ACTUAL continuous time)\n"
                f"   Violation: {violation_type}\n"
                f"   Time Range: 120s+"
            )
            
        except Exception as e:
            logger.error(f"Failed to record continuous violation removal: {e}")
            logger.error(traceback.format_exc())
    
    def get_continuous_removal_events(self) -> List[Dict]:
        """
        ✅ NEW METHOD - Get list of 120-second removal events
        
        Returns:
            List[Dict]: List of removal event dictionaries with actual 120+ durations
        
        Example return:
        [
            {
                'timestamp': 1763377500.123456,
                'removal_number': 1,
                'violation_type': 'Face not visible',
                'duration': 125.5,
                'time_range': '120s+',
                'removal_type': '120_second_continuous',
                'penalty_applied': 1.0,
                'message': '🚫 Removed: Continuous Face not visible for 125 seconds'
            },
            {
                'timestamp': 1763378600.789012,
                'removal_number': 2,
                'violation_type': 'Hand near face',
                'duration': 132.3,
                'time_range': '120s+',
                'removal_type': '120_second_continuous',
                'penalty_applied': 1.0,
                'message': '🚫 Removed: Continuous Hand near face for 132 seconds'
            }
        ]
        
        Example:
            >>> session = AttendanceSession.objects.get(meeting_id='123', user_id='456')
            >>> removals = session.get_continuous_removal_events()
            >>> print(f"Total 120-second removals: {len(removals)}")
            >>> for event in removals:
            ...     print(f"Removal #{event['removal_number']}: {event['violation_type']} - {event['duration']}s")
        
        Notes:
            - Returns events from violations column's 'continuous_removals' array
            - Duration values are ACTUAL 120+ seconds, not 21 seconds
            - Count should match continuous_violation_removal_count column
        """
        try:
            messages = self.get_behavior_messages()
            return messages.get('continuous_removals', [])
        except Exception as e:
            logger.warning(f"Failed to get continuous removal events for {self.user_id}: {e}")
            return []
    
    def _get_time_range(self, duration: float) -> str:
        """
        ✅ EXISTING METHOD - Categorize violation duration into time range
        
        Args:
            duration (float): Duration in seconds
        
        Returns:
            str: Time range category
        
        Time Range Categories:
            - '0-20s': 0 to <20 seconds
            - '20-40s': 20 to <40 seconds
            - '40-60s': 40 to <60 seconds
            - '60-80s': 60 to <80 seconds
            - '80-100s': 80 to <100 seconds
            - '100-120s': 100 to <120 seconds
            - '120s+': 120+ seconds
        
        Example:
            >>> session._get_time_range(15.5)
            '0-20s'
            >>> session._get_time_range(45.2)
            '40-60s'
            >>> session._get_time_range(125.0)
            '120s+'
        
        Notes:
            - Used for analytics and message categorization
            - Helps identify violation patterns by duration
        """
        if duration < 20:
            return '0-20s'
        elif duration < 40:
            return '20-40s'
        elif duration < 60:
            return '40-60s'
        elif duration < 80:
            return '60-80s'
        elif duration < 100:
            return '80-100s'
        elif duration < 120:
            return '100-120s'
        else:
            return '120s+'
    
    def get_behavior_warning_count(self) -> int:
        """
        ✅ EXISTING METHOD - Get total number of behavior warnings issued
        
        Returns:
            int: Number of warnings in history
        
        Example:
            >>> session.get_behavior_warning_count()
            4
        """
        messages = self.get_behavior_messages()
        return len(messages.get('warnings', []))
    
    def get_behavior_detection_count(self) -> int:
        """
        ✅ EXISTING METHOD - Get total number of behavior detections issued
        
        Returns:
            int: Number of detections in history
        
        Example:
            >>> session.get_behavior_detection_count()
            3
        """
        messages = self.get_behavior_messages()
        return len(messages.get('detections', []))
    
    def get_continuous_removal_count(self) -> int:
        """
        ✅ NEW METHOD - Get total number of 120-second removal events
        
        Returns:
            int: Number of 120-second removal events in history
        
        Example:
            >>> session.get_continuous_removal_count()
            2
        
        Notes:
            - Returns count from violations column's 'continuous_removals' array
            - Should match continuous_violation_removal_count column value
        """
        messages = self.get_behavior_messages()
        return len(messages.get('continuous_removals', []))
    
    def get_latest_behavior_warning(self) -> Optional[Dict]:
        """
        ✅ EXISTING METHOD - Get the most recent behavior warning
        
        Returns:
            Optional[Dict]: Latest warning message or None if no warnings
        
        Example:
            >>> latest = session.get_latest_behavior_warning()
            >>> if latest:
            ...     print(f"Last warning: {latest['violation_type']} at {latest['timestamp']}")
        """
        messages = self.get_behavior_messages()
        warnings = messages.get('warnings', [])
        return warnings[-1] if warnings else None
    
    def get_latest_behavior_detection(self) -> Optional[Dict]:
        """
        ✅ EXISTING METHOD - Get the most recent behavior detection
        
        Returns:
            Optional[Dict]: Latest detection message or None if no detections
        
        Example:
            >>> latest = session.get_latest_behavior_detection()
            >>> if latest:
            ...     print(f"Last detection: {latest['violation_type']} - Penalty: {latest['penalty_applied']}%")
        """
        messages = self.get_behavior_messages()
        detections = messages.get('detections', [])
        return detections[-1] if detections else None
    
    def get_latest_continuous_removal(self) -> Optional[Dict]:
        """
        ✅ NEW METHOD - Get the most recent 120-second removal event
        
        Returns:
            Optional[Dict]: Latest removal event or None if no removals
        
        Example:
            >>> latest = session.get_latest_continuous_removal()
            >>> if latest:
            ...     print(f"Last 120s removal: {latest['violation_type']} - Duration: {latest['duration']}s")
        """
        messages = self.get_behavior_messages()
        removals = messages.get('continuous_removals', [])
        return removals[-1] if removals else None
    
    def clear_behavior_messages(self):
        """
        ✅ ENHANCED METHOD - Clear all behavior warning/detection/removal messages
        
        Use Cases:
            - Manual reset by admin
            - Session restart
            - Testing/debugging
        
        Example:
            >>> session.clear_behavior_messages()
            >>> session.save()
        
        Warning:
            - This will permanently delete all warning/detection/removal history
            - Cannot be undone
        """
        self.violations = json.dumps({'warnings': [], 'detections': [], 'continuous_removals': []})
        logger.info(f"Cleared all behavior messages for {self.user_id}")
    
    def get_total_removals(self) -> int:
        """
        ✅ NEW METHOD - Get total number of removals (all types)
        
        Returns:
            int: Sum of identity_removal_count + behavior_removal_count
        
        Example:
            >>> session.get_total_removals()
            5  # 2 identity + 3 behavior
        
        Notes:
            - Does NOT include continuous_violation_removal_count 
              (that's a subset of behavior_removal_count)
        """
        return self.identity_removal_count + self.behavior_removal_count
    
    def get_removal_summary(self) -> Dict:
        """
        ✅ NEW METHOD - Get comprehensive removal statistics
        
        Returns:
            Dict: Summary of all removal types with counts and events
        
        Example:
            >>> summary = session.get_removal_summary()
            >>> print(summary)
            {
                'identity_removals': 2,
                'behavior_removals': 3,
                'continuous_120s_removals': 2,
                'total_removals': 5,
                'identity_warnings_issued': 6,
                'identity_current_cycle': 0,
                'continuous_removal_events': [...]
            }
        """
        return {
            'identity_removals': self.identity_removal_count,
            'behavior_removals': self.behavior_removal_count,
            'continuous_120s_removals': self.continuous_violation_removal_count,
            'total_removals': self.get_total_removals(),
            'identity_warnings_issued': self.identity_total_warnings_issued,
            'identity_current_cycle': self.identity_current_cycle_warnings,
            'continuous_removal_events': self.get_continuous_removal_events(),
            'behavior_warnings': self.get_behavior_warning_count(),
            'behavior_detections': self.get_behavior_detection_count(),
            'continuous_violation_time': self.continuous_violation_time,
            'last_violation_type': self.last_violation_type,
        }
        
# ==================== MEDIAPIPE INITIALIZATION ====================

mp_face = mp.solutions.face_detection.FaceDetection(min_detection_confidence=AttendanceConfig.FACE_DETECTION_CONFIDENCE)
mp_mesh = mp.solutions.face_mesh.FaceMesh(refine_landmarks=True, min_detection_confidence=0.5, min_tracking_confidence=0.5)
mp_pose = mp.solutions.pose.Pose(min_detection_confidence=0.5)
mp_hands = mp.solutions.hands.Hands(min_detection_confidence=0.5)

attendance_sessions = {}

def release_face_model_gpu():
    """Release face model GPU memory after detection"""
    try:
        from face_embeddings import face_model
        if face_model is not None:
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                logger.debug("GPU cache cleared after face detection")
    except Exception as e:
        logger.debug(f"Could not clear GPU cache: {e}")

# ==================== UTILITY FUNCTIONS ====================

def validate_session_data(meeting_id: str, user_id):
    """Validate session data"""
    user_id = str(user_id)
    if not meeting_id or not user_id:
        raise ValidationError("meeting_id and user_id are required")
    if len(meeting_id) > 36:
        raise ValidationError("meeting_id too long")
    if len(user_id) > 100:
        raise ValidationError("user_id too long")
    
    session_key = get_session_key(meeting_id, user_id)
    concurrent_sessions = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
    logger.debug(f"MULTI-USER: Validation for {user_id}. {len(concurrent_sessions)} sessions active")

def get_session_key(meeting_id: str, user_id: str) -> str:
    """Generate unique session key"""
    return f"{meeting_id}_{user_id}"

def decode_image(b64: str) -> Optional[np.ndarray]:
    """Decode base64 image"""
    try:
        b64 = b64.split(',')[1] if ',' in b64 else b64
        image = Image.open(io.BytesIO(base64.b64decode(b64)))
        return cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    except Exception as e:
        logger.error(f"Error decoding image: {e}")
        return None

def enhanced_ear(left_eye: List, right_eye: List) -> float:
    """Calculate Enhanced Eye Aspect Ratio"""
    try:
        A = euclidean((left_eye[1].x, left_eye[1].y), (left_eye[5].x, left_eye[5].y))
        B = euclidean((left_eye[2].x, left_eye[2].y), (left_eye[4].x, left_eye[4].y))
        C = euclidean((left_eye[0].x, left_eye[0].y), (left_eye[3].x, left_eye[3].y))
        left_ear = (A + B) / (2.0 * C)
        
        A = euclidean((right_eye[1].x, right_eye[1].y), (right_eye[5].x, right_eye[5].y))
        B = euclidean((right_eye[2].x, right_eye[2].y), (right_eye[4].x, right_eye[4].y))
        C = euclidean((right_eye[0].x, right_eye[0].y), (right_eye[3].x, right_eye[3].y))
        right_ear = (A + B) / (2.0 * C)
        
        return (left_ear + right_ear) / 2
    except Exception as e:
        logger.error(f"Error calculating EAR: {e}")
        return 0.25

def is_fully_lying_down(landmarks) -> bool:
    """Check if person is lying down"""
    try:
        y_vals = [landmarks[i].y for i in [11, 12, 23, 24, 25, 26]]
        return np.std(y_vals) < AttendanceConfig.POSE_VARIANCE_THRESHOLD
    except Exception as e:
        logger.error(f"Error checking pose: {e}")
        return False

def get_extended_tracking_data(attendance_obj):
    """Get extended tracking data from database"""
    try:
        if hasattr(attendance_obj, 'detection_counts') and attendance_obj.detection_counts:
            if attendance_obj.detection_counts.startswith('{'):
                return json.loads(attendance_obj.detection_counts)
            else:
                return {
                    'detection_counts': int(attendance_obj.detection_counts) if attendance_obj.detection_counts.isdigit() else 0,
                    'warning_count': 0,
                    'is_removed_from_meeting': False,
                    'removal_timestamp': None,
                    'removal_reason': '',
                    'continuous_violation_start_time': None,
                    'last_detection_time': 0.0,
                    'detection_penalty_applied': False,
                    'warning_phase_complete': False,
                    'camera_resume_expected': False,
                    'camera_resume_deadline': None,
                    'camera_confirmation_token': None,
                    'camera_verified_at': None,
                    'grace_period_active': False,
                    'grace_period_until': None,
                    # ✅ NEW: Detection penalty tracking fields
                    'total_detection_penalty': 0.0,
                    'detection_batches_completed': 0,
                }
        else:
            return {
                'detection_counts': 0,
                'warning_count': 0,
                'is_removed_from_meeting': False,
                'removal_timestamp': None,
                'removal_reason': '',
                'continuous_violation_start_time': None,
                'last_detection_time': 0.0,
                'detection_penalty_applied': False,
                'warning_phase_complete': False,
                'camera_resume_expected': False,
                'camera_resume_deadline': None,
                'camera_confirmation_token': None,
                'camera_verified_at': None,
                'grace_period_active': False,
                'grace_period_until': None,
                # ✅ NEW: Detection penalty tracking fields
                'total_detection_penalty': 0.0,
                'detection_batches_completed': 0,
            }
    except (json.JSONDecodeError, AttributeError):
        return {
            'detection_counts': 0,
            'warning_count': 0,
            'is_removed_from_meeting': False,
            'removal_timestamp': None,
            'removal_reason': '',
            'continuous_violation_start_time': None,
            'last_detection_time': 0.0,
            'detection_penalty_applied': False,
            'warning_phase_complete': False,
            'camera_resume_expected': False,
            'camera_resume_deadline': None,
            'camera_confirmation_token': None,
            'camera_verified_at': None,
            'grace_period_active': False,
            'grace_period_until': None,
            # ✅ NEW: Detection penalty tracking fields
            'total_detection_penalty': 0.0,
            'detection_batches_completed': 0,
        }

def save_extended_tracking_data(attendance_obj, extended_data):
    """Save extended tracking data"""
    try:
        attendance_obj.detection_counts = json.dumps(extended_data)
        attendance_obj.save()
        logger.info(f"DB SAVE: Extended tracking data for {attendance_obj.user_id}")
    except Exception as e:
        logger.error(f"Failed to save extended tracking data: {e}")

def calculate_current_break_time(session, current_time):
    """Calculate current break time"""
    if session.get('is_currently_on_break') and session.get('current_break_start_time'):
        current_break_duration = current_time - session['current_break_start_time']
        return session.get('total_break_time_used', 0) + current_break_duration
    return session.get('total_break_time_used', 0)

def update_break_time_used(session, attendance_obj, current_time):
    """Update break time used"""
    if session.get('is_currently_on_break') and session.get('current_break_start_time'):
        current_break_duration = current_time - session['current_break_start_time']
        session['total_break_time_used'] += current_break_duration
        
        break_session = {
            'start_time': session['current_break_start_time'],
            'end_time': current_time,
            'duration': current_break_duration,
            'break_number': session.get('break_count', 0)
        }
        
        if 'break_sessions' not in session:
            session['break_sessions'] = []
        session['break_sessions'].append(break_session)
        
        attendance_obj.total_break_time_used = int(session['total_break_time_used'])
        attendance_obj.break_sessions = json.dumps(session['break_sessions'])
        
        logger.info(f"MULTI-USER: Break session recorded for {attendance_obj.user_id}: {current_break_duration:.1f}s")

def generate_camera_verification_token(meeting_id: str, user_id: str, timestamp: float) -> str:
    """Generate verification token"""
    import hashlib
    data = f"{meeting_id}_{user_id}_{timestamp}_{uuid.uuid4()}"
    return hashlib.sha256(data.encode()).hexdigest()[:32]

# ==================== HELPER: Run async code in sync context ====================

# ==================== IDENTITY VERIFICATION HELPER FUNCTIONS ====================

def run_async_verification(frame, user_id):
    """
    ✅ NEW FUNCTION - Helper function to run identity verification in sync context
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            from core.FaceAuth.unified_face_service import get_unified_face_service
            face_service = get_unified_face_service()
            
            result = loop.run_until_complete(
                face_service.verify_face(
                    frame=frame,
                    user_id=user_id,
                    threshold=AttendanceConfig.IDENTITY_FACE_THRESHOLD,
                    method='cosine'
                )
            )
            
            logger.debug(
                f"Identity verification result for {user_id}: "
                f"verified={result[0]}, similarity={result[1]:.3f}"
            )
            
            return result
            
        finally:
            loop.close()
            
    except ImportError as e:
        logger.error(f"Failed to import unified_face_service: {e}")
        logger.error("Identity verification will be skipped")
        return (True, 1.0)
        
    except Exception as e:
        logger.error(f"Error in identity verification for {user_id}: {e}")
        logger.error(traceback.format_exc())
        return (True, 1.0)


def check_identity_verification(session, db_session, frame, user_id, current_time):
    """
    ✅ ENHANCED: Track warnings per cycle (1/3, 2/3, 3/3) but store cumulative total
    ✅ NEW: Applies 1% attendance penalty when user is removed for identity verification
    
    New Features:
    - Uses identity_current_cycle_warnings for frontend display (always shows 1/3, 2/3, 3/3)
    - Uses identity_total_warnings_issued for backend tracking (1,2,3,4,5,6...)
    - Increments identity_removal_count when user is removed
    - Tracks behavior_removal_count separately (unchanged by this function)
    - Resets cycle warnings to 0 after removal (ready for next rejoin)
    - ✅ NEW: Applies 1% penalty to attendance_percentage on removal
    
    Returns:
        dict: Identity verification result with all tracking data
        None: If check should be skipped (too soon since last check)
    """
    # ============================================================
    # STEP 1: Check if enough time passed since last check (1 second interval)
    # ============================================================
    last_check = db_session.identity_last_check_time or 0
    time_since_last_check = current_time - last_check
    
    if time_since_last_check < AttendanceConfig.IDENTITY_CHECK_INTERVAL:
        logger.debug(
            f"Identity check skipped for {user_id}: "
            f"{time_since_last_check:.2f}s since last check"
        )
        return None
    
    # Update last check time
    db_session.identity_last_check_time = current_time
    
    # ============================================================
    # STEP 2: Run face verification
    # ============================================================
    is_verified, similarity = run_async_verification(frame, user_id)
    
    logger.debug(
        f"Identity check for {user_id}: "
        f"verified={is_verified}, "
        f"similarity={similarity:.3f}, "
        f"consecutive={db_session.identity_consecutive_unknown_seconds}s, "
        f"cycle_warnings={db_session.identity_current_cycle_warnings}/3, "
        f"total_warnings={db_session.identity_total_warnings_issued}, "
        f"identity_removals={db_session.identity_removal_count}, "
        f"behavior_removals={db_session.behavior_removal_count}"
    )
    
    # ============================================================
    # CASE A: REGISTERED PERSON DETECTED (Verified = True)
    # ============================================================
    if is_verified:
        previous_consecutive = db_session.identity_consecutive_unknown_seconds
        
        # If there was a consecutive count, reset it
        if previous_consecutive > 0:
            logger.info(
                f"\n{'='*70}\n"
                f"✅ IDENTITY VERIFIED\n"
                f"{'='*70}\n"
                f"User ID: {user_id}\n"
                f"Similarity: {similarity:.3f}\n"
                f"Previous Consecutive: {previous_consecutive}s\n"
                f"Action: Resetting consecutive counter to 0\n"
                f"Current Cycle Warnings: {db_session.identity_current_cycle_warnings}/3\n"
                f"Total Warnings Issued: {db_session.identity_total_warnings_issued}\n"
                f"Identity Removals: {db_session.identity_removal_count}\n"
                f"Behavior Removals: {db_session.behavior_removal_count}\n"
                f"{'='*70}\n"
            )
            
            # Reset consecutive counter
            db_session.identity_consecutive_unknown_seconds = 0
            db_session.save()
        
        # Update session state
        session['identity_consecutive_unknown_seconds'] = 0
        
        # Return verification success
        return {
            'identity_verified': True,
            'identity_similarity': similarity,
            'identity_warning_count': db_session.identity_current_cycle_warnings,  # Frontend: cycle count
            'identity_consecutive_unknown': 0,
            'identity_popup': None,
            
            # ✅ NEW: Removal tracking data
            'identity_removal_count': db_session.identity_removal_count,
            'identity_total_warnings': db_session.identity_total_warnings_issued,
            'identity_current_cycle_warnings': db_session.identity_current_cycle_warnings,
            'behavior_removal_count': db_session.behavior_removal_count,
        }
    
    # ============================================================
    # CASE B: UNKNOWN PERSON DETECTED (Verified = False)
    # ============================================================
    else:
        # Increment counters
        db_session.identity_consecutive_unknown_seconds += 1
        db_session.identity_total_unknown_seconds += 1
        
        consecutive = db_session.identity_consecutive_unknown_seconds
        total = db_session.identity_total_unknown_seconds
        
        logger.warning(
            f"\n{'='*70}\n"
            f"🚫 UNKNOWN PERSON DETECTED\n"
            f"{'='*70}\n"
            f"User ID: {user_id}\n"
            f"Similarity: {similarity:.3f} (threshold: {AttendanceConfig.IDENTITY_FACE_THRESHOLD})\n"
            f"Consecutive Seconds: {consecutive}/{AttendanceConfig.IDENTITY_UNKNOWN_THRESHOLD}\n"
            f"Total Unknown Time: {total}s\n"
            f"Current Cycle Warnings: {db_session.identity_current_cycle_warnings}/3\n"
            f"Total Warnings Issued: {db_session.identity_total_warnings_issued}\n"
            f"Identity Removals: {db_session.identity_removal_count}\n"
            f"Behavior Removals: {db_session.behavior_removal_count}\n"
            f"{'='*70}\n"
        )
        
        # Update session state
        session['identity_consecutive_unknown_seconds'] = consecutive
        session['identity_total_unknown_seconds'] = total
        
        # ============================================================
        # CHECK: Has threshold been reached? (5 consecutive seconds)
        # ============================================================
        if consecutive >= AttendanceConfig.IDENTITY_UNKNOWN_THRESHOLD:
            # ============================================================
            # THRESHOLD REACHED: Issue Warning
            # ============================================================
            
            # ✅ INCREMENT BOTH COUNTERS
            db_session.identity_current_cycle_warnings += 1  # Cycle: 1, 2, 3
            db_session.identity_total_warnings_issued += 1   # Total: 1, 2, 3, 4, 5, 6...
            
            current_cycle_warning = db_session.identity_current_cycle_warnings
            total_warnings = db_session.identity_total_warnings_issued
            
            # ============================================================
            # Record warning event in history
            # ============================================================
            warnings_list = db_session.get_identity_warnings()
            warning_event = {
                'timestamp': current_time,
                'cycle_warning_number': current_cycle_warning,  # 1, 2, or 3
                'total_warning_number': total_warnings,         # 1, 2, 3, 4, 5, 6...
                'consecutive_seconds': consecutive,
                'similarity': similarity,
                'total_unknown_seconds': total,
                'removal_cycle': db_session.identity_removal_count + 1,
                'identity_removals': db_session.identity_removal_count,
                'behavior_removals': db_session.behavior_removal_count,
            }
            warnings_list.append(warning_event)
            db_session.identity_warnings = json.dumps(warnings_list)
            
            # ============================================================
            # Reset consecutive counter (fresh start for next warning)
            # ============================================================
            db_session.identity_consecutive_unknown_seconds = 0
            
            # ============================================================
            # Update session state
            # ============================================================
            session['identity_warning_count'] = current_cycle_warning
            session['identity_consecutive_unknown_seconds'] = 0
            session['identity_warnings'] = warnings_list
            session['identity_total_warnings'] = total_warnings
            session['identity_removal_count'] = db_session.identity_removal_count
            session['identity_current_cycle_warnings'] = current_cycle_warning
            session['behavior_removal_count'] = db_session.behavior_removal_count
            
            popup_message = None
            action = None
            
            # ============================================================
            # DETERMINE WARNING LEVEL & MESSAGE
            # Frontend always sees: Warning 1/3, 2/3, 3/3
            # Backend tracks: Total 1, 2, 3, 4, 5, 6...
            # ============================================================
            
            if current_cycle_warning == 1:
                # ============================================================
                # WARNING #1 of 3
                # ============================================================
                popup_message = (
                    f"⚠️ Warning 1/3: Unknown person detected. "
                    f"Please ensure only you are visible in the camera."
                )
                action = "identity_warning_1"
                
                logger.error(
                    f"\n{'='*80}\n"
                    f"⚠️  IDENTITY WARNING #1/3 (Total: #{total_warnings})\n"
                    f"{'='*80}\n"
                    f"User: {user_id}\n"
                    f"Cycle Warning: 1/3\n"
                    f"Total Warnings Issued: {total_warnings}\n"
                    f"Identity Removal Count: {db_session.identity_removal_count}\n"
                    f"Behavior Removal Count: {db_session.behavior_removal_count}\n"
                    f"Reason: Unknown person for {AttendanceConfig.IDENTITY_UNKNOWN_THRESHOLD}+ seconds\n"
                    f"Similarity: {similarity:.3f}\n"
                    f"Total Unknown Time: {total}s\n"
                    f"{'='*80}\n"
                )
                
            elif current_cycle_warning == 2:
                # ============================================================
                # WARNING #2 of 3 - CRITICAL
                # ============================================================
                popup_message = (
                    f"⚠️⚠️ Warning 2/3: Identity verification failed again. "
                    f"ONE MORE failure will remove you from the meeting!"
                )
                action = "identity_warning_2"
                
                logger.error(
                    f"\n{'='*80}\n"
                    f"⚠️⚠️  IDENTITY WARNING #2/3 (Total: #{total_warnings}) - CRITICAL\n"
                    f"{'='*80}\n"
                    f"User: {user_id}\n"
                    f"Cycle Warning: 2/3\n"
                    f"Total Warnings Issued: {total_warnings}\n"
                    f"Identity Removal Count: {db_session.identity_removal_count}\n"
                    f"Behavior Removal Count: {db_session.behavior_removal_count}\n"
                    f"Similarity: {similarity:.3f}\n"
                    f"Total Unknown Time: {total}s\n"
                    f"⚠️  ONE MORE WARNING = REMOVAL!\n"
                    f"{'='*80}\n"
                )
                
            elif current_cycle_warning >= AttendanceConfig.IDENTITY_MAX_WARNINGS:
                # ============================================================
                # WARNING #3 of 3 - REMOVAL TRIGGERED
                # ============================================================
                popup_message = (
                    f"🚫 You have been removed from the meeting due to identity "
                    f"verification failure (3 warnings). You can rejoin after "
                    f"correcting the issue."
                )
                action = "identity_removal"
                
                # ✅ INCREMENT IDENTITY REMOVAL COUNTER
                db_session.identity_removal_count += 1
                
                # ============================================================
                # ✅ NEW: APPLY 1% ATTENDANCE PENALTY FOR IDENTITY REMOVAL
                # ============================================================
                identity_removal_penalty = 1.0  # 1% penalty per identity removal
                
                # Get current penalty from session
                current_penalty = session.get('attendance_penalty', 0.0)
                
                # Add identity removal penalty
                session['attendance_penalty'] = current_penalty + identity_removal_penalty
                
                # Calculate new attendance percentage
                new_attendance_percentage = max(0, 100 - session['attendance_penalty'])
                
                # Update database with new penalty and percentages
                db_session.attendance_penalty = session['attendance_penalty']
                db_session.attendance_percentage = new_attendance_percentage
                db_session.engagement_score = new_attendance_percentage
                db_session.focus_score = new_attendance_percentage
                
                # Set removal flags
                db_session.identity_is_removed = True
                db_session.identity_removal_time = timezone.now()
                db_session.identity_can_rejoin = True
                db_session.session_active = False
                
                # ✅ RESET CYCLE COUNTER (ready for next rejoin)
                db_session.identity_current_cycle_warnings = 0
                
                # Update session state
                session['identity_is_removed'] = True
                session['identity_can_rejoin'] = True
                session['session_active'] = False
                session['identity_removal_count'] = db_session.identity_removal_count
                session['identity_current_cycle_warnings'] = 0  # Reset for next cycle
                
                logger.critical(
                    f"\n{'='*80}\n"
                    f"🚫 USER REMOVED - Identity Verification Failed\n"
                    f"{'='*80}\n"
                    f"User: {user_id}\n"
                    f"Cycle Warnings: 3/3 (REMOVAL TRIGGERED)\n"
                    f"Total Warnings Issued: {total_warnings}\n"
                    f"Identity Removal Count: {db_session.identity_removal_count} ← INCREMENTED\n"
                    f"Behavior Removal Count: {db_session.behavior_removal_count} (unchanged)\n"
                    f"Removal Time: {db_session.identity_removal_time}\n"
                    f"Can Rejoin: {db_session.identity_can_rejoin}\n"
                    f"Next Cycle Will Start: Warning 1/3 again\n"
                    f"Cycle Counter Reset: {db_session.identity_current_cycle_warnings}/3\n"
                    f"💰 PENALTY APPLIED:\n"
                    f"  - Identity Removal Penalty: {identity_removal_penalty}%\n"
                    f"  - Previous Total Penalty: {current_penalty:.4f}%\n"
                    f"  - New Total Penalty: {session['attendance_penalty']:.4f}%\n"
                    f"  - New Attendance Percentage: {new_attendance_percentage:.2f}%\n"
                    f"  - Engagement Score: {new_attendance_percentage:.2f}%\n"
                    f"{'='*80}\n"
                )
            
            # ============================================================
            # Save all changes to database
            # ============================================================
            db_session.save()
            
            logger.info(
                f"💾 Identity state saved to database for {user_id}\n"
                f"   Cycle Warnings: {current_cycle_warning}/3\n"
                f"   Total Warnings: {total_warnings}\n"
                f"   Identity Removals: {db_session.identity_removal_count}\n"
                f"   Behavior Removals: {db_session.behavior_removal_count}"
            )
            
            # ============================================================
            # Return warning/removal result
            # ============================================================
            return {
                'identity_verified': False,
                'identity_similarity': similarity,
                'identity_warning_count': current_cycle_warning,  # Frontend: cycle count (1, 2, or 3)
                'identity_consecutive_unknown': 0,  # Reset after warning
                'identity_total_unknown': total,
                'identity_popup': popup_message,
                'identity_action': action,
                'identity_is_removed': db_session.identity_is_removed,
                'identity_can_rejoin': db_session.identity_can_rejoin,
                
                # ✅ NEW: Removal tracking data
                'identity_removal_count': db_session.identity_removal_count,
                'identity_total_warnings': total_warnings,  # Backend: cumulative count
                'identity_current_cycle_warnings': current_cycle_warning,  # Frontend display
                'behavior_removal_count': db_session.behavior_removal_count,
            }
        
        else:
            # ============================================================
            # THRESHOLD NOT REACHED: Still accumulating
            # ============================================================
            remaining = AttendanceConfig.IDENTITY_UNKNOWN_THRESHOLD - consecutive
            
            logger.debug(
                f"Identity accumulating for {user_id}: "
                f"{consecutive}/{AttendanceConfig.IDENTITY_UNKNOWN_THRESHOLD}s "
                f"({remaining}s until warning) | "
                f"Cycle: {db_session.identity_current_cycle_warnings}/3 | "
                f"Total: {db_session.identity_total_warnings_issued}"
            )
            
            # Save consecutive counter update
            db_session.save()
            
            # Return accumulation status (no popup yet)
            return {
                'identity_verified': False,
                'identity_similarity': similarity,
                'identity_warning_count': db_session.identity_current_cycle_warnings,
                'identity_consecutive_unknown': consecutive,
                'identity_total_unknown': total,
                'identity_popup': None,  # No popup until 5 seconds
                
                # ✅ NEW: Removal tracking data
                'identity_removal_count': db_session.identity_removal_count,
                'identity_total_warnings': db_session.identity_total_warnings_issued,
                'identity_current_cycle_warnings': db_session.identity_current_cycle_warnings,
                'behavior_removal_count': db_session.behavior_removal_count,
            }
# ==================== INTEGRATION HOOKS ====================

def start_attendance_tracking(meeting_id: str, user_id, user_name: str = None) -> bool:
    """
    ✅ ENHANCED: Initialize attendance tracking with identity verification support
    AND continuous violation tracking AND removal count tracking
    AND behavior message storage AND 120-second removal counter
    
    Start or resume attendance tracking for a user in a meeting.
    Handles both first-time joins and rejoins after disconnection/removal.
    
    Args:
        meeting_id (str): Unique meeting identifier
        user_id (str): User identifier
        user_name (str, optional): Display name for user
    
    Returns:
        bool: True if tracking started successfully, False otherwise
    
    Behavior:
        - First Join: Creates fresh session with all defaults
        - Rejoin: Loads previous state, preserves penalties and warnings
        - Identity State: Preserved warnings, reset consecutive counter on rejoin
        - Continuous Violations: Reset timer on rejoin to allow fresh start
        - Behavior Messages: Load warning/detection messages from violations column
        - Preserves removal counts across rejoins (including 120-second removals)
    """
    user_id = str(user_id)
    session_key = get_session_key(meeting_id, user_id)
    
    concurrent_sessions = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
    
    if session_key in attendance_sessions:
        logger.warning(f"MULTI-USER: Session already exists in memory for {meeting_id}_{user_id}")
        return True
    
    # ============================================================================
    # CHECK DATABASE FOR EXISTING SESSION (REJOIN SCENARIO)
    # ============================================================================
    try:
        existing_db = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
        
        logger.info(f"🔄 REJOIN DETECTED: Loading previous data for {user_id}")
        
        # ============================================================
        # LOAD EXTENDED TRACKING DATA
        # ============================================================
        extended_data = get_extended_tracking_data(existing_db)
        
        was_previously_removed = extended_data.get('is_removed_from_meeting', False)
        previous_removal_reason = extended_data.get('removal_reason', '')
        
        # ============================================================
        # LOAD BREAK SESSIONS
        # ============================================================
        try:
            break_sessions_list = json.loads(existing_db.break_sessions) if existing_db.break_sessions else []
        except json.JSONDecodeError:
            break_sessions_list = []
        
        # ============================================================
        # LOAD VIOLATIONS LIST (LEGACY - Keep for backward compatibility)
        # ============================================================
        try:
            violations_list = json.loads(existing_db.violations) if existing_db.violations else []
        except json.JSONDecodeError:
            violations_list = []
        
        # ============================================================
        # ✅✅✅ NEW: LOAD BEHAVIOR MESSAGES FROM VIOLATIONS COLUMN
        # ============================================================
        behavior_messages = existing_db.get_behavior_messages()
        
        logger.info(
            f"📊 BEHAVIOR MESSAGES LOADED for {user_id}:\n"
            f"  - Warning Messages: {len(behavior_messages.get('warnings', []))}\n"
            f"  - Detection Messages: {len(behavior_messages.get('detections', []))}\n"
            f"  - 120s Removal Events: {len(behavior_messages.get('continuous_removals', []))}"
        )
        
        # ============================================================
        # LOAD AND PRESERVE VIOLATION START TIMES
        # ============================================================
        try:
            violation_start_dict = json.loads(existing_db.violation_start_times) if existing_db.violation_start_times else {}
            logger.info(f"📊 PRESERVED violation_start_times for {user_id}: {list(violation_start_dict.keys())}")
        except json.JSONDecodeError:
            violation_start_dict = {}
            logger.warning(f"⚠️ Could not parse violation_start_times for {user_id}, initializing empty")
        
        # ============================================================
        # CALCULATE BREAK STATUS
        # ============================================================
        total_break_used = existing_db.total_break_time_used or 0
        max_break_allowed = existing_db.max_break_time_allowed or AttendanceConfig.MAX_TOTAL_BREAK_TIME
        
        should_break_be_used = total_break_used >= max_break_allowed
        
        logger.info(
            f"📊 BREAK STATUS for {user_id}:\n"
            f"  - Break Time Used: {total_break_used}s\n"
            f"  - Max Allowed: {max_break_allowed}s\n"
            f"  - Remaining: {max_break_allowed - total_break_used}s\n"
            f"  - Database break_used: {existing_db.break_used}\n"
            f"  - Calculated break_used: {should_break_be_used}\n"
            f"  - Will use: {should_break_be_used}"
        )
        
        # ============================================================
        # SET REJOIN TIMING
        # ============================================================
        current_time = time.time()
        rejoin_last_detection_time = current_time - AttendanceConfig.DETECTION_INTERVAL
        
        # ============================================================
        # LOAD IDENTITY VERIFICATION STATE (WITH NEW COLUMNS)
        # ============================================================
        identity_current_cycle_warnings = existing_db.identity_current_cycle_warnings
        identity_total_warnings = existing_db.identity_total_warnings_issued
        identity_total_unknown = existing_db.identity_total_unknown_seconds
        identity_warnings_list = existing_db.get_identity_warnings()
        identity_was_removed = existing_db.identity_is_removed
        identity_removal_count = existing_db.identity_removal_count
        behavior_removal_count = existing_db.behavior_removal_count
        continuous_violation_removal_count = existing_db.continuous_violation_removal_count  # ✅ NEW
        
        # Reset identity removal state on rejoin (allow restart)
        identity_is_removed = False
        identity_consecutive_unknown = 0
        
        logger.info(
            f"📊 IDENTITY STATUS for {user_id}:\n"
            f"  - Current Cycle Warnings: {identity_current_cycle_warnings}/3\n"
            f"  - Total Warnings Issued: {identity_total_warnings}\n"
            f"  - Identity Removal Count: {identity_removal_count}\n"
            f"  - Behavior Removal Count: {behavior_removal_count}\n"
            f"  - 120-Second Removal Count: {continuous_violation_removal_count}\n"
            f"  - Total Unknown Time: {identity_total_unknown}s\n"
            f"  - Was Removed: {identity_was_removed}\n"
            f"  - Consecutive Reset: Yes (allowing fresh start)\n"
            f"  - Warnings Preserved: {len(identity_warnings_list)}"
        )
        
        # ============================================================
        # CREATE SESSION WITH PRESERVED DATA
        # ============================================================
        attendance_sessions[session_key] = {
            "meeting_id": meeting_id,
            "user_id": user_id,
            "user_name": user_name or f"User_{user_id}",
            
            # ==================== BEHAVIOR VIOLATIONS (PRESERVED) ====================
            "popup_count": existing_db.popup_count,
            "warning_count": extended_data.get('warning_count', 0),
            "detection_counts": extended_data.get('detection_counts', 0),
            "total_detections": existing_db.total_detections,
            
            "attendance_penalty": float(existing_db.attendance_penalty),
            "total_detection_penalty_applied": (extended_data.get('detection_counts', 0) // 3) * AttendanceConfig.DETECTION_PENALTY_3,
            
            "session_active": True,
            "break_used": should_break_be_used,
            "violations": violations_list,
            "last_popup_time": 0,
            "violation_start_times": violation_start_dict,
            
            "start_time": existing_db.session_start_time,
            "last_activity": timezone.now(),
            "last_face_movement_time": current_time,
            "inactivity_popup_shown": False,
            
            # ==================== REMOVAL STATE (RESET ON REJOIN) ====================
            "is_removed_from_meeting": False,  # ✅ RESET: Allow user to restart
            "removal_timestamp": None,  # ✅ RESET: Clear removal timestamp
            "removal_reason": "",  # ✅ RESET: Clear removal reason
            "continuous_violation_start_time": None,  # ✅ RESET: Clear continuous violation timer
            "continuous_violation_type": None,  # ✅ RESET: Clear tracked violation type
            "last_detection_time": rejoin_last_detection_time,
            "detection_penalty_applied": extended_data.get('detection_penalty_applied', False),
            "warning_phase_complete": extended_data.get('warning_phase_complete', False),
            
            # ==================== BREAK SYSTEM (PRESERVED) ====================
            "total_break_time_used": total_break_used,
            "current_break_start_time": None,
            "is_currently_on_break": False,
            "break_count": existing_db.break_count or 0,
            "break_sessions": break_sessions_list,
            "max_break_time_allowed": max_break_allowed,
            
            # ==================== CAMERA VERIFICATION (RESET) ====================
            "camera_resume_expected": False,
            "camera_resume_deadline": None,
            "camera_confirmation_token": None,
            "camera_verified_at": current_time,
            
            # ==================== GRACE PERIOD (RESET) ====================
            "grace_period_active": False,
            "grace_period_until": None,
            
            # ==================== BASELINE ESTABLISHMENT (RESET) ====================
            "baseline_ear": None,
            "baseline_yaw": None,
            "baseline_samples": 0,
            "baseline_established": False,
            "face_detected": False,
            
            # ==================== PERFORMANCE METRICS (PRESERVED) ====================
            "frame_processing_count": existing_db.frame_processing_count,
            "active_participation_time": existing_db.active_participation_time,
            "violation_severity_score": float(existing_db.violation_severity_score),
            "continuous_violation_time": 0,  # ✅ RESET: Clear continuous time
            "last_violation_type": "",
            "metrics_history": [],
            
            # ==================== SESSION METADATA ====================
            "session_started_at": current_time,
            "isolation_verified": True,
            "concurrent_participants_at_start": len(concurrent_sessions),
            
            # ==================== IDENTITY VERIFICATION STATE (PRESERVED/RESET) ====================
            "identity_warning_count": identity_current_cycle_warnings,  # PRESERVED (cycle count)
            "identity_consecutive_unknown_seconds": identity_consecutive_unknown,  # RESET to 0
            "identity_total_unknown_seconds": identity_total_unknown,  # PRESERVED
            "identity_is_removed": identity_is_removed,  # RESET to False
            "identity_warnings": identity_warnings_list,  # PRESERVED (history)
            "identity_last_check_time": current_time,  # RESET
            "identity_can_rejoin": True,  # Always True
            
            # ==================== REMOVAL TRACKING (PRESERVED) ====================
            "identity_removal_count": identity_removal_count,  # PRESERVED
            "identity_total_warnings": identity_total_warnings,  # PRESERVED
            "identity_current_cycle_warnings": identity_current_cycle_warnings,  # PRESERVED
            "behavior_removal_count": behavior_removal_count,  # PRESERVED
            "continuous_violation_removal_count": continuous_violation_removal_count,  # ✅ NEW PRESERVED
            
            # ==================== ✅✅✅ NEW: BEHAVIOR MESSAGES (PRESERVED) ====================
            "behavior_messages": behavior_messages,  # PRESERVED from violations column
        }
        
        # ============================================================
        # UPDATE DATABASE
        # ============================================================
        existing_db.session_active = True
        existing_db.is_currently_on_break = False
        existing_db.current_break_start_time = None
        existing_db.last_face_movement_time = current_time
        existing_db.inactivity_popup_shown = False
        
        # Fix break_used flag if needed
        if existing_db.break_used != should_break_be_used:
            logger.info(f"🔧 FIXING break_used flag for {user_id}: {existing_db.break_used} → {should_break_be_used}")
            existing_db.break_used = should_break_be_used
        
        # Preserve violation_start_times
        existing_db.violation_start_times = json.dumps(violation_start_dict)
        
        # Reset identity removal state in database
        existing_db.identity_is_removed = identity_is_removed  # False
        existing_db.identity_consecutive_unknown_seconds = identity_consecutive_unknown  # 0
        existing_db.identity_last_check_time = current_time
        
        # ✅ DON'T RESET: Removal counts are preserved across rejoins
        # existing_db.identity_removal_count - PRESERVED
        # existing_db.identity_total_warnings_issued - PRESERVED
        # existing_db.identity_current_cycle_warnings - PRESERVED
        # existing_db.behavior_removal_count - PRESERVED
        # existing_db.continuous_violation_removal_count - PRESERVED ✅ NEW
        
        # ✅ DON'T RESET: Behavior messages are preserved in violations column
        # existing_db.violations - PRESERVED (contains behavior messages)
        
        existing_db.save()
        
        # ============================================================
        # UPDATE EXTENDED TRACKING DATA
        # ============================================================
        extended_data['continuous_violation_start_time'] = None  # ✅ RESET
        extended_data['last_detection_time'] = rejoin_last_detection_time
        extended_data['is_removed_from_meeting'] = False  # ✅ RESET
        extended_data['removal_timestamp'] = None  # ✅ RESET
        extended_data['removal_reason'] = ''  # ✅ RESET
        extended_data['camera_resume_expected'] = False
        extended_data['camera_resume_deadline'] = None
        extended_data['camera_confirmation_token'] = None
        extended_data['camera_verified_at'] = current_time
        extended_data['grace_period_active'] = False
        extended_data['grace_period_until'] = None
        save_extended_tracking_data(existing_db, extended_data)
        
        # ============================================================
        # LOG REJOIN SUCCESS
        # ============================================================
        logger.info(
            f"✅ REJOIN SUCCESSFUL for {user_id}:\n"
            f"  ==================== BEHAVIOR TRACKING ====================\n"
            f"  - Warnings: {existing_db.popup_count}\n"
            f"  - Detections: {extended_data.get('detection_counts', 0)}\n"
            f"  - Penalty: {existing_db.attendance_penalty:.4f}%\n"
            f"  - Detection Penalty: {(extended_data.get('detection_counts', 0) // 3) * AttendanceConfig.DETECTION_PENALTY_3:.4f}%\n"
            f"  - Violation Start Times: {len(violation_start_dict)} preserved\n"
            f"  - Continuous Violation Timer: RESET (fresh start)\n"
            f"  - Removal Status: {'CLEARED (was removed)' if was_previously_removed else 'Not removed'}\n"
            f"  ==================== BEHAVIOR MESSAGES ====================\n"
            f"  - Warning Messages: {len(behavior_messages.get('warnings', []))}\n"
            f"  - Detection Messages: {len(behavior_messages.get('detections', []))}\n"
            f"  - 120s Removal Events: {len(behavior_messages.get('continuous_removals', []))}\n"
            f"  - Messages Status: PRESERVED from violations column\n"
            f"  ==================== IDENTITY VERIFICATION ====================\n"
            f"  - Current Cycle Warnings: {identity_current_cycle_warnings}/3\n"
            f"  - Total Warnings Issued: {identity_total_warnings}\n"
            f"  - Identity Removal Count: {identity_removal_count}\n"
            f"  - Identity Total Unknown: {identity_total_unknown}s\n"
            f"  - Identity Removal Status: CLEARED (can restart)\n"
            f"  ==================== REMOVAL TRACKING ====================\n"
            f"  - Identity Removals: {identity_removal_count}\n"
            f"  - Behavior Removals: {behavior_removal_count}\n"
            f"  - 120-Second Removals: {continuous_violation_removal_count}\n"
            f"  - Total Removals: {identity_removal_count + behavior_removal_count}\n"
            f"  ==================== BREAK SYSTEM ====================\n"
            f"  - Break Time Used: {total_break_used}s / {max_break_allowed}s\n"
            f"  - Break Available: {max_break_allowed - total_break_used}s\n"
            f"  - break_used Flag: {should_break_be_used}\n"
            f"  ==================== SESSION INFO ====================\n"
            f"  - Baseline Reset: Yes (will re-establish)\n"
            f"  - Detection Ready: Yes (last_detection_time set to past)\n"
            f"  - Meeting has {len(concurrent_sessions) + 1} participants"
        )
        
        return True
        
    except AttendanceSession.DoesNotExist:
        # ============================================================================
        # FIRST TIME JOIN - Create fresh session
        # ============================================================================
        logger.info(f"🆕 FIRST JOIN: Creating new session for {user_id}")
        
        current_time = time.time()
        
        # ============================================================
        # CREATE FRESH SESSION
        # ============================================================
        attendance_sessions[session_key] = {
            "meeting_id": meeting_id,
            "user_id": user_id,
            "user_name": user_name or f"User_{user_id}",
            
            # ==================== BEHAVIOR VIOLATIONS (FRESH START) ====================
            "popup_count": 0,
            "warning_count": 0,
            "detection_counts": 0,
            "total_detections": 0,
            "attendance_penalty": 0.0,
            "session_active": True,
            "break_used": False,
            "violations": [],
            "last_popup_time": 0,
            "violation_start_times": {},
            "start_time": timezone.now(),
            "last_activity": timezone.now(),
            "last_face_movement_time": current_time,
            "inactivity_popup_shown": False,
            
            # ==================== REMOVAL STATE (FRESH START) ====================
            "is_removed_from_meeting": False,
            "removal_timestamp": None,
            "removal_reason": "",
            "continuous_violation_start_time": None,  # ✅ Initialize continuous violation timer
            "continuous_violation_type": None,  # ✅ Initialize violation type tracker
            "last_detection_time": 0.0,
            "detection_penalty_applied": False,
            "warning_phase_complete": False,
            
            "total_detection_penalty_applied": 0.0,
            
            # ==================== BREAK SYSTEM (FRESH START) ====================
            "total_break_time_used": 0,
            "current_break_start_time": None,
            "is_currently_on_break": False,
            "break_count": 0,
            "break_sessions": [],
            "max_break_time_allowed": AttendanceConfig.MAX_TOTAL_BREAK_TIME,
            
            # ==================== CAMERA VERIFICATION (FRESH START) ====================
            "camera_resume_expected": False,
            "camera_resume_deadline": None,
            "camera_confirmation_token": None,
            "camera_verified_at": current_time,
            
            # ==================== GRACE PERIOD (FRESH START) ====================
            "grace_period_active": False,
            "grace_period_until": None,
            
            # ==================== BASELINE ESTABLISHMENT (FRESH START) ====================
            "baseline_ear": None,
            "baseline_yaw": None,
            "baseline_samples": 0,
            "baseline_established": False,
            "face_detected": False,
            
            # ==================== PERFORMANCE METRICS (FRESH START) ====================
            "frame_processing_count": 0,
            "active_participation_time": 0,
            "violation_severity_score": 0.0,
            "continuous_violation_time": 0,
            "last_violation_type": "",
            "metrics_history": [],
            
            # ==================== SESSION METADATA ====================
            "session_started_at": current_time,
            "isolation_verified": True,
            "concurrent_participants_at_start": len(concurrent_sessions),
            
            # ==================== IDENTITY VERIFICATION STATE (FRESH START) ====================
            "identity_warning_count": 0,
            "identity_consecutive_unknown_seconds": 0,
            "identity_total_unknown_seconds": 0,
            "identity_is_removed": False,
            "identity_warnings": [],
            "identity_last_check_time": current_time,
            "identity_can_rejoin": True,
            
            # ==================== REMOVAL TRACKING (FRESH START) ====================
            "identity_removal_count": 0,
            "identity_total_warnings": 0,
            "identity_current_cycle_warnings": 0,
            "behavior_removal_count": 0,
            "continuous_violation_removal_count": 0,  # ✅ NEW
            
            # ==================== ✅✅✅ NEW: BEHAVIOR MESSAGES (FRESH START) ====================
            "behavior_messages": {'warnings': [], 'detections': [], 'continuous_removals': []},  # ✅ NEW: Added continuous_removals
        }

        try:
            # ============================================================
            # CREATE EXTENDED TRACKING DATA
            # ============================================================
            extended_tracking = {
                'detection_counts': 0,
                'warning_count': 0,
                'is_removed_from_meeting': False,
                'removal_timestamp': None,
                'removal_reason': '',
                'continuous_violation_start_time': None,  # ✅ Initialize in extended data
                'last_detection_time': 0.0,
                'detection_penalty_applied': False,
                'warning_phase_complete': False,
                'camera_resume_expected': False,
                'camera_resume_deadline': None,
                'camera_confirmation_token': None,
                'camera_verified_at': current_time,
                'grace_period_active': False,
                'grace_period_until': None,
                'total_detection_penalty': 0.0,
                'detection_batches_completed': 0,
            }
            
            # ============================================================
            # CREATE DATABASE RECORD
            # ============================================================
            AttendanceSession.objects.update_or_create(
                meeting_id=meeting_id,
                user_id=user_id,
                defaults={
                    # Behavior violation fields
                    'popup_count': 0,
                    'detection_counts': json.dumps(extended_tracking),
                    'violation_start_times': '{}',
                    'total_detections': 0,
                    'attendance_penalty': 0.0,
                    'session_active': True,
                    'break_used': False,
                    'violations': json.dumps({'warnings': [], 'detections': [], 'continuous_removals': []}),  # ✅ NEW: Initialize with continuous_removals
                    'session_start_time': timezone.now(),
                    'last_activity': timezone.now(),
                    'last_face_movement_time': current_time,
                    'inactivity_popup_shown': False,
                    'total_session_time': 0,
                    'active_participation_time': 0,
                    'violation_severity_score': 0.0,
                    'frame_processing_count': 0,
                    'engagement_score': 100.00,
                    'attendance_percentage': 100.00,
                    'focus_score': 100.00,
                    
                    # Break system fields
                    'total_break_time_used': 0,
                    'current_break_start_time': None,
                    'break_sessions': '[]',
                    'max_break_time_allowed': AttendanceConfig.MAX_TOTAL_BREAK_TIME,
                    'is_currently_on_break': False,
                    'break_count': 0,
                    'last_break_calculation': 0.0,
                    
                    # Identity verification fields
                    'identity_warning_count': 0,
                    'identity_consecutive_unknown_seconds': 0,
                    'identity_total_unknown_seconds': 0,
                    'identity_is_removed': False,
                    'identity_removal_time': None,
                    'identity_can_rejoin': True,
                    'identity_warnings': '[]',
                    'identity_last_check_time': current_time,
                    
                    # Removal tracking fields
                    'identity_removal_count': 0,
                    'identity_total_warnings_issued': 0,
                    'identity_current_cycle_warnings': 0,
                    'behavior_removal_count': 0,
                    'continuous_violation_removal_count': 0,  # ✅ NEW
                }
            )
            
            final_concurrent_count = len([k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")])
            
            logger.info(
                f"✅ FIRST JOIN SUCCESSFUL for {user_id}:\n"
                f"  ==================== SESSION CREATED ====================\n"
                f"  - All counters initialized to 0\n"
                f"  - Continuous violation timer: NULL (not started)\n"
                f"  - Attendance: 100%\n"
                f"  - Break time available: {AttendanceConfig.MAX_TOTAL_BREAK_TIME}s\n"
                f"  - Identity verification: Active\n"
                f"  ==================== BEHAVIOR MESSAGES ====================\n"
                f"  - Warning Messages: 0 (initialized)\n"
                f"  - Detection Messages: 0 (initialized)\n"
                f"  - 120s Removal Events: 0 (initialized)\n"
                f"  - Messages Status: Empty structure created\n"
                f"  ==================== REMOVAL TRACKING ====================\n"
                f"  - Identity Removal Count: 0\n"
                f"  - Identity Total Warnings: 0\n"
                f"  - Identity Current Cycle: 0/3\n"
                f"  - Behavior Removal Count: 0\n"
                f"  - 120-Second Removal Count: 0\n"
                f"  ==================== MEETING INFO ====================\n"
                f"  - Meeting participants: {final_concurrent_count}\n"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"MULTI-USER: Failed to start tracking for {meeting_id}_{user_id}: {e}")
            logger.error(traceback.format_exc())
            if session_key in attendance_sessions:
                del attendance_sessions[session_key]
            return False




def stop_attendance_tracking(meeting_id: str, user_id) -> bool:
    """Stop tracking for user"""
    user_id = str(user_id)
    session_key = get_session_key(meeting_id, user_id)
    
    other_participants = [k for k in attendance_sessions.keys() 
                         if k.startswith(f"{meeting_id}_") and k != session_key]
    
    logger.info(f"MULTI-USER: Stopping tracking for {user_id}. {len(other_participants)} other participants unaffected")
    
    if session_key in attendance_sessions:
        session = attendance_sessions[session_key]
        current_time = time.time()
        
        try:
            attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
            
            if session.get('is_currently_on_break'):
                update_break_time_used(session, attendance_obj, current_time)
                session['is_currently_on_break'] = False
                session['current_break_start_time'] = None
                attendance_obj.is_currently_on_break = False
                attendance_obj.current_break_start_time = None
                attendance_obj.save()
                
        except AttendanceSession.DoesNotExist:
            pass
        
        store_attendance_to_db(meeting_id, user_id)
        del attendance_sessions[session_key]
        
        remaining_participants = [k for k in attendance_sessions.keys() 
                                if k.startswith(f"{meeting_id}_")]
        logger.info(f"MULTI-USER: User {user_id} stopped. {len(remaining_participants)} participants continue")
        
        return True
    
    logger.warning(f"MULTI-USER: No session found for {meeting_id}_{user_id}")
    return False



def store_attendance_to_db(meeting_id: str, user_id: str) -> bool:
    """
    ✅ ENHANCED: Store attendance session data to database with identity verification
    AND removal tracking AND behavior message storage
    
    Persists all session data from memory to database, including behavior violations,
    identity verification state, break time, performance metrics, removal counts,
    and behavior warning/detection messages.
    
    Args:
        meeting_id (str): Meeting identifier
        user_id (str): User identifier
    
    Returns:
        bool: True if data saved successfully, False otherwise
    
    Database Operations:
        - Updates or creates AttendanceSession record
        - Saves behavior violation tracking
        - Saves identity verification state
        - Saves break time usage
        - Saves performance metrics
        - Saves extended tracking data (JSON)
        - Saves removal tracking (identity_removal_count, behavior_removal_count, etc.)
        - ✅ NEW: Saves behavior warning/detection messages to violations column
    
    Error Handling:
        - Uses database transaction for atomicity
        - Logs errors and exceptions
        - Returns False on failure
        - Continues execution even if save fails
    """
    session_key = get_session_key(meeting_id, user_id)
    
    if session_key not in attendance_sessions:
        logger.warning(f"⚠️ Cannot store attendance: Session not found for {meeting_id}_{user_id}")
        return False
    
    state = attendance_sessions[session_key]
    
    try:
        with transaction.atomic():
            logger.info(f"💾 STORING ATTENDANCE DATA for {user_id}")
            
            # ==================== PREPARE EXTENDED TRACKING DATA ====================
            extended_tracking = {
                'detection_counts': state.get("detection_counts", 0),
                'warning_count': state.get("warning_count", 0),
                'is_removed_from_meeting': state.get("is_removed_from_meeting", False),
                'removal_timestamp': state.get("removal_timestamp").isoformat() if state.get("removal_timestamp") else None,
                'removal_reason': state.get("removal_reason", ""),
                'continuous_violation_start_time': state.get("continuous_violation_start_time"),
                'last_detection_time': state.get("last_detection_time", 0.0),
                'detection_penalty_applied': state.get("detection_penalty_applied", False),
                'warning_phase_complete': state.get("warning_phase_complete", False),
                'camera_resume_expected': state.get("camera_resume_expected", False),
                'camera_resume_deadline': state.get("camera_resume_deadline"),
                'camera_confirmation_token': state.get("camera_confirmation_token"),
                'camera_verified_at': state.get("camera_verified_at"),
                'grace_period_active': state.get("grace_period_active", False),
                'grace_period_until': state.get("grace_period_until"),
                'total_detection_penalty': state.get("total_detection_penalty_applied", 0.0),
                'detection_batches_completed': state.get("detection_counts", 0) // 3,
            }
            
            # ==================== PREPARE VIOLATION DATA (LEGACY - Keep for backward compatibility) ====================
            violations_list = state.get("violations", [])
            if isinstance(violations_list, list):
                violations_json = json.dumps(violations_list)
            else:
                violations_json = '[]'
            
            # ==================== ✅✅✅ NEW: PREPARE BEHAVIOR MESSAGES DATA ====================
            behavior_messages = state.get("behavior_messages", {'warnings': [], 'detections': []})
            
            # Ensure proper structure
            if not isinstance(behavior_messages, dict):
                behavior_messages = {'warnings': [], 'detections': []}
            
            if 'warnings' not in behavior_messages:
                behavior_messages['warnings'] = []
            
            if 'detections' not in behavior_messages:
                behavior_messages['detections'] = []
            
            # Convert to JSON for storage in violations column
            behavior_messages_json = json.dumps(behavior_messages)
            
            logger.debug(
                f"📊 BEHAVIOR MESSAGES prepared for {user_id}:\n"
                f"  - Warnings: {len(behavior_messages.get('warnings', []))}\n"
                f"  - Detections: {len(behavior_messages.get('detections', []))}"
            )
            
            # ==================== PREPARE VIOLATION START TIMES ====================
            violation_start_times_dict = state.get("violation_start_times", {})
            if isinstance(violation_start_times_dict, dict):
                violation_start_times_json = json.dumps(violation_start_times_dict)
            else:
                violation_start_times_json = '{}'
            
            # ==================== PREPARE BREAK SESSIONS ====================
            break_sessions_list = state.get("break_sessions", [])
            if isinstance(break_sessions_list, list):
                break_sessions_json = json.dumps(break_sessions_list)
            else:
                break_sessions_json = '[]'
            
            # ==================== CALCULATE ATTENDANCE PERCENTAGE ====================
            attendance_penalty = state.get("attendance_penalty", 0.0)
            attendance_percentage = max(0, 100 - attendance_penalty)
            
            # ==================== PREPARE IDENTITY WARNINGS ====================
            identity_warnings_list = state.get("identity_warnings", [])
            if isinstance(identity_warnings_list, list):
                identity_warnings_json = json.dumps(identity_warnings_list)
            else:
                identity_warnings_json = '[]'
            
            # ==================== UPDATE OR CREATE DATABASE RECORD ====================
            attendance_record, created = AttendanceSession.objects.update_or_create(
                meeting_id=meeting_id,
                user_id=user_id,
                defaults={
                    # ==================== BEHAVIOR VIOLATION FIELDS ====================
                    'popup_count': state.get("popup_count", 0),
                    'detection_counts': json.dumps(extended_tracking),
                    'violation_start_times': violation_start_times_json,
                    'total_detections': state.get("total_detections", 0),
                    'attendance_penalty': attendance_penalty,
                    'session_active': state.get("session_active", True),
                    'break_used': state.get("break_used", False),
                    'violations': behavior_messages_json,  # ✅✅✅ MODIFIED: Store behavior messages instead of legacy violations
                    'last_activity': timezone.now(),
                    'last_face_movement_time': state.get("last_face_movement_time", time.time()),
                    'inactivity_popup_shown': state.get("inactivity_popup_shown", False),
                    'last_popup_time': state.get("last_popup_time", 0.0),
                    
                    # ==================== PERFORMANCE METRICS ====================
                    'total_session_time': int(time.time() - state.get("session_started_at", time.time())),
                    'active_participation_time': state.get("active_participation_time", 0),
                    'violation_severity_score': state.get("violation_severity_score", 0.0),
                    'frame_processing_count': state.get("frame_processing_count", 0),
                    'last_violation_type': state.get("last_violation_type", ""),
                    'continuous_violation_time': state.get("continuous_violation_time", 0),
                    
                    # ==================== ATTENDANCE SCORES ====================
                    'engagement_score': attendance_percentage,
                    'attendance_percentage': attendance_percentage,
                    'focus_score': attendance_percentage,
                    
                    # ==================== BREAK SYSTEM FIELDS ====================
                    'total_break_time_used': state.get("total_break_time_used", 0),
                    'current_break_start_time': state.get("current_break_start_time"),
                    'break_sessions': break_sessions_json,
                    'max_break_time_allowed': state.get("max_break_time_allowed", AttendanceConfig.MAX_TOTAL_BREAK_TIME),
                    'is_currently_on_break': state.get("is_currently_on_break", False),
                    'break_count': state.get("break_count", 0),
                    'last_break_calculation': state.get("last_break_calculation", 0.0),
                    
                    # ==================== IDENTITY VERIFICATION FIELDS ====================
                    'identity_warning_count': state.get("identity_warning_count", 0),
                    'identity_consecutive_unknown_seconds': state.get("identity_consecutive_unknown_seconds", 0),
                    'identity_total_unknown_seconds': state.get("identity_total_unknown_seconds", 0),
                    'identity_is_removed': state.get("identity_is_removed", False),
                    'identity_removal_time': timezone.now() if state.get("identity_is_removed", False) and not created else None,
                    'identity_can_rejoin': state.get("identity_can_rejoin", True),
                    'identity_warnings': identity_warnings_json,
                    'identity_last_check_time': state.get("identity_last_check_time", 0.0),
                    
                    # ==================== REMOVAL TRACKING FIELDS ====================
                    'identity_removal_count': state.get("identity_removal_count", 0),
                    'identity_total_warnings_issued': state.get("identity_total_warnings", 0),
                    'identity_current_cycle_warnings': state.get("identity_current_cycle_warnings", 0),
                    'behavior_removal_count': state.get("behavior_removal_count", 0),
                }
            )
            
            action = "CREATED" if created else "UPDATED"
            
            # ==================== LOG SUCCESS WITH COMPREHENSIVE DETAILS ====================
            logger.info(
                f"✅ ATTENDANCE DATA {action} for {user_id}:\n"
                f"  ==================== BEHAVIOR TRACKING ====================\n"
                f"  - Session Active: {state.get('session_active', True)}\n"
                f"  - Popup Count: {state.get('popup_count', 0)}\n"
                f"  - Warning Count: {state.get('warning_count', 0)}/{AttendanceConfig.MAX_WARNING_MESSAGES}\n"
                f"  - Detection Count: {state.get('detection_counts', 0)}\n"
                f"  - Total Detections: {state.get('total_detections', 0)}\n"
                f"  - Attendance Penalty: {attendance_penalty:.4f}%\n"
                f"  - Attendance Percentage: {attendance_percentage:.2f}%\n"
                f"  - Violations Tracked: {len(violation_start_times_dict)}\n"
                f"  - Is Removed (Behavior): {state.get('is_removed_from_meeting', False)}\n"
                f"  - Warning Phase Complete: {state.get('warning_phase_complete', False)}\n"
                f"  - Detection Penalty Applied: {state.get('detection_penalty_applied', False)}\n"
                f"  ==================== ✅✅✅ BEHAVIOR MESSAGES ====================\n"
                f"  - Warning Messages: {len(behavior_messages.get('warnings', []))}\n"
                f"  - Detection Messages: {len(behavior_messages.get('detections', []))}\n"
                f"  - Messages Stored in: violations column\n"
                f"  - Message Structure: {{'warnings': [...], 'detections': [...]}}\n"
                f"  ==================== IDENTITY VERIFICATION ====================\n"
                f"  - Identity Warning Count: {state.get('identity_warning_count', 0)}/{AttendanceConfig.IDENTITY_MAX_WARNINGS}\n"
                f"  - Identity Consecutive Unknown: {state.get('identity_consecutive_unknown_seconds', 0)}s\n"
                f"  - Identity Total Unknown: {state.get('identity_total_unknown_seconds', 0)}s\n"
                f"  - Is Removed (Identity): {state.get('identity_is_removed', False)}\n"
                f"  - Identity Warnings History: {len(identity_warnings_list)} events\n"
                f"  - Can Rejoin: {state.get('identity_can_rejoin', True)}\n"
                f"  ==================== REMOVAL TRACKING ====================\n"
                f"  - Identity Removal Count: {state.get('identity_removal_count', 0)}\n"
                f"  - Identity Total Warnings Issued: {state.get('identity_total_warnings', 0)}\n"
                f"  - Identity Current Cycle Warnings: {state.get('identity_current_cycle_warnings', 0)}/3\n"
                f"  - Behavior Removal Count: {state.get('behavior_removal_count', 0)}\n"
                f"  - Total Removals: {state.get('identity_removal_count', 0) + state.get('behavior_removal_count', 0)}\n"
                f"  ==================== BREAK SYSTEM ====================\n"
                f"  - Break Used: {state.get('break_used', False)}\n"
                f"  - Total Break Time: {state.get('total_break_time_used', 0)}s / {state.get('max_break_time_allowed', 300)}s\n"
                f"  - Break Count: {state.get('break_count', 0)}\n"
                f"  - Currently On Break: {state.get('is_currently_on_break', False)}\n"
                f"  - Break Sessions: {len(break_sessions_list)}\n"
                f"  ==================== PERFORMANCE ====================\n"
                f"  - Frame Processing Count: {state.get('frame_processing_count', 0)}\n"
                f"  - Engagement Score: {attendance_percentage:.2f}%\n"
                f"  - Focus Score: {attendance_percentage:.2f}%\n"
                f"  - Violation Severity Score: {state.get('violation_severity_score', 0.0):.2f}\n"
                f"  ==================== SESSION INFO ====================\n"
                f"  - Session Start: {state.get('start_time', 'Unknown')}\n"
                f"  - Last Activity: {timezone.now()}\n"
                f"  - Total Session Time: {int(time.time() - state.get('session_started_at', time.time()))}s\n"
            )
            
            return True
            
    except Exception as e:
        logger.error(f"❌ FAILED TO STORE ATTENDANCE for {user_id}: {e}")
        logger.error(traceback.format_exc())
        return False


def store_all_active_sessions_to_db(meeting_id: str = None) -> dict:
    """
    ✅ BATCH STORAGE - Store all active sessions to database at once
    
    This function saves ALL active attendance sessions to the database in one operation.
    Useful for meeting end, server shutdown, or periodic backups.
    
    Args:
        meeting_id (str, optional): If provided, only stores sessions for this specific meeting.
                                   If None, stores ALL active sessions across all meetings.
    
    Returns:
        dict: Summary of storage results
            {
                'total': int,           # Total number of sessions attempted
                'success': int,         # Number of successfully saved sessions
                'failed': int,          # Number of failed saves
                'failed_users': list,   # List of user_ids that failed to save
                'meeting_ids': list,    # List of meeting IDs processed
                'duration': float       # Time taken in seconds
            }
    
    Example Usage:
        # Save all sessions for specific meeting
        >>> results = store_all_active_sessions_to_db("meeting_123")
        >>> print(f"Saved {results['success']} out of {results['total']} sessions")
        
        # Save all sessions across all meetings
        >>> results = store_all_active_sessions_to_db()
        >>> print(f"Total meetings processed: {len(results['meeting_ids'])}")
    
    Use Cases:
        1. Meeting End: Save all participants when meeting finishes
        2. Server Shutdown: Save all data before server restart
        3. Manual Backup: Admin triggers backup of all sessions
        4. Scheduled Backup: Cron job runs every hour
    
    Notes:
        - Uses store_attendance_to_db() internally for each session
        - Continues processing even if individual saves fail
        - Logs detailed information about failures
        - Thread-safe operation
    """
    import time as time_module
    
    start_time = time_module.time()
    
    results = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'failed_users': [],
        'meeting_ids': [],
        'duration': 0
    }
    
    try:
        # ============================================================
        # STEP 1: Filter sessions based on meeting_id
        # ============================================================
        if meeting_id:
            # Store only sessions for specific meeting
            sessions_to_store = [
                (session_key, session_data) 
                for session_key, session_data in attendance_sessions.items() 
                if session_key.startswith(f"{meeting_id}_")
            ]
            logger.info(f"💾 BATCH STORAGE: Filtering sessions for meeting {meeting_id}")
        else:
            # Store all active sessions
            sessions_to_store = list(attendance_sessions.items())
            logger.info(f"💾 BATCH STORAGE: Processing ALL active sessions")
        
        results['total'] = len(sessions_to_store)
        
        if results['total'] == 0:
            logger.info("✅ BATCH STORAGE: No active sessions to store")
            results['duration'] = time_module.time() - start_time
            return results
        
        logger.info(
            f"💾 BATCH STORAGE STARTED:\n"
            f"  - Total Sessions: {results['total']}\n"
            f"  - Meeting Filter: {meeting_id if meeting_id else 'All meetings'}\n"
            f"  - Timestamp: {timezone.now()}"
        )
        
        # Track unique meeting IDs
        processed_meetings = set()
        
        # ============================================================
        # STEP 2: Process each session
        # ============================================================
        for idx, (session_key, session_data) in enumerate(sessions_to_store, 1):
            try:
                # Extract meeting_id and user_id from session data
                session_meeting_id = session_data.get('meeting_id')
                session_user_id = session_data.get('user_id')
                
                # Validate session data
                if not session_meeting_id or not session_user_id:
                    logger.warning(
                        f"⚠️ BATCH STORAGE [{idx}/{results['total']}]: "
                        f"Invalid session data for {session_key} - Missing meeting_id or user_id"
                    )
                    results['failed'] += 1
                    continue
                
                # Track meeting ID
                processed_meetings.add(session_meeting_id)
                
                # Store the session
                logger.debug(f"💾 BATCH STORAGE [{idx}/{results['total']}]: Storing {session_user_id}...")
                
                success = store_attendance_to_db(session_meeting_id, session_user_id)
                
                if success:
                    results['success'] += 1
                    logger.debug(f"✅ BATCH STORAGE [{idx}/{results['total']}]: {session_user_id} saved")
                else:
                    results['failed'] += 1
                    results['failed_users'].append(session_user_id)
                    logger.warning(f"⚠️ BATCH STORAGE [{idx}/{results['total']}]: {session_user_id} failed")
                
            except Exception as e:
                results['failed'] += 1
                
                # Try to get user_id for error reporting
                try:
                    failed_user_id = session_data.get('user_id', 'unknown')
                    results['failed_users'].append(failed_user_id)
                except:
                    failed_user_id = 'unknown'
                
                logger.error(
                    f"❌ BATCH STORAGE [{idx}/{results['total']}]: "
                    f"Exception while storing {session_key}: {e}"
                )
                logger.error(traceback.format_exc())
        
        # ============================================================
        # STEP 3: Calculate duration and finalize results
        # ============================================================
        results['duration'] = time_module.time() - start_time
        results['meeting_ids'] = list(processed_meetings)
        
        # ============================================================
        # STEP 4: Log final summary
        # ============================================================
        logger.info(
            f"\n{'='*80}\n"
            f"✅ BATCH STORAGE COMPLETE\n"
            f"{'='*80}\n"
            f"  Total Sessions: {results['total']}\n"
            f"  Successfully Saved: {results['success']}\n"
            f"  Failed: {results['failed']}\n"
            f"  Success Rate: {(results['success'] / results['total'] * 100) if results['total'] > 0 else 0:.1f}%\n"
            f"  Meetings Processed: {len(results['meeting_ids'])}\n"
            f"  Duration: {results['duration']:.2f}s\n"
            f"  Failed Users: {results['failed_users'] if results['failed_users'] else 'None'}\n"
            f"{'='*80}\n"
        )
        
        return results
        
    except Exception as e:
        logger.error(f"❌ BATCH STORAGE FAILED: Critical error - {e}")
        logger.error(traceback.format_exc())
        
        results['duration'] = time_module.time() - start_time
        return results



def cleanup_old_sessions(hours: int = 24, dry_run: bool = False) -> dict:
    """
    ✅ DATABASE CLEANUP - Remove old inactive sessions from database
    
    This function deletes attendance sessions that have been inactive for a specified
    number of hours. Helps maintain database performance and storage efficiency.
    
    Args:
        hours (int): Delete sessions inactive for this many hours (default: 24)
        dry_run (bool): If True, only count sessions without deleting (default: False)
    
    Returns:
        dict: Cleanup results
            {
                'deleted_count': int,        # Number of sessions deleted
                'meeting_count': int,        # Number of meetings affected
                'oldest_session': datetime,  # Timestamp of oldest deleted session
                'space_freed_mb': float,     # Estimated space freed (MB)
                'duration': float,           # Time taken in seconds
                'dry_run': bool             # Whether this was a dry run
            }
    
    Example Usage:
        # Delete sessions older than 24 hours
        >>> result = cleanup_old_sessions(hours=24)
        >>> print(f"Deleted {result['deleted_count']} sessions")
        
        # Delete sessions older than 7 days
        >>> result = cleanup_old_sessions(hours=168)
        
        # Dry run to see what would be deleted
        >>> result = cleanup_old_sessions(hours=24, dry_run=True)
        >>> print(f"Would delete {result['deleted_count']} sessions")
    
    Use Cases:
        1. Daily Cleanup: Run as cron job every night
        2. Weekly Maintenance: Deep clean old sessions
        3. Pre-deployment: Clean before major updates
        4. Manual Cleanup: Admin triggers cleanup
    
    Cron Job Examples:
        # Daily at 2 AM
        0 2 * * * cd /path/to/project && python manage.py cleanup_attendance --hours=24
        
        # Weekly on Sunday at 3 AM
        0 3 * * 0 cd /path/to/project && python manage.py cleanup_attendance --hours=168
    
    Notes:
        - Only deletes sessions with session_active=False
        - Preserves active sessions regardless of age
        - Uses database transaction for safety
        - Logs detailed information about deleted sessions
    """
    import time as time_module
    from datetime import timedelta
    
    start_time = time_module.time()
    
    results = {
        'deleted_count': 0,
        'meeting_count': 0,
        'oldest_session': None,
        'space_freed_mb': 0.0,
        'duration': 0,
        'dry_run': dry_run
    }
    
    try:
        logger.info(
            f"\n{'='*80}\n"
            f"🧹 DATABASE CLEANUP STARTED\n"
            f"{'='*80}\n"
            f"  Cutoff Age: {hours} hours\n"
            f"  Dry Run: {dry_run}\n"
            f"  Timestamp: {timezone.now()}\n"
            f"{'='*80}"
        )
        
        # ============================================================
        # STEP 1: Calculate cutoff time
        # ============================================================
        cutoff_time = timezone.now() - timedelta(hours=hours)
        
        logger.info(f"🕐 CLEANUP: Cutoff time is {cutoff_time}")
        
        # ============================================================
        # STEP 2: Find old inactive sessions
        # ============================================================
        old_sessions = AttendanceSession.objects.filter(
            last_activity__lt=cutoff_time,
            session_active=False  # Only delete inactive sessions
        )
        
        # Get count before deletion
        results['deleted_count'] = old_sessions.count()
        
        if results['deleted_count'] == 0:
            logger.info("✅ CLEANUP: No old sessions found to delete")
            results['duration'] = time_module.time() - start_time
            return results
        
        # ============================================================
        # STEP 3: Gather statistics before deletion
        # ============================================================
        try:
            # Get unique meeting IDs
            meeting_ids = list(old_sessions.values_list('meeting_id', flat=True).distinct())
            results['meeting_count'] = len(meeting_ids)
            
            # Get oldest session timestamp
            oldest = old_sessions.order_by('last_activity').first()
            if oldest:
                results['oldest_session'] = oldest.last_activity
            
            # Estimate space (rough calculation: ~2KB per session)
            results['space_freed_mb'] = (results['deleted_count'] * 2) / 1024
            
            logger.info(
                f"📊 CLEANUP ANALYSIS:\n"
                f"  - Sessions to Delete: {results['deleted_count']}\n"
                f"  - Meetings Affected: {results['meeting_count']}\n"
                f"  - Oldest Session: {results['oldest_session']}\n"
                f"  - Estimated Space: {results['space_freed_mb']:.2f} MB\n"
                f"  - Meeting IDs: {meeting_ids[:10]}{'...' if len(meeting_ids) > 10 else ''}"
            )
            
        except Exception as e:
            logger.warning(f"⚠️ Could not gather statistics: {e}")
        
        # ============================================================
        # STEP 4: Delete sessions (or skip if dry run)
        # ============================================================
        if dry_run:
            logger.info(
                f"🔍 DRY RUN: Would delete {results['deleted_count']} sessions "
                f"from {results['meeting_count']} meetings"
            )
        else:
            logger.info(f"🗑️ CLEANUP: Deleting {results['deleted_count']} sessions...")
            
            # Delete in transaction for safety
            with transaction.atomic():
                deleted_count = old_sessions.delete()[0]  # Returns tuple (count, details)
                
                logger.info(f"✅ CLEANUP: Successfully deleted {deleted_count} sessions")
        
        # ============================================================
        # STEP 5: Calculate duration and log summary
        # ============================================================
        results['duration'] = time_module.time() - start_time
        
        logger.info(
            f"\n{'='*80}\n"
            f"✅ DATABASE CLEANUP COMPLETE\n"
            f"{'='*80}\n"
            f"  Deleted Sessions: {results['deleted_count']}\n"
            f"  Meetings Affected: {results['meeting_count']}\n"
            f"  Oldest Session: {results['oldest_session']}\n"
            f"  Space Freed: {results['space_freed_mb']:.2f} MB\n"
            f"  Duration: {results['duration']:.2f}s\n"
            f"  Dry Run: {dry_run}\n"
            f"{'='*80}\n"
        )
        
        return results
        
    except Exception as e:
        logger.error(f"❌ CLEANUP FAILED: {e}")
        logger.error(traceback.format_exc())
        
        results['duration'] = time_module.time() - start_time
        return results


def auto_store_attendance_periodic(meeting_id: str, interval_seconds: int = 60, max_iterations: int = None) -> None:
    """
    ✅ AUTO-BACKUP - Periodically store attendance data in background
    
    This function runs in a background thread and automatically saves all session
    data to the database at regular intervals. Useful for long meetings to prevent
    data loss in case of server crashes.
    
    Args:
        meeting_id (str): Meeting identifier to backup
        interval_seconds (int): Seconds between each backup (default: 60)
        max_iterations (int, optional): Maximum number of backups to perform.
                                       If None, runs until meeting ends.
    
    Returns:
        None (runs in background thread)
    
    Example Usage:
        # Start auto-backup for a meeting (in background thread)
        >>> import threading
        >>> backup_thread = threading.Thread(
        ...     target=auto_store_attendance_periodic,
        ...     args=("meeting_123", 60),
        ...     daemon=True
        ... )
        >>> backup_thread.start()
        
        # With maximum iterations
        >>> backup_thread = threading.Thread(
        ...     target=auto_store_attendance_periodic,
        ...     args=("meeting_123", 60, 120),  # Max 120 backups (2 hours)
        ...     daemon=True
        ... )
        >>> backup_thread.start()
    
    Use Cases:
        1. Long Meetings: Auto-save during 2+ hour meetings
        2. Unstable Servers: Backup frequently on unreliable infrastructure
        3. High-Value Meetings: Critical exams or interviews
        4. Development/Testing: Prevent data loss during testing
    
    Stopping the Auto-Backup:
        - Automatically stops when no active sessions remain for the meeting
        - Automatically stops if max_iterations is reached
        - Use daemon=True when creating thread for auto-cleanup
    
    Performance Considerations:
        - Each backup saves ALL participants in the meeting
        - Default 60s interval = 60 database writes per hour per meeting
        - For 50 participants = 3000 DB operations per hour
        - Adjust interval based on your database capacity
    
    Recommended Intervals:
        - Short meetings (<30 min): Don't use auto-backup
        - Medium meetings (30-60 min): 120s interval
        - Long meetings (1-2 hours): 60s interval
        - Very long meetings (2+ hours): 30s interval
        - Critical exams: 30s interval
    
    Notes:
        - Must be run in a separate thread (daemon=True recommended)
        - Thread-safe operation
        - Gracefully handles exceptions
        - Logs all backup operations
        - Stops automatically when meeting ends
    """
    import time as time_module
    
    logger.info(
        f"\n{'='*80}\n"
        f"🔄 AUTO-BACKUP STARTED\n"
        f"{'='*80}\n"
        f"  Meeting ID: {meeting_id}\n"
        f"  Interval: {interval_seconds}s\n"
        f"  Max Iterations: {max_iterations if max_iterations else 'Unlimited'}\n"
        f"  Thread: {threading.current_thread().name}\n"
        f"  Started At: {timezone.now()}\n"
        f"{'='*80}"
    )
    
    iteration = 0
    total_saved = 0
    total_failed = 0
    
    try:
        while True:
            iteration += 1
            
            # ============================================================
            # STEP 1: Check if meeting still has active sessions
            # ============================================================
            active_sessions = [
                session_key 
                for session_key in attendance_sessions.keys() 
                if session_key.startswith(f"{meeting_id}_")
            ]
            
            if not active_sessions:
                logger.info(
                    f"🛑 AUTO-BACKUP STOPPED: No active sessions for meeting {meeting_id}\n"
                    f"  Total Iterations: {iteration - 1}\n"
                    f"  Total Saved: {total_saved}\n"
                    f"  Total Failed: {total_failed}"
                )
                break
            
            # ============================================================
            # STEP 2: Check if max iterations reached
            # ============================================================
            if max_iterations and iteration > max_iterations:
                logger.info(
                    f"🛑 AUTO-BACKUP STOPPED: Max iterations ({max_iterations}) reached\n"
                    f"  Total Saved: {total_saved}\n"
                    f"  Total Failed: {total_failed}"
                )
                break
            
            # ============================================================
            # STEP 3: Perform backup
            # ============================================================
            try:
                logger.debug(
                    f"🔄 AUTO-BACKUP [{iteration}]: Starting backup for {len(active_sessions)} sessions..."
                )
                
                # Store all sessions for this meeting
                results = store_all_active_sessions_to_db(meeting_id)
                
                total_saved += results['success']
                total_failed += results['failed']
                
                logger.info(
                    f"✅ AUTO-BACKUP [{iteration}]: Completed\n"
                    f"  - Active Sessions: {len(active_sessions)}\n"
                    f"  - Saved: {results['success']}\n"
                    f"  - Failed: {results['failed']}\n"
                    f"  - Duration: {results['duration']:.2f}s\n"
                    f"  - Next Backup In: {interval_seconds}s"
                )
                
                # Log warning if failures occurred
                if results['failed'] > 0:
                    logger.warning(
                        f"⚠️ AUTO-BACKUP [{iteration}]: Some sessions failed to save\n"
                        f"  Failed Users: {results['failed_users']}"
                    )
                
            except Exception as e:
                logger.error(f"❌ AUTO-BACKUP [{iteration}]: Backup failed - {e}")
                logger.error(traceback.format_exc())
                total_failed += len(active_sessions)
            
            # ============================================================
            # STEP 4: Sleep until next backup
            # ============================================================
            logger.debug(f"💤 AUTO-BACKUP [{iteration}]: Sleeping for {interval_seconds}s...")
            time_module.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info(f"🛑 AUTO-BACKUP INTERRUPTED: Manual stop requested")
        
    except Exception as e:
        logger.error(f"❌ AUTO-BACKUP CRASHED: {e}")
        logger.error(traceback.format_exc())
        
    finally:
        logger.info(
            f"\n{'='*80}\n"
            f"🛑 AUTO-BACKUP TERMINATED\n"
            f"{'='*80}\n"
            f"  Meeting ID: {meeting_id}\n"
            f"  Total Iterations: {iteration}\n"
            f"  Total Sessions Saved: {total_saved}\n"
            f"  Total Failures: {total_failed}\n"
            f"  Success Rate: {(total_saved / (total_saved + total_failed) * 100) if (total_saved + total_failed) > 0 else 0:.1f}%\n"
            f"  Ended At: {timezone.now()}\n"
            f"{'='*80}\n"
        )        

# ==================== CAMERA VERIFICATION ====================

@csrf_exempt
@require_http_methods(["POST"])
def verify_camera_resumed(request):
    """Verify camera was re-enabled after break"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        confirmation_token = data.get('confirmation_token')
        camera_active = data.get('camera_active', False)
        
        if not all([meeting_id, user_id, confirmation_token]):
            return JsonResponse({
                'success': False,
                'error': 'Missing required fields'
            }, status=400)
        
        user_id = str(user_id)
        validate_session_data(meeting_id, user_id)
        session_key = get_session_key(meeting_id, user_id)
        
        if session_key not in attendance_sessions:
            return JsonResponse({
                'success': False,
                'error': 'Session not found'
            }, status=404)
        
        session = attendance_sessions[session_key]
        expected_token = session.get('camera_confirmation_token')
        
        if not expected_token:
            logger.warning(f"CAMERA VERIFY: No token expected for {user_id}")
            return JsonResponse({
                'success': False,
                'error': 'No camera verification expected'
            }, status=400)
        
        if confirmation_token != expected_token:
            logger.warning(f"CAMERA VERIFY: Invalid token for {user_id}")
            return JsonResponse({
                'success': False,
                'error': 'Invalid confirmation token'
            }, status=403)
        
        deadline = session.get('camera_resume_deadline', 0)
        current_time = time.time()
        
        if current_time > deadline:
            logger.warning(f"CAMERA VERIFY: Deadline exceeded for {user_id}")
            return JsonResponse({
                'success': False,
                'error': 'Verification deadline exceeded',
                'requires_manual_restart': True
            }, status=408)
        
        if not camera_active:
            logger.warning(f"CAMERA VERIFY: Camera not active for {user_id}")
            return JsonResponse({
                'success': False,
                'error': 'Camera not active',
                'retry_required': True
            }, status=400)
        
        session['camera_resume_expected'] = False
        session['camera_resume_deadline'] = None
        session['camera_confirmation_token'] = None
        session['camera_verified_at'] = current_time
        
        try:
            attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
            extended_data = get_extended_tracking_data(attendance_obj)
            extended_data['camera_resume_expected'] = False
            extended_data['camera_resume_deadline'] = None
            extended_data['camera_confirmation_token'] = None
            extended_data['camera_verified_at'] = current_time
            save_extended_tracking_data(attendance_obj, extended_data)
        except AttendanceSession.DoesNotExist:
            pass
        
        logger.info(f"CAMERA VERIFIED for {user_id}")
        
        return JsonResponse({
            'success': True,
            'message': 'Camera verified successfully',
            'detection_can_start': True,
            'timestamp': current_time,
            'verified_at': current_time
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON'}, status=400)
    except ValidationError as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=400)
    except Exception as e:
        logger.error(f"Camera verification error: {e}")
        return JsonResponse({'success': False, 'error': 'Verification failed'}, status=500)

# ==================== PAUSE/RESUME WITH GRACE PERIOD ====================

@csrf_exempt
@require_http_methods(["POST"])
def pause_resume_attendance(request):
    """
    ✅ FIXED: Enhanced pause/resume with STRICT 5-minute break enforcement
    ✅ UPDATED: Preserves violation_start_times across breaks (never clears them)
    
    Changes from original:
    - Blocks break requests if >= 300 seconds already used
    - Prevents break if currently on break
    - Applies break penalty correctly
    - Sets break_used flag automatically when time exhausted
    - ✅ NEW: Does NOT clear violation_start_times on resume
    
    NO NEW DATABASE COLUMNS REQUIRED - Uses existing structure
    """
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        action = data.get('action')
        
        if not meeting_id or not user_id:
            return JsonResponse({
                'success': False, 
                'error': 'meeting_id and user_id are required'
            }, status=400)
            
        if action not in ['pause', 'resume']:
            return JsonResponse({
                'success': False, 
                'error': 'action must be "pause" or "resume"'
            }, status=400)
        
        user_id = str(user_id)
        validate_session_data(meeting_id, user_id)
        session_key = get_session_key(meeting_id, user_id)
        
        other_participants = [k for k in attendance_sessions.keys() 
                            if k.startswith(f"{meeting_id}_") and k != session_key]
        
        logger.info(f"MULTI-USER: {action} request for {user_id}. {len(other_participants)} other participants unaffected")
        
        if session_key not in attendance_sessions:
            return JsonResponse({
                'success': False, 
                'error': 'No active attendance session'
            }, status=404)
        
        session = attendance_sessions[session_key]
        current_time = time.time()
        
        try:
            attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
        except AttendanceSession.DoesNotExist:
            return JsonResponse({
                'success': False, 
                'error': 'Attendance session not found in database'
            }, status=404)
        
        # ✅ INITIALIZE: Break tracking if missing
        if 'total_break_time_used' not in session:
            session['total_break_time_used'] = attendance_obj.total_break_time_used or 0
            session['current_break_start_time'] = attendance_obj.current_break_start_time
            session['is_currently_on_break'] = attendance_obj.is_currently_on_break
            session['break_count'] = attendance_obj.break_count or 0
            session['break_used'] = attendance_obj.break_used
            session['max_break_time_allowed'] = attendance_obj.max_break_time_allowed or AttendanceConfig.MAX_TOTAL_BREAK_TIME
            try:
                session['break_sessions'] = json.loads(attendance_obj.break_sessions) if attendance_obj.break_sessions else []
            except json.JSONDecodeError:
                session['break_sessions'] = []
        
        max_break_time = session.get('max_break_time_allowed', AttendanceConfig.MAX_TOTAL_BREAK_TIME)
        
        # ============================================================================
        # ✅ ACTION: PAUSE (Take Break)
        # ============================================================================
        if action == 'pause':
            # ✅ CHECK 1: Already on break?
            if session.get('is_currently_on_break', False):
                current_total_break_time = calculate_current_break_time(session, current_time)
                return JsonResponse({
                    'success': False,
                    'error': 'Already on break',
                    'break_time_remaining': max(0, max_break_time - current_total_break_time),
                    'total_break_time_used': current_total_break_time,
                    'is_on_break': True,
                }, status=400)
            
            # ✅ CHECK 2: Calculate total break time used (including any current break)
            current_total_break_time = calculate_current_break_time(session, current_time)
            
            # ============================================================================
            # ✅ CRITICAL FIX: ENFORCE 5-MINUTE LIMIT - Block if already used >= 300s
            # ============================================================================
            if current_total_break_time >= max_break_time:
                logger.warning(
                    f"🚫 BREAK DENIED for {user_id}: Already used {current_total_break_time}s / {max_break_time}s"
                )
                return JsonResponse({
                    'success': False,
                    'error': 'Break time limit exceeded - No more break time available',
                    'message': 'You have already used your full 5-minute break allowance',
                    'total_break_time_used': current_total_break_time,
                    'max_break_time_allowed': max_break_time,
                    'break_time_remaining': 0,
                    'break_time_exhausted': True,
                    'is_on_break': False,
                    'can_take_break': False,
                }, status=403)
            
            # ✅ CHECK 3: How much break time is available?
            break_duration_available = max(0, max_break_time - current_total_break_time)
            
            if break_duration_available <= 0:
                logger.warning(
                    f"🚫 BREAK DENIED for {user_id}: No break time remaining"
                )
                return JsonResponse({
                    'success': False,
                    'error': 'No break time remaining',
                    'message': 'You have used all your break time',
                    'total_break_time_used': current_total_break_time,
                    'max_break_time_allowed': max_break_time,
                    'break_time_remaining': 0,
                    'break_time_exhausted': True,
                    'is_on_break': False,
                    'can_take_break': False,
                }, status=403)
            
            # ✅ ALLOW BREAK: Update session state
            session['is_currently_on_break'] = True
            session['current_break_start_time'] = current_time
            session['session_active'] = False
            session['break_count'] += 1
            
            # Note: Don't set break_used=True here! Only when time exhausted on resume.
            
            logger.info(
                f"✅ BREAK #{session['break_count']} STARTED for {user_id}:\n"
                f"  - Available: {break_duration_available}s\n"
                f"  - Already Used: {current_total_break_time}s\n"
                f"  - Remaining After: {max_break_time - current_total_break_time}s"
            )
            
            # ✅ UPDATE DATABASE
            attendance_obj.is_currently_on_break = True
            attendance_obj.current_break_start_time = current_time
            attendance_obj.session_active = False
            attendance_obj.break_count = session['break_count']
            attendance_obj.save()
            
            return JsonResponse({
                'success': True,
                'action': 'paused',
                'message': f'Break #{session["break_count"]} started',
                'break_time_remaining': break_duration_available,
                'total_break_time_used': current_total_break_time,
                'max_break_time_allowed': max_break_time,
                'break_count': session['break_count'],
                'break_used': session.get('break_used', False),
                'is_on_break': True,
                'break_start_time': current_time,
                'break_duration': break_duration_available,
                'camera_should_disable': True,
                'warning': f'You have {break_duration_available}s of break time remaining',
            })
        
        # ============================================================================
        # ✅ ACTION: RESUME (End Break)
        # ============================================================================
        elif action == 'resume':
            # ✅ CHECK: Not currently on break?
            if not session.get('is_currently_on_break', False):
                return JsonResponse({
                    'success': False,
                    'error': 'Not currently on break',
                    'break_time_remaining': max(0, max_break_time - session.get('total_break_time_used', 0)),
                    'total_break_time_used': session.get('total_break_time_used', 0),
                    'is_on_break': False,
                }, status=400)
            
            # ✅ CALCULATE: Break duration for this session
            update_break_time_used(session, attendance_obj, current_time)
            break_duration_used = current_time - session['current_break_start_time'] if session.get('current_break_start_time') else 0
            
            # ============================================================================
            # ✅ APPLY BREAK PENALTY: 1% per 5 minutes (300 seconds)
            # ============================================================================
            penalty_per_5_minutes = AttendanceConfig.BREAK_PENALTY  # 1.0%
            break_penalty = (break_duration_used / 300.0) * penalty_per_5_minutes
            session['attendance_penalty'] += break_penalty
            
            logger.info(
                f"💰 BREAK #{session['break_count']} PENALTY for {user_id}: "
                f"{break_penalty:.4f}% for {break_duration_used:.1f}s break. "
                f"Total penalty now: {session['attendance_penalty']:.4f}%"
            )
            
            # ============================================================================
            # ✅ CHECK: Automatically set break_used flag if time exhausted
            # ============================================================================
            total_break_used = session['total_break_time_used']
            if total_break_used >= max_break_time:
                session['break_used'] = True
                logger.info(f"🚫 break_used AUTO-SET to True for {user_id} - Time exhausted ({total_break_used}s >= {max_break_time}s)")
            
            # ✅ UPDATE SESSION STATE
            session['is_currently_on_break'] = False
            session['current_break_start_time'] = None
            session['session_active'] = True
            session['last_face_movement_time'] = current_time
            session['inactivity_popup_shown'] = False
            
            # ✅ CRITICAL: DO NOT CLEAR violation_start_times
            # Violations accumulated before break remain valid after break
            # This preserves the violation history across breaks
            logger.info(
                f"🔒 VIOLATION TIMES PRESERVED for {user_id} after break: "
                f"{len(session.get('violation_start_times', {}))} violations kept"
            )
            
            # ✅ ACTIVATE GRACE PERIOD
            grace_period_until = current_time + AttendanceConfig.GRACE_PERIOD_DURATION
            session['grace_period_active'] = True
            session['grace_period_until'] = grace_period_until
            
            # ✅ CAMERA VERIFICATION
            verification_token = generate_camera_verification_token(meeting_id, user_id, current_time)
            verification_deadline = current_time + AttendanceConfig.CAMERA_VERIFICATION_TIMEOUT
            
            session['camera_resume_expected'] = True
            session['camera_resume_deadline'] = verification_deadline
            session['camera_confirmation_token'] = verification_token
            
            logger.info(f"⏱️ GRACE PERIOD ACTIVATED for {user_id}: {AttendanceConfig.GRACE_PERIOD_DURATION}s")
            
            # ============================================================================
            # ✅ UPDATE DATABASE: Penalty, percentage, engagement score, break_used
            # ============================================================================
            attendance_obj.is_currently_on_break = False
            attendance_obj.current_break_start_time = None
            attendance_obj.session_active = True
            attendance_obj.last_face_movement_time = current_time
            attendance_obj.inactivity_popup_shown = False
            
            attendance_obj.attendance_penalty = session['attendance_penalty']
            new_percentage = max(0, 100 - session['attendance_penalty'])
            attendance_obj.attendance_percentage = new_percentage
            attendance_obj.engagement_score = new_percentage
            
            # ✅ Update break_used flag in database
            attendance_obj.break_used = session['break_used']
            
            # ✅ PRESERVE violation_start_times in database (don't reset)
            attendance_obj.violation_start_times = json.dumps(session.get('violation_start_times', {}))
            
            attendance_obj.save()
            
            logger.info(
                f"💾 DB UPDATED: User {user_id} - "
                f"Penalty: {session['attendance_penalty']:.4f}%, "
                f"Attendance: {new_percentage:.2f}%, "
                f"Engagement: {new_percentage:.2f}%, "
                f"break_used: {session['break_used']}, "
                f"Violation times preserved: {len(session.get('violation_start_times', {}))}"
            )
            
            # ✅ UPDATE EXTENDED DATA
            try:
                extended_data = get_extended_tracking_data(attendance_obj)
                extended_data['camera_resume_expected'] = True
                extended_data['camera_resume_deadline'] = verification_deadline
                extended_data['camera_confirmation_token'] = verification_token
                extended_data['grace_period_active'] = True
                extended_data['grace_period_until'] = grace_period_until
                save_extended_tracking_data(attendance_obj, extended_data)
            except Exception as e:
                logger.error(f"Failed to save camera verification: {e}")
            
            break_time_remaining = max(0, max_break_time - session['total_break_time_used'])
            break_exhausted = break_time_remaining <= 0
            
            logger.info(
                f"✅ BREAK #{session['break_count']} ENDED for {user_id}:\n"
                f"  - Duration: {break_duration_used:.1f}s\n"
                f"  - Total Used: {session['total_break_time_used']}s\n"
                f"  - Remaining: {break_time_remaining}s\n"
                f"  - break_used: {session['break_used']}\n"
                f"  - Violation times preserved: {len(session.get('violation_start_times', {}))}\n"
                f"  - Grace period active"
            )
            
            response_data = {
                'success': True,
                'action': 'resumed',
                'message': f'Break #{session["break_count"]} ended. Detection resumed.',
                'break_time_remaining': break_time_remaining,
                'total_break_time_used': session['total_break_time_used'],
                'max_break_time_allowed': max_break_time,
                'break_count': session['break_count'],
                'break_used': session.get('break_used', False),
                'is_on_break': False,
                'can_take_more_breaks': not break_exhausted and break_time_remaining > 0,
                'break_duration_used': break_duration_used,
                
                # ✅ BREAK EXHAUSTION WARNING
                'break_time_exhausted': break_exhausted,
                'break_warning': 'No more break time available - break_used flag set' if break_exhausted else None,
                
                # ✅ Break penalty information
                'break_penalty_applied': break_penalty,
                'break_penalty_percentage': f"{break_penalty:.4f}%",
                'total_penalty': session['attendance_penalty'],
                'total_penalty_percentage': f"{session['attendance_penalty']:.4f}%",
                'attendance_percentage': new_percentage,
                'engagement_score': new_percentage,
                'new_attendance_percentage': float(new_percentage),
                
                # ✅ Grace period
                'grace_period_active': True,
                'grace_period_duration_seconds': AttendanceConfig.GRACE_PERIOD_DURATION,
                'grace_period_expires_in': AttendanceConfig.GRACE_PERIOD_DURATION,
                'grace_period_message': f'Camera resumed - grace period {AttendanceConfig.GRACE_PERIOD_DURATION}s active',
                
                # ✅ Camera verification
                'camera_should_resume': True,
                'camera_required': True,
                'camera_enforcement': 'mandatory',
                'next_action': 'enable_camera_immediately',
                'camera_verification_required': True,
                'camera_verification_deadline': verification_deadline,
                'camera_confirmation_token': verification_token,
                'camera_verification_timeout_seconds': AttendanceConfig.CAMERA_VERIFICATION_TIMEOUT,
                
                # ✅ Violation history preserved
                'violation_times_preserved': len(session.get('violation_start_times', {})),
                'violation_history_kept': True,
            }
            
            return JsonResponse(response_data)
        
    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON'}, status=400)
    except ValidationError as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=400)
    except Exception as e:
        logger.error(f"Error in pause_resume: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return JsonResponse({'success': False, 'error': 'Internal server error'}, status=500)
        
             
# ==================== DETECT VIOLATIONS (SYNC - NO ASYNC) ====================

# ==================== COMPLETE detect_violations FUNCTION ====================
# ✅ This is the COMPLETE function with ALL fixes for 120-second removal tracking


@csrf_exempt
@require_http_methods(["POST"])
def detect_violations(request):
    """
    ✅ UPDATED: Complete detect_violations with CORRECT 20-second threshold
    ✅ Changed from 21 seconds to 20 seconds for violation detection
    
    KEY FEATURES:
    1. 20-second timer ONLY starts AFTER warning phase (4 warnings complete)
    2. Penalties ONLY apply AFTER warning phase (in detection phase)
    3. Timer pauses if violations have gaps
    4. Warning phase = NO PENALTIES, just warnings
    5. Detection phase = penalties start (0.25% per 3 detections)
    
    ALL EXISTING FEATURES PRESERVED:
    - Identity verification
    - Grace period
    - Camera verification
    - Break system
    - Removal count tracking
    - Behavior message storage
    - 120-second continuous violation removal
    """
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        frame_data = data.get('frame')
        
        validate_session_data(meeting_id, user_id)
        
        if not frame_data:
            return JsonResponse({"status": "error", "message": "Missing data"}, status=400)

        session_key = get_session_key(meeting_id, user_id)
        concurrent_sessions = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
        
        if session_key not in attendance_sessions:
            logger.info(f"MULTI-USER: Auto-starting session for {user_id}")
            start_success = start_attendance_tracking(meeting_id, user_id)
            if not start_success:
                return JsonResponse({"status": "error", "message": "Failed to start session"}, status=500)

        session = attendance_sessions[session_key]
        
        # ============================================================
        # INITIALIZE SESSION FIELDS
        # ============================================================
        if 'violation_start_times' not in session:
            session['violation_start_times'] = {}
        
        # Continuous timer fields
        if 'violation_continuous_timer' not in session:
            session['violation_continuous_timer'] = None
        if 'violation_current_type' not in session:
            session['violation_current_type'] = None
        if 'violation_popup_shown' not in session:
            session['violation_popup_shown'] = False
        
        if 'popup_count' not in session:
            session['popup_count'] = 0
        if 'warning_count' not in session:
            session['warning_count'] = 0
        if 'detection_counts' not in session:
            session['detection_counts'] = 0
        if 'last_popup_time' not in session:
            session['last_popup_time'] = 0
        if 'inactivity_popup_shown' not in session:
            session['inactivity_popup_shown'] = False
        if 'baseline_established' not in session:
            session['baseline_established'] = False
        if 'baseline_ear' not in session:
            session['baseline_ear'] = None
        if 'baseline_yaw' not in session:
            session['baseline_yaw'] = None
        if 'baseline_samples' not in session:
            session['baseline_samples'] = 0
        if 'face_detected' not in session:
            session['face_detected'] = False
        if 'last_face_movement_time' not in session:
            session['last_face_movement_time'] = time.time()
        if 'total_detections' not in session:
            session['total_detections'] = 0
        if 'is_removed_from_meeting' not in session:
            session['is_removed_from_meeting'] = False
        if 'continuous_violation_start_time' not in session:
            session['continuous_violation_start_time'] = None
        if 'last_detection_time' not in session:
            session['last_detection_time'] = 0.0
        if 'detection_penalty_applied' not in session:
            session['detection_penalty_applied'] = False
        if 'warning_phase_complete' not in session:
            session['warning_phase_complete'] = False
        if 'total_detection_penalty_applied' not in session:
            session['total_detection_penalty_applied'] = 0.0
        if 'total_break_time_used' not in session:
            session['total_break_time_used'] = 0
        if 'current_break_start_time' not in session:
            session['current_break_start_time'] = None
        if 'is_currently_on_break' not in session:
            session['is_currently_on_break'] = False
        if 'break_count' not in session:
            session['break_count'] = 0
        if 'break_sessions' not in session:
            session['break_sessions'] = []
        if 'max_break_time_allowed' not in session:
            session['max_break_time_allowed'] = AttendanceConfig.MAX_TOTAL_BREAK_TIME
        if 'camera_resume_expected' not in session:
            session['camera_resume_expected'] = False
        if 'camera_resume_deadline' not in session:
            session['camera_resume_deadline'] = None
        if 'camera_verified_at' not in session:
            session['camera_verified_at'] = None
        if 'grace_period_active' not in session:
            session['grace_period_active'] = False
        if 'grace_period_until' not in session:
            session['grace_period_until'] = None
        if 'frame_processing_count' not in session:
            session['frame_processing_count'] = 0
        if 'active_participation_time' not in session:
            session['active_participation_time'] = 0
        if 'violation_severity_score' not in session:
            session['violation_severity_score'] = 0.0
        if 'continuous_violation_time' not in session:
            session['continuous_violation_time'] = 0
        if 'last_violation_type' not in session:
            session['last_violation_type'] = ""
        if 'attendance_penalty' not in session:
            session['attendance_penalty'] = 0.0
        if 'violations' not in session:
            session['violations'] = []
        
        # Identity verification fields
        if 'identity_warning_count' not in session:
            session['identity_warning_count'] = 0
        if 'identity_consecutive_unknown_seconds' not in session:
            session['identity_consecutive_unknown_seconds'] = 0
        if 'identity_total_unknown_seconds' not in session:
            session['identity_total_unknown_seconds'] = 0
        if 'identity_is_removed' not in session:
            session['identity_is_removed'] = False
        if 'identity_warnings' not in session:
            session['identity_warnings'] = []
        if 'identity_last_check_time' not in session:
            session['identity_last_check_time'] = 0.0
        if 'identity_can_rejoin' not in session:
            session['identity_can_rejoin'] = True
        
        # Removal tracking fields
        if 'identity_removal_count' not in session:
            session['identity_removal_count'] = 0
        if 'identity_total_warnings' not in session:
            session['identity_total_warnings'] = 0
        if 'identity_current_cycle_warnings' not in session:
            session['identity_current_cycle_warnings'] = 0
        if 'behavior_removal_count' not in session:
            session['behavior_removal_count'] = 0
        if 'continuous_violation_removal_count' not in session:
            session['continuous_violation_removal_count'] = 0
        
        # Behavior messages field
        if 'behavior_messages' not in session:
            session['behavior_messages'] = {'warnings': [], 'detections': [], 'continuous_removals': []}

        # ============================================================
        # SYNC FROM DATABASE
        # ============================================================
        try:
            db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
            extended_data = get_extended_tracking_data(db_session)
            
            if session.get('warning_count', 0) == 0 and extended_data.get('warning_count', 0) > 0:
                session['warning_count'] = extended_data.get('warning_count', 0)
                session['popup_count'] = db_session.popup_count
                session['detection_counts'] = extended_data.get('detection_counts', 0)
                session['total_detection_penalty_applied'] = (extended_data.get('detection_counts', 0) // 3) * AttendanceConfig.DETECTION_PENALTY_3
                session['detection_penalty_applied'] = extended_data.get('detection_penalty_applied', False)
                session['warning_phase_complete'] = extended_data.get('warning_phase_complete', False)
                session['attendance_penalty'] = float(db_session.attendance_penalty)
                session['total_break_time_used'] = db_session.total_break_time_used or 0
                session['break_count'] = db_session.break_count or 0
                session['is_currently_on_break'] = db_session.is_currently_on_break
                session['is_removed_from_meeting'] = extended_data.get('is_removed_from_meeting', False)
                session['continuous_violation_start_time'] = extended_data.get('continuous_violation_start_time')
                
                # Sync identity verification state
                session['identity_warning_count'] = db_session.identity_warning_count
                session['identity_consecutive_unknown_seconds'] = db_session.identity_consecutive_unknown_seconds
                session['identity_total_unknown_seconds'] = db_session.identity_total_unknown_seconds
                session['identity_is_removed'] = db_session.identity_is_removed
                session['identity_warnings'] = db_session.get_identity_warnings()
                session['identity_last_check_time'] = db_session.identity_last_check_time
                session['identity_can_rejoin'] = db_session.identity_can_rejoin
                
                # Sync removal tracking fields
                session['identity_removal_count'] = db_session.identity_removal_count
                session['identity_total_warnings'] = db_session.identity_total_warnings_issued
                session['identity_current_cycle_warnings'] = db_session.identity_current_cycle_warnings
                session['behavior_removal_count'] = db_session.behavior_removal_count
                session['continuous_violation_removal_count'] = db_session.continuous_violation_removal_count
                
                # Sync behavior messages
                session['behavior_messages'] = db_session.get_behavior_messages()
                
                try:
                    db_violation_times = json.loads(db_session.violation_start_times) if db_session.violation_start_times else {}
                    if db_violation_times:
                        session['violation_start_times'] = db_violation_times
                except json.JSONDecodeError:
                    pass
        except AttendanceSession.DoesNotExist:
            db_session = None

        # ============================================================
        # GRACE PERIOD CHECK
        # ============================================================
        current_time = time.time()
        if session.get('grace_period_active', False):
            grace_until = session.get('grace_period_until', 0)
            
            if current_time < grace_until:
                time_remaining = grace_until - current_time
                return JsonResponse({
                    "status": "ok",
                    "popup": "",
                    "violations": [],
                    "attendance_percentage": max(0, 100 - session.get("attendance_penalty", 0)),
                    "grace_period_active": True,
                    "grace_period_expires_in": time_remaining,
                })
            else:
                session['grace_period_active'] = False
                session['grace_period_until'] = None
        
        # ============================================================
        # CAMERA VERIFICATION CHECK
        # ============================================================
        if session.get('camera_resume_expected', False) and int(session.get('break_count', 0)) > 0:
            deadline = session.get('camera_resume_deadline', 0)
            if current_time > deadline:
                session['camera_resume_expected'] = False
                return JsonResponse({
                    "status": "camera_verification_failed",
                    "message": "Camera verification deadline exceeded",
                })
            else:
                return JsonResponse({
                    "status": "awaiting_camera_verification",
                    "message": "Waiting for camera",
                })
        
        # ============================================================
        # BEHAVIOR REMOVAL CHECK
        # ============================================================
        if session.get('is_removed_from_meeting', False):
            return JsonResponse({
                "status": "removed_from_meeting",
                "message": "You were removed from meeting due to continuous violations.",
                "removal_type": "continuous_violations",
            })
        
        # ============================================================
        # IDENTITY REMOVAL CHECK
        # ============================================================
        if session.get('identity_is_removed', False):
            return JsonResponse({
                "status": "removed_from_meeting",
                "message": "You were removed due to identity verification failure.",
                "removal_type": "identity_verification",
            })
        
        # ============================================================
        # BREAK CHECK
        # ============================================================
        if session.get('is_currently_on_break', False):
            return JsonResponse({
                "status": "session_paused", 
                "message": "Session is paused (break mode)",
            })

        # ============================================================
        # SESSION ACTIVE CHECK
        # ============================================================
        if not session["session_active"]:
            return JsonResponse({"status": "session_paused", "message": "Session is paused"})

        # ============================================================
        # IDENTITY VERIFICATION
        # ============================================================
        identity_result = None
        if db_session:
            frame = decode_image(frame_data)
            if frame is not None:
                identity_result = check_identity_verification(
                    session, db_session, frame, user_id, current_time
                )

        # ============================================================
        # PROCESS FRAME FOR BEHAVIOR DETECTION
        # ============================================================
        frame = decode_image(frame_data)
        if frame is None:
            return JsonResponse({"status": "error", "message": "Failed to decode frame"}, status=400)
        
        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        session["frame_processing_count"] += 1
        
        # MediaPipe processing
        face_results = mp_face.process(rgb)
        mesh_results = mp_mesh.process(rgb)
        pose_results = mp_pose.process(rgb)
        hand_results = mp_hands.process(rgb)
        
        violations = []
        immediate_violations = []
        baseline_violations = []
        popup = ""
        
        session["face_detected"] = False
        
        # ============================================================
        # FACE DETECTION
        # ============================================================
        if face_results.detections:
            num_faces = len(face_results.detections)
            if num_faces > 1:
                violations.append("Multiple faces detected")
                immediate_violations.append("Multiple faces detected")
            elif num_faces == 1:
                session["face_detected"] = True
                session["last_face_movement_time"] = current_time
                session["inactivity_popup_shown"] = False
        
        # ============================================================
        # BASELINE ESTABLISHMENT
        # ============================================================
        if not session.get("baseline_established", False):
            if mesh_results.multi_face_landmarks and pose_results.pose_landmarks:
                landmarks = mesh_results.multi_face_landmarks[0].landmark
                
                left_eye = [landmarks[i] for i in [33, 160, 158, 133, 153, 144]]
                right_eye = [landmarks[i] for i in [362, 385, 387, 263, 373, 380]]
                ear = enhanced_ear(left_eye, right_eye)
                
                nose = landmarks[1]
                left_face = landmarks[234]
                right_face = landmarks[454]
                dx = right_face.x - left_face.x
                dz = right_face.z - left_face.z
                yaw = np.degrees(np.arctan2(dz, dx))
                
                if session["baseline_ear"] is None:
                    session["baseline_ear"] = ear
                else:
                    session["baseline_ear"] = (session["baseline_ear"] * session["baseline_samples"] + ear) / (session["baseline_samples"] + 1)
                
                if session["baseline_yaw"] is None:
                    session["baseline_yaw"] = yaw
                else:
                    session["baseline_yaw"] = (session["baseline_yaw"] * session["baseline_samples"] + yaw) / (session["baseline_samples"] + 1)
                
                session["baseline_samples"] += 1
                
                if session["baseline_samples"] >= AttendanceConfig.BASELINE_FRAMES_REQUIRED:
                    session["baseline_established"] = True
                    logger.info(f"BASELINE ESTABLISHED for {user_id}")
        
        # ============================================================
        # VIOLATION DETECTION
        # ============================================================
        if session.get("baseline_established", False):
            # Eyes closed
            if mesh_results.multi_face_landmarks:
                landmarks = mesh_results.multi_face_landmarks[0].landmark
                left_eye = [landmarks[i] for i in [33, 160, 158, 133, 153, 144]]
                right_eye = [landmarks[i] for i in [362, 385, 387, 263, 373, 380]]
                ear = enhanced_ear(left_eye, right_eye)
                baseline_ear = session.get("baseline_ear", 0.22)
                if ear < baseline_ear * 0.7:
                    violations.append("Eyes closed")
            
            # Head turned
            if mesh_results.multi_face_landmarks:
                landmarks = mesh_results.multi_face_landmarks[0].landmark
                left_face = landmarks[234]
                right_face = landmarks[454]
                dx = right_face.x - left_face.x
                dz = right_face.z - left_face.z
                yaw = np.degrees(np.arctan2(dz, dx))
                baseline_yaw = session.get("baseline_yaw", 0)
                if abs(yaw - baseline_yaw) > 25:
                    violations.append("Head turned")
            
            # Hand near face
            if hand_results.multi_hand_landmarks and mesh_results.multi_face_landmarks:
                face_landmarks = mesh_results.multi_face_landmarks[0].landmark
                nose = face_landmarks[1]
                for hand_landmarks in hand_results.multi_hand_landmarks:
                    for landmark in hand_landmarks.landmark:
                        distance = np.sqrt((landmark.x - nose.x)**2 + (landmark.y - nose.y)**2)
                        if distance < 0.12:
                            violations.append("Hand near face")
                            break
            
            # Lying down
            if pose_results.pose_landmarks:
                if is_fully_lying_down(pose_results.pose_landmarks.landmark):
                    violations.append("Lying down")
            
            # Face not visible
            if not session.get("face_detected", False):
                violations.append("Face not visible")
            
            # Inactivity
            time_since_movement = current_time - session.get("last_face_movement_time", current_time)
            if time_since_movement > 20:
                violations.append("Inactivity detected")
            elif time_since_movement > 10:
                if not session.get("inactivity_popup_shown", False):
                    popup = "⚠️ Inactivity detected. Please show movement."
                    session["inactivity_popup_shown"] = True
        
        # ============================================================
        # ✅✅✅ CONTINUOUS 20-SECOND TIMER (SINGLE VIOLATION) - UPDATED
        # ============================================================
        unique_violations = list(set(violations))
        violation_to_show = None
        violation_duration = 0.0
        
        if unique_violations:
            first_violation = unique_violations[0]
            
            if session['violation_current_type'] == first_violation:
                # Same violation continuing
                if session['violation_continuous_timer'] is not None:
                    violation_duration = current_time - session['violation_continuous_timer']
                    
                    # ✅ CHANGED: 21.0 → 20.0 seconds threshold
                    if violation_duration >= 20.0 and not session['violation_popup_shown']:
                        violation_to_show = first_violation
                        logger.info(f"✅ THRESHOLD: '{first_violation}' at {violation_duration:.1f}s")
            else:
                # New/different violation
                session['violation_continuous_timer'] = current_time
                session['violation_current_type'] = first_violation
                session['violation_popup_shown'] = False
                logger.info(f"🆕 NEW PERIOD: '{first_violation}' at {current_time}")
        else:
            # No violations
            if session['violation_current_type'] is not None:
                final_duration = current_time - session['violation_continuous_timer'] if session['violation_continuous_timer'] else 0
                logger.info(f"🛑 STOPPED: '{session['violation_current_type']}' after {final_duration:.1f}s")
                session['violation_continuous_timer'] = None
                session['violation_current_type'] = None
                session['violation_popup_shown'] = False
        
        # ============================================================
        # WARNING/DETECTION PHASE
        # ============================================================
        if violation_to_show is not None and (current_time - session.get("last_popup_time", 0)) >= 20:
            
            if session["popup_count"] < 4:
                # ============================================================
                # ✅ WARNING PHASE - NO PENALTIES
                # ============================================================
                session["popup_count"] += 1
                session["warning_count"] = session["popup_count"]
                session["last_popup_time"] = current_time
                session['violation_popup_shown'] = True
                
                popup = f"⚠️ Warning {session['popup_count']}/4: {violation_to_show}"
                
                logger.warning(
                    f"⚠️ WARNING #{session['popup_count']}/4 for {user_id}: {violation_to_show}\n"
                    f"   Duration: {violation_duration:.1f}s\n"
                    f"   ⚠️ WARNING PHASE - NO PENALTY APPLIED"
                )
                
                # Save to database
                try:
                    db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                    extended_data = get_extended_tracking_data(db_session)
                    extended_data['warning_count'] = session["popup_count"]
                    db_session.popup_count = session["popup_count"]
                    save_extended_tracking_data(db_session, extended_data)
                    db_session.save()
                except Exception as e:
                    logger.error(f"Failed to save warning: {e}")
                
                # Save warning message
                try:
                    db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                    db_session.add_behavior_warning(
                        warning_number=session["popup_count"],
                        violation_type=violation_to_show,
                        duration=violation_duration,
                        timestamp=current_time
                    )
                    db_session.save()
                    session["behavior_messages"] = db_session.get_behavior_messages()
                except Exception as e:
                    logger.error(f"Failed to save warning message: {e}")
                
                # Check if warning phase complete
                if session["popup_count"] >= 4:
                    session["warning_phase_complete"] = True
                    logger.warning(f"✅ WARNING PHASE COMPLETE for {user_id} - Detection phase begins")
            
            elif session.get("warning_phase_complete", False):
                # ============================================================
                # ✅ DETECTION PHASE - PENALTIES START HERE
                # ============================================================
                time_since_last_detection = current_time - session.get("last_detection_time", 0)
                
                if time_since_last_detection >= 20:
                    session["detection_counts"] += 1
                    session["last_detection_time"] = current_time
                    session["last_popup_time"] = current_time
                    session['violation_popup_shown'] = True
                    
                    # Save detection
                    try:
                        db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                        extended_data = get_extended_tracking_data(db_session)
                        extended_data['detection_counts'] = session["detection_counts"]
                        extended_data['last_detection_time'] = current_time
                        save_extended_tracking_data(db_session, extended_data)
                    except Exception as e:
                        logger.error(f"Failed to save detection: {e}")
                    
                    # ✅ CALCULATE PENALTY (Every 3 detections = 0.25%)
                    batches_completed = session["detection_counts"] // 3
                    total_detection_penalty = batches_completed * 0.25
                    previous_penalty = session.get("total_detection_penalty_applied", 0.0)
                    new_penalty = total_detection_penalty - previous_penalty
                    
                    if new_penalty > 0:
                        session["attendance_penalty"] += new_penalty
                        session["total_detection_penalty_applied"] = total_detection_penalty
                        
                        logger.error(
                            f"💰 PENALTY APPLIED for {user_id}:\n"
                            f"   Detection: #{session['detection_counts']}\n"
                            f"   Penalty: {new_penalty:.4f}%\n"
                            f"   Total Penalty: {session['attendance_penalty']:.4f}%"
                        )
                        
                        popup = f"🔴 Detection {session['detection_counts']}: {violation_to_show}. Penalty: {new_penalty:.2f}%"
                        
                        # Save penalty
                        try:
                            db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                            db_session.attendance_penalty = session["attendance_penalty"]
                            db_session.attendance_percentage = max(0, 100 - session["attendance_penalty"])
                            db_session.engagement_score = max(0, 100 - session["attendance_penalty"])
                            
                            extended_data = get_extended_tracking_data(db_session)
                            extended_data['detection_penalty_applied'] = True
                            extended_data['total_detection_penalty'] = total_detection_penalty
                            save_extended_tracking_data(db_session, extended_data)
                            db_session.save()
                        except Exception as e:
                            logger.error(f"Failed to save penalty: {e}")
                    else:
                        popup = f"🔴 Detection {session['detection_counts']}: {violation_to_show}"
                    
                    # Save detection message
                    try:
                        db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                        db_session.add_behavior_detection(
                            detection_number=session["detection_counts"],
                            violation_type=violation_to_show,
                            duration=violation_duration,
                            timestamp=current_time,
                            penalty=new_penalty if new_penalty > 0 else 0.0
                        )
                        db_session.save()
                        session["behavior_messages"] = db_session.get_behavior_messages()
                    except Exception as e:
                        logger.error(f"Failed to save detection message: {e}")
        
        # ============================================================
        # ✅✅✅ FIXED: 2-MINUTE REMOVAL (ONLY IN DETECTION PHASE)
        # ============================================================
        if unique_violations:
            # ✅ KEY FIX: Only start 2-min timer AFTER warning phase complete
            if session.get("warning_phase_complete", False):
                # Warning phase is complete, NOW start 2-minute timer
                if session.get("continuous_violation_start_time") is None:
                    session["continuous_violation_start_time"] = current_time
                    logger.info(
                        f"🚨 2-MIN TIMER STARTED for {user_id}\n"
                        f"   ✅ Warning phase complete (4/4 warnings)\n"
                        f"   ✅ Now in detection phase - 2-minute timer active"
                    )
                
                # Check duration
                continuous_duration = current_time - session["continuous_violation_start_time"]
                
                logger.debug(
                    f"⏱️ 2-MIN TIMER: {continuous_duration:.1f}s / 120s for {user_id}"
                )
                
                # Remove after 2 minutes (120 seconds)
                if continuous_duration >= 120:
                    session["is_removed_from_meeting"] = True
                    session["removal_timestamp"] = timezone.now()
                    session["removal_reason"] = f"Continuous violations for {continuous_duration:.0f}s in detection phase"
                    session["attendance_penalty"] += 1.0  # 1% penalty
                    session["session_active"] = False
                    
                    # ✅ INCREMENT REMOVAL COUNTERS
                    session["behavior_removal_count"] += 1
                    session["continuous_violation_removal_count"] += 1
                    
                    # Save removal
                    try:
                        db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                        db_session.behavior_removal_count = session["behavior_removal_count"]
                        db_session.continuous_violation_removal_count = session["continuous_violation_removal_count"]
                        
                        # ✅ Save 120-second removal event to violations column
                        db_session.add_continuous_violation_removal(
                            duration=continuous_duration,
                            timestamp=current_time,
                            violation_type=first_violation
                        )
                        
                        extended_data = get_extended_tracking_data(db_session)
                        extended_data['is_removed_from_meeting'] = True
                        extended_data['removal_timestamp'] = session["removal_timestamp"].isoformat()
                        extended_data['removal_reason'] = session["removal_reason"]
                        save_extended_tracking_data(db_session, extended_data)
                        
                        db_session.attendance_penalty = session["attendance_penalty"]
                        db_session.session_active = False
                        db_session.save()
                        
                        # Update session behavior messages
                        session["behavior_messages"] = db_session.get_behavior_messages()
                        
                        logger.critical(
                            f"🚫 USER REMOVED after 120s in DETECTION PHASE\n"
                            f"   User: {user_id}\n"
                            f"   Duration: {continuous_duration:.0f}s\n"
                            f"   Warnings: 4/4 (complete)\n"
                            f"   Detections: {session['detection_counts']}\n"
                            f"   Behavior Removal Count: {db_session.behavior_removal_count}\n"
                            f"   120s Removal Count: {db_session.continuous_violation_removal_count}"
                        )
                    except Exception as e:
                        logger.error(f"Failed to save removal: {e}")
                    
                    store_attendance_to_db(meeting_id, user_id)
                    
                    return JsonResponse({
                        "status": "participant_removed",
                        "message": f"Removed after {continuous_duration:.0f}s of continuous violations in detection phase",
                        "removal_reason": session["removal_reason"],
                        "behavior_removal_count": session["behavior_removal_count"],
                        "continuous_violation_removal_count": session["continuous_violation_removal_count"],
                    })
            else:
                # ✅ WARNING PHASE - DO NOT START 2-MINUTE TIMER
                logger.debug(
                    f"⏸️ 2-MIN TIMER NOT STARTED for {user_id}\n"
                    f"   Reason: Still in warning phase ({session['popup_count']}/4 warnings)\n"
                    f"   Timer will start AFTER 4 warnings complete"
                )
                # Ensure timer is not running during warning phase
                if session.get("continuous_violation_start_time") is not None:
                    logger.warning(
                        f"⚠️ RESETTING 2-MIN TIMER for {user_id}\n"
                        f"   Reason: Should not run during warning phase"
                    )
                    session["continuous_violation_start_time"] = None
        else:
            # No violations - reset timer
            if session.get("continuous_violation_start_time") is not None:
                logger.info(f"✅ 2-MIN TIMER RESET for {user_id} - No violations")
                session["continuous_violation_start_time"] = None
        
        # ============================================================
        # CALCULATE ATTENDANCE PERCENTAGE
        # ============================================================
        percentage = max(0, 100 - session.get("attendance_penalty", 0))
        
        # ============================================================
        # SAVE TO DATABASE
        # ============================================================
        try:
            db_session = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
            
            extended_data = {
                'detection_counts': session.get("detection_counts", 0),
                'warning_count': session.get("warning_count", 0),
                'is_removed_from_meeting': session.get("is_removed_from_meeting", False),
                'removal_timestamp': session.get("removal_timestamp").isoformat() if session.get("removal_timestamp") else None,
                'removal_reason': session.get("removal_reason", ""),
                'continuous_violation_start_time': session.get("continuous_violation_start_time"),
                'last_detection_time': session.get("last_detection_time", 0.0),
                'detection_penalty_applied': session.get("detection_penalty_applied", False),
                'warning_phase_complete': session.get("warning_phase_complete", False),
                'camera_resume_expected': session.get("camera_resume_expected", False),
                'camera_resume_deadline': session.get("camera_resume_deadline"),
                'camera_confirmation_token': session.get("camera_confirmation_token"),
                'camera_verified_at': session.get("camera_verified_at"),
                'grace_period_active': session.get("grace_period_active", False),
                'grace_period_until': session.get("grace_period_until"),
                'total_detection_penalty': session.get("total_detection_penalty_applied", 0.0),
                'detection_batches_completed': session.get("detection_counts", 0) // 3,
            }
            
            db_session.popup_count = session["popup_count"]
            db_session.detection_counts = json.dumps(extended_data)
            db_session.total_detections = session["total_detections"]
            db_session.attendance_penalty = session.get("attendance_penalty", 0.0)
            db_session.session_active = session.get("session_active", True)
            db_session.violations = json.dumps(session.get("behavior_messages", {'warnings': [], 'detections': [], 'continuous_removals': []}))
            db_session.last_activity = timezone.now()
            db_session.attendance_percentage = percentage
            db_session.engagement_score = percentage
            db_session.frame_processing_count = session["frame_processing_count"]
            
            # Identity fields
            db_session.identity_warning_count = session.get("identity_warning_count", 0)
            db_session.identity_consecutive_unknown_seconds = session.get("identity_consecutive_unknown_seconds", 0)
            db_session.identity_total_unknown_seconds = session.get("identity_total_unknown_seconds", 0)
            db_session.identity_is_removed = session.get("identity_is_removed", False)
            if session.get("identity_is_removed") and not db_session.identity_removal_time:
                db_session.identity_removal_time = timezone.now()
            db_session.identity_warnings = json.dumps(session.get("identity_warnings", []))
            db_session.identity_last_check_time = session.get("identity_last_check_time", current_time)
            
            # Removal tracking
            db_session.identity_removal_count = session.get("identity_removal_count", 0)
            db_session.identity_total_warnings_issued = session.get("identity_total_warnings", 0)
            db_session.identity_current_cycle_warnings = session.get("identity_current_cycle_warnings", 0)
            db_session.behavior_removal_count = session.get("behavior_removal_count", 0)
            db_session.continuous_violation_removal_count = session.get("continuous_violation_removal_count", 0)
            
            db_session.save()
        except AttendanceSession.DoesNotExist:
            pass
        
        # Calculate continuous duration
        continuous_duration = 0
        if session.get("continuous_violation_start_time") and session.get("warning_phase_complete", False):
            continuous_duration = current_time - session["continuous_violation_start_time"]
        
        # ============================================================
        # PREPARE RESPONSE
        # ============================================================
        response_data = {
            "status": "ok",
            "popup": popup,
            "violations": violations,
            "immediate_violations": immediate_violations,
            "baseline_violations": baseline_violations,
            "attendance_percentage": percentage,
            "baseline_established": session.get("baseline_established", False),
            "face_detected": session.get("face_detected", False),
            "frame_count": session["frame_processing_count"],
            "popup_count": session["popup_count"],
            "warning_count": session.get("warning_count", 0),
            "max_warnings": 4,
            "detection_counts": session.get("detection_counts", 0),
            "warning_phase_complete": session.get("warning_phase_complete", False),
            "in_warning_phase": session["popup_count"] < 4,
            "in_detection_phase": session.get("warning_phase_complete", False),
            
            # ✅ 2-minute timer info (only active in detection phase)
            "continuous_violation_duration": continuous_duration,
            "time_until_removal": max(0, 120 - continuous_duration) if continuous_duration > 0 else 0,
            "continuous_violation_active": session.get("continuous_violation_start_time") is not None and session.get("warning_phase_complete", False),
            "removal_threshold_seconds": 120,
            
            # Identity data
            "identity_warning_count": session.get("identity_warning_count", 0),
            "identity_is_removed": session.get("identity_is_removed", False),
            
            # Removal tracking
            "identity_removal_count": session.get("identity_removal_count", 0),
            "behavior_removal_count": session.get("behavior_removal_count", 0),
            "continuous_violation_removal_count": session.get("continuous_violation_removal_count", 0),
            "total_removals": session.get("identity_removal_count", 0) + session.get("behavior_removal_count", 0),
            
            # Behavior messages
            "behavior_warnings": session.get("behavior_messages", {}).get('warnings', []),
            "behavior_detections": session.get("behavior_messages", {}).get('detections', []),
            "continuous_removals": session.get("behavior_messages", {}).get('continuous_removals', []),
            
            # Debug info
            "continuous_timer_debug": {
                "timer_active": session['violation_continuous_timer'] is not None,
                "current_violation": session['violation_current_type'],
                "duration": violation_duration,
                "popup_shown": session['violation_popup_shown'],
                "warning_phase_complete": session.get("warning_phase_complete", False),
                "two_min_timer_active": session.get("continuous_violation_start_time") is not None and session.get("warning_phase_complete", False),
                "threshold_used": "20 seconds",  # ✅ Updated to reflect new threshold
            }
        }
        
        # Add identity popup if present
        if identity_result and identity_result.get('identity_popup'):
            response_data['identity_popup'] = identity_result['identity_popup']
            response_data['identity_verified'] = identity_result.get('identity_verified', False)
        else:
            response_data['identity_popup'] = None
            response_data['identity_verified'] = identity_result.get('identity_verified', True) if identity_result else True
        
        return JsonResponse(response_data)
        
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "message": "Invalid JSON"}, status=400)
    except ValidationError as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=400)
    except Exception as e:
        logger.error(f"Error in detect_violations: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return JsonResponse({"status": "error", "message": "Internal server error"}, status=500)


# ==================== BREAK ENDPOINT ====================

@csrf_exempt
@require_http_methods(["POST"])
def take_break(request):
    """Handle break (legacy endpoint)"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get("meeting_id")
        user_id = data.get("user_id")
        
        validate_session_data(meeting_id, user_id)
        session_key = get_session_key(meeting_id, user_id)
        
        other_participants = [k for k in attendance_sessions.keys() 
                            if k.startswith(f"{meeting_id}_") and k != session_key]
        
        if session_key not in attendance_sessions:
            return JsonResponse({"status": "error", "message": "Session not active"}, status=403)

        session = attendance_sessions[session_key]
        if session["break_used"]:
            return JsonResponse({"status": "error", "message": "Break already used"}, status=400)

        session["break_used"] = True
        session["session_active"] = False
        session["attendance_penalty"] += AttendanceConfig.BREAK_PENALTY
        session["is_currently_on_break"] = True
        session["current_break_start_time"] = time.time()
        
        logger.info(f"MULTI-USER: Legacy break started for {user_id}")
        
        try:
            attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
            attendance_obj.break_used = True
            attendance_obj.session_active = False
            attendance_obj.attendance_penalty = session["attendance_penalty"]
            attendance_obj.is_currently_on_break = True
            attendance_obj.current_break_start_time = session["current_break_start_time"]
            attendance_obj.save()
        except AttendanceSession.DoesNotExist:
            pass

        def resume_after_break():
            time.sleep(AttendanceConfig.BREAK_DURATION)
            if session_key in attendance_sessions:
                session["session_active"] = True
                session["last_face_movement_time"] = time.time()
                session["popup_count"] = 0
                session["violations"] = []
                session["violation_start_times"] = {}
                session["is_currently_on_break"] = False
                session["current_break_start_time"] = None

                try:
                    attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                    current_time_local = time.time()
                    session["is_currently_on_break"] = True
                    session["current_break_start_time"] = session.get("current_break_start_time") or (current_time_local - AttendanceConfig.BREAK_DURATION)
                    update_break_time_used(session, attendance_obj, current_time_local)
                    session["is_currently_on_break"] = False
                    session["current_break_start_time"] = None
                    attendance_obj.is_currently_on_break = False
                    attendance_obj.current_break_start_time = None
                    attendance_obj.session_active = True
                    attendance_obj.save()
                except AttendanceSession.DoesNotExist:
                    pass

                verification_token = generate_camera_verification_token(meeting_id, user_id, time.time())
                verification_deadline = time.time() + AttendanceConfig.CAMERA_VERIFICATION_TIMEOUT
                session["camera_resume_expected"] = True
                session["camera_resume_deadline"] = verification_deadline
                session["camera_confirmation_token"] = verification_token

                try:
                    attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                    extended_data = get_extended_tracking_data(attendance_obj)
                    extended_data["camera_resume_expected"] = True
                    extended_data["camera_resume_deadline"] = verification_deadline
                    extended_data["camera_confirmation_token"] = verification_token
                    save_extended_tracking_data(attendance_obj, extended_data)
                except Exception as e:
                    logger.error(f"Failed to save camera resume enforcement: {e}")

                logger.info(f"Legacy break ended for {user_id}")
                
                try:
                    attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
                    attendance_obj.session_active = True
                    attendance_obj.save()
                except AttendanceSession.DoesNotExist:
                    pass

        threading.Thread(target=resume_after_break, daemon=True).start()

        return JsonResponse({
            "status": "break_used",
            "message": f"Break granted for {AttendanceConfig.BREAK_DURATION} seconds (5 minutes)",
        })
        
    except Exception as e:
        logger.error(f"Error in take_break: {e}")
        return JsonResponse({"status": "error", "message": "Internal server error"}, status=500)

# ==================== REST OF THE CODE CONTINUES AS BEFORE ====================
# (get_attendance_status, start_attendance_tracking_api, stop_attendance_tracking_api, urlpatterns)
# ... [Keep all remaining functions exactly as they were in your original code]

@csrf_exempt
@require_http_methods(["GET"])
def get_attendance_status(request):
    """
    ✅ ENHANCED: Get attendance status with continuous violation tracking
    AND removal count tracking AND behavior message history
    
    Returns comprehensive attendance data including:
    - Behavior violation status (warnings, detections, penalties)
    - Identity verification status
    - Continuous violation tracking and time until removal
    - Break time usage
    - Camera verification status
    - Removal tracking (identity_removal_count, behavior_removal_count, etc.)
    - Behavior warning/detection message history
    """
    try:
        meeting_id = request.GET.get('meeting_id')
        user_id = request.GET.get('user_id')
        
        if not meeting_id or not user_id:
            return JsonResponse({"error": "meeting_id and user_id required"}, status=400)
        
        user_id = str(user_id)
        validate_session_data(meeting_id, user_id)
        session_key = get_session_key(meeting_id, user_id)
        
        concurrent_sessions = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
        other_participants_count = len([k for k in concurrent_sessions if k != session_key])
        
        # ============================================================
        # CASE 1: ACTIVE SESSION IN MEMORY
        # ============================================================
        if session_key in attendance_sessions:
            session = attendance_sessions[session_key]
            total_time = (timezone.now() - session["start_time"]).total_seconds()
            current_time = time.time()
            
            # Calculate break time
            current_total_break_time = calculate_current_break_time(session, current_time)
            break_time_remaining = max(0, session.get('max_break_time_allowed', 300) - current_total_break_time)
            
            # Calculate continuous violation duration
            continuous_duration = 0
            continuous_active = False
            time_until_removal = 0
            
            if session.get("continuous_violation_start_time"):
                continuous_duration = current_time - session["continuous_violation_start_time"]
                continuous_active = True
                time_until_removal = max(0, AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME - continuous_duration)
            
            # ✅✅✅ NEW: Get behavior messages from session
            behavior_messages = session.get("behavior_messages", {'warnings': [], 'detections': []})
            
            return JsonResponse({
                "status": "active",
                "meeting_id": meeting_id,
                "user_id": user_id,
                
                # ==================== SESSION STATUS ====================
                "session_active": session["session_active"],
                "session_duration": int(total_time),
                "frame_count": session.get("frame_processing_count", 0),
                "real_time": True,
                
                # ==================== ATTENDANCE SCORES ====================
                "attendance_percentage": max(0, 100 - session.get("attendance_penalty", 0)),
                "engagement_score": max(0, 100 - session.get("attendance_penalty", 0)),
                "focus_score": max(0, 100 - session.get("attendance_penalty", 0)),
                "attendance_penalty": session.get("attendance_penalty", 0.0),
                
                # ==================== BEHAVIOR VIOLATIONS ====================
                "popup_count": session["popup_count"],
                "warning_count": session.get("warning_count", 0),
                "max_warnings": AttendanceConfig.MAX_WARNING_MESSAGES,
                "detection_counts": session.get("detection_counts", 0),
                "total_detections": session.get("total_detections", 0),
                "detection_penalty_applied": session.get("detection_penalty_applied", False),
                "warning_phase_complete": session.get("warning_phase_complete", False),
                "in_warning_phase": session["popup_count"] < AttendanceConfig.MAX_WARNING_MESSAGES,
                "in_detection_phase": session.get("warning_phase_complete", False),
                "violations": session.get("violations", []),
                "violation_start_times_count": len(session.get("violation_start_times", {})),
                "violation_history": list(session.get("violation_start_times", {}).keys()),
                
                # ==================== CONTINUOUS VIOLATION TRACKING ====================
                "continuous_violation_duration": continuous_duration,
                "continuous_violation_active": continuous_active,
                "time_until_removal": time_until_removal,
                "removal_threshold_seconds": AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME,
                "continuous_violation_penalty": AttendanceConfig.CONTINUOUS_2MIN_PENALTY,
                
                # ==================== REMOVAL STATUS ====================
                "is_removed_from_meeting": session.get("is_removed_from_meeting", False),
                "removal_timestamp": session.get("removal_timestamp").isoformat() if session.get("removal_timestamp") else None,
                "removal_reason": session.get("removal_reason", ""),
                "removal_type": "continuous_violations" if session.get("is_removed_from_meeting", False) else None,
                
                # ==================== IDENTITY VERIFICATION ====================
                "identity_warning_count": session.get("identity_warning_count", 0),
                "identity_max_warnings": AttendanceConfig.IDENTITY_MAX_WARNINGS,
                "identity_consecutive_unknown": session.get("identity_consecutive_unknown_seconds", 0),
                "identity_total_unknown": session.get("identity_total_unknown_seconds", 0),
                "identity_is_removed": session.get("identity_is_removed", False),
                "identity_can_rejoin": session.get("identity_can_rejoin", True),
                
                # ==================== REMOVAL TRACKING ====================
                "identity_removal_count": session.get("identity_removal_count", 0),
                "identity_total_warnings_issued": session.get("identity_total_warnings", 0),
                "identity_current_cycle_warnings": session.get("identity_current_cycle_warnings", 0),
                "behavior_removal_count": session.get("behavior_removal_count", 0),
                "total_removals": session.get("identity_removal_count", 0) + session.get("behavior_removal_count", 0),
                
                # ==================== ✅✅✅ NEW: BEHAVIOR MESSAGES ====================
                "behavior_warnings": behavior_messages.get('warnings', []),
                "behavior_detections": behavior_messages.get('detections', []),
                "behavior_warning_count": len(behavior_messages.get('warnings', [])),
                "behavior_detection_count": len(behavior_messages.get('detections', [])),
                
                # ==================== BREAK SYSTEM ====================
                "break_used": session.get("break_used", False),
                "is_on_break": session.get("is_currently_on_break", False),
                "total_break_time_used": current_total_break_time,
                "break_time_remaining": break_time_remaining,
                "break_count": session.get("break_count", 0),
                "max_break_time_allowed": session.get('max_break_time_allowed', 300),
                "can_take_break": break_time_remaining > 0,
                
                # ==================== CAMERA VERIFICATION ====================
                "camera_verification_pending": session.get("camera_resume_expected", False),
                "camera_verification_deadline": session.get("camera_resume_deadline"),
                "camera_verified": session.get("camera_verified_at") is not None,
                "camera_verified_at": session.get("camera_verified_at"),
                "camera_should_resume": bool(session.get("camera_resume_expected", False) and session.get("session_active", False)),
                "camera_confirmation_token": session.get("camera_confirmation_token"),
                
                # ==================== BASELINE & DETECTION ====================
                "baseline_established": session.get("baseline_established", False),
                "baseline_samples": session.get("baseline_samples", 0),
                "face_detected": session.get("face_detected", False),
                
                # ==================== GRACE PERIOD ====================
                "grace_period_active": session.get("grace_period_active", False),
                "grace_period_until": session.get("grace_period_until"),
                
                # ==================== MULTI-USER INFO ====================
                "user_isolation_verified": True,
                "concurrent_participants": len(concurrent_sessions),
                "other_participants_count": other_participants_count,
            })
        
        # ============================================================
        # CASE 2: SESSION IN DATABASE (NOT IN MEMORY)
        # ============================================================
        try:
            attendance_obj = AttendanceSession.objects.get(meeting_id=meeting_id, user_id=user_id)
            break_time_remaining = max(0, (attendance_obj.max_break_time_allowed or 300) - (attendance_obj.total_break_time_used or 0))
            extended_data = get_extended_tracking_data(attendance_obj)
            
            # Calculate continuous violation duration from extended data
            continuous_duration = 0
            continuous_active = False
            time_until_removal = 0
            
            if extended_data.get('continuous_violation_start_time'):
                current_time = time.time()
                continuous_duration = current_time - extended_data.get('continuous_violation_start_time')
                continuous_active = True
                time_until_removal = max(0, AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME - continuous_duration)
            
            # Load violation start times
            try:
                violation_start_times = json.loads(attendance_obj.violation_start_times) if attendance_obj.violation_start_times else {}
            except json.JSONDecodeError:
                violation_start_times = {}
            
            # ✅✅✅ NEW: Get behavior messages from database
            behavior_messages = attendance_obj.get_behavior_messages()
            
            return JsonResponse({
                "status": "found",
                "meeting_id": meeting_id,
                "user_id": user_id,
                
                # ==================== SESSION STATUS ====================
                "session_active": attendance_obj.session_active,
                "session_duration": attendance_obj.total_session_time,
                "frame_count": attendance_obj.frame_processing_count,
                "real_time": False,
                
                # ==================== ATTENDANCE SCORES ====================
                "attendance_percentage": float(attendance_obj.attendance_percentage),
                "engagement_score": float(attendance_obj.engagement_score),
                "focus_score": float(attendance_obj.focus_score),
                "attendance_penalty": float(attendance_obj.attendance_penalty),
                
                # ==================== BEHAVIOR VIOLATIONS ====================
                "popup_count": attendance_obj.popup_count,
                "warning_count": extended_data.get('warning_count', 0),
                "max_warnings": AttendanceConfig.MAX_WARNING_MESSAGES,
                "detection_counts": extended_data.get('detection_counts', 0),
                "total_detections": attendance_obj.total_detections,
                "detection_penalty_applied": extended_data.get('detection_penalty_applied', False),
                "warning_phase_complete": extended_data.get('warning_phase_complete', False),
                "in_warning_phase": attendance_obj.popup_count < AttendanceConfig.MAX_WARNING_MESSAGES,
                "in_detection_phase": extended_data.get('warning_phase_complete', False),
                "violations": attendance_obj.get_violation_list(),
                "violation_start_times_count": len(violation_start_times),
                "violation_history": list(violation_start_times.keys()),
                
                # ==================== CONTINUOUS VIOLATION TRACKING ====================
                "continuous_violation_duration": continuous_duration,
                "continuous_violation_active": continuous_active,
                "time_until_removal": time_until_removal,
                "removal_threshold_seconds": AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME,
                "continuous_violation_penalty": AttendanceConfig.CONTINUOUS_2MIN_PENALTY,
                
                # ==================== REMOVAL STATUS ====================
                "is_removed_from_meeting": extended_data.get('is_removed_from_meeting', False),
                "removal_timestamp": extended_data.get('removal_timestamp'),
                "removal_reason": extended_data.get('removal_reason', ''),
                "removal_type": "continuous_violations" if extended_data.get('is_removed_from_meeting', False) else None,
                
                # ==================== IDENTITY VERIFICATION ====================
                "identity_warning_count": attendance_obj.identity_warning_count,
                "identity_max_warnings": AttendanceConfig.IDENTITY_MAX_WARNINGS,
                "identity_consecutive_unknown": attendance_obj.identity_consecutive_unknown_seconds,
                "identity_total_unknown": attendance_obj.identity_total_unknown_seconds,
                "identity_is_removed": attendance_obj.identity_is_removed,
                "identity_can_rejoin": attendance_obj.identity_can_rejoin,
                "identity_removal_time": attendance_obj.identity_removal_time.isoformat() if attendance_obj.identity_removal_time else None,
                
                # ==================== REMOVAL TRACKING ====================
                "identity_removal_count": attendance_obj.identity_removal_count,
                "identity_total_warnings_issued": attendance_obj.identity_total_warnings_issued,
                "identity_current_cycle_warnings": attendance_obj.identity_current_cycle_warnings,
                "behavior_removal_count": attendance_obj.behavior_removal_count,
                "total_removals": attendance_obj.identity_removal_count + attendance_obj.behavior_removal_count,
                
                # ==================== ✅✅✅ NEW: BEHAVIOR MESSAGES ====================
                "behavior_warnings": behavior_messages.get('warnings', []),
                "behavior_detections": behavior_messages.get('detections', []),
                "behavior_warning_count": len(behavior_messages.get('warnings', [])),
                "behavior_detection_count": len(behavior_messages.get('detections', [])),
                
                # ==================== BREAK SYSTEM ====================
                "break_used": attendance_obj.break_used,
                "is_on_break": attendance_obj.is_currently_on_break,
                "total_break_time_used": attendance_obj.total_break_time_used or 0,
                "break_time_remaining": break_time_remaining,
                "break_count": attendance_obj.break_count or 0,
                "max_break_time_allowed": attendance_obj.max_break_time_allowed or 300,
                "can_take_break": break_time_remaining > 0,
                
                # ==================== CAMERA VERIFICATION ====================
                "camera_verification_pending": extended_data.get('camera_resume_expected', False),
                "camera_verification_deadline": extended_data.get('camera_resume_deadline'),
                "camera_verified": extended_data.get('camera_verified_at') is not None,
                "camera_verified_at": extended_data.get('camera_verified_at'),
                "camera_should_resume": bool(extended_data.get('camera_resume_expected', False) and attendance_obj.session_active),
                "camera_confirmation_token": extended_data.get('camera_confirmation_token'),
                
                # ==================== BASELINE & DETECTION ====================
                "baseline_established": True,  # Assume established if in DB
                "baseline_samples": AttendanceConfig.BASELINE_FRAMES_REQUIRED,
                "face_detected": False,  # Not available from DB
                
                # ==================== GRACE PERIOD ====================
                "grace_period_active": extended_data.get('grace_period_active', False),
                "grace_period_until": extended_data.get('grace_period_until'),
                
                # ==================== MULTI-USER INFO ====================
                "user_isolation_verified": True,
                "concurrent_participants": len(concurrent_sessions),
                "other_participants_count": other_participants_count,
            })
            
        except AttendanceSession.DoesNotExist:
            # ============================================================
            # CASE 3: NO SESSION EXISTS
            # ============================================================
            return JsonResponse({
                "status": "not_started",
                "message": "Attendance tracking not started",
                "meeting_id": meeting_id,
                "user_id": user_id,
                
                # ==================== SESSION STATUS ====================
                "session_active": False,
                "session_duration": 0,
                "frame_count": 0,
                "real_time": False,
                
                # ==================== ATTENDANCE SCORES ====================
                "attendance_percentage": 100,
                "engagement_score": 100,
                "focus_score": 100,
                "attendance_penalty": 0.0,
                
                # ==================== BEHAVIOR VIOLATIONS ====================
                "popup_count": 0,
                "warning_count": 0,
                "max_warnings": AttendanceConfig.MAX_WARNING_MESSAGES,
                "detection_counts": 0,
                "total_detections": 0,
                "detection_penalty_applied": False,
                "warning_phase_complete": False,
                "in_warning_phase": True,
                "in_detection_phase": False,
                "violations": [],
                "violation_start_times_count": 0,
                "violation_history": [],
                
                # ==================== CONTINUOUS VIOLATION TRACKING ====================
                "continuous_violation_duration": 0,
                "continuous_violation_active": False,
                "time_until_removal": AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME,
                "removal_threshold_seconds": AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME,
                "continuous_violation_penalty": AttendanceConfig.CONTINUOUS_2MIN_PENALTY,
                
                # ==================== REMOVAL STATUS ====================
                "is_removed_from_meeting": False,
                "removal_timestamp": None,
                "removal_reason": "",
                "removal_type": None,
                
                # ==================== IDENTITY VERIFICATION ====================
                "identity_warning_count": 0,
                "identity_max_warnings": AttendanceConfig.IDENTITY_MAX_WARNINGS,
                "identity_consecutive_unknown": 0,
                "identity_total_unknown": 0,
                "identity_is_removed": False,
                "identity_can_rejoin": True,
                "identity_removal_time": None,
                
                # ==================== REMOVAL TRACKING ====================
                "identity_removal_count": 0,
                "identity_total_warnings_issued": 0,
                "identity_current_cycle_warnings": 0,
                "behavior_removal_count": 0,
                "total_removals": 0,
                
                # ==================== ✅✅✅ NEW: BEHAVIOR MESSAGES ====================
                "behavior_warnings": [],
                "behavior_detections": [],
                "behavior_warning_count": 0,
                "behavior_detection_count": 0,
                
                # ==================== BREAK SYSTEM ====================
                "break_used": False,
                "is_on_break": False,
                "total_break_time_used": 0,
                "break_time_remaining": 300,
                "break_count": 0,
                "max_break_time_allowed": 300,
                "can_take_break": True,
                
                # ==================== CAMERA VERIFICATION ====================
                "camera_verification_pending": False,
                "camera_verification_deadline": None,
                "camera_verified": False,
                "camera_verified_at": None,
                "camera_should_resume": False,
                "camera_confirmation_token": None,
                
                # ==================== BASELINE & DETECTION ====================
                "baseline_established": False,
                "baseline_samples": 0,
                "face_detected": False,
                
                # ==================== GRACE PERIOD ====================
                "grace_period_active": False,
                "grace_period_until": None,
                
                # ==================== MULTI-USER INFO ====================
                "user_isolation_verified": True,
                "concurrent_participants": len(concurrent_sessions),
                "other_participants_count": other_participants_count,
            })
        
    except ValidationError as e:
        logger.error(f"Validation error in get_attendance_status: {e}")
        return JsonResponse({"error": str(e)}, status=400)
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({"error": "Internal server error"}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def start_attendance_tracking_api(request):
    """Start tracking API"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        user_name = data.get('user_name', f'User_{user_id}')
        
        if not meeting_id or not user_id:
            return JsonResponse({'success': False, 'error': 'meeting_id and user_id required'}, status=400)
        
        user_id_str = str(user_id)
        validate_session_data(meeting_id, user_id_str)
        
        concurrent_sessions = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
        success = start_attendance_tracking(meeting_id, user_id_str, user_name)
        
        if success:
            final_concurrent_sessions = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
            
            return JsonResponse({
                'success': True,
                'status': 'started',
                'message': 'Independent attendance tracking started - 5 minute break available',
                'meeting_id': meeting_id,
                'user_id': user_id_str,
                'user_name': user_name,
                'timestamp': timezone.now().isoformat(),
                'max_break_time_allowed': AttendanceConfig.MAX_TOTAL_BREAK_TIME,
                'max_warnings': AttendanceConfig.MAX_WARNING_MESSAGES,
                'grace_period_duration': AttendanceConfig.GRACE_PERIOD_DURATION,
                'detection_interval_seconds': AttendanceConfig.DETECTION_INTERVAL,
                'auto_removal_time_seconds': AttendanceConfig.VIOLATION_AUTO_REMOVAL_TIME,
                'camera_verification_timeout_seconds': AttendanceConfig.CAMERA_VERIFICATION_TIMEOUT,
                'user_isolation_verified': True,
                'concurrent_participants_before': len(concurrent_sessions),
                'concurrent_participants_after': len(final_concurrent_sessions),
            })
        else:
            return JsonResponse({'success': False, 'error': 'Failed to start tracking'}, status=500)
            
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except ValidationError as e:
        return JsonResponse({'error': str(e)}, status=400)
    except Exception as e:
        logger.error(f"Error starting tracking: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def stop_attendance_tracking_api(request):
    """Stop tracking API with SAFE GPU cleanup"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        
        if not meeting_id or not user_id:
            return JsonResponse({'success': False, 'error': 'meeting_id and user_id required'}, status=400)
        
        user_id_str = str(user_id)
        validate_session_data(meeting_id, user_id_str)
        
        concurrent_sessions_before = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
        session_key = get_session_key(meeting_id, user_id_str)
        other_participants_before = [k for k in concurrent_sessions_before if k != session_key]
        
        # ✅ FIRST: Stop the attendance session (stops frame processing)
        success = stop_attendance_tracking(meeting_id, user_id_str)
        
        concurrent_sessions_after = [k for k in attendance_sessions.keys() if k.startswith(f"{meeting_id}_")]
        
        is_last_participant = len(concurrent_sessions_after) == 0
        gpu_released = False
        
        if success and is_last_participant:
            # ✅ CRITICAL: Wait for all CUDA operations to complete
            logger.info(f"⏳ Last participant left. Waiting for face detection to fully stop...")
            
            # Give time for any in-flight frame processing to complete
            import time
            time.sleep(2)  # Wait 2 seconds for frame processing to finish
            
            # ✅ Force synchronization with CUDA
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.synchronize()  # Wait for all CUDA operations
                    logger.info("✅ CUDA synchronized")
            except Exception as e:
                logger.warning(f"⚠️ CUDA sync failed: {e}")
            
            # ✅ Additional safety: Wait another second
            time.sleep(1)
            
            # ✅ NOW safe to unload face model
            try:
                logger.info(f"🔄 Attempting to release face model GPU memory...")
                
                import sys
                import os
                
                current_dir = os.path.dirname(os.path.abspath(__file__))
                faceauth_dir = os.path.join(current_dir, '..', 'FaceAuth')
                faceauth_dir = os.path.abspath(faceauth_dir)
                
                if faceauth_dir not in sys.path:
                    sys.path.insert(0, faceauth_dir)
                
                from face_model_shared import unload_face_model
                unload_face_model()
                gpu_released = True
                logger.info(f"✅ Face model GPU memory released safely for meeting {meeting_id}")
                
            except ImportError as ie:
                logger.warning(f"⚠️ Could not import face_model_shared: {ie}")
                
                # Fallback: Manual GPU cleanup
                try:
                    import torch
                    if torch.cuda.is_available():
                        torch.cuda.synchronize()  # Sync again before cleanup
                        torch.cuda.empty_cache()
                        import gc
                        gc.collect()
                        gpu_released = True
                        logger.info("✅ Manual GPU cache cleanup completed")
                except ImportError:
                    logger.warning("⚠️ PyTorch not available for GPU cleanup")
                except Exception as e:
                    logger.warning(f"⚠️ Manual GPU cleanup failed: {e}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not release GPU memory: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            if not is_last_participant:
                logger.info(f"ℹ️ Meeting {meeting_id} still has {len(concurrent_sessions_after)} active participants. GPU memory retained.")
        
        return JsonResponse({
            'success': True,
            'status': 'stopped',
            'message': 'Tracking stopped for user only',
            'meeting_id': meeting_id,
            'user_id': user_id_str,
            'timestamp': timezone.now().isoformat(),
            'user_isolation_verified': True,
            'concurrent_participants_before': len(concurrent_sessions_before),
            'concurrent_participants_after': len(concurrent_sessions_after),
            'other_participants_unaffected': len(other_participants_before),
            'is_last_participant': is_last_participant,
            'gpu_memory_released': gpu_released,
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except ValidationError as e:
        return JsonResponse({'error': str(e)}, status=400)
    except Exception as e:
        logger.error(f"Error stopping tracking: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return JsonResponse({'error': 'Internal server error'}, status=500)

# ==================== URL PATTERNS ====================

urlpatterns = [
    path('api/attendance/start/', start_attendance_tracking_api, name='attendance_start'),
    path('api/attendance/stop/', stop_attendance_tracking_api, name='attendance_stop'),
    path('api/attendance/detect/', detect_violations, name='attendance_detect_violations'),
    path('api/attendance/break/', take_break, name='attendance_take_break'),
    path('api/attendance/status/', get_attendance_status, name='attendance_get_status'),
    path('api/attendance/pause-resume/', pause_resume_attendance, name='attendance_pause_resume'),
    path('api/attendance/verify-camera/', verify_camera_resumed, name='attendance_verify_camera'),
]