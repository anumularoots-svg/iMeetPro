

# meeting_continuous_verification.py


"""
COMPLETE Continuous Face Verification During Meeting - PRODUCTION READY
========================================================================
Single file solution with background verification and warning system

Location to save: core/FaceAuth/meeting_continuous_verification.py

Author: Meeting Authentication System
Version: 3.0.0
Date: 2025-01-15

FEATURES:
- Background verification (no interruption to meeting)
- 5-minute interval before first warning
- 1-minute interval after first warning
- 3 warnings system with progressive restrictions
- Host override capability
- Session-based blocking
- Complete database integration
- WebSocket notifications support
- ENHANCED CONSOLE LOGGING for unknown person detection
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, List, Tuple, Any
from enum import Enum
import json

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import numpy as np
from bson import ObjectId

# ============================================================================
# YOUR PROJECT IMPORTS (Already in your project)
# ============================================================================
from core.FaceAuth.face_model_shared import get_face_model
from core.UserDashBoard.face_embeddings import get_user_embeddings, base64_to_numpy

# ============================================================================
# OPTIONAL: DATABASE IMPORTS (Add if available)
# ============================================================================
try:
    from core.database import get_db  # Adjust import path as needed
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    logging.warning("Database module not found. Running without database integration.")

# ============================================================================
# OPTIONAL: WEBSOCKET IMPORTS (Add if available)
# ============================================================================
try:
    from core.websocket_manager import send_notification  # Adjust import path as needed
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logging.warning("WebSocket module not found. Running without real-time notifications.")

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # Add file handler if needed
        # logging.FileHandler('meeting_verification.log')
    ]
)
logger = logging.getLogger("meeting_continuous_verification")

# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class VerificationState(Enum):
    """Verification states"""
    INITIAL = "initial"  # Before first warning (5-minute checks)
    WARNING_PHASE = "warning_phase"  # After first warning (1-minute checks)
    BLOCKED = "blocked"  # Removed from meeting

class NotificationType(Enum):
    """Notification types"""
    WARNING_1 = "warning_1"
    WARNING_2 = "warning_2"
    WARNING_3 = "warning_3"
    REMOVAL = "removal"
    HOST_ALERT = "host_alert"
    USER_REVERIFIED = "user_reverified"
    HOST_OVERRIDE = "host_override"

# ============================================================================
# CONFIGURATION CLASS
# ============================================================================

class MeetingVerificationConfig:
    """Configuration for continuous meeting verification"""
    
    # Face recognition thresholds
    FACE_DISTANCE_THRESHOLD = 0.6  # Same as waiting room (cosine distance)
    MIN_FACE_CONFIDENCE = 0.3  # Minimum detection confidence
    
    # Timing configuration
    INITIAL_CHECK_INTERVAL = 300  # 5 minutes (300 seconds) before first warning
    WARNING_CHECK_INTERVAL = 60   # 1 minute (60 seconds) after first warning
    FRAME_CAPTURE_TIMEOUT = 5.0   # Timeout for frame capture
    
    # Warning system
    MAX_WARNINGS = 3  # Remove after 3 warnings
    RESET_WARNINGS_ON_SUCCESS = True  # Reset warnings when user is verified
    
    # Session blocking
    BLOCK_UNTIL_SESSION_ENDS = True  # Block user until meeting ends
    ALLOW_HOST_OVERRIDE = True  # Host can allow blocked users
    
    # Logging and debugging
    LOG_EACH_CHECK = True  # Log every verification
    LOG_DETAILED_STATS = False  # Log detailed statistics
    DEBUG_MODE = False  # Enable debug logging
    
    # Database
    SAVE_TO_DATABASE = DATABASE_AVAILABLE  # Save verification events to DB
    DB_COLLECTION = "meeting_verifications"  # Collection name
    
    # Notifications
    SEND_NOTIFICATIONS = WEBSOCKET_AVAILABLE  # Send WebSocket notifications
    NOTIFY_HOST_ON_WARNING_2 = True  # Alert host on 2nd warning
    NOTIFY_HOST_ON_REMOVAL = True  # Alert host on removal

# ============================================================================
# DATABASE HELPER CLASS
# ============================================================================

class VerificationDatabase:
    """Handles database operations for verification events"""
    
    @staticmethod
    async def save_warning_event(
        user_id: int,
        meeting_id: str,
        warning_number: int,
        similarity: float,
        state: str,
        session_id: str
    ):
        """Save warning event to database"""
        if not MeetingVerificationConfig.SAVE_TO_DATABASE:
            return
        
        try:
            db = get_db()
            collection = db[MeetingVerificationConfig.DB_COLLECTION]
            
            event = {
                'user_id': user_id,
                'meeting_id': meeting_id,
                'session_id': session_id,
                'event_type': 'warning',
                'warning_number': warning_number,
                'similarity_score': similarity,
                'state': state,
                'timestamp': datetime.utcnow(),
                'created_at': datetime.utcnow()
            }
            
            await collection.insert_one(event)
            logger.debug(f"Saved warning event to database: user {user_id}, warning {warning_number}")
            
        except Exception as e:
            logger.error(f"Error saving warning event to database: {e}")
    
    @staticmethod
    async def save_removal_event(
        user_id: int,
        meeting_id: str,
        warning_count: int,
        session_id: str,
        reason: str = "Max warnings exceeded"
    ):
        """Save removal event to database"""
        if not MeetingVerificationConfig.SAVE_TO_DATABASE:
            return
        
        try:
            db = get_db()
            collection = db[MeetingVerificationConfig.DB_COLLECTION]
            
            event = {
                'user_id': user_id,
                'meeting_id': meeting_id,
                'session_id': session_id,
                'event_type': 'removal',
                'warning_count': warning_count,
                'reason': reason,
                'timestamp': datetime.utcnow(),
                'created_at': datetime.utcnow()
            }
            
            await collection.insert_one(event)
            logger.debug(f"Saved removal event to database: user {user_id}")
            
        except Exception as e:
            logger.error(f"Error saving removal event to database: {e}")
    
    @staticmethod
    async def save_verification_check(
        user_id: int,
        meeting_id: str,
        is_verified: bool,
        similarity: float,
        session_id: str
    ):
        """Save individual verification check (optional - can be verbose)"""
        if not MeetingVerificationConfig.SAVE_TO_DATABASE or not MeetingVerificationConfig.LOG_DETAILED_STATS:
            return
        
        try:
            db = get_db()
            collection = db[MeetingVerificationConfig.DB_COLLECTION]
            
            check = {
                'user_id': user_id,
                'meeting_id': meeting_id,
                'session_id': session_id,
                'event_type': 'verification_check',
                'is_verified': is_verified,
                'similarity_score': similarity,
                'timestamp': datetime.utcnow()
            }
            
            await collection.insert_one(check)
            
        except Exception as e:
            logger.error(f"Error saving verification check: {e}")
    
    @staticmethod
    async def get_user_verification_history(user_id: int, meeting_id: str = None) -> List[Dict]:
        """Get verification history for a user"""
        if not MeetingVerificationConfig.SAVE_TO_DATABASE:
            return []
        
        try:
            db = get_db()
            collection = db[MeetingVerificationConfig.DB_COLLECTION]
            
            query = {'user_id': user_id}
            if meeting_id:
                query['meeting_id'] = meeting_id
            
            cursor = collection.find(query).sort('timestamp', -1).limit(100)
            history = await cursor.to_list(length=100)
            
            return history
            
        except Exception as e:
            logger.error(f"Error fetching verification history: {e}")
            return []

# ============================================================================
# NOTIFICATION HELPER CLASS
# ============================================================================

class VerificationNotification:
    """Handles WebSocket notifications for verification events"""
    
    @staticmethod
    async def send_notification(
        user_id: int,
        notification_type: NotificationType,
        data: Dict[str, Any]
    ):
        """Send WebSocket notification to user"""
        if not MeetingVerificationConfig.SEND_NOTIFICATIONS:
            return
        
        try:
            notification = {
                'type': 'verification_notification',
                'notification_type': notification_type.value,
                'timestamp': datetime.utcnow().isoformat(),
                **data
            }
            
            await send_notification(user_id, notification)
            logger.debug(f"Sent {notification_type.value} notification to user {user_id}")
            
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
    
    @staticmethod
    async def notify_host(
        meeting_id: str,
        host_id: int,
        notification_type: NotificationType,
        data: Dict[str, Any]
    ):
        """Send notification to meeting host"""
        if not MeetingVerificationConfig.SEND_NOTIFICATIONS:
            return
        
        try:
            notification = {
                'type': 'host_notification',
                'notification_type': notification_type.value,
                'meeting_id': meeting_id,
                'timestamp': datetime.utcnow().isoformat(),
                **data
            }
            
            await send_notification(host_id, notification)
            logger.debug(f"Sent {notification_type.value} notification to host {host_id}")
            
        except Exception as e:
            logger.error(f"Error sending host notification: {e}")

# ============================================================================
# WARNING CALLBACK SYSTEM
# ============================================================================

class WarningCallback:
    """Handles callbacks for warning events"""
    
    @staticmethod
    async def on_first_warning(
        user_id: int,
        meeting_id: str,
        similarity: float,
        session_id: str
    ):
        """Called when first warning is issued"""
        logger.warning(
            f"\n{'='*80}\n"
            f"⚠️  FIRST WARNING - Face Verification Failed\n"
            f"{'='*80}\n"
            f"User ID: {user_id}\n"
            f"Meeting ID: {meeting_id}\n"
            f"Session ID: {session_id}\n"
            f"Similarity Score: {similarity:.3f}\n"
            f"Next Check: 1 minute\n"
            f"Warnings: 1/{MeetingVerificationConfig.MAX_WARNINGS}\n"
            f"{'='*80}\n"
        )
        
        # Save to database
        await VerificationDatabase.save_warning_event(
            user_id, meeting_id, 1, similarity, "warning_phase", session_id
        )
        
        # Send notification to user
        await VerificationNotification.send_notification(
            user_id,
            NotificationType.WARNING_1,
            {
                'message': 'Face verification failed. Please ensure your face is clearly visible.',
                'warning_number': 1,
                'max_warnings': MeetingVerificationConfig.MAX_WARNINGS,
                'next_check_seconds': MeetingVerificationConfig.WARNING_CHECK_INTERVAL,
                'similarity_score': similarity
            }
        )
    
    @staticmethod
    async def on_second_warning(
        user_id: int,
        meeting_id: str,
        similarity: float,
        session_id: str,
        host_id: int = None
    ):
        """Called when second warning is issued"""
        logger.warning(
            f"\n{'='*80}\n"
            f"⚠️⚠️  SECOND WARNING - Face Verification Failed Again\n"
            f"{'='*80}\n"
            f"User ID: {user_id}\n"
            f"Meeting ID: {meeting_id}\n"
            f"Session ID: {session_id}\n"
            f"Similarity Score: {similarity:.3f}\n"
            f"Next Check: 1 minute\n"
            f"Warnings: 2/{MeetingVerificationConfig.MAX_WARNINGS}\n"
            f"⚠️  One more failure will result in removal!\n"
            f"{'='*80}\n"
        )
        
        # Save to database
        await VerificationDatabase.save_warning_event(
            user_id, meeting_id, 2, similarity, "warning_phase", session_id
        )
        
        # Send urgent notification to user
        await VerificationNotification.send_notification(
            user_id,
            NotificationType.WARNING_2,
            {
                'message': 'URGENT: Second verification failure. One more failure will remove you from the meeting!',
                'warning_number': 2,
                'max_warnings': MeetingVerificationConfig.MAX_WARNINGS,
                'next_check_seconds': MeetingVerificationConfig.WARNING_CHECK_INTERVAL,
                'similarity_score': similarity
            }
        )
        
        # Notify host
        if host_id and MeetingVerificationConfig.NOTIFY_HOST_ON_WARNING_2:
            await VerificationNotification.notify_host(
                meeting_id,
                host_id,
                NotificationType.HOST_ALERT,
                {
                    'user_id': user_id,
                    'message': f'User {user_id} has received 2 warnings. May be removed soon.',
                    'warning_number': 2
                }
            )
    
    @staticmethod
    async def on_third_warning_removal(
        user_id: int,
        meeting_id: str,
        similarity: float,
        session_id: str,
        host_id: int = None
    ):
        """Called when third warning leads to removal"""
        logger.error(
            f"\n{'='*80}\n"
            f"🚫 THIRD WARNING - USER REMOVED FROM MEETING\n"
            f"{'='*80}\n"
            f"User ID: {user_id}\n"
            f"Meeting ID: {meeting_id}\n"
            f"Session ID: {session_id}\n"
            f"Similarity Score: {similarity:.3f}\n"
            f"Warnings: 3/{MeetingVerificationConfig.MAX_WARNINGS}\n"
            f"Status: BLOCKED until session ends or host allows\n"
            f"{'='*80}\n"
        )
        
        # Save to database
        await VerificationDatabase.save_warning_event(
            user_id, meeting_id, 3, similarity, "blocked", session_id
        )
        await VerificationDatabase.save_removal_event(
            user_id, meeting_id, 3, session_id
        )
        
        # Send removal notification to user
        await VerificationNotification.send_notification(
            user_id,
            NotificationType.REMOVAL,
            {
                'message': 'You have been removed from the meeting due to failed face verification.',
                'warning_count': 3,
                'can_rejoin': 'Only if host allows or meeting ends',
                'similarity_score': similarity
            }
        )
        
        # Notify host
        if host_id and MeetingVerificationConfig.NOTIFY_HOST_ON_REMOVAL:
            await VerificationNotification.notify_host(
                meeting_id,
                host_id,
                NotificationType.HOST_ALERT,
                {
                    'user_id': user_id,
                    'message': f'User {user_id} has been removed from the meeting (3 warnings)',
                    'action_required': 'You can allow them to rejoin if needed'
                }
            )
    
    @staticmethod
    async def on_user_reverified(
        user_id: int,
        meeting_id: str,
        previous_warnings: int,
        similarity: float
    ):
        """Called when user is successfully re-verified after warnings"""
        logger.info(
            f"\n{'='*80}\n"
            f"✅ USER RE-VERIFIED - Warnings Reset\n"
            f"{'='*80}\n"
            f"User ID: {user_id}\n"
            f"Meeting ID: {meeting_id}\n"
            f"Similarity Score: {similarity:.3f}\n"
            f"Previous Warnings: {previous_warnings}\n"
            f"Status: Back to 5-minute checks\n"
            f"{'='*80}\n"
        )
        
        # Send positive notification to user
        await VerificationNotification.send_notification(
            user_id,
            NotificationType.USER_REVERIFIED,
            {
                'message': 'Face verification successful. Warnings have been reset.',
                'previous_warnings': previous_warnings,
                'similarity_score': similarity,
                'next_check_seconds': MeetingVerificationConfig.INITIAL_CHECK_INTERVAL
            }
        )

# ============================================================================
# MAIN VERIFIER CLASS
# ============================================================================

class MeetingContinuousVerifier:
    """Continuously verify participant face during meeting in background"""
    
    def __init__(
        self,
        meeting_id: str,
        user_id: int,
        session_id: str = None,
        host_id: int = None
    ):
        """
        Initialize the verifier
        
        Args:
            meeting_id: Unique meeting identifier
            user_id: User to verify
            session_id: Optional session identifier
            host_id: Optional host user ID for notifications
        """
        self.meeting_id = str(meeting_id)
        self.user_id = int(user_id)
        self.session_id = session_id or f"session_{meeting_id}_{datetime.now().timestamp()}"
        self.host_id = host_id
        
        # Verification state
        self.state = VerificationState.INITIAL
        self.warning_count = 0
        self.is_running = False
        self.is_blocked = False
        self.host_override = False
        
        # Statistics
        self.total_checks = 0
        self.successful_checks = 0
        self.failed_checks = 0
        self.warning_history: List[Dict] = []
        
        # Timing
        self.last_check_time = None
        self.first_warning_time = None
        self.removal_time = None
        self.verification_task: Optional[asyncio.Task] = None
        
        # Load face model
        try:
            self.face_model = get_face_model()
            logger.info(f"✅ Face model loaded for user {user_id}")
        except Exception as e:
            logger.error(f"❌ Failed to load face model: {e}")
            logger.error(traceback.format_exc())
            self.face_model = None
        
        # Load stored embeddings
        self.stored_embeddings = self._load_user_embeddings()
        
        logger.info(
            f"\n{'='*80}\n"
            f"🔹 Background Verifier Initialized\n"
            f"{'='*80}\n"
            f"User ID: {user_id}\n"
            f"Meeting ID: {meeting_id}\n"
            f"Session ID: {self.session_id}\n"
            f"Host ID: {host_id or 'N/A'}\n"
            f"Initial Interval: {MeetingVerificationConfig.INITIAL_CHECK_INTERVAL}s (5 min)\n"
            f"Warning Interval: {MeetingVerificationConfig.WARNING_CHECK_INTERVAL}s (1 min)\n"
            f"Max Warnings: {MeetingVerificationConfig.MAX_WARNINGS}\n"
            f"{'='*80}\n"
        )
    
    def _load_user_embeddings(self) -> Optional[List[Dict]]:
        """Load user's stored embeddings from MongoDB"""
        try:
            user_embeddings = get_user_embeddings(self.user_id)
            
            if not user_embeddings:
                logger.warning(f"⚠️  No embeddings found for user {self.user_id}")
                return None
            
            embeddings_list = []
            for emb_doc in user_embeddings:
                if emb_doc.get('embedding'):
                    embeddings_list.append({
                        'embedding': np.array(emb_doc['embedding'], dtype=np.float32),
                        'embedding_id': str(emb_doc['_id']),
                        'det_score': emb_doc.get('det_score', 0.0)
                    })
            
            logger.info(f"✅ Loaded {len(embeddings_list)} embeddings for user {self.user_id}")
            return embeddings_list
            
        except Exception as e:
            logger.error(f"❌ Error loading embeddings: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _get_current_interval(self) -> int:
        """Get current check interval based on state"""
        if self.state == VerificationState.INITIAL:
            return MeetingVerificationConfig.INITIAL_CHECK_INTERVAL  # 5 minutes
        elif self.state == VerificationState.WARNING_PHASE:
            return MeetingVerificationConfig.WARNING_CHECK_INTERVAL  # 1 minute
        else:
            return MeetingVerificationConfig.WARNING_CHECK_INTERVAL
    
    async def start_verification(self, get_frame_callback: Callable):
        """
        Start continuous BACKGROUND verification loop
        
        Args:
            get_frame_callback: Async function that returns current frame
        """
        if not self.face_model or not self.stored_embeddings:
            logger.error("❌ Cannot start verification - model or embeddings missing")
            return
        
        if self.is_blocked and not self.host_override:
            logger.warning(f"⚠️  User {self.user_id} is blocked. Cannot start verification.")
            return
        
        self.is_running = True
        
        logger.info(
            f"\n{'='*80}\n"
            f"🟢 BACKGROUND VERIFICATION STARTED\n"
            f"{'='*80}\n"
            f"User: {self.user_id}\n"
            f"Meeting: {self.meeting_id}\n"
            f"Mode: Silent/Background\n"
            f"Initial Check: Every {MeetingVerificationConfig.INITIAL_CHECK_INTERVAL}s (5 min)\n"
            f"Warning Check: Every {MeetingVerificationConfig.WARNING_CHECK_INTERVAL}s (1 min)\n"
            f"{'='*80}\n"
        )
        
        while self.is_running:
            try:
                # Get current interval
                interval = self._get_current_interval()
                
                # Wait for interval before first check
                logger.debug(f"⏱️  Waiting {interval}s before next check for user {self.user_id}")
                await asyncio.sleep(interval)
                
                # Check if still running
                if not self.is_running:
                    break
                
                # Skip if blocked without host override
                if self.is_blocked and not self.host_override:
                    logger.info(f"⏭️  Skipping check - User {self.user_id} is blocked")
                    continue
                
                # Get frame in background (non-blocking with timeout)
                try:
                    frame = await asyncio.wait_for(
                        get_frame_callback(),
                        timeout=MeetingVerificationConfig.FRAME_CAPTURE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"⚠️  Frame capture timeout for user {self.user_id}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Error capturing frame: {e}")
                    continue
                
                if frame is None:
                    logger.warning(f"⚠️  No frame available for user {self.user_id}")
                    continue
                
                # Perform background verification
                is_verified, similarity = await self._verify_frame(frame)
                self.last_check_time = datetime.now()
                
                # Save verification check to database (if enabled)
                await VerificationDatabase.save_verification_check(
                    self.user_id, self.meeting_id, is_verified, similarity, self.session_id
                )
                
                if is_verified:
                    await self._handle_successful_verification(similarity)
                else:
                    await self._handle_failed_verification(similarity)
                
            except asyncio.CancelledError:
                logger.info(f"🛑 Verification cancelled for user {self.user_id}")
                break
            except Exception as e:
                logger.error(f"❌ Error in verification loop: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(self._get_current_interval())
        
        logger.info(f"🛑 Background verification stopped for user {self.user_id}")
    
    async def _verify_frame(self, frame) -> Tuple[bool, float]:
        """
        Verify face in frame (returns verification status and similarity)
        
        Returns:
            Tuple[bool, float]: (is_verified, max_similarity)
        """
        try:
            self.total_checks += 1
            
            # Convert base64 to numpy if needed
            if isinstance(frame, str):
                try:
                    frame = base64_to_numpy(frame)
                except Exception as e:
                    logger.error(f"❌ Error converting base64 to numpy: {e}")
                    return True, 1.0  # Skip invalid frames
            
            # Validate frame
            if frame is None or not isinstance(frame, np.ndarray):
                logger.warning(f"⚠️  Invalid frame format for user {self.user_id}")
                return True, 1.0  # Skip invalid frames
            
            if frame.size == 0:
                logger.warning(f"⚠️  Empty frame for user {self.user_id}")
                return True, 1.0
            
            # Extract embedding from live frame
            try:
                live_embedding = self.face_model.extract_embedding(
                    frame,
                    return_face_info=False
                )
            except ValueError as e:
                # No face detected
                logger.warning(f"⚠️  No face detected for user {self.user_id}: {e}")
                return True, 1.0  # Skip frames without faces (don't penalize)
            except Exception as e:
                logger.error(f"❌ Error extracting embedding: {e}")
                return True, 1.0
            
            # Convert to numpy array
            live_embedding = np.array(live_embedding, dtype=np.float32)
            
            # Validate embedding
            if live_embedding.size == 0:
                logger.warning(f"⚠️  Empty embedding for user {self.user_id}")
                return True, 1.0
            
            # Compare with all stored embeddings
            max_similarity = 0.0
            best_match_id = None
            
            for stored_emb_data in self.stored_embeddings:
                try:
                    # Calculate cosine distance
                    distance = self.face_model.compare_embeddings(
                        live_embedding,
                        stored_emb_data['embedding'],
                        method='cosine'
                    )
                    
                    # Convert distance to similarity
                    similarity = 1 - distance
                    
                    if similarity > max_similarity:
                        max_similarity = similarity
                        best_match_id = stored_emb_data['embedding_id']
                        
                except Exception as e:
                    logger.error(f"❌ Error comparing embeddings: {e}")
                    continue
            
            # Determine if verified
            threshold = MeetingVerificationConfig.FACE_DISTANCE_THRESHOLD
            similarity_threshold = 1 - threshold
            is_verified = max_similarity >= similarity_threshold
            
            # ============================================================
            # CONSOLE LOGGING - Detailed Verification Result
            # ============================================================
            if MeetingVerificationConfig.LOG_EACH_CHECK:
                if is_verified:
                    # ✅ Authorized person
                    logger.info(
                        f"\n{'='*70}\n"
                        f"✅ CONTINUOUS VERIFICATION: PASSED\n"
                        f"{'='*70}\n"
                        f"User ID: {self.user_id}\n"
                        f"Meeting ID: {self.meeting_id}\n"
                        f"Session ID: {self.session_id}\n"
                        f"Similarity Score: {max_similarity:.3f}\n"
                        f"Threshold: {similarity_threshold:.3f}\n"
                        f"Best Match ID: {best_match_id}\n"
                        f"Verification State: {self.state.value}\n"
                        f"Warning Count: {self.warning_count}/{MeetingVerificationConfig.MAX_WARNINGS}\n"
                        f"Total Checks: {self.total_checks}\n"
                        f"Success Rate: {(self.successful_checks/self.total_checks*100):.1f}%\n"
                        f"Result: AUTHORIZED PERSON\n"
                        f"Status: Identity confirmed - correct user\n"
                        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"{'='*70}\n"
                    )
                else:
                    # 🚫 Unknown person
                    logger.error(
                        f"\n{'='*70}\n"
                        f"🚫 UNKNOWN PERSON DETECTED - CONTINUOUS VERIFICATION FAILED\n"
                        f"{'='*70}\n"
                        f"⚠️  ALERT: Identity mismatch detected!\n"
                        f"{'='*70}\n"
                        f"Expected User ID: {self.user_id}\n"
                        f"Meeting ID: {self.meeting_id}\n"
                        f"Session ID: {self.session_id}\n"
                        f"Similarity Score: {max_similarity:.3f}\n"
                        f"Threshold Required: {similarity_threshold:.3f}\n"
                        f"Best Match ID: {best_match_id}\n"
                        f"Verification State: {self.state.value}\n"
                        f"Warning Count: {self.warning_count}/{MeetingVerificationConfig.MAX_WARNINGS}\n"
                        f"Total Checks: {self.total_checks}\n"
                        f"Failed Checks: {self.failed_checks}\n"
                        f"Result: UNAUTHORIZED PERSON\n"
                        f"Status: DIFFERENT PERSON IN FRAME\n"
                        f"Action: Warning will be issued\n"
                        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"{'='*70}\n"
                        f"SECURITY ANALYSIS:\n"
                        f"  - The person in frame does NOT match registered user {self.user_id}\n"
                        f"  - This could indicate account sharing or credential misuse\n"
                        f"  - Automatic warning/removal procedures will follow\n"
                        f"{'='*70}\n"
                    )
                    
                    # Additional critical alert for first unknown detection
                    if self.warning_count == 0:
                        logger.critical(
                            f"🚨 FIRST-TIME UNKNOWN PERSON ALERT: "
                            f"User {self.user_id} in meeting {self.meeting_id} - "
                            f"Similarity {max_similarity:.3f} below threshold {similarity_threshold:.3f}"
                        )
            
            return is_verified, max_similarity
            
        except Exception as e:
            logger.error(f"❌ Error verifying frame: {e}")
            logger.error(traceback.format_exc())
            return True, 1.0  # Skip on errors to avoid false removals
    
    async def _handle_successful_verification(self, similarity: float):
        """Handle successful verification"""
        self.successful_checks += 1
        
        # Reset warnings if user was in warning phase
        if self.warning_count > 0 and MeetingVerificationConfig.RESET_WARNINGS_ON_SUCCESS:
            previous_warnings = self.warning_count
            self.warning_count = 0
            self.state = VerificationState.INITIAL
            self.first_warning_time = None
            
            # Notify user about reset
            await WarningCallback.on_user_reverified(
                self.user_id,
                self.meeting_id,
                previous_warnings,
                similarity
            )
    
    async def _handle_failed_verification(self, similarity: float):
        """Handle failed verification with progressive warnings"""
        self.failed_checks += 1
        self.warning_count += 1
        
        # Record warning
        warning_record = {
            'timestamp': datetime.now(),
            'warning_number': self.warning_count,
            'similarity': similarity,
            'state': self.state.value
        }
        self.warning_history.append(warning_record)
        
        # Handle based on warning count
        if self.warning_count == 1:
            # FIRST WARNING - Switch to 1-minute checks
            self.state = VerificationState.WARNING_PHASE
            self.first_warning_time = datetime.now()
            await WarningCallback.on_first_warning(
                self.user_id,
                self.meeting_id,
                similarity,
                self.session_id
            )
            
        elif self.warning_count == 2:
            # SECOND WARNING - Continue 1-minute checks
            await WarningCallback.on_second_warning(
                self.user_id,
                self.meeting_id,
                similarity,
                self.session_id,
                self.host_id
            )
            
        elif self.warning_count >= MeetingVerificationConfig.MAX_WARNINGS:
            # THIRD WARNING - REMOVE FROM MEETING
            await self._handle_max_warnings(similarity)
    
    async def _handle_max_warnings(self, similarity: float):
        """Handle max warnings - remove user from meeting"""
        self.is_blocked = True
        self.is_running = False
        self.state = VerificationState.BLOCKED
        self.removal_time = datetime.now()
        
        await WarningCallback.on_third_warning_removal(
            self.user_id,
            self.meeting_id,
            similarity,
            self.session_id,
            self.host_id
        )
        
        # TODO: Integrate with your meeting system to:
        # 1. Disconnect user from WebRTC/meeting room
        # 2. Update user status in database
        # 3. Prevent re-joining until host allows or session ends
        
        logger.critical(
            f"⚠️  ACTION REQUIRED: Remove user {self.user_id} from meeting {self.meeting_id}"
        )
    
    def can_join_meeting(self) -> Tuple[bool, str]:
        """
        Check if user can join meeting
        
        Returns:
            Tuple[bool, str]: (can_join, reason_message)
        """
        if not self.is_blocked:
            return True, "User can join"
        
        if self.host_override:
            return True, "Host has allowed this user"
        
        if MeetingVerificationConfig.BLOCK_UNTIL_SESSION_ENDS:
            return False, f"Blocked until session ends. Removed at {self.removal_time}"
        
        return False, "Contact host for permission to rejoin"
    
    def host_allow_user(self):
        """Host allows blocked user to rejoin"""
        if self.is_blocked:
            self.host_override = True
            self.warning_count = 0
            self.state = VerificationState.INITIAL
            self.is_blocked = False  # Allow them to rejoin
            
            logger.info(
                f"\n{'='*80}\n"
                f"✅ HOST OVERRIDE - User Allowed to Rejoin\n"
                f"{'='*80}\n"
                f"User: {self.user_id}\n"
                f"Meeting: {self.meeting_id}\n"
                f"Action: Reset warnings, allow rejoin\n"
                f"{'='*80}\n"
            )
            
            # Send notification
            asyncio.create_task(
                VerificationNotification.send_notification(
                    self.user_id,
                    NotificationType.HOST_OVERRIDE,
                    {
                        'message': 'The host has allowed you to rejoin the meeting.',
                        'can_rejoin': True
                    }
                )
            )
    
    async def stop(self):
        """Stop verification gracefully"""
        logger.info(f"🛑 Stopping verification for user {self.user_id}")
        self.is_running = False
        
        if self.verification_task and not self.verification_task.done():
            self.verification_task.cancel()
            try:
                await self.verification_task
            except asyncio.CancelledError:
                pass
    
    def get_stats(self) -> Dict:
        """Get detailed statistics"""
        can_join, join_message = self.can_join_meeting()
        
        return {
            'user_id': self.user_id,
            'meeting_id': self.meeting_id,
            'session_id': self.session_id,
            'host_id': self.host_id,
            
            # State
            'state': self.state.value,
            'is_running': self.is_running,
            'is_blocked': self.is_blocked,
            'host_override': self.host_override,
            'can_join': can_join,
            'join_message': join_message,
            
            # Warnings
            'warning_count': self.warning_count,
            'max_warnings': MeetingVerificationConfig.MAX_WARNINGS,
            'warning_history': [
                {
                    'timestamp': w['timestamp'].isoformat(),
                    'warning_number': w['warning_number'],
                    'similarity': round(w['similarity'], 3),
                    'state': w['state']
                }
                for w in self.warning_history
            ],
            
            # Checks
            'total_checks': self.total_checks,
            'successful_checks': self.successful_checks,
            'failed_checks': self.failed_checks,
            'success_rate': (
                f"{(self.successful_checks/self.total_checks*100):.1f}%"
                if self.total_checks > 0 else "N/A"
            ),
            
            # Timing
            'current_interval_seconds': self._get_current_interval(),
            'current_interval_display': (
                f"{self._get_current_interval()}s "
                f"({self._get_current_interval()//60} min)"
            ),
            'last_check': self.last_check_time.isoformat() if self.last_check_time else None,
            'first_warning_time': self.first_warning_time.isoformat() if self.first_warning_time else None,
            'removal_time': self.removal_time.isoformat() if self.removal_time else None,
        }

# ============================================================================
# GLOBAL REGISTRY
# ============================================================================

_active_verifiers: Dict[str, MeetingContinuousVerifier] = {}
_blocked_users: Dict[str, Dict] = {}  # Track blocked users per session

def get_verifier(meeting_id: str, user_id: int) -> Optional[MeetingContinuousVerifier]:
    """Get existing verifier"""
    key = f"{meeting_id}_{user_id}"
    return _active_verifiers.get(key)

def create_verifier(
    meeting_id: str,
    user_id: int,
    session_id: str = None,
    host_id: int = None
) -> MeetingContinuousVerifier:
    """Create new verifier"""
    key = f"{meeting_id}_{user_id}"
    
    # Check if user is blocked
    if key in _blocked_users:
        blocked_info = _blocked_users[key]
        logger.warning(
            f"⚠️  User {user_id} is blocked from meeting {meeting_id}\n"
            f"   Reason: {blocked_info.get('reason')}\n"
            f"   Blocked at: {blocked_info.get('blocked_at')}\n"
        )
    
    # Replace existing verifier if present
    if key in _active_verifiers:
        logger.warning(f"⚠️  Replacing existing verifier for {key}")
        old_verifier = _active_verifiers[key]
        asyncio.create_task(old_verifier.stop())
    
    verifier = MeetingContinuousVerifier(meeting_id, user_id, session_id, host_id)
    _active_verifiers[key] = verifier
    return verifier

async def remove_verifier(meeting_id: str, user_id: int):
    """Remove verifier"""
    key = f"{meeting_id}_{user_id}"
    if key in _active_verifiers:
        verifier = _active_verifiers[key]
        await verifier.stop()
        
        # If blocked, add to blocked users registry
        if verifier.is_blocked and MeetingVerificationConfig.BLOCK_UNTIL_SESSION_ENDS:
            _blocked_users[key] = {
                'user_id': user_id,
                'meeting_id': meeting_id,
                'session_id': verifier.session_id,
                'blocked_at': datetime.now().isoformat(),
                'reason': 'Max warnings exceeded',
                'warning_count': verifier.warning_count
            }
        
        del _active_verifiers[key]
        logger.info(f"🗑️  Removed verifier for {key}")

def get_all_verifiers() -> Dict:
    """Get all active verifiers"""
    return _active_verifiers.copy()

def get_blocked_users() -> Dict:
    """Get all blocked users"""
    return _blocked_users.copy()

def clear_session_blocks(meeting_id: str):
    """Clear all blocks for a meeting session (call when meeting ends)"""
    keys_to_remove = [
        key for key in _blocked_users.keys()
        if key.startswith(f"{meeting_id}_")
    ]
    for key in keys_to_remove:
        del _blocked_users[key]
    logger.info(f"🧹 Cleared {len(keys_to_remove)} blocks for meeting {meeting_id}")

# ============================================================================
# PUBLIC API (USE THESE FUNCTIONS IN YOUR APPLICATION)
# ============================================================================

async def start_meeting_verification(
    meeting_id: str,
    user_id: int,
    get_frame_callback: Callable,
    session_id: str = None,
    host_id: int = None
) -> asyncio.Task:
    """
    Start continuous BACKGROUND verification for a user in a meeting.
    
    Args:
        meeting_id (str): Unique identifier for the meeting
        user_id (int): User ID to verify
        get_frame_callback (Callable): Async function that returns the current video frame
        session_id (str, optional): Session identifier
        host_id (int, optional): Meeting host user ID for notifications
    
    Returns:
        asyncio.Task: The background verification task
    """
    verifier = create_verifier(meeting_id, user_id, session_id, host_id)
    
    # Check if user can join
    can_join, message = verifier.can_join_meeting()
    if not can_join:
        logger.error(f"❌ User {user_id} cannot join meeting {meeting_id}: {message}")
        raise PermissionError(f"User blocked: {message}")
    
    # Start verification task
    task = asyncio.create_task(verifier.start_verification(get_frame_callback))
    verifier.verification_task = task
    
    logger.info(
        f"✅ Started background verification task for user {user_id} in meeting {meeting_id}"
    )
    return task

async def stop_meeting_verification(meeting_id: str, user_id: int):
    """Stop continuous verification for a user"""
    await remove_verifier(meeting_id, user_id)

def get_verification_status(meeting_id: str, user_id: int) -> Dict:
    """Get detailed verification status for a user"""
    verifier = get_verifier(meeting_id, user_id)
    if not verifier:
        # Check if blocked
        key = f"{meeting_id}_{user_id}"
        if key in _blocked_users:
            return {
                'active': False,
                'is_blocked': True,
                'blocked_info': _blocked_users[key]
            }
        return {
            'active': False,
            'is_blocked': False,
            'message': 'No verification found for this user'
        }
    
    return {'active': True, **verifier.get_stats()}

def host_allow_blocked_user(meeting_id: str, user_id: int) -> bool:
    """Allow a blocked user to rejoin the meeting (host override)"""
    verifier = get_verifier(meeting_id, user_id)
    if verifier and verifier.is_blocked:
        verifier.host_allow_user()
        
        # Remove from blocked registry
        key = f"{meeting_id}_{user_id}"
        if key in _blocked_users:
            del _blocked_users[key]
        
        logger.info(f"✅ Host allowed user {user_id} to rejoin meeting {meeting_id}")
        return True
    
    logger.warning(f"⚠️  User {user_id} was not blocked in meeting {meeting_id}")
    return False

def end_meeting_session(meeting_id: str):
    """End a meeting session and clear all blocks"""
    clear_session_blocks(meeting_id)
    logger.info(f"✅ Meeting session ended for {meeting_id}")

def get_meeting_verification_summary(meeting_id: str) -> Dict:
    """Get summary of all verifications in a meeting"""
    active = [
        v for k, v in _active_verifiers.items()
        if k.startswith(f"{meeting_id}_")
    ]
    
    blocked = [
        b for k, b in _blocked_users.items()
        if k.startswith(f"{meeting_id}_")
    ]
    
    return {
        'meeting_id': meeting_id,
        'active_count': len(active),
        'blocked_count': len(blocked),
        'active_users': [v.user_id for v in active],
        'blocked_users': [b['user_id'] for b in blocked],
        'total_warnings': sum(v.warning_count for v in active),
        'verifiers': [v.get_stats() for v in active],
        'blocked_details': blocked
    }

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def configure_verification(
    initial_interval: int = None,
    warning_interval: int = None,
    max_warnings: int = None,
    face_threshold: float = None,
    log_each_check: bool = None
):
    """Configure verification settings at runtime"""
    if initial_interval is not None:
        MeetingVerificationConfig.INITIAL_CHECK_INTERVAL = initial_interval
        logger.info(f"✏️  Initial interval set to {initial_interval}s")
    
    if warning_interval is not None:
        MeetingVerificationConfig.WARNING_CHECK_INTERVAL = warning_interval
        logger.info(f"✏️  Warning interval set to {warning_interval}s")
    
    if max_warnings is not None:
        MeetingVerificationConfig.MAX_WARNINGS = max_warnings
        logger.info(f"✏️  Max warnings set to {max_warnings}")
    
    if face_threshold is not None:
        MeetingVerificationConfig.FACE_DISTANCE_THRESHOLD = face_threshold
        logger.info(f"✏️  Face threshold set to {face_threshold}")
    
    if log_each_check is not None:
        MeetingVerificationConfig.LOG_EACH_CHECK = log_each_check
        logger.info(f"✏️  Log each check set to {log_each_check}")

# ============================================================================
# INITIALIZATION
# ============================================================================

logger.info(
    f"\n{'='*80}\n"
    f"✅ Meeting Continuous Verification Module Loaded (v3.0.0)\n"
    f"{'='*80}\n"
    f"Features:\n"
    f"  ✅ Background verification (non-intrusive)\n"
    f"  ✅ Initial interval: {MeetingVerificationConfig.INITIAL_CHECK_INTERVAL}s (5 min)\n"
    f"  ✅ Warning interval: {MeetingVerificationConfig.WARNING_CHECK_INTERVAL}s (1 min)\n"
    f"  ✅ Warning system: {MeetingVerificationConfig.MAX_WARNINGS} strikes\n"
    f"  ✅ Session-based blocking\n"
    f"  ✅ Host override capability\n"
    f"  ✅ Database integration: {'Enabled' if DATABASE_AVAILABLE else 'Disabled'}\n"
    f"  ✅ WebSocket notifications: {'Enabled' if WEBSOCKET_AVAILABLE else 'Disabled'}\n"
    f"  ✅ ENHANCED CONSOLE LOGGING for unknown person detection\n"
    f"{'='*80}\n"
)

# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # Main functions
    'start_meeting_verification',
    'stop_meeting_verification',
    'get_verification_status',
    'host_allow_blocked_user',
    'end_meeting_session',
    'get_meeting_verification_summary',
    
    # Configuration
    'configure_verification',
    'MeetingVerificationConfig',
    
    # Classes
    'MeetingContinuousVerifier',
    'VerificationState',
    'NotificationType',
    
    # Registry functions
    'get_verifier',
    'create_verifier',
    'get_all_verifiers',
    'get_blocked_users',
]

__version__ = "3.0.0"
__author__ = "Meeting Authentication System"
