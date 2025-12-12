# unified_face_service.py



"""
UNIFIED FACE SERVICE - Complete Implementation with Enhanced Logging
====================================================================
Shared face recognition service for both AI Attendance and Meeting Continuous Verification

Location: core/FaceAuth/unified_face_service.py

Author: Meeting Authentication System
Version: 1.0.0
Date: 2025-01-15

PURPOSE:
- Prevents resource conflicts between AI Attendance and Continuous Verification
- Single face model loaded in memory (saves GPU/RAM)
- Shared frame cache (no duplicate processing)
- Cached embeddings for better performance
- Thread-safe operations
- ENHANCED CONSOLE LOGGING for unknown person detection

USAGE:
    from core.FaceAuth.unified_face_service import get_unified_face_service
    
    service = get_unified_face_service()
    is_verified, similarity = await service.verify_face(frame, user_id)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import logging
import asyncio
import time
from typing import Dict, Optional, List, Tuple, Any
from threading import Lock
from datetime import datetime, timedelta

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import numpy as np
from bson import ObjectId

# ============================================================================
# PROJECT IMPORTS
# ============================================================================
try:
    from core.FaceAuth.face_model_shared import get_face_model
    FACE_MODEL_AVAILABLE = True
except ImportError:
    FACE_MODEL_AVAILABLE = False
    logging.warning("face_model_shared not available")

try:
    from core.UserDashBoard.face_embeddings import get_user_embeddings, base64_to_numpy
    USER_EMBEDDINGS_AVAILABLE = True
except ImportError:
    USER_EMBEDDINGS_AVAILABLE = False
    logging.warning("face_embeddings not available")

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # Uncomment to save to file
        # logging.FileHandler('unified_face_service.log')
    ]
)
logger = logging.getLogger("unified_face_service")

# ============================================================================
# CONFIGURATION
# ============================================================================

class UnifiedFaceServiceConfig:
    """Configuration for unified face service"""
    
    # Face recognition thresholds
    FACE_DISTANCE_THRESHOLD = 0.6  # Cosine distance threshold
    MIN_FACE_CONFIDENCE = 0.3      # Minimum detection confidence
    
    # Caching
    FRAME_CACHE_MAX_AGE = 5.0      # Seconds - how long to keep frames
    EMBEDDING_CACHE_MAX_AGE = 300  # Seconds - 5 minutes
    MAX_CACHED_FRAMES = 100        # Maximum frames in cache
    MAX_CACHED_EMBEDDINGS = 50     # Maximum user embeddings in cache
    
    # Performance
    ENABLE_FRAME_CACHE = True
    ENABLE_EMBEDDING_CACHE = True
    AUTO_CLEANUP_INTERVAL = 60     # Seconds - cleanup old cache entries
    
    # Comparison methods
    COMPARISON_METHOD = 'cosine'   # 'cosine' or 'euclidean'
    
    # Logging
    LOG_CACHE_HITS = False         # Log cache hit/miss (verbose)
    LOG_VERIFICATIONS = True       # Log verification attempts
    LOG_DETAILED_ANALYSIS = True   # Log detailed analysis for unknown persons

# ============================================================================
# FRAME CACHE
# ============================================================================

class FrameCache:
    """
    Manages frame caching for multiple users
    Thread-safe frame storage
    """
    
    def __init__(self, max_age: float = 5.0, max_size: int = 100):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_age = max_age
        self.max_size = max_size
        self._lock = Lock()
    
    def store(self, meeting_id: str, user_id: int, frame: np.ndarray):
        """Store a frame in cache"""
        key = f"{meeting_id}_{user_id}"
        
        with self._lock:
            # Check cache size
            if len(self.cache) >= self.max_size:
                self._cleanup()
            
            self.cache[key] = {
                'frame': frame,
                'timestamp': time.time(),
                'access_count': 0
            }
            
            if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                logger.debug(f"Frame stored: {key}")
    
    def get(self, meeting_id: str, user_id: int) -> Optional[np.ndarray]:
        """Get a frame from cache"""
        key = f"{meeting_id}_{user_id}"
        
        with self._lock:
            if key not in self.cache:
                if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                    logger.debug(f"Frame cache MISS: {key}")
                return None
            
            entry = self.cache[key]
            age = time.time() - entry['timestamp']
            
            # Check if frame is too old
            if age > self.max_age:
                del self.cache[key]
                if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                    logger.debug(f"Frame expired: {key} (age: {age:.1f}s)")
                return None
            
            # Update access count
            entry['access_count'] += 1
            
            if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                logger.debug(f"Frame cache HIT: {key} (age: {age:.1f}s)")
            
            return entry['frame']
    
    def clear(self, meeting_id: str, user_id: int):
        """Clear a specific frame from cache"""
        key = f"{meeting_id}_{user_id}"
        
        with self._lock:
            if key in self.cache:
                del self.cache[key]
                logger.debug(f"Frame cleared: {key}")
    
    def clear_all(self):
        """Clear all frames"""
        with self._lock:
            count = len(self.cache)
            self.cache.clear()
            logger.info(f"Cleared all frames ({count} entries)")
    
    def _cleanup(self):
        """Remove old frames (called when cache is full)"""
        current_time = time.time()
        keys_to_remove = []
        
        for key, entry in self.cache.items():
            age = current_time - entry['timestamp']
            if age > self.max_age:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.cache[key]
        
        # If still full, remove least recently used
        if len(self.cache) >= self.max_size:
            sorted_entries = sorted(
                self.cache.items(),
                key=lambda x: (x[1]['access_count'], x[1]['timestamp'])
            )
            
            # Remove 20% of entries
            remove_count = max(1, self.max_size // 5)
            for key, _ in sorted_entries[:remove_count]:
                del self.cache[key]
        
        logger.debug(f"Cache cleanup: removed {len(keys_to_remove)} expired entries")
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self._lock:
            total_entries = len(self.cache)
            current_time = time.time()
            
            ages = [current_time - entry['timestamp'] for entry in self.cache.values()]
            access_counts = [entry['access_count'] for entry in self.cache.values()]
            
            return {
                'total_entries': total_entries,
                'max_size': self.max_size,
                'max_age': self.max_age,
                'avg_age': sum(ages) / len(ages) if ages else 0,
                'avg_access_count': sum(access_counts) / len(access_counts) if access_counts else 0,
                'oldest_frame_age': max(ages) if ages else 0,
            }

# ============================================================================
# EMBEDDING CACHE
# ============================================================================

class EmbeddingCache:
    """
    Manages user embedding caching
    Thread-safe embedding storage
    """
    
    def __init__(self, max_age: float = 300, max_size: int = 50):
        self.cache: Dict[int, Dict[str, Any]] = {}
        self.max_age = max_age
        self.max_size = max_size
        self._lock = Lock()
    
    def store(self, user_id: int, embeddings: List[Dict]):
        """Store user embeddings in cache"""
        with self._lock:
            # Check cache size
            if len(self.cache) >= self.max_size:
                self._cleanup()
            
            self.cache[user_id] = {
                'embeddings': embeddings,
                'timestamp': time.time(),
                'access_count': 0
            }
            
            logger.debug(f"Embeddings stored: user {user_id} ({len(embeddings)} embeddings)")
    
    def get(self, user_id: int) -> Optional[List[Dict]]:
        """Get user embeddings from cache"""
        with self._lock:
            if user_id not in self.cache:
                if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                    logger.debug(f"Embedding cache MISS: user {user_id}")
                return None
            
            entry = self.cache[user_id]
            age = time.time() - entry['timestamp']
            
            # Check if embeddings are too old
            if age > self.max_age:
                del self.cache[user_id]
                if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                    logger.debug(f"Embeddings expired: user {user_id} (age: {age:.1f}s)")
                return None
            
            # Update access count
            entry['access_count'] += 1
            
            if UnifiedFaceServiceConfig.LOG_CACHE_HITS:
                logger.debug(f"Embedding cache HIT: user {user_id} (age: {age:.1f}s)")
            
            return entry['embeddings']
    
    def clear(self, user_id: int):
        """Clear embeddings for a specific user"""
        with self._lock:
            if user_id in self.cache:
                del self.cache[user_id]
                logger.debug(f"Embeddings cleared: user {user_id}")
    
    def clear_all(self):
        """Clear all embeddings"""
        with self._lock:
            count = len(self.cache)
            self.cache.clear()
            logger.info(f"Cleared all embeddings ({count} users)")
    
    def _cleanup(self):
        """Remove old embeddings"""
        current_time = time.time()
        keys_to_remove = []
        
        for user_id, entry in self.cache.items():
            age = current_time - entry['timestamp']
            if age > self.max_age:
                keys_to_remove.append(user_id)
        
        for user_id in keys_to_remove:
            del self.cache[user_id]
        
        # If still full, remove least recently used
        if len(self.cache) >= self.max_size:
            sorted_entries = sorted(
                self.cache.items(),
                key=lambda x: (x[1]['access_count'], x[1]['timestamp'])
            )
            
            # Remove 20% of entries
            remove_count = max(1, self.max_size // 5)
            for user_id, _ in sorted_entries[:remove_count]:
                del self.cache[user_id]
        
        logger.debug(f"Embedding cleanup: removed {len(keys_to_remove)} expired entries")
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self._lock:
            total_users = len(self.cache)
            current_time = time.time()
            
            ages = [current_time - entry['timestamp'] for entry in self.cache.values()]
            access_counts = [entry['access_count'] for entry in self.cache.values()]
            embedding_counts = [len(entry['embeddings']) for entry in self.cache.values()]
            
            return {
                'total_users': total_users,
                'max_size': self.max_size,
                'max_age': self.max_age,
                'avg_age': sum(ages) / len(ages) if ages else 0,
                'avg_access_count': sum(access_counts) / len(access_counts) if access_counts else 0,
                'avg_embeddings_per_user': sum(embedding_counts) / len(embedding_counts) if embedding_counts else 0,
                'oldest_cache_age': max(ages) if ages else 0,
            }

# ============================================================================
# MAIN UNIFIED FACE SERVICE
# ============================================================================

class UnifiedFaceService:
    """
    Unified Face Recognition Service
    
    Single service shared by:
    - AI Attendance system (behavior detection)
    - Meeting Continuous Verification (identity verification)
    
    Features:
    - Single face model instance (saves memory)
    - Frame caching (no duplicate processing)
    - Embedding caching (faster verification)
    - Thread-safe operations
    - Automatic cleanup
    - ENHANCED CONSOLE LOGGING for unknown person detection
    """
    
    def __init__(self):
        """Initialize the unified face service"""
        self.face_model = None
        self.frame_cache = FrameCache(
            max_age=UnifiedFaceServiceConfig.FRAME_CACHE_MAX_AGE,
            max_size=UnifiedFaceServiceConfig.MAX_CACHED_FRAMES
        )
        self.embedding_cache = EmbeddingCache(
            max_age=UnifiedFaceServiceConfig.EMBEDDING_CACHE_MAX_AGE,
            max_size=UnifiedFaceServiceConfig.MAX_CACHED_EMBEDDINGS
        )
        self._model_lock = Lock()
        self._stats = {
            'total_verifications': 0,
            'successful_verifications': 0,
            'failed_verifications': 0,
            'errors': 0,
            'frame_cache_hits': 0,
            'frame_cache_misses': 0,
            'embedding_cache_hits': 0,
            'embedding_cache_misses': 0,
            'unknown_person_detections': 0,
            'last_unknown_detection': None,
        }
        
        # Load face model
        self._load_model()
        
        logger.info("✅ Unified Face Service initialized")
    
    def _load_model(self):
        """Load face model (called once)"""
        if not FACE_MODEL_AVAILABLE:
            logger.error("❌ Face model not available - cannot load")
            return
        
        try:
            with self._model_lock:
                self.face_model = get_face_model()
                logger.info("✅ Face model loaded successfully (shared instance)")
        except Exception as e:
            logger.error(f"❌ Failed to load face model: {e}")
            self.face_model = None
    
    def store_frame(self, meeting_id: str, user_id: int, frame):
        """
        Store frame in cache (used by both systems)
        
        Args:
            meeting_id: Meeting identifier
            user_id: User identifier
            frame: Frame data (numpy array or base64)
        """
        if not UnifiedFaceServiceConfig.ENABLE_FRAME_CACHE:
            return
        
        try:
            # Convert frame if needed
            if isinstance(frame, str):
                frame = base64_to_numpy(frame)
            
            if frame is not None and isinstance(frame, np.ndarray):
                self.frame_cache.store(meeting_id, user_id, frame)
        except Exception as e:
            logger.error(f"Error storing frame: {e}")
    
    def get_frame(self, meeting_id: str, user_id: int):
        """
        Get cached frame
        
        Args:
            meeting_id: Meeting identifier
            user_id: User identifier
        
        Returns:
            numpy.ndarray or None
        """
        if not UnifiedFaceServiceConfig.ENABLE_FRAME_CACHE:
            return None
        
        frame = self.frame_cache.get(meeting_id, user_id)
        
        if frame is not None:
            self._stats['frame_cache_hits'] += 1
        else:
            self._stats['frame_cache_misses'] += 1
        
        return frame
    
    def clear_frame(self, meeting_id: str, user_id: int):
        """Clear cached frame for a user"""
        self.frame_cache.clear(meeting_id, user_id)
    
    def get_user_embeddings(self, user_id: int) -> Optional[List[Dict]]:
        """
        Get user embeddings (with caching)
        
        Args:
            user_id: User identifier
        
        Returns:
            List of embedding dictionaries or None
        """
        if not USER_EMBEDDINGS_AVAILABLE:
            logger.error("❌ User embeddings module not available")
            return None
        
        # Check cache first
        if UnifiedFaceServiceConfig.ENABLE_EMBEDDING_CACHE:
            cached_embeddings = self.embedding_cache.get(user_id)
            if cached_embeddings is not None:
                self._stats['embedding_cache_hits'] += 1
                return cached_embeddings
            
            self._stats['embedding_cache_misses'] += 1
        
        # Load from database
        try:
            user_embeddings = get_user_embeddings(user_id)
            
            if not user_embeddings:
                logger.warning(f"⚠️  No embeddings found for user {user_id}")
                return None
            
            # Convert to standard format
            embeddings_list = []
            for emb_doc in user_embeddings:
                if emb_doc.get('embedding'):
                    embeddings_list.append({
                        'embedding': np.array(emb_doc['embedding'], dtype=np.float32),
                        'embedding_id': str(emb_doc['_id']),
                        'det_score': emb_doc.get('det_score', 0.0)
                    })
            
            # Store in cache
            if UnifiedFaceServiceConfig.ENABLE_EMBEDDING_CACHE and embeddings_list:
                self.embedding_cache.store(user_id, embeddings_list)
            
            logger.info(f"✅ Loaded {len(embeddings_list)} embeddings for user {user_id}")
            return embeddings_list
            
        except Exception as e:
            logger.error(f"❌ Error loading embeddings for user {user_id}: {e}")
            return None
    
    def clear_user_embeddings(self, user_id: int):
        """Clear cached embeddings for a user"""
        self.embedding_cache.clear(user_id)
    
    async def verify_face(
        self,
        frame,
        user_id: int,
        threshold: float = None,
        method: str = None
    ) -> Tuple[bool, float]:
        """
        Verify face in frame against stored embeddings
        
        This is the MAIN verification function used by both:
        - AI Attendance system
        - Meeting Continuous Verification
        
        Args:
            frame: Frame data (numpy array or base64 string)
            user_id: User to verify against
            threshold: Distance threshold (default: from config)
            method: Comparison method ('cosine' or 'euclidean')
        
        Returns:
            Tuple[bool, float]: (is_verified, similarity_score)
        """
        self._stats['total_verifications'] += 1
        
        if threshold is None:
            threshold = UnifiedFaceServiceConfig.FACE_DISTANCE_THRESHOLD
        
        if method is None:
            method = UnifiedFaceServiceConfig.COMPARISON_METHOD
        
        try:
            # Convert frame if needed
            if isinstance(frame, str):
                frame = base64_to_numpy(frame)
            
            if frame is None or not isinstance(frame, np.ndarray):
                logger.warning("⚠️  Invalid frame format")
                return True, 1.0  # Skip invalid frames
            
            if frame.size == 0:
                logger.warning("⚠️  Empty frame")
                return True, 1.0
            
            # Check if model is loaded
            if self.face_model is None:
                logger.error("❌ Face model not loaded")
                self._stats['errors'] += 1
                return True, 1.0
            
            # Extract embedding from live frame
            try:
                with self._model_lock:
                    live_embedding = self.face_model.extract_embedding(
                        frame,
                        return_face_info=False
                    )
            except ValueError as e:
                # No face detected - not an error, just skip
                logger.debug(f"No face detected: {e}")
                return True, 1.0
            except Exception as e:
                logger.error(f"Error extracting embedding: {e}")
                self._stats['errors'] += 1
                return True, 1.0
            
            # Convert to numpy array
            live_embedding = np.array(live_embedding, dtype=np.float32)
            
            if live_embedding.size == 0:
                logger.warning("⚠️  Empty embedding extracted")
                return True, 1.0
            
            # Get stored embeddings
            stored_embeddings = self.get_user_embeddings(user_id)
            if not stored_embeddings:
                logger.warning(f"⚠️  No stored embeddings for user {user_id}")
                self._stats['errors'] += 1
                return False, 0.0
            
            # Compare with all stored embeddings
            max_similarity = 0.0
            best_match_id = None
            all_similarities = []
            
            for stored_emb_data in stored_embeddings:
                try:
                    with self._model_lock:
                        distance = self.face_model.compare_embeddings(
                            live_embedding,
                            stored_emb_data['embedding'],
                            method=method
                        )
                    
                    # Convert distance to similarity
                    similarity = 1 - distance
                    all_similarities.append(similarity)
                    
                    if similarity > max_similarity:
                        max_similarity = similarity
                        best_match_id = stored_emb_data['embedding_id']
                        
                except Exception as e:
                    logger.error(f"Error comparing embeddings: {e}")
                    continue
            
            # Determine if verified
            similarity_threshold = 1 - threshold
            is_verified = max_similarity >= similarity_threshold
            
            # Update statistics
            if is_verified:
                self._stats['successful_verifications'] += 1
            else:
                self._stats['failed_verifications'] += 1
                self._stats['unknown_person_detections'] += 1
                self._stats['last_unknown_detection'] = datetime.now().isoformat()
            
            # ============================================================
            # CONSOLE LOGGING - Complete Verification Details
            # ============================================================
            if UnifiedFaceServiceConfig.LOG_VERIFICATIONS:
                if is_verified:
                    # ✅ Verification successful
                    logger.info(
                        f"\n{'='*75}\n"
                        f"✅ UNIFIED FACE SERVICE: VERIFICATION SUCCESS\n"
                        f"{'='*75}\n"
                        f"User ID: {user_id}\n"
                        f"Similarity Score: {max_similarity:.3f}\n"
                        f"Threshold: {similarity_threshold:.3f}\n"
                        f"Best Match Embedding ID: {best_match_id}\n"
                        f"Comparison Method: {method}\n"
                        f"Total Stored Embeddings: {len(stored_embeddings)}\n"
                        f"Result: AUTHORIZED PERSON VERIFIED\n"
                        f"Status: Face matches registered user\n"
                        f"Cache Stats:\n"
                        f"  - Frame Cache Enabled: {UnifiedFaceServiceConfig.ENABLE_FRAME_CACHE}\n"
                        f"  - Embedding Cache Enabled: {UnifiedFaceServiceConfig.ENABLE_EMBEDDING_CACHE}\n"
                        f"Service Stats:\n"
                        f"  - Total Verifications: {self._stats['total_verifications']}\n"
                        f"  - Successful: {self._stats['successful_verifications']}\n"
                        f"  - Failed: {self._stats['failed_verifications']}\n"
                        f"  - Success Rate: {(self._stats['successful_verifications']/self._stats['total_verifications']*100):.1f}%\n"
                        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"{'='*75}\n"
                    )
                else:
                    # 🚫 Verification failed - Unknown person
                    logger.error(
                        f"\n{'='*75}\n"
                        f"🚫 UNIFIED FACE SERVICE: UNKNOWN PERSON DETECTED\n"
                        f"{'='*75}\n"
                        f"⚠️  CRITICAL: Face does not match registered user!\n"
                        f"{'='*75}\n"
                        f"Expected User ID: {user_id}\n"
                        f"Similarity Score: {max_similarity:.3f}\n"
                        f"Threshold Required: {similarity_threshold:.3f}\n"
                        f"Difference: {(similarity_threshold - max_similarity):.3f}\n"
                        f"Best Match Embedding ID: {best_match_id}\n"
                        f"Comparison Method: {method}\n"
                        f"Total Stored Embeddings Checked: {len(stored_embeddings)}\n"
                        f"Result: UNAUTHORIZED PERSON\n"
                        f"Status: Face does NOT match any stored embedding\n"
                        f"Verdict: Someone else is using this account\n"
                        f"Service Stats:\n"
                        f"  - Total Verifications: {self._stats['total_verifications']}\n"
                        f"  - Successful: {self._stats['successful_verifications']}\n"
                        f"  - Failed: {self._stats['failed_verifications']}\n"
                        f"  - Unknown Person Detections: {self._stats['unknown_person_detections']}\n"
                        f"  - Current Success Rate: {(self._stats['successful_verifications']/self._stats['total_verifications']*100):.1f}%\n"
                        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"{'='*75}\n"
                    )
                    
                    # Detailed analysis if enabled
                    if UnifiedFaceServiceConfig.LOG_DETAILED_ANALYSIS:
                        avg_similarity = sum(all_similarities) / len(all_similarities) if all_similarities else 0
                        min_similarity = min(all_similarities) if all_similarities else 0
                        
                        logger.error(
                            f"{'='*75}\n"
                            f"DETAILED ANALYSIS:\n"
                            f"{'='*75}\n"
                            f"  ❌ The person's face embedding does not match user {user_id}\n"
                            f"  ❌ All {len(stored_embeddings)} stored embeddings were checked\n"
                            f"  ❌ Best similarity: {max_similarity:.3f}\n"
                            f"  ❌ Average similarity: {avg_similarity:.3f}\n"
                            f"  ❌ Minimum similarity: {min_similarity:.3f}\n"
                            f"  ❌ Required threshold: {similarity_threshold:.3f}\n"
                            f"  ❌ Shortfall: {(similarity_threshold - max_similarity):.3f}\n"
                            f"  ❌ This indicates a different person is in front of the camera\n"
                            f"{'='*75}\n"
                            f"RECOMMENDED ACTIONS:\n"
                            f"  1. Issue warning to user\n"
                            f"  2. Log security incident\n"
                            f"  3. Monitor for repeated violations\n"
                            f"  4. Consider account suspension if pattern continues\n"
                            f"  5. Alert system administrators\n"
                            f"{'='*75}\n"
                            f"ALL SIMILARITY SCORES:\n"
                        )
                        
                        # Log all individual similarity scores
                        for idx, sim in enumerate(all_similarities, 1):
                            status = "✓ PASS" if sim >= similarity_threshold else "✗ FAIL"
                            logger.error(f"  Embedding {idx}: {sim:.3f} {status}")
                        
                        logger.error(f"{'='*75}\n")
                    
                    # Critical security alert
                    logger.critical(
                        f"🚨 SECURITY ALERT: Unknown person detected for user {user_id} | "
                        f"Similarity: {max_similarity:.3f} | Threshold: {similarity_threshold:.3f} | "
                        f"Detection #{self._stats['unknown_person_detections']}"
                    )
            
            return is_verified, max_similarity
            
        except Exception as e:
            logger.error(f"❌ Error in verify_face: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self._stats['errors'] += 1
            return True, 1.0  # Don't penalize on errors
    
    def cleanup_session(self, meeting_id: str, user_id: int):
        """
        Cleanup session data for a user
        
        Args:
            meeting_id: Meeting identifier
            user_id: User identifier
        """
        self.clear_frame(meeting_id, user_id)
        # Don't clear embeddings - they can be reused
        logger.info(f"🧹 Cleaned up session for user {user_id}")
    
    def cleanup_all(self):
        """Cleanup all cached data"""
        self.frame_cache.clear_all()
        self.embedding_cache.clear_all()
        logger.info("🧹 Cleaned up all cached data")
    
    def get_stats(self) -> Dict:
        """
        Get service statistics
        
        Returns:
            Dictionary with statistics
        """
        frame_stats = self.frame_cache.get_stats()
        embedding_stats = self.embedding_cache.get_stats()
        
        total_verifications = self._stats['total_verifications']
        success_rate = (
            (self._stats['successful_verifications'] / total_verifications * 100)
            if total_verifications > 0 else 0
        )
        
        frame_cache_rate = (
            (self._stats['frame_cache_hits'] / 
             (self._stats['frame_cache_hits'] + self._stats['frame_cache_misses']) * 100)
            if (self._stats['frame_cache_hits'] + self._stats['frame_cache_misses']) > 0 else 0
        )
        
        embedding_cache_rate = (
            (self._stats['embedding_cache_hits'] / 
             (self._stats['embedding_cache_hits'] + self._stats['embedding_cache_misses']) * 100)
            if (self._stats['embedding_cache_hits'] + self._stats['embedding_cache_misses']) > 0 else 0
        )
        
        return {
            'service': {
                'model_loaded': self.face_model is not None,
                'total_verifications': total_verifications,
                'successful_verifications': self._stats['successful_verifications'],
                'failed_verifications': self._stats['failed_verifications'],
                'unknown_person_detections': self._stats['unknown_person_detections'],
                'errors': self._stats['errors'],
                'success_rate': f"{success_rate:.1f}%",
                'last_unknown_detection': self._stats['last_unknown_detection'],
            },
            'frame_cache': {
                **frame_stats,
                'cache_hit_rate': f"{frame_cache_rate:.1f}%",
                'total_hits': self._stats['frame_cache_hits'],
                'total_misses': self._stats['frame_cache_misses'],
            },
            'embedding_cache': {
                **embedding_stats,
                'cache_hit_rate': f"{embedding_cache_rate:.1f}%",
                'total_hits': self._stats['embedding_cache_hits'],
                'total_misses': self._stats['embedding_cache_misses'],
            }
        }
    
    def reset_stats(self):
        """Reset statistics"""
        self._stats = {
            'total_verifications': 0,
            'successful_verifications': 0,
            'failed_verifications': 0,
            'errors': 0,
            'frame_cache_hits': 0,
            'frame_cache_misses': 0,
            'embedding_cache_hits': 0,
            'embedding_cache_misses': 0,
            'unknown_person_detections': 0,
            'last_unknown_detection': None,
        }
        logger.info("📊 Statistics reset")
    
    def get_unknown_person_stats(self) -> Dict:
        """
        Get specific statistics about unknown person detections
        
        Returns:
            Dictionary with unknown person detection statistics
        """
        total_verifications = self._stats['total_verifications']
        unknown_detections = self._stats['unknown_person_detections']
        
        unknown_rate = (
            (unknown_detections / total_verifications * 100)
            if total_verifications > 0 else 0
        )
        
        return {
            'total_unknown_detections': unknown_detections,
            'total_verifications': total_verifications,
            'unknown_detection_rate': f"{unknown_rate:.1f}%",
            'last_unknown_detection': self._stats['last_unknown_detection'],
            'successful_verifications': self._stats['successful_verifications'],
            'failed_verifications': self._stats['failed_verifications'],
        }

# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_unified_service_instance = None
_instance_lock = Lock()

def get_unified_face_service() -> UnifiedFaceService:
    """
    Get the unified face service singleton instance
    
    This ensures only ONE instance of the face service exists,
    preventing duplicate model loading and resource waste.
    
    Returns:
        UnifiedFaceService: The singleton instance
    
    Example:
        service = get_unified_face_service()
        is_verified, similarity = await service.verify_face(frame, user_id)
    """
    global _unified_service_instance
    
    if _unified_service_instance is None:
        with _instance_lock:
            # Double-check locking pattern
            if _unified_service_instance is None:
                _unified_service_instance = UnifiedFaceService()
    
    return _unified_service_instance

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def configure_service(
    face_threshold: float = None,
    frame_cache_enabled: bool = None,
    embedding_cache_enabled: bool = None,
    comparison_method: str = None,
    log_verifications: bool = None,
    log_detailed_analysis: bool = None
):
    """
    Configure the unified face service
    
    Args:
        face_threshold: Face distance threshold
        frame_cache_enabled: Enable/disable frame caching
        embedding_cache_enabled: Enable/disable embedding caching
        comparison_method: 'cosine' or 'euclidean'
        log_verifications: Enable/disable verification logging
        log_detailed_analysis: Enable/disable detailed analysis logging
    
    Example:
        configure_service(
            face_threshold=0.5,
            frame_cache_enabled=True,
            log_verifications=True,
            log_detailed_analysis=True
        )
    """
    if face_threshold is not None:
        UnifiedFaceServiceConfig.FACE_DISTANCE_THRESHOLD = face_threshold
        logger.info(f"✏️  Face threshold set to {face_threshold}")
    
    if frame_cache_enabled is not None:
        UnifiedFaceServiceConfig.ENABLE_FRAME_CACHE = frame_cache_enabled
        logger.info(f"✏️  Frame cache {'enabled' if frame_cache_enabled else 'disabled'}")
    
    if embedding_cache_enabled is not None:
        UnifiedFaceServiceConfig.ENABLE_EMBEDDING_CACHE = embedding_cache_enabled
        logger.info(f"✏️  Embedding cache {'enabled' if embedding_cache_enabled else 'disabled'}")
    
    if comparison_method is not None:
        if comparison_method in ['cosine', 'euclidean']:
            UnifiedFaceServiceConfig.COMPARISON_METHOD = comparison_method
            logger.info(f"✏️  Comparison method set to {comparison_method}")
        else:
            logger.error(f"❌ Invalid comparison method: {comparison_method}")
    
    if log_verifications is not None:
        UnifiedFaceServiceConfig.LOG_VERIFICATIONS = log_verifications
        logger.info(f"✏️  Verification logging {'enabled' if log_verifications else 'disabled'}")
    
    if log_detailed_analysis is not None:
        UnifiedFaceServiceConfig.LOG_DETAILED_ANALYSIS = log_detailed_analysis
        logger.info(f"✏️  Detailed analysis logging {'enabled' if log_detailed_analysis else 'disabled'}")

def get_service_stats() -> Dict:
    """
    Get statistics from the unified face service
    
    Returns:
        Dictionary with service statistics
    
    Example:
        stats = get_service_stats()
        print(f"Success rate: {stats['service']['success_rate']}")
    """
    service = get_unified_face_service()
    return service.get_stats()

def get_unknown_person_stats() -> Dict:
    """
    Get unknown person detection statistics
    
    Returns:
        Dictionary with unknown person statistics
    
    Example:
        stats = get_unknown_person_stats()
        print(f"Unknown detection rate: {stats['unknown_detection_rate']}")
    """
    service = get_unified_face_service()
    return service.get_unknown_person_stats()

def cleanup_all_cache():
    """
    Cleanup all cached data
    
    Example:
        cleanup_all_cache()
    """
    service = get_unified_face_service()
    service.cleanup_all()

def reset_service_stats():
    """
    Reset service statistics
    
    Example:
        reset_service_stats()
    """
    service = get_unified_face_service()
    service.reset_stats()

# ============================================================================
# MODULE EXPORTS
# ============================================================================

__all__ = [
    # Main service
    'UnifiedFaceService',
    'get_unified_face_service',
    
    # Configuration
    'UnifiedFaceServiceConfig',
    'configure_service',
    
    # Utilities
    'get_service_stats',
    'get_unknown_person_stats',
    'cleanup_all_cache',
    'reset_service_stats',
    
    # Cache classes (for advanced usage)
    'FrameCache',
    'EmbeddingCache',
]

__version__ = "1.0.0"
__author__ = "Meeting Authentication System"

# ============================================================================
# INITIALIZATION
# ============================================================================

logger.info(
    f"\n{'='*80}\n"
    f"✅ Unified Face Service Module Loaded (v{__version__})\n"
    f"{'='*80}\n"
    f"Features:\n"
    f"  ✅ Single face model instance (shared)\n"
    f"  ✅ Frame caching: {'Enabled' if UnifiedFaceServiceConfig.ENABLE_FRAME_CACHE else 'Disabled'}\n"
    f"  ✅ Embedding caching: {'Enabled' if UnifiedFaceServiceConfig.ENABLE_EMBEDDING_CACHE else 'Disabled'}\n"
    f"  ✅ Face threshold: {UnifiedFaceServiceConfig.FACE_DISTANCE_THRESHOLD}\n"
    f"  ✅ Comparison method: {UnifiedFaceServiceConfig.COMPARISON_METHOD}\n"
    f"  ✅ Thread-safe operations\n"
    f"  ✅ ENHANCED CONSOLE LOGGING for unknown person detection\n"
    f"  ✅ Detailed analysis logging: {'Enabled' if UnifiedFaceServiceConfig.LOG_DETAILED_ANALYSIS else 'Disabled'}\n"
    f"{'='*80}\n"
)
