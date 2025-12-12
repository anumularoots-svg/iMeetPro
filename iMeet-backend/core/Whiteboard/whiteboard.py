# core/Whiteboard/whiteboard.py - COMPLETE FIXED VERSION WITH CLEAN LOGGING
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.db import connection, transaction
from django.utils import timezone
import json
import logging
import uuid
import pytz
import redis
import os
from typing import Dict, List, Optional
import traceback
from redis.connection import ConnectionPool
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

logger = logging.getLogger('whiteboard')
IST_TIMEZONE = pytz.timezone("Asia/Kolkata")

# ================================
# LOGGING HELPERS
# ================================
from collections import defaultdict
from datetime import datetime, timedelta
import threading

class LogRateLimiter:
    """Thread-safe rate limiter for logging"""
    def __init__(self, seconds=5):
        self.last_log = defaultdict(lambda: datetime.min)
        self.interval = timedelta(seconds=seconds)
        self.lock = threading.Lock()
    
    def should_log(self, key: str) -> bool:
        with self.lock:
            now = datetime.now()
            if now - self.last_log[key] > self.interval:
                self.last_log[key] = now
                return True
            return False

# Initialize rate limiter globally
log_limiter = LogRateLimiter(seconds=10)  # Log once per 10 seconds max

def log_drawings_verified(drawing_ids):
    """Log verification of drawings in Redis"""
    logger.info(f"✅ Verified {len(drawing_ids)} drawings in Redis")

def log_drawings_saving(drawing_ids):
    """Log saving drawings to Redis"""
    logger.info(f"💾 Saving {len(drawing_ids)} drawings to Redis...")

def log_undo_stack(count):
    """Log undo stack update"""
    logger.info(f"📝 Undo stack updated: {count} items (max=50)")

def log_redo_stack(count):
    """Log redo stack update"""
    logger.info(f"🔁 Redo stack updated: {count} items")

def log_cache_summary(draw_count, undo_count, redo_count):
    """Log cache state summary"""
    logger.info(f"📊 Redis Cache → Drawings={draw_count}, Undo={undo_count}, Redo={redo_count}")

def log_state_transition(meeting_id, can_undo, can_redo, undo_count, redo_count):
    """Log state transition with undo/redo capabilities"""
    logger.info(
        f"✅ Meeting {meeting_id[:8]} → Undo={'Yes' if can_undo else 'No'}({undo_count}), "
        f"Redo={'Yes' if can_redo else 'No'}({redo_count})"
    )

# ================================
# REDIS CONFIGURATION
# ================================

# Redis configuration with connection pooling
REDIS_CONFIG = {
    'host': os.getenv("REDIS_HOST", "127.0.0.1"),
    'port': int(os.getenv("REDIS_PORT", 6379)),
    'db': int(os.getenv("REDIS_DB", 0)),
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
    'socket_keepalive': True,
    'socket_keepalive_options': {},
    'health_check_interval': 30,
    'retry_on_timeout': True,
    'retry_on_error': [ConnectionError, TimeoutError],
    'retry': Retry(ExponentialBackoff(), 3)
}

# Initialize Redis with connection pool
try:
    pool = ConnectionPool(**REDIS_CONFIG, max_connections=20)
    redis_client = redis.Redis(connection_pool=pool)
    redis_client.ping()
    REDIS_AVAILABLE = True
    logger.info("✅ Redis connected successfully for whiteboard cache")
except Exception as e:
    logger.warning(f"⚠️ Redis not available: {e}")
    redis_client = None
    REDIS_AVAILABLE = False

# Cache key patterns
CACHE_KEYS = {
    'session': 'whiteboard:session:{meeting_id}',
    'drawings': 'whiteboard:drawings:{meeting_id}',
    'settings': 'whiteboard:settings:{meeting_id}',
    'participants': 'whiteboard:participants:{meeting_id}',
    'history': 'whiteboard:history:{meeting_id}',
    'undo_stack': 'whiteboard:undo:{meeting_id}',
    'redo_stack': 'whiteboard:redo:{meeting_id}',
    'checkpoints': 'whiteboard:checkpoints:{meeting_id}',
    'permissions': 'whiteboard:permissions:{meeting_id}'
}

# Cache TTL (Time To Live) in seconds
CACHE_TTL = {
    'session': 3600,
    'drawings': 7200,
    'settings': 3600,
    'participants': 600,
    'history': 1800,
    'undo_stack': 7200,
    'redo_stack': 7200,
    'checkpoints': 3600,
    'permissions': 3600
}

# ============================================
# WHITEBOARDCACHE CLASS - OPTIMIZED
# ============================================

class WhiteboardCache:
    """Enhanced cache manager with rate-limited logging and improved error handling"""
    
    @staticmethod
    def safe_redis_operation(operation, default_value=None):
        """Wrapper for safe Redis operations with retry logic"""
        if not REDIS_AVAILABLE or not redis_client:
            # Only log warning occasionally
            if log_limiter.should_log("redis_unavailable"):
                logger.warning("⚠️ Redis not available for operation")
            return default_value
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = operation()
                return result
            except (redis.ConnectionError, redis.TimeoutError) as e:
                # Only log retry warnings occasionally
                if log_limiter.should_log(f"redis_retry_{attempt}"):
                    logger.warning(f"⚠️ Redis operation failed (attempt {attempt + 1}): {e}")
                
                if attempt == max_retries - 1:
                    logger.error(f"❌ Redis operation failed after {max_retries} attempts")
                    return default_value
                continue
            except Exception as e:
                logger.error(f"❌ Unexpected Redis error: {e}")
                return default_value
        
        return default_value
    
    # ============================================
    # SESSION MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_session(meeting_id: str) -> Optional[Dict]:
        """Get session data from cache"""
        def operation():
            key = CACHE_KEYS['session'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            return json.loads(data) if data else None
        
        return WhiteboardCache.safe_redis_operation(operation, None)
    
    @staticmethod
    def set_session(meeting_id: str, session_data: Dict) -> bool:
        """Save session data to cache"""
        def operation():
            key = CACHE_KEYS['session'].format(meeting_id=meeting_id)
            redis_client.setex(key, CACHE_TTL['session'], json.dumps(session_data))
            
            # Only log session creation occasionally
            if log_limiter.should_log(f"session_created_{meeting_id}"):
                logger.info(f"✅ Session saved for meeting {meeting_id}")
            
            return True
        
        return WhiteboardCache.safe_redis_operation(operation, False)
    
    # ============================================
    # DRAWINGS MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_drawings(meeting_id: str) -> List[Dict]:
        """Get all drawings from cache - OPTIMIZED LOGGING"""
        def operation():
            key = CACHE_KEYS['drawings'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            result = json.loads(data) if data else []
            
            # ✅ FIXED: Only log occasionally, use DEBUG level
            if log_limiter.should_log(f"get_drawings_{meeting_id}"):
                logger.debug(f"📊 Retrieved {len(result)} drawings from cache")
            
            return result
        
        result = WhiteboardCache.safe_redis_operation(operation, [])
        return result if isinstance(result, list) else []
    
    @staticmethod
    def set_drawings(meeting_id: str, drawings: List[Dict]) -> bool:
        """Save drawings to cache - OPTIMIZED LOGGING"""
        def operation():
            key = CACHE_KEYS['drawings'].format(meeting_id=meeting_id)
            
            # ✅ FIXED: Only log occasionally, use DEBUG level
            if log_limiter.should_log(f"set_drawings_{meeting_id}"):
                logger.debug(f"💾 Saving {len(drawings)} drawings to cache")
            
            redis_client.setex(key, CACHE_TTL['drawings'], json.dumps(drawings))
            
            # ✅ FIXED: Remove verification logging (too verbose)
            # Verification is implicit - if setex succeeds, data is saved
            
            return True
        
        return WhiteboardCache.safe_redis_operation(operation, False)
    
    @staticmethod
    def add_drawing(meeting_id: str, drawing: Dict) -> bool:
        """Add a single drawing to cache - OPTIMIZED LOGGING"""
        try:
            drawings = WhiteboardCache.get_drawings(meeting_id)
            drawings.append(drawing)
            success = WhiteboardCache.set_drawings(meeting_id, drawings)
            
            # ✅ FIXED: Only log important events
            if success and log_limiter.should_log(f"drawing_added_{meeting_id}"):
                logger.debug(f"✅ Drawing added, total: {len(drawings)}")
            
            return success
        except Exception as e:
            logger.error(f"❌ Error adding drawing: {e}")
            return False
    
    @staticmethod
    def clear_drawings(meeting_id: str) -> bool:
        """Clear all drawings - KEEP LOGGING (important event)"""
        success = WhiteboardCache.set_drawings(meeting_id, [])
        if success:
            logger.info(f"🗑️ Cleared all drawings for meeting {meeting_id}")
        return success
    
    # ============================================
    # SETTINGS MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_settings(meeting_id: str) -> Dict:
        """Get whiteboard settings from cache"""
        def operation():
            key = CACHE_KEYS['settings'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            return json.loads(data) if data else {
                'background_color': '#ffffff', 
                'grid_enabled': False
            }
        
        result = WhiteboardCache.safe_redis_operation(operation, {
            'background_color': '#ffffff', 
            'grid_enabled': False
        })
        return result if isinstance(result, dict) else {
            'background_color': '#ffffff', 
            'grid_enabled': False
        }
    
    @staticmethod
    def set_settings(meeting_id: str, settings: Dict) -> bool:
        """Save whiteboard settings to cache"""
        def operation():
            key = CACHE_KEYS['settings'].format(meeting_id=meeting_id)
            redis_client.setex(key, CACHE_TTL['settings'], json.dumps(settings))
            
            # Only log settings changes occasionally
            if log_limiter.should_log(f"settings_updated_{meeting_id}"):
                logger.info(f"⚙️ Settings updated for meeting {meeting_id}")
            
            return True
        
        return WhiteboardCache.safe_redis_operation(operation, False)
    
    # ============================================
    # UNDO STACK MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_undo_stack(meeting_id: str) -> List[Dict]:
        """Get undo stack from cache - OPTIMIZED LOGGING"""
        def operation():
            key = CACHE_KEYS['undo_stack'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            result = json.loads(data) if data else []
            
            # ✅ FIXED: Only log occasionally, use DEBUG level
            if log_limiter.should_log(f"get_undo_{meeting_id}"):
                logger.debug(f"📊 Undo stack: {len(result)} items")
            
            return result
        
        result = WhiteboardCache.safe_redis_operation(operation, [])
        return result if isinstance(result, list) else []
    
    @staticmethod
    def set_undo_stack(meeting_id: str, undo_stack: List[Dict]) -> bool:
        """Save undo stack to cache - OPTIMIZED LOGGING"""
        def operation():
            key = CACHE_KEYS['undo_stack'].format(meeting_id=meeting_id)
            undo_stack_limited = undo_stack[-50:]  # Keep only last 50 actions
            redis_client.setex(key, CACHE_TTL['undo_stack'], json.dumps(undo_stack_limited))
            
            # ✅ FIXED: Only log occasionally, use DEBUG level
            if log_limiter.should_log(f"set_undo_{meeting_id}"):
                logger.debug(f"📝 Undo stack updated: {len(undo_stack_limited)} items (max=50)")
            
            return True
        
        return WhiteboardCache.safe_redis_operation(operation, False)
    
    @staticmethod
    def push_undo_action(meeting_id: str, action: Dict) -> bool:
        """Push action to undo stack - OPTIMIZED LOGGING"""
        try:
            # ✅ FIXED: Only log important undo operations
            if log_limiter.should_log(f"push_undo_{meeting_id}"):
                logger.debug(f"🔖 Pushing undo action for meeting {meeting_id}")
            
            undo_stack = WhiteboardCache.get_undo_stack(meeting_id)
            undo_stack.append(action)
            success = WhiteboardCache.set_undo_stack(meeting_id, undo_stack)
            
            # ✅ FIXED: Remove verification logging (handled by set_undo_stack)
            
            return success
        except Exception as e:
            logger.error(f"❌ Error pushing undo action: {e}")
            return False
    
    @staticmethod
    def pop_undo_action(meeting_id: str) -> Optional[Dict]:
        """Pop action from undo stack - KEEP LOGGING (important event)"""
        try:
            undo_stack = WhiteboardCache.get_undo_stack(meeting_id)
            if not undo_stack:
                logger.warning(f"⚠️ Undo stack is empty for meeting {meeting_id}")
                return None
            
            action = undo_stack.pop()
            WhiteboardCache.set_undo_stack(meeting_id, undo_stack)
            
            # Log undo operations (important for debugging)
            logger.info(f"↩️ Undo action popped. Stack: {len(undo_stack)} items remaining")
            
            return action
        except Exception as e:
            logger.error(f"❌ Error popping undo action: {e}")
            return None
    
    # ============================================
    # REDO STACK MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_redo_stack(meeting_id: str) -> List[Dict]:
        """Get redo stack from cache - OPTIMIZED LOGGING"""
        def operation():
            key = CACHE_KEYS['redo_stack'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            result = json.loads(data) if data else []
            
            # ✅ FIXED: Only log occasionally, use DEBUG level
            if log_limiter.should_log(f"get_redo_{meeting_id}"):
                logger.debug(f"🔁 Redo stack: {len(result)} items")
            
            return result
        
        result = WhiteboardCache.safe_redis_operation(operation, [])
        return result if isinstance(result, list) else []
    
    @staticmethod
    def set_redo_stack(meeting_id: str, redo_stack: List[Dict]) -> bool:
        """Save redo stack to cache - OPTIMIZED LOGGING"""
        def operation():
            key = CACHE_KEYS['redo_stack'].format(meeting_id=meeting_id)
            redo_stack_limited = redo_stack[-50:]  # Keep only last 50 actions
            redis_client.setex(key, CACHE_TTL['redo_stack'], json.dumps(redo_stack_limited))
            
            # ✅ FIXED: Only log occasionally, use DEBUG level
            if log_limiter.should_log(f"set_redo_{meeting_id}"):
                logger.debug(f"🔁 Redo stack updated: {len(redo_stack_limited)} items")
            
            return True
        
        return WhiteboardCache.safe_redis_operation(operation, False)
    
    @staticmethod
    def push_redo_action(meeting_id: str, action: Dict) -> bool:
        """Push action to redo stack - OPTIMIZED LOGGING"""
        try:
            # ✅ FIXED: Only log important redo operations
            if log_limiter.should_log(f"push_redo_{meeting_id}"):
                logger.debug(f"🔖 Pushing redo action for meeting {meeting_id}")
            
            redo_stack = WhiteboardCache.get_redo_stack(meeting_id)
            redo_stack.append(action)
            success = WhiteboardCache.set_redo_stack(meeting_id, redo_stack)
            
            # ✅ FIXED: Remove verification logging
            
            return success
        except Exception as e:
            logger.error(f"❌ Error pushing redo action: {e}")
            return False
    
    @staticmethod
    def pop_redo_action(meeting_id: str) -> Optional[Dict]:
        """Pop action from redo stack - KEEP LOGGING (important event)"""
        try:
            redo_stack = WhiteboardCache.get_redo_stack(meeting_id)
            if not redo_stack:
                logger.warning(f"⚠️ Redo stack is empty for meeting {meeting_id}")
                return None
            
            action = redo_stack.pop()
            WhiteboardCache.set_redo_stack(meeting_id, redo_stack)
            
            # Log redo operations (important for debugging)
            logger.info(f"↪️ Redo action popped. Stack: {len(redo_stack)} items remaining")
            
            return action
        except Exception as e:
            logger.error(f"❌ Error popping redo action: {e}")
            return None
    
    @staticmethod
    def clear_redo_stack(meeting_id: str) -> bool:
        """Clear redo stack - OPTIMIZED LOGGING"""
        success = WhiteboardCache.set_redo_stack(meeting_id, [])
        
        # ✅ FIXED: Only log occasionally
        if success and log_limiter.should_log(f"clear_redo_{meeting_id}"):
            logger.debug(f"🧹 Cleared redo stack for meeting {meeting_id}")
        
        return success
    
    # ============================================
    # CHECKPOINT MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_checkpoints(meeting_id: str) -> List[Dict]:
        """Get checkpoints from cache"""
        def operation():
            key = CACHE_KEYS['checkpoints'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            return json.loads(data) if data else []
        
        result = WhiteboardCache.safe_redis_operation(operation, [])
        return result if isinstance(result, list) else []
    
    @staticmethod
    def save_checkpoint(meeting_id: str, checkpoint_data: Dict) -> bool:
        """Save checkpoint to cache - KEEP LOGGING (important event)"""
        try:
            checkpoints = WhiteboardCache.get_checkpoints(meeting_id)
            checkpoint = {
                'id': str(uuid.uuid4()),
                'name': checkpoint_data.get('name', f'Checkpoint {len(checkpoints) + 1}'),
                'data': checkpoint_data,
                'created_at': timezone.now().isoformat(),
                'meeting_id': meeting_id
            }
            checkpoints.append(checkpoint)
            checkpoints = checkpoints[-20:]  # Keep only last 20 checkpoints
            
            def operation():
                key = CACHE_KEYS['checkpoints'].format(meeting_id=meeting_id)
                redis_client.setex(key, CACHE_TTL['checkpoints'], json.dumps(checkpoints))
                return True
            
            success = WhiteboardCache.safe_redis_operation(operation, False)
            
            # ✅ Keep checkpoint logging (important events)
            if success:
                logger.info(f"📌 Checkpoint saved: {checkpoint['name']} for meeting {meeting_id}")
            
            return success
        except Exception as e:
            logger.error(f"❌ Error saving checkpoint: {e}")
            return False
    
    # ============================================
    # HISTORY MANAGEMENT
    # ============================================
    
    @staticmethod
    def get_history(meeting_id: str) -> List[Dict]:
        """Get history from cache"""
        def operation():
            key = CACHE_KEYS['history'].format(meeting_id=meeting_id)
            data = redis_client.get(key)
            return json.loads(data) if data else []
        
        result = WhiteboardCache.safe_redis_operation(operation, [])
        return result if isinstance(result, list) else []
    
    @staticmethod
    def add_history_entry(meeting_id: str, entry: Dict) -> bool:
        """Add entry to history - OPTIMIZED LOGGING"""
        try:
            history = WhiteboardCache.get_history(meeting_id)
            history.append(entry)
            history = history[-100:]  # Keep only last 100 history entries
            
            def operation():
                key = CACHE_KEYS['history'].format(meeting_id=meeting_id)
                redis_client.setex(key, CACHE_TTL['history'], json.dumps(history))
                return True
            
            success = WhiteboardCache.safe_redis_operation(operation, False)
            
            # ✅ FIXED: Only log occasionally
            if success and log_limiter.should_log(f"history_added_{meeting_id}"):
                logger.debug(f"📜 History entry added for meeting {meeting_id}")
            
            return success
        except Exception as e:
            logger.error(f"❌ Error adding history entry: {e}")
            return False


# ================================
# ENDPOINT IMPLEMENTATIONS
# ================================

@require_http_methods(["POST"])
@csrf_exempt
def create_whiteboard_session(request):
    """Create whiteboard session - OPTIMIZED LOGGING"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        
        # ✅ ONLY log session creation (important events)
        if log_limiter.should_log(f"create_session_{meeting_id}"):
            logger.info(f"🔖 Creating session for meeting {meeting_id}")
        
        if not meeting_id:
            return JsonResponse({'success': False, 'error': 'meeting_id required'}, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        session_data = {
            'session_id': f"wb_{meeting_id}_{int(current_time.timestamp())}",
            'meeting_id': meeting_id,
            'created_by': user_id,
            'created_at': current_time.isoformat(),
            'is_active': True,
            'status': 'created'
        }
        
        try:
            WhiteboardCache.set_drawings(meeting_id, [])
            WhiteboardCache.set_undo_stack(meeting_id, [])
            WhiteboardCache.set_redo_stack(meeting_id, [])
            
            # ✅ Log initialization once
            if log_limiter.should_log(f"init_stacks_{meeting_id}"):
                logger.debug(f"✅ Initialized stacks for {meeting_id}")
        except Exception as e:
            logger.error(f"❌ Cache init error: {e}")
        
        return JsonResponse({
            'success': True,
            'message': 'Session created',
            'session': session_data
        })
        
    except Exception as e:
        logger.error(f"❌ Session creation error: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@require_http_methods(["GET"])
@csrf_exempt
def get_whiteboard_state(request, meeting_id):
    """Get complete whiteboard state - OPTIMIZED LOGGING"""
    try:
        # ✅ ONLY log occasionally, not every call
        if log_limiter.should_log(f"get_state_{meeting_id}"):
            logger.info(f"📊 Getting whiteboard state for meeting {meeting_id}")
        
        drawings = []
        undo_stack = []
        redo_stack = []
        
        try:
            drawings = WhiteboardCache.get_drawings(meeting_id) or []
            undo_stack = WhiteboardCache.get_undo_stack(meeting_id) or []
            redo_stack = WhiteboardCache.get_redo_stack(meeting_id) or []
            
            # ✅ ONLY log summary occasionally
            if log_limiter.should_log(f"cache_summary_{meeting_id}"):
                logger.debug(f"📊 Cache: Drawings={len(drawings)}, Undo={len(undo_stack)}, Redo={len(redo_stack)}")
            
        except Exception as cache_error:
            logger.error(f"❌ Cache error: {cache_error}")
        
        whiteboard_state = {
            'meeting_id': meeting_id,
            'drawings': drawings,
            'total_drawings': len(drawings),
            'can_undo': len(undo_stack) > 0,
            'can_redo': len(redo_stack) > 0,
            'undo_count': len(undo_stack),
            'redo_count': len(redo_stack),
            'can_go_back': len(undo_stack) > 0,
            'can_go_forward': len(redo_stack) > 0,
            'checkpoints': [],
            'current_checkpoint': -1,
            'updated_at': timezone.now().isoformat()
        }
        
        # ✅ ONLY log state transitions occasionally
        if log_limiter.should_log(f"state_transition_{meeting_id}"):
            logger.debug(
                f"✅ Meeting {meeting_id[:8]} → Undo={len(undo_stack)}, Redo={len(redo_stack)}"
            )
        
        return JsonResponse({
            'success': True,
            'whiteboard': whiteboard_state
        })
        
    except Exception as e:
        logger.error(f"❌ Error getting whiteboard state: {e}")
        return JsonResponse({
            'success': False, 
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def add_drawing(request):
    """Add a new drawing with undo/redo support - FIXED FOR FREEHAND"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        drawing_id = data.get('drawing_id', str(uuid.uuid4()))
        tool_type = data.get('tool_type', data.get('tool', 'pen'))

        if not all([meeting_id, user_id]):
            return JsonResponse({'success': False, 'error': 'meeting_id and user_id are required'}, status=400)

        current_time = timezone.now().astimezone(IST_TIMEZONE)
        current_drawings = WhiteboardCache.get_drawings(meeting_id) or []

        # ✅ CRITICAL FIX: Handle both shape and path (freehand) drawings
        drawing_data = data.get('drawing_data', {})
        
        # If drawing_data is empty or invalid, build it from request data
        if not drawing_data or not isinstance(drawing_data, dict):
            # Check if this is a freehand/path drawing
            if tool_type in ['pen', 'pencil', 'brush', 'marker', 'highlighter', 'eraser']:
                # ✅ FREEHAND: Store points
                points = data.get('points', [])
                if not points and 'from' in data and 'to' in data:
                    # Single stroke - create points array
                    points = [data['from'], data['to']]
                
                drawing_data = {
                    'type': 'path',
                    'points': points,
                }
            else:
                # ✅ SHAPE: Store start/end
                drawing_data = {
                    'type': 'shape',
                    'start': data.get('start'),
                    'end': data.get('end'),
                }

        # ✅ Build complete drawing object with ALL data
        drawing = {
            'drawing_id': drawing_id,
            'user_id': user_id,
            'tool_type': tool_type,
            'stroke_color': data.get('stroke_color', data.get('color', '#000000')),
            'stroke_width': data.get('stroke_width', data.get('brushSize', 2)),
            'fill_color': data.get('fill_color'),
            'opacity': data.get('opacity', 1.0),
            'drawing_data': drawing_data,
            'timestamp': current_time.isoformat(),
            'layer_index': data.get('layer_index', 0)
        }

        new_drawings = current_drawings + [drawing]
        
        if not WhiteboardCache.set_drawings(meeting_id, new_drawings):
            return JsonResponse({'success': False, 'error': 'Failed to save drawing'}, status=500)

        # ✅ CRITICAL: Store COMPLETE drawing in undo stack
        undo_action = {
            'type': 'add_drawing',
            'drawing_id': drawing_id,
            'drawing': drawing.copy(),  # Complete drawing object
            'timestamp': current_time.isoformat(),
            'user_id': user_id
        }
        WhiteboardCache.push_undo_action(meeting_id, undo_action)
        WhiteboardCache.clear_redo_stack(meeting_id)

        updated_undo_stack = WhiteboardCache.get_undo_stack(meeting_id) or []
        updated_redo_stack = WhiteboardCache.get_redo_stack(meeting_id) or []

        logger.info(f"✅ Drawing added: {drawing_id} ({tool_type}), Undo stack: {len(updated_undo_stack)}")

        return JsonResponse({
            'success': True,
            'message': 'Drawing added successfully',
            'drawing': drawing,
            'drawings': new_drawings,
            'state': {
                'can_undo': len(updated_undo_stack) > 0,
                'can_redo': len(updated_redo_stack) > 0,
                'undo_count': len(updated_undo_stack),
                'redo_count': len(updated_redo_stack),
                'total_drawings': len(new_drawings)
            }
        })

    except Exception as e:
        logger.error(f"❌ Error in add_drawing: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def undo_action(request):
    """Undo the last action - COMPLETELY FIXED"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')

        logger.info(f"🔄 UNDO START - Meeting: {meeting_id}")

        current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        
        logger.info(f"📊 BEFORE UNDO: {len(current_drawings)} drawings")

        # Get the last action from undo stack
        last_action = WhiteboardCache.pop_undo_action(meeting_id)
        if not last_action:
            logger.warning("⚠️ Nothing to undo")
            return JsonResponse({'success': False, 'error': 'Nothing to undo'}, status=400)

        logger.info(f"🔍 Undo action type: {last_action['type']}")

        if last_action['type'] == 'add_drawing':
            drawing_id_to_remove = last_action.get('drawing_id')
            drawing_to_restore = last_action.get('drawing')
            
            if not drawing_id_to_remove:
                logger.error("❌ No drawing_id in undo action")
                return JsonResponse({'success': False, 'error': 'Invalid undo data'}, status=400)
            
            logger.info(f"🗑️ Removing drawing: {drawing_id_to_remove}")
            
            # ✅ Remove the specific drawing
            new_drawings = [d for d in current_drawings if d.get('drawing_id') != drawing_id_to_remove]
            
            logger.info(f"📊 AFTER UNDO: {len(new_drawings)} drawings (removed 1)")
            
            # ✅ Save the COMPLETE drawing to redo stack
            if drawing_to_restore:
                redo_action = {
                    'type': 'add_drawing',
                    'drawing_id': drawing_id_to_remove,
                    'drawing': drawing_to_restore,  # Complete drawing with all data
                    'timestamp': current_time.isoformat(),
                    'user_id': user_id
                }
                WhiteboardCache.push_redo_action(meeting_id, redo_action)
                logger.info(f"✅ Saved to redo stack: {drawing_id_to_remove}")
            
            WhiteboardCache.set_drawings(meeting_id, new_drawings)
            
        elif last_action['type'] == 'clear_whiteboard':
            previous_drawings = last_action.get('previous_state', [])
            logger.info(f"🔄 Restoring {len(previous_drawings)} drawings after clear")
            
            redo_action = {
                'type': 'clear_whiteboard',
                'previous_state': current_drawings.copy(),
                'timestamp': current_time.isoformat(),
                'user_id': user_id
            }
            WhiteboardCache.push_redo_action(meeting_id, redo_action)
            WhiteboardCache.set_drawings(meeting_id, previous_drawings)
            new_drawings = previous_drawings
        else:
            new_drawings = current_drawings

        updated_undo = WhiteboardCache.get_undo_stack(meeting_id) or []
        updated_redo = WhiteboardCache.get_redo_stack(meeting_id) or []

        logger.info(f"✅ Undo complete - Undo: {len(updated_undo)}, Redo: {len(updated_redo)}")

        return JsonResponse({
            'success': True,
            'message': 'Action undone successfully',
            'undone_action': last_action['type'],
            'drawings': new_drawings,
            'state': {
                'can_undo': len(updated_undo) > 0,
                'can_redo': len(updated_redo) > 0,
                'undo_count': len(updated_undo),
                'redo_count': len(updated_redo),
                'total_drawings': len(new_drawings)
            }
        })

    except Exception as e:
        logger.error(f"❌ UNDO ERROR: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def redo_action(request):
    """Redo the last undone action - COMPLETELY FIXED"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')

        logger.info(f"🔄 REDO START - Meeting: {meeting_id}")

        current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        
        logger.info(f"📊 BEFORE REDO: {len(current_drawings)} drawings")

        # Get the last action from redo stack
        last_redo_action = WhiteboardCache.pop_redo_action(meeting_id)
        if not last_redo_action:
            logger.warning("⚠️ Nothing to redo")
            return JsonResponse({'success': False, 'error': 'Nothing to redo'}, status=400)

        logger.info(f"🔍 Redo action type: {last_redo_action['type']}")

        if last_redo_action['type'] == 'add_drawing':
            drawing_to_restore = last_redo_action.get('drawing')
            
            if not drawing_to_restore:
                logger.error("❌ Cannot redo - missing drawing data")
                return JsonResponse({'success': False, 'error': 'Cannot redo - missing drawing data'}, status=400)
            
            drawing_id = drawing_to_restore.get('drawing_id')
            logger.info(f"➕ Re-adding drawing: {drawing_id}")
            
            # ✅ Add the complete drawing back to the END
            new_drawings = current_drawings + [drawing_to_restore]
            
            logger.info(f"📊 AFTER REDO: {len(new_drawings)} drawings (added 1)")
            
            # ✅ Save to undo stack for potential re-undo
            undo_action = {
                'type': 'add_drawing',
                'drawing_id': drawing_id,
                'drawing': drawing_to_restore.copy(),
                'timestamp': current_time.isoformat(),
                'user_id': user_id
            }
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.set_drawings(meeting_id, new_drawings)
                
        elif last_redo_action['type'] == 'clear_whiteboard':
            logger.info(f"🗑️ Re-clearing whiteboard")
            new_drawings = []
            
            undo_action = {
                'type': 'clear_whiteboard',
                'previous_state': current_drawings.copy(),
                'timestamp': current_time.isoformat(),
                'user_id': user_id
            }
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.set_drawings(meeting_id, new_drawings)
        else:
            new_drawings = current_drawings

        updated_undo = WhiteboardCache.get_undo_stack(meeting_id) or []
        updated_redo = WhiteboardCache.get_redo_stack(meeting_id) or []

        logger.info(f"✅ Redo complete - Undo: {len(updated_undo)}, Redo: {len(updated_redo)}")

        return JsonResponse({
            'success': True,
            'message': 'Action redone successfully',
            'redone_action': last_redo_action['type'],
            'drawings': new_drawings,
            'state': {
                'can_undo': len(updated_undo) > 0,
                'can_redo': len(updated_redo) > 0,
                'undo_count': len(updated_undo),
                'redo_count': len(updated_redo),
                'total_drawings': len(new_drawings)
            }
        })

    except Exception as e:
        logger.error(f"❌ REDO ERROR: {e}")
        logger.error(traceback.format_exc())
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def clear_whiteboard(request):
    """Clear all drawings with undo support - FIXED VERSION"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        
        logger.info(f"🗑️ Clear whiteboard requested for meeting {meeting_id}")
        
        if not meeting_id:
            return JsonResponse({'success': False, 'error': 'meeting_id is required'}, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        current_drawings = []
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id)
        except Exception as get_error:
            logger.error(f"❌ Error getting current drawings: {get_error}")
        
        # CRITICAL FIX: Save current state to undo stack before clearing
        if current_drawings:
            try:
                undo_action = {
                    'type': 'clear_whiteboard',
                    'previous_state': current_drawings.copy(),  # ✅ Save complete state
                    'new_state': [],                            # ✅ Empty state after clear
                    'timestamp': current_time.isoformat(),
                    'user_id': user_id
                }
                WhiteboardCache.push_undo_action(meeting_id, undo_action)
                WhiteboardCache.clear_redo_stack(meeting_id)
            except Exception as undo_error:
                logger.error(f"❌ Error setting up undo for clear: {undo_error}")
        
        # Clear drawings
        try:
            WhiteboardCache.set_drawings(meeting_id, [])
            logger.info(f"✅ Cleared {len(current_drawings)} drawings")
        except Exception as clear_error:
            logger.error(f"❌ Error clearing drawings: {clear_error}")
            return JsonResponse({'success': False, 'error': 'Failed to clear drawings'}, status=500)
        
        # Get updated counts
        updated_undo = WhiteboardCache.get_undo_stack(meeting_id) or []
        updated_redo = WhiteboardCache.get_redo_stack(meeting_id) or []
        
        return JsonResponse({
            'success': True,
            'message': 'Whiteboard cleared successfully',
            'drawings_cleared': len(current_drawings),
            'state': {
                'can_undo': len(updated_undo) > 0,
                'can_redo': len(updated_redo) > 0,
                'undo_count': len(updated_undo),
                'redo_count': len(updated_redo)
            },
            'broadcast_data': {
                'type': 'whiteboard_clear',
                'meeting_id': meeting_id,
                'user_id': user_id,
                'timestamp': current_time.isoformat()
            }
        })
            
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error clearing whiteboard: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({'success': False, 'error': f'Failed to clear whiteboard: {str(e)}'}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def update_whiteboard_settings(request):
    """Update whiteboard settings in cache"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        
        logger.info(f"⚙️ Update settings requested for meeting {meeting_id}, user {user_id}")
        
        if not meeting_id:
            return JsonResponse({'success': False, 'error': 'meeting_id is required'}, status=400)
        
        current_settings = {'background_color': '#ffffff', 'grid_enabled': False}
        try:
            current_settings = WhiteboardCache.get_settings(meeting_id)
        except Exception as get_error:
            logger.error(f"❌ Error getting current settings: {get_error}")
        
        if 'background_color' in data:
            current_settings['background_color'] = data['background_color']
        
        if 'grid_enabled' in data:
            current_settings['grid_enabled'] = bool(data['grid_enabled'])
        
        success = False
        try:
            success = WhiteboardCache.set_settings(meeting_id, current_settings)
        except Exception as set_error:
            logger.error(f"❌ Error setting settings: {set_error}")
        
        if success:
            current_time = timezone.now().astimezone(IST_TIMEZONE)
            try:
                history_entry = {
                    'action': 'update_settings',
                    'user_id': user_id,
                    'settings': current_settings,
                    'timestamp': current_time.isoformat()
                }
                WhiteboardCache.add_history_entry(meeting_id, history_entry)
            except Exception as history_error:
                logger.error(f"❌ Error adding history entry: {history_error}")
            
            logger.info(f"✅ Updated whiteboard settings for meeting {meeting_id}")
            
            return JsonResponse({
                'success': True,
                'message': 'Settings updated successfully',
                'settings': current_settings,
                'broadcast_data': {
                    'type': 'whiteboard_settings_update',
                    'meeting_id': meeting_id,
                    'settings': current_settings,
                    'timestamp': current_time.isoformat()
                }
            })
        else:
            return JsonResponse({'success': False, 'error': 'Failed to update settings in cache'}, status=500)
                
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error updating settings: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({'success': False, 'error': f'Failed to update settings: {str(e)}'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def create_checkpoint(request):
    """Create checkpoint - REDUCED LOGGING"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        checkpoint_name = data.get('name', '')
        
        if not all([meeting_id, user_id]):
            return JsonResponse({'success': False, 'error': 'meeting_id and user_id required'}, status=400)
        
        # ✅ Removed excessive logging - only log on creation
        drawings = []
        settings = {'background_color': '#ffffff', 'grid_enabled': False}
        
        try:
            drawings = WhiteboardCache.get_drawings(meeting_id)
            settings = WhiteboardCache.get_settings(meeting_id)
        except Exception as e:
            logger.error(f"❌ Error getting state for checkpoint: {e}")
        
        checkpoint_data = {
            'meeting_id': meeting_id,
            'drawings': drawings,
            'settings': settings,
            'total_drawings': len(drawings),
            'created_by': user_id,
            'name': checkpoint_name or f'Checkpoint {timezone.now().strftime("%H:%M:%S")}',
            'checkpoint_timestamp': timezone.now().isoformat()
        }
        
        success = WhiteboardCache.save_checkpoint(meeting_id, checkpoint_data)
        
        if success:
            # ✅ ONLY log important checkpoint events
            logger.info(f"✅ Checkpoint created: {checkpoint_data['name']} ({len(drawings)} drawings)")
            
            return JsonResponse({
                'success': True,
                'message': 'Checkpoint created',
                'checkpoint_name': checkpoint_data['name'],
                'drawings_count': len(drawings)
            })
        else:
            return JsonResponse({'success': False, 'error': 'Failed to save'}, status=500)
            
    except Exception as e:
        logger.error(f"❌ Checkpoint error: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def navigate_to_state(request):
    """Navigate to a specific checkpoint state"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        checkpoint_id = data.get('checkpoint_id')
        
        if not all([meeting_id, user_id, checkpoint_id]):
            return JsonResponse({'success': False, 'error': 'meeting_id, user_id, and checkpoint_id are required'}, status=400)
        
        checkpoints = []
        try:
            checkpoints = WhiteboardCache.get_checkpoints(meeting_id)
        except Exception as get_error:
            logger.error(f"❌ Error getting checkpoints: {get_error}")
            return JsonResponse({'success': False, 'error': 'Failed to get checkpoints'}, status=500)
        
        target_checkpoint = None
        for checkpoint in checkpoints:
            if checkpoint['id'] == checkpoint_id:
                target_checkpoint = checkpoint
                break
        
        if not target_checkpoint:
            return JsonResponse({'success': False, 'error': 'Checkpoint not found'}, status=404)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        
        current_drawings = []
        current_settings = {'background_color': '#ffffff', 'grid_enabled': False}
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id)
            current_settings = WhiteboardCache.get_settings(meeting_id)
            
            undo_action = {
                'type': 'navigate_to_checkpoint',
                'previous_state': current_drawings.copy(),
                'previous_settings': current_settings.copy(),
                'timestamp': current_time.isoformat(),
                'user_id': user_id
            }
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.clear_redo_stack(meeting_id)
        except Exception as save_error:
            logger.error(f"❌ Error saving current state for navigation: {save_error}")
        
        checkpoint_data = target_checkpoint['data']
        try:
            WhiteboardCache.set_drawings(meeting_id, checkpoint_data.get('drawings', []))
            WhiteboardCache.set_settings(meeting_id, checkpoint_data.get('settings', {'background_color': '#ffffff', 'grid_enabled': False}))
        except Exception as restore_error:
            logger.error(f"❌ Error restoring checkpoint state: {restore_error}")
            return JsonResponse({'success': False, 'error': 'Failed to restore checkpoint state'}, status=500)
        
        try:
            history_entry = {
                'action': 'navigate_to_checkpoint',
                'user_id': user_id,
                'checkpoint_id': checkpoint_id,
                'checkpoint_name': target_checkpoint.get('name', 'Unknown'),
                'timestamp': current_time.isoformat()
            }
            WhiteboardCache.add_history_entry(meeting_id, history_entry)
        except Exception as history_error:
            logger.error(f"❌ Error adding navigation to history: {history_error}")
        
        logger.info(f"✅ Navigated to checkpoint {checkpoint_id} for meeting {meeting_id}")
        
        updated_drawings = checkpoint_data.get('drawings', [])
        
        return JsonResponse({
            'success': True,
            'message': 'Navigated to checkpoint successfully',
            'checkpoint_name': target_checkpoint.get('name'),
            'drawings_count': len(checkpoint_data.get('drawings', [])),
            'drawings': updated_drawings,
            'broadcast_data': {
                'type': 'whiteboard_navigate_checkpoint',
                'meeting_id': meeting_id,
                'checkpoint_id': checkpoint_id,
                'user_id': user_id,
                'timestamp': current_time.isoformat()
            }
        })
        
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error navigating to checkpoint: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({'success': False, 'error': f'Failed to navigate to checkpoint: {str(e)}'}, status=500)


@require_http_methods(["GET"])
@csrf_exempt
def get_history(request, meeting_id):
    """Get whiteboard history from cache"""
    try:
        logger.info(f"📜 Getting history for meeting {meeting_id}")
        
        history = []
        checkpoints = []
        
        try:
            history = WhiteboardCache.get_history(meeting_id)
            checkpoints = WhiteboardCache.get_checkpoints(meeting_id)
        except Exception as get_error:
            logger.error(f"❌ Error getting history/checkpoints: {get_error}")
        
        return JsonResponse({
            'success': True,
            'history': history,
            'checkpoints': checkpoints,
            'total_history_entries': len(history),
            'total_checkpoints': len(checkpoints)
        })
        
    except Exception as e:
        logger.error(f"❌ Error getting history for meeting {meeting_id}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({'success': False, 'error': f'Failed to get history: {str(e)}'}, status=500)


@require_http_methods(["GET"])
@csrf_exempt
def get_cache_status(request):
    """Get Redis cache status"""
    try:
        if REDIS_AVAILABLE and redis_client:
            try:
                info = redis_client.info()
                key_counts = {}
                
                for key_type, pattern in [
                    ('sessions', 'whiteboard:session:*'),
                    ('drawings', 'whiteboard:drawings:*'),
                    ('settings', 'whiteboard:settings:*'),
                    ('history', 'whiteboard:history:*'),
                    ('checkpoints', 'whiteboard:checkpoints:*'),
                    ('undo_stacks', 'whiteboard:undo:*'),
                    ('redo_stacks', 'whiteboard:redo:*')
                ]:
                    try:
                        key_counts[key_type] = len(redis_client.keys(pattern))
                    except Exception as key_error:
                        logger.error(f"❌ Error counting {key_type} keys: {key_error}")
                        key_counts[key_type] = 0
                
                return JsonResponse({
                    'success': True,
                    'cache_available': True,
                    'redis_info': {
                        'connected_clients': info.get('connected_clients', 0),
                        'used_memory_human': info.get('used_memory_human', '0B'),
                        'total_connections_received': info.get('total_connections_received', 0),
                        'uptime_in_seconds': info.get('uptime_in_seconds', 0)
                    },
                    'cache_keys': key_counts
                })
            except Exception as redis_error:
                logger.error(f"❌ Error getting Redis info: {redis_error}")
                return JsonResponse({
                    'success': True,
                    'cache_available': False,
                    'message': f'Redis error: {str(redis_error)}'
                })
        else:
            return JsonResponse({
                'success': True,
                'cache_available': False,
                'message': 'Redis cache not available'
            })
    except Exception as e:
        logger.error(f"❌ Error getting cache status: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({
            'success': False,
            'error': f'Failed to get cache status: {str(e)}'
        }, status=500)


# FIXED: Add text drawing
@require_http_methods(["POST"])
@csrf_exempt
def add_text(request):
    """Add text to whiteboard with undo/redo support"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        text_id = data.get('text_id', str(uuid.uuid4()))
        
        logger.info(f"Adding text {text_id} for meeting {meeting_id}")
        
        if not all([meeting_id, user_id]):
            return JsonResponse({
                'success': False,
                'error': 'meeting_id and user_id are required'
            }, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        
        # Initialize variables
        current_drawings = []
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        except Exception as cache_error:
            logger.error(f"Cache error: {cache_error}")
        
        # Create text object
        text_drawing = {
            'drawing_id': text_id,
            'user_id': user_id,
            'tool_type': 'text',
            'text_content': data.get('text', ''),
            'x': data.get('x', 0),
            'y': data.get('y', 0),
            'font_size': data.get('font_size', 16),
            'font_family': data.get('font_family', 'Arial'),
            'font_weight': data.get('font_weight', 'normal'),
            'font_style': data.get('font_style', 'normal'),
            'text_align': data.get('text_align', 'left'),
            'stroke_color': data.get('color', '#000000'),
            'opacity': data.get('opacity', 1.0),
            'width': data.get('width'),
            'height': data.get('height'),
            'timestamp': current_time.isoformat(),
            'layer_index': data.get('layer_index', 0)
        }
        
        # Save for undo
        undo_action = {
            'type': 'add_text',
            'drawing_id': text_id,
            'previous_state': current_drawings.copy() if current_drawings else [],
            'timestamp': current_time.isoformat(),
            'user_id': user_id
        }
        
        try:
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.clear_redo_stack(meeting_id)
        except Exception as stack_error:
            logger.error(f"Error updating stacks: {stack_error}")
        
        # Add text drawing
        success = False
        try:
            success = WhiteboardCache.add_drawing(meeting_id, text_drawing)
        except Exception as add_error:
            logger.error(f"Error adding text: {add_error}")
        
        if success:
            logger.info(f"Added text {text_id} to meeting {meeting_id}")
            
            return JsonResponse({
                'success': True,
                'message': 'Text added successfully',
                'drawing': text_drawing,
                'broadcast_data': {
                    'type': 'whiteboard_text_add',
                    'meeting_id': meeting_id,
                    'drawing': text_drawing,
                    'timestamp': current_time.isoformat()
                }
            })
        else:
            return JsonResponse({'success': False, 'error': 'Failed to save text'}, status=500)
            
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"Error adding text: {e}")
        return JsonResponse({'success': False, 'error': f'Failed to add text: {str(e)}'}, status=500)


# FIXED: Update text
@require_http_methods(["POST"])
@csrf_exempt
def update_text(request):
    """Update existing text on whiteboard"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        text_id = data.get('text_id')
        
        if not all([meeting_id, user_id, text_id]):
            return JsonResponse({
                'success': False,
                'error': 'meeting_id, user_id, and text_id are required'
            }, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        current_drawings = []
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        except Exception as cache_error:
            logger.error(f"Cache error: {cache_error}")
            return JsonResponse({'success': False, 'error': 'Failed to get drawings'}, status=500)
        
        # Find and update the text
        text_found = False
        updated_drawings = []
        
        for drawing in current_drawings:
            if drawing.get('drawing_id') == text_id and drawing.get('tool_type') == 'text':
                text_found = True
                # Update text properties
                updated_drawing = drawing.copy()
                if 'text' in data:
                    updated_drawing['text_content'] = data['text']
                if 'x' in data:
                    updated_drawing['x'] = data['x']
                if 'y' in data:
                    updated_drawing['y'] = data['y']
                if 'font_size' in data:
                    updated_drawing['font_size'] = data['font_size']
                if 'color' in data:
                    updated_drawing['stroke_color'] = data['color']
                if 'width' in data:
                    updated_drawing['width'] = data['width']
                if 'height' in data:
                    updated_drawing['height'] = data['height']
                
                updated_drawing['timestamp'] = current_time.isoformat()
                updated_drawings.append(updated_drawing)
            else:
                updated_drawings.append(drawing)
        
        if not text_found:
            return JsonResponse({'success': False, 'error': 'Text not found'}, status=404)
        
        # Save for undo
        undo_action = {
            'type': 'update_text',
            'drawing_id': text_id,
            'previous_state': current_drawings.copy(),
            'timestamp': current_time.isoformat(),
            'user_id': user_id
        }
        
        try:
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.clear_redo_stack(meeting_id)
            WhiteboardCache.set_drawings(meeting_id, updated_drawings)
        except Exception as update_error:
            logger.error(f"Error updating text: {update_error}")
            return JsonResponse({'success': False, 'error': 'Failed to update text'}, status=500)
        
        return JsonResponse({
            'success': True,
            'message': 'Text updated successfully',
            'broadcast_data': {
                'type': 'whiteboard_text_update',
                'meeting_id': meeting_id,
                'text_id': text_id,
                'timestamp': current_time.isoformat()
            }
        })
        
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"Error updating text: {e}")
        return JsonResponse({'success': False, 'error': f'Failed to update text: {str(e)}'}, status=500)


# FIXED: Select items (text or drawings)
@require_http_methods(["POST"])
@csrf_exempt
def select_items(request):
    """Select multiple items on whiteboard (for moving, deleting, etc.)"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        selection_type = data.get('selection_type', 'rectangle')  # rectangle or lasso
        selected_ids = data.get('selected_ids', [])
        
        if not all([meeting_id, user_id]):
            return JsonResponse({
                'success': False,
                'error': 'meeting_id and user_id are required'
            }, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        current_drawings = []
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        except Exception as cache_error:
            logger.error(f"Cache error: {cache_error}")
        
        # Filter selected items
        selected_items = [
            drawing for drawing in current_drawings 
            if drawing.get('drawing_id') in selected_ids
        ]
        
        return JsonResponse({
            'success': True,
            'message': f'Selected {len(selected_items)} items',
            'selected_items': selected_items,
            'selected_count': len(selected_items),
            'selection_type': selection_type
        })
        
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"Error selecting items: {e}")
        return JsonResponse({'success': False, 'error': f'Failed to select items: {str(e)}'}, status=500)


# FIXED: Delete selected items
@require_http_methods(["POST"])
@csrf_exempt
def delete_selected_items(request):
    """Delete selected items from whiteboard"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        selected_ids = data.get('selected_ids', [])
        
        if not all([meeting_id, user_id]):
            return JsonResponse({
                'success': False,
                'error': 'meeting_id and user_id are required'
            }, status=400)
        
        if not selected_ids:
            return JsonResponse({'success': False, 'error': 'No items selected'}, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        current_drawings = []
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        except Exception as cache_error:
            logger.error(f"Cache error: {cache_error}")
            return JsonResponse({'success': False, 'error': 'Failed to get drawings'}, status=500)
        
        # Save for undo
        undo_action = {
            'type': 'delete_selected',
            'selected_ids': selected_ids,
            'previous_state': current_drawings.copy(),
            'timestamp': current_time.isoformat(),
            'user_id': user_id
        }
        
        # Remove selected items
        updated_drawings = [
            drawing for drawing in current_drawings 
            if drawing.get('drawing_id') not in selected_ids
        ]
        
        deleted_count = len(current_drawings) - len(updated_drawings)
        
        try:
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.clear_redo_stack(meeting_id)
            WhiteboardCache.set_drawings(meeting_id, updated_drawings)
        except Exception as delete_error:
            logger.error(f"Error deleting items: {delete_error}")
            return JsonResponse({'success': False, 'error': 'Failed to delete items'}, status=500)
        
        logger.info(f"Deleted {deleted_count} items from meeting {meeting_id}")
        
        return JsonResponse({
            'success': True,
            'message': f'Deleted {deleted_count} items',
            'deleted_count': deleted_count,
            'broadcast_data': {
                'type': 'whiteboard_items_deleted',
                'meeting_id': meeting_id,
                'deleted_ids': selected_ids,
                'user_id': user_id,
                'timestamp': current_time.isoformat()
            }
        })
        
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"Error deleting items: {e}")
        return JsonResponse({'success': False, 'error': f'Failed to delete items: {str(e)}'}, status=500)


# FIXED: Move selected items
@require_http_methods(["POST"])
@csrf_exempt
def move_selected_items(request):
    """Move selected items on whiteboard"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        selected_ids = data.get('selected_ids', [])
        delta_x = data.get('delta_x', 0)
        delta_y = data.get('delta_y', 0)
        
        if not all([meeting_id, user_id]):
            return JsonResponse({
                'success': False,
                'error': 'meeting_id and user_id are required'
            }, status=400)
        
        if not selected_ids:
            return JsonResponse({'success': False, 'error': 'No items selected'}, status=400)
        
        current_time = timezone.now().astimezone(IST_TIMEZONE)
        current_drawings = []
        
        try:
            current_drawings = WhiteboardCache.get_drawings(meeting_id) or []
        except Exception as cache_error:
            logger.error(f"Cache error: {cache_error}")
            return JsonResponse({'success': False, 'error': 'Failed to get drawings'}, status=500)
        
        # Save for undo
        undo_action = {
            'type': 'move_selected',
            'selected_ids': selected_ids,
            'previous_state': current_drawings.copy(),
            'delta_x': delta_x,
            'delta_y': delta_y,
            'timestamp': current_time.isoformat(),
            'user_id': user_id
        }
        
        # Move selected items
        updated_drawings = []
        for drawing in current_drawings:
            if drawing.get('drawing_id') in selected_ids:
                moved_drawing = drawing.copy()
                
                # Handle different drawing types
                if drawing.get('tool_type') == 'text':
                    moved_drawing['x'] = drawing.get('x', 0) + delta_x
                    moved_drawing['y'] = drawing.get('y', 0) + delta_y
                else:
                    # For path-based drawings, move all points
                    if 'drawing_data' in drawing and 'points' in drawing['drawing_data']:
                        moved_drawing['drawing_data'] = drawing['drawing_data'].copy()
                        moved_drawing['drawing_data']['points'] = [
                            {'x': pt['x'] + delta_x, 'y': pt['y'] + delta_y}
                            for pt in drawing['drawing_data']['points']
                        ]
                    elif 'points' in drawing:
                        moved_drawing['points'] = [
                            {'x': pt['x'] + delta_x, 'y': pt['y'] + delta_y}
                            for pt in drawing['points']
                        ]
                
                moved_drawing['timestamp'] = current_time.isoformat()
                updated_drawings.append(moved_drawing)
            else:
                updated_drawings.append(drawing)
        
        try:
            WhiteboardCache.push_undo_action(meeting_id, undo_action)
            WhiteboardCache.clear_redo_stack(meeting_id)
            WhiteboardCache.set_drawings(meeting_id, updated_drawings)
        except Exception as move_error:
            logger.error(f"Error moving items: {move_error}")
            return JsonResponse({'success': False, 'error': 'Failed to move items'}, status=500)
        
        return JsonResponse({
            'success': True,
            'message': f'Moved {len(selected_ids)} items',
            'broadcast_data': {
                'type': 'whiteboard_items_moved',
                'meeting_id': meeting_id,
                'moved_ids': selected_ids,
                'delta_x': delta_x,
                'delta_y': delta_y,
                'user_id': user_id,
                'timestamp': current_time.isoformat()
            }
        })
        
    except json.JSONDecodeError as e:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"Error moving items: {e}")
        return JsonResponse({'success': False, 'error': f'Failed to move items: {str(e)}'}, status=500)