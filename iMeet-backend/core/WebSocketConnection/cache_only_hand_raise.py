# cache_only_hand_raise.py - Ephemeral Hand Raise System (Cache Only, No Database)
from core.WebSocketConnection import enhanced_logging_config
import redis
import json
import os
import time
import logging
from datetime import datetime, timedelta
from django.utils import timezone
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.db import connection

# Configure logging
logger = logging.getLogger('cache_hand_raise')

# Redis configuration for cache-only hand raise
CACHE_HAND_RAISE_CONFIG = {
    'host': os.getenv("CACHE_HAND_RAISE_HOST", "localhost"),
    'port': int(os.getenv("CACHE_HAND_RAISE_PORT", 6379)),
    'db': int(os.getenv("CACHE_HAND_RAISE_DB", 4)),
    'decode_responses': os.getenv("CACHE_HAND_RAISE_DECODE_RESPONSES", "True") == "True",
    'socket_timeout': int(os.getenv("CACHE_HAND_RAISE_SOCKET_TIMEOUT", 5)),
    'socket_connect_timeout': int(os.getenv("CACHE_HAND_RAISE_CONNECT_TIMEOUT", 5)),
    'retry_on_timeout': os.getenv("CACHE_HAND_RAISE_RETRY_ON_TIMEOUT", "True") == "True"
}

# Initialize cache-only hand raise Redis client
try:
    cache_hand_raise_redis = redis.Redis(**CACHE_HAND_RAISE_CONFIG)
    cache_hand_raise_redis.ping()
    logger.info("✅ Cache-only hand raise Redis connected successfully")
except Exception as e:
    logger.warning(f"⚠ Cache-only hand raise Redis not available: {e}")
    cache_hand_raise_redis = None

# Cache settings - hand raises exist ONLY during meeting
CACHE_SETTINGS = {
    'MAX_HANDS_PER_ROOM': 200,      # Limit raised hands per room
    'ACKNOWLEDGMENT_TTL': 30,       # Acknowledged hands expire in 30 seconds
    'CLEANUP_IMMEDIATE': True,      # Delete immediately when meeting ends
    'MAX_USER_NAME_LENGTH': 100,    # Maximum user name length
    'HAND_RAISE_QUEUE': True        # Maintain order of raised hands
}

# Hand raise status constants
HAND_STATUS = {
    'WAITING': 'waiting',
    'ACKNOWLEDGED': 'acknowledged',
    'DENIED': 'denied',
    'LOWERED': 'lowered'
}

class CacheOnlyHandRaiseManager:
    """Manages hand raises ONLY in cache - NO database storage"""
    
    def __init__(self):
        self.redis_client = cache_hand_raise_redis
        self.enabled = cache_hand_raise_redis is not None
        logger.info(f"✋ Cache-only hand raise manager initialized: {'Enabled' if self.enabled else 'Disabled'}")
    
    def _get_hands_key(self, meeting_id):
        """Generate Redis key for raised hands"""
        return f"cache_hands:{meeting_id}"
    
    def _get_queue_key(self, meeting_id):
        """Generate Redis key for hand raise queue (order)"""
        return f"cache_hand_queue:{meeting_id}"
    
    def _get_meeting_status_key(self, meeting_id):
        """Generate Redis key for meeting status"""
        return f"cache_hand_meeting:{meeting_id}"
    
    def _get_acknowledged_key(self, meeting_id):
        """Generate Redis key for acknowledged hands (temporary)"""
        return f"cache_acknowledged:{meeting_id}"
    
    def start_meeting_hand_raise(self, meeting_id):
        """Initialize hand raise system for a meeting"""
        if not self.enabled:
            logger.warning("Redis not available for hand raise")
            return False
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            meeting_data = {
                'meeting_id': meeting_id,
                'started_at': timezone.now().isoformat(),
                'status': 'active',
                'total_hands_raised': 0,
                'total_acknowledged': 0,
                'total_denied': 0
            }
            
            # Set meeting as active (no expiration until meeting ends)
            self.redis_client.set(status_key, json.dumps(meeting_data))
            
            logger.info(f"✋ Started cache-only hand raise for meeting: {meeting_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start meeting hand raise: {e}")
            return False
    
    def raise_hand(self, meeting_id, user_id, user_name, participant_identity=None):
        """Raise hand (cache only)"""
        if not self.enabled:
            return False
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            queue_key = self._get_queue_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Check if meeting is active
            meeting_status = self.redis_client.get(status_key)
            if not meeting_status:
                logger.warning(f"Meeting {meeting_id} not active, cannot raise hand")
                return False
            
            # Check if user already has hand raised
            if self.redis_client.hexists(hands_key, user_id):
                logger.warning(f"User {user_id} already has hand raised in meeting {meeting_id}")
                return False
            
            # Prepare hand raise data
            hand_data = {
                'user_id': str(user_id),
                'user_name': user_name[:CACHE_SETTINGS['MAX_USER_NAME_LENGTH']],
                'participant_identity': participant_identity,
                'timestamp': timezone.now().isoformat(),
                'status': HAND_STATUS['WAITING'],
                'raised_at': time.time()
            }
            
            # Add to hands hash
            self.redis_client.hset(hands_key, user_id, json.dumps(hand_data))
            
            # Add to queue (for ordering)
            self.redis_client.rpush(queue_key, user_id)
            
            # Limit queue size
            self.redis_client.ltrim(queue_key, -CACHE_SETTINGS['MAX_HANDS_PER_ROOM'], -1)
            
            # Update statistics
            status_data = json.loads(meeting_status)
            status_data['total_hands_raised'] = status_data.get('total_hands_raised', 0) + 1
            status_data['last_hand_at'] = timezone.now().isoformat()
            self.redis_client.set(status_key, json.dumps(status_data))
            
            logger.info(f"✋ User {user_id} ({user_name}) raised hand in meeting {meeting_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to raise hand: {e}")
            return False
    
    def lower_hand(self, meeting_id, user_id):
        """Lower hand (remove from cache)"""
        if not self.enabled:
            return False
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            queue_key = self._get_queue_key(meeting_id)
            
            # Check if hand is raised
            if not self.redis_client.hexists(hands_key, user_id):
                logger.warning(f"User {user_id} does not have hand raised in meeting {meeting_id}")
                return False
            
            # Remove from hands hash
            self.redis_client.hdel(hands_key, user_id)
            
            # Remove from queue
            self.redis_client.lrem(queue_key, 0, user_id)
            
            logger.info(f"✋ User {user_id} lowered hand in meeting {meeting_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to lower hand: {e}")
            return False
    
    def acknowledge_hand(self, meeting_id, host_user_id, participant_user_id, action='acknowledge'):
        """Host acknowledges or denies a raised hand"""
        if not self.enabled:
            return False
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            queue_key = self._get_queue_key(meeting_id)
            ack_key = self._get_acknowledged_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Get hand data
            hand_data_str = self.redis_client.hget(hands_key, participant_user_id)
            if not hand_data_str:
                logger.warning(f"No raised hand found for user {participant_user_id}")
                return False
            
            hand_data = json.loads(hand_data_str)
            hand_data['status'] = HAND_STATUS['ACKNOWLEDGED'] if action == 'acknowledge' else HAND_STATUS['DENIED']
            hand_data['acknowledged_by'] = host_user_id
            hand_data['acknowledged_at'] = timezone.now().isoformat()
            hand_data['action'] = action
            
            if action == 'acknowledge':
                # Move to acknowledged (temporary storage)
                self.redis_client.hset(ack_key, participant_user_id, json.dumps(hand_data))
                self.redis_client.expire(ack_key, CACHE_SETTINGS['ACKNOWLEDGMENT_TTL'])
            
            # Remove from active hands
            self.redis_client.hdel(hands_key, participant_user_id)
            self.redis_client.lrem(queue_key, 0, participant_user_id)
            
            # Update statistics
            meeting_status = self.redis_client.get(status_key)
            if meeting_status:
                status_data = json.loads(meeting_status)
                if action == 'acknowledge':
                    status_data['total_acknowledged'] = status_data.get('total_acknowledged', 0) + 1
                else:
                    status_data['total_denied'] = status_data.get('total_denied', 0) + 1
                status_data['last_action_at'] = timezone.now().isoformat()
                self.redis_client.set(status_key, json.dumps(status_data))
            
            logger.info(f"✅ Host {host_user_id} {action}d hand from {participant_user_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to acknowledge hand: {e}")
            return False
    
    def clear_all_hands(self, meeting_id, host_user_id):
        """Clear all raised hands"""
        if not self.enabled:
            return 0
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            queue_key = self._get_queue_key(meeting_id)
            
            # Get count before clearing
            hands_count = self.redis_client.hlen(hands_key)
            
            # Clear all hands
            self.redis_client.delete(hands_key, queue_key)
            
            logger.info(f"🧹 Host {host_user_id} cleared {hands_count} hands in meeting {meeting_id}")
            return hands_count
            
        except Exception as e:
            logger.error(f"❌ Failed to clear all hands: {e}")
            return 0
    
    def get_raised_hands(self, meeting_id):
        """Get all currently raised hands in order"""
        if not self.enabled:
            return []
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            queue_key = self._get_queue_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Check if meeting is active
            if not self.redis_client.exists(status_key):
                logger.warning(f"Meeting {meeting_id} not found in cache")
                return []
            
            # Get ordered list of user IDs
            ordered_user_ids = self.redis_client.lrange(queue_key, 0, -1)
            
            raised_hands = []
            for user_id in ordered_user_ids:
                hand_data_str = self.redis_client.hget(hands_key, user_id)
                if hand_data_str:
                    try:
                        hand_data = json.loads(hand_data_str)
                        raised_hands.append({
                            'id': f"hand_{user_id}_{int(hand_data.get('raised_at', time.time()))}",
                            'user_id': hand_data['user_id'],
                            'user': {
                                'user_id': hand_data['user_id'],
                                'full_name': hand_data['user_name'],
                                'profile_picture': None
                            },
                            'timestamp': hand_data['timestamp'],
                            'status': hand_data['status'],
                            'participant_identity': hand_data.get('participant_identity'),
                            'raised_at': hand_data.get('raised_at')
                        })
                    except json.JSONDecodeError:
                        continue
            
            return raised_hands
            
        except Exception as e:
            logger.error(f"❌ Failed to get raised hands: {e}")
            return []
    
    def get_hands_count(self, meeting_id):
        """Get total count of raised hands"""
        if not self.enabled:
            return 0
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            return self.redis_client.hlen(hands_key)
        except Exception as e:
            logger.error(f"❌ Failed to get hands count: {e}")
            return 0
    
    def is_hand_raised(self, meeting_id, user_id):
        """Check if user has hand raised"""
        if not self.enabled:
            return False
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            return self.redis_client.hexists(hands_key, user_id)
        except Exception as e:
            logger.error(f"❌ Failed to check hand status: {e}")
            return False
    
    def end_meeting_hand_raise(self, meeting_id):
        """End meeting and DELETE ALL hand raise data immediately"""
        if not self.enabled:
            return False
        
        try:
            hands_key = self._get_hands_key(meeting_id)
            queue_key = self._get_queue_key(meeting_id)
            ack_key = self._get_acknowledged_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Get final stats before deletion
            final_stats = self.get_meeting_stats(meeting_id)
            
            # DELETE ALL hand raise data for this meeting
            deleted_keys = self.redis_client.delete(
                hands_key,
                queue_key,
                ack_key,
                status_key
            )
            
            logger.info(f"🗑 DELETED all hand raise data for meeting {meeting_id}")
            logger.info(f"   - Final stats: {final_stats}")
            logger.info(f"   - Redis keys deleted: {deleted_keys}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to end meeting hand raise: {e}")
            return False
    
    def is_meeting_active(self, meeting_id):
        """Check if meeting hand raise is active"""
        if not self.enabled:
            return False
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            return self.redis_client.exists(status_key)
        except Exception as e:
            logger.error(f"❌ Failed to check meeting status: {e}")
            return False
    
    def get_meeting_stats(self, meeting_id):
        """Get meeting hand raise statistics"""
        if not self.enabled:
            return None
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            status_data = self.redis_client.get(status_key)
            
            if status_data:
                data = json.loads(status_data)
                current_hands_count = self.get_hands_count(meeting_id)
                
                return {
                    'meeting_id': meeting_id,
                    'started_at': data.get('started_at'),
                    'total_hands_raised': data.get('total_hands_raised', 0),
                    'total_acknowledged': data.get('total_acknowledged', 0),
                    'total_denied': data.get('total_denied', 0),
                    'current_raised_hands': current_hands_count,
                    'last_hand_at': data.get('last_hand_at'),
                    'last_action_at': data.get('last_action_at'),
                    'status': data.get('status', 'unknown'),
                    'storage_type': 'cache_only'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get meeting stats: {e}")
            return None

# Initialize the cache-only hand raise manager
cache_hand_raise_manager = CacheOnlyHandRaiseManager()

# API ENDPOINTS

@require_http_methods(["POST"])
@csrf_exempt
def start_meeting_hand_raise(request):
    """Start hand raise system for a meeting (initialize cache)"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        
        if not meeting_id:
            return JsonResponse({'error': 'meeting_id is required'}, status=400)
        
        success = cache_hand_raise_manager.start_meeting_hand_raise(meeting_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Meeting hand raise started (cache-only)',
                'meeting_id': meeting_id,
                'storage_type': 'cache_only',
                'auto_delete_on_end': True
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to start meeting hand raise'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error starting meeting hand raise: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def raise_hand(request):
    """Raise hand (cache only, no database)"""
    try:
        data = json.loads(request.body)
        
        # Validate required fields
        required_fields = ['meeting_id', 'user_id', 'user_name']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        action = data.get('action', 'raise')  # 'raise' or 'lower'
        
        # Check if meeting is active
        if not cache_hand_raise_manager.is_meeting_active(data['meeting_id']):
            return JsonResponse({
                'error': 'Meeting hand raise not active or meeting has ended'
            }, status=400)
        
        if action == 'raise':
            # Check if hand is already raised
            if cache_hand_raise_manager.is_hand_raised(data['meeting_id'], data['user_id']):
                return JsonResponse({
                    'error': 'Hand is already raised'
                }, status=400)
            
            success = cache_hand_raise_manager.raise_hand(
                data['meeting_id'],
                data['user_id'],
                data['user_name'],
                data.get('participant_identity')
            )
        else:  # lower
            success = cache_hand_raise_manager.lower_hand(
                data['meeting_id'],
                data['user_id']
            )
        
        if success:
            # Prepare data for LiveKit broadcast
            livekit_data = {
                'type': 'hand_raise',
                'action': action,
                'user_id': data['user_id'],
                'user_name': data['user_name'],
                'participant_identity': data.get('participant_identity'),
                'timestamp': timezone.now().isoformat(),
                'meeting_id': data['meeting_id']
            }
            
            return JsonResponse({
                'success': True,
                'message': f'Hand {action}d successfully',
                'send_via_livekit': True,
                'broadcast_to_all': True,
                'data': livekit_data,
                'storage_type': 'cache_only',
                'will_delete_on_meeting_end': True
            }, status=200)
        else:
            return JsonResponse({
                'error': f'Failed to {action} hand'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error in raise_hand: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def acknowledge_hand(request):
    """Host acknowledges or denies a raised hand"""
    try:
        data = json.loads(request.body)
        
        required_fields = ['meeting_id', 'host_user_id', 'participant_user_id']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        # Verify host permissions
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT Host_ID FROM tbl_Meetings WHERE ID = %s", [data['meeting_id']])
                row = cursor.fetchone()
                if not row or str(row[0]) != str(data['host_user_id']):
                    return JsonResponse({'error': 'Only the host can acknowledge hands'}, status=403)
        except Exception as e:
            logger.warning(f"Could not verify host permissions: {e}")
        
        # Check if meeting is active
        if not cache_hand_raise_manager.is_meeting_active(data['meeting_id']):
            return JsonResponse({
                'error': 'Meeting not active'
            }, status=400)
        
        action = data.get('action', 'acknowledge')  # 'acknowledge' or 'deny'
        
        success = cache_hand_raise_manager.acknowledge_hand(
            data['meeting_id'],
            data['host_user_id'],
            data['participant_user_id'],
            action
        )
        
        if success:
            # Prepare data for LiveKit broadcast
            livekit_data = {
                'type': 'hand_acknowledgment',
                'action': action,
                'participant_user_id': data['participant_user_id'],
                'participant_name': data.get('participant_name', 'Participant'),
                'host_user_id': data['host_user_id'],
                'timestamp': timezone.now().isoformat(),
                'meeting_id': data['meeting_id']
            }
            
            return JsonResponse({
                'success': True,
                'message': f'Hand {action}d successfully',
                'send_via_livekit': True,
                'broadcast_to_all': True,
                'data': livekit_data,
                'storage_type': 'cache_only'
            }, status=200)
        else:
            return JsonResponse({
                'error': f'Failed to {action} hand'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error acknowledging hand: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def clear_all_hands(request):
    """Host clears all raised hands"""
    try:
        data = json.loads(request.body)
        
        required_fields = ['meeting_id', 'host_user_id']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        # Verify host permissions
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT Host_ID FROM tbl_Meetings WHERE ID = %s", [data['meeting_id']])
                row = cursor.fetchone()
                if not row or str(row[0]) != str(data['host_user_id']):
                    return JsonResponse({'error': 'Only the host can clear all hands'}, status=403)
        except Exception as e:
            logger.warning(f"Could not verify host permissions: {e}")
        
        # Check if meeting is active
        if not cache_hand_raise_manager.is_meeting_active(data['meeting_id']):
            return JsonResponse({
                'error': 'Meeting not active'
            }, status=400)
        
        cleared_count = cache_hand_raise_manager.clear_all_hands(
            data['meeting_id'],
            data['host_user_id']
        )
        
        # Prepare data for LiveKit broadcast
        livekit_data = {
            'type': 'clear_all_hands',
            'host_user_id': data['host_user_id'],
            'cleared_count': cleared_count,
            'timestamp': timezone.now().isoformat(),
            'meeting_id': data['meeting_id']
        }
        
        return JsonResponse({
            'success': True,
            'message': f'All hands cleared successfully ({cleared_count} hands)',
            'send_via_livekit': True,
            'broadcast_to_all': True,
            'data': livekit_data,
            'cleared_count': cleared_count,
            'storage_type': 'cache_only'
        }, status=200)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error clearing all hands: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_raised_hands(request, meeting_id):
    """Get all currently raised hands from cache"""
    try:
        # Check if meeting is active
        if not cache_hand_raise_manager.is_meeting_active(meeting_id):
            return JsonResponse({
                'success': False,
                'error': 'Meeting not found or has ended',
                'raised_hands': [],
                'note': 'Hand raise data is automatically deleted when meeting ends'
            }, status=404)
        
        # Get raised hands from cache
        raised_hands = cache_hand_raise_manager.get_raised_hands(meeting_id)
        hands_count = cache_hand_raise_manager.get_hands_count(meeting_id)
        
        return JsonResponse({
            'success': True,
            'meeting_id': meeting_id,
            'raised_hands': raised_hands,
            'total_count': hands_count,
            'storage_type': 'cache_only',
            'warning': 'Hand raise data will be deleted when meeting ends'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting raised hands: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def sync_hand_raise_state(request):
    """Sync hand raise state for new participants"""
    try:
        data = json.loads(request.body)
        
        required_fields = ['meeting_id', 'user_id']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        # Check if meeting is active
        if not cache_hand_raise_manager.is_meeting_active(data['meeting_id']):
            return JsonResponse({
                'error': 'Meeting not active'
            }, status=400)
        
        # Get current raised hands
        current_hands = cache_hand_raise_manager.get_raised_hands(data['meeting_id'])
        
        # Prepare data for LiveKit broadcast (send only to requesting user)
        livekit_data = {
            'type': 'hand_state_sync',
            'requesting_user_id': data['user_id'],
            'current_hands': [
                {
                    'user_id': hand['user_id'],
                    'user_name': hand['user']['full_name'],
                    'timestamp': hand['timestamp'],
                    'status': hand['status']
                }
                for hand in current_hands
            ],
            'timestamp': timezone.now().isoformat(),
            'meeting_id': data['meeting_id']
        }
        
        return JsonResponse({
            'success': True,
            'message': 'Hand raise state synced',
            'send_via_livekit': True,
            'broadcast_to_participant': data['user_id'],
            'data': livekit_data,
            'hands_count': len(current_hands),
            'storage_type': 'cache_only'
        }, status=200)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error syncing hand raise state: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def end_meeting_hand_raise(request):
    """End meeting and DELETE ALL hand raise data immediately"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        
        if not meeting_id:
            return JsonResponse({'error': 'meeting_id is required'}, status=400)
        
        # Get stats before deletion
        stats = cache_hand_raise_manager.get_meeting_stats(meeting_id)
        
        # Delete ALL hand raise data
        success = cache_hand_raise_manager.end_meeting_hand_raise(meeting_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Meeting ended - ALL hand raise data DELETED',
                'meeting_id': meeting_id,
                'deleted_stats': stats,
                'storage_type': 'cache_only'
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to end meeting hand raise'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error ending meeting hand raise: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_meeting_hand_raise_stats(request, meeting_id):
    """Get meeting hand raise statistics"""
    try:
        stats = cache_hand_raise_manager.get_meeting_stats(meeting_id)
        
        if stats:
            return JsonResponse({
                'success': True,
                'stats': stats
            }, status=200)
        else:
            return JsonResponse({
                'success': False,
                'error': 'Meeting not found or has ended',
                'note': 'Hand raise data is deleted when meeting ends'
            }, status=404)
        
    except Exception as e:
        logger.error(f"❌ Error getting meeting hand raise stats: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def check_hand_status(request, meeting_id, user_id):
    """Check if a specific user has their hand raised"""
    try:
        # Check if meeting is active
        if not cache_hand_raise_manager.is_meeting_active(meeting_id):
            return JsonResponse({
                'success': True,
                'hand_raised': False,
                'note': 'Meeting not active'
            }, status=200)
        
        hand_raised = cache_hand_raise_manager.is_hand_raised(meeting_id, user_id)
        
        return JsonResponse({
            'success': True,
            'meeting_id': meeting_id,
            'user_id': user_id,
            'hand_raised': hand_raised,
            'storage_type': 'cache_only'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error checking hand status: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

# URL patterns for cache-only hand raise
urlpatterns = [
    path('api/cache-hand-raise/start/', start_meeting_hand_raise, name='start_meeting_hand_raise'),
    path('api/cache-hand-raise/raise/', raise_hand, name='raise_hand'),
    path('api/cache-hand-raise/acknowledge/', acknowledge_hand, name='acknowledge_hand'),
    path('api/cache-hand-raise/clear-all/', clear_all_hands, name='clear_all_hands'),
    path('api/cache-hand-raise/hands/<str:meeting_id>/', get_raised_hands, name='get_raised_hands'),
    path('api/cache-hand-raise/sync/', sync_hand_raise_state, name='sync_hand_raise_state'),
    path('api/cache-hand-raise/end/', end_meeting_hand_raise, name='end_meeting_hand_raise'),
    path('api/cache-hand-raise/stats/<str:meeting_id>/', get_meeting_hand_raise_stats, name='get_meeting_hand_raise_stats'),
    path('api/cache-hand-raise/check/<str:meeting_id>/<str:user_id>/', check_hand_status, name='check_hand_status'),
]   