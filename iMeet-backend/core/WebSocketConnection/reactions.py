# cache_only_reactions.py - Ephemeral Reactions System (Cache Only, No Database)

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
logger = logging.getLogger('cache_reactions')

# Redis configuration for cache-only reactions
CACHE_REACTIONS_CONFIG = {
    'host': os.getenv("CACHE_REACTIONS_HOST", "localhost"),
    'port': int(os.getenv("CACHE_REACTIONS_PORT", 6379)),
    'db': int(os.getenv("CACHE_REACTIONS_DB", 5)),
    'decode_responses': os.getenv("CACHE_REACTIONS_DECODE_RESPONSES", "True") == "True",
    'socket_timeout': int(os.getenv("CACHE_REACTIONS_SOCKET_TIMEOUT", 5)),
    'socket_connect_timeout': int(os.getenv("CACHE_REACTIONS_CONNECT_TIMEOUT", 5)),
    'retry_on_timeout': os.getenv("CACHE_REACTIONS_RETRY_ON_TIMEOUT", "True") == "True"
}

# Initialize cache-only reactions Redis client
try:
    cache_reactions_redis = redis.Redis(**CACHE_REACTIONS_CONFIG)
    cache_reactions_redis.ping()
    logger.info("✅ Cache-only reactions Redis connected successfully")
except Exception as e:
    logger.warning(f"⚠ Cache-only reactions Redis not available: {e}")
    cache_reactions_redis = None

# Cache settings - reactions exist ONLY during meeting
CACHE_SETTINGS = {
    'MAX_REACTIONS_PER_ROOM': 1000,     # Limit reactions per room
    'REACTION_DISPLAY_TTL': 5,          # Individual reactions disappear after 5 seconds
    'CLEANUP_IMMEDIATE': True,          # Delete immediately when meeting ends
    'MAX_USER_NAME_LENGTH': 100,        # Maximum user name length
    'REACTION_BURST_LIMIT': 10,         # Max reactions per user per 10 seconds
    'AUTO_START_ON_FIRST_REACTION': True # Auto-start reactions if not initialized
}

# Allowed reactions - only these 7 emojis
ALLOWED_REACTIONS = {
    '👍': 'thumbs_up',
    '👎': 'thumbs_down', 
    '❤️': 'heart',
    '👏': 'clap',
    '🎉': 'celebration',
    '🔥': 'fire',
    '🤔': 'thinking'
}

class CacheOnlyReactionsManager:
    """Manages reactions ONLY in cache - NO database storage"""
    
    def __init__(self):
        self.redis_client = cache_reactions_redis
        self.enabled = cache_reactions_redis is not None
        logger.info(f"😊 Cache-only reactions manager initialized: {'Enabled' if self.enabled else 'Disabled'}")
    
    def _get_reactions_key(self, meeting_id):
        """Generate Redis key for active reactions"""
        return f"cache_reactions:{meeting_id}"
    
    def _get_reaction_counts_key(self, meeting_id):
        """Generate Redis key for reaction counts by type"""
        return f"cache_reaction_counts:{meeting_id}"
    
    def _get_user_reactions_key(self, meeting_id):
        """Generate Redis key for user reaction history (for burst limiting)"""
        return f"cache_user_reactions:{meeting_id}"
    
    def _get_meeting_status_key(self, meeting_id):
        """Generate Redis key for meeting status"""
        return f"cache_reaction_meeting:{meeting_id}"
    
    def start_meeting_reactions(self, meeting_id):
        """Initialize reactions system for a meeting"""
        if not self.enabled:
            logger.warning("Redis not available for reactions")
            return False
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Check if already initialized
            if self.redis_client.exists(status_key):
                logger.info(f"😊 Reactions already initialized for meeting: {meeting_id}")
                return True
            
            meeting_data = {
                'meeting_id': meeting_id,
                'started_at': timezone.now().isoformat(),
                'status': 'active',
                'total_reactions': 0,
                'reactions_by_type': {reaction_type: 0 for reaction_type in ALLOWED_REACTIONS.values()}
            }
            
            # Set meeting as active (no expiration until meeting ends)
            self.redis_client.set(status_key, json.dumps(meeting_data))
            
            # Initialize reaction counts
            counts_key = self._get_reaction_counts_key(meeting_id)
            for emoji, reaction_type in ALLOWED_REACTIONS.items():
                self.redis_client.hset(counts_key, reaction_type, 0)
            
            logger.info(f"😊 Started cache-only reactions for meeting: {meeting_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start meeting reactions: {e}")
            return False
    
    def add_reaction(self, meeting_id, user_id, user_name, emoji, participant_identity=None):
        """Add reaction (cache only) - Auto-start if not initialized"""
        if not self.enabled:
            return False
        
        try:
            # Validate emoji
            if emoji not in ALLOWED_REACTIONS:
                logger.warning(f"Invalid reaction emoji: {emoji}")
                return False
            
            # Auto-start reactions if not initialized
            if not self.is_meeting_active(meeting_id):
                if CACHE_SETTINGS['AUTO_START_ON_FIRST_REACTION']:
                    logger.info(f"🚀 Auto-starting reactions for meeting: {meeting_id}")
                    if not self.start_meeting_reactions(meeting_id):
                        logger.error(f"❌ Failed to auto-start reactions for meeting: {meeting_id}")
                        return False
                else:
                    logger.warning(f"Meeting {meeting_id} not active, cannot add reaction")
                    return False
            
            reactions_key = self._get_reactions_key(meeting_id)
            counts_key = self._get_reaction_counts_key(meeting_id)
            user_reactions_key = self._get_user_reactions_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Check burst limit for user
            current_time = time.time()
            recent_reactions = self.redis_client.lrange(user_reactions_key, 0, -1)
            
            # Count recent reactions from this user
            recent_count = 0
            for reaction_data_str in recent_reactions:
                try:
                    reaction_data = json.loads(reaction_data_str)
                    if (reaction_data.get('user_id') == str(user_id) and 
                        current_time - reaction_data.get('timestamp', 0) < 10):  # 10 seconds
                        recent_count += 1
                except json.JSONDecodeError:
                    continue
            
            if recent_count >= CACHE_SETTINGS['REACTION_BURST_LIMIT']:
                logger.warning(f"User {user_id} hit reaction burst limit")
                return False
            
            # Prepare reaction data
            reaction_data = {
                'id': f"reaction_{user_id}_{int(current_time * 1000)}",
                'user_id': str(user_id),
                'user_name': user_name[:CACHE_SETTINGS['MAX_USER_NAME_LENGTH']],
                'participant_identity': participant_identity,
                'emoji': emoji,
                'reaction_type': ALLOWED_REACTIONS[emoji],
                'timestamp': current_time,
                'created_at': timezone.now().isoformat(),
                'expires_at': current_time + CACHE_SETTINGS['REACTION_DISPLAY_TTL']
            }
            
            # Add to active reactions list
            self.redis_client.lpush(reactions_key, json.dumps(reaction_data))
            
            # Limit list size to prevent memory overflow
            self.redis_client.ltrim(reactions_key, 0, CACHE_SETTINGS['MAX_REACTIONS_PER_ROOM'] - 1)
            
            # Add to user reactions for burst limiting
            self.redis_client.lpush(user_reactions_key, json.dumps(reaction_data))
            self.redis_client.ltrim(user_reactions_key, 0, 100)  # Keep recent history
            
            # Increment reaction count by type
            reaction_type = ALLOWED_REACTIONS[emoji]
            self.redis_client.hincrby(counts_key, reaction_type, 1)
            
            # Update meeting statistics
            meeting_status = self.redis_client.get(status_key)
            if meeting_status:
                status_data = json.loads(meeting_status)
                status_data['total_reactions'] = status_data.get('total_reactions', 0) + 1
                status_data['reactions_by_type'][reaction_type] = status_data['reactions_by_type'].get(reaction_type, 0) + 1
                status_data['last_reaction_at'] = timezone.now().isoformat()
                self.redis_client.set(status_key, json.dumps(status_data))
            
            logger.info(f"😊 User {user_id} ({user_name}) added reaction {emoji} in meeting {meeting_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to add reaction: {e}")
            return False
    
    def get_active_reactions(self, meeting_id):
        """Get currently active reactions (not expired)"""
        if not self.enabled:
            return []
        
        try:
            reactions_key = self._get_reactions_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Auto-start if meeting exists but reactions not initialized
            if not self.redis_client.exists(status_key):
                # Check if this is a valid meeting by trying to verify in database
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT ID FROM tbl_Meetings WHERE ID = %s", [meeting_id])
                        if cursor.fetchone():
                            logger.info(f"🚀 Auto-starting reactions for existing meeting: {meeting_id}")
                            self.start_meeting_reactions(meeting_id)
                        else:
                            logger.warning(f"Meeting {meeting_id} not found in database")
                            return []
                except Exception as e:
                    logger.warning(f"Could not verify meeting in database: {e}")
                    return []
            
            # Get all reactions
            raw_reactions = self.redis_client.lrange(reactions_key, 0, -1)
            
            active_reactions = []
            current_time = time.time()
            expired_reactions = []
            
            for raw_reaction in raw_reactions:
                try:
                    reaction_data = json.loads(raw_reaction)
                    
                    # Check if reaction has expired
                    if current_time > reaction_data.get('expires_at', 0):
                        expired_reactions.append(raw_reaction)
                        continue
                    
                    # Add to active list
                    active_reactions.append({
                        'id': reaction_data['id'],
                        'user_id': reaction_data['user_id'],
                        'user': {
                            'user_id': reaction_data['user_id'],
                            'full_name': reaction_data['user_name'],
                            'profile_picture': None
                        },
                        'emoji': reaction_data['emoji'],
                        'reaction_type': reaction_data['reaction_type'],
                        'timestamp': reaction_data['created_at'],
                        'participant_identity': reaction_data.get('participant_identity'),
                        'expires_at': reaction_data['expires_at'],
                        'time_remaining': max(0, reaction_data['expires_at'] - current_time)
                    })
                    
                except json.JSONDecodeError:
                    expired_reactions.append(raw_reaction)
                    continue
            
            # Clean up expired reactions
            if expired_reactions:
                for expired in expired_reactions:
                    self.redis_client.lrem(reactions_key, 0, expired)
                logger.info(f"🧹 Cleaned up {len(expired_reactions)} expired reactions")
            
            # Sort by timestamp (newest first)
            active_reactions.sort(key=lambda x: x['timestamp'], reverse=True)
            
            return active_reactions
            
        except Exception as e:
            logger.error(f"❌ Failed to get active reactions: {e}")
            return []
    
    def get_reaction_counts(self, meeting_id):
        """Get total reaction counts by type"""
        if not self.enabled:
            return {}
        
        try:
            counts_key = self._get_reaction_counts_key(meeting_id)
            raw_counts = self.redis_client.hgetall(counts_key)
            
            # Convert to proper format with emojis
            reaction_counts = {}
            for reaction_type, count in raw_counts.items():
                # Find emoji for this reaction type
                emoji = None
                for e, rt in ALLOWED_REACTIONS.items():
                    if rt == reaction_type:
                        emoji = e
                        break
                
                if emoji:
                    reaction_counts[emoji] = {
                        'emoji': emoji,
                        'reaction_type': reaction_type,
                        'count': int(count)
                    }
            
            return reaction_counts
            
        except Exception as e:
            logger.error(f"❌ Failed to get reaction counts: {e}")
            return {}
    
    def get_reactions_count(self, meeting_id):
        """Get total count of all reactions"""
        if not self.enabled:
            return 0
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            status_data = self.redis_client.get(status_key)
            
            if status_data:
                data = json.loads(status_data)
                return data.get('total_reactions', 0)
            
            return 0
            
        except Exception as e:
            logger.error(f"❌ Failed to get reactions count: {e}")
            return 0
    
    def clear_all_reactions(self, meeting_id, host_user_id):
        """Clear all reactions"""
        if not self.enabled:
            return 0
        
        try:
            reactions_key = self._get_reactions_key(meeting_id)
            
            # Get count before clearing
            reactions_count = self.redis_client.llen(reactions_key)
            
            # Clear all reactions
            self.redis_client.delete(reactions_key)
            
            logger.info(f"🧹 Host {host_user_id} cleared {reactions_count} reactions in meeting {meeting_id}")
            return reactions_count
            
        except Exception as e:
            logger.error(f"❌ Failed to clear all reactions: {e}")
            return 0
    
    def end_meeting_reactions(self, meeting_id):
        """End meeting and DELETE ALL reaction data immediately"""
        if not self.enabled:
            return False
        
        try:
            reactions_key = self._get_reactions_key(meeting_id)
            counts_key = self._get_reaction_counts_key(meeting_id)
            user_reactions_key = self._get_user_reactions_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            # Get final stats before deletion
            final_stats = self.get_meeting_stats(meeting_id)
            
            # DELETE ALL reaction data for this meeting
            deleted_keys = self.redis_client.delete(
                reactions_key,
                counts_key,
                user_reactions_key,
                status_key
            )
            
            logger.info(f"🗑 DELETED all reaction data for meeting {meeting_id}")
            logger.info(f"   - Final stats: {final_stats}")
            logger.info(f"   - Redis keys deleted: {deleted_keys}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to end meeting reactions: {e}")
            return False
    
    def is_meeting_active(self, meeting_id):
        """Check if meeting reactions is active"""
        if not self.enabled:
            return False
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            return self.redis_client.exists(status_key)
        except Exception as e:
            logger.error(f"❌ Failed to check meeting status: {e}")
            return False
    
    def get_meeting_stats(self, meeting_id):
        """Get meeting reactions statistics"""
        if not self.enabled:
            return None
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            status_data = self.redis_client.get(status_key)
            
            if status_data:
                data = json.loads(status_data)
                current_active_count = len(self.get_active_reactions(meeting_id))
                reaction_counts = self.get_reaction_counts(meeting_id)
                
                return {
                    'meeting_id': meeting_id,
                    'started_at': data.get('started_at'),
                    'total_reactions': data.get('total_reactions', 0),
                    'current_active_reactions': current_active_count,
                    'reactions_by_type': data.get('reactions_by_type', {}),
                    'reaction_counts': reaction_counts,
                    'last_reaction_at': data.get('last_reaction_at'),
                    'status': data.get('status', 'unknown'),
                    'allowed_reactions': list(ALLOWED_REACTIONS.keys()),
                    'storage_type': 'cache_only'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get meeting stats: {e}")
            return None

# Initialize the cache-only reactions manager
cache_reactions_manager = CacheOnlyReactionsManager()

# API ENDPOINTS

@require_http_methods(["POST"])
@csrf_exempt
def start_meeting_reactions(request):
    """Start reactions system for a meeting (initialize cache)"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        
        if not meeting_id:
            return JsonResponse({'error': 'meeting_id is required'}, status=400)
        
        success = cache_reactions_manager.start_meeting_reactions(meeting_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Meeting reactions started (cache-only)',
                'meeting_id': meeting_id,
                'storage_type': 'cache_only',
                'allowed_reactions': list(ALLOWED_REACTIONS.keys()),
                'reaction_display_duration': CACHE_SETTINGS['REACTION_DISPLAY_TTL'],
                'auto_delete_on_end': True,
                'auto_start_enabled': CACHE_SETTINGS['AUTO_START_ON_FIRST_REACTION']
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to start meeting reactions'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error starting meeting reactions: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def add_reaction(request):
    """
    Add reaction with INSTANT broadcast - Zero delay
    Includes: Auto-start, burst limiting, validation, LiveKit data, error handling
    """
    try:
        # Step 1: Parse JSON
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON format'}, status=400)
        
        # Step 2: Validate required fields
        required_fields = ['meeting_id', 'user_id', 'user_name', 'emoji']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}',
                'missing_fields': missing_fields
            }, status=400)
        
        meeting_id = data['meeting_id']
        user_id = str(data['user_id'])
        user_name = data['user_name'][:CACHE_SETTINGS['MAX_USER_NAME_LENGTH']].strip()
        emoji = data['emoji']
        participant_identity = data.get('participant_identity', f"user_{user_id}")
        
        # Step 3: Validate emoji
        if emoji not in ALLOWED_REACTIONS:
            return JsonResponse({
                'error': f'Invalid reaction. Allowed reactions: {", ".join(ALLOWED_REACTIONS.keys())}',
                'allowed_reactions': list(ALLOWED_REACTIONS.keys())
            }, status=400)
        
        # Step 4: INSTANT - Generate reaction ID and timestamp immediately
        reaction_timestamp = time.time()
        reaction_timestamp_ms = int(reaction_timestamp * 1000)  # Milliseconds
        reaction_id = f"reaction_{user_id}_{reaction_timestamp_ms}"
        reaction_type = ALLOWED_REACTIONS[emoji]
        
        # Step 5: Check if meeting is active (auto-start if needed)
        if not cache_reactions_manager.is_meeting_active(meeting_id):
            if CACHE_SETTINGS['AUTO_START_ON_FIRST_REACTION']:
                logger.info(f"🚀 AUTO-STARTING reactions for meeting: {meeting_id}")
                auto_start_success = cache_reactions_manager.start_meeting_reactions(meeting_id)
                if not auto_start_success:
                    return JsonResponse({
                        'error': 'Failed to initialize meeting reactions',
                        'detail': 'Redis may be unavailable'
                    }, status=503)
            else:
                return JsonResponse({
                    'error': 'Meeting reactions not initialized',
                    'detail': 'Please start meeting reactions first'
                }, status=400)
        
        # Step 6: Check burst limit (prevent spam)
        try:
            user_reactions_key = cache_reactions_manager._get_user_reactions_key(meeting_id)
            recent_reactions = cache_reactions_manager.redis_client.lrange(user_reactions_key, 0, -1)
            
            # Count recent reactions from this user in last 10 seconds
            recent_count = 0
            burst_window = 10  # seconds
            for reaction_data_str in recent_reactions:
                try:
                    reaction_data = json.loads(reaction_data_str)
                    if (reaction_data.get('user_id') == user_id and 
                        reaction_timestamp - reaction_data.get('timestamp', 0) < burst_window):
                        recent_count += 1
                except (json.JSONDecodeError, KeyError):
                    continue
            
            if recent_count >= CACHE_SETTINGS['REACTION_BURST_LIMIT']:
                logger.warning(f"⚠️ User {user_id} hit reaction burst limit")
                return JsonResponse({
                    'error': 'Reaction rate limit exceeded',
                    'detail': f'Maximum {CACHE_SETTINGS["REACTION_BURST_LIMIT"]} reactions per {burst_window} seconds',
                    'retry_after': burst_window
                }, status=429)
        except Exception as burst_check_error:
            # Don't fail the request if burst check fails
            logger.warning(f"⚠️ Burst check failed (non-critical): {burst_check_error}")
        
        # Step 7: INSTANT - Prepare LiveKit broadcast data (BEFORE storage)
        livekit_data = {
            'type': 'reaction_notification',  # CRITICAL: Must match frontend listener
            'action': 'add',
            'user_id': user_id,
            'user_name': user_name,
            'participant_identity': participant_identity,
            'emoji': emoji,
            'reaction_type': reaction_type,
            'timestamp': reaction_timestamp_ms,  # Milliseconds for frontend
            'created_at': timezone.now().isoformat(),
            'meeting_id': meeting_id,
            'display_duration': CACHE_SETTINGS['REACTION_DISPLAY_TTL'],
            'reaction_id': reaction_id,
            'expires_at': reaction_timestamp + CACHE_SETTINGS['REACTION_DISPLAY_TTL']
        }
        
        # Step 8: Store in Redis (non-blocking - after response prepared)
        redis_storage_success = False
        storage_warning = None
        
        try:
            # Prepare complete reaction data for storage
            reaction_data = {
                'id': reaction_id,
                'user_id': user_id,
                'user_name': user_name,
                'participant_identity': participant_identity,
                'emoji': emoji,
                'reaction_type': reaction_type,
                'timestamp': reaction_timestamp,
                'timestamp_ms': reaction_timestamp_ms,
                'created_at': timezone.now().isoformat(),
                'expires_at': reaction_timestamp + CACHE_SETTINGS['REACTION_DISPLAY_TTL'],
                'meeting_id': meeting_id
            }
            
            # Get Redis keys
            reactions_key = cache_reactions_manager._get_reactions_key(meeting_id)
            counts_key = cache_reactions_manager._get_reaction_counts_key(meeting_id)
            user_reactions_key = cache_reactions_manager._get_user_reactions_key(meeting_id)
            status_key = cache_reactions_manager._get_meeting_status_key(meeting_id)
            
            # Add to active reactions list
            cache_reactions_manager.redis_client.lpush(reactions_key, json.dumps(reaction_data))
            
            # Limit list size to prevent memory overflow
            cache_reactions_manager.redis_client.ltrim(
                reactions_key, 
                0, 
                CACHE_SETTINGS['MAX_REACTIONS_PER_ROOM'] - 1
            )
            
            # Add to user reactions for burst limiting
            cache_reactions_manager.redis_client.lpush(user_reactions_key, json.dumps(reaction_data))
            cache_reactions_manager.redis_client.ltrim(user_reactions_key, 0, 100)  # Keep recent history
            
            # Increment reaction count by type
            cache_reactions_manager.redis_client.hincrby(counts_key, reaction_type, 1)
            
            # Update meeting statistics
            meeting_status = cache_reactions_manager.redis_client.get(status_key)
            if meeting_status:
                try:
                    status_data = json.loads(meeting_status)
                    status_data['total_reactions'] = status_data.get('total_reactions', 0) + 1
                    status_data['reactions_by_type'] = status_data.get('reactions_by_type', {})
                    status_data['reactions_by_type'][reaction_type] = \
                        status_data['reactions_by_type'].get(reaction_type, 0) + 1
                    status_data['last_reaction_at'] = timezone.now().isoformat()
                    status_data['last_reaction_user'] = user_name
                    status_data['last_reaction_emoji'] = emoji
                    cache_reactions_manager.redis_client.set(status_key, json.dumps(status_data))
                except (json.JSONDecodeError, KeyError) as status_error:
                    logger.warning(f"⚠️ Failed to update meeting status: {status_error}")
            
            redis_storage_success = True
            logger.info(f"✅ User {user_id} ({user_name}) added reaction {emoji} in meeting {meeting_id}")
            
        except redis.exceptions.RedisError as redis_error:
            # Redis storage failed - log but don't fail the request
            logger.error(f"❌ Redis storage failed: {redis_error}")
            storage_warning = 'Reaction sent but storage may have failed'
            # Still continue - broadcast can work without Redis
            
        except Exception as storage_error:
            logger.error(f"❌ Unexpected storage error: {storage_error}")
            storage_warning = 'Reaction sent but storage encountered an error'
        
        # Step 9: INSTANT - Return response immediately for zero-latency
        response_data = {
            'success': True,
            'message': 'Reaction added - broadcast immediately',
            'reaction_id': reaction_id,
            'broadcast_data': livekit_data,  # Complete LiveKit data for broadcasting
            'should_broadcast': True,
            'broadcast_immediately': True,  # Signal to frontend to broadcast NOW
            'storage_type': 'cache_only',
            'display_duration_seconds': CACHE_SETTINGS['REACTION_DISPLAY_TTL'],
            'timestamp': reaction_timestamp_ms,
            'redis_storage_success': redis_storage_success,
            'will_auto_clear': True,
            'auto_started': not cache_reactions_manager.is_meeting_active(meeting_id)
        }
        
        # Add storage warning if Redis failed
        if storage_warning:
            response_data['storage_warning'] = storage_warning
        
        return JsonResponse(response_data, status=200)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Critical error in add_reaction: {e}", exc_info=True)
        return JsonResponse({
            'error': 'Internal server error',
            'detail': str(e)
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def clear_all_reactions(request):
    """Host clears all reactions"""
    try:
        data = json.loads(request.body)
        
        required_fields = ['meeting_id', 'host_user_id']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        # Verify host permissions (optional check)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT Host_ID FROM tbl_Meetings WHERE ID = %s", [data['meeting_id']])
                row = cursor.fetchone()
                if not row or str(row[0]) != str(data['host_user_id']):
                    return JsonResponse({'error': 'Only the host can clear all reactions'}, status=403)
        except Exception as e:
            logger.warning(f"Could not verify host permissions: {e}")
        
        # Auto-start reactions if needed
        if not cache_reactions_manager.is_meeting_active(data['meeting_id']):
            cache_reactions_manager.start_meeting_reactions(data['meeting_id'])
        
        cleared_count = cache_reactions_manager.clear_all_reactions(
            data['meeting_id'],
            data['host_user_id']
        )
        
        # Prepare data for LiveKit broadcast
        livekit_data = {
            'type': 'clear_all_reactions',
            'host_user_id': data['host_user_id'],
            'cleared_count': cleared_count,
            'timestamp': timezone.now().isoformat(),
            'meeting_id': data['meeting_id']
        }
        
        return JsonResponse({
            'success': True,
            'message': f'All reactions cleared successfully ({cleared_count} reactions)',
            'broadcast_data': livekit_data,  # Data to broadcast via LiveKit
            'should_broadcast': True,
            'cleared_count': cleared_count,
            'storage_type': 'cache_only'
        }, status=200)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error clearing all reactions: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_active_reactions(request, meeting_id):
    """Get currently active reactions from cache - Auto-starts if needed"""
    try:
        # Get active reactions from cache (auto-starts if needed)
        active_reactions = cache_reactions_manager.get_active_reactions(meeting_id)
        reaction_counts = cache_reactions_manager.get_reaction_counts(meeting_id)
        total_reactions = cache_reactions_manager.get_reactions_count(meeting_id)
        is_active = cache_reactions_manager.is_meeting_active(meeting_id)
        
        return JsonResponse({
            'success': True,
            'meeting_id': meeting_id,
            'active_reactions': active_reactions,
            'reaction_counts': reaction_counts,
            'total_reactions_count': total_reactions,
            'allowed_reactions': list(ALLOWED_REACTIONS.keys()),
            'storage_type': 'cache_only',
            'display_duration_seconds': CACHE_SETTINGS['REACTION_DISPLAY_TTL'],
            'is_active': is_active,
            'auto_start_enabled': CACHE_SETTINGS['AUTO_START_ON_FIRST_REACTION'],
            'note': 'Reaction data will be deleted when meeting ends'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting active reactions: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_reaction_counts(request, meeting_id):
    """Get reaction counts by type from cache"""
    try:
        # Auto-start if needed
        if not cache_reactions_manager.is_meeting_active(meeting_id):
            cache_reactions_manager.start_meeting_reactions(meeting_id)
        
        # Get reaction counts from cache
        reaction_counts = cache_reactions_manager.get_reaction_counts(meeting_id)
        total_reactions = cache_reactions_manager.get_reactions_count(meeting_id)
        
        return JsonResponse({
            'success': True,
            'meeting_id': meeting_id,
            'reaction_counts': reaction_counts,
            'total_reactions_count': total_reactions,
            'allowed_reactions': list(ALLOWED_REACTIONS.keys()),
            'storage_type': 'cache_only'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting reaction counts: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def end_meeting_reactions(request):
    """End meeting and DELETE ALL reaction data immediately"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        
        if not meeting_id:
            return JsonResponse({'error': 'meeting_id is required'}, status=400)
        
        # Get stats before deletion
        stats = cache_reactions_manager.get_meeting_stats(meeting_id)
        
        # Delete ALL reaction data
        success = cache_reactions_manager.end_meeting_reactions(meeting_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Meeting ended - ALL reaction data DELETED',
                'meeting_id': meeting_id,
                'deleted_stats': stats,
                'storage_type': 'cache_only'
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to end meeting reactions'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error ending meeting reactions: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_meeting_reactions_stats(request, meeting_id):
    """Get meeting reactions statistics"""
    try:
        stats = cache_reactions_manager.get_meeting_stats(meeting_id)
        
        if stats:
            return JsonResponse({
                'success': True,
                'stats': stats
            }, status=200)
        else:
            # Auto-start and return empty stats
            cache_reactions_manager.start_meeting_reactions(meeting_id)
            stats = cache_reactions_manager.get_meeting_stats(meeting_id)
            
            return JsonResponse({
                'success': True,
                'stats': stats,
                'auto_started': True
            }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting meeting reactions stats: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_allowed_reactions(request):
    """Get list of allowed reaction emojis"""
    try:
        return JsonResponse({
            'success': True,
            'allowed_reactions': [
                {
                    'emoji': emoji,
                    'reaction_type': reaction_type,
                    'description': reaction_type.replace('_', ' ').title()
                }
                for emoji, reaction_type in ALLOWED_REACTIONS.items()
            ],
            'display_duration_seconds': CACHE_SETTINGS['REACTION_DISPLAY_TTL'],
            'burst_limit_per_10_seconds': CACHE_SETTINGS['REACTION_BURST_LIMIT'],
            'auto_start_enabled': CACHE_SETTINGS['AUTO_START_ON_FIRST_REACTION']
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting allowed reactions: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

# URL patterns for cache-only reactions
urlpatterns = [
    path('api/cache-reactions/start/', start_meeting_reactions, name='start_meeting_reactions'),
    path('api/cache-reactions/add/', add_reaction, name='add_reaction'),
    path('api/cache-reactions/clear-all/', clear_all_reactions, name='clear_all_reactions'),
    path('api/cache-reactions/active/<str:meeting_id>/', get_active_reactions, name='get_active_reactions'),
    path('api/cache-reactions/counts/<str:meeting_id>/', get_reaction_counts, name='get_reaction_counts'),
    path('api/cache-reactions/end/', end_meeting_reactions, name='end_meeting_reactions'),
    path('api/cache-reactions/stats/<str:meeting_id>/', get_meeting_reactions_stats, name='get_meeting_reactions_stats'),
    path('api/cache-reactions/allowed/', get_allowed_reactions, name='get_allowed_reactions'),
]