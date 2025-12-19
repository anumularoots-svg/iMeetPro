# ALL YOUR EXISTING CODE + LiveKit functionality
from core.WebSocketConnection import enhanced_logging_config
from django.db import connection, transaction, models
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from core.AI_Attendance.Attendance import start_attendance_tracking, stop_attendance_tracking
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.utils import timezone
from django.contrib.auth.models import User
from datetime import datetime, timedelta 
from django.db.utils import ProgrammingError, OperationalError
import jwt as jwt_decode
import pytz
import ssl
import jwt
from datetime import datetime, timedelta
import json
import logging
import uuid
import aiopg
import redis
from django.core.mail import send_mail
from django.conf import settings
import os
from tempfile import TemporaryDirectory
import time
from typing import Optional, Dict, List, Any
import asyncio  # Add this import
import aiohttp
import concurrent.futures
import asyncio
import aiohttp
import concurrent.futures
import threading
import time
import ssl
import json
import logging
import threading
from django.core.mail import EmailMessage
from django.conf import settings
from typing import Optional, Dict, List, Any
import pytz
import urllib3
import random
import string
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import logging
from .notifications import (
    ensure_notification_tables,
    create_meeting_notifications,
    schedule_meeting_reminders,
    create_host_notification,
    _get_host_email_by_id,
)
logger = logging.getLogger('meetings') 


DATABASE_URL = os.getenv("DATABASE_URL")

# Add to your existing imports in meetings.py
try:
    from core.AI_Attendance.Attendance import start_attendance_tracking, stop_attendance_tracking
    ATTENDANCE_ENABLED = True
    print("✅ Attendance module loaded successfully")
except ImportError as e:
    ATTENDANCE_ENABLED = False
    print(f"⚠️ Attendance module not available: {e}")

# LiveKit Integration
# FIXED: Check LiveKit availability FIRST before using it
try:
    from livekit.api import LiveKitAPI, CreateRoomRequest, ListRoomsRequest, ListParticipantsRequest
    from livekit.api import AccessToken, VideoGrants
    LIVEKIT_AVAILABLE = True
    logging.info("✅ LiveKit SDK imported successfully")
except ImportError as e:
    LIVEKIT_AVAILABLE = False
    logging.warning(f"⚠️ LiveKit SDK not available: {e}")
    # Create dummy classes to prevent import errors
    class LiveKitAPI:
        pass
    class CreateRoomRequest:
        pass
    class ListRoomsRequest:
        pass
    class ListParticipantsRequest:
        pass
    class AccessToken:
        pass
    class VideoGrants:
        pass

# Redis configuration for caching
REDIS_CONFIG = {
    'host': os.getenv("REDIS_HOST", "127.0.0.1"),
    'port': int(os.getenv("REDIS_PORT", 6379)),
    'db': int(os.getenv("REDIS_DB", 0)),
    'decode_responses': os.getenv("REDIS_DECODE_RESPONSES", "True") == "True"
}

# Initialize Redis client
try:
    redis_client = redis.Redis(**REDIS_CONFIG)
    redis_client.ping()  # Test connection
    logging.info("✅ Redis connected successfully")
except Exception as e:
    logging.warning(f"⚠️ Redis not available: {e}")
    redis_client = None

# Global Variables (all your existing constants)
TBL_MEETINGS = 'tbl_Meetings'
TBL_CALENDAR_MEETING = 'tbl_CalendarMeetings'
TBL_SCHEDULED_MEETINGS = 'tbl_ScheduledMeetings'

SUCCESS_STATUS = 200
CREATED_STATUS = 201
BAD_REQUEST_STATUS = 400
UNAUTHORIZED_STATUS = 401
FORBIDDEN_STATUS = 403
NOT_FOUND_STATUS = 404
SERVER_ERROR_STATUS = 500

LOG_FILE_PATH = 'meetings_debug.log'
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'

logging.basicConfig(filename=LOG_FILE_PATH, level=LOG_LEVEL, format=LOG_FORMAT)

# LiveKit Configuration
LIVEKIT_CONFIG = {
    'url': os.getenv("LIVEKIT_URL"),
    'api_key': os.getenv("LIVEKIT_API_KEY"),
    'api_secret': os.getenv("LIVEKIT_API_SECRET"),
    'ttl': int(os.getenv("LIVEKIT_TTL", 3600))
}

class ProductionLiveKitService:
    """Production LiveKit service optimized for 50+ participants with fast joining"""
    
    def __init__(self):
        self.config = LIVEKIT_CONFIG
        self.redis_client = None
        
        # Create SSL context that ignores certificate validation
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        # Optional Redis connection
        try:
            self.redis_client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=int(os.getenv('REDIS_PORT', 6379)), db=0, decode_responses=True)
            self.redis_client.ping()
            logging.info("✅ Redis connected for caching")
        except:
            logging.info("ℹ Redis not available, proceeding without caching")
    
    def generate_admin_token(self) -> str:
        """Generate admin JWT token with correct structure for LiveKit API"""
        try:
            import jwt
            
            now = int(time.time())
            
            payload = {
                'iss': self.config['api_key'],
                'sub': 'django_admin',
                'iat': now,
                'nbf': now,
                'exp': now + 600,  # Extended from 300 to 600 seconds (10 minutes)
                'video': {
                    'roomList': True,
                    'roomCreate': True,
                    'roomJoin': True,
                    'roomAdmin': True,
                    'roomRecord': True,
                    'canPublish': True,
                    'canSubscribe': True,
                    'canPublishData': True,
                    'canUpdateOwnMetadata': True,
                    'hidden': False,
                    'recorder': False
                }
            }
            
            token = jwt.encode(payload, self.config['api_secret'], algorithm='HS256')
            
            return token
            
        except Exception as e:
            logging.error(f"❌ Admin token generation failed: {e}")
            raise Exception(f"Failed to generate admin token: {str(e)}")

    def generate_room_specific_token(self, room_name: str) -> str:
        """Generate room-specific JWT token for participant operations"""
        try:
            import jwt
            
            now = int(time.time())
            
            payload = {
                'iss': self.config['api_key'],
                'sub': 'django_room_admin',
                'iat': now,
                'nbf': now,
                'exp': now + 600,  # Extended from 300 to 600 seconds
                'video': {
                    'room': room_name,
                    'roomList': True,
                    'roomAdmin': True,
                    'roomJoin': True,
                    'canPublish': True,
                    'canSubscribe': True,
                    'canPublishData': True,
                    'canUpdateOwnMetadata': True,
                    'hidden': False,
                    'recorder': False
                }
            }
            
            token = jwt.encode(payload, self.config['api_secret'], algorithm='HS256')
            logging.info(f"✅ Generated room-specific token for {room_name}")
            
            return token
            
        except Exception as e:
            logging.error(f"❌ Room-specific token generation failed: {e}")
            raise Exception(f"Failed to generate room-specific token: {str(e)}")

    def generate_access_token(self, room_name: str, participant_name: str, 
                        metadata: Dict = None, permissions: Dict = None) -> str:
        """Generate LiveKit access token optimized for 50+ participants"""
        try:
            import jwt
            
            now = int(time.time())
            
            # OPTIMIZED: Enhanced permissions for 50+ participants
            if not permissions:
                permissions = {
                    'canPublish': True,
                    'canSubscribe': True,
                    'canPublishData': True,
                    'canUpdateOwnMetadata': True,
                    'roomJoin': True,
                    'hidden': False,
                    'recorder': False,
                    # OPTIMIZED: Source-specific permissions for better performance
                    'canPublishSources': ['camera', 'microphone', 'screen_share', 'screen_share_audio'],
                    'canSubscribeSources': ['camera', 'microphone', 'screen_share', 'screen_share_audio']
                }
            
            # Remove any muting flags for immediate audio
            if 'startMuted' in permissions:
                del permissions['startMuted']
            if 'startWithAudioMuted' in permissions:
                del permissions['startWithAudioMuted']
            
            payload = {
                'iss': self.config['api_key'],
                'sub': participant_name,
                'iat': now,
                'nbf': now,
                'exp': now + self.config.get('ttl', 7200),  # Extended to 2 hours for stability
                'video': {
                    'room': room_name,
                    'roomJoin': permissions.get('roomJoin', True),
                    'canPublish': permissions.get('canPublish', True),
                    'canSubscribe': permissions.get('canSubscribe', True),
                    'canPublishData': permissions.get('canPublishData', True),
                    'canUpdateOwnMetadata': permissions.get('canUpdateOwnMetadata', True),
                    'hidden': permissions.get('hidden', False),
                    'recorder': permissions.get('recorder', False),
                    'canPublishSources': permissions.get('canPublishSources'),
                    'canSubscribeSources': permissions.get('canSubscribeSources'),
                    # OPTIMIZED: Add room admin permissions for hosts
                    'roomAdmin': permissions.get('roomAdmin', False),
                    'roomCreate': permissions.get('roomCreate', False),
                    'roomList': permissions.get('roomList', False),
                    'roomRecord': permissions.get('roomRecord', False)
                },
                # OPTIMIZED: Room configuration for 50+ participants
                'roomConfig': {
                    'audioEnabled': True,
                    'videoEnabled': True,
                    'autoSubscribe': True,
                    'audioPriority': 'high',
                    'adaptiveStream': True,
                    'dynacast': True,
                    'startAudioMuted': False,
                    'startVideoMuted': False,
                    # PERFORMANCE: Optimizations for large groups
                    'simulcast': True,
                    'maxBitrate': 2000000,  # 2 Mbps max
                    'preferredCodec': 'vp8',  # Better for large groups
                    'enableRedundantEncoding': True
                }
            }
            
            # Enhanced metadata with performance hints
            enhanced_metadata = {
                'audioFirst': True,
                'autoSubscribeAudio': True,
                'fastJoin': True,
                'largeGroup': True,
                'optimizedFor50Plus': True
            }
            
            if metadata:
                enhanced_metadata.update(metadata)
            
            payload['metadata'] = json.dumps(enhanced_metadata)
            
            token = jwt.encode(payload, self.config['api_secret'], algorithm='HS256')
            
            logging.info(f"✅ Generated optimized access token for {participant_name} in room {room_name}")
            
            return token
            
        except Exception as e:
            logging.error(f"❌ Token generation failed: {e}")
            raise Exception(f"Failed to generate access token: {str(e)}")

    def create_room(self, room_name: str, room_config: Dict) -> Dict:
        """
        Create LiveKit room OPTIMIZED FOR UNLIMITED PARTICIPANTS
        
        KEY CHANGES:
        - REMOVED: max_participants: 100
        - ADDED: empty_timeout: 300 (auto-cleanup after 5 minutes)
        - ADDED: departure_timeout: 60
        """
        try:
            # ✅ ENHANCED: No participant limit - unlimited concurrent meetings
            enhanced_config = {
                # REMOVED: 'max_participants': 100,  ❌ DELETE THIS
                
                # ADDED: Auto-cleanup for empty rooms
                'empty_timeout': 300,  # ✅ Auto-delete room after 5 minutes empty
                'departure_timeout': 60,  # ✅ Auto-remove inactive participants after 1 minute
                
                # PERFORMANCE: Audio-first configuration
                'audio_only': False,
                'min_playout_delay': 0,
                'max_playout_delay': 150,
                'sync_streams': False,
                'adaptive_stream': True,
                'dynacast': True,
                'audio_priority': 'high',
                'start_audio_muted': False,
                'start_video_muted': False,
                
                # OPTIMIZATION: Large group optimizations (no limit)
                'simulcast_enabled': True,
                'preferred_codec': 'vp8',
                'max_bitrate': 2000000,
                'enable_redundant_encoding': True,
                'subscriber_bandwidth_limit': 1500000
            }
            
            room_result = self._create_room_via_api(room_name, enhanced_config)
            if room_result:
                return room_result
            
            # Fallback response (no max_participants limit)
            return {
                'name': room_name,
                'sid': f'django_room_{room_name}_{int(time.time())}',
                'room_sid': f'django_room_{room_name}_{int(time.time())}',
                'creation_time': time.time(),
                'max_participants': None,  # ✅ No limit
                'metadata': json.dumps({
                    'created_at': time.time(),
                    'created_by': 'django_optimized_unlimited',
                    'room_config': enhanced_config,
                    'audio_priority': 'high',
                    'auto_cleanup_minutes': 5,
                    'unlimited_participants': True
                }),
                'ssl_fixed': True,
                'real_livekit_room': False,
                'fallback': True,
                'optimized_for_unlimited_participants': True,
                'empty_timeout_minutes': 5,
                'auto_cleanup_enabled': True
            }
            
        except Exception as e:
            logging.error(f"Room creation error: {e}")
            return self._fallback_room_response(room_name)

    def _create_room_via_api(self, room_name: str, room_config: Dict) -> Optional[Dict]:
        """
        Create room via direct API call - UNLIMITED PARTICIPANTS
        
        KEY CHANGES:
        - REMOVED max_participants from payload
        - ADDED empty_timeout and departure_timeout for auto-cleanup
        """
        max_retries = 3
        base_timeout = 15
        
        for attempt in range(max_retries):
            try:
                import requests
                
                admin_token = self.generate_admin_token()
                
                url = f"{self.config['url']}/twirp/livekit.RoomService/CreateRoom"
                
                headers = {
                    'Authorization': f'Bearer {admin_token}',
                    'Content-Type': 'application/json'
                }
                
                # ✅ OPTIMIZED: No max_participants limit
                payload = {
                    'name': room_name,
                    'empty_timeout': room_config.get('empty_timeout', 300),  # ✅ Auto-cleanup: 5 min
                    'departure_timeout': room_config.get('departure_timeout', 60),  # Auto-remove: 1 min
                    # REMOVED: 'max_participants': room_config.get('max_participants', 100),
                    'metadata': json.dumps({
                        'created_at': time.time(),
                        'created_by': 'django_api_optimized_unlimited',
                        'room_config': room_config,
                        'audio_priority': 'high',
                        'auto_subscribe_audio': True,
                        'unlimited_participants': True,
                        'empty_timeout_minutes': 5
                    }),
                    'min_playout_delay': room_config.get('min_playout_delay', 0),
                    'max_playout_delay': room_config.get('max_playout_delay', 150),
                    'sync_streams': room_config.get('sync_streams', False)
                }
                
                timeout = base_timeout + (attempt * 5)
                
                response = requests.post(
                    url, 
                    headers=headers, 
                    json=payload, 
                    verify=False,
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'sid' in result and 'room_sid' not in result:
                        result['room_sid'] = result['sid']
                    
                    logging.info(f"✅ Successfully created unlimited room: {room_name}")
                    logging.info(f"🔍 Room response: {result}")
                    
                    return result
                else:
                    logging.error(f"❌ Room creation API failed: {response.status_code} - {response.text}")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(2 ** attempt)
                    
            except Exception as e:
                logging.error(f"Room creation via API error (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
        
        return None

    def get_room(self, room_name: str) -> Optional[Dict]:
        """Get room information with extended timeout and retry logic for 50+ participants"""
        max_retries = 3
        base_timeout = 10  # Increased from 3 to 10 seconds
        
        for attempt in range(max_retries):
            try:
                import requests
                
                admin_token = self.generate_admin_token()
                
                url = f"{self.config['url']}/twirp/livekit.RoomService/ListRooms"
                
                headers = {
                    'Authorization': f'Bearer {admin_token}',
                    'Content-Type': 'application/json'
                }
                
                payload = {
                    'names': [room_name]
                }
                
                # Progressive timeout increase
                timeout = base_timeout + (attempt * 3)
                
                response = requests.post(
                    url, 
                    headers=headers, 
                    json=payload, 
                    verify=False,
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    rooms = result.get('rooms', [])
                    
                    for room in rooms:
                        if room.get('name') == room_name:
                            logging.info(f"✅ Found room: {room_name}")
                            return room
                    
                    return None
                else:
                    logging.error(f"❌ Get room API failed: {response.status_code} - {response.text}")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(2 ** attempt)
                
            except requests.exceptions.Timeout:
                logging.warning(f"⏰ Timeout getting room {room_name} (attempt {attempt + 1})")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
            except Exception as e:
                logging.error(f"Error getting room {room_name}: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
        
        return None

    def list_participants(self, room_name: str) -> List[Dict]:
        """List participants with extended timeout and retry logic for 50+ participants"""
        max_retries = 3
        base_timeout = 15  # Increased from 3 to 15 seconds
        
        for attempt in range(max_retries):
            try:
                import requests
                
                logging.info(f"📊 Listing participants for {room_name} (attempt {attempt + 1})")
                
                room_token = self.generate_room_specific_token(room_name)
                
                url = f"{self.config['url']}/twirp/livekit.RoomService/ListParticipants"
                
                headers = {
                    'Authorization': f'Bearer {room_token}',
                    'Content-Type': 'application/json'
                }
                
                payload = {
                    'room': room_name
                }
                
                # Progressive timeout increase with exponential backoff
                timeout = base_timeout + (attempt * 5)
                
                response = requests.post(
                    url, 
                    headers=headers, 
                    json=payload, 
                    verify=False,
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    participants = []
                    for p in result.get('participants', []):
                        participants.append({
                            'identity': p.get('identity', ''),
                            'name': p.get('name', ''),  
                            'state': p.get('state', 'ACTIVE'),
                            'tracks': p.get('tracks', []),
                            'metadata': p.get('metadata', ''),
                            'joined_at': p.get('joined_at'),
                            'is_publisher': len(p.get('tracks', [])) > 0,
                            'connection_quality': p.get('connection_quality', 'unknown'),
                            # ADDED: Additional info for large groups
                            'track_count': len(p.get('tracks', [])),
                            'has_video': any(track.get('type') == 'video' for track in p.get('tracks', [])),
                            'has_audio': any(track.get('type') == 'audio' for track in p.get('tracks', []))
                        })
                    
                    logging.info(f"✅ Found {len(participants)} LiveKit participants in {room_name}")
                    return participants
                    
                elif response.status_code == 404:
                    logging.info(f"ℹ Room {room_name} not found or has no participants")
                    return []
                else:
                    logging.warning(f"❌ API failed: {response.status_code} - {response.text}")
                    if attempt == max_retries - 1:
                        return []
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            except requests.exceptions.Timeout:
                logging.warning(f"⏰ Timeout listing participants for {room_name} (attempt {attempt + 1})")
                if attempt == max_retries - 1:
                    return []
                time.sleep(2 ** attempt)
            except Exception as e:
                logging.error(f"Error listing participants for {room_name}: {e}")
                if attempt == max_retries - 1:
                    return []
                time.sleep(2 ** attempt)
        
        return []

    def remove_participant(self, room_name: str, participant_identity: str, reason: str = "MANUAL_DISCONNECT") -> bool:
        """Remove participant from LiveKit room and prevent reconnection"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                import requests
                
                admin_token = self.generate_admin_token()
                
                url = f"{self.config['url']}/twirp/livekit.RoomService/RemoveParticipant"
                
                headers = {
                    'Authorization': f'Bearer {admin_token}',
                    'Content-Type': 'application/json'
                }
                
                payload = {
                    'room': room_name,
                    'identity': participant_identity,
                    'reason': reason
                }
                
                response = requests.post(
                    url, 
                    headers=headers, 
                    json=payload, 
                    verify=False,
                    timeout=10
                )
                
                if response.status_code == 200:
                    logging.info(f"Removed participant {participant_identity} from room {room_name}")
                    return True
                else:
                    logging.warning(f"Remove participant failed: {response.status_code}")
                    if attempt == max_retries - 1:
                        return False
                    time.sleep(2 ** attempt)
                    
            except Exception as e:
                logging.error(f"Error removing participant: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(2 ** attempt)
        
        return False

    def close_room(self, room_name: str) -> bool:
        """Close/delete LiveKit room completely to prevent any reconnection"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                import requests
                
                admin_token = self.generate_admin_token()
                
                url = f"{self.config['url']}/twirp/livekit.RoomService/DeleteRoom"
                
                headers = {
                    'Authorization': f'Bearer {admin_token}',
                    'Content-Type': 'application/json'
                }
                
                payload = {
                    'room': room_name
                }
                
                response = requests.post(
                    url, 
                    headers=headers, 
                    json=payload, 
                    verify=False,
                    timeout=10
                )
                
                if response.status_code == 200:
                    logging.info(f"Closed room {room_name}")
                    return True
                else:
                    logging.warning(f"Close room failed: {response.status_code}")
                    if attempt == max_retries - 1:
                        return False
                    time.sleep(2 ** attempt)
                    
            except Exception as e:
                logging.error(f"Error closing room: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(2 ** attempt)
        
        return False

    def cleanup_empty_rooms(self) -> Dict:
        """
        ✅ NEW FUNCTION: Cleanup empty rooms after 5 minutes
        
        Call this periodically (every 5 minutes) via a background task:
        - Identifies rooms that have been empty for 5+ minutes
        - Closes/deletes those rooms
        - Logs cleanup actions
        """
        try:
            logging.info("🧹 [CLEANUP] Starting empty room cleanup...")
            
            cleanup_results = {
                'rooms_checked': 0,
                'rooms_cleaned': 0,
                'errors': [],
                'timestamp': time.time()
            }
            
            # Get all active meetings
            try:
                with connection.cursor() as cursor:
                    # Get meetings with their creation times
                    cursor.execute("""
                        SELECT ID, LiveKit_Room_Name, Created_At, Status
                        FROM tbl_Meetings
                        WHERE Status = 'active' AND LiveKit_Room_Name IS NOT NULL
                        ORDER BY Created_At ASC
                    """)
                    
                    meetings = cursor.fetchall()
                    cleanup_results['rooms_checked'] = len(meetings)
                    
                    current_time = timezone.now()
                    cleanup_timeout = timedelta(minutes=60)
                    
                    for meeting_id, room_name, created_at, status in meetings:
                        try:
                            # Calculate time since room creation
                            if created_at.tzinfo is None:
                                created_at = timezone.make_aware(created_at)
                            
                            time_since_creation = current_time - created_at
                            
                            # Check if room is empty
                            try:
                                participants = self.list_participants(room_name)
                                participant_count = len(participants)
                            except Exception as e:
                                logging.warning(f"Could not get participant count for {room_name}: {e}")
                                participant_count = 0
                            
                            # If room is empty for 5+ minutes, cleanup
                            if participant_count == 0 and time_since_creation >= cleanup_timeout:
                                logging.info(f"🗑️ [CLEANUP] Empty room detected: {room_name} (created {time_since_creation.total_seconds()}s ago)")
                                
                                try:
                                    # Close the room in LiveKit
                                    self.close_room(room_name)
                                    
                                    # Mark as ended in database
                                    cursor.execute("""
                                        UPDATE tbl_Meetings
                                        SET Status = 'ended', Ended_At = %s
                                        WHERE ID = %s
                                    """, [current_time, meeting_id])
                                    
                                    logging.info(f"✅ [CLEANUP] Cleaned up room: {room_name} (Meeting: {meeting_id})")
                                    cleanup_results['rooms_cleaned'] += 1
                                    
                                except Exception as cleanup_error:
                                    error_msg = f"Failed to cleanup {room_name}: {str(cleanup_error)}"
                                    logging.error(f"❌ [CLEANUP] {error_msg}")
                                    cleanup_results['errors'].append(error_msg)
                            
                            else:
                                logging.debug(f"ℹ️ [CLEANUP] Room active or recent: {room_name} ({participant_count} participants, {time_since_creation.total_seconds()}s old)")
                                
                        except Exception as room_error:
                            error_msg = f"Error processing room {room_name}: {str(room_error)}"
                            logging.error(f"❌ [CLEANUP] {error_msg}")
                            cleanup_results['errors'].append(error_msg)
                            continue
                    
            except Exception as query_error:
                error_msg = f"Database query error: {str(query_error)}"
                logging.error(f"❌ [CLEANUP] {error_msg}")
                cleanup_results['errors'].append(error_msg)
            
            logging.info(f"""
                    ✅ [CLEANUP] Cleanup completed:
                    - Rooms checked: {cleanup_results['rooms_checked']}
                    - Rooms cleaned: {cleanup_results['rooms_cleaned']}
                    - Errors: {len(cleanup_results['errors'])}
                            """)
            
            return cleanup_results
            
        except Exception as e:
            logging.error(f"❌ [CLEANUP] Critical error in cleanup_empty_rooms: {e}")
            return {
                'error': str(e),
                'timestamp': time.time(),
                'rooms_checked': 0,
                'rooms_cleaned': 0
            }

    def force_disconnect_participant(self, room_name: str, participant_identity: str) -> bool:
        """Force disconnect a participant and prevent any automatic reconnection"""
        try:
            # First try to remove participant
            removed = self.remove_participant(room_name, participant_identity, "FORCE_DISCONNECT")
            
            # If removal fails, try alternative methods
            if not removed:
                logging.warning(f"Failed to remove participant {participant_identity}, trying alternative disconnect")
                
                # Try muting all tracks as alternative
                try:
                    self.mute_participant_tracks(room_name, participant_identity)
                except:
                    pass
            
            return removed
            
        except Exception as e:
            logging.error(f"Force disconnect failed: {e}")
            return False

    def mute_participant_tracks(self, room_name: str, participant_identity: str) -> bool:
        """Mute all tracks for a participant as disconnect alternative"""
        try:
            import requests
            
            admin_token = self.generate_admin_token()
            
            url = f"{self.config['url']}/twirp/livekit.RoomService/MutePublishedTrack"
            
            headers = {
                'Authorization': f'Bearer {admin_token}',
                'Content-Type': 'application/json'
            }
            
            # Mute all possible track types
            track_types = ['audio', 'video', 'data']
            success = False
            
            for track_type in track_types:
                payload = {
                    'room': room_name,
                    'identity': participant_identity,
                    'track_sid': '',  # Empty means all tracks of this type
                    'muted': True
                }
                
                try:
                    response = requests.post(
                        url, 
                        headers=headers, 
                        json=payload, 
                        verify=False,
                        timeout=5
                    )
                    
                    if response.status_code == 200:
                        success = True
                except:
                    continue
            
            return success
            
        except Exception as e:
            logging.error(f"Mute tracks failed: {e}")
            return False
         
    def _fallback_room_response(self, room_name: str) -> Dict:
        """
        Fallback room response OPTIMIZED FOR UNLIMITED PARTICIPANTS
        
        KEY CHANGES:
        - Removed max_participants limit
        - Added 5-minute auto-cleanup timeout
        """
        fallback_sid = f'fallback_{room_name}_{int(time.time())}'
        
        return {
            'name': room_name,
            'sid': fallback_sid,
            'room_sid': fallback_sid,
            'creation_time': time.time(),
            'max_participants': None,  # ✅ NO LIMIT
            'metadata': json.dumps({
                'fallback': True,
                'created_at': time.time(),
                'audio_priority': 'high',
                'auto_cleanup_enabled': True,
                'empty_timeout_minutes': 5,
                'unlimited_participants': True
            }),
            'fallback': True,
            'real_livekit_room': False,
            'optimized_for_unlimited_participants': True,
            'empty_timeout_minutes': 5,
            'auto_cleanup_enabled': True
        }

# Initialize the service
livekit_service = ProductionLiveKitService() 

# OPTIMIZED: Connection tracking for 50+ participants
CONNECTION_QUEUE = {}
CONNECTION_LIMITS = {
    'MAX_CONCURRENT_JOINS': 100,  # ✅ Increased from 50 (no participant limit)
    'MAX_PARTICIPANTS_PER_ROOM': None,  # ✅ UNLIMITED - was 100
    'CONNECTION_TIMEOUT': 120,
    'CLEANUP_INTERVAL': 30,
    'GRACE_PERIOD': 300,
    'QUEUE_WAIT_TIME': 1,
    'MAX_RETRIES': 3,
    'RETRY_DELAY': 2,
    'EMPTY_ROOM_CLEANUP_MINUTES': 5  # ✅ NEW: Auto-cleanup timeout
}

def manage_connection_queue(room_name: str, user_id: str, action: str = 'join'):
    """
    ✅ OPTIMIZED: Connection queue for unlimited concurrent participants
    
    KEY CHANGES:
    - Removed per-room participant limits
    - Increased max concurrent joins to 100
    - No longer enforces 100 participant cap
    """
    import time
    
    current_time = time.time()
    
    # Initialize room queue if it doesn't exist
    if room_name not in CONNECTION_QUEUE:
        CONNECTION_QUEUE[room_name] = {
            'active_connections': {},
            'waiting_queue': [],
            'last_cleanup': current_time,
            'total_processed': 0,
            'peak_concurrent': 0,
            'unlimited_mode': True  # ✅ NEW: Mark as unlimited
        }
    
    room_queue = CONNECTION_QUEUE[room_name]
    
    # Cleanup old connections every 30 seconds
    if current_time - room_queue['last_cleanup'] > 30:
        room_queue['active_connections'] = {
            uid: timestamp for uid, timestamp in room_queue['active_connections'].items()
            if current_time - timestamp < 120
        }
        room_queue['last_cleanup'] = current_time
        
        current_concurrent = len(room_queue['active_connections'])
        if current_concurrent > room_queue['peak_concurrent']:
            room_queue['peak_concurrent'] = current_concurrent
    
    if action == 'join':
        # Check if user is already connecting
        if user_id in room_queue['active_connections']:
            return {
                'status': 'already_connecting',
                'position': 0,
                'estimated_wait': 0,
                'active_connections': len(room_queue['active_connections']),
                'unlimited_mode': True
            }
        
        # ✅ UNLIMITED: Allow up to 100 concurrent joins (no per-room limit)
        current_connections = len(room_queue['active_connections'])
        MAX_CONCURRENT = 100  # ✅ Max joins at once (not per-room limit)
        
        if current_connections >= MAX_CONCURRENT:
            # Add to waiting queue
            if user_id not in room_queue['waiting_queue']:
                room_queue['waiting_queue'].append(user_id)
            
            position = room_queue['waiting_queue'].index(user_id) + 1
            estimated_wait = position * 1
            
            return {
                'status': 'queued',
                'position': position,
                'estimated_wait': estimated_wait,
                'active_connections': current_connections,
                'message': f'You are #{position} in the connection queue',
                'unlimited_mode': True
            }
        else:
            # Allow immediate connection
            room_queue['active_connections'][user_id] = current_time
            
            if user_id in room_queue['waiting_queue']:
                room_queue['waiting_queue'].remove(user_id)
            
            return {
                'status': 'allowed',
                'position': 0,
                'estimated_wait': 0,
                'active_connections': current_connections + 1,
                'total_processed': room_queue['total_processed'],
                'unlimited_mode': True
            }
    
    elif action == 'leave':
        if user_id in room_queue['active_connections']:
            del room_queue['active_connections'][user_id]
        
        if user_id in room_queue['waiting_queue']:
            room_queue['waiting_queue'].remove(user_id)
        
        return {
            'status': 'removed',
            'active_connections': len(room_queue['active_connections']),
            'unlimited_mode': True
        }
    
    elif action == 'check':
        current_connections = len(room_queue['active_connections'])
        
        if user_id in room_queue['active_connections']:
            return {
                'status': 'connecting',
                'position': 0,
                'active_connections': current_connections,
                'unlimited_mode': True
            }
        elif user_id in room_queue['waiting_queue']:
            position = room_queue['waiting_queue'].index(user_id) + 1
            estimated_wait = position * 1
            return {
                'status': 'queued',
                'position': position,
                'estimated_wait': estimated_wait,
                'active_connections': current_connections,
                'unlimited_mode': True
            }
        else:
            return {
                'status': 'not_in_queue',
                'position': 0,
                'active_connections': current_connections,
                'unlimited_mode': True
            }
    
    elif action == 'stats':
        return {
            'status': 'stats',
            'active_connections': len(room_queue['active_connections']),
            'queue_length': len(room_queue['waiting_queue']),
            'total_processed': room_queue['total_processed'],
            'peak_concurrent': room_queue['peak_concurrent'],
            'unlimited_mode': True
        }
    
    return {
        'status': 'unknown',
        'active_connections': len(room_queue.get('active_connections', {})),
        'unlimited_mode': True
    }

# ADDITIONAL: Helper function for monitoring large groups
def get_room_performance_metrics(room_name: str) -> Dict:
    """Get performance metrics for rooms with 50+ participants"""
    try:
        metrics = {
            'room_name': room_name,
            'timestamp': time.time(),
            'connection_queue': manage_connection_queue(room_name, 'monitor', 'stats'),
            'livekit_enabled': LIVEKIT_ENABLED,
            'service_available': livekit_service is not None
        }
        
        # Get LiveKit metrics if available
        if LIVEKIT_ENABLED and livekit_service:
            try:
                # Get participant count with timeout
                participants = livekit_service.list_participants(room_name)
                room_info = livekit_service.get_room(room_name)
                
                metrics['livekit_metrics'] = {
                    'participant_count': len(participants),
                    'room_exists': room_info is not None,
                    'participants_with_video': len([p for p in participants if p.get('has_video', False)]),
                    'participants_with_audio': len([p for p in participants if p.get('has_audio', False)]),
                    'total_tracks': sum(len(p.get('tracks', [])) for p in participants),
                    'room_info': room_info
                }
            except Exception as e:
                metrics['livekit_metrics'] = {
                    'error': str(e),
                    'participant_count': 0,
                    'room_exists': False
                }
        
        return metrics
        
    except Exception as e:
        return {
            'room_name': room_name,
            'error': str(e),
            'timestamp': time.time()
        }

# PERFORMANCE: Cache frequently accessed room data
def get_cached_room_info(room_name: str) -> Optional[Dict]:
    """Get cached room information for better performance"""
    try:
        if livekit_service.redis_client:
            cache_key = f"room_info:{room_name}"
            cached_data = livekit_service.redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
    except Exception as e:
        logging.warning(f"Cache retrieval error: {e}")
    
    return None

def cache_room_info(room_name: str, room_data: Dict, ttl: int = 30):
    """Cache room information for 30 seconds"""
    try:
        if livekit_service.redis_client:
            cache_key = f"room_info:{room_name}"
            livekit_service.redis_client.setex(
                cache_key, 
                ttl, 
                json.dumps(room_data)
            )
    except Exception as e:
        logging.warning(f"Cache storage error: {e}")

def manage_connection_queue(room_name: str, user_id: str, action: str = 'join'):
    """
    OPTIMIZED: Connection queue for 50+ participants with fast processing
    """
    import time
    
    current_time = time.time()
    
    # Initialize room queue if it doesn't exist
    if room_name not in CONNECTION_QUEUE:
        CONNECTION_QUEUE[room_name] = {
            'active_connections': {},
            'waiting_queue': [],
            'last_cleanup': current_time
        }
    
    room_queue = CONNECTION_QUEUE[room_name]
    
    # Cleanup old connections (timeout after 2 minutes instead of 30 seconds)
    if current_time - room_queue['last_cleanup'] > 30:  # Cleanup every 30 seconds
        room_queue['active_connections'] = {
            uid: timestamp for uid, timestamp in room_queue['active_connections'].items()
            if current_time - timestamp < 120  # 2 minutes timeout instead of 30 seconds
        }
        room_queue['last_cleanup'] = current_time
    
    if action == 'join':
        # Check if user is already connecting
        if user_id in room_queue['active_connections']:
            return {
                'status': 'already_connecting',
                'position': 0,
                'estimated_wait': 0,
                'active_connections': len(room_queue['active_connections'])
            }
        
        # INCREASED: Allow more concurrent connections for 50+ participants
        current_connections = len(room_queue['active_connections'])
        MAX_CONCURRENT = 50  # Increased from 10 to 50
        
        if current_connections >= MAX_CONCURRENT:
            # Add to waiting queue
            if user_id not in room_queue['waiting_queue']:
                room_queue['waiting_queue'].append(user_id)
            
            position = room_queue['waiting_queue'].index(user_id) + 1
            estimated_wait = position * 1  # Reduced from 2 to 1 second per person
            
            return {
                'status': 'queued',
                'position': position,
                'estimated_wait': estimated_wait,
                'active_connections': current_connections,
                'message': f'You are #{position} in the connection queue'
            }
        else:
            # Allow immediate connection
            room_queue['active_connections'][user_id] = current_time
            
            # Remove from waiting queue if they were there
            if user_id in room_queue['waiting_queue']:
                room_queue['waiting_queue'].remove(user_id)
            
            return {
                'status': 'allowed',
                'position': 0,
                'estimated_wait': 0,
                'active_connections': current_connections + 1
            }
    
    elif action == 'leave':
        # Remove from active connections
        if user_id in room_queue['active_connections']:
            del room_queue['active_connections'][user_id]
        
        # Remove from waiting queue
        if user_id in room_queue['waiting_queue']:
            room_queue['waiting_queue'].remove(user_id)
        
        return {
            'status': 'removed',
            'active_connections': len(room_queue['active_connections'])
        }
    
    elif action == 'check':
        # Check queue status
        current_connections = len(room_queue['active_connections'])
        
        if user_id in room_queue['active_connections']:
            return {
                'status': 'connecting',
                'position': 0,
                'active_connections': current_connections
            }
        elif user_id in room_queue['waiting_queue']:
            position = room_queue['waiting_queue'].index(user_id) + 1
            estimated_wait = position * 1  # Reduced wait time
            return {
                'status': 'queued',
                'position': position,
                'estimated_wait': estimated_wait,
                'active_connections': current_connections
            }
        else:
            return {
                'status': 'not_in_queue',
                'position': 0,
                'active_connections': current_connections
            }
    
    return {'status': 'unknown', 'active_connections': 0}

@require_http_methods(["GET"])
@csrf_exempt
def check_connection_queue(request, meeting_id):
    """
    SCALABILITY FIX: Check connection queue status for a meeting
    """
    try:
        user_id = request.GET.get('user_id')
        if not user_id:
            return JsonResponse({'error': 'user_id parameter required'}, status=400)
        
        # Get room name
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT LiveKit_Room_Name FROM tbl_Meetings WHERE ID = %s", [meeting_id])
                row = cursor.fetchone()
                room_name = row[0] if row and row[0] else f"meeting_{meeting_id}"
        except:
            room_name = f"meeting_{meeting_id}"
        
        queue_status = manage_connection_queue(room_name, str(user_id), 'check')
        
        return JsonResponse({
            'success': True,
            'meeting_id': meeting_id,
            'room_name': room_name,
            'user_id': user_id,
            'queue_status': queue_status
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

def get_room_participant_count_with_cache(room_name: str) -> int:
    """
    SCALABILITY FIX: Get participant count with Redis caching to reduce API calls
    """
    cache_key = f"participant_count:{room_name}"
    
    # Try Redis cache first
    try:
        if redis_client:
            cached_count = redis_client.get(cache_key)
            if cached_count is not None:
                return int(cached_count)
    except Exception as e:
        logging.warning(f"Redis cache error: {e}")
    
    # Fallback to LiveKit API
    try:
        if LIVEKIT_ENABLED and livekit_service:
            participants = livekit_service.list_participants(room_name)
            count = len(participants)
            
            # Cache for 10 seconds
            try:
                if redis_client:
                    redis_client.setex(cache_key, 10, count)
            except:
                pass
            
            return count
    except Exception as e:
        logging.warning(f"LiveKit API error: {e}")
    
    return 0

@require_http_methods(["POST"])
@csrf_exempt
def join_meeting_with_queue(request):
    """
    SCALABILITY FIX: Join meeting with connection queue management
    This endpoint should be called before join_livekit_meeting
    """
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id') or data.get('meetingId')
        user_id = str(data.get('user_id') or data.get('userId'))
        
        if not meeting_id or not user_id:
            return JsonResponse({
                'error': 'meeting_id and user_id required'
            }, status=400)
        
        # Get room name
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT LiveKit_Room_Name FROM tbl_Meetings WHERE ID = %s", [meeting_id])
                row = cursor.fetchone()
                room_name = row[0] if row and row[0] else f"meeting_{meeting_id}"
        except:
            room_name = f"meeting_{meeting_id}"
        
        # Check current participant count
        current_count = get_room_participant_count_with_cache(room_name)
        
        if current_count >= CONNECTION_LIMITS['MAX_PARTICIPANTS_PER_ROOM']:
            return JsonResponse({
                'error': 'Meeting is at maximum capacity',
                'current_participants': current_count,
                'max_participants': CONNECTION_LIMITS['MAX_PARTICIPANTS_PER_ROOM']
            }, status=429)
        
        # Manage connection queue
        queue_status = manage_connection_queue(room_name, user_id, 'join')
        
        if queue_status['status'] == 'allowed':
            return JsonResponse({
                'success': True,
                'message': 'Connection allowed',
                'queue_status': queue_status,
                'can_proceed': True,
                'room_name': room_name
            })
        elif queue_status['status'] == 'queued':
            return JsonResponse({
                'success': True,
                'message': 'Added to connection queue',
                'queue_status': queue_status,
                'can_proceed': False,
                'room_name': room_name
            }, status=202)  # Accepted but not yet processed
        else:
            return JsonResponse({
                'success': True,
                'message': 'Already in connection process',
                'queue_status': queue_status,
                'can_proceed': True,
                'room_name': room_name
            })
            
    except Exception as e:
        logging.error(f"Queue management error: {e}")
        return JsonResponse({'error': str(e)}, status=500)

# MISSING HELPER FUNCTIONS - Add these after your ProductionLiveKitService class

def ensure_room_exists(room_name: str, meeting_id: str) -> bool:
    """Ensure LiveKit room exists"""
    try:
        if not LIVEKIT_ENABLED or not livekit_service:
            return False
        
        # Check if room already exists
        existing_room = livekit_service.get_room(room_name)
        if existing_room:
            logging.info(f"✅ Room {room_name} already exists")
            return True
        
        # Create new room
        room_config = {
            'max_participants': 200,
            'empty_timeout': 300,
            'departure_timeout': 60
        }
        
        result = livekit_service.create_room(room_name, room_config)
        
        if result and 'name' in result:
            logging.info(f"✅ Created room {room_name} for meeting {meeting_id}")
            return True
        else:
            logging.warning(f"⚠️ Failed to create room {room_name}")
            return False
            
    except Exception as e:
        logging.error(f"Error ensuring room exists: {e}")
        return False

async def send_capacity_alert(meeting_id: str, metrics: Dict):
    """Send alert when meeting approaches capacity"""
    try:
        participant_count = metrics.get('participant_count', 0)
        
        if participant_count > 180:
            alert_message = f"⚠️ Meeting {meeting_id} is near capacity: {participant_count}/200 participants"
            
            # Log the alert
            logging.warning(alert_message)
            
            # Could send email, webhook, or other notification here
            # For now, just log it
            
            # Store alert in Redis if available
            if redis_client:
                alert_data = {
                    'meeting_id': meeting_id,
                    'participant_count': participant_count,
                    'timestamp': timezone.now().isoformat(),
                    'message': alert_message
                }
                redis_client.lpush(f"alerts:{meeting_id}", json.dumps(alert_data))
                redis_client.ltrim(f"alerts:{meeting_id}", 0, 10)  # Keep last 10 alerts
                
    except Exception as e:
        logging.error(f"Error sending capacity alert: {e}")

# FAST participant recording without Status column

def record_participant_leave_fast(meeting_id, user_id):
    """Fast participant leave recording"""
    try:
        from django.db import connection
        from django.utils import timezone
        
        ist_timezone = pytz.timezone("Asia/Kolkata")
        leave_time = timezone.now().astimezone(ist_timezone)
        
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE tbl_Participants 
                SET Leave_Time = %s
                WHERE Meeting_ID = %s AND User_ID = %s AND Leave_Time IS NULL
            """, [leave_time, meeting_id, user_id])
            
            if cursor.rowcount > 0:
                logging.info(f"✅ Fast recorded leave for user {user_id}")
                return True
            return False
    except Exception as e:
        logging.warning(f"Failed to record leave: {e}")
        return False


async def get_meeting_info_cached(meeting_id: str) -> Dict:
    """Get meeting info with Redis caching"""
    cache_key = f"meeting:{meeting_id}"
    
    # Try cache first
    try:
        cached = redis_client.get(cache_key)
        if cached:
            return json.loads(cached)
    except:
        pass
    
    # Database lookup with connection pooling
    async with aiopg.create_pool(DATABASE_URL) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    SELECT Host_ID, Meeting_Name, Status, LiveKit_Room_Name, 
                           Is_Recording_Enabled, Waiting_Room_Enabled
                    FROM tbl_Meetings 
                    WHERE ID = %s
                """, [meeting_id])
                
                row = await cursor.fetchone()
                if not row:
                    return None
                
                meeting_info = {
                    'host_id': row[0],
                    'meeting_name': row[1],
                    'status': row[2],
                    'livekit_room_name': row[3],
                    'recording_enabled': row[4],
                    'waiting_room_enabled': row[5]
                }
                
                # Cache for 5 minutes
                redis_client.setex(cache_key, 300, json.dumps(meeting_info))
                
                return meeting_info

async def record_participant_join_async(meeting_id: str, user_id: str, user_name: str, is_host: bool):
    """Async participant recording with batch operations"""
    try:
        # Use connection pool
        async with aiopg.create_pool(DATABASE_URL) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO tbl_Participants 
                        (Meeting_ID, User_ID, Full_Name, Join_Time, Role, Status)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE Join_Time = VALUES(Join_Time)
                    """, [
                        meeting_id, user_id, user_name, 
                        timezone.now(), 'host' if is_host else 'participant', 'active'
                    ])
                    
                    # Update participant count in cache
                    cache_key = f"participant_count:{meeting_id}"
                    redis_client.incr(cache_key)
                    redis_client.expire(cache_key, 3600)
                    
    except Exception as e:
        logging.error(f"Failed to record participant: {e}")

async def get_room_participant_count_cached(room_name: str) -> int:
    """Get participant count with caching"""
    cache_key = f"room_count:{room_name}"
    
    try:
        cached_count = redis_client.get(cache_key)
        if cached_count:
            return int(cached_count)
    except:
        pass
    
    # Fallback to LiveKit API
    try:
        api = LiveKitAPI(LIVEKIT_CONFIG['url'], LIVEKIT_CONFIG['api_key'], LIVEKIT_CONFIG['api_secret'])
        participants = await api.room.list_participants(ListParticipantsRequest(room=room_name))
        count = len(participants.participants)
        
        # Cache for 30 seconds
        redis_client.setex(cache_key, 30, count)
        return count
        
    except:
        return 0


# Disable slow room lookups in List_All_Meetings
def List_All_Meetings_Fast(request):
    """Fast meeting list without slow LiveKit lookups"""
    try:
        with connection.cursor() as cursor:
            select_query = f"""
            SELECT ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status, Created_At, 
                   Started_At, Ended_At, Is_Recording_Enabled, Waiting_Room_Enabled,
                   LiveKit_Room_Name, LiveKit_Room_SID
            FROM {TBL_MEETINGS}
            ORDER BY Created_At DESC
            LIMIT 100
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            meetings = []
            
            for row in rows:
                meeting = dict(zip([
                    'ID', 'Host_ID', 'Meeting_Name', 'Meeting_Type', 'Meeting_Link', 'Status', 'Created_At',
                    'Started_At', 'Ended_At', 'Is_Recording_Enabled', 'Waiting_Room_Enabled',
                    'LiveKit_Room_Name', 'LiveKit_Room_SID'
                ], row))
                
                # Skip slow LiveKit status checks - just set defaults
                meeting['LiveKit_Room_Active'] = False
                meeting['LiveKit_Participants'] = 0
                meetings.append(meeting)
                
    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)

    return JsonResponse(meetings, safe=False, status=200)


# ADDITIONAL FIX: Add this function to test token generation manually
def test_token_generation():
    """Test function to verify token generation works correctly"""
    try:
        if not LIVEKIT_ENABLED or not livekit_service:
            print("❌ LiveKit not enabled or service not available")
            return False
        
        test_room = "test_room_123"
        test_participant = "test_user_456"
        test_metadata = {"user_id": "456", "role": "host"}
        test_permissions = {
            'canPublish': True,
            'canSubscribe': True,
            'canPublishData': True,
            'canUpdateOwnMetadata': True,
            'roomJoin': True,
            'hidden': False,
            'recorder': False
        }
        
        print("🧪 Testing token generation...")
        token = livekit_service.generate_access_token(
            room_name=test_room,
            participant_name=test_participant,
            metadata=test_metadata,
            permissions=test_permissions
        )
        
        # Decode and verify
        import jwt as jwt_decode
        decoded = jwt_decode.decode(token, options={"verify_signature": False})
        
        if 'video' in decoded and decoded['video'].get('room') == test_room:
            print("✅ Token generation test PASSED")
            return True
        else:
            print("❌ Token generation test FAILED - video grants missing")
            return False
            
    except Exception as e:
        print(f"❌ Token generation test FAILED with error: {e}")
        return False
# ALTERNATIVE FIX: If the above doesn't work, use this manual token generation
def generate_manual_livekit_token(room_name: str, participant_name: str, 
                                api_key: str, api_secret: str, 
                                permissions: Dict = None) -> str:
    """Manual token generation as ultimate fallback"""
    try:
        import jwt
        import time
        
        # Current time
        now = int(time.time())
        
        # Default permissions
        default_permissions = {
            'canPublish': True,
            'canSubscribe': True,
            'canPublishData': True,
            'canUpdateOwnMetadata': True,
            'roomJoin': True,
            'hidden': False,
            'recorder': False
        }
        
        if permissions:
            default_permissions.update(permissions)
        
        # JWT payload with video grants
        payload = {
            'iss': api_key,  # Issuer (API key)
            'sub': participant_name,  # Subject (participant identity)
            'iat': now,  # Issued at
            'nbf': now,  # Not before
            'exp': now + 3600,  # Expires in 1 hour
            'video': {  # CRITICAL: Video grants section
                'room': room_name,
                'roomJoin': default_permissions['roomJoin'],
                'canPublish': default_permissions['canPublish'],
                'canSubscribe': default_permissions['canSubscribe'],
                'canPublishData': default_permissions['canPublishData'],
                'canUpdateOwnMetadata': default_permissions['canUpdateOwnMetadata'],
                'hidden': default_permissions['hidden'],
                'recorder': default_permissions['recorder']
            }
        }
        
        # Generate JWT token
        token = jwt.encode(payload, api_secret, algorithm='HS256')
        
        logging.info(f"✅ Manual token generated successfully")
        logging.info(f"🔍 Payload: {payload}")
        
        return token
        
    except Exception as e:
        logging.error(f"❌ Manual token generation failed: {e}")
        raise

# Initialize LiveKit service
try:
    if LIVEKIT_AVAILABLE:
        livekit_service = ProductionLiveKitService()
        LIVEKIT_ENABLED = True
        logging.info("✅ LiveKit service initialized successfully")
    else:
        livekit_service = None
        LIVEKIT_ENABLED = False
        logging.warning("⚠️ LiveKit service not available")
except Exception as e:
    LIVEKIT_ENABLED = False
    livekit_service = None
    logging.warning(f"⚠️ LiveKit service initialization failed: {e}")

def create_meeting_id():
    chars = string.ascii_lowercase + string.digits  # a-z + 0-9
    
    part1 = ''.join(random.choices(chars, k=4))  # 4 chars
    part2 = ''.join(random.choices(chars, k=4))  # 4 chars
    part3 = ''.join(random.choices(chars, k=3))  # 3 chars
    
    return f"{part1}-{part2}-{part3}"  # total length = 13 chars

class Meetings(models.Model):
    id = models.CharField(
        primary_key=True,
        max_length=20,
        default=create_meeting_id,   # 13-char Google Meet style
        editable=False
    )
    host_id = models.ForeignKey(User, on_delete=models.DO_NOTHING, related_name='meetings')
    meeting_name = models.CharField(max_length=200, blank=True, null=True)
    meeting_type = models.CharField(max_length=50, choices=[
        ('CalendarMeeting', 'CalendarMeeting'),
        ('ScheduleMeeting', 'ScheduleMeeting'),
        ('InstantMeeting', 'InstantMeeting')
    ], blank=True, null=True)
    meeting_link = models.CharField(max_length=500, blank=True, null=True)
    status = models.CharField(max_length=50, choices=[
        ('active', 'active'),
        ('ended', 'ended'),
        ('scheduled', 'scheduled')
    ], default='active', blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now, blank=True, null=True)
    started_at = models.DateTimeField(blank=True, null=True)
    ended_at = models.DateTimeField(blank=True, null=True)
    is_recording_enabled = models.BooleanField(default=False, blank=True, null=True)
    waiting_room_enabled = models.BooleanField(default=False, blank=True, null=True)
    livekit_room_name = models.CharField(max_length=100, blank=True, null=True)
    livekit_room_sid = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        db_table = 'tbl_Meetings'

    def __str__(self):
        return self.meeting_name or "Unnamed Meeting"

def create_meetings_table():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_Meetings (
                    ID VARCHAR(20) PRIMARY KEY,
                    Host_ID INT,
                    Meeting_Name VARCHAR(200) NULL,
                    Meeting_Type VARCHAR(50) NULL,
                    Meeting_Link VARCHAR(500) NULL,
                    Status VARCHAR(50) NULL DEFAULT 'active',
                    Created_At DATETIME DEFAULT CURRENT_TIMESTAMP,
                    Started_At DATETIME NULL,
                    Ended_At DATETIME NULL,
                    Is_Recording_Enabled BOOLEAN DEFAULT 0,
                    Waiting_Room_Enabled BOOLEAN DEFAULT 0,
                    LiveKit_Room_Name VARCHAR(100) NULL,
                    LiveKit_Room_SID VARCHAR(100) NULL,
                    CONSTRAINT FK_Meetings_Users FOREIGN KEY (Host_ID)
                        REFERENCES tbl_Users(ID)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT
                );
            """)
            
            # Add LiveKit columns if they don't exist
            try:
                cursor.execute("ALTER TABLE tbl_Meetings ADD COLUMN LiveKit_Room_Name VARCHAR(100) NULL")
            except:
                pass  # Column already exists
            
            try:
                cursor.execute("ALTER TABLE tbl_Meetings ADD COLUMN LiveKit_Room_SID VARCHAR(100) NULL")
            except:
                pass  # Column already exists
                
            logging.debug("tbl_Meetings table created or exists with LiveKit columns")
    except Exception as e:
        logging.error(f"Failed to create tbl_Meetings table: {e}")

def create_scheduled_meetings_table():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_ScheduledMeetings (
                    id VARCHAR(20) PRIMARY KEY,
                    host_id INT NULL,
                    title VARCHAR(200) NULL,
                    description VARCHAR(1000) NULL,
                    location VARCHAR(255) NULL,
                    start_time DATETIME NULL,
                    end_time DATETIME NULL,
                    start_date DATETIME NULL,                -- NEW: Meeting visibility start date
                    end_date DATETIME NULL,                  -- NEW: Meeting visibility end date
                    timezone VARCHAR(100) NULL,
                    duration_minutes INT NULL,
                    is_recurring BOOLEAN DEFAULT 0,
                    recurrence_type VARCHAR(50) NULL,
                    recurrence_interval INT NULL,
                    recurrence_occurrences INT NULL,
                    recurrence_end_date DATETIME NULL,
                    selected_days TEXT NULL,                
                    selected_month_dates TEXT NULL,         
                    monthly_pattern VARCHAR(50) NULL DEFAULT 'same-date',
                    settings_waiting_room BOOLEAN DEFAULT 0,
                    settings_recording BOOLEAN DEFAULT 0,
                    settings_allow_chat BOOLEAN DEFAULT 1,
                    settings_allow_screen_share BOOLEAN DEFAULT 1,
                    settings_mute_participants BOOLEAN DEFAULT 0,
                    settings_require_password BOOLEAN DEFAULT 0,
                    settings_password VARCHAR(100) NULL,
                    reminders_email BOOLEAN DEFAULT 1,
                    reminders_browser BOOLEAN DEFAULT 1,
                    reminders_times VARCHAR(500) NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    email TEXT NULL,
                    CONSTRAINT FK_tbl_ScheduledMeetings_Users FOREIGN KEY (host_id)
                        REFERENCES tbl_Users(ID)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,
                    CONSTRAINT FK_Sched_Meeting_MeetingID FOREIGN KEY (id)
                        REFERENCES tbl_Meetings(ID)
                        ON DELETE CASCADE
                );
            """)
            logging.debug("✅ tbl_ScheduledMeetings table created or already exists with new date columns.")
    except Exception as e:
        logging.error(f"❌ Failed to create tbl_ScheduledMeetings table: {e}")

def create_calendar_meeting_table():
    """Create calendar meetings table with calendar integration support"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_CalendarMeetings (
                    ID VARCHAR(20) PRIMARY KEY,
                    Host_ID INT NULL,
                    title VARCHAR(255) NULL,
                    startTime DATETIME NULL,
                    endTime DATETIME NULL,
                    duration INT NULL,
                    email text NULL,
                    guestEmails text NULL,
                    provider VARCHAR(50) NULL,
                    meetingUrl VARCHAR(512) NULL,
                    location VARCHAR(255) NULL,
                    attendees TEXT NULL,
                    reminderMinutes TEXT NULL,
                    Settings_CreateCalendarEvent BOOLEAN DEFAULT 0,
                    Settings_SendInvitations BOOLEAN DEFAULT 0,
                    Settings_SetReminders BOOLEAN DEFAULT 0,
                    Settings_AddMeetingLink BOOLEAN DEFAULT 0,
                    Settings_AddToHostCalendar BOOLEAN DEFAULT 1,
                    Settings_AddToParticipantCalendars BOOLEAN DEFAULT 1,
                    CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT FK_CalendarMeetings_Users FOREIGN KEY (Host_ID)
                        REFERENCES tbl_Users(ID)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,
                    CONSTRAINT FK_CalendarMeetings_Meetings FOREIGN KEY (ID)
                        REFERENCES tbl_Meetings(ID)
                        ON DELETE CASCADE
                        ON UPDATE RESTRICT
                )
            """)
            logging.debug("tbl_CalendarMeetings table created or exists with calendar integration")
    except Exception as e:
        logging.error(f"Failed to create tbl_CalendarMeetings table: {e}")

def send_meeting_invitations(data):
    """
    Send meeting invitations - handles both Calendar and Schedule meetings
    """
    meeting_title = data.get('meeting_title', 'Meeting')
    guest_emails = data.get('guest_emails', [])
    meeting_type = data.get('meeting_type', '')
    start_time = data.get('start_time', '')
    duration = data.get('duration')
    meeting_url = data.get('meeting_url', '')
    custom_subject = data.get('subject', '')
    
    # Calendar meeting specific data
    organizer_email = data.get('organizer_email', '')
    location = data.get('location', '')
    meeting_id = data.get('meeting_id', '')
    description = data.get('description', '')
    is_recurring = data.get('is_recurring', False)
    recurrence_info = data.get('recurrence_info', {})
    
    if not guest_emails:
        logging.warning("No guest emails provided")
        return 0, ['No guest emails']
    
    # Ensure guest_emails is a list
    if isinstance(guest_emails, str):
        guest_emails = [email.strip() for email in guest_emails.split(',') if email.strip()]
    
    def send_emails_core():
        try:
            logging.info(f"Sending emails to {len(guest_emails)} recipients for {meeting_type}")
            
            # Format start time
            formatted_start_time = start_time
            calendar_dates = ""
            calendar_end_dates = ""
            
            if isinstance(start_time, str) and start_time:
                try:
                    dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    ist_timezone = pytz.timezone("Asia/Kolkata")
                    if dt.tzinfo:
                        dt = dt.astimezone(ist_timezone)
                    else:
                        dt = ist_timezone.localize(dt)
                    
                    formatted_start_time = dt.strftime('%A, %B %d, %Y at %I:%M %p')
                    calendar_dates = dt.strftime('%Y%m%dT%H%M%S')
                    end_dt = dt + timedelta(minutes=int(duration))
                    calendar_end_dates = end_dt.strftime('%Y%m%dT%H%M%S')
                except Exception as e:
                    logging.error(f"Error formatting date: {e}")
            
            # Create calendar link
            calendar_url = f"https://calendar.google.com/calendar/render?action=TEMPLATE&text={meeting_title.replace(' ', '+')}&dates={calendar_dates}Z/{calendar_end_dates}Z&details=Join+meeting:+{meeting_url}"
            
            # Create subject
            subject = custom_subject if custom_subject else f"Meeting Invitation: {meeting_title}"
            
            # Create message body
            location_section = f"📍 Location: {location}\n" if location else ""
            description_section = f"📝 Description: {description}\n\n" if description else ""
            recurring_section = ""
            
            if is_recurring and recurrence_info:
                recurring_section = f"""🔁 This is a recurring meeting.
Pattern: {recurrence_info.get('type', 'Weekly')} recurring
{f"Ends on: {recurrence_info.get('end_date')}" if recurrence_info.get('end_date') else "Continues indefinitely"}

"""
            
            if meeting_type == 'CalendarMeeting':
                message = f"""Hello,

You are invited to join a calendar meeting:

🆔 Meeting: {meeting_title}
🗓 Date & Time: {formatted_start_time}
⏰ Duration: {duration} minutes
💻 Join Link: {meeting_url}

{location_section}{description_section}📌 Meeting Details:
- 🆔 Meeting ID: {meeting_id}
- 👤 Host: {organizer_email or 'Meeting Host'}

⚠ Important Reminders:
- ⏳ Please join 5 minutes before the scheduled time
- 🎥 Ensure your camera and microphone are working
- 🌐 Have a stable internet connection

➕ Add to Calendar: {calendar_url}

Best regards,  
Meet Pro Team"""

            elif meeting_type == 'ScheduleMeeting':
                message = f"""Hello,

You are invited to join a 📅 scheduled meeting:

🆔 Meeting: {meeting_title}
🗓 Date & Time: {formatted_start_time}
⏰ Duration: {duration} minutes
💻 Join Link: {meeting_url}

{location_section}{description_section}{recurring_section}📌 Meeting Details:
- 🆔 Meeting ID: {meeting_id}
- 👤 Host: {organizer_email or 'Meeting Host'}

⚠ Important Reminders:
- ⏳ Please join 5 minutes before the scheduled time
- 🎥 Ensure your camera and microphone are working
- 🌐 Have a stable internet connection

➕ Add to Calendar: {calendar_url}

Best regards,  
Meet Pro Team"""

            else:
                message = f"""Hello,

You are invited to join a meeting:

🆔 Meeting: {meeting_title}
🗓 Date & Time: {formatted_start_time}
⏰ Duration: {duration} minutes
💻 Join Link: {meeting_url}

Best regards,  
Meet Pro Team"""

            # Send emails
            successful_sends = 0
            failed_emails = []
            
            batch_size = 10
            for i in range(0, len(guest_emails), batch_size):
                batch = guest_emails[i:i + batch_size]
                
                try:
                    email_message = EmailMessage(
                        subject=subject,
                        body=message,
                        from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', 'noreply@meetpro.com'),
                        to=batch,
                    )
                    
                    email_message.send(fail_silently=False)
                    successful_sends += len(batch)
                    logging.info(f"Successfully sent batch of {len(batch)} emails")
                    time.sleep(0.5)
                    
                except Exception as e:
                    logging.error(f"Failed to send email batch: {e}")
                    failed_emails.extend(batch)
            
            logging.info(f"Email sending completed: {successful_sends}/{len(guest_emails)} emails sent")
            return successful_sends, failed_emails
                
        except Exception as e:
            logging.error(f"Critical error in email sending: {e}")
            return 0, guest_emails
    
    # Calendar meetings send synchronously, Schedule meetings send asynchronously
    if meeting_type == 'CalendarMeeting':
        logging.info(f"Sending emails synchronously for {meeting_type}")
        sent_count, failed_list = send_emails_core()
        return sent_count, failed_list
    else:
        # For Schedule meetings - send asynchronously
        def send_emails_async():
            send_emails_core()
        
        email_thread = threading.Thread(target=send_emails_async)
        email_thread.daemon = True
        email_thread.start()
        
        logging.info(f"Started async email sending for {len(guest_emails)} invitations")
        return len(guest_emails), []

@csrf_exempt
def get_all_meetings(request):
    try:
        with connection.cursor() as cursor:
            # ✅ Get today's date as a string (your style)
            today = datetime.now().strftime('%Y-%m-%d')

            # ✅ Show all meetings today or later
            select_query = f"""
                SELECT * FROM {TBL_MEETINGS}
                WHERE Meeting_Date >= %s
                ORDER BY Meeting_Date ASC, Meeting_Time ASC
            """
            cursor.execute(select_query, [today])
            rows = cursor.fetchall()

            meetings = []
            for row in rows:
                meetings.append({
                    "Meeting_ID": row[0],
                    "Meeting_Title": row[1],
                    "Meeting_Description": row[2],
                    "Meeting_Date": row[3],
                    "Meeting_Time": row[4],
                    "Meeting_Status": row[5],
                    "Trainer_ID": row[6],
                    "Trainer_Name": row[7],
                    "Created_On": row[8],
                    "Updated_On": row[9]
                })

        return JsonResponse({"data": meetings}, status=200)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def calculate_meeting_status(started_at, ended_at, duration_minutes=60):
    """
    Calculate real-time meeting status based on current time
    Returns: 'scheduled', 'inprogress', or 'ended'
    """
    from datetime import datetime, timedelta
    import pytz
    
    try:
        # Get current IST time
        ist_timezone = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist_timezone)
        
        # Handle timezone-naive datetime objects
        if started_at and isinstance(started_at, datetime):
            if started_at.tzinfo is None:
                started_at = ist_timezone.localize(started_at)
            else:
                started_at = started_at.astimezone(ist_timezone)
        
        if ended_at and isinstance(ended_at, datetime):
            if ended_at.tzinfo is None:
                ended_at = ist_timezone.localize(ended_at)
            else:
                ended_at = ended_at.astimezone(ist_timezone)
        
        # If no start time, meeting is scheduled
        if not started_at:
            return 'scheduled'
            
        # Meeting hasn't started yet
        if current_time < started_at:
            return 'scheduled'
            
        # If we have an end time, use it
        if ended_at:
            if current_time > ended_at:
                return 'ended'
            else:
                return 'inprogress'
        else:
            # No end time - calculate based on start time + duration
            calculated_end = started_at + timedelta(minutes=duration_minutes)
            if current_time > calculated_end:
                return 'ended'
            else:
                return 'inprogress'
                
    except Exception as e:
        logging.error(f"Error calculating meeting status: {e}")
        return 'scheduled'  # Safe fallback

@require_http_methods(["POST"])
@csrf_exempt
def Create_Calendar_Meeting(request):
    """FIXED: Create Calendar Meeting with duplicate prevention + fully working mail + notification system"""
    create_meetings_table()
    create_calendar_meeting_table()
    ensure_notification_tables()

    try:
        # --- Parse request data ---
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            return JsonResponse({"Error": "Expected a single meeting object"}, status=400)
    except json.JSONDecodeError as e:
        return JsonResponse({"Error": f"Invalid JSON: {e}"}, status=400)

    # --- Timezone ---
    ist = pytz.timezone("Asia/Kolkata")

    def parse_datetime(dt_str):
        if not dt_str:
            return None
        try:
            if isinstance(dt_str, str):
                dt_str = dt_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
            else:
                dt = dt_str
            if dt.tzinfo is None:
                dt = ist.localize(dt)
            else:
                dt = dt.astimezone(ist)
            return dt
        except Exception as e:
            logging.error(f"Datetime parse error: {e}")
            return None

    # --- Extract required fields ---
    host_id = data.get('Host_ID') or data.get('host_id')
    title = data.get('Meeting_Name') or data.get('title')
    if not host_id:
        return JsonResponse({"Error": "Host_ID is required"}, status=400)
    if not title:
        return JsonResponse({"Error": "Meeting title is required"}, status=400)

    start_dt = parse_datetime(data.get('startTime') or data.get('start_time'))
    end_dt = parse_datetime(data.get('endTime') or data.get('end_time'))
    
    # === PREVENT PAST MEETINGS ===
    now = datetime.now(ist)

    if start_dt and start_dt < now:
        return JsonResponse({"Error": "Cannot create meeting in past time"}, status=400)

    # === CRITICAL FIX: PREVENT DUPLICATE MEETINGS ===
    try:
        with connection.cursor() as cursor:
            # Check for duplicate meeting created within last 30 seconds with same host, title, and time
            cursor.execute("""
                SELECT ID, Meeting_Link, Meeting_Name 
                FROM tbl_Meetings 
                WHERE Host_ID = %s 
                AND Meeting_Name = %s 
                AND Started_At = %s 
                AND Ended_At = %s
                AND Created_At > DATE_SUB(NOW(), INTERVAL 30 SECOND)
                LIMIT 1
            """, [host_id, title, start_dt, end_dt])
            
            existing_meeting = cursor.fetchone()
            if existing_meeting:
                logging.warning(f"⚠️ Duplicate meeting detected, returning existing: {existing_meeting[0]}")
                return JsonResponse({
                    "Message": "Meeting already exists (duplicate prevented)",
                    "Meeting_ID": existing_meeting[0],
                    "Meeting_Link": existing_meeting[1],
                    "Meeting_Name": existing_meeting[2],
                    "duplicate": True
                }, status=200)
    except Exception as e:
        logging.warning(f"Duplicate check query failed (continuing): {e}")

    # meeting_uuid = str(uuid.uuid4())
    meeting_id = create_meeting_id()   # 13-char ID
    duration = data.get('duration') or data.get('duration_minutes') or (int((end_dt - start_dt).total_seconds() / 60) if start_dt and end_dt else 60)
    duration = int(duration)
    status = calculate_meeting_status(start_dt, end_dt, duration)

    # --- Base meeting data ---
    meeting_url = f"https://imeetpro.lancieretech.com/meeting/{meeting_id}"
    livekit_room = f"meeting_{meeting_id}"
    created_at = timezone.now().astimezone(ist).strftime('%Y-%m-%d %H:%M:%S')

    # --- Participants ---
    participants = data.get('Participants') or data.get('guestEmails') or data.get('attendees')
    guest_emails = []
    if isinstance(participants, str):
        guest_emails = [e.strip() for e in participants.split(',') if e.strip() and '@' in e]
    elif isinstance(participants, list):
        for p in participants:
            if isinstance(p, dict) and p.get('email'):
                guest_emails.append(p['email'].strip())
            elif isinstance(p, str) and '@' in p:
                guest_emails.append(p.strip())
    guest_emails = list(dict.fromkeys(guest_emails))  # remove duplicates
    logging.info(f"Found {len(guest_emails)} guest emails: {guest_emails}")

    # --- Validate host ---
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM tbl_Users WHERE ID=%s AND Status=1", [host_id])
            if cursor.fetchone()[0] == 0:
                return JsonResponse({"Error": "Invalid or inactive Host_ID"}, status=400)
    except Exception as e:
        return JsonResponse({"Error": str(e)}, status=500)

    # --- Insert into DB ---
    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO tbl_Meetings (
                        ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status,
                        Created_At, Started_At, Ended_At, Is_Recording_Enabled,
                        Waiting_Room_Enabled, LiveKit_Room_Name
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, [
                    meeting_id, host_id, title, 'CalendarMeeting', meeting_url, status,
                    created_at, start_dt, end_dt,
                    data.get('Is_Recording_Enabled', 0),
                    data.get('Waiting_Room_Enabled', 0),
                    livekit_room
                ])

                cursor.execute("""
                    INSERT INTO tbl_CalendarMeetings (
                        ID, Host_ID, title, startTime, endTime, duration, email, guestEmails,
                        provider, meetingUrl, location, attendees, reminderMinutes,
                        Settings_CreateCalendarEvent, Settings_SendInvitations,
                        Settings_SetReminders, Settings_AddMeetingLink,
                        Settings_AddToHostCalendar, Settings_AddToParticipantCalendars, CreatedAt
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, [
                    meeting_id, host_id, title, start_dt, end_dt, duration,
                    data.get('email'), ",".join(guest_emails), data.get('provider', 'internal'),
                    meeting_url, data.get('location'), ";".join(guest_emails),
                    json.dumps(data.get('reminderTimes', [15, 30])),
                    1, 1, 1, 1, 1, 1, created_at
                ])
        logging.info(f"✅ Calendar meeting created: {meeting_id}")
    except Exception as e:
        logging.error(f"DB insert failed: {e}")
        return JsonResponse({"Error": str(e)}, status=500)

    # --- Notifications & reminders ---
    notification_results = {
        "in_app_notifications": {"sent": 0, "failed": 0},
        "reminders_scheduled": 0
    }

    if guest_emails:
        start_time_str = start_dt.strftime('%Y-%m-%d %H:%M:%S') if start_dt else None
        notification_results["in_app_notifications"] = create_meeting_notifications(
            meeting_id=meeting_id,
            meeting_title=title,
            participant_emails=guest_emails,
            start_time=start_time_str,
            meeting_url=meeting_url
        )

        # Schedule reminder emails only once (30 minutes before the meeting)
        if start_dt and start_dt > datetime.now(ist):
            notification_results["reminders_scheduled"] = schedule_meeting_reminders(
                meeting_id=meeting_id,
                meeting_title=title,
                participant_emails=guest_emails,
                start_time=start_time_str,
                meeting_url=meeting_url,
                reminder_minutes=[30]  # Send reminder only 30 minutes before
            )

    # --- Host notification ---
    host_email = data.get('email') or _get_host_email_by_id(host_id)
    if host_email:
        create_host_notification(
            meeting_id=meeting_id,
            meeting_title=title,
            host_email=host_email,
            start_time=start_dt.strftime('%Y-%m-%d %H:%M:%S') if start_dt else None,
            meeting_url=meeting_url
        )

    # --- ✅ FIXED: Async Email Sending (like ScheduleMeeting) ---
    if guest_emails:
        email_data = {
            'meeting_title': title,
            'guest_emails': guest_emails,
            'meeting_type': 'CalendarMeeting',
            'start_time': start_dt.strftime('%Y-%m-%d %H:%M:%S') if start_dt else '',
            'duration': duration,
            'meeting_url': meeting_url,
            'meeting_id': meeting_id,
            'organizer_email': host_email or 'noreply@meetpro.com',
            'location': data.get('location', ''),
            'description': data.get('description', ''),
            'subject': f"Meeting Invitation: {title}"
        }

        import threading
        def async_email_send():
            try:
                send_meeting_invitations(email_data)
                logging.info(f"Background email thread completed for {len(guest_emails)} participants")
            except Exception as e:
                logging.error(f"Async email send error: {e}")

        threading.Thread(target=async_email_send, daemon=True).start()
        logging.info(f"📧 Started background email sending for {len(guest_emails)} participants")

    # --- Final Response ---
    return JsonResponse({
        "Message": "CalendarMeeting created successfully with notifications and emails",
        "Meeting_ID": meeting_id,
        "Meeting_Link": meeting_url,
        "Meeting_Name": title,
        "Duration": duration,
        "Participants": len(guest_emails),
        "Participants_List": guest_emails,
        "Notification_Results": notification_results,
        "Host_Email": host_email,
        "status": status,
        "LiveKit_Room": livekit_room
    }, status=201)

@require_http_methods(["POST"])
@csrf_exempt
def Create_Schedule_Meeting(request):
    """COMPLETE: Create scheduled meeting with FULL notification system and status fix"""
    try:
        # Ensure tables exist - INCLUDING NOTIFICATION TABLES
        create_meetings_table()
        create_scheduled_meetings_table()
        ensure_notification_tables()  # PRESERVED: Notification tables

        # Parse JSON data - UNCHANGED
        try:
            data = json.loads(request.body)
            logging.info(f"Received meeting creation request: {json.dumps(data, indent=2, default=str)}")
            
            if isinstance(data, list) and len(data) == 1:
                data = data[0]
            elif isinstance(data, list):
                return JsonResponse({"Error": "Expected a single meeting object, not a list"}, status=400)
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON: {e}")
            return JsonResponse({"Error": "Invalid JSON format"}, status=400)

        # IST timezone setup - UNCHANGED
        ist_timezone = pytz.timezone("Asia/Kolkata")
        
        def safe_datetime_convert(dt_str, field_name):
            """Safely convert datetime string with comprehensive error handling - UNCHANGED"""
            if not dt_str:
                logging.warning(f"No datetime provided for {field_name}")
                return None
            
            try:
                if isinstance(dt_str, str):
                    try:
                        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
                    except ValueError:
                        from dateutil import parser
                        dt = parser.parse(dt_str)
                    
                    if dt.tzinfo is None:
                        dt = ist_timezone.localize(dt)
                    else:
                        dt = dt.astimezone(ist_timezone)
                    
                    result = dt.strftime('%Y-%m-%d %H:%M:%S')
                    logging.info(f"Converted {field_name}: {dt_str} -> {result}")
                    return result
                    
                return dt_str
            except Exception as e:
                logging.error(f"Failed to parse {field_name} datetime '{dt_str}': {e}")
                return datetime.now(ist_timezone).strftime('%Y-%m-%d %H:%M:%S')

        # Validate required fields - UNCHANGED
        required_fields = ['Meeting_Name', 'Host_ID']
        for field in required_fields:
            if not data.get(field):
                error_msg = f"{field} is required"
                logging.error(f"Validation error: {error_msg}")
                return JsonResponse({"Error": error_msg}, status=400)

        # Host ID validation and conversion - UNCHANGED
        host_id = data.get('Host_ID')
        if host_id == 'default-host':
            host_id = 1
            logging.info("Converted 'default-host' to Host_ID: 1")
        
        try:
            host_id = int(host_id)
            data['Host_ID'] = host_id
        except (ValueError, TypeError):
            logging.error(f"Invalid Host_ID: {host_id}")
            return JsonResponse({"Error": "Host_ID must be a valid integer"}, status=400)

        # Validate host exists in database - UNCHANGED
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM tbl_Users WHERE ID = %s", [host_id])
                if cursor.fetchone()[0] == 0:
                    logging.error(f"Host ID {host_id} not found in database")
                    return JsonResponse({"Error": f"Host_ID {host_id} does not exist"}, status=400)
        except Exception as e:
            logging.error(f"Database error checking host: {e}")
            return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)

        # Generate UUID - UNCHANGED
        try:
            # meeting_uuid = str(uuid.uuid4())
            meeting_id = create_meeting_id()
            logging.info(f"Generated meeting ID: {meeting_id}")
        except Exception as e:
            logging.error(f"Failed to generate UUID: {e}")
            return JsonResponse({"Error": "Failed to generate meeting ID"}, status=500)

        # Duration handling - UNCHANGED
        duration_minutes = int(data.get('duration_minutes', 60))
        logging.info(f"Meeting duration: {duration_minutes} minutes")

        # Safe datetime conversion for start time - UNCHANGED
        started_at = safe_datetime_convert(data.get('Started_At'), 'Started_At')
        
        # Calculate end time based on start time + duration - UNCHANGED
        ended_at = None
        if started_at:
            try:
                start_dt = datetime.strptime(started_at, '%Y-%m-%d %H:%M:%S')
                start_dt = ist_timezone.localize(start_dt)
                end_dt = start_dt + timedelta(minutes=duration_minutes)
                ended_at = end_dt.strftime('%Y-%m-%d %H:%M:%S')
                logging.info(f"Calculated end time - Start: {started_at}, Duration: {duration_minutes}min, End: {ended_at}")
            except Exception as e:
                logging.error(f"Failed to calculate end time: {e}")
                ended_at = safe_datetime_convert(data.get('Ended_At'), 'Ended_At')
        else:
            ended_at = safe_datetime_convert(data.get('Ended_At'), 'Ended_At')

        # === PREVENT PAST MEETINGS ===
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)

        # Convert string → datetime before check
        if started_at:
            start_dt_check = datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S")
            start_dt_check = ist.localize(start_dt_check)

            if start_dt_check < now:
                return JsonResponse({"Error": "Cannot create meeting in past time"}, status=400)

        
        # Handle start_date and end_date from frontend - UNCHANGED
        start_date = safe_datetime_convert(data.get('start_date'), 'start_date')
        end_date = safe_datetime_convert(data.get('end_date'), 'end_date')
        
        if not start_date:
            start_date = started_at or datetime.now(ist_timezone).strftime('%Y-%m-%d %H:%M:%S')
        
        if not end_date:
            end_date = ended_at or datetime.now(ist_timezone).strftime('%Y-%m-%d %H:%M:%S')

        created_at = datetime.now(ist_timezone).strftime('%Y-%m-%d %H:%M:%S')

        # Process participant emails - UNCHANGED
        participants_data = data.get('participants') or data.get('Participants') or []
        email_str = data.get('email') or data.get('Email') or ''
        
        if not email_str and participants_data:
            try:
                if isinstance(participants_data, str):
                    participants_data = json.loads(participants_data)
                
                if isinstance(participants_data, list):
                    emails = []
                    for p in participants_data:
                        if isinstance(p, dict) and p.get('email'):
                            emails.append(p['email'].strip())
                        elif isinstance(p, str) and '@' in p:
                            emails.append(p.strip())
                    
                    email_str = ",".join(emails)
                    logging.info(f"Extracted participant emails: {email_str}")
                    
            except Exception as e:
                logging.warning(f"Failed to extract participant emails: {e}")
                email_str = ''

        # Process recurrence data - UNCHANGED (ALL ORIGINAL RECURRENCE LOGIC)
        recurring_data = data.get('recurrence', {})
        selected_days = None
        selected_month_dates = None
        monthly_pattern = None
        is_recurring = False
        recurrence_type = None
        recurrence_interval = None
        recurrence_occurrences = None
        recurrence_end_date = None
        
        if bool(recurring_data.get('enabled')):
            is_recurring = True
            recurrence_type = recurring_data.get('type')
            recurrence_interval = recurring_data.get('interval', 1) if recurrence_type else None
            
            if recurring_data.get('endDate') and not recurring_data.get('occurrences'):
                recurrence_occurrences = None
                recurrence_end_date = safe_datetime_convert(recurring_data.get('endDate'), 'recurrence_end_date')
            elif recurring_data.get('occurrences') and not recurring_data.get('endDate'):
                recurrence_occurrences = recurring_data.get('occurrences')
                recurrence_end_date = None
            else:
                if recurring_data.get('endDate'):
                    recurrence_end_date = safe_datetime_convert(recurring_data.get('endDate'), 'recurrence_end_date')
                    recurrence_occurrences = None
                else:
                    recurrence_occurrences = recurring_data.get('occurrences')
                    recurrence_end_date = None
            
            if recurrence_type == 'weekly':
                if 'selectedDays' in recurring_data and isinstance(recurring_data['selectedDays'], list):
                    selected_days = json.dumps(recurring_data['selectedDays'])
                selected_month_dates = None
                monthly_pattern = None
            elif recurrence_type == 'monthly':
                if 'selectedMonthDates' in recurring_data and isinstance(recurring_data['selectedMonthDates'], list):
                    selected_month_dates = json.dumps(recurring_data['selectedMonthDates'])
                monthly_pattern = recurring_data.get('monthlyPattern', 'same-date')
                selected_days = None
            elif recurrence_type == 'daily':
                selected_days = None
                selected_month_dates = None
                monthly_pattern = None
        
        if not is_recurring:
            recurrence_type = None
            recurrence_interval = None
            recurrence_occurrences = None
            recurrence_end_date = None
            selected_days = None
            selected_month_dates = None
            monthly_pattern = None

        # Process settings data - UNCHANGED (ALL ORIGINAL SETTINGS LOGIC)
        settings = data.get('settings', {})
        settings_waiting_room = 1 if settings.get('waitingRoom', data.get('settings_waiting_room', True)) else 0
        settings_recording = 1 if settings.get('recording', data.get('settings_recording', False)) else 0
        settings_allow_chat = 1 if settings.get('allowChat', data.get('settings_allow_chat', True)) else 0
        settings_allow_screen_share = 1 if settings.get('allowScreenShare', data.get('settings_allow_screen_share', True)) else 0
        settings_mute_participants = 1 if settings.get('muteParticipants', data.get('settings_mute_participants', False)) else 0
        settings_require_password = 1 if settings.get('requirePassword', data.get('settings_require_password', False)) else 0
        settings_password = settings.get('password', data.get('settings_password'))

        # Process reminders data - UNCHANGED (ALL ORIGINAL REMINDERS LOGIC)
        reminders = data.get('reminders', {})
        reminders_email = 1 if reminders.get('email', data.get('reminders_email', True)) else 0
        reminders_browser = 1 if reminders.get('browser', data.get('reminders_browser', True)) else 0
        
        reminders_times = reminders.get('reminderTimes', data.get('reminders_times', [15, 5]))
        try:
            if isinstance(reminders_times, list):
                reminders_times = json.dumps(reminders_times)
            elif not isinstance(reminders_times, str):
                reminders_times = json.dumps([15, 5])
        except Exception as e:
            logging.warning(f"Failed to process reminders: {e}")
            reminders_times = json.dumps([15, 5])

        # ONLY CHANGE: Calculate initial status based on time
        initial_status = calculate_meeting_status(
            datetime.strptime(started_at, '%Y-%m-%d %H:%M:%S') if started_at else None,
            datetime.strptime(ended_at, '%Y-%m-%d %H:%M:%S') if ended_at else None,
            duration_minutes
        )

        # Prepare meeting data - UNCHANGED except status calculation
        meeting_data = {
            'id': meeting_id,
            'host_id': host_id,
            'meeting_name': data.get('Meeting_Name', '').strip(),
            'meeting_type': 'ScheduleMeeting',
            'meeting_link': f"https://imeetpro.lancieretech.com/meeting/{meeting_id}",
            'livekit_room_name': f"meeting_{meeting_id}",
            'status': initial_status,  # CHANGED: Use calculated status
            'started_at': started_at,
            'ended_at': ended_at,
            'created_at': created_at,
            'is_recording_enabled': settings_recording,
            'waiting_room_enabled': settings_waiting_room,
            'title': data.get('title', data.get('Meeting_Name', '')).strip(),
            'description': data.get('description', ''),
            'location': data.get('location', ''),
            'start_date': start_date,
            'end_date': end_date,
            'timezone': data.get('timezone', 'Asia/Kolkata'),
            'duration_minutes': duration_minutes,
            'is_recurring': 1 if is_recurring else 0,
            'recurrence_type': recurrence_type,
            'recurrence_interval': recurrence_interval,
            'recurrence_occurrences': recurrence_occurrences,
            'recurrence_end_date': recurrence_end_date,
            'selected_days': selected_days,
            'selected_month_dates': selected_month_dates,
            'monthly_pattern': monthly_pattern,
            'settings_waiting_room': settings_waiting_room,
            'settings_recording': settings_recording,
            'settings_allow_chat': settings_allow_chat,
            'settings_allow_screen_share': settings_allow_screen_share,
            'settings_mute_participants': settings_mute_participants,
            'settings_require_password': settings_require_password,
            'settings_password': settings_password,
            'email': email_str,
            'reminders_email': reminders_email,
            'reminders_browser': reminders_browser,
            'reminders_times': reminders_times
        }

        # Database transaction - UNCHANGED (ALL ORIGINAL DATABASE LOGIC)
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    # Insert into tbl_Meetings
                    meetings_query = """
                        INSERT INTO tbl_Meetings (
                            ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status, Created_At,
                            Started_At, Ended_At, Is_Recording_Enabled, Waiting_Room_Enabled, LiveKit_Room_Name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    meetings_params = [
                        meeting_data['id'], meeting_data['host_id'], meeting_data['meeting_name'],
                        meeting_data['meeting_type'], meeting_data['meeting_link'], meeting_data['status'],
                        meeting_data['created_at'], meeting_data['started_at'], meeting_data['ended_at'],
                        meeting_data['is_recording_enabled'], meeting_data['waiting_room_enabled'],
                        meeting_data['livekit_room_name']
                    ]
                    
                    cursor.execute(meetings_query, meetings_params)
                    
                    # Insert into tbl_ScheduledMeetings
                    scheduled_query = """
                        INSERT INTO tbl_ScheduledMeetings (
                            id, host_id, title, description, location, start_time, end_time, start_date, end_date, timezone,
                            duration_minutes, is_recurring, recurrence_type, recurrence_interval,
                            recurrence_occurrences, recurrence_end_date,
                            selected_days, selected_month_dates, monthly_pattern,
                            settings_waiting_room, settings_recording, settings_allow_chat,
                            settings_allow_screen_share, settings_mute_participants,
                            settings_require_password, settings_password, email,
                            reminders_email, reminders_browser, reminders_times, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    scheduled_params = [
                        meeting_data['id'], meeting_data['host_id'], meeting_data['title'],
                        meeting_data['description'], meeting_data['location'], meeting_data['started_at'],
                        meeting_data['ended_at'], meeting_data['start_date'], meeting_data['end_date'],
                        meeting_data['timezone'], meeting_data['duration_minutes'], meeting_data['is_recurring'],
                        meeting_data['recurrence_type'], meeting_data['recurrence_interval'],
                        meeting_data['recurrence_occurrences'], meeting_data['recurrence_end_date'],
                        meeting_data['selected_days'], meeting_data['selected_month_dates'],
                        meeting_data['monthly_pattern'], meeting_data['settings_waiting_room'],
                        meeting_data['settings_recording'], meeting_data['settings_allow_chat'],
                        meeting_data['settings_allow_screen_share'], meeting_data['settings_mute_participants'],
                        meeting_data['settings_require_password'], meeting_data['settings_password'],
                        meeting_data['email'], meeting_data['reminders_email'], meeting_data['reminders_browser'],
                        meeting_data['reminders_times'], meeting_data['created_at']
                    ]
                    
                    cursor.execute(scheduled_query, scheduled_params)
                    logging.info("Database inserts completed successfully")
                    
        except Exception as e:
            logging.error(f"Database transaction failed: {str(e)}")
            return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)

        # PRESERVED: COMPLETE NOTIFICATION SYSTEM
        participant_emails = []
        if email_str:
            participant_emails = [email.strip() for email in email_str.split(',') if email.strip() and '@' in email]
        
        notification_results = {
            "in_app_notifications": {"sent": 0, "failed": 0},
            "reminders_scheduled": 0
        }
        
        if participant_emails:
            # Create immediate in-app notifications
            notification_results["in_app_notifications"] = create_meeting_notifications(
                meeting_id=meeting_id,
                meeting_title=meeting_data['title'] or meeting_data['meeting_name'],
                participant_emails=participant_emails,
                start_time=started_at,
                meeting_url=meeting_data['meeting_link']
            )
            
            # Schedule reminder emails only once (30 minutes before the meeting)
            if reminders_browser and started_at:
                reminder_times = json.loads(reminders_times)
                notification_results["reminders_scheduled"] = schedule_meeting_reminders(
                    meeting_id=meeting_id,
                    meeting_title=meeting_data['title'] or meeting_data['meeting_name'],
                    participant_emails=participant_emails,
                    start_time=started_at,
                    meeting_url=meeting_data['meeting_link'],
                    reminder_minutes=reminder_times
                )
        
        # PRESERVED: Host-side notification
        host_email = data.get('Host_Email') or _get_host_email_by_id(host_id)
        create_host_notification(
            meeting_id=meeting_id,
            meeting_title=meeting_data['title'] or meeting_data['meeting_name'],
            host_email=host_email,
            start_time=started_at,
            meeting_url=meeting_data['meeting_link']
        )

        # PRESERVED: EMAIL SENDING LOGIC
        email_will_be_sent = False
        participant_count = len(participant_emails)
        
        if email_str and reminders_email:
            if participant_emails:
                email_will_be_sent = True
                
                # Prepare email data
                email_data = {
                    'meeting_title': meeting_data['title'] or meeting_data['meeting_name'],
                    'guest_emails': participant_emails,
                    'meeting_type': 'ScheduleMeeting',
                    'start_time': started_at,
                    'duration': duration_minutes,
                    'meeting_url': meeting_data['meeting_link'],
                    'meeting_id': meeting_id,
                    'location': meeting_data['location'],
                    'description': meeting_data['description'],
                    'is_recurring': is_recurring,
                    'recurrence_info': {
                        'type': recurrence_type,
                        'end_date': recurrence_end_date
                    } if is_recurring else None
                }
                
                # Start email sending in background thread
                import threading
                email_thread = threading.Thread(
                    target=send_meeting_invitations,
                    args=(email_data,),
                    daemon=True
                )
                email_thread.start()
                
                logging.info(f"Started background email sending for {participant_count} participants")
            else:
                logging.info("No valid participant emails found")

        # COMPLETE RESPONSE WITH ALL DATA INCLUDING NOTIFICATIONS
        response_data = {
            "Message": "ScheduleMeeting created successfully with notifications",
            "Meeting_ID": meeting_id,
            "Meeting_Link": meeting_data['meeting_link'],
            "LiveKit_Room": meeting_data['livekit_room_name'],
            "start_date": start_date,
            "end_date": end_date,
            "duration_minutes": duration_minutes,
            "calculated_end_time": ended_at,
            "is_recurring": is_recurring,
            "recurrence_type": recurrence_type,
            "participant_count": participant_count,
            "Participants_List": participant_emails,
            
            # PRESERVED: Notification results
            "Notification_Results": notification_results,
            "email_status": "sending_in_background" if email_will_be_sent else "no_emails_to_send",
            "notifications": f"In-app notifications sent to {participant_count} participants" if participant_count else "No participants to notify",
            
            # ADDED: Status information
            "status": initial_status,
            "status_calculated": True
        }
        
        logging.info(f"ScheduleMeeting created successfully with notifications - Meeting ID: {meeting_id}")
        return JsonResponse(response_data, status=201)
        
    except Exception as e:
        logging.error(f"Unexpected error in Create_Schedule_Meeting: {str(e)}")
        return JsonResponse({"Error": f"Internal server error: {str(e)}"}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def Create_Instant_Meeting(request):
    create_meetings_table()
    ist_timezone = pytz.timezone("Asia/Kolkata")

    # ------------------------- Parse JSON -------------------------
    try:
        data = json.loads(request.body)
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            return JsonResponse({"Error": "Expected a single meeting object, not a list"}, status=400)
    except:
        return JsonResponse({"Error": "Invalid JSON format"}, status=400)

    # ------------------------- Basic Data -------------------------
    data['Meeting_Type'] = 'InstantMeeting'
    data['Meeting_Name'] = data.get('Meeting_Name', 'Instant Meeting')

    # ------------------------- Validate Host -------------------------
    if not data.get('Host_ID'):
        return JsonResponse({"Error": "Host_ID is required"}, status=400)

    if data['Host_ID'] == "default-host":
        data['Host_ID'] = 1

    try:
        data['Host_ID'] = int(data['Host_ID'])
    except:
        return JsonResponse({"Error": "Host_ID must be an integer"}, status=400)

    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM tbl_Users WHERE ID=%s AND Status=1", [data['Host_ID']])
        if cursor.fetchone()[0] == 0:
            return JsonResponse({"Error": "Invalid or inactive Host_ID"}, status=400)

    # =====================================================================
    # ⭐ CRITICAL FIX — PREVENT DUPLICATE INSTANT MEETINGS
    # =====================================================================
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT ID
                FROM tbl_Meetings
                WHERE Host_ID = %s
                  AND Meeting_Type = 'InstantMeeting'
                  AND TIMESTAMPDIFF(SECOND, Created_At, NOW()) <= 10
                ORDER BY Created_At DESC
                LIMIT 1
            """, [data['Host_ID']])
            existing = cursor.fetchone()

        if existing:
            # If duplicate detected, return existing meeting
            return JsonResponse({
                "Message": "InstantMeeting already active",
                "Meeting_ID": existing[0],
                "Duplicate_Prevented": True
            }, status=200)

    except Exception as e:
        logging.error(f"Duplicate prevent error: {e}")
    # =====================================================================

    # ------------------------- Generate New Meeting -------------------------
    # meeting_uuid = str(uuid.uuid4())
    meeting_id = create_meeting_id()
    logging.info(f"Generated Meeting ID: {meeting_id}")

    base_url = "https://imeetpro.lancieretech.com"
    data['Meeting_Link'] = f"{base_url}/meeting/{meeting_id}"
    data['LiveKit_Room_Name'] = f"meeting_{meeting_id}"

    data['Status'] = data.get('Status', 'active')
    data['Is_Recording_Enabled'] = int(bool(data.get('Is_Recording_Enabled', 0)))
    data['Waiting_Room_Enabled'] = int(bool(data.get('Waiting_Room_Enabled', 0)))

    # now_ist = timezone.now().astimezone(ist_timezone).strftime('%Y-%m-%d %H:%M:%S')
    now_ist = timezone.now().astimezone(ist_timezone)
    data['Created_At'] = now_ist
    data['Started_At'] = None
    data['Ended_At'] = None

    # ------------------------- Insert New Meeting -------------------------
    livekit_room_sid = None
    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {TBL_MEETINGS} (
                        ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status,
                        Created_At, Started_At, Ended_At,
                        Is_Recording_Enabled, Waiting_Room_Enabled, LiveKit_Room_Name
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, [
                    meeting_id,
                    data['Host_ID'],
                    data['Meeting_Name'],
                    data['Meeting_Type'],
                    data['Meeting_Link'],
                    data['Status'],
                    data['Created_At'],
                    data['Started_At'],
                    data['Ended_At'],
                    data['Is_Recording_Enabled'],
                    data['Waiting_Room_Enabled'],
                    data['LiveKit_Room_Name']
                ])
    except Exception as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)

    # ------------------------- Create LiveKit Room -------------------------
    livekit_room = {}
    if LIVEKIT_ENABLED and livekit_service:
        try:
            livekit_room = livekit_service.create_room(
                room_name=data['LiveKit_Room_Name'],
                room_config={
                    'max_participants': 200,
                    'empty_timeout': 300,
                    'departure_timeout': 30,
                    'enable_recording': bool(data['Is_Recording_Enabled'])
                }
            )

            livekit_room_sid = (
                livekit_room.get('sid') or
                livekit_room.get('room_sid') or
                livekit_room.get('name')
            )

            if livekit_room_sid:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE tbl_Meetings
                        SET LiveKit_Room_SID=%s
                        WHERE ID=%s
                    """, [livekit_room_sid, meeting_id])

        except Exception as e:
            logging.warning(f"LiveKit room creation failed: {e}")

    # ------------------------- Return Response -------------------------
    return JsonResponse({
        "Message": "InstantMeeting created successfully",
        "Meeting_ID": meeting_id,
        "Meeting_Link": data['Meeting_Link'],
        "Meeting_Name": data['Meeting_Name'],
        "Status": data['Status'],
        "Host_ID": data['Host_ID'],
        "LiveKit_Room": data['LiveKit_Room_Name'],
        "LiveKit_URL": LIVEKIT_CONFIG['url'] if LIVEKIT_ENABLED else None,
        "LiveKit_Enabled": LIVEKIT_ENABLED,
        "LiveKit_Room_SID": livekit_room_sid,
        "LiveKit_Room_Created": livekit_room_sid is not None,
        "LiveKit_Response": livekit_room
    }, status=201)

@require_http_methods(["GET"])
@csrf_exempt
def List_All_Meetings(request):
    create_meetings_table()

    try:
        with connection.cursor() as cursor:
            select_query = f"""
            SELECT ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status, Created_At, 
                   Started_At, Ended_At, Is_Recording_Enabled, Waiting_Room_Enabled,
                   LiveKit_Room_Name, LiveKit_Room_SID
            FROM {TBL_MEETINGS}
            ORDER BY Created_At DESC
            LIMIT 50
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            meetings = []
            
            for row in rows:
                meeting = dict(zip([
                    'ID', 'Host_ID', 'Meeting_Name', 'Meeting_Type', 'Meeting_Link', 'Status', 'Created_At',
                    'Started_At', 'Ended_At', 'Is_Recording_Enabled', 'Waiting_Room_Enabled',
                    'LiveKit_Room_Name', 'LiveKit_Room_SID'
                ], row))
                
                # CRITICAL FIX: Skip slow LiveKit API calls in list view
                # Only add basic defaults to avoid timeouts
                meeting['LiveKit_Room_Active'] = False
                meeting['LiveKit_Participants'] = 0
                
                # OPTIONAL: Only check LiveKit for active meetings if really needed
                # Remove or comment out this block to eliminate timeouts completely:
                """
                if LIVEKIT_ENABLED and livekit_service and meeting['LiveKit_Room_Name'] and meeting['Status'] == 'active':
                    try:
                        # Use timeout for individual calls
                        import signal
                        def timeout_handler(signum, frame):
                            raise TimeoutError()
                        
                        signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(2)  # 2 second timeout
                        
                        room_info = livekit_service.get_room(meeting['LiveKit_Room_Name'])
                        if room_info:
                            participants = livekit_service.list_participants(meeting['LiveKit_Room_Name'])
                            meeting['LiveKit_Room_Active'] = True
                            meeting['LiveKit_Participants'] = len(participants)
                        
                        signal.alarm(0)  # Clear timeout
                    except (TimeoutError, Exception):
                        # Silently fail and use defaults
                        pass
                """
                
                meetings.append(meeting)
                
    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse(meetings, safe=False, status=SUCCESS_STATUS)

# ENHANCED: Get Meeting with LiveKit info
@require_http_methods(["GET"])
@csrf_exempt
def Get_Meeting(request, id):
    create_meetings_table()

    try:
        with connection.cursor() as cursor:
            select_query = f"""
            SELECT ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status, Created_At, 
                   Started_At, Ended_At, Is_Recording_Enabled, Waiting_Room_Enabled,
                   LiveKit_Room_Name, LiveKit_Room_SID
            FROM {TBL_MEETINGS}
            WHERE ID = %s
            """
            cursor.execute(select_query, [id])
            row = cursor.fetchone()
    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    if row:
        meeting = dict(zip([
            'ID', 'Host_ID', 'Meeting_Name', 'Meeting_Type', 'Meeting_Link', 'Status', 'Created_At',
            'Started_At', 'Ended_At', 'Is_Recording_Enabled', 'Waiting_Room_Enabled',
            'LiveKit_Room_Name', 'LiveKit_Room_SID'
        ], row))

        # Add host_name
        try:
            with connection.cursor() as cursor2:
                cursor2.execute("SELECT Full_Name FROM tbl_Users WHERE ID = %s", [meeting['Host_ID']])
                name_row = cursor2.fetchone()
                meeting['host_name'] = name_row[0] if name_row else "Host"
        except Exception as e:
            meeting['host_name'] = "Host"

        # ENHANCED: Add LiveKit information
        meeting['LiveKit_URL'] = LIVEKIT_CONFIG['url'] if LIVEKIT_ENABLED else None
        meeting['LiveKit_Enabled'] = LIVEKIT_ENABLED
        
        # Get current LiveKit room status if available
        if LIVEKIT_ENABLED and livekit_service and meeting['LiveKit_Room_Name']:
            try:
                room_info = livekit_service.get_room(meeting['LiveKit_Room_Name'])
                participants = livekit_service.list_participants(meeting['LiveKit_Room_Name'])
                
                meeting['LiveKit_Room_Info'] = room_info
                meeting['Current_Participants'] = participants
                meeting['Participant_Count'] = len(participants)
                meeting['LiveKit_Room_Active'] = room_info is not None
            except Exception as e:
                logging.warning(f"Could not get LiveKit room info: {e}")
                meeting['LiveKit_Room_Info'] = None
                meeting['Current_Participants'] = []
                meeting['Participant_Count'] = 0
                meeting['LiveKit_Room_Active'] = False

        return JsonResponse(meeting, status=SUCCESS_STATUS)

    return JsonResponse({"Error": "Meeting not found"}, status=NOT_FOUND_STATUS)


@require_http_methods(["PUT"])
@csrf_exempt
def Update_Meeting(request, id):
    """
    Update existing meetings only - no new row creation.
    Returns error if meeting ID not found in any table.
    """
    create_meetings_table()
    create_scheduled_meetings_table()
    create_calendar_meeting_table()

    try:
        data = json.loads(request.body)
        logging.debug(f"UPDATE_MEETING: Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
            logging.debug(f"UPDATE_MEETING: Unwrapped list to: {json.dumps(data, indent=2)}")
        elif isinstance(data, list):
            logging.error("UPDATE_MEETING: Expected single meeting object, got list")
            return JsonResponse({"Error": "Expected a single meeting object, not a list"}, status=400)
    except json.JSONDecodeError as e:
        logging.error(f"UPDATE_MEETING: Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=400)

    # IST timezone support
    import pytz
    from datetime import datetime, timedelta
    ist_timezone = pytz.timezone("Asia/Kolkata")

    data['Meeting_Name'] = data.get('Meeting_Name')
    data['Meeting_Type'] = data.get('Meeting_Type')
    data['Meeting_Link'] = data.get('Meeting_Link')
    data['Status'] = data.get('Status')
    data['Host_ID'] = data.get('Host_ID')
    data['Is_Recording_Enabled'] = data.get('Is_Recording_Enabled')
    data['Waiting_Room_Enabled'] = data.get('Waiting_Room_Enabled')
    data['Started_At'] = data.get('Started_At')
    data['Ended_At'] = data.get('Ended_At')

    # Enhanced timezone parsing function
    def parse_meeting_datetime(dt_str, meeting_timezone='Asia/Kolkata'):
        """
        Parse meeting datetime with proper timezone handling
        """
        try:
            if not dt_str:
                return None
                
            # Parse the ISO string
            if isinstance(dt_str, str):
                dt_str = dt_str.replace('Z', '+00:00')
                dt_parsed = datetime.fromisoformat(dt_str)
            else:
                dt_parsed = dt_str
                
            # Get meeting timezone
            meeting_tz = pytz.timezone(meeting_timezone)
            
            if dt_parsed.tzinfo is None:
                # No timezone info - localize to meeting timezone
                result = meeting_tz.localize(dt_parsed)
                logging.info(f"UPDATE_MEETING: Localized naive datetime to {meeting_timezone}: {dt_parsed} -> {result}")
            else:
                # Has timezone info - check if it needs conversion
                utc_tz = pytz.UTC
                if dt_parsed.tzinfo == utc_tz or str(dt_parsed.tzinfo) == 'UTC':
                    utc_dt = dt_parsed.replace(tzinfo=None)
                    
                    # For IST (UTC+5:30), check if this should be IST time
                    if meeting_timezone == 'Asia/Kolkata':
                        potential_ist_time = utc_dt + timedelta(hours=5, minutes=30)
                        hour = potential_ist_time.hour
                        if 6 <= hour <= 23:
                            result = meeting_tz.localize(potential_ist_time)
                            logging.info(f"UPDATE_MEETING: Corrected UTC time back to IST: {utc_dt} -> {potential_ist_time} -> {result}")
                        else:
                            result = dt_parsed.astimezone(meeting_tz)
                            logging.info(f"UPDATE_MEETING: Converted UTC to {meeting_timezone}: {dt_parsed} -> {result}")
                    else:
                        result = dt_parsed.astimezone(meeting_tz)
                        logging.info(f"UPDATE_MEETING: Converted to {meeting_timezone}: {dt_parsed} -> {result}")
                else:
                    result = dt_parsed.astimezone(meeting_tz)
                    logging.info(f"UPDATE_MEETING: Converted from {dt_parsed.tzinfo} to {meeting_timezone}: {dt_parsed} -> {result}")
                    
            return result
            
        except Exception as e:
            logging.error(f"UPDATE_MEETING: Failed to parse meeting datetime '{dt_str}': {e}")
            return dt_str

    # Parse datetime fields
    meeting_timezone = data.get('timezone', 'Asia/Kolkata')
    logging.info(f"UPDATE_MEETING: Processing meeting with timezone: {meeting_timezone}")
    
    data['Started_At'] = parse_meeting_datetime(data['Started_At'], meeting_timezone)
    data['Ended_At'] = parse_meeting_datetime(data['Ended_At'], meeting_timezone)
    data['start_time'] = parse_meeting_datetime(data.get('start_time'), meeting_timezone)
    data['end_time'] = parse_meeting_datetime(data.get('end_time'), meeting_timezone)
    data['start_date'] = parse_meeting_datetime(data.get('start_date'), meeting_timezone)
    data['end_date'] = parse_meeting_datetime(data.get('end_date'), meeting_timezone)

    # Log parsed times
    logging.info(f"UPDATE_MEETING: Parsed times - Started_At: {data['Started_At']}, start_time: {data['start_time']}, end_time: {data['end_time']}")

    # Validation
    valid_meeting_types = {'CalendarMeeting', 'ScheduleMeeting', 'InstantMeeting', None}
    if data['Meeting_Type'] not in valid_meeting_types:
        return JsonResponse({"Error": "Invalid Meeting_Type"}, status=400)

    valid_statuses = {'active', 'ended', 'scheduled', None}
    if data['Status'] not in valid_statuses:
        return JsonResponse({"Error": "Invalid Status"}, status=400)

    if data['Meeting_Name'] and len(data['Meeting_Name']) > 200:
        return JsonResponse({"Error": "Meeting_Name must be max 200 characters"}, status=400)
    if data['Meeting_Link'] and len(data['Meeting_Link']) > 500:
        return JsonResponse({"Error": "Meeting_Link must be max 500 characters"}, status=400)

    if data['Host_ID']:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM tbl_Users WHERE ID = %s AND Status = 1", [data['Host_ID']])
                if cursor.fetchone()[0] == 0:
                    return JsonResponse({"Error": "Invalid or inactive Host_ID"}, status=400)
        except Exception as e:
            return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                # STEP 1: Check if meeting exists in main table
                logging.info(f"UPDATE_MEETING: Checking if meeting {id} exists in main table")
                cursor.execute("""
                    SELECT Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status, Started_At, Ended_At,
                           Is_Recording_Enabled, Waiting_Room_Enabled, LiveKit_Room_Name, LiveKit_Room_SID
                    FROM tbl_Meetings
                    WHERE ID = %s
                """, [id])
                row = cursor.fetchone()
                
                if not row:
                    logging.error(f"UPDATE_MEETING: Meeting {id} not found in tbl_Meetings")
                    return JsonResponse({"Error": f"Meeting {id} not found"}, status=404)

                existing = dict(zip([
                    'Host_ID', 'Meeting_Name', 'Meeting_Type', 'Meeting_Link', 'Status', 'Started_At',
                    'Ended_At', 'Is_Recording_Enabled', 'Waiting_Room_Enabled', 'LiveKit_Room_Name', 'LiveKit_Room_SID'
                ], row))
                
                logging.info(f"UPDATE_MEETING: Found meeting {id} with type: {existing['Meeting_Type']}")
                
                # Use provided or existing values
                host_id = data['Host_ID'] if data['Host_ID'] is not None else existing['Host_ID']
                meeting_name = data['Meeting_Name'] if data['Meeting_Name'] is not None else existing['Meeting_Name']
                meeting_type = data['Meeting_Type'] if data['Meeting_Type'] is not None else existing['Meeting_Type']
                meeting_link = data['Meeting_Link'] if data['Meeting_Link'] is not None else existing['Meeting_Link']
                status = data['Status'] if data['Status'] is not None else existing['Status']
                started_at = data['Started_At'] if data['Started_At'] is not None else existing['Started_At']
                ended_at = data['Ended_At'] if data['Ended_At'] is not None else existing['Ended_At']
                is_recording_enabled = data['Is_Recording_Enabled'] if data['Is_Recording_Enabled'] is not None else existing['Is_Recording_Enabled']
                waiting_room_enabled = data['Waiting_Room_Enabled'] if data['Waiting_Room_Enabled'] is not None else existing['Waiting_Room_Enabled']

                # Ensure critical defaults
                if not started_at:
                    started_at = existing['Started_At'] or timezone.now()
                if not status:
                    status = existing['Status'] or 'scheduled'

                # STEP 2: Update main meetings table
                logging.info(f"UPDATE_MEETING: Updating main table for meeting {id}")
                cursor.execute("""
                    UPDATE tbl_Meetings
                    SET Host_ID = %s,
                        Meeting_Name = %s,
                        Meeting_Type = %s,
                        Meeting_Link = %s,
                        Status = %s,
                        Started_At = %s,
                        Ended_At = %s,
                        Is_Recording_Enabled = %s,
                        Waiting_Room_Enabled = %s
                    WHERE ID = %s
                """, [
                    host_id, meeting_name, meeting_type, meeting_link, status, started_at,
                    ended_at, 1 if is_recording_enabled else 0,
                    1 if waiting_room_enabled else 0, id
                ])
                
                main_updated_rows = cursor.rowcount
                logging.info(f"UPDATE_MEETING: Main table update affected {main_updated_rows} rows")

                # Handle email field for ScheduleMeeting
                email_field = data.get('email')
                if isinstance(email_field, list):
                    email_field = ",".join([str(e) for e in email_field if e])
                elif email_field is not None:
                    email_field = str(email_field)

                # STEP 3: Handle meeting type-specific updates
                if meeting_type == 'ScheduleMeeting':
                    logging.info(f"UPDATE_MEETING: Processing ScheduleMeeting update for {id}")
                    
                    # Check if exists in scheduled table
                    cursor.execute("""
                        SELECT COUNT(*) FROM tbl_ScheduledMeetings WHERE id = %s
                    """, [id])
                    scheduled_exists = cursor.fetchone()[0] > 0
                    
                    if not scheduled_exists:
                        logging.error(f"UPDATE_MEETING: ScheduleMeeting {id} not found in tbl_ScheduledMeetings")
                        return JsonResponse({"Error": f"ScheduleMeeting {id} not found in scheduled meetings table"}, status=404)
                    
                    # Get existing ScheduleMeeting data
                    cursor.execute("""
                        SELECT email, selected_days, selected_month_dates, monthly_pattern, start_date, end_date
                        FROM tbl_ScheduledMeetings
                        WHERE id = %s
                    """, [id])
                    scheduled_row = cursor.fetchone()
                    
                    existing_scheduled = {
                        'email': scheduled_row[0],
                        'selected_days': scheduled_row[1],
                        'selected_month_dates': scheduled_row[2], 
                        'monthly_pattern': scheduled_row[3],
                        'start_date': scheduled_row[4],
                        'end_date': scheduled_row[5]
                    }
                    
                    # Use provided email or existing
                    final_email = email_field if email_field is not None else existing_scheduled.get('email')

                    # Handle visibility dates with recurrence support
                    start_date = data.get('start_date') or existing_scheduled.get('start_date') or started_at or datetime.now(pytz.timezone(meeting_timezone))
                    end_date = data.get('end_date') or existing_scheduled.get('end_date')
                    
                    # Process recurrence data
                    recurring_data = data.get('recurrence', {})
                    recurrence_end_date = None
                    
                    if recurring_data.get('enabled'):
                        if recurring_data.get('endDate'):
                            try:
                                recurrence_end_date = parse_meeting_datetime(recurring_data.get('endDate'), meeting_timezone)
                            except Exception as e:
                                logging.error(f"UPDATE_MEETING: Failed to parse recurrence endDate: {e}")
                        elif data.get('recurrence_end_date'):
                            try:
                                recurrence_end_date = parse_meeting_datetime(data.get('recurrence_end_date'), meeting_timezone)
                            except Exception as e:
                                logging.error(f"UPDATE_MEETING: Failed to parse top-level recurrence_end_date: {e}")
                    
                    # Calculate end_date if not provided
                    if end_date is None:
                        if recurring_data.get('enabled') and recurrence_end_date:
                            end_date = recurrence_end_date + timedelta(days=1)
                        elif recurring_data.get('enabled'):
                            end_date = started_at + timedelta(days=365)
                        else:
                            end_date = ended_at or (started_at + timedelta(hours=2))

                    # Process recurrence settings
                    selected_days = existing_scheduled.get('selected_days')
                    selected_month_dates = existing_scheduled.get('selected_month_dates')
                    monthly_pattern = existing_scheduled.get('monthly_pattern', 'same-date')
                    
                    if recurring_data.get('enabled'):
                        recurrence_type = recurring_data.get('type', 'none')
                        if recurrence_type == 'weekly' and 'selectedDays' in recurring_data:
                            selected_days = json.dumps(recurring_data['selectedDays'])
                        elif recurrence_type == 'monthly':
                            if 'selectedMonthDates' in recurring_data:
                                selected_month_dates = json.dumps(recurring_data['selectedMonthDates'])
                            if 'monthlyPattern' in recurring_data:
                                monthly_pattern = recurring_data['monthlyPattern']
                    else:
                        selected_days = None
                        selected_month_dates = None
                        monthly_pattern = 'same-date'
                        recurrence_end_date = None

                    # Handle settings
                    settings = data.get('settings', {})
                    settings_waiting_room = settings.get('waitingRoom', waiting_room_enabled) if 'waitingRoom' in settings else data.get('settings_waiting_room', waiting_room_enabled)
                    settings_recording = settings.get('recording', is_recording_enabled) if 'recording' in settings else data.get('settings_recording', is_recording_enabled)
                    settings_allow_chat = settings.get('allowChat', True) if 'allowChat' in settings else data.get('settings_allow_chat', 1)
                    settings_allow_screen_share = settings.get('allowScreenShare', True) if 'allowScreenShare' in settings else data.get('settings_allow_screen_share', 1)
                    settings_mute_participants = settings.get('muteParticipants', False) if 'muteParticipants' in settings else data.get('settings_mute_participants', 0)
                    settings_require_password = settings.get('requirePassword', False) if 'requirePassword' in settings else data.get('settings_require_password', 0)
                    settings_password = settings.get('password') if 'password' in settings else data.get('settings_password')

                    # Handle reminders
                    reminders = data.get('reminders', {})
                    reminders_email = reminders.get('email', True) if 'email' in reminders else data.get('reminders_email', 1)
                    reminders_browser = reminders.get('browser', True) if 'browser' in reminders else data.get('reminders_browser', 1)
                    reminders_times = json.dumps(reminders.get('reminderTimes', [15, 5])) if 'reminderTimes' in reminders else data.get('reminders_times', '[15, 5]')

                    # Update ScheduleMeeting table
                    cursor.execute("""
                        UPDATE tbl_ScheduledMeetings
                        SET 
                            title = %s,
                            description = %s,
                            location = %s,
                            start_time = %s,
                            end_time = %s,
                            start_date = %s,
                            end_date = %s,
                            timezone = %s,
                            duration_minutes = %s,
                            is_recurring = %s,
                            recurrence_type = %s,
                            recurrence_interval = %s,
                            recurrence_occurrences = %s,
                            recurrence_end_date = %s,
                            selected_days = %s,
                            selected_month_dates = %s,
                            monthly_pattern = %s,
                            settings_waiting_room = %s,
                            settings_recording = %s,
                            settings_allow_chat = %s,
                            settings_allow_screen_share = %s,
                            settings_mute_participants = %s,
                            settings_require_password = %s,
                            settings_password = %s,
                            email = %s,
                            reminders_email = %s,
                            reminders_browser = %s,
                            reminders_times = %s
                        WHERE id = %s
                    """, [
                        data.get('title', meeting_name),
                        data.get('description'),
                        data.get('location'),
                        data.get('start_time', started_at),
                        data.get('end_time', ended_at),
                        start_date,
                        end_date,
                        data.get('timezone', meeting_timezone),
                        data.get('duration_minutes', 60),
                        1 if recurring_data.get('enabled') else 0,
                        recurring_data.get('type') if recurring_data.get('enabled') else None,
                        recurring_data.get('interval', 1) if recurring_data.get('enabled') else None,
                        recurring_data.get('occurrences') if recurring_data.get('enabled') else None,
                        recurrence_end_date,
                        selected_days,
                        selected_month_dates,
                        monthly_pattern,
                        1 if settings_waiting_room else 0,
                        1 if settings_recording else 0,
                        1 if settings_allow_chat else 0,
                        1 if settings_allow_screen_share else 0,
                        1 if settings_mute_participants else 0,
                        1 if settings_require_password else 0,
                        settings_password,
                        final_email,
                        1 if reminders_email else 0,
                        1 if reminders_browser else 0,
                        reminders_times,
                        id
                    ])
                    
                    scheduled_updated_rows = cursor.rowcount
                    logging.info(f"UPDATE_MEETING: ScheduleMeeting table update affected {scheduled_updated_rows} rows")
                    
                    if scheduled_updated_rows == 0:
                        logging.error(f"UPDATE_MEETING: Failed to update ScheduleMeeting {id}")
                        return JsonResponse({"Error": f"Failed to update ScheduleMeeting {id}"}, status=500)

                elif meeting_type == 'CalendarMeeting':
                    logging.info(f"UPDATE_MEETING: Processing CalendarMeeting update for {id}")
                    
                    try:
                        # STEP 1: First check if record exists in calendar table
                        cursor.execute("""
                            SELECT ID, email, guestEmails, provider, attendees, location, duration,
                                   reminderMinutes, Settings_CreateCalendarEvent, Settings_SendInvitations,
                                   Settings_SetReminders, Settings_AddMeetingLink, Settings_AddToHostCalendar,
                                   Settings_AddToParticipantCalendars
                            FROM tbl_CalendarMeetings
                            WHERE ID = %s
                        """, [id])
                        
                        calendar_row = cursor.fetchone()
                        
                        # STEP 2: If record doesn't exist, return error (DO NOT CREATE)
                        if not calendar_row:
                            logging.error(f"UPDATE_MEETING: CalendarMeeting {id} not found in tbl_CalendarMeetings")
                            return JsonResponse({
                                "Error": f"CalendarMeeting with ID {id} not found in calendar meetings table. Update operation requires existing record."
                            }, status=404)
                        
                        # STEP 3: Get existing data for fallback values
                        existing_calendar = {
                            'ID': calendar_row[0],
                            'email': calendar_row[1],
                            'guestEmails': calendar_row[2], 
                            'provider': calendar_row[3],
                            'attendees': calendar_row[4],
                            'location': calendar_row[5],
                            'duration': calendar_row[6],
                            'reminderMinutes': calendar_row[7],
                            'Settings_CreateCalendarEvent': calendar_row[8],
                            'Settings_SendInvitations': calendar_row[9],
                            'Settings_SetReminders': calendar_row[10],
                            'Settings_AddMeetingLink': calendar_row[11],
                            'Settings_AddToHostCalendar': calendar_row[12],
                            'Settings_AddToParticipantCalendars': calendar_row[13]
                        }
                        
                        logging.info(f"UPDATE_MEETING: Found existing CalendarMeeting record with ID: {existing_calendar['ID']}")
                        
                        # STEP 4: Process email field (use existing if not provided)
                        calendar_email = existing_calendar.get('email')
                        if data.get('email') and str(data.get('email')).strip():
                            calendar_email = str(data.get('email')).strip()
                        elif data.get('organizer') and str(data.get('organizer')).strip():
                            calendar_email = str(data.get('organizer')).strip()
                        elif data.get('Participants') and len(data.get('Participants')) > 0:
                            first_participant = data.get('Participants')[0]
                            if isinstance(first_participant, dict) and first_participant.get('email'):
                                calendar_email = str(first_participant.get('email')).strip()
                            elif isinstance(first_participant, str) and first_participant.strip():
                                calendar_email = str(first_participant).strip()
                        
                        # STEP 5: Process guest emails (use existing if not provided)
                        guest_emails = existing_calendar.get('guestEmails')
                        if data.get('guestEmails') is not None:
                            guest_emails_raw = data.get('guestEmails')
                            if isinstance(guest_emails_raw, list):
                                valid_emails = [str(email).strip() for email in guest_emails_raw if email and str(email).strip()]
                                guest_emails = ",".join(valid_emails) if valid_emails else guest_emails
                            elif isinstance(guest_emails_raw, str):
                                guest_emails = guest_emails_raw.strip() if guest_emails_raw.strip() else guest_emails
                        elif data.get('participants') is not None:
                            participants_raw = data.get('participants')
                            if isinstance(participants_raw, list):
                                valid_emails = [str(email).strip() for email in participants_raw if email and str(email).strip()]
                                guest_emails = ",".join(valid_emails) if valid_emails else guest_emails
                            elif isinstance(participants_raw, str):
                                guest_emails = participants_raw.strip() if participants_raw.strip() else guest_emails
                        elif data.get('Participants'):
                            participants = data.get('Participants', [])
                            if participants and isinstance(participants, list):
                                participant_emails = []
                                for participant in participants:
                                    if isinstance(participant, dict) and participant.get('email'):
                                        email = str(participant.get('email')).strip()
                                        if email:
                                            participant_emails.append(email)
                                    elif isinstance(participant, str) and participant.strip():
                                        participant_emails.append(str(participant).strip())
                                guest_emails = ",".join(participant_emails) if participant_emails else guest_emails
                        
                        # STEP 6: Process attendees (use existing if not provided)
                        attendees = existing_calendar.get('attendees')
                        if data.get('attendees') is not None:
                            attendees_raw = data.get('attendees')
                            if isinstance(attendees_raw, list):
                                valid_emails = [str(email).strip() for email in attendees_raw if email and str(email).strip()]
                                attendees = ";".join(valid_emails) if valid_emails else attendees
                            elif isinstance(attendees_raw, str):
                                attendees = attendees_raw.strip() if attendees_raw.strip() else attendees
                        elif guest_emails:
                            emails = [email.strip() for email in str(guest_emails).split(',') if email.strip()]
                            attendees = ";".join(emails) if emails else attendees
                        
                        # STEP 7: Handle other fields with fallbacks
                        provider = data.get('provider') or data.get('Provider') or existing_calendar.get('provider', 'internal')
                        location = data.get('location') or data.get('Location') or existing_calendar.get('location')
                        
                        # Handle duration - FIXED TYPE CONVERSION
                        duration = existing_calendar.get('duration', 60)  # Default fallback
                        try:
                            if data.get('duration') is not None:
                                duration = int(data.get('duration'))
                            elif data.get('duration_minutes') is not None:
                                duration = int(data.get('duration_minutes'))
                            elif data.get('Duration_Minutes') is not None:
                                duration = int(data.get('Duration_Minutes'))
                            elif started_at and ended_at:
                                duration = int((ended_at - started_at).total_seconds() / 60)
                        except (ValueError, TypeError) as e:
                            logging.warning(f"UPDATE_MEETING: Invalid duration value, using existing/default: {e}")
                            duration = existing_calendar.get('duration', 60)
                        
                        # Ensure duration is valid integer
                        if not isinstance(duration, int) or duration <= 0:
                            duration = 60
                        
                        # Handle reminder minutes with fallback - FIXED JSON HANDLING
                        reminder_minutes = existing_calendar.get('reminderMinutes', '[15, 30]')
                        try:
                            if data.get('ReminderMinutes') is not None:
                                reminder_minutes = json.dumps(data.get('ReminderMinutes'))
                            elif data.get('reminderMinutes') is not None:
                                reminder_minutes = json.dumps(data.get('reminderMinutes'))
                            elif data.get('reminders', {}).get('reminderTimes') is not None:
                                reminder_minutes = json.dumps(data.get('reminders', {}).get('reminderTimes'))
                            elif data.get('CalendarSettings', {}).get('reminderTimes') is not None:
                                reminder_minutes = json.dumps(data.get('CalendarSettings', {}).get('reminderTimes'))
                        except (TypeError, ValueError) as e:
                            logging.warning(f"UPDATE_MEETING: Invalid reminder minutes, using existing: {e}")
                            reminder_minutes = existing_calendar.get('reminderMinutes', '[15, 30]')
                        
                        # Ensure reminder_minutes is valid JSON string
                        if not isinstance(reminder_minutes, str):
                            reminder_minutes = '[15, 30]'
                        
                        # STEP 8: Handle settings with fallbacks to existing values
                        def safe_bool_convert(value, default=False):
                            """Safely convert value to boolean"""
                            if value is None:
                                return default
                            if isinstance(value, bool):
                                return value
                            if isinstance(value, int):
                                return bool(value)
                            if isinstance(value, str):
                                return value.lower() in ('true', '1', 'yes')
                            return default
                        
                        settings_create_calendar_event = existing_calendar.get('Settings_CreateCalendarEvent', 1)
                        settings_send_invitations = existing_calendar.get('Settings_SendInvitations', 1)
                        settings_set_reminders = existing_calendar.get('Settings_SetReminders', 1)
                        settings_add_meeting_link = existing_calendar.get('Settings_AddMeetingLink', 1)
                        settings_add_to_host_calendar = existing_calendar.get('Settings_AddToHostCalendar', 1)
                        settings_add_to_participant_calendars = existing_calendar.get('Settings_AddToParticipantCalendars', 1)
                        
                        # Update settings only if provided in request
                        settings = data.get('Settings', {})
                        calendar_settings = data.get('CalendarSettings', {})
                        
                        if 'createCalendarEvent' in settings:
                            settings_create_calendar_event = 1 if safe_bool_convert(settings['createCalendarEvent']) else 0
                        elif 'Settings_CreateCalendarEvent' in data:
                            settings_create_calendar_event = 1 if safe_bool_convert(data['Settings_CreateCalendarEvent']) else 0
                        
                        if 'sendInvitations' in settings:
                            settings_send_invitations = 1 if safe_bool_convert(settings['sendInvitations']) else 0
                        elif 'Settings_SendInvitations' in data:
                            settings_send_invitations = 1 if safe_bool_convert(data['Settings_SendInvitations']) else 0
                        
                        if 'setReminders' in settings:
                            settings_set_reminders = 1 if safe_bool_convert(settings['setReminders']) else 0
                        elif 'Settings_SetReminders' in data:
                            settings_set_reminders = 1 if safe_bool_convert(data['Settings_SetReminders']) else 0
                        
                        if 'addMeetingLink' in settings:
                            settings_add_meeting_link = 1 if safe_bool_convert(settings['addMeetingLink']) else 0
                        elif 'Settings_AddMeetingLink' in data:
                            settings_add_meeting_link = 1 if safe_bool_convert(data['Settings_AddMeetingLink']) else 0
                        
                        if 'addToHostCalendar' in calendar_settings:
                            settings_add_to_host_calendar = 1 if safe_bool_convert(calendar_settings['addToHostCalendar']) else 0
                        elif 'Settings_AddToHostCalendar' in data:
                            settings_add_to_host_calendar = 1 if safe_bool_convert(data['Settings_AddToHostCalendar']) else 0
                        
                        if 'addToParticipantCalendars' in calendar_settings:
                            settings_add_to_participant_calendars = 1 if safe_bool_convert(calendar_settings['addToParticipantCalendars']) else 0
                        elif 'Settings_AddToParticipantCalendars' in data:
                            settings_add_to_participant_calendars = 1 if safe_bool_convert(data['Settings_AddToParticipantCalendars']) else 0

                        # STEP 9: Perform the UPDATE (NOT INSERT)
                        logging.info(f"UPDATE_MEETING: About to update CalendarMeeting ID {id}")
                        
                        cursor.execute("""
                            UPDATE tbl_CalendarMeetings
                            SET 
                                title = %s,
                                startTime = %s,
                                endTime = %s,
                                duration = %s,
                                email = %s,
                                guestEmails = %s,
                                provider = %s,
                                meetingUrl = %s,
                                location = %s,
                                attendees = %s,
                                reminderMinutes = %s,
                                Settings_CreateCalendarEvent = %s,
                                Settings_SendInvitations = %s,
                                Settings_SetReminders = %s,
                                Settings_AddMeetingLink = %s,
                                Settings_AddToHostCalendar = %s,
                                Settings_AddToParticipantCalendars = %s
                            WHERE ID = %s
                        """, [
                            data.get('title') or data.get('Meeting_Name') or meeting_name,
                            started_at,
                            ended_at,
                            duration,  # Now properly handled as integer
                            calendar_email,
                            guest_emails,
                            provider,
                            meeting_link,
                            location,
                            attendees,
                            reminder_minutes,  # Now properly handled as JSON string
                            settings_create_calendar_event,
                            settings_send_invitations,
                            settings_set_reminders,
                            settings_add_meeting_link,
                            settings_add_to_host_calendar,
                            settings_add_to_participant_calendars,
                            id  # WHERE ID = this exact ID
                        ])
                        
                        calendar_updated_rows = cursor.rowcount
                        logging.info(f"UPDATE_MEETING: CalendarMeeting UPDATE affected {calendar_updated_rows} rows for ID {id}")
                        
                        # STEP 10: Verify the update worked
                        if calendar_updated_rows == 0:
                            logging.error(f"UPDATE_MEETING: No rows updated for CalendarMeeting {id} - this should not happen")
                            return JsonResponse({
                                "Error": f"Failed to update CalendarMeeting {id}. No rows were affected by the update operation."
                            }, status=500)
                        elif calendar_updated_rows > 1:
                            logging.error(f"UPDATE_MEETING: Multiple rows ({calendar_updated_rows}) updated for CalendarMeeting {id} - this indicates duplicate records")
                            return JsonResponse({
                                "Error": f"Multiple records updated for CalendarMeeting {id}. Database may have duplicate records."
                            }, status=500)
                        else:
                            logging.info(f"UPDATE_MEETING: Successfully updated exactly 1 CalendarMeeting record with ID {id}")
                            
                    except Exception as calendar_error:
                        logging.error(f"UPDATE_MEETING: Error in CalendarMeeting update section for {id}: {calendar_error}")
                        import traceback
                        logging.error(f"UPDATE_MEETING: CalendarMeeting error traceback: {traceback.format_exc()}")
                        return JsonResponse({
                            "Error": f"Failed to update CalendarMeeting {id}: {str(calendar_error)}"
                        }, status=500)

                elif meeting_type == 'InstantMeeting':
                    # InstantMeeting only updates main table (already done above)
                    logging.info(f"UPDATE_MEETING: InstantMeeting {id} updated (main table only)")

                # Collect updated fields for response
                updated_fields = [k for k in data.keys() if data[k] is not None]
                
                logging.info(f"UPDATE_MEETING: Successfully updated meeting {id} of type {meeting_type}")

    except Exception as e:
        logging.error(f"UPDATE_MEETING: Error updating meeting {id}: {e}")
        import traceback
        logging.error(f"UPDATE_MEETING: Full traceback: {traceback.format_exc()}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)

    # Return success response
    return JsonResponse({
        "Message": "Meeting updated successfully",
        "Meeting_ID": id,
        "Meeting_Type": meeting_type,
        "Updated_Fields": updated_fields
    }, status=200)


# GET endpoint to retrieve meeting details for editing
@require_http_methods(["GET"])
@csrf_exempt
def Get_Meeting_Details(request, id):
    """Get meeting details for editing"""
    try:
        with connection.cursor() as cursor:
            # Get main meeting data
            cursor.execute(f"""
                SELECT ID, Host_ID, Meeting_Name, Meeting_Type, Meeting_Link, Status, 
                       Started_At, Ended_At, Is_Recording_Enabled, Waiting_Room_Enabled,
                       LiveKit_Room_Name, LiveKit_Room_SID, Created_At
                FROM {TBL_MEETINGS}
                WHERE ID = %s
            """, [id])
            
            meeting_row = cursor.fetchone()
            if not meeting_row:
                return JsonResponse({"Error": "Meeting not found"}, status=NOT_FOUND_STATUS)
            
            meeting_data = dict(zip([
                'ID', 'Host_ID', 'Meeting_Name', 'Meeting_Type', 'Meeting_Link', 'Status',
                'Started_At', 'Ended_At', 'Is_Recording_Enabled', 'Waiting_Room_Enabled',
                'LiveKit_Room_Name', 'LiveKit_Room_SID', 'Created_At'
            ], meeting_row))
            
            # Convert datetime fields to ISO strings
            for field in ['Started_At', 'Ended_At', 'Created_At']:
                if meeting_data[field]:
                    meeting_data[field] = meeting_data[field].isoformat()
            
            # Get calendar-specific data if it's a CalendarMeeting
            if meeting_data['Meeting_Type'] == 'CalendarMeeting':
                cursor.execute(f"""
                    SELECT title, startTime, endTime, duration, email, guestEmails, provider,
                           meetingUrl, location, attendees, reminderMinutes,
                           Settings_CreateCalendarEvent, Settings_SendInvitations,
                           Settings_SetReminders, Settings_AddMeetingLink,
                           Settings_AddToHostCalendar, Settings_AddToParticipantCalendars
                    FROM tbl_CalendarMeetings
                    WHERE ID = %s
                """, [id])
                
                calendar_row = cursor.fetchone()
                if calendar_row:
                    calendar_data = dict(zip([
                        'title', 'startTime', 'endTime', 'duration', 'email', 'guestEmails', 'provider',
                        'meetingUrl', 'location', 'attendees', 'reminderMinutes',
                        'Settings_CreateCalendarEvent', 'Settings_SendInvitations',
                        'Settings_SetReminders', 'Settings_AddMeetingLink',
                        'Settings_AddToHostCalendar', 'Settings_AddToParticipantCalendars'
                    ], calendar_row))
                    
                    # Convert datetime fields
                    for field in ['startTime', 'endTime']:
                        if calendar_data[field]:
                            calendar_data[field] = calendar_data[field].isoformat()
                    
                    # Parse reminder minutes
                    try:
                        if calendar_data['reminderMinutes']:
                            calendar_data['reminderMinutes'] = json.loads(calendar_data['reminderMinutes'])
                    except:
                        calendar_data['reminderMinutes'] = [15, 30]
                    
                    # Merge calendar data into meeting data
                    meeting_data.update(calendar_data)
            
            # Format response for frontend
            response_data = {
                "id": meeting_data['ID'],
                "meeting_id": meeting_data['ID'],
                "Meeting_ID": meeting_data['ID'],
                "title": meeting_data.get('title') or meeting_data['Meeting_Name'],
                "Meeting_Name": meeting_data['Meeting_Name'],
                "start_time": meeting_data.get('startTime') or meeting_data['Started_At'],
                "end_time": meeting_data.get('endTime') or meeting_data['Ended_At'],
                "Started_At": meeting_data['Started_At'],
                "Ended_At": meeting_data['Ended_At'],
                "duration": meeting_data.get('duration', 60),
                "location": meeting_data.get('location', ''),
                "email": meeting_data.get('email', ''),
                "guestEmails": meeting_data.get('guestEmails', ''),
                "attendees": meeting_data.get('guestEmails', '').split(',') if meeting_data.get('guestEmails') else [],
                "Host_ID": meeting_data['Host_ID'],
                "Status": meeting_data['Status'],
                "Meeting_Type": meeting_data['Meeting_Type'],
                "Meeting_Link": meeting_data['Meeting_Link'],
                "provider": meeting_data.get('provider', 'internal'),
                
                # Settings
                "Settings": {
                    "createCalendarEvent": bool(meeting_data.get('Settings_CreateCalendarEvent', True)),
                    "sendInvitations": bool(meeting_data.get('Settings_SendInvitations', True)),
                    "setReminders": bool(meeting_data.get('Settings_SetReminders', True)),
                    "addMeetingLink": bool(meeting_data.get('Settings_AddMeetingLink', True)),
                },
                
                # Calendar Integration Settings
                "CalendarSettings": {
                    "addToHostCalendar": bool(meeting_data.get('Settings_AddToHostCalendar', True)),
                    "addToParticipantCalendars": bool(meeting_data.get('Settings_AddToParticipantCalendars', True)),
                    "reminderTimes": meeting_data.get('reminderMinutes', [15, 30])
                },
                
                "reminderMinutes": meeting_data.get('reminderMinutes', [15, 30])
            }
            
            return JsonResponse(response_data, status=200)
            
    except Exception as e:
        logging.error(f"Get meeting details error: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)


# ENHANCED: Delete Meeting with LiveKit cleanup
@require_http_methods(["DELETE"])
@csrf_exempt
def Delete_Meeting(request, id):
    create_meetings_table()

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Determine the Meeting_Type and get LiveKit info
                cursor.execute(f"SELECT Meeting_Type, LiveKit_Room_Name FROM {TBL_MEETINGS} WHERE ID = %s", [id])
                meeting_row = cursor.fetchone()
                if not meeting_row:
                    logging.error(f"Meeting ID {id} not found")
                    return JsonResponse({"Error": "Meeting not found"}, status=NOT_FOUND_STATUS)

                meeting_type, livekit_room_name = meeting_row

                # Delete from scheduled or calendar tables first if needed
                if meeting_type == 'ScheduleMeeting':
                    cursor.execute(f"DELETE FROM {TBL_SCHEDULED_MEETINGS} WHERE id = %s", [id])
                elif meeting_type == 'CalendarMeeting':
                    cursor.execute(f"DELETE FROM {TBL_CALENDAR_MEETING} WHERE ID = %s", [id])

                # Then delete from tbl_Meetings
                cursor.execute(f"DELETE FROM {TBL_MEETINGS} WHERE ID = %s", [id])
                if cursor.rowcount == 0:
                    logging.error(f"Meeting ID {id} not deleted")
                    return JsonResponse({"Error": "Failed to delete meeting"}, status=SERVER_ERROR_STATUS)

                # ENHANCED: Clean up LiveKit room
                if LIVEKIT_ENABLED and livekit_service and livekit_room_name:
                    try:
                        livekit_service.delete_room(livekit_room_name)
                        logging.info(f"✅ Deleted LiveKit room: {livekit_room_name}")
                    except Exception as e:
                        logging.warning(f"⚠️  Failed to delete LiveKit room: {e}")
                        # Don't fail the deletion if LiveKit cleanup fails

    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": f"Meeting ID {id} deleted successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Allow_From_Waiting_Room(request, id):
    create_meetings_table()

    try:
        with connection.cursor() as cursor:
            select_query = f"""
            SELECT Host_ID, Waiting_Room_Enabled
            FROM {TBL_MEETINGS}
            WHERE ID = %s
            """
            cursor.execute(select_query, [id])
            row = cursor.fetchone()
            if not row:
                logging.error(f"Meeting ID {id} not found")
                return JsonResponse({"Error": "Meeting not found"}, status=NOT_FOUND_STATUS)

            host_id, waiting_room_enabled = row[0], row[1]
            if not waiting_room_enabled:
                logging.error("Waiting room is not enabled for this meeting")
                return JsonResponse({"Error": "Waiting room is not enabled for this meeting"}, status=BAD_REQUEST_STATUS)

            # Placeholder for host permission check (since authentication isn't fully implemented)
            # In a real scenario, you'd check if the requesting user matches the Host_ID

            # Logic to allow users from waiting room would be implemented via WebSocket in meetings_consumers.py
    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": "Users allowed from waiting room"}, status=SUCCESS_STATUS)


@require_http_methods(["GET"])
@csrf_exempt
def Get_Schedule_Meetings(request):
    """FIXED: Get scheduled meetings with real-time status calculation - ALL ORIGINAL FUNCTIONALITY PRESERVED"""
    try:
        # Get current datetime for filtering
        current_datetime = datetime.now()
        
        with connection.cursor() as cursor:
            # UNCHANGED: Original query with all schedule meeting fields
            query = """
            SELECT 
                sm.id, sm.host_id, sm.title, sm.description, sm.location,
                sm.start_time, sm.end_time, sm.start_date, sm.end_date, sm.timezone, sm.duration_minutes,
                sm.is_recurring, sm.recurrence_type, sm.recurrence_interval,
                sm.recurrence_occurrences, sm.recurrence_end_date,
                sm.selected_days, sm.selected_month_dates, sm.monthly_pattern,
                sm.settings_waiting_room, sm.settings_recording, sm.settings_allow_chat,
                sm.settings_allow_screen_share, sm.settings_mute_participants,
                sm.settings_require_password, sm.settings_password,
                sm.reminders_email, sm.reminders_browser, sm.reminders_times,
                sm.created_at, sm.email,
                m.Status, m.Meeting_Link, m.Is_Recording_Enabled, m.Waiting_Room_Enabled,
                m.LiveKit_Room_Name, m.LiveKit_Room_SID
            FROM tbl_ScheduledMeetings sm
            INNER JOIN tbl_Meetings m ON sm.id = m.ID
            WHERE sm.start_date <= %s AND sm.end_date >= %s
            ORDER BY sm.start_time ASC
            """
            cursor.execute(query, [current_datetime, current_datetime])
            rows = cursor.fetchall()

            meetings = []
            for row in rows:
                # ONLY CHANGE: Calculate real-time status
                started_at = row[5]  # start_time
                ended_at = row[6]    # end_time
                duration_minutes = row[10] or 60  # duration_minutes
                stored_status = row[31]  # m.Status
                
                # Calculate actual status based on time
                calculated_status = calculate_meeting_status(started_at, ended_at, duration_minutes)
                
                # UNCHANGED: All original meeting data structure preserved
                meeting = {
                    "ID": str(row[0]),
                    "Host_ID": row[1],
                    "title": row[2],
                    "description": row[3],
                    "location": row[4],
                    "start_time": row[5].isoformat() if row[5] else None,
                    "end_time": row[6].isoformat() if row[6] else None,
                    "start_date": row[7].isoformat() if row[7] else None,
                    "end_date": row[8].isoformat() if row[8] else None,
                    "timezone": row[9],
                    "duration_minutes": row[10],
                    "is_recurring": bool(row[11]),
                    "recurrence_type": row[12],
                    "recurrence_interval": row[13],
                    "recurrence_occurrences": row[14],
                    "recurrence_end_date": row[15].isoformat() if row[15] else None,
                    "selected_days": json.loads(row[16]) if row[16] else [],
                    "selected_month_dates": json.loads(row[17]) if row[17] else [],
                    "monthly_pattern": row[18],
                    "settings_waiting_room": bool(row[19]),
                    "settings_recording": bool(row[20]),
                    "settings_allow_chat": bool(row[21]),
                    "settings_allow_screen_share": bool(row[22]),
                    "settings_mute_participants": bool(row[23]),
                    "settings_require_password": bool(row[24]),
                    "settings_password": row[25],
                    "reminders_email": bool(row[26]),
                    "reminders_browser": bool(row[27]),
                    "reminders_times": json.loads(row[28]) if row[28] else [],
                    "created_at": row[29].isoformat() if row[29] else None,
                    "email": row[30],
                    
                    # ONLY CHANGE: Use calculated status instead of stored status
                    "Status": calculated_status,
                    "status": calculated_status,  # For frontend compatibility
                    
                    # UNCHANGED: All other original fields
                    "Meeting_Link": row[32],
                    "Is_Recording_Enabled": bool(row[33]),
                    "Waiting_Room_Enabled": bool(row[34]),
                    "LiveKit_Room_Name": row[35],
                    "LiveKit_Room_SID": row[36],
                    "LiveKit_URL": LIVEKIT_CONFIG['url'] if LIVEKIT_ENABLED else None,
                    "LiveKit_Enabled": LIVEKIT_ENABLED
                }
                
                # Add real-time LiveKit status (UNCHANGED from original)
                if LIVEKIT_ENABLED and livekit_service and meeting["LiveKit_Room_Name"]:
                    try:
                        room_info = livekit_service.get_room(meeting["LiveKit_Room_Name"])
                        meeting["LiveKit_Room_Active"] = room_info is not None
                        meeting["LiveKit_Participants"] = len(livekit_service.list_participants(meeting["LiveKit_Room_Name"])) if room_info else 0
                    except:
                        meeting["LiveKit_Room_Active"] = False
                        meeting["LiveKit_Participants"] = 0
                
                meetings.append(meeting)

            logging.info(f"Retrieved {len(meetings)} visible meetings with calculated statuses")
            return JsonResponse(meetings, safe=False, status=SUCCESS_STATUS)
            
    except Exception as e:
        logging.error(f"Database error in Get_Schedule_Meetings: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)


@require_http_methods(["GET"])
@csrf_exempt
def Get_Calendar_Meetings(request):
    """FIXED: Get all calendar meetings with real-time status - ALL FUNCTIONALITY PRESERVED"""
    try:
        with connection.cursor() as cursor:
            # ✅ FIXED: Added WHERE filter to include only valid meetings
            select_query = """
            SELECT 
                cm.ID, cm.Host_ID, cm.title, cm.startTime, cm.endTime, cm.duration,
                cm.email, cm.guestEmails, cm.provider, cm.meetingUrl, cm.location,
                cm.attendees, cm.reminderMinutes, cm.Settings_CreateCalendarEvent,
                cm.Settings_SendInvitations, cm.Settings_SetReminders, cm.Settings_AddMeetingLink,
                cm.Settings_AddToHostCalendar, cm.Settings_AddToParticipantCalendars,
                cm.CreatedAt, m.Status, m.Is_Recording_Enabled, m.Waiting_Room_Enabled,
                m.LiveKit_Room_Name, m.LiveKit_Room_SID
            FROM tbl_CalendarMeetings cm
            INNER JOIN tbl_Meetings m ON cm.ID = m.ID
            WHERE (m.Status IS NULL OR m.Status NOT IN ('deleted', 'cancelled'))
            ORDER BY cm.startTime DESC
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            
            meetings = []
            for row in rows:
                # ✅ Calculate real-time status for each meeting
                started_at = row[3]  # startTime
                ended_at = row[4]    # endTime
                duration_minutes = row[5] or 60  # duration
                calculated_status = calculate_meeting_status(started_at, ended_at, duration_minutes)

                # ✅ Email parsing logic (unchanged)
                guest_emails_raw = row[7]
                attendees_raw = row[11]
                guest_emails_list = parse_enhanced_guest_emails(guest_emails_raw, "guestEmails")
                attendees_list = parse_enhanced_guest_emails(attendees_raw, "attendees")

                if len(guest_emails_list) >= len(attendees_list):
                    final_email_list = guest_emails_list
                else:
                    final_email_list = attendees_list

                if len(guest_emails_list) > 0 and len(attendees_list) > 0:
                    all_emails = set(guest_emails_list + attendees_list)
                    if len(all_emails) > len(final_email_list):
                        final_email_list = list(all_emails)

                reminder_minutes = parse_reminder_minutes(row[12])

                # ✅ Build meeting dictionary (unchanged structure)
                meeting = {
                    'ID': str(row[0]),
                    'id': str(row[0]),
                    'meeting_id': str(row[0]),
                    'Meeting_ID': str(row[0]),

                    'Host_ID': row[1],
                    'host_id': row[1],
                    'title': row[2],
                    'Meeting_Name': row[2],
                    'meetingTitle': row[2],

                    'startTime': row[3].isoformat() if row[3] else None,
                    'Started_At': row[3].isoformat() if row[3] else None,
                    'start_time': row[3].isoformat() if row[3] else None,
                    'meetingStartTime': row[3].isoformat() if row[3] else None,

                    'endTime': row[4].isoformat() if row[4] else None,
                    'Ended_At': row[4].isoformat() if row[4] else None,
                    'end_time': row[4].isoformat() if row[4] else None,
                    'meetingEndTime': row[4].isoformat() if row[4] else None,

                    'duration': row[5] or 60,
                    'Duration_Minutes': row[5] or 60,
                    'meetingDuration': row[5] or 60,

                    'email': row[6],
                    'organizer': row[6],

                    'guestEmails': final_email_list,
                    'guestEmailsRaw': guest_emails_raw,
                    'guest_emails': final_email_list,
                    'attendees': final_email_list,
                    'attendee_emails': final_email_list,
                    'participants': final_email_list,
                    'participantEmails': final_email_list,
                    'Participants': final_email_list,
                    'attendeesRaw': attendees_raw,

                    'provider': row[8] or 'internal',
                    'Provider': row[8] or 'internal',
                    'Meeting_Link': row[9],
                    'meetingUrl': row[9],
                    'meeting_url': row[9],
                    'location': row[10] or '',
                    'Location': row[10] or '',

                    'reminderMinutes': reminder_minutes,
                    'ReminderMinutes': reminder_minutes,

                    'Settings': {
                        'createCalendarEvent': bool(row[13]),
                        'sendInvitations': bool(row[14]),
                        'setReminders': bool(row[15]),
                        'addMeetingLink': bool(row[16]),
                    },

                    'Settings_CreateCalendarEvent': bool(row[13]),
                    'Settings_SendInvitations': bool(row[14]),
                    'Settings_SetReminders': bool(row[15]),
                    'Settings_AddMeetingLink': bool(row[16]),

                    'CalendarSettings': {
                        'addToHostCalendar': bool(row[17]) if row[17] is not None else True,
                        'addToParticipantCalendars': bool(row[18]) if row[18] is not None else True,
                        'reminderTimes': reminder_minutes
                    },
                    'Settings_AddToHostCalendar': bool(row[17]) if row[17] is not None else True,
                    'Settings_AddToParticipantCalendars': bool(row[18]) if row[18] is not None else True,

                    # ✅ Always use calculated status
                    'Status': calculated_status,
                    'status': calculated_status,

                    'CreatedAt': row[19].isoformat() if row[19] else None,
                    'Is_Recording_Enabled': bool(row[20]),
                    'recordingEnabled': bool(row[20]),
                    'Waiting_Room_Enabled': bool(row[21]),
                    'waitingRoomEnabled': bool(row[21]),
                    'type': 'calendar',

                    'LiveKit_Room_Name': row[22] if len(row) > 22 else None,
                    'LiveKit_Room_SID': row[23] if len(row) > 23 else None,

                    'participant_count': len(final_email_list),
                    'has_participants': len(final_email_list) > 0
                }

                meetings.append(meeting)

            logging.info(f"Returning {len(meetings)} calendar meetings with calculated statuses")
            return JsonResponse(meetings, safe=False, status=200)

    except Exception as e:
        logging.error(f"Database error in Get_Calendar_Meetings: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
async def manage_participant_quality(request):
    """Dynamic quality management for performance"""
    try:
        data = json.loads(request.body)
        room_name = data.get('room_name')
        participant_count = data.get('participant_count', 0)
        
        # Auto-adjust quality based on participant count
        if participant_count > 150:
            quality_config = {
                'video_resolution': '480p',
                'video_fps': 15,
                'audio_bitrate': 32000,
                'enable_simulcast': True
            }
        elif participant_count > 100:
            quality_config = {
                'video_resolution': '720p',
                'video_fps': 20,
                'audio_bitrate': 64000,
                'enable_simulcast': True
            }
        else:
            quality_config = {
                'video_resolution': '1080p',
                'video_fps': 30,
                'audio_bitrate': 128000,
                'enable_simulcast': False
            }
        
        # Broadcast quality update via LiveKit data channel
        await broadcast_quality_update(room_name, quality_config)
        
        return JsonResponse({
            'success': True,
            'quality_config': quality_config,
            'participant_count': participant_count
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

async def broadcast_quality_update(room_name: str, config: Dict):
    """Broadcast quality settings to all participants"""
    try:
        message = {
            'type': 'quality_update',
            'config': config,
            'timestamp': timezone.now().isoformat()
        }
        
        # Use LiveKit data publishing
        api = LiveKitAPI(LIVEKIT_CONFIG['url'], LIVEKIT_CONFIG['api_key'], LIVEKIT_CONFIG['api_secret'])
        await api.room.send_data(
            room=room_name,
            data=json.dumps(message).encode(),
            kind='reliable'
        )
        
    except Exception as e:
        logging.error(f"Failed to broadcast quality update: {e}")

async def monitor_meeting_performance(meeting_id: str):
    """Real-time performance monitoring"""
    try:
        # Get current metrics
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT LiveKit_Room_Name FROM tbl_Meetings WHERE ID = %s
            """, [meeting_id])
            
            row = cursor.fetchone()
            if not row:
                return
            
            room_name = row[0]
            
            # Get LiveKit metrics
            api = LiveKitAPI(LIVEKIT_CONFIG['url'], LIVEKIT_CONFIG['api_key'], LIVEKIT_CONFIG['api_secret'])
            participants = await api.room.list_participants(ListParticipantsRequest(room=room_name))
            
            metrics = {
                'participant_count': len(participants.participants),
                'active_publishers': sum(1 for p in participants.participants if p.tracks),
                'total_bandwidth': sum(track.bandwidth for p in participants.participants for track in p.tracks),
                'timestamp': timezone.now().isoformat()
            }
            
            # Store metrics
            redis_client.lpush(f"metrics:{meeting_id}", json.dumps(metrics))
            redis_client.ltrim(f"metrics:{meeting_id}", 0, 100)  # Keep last 100 entries
            
            # Alert if performance issues
            if metrics['participant_count'] > 180:
                await send_capacity_alert(meeting_id, metrics)
            
    except Exception as e:
        logging.error(f"Performance monitoring error: {e}")


def parse_enhanced_guest_emails(email_data, source_name="unknown"):
    """ENHANCED: Parse guest emails with comprehensive logging and error handling"""
    if not email_data:
        logging.info(f"  {source_name}: No data provided")
        return []
    
    try:
        logging.info(f"  {source_name}: Processing {repr(email_data)} (type: {type(email_data)})")
        
        # Case 1: Already a list
        if isinstance(email_data, list):
            emails = []
            for item in email_data:
                if isinstance(item, str) and item.strip() and '@' in item:
                    emails.append(item.strip())
                elif isinstance(item, dict) and item.get('email') and '@' in item['email']:
                    emails.append(item['email'].strip())
            logging.info(f"  {source_name}: Parsed {len(emails)} emails from list: {emails}")
            return emails
        
        # Case 2: String data
        if isinstance(email_data, str):
            email_data = email_data.strip()
            if not email_data:
                logging.info(f"  {source_name}: Empty string after strip")
                return []
                
            # Try JSON parsing first
            try:
                parsed = json.loads(email_data)
                if isinstance(parsed, list):
                    emails = []
                    for item in parsed:
                        if isinstance(item, str) and item.strip() and '@' in item:
                            emails.append(item.strip())
                    logging.info(f"  {source_name}: Parsed {len(emails)} emails from JSON: {emails}")
                    return emails
            except (json.JSONDecodeError, ValueError):
                logging.info(f"  {source_name}: Not valid JSON, trying delimiter parsing")
            
            # Split by common delimiters
            emails = []
            for delimiter in [',', ';', '\n', '|']:
                if delimiter in email_data:
                    potential_emails = email_data.split(delimiter)
                    emails = [email.strip() for email in potential_emails 
                             if email.strip() and '@' in email.strip()]
                    if emails:
                        logging.info(f"  {source_name}: Split by '{delimiter}' found {len(emails)} emails: {emails}")
                        return emails
            
            # Single email case
            if '@' in email_data:
                emails = [email_data.strip()]
                logging.info(f"  {source_name}: Single email found: {emails}")
                return emails
        
        logging.info(f"  {source_name}: No valid email format found")
        return []
        
    except Exception as e:
        logging.error(f"  {source_name}: Error parsing emails: {e}")
        return []


def parse_reminder_minutes(reminder_data):
    """ENHANCED: Parse reminder minutes with better error handling"""
    if not reminder_data:
        return [15, 30]  # Default
    
    try:
        logging.info(f"Parsing reminder data: {repr(reminder_data)} (type: {type(reminder_data)})")
        
        if isinstance(reminder_data, str):
            try:
                parsed = json.loads(reminder_data)
                if isinstance(parsed, list) and all(isinstance(x, int) for x in parsed):
                    logging.info(f"Parsed reminder minutes from JSON: {parsed}")
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
        elif isinstance(reminder_data, list):
            if all(isinstance(x, int) for x in reminder_data):
                logging.info(f"Using reminder minutes list: {reminder_data}")
                return reminder_data
        elif isinstance(reminder_data, int):
            logging.info(f"Converting single reminder minute: {reminder_data}")
            return [reminder_data]
        
        logging.info(f"Using default reminder minutes: [15, 30]")
        return [15, 30]  # Default fallback
        
    except Exception as e:
        logging.error(f"Error parsing reminder minutes: {e}")
        return [15, 30]

def format_meeting_for_frontend(row, user_id=None, user_email=None):
    """Format database row for frontend consumption with proper email handling"""
    try:
        if not row or len(row) < 23:
            logging.error(f"Invalid row data: {row}")
            return None
            
        # Parse guest emails properly
        guest_emails_raw = row[7]  # guestEmails field
        guest_emails_list = []
        
        if guest_emails_raw:
            if isinstance(guest_emails_raw, str):
                # Split by comma and clean
                guest_emails_list = [email.strip() for email in guest_emails_raw.split(',') if email.strip()]
            elif isinstance(guest_emails_raw, list):
                guest_emails_list = guest_emails_raw
        
        # Parse attendees (semicolon separated)
        attendees_raw = row[11]  # attendees field  
        attendee_emails_list = []
        
        if attendees_raw:
            if isinstance(attendees_raw, str):
                # Split by semicolon and clean
                attendee_emails_list = [email.strip() for email in attendees_raw.split(';') if email.strip()]
            elif isinstance(attendees_raw, list):
                attendee_emails_list = attendees_raw
        
        # Parse reminder minutes
        reminder_minutes = []
        if row[12]:  # reminderMinutes field
            try:
                if isinstance(row[12], str):
                    reminder_minutes = json.loads(row[12])
                elif isinstance(row[12], list):
                    reminder_minutes = row[12]
                else:
                    reminder_minutes = [15, 30]
            except:
                reminder_minutes = [15, 30]
        else:
            reminder_minutes = [15, 30]
        
        # Determine user role
        is_host = str(row[1]) == str(user_id) if user_id else False
        is_participant = False
        
        if user_email:
            is_participant = (
                user_email in guest_emails_list or 
                user_email in attendee_emails_list or
                user_email == row[6]  # email field
            )
        
        # Return formatted meeting data
        meeting = {
            # Basic IDs
            'id': str(row[0]),
            'meeting_id': str(row[0]),
            'ID': str(row[0]),
            'Meeting_ID': str(row[0]),
            
            # Title fields
            'title': row[2],
            'meetingTitle': row[2],
            'Meeting_Name': row[2],
            
            # Time fields
            'startTime': row[3].isoformat() if row[3] else None,
            'start_time': row[3].isoformat() if row[3] else None,
            'Started_At': row[3].isoformat() if row[3] else None,
            'endTime': row[4].isoformat() if row[4] else None,
            'end_time': row[4].isoformat() if row[4] else None,
            'Ended_At': row[4].isoformat() if row[4] else None,
            
            # Duration
            'duration': row[5],
            'meetingDuration': row[5],
            'Duration_Minutes': row[5],
            
            # Email fields - THIS IS THE KEY FIX
            'email': row[6],
            'organizer': row[6],
            
            # CRITICAL: Properly formatted email lists
            'guestEmails': guest_emails_list,  # Array format for frontend
            'guestEmailsRaw': row[7],         # Raw format for debugging
            'guest_emails': guest_emails_list, # Alternative field name
            'attendees': guest_emails_list,    # Frontend expects this
            'attendee_emails': guest_emails_list,
            'participants': guest_emails_list, # Frontend uses this too
            'attendeesRaw': row[11],          # Raw attendees for debugging
            
            # Location
            'location': row[10] or '',
            'Location': row[10] or '',
            
            # URLs
            'Meeting_Link': row[9],
            'meetingUrl': row[9],
            'meeting_url': row[9],
            
            # Provider
            'provider': row[8] or 'internal',
            'Provider': row[8] or 'internal',
            
            # Reminder settings
            'reminderMinutes': reminder_minutes,
            'ReminderMinutes': reminder_minutes,
            
            # Meeting Settings
            'Settings': {
                'createCalendarEvent': bool(row[13]),
                'sendInvitations': bool(row[14]),
                'setReminders': bool(row[15]),
                'addMeetingLink': bool(row[16]),
            },
            'Settings_CreateCalendarEvent': bool(row[13]),
            'Settings_SendInvitations': bool(row[14]),
            'Settings_SetReminders': bool(row[15]),
            'Settings_AddMeetingLink': bool(row[16]),
            
            # Calendar Integration Settings
            'CalendarSettings': {
                'addToHostCalendar': bool(row[17]),
                'addToParticipantCalendars': bool(row[18]),
                'reminderTimes': reminder_minutes
            },
            'Settings_AddToHostCalendar': bool(row[17]),
            'Settings_AddToParticipantCalendars': bool(row[18]),
            
            # Other fields
            'CreatedAt': row[19].isoformat() if row[19] else None,
            'Status': row[20],
            'Is_Recording_Enabled': bool(row[21]),
            'Waiting_Room_Enabled': bool(row[22]),
            'type': 'calendar',
            
            # User role info
            'is_host': is_host,
            'is_participant': is_participant,
            'user_role': 'host' if is_host else 'participant',
            'Host_ID': row[1],
            'host_id': row[1],
            
            # LiveKit fields
            'LiveKit_Room_Name': row[23] if len(row) > 23 else None,
            'LiveKit_Room_SID': row[24] if len(row) > 24 else None
        }
        
        return meeting
        
    except Exception as e:
        logging.error(f"Error in format_meeting_for_frontend: {e}")
        return None

@require_http_methods(["GET"])
@csrf_exempt
def Get_User_Schedule_Meetings(request):
    """FIXED: Get user scheduled meetings with real-time status - ALL FUNCTIONALITY PRESERVED"""
    try:
        user_email = request.GET.get('user_email', '')
        user_id = request.GET.get('user_id', '')
        
        if not user_id and not user_email:
            return JsonResponse({"Error": "User ID or email required"}, status=400)
        
        # UNCHANGED: Original imports and setup
        from core.utils.date_utils import get_current_ist_datetime
        from core.utils.recurring_calculator import calculate_next_occurrence
        
        current_datetime = get_current_ist_datetime()
        current_date = current_datetime.date()
        
        with connection.cursor() as cursor:
            # UNCHANGED: Original comprehensive query
            query = """
            SELECT 
                sm.id, sm.host_id, sm.title, sm.description, sm.location,
                sm.start_time, sm.end_time, sm.start_date, sm.end_date, sm.timezone, sm.duration_minutes,
                sm.is_recurring, sm.recurrence_type, sm.recurrence_interval,
                sm.recurrence_occurrences, sm.recurrence_end_date,
                sm.selected_days, sm.selected_month_dates, sm.monthly_pattern,
                sm.settings_waiting_room, sm.settings_recording, sm.settings_allow_chat,
                sm.settings_allow_screen_share, sm.settings_mute_participants,
                sm.settings_require_password, sm.settings_password,
                sm.reminders_email, sm.reminders_browser, sm.reminders_times,
                sm.created_at, sm.email,
                m.Status, m.Meeting_Link, m.Is_Recording_Enabled, m.Waiting_Room_Enabled,
                m.Meeting_Name, m.Meeting_Type, m.LiveKit_Room_Name, m.LiveKit_Room_SID,
                u.full_name as host_full_name, u.email as host_email
            FROM tbl_ScheduledMeetings sm
            INNER JOIN tbl_Meetings m ON sm.id = m.ID
            LEFT JOIN tbl_Users u ON sm.host_id = u.ID
            WHERE (sm.host_id = %s OR (sm.email IS NOT NULL AND sm.email LIKE %s))
              AND m.Status NOT IN ('deleted', 'cancelled', 'recurrence_ended')
              AND (
                  (sm.is_recurring = 0 AND sm.end_time >= %s)
                  OR
                  (sm.is_recurring = 1 AND DATE(sm.start_date) <= %s AND DATE(sm.end_date) >= %s)
                  OR
                  (sm.start_time >= %s OR sm.end_time >= %s)
              )
            ORDER BY sm.start_time ASC
            """
            
            email_pattern = f'%{user_email}%' if user_email else ''
            one_week_ago = current_datetime - timedelta(days=7)
            
            cursor.execute(query, [
                user_id, email_pattern, current_datetime, current_date, current_date, one_week_ago, current_datetime
            ])
            rows = cursor.fetchall()

            logging.info(f"Query returned {len(rows)} meetings for user {user_id}")

            meetings = []
            for row in rows:
                try:
                    # UNCHANGED: Original participant processing
                    participant_emails = []
                    if row[30] and isinstance(row[30], str):
                        participant_emails = [email.strip() for email in row[30].split(',') if email.strip()]
                    
                    is_host = str(row[1]) == str(user_id)
                    is_participant = user_email in participant_emails if user_email else False
                    
                    if is_host or is_participant:
                        # UNCHANGED: Original recurring meeting logic
                        display_start_time = row[5].isoformat() if row[5] else None
                        display_end_time = row[6].isoformat() if row[6] else None
                        is_today_meeting = False
                        duration_minutes = row[10] or 60
                        
                        if row[11]:  # is_recurring - UNCHANGED logic
                            try:
                                meeting_data = {
                                    'start_time': display_start_time,
                                    'end_time': display_end_time,
                                    'is_recurring': True,
                                    'recurrence_type': row[12],
                                    'recurrence_interval': row[13] or 1,
                                    'recurrence_end_date': row[15].isoformat() if row[15] else None,
                                    'selected_days': row[16],
                                    'selected_month_dates': row[17],
                                    'monthly_pattern': row[18] or 'same-date'
                                }
                                
                                next_occurrence = calculate_next_occurrence(meeting_data, current_datetime)
                                if next_occurrence:
                                    display_start_time = next_occurrence['next_start_time']
                                    display_end_time = next_occurrence['next_end_time']
                                    is_today_meeting = next_occurrence.get('is_today', False)
                            except Exception as e:
                                logging.error(f"Error calculating next occurrence: {e}")

                        # ONLY CHANGE: Calculate status based on display times
                        start_dt = None
                        end_dt = None
                        if display_start_time:
                            start_dt = datetime.fromisoformat(display_start_time.replace('Z', '+00:00')) if isinstance(display_start_time, str) else display_start_time
                        if display_end_time:
                            end_dt = datetime.fromisoformat(display_end_time.replace('Z', '+00:00')) if isinstance(display_end_time, str) else display_end_time
                            
                        calculated_status = calculate_meeting_status(start_dt, end_dt, duration_minutes)

                        # UNCHANGED: All original meeting data structure
                        meeting = {
                            "ID": str(row[0]),
                            "Meeting_ID": str(row[0]),
                            "Host_ID": row[1],
                            "title": row[2] or "Untitled Meeting",
                            "Meeting_Name": row[35] or row[2] or "Untitled Meeting",
                            "description": row[3] or "",
                            "location": row[4] or "",
                            "start_time": display_start_time,
                            "Started_At": display_start_time,
                            "end_time": display_end_time,
                            "Ended_At": display_end_time,
                            "original_start_time": row[5].isoformat() if row[5] else None,
                            "original_end_time": row[6].isoformat() if row[6] else None,
                            "start_date": row[7].isoformat() if row[7] else None,
                            "end_date": row[8].isoformat() if row[8] else None,
                            "timezone": row[9] or "Asia/Kolkata",
                            "duration_minutes": duration_minutes,
                            "is_recurring": bool(row[11]),
                            "recurrence_type": row[12],
                            "selected_days": json.loads(row[16]) if row[16] else [],
                            "selected_month_dates": json.loads(row[17]) if row[17] else [],
                            "is_today_meeting": is_today_meeting,
                            "settings_waiting_room": bool(row[19]),
                            "settings_recording": bool(row[20]),
                            "email": row[30] or "",
                            "participants": participant_emails,
                            
                            # ONLY CHANGE: Use calculated status
                            "Status": calculated_status,
                            "status": calculated_status,
                            
                            # UNCHANGED: All remaining original fields
                            "Meeting_Link": row[32] or "",
                            "Meeting_Type": row[36] or "ScheduleMeeting",
                            "is_host": is_host,
                            "host_name": row[39] or "",
                            "host_email": row[40] or "",
                        }
                        
                        meetings.append(meeting)
                        
                except Exception as row_error:
                    logging.error(f"Error processing meeting row: {row_error}")
                    continue

            # UNCHANGED: Original sorting and response structure
            meetings.sort(key=lambda m: m.get('start_time') or '9999-12-31')
            
            response_data = {
                "meetings": meetings,
                "summary": {
                    "total_meetings": len(meetings),
                    "recurring_meetings": len([m for m in meetings if m.get('is_recurring')]),
                    "todays_meetings": len([m for m in meetings if m.get('is_today_meeting')]),
                }
            }
            
            logging.info(f"Retrieved {len(meetings)} meetings with calculated statuses")
            return JsonResponse(response_data, safe=False, status=200)
            
    except Exception as e:
        logging.error(f"Error in Get_User_Schedule_Meetings: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=500)


@require_http_methods(["GET"])
@csrf_exempt
def Get_User_Calendar_Meetings(request):
    """FIXED: Get user's calendar meetings with real-time status - ALL FUNCTIONALITY PRESERVED"""
    try:
        user_email = request.GET.get('user_email', '')
        user_id = request.GET.get('user_id', '')
        start_date = request.GET.get('start_date', '')
        end_date = request.GET.get('end_date', '')
        
        if not user_id and not user_email:
            return JsonResponse({"Error": "User ID or email required"}, status=400)
        
        with connection.cursor() as cursor:
            # UNCHANGED: Original comprehensive query
            base_query = """
            SELECT 
                cm.ID, cm.Host_ID, cm.title, cm.startTime, cm.endTime, cm.duration,
                cm.email, cm.guestEmails, cm.provider, cm.meetingUrl, cm.location,
                cm.attendees, cm.reminderMinutes, cm.Settings_CreateCalendarEvent,
                cm.Settings_SendInvitations, cm.Settings_SetReminders, cm.Settings_AddMeetingLink,
                cm.Settings_AddToHostCalendar, cm.Settings_AddToParticipantCalendars,
                cm.CreatedAt, m.Status, m.Is_Recording_Enabled, m.Waiting_Room_Enabled,
                m.LiveKit_Room_Name, m.LiveKit_Room_SID, m.Meeting_Name as meeting_name
            FROM tbl_CalendarMeetings cm
            INNER JOIN tbl_Meetings m ON cm.ID = m.ID
            WHERE (
                cm.Host_ID = %s 
                OR (cm.guestEmails IS NOT NULL AND cm.guestEmails LIKE %s)
                OR (cm.attendees IS NOT NULL AND cm.attendees LIKE %s)
                OR (cm.email = %s)
            )
            AND (m.Status IS NULL OR m.Status NOT IN ('deleted', 'cancelled'))
            """
            
            # UNCHANGED: Original parameter building
            params = []
            if user_id:
                params.append(user_id)
            else:
                params.append(None)
                
            email_pattern = f'%{user_email}%' if user_email else '%'
            params.extend([email_pattern, email_pattern, user_email or ''])
            
            if start_date and end_date:
                base_query += " AND cm.startTime BETWEEN %s AND %s"
                params.extend([start_date, end_date])
            elif start_date:
                base_query += " AND cm.startTime >= %s"
                params.append(start_date)
            elif end_date:
                base_query += " AND cm.startTime <= %s"
                params.append(end_date)
            
            base_query += " ORDER BY cm.startTime DESC"
            
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            
            meetings = []
            for row in rows:
                try:
                    # ONLY CHANGE: Calculate real-time status
                    started_at = row[3]  # startTime
                    ended_at = row[4]    # endTime
                    duration_minutes = row[5] or 60  # duration
                    stored_status = row[19]  # m.Status
                    
                    calculated_status = calculate_meeting_status(started_at, ended_at, duration_minutes)
                    if not stored_status or stored_status in ('', None):
                        stored_status = calculated_status

                    # UNCHANGED: All original email parsing logic
                    guest_emails_raw = row[7]
                    attendees_raw = row[11]
                    
                    guest_emails_list = parse_enhanced_guest_emails(guest_emails_raw, "guestEmails")
                    attendees_list = parse_enhanced_guest_emails(attendees_raw, "attendees")
                    
                    if len(guest_emails_list) >= len(attendees_list):
                        final_email_list = guest_emails_list
                        primary_source = "guestEmails"
                    else:
                        final_email_list = attendees_list  
                        primary_source = "attendees"
                    
                    if len(guest_emails_list) > 0 and len(attendees_list) > 0:
                        all_emails = set(guest_emails_list + attendees_list)
                        if len(all_emails) > len(final_email_list):
                            final_email_list = list(all_emails)
                            primary_source = "merged"
                    
                    reminder_minutes = parse_reminder_minutes(row[12])
                    
                    is_host = str(row[1]) == str(user_id) if user_id else False
                    is_participant = False
                    
                    if user_email:
                        is_participant = (
                            user_email.lower() in [email.lower() for email in final_email_list] or 
                            user_email.lower() == (row[6] or '').lower()
                        )
                    
                    # UNCHANGED: All original meeting data structure
                    meeting = {
                        'ID': str(row[0]),
                        'id': str(row[0]),
                        'meeting_id': str(row[0]),
                        'Meeting_ID': str(row[0]),
                        
                        'Host_ID': row[1],
                        'host_id': row[1],
                        'title': row[2] or row[25] or 'Untitled Meeting',
                        'Meeting_Name': row[2] or row[25] or 'Untitled Meeting',
                        'meetingTitle': row[2] or row[25] or 'Untitled Meeting',
                        
                        'startTime': row[3].isoformat() if row[3] else None,
                        'Started_At': row[3].isoformat() if row[3] else None,
                        'start_time': row[3].isoformat() if row[3] else None,
                        'meetingStartTime': row[3].isoformat() if row[3] else None,
                        
                        'endTime': row[4].isoformat() if row[4] else None,
                        'Ended_At': row[4].isoformat() if row[4] else None,
                        'end_time': row[4].isoformat() if row[4] else None,
                        'meetingEndTime': row[4].isoformat() if row[4] else None,
                        
                        'duration': row[5] or 60,
                        'Duration_Minutes': row[5] or 60,
                        'meetingDuration': row[5] or 60,
                        
                        'email': row[6],
                        'organizer': row[6],
                        
                        # UNCHANGED: All email fields
                        'guestEmails': final_email_list,
                        'guestEmailsRaw': guest_emails_raw or '',
                        'guest_emails': final_email_list,
                        'attendees': final_email_list,
                        'attendee_emails': final_email_list,
                        'participants': final_email_list,
                        'participantEmails': final_email_list,
                        'Participants': final_email_list,
                        'attendeesRaw': attendees_raw or '',
                        
                        'provider': row[8] or 'internal',
                        'Provider': row[8] or 'internal',
                        'Meeting_Link': row[9],
                        'meetingUrl': row[9],
                        'meeting_url': row[9],
                        'location': row[10] or '',
                        'Location': row[10] or '',
                        
                        'reminderMinutes': reminder_minutes,
                        'ReminderMinutes': reminder_minutes,
                        
                        'Settings': {
                            'createCalendarEvent': bool(row[13]),
                            'sendInvitations': bool(row[14]),
                            'setReminders': bool(row[15]),
                            'addMeetingLink': bool(row[16]),
                        },
                        
                        'Settings_CreateCalendarEvent': bool(row[13]),
                        'Settings_SendInvitations': bool(row[14]),
                        'Settings_SetReminders': bool(row[15]),
                        'Settings_AddMeetingLink': bool(row[16]),
                        
                        'CalendarSettings': {
                            'addToHostCalendar': bool(row[17]) if row[17] is not None else True,
                            'addToParticipantCalendars': bool(row[18]) if row[18] is not None else True,
                            'reminderTimes': reminder_minutes
                        },
                        'Settings_AddToHostCalendar': bool(row[17]) if row[17] is not None else True,
                        'Settings_AddToParticipantCalendars': bool(row[18]) if row[18] is not None else True,
                        
                        # ONLY CHANGE: Use calculated status
                        'Status': calculated_status,
                        'status': calculated_status,
                        
                        # UNCHANGED: All remaining fields
                        'CreatedAt': row[19].isoformat() if row[19] else None,
                        'Is_Recording_Enabled': bool(row[20]),
                        'recordingEnabled': bool(row[20]),
                        'Waiting_Room_Enabled': bool(row[21]),
                        'waitingRoomEnabled': bool(row[21]),
                        'type': 'calendar',
                        
                        'LiveKit_Room_Name': row[22] if len(row) > 22 else None,
                        'LiveKit_Room_SID': row[23] if len(row) > 23 else None,
                        
                        'is_host': is_host,
                        'is_participant': is_participant,
                        'user_role': 'host' if is_host else 'participant',
                        
                        'participant_count': len(final_email_list),
                        'has_participants': len(final_email_list) > 0
                    }
                    
                    meetings.append(meeting)
                    
                except Exception as row_error:
                    logging.error(f"Error processing meeting row: {row_error}")
                    continue
            
            return JsonResponse(meetings, safe=False, status=200)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logging.error(f"Error in Get_User_Calendar_Meetings: {str(e)}")
        return JsonResponse({
            "Error": f"Server error: {str(e)}"
        }, status=500)

# NEW LIVEKIT ENDPOINTS
# meetings.py - FIXED LiveKit join_livekit_meeting function

@require_http_methods(["POST"])
@csrf_exempt
def join_livekit_meeting(request):
    """FIXED: Fast join for 50+ participants with proper parameter validation"""
    try:
        logging.info("🚀 LiveKit join request received")
        
        # Check if LiveKit is available
        if not LIVEKIT_ENABLED:
            logging.warning("LiveKit service not available")
            return JsonResponse({
                'error': 'LiveKit service not available',
                'fallback_mode': True,
                'message': 'Video conferencing temporarily unavailable'
            }, status=503)
        
        if not livekit_service:
            logging.warning("LiveKit service not initialized")
            return JsonResponse({
                'error': 'LiveKit service not initialized',
                'fallback_mode': True,
                'message': 'Video conferencing temporarily unavailable'
            }, status=503)
        
        # FIXED: Better request parsing with detailed error handling
        try:
            request_body = request.body.decode('utf-8')
            logging.info(f"📥 Raw request body: {request_body}")
            data = json.loads(request_body)
            logging.info(f"📥 Parsed JSON data: {json.dumps(data, indent=2)}")
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in request body: {e}")
            return JsonResponse({
                'error': 'Invalid JSON format',
                'details': str(e),
                'received_content_type': request.content_type,
                'received_body_length': len(request.body) if request.body else 0
            }, status=400)
        
        # FIXED: Enhanced parameter extraction with multiple fallbacks
        meeting_id = (
            data.get('meeting_id') or 
            data.get('meetingId') or 
            data.get('Meeting_ID') or
            data.get('realMeetingId') or
            data.get('id')
        )
        
        user_id_raw = (
            data.get('user_id') or
            data.get('userId') or 
            data.get('User_ID') or
            data.get('participantId')
        )
        
        user_id = str(user_id_raw) if user_id_raw is not None else None
        
        user_name = (
            data.get('user_name') or
            data.get('displayName') or 
            data.get('Full_Name') or 
            data.get('name') or
            data.get('participantName') or
            f'User_{user_id}' if user_id else 'Anonymous'
        )
        
        is_host = bool(
            data.get('is_host', False) or
            data.get('isHost', False) or
            data.get('IsHost', False)
        )
        
        logging.info(f"📋 Extracted parameters: meeting_id={meeting_id}, user_id={user_id}, user_name={user_name}, is_host={is_host}")
        
        # FIXED: Better validation with specific error messages
        if not meeting_id:
            available_keys = list(data.keys())
            return JsonResponse({
                'error': 'meeting_id is required',
                'details': 'meeting_id parameter is missing from request',
                'available_parameters': available_keys,
                'expected_parameters': ['meeting_id', 'meetingId', 'Meeting_ID', 'realMeetingId', 'id']
            }, status=400)
        
        if not user_id:
            available_keys = list(data.keys())
            return JsonResponse({
                'error': 'user_id is required',
                'details': 'user_id parameter is missing from request',
                'available_parameters': available_keys,
                'expected_parameters': ['user_id', 'userId', 'User_ID', 'participantId']
            }, status=400)
        
        # FIXED: Validate meeting exists with better error handling
        host_id = None
        meeting_name = None
        status = None
        livekit_room_name = None
        
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT Host_ID, Meeting_Name, Status, LiveKit_Room_Name 
                    FROM tbl_Meetings 
                    WHERE ID = %s
                """, [meeting_id])
                
                meeting_row = cursor.fetchone()
                if meeting_row:
                    host_id, meeting_name, status, livekit_room_name = meeting_row
                    logging.info(f"📋 Found meeting: {meeting_name} (Status: {status})")
                    
                    if status and status.lower() == 'ended':
                        return JsonResponse({
                            'error': 'Meeting has ended',
                            'meeting_id': meeting_id,
                            'meeting_name': meeting_name,
                            'status': status
                        }, status=400)
                else:
                    logging.error(f"Meeting not found: {meeting_id}")
                    return JsonResponse({
                        'error': 'Meeting not found',
                        'meeting_id': meeting_id,
                        'details': 'No meeting exists with the provided ID'
                    }, status=404)
                        
        except Exception as db_error:
            logging.error(f"Database error: {db_error}")
            return JsonResponse({
                'error': 'Database connection failed',
                'details': str(db_error)
            }, status=500)
        
        # Determine participant role
        is_host_user = is_host or (str(user_id) == str(host_id))
        participant_role = 'host' if is_host_user else 'participant'
        
        # Use existing room name or create default
        room_name = livekit_room_name or f"meeting_{meeting_id}"
        
        # Generate unique participant identity
        import random
        timestamp = int(time.time())
        random_suffix = random.randint(1000, 9999)
        participant_identity = f"user_{user_id}_{timestamp}_{random_suffix}"
        
        logging.info(f"🎭 Generated participant identity: {participant_identity}")
        
        # Prepare participant metadata
        participant_metadata = {
            'user_id': user_id,
            'meeting_id': meeting_id,
            'role': participant_role,
            'full_name': user_name,
            'joined_at': timezone.now().isoformat(),
            'is_host': is_host_user,
            'participant_identity': participant_identity,
            'fast_join': True
        }
        
        # Permissions for 50+ participants
        permissions = {
            'canPublish': True,
            'canSubscribe': True,
            'canPublishData': True,
            'canUpdateOwnMetadata': True,
            'roomJoin': True,
            'hidden': False,
            'recorder': False,
            'roomAdmin': is_host_user,
            'roomCreate': is_host_user,
            'roomList': False,
            'roomRecord': is_host_user
        }
        
        # FIXED: Generate access token with proper error handling
        access_token = None
        try:
            logging.info(f"🔐 Generating access token for room: {room_name}, participant: {participant_identity}")
            access_token = livekit_service.generate_access_token(
                room_name=room_name,
                participant_name=participant_identity,
                metadata=participant_metadata,
                permissions=permissions
            )
            logging.info(f"✅ Access token generated successfully")
        except Exception as token_error:
            logging.error(f"Token generation failed: {token_error}")
            return JsonResponse({
                'error': 'Token generation failed',
                'details': str(token_error),
                'meeting_id': meeting_id,
                'room_name': room_name,
                'participant_identity': participant_identity
            }, status=500)
        
        # FIXED: Record participant join asynchronously (non-blocking)
        try:
            ist_timezone = pytz.timezone("Asia/Kolkata")
            join_time = timezone.now().astimezone(ist_timezone)
            
            with connection.cursor() as cursor:
                # Check if participant already exists
                cursor.execute("""
                    SELECT ID FROM tbl_Participants 
                    WHERE Meeting_ID = %s AND User_ID = %s AND Leave_Time IS NULL
                """, [meeting_id, user_id])
                
                existing = cursor.fetchone()
                if not existing:
                    # Insert new participant record
                    cursor.execute("""
                        INSERT INTO tbl_Participants 
                        (Meeting_ID, User_ID, Full_Name, Join_Time, Role, Status)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, [meeting_id, user_id, user_name, join_time, participant_role, 'active'])
                    
                    logging.info(f"✅ Participant join recorded for user {user_id}")
                else:
                    # Update existing record
                    cursor.execute("""
                        UPDATE tbl_Participants 
                        SET Join_Time = %s, Status = %s, Full_Name = %s
                        WHERE Meeting_ID = %s AND User_ID = %s AND Leave_Time IS NULL
                    """, [join_time, 'active', user_name, meeting_id, user_id])
                    
                    logging.info(f"✅ Updated existing participant record for user {user_id}")
                        
        except Exception as participant_error:
            logging.warning(f"Failed to record participant join (non-critical): {participant_error}")
        
        # SUCCESS: Fast response for immediate connection
        response_data = {
            'success': True,
            'access_token': access_token,
            'room_name': room_name,
            'participant_identity': participant_identity,
            'livekit_url': LIVEKIT_CONFIG['url'],
            'meeting_info': {
                'meeting_id': meeting_id,
                'meeting_name': meeting_name or 'Meeting',
                'host_id': host_id,
                'is_host': is_host_user,
                'role': participant_role,
                'status': status,
                'max_participants': 100
            },
            'config': {
                'video_enabled': True,
                'audio_enabled': True,
                'screen_share_enabled': True,
                'chat_enabled': True,
                'max_participants': 100,
                'quality_level': 'medium',
                'adaptive_stream': True,
                'dynacast': True,
                'fast_join_mode': True
            },
            'debug_info': {
                'backend_version': '1.0',
                'livekit_enabled': LIVEKIT_ENABLED,
                'participant_extraction': {
                    'meeting_id_from': [k for k in data.keys() if 'meeting' in k.lower() or k.lower() == 'id'],
                    'user_id_from': [k for k in data.keys() if 'user' in k.lower() or 'participant' in k.lower()],
                    'extracted_meeting_id': meeting_id,
                    'extracted_user_id': user_id,
                    'extracted_user_name': user_name,
                    'extracted_is_host': is_host
                },
                'room_info': {
                    'room_name': room_name,
                    'livekit_room_name': livekit_room_name,
                    'meeting_status': status
                }
            },
            'join_timestamp': timezone.now().isoformat()
        }
        
        logging.info(f"✅ FAST JOIN SUCCESS: User {user_id} ({user_name}) joined meeting {meeting_id} as {participant_role}")
        
        return JsonResponse(response_data)
        
    except Exception as e:
        logging.error(f"❌ Critical error in join_livekit_meeting: {e}")
        import traceback
        logging.error(f"❌ Full traceback: {traceback.format_exc()}")
        
        return JsonResponse({
            'error': f'Internal server error: {str(e)}',
            'details': 'Check server logs for more information',
            'traceback': traceback.format_exc() if logging.getLogger().isEnabledFor(logging.DEBUG) else None
        }, status=500)
               
@require_http_methods(["POST"])
@csrf_exempt
def bulk_send_invitations(request):
    """Handle bulk invitation sending from frontend"""
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        emails = data.get('emails', [])
        meeting_title = data.get('meeting_title', 'Meeting Invitation')
        
        if not emails:
            return JsonResponse({"error": "No emails provided"}, status=400)
        
        # Validate emails
        valid_emails = []
        for email in emails:
            if email and '@' in email:
                valid_emails.append(email.strip())
        
        if not valid_emails:
            return JsonResponse({"error": "No valid emails provided"}, status=400)
        
        # Get meeting details if meeting_id is provided
        meeting_data = None
        if meeting_id and meeting_id != 'new-meeting' and meeting_id != 'new-calendar-meeting':
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT * FROM tbl_Meetings 
                        WHERE ID = %s OR ID = %s
                    """, [meeting_id, str(meeting_id)])
                    
                    row = cursor.fetchone()
                    if row:
                        columns = [desc[0] for desc in cursor.description]
                        meeting_data = dict(zip(columns, row))
            except Exception as e:
                logging.warning(f"Could not fetch meeting data: {e}")
        
        # Prepare email data
        email_data = {
            'meeting_title': meeting_title,
            'guest_emails': valid_emails,
            'meeting_type': 'BulkInvite',
            'meeting_url': f"https://imeetpro.lancieretech.com/meeting/{meeting_id}" if meeting_id else None
        }
        
        # If we have meeting data, add more details
        if meeting_data:
            email_data.update({
                'start_time': meeting_data.get('Started_At'),
                'duration': 60,  # Default duration
                'meeting_url': meeting_data.get('Meeting_Link')
            })
        
        # Send invitations using your existing function
        try:
            send_meeting_invitations(email_data)
            success_message = f"Bulk invitations sent successfully to {len(valid_emails)} participants"
            logging.info(success_message)
        except Exception as email_error:
            logging.error(f"Failed to send bulk invitations: {email_error}")
            # Continue anyway, as we still want to return the emails for local processing
        
        return JsonResponse({
            "success": True,
            "message": f"Bulk invitations processed for {len(valid_emails)} participants",
            "emails_processed": valid_emails,
            "meeting_id": meeting_id
        })
        
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in bulk invite request: {e}")
        return JsonResponse({"error": "Invalid JSON format"}, status=400)
    except Exception as e:
        logging.error(f"Error in bulk_send_invitations: {e}")
        return JsonResponse({"error": f"Server error: {str(e)}"}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def leave_livekit_meeting(request):
    """UPDATED: Leave a LiveKit meeting with attendance tracking integration"""
    if not LIVEKIT_ENABLED:
        return JsonResponse({'error': 'LiveKit service not available'}, status=503)
    
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        participant_identity = data.get('participant_identity')
        leave_reason = data.get('reason', 'user_initiated')
        
        if not all([meeting_id, user_id]):
            return JsonResponse({'error': 'meeting_id and user_id are required'}, status=400)
        
        room_name = f"meeting_{meeting_id}"
        
        # Get actual room name from database
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT LiveKit_Room_Name FROM tbl_Meetings WHERE ID = %s", [meeting_id])
                row = cursor.fetchone()
                if row and row[0]:
                    room_name = row[0]
        except Exception as e:
            logging.warning(f"Could not get room name from database: {e}")
        
        # FIXED: Remove from LiveKit room with timeout protection
        try:
            if participant_identity:
                import signal
                def timeout_handler(signum, frame):
                    raise TimeoutError("LiveKit remove timeout")
                
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(2)  # FIXED: 2 second timeout
                
                try:
                    livekit_service.remove_participant(room_name, participant_identity)
                    logging.info(f"✅ Removed participant {participant_identity} from LiveKit room")
                except AttributeError:
                    # remove_participant method might not exist, that's okay
                    logging.info(f"ℹ️ LiveKit will handle participant removal automatically")
                except Exception as remove_error:
                    logging.warning(f"Could not remove participant from LiveKit: {remove_error}")
                finally:
                    signal.alarm(0)
        except (TimeoutError, Exception) as e:
            logging.warning(f"LiveKit removal timeout/error: {e}")
        
        # UPDATED: Record participant leave with immediate processing
        try:
            participant_data = {
                'meeting_id': meeting_id,
                'user_id': user_id,
                'event_type': 'leave',
                'action': 'leave',
                'reason': leave_reason,
                'manual_leave': True,
                'source': 'livekit_leave_endpoint',
                'immediate': True
            }
            
            from types import SimpleNamespace
            mock_request = SimpleNamespace()
            mock_request.body = json.dumps(participant_data).encode()
            mock_request.method = 'POST'
            
            participant_response = record_participant_leave(mock_request)
            
            leave_recorded = False
            if hasattr(participant_response, 'content'):
                participant_result = json.loads(participant_response.content.decode())
                if participant_result.get('success'):
                    leave_recorded = True
                    logging.info(f"✅ Immediate participant leave recorded for user {user_id}")
                else:
                    logging.warning(f"⚠️ Failed to record participant leave: {participant_result}")
                    
        except Exception as participant_error:
            logging.warning(f"Failed to record participant leave: {participant_error}")
            leave_recorded = False
        
        # ADDED: Verify user is really gone from LiveKit with timeout protection
        verification_result = {'user_still_in_livekit': False, 'verification_completed': False}
        
        if LIVEKIT_ENABLED and livekit_service:
            try:
                import signal
                def timeout_handler(signum, frame):
                    raise TimeoutError("Verification timeout")
                
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(2)  # FIXED: 2 second timeout for verification
                
                try:
                    current_participants = livekit_service.list_participants(room_name)
                    user_still_in_livekit = False
                    
                    for p in current_participants:
                        identity = p.get('identity', '')
                        metadata = p.get('metadata', {})
                        
                        # Check if this participant matches our user
                        if str(user_id) in identity:
                            user_still_in_livekit = True
                            break
                        
                        # Also check metadata
                        if isinstance(metadata, dict) and str(metadata.get('user_id')) == str(user_id):
                            user_still_in_livekit = True
                            break
                        elif isinstance(metadata, str):
                            try:
                                meta_dict = json.loads(metadata)
                                if str(meta_dict.get('user_id')) == str(user_id):
                                    user_still_in_livekit = True
                                    break
                            except:
                                pass
                    
                    verification_result = {
                        'user_still_in_livekit': user_still_in_livekit,
                        'verification_completed': True,
                        'remaining_participants': len(current_participants)
                    }
                    
                    if not user_still_in_livekit:
                        logging.info(f"✅ Verified: User {user_id} is no longer in LiveKit room {room_name}")
                    else:
                        logging.warning(f"⚠️ User {user_id} still appears in LiveKit after leave attempt")
                        
                finally:
                    signal.alarm(0)
                    
            except (TimeoutError, Exception) as e:
                logging.warning(f"Could not verify LiveKit leave status: {e}")
                verification_result = {
                    'user_still_in_livekit': False,
                    'verification_completed': False,
                    'error': str(e)
                }
        
        # ========== ATTENDANCE INTEGRATION ==========
        # Stop attendance tracking before leaving
        try:
            from core.AI_Attendance.Attendance import stop_attendance_tracking
            attendance_stopped = stop_attendance_tracking(meeting_id, user_id)
            if attendance_stopped:
                logging.info(f"✅ ATTENDANCE: Stopped tracking for user {user_id} in meeting {meeting_id}")
                attendance_tracking_stopped = True
            else:
                logging.warning(f"⚠️ ATTENDANCE: No active tracking found for user {user_id}")
                attendance_tracking_stopped = False
        except Exception as attendance_error:
            logging.error(f"❌ ATTENDANCE: Error stopping tracking: {attendance_error}")
            attendance_tracking_stopped = False
        # ============================================

        logging.info(f"✅ User {user_id} leave process completed for meeting {meeting_id}")
        
        return JsonResponse({
            'success': True,
            'message': 'Successfully left meeting',
            'meeting_id': meeting_id,
            'user_id': user_id,
            'room_name': room_name,
            'leave_recorded': leave_recorded,
            'verification': verification_result,
            'leave_reason': leave_reason,
            'attendance_tracking_stopped': attendance_tracking_stopped,
            'timestamp': timezone.now().isoformat(),
            'performance_optimizations': {
                'timeouts_reduced': '2 seconds',
                'immediate_processing': True,
                'verification_included': True,
                'attendance_integration': True
            }
        })
        
    except Exception as e:
        logging.error(f"❌ Error leaving meeting: {e}")
        return JsonResponse({'error': f'Failed to leave meeting: {str(e)}'}, status=500)
          
@require_http_methods(["GET"])
@csrf_exempt
def get_meeting_participants(request, meeting_id):
    """Get current participants in a LiveKit meeting"""
    if not LIVEKIT_ENABLED:
        return JsonResponse({'error': 'LiveKit service not available'}, status=503)
    
    try:
        room_name = f"meeting_{meeting_id}"
        participants = livekit_service.list_participants(room_name)
        
        return JsonResponse({
            'success': True,
            'meeting_id': meeting_id,
            'participants': participants,
            'total_count': len(participants)
        })
        
    except Exception as e:
        logging.error(f"❌ Error getting participants: {e}")
        return JsonResponse({'error': f'Failed to get participants: {str(e)}'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_livekit_connection_info(request, meeting_id):
    """Get LiveKit connection information for a meeting"""
    if not LIVEKIT_ENABLED:
        return JsonResponse({'error': 'LiveKit not available'}, status=503)
    
    try:
        # Verify meeting exists
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT Host_ID, Meeting_Name, Status, LiveKit_Room_Name 
                FROM tbl_Meetings 
                WHERE ID = %s
            """, [meeting_id])
            
            row = cursor.fetchone()
            if not row:
                return JsonResponse({'error': 'Meeting not found'}, status=404)
            
            host_id, meeting_name, status, livekit_room_name = row
            
            if status.lower() == 'ended':
                return JsonResponse({'error': 'Meeting has ended'}, status=400)
        
        room_name = livekit_room_name or f"meeting_{meeting_id}"
        
        # Get room info and participants
        room_info = livekit_service.get_room(room_name)
        participants = livekit_service.list_participants(room_name)
        
        connection_info = {
            'meeting_id': meeting_id,
            'meeting_name': meeting_name,
            'room_name': room_name,
            'livekit_url': LIVEKIT_CONFIG['url'],
            'status': status,
            'host_id': host_id,
            'room_exists': room_info is not None,
            'current_participants': participants,
            'participant_count': len(participants),
            'max_participants': 200
        }
        
        return JsonResponse(connection_info)
        
    except Exception as e:
        logging.error(f"Error getting LiveKit connection info: {e}")
        return JsonResponse({'error': str(e)}, status=500)

# meetings.py - ENHANCED LiveKit Backend with WebSocket Replacement Features

# Add these additional endpoints to replace WebSocket functionality
@require_http_methods(["POST"])
@csrf_exempt
def update_participant_status(request):
    """Update participant audio/video status via REST instead of WebSocket"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        status_type = data.get('status_type')  # 'audio' or 'video'
        is_enabled = data.get('is_enabled')
        
        if not all([meeting_id, user_id, status_type]) or is_enabled is None:
            return JsonResponse({'error': 'meeting_id, user_id, status_type, and is_enabled required'}, status=400)
        
        # Update participant status in database
        try:
            with connection.cursor() as cursor:
                if status_type == 'audio':
                    cursor.execute("""
                        UPDATE tbl_Participants 
                        SET Audio_Enabled = %s
                        WHERE Meeting_ID = %s AND User_ID = %s AND Leave_Time IS NULL
                    """, [is_enabled, meeting_id, user_id])
                elif status_type == 'video':
                    cursor.execute("""
                        UPDATE tbl_Participants 
                        SET Video_Enabled = %s
                        WHERE Meeting_ID = %s AND User_ID = %s AND Leave_Time IS NULL
                    """, [is_enabled, meeting_id, user_id])
        except Exception as e:
            logging.warning(f"Could not update participant status: {e}")
        
        return JsonResponse({
            'success': True,
            'message': f'{status_type} status updated',
            'broadcast_via_livekit': True,
            'data': {
                'type': f'{status_type}_toggle',
                'is_enabled': is_enabled,
                'user_id': user_id,
                'timestamp': timezone.now().isoformat()
            }
        })
        
    except Exception as e:
        logging.error(f"Error updating participant status: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def record_meeting_event(request):
    """Record meeting events (screen share, recording, etc.) via REST"""
    try:
        data = json.loads(request.body)
        
        meeting_id = data.get('meeting_id')
        user_id = data.get('user_id')
        event_type = data.get('event_type')
        event_data = data.get('event_data', {})
        
        if not all([meeting_id, user_id, event_type]):
            return JsonResponse({'error': 'meeting_id, user_id, and event_type required'}, status=400)
        
        # Store event in database
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO tbl_MeetingEvents 
                    (Meeting_ID, User_ID, Event_Type, Event_Data, Created_At)
                    VALUES (%s, %s, %s, %s, %s)
                """, [meeting_id, user_id, event_type, json.dumps(event_data), timezone.now()])
        except Exception as e:
            logging.warning(f"Could not store meeting event: {e}")
        
        return JsonResponse({
            'success': True,
            'message': 'Event recorded',
            'broadcast_via_livekit': True,
            'data': {
                'type': event_type,
                'user_id': user_id,
                'event_data': event_data,
                'timestamp': timezone.now().isoformat()
            }
        })
        
    except Exception as e:
        logging.error(f"Error recording meeting event: {e}")
        return JsonResponse({'error': str(e)}, status=500)

# Enhanced LiveKit service with better error handling
class EnhancedProductionLiveKitService(ProductionLiveKitService):
    """Enhanced LiveKit service with additional features"""
    
    def get_meeting_stats(self, meeting_id: str) -> Dict:
        """Get comprehensive meeting statistics"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT LiveKit_Room_Name FROM tbl_Meetings WHERE ID = %s
                """, [meeting_id])
                
                row = cursor.fetchone()
                if not row:
                    return {'error': 'Meeting not found'}
                
                room_name = row[0]
                if not room_name:
                    return {'error': 'No LiveKit room associated with meeting'}
                
                # Get room info
                room_info = self.get_room(room_name)
                participants = self.list_participants(room_name)
                
                # Get database stats
                cursor.execute("""
                    SELECT COUNT(*) as total_participants,
                           AVG(Engagement_Score) as avg_engagement,
                           AVG(Attendance_Percentage) as avg_attendance
                    FROM tbl_Participants 
                    WHERE Meeting_ID = %s
                """, [meeting_id])
                
                db_stats = cursor.fetchone()
                
                return {
                    'meeting_id': meeting_id,
                    'room_name': room_name,
                    'room_active': room_info is not None,
                    'current_participants': len(participants),
                    'total_participants_joined': db_stats[0] if db_stats else 0,
                    'average_engagement': float(db_stats[1]) if db_stats and db_stats[1] else 0.0,
                    'average_attendance': float(db_stats[2]) if db_stats and db_stats[2] else 0.0,
                    'participants_details': participants,
                    'room_info': room_info
                }
                
        except Exception as e:
            logging.error(f"Error getting meeting stats: {e}")
            return {'error': str(e)}
    
    def cleanup_empty_rooms(self):
        """Clean up empty LiveKit rooms"""
        try:
            # This would be called periodically to clean up unused rooms
            logging.info("🧹 Cleaning up empty LiveKit rooms...")
            
            # Implementation depends on your cleanup policy
            # You might want to delete rooms that have been empty for X minutes
            
            return True
        except Exception as e:
            logging.error(f"Error cleaning up rooms: {e}")
            return False

@require_http_methods(["GET"])
@csrf_exempt
def health_check(request):
    return JsonResponse({"status": "ok", "message": "Calendar service healthy"}, status=200)
     
# UPDATED URL PATTERNS with new endpoints
urlpatterns = [
    # Existing meeting URLs (unchanged)
    path('api/meetings/calendar-meeting', Create_Calendar_Meeting, name='Create_Calendar_Meeting'),
    path('api/meetings/calendar-meetings', Get_Calendar_Meetings, name='Get_Calendar_Meetings'),
    path('api/meetings/schedule-meeting', Create_Schedule_Meeting, name='Create_Schedule_Meeting'),
    path('api/meetings/instant-meeting', Create_Instant_Meeting, name='Create_Instant_Meeting'),
    path('api/meetings/list', List_All_Meetings, name='List_All_Meetings'),
    path('api/meetings/get/<str:id>', Get_Meeting, name='Get_Meeting'),
    path('api/meetings/update/<str:id>', Update_Meeting, name='Update_Meeting'),
    path('api/meetings/delete/<str:id>', Delete_Meeting, name='Delete_Meeting'),
    path('api/meetings/schedule-meetings', Get_Schedule_Meetings, name='Get_Schedule_Meetings'),
    path('api/meetings/user-schedule-meetings', Get_User_Schedule_Meetings, name='Get_User_Schedule_Meetings'),
    path('api/meetings/user-calendar-meetings', Get_User_Calendar_Meetings, name='Get_User_Calendar_Meetings'),
    path('api/meetings/<str:id>/allow-from-waiting-room', Allow_From_Waiting_Room, name='Allow_From_Waiting_Room'),
    
    # LiveKit core endpoints
    path('api/livekit/leave-meeting/', leave_livekit_meeting, name='leave_livekit_meeting'),
    path('api/livekit/participants/<str:meeting_id>/', get_meeting_participants, name='get_meeting_participants'),
    path('api/livekit/connection-info/<str:meeting_id>/', get_livekit_connection_info, name='get_livekit_connection_info'),
    
    # NEW: WebSocket replacement endpoints
    path('api/livekit/update-status/', update_participant_status, name='update_participant_status'),
    path('api/livekit/record-event/', record_meeting_event, name='record_meeting_event'),

    # SCALABILITY FIX: New endpoints for connection management
    path('api/meetings/check-queue/<str:meeting_id>/', check_connection_queue, name='check_connection_queue'),
    path('api/meetings/join-with-queue/', join_meeting_with_queue, name='join_meeting_with_queue'),
    path('api/invitations/bulk-send', bulk_send_invitations, name='bulk_send_invitations'),
    path("api/health", health_check, name="health_check"),
]