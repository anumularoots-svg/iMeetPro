# cache_only_chat.py - Fixed Enhanced Ephemeral Chat System with Private File Upload
from core.WebSocketConnection import enhanced_logging_config
import redis
import json
import time
import logging
import os
import hashlib
import base64
import mimetypes
from datetime import datetime, timedelta
from django.utils import timezone
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.conf import settings

# Configure logging
logger = logging.getLogger('cache_chat')

# Redis configuration for cache-only chat
CACHE_CHAT_CONFIG = {
    'host': os.getenv("CACHE_CHAT_HOST", "localhost"),
    'port': int(os.getenv("CACHE_CHAT_PORT", 6379)),
    'db': int(os.getenv("CACHE_CHAT_DB", 3)),
    'decode_responses': os.getenv("CACHE_CHAT_DECODE_RESPONSES", "True") == "True",
    'socket_timeout': int(os.getenv("CACHE_CHAT_SOCKET_TIMEOUT", 5)),
    'socket_connect_timeout': int(os.getenv("CACHE_CHAT_CONNECT_TIMEOUT", 5)),
    'retry_on_timeout': os.getenv("CACHE_CHAT_RETRY_ON_TIMEOUT", "True") == "True"
}

# Initialize cache-only chat Redis client
try:
    cache_chat_redis = redis.Redis(**CACHE_CHAT_CONFIG)
    cache_chat_redis.ping()
    logger.info("✅ Enhanced cache-only chat Redis connected successfully")
except Exception as e:
    logger.warning(f"⚠ Enhanced cache-only chat Redis not available: {e}")
    cache_chat_redis = None

# Enhanced cache settings
CACHE_SETTINGS = {
    'MAX_MESSAGES_PER_ROOM': 500,
    'TYPING_INDICATOR_TTL': 10,
    'MESSAGE_BATCH_SIZE': 100,
    'CLEANUP_IMMEDIATE': True,
    'MAX_MESSAGE_LENGTH': 1000,
    'MAX_FILE_SIZE': 50 * 1024 * 1024,
    'ALLOWED_FILE_TYPES': {
        'application/pdf': '.pdf',
        'application/msword': '.doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
        'application/vnd.ms-excel': '.xls',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
        'application/vnd.ms-powerpoint': '.ppt',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
        'text/plain': '.txt',
        'text/csv': '.csv',
        'application/rtf': '.rtf',
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'image/svg+xml': '.svg',
        'image/webp': '.webp',
        'image/bmp': '.bmp',
        'image/tiff': '.tiff',
        'application/zip': '.zip',
        'application/x-rar-compressed': '.rar',
        'application/x-7z-compressed': '.7z',
        'application/x-tar': '.tar',
        'application/gzip': '.gz',
        'audio/mpeg': '.mp3',
        'audio/wav': '.wav',
        'audio/ogg': '.ogg',
        'audio/aac': '.aac',
        'audio/flac': '.flac',
        'video/mp4': '.mp4',
        'video/avi': '.avi',
        'video/quicktime': '.mov',
        'video/x-msvideo': '.avi',
        'video/webm': '.webm',
        'application/json': '.json',
        'application/xml': '.xml',
        'text/html': '.html',
        'text/css': '.css',
        'text/javascript': '.js',
        'application/javascript': '.js',
    },
    'FILE_CACHE_TTL': 3600 * 24 * 7,
    'BROADCAST_DELAY': 0.1,
    'SYNC_INTERVAL': 2,
}

class EnhancedCacheOnlyChatManager:
    """Enhanced chat manager with fixed private message and file filtering"""
    
    def __init__(self):
        self.redis_client = cache_chat_redis
        self.enabled = cache_chat_redis is not None
        logger.info(f"🗨 Enhanced cache-only chat manager initialized: {'Enabled' if self.enabled else 'Disabled'}")
    
    def _get_chat_key(self, meeting_id):
        return f"cache_chat:{meeting_id}"
    
    def _get_files_key(self, meeting_id):
        return f"cache_files:{meeting_id}"
    
    def _get_file_data_key(self, file_id):
        return f"cache_file_data:{file_id}"
    
    def _get_typing_key(self, meeting_id):
        return f"cache_typing:{meeting_id}"
    
    def _get_participants_key(self, meeting_id):
        return f"cache_participants:{meeting_id}"
    
    def _get_meeting_status_key(self, meeting_id):
        return f"cache_meeting_status:{meeting_id}"
    
    def _validate_file(self, file_data, filename, content_type):
        if len(file_data) > CACHE_SETTINGS['MAX_FILE_SIZE']:
            return False, f"File too large (max {CACHE_SETTINGS['MAX_FILE_SIZE'] / 1024 / 1024:.1f}MB)"
        
        if content_type not in CACHE_SETTINGS['ALLOWED_FILE_TYPES']:
            return False, f"File type not allowed: {content_type}"
        
        if not filename or len(filename) > 255:
            return False, "Invalid filename"
        
        dangerous_extensions = ['.exe', '.bat', '.cmd', '.com', '.pif', '.scr', '.vbs', '.js']
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext in dangerous_extensions:
            return False, f"File type not allowed for security reasons: {file_ext}"
        
        return True, "File is valid"
    
    def _generate_file_id(self, meeting_id, filename, user_id):
        timestamp = str(int(time.time() * 1000))
        content = f"{meeting_id}{filename}{user_id}_{timestamp}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _format_file_size(self, size_bytes):
        if size_bytes == 0:
            return "0 Bytes"
        
        size_names = ["Bytes", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"
    
    def _sanitize_filename(self, filename):
        import re
        sanitized = re.sub(r'[^\w\s\-_\.]', '', filename)
        sanitized = re.sub(r'\.{2,}', '.', sanitized)
        return sanitized[:255]
    
    def start_meeting_chat(self, meeting_id):
        if not self.enabled:
            logger.warning("Redis not available for chat")
            return False
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            meeting_data = {
                'meeting_id': meeting_id,
                'started_at': timezone.now().isoformat(),
                'status': 'active',
                'message_count': 0,
                'file_count': 0,
                'participant_count': 0,
                'last_activity': timezone.now().isoformat()
            }
            
            self.redis_client.set(status_key, json.dumps(meeting_data))
            
            logger.info(f"🎬 Started enhanced cache-only chat for meeting: {meeting_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start meeting chat: {e}")
            return False
    
    def add_message(self, meeting_id, message_data):
        if not self.enabled:
            return False
        
        try:
            chat_key = self._get_chat_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            meeting_status = self.redis_client.get(status_key)
            if not meeting_status:
                logger.warning(f"Meeting {meeting_id} not active, cannot add message")
                return False
            
            message_id = message_data.get('id', f"{int(time.time() * 1000)}_{hash(str(message_data))}")
            
            recipients = message_data.get('recipients', [])
            is_private = message_data.get('is_private', False)
            
            message = {
                'id': message_id,
                'user_id': str(message_data.get('user_id', '')),
                'user_name': message_data.get('user_name', 'Anonymous'),
                'message': message_data.get('message', '').strip()[:CACHE_SETTINGS['MAX_MESSAGE_LENGTH']],
                'timestamp': message_data.get('timestamp', timezone.now().isoformat()),
                'message_type': message_data.get('message_type', 'text'),
                'is_private': is_private,
                'recipients': recipients if is_private else [],
                'sender_is_host': message_data.get('sender_is_host', False),
                'file_id': message_data.get('file_id'),
                'file_metadata': message_data.get('file_metadata'),
                'file_data': message_data.get('file_data')
            }
            
            pipe = self.redis_client.pipeline()
            pipe.lpush(chat_key, json.dumps(message))
            pipe.ltrim(chat_key, 0, CACHE_SETTINGS['MAX_MESSAGES_PER_ROOM'] - 1)
            pipe.execute()
            
            status_data = json.loads(meeting_status)
            status_data['message_count'] = status_data.get('message_count', 0) + 1
            status_data['last_message_at'] = timezone.now().isoformat()
            status_data['last_activity'] = timezone.now().isoformat()
            self.redis_client.set(status_key, json.dumps(status_data))
            
            logger.info(f"📝 Message added instantly (private: {is_private}, recipients: {len(recipients)})")
            return message_id
            
        except Exception as e:
            logger.error(f"❌ Failed to add message: {e}")
            return False

    def upload_file(self, meeting_id, file_data, filename, content_type, user_id, user_name, is_private=False, recipients=None):
        """Upload file with support for private recipients - FIXED"""
        if not self.enabled:
            return False, "Redis not available"
        
        if recipients is None:
            recipients = []
        
        # Ensure recipients is always a list of strings
        recipients = [str(r) for r in recipients if r] if recipients else []
        
        try:
            if not isinstance(file_data, bytes):
                logger.error(f"File data must be bytes, got {type(file_data)}")
                return False, "Invalid file data type"
            
            filename = self._sanitize_filename(filename)
            
            is_valid, validation_msg = self._validate_file(file_data, filename, content_type)
            if not is_valid:
                return False, validation_msg
            
            status_key = self._get_meeting_status_key(meeting_id)
            meeting_status = self.redis_client.get(status_key)
            if not meeting_status:
                return False, "Meeting not active"
            
            file_id = self._generate_file_id(meeting_id, filename, user_id)
            
            file_data_key = self._get_file_data_key(file_id)
            try:
                encoded_data = base64.b64encode(file_data).decode('ascii')
                logger.info(f"📦 Storing file data: {len(encoded_data)} chars, original: {len(file_data)} bytes")
            except Exception as encode_error:
                logger.error(f"Failed to encode file data: {encode_error}")
                return False, "Failed to encode file data"
            
            self.redis_client.set(file_data_key, encoded_data, ex=CACHE_SETTINGS['FILE_CACHE_TTL'])
            
            test_retrieve = self.redis_client.get(file_data_key)
            if not test_retrieve or len(test_retrieve) != len(encoded_data):
                logger.error(f"Failed to store file data in Redis for {file_id}")
                return False, "Failed to store file data"
            
            logger.info(f"✅ File data stored successfully in Redis: {file_id}")
            
            files_key = self._get_files_key(meeting_id)
            file_metadata = {
                'file_id': file_id,
                'filename': filename,
                'content_type': content_type,
                'size': len(file_data),
                'uploaded_by': str(user_id),
                'uploaded_by_name': user_name,
                'uploaded_at': timezone.now().isoformat(),
                'meeting_id': meeting_id,
                'encoding': 'base64',
                'is_private': is_private,
                'recipients': recipients
            }
            
            self.redis_client.hset(files_key, file_id, json.dumps(file_metadata))
            
            status_data = json.loads(meeting_status)
            status_data['file_count'] = status_data.get('file_count', 0) + 1
            status_data['last_activity'] = timezone.now().isoformat()
            self.redis_client.set(status_key, json.dumps(status_data))
            
            human_size = self._format_file_size(len(file_data))
            is_image = content_type.startswith('image/')
            
            file_message_text = (
                f"📷 Shared an image: {filename} ({human_size})" if is_image 
                else f"📎 Shared a file: {filename} ({human_size})"
            )
            
            # FIX: Ensure is_private and recipients are correctly passed to message
            file_message_data = {
                'user_id': str(user_id),
                'user_name': user_name,
                'message': file_message_text,
                'message_type': 'file',
                'file_id': file_id,
                'file_metadata': file_metadata,
                'file_data': json.dumps({
                    'name': filename,
                    'size': len(file_data),
                    'type': content_type,
                    'file_id': file_id,
                    'upload_id': file_id,
                    'originalUrl': f'/api/cache-chat/files/{file_id}/',
                    'url': f'/api/cache-chat/files/{file_id}/',
                    'isFromPreviousSession': False
                }),
                'is_private': is_private,
                'recipients': recipients if is_private else [],
                'sender_is_host': False
            }
            
            logger.info(f"📝 Creating file message with privacy settings:")
            logger.info(f"   - Is Private: {is_private}")
            logger.info(f"   - Recipients: {recipients}")
            
            message_id = self.add_message(meeting_id, file_message_data)
            
            if message_id:
                logger.info(f"📎 File uploaded successfully: {filename} ({len(file_data)} bytes)")
                logger.info(f"   - Private: {is_private}")
                logger.info(f"   - Recipients: {recipients}")
                
                return True, {
                    'file_id': file_id,
                    'message_id': message_id,
                    'download_url': f'/api/cache-chat/files/{file_id}/',
                    'filename': filename,
                    'size': len(file_data),
                    'content_type': content_type,
                    'is_private': is_private,
                    'recipients': recipients
                }
            else:
                self.redis_client.delete(file_data_key)
                self.redis_client.hdel(files_key, file_id)
                return False, "Failed to create file message"
            
        except Exception as e:
            logger.error(f"❌ Failed to upload file: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False, f"Upload failed: {str(e)}"


    def get_file(self, file_id):
        if not self.enabled:
            return None, None
        
        try:
            file_data_key = self._get_file_data_key(file_id)
            encoded_data = self.redis_client.get(file_data_key)
            
            if not encoded_data:
                logger.warning(f"File data not found for ID: {file_id}")
                return None, None
            
            try:
                file_data = base64.b64decode(encoded_data)
                logger.info(f"📥 Successfully decoded file data: {len(file_data)} bytes for {file_id}")
            except Exception as decode_error:
                logger.error(f"Failed to decode file data for {file_id}: {decode_error}")
                return None, None
            
            metadata = None
            keys = self.redis_client.keys("cache_files:*")
            for key in keys:
                try:
                    files_data = self.redis_client.hgetall(key)
                    if file_id in files_data:
                        metadata_str = files_data[file_id]
                        metadata = json.loads(metadata_str)
                        
                        if isinstance(metadata, dict):
                            logger.info(f"📄 Found metadata for file {file_id} in {key}")
                            break
                        else:
                            logger.warning(f"Invalid metadata format for file {file_id}")
                            
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"Error processing metadata for file {file_id}: {e}")
                    continue
            
            if not metadata:
                logger.warning(f"No metadata found for file {file_id}, creating basic metadata")
                metadata = {
                    'file_id': file_id,
                    'filename': f'file_{file_id}',
                    'content_type': 'application/octet-stream',
                    'size': len(file_data),
                    'uploaded_at': timezone.now().isoformat()
                }
            
            return file_data, metadata
            
        except Exception as e:
            logger.error(f"❌ Failed to get file {file_id}: {e}")
            return None, None

    def get_messages(self, meeting_id, limit=100, offset=0, user_id=None, is_host=False):
        """Get messages with corrected private message filtering"""
        if not self.enabled:
            return []
        
        try:
            chat_key = self._get_chat_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            if not self.redis_client.exists(status_key):
                logger.warning(f"Meeting {meeting_id} not found in cache")
                return []
            
            end_index = offset + limit - 1
            raw_messages = self.redis_client.lrange(chat_key, offset, end_index)
            
            messages = []
            for raw_msg in raw_messages:
                try:
                    message = json.loads(raw_msg)
                    
                    # Fixed private message filtering
                    if message.get('is_private', False):
                        recipients = message.get('recipients', [])
                        sender_id = str(message.get('user_id', ''))
                        current_user_id = str(user_id) if user_id else None
                        
                        # Show message if:
                        # 1. Current user is the sender (ALWAYS see own messages)
                        # 2. Current user is the host (see all private messages)
                        # 3. Current user is in recipients list
                        can_see_message = (
                            (current_user_id and current_user_id == sender_id) or
                            is_host or
                            (current_user_id and str(current_user_id) in [str(r) for r in recipients])
                        )
                        
                        if not can_see_message:
                            logger.info(f"⏭ Filtering out private message - user not authorized: {current_user_id} not in {sender_id}, {recipients}, is_host={is_host}")
                            continue
                    
                    messages.append(message)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse message: {raw_msg}")
                    continue
            
            return list(reversed(messages))
            
        except Exception as e:
            logger.error(f"❌ Failed to get messages from cache: {e}")
            return []

    def get_meeting_files(self, meeting_id):
        if not self.enabled:
            return []
        
        try:
            files_key = self._get_files_key(meeting_id)
            files_data = self.redis_client.hgetall(files_key)
            
            files = []
            for file_id, metadata_str in files_data.items():
                try:
                    metadata = json.loads(metadata_str)
                    metadata['size_formatted'] = self._format_file_size(metadata.get('size', 0))
                    files.append(metadata)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse file metadata: {metadata_str}")
                    continue
            
            files.sort(key=lambda x: x.get('uploaded_at', ''), reverse=True)
            return files
            
        except Exception as e:
            logger.error(f"❌ Failed to get meeting files: {e}")
            return []
    
    def delete_file(self, meeting_id, file_id, user_id):
        if not self.enabled:
            return False, "Redis not available"
        
        try:
            files_key = self._get_files_key(meeting_id)
            file_data_key = self._get_file_data_key(file_id)
            
            file_metadata_str = self.redis_client.hget(files_key, file_id)
            if not file_metadata_str:
                return False, "File not found"
            
            file_metadata = json.loads(file_metadata_str)
            
            if file_metadata.get('uploaded_by') != str(user_id):
                return False, "Not authorized to delete this file"
            
            self.redis_client.delete(file_data_key)
            self.redis_client.hdel(files_key, file_id)
            
            status_key = self._get_meeting_status_key(meeting_id)
            meeting_status = self.redis_client.get(status_key)
            if meeting_status:
                status_data = json.loads(meeting_status)
                status_data['file_count'] = max(0, status_data.get('file_count', 0) - 1)
                status_data['last_activity'] = timezone.now().isoformat()
                self.redis_client.set(status_key, json.dumps(status_data))
            
            logger.info(f"🗑 File deleted: {file_id} from meeting {meeting_id}")
            return True, "File deleted successfully"
            
        except Exception as e:
            logger.error(f"❌ Failed to delete file: {e}")
            return False, f"Delete failed: {str(e)}"
    
    def get_message_count(self, meeting_id):
        if not self.enabled:
            return 0
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            status_data = self.redis_client.get(status_key)
            
            if status_data:
                data = json.loads(status_data)
                return data.get('message_count', 0)
            
            return 0
            
        except Exception as e:
            logger.error(f"❌ Failed to get message count: {e}")
            return 0
    
    def end_meeting_chat(self, meeting_id):
        if not self.enabled:
            return False
        
        try:
            chat_key = self._get_chat_key(meeting_id)
            files_key = self._get_files_key(meeting_id)
            typing_key = self._get_typing_key(meeting_id)
            participants_key = self._get_participants_key(meeting_id)
            status_key = self._get_meeting_status_key(meeting_id)
            
            message_count = self.get_message_count(meeting_id)
            files = self.get_meeting_files(meeting_id)
            file_count = len(files)
            
            for file_metadata in files:
                file_data_key = self._get_file_data_key(file_metadata['file_id'])
                self.redis_client.delete(file_data_key)
            
            deleted_keys = self.redis_client.delete(
                chat_key, 
                files_key,
                typing_key, 
                participants_key, 
                status_key
            )
            
            logger.info(f"🗑 DELETED all enhanced chat data for meeting {meeting_id}")
            logger.info(f"   - Total messages deleted: {message_count}")
            logger.info(f"   - Total files deleted: {file_count}")
            logger.info(f"   - Redis keys deleted: {deleted_keys}")
            
            return {
                'messages_deleted': message_count,
                'files_deleted': file_count,
                'keys_deleted': deleted_keys
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to end meeting chat: {e}")
            return False
    
    def update_typing_status(self, meeting_id, user_id, user_name, is_typing=True):
        if not self.enabled:
            return False
        
        try:
            typing_key = self._get_typing_key(meeting_id)
            
            if is_typing:
                typing_data = {
                    'user_id': str(user_id),
                    'user_name': user_name,
                    'timestamp': time.time()
                }
                self.redis_client.hset(typing_key, str(user_id), json.dumps(typing_data))
                self.redis_client.expire(typing_key, CACHE_SETTINGS['TYPING_INDICATOR_TTL'])
            else:
                self.redis_client.hdel(typing_key, str(user_id))
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update typing status: {e}")
            return False
    
    def get_typing_users(self, meeting_id):
        if not self.enabled:
            return []
        
        try:
            typing_key = self._get_typing_key(meeting_id)
            typing_data = self.redis_client.hgetall(typing_key)
            
            current_time = time.time()
            active_typers = []
            
            for user_id, data_str in typing_data.items():
                try:
                    data = json.loads(data_str)
                    if current_time - data['timestamp'] < CACHE_SETTINGS['TYPING_INDICATOR_TTL']:
                        active_typers.append({
                            'user_id': data['user_id'],
                            'user_name': data['user_name'],
                            'timestamp': data['timestamp']
                        })
                    else:
                        self.redis_client.hdel(typing_key, user_id)
                except (json.JSONDecodeError, KeyError):
                    self.redis_client.hdel(typing_key, user_id)
                    continue
            
            return active_typers
            
        except Exception as e:
            logger.error(f"❌ Failed to get typing users: {e}")
            return []
    
    def is_meeting_active(self, meeting_id):
        if not self.enabled:
            return False
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            return self.redis_client.exists(status_key)
        except Exception as e:
            logger.error(f"❌ Failed to check meeting status: {e}")
            return False
    
    def get_meeting_stats(self, meeting_id):
        if not self.enabled:
            return None
        
        try:
            status_key = self._get_meeting_status_key(meeting_id)
            status_data = self.redis_client.get(status_key)
            
            if status_data:
                data = json.loads(status_data)
                chat_key = self._get_chat_key(meeting_id)
                current_message_count = self.redis_client.llen(chat_key)
                
                return {
                    'meeting_id': meeting_id,
                    'started_at': data.get('started_at'),
                    'total_messages': data.get('message_count', 0),
                    'total_files': data.get('file_count', 0),
                    'current_cached_messages': current_message_count,
                    'last_message_at': data.get('last_message_at'),
                    'last_activity': data.get('last_activity'),
                    'status': data.get('status', 'unknown'),
                    'storage_type': 'enhanced_cache_only'
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get meeting stats: {e}")
            return None

    def cleanup_expired_meetings(self):
        if not self.enabled:
            return
        
        try:
            status_keys = self.redis_client.keys("cache_meeting_status:*")
            current_time = timezone.now()
            
            for status_key in status_keys:
                try:
                    status_data = self.redis_client.get(status_key)
                    if status_data:
                        data = json.loads(status_data)
                        last_activity = data.get('last_activity')
                        
                        if last_activity:
                            last_activity_time = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                            time_diff = (current_time - last_activity_time).total_seconds()
                            
                            if time_diff > 86400:
                                meeting_id = status_key.split(':')[-1]
                                logger.info(f"🧹 Cleaning up inactive meeting: {meeting_id}")
                                self.end_meeting_chat(meeting_id)
                                
                except Exception as e:
                    logger.error(f"❌ Error cleaning up meeting: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Failed to cleanup expired meetings: {e}")

# Initialize the enhanced cache-only chat manager
enhanced_cache_chat_manager = EnhancedCacheOnlyChatManager()

# ENHANCED API ENDPOINTS

@require_http_methods(["POST"])
@csrf_exempt
def start_meeting_chat(request):
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        
        if not meeting_id:
            return JsonResponse({'error': 'meeting_id is required'}, status=400)
        
        success = enhanced_cache_chat_manager.start_meeting_chat(meeting_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Enhanced meeting chat started (cache-only)',
                'meeting_id': meeting_id,
                'storage_type': 'enhanced_cache_only',
                'auto_delete_on_end': True,
                'features': ['messages', 'files', 'typing_indicators', 'real_time_sync', 'cross_user_file_access', 'private_messages', 'private_files'],
                'settings': {
                    'max_file_size': CACHE_SETTINGS['MAX_FILE_SIZE'],
                    'max_message_length': CACHE_SETTINGS['MAX_MESSAGE_LENGTH'],
                    'file_cache_ttl_days': CACHE_SETTINGS['FILE_CACHE_TTL'] // (24 * 3600),
                    'supported_file_types': list(CACHE_SETTINGS['ALLOWED_FILE_TYPES'].keys())
                }
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to start meeting chat'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error starting meeting chat: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def send_cache_chat_message(request):
    try:
        data = json.loads(request.body)
        
        required_fields = ['meeting_id', 'user_id', 'user_name', 'message']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        message_text = data['message'].strip()
        if not message_text:
            return JsonResponse({'error': 'Message cannot be empty'}, status=400)
        
        if len(message_text) > CACHE_SETTINGS['MAX_MESSAGE_LENGTH']:
            return JsonResponse({
                'error': f'Message too long (max {CACHE_SETTINGS["MAX_MESSAGE_LENGTH"]} characters)'
            }, status=400)
        
        if not enhanced_cache_chat_manager.is_meeting_active(data['meeting_id']):
            return JsonResponse({
                'error': 'Meeting chat not active or meeting has ended'
            }, status=400)
        
        message_data = {
            'id': f"{data['meeting_id']}{data['user_id']}{int(time.time() * 1000)}",
            'user_id': data['user_id'],
            'user_name': data['user_name'],
            'message': message_text,
            'timestamp': timezone.now().isoformat(),
            'message_type': data.get('message_type', 'text'),
            'is_private': data.get('is_private', False),
            'recipients': data.get('recipients', []),
            'sender_is_host': data.get('sender_is_host', False),
            'file_data': data.get('file_data')
        }
        
        message_id = enhanced_cache_chat_manager.add_message(data['meeting_id'], message_data)
        
        if message_id:
            return JsonResponse({
                'success': True,
                'message_id': message_id,
                'timestamp': message_data['timestamp'],
                'storage_type': 'enhanced_cache_only',
                'will_delete_on_meeting_end': True,
                'broadcast_success': True
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to send and broadcast message'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error sending enhanced cache chat message: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def upload_chat_file(request):
    """FIXED: Properly handle private file recipients"""
    try:
        meeting_id = request.POST.get('meeting_id')
        user_id = request.POST.get('user_id')
        user_name = request.POST.get('user_name')
        is_private_str = request.POST.get('is_private', 'false')
        recipients_str = request.POST.get('recipients', '[]')
        
        # FIX: Ensure meeting, user info is present
        if not all([meeting_id, user_id, user_name]):
            return JsonResponse({
                'error': 'Missing required fields: meeting_id, user_id, user_name'
            }, status=400)
        
        # FIX: Properly parse is_private as boolean
        is_private = is_private_str.lower() == 'true'
        logger.info(f"📎 File upload - is_private string: '{is_private_str}' -> boolean: {is_private}")
        
        # FIX: Properly parse recipients array
        try:
            if isinstance(recipients_str, str):
                recipients = json.loads(recipients_str) if recipients_str.strip() else []
            else:
                recipients = recipients_str
            
            # Ensure it's a list
            if not isinstance(recipients, list):
                recipients = [recipients] if recipients else []
            
            # Convert all recipients to strings
            recipients = [str(r) for r in recipients if r]
            
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse recipients: {e}")
            recipients = []
        
        logger.info(f"📎 File upload - Parsed recipients:")
        logger.info(f"   - Recipients string from client: {recipients_str}")
        logger.info(f"   - Parsed recipients list: {recipients}")
        logger.info(f"   - Is Private: {is_private}")
        logger.info(f"   - Recipients count: {len(recipients)}")
        
        # FIX: Check if meeting is active
        if not enhanced_cache_chat_manager.is_meeting_active(meeting_id):
            return JsonResponse({
                'error': 'Meeting not active or has ended'
            }, status=400)
        
        # Check for file
        if 'file' not in request.FILES:
            return JsonResponse({'error': 'No file uploaded'}, status=400)
        
        uploaded_file = request.FILES['file']
        
        logger.info(f"📎 File upload debug info:")
        logger.info(f"   - Original filename: {uploaded_file.name}")
        logger.info(f"   - Content type: {uploaded_file.content_type}")
        logger.info(f"   - File size: {uploaded_file.size}")
        logger.info(f"   - Is private: {is_private}")
        logger.info(f"   - Recipients: {recipients}")
        
        # Read file data
        try:
            file_data = uploaded_file.read()
            logger.info(f"   - Bytes read: {len(file_data)}")
        except Exception as read_error:
            logger.error(f"❌ Failed to read file: {read_error}")
            return JsonResponse({'error': 'Failed to read uploaded file'}, status=500)
        
        filename = uploaded_file.name
        content_type = uploaded_file.content_type or mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        
        # Validate file data is bytes
        if not isinstance(file_data, bytes):
            logger.error(f"❌ File data is not bytes: {type(file_data)}")
            return JsonResponse({'error': 'Invalid file data format'}, status=500)
        
        logger.info(f"📤 Starting upload: {filename} ({len(file_data)} bytes)")
        
        # FIX: Pass is_private and recipients to backend manager
        success, result = enhanced_cache_chat_manager.upload_file(
            meeting_id, 
            file_data, 
            filename, 
            content_type, 
            user_id, 
            user_name,
            is_private=is_private,
            recipients=recipients if is_private else []
        )
        
        if success:
            logger.info(f"✅ File upload successful: {filename}")
            logger.info(f"   - Response is_private: {result['is_private']}")
            logger.info(f"   - Response recipients: {result['recipients']}")
            
            return JsonResponse({
                'success': True,
                'file_id': result['file_id'],
                'message_id': result['message_id'],
                'filename': result['filename'],
                'size': result['size'],
                'content_type': result['content_type'],
                'storage_type': 'enhanced_cache_only',
                'download_url': result['download_url'],
                'is_private': result['is_private'],
                'recipients': result['recipients'],
                'cross_user_access': not is_private,
                'cache_ttl_days': CACHE_SETTINGS['FILE_CACHE_TTL'] // (24 * 3600),
                'debug_info': {
                    'original_size': uploaded_file.size,
                    'processed_size': len(file_data),
                    'content_type_detected': content_type,
                    'is_private_received': is_private,
                    'recipients_received': recipients
                }
            }, status=200)
        else:
            logger.error(f"❌ File upload failed: {result}")
            return JsonResponse({'error': result}, status=400)
        
    except Exception as e:
        logger.error(f"❌ Upload error: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def download_chat_file(request, file_id):
    try:
        logger.info(f"📥 File download request for: {file_id}")
        
        file_data, metadata = enhanced_cache_chat_manager.get_file(file_id)
        
        if not file_data:
            logger.warning(f"❌ File not found: {file_id}")
            return JsonResponse({'error': 'File not found or expired'}, status=404)
        
        if not metadata:
            content_type = 'application/octet-stream'
            filename = f'file_{file_id}'
            logger.warning(f"⚠️ Using default metadata for {file_id}")
        else:
            content_type = metadata.get('content_type', 'application/octet-stream')
            filename = metadata.get('filename', f'file_{file_id}')
            logger.info(f"✅ Using stored metadata for {file_id}: {filename}")
        
        response = HttpResponse(
            file_data,
            content_type=content_type
        )
        
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['Content-Length'] = len(file_data)
        response['Cache-Control'] = 'private, max-age=3600'
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        
        if content_type.startswith('text/'):
            response['Content-Type'] = f'{content_type}; charset=utf-8'
        
        logger.info(f"📥 File download served: {filename} ({len(file_data)} bytes)")
        return response
        
    except Exception as e:
        logger.error(f"❌ Error downloading file {file_id}: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_cache_chat_history(request, meeting_id):
    try:
        limit = min(int(request.GET.get('limit', 100)), 500)
        offset = int(request.GET.get('offset', 0))
        user_id = request.GET.get('user_id')
        is_host = request.GET.get('is_host', 'false').lower() == 'true'
        
        if not enhanced_cache_chat_manager.is_meeting_active(meeting_id):
            return JsonResponse({
                'success': False,
                'error': 'Meeting not found or has ended',
                'messages': [],
                'note': 'Enhanced chat messages are automatically deleted when meeting ends'
            }, status=404)
        
        messages = enhanced_cache_chat_manager.get_messages(
            meeting_id, 
            limit, 
            offset,
            user_id=user_id,
            is_host=is_host
        )
        total_count = enhanced_cache_chat_manager.get_message_count(meeting_id)
        
        for message in messages:
            if message.get('message_type') == 'file' and message.get('file_id'):
                message['download_url'] = f'/api/cache-chat/files/{message["file_id"]}/'
                if message.get('file_data'):
                    try:
                        if isinstance(message['file_data'], str):
                            file_data_obj = json.loads(message['file_data'])
                        else:
                            file_data_obj = message['file_data']
                        
                        file_data_obj['url'] = f'/api/cache-chat/files/{message["file_id"]}/'
                        file_data_obj['originalUrl'] = f'/api/cache-chat/files/{message["file_id"]}/'
                        file_data_obj['isFromPreviousSession'] = False
                        message['file_data'] = json.dumps(file_data_obj) if isinstance(message['file_data'], str) else file_data_obj
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Failed to update file_data URLs: {e}")
        
        return JsonResponse({
            'success': True,
            'messages': messages,
            'count': len(messages),
            'total_count': total_count,
            'storage_type': 'enhanced_cache_only',
            'real_time_sync': True,
            'cross_user_file_access': True,
            'private_chat_enabled': True,
            'private_files_enabled': True,
            'sync_interval': CACHE_SETTINGS['SYNC_INTERVAL'],
            'warning': 'Messages will be deleted when meeting ends'
        }, status=200)
        
    except ValueError as e:
        return JsonResponse({'error': f'Invalid parameter: {e}'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error getting enhanced cache chat history: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def update_typing_indicator(request):
    try:
        data = json.loads(request.body)
        
        required_fields = ['meeting_id', 'user_id', 'user_name']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JsonResponse({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }, status=400)
        
        if not enhanced_cache_chat_manager.is_meeting_active(data['meeting_id']):
            return JsonResponse({
                'error': 'Meeting not active'
            }, status=400)
        
        is_typing = data.get('is_typing', True)
        
        success = enhanced_cache_chat_manager.update_typing_status(
            data['meeting_id'],
            data['user_id'],
            data['user_name'],
            is_typing
        )
        
        if success:
            return JsonResponse({
                'success': True,
                'storage_type': 'enhanced_cache_only'
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to update typing status'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error updating typing indicator: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_typing_users(request, meeting_id):
    try:
        if not enhanced_cache_chat_manager.is_meeting_active(meeting_id):
            return JsonResponse({
                'success': True,
                'typing_users': [],
                'count': 0,
                'note': 'Meeting not active'
            }, status=200)
        
        typing_users = enhanced_cache_chat_manager.get_typing_users(meeting_id)
        
        return JsonResponse({
            'success': True,
            'typing_users': typing_users,
            'count': len(typing_users),
            'storage_type': 'enhanced_cache_only'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting typing users: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def end_meeting_chat(request):
    try:
        data = json.loads(request.body)
        meeting_id = data.get('meeting_id')
        
        if not meeting_id:
            return JsonResponse({'error': 'meeting_id is required'}, status=400)
        
        stats = enhanced_cache_chat_manager.get_meeting_stats(meeting_id)
        deletion_result = enhanced_cache_chat_manager.end_meeting_chat(meeting_id)
        
        if deletion_result:
            return JsonResponse({
                'success': True,
                'message': 'Enhanced meeting ended - ALL chat messages and files DELETED',
                'meeting_id': meeting_id,
                'deleted_stats': stats,
                'deletion_summary': deletion_result,
                'storage_type': 'enhanced_cache_only'
            }, status=200)
        else:
            return JsonResponse({
                'error': 'Failed to end meeting chat'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON format'}, status=400)
    except Exception as e:
        logger.error(f"❌ Error ending meeting chat: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_meeting_chat_stats(request, meeting_id):
    try:
        stats = enhanced_cache_chat_manager.get_meeting_stats(meeting_id)
        
        if stats:
            return JsonResponse({
                'success': True,
                'stats': stats
            }, status=200)
        else:
            return JsonResponse({
                'success': False,
                'error': 'Meeting not found or has ended',
                'note': 'Enhanced chat data and files are deleted when meeting ends'
            }, status=404)
        
    except Exception as e:
        logger.error(f"❌ Error getting meeting stats: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_meeting_files(request, meeting_id):
    try:
        if not enhanced_cache_chat_manager.is_meeting_active(meeting_id):
            return JsonResponse({
                'success': False,
                'error': 'Meeting not found or has ended',
                'files': [],
                'note': 'Files are automatically deleted when meeting ends'
            }, status=404)
        
        files = enhanced_cache_chat_manager.get_meeting_files(meeting_id)
        
        for file_info in files:
            file_info['download_url'] = f'/api/cache-chat/files/{file_info["file_id"]}/'
            
        return JsonResponse({
            'success': True,
            'files': files,
            'count': len(files),
            'storage_type': 'enhanced_cache_only',
            'cross_user_access': True,
            'private_files_supported': True,
            'max_file_size': CACHE_SETTINGS['MAX_FILE_SIZE'],
            'cache_ttl_days': CACHE_SETTINGS['FILE_CACHE_TTL'] // (24 * 3600),
            'warning': 'Files will be deleted when meeting ends'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting meeting files: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["DELETE"])
@csrf_exempt
def delete_chat_file(request, meeting_id, file_id):
    try:
        user_id = request.GET.get('user_id')
        if not user_id:
            return JsonResponse({'error': 'user_id required'}, status=400)
        
        success, message = enhanced_cache_chat_manager.delete_file(meeting_id, file_id, user_id)
        
        if success:
            return JsonResponse({
                'success': True,
                'message': message
            }, status=200)
        else:
            return JsonResponse({
                'error': message
            }, status=400)
        
    except Exception as e:
        logger.error(f"❌ Error deleting file: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def get_supported_file_types(request):
    try:
        categories = {
            'documents': [],
            'images': [],
            'audio': [],
            'video': [],
            'archives': [],
            'other': []
        }
        
        for content_type, extension in CACHE_SETTINGS['ALLOWED_FILE_TYPES'].items():
            if content_type.startswith('image/'):
                categories['images'].append(extension)
            elif content_type.startswith('audio/'):
                categories['audio'].append(extension)
            elif content_type.startswith('video/'):
                categories['video'].append(extension)
            elif content_type in ['application/zip', 'application/x-rar-compressed', 'application/x-7z-compressed']:
                categories['archives'].append(extension)
            elif content_type.startswith('application/') and 'document' in content_type.lower():
                categories['documents'].append(extension)
            elif content_type in ['application/pdf', 'text/plain', 'text/csv', 'application/rtf']:
                categories['documents'].append(extension)
            else:
                categories['other'].append(extension)
        
        return JsonResponse({
            'success': True,
            'supported_types': CACHE_SETTINGS['ALLOWED_FILE_TYPES'],
            'max_file_size': CACHE_SETTINGS['MAX_FILE_SIZE'],
            'max_file_size_formatted': f"{CACHE_SETTINGS['MAX_FILE_SIZE'] / 1024 / 1024:.1f} MB",
            'categories': categories,
            'total_types': len(CACHE_SETTINGS['ALLOWED_FILE_TYPES']),
            'cache_ttl_days': CACHE_SETTINGS['FILE_CACHE_TTL'] // (24 * 3600),
            'private_files_supported': True
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error getting supported file types: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def cleanup_expired_meetings(request):
    try:
        enhanced_cache_chat_manager.cleanup_expired_meetings()
        return JsonResponse({
            'success': True,
            'message': 'Cleanup completed successfully'
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Error in cleanup: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def health_check(request):
    try:
        redis_status = "connected" if enhanced_cache_chat_manager.enabled else "disconnected"
        
        if enhanced_cache_chat_manager.enabled:
            try:
                cache_chat_redis.ping()
                redis_latency = "low"
            except:
                redis_status = "error"
                redis_latency = "high"
        else:
            redis_latency = "unknown"
        
        return JsonResponse({
            'success': True,
            'status': 'healthy',
            'redis_status': redis_status,
            'redis_latency': redis_latency,
            'version': '3.0.1',
            'features': ['enhanced_file_upload', 'message_broadcasting', 'real_time_sync', 'private_messages', 'private_files', 'cross_user_file_access'],
            'file_cache_ttl_days': CACHE_SETTINGS['FILE_CACHE_TTL'] // (24 * 3600),
            'timestamp': timezone.now().isoformat()
        }, status=200)
        
    except Exception as e:
        logger.error(f"❌ Health check error: {e}")
        return JsonResponse({
            'success': False,
            'status': 'unhealthy',
            'error': str(e)
        }, status=500)

# Enhanced URL patterns
urlpatterns = [
    path('api/cache-chat/start/', start_meeting_chat, name='start_meeting_chat'),
    path('api/cache-chat/send/', send_cache_chat_message, name='send_cache_chat_message'),
    path('api/cache-chat/upload/', upload_chat_file, name='upload_chat_file'),
    path('api/cache-chat/files/<str:file_id>/', download_chat_file, name='download_chat_file'),
    path('api/cache-chat/meeting-files/<str:meeting_id>/', get_meeting_files, name='get_meeting_files'),
    path('api/cache-chat/delete-file/<str:meeting_id>/<str:file_id>/', delete_chat_file, name='delete_chat_file'),
    path('api/cache-chat/history/<str:meeting_id>/', get_cache_chat_history, name='get_cache_chat_history'),
    path('api/cache-chat/typing/', update_typing_indicator, name='update_typing_indicator'),
    path('api/cache-chat/typing-users/<str:meeting_id>/', get_typing_users, name='get_typing_users'),
    path('api/cache-chat/end/', end_meeting_chat, name='end_meeting_chat'),
    path('api/cache-chat/stats/<str:meeting_id>/', get_meeting_chat_stats, name='get_meeting_chat_stats'),
    path('api/cache-chat/supported-types/', get_supported_file_types, name='get_supported_file_types'),
    path('api/cache-chat/cleanup/', cleanup_expired_meetings, name='cleanup_expired_meetings'),
    path('api/cache-chat/health/', health_check, name='health_check'),
]