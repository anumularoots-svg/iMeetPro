# core/livekit_recording/token_views.py
import os
import json
import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import connection

logger = logging.getLogger(__name__)

# LiveKit credentials
LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY", "api_0582831c57af5e58e53234d700146c24")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET", "ee6b633f7a8eeaaf640a1d6f673d1238dcb0a5645ef9886e34709666a1800788")
LIVEKIT_URL = os.getenv("LIVEKIT_URL", "wss://44.201.44.40:8881")

try:
    from livekit.api import AccessToken, VideoGrants
    LIVEKIT_AVAILABLE = True
except ImportError:
    LIVEKIT_AVAILABLE = False
    logger.warning("LiveKit API not available")


@require_http_methods(["POST"])
@csrf_exempt
def get_livekit_token(request):
    """
    Generate LiveKit token for joining a meeting room
    Expected payload: { "room_name": "meeting-id", "participant_name": "user-name", "participant_identity": "user-id" }
    """
    if not LIVEKIT_AVAILABLE:
        return JsonResponse({"error": "LiveKit not available"}, status=500)
    
    try:
        data = json.loads(request.body) if request.body else {}
        
        room_name = data.get('room_name')
        participant_name = data.get('participant_name', 'Guest')
        participant_identity = data.get('participant_identity', participant_name)
        
        if not room_name:
            return JsonResponse({"error": "room_name is required"}, status=400)
        
        # Create access token
        token = AccessToken(
            api_key=LIVEKIT_API_KEY,
            api_secret=LIVEKIT_API_SECRET
        )
        
        # Set participant info
        token.identity = participant_identity
        token.name = participant_name
        
        # Set video grants (permissions)
        grant = VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=True,
            can_subscribe=True,
            can_publish_data=True,
        )
        token.video_grant = grant
        
        # Generate JWT token
        jwt_token = token.to_jwt()
        
        logger.info(f"Generated LiveKit token for {participant_name} in room {room_name}")
        
        return JsonResponse({
            "success": True,
            "token": jwt_token,
            "url": LIVEKIT_URL,
            "room_name": room_name,
            "participant_name": participant_name,
            "participant_identity": participant_identity
        })
        
    except Exception as e:
        logger.error(f"Error generating LiveKit token: {e}")
        return JsonResponse({
            "error": str(e),
            "success": False
        }, status=500)


@require_http_methods(["POST"])
@csrf_exempt  
def join_meeting(request, meeting_id):
    """
    Join a meeting - validates meeting and returns LiveKit token
    Expected payload: { "participant_name": "user-name", "user_id": "user-id" }
    """
    if not LIVEKIT_AVAILABLE:
        return JsonResponse({"error": "LiveKit not available"}, status=500)
    
    try:
        data = json.loads(request.body) if request.body else {}
        
        participant_name = data.get('participant_name', 'Guest')
        user_id = data.get('user_id', participant_name)
        
        # Verify meeting exists
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT ID, Meeting_Name, Host_ID, LiveKit_Room_Name 
                FROM tbl_Meetings 
                WHERE ID = %s OR Meeting_ID = %s
            """, [meeting_id, meeting_id])
            row = cursor.fetchone()
            
            if not row:
                return JsonResponse({"error": "Meeting not found"}, status=404)
            
            db_id, meeting_name, host_id, livekit_room = row
            
        # Use existing room name or create one
        room_name = livekit_room or f"meeting_{db_id}"
        
        # Create access token
        token = AccessToken(
            api_key=LIVEKIT_API_KEY,
            api_secret=LIVEKIT_API_SECRET
        )
        
        token.identity = str(user_id)
        token.name = participant_name
        
        grant = VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=True,
            can_subscribe=True,
            can_publish_data=True,
        )
        token.video_grant = grant
        
        jwt_token = token.to_jwt()
        
        # Update meeting with LiveKit room name if not set
        if not livekit_room:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE tbl_Meetings SET LiveKit_Room_Name = %s WHERE ID = %s",
                    [room_name, db_id]
                )
        
        logger.info(f"User {participant_name} joining meeting {meeting_id}")
        
        return JsonResponse({
            "success": True,
            "token": jwt_token,
            "url": LIVEKIT_URL,
            "room_name": room_name,
            "meeting_id": meeting_id,
            "meeting_name": meeting_name,
            "participant_name": participant_name
        })
        
    except Exception as e:
        logger.error(f"Error joining meeting {meeting_id}: {e}")
        return JsonResponse({
            "error": str(e),
            "success": False
        }, status=500)
