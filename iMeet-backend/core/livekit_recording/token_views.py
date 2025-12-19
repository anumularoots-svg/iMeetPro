# core/livekit_recording/token_views.py
import os
import json
import logging
import jwt
import time
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import connection

logger = logging.getLogger(__name__)

# LiveKit credentials
LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY", "api_0582831c57af5e58e53234d700146c24")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET", "ee6b633f7a8eeaaf640a1d6f673d1238dcb0a5645ef9886e34709666a1800788")
LIVEKIT_URL = os.getenv("LIVEKIT_URL", "wss://44.201.44.40:8881")


def generate_livekit_token(identity, name, room_name, can_publish=True, can_subscribe=True):
    """Generate LiveKit JWT token manually"""
    now = int(time.time())
    exp = now + 86400  # 24 hours
    
    claims = {
        "iss": LIVEKIT_API_KEY,
        "sub": identity,
        "name": name,
        "iat": now,
        "exp": exp,
        "nbf": now,
        "video": {
            "room": room_name,
            "roomJoin": True,
            "canPublish": can_publish,
            "canSubscribe": can_subscribe,
            "canPublishData": True
        }
    }
    
    token = jwt.encode(claims, LIVEKIT_API_SECRET, algorithm="HS256")
    return token


@require_http_methods(["POST"])
@csrf_exempt
def get_livekit_token(request):
    """
    Generate LiveKit token for joining a meeting room
    Expected payload: { "room_name": "meeting-id", "participant_name": "user-name" }
    """
    try:
        data = json.loads(request.body) if request.body else {}
        
        room_name = data.get('room_name')
        participant_name = data.get('participant_name', 'Guest')
        participant_identity = data.get('participant_identity', participant_name)
        
        if not room_name:
            return JsonResponse({"error": "room_name is required"}, status=400)
        
        # Generate token
        token = generate_livekit_token(
            identity=participant_identity,
            name=participant_name,
            room_name=room_name
        )
        
        logger.info(f"Generated LiveKit token for {participant_name} in room {room_name}")
        
        return JsonResponse({
            "success": True,
            "token": token,
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
        
        # Generate token
        token = generate_livekit_token(
            identity=str(user_id),
            name=participant_name,
            room_name=room_name
        )
        
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
            "token": token,
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


@require_http_methods(["POST"])
@csrf_exempt
def join_meeting_livekit(request):
    """
    Join meeting via LiveKit - matches frontend expectation
    Expected payload: { "meeting_id": "...", "user_id": "...", "user_name": "...", "is_host": false }
    """
    try:
        data = json.loads(request.body) if request.body else {}
        
        meeting_id = data.get('meeting_id') or data.get('meetingId')
        user_id = data.get('user_id') or data.get('userId')
        user_name = data.get('user_name') or data.get('displayName') or data.get('userName', 'Guest')
        is_host = data.get('is_host') or data.get('isHost', False)
        
        if not meeting_id:
            return JsonResponse({"error": "meeting_id is required", "success": False}, status=400)
        
        # Generate participant identity
        participant_identity = f"user_{user_id}" if user_id else user_name
        
        # Create room name from meeting ID
        room_name = f"meeting_{meeting_id}"
        
        # Try to get meeting info from database
        meeting_info = None
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT ID, Meeting_Name, Host_ID, LiveKit_Room_Name 
                    FROM tbl_Meetings 
                    WHERE ID = %s OR Meeting_ID = %s
                    LIMIT 1
                """, [meeting_id, meeting_id])
                row = cursor.fetchone()
                
                if row:
                    db_id, meeting_name, host_id, livekit_room = row
                    room_name = livekit_room or f"meeting_{db_id}"
                    meeting_info = {
                        "id": db_id,
                        "name": meeting_name,
                        "host_id": host_id
                    }
                    
                    # Update LiveKit room name if not set
                    if not livekit_room:
                        cursor.execute(
                            "UPDATE tbl_Meetings SET LiveKit_Room_Name = %s WHERE ID = %s",
                            [room_name, db_id]
                        )
        except Exception as db_error:
            logger.warning(f"DB lookup failed, using meeting_id directly: {db_error}")
        
        # Generate token
        token = generate_livekit_token(
            identity=participant_identity,
            name=user_name,
            room_name=room_name,
            can_publish=True,
            can_subscribe=True
        )
        
        logger.info(f"LiveKit join: user={user_name}, room={room_name}, is_host={is_host}")
        
        return JsonResponse({
            "success": True,
            "access_token": token,
            "livekit_url": LIVEKIT_URL,
            "room_name": room_name,
            "participant_identity": participant_identity,
            "meeting_info": meeting_info
        })
        
    except Exception as e:
        logger.error(f"Error in join_meeting_livekit: {e}")
        return JsonResponse({
            "error": str(e),
            "success": False
        }, status=500)
