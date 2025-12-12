# core/stream_recording/views.py - FIXED VERSION

import json
import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db import connection
from django.utils import timezone

from core.livekit_recording.recording_service import stream_recording_service

logger = logging.getLogger(__name__)

# === STREAM RECORDING VIEWS - FIXED VERSION ===

@require_http_methods(["POST"])
@csrf_exempt
def start_stream_recording(request, meeting_id):
    """Start LiveKit stream recording - captures all participant streams"""
    try:
        # Parse request data
        data = json.loads(request.body) if request.body else {}
        user_id = data.get('user_id') or request.GET.get('user_id')
        room_name = data.get('room_name') or f"meeting_{meeting_id}"
        
        if not user_id:
            return JsonResponse({"Error": "Missing user_id"}, status=400)
        
        # Check if meeting exists in database
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT Host_ID FROM tbl_Meetings WHERE ID = %s", [meeting_id])
                row = cursor.fetchone()
                if not row:
                    return JsonResponse({"Error": "Meeting not found"}, status=404)
        except Exception as db_error:
            logger.warning(f"Database check failed: {db_error}")
        
        # Start stream recording - SYNCHRONOUS CALL (FIXED)
        result = stream_recording_service.start_stream_recording(meeting_id, user_id, room_name)
        
        if result.get("status") == "success":
            # Update database to reflect recording state
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "UPDATE tbl_Meetings SET Is_Recording_Enabled = 1 WHERE ID = %s",
                        [meeting_id]
                    )
            except Exception as db_error:
                logger.warning(f"Failed to update database recording status: {db_error}")
            
            logger.info(f"Stream recording started for meeting {meeting_id}")
            return JsonResponse(result)
        else:
            return JsonResponse(result, status=500)
            
    except Exception as e:
        logger.error(f"Error starting stream recording: {e}")
        return JsonResponse({
            "status": "error",
            "message": f"Server error: {str(e)}",
            "meeting_id": meeting_id
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def stop_stream_recording(request, meeting_id):
    """Stop LiveKit stream recording and process the video"""
    try:
        # Stop stream recording - SYNCHRONOUS CALL (FIXED)
        result = stream_recording_service.stop_stream_recording(meeting_id)
        
        # Update database to reflect stopped state
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE tbl_Meetings SET Is_Recording_Enabled = 0 WHERE ID = %s",
                    [meeting_id]
                )
        except Exception as db_error:
            logger.warning(f"Failed to update database recording status: {db_error}")
        
        if result.get("status") in ["success", "partial_success"]:
            logger.info(f"Stream recording stopped for meeting {meeting_id}")
            return JsonResponse(result)
        else:
            return JsonResponse(result, status=500)
            
    except Exception as e:
        logger.error(f"Error stopping stream recording: {e}")
        return JsonResponse({
            "status": "error",
            "message": f"Server error: {str(e)}",
            "meeting_id": meeting_id
        }, status=500)

@require_http_methods(["GET"])
def get_stream_recording_status(request, meeting_id):
    """Get current stream recording status"""
    try:
        result = stream_recording_service.get_recording_status(meeting_id)
        return JsonResponse(result)
        
    except Exception as e:
        logger.error(f"Error getting recording status: {e}")
        return JsonResponse({
            "status": "error",
            "message": f"Server error: {str(e)}",
            "meeting_id": meeting_id
        }, status=500)

@require_http_methods(["GET"])
def list_active_stream_recordings(request):
    """List all active stream recordings"""
    try:
        active_recordings = stream_recording_service.list_active_recordings()
        return JsonResponse({
            "status": "success",
            "active_recordings": active_recordings,
            "total_count": len(active_recordings)
        })
        
    except Exception as e:
        logger.error(f"Error listing active recordings: {e}")
        return JsonResponse({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }, status=500)

# === UPDATED LEGACY VIEWS TO USE STREAM RECORDING - FIXED VERSION ===

@require_http_methods(["POST"])
@csrf_exempt
def Start_Recording_Stream(request, id):
    """Updated Start_Recording to use LiveKit stream recording - FIXED VERSION"""
    try:
        # Parse request body
        recording_settings = {}
        if request.body:
            try:
                recording_settings = json.loads(request.body)
            except json.JSONDecodeError:
                pass

        with connection.cursor() as cursor:
            select_query = """
            SELECT Host_ID, Is_Recording_Enabled, Meeting_Name
            FROM tbl_Meetings
            WHERE ID = %s
            """
            cursor.execute(select_query, [id])
            row = cursor.fetchone()
            if not row:
                logger.error(f"Meeting ID {id} not found")
                return JsonResponse({"Error": "Meeting not found"}, status=404)

            host_id, is_recording_enabled, meeting_name = row
            
            # Check if recording is already active
            if is_recording_enabled:
                logger.info(f"Recording is already active for meeting {id}")
                return JsonResponse({
                    "Message": "Recording is already active",
                    "success": True,
                    "already_recording": True,
                    "is_recording": True,
                    "meeting_id": id,
                    "recording_type": "django_compatible_livekit_stream"
                }, status=200)

            # Start stream recording - SYNCHRONOUS CALL (FIXED)
            room_name = recording_settings.get('room_name', f"meeting_{id}")
            result = stream_recording_service.start_stream_recording(id, str(host_id), room_name)
            
            if result.get("status") == "success":
                # Update database
                started_at = timezone.now()
                cursor.execute(
                    "UPDATE tbl_Meetings SET Is_Recording_Enabled = 1, Started_At = %s WHERE ID = %s",
                    [started_at, id]
                )
                
                logger.info(f"Stream recording started for meeting {id}")
                
                return JsonResponse({
                    "Message": "Stream recording started - capturing all participant streams",
                    "success": True,
                    "already_recording": False,
                    "is_recording": True,
                    "meeting_id": id,
                    "recording_id": result.get("recording_id"),
                    "recording_type": "django_compatible_livekit_stream",
                    "screen_share_required": False,  # Works with or without screen share
                    "user_interaction_required": False,  # Server-side recording
                    "bot_joining": True,
                    "captures": "all_video_audio_streams",
                    "room_name": room_name,
                    "recorder_identity": result.get("recorder_identity"),
                    "ssl_fixed": True,
                    "sync_handlers": True,
                    "settings": recording_settings
                })
                
            elif result.get("status") in ["already_active", "already_exists"]:
                # Recording already exists - sync database
                started_at = timezone.now()
                cursor.execute(
                    "UPDATE tbl_Meetings SET Is_Recording_Enabled = 1, Started_At = %s WHERE ID = %s",
                    [started_at, id]
                )
                
                return JsonResponse({
                    "Message": "Recording was already active",
                    "success": True,
                    "already_recording": True,
                    "is_recording": True,
                    "meeting_id": id,
                    "recording_id": result.get("recording_id"),
                    "recording_type": "django_compatible_livekit_stream"
                })
            else:
                return JsonResponse({
                    "Error": result.get("message", "Failed to start stream recording"),
                    "success": False,
                    "meeting_id": id,
                    "recording_type": "django_compatible_livekit_stream",
                    "ssl_error": "UnknownIssuer" in str(result.get("message", ""))
                }, status=500)
            
    except Exception as e:
        logger.error(f"Database error starting recording for meeting {id}: {e}")
        return JsonResponse({
            "Error": f"Database error: {str(e)}",
            "success": False,
            "meeting_id": id
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def Stop_Recording_Stream(request, id):
    """Updated Stop_Recording to use LiveKit stream recording - FIXED VERSION"""
    try:
        # Get meeting info
        with connection.cursor() as cursor:
            cursor.execute("""
            SELECT Host_ID, Is_Recording_Enabled, Meeting_Name
            FROM tbl_Meetings
            WHERE ID = %s
            """, [id])
            
            row = cursor.fetchone()
            if not row:
                logger.error(f"Meeting ID {id} not found")
                return JsonResponse({"Error": "Meeting not found"}, status=404)

            host_id, is_recording_enabled, meeting_name = row
            
            if not is_recording_enabled:
                return JsonResponse({
                    "Message": "Recording was not active",
                    "meeting_id": id,
                    "is_recording": False,
                    "success": True,
                    "recording_type": "django_compatible_livekit_stream"
                }, status=200)

        # Stop stream recording - SYNCHRONOUS CALL (FIXED)
        result = stream_recording_service.stop_stream_recording(id)
        
        # Update database to reflect stopped state
        ended_at = timezone.now()
        with connection.cursor() as cursor:
            cursor.execute("""
            UPDATE tbl_Meetings
            SET Is_Recording_Enabled = 0, Ended_At = %s
            WHERE ID = %s
            """, [ended_at, id])
        
        if result.get("status") == "success":
            logger.info(f"Stream recording stopped and processed for meeting {id}")
            
            # Get processing results
            processing_result = result.get("processing_result", {})
            
            return JsonResponse({
                "Message": "Stream recording stopped and processed successfully",
                "success": True,
                "meeting_id": id,
                "meeting_name": meeting_name,
                "is_recording": False,
                "recording_type": "django_compatible_livekit_stream",
                "processing_completed": processing_result.get("status") == "success",
                "video_url": processing_result.get("video_url"),
                "transcript_url": processing_result.get("transcript_url"),
                "summary_url": processing_result.get("summary_url"),
                "subtitle_urls": processing_result.get("subtitle_urls", {}),
                "image_url": processing_result.get("image_url"),
                "file_size": result.get("file_size", 0),
                "transcription_available": bool(processing_result.get("transcript_url")),
                "summary_available": bool(processing_result.get("summary_url")),
                "streams_captured": "all_participant_streams",
                "ssl_fixed": True,
                "sync_handlers": True
            })
            
        elif result.get("status") == "partial_success":
            return JsonResponse({
                "Message": "Recording stopped but processing failed",
                "success": True,  # Recording stopped successfully
                "meeting_id": id,
                "meeting_name": meeting_name,
                "is_recording": False,
                "recording_type": "django_compatible_livekit_stream",
                "recording_stopped": True,
                "processing_failed": True,
                "error": result.get("error"),
                "raw_file_path": result.get("file_path")
            })
        else:
            return JsonResponse({
                "Error": result.get("message", "Failed to stop stream recording"),
                "success": False,
                "meeting_id": id,
                "recording_type": "django_compatible_livekit_stream"
            }, status=500)

    except Exception as e:
        logger.error(f"Critical failure stopping recording for meeting {id}: {e}")
        
        # Still update database to show recording stopped
        try:
            ended_at = timezone.now()
            with connection.cursor() as cursor:
                cursor.execute("""
                UPDATE tbl_Meetings
                SET Is_Recording_Enabled = 0, Ended_At = %s
                WHERE ID = %s
                """, [ended_at, id])
        except Exception:
            pass
        
        return JsonResponse({
            "Error": f"Critical error: {str(e)}", 
            "success": False,
            "meeting_id": id,
            "error_type": type(e).__name__
        }, status=500)

# === FIXED LEGACY RECORDING VIEWS ===

@require_http_methods(["POST"])
@csrf_exempt
def Start_Recording(request, id):
    """Start LiveKit stream recording - FIXED VERSION"""
    try:
        # Parse request body for additional settings
        recording_settings = {}
        if request.body:
            try:
                recording_settings = json.loads(request.body)
            except json.JSONDecodeError:
                pass

        with connection.cursor() as cursor:
            select_query = """
            SELECT Host_ID, Is_Recording_Enabled, Meeting_Name
            FROM tbl_Meetings
            WHERE ID = %s
            """
            cursor.execute(select_query, [id])
            row = cursor.fetchone()
            if not row:
                logger.error(f"Meeting ID {id} not found")
                return JsonResponse({"Error": "Meeting not found"}, status=404)

            host_id, is_recording_enabled, meeting_name = row
            
            # Check if recording is already active
            if is_recording_enabled:
                logger.info(f"Recording is already active for meeting {id}")
                return JsonResponse({
                    "Message": "Recording is already active",
                    "success": True,
                    "already_recording": True,
                    "is_recording": True,
                    "meeting_id": id,
                    "recording_type": "livekit_stream"
                }, status=200)

            # Start LiveKit stream recording
            try:
                room_name = recording_settings.get('room_name', f"meeting_{id}")
                
                # FIXED: Direct synchronous call
                result = stream_recording_service.start_stream_recording(id, str(host_id), room_name)
                
                if result.get("status") == "success":
                    # Update database to reflect recording state
                    started_at = timezone.now()
                    update_query = """
                    UPDATE tbl_Meetings
                    SET Is_Recording_Enabled = 1, Started_At = %s
                    WHERE ID = %s
                    """
                    cursor.execute(update_query, [started_at, id])
                    
                    logger.info(f"Stream recording started for meeting {id}")
                    
                    return JsonResponse({
                        "Message": "Stream recording started - capturing all participant streams",
                        "success": True,
                        "already_recording": False,
                        "is_recording": True,
                        "meeting_id": id,
                        "recording_id": result.get("recording_id"),
                        "recording_type": "livekit_stream",
                        "screen_share_required": False,
                        "user_interaction_required": False,
                        "bot_joining": True,
                        "captures": "all_video_audio_streams_and_screen_shares",
                        "room_name": room_name,
                        "recorder_identity": result.get("recorder_identity"),
                        "like_google_meet": True,
                        "records_all_participants": True,
                        "ssl_fixed": True,
                        "sync_handlers": True,
                        "settings": recording_settings
                    })
                    
                elif result.get("status") in ["already_active", "already_exists"]:
                    # Recording already exists - sync database
                    started_at = timezone.now()
                    cursor.execute(update_query, [started_at, id])
                    
                    return JsonResponse({
                        "Message": "Recording was already active",
                        "success": True,
                        "already_recording": True,
                        "is_recording": True,
                        "meeting_id": id,
                        "recording_id": result.get("recording_id"),
                        "recording_type": "livekit_stream"
                    })
                    
                else:
                    return JsonResponse({
                        "Error": result.get("message", "Failed to start stream recording"),
                        "success": False,
                        "meeting_id": id,
                        "recording_type": "livekit_stream",
                        "ssl_error": "UnknownIssuer" in str(result.get("message", ""))
                    }, status=500)
                    
            except Exception as e:
                logger.error(f"Error starting stream recording: {e}")
                return JsonResponse({
                    "Error": f"Stream recording failed: {str(e)}",
                    "success": False,
                    "meeting_id": id
                }, status=500)
            
    except Exception as e:
        logger.error(f"Database error starting recording for meeting {id}: {e}")
        return JsonResponse({
            "Error": f"Database error: {str(e)}",
            "success": False,
            "meeting_id": id
        }, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def Stop_Recording(request, id):
    """Stop LiveKit stream recording - FIXED VERSION"""
    try:
        # Get meeting info BEFORE updating
        with connection.cursor() as cursor:
            cursor.execute("""
            SELECT Host_ID, Is_Recording_Enabled, Meeting_Name
            FROM tbl_Meetings
            WHERE ID = %s
            """, [id])
            
            row = cursor.fetchone()
            if not row:
                logger.error(f"Meeting ID {id} not found")
                return JsonResponse({"Error": "Meeting not found"}, status=404)

            host_id, is_recording_enabled, meeting_name = row
            
            if not is_recording_enabled:
                return JsonResponse({
                    "Message": "Recording was not active",
                    "meeting_id": id,
                    "is_recording": False,
                    "success": True,
                    "recording_type": "livekit_stream"
                }, status=200)

        # Stop LiveKit stream recording
        try:
            # FIXED: Direct synchronous call
            result = stream_recording_service.stop_stream_recording(id)
            
            # Update database to reflect stopped state FIRST
            ended_at = timezone.now()
            with connection.cursor() as cursor:
                cursor.execute("""
                UPDATE tbl_Meetings
                SET Is_Recording_Enabled = 0, Ended_At = %s
                WHERE ID = %s
                """, [ended_at, id])
            
            # Handle processing based on result
            if result and result.get("status") == "success":
                # Check if processing was completed
                processing_result = result.get("processing_result", {})
                
                if processing_result.get("status") == "success":
                    logger.info(f"Stream recording stopped AND PROCESSED for meeting {id}")
                    
                    return JsonResponse({
                        "Message": "Stream recording stopped and processed successfully",
                        "success": True,
                        "meeting_id": id,
                        "meeting_name": meeting_name,
                        "is_recording": False,
                        "recording_type": "livekit_stream",
                        "processing_completed": True,
                        "video_url": processing_result.get("video_url"),
                        "transcript_url": processing_result.get("transcript_url"),
                        "summary_url": processing_result.get("summary_url"),
                        "subtitle_urls": processing_result.get("subtitle_urls", {}),
                        "image_url": processing_result.get("image_url"),
                        "file_size": result.get("file_size", 0),
                        "transcription_available": bool(processing_result.get("transcript_url")),
                        "summary_available": bool(processing_result.get("summary_url")),
                        "streams_captured": "all_participant_streams",
                        "like_google_meet": True,
                        "captured_all_participants": True,
                        "ssl_fixed": True,
                        "sync_handlers": True
                    })
                else:
                    # Recording stopped but processing failed
                    return JsonResponse({
                        "Message": "Recording stopped but processing failed",
                        "success": True,
                        "meeting_id": id,
                        "meeting_name": meeting_name,
                        "is_recording": False,
                        "recording_type": "livekit_stream",
                        "recording_stopped": True,
                        "processing_failed": True,
                        "processing_error": processing_result.get("error"),
                        "file_path": result.get("file_path"),
                        "file_size": result.get("file_size", 0),
                        "suggestion": "Raw file available for manual processing"
                    })
                    
            elif result and result.get("status") == "partial_success":
                return JsonResponse({
                    "Message": "Stream recording stopped but had processing issues",
                    "success": True,
                    "meeting_id": id,
                    "meeting_name": meeting_name,
                    "is_recording": False,
                    "recording_type": "livekit_stream",
                    "recording_stopped": True,
                    "processing_issues": True,
                    "error": result.get("error"),
                    "file_path": result.get("file_path")
                })
            else:
                error_msg = "Stream recording failed to produce valid output"
                if result:
                    error_msg = result.get("message", error_msg)
                
                logger.warning(f"Stream recording failed for meeting {id}: {error_msg}")
                
                return JsonResponse({
                    "Message": "Recording stopped but stream recording failed",
                    "success": True,
                    "meeting_id": id,
                    "meeting_name": meeting_name,
                    "is_recording": False,
                    "recording_type": "livekit_stream",
                    "recording_stopped": True,
                    "stream_recording_failed": True,
                    "error": error_msg,
                    "reason": "Stream recording bot failed to capture meeting content",
                    "ssl_error": "UnknownIssuer" in str(error_msg)
                })
                
        except Exception as e:
            logger.error(f"Error stopping stream recording: {e}")
            
            # Still update database to show recording stopped
            try:
                ended_at = timezone.now()
                with connection.cursor() as cursor:
                    cursor.execute("""
                    UPDATE tbl_Meetings
                    SET Is_Recording_Enabled = 0, Ended_At = %s
                    WHERE ID = %s
                    """, [ended_at, id])
            except Exception:
                pass
            
            return JsonResponse({
                "Error": f"Failed to stop stream recording: {str(e)}",
                "success": False,
                "meeting_id": id,
                "recording_type": "livekit_stream",
                "error_type": type(e).__name__
            }, status=500)

    except Exception as e:
        logger.error(f"Critical failure for meeting {id}: {e}")
        
        return JsonResponse({
            "Error": f"Critical error: {str(e)}", 
            "success": False,
            "meeting_id": id,
            "error_type": type(e).__name__
        }, status=500)
    