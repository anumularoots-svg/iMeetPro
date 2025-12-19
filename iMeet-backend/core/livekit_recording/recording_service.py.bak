# Now safe to do other imports
from core.WebSocketConnection import enhanced_logging_config
import asyncio
import threading
import time
import logging
import weakref
import os
from functools import wraps
import json
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
import subprocess
from pathlib import Path
import signal
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import cv2
import numpy as np
from PIL import Image
import ssl
import wave
import struct
from collections import deque
import math

# ADD THIS AT THE TOP (after imports):
import boto3
import io

# Configure S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "connectly-storage")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

S3_FOLDERS = {
    "videos": os.getenv("S3_FOLDER_VIDEOS", "videos"),
    "recordings_temp": os.getenv("S3_FOLDER_RECORDINGS_TEMP", "recordings_temp")
}

# Configure SSL to trust self-signed certificates BEFORE importing LiveKit
def configure_ssl_bypass():
    """Configure SSL to accept self-signed certificates"""
    try:
        import ssl
        import urllib3
        from urllib3.exceptions import InsecureRequestWarning
        
        # Disable SSL warnings
        urllib3.disable_warnings(InsecureRequestWarning)
        
        # Create unverified SSL context
        ssl._create_default_https_context = ssl._create_unverified_context
        
        # Set additional environment variables for Rust/WebRTC
        os.environ.update({
            'LIVEKIT_ACCEPT_INVALID_CERTS': '1',
            'LIVEKIT_SKIP_CERT_VERIFICATION': '1',
            'LIVEKIT_DISABLE_SSL_VERIFICATION': '1',
            'RUSTLS_DANGEROUS_INSECURE_CLIENT': '1',
            'RUST_TLS_DANGEROUS_DISABLE_VERIFICATION': '1',
            'WEBRTC_IGNORE_SSL_ERRORS': '1',
            'WEBSOCKET_SSL_VERIFY': 'false'
        })
        
        logging.info("✅ SSL bypass configured for self-signed certificates")
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to configure SSL bypass: {e}")
        return False

# Configure SSL BEFORE importing LiveKit
configure_ssl_bypass()

# Force LiveKit to use a more compatible event loop policy
if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
elif hasattr(asyncio, 'DefaultEventLoopPolicy'):
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

# Patch asyncio to handle closed loop errors more gracefully
original_put_nowait = getattr(asyncio.Queue, 'put_nowait', None)

def safe_put_nowait(self, item):
    """Safe version of put_nowait that handles closed loops"""
    try:
        if hasattr(asyncio.Queue, '_put_nowait_original'):
            return self._put_nowait_original(item)
        else:
            return self._put_nowait(item)
    except RuntimeError as e:
        if "Event loop is closed" in str(e):
            pass
        else:
            raise

if original_put_nowait and not hasattr(asyncio.Queue, '_put_nowait_original'):
    asyncio.Queue._put_nowait_original = original_put_nowait
    asyncio.Queue.put_nowait = safe_put_nowait

from pymongo import MongoClient
from django.db import connection
from django.conf import settings

try:
    from livekit import api, rtc
    import jwt
    LIVEKIT_SDK_AVAILABLE = True
    logging.info("✅ LiveKit SDK loaded successfully")
except ImportError:
    LIVEKIT_SDK_AVAILABLE = False
    logging.error("❌ LiveKit SDK not available. Install with: pip install livekit")

logger = logging.getLogger('recording_service_module')

def setup_livekit_logging():
    """Set up logging to reduce LiveKit noise"""
    livekit_loggers = [
        'livekit',
        'livekit.rtc',
        'livekit.api',
        'livekit_ffi'
    ]
    
    for logger_name in livekit_loggers:
        lk_logger = logging.getLogger(logger_name)
        lk_logger.setLevel(logging.ERROR)
        
        class EventLoopErrorFilter(logging.Filter):
            def filter(self, record):
                message = record.getMessage()
                return not ("Event loop is closed" in message or 
                          "error putting to queue" in message)
        
        lk_logger.addFilter(EventLoopErrorFilter())

setup_livekit_logging()

class LiveKitEventLoopManager:
    """Manages LiveKit event loops to prevent 'Event loop is closed' errors"""
    
    def __init__(self):
        self._active_loops = weakref.WeakSet()
        self._cleanup_locks = {}
        self._shutdown_event = threading.Event()
        
    def register_loop(self, loop, identifier):
        """Register a loop for management"""
        self._active_loops.add(loop)
        self._cleanup_locks[identifier] = threading.Lock()
        
    def safe_run_until_complete(self, loop, coro, timeout=30, identifier=None):
        """Run coroutine with timeout and proper error handling"""
        if identifier and identifier in self._cleanup_locks:
            with self._cleanup_locks[identifier]:
                return self._run_with_timeout(loop, coro, timeout)
        else:
            return self._run_with_timeout(loop, coro, timeout)
    
    def _run_with_timeout(self, loop, coro, timeout):
        """Internal method to run coroutine with timeout"""
        try:
            if loop.is_closed():
                return None
                
            task = asyncio.ensure_future(coro, loop=loop)
            return loop.run_until_complete(
                asyncio.wait_for(task, timeout=timeout)
            )
            
        except asyncio.TimeoutError:
            logger.warning(f"Operation timed out after {timeout}s")
            return None
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                logger.debug("Event loop was already closed - this is expected during cleanup")
                return None
            raise
        except Exception as e:
            logger.warning(f"Operation failed: {e}")
            return None
    
    def force_cleanup_loop(self, loop, identifier=None):
        """Force cleanup of a loop with maximum effort"""
        if not loop or loop.is_closed():
            return
            
        try:
            if identifier and identifier in self._cleanup_locks:
                with self._cleanup_locks[identifier]:
                    self._do_force_cleanup(loop)
            else:
                self._do_force_cleanup(loop)
                
        except Exception as e:
            logger.warning(f"Force cleanup error: {e}")
        finally:
            if identifier and identifier in self._cleanup_locks:
                del self._cleanup_locks[identifier]
    
    def cleanup_all_loops(self):
        """Cleanup all managed loops"""
        try:
            logger.info("Cleaning up all event loops...")
            for loop in list(self._active_loops):
                try:
                    if not loop.is_closed():
                        self._do_force_cleanup(loop)
                except:
                    pass
            
            self._active_loops.clear()
            self._cleanup_locks.clear()
            logger.info("All event loops cleaned up")
        except Exception as e:
            logger.warning(f"Error during cleanup_all_loops: {e}")
    
    def _do_force_cleanup(self, loop):
        """Perform the actual force cleanup"""
        try:
            if not loop.is_closed():
                pending = asyncio.all_tasks(loop)
                if pending:
                    for task in pending:
                        if not task.done():
                            task.cancel()
                    
                    try:
                        loop.run_until_complete(
                            asyncio.wait_for(
                                asyncio.gather(*pending, return_exceptions=True),
                                timeout=5.0
                            )
                        )
                    except:
                        pass
            
            time.sleep(2.0)
            
            if not loop.is_closed():
                loop.close()
                
        except Exception:
            try:
                if not loop.is_closed():
                    loop.close()
            except:
                pass

loop_manager = LiveKitEventLoopManager()

# ====== S3 CHUNK UPLOADER (NO CHANGES NEEDED) ======
class S3ChunkUploader:
    """Uploads file chunks to S3 using MULTIPART UPLOAD"""
    
    def __init__(self, bucket: str, s3_key: str, chunk_size_mb: int = 5):
        self.bucket = bucket
        self.s3_key = s3_key
        self.chunk_size = chunk_size_mb * 1024 * 1024
        
        self.last_uploaded_size = 0
        self.total_uploaded = 0
        self.is_uploading = True
        self.upload_thread = None
        self.lock = threading.Lock()
        
        self.multipart_upload_id = None
        self.part_number = 0
        self.uploaded_parts = []
        
        logger.info(f"🚀 S3 Chunk Uploader (Multipart) initialized: {s3_key} ({chunk_size_mb}MB chunks)")
    
    def start_chunk_monitor(self, local_file_path: str):
        """Start background thread to monitor and upload chunks"""
        self.upload_thread = threading.Thread(
            target=self._chunk_upload_loop,
            args=(local_file_path,),
            daemon=False
        )
        self.upload_thread.start()
        logger.info(f"📤 Chunk upload monitor started for: {local_file_path}")
    
    def _chunk_upload_loop(self, local_file_path: str):
        """Continuously monitor local file and upload new chunks to S3 using multipart"""
        try:
            check_interval = 0.5
            last_log_time = time.time()
            
            while self.is_uploading:
                try:
                    if not os.path.exists(local_file_path):
                        time.sleep(check_interval)
                        continue
                    
                    current_size = os.path.getsize(local_file_path)
                    
                    if current_size >= self.last_uploaded_size + self.chunk_size:
                        self.part_number += 1
                        self._upload_chunk_multipart(
                            local_file_path,
                            self.last_uploaded_size,
                            current_size,
                            self.part_number
                        )
                        self.last_uploaded_size = current_size
                    
                    now = time.time()
                    if now - last_log_time >= 5:
                        logger.info(
                            f"📊 Upload progress: {self.total_uploaded / (1024*1024):.1f}MB uploaded, "
                            f"Local file: {current_size / (1024*1024):.1f}MB, "
                            f"Parts: {len(self.uploaded_parts)}"
                        )
                        last_log_time = now
                    
                    time.sleep(check_interval)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Chunk monitor error: {e}")
                    time.sleep(check_interval)
        
        except Exception as e:
            logger.error(f"❌ Chunk upload loop failed: {e}")
        finally:
            logger.info("🛑 Chunk upload monitor stopped")
    
    def _upload_chunk_multipart(self, local_file_path: str, start_byte: int, end_byte: int, part_number: int):
        """Upload a chunk using S3 multipart upload"""
        try:
            if self.multipart_upload_id is None:
                response = s3_client.create_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.s3_key
                )
                self.multipart_upload_id = response['UploadId']
                logger.info(f"✅ Initiated multipart upload: {self.multipart_upload_id}")
            
            with open(local_file_path, 'rb') as f:
                f.seek(start_byte)
                chunk_data = f.read(end_byte - start_byte)
            
            if not chunk_data:
                return
            
            chunk_size_mb = len(chunk_data) / (1024 * 1024)
            
            response = s3_client.upload_part(
                Bucket=self.bucket,
                Key=self.s3_key,
                PartNumber=part_number,
                UploadId=self.multipart_upload_id,
                Body=chunk_data
            )
            
            etag = response['ETag']
            part_info = {
                'ETag': etag,
                'PartNumber': part_number
            }
            self.uploaded_parts.append(part_info)
            
            with self.lock:
                self.total_uploaded += len(chunk_data)
            
            logger.info(
                f"✅ Part {part_number} uploaded: {chunk_size_mb:.1f}MB "
                f"(Total: {self.total_uploaded / (1024*1024):.1f}MB) | ETag: {etag[:20]}..."
            )
        
        except Exception as e:
            logger.error(f"❌ Part {part_number} upload failed: {e}")
    
    def stop_and_upload_final(self, local_file_path: str):
        """Stop monitoring, upload final chunk, and complete multipart upload"""
        self.is_uploading = False
        
        if self.upload_thread and self.upload_thread.is_alive():
            logger.info("⏳ Waiting for chunk upload thread to finish...")
            self.upload_thread.join(timeout=60)
        
        try:
            if os.path.exists(local_file_path):
                current_size = os.path.getsize(local_file_path)
                
                if current_size > self.last_uploaded_size:
                    logger.info(f"📤 Uploading final chunk: {current_size - self.last_uploaded_size} bytes")
                    self.part_number += 1
                    self._upload_chunk_multipart(
                        local_file_path,
                        self.last_uploaded_size,
                        current_size,
                        self.part_number
                    )
                
                logger.info(f"✅ All chunks uploaded: {self.total_uploaded / (1024*1024):.1f}MB total")
        except Exception as e:
            logger.error(f"❌ Final chunk upload failed: {e}")
        
        try:
            if self.multipart_upload_id and len(self.uploaded_parts) > 0:
                logger.info(f"🔗 Completing multipart upload with {len(self.uploaded_parts)} parts...")
                
                self.uploaded_parts.sort(key=lambda x: x['PartNumber'])
                
                response = s3_client.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.s3_key,
                    UploadId=self.multipart_upload_id,
                    MultipartUpload={
                        'Parts': self.uploaded_parts
                    }
                )
                
                logger.info(f"✅ Multipart upload completed: {response['Key']}")
                logger.info(f"📊 Final file ETag: {response['ETag']}")
            else:
                logger.warning("⚠️ No multipart upload to complete")
        
        except Exception as e:
            logger.error(f"❌ Multipart upload completion failed: {e}")
            try:
                if self.multipart_upload_id:
                    s3_client.abort_multipart_upload(
                        Bucket=self.bucket,
                        Key=self.s3_key,
                        UploadId=self.multipart_upload_id
                    )
                    logger.info(f"🛑 Aborted multipart upload")
            except Exception as abort_error:
                logger.warning(f"⚠️ Could not abort multipart upload: {abort_error}")


# ====== 🎬 AGGRESSIVE FRAME INTERPOLATOR ======
class AggressiveFrameProcessor:
    """Creates truly SMOOTH video with aggressive temporal interpolation"""
     
    def __init__(self, stream_recorder, target_fps=20):
        self.stream_recorder = stream_recorder
        self.target_fps = target_fps
        self.frame_interval = 1.0 / target_fps
        self.raw_frame_queue = deque()
        self.is_processing = False
        self.processor_thread = None
        self.frames_processed = 0
        self.frames_queued = 0
        self.last_frame = None
        self.last_frame_time = 0
        
        logger.info(f"✅ AGGRESSIVE Frame Interpolator initialized - Target: {target_fps} FPS")
    
    def queue_raw_frame(self, livekit_frame, timestamp, source_type):
        """Queue RAW LiveKit frame for processing"""
        self.raw_frame_queue.append({
            'livekit_frame': livekit_frame,
            'timestamp': timestamp,
            'source_type': source_type
        })
        self.frames_queued += 1
    
    def start(self):
        """Start the fast processing thread"""
        self.is_processing = True
        self.processor_thread = threading.Thread(
            target=self._fast_processing_loop,
            daemon=False,
            name="FastFrameProcessor"
        )
        self.processor_thread.start()
        logger.info(f"🚀 AGGRESSIVE frame processing started - Target: {self.target_fps} FPS")
    
    def stop(self):
        """Stop the processing thread"""
        self.is_processing = False
        if self.processor_thread and self.processor_thread.is_alive():
            self.processor_thread.join(timeout=10)
        logger.info(f"✅ AGGRESSIVE frame interpolator stopped. Processed: {self.frames_processed}")
      
    def _fast_processing_loop(self):
        """Background thread: AGGRESSIVE frame interpolation for truly smooth video"""
        logger.info("🎬 AGGRESSIVE frame interpolation started")
        
        output_interval = 1.0 / self.target_fps  # 0.05s for 20 FPS
        next_output_time = 0
        last_real_frame = None
        last_real_timestamp = 0
        
        while self.is_processing or len(self.raw_frame_queue) > 0:
            try:
                current_time = time.perf_counter() - self.stream_recorder.start_perf_counter
                
                # Process incoming real frames
                while len(self.raw_frame_queue) > 0:
                    frame_data = self.raw_frame_queue.popleft()
                    opencv_frame = self._convert_livekit_to_opencv(frame_data['livekit_frame'])
                    
                    if opencv_frame is not None:
                        # Store as the latest real frame
                        last_real_frame = opencv_frame.copy()
                        last_real_timestamp = frame_data['timestamp']
                        
                        # Add the real frame
                        self.stream_recorder.add_video_frame(
                            opencv_frame,
                            source_type=frame_data['source_type'],
                            timestamp_override=frame_data['timestamp']
                        )
                        self.frames_processed += 1
                
                # AGGRESSIVE INTERPOLATION: Fill gaps with regular intervals
                if last_real_frame is not None and current_time >= next_output_time:
                    # Calculate how many frames to create
                    time_since_last_real = current_time - last_real_timestamp
                    
                    if time_since_last_real < 2.0:  # Within 2 seconds of real frame
                        # Create interpolated frame with slight motion blur for smoothness
                        interpolated_frame = self._create_smooth_frame(
                            last_real_frame, 
                            current_time, 
                            last_real_timestamp
                        )
                        
                        self.stream_recorder.add_video_frame(
                            interpolated_frame,
                            source_type="smooth_interpolated",
                            timestamp_override=current_time
                        )
                        self.frames_processed += 1
                    
                    next_output_time += output_interval
                
                # Efficient sleep based on next required frame time
                sleep_time = max(0.001, (next_output_time - current_time) / 2)
                time.sleep(min(sleep_time, 0.01))
                    
            except Exception as e:
                logger.warning(f"⚠️ SMOOTH processing error: {e}")
                time.sleep(0.01)
                continue
        
        logger.info(f"✅ AGGRESSIVE interpolation finished. Total: {self.frames_processed}")
    
    def _create_smooth_frame(self, base_frame, current_time, base_timestamp):
        """Create smooth interpolated frame with subtle motion blur"""
        try:
            # Calculate age of base frame
            frame_age = current_time - base_timestamp
            
            # Add subtle motion blur based on age for smoothness
            if frame_age > 0.2:  # Older than 200ms
                # Apply slight blur to suggest motion/staleness
                blurred = cv2.GaussianBlur(base_frame, (3, 3), 0.5)
                # Blend with original (more blur = more age)
                blur_factor = min(0.3, frame_age * 0.1)
                smooth_frame = cv2.addWeighted(base_frame, 1 - blur_factor, blurred, blur_factor, 0)
            else:
                smooth_frame = base_frame.copy()
            
            # Add tiny timestamp overlay for debugging (optional)
            if hasattr(self, 'debug_mode') and self.debug_mode:
                cv2.putText(smooth_frame, f"{frame_age:.1f}s", 
                           (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 1)
            
            return smooth_frame
            
        except Exception:
            return base_frame.copy()  # Fallback to original
    
    def _convert_livekit_to_opencv(self, frame):
        """Optimized frame conversion"""
        try:
            if not frame or not hasattr(frame, 'width'):
                return None
            
            width, height = frame.width, frame.height
            
            # Try RGBA first (fastest)
            try:
                rgba_frame = frame.convert(rtc.VideoBufferType.RGBA)
                if rgba_frame and rgba_frame.data:
                    rgba_data = rgba_frame.data
                    expected_size = height * width * 4
                    
                    rgba_array = np.frombuffer(rgba_data, dtype=np.uint8, count=expected_size)
                    if len(rgba_array) == expected_size:
                        rgba_array = rgba_array.reshape((height, width, 4))
                        return cv2.cvtColor(rgba_array, cv2.COLOR_RGBA2BGR)
            except:
                pass
            
            # Fallback to RGB24
            try:
                rgb_frame = frame.convert(rtc.VideoBufferType.RGB24)
                if rgb_frame and rgb_frame.data:
                    rgb_data = rgb_frame.data
                    expected_size = height * width * 3
                    
                    rgb_array = np.frombuffer(rgb_data, dtype=np.uint8, count=expected_size)
                    if len(rgb_array) == expected_size:
                        rgb_array = rgb_array.reshape((height, width, 3))
                        return cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)
            except:
                pass
            
            return None
            
        except Exception:
            return None

class StreamingRecordingWithChunks:
    """Recording with streaming chunk uploads to S3 and FAST VIDEO"""
    
    def __init__(self, meeting_id: str, target_fps: int = 20):
        self.meeting_id = meeting_id
        self.target_fps = target_fps  # FIXED TARGET FPS for fast smooth output
        self.s3_prefix = f"{S3_FOLDERS['recordings_temp']}/{meeting_id}"
        
        self.temp_video_fd, self.temp_video_path = tempfile.mkstemp(
            suffix='.avi',
            prefix=f'recording_{meeting_id}_'
        )
        os.close(self.temp_video_fd)
        
        self.s3_video_key = f"{self.s3_prefix}/raw_video_{meeting_id}.avi"
        self.chunk_uploader = None
        
        # 🎬 AGGRESSIVE frame processor with target FPS
        self.frame_processor = AggressiveFrameProcessor(self, target_fps)
        
        self.video_frames = []
        self.raw_audio_data = []
        self.start_time = None
        self.start_perf_counter = None
        self.is_recording = False
        self.frame_lock = threading.Lock()
        self.audio_lock = threading.Lock()
        
        self.active_audio_tracks = {}
        self.participant_audio_buffers = {}
        self.processing_tracks = set()
        
        self.AUDIO_BUFFER_SIZE = 4800
        self.frame_lookup = None
        self.frame_lookup_built = False
        
        logger.info(f"✅ FAST Streaming Recorder - Target: {target_fps} FPS")
        logger.info(f"📝 Temp file: {self.temp_video_path}")
    
    def start_recording(self):
        """Start recording and chunk uploader"""
        self.start_time = time.time()
        self.start_perf_counter = time.perf_counter()
        self.is_recording = True
        self.video_frames = []
        self.raw_audio_data = []
        self.frame_lookup = None
        self.frame_lookup_built = False
        
        # 🎬 Start fast frame processor
        self.frame_processor.start()
        
        self.chunk_uploader = S3ChunkUploader(
            bucket=AWS_S3_BUCKET,
            s3_key=self.s3_video_key,
            chunk_size_mb=5
        )
        self.chunk_uploader.start_chunk_monitor(self.temp_video_path)
        
        logger.info(f"🎬 Recording started with {self.target_fps} FPS FAST processing")
    
    def stop_recording(self):
        """Stop recording and finalize uploads"""
        self.is_recording = False
        
        # 🎬 Stop fast frame processor
        self.frame_processor.stop()
        
        with self.audio_lock:
            if hasattr(self, 'participant_audio_buffers'):
                for participant_id, participant_buffer in self.participant_audio_buffers.items():
                    if len(participant_buffer['buffer']) > 0:
                        buffer_data = participant_buffer['buffer'].copy()
                        self.raw_audio_data.append({
                            'timestamp': participant_buffer['buffer_start_time'],
                            'samples': buffer_data,
                            'participant': participant_id
                        })
                
                self.participant_audio_buffers = {}
            
            self.active_audio_tracks = {}
        
        logger.info("⏹️ Recording stopped")
    
    def add_video_frame(self, frame, source_type="video", timestamp_override=None):
        """Add video frame with HIGH-PRECISION timestamp"""

        if not self.is_recording:
            return

        if timestamp_override is not None:
            timestamp = timestamp_override
        else:
            timestamp = time.perf_counter() - self.start_perf_counter

        with self.frame_lock:

            class TimestampedFrame:
                def __init__(self, frame, timestamp, source_type="placeholder"):
                    self.frame = frame
                    self.timestamp = timestamp
                    self.source_type = source_type
                    self.capture_time = time.perf_counter()

            if frame is None:
                return

            timestamped_frame = TimestampedFrame(frame, timestamp, source_type)
            self.video_frames.append(timestamped_frame)

            if source_type in ["video", "screen_share"] and frame is not None:
                self.current_screen_frame = (
                    frame.copy()
                    if hasattr(self, "current_screen_frame")
                    else frame
                )

    def add_audio_samples(self, samples, participant_id="unknown", track_id=None, track_source=None):
        """Add audio samples with FIXED-SIZE buffering for smooth playback"""
        if not self.is_recording or not samples:
            return
        
        with self.audio_lock:
            if track_source:
                track_key = f"{participant_id}_{track_source}"
            else:
                track_key = f"{participant_id}_microphone"
            
            if track_id:
                if track_key in self.active_audio_tracks:
                    if self.active_audio_tracks[track_key] != track_id:
                        return
                else:
                    self.active_audio_tracks[track_key] = track_id
                    source_name = track_source or "microphone"
                    logger.info(f"✅ Using {source_name} audio track {track_id} for {participant_id}")
            
            timestamp = time.perf_counter() - self.start_perf_counter
            
            if track_key not in self.participant_audio_buffers:
                self.participant_audio_buffers[track_key] = {
                    'buffer': [],
                    'buffer_start_time': timestamp,
                    'participant': participant_id,
                    'source': track_source or 'microphone'
                }
            
            participant_buffer = self.participant_audio_buffers[track_key]
            
            if isinstance(samples, list):
                participant_buffer['buffer'].extend(samples)
            else:
                participant_buffer['buffer'].extend(samples.tolist() if hasattr(samples, 'tolist') else list(samples))
            
            buffer_size = len(participant_buffer['buffer'])
            
            if buffer_size >= self.AUDIO_BUFFER_SIZE:
                chunk_to_flush = participant_buffer['buffer'][:self.AUDIO_BUFFER_SIZE]
                
                self.raw_audio_data.append({
                    'timestamp': participant_buffer['buffer_start_time'],
                    'samples': chunk_to_flush,
                    'participant': participant_id,
                    'source': track_source or 'microphone'
                })
                
                participant_buffer['buffer'] = participant_buffer['buffer'][self.AUDIO_BUFFER_SIZE:]
                
                chunk_duration = self.AUDIO_BUFFER_SIZE / (48000 * 2)
                participant_buffer['buffer_start_time'] += chunk_duration
    
    def get_current_screen_frame(self):
        """Get current screen frame for placeholder generation"""
        with self.frame_lock:
            if hasattr(self, 'current_screen_frame'):
                return self.current_screen_frame.copy() if self.current_screen_frame is not None else None
            return None
    
    def create_placeholder_frame(self, frame_number, timestamp):
        """Create frame - use current screen OR placeholder text"""
        with self.frame_lock:
            if hasattr(self, 'current_screen_frame') and self.current_screen_frame is not None:
                if self.video_frames:
                    latest_screen_time = max([
                        f.timestamp for f in self.video_frames 
                        if f.source_type == "screen_share"
                    ], default=0)
                    
                    if timestamp - latest_screen_time < 5.0:
                        return self.current_screen_frame.copy()
        
        frame = np.zeros((720, 1280, 3), dtype=np.uint8)
        frame[:] = [20, 20, 20]
        
        cv2.putText(frame, "Waiting for screen share...", 
                    (320, 340), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (255, 255, 255), 2)
        
        cv2.putText(frame, f"Recording: {timestamp:.1f}s", 
                    (480, 400), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (180, 180, 180), 2)
        
        if int(timestamp * 2) % 2 == 0:
            cv2.circle(frame, (440, 395), 10, (0, 0, 255), -1)
        
        return frame

    def generate_synchronized_video(self):
        """Generate video with FIXED TARGET FPS for fast smooth playback"""
        
        if not self.video_frames and not self.raw_audio_data:
            logger.error("❌ No frames or audio recorded")
            return None, None
        
        # Calculate recording duration
        max_video_time = max([f.timestamp for f in self.video_frames]) if self.video_frames else 0
        max_audio_time = max([d['timestamp'] for d in self.raw_audio_data]) if self.raw_audio_data else 0
        recording_duration = max(max_video_time, max_audio_time, 1.0)
        
        # 🎬 CRITICAL: USE FIXED TARGET FPS for fast processing
        output_fps = self.target_fps  # Always use target FPS for fast smooth playback
        
        logger.info(f"🎬 Generating FAST smooth video: {recording_duration:.1f}s")
        logger.info(f"📊 Total captured frames: {len(self.video_frames)}")
        logger.info(f"📊 TARGET OUTPUT FPS: {output_fps} (FIXED for fast smooth playback)")
        logger.info(f"📊 Total audio chunks: {len(self.raw_audio_data)}")
        
        frame_interval = 1.0 / output_fps
        total_frames = int(recording_duration * output_fps)
        
        audio_s3_key = f"{self.s3_prefix}/raw_audio_{self.meeting_id}.wav"
        
        return self._generate_video_with_fast_encoding(
            total_frames, frame_interval, audio_s3_key, recording_duration, output_fps
        )
 
    def _generate_video_with_fast_encoding(self, total_frames, frame_interval, 
                                 audio_s3_key, recording_duration, output_fps):
        """Generate video with FAST ENCODING and FIXED FPS"""
        try:
            if not self.frame_lookup_built:
                self._build_optimized_frame_lookup()
            
            # Check GPU availability
            nvenc_available = False
            try:
                check_nvenc = subprocess.run(
                    ['ffmpeg', '-h', 'encoder=h264_nvenc'],
                    capture_output=True, text=True, timeout=5
                )
                nvenc_available = (check_nvenc.returncode == 0)
            except Exception:
                nvenc_available = False
            
            base_ffmpeg_cmd = [
                'ffmpeg', '-y',
                '-f', 'rawvideo',
                '-vcodec', 'rawvideo',
                '-pix_fmt', 'bgr24',
                '-s', '1280x720',
                '-r', str(output_fps),  # FIXED target FPS input
                '-i', '-'
            ]

            if nvenc_available:
                logger.info(f"🚀 GPU FAST ENCODING @ {output_fps} FPS (FIXED)")
                base_ffmpeg_cmd += [
                    '-c:v', 'h264_nvenc',
                    '-preset', 'p2',  # FASTEST preset for speed
                    '-tune', 'hq',
                    '-rc', 'vbr',
                    '-cq', '23',      # Balanced quality for speed
                    '-b:v', '4M',     # Reasonable bitrate
                    '-maxrate', '6M',
                    '-bufsize', '12M',
                    '-pix_fmt', 'yuv420p',
                    '-r', str(output_fps),  # FIXED output FPS
                    '-g', str(int(output_fps)),  # 1 second GOP for fast seeking
                    '-bf', '2',       # Fewer B-frames for speed
                    '-refs', '2',     # Fewer reference frames for speed
                    '-profile:v', 'high',
                    '-level', '4.1',
                    '-f', 'avi',
                    self.temp_video_path
                ]
            else:
                logger.info(f"⚙️ CPU FAST ENCODING @ {output_fps} FPS (FIXED)")
                base_ffmpeg_cmd += [
                    '-c:v', 'libx264',
                    '-preset', 'ultrafast',  # FASTEST CPU preset
                    '-crf', '25',           # Balanced quality for speed
                    '-pix_fmt', 'yuv420p',
                    '-r', str(output_fps),  # FIXED output FPS
                    '-g', str(int(output_fps)),  # 1 second GOP
                    '-bf', '0',             # No B-frames for speed
                    '-refs', '1',           # Minimal reference frames
                    '-tune', 'zerolatency', # Fast encoding tune
                    '-profile:v', 'high',
                    '-level', '4.1',
                    '-f', 'avi',
                    self.temp_video_path
                ]
            
            ffmpeg_env = os.environ.copy()
            ffmpeg_env['CUDA_VISIBLE_DEVICES'] = '0'
            ffmpeg_env['CUDA_DEVICE_ORDER'] = 'PCI_BUS_ID'
            ffmpeg_env.pop('NVIDIA_DISABLE', None)
            
            logger.info(f"🎯 Starting FAST FFmpeg encoding: {self.temp_video_path}")
            logger.info(f"📊 Expected frames: {total_frames} @ {output_fps} FPS")
            
            process = subprocess.Popen(
                base_ffmpeg_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=ffmpeg_env,
                bufsize=10485760
            )
            
            logger.info(f"🎞️ FFmpeg started (PID: {process.pid})")
            
            start_time = time.time()
            total_frames_written = 0
            last_log_time = start_time
            
            last_written_frame = None
            frame_duplicates = 0
            exact_matches = 0
            
            for frame_num in range(total_frames):
                target_timestamp = frame_num * frame_interval
                best_frame = self._find_best_frame_fast(target_timestamp, frame_interval)
                
                if best_frame is None:
                    if last_written_frame is not None:
                        # Use the last valid frame
                        best_frame = last_written_frame.copy()
                        frame_duplicates += 1
                        
                        # Add subtle aging effect for very old frames
                        if frame_duplicates > output_fps * 3:  # More than 3 seconds old
                            best_frame = cv2.addWeighted(
                                best_frame, 0.95,
                                cv2.GaussianBlur(best_frame, (3, 3), 0.5), 0.05, 0
                            )
                    else:
                        # Create placeholder
                        best_frame = self.create_placeholder_frame(frame_num, target_timestamp)
                else:
                    # Found a real frame - reset duplicate counter
                    exact_matches += 1
                    frame_duplicates = 0
                
                # Ensure proper frame size
                if best_frame.shape[:2] != (720, 1280):
                    best_frame = cv2.resize(best_frame, (1280, 720))
                
                try:
                    process.stdin.write(best_frame.tobytes())
                    total_frames_written += 1
                    last_written_frame = best_frame
                    
                    # Flush more frequently for stability
                    if frame_num % 30 == 0:  # Every 1.5 seconds at 20fps
                        try:
                            process.stdin.flush()
                        except Exception:
                            pass
                    
                    # Enhanced progress logging
                    now = time.time()
                    if now - last_log_time >= 2:
                        elapsed = now - start_time
                        fps = total_frames_written / elapsed if elapsed > 0 else 0
                        progress = (frame_num / total_frames) * 100
                        eta = (total_frames - frame_num) / fps / 60 if fps > 0 else 0
                        
                        logger.info(
                            f"🎬 SMOOTH Progress: {progress:.1f}% | Speed: {fps:.0f} fps | "
                            f"ETA: {eta:.1f} min | Real: {exact_matches} | Dupe: {frame_duplicates} | "
                            f"S3: {self.chunk_uploader.total_uploaded / (1024*1024):.1f}MB"
                        )
                        last_log_time = now
                
                except (BrokenPipeError, IOError) as e:
                    logger.error(f"❌ Pipe error at frame {frame_num}: {e}")
                    break
            
            logger.info(f"✅ AGGRESSIVE interpolation: {total_frames_written} frames @ {output_fps} FPS")
            logger.info("🔚 Closing FFmpeg stdin...")
            
            try:
                process.stdin.close()
                logger.info("⏳ Waiting for FFmpeg to finish fast encoding...")
                return_code = process.wait(timeout=60)  # Shorter timeout for fast encoding
                
                if return_code != 0:
                    stderr_output = process.stderr.read().decode('utf-8', errors='ignore')
                    logger.error(f"❌ FFmpeg exited with code {return_code}")
                    logger.error(f"FFmpeg stderr: {stderr_output[-1000:]}")
                else:
                    logger.info("✅ FFmpeg FAST encoding completed successfully")
                    
            except subprocess.TimeoutExpired:
                logger.warning("⚠️ FFmpeg timeout - killing process")
                process.kill()
                process.wait()
            except Exception as e:
                logger.warning(f"FFmpeg close warning: {e}")
            
            # Verify local file
            if not os.path.exists(self.temp_video_path):
                logger.error(f"❌ Local video file not created: {self.temp_video_path}")
                return None, None
            
            local_file_size = os.path.getsize(self.temp_video_path)
            if local_file_size == 0:
                logger.error(f"❌ Local video file is empty")
                return None, None
            
            logger.info(f"✅ FAST video file: {local_file_size:,} bytes @ {output_fps} FPS")
            
            logger.info("⏳ Finalizing S3 uploads...")
            self.chunk_uploader.stop_and_upload_final(self.temp_video_path)
            
            # Verify S3 upload
            try:
                response = s3_client.head_object(Bucket=AWS_S3_BUCKET, Key=self.s3_video_key)
                video_size = response['ContentLength']
                logger.info(f"✅ FAST Video in S3: {video_size:,} bytes @ {output_fps} FPS")
            except Exception as e:
                logger.error(f"❌ S3 verification failed: {e}")
                return None, None
            
            # Clean up local temp file
            try:
                os.remove(self.temp_video_path)
                logger.info(f"🧹 Temp file deleted: {self.temp_video_path}")
            except Exception as e:
                logger.warning(f"⚠️ Could not delete temp file: {e}")
            
            # Generate audio
            self._generate_smooth_audio_to_s3(audio_s3_key, recording_duration)
            
            return self.s3_video_key, audio_s3_key
        
        except Exception as e:
            logger.error(f"❌ FAST video generation failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            try:
                if 'process' in locals() and process.poll() is None:
                    process.kill()
                    stderr_output = process.stderr.read().decode('utf-8', errors='ignore')
                    logger.error(f"FFmpeg stderr: {stderr_output[-1000:]}")
            except Exception:
                pass
            
            return None, None
            
    def _build_optimized_frame_lookup(self):
        """Build frame lookup with proper indexing"""
        logger.info("🔨 Building frame lookup for FAST playback...")
        start_build = time.time()
        
        sorted_frames = sorted(self.video_frames, key=lambda f: f.timestamp)
        
        if len(sorted_frames) == 0:
            logger.warning("⚠️ WARNING: No video frames found!")
            self.frame_lookup = {}
            self.frame_lookup_built = True
            return
        
        self.frame_lookup = {}
        self.sorted_frame_list = []
        
        for frame_obj in sorted_frames:
            if frame_obj.source_type in ["video", "screen_share", "fast_duplicate"]:
                # Use high-precision indexing for fast lookup
                frame_key = int(frame_obj.timestamp * 1000)  # 0.001s precision
                
                # Keep the latest frame at each timestamp
                self.frame_lookup[frame_key] = frame_obj.frame
                
                self.sorted_frame_list.append({
                    'timestamp': frame_obj.timestamp,
                    'frame': frame_obj.frame,
                    'frame_key': frame_key,
                    'source_type': frame_obj.source_type
                })
        
        build_time = time.time() - start_build
        total_duration = sorted_frames[-1].timestamp if sorted_frames else 0
        
        logger.info(f"✅ FAST frame index: {len(self.frame_lookup)} frames in {build_time:.2f}s")
        logger.info(f"📊 Total duration: {total_duration:.1f}s")
        
        self.frame_lookup_built = True
 
    def _find_best_frame_fast(self, target_timestamp, frame_interval):
        """AGGRESSIVE frame finding - always return something for smooth playback"""
        if not self.frame_lookup_built or len(self.sorted_frame_list) == 0:
            return None
        
        # High-precision key matching first
        frame_key = int(target_timestamp * 1000)
        
        # Direct match
        if frame_key in self.frame_lookup:
            return self.frame_lookup[frame_key]
        
        # AGGRESSIVE search - much wider tolerance for smooth playback
        search_range = int(frame_interval * 1000 * 20)  # 20x frame interval tolerance
        for offset in range(1, search_range):
            if frame_key - offset in self.frame_lookup:
                return self.frame_lookup[frame_key - offset]
            if frame_key + offset in self.frame_lookup:
                return self.frame_lookup[frame_key + offset]
        
        # Find closest frame - ALWAYS return something
        if len(self.sorted_frame_list) > 0:
            closest_frame = min(
                self.sorted_frame_list,
                key=lambda f: abs(f['timestamp'] - target_timestamp)
            )
            
            # VERY generous acceptance - accept any frame within 10 seconds
            if abs(closest_frame['timestamp'] - target_timestamp) < 10.0:
                return closest_frame['frame']
        
        # LAST RESORT: Return the latest frame we have
        if len(self.sorted_frame_list) > 0:
            return self.sorted_frame_list[-1]['frame']
        
        return None

    def _generate_smooth_audio_to_s3(self, audio_s3_key, duration):
        """Generate audio and upload to S3"""
        try:
            sample_rate = 48000
            total_samples = int(duration * sample_rate * 2)
            
            if not self.raw_audio_data or len(self.raw_audio_data) == 0:
                logger.warning("No audio data available, creating silent audio in S3")
                self._create_silent_audio_s3(audio_s3_key, duration)
                return
            
            final_audio = np.zeros(total_samples, dtype=np.float64)
            sample_count = np.zeros(total_samples, dtype=np.int32)
            
            logger.info(f"Processing {len(self.raw_audio_data)} audio chunks with sub-sample precision")
            
            sorted_audio = sorted(self.raw_audio_data, key=lambda x: x['timestamp'])
            
            successful_chunks = 0
            skipped_chunks = 0
            participants_detected = set()
            audio_sources = {'microphone': 0, 'screen_share_audio': 0}
            
            for audio_chunk in sorted_audio:
                timestamp = audio_chunk['timestamp']
                samples = audio_chunk['samples']
                participant = audio_chunk.get('participant', 'unknown')
                source = audio_chunk.get('source', 'microphone')
                
                participants_detected.add(participant)
                audio_sources[source] = audio_sources.get(source, 0) + 1
                
                if not samples or len(samples) == 0:
                    skipped_chunks += 1
                    continue
                
                try:
                    if isinstance(samples, list):
                        audio_data = np.array(samples, dtype=np.float64)
                    else:
                        audio_data = samples.astype(np.float64)
                    
                    if len(audio_data) == 0:
                        skipped_chunks += 1
                        continue
                    
                    if len(audio_data) % 2 != 0:
                        audio_data = np.append(audio_data, 0)
                    
                    start_sample_float = timestamp * sample_rate * 2
                    start_sample = int(start_sample_float)
                    sub_sample_offset = start_sample_float - start_sample
                    
                    if start_sample >= total_samples:
                        skipped_chunks += 1
                        continue
                    
                    end_sample = min(start_sample + len(audio_data), total_samples)
                    audio_length = end_sample - start_sample
                    
                    if audio_length > 0:
                        if sub_sample_offset > 0.01:
                            interpolated = audio_data[:audio_length].copy()
                            if audio_length > 1:
                                interpolated[1:] = (1 - sub_sample_offset) * audio_data[:audio_length-1] + \
                                                sub_sample_offset * audio_data[1:audio_length]
                            final_audio[start_sample:end_sample] += interpolated
                        else:
                            final_audio[start_sample:end_sample] += audio_data[:audio_length]
                        
                        sample_count[start_sample:end_sample] += 1
                        successful_chunks += 1
                    else:
                        skipped_chunks += 1
                        
                except Exception as chunk_error:
                    logger.debug(f"Skipping audio chunk: {chunk_error}")
                    skipped_chunks += 1
                    continue
            
            logger.info(f"Audio: {successful_chunks} chunks processed, {skipped_chunks} skipped")
            logger.info(f"👥 Participants: {len(participants_detected)}")
            logger.info(f"🎤 Sources: {audio_sources['microphone']} mic, {audio_sources.get('screen_share_audio', 0)} screen")
            
            max_amplitude_before = np.max(np.abs(final_audio))
            if max_amplitude_before == 0:
                logger.warning("No audio signal detected")
                self._create_silent_audio_s3(audio_s3_key, duration)
                return
            
            overlap_mask = sample_count > 1
            if np.any(overlap_mask):
                final_audio[overlap_mask] = final_audio[overlap_mask] / np.sqrt(sample_count[overlap_mask])
                max_overlap = np.max(sample_count)
                overlap_percentage = (np.sum(overlap_mask) / len(final_audio)) * 100
                logger.info(f"🎵 Audio mixing: {max_overlap} max speakers, {overlap_percentage:.1f}% overlap")
            
            max_amplitude_after = np.max(np.abs(final_audio))
            target_amplitude = 18000.0
            
            if max_amplitude_after > 28000:
                threshold = 20000.0
                ratio = 0.7
                mask_above = np.abs(final_audio) > threshold
                final_audio[mask_above] = np.sign(final_audio[mask_above]) * (
                    threshold + (np.abs(final_audio[mask_above]) - threshold) * ratio
                )
                logger.info(f"🔊 AGC: Soft-knee compression applied")
            elif max_amplitude_after < 8000:
                boost_ratio = target_amplitude / max_amplitude_after
                final_audio = final_audio * boost_ratio
                logger.info(f"🔊 AGC: Boosted {max_amplitude_after:.0f} → {target_amplitude:.0f}")
            elif max_amplitude_after > 20000:
                compression_ratio = 18000.0 / max_amplitude_after
                final_audio = final_audio * compression_ratio
                logger.info(f"🔊 AGC: Gentle compression")
            else:
                logger.info(f"🔊 AGC: Optimal range ({max_amplitude_after:.0f})")
            
            final_audio_int16 = np.clip(final_audio, -32768, 32767).astype(np.int16)
            
            clipped_samples = np.sum((final_audio < -32768) | (final_audio > 32767))
            if clipped_samples > 0:
                clipped_percentage = (clipped_samples / len(final_audio)) * 100
                if clipped_percentage > 0.1:
                    logger.warning(f"⚠️ Audio clipping: {clipped_percentage:.3f}%")
                else:
                    logger.info(f"✅ Minimal clipping: {clipped_percentage:.3f}%")
            else:
                logger.info(f"✅ Perfect audio - no clipping")
            
            wav_buffer = io.BytesIO()
            with wave.open(wav_buffer, 'wb') as wav_file:
                wav_file.setnchannels(2)
                wav_file.setsampwidth(2)
                wav_file.setframerate(sample_rate)
                wav_file.writeframes(final_audio_int16.tobytes())
            
            wav_buffer.seek(0)
            audio_bytes = wav_buffer.read()
            
            s3_client.put_object(
                Bucket=AWS_S3_BUCKET,
                Key=audio_s3_key,
                Body=audio_bytes,
                ContentType='audio/wav'
            )
            
            audio_duration = len(final_audio_int16) / (sample_rate * 2)
            file_size = len(audio_bytes)
            final_max = np.max(np.abs(final_audio_int16))
            logger.info(f"✅ Audio uploaded to S3: {audio_duration:.1f}s, {file_size:,} bytes, amplitude: {final_max:.0f}")
            
        except Exception as e:
            logger.error(f"Error generating audio: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self._create_silent_audio_s3(audio_s3_key, duration)

    def _create_silent_audio_s3(self, audio_s3_key, duration):
        try:
            sample_rate = 48000
            total_samples = int(duration * sample_rate)
            
            silent_audio = np.zeros(total_samples * 2, dtype=np.int16)
            
            wav_buffer = io.BytesIO()
            with wave.open(wav_buffer, 'wb') as wav_file:
                wav_file.setnchannels(2)
                wav_file.setsampwidth(2)
                wav_file.setframerate(sample_rate)
                wav_file.writeframes(silent_audio.tobytes())
            
            wav_buffer.seek(0)
            audio_bytes = wav_buffer.read()
            
            s3_client.put_object(
                Bucket=AWS_S3_BUCKET,
                Key=audio_s3_key,
                Body=audio_bytes,
                ContentType='audio/wav'
            )
            
            logger.info(f"Created silent audio in S3: {duration:.1f}s - {audio_s3_key}")
            
        except Exception as e:
            logger.error(f"Error creating silent audio: {e}")

class TimestampedFrame:
    """Frame with HIGH-PRECISION timestamp for proper synchronization"""
    def __init__(self, frame, timestamp, source_type="placeholder"):
        self.frame = frame
        self.timestamp = timestamp
        self.source_type = source_type
        self.capture_time = time.perf_counter()
     
class FixedRecordingBot:
    """Fixed recording bot with FAST FRAME DUPLICATION"""
    
    def __init__(self, room_url: str, token: str, room_name: str, meeting_id: str,
                 result_queue: queue.Queue, stop_event: threading.Event, target_fps: int = 20):
        
        self.room_url = room_url
        self.token = token
        self.room_name = room_name
        self.meeting_id = meeting_id
        self.result_queue = result_queue
        self.stop_event = stop_event
        self.target_fps = target_fps
        
        self.room = None
        self.is_connected = False
        
        # 🎬 FAST recording with target FPS
        self.stream_recorder = StreamingRecordingWithChunks(meeting_id, target_fps)
        
        self.active_video_streams = {}
        self.active_audio_streams = {}
        
        logger.info(f"✅ FAST Recording Bot - Target: {target_fps} FPS for meeting: {meeting_id}")

    async def run_recording(self):
        """Main recording method with FAST playback support"""
        try:
            self.room = rtc.Room()
            self.room.on("track_subscribed", self._on_track_subscribed)
            self.room.on("track_unsubscribed", self._on_track_unsubscribed)
            self.room.on("connected", self._on_connected)
            self.room.on("disconnected", self._on_disconnected)
            
            logger.info(f"🔗 Attempting WSS connection to: {self.room_url}")
            
            try:
                await asyncio.wait_for(
                    self.room.connect(self.room_url, self.token),
                    timeout=60.0
                )
                logger.info("✅ Connected via WSS successfully")
                
            except Exception as e:
                logger.error(f"❌ WSS connection failed: {e}")
                logger.info("🔄 Trying direct HTTP fallback...")
                
                http_url = "ws://192.168.48.201:8880"
                try:
                    await asyncio.wait_for(
                        self.room.connect(http_url, self.token),
                        timeout=30.0
                    )
                    logger.info("✅ Connected via HTTP fallback successfully")
                except Exception as fallback_error:
                    logger.error(f"❌ HTTP fallback also failed: {fallback_error}")
                    raise Exception("Both WSS and HTTP connections failed")
            
            logger.info("✅ Room connection established")
            self.result_queue.put_nowait((True, None))
            
            await self._start_fast_recording()
            
        except Exception as e:
            logger.error(f"❌ Recording error: {e}")
            try:
                self.result_queue.put_nowait((False, str(e)))
            except:
                pass
        finally:
            await self._finalize()

    async def _start_fast_recording(self):
        """Start FAST continuous recording"""
        logger.info(f"🎬 Starting FAST recording @ {self.target_fps} FPS")
        
        self.stream_recorder.start_recording()
        
        # Start placeholder generation for fast video
        asyncio.create_task(self._fast_placeholder_loop())
        
        while not self.stop_event.is_set():
            await asyncio.sleep(0.1)
        
        self.stream_recorder.stop_recording()
        
        logger.info("FAST recording completed - generating output")

    async def _fast_placeholder_loop(self):
        """AGGRESSIVE placeholder generation to ensure smooth frame rate"""
        PLACEHOLDER_INTERVAL = 1.0 / self.target_fps
        
        next_frame_time = time.perf_counter()
        last_placeholder_time = 0
        placeholder_count = 0
        
        logger.info(f"🎬 AGGRESSIVE placeholder loop started - Target: {self.target_fps} FPS")
        
        while not self.stop_event.is_set():
            current_time = time.perf_counter()
            
            # ALWAYS generate frames at target FPS rate
            if current_time >= next_frame_time:
                timestamp = current_time - self.stream_recorder.start_perf_counter
                
                # Check if we have recent screen content
                current_screen_frame = self.stream_recorder.get_current_screen_frame()
                has_recent_screen = False
                
                with self.stream_recorder.frame_lock:
                    if self.stream_recorder.video_frames:
                        latest_screen_time = max(
                            (f.timestamp for f in self.stream_recorder.video_frames 
                            if f.source_type in ["screen_share", "smooth_interpolated"]),
                            default=0
                        )
                        has_recent_screen = (timestamp - latest_screen_time) < 0.5  # 500ms tolerance
                
                # Always create a frame if we don't have very recent content
                if not has_recent_screen:
                    if current_screen_frame is not None:
                        # Use current screen frame with aging effect
                        age = timestamp - last_placeholder_time
                        placeholder = self._create_aged_screen_frame(current_screen_frame, age)
                        frame_type = "aged_screen"
                    else:
                        # Create waiting placeholder
                        placeholder = self.stream_recorder.create_placeholder_frame(placeholder_count, timestamp)
                        frame_type = "waiting_placeholder"
                    
                    self.stream_recorder.add_video_frame(
                        placeholder, frame_type, timestamp_override=timestamp
                    )
                    placeholder_count += 1
                    last_placeholder_time = timestamp
                
                next_frame_time = current_time + PLACEHOLDER_INTERVAL
            
            # Sleep for a fraction of the frame interval
            await asyncio.sleep(PLACEHOLDER_INTERVAL / 4)
        
        logger.info(f"✅ AGGRESSIVE placeholder loop stopped. Generated: {placeholder_count} placeholders")
    
    def _create_aged_screen_frame(self, screen_frame, age_seconds):
        """Create screen frame with aging effect to show staleness"""
        try:
            if age_seconds < 1.0:
                return screen_frame.copy()
            
            # Apply aging effects
            aged_frame = screen_frame.copy()
            
            # Slight darkening for older frames
            if age_seconds > 2.0:
                darken_factor = min(0.15, age_seconds * 0.02)
                aged_frame = cv2.convertScaleAbs(aged_frame, alpha=(1 - darken_factor), beta=0)
            
            # Add subtle blur for very old frames
            if age_seconds > 5.0:
                blur_amount = min(3, int(age_seconds / 3))
                if blur_amount > 0:
                    aged_frame = cv2.GaussianBlur(aged_frame, (blur_amount * 2 + 1, blur_amount * 2 + 1), 0)
            
            # Optional: Add age indicator
            if age_seconds > 3.0:
                cv2.putText(aged_frame, f"Screen: {age_seconds:.1f}s ago", 
                           (10, aged_frame.shape[0] - 20), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.6, (100, 100, 100), 2)
            
            return aged_frame
        except:
            return screen_frame.copy()

    def _on_track_subscribed(self, track, publication, participant):
        """Handle new track subscription with FAST processing"""
        try:
            if track.sid in self.stream_recorder.processing_tracks:
                logger.debug(f"⏩ Already processing track {track.sid}, skipping")
                return
            
            self.stream_recorder.processing_tracks.add(track.sid)
            
            if track.kind == rtc.TrackKind.KIND_VIDEO:
                is_screen_share = False
                
                if hasattr(track, 'name'):
                    track_name_lower = track.name.lower()
                    if any(keyword in track_name_lower for keyword in ['screen', 'display', 'desktop', 'share']):
                        is_screen_share = True
                        logger.info(f"✅ Detected screen share via name: {track.name}")
                
                if not is_screen_share and hasattr(publication, 'name'):
                    pub_name_lower = publication.name.lower()
                    if any(keyword in pub_name_lower for keyword in ['screen', 'display', 'desktop', 'share']):
                        is_screen_share = True
                        logger.info(f"✅ Detected screen share via publication name: {publication.name}")
                
                if not is_screen_share:
                    try:
                        if hasattr(track, 'source'):
                            source_str = str(track.source).lower()
                            if 'camera' not in source_str:
                                is_screen_share = True
                                logger.info(f"✅ Detected screen share - source: {source_str}")
                    except Exception as e:
                        logger.debug(f"Source check failed: {e}")
                
                if not is_screen_share:
                    logger.warning(f"⛔ REJECTING camera/unknown video from {participant.identity}")
                    self.stream_recorder.processing_tracks.discard(track.sid)
                    return
                
                existing_screen_count = sum(
                    1 for k in self.active_video_streams.keys() 
                    if participant.identity in k
                )
                
                if existing_screen_count >= 1:
                    logger.debug(f"⏩ Participant {participant.identity} already has screen share track")
                    self.stream_recorder.processing_tracks.discard(track.sid)
                    return
                
                task = asyncio.create_task(self._capture_video_stream_fast(track, participant))
                self.active_video_streams[f"screen_{participant.identity}_{track.sid}"] = task
                logger.info(f"✅ Started FAST SCREEN capture from {participant.identity} (track: {track.sid})")
                
            elif track.kind == rtc.TrackKind.KIND_AUDIO:
                track_source = "microphone"
                
                try:
                    if hasattr(track, 'name'):
                        track_name_lower = track.name.lower()
                        if any(keyword in track_name_lower for keyword in ['screen', 'desktop', 'system', 'share']):
                            track_source = "screen_share_audio"
                    
                    if track_source == "microphone" and hasattr(track, 'source'):
                        source_str = str(track.source).lower()
                        if any(keyword in source_str for keyword in ['screen', 'desktop', 'system']):
                            track_source = "screen_share_audio"
                except Exception as e:
                    logger.debug(f"Audio source detection warning: {e}")
                
                track_type_prefix = f"audio_{participant.identity}_{track_source}_"
                existing_audio_count = sum(
                    1 for k in self.active_audio_streams.keys() 
                    if k.startswith(track_type_prefix)
                )
                
                if existing_audio_count >= 1:
                    logger.debug(f"⏩ Participant {participant.identity} already has {track_source} track")
                    self.stream_recorder.processing_tracks.discard(track.sid)
                    return
                
                task = asyncio.create_task(
                    self._capture_audio_stream(track, participant, track_source)
                )
                self.active_audio_streams[f"{track_type_prefix}{track.sid}"] = task
                logger.info(f"✅ Started {track_source} capture from {participant.identity} (track: {track.sid})")
                
        except Exception as e:
            logger.error(f"❌ Track subscription error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.stream_recorder.processing_tracks.discard(track.sid)

    def _on_track_unsubscribed(self, track, publication, participant):
        """Handle track unsubscription with cleanup"""
        try:
            self.stream_recorder.processing_tracks.discard(track.sid)
            
            if track.kind == rtc.TrackKind.KIND_VIDEO:
                for key in list(self.active_video_streams.keys()):
                    if track.sid in key:
                        self.active_video_streams[key].cancel()
                        del self.active_video_streams[key]
                        logger.info(f"Stopped video capture from {participant.identity}")
                        break
                    
            elif track.kind == rtc.TrackKind.KIND_AUDIO:
                for key in list(self.active_audio_streams.keys()):
                    if track.sid in key:
                        self.active_audio_streams[key].cancel()
                        del self.active_audio_streams[key]
                        logger.info(f"Stopped audio capture from {participant.identity}")
                        break
                    
        except Exception as e:
            logger.error(f"Track unsubscription error: {e}")

    async def _capture_video_stream_fast(self, track, participant):
        """Capture with FAST frame processing for target FPS"""
        try:
            stream = rtc.VideoStream(track, capacity=240)
            
            frame_count = 0
            start_time = time.perf_counter()
            last_log_time = start_time

            logger.info(f"📺 Starting FAST capture from {participant.identity} @ {self.target_fps} FPS target")

            async for frame_event in stream:
                if self.stop_event.is_set():
                    break

                current_time = time.perf_counter()

                lf = frame_event.frame if hasattr(frame_event, "frame") else frame_event
                
                if lf is None:
                    continue

                # Get timestamp
                try:
                    timestamp = frame_event.timestamp / 1e9
                except:
                    timestamp = current_time - self.stream_recorder.start_perf_counter

                # ✅ Queue RAW frame for FAST processing
                self.stream_recorder.frame_processor.queue_raw_frame(
                    livekit_frame=lf,
                    timestamp=timestamp,
                    source_type="screen_share"
                )
                frame_count += 1

                # Log every 5 seconds
                if current_time - last_log_time >= 5.0:
                    elapsed = current_time - start_time
                    actual_fps = frame_count / elapsed if elapsed > 0 else 0
                    queue_size = len(self.stream_recorder.frame_processor.raw_frame_queue)
                    
                    logger.info(
                        f"📺 FAST: {frame_count} frames in {elapsed:.1f}s = {actual_fps:.1f} FPS capture, "
                        f"Target: {self.target_fps} FPS, Queue: {queue_size}"
                    )
                    last_log_time = current_time

        except Exception as e:
            logger.error(f"❌ FAST video capture error: {e}")
     
    async def _capture_audio_stream(self, track, participant, track_source="microphone"):
        """Capture audio stream with proper source detection"""
        try:
            stream = rtc.AudioStream(track)
            sample_count = 0
            
            logger.info(f"Starting {track_source} capture from {participant.identity}")
            
            async for frame_event in stream:
                if self.stop_event.is_set():
                    break
                
                frame = frame_event.frame if hasattr(frame_event, 'frame') else frame_event
                
                if frame:
                    samples = self._convert_frame_to_audio_simple(frame)
                    
                    if samples:
                        self.stream_recorder.add_audio_samples(
                            samples, 
                            participant.identity,
                            track.sid,
                            track_source
                        )
                        sample_count += len(samples)
                        
                        if sample_count % 48000 == 0:
                            logger.info(f"Captured {sample_count} {track_source} samples from {participant.identity}")
            
            logger.info(f"Audio capture completed: {sample_count} {track_source} samples from {participant.identity}")
            
            self.stream_recorder.processing_tracks.discard(track.sid)
            
        except Exception as e:
            logger.error(f"Audio capture error: {e}")
            self.stream_recorder.processing_tracks.discard(track.sid)

    def _convert_frame_to_audio_simple(self, frame):
        """Convert LiveKit audio frame to samples with proper format detection"""
        try:
            if not frame or not hasattr(frame, 'data') or not frame.data:
                return None
            
            sample_rate = getattr(frame, 'sample_rate', 48000)
            num_channels = getattr(frame, 'num_channels', 1)
            samples_per_channel = getattr(frame, 'samples_per_channel', 0)
            
            if not hasattr(self, '_logged_audio_format'):
                logger.info(f"🎵 Audio: {sample_rate}Hz, {num_channels}ch, {samples_per_channel} samples/ch")
                self._logged_audio_format = True
            
            try:
                audio_array = np.frombuffer(frame.data, dtype=np.int16)
                
                if len(audio_array) == 0:
                    return None
                
                if num_channels == 1:
                    stereo_audio = np.repeat(audio_array, 2)
                    return stereo_audio.tolist()
                elif num_channels == 2:
                    return audio_array.tolist()
                else:
                    reshaped = audio_array.reshape(-1, num_channels)
                    stereo_audio = reshaped[:, :2].flatten()
                    return stereo_audio.tolist()
                
            except:
                try:
                    audio_array = np.frombuffer(frame.data, dtype=np.float32)
                    audio_array = np.clip(audio_array, -1.0, 1.0)
                    audio_array = (audio_array * 32767.0).astype(np.int16)
                    
                    if len(audio_array) == 0:
                        return None
                    
                    if num_channels == 1:
                        stereo_audio = np.repeat(audio_array, 2)
                        return stereo_audio.tolist()
                    elif num_channels == 2:
                        return audio_array.tolist()
                    else:
                        reshaped = audio_array.reshape(-1, num_channels)
                        stereo_audio = reshaped[:, :2].flatten()
                        return stereo_audio.tolist()
                    
                except:
                    return None
            
        except Exception as e:
            logger.debug(f"Audio conversion error: {e}")
            return None

    def _on_connected(self):
        """Handle room connection"""
        logger.info("✅ Connected to room")
        self.is_connected = True

    def _on_disconnected(self, reason):
        """Handle room disconnection"""
        logger.warning(f"⚠️ Room disconnected: {reason}")

    async def _finalize(self):
        """Finalize recording and generate FAST output"""
        try:
            logger.info("Finalizing FAST recording...")
            
            for task in list(self.active_video_streams.values()):
                task.cancel()
            for task in list(self.active_audio_streams.values()):
                task.cancel()
            
            await asyncio.sleep(1.0)
            
            # Generate FAST video
            video_path, audio_path = self.stream_recorder.generate_synchronized_video()
            
            self.final_video_path = video_path
            self.final_audio_path = audio_path
            
            if self.room and self.is_connected:
                try:
                    await asyncio.wait_for(self.room.disconnect(), timeout=30.0)
                except:
                    pass
            
            logger.info("FAST recording finalized successfully")
            
        except Exception as e:
            logger.error(f"Finalization error: {e}")

class FixedGoogleMeetRecorder:
    """Fixed Google Meet style recorder with FAST PLAYBACK"""
    
    def __init__(self):
        # CORRECTED: Use HTTPS URL for API calls, WSS for WebSocket
        self.livekit_url = os.getenv("LIVEKIT_URL", "https://192.168.48.201:8881")
        self.livekit_wss_url = os.getenv("LIVEKIT_WSS_URL", "wss://192.168.48.201:8881")
        
        # Get API credentials from environment
        self.api_key = os.getenv("LIVEKIT_API_KEY", "sridhar_ec9969265170a7d374da49d6b55f8ff4")
        self.api_secret = os.getenv("LIVEKIT_API_SECRET", "409150d1e2f40c1ebfcdd414c9c7b25c662d3770c08c1a6a945db8209ebfff3c")
        
        # 🎬 FAST VIDEO SETTINGS
        self.target_fps = int(os.getenv("FAST_VIDEO_FPS", "20"))  # Configurable target FPS
        
        # 🎥 ADVANCED SMOOTHING (optional - slower but smoother)
        # Set USE_ADVANCED_SMOOTHING=true for better interpolation at cost of longer processing time
        # Default: false (uses fast fps conversion)
        # Advanced: true (uses minterpolate with blend mode for smoother motion)
        
        logger.info(f"🌐 LiveKit HTTPS URL: {self.livekit_url}")
        logger.info(f"🔌 LiveKit WSS URL: {self.livekit_wss_url}")
        logger.info(f"🎬 FAST Video Target FPS: {self.target_fps}")
        logger.info(f"🔑 API Key: {self.api_key}")
        
        mongo_uri = os.getenv("MONGO_URI", "mongodb://connectly:LT%40connect25@192.168.48.201:27017/connectlydb?authSource=admin")
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[os.getenv("MONGO_DB", "connectlydb")]
        self.collection = self.db["test"]
        
        self.s3_recordings_prefix = S3_FOLDERS['recordings_temp']
        
        self.active_recordings = {}
        self._global_lock = threading.RLock()
        
        self.thread_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="FastRecorder")
        
        logger.info(f"✅ FAST Google Meet Style Recorder initialized")

    def generate_recorder_token(self, room_name: str, recorder_identity: str) -> str:
        """Generate JWT token for recording bot - ONLY screen share and microphone"""
        try:
            now = int(time.time())
            payload = {
                'iss': self.api_key,
                'sub': recorder_identity,
                'iat': now,
                'nbf': now,
                'exp': now + 7200,
                'video': {
                    'room': room_name,
                    'roomJoin': True,
                    'roomList': True,
                    'roomAdmin': True,
                    'roomCreate': False,
                    'roomRecord': True,
                    'canPublish': False,
                    'canSubscribe': True,
                    'canPublishData': False,
                    'canUpdateOwnMetadata': True,
                    'canPublishSources': [],
                    'canSubscribeSources': ['microphone', 'screen_share', 'screen_share_audio'],
                    'hidden': True,
                    'recorder': True
                }
            }
            
            token = jwt.encode(payload, self.api_secret, algorithm='HS256')
            logger.info(f"✅ Generated recorder token (FAST mode) for room: {room_name}")
            return token
            
        except Exception as e:
            logger.error(f"❌ Token generation failed: {e}")
            raise
            
    def start_stream_recording(self, meeting_id: str, host_user_id: str, room_name: str = None) -> Dict:
        """Start FAST Google Meet style recording"""
        if not room_name:
            room_name = f"meeting_{meeting_id}"
        
        with self._global_lock:
            if meeting_id in self.active_recordings:
                return {
                    "status": "already_active",
                    "message": "Recording already in progress",
                    "meeting_id": meeting_id
                }
        
        try:
            timestamp = int(time.time())
            recording_metadata = {
                "meeting_id": meeting_id,
                "host_user_id": host_user_id,
                "room_name": room_name,
                "recording_status": "starting",
                "recording_type": "fast_google_meet",
                "target_fps": self.target_fps,
                "start_time": datetime.now(),
                "created_at": datetime.now()
            }
            
            result = self.collection.insert_one(recording_metadata)
            recording_doc_id = str(result.inserted_id)
            
            recorder_identity = f"fast_recorder_{meeting_id}_{timestamp}"
            
            success, error_msg = self._start_fast_recording(
                room_name, meeting_id, host_user_id, recording_doc_id, recorder_identity
            )
            
            if success:
                self.collection.update_one(
                    {"_id": result.inserted_id},
                    {"$set": {"recording_status": "active", "recorder_identity": recorder_identity}}
                )
                
                return {
                    "status": "success",
                    "message": f"FAST recording started @ {self.target_fps} FPS",
                    "meeting_id": meeting_id,
                    "recording_id": recording_doc_id,
                    "recorder_identity": recorder_identity,
                    "target_fps": self.target_fps
                }
            else:
                self.collection.update_one(
                    {"_id": result.inserted_id},
                    {"$set": {"recording_status": "failed", "error": error_msg}}
                )
                return {
                    "status": "error",
                    "message": error_msg,
                    "meeting_id": meeting_id
                }
                
        except Exception as e:
            logger.error(f"❌ Error starting FAST recording: {e}")
            return {
                "status": "error",
                "message": f"FAST recording start failed: {str(e)}",
                "meeting_id": meeting_id
            }

    def _start_fast_recording(self, room_name: str, meeting_id: str, host_user_id: str,
                               recording_doc_id: str, recorder_identity: str) -> Tuple[bool, Optional[str]]:
        """Start FAST recording process"""
        try:
            recorder_token = self.generate_recorder_token(room_name, recorder_identity)
            
            result_queue = queue.Queue()
            stop_event = threading.Event()
            
            future = self.thread_pool.submit(
                self._run_fast_recording_task,
                self.livekit_wss_url, recorder_token, room_name, meeting_id,
                result_queue, stop_event, self.target_fps
            )
            
            try:
                success, error_msg = result_queue.get(timeout=60)
                
                if success:
                    with self._global_lock:
                        self.active_recordings[meeting_id] = {
                            "room_name": room_name,
                            "recording_doc_id": recording_doc_id,
                            "recorder_identity": recorder_identity,
                            "start_time": datetime.now(),
                            "host_user_id": host_user_id,
                            "stop_event": stop_event,
                            "recording_future": future,
                            "target_fps": self.target_fps
                        }
                    
                    return True, None
                else:
                    stop_event.set()
                    return False, error_msg
                    
            except queue.Empty:
                stop_event.set()
                return False, "FAST recording connection timeout"
                
        except Exception as e:
            logger.error(f"❌ Error starting FAST recording: {e}")
            return False, str(e)

    def _run_fast_recording_task(self, room_url: str, token: str, room_name: str,
                                  meeting_id: str, result_queue: queue.Queue, 
                                  stop_event: threading.Event, target_fps: int):
        """Run FAST recording task"""
        identifier = f"fast_recording_{meeting_id}"
        loop = None
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop_manager.register_loop(loop, identifier)
            
            bot = FixedRecordingBot(
                room_url=room_url,
                token=token,
                room_name=room_name,
                meeting_id=meeting_id,
                result_queue=result_queue,
                stop_event=stop_event,
                target_fps=target_fps  # Pass target FPS
            )
            
            result = loop_manager.safe_run_until_complete(
                loop, 
                bot.run_recording(),
                timeout=None,
                identifier=identifier
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ FAST recording task error: {e}")
            try:
                result_queue.put_nowait((False, str(e)))
            except:
                pass
        finally:
            if loop:
                loop_manager.force_cleanup_loop(loop, identifier)

    def stop_stream_recording(self, meeting_id: str) -> Dict:
        with self._global_lock:
            if meeting_id not in self.active_recordings:
                return {
                    "status": "error",
                    "message": "No active recording found",
                    "meeting_id": meeting_id
                }
            
            recording_info = self.active_recordings[meeting_id].copy()
        
        try:
            logger.info(f"🛑 Stopping FAST recording for meeting {meeting_id}")
            
            stop_event = recording_info.get("stop_event")
            if stop_event:
                stop_event.set()
            
            recording_future = recording_info.get("recording_future")
            
            if recording_future:
                logger.info("✅ FAST stop signal sent. Finalization will continue in background...")

                threading.Thread(
                    target=self._async_finalize_fast_recording,
                    args=(meeting_id, recording_info),
                    daemon=True
                ).start()

                return {
                    "status": "success",
                    "message": "FAST recording stopped. Processing will continue in background.",
                    "meeting_id": meeting_id
                }

            with self._global_lock:
                if meeting_id in self.active_recordings:
                    del self.active_recordings[meeting_id]

            return {
                "status": "success",
                "message": "FAST recording stopped.",
                "meeting_id": meeting_id
            }

        except Exception as e:
            logger.error(f"❌ Error stopping FAST recording: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                "status": "error",
                "message": f"Failed to stop FAST recording: {str(e)}",
                "meeting_id": meeting_id
            }

    def _async_finalize_fast_recording(self, meeting_id: str, recording_info: dict):
        """Run full S3/FFmpeg finalization with FAST PROCESSING"""
        try:
            recording_future = recording_info.get("recording_future")
            if recording_future:
                logger.info(f"🎬 FAST background finalization started for {meeting_id}")
                recording_future.result()
                logger.info(f"✅ FAST background finalization done for {meeting_id}")
            else:
                logger.warning(f"⚠️ No recording_future found for {meeting_id}")

            s3_prefix = f"{S3_FOLDERS['recordings_temp']}/{meeting_id}"
            raw_video_s3_key = f"{s3_prefix}/raw_video_{meeting_id}.avi"
            raw_audio_s3_key = f"{s3_prefix}/raw_audio_{meeting_id}.wav"

            # Create final MP4 with FAST DUPLICATION
            final_video_s3_key = self._create_final_video_fast_duplication(
                raw_video_s3_key, raw_audio_s3_key, meeting_id
            )

            if not final_video_s3_key:
                logger.error(f"❌ Failed to create final FAST video for {meeting_id}")
                return

            # Trigger pipeline
            processing_result = None
            try:
                processing_result = self._trigger_processing_pipeline(
                    final_video_s3_key, meeting_id,
                    recording_info.get("host_user_id"),
                    recording_info.get("recording_doc_id")
                )
                logger.info(f"✅ Triggered processing pipeline for {meeting_id}")
            except Exception as e:
                logger.error(f"⚠️ Pipeline trigger failed for {meeting_id}: {e}")

            # Clean up temp S3 folder
            try:
                logger.info(f"🧹 Deleting temp S3 folder: {s3_prefix}")
                self._delete_s3_folder(s3_prefix)
            except Exception as e:
                logger.warning(f"⚠️ Could not delete temp folder: {e}")

            # Update DB
            try:
                self.collection.update_one(
                    {"_id": recording_info.get("recording_doc_id")},
                    {"$set": {
                        "recording_status": "completed",
                        "completed_at": datetime.now(),
                        "file_path": final_video_s3_key,
                        "processing_result": processing_result,
                        "video_type": "fast_smooth_duplicated"
                    }}
                )
                logger.info(f"✅ DB updated for FAST video {meeting_id}")
            except Exception as db_err:
                logger.warning(f"⚠️ DB update failed: {db_err}")

        except Exception as e:
            logger.error(f"❌ FAST background finalization failed for {meeting_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _create_final_video_fast_duplication(self, video_s3_key: str, audio_s3_key: Optional[str] = None, 
                      meeting_id: Optional[str] = None) -> Optional[str]:
        """
        Create final MP4 with FAST FRAME DUPLICATION (no motion interpolation)
        """
        temp_video_file = None
        temp_audio_file = None
        temp_final_file = None
        
        try:
            import tempfile
            import subprocess
            
            final_output_key = video_s3_key.replace('.avi', '_fast_final.mp4')
            
            logger.info(f"🎬 Creating FAST SMOOTH video with SIMPLE DUPLICATION")
            
            # Create temp files
            temp_video_fd, temp_video_file = tempfile.mkstemp(suffix='.avi', prefix='raw_video_')
            os.close(temp_video_fd)
            
            temp_audio_fd, temp_audio_file = tempfile.mkstemp(suffix='.wav', prefix='raw_audio_')
            os.close(temp_audio_fd)
            
            temp_final_fd, temp_final_file = tempfile.mkstemp(suffix='.mp4', prefix='fast_final_')
            os.close(temp_final_fd)
            
            # Download from S3
            logger.info(f"📥 Downloading from S3...")
            try:
                s3_client.download_file(AWS_S3_BUCKET, video_s3_key, temp_video_file)
                video_size = os.path.getsize(temp_video_file)
                logger.info(f"✅ Video: {video_size:,} bytes")
                
                s3_client.download_file(AWS_S3_BUCKET, audio_s3_key, temp_audio_file)
                audio_size = os.path.getsize(temp_audio_file)
                logger.info(f"✅ Audio: {audio_size:,} bytes")
            except Exception as e:
                logger.error(f"❌ S3 download failed: {e}")
                return None
            
            # Simple target FPS - no complex interpolation
            target_fps = 24  # Standard smooth playback FPS
            
            logger.info(f"🎬 Target: {target_fps} FPS (FAST PROCESSING - no interpolation)")
            
            # Check GPU availability
            nvenc_available = False
            try:
                result = subprocess.run(
                    ['ffmpeg', '-hide_banner', '-encoders'],
                    capture_output=True, text=True, timeout=5
                )
                nvenc_available = 'h264_nvenc' in result.stdout
            except:
                pass
            
            # Check for advanced smoothing environment variable
            use_advanced_smoothing = os.getenv("USE_ADVANCED_SMOOTHING", "false").lower() == "true"
            
            success = False
            
            # FAST GPU encoding with optional advanced smoothing
            if nvenc_available:
                if use_advanced_smoothing:
                    logger.info(f"🚀 GPU ADVANCED SMOOTHING @ {target_fps} FPS...")
                    # Use fast minterpolate settings
                    video_filter = f'minterpolate=fps={target_fps}:mi_mode=blend'
                else:
                    logger.info(f"🚀 GPU FAST ENCODING @ {target_fps} FPS...")
                    # Use simple fps conversion
                    video_filter = f'fps={target_fps}'
                
                ffmpeg_cmd_gpu = [
                    'ffmpeg', '-y',
                    '-i', temp_video_file,
                    '-i', temp_audio_file,
                    # SMOOTH frame rate conversion with proper FFmpeg syntax
                    '-c:v', 'h264_nvenc',
                    '-preset', 'p4',  # Good quality, faster than p6
                    '-tune', 'hq',
                    '-profile:v', 'high',
                    '-level', '4.2',
                    '-rc', 'vbr',
                    '-cq', '20',      # Good quality
                    '-b:v', '5M',     # Good bitrate
                    '-maxrate', '8M',
                    '-bufsize', '16M',
                    '-pix_fmt', 'yuv420p',
                    # Use the selected video filter
                    '-filter:v', video_filter,
                    '-fps_mode', 'cfr',  # Constant frame rate
                    '-g', str(target_fps),  # 1 second GOP for smooth seeking
                    '-bf', '3',
                    '-refs', '3',
                    # Audio
                    '-c:a', 'aac',
                    '-b:a', '192k',
                    '-ar', '48000',
                    '-ac', '2',
                    '-af', 'asetpts=PTS-STARTPTS',
                    # Container
                    '-movflags', '+faststart',
                    '-max_interleave_delta', '0',
                    temp_final_file
                ]
                
                ffmpeg_env = os.environ.copy()
                ffmpeg_env['CUDA_VISIBLE_DEVICES'] = '0'
                
                # Adjust timeout based on processing mode
                timeout_seconds = 300 if use_advanced_smoothing else 120  # 5min vs 2min
                
                result = subprocess.run(
                    ffmpeg_cmd_gpu,
                    capture_output=True,
                    text=True,
                    env=ffmpeg_env,
                    timeout=timeout_seconds
                )
                
                if result.returncode == 0 and os.path.exists(temp_final_file) and os.path.getsize(temp_final_file) > 0:
                    logger.info(f"✅ GPU FAST ENCODING successful @ {target_fps} FPS")
                    success = True
                else:
                    logger.warning(f"⚠️ GPU encoding failed, trying CPU...")
                    logger.warning(f"GPU error: {result.stderr[-500:]}")
                    if os.path.exists(temp_final_file):
                        os.remove(temp_final_file)
                        temp_final_fd, temp_final_file = tempfile.mkstemp(suffix='.mp4', prefix='fast_final_')
                        os.close(temp_final_fd)
            
            # CPU FALLBACK - fast encoding with same smoothing options
            if not success:
                if use_advanced_smoothing:
                    logger.info(f"⚙️ CPU ADVANCED SMOOTHING @ {target_fps} FPS...")
                    video_filter = f'minterpolate=fps={target_fps}:mi_mode=blend'
                else:
                    logger.info(f"⚙️ CPU FAST ENCODING @ {target_fps} FPS...")
                    video_filter = f'fps={target_fps}'
                
                ffmpeg_cmd_cpu = [
                    'ffmpeg', '-y',
                    '-i', temp_video_file,
                    '-i', temp_audio_file,
                    # SMOOTH frame rate conversion with proper FFmpeg syntax
                    '-c:v', 'libx264',
                    '-preset', 'medium',  # Balanced speed/quality
                    '-tune', 'film',
                    '-profile:v', 'high',
                    '-level', '4.2',
                    '-crf', '20',       # Good quality
                    '-pix_fmt', 'yuv420p',
                    # Use the selected video filter
                    '-filter:v', video_filter,
                    '-fps_mode', 'cfr',  # Constant frame rate
                    '-g', str(target_fps),  # 1 second GOP
                    '-bf', '3',
                    '-refs', '3',
                    # Audio
                    '-c:a', 'aac',
                    '-b:a', '192k',
                    '-ar', '48000',
                    '-ac', '2',
                    '-af', 'asetpts=PTS-STARTPTS',
                    # Container
                    '-movflags', '+faststart',
                    '-max_interleave_delta', '0',
                    temp_final_file
                ]
                
                # Adjust timeout based on processing mode
                timeout_seconds = 600 if use_advanced_smoothing else 180  # 10min vs 3min
                
                result = subprocess.run(
                    ffmpeg_cmd_cpu,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds
                )
                
                if result.returncode == 0 and os.path.exists(temp_final_file) and os.path.getsize(temp_final_file) > 0:
                    logger.info(f"✅ CPU FAST ENCODING successful @ {target_fps} FPS")
                    success = True
                else:
                    logger.error(f"❌ CPU encoding also failed")
                    logger.error(f"Error: {result.stderr[-1000:]}")
                    return None
            
            # Verify final file
            if not success or not os.path.exists(temp_final_file):
                logger.error(f"❌ FAST ENCODING failed - no output file")
                return None
            
            final_size = os.path.getsize(temp_final_file)
            if final_size == 0:
                logger.error(f"❌ Output file is empty")
                return None
            
            logger.info(f"✅ FAST final video: {final_size:,} bytes @ {target_fps} FPS")
            
            # Upload to S3
            logger.info(f"📤 Uploading FAST video to S3: {final_output_key}")
            try:
                with open(temp_final_file, 'rb') as f:
                    s3_client.upload_fileobj(f, AWS_S3_BUCKET, final_output_key)
                logger.info(f"✅ FAST video uploaded successfully")
            except Exception as e:
                logger.error(f"❌ Upload failed: {e}")
                return None
            
            # Verify upload
            try:
                response = s3_client.head_object(Bucket=AWS_S3_BUCKET, Key=final_output_key)
                s3_size = response['ContentLength']
                logger.info(f"✅ FAST video verified in S3: {s3_size:,} bytes @ {target_fps} FPS")
            except Exception as e:
                logger.error(f"❌ Verification failed: {e}")
                return None
            
            # Clean up intermediate S3 files
            logger.info("🧹 Cleaning up intermediate files...")
            try:
                s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=video_s3_key)
                s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=audio_s3_key)
                logger.info("✅ Intermediate files deleted")
            except Exception as e:
                logger.warning(f"⚠️ Cleanup warning: {e}")
            
            return final_output_key
        
        except Exception as e:
            logger.error(f"❌ Error creating FAST video: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
        
        finally:
            # Clean up temp files
            for temp_file in [temp_video_file, temp_audio_file, temp_final_file]:
                try:
                    if temp_file and os.path.exists(temp_file):
                        os.remove(temp_file)
                        logger.debug(f"Deleted temp file: {temp_file}")
                except Exception as e:
                    logger.debug(f"Could not delete {temp_file}: {e}")

    def _detect_video_fps(self, video_file_path: str) -> float:
        """Detect FPS from video file using ffprobe"""
        try:
            result = subprocess.run(
                [
                    'ffprobe', '-v', 'error',
                    '-select_streams', 'v:0',
                    '-show_entries', 'stream=r_frame_rate',
                    '-of', 'default=noprint_wrappers=1:nokey=1',
                    video_file_path
                ],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0 and result.stdout.strip():
                fps_str = result.stdout.strip()
                if '/' in fps_str:
                    num, den = fps_str.split('/')
                    detected_fps = float(num) / float(den)
                else:
                    detected_fps = float(fps_str)
                
                logger.info(f"📊 Detected input FPS: {detected_fps:.2f}")
                return detected_fps
            else:
                logger.warning("⚠️ Could not detect FPS, using 20 FPS default")
                return 20.0
                
        except Exception as e:
            logger.warning(f"⚠️ FPS detection failed: {e}, using 20 FPS default")
            return 20.0
    
    def _delete_s3_folder(self, prefix: str):
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=obj['Key'])
                        logger.info(f"✅ Deleted: {obj['Key']}")
            
            logger.info(f"✅ Deleted S3 folder: {prefix}")
        except Exception as e:
            logger.error(f"Error deleting S3 folder: {e}")
            
    def _trigger_processing_pipeline(self, video_file_path: str, meeting_id: str,
                               host_user_id: str, recording_doc_id: str) -> Dict:
        """Trigger the video processing pipeline"""
        try:
            import tempfile
            import os
            
            logger.info(f"🎬 Processing pipeline triggered for FAST video: {meeting_id}")
            
            if video_file_path.startswith('s3://'):
                s3_key = video_file_path.replace('s3://' + AWS_S3_BUCKET + '/', '')
            elif video_file_path.startswith('recordings_temp/'):
                s3_key = video_file_path
            else:
                s3_key = video_file_path
            
            logger.info(f"📍 S3 Key: {s3_key}")
            
            # Download from S3
            temp_fd, temp_video_path = tempfile.mkstemp(
                suffix='.mp4',
                prefix=f'process_fast_video_{meeting_id}_'
            )
            os.close(temp_fd)
            
            try:
                s3_client.download_file(
                    Bucket=AWS_S3_BUCKET,
                    Key=s3_key,
                    Filename=temp_video_path
                )
                
                file_size = os.path.getsize(temp_video_path)
                logger.info(f"✅ Downloaded FAST video: {file_size:,} bytes from S3")
                
                if file_size == 0:
                    raise Exception("Downloaded video file is empty")
                
            except Exception as download_error:
                logger.error(f"❌ Failed to download video from S3: {download_error}")
                raise Exception(f"S3 download failed: {str(download_error)}")
            
            from core.UserDashBoard.recordings import process_video_sync
            
            result = process_video_sync(temp_video_path, meeting_id, host_user_id)
            
            # Clean up temp file after processing
            try:
                if os.path.exists(temp_video_path):
                    os.remove(temp_video_path)
                    logger.info(f"🧹 Deleted temp video file: {temp_video_path}")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Could not delete temp file: {cleanup_error}")
            
            # Process result
            if result.get("status") == "success":
                processing_data = {
                    "recording_status": "completed",
                    "processing_completed": True,
                    "video_url": result.get("video_url"),
                    "transcript_url": result.get("transcript_url"),
                    "summary_url": result.get("summary_url"),
                    "image_url": result.get("summary_image_url"),
                    "subtitles": result.get("subtitle_urls", {}),
                    "file_size": result.get("file_size", 0),
                    "processing_end_time": datetime.now(),
                    "encoder_used": result.get("encoder_used"),
                    "gpu_accelerated": result.get("gpu_accelerated"),
                    "video_type": "fast_smooth_duplicated"
                }
                
                try:
                    from bson import ObjectId
                    if len(recording_doc_id) == 24:
                        self.collection.update_one(
                            {"_id": ObjectId(recording_doc_id)},
                            {"$set": processing_data}
                        )
                        logger.info(f"✅ Updated MongoDB with FAST processing results")
                except Exception as db_error:
                    logger.warning(f"Database update error: {db_error}")
                
                return {
                    "status": "success",
                    "processing_completed": True,
                    "video_url": result.get("video_url"),
                    "transcript_url": result.get("transcript_url"),
                    "summary_url": result.get("summary_url"),
                    "video_type": "fast_smooth_duplicated"
                }
            else:
                error_msg = result.get("error", "Unknown processing error")
                logger.error(f"❌ Processing failed: {error_msg}")
                return {
                    "status": "error",
                    "error": error_msg
                }
                
        except Exception as e:
            logger.error(f"❌ Processing pipeline error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "status": "error",
                "error": str(e)
            }

    def get_recording_status(self, meeting_id: str) -> Dict:
        """Get current recording status"""
        with self._global_lock:
            if meeting_id in self.active_recordings:
                recording_info = self.active_recordings[meeting_id]
                return {
                    "meeting_id": meeting_id,
                    "status": "active",
                    "start_time": recording_info["start_time"].isoformat(),
                    "room_name": recording_info["room_name"],
                    "is_active": True,
                    "target_fps": recording_info.get("target_fps", 20),
                    "recording_type": "fast"
                }
        
        return {
            "meeting_id": meeting_id,
            "status": "no_recording",
            "is_active": False
        }

    def list_active_recordings(self) -> List[Dict]:
        """List all active recordings"""
        with self._global_lock:
            return [
                {
                    "meeting_id": meeting_id,
                    "recording_id": info.get("recording_doc_id"),
                    "start_time": info.get("start_time").isoformat() if info.get("start_time") else None,
                    "room_name": info.get("room_name"),
                    "host_user_id": info.get("host_user_id"),
                    "target_fps": info.get("target_fps", 20),
                    "recording_type": "fast"
                }
                for meeting_id, info in self.active_recordings.items()
            ]

# Initialize the FAST service
fixed_google_meet_recorder = FixedGoogleMeetRecorder()
stream_recording_service = fixed_google_meet_recorder

# Cleanup handler
import atexit

def cleanup_recording_service():
    """Cleanup function to properly shut down recordings on exit"""
    try:
        logger.info("🛑 Shutting down FAST recording service...")
        with fixed_google_meet_recorder._global_lock:
            for meeting_id in list(fixed_google_meet_recorder.active_recordings.keys()):
                try:
                    fixed_google_meet_recorder.stop_stream_recording(meeting_id)
                except Exception as e:
                    logger.error(f"Error stopping recording {meeting_id}: {e}")
        
        fixed_google_meet_recorder.thread_pool.shutdown(wait=False)
        loop_manager.cleanup_all_loops()
        logger.info("✅ FAST recording service shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during recording service shutdown: {e}")

atexit.register(cleanup_recording_service)