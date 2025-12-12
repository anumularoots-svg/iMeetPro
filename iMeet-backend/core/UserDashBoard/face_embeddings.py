# face_embeddings.py


"""
Face Embeddings Module - Updated to use Shared Model
=====================================================
Uses shared InsightFace model from ../FaceAuth/face_model_shared.py

Author: Face Recognition System  
Version: 2.0.0 - Shared Model Integration
"""
import os
 
# --- Force deterministic CUDA visibility ---
os.environ.update({
    "CUDA_VISIBLE_DEVICES": "0",        # pick your active GPU
    "CUDA_MODULE_LOADING": "LAZY",
    "ORT_CUDA_UNAVAILABLE_FAIL": "0",
    "ORT_TENSORRT_UNAVAILABLE_FAIL": "0",
    "OMP_NUM_THREADS": "4",
})

import os
import sys
import logging
import numpy as np
import base64
from io import BytesIO
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import cv2
from PIL import Image

# MongoDB and S3 imports
from pymongo import MongoClient
from bson import ObjectId
import boto3
from botocore.exceptions import ClientError

# ============================================================================
# IMPORT SHARED FACE MODEL FROM FACEAUTH FOLDER
# ============================================================================
# Add FaceAuth folder to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
faceauth_dir = os.path.join(current_dir, '..', 'FaceAuth')
faceauth_dir = os.path.abspath(faceauth_dir)

if faceauth_dir not in sys.path:
    sys.path.insert(0, faceauth_dir)

try:
    from face_model_shared import get_face_model, compare_embeddings
    FACE_RECOGNITION_ENABLED = True
    face_model = None
except ImportError as e:
    FACE_RECOGNITION_ENABLED = False
    face_model = None
    print(f"⚠️  Warning: Could not import face_model_shared from {faceauth_dir}")
    print(f"   Error: {e}")
    print("   Face recognition features will be disabled")

# Configure logging
logger = logging.getLogger("face_embeddings")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/face_embeddings.log'),
        logging.StreamHandler()
    ]
)

if FACE_RECOGNITION_ENABLED:
    logger.info("✅ Using shared face model from ../FaceAuth/face_model_shared.py")
else:
    logger.warning("⚠️  Face recognition DISABLED - shared model not available")

# ============================================================================
# CONFIGURATION
# ============================================================================

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "connectlydb")

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "connectly-storage")

# Initialize MongoDB client
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client[MONGO_DB]
    face_embeddings_collection = db["face_embeddings"]
    profile_photos_collection = db["profile_photos"]
    logger.info(f"✓ MongoDB connected successfully to {MONGO_DB}")
except Exception as e:
    logger.error(f"✗ MongoDB connection failed: {e}")
    db = None
    face_embeddings_collection = None
    profile_photos_collection = None

# Initialize S3 client
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    logger.info(f"✓ S3 client initialized for bucket: {AWS_S3_BUCKET}")
except Exception as e:
    logger.error(f"✗ S3 client initialization failed: {e}")
    s3_client = None

# ============================================================================
# BACKWARD COMPATIBILITY - Keep old class for imports
# ============================================================================
class FaceEmbeddingEngine:
    """
    Backward compatibility wrapper - now uses shared model.
    Keeps same interface as before so existing code doesn't break.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FaceEmbeddingEngine, cls).__new__(cls)
        return cls._instance
    
    def get_app(self):
        """Get the FaceAnalysis app instance from shared model"""
        if face_model is None:
            raise RuntimeError("Shared face model not initialized")
        return face_model.get_app()
    
    def is_ready(self):
        """Check if shared model is ready"""
        return face_model is not None and face_model.is_ready()


# Global instance for backward compatibility
try:
    face_engine = FaceEmbeddingEngine()
    if FACE_RECOGNITION_ENABLED:
        logger.info("✓ Face embedding engine ready (using shared model)")
except Exception as e:
    logger.error(f"✗ Face embedding engine initialization failed: {e}")
    face_engine = None


# ============================================================================
# IMAGE PROCESSING UTILITIES
# ============================================================================
def ensure_face_model_loaded():
    """Ensure face model is loaded before use"""
    global face_model
    
    if face_model is None and FACE_RECOGNITION_ENABLED:
        logger.info("🔄 Loading face model on-demand...")
        face_model = get_face_model()
        logger.info("✅ Face model loaded successfully")
    
    return face_model

def base64_to_numpy(base64_string: str) -> Optional[np.ndarray]:
    """
    Convert base64 string to numpy array (OpenCV format)
    
    Args:
        base64_string: Base64 encoded image
        
    Returns:
        numpy array in BGR format (OpenCV) or None if error
    """
    try:
        # Remove data URI prefix if present
        if 'base64,' in base64_string:
            base64_string = base64_string.split('base64,')[1]
        
        # Decode base64
        img_bytes = base64.b64decode(base64_string)
        
        # Convert to PIL Image
        pil_image = Image.open(BytesIO(img_bytes))
        
        # Convert to RGB if needed
        if pil_image.mode != 'RGB':
            pil_image = pil_image.convert('RGB')
        
        # Convert to numpy array
        img_array = np.array(pil_image)
        
        # Convert RGB to BGR (OpenCV format)
        img_bgr = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
        
        logger.debug(f"✓ Converted base64 to numpy array: {img_bgr.shape}")
        return img_bgr
        
    except Exception as e:
        logger.error(f"✗ Error converting base64 to numpy: {e}")
        return None


def download_image_from_s3(s3_key: str) -> Optional[np.ndarray]:
    """
    Download image from S3 and convert to numpy array
    
    Args:
        s3_key: S3 object key
        
    Returns:
        numpy array in BGR format or None if error
    """
    try:
        if s3_client is None:
            logger.error("S3 client not initialized")
            return None
            
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
        img_bytes = response['Body'].read()
        
        # Convert to numpy array
        img_array = np.frombuffer(img_bytes, np.uint8)
        img_bgr = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        
        if img_bgr is None:
            logger.error(f"Failed to decode image from S3: {s3_key}")
            return None
        
        logger.debug(f"✓ Downloaded image from S3: {s3_key}, shape: {img_bgr.shape}")
        return img_bgr
        
    except ClientError as e:
        logger.error(f"✗ S3 download error for {s3_key}: {e}")
        return None
    except Exception as e:
        logger.error(f"✗ Error downloading from S3: {e}")
        return None


# ============================================================================
# FACE DETECTION AND EMBEDDING GENERATION - Using Shared Model
# ============================================================================

def detect_and_generate_embedding(image: np.ndarray, user_id: int) -> Dict:
    """
    Detect face and generate embedding using SHARED InsightFace model
    Now with lazy loading support
    """
    try:
        # Ensure model is loaded
        model = ensure_face_model_loaded()
        
        if not FACE_RECOGNITION_ENABLED or model is None:
            return {
                'success': False,
                'error': 'Face recognition not available'
            }
        
        # Use shared model to extract embedding with face info
        try:
            face_info = model.extract_embedding(image, return_face_info=True)
        except ValueError as ve:
            logger.warning(f"No face detected for user {user_id}: {ve}")
            return {
                'success': False,
                'error': str(ve)
            }
        
        # Format result to match original API
        result = {
            'success': True,
            'embedding': face_info['embedding'],
            'embedding_size': len(face_info['embedding']),
            'bbox': face_info['bbox'],
            'landmarks': face_info['landmarks'],
            'det_score': face_info['det_score'],
            'age': face_info.get('age'),
            'gender': face_info.get('gender'),
            'face_count': face_info['face_count']
        }
        
        logger.info(f"✓ Generated embedding for user {user_id}, det_score: {face_info['det_score']:.3f}, faces: {face_info['face_count']}")
        return result
        
    except Exception as e:
        logger.error(f"✗ Error generating embedding for user {user_id}: {e}")
        logger.exception("Full traceback:")
        return {
            'success': False,
            'error': str(e)
        }


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def store_face_embedding(user_id: int, photo_id: str, embedding_data: Dict) -> Optional[str]:
    """
    Store face embedding in MongoDB
    
    Args:
        user_id: User ID from MySQL
        photo_id: MongoDB ObjectId of profile photo
        embedding_data: Dictionary containing embedding and face attributes
        
    Returns:
        Embedding document ID or None
    """
    try:
        if face_embeddings_collection is None:
            logger.error("Face embeddings collection not available")
            return None
            
        if not embedding_data.get('success'):
            logger.error(f"Cannot store failed embedding for user {user_id}")
            return None
        
        # Create embedding document
        embedding_doc = {
            'user_id': int(user_id),
            'photo_id': photo_id,
            'embedding': embedding_data['embedding'],
            'embedding_size': embedding_data['embedding_size'],
            'bbox': embedding_data['bbox'],
            'landmarks': embedding_data['landmarks'],
            'det_score': embedding_data['det_score'],
            'age': embedding_data.get('age'),
            'gender': embedding_data.get('gender'),
            'face_count': embedding_data.get('face_count', 1),
            'model': 'buffalo_l',  # Model version
            'created_at': datetime.utcnow(),
            'status': 'active'
        }
        
        # Insert into MongoDB
        result = face_embeddings_collection.insert_one(embedding_doc)
        embedding_id = str(result.inserted_id)
        
        logger.info(f"✓ Stored embedding {embedding_id} for user {user_id}")
        return embedding_id
        
    except Exception as e:
        logger.error(f"✗ Error storing embedding for user {user_id}: {e}")
        logger.exception("Full traceback:")
        return None


def get_face_embedding(embedding_id: str) -> Optional[Dict]:
    """
    Retrieve face embedding by ID
    
    Args:
        embedding_id: MongoDB ObjectId as string
        
    Returns:
        Embedding document or None
    """
    try:
        if face_embeddings_collection is None:
            return None
            
        doc = face_embeddings_collection.find_one({'_id': ObjectId(embedding_id)})
        
        if doc and doc.get('status') == 'active':
            doc['_id'] = str(doc['_id'])
            doc['photo_id'] = str(doc['photo_id'])
            return doc
        
        return None
        
    except Exception as e:
        logger.error(f"✗ Error retrieving embedding {embedding_id}: {e}")
        return None


def get_user_embeddings(user_id: int) -> List[Dict]:
    """
    Get all active embeddings for a user
    
    Args:
        user_id: User ID from MySQL
        
    Returns:
        List of embedding documents
    """
    try:
        if face_embeddings_collection is None:
            return []
            
        embeddings = face_embeddings_collection.find(
            {'user_id': int(user_id), 'status': 'active'},
            sort=[('created_at', -1)]
        )
        
        result = []
        for doc in embeddings:
            doc['_id'] = str(doc['_id'])
            doc['photo_id'] = str(doc['photo_id'])
            result.append(doc)
        
        logger.debug(f"Found {len(result)} embeddings for user {user_id}")
        return result
        
    except Exception as e:
        logger.error(f"✗ Error getting embeddings for user {user_id}: {e}")
        return []


def delete_face_embedding(embedding_id: str, permanent: bool = False) -> bool:
    """
    Delete face embedding
    
    Args:
        embedding_id: MongoDB ObjectId as string
        permanent: If True, permanently delete, else soft delete
        
    Returns:
        Success status
    """
    try:
        if face_embeddings_collection is None:
            return False
            
        if permanent:
            result = face_embeddings_collection.delete_one({'_id': ObjectId(embedding_id)})
            logger.info(f"✓ Permanently deleted embedding {embedding_id}")
            return result.deleted_count > 0
        else:
            result = face_embeddings_collection.update_one(
                {'_id': ObjectId(embedding_id)},
                {'$set': {'status': 'deleted', 'deleted_at': datetime.utcnow()}}
            )
            logger.info(f"✓ Soft deleted embedding {embedding_id}")
            return result.modified_count > 0
        
    except Exception as e:
        logger.error(f"✗ Error deleting embedding {embedding_id}: {e}")
        return False


# ============================================================================
# FACE RECOGNITION AND SIMILARITY - Using Shared Model
# ============================================================================

def calculate_similarity(embedding1: np.ndarray, embedding2: np.ndarray) -> float:
    """
    Calculate cosine similarity between two embeddings
    Now uses shared model's compare_embeddings function
    
    Args:
        embedding1: First embedding vector
        embedding2: Second embedding vector
        
    Returns:
        Similarity score (0-1, higher is more similar)
    """
    try:
        if FACE_RECOGNITION_ENABLED:
            # Use shared model's comparison (returns distance, so convert to similarity)
            distance = compare_embeddings(embedding1, embedding2, method='cosine')
            similarity = 1 - distance  # Convert distance to similarity
            return float(similarity)
        else:
            # Fallback calculation
            embedding1_norm = embedding1 / np.linalg.norm(embedding1)
            embedding2_norm = embedding2 / np.linalg.norm(embedding2)
            similarity = np.dot(embedding1_norm, embedding2_norm)
            return float(similarity)
        
    except Exception as e:
        logger.error(f"✗ Error calculating similarity: {e}")
        return 0.0


def find_matching_user(query_embedding: np.ndarray, threshold: float = 0.6) -> Optional[Dict]:
    """
    Find user with matching face embedding
    
    Args:
        query_embedding: Query face embedding
        threshold: Similarity threshold (0-1)
        
    Returns:
        Dictionary with user_id and similarity score, or None
    """
    try:
        if face_embeddings_collection is None:
            logger.error("Face embeddings collection not available")
            return None
            
        # Get all active embeddings
        all_embeddings = face_embeddings_collection.find({'status': 'active'})
        
        best_match = None
        best_similarity = 0.0
        comparisons = 0
        
        for doc in all_embeddings:
            stored_embedding = np.array(doc['embedding'])
            similarity = calculate_similarity(query_embedding, stored_embedding)
            comparisons += 1
            
            if similarity > best_similarity and similarity >= threshold:
                best_similarity = similarity
                best_match = {
                    'user_id': doc['user_id'],
                    'embedding_id': str(doc['_id']),
                    'similarity': similarity,
                    'det_score': doc['det_score']
                }
        
        if best_match:
            logger.info(f"✓ Found match: User {best_match['user_id']}, similarity: {best_similarity:.3f} (compared {comparisons} embeddings)")
        else:
            logger.info(f"No matching user found above threshold {threshold} (compared {comparisons} embeddings)")
        
        return best_match
        
    except Exception as e:
        logger.error(f"✗ Error finding matching user: {e}")
        return None


def verify_face_match(user_id: int, query_image: np.ndarray, threshold: float = 0.6) -> Dict:
    """
    Verify if query image matches a specific user
    NOW WITH MULTI-FACE DETECTION
    
    Args:
        user_id: User ID to verify against
        query_image: Query image as numpy array
        threshold: Similarity threshold
        
    Returns:
        Verification result dictionary
    """
    try:
        # Generate embedding for query image
        query_result = detect_and_generate_embedding(query_image, user_id)
        
        if not query_result['success']:
            return {
                'verified': False,
                'error': query_result.get('error', 'Failed to detect face')
            }
        
        # ================================================================
        # NEW: CHECK FOR MULTIPLE FACES
        # ================================================================
        face_count = query_result.get('face_count', 1)
        
        if face_count > 1:
            logger.error(
                f"🚫 MULTIPLE PEOPLE DETECTED DURING VERIFICATION\n"
                f"   User ID: {user_id}\n"
                f"   Detected Faces: {face_count}\n"
                f"   Status: VERIFICATION REJECTED\n"
                f"   Reason: Multiple people in frame during meeting"
            )
            
            return {
                'verified': False,
                'error': f'Multiple people detected ({face_count}). Only single participant allowed.',
                'face_count': face_count,
                'error_code': 'MULTIPLE_FACES_DETECTED',
                'details': {
                    'detected_faces': face_count,
                    'allowed_faces': 1,
                    'message': 'Only the registered participant should be visible.',
                    'action_required': 'Ensure you are alone in front of the camera.'
                }
            }
        
        logger.info(f"✅ Single face detected (face_count: {face_count}) - proceeding with verification")
        # ================================================================
        
        query_embedding = np.array(query_result['embedding'])
        
        # Get user's stored embeddings
        user_embeddings = get_user_embeddings(user_id)
        
        if not user_embeddings:
            return {
                'verified': False,
                'error': 'No stored embeddings for user'
            }
        
        # Compare with all user embeddings
        similarities = []
        for emb_doc in user_embeddings:
            stored_embedding = np.array(emb_doc['embedding'])
            similarity = calculate_similarity(query_embedding, stored_embedding)
            similarities.append(similarity)
        
        max_similarity = max(similarities)
        verified = max_similarity >= threshold
        
        result = {
            'verified': verified,
            'user_id': user_id,
            'max_similarity': max_similarity,
            'threshold': threshold,
            'embeddings_compared': len(similarities),
            'face_count': face_count,  # NEW: Include face count in response
            'single_participant_confirmed': True  # NEW
        }
        
        logger.info(f"✓ Verification for user {user_id}: {verified}, similarity: {max_similarity:.3f}")
        return result
        
    except Exception as e:
        logger.error(f"✗ Error verifying face for user {user_id}: {e}")
        return {
            'verified': False,
            'error': str(e)
        }


# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================

def process_profile_photo_embedding(user_id: int, photo_id: str) -> Optional[str]:
    """
    Process profile photo and generate embedding using SHARED MODEL
    Main function to be called from users.py
    
    Args:
        user_id: User ID from MySQL
        photo_id: MongoDB ObjectId of profile photo
        
    Returns:
        Embedding ID or None
    """
    try:
        logger.info(f"Processing embedding for user {user_id}, photo {photo_id}")
        
        if not FACE_RECOGNITION_ENABLED:
            logger.error("Face recognition not enabled - shared model not available")
            return None
        
        if profile_photos_collection is None:
            logger.error("Profile photos collection not available")
            return None
        
        # Get photo metadata from MongoDB
        photo_doc = profile_photos_collection.find_one({'_id': ObjectId(photo_id)})
        
        if not photo_doc or photo_doc.get('status') != 'active':
            logger.error(f"Photo {photo_id} not found or inactive")
            return None
        
        # Download image from S3
        s3_key = photo_doc['s3_key']
        image = download_image_from_s3(s3_key)
        
        if image is None:
            logger.error(f"Failed to download image from S3: {s3_key}")
            return None
        
        # Generate face embedding using SHARED MODEL
        embedding_data = detect_and_generate_embedding(image, user_id)
        
        if not embedding_data['success']:
            logger.error(f"Failed to generate embedding: {embedding_data.get('error')}")
            return None
        
        # Store embedding in MongoDB
        embedding_id = store_face_embedding(user_id, photo_id, embedding_data)
        
        if embedding_id:
            # Update photo document with embedding reference
            profile_photos_collection.update_one(
                {'_id': ObjectId(photo_id)},
                {'$set': {
                    'embedding_id': embedding_id,
                    'has_embedding': True,
                    'embedding_generated_at': datetime.utcnow()
                }}
            )
            
            logger.info(f"✓ Successfully processed embedding {embedding_id} for user {user_id}")
        
        return embedding_id
        
    except Exception as e:
        logger.error(f"✗ Error processing profile photo embedding: {e}")
        logger.exception("Full traceback:")
        return None


def process_image_for_recognition(image_base64: str) -> Optional[Dict]:
    """
    Process an image for face recognition (login/verification)
    Uses SHARED MODEL with lazy loading
    """
    try:
        # Ensure model is loaded
        model = ensure_face_model_loaded()
        
        if not FACE_RECOGNITION_ENABLED or model is None:
            return {
                'success': False,
                'error': 'Face recognition service not available'
            }
        
        # Convert base64 to numpy
        image = base64_to_numpy(image_base64)
        
        if image is None:
            return {
                'success': False,
                'error': 'Invalid image data'
            }
        
        # Generate embedding using SHARED MODEL
        try:
            face_info = face_model.extract_embedding(image, return_face_info=True)
        except ValueError as ve:
            return {
                'success': False,
                'error': str(ve)
            }
        
        query_embedding = np.array(face_info['embedding'])
        
        # Find matching user
        match = find_matching_user(query_embedding, threshold=0.6)
        
        if match:
            return {
                'success': True,
                'matched': True,
                'user_id': match['user_id'],
                'similarity': match['similarity'],
                'det_score': face_info['det_score']
            }
        else:
            return {
                'success': True,
                'matched': False,
                'message': 'No matching user found'
            }
        
    except Exception as e:
        logger.error(f"✗ Error in face recognition: {e}")
        logger.exception("Full traceback:")
        return {
            'success': False,
            'error': str(e)
        }


# ============================================================================
# CLEANUP AND MAINTENANCE
# ============================================================================

def cleanup_orphaned_embeddings() -> int:
    """
    Remove embeddings for deleted photos
    
    Returns:
        Number of embeddings cleaned up
    """
    try:
        if face_embeddings_collection is None or profile_photos_collection is None:
            return 0
            
        # Find embeddings with deleted or missing photos
        embeddings = face_embeddings_collection.find({'status': 'active'})
        
        cleanup_count = 0
        for emb_doc in embeddings:
            photo_doc = profile_photos_collection.find_one({'_id': ObjectId(emb_doc['photo_id'])})
            
            if not photo_doc or photo_doc.get('status') != 'active':
                # Soft delete embedding
                face_embeddings_collection.update_one(
                    {'_id': emb_doc['_id']},
                    {'$set': {'status': 'deleted', 'deleted_at': datetime.utcnow()}}
                )
                cleanup_count += 1
        
        logger.info(f"✓ Cleaned up {cleanup_count} orphaned embeddings")
        return cleanup_count
        
    except Exception as e:
        logger.error(f"✗ Error cleaning up embeddings: {e}")
        return 0


def get_embedding_stats() -> Dict:
    """
    Get statistics about stored embeddings
    
    Returns:
        Statistics dictionary
    """
    try:
        if face_embeddings_collection is None:
            return {}
            
        total_embeddings = face_embeddings_collection.count_documents({'status': 'active'})
        total_users = len(face_embeddings_collection.distinct('user_id', {'status': 'active'}))
        
        avg_det_score = face_embeddings_collection.aggregate([
            {'$match': {'status': 'active'}},
            {'$group': {'_id': None, 'avg_score': {'$avg': '$det_score'}}}
        ])
        
        avg_score = list(avg_det_score)
        avg_score = avg_score[0]['avg_score'] if avg_score else 0.0
        
        stats = {
            'total_embeddings': total_embeddings,
            'total_users_with_embeddings': total_users,
            'average_detection_score': round(avg_score, 3),
            'model': 'buffalo_l (shared)',
            'embedding_dimension': 512,
            'using_shared_model': FACE_RECOGNITION_ENABLED
        }
        
        logger.info(f"Embedding stats: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"✗ Error getting stats: {e}")
        return {}


# ============================================================================
# INITIALIZATION CHECK
# ============================================================================

def check_face_recognition_ready() -> Dict:
    """
    Check if face recognition system is ready
    
    Returns:
        Status dictionary
    """
    status = {
        'ready': False,
        'face_engine': False,
        'shared_model': False,
        'mongodb': False,
        's3': False,
        'errors': []
    }
    
    # Check shared face model
    if FACE_RECOGNITION_ENABLED and face_model is not None:
        status['face_engine'] = True
        status['shared_model'] = True
    else:
        status['errors'].append('Shared face model not initialized')
    
    # Check MongoDB
    if face_embeddings_collection is not None:
        status['mongodb'] = True
    else:
        status['errors'].append('MongoDB not connected')
    
    # Check S3
    if s3_client is not None:
        status['s3'] = True
    else:
        status['errors'].append('S3 client not initialized')
    
    # Overall ready status
    status['ready'] = status['face_engine'] and status['mongodb'] and status['s3']
    
    return status


# Log initialization status
init_status = check_face_recognition_ready()
if init_status['ready']:
    logger.info("✓✓✓ Face recognition system fully initialized and ready (SHARED MODEL from FaceAuth) ✓✓✓")
else:
    logger.warning(f"⚠ Face recognition system partially initialized. Errors: {init_status['errors']}")