from django.db import connection, transaction
from datetime import datetime, timedelta
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.db import models
from django.db.utils import ProgrammingError, OperationalError
from django.utils import timezone
import pytz
import json
import logging
import re
import os
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import timedelta
from pymongo import MongoClient
from bson import ObjectId
import base64
from urllib.parse import quote_plus  # ADD THIS IMPORT
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import uuid
# Global Variables
TBL_USER = 'tbl_Users'

SUCCESS_STATUS = 200
CREATED_STATUS = 201
BAD_REQUEST_STATUS = 400
UNAUTHORIZED_STATUS = 401
FORBIDDEN_STATUS = 403
NOT_FOUND_STATUS = 404
SERVER_ERROR_STATUS = 500

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "connectly-storage")

# S3 Folders
S3_FOLDERS = {
    "videos": os.getenv("S3_FOLDER_VIDEOS", "videos"),
    "transcripts": os.getenv("S3_FOLDER_TRANSCRIPTS", "transcripts"),
    "summary": os.getenv("S3_FOLDER_SUMMARY", "summary"),
    "images": os.getenv("S3_FOLDER_IMAGES", "summary_image"),
    "subtitles": os.getenv("S3_FOLDER_SUBTITLES", "subtitles"),
    "profile_photos": "profile_photos"
}

# ✅ LOGGER INITIALIZATION (MOVED UP)
logger = logging.getLogger("profile_photo_handler")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)

# ✅ AWS CREDENTIALS VALIDATION (AFTER logger is defined)
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    logger.error("❌ AWS credentials not configured! Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env")
    logger.warning("⚠️ Profile photo uploads will fail until AWS credentials are configured")
    S3_ENABLED = False
else:
    S3_ENABLED = True
    logger.info("✅ AWS credentials loaded successfully")

# S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[os.getenv("MONGO_DB", "connectlydb")]
profile_photos_collection = db["profile_photos"]

# Email configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", os.getenv("EMAIL_HOST", "smtp.gmail.com"))
SMTP_PORT = int(os.getenv("SMTP_PORT", os.getenv("EMAIL_PORT", 587)))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", os.getenv("EMAIL_HOST_USER"))
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", os.getenv("EMAIL_HOST_PASSWORD"))
FROM_EMAIL = os.getenv("FROM_EMAIL", os.getenv("DEFAULT_FROM_EMAIL", SMTP_USERNAME))

# (Face embeddings imports continue below...)

# ============================================================================
# FACE EMBEDDINGS IMPORTS - Add after line 17, before "# Global Variables"
# ============================================================================
try:
    from .face_embeddings import (
        process_profile_photo_embedding,
        process_image_for_recognition,
        get_user_embeddings,
        delete_face_embedding,
        verify_face_match,
        get_embedding_stats,
        cleanup_orphaned_embeddings,
        base64_to_numpy,
        check_face_recognition_ready
    )
    FACE_RECOGNITION_ENABLED = True
    logging.info("✓ Face recognition module imported successfully")
except ImportError as e:
    logging.warning(f"⚠ Face recognition module not available: {e}")
    FACE_RECOGNITION_ENABLED = False
    # Create dummy functions if import fails
    def process_profile_photo_embedding(*args, **kwargs): return None
    def process_image_for_recognition(*args, **kwargs): return None
    def get_user_embeddings(*args, **kwargs): return []
    def delete_face_embedding(*args, **kwargs): return False
    def verify_face_match(*args, **kwargs): return {'verified': False}
    def get_embedding_stats(*args, **kwargs): return {}
    def cleanup_orphaned_embeddings(*args, **kwargs): return 0
    def base64_to_numpy(*args, **kwargs): return None
    def check_face_recognition_ready(*args, **kwargs): return {'ready': False}

class User(models.Model):
    id = models.AutoField(primary_key=True)
    full_name = models.CharField(max_length=100)
    email = models.EmailField(max_length=100, unique=True)
    password = models.CharField(max_length=255)
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    address = models.CharField(max_length=255, blank=True, null=True)
    country = models.CharField(max_length=50, blank=True, null=True)
    status = models.BooleanField(default=True)
    status_code = models.CharField(max_length=1, default='u')
    country_code = models.CharField(max_length=10, blank=True, null=True)
    languages = models.CharField(max_length=100, blank=True, null=True)
    agreeToTerms = models.BooleanField(default=False)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'tbl_Users'

def validate_password(password):
    """Validate password: 8+ chars, 1 upper, 1 lower, 1 number, 1 special char"""
    if not password:
        return False, "Password cannot be empty"
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    if not re.search(r'[A-Z]', password):
        return False, "Password must contain at least one uppercase letter"
    if not re.search(r'[a-z]', password):
        return False, "Password must contain at least one lowercase letter"
    if not re.search(r'\d', password):
        return False, "Password must contain at least one number"
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        return False, "Password must contain at least one special character"
    return True, ""

def generate_OTP():
    """Generate a 6-digit OTP"""
    return ''.join([str(random.randint(0, 9)) for _ in range(6)])

def send_OTP_email(email, OTP):
    """Send OTP to the provided email address"""
    try:
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = email
        msg['Subject'] = 'Your Password Reset OTP'
        
        body = f"Your OTP for password reset is: {OTP}\nThis OTP is valid for 10 minutes."
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, email, msg.as_string())
        server.quit()
        logging.debug(f"OTP {OTP} sent to {email}")
        return True
    except Exception as e:
        logging.error(f"Failed to send OTP to {email}: {e}")
        return False

def create_user_table():
    """Create tbl_Users table with profile_photo_id, edited_photo_id, photo_code and face_embedding_id columns"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_Users (
                    ID INT AUTO_INCREMENT PRIMARY KEY,
                    full_name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    password VARCHAR(255) NOT NULL,
                    phone_number VARCHAR(20) DEFAULT NULL,
                    address VARCHAR(255) DEFAULT NULL,
                    country VARCHAR(50) DEFAULT NULL,
                    Status BOOLEAN DEFAULT TRUE,
                    status_Code CHAR(1) DEFAULT 'u',
                    country_code VARCHAR(10) DEFAULT NULL,
                    languages VARCHAR(100) DEFAULT NULL,
                    agreeToTerms BOOLEAN DEFAULT FALSE,
                    profile_photo_id VARCHAR(100) DEFAULT NULL,
                    edited_photo_id VARCHAR(100) DEFAULT NULL,
                    photo_code TINYINT DEFAULT 0,
                    face_embedding_id VARCHAR(100) DEFAULT NULL,
                    Created_At DATETIME DEFAULT CURRENT_TIMESTAMP,
                    Updated_At DATETIME DEFAULT NULL
                )
            """)
            logging.debug("tbl_Users table created with profile_photo_id, edited_photo_id, photo_code, face_embedding_id")
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Failed to create tbl_Users table: {e}")

def create_otp_table():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tbl_OTP_Reset (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(100) NOT NULL,
                    otp VARCHAR(6) NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME NOT NULL,
                    used BOOLEAN DEFAULT FALSE
                )
            """)
            logging.debug("tbl_OTP_Reset table created or exists")
    except Exception as e:
        logging.error(f"Failed to create tbl_OTP_Reset table: {e}")

def get_mongo_client():
    """Get MongoDB client connection"""
    try:
        logging.info(f"Attempting MongoDB connection with URI: {MONGO_URI.split('@')[0]}@****")
        
        client = MongoClient(
            MONGO_URI, 
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        
        # Test the connection
        client.server_info()
        db = client[MONGO_DB]
        
        logging.info(f"✓ MongoDB connection successful to database: {MONGO_DB}")
        return db
        
    except Exception as e:
        logging.error(f"✗ MongoDB connection error: {e}")
        logging.error(f"MongoDB URI used: {MONGO_URI}")
        logging.error(f"Target database: {MONGO_DB}")
        return None

def store_profile_photo(user_id, photo_base64):
    """
    Store profile photo in MongoDB
    Args:
        user_id: User ID from MySQL
        photo_base64: Base64 encoded image string
    Returns: 
        photo_id (str) or None
    """
    try:
        db = get_mongo_client()
        if db is None:  # FIX: Compare with None instead of using if not db
            logging.error("Failed to connect to MongoDB")
            return None
        
        # Remove base64 prefix if present (data:image/jpeg;base64,)
        if 'base64,' in photo_base64:
            photo_base64 = photo_base64.split('base64,')[1]
        
        # Decode base64 to bytes
        photo_bytes = base64.b64decode(photo_base64)
        
        # Calculate file size
        file_size = len(photo_bytes)
        
        # Validate file size (max 5MB)
        max_size = 5 * 1024 * 1024  # 5MB in bytes
        if file_size > max_size:
            logging.error(f"Photo size {file_size} exceeds maximum {max_size}")
            return None
        
        # Create document for MongoDB
        photo_document = {
            'user_id': int(user_id),
            'photo_data': photo_bytes,
            'content_type': 'image/jpeg',
            'uploaded_at': datetime.utcnow(),
            'file_size': file_size,
            'status': 'active'
        }
        
        # Insert into MongoDB
        result = db.profile_photos.insert_one(photo_document)
        
        logging.info(f"✓ Photo stored for user {user_id}: {result.inserted_id}, size: {file_size} bytes")
        return str(result.inserted_id)
        
    except base64.binascii.Error as e:
        logging.error(f"Invalid base64 data: {e}")
        return None
    except Exception as e:
        logging.error(f"Error storing photo: {e}")
        logging.exception("Full traceback:")
        return None

def get_profile_photo(photo_id):
    """
    Retrieve profile photo from MongoDB
    Args:
        photo_id: MongoDB ObjectId as string
    Returns: 
        base64 encoded photo string or None
    """
    try:
        db = get_mongo_client()
        if db is None:  # FIX: Compare with None instead of using if not db
            return None
        
        # Convert string to ObjectId
        photo_doc = db.profile_photos.find_one({'_id': ObjectId(photo_id)})
        
        if photo_doc and photo_doc.get('status') == 'active':
            # Convert bytes to base64
            photo_base64 = base64.b64encode(photo_doc['photo_data']).decode('utf-8')
            return f"data:{photo_doc['content_type']};base64,{photo_base64}"
        
        return None
        
    except Exception as e:
        logging.error(f"Error retrieving photo {photo_id}: {e}")
        return None

def delete_profile_photo(photo_id):
    """
    Soft delete profile photo in MongoDB
    Args:
        photo_id: MongoDB ObjectId as string
    Returns: 
        bool - Success status
    """
    try:
        db = get_mongo_client()
        if db is None:  # FIX: Compare with None instead of using if not db
            return False
        
        result = db.profile_photos.update_one(
            {'_id': ObjectId(photo_id)},
            {'$set': {'status': 'deleted', 'deleted_at': datetime.utcnow()}}
        )
        
        return result.modified_count > 0
        
    except Exception as e:
        logging.error(f"Error deleting photo {photo_id}: {e}")
        return False

def generate_unique_photo_filename(user_id: int, file_extension: str = "jpg") -> str:
    """
    Generate unique filename for profile photo
    Args:
        user_id: User ID from MySQL
        file_extension: File extension (jpg, png, etc.)
    Returns:
        Unique filename string
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    return f"user_{user_id}_{timestamp}_{unique_id}.{file_extension}"


def detect_image_format(base64_string: str) -> str:
    """
    Detect image format from base64 string
    Args:
        base64_string: Base64 encoded image
    Returns:
        File extension (jpg, png, etc.)
    """
    if base64_string.startswith('data:'):
        if 'image/png' in base64_string:
            return 'png'
        elif 'image/jpeg' in base64_string or 'image/jpg' in base64_string:
            return 'jpg'
        elif 'image/gif' in base64_string:
            return 'gif'
        elif 'image/webp' in base64_string:
            return 'webp'
    return 'jpg'  # Default to jpg

def store_profile_photo_s3(user_id: int, photo_base64: str) -> dict:
    """
    Store profile photo in AWS S3 and metadata in MongoDB
    """
    try:
        # ✅ CHECK AWS CREDENTIALS FIRST
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            logger.error("❌ AWS credentials not configured. Cannot upload to S3.")
            logger.error("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            return None
        
        # Remove base64 prefix if present
        if 'base64,' in photo_base64:
            photo_base64 = photo_base64.split('base64,')[1]
        
        # Decode base64 to bytes
        photo_bytes = base64.b64decode(photo_base64)
        
        # Calculate file size
        file_size = len(photo_bytes)
        
        # Validate file size (max 5MB)
        max_size = 5 * 1024 * 1024  # 5MB
        if file_size > max_size:
            logger.error(f"Photo size {file_size} exceeds maximum {max_size}")
            return None
        
        # Detect image format
        file_extension = detect_image_format(photo_base64)
        content_type = f"image/{file_extension}"
        
        # Generate unique filename
        filename = generate_unique_photo_filename(user_id, file_extension)
        s3_key = f"{S3_FOLDERS['profile_photos']}/{filename}"
        
        # ✅ ADD BETTER ERROR HANDLING HERE
        try:
            # Upload to S3
            s3_client.put_object(
                Bucket=AWS_S3_BUCKET,
                Key=s3_key,
                Body=photo_bytes,
                ContentType=content_type,
                Metadata={
                    'user_id': str(user_id),
                    'uploaded_at': datetime.utcnow().isoformat()
                }
            )
            logger.info(f"✅ Uploaded to S3: {s3_key}")
            
        except NoCredentialsError:
            logger.error("❌ AWS Credentials not available or invalid")
            logger.error("Check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            return None
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"❌ AWS S3 Error [{error_code}]: {error_message}")
            return None
        
        # Generate S3 URL
        s3_url = f"https://{AWS_S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        
        # Store metadata in MongoDB
        photo_document = {
            'user_id': int(user_id),
            's3_key': s3_key,
            's3_url': s3_url,
            's3_bucket': AWS_S3_BUCKET,
            's3_region': AWS_REGION,
            'filename': filename,
            'content_type': content_type,
            'file_size': file_size,
            'uploaded_at': datetime.utcnow(),
            'status': 'active'
        }
        
        result = profile_photos_collection.insert_one(photo_document)
        photo_id = str(result.inserted_id)
        
        logger.info(f"✓ Photo stored in S3 for user {user_id}: {s3_key}, size: {file_size} bytes")
        
        return {
            'photo_id': photo_id,
            's3_key': s3_key,
            's3_url': s3_url,
            'file_size': file_size
        }
        
    except base64.binascii.Error as e:
        logger.error(f"Invalid base64 data: {e}")
        return None
    except Exception as e:
        logger.error(f"Error storing photo: {e}")
        logger.exception("Full traceback:")
        return None
        
def get_profile_photo_s3(photo_id: str, return_base64: bool = True) -> dict:
    """
    Retrieve profile photo from S3
    Args:
        photo_id: MongoDB ObjectId as string
        return_base64: If True, return base64 encoded image, else return S3 URL
    Returns:
        Dictionary with photo data or None
    """
    try:
        # Get metadata from MongoDB
        photo_doc = profile_photos_collection.find_one({'_id': ObjectId(photo_id)})
        
        if not photo_doc or photo_doc.get('status') != 'active':
            logger.warning(f"Photo {photo_id} not found or inactive")
            return None
        
        s3_key = photo_doc['s3_key']
        
        if return_base64:
            # Download from S3 and convert to base64
            response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
            photo_bytes = response['Body'].read()
            photo_base64 = base64.b64encode(photo_bytes).decode('utf-8')
            
            return {
                'photo_id': photo_id,
                'photo_data': f"data:{photo_doc['content_type']};base64,{photo_base64}",
                's3_url': photo_doc['s3_url'],
                'content_type': photo_doc['content_type'],
                'file_size': photo_doc['file_size']
            }
        else:
            # Return S3 URL only (for direct access)
            return {
                'photo_id': photo_id,
                's3_url': photo_doc['s3_url'],
                'content_type': photo_doc['content_type'],
                'file_size': photo_doc['file_size']
            }
        
    except ClientError as e:
        logger.error(f"S3 error retrieving photo {photo_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error retrieving photo {photo_id}: {e}")
        return None

def get_profile_photo_by_user_id(user_id: int, return_base64: bool = True) -> dict:
    """
    Get active profile photo for a user
    Args:
        user_id: User ID from MySQL
        return_base64: If True, return base64 encoded image
    Returns:
        Dictionary with photo data or None
    """
    try:
        # Find active photo for user
        photo_doc = profile_photos_collection.find_one(
            {'user_id': int(user_id), 'status': 'active'},
            sort=[('uploaded_at', 1)]  # Get most recent
        )
        
        if not photo_doc:
            logger.warning(f"No active photo found for user {user_id}")
            return None
        
        photo_id = str(photo_doc['_id'])
        return get_profile_photo_s3(photo_id, return_base64)
        
    except Exception as e:
        logger.error(f"Error getting photo for user {user_id}: {e}")
        return None

def update_profile_photo_s3(user_id: int, photo_base64: str, old_photo_id: str = None) -> dict:
    """
    Update profile photo (upload new, soft delete old)
    Args:
        user_id: User ID from MySQL
        photo_base64: Base64 encoded new image
        old_photo_id: MongoDB ObjectId of old photo (optional)
    Returns:
        Dictionary with new photo info or None
    """
    try:
        # Upload new photo
        new_photo_result = store_profile_photo_s3(user_id, photo_base64)
        
        if not new_photo_result:
            return None
        
        # Soft delete old photo if provided
        if old_photo_id:
            delete_profile_photo_s3(old_photo_id)
        else:
            # Soft delete any active photos for this user
            profile_photos_collection.update_many(
                {'user_id': int(user_id), 'status': 'active'},
                {'$set': {'status': 'replaced', 'replaced_at': datetime.utcnow()}}
            )
        
        logger.info(f"✓ Photo updated for user {user_id}")
        return new_photo_result
        
    except Exception as e:
        logger.error(f"Error updating photo for user {user_id}: {e}")
        return None

def delete_profile_photo_s3(photo_id: str, permanent: bool = False) -> bool:
    """
    Delete profile photo from S3 and MongoDB
    Args:
        photo_id: MongoDB ObjectId as string
        permanent: If True, physically delete from S3, else soft delete
    Returns:
        bool - Success status
    """
    try:
        # Get metadata
        photo_doc = profile_photos_collection.find_one({'_id': ObjectId(photo_id)})
        
        if not photo_doc:
            logger.warning(f"Photo {photo_id} not found")
            return False
        
        if permanent:
            # Delete from S3
            s3_key = photo_doc['s3_key']
            s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
            
            # Delete from MongoDB
            profile_photos_collection.delete_one({'_id': ObjectId(photo_id)})
            logger.info(f"✓ Permanently deleted photo {photo_id} from S3 and MongoDB")
        else:
            # Soft delete (mark as deleted in MongoDB)
            profile_photos_collection.update_one(
                {'_id': ObjectId(photo_id)},
                {'$set': {'status': 'deleted', 'deleted_at': datetime.utcnow()}}
            )
            logger.info(f"✓ Soft deleted photo {photo_id}")
        
        return True
        
    except ClientError as e:
        logger.error(f"S3 error deleting photo {photo_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error deleting photo {photo_id}: {e}")
        return False

def cleanup_old_deleted_photos(days: int = 30) -> int:
    """
    Permanently delete photos marked as deleted after specified days
    Args:
        days: Number of days after which to permanently delete
    Returns:
        Number of photos cleaned up
    """
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Find deleted photos older than cutoff
        deleted_photos = profile_photos_collection.find({
            'status': 'deleted',
            'deleted_at': {'$lt': cutoff_date}
        })
        
        cleanup_count = 0
        for photo in deleted_photos:
            if delete_profile_photo_s3(str(photo['_id']), permanent=True):
                cleanup_count += 1
        
        logger.info(f"✓ Cleaned up {cleanup_count} old deleted photos")
        return cleanup_count
        
    except Exception as e:
        logger.error(f"Error cleaning up old photos: {e}")
        return 0

def get_user_storage_usage(user_id: int) -> dict:
    """
    Calculate total storage used by user's photos
    Args:
        user_id: User ID from MySQL
    Returns:
        Dictionary with storage stats
    """
    try:
        pipeline = [
            {'$match': {'user_id': int(user_id), 'status': 'active'}},
            {'$group': {
                '_id': None,
                'total_size': {'$sum': '$file_size'},
                'photo_count': {'$sum': 1}
            }}
        ]
        
        result = list(profile_photos_collection.aggregate(pipeline))
        
        if result:
            return {
                'user_id': user_id,
                'total_size_bytes': result[0]['total_size'],
                'total_size_mb': round(result[0]['total_size'] / (1024 * 1024), 2),
                'photo_count': result[0]['photo_count']
            }
        else:
            return {
                'user_id': user_id,
                'total_size_bytes': 0,
                'total_size_mb': 0.0,
                'photo_count': 0
            }
        
    except Exception as e:
        logger.error(f"Error calculating storage for user {user_id}: {e}")
        return None

def validate_and_resize_photo(photo_base64: str, max_width: int = 1024, max_height: int = 1024) -> str:
    """
    Validate and optionally resize photo before upload
    Args:
        photo_base64: Base64 encoded image
        max_width: Maximum width in pixels
        max_height: Maximum height in pixels
    Returns:
        Resized base64 image or original if no resize needed
    Note: Requires Pillow library
    """
    try:
        from PIL import Image
        from io import BytesIO
        
        # Remove base64 prefix
        if 'base64,' in photo_base64:
            header, photo_base64 = photo_base64.split('base64,')
        else:
            header = None
        
        # Decode
        photo_bytes = base64.b64decode(photo_base64)
        image = Image.open(BytesIO(photo_bytes))
        
        # Check if resize needed
        if image.width > max_width or image.height > max_height:
            image.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)
            
            # Convert back to base64
            buffer = BytesIO()
            image.save(buffer, format=image.format or 'JPEG')
            resized_bytes = buffer.getvalue()
            resized_base64 = base64.b64encode(resized_bytes).decode('utf-8')
            
            if header:
                return f"{header}base64,{resized_base64}"
            return resized_base64
        
        return photo_base64 if not header else f"{header}base64,{photo_base64}"
        
    except ImportError:
        logger.warning("Pillow not installed, skipping resize")
        return photo_base64
    except Exception as e:
        logger.error(f"Error resizing photo: {e}")
        return photo_base64

@require_http_methods(["POST"])
@csrf_exempt
def Register_User(request):
    """
    Register a new user with profile photo stored in AWS S3 and face embedding
    profile_photo_id = registration capture photo (PERMANENT)
    face_embedding_id = embedding from registration photo (PERMANENT)
    edited_photo_id = NULL (no edited photo yet)
    photo_code = 0 (using registration photo)
    """
    create_user_table()
    
    try:
        data = json.loads(request.body)
        logging.debug(f"Received registration request")
        
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            return JsonResponse(
                {"Error": "Expected a single user object, not a list"}, 
                status=400
            )
    except json.JSONDecodeError as e:
        return JsonResponse({"Error": "Invalid JSON format"}, status=400)

    # Required fields validation
    required_fields = ['full_name', 'email', 'password', 'profile_photo']
    missing_fields = [
        field for field in required_fields 
        if field not in data or not data[field]
    ]
    
    if missing_fields:
        return JsonResponse(
            {"Error": f"Missing required fields: {', '.join(missing_fields)}"}, 
            status=400
        )

    # Field validations
    if len(data['full_name']) > 100:
        return JsonResponse({"Error": "full_name must be max 100 characters"}, status=400)
    
    if len(data['email']) > 100:
        return JsonResponse({"Error": "email must be max 100 characters"}, status=400)
    
    # Password validation
    is_valid, error_message = validate_password(data['password'])
    if not is_valid:
        return JsonResponse({"Error": error_message}, status=400)

    # Optional fields with defaults
    optional_fields = {
        'phone_number': None,
        'address': None,
        'country': None,
        'status_code': 'u',
        'status': 1,
        'country_code': None,
        'languages': 'English',
        'agreeToTerms': 0
    }
    for field in optional_fields:
        if field not in data or data[field] == '':
            data[field] = optional_fields[field]

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Check if email exists
                cursor.execute(
                    "SELECT COUNT(*) FROM tbl_Users WHERE email = %s AND Status = 1", 
                    [data['email']]
                )
                if cursor.fetchone()[0] > 0:
                    return JsonResponse({"Error": "Email already exists"}, status=400)

                # Insert user
                insert_query = """
                    INSERT INTO tbl_Users (
                        full_name, email, password, phone_number, address, country, 
                        Status, status_Code, country_code, languages, agreeToTerms, 
                        profile_photo_id, edited_photo_id, photo_code, face_embedding_id, 
                        Created_At, Updated_At
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NULL)
                """
                values = [
                    data['full_name'], data['email'], data['password'],
                    data['phone_number'], data['address'], data['country'],
                    data['status'], data['status_code'], data['country_code'],
                    data['languages'], data['agreeToTerms'], 
                    None,  # profile_photo_id
                    None,  # edited_photo_id
                    0,     # photo_code = 0 (using registration photo)
                    None   # face_embedding_id
                ]
                
                cursor.execute(insert_query, values)
                cursor.execute("SELECT LAST_INSERT_ID()")
                user_id = cursor.fetchone()[0]
                
                logger.info(f"✓ User created with ID: {user_id}")
                
                # ============================================================
                # STORE REGISTRATION PHOTO IN S3 (PERMANENT)
                # ============================================================
                photo_result = None
                embedding_id = None
                
                if data.get('profile_photo'):
                    logger.info(f"Storing registration photo for user {user_id}...")
                    photo_result = store_profile_photo_s3(user_id, data['profile_photo'])
                    
                    if photo_result:
                        # Update profile_photo_id (PERMANENT registration photo)
                        cursor.execute(
                            """UPDATE tbl_Users 
                               SET profile_photo_id = %s, photo_code = 0 
                               WHERE ID = %s""",
                            [photo_result['photo_id'], user_id]
                        )
                        logger.info(f"✓ Registration photo ID {photo_result['photo_id']} stored for user {user_id}")
                        
                        # ====================================================
                        # GENERATE FACE EMBEDDING (PERMANENT)
                        # ====================================================
                        if FACE_RECOGNITION_ENABLED:
                            try:
                                logger.info(f"Generating face embedding for user {user_id}...")
                                embedding_id = process_profile_photo_embedding(
                                    user_id, 
                                    photo_result['photo_id']
                                )
                                
                                if embedding_id:
                                    cursor.execute(
                                        "UPDATE tbl_Users SET face_embedding_id = %s WHERE ID = %s",
                                        [embedding_id, user_id]
                                    )
                                    logger.info(f"✓✓✓ Face embedding {embedding_id} generated for user {user_id}")
                                else:
                                    logger.warning(f"⚠ Failed to generate face embedding for user {user_id}")
                                    
                            except Exception as emb_error:
                                logger.error(f"✗ Error generating face embedding: {emb_error}")
                        else:
                            logger.warning("⚠ Face recognition disabled - skipping embedding")
                    else:
                        logger.warning(f"⚠ Failed to store photo for user {user_id}")

    except Exception as e:
        logger.error(f"✗ Registration error: {e}")
        return JsonResponse({"Error": "Registration failed"}, status=500)

    # ========================================================================
    # RESPONSE
    # ========================================================================
    response_data = {
        "Message": "User registered successfully",
        "User_Id": user_id,
        "Email": data['email'],
        "Full_Name": data['full_name']
    }
    
    if photo_result:
        response_data.update({
            "Photo_Id": photo_result['photo_id'],
            "Photo_URL": photo_result['s3_url'],
            "Photo_Size_Bytes": photo_result['file_size'],
            "Photo_Code": 0,
            "Photo_Type": "registration"
        })
        
    if embedding_id:
        response_data.update({
            "Embedding_Id": embedding_id,
            "Face_Recognition_Enabled": True,
            "Face_Login_Available": True
        })
    else:
        response_data.update({
            "Face_Recognition_Enabled": False
        })
    
    logger.info(f"✓✓✓ Registration completed for user {user_id}")
    return JsonResponse(response_data, status=201)

@require_http_methods(["GET"])
@csrf_exempt
def Get_Profile_Photo(request, photo_id):
    """
    Get profile photo by MongoDB ObjectId
    GET /api/profile-photo/<photo_id>/
    """
    try:
        if not photo_id:
            return JsonResponse({"Error": "Photo ID is required"}, status=400)
        
        # Get base64 or URL based on query parameter
        return_base64 = request.GET.get('base64', 'true').lower() == 'true'
        
        photo_data = get_profile_photo_s3(photo_id, return_base64=return_base64)
        
        if photo_data:
            return JsonResponse({
                "success": True,
                "photo_id": photo_id,
                "photo": photo_data.get('photo_data'),  # Base64 if requested
                "s3_url": photo_data['s3_url'],
                "content_type": photo_data['content_type'],
                "file_size": photo_data['file_size']
            }, status=200)
        else:
            return JsonResponse({"Error": "Photo not found"}, status=404)
            
    except Exception as e:
        logger.error(f"Error in Get_Profile_Photo: {e}")
        return JsonResponse({"Error": "Failed to retrieve photo"}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def Get_User_Profile_Photo(request, user_id):
    """
    Get profile photo by user ID
    GET /api/user-photo/<user_id>/
    """
    try:
        if not user_id:
            return JsonResponse({"Error": "User ID is required"}, status=400)
        
        # Check if user exists
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM tbl_Users WHERE id = %s AND Status = 1",
                [user_id]
            )
            if not cursor.fetchone():
                return JsonResponse({"Error": "User not found"}, status=404)
        
        # Get photo
        return_base64 = request.GET.get('base64', 'true').lower() == 'true'
        photo_data = get_profile_photo_by_user_id(user_id, return_base64=return_base64)
        
        if photo_data:
            return JsonResponse({
                "success": True,
                "user_id": user_id,
                "photo_id": photo_data['photo_id'],
                "photo": photo_data.get('photo_data'),  # Base64 if requested
                "s3_url": photo_data['s3_url'],
                "content_type": photo_data['content_type'],
                "file_size": photo_data['file_size']
            }, status=200)
        else:
            return JsonResponse({"Error": "User photo not found"}, status=404)
            
    except Exception as e:
        logger.error(f"Error in Get_User_Profile_Photo: {e}")
        return JsonResponse({"Error": "Failed to retrieve user photo"}, status=500)

@require_http_methods(["PUT", "POST"])
@csrf_exempt
def Update_Profile_Photo(request, user_id):
    """
    Update user's DISPLAY profile photo only
    Stores in edited_photo_id, sets photo_code = 1
    DOES NOT touch profile_photo_id or face_embedding_id (they are PERMANENT)
    
    PUT/POST /api/update-photo/<user_id>/
    """
    try:
        data = json.loads(request.body)
        logger.info(f"Photo update request for user {user_id}")
        
        if 'profile_photo' not in data or not data['profile_photo']:
            return JsonResponse({"Error": "profile_photo is required"}, status=400)
        
        # ====================================================================
        # GET CURRENT USER DATA
        # ====================================================================
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT profile_photo_id, edited_photo_id, photo_code, face_embedding_id 
                   FROM tbl_Users WHERE ID = %s AND Status = 1""",
                [user_id]
            )
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"User {user_id} not found")
                return JsonResponse({"Error": "User not found"}, status=404)
            
            profile_photo_id = result[0]      # Registration photo (PERMANENT)
            old_edited_photo_id = result[1]   # Previous edited photo (can be deleted)
            old_photo_code = result[2]
            face_embedding_id = result[3]     # PERMANENT - don't touch
            
            logger.info(f"User {user_id}: profile_photo_id={profile_photo_id}, old_edited_photo_id={old_edited_photo_id}, photo_code={old_photo_code}")
        
        # ====================================================================
        # UPLOAD NEW DISPLAY PHOTO TO S3
        # ====================================================================
        logger.info(f"Uploading new display photo for user {user_id}...")
        photo_result = store_profile_photo_s3(user_id, data['profile_photo'])
        
        if not photo_result:
            logger.error(f"Failed to store new photo for user {user_id}")
            return JsonResponse({"Error": "Failed to store new photo"}, status=500)
        
        logger.info(f"✓ New display photo stored: {photo_result['photo_id']}")
        
        # ====================================================================
        # SOFT DELETE OLD EDITED PHOTO (if exists)
        # ====================================================================
        if old_edited_photo_id:
            logger.info(f"Soft deleting old edited photo: {old_edited_photo_id}")
            delete_profile_photo_s3(old_edited_photo_id, permanent=False)
        
        # ====================================================================
        # UPDATE USER RECORD - edited_photo_id and photo_code ONLY
        # profile_photo_id and face_embedding_id remain UNTOUCHED
        # ====================================================================
        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE tbl_Users 
                   SET edited_photo_id = %s, 
                       photo_code = 1, 
                       Updated_At = NOW() 
                   WHERE ID = %s""",
                [photo_result['photo_id'], user_id]
            )
            logger.info(f"✓ User {user_id} updated: edited_photo_id={photo_result['photo_id']}, photo_code=1")
        
        # ====================================================================
        # RESPONSE (NO embedding changes)
        # ====================================================================
        response_data = {
            "Message": "Profile photo updated successfully (display only)",
            "User_Id": user_id,
            "Edited_Photo_Id": photo_result['photo_id'],
            "Photo_URL": photo_result['s3_url'],
            "Photo_Size_Bytes": photo_result['file_size'],
            "Photo_Code": 1,
            "Photo_Type": "edited",
            "Old_Edited_Photo_Deleted": bool(old_edited_photo_id),
            "Registration_Photo_Id": profile_photo_id,
            "Face_Embedding_Id": face_embedding_id,
            "Note": "Face recognition still uses registration photo"
        }
        
        logger.info(f"✓✓✓ Display photo update completed for user {user_id}")
        return JsonResponse(response_data, status=200)
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON format")
        return JsonResponse({"Error": "Invalid JSON format"}, status=400)
    except Exception as e:
        logger.error(f"✗ Error updating profile photo: {e}")
        return JsonResponse({"Error": "Failed to update profile photo"}, status=500)

@require_http_methods(["DELETE"])
@csrf_exempt
def Delete_Profile_Photo(request, user_id):
    """
    Delete user's EDITED profile photo only
    Sets photo_code = 0 (fallback to registration photo)
    
    DOES NOT touch profile_photo_id or face_embedding_id (they are PERMANENT)
    
    DELETE /api/delete-photo/<user_id>/?permanent=false
    """
    try:
        permanent = request.GET.get('permanent', 'false').lower() == 'true'
        logger.info(f"Delete edited photo request for user {user_id}, permanent={permanent}")
        
        # ====================================================================
        # GET CURRENT USER DATA
        # ====================================================================
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT profile_photo_id, edited_photo_id, photo_code, face_embedding_id 
                   FROM tbl_Users WHERE ID = %s AND Status = 1""",
                [user_id]
            )
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"User {user_id} not found")
                return JsonResponse({"Error": "User not found"}, status=404)
            
            profile_photo_id = result[0]   # PERMANENT - don't touch
            edited_photo_id = result[1]    # This is what we delete
            current_photo_code = result[2]
            face_embedding_id = result[3]  # PERMANENT - don't touch
            
            logger.info(f"User {user_id}: profile_photo_id={profile_photo_id}, edited_photo_id={edited_photo_id}, photo_code={current_photo_code}")
        
        # ====================================================================
        # CHECK IF THERE'S AN EDITED PHOTO TO DELETE
        # ====================================================================
        if not edited_photo_id:
            logger.info(f"User {user_id} has no edited photo to delete")
            return JsonResponse({
                "Message": "No edited photo to delete. Already using registration photo.",
                "User_Id": user_id,
                "Photo_Code": 0,
                "Photo_Type": "registration",
                "Active_Photo_Id": profile_photo_id
            }, status=200)
        
        if current_photo_code == 0:
            logger.info(f"User {user_id} is already using registration photo")
            # Still delete the edited photo from storage
        
        # ====================================================================
        # DELETE EDITED PHOTO FROM S3
        # ====================================================================
        logger.info(f"Deleting edited photo {edited_photo_id} from S3...")
        photo_deleted = delete_profile_photo_s3(edited_photo_id, permanent=permanent)
        
        if not photo_deleted:
            logger.error(f"Failed to delete edited photo {edited_photo_id}")
            return JsonResponse({"Error": "Failed to delete photo"}, status=500)
        
        logger.info(f"✓ Edited photo deleted")
        
        # ====================================================================
        # UPDATE USER RECORD - ONLY edited_photo_id and photo_code
        # profile_photo_id and face_embedding_id remain UNTOUCHED
        # ====================================================================
        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE tbl_Users 
                   SET edited_photo_id = NULL, 
                       photo_code = 0, 
                       Updated_At = NOW() 
                   WHERE ID = %s""",
                [user_id]
            )
            logger.info(f"✓ User {user_id} updated: edited_photo_id=NULL, photo_code=0")
        
        # ====================================================================
        # RESPONSE
        # ====================================================================
        response_data = {
            "Message": "Edited photo deleted. Now using registration photo.",
            "User_Id": user_id,
            "Edited_Photo_Deleted": True,
            "Photo_Code": 0,
            "Photo_Type": "registration",
            "Active_Photo_Id": profile_photo_id,
            "Permanent": permanent,
            "Delete_Type": "Permanent" if permanent else "Soft Delete",
            "Registration_Photo_Id": profile_photo_id,
            "Face_Embedding_Id": face_embedding_id,
            "Note": "Registration photo and face embedding remain unchanged"
        }
        
        logger.info(f"✓✓✓ Edited photo deletion completed for user {user_id}")
        return JsonResponse(response_data, status=200)
        
    except Exception as e:
        logger.error(f"✗ Error deleting edited photo: {e}")
        return JsonResponse({"Error": "Failed to delete photo"}, status=500)

@require_http_methods(["GET"])
@csrf_exempt
def Get_User_Storage_Stats(request, user_id):
    """
    Get storage statistics for a user
    GET /api/user-storage/<user_id>/
    """
    try:
        storage_stats = get_user_storage_usage(user_id)
        
        if storage_stats:
            return JsonResponse({
                "success": True,
                "user_id": user_id,
                "storage": storage_stats
            }, status=200)
        else:
            return JsonResponse({"Error": "Failed to calculate storage"}, status=500)
            
    except Exception as e:
        logger.error(f"Error getting storage stats: {e}")
        return JsonResponse({"Error": "Failed to get storage stats"}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def Cleanup_Deleted_Photos(request):
    """
    Admin endpoint to cleanup old deleted photos
    POST /api/admin/cleanup-photos/
    Body: {"days": 30}
    """
    try:
        data = json.loads(request.body)
        days = int(data.get('days', 30))
        
        # Add admin authentication here
        # if not request.user.is_admin:
        #     return JsonResponse({"Error": "Unauthorized"}, status=403)
        
        cleanup_count = cleanup_old_deleted_photos(days)
        
        return JsonResponse({
            "Message": f"Cleaned up {cleanup_count} old photos",
            "Cleanup_Count": cleanup_count,
            "Days_Threshold": days
        }, status=200)
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")
        return JsonResponse({"Error": "Cleanup failed"}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def Login_User(request):
    create_user_table()
    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
            logging.debug(f"Unwrapped list to: {json.dumps(data, indent=2)}")
        elif isinstance(data, list):
            logging.error("Expected single login object, got list")
            return JsonResponse({"Error": "Expected a single login object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)
    required_fields = ['Credential', 'Password']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or data[field] == ""]
    if missing_fields:
        logging.error(f"Missing/null fields: {', '.join(missing_fields)}")
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    credential = data['Credential']
    password = data['Password']

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT ID, password, full_name
                FROM tbl_Users
                WHERE (email = %s OR phone_number = %s) AND status_Code = 'u' AND Status = 1
            """, [credential, credential])
            row = cursor.fetchone()
            if row and row[1] == password:
                user_id = row[0]
                full_name = row[2]
                request.session['User_Id'] = user_id
                request.session['User_Type'] = 'user'
                request.session.set_expiry(86400)
                return JsonResponse({
                    "Message": "Login successful",
                    "Entity_Type": "user",
                    "Id": user_id,
                    "Name": full_name,
                    "Session_Timeout": 86400
                }, status=SUCCESS_STATUS)
            logging.error(f"Login failed for credential: {credential}")
            return JsonResponse({"Error": "Invalid credential or password, or user inactive"}, status=UNAUTHORIZED_STATUS)
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Logout_User(request):
    try:
        request.session.flush()
        return JsonResponse({"Message": "Logout successful"}, status=SUCCESS_STATUS)
    except Exception as e:
        logging.error(f"Logout error: {e}")
        return JsonResponse({"Error": f"Logout error: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Forgot_Password(request):
    create_user_table()
    create_otp_table()
    
    try:
        data = json.loads(request.body)
        email = data.get('email')
        
        if not email:
            return JsonResponse({"Error": "email is required"}, status=400)

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM tbl_Users WHERE email = %s AND Status = 1", [email])
            if cursor.fetchone()[0] == 0:
                return JsonResponse({"Error": "email not found or user inactive"}, status=404)

        ist_timezone = pytz.timezone('Asia/Kolkata')
        current_time = timezone.now().astimezone(ist_timezone)
        expires_at = current_time + timezone.timedelta(minutes=10)
        
        OTP = generate_OTP()
        
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM tbl_OTP_Reset WHERE email = %s", [email])
            
            cursor.execute("""
                INSERT INTO tbl_OTP_Reset (email, otp, expires_at)
                VALUES (%s, %s, %s)
            """, [email, OTP, expires_at])

        if not send_OTP_email(email, OTP):
            return JsonResponse({"Error": "Failed to send OTP to email"}, status=500)

        return JsonResponse({"Message": "OTP sent to email. Please verify to reset password."}, status=200)
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return JsonResponse({"Error": str(e)}, status=500)

@require_http_methods(["POST"])
@csrf_exempt  
def Reset_Password(request):
    create_user_table()
    create_otp_table()

    try:
        data = json.loads(request.body)
        received_OTP = data.get('OTP') or data.get('otp')
        password = data.get('password')
        email = data.get('email')

        if not received_OTP or not password or not email:
            return JsonResponse({"Error": "OTP, password, and email are required"}, status=400)

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT otp, expires_at FROM tbl_OTP_Reset 
                WHERE email = %s AND used = 0 
                ORDER BY created_at DESC
            """, [email])
            
            otp_record = cursor.fetchone()
            
            if not otp_record:
                return JsonResponse({"Error": "No pending OTP found. Please request again."}, status=400)
            
            stored_otp, expires_at = otp_record
            
            ist_timezone = pytz.timezone('Asia/Kolkata')
            current_time = timezone.now().astimezone(ist_timezone)
            
            if timezone.is_naive(expires_at):
                expires_at = ist_timezone.localize(expires_at)
            elif expires_at.tzinfo != ist_timezone:
                expires_at = expires_at.astimezone(ist_timezone)
            
            if current_time > expires_at:
                cursor.execute("DELETE FROM tbl_OTP_Reset WHERE email = %s", [email])
                return JsonResponse({"Error": "OTP has expired"}, status=400)
            
            if received_OTP != stored_otp:
                return JsonResponse({"Error": "Invalid OTP"}, status=401)

        is_valid, error_message = validate_password(password)
        if not is_valid:
            return JsonResponse({"Error": error_message}, status=400)

        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE tbl_Users
                    SET password = %s, Updated_At = NOW()
                    WHERE email = %s AND Status = 1
                """, [password, email])
                
                if cursor.rowcount == 0:
                    return JsonResponse({"Error": "User not found or inactive"}, status=404)
                
                cursor.execute("UPDATE tbl_OTP_Reset SET used = 1 WHERE email = %s", [email])

        return JsonResponse({"Message": "Password reset successfully"}, status=200)
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return JsonResponse({"Error": str(e)}, status=500)

@require_http_methods(["POST"])
@csrf_exempt
def Add_User(request):
    create_user_table()

    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
            logging.debug(f"Unwrapped list to: {json.dumps(data, indent=2)}")
        elif isinstance(data, list):
            logging.error("Expected single user object, got list")
            return JsonResponse({"Error": "Expected a single user object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    required_fields = ['full_name', 'email', 'password']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or (isinstance(data[field], str) and data[field] == "")]
    if missing_fields:
        logging.error(f"Missing/null fields: {', '.join(missing_fields)}")
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    if len(data['full_name']) > 100:
        logging.error(f"full_name too long: {data['full_name']}")
        return JsonResponse({"Error": "full_name must be max 100 characters"}, status=BAD_REQUEST_STATUS)
    
    if len(data['email']) > 100:
        logging.error(f"email too long: {data['email']}")
        return JsonResponse({"Error": "email must be max 100 characters"}, status=BAD_REQUEST_STATUS)
    
    if len(data['password']) > 255:
        logging.error("password too long")
        return JsonResponse({"Error": "password must be max 255 characters"}, status=BAD_REQUEST_STATUS)
    
    if data.get('phone_number') and (len(data['phone_number']) > 20 or not re.match(r'^\d+$', data['phone_number'])):
        logging.error(f"Invalid phone_number: {data['phone_number']}")
        return JsonResponse({"Error": "phone_number must be digits only and max 20 characters"}, status=BAD_REQUEST_STATUS)
    
    if data.get('address') and len(data['address']) > 255:
        logging.error(f"address too long: {data['address']}")
        return JsonResponse({"Error": "address must be max 255 characters"}, status=BAD_REQUEST_STATUS)
    
    if data.get('country') and len(data['country']) > 50:
        logging.error(f"country too long: {data['country']}")
        return JsonResponse({"Error": "country must be max 50 characters"}, status=BAD_REQUEST_STATUS)
    
    if data.get('country_code') and len(data['country_code']) > 10:
        logging.error(f"country_code too long: {data['country_code']}")
        return JsonResponse({"Error": "country_code must be max 10 characters"}, status=BAD_REQUEST_STATUS)
    
    if data.get('languages') and len(data['languages']) > 100:
        logging.error(f"languages too long: {data['languages']}")
        return JsonResponse({"Error": "languages must be max 100 characters"}, status=BAD_REQUEST_STATUS)

    is_valid, error_message = validate_password(data['password'])
    if not is_valid:
        logging.error(f"Password validation failed: {error_message}")
        return JsonResponse({"Error": error_message}, status=BAD_REQUEST_STATUS)

    optional_fields = {
        'phone_number': None,
        'address': None,
        'country': None,
        'status_code': 'u',
        'status': 1,
        'country_code': None,
        'languages': None,
        'agreeToTerms': 0
    }
    for field in optional_fields:
        data[field] = data.get(field, optional_fields[field])

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM tbl_Users WHERE email = %s AND Status = 1", [data['email']])
                if cursor.fetchone()[0] > 0:
                    logging.error(f"email {data['email']} exists")
                    return JsonResponse({"Error": "email already exists"}, status=BAD_REQUEST_STATUS)

                insert_query = """
                    INSERT INTO tbl_Users (
                        full_name, email, password, phone_number, address, country, Status, status_Code,
                        country_code, languages, agreeToTerms, Created_At, Updated_At
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NULL)
                """
                values = [
                    data['full_name'],
                    data['email'],
                    data['password'],
                    data['phone_number'],
                    data['address'],
                    data['country'],
                    data['status'],
                    data['status_code'],
                    data['country_code'],
                    data['languages'],
                    data['agreeToTerms'],
                ]
                cursor.execute(insert_query, values)
                cursor.execute("SELECT LAST_INSERT_ID()")
                user_id = cursor.fetchone()[0]

    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": "User added successfully", "User_Id": user_id}, status=CREATED_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def List_All_Users(request):
    create_user_table()

    try:
        with connection.cursor() as cursor:
            select_query = """
                SELECT ID, full_name, email, phone_number, address, country, Status, status_Code,
                       country_code, languages, agreeToTerms, Created_At, Updated_At
                FROM tbl_Users
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            users = [
                dict(zip([
                    'ID', 'full_name', 'email', 'phone_number', 'address', 'country', 'Status',
                    'status_Code', 'country_code', 'languages', 'agreeToTerms', 'Created_At', 'Updated_At'
                ], row))
                for row in rows
            ]
    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse(users, safe=False, status=SUCCESS_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def Get_User(request, id):
    create_user_table()
    try:
        with connection.cursor() as cursor:
            # ✅ UPDATED: Include photo columns
            select_query = """
                SELECT ID, full_name, email, phone_number, address, country, Status, status_Code,
                       country_code, languages, agreeToTerms, 
                       profile_photo_id, edited_photo_id, photo_code, face_embedding_id,
                       Created_At, Updated_At
                FROM tbl_Users
                WHERE ID = %s
            """
            cursor.execute(select_query, [id])
            row = cursor.fetchone()
    except Exception as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)
    if row:
        user = {
            'ID': row[0],
            'full_name': row[1],
            'email': row[2],
            'phone_number': row[3],
            'address': row[4],
            'country': row[5],
            'Status': row[6],
            'status_Code': row[7],
            'country_code': row[8],
            'languages': row[9],
            'agreeToTerms': row[10],
            'profile_photo_id': row[11],    # ✅ NEW
            'edited_photo_id': row[12],     # ✅ NEW
            'photo_code': row[13],          # ✅ NEW
            'face_embedding_id': row[14],   # ✅ NEW
            'Created_At': row[15],
            'Updated_At': row[16]
        }
        return JsonResponse(user, status=SUCCESS_STATUS)
    logging.error(f"User ID {id} not found")
    return JsonResponse({"Error": "User not found"}, status=NOT_FOUND_STATUS)

@require_http_methods(["PUT", "PATCH"])
@csrf_exempt
def Update_User(request, id):
    create_user_table()

    # ---------- Parse & unwrap ----------
    try:
        data = json.loads(request.body or "{}")
        if isinstance(data, list):
            if len(data) != 1:
                return JsonResponse({"Error": "Expected a single user object"}, status=BAD_REQUEST_STATUS)
            data = data[0]
    except json.JSONDecodeError:
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    # ---------- Normalize inputs (avoid UI object shapes) ----------
    # country may arrive as {label, value} or {code, name}
    if isinstance(data.get('country'), dict):
        c = data['country']
        data['country'] = c.get('value') or c.get('code') or c.get('name') or None

    # languages may arrive as list
    if isinstance(data.get('languages'), list):
        data['languages'] = ",".join([str(x).strip() for x in data['languages'] if str(x).strip()]) or None

    # phone: allow +, spaces, hyphens from UI; store digits only (or keep raw; choose one)
    if data.get('phone_number'):
        raw = str(data['phone_number'])
        digits_only = re.sub(r'\D', '', raw)
        data['phone_number'] = digits_only

    # ---------- Validate required ----------
    required_fields = ['full_name', 'email']
    missing = [f for f in required_fields if not str(data.get(f) or "").strip()]
    if missing:
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing)}"}, status=BAD_REQUEST_STATUS)

    # ---------- Light validation (lengths) ----------
    if len(data['full_name']) > 100:  return JsonResponse({"Error":"full_name must be max 100 characters"}, status=BAD_REQUEST_STATUS)
    if len(data['email']) > 100:      return JsonResponse({"Error":"email must be max 100 characters"}, status=BAD_REQUEST_STATUS)
    if data.get('password') and len(data['password']) > 255:
        return JsonResponse({"Error":"password must be max 255 characters"}, status=BAD_REQUEST_STATUS)
    if data.get('phone_number') and len(data['phone_number']) > 20:
        return JsonResponse({"Error":"phone_number must be digits only and max 20 characters"}, status=BAD_REQUEST_STATUS)
    if data.get('address') and len(data['address']) > 255:
        return JsonResponse({"Error":"address must be max 255 characters"}, status=BAD_REQUEST_STATUS)
    if data.get('country') and len(str(data['country'])) > 50:
        return JsonResponse({"Error":"country must be max 50 characters"}, status=BAD_REQUEST_STATUS)
    if data.get('country_code') and len(str(data['country_code'])) > 10:
        return JsonResponse({"Error":"country_code must be max 10 characters"}, status=BAD_REQUEST_STATUS)
    if data.get('languages') and len(str(data['languages'])) > 100:
        return JsonResponse({"Error":"languages must be max 100 characters"}, status=BAD_REQUEST_STATUS)

    # optional defaults (don’t clobber with None unless provided)
    defaults = {
        'password': None,
        'phone_number': None,
        'address': None,
        'country': None,
        'status': 1,
        'status_code': 'u',
        'country_code': None,
        'languages': None,
        'agreeToTerms': 0
    }
    for k, v in defaults.items():
        if k not in data:
            data[k] = v

    # password rule
    if data.get('password'):
        ok, msg = validate_password(data['password'])
        if not ok:
            return JsonResponse({"Error": msg}, status=BAD_REQUEST_STATUS)

    # ---------- Execute update & return updated row ----------
    try:
        with transaction.atomic():
            with connection.cursor() as cur:
                # unique email (except self)
                cur.execute("SELECT COUNT(*) FROM tbl_Users WHERE email=%s AND ID<>%s", [data['email'], id])
                if cur.fetchone()[0] > 0:
                    return JsonResponse({"Error": "email already exists"}, status=BAD_REQUEST_STATUS)

                cur.execute("""
                    UPDATE tbl_Users
                    SET full_name = %s,
                        email = %s,
                        password = COALESCE(%s, password),
                        phone_number = %s,
                        address = %s,
                        country = %s,
                        Status = %s,
                        status_Code = %s,
                        country_code = %s,
                        languages = %s,
                        agreeToTerms = %s,
                        Updated_At = NOW()
                    WHERE ID = %s
                """, [
                    data['full_name'],
                    data['email'],
                    data['password'],
                    data['phone_number'],
                    data['address'],
                    data['country'],
                    data['status'],
                    data['status_code'],
                    data['country_code'],
                    data['languages'],
                    data['agreeToTerms'],
                    id
                ])

                if cur.rowcount == 0:
                    return JsonResponse({"Error": "User not found"}, status=NOT_FOUND_STATUS)

                # 👉 return the fresh record so the FE can replace its state
                cur.execute("""
                    SELECT ID, full_name, email, phone_number, address, country,
                           Status, status_Code, country_code, languages, agreeToTerms,
                           Created_At, Updated_At
                    FROM tbl_Users WHERE ID = %s
                """, [id])
                r = cur.fetchone()
    except Exception as e:
        logging.exception("Update failed")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    user = {
        "id": r[0],
        "full_name": r[1],
        "email": r[2],
        "phone_number": r[3],
        "address": r[4],
        "country": r[5],
        "status": bool(r[6]),
        "status_code": r[7],
        "country_code": r[8],
        "languages": (r[9].split(",") if r[9] else []),  # FE-friendly
        "agreeToTerms": bool(r[10]),
        "created_at": r[11],
        "updated_at": r[12],
    }
    return JsonResponse({"user": user, "Message": "User updated successfully"}, status=SUCCESS_STATUS)
    
@require_http_methods(["DELETE"])
@csrf_exempt
def Delete_User(request, id):
    create_user_table()

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                select_query = f"SELECT full_name FROM tbl_Users WHERE ID = %s AND Status = 1"
                cursor.execute(select_query, [id])
                row = cursor.fetchone()
                if not row:
                    logging.error(f"User ID {id} not found or inactive")
                    return JsonResponse({"Error": "User not found or inactive"}, status=NOT_FOUND_STATUS)

                delete_action = f"UPDATE tbl_Users SET Status = 0, Updated_At = NOW() WHERE ID = %s AND Status = 1"
                cursor.execute(delete_action, [id])
                if cursor.rowcount == 0:
                    logging.error(f"User ID {id} not found or inactive")
                    return JsonResponse({"Error": "User not found or inactive"}, status=NOT_FOUND_STATUS)

    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": f"User ID {id} deleted successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Validate_User_Data(request):
    create_user_table()
    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
            logging.debug(f"Unwrapped list to: {json.dumps(data, indent=2)}")
        elif isinstance(data, list):
            logging.error("Expected single validation object, got list")
            return JsonResponse({"Error": "Expected a single validation object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)
    
    validation_results = {
        "Email": {"is_valid": True, "message": ""},  
        "password": {"is_valid": True, "message": ""}
    }
    
    user_exists = False
    if 'email' in data and data['email']:
        if len(data['email']) > 100:
            logging.error(f"Email too long: {data['email']}")
            validation_results["Email"] = {"is_valid": False, "message": "Email must be max 100 characters"}
        else:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM tbl_Users WHERE email = %s AND Status = 1", [data['email']])
                    if cursor.fetchone()[0] > 0:
                        user_exists = True
                        logging.info(f"Email {data['email']} exists in database")
                        validation_results["Email"] = {"is_valid": False, "message": "email already exists"}
                    else:
                        logging.info(f"Email {data['email']} does not exist in database")
                        validation_results["Email"] = {"is_valid": True, "message": "email available"}
            except (ProgrammingError, OperationalError) as e:
                logging.error(f"Database error checking email: {e}")
                return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)
    
    if data.get('password'):
        is_valid_format, format_error_message = validate_password(data['password'])
        if not is_valid_format:
            logging.error(f"Password format validation failed: {format_error_message}")
            validation_results["password"] = {"is_valid": False, "message": format_error_message}
        elif len(data['password']) > 255:
            logging.error("Password too long")
            validation_results["password"] = {"is_valid": False, "message": "Password must be max 255 characters"}
        else:
            if user_exists:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SELECT password FROM tbl_Users WHERE email = %s AND Status = 1", [data['email']])
                        user_record = cursor.fetchone()
                        
                        if user_record:
                            stored_password = user_record[0]
                            
                            if data['password'] == stored_password:
                                logging.info(f"Password matches for email: {data['email']}")
                                validation_results["password"] = {"is_valid": True, "message": "password matches"}
                            else:
                                logging.error(f"Password doesn't match for email: {data['email']}")
                                validation_results["password"] = {"is_valid": False, "message": "password doesn't match"}
                        else:
                            validation_results["password"] = {"is_valid": True, "message": "password format valid"}
                except (ProgrammingError, OperationalError) as e:
                    logging.error(f"Database error checking password: {e}")
                    return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)
            else:
                logging.info(f"Password format valid but user doesn't exist")
                validation_results["password"] = {"is_valid": True, "message": "password format valid"}
    
    return JsonResponse(validation_results, status=SUCCESS_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def Get_Active_Profile_Photo(request, user_id):
    """
    Get the ACTIVE profile photo based on photo_code
    photo_code = 0 → registration photo (profile_photo_id)
    photo_code = 1 → edited photo (edited_photo_id)
    
    GET /api/user-active-photo/<user_id>/
    """
    try:
        if not user_id:
            return JsonResponse({"Error": "User ID is required"}, status=400)
        
        # Get user photo info
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT profile_photo_id, edited_photo_id, photo_code, face_embedding_id 
                   FROM tbl_Users WHERE ID = %s AND Status = 1""",
                [user_id]
            )
            result = cursor.fetchone()
            
            if not result:
                return JsonResponse({"Error": "User not found"}, status=404)
            
            profile_photo_id = result[0]   # Registration photo (PERMANENT)
            edited_photo_id = result[1]    # Edited display photo
            photo_code = result[2] or 0    # Default to 0 if NULL
            face_embedding_id = result[3]  # For reference
            
            # Determine which photo to return based on photo_code
            if photo_code == 1 and edited_photo_id:
                active_photo_id = edited_photo_id
                photo_type = "edited"
            else:
                active_photo_id = profile_photo_id
                photo_type = "registration"
            
            if not active_photo_id:
                return JsonResponse({
                    "Error": "User has no photo",
                    "User_Id": user_id,
                    "Photo_Code": photo_code
                }, status=404)
        
        # Get photo data from S3
        return_base64 = request.GET.get('base64', 'true').lower() == 'true'
        photo_data = get_profile_photo_s3(active_photo_id, return_base64=return_base64)
        
        if photo_data:
            return JsonResponse({
                "success": True,
                "user_id": user_id,
                "active_photo_id": active_photo_id,
                "photo_code": photo_code,
                "photo_type": photo_type,
                "photo": photo_data.get('photo_data'),
                "s3_url": photo_data['s3_url'],
                "content_type": photo_data['content_type'],
                "file_size": photo_data['file_size'],
                "registration_photo_id": profile_photo_id,
                "edited_photo_id": edited_photo_id,
                "face_embedding_id": face_embedding_id
            }, status=200)
        else:
            return JsonResponse({"Error": "Photo not found in storage"}, status=404)
            
    except Exception as e:
        logger.error(f"Error in Get_Active_Profile_Photo: {e}")
        return JsonResponse({"Error": "Failed to retrieve photo"}, status=500)
# ============================================================================
# FACE RECOGNITION ENDPOINTS - Add these BEFORE urlpatterns
# ============================================================================

@require_http_methods(["POST"])
@csrf_exempt
def Face_Recognition_Login(request):
    """
    Login using face recognition
    POST /api/auth/face-login/
    Body: {"face_image": "data:image/jpeg;base64,..."}
    """
    try:
        data = json.loads(request.body)
        
        if 'face_image' not in data or not data['face_image']:
            return JsonResponse({"Error": "face_image is required"}, status=400)
        
        if not FACE_RECOGNITION_ENABLED:
            return JsonResponse({"Error": "Face recognition not available"}, status=503)
        
        # Process face recognition
        recognition_result = process_image_for_recognition(data['face_image'])
        
        if not recognition_result.get('success'):
            return JsonResponse({
                "Error": recognition_result.get('error', 'Face recognition failed')
            }, status=400)
        
        if not recognition_result.get('matched'):
            return JsonResponse({
                "Error": "No matching user found",
                "Message": "Face not recognized in the system"
            }, status=404)
        
        # User matched, retrieve user details
        user_id = recognition_result['user_id']
        
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT ID, full_name, email, Status, status_Code
                FROM tbl_Users
                WHERE ID = %s AND Status = 1
            """, [user_id])
            
            user_row = cursor.fetchone()
            
            if not user_row:
                return JsonResponse({"Error": "User not found or inactive"}, status=404)
            
            # Create session
            request.session['User_Id'] = user_id
            request.session['User_Type'] = 'user'
            request.session['Login_Method'] = 'face_recognition'
            request.session.set_expiry(86400)
            
            return JsonResponse({
                "Message": "Face recognition login successful",
                "Login_Method": "Face Recognition",
                "Entity_Type": "user",
                "Id": user_id,
                "Name": user_row[1],
                "Email": user_row[2],
                "Similarity_Score": recognition_result['similarity'],
                "Detection_Score": recognition_result['det_score'],
                "Session_Timeout": 86400
            }, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"Error": "Invalid JSON format"}, status=400)
    except Exception as e:
        logger.error(f"Error in face recognition login: {e}")
        return JsonResponse({"Error": "Face recognition login failed"}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def Verify_User_Face(request, user_id):
    """
    Verify if a face image matches a specific user
    POST /api/user/verify-face/<user_id>/
    Body: {"face_image": "data:image/jpeg;base64,...", "threshold": 0.6}
    """
    try:
        data = json.loads(request.body)
        
        if 'face_image' not in data or not data['face_image']:
            return JsonResponse({"Error": "face_image is required"}, status=400)
        
        if not FACE_RECOGNITION_ENABLED:
            return JsonResponse({"Error": "Face recognition not available"}, status=503)
        
        threshold = float(data.get('threshold', 0.6))
        
        # Convert base64 to numpy array
        image = base64_to_numpy(data['face_image'])
        
        if image is None:
            return JsonResponse({"Error": "Invalid image data"}, status=400)
        
        # Verify face match
        verification_result = verify_face_match(user_id, image, threshold)
        
        if verification_result.get('error'):
            return JsonResponse({"Error": verification_result['error']}, status=400)
        
        return JsonResponse({
            "success": True,
            "verified": verification_result['verified'],
            "user_id": user_id,
            "max_similarity": verification_result['max_similarity'],
            "threshold": verification_result['threshold'],
            "embeddings_compared": verification_result['embeddings_compared']
        }, status=200)
        
    except json.JSONDecodeError:
        return JsonResponse({"Error": "Invalid JSON format"}, status=400)
    except Exception as e:
        logger.error(f"Error verifying face: {e}")
        return JsonResponse({"Error": "Face verification failed"}, status=500)


@require_http_methods(["GET"])
@csrf_exempt
def Get_User_Embeddings(request, user_id):
    """
    Get all face embeddings for a user
    GET /api/user/embeddings/<user_id>/
    """
    try:
        if not FACE_RECOGNITION_ENABLED:
            return JsonResponse({"Error": "Face recognition not available"}, status=503)
        
        embeddings = get_user_embeddings(user_id)
        
        if not embeddings:
            return JsonResponse({
                "Message": "No embeddings found for user",
                "user_id": user_id,
                "embeddings": []
            }, status=200)
        
        # Remove actual embedding vectors (too large)
        simplified_embeddings = []
        for emb in embeddings:
            simplified_embeddings.append({
                'embedding_id': emb['_id'],
                'photo_id': emb['photo_id'],
                'det_score': emb['det_score'],
                'age': emb.get('age'),
                'gender': emb.get('gender'),
                'face_count': emb.get('face_count'),
                'created_at': emb['created_at'],
                'embedding_size': emb['embedding_size']
            })
        
        return JsonResponse({
            "success": True,
            "user_id": user_id,
            "total_embeddings": len(embeddings),
            "embeddings": simplified_embeddings
        }, status=200)
        
    except Exception as e:
        logger.error(f"Error getting user embeddings: {e}")
        return JsonResponse({"Error": "Failed to get embeddings"}, status=500)


@require_http_methods(["GET"])
@csrf_exempt
def Get_Embedding_Statistics(request):
    """
    Get system-wide embedding statistics
    GET /api/admin/embedding-stats/
    """
    try:
        if not FACE_RECOGNITION_ENABLED:
            return JsonResponse({"Error": "Face recognition not available"}, status=503)
        
        stats = get_embedding_stats()
        
        return JsonResponse({
            "success": True,
            "statistics": stats
        }, status=200)
        
    except Exception as e:
        logger.error(f"Error getting embedding stats: {e}")
        return JsonResponse({"Error": "Failed to get statistics"}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def Regenerate_User_Embedding(request, user_id):
    """
    Regenerate face embedding for user's current profile photo
    POST /api/user/regenerate-embedding/<user_id>/
    """
    try:
        if not FACE_RECOGNITION_ENABLED:
            return JsonResponse({"Error": "Face recognition not available"}, status=503)
        
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT profile_photo_id, face_embedding_id FROM tbl_Users WHERE ID = %s AND Status = 1",
                [user_id]
            )
            result = cursor.fetchone()
            
            if not result:
                return JsonResponse({"Error": "User not found"}, status=404)
            
            photo_id = result[0]
            old_embedding_id = result[1]
            
            if not photo_id:
                return JsonResponse({"Error": "User has no profile photo"}, status=404)
            
            # Delete old embedding if exists
            if old_embedding_id:
                delete_face_embedding(old_embedding_id, permanent=False)
            
            # Generate new embedding
            new_embedding_id = process_profile_photo_embedding(user_id, photo_id)
            
            if not new_embedding_id:
                return JsonResponse({"Error": "Failed to generate embedding"}, status=500)
            
            # Update user record
            cursor.execute(
                "UPDATE tbl_Users SET face_embedding_id = %s, Updated_At = NOW() WHERE ID = %s",
                [new_embedding_id, user_id]
            )
            
            return JsonResponse({
                "Message": "Face embedding regenerated successfully",
                "user_id": user_id,
                "embedding_id": new_embedding_id
            }, status=200)
            
    except Exception as e:
        logger.error(f"Error regenerating embedding: {e}")
        return JsonResponse({"Error": "Failed to regenerate embedding"}, status=500)

urlpatterns = [
    path('api/auth/register', Register_User, name='Register_User'),
    path('api/auth/login', Login_User, name='Login_User'),
    path('api/user/logout', Logout_User, name='Logout_User'),
    path('api/auth/forgot-password', Forgot_Password, name='Forgot_Password'),
    path('api/auth/reset-password', Reset_Password, name='Reset_Password'),
    path('api/user/add', Add_User, name='Add_User'),
    path('api/user/lists', List_All_Users, name='List_All_Users'),
    path('api/user/list/<int:id>', Get_User, name='Get_User'),
    # path('api/use/update/<int:id>', Update_User, name='Update_User'),
    path('api/auth/update-profile/<int:id>/', Update_User, name='update-profile'),
    path('api/user/remove/<int:id>', Delete_User, name='Delete_User'),
    path('api/user/validate', Validate_User_Data, name='Validate_User_Data'),
    path('api/profile-photo/<str:photo_id>/', Get_Profile_Photo, name='get_profile_photo'),
    path('api/user-photo/<int:user_id>/', Get_User_Profile_Photo, name='get_user_profile_photo'),
    path('api/update-photo/<int:user_id>/', Update_Profile_Photo, name='update_profile_photo'),
    path('api/delete-photo/<int:user_id>/', Delete_Profile_Photo, name='delete_profile_photo'),
    path('api/user-active-photo/<int:user_id>/', Get_Active_Profile_Photo, name='get_active_profile_photo'),
    path('api/auth/face-login/', Face_Recognition_Login, name='face_recognition_login'),
    path('api/user/verify-face/<int:user_id>/', Verify_User_Face, name='verify_user_face'),
    path('api/user/embeddings/<int:user_id>/', Get_User_Embeddings, name='get_user_embeddings'),
    path('api/user/regenerate-embedding/<int:user_id>/', Regenerate_User_Embedding, name='regenerate_embedding'),
    path('api/admin/embedding-stats/', Get_Embedding_Statistics, name='embedding_stats'),
]