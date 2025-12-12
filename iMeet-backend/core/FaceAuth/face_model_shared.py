 

# face_model_shared.py

"""
Shared Face Recognition Model Module
=====================================
Singleton InsightFace model that can be imported by both:
- face_embeddings.py (Registration)
- face_auth.py (Verification)

This ensures both services use the SAME model instance.

Author: Face Recognition System
Version: 1.0.0
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
import numpy as np
from io import BytesIO
from PIL import Image
import cv2
from insightface.app import FaceAnalysis
import logging

# ============================================================================
# LOGGING
# ============================================================================
logger = logging.getLogger("face_model_shared")

# ============================================================================
# CONFIGURATION
# ============================================================================
FACE_MODEL_NAME = os.getenv("FACE_MODEL_NAME", "buffalo_l")
FACE_DETECTION_SIZE = tuple(map(int, os.getenv("FACE_DETECTION_SIZE", "640,640").split(",")))

# ============================================================================
# SHARED INSIGHTFACE MODEL - SINGLETON
# ============================================================================
class SharedFaceModel:
    """
    Singleton class for InsightFace model management.
    Can be imported and used by multiple services.
    
    Usage:
        from face_model_shared import get_face_model
        
        face_model = get_face_model()
        embedding = face_model.extract_embedding(image_data)
    """
    _instance = None
    _initialized = False
    _app = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SharedFaceModel, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._initialize_model()
            self._initialized = True

    def _initialize_model(self):
        """Initialize InsightFace model once"""
        try:
            logger.info(f"🔹 Initializing Shared InsightFace Model: {FACE_MODEL_NAME}")
            logger.info(f"   Detection Size: {FACE_DETECTION_SIZE}")
            
            self._app = FaceAnalysis(
                name=FACE_MODEL_NAME,
                providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
            )
            
            # Prepare with detection size
            self._app.prepare(ctx_id=-1, det_size=FACE_DETECTION_SIZE)
            
            logger.info("✅ Shared InsightFace Model initialized successfully")
            logger.info(f"   Model: {FACE_MODEL_NAME}")
            logger.info(f"   Providers: {self._app.det_model.session.get_providers()}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize InsightFace model: {e}")
            raise

    def unload_model(self):
        """Unload the model and free GPU memory"""
        try:
            if self._app is not None:
                logger.info("🔹 Unloading InsightFace model and releasing GPU memory...")
                
                # Delete the model
                del self._app
                self._app = None
                
                # Force GPU cache cleanup
                try:
                    import torch
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                        torch.cuda.synchronize()
                        logger.info("✅ CUDA cache cleared")
                except ImportError:
                    pass
                
                # Force garbage collection
                import gc
                gc.collect()
                
                logger.info("✅ InsightFace model unloaded, GPU memory released")
                self._initialized = False
                
        except Exception as e:
            logger.error(f"❌ Error unloading model: {e}")
            
    def get_app(self):
        """Get the FaceAnalysis app instance"""
        if self._app is None:
            raise RuntimeError("FaceAnalysis model not initialized")
        return self._app

    def is_ready(self):
        """Check if model is ready"""
        return self._app is not None

    def extract_embedding(self, image_data, return_face_info=False):
        """
        Extract face embedding from image.
        
        Args:
            image_data: Can be:
                - bytes: Raw image bytes
                - file-like object: File upload object
                - numpy array: BGR image array
                - PIL Image: PIL Image object
            return_face_info: If True, return additional face detection info
        
        Returns:
            If return_face_info=False:
                list: 512-dimensional embedding
            If return_face_info=True:
                dict: {
                    'embedding': [512-dim list],
                    'bbox': [x1, y1, x2, y2],
                    'landmarks': [[x, y], ...],
                    'det_score': float,
                    'age': int (if available),
                    'gender': str (if available),
                    'face_count': int
                }
        
        Raises:
            ValueError: If no face detected or processing fails
        """
        try:
            # Convert input to numpy array (BGR format)
            np_img = self._convert_to_numpy(image_data)
            
            # Detect faces
            faces = self._app.get(np_img)
            
            if not faces:
                raise ValueError("No face detected. Ensure face is clearly visible and well-lit.")
            
            if len(faces) > 1:
                logger.warning(f"⚠️  Multiple faces detected ({len(faces)}). Using largest face.")
            
            # Use largest face (by bounding box area)
            face = max(faces, key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]))
            
            # Extract embedding
            embedding = face.embedding.tolist()
            
            logger.debug(f"✅ Embedding extracted (dimension: {len(embedding)})")
            
            # Return based on flag
            if not return_face_info:
                return embedding
            
            # Return with additional info
            face_info = {
                'embedding': embedding,
                'bbox': face.bbox.tolist(),
                'landmarks': face.kps.tolist() if hasattr(face, 'kps') else None,
                'det_score': float(face.det_score),
                'age': int(face.age) if hasattr(face, 'age') else None,
                'gender': 'male' if hasattr(face, 'gender') and face.gender == 1 else 'female' if hasattr(face, 'gender') and face.gender == 0 else None,
                'face_count': len(faces)
            }
            
            return face_info
            
        except ValueError as ve:
            raise ve
        except Exception as e:
            logger.error(f"❌ Error extracting embedding: {e}", exc_info=True)
            raise ValueError(f"Failed to process image: {str(e)}")

    def _convert_to_numpy(self, image_data):
        """
        Convert various image formats to numpy array (BGR).
        
        Args:
            image_data: Image in various formats
            
        Returns:
            numpy.ndarray: Image in BGR format (OpenCV)
        """
        try:
            # Handle file-like objects (Django UploadedFile)
            if hasattr(image_data, 'read'):
                image_data = image_data.read()
            
            # Handle bytes
            if isinstance(image_data, bytes):
                img = Image.open(BytesIO(image_data)).convert("RGB")
                np_img = np.array(img)
                # Convert RGB to BGR (OpenCV format)
                np_img = cv2.cvtColor(np_img, cv2.COLOR_RGB2BGR)
                return np_img
            
            # Handle PIL Image
            elif hasattr(image_data, 'mode'):  # PIL Image
                if image_data.mode != 'RGB':
                    image_data = image_data.convert('RGB')
                np_img = np.array(image_data)
                # Convert RGB to BGR
                np_img = cv2.cvtColor(np_img, cv2.COLOR_RGB2BGR)
                return np_img
            
            # Handle numpy array
            elif isinstance(image_data, np.ndarray):
                # Assume already in BGR format
                return image_data
            
            else:
                raise ValueError(f"Unsupported image data type: {type(image_data)}")
                
        except Exception as e:
            logger.error(f"❌ Error converting image to numpy: {e}")
            raise ValueError(f"Failed to convert image: {str(e)}")

    def detect_face(self, image_data):
        """
        Detect face in image and return detection info.
        
        Args:
            image_data: Image in supported format
            
        Returns:
            dict: {
                'detected': bool,
                'face_count': int,
                'largest_face_bbox': [x1, y1, x2, y2],
                'det_score': float
            }
        """
        try:
            np_img = self._convert_to_numpy(image_data)
            faces = self._app.get(np_img)
            
            if not faces:
                return {
                    'detected': False,
                    'face_count': 0,
                    'largest_face_bbox': None,
                    'det_score': 0.0
                }
            
            # Get largest face
            face = max(faces, key=lambda f: (f.bbox[2] - f.bbox[0]) * (f.bbox[3] - f.bbox[1]))
            
            return {
                'detected': True,
                'face_count': len(faces),
                'largest_face_bbox': face.bbox.tolist(),
                'det_score': float(face.det_score)
            }
            
        except Exception as e:
            logger.error(f"❌ Error detecting face: {e}")
            return {
                'detected': False,
                'face_count': 0,
                'largest_face_bbox': None,
                'det_score': 0.0,
                'error': str(e)
            }

    def compare_embeddings(self, embedding1, embedding2, method='cosine'):
        """
        Compare two face embeddings.
        
        Args:
            embedding1: First embedding (list or numpy array)
            embedding2: Second embedding (list or numpy array)
            method: 'cosine' or 'euclidean'
            
        Returns:
            float: Distance (lower = more similar)
        """
        try:
            v1 = np.array(embedding1, dtype=np.float32)
            v2 = np.array(embedding2, dtype=np.float32)
            
            if len(v1) != len(v2):
                raise ValueError(f"Embedding dimension mismatch: {len(v1)} vs {len(v2)}")
            
            if method == 'cosine':
                # Cosine distance
                norm1 = np.linalg.norm(v1)
                norm2 = np.linalg.norm(v2)
                
                if norm1 == 0 or norm2 == 0:
                    raise ValueError("Zero vector detected")
                
                cosine_sim = np.dot(v1, v2) / (norm1 * norm2)
                distance = 1 - cosine_sim
                
            elif method == 'euclidean':
                # Euclidean distance
                distance = float(np.linalg.norm(v1 - v2))
                
            else:
                raise ValueError(f"Unknown method: {method}")
            
            return float(distance)
            
        except Exception as e:
            logger.error(f"❌ Error comparing embeddings: {e}")
            raise


# ============================================================================
# GLOBAL INSTANCE AND HELPER FUNCTION
# ============================================================================

# Global singleton instance
_global_face_model = None

def get_face_model():
    """
    Get the global shared face model instance.
    
    Usage:
        from face_model_shared import get_face_model
        
        model = get_face_model()
        embedding = model.extract_embedding(image)
    
    Returns:
        SharedFaceModel: The singleton instance
    """
    global _global_face_model
    
    if _global_face_model is None:
        _global_face_model = SharedFaceModel()
    
    return _global_face_model

def unload_face_model():
    """
    Unload the global face model and free GPU memory.
    
    Usage:
        from face_model_shared import unload_face_model
        
        # After face detection/verification is done
        unload_face_model()
    """
    global _global_face_model
    
    if _global_face_model is not None:
        _global_face_model.unload_model()
        _global_face_model = None
        logger.info("🔄 Global face model instance reset")
# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def extract_embedding(image_data, return_face_info=False):
    """
    Convenience function to extract embedding without getting model instance.
    
    Args:
        image_data: Image in supported format
        return_face_info: Return additional face info
        
    Returns:
        Embedding or face info dict
    """
    model = get_face_model()
    return model.extract_embedding(image_data, return_face_info=return_face_info)


def detect_face(image_data):
    """
    Convenience function to detect face without getting model instance.
    
    Args:
        image_data: Image in supported format
        
    Returns:
        dict: Detection info
    """
    model = get_face_model()
    return model.detect_face(image_data)


def compare_embeddings(embedding1, embedding2, method='cosine'):
    """
    Convenience function to compare embeddings.
    
    Args:
        embedding1: First embedding
        embedding2: Second embedding
        method: Comparison method
        
    Returns:
        float: Distance
    """
    model = get_face_model()
    return model.compare_embeddings(embedding1, embedding2, method=method)


# ============================================================================
# INITIALIZATION
# ============================================================================

# Initialize on import
# try:
#     _ = get_face_model()
#     logger.info("✅ Shared face model initialized successfully")
# except Exception as e:
#     logger.error(f"❌ Failed to initialize shared face model: {e}")


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    """Test the shared model"""
    import sys
    
    print("=" * 80)
    print("🧪 Testing Shared Face Model")
    print("=" * 80)
    
    try:
        # Get model instance
        model = get_face_model()
        print(f"✅ Model initialized: {model.is_ready()}")
        
        # Test with a sample image (you need to provide one)
        if len(sys.argv) > 1:
            image_path = sys.argv[1]
            print(f"\n📸 Testing with image: {image_path}")
            
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            # Test face detection
            print("\n🔍 Testing face detection...")
            detection_result = model.detect_face(image_data)
            print(f"   Detected: {detection_result['detected']}")
            print(f"   Face count: {detection_result['face_count']}")
            print(f"   Detection score: {detection_result['det_score']:.4f}")
            
            # Test embedding extraction
            print("\n🧮 Testing embedding extraction...")
            face_info = model.extract_embedding(image_data, return_face_info=True)
            print(f"   Embedding dimension: {len(face_info['embedding'])}")
            print(f"   Detection score: {face_info['det_score']:.4f}")
            print(f"   Age: {face_info['age']}")
            print(f"   Gender: {face_info['gender']}")
            
            print("\n✅ All tests passed!")
        else:
            print("\n⚠️  No test image provided. Usage: python face_model_shared.py <image_path>")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("=" * 80)