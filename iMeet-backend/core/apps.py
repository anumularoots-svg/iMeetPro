# """
# Core App Configuration
# ======================
# Initialize GPU resources when Django starts
# """
# from django.apps import AppConfig
# import logging
# logger = logging.getLogger(__name__)

# class CoreConfig(AppConfig):
#     default_auto_field = 'django.db.models.BigAutoField'
#     name = 'core'
#     verbose_name = 'Core Application'
    
#     def ready(self):
#         """
#         Called when Django app is ready.
#         CRITICAL: Register all GPU resources HERE before any other code runs.
#         """
#         # Prevent double-initialization during Django reload
#         if hasattr(self, '_gpu_resources_initialized'):
#             logger.info("🔄 GPU resources already initialized (reload detected)")
#             return
            
#         logger.info("=" * 80)
#         logger.info("🚀 Core App Ready - Initializing GPU Resources")
#         logger.info("=" * 80)
        
#         try:
#             # Step 1: Import the manager first
#             from core.utils.gpu_resource_manager import get_gpu_resource_manager
#             manager = get_gpu_resource_manager()
#             logger.info("✅ GPU Resource Manager instance created")
            
#             # Step 2: Register all resource types
#             from core.utils.gpu_resources import register_all_gpu_resources
#             register_all_gpu_resources()
            
#             # Step 3: Verify registration
#             manager = get_gpu_resource_manager()
#             info = manager.get_all_resources_info()
            
#             logger.info("=" * 80)
#             logger.info("📊 GPU Resource Manager Initialization Complete")
#             logger.info(f"   Active Resources: {info['statistics']['active_resources']}")
#             logger.info(f"   Total Owners: {info['total_owners']}")
#             logger.info("=" * 80)
            
#             # Mark as initialized
#             self._gpu_resources_initialized = True
            
#         except ImportError as e:
#             logger.error("=" * 80)
#             logger.error(f"❌ CRITICAL: Failed to import GPU resource modules: {e}")
#             logger.error("   GPU features will be DISABLED")
#             logger.error("=" * 80)
#             import traceback
#             logger.error(traceback.format_exc())
            
#         except Exception as e:
#             logger.error("=" * 80)
#             logger.error(f"❌ CRITICAL: Failed to initialize GPU resources: {e}")
#             logger.error("   GPU features may not work correctly")
#             logger.error("=" * 80)
#             import traceback
#             logger.error(traceback.format_exc())


# from django.apps import AppConfig


# class CoreConfig(AppConfig):
#     default_auto_field = 'django.db.models.BigAutoField'
#     name = 'core'

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'core'
    
    def ready(self):
        """Initialize LiveKit cleanup scheduler on app startup"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from core.WebSocketConnection.meetings import livekit_service
            
            scheduler = BackgroundScheduler()
            
                # DISABLED: Cleanup scheduler disabled for testing
                # scheduler.add_job(
                #     func=livekit_service.cleanup_empty_rooms,
                #     trigger="interval",
                #     minutes=5,
                #     id="cleanup_empty_rooms",
                #     name="Cleanup empty LiveKit rooms every 5 minutes",
                #     replace_existing=True,
                #     max_instances=1,
                #     coalesce=True
                # )
                # scheduler.start()
                logger.info("⚠️ [STARTUP] Cleanup scheduler DISABLED")
                logger.info("🔄 [SCHEDULER] Cleanup interval: 5 minutes")
                logger.info("🧹 [SCHEDULER] Auto-deletes empty rooms after 5 minutes of inactivity")
        
        except ImportError as e:
            logger.warning(f"⚠️ [STARTUP] APScheduler not installed: {e}")
            logger.warning("📦 Install with: pip install apscheduler")
        
        except Exception as e:
            logger.error(f"❌ [STARTUP] Failed to initialize cleanup scheduler: {e}")