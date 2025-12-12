# from django.db import connections, transaction
# import logging
# import time

# logger = logging.getLogger('django.db')

# class DatabaseConnectionMiddleware:
#     def __init__(self, get_response):
#         self.get_response = get_response
    
#     def __call__(self, request):
#         start_time = time.time()
        
#         try:
#             response = self.get_response(request)
#             return response
#         except Exception as e:
#             logger.error(f"Database error in request {request.path}: {e}")
#             raise
#         finally:
#             # Close connections only for heavy operations
#             if any(endpoint in request.path for endpoint in ['/sync-optimized/', '/live-enhanced/']):
#                 connections.close_all()
            
#             # Log only slow requests
#             end_time = time.time()
#             request_time = end_time - start_time
#             if request_time > 3.0:
#                 logger.warning(f"Slow request {request.path}: {request_time:.2f}s")
    
#     def process_exception(self, request, exception):
#         connections.close_all()
#         return None