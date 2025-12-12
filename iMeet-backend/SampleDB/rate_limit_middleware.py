# import time
# from collections import defaultdict
# from django.http import JsonResponse

# class SyncRateLimitMiddleware:
#     """Fixed rate limiting that doesn't block legitimate requests"""
    
#     def __init__(self, get_response):
#         self.get_response = get_response
#         self.request_history = defaultdict(list)
#         self.cleanup_interval = 30
#         self.last_cleanup = time.time()
    
#     def __call__(self, request):
#         current_time = time.time()
        
#         # Cleanup old requests periodically
#         if current_time - self.last_cleanup > self.cleanup_interval:
#             self.cleanup_old_requests(current_time)
        
#         # Only apply rate limiting to specific heavy endpoints
#         if self.should_rate_limit(request.path):
#             meeting_id = self.extract_meeting_id(request.path)
            
#             # Much more lenient rate limiting - allow 2 requests per second
#             recent_requests = self.request_history[meeting_id]
#             if len(recent_requests) >= 2:
#                 # Check if both recent requests were within last 1 second
#                 recent_in_second = [req for req in recent_requests if current_time - req < 1.0]
#                 if len(recent_in_second) >= 2:
#                     return JsonResponse({
#                         'success': False,
#                         'error': 'Rate limited - too many requests',
#                         'retry_after': 0.5
#                     }, status=429)
            
#             # Record this request
#             recent_requests.append(current_time)
            
#             # Keep only last 5 requests
#             if len(recent_requests) > 5:
#                 recent_requests.pop(0)
        
#         return self.get_response(request)
    
#     def should_rate_limit(self, path):
#         """Only rate limit truly heavy operations"""
#         heavy_endpoints = [
#             '/sync-optimized/',
#             '/live-enhanced/',  # Only these heavy endpoints
#         ]
#         return any(endpoint in path for endpoint in heavy_endpoints)
    
#     def extract_meeting_id(self, path):
#         """Extract meeting ID from path"""
#         parts = path.strip('/').split('/')
#         for part in reversed(parts):
#             if part and len(part) > 10 and '-' in part:  # Looks like a UUID
#                 return part
#         return 'unknown'
    
#     def cleanup_old_requests(self, current_time):
#         """Remove requests older than 5 seconds"""
#         for meeting_id in list(self.request_history.keys()):
#             self.request_history[meeting_id] = [
#                 req_time for req_time in self.request_history[meeting_id]
#                 if current_time - req_time < 5.0  # Only keep last 5 seconds
#             ]
#             if not self.request_history[meeting_id]:
#                 del self.request_history[meeting_id]
        
#         self.last_cleanup = current_time