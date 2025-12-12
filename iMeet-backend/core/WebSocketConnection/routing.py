# # 1. use routing.py
# from django.urls import re_path
# from .unified_consumer import UnifiedMeetingConsumer

# websocket_urlpatterns = [
#     re_path(r'ws/meeting/(?P<meeting_id>[^/]+)/$', UnifiedMeetingConsumer.as_asgi()),
# ]


# 2.routing.py
# from django.urls import re_path
# from .unified_consumer import UnifiedMeetingConsumer

# websocket_urlpatterns = [
#     re_path(r'^ws/meeting/(?P<meeting_id>[^/]+)/?$', UnifiedMeetingConsumer.as_asgi()),
# ]


# FIXED: routing.py - Create this file in core/WebSocketConnection/routing.py

from django.urls import re_path
from .meetings_consumers import MeetingConsumer

websocket_urlpatterns = [
    # FIXED: Handle all meeting ID formats including instant
    re_path(r'wss/meeting/(?P<meeting_id>[^/]+)/$', MeetingConsumer.as_asgi()),
]