# core/stream_recording/urls.py

from django.urls import path
from . import views

urlpatterns = [
    # Stream Recording APIs
    path('api/stream-recording/start/<str:meeting_id>', views.start_stream_recording, name='start_stream_recording'),
    path('api/stream-recording/stop/<str:meeting_id>', views.stop_stream_recording, name='stop_stream_recording'),
    path('api/stream-recording/status/<str:meeting_id>', views.get_stream_recording_status, name='get_stream_recording_status'),
    path('api/stream-recording/list-active', views.list_active_stream_recordings, name='list_active_stream_recordings'),
    
    # Replace existing recording URLs with these:
    path('api/meetings/<str:id>/start-recording', views.Start_Recording_Stream, name='Start_Recording_Stream'),
    path('api/meetings/<str:id>/stop-recording', views.Stop_Recording_Stream, name='Stop_Recording_Stream'),
]