# core/Whiteboard/urls.py - COMPLETE FIXED VERSION
from django.urls import path
from . import whiteboard

urlpatterns = [
    # Session management
    path('api/whiteboard/create-session/', whiteboard.create_whiteboard_session, name='create_whiteboard_session'),
    path('api/whiteboard/state/<str:meeting_id>/', whiteboard.get_whiteboard_state, name='get_whiteboard_state'),
    path('api/whiteboard/update-settings/', whiteboard.update_whiteboard_settings, name='update_whiteboard_settings'),
    
    # Drawing operations
    path('api/whiteboard/add-drawing/', whiteboard.add_drawing, name='add_drawing'),
    path('api/whiteboard/clear/', whiteboard.clear_whiteboard, name='clear_whiteboard'),
    
    # CRITICAL: Undo/Redo operations - These MUST be accessible
    path('api/whiteboard/undo/', whiteboard.undo_action, name='undo_action'),
    path('api/whiteboard/redo/', whiteboard.redo_action, name='redo_action'),
    
    # Navigation and checkpoints
    path('api/whiteboard/create-checkpoint/', whiteboard.create_checkpoint, name='create_checkpoint'),
    path('api/whiteboard/navigate-to-state/', whiteboard.navigate_to_state, name='navigate_to_state'),
    path('api/whiteboard/history/<str:meeting_id>/', whiteboard.get_history, name='get_whiteboard_history'),
    
    # Text operations
    path('api/whiteboard/add-text/', whiteboard.add_text, name='add_text'),
    path('api/whiteboard/update-text/', whiteboard.update_text, name='update_text'),
    
    # Selection operations
    path('api/whiteboard/select-items/', whiteboard.select_items, name='select_items'),
    path('api/whiteboard/delete-selected/', whiteboard.delete_selected_items, name='delete_selected_items'),
    path('api/whiteboard/move-selected/', whiteboard.move_selected_items, name='move_selected_items'),
    
    # Cache status and debugging
    path('api/whiteboard/cache-status/', whiteboard.get_cache_status, name='get_cache_status'),
]