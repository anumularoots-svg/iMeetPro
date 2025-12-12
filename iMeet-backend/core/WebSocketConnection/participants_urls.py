# core/WebSocketConnection/participants_urls.py
from django.urls import path
from .participants import (
    record_participant_join,
    record_participant_leave,
    Get_User_Meeting_History,
    Get_User_Meetings_By_Date,
    Get_User_Today_Meetings,
    list_participants_basic,
    Leave_Meeting,
    Get_Live_Participants_Enhanced_No_Status,
    Sync_LiveKit_Participants_Fixed,
    end_meeting,
    assign_co_host,
    remove_co_host,
    get_co_hosts,
    check_co_host_status,
    remove_participant_from_meeting,
)

urlpatterns = [
    # CRITICAL: Core participant endpoints that are missing
    path('api/participants/record-join/', record_participant_join, name='record_participant_join'),
    path('api/participants/record-leave/', record_participant_leave, name='record_participant_leave'),
    
    # Enhanced LiveKit integration endpoints
    # path('api/participants/live/<str:meeting_id>/', Get_Live_Participants, name='Get_Live_Participants'),
    path('api/participants/live-enhanced/<str:meeting_id>/', Get_Live_Participants_Enhanced_No_Status, name='Get_Live_Participants_Enhanced'),
    
    # Participant sync endpoints
    # path('api/participants/sync/<str:meeting_id>/', Sync_LiveKit_Participants, name='Sync_LiveKit_Participants'),
    path('api/participants/sync-optimized/<str:meeting_id>/', Sync_LiveKit_Participants_Fixed, name='Sync_LiveKit_Participants_Fixed'),
    
    # Basic participant management
    path('api/participants/list/<str:meeting_id>/', list_participants_basic, name='list_participants_basic'),
    
    path('api/participants/leave/<int:participant_id>/', Leave_Meeting, name='Leave_Meeting'),
    path('api/meetings/<str:meeting_id>/end', end_meeting, name='end_meeting'),

    # Co-host management APIs
    path('api/meetings/assign-cohost/', assign_co_host, name='assign_co_host'),
    path('api/meetings/remove-cohost/', remove_co_host, name='remove_co_host'),
    path('api/meetings/cohosts/<str:meeting_id>/', get_co_hosts, name='get_co_hosts'),
    path('api/meetings/check-cohost/<str:meeting_id>/<str:user_id>/', check_co_host_status, name='check_co_host_status'),
    path('api/meetings/user-meeting-history', Get_User_Meeting_History, name='Get_User_Meeting_History'),
    path('api/meetings/user-meetings-by-date', Get_User_Meetings_By_Date, name='Get_User_Meetings_By_Date'),
    path('api/meetings/user-today-meetings', Get_User_Today_Meetings, name='Get_User_Today_Meetings'),
    # Participant removal API
    path('api/meetings/remove-participant/', remove_participant_from_meeting, name='remove_participant_from_meeting'),
]