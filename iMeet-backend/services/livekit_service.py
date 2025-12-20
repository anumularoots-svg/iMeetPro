import os
import logging
from livekit import api

logger = logging.getLogger(__name__)

class LiveKitService:
    def __init__(self):
        self.api_key = os.environ.get('LIVEKIT_API_KEY')
        self.api_secret = os.environ.get('LIVEKIT_API_SECRET')
        self.url = os.environ.get('LIVEKIT_URL')
    
    def generate_token(self, room_name, participant_identity, participant_name, is_host=False):
        token = api.AccessToken(self.api_key, self.api_secret)
        token.with_identity(participant_identity)
        token.with_name(participant_name)
        token.with_ttl(6 * 60 * 60)
        
        grant = api.VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=True,
            can_subscribe=True,
            can_publish_data=True,
        )
        
        if is_host:
            grant.room_admin = True
            grant.room_record = True
        
        token.with_grants(grant)
        return token.to_jwt()
    
    def get_url(self):
        return self.url

livekit_service = LiveKitService()
