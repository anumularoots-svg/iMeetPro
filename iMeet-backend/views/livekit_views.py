import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from services.livekit_service import livekit_service

@csrf_exempt
def get_livekit_token(request):
    if request.method == 'POST':
        data = json.loads(request.body)
    else:
        data = request.GET
    
    room = data.get('room')
    identity = data.get('identity')
    name = data.get('name')
    is_host = str(data.get('is_host', 'false')).lower() == 'true'
    
    if not all([room, identity, name]):
        return JsonResponse({'error': 'Missing params'}, status=400)
    
    token = livekit_service.generate_token(room, identity, name, is_host)
    
    return JsonResponse({
        'success': True,
        'token': token,
        'url': livekit_service.get_url(),
        'room': room
    })

def get_livekit_config(request):
    return JsonResponse({
        'success': True,
        'url': livekit_service.get_url()
    })
