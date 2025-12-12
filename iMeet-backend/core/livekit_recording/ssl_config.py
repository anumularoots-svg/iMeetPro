# core/livekit_recording/ssl_config.py - SSL Certificate Fix

import ssl
import os
import logging

logger = logging.getLogger(__name__)

def configure_ssl_for_livekit():
    """Configure SSL to handle self-signed certificates for LiveKit"""
    try:
        # Create an SSL context that accepts self-signed certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Set environment variables to disable SSL verification for LiveKit
        os.environ['PYTHONHTTPSVERIFY'] = '0'
        os.environ['CURL_CA_BUNDLE'] = ''
        os.environ['REQUESTS_CA_BUNDLE'] = ''
        
        logger.info("SSL configured to accept self-signed certificates")
        return ssl_context
        
    except Exception as e:
        logger.error(f"Failed to configure SSL: {e}")
        return None

def create_insecure_websocket_url(url: str) -> str:
    """Convert HTTPS/WSS URLs to HTTP/WS for self-signed certificates"""
    try:
        if url.startswith('wss://'):
            # For development with self-signed certificates, use ws:// instead
            insecure_url = url.replace('wss://', 'ws://').replace(':7881', ':7880')
            logger.warning(f"Using insecure WebSocket URL: {insecure_url}")
            return insecure_url
        elif url.startswith('https://'):
            insecure_url = url.replace('https://', 'http://').replace(':7881', ':7880')
            logger.warning(f"Using insecure HTTP URL: {insecure_url}")
            return insecure_url
        return url
    except Exception as e:
        logger.error(f"Failed to create insecure URL: {e}")
        return url

# Apply SSL configuration on import
configure_ssl_for_livekit()