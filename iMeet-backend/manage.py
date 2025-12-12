#!/usr/bin/env python
import os
import sys
import os

# ✅ Force ONNXRuntime to pick the correct GPU before Django loads anything
os.environ["CUDA_VISIBLE_DEVICES"] = "0"
os.environ["ORT_DEVICE_ID"] = "0"
os.environ["ORT_CUDA_UNIFIED_MEMORY"] = "1"
os.environ["ORT_CUDA_CONTEXT_ALLOW_MULTIPLE_DEVICES"] = "1"


# FORCE SOFTWARE-ONLY PROCESSING BEFORE ANY IMPORTS
os.environ.update({
    'CUDA_VISIBLE_DEVICES': '',
    'LIVEKIT_DISABLE_HARDWARE_ACCELERATION': '1',
    'WEBRTC_DISABLE_HW_DECODER': '1',
    'WEBRTC_DISABLE_HW_ENCODER': '1'
})


#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SampleDB.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
