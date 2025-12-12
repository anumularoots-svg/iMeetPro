from django.urls import path
from . import notifications

urlpatterns = [
    # 🔔 Fetch notifications
    path('api/notifications/', notifications.get_user_notifications, name='get_notifications'),
    path('api/notifications/count/', notifications.get_notification_count, name='get_notification_count'),
    # ✅ Corrected endpoints (no path variables, use JSON body)
    path('api/notifications/mark-read/', notifications.mark_notification_as_read, name='mark_notification_read'),
    path('api/notifications/mark-all-read/', notifications.mark_all_notifications_as_read, name='mark_all_notifications_read'),
    path('api/notifications/delete/', notifications.delete_notification, name='delete_notification'),
    # 🕒 Optional background task
    path('api/notifications/process-reminders/', notifications.process_reminder_notifications, name='process_reminder_notifications'),
]