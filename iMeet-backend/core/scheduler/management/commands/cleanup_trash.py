from django.core.management.base import BaseCommand
from datetime import datetime, timedelta
import logging
from bson import ObjectId
from core.UserDashBoard.recordings import collection, delete_from_s3  # adjust import if different

TRASH_RETENTION_DAYS = 15
logger = logging.getLogger(_name_)

class Command(BaseCommand):
    help = "Permanently delete trashed videos older than TRASH_RETENTION_DAYS"

    def handle(self, *args, **options):
        cutoff = datetime.now() - timedelta(days=TRASH_RETENTION_DAYS)
        old_videos = list(collection.find({
            "is_trashed": True,
            "trashed_at": {"$lt": cutoff}
        }))

        for video in old_videos:
            try:
                # Delete S3 files
                s3_keys_to_delete = []
                for url_field in ['video_url', 'transcript_url', 'summary_url', 'image_url']:
                    url = video.get(url_field)
                    if url:
                        s3_key = '/'.join(url.split('/')[-2:])
                        s3_keys_to_delete.append(s3_key)

                for s3_key in s3_keys_to_delete:
                    delete_from_s3(s3_key)

                # Delete MongoDB record
                collection.delete_one({"_id": video["_id"]})
                logger.info(f"♻ Permanently deleted trashed video {video['_id']}")
            except Exception as e:
                logger.error(f"Failed to cleanup video {video.get('_id')}: {e}")

        self.stdout.write(self.style.SUCCESS("✅ Trash cleanup completed"))