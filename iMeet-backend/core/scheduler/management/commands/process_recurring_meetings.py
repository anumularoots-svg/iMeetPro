from django.core.management.base import BaseCommand
import logging
from core.scheduler.recurring_scheduler import update_recurring_meetings, cleanup_old_meetings
from core.scheduler.email_scheduler import send_recurring_meeting_notifications

class Command(BaseCommand):
    help = 'Process recurring meetings and send daily notifications'

    def add_arguments(self, parser):
        parser.add_argument(
            '--send-emails',
            action='store_true',
            help='Send email notifications',
        )
        parser.add_argument(
            '--update-meetings',
            action='store_true',
            help='Update meeting times',
        )
        parser.add_argument(
            '--cleanup',
            action='store_true',
            help='Clean up old meetings',
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Run all processes',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting recurring meetings processor...'))
        
        try:
            if options['all'] or options['update_meetings']:
                self.stdout.write('Updating recurring meetings...')
                result = update_recurring_meetings()
                
                if result['success']:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Meetings updated: {result['updated_count']}, "
                            f"Meetings ended: {result['ended_count']}"
                        )
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR(f"Error updating meetings: {result['error']}")
                    )
            
            if options['all'] or options['send_emails']:
                self.stdout.write('Sending email notifications...')
                result = send_recurring_meeting_notifications()
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Invitations sent: {result['invitations_sent']}, "
                        f"Reminders sent: {result['reminders_sent']}"
                    )
                )
            
            if options['all'] or options['cleanup']:
                self.stdout.write('Cleaning up old meetings...')
                archived_count = cleanup_old_meetings()
                
                self.stdout.write(
                    self.style.SUCCESS(f"Archived {archived_count} old meetings")
                )
            
            self.stdout.write(self.style.SUCCESS('Recurring meetings processing completed!'))
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error processing recurring meetings: {str(e)}')
            )
            logging.error(f"Management command error: {e}")
            raise