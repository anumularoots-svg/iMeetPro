from django.conf import settings
from django.db import connection, transaction
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.db import models
import json
from django.db.utils import ProgrammingError, OperationalError
from django.utils import timezone
import logging
import uuid
import re
from datetime import datetime
import pytz
from core.WebSocketConnection.meetings import Meetings
from core.UserDashBoard.users import User  # Adjust if path differs

# Global Variables
TBL_MEETING_INVITATIONS = 'tbl_Meeting_Invitations'
TBL_LOGS = 'tbl_Logs'
TBL_MEETINGS = 'tbl_Meetings'
TBL_USERS = 'tbl_Users'

VALID_LOGIN_TYPES = {'super_admin', 'user'}
VALID_MODIFICATION_TYPES = {'CREATE', 'UPDATE', 'DELETE', 'READ'}
VALID_INVITATION_STATUSES = {'sent', 'delivered', 'opened', 'bounced'}
VALID_RSVP_STATUSES = {'pending', 'accepted', 'declined', 'maybe'}
LOG_FILE_PATH = 'meeting_invitations_debug.log'
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'

SUCCESS_STATUS = 200
CREATED_STATUS = 201
BAD_REQUEST_STATUS = 400
UNAUTHORIZED_STATUS = 401
FORBIDDEN_STATUS = 403
NOT_FOUND_STATUS = 404
SERVER_ERROR_STATUS = 500

# Column length limits as per SQL schema
COLUMN_LIMITS = {
    'Email': 255,
    'Full_Name': 100,
    'Invitation_Status': 20,
    'RSVP_Status': 20
}

# Configure logging
logging.basicConfig(filename=LOG_FILE_PATH, level=LOG_LEVEL, format=LOG_FORMAT)

def validate_field_lengths(data):
    """Validate that field lengths do not exceed schema limits"""
    for field, max_length in COLUMN_LIMITS.items():
        if field in data and data[field] is not None and len(str(data[field])) > max_length:
            return False, f"{field} must be max {max_length} characters"
    return True, ""

def validate_email(email):
    """Validate email format"""
    email_pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')
    if not email_pattern.match(email):
        return False, "Invalid email format"
    return True, email

def validate_invitation_status(status):
    """Validate Invitation_Status"""
    if status not in VALID_INVITATION_STATUSES:
        return False, f"Invitation_Status must be one of: {', '.join(VALID_INVITATION_STATUSES)}"
    return True, status

def validate_rsvp_status(status):
    """Validate RSVP_Status"""
    if status not in VALID_RSVP_STATUSES:
        return False, f"RSVP_Status must be one of: {', '.join(VALID_RSVP_STATUSES)}"
    return True, status

def validate_datetime_field(dt_str, field_name):
    """Validate that datetime field is in ISO format"""
    if not dt_str:
        return True, None
    try:
        return True, datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        return False, f"{field_name} must be in ISO format"

def log_modification(request, login_type, modification_type, table_name, transaction_description=None, role=None):
    """Log modifications to tbl_Logs"""
    if login_type not in VALID_LOGIN_TYPES or modification_type not in VALID_MODIFICATION_TYPES:
        logging.error(f"Invalid login_type: {login_type} or modification_type: {modification_type}")
        return

    if not transaction_description:
        transaction_description = f"{modification_type} operation on {table_name}"

    ist_timezone = pytz.timezone('Asia/Kolkata')
    timestamp = timezone.now().astimezone(ist_timezone)

    try:
        with connection.cursor() as cursor:
            logging.debug(f"Attempting to insert log: login_type={login_type}, modification_type={modification_type}")
            insert_query = f"""
            INSERT INTO {TBL_LOGS} (login_type, role, modification_type, transaction_description, table_name, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, [
                login_type,
                role,
                modification_type,
                transaction_description,
                table_name,
                timestamp
            ])
            logging.debug(f"Log inserted: {transaction_description}")
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Logging failed: {e}")

class MeetingInvitation(models.Model):
    id = models.AutoField(primary_key=True)
    meeting_id = models.UUIDField(db_column='Meeting_ID')
    user_id = models.ForeignKey(User, on_delete=models.DO_NOTHING, db_column='User_ID')
    email = models.CharField(max_length=255, db_column='Email')
    full_name = models.CharField(max_length=100, db_column='Full_Name')
    invitation_status = models.CharField(max_length=20, db_column='Invitation_Status')
    rsvp_status = models.CharField(max_length=20, db_column='RSVP_Status')
    invite_token = models.UUIDField(db_column='Invite_Token', default=uuid.uuid4)
    sent_at = models.DateTimeField(db_column='Sent_At')
    opened_at = models.DateTimeField(blank=True, null=True, db_column='Opened_At')
    responded_at = models.DateTimeField(blank=True, null=True, db_column='Responded_At')
    created_at = models.DateTimeField(auto_now_add=True, db_column='Created_At')

    class Meta:
        db_table = 'tbl_Meeting_Invitations'

def create_meeting_invitations_table():
    """Create tbl_Meeting_Invitations table if it doesn't exist"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tbl_Meeting_Invitations')
            BEGIN
                CREATE TABLE tbl_Meeting_Invitations (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    Meeting_ID UNIQUEIDENTIFIER,
                    User_ID INT,
                    Email NVARCHAR(255),
                    Full_Name NVARCHAR(100),  
                    Invitation_Status NVARCHAR(20) CHECK (Invitation_Status IN ('sent', 'delivered', 'opened', 'bounced')),
                    RSVP_Status NVARCHAR(20) CHECK (RSVP_Status IN ('pending', 'accepted', 'declined', 'maybe')),
                    Invite_Token UNIQUEIDENTIFIER DEFAULT NEWID(),
                    Sent_At DATETIME,
                    Opened_At DATETIME,
                    Responded_At DATETIME,
                    Created_At DATETIME DEFAULT GETDATE(),
                    CONSTRAINT FK_Invitations_Meeting FOREIGN KEY (Meeting_ID)
                        REFERENCES tbl_Meetings(ID)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                    CONSTRAINT FK_Invitations_User FOREIGN KEY (User_ID)
                        REFERENCES tbl_Users(ID)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION
                )
            END
            """)
            logging.debug("tbl_Meeting_Invitations table created or exists")
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Failed to create tbl_Meeting_Invitations table: {e}")
        return JsonResponse({"Error": f"Failed to create tbl_Meeting_Invitations table: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Create_Meeting_Invitation(request):
    create_meeting_invitations_table()

    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
            logging.debug(f"Unwrapped list to: {json.dumps(data, indent=2)}")
        elif isinstance(data, list):
            logging.error("Expected single invitation object, got list")
            return JsonResponse({"Error": "Expected a single invitation object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    required_fields = ['Meeting_ID', 'User_ID', 'Email', 'Full_Name', 'Invitation_Status', 'RSVP_Status', 'Sent_At']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or data[field] == ""]
    if missing_fields:
        logging.error(f"Missing/null fields: {', '.join(missing_fields)}")
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    # Validate field lengths
    is_valid, error_message = validate_field_lengths(data)
    if not is_valid:
        logging.error(error_message)
        return JsonResponse({"Error": error_message}, status=BAD_REQUEST_STATUS)

    # Validate Meeting_ID as UUID
    try:
        meeting_id = uuid.UUID(data['Meeting_ID'])
    except ValueError:
        logging.error(f"Invalid Meeting_ID format: {data['Meeting_ID']}")
        return JsonResponse({"Error": "Invalid Meeting_ID format"}, status=BAD_REQUEST_STATUS)

    # Validate Meeting_ID existence
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_MEETINGS} WHERE ID = %s", [str(meeting_id)])
            if cursor.fetchone()[0] == 0:
                logging.error(f"Meeting_ID {meeting_id} not found")
                return JsonResponse({"Error": "Meeting_ID not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error checking Meeting_ID: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    # Validate User_ID
    try:
        user_id = int(data['User_ID'])
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_USERS} WHERE ID = %s", [user_id])
            if cursor.fetchone()[0] == 0:
                logging.error(f"User_ID {user_id} not found")
                return JsonResponse({"Error": "User_ID not found"}, status=NOT_FOUND_STATUS)
    except (ValueError, ProgrammingError, OperationalError) as e:
        logging.error(f"Invalid or error checking User_ID: {e}")
        return JsonResponse({"Error": f"Invalid User_ID or database error: {str(e)}"}, status=BAD_REQUEST_STATUS)

    # Validate Email
    is_valid, result = validate_email(data['Email'])
    if not is_valid:
        logging.error(result)
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    email = result

    # Validate Invitation_Status
    is_valid, result = validate_invitation_status(data['Invitation_Status'])
    if not is_valid:
        logging.error(result)
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    invitation_status = result

    # Validate RSVP_Status
    is_valid, result = validate_rsvp_status(data['RSVP_Status'])
    if not is_valid:
        logging.error(result)
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    rsvp_status = result

    # Validate Sent_At
    is_valid, sent_at = validate_datetime_field(data['Sent_At'], 'Sent_At')
    if not is_valid:
        logging.error(sent_at)
        return JsonResponse({"Error": sent_at}, status=BAD_REQUEST_STATUS)

    # Validate Opened_At (if provided)
    is_valid, opened_at = validate_datetime_field(data.get('Opened_At'), 'Opened_At')
    if not is_valid:
        logging.error(opened_at)
        return JsonResponse({"Error": opened_at}, status=BAD_REQUEST_STATUS)

    # Validate Responded_At (if provided)
    is_valid, responded_at = validate_datetime_field(data.get('Responded_At'), 'Responded_At')
    if not is_valid:
        logging.error(responded_at)
        return JsonResponse({"Error": responded_at}, status=BAD_REQUEST_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                insert_query = f"""
                INSERT INTO {TBL_MEETING_INVITATIONS} (
                    Meeting_ID, User_ID, Email, Full_Name, Invitation_Status, RSVP_Status, Invite_Token,
                    Sent_At, Opened_At, Responded_At
                )
                OUTPUT INSERTED.ID, INSERTED.Invite_Token
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                invite_token = uuid.uuid4()
                values = [
                    str(meeting_id),
                    user_id,
                    email,
                    data['Full_Name'],
                    invitation_status,
                    rsvp_status,
                    str(invite_token),
                    sent_at,
                    opened_at,
                    responded_at
                ]
                cursor.execute(insert_query, values)
                result = cursor.fetchone()
                invitation_id, inserted_token = result[0], result[1]

            log_modification(
                request,
                login_type='user',
                modification_type='CREATE',
                table_name=TBL_MEETING_INVITATIONS,
                transaction_description=f"Created invitation ID {invitation_id} for Meeting_ID {meeting_id}"
            )
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({
        "Message": "Meeting invitation created successfully",
        "Invitation_ID": invitation_id,
        "Invite_Token": str(inserted_token)
    }, status=CREATED_STATUS)

@require_http_methods(["PUT"])
@csrf_exempt
def Update_Invitation_Status(request, invite_token):
    create_meeting_invitations_table()

    try:
        invite_token = uuid.UUID(invite_token)
    except ValueError:
        logging.error(f"Invalid Invite_Token format: {invite_token}")
        return JsonResponse({"Error": "Invalid Invite_Token format"}, status=BAD_REQUEST_STATUS)

    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            logging.error("Expected single status object, got list")
            return JsonResponse({"Error": "Expected a single status object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    if 'Invitation_Status' not in data or data['Invitation_Status'] is None or data['Invitation_Status'] == "":
        logging.error("Missing or empty Invitation_Status")
        return JsonResponse({"Error": "Missing or empty Invitation_Status"}, status=BAD_REQUEST_STATUS)

    # Validate Invitation_Status
    is_valid, invitation_status = validate_invitation_status(data['Invitation_Status'])
    if not is_valid:
        logging.error(invitation_status)
        return JsonResponse({"Error": invitation_status}, status=BAD_REQUEST_STATUS)

    # Validate Opened_At (if provided or if status is 'opened')
    opened_at = None
    if invitation_status == 'opened' or data.get('Opened_At'):
        is_valid, opened_at = validate_datetime_field(data.get('Opened_At') or timezone.now().isoformat(), 'Opened_At')
        if not is_valid:
            logging.error(opened_at)
            return JsonResponse({"Error": opened_at}, status=BAD_REQUEST_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                update_query = f"""
                UPDATE {TBL_MEETING_INVITATIONS}
                SET Invitation_Status = %s,
                    Opened_At = %s
                WHERE Invite_Token = %s
                """
                values = [invitation_status, opened_at, str(invite_token)]
                cursor.execute(update_query, values)
                if cursor.rowcount == 0:
                    logging.error(f"Invite_Token {invite_token} not found")
                    return JsonResponse({"Error": "Invitation not found"}, status=NOT_FOUND_STATUS)

            log_modification(
                request,
                login_type='user',
                modification_type='UPDATE',
                table_name=TBL_MEETING_INVITATIONS,
                transaction_description=f"Updated invitation status for Invite_Token {invite_token}"
            )
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": "Invitation status updated successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["PUT"])
@csrf_exempt
def Update_RSVP_Status(request, invite_token):
    create_meeting_invitations_table()

    try:
        invite_token = uuid.UUID(invite_token)
    except ValueError:
        logging.error(f"Invalid Invite_Token format: {invite_token}")
        return JsonResponse({"Error": "Invalid Invite_Token format"}, status=BAD_REQUEST_STATUS)

    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            logging.error("Expected single RSVP object, got list")
            return JsonResponse({"Error": "Expected a single RSVP object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    if 'RSVP_Status' not in data or data['RSVP_Status'] is None or data['RSVP_Status'] == "":
        logging.error("Missing or empty RSVP_Status")
        return JsonResponse({"Error": "Missing or empty RSVP_Status"}, status=BAD_REQUEST_STATUS)

    # Validate RSVP_Status
    is_valid, rsvp_status = validate_rsvp_status(data['RSVP_Status'])
    if not is_valid:
        logging.error(rsvp_status)
        return JsonResponse({"Error": rsvp_status}, status=BAD_REQUEST_STATUS)

    # Set Responded_At
    is_valid, responded_at = validate_datetime_field(data.get('Responded_At') or timezone.now().isoformat(), 'Responded_At')
    if not is_valid:
        logging.error(responded_at)
        return JsonResponse({"Error": responded_at}, status=BAD_REQUEST_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                update_query = f"""
                UPDATE {TBL_MEETING_INVITATIONS}
                SET RSVP_Status = %s,
                    Responded_At = %s
                WHERE Invite_Token = %s
                """
                values = [rsvp_status, responded_at, str(invite_token)]
                cursor.execute(update_query, values)
                if cursor.rowcount == 0:
                    logging.error(f"Invite_Token {invite_token} not found")
                    return JsonResponse({"Error": "Invitation not found"}, status=NOT_FOUND_STATUS)

            log_modification(
                request,
                login_type='user',
                modification_type='UPDATE',
                table_name=TBL_MEETING_INVITATIONS,
                transaction_description=f"Updated RSVP status for Invite_Token {invite_token}"
            )
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": "RSVP status updated successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def Get_Meeting_Invitation(request, invite_token):
    create_meeting_invitations_table()

    try:
        invite_token = uuid.UUID(invite_token)
    except ValueError:
        logging.error(f"Invalid Invite_Token format: {invite_token}")
        return JsonResponse({"Error": "Invalid Invite_Token format"}, status=BAD_REQUEST_STATUS)

    try:
        with connection.cursor() as cursor:
            select_query = f"""
            SELECT ID, Meeting_ID, User_ID, Email, Full_Name, Invitation_Status, RSVP_Status,
                   Invite_Token, Sent_At, Opened_At, Responded_At, Created_At
            FROM {TBL_MEETING_INVITATIONS}
            WHERE Invite_Token = %s
            """
            cursor.execute(select_query, [str(invite_token)])
            row = cursor.fetchone()
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    if row:
        invitation = dict(zip([
            'ID', 'Meeting_ID', 'User_ID', 'Email', 'Full_Name', 'Invitation_Status', 'RSVP_Status',
            'Invite_Token', 'Sent_At', 'Opened_At', 'Responded_At', 'Created_At'
        ], row))
        log_modification(
            request,
            login_type='user',
            modification_type='READ',
            table_name=TBL_MEETING_INVITATIONS,
            transaction_description=f"Retrieved invitation for Invite_Token {invite_token}"
        )
        return JsonResponse(invitation, status=SUCCESS_STATUS)
    logging.error(f"Invite_Token {invite_token} not found")
    return JsonResponse({"Error": "Invitation not found"}, status=NOT_FOUND_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def List_Meeting_Invitations(request):
    create_meeting_invitations_table()

    try:
        with connection.cursor() as cursor:
            select_query = f"""
            SELECT ID, Meeting_ID, User_ID, Email, Full_Name, Invitation_Status, RSVP_Status,
                   Invite_Token, Sent_At, Opened_At, Responded_At, Created_At
            FROM {TBL_MEETING_INVITATIONS}
            """
            cursor.execute(select_query)
            rows = cursor.fetchall()
            invitations = [
                dict(zip([
                    'ID', 'Meeting_ID', 'User_ID', 'Email', 'Full_Name', 'Invitation_Status', 'RSVP_Status',
                    'Invite_Token', 'Sent_At', 'Opened_At', 'Responded_At', 'Created_At'
                ], row))
                for row in rows
            ]

        log_modification(
            request,
            login_type='user',
            modification_type='READ',
            table_name=TBL_MEETING_INVITATIONS,
            transaction_description="Retrieved all meeting invitations"
        )
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse(invitations, safe=False, status=SUCCESS_STATUS)

@require_http_methods(["PUT"])
@csrf_exempt
def Update_Meeting_Invitation(request, invite_token):
    create_meeting_invitations_table()

    try:
        invite_token = uuid.UUID(invite_token)
    except ValueError:
        logging.error(f"Invalid Invite_Token format: {invite_token}")
        return JsonResponse({"Error": "Invalid Invite_Token format"}, status=BAD_REQUEST_STATUS)

    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            logging.error("Expected single invitation object, got list")
            return JsonResponse({"Error": "Expected a single invitation object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    required_fields = ['Meeting_ID', 'User_ID', 'Email', 'Full_Name', 'Invitation_Status', 'RSVP_Status', 'Sent_At']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or data[field] == ""]
    if missing_fields:
        logging.error(f"Missing/null fields: {', '.join(missing_fields)}")
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    # Validate field lengths
    is_valid, error_message = validate_field_lengths(data)
    if not is_valid:
        logging.error(error_message)
        return JsonResponse({"Error": error_message}, status=BAD_REQUEST_STATUS)

    # Validate Meeting_ID as UUID
    try:
        meeting_id = uuid.UUID(data['Meeting_ID'])
    except ValueError:
        logging.error(f"Invalid Meeting_ID format: {data['Meeting_ID']}")
        return JsonResponse({"Error": "Invalid Meeting_ID format"}, status=BAD_REQUEST_STATUS)

    # Validate Meeting_ID existence
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_MEETINGS} WHERE ID = %s", [str(meeting_id)])
            if cursor.fetchone()[0] == 0:
                logging.error(f"Meeting_ID {meeting_id} not found")
                return JsonResponse({"Error": "Meeting_ID not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error checking Meeting_ID: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    # Validate User_ID
    try:
        user_id = int(data['User_ID'])
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_USERS} WHERE ID = %s", [user_id])
            if cursor.fetchone()[0] == 0:
                logging.error(f"User_ID {user_id} not found")
                return JsonResponse({"Error": "User_ID not found"}, status=NOT_FOUND_STATUS)
    except (ValueError, ProgrammingError, OperationalError) as e:
        logging.error(f"Invalid or error checking User_ID: {e}")
        return JsonResponse({"Error": f"Invalid User_ID or database error: {str(e)}"}, status=BAD_REQUEST_STATUS)

    # Validate Email
    is_valid, email = validate_email(data['Email'])
    if not is_valid:
        logging.error(email)
        return JsonResponse({"Error": email}, status=BAD_REQUEST_STATUS)

    # Validate Invitation_Status
    is_valid, invitation_status = validate_invitation_status(data['Invitation_Status'])
    if not is_valid:
        logging.error(invitation_status)
        return JsonResponse({"Error": invitation_status}, status=BAD_REQUEST_STATUS)

    # Validate RSVP_Status
    is_valid, rsvp_status = validate_rsvp_status(data['RSVP_Status'])
    if not is_valid:
        logging.error(rsvp_status)
        return JsonResponse({"Error": rsvp_status}, status=BAD_REQUEST_STATUS)

    # Validate Sent_At
    is_valid, sent_at = validate_datetime_field(data['Sent_At'], 'Sent_At')
    if not is_valid:
        logging.error(sent_at)
        return JsonResponse({"Error": sent_at}, status=BAD_REQUEST_STATUS)

    # Validate Opened_At (if provided)
    is_valid, opened_at = validate_datetime_field(data.get('Opened_At'), 'Opened_At')
    if not is_valid:
        logging.error(opened_at)
        return JsonResponse({"Error": opened_at}, status=BAD_REQUEST_STATUS)

    # Validate Responded_At (if provided)
    is_valid, responded_at = validate_datetime_field(data.get('Responded_At'), 'Responded_At')
    if not is_valid:
        logging.error(responded_at)
        return JsonResponse({"Error": responded_at}, status=BAD_REQUEST_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                update_query = f"""
                UPDATE {TBL_MEETING_INVITATIONS}
                SET Meeting_ID = %s,
                    User_ID = %s,
                    Email = %s,
                    Full_Name = %s,
                    Invitation_Status = %s,
                    RSVP_Status = %s,
                    Sent_At = %s,
                    Opened_At = %s,
                    Responded_At = %s
                WHERE Invite_Token = %s
                """
                values = [
                    str(meeting_id),
                    user_id,
                    email,
                    data['Full_Name'],
                    invitation_status,
                    rsvp_status,
                    sent_at,
                    opened_at,
                    responded_at,
                    str(invite_token)
                ]
                cursor.execute(update_query, values)
                if cursor.rowcount == 0:
                    logging.error(f"Invite_Token {invite_token} not found")
                    return JsonResponse({"Error": "Invitation not found"}, status=NOT_FOUND_STATUS)

            log_modification(
                request,
                login_type='user',
                modification_type='UPDATE',
                table_name=TBL_MEETING_INVITATIONS,
                transaction_description=f"Updated invitation for Invite_Token {invite_token}"
            )
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": "Meeting invitation updated successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["DELETE"])
@csrf_exempt
def Delete_Meeting_Invitation(request, invite_token):
    create_meeting_invitations_table()

    try:
        invite_token = uuid.UUID(invite_token)
    except ValueError:
        logging.error(f"Invalid Invite_Token format: {invite_token}")
        return JsonResponse({"Error": "Invalid Invite_Token format"}, status=BAD_REQUEST_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                select_query = f"SELECT ID FROM {TBL_MEETING_INVITATIONS} WHERE Invite_Token = %s"
                cursor.execute(select_query, [str(invite_token)])
                row = cursor.fetchone()
                if not row:
                    logging.error(f"Invite_Token {invite_token} not found")
                    return JsonResponse({"Error": "Invitation not found"}, status=NOT_FOUND_STATUS)
                invitation_id = row[0]

                delete_query = f"DELETE FROM {TBL_MEETING_INVITATIONS} WHERE Invite_Token = %s"
                cursor.execute(delete_query, [str(invite_token)])

                log_modification(
                    request,
                    login_type='user',
                    modification_type='DELETE',
                    table_name=TBL_MEETING_INVITATIONS,
                    transaction_description=f"Deleted invitation ID {invitation_id} (Invite_Token: {invite_token})"
                )
    except (ProgrammingError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": f"Invitation with Invite_Token {invite_token} deleted successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Validate_Meeting_Invitation_Data(request):
    create_meeting_invitations_table()

    try:
        data = json.loads(request.body)
        logging.debug(f"Received JSON: {json.dumps(data, indent=2)}")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            logging.error("Expected single validation object, got list")
            return JsonResponse({"Error": "Expected a single validation object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    required_fields = ['Meeting_ID']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or data[field] == ""]
    if missing_fields:
        logging.error(f"Missing/null fields: {', '.join(missing_fields)}")
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    # Initialize validation results
    validation_results = {
        "Meeting_ID": {"is_valid": True, "message": ""},
        "User_ID": {"is_valid": True, "message": ""},
        "Email": {"is_valid": True, "message": ""},
        "Full_Name": {"is_valid": True, "message": ""},
        "Invitation_Status": {"is_valid": True, "message": ""},
        "RSVP_Status": {"is_valid": True, "message": ""},
        "Sent_At": {"is_valid": True, "message": ""},
        "Opened_At": {"is_valid": True, "message": ""},
        "Responded_At": {"is_valid": True, "message": ""}
    }

    # Validate Meeting_ID
    if data.get('Meeting_ID'):
        try:
            meeting_id = uuid.UUID(data['Meeting_ID'])
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {TBL_MEETINGS} WHERE ID = %s", [str(meeting_id)])
                if cursor.fetchone()[0] == 0:
                    logging.error(f"Meeting_ID {meeting_id} not found")
                    validation_results["Meeting_ID"] = {"is_valid": False, "message": "Meeting_ID not found"}
        except ValueError:
            logging.error(f"Invalid Meeting_ID format: {data['Meeting_ID']}")
            validation_results["Meeting_ID"] = {"is_valid": False, "message": "Invalid Meeting_ID format"}

    # Validate User_ID
    if data.get('User_ID'):
        try:
            user_id = int(data['User_ID'])
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {TBL_USERS} WHERE ID = %s", [user_id])
                if cursor.fetchone()[0] == 0:
                    logging.error(f"User_ID {user_id} not found")
                    validation_results["User_ID"] = {"is_valid": False, "message": "User_ID not found"}
        except (ValueError, ProgrammingError, OperationalError) as e:
            logging.error(f"Invalid or error checking User_ID: {e}")
            validation_results["User_ID"] = {"is_valid": False, "message": f"Invalid User_ID or database error: {str(e)}"}

    # Validate Email
    if data.get('Email'):
        is_valid, error_message = validate_email(data['Email'])
        if not is_valid:
            logging.error(error_message)
            validation_results["Email"] = {"is_valid": False, "message": error_message}

    # Validate Full_Name
    if data.get('Full_Name'):
        is_valid, error_message = validate_field_lengths({'Full_Name': data['Full_Name']})
        if not is_valid:
            logging.error(error_message)
            validation_results["Full_Name"] = {"is_valid": False, "message": error_message}

    # Validate Invitation_Status
    if data.get('Invitation_Status'):
        is_valid, error_message = validate_invitation_status(data['Invitation_Status'])
        if not is_valid:
            logging.error(error_message)
            validation_results["Invitation_Status"] = {"is_valid": False, "message": error_message}

    # Validate RSVP_Status
    if data.get('RSVP_Status'):
        is_valid, error_message = validate_rsvp_status(data['RSVP_Status'])
        if not is_valid:
            logging.error(error_message)
            validation_results["RSVP_Status"] = {"is_valid": False, "message": error_message}

    # Validate Sent_At
    if data.get('Sent_At'):
        is_valid, error_message = validate_datetime_field(data['Sent_At'], 'Sent_At')
        if not is_valid:
            logging.error(error_message)
            validation_results["Sent_At"] = {"is_valid": False, "message": error_message}

    # Validate Opened_At
    if data.get('Opened_At'):
        is_valid, error_message = validate_datetime_field(data['Opened_At'], 'Opened_At')
        if not is_valid:
            logging.error(error_message)
            validation_results["Opened_At"] = {"is_valid": False, "message": error_message}

    # Validate Responded_At
    if data.get('Responded_At'):
        is_valid, error_message = validate_datetime_field(data['Responded_At'], 'Responded_At')
        if not is_valid:
            logging.error(error_message)
            validation_results["Responded_At"] = {"is_valid": False, "message": error_message}

    # Log the validation attempt
    log_modification(
        request,
        login_type='user',
        modification_type='READ',
        table_name=TBL_MEETING_INVITATIONS,
        transaction_description=f"Validated invitation data: Meeting_ID={data.get('Meeting_ID', 'N/A')}"
    )

    return JsonResponse(validation_results, status=SUCCESS_STATUS)

# URL patterns
urlpatterns = [
    path('api/meeting-invitation/create', Create_Meeting_Invitation, name='Create_Meeting_Invitation'),
    path('api/meeting-invitation/update-status/<str:invite_token>', Update_Invitation_Status, name='Update_Invitation_Status'),
    path('api/meeting-invitation/update-rsvp/<str:invite_token>', Update_RSVP_Status, name='Update_RSVP_Status'),
    path('api/meeting-invitation/get/<str:invite_token>', Get_Meeting_Invitation, name='Get_Meeting_Invitation'),
    path('api/meeting-invitation/list', List_Meeting_Invitations, name='List_Meeting_Invitations'),
    path('api/meeting-invitation/update/<str:invite_token>', Update_Meeting_Invitation, name='Update_Meeting_Invitation'),
    path('api/meeting-invitation/delete/<str:invite_token>', Delete_Meeting_Invitation, name='Delete_Meeting_Invitation'),
    path('api/meeting-invitation/validate', Validate_Meeting_Invitation_Data, name='Validate_Meeting_Invitation_Data'),
]