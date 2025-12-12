from django.db import connection, transaction
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.urls import path
from django.db import models
from django.conf import settings
import json
from django.db.utils import ProgrammingError, OperationalError
from django.utils import timezone
import pytz
from core.WebSocketConnection.meetings import Meetings  # Import Meeting model

# Global Variables
TBL_FEEDBACK = 'tbl_Feedback'
TBL_MEETINGS = 'tbl_Meetings'
TBL_USERS = 'tbl_Users'  # Updated to match schema

VALID_LOGIN_TYPES = {'super_admin', 'user'}
VALID_MODIFICATION_TYPES = {'CREATE', 'UPDATE', 'DELETE', 'READ'}
VALID_FEEDBACK_TYPES = {'General', 'Technical', 'Content', 'Other'}  # Example set

SUCCESS_STATUS = 200
CREATED_STATUS = 201
BAD_REQUEST_STATUS = 400
UNAUTHORIZED_STATUS = 401
FORBIDDEN_STATUS = 403
NOT_FOUND_STATUS = 404
SERVER_ERROR_STATUS = 500

# Column length limits as per SQL schema
COLUMN_LIMITS = {
    'Comments': 4000,  # TEXT field
    'Feedback_Type': 50
}

def validate_field_lengths(data):
    """Validate that field lengths do not exceed schema limits"""
    for field, max_length in COLUMN_LIMITS.items():
        if field in data and data[field] is not None and len(str(data[field])) > max_length:
            return False, f"{field} must be max {max_length} characters"
    return True, ""

def validate_rating(rating):
    """Validate that rating is between 1 and 5"""
    try:
        rating = int(rating)
        if rating < 1 or rating > 5:
            return False, "Rating must be between 1 and 5"
        return True, rating
    except (ValueError, TypeError):
        return False, "Rating must be an integer"

def validate_feedback_type(feedback_type):
    """Validate Feedback_Type"""
    if feedback_type not in VALID_FEEDBACK_TYPES:
        return False, f"Feedback_Type must be one of: {', '.join(VALID_FEEDBACK_TYPES)}"
    return True, feedback_type

class Feedback(models.Model):
    id = models.AutoField(primary_key=True)
    # meeting_id = models.UUIDField(db_column='Meeting_ID')
    meeting_id = models.CharField(max_length=20, db_column='Meeting_ID')
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.DO_NOTHING, db_column='User_ID')
    rating = models.IntegerField()
    comments = models.TextField(blank=True, null=True)
    feedback_type = models.CharField(max_length=50, blank=True, null=True)
    submitted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'tbl_Feedback'

def create_feedback_table():
    """Create tbl_Feedback table if it doesn't exist - MYSQL VERSION"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS tbl_Feedback (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Meeting_ID VARCHAR(20) NOT NULL,
                User_ID INT NOT NULL,
                Rating INT NOT NULL CHECK (Rating BETWEEN 1 AND 5),
                Comments TEXT,
                Feedback_Type VARCHAR(50),
                Submitted_At DATETIME DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_Feedback_Meeting FOREIGN KEY (Meeting_ID)
                    REFERENCES tbl_Meetings(ID)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION,
                CONSTRAINT FK_Feedback_User FOREIGN KEY (User_ID)
                    REFERENCES tbl_Users(ID)
                    ON DELETE NO ACTION
                    ON UPDATE NO ACTION
            )
            """)
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Failed to create tbl_Feedback table: {str(e)}"}, status=SERVER_ERROR_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Create_Feedback(request):
    create_feedback_table()

    try:
        data = json.loads(request.body)
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            return JsonResponse({"Error": "Expected a single feedback object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    required_fields = ['Meeting_ID', 'User_ID', 'Rating', 'Feedback_Type']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or data[field] == ""]
    if missing_fields:
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    # Validate field lengths
    is_valid, error_message = validate_field_lengths(data)
    if not is_valid:
        return JsonResponse({"Error": error_message}, status=BAD_REQUEST_STATUS)

    # Validate Rating
    is_valid, result = validate_rating(data['Rating'])
    if not is_valid:
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    rating = result

    # Validate Feedback_Type
    is_valid, result = validate_feedback_type(data['Feedback_Type'])
    if not is_valid:
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    feedback_type = result

    # Validate Meeting_ID as UUID
    try:
        meeting_id = data['Meeting_ID']
    except ValueError:
        return JsonResponse({"Error": "Invalid Meeting_ID format"}, status=BAD_REQUEST_STATUS)

    # Validate Meeting_ID existence
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_MEETINGS} WHERE ID = %s", [str(meeting_id)])
            if cursor.fetchone()[0] == 0:
                return JsonResponse({"Error": "Meeting_ID not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    # Validate User_ID
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_USERS} WHERE ID = %s", [data['User_ID']])
            if cursor.fetchone()[0] == 0:
                return JsonResponse({"Error": "User_ID not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    # FIXED: MySQL compatible INSERT with LAST_INSERT_ID()
    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                insert_query = f"""
                INSERT INTO {TBL_FEEDBACK} (
                    Meeting_ID, User_ID, Rating, Comments, Feedback_Type
                )
                VALUES (%s, %s, %s, %s, %s)
                """
                values = [
                    str(meeting_id),
                    data['User_ID'],
                    rating,
                    data.get('Comments'),
                    feedback_type
                ]
                cursor.execute(insert_query, values)
                
                # MYSQL WAY: Get the last inserted ID
                cursor.execute("SELECT LAST_INSERT_ID()")
                inserted_id = cursor.fetchone()[0]
                
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({
        "Message": "Feedback created successfully",
        "Feedback_ID": inserted_id
    }, status=CREATED_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def List_All_Feedback(request):
    create_feedback_table()

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
            SELECT f.ID, f.Meeting_ID, f.User_ID, f.Rating, f.Comments, 
                   f.Feedback_Type, f.Submitted_At
            FROM {TBL_FEEDBACK} f
            ORDER BY f.Submitted_At DESC
            """)
            rows = cursor.fetchall()
            
            feedback_list = []
            for row in rows:
                feedback_list.append({
                    "ID": row[0],
                    "Meeting_ID": str(row[1]),
                    "User_ID": row[2],
                    "Rating": row[3],
                    "Comments": row[4],
                    "Feedback_Type": row[5],
                    "Submitted_At": row[6].isoformat() if row[6] else None
                })
                
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Feedback": feedback_list}, status=SUCCESS_STATUS)

@require_http_methods(["GET"])
@csrf_exempt
def Get_Feedback(request, id):
    create_feedback_table()

    try:
        feedback_id = int(id)
    except ValueError:
        return JsonResponse({"Error": "Invalid ID format"}, status=BAD_REQUEST_STATUS)

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
            SELECT f.ID, f.Meeting_ID, f.User_ID, f.Rating, f.Comments, 
                   f.Feedback_Type, f.Submitted_At
            FROM {TBL_FEEDBACK} f
            WHERE f.ID = %s
            """, [feedback_id])
            row = cursor.fetchone()
            
            if not row:
                return JsonResponse({"Error": "Feedback not found"}, status=NOT_FOUND_STATUS)
                
            feedback = {
                "ID": row[0],
                "Meeting_ID": str(row[1]),
                "User_ID": row[2],
                "Rating": row[3],
                "Comments": row[4],
                "Feedback_Type": row[5],
                "Submitted_At": row[6].isoformat() if row[6] else None
            }
                
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Feedback": feedback}, status=SUCCESS_STATUS)

@require_http_methods(["PUT"])
@csrf_exempt
def Update_Feedback(request, id):
    create_feedback_table()

    try:
        feedback_id = int(id)
    except ValueError:
        return JsonResponse({"Error": "Invalid ID format"}, status=BAD_REQUEST_STATUS)

    try:
        data = json.loads(request.body)
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            return JsonResponse({"Error": "Expected a single feedback object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    required_fields = ['Meeting_ID', 'User_ID', 'Rating', 'Feedback_Type']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None or data[field] == ""]
    if missing_fields:
        return JsonResponse({"Error": f"Missing or empty required fields: {', '.join(missing_fields)}"}, status=BAD_REQUEST_STATUS)

    # Validate field lengths
    is_valid, error_message = validate_field_lengths(data)
    if not is_valid:
        return JsonResponse({"Error": error_message}, status=BAD_REQUEST_STATUS)

    # Validate Rating
    is_valid, result = validate_rating(data['Rating'])
    if not is_valid:
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    rating = result

    # Validate Feedback_Type
    is_valid, result = validate_feedback_type(data['Feedback_Type'])
    if not is_valid:
        return JsonResponse({"Error": result}, status=BAD_REQUEST_STATUS)
    feedback_type = result

    # Validate Meeting_ID as UUID
    try:
        meeting_id = data['Meeting_ID']
    except ValueError:
        return JsonResponse({"Error": "Invalid Meeting_ID format"}, status=BAD_REQUEST_STATUS)

    # Validate Meeting_ID existence
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_MEETINGS} WHERE ID = %s", [str(meeting_id)])
            if cursor.fetchone()[0] == 0:
                return JsonResponse({"Error": "Meeting_ID not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    # Validate User_ID
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TBL_USERS} WHERE ID = %s", [data['User_ID']])
            if cursor.fetchone()[0] == 0:
                return JsonResponse({"Error": "User_ID not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                update_query = f"""
                UPDATE {TBL_FEEDBACK}
                SET Meeting_ID = %s,
                    User_ID = %s,
                    Rating = %s,
                    Comments = %s,
                    Feedback_Type = %s
                WHERE ID = %s
                """
                values = [
                    str(meeting_id),
                    data['User_ID'],
                    rating,
                    data.get('Comments'),
                    feedback_type,
                    feedback_id
                ]
                cursor.execute(update_query, values)
                if cursor.rowcount == 0:
                    return JsonResponse({"Error": "Feedback not found"}, status=NOT_FOUND_STATUS)
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": "Feedback updated successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["DELETE"])
@csrf_exempt
def Delete_Feedback(request, id):
    create_feedback_table()

    try:
        feedback_id = int(id)
    except ValueError:
        return JsonResponse({"Error": "Invalid ID format"}, status=BAD_REQUEST_STATUS)

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                select_query = f"SELECT ID FROM {TBL_FEEDBACK} WHERE ID = %s"
                cursor.execute(select_query, [feedback_id])
                row = cursor.fetchone()
                if not row:
                    return JsonResponse({"Error": "Feedback not found"}, status=NOT_FOUND_STATUS)

                delete_query = f"DELETE FROM {TBL_FEEDBACK} WHERE ID = %s"
                cursor.execute(delete_query, [feedback_id])
    except (ProgrammingError, OperationalError) as e:
        return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    return JsonResponse({"Message": f"Feedback ID {feedback_id} deleted successfully"}, status=SUCCESS_STATUS)

@require_http_methods(["POST"])
@csrf_exempt
def Validate_Feedback_Data(request):
    create_feedback_table()

    try:
        data = json.loads(request.body)
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        elif isinstance(data, list):
            return JsonResponse({"Error": "Expected a single validation object, not a list"}, status=BAD_REQUEST_STATUS)
    except json.JSONDecodeError as e:
        return JsonResponse({"Error": "Invalid JSON format"}, status=BAD_REQUEST_STATUS)

    # Initialize validation results
    validation_results = {
        "Meeting_ID": {"is_valid": True, "message": ""},
        "User_ID": {"is_valid": True, "message": ""},
        "Rating": {"is_valid": True, "message": ""},
        "Comments": {"is_valid": True, "message": ""},
        "Feedback_Type": {"is_valid": True, "message": ""}
    }

    # Validate Meeting_ID (if provided)
    if data.get('Meeting_ID'):
        try:
            meeting_id = data['Meeting_ID']
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {TBL_MEETINGS} WHERE ID = %s", [str(meeting_id)])
                if cursor.fetchone()[0] == 0:
                    validation_results["Meeting_ID"] = {"is_valid": False, "message": "Meeting_ID not found"}
        except ValueError:
            validation_results["Meeting_ID"] = {"is_valid": False, "message": "Invalid Meeting_ID format"}

    # Validate User_ID (if provided)
    if data.get('User_ID'):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {TBL_USERS} WHERE ID = %s", [data['User_ID']])
                if cursor.fetchone()[0] == 0:
                    validation_results["User_ID"] = {"is_valid": False, "message": "User_ID not found"}
        except (ProgrammingError, OperationalError) as e:
            return JsonResponse({"Error": f"Database error: {str(e)}"}, status=SERVER_ERROR_STATUS)

    # Validate Rating (if provided)
    if data.get('Rating'):
        is_valid, error_message = validate_rating(data['Rating'])
        if not is_valid:
            validation_results["Rating"] = {"is_valid": False, "message": error_message}

    # Validate Comments (if provided)
    if data.get('Comments'):
        is_valid, error_message = validate_field_lengths({'Comments': data['Comments']})
        if not is_valid:
            validation_results["Comments"] = {"is_valid": False, "message": error_message}

    # Validate Feedback_Type (if provided)
    if data.get('Feedback_Type'):
        is_valid, error_message = validate_feedback_type(data['Feedback_Type'])
        if not is_valid:
            validation_results["Feedback_Type"] = {"is_valid": False, "message": error_message}

    return JsonResponse(validation_results, status=SUCCESS_STATUS)

# Update URLs to include the endpoints
urlpatterns = [
    path('api/feedback/create', Create_Feedback, name='Create_Feedback'),
    path('api/feedback/feedbacks', List_All_Feedback, name='List_All_Feedback'),
    path('api/feedback/feedback/<int:id>', Get_Feedback, name='Get_Feedback'),
    path('api/feedback/update/<int:id>', Update_Feedback, name='Update_Feedback'),
    path('api/feedback/delete/<int:id>', Delete_Feedback, name='Delete_Feedback'),
    path('api/feedback/validate', Validate_Feedback_Data, name='Validate_Feedback_Data'),
]