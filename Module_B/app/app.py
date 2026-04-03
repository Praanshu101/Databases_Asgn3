import logging
from functools import wraps
from flask import Flask, request, jsonify, render_template
import pymysql
import jwt
import datetime
from werkzeug.security import check_password_hash

# Initialize Flask App
app = Flask(__name__)
app.config['SECRET_KEY'] = 'cs432_secret_key_'

# Setup local security logging
logging.basicConfig(
    filename='audit.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Database Connection Helper
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='root',          # Change to your MySQL user
        password='Samarth@05',  # Change to your MySQL password
        database='shuttle_system',
        cursorclass=pymysql.cursors.DictCursor
    )

# --- MIDDLEWARE ---
import time

@app.before_request
def start_timer():
    """Start a timer before every request."""
    request.start_time = time.time()

@app.after_request
def log_request_performance(response):
    """Calculate the execution time and log it for API routes."""
    # We only care about profiling the API endpoints, not the HTML page loads
    if request.path.startswith('/api/'):
        duration = (time.time() - request.start_time) * 1000  # Convert to milliseconds
        logging.info(f"PERFORMANCE: {request.method} {request.path} - Execution Time: {duration:.2f} ms")
    return response

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]
        
        if not token:
            logging.warning("NO SESSION FOUND: Attempted access without a valid session token.")
            return jsonify({'error': 'No session found'}), 401

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            current_user = data
        except jwt.ExpiredSignatureError:
            logging.warning(f"SESSION EXPIRED: User {data['username']} session expired.")
            return jsonify({'error': 'Session expired'}), 401
        except jwt.InvalidTokenError:
            logging.warning("INVALID SESSION: Attempted access with an invalid session token.")
            return jsonify({'error': 'Invalid session token'}), 401

        return f(current_user, *args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(current_user, *args, **kwargs):
        if current_user.get('role') != 'Admin':
            logging.warning(f"UNAUTHORIZED ACCESS ATTEMPT: User {current_user['username']} attempted to access Admin API.")
            return jsonify({'error': 'Admin access required'}), 403
        return f(current_user, *args, **kwargs)
    return decorated

# --- WEB UI ROUTES ---

@app.route('/')
def index():
    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

# --- API ENDPOINTS ---

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({'error': 'Missing parameters'}), 401

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM USER_ACCOUNT WHERE Username = %s", (data['username'],))
            user = cursor.fetchone()

            if user and user['PasswordHash'] == data['password']: 
                token_payload = {
                    'username': user['Username'],
                    'member_id': user['MemberID'],
                    'role': user['Role'],
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=2)
                }
                session_token = jwt.encode(token_payload, app.config['SECRET_KEY'], algorithm="HS256")
                
                logging.info(f"User {user['Username']} logged in successfully.")
                return jsonify({
                    'message': 'Login successful',
                    'session_token': session_token
                }), 200
            else:
                return jsonify({'error': 'Invalid credentials'}), 401
    finally:
        conn.close()

@app.route('/isAuth', methods=['GET'])
@token_required
def is_auth(current_user):
    return jsonify({
        "message": "User is authenticated", 
        "username": current_user['username'], 
        "role": current_user['role'], 
        "expiry": str(datetime.datetime.utcfromtimestamp(current_user['exp']))
    }), 200

# --- REGULAR USER APIs ---

@app.route('/api/profile', methods=['GET', 'PUT'])
@token_required
def profile(current_user):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if request.method == 'GET':
                cursor.execute("SELECT Name, Email, Phone FROM MEMBER WHERE MemberID = %s", (current_user['member_id'],))
                return jsonify(cursor.fetchone()), 200

            if request.method == 'PUT':
                data = request.get_json()
                cursor.execute("UPDATE MEMBER SET Email = %s, Phone = %s WHERE MemberID = %s", 
                               (data['email'], data['phone'], current_user['member_id']))
                conn.commit()
                logging.info(f"DATA MODIFICATION: User {current_user['username']} updated their profile.") 
                return jsonify({'message': 'Profile updated'}), 200
    finally:
        conn.close()

@app.route('/api/bookings', methods=['GET'])
@token_required
def user_bookings(current_user):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # cursor.execute("""
            #     SELECT b.BookingID, b.TripID, b.SeatNo, b.Status, t.Date, t.Status AS TripStatus 
            #     FROM BOOKING b 
            #     JOIN TRIP t ON b.TripID = t.TripID 
            #     WHERE b.MemberID = %s AND t.Status != 'Cancelled'
            # """, (current_user['member_id'],))
            cursor.execute("""
                SELECT b.BookingID, b.TripID, b.SeatNo, b.Status, t.Date, t.Status AS TripStatus 
                FROM BOOKING b 
                JOIN TRIP t ON b.TripID = t.TripID 
                WHERE b.MemberID = %s AND t.Status != 'Cancelled'
                ORDER BY t.Date DESC
            """, (current_user['member_id'],))
            return jsonify(cursor.fetchall()), 200
    finally:
        conn.close()

# --- ADMIN APIs ---

@app.route('/api/admin/form-data', methods=['GET'])
@token_required
@admin_required
def get_form_data(current_user):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT RouteID, Source, Destination FROM ROUTE")
            routes = cursor.fetchall()
            cursor.execute("SELECT ShuttleID, PlateNo, Capacity FROM SHUTTLE")
            shuttles = cursor.fetchall()
            cursor.execute("SELECT DriverID, Name FROM DRIVER")
            drivers = cursor.fetchall()
            return jsonify({'routes': routes, 'shuttles': shuttles, 'drivers': drivers}), 200
    finally:
        conn.close()

@app.route('/api/admin/trips', methods=['GET', 'POST'])
@token_required
@admin_required
def manage_trips(current_user):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if request.method == 'GET':
                cursor.execute("""
                    SELECT t.TripID, s.ScheduleID, t.Date, s.StartTime, s.EndTime, t.Status 
                    FROM TRIP t JOIN SCHEDULE s ON t.ScheduleID = s.ScheduleID
                """)
                trips = cursor.fetchall()
                
                # Convert timedelta objects to strings for JSON serialization
                for trip in trips:
                    if trip['StartTime'] is not None:
                        trip['StartTime'] = str(trip['StartTime'])
                    if trip['EndTime'] is not None:
                        trip['EndTime'] = str(trip['EndTime'])
                        
                return jsonify(trips), 200
            
            if request.method == 'POST':
                print("HERE2")

                data = request.get_json()
                day_of_week = datetime.datetime.strptime(data['date'], '%Y-%m-%d').strftime('%A')
                
                cursor.execute("SELECT MAX(ScheduleID) as max_id FROM SCHEDULE")
                new_schedule_id = (cursor.fetchone()['max_id'] or 0) + 1

                cursor.execute("""
                    INSERT INTO SCHEDULE (ScheduleID, RouteID, ShuttleID, DriverID, StartTime, EndTime, DayOfWeek) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (new_schedule_id, data['route_id'], data['shuttle_id'], data['driver_id'], data['start_time'], data['end_time'], day_of_week))

                cursor.execute("SELECT MAX(TripID) as max_id FROM TRIP")
                new_trip_id = (cursor.fetchone()['max_id'] or 0) + 1

                cursor.execute("INSERT INTO TRIP (TripID, ScheduleID, Date, Status) VALUES (%s, %s, %s, 'Scheduled')", 
                               (new_trip_id, new_schedule_id, data['date']))
                conn.commit()
                
                logging.info(f"ADMIN ACTION: Admin {current_user['username']} added a new trip (TripID: {new_trip_id}).")
                return jsonify({'message': 'Trip and Schedule added successfully'}), 201
    finally:
        conn.close()

@app.route('/api/change-password', methods=['PUT'])
@token_required
def change_password(current_user):
    data = request.get_json()
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password:
        return jsonify({'error': 'Missing parameters'}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Fetch current password hash 
            cursor.execute("SELECT PasswordHash FROM USER_ACCOUNT WHERE MemberID = %s", (current_user['member_id'],))
            user = cursor.fetchone()
            print("HERE3")

            # Verify current password matches what is in the DB
            if not user or user['PasswordHash'] != current_password:
                return jsonify({'error': 'Incorrect current password'}), 401

            # Update to new password
            cursor.execute("UPDATE USER_ACCOUNT SET PasswordHash = %s WHERE MemberID = %s", 
                           (new_password, current_user['member_id']))
            conn.commit()
            
            # Log the security event
            logging.info(f"SECURITY: User {current_user['username']} changed their password.")
            return jsonify({'message': 'Password updated successfully'}), 200
    finally:
        conn.close()

@app.route('/api/admin/trips/<int:trip_id>', methods=['DELETE'])
@token_required
@admin_required
def cancel_trip(current_user, trip_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE TRIP SET Status = 'Cancelled' WHERE TripID = %s", (trip_id,))
            conn.commit()
            logging.info(f"ADMIN ACTION: Admin {current_user['username']} cancelled TripID {trip_id}.")
            return jsonify({'message': 'Trip cancelled'}), 200
    finally:
        conn.close()

@app.route('/api/admin/users', methods=['GET', 'POST'])
@token_required
@admin_required
def manage_users(current_user):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if request.method == 'GET':
                cursor.execute("""
                    SELECT m.MemberID, m.Name, m.Email, COALESCE(u.Role, 'No Login') as Role 
                    FROM MEMBER m 
                    LEFT JOIN USER_ACCOUNT u ON m.MemberID = u.MemberID
                """)
                return jsonify(cursor.fetchall()), 200
                
            if request.method == 'POST':
                data = request.get_json()
                
                cursor.execute("SELECT MAX(MemberID) as max_id FROM MEMBER")
                new_member_id = (cursor.fetchone()['max_id'] or 0) + 1
                
                cursor.execute("INSERT INTO MEMBER (MemberID, Name, Email, Phone, Age) VALUES (%s, %s, %s, %s, %s)",
                               (new_member_id, data['name'], data['email'], data['phone'], data['age']))
                
                cursor.execute("SELECT MAX(AccountID) as max_id FROM USER_ACCOUNT")
                new_account_id = (cursor.fetchone()['max_id'] or 0) + 1
                
                cursor.execute("INSERT INTO USER_ACCOUNT (AccountID, MemberID, Username, PasswordHash, Role) VALUES (%s, %s, %s, %s, %s)",
                               (new_account_id, new_member_id, data['username'], 'password123', data['role']))
                conn.commit()
                
                logging.info(f"ADMIN ACTION: Admin {current_user['username']} added new user {data['username']} (MemberID: {new_member_id}).")
                return jsonify({'message': 'User added successfully'}), 201
    finally:
        conn.close()

@app.route('/api/admin/users/<int:member_id>', methods=['DELETE'])
@token_required
@admin_required
def delete_user(current_user, member_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT BookingID FROM BOOKING WHERE MemberID = %s", (member_id,))
            bookings = cursor.fetchall()
            for b in bookings:
                booking_id = b['BookingID']
                cursor.execute("DELETE FROM PENALTY WHERE BookingID = %s", (booking_id,))
                cursor.execute("DELETE FROM TICKET WHERE BookingID = %s", (booking_id,))
            
            cursor.execute("DELETE FROM BOOKING WHERE MemberID = %s", (member_id,))
            cursor.execute("DELETE FROM MEMBER WHERE MemberID = %s", (member_id,))
            
            conn.commit()
            logging.info(f"ADMIN ACTION: Admin {current_user['username']} deleted MemberID {member_id}.")
            return jsonify({'message': 'User and associated records deleted'}), 200
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)