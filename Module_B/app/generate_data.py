import pymysql
import random
import datetime

def get_db_connection():
    return pymysql.connect(host='localhost', user='root', password='Samarth@05', database='shuttle_system')

def seed_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    print("Fetching valid references from the database...")

    try:
        # 1. Fetch valid MemberIDs so we don't hit Foreign Key errors
        cursor.execute("SELECT MemberID FROM MEMBER")
        member_ids = [row[0] for row in cursor.fetchall()]
        
        if not member_ids:
            print("Error: No members found! Please add a member via the Admin dashboard first.")
            return

        # 2. Fetch a valid ScheduleID
        cursor.execute("SELECT ScheduleID FROM SCHEDULE LIMIT 1")
        schedule_row = cursor.fetchone()
        if not schedule_row:
            print("Error: No schedules found!")
            return
        valid_schedule_id = schedule_row[0]

        # 3. Generate 1,000 Dummy Trips
        print("Inserting 1,000 dummy Trips...")
        cursor.execute("SELECT MAX(TripID) FROM TRIP")
        max_trip = cursor.fetchone()[0] or 0
        
        trip_data = []
        valid_trip_ids = []
        for i in range(1, 1001):
            new_trip_id = max_trip + i
            date = datetime.date.today() + datetime.timedelta(days=random.randint(-30, 30))
            trip_data.append((new_trip_id, valid_schedule_id, date, 'Scheduled'))
            valid_trip_ids.append(new_trip_id)
        
        cursor.executemany(
            "INSERT INTO TRIP (TripID, ScheduleID, Date, Status) VALUES (%s, %s, %s, %s)", 
            trip_data
        )

        # 4. Generate 50,000 Dummy Bookings
        print("Inserting 50,000 dummy Bookings... This will take a few seconds.")
        cursor.execute("SELECT MAX(BookingID) FROM BOOKING")
        max_booking = cursor.fetchone()[0] or 0

        booking_data = []
        for i in range(1, 50001):
            new_booking_id = max_booking + i
            # Randomly pick from the members that ACTUALLY exist in the DB
            member_id = random.choice(member_ids)  
            trip_id = random.choice(valid_trip_ids)
            seat_no = random.randint(1, 40)
            booking_data.append((new_booking_id, member_id, trip_id, seat_no, 'Confirmed'))

        cursor.executemany(
            "INSERT INTO BOOKING (BookingID, MemberID, TripID, SeatNo, Status) VALUES (%s, %s, %s, %s, %s)", 
            booking_data
        )
        
        conn.commit()
        print("Success! The database is now scaled up for benchmarking.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    seed_database()