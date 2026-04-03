import pymysql
import time

def run_benchmark():
    conn = pymysql.connect(host='localhost', user='root', password='Samarth@05', database='shuttle_system')
    cursor = conn.cursor()
    
    query = """
        SELECT b.BookingID, b.TripID, b.SeatNo, b.Status, t.Date, t.Status AS TripStatus 
        FROM BOOKING b 
        JOIN TRIP t ON b.TripID = t.TripID 
        WHERE b.MemberID = 1 AND t.Status != 'Cancelled'
        ORDER BY t.Date DESC
    """
    
    # Run it 50 times to get a stable average
    start_time = time.time()
    for _ in range(50):
        cursor.execute(query)
        cursor.fetchall()
    end_time = time.time()
    
    avg_time = ((end_time - start_time) / 50) * 1000 # Convert to milliseconds
    print(f"Average Execution Time: {avg_time:.2f} ms")
    conn.close()

if __name__ == '__main__':
    run_benchmark()