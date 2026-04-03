
EXPLAIN SELECT b.BookingID, b.TripID, b.SeatNo, b.Status, t.Date, t.Status AS TripStatus 
FROM BOOKING b 
JOIN TRIP t ON b.TripID = t.TripID 
WHERE b.MemberID = 1 AND t.Status != 'Cancelled'
ORDER BY t.Date DESC;

-- Index to instantly find all bookings for a specific member
create INDEX idx_member_booking ON BOOKING(MemberID, TripID);
-- drop index idx_member_booking on BOOKING;

-- Index to optimize the filtering of status and the sorting of the date
CREATE INDEX idx_trip_status_date ON TRIP(Status, Date);
-- drop index idx_trip_status_date on TRIP;


EXPLAIN SELECT b.BookingID, b.TripID, b.SeatNo, b.Status, t.Date, t.Status AS TripStatus 
FROM BOOKING b 
JOIN TRIP t ON b.TripID = t.TripID 
WHERE b.MemberID = 1 AND t.Status != 'Cancelled'
ORDER BY t.Date DESC;