"""
Booking model for resort booking system using custom RDBMS
Main booking records - matches your original Outback structure
"""
import sys
from pathlib import Path
from datetime import datetime

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

sys.path.append(str(Path(__file__).parent.parent / "rdbms"))

from database import Database  # noqa


class Booking:
    """Booking model - main booking records"""

    def __init__(self, db: Database):
        self.db = db
        self.table_name = "bookings"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create bookings table if it doesn't exist"""
        if self.table_name not in self.db.list_tables():
            create_query = """
            CREATE TABLE bookings (
                bookingId INT PRIMARY KEY,
                customerId INT NOT NULL,
                username TEXT NOT NULL,
                emailAddress TEXT NOT NULL,
                phoneNumber TEXT NOT NULL,
                checkInDate TEXT NOT NULL,
                checkOutDate TEXT NOT NULL,
                adultsCount INT NOT NULL,
                childrenCount INT NOT NULL,
                specialRequests TEXT,
                status TEXT NOT NULL,
                dateCreated TEXT NOT NULL,
                lastUpdated TEXT NOT NULL
            )
            """
            result = self.db.execute_query(create_query)
            if not result.success:
                raise Exception(
                    f"Failed to create bookings table: {result.error}"
                )

    def create(self, booking_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new booking"""
        booking_id = self._get_next_id()

        booking_data["bookingId"] = booking_id
        booking_data["status"] = booking_data.get("status", "pending")
        booking_data["dateCreated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        booking_data["lastUpdated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        insert_query = f"""
        INSERT INTO bookings (bookingId, customerId, username, emailAddress, phoneNumber,
                            checkInDate, checkOutDate, adultsCount, childrenCount, 
                            specialRequests, status, dateCreated, lastUpdated)
        VALUES ({booking_data['bookingId']}, {booking_data['customerId']}, 
                '{booking_data['username']}', '{booking_data['emailAddress']}', 
                '{booking_data['phoneNumber']}', '{booking_data['checkInDate']}', 
                '{booking_data['checkOutDate']}', {booking_data['adultsCount']}, 
                {booking_data['childrenCount']}, '{booking_data.get('specialRequests', '')}',
                '{booking_data['status']}', '{booking_data['dateCreated']}', 
                '{booking_data['lastUpdated']}')
        """

        result = self.db.execute_query(insert_query)
        if result.success:
            return self.get_by_id(booking_id)

        else:
            raise Exception(f"Failed to create booking: {result.error}")

    def get_by_id(self, booking_id: int) -> Optional[Dict[str, Any]]:
        """Get booking by ID"""
        query = f"SELECT * FROM bookings WHERE bookingId = {booking_id}"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]

        return None

    def get_by_customer(self, customer_id: int) -> List[Dict[str, Any]]:
        """Get bookings by customer ID"""
        query = f"SELECT * FROM bookings WHERE customerId = {customer_id}"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all bookings"""
        query = "SELECT * FROM bookings ORDER BY bookingId DESC"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def get_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get bookings by status"""
        query = f"SELECT * FROM bookings WHERE status = '{status}'"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def update(self, booking_id: int, update_data: Dict[str, Any]) -> bool:
        """Update booking"""
        update_data["lastUpdated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        set_clauses = []
        for key, value in update_data.items():
            if key != "bookingId":
                if isinstance(value, str):
                    set_clauses.append(f"{key} = '{value}'")

                else:
                    set_clauses.append(f"{key} = {value}")

        if not set_clauses:
            return False

        query = f"UPDATE bookings SET {', '.join(set_clauses)} WHERE bookingId = {booking_id}"
        result = self.db.execute_query(query)
        return result.success

    def delete(self, booking_id: int) -> bool:
        """Delete booking if pending or cancelled"""
        booking = self.get_by_id(booking_id)
        if booking and booking["status"] in ["pending", "cancelled"]:
            query = f"DELETE FROM bookings WHERE bookingId = {booking_id}"
            result = self.db.execute_query(query)
            return result.success

        return False

    def _get_next_id(self) -> int:
        """Get next available booking ID"""
        query = "SELECT bookingId FROM bookings"
        result = self.db.execute_query(query)

        if result.success and result.data:
            max_id = max(row["bookingId"] for row in result.data)
            return max_id + 1

        return 1
