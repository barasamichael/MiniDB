"""
Payment model for resort booking system using custom RDBMS
Completed payment records
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


class Payment:
    """Payment model - completed payment records"""

    def __init__(self, db: Database):
        self.db = db
        self.table_name = "payments"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create payments table if it doesn't exist"""
        if self.table_name not in self.db.list_tables():
            create_query = """
            CREATE TABLE payments (
                paymentId INT PRIMARY KEY,
                bookingId INT NOT NULL,
                receiptNumber TEXT UNIQUE NOT NULL,
                amount REAL NOT NULL,
                paymentMethod TEXT NOT NULL,
                status TEXT NOT NULL,
                processedAt TEXT NOT NULL
            )
            """
            result = self.db.execute_query(create_query)
            if not result.success:
                raise Exception(
                    f"Failed to create payments table: {result.error}"
                )

    def create(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new payment"""
        payment_id = self._get_next_id()

        payment_data["paymentId"] = payment_id
        payment_data["status"] = payment_data.get("status", "successful")
        payment_data["processedAt"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        insert_query = f"""
        INSERT INTO payments (paymentId, bookingId, receiptNumber, amount, paymentMethod, status, processedAt)
        VALUES ({payment_data['paymentId']}, {payment_data['bookingId']}, 
                '{payment_data['receiptNumber']}', {payment_data['amount']}, 
                '{payment_data['paymentMethod']}', '{payment_data['status']}', 
                '{payment_data['processedAt']}')
        """

        result = self.db.execute_query(insert_query)
        if result.success:
            return self.get_by_id(payment_id)
        else:
            raise Exception(f"Failed to create payment: {result.error}")

    def get_by_id(self, payment_id: int) -> Optional[Dict[str, Any]]:
        """Get payment by ID"""
        query = f"SELECT * FROM payments WHERE paymentId = {payment_id}"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]
        return None

    def get_by_booking(self, booking_id: int) -> List[Dict[str, Any]]:
        """Get payments by booking ID"""
        query = f"SELECT * FROM payments WHERE bookingId = {booking_id}"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []
        return []

    def get_by_receipt_number(
        self, receipt_number: str
    ) -> Optional[Dict[str, Any]]:
        """Get payment by receipt number"""
        query = (
            f"SELECT * FROM payments WHERE receiptNumber = '{receipt_number}'"
        )
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]
        return None

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all payments"""
        query = "SELECT * FROM payments"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []
        return []

    def _get_next_id(self) -> int:
        """Get next available payment ID"""
        query = "SELECT paymentId FROM payments"
        result = self.db.execute_query(query)

        if result.success and result.data:
            max_id = max(row["paymentId"] for row in result.data)
            return max_id + 1
        return 1
