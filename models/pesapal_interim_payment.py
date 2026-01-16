"""
PesaPal Interim Payment model for resort booking system using custom RDBMS
Tracks payment processing with PesaPal
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


class PesapalInterimPayment:
    """PesaPal Interim Payment model - tracks payment processing"""

    def __init__(self, db: Database):
        self.db = db
        self.table_name = "pesapal_interim_payments"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create pesapal_interim_payments table if it doesn't exist"""
        if self.table_name not in self.db.list_tables():
            create_query = """
            CREATE TABLE pesapal_interim_payments (
                pesapalInterimPaymentId INT PRIMARY KEY,
                bookingId INT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                iframeSrc TEXT NOT NULL,
                orderTrackingId TEXT NOT NULL,
                merchantReference TEXT NOT NULL,
                dateCreated TEXT NOT NULL,
                lastUpdated TEXT NOT NULL
            )
            """
            result = self.db.execute_query(create_query)
            if not result.success:
                raise Exception(
                    f"Failed to create pesapal_interim_payments table: {result.error}"
                )

    def create(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new interim payment"""
        payment_id = self._get_next_id()

        payment_data["pesapalInterimPaymentId"] = payment_id
        payment_data["status"] = payment_data.get("status", "SAVED")
        payment_data["dateCreated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        payment_data["lastUpdated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        insert_query = f"""
        INSERT INTO pesapal_interim_payments (pesapalInterimPaymentId, bookingId, amount, status,
                                            iframeSrc, orderTrackingId, merchantReference, 
                                            dateCreated, lastUpdated)
        VALUES ({payment_data['pesapalInterimPaymentId']}, {payment_data['bookingId']}, 
                {payment_data['amount']}, '{payment_data['status']}', 
                '{payment_data['iframeSrc']}', '{payment_data['orderTrackingId']}', 
                '{payment_data['merchantReference']}', '{payment_data['dateCreated']}', 
                '{payment_data['lastUpdated']}')
        """

        result = self.db.execute_query(insert_query)
        if result.success:
            return self.get_by_id(payment_id)

        else:
            raise Exception(f"Failed to create interim payment: {result.error}")

    def get_by_id(self, payment_id: int) -> Optional[Dict[str, Any]]:
        """Get interim payment by ID"""
        query = f"SELECT * FROM pesapal_interim_payments WHERE pesapalInterimPaymentId = {payment_id}"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]

        return None

    def get_by_booking(self, booking_id: int) -> List[Dict[str, Any]]:
        """Get interim payments by booking ID"""
        query = f"SELECT * FROM pesapal_interim_payments WHERE bookingId = {booking_id}"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def get_by_order_tracking_id(
        self, tracking_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get interim payment by order tracking ID"""
        query = f"SELECT * FROM pesapal_interim_payments WHERE orderTrackingId = '{tracking_id}'"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]

        return None

    def update(self, payment_id: int, update_data: Dict[str, Any]) -> bool:
        """Update interim payment"""
        update_data["lastUpdated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        set_clauses = []
        for key, value in update_data.items():
            if key != "pesapalInterimPaymentId":
                if isinstance(value, str):
                    set_clauses.append(f"{key} = '{value}'")
                else:
                    set_clauses.append(f"{key} = {value}")

        if not set_clauses:
            return False

        query = f"UPDATE pesapal_interim_payments SET {', '.join(set_clauses)} WHERE pesapalInterimPaymentId = {payment_id}"
        result = self.db.execute_query(query)
        return result.success

    def delete(self, payment_id: int) -> bool:
        """Delete interim payment"""
        query = f"DELETE FROM pesapal_interim_payments WHERE pesapalInterimPaymentId = {payment_id}"
        result = self.db.execute_query(query)
        return result.success

    def _get_next_id(self) -> int:
        """Get next available interim payment ID"""
        query = "SELECT pesapalInterimPaymentId FROM pesapal_interim_payments"
        result = self.db.execute_query(query)

        if result.success and result.data:
            max_id = max(row["pesapalInterimPaymentId"] for row in result.data)
            return max_id + 1

        return 1
