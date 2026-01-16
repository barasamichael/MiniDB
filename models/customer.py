"""
Customer model for resort booking system using custom RDBMS
Guests who make bookings - not for login, just contact info
"""
import sys
from typing import Any
from typing import Dict
from typing import List
from pathlib import Path
from typing import Optional
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent / "rdbms"))

from database import Database  # noqa


class Customer:
    """Customer model - guests making bookings (no authentication)"""

    def __init__(self, db: Database):
        self.db = db
        self.table_name = "customers"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create customers table if it doesn't exist"""
        if self.table_name not in self.db.list_tables():
            create_query = """
            CREATE TABLE customers (
                customerId INT PRIMARY KEY,
                fullName TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phoneNumber TEXT NOT NULL,
                dateCreated TEXT NOT NULL
            )
            """
            result = self.db.execute_query(create_query)
            if not result.success:
                raise Exception(
                    f"Failed to create customers table: {result.error}"
                )

    def create(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new customer"""
        customer_id = self._get_next_id()

        customer_data["customerId"] = customer_id
        customer_data["dateCreated"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        insert_query = f"""
        INSERT INTO customers (customerId, fullName, email, phoneNumber, dateCreated)
        VALUES ({customer_id}, '{customer_data['fullName']}', '{customer_data['email']}', 
                '{customer_data['phoneNumber']}', '{customer_data['dateCreated']}')
        """

        result = self.db.execute_query(insert_query)
        if result.success:
            return self.get_by_id(customer_id)

        else:
            raise Exception(f"Failed to create customer: {result.error}")

    def get_by_id(self, customer_id: int) -> Optional[Dict[str, Any]]:
        """Get customer by ID"""
        query = f"SELECT * FROM customers WHERE customerId = {customer_id}"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]

        return None

    def get_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get customer by email"""
        query = f"SELECT * FROM customers WHERE email = '{email}'"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]

        return None

    def get_or_create_by_email(
        self, customer_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get existing customer or create new one by email"""
        existing = self.get_by_email(customer_data["email"])
        if existing:
            return existing

        return self.create(customer_data)

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all customers"""
        query = "SELECT * FROM customers"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def _get_next_id(self) -> int:
        """Get next available customer ID"""
        query = "SELECT customerId FROM customers"
        result = self.db.execute_query(query)

        if result.success and result.data:
            max_id = max(row["customerId"] for row in result.data)
            return max_id + 1

        return 1
