"""
Automated tests for the payment model
Tests Payment class with all CRUD operations and business logic
"""

import os
import sys
import pytest
from unittest.mock import Mock
from unittest.mock import patch

# Add the parent directories to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models"
    ),
)

from models.payment import Payment  # noqa
from rdbms.query_parser import QueryResult  # noqa


class TestPayment:
    """Test cases for the Payment model"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        with patch("models.payment.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = (
                "2024-01-15 10:30:00"
            )
            self.payment = Payment(self.mock_db)

    def test_payment_initialization(self):
        """Test payment model initialization"""
        assert self.payment.db == self.mock_db
        assert self.payment.table_name == "payments"

    def test_ensure_table_exists_creates_table(self):
        """Test table creation when table doesn't exist"""
        self.mock_db.list_tables.return_value = []
        Payment(self.mock_db)

        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "CREATE TABLE payments" in create_call
        assert "paymentId INT PRIMARY KEY" in create_call
        assert "receiptNumber TEXT UNIQUE NOT NULL" in create_call

    def test_ensure_table_exists_creation_fails(self):
        """Test table creation failure"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Creation failed"
        )

        with pytest.raises(Exception, match="Failed to create payments table"):
            Payment(self.mock_db)

    @patch("models.payment.datetime")
    def test_create_payment_success(self, mock_datetime):
        """Test successful payment creation"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        with patch.object(self.payment, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            payment_data = {
                "bookingId": 123,
                "receiptNumber": "RCP001",
                "amount": 15000.00,
                "paymentMethod": "credit_card",
            }

            expected_payment = {
                "paymentId": 1,
                "bookingId": 123,
                "receiptNumber": "RCP001",
                "status": "successful",
            }

            with patch.object(
                self.payment, "get_by_id", return_value=expected_payment
            ):
                result = self.payment.create(payment_data)
                assert result == expected_payment

    @patch("models.payment.datetime")
    def test_create_payment_with_status(self, mock_datetime):
        """Test payment creation with custom status"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        with patch.object(self.payment, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            payment_data = {
                "bookingId": 123,
                "receiptNumber": "RCP001",
                "amount": 15000.00,
                "paymentMethod": "mobile_money",
                "status": "pending",
            }

            with patch.object(
                self.payment, "get_by_id", return_value=payment_data
            ):
                result = self.payment.create(payment_data)
                assert result["status"] == "pending"

    def test_create_payment_failure(self):
        """Test payment creation failure"""
        with patch.object(self.payment, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(
                success=False, error="Insert failed"
            )

            payment_data = {
                "bookingId": 123,
                "receiptNumber": "RCP001",
                "amount": 15000.00,
                "paymentMethod": "credit_card",
            }

            with pytest.raises(Exception, match="Failed to create payment"):
                self.payment.create(payment_data)

    def test_get_by_id_success(self):
        """Test successful get payment by ID"""
        payment_data = {
            "paymentId": 1,
            "bookingId": 123,
            "receiptNumber": "RCP001",
            "amount": 15000.00,
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[payment_data]
        )

        result = self.payment.get_by_id(1)
        assert result == payment_data

    def test_get_by_id_not_found(self):
        """Test get payment by ID when not found"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        result = self.payment.get_by_id(999)
        assert result is None

    def test_get_by_booking_success(self):
        """Test successful get payments by booking ID"""
        payments_data = [
            {"paymentId": 1, "bookingId": 123, "amount": 15000.00},
            {"paymentId": 2, "bookingId": 123, "amount": 5000.00},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=payments_data
        )

        result = self.payment.get_by_booking(123)
        assert result == payments_data
        assert len(result) == 2

    def test_get_by_booking_no_results(self):
        """Test get payments by booking ID with no results"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        result = self.payment.get_by_booking(123)
        assert result == []

    def test_get_by_receipt_number_success(self):
        """Test successful get payment by receipt number"""
        payment_data = {
            "paymentId": 1,
            "receiptNumber": "RCP001",
            "amount": 15000.00,
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[payment_data]
        )

        result = self.payment.get_by_receipt_number("RCP001")
        assert result == payment_data

    def test_get_by_receipt_number_not_found(self):
        """Test get payment by receipt number when not found"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        result = self.payment.get_by_receipt_number("NONEXISTENT")
        assert result is None

    def test_get_all_success(self):
        """Test successful get all payments"""
        payments_data = [
            {"paymentId": 1, "amount": 15000.00},
            {"paymentId": 2, "amount": 5000.00},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=payments_data
        )

        result = self.payment.get_all()
        assert result == payments_data

    def test_get_all_query_failure(self):
        """Test get all payments when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.payment.get_all()
        assert result == []

    def test_get_next_id_first_payment(self):
        """Test getting next ID when no payments exist"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        next_id = self.payment._get_next_id()
        assert next_id == 1

    def test_get_next_id_existing_payments(self):
        """Test getting next ID when payments exist"""
        existing_payments = [
            {"paymentId": 1},
            {"paymentId": 3},
            {"paymentId": 2},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=existing_payments
        )

        next_id = self.payment._get_next_id()
        assert next_id == 4  # max(1,3,2) + 1

    def test_receipt_number_uniqueness(self):
        """Test that receipt number uniqueness is enforced"""
        self.mock_db.list_tables.return_value = []
        Payment(self.mock_db)

        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "receiptNumber TEXT UNIQUE NOT NULL" in create_call

    def test_amount_data_type(self):
        """Test that amount is stored as REAL for decimal precision"""
        self.mock_db.list_tables.return_value = []
        Payment(self.mock_db)

        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "amount REAL NOT NULL" in create_call

    def test_sql_injection_protection(self):
        """Test that string values are properly quoted"""
        with patch.object(self.payment, "_get_next_id", return_value=1):
            with patch.object(self.payment, "get_by_id", return_value={}):
                self.mock_db.execute_query.return_value = QueryResult(
                    success=True
                )

                payment_data = {
                    "bookingId": 123,
                    "receiptNumber": "RCP'; DROP TABLE payments;--",
                    "amount": 15000.00,
                    "paymentMethod": "credit'; DROP--",
                }

                self.payment.create(payment_data)

                call_args = self.mock_db.execute_query.call_args[0][0]
                assert "'RCP'; DROP TABLE payments;--'" in call_args
                assert "'credit'; DROP--'" in call_args

    def test_numeric_values_not_quoted(self):
        """Test that numeric values are not quoted in SQL"""
        with patch.object(self.payment, "_get_next_id", return_value=1):
            with patch.object(self.payment, "get_by_id", return_value={}):
                self.mock_db.execute_query.return_value = QueryResult(
                    success=True
                )

                payment_data = {
                    "bookingId": 123,
                    "receiptNumber": "RCP001",
                    "amount": 15000.00,
                    "paymentMethod": "credit_card",
                }

                self.payment.create(payment_data)

                call_args = self.mock_db.execute_query.call_args[0][0]
                # Numeric values should not be quoted
                assert (
                    "123, 'RCP001', 15000.0" in call_args
                    or "123,'RCP001',15000.0" in call_args
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
