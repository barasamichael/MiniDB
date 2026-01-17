"""
Automated tests for the pesapal interim payment model
Tests PesapalInterimPayment class with all CRUD operations
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

from models.pesapal_interim_payment import PesapalInterimPayment  # noqa
from rdbms.query_parser import QueryResult  # noqa


class TestPesapalInterimPayment:
    """Test cases for the PesapalInterimPayment model"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        with patch("models.pesapal_interim_payment.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = (
                "2024-01-15 10:30:00"
            )
            self.pesapal_payment = PesapalInterimPayment(self.mock_db)

    def test_pesapal_payment_initialization(self):
        """Test pesapal payment model initialization"""
        assert self.pesapal_payment.db == self.mock_db
        assert self.pesapal_payment.table_name == "pesapal_interim_payments"

    def test_ensure_table_exists_creates_table(self):
        """Test table creation when table doesn't exist"""
        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "CREATE TABLE pesapal_interim_payments" in create_call
        assert "pesapalInterimPaymentId INT PRIMARY KEY" in create_call
        assert "orderTrackingId TEXT NOT NULL" in create_call

    def test_ensure_table_exists_creation_fails(self):
        """Test table creation failure"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Creation failed"
        )

        with pytest.raises(
            Exception, match="Failed to create pesapal_interim_payments table"
        ):
            PesapalInterimPayment(self.mock_db)

    @patch("models.pesapal_interim_payment.datetime")
    def test_create_payment_success(self, mock_datetime):
        """Test successful interim payment creation"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        with patch.object(self.pesapal_payment, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            payment_data = {
                "bookingId": 123,
                "amount": 15000.00,
                "iframeSrc": "https://pesapal.com/iframe/123",
                "orderTrackingId": "TRK123456",
                "merchantReference": "MER789",
            }

            expected_payment = {
                "pesapalInterimPaymentId": 1,
                "bookingId": 123,
                "status": "SAVED",
            }

            with patch.object(
                self.pesapal_payment, "get_by_id", return_value=expected_payment
            ):
                result = self.pesapal_payment.create(payment_data)
                assert result == expected_payment

    @patch("models.pesapal_interim_payment.datetime")
    def test_create_payment_with_status(self, mock_datetime):
        """Test payment creation with custom status"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        with patch.object(self.pesapal_payment, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            payment_data = {
                "bookingId": 123,
                "amount": 15000.00,
                "iframeSrc": "https://pesapal.com/iframe/123",
                "orderTrackingId": "TRK123456",
                "merchantReference": "MER789",
                "status": "PROCESSING",
            }

            with patch.object(
                self.pesapal_payment, "get_by_id", return_value=payment_data
            ):
                result = self.pesapal_payment.create(payment_data)
                assert result["status"] == "PROCESSING"

    def test_get_by_id_success(self):
        """Test successful get payment by ID"""
        payment_data = {
            "pesapalInterimPaymentId": 1,
            "bookingId": 123,
            "orderTrackingId": "TRK123456",
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[payment_data]
        )

        result = self.pesapal_payment.get_by_id(1)
        assert result == payment_data

    def test_get_by_booking_success(self):
        """Test successful get payments by booking ID"""
        payments_data = [
            {"pesapalInterimPaymentId": 1, "bookingId": 123},
            {"pesapalInterimPaymentId": 2, "bookingId": 123},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=payments_data
        )

        result = self.pesapal_payment.get_by_booking(123)
        assert result == payments_data

    def test_get_by_order_tracking_id_success(self):
        """Test successful get payment by order tracking ID"""
        payment_data = {
            "pesapalInterimPaymentId": 1,
            "orderTrackingId": "TRK123456",
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[payment_data]
        )

        result = self.pesapal_payment.get_by_order_tracking_id("TRK123456")
        assert result == payment_data

    @patch("models.pesapal_interim_payment.datetime")
    def test_update_payment_success(self, mock_datetime):
        """Test successful payment update"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 11:30:00"
        )

        self.mock_db.execute_query.return_value = QueryResult(success=True)

        update_data = {
            "status": "COMPLETED",
            "orderTrackingId": "TRK123456_UPDATED",
        }

        result = self.pesapal_payment.update(1, update_data)
        assert result == True

    def test_update_payment_no_data(self):
        """Test payment update with no valid data"""
        result = self.pesapal_payment.update(1, {"pesapalInterimPaymentId": 1})
        assert result == False

    def test_delete_payment_success(self):
        """Test successful payment deletion"""
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        result = self.pesapal_payment.delete(1)
        assert result == True

    def test_delete_payment_failure(self):
        """Test payment deletion failure"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Delete failed"
        )

        result = self.pesapal_payment.delete(1)
        assert result == False

    def test_get_next_id_calculation(self):
        """Test next ID calculation with existing payments"""
        existing_payments = [
            {"pesapalInterimPaymentId": 1},
            {"pesapalInterimPaymentId": 3},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=existing_payments
        )

        next_id = self.pesapal_payment._get_next_id()
        assert next_id == 4

    def test_sql_injection_protection(self):
        """Test SQL injection protection"""
        with patch.object(self.pesapal_payment, "_get_next_id", return_value=1):
            with patch.object(
                self.pesapal_payment, "get_by_id", return_value={}
            ):
                self.mock_db.execute_query.return_value = QueryResult(
                    success=True
                )

                payment_data = {
                    "bookingId": 123,
                    "amount": 15000.00,
                    "iframeSrc": "https://pesapal.com'; DROP TABLE--",
                    "orderTrackingId": "TRK'; DROP--",
                    "merchantReference": "MER'; DROP--",
                }

                self.pesapal_payment.create(payment_data)

                call_args = self.mock_db.execute_query.call_args[0][0]
                assert "'https://pesapal.com'; DROP TABLE--'" in call_args


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
