"""
Automated tests for the booking model
Tests Booking class with all CRUD operations and business logic
"""

import os
import sys
import pytest
from unittest.mock import Mock
from unittest.mock import patch

# Add the parent directories to the path
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "models"))

from models.booking import Booking  # noqa
from rdbms.query_parser import QueryResult  # noqa


class TestBooking:
    """Test cases for the Booking model"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()

        # Mock successful table creation
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        with patch("models.booking.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = (
                "2024-01-15 10:30:00"
            )
            self.booking = Booking(self.mock_db)

    def test_booking_initialization(self):
        """Test booking model initialization"""
        assert self.booking.db == self.mock_db
        assert self.booking.table_name == "bookings"

    def test_ensure_table_exists_creates_table(self):
        """Test table creation when table doesn't exist"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        Booking(self.mock_db)

        # Verify table creation was attempted
        self.mock_db.execute_query.assert_called()
        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "CREATE TABLE bookings" in create_call

    def test_ensure_table_exists_table_already_exists(self):
        """Test when table already exists"""
        # Reset mock to clear setup calls
        self.mock_db.reset_mock()
        self.mock_db.list_tables.return_value = ["bookings"]

        Booking(self.mock_db)

        # Should not attempt to create table
        assert self.mock_db.execute_query.call_count == 0

    def test_ensure_table_exists_creation_fails(self):
        """Test table creation failure"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Creation failed"
        )

        with pytest.raises(Exception, match="Failed to create bookings table"):
            Booking(self.mock_db)

    @patch("models.booking.datetime")
    def test_create_booking_success(self, mock_datetime):
        """Test successful booking creation"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        # Mock get_next_id to return 1
        with patch.object(self.booking, "_get_next_id", return_value=1):
            # Mock successful insert and get_by_id
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            booking_data = {
                "customerId": 123,
                "username": "john_doe",
                "emailAddress": "john@example.com",
                "phoneNumber": "+1234567890",
                "checkInDate": "2024-02-01",
                "checkOutDate": "2024-02-05",
                "adultsCount": 2,
                "childrenCount": 1,
                "specialRequests": "Late check-in",
            }

            expected_booking = {
                "bookingId": 1,
                "customerId": 123,
                "username": "john_doe",
                "status": "pending",
            }

            with patch.object(
                self.booking, "get_by_id", return_value=expected_booking
            ):
                result = self.booking.create(booking_data)

                assert result == expected_booking
                # Verify database insert was called
                self.mock_db.execute_query.assert_called()

    @patch("models.booking.datetime")
    def test_create_booking_with_status(self, mock_datetime):
        """Test booking creation with custom status"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        with patch.object(self.booking, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            booking_data = {
                "customerId": 123,
                "username": "john_doe",
                "emailAddress": "john@example.com",
                "phoneNumber": "+1234567890",
                "checkInDate": "2024-02-01",
                "checkOutDate": "2024-02-05",
                "adultsCount": 2,
                "childrenCount": 0,
                "status": "confirmed",
            }

            with patch.object(
                self.booking, "get_by_id", return_value=booking_data
            ):
                result = self.booking.create(booking_data)

                # Should use provided status instead of default
                assert result["status"] == "confirmed"

    def test_create_booking_failure(self):
        """Test booking creation failure"""
        with patch.object(self.booking, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(
                success=False, error="Insert failed"
            )

            booking_data = {
                "customerId": 123,
                "username": "john_doe",
                "emailAddress": "john@example.com",
                "phoneNumber": "+1234567890",
                "checkInDate": "2024-02-01",
                "checkOutDate": "2024-02-05",
                "adultsCount": 2,
                "childrenCount": 0,
            }

            with pytest.raises(Exception, match="Failed to create booking"):
                self.booking.create(booking_data)

    def test_get_by_id_success(self):
        """Test successful get booking by ID"""
        booking_data = {
            "bookingId": 1,
            "customerId": 123,
            "username": "john_doe",
            "status": "pending",
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[booking_data]
        )

        result = self.booking.get_by_id(1)

        assert result == booking_data
        # Verify correct SQL was executed
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "SELECT * FROM bookings WHERE bookingId = 1" in call_args

    def test_get_by_id_not_found(self):
        """Test get booking by ID when not found"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        result = self.booking.get_by_id(999)

        assert result is None

    def test_get_by_id_query_failure(self):
        """Test get booking by ID when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.booking.get_by_id(1)

        assert result is None

    def test_get_by_customer_success(self):
        """Test successful get bookings by customer ID"""
        bookings_data = [
            {"bookingId": 1, "customerId": 123, "status": "pending"},
            {"bookingId": 2, "customerId": 123, "status": "confirmed"},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=bookings_data
        )

        result = self.booking.get_by_customer(123)

        assert result == bookings_data
        assert len(result) == 2

    def test_get_by_customer_no_results(self):
        """Test get bookings by customer ID with no results"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        result = self.booking.get_by_customer(123)

        assert result == []

    def test_get_by_customer_query_failure(self):
        """Test get bookings by customer ID when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.booking.get_by_customer(123)

        assert result == []

    def test_get_all_success(self):
        """Test successful get all bookings"""
        bookings_data = [
            {"bookingId": 1, "status": "pending"},
            {"bookingId": 2, "status": "confirmed"},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=bookings_data
        )

        result = self.booking.get_all()

        assert result == bookings_data
        # Verify ORDER BY clause
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "ORDER BY bookingId DESC" in call_args

    def test_get_by_status_success(self):
        """Test successful get bookings by status"""
        pending_bookings = [
            {"bookingId": 1, "status": "pending"},
            {"bookingId": 3, "status": "pending"},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=pending_bookings
        )

        result = self.booking.get_by_status("pending")

        assert result == pending_bookings
        # Verify WHERE clause
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "WHERE status = 'pending'" in call_args

    @patch("models.booking.datetime")
    def test_update_booking_success(self, mock_datetime):
        """Test successful booking update"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 11:30:00"
        )

        self.mock_db.execute_query.return_value = QueryResult(success=True)

        update_data = {
            "status": "confirmed",
            "specialRequests": "Updated request",
        }

        result = self.booking.update(1, update_data)

        assert result == True
        # Verify UPDATE query was called
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "UPDATE bookings SET" in call_args
        assert "status = 'confirmed'" in call_args
        assert "lastUpdated = '2024-01-15 11:30:00'" in call_args

    def test_update_booking_no_data(self):
        """Test booking update with no update data"""
        result = self.booking.update(1, {"bookingId": 1})  # Only bookingId

        assert result == False
        # Should not call database
        assert self.mock_db.execute_query.call_count == 0

    def test_update_booking_failure(self):
        """Test booking update failure"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Update failed"
        )

        update_data = {"status": "confirmed"}
        result = self.booking.update(1, update_data)

        assert result == False

    def test_delete_booking_success_pending(self):
        """Test successful deletion of pending booking"""
        # Mock get_by_id to return pending booking
        pending_booking = {"bookingId": 1, "status": "pending"}

        with patch.object(
            self.booking, "get_by_id", return_value=pending_booking
        ):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            result = self.booking.delete(1)

            assert result == True
            # Verify DELETE query was called
            call_args = self.mock_db.execute_query.call_args[0][0]
            assert "DELETE FROM bookings WHERE bookingId = 1" in call_args

    def test_delete_booking_success_cancelled(self):
        """Test successful deletion of cancelled booking"""
        cancelled_booking = {"bookingId": 1, "status": "cancelled"}

        with patch.object(
            self.booking, "get_by_id", return_value=cancelled_booking
        ):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            result = self.booking.delete(1)

            assert result == True

    def test_delete_booking_invalid_status(self):
        """Test deletion of booking with invalid status"""
        confirmed_booking = {"bookingId": 1, "status": "confirmed"}

        with patch.object(
            self.booking, "get_by_id", return_value=confirmed_booking
        ):
            result = self.booking.delete(1)

            assert result == False
            # Should not call DELETE query
            assert self.mock_db.execute_query.call_count == 0

    def test_delete_booking_not_found(self):
        """Test deletion of non-existent booking"""
        with patch.object(self.booking, "get_by_id", return_value=None):
            result = self.booking.delete(999)

            assert result == False

    def test_delete_booking_query_failure(self):
        """Test deletion failure at database level"""
        pending_booking = {"bookingId": 1, "status": "pending"}

        with patch.object(
            self.booking, "get_by_id", return_value=pending_booking
        ):
            self.mock_db.execute_query.return_value = QueryResult(
                success=False, error="Delete failed"
            )

            result = self.booking.delete(1)

            assert result == False

    def test_get_next_id_first_booking(self):
        """Test getting next ID when no bookings exist"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        next_id = self.booking._get_next_id()

        assert next_id == 1

    def test_get_next_id_existing_bookings(self):
        """Test getting next ID when bookings exist"""
        existing_bookings = [
            {"bookingId": 1},
            {"bookingId": 3},
            {"bookingId": 2},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=existing_bookings
        )

        next_id = self.booking._get_next_id()

        assert next_id == 4  # max(1,3,2) + 1

    def test_get_next_id_query_failure(self):
        """Test getting next ID when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        next_id = self.booking._get_next_id()

        assert next_id == 1  # Default when query fails

    def test_sql_injection_protection(self):
        """Test that string values are properly quoted in SQL"""
        with patch.object(self.booking, "_get_next_id", return_value=1):
            with patch.object(self.booking, "get_by_id", return_value={}):
                self.mock_db.execute_query.return_value = QueryResult(
                    success=True
                )

                booking_data = {
                    "customerId": 123,
                    "username": "john'drop;--",
                    "emailAddress": "test@example.com",
                    "phoneNumber": "+1234567890",
                    "checkInDate": "2024-02-01",
                    "checkOutDate": "2024-02-05",
                    "adultsCount": 2,
                    "childrenCount": 0,
                    "specialRequests": "Special'; DROP TABLE bookings;--",
                }

                self.booking.create(booking_data)

                # Verify that string values are quoted
                call_args = self.mock_db.execute_query.call_args[0][0]
                assert "'john'drop;--'" in call_args
                assert "'Special'; DROP TABLE bookings;--'" in call_args

    def test_boolean_and_numeric_handling(self):
        """Test proper handling of boolean and numeric values in queries"""
        update_data = {
            "adultsCount": 3,
            "childrenCount": 0,
            "status": "confirmed",
        }

        self.mock_db.execute_query.return_value = QueryResult(success=True)

        self.booking.update(1, update_data)

        call_args = self.mock_db.execute_query.call_args[0][0]
        # Numeric values should not be quoted
        assert "adultsCount = 3" in call_args
        assert "childrenCount = 0" in call_args
        # String values should be quoted
        assert "status = 'confirmed'" in call_args


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
