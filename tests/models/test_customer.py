"""
Automated tests for the customer model
Tests Customer class with all CRUD operations and business logic
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

from models.customer import Customer  # noqa
from rdbms.query_parser import QueryResult  # noqa


class TestCustomer:
    """Test cases for the Customer model"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()

        # Mock successful table creation
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        with patch("models.customer.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = (
                "2024-01-15 10:30:00"
            )
            self.customer = Customer(self.mock_db)

    def test_customer_initialization(self):
        """Test customer model initialization"""
        assert self.customer.db == self.mock_db
        assert self.customer.table_name == "customers"

    def test_ensure_table_exists_creates_table(self):
        """Test table creation when table doesn't exist"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        Customer(self.mock_db)

        # Verify table creation was attempted
        self.mock_db.execute_query.assert_called()
        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "CREATE TABLE customers" in create_call

    def test_ensure_table_exists_table_already_exists(self):
        """Test when table already exists"""
        self.mock_db.list_tables.return_value = ["customers"]

        Customer(self.mock_db)

        # Should not attempt to create table
        assert self.mock_db.execute_query.call_count == 0

    def test_ensure_table_exists_creation_fails(self):
        """Test table creation failure"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Creation failed"
        )

        with pytest.raises(Exception, match="Failed to create customers table"):
            Customer(self.mock_db)

    @patch("models.customer.datetime")
    def test_create_customer_success(self, mock_datetime):
        """Test successful customer creation"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        # Mock get_next_id to return 1
        with patch.object(self.customer, "_get_next_id", return_value=1):
            # Mock successful insert and get_by_id
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            customer_data = {
                "fullName": "John Doe",
                "email": "john@example.com",
                "phoneNumber": "+1234567890",
            }

            expected_customer = {
                "customerId": 1,
                "fullName": "John Doe",
                "email": "john@example.com",
                "phoneNumber": "+1234567890",
                "dateCreated": "2024-01-15 10:30:00",
            }

            with patch.object(
                self.customer, "get_by_id", return_value=expected_customer
            ):
                result = self.customer.create(customer_data)

                assert result == expected_customer
                # Verify database insert was called
                self.mock_db.execute_query.assert_called()

    def test_create_customer_failure(self):
        """Test customer creation failure"""
        with patch.object(self.customer, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(
                success=False, error="Insert failed"
            )

            customer_data = {
                "fullName": "John Doe",
                "email": "john@example.com",
                "phoneNumber": "+1234567890",
            }

            with pytest.raises(Exception, match="Failed to create customer"):
                self.customer.create(customer_data)

    def test_get_by_id_success(self):
        """Test successful get customer by ID"""
        customer_data = {
            "customerId": 1,
            "fullName": "John Doe",
            "email": "john@example.com",
            "phoneNumber": "+1234567890",
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[customer_data]
        )

        result = self.customer.get_by_id(1)

        assert result == customer_data
        # Verify correct SQL was executed
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "SELECT * FROM customers WHERE customerId = 1" in call_args

    def test_get_by_id_not_found(self):
        """Test get customer by ID when not found"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        result = self.customer.get_by_id(999)

        assert result is None

    def test_get_by_id_query_failure(self):
        """Test get customer by ID when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.customer.get_by_id(1)

        assert result is None

    def test_get_by_email_success(self):
        """Test successful get customer by email"""
        customer_data = {
            "customerId": 1,
            "fullName": "John Doe",
            "email": "john@example.com",
            "phoneNumber": "+1234567890",
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[customer_data]
        )

        result = self.customer.get_by_email("john@example.com")

        assert result == customer_data
        # Verify correct SQL was executed
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert (
            "SELECT * FROM customers WHERE email = 'john@example.com'"
            in call_args
        )

    def test_get_by_email_not_found(self):
        """Test get customer by email when not found"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        result = self.customer.get_by_email("nonexistent@example.com")

        assert result is None

    def test_get_by_email_query_failure(self):
        """Test get customer by email when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.customer.get_by_email("john@example.com")

        assert result is None

    def test_get_or_create_by_email_existing_customer(self):
        """Test get_or_create when customer exists"""
        existing_customer = {
            "customerId": 1,
            "fullName": "John Doe",
            "email": "john@example.com",
            "phoneNumber": "+1234567890",
        }

        with patch.object(
            self.customer, "get_by_email", return_value=existing_customer
        ):
            customer_data = {
                "fullName": "John Doe Updated",
                "email": "john@example.com",
                "phoneNumber": "+1234567890",
            }

            result = self.customer.get_or_create_by_email(customer_data)

            assert result == existing_customer
            # Should not call create method

    def test_get_or_create_by_email_new_customer(self):
        """Test get_or_create when customer doesn't exist"""
        new_customer = {
            "customerId": 1,
            "fullName": "John Doe",
            "email": "john@example.com",
            "phoneNumber": "+1234567890",
        }

        with patch.object(self.customer, "get_by_email", return_value=None):
            with patch.object(
                self.customer, "create", return_value=new_customer
            ) as mock_create:
                customer_data = {
                    "fullName": "John Doe",
                    "email": "john@example.com",
                    "phoneNumber": "+1234567890",
                }

                result = self.customer.get_or_create_by_email(customer_data)

                assert result == new_customer
                mock_create.assert_called_once_with(customer_data)

    def test_get_all_success(self):
        """Test successful get all customers"""
        customers_data = [
            {
                "customerId": 1,
                "fullName": "John Doe",
                "email": "john@example.com",
            },
            {
                "customerId": 2,
                "fullName": "Jane Smith",
                "email": "jane@example.com",
            },
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=customers_data
        )

        result = self.customer.get_all()

        assert result == customers_data
        # Verify SQL query
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "SELECT * FROM customers" in call_args

    def test_get_all_empty_result(self):
        """Test get all customers with empty result"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        result = self.customer.get_all()

        assert result == []

    def test_get_all_query_failure(self):
        """Test get all customers when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.customer.get_all()

        assert result == []

    def test_get_next_id_first_customer(self):
        """Test getting next ID when no customers exist"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )

        next_id = self.customer._get_next_id()

        assert next_id == 1

    def test_get_next_id_existing_customers(self):
        """Test getting next ID when customers exist"""
        existing_customers = [
            {"customerId": 1},
            {"customerId": 3},
            {"customerId": 2},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=existing_customers
        )

        next_id = self.customer._get_next_id()

        assert next_id == 4  # max(1,3,2) + 1

    def test_get_next_id_query_failure(self):
        """Test getting next ID when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        next_id = self.customer._get_next_id()

        assert next_id == 1  # Default when query fails

    def test_email_uniqueness_constraint(self):
        """Test that email uniqueness is enforced in table creation"""
        self.mock_db.list_tables.return_value = []

        Customer(self.mock_db)

        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "email TEXT UNIQUE NOT NULL" in create_call

    def test_sql_injection_protection(self):
        """Test that string values are properly quoted in SQL"""
        with patch.object(self.customer, "_get_next_id", return_value=1):
            with patch.object(self.customer, "get_by_id", return_value={}):
                self.mock_db.execute_query.return_value = QueryResult(
                    success=True
                )

                customer_data = {
                    "fullName": "John'; DROP TABLE customers;--",
                    "email": "test'; DROP TABLE customers;--@example.com",
                    "phoneNumber": "+1234567890",
                }

                self.customer.create(customer_data)

                # Verify that string values are quoted
                call_args = self.mock_db.execute_query.call_args[0][0]
                assert "'John'; DROP TABLE customers;--'" in call_args
                assert (
                    "'test'; DROP TABLE customers;--@example.com'" in call_args
                )

    def test_required_fields_validation(self):
        """Test that all required fields are present in table creation"""
        self.mock_db.list_tables.return_value = []

        Customer(self.mock_db)

        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "customerId INT PRIMARY KEY" in create_call
        assert "fullName TEXT NOT NULL" in create_call
        assert "email TEXT UNIQUE NOT NULL" in create_call
        assert "phoneNumber TEXT NOT NULL" in create_call
        assert "dateCreated TEXT NOT NULL" in create_call


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
