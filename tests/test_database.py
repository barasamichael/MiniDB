"""
Comprehensive automated tests for the database module
Tests Database class with all functionality including SQL execution
"""

import os
import sys
import pytest
import tempfile
from unittest.mock import Mock
from unittest.mock import patch

# Add the parent directory to the path to import the rdbms module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.query_parser import QueryResult
from rdbms.query_parser import ParsedQuery
from rdbms.database import Database

class TestDatabase:
    """Test cases for the Database class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp(prefix="test_database_")

        # Mock dependencies to avoid file system operations
        with patch("rdbms.database.QueryParser") as mock_parser, patch(
            "rdbms.database.StorageEngine"
        ) as mock_storage_engine, patch(
            "rdbms.database.MemoryStorage"
        ) as mock_memory_storage:
            # Setup mock parser
            self.mock_parser = Mock()
            mock_parser.return_value = self.mock_parser

            # Setup mock storage
            self.mock_storage = Mock()
            mock_storage_engine.return_value = self.mock_storage
            mock_memory_storage.return_value = self.mock_storage

            # Mock storage.list_tables to return empty list initially
            self.mock_storage.list_tables.return_value = []

            # Create database instance
            self.database = Database("test_db", "file", self.test_dir)

            # Manually set mocked objects
            self.database.storage = self.mock_storage

    def teardown_method(self):
        """Clean up after tests"""
        if os.path.exists(self.test_dir):
            import shutil

            shutil.rmtree(self.test_dir)

    @patch("rdbms.database.StorageEngine")
    @patch("rdbms.database.QueryParser")
    def test_database_initialization_file_storage(
        self, mock_parser, mock_storage_engine
    ):
        """Test database initialization with file storage"""
        mock_storage = Mock()
        mock_storage.list_tables.return_value = []
        mock_storage_engine.return_value = mock_storage

        db = Database("test_db", "file", "/test/dir", "json")

        assert db.name == "test_db"
        mock_storage_engine.assert_called_once_with("/test/dir", "json")
        mock_parser.assert_called_once()

    @patch("rdbms.database.MemoryStorage")
    @patch("rdbms.database.QueryParser")
    def test_database_initialization_memory_storage(
        self, mock_parser, mock_memory_storage
    ):
        """Test database initialization with memory storage"""
        mock_storage = Mock()
        mock_storage.list_tables.return_value = []
        mock_memory_storage.return_value = mock_storage

        db = Database("test_db", "memory")

        assert db.name == "test_db"
        mock_memory_storage.assert_called_once()

    @patch("rdbms.database.Table.from_dict")
    def test_load_tables_from_storage_success(self, mock_from_dict):
        """Test successful loading of tables from storage"""
        # Setup mock storage to return table names and data
        self.mock_storage.list_tables.return_value = ["users", "products"]

        table_data = {"name": "users", "columns": [], "rows": []}
        self.mock_storage.load_table.return_value = table_data

        mock_table = Mock()
        mock_from_dict.return_value = mock_table

        # Call the private method directly
        self.database._load_tables_from_storage()

        # Verify tables were loaded
        assert "users" in self.database.tables
        assert "products" in self.database.tables
        assert self.database.tables["users"] == mock_table

    @patch("rdbms.database.Table.from_dict")
    @patch("builtins.print")
    def test_load_tables_from_storage_error(self, mock_print, mock_from_dict):
        """Test loading tables with error"""
        self.mock_storage.list_tables.return_value = ["corrupt_table"]
        self.mock_storage.load_table.return_value = {"name": "corrupt_table"}

        mock_from_dict.side_effect = Exception("Corrupted table data")

        self.database._load_tables_from_storage()

        # Verify warning was printed
        mock_print.assert_called_with(
            "Warning: Could not load table 'corrupt_table': Corrupted table data"
        )

    def test_save_table_to_storage_success(self):
        """Test successful table saving to storage"""
        mock_table = Mock()
        mock_table.to_dict.return_value = {
            "name": "users",
            "data": "table_data",
        }
        self.database.tables["users"] = mock_table

        self.mock_storage.save_table.return_value = True

        result = self.database._save_table_to_storage("users")

        assert result == True
        self.mock_storage.save_table.assert_called_once_with(
            "users", {"name": "users", "data": "table_data"}
        )

    def test_save_table_to_storage_table_not_found(self):
        """Test saving non-existent table to storage"""
        result = self.database._save_table_to_storage("nonexistent")
        assert result == False

    def test_execute_query_success(self):
        """Test successful query execution"""
        parsed_query = ParsedQuery(query_type="SELECT", table_name="users")
        self.mock_parser.parse.return_value = parsed_query

        with patch.object(self.database, "_execute_select") as mock_execute:
            expected_result = QueryResult(success=True, data=[])
            mock_execute.return_value = expected_result

            result = self.database.execute_query("SELECT * FROM users")

            assert result == expected_result
            self.mock_parser.parse.assert_called_once_with(
                "SELECT * FROM users"
            )
            mock_execute.assert_called_once_with(parsed_query)

    def test_execute_query_parsing_error(self):
        """Test query execution with parsing error"""
        self.mock_parser.parse.side_effect = Exception("Invalid SQL")

        result = self.database.execute_query("INVALID SQL")

        assert result.success == False
        assert "Invalid SQL" in result.error

    def test_execute_query_unsupported_type(self):
        """Test query execution with unsupported query type"""
        parsed_query = ParsedQuery(query_type="UNSUPPORTED")
        self.mock_parser.parse.return_value = parsed_query

        result = self.database.execute_query("UNSUPPORTED QUERY")

        assert result.success == False
        assert "Unsupported query type" in result.error

    def test_execute_create_table_success(self):
        """Test CREATE TABLE execution"""
        parsed_query = ParsedQuery(
            query_type="CREATE_TABLE",
            table_name="users",
            columns=[
                {
                    "name": "id",
                    "type": "INT",
                    "primary_key": True,
                    "unique": False,
                    "nullable": False,
                },
                {
                    "name": "name",
                    "type": "TEXT",
                    "primary_key": False,
                    "unique": False,
                    "nullable": True,
                },
            ],
        )

        self.mock_storage.save_table.return_value = True

        with patch("rdbms.database.Column") as mock_column, patch(
            "rdbms.database.Table"
        ) as mock_table:
            mock_col_instance = Mock()
            mock_column.return_value = mock_col_instance

            mock_table_instance = Mock()
            mock_table.return_value = mock_table_instance

            result = self.database._execute_create_table(parsed_query)

            assert result.success == True
            assert "created successfully" in result.message
            assert "users" in self.database.tables

    def test_execute_create_table_already_exists(self):
        """Test CREATE TABLE with existing table"""
        self.database.tables["users"] = Mock()

        parsed_query = ParsedQuery(
            query_type="CREATE_TABLE", table_name="users"
        )

        result = self.database._execute_create_table(parsed_query)

        assert result.success == False
        assert "already exists" in result.error

    def test_execute_create_table_storage_error(self):
        """Test CREATE TABLE with storage error"""
        parsed_query = ParsedQuery(
            query_type="CREATE_TABLE",
            table_name="users",
            columns=[
                {
                    "name": "id",
                    "type": "INT",
                    "primary_key": True,
                    "unique": False,
                    "nullable": False,
                }
            ],
        )

        self.mock_storage.save_table.return_value = False

        with patch("rdbms.database.Column"), patch("rdbms.database.Table"):
            result = self.database._execute_create_table(parsed_query)

            assert result.success == False
            assert "Failed to save table" in result.error

    def test_execute_insert_success(self):
        """Test INSERT execution"""
        mock_table = Mock()
        mock_table.column_order = ["id", "name"]
        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(
            query_type="INSERT",
            table_name="users",
            values=[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
        )

        with patch.object(self.database, "_save_table_to_storage"):
            result = self.database._execute_insert(parsed_query)

            assert result.success == True
            assert result.rows_affected == 2
            assert "Inserted 2 row(s)" in result.message
            assert mock_table.insert.call_count == 2

    def test_execute_insert_table_not_found(self):
        """Test INSERT with non-existent table"""
        parsed_query = ParsedQuery(
            query_type="INSERT", table_name="nonexistent"
        )

        result = self.database._execute_insert(parsed_query)

        assert result.success == False
        assert "does not exist" in result.error

    def test_execute_insert_with_values_only(self):
        """Test INSERT with values only (no column names)"""
        mock_table = Mock()
        mock_table.column_order = ["id", "name"]
        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(
            query_type="INSERT",
            table_name="users",
            values=[{"__values_only__": [1, "John"]}],
        )

        with patch.object(self.database, "_save_table_to_storage"):
            result = self.database._execute_insert(parsed_query)

            assert result.success == True
            mock_table.insert.assert_called_once_with({"id": 1, "name": "John"})

    def test_execute_select_success(self):
        """Test SELECT execution"""
        mock_table = Mock()
        mock_table.select.return_value = [{"id": 1, "name": "John"}]
        mock_table.column_order = ["id", "name"]
        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(
            query_type="SELECT",
            table_name="users",
            columns=["id", "name"],
            where_clause={"id": 1},
        )

        result = self.database._execute_select(parsed_query)

        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["id"] == 1
        mock_table.select.assert_called_once_with(
            columns=["id", "name"], where_clause={"id": 1}
        )

    def test_execute_select_with_join(self):
        """Test SELECT with JOIN"""
        self.database.tables["users"] = Mock()

        parsed_query = ParsedQuery(
            query_type="SELECT", table_name="users", join_table="orders"
        )

        with patch.object(self.database, "_execute_join_select") as mock_join:
            expected_result = QueryResult(success=True, data=[])
            mock_join.return_value = expected_result

            result = self.database._execute_select(parsed_query)

            assert result == expected_result
            mock_join.assert_called_once_with(parsed_query)

    def test_execute_join_select_success(self):
        """Test JOIN SELECT execution"""
        # Setup mock tables
        mock_users_table = Mock()
        mock_users_table.select.return_value = [{"id": 1, "name": "John"}]

        mock_orders_table = Mock()
        mock_orders_table.select.return_value = [
            {"id": 101, "user_id": 1, "total": 100}
        ]

        self.database.tables["users"] = mock_users_table
        self.database.tables["orders"] = mock_orders_table

        parsed_query = ParsedQuery(
            query_type="SELECT",
            table_name="users",
            join_table="orders",
            join_condition={"left_column": "id", "right_column": "user_id"},
            columns=["*"],
        )

        result = self.database._execute_join_select(parsed_query)

        assert result.success == True
        assert len(result.data) == 1
        # Should have combined columns with table prefixes
        expected_keys = {
            "users.id",
            "users.name",
            "orders.id",
            "orders.user_id",
            "orders.total",
        }
        assert set(result.data[0].keys()) == expected_keys

    def test_execute_join_select_table_not_found(self):
        """Test JOIN SELECT with missing table"""
        parsed_query = ParsedQuery(
            query_type="SELECT", table_name="users", join_table="nonexistent"
        )

        # Add users table but not the join table
        self.database.tables["users"] = Mock()

        result = self.database._execute_join_select(parsed_query)

        assert result.success == False
        assert "does not exist" in result.error

    def test_execute_update_success(self):
        """Test UPDATE execution"""
        mock_table = Mock()
        mock_table.update.return_value = 2
        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(
            query_type="UPDATE",
            table_name="users",
            set_values={"name": "Johnny"},
            where_clause={"id": 1},
        )

        with patch.object(self.database, "_save_table_to_storage"):
            result = self.database._execute_update(parsed_query)

            assert result.success == True
            assert result.rows_affected == 2
            assert "Updated 2 row(s)" in result.message
            mock_table.update.assert_called_once_with(
                set_values={"name": "Johnny"}, where_clause={"id": 1}
            )

    def test_execute_update_table_not_found(self):
        """Test UPDATE with non-existent table"""
        parsed_query = ParsedQuery(
            query_type="UPDATE", table_name="nonexistent"
        )

        result = self.database._execute_update(parsed_query)

        assert result.success == False
        assert "does not exist" in result.error

    def test_execute_delete_success(self):
        """Test DELETE execution"""
        mock_table = Mock()
        mock_table.delete.return_value = 1
        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(
            query_type="DELETE", table_name="users", where_clause={"id": 1}
        )

        with patch.object(self.database, "_save_table_to_storage"):
            result = self.database._execute_delete(parsed_query)

            assert result.success == True
            assert result.rows_affected == 1
            assert "Deleted 1 row(s)" in result.message
            mock_table.delete.assert_called_once_with(where_clause={"id": 1})

    def test_execute_delete_table_not_found(self):
        """Test DELETE with non-existent table"""
        parsed_query = ParsedQuery(
            query_type="DELETE", table_name="nonexistent"
        )

        result = self.database._execute_delete(parsed_query)

        assert result.success == False
        assert "does not exist" in result.error

    def test_execute_drop_table_success(self):
        """Test DROP TABLE execution"""
        mock_table = Mock()
        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(query_type="DROP_TABLE", table_name="users")

        result = self.database._execute_drop_table(parsed_query)

        assert result.success == True
        assert "dropped successfully" in result.message
        assert "users" not in self.database.tables
        self.mock_storage.delete_table.assert_called_once_with("users")

    def test_execute_drop_table_not_found(self):
        """Test DROP TABLE with non-existent table"""
        parsed_query = ParsedQuery(
            query_type="DROP_TABLE", table_name="nonexistent"
        )

        result = self.database._execute_drop_table(parsed_query)

        assert result.success == False
        assert "does not exist" in result.error

    def test_execute_show_tables(self):
        """Test SHOW TABLES execution"""
        mock_table1 = Mock()
        mock_table1.get_row_count.return_value = 5

        mock_table2 = Mock()
        mock_table2.get_row_count.return_value = 10

        self.database.tables["users"] = mock_table1
        self.database.tables["products"] = mock_table2

        result = self.database._execute_show_tables()

        assert result.success == True
        assert len(result.data) == 2
        assert result.columns == ["table_name", "row_count"]

        # Check table info
        table_names = [row["table_name"] for row in result.data]
        assert "users" in table_names
        assert "products" in table_names

    def test_execute_describe_success(self):
        """Test DESCRIBE execution"""
        mock_column = Mock()
        mock_column.name = "id"
        mock_column.data_type = "INT"
        mock_column.nullable = False
        mock_column.primary_key = True
        mock_column.unique = False

        mock_table = Mock()
        mock_table.column_order = ["id"]
        mock_table.columns = {"id": mock_column}

        self.database.tables["users"] = mock_table

        parsed_query = ParsedQuery(query_type="DESCRIBE", table_name="users")

        result = self.database._execute_describe(parsed_query)

        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["column_name"] == "id"
        assert result.data[0]["data_type"] == "INT"
        assert result.data[0]["nullable"] == "NO"
        assert result.data[0]["key"] == "PRI"

    def test_execute_describe_table_not_found(self):
        """Test DESCRIBE with non-existent table"""
        parsed_query = ParsedQuery(
            query_type="DESCRIBE", table_name="nonexistent"
        )

        result = self.database._execute_describe(parsed_query)

        assert result.success == False
        assert "does not exist" in result.error

    def test_evaluate_join_condition_success(self):
        """Test JOIN condition evaluation"""
        left_row = {"id": 1, "name": "John"}
        right_row = {"user_id": 1, "total": 100}
        condition = {"left_column": "id", "right_column": "user_id"}

        result = self.database._evaluate_join_condition(
            left_row, right_row, condition
        )
        assert result == True

    def test_evaluate_join_condition_no_match(self):
        """Test JOIN condition evaluation with no match"""
        left_row = {"id": 1, "name": "John"}
        right_row = {"user_id": 2, "total": 100}
        condition = {"left_column": "id", "right_column": "user_id"}

        result = self.database._evaluate_join_condition(
            left_row, right_row, condition
        )
        assert result == False

    def test_evaluate_join_condition_empty(self):
        """Test JOIN condition evaluation with empty condition (CROSS JOIN)"""
        left_row = {"id": 1}
        right_row = {"user_id": 2}
        condition = {}

        result = self.database._evaluate_join_condition(
            left_row, right_row, condition
        )
        assert result == True

    def test_evaluate_where_clause_on_joined_row_exact_match(self):
        """Test WHERE clause evaluation on joined row with exact match"""
        row = {"users.id": 1, "users.name": "John", "orders.total": 100}
        where_clause = {"users.id": 1}

        result = self.database._evaluate_where_clause_on_joined_row(
            row, where_clause
        )
        assert result == True

    def test_evaluate_where_clause_on_joined_row_with_prefix(self):
        """Test WHERE clause evaluation on joined row with column prefix matching"""
        row = {"users.id": 1, "users.name": "John", "orders.total": 100}
        where_clause = {"id": 1}  # Should match users.id

        result = self.database._evaluate_where_clause_on_joined_row(
            row, where_clause
        )
        assert result == True

    def test_evaluate_where_clause_on_joined_row_no_match(self):
        """Test WHERE clause evaluation on joined row with no match"""
        row = {"users.id": 1, "users.name": "John"}
        where_clause = {"id": 2}

        result = self.database._evaluate_where_clause_on_joined_row(
            row, where_clause
        )
        assert result == False

    def test_get_table_exists(self):
        """Test getting existing table"""
        mock_table = Mock()
        self.database.tables["users"] = mock_table

        result = self.database.get_table("users")
        assert result == mock_table

    def test_get_table_not_exists(self):
        """Test getting non-existent table"""
        result = self.database.get_table("nonexistent")
        assert result is None

    def test_list_tables(self):
        """Test listing all tables"""
        self.database.tables["users"] = Mock()
        self.database.tables["products"] = Mock()

        result = self.database.list_tables()
        assert set(result) == {"users", "products"}

    def test_get_database_stats(self):
        """Test getting database statistics"""
        self.database.tables["users"] = Mock()
        self.database.tables["products"] = Mock()

        self.mock_storage.get_database_stats.return_value = {
            "table_count": 2,
            "total_rows": 100,
        }

        result = self.database.get_database_stats()

        assert result["table_count"] == 2
        assert result["total_rows"] == 100
        assert result["tables_in_memory"] == 2

    def test_backup_success(self):
        """Test successful database backup"""
        self.mock_storage.backup_database.return_value = True

        result = self.database.backup("/tmp/backup")

        assert result == True
        self.mock_storage.backup_database.assert_called_once_with("/tmp/backup")

    def test_backup_failure(self):
        """Test failed database backup"""
        self.mock_storage.backup_database.return_value = False

        result = self.database.backup("/tmp/backup")

        assert result == False

    def test_restore_success(self):
        """Test successful database restore"""
        self.mock_storage.restore_database.return_value = True
        self.mock_storage.list_tables.return_value = ["users"]
        self.mock_storage.load_table.return_value = {"name": "users"}

        # Add a table to be cleared during restore
        self.database.tables["old_table"] = Mock()

        with patch("rdbms.database.Table.from_dict") as mock_from_dict:
            mock_table = Mock()
            mock_from_dict.return_value = mock_table

            result = self.database.restore("/tmp/backup")

            assert result == True
            assert "old_table" not in self.database.tables
            assert "users" in self.database.tables

    def test_restore_failure(self):
        """Test failed database restore"""
        self.mock_storage.restore_database.return_value = False

        result = self.database.restore("/tmp/backup")

        assert result == False

    def test_close_database(self):
        """Test database closure"""
        mock_table = Mock()
        self.database.tables["users"] = mock_table

        with patch.object(self.database, "_save_table_to_storage") as mock_save:
            self.database.close()

            mock_save.assert_called_once_with("users")
            self.mock_storage.close.assert_called_once()
            assert len(self.database.tables) == 0

    def test_query_type_routing(self):
        """Test that all query types are routed correctly"""
        query_types = [
            ("CREATE_TABLE", "_execute_create_table"),
            ("INSERT", "_execute_insert"),
            ("SELECT", "_execute_select"),
            ("UPDATE", "_execute_update"),
            ("DELETE", "_execute_delete"),
            ("DROP_TABLE", "_execute_drop_table"),
            ("SHOW_TABLES", "_execute_show_tables"),
            ("DESCRIBE", "_execute_describe"),
        ]

        for query_type, method_name in query_types:
            parsed_query = ParsedQuery(query_type=query_type, table_name="test")
            self.mock_parser.parse.return_value = parsed_query

            with patch.object(self.database, method_name) as mock_method:
                expected_result = QueryResult(success=True)
                mock_method.return_value = expected_result

                result = self.database.execute_query(f"MOCK {query_type} QUERY")

                assert result == expected_result
                mock_method.assert_called_once()

    def test_integration_create_and_insert(self):
        """Test integration of CREATE TABLE and INSERT operations"""
        # Mock dependencies for create table
        with patch("rdbms.database.Column") as mock_column, patch(
            "rdbms.database.Table"
        ) as mock_table:
            mock_col = Mock()
            mock_column.return_value = mock_col

            mock_table_instance = Mock()
            mock_table_instance.column_order = ["id", "name"]
            mock_table.return_value = mock_table_instance

            self.mock_storage.save_table.return_value = True

            # Create table
            create_query = ParsedQuery(
                query_type="CREATE_TABLE",
                table_name="users",
                columns=[
                    {
                        "name": "id",
                        "type": "INT",
                        "primary_key": True,
                        "unique": False,
                        "nullable": False,
                    }
                ],
            )

            create_result = self.database._execute_create_table(create_query)
            assert create_result.success == True

            # Insert data
            insert_query = ParsedQuery(
                query_type="INSERT",
                table_name="users",
                values=[{"id": 1, "name": "John"}],
            )

            insert_result = self.database._execute_insert(insert_query)
            assert insert_result.success == True


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
