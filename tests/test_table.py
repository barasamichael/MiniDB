"""
Automated tests for the table module
Tests Column and Table classes with all functionality
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.table import Table  # noqa
from rdbms.table import Column  # noqa


class TestColumn:
    """Test cases for the Column class"""

    def test_column_creation_basic(self):
        """Test basic column creation"""
        col = Column("test_col", "INT")
        assert col.name == "test_col"
        assert col.data_type == "INT"
        assert col.primary_key == False
        assert col.unique == False
        assert col.nullable == True

    def test_column_creation_with_constraints(self):
        """Test column creation with constraints"""
        col = Column("id", "INT", primary_key=True, unique=True, nullable=False)
        assert col.name == "id"
        assert col.data_type == "INT"
        assert col.primary_key == True
        assert col.unique == True
        assert col.nullable == False

    def test_column_primary_key_sets_nullable_false(self):
        """Test that primary key automatically sets nullable to False"""
        col = Column("id", "INT", primary_key=True, nullable=True)
        assert col.nullable == False

    def test_column_data_type_case_insensitive(self):
        """Test that data type is converted to uppercase"""
        col = Column("test", "int")
        assert col.data_type == "INT"

    def test_unsupported_data_type(self):
        """Test error handling for unsupported data types"""
        with pytest.raises(ValueError, match="Unsupported data type"):
            Column("test", "UNSUPPORTED")

    def test_validate_value_int(self):
        """Test integer value validation"""
        col = Column("test", "INT")
        assert col.validate_value(42) == 42
        assert col.validate_value("42") == 42
        assert col.validate_value(42.7) == 42

        with pytest.raises(ValueError):
            col.validate_value("not_a_number")

    def test_validate_value_text(self):
        """Test text value validation"""
        col = Column("test", "TEXT")
        assert col.validate_value("hello") == "hello"
        assert col.validate_value(123) == "123"
        assert col.validate_value(45.67) == "45.67"

    def test_validate_value_varchar(self):
        """Test varchar value validation"""
        col = Column("test", "VARCHAR")
        assert col.validate_value("hello") == "hello"
        assert col.validate_value(123) == "123"

    def test_validate_value_real(self):
        """Test real value validation"""
        col = Column("test", "REAL")
        assert col.validate_value(3.14) == 3.14
        assert col.validate_value("3.14") == 3.14
        assert col.validate_value(42) == 42.0

        with pytest.raises(ValueError):
            col.validate_value("not_a_number")

    def test_validate_value_boolean(self):
        """Test boolean value validation"""
        col = Column("test", "BOOLEAN")
        assert col.validate_value(True) == True
        assert col.validate_value(False) == False
        assert col.validate_value("false") == False
        assert col.validate_value("0") == False
        assert col.validate_value("no") == False

        with pytest.raises(ValueError):
            col.validate_value("invalid_bool")

    def test_validate_value_null_nullable(self):
        """Test null value validation for nullable column"""
        col = Column("test", "INT", nullable=True)
        assert col.validate_value(None) is None

    def test_validate_value_null_not_nullable(self):
        """Test null value validation for non-nullable column"""
        col = Column("test", "INT", nullable=False)
        with pytest.raises(ValueError, match="cannot be NULL"):
            col.validate_value(None)

    def test_to_dict(self):
        """Test column serialization to dictionary"""
        col = Column(
            "test", "INT", primary_key=True, unique=True, nullable=False
        )
        expected = {
            "name": "test",
            "data_type": "INT",
            "primary_key": True,
            "unique": True,
            "nullable": False,
        }
        assert col.to_dict() == expected

    def test_from_dict(self):
        """Test column creation from dictionary"""
        data = {
            "name": "test",
            "data_type": "VARCHAR",
            "primary_key": False,
            "unique": True,
            "nullable": True,
        }
        col = Column.from_dict(data)
        assert col.name == "test"
        assert col.data_type == "VARCHAR"
        assert col.primary_key == False
        assert col.unique == True
        assert col.nullable == True


class TestTable:
    """Test cases for the Table class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.columns = [
            Column("id", "INT", primary_key=True),
            Column("name", "TEXT", nullable=False),
            Column("age", "INT"),
            Column("email", "VARCHAR", unique=True),
        ]
        self.table = Table("users", self.columns)

    def test_table_creation(self):
        """Test basic table creation"""
        assert self.table.name == "users"
        assert len(self.table.columns) == 4
        assert self.table.primary_key == "id"
        assert "id" in self.table.unique_columns
        assert "email" in self.table.unique_columns

    def test_table_multiple_primary_keys_error(self):
        """Test error when multiple primary keys are defined"""
        columns = [
            Column("id1", "INT", primary_key=True),
            Column("id2", "INT", primary_key=True),
        ]
        with pytest.raises(ValueError, match="only one primary key"):
            Table("test", columns)

    def test_insert_valid_row(self):
        """Test inserting a valid row"""
        row = {"id": 1, "name": "John", "age": 25, "email": "john@test.com"}
        result = self.table.insert(row)
        assert result == True
        assert len(self.table.rows) == 1
        assert self.table.rows[0]["id"] == 1

    def test_insert_partial_row(self):
        """Test inserting a row with missing nullable columns"""
        row = {"id": 1, "name": "John"}
        result = self.table.insert(row)
        assert result == True
        assert self.table.rows[0]["age"] is None
        assert self.table.rows[0]["email"] is None

    def test_insert_missing_required_column(self):
        """Test inserting a row missing a required column"""
        row = {"id": 1, "age": 25}  # Missing required "name"
        with pytest.raises(ValueError, match="cannot be NULL"):
            self.table.insert(row)

    def test_insert_unknown_column(self):
        """Test inserting a row with unknown column"""
        row = {"id": 1, "name": "John", "unknown_col": "value"}
        with pytest.raises(ValueError, match="Unknown column"):
            self.table.insert(row)

    def test_insert_duplicate_primary_key(self):
        """Test inserting duplicate primary key"""
        self.table.insert({"id": 1, "name": "John"})
        with pytest.raises(ValueError, match="Duplicate value.*primary key"):
            self.table.insert({"id": 1, "name": "Jane"})

    def test_insert_duplicate_unique_column(self):
        """Test inserting duplicate unique column value"""
        self.table.insert({"id": 1, "name": "John", "email": "john@test.com"})
        with pytest.raises(ValueError, match="Duplicate value.*unique"):
            self.table.insert(
                {"id": 2, "name": "Jane", "email": "john@test.com"}
            )

    def test_select_all_rows(self):
        """Test selecting all rows"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "Jane", "age": 30})

        results = self.table.select()
        assert len(results) == 2
        assert results[0]["name"] == "John"
        assert results[1]["name"] == "Jane"

    def test_select_specific_columns(self):
        """Test selecting specific columns"""
        self.table.insert({"id": 1, "name": "John", "age": 25})

        results = self.table.select(columns=["name", "age"])
        assert len(results) == 1
        assert set(results[0].keys()) == {"name", "age"}
        assert "id" not in results[0]

    def test_select_star_columns(self):
        """Test selecting with * wildcard"""
        self.table.insert({"id": 1, "name": "John", "age": 25})

        results = self.table.select(columns=["*"])
        assert len(results) == 1
        assert set(results[0].keys()) == {"id", "name", "age", "email"}

    def test_select_with_where_clause(self):
        """Test selecting with where clause"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "Jane", "age": 30})

        results = self.table.select(where_clause={"name": "John"})
        assert len(results) == 1
        assert results[0]["name"] == "John"

    def test_select_with_multiple_conditions(self):
        """Test selecting with multiple where conditions"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "John", "age": 30})

        results = self.table.select(where_clause={"name": "John", "age": 25})
        assert len(results) == 1
        assert results[0]["id"] == 1

    def test_select_where_unknown_column(self):
        """Test selecting with unknown column in where clause"""
        with pytest.raises(ValueError, match="Unknown column in WHERE clause"):
            self.table.select(where_clause={"unknown_col": "value"})

    def test_update_rows(self):
        """Test updating rows"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "Jane", "age": 30})

        count = self.table.update({"age": 26}, {"name": "John"})
        assert count == 1

        results = self.table.select(where_clause={"name": "John"})
        assert results[0]["age"] == 26

    def test_update_all_rows(self):
        """Test updating all rows (no where clause)"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "Jane", "age": 30})

        count = self.table.update({"age": 35})
        assert count == 2

        results = self.table.select()
        assert all(row["age"] == 35 for row in results)

    def test_update_with_validation_error(self):
        """Test update with validation error"""
        self.table.insert({"id": 1, "name": "John", "age": 25})

        with pytest.raises(ValueError):
            self.table.update({"age": "not_a_number"}, {"id": 1})

    def test_update_with_unique_constraint_violation(self):
        """Test update causing unique constraint violation"""
        self.table.insert({"id": 1, "name": "John", "email": "john@test.com"})
        self.table.insert({"id": 2, "name": "Jane", "email": "jane@test.com"})

        with pytest.raises(ValueError, match="Duplicate value.*unique"):
            self.table.update({"email": "john@test.com"}, {"id": 2})

    def test_delete_rows(self):
        """Test deleting rows"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "Jane", "age": 30})

        count = self.table.delete({"name": "John"})
        assert count == 1
        assert len(self.table.rows) == 1
        assert self.table.rows[0]["name"] == "Jane"

    def test_delete_all_rows(self):
        """Test deleting all rows"""
        self.table.insert({"id": 1, "name": "John", "age": 25})
        self.table.insert({"id": 2, "name": "Jane", "age": 30})

        count = self.table.delete()
        assert count == 2
        assert len(self.table.rows) == 0

    def test_delete_no_matches(self):
        """Test deleting with no matching rows"""
        self.table.insert({"id": 1, "name": "John", "age": 25})

        count = self.table.delete({"name": "NonExistent"})
        assert count == 0
        assert len(self.table.rows) == 1

    def test_indexes_updated_on_insert(self):
        """Test that indexes are properly updated on insert"""
        self.table.insert({"id": 1, "name": "John", "email": "john@test.com"})

        assert 1 in self.table.indexes["id"]
        assert "john@test.com" in self.table.indexes["email"]

    def test_indexes_updated_on_delete(self):
        """Test that indexes are properly updated on delete"""
        self.table.insert({"id": 1, "name": "John", "email": "john@test.com"})
        self.table.insert({"id": 2, "name": "Jane", "email": "jane@test.com"})

        self.table.delete({"id": 1})

        assert 1 not in self.table.indexes["id"]
        assert "john@test.com" not in self.table.indexes["email"]
        assert 2 in self.table.indexes["id"]

    def test_get_row_count(self):
        """Test getting row count"""
        assert self.table.get_row_count() == 0

        self.table.insert({"id": 1, "name": "John"})
        assert self.table.get_row_count() == 1

        self.table.insert({"id": 2, "name": "Jane"})
        assert self.table.get_row_count() == 2

    def test_get_schema(self):
        """Test getting table schema"""
        schema = self.table.get_schema()

        assert schema["name"] == "users"
        assert len(schema["columns"]) == 4
        assert schema["primary_key"] == "id"
        assert schema["row_count"] == 0

        # Check column details
        id_col = next(col for col in schema["columns"] if col["name"] == "id")
        assert id_col["data_type"] == "INT"
        assert id_col["primary_key"] == True

    def test_to_dict(self):
        """Test table serialization"""
        self.table.insert({"id": 1, "name": "John", "age": 25})

        data = self.table.to_dict()

        assert data["name"] == "users"
        assert len(data["columns"]) == 4
        assert data["column_order"] == ["id", "name", "age", "email"]
        assert len(data["rows"]) == 1
        assert data["primary_key"] == "id"
        assert set(data["unique_columns"]) == {"id", "email"}

    def test_edge_case_empty_table_operations(self):
        """Test operations on empty table"""
        # Select from empty table
        results = self.table.select()
        assert results == []

        # Update empty table
        count = self.table.update({"age": 25})
        assert count == 0

        # Delete from empty table
        count = self.table.delete()
        assert count == 0

    def test_edge_case_null_values_in_indexes(self):
        """Test handling of null values in indexed columns"""
        # Insert row with null email (unique column)
        self.table.insert({"id": 1, "name": "John", "email": None})
        self.table.insert({"id": 2, "name": "Jane", "email": None})

        # Both should be allowed since null values don't violate unique constraints
        assert len(self.table.rows) == 2

    def test_data_type_conversion_edge_cases(self):
        """Test edge cases in data type conversion"""
        # Insert with type conversion
        self.table.insert(
            {
                "id": "123",  # String to int
                "name": 456,  # Int to string
                "age": 25.7,  # Float to int (should truncate)
            }
        )

        row = self.table.rows[0]
        assert row["id"] == 123
        assert row["name"] == "456"
        assert row["age"] == 25

    def test_boolean_edge_cases(self):
        """Test boolean validation edge cases"""
        bool_col = Column("active", "BOOLEAN")

        # Test various falsy values
        assert bool_col.validate_value("FALSE") == False
        assert bool_col.validate_value("False") == False
        assert bool_col.validate_value("0") == False
        assert bool_col.validate_value("NO") == False

        # Test truthy values
        assert bool_col.validate_value("true") == True
        assert bool_col.validate_value("TRUE") == True
        assert bool_col.validate_value("1") == True
        assert bool_col.validate_value("yes") == True

        # Test invalid values
        with pytest.raises(ValueError):
            bool_col.validate_value("maybe")

    def test_complex_where_clause_matching(self):
        """Test complex where clause scenarios"""
        self.table.insert({"id": 1, "name": "John", "age": None})
        self.table.insert({"id": 2, "name": "Jane", "age": 25})

        # Test matching with None values
        results = self.table.select(where_clause={"age": None})
        assert len(results) == 1
        assert results[0]["name"] == "John"

    def test_rebuild_indexes_after_multiple_deletes(self):
        """Test index rebuilding after multiple deletions"""
        # Insert multiple rows
        for i in range(5):
            self.table.insert({"id": i, "name": f"User{i}"})

        # Delete some rows
        self.table.delete({"id": 1})
        self.table.delete({"id": 3})

        # Check that remaining indexes are correct
        remaining_ids = set(self.table.indexes["id"].keys())
        expected_ids = {0, 2, 4}
        assert remaining_ids == expected_ids


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
