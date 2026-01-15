"""
Comprehensive automated tests for the query_parser module
Tests QueryParser, ParsedQuery, and QueryResult classes with all functionality
"""

import os
import sys
import pytest

# Add the parent directory to the path to import the rdbms module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.query_parser import QueryResult
from rdbms.query_parser import ParsedQuery
from rdbms.query_parser import QueryParser


class TestParsedQuery:
    """Test cases for the ParsedQuery dataclass"""

    def test_parsed_query_creation(self):
        """Test basic ParsedQuery creation"""
        query = ParsedQuery(query_type="SELECT", table_name="users")
        assert query.query_type == "SELECT"
        assert query.table_name == "users"
        assert query.columns is None
        assert query.values is None

    def test_parsed_query_with_all_fields(self):
        """Test ParsedQuery with all fields"""
        query = ParsedQuery(
            query_type="SELECT",
            table_name="users",
            columns=["id", "name"],
            values=[{"id": 1, "name": "John"}],
            where_clause={"id": 1},
            set_values={"name": "Jane"},
            join_table="orders",
            join_condition={"user_id": "id"},
            order_by=["name"],
            limit=10,
        )

        assert query.query_type == "SELECT"
        assert query.table_name == "users"
        assert query.columns == ["id", "name"]
        assert query.values == [{"id": 1, "name": "John"}]
        assert query.where_clause == {"id": 1}
        assert query.set_values == {"name": "Jane"}
        assert query.join_table == "orders"
        assert query.join_condition == {"user_id": "id"}
        assert query.order_by == ["name"]
        assert query.limit == 10


class TestQueryResult:
    """Test cases for the QueryResult dataclass"""

    def test_query_result_success(self):
        """Test successful QueryResult"""
        result = QueryResult(
            success=True,
            data=[{"id": 1, "name": "John"}],
            columns=["id", "name"],
            rows_affected=1,
            message="Query executed successfully",
        )

        assert result.success == True
        assert result.data == [{"id": 1, "name": "John"}]
        assert result.columns == ["id", "name"]
        assert result.rows_affected == 1
        assert result.message == "Query executed successfully"
        assert result.error is None

    def test_query_result_error(self):
        """Test error QueryResult"""
        result = QueryResult(success=False, error="Table not found")

        assert result.success == False
        assert result.error == "Table not found"
        assert result.data is None
        assert result.rows_affected == 0


class TestQueryParser:
    """Test cases for the QueryParser class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.parser = QueryParser()

    def test_parser_initialization(self):
        """Test parser initialization"""
        assert self.parser is not None
        assert hasattr(self.parser, "keywords")
        assert hasattr(self.parser, "data_types")
        assert "SELECT" in self.parser.keywords
        assert "CREATE" in self.parser.keywords

    def test_empty_query(self):
        """Test parsing empty query"""
        with pytest.raises(ValueError, match="Empty query"):
            self.parser.parse("")

    def test_whitespace_only_query(self):
        """Test parsing whitespace-only query"""
        with pytest.raises(ValueError, match="Empty query"):
            self.parser.parse("   ")

    def test_unsupported_query_type(self):
        """Test parsing unsupported query type"""
        with pytest.raises(ValueError, match="Unsupported query type"):
            self.parser.parse("GRANT ALL PRIVILEGES")

    # CREATE TABLE Tests
    def test_parse_create_table_basic(self):
        """Test parsing basic CREATE TABLE statement"""
        query = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)"
        result = self.parser.parse(query)

        assert result.query_type == "CREATE_TABLE"
        assert result.table_name == "users"
        assert len(result.columns) == 2

        # Check first column
        id_col = result.columns[0]
        assert id_col["name"] == "id"
        assert id_col["type"] == "INT"
        assert id_col["primary_key"] == True
        assert id_col["nullable"] == False

        # Check second column
        name_col = result.columns[1]
        assert name_col["name"] == "name"
        assert name_col["type"] == "TEXT"
        assert name_col["primary_key"] == False
        assert name_col["nullable"] == False

    def test_parse_create_table_with_constraints(self):
        """Test CREATE TABLE with various constraints"""
        query = "CREATE TABLE products (id INT PRIMARY KEY, code VARCHAR UNIQUE, price REAL, active BOOLEAN)"
        result = self.parser.parse(query)

        assert result.query_type == "CREATE_TABLE"
        assert result.table_name == "products"
        assert len(result.columns) == 4

        # Check UNIQUE constraint
        code_col = result.columns[1]
        assert code_col["name"] == "code"
        assert code_col["unique"] == True

    def test_parse_create_table_invalid_syntax(self):
        """Test CREATE TABLE with invalid syntax"""
        with pytest.raises(ValueError, match="Invalid CREATE TABLE syntax"):
            self.parser.parse("CREATE TABLE")

    def test_parse_create_table_unsupported_data_type(self):
        """Test CREATE TABLE with unsupported data type"""
        with pytest.raises(ValueError, match="Unsupported data type"):
            self.parser.parse("CREATE TABLE test (id BIGINT)")

    # INSERT Tests
    def test_parse_insert_with_columns(self):
        """Test INSERT with specified columns"""
        query = "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')"
        result = self.parser.parse(query)

        assert result.query_type == "INSERT"
        assert result.table_name == "users"
        assert result.columns == ["id", "name"]
        assert len(result.values) == 2
        assert result.values[0] == {"id": 1, "name": "John"}
        assert result.values[1] == {"id": 2, "name": "Jane"}

    def test_parse_insert_without_columns(self):
        """Test INSERT without specified columns"""
        query = "INSERT INTO users VALUES (1, 'John')"
        result = self.parser.parse(query)

        assert result.query_type == "INSERT"
        assert result.table_name == "users"
        assert result.columns is None
        assert len(result.values) == 1

    def test_parse_insert_invalid_syntax(self):
        """Test INSERT with invalid syntax"""
        with pytest.raises(ValueError, match="Invalid INSERT syntax"):
            self.parser.parse("INSERT users VALUES (1, 'John')")

    def test_parse_insert_missing_values(self):
        """Test INSERT without VALUES"""
        with pytest.raises(
            ValueError, match="INSERT statement must include VALUES"
        ):
            self.parser.parse("INSERT INTO users (id, name)")

    def test_parse_insert_column_value_mismatch(self):
        """Test INSERT with mismatched columns and values"""
        with pytest.raises(ValueError, match="Number of values doesn't match"):
            self.parser.parse("INSERT INTO users (id, name) VALUES (1)")

    # SELECT Tests
    def test_parse_select_basic(self):
        """Test basic SELECT statement"""
        query = "SELECT id, name FROM users"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"
        assert result.columns == ["id", "name"]
        assert result.where_clause is None

    def test_parse_select_star(self):
        """Test SELECT * statement"""
        query = "SELECT * FROM users"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"
        assert result.columns == ["*"]

    def test_parse_select_with_where(self):
        """Test SELECT with WHERE clause"""
        query = "SELECT name FROM users WHERE id = 1"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"
        assert result.where_clause == {"id": 1}

    def test_parse_select_with_complex_where(self):
        """Test SELECT with complex WHERE clause"""
        query = "SELECT * FROM users WHERE id = 1 AND name = 'John'"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.where_clause == {"id": 1, "name": "John"}

    def test_parse_select_with_order_by(self):
        """Test SELECT with ORDER BY"""
        query = "SELECT * FROM users ORDER BY name, id"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.order_by == ["name", "id"]

    def test_parse_select_with_limit(self):
        """Test SELECT with LIMIT"""
        query = "SELECT * FROM users LIMIT 10"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.limit == 10

    def test_parse_select_with_join(self):
        """Test SELECT with JOIN"""
        query = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"
        assert result.join_table == "orders"
        assert result.join_condition is not None

    def test_parse_select_invalid_syntax(self):
        """Test SELECT with invalid syntax"""
        with pytest.raises(
            ValueError, match="SELECT statement must include FROM clause"
        ):
            self.parser.parse("SELECT")

    def test_parse_select_missing_from(self):
        """Test SELECT without FROM clause"""
        with pytest.raises(
            ValueError, match="SELECT statement must include FROM clause"
        ):
            self.parser.parse("SELECT id, name")

    # UPDATE Tests
    def test_parse_update_basic(self):
        """Test basic UPDATE statement"""
        query = "UPDATE users SET name = 'Jane' WHERE id = 1"
        result = self.parser.parse(query)

        assert result.query_type == "UPDATE"
        assert result.table_name == "users"
        assert result.set_values == {"name": "Jane"}
        assert result.where_clause == {"id": 1}

    def test_parse_update_multiple_columns(self):
        """Test UPDATE with multiple columns"""
        query = "UPDATE users SET name = 'Jane', age = 25 WHERE id = 1"
        result = self.parser.parse(query)

        assert result.query_type == "UPDATE"
        assert result.set_values == {"name": "Jane", "age": 25}

    def test_parse_update_without_where(self):
        """Test UPDATE without WHERE clause"""
        query = "UPDATE users SET active = TRUE"
        result = self.parser.parse(query)

        assert result.query_type == "UPDATE"
        assert result.where_clause is None

    def test_parse_update_invalid_syntax(self):
        """Test UPDATE with invalid syntax"""
        with pytest.raises(ValueError, match="Invalid UPDATE syntax"):
            self.parser.parse("UPDATE users")

    # DELETE Tests
    def test_parse_delete_with_where(self):
        """Test DELETE with WHERE clause"""
        query = "DELETE FROM users WHERE id = 1"
        result = self.parser.parse(query)

        assert result.query_type == "DELETE"
        assert result.table_name == "users"
        assert result.where_clause == {"id": 1}

    def test_parse_delete_without_where(self):
        """Test DELETE without WHERE clause"""
        query = "DELETE FROM users"
        result = self.parser.parse(query)

        assert result.query_type == "DELETE"
        assert result.table_name == "users"
        assert result.where_clause is None

    def test_parse_delete_invalid_syntax(self):
        """Test DELETE with invalid syntax"""
        with pytest.raises(ValueError, match="Invalid DELETE syntax"):
            self.parser.parse("DELETE users")

    # DROP TABLE Tests
    def test_parse_drop_table(self):
        """Test DROP TABLE statement"""
        query = "DROP TABLE users"
        result = self.parser.parse(query)

        assert result.query_type == "DROP_TABLE"
        assert result.table_name == "users"

    def test_parse_drop_table_invalid_syntax(self):
        """Test DROP TABLE with invalid syntax"""
        with pytest.raises(ValueError, match="Invalid DROP TABLE syntax"):
            self.parser.parse("DROP TABLE")

    # SHOW TABLES Tests
    def test_parse_show_tables(self):
        """Test SHOW TABLES statement"""
        query = "SHOW TABLES"
        result = self.parser.parse(query)

        assert result.query_type == "SHOW TABLES"

    # DESCRIBE Tests
    def test_parse_describe(self):
        """Test DESCRIBE statement"""
        query = "DESCRIBE users"
        result = self.parser.parse(query)

        assert result.query_type == "DESCRIBE"
        assert result.table_name == "users"

    def test_parse_desc(self):
        """Test DESC statement"""
        query = "DESC users"
        result = self.parser.parse(query)

        assert result.query_type == "DESCRIBE"
        assert result.table_name == "users"

    def test_parse_describe_invalid_syntax(self):
        """Test DESCRIBE with invalid syntax"""
        with pytest.raises(ValueError, match="Invalid DESCRIBE syntax"):
            self.parser.parse("DESCRIBE")

    # Value Parsing Tests
    def test_parse_value_string(self):
        """Test parsing string values"""
        assert self.parser._parse_value("'hello'") == "hello"
        assert self.parser._parse_value('"world"') == "world"

    def test_parse_value_number(self):
        """Test parsing numeric values"""
        assert self.parser._parse_value("123") == 123
        assert self.parser._parse_value("45.67") == 45.67

    def test_parse_value_boolean(self):
        """Test parsing boolean values"""
        assert self.parser._parse_value("TRUE") == True
        assert self.parser._parse_value("FALSE") == False
        assert self.parser._parse_value("true") == True

    def test_parse_value_null(self):
        """Test parsing NULL values"""
        assert self.parser._parse_value("NULL") is None
        assert self.parser._parse_value("null") is None

    def test_parse_value_unquoted_string(self):
        """Test parsing unquoted strings"""
        assert self.parser._parse_value("username") == "username"

    # Utility Method Tests
    def test_split_column_definitions(self):
        """Test splitting column definitions"""
        columns_str = "id INT PRIMARY KEY, name VARCHAR(50), age INT"
        result = self.parser._split_column_definitions(columns_str)

        assert len(result) == 3
        assert result[0] == "id INT PRIMARY KEY"
        assert result[1] == "name VARCHAR(50)"
        assert result[2] == "age INT"

    def test_parse_column_definition(self):
        """Test parsing individual column definitions"""
        col_def = "id INT PRIMARY KEY"
        result = self.parser._parse_column_definition(col_def)

        assert result["name"] == "id"
        assert result["type"] == "INT"
        assert result["primary_key"] == True
        assert result["nullable"] == False

    def test_parse_column_definition_with_unique(self):
        """Test parsing column definition with UNIQUE constraint"""
        col_def = "email VARCHAR UNIQUE NOT NULL"
        result = self.parser._parse_column_definition(col_def)

        assert result["name"] == "email"
        assert result["type"] == "VARCHAR"
        assert result["unique"] == True
        assert result["nullable"] == False

    def test_parse_column_definition_invalid(self):
        """Test parsing invalid column definition"""
        with pytest.raises(ValueError, match="Invalid column definition"):
            self.parser._parse_column_definition("just_name")

    # Data Type Mapping Tests
    def test_data_type_mappings(self):
        """Test data type mappings"""
        assert self.parser.data_types["INTEGER"] == "INT"
        assert self.parser.data_types["STRING"] == "TEXT"
        assert self.parser.data_types["FLOAT"] == "REAL"
        assert self.parser.data_types["BOOL"] == "BOOLEAN"

    # Query Formatting Tests
    def test_format_query_result_success(self):
        """Test formatting successful query result"""
        result = QueryResult(
            success=True,
            data=[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
            columns=["id", "name"],
        )

        formatted = self.parser.format_query_result(result)
        assert "John" in formatted
        assert "Jane" in formatted
        assert "2 row(s) returned" in formatted

    def test_format_query_result_error(self):
        """Test formatting error result"""
        result = QueryResult(success=False, error="Table not found")

        formatted = self.parser.format_query_result(result)
        assert formatted == "Error: Table not found"

    def test_format_query_result_message(self):
        """Test formatting result with message"""
        result = QueryResult(success=True, message="Table created successfully")

        formatted = self.parser.format_query_result(result)
        assert formatted == "Table created successfully"

    def test_format_query_result_no_data(self):
        """Test formatting result with no data"""
        result = QueryResult(success=True, data=None)

        formatted = self.parser.format_query_result(result)
        assert formatted == "No results"

    def test_format_query_result_empty_data(self):
        """Test formatting result with empty data"""
        result = QueryResult(success=True, data=[], columns=["id", "name"])

        formatted = self.parser.format_query_result(result)
        assert "0 row(s) returned" in formatted

    # Edge Cases and Error Handling
    def test_semicolon_handling(self):
        """Test handling of trailing semicolons"""
        query = "SELECT * FROM users;"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"

    def test_case_insensitive_parsing(self):
        """Test case-insensitive parsing"""
        query = "select * from users where id = 1"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"

    def test_whitespace_normalization(self):
        """Test whitespace normalization"""
        query = "SELECT   *    FROM    users    WHERE   id   =   1"
        result = self.parser.parse(query)

        assert result.query_type == "SELECT"
        assert result.table_name == "users"
        assert result.where_clause == {"id": 1}


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
