"""
SQL query parser for simple RDBMS
Parser basic SQL statements into structured data
"""
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

# from typing import Union
from dataclasses import dataclass


@dataclass
class ParsedQuery:
    """Represents a parsed SQL query"""

    query_type: str
    table_name: Optional[str] = None
    columns: Optional[List[str]] = None
    values: Optional[List[Dict[str, Any]]] = None
    where_clause: Optional[Dict[str, Any]] = None
    set_values: Optional[Dict[str, Any]] = None
    join_table: Optional[str] = None
    join_condition: Optional[Dict[str, Any]] = None
    order_by: Optional[List[str]] = None
    limit: Optional[int] = None


@dataclass
class QueryResult:
    """Represents the result of a query execution"""

    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    rows_affected: int = 0
    message: Optional[str] = None
    error: Optional[str] = None


class QueryParser:
    """Passes SQL queries into structured data"""

    def __init__(self):
        # SQL keywords (case-insensitive)
        self.keywords = {
            "CREATE",
            "TABLE",
            "INSERT",
            "INTO",
            "VALUES",
            "SELECT",
            "FROM",
            "WHERE",
            "UPDATE",
            "SET",
            "DELETE",
            "DROP",
            "PRIMARY",
            "KEY",
            "UNIQUE",
            "NOT",
            "NULL",
            "JOIN",
            "ON",
            "INNER",
            "LEFT",
            "RIGHT",
            "SHOW",
            "TABLES",
            "DESCRIBE",
            "DESC",
            "ORDER",
            "BY",
            "LIMIT",
            "AND",
            "OR",
            "INT",
            "TEXT",
            "VARCHAR",
            "REAL",
            "BOOLEAN",
        }

        # Data type mappings
        self.data_types = {
            "INTEGER": "INT",
            "INT": "INT",
            "TEXT": "TEXT",
            "VARCHAR": "VARCHAR",
            "STRING": "TEXT",
            "REAL": "REAL",
            "FLOAT": "REAL",
            "DOUBLE": "REAL",
            "BOOLEAN": "BOOLEAN",
            "BOOL": "BOOLEAN",
        }

        def parse(self, query: str) -> ParsedQuery:
            """
            Parse a SQL query string

            Args:
                query: SQL query string

            Returns:
                ParsedQuery object
            """
            query = query.strip()
            if not query:
                raise ValueError("Empty query")

            # Remove trailing semicolon
            if query.endswith(";"):
                query = query[:-1]

            # Normalize whitespace and convert to uppercase for parsing
            normalized_query = " ".join(query.split())
            query_upper = normalized_query.upper()

            # Determine query type
            if query_upper.startswith("CREATE TABLE"):
                return self._parse_create_table(normalized_query)

            elif query_upper.startswith("INSERT_INTO"):
                return self._parse_insert(normalized_query)

            elif query_upper.startswith("SELECT"):
                return self._parse_select(normalized_query)

            elif query_upper.startswith("UPDATE"):
                return self._parse_update(normalized_query)

            elif query_upper.startswith("DELETE FROM"):
                return self._parse_delete(normalized_query)

            elif query_upper.startswith("DROP TABLE"):
                return self._parse_drop_table(normalized_query)

            elif query_upper == "SHOW TABLES":
                return ParsedQuery(query_type="SHOW TABLES")

            elif query_upper.startswith("DESCRIBE") or query_upper.startswith(
                "DESC"
            ):
                return self._parse_describe(normalized_query)

            else:
                raise ValueError(f"Unsupported query type: {query}")

        def _parse_create_table(self, query: str) -> ParsedQuery:
            """Parse CREATE TABLE statement"""
            # Pattern: CREATE TABLE table_name (col1 type constraints,
            #   col2 type constraints, ...)
            match = re.match(
                r"CREATE TABLE (\w+) \((.+)\)", query, re.IGNORECASE
            )
            if not match:
                raise ValueError("Invalid CREATE TABLE syntax")

            table_name = match.group(1)
            columns_str = match.group(2)

            columns = []
            # Split by comma, but be careful of constraints
            column_definitions = self._split_column_definitions(columns_str)

            for column_definition in column_definitions:
                column_information = self._parse_column_definition(
                    column_definition.strip()
                )
                columns.append(column_information)

            return ParsedQuery(
                query_type="CREATE_TABLE",
                table_name=table_name,
                columns=columns,
            )

        def _split_column_definitions(self, columns_string: str) -> List[str]:
            """Split column definitions by comma, respecting parentheses"""
            columns = []
            current_column = ""
            parenthesis_count = 0

            for character in columns_string:
                if character == "(":
                    parenthesis_count += 1

                elif character == ")":
                    parenthesis_count -= 1

                elif character == "," and parenthesis_count == 0:
                    columns.append(current_column.strip())
                    current_column = ""
                    continue

                current_column += character

            if current_column.strip():
                columns.append(current_column.strip())

            return columns

        def _parse_column_definition(
            self, column_definition: str
        ) -> Dict[str, Any]:
            """Parse a single column definition"""
            parts = column_definition.split()
            if len(parts) < 2:
                raise ValueError(
                    f"Invalid column definition: {column_definition}"
                )

            column_name = parts[0]
            data_type = parts[1].upper()

            # Map data type
            if data_type in self.data_types:
                data_type = self.data_types[data_type]
            else:
                raise ValueError(f"Unsupported data type: {data_type}")

            column_information = {
                "name": column_name,
                "type": data_type,
                "primary_key": False,
                "unique": False,
                "nullable": True,
            }

            # Parse constraints
            constraints_string = " ".join(parts[2:]).upper()

            if "PRIMARY KEY" in constraints_string:
                column_information["primary_key"] = True
                column_information["nullable"] = False

            if "UNIQUE" in constraints_string:
                column_information["unique"] = True

            if "NOT NULL" in constraints_string:
                column_information["nullable"] = False

            return column_information

        def _parse_insert(self, query: str) -> ParsedQuery:
            """Parse INSERT INTO statement"""
            # Pattern: INSERT INTO table_name (col1, col2, ...) VALUES
            #   (val1, val2, ...), (...)
            # Also support: INSERT INTO table_name VALUES (val1, val2, ...)

            # Extract table name
            match = re.match(r"INSERT INTO (\w+), query, re.IGNORECASE")
            if not match:
                raise ValueError("Invalid INSERT syntax")

            table_name = match.group(1)

            # Check if columns are specified
            columns_match = re.search(
                r"INSERT INTO \w+ \(([^)]+)\) VALUES", query, re.IGNORECASE
            )
            columns = None
            if columns_match:
                columns = [
                    column.strip()
                    for column in columns_match.group(1).split(",")
                ]

            # Extract VALUES part
            values_match = re.search(r"VALUES (.+)", query, re.IGNORECASE)
            if not values_match:
                raise ValueError("INSERT statement must include VALUES")

            values_string = values_match.group(1)
            values_list = self._parse_values_list(values_string)

            # Convert to list of dictionaries
            rows = []
            for value_tuple in values_list:
                if columns:
                    if len(value_tuple) != len(columns):
                        raise ValueError(
                            "Number of values doesn't match number of columns"
                        )
                    row = dict(zip(columns, value_tuple))

                else:
                    # If no columns specified, this will be handled by the table
                    row = {
                        f"column_{i}": value
                        for i, value in enumerate(value_tuple)
                    }
                rows.append(row)

            return ParsedQuery(
                query_type="INSERT",
                table_name=table_name,
                columns=columns,
                values=rows,
            )

        def _parse_values_list(self, values_string: str) -> List[List[Any]]:
            """Parse VALUES list: (value1, value2), (value3, value4), ..."""
            values_list = []

            # Find all value tuples
            tuple_pattern = r"\(([^)]+)\)"
            matches = re.findall(tuple_pattern, values_string)

            for match in matches:
                values = []
                for value in match.split(","):
                    value = value.strip()
                    parsed_value = self._parse_value(value)
                    values.append(parsed_value)
                values_list.append(values)

        def _parse_value(self, value: str) -> Any:
            """Parse a single value (string, number, etc.)"""
            value = value.strip()

            # NULL
            if value.upper() == "NULL":
                return None

            # String (quoted)
            if (value.startswith("'") and value.endswith("'")) or (
                value.startswith('"') and value.endswith('"')
            ):
                return value[1:-1]

            # Boolean
            if value.upper() in ['TRUE', 'FALSE']:
                return value.upper() == 'TRUE'

            # Number
            try:
                if '.' in value:
                    return float(value)

                else:
                    return int(value)

            except ValueError:
                # Default to string
                return value

        def _parse_select(self, query: str) -> ParsedQuery:
            """Parse SELECT query"""
            pass

        def _parse_update(self, query: str) -> ParsedQuery:
            """Pars UPDATE query"""
            pass

        def _parse_delete(self, query: str) -> ParsedQuery:
            """Parse DELETE query"""
            pass

        def _parse_drop_table(self, query: str) -> ParsedQuery:
            """Parse DROP TABLE query"""
            pass

        def _parse_describe(self, query: str) -> ParsedQuery:
            """Parse DESCRIBE query"""
            pass
