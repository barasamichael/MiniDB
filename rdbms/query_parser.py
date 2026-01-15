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
    """Parses SQL queries into structured data"""

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

        # Handle invalid SQL patterns first
        if query_upper.startswith("INSERT ") and not query_upper.startswith(
            "INSERT INTO"
        ):
            raise ValueError("Invalid INSERT syntax")

        if query_upper.startswith("DELETE ") and not query_upper.startswith(
            "DELETE FROM"
        ):
            raise ValueError("Invalid DELETE syntax")

        # Determine query type
        if query_upper.startswith("CREATE TABLE"):
            return self._parse_create_table(normalized_query)

        elif query_upper.startswith("INSERT INTO"):
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
        # Pattern: CREATE TABLE table_name (col1 type constraints, col2 type constraints, ...)
        match = re.match(r"CREATE TABLE (\w+) \((.+)\)", query, re.IGNORECASE)
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
            raise ValueError(f"Invalid column definition: {column_definition}")

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
        # Pattern: INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...), (...)
        # Also support: INSERT INTO table_name VALUES (val1, val2, ...)

        # Extract table name
        match = re.match(r"INSERT INTO (\w+)", query, re.IGNORECASE)
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
                column.strip() for column in columns_match.group(1).split(",")
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
                row = {"__values_only__": list(value_tuple)}
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

        return values_list

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
        if value.upper() in ["TRUE", "FALSE"]:
            return value.upper() == "TRUE"

        # Number
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            # Default to string
            return value

    def _parse_select(self, query: str) -> ParsedQuery:
        """Parse SELECT query"""
        # Extract columns
        select_match = re.match(r"SELECT (.+?) FROM", query, re.IGNORECASE)
        if not select_match:
            raise ValueError("SELECT statement must include FROM clause")

        columns_string = select_match.group(1).strip()
        if columns_string == "*":
            columns = ["*"]
        else:
            columns = [column.strip() for column in columns_string.split(",")]

        # Extract table names (handle JOINs)
        from_match = re.search(r"FROM (\w+)(?:\s+\w+)?", query, re.IGNORECASE)
        if not from_match:
            raise ValueError("SELECT statement must include FROM clause")

        table_name = from_match.group(1)

        # Check for JOIN
        join_table = None
        join_condition = None
        join_match = re.search(
            r"(?:INNER |LEFT |RIGHT )?JOIN (\w+)(?:\s+\w+)?\s+ON ([^WHERE\s]+(?:\s*=\s*[^WHERE\s]+)*)",
            query,
            re.IGNORECASE,
        )

        if join_match:
            join_table = join_match.group(1)
            join_on = join_match.group(2).strip()
            join_condition = self._parse_join_condition(join_on)

        # Parse WHERE clause
        where_clause = self._parse_where_clause(query)

        # Parse ORDER BY (basic implementation)
        order_by = None
        order_match = re.search(
            r"ORDER BY\s+(.+?)(?:\s+LIMIT|$)",
            query,
            re.IGNORECASE,
        )
        if order_match:
            order_by = [
                column.strip() for column in order_match.group(1).split(",")
            ]

        # Parse LIMIT
        limit = None
        limit_match = re.search(r"LIMIT (\d+)", query, re.IGNORECASE)
        if limit_match:
            limit = int(limit_match.group(1))

        return ParsedQuery(
            query_type="SELECT",
            table_name=table_name,
            columns=columns,
            join_table=join_table,
            join_condition=join_condition,
            where_clause=where_clause,
            order_by=order_by,
            limit=limit,
        )

    def _parse_join_condition(self, join_on: str) -> Dict[str, Any]:
        """Parse JOIN ON condition"""
        # Simple implementation: table1.col = table2.col
        match = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", join_on.strip())
        if match:
            return {
                "left_table": match.group(1),
                "left_column": match.group(2),
                "right_table": match.group(3),
                "right_column": match.group(4),
            }

        return {}

    def _parse_where_clause(self, query: str) -> Optional[Dict[str, Any]]:
        """Parse WHERE clause (basic implementation)"""
        where_match = re.search(
            r"WHERE (.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+GROUP\s+BY|$)",
            query,
            re.IGNORECASE,
        )
        if not where_match:
            return None

        where_str = where_match.group(1).strip()

        # Simple implementation: only handle AND conditions with equals
        conditions = {}

        # Split by AND
        and_parts = re.split(r"\s+AND\s+", where_str, flags=re.IGNORECASE)

        for part in and_parts:
            # Handle simple equality: column = value
            eq_match = re.match(r"(\w+(?:\.\w+)?)\s*=\s*(.+)", part.strip())
            if eq_match:
                col_name = eq_match.group(1)
                value_str = eq_match.group(2).strip()
                value = self._parse_value(value_str)
                conditions[col_name] = value

        return conditions if conditions else None

    def _parse_update(self, query: str) -> ParsedQuery:
        """Parse UPDATE statement"""
        # Pattern: UPDATE table_name SET col1=val1, col2=val2 WHERE condition
        match = re.match(
            r"UPDATE (\w+) SET (.+?)(?:\s+WHERE\s+(.+))?$",
            query,
            re.IGNORECASE,
        )
        if not match:
            raise ValueError("Invalid UPDATE syntax")

        table_name = match.group(1)
        set_str = match.group(2)

        # Parse SET clause
        set_values = {}
        set_parts = [part.strip() for part in set_str.split(",")]

        for part in set_parts:
            eq_match = re.match(r"(\w+)\s*=\s*(.+)", part)
            if eq_match:
                col_name = eq_match.group(1)
                value_str = eq_match.group(2).strip()
                value = self._parse_value(value_str)
                set_values[col_name] = value

        # Parse WHERE clause
        where_clause = self._parse_where_clause(query)

        return ParsedQuery(
            query_type="UPDATE",
            table_name=table_name,
            set_values=set_values,
            where_clause=where_clause,
        )

    def _parse_delete(self, query: str) -> ParsedQuery:
        """Parse DELETE statement"""
        # Pattern: DELETE FROM table_name WHERE condition
        match = re.match(
            r"DELETE FROM (\w+)(?:\s+WHERE\s+(.+))?$", query, re.IGNORECASE
        )
        if not match:
            raise ValueError("Invalid DELETE syntax")

        table_name = match.group(1)

        # Parse WHERE clause
        where_clause = self._parse_where_clause(query)

        return ParsedQuery(
            query_type="DELETE",
            table_name=table_name,
            where_clause=where_clause,
        )

    def _parse_drop_table(self, query: str) -> ParsedQuery:
        """Parse DROP TABLE statement"""
        match = re.match(r"DROP TABLE (\w+)", query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DROP TABLE syntax")

        table_name = match.group(1)

        return ParsedQuery(query_type="DROP_TABLE", table_name=table_name)

    def _parse_describe(self, query: str) -> ParsedQuery:
        """Parse DESCRIBE statement"""
        match = re.match(r"(?:DESCRIBE|DESC) (\w+)", query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DESCRIBE syntax")

        table_name = match.group(1)

        return ParsedQuery(query_type="DESCRIBE", table_name=table_name)

    def format_query_result(self, result: QueryResult) -> str:
        """Format query result for display"""
        if not result.success:
            return f"Error: {result.error}"

        if result.message:
            return result.message

        if not result.data:
            if (
                result.columns
                and isinstance(result.data, list)
                and len(result.data) == 0
            ):
                # Empty list with columns - show table format
                pass  # Continue to table formatting
            else:
                return "No results"

        # Format as table
        if result.columns:
            # Calculate column widths
            col_widths = {}
            for col in result.columns:
                col_widths[col] = max(
                    len(str(col)),
                    max(len(str(row.get(col, ""))) for row in result.data)
                    if result.data
                    else 0,
                )

            # Create header
            header = " | ".join(
                col.ljust(col_widths[col]) for col in result.columns
            )
            separator = "-" * len(header)

            # Create rows
            rows = []
            for row in result.data:
                row_str = " | ".join(
                    str(row.get(col, "")).ljust(col_widths[col])
                    for col in result.columns
                )
                rows.append(row_str)

            result_lines = [header, separator] + rows
            result_lines.append(f"\n{len(result.data)} row(s) returned")

            return "\n".join(result_lines)

        return str(result.data)
