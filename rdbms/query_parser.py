"""
SQL query parser for simple RDBMS
Parser basic SQL statements into structured data
"""
# import re
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
            pass
