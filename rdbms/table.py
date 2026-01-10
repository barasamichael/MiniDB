"""
Table implementation for simple RDBMS
Handles schema definition, data validation, and basic operations
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

# from typing import Union


class Column:
    """Represents a column in a table with type and constraints"""

    def __init__(
        self,
        name: str,
        data_type: str,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
    ):
        self.name = name
        self.data_type = data_type.upper()
        self.primary_key = primary_key
        self.unique = unique
        self.nullable = not primary_key and nullable

        # Supported data types
        self.supported_types = {"INT", "TEXT", "VARCHAR", "REAL", "BOOLEAN"}
        if self.data_type not in self.supported_types:
            raise ValueError(f"Unsupported data type: {data_type}")

    def validate_value(self, value: Any) -> Any:
        """Validate and convert value according to column type"""
        if value is None:
            if not self.nullable:
                raise ValueError(f"Column '{self.name}' cannot be NULL")
            return None

        try:
            if self.data_type == "INT":
                return int(value)

            elif self.data_type in ["TEXT", "VARCHAR"]:
                return str(value)

            elif self.data_type == "REAL":
                return float(value)

            elif self.data_type == "BOOLEAN":
                if isinstance(value, bool):
                    return value

                if str(value).lower() in ["false", "0", "no"]:
                    return False

                else:
                    raise ValueError(f"Invalid boolean value: {value}")

            else:
                return value
        except (ValueError, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for column '{self.name}' of type "
                + "{self.data_type}"
            )

    def to_dict(self) -> Dict:
        """Convert column to dictionary for serialization"""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "primary_key": self.primary_key,
            "unique": self.unique,
            "nullable": self.nullable,
        }

    @classmethod
    def from_dict(cls, data: Dict):
        """Create column from dictionary"""
        return cls(
            data["name"],
            data["data_type"],
            data["primary_key"],
            data["unique"],
            data["nullable"],
        )


class Table:
    """Represents a database table with schema and data"""

    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = {column.name: column for column in columns}
        self.column_order = [column.name for column in columns]
        self.rows: List[Dict[str, Any]] = []
        self.indexes: Dict[str, Dict[Any, List[int]]] = {}

        # Identify primary key and unique columns
        self.primary_key = None
        self.unique_columns = set()

        for column in columns:
            if column.primary_key:
                if self.primary_key:
                    raise ValueError("Table can have only one primary key")
                self.primary_key = column.name
                self.unique_columns.add(column.name)

        # Create indexes for primary key and unique columns
        self._create_indexes()

    def _create_indexes(self):
        """Create indexes for primary key and unique columns"""
        for column_name in self.unique_columns:
            self.indexes[column_name] = {}

    def _update_indexes(self, row: Dict[str, Any], row_index: int):
        """Update indexes when a row is added"""
        for column_name in self.indexes:
            value = row.get(column_name)

            if value is not None:
                if value not in self.indexes[column_name]:
                    self.indexes[column_name][value] = []

                self.indexes[column_name][value].append(row_index)

    def _remove_from_indexes(self, row: Dict[str, Any], row_index: int):
        """Remove row from indexes when deleted"""
        for column_name in self.indexes:
            value = row.get(column_name)

            if value is not None and value in self.indexes[column_name]:
                self.indexes[column_name][value].remove(row_index)
                if not self.indexes[column_name][value]:
                    del self.indexes[column_name][value]

    def _validate_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a row against the table schema"""
        validated_row = {}

        # Check for unknown columns
        for column_name in row:
            if column_name not in self.columns:
                raise ValueError(f"Unknown column: {column_name}")

        # Validate each column
        for column_name, column in self.columns.items():
            value = row.get(column_name)
            validated_value = column.validate_value(value)
            validated_row[column_name] = validated_value

            # Check unique constraints
            if (
                validated_value is not None
                and column_name in self.unique_columns
            ):
                if validated_value in self.indexes.get(column_name, {}):
                    constraint_type = (
                        "primary key"
                        if column_name == self.primary_key
                        else "unique"
                    )
                    raise ValueError(
                        f"Duplicate value '{validated_value}' for {constraint_type}"
                    )

    def insert(self, row: Dict[str, Any]) -> bool:
        """Insert a row into the table"""
        validated_row = self._validate_row(row)

        # Add the new row
        row_index = len(self.rows)
        self.rows.append(validated_row)

        # Update indexes
        self._update_indexes(validated_row, row_index)

        return True

    def select(
        self,
        columns: Optional[List[str]] = None,
        where_clause: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Select rows from the table"""
        # If no columns specified, select all
        if columns is None:
            columns = self.column_order

        # Validate columns
        for column in columns:
            if column not in self.columns and column != "*":
                raise ValueError(f"Unknown column: {column}")

        if columns == ["*"]:
            columns = self.column_order

        # Get matching rows
        matching_rows = []
        for row in self.rows:
            if self._matches_where_clause(row, where_clause):
                # Project only requested columns
                projected_row = {column: row.get(column) for column in columns}
                matching_rows.append(projected_row)

        return matching_rows

    def update(self):
        pass

    def delete(self):
        pass

    def to_dict(self) -> Dict:
        pass

    def _rebuild_indexes(self):
        """Rebuild all indexes after deletion"""
        pass

    def _matches_where_clause(
        self, row: Dict[str, Any], where_clause: Optional[Dict[str, Any]]
    ) -> bool:
        """Check if a row matches the where clause"""
        if where_clause is None:
            return True

        for column_name, expected_value in where_clause.items():
            if column_name not in self.columns:
                raise ValueError(
                    f"Unknown column in WHERE clause {column_name}"
                )

            actual_value = row.get(column_name)
            if actual_value != expected_value:
                return False

        return True

    @classmethod
    def from_dict(cls, data: Dict):
        pass
