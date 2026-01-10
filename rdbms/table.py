"""
Table implementation for simple RDBMS
Handles schema definition, data validation, and basic operations
"""

from typing import Any
from typing import Dict

# from typing import List
# from typing import Union
# from typing import Optional


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
        pass

    def to_dict(self) -> Dict:
        """Convert column to dictionary for serialization"""
        pass

    @classmethod
    def from_dict(cls, data: Dict):
        """Create column from dictionary"""
        pass


class Table:
    """Represents a database table with schema and data"""

    pass
