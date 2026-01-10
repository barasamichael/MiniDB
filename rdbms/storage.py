"""
Storage engine for simple RDBMS
Handles persistence, file I/O, and data serialization
"""
import json
import time
import pickle

# import shutil
import threading
from pathlib import Path
from typing import List
from typing import Dict


class StorageEngine:
    """Handles data persistence and file operations"""

    def __init__(self, data_dir: str = "data", storage_format: str = "json"):
        """
        Initialize storage engine

        Args:
            data_dir: Directory to store database files
            storage_format: 'json' or 'pickle' for serialization
        """
        self.data_dir = Path(data_dir)
        self.storage_format = storage_format.lower()
        self.metadata_file = self.data_dir / "database_metadata.json"
        self._lock = threading.Lock()

        # Validate storage format
        if self.storage_format not in ["json", "pickle"]:
            raise ValueError("Storage format must be 'json' or 'pickle'")
        self.data_dir.mkdir(exist_ok=True)

        self._init_metadata()

    def _init_metadata(self):
        """Initialize or load database metadata"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r") as file:
                    self.metadata = json.load(file)

            except (json.JSONDecodeError, FileNotFoundError):
                self.metadata = self._create_default_metadata()
                self._save_metadata()

        else:
            self.metadata = self._create_default_metadata()
            self._save_metadata()

    def _create_default_metadata(self) -> Dict:
        """Create default metadata structure"""
        return {
            "version": "1.0",
            "created": time.time(),
            "last_modified": time.time(),
            "storage_format": self.storage_format,
            "tables": {},
            "table_count": 0,
        }

    def save_metadata(self):
        """Save metadata to disk"""
        self.metadata["last_modified"] = time.time()
        with open(self.metadata_file, "w") as file:
            json.dump(self.metadata, file, indent=2)

    def _get_table_filename(self, table_name: str) -> Path:
        """Get the filename for a table's data file"""
        extension = ".json" if self.storage_format == "json" else ".pkl"
        return self.data_dir / f"table_{table_name}{extension}"

    def save_table(self, table_name: str, table_data: Dict) -> bool:
        """
        Save table data to disk

        Args:
            table_name: Name of the table
            table_data: Table data as dictionary

        Returns:
            True if successful
        """
        with self._lock:
            try:
                filename = self._get_table_filename(table_name)

                if self.storage_format == "json":
                    with open(filename, "w") as file:
                        json.dump(table_data, file, indent=2, default=str)

                else:
                    with open(filename, "wb") as file:
                        pickle.dump(table_data, file)

                self.metadata["tables"][table_name] = {
                    "filename": str(filename),
                    "created": time.time()
                    if table_name not in self.metadata["tables"]
                    else self.metadata["tables"][table_name].get(
                        "created", time.time()
                    ),
                    "last_modified": time.time(),
                    "row_count": len(table_data.get("rows", [])),
                    "size_bytes": filename.stat().st_size
                    if filename.exists()
                    else 0,
                }

                if table_name not in [
                    table for table in self.metadata["tables"]
                ]:
                    self.metadata["table_count"] += 1

                self._save_metadata()
                return True

            except Exception as e:
                print(e.str())

    def load_table(self) -> bool:
        pass

    def delete_table(self) -> bool:
        pass

    def list_tables(self) -> List(str):
        pass

    def table_exists(self) -> bool:
        pass

    def get_table_info(self):
        pass

    def get_database_stats(self):
        pass

    def backup_database(self):
        pass

    def restore_database(self):
        pass

    def compact_database(self):
        pass

    def close(self):
        pass


class MemoryStorage:
    """In-memory storage for testing or temporary databases"""

    pass
