"""
Automated tests for the storage module
Tests StorageEngine and MemoryStorage classes with all functionality
"""

import os
import sys
import pytest
import shutil
import tempfile
from pathlib import Path

# Add the parent directory to the path to import the rdbms module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.storage import StorageEngine # noqa
from rdbms.storage import MemoryStorage # noqa


class TestStorageEngine:
    """Test cases for the StorageEngine class"""

    def setup_method(self):
        """Set up test fixtures"""
        # Use temporary directory for each test
        self.test_dir = tempfile.mkdtemp(prefix="test_storage_")
        self.storage = StorageEngine(
            data_dir=self.test_dir, storage_format="json"
        )

        # Sample table data
        self.sample_table_data = {
            "name": "users",
            "columns": [
                {
                    "name": "id",
                    "data_type": "INT",
                    "primary_key": True,
                    "unique": True,
                    "nullable": False,
                },
                {
                    "name": "name",
                    "data_type": "TEXT",
                    "primary_key": False,
                    "unique": False,
                    "nullable": False,
                },
            ],
            "column_order": ["id", "name"],
            "rows": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
            "primary_key": "id",
            "unique_columns": ["id"],
        }

    def teardown_method(self):
        """Clean up after each test"""
        self.storage.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_storage_engine_initialization(self):
        """Test storage engine initialization"""
        assert self.storage.data_dir == Path(self.test_dir)
        assert self.storage.storage_format == "json"
        assert self.storage.metadata_file.exists()
        assert self.storage.metadata["version"] == "1.0"
        assert self.storage.metadata["storage_format"] == "json"
        assert self.storage.metadata["table_count"] == 0

    def test_storage_engine_pickle_format(self):
        """Test storage engine with pickle format"""
        pickle_dir = tempfile.mkdtemp(prefix="test_pickle_")
        try:
            pickle_storage = StorageEngine(
                data_dir=pickle_dir, storage_format="pickle"
            )
            assert pickle_storage.storage_format == "pickle"
            pickle_storage.close()
        finally:
            if os.path.exists(pickle_dir):
                shutil.rmtree(pickle_dir)

    def test_invalid_storage_format(self):
        """Test error handling for invalid storage format"""
        with pytest.raises(ValueError, match="Storage format must be"):
            StorageEngine(storage_format="invalid")

    def test_save_table_json(self):
        """Test saving table data in JSON format"""
        result = self.storage.save_table("users", self.sample_table_data)
        assert result == True

        # Check file was created
        table_file = self.storage._get_table_filename("users")
        assert table_file.exists()

        # Check metadata was updated
        assert "users" in self.storage.metadata["tables"]
        assert self.storage.metadata["table_count"] == 1
        assert self.storage.metadata["tables"]["users"]["row_count"] == 2

    def test_save_table_pickle(self):
        """Test saving table data in pickle format"""
        pickle_dir = tempfile.mkdtemp(prefix="test_pickle_save_")
        try:
            pickle_storage = StorageEngine(
                data_dir=pickle_dir, storage_format="pickle"
            )
            result = pickle_storage.save_table("users", self.sample_table_data)
            assert result == True

            # Check file was created with .pkl extension
            table_file = pickle_storage._get_table_filename("users")
            assert table_file.exists()
            assert table_file.suffix == ".pkl"

            pickle_storage.close()
        finally:
            if os.path.exists(pickle_dir):
                shutil.rmtree(pickle_dir)

    def test_load_table_json(self):
        """Test loading table data from JSON"""
        # Save first
        self.storage.save_table("users", self.sample_table_data)

        # Load and verify
        loaded_data = self.storage.load_table("users")
        assert loaded_data is not None
        assert loaded_data["name"] == "users"
        assert len(loaded_data["rows"]) == 2
        assert loaded_data["rows"][0]["id"] == 1
        assert loaded_data["rows"][0]["name"] == "John"

    def test_load_table_pickle(self):
        """Test loading table data from pickle"""
        pickle_dir = tempfile.mkdtemp(prefix="test_pickle_load_")
        try:
            pickle_storage = StorageEngine(
                data_dir=pickle_dir, storage_format="pickle"
            )

            # Save first
            pickle_storage.save_table("users", self.sample_table_data)

            # Load and verify
            loaded_data = pickle_storage.load_table("users")
            assert loaded_data is not None
            assert loaded_data["name"] == "users"
            assert len(loaded_data["rows"]) == 2

            pickle_storage.close()
        finally:
            if os.path.exists(pickle_dir):
                shutil.rmtree(pickle_dir)

    def test_load_nonexistent_table(self):
        """Test loading a table that doesn't exist"""
        result = self.storage.load_table("nonexistent")
        assert result is None

    def test_delete_table(self):
        """Test deleting a table"""
        # Save first
        self.storage.save_table("users", self.sample_table_data)
        assert self.storage.table_exists("users")

        # Delete
        result = self.storage.delete_table("users")
        assert result == True
        assert not self.storage.table_exists("users")
        assert self.storage.metadata["table_count"] == 0

        # Check file was deleted
        table_file = self.storage._get_table_filename("users")
        assert not table_file.exists()

    def test_delete_nonexistent_table(self):
        """Test deleting a table that doesn't exist"""
        result = self.storage.delete_table("nonexistent")
        assert result == False

    def test_list_tables(self):
        """Test listing all tables"""
        # Initially empty
        assert self.storage.list_tables() == []

        # Add some tables
        self.storage.save_table("users", self.sample_table_data)
        self.storage.save_table("products", {"name": "products", "rows": []})

        tables = self.storage.list_tables()
        assert len(tables) == 2
        assert "users" in tables
        assert "products" in tables

    def test_table_exists(self):
        """Test checking if table exists"""
        assert not self.storage.table_exists("users")

        self.storage.save_table("users", self.sample_table_data)
        assert self.storage.table_exists("users")

    def test_get_table_info(self):
        """Test getting table metadata"""
        # Nonexistent table
        assert self.storage.get_table_info("nonexistent") is None

        # Save table and check info
        self.storage.save_table("users", self.sample_table_data)
        info = self.storage.get_table_info("users")

        assert info is not None
        assert info["row_count"] == 2
        assert "created" in info
        assert "last_modified" in info
        assert "size_bytes" in info

    def test_get_database_stats(self):
        """Test getting overall database statistics"""
        # Empty database
        stats = self.storage.get_database_stats()
        assert stats["table_count"] == 0
        assert stats["total_rows"] == 0
        assert stats["storage_format"] == "json"

        # Add some data
        self.storage.save_table("users", self.sample_table_data)
        self.storage.save_table(
            "products", {"name": "products", "rows": [{"id": 1}]}
        )

        stats = self.storage.get_database_stats()
        assert stats["table_count"] == 2
        assert stats["total_rows"] == 3  # 2 from users + 1 from products
        assert "total_size_bytes" in stats
        assert "data_directory" in stats

    def test_backup_database(self):
        """Test database backup functionality"""
        # Add some data
        self.storage.save_table("users", self.sample_table_data)

        backup_dir = tempfile.mkdtemp(prefix="test_backup_")
        try:
            result = self.storage.backup_database(backup_dir)
            assert result == True

            # Check backup files exist
            backup_path = Path(backup_dir)
            assert (backup_path / "database_metadata.json").exists()
            assert (backup_path / "table_users.json").exists()

        finally:
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)

    def test_restore_database(self):
        """Test database restore functionality"""
        # Create original data
        self.storage.save_table("users", self.sample_table_data)

        # Create backup
        backup_dir = tempfile.mkdtemp(prefix="test_restore_backup_")
        self.storage.backup_database(backup_dir)

        # Modify original data
        self.storage.save_table("modified", {"name": "modified", "rows": []})
        assert self.storage.table_exists("modified")

        try:
            # Restore from backup
            result = self.storage.restore_database(backup_dir)
            assert result == True

            # Check original data is restored
            assert self.storage.table_exists("users")
            assert not self.storage.table_exists("modified")

            loaded_data = self.storage.load_table("users")
            assert len(loaded_data["rows"]) == 2

        finally:
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)

    def test_restore_from_nonexistent_backup(self):
        """Test restore from nonexistent backup directory"""
        result = self.storage.restore_database("/nonexistent/path")
        assert result == False

    def test_compact_database(self):
        """Test database compaction"""
        # Add some data
        self.storage.save_table("users", self.sample_table_data)
        self.storage.save_table("products", {"name": "products", "rows": []})

        result = self.storage.compact_database()
        assert result == True

        # Verify tables still exist and are readable
        assert self.storage.table_exists("users")
        assert self.storage.table_exists("products")

        loaded_data = self.storage.load_table("users")
        assert len(loaded_data["rows"]) == 2

    def test_metadata_persistence(self):
        """Test that metadata persists across storage instances"""
        # Save data with first instance
        self.storage.save_table("users", self.sample_table_data)
        self.storage.close()

        # Create new instance and verify metadata
        new_storage = StorageEngine(data_dir=self.test_dir)
        assert new_storage.table_exists("users")
        assert new_storage.metadata["table_count"] == 1
        new_storage.close()

    def test_threading_safety(self):
        """Test basic threading safety with locks"""
        import threading

        results = []

        def save_table(table_name, data):
            result = self.storage.save_table(table_name, data)
            results.append(result)

        # Create multiple threads
        threads = []
        for i in range(5):
            table_data = {"name": f"table_{i}", "rows": [{"id": i}]}
            thread = threading.Thread(
                target=save_table, args=(f"table_{i}", table_data)
            )
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify all operations succeeded
        assert len(results) == 5
        assert all(results)
        assert self.storage.metadata["table_count"] == 5

    def test_error_handling_corrupted_metadata(self):
        """Test handling of corrupted metadata file"""
        # Corrupt the metadata file
        with open(self.storage.metadata_file, "w") as f:
            f.write("invalid json")

        # Create new storage instance - should create fresh metadata
        new_storage = StorageEngine(data_dir=self.test_dir)
        assert new_storage.metadata["table_count"] == 0
        new_storage.close()

    def test_save_load_with_special_characters(self):
        """Test saving and loading table data with special characters"""
        special_data = {
            "name": "special_chars",
            "rows": [
                {"text": "Hello, 世界! 🌟"},
                {"text": "Symbols: @#$%^&*()"},
                {"text": "Quotes: 'single' \"double\""},
            ],
        }

        result = self.storage.save_table("special", special_data)
        assert result == True

        loaded_data = self.storage.load_table("special")
        assert loaded_data is not None
        assert loaded_data["rows"][0]["text"] == "Hello, 世界! 🌟"

    def test_large_table_handling(self):
        """Test handling of relatively large table data"""
        large_data = {
            "name": "large_table",
            "rows": [{"id": i, "data": f"row_{i}" * 100} for i in range(1000)],
        }

        result = self.storage.save_table("large", large_data)
        assert result == True

        loaded_data = self.storage.load_table("large")
        assert loaded_data is not None
        assert len(loaded_data["rows"]) == 1000


class TestMemoryStorage:
    """Test cases for the MemoryStorage class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.memory_storage = MemoryStorage()
        self.sample_table_data = {
            "name": "users",
            "rows": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
        }

    def test_memory_storage_initialization(self):
        """Test memory storage initialization"""
        assert self.memory_storage.tables == {}
        assert self.memory_storage.metadata["version"] == "1.0"
        assert self.memory_storage.metadata["storage_format"] == "memory"
        assert self.memory_storage.metadata["table_count"] == 0

    def test_save_table_memory(self):
        """Test saving table in memory"""
        result = self.memory_storage.save_table("users", self.sample_table_data)
        assert result == True
        assert "users" in self.memory_storage.tables
        assert self.memory_storage.metadata["table_count"] == 1

    def test_load_table_memory(self):
        """Test loading table from memory"""
        # Save first
        self.memory_storage.save_table("users", self.sample_table_data)

        # Load and verify
        loaded_data = self.memory_storage.load_table("users")
        assert loaded_data is not None
        assert loaded_data["name"] == "users"
        assert len(loaded_data["rows"]) == 2

        # Verify it's a copy, not a reference
        loaded_data["rows"].append({"id": 3, "name": "Bob"})
        original = self.memory_storage.load_table("users")
        assert len(original["rows"]) == 2  # Original unchanged

    def test_load_nonexistent_table_memory(self):
        """Test loading nonexistent table from memory"""
        result = self.memory_storage.load_table("nonexistent")
        assert result is None

    def test_delete_table_memory(self):
        """Test deleting table from memory"""
        # Save first
        self.memory_storage.save_table("users", self.sample_table_data)
        assert self.memory_storage.table_exists("users")

        # Delete
        result = self.memory_storage.delete_table("users")
        assert result == True
        assert not self.memory_storage.table_exists("users")
        assert self.memory_storage.metadata["table_count"] == 0

    def test_delete_nonexistent_table_memory(self):
        """Test deleting nonexistent table from memory"""
        result = self.memory_storage.delete_table("nonexistent")
        assert result == False

    def test_list_tables_memory(self):
        """Test listing tables in memory"""
        # Initially empty
        assert self.memory_storage.list_tables() == []

        # Add tables
        self.memory_storage.save_table("users", self.sample_table_data)
        self.memory_storage.save_table(
            "products", {"name": "products", "rows": []}
        )

        tables = self.memory_storage.list_tables()
        assert len(tables) == 2
        assert "users" in tables
        assert "products" in tables

    def test_table_exists_memory(self):
        """Test checking table existence in memory"""
        assert not self.memory_storage.table_exists("users")

        self.memory_storage.save_table("users", self.sample_table_data)
        assert self.memory_storage.table_exists("users")

    def test_get_table_info_memory(self):
        """Test getting table info from memory"""
        # Nonexistent table
        assert self.memory_storage.get_table_info("nonexistent") is None

        # Save and get info
        self.memory_storage.save_table("users", self.sample_table_data)
        info = self.memory_storage.get_table_info("users")

        assert info is not None
        assert info["row_count"] == 2
        assert "created" in info
        assert "last_modified" in info

    def test_get_database_stats_memory(self):
        """Test getting database stats from memory"""
        # Empty database
        stats = self.memory_storage.get_database_stats()
        assert stats["table_count"] == 0
        assert stats["total_rows"] == 0
        assert stats["storage_format"] == "memory"

        # Add data
        self.memory_storage.save_table("users", self.sample_table_data)
        self.memory_storage.save_table(
            "products", {"name": "products", "rows": [{}]}
        )

        stats = self.memory_storage.get_database_stats()
        assert stats["table_count"] == 2
        assert stats["total_rows"] == 3

    def test_memory_storage_isolation(self):
        """Test that memory storage instances are isolated"""
        storage1 = MemoryStorage()
        storage2 = MemoryStorage()

        storage1.save_table("table1", {"rows": []})

        assert storage1.table_exists("table1")
        assert not storage2.table_exists("table1")

    def test_close_memory(self):
        """Test closing memory storage"""
        self.memory_storage.close()  # Should not raise any errors


class TestStorageComparison:
    """Test compatibility between different storage backends"""

    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp(prefix="test_comparison_")
        self.sample_data = {
            "name": "test_table",
            "rows": [{"id": 1, "value": "test"}],
        }

    def teardown_method(self):
        """Clean up after tests"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_json_pickle_compatibility(self):
        """Test that data saved in JSON can be loaded and resaved in pickle"""
        # Save with JSON
        json_storage = StorageEngine(
            data_dir=self.test_dir, storage_format="json"
        )
        json_storage.save_table("test", self.sample_data)
        loaded_json = json_storage.load_table("test")
        json_storage.close()

        # Load with pickle (should create new pickle file)
        pickle_dir = tempfile.mkdtemp(prefix="test_pickle_compat_")
        try:
            pickle_storage = StorageEngine(
                data_dir=pickle_dir, storage_format="pickle"
            )
            pickle_storage.save_table("test", loaded_json)
            loaded_pickle = pickle_storage.load_table("test")

            # Data should be equivalent
            assert loaded_pickle["name"] == loaded_json["name"]
            assert loaded_pickle["rows"] == loaded_json["rows"]

            pickle_storage.close()
        finally:
            if os.path.exists(pickle_dir):
                shutil.rmtree(pickle_dir)

    def test_memory_disk_compatibility(self):
        """Test that data can be transferred between memory and disk storage"""
        # Save in memory
        memory_storage = MemoryStorage()
        memory_storage.save_table("test", self.sample_data)
        memory_data = memory_storage.load_table("test")

        # Transfer to disk
        disk_storage = StorageEngine(data_dir=self.test_dir)
        disk_storage.save_table("test", memory_data)
        disk_data = disk_storage.load_table("test")

        # Data should be equivalent
        assert disk_data["name"] == memory_data["name"]
        assert disk_data["rows"] == memory_data["rows"]

        disk_storage.close()


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
