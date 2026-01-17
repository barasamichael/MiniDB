"""
Automated tests for the room model
Tests Room class with all CRUD operations and business logic
"""

import os
import sys
import pytest
from unittest.mock import Mock
from unittest.mock import patch

# Add the parent directories to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models"
    ),
)

from models.room import Room  # noqa
from rdbms.query_parser import QueryResult  # noqa


class TestRoom:
    """Test cases for the Room model"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_db = Mock()
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        with patch("models.room.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = (
                "2024-01-15 10:30:00"
            )
            # Mock get_all to return empty for seeding check
            with patch.object(Room, "get_all", return_value=[]):
                self.room = Room(self.mock_db)

    def test_room_initialization(self):
        """Test room model initialization"""
        assert self.room.db == self.mock_db
        assert self.room.table_name == "rooms"

    def test_ensure_table_exists_creates_table(self):
        """Test table creation when table doesn't exist"""
        self.mock_db.list_tables.return_value = []
        with patch.object(Room, "get_all", return_value=[]):
            Room(self.mock_db)

        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "CREATE TABLE rooms" in create_call
        assert "roomId INT PRIMARY KEY" in create_call
        assert "roomNumber TEXT UNIQUE NOT NULL" in create_call
        assert "isAvailable BOOLEAN NOT NULL" in create_call
        assert "pricePerNight REAL NOT NULL" in create_call

    def test_ensure_table_exists_creation_fails(self):
        """Test table creation failure"""
        self.mock_db.list_tables.return_value = []
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Creation failed"
        )

        with pytest.raises(Exception, match="Failed to create rooms table"):
            Room(self.mock_db)

    def test_seed_initial_rooms_when_empty(self):
        """Test that initial rooms are seeded when table is empty"""
        self.mock_db.list_tables.return_value = []

        with patch.object(Room, "get_all", return_value=[]):
            with patch.object(Room, "create") as mock_create:
                Room(self.mock_db)

                # Should call create for initial rooms (6 rooms)
                assert mock_create.call_count == 6

                # Check that standard, deluxe, and suite rooms are created
                call_args_list = [
                    call[0][0] for call in mock_create.call_args_list
                ]
                room_types = [room["roomType"] for room in call_args_list]
                assert "Standard" in room_types
                assert "Deluxe" in room_types
                assert "Suite" in room_types

    def test_seed_initial_rooms_when_not_empty(self):
        """Test that initial rooms are not seeded when table has data"""
        existing_rooms = [{"roomId": 1, "roomNumber": "101"}]

        with patch.object(Room, "get_all", return_value=existing_rooms):
            with patch.object(Room, "create") as mock_create:
                Room(self.mock_db)

                # Should not call create when rooms already exist
                mock_create.assert_not_called()

    @patch("models.room.datetime")
    def test_create_room_success(self, mock_datetime):
        """Test successful room creation"""
        mock_datetime.now.return_value.strftime.return_value = (
            "2024-01-15 10:30:00"
        )

        with patch.object(self.room, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            room_data = {
                "roomNumber": "101",
                "roomType": "Standard",
                "capacity": 2,
                "pricePerNight": 8500.00,
                "description": "Standard room",
                "amenities": "WiFi||AC||TV",
                "features": "Garden View",
                "imageUrl": "room_1.jpg",
            }

            expected_room = {
                "roomId": 1,
                "roomNumber": "101",
                "roomType": "Standard",
                "isAvailable": True,
                "dateCreated": "2024-01-15 10:30:00",
            }

            with patch.object(
                self.room, "get_by_id", return_value=expected_room
            ):
                result = self.room.create(room_data)
                assert result == expected_room

    def test_create_room_with_availability_false(self):
        """Test room creation with custom availability"""
        with patch.object(self.room, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(success=True)

            room_data = {
                "roomNumber": "102",
                "roomType": "Standard",
                "capacity": 2,
                "pricePerNight": 8500.00,
                "description": "Standard room",
                "isAvailable": False,
            }

            with patch.object(self.room, "get_by_id", return_value=room_data):
                result = self.room.create(room_data)
                assert result["isAvailable"] == False

    def test_create_room_failure(self):
        """Test room creation failure"""
        with patch.object(self.room, "_get_next_id", return_value=1):
            self.mock_db.execute_query.return_value = QueryResult(
                success=False, error="Insert failed"
            )

            room_data = {
                "roomNumber": "101",
                "roomType": "Standard",
                "capacity": 2,
                "pricePerNight": 8500.00,
            }

            with pytest.raises(Exception, match="Failed to create room"):
                self.room.create(room_data)

    def test_get_by_id_success(self):
        """Test successful get room by ID"""
        room_data = {
            "roomId": 1,
            "roomNumber": "101",
            "roomType": "Standard",
            "capacity": 2,
            "pricePerNight": 8500.00,
        }

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[room_data]
        )

        result = self.room.get_by_id(1)
        assert result == room_data

    def test_get_by_id_not_found(self):
        """Test get room by ID when not found"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        result = self.room.get_by_id(999)
        assert result is None

    def test_get_by_type_success(self):
        """Test successful get rooms by type"""
        rooms_data = [
            {"roomId": 1, "roomType": "Standard", "isAvailable": True},
            {"roomId": 2, "roomType": "Standard", "isAvailable": True},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=rooms_data
        )

        result = self.room.get_by_type("Standard")
        assert result == rooms_data

        # Verify WHERE clause includes both type and availability
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "WHERE roomType = 'Standard'" in call_args
        assert "isAvailable = true" in call_args

    def test_get_by_type_no_results(self):
        """Test get rooms by type with no results"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        result = self.room.get_by_type("NonExistentType")
        assert result == []

    def test_get_all_success(self):
        """Test successful get all rooms"""
        rooms_data = [
            {"roomId": 1, "roomType": "Standard"},
            {"roomId": 2, "roomType": "Deluxe"},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=rooms_data
        )

        result = self.room.get_all()
        assert result == rooms_data

    def test_get_all_query_failure(self):
        """Test get all rooms when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        result = self.room.get_all()
        assert result == []

    def test_get_available_rooms_success(self):
        """Test successful get available rooms"""
        available_rooms = [
            {"roomId": 1, "roomType": "Standard", "isAvailable": True},
            {"roomId": 3, "roomType": "Suite", "isAvailable": True},
        ]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=available_rooms
        )

        result = self.room.get_available_rooms()
        assert result == available_rooms

        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "WHERE isAvailable = true" in call_args

    def test_update_room_success(self):
        """Test successful room update"""
        self.mock_db.execute_query.return_value = QueryResult(success=True)

        update_data = {
            "pricePerNight": 9000.00,
            "isAvailable": False,
            "description": "Updated description",
        }

        result = self.room.update(1, update_data)
        assert result == True

        # Verify UPDATE query structure
        call_args = self.mock_db.execute_query.call_args[0][0]
        assert "UPDATE rooms SET" in call_args
        assert "WHERE roomId = 1" in call_args

    def test_update_room_no_data(self):
        """Test room update with no valid data"""
        result = self.room.update(1, {"roomId": 1})
        assert result == False

        # Should not call database
        assert self.mock_db.execute_query.call_count == 0

    def test_update_room_failure(self):
        """Test room update failure"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Update failed"
        )

        update_data = {"pricePerNight": 9000.00}
        result = self.room.update(1, update_data)
        assert result == False

    def test_get_next_id_first_room(self):
        """Test getting next ID when no rooms exist"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=[]
        )
        next_id = self.room._get_next_id()
        assert next_id == 1

    def test_get_next_id_existing_rooms(self):
        """Test next ID calculation with existing rooms"""
        existing_rooms = [{"roomId": 1}, {"roomId": 3}, {"roomId": 2}]

        self.mock_db.execute_query.return_value = QueryResult(
            success=True, data=existing_rooms
        )

        next_id = self.room._get_next_id()
        assert next_id == 4  # max(1,3,2) + 1

    def test_get_next_id_query_failure(self):
        """Test getting next ID when query fails"""
        self.mock_db.execute_query.return_value = QueryResult(
            success=False, error="Query failed"
        )

        next_id = self.room._get_next_id()
        assert next_id == 1

    def test_initial_room_data_structure(self):
        """Test that initial rooms have correct data structure"""
        self.mock_db.list_tables.return_value = []

        with patch.object(Room, "get_all", return_value=[]):
            with patch.object(Room, "create") as mock_create:
                Room(self.mock_db)

                # Get the first room created
                first_room = mock_create.call_args_list[0][0][0]

                # Verify required fields
                assert "roomNumber" in first_room
                assert "roomType" in first_room
                assert "capacity" in first_room
                assert "pricePerNight" in first_room
                assert "amenities" in first_room
                assert "features" in first_room
                assert "isAvailable" in first_room
                assert first_room["isAvailable"] == True

    def test_initial_room_types_and_pricing(self):
        """Test that initial rooms have correct types and pricing"""
        with patch.object(Room, "get_all", return_value=[]):
            with patch.object(Room, "create") as mock_create:
                Room(self.mock_db)

                created_rooms = [
                    call[0][0] for call in mock_create.call_args_list
                ]

                # Check Standard rooms
                standard_rooms = [
                    r for r in created_rooms if r["roomType"] == "Standard"
                ]
                assert len(standard_rooms) == 2
                assert all(
                    r["pricePerNight"] == 8500.00 for r in standard_rooms
                )

                # Check Deluxe rooms
                deluxe_rooms = [
                    r for r in created_rooms if r["roomType"] == "Deluxe"
                ]
                assert len(deluxe_rooms) == 2
                assert all(r["pricePerNight"] == 12500.00 for r in deluxe_rooms)

                # Check Suite rooms
                suite_rooms = [
                    r for r in created_rooms if r["roomType"] == "Suite"
                ]
                assert len(suite_rooms) == 2
                assert all(r["pricePerNight"] == 20000.00 for r in suite_rooms)

    def test_room_number_uniqueness(self):
        """Test that room number uniqueness is enforced"""
        create_call = self.mock_db.execute_query.call_args[0][0]
        assert "roomNumber TEXT UNIQUE NOT NULL" in create_call

    def test_boolean_handling_in_sql(self):
        """Test proper handling of boolean values in SQL"""
        update_data = {
            "isAvailable": False,
            "capacity": 4,
            "pricePerNight": 9500.50,
        }

        self.mock_db.execute_query.return_value = QueryResult(success=True)

        self.room.update(1, update_data)

        call_args = self.mock_db.execute_query.call_args[0][0]
        # Boolean should be represented properly
        assert "isAvailable = False" in call_args
        # Numeric should not be quoted
        assert "capacity = 4" in call_args
        assert "pricePerNight = 9500.5" in call_args

    def test_string_handling_in_sql(self):
        """Test proper handling of string values in SQL"""
        update_data = {
            "description": "Updated room description",
            "amenities": "WiFi||AC||TV||Mini Bar",
        }

        self.mock_db.execute_query.return_value = QueryResult(success=True)

        self.room.update(1, update_data)

        call_args = self.mock_db.execute_query.call_args[0][0]
        # String values should be quoted
        assert "'Updated room description'" in call_args
        assert "'WiFi||AC||TV||Mini Bar'" in call_args

    def test_sql_injection_protection(self):
        """Test SQL injection protection"""
        with patch.object(self.room, "_get_next_id", return_value=1):
            with patch.object(self.room, "get_by_id", return_value={}):
                self.mock_db.execute_query.return_value = QueryResult(
                    success=True
                )

                room_data = {
                    "roomNumber": "101'; DROP TABLE rooms;--",
                    "roomType": "Standard'; DROP--",
                    "capacity": 2,
                    "pricePerNight": 8500.00,
                    "description": "Test'; DROP TABLE rooms;--",
                }

                self.room.create(room_data)

                call_args = self.mock_db.execute_query.call_args[0][0]
                # Verify malicious strings are properly quoted
                assert "'101'; DROP TABLE rooms;--'" in call_args
                assert "'Standard'; DROP--'" in call_args
                assert "'Test'; DROP TABLE rooms;--'" in call_args

    def test_amenities_and_features_format(self):
        """Test that amenities and features use proper delimiter format"""
        with patch.object(Room, "get_all", return_value=[]):
            with patch.object(Room, "create") as mock_create:
                Room(self.mock_db)

                first_room = mock_create.call_args_list[0][0][0]

                # Check amenities format (pipe-delimited)
                assert "||" in first_room["amenities"]
                assert "||" in first_room["features"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
