"""
Room model for resort booking system using custom RDBMS
Individual rooms that can be booked
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

sys.path.append(str(Path(__file__).parent.parent / "rdbms"))

from database import Database  # noqa


class Room:
    """Room model - individual bookable rooms"""

    def __init__(self, db: Database):
        self.db = db
        self.table_name = "rooms"
        self._ensure_table_exists()
        self._seed_initial_rooms()

    def _ensure_table_exists(self):
        """Create rooms table if it doesn't exist"""
        if self.table_name not in self.db.list_tables():
            create_query = """
            CREATE TABLE rooms (
                roomId INT PRIMARY KEY,
                roomNumber TEXT UNIQUE NOT NULL,
                roomType TEXT NOT NULL,
                capacity INT NOT NULL,
                pricePerNight REAL NOT NULL,
                description TEXT,
                amenities TEXT,
                features TEXT,
                imageUrl TEXT,
                isAvailable BOOLEAN NOT NULL,
                dateCreated TEXT NOT NULL
            )
            """
            result = self.db.execute_query(create_query)
            if not result.success:
                raise Exception(f"Failed to create rooms table: {result.error}")

    def _seed_initial_rooms(self):
        """Seed initial room data if table is empty"""
        existing_rooms = self.get_all()
        if not existing_rooms:
            initial_rooms = [
                {
                    "roomNumber": "101",
                    "roomType": "Standard",
                    "capacity": 2,
                    "pricePerNight": 8500.00,
                    "description": "Comfortable standard room with garden view",
                    "amenities": "WiFi||AC||TV||Mini Fridge",
                    "features": "Garden View||Private Bathroom",
                    "imageUrl": "room_1.jpg",
                    "isAvailable": True,
                },
                {
                    "roomNumber": "102",
                    "roomType": "Standard",
                    "capacity": 2,
                    "pricePerNight": 8500.00,
                    "description": "Comfortable standard room with garden view",
                    "amenities": "WiFi||AC||TV||Mini Fridge",
                    "features": "Garden View||Private Bathroom",
                    "imageUrl": "room_2.jpg",
                    "isAvailable": True,
                },
                {
                    "roomNumber": "201",
                    "roomType": "Deluxe",
                    "capacity": 3,
                    "pricePerNight": 12500.00,
                    "description": "Spacious deluxe room with balcony",
                    "amenities": "WiFi||AC||TV||Mini Fridge||Balcony||Room Service",
                    "features": "Balcony||Mountain View||Private Bathroom",
                    "imageUrl": "room_3.jpg",
                    "isAvailable": True,
                },
                {
                    "roomNumber": "202",
                    "roomType": "Deluxe",
                    "capacity": 3,
                    "pricePerNight": 12500.00,
                    "description": "Spacious deluxe room with balcony",
                    "amenities": "WiFi||AC||TV||Mini Fridge||Balcony||Room Service",
                    "features": "Balcony||Mountain View||Private Bathroom",
                    "imageUrl": "room_4.jpg",
                    "isAvailable": True,
                },
                {
                    "roomNumber": "301",
                    "roomType": "Suite",
                    "capacity": 4,
                    "pricePerNight": 20000.00,
                    "description": "Luxury suite with living area",
                    "amenities": "WiFi||AC||TV||Mini Fridge||Balcony||Room Service||Kitchenette||Jacuzzi",
                    "features": "Living Area||Kitchenette||Jacuzzi||Panoramic View",
                    "imageUrl": "room_5.jpg",
                    "isAvailable": True,
                },
                {
                    "roomNumber": "302",
                    "roomType": "Suite",
                    "capacity": 4,
                    "pricePerNight": 20000.00,
                    "description": "Luxury suite with living area",
                    "amenities": "WiFi||AC||TV||Mini Fridge||Balcony||Room Service||Kitchenette||Jacuzzi",
                    "features": "Living Area||Kitchenette||Jacuzzi||Panoramic View",
                    "imageUrl": "room_6.jpg",
                    "isAvailable": True,
                },
            ]

            for room_data in initial_rooms:
                self.create(room_data)

    def create(self, room_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new room"""
        room_id = self._get_next_id()

        room_data["roomId"] = room_id
        room_data["dateCreated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        room_data["isAvailable"] = room_data.get("isAvailable", True)

        insert_query = f"""
        INSERT INTO rooms (roomId, roomNumber, roomType, capacity, pricePerNight, description, 
                          amenities, features, imageUrl, isAvailable, dateCreated)
        VALUES ({room_id}, '{room_data['roomNumber']}', '{room_data['roomType']}', 
                {room_data['capacity']}, {room_data['pricePerNight']}, '{room_data['description']}',
                '{room_data['amenities']}', '{room_data['features']}', '{room_data['imageUrl']}',
                {room_data['isAvailable']}, '{room_data['dateCreated']}')
        """

        result = self.db.execute_query(insert_query)
        if result.success:
            return self.get_by_id(room_id)
        else:
            raise Exception(f"Failed to create room: {result.error}")

    def get_by_id(self, room_id: int) -> Optional[Dict[str, Any]]:
        """Get room by ID"""
        query = f"SELECT * FROM rooms WHERE roomId = {room_id}"
        result = self.db.execute_query(query)

        if result.success and result.data:
            return result.data[0]

        return None

    def get_by_type(self, room_type: str) -> List[Dict[str, Any]]:
        """Get rooms by type"""
        query = f"SELECT * FROM rooms WHERE roomType = '{room_type}' AND isAvailable = true"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all rooms"""
        query = "SELECT * FROM rooms"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def get_available_rooms(self) -> List[Dict[str, Any]]:
        """Get all available rooms"""
        query = "SELECT * FROM rooms WHERE isAvailable = true"
        result = self.db.execute_query(query)

        if result.success:
            return result.data or []

        return []

    def update(self, room_id: int, update_data: Dict[str, Any]) -> bool:
        """Update room"""
        set_clauses = []
        for key, value in update_data.items():
            if key != "roomId":
                if isinstance(value, str):
                    set_clauses.append(f"{key} = '{value}'")

                else:
                    set_clauses.append(f"{key} = {value}")

        if not set_clauses:
            return False

        query = f"UPDATE rooms SET {', '.join(set_clauses)} WHERE roomId = {room_id}"
        result = self.db.execute_query(query)
        return result.success

    def _get_next_id(self) -> int:
        """Get next available room ID"""
        query = "SELECT roomId FROM rooms"
        result = self.db.execute_query(query)

        if result.success and result.data:
            max_id = max(row["roomId"] for row in result.data)
            return max_id + 1

        return 1
