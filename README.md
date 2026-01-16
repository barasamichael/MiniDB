# MiniDB Resort Booking System

## Introduction

MiniDB Resort Booking System is a comprehensive web application for managing hotel and resort bookings, built using a custom-developed relational database management system (RDBMS). This project demonstrates the implementation of a complete booking workflow with payment processing through PesaPal integration, all powered by a custom database engine built from scratch.

The application serves as both a functional resort booking platform and a showcase of custom database technology, featuring SQL-like query processing, transaction management, and persistent data storage without relying on external database systems.

## Table of Contents

- [Features](#features)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Data Structures](#data-structures)
- [Custom RDBMS Architecture](#custom-rdbms-architecture)
- [Database Models](#database-models)
- [View Functions and API Endpoints](#view-functions-and-api-endpoints)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Features

### Core Functionality
- **Room Management**: Pre-seeded room inventory with different types (Standard, Deluxe, Suite)
- **Customer Registration**: Automatic customer creation and management
- **Booking System**: Complete reservation workflow with date validation
- **Payment Processing**: Integrated PesaPal payment gateway with full transaction tracking
- **Admin Dashboard**: Comprehensive booking and payment management interface
- **Booking Status Inquiry**: Customer self-service booking verification system

### Technical Features
- **Custom RDBMS**: Built-in relational database management system
- **SQL Parser**: SQL-like query language with support for complex operations
- **REPL Interface**: Interactive command-line database shell
- **Data Persistence**: JSON and Pickle-based storage engine
- **Multi-line Query Support**: Advanced query input with completion detection
- **Transaction Safety**: ACID compliance for critical operations

## Technology Stack

### Backend
- **Python 3.8+**: Core application language
- **Flask**: Web framework for HTTP handling and routing
- **Custom RDBMS**: Proprietary database management system

### Frontend
- **HTML5/CSS3**: User interface markup and styling
- **JavaScript**: Client-side interactivity and AJAX requests

### Payment Integration
- **PesaPal API**: Payment processing and transaction management

### Development Tools
- **pytest**: Testing framework for unit and integration tests
- **Flask-Mail**: Email notification system

## Project Structure

```
MiniDB/
├── README.md                          # Project documentation
├── app.py                             # Main Flask application
├── requirements.txt                   # Python dependencies
├── models/                            # Database model classes
│   ├── __init__.py
│   ├── customer.py                    # Customer data model
│   ├── room.py                        # Room inventory model
│   ├── booking.py                     # Booking management model
│   ├── payment.py                     # Payment records model
│   └── pesapal_interim_payment.py     # Payment processing model
├── rdbms/                             # Custom RDBMS implementation
│   ├── __init__.py
│   ├── database.py                    # Main database engine
│   ├── query_parser.py                # SQL query parser and executor
│   ├── storage.py                     # Data persistence layer
│   ├── table.py                       # Table and column management
│   └── repl.py                        # Interactive database shell
├── utilities/                         # Helper functions and integrations
│   ├── authentication.py              # Session management utilities
│   ├── email_utils.py                 # Email notification system
│   ├── logging.py                     # Application logging configuration
│   ├── pesapal_payment.py             # PesaPal payment integration
│   └── securities.py                  # Input validation and security
├── tests/                             # Test suite
│   ├── test_database.py               # Database engine tests
│   ├── test_query_parser.py           # SQL parser tests
│   ├── test_storage.py                # Storage layer tests
│   ├── test_table.py                  # Table management tests
│   └── test_repl.py                   # REPL interface tests
├── templates/                         # HTML templates (To Be Created)
│   ├── base.html                      # Base template layout
│   ├── index.html                     # Homepage template
│   ├── booking.html                   # Booking form template
│   ├── room_details.html              # Room information template
│   ├── payment_iframe.html            # Payment processing template
│   ├── admin_dashboard.html           # Administrative interface
│   └── error/                         # Error page templates
└── static/                            # Static assets (To Be Created)
    ├── css/                           # Stylesheet files
    ├── js/                            # JavaScript files
    └── images/                        # Image assets
```

## Data Structures

The application utilizes five core data entities to manage the complete booking workflow:

### Customer Entity
```python
{
    'customerId': int,          # Primary key
    'fullName': str,            # Customer full name
    'email': str,               # Unique email address
    'phoneNumber': str,         # Contact number
    'dateCreated': str          # Registration timestamp
}
```

### Room Entity
```python
{
    'roomId': int,              # Primary key
    'roomNumber': str,          # Unique room identifier
    'roomType': str,            # Room category (Standard/Deluxe/Suite)
    'capacity': int,            # Maximum occupancy
    'pricePerNight': float,     # Nightly rate
    'description': str,         # Room description
    'amenities': str,           # Pipe-delimited amenities list
    'features': str,            # Pipe-delimited features list
    'imageUrl': str,            # Room image reference
    'isAvailable': bool,        # Availability status
    'dateCreated': str          # Creation timestamp
}
```

### Booking Entity
```python
{
    'bookingId': int,           # Primary key
    'customerId': int,          # Foreign key to Customer
    'username': str,            # Guest name
    'emailAddress': str,        # Contact email
    'phoneNumber': str,         # Contact number
    'checkInDate': str,         # Arrival date (YYYY-MM-DD)
    'checkOutDate': str,        # Departure date (YYYY-MM-DD)
    'adultsCount': int,         # Number of adult guests
    'childrenCount': int,       # Number of children
    'specialRequests': str,     # Additional requirements
    'status': str,              # Booking state (pending/confirmed/cancelled)
    'dateCreated': str,         # Creation timestamp
    'lastUpdated': str          # Last modification timestamp
}
```

### Payment Entity
```python
{
    'paymentId': int,           # Primary key
    'bookingId': int,           # Foreign key to Booking
    'receiptNumber': str,       # Unique transaction identifier
    'amount': float,            # Payment amount
    'paymentMethod': str,       # Payment provider and method
    'status': str,              # Payment status (successful/failed)
    'processedAt': str          # Processing timestamp
}
```

### PesaPal Interim Payment Entity
```python
{
    'pesapalInterimPaymentId': int,  # Primary key
    'bookingId': int,                # Foreign key to Booking
    'amount': float,                 # Transaction amount
    'status': str,                   # Processing status (SAVED/COMPLETED/FAILED)
    'iframeSrc': str,                # PesaPal payment URL
    'orderTrackingId': str,          # PesaPal transaction identifier
    'merchantReference': str,        # Merchant reference number
    'dateCreated': str,              # Creation timestamp
    'lastUpdated': str               # Last update timestamp
}
```

## Custom RDBMS Architecture

### Core Components

#### Database Engine (`database.py`)
The central orchestrator that manages all database operations and coordinates between different subsystems:

- **Query Execution**: Processes SQL-like commands through the query parser
- **Transaction Management**: Ensures data integrity during complex operations
- **Storage Coordination**: Manages data persistence through the storage layer
- **Index Management**: Maintains indexes for primary keys and unique constraints
- **Table Operations**: Handles CREATE, ALTER, and DROP table operations

#### Query Parser (`query_parser.py`)
Implements a comprehensive SQL-like query language with support for:

- **DDL Operations**: CREATE TABLE, DROP TABLE, DESCRIBE
- **DML Operations**: INSERT, SELECT, UPDATE, DELETE
- **Query Optimization**: WHERE clause parsing with multiple operators (=, !=, <, >, <=, >=)
- **Join Operations**: INNER JOIN with ON conditions
- **Logical Operators**: AND, OR logical combinations
- **Advanced Features**: ORDER BY, LIMIT, aggregate functions
- **Data Type Support**: INT, TEXT, VARCHAR, REAL, BOOLEAN with automatic type conversion

#### Storage Engine (`storage.py`)
Provides persistent data storage with multiple backend options:

- **File-Based Storage**: JSON and Pickle serialization formats
- **Memory Storage**: In-memory database for testing and development
- **Backup and Recovery**: Database backup and restoration capabilities
- **Metadata Management**: Table schema and index information persistence
- **Thread Safety**: Concurrent access protection with locking mechanisms

#### Table Management (`table.py`)
Handles individual table operations and schema management:

- **Schema Definition**: Column types, constraints, and validation rules
- **Data Validation**: Type checking, NOT NULL, and UNIQUE constraint enforcement
- **Index Maintenance**: Automatic indexing for primary keys and unique columns
- **CRUD Operations**: Create, Read, Update, Delete with constraint validation
- **Relationship Integrity**: Foreign key-like behavior through application logic

#### REPL Interface (`repl.py`)
Interactive command-line interface for database administration:

- **Multi-line Query Support**: Complex query composition across multiple lines
- **Command History**: Persistent command history with readline integration
- **Tab Completion**: Context-aware command completion
- **Database Statistics**: Real-time database performance and storage metrics
- **Administrative Commands**: Backup, restore, and maintenance operations

### Advanced Features

#### Constraint System
- **Primary Key Enforcement**: Automatic uniqueness validation
- **Unique Constraints**: Multi-column unique index support
- **NOT NULL Validation**: Null value prevention with type-specific defaults
- **Data Type Validation**: Automatic type conversion and validation

#### Index Management
- **Automatic Indexing**: Primary key and unique constraint indexes
- **Query Optimization**: Index-based lookup optimization for WHERE clauses
- **Index Maintenance**: Automatic index updates during INSERT, UPDATE, DELETE

#### Transaction Safety
- **Atomic Operations**: All-or-nothing transaction semantics
- **Consistency Checking**: Constraint validation before commit
- **Isolation**: Concurrent access protection
- **Durability**: Persistent storage with backup capabilities

## Database Models

### Object-Relational Mapping (ORM)
Each model class provides an object-oriented interface to the underlying RDBMS:

#### Customer Model
Manages guest information and contact details:
```python
customer_model = Customer(db)
customer = customer_model.create({
    'fullName': 'Barasa Michael Murunga',
    'email': 'michael.barasa@strathmore.edu',
    'phoneNumber': '+254 114 742 348'
})
```

#### Room Model
Handles room inventory with automatic seeding:
```python
room_model = Room(db)
available_rooms = room_model.get_available_rooms()
deluxe_rooms = room_model.get_by_type('Deluxe')
```

#### Booking Model
Manages the complete reservation lifecycle:
```python
booking_model = Booking(db)
booking = booking_model.create({
    'customerId': customer_id,
    'checkInDate': '2026-02-14',
    'checkOutDate': '2026-02-15',
    'adultsCount': 2,
    'status': 'pending'
})
```

#### Payment Model
Tracks completed financial transactions:
```python
payment_model = Payment(db)
payment = payment_model.create({
    'bookingId': booking_id,
    'receiptNumber': 'RCP000002',
    'amount': 25000.00,
    'paymentMethod': 'PesaPal - MPESA KE'
})
```

#### PesaPal Interim Payment Model
Manages payment processing workflow:
```python
interim_model = PesapalInterimPayment(db)
interim_payment = interim_model.create({
    'bookingId': booking_id,
    'orderTrackingId': tracking_id,
    'iframeSrc': payment_url,
    'status': 'SAVED'
})
```

## View Functions and API Endpoints

### Public Routes

#### Homepage and Room Display
- **GET /**: Homepage with room type showcase
- **GET /rooms/<room_type>**: Detailed room information
- **GET /api/check-availability**: Real-time room availability checking

#### Booking Management
- **GET /booking**: Booking form interface
- **POST /payment/pesapal/iframe**: Booking submission and payment initialization
- **GET /payment/pesapal/redirect/booking_payment**: Payment completion handling
- **GET /payment/pesapal/ipn/booking-payment**: PesaPal webhook processing

#### Customer Self-Service
- **GET /booking-status**: Booking inquiry form
- **POST /api/booking-status**: Booking status verification

### Administrative Routes

#### Authentication
- **GET /admin/login**: Administrative login interface
- **POST /admin/login**: Credential verification
- **GET /admin/logout**: Session termination

#### Dashboard Operations
- **GET /admin/dashboard**: Comprehensive booking and payment overview
- **GET /admin/filter-bookings**: Advanced booking filtering and search

#### Booking Administration
- **POST /admin/bookings/<id>/cancel**: Booking cancellation
- **POST /admin/bookings/<id>/delete**: Booking removal (pending/cancelled only)

### Supported Functionality

#### Customer-Facing Features
1. **Room Browsing**: Interactive room catalog with filtering
2. **Availability Checking**: Real-time date-based availability
3. **Booking Creation**: Comprehensive reservation form
4. **Payment Processing**: Secure PesaPal integration
5. **Booking Inquiry**: Self-service status checking

#### Administrative Features
1. **Booking Management**: View, filter, cancel, and delete reservations
2. **Payment Tracking**: Complete transaction history and status monitoring
3. **Customer Management**: Guest information and booking history
4. **Room Administration**: Inventory status and availability monitoring
5. **Reporting**: Date-range filtering and status-based reporting

#### Technical Features
1. **Database Operations**: Full CRUD operations through custom RDBMS
2. **Query Processing**: Complex SQL-like queries with joins and filtering
3. **Data Validation**: Comprehensive input validation and sanitization
4. **Error Handling**: Graceful error management with user-friendly messages
5. **Email Notifications**: Automated booking confirmations and updates

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager
- Virtual environment (recommended)

### Setup Instructions

1. **Clone the repository**:
```bash
git clone https://github.com/barasamichael/MiniDB.git
cd MiniDB
```

2. **Create and activate virtual environment**:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Initialize the database**:
```bash
python -c "from app import db; print('Database initialized')"
```

## Configuration

### Application Settings
Update the following configuration values in `app.py`:

```python
# Security Configuration
app.config['SECRET_KEY'] = 'your-secure-secret-key-here'

# PesaPal Integration
app.config['PESAPAL_CONSUMER_KEY'] = 'your-pesapal-consumer-key'
app.config['PESAPAL_CONSUMER_SECRET'] = 'your-pesapal-consumer-secret'
app.config['PESAPAL_BASE_URL'] = 'https://cybqa.pesapal.com/pesapalv3'

# Administrative Access
ADMIN_USERNAME = "your-admin-username"
ADMIN_PASSWORD = "your-secure-admin-password"
```

### PesaPal Configuration
1. Register for a PesaPal merchant account
2. Obtain API credentials from the PesaPal dashboard
3. Configure IPN (Instant Payment Notification) URLs
4. Set up callback URLs for payment completion

## Usage

### Starting the Application
```bash
python app.py
```

The application will be available at `http://localhost:5000`

### Database Administration
Access the interactive database shell:
```bash
python -m rdbms.repl
```

Available REPL commands:
- `.help`: Display command reference
- `.tables`: List all tables
- `.stats`: Show database statistics
- `.backup <path>`: Create database backup
- `.clear`: Clear terminal screen
- `.history`: Show command history

### Sample Database Operations
```sql
-- View all bookings
SELECT * FROM bookings WHERE status = 'confirmed';

-- Check room availability
SELECT * FROM rooms WHERE isAvailable = true;

-- Find customer by email
SELECT * FROM customers WHERE email = 'michael.barasa@strathmore.edu';

-- Join bookings with customer information
SELECT customers.fullName, bookings.checkInDate, bookings.status 
FROM customers JOIN bookings ON customers.customerId = bookings.customerId;
```

## API Documentation

### Booking Status API
**Endpoint**: `POST /api/booking-status`

**Request Body**:
```json
{
    "receiptNumber": "RCP000002",
    "email": "michael.barasa@strathmore.edu"
}
```

**Response**:
```json
{
    "success": true,
    "booking": {
        "bookingId": 1,
        "guestName": "Michael Murunga Barasa",
        "checkInDate": "2026-02-14",
        "checkOutDate": "2026-02-15",
        "status": "confirmed"
    },
    "payment": {
        "receiptNumber": "RCP000002",
        "amount": 25000.00,
        "status": "successful"
    }
}
```

### Availability Check API
**Endpoint**: `GET /api/check-availability?checkIn=2026-02-03&checkOut=2026-02-04`

**Response**:
```json
{
    "success": true,
    "room_types": [
        {
            "name": "Deluxe",
            "available_count": 2,
            "price": 12500.00,
            "capacity": 3
        }
    ]
}
```

## Testing

### Running Tests
Execute the complete test suite:
```bash
pytest tests/ -v
```

### Test Categories
- **Unit Tests**: Individual component testing
- **Integration Tests**: Cross-component functionality
- **Database Tests**: RDBMS operation validation
- **API Tests**: Endpoint behavior verification

### Coverage Report
```bash
pytest --cov=. --cov-report=html tests/
```

## Contributing

### Development Guidelines
1. Follow PEP 8 coding standards
2. Write comprehensive test coverage for new features
3. Update documentation for API changes
4. Use meaningful commit messages

### Submission Process
1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Submit a pull request with detailed description

## License

This project is licensed under the MIT License. See the LICENSE file for details.

---

**Note**: This project is designed for educational and demonstration purposes, showcasing custom database implementation and web application development. For production use, consider additional security hardening and performance optimization.
