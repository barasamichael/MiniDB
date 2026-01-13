"""
Interactive REPL (Real-Eval-Print-Loop) for Pesapal RDBMS
Command-line interface for executing SQL queries
"""

import sys
import os
import readline
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from database import Database
from query_parser import QueryParser


class RDBMS_REPL:
    """Interactive shell for the RDBMS"""

    def __init__(
        self,
        database_name: str = "pesapal_db",
        storage_type: str = "file",
        data_dir: str = "data",
    ):
        """
        Initialize REPL

        Args:
            database_name: Database name
            storage_type: 'file' or 'memory'
            data_dir: Directory for database files
        """
        self.db_name = database_name
        self.database = Database(database_name, storage_type, data_dir)
        self.parser = QueryParser()
        self.running = True

        # Setup readline for command history
        self.history_file = Path.home() / ".pesapal_rdbms_history"
        self._setup_readline()

        # Welcome message
        self._print_welcome()

    def _setup_readline(self):
        """Setup readline for command history and completion"""
        try:
            # Load command history
            if self.history_file.exists():
                readline.read_history_file(str(self.history_file))

            # Set history length
            readline.set_history_length(1000)

            # Enable tab completion
            readline.parse_and_bind("tab: complete")

        except Exception:
            # readline might not be available for all systems
            pass

    def _save_history(self):
        """Save command history"""
        try:
            readline.write_history_file(str(self.history_file))

        except Exception:
            pass

    def _print_welcome(self):
        """Print welcome message"""
        print("=" * 60)
        print("Welcome to Pesapal RDBMS")
        print("=" * 60)
        print(f"Database: {self.db_name}")
        print(f"Storage: {self.database.storage.__class__.__name__}")
        print("\nType your SQL commands below.")
        print("Commands:")
        print("  .help     - Show help")
        print("  .tables   - Show all tables")
        print("  .stats    - Show database statistics")
        print("  .backup   - Create database backup")
        print("  .restore  - Restore from backup")
        print("  .exit     - Exit REPL")
        print("=" * 60)
    
    def _print_help(self):
        """Print help message"""
        print("\nPesapal RDBMS Help")
        print("-" * 40)
        print("\nSupported SQL Commands:")
        print("  CREATE TABLE name (col1 type constraints, col2 type, ...)")
        print("  INSERT INTO table (cols) VALUES (vals), (vals2), ...")
        print("  SELECT * FROM table WHERE condition")
        print("  SELECT col1, col2 FROM table1 JOIN table2 ON condition")
        print("  UPDATE table SET col=value WHERE condition")
        print("  DELETE FROM table WHERE condition")
        print("  DROP TABLE table")
        print("  SHOW TABLES")
        print("  DESCRIBE table")
        
        print("\nData Types:")
        print("  INT, TEXT, VARCHAR, REAL, BOOLEAN")
        
        print("\nConstraints:")
        print("  PRIMARY KEY, UNIQUE, NOT NULL")
        
        print("\nREPL Commands:")
        print("  .help     - Show this help")
        print("  .tables   - List all tables")
        print("  .stats    - Database statistics")
        print("  .backup <path>   - Backup database")
        print("  .restore <path>  - Restore database")
        print("  .exit     - Exit REPL")
        
        print("\nExamples:")
        print("  CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);")
        print("  INSERT INTO users VALUES (1, 'John'), (2, 'Jane');")
        print("  SELECT * FROM users WHERE id = 1;")
        print("  UPDATE users SET name = 'Johnny' WHERE id = 1;")
        print("-" * 40)

    def _handle_repl_command(self, command: str) -> bool:
        """
        Handle REPL-specific commands (starting with .)

        Returns:
            True if command was handled, False otherwise
        """
        command = command.strip().lower()

        if command == ".help":
            self._print_help()
            return True

        elif command == ".tables":
            self._print_tables()
            return True

        elif command == ".stats":
            self._show_stats()
            return True

        elif command.startswith('.backup'):
            parts = command.split(maxsplit=1)
            if len(parts) > 1:
                self._backup_database(parts[1])

            else:
                print("Usage: .backup <path>")

            return True

        elif command.startswith('.restore'):
            parts = command.split(maxsplit=1)
            if len(parts) > 1:
                self._restore_database(parts[1])

            else:
                print("Usage: .restore <path>")

            return True

        elif command in ['.exit', '.quit']:
            self._exit_repl()
            return True

        return False

    def _show_tables(self):
        pass

    def _show_stats(self):
        pass

    def _backup_database(self, backup_path: str):
        pass

    def _restore_database(self, backup_path: str):
        pass

    def _exit_repl(self):
        pass

    def _execute_sql_query(self, query: str):
        pass

    def _get_input(self) -> str:
        pass

    def run(self):
        pass

def main():
    pass

if __name__ == "__main__":
    main()
