"""
Interactive REPL (Real-Eval-Print-Loop) for MiniDB
Command-line interface for executing SQL queries
"""

import sys
import readline
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from database import Database
from query_parser import QueryParser


class RDBMS_REPL:
    """Interactive shell for the MiniDB"""

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
        print("Welcome to MiniDB")
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
        print("\nMiniDB Help")
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

        elif command.startswith(".backup"):
            parts = command.split(maxsplit=1)
            if len(parts) > 1:
                self._backup_database(parts[1])

            else:
                print("Usage: .backup <path>")

            return True

        elif command.startswith(".restore"):
            parts = command.split(maxsplit=1)
            if len(parts) > 1:
                self._restore_database(parts[1])

            else:
                print("Usage: .restore <path>")

            return True

        elif command in [".exit", ".quit"]:
            self._exit_repl()
            return True

        return False

    def _show_tables(self):
        """Show all tables"""
        tables = self.database.list_tables()
        if tables:
            print(f"\nTables in {self.db_name}:")
            print("-" * 30)
            for i, table_name in enumerate(tables, 1):
                table = self.database.get_table(table_name)
                row_count = table.get_row_count() if table else 0
                print(f"  {i}. {table_name} ({row_count} rows)")
            print("-" * 30)

        else:
            print("No tables found")

    def _show_stats(self):
        """Show database statistics"""
        stats = self.database.get_database_stats()
        print(f"\nDatabase Statistics: {self.db_name}")
        print("-" * 40)
        print(f"Tables: {stats.get('table_count', 0)}")
        print(f"Total rows: {stats.get('total_rows', 0)}")
        print(f"Storage format: {stats.get('storage_format', 'unknown')}")
        print(f"Data directory: {stats.get('data_directory', 'N/A')}")

        if "total_size_bytes" in stats:
            size_mb = stats["total_size_bytes"] / (1024 * 1024)
            print(f"Total size: {size_mb:.2f} MB")

        print("-" * 40)

    def _backup_database(self, backup_path: str):
        """Create database backup"""
        try:
            if self.database.backup(backup_path):
                print(f"Database backed up to: {backup_path}")

            else:
                print(f"Failed to backup database to: {backup_path}")

        except Exception as e:
            print(f"Backup error: {e}")

    def _restore_database(self, backup_path: str):
        """Restore database from backup"""
        try:
            if self.database.restore(backup_path):
                print(f"Database restored from: {backup_path}")

            else:
                print(f"Failed to restore database from: {backup_path}")

        except Exception as e:
            print(f"Restore error: {e}")

    def _exit_repl(self):
        print("Saving data...")
        self.database.close()
        self._save_history()
        print("Goodbye!")

        self.running = False

    def _execute_sql_query(self, query: str):
        """Execute SQL query and display results"""
        try:
            result = self.database.execute_query(query)

            if result.success:
                if result.data:
                    # Format and display results in a nice table
                    formatted_result = self.parser.format_query_result(result)
                    print(f"\n{formatted_result}")

                elif result.message:
                    print(f"{result.message}")

                else:
                    print("Query executed successfully")

            else:
                print(f"Error: {result.error}")

        except Exception as e:
            print(f"Unexpected error: {e}")

    def _get_input(self) -> str:
        """Get input from user with prompt"""
        try:
            return input("pesapal_rdbms> ")

        except EOFError:
            # Handle Ctrl+D
            self._exit_repl()
            return ""

        except KeyboardInterrupt:
            # Handle Ctrl+C
            print("\n(Use .exit to quit)")
            return ""

    def run(self):
        while self.running:
            try:
                # Get user input
                user_input = self._get_input().strip()

                # Skip empty input
                if not user_input:
                    continue

                # Handle REPL commands
                if user_input.startswith("."):
                    self._handle_repl_command(user_input)
                    continue

                # Handle SQL queries
                self._execute_sql_query(user_input)

            except KeyboardInterrupt:
                print("\n(Use .exit to quit)")
                continue

            except Exception as e:
                print(f"Unexpected error: {e}")
                continue


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="MiniDB - Interactive Database Shell"
    )

    parser.add_argument(
        "--db-name",
        default="pesapal_db",
        help="Database name (default: pesapal_db)",
    )

    parser.add_argument(
        "--storage-type",
        choices=["file", "memory"],
        default="file",
        help="Storage type: file or memory (default: file)",
    )

    parser.add_argument(
        "--data-dir",
        default="data",
        help="Data directory for file storage (default: data)",
    )

    parser.add_argument(
        "--storage-format",
        choices=["json", "pickle"],
        default="json",
        help="Storage format for file storage (default: json)",
    )

    parser.add_argument(
        "--execute", "-e", help="Execute a single SQL query and exit"
    )

    args = parser.parse_args()

    try:
        # Initialize REPL
        repl = RDBMS_REPL(args.db_name, args.storage_type, args.data_dir)

        # If --execute is provided, run single query and exit
        if args.execute:
            print(f"Executing: {args.execute}")
            repl._execute_sql_query(args.execute)
            repl.database.close()
            return

        # Start interactive REPL
        repl.run()

    except KeyboardInterrupt:
        print("\nGoodbye!")

    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
