"""
Interactive REPL (Real-Eval-Print-Loop) for MiniDB
Command-line interface for executing SQL queries
"""

import sys
import os
import readline
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from database import Database  # noqa
from query_parser import QueryParser  # noqa


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

        # Multi-line input support
        self.multi_line_buffer = []
        self.in_multi_line = False

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

    def _clear_screen(self):
        """Clear the terminal screen"""
        # Clear screen for both Windows and Unix-like systems
        os.system("cls" if os.name == "nt" else "clear")

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
        print("  .history  - Show command history")
        print("  .clear    - Clear screen")
        print("  .exit     - Exit REPL")
        print(
            "\nMulti-line mode: End commands with semicolon (;) or type 'GO' on new line"
        )
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
        print("  .history  - Show command history")
        print("  .clear    - Clear screen")
        print("  .exit     - Exit REPL")

        print("\nMulti-line Input:")
        print("  Commands can span multiple lines")
        print("  End with semicolon (;) or type 'GO' on new line")
        print("  Use Ctrl+C to cancel multi-line input")

        print("\nExamples:")
        print("  CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);")
        print("  INSERT INTO users VALUES (1, 'John'), (2, 'Jane');")
        print("  SELECT * FROM users WHERE id = 1;")
        print("  UPDATE users SET name = 'Johnny' WHERE id = 1;")
        print("-" * 40)

    def _show_history(self):
        """Show command history"""
        try:
            history_length = readline.get_current_history_length()
            if history_length == 0:
                print("No command history available.")
                return

            print(
                f"\nCommand History (last {min(20, history_length)} commands):"
            )
            print("-" * 50)

            # Show last 20 commands
            start_idx = max(1, history_length - 19)
            for i in range(start_idx, history_length + 1):
                try:
                    cmd = readline.get_history_item(i)
                    # Don't show .history commands
                    if cmd and not cmd.startswith(".history"):
                        print(f"  {i:3d}: {cmd}")
                except:
                    continue

            print("-" * 50)

        except Exception as e:
            print(f"Error retrieving history: {e}")

    def _is_command_complete(self, command: str) -> bool:
        """Check if a command is complete (ends with semicolon or is a single-line command)"""
        command = command.strip()

        # Empty command is not complete
        if not command:
            return False

        # REPL commands are always complete
        if command.startswith("."):
            return True

        # Check for semicolon at the end
        if command.endswith(";"):
            return True

        # Check for 'GO' keyword (SQL Server style)
        lines = command.split("\n")
        if len(lines) > 1 and lines[-1].strip().upper() == "GO":
            return True

        # Some single-line commands that don't need semicolons
        single_line_commands = ["SHOW TABLES", "GO"]
        if command.upper() in single_line_commands:
            return True

        return False

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
            self._show_tables()
            return True

        elif command == ".stats":
            self._show_stats()
            return True

        elif command == ".history":
            self._show_history()
            return True

        elif command == ".clear":
            self._clear_screen()
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
            # Clean up the query (remove GO keyword and extra whitespace)
            query = query.strip()
            if query.upper().endswith("GO"):
                query = query[:-2].strip()

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

    def _get_input(self, prompt: str = "pesapal_rdbms> ") -> str:
        """Get input from user with prompt"""
        try:
            return input(prompt)
        except EOFError:
            # Handle Ctrl+D
            self._exit_repl()
            return ""
        except KeyboardInterrupt:
            # Handle Ctrl+C
            if self.in_multi_line:
                print("\n(Multi-line input cancelled)")
                self.multi_line_buffer = []
                self.in_multi_line = False
                return ""
            else:
                print("\n(Use .exit to quit)")
                return ""

    def _handle_multi_line_input(self, line: str) -> str:
        """Handle multi-line input and return complete command when ready"""
        # If we're not in multi-line mode, check if we should enter it
        if not self.in_multi_line:
            if self._is_command_complete(line):
                return line  # Single line command, return as-is
            else:
                # Enter multi-line mode
                self.in_multi_line = True
                self.multi_line_buffer = [line]
                return ""  # Not ready yet

        # We're in multi-line mode
        if line.strip().upper() == "GO" or line.strip() == "":
            # Check if current buffer forms a complete command
            current_command = "\n".join(self.multi_line_buffer)
            # Add semicolon for completion check
            if self._is_command_complete(current_command + ";"):
                complete_command = current_command
                self.multi_line_buffer = []
                self.in_multi_line = False
                return complete_command

        # Add line to buffer
        self.multi_line_buffer.append(line)

        # Check if the complete buffer is now a finished command
        current_command = "\n".join(self.multi_line_buffer)
        if self._is_command_complete(current_command):
            complete_command = current_command
            self.multi_line_buffer = []
            self.in_multi_line = False
            return complete_command

        return ""  # Not ready yet

    def run(self):
        """Main REPL loop"""
        while self.running:
            try:
                # Determine the prompt based on multi-line state
                if self.in_multi_line:
                    prompt = "        ...> "  # Continuation prompt
                else:
                    prompt = "pesapal_rdbms> "

                # Get user input
                user_input = self._get_input(prompt)

                # Handle multi-line input
                complete_command = self._handle_multi_line_input(user_input)

                # If we don't have a complete command yet, continue
                if not complete_command:
                    continue

                # Skip empty commands
                if not complete_command.strip():
                    continue

                # Handle REPL commands
                if complete_command.startswith("."):
                    self._handle_repl_command(complete_command)
                    continue

                # Handle SQL queries
                self._execute_sql_query(complete_command)

            except KeyboardInterrupt:
                if self.in_multi_line:
                    print("\n(Multi-line input cancelled)")
                    self.multi_line_buffer = []
                    self.in_multi_line = False
                else:
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
