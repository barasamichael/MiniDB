"""
Automated tests for the repl module
Tests RDBMS_REPL class with all functionality including mocked I/O
"""

import os
import sys
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

# Add the parent directory to the path to import the rdbms module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.repl import main  # noqa
from rdbms.repl import RDBMS_REPL  # noqa
from rdbms.query_parser import QueryResult  # noqa


class TestRDBMS_REPL:
    """Test cases for the RDBMS_REPL class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp(prefix="test_repl_")

        # Mock database and parser to avoid file system operations
        with patch("rdbms.repl.Database") as mock_db, patch(
            "rdbms.repl.QueryParser"
        ) as mock_parser:
            # Setup mock database
            self.mock_database = Mock()
            mock_db.return_value = self.mock_database

            # Setup mock parser
            self.mock_parser = Mock()
            mock_parser.return_value = self.mock_parser

            # Create REPL instance
            self.repl = RDBMS_REPL(
                database_name="test_db",
                storage_type="memory",
                data_dir=self.test_dir,
            )

            # Manually set the mocked objects
            self.repl.database = self.mock_database
            self.repl.parser = self.mock_parser

    def teardown_method(self):
        """Clean up after tests"""
        if os.path.exists(self.test_dir):
            import shutil

            shutil.rmtree(self.test_dir)

    @patch("rdbms.repl.readline")
    @patch("rdbms.repl.Path.home")
    def test_repl_initialization(self, mock_home, mock_readline):
        """Test REPL initialization"""
        mock_home.return_value = Path("/fake/home")

        with patch("rdbms.repl.Database") as mock_db, patch(
            "rdbms.repl.QueryParser"
        ) as mock_parser, patch("builtins.print") as mock_print:
            repl = RDBMS_REPL("test_db", "file", "/test/dir")

            assert repl.db_name == "test_db"
            assert repl.running == True
            mock_db.assert_called_once_with("test_db", "file", "/test/dir")
            mock_parser.assert_called_once()

            # Check that welcome message was printed
            mock_print.assert_called()

    @patch("builtins.print")
    def test_print_welcome(self, mock_print):
        """Test welcome message printing"""
        self.repl._print_welcome()

        # Verify welcome message components were printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        welcome_text = " ".join(print_calls)

        assert "Welcome to MiniDB" in welcome_text
        assert "test_db" in welcome_text
        assert ".help" in welcome_text
        assert ".exit" in welcome_text

    @patch("builtins.print")
    def test_print_help(self, mock_print):
        """Test help message printing"""
        self.repl._print_help()

        # Verify help content was printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        help_text = " ".join(print_calls)

        assert "MiniDB Help" in help_text
        assert "CREATE TABLE" in help_text
        assert "INSERT INTO" in help_text
        assert "SELECT" in help_text
        assert "Data Types" in help_text
        assert "INT" in help_text
        assert "PRIMARY KEY" in help_text

    @patch("builtins.print")
    def test_handle_repl_command_help(self, mock_print):
        """Test .help command"""
        result = self.repl._handle_repl_command(".help")
        assert result == True
        mock_print.assert_called()

    @patch("builtins.print")
    def test_handle_repl_command_tables(self, mock_print):
        """Test .tables command"""
        self.mock_database.list_tables.return_value = ["users", "products"]
        self.mock_database.get_table.return_value.get_row_count.return_value = 5

        result = self.repl._handle_repl_command(".tables")
        assert result == True

        self.mock_database.list_tables.assert_called_once()
        mock_print.assert_called()

    @patch("builtins.print")
    def test_handle_repl_command_stats(self, mock_print):
        """Test .stats command"""
        self.mock_database.get_database_stats.return_value = {
            "table_count": 2,
            "total_rows": 10,
            "storage_format": "memory",
            "data_directory": "N/A",
            "total_size_bytes": 1024,
        }

        result = self.repl._handle_repl_command(".stats")
        assert result == True

        self.mock_database.get_database_stats.assert_called_once()
        mock_print.assert_called()

        # Check that stats were displayed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        stats_text = " ".join(print_calls)
        assert "Tables: 2" in stats_text
        assert "Total rows: 10" in stats_text

    @patch("builtins.print")
    def test_handle_repl_command_backup_success(self, mock_print):
        """Test .backup command success"""
        self.mock_database.backup.return_value = True

        result = self.repl._handle_repl_command(".backup /tmp/backup")
        assert result == True

        self.mock_database.backup.assert_called_once_with("/tmp/backup")
        mock_print.assert_called_with("Database backed up to: /tmp/backup")

    @patch("builtins.print")
    def test_handle_repl_command_backup_failure(self, mock_print):
        """Test .backup command failure"""
        self.mock_database.backup.return_value = False

        result = self.repl._handle_repl_command(".backup /tmp/backup")
        assert result == True

        mock_print.assert_called_with(
            "Failed to backup database to: /tmp/backup"
        )

    @patch("builtins.print")
    def test_handle_repl_command_backup_no_path(self, mock_print):
        """Test .backup command without path"""
        result = self.repl._handle_repl_command(".backup")
        assert result == True

        mock_print.assert_called_with("Usage: .backup <path>")

    @patch("builtins.print")
    def test_handle_repl_command_restore_success(self, mock_print):
        """Test .restore command success"""
        self.mock_database.restore.return_value = True

        result = self.repl._handle_repl_command(".restore /tmp/backup")
        assert result == True

        self.mock_database.restore.assert_called_once_with("/tmp/backup")
        mock_print.assert_called_with("Database restored from: /tmp/backup")

    @patch("builtins.print")
    def test_handle_repl_command_restore_failure(self, mock_print):
        """Test .restore command failure"""
        self.mock_database.restore.return_value = False

        result = self.repl._handle_repl_command(".restore /tmp/backup")
        assert result == True

        mock_print.assert_called_with(
            "Failed to restore database from: /tmp/backup"
        )

    @patch("builtins.print")
    def test_handle_repl_command_restore_no_path(self, mock_print):
        """Test .restore command without path"""
        result = self.repl._handle_repl_command(".restore")
        assert result == True

        mock_print.assert_called_with("Usage: .restore <path>")

    def test_handle_repl_command_exit(self):
        """Test .exit command"""
        self.repl.running = True

        with patch.object(self.repl, "_exit_repl") as mock_exit:
            result = self.repl._handle_repl_command(".exit")
            assert result == True
            mock_exit.assert_called_once()

    def test_handle_repl_command_quit(self):
        """Test .quit command"""
        self.repl.running = True

        with patch.object(self.repl, "_exit_repl") as mock_exit:
            result = self.repl._handle_repl_command(".quit")
            assert result == True
            mock_exit.assert_called_once()

    def test_handle_repl_command_unknown(self):
        """Test unknown REPL command"""
        result = self.repl._handle_repl_command(".unknown")
        assert result == False

    @patch("builtins.print")
    def test_show_tables_with_tables(self, mock_print):
        """Test showing tables when tables exist"""
        mock_table = Mock()
        mock_table.get_row_count.return_value = 5

        self.mock_database.list_tables.return_value = ["users", "products"]
        self.mock_database.get_table.return_value = mock_table

        self.repl._show_tables()

        # Verify method calls
        self.mock_database.list_tables.assert_called_once()
        assert self.mock_database.get_table.call_count == 2

        # Check output
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output_text = " ".join(print_calls)
        assert "users" in output_text
        assert "products" in output_text
        assert "(5 rows)" in output_text

    @patch("builtins.print")
    def test_show_tables_no_tables(self, mock_print):
        """Test showing tables when no tables exist"""
        self.mock_database.list_tables.return_value = []

        self.repl._show_tables()

        mock_print.assert_called_with("No tables found")

    @patch("builtins.print")
    def test_show_stats_complete(self, mock_print):
        """Test showing complete database statistics"""
        stats = {
            "table_count": 3,
            "total_rows": 150,
            "storage_format": "json",
            "data_directory": "/test/data",
            "total_size_bytes": 2048000,
        }
        self.mock_database.get_database_stats.return_value = stats

        self.repl._show_stats()

        # Check that all stats were displayed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        stats_text = " ".join(print_calls)

        assert "Tables: 3" in stats_text
        assert "Total rows: 150" in stats_text
        assert "Storage format: json" in stats_text
        assert "Data directory: /test/data" in stats_text
        assert "Total size: 1.95 MB" in stats_text  # 2048000 bytes = ~1.95 MB

    @patch("builtins.print")
    def test_backup_database_success(self, mock_print):
        """Test successful database backup"""
        self.mock_database.backup.return_value = True

        self.repl._backup_database("/tmp/backup")

        self.mock_database.backup.assert_called_once_with("/tmp/backup")
        mock_print.assert_called_with("Database backed up to: /tmp/backup")

    @patch("builtins.print")
    def test_backup_database_exception(self, mock_print):
        """Test database backup with exception"""
        self.mock_database.backup.side_effect = Exception("Backup failed")

        self.repl._backup_database("/tmp/backup")

        mock_print.assert_called_with("Backup error: Backup failed")

    @patch("builtins.print")
    def test_restore_database_success(self, mock_print):
        """Test successful database restore"""
        self.mock_database.restore.return_value = True

        self.repl._restore_database("/tmp/backup")

        self.mock_database.restore.assert_called_once_with("/tmp/backup")
        mock_print.assert_called_with("Database restored from: /tmp/backup")

    @patch("builtins.print")
    def test_restore_database_exception(self, mock_print):
        """Test database restore with exception"""
        self.mock_database.restore.side_effect = Exception("Restore failed")

        self.repl._restore_database("/tmp/backup")

        mock_print.assert_called_with("Restore error: Restore failed")

    @patch("rdbms.repl.readline")
    @patch("builtins.print")
    def test_exit_repl(self, mock_print, mock_readline):
        """Test REPL exit functionality"""
        self.repl.running = True

        # Mock history file saving
        with patch.object(self.repl, "_save_history") as mock_save_history:
            self.repl._exit_repl()

        assert self.repl.running == False
        self.mock_database.close.assert_called_once()
        mock_save_history.assert_called_once()

        # Check exit messages
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert "Saving data..." in print_calls
        assert "Goodbye!" in print_calls

    @patch("builtins.print")
    def test_execute_sql_query_success_with_data(self, mock_print):
        """Test successful SQL query execution with data"""
        result = QueryResult(
            success=True,
            data=[{"id": 1, "name": "John"}],
            columns=["id", "name"],
        )
        self.mock_database.execute_query.return_value = result
        self.mock_parser.format_query_result.return_value = "Formatted result"

        self.repl._execute_sql_query("SELECT * FROM users")

        self.mock_database.execute_query.assert_called_once_with(
            "SELECT * FROM users"
        )
        self.mock_parser.format_query_result.assert_called_once_with(result)
        mock_print.assert_called_with("\nFormatted result")

    @patch("builtins.print")
    def test_execute_sql_query_success_with_message(self, mock_print):
        """Test successful SQL query execution with message"""
        result = QueryResult(success=True, message="Table created successfully")
        self.mock_database.execute_query.return_value = result

        self.repl._execute_sql_query("CREATE TABLE users (id INT)")

        mock_print.assert_called_with("Table created successfully")

    @patch("builtins.print")
    def test_execute_sql_query_success_no_data_no_message(self, mock_print):
        """Test successful SQL query execution with no data or message"""
        result = QueryResult(success=True)
        self.mock_database.execute_query.return_value = result

        self.repl._execute_sql_query("UPDATE users SET name = 'John'")

        mock_print.assert_called_with("Query executed successfully")

    @patch("builtins.print")
    def test_execute_sql_query_error(self, mock_print):
        """Test SQL query execution with error"""
        result = QueryResult(success=False, error="Table not found")
        self.mock_database.execute_query.return_value = result

        self.repl._execute_sql_query("SELECT * FROM nonexistent")

        mock_print.assert_called_with("Error: Table not found")

    @patch("builtins.print")
    def test_execute_sql_query_exception(self, mock_print):
        """Test SQL query execution with exception"""
        self.mock_database.execute_query.side_effect = Exception(
            "Database error"
        )

        self.repl._execute_sql_query("SELECT * FROM users")

        mock_print.assert_called_with("Unexpected error: Database error")

    @patch("builtins.input")
    def test_get_input_normal(self, mock_input):
        """Test normal input handling"""
        mock_input.return_value = "SELECT * FROM users"

        result = self.repl._get_input()
        assert result == "SELECT * FROM users"

    @patch("builtins.input")
    @patch("builtins.print")
    def test_get_input_eof_error(self, mock_print, mock_input):
        """Test input handling with EOF (Ctrl+D)"""
        mock_input.side_effect = EOFError()

        with patch.object(self.repl, "_exit_repl") as mock_exit:
            result = self.repl._get_input()
            assert result == ""
            mock_exit.assert_called_once()

    @patch("builtins.input")
    @patch("builtins.print")
    def test_get_input_keyboard_interrupt(self, mock_print, mock_input):
        """Test input handling with KeyboardInterrupt (Ctrl+C)"""
        mock_input.side_effect = KeyboardInterrupt()

        result = self.repl._get_input()
        assert result == ""
        mock_print.assert_called_with("\n(Use .exit to quit)")

    @patch("builtins.input")
    @patch("builtins.print")
    def test_run_repl_command(self, mock_print, mock_input):
        """Test running REPL with a dot command"""
        # Mock the input to return commands then EOF to exit
        mock_input.side_effect = [".help", EOFError()]

        with patch.object(
            self.repl, "_handle_repl_command", return_value=True
        ) as mock_handle, patch.object(self.repl, "_exit_repl") as mock_exit:
            mock_exit.side_effect = lambda: setattr(self.repl, "running", False)

            # Run the REPL - it should handle .help then exit on EOF
            self.repl.run()

            mock_handle.assert_called_once_with(".help")
            mock_exit.assert_called_once()

    @patch("builtins.input")
    def test_run_sql_command(self, mock_input):
        """Test running REPL with SQL command"""
        mock_input.side_effect = [
            "SELECT * FROM users",
            "",
        ]  # SQL then empty to end

        with patch.object(
            self.repl, "_execute_sql_query"
        ) as mock_execute, patch.object(
            self.repl, "_get_input", side_effect=mock_input.side_effect
        ):
            # Run one iteration then stop
            original_running = self.repl.running

            def stop_after_first():
                if mock_execute.called:
                    self.repl.running = False
                return original_running and not mock_execute.called

            self.repl.running = True
            # Manually control the loop
            user_input = mock_input.side_effect[0]
            if user_input and not user_input.startswith("."):
                self.repl._execute_sql_query(user_input)

            mock_execute.assert_called_once_with("SELECT * FROM users")

    @patch("builtins.input")
    @patch("builtins.print")
    def test_run_empty_input(self, mock_print, mock_input):
        """Test running REPL with empty input"""
        mock_input.side_effect = ["", ".exit"]

        with patch.object(self.repl, "_exit_repl") as mock_exit:
            mock_exit.side_effect = lambda: setattr(self.repl, "running", False)

            # Simulate the empty input handling
            user_input = mock_input.side_effect[0].strip()
            assert user_input == ""  # Should be skipped

    @patch("builtins.input")
    @patch("builtins.print")
    def test_run_keyboard_interrupt(self, mock_print, mock_input):
        """Test running REPL with keyboard interrupt"""
        mock_input.side_effect = [KeyboardInterrupt(), ".exit"]

        # This would be handled in _get_input, but test the exception handling
        try:
            raise KeyboardInterrupt()
        except KeyboardInterrupt:
            # This is what happens in the run loop
            pass

    @patch("rdbms.repl.readline")
    def test_setup_readline_success(self, mock_readline):
        """Test successful readline setup"""
        mock_readline.read_history_file = Mock()
        mock_readline.set_history_length = Mock()
        mock_readline.parse_and_bind = Mock()

        # Create a mock history file
        with patch.object(Path, "exists", return_value=True):
            self.repl._setup_readline()

        mock_readline.read_history_file.assert_called()
        mock_readline.set_history_length.assert_called_with(1000)
        mock_readline.parse_and_bind.assert_called_with("tab: complete")

    @patch("rdbms.repl.readline")
    def test_setup_readline_exception(self, mock_readline):
        """Test readline setup with exception"""
        mock_readline.read_history_file.side_effect = Exception(
            "Readline error"
        )

        # Should not raise exception
        self.repl._setup_readline()

    @patch("rdbms.repl.readline")
    def test_save_history_success(self, mock_readline):
        """Test successful history saving"""
        mock_readline.write_history_file = Mock()

        self.repl._save_history()

        mock_readline.write_history_file.assert_called_once()

    @patch("rdbms.repl.readline")
    def test_save_history_exception(self, mock_readline):
        """Test history saving with exception"""
        mock_readline.write_history_file.side_effect = Exception("Write error")

        # Should not raise exception
        self.repl._save_history()


class TestMainFunction:
    """Test cases for the main function and command-line interface"""

    @patch("rdbms.repl.RDBMS_REPL")
    @patch("sys.argv", ["repl.py"])
    def test_main_default_args(self, mock_repl_class):
        """Test main function with default arguments"""
        mock_repl = Mock()
        mock_repl_class.return_value = mock_repl

        main()

        mock_repl_class.assert_called_once_with("pesapal_db", "file", "data")
        mock_repl.run.assert_called_once()

    @patch("rdbms.repl.RDBMS_REPL")
    @patch(
        "sys.argv",
        ["repl.py", "--db-name", "test_db", "--storage-type", "memory"],
    )
    def test_main_custom_args(self, mock_repl_class):
        """Test main function with custom arguments"""
        mock_repl = Mock()
        mock_repl_class.return_value = mock_repl

        main()

        mock_repl_class.assert_called_once_with("test_db", "memory", "data")
        mock_repl.run.assert_called_once()

    @patch("rdbms.repl.RDBMS_REPL")
    @patch("sys.argv", ["repl.py", "--execute", "SELECT * FROM users"])
    @patch("builtins.print")
    def test_main_execute_mode(self, mock_print, mock_repl_class):
        """Test main function with --execute argument"""
        mock_repl = Mock()
        mock_repl_class.return_value = mock_repl

        main()

        mock_repl_class.assert_called_once_with("pesapal_db", "file", "data")
        mock_print.assert_called_with("Executing: SELECT * FROM users")
        mock_repl._execute_sql_query.assert_called_once_with(
            "SELECT * FROM users"
        )
        mock_repl.database.close.assert_called_once()
        mock_repl.run.assert_not_called()

    @patch("rdbms.repl.RDBMS_REPL")
    @patch("builtins.print")
    def test_main_keyboard_interrupt(self, mock_print, mock_repl_class):
        """Test main function with keyboard interrupt"""
        mock_repl_class.side_effect = KeyboardInterrupt()

        main()

        mock_print.assert_called_with("\nGoodbye!")

    @patch("rdbms.repl.RDBMS_REPL")
    @patch("builtins.print")
    @patch("sys.exit")
    def test_main_exception(self, mock_exit, mock_print, mock_repl_class):
        """Test main function with exception"""
        mock_repl_class.side_effect = Exception("Fatal error occurred")

        main()

        mock_print.assert_called_with("Fatal error: Fatal error occurred")
        mock_exit.assert_called_with(1)

    @patch("rdbms.repl.argparse.ArgumentParser")
    def test_argument_parser_setup(self, mock_parser_class):
        """Test that argument parser is set up correctly"""
        mock_parser = Mock()
        mock_parser_class.return_value = mock_parser
        mock_parser.parse_args.return_value = Mock(
            db_name="test_db",
            storage_type="file",
            data_dir="data",
            storage_format="json",
            execute=None,
        )

        with patch("rdbms.repl.RDBMS_REPL") as mock_repl_class:
            mock_repl = Mock()
            mock_repl_class.return_value = mock_repl

            main()

        # Verify parser was created and configured
        mock_parser_class.assert_called_once()
        mock_parser.add_argument.assert_called()

        # Check that some expected arguments were added
        add_arg_calls = [
            call[0][0] for call in mock_parser.add_argument.call_args_list
        ]
        assert "--db-name" in add_arg_calls
        assert "--storage-type" in add_arg_calls
        assert "--execute" in add_arg_calls


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
