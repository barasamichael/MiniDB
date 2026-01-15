"""
Main database engine that orchestrates tables, storage and queries
"""
from table import Table
from table import Column
from typing import Dict
from typing import List
from typing import Optional
from storage import StorageEngine
from storage import MemoryStorage
from query_parser import QueryParser
from query_parser import QueryResult


class Database:
    """Main database engine"""

    def __init__(
        self,
        name: str = "pesapal_db",
        storage_type: str = "file",
        data_dir: str = "data",
        storage_format: str = "json",
    ):
        """
        Initialize database

        Args:
            name: Database name
            storage_type: 'file' or 'memory'
            data_dir: Directory for file storage
            storage_format: 'json' or 'pickle' for file storage
        """
        self.name = name
        self.tables: Dict[str, Table] = {}
        self.query_parser = QueryParser()

        # Initialize storage
        if storage_type == "memory":
            self.storage = MemoryStorage()

        else:
            self.storage = StorageEngine(data_dir, storage_format)

        # Load existing tables from storage
        self._load_tables_from_storage()

    def _load_tables_from_storage(self):
        """Load all tables from storage into memory"""
        for table_name in self.storage.list_tables():
            table_data = self.storage.load_table(table_name)
            if table_data:
                try:
                    table = Table.from_dict(table_data)
                    self.tables[table_name] = table

                except Exception as e:
                    print(f"Warning: Could not load table '{table_name}': {e}")

    def _save_table_to_storage(self, table_name: str) -> bool:
        """Save a table to persistent storage"""
        if table_name in self.tables:
            return self.storage.save_table(
                table_name, self.tables[table_name].to_dict()
            )
        return False

    def execute_query(self, query: str) -> QueryResult:
        """
        Execute a SQL query

        Args:
            query: SQL query string

        Returns:
            QueryResult object with results and metadata
        """
        try:
            parsed_query = self.query_parser.parse(query.strip())

            if parsed_query.query_type == "CREATE_TABLE":
                return self._execute_create_table(parsed_query)

            elif parsed_query.query_type == "INSERT":
                return self._execute_insert(parsed_query)

            elif parsed_query.query_type == "SELECT":
                return self._execute_select(parsed_query)

            elif parsed_query.query_type == "UPDATE":
                return self._execute_update(parsed_query)

            elif parsed_query.query_type == "DELETE":
                return self._execute_delete(parsed_query)

            elif parsed_query.query_type == "DROP_TABLE":
                return self._execute_drop_table(parsed_query)

            elif parsed_query.query_type == "SHOW_TABLES":
                return self._execute_show_tables()

            elif parsed_query.query_type == "DESCRIBE":
                return self._execute_describe(parsed_query)

            else:
                return QueryResult(
                    success=False,
                    error=f"Unsupported query type: {parsed_query.query_type}",
                )

        except Exception as e:
            return QueryResult(success=False, error=str(e))

    def _execute_create_table(self, parsed_query) -> QueryResult:
        """Execute CREATE TABLE query"""
        table_name = parsed_query.table_name

        if table_name in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' already exists"
            )

        try:
            # Create columns from parsed data
            columns = []
            for col_def in parsed_query.columns:
                column = Column(
                    name=col_def["name"],
                    data_type=col_def["type"],
                    primary_key=col_def.get("primary_key", False),
                    unique=col_def.get("unique", False),
                    nullable=col_def.get("nullable", True),
                )
                columns.append(column)

            # Create table
            table = Table(table_name, columns)
            self.tables[table_name] = table

            # Save to storage
            if self._save_table_to_storage(table_name):
                return QueryResult(
                    success=True,
                    message=f"Table '{table_name}' created successfully",
                )

            else:
                return QueryResult(
                    success=False,
                    error=f"Failed to save table '{table_name}' to storage",
                )

        except Exception as e:
            return QueryResult(
                success=False, error=f"Error creating table: {str(e)}"
            )

    def _execute_insert(self, parsed_query) -> QueryResult:
        """Execute INSERT query"""
        table_name = parsed_query.table_name

        if table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' does not exist"
            )

        try:
            table = self.tables[table_name]

            # Insert each row
            rows_inserted = 0
            for row_data in parsed_query.values:
                if "__values_only__" in row_data:
                    values_list = row_data["__values_only__"]
                    mapped_row = dict(zip(table.column_order, values_list))
                    table.insert(mapped_row)  # Pass properly mapped row
                else:
                    table.insert(row_data)  # Already has column names
                rows_inserted += 1

            # Save to storage
            self._save_table_to_storage(table_name)

            return QueryResult(
                success=True,
                rows_affected=rows_inserted,
                message=f"Inserted {rows_inserted} row(s) into '{table_name}'",
            )

        except Exception as e:
            return QueryResult(success=False, error=f"Insert failed: {str(e)}")

    def _execute_select(self, parsed_query) -> QueryResult:
        """Execute SELECT query"""
        table_name = parsed_query.table_name

        if table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' does not exist"
            )

        try:
            table = self.tables[table_name]

            # Handle JOIN queries
            if parsed_query.join_table:
                return self._execute_join_select(parsed_query)

            # Regular SELECT
            results = table.select(
                columns=parsed_query.columns,
                where_clause=parsed_query.where_clause,
            )

            return QueryResult(
                success=True,
                data=results,
                columns=parsed_query.columns
                if parsed_query.columns
                else table.column_order,
                rows_affected=len(results),
            )

        except Exception as e:
            return QueryResult(success=False, error=f"Select failed: {str(e)}")

    def _execute_join_select(self, parsed_query) -> QueryResult:
        """Execute SELECT query with JOIN"""
        left_table_name = parsed_query.table_name
        right_table_name = parsed_query.join_table

        if left_table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{left_table_name}' does not exist"
            )

        if right_table_name not in self.tables:
            return QueryResult(
                success=False,
                error=f"Table '{right_table_name}' does not exist",
            )

        try:
            left_table = self.tables[left_table_name]
            right_table = self.tables[right_table_name]

            # Get all rows from both tables
            left_rows = left_table.select()
            right_rows = right_table.select()

            # Perform join based on join condition
            join_results = []
            join_condition = parsed_query.join_condition

            for left_row in left_rows:
                for right_row in right_rows:
                    # Check join condition
                    if self._evaluate_join_condition(
                        left_row, right_row, join_condition
                    ):
                        # Combine rows (prefix column names with table names)
                        combined_row = {}
                        for col, val in left_row.items():
                            combined_row[f"{left_table_name}.{col}"] = val
                        for col, val in right_row.items():
                            combined_row[f"{right_table_name}.{col}"] = val

                        join_results.append(combined_row)

            # Apply WHERE clause if present
            if parsed_query.where_clause:
                filtered_results = []
                for row in join_results:
                    if self._evaluate_where_clause_on_joined_row(
                        row, parsed_query.where_clause
                    ):
                        filtered_results.append(row)
                join_results = filtered_results

            # Project columns
            if parsed_query.columns and parsed_query.columns != ["*"]:
                projected_results = []
                for row in join_results:
                    projected_row = {}
                    for col in parsed_query.columns:
                        if col in row:
                            projected_row[col] = row[col]

                        else:
                            # Try to find column without table prefix
                            found = False
                            for full_col in row:
                                if full_col.endswith(f".{col}"):
                                    projected_row[col] = row[full_col]
                                    found = True
                                    break

                            if not found:
                                projected_row[col] = None
                    projected_results.append(projected_row)
                join_results = projected_results

            return QueryResult(
                success=True,
                data=join_results,
                columns=list(join_results[0].keys()) if join_results else [],
                rows_affected=len(join_results),
            )

        except Exception as e:
            return QueryResult(
                success=False, error=f"Join query failed: {str(e)}"
            )

    def _evaluate_join_condition(
        self, left_row: Dict, right_row: Dict, condition: Dict
    ) -> bool:
        """Evaluate JOIN ON condition"""
        if not condition:
            return True  # CROSS JOIN

        left_col = condition.get("left_column")
        right_col = condition.get("right_column")

        if left_col and right_col:
            return left_row.get(left_col) == right_row.get(right_col)

        return True

    def _evaluate_where_clause_on_joined_row(
        self, row: Dict, where_clause: Dict
    ) -> bool:
        """Evaluate WHERE clause on a joined row"""
        for col_name, expected_value in where_clause.items():
            # Try exact match first
            if col_name in row:
                if row[col_name] != expected_value:
                    return False

            else:
                # Try to find column with table prefix
                found = False
                for full_col in row:
                    if full_col.endswith(f".{col_name}"):
                        if row[full_col] != expected_value:
                            return False

                        found = True
                        break

                if not found:
                    return False
        return True

    def _execute_update(self, parsed_query) -> QueryResult:
        """Execute UPDATE query"""
        table_name = parsed_query.table_name

        if table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' does not exist"
            )

        try:
            table = self.tables[table_name]
            rows_updated = table.update(
                set_values=parsed_query.set_values,
                where_clause=parsed_query.where_clause,
            )

            # Save to storage
            self._save_table_to_storage(table_name)

            return QueryResult(
                success=True,
                rows_affected=rows_updated,
                message=f"Updated {rows_updated} row(s) in '{table_name}'",
            )

        except Exception as e:
            return QueryResult(success=False, error=f"Update failed: {str(e)}")

    def _execute_delete(self, parsed_query) -> QueryResult:
        """Execute DELETE query"""
        table_name = parsed_query.table_name

        if table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' does not exist"
            )

        try:
            table = self.tables[table_name]
            rows_deleted = table.delete(where_clause=parsed_query.where_clause)

            # Save to storage
            self._save_table_to_storage(table_name)

            return QueryResult(
                success=True,
                rows_affected=rows_deleted,
                message=f"Deleted {rows_deleted} row(s) from '{table_name}'",
            )

        except Exception as e:
            return QueryResult(success=False, error=f"Delete failed: {str(e)}")

    def _execute_drop_table(self, parsed_query) -> QueryResult:
        """Execute DROP TABLE query"""
        table_name = parsed_query.table_name

        if table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' does not exist"
            )

        try:
            # Remove from memory
            del self.tables[table_name]

            # Remove from storage
            self.storage.delete_table(table_name)

            return QueryResult(
                success=True,
                message=f"Table '{table_name}' dropped successfully",
            )

        except Exception as e:
            return QueryResult(
                success=False, error=f"Drop table failed: {str(e)}"
            )

    def _execute_show_tables(self) -> QueryResult:
        """Execute SHOW TABLES query"""
        tables_info = []
        for table_name, table in self.tables.items():
            tables_info.append(
                {"table_name": table_name, "row_count": table.get_row_count()}
            )

        return QueryResult(
            success=True,
            data=tables_info,
            columns=["table_name", "row_count"],
            rows_affected=len(tables_info),
        )

    def _execute_describe(self, parsed_query) -> QueryResult:
        """Execute DESCRIBE query"""
        table_name = parsed_query.table_name

        if table_name not in self.tables:
            return QueryResult(
                success=False, error=f"Table '{table_name}' does not exist"
            )

        table = self.tables[table_name]
        schema_info = []

        for col_name in table.column_order:
            column = table.columns[col_name]
            schema_info.append(
                {
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "nullable": "YES" if column.nullable else "NO",
                    "key": "PRI"
                    if column.primary_key
                    else ("UNI" if column.unique else ""),
                    "default": None,
                }
            )

        return QueryResult(
            success=True,
            data=schema_info,
            columns=["column_name", "data_type", "nullable", "key", "default"],
            rows_affected=len(schema_info),
        )

    def get_table(self, table_name: str) -> Optional[Table]:
        """Get a table by name"""
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        """Get list of all table names"""
        return list(self.tables.keys())

    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        stats = self.storage.get_database_stats()
        stats["tables_in_memory"] = len(self.tables)
        return stats

    def backup(self, backup_path: str) -> bool:
        """Create a backup of the database"""
        return self.storage.backup_database(backup_path)

    def restore(self, backup_path: str) -> bool:
        """Restore database from backup"""
        success = self.storage.restore_database(backup_path)
        if success:
            self.tables.clear()
            self._load_tables_from_storage()
        return success

    def close(self):
        """Close database and clean up resources"""
        # Save all tables
        for table_name in self.tables:
            self._save_table_to_storage(table_name)

        # Close storage
        self.storage.close()

        # Clear memory
        self.tables.clear()
