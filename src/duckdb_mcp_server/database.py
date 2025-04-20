"""
DuckDB connection and query handling for the MCP server.
"""

import logging
import os
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb

from .config import Config
from .credentials import setup_s3_credentials

logger = logging.getLogger("mcp-server-duckdb.database")


class DuckDBClient:
    """
    DuckDB client that handles database connections and query execution.
    
    This class manages connections to DuckDB and ensures proper setup
    for S3 and other extensions.
    """

    def __init__(self, config: Config):
        """
        Initialize the DuckDB client with the given configuration.
        
        Args:
            config: Server configuration
        """
        self.config = config
        self._initialize_database()
        
    def _initialize_database(self) -> None:
        """Initialize the database file and directory if needed."""
        dir_path = self.config.db_path.parent
        
        # Create parent directory if it doesn't exist
        if not dir_path.exists():
            if self.config.readonly:
                raise ValueError(
                    f"Database directory does not exist: {dir_path} and readonly mode is enabled."
                )
                
            logger.info(f"Creating directory: {dir_path}")
            dir_path.mkdir(parents=True, exist_ok=True)
            
        # Create database file if it doesn't exist
        if not self.config.db_path.exists() and not self.config.readonly:
            logger.info(f"Creating DuckDB database: {self.config.db_path}")
            # Create and close the database - this ensures the file exists
            conn = duckdb.connect(str(self.config.db_path))
            conn.close()
            
        # Validate database file exists if readonly
        if self.config.readonly and not self.config.db_path.exists():
            raise ValueError(
                f"Database file does not exist: {self.config.db_path} and readonly mode is enabled."
            )

    @contextmanager
    def get_connection(self):
        """
        Get a DuckDB connection with proper extensions and settings.
        
        This context manager ensures the connection is properly configured
        and closed after use.
        
        Yields:
            duckdb.DuckDBPyConnection: Configured DuckDB connection
        """
        connection = duckdb.connect(
            str(self.config.db_path), 
            read_only=self.config.readonly
        )
        
        try:
            # Load httpfs extension for S3 support
            connection.execute("INSTALL httpfs; LOAD httpfs;")
            
            # Configure S3 credentials
            setup_s3_credentials(connection, self.config)
            
            # Install and load other potentially useful extensions
            connection.execute("INSTALL json; LOAD json;")
            
            yield connection
        finally:
            connection.close()
            
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Tuple[List[Any], List[str]]:
        """
        Execute a SQL query on the DuckDB database.
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            
        Returns:
            Tuple of (results, column_names)
        
        Raises:
            duckdb.Error: If the query execution fails
        """
        with self.get_connection() as connection:
            logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
            
            # Execute the query
            result = connection.execute(query, parameters if parameters else {})
            
            # Get column names
            column_names = [col[0] for col in result.description]
            
            # Fetch all results
            rows = result.fetchall()
            
            return rows, column_names
    
    def format_result(self, results: List[Any], column_names: List[str]) -> str:
        """
        Format query results as a readable text table.
        
        Args:
            results: Query result rows
            column_names: Column names
            
        Returns:
            Formatted string representation of the results
        """
        if not results:
            return "Query executed successfully. No results returned."
            
        # Format each row as string
        rows_as_str = []
        
        # Add header row
        rows_as_str.append(" | ".join(column_names))
        
        # Add separator
        rows_as_str.append("-" * (sum(len(col) for col in column_names) + 3 * (len(column_names) - 1)))
        
        # Add data rows
        for row in results:
            # Convert each value in the row to string
            str_row = [str(val) if val is not None else "NULL" for val in row]
            rows_as_str.append(" | ".join(str_row))
            
        return "\n".join(rows_as_str)
    
    def query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Execute a query and return formatted results.
        
        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            
        Returns:
            Formatted result string
            
        Raises:
            Exception: If the query execution fails
        """
        try:
            results, column_names = self.execute_query(query, parameters)
            return self.format_result(results, column_names)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return f"Error executing query: {str(e)}"
            
    def get_parquet_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from a Parquet file.
        
        Args:
            file_path: Path to the Parquet file (can be local or S3)
            
        Returns:
            Dictionary containing metadata information
            
        Raises:
            Exception: If metadata extraction fails
        """
        try:
            # Query parquet_metadata() function to get file metadata
            query = f"SELECT * FROM parquet_metadata('{file_path}')"
            results, column_names = self.execute_query(query)
            
            # Format into a more useful structure
            metadata = {
                "file_path": file_path,
                "schema": self.get_schema(file_path),
                "row_count": None,
                "columns": []
            }
            
            # Extract row count if available
            count_query = f"SELECT COUNT(*) FROM '{file_path}'"
            count_results, _ = self.execute_query(count_query)
            if count_results and count_results[0]:
                metadata["row_count"] = count_results[0][0]
                
            return metadata
        except Exception as e:
            logger.error(f"Error extracting Parquet metadata: {str(e)}")
            raise
            
    def get_schema(self, file_path: str) -> List[Dict[str, str]]:
        """
        Get the schema of a file (works with CSV, Parquet, JSON).
        
        Args:
            file_path: Path to the file (can be local or S3)
            
        Returns:
            List of column definitions with name and type
            
        Raises:
            Exception: If schema extraction fails
        """
        try:
            # Use DESCRIBE to get schema information
            query = f"DESCRIBE SELECT * FROM '{file_path}' LIMIT 0"
            results, column_names = self.execute_query(query)
            
            # Format into a more useful structure
            schema = []
            for row in results:
                schema.append({
                    "column_name": row[0],
                    "column_type": row[1]
                })
                
            return schema
        except Exception as e:
            logger.error(f"Error extracting schema: {str(e)}")
            raise
            
    def analyze_data(self, file_path: str) -> Dict[str, Any]:
        """
        Perform basic statistical analysis on the data.
        
        Args:
            file_path: Path to the file (can be local or S3)
            
        Returns:
            Dictionary containing analysis results
            
        Raises:
            Exception: If analysis fails
        """
        try:
            # Get schema first to understand column types
            schema = self.get_schema(file_path)
            
            # Collect all numeric and date columns for analysis
            numeric_columns = []
            date_columns = []
            categorical_columns = []
            
            for col in schema:
                col_type = col["column_type"].upper()
                col_name = col["column_name"]
                
                if any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"]):
                    numeric_columns.append(col_name)
                elif any(t in col_type for t in ["DATE", "TIMESTAMP", "TIME"]):
                    date_columns.append(col_name)
                elif any(t in col_type for t in ["VARCHAR", "TEXT", "CHAR", "STRING"]):
                    categorical_columns.append(col_name)
            
            analysis = {
                "file_path": file_path,
                "row_count": None,
                "numeric_analysis": {},
                "date_analysis": {},
                "categorical_analysis": {}
            }
            
            # Get row count
            count_query = f"SELECT COUNT(*) FROM '{file_path}'"
            count_results, _ = self.execute_query(count_query)
            if count_results and count_results[0]:
                analysis["row_count"] = count_results[0][0]
            
            # Analyze numeric columns
            if numeric_columns:
                metrics = ["MIN", "MAX", "AVG", "MEDIAN", "STDDEV"]
                select_parts = []
                
                for col in numeric_columns:
                    for metric in metrics:
                        select_parts.append(f"{metric}(\"{col}\") as {col}_{metric.lower()}")
                
                if select_parts:
                    numeric_query = f"SELECT {', '.join(select_parts)} FROM '{file_path}'"
                    numeric_results, numeric_cols = self.execute_query(numeric_query)
                    
                    if numeric_results:
                        # Convert results to a more usable format
                        result_dict = {}
                        for i, col_name in enumerate(numeric_cols):
                            result_dict[col_name] = numeric_results[0][i]
                        
                        # Organize by column
                        for col in numeric_columns:
                            analysis["numeric_analysis"][col] = {
                                "min": result_dict.get(f"{col}_min"),
                                "max": result_dict.get(f"{col}_max"),
                                "avg": result_dict.get(f"{col}_avg"),
                                "median": result_dict.get(f"{col}_median"),
                                "stddev": result_dict.get(f"{col}_stddev")
                            }
            
            # Analyze date columns
            if date_columns:
                for col in date_columns:
                    date_query = f"SELECT MIN(\"{col}\"), MAX(\"{col}\") FROM '{file_path}'"
                    date_results, _ = self.execute_query(date_query)
                    
                    if date_results and date_results[0]:
                        analysis["date_analysis"][col] = {
                            "min_date": str(date_results[0][0]),
                            "max_date": str(date_results[0][1])
                        }
            
            # Analyze categorical columns (top values)
            if categorical_columns:
                for col in categorical_columns:
                    # Get top 5 most frequent values
                    cat_query = f"""
                    SELECT \"{col}\", COUNT(*) as count 
                    FROM '{file_path}' 
                    WHERE \"{col}\" IS NOT NULL 
                    GROUP BY \"{col}\" 
                    ORDER BY count DESC 
                    LIMIT 5
                    """
                    cat_results, _ = self.execute_query(cat_query)
                    
                    if cat_results:
                        top_values = []
                        for row in cat_results:
                            top_values.append({
                                "value": str(row[0]),
                                "count": row[1]
                            })
                        
                        analysis["categorical_analysis"][col] = {
                            "top_values": top_values
                        }
            
            return analysis
        except Exception as e:
            logger.error(f"Error analyzing data: {str(e)}")
            raise
            
    def suggest_visualizations(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Suggest possible visualizations based on data analysis.
        
        Args:
            file_path: Path to the file (can be local or S3)
            
        Returns:
            List of visualization suggestions
            
        Raises:
            Exception: If suggestion generation fails
        """
        try:
            # First analyze the data
            analysis = self.analyze_data(file_path)
            suggestions = []
            
            # Suggest time series if we have date columns and numeric columns
            if analysis["date_analysis"] and analysis["numeric_analysis"]:
                for date_col in analysis["date_analysis"]:
                    for numeric_col in analysis["numeric_analysis"]:
                        suggestions.append({
                            "type": "time_series",
                            "title": f"{numeric_col} over time",
                            "description": f"Line chart showing {numeric_col} values over time ({date_col})",
                            "query": f"""
                            SELECT 
                                \"{date_col}\", 
                                \"{numeric_col}\" 
                            FROM 
                                '{file_path}' 
                            WHERE 
                                \"{date_col}\" IS NOT NULL AND 
                                \"{numeric_col}\" IS NOT NULL 
                            ORDER BY 
                                \"{date_col}\"
                            """
                        })
            
            # Suggest bar charts for categorical data
            if analysis["categorical_analysis"] and analysis["numeric_analysis"]:
                for cat_col in analysis["categorical_analysis"]:
                    for numeric_col in analysis["numeric_analysis"]:
                        suggestions.append({
                            "type": "bar_chart",
                            "title": f"{numeric_col} by {cat_col}",
                            "description": f"Bar chart showing average {numeric_col} for each {cat_col} category",
                            "query": f"""
                            SELECT 
                                \"{cat_col}\", 
                                AVG(\"{numeric_col}\") as avg_{numeric_col} 
                            FROM 
                                '{file_path}' 
                            WHERE 
                                \"{cat_col}\" IS NOT NULL 
                            GROUP BY 
                                \"{cat_col}\" 
                            ORDER BY 
                                avg_{numeric_col} DESC 
                            LIMIT 10
                            """
                        })
            
            # Suggest scatter plots for pairs of numeric columns
            numeric_cols = list(analysis["numeric_analysis"].keys())
            if len(numeric_cols) >= 2:
                for i in range(min(len(numeric_cols), 3)):  # Limit to first 3 columns to avoid too many suggestions
                    for j in range(i+1, min(len(numeric_cols), 4)):
                        suggestions.append({
                            "type": "scatter_plot",
                            "title": f"{numeric_cols[i]} vs {numeric_cols[j]}",
                            "description": f"Scatter plot showing relationship between {numeric_cols[i]} and {numeric_cols[j]}",
                            "query": f"""
                            SELECT 
                                \"{numeric_cols[i]}\", 
                                \"{numeric_cols[j]}\" 
                            FROM 
                                '{file_path}' 
                            WHERE 
                                \"{numeric_cols[i]}\" IS NOT NULL AND 
                                \"{numeric_cols[j]}\" IS NOT NULL
                            LIMIT 1000
                            """
                        })
            
            return suggestions
        except Exception as e:
            logger.error(f"Error generating visualization suggestions: {str(e)}")
            raise

    # Methods for MCP tool operations
    
    def handle_query_tool(self, query: str, session_id: str) -> str:
        """
        Execute a query and provide helpful suggestions.
        
        Args:
            query: SQL query to execute
            session_id: Session ID for context
            
        Returns:
            Formatted result with suggestions
        """
        # Execute the query
        result = self.query(query)
        
        # Extract file paths from query if it's a CREATE TABLE query
        lower_query = query.lower()
        suggestion = ""
        
        if "create table" in lower_query and "as select" in lower_query:
            # Add suggestions for multi-file operations with union_by_name
            if "read_" in lower_query and ("*" in lower_query or "[" in lower_query):
                if "union_by_name" not in lower_query:
                    suggestion += ("\n\nReminder: When working with multiple files that might have "
                                  "different schemas, use the union_by_name parameter to avoid schema conflicts:"
                                  "\nSELECT * FROM read_parquet('path/*.parquet', union_by_name=true)")
        
        elif any(reader in lower_query for reader in ["read_parquet", "read_csv", "read_json"]) and "create table" not in lower_query:
            # Recommend caching to a table for direct file access queries
            short_id = session_id[:8]
            table_suggestion = f"cached_data_session_{short_id}"
            suggestion = (f"\n\nTIP: For better performance with remote or multiple files, consider caching the data first:"
                        f"\nCREATE TABLE {table_suggestion} AS {query}"
                        f"\n\nThen you can query the cached table directly:"
                        f"\nSELECT * FROM {table_suggestion} LIMIT 10;")
        
        # Combine result and suggestion
        return result + suggestion
        
    def extract_file_paths_from_query(self, query: str) -> List[str]:
        """
        Extract file paths from a SQL query.
        
        Args:
            query: SQL query
            
        Returns:
            List of file paths found in the query
        """
        # Simple pattern matching to find file paths
        if "read_" in query:
            start_idx = query.find("read_")
            if start_idx > 0:
                end_idx = query.find(")", start_idx)
                if end_idx > 0:
                    file_part = query[start_idx:end_idx]
                    # Extract paths inside quotes
                    import re
                    file_paths = re.findall(r"'([^']*)'", file_part)
                    return file_paths
        return []
        
    def extract_table_name_from_query(self, query: str) -> Optional[str]:
        """
        Extract table name from a CREATE TABLE query.
        
        Args:
            query: SQL query
            
        Returns:
            Table name or None if not a CREATE TABLE query
        """
        lower_query = query.lower()
        if "create table" in lower_query:
            try:
                # Extract table name using basic parsing
                parts = query.split("CREATE TABLE", 1)[1].split("AS")[0].strip()
                table_name = parts.split()[0].strip()
                return table_name
            except (IndexError, ValueError):
                pass
        return None
    
    def handle_analyze_schema_tool(self, file_path_or_table: str) -> str:
        """
        Analyze the schema of a file or table.
        
        Args:
            file_path_or_table: Path to file or table name
            
        Returns:
            Schema information as formatted text
        """
        # Try to identify if we're dealing with a file path or table name
        if file_path_or_table.startswith("s3://") or any(ext in file_path_or_table for ext in [".parquet", ".csv", ".json"]):
            # It's a file path
            try:
                # Execute DESCRIBE on the file path
                query = f"DESCRIBE SELECT * FROM '{file_path_or_table}' LIMIT 0"
                return self.query(query)
            except Exception as e:
                logger.error(f"Error analyzing schema: {str(e)}")
                return f"Error analyzing schema: {str(e)}"
        else:
            # Assume it's a table name
            try:
                # Execute DESCRIBE on the table name
                query = f"DESCRIBE {file_path_or_table}"
                return self.query(query)
            except Exception as e:
                logger.error(f"Error analyzing schema: {str(e)}")
                return f"Error analyzing schema: {str(e)}"
    
    def handle_analyze_data_tool(self, table_name: str) -> str:
        """
        Analyze data in a table.
        
        Args:
            table_name: Name of the table to analyze
            
        Returns:
            Analysis results as formatted text
        """
        try:
            # Generate and execute summary queries
            summary_parts = []
            
            # Basic table info
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            count_result = self.query(count_query)
            summary_parts.append(f"Table: {table_name}\n{count_result}")
            
            # Preview data
            preview_query = f"SELECT * FROM {table_name} LIMIT 5"
            preview_result = self.query(preview_query)
            summary_parts.append(f"Data Preview:\n{preview_result}")
            
            # Get column information
            columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
            columns_result, col_names = self.execute_query(columns_query)
            
            if columns_result:
                # Collect column types for more specific analysis
                numeric_columns = []
                date_columns = []
                categorical_columns = []
                
                for col in columns_result:
                    col_type = col[1].upper()
                    col_name = col[0]
                    
                    if any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"]):
                        numeric_columns.append(col_name)
                    elif any(t in col_type for t in ["DATE", "TIMESTAMP", "TIME"]):
                        date_columns.append(col_name)
                    elif any(t in col_type for t in ["VARCHAR", "TEXT", "CHAR", "STRING"]):
                        categorical_columns.append(col_name)
                
                # Analyze numeric columns
                if numeric_columns:
                    for col in numeric_columns[:3]:  # Limit to first 3 to avoid too much output
                        stats_query = f"""
                        SELECT 
                            MIN("{col}") as min_value,
                            MAX("{col}") as max_value,
                            AVG("{col}") as avg_value,
                            MEDIAN("{col}") as median_value
                        FROM {table_name}
                        """
                        stats_result = self.query(stats_query)
                        summary_parts.append(f"Stats for {col}:\n{stats_result}")
                
                # Distribution for categorical columns
                if categorical_columns:
                    for col in categorical_columns[:2]:  # Limit to first 2
                        dist_query = f"""
                        SELECT "{col}", COUNT(*) as count
                        FROM {table_name}
                        GROUP BY "{col}"
                        ORDER BY count DESC
                        LIMIT 5
                        """
                        dist_result = self.query(dist_query)
                        summary_parts.append(f"Distribution for {col}:\n{dist_result}")
            
            # Combine all summary parts
            return "\n\n".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Error analyzing data: {str(e)}")
            return f"Error analyzing data: {str(e)}"
            
    def handle_suggest_visualizations_tool(self, table_name: str) -> str:
        """
        Suggest visualizations for a table.
        
        Args:
            table_name: Name of the table to analyze
            
        Returns:
            Visualization suggestions as formatted text
        """
        try:
            # Get column information
            columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
            columns_result, _ = self.execute_query(columns_query)
            
            visualization_suggestions = [
                f"# Visualization Queries for {table_name}\n\n"
                "Here are some suggested queries to prepare data for different visualization types:"
            ]
            
            # Basic frequency chart
            if columns_result:
                # Find a good categorical column
                categorical_columns = [col[0] for col in columns_result if 
                                     col[1].lower() in ('varchar', 'text', 'char', 'enum')]
                numeric_columns = [col[0] for col in columns_result if 
                                  col[1].lower() in ('integer', 'bigint', 'double', 'float', 'decimal')]
                date_columns = [col[0] for col in columns_result if 
                               'date' in col[1].lower() or 'time' in col[1].lower()]
                
                # Bar chart
                if categorical_columns and numeric_columns:
                    cat_col = categorical_columns[0]
                    num_col = numeric_columns[0]
                    visualization_suggestions.append(f"""
## Bar Chart
```sql
SELECT {cat_col}, SUM({num_col}) as total
FROM {table_name}
GROUP BY {cat_col}
ORDER BY total DESC
LIMIT 10;
```
                    """)
                
                # Time series
                if date_columns and numeric_columns:
                    date_col = date_columns[0]
                    num_col = numeric_columns[0]
                    visualization_suggestions.append(f"""
## Time Series Chart
```sql
SELECT {date_col}, SUM({num_col}) as total
FROM {table_name}
GROUP BY {date_col}
ORDER BY {date_col};
```
                    """)
                
                # For scatter plots
                if len(numeric_columns) >= 2:
                    vis1 = numeric_columns[0]
                    vis2 = numeric_columns[1]
                    visualization_suggestions.append(f"""
## Scatter Plot
```sql
SELECT {vis1}, {vis2}
FROM {table_name}
LIMIT 1000;
```
                    """)
            
            return "\n\n".join(visualization_suggestions)
            
        except Exception as e:
            logger.error(f"Error suggesting visualizations: {str(e)}")
            return f"Error suggesting visualizations: {str(e)}"

    def generate_table_cache_suggestion(self, file_path: str, session_id: str) -> str:
        """
        Generate a suggestion to cache a file into a table.
        
        Args:
            file_path: Path to the file
            session_id: Session ID for context
            
        Returns:
            Suggestion text
        """
        short_id = session_id[:8]
        recommended_table = f"cached_data_session_{short_id}"
        
        suggestion = (f"To work with this data efficiently, first cache it into a table:\n\n"
                     f"```sql\nCREATE TABLE {recommended_table} AS SELECT * FROM ")
        
        if ".parquet" in file_path or file_path.endswith(".parq"):
            suggestion += f"read_parquet('{file_path}'"
        elif ".csv" in file_path:
            suggestion += f"read_csv('{file_path}'"
        elif ".json" in file_path:
            suggestion += f"read_json('{file_path}'"
        else:
            suggestion += f"read_parquet('{file_path}'"
            
        # Add union_by_name if it might be a glob pattern
        if "*" in file_path or "[" in file_path:
            suggestion += ", union_by_name=true"
            
        suggestion += ");\n```\n\nThen you can query the cached table directly."
        
        return suggestion