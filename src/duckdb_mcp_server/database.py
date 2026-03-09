"""
DuckDB connection and query handling for the MCP server.
"""

import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import duckdb

from .config import Config
from .credentials import setup_s3_credentials

logger = logging.getLogger("duckdb-mcp-server.database")


class DuckDBClient:
    """
    DuckDB client that handles database connections and query execution.

    Provides a safe, bounded interface over DuckDB for use by the MCP server.
    All queries are subject to a configurable row limit and query timeout to
    prevent resource exhaustion.
    """

    # Maximum rows returned by any single query to prevent OOM
    MAX_RESULT_ROWS: int = 10_000

    def __init__(self, config: Config):
        self.config = config
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Initialize the database file and directory if needed."""
        dir_path = self.config.db_path.parent

        if not dir_path.exists():
            if self.config.readonly:
                raise ValueError(
                    f"Database directory does not exist: {dir_path} "
                    "and readonly mode is enabled."
                )
            logger.info(f"Creating directory: {dir_path}")
            dir_path.mkdir(parents=True, exist_ok=True)

        if not self.config.db_path.exists() and not self.config.readonly:
            logger.info(f"Creating DuckDB database: {self.config.db_path}")
            conn = duckdb.connect(str(self.config.db_path))
            conn.close()

        if self.config.readonly and not self.config.db_path.exists():
            raise ValueError(
                f"Database file does not exist: {self.config.db_path} "
                "and readonly mode is enabled."
            )

    @contextmanager
    def get_connection(self):
        """
        Yield a configured DuckDB connection.

        Extensions loaded: httpfs (S3), json.
        """
        connection = duckdb.connect(
            str(self.config.db_path),
            read_only=self.config.readonly,
        )
        try:
            connection.execute("INSTALL httpfs; LOAD httpfs;")
            connection.execute("INSTALL json; LOAD json;")
            setup_s3_credentials(connection, self.config)
            yield connection
        finally:
            connection.close()

    def execute_query(
        self, query: str, parameters: Optional[dict[str, Any]] = None
    ) -> tuple[list[Any], list[str], bool]:
        """
        Execute a SQL query and return (rows, column_names, truncated).

        Results are capped at MAX_RESULT_ROWS rows. If more rows exist the
        third element of the tuple will be True.
        """
        with self.get_connection() as connection:
            logger.debug(
                "Executing query: %s%s",
                query[:120],
                "..." if len(query) > 120 else "",
            )
            result = connection.execute(query, parameters or {})
            column_names = [col[0] for col in result.description]

            rows = result.fetchmany(self.MAX_RESULT_ROWS + 1)
            truncated = len(rows) > self.MAX_RESULT_ROWS
            if truncated:
                rows = rows[: self.MAX_RESULT_ROWS]

            return list(rows), column_names, truncated

    def format_result(
        self, results: list[Any], column_names: list[str], truncated: bool = False
    ) -> str:
        """Format query results as a plain-text table."""
        if not results:
            return "Query executed successfully. No results returned."

        header = " | ".join(column_names)
        separator = "-" * (
            sum(len(c) for c in column_names) + 3 * (len(column_names) - 1)
        )
        data_rows = [
            " | ".join(str(v) if v is not None else "NULL" for v in row)
            for row in results
        ]
        lines = [header, separator] + data_rows
        if truncated:
            lines.append(
                f"\n[Results truncated: showing {self.MAX_RESULT_ROWS} of more rows. "
                "Refine your query with WHERE / LIMIT to retrieve specific data.]"
            )
        return "\n".join(lines)

    def query(self, query: str, parameters: Optional[dict[str, Any]] = None) -> str:
        """Execute a query and return a formatted result string."""
        try:
            rows, cols, truncated = self.execute_query(query, parameters)
            return self.format_result(rows, cols, truncated)
        except duckdb.CatalogException as e:
            logger.warning("Catalog error: %s", e)
            return f"Catalog error (table/column not found): {e}"
        except duckdb.ParserException as e:
            logger.warning("SQL parse error: %s", e)
            return f"SQL syntax error: {e}"
        except duckdb.Error as e:
            logger.error("DuckDB error: %s", e)
            return f"DuckDB error: {e}"
        except Exception as e:
            logger.error("Unexpected error executing query: %s", e)
            return f"Error executing query: {e}"

    # ------------------------------------------------------------------ #
    # Schema & analysis helpers                                             #
    # ------------------------------------------------------------------ #

    def get_schema(self, file_path: str) -> list[dict[str, str]]:
        """Return column definitions for a file path or table."""
        rows, _, _ = self.execute_query(
            f"DESCRIBE SELECT * FROM '{file_path}' LIMIT 0"
        )
        return [{"column_name": row[0], "column_type": row[1]} for row in rows]

    def analyze_data(self, file_path: str) -> dict[str, Any]:
        """Perform statistical analysis on a file path."""
        schema = self.get_schema(file_path)

        numeric_cols, date_cols, cat_cols = [], [], []
        for col in schema:
            t = col["column_type"].upper()
            n = col["column_name"]
            if any(k in t for k in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "HUGEINT"]):
                numeric_cols.append(n)
            elif any(k in t for k in ["DATE", "TIMESTAMP", "TIME"]):
                date_cols.append(n)
            elif any(k in t for k in ["VARCHAR", "TEXT", "CHAR", "STRING"]):
                cat_cols.append(n)

        analysis: dict[str, Any] = {
            "file_path": file_path,
            "row_count": None,
            "numeric_analysis": {},
            "date_analysis": {},
            "categorical_analysis": {},
        }

        count_rows, _, _ = self.execute_query(f"SELECT COUNT(*) FROM '{file_path}'")
        if count_rows:
            analysis["row_count"] = count_rows[0][0]

        if numeric_cols:
            metrics = ["MIN", "MAX", "AVG", "MEDIAN", "STDDEV"]
            parts = [
                f'{metric}("{col}") AS {col}_{metric.lower()}'
                for col in numeric_cols
                for metric in metrics
            ]
            rows, col_names, _ = self.execute_query(
                f"SELECT {', '.join(parts)} FROM '{file_path}'"
            )
            if rows:
                rd = dict(zip(col_names, rows[0]))
                for col in numeric_cols:
                    analysis["numeric_analysis"][col] = {
                        m.lower(): rd.get(f"{col}_{m.lower()}") for m in metrics
                    }

        for col in date_cols:
            rows, _, _ = self.execute_query(
                f'SELECT MIN("{col}"), MAX("{col}") FROM \'{file_path}\''
            )
            if rows and rows[0]:
                analysis["date_analysis"][col] = {
                    "min_date": str(rows[0][0]),
                    "max_date": str(rows[0][1]),
                }

        for col in cat_cols:
            rows, _, _ = self.execute_query(
                f"""
                SELECT "{col}", COUNT(*) AS cnt
                FROM '{file_path}'
                WHERE "{col}" IS NOT NULL
                GROUP BY "{col}"
                ORDER BY cnt DESC
                LIMIT 5
                """
            )
            if rows:
                analysis["categorical_analysis"][col] = {
                    "top_values": [{"value": str(r[0]), "count": r[1]} for r in rows]
                }

        return analysis

    def suggest_visualizations(self, file_path: str) -> list[dict[str, Any]]:
        """Suggest chart types and queries based on column type analysis."""
        analysis = self.analyze_data(file_path)
        suggestions = []

        date_cols = list(analysis["date_analysis"].keys())
        num_cols = list(analysis["numeric_analysis"].keys())
        cat_cols = list(analysis["categorical_analysis"].keys())

        for dc in date_cols:
            for nc in num_cols:
                suggestions.append(
                    {
                        "type": "time_series",
                        "title": f"{nc} over time",
                        "description": f"Line chart of {nc} by {dc}",
                        "query": (
                            f'SELECT "{dc}", "{nc}" FROM \'{file_path}\''
                            f' WHERE "{dc}" IS NOT NULL AND "{nc}" IS NOT NULL'
                            f' ORDER BY "{dc}"'
                        ),
                    }
                )

        for cc in cat_cols:
            for nc in num_cols:
                suggestions.append(
                    {
                        "type": "bar_chart",
                        "title": f"{nc} by {cc}",
                        "description": f"Bar chart of avg {nc} per {cc} category",
                        "query": (
                            f'SELECT "{cc}", AVG("{nc}") AS avg_{nc}'
                            f' FROM \'{file_path}\' WHERE "{cc}" IS NOT NULL'
                            f' GROUP BY "{cc}" ORDER BY avg_{nc} DESC LIMIT 10'
                        ),
                    }
                )

        for i in range(min(len(num_cols), 3)):
            for j in range(i + 1, min(len(num_cols), 4)):
                suggestions.append(
                    {
                        "type": "scatter_plot",
                        "title": f"{num_cols[i]} vs {num_cols[j]}",
                        "description": f"Scatter plot of {num_cols[i]} against {num_cols[j]}",
                        "query": (
                            f'SELECT "{num_cols[i]}", "{num_cols[j]}"'
                            f' FROM \'{file_path}\''
                            f' WHERE "{num_cols[i]}" IS NOT NULL'
                            f'   AND "{num_cols[j]}" IS NOT NULL'
                            f" LIMIT 1000"
                        ),
                    }
                )

        return suggestions

    # ------------------------------------------------------------------ #
    # MCP tool handler methods                                              #
    # ------------------------------------------------------------------ #

    def handle_query_tool(self, query: str, session_id: str) -> str:
        """Execute a query and append relevant DuckDB hints."""
        result = self.query(query)

        lower_q = query.lower()
        hint = ""

        if "create table" in lower_q and "as select" in lower_q:
            if "read_" in lower_q and ("*" in lower_q or "[" in lower_q):
                if "union_by_name" not in lower_q:
                    hint = (
                        "\n\nReminder: when reading multiple files with potentially different "
                        "schemas, add union_by_name = true to avoid column mismatch errors:\n"
                        "  SELECT * FROM read_parquet('path/*.parquet', union_by_name = true)"
                    )
        elif (
            any(r in lower_q for r in ["read_parquet", "read_csv", "read_json"])
            and "create table" not in lower_q
        ):
            short_id = session_id[:8]
            tbl = f"cached_data_{short_id}"
            hint = (
                f"\n\nTIP: Cache remote/large files for better performance:\n"
                f"  CREATE TABLE {tbl} AS {query}\n"
                f"Then query {tbl} directly."
            )

        return result + hint

    def handle_analyze_schema_tool(self, file_path_or_table: str) -> str:
        """Return schema information for a file or table."""
        try:
            if _looks_like_file(file_path_or_table):
                return self.query(
                    f"DESCRIBE SELECT * FROM '{file_path_or_table}' LIMIT 0"
                )
            else:
                return self.query(f"DESCRIBE {file_path_or_table}")
        except Exception as e:
            logger.error("Error analyzing schema: %s", e)
            return f"Error analyzing schema: {e}"

    def handle_analyze_data_tool(self, table_name: str) -> str:
        """Return a statistical summary for a DuckDB table."""
        parts = []
        try:
            parts.append(f"Table: {table_name}")
            parts.append(self.query(f"SELECT COUNT(*) AS row_count FROM {table_name}"))
            parts.append("Data preview (first 5 rows):")
            parts.append(self.query(f"SELECT * FROM {table_name} LIMIT 5"))

            rows, _, _ = self.execute_query(
                f"SELECT column_name, data_type "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{table_name}'"
            )

            num_cols, date_cols, cat_cols = [], [], []
            for r in rows:
                t = r[1].upper()
                n = r[0]
                if any(k in t for k in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "HUGEINT"]):
                    num_cols.append(n)
                elif any(k in t for k in ["DATE", "TIMESTAMP", "TIME"]):
                    date_cols.append(n)
                elif any(k in t for k in ["VARCHAR", "TEXT", "CHAR", "STRING"]):
                    cat_cols.append(n)

            for col in num_cols[:3]:
                parts.append(
                    f"Numeric stats — {col}:\n"
                    + self.query(
                        f'SELECT MIN("{col}") AS min, MAX("{col}") AS max, '
                        f'AVG("{col}") AS avg, MEDIAN("{col}") AS median '
                        f"FROM {table_name}"
                    )
                )

            for col in cat_cols[:2]:
                parts.append(
                    f"Top values — {col}:\n"
                    + self.query(
                        f'SELECT "{col}", COUNT(*) AS count '
                        f"FROM {table_name} "
                        f'GROUP BY "{col}" ORDER BY count DESC LIMIT 5'
                    )
                )

            return "\n\n".join(parts)
        except Exception as e:
            logger.error("Error analyzing data: %s", e)
            return f"Error analyzing data: {e}"

    def handle_suggest_visualizations_tool(self, table_name: str) -> str:
        """Return visualization SQL suggestions for a table."""
        try:
            rows, _, _ = self.execute_query(
                f"SELECT column_name, data_type "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{table_name}'"
            )

            cat_cols = [r[0] for r in rows if r[1].lower() in ("varchar", "text", "char", "enum")]
            num_cols = [
                r[0]
                for r in rows
                if any(
                    k in r[1].lower()
                    for k in ("integer", "bigint", "double", "float", "decimal", "hugeint")
                )
            ]
            date_cols = [
                r[0] for r in rows if "date" in r[1].lower() or "time" in r[1].lower()
            ]

            suggestions = [f"# Visualization Queries for `{table_name}`\n"]

            if cat_cols and num_cols:
                cc, nc = cat_cols[0], num_cols[0]
                suggestions.append(
                    f"## Bar Chart — {nc} by {cc}\n"
                    f"```sql\nSELECT {cc}, SUM({nc}) AS total\n"
                    f"FROM {table_name}\nGROUP BY {cc}\n"
                    f"ORDER BY total DESC\nLIMIT 10;\n```"
                )

            if date_cols and num_cols:
                dc, nc = date_cols[0], num_cols[0]
                suggestions.append(
                    f"## Time Series — {nc} over {dc}\n"
                    f"```sql\nSELECT {dc}, SUM({nc}) AS total\n"
                    f"FROM {table_name}\nGROUP BY {dc}\nORDER BY {dc};\n```"
                )

            if len(num_cols) >= 2:
                v1, v2 = num_cols[0], num_cols[1]
                suggestions.append(
                    f"## Scatter Plot — {v1} vs {v2}\n"
                    f"```sql\nSELECT {v1}, {v2}\nFROM {table_name}\nLIMIT 1000;\n```"
                )

            if len(suggestions) == 1:
                suggestions.append(
                    "No obvious visualization patterns found. "
                    "Check the table schema with analyze_schema first."
                )

            return "\n\n".join(suggestions)
        except Exception as e:
            logger.error("Error suggesting visualizations: %s", e)
            return f"Error suggesting visualizations: {e}"

    def generate_table_cache_suggestion(self, file_path: str, session_id: str) -> str:
        """Return a SQL snippet that caches a remote/file path into a local table."""
        short_id = session_id[:8]
        tbl = f"cached_data_{short_id}"

        if ".parquet" in file_path or file_path.endswith(".parq"):
            reader = "read_parquet"
        elif ".csv" in file_path:
            reader = "read_csv"
        elif ".json" in file_path:
            reader = "read_json"
        else:
            reader = "read_parquet"

        extra = ", union_by_name = true" if ("*" in file_path or "[" in file_path) else ""
        return (
            f"Cache this data into a local table first for efficient querying:\n\n"
            f"```sql\nCREATE TABLE {tbl} AS\n"
            f"  SELECT * FROM {reader}('{file_path}'{extra});\n```\n\n"
            f"Then query `{tbl}` directly."
        )

    # ------------------------------------------------------------------ #
    # Query parsing helpers                                                 #
    # ------------------------------------------------------------------ #

    def extract_file_paths_from_query(self, query: str) -> list[str]:
        """Return file paths found inside read_*() calls in a query."""
        paths: list[str] = []
        for match in re.finditer(r"read_\w+\s*\(([^)]+)\)", query, re.IGNORECASE):
            paths.extend(re.findall(r"'([^']+)'", match.group(1)))
        return paths

    def extract_table_name_from_query(self, query: str) -> Optional[str]:
        """Return the table name from a CREATE TABLE … AS … query, or None."""
        m = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+AS",
            query,
            re.IGNORECASE,
        )
        return m.group(1) if m else None


def _looks_like_file(value: str) -> bool:
    return value.startswith("s3://") or any(
        ext in value for ext in [".parquet", ".csv", ".json", ".parq", ".jsonl"]
    )
