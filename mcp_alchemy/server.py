import os
import json
import hashlib
from typing import Annotated, Any

from fastmcp import FastMCP
from fastmcp.utilities.logging import get_logger

from sqlalchemy import inspect, text
from sqlalchemy.engine import Inspector

from pydantic import Field

from mcp_alchemy.models import DatabaseManager, QueryResult

### Helpers ###

def tests_set_global(k: str, v: Any) -> None:
    globals()[k] = v

### Database ###

logger = get_logger(__name__)
database_manager = DatabaseManager.from_environment()



### Constants ###

VERSION = "2025.8.15.91819"
AVAILABLE_DATABASES = database_manager.get_available_databases_text()
EXECUTE_QUERY_MAX_CHARS = int(os.environ.get('EXECUTE_QUERY_MAX_CHARS', 4000))
CLAUDE_LOCAL_FILES_PATH = os.environ.get('CLAUDE_LOCAL_FILES_PATH')

### MCP ###

mcp = FastMCP("MCP Alchemy")
get_logger(__name__).info(f"Starting MCP Alchemy version {VERSION}")

@mcp.tool(
    description=f"Return table names in the database. If 'q' is provided, filter to names containing that substring. {AVAILABLE_DATABASES}"
)
def get_table_names(
    database: Annotated[str, Field(description="Database to query")],
    q: Annotated[str | None, Field(default=None, description="Optional substring to search for in table names (if not provided, returns all tables)")]
) -> str:
    with database_manager.get_connection(database) as conn:
        inspector = inspect(conn)
        table_names = inspector.get_table_names()
        if q:
            table_names = [name for name in table_names if q in name]
        return ", ".join(table_names)

@mcp.tool(description=f"Returns schema and relation information for the given tables. {AVAILABLE_DATABASES}")
def schema_definitions(
        database: Annotated[str, Field(description="Database to query")],
        table_names: Annotated[list[str], Field(default_factory=list, description="The names of the tables to get the schema for")]
    ) -> str:
    def format(inspector: Inspector, table_name: str) -> str:
        columns = inspector.get_columns(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        primary_keys = set(inspector.get_pk_constraint(table_name)["constrained_columns"])
        result = [f"{table_name}:"]

        # Process columns
        show_key_only = {"nullable", "autoincrement"}
        for column in columns:
            if "comment" in column:
                del column["comment"]
            name = column.pop("name")
            column_parts = (["primary key"] if name in primary_keys else []) + [str(
                column.pop("type"))] + [k if k in show_key_only else f"{k}={v}" for k, v in column.items() if v]
            result.append(f"    {name}: " + ", ".join(column_parts))

        # Process indexes
        try:
            indexes = inspector.get_indexes(table_name)
            if indexes:
                result.extend(["", "    Indexes:"])
                for index in indexes:
                    name = index.get("name", "unnamed")
                    columns = ", ".join(index["column_names"])
                    unique = "unique" if index.get("unique") else ""
                    unique_str = f" {unique}" if unique else ""
                    result.append(f"      {name} on ({columns}){unique_str}")
        except (NotImplementedError, AttributeError):
            # Some databases don't support index introspection
            pass

        # Process unique constraints
        try:
            unique_constraints = inspector.get_unique_constraints(table_name)
            if unique_constraints:
                result.extend(["", "    Unique Constraints:"])
                for constraint in unique_constraints:
                    name = constraint.get("name", "unnamed")
                    columns = ", ".join(constraint["column_names"])
                    result.append(f"      {name} on ({columns})")
        except (NotImplementedError, AttributeError):
            # Some databases don't support unique constraint introspection
            pass

        # Process check constraints
        try:
            check_constraints = inspector.get_check_constraints(table_name)
            if check_constraints:
                result.extend(["", "    Check Constraints:"])
                for constraint in check_constraints:
                    name = constraint.get("name", "unnamed")
                    sqltext = constraint.get("sqltext", "unknown")
                    result.append(f"      {name}: {sqltext}")
        except (NotImplementedError, AttributeError):
            # Some databases don't support check constraint introspection
            pass

        # Process relationships
        if foreign_keys:
            result.extend(["", "    Relationships:"])
            for fk in foreign_keys:
                constrained_columns = ", ".join(fk['constrained_columns'])
                referred_table = fk['referred_table']
                referred_columns = ", ".join(fk['referred_columns'])
                result.append(f"      {constrained_columns} -> {referred_table}.{referred_columns}")

        return "\n".join(result)

    with database_manager.get_connection(database) as conn:
        inspector = inspect(conn)
        return "\n".join(format(inspector, table_name) for table_name in table_names)

def execute_query_description():
    parts = [
        f"Execute a SQL query and return results in a readable format. Results will be truncated after {EXECUTE_QUERY_MAX_CHARS} characters."
    ]
    if CLAUDE_LOCAL_FILES_PATH:
        parts.append("Claude Desktop may fetch the full result set via an url for analysis and artifacts.")
    parts.append(
        "IMPORTANT: You MUST use the params parameter for query parameter substitution (e.g. 'WHERE id = :id' with params={'id': 123}) to prevent SQL injection. Direct string concatenation is a serious security risk."
    )
    parts.append(AVAILABLE_DATABASES)
    return " ".join(parts)

@mcp.tool(description=execute_query_description())
def execute_query(
    database: Annotated[str, Field(description="Database to query")],
    query: Annotated[str, Field(description="SQL query to execute")],
    params: Annotated[dict[str, Any], Field(default_factory=dict, description="Query parameters for safe substitution")]
) -> QueryResult:
    def save_query_result(result: QueryResult) -> str | None:
        """Save complete result set for Claude if configured"""
        if not CLAUDE_LOCAL_FILES_PATH:
            return None

        file_hash = hashlib.sha256(result.model_dump_json().encode()).hexdigest()
        file_name = f"{file_hash}.json"

        with open(os.path.join(CLAUDE_LOCAL_FILES_PATH, file_name), 'w') as f:
            json.dump(result.model_dump(), f)

        return (
            f"Full result set url: https://cdn.jsdelivr.net/pyodide/claude-local-files/{file_name}"
            " (format: QueryResult JSON with columns/rows structure)"
            " (ALWAYS prefer fetching this url in artifacts instead of hardcoding the values if at all possible)")

    try:
        with database_manager.get_connection(database) as connection:
            cursor_result = connection.execute(text(query), params)

            if not cursor_result.returns_rows:
                # For non-SELECT queries, return empty result with affected row count
                return QueryResult(
                    columns=[],
                    rows=[],
                    database_row_count=cursor_result.rowcount,
                    truncated=False
                )

            # Create QueryResult from SQLAlchemy result
            # Use a reasonable row limit to prevent memory issues
            MAX_ROWS = 10000  # Much higher than old character limit
            query_result = QueryResult.from_sqlalchemy_result(cursor_result, max_rows=MAX_ROWS)

            # Save full result for Claude if configured
            _ = save_query_result(query_result)

            return query_result
    except Exception as e:
        # For errors, return empty result (could add error field to QueryResult if needed)
        raise e

def main():
    mcp.run()

if __name__ == "__main__":
    main()