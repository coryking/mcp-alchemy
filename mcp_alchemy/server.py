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

@mcp.tool(description=f"Return all table names in the database separated by comma. {AVAILABLE_DATABASES}")
def all_table_names(database: Annotated[str, Field(description="Database to query")]) -> str:
    with database_manager.get_connection(database) as conn:
        inspector = inspect(conn)
        return ", ".join(inspector.get_table_names())

@mcp.tool(
    description=f"Return all table names in the database containing the substring 'q' separated by comma. {AVAILABLE_DATABASES}"
)
def filter_table_names(
    database: Annotated[str, Field(description="Database to query")],
    q: Annotated[str, Field(description="Substring to search for in table names")]
) -> str:
    with database_manager.get_connection(database) as conn:
        inspector = inspect(conn)
        return ", ".join(x for x in inspector.get_table_names() if q in x)

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