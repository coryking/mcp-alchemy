import os
import json
import hashlib
from typing import Annotated, Any

from fastmcp import Context, FastMCP
from fastmcp.utilities.logging import get_logger

from sqlalchemy import text, inspect
from sqlalchemy.engine import Inspector

from pydantic import Field

from mcp_alchemy.models import DatabaseManager, QueryResult

### Database ###

logger = get_logger(__name__)
database_manager = DatabaseManager.from_environment()

### Helpers ###

def tests_set_global(k: str, v: Any) -> None:
    globals()[k] = v

async def validate_or_elicit_database(database: str | None, ctx: Context) -> str | None:
    """Validate database name or elicit from user if invalid.

    Returns:
        Valid database name if found or user accepts elicitation
        None if user declines/cancels or client doesn't support elicitation
    """
    if database and database in database_manager.databases:
        return database

    try:
        message = (
            f"Database '{database}' not found. Which database do you want to use?"
            if database
            else "Which database do you want to use?"
        )
        result = await ctx.elicit(
            message,
            response_type=database_manager.get_available_databases()
        )

        if result.action == "accept":
            return result.data
        return None
    except Exception:
        # Client doesn't support elicitation or other error
        return None




### Constants ###

VERSION = "2025.8.15.91819"
AVAILABLE_DATABASES = database_manager.get_available_databases_text()
EXECUTE_QUERY_MAX_CHARS = int(os.environ.get('EXECUTE_QUERY_MAX_CHARS', 4000))
CLAUDE_LOCAL_FILES_PATH = os.environ.get('CLAUDE_LOCAL_FILES_PATH')

### MCP ###

mcp = FastMCP(name="Database Query MCP Tool",
    version=VERSION,
    instructions=f"""
    A MCP server that connects to your database and allows you to query it.

    Available databases:
    {AVAILABLE_DATABASES}
    """,

)
get_logger(__name__).info(f"Starting MCP Alchemy version {VERSION}")

@mcp.tool(
    description=f"Return table names in the database. If 'q' is provided, filter to names containing that substring. DB's:{database_manager.get_available_databases_text_with_description()}"
)
async def get_table_names(
    ctx: Context,
    database: Annotated[str | None, Field(description="Database to query")],
    q: Annotated[str | None, Field(default=None, description="Optional substring to search for in table names (if not provided, returns all tables)")]
) -> str:
    database = await validate_or_elicit_database(database, ctx)
    if database is None:
        return f"Available databases:\n{AVAILABLE_DATABASES}"

    async with database_manager.connection(database) as conn:
        def _get_tables(sync_conn):
            inspector = inspect(sync_conn)
            table_names = inspector.get_table_names()
            if q:
                return [name for name in table_names if q in name]
            return table_names

        table_names = await conn.run_sync(_get_tables)
        return ", ".join(table_names)

@mcp.tool(description="Returns schema and relation information for the given tables.")
async def schema_definitions(
        ctx: Context,
        database: Annotated[str, Field(description="Database to query")],
        table_names: Annotated[list[str], Field(default_factory=list, description="The names of the tables to get the schema for")]
    ) -> str:

    selected_database = await validate_or_elicit_database(database, ctx)
    if selected_database is None:
        return f"Available databases:\n{AVAILABLE_DATABASES}"

    def format(inspector: Inspector, table_name: str) -> str:
        columns = inspector.get_columns(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        primary_keys = set(inspector.get_pk_constraint(table_name)["constrained_columns"])
        result = [f"Table: '{table_name}'"]

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

    async with database_manager.connection(selected_database) as conn:
        overall_result = []
        if database != selected_database:
            overall_result.append(f"The user selected this database: '{selected_database}'")
        else:
            overall_result.append(f"Database: '{selected_database}'")
        def _get_schema(sync_conn):
            inspector = inspect(sync_conn)
            for table_name in table_names:
                formatted = format(inspector, table_name)
                overall_result.append(formatted)
            return "\n".join(overall_result)

        return await conn.run_sync(_get_schema)

def execute_query_description():
    parts = [
        f"Execute a SQL query and return results in a readable format. Results will be truncated after {EXECUTE_QUERY_MAX_CHARS} characters."
    ]
    if CLAUDE_LOCAL_FILES_PATH:
        parts.append("Claude Desktop may fetch the full result set via an url for analysis and artifacts.")
    parts.append(
        "IMPORTANT: You MUST use the params parameter for query parameter substitution (e.g. 'WHERE id = :id' with params={'id': 123}) to prevent SQL injection. Direct string concatenation is a serious security risk."
    )
    #parts.append(AVAILABLE_DATABASES)
    return " ".join(parts)

@mcp.tool(description=execute_query_description())
async def execute_query(
    ctx: Context,
    database: Annotated[str, Field(description="Database to query", examples=database_manager.get_available_databases())],
    query: Annotated[str, Field(description="SQL query to execute")],
    params: Annotated[dict[str, Any], Field(default_factory=dict, description="Query parameters for safe substitution")]
) -> QueryResult | str:

    database = await validate_or_elicit_database(database, ctx)
    if database is None:
        return f"Available databases:\n{AVAILABLE_DATABASES}"

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
        config = database_manager.get_database(database)
        async with config.connection() as connection:
            cursor_result = await connection.execute(text(query), params)

            if not cursor_result.returns_rows:
                # For non-SELECT queries, return empty result with affected row count
                return QueryResult(
                    database_name=database,
                    columns=[],
                    rows=[],
                    database_row_count=cursor_result.rowcount,
                    truncated=False
                )

            # Create QueryResult from SQLAlchemy result
            # Use a reasonable row limit to prevent memory issues
            MAX_ROWS = 10000  # Much higher than old character limit
            query_result = QueryResult.from_sqlalchemy_result(database, cursor_result, max_rows=MAX_ROWS)

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