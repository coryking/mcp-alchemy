from dataclasses import dataclass, field
import os
import logging
import sys
from typing import Any
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection, create_async_engine
from sqlalchemy.engine import Result, Engine
from sqlalchemy import inspect as sqlalchemy_inspect
from auth.tokens import token_cache
from pydantic import BaseModel, Field, computed_field
logger = logging.getLogger(__name__)

class QueryResult(BaseModel):
    columns: list[str] = Field(default_factory=list, description="The columns of the query result")
    rows: list[list[Any]] = Field(default_factory=list, description="The rows of the query result")
    database_row_count: int = Field(default=0, description="Total rows returned by the database query")
    truncated: bool = Field(default=False, description="Whether results were truncated for response size")

    @computed_field
    @property
    def returned_row_count(self) -> int:
        """The number of rows included in this response"""
        return len(self.rows)

    @classmethod
    def from_sqlalchemy_result(cls, result: Result[Any], max_rows: int = None) -> "QueryResult":
        """Create QueryResult directly from SQLAlchemy Result"""
        columns = list(result.keys())
        rows = []
        database_row_count = 0

        # Collect all rows
        for row in result:
            database_row_count += 1
            rows.append(list(row))

        # Create instance
        instance = cls(
            columns=columns,
            rows=rows,
            database_row_count=database_row_count,
            truncated=False
        )

        # Apply truncation if needed
        if max_rows and database_row_count > max_rows:
            instance.truncated = True
            instance.rows = instance.rows[:max_rows]

        return instance

def create_engine_for_config(config: "DatabaseConfig") -> AsyncEngine:
    """Create async engine with MCP-optimized settings for a specific database config"""
    import json
    db_engine_options = os.environ.get('DB_ENGINE_OPTIONS')
    user_options = json.loads(db_engine_options) if db_engine_options else {}

    # MCP-optimized defaults that can be overridden by user
    options = {
        'isolation_level': 'AUTOCOMMIT',
        # Test connections before use (handles MySQL 8hr timeout, network drops)
        'pool_pre_ping': True,
        # Keep minimal connections (MCP typically handles one request at a time)
        'pool_size': 1,
        # Allow temporary burst capacity for edge cases
        'max_overflow': 2,
        # Force refresh connections older than 1hr (well under MySQL's 8hr default)
        'pool_recycle': 3600,
        # User can override any of the above
        **user_options
    }

    return create_async_engine(config.get_resolved_url(), **options)


@dataclass
class DatabaseConfig:
    name: str
    url: str
    description: str = ""
    available: bool = True
    read_only: bool = False
    engine: AsyncEngine | None = None

    def get_resolved_url(self) -> str:
        """Get connection URL with AZURE_TOKEN replaced if present"""
        if 'AZURE_TOKEN' in self.url:
            return self.url.replace('AZURE_TOKEN', token_cache.get_token())
        return self.url

    def get_engine(self) -> AsyncEngine:
        """Get or create the SQLAlchemy engine for this database"""
        if self.engine is None:
            self.engine = create_engine_for_config(self)
        return self.engine

    def mark_unavailable(self) -> None:
        """Mark this database as unavailable"""
        self.available = False
        if self.engine:
            try:
                import asyncio
                asyncio.create_task(self.engine.dispose())
            except Exception:
                pass
        self.engine = None

    @asynccontextmanager
    async def connection(self):
        """Get a connection with proper setup (version, read-only enforcement)"""
        from sqlalchemy import text

        if not self.available:
            raise ValueError(f"Database '{self.name}' is not available")

        engine = self.get_engine()
        async with engine.connect() as conn:
            # Set version variable for databases that support it
            try:
                _ = await conn.execute(text("SET @mcp_alchemy_version = '2025.8.15.91819'"))
            except Exception:
                # Some databases don't support session variables
                pass

            # Set read-only mode if configured
            if self.read_only:
                try:
                    # For PostgreSQL, set default transaction read-only
                    if 'postgresql' in str(engine.url):
                        _ = await conn.execute(text("SET SESSION default_transaction_read_only = on"))
                    else:
                        # For other databases, try to set transaction read-only
                        _ = await conn.execute(text("SET TRANSACTION READ ONLY"))
                except Exception as e:
                    # If we cannot ensure read-only mode, we cannot use the database and need to violently puke
                    # it is *crucial* to puke otherwise the LLM could easily change shit it shouldn't.
                    logger.error(f"Failed to set read-only mode for database '{self.name}': {e}")
                    sys.exit(1)

            yield conn

    def to_description_text(self) -> str:
        desc_parts: list[str] = []
        if self.description:
            desc_parts.append(self.description)
        if self.read_only:
            desc_parts.append("read-only")
        desc = f" ({', '.join(desc_parts)})" if desc_parts else ""
        return f"{self.name}{desc}"


@dataclass
class DatabaseManager:
    """Manages a collection of database configurations"""
    databases: dict[str, DatabaseConfig] = field(default_factory=dict)

    @classmethod
    def from_environment(cls) -> "DatabaseManager":
        """Create DatabaseManager by parsing environment variables"""
        manager = cls()

        for key, value in os.environ.items():
            if key.startswith('DB_') and key.endswith('_URL'):
                # Extract database name from key (e.g., DB_PRODUCTION_URL -> production)
                db_name_part = key[3:-4]  # Remove 'DB_' prefix and '_URL' suffix
                db_name = db_name_part.lower()

                # Check for duplicate names
                if db_name in manager.databases:
                    import sys
                    print(f"Error: Duplicate database name '{db_name}' found in environment variables", file=sys.stderr)
                    sys.exit(1)

                # Get description (optional)
                desc_key = f'DB_{db_name_part}_DESC'
                description = os.environ.get(desc_key, '')

                # Get read-only setting (optional)
                readonly_key = f'DB_{db_name_part}_READ_ONLY'
                read_only = os.environ.get(readonly_key, '').lower() in ('true', '1', 'yes', 'on')

                manager.databases[db_name] = DatabaseConfig(
                    name=db_name,
                    url=value,
                    description=description,
                    read_only=read_only
                )

        # Backwards compatibility: if no DB_*_URL vars, try DB_URL
        if not manager.databases:
            if 'DB_URL' in os.environ:
                # Check for read-only setting (optional)
                read_only = os.environ.get('DB_READ_ONLY', '').lower() in ('true', '1', 'yes', 'on')

                manager.databases['default'] = DatabaseConfig(
                    name='default',
                    url=os.environ['DB_URL'],
                    description='Default database',
                    read_only=read_only
                )
            else:
                import sys
                print("Error: No database configuration found. Set DB_{NAME}_URL environment variables or DB_URL", file=sys.stderr)
                sys.exit(1)

        return manager

    def get_database(self, name: str) -> DatabaseConfig:
        """Get database config by name (case insensitive)"""
        name = name.lower()
        if name not in self.databases:
            raise ValueError(f"Database '{name}' is not configured")
        return self.databases[name]

    def connection(self, database: str):
        """Get a connection context manager for the specified database"""
        return self.get_database(database).connection()

    def get_available_databases(self) -> list[str]:
        """Get available databases"""
        return [config.name for config in self.databases.values() if config.available]

    def get_available_databases_text(self) -> str:
        """Get formatted text of available databases for tool descriptions"""
        return "\n".join(self.get_available_databases())