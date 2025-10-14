from dataclasses import dataclass, field
import os
import logging
from typing import Any
from sqlalchemy.engine import Engine, Connection, Result
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

def create_engine_for_config(config: "DatabaseConfig") -> Engine:
    """Create engine with MCP-optimized settings for a specific database config"""
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

    from sqlalchemy import create_engine
    return create_engine(config.get_resolved_url(), **options)


@dataclass
class DatabaseConfig:
    name: str
    url: str
    description: str = ""
    available: bool = True
    engine: Engine | None = None

    def get_resolved_url(self) -> str:
        """Get connection URL with AZURE_TOKEN replaced if present"""
        if 'AZURE_TOKEN' in self.url:
            return self.url.replace('AZURE_TOKEN', token_cache.get_token())
        return self.url

    def get_engine(self) -> Engine:
        """Get or create the SQLAlchemy engine for this database"""
        if self.engine is None:
            self.engine = create_engine_for_config(self)
        return self.engine

    def mark_unavailable(self) -> None:
        """Mark this database as unavailable"""
        self.available = False
        if self.engine:
            try:
                self.engine.dispose()
            except Exception:
                pass
        self.engine = None

    def get_connection(self) -> Connection:
        """Get a working database connection with retry logic"""
        if not self.available:
            raise ValueError(f"Database '{self.name}' is not available")

        try:
            return self._get_connection_attempt()
        except Exception as e:
            logger.warning(f"Connection failed for '{self.name}', retrying: {e}")
            self._reset_for_retry()
            return self._get_connection_attempt()

    def _get_connection_attempt(self) -> Connection:
        """Single connection attempt"""
        from sqlalchemy import text

        engine = self.get_engine()
        connection = engine.connect()

        # Set version variable for databases that support it
        try:
            # Avoid circular import by hardcoding version or getting it differently
            connection.execute(text("SET @mcp_alchemy_version = '2025.8.15.91819'"))
        except Exception:
            # Some databases don't support session variables
            pass

        return connection

    def _reset_for_retry(self) -> None:
        """Reset state for retry attempt"""
        self.mark_unavailable()
        self.engine = None  # Force recreation


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

                manager.databases[db_name] = DatabaseConfig(
                    name=db_name,
                    url=value,
                    description=description
                )

        # Backwards compatibility: if no DB_*_URL vars, try DB_URL
        if not manager.databases:
            if 'DB_URL' in os.environ:
                manager.databases['default'] = DatabaseConfig(
                    name='default',
                    url=os.environ['DB_URL'],
                    description='Default database'
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

    def get_available_databases_text(self) -> str:
        """Get formatted text of available databases for tool descriptions"""
        available_dbs = []
        for config in self.databases.values():
            if config.available:
                desc = f" ({config.description})" if config.description else ""
                available_dbs.append(f"{config.name}{desc}")
        return "Available databases: " + ", ".join(available_dbs)

    def get_connection(self, database: str) -> Connection:
        """Get connection for database (case insensitive)"""
        config = self.get_database(database)
        return config.get_connection()

    def get_db_info(self, database: str) -> str:
        """Get database info for a specific database"""
        with self.get_connection(database) as conn:
            engine = conn.engine
            url = engine.url
            version_info = engine.dialect.server_version_info
            version_str = '.'.join(str(x) for x in version_info) if version_info else "unknown"

            result = [
                f"Connected to {engine.dialect.name}",
                f"version {version_str}",
                f"database {url.database}",
            ]

            if url.host:
                result.append(f"on {url.host}")

            if url.username:
                result.append(f"as user {url.username}")

            return " ".join(result) + "."