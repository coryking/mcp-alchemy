# MCP Alchemy

**Status: Works great and is in daily use without any known bugs.**

> **Azure Support**: This fork adds seamless Azure CLI token authentication for Azure PostgreSQL Flexible Server. No password needed - just use `AZURE_TOKEN` in your connection string and let the server handle token management. [Learn more about Azure authentication](#azure-authentication).
>
> ```python
> # Example connection string with Azure CLI authentication
> DB_URL="postgresql://user@myserver:AZURE_TOKEN@myserver.postgres.database.azure.com/dbname"
> ```

Let Claude be your database expert! MCP Alchemy connects Claude Desktop directly to your databases, allowing it to:

- Help you explore and understand your database structure
- Assist in writing and validating SQL queries
- Displays relationships between tables
- Analyze large datasets and create reports
- Claude Desktop Can analyse and create artifacts for very large datasets using [claude-local-files](https://github.com/runekaagaard/claude-local-files)
- Supports Azure CLI token authentication for Azure PostgreSQL Flex servers ([learn more](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-azure-ad-authentication))

Works with PostgreSQL, MySQL, MariaDB, SQLite, Oracle, MS SQL Server and a host of other [SQLAlchemy-compatible](https://docs.sqlalchemy.org/en/20/dialects/) databases.

![MCP Alchemy in action](screenshot.png)

## API

### Tools

- **all_table_names**

  - Return all table names in the database
  - No input required
  - Returns comma-separated list of tables

  ```
  users, orders, products, categories
  ```

- **filter_table_names**

  - Find tables matching a substring
  - Input: `q` (string)
  - Returns matching table names

  ```
  Input: "user"
  Returns: "users, user_roles, user_permissions"
  ```

- **schema_definitions**

  - Get detailed schema for specified tables
  - Input: `table_names` (string[])
  - Returns table definitions including:
    - Column names and types
    - Primary keys
    - Foreign key relationships
    - Nullable flags

  ```
  users:
      id: INTEGER, primary key, autoincrement
      email: VARCHAR(255), nullable
      created_at: DATETIME

      Relationships:
        id -> orders.user_id
  ```

- **execute_query**

  - Execute SQL query with vertical output format
  - Inputs:
    - `query` (string): SQL query
    - `params` (object, optional): Query parameters
  - Returns results in clean vertical format:

  ```
  1. row
  id: 123
  name: John Doe
  created_at: 2024-03-15T14:30:00
  email: NULL

  Result: 1 rows
  ```

  - Features:
    - Smart truncation of large results
    - Full result set access via [claude-local-files](https://github.com/runekaagaard/claude-local-files) integration
    - Clean NULL value display
    - ISO formatted dates
    - Clear row separation

## Usage with Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "my_database": {
      "command": "uv",
      "args": ["--directory", "/path/to/mcp-alchemy", "run", "server.py"],
      "env": {
        "DB_URL": "mysql+pymysql://root:secret@localhost/databasename"
      }
    }
  }
}
```

Environment Variables:

- `DB_URL`: SQLAlchemy [database URL](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls) (required)
  Examples:
  - PostgreSQL: `postgresql://user:password@localhost/dbname`
  - MySQL: `mysql+pymysql://user:password@localhost/dbname`
  - MariaDB: `mariadb+pymysql://user:password@localhost/dbname`
  - SQLite: `sqlite:///path/to/database.db`
  - Azure PostgreSQL Flex with CLI auth: `postgresql://user@myserver:AZURE_TOKEN@myserver.postgres.database.azure.com/dbname`
    - The string `AZURE_TOKEN` will be automatically replaced with an Azure CLI token
    - Requires Azure CLI to be logged in (`az login`)
- `CLAUDE_LOCAL_FILES_PATH`: Directory for full result sets (optional)
- `EXECUTE_QUERY_MAX_CHARS`: Maximum output length (optional, default 4000)

## Installation

1. Clone repository:

```bash
git clone https://github.com/runekaagaard/mcp-alchemy.git
```

2. Ensure you have uv

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Add database to claude_desktop_config.json (see above)

## Database Drivers

The following database drivers are included by default:

- SQLite: Built into Python, no additional installation needed
- MySQL/MariaDB: Via `pymysql`
- PostgreSQL: Via `psycopg2-binary`

To use other databases supported by SQLAlchemy, install the appropriate driver:

```bash
# Microsoft SQL Server
uv pip install pymssql

# Oracle
uv pip install cx_oracle

# Other databases
# See: https://docs.sqlalchemy.org/en/20/dialects/
```

## Azure Authentication

MCP Alchemy supports Azure CLI token-based authentication, tested with Azure PostgreSQL Flexible Server. While the token authentication mechanism might work with other Azure database services, it has only been verified with PostgreSQL Flex.

To use Azure authentication:

1. Ensure you're logged in with Azure CLI (`az login`)
2. Use the special `AZURE_TOKEN` placeholder in your connection string:

```
postgresql://user@myserver:AZURE_TOKEN@myserver.postgres.database.azure.com/dbname
```

The `AZURE_TOKEN` string will be automatically replaced with a valid Azure access token. Token management (including refresh) is handled automatically.

For more details about Azure authentication with PostgreSQL Flex Server, see the [Microsoft documentation](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-azure-ad-authentication).

## Claude Local Files

When [claude-local-files](https://github.com/runekaagaard/claude-local-files) is configured:

- Access complete result sets beyond Claude's context window
- Generate detailed reports and visualizations
- Perform deep analysis on large datasets
- Export results for further processing

The integration automatically activates when `CLAUDE_LOCAL_FILES_PATH` is set.

## Contributing

Contributions are warmly welcomed! Whether it's bug reports, feature requests, documentation improvements, or code contributions - all input is valuable. Feel free to:

- Open an issue to report bugs or suggest features
- Submit pull requests with improvements
- Enhance documentation or share your usage examples
- Ask questions and share your experiences

The goal is to make database interaction with Claude even better, and your insights and contributions help achieve that.

## License

Mozilla Public License Version 2.0
