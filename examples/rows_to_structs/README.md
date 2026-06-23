# Description

This is a simple example demonstrating how to use `pgx.RowsToStructs` to directly
scan query results into a slice of structs. It showcases:

- Case-insensitive column matching
- `db` struct tags with modifiers (e.g., `db:"column_name,omitempty"`)
- NULL safety checks for non-nullable fields
- Embedded struct expansion

## Connection configuration

The database connection is configured via DATABASE_URL and standard PostgreSQL environment variables (PGHOST, PGUSER, etc.)

You can either export them then run the example:

    export DATABASE_URL="postgres://localhost:5432/postgres"
    go run main.go

Or you can prefix the execution with the environment variables:

    DATABASE_URL="postgres://localhost:5432/postgres" go run main.go
