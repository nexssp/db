# NFlow database bridge

This local NFlow bundle provides a switchable SQL layer for SQLite and PostgreSQL using Go's `database/sql`. Declare a named database in `.nflow` source and bind pipelines to that alias. Pools open lazily and close with the NFlow host. The included PostgreSQL driver is `pgx` through `database/sql`; SQLite uses `modernc.org/sqlite`.

## Choose a backend

A provider switch is made in one `@db` declaration. Keep the alias (for example, `app`) stable so the pipelines using it do not need provider-specific changes.

```nflow
@require ./nexssflow

# Development: local SQLite file
@db sqlite as app {
  path: "./data/app.sqlite"
  migration_mode: "auto"
}
```

```nflow
@require ./nexssflow

# Production: PostgreSQL, connection string kept in an environment variable
@db postgres as app {
  dsn: "${DATABASE_URL}"
  migration_mode: "manual"
  max_open_conns: 20
  max_idle_conns: 5
}
```

`sqlite` requires `path`; `postgres` requires `dsn`. Options are `tenant_mode`, `tenant_column`, `migration_mode`, `max_open_conns`, and `max_idle_conns` as shown below. `migration_mode` defaults to `manual`; tenant mode defaults to `none`. A PostgreSQL server is not bundled, so the configured server must be reachable at runtime.

SQL actions use portable `?` bind markers for both providers. The PostgreSQL dialect rewrites them to `$1`, `$2`, and so on, ignoring question marks inside SQL strings, comments, quoted identifiers, and dollar-quoted text. In PostgreSQL SQL, write `??` for a literal JSON question-mark operator. Prefer portable SQL in shared flows; provider-specific migration SQL is available when DDL differs.

## Tenant isolation

There are three modes:

- **`none`** (default): one database, no tenant routing.
- **`database`**: one SQLite file or PostgreSQL database per tenant. Put `{tenant}` in the SQLite path or PostgreSQL DSN; each verified tenant gets a separate handle and independent migration history.
- **`shared`**: one SQLite or PostgreSQL database, with every mapped row stored under a tenant column (default `tenant_id`). The tenant column must exist in each mapped table and is injected by the bridge, not by the request.

### Database per tenant

```nflow
@db sqlite as app {
  path: "./data/tenant-{tenant}.sqlite"
  tenant_mode: "database"
  migration_mode: "auto"
}
```

```nflow
@db postgres as app {
  dsn: "postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db.example:5432/{tenant}?sslmode=require"
  tenant_mode: "database"
  migration_mode: "manual"
}
```

Use deployment secrets for credentials rather than committing them in `.nflow` source. Tenant IDs are validated before they can affect a path or DSN. The bridge obtains the tenant only from the trusted per-request context exposed by the Kernel; it rejects a caller-supplied `tenant` action field. In the host, authenticate and authorize first, then derive context from verified claims, for example:

```go
ctx = xctx.WithTenantID(r.Context(), verifiedClaims.TenantID)
next.ServeHTTP(w, r.WithContext(ctx))
```

Do not trust a public request body, query parameter, or `X-Tenant-ID` header as the tenant authority. This project bundle does not add authentication or decide which tenant a user may access; the host must establish that trusted context. Without it, tenant-mode data actions fail closed.

### Shared database with tenant column

```nflow
@db postgres as app {
  dsn: "${DATABASE_URL}"
  tenant_mode: "shared"
  tenant_column: "tenant_id"
  migration_mode: "manual"
}
```

Map the business fields, but leave `tenant_id` out of the NFlow write schema. `db.insert` adds the verified tenant automatically. `db.find`, `db.update`, and `db.delete` add their own tenant predicate; their `filters` cannot override it. `@dbmap` rejects mapping a schema field to the reserved tenant column. Raw SQL actions (`db.exec`, `db.query`, `db.transaction`, and `db.query_mapped`) are disabled for shared mode because arbitrary SQL cannot be safely tenant-scoped by this bridge. Keep privileged maintenance/migration pipelines out of caller-controlled routes.

Shared mode works on **both SQLite and PostgreSQL**; it is not limited to PostgreSQL. The migration must create a tenant column of a type that fits the tenant IDs, and every business table used by mapped CRUD must include it. Add indexes/unique constraints that include `tenant_id` where appropriate.

## Schema mapping and CRUD

The schema parser preserves field tags, so persistence is opt-in through `db:"column"` and `@dbmap`:

```nflow
@schema NoteWrite {
  Title string `json:"title" db:"title" validate:"required"`
  Completed bool `json:"completed" db:"is_done" default:"false"`
  Priority int `json:"priority" db:"priority"`
}
@dbmap app NoteWrite as notes

@pipeline create_note
  :request-schema=NoteWrite
  db.insert @{ database: "app", schema: "NoteWrite", record: . }
@end

@pipeline list_notes
  db.find @{ database: "app", schema: "NoteWrite", filters: { completed: false }, order_by: [{ field: "priority", direction: "desc" }], limit: 100 }
@end
```

`@dbmap <database> <schema> as <table>` must follow `@db` and the schema. Every field needs `db:"column"` or `db:"-"`; mapped values are scalar strings, numbers, or booleans, and table/column names are restricted to simple ASCII SQL identifiers. Use a narrow write schema for API data that contains read-only fields.

Actions are:

- `db.insert`: validates and applies NFlow defaults, rejects undeclared fields, and binds values. Missing fields without NFlow defaults are omitted so a SQL column default can apply; explicit nullable values are bound as `NULL`.
- `db.find`: reads mapped rows. Optional `filters` is an equality-match object, `order_by` is an array of `{field, direction}` items, and `limit` must be between 1 and 10,000. If omitted, limit defaults to 10,000.
- `db.update`: requires non-empty `values` and `filters` objects; only mapped fields are accepted.
- `db.delete`: requires non-empty `filters`; it will not perform an unbounded delete.
- `db.query_mapped`: for trusted non-shared SQL, maps a parameterized query into schema JSON names/types; the query must return every mapped column exactly once with its DB column name.

Mapped reads return `{ rows, count }`, cap materialized results at 10,000, and reject missing, extra, or duplicate columns. `db.exec`, `db.query`, and `db.transaction` accept parameterized SQL and remain available in `none` and `database` modes. They and `db.query_mapped` accept an optional non-empty `error_message` to return a stable internal error while retaining the database failure as its cause. Do not interpolate external values into query text.

## Migrations

Declare each version once after its database. Migrations must contain one SQL statement per direction; split multi-statement changes into multiple ordered versions. A migration is checksummed, recorded in `nflow_schema_migrations`, and applied transactionally where the backend supports transactional DDL.

```nflow
@migration app 1 create_notes {
  sqlite_up: "CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL)"
  sqlite_down: "DROP TABLE notes"
  postgres_up: "CREATE TABLE notes (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, title TEXT NOT NULL)"
  postgres_down: "DROP TABLE notes"
}
```

When the migration SQL differs by backend, provide common `up`/`down`, or the selected pair `sqlite_up`/`sqlite_down` and `postgres_up`/`postgres_down`. `@migration` selects the pair matching its `@db` declaration at compile time.

- **Development:** set `migration_mode: "auto"` to apply pending versions on the first database open. In database-per-tenant mode, this happens independently in each tenant database.
- **Production:** keep the default `migration_mode: "manual"`, run the privileged `db.migrate` action before serving traffic, and then check with `db.migration_status`. Manual mode never silently changes a production schema on normal data access.
- `db.migrate` applies pending migrations by default. With `direction: "down"`, it rolls back only the latest applied migration (the optional `version` must match that latest version). Migration SQL changes after application are reported as checksum mismatches; applied migrations are not silently rewritten.
- Run per-tenant manual migrations inside a trusted context for that tenant. The bridge does not enumerate tenants from an identity provider or expose a bypass that takes a tenant ID from request input.

## Current application boundary

The built-in NexssTerm app store uses a runtime-selected SQLite or PostgreSQL connection for its profiles, settings, SSH commands and launch history. The theme API's GET/PUT workflows use this bundle as a focused integration, selecting the same configured app database; other persistence actions remain host-owned. App-store configuration and its manual migration command are documented in the project README. This desktop store remains single-tenant: the bundle's per-tenant and shared-tenant modes require a trusted tenant context, which the app does not currently have.

## Verification

Install [Task 3.x](https://taskfile.dev/) and [golangci-lint v2.14.0](https://golangci-lint.run/) first. The Taskfile is the source of truth for local checks and the GitHub Actions workflows:

```sh
task --list
task test
task lint
task lint:nflow
task test:nflow
task pre-push
```

`task ci` runs the full Go quality gate and NFlow specification gate. GitHub Actions runs those gates as separate workflows. The `spec/migrations/main.nflow` fixture demonstrates automatic SQLite migrations and mapped CRUD. Go tests cover PostgreSQL parameter rewriting/driver registration, tenant routing and isolation, shared-tenant inserts/reads/updates/deletes, and migration up/down behavior.
