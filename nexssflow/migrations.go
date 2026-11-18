package nexssflow

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const migrationTable = "nflow_schema_migrations"

type appliedMigration struct {
	Version   int64
	Name      string
	Checksum  string
	AppliedAt string
}

func migrateDatabase(ctx context.Context, database *sql.DB, declaration databaseDecl, direction string, targetVersion int64) (int, error) {
	if direction != "up" && direction != "down" {
		return 0, fmt.Errorf("db.migrate: direction must be up or down")
	}
	if targetVersion < 0 {
		return 0, fmt.Errorf("db.migrate: version cannot be negative")
	}

	tx, err := database.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if declaration.Driver == "postgres" {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", "nflow-migrations:"+declaration.Name); err != nil {
			return 0, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
		}
	}
	createTable := `CREATE TABLE IF NOT EXISTS "` + migrationTable + `" (
		"version" BIGINT PRIMARY KEY,
		"name" TEXT NOT NULL,
		"checksum" TEXT NOT NULL,
		"applied_at" TEXT NOT NULL
	)`
	if _, err := tx.ExecContext(ctx, createTable); err != nil {
		return 0, fmt.Errorf("create migration table: %w", err)
	}
	applied, err := readAppliedMigrations(ctx, tx)
	if err != nil {
		return 0, err
	}
	registered := make(map[int64]migrationDecl, len(declaration.Migrations))
	migrations := append([]migrationDecl(nil), declaration.Migrations...)
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	for _, migration := range migrations {
		if migration.Version < 1 || strings.TrimSpace(migration.Name) == "" {
			return 0, fmt.Errorf("migration has invalid version or name")
		}
		if _, duplicate := registered[migration.Version]; duplicate {
			return 0, fmt.Errorf("migration version %d is duplicated", migration.Version)
		}
		registered[migration.Version] = migration
	}
	for version, appliedMigration := range applied {
		migration, ok := registered[version]
		if !ok {
			return 0, fmt.Errorf("database has migration version %d (%s), but this version is not registered in NFlow", version, appliedMigration.Name)
		}
		if checksum(migration.Up) != appliedMigration.Checksum {
			return 0, fmt.Errorf("migration %d (%s) checksum changed after it was applied", version, migration.Name)
		}
	}

	changed := 0
	if direction == "up" {
		for _, migration := range migrations {
			if _, ok := applied[migration.Version]; ok {
				continue
			}
			if strings.TrimSpace(migration.Up) == "" || strings.Contains(strings.TrimSpace(migration.Up), ";") {
				return 0, fmt.Errorf("migration %d (%s) must contain exactly one SQL statement without a trailing semicolon", migration.Version, migration.Name)
			}
			if _, err := tx.ExecContext(ctx, migration.Up); err != nil {
				return 0, fmt.Errorf("apply migration %d (%s): %w", migration.Version, migration.Name, err)
			}
			_, err := tx.ExecContext(ctx,
				`INSERT INTO "`+migrationTable+`" ("version", "name", "checksum", "applied_at") VALUES (`+
					placeholder(declaration.Driver, 1)+`, `+placeholder(declaration.Driver, 2)+`, `+placeholder(declaration.Driver, 3)+`, `+placeholder(declaration.Driver, 4)+`)`,
				migration.Version, migration.Name, checksum(migration.Up), time.Now().UTC().Format(time.RFC3339Nano),
			)
			if err != nil {
				return 0, fmt.Errorf("record migration %d (%s): %w", migration.Version, migration.Name, err)
			}
			changed++
		}
	} else {
		if len(applied) == 0 {
			return 0, fmt.Errorf("no applied migrations to roll back")
		}
		versions := make([]int64, 0, len(applied))
		for version := range applied {
			versions = append(versions, version)
		}
		sort.Slice(versions, func(i, j int) bool { return versions[i] > versions[j] })
		latest := versions[0]
		if targetVersion != 0 && targetVersion != latest {
			return 0, fmt.Errorf("only the latest applied migration (%d) can be rolled back; requested %d", latest, targetVersion)
		}
		migration := registered[latest]
		if strings.TrimSpace(migration.Down) == "" || strings.Contains(strings.TrimSpace(migration.Down), ";") {
			return 0, fmt.Errorf("migration %d (%s) must contain exactly one down SQL statement without a trailing semicolon", migration.Version, migration.Name)
		}
		if _, err := tx.ExecContext(ctx, migration.Down); err != nil {
			return 0, fmt.Errorf("roll back migration %d (%s): %w", migration.Version, migration.Name, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM "`+migrationTable+`" WHERE "version" = `+placeholder(declaration.Driver, 1), latest); err != nil {
			return 0, fmt.Errorf("remove migration record %d: %w", latest, err)
		}
		changed = 1
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit migrations: %w", err)
	}
	return changed, nil
}

func readAppliedMigrations(ctx context.Context, tx *sql.Tx) (map[int64]appliedMigration, error) {
	rows, err := tx.QueryContext(ctx, `SELECT "version", "name", "checksum", "applied_at" FROM "`+migrationTable+`" ORDER BY "version"`)
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()
	applied := make(map[int64]appliedMigration)
	for rows.Next() {
		var migration appliedMigration
		if err := rows.Scan(&migration.Version, &migration.Name, &migration.Checksum, &migration.AppliedAt); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[migration.Version] = migration
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close applied migrations: %w", err)
	}
	return applied, nil
}

func migrationStatuses(ctx context.Context, database *sql.DB, declaration databaseDecl) ([]map[string]any, error) {
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin migration status transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if declaration.Driver == "postgres" {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", "nflow-migrations:"+declaration.Name); err != nil {
			return nil, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "`+migrationTable+`" (
		"version" BIGINT PRIMARY KEY,
		"name" TEXT NOT NULL,
		"checksum" TEXT NOT NULL,
		"applied_at" TEXT NOT NULL
	)`); err != nil {
		return nil, fmt.Errorf("create migration table: %w", err)
	}
	applied, err := readAppliedMigrations(ctx, tx)
	if err != nil {
		return nil, err
	}
	registered := make(map[int64]migrationDecl, len(declaration.Migrations))
	for _, migration := range declaration.Migrations {
		registered[migration.Version] = migration
	}
	versions := make([]int64, 0, len(applied)+len(registered))
	seen := make(map[int64]bool, len(applied)+len(registered))
	for version := range applied {
		versions = append(versions, version)
		seen[version] = true
	}
	for version := range registered {
		if !seen[version] {
			versions = append(versions, version)
		}
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	statuses := make([]map[string]any, 0, len(versions))
	for _, version := range versions {
		migration, exists := applied[version]
		if exists {
			status := "applied"
			if definition, ok := registered[version]; !ok {
				status = "unregistered"
			} else if checksum(definition.Up) != migration.Checksum {
				status = "checksum_mismatch"
			}
			statuses = append(statuses, map[string]any{
				"version": version, "name": migration.Name, "status": status,
				"applied_at": migration.AppliedAt,
			})
			continue
		}
		statuses = append(statuses, map[string]any{
			"version": version, "name": registered[version].Name, "status": "pending",
		})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit migration status transaction: %w", err)
	}
	return statuses, nil
}

func checksum(statement string) string {
	hash := sha256.Sum256([]byte(strings.TrimSpace(statement)))
	return hex.EncodeToString(hash[:])
}

func migrationDatabase(ctx context.Context, request map[string]any) (*registry, databaseDecl, *sql.DB, string, error) {
	name, ok := request["database"].(string)
	if !ok || strings.TrimSpace(name) == "" {
		return nil, databaseDecl{}, nil, "", fmt.Errorf("db.migrate: database alias is required")
	}
	store, err := registryFromContext(ctx)
	if err != nil {
		return nil, databaseDecl{}, nil, "", err
	}
	declaration, err := store.declaration(name)
	if err != nil {
		return nil, databaseDecl{}, nil, "", err
	}
	tenant, err := actionTenant(ctx, declaration, request, true)
	if err != nil {
		return nil, databaseDecl{}, nil, "", err
	}
	database, err := store.database(name, tenant)
	if err != nil {
		return nil, databaseDecl{}, nil, "", err
	}
	return store, declaration, database, tenant, nil
}

func migrateAction(ctx context.Context, request map[string]any) (map[string]any, error) {
	_, declaration, database, tenant, err := migrationDatabase(ctx, request)
	if err != nil {
		return nil, err
	}
	direction := "up"
	if value, exists := request["direction"]; exists {
		var ok bool
		direction, ok = value.(string)
		if !ok {
			return nil, fmt.Errorf("db.migrate: direction must be a string")
		}
		direction = strings.ToLower(strings.TrimSpace(direction))
	}
	version, err := optionalInt64(request["version"])
	if err != nil {
		return nil, fmt.Errorf("db.migrate: %w", err)
	}
	changed, err := migrateDatabase(ctx, database, declaration, direction, version)
	if err != nil {
		return nil, err
	}
	return map[string]any{"database": declaration.Name, "tenant": tenant, "direction": direction, "changed": changed}, nil
}

func migrationStatusAction(ctx context.Context, request map[string]any) (map[string]any, error) {
	_, declaration, database, tenant, err := migrationDatabase(ctx, request)
	if err != nil {
		return nil, err
	}
	statuses, err := migrationStatuses(ctx, database, declaration)
	if err != nil {
		return nil, err
	}
	return map[string]any{"database": declaration.Name, "tenant": tenant, "migrations": statuses}, nil
}

func optionalInt64(value any) (int64, error) {
	if value == nil {
		return 0, nil
	}
	switch n := value.(type) {
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		if !math.IsNaN(n) && !math.IsInf(n, 0) && math.Trunc(n) == n && n >= math.MinInt64 && n <= math.MaxInt64 {
			return int64(n), nil
		}
	case float32:
		f := float64(n)
		if !math.IsNaN(f) && !math.IsInf(f, 0) && math.Trunc(f) == f && f >= math.MinInt64 && f <= math.MaxInt64 {
			return int64(f), nil
		}
	case uint:
		if uint64(n) <= uint64(^uint64(0)>>1) {
			return int64(n), nil
		}
	case uint64:
		if n <= uint64(^uint64(0)>>1) {
			return int64(n), nil
		}
	}
	return 0, fmt.Errorf("version must be an integer")
}
