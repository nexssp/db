package nexssflow

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/xctx"
)

func TestPostgresPlaceholderRewrite(t *testing.T) {
	statement := `SELECT * FROM docs WHERE title = ? AND literal = '?' AND json ?? ? -- ?
AND note = "?" /* ? */ AND body = $tag$?$tag$ AND id = ?`
	want := `SELECT * FROM docs WHERE title = $1 AND literal = '?' AND json ? $2 -- ?
AND note = "?" /* ? */ AND body = $tag$?$tag$ AND id = $3`
	if got := rewritePlaceholders("postgres", statement); got != want {
		t.Fatalf("rewritePlaceholders() = %q, want %q", got, want)
	}
	if got := rewritePlaceholders("sqlite", "SELECT ?"); got != "SELECT ?" {
		t.Fatalf("SQLite statement unexpectedly changed: %q", got)
	}
}

func TestDatabaseAndMigrationDirectives(t *testing.T) {
	out := map[string]any{}
	databaseReq := compiler.DirectiveReq{
		Lines: []string{
			`@db sqlite as local {`,
			`  path: ":memory:"`,
			`  tenant_mode: "shared"`,
			`  migration_mode: "auto"`,
			`}`,
		},
		I:   0,
		Out: out,
	}
	if _, err := databaseDirective().Handler(context.Background(), databaseReq); err != nil {
		t.Fatalf("parse @db directive: %v", err)
	}
	declarations := declarationsFromMeta(out)
	if len(declarations) != 1 || declarations[0].TenantMode != tenantModeShared || declarations[0].TenantColumn != "tenant_id" || declarations[0].MigrationMode != migrationModeAuto {
		t.Fatalf("database declaration = %#v", declarations)
	}

	migrationReq := compiler.DirectiveReq{
		Lines: []string{
			`@migration local 1 create_notes {`,
			`  up: "CREATE TABLE notes (id INTEGER PRIMARY KEY)"`,
			`  down: "DROP TABLE notes"`,
			`}`,
		},
		I:   0,
		Out: out,
	}
	if _, err := migrationDirective().Handler(context.Background(), migrationReq); err != nil {
		t.Fatalf("parse @migration directive: %v", err)
	}
	declarations = declarationsFromMeta(out)
	if len(declarations[0].Migrations) != 1 || declarations[0].Migrations[0].Up != "CREATE TABLE notes (id INTEGER PRIMARY KEY)" {
		t.Fatalf("migration declaration = %#v", declarations[0].Migrations)
	}

	postgresReq := compiler.DirectiveReq{
		Lines: []string{
			`@db postgres as production {`,
			`  dsn: "${POSTGRES_URL}/{tenant}"`,
			`  tenant_mode: "database"`,
			`  migration_mode: "manual"`,
			`}`,
		},
		I:   0,
		Out: map[string]any{},
	}
	if _, err := databaseDirective().Handler(context.Background(), postgresReq); err != nil {
		t.Fatalf("parse per-tenant PostgreSQL @db: %v", err)
	}
	postgres := declarationsFromMeta(postgresReq.Out)[0]
	if postgres.Driver != "postgres" || postgres.TenantMode != tenantModeDatabase || !strings.Contains(postgres.DSN, tenantPlaceholder) {
		t.Fatalf("PostgreSQL declaration = %#v", postgres)
	}
}

func TestAutomaticAndManualMigrations(t *testing.T) {
	migration := migrationDecl{Version: 1, Name: "create_notes", Up: "CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL)", Down: "DROP TABLE notes"}
	manual := databaseDecl{Name: "manual", Driver: "sqlite", Path: ":memory:", MigrationMode: migrationModeManual, Migrations: []migrationDecl{migration}}
	store := newRegistry([]databaseDecl{manual})
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	status, err := migrationStatusAction(ctx, map[string]any{"database": "manual"})
	if err != nil {
		t.Fatalf("read manual migration status: %v", err)
	}
	statuses := status["migrations"].([]map[string]any)
	if len(statuses) != 1 || statuses[0]["status"] != "pending" {
		t.Fatalf("initial status = %#v, want one pending migration", statuses)
	}
	if _, err := query(ctx, map[string]any{"database": "manual", "query": "SELECT * FROM notes"}); err == nil {
		t.Fatal("manual migration unexpectedly ran on first database access")
	}
	result, err := migrateAction(ctx, map[string]any{"database": "manual"})
	if err != nil || result["changed"] != 1 {
		t.Fatalf("apply manual migration = %#v, %v; want one change", result, err)
	}
	result, err = migrateAction(ctx, map[string]any{"database": "manual"})
	if err != nil || result["changed"] != 0 {
		t.Fatalf("reapply manual migrations = %#v, %v; want no changes", result, err)
	}
	result, err = migrateAction(ctx, map[string]any{"database": "manual", "direction": "down"})
	if err != nil || result["changed"] != 1 {
		t.Fatalf("roll back latest migration = %#v, %v", result, err)
	}
	if _, err := query(ctx, map[string]any{"database": "manual", "query": "SELECT * FROM notes"}); err == nil {
		t.Fatal("down migration did not remove notes table")
	}

	automatic := databaseDecl{Name: "automatic", Driver: "sqlite", Path: ":memory:", MigrationMode: migrationModeAuto, Migrations: []migrationDecl{migration}}
	autoStore := newRegistry([]databaseDecl{automatic})
	autoCtx := withRegistry(context.Background(), autoStore)
	t.Cleanup(func() {
		if err := autoStore.close(context.Background()); err != nil {
			t.Errorf("close automatic registry: %v", err)
		}
	})
	if _, err := query(autoCtx, map[string]any{"database": "automatic", "query": "SELECT COUNT(*) AS n FROM notes"}); err != nil {
		t.Fatalf("automatic migration did not create notes table: %v", err)
	}
}

func TestPerTenantSQLiteDatabasesAreIsolated(t *testing.T) {
	root := t.TempDir()
	declaration := databaseDecl{
		Name: "tenant_db", Driver: "sqlite", Path: filepath.Join(root, "{tenant}.db"),
		TenantMode: tenantModeDatabase, MigrationMode: migrationModeAuto,
		Migrations: []migrationDecl{{
			Version: 1, Name: "create_tenant_meta",
			Up: "CREATE TABLE tenant_meta (value TEXT NOT NULL)", Down: "DROP TABLE tenant_meta",
		}},
	}
	store := newRegistry([]databaseDecl{declaration})
	base := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})
	alpha := xctx.WithTenantID(base, "alpha")
	if _, err := execute(alpha, map[string]any{"database": "tenant_db", "query": "CREATE TABLE items (value TEXT NOT NULL)"}); err != nil {
		t.Fatalf("create tenant table: %v", err)
	}
	if _, err := execute(alpha, map[string]any{"database": "tenant_db", "query": "INSERT INTO items(value) VALUES (?)", "args": []any{"alpha"}}); err != nil {
		t.Fatalf("insert alpha item: %v", err)
	}
	if _, err := query(alpha, map[string]any{"database": "tenant_db", "query": "SELECT COUNT(*) AS n FROM tenant_meta"}); err != nil {
		t.Fatalf("automatic migration missing for alpha: %v", err)
	}
	beta := xctx.WithTenantID(base, "beta")
	if _, err := query(beta, map[string]any{"database": "tenant_db", "query": "SELECT value FROM items"}); err == nil {
		t.Fatal("tenant beta unexpectedly read tenant alpha's database")
	}
	if _, err := query(beta, map[string]any{"database": "tenant_db", "query": "SELECT COUNT(*) AS n FROM tenant_meta"}); err != nil {
		t.Fatalf("automatic migration missing for beta: %v", err)
	}
	if _, err := query(base, map[string]any{"database": "tenant_db", "query": "SELECT value FROM items"}); err == nil {
		t.Fatal("per-tenant database action accepted a missing trusted tenant context")
	}
	if _, err := store.database("tenant_db", "../escape"); err == nil {
		t.Fatal("per-tenant database accepted a path-traversal tenant ID")
	}
}

func TestSharedTableActionsEnforceTenantScope(t *testing.T) {
	declaration := databaseDecl{
		Name: "shared", Driver: "sqlite", Path: ":memory:", TenantMode: tenantModeShared,
		TenantColumn: "tenant_id", MigrationMode: migrationModeManual,
	}
	mapping := testNoteMapping()
	mapping.Database = "shared"
	store := newRegistry([]databaseDecl{declaration}, mapping)
	base := withRegistry(context.Background(), store)
	database, err := store.database("shared")
	if err != nil {
		t.Fatalf("open shared database for fixture setup: %v", err)
	}
	if _, err := database.Exec(`CREATE TABLE notes (title TEXT NOT NULL, is_done INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 0, tenant_id TEXT NOT NULL)`); err != nil {
		t.Fatalf("create fixture table: %v", err)
	}
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	alpha := xctx.WithTenantID(base, "alpha")
	if _, err := insertMapped(alpha, map[string]any{"database": "shared", "schema": "NoteWrite", "record": map[string]any{"title": "First"}}); err != nil {
		t.Fatalf("insert alpha note: %v", err)
	}
	beta := xctx.WithTenantID(base, "beta")
	if _, err := insertMapped(beta, map[string]any{"database": "shared", "schema": "NoteWrite", "record": map[string]any{"title": "Second"}}); err != nil {
		t.Fatalf("insert beta note: %v", err)
	}
	alphaRows, err := findMapped(alpha, map[string]any{"database": "shared", "schema": "NoteWrite", "order_by": []any{map[string]any{"field": "title", "direction": "asc"}}})
	if err != nil {
		t.Fatalf("read alpha notes: %v", err)
	}
	rows := alphaRows["rows"].([]map[string]any)
	if len(rows) != 1 || rows[0]["title"] != "First" {
		t.Fatalf("alpha read = %#v; want only First", rows)
	}
	if _, err := updateMapped(alpha, map[string]any{
		"database": "shared", "schema": "NoteWrite",
		"values": map[string]any{"title": "Changed"}, "filters": map[string]any{"title": "First"},
	}); err != nil {
		t.Fatalf("tenant-scoped update: %v", err)
	}
	betaRows, err := findMapped(beta, map[string]any{"database": "shared", "schema": "NoteWrite"})
	if err != nil {
		t.Fatalf("read beta notes after alpha update: %v", err)
	}
	if got := betaRows["rows"].([]map[string]any)[0]["title"]; got != "Second" {
		t.Fatalf("alpha update changed beta row to %v", got)
	}
	if _, err := deleteMapped(alpha, map[string]any{"database": "shared", "schema": "NoteWrite", "filters": map[string]any{"title": "Changed"}}); err != nil {
		t.Fatalf("tenant-scoped delete: %v", err)
	}
	betaRows, err = findMapped(beta, map[string]any{"database": "shared", "schema": "NoteWrite"})
	if err != nil || betaRows["count"] != 1 {
		t.Fatalf("beta row after alpha delete = %#v, %v", betaRows, err)
	}
	if _, err := query(alpha, map[string]any{"database": "shared", "query": "SELECT title FROM notes"}); err == nil {
		t.Fatal("shared mode permitted raw SQL that can bypass tenant predicates")
	}
	if _, err := findMapped(alpha, map[string]any{"database": "shared", "schema": "NoteWrite", "tenant": "beta"}); err == nil {
		t.Fatal("shared mode accepted caller-selected tenant input")
	}
	if _, err := findMapped(base, map[string]any{"database": "shared", "schema": "NoteWrite"}); err == nil {
		t.Fatal("shared mode accepted a missing trusted tenant context")
	}
}

func TestPostgresDriverIsRegistered(t *testing.T) {
	for _, driver := range sql.Drivers() {
		if driver == "pgx" {
			return
		}
	}
	t.Fatal("database/sql PostgreSQL driver `pgx` is not registered")
}

func TestSQLiteHomePathIsExpandedBeforeOpen(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	path := filepath.Join(home, ".nexss", "db", "app.db")
	database, err := openDatabase(databaseDecl{Driver: "sqlite", Path: "~/.nexss/db/app.db"}, "")
	if err != nil {
		t.Fatalf("open SQLite home path: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQLite database: %v", err)
		}
	})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected SQLite file at expanded home path %q: %v", path, err)
	}
}
