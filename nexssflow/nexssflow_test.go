package nexssflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	flowschema "github.com/nexssp/flow/extensions/schema"
	"github.com/nexssp/kernel/xerr"
)

func TestParameterizedExecAndQuery(t *testing.T) {
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}})
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	if _, err := execute(ctx, map[string]any{
		"database": "local",
		"query":    "CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL)",
		"args":     []any{},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	inserted, err := execute(ctx, map[string]any{
		"database": "local",
		"query":    "INSERT INTO notes (title) VALUES (?)",
		"args":     []any{"First"},
	})
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if got := inserted["rows_affected"]; got != int64(1) {
		t.Fatalf("rows_affected = %v, want 1", got)
	}

	queried, err := query(ctx, map[string]any{
		"database": "local",
		"query":    "SELECT id, title FROM notes WHERE title = ?",
		"args":     []string{"First"},
	})
	if err != nil {
		t.Fatalf("query row: %v", err)
	}
	rows, ok := queried["rows"].([]map[string]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %#v, want one row", queried["rows"])
	}
	if got := rows[0]["title"]; got != "First" {
		t.Fatalf("title = %#v, want First", got)
	}
	if got := rows[0]["id"]; got != int64(1) {
		t.Fatalf("id = %#v, want int64(1)", got)
	}
}

func TestTransactionCommitsAndRollsBackAtomically(t *testing.T) {
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}})
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})
	for _, statement := range []string{
		"CREATE TABLE ssh_commands (id TEXT PRIMARY KEY)",
		"CREATE TABLE ssh_command_undo (id INTEGER PRIMARY KEY)",
		"INSERT INTO ssh_commands (id) VALUES ('custom')",
		"INSERT INTO ssh_command_undo (id) VALUES (1)",
	} {
		if _, err := execute(ctx, map[string]any{"database": "local", "query": statement}); err != nil {
			t.Fatalf("execute setup statement %q: %v", statement, err)
		}
	}

	steps := []any{
		map[string]any{"query": "DELETE FROM ssh_commands WHERE id = ?", "args": []any{"custom"}},
		map[string]any{"query": "DELETE FROM ssh_command_undo WHERE id = 1", "args": []any{}},
		map[string]any{"query": "DELETE FROM missing_table WHERE id = 1", "args": []any{}},
	}
	_, err := executeTransaction(ctx, map[string]any{
		"database":      "local",
		"steps":         steps,
		"error_message": "Could not delete SSH command",
	})
	if err == nil {
		t.Fatal("transaction accepted a failing SQL step")
	}
	appErr, ok := err.(*xerr.AppError)
	if !ok || appErr.Kind != xerr.KindInternal || appErr.Message != "Could not delete SSH command" {
		t.Fatalf("transaction error = %#v, want Internal / Could not delete SSH command", err)
	}
	if appErr.Cause == nil || !strings.Contains(appErr.Cause.Error(), "db.transaction") {
		t.Fatalf("transaction cause = %v, want the wrapped transaction error", appErr.Cause)
	}
	assertRowCount := func(table string, want int64) {
		t.Helper()
		result, err := query(ctx, map[string]any{
			"database": "local",
			"query":    "SELECT COUNT(*) AS count FROM " + table,
		})
		if err != nil {
			t.Fatalf("count %s rows: %v", table, err)
		}
		rows := result["rows"].([]map[string]any)
		if got := rows[0]["count"]; got != want {
			t.Fatalf("%s row count = %v, want %d", table, got, want)
		}
	}
	assertRowCount("ssh_commands", 1)
	assertRowCount("ssh_command_undo", 1)

	committed, err := executeTransaction(ctx, map[string]any{
		"database": "local",
		"steps":    steps[:2],
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	results, ok := committed["results"].([]map[string]any)
	if !ok || len(results) != 2 || results[0]["rows_affected"] != int64(1) || results[1]["rows_affected"] != int64(1) {
		t.Fatalf("transaction results = %#v, want two statements affecting one row each", committed["results"])
	}
	assertRowCount("ssh_commands", 0)
	assertRowCount("ssh_command_undo", 0)
}

func TestQueryErrorMessagePreservesCause(t *testing.T) {
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}})
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	_, err := query(ctx, map[string]any{
		"database":      "local",
		"query":         "SELECT theme FROM missing_app_settings",
		"args":          []any{},
		"error_message": "Could not load app theme",
	})
	if err == nil {
		t.Fatal("query accepted a missing table")
	}
	appErr, ok := err.(*xerr.AppError)
	if !ok || appErr.Kind != xerr.KindInternal || appErr.Message != "Could not load app theme" {
		t.Fatalf("query error = %#v, want Internal / Could not load app theme", err)
	}
	if appErr.Cause == nil || !strings.Contains(appErr.Cause.Error(), "db.query") {
		t.Fatalf("query cause = %v, want the wrapped database query error", appErr.Cause)
	}
}

func TestQueryMappedErrorMessagePreservesCause(t *testing.T) {
	mapping := tableMapping{
		Database: "local",
		Schema: flowschema.Schema{
			Name: "SSHCommand",
			Fields: []flowschema.Field{{
				Name:     "ID",
				JSONName: "id",
				Type:     "string",
				Kind:     flowschema.KindString,
			}},
		},
		Table:   "ssh_commands",
		Columns: []mappedColumn{{JSONName: "id", Name: "id"}},
	}
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}}, mapping)
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	_, err := queryMapped(ctx, map[string]any{
		"database":      "local",
		"schema":        "SSHCommand",
		"query":         "SELECT id FROM missing_ssh_commands",
		"args":          []any{},
		"error_message": "Could not load SSH commands",
	})
	if err == nil {
		t.Fatal("queryMapped accepted a missing table")
	}
	appErr, ok := err.(*xerr.AppError)
	if !ok {
		t.Fatalf("queryMapped error = %T, want *xerr.AppError: %v", err, err)
	}
	if appErr.Kind != xerr.KindInternal || appErr.Message != "Could not load SSH commands" {
		t.Fatalf("queryMapped AppError = %#v, want Internal / Could not load SSH commands", appErr)
	}
	if appErr.Cause == nil || !strings.Contains(appErr.Cause.Error(), "db.query") {
		t.Fatalf("queryMapped cause = %v, want the wrapped database query error", appErr.Cause)
	}
	public := appErr.Public("")
	if public.Error != string(xerr.KindInternal) || public.Message != "internal server error" {
		t.Fatalf("public error = %#v, want the existing Internal response", public)
	}
}

func TestExecErrorMessagePreservesCause(t *testing.T) {
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}})
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	_, err := execute(ctx, map[string]any{
		"database":      "local",
		"query":         "UPDATE missing_ssh_commands SET use_count = use_count + 1 WHERE id = ?",
		"args":          []any{"missing"},
		"error_message": "Could not update command usage",
	})
	if err == nil {
		t.Fatal("exec accepted a missing table")
	}
	appErr, ok := err.(*xerr.AppError)
	if !ok {
		t.Fatalf("exec error = %T, want *xerr.AppError: %v", err, err)
	}
	if appErr.Kind != xerr.KindInternal || appErr.Message != "Could not update command usage" {
		t.Fatalf("exec AppError = %#v, want Internal / Could not update command usage", appErr)
	}
	if appErr.Cause == nil || !strings.Contains(appErr.Cause.Error(), "db.exec") {
		t.Fatalf("exec cause = %v, want the wrapped database execution error", appErr.Cause)
	}
	public := appErr.Public("")
	if public.Error != string(xerr.KindInternal) || public.Message != "internal server error" {
		t.Fatalf("public error = %#v, want the existing Internal response", public)
	}
}

func TestBundleInheritsDatabaseRegistryIntoChildScope(t *testing.T) {
	t.Parallel()

	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}})
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	parentMeta := map[string]any{registryMetaKey: store}
	childMeta := make(map[string]any)
	inherit := Bundle(nil).InheritScope
	if inherit == nil {
		t.Fatal("DB bundle did not register scope inheritance")
	}
	if err := inherit(parentMeta, childMeta); err != nil {
		t.Fatalf("inherit DB registry: %v", err)
	}
	if got := registryFromMeta(childMeta); got != store {
		t.Fatalf("child registry = %p, want shared registry %p", got, store)
	}
	if got := registryFromMeta(parentMeta); got != store {
		t.Fatalf("parent registry = %p, want unchanged registry %p", got, store)
	}
}

func TestQueryRejectsNonArrayArgs(t *testing.T) {
	_, err := queryArgs("not-an-array")
	if err == nil {
		t.Fatal("queryArgs accepted a scalar")
	}
}

func TestQueryCapsMaterializedRows(t *testing.T) {
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}})
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	statement := fmt.Sprintf(`WITH RECURSIVE valueset(value) AS (
		VALUES(1) UNION ALL SELECT value + 1 FROM valueset WHERE value < %d
	) SELECT value FROM valueset`, maxQueryRows+1)
	_, err := query(ctx, map[string]any{"database": "local", "query": statement})
	if err == nil {
		t.Fatal("query accepted a result above the row cap")
	}
}

func TestResolveDatabasePathExpandsUserHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("get home directory: %v", err)
	}

	got, err := resolveDatabasePath("~/.nexss/db/database.db")
	if err != nil {
		t.Fatalf("resolve database path: %v", err)
	}
	want := filepath.Join(home, ".nexss", "db", "database.db")
	if got != want {
		t.Fatalf("resolved path = %q, want %q", got, want)
	}
}

func TestResolveDatabasePathPreservesMemoryDSN(t *testing.T) {
	got, err := resolveDatabasePath(":memory:")
	if err != nil {
		t.Fatalf("resolve memory DSN: %v", err)
	}
	if got != ":memory:" {
		t.Fatalf("resolved DSN = %q, want :memory:", got)
	}
}
