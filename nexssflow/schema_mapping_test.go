package nexssflow

import (
	"context"
	"testing"

	"github.com/nexssp/flow/compiler"
	flowschema "github.com/nexssp/flow/extensions/schema"
)

func TestMappedInsertAppliesSchemaAndSQLiteDefaults(t *testing.T) {
	mapping := testNoteMapping()
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}}, mapping)
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	if _, err := execute(ctx, map[string]any{
		"database": "local",
		"query":    "CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL, is_done INTEGER NOT NULL DEFAULT 1, priority INTEGER NOT NULL DEFAULT 9)",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	inserted, err := insertMapped(ctx, map[string]any{
		"database": "local",
		"schema":   "NoteWrite",
		"record":   map[string]any{"title": "First"},
	})
	if err != nil {
		t.Fatalf("insert mapped row: %v", err)
	}
	if got := inserted["rows_affected"]; got != int64(1) {
		t.Fatalf("rows_affected = %v, want 1", got)
	}

	queried, err := query(ctx, map[string]any{
		"database": "local",
		"query":    "SELECT title, is_done, priority FROM notes",
	})
	if err != nil {
		t.Fatalf("query mapped row: %v", err)
	}
	rows, ok := queried["rows"].([]map[string]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %#v, want one row", queried["rows"])
	}
	if got := rows[0]["title"]; got != "First" {
		t.Fatalf("title = %#v, want First", got)
	}
	if got := rows[0]["is_done"]; got != int64(0) {
		t.Fatalf("is_done = %#v, want 0 from NFlow default false", got)
	}
	if got := rows[0]["priority"]; got != int64(9) {
		t.Fatalf("priority = %#v, want SQLite default 9 for omitted field", got)
	}

	_, err = insertMapped(ctx, map[string]any{
		"database": "local",
		"schema":   "NoteWrite",
		"record":   map[string]any{"title": "Ignored", "unexpected": true},
	})
	if err == nil {
		t.Fatal("insert accepted an undeclared record field")
	}
	queried, err = query(ctx, map[string]any{"database": "local", "query": "SELECT COUNT(*) AS count FROM notes"})
	if err != nil {
		t.Fatalf("count rows after rejected insert: %v", err)
	}
	rows = queried["rows"].([]map[string]any)
	if got := rows[0]["count"]; got != int64(1) {
		t.Fatalf("row count after rejected insert = %#v, want 1", got)
	}
}

func TestMappedQueryReturnsTypedSchemaRows(t *testing.T) {
	store := newRegistry([]databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}}, testNoteMapping())
	ctx := withRegistry(context.Background(), store)
	t.Cleanup(func() {
		if err := store.close(context.Background()); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})

	if _, err := execute(ctx, map[string]any{
		"database": "local",
		"query":    "CREATE TABLE notes (title TEXT NOT NULL, is_done INTEGER NOT NULL, priority INTEGER NOT NULL)",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, args := range [][]any{{"First", 0, 9}, {"Second", 1, 2}} {
		if _, err := execute(ctx, map[string]any{
			"database": "local",
			"query":    "INSERT INTO notes (title, is_done, priority) VALUES (?, ?, ?)",
			"args":     args,
		}); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	queried, err := queryMapped(ctx, map[string]any{
		"database": "local",
		"schema":   "NoteWrite",
		"query":    "SELECT title, is_done, priority FROM notes ORDER BY priority",
	})
	if err != nil {
		t.Fatalf("query mapped rows: %v", err)
	}
	rows, ok := queried["rows"].([]map[string]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("rows = %#v, want two rows", queried["rows"])
	}
	if got := rows[0]["title"]; got != "Second" {
		t.Fatalf("first title = %#v, want Second", got)
	}
	if got := rows[0]["completed"]; got != true {
		t.Fatalf("first completed = %#v, want true", got)
	}
	if got := rows[1]["completed"]; got != false {
		t.Fatalf("second completed = %#v, want false", got)
	}
	if got := rows[1]["priority"]; got != int64(9) {
		t.Fatalf("second priority = %#v, want int64(9)", got)
	}
	if got := queried["count"]; got != 2 {
		t.Fatalf("count = %#v, want 2", got)
	}

	_, err = queryMapped(ctx, map[string]any{
		"database": "local",
		"schema":   "NoteWrite",
		"query":    "SELECT title, is_done FROM notes",
	})
	if err == nil {
		t.Fatal("mapped query accepted an incomplete column set")
	}
}

func TestDatabaseMappingDirectiveRequiresExplicitColumnTags(t *testing.T) {
	directive := databaseMappingDirective()
	_, err := directive.Handler(context.Background(), compiler.DirectiveReq{
		Lines: []string{"@dbmap local NoteWrite as notes"},
		I:     0,
		Out: map[string]any{
			declarationsMetaKey: []databaseDecl{{Name: "local", Driver: "sqlite", Path: ":memory:"}},
			"schemas": []flowschema.Schema{{
				Name: "NoteWrite",
				Fields: []flowschema.Field{{
					Name:     "Title",
					JSONName: "title",
					Type:     "string",
					Kind:     flowschema.KindString,
				}},
			}},
		},
	})
	if err == nil {
		t.Fatal("@dbmap accepted a schema field without an explicit db tag")
	}
}

func testNoteMapping() tableMapping {
	return tableMapping{
		Database: "local",
		Table:    "notes",
		Schema: flowschema.Schema{
			Name: "NoteWrite",
			Fields: []flowschema.Field{
				{
					Name:     "Title",
					JSONName: "title",
					Type:     "string",
					Kind:     flowschema.KindString,
					Tags:     map[string]string{"db": "title", "validate": "required"},
				},
				{
					Name:            "Completed",
					JSONName:        "completed",
					Type:            "bool",
					Kind:            flowschema.KindBool,
					Tags:            map[string]string{"db": "is_done", "default": "false"},
					TypedDefault:    false,
					HasTypedDefault: true,
				},
				{
					Name:     "Priority",
					JSONName: "priority",
					Type:     "int",
					Kind:     flowschema.KindInt,
					Tags:     map[string]string{"db": "priority"},
				},
			},
		},
		Columns: []mappedColumn{
			{JSONName: "title", Name: "title"},
			{JSONName: "completed", Name: "is_done"},
			{JSONName: "priority", Name: "priority"},
		},
	}
}
