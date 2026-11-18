package nexssflow

import (
	"context"
	"fmt"
	"go/token"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/nexssp/flow/compiler"
)

const declarationsMetaKey = "db.declarations"

func databaseDirective() compiler.Directive {
	return compiler.Directive{
		Name:    "db",
		Example: `@db sqlite as app { path: "./app.db" tenant_mode: "shared" tenant_column: "tenant_id" migration_mode: "auto" }`,
		Handler: func(_ context.Context, req compiler.DirectiveReq) (compiler.DirectiveRes, error) {
			line := strings.TrimSpace(req.Lines[req.I])
			rest := strings.TrimSpace(strings.TrimPrefix(line, "@db"))
			pos := compiler.Position{File: req.File, Line: req.I + 1}
			header, hasBlock := splitHeader(rest)
			if !hasBlock {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db: expected `sqlite|postgres as <alias> { path|dsn: value }`")
			}
			parts := strings.Fields(header)
			if len(parts) != 3 || parts[1] != "as" {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db: expected `sqlite|postgres as <alias> { path|dsn: value }`")
			}
			driver, name := parts[0], parts[2]
			if driver != "sqlite" && driver != "postgres" {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db: unsupported driver %q (supported: sqlite, postgres)", driver)
			}
			if !token.IsIdentifier(name) {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db: invalid alias %q", name)
			}

			options, next, err := compiler.ParseBlockOptions(req.Lines, req.I)
			if err != nil {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: %v", name, err)
			}
			allowed := map[string]bool{
				"path": true, "dsn": true, "tenant_mode": true, "tenant_column": true,
				"migration_mode": true, "max_open_conns": true, "max_idle_conns": true,
			}
			for _, key := range slices.Sorted(maps.Keys(options)) {
				if !allowed[key] {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: unknown option %q", name, key)
				}
			}
			path, dsn := strings.TrimSpace(options["path"]), strings.TrimSpace(options["dsn"])
			if driver == "sqlite" && (path == "" || dsn != "") {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, `@db %s: sqlite requires path and does not accept dsn`, name)
			}
			if driver == "postgres" && (dsn == "" || path != "") {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, `@db %s: postgres requires dsn and does not accept path`, name)
			}

			tenantMode := strings.TrimSpace(options["tenant_mode"])
			if tenantMode == "" {
				tenantMode = tenantModeNone
			}
			if tenantMode != tenantModeNone && tenantMode != tenantModeDatabase && tenantMode != tenantModeShared {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: tenant_mode must be none, database, or shared", name)
			}
			tenantColumn := strings.TrimSpace(options["tenant_column"])
			if tenantMode == tenantModeShared {
				if tenantColumn == "" {
					tenantColumn = "tenant_id"
				}
				if !isSQLIdentifier(tenantColumn) {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: tenant_column must be a simple SQL identifier", name)
				}
			} else if tenantColumn != "" {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: tenant_column is only valid with tenant_mode shared", name)
			}
			if tenantMode == tenantModeDatabase {
				source := path
				if driver == "postgres" {
					source = dsn
				}
				if !strings.Contains(source, tenantPlaceholder) {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: tenant_mode database requires `{tenant}` in the path or dsn", name)
				}
			}

			migrationMode := strings.TrimSpace(options["migration_mode"])
			if migrationMode == "" {
				migrationMode = migrationModeManual
			}
			if migrationMode != migrationModeAuto && migrationMode != migrationModeManual {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: migration_mode must be auto or manual", name)
			}
			maxOpen, err := parsePositiveOption(options, "max_open_conns")
			if err != nil {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: %v", name, err)
			}
			maxIdle, err := parseNonNegativeOption(options, "max_idle_conns")
			if err != nil {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: %v", name, err)
			}
			if maxOpen > 0 && maxIdle > maxOpen {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: max_idle_conns cannot exceed max_open_conns", name)
			}

			declarations := declarationsFromMeta(req.Out)
			for _, declaration := range declarations {
				if declaration.Name == name {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@db %s: duplicate declaration", name)
				}
			}
			declarations = append(declarations, databaseDecl{
				Name: name, Driver: driver, Path: path, DSN: dsn,
				TenantMode: tenantMode, TenantColumn: tenantColumn,
				MigrationMode: migrationMode, MaxOpenConns: maxOpen, MaxIdleConns: maxIdle,
			})
			req.Out[declarationsMetaKey] = declarations
			return compiler.DirectiveRes{Next: next}, nil
		},
	}
}

func migrationDirective() compiler.Directive {
	return compiler.Directive{
		Name:    "migration",
		Example: `@migration app 1 create_notes { up: "CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL)" down: "DROP TABLE notes" }`,
		Handler: func(_ context.Context, req compiler.DirectiveReq) (compiler.DirectiveRes, error) {
			pos := compiler.Position{File: req.File, Line: req.I + 1}
			header, hasBlock := splitHeader(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(req.Lines[req.I]), "@migration")))
			if !hasBlock {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration: expected `@migration <database> <version> <name> { up: \"...\" down: \"...\" }`")
			}
			parts := strings.Fields(header)
			if len(parts) != 3 {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration: expected database, version, and name")
			}
			version, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil || version < 1 {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration: version must be a positive integer")
			}
			if !token.IsIdentifier(parts[2]) {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration: invalid name %q", parts[2])
			}
			options, next, err := compiler.ParseBlockOptions(req.Lines, req.I)
			if err != nil {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration %s: %v", parts[2], err)
			}
			allowed := map[string]bool{"up": true, "down": true, "sqlite_up": true, "sqlite_down": true, "postgres_up": true, "postgres_down": true}
			for _, key := range slices.Sorted(maps.Keys(options)) {
				if !allowed[key] {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration %s: unknown option %q", parts[2], key)
				}
			}
			decls := declarationsFromMeta(req.Out)
			declIndex := -1
			for i := range decls {
				if decls[i].Name == parts[0] {
					declIndex = i
					break
				}
			}
			if declIndex < 0 {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration: database %q must be declared with @db first", parts[0])
			}
			decl := &decls[declIndex]
			up, down := strings.TrimSpace(options["up"]), strings.TrimSpace(options["down"])
			if decl.Driver == "sqlite" {
				if up == "" {
					up = strings.TrimSpace(options["sqlite_up"])
				}
				if down == "" {
					down = strings.TrimSpace(options["sqlite_down"])
				}
			} else {
				if up == "" {
					up = strings.TrimSpace(options["postgres_up"])
				}
				if down == "" {
					down = strings.TrimSpace(options["postgres_down"])
				}
			}
			if up == "" || down == "" {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration %s: provide non-empty up and down SQL for the %s database", parts[2], decl.Driver)
			}
			for _, prior := range decl.Migrations {
				if prior.Version == version {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration %s: duplicate version %d for database %q", parts[2], version, parts[0])
				}
				if prior.Name == parts[2] {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@migration: duplicate name %q for database %q", parts[2], parts[0])
				}
			}
			decl.Migrations = append(decl.Migrations, migrationDecl{Version: version, Name: parts[2], Up: up, Down: down})
			req.Out[declarationsMetaKey] = decls
			return compiler.DirectiveRes{Next: next}, nil
		},
	}
}

func parsePositiveOption(options map[string]string, name string) (int, error) {
	value := strings.TrimSpace(options[name])
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func parseNonNegativeOption(options map[string]string, name string) (int, error) {
	value := strings.TrimSpace(options[name])
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return parsed, nil
}

func splitHeader(rest string) (string, bool) {
	for i := 0; i < len(rest); i++ {
		if rest[i] == '{' {
			return strings.TrimSpace(rest[:i]), true
		}
		if rest[i] == '"' || rest[i] == '\'' {
			quote := rest[i]
			for i++; i < len(rest) && rest[i] != quote; i++ {
				if rest[i] == '\\' && i+1 < len(rest) {
					i++
				}
			}
		}
	}
	return strings.TrimSpace(rest), false
}

func declarationsFromMeta(meta map[string]any) []databaseDecl {
	declarations, _ := meta[declarationsMetaKey].([]databaseDecl)
	return declarations
}
