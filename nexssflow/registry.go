package nexssflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const (
	tenantModeNone      = "none"
	tenantModeDatabase  = "database"
	tenantModeShared    = "shared"
	migrationModeAuto   = "auto"
	migrationModeManual = "manual"
	tenantPlaceholder   = "{tenant}"
)

var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)

type databaseDecl struct {
	Name          string
	Driver        string
	Path          string
	DSN           string
	TenantMode    string
	TenantColumn  string
	MigrationMode string
	MaxOpenConns  int
	MaxIdleConns  int
	Migrations    []migrationDecl
}

type migrationDecl struct {
	Version int64
	Name    string
	Up      string
	Down    string
}

type registry struct {
	mu           sync.Mutex
	declarations map[string]databaseDecl
	mappings     map[mappingKey]tableMapping
	handles      map[string]*sql.DB
	closed       bool
}

type registryContextKey struct{}

func newRegistry(declarations []databaseDecl, mappings ...tableMapping) *registry {
	r := &registry{
		declarations: make(map[string]databaseDecl, len(declarations)),
		mappings:     make(map[mappingKey]tableMapping, len(mappings)),
		handles:      make(map[string]*sql.DB, len(declarations)),
	}
	for _, declaration := range declarations {
		r.declarations[declaration.Name] = declaration
	}
	for _, mapping := range mappings {
		r.mappings[mappingKey{Database: mapping.Database, Schema: mapping.Schema.Name}] = mapping
	}
	return r
}

func (r *registry) declaration(name string) (databaseDecl, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	declaration, ok := r.declarations[name]
	if !ok {
		return databaseDecl{}, fmt.Errorf("db: unknown database %q", name)
	}
	return declaration, nil
}

func (r *registry) mapping(database, schema string) (tableMapping, bool) {
	mapping, ok := r.mappings[mappingKey{Database: database, Schema: schema}]
	return mapping, ok
}

func (r *registry) database(name string, tenantIDs ...string) (*sql.DB, error) {
	tenant := ""
	if len(tenantIDs) > 0 {
		tenant = strings.TrimSpace(tenantIDs[0])
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("db: registry is closed")
	}
	declaration, ok := r.declarations[name]
	if !ok {
		return nil, fmt.Errorf("db: unknown database %q", name)
	}
	if declaration.TenantMode == "" {
		declaration.TenantMode = tenantModeNone
	}
	if declaration.TenantMode == tenantModeDatabase {
		if err := validateTenantID(tenant); err != nil {
			return nil, err
		}
	} else if declaration.TenantMode == tenantModeShared && tenant != "" {
		if err := validateTenantID(tenant); err != nil {
			return nil, err
		}
	} else if tenant != "" && !validTenantID(tenant) {
		return nil, fmt.Errorf("db: invalid tenant ID %q", tenant)
	}

	handleKey := name
	if declaration.TenantMode == tenantModeDatabase {
		handleKey += "\x00" + tenant
	}
	if database := r.handles[handleKey]; database != nil {
		return database, nil
	}

	database, err := openDatabase(declaration, tenant)
	if err != nil {
		return nil, fmt.Errorf("db %s: open: %w", name, err)
	}
	r.handles[handleKey] = database
	if declaration.MigrationMode == migrationModeAuto && len(declaration.Migrations) > 0 {
		if _, err := migrateDatabase(context.Background(), database, declaration, "up", 0); err != nil {
			delete(r.handles, handleKey)
			_ = database.Close()
			return nil, fmt.Errorf("db %s: automatic migrations: %w", name, err)
		}
	}
	return database, nil
}

func openDatabase(declaration databaseDecl, tenant string) (*sql.DB, error) {
	dsn := declaration.DSN
	if declaration.Driver == "sqlite" {
		dsn = declaration.Path
	}
	dsn = os.ExpandEnv(dsn)
	if declaration.TenantMode == tenantModeDatabase {
		if err := validateTenantID(tenant); err != nil {
			return nil, err
		}
		// Restrict tenant IDs to safe slugs before interpolation. Escaping the
		// value as a URL path segment also supports URL-form PostgreSQL DSNs.
		dsn = strings.ReplaceAll(dsn, tenantPlaceholder, url.PathEscape(tenant))
	}
	if declaration.Driver == "sqlite" {
		resolvedPath, err := resolveDatabasePath(dsn)
		if err != nil {
			return nil, fmt.Errorf("resolve path: %w", err)
		}
		dsn = resolvedPath
		if dsn != ":memory:" && !strings.HasPrefix(dsn, "file:") {
			if dir := filepath.Dir(dsn); dir != "." && dir != "" {
				if err := os.MkdirAll(dir, 0700); err != nil {
					return nil, fmt.Errorf("create database directory: %w", err)
				}
			}
		}
	}
	driver := declaration.Driver
	if driver == "postgres" {
		driver = "pgx"
	}
	database, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	maxOpen := declaration.MaxOpenConns
	maxIdle := declaration.MaxIdleConns
	if maxOpen == 0 {
		if declaration.Driver == "sqlite" {
			maxOpen = 1
		} else {
			maxOpen = 10
		}
	}
	if maxIdle == 0 {
		if declaration.Driver == "sqlite" {
			maxIdle = 1
		} else {
			maxIdle = 5
		}
	}
	database.SetMaxOpenConns(maxOpen)
	database.SetMaxIdleConns(maxIdle)
	if declaration.Driver == "sqlite" {
		if _, err := database.Exec("PRAGMA busy_timeout = 5000"); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("configure SQLite: %w", err)
		}
		if _, err := database.Exec("PRAGMA foreign_keys = ON"); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("enable SQLite foreign keys: %w", err)
		}
	}
	if err := database.Ping(); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	return database, nil
}

func validateTenantID(tenant string) error {
	if !validTenantID(tenant) {
		return fmt.Errorf("db: tenant is required and must be a safe slug (1-63 letters, digits, underscores, or hyphens; first character must be alphanumeric)")
	}
	return nil
}

func validTenantID(tenant string) bool {
	return tenantIDPattern.MatchString(tenant)
}

func resolveDatabasePath(path string) (string, error) {
	path = os.ExpandEnv(path)
	if path != "~" && !strings.HasPrefix(path, "~/") && !strings.HasPrefix(path, `~\`) {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find home directory: %w", err)
	}
	relative := strings.TrimLeft(path[1:], `/\`)
	return filepath.Join(home, filepath.FromSlash(relative)), nil
}

func (r *registry) close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	handles := make([]*sql.DB, 0, len(r.handles))
	for _, database := range r.handles {
		handles = append(handles, database)
	}
	r.handles = nil
	r.mu.Unlock()

	var errs []error
	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}
	for _, database := range handles {
		if err := database.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func withRegistry(ctx context.Context, store *registry) context.Context {
	return context.WithValue(ctx, registryContextKey{}, store)
}

func registryFromContext(ctx context.Context) (*registry, error) {
	store, ok := ctx.Value(registryContextKey{}).(*registry)
	if !ok || store == nil {
		return nil, errors.New("db: no @db declaration is active for this pipeline")
	}
	return store, nil
}
