package nexssflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

const maxQueryRows = 10000

func library() action.Library {
	return action.Library{
		Name: "db",
		Actions: []action.AnyAction{
			action.New("db.exec", execute).Description("Execute a parameterized SQL statement; unavailable for shared-tenant databases.").Build(),
			action.New("db.transaction", executeTransaction).Description("Execute ordered parameterized SQL statements atomically; unavailable for shared-tenant databases.").Build(),
			action.New("db.query", query).Description("Run a parameterized SQL query and return rows as objects; unavailable for shared-tenant databases.").Build(),
			action.New("db.query_mapped", queryMapped).Description("Run a parameterized SQL query and map rows to an explicitly mapped schema; unavailable for shared-tenant databases.").Build(),
			action.New("db.insert", insertMapped).Description("Insert one schema-mapped record, adding the tenant column when configured.").Build(),
			action.New("db.find", findMapped).Description("Read mapped rows with tenant scope enforced for shared-tenant databases.").Build(),
			action.New("db.update", updateMapped).Description("Update mapped fields with tenant scope enforced for shared-tenant databases.").Build(),
			action.New("db.delete", deleteMapped).Description("Delete mapped rows with tenant scope enforced for shared-tenant databases.").Build(),
			action.New("db.migrate", migrateAction).Description("Apply pending migrations or roll back the latest applied migration for one database/tenant.").Build(),
			action.New("db.migration_status", migrationStatusAction).Description("List applied and pending migrations for one database/tenant.").Build(),
		},
	}
}

func execute(ctx context.Context, request map[string]any) (response map[string]any, returnErr error) {
	errorMessage := ""
	if value, exists := request["error_message"]; exists {
		message, ok := value.(string)
		if !ok || strings.TrimSpace(message) == "" {
			return nil, fmt.Errorf("db.exec: error_message must be a non-empty string")
		}
		errorMessage = strings.TrimSpace(message)
		defer func() {
			if returnErr != nil {
				returnErr = xerr.Internal(errorMessage, returnErr)
			}
		}()
	}
	store, database, statement, args, declaration, err := actionInputs(ctx, request)
	if err != nil {
		return nil, err
	}
	execResult, err := database.ExecContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("db.exec %q: %w", store, err)
	}
	rowsAffected, lastInsertID, err := resultValues(declaration.Driver, execResult)
	if err != nil {
		return nil, fmt.Errorf("db.exec %q: %w", store, err)
	}
	return map[string]any{"rows_affected": rowsAffected, "last_insert_id": lastInsertID}, nil
}

type transactionStep struct {
	query string
	args  []any
}

func executeTransaction(ctx context.Context, request map[string]any) (response map[string]any, returnErr error) {
	errorMessage := ""
	if value, exists := request["error_message"]; exists {
		message, ok := value.(string)
		if !ok || strings.TrimSpace(message) == "" {
			return nil, fmt.Errorf("db.transaction: error_message must be a non-empty string")
		}
		errorMessage = strings.TrimSpace(message)
		defer func() {
			if returnErr != nil {
				returnErr = xerr.Internal(errorMessage, returnErr)
			}
		}()
	}

	store, ok := request["database"].(string)
	if !ok || strings.TrimSpace(store) == "" {
		return nil, fmt.Errorf("db.transaction: database alias is required")
	}
	steps, err := transactionSteps(request["steps"])
	if err != nil {
		return nil, err
	}
	registry, err := registryFromContext(ctx)
	if err != nil {
		return nil, err
	}
	declaration, err := registry.declaration(store)
	if err != nil {
		return nil, err
	}
	if err := requireRawSQLAllowed(declaration); err != nil {
		return nil, err
	}
	tenant, err := actionTenant(ctx, declaration, request, false)
	if err != nil {
		return nil, err
	}
	database, err := registry.database(store, tenant)
	if err != nil {
		return nil, err
	}
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("db.transaction %q: begin: %w", store, err)
	}
	defer func() { _ = tx.Rollback() }()

	results := make([]map[string]any, 0, len(steps))
	for i, step := range steps {
		statement := rewritePlaceholders(declaration.Driver, step.query)
		execResult, err := tx.ExecContext(ctx, statement, step.args...)
		if err != nil {
			return nil, fmt.Errorf("db.transaction %q step %d: %w", store, i+1, err)
		}
		rowsAffected, lastInsertID, err := resultValues(declaration.Driver, execResult)
		if err != nil {
			return nil, fmt.Errorf("db.transaction %q step %d: %w", store, i+1, err)
		}
		results = append(results, map[string]any{"rows_affected": rowsAffected, "last_insert_id": lastInsertID})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("db.transaction %q: commit: %w", store, err)
	}
	return map[string]any{"results": results}, nil
}

func transactionSteps(value any) ([]transactionStep, error) {
	if value == nil {
		return nil, fmt.Errorf("db.transaction: steps must be a non-empty array")
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, fmt.Errorf("db.transaction: steps must be an array")
	}
	if reflected.Len() == 0 {
		return nil, fmt.Errorf("db.transaction: steps must be a non-empty array")
	}
	steps := make([]transactionStep, 0, reflected.Len())
	for i := range reflected.Len() {
		step, ok := reflected.Index(i).Interface().(map[string]any)
		if !ok {
			return nil, fmt.Errorf("db.transaction: step %d must be an object", i+1)
		}
		query, ok := step["query"].(string)
		if !ok || strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("db.transaction: step %d query is required", i+1)
		}
		args, err := queryArgs(step["args"])
		if err != nil {
			return nil, fmt.Errorf("db.transaction: step %d: %w", i+1, err)
		}
		steps = append(steps, transactionStep{query: query, args: args})
	}
	return steps, nil
}

func query(ctx context.Context, request map[string]any) (response map[string]any, returnErr error) {
	errorMessage := ""
	if value, exists := request["error_message"]; exists {
		message, ok := value.(string)
		if !ok || strings.TrimSpace(message) == "" {
			return nil, fmt.Errorf("db.query: error_message must be a non-empty string")
		}
		errorMessage = strings.TrimSpace(message)
		defer func() {
			if returnErr != nil {
				returnErr = xerr.Internal(errorMessage, returnErr)
			}
		}()
	}
	result, err := queryRows(ctx, request)
	if err != nil {
		return nil, err
	}
	return map[string]any{"rows": result.rows, "count": len(result.rows)}, nil
}

type queryResult struct {
	columns []string
	rows    []map[string]any
}

func queryRows(ctx context.Context, request map[string]any) (result queryResult, returnErr error) {
	store, database, statement, args, _, err := actionInputs(ctx, request)
	if err != nil {
		return queryResult{}, err
	}
	return queryRowsOn(ctx, database, statement, args, store)
}

func queryRowsOn(ctx context.Context, database *sql.DB, statement string, args []any, label string) (result queryResult, returnErr error) {
	rows, err := database.QueryContext(ctx, statement, args...)
	if err != nil {
		return queryResult{}, fmt.Errorf("db.query %q: %w", label, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf("db.query %q: close rows: %w", label, closeErr)
			if returnErr == nil {
				result = queryResult{}
				returnErr = closeErr
				return
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return queryResult{}, fmt.Errorf("db.query %q: columns: %w", label, err)
	}
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for i := range values {
		destinations[i] = &values[i]
	}
	result = queryResult{columns: columns, rows: make([]map[string]any, 0)}
	for rows.Next() {
		if len(result.rows) == maxQueryRows {
			return queryResult{}, fmt.Errorf("db.query %q: result exceeds %d rows; add a SQL LIMIT", label, maxQueryRows)
		}
		if err := rows.Scan(destinations...); err != nil {
			return queryResult{}, fmt.Errorf("db.query %q: scan row: %w", label, err)
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			value := values[i]
			if bytes, ok := value.([]byte); ok {
				value = string(bytes)
			}
			row[column] = value
		}
		result.rows = append(result.rows, row)
	}
	if err := rows.Err(); err != nil {
		return queryResult{}, fmt.Errorf("db.query %q: rows: %w", label, err)
	}
	return result, nil
}

func actionInputs(ctx context.Context, request map[string]any) (string, *sql.DB, string, []any, databaseDecl, error) {
	store, ok := request["database"].(string)
	if !ok || strings.TrimSpace(store) == "" {
		return "", nil, "", nil, databaseDecl{}, fmt.Errorf("db: database alias is required")
	}
	statement, ok := request["query"].(string)
	if !ok || strings.TrimSpace(statement) == "" {
		return "", nil, "", nil, databaseDecl{}, fmt.Errorf("db: query is required")
	}
	args, err := queryArgs(request["args"])
	if err != nil {
		return "", nil, "", nil, databaseDecl{}, err
	}
	registry, err := registryFromContext(ctx)
	if err != nil {
		return "", nil, "", nil, databaseDecl{}, err
	}
	declaration, err := registry.declaration(store)
	if err != nil {
		return "", nil, "", nil, databaseDecl{}, err
	}
	if err := requireRawSQLAllowed(declaration); err != nil {
		return "", nil, "", nil, databaseDecl{}, err
	}
	tenant, err := actionTenant(ctx, declaration, request, false)
	if err != nil {
		return "", nil, "", nil, databaseDecl{}, err
	}
	database, err := registry.database(store, tenant)
	if err != nil {
		return "", nil, "", nil, databaseDecl{}, err
	}
	return store, database, rewritePlaceholders(declaration.Driver, statement), args, declaration, nil
}

func resultValues(driver string, result sql.Result) (int64, any, error) {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, nil, fmt.Errorf("rows affected: %w", err)
	}
	if driver != "sqlite" {
		// PostgreSQL does not support sql.Result.LastInsertId. Use RETURNING
		// through db.query when a generated identifier is needed.
		return rowsAffected, nil, nil
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, nil, fmt.Errorf("last insert id: %w", err)
	}
	return rowsAffected, lastInsertID, nil
}

func queryArgs(value any) ([]any, error) {
	if value == nil {
		return nil, nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, fmt.Errorf("db: args must be an array")
	}
	args := make([]any, reflected.Len())
	for i := range reflected.Len() {
		args[i] = reflected.Index(i).Interface()
	}
	return args, nil
}
