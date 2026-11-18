package nexssflow

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/nexssp/flow/compiler"
	flowschema "github.com/nexssp/flow/extensions/schema"
	"github.com/nexssp/kernel/xerr"
)

const mappingsMetaKey = "db.mappings"

type mappingKey struct {
	Database string
	Schema   string
}

type tableMapping struct {
	Database string
	Schema   flowschema.Schema
	Table    string
	Columns  []mappedColumn
}

type mappedColumn struct {
	JSONName string
	Name     string
}

func databaseMappingDirective() compiler.Directive {
	return compiler.Directive{
		Name:    "dbmap",
		Example: `@dbmap local NoteWrite as notes`,
		Handler: func(_ context.Context, req compiler.DirectiveReq) (compiler.DirectiveRes, error) {
			pos := compiler.Position{File: req.File, Line: req.I + 1}
			line := strings.TrimSpace(req.Lines[req.I])
			parts := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "@dbmap")))
			if len(parts) != 4 || parts[2] != "as" {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap: expected `@dbmap <database> <schema> as <table>`")
			}
			database, schemaName, table := parts[0], parts[1], parts[3]
			if !isSQLIdentifier(table) {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap: invalid SQL table name %q", table)
			}
			declaredDatabase := false
			var databaseConfig databaseDecl
			for _, declaration := range declarationsFromMeta(req.Out) {
				if declaration.Name == database {
					declaredDatabase = true
					databaseConfig = declaration
					break
				}
			}
			if !declaredDatabase {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap: database %q must be declared with @db first", database)
			}

			var declaredSchema flowschema.Schema
			foundSchema := false
			for _, candidate := range flowschema.VisibleSchemasFromMeta(req.Out) {
				if candidate.Name == schemaName {
					declaredSchema = candidate
					foundSchema = true
					break
				}
			}
			if !foundSchema {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap: schema %q must be declared or included first", schemaName)
			}

			mapping := tableMapping{Database: database, Schema: declaredSchema, Table: table}
			seenColumns := make(map[string]string, len(declaredSchema.Fields))
			for _, field := range declaredSchema.Fields {
				column, tagged := field.Tags["db"]
				if !tagged {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: field %q needs an explicit `db:\"column\"` or `db:\"-\"` tag", schemaName, field.JSONName)
				}
				column = strings.TrimSpace(column)
				if column == "-" {
					continue
				}
				if !isSQLIdentifier(column) {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: invalid SQL column name %q for field %q", schemaName, column, field.JSONName)
				}
				if databaseConfig.TenantMode == tenantModeShared && column == databaseConfig.TenantColumn {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: tenant column %q is reserved and filled from trusted request context", schemaName, column)
				}
				if prior, duplicate := seenColumns[column]; duplicate {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: fields %q and %q map to the same column %q", schemaName, prior, field.JSONName, column)
				}
				if !isSQLScalar(field) {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: field %q has unsupported SQL value type %q; map only scalar string, numeric, or boolean fields", schemaName, field.JSONName, field.Type)
				}
				seenColumns[column] = field.JSONName
				mapping.Columns = append(mapping.Columns, mappedColumn{JSONName: field.JSONName, Name: column})
			}
			if len(mapping.Columns) == 0 {
				return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: at least one field must map to a SQL column", schemaName)
			}

			mappings, _ := req.Out[mappingsMetaKey].([]tableMapping)
			for _, prior := range mappings {
				if prior.Database == mapping.Database && prior.Schema.Name == mapping.Schema.Name {
					return compiler.DirectiveRes{}, compiler.SourceError(pos, "@dbmap %s: duplicate mapping for database %q", schemaName, database)
				}
			}
			req.Out[mappingsMetaKey] = append(mappings, mapping)
			return compiler.DirectiveRes{Next: req.I + 1}, nil
		},
	}
}

func insertMapped(ctx context.Context, request map[string]any) (map[string]any, error) {
	databaseName, ok := request["database"].(string)
	if !ok || strings.TrimSpace(databaseName) == "" {
		return nil, fmt.Errorf("db.insert: database alias is required")
	}
	schemaName, ok := request["schema"].(string)
	if !ok || strings.TrimSpace(schemaName) == "" {
		return nil, fmt.Errorf("db.insert: schema name is required")
	}
	record, ok := request["record"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("db.insert: record must be an object")
	}
	store, err := registryFromContext(ctx)
	if err != nil {
		return nil, err
	}
	declaration, err := store.declaration(databaseName)
	if err != nil {
		return nil, err
	}
	tenant, err := actionTenant(ctx, declaration, request, false)
	if err != nil {
		return nil, err
	}
	mapping, ok := store.mapping(databaseName, schemaName)
	if !ok {
		return nil, fmt.Errorf("db.insert: no @dbmap for database %q and schema %q", databaseName, schemaName)
	}
	if err := rejectUnknownRecordFields(mapping.Schema, record); err != nil {
		return nil, err
	}
	effective, err := flowschema.ApplyDefaultsAndValidate(ctx, mapping.Schema, record)
	if err != nil {
		return nil, fmt.Errorf("db.insert %s: %w", schemaName, err)
	}

	columns := make([]string, 0, len(mapping.Columns)+1)
	placeholders := make([]string, 0, len(mapping.Columns)+1)
	args := make([]any, 0, len(mapping.Columns)+1)
	for _, column := range mapping.Columns {
		value, present := effective[column.JSONName]
		if !present {
			continue
		}
		columns = append(columns, `"`+column.Name+`"`)
		placeholders = append(placeholders, placeholder(declaration.Driver, len(args)+1))
		args = append(args, value)
	}
	if declaration.TenantMode == tenantModeShared {
		columns = append(columns, `"`+declaration.TenantColumn+`"`)
		placeholders = append(placeholders, placeholder(declaration.Driver, len(args)+1))
		args = append(args, tenant)
	}

	statement := `INSERT INTO "` + mapping.Table + `" DEFAULT VALUES`
	if len(columns) > 0 {
		statement = `INSERT INTO "` + mapping.Table + `" (` + strings.Join(columns, ", ") + `) VALUES (` + strings.Join(placeholders, ", ") + ")"
	}
	database, err := store.database(databaseName, tenant)
	if err != nil {
		return nil, err
	}
	result, err := database.ExecContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("db.insert %s: %w", schemaName, err)
	}
	rowsAffected, lastInsertID, err := resultValues(declaration.Driver, result)
	if err != nil {
		return nil, fmt.Errorf("db.insert %s: %w", schemaName, err)
	}
	return map[string]any{"rows_affected": rowsAffected, "last_insert_id": lastInsertID}, nil
}

func queryMapped(ctx context.Context, request map[string]any) (mappedResponse map[string]any, returnErr error) {
	errorMessage := ""
	if value, exists := request["error_message"]; exists {
		message, ok := value.(string)
		if !ok || strings.TrimSpace(message) == "" {
			return nil, fmt.Errorf("db.query_mapped: error_message must be a non-empty string")
		}
		errorMessage = strings.TrimSpace(message)
		defer func() {
			if returnErr != nil {
				returnErr = xerr.Internal(errorMessage, returnErr)
			}
		}()
	}
	databaseName, ok := request["database"].(string)
	if !ok || strings.TrimSpace(databaseName) == "" {
		return nil, fmt.Errorf("db.query_mapped: database alias is required")
	}
	schemaName, ok := request["schema"].(string)
	if !ok || strings.TrimSpace(schemaName) == "" {
		return nil, fmt.Errorf("db.query_mapped: schema name is required")
	}
	store, err := registryFromContext(ctx)
	if err != nil {
		return nil, err
	}
	mapping, ok := store.mapping(databaseName, schemaName)
	if !ok {
		return nil, fmt.Errorf("db.query_mapped: no @dbmap for database %q and schema %q", databaseName, schemaName)
	}
	if len(mapping.Columns) != len(mapping.Schema.Fields) {
		return nil, fmt.Errorf("db.query_mapped %s: every schema field must map to a column", schemaName)
	}

	result, err := queryRows(ctx, request)
	if err != nil {
		return nil, err
	}
	columnsByName := make(map[string]mappedColumn, len(mapping.Columns))
	fieldsByJSONName := make(map[string]flowschema.Field, len(mapping.Schema.Fields))
	for _, column := range mapping.Columns {
		columnsByName[column.Name] = column
	}
	for _, field := range mapping.Schema.Fields {
		fieldsByJSONName[field.JSONName] = field
	}
	if len(result.columns) != len(mapping.Columns) {
		return nil, fmt.Errorf("db.query_mapped %s: query returned %d columns; mapping requires %d", schemaName, len(result.columns), len(mapping.Columns))
	}
	seenColumns := make(map[string]struct{}, len(result.columns))
	for _, name := range result.columns {
		if _, duplicate := seenColumns[name]; duplicate {
			return nil, fmt.Errorf("db.query_mapped %s: query returned duplicate column %q", schemaName, name)
		}
		if _, mapped := columnsByName[name]; !mapped {
			return nil, fmt.Errorf("db.query_mapped %s: query returned unmapped column %q", schemaName, name)
		}
		seenColumns[name] = struct{}{}
	}

	rows := make([]map[string]any, len(result.rows))
	for i, row := range result.rows {
		mappedRow := make(map[string]any, len(mapping.Columns))
		for _, column := range mapping.Columns {
			field := fieldsByJSONName[column.JSONName]
			value, err := mapDatabaseValue(row[column.Name], field)
			if err != nil {
				return nil, fmt.Errorf("db.query_mapped %s: %w", schemaName, err)
			}
			mappedRow[column.JSONName] = value
		}
		rows[i] = mappedRow
	}
	return map[string]any{"rows": rows, "count": len(rows)}, nil
}

func mapDatabaseValue(value any, field flowschema.Field) (any, error) {
	if value == nil && field.Pointer {
		return nil, nil
	}
	switch field.Kind {
	case flowschema.KindString:
		if value, ok := value.(string); ok {
			return value, nil
		}
	case flowschema.KindInt:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return value, nil
		}
	case flowschema.KindFloat:
		switch value := value.(type) {
		case int64:
			return float64(value), nil
		case float64:
			return value, nil
		}
	case flowschema.KindBool:
		switch value := value.(type) {
		case bool:
			return value, nil
		case int64:
			return value != 0, nil
		}
	}
	return nil, fmt.Errorf("column for field %q has incompatible value type %T", field.JSONName, value)
}

func rejectUnknownRecordFields(schema flowschema.Schema, record map[string]any) error {
	known := make(map[string]struct{}, len(schema.Fields))
	for _, field := range schema.Fields {
		known[field.JSONName] = struct{}{}
	}
	for name := range record {
		if _, ok := known[name]; !ok {
			return fmt.Errorf("db.insert %s: field %q is not declared by the mapped schema", schema.Name, name)
		}
	}
	return nil
}

func isSQLScalar(field flowschema.Field) bool {
	if field.Slice || field.Map {
		return false
	}
	switch field.Kind {
	case flowschema.KindString, flowschema.KindInt, flowschema.KindFloat, flowschema.KindBool:
		return true
	default:
		return false
	}
}

func isSQLIdentifier(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for i := range len(value) {
		char := value[i]
		if i == 0 {
			if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && char != '_' {
				return false
			}
			continue
		}
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

func tableMappingsFromMeta(meta map[string]any) []tableMapping {
	mappings, _ := meta[mappingsMetaKey].([]tableMapping)
	return mappings
}
