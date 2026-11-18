package nexssflow

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"

	flowschema "github.com/nexssp/flow/extensions/schema"
)

type mappedActionContext struct {
	registry    *registry
	declaration databaseDecl
	database    *sql.DB
	tenant      string
	mapping     tableMapping
}

func mappedInputs(ctx context.Context, request map[string]any, actionName string) (mappedActionContext, error) {
	databaseName, ok := request["database"].(string)
	if !ok || strings.TrimSpace(databaseName) == "" {
		return mappedActionContext{}, fmt.Errorf("%s: database alias is required", actionName)
	}
	schemaName, ok := request["schema"].(string)
	if !ok || strings.TrimSpace(schemaName) == "" {
		return mappedActionContext{}, fmt.Errorf("%s: schema name is required", actionName)
	}
	store, err := registryFromContext(ctx)
	if err != nil {
		return mappedActionContext{}, err
	}
	declaration, err := store.declaration(databaseName)
	if err != nil {
		return mappedActionContext{}, err
	}
	tenant, err := actionTenant(ctx, declaration, request, false)
	if err != nil {
		return mappedActionContext{}, err
	}
	mapping, ok := store.mapping(databaseName, schemaName)
	if !ok {
		return mappedActionContext{}, fmt.Errorf("%s: no @dbmap for database %q and schema %q", actionName, databaseName, schemaName)
	}
	database, err := store.database(databaseName, tenant)
	if err != nil {
		return mappedActionContext{}, err
	}
	return mappedActionContext{
		registry: store, declaration: declaration, database: database,
		tenant: tenant, mapping: mapping,
	}, nil
}

func findMapped(ctx context.Context, request map[string]any) (map[string]any, error) {
	input, err := mappedInputs(ctx, request, "db.find")
	if err != nil {
		return nil, err
	}
	if err := requireReadableMapping(input.mapping); err != nil {
		return nil, err
	}
	filters, err := mappedValues(request["filters"], input.mapping, "db.find filters", false)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(input.mapping.Columns))
	for i, column := range input.mapping.Columns {
		columns[i] = `"` + column.Name + `"`
	}
	statement := `SELECT ` + strings.Join(columns, ", ") + ` FROM "` + input.mapping.Table + `"`
	args := make([]any, 0, len(filters)+2)
	conditions := make([]string, 0, len(filters)+1)
	if input.declaration.TenantMode == tenantModeShared {
		conditions = append(conditions, `"`+input.declaration.TenantColumn+`" = `+placeholder(input.declaration.Driver, len(args)+1))
		args = append(args, input.tenant)
	}
	appendFilters(&conditions, &args, filters, input.mapping, input.declaration.Driver)
	if len(conditions) > 0 {
		statement += " WHERE " + strings.Join(conditions, " AND ")
	}
	orderBy, err := mappedOrderBy(request["order_by"], input.mapping)
	if err != nil {
		return nil, err
	}
	if orderBy != "" {
		statement += " ORDER BY " + orderBy
	}
	limit, err := queryLimit(request["limit"])
	if err != nil {
		return nil, err
	}
	statement += " LIMIT " + placeholder(input.declaration.Driver, len(args)+1)
	args = append(args, limit)
	result, err := queryRowsOn(ctx, input.database, rewritePlaceholders(input.declaration.Driver, statement), args, input.declaration.Name)
	if err != nil {
		return nil, err
	}
	rows, err := mappedRows(input.mapping, result, "db.find")
	if err != nil {
		return nil, err
	}
	return map[string]any{"rows": rows, "count": len(rows)}, nil
}

func updateMapped(ctx context.Context, request map[string]any) (map[string]any, error) {
	input, err := mappedInputs(ctx, request, "db.update")
	if err != nil {
		return nil, err
	}
	values, err := mappedValues(request["values"], input.mapping, "db.update values", true)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("db.update: values must be a non-empty object")
	}
	filters, err := mappedValues(request["filters"], input.mapping, "db.update filters", false)
	if err != nil {
		return nil, err
	}
	if len(filters) == 0 {
		return nil, fmt.Errorf("db.update: filters must be a non-empty object")
	}
	setNames := sortedMapKeys(values)
	args := make([]any, 0, len(values)+len(filters)+1)
	sets := make([]string, 0, len(values))
	for _, name := range setNames {
		column := mappedColumnByJSONName(input.mapping, name)
		sets = append(sets, `"`+column.Name+`" = `+placeholder(input.declaration.Driver, len(args)+1))
		args = append(args, values[name])
	}
	conditions := make([]string, 0, len(filters)+1)
	if input.declaration.TenantMode == tenantModeShared {
		conditions = append(conditions, `"`+input.declaration.TenantColumn+`" = `+placeholder(input.declaration.Driver, len(args)+1))
		args = append(args, input.tenant)
	}
	appendFilters(&conditions, &args, filters, input.mapping, input.declaration.Driver)
	statement := `UPDATE "` + input.mapping.Table + `" SET ` + strings.Join(sets, ", ") + ` WHERE ` + strings.Join(conditions, " AND ")
	result, err := input.database.ExecContext(ctx, rewritePlaceholders(input.declaration.Driver, statement), args...)
	if err != nil {
		return nil, fmt.Errorf("db.update %s: %w", input.mapping.Schema.Name, err)
	}
	rowsAffected, _, err := resultValues(input.declaration.Driver, result)
	if err != nil {
		return nil, fmt.Errorf("db.update %s: %w", input.mapping.Schema.Name, err)
	}
	return map[string]any{"rows_affected": rowsAffected}, nil
}

func deleteMapped(ctx context.Context, request map[string]any) (map[string]any, error) {
	input, err := mappedInputs(ctx, request, "db.delete")
	if err != nil {
		return nil, err
	}
	filters, err := mappedValues(request["filters"], input.mapping, "db.delete filters", false)
	if err != nil {
		return nil, err
	}
	if len(filters) == 0 {
		return nil, fmt.Errorf("db.delete: filters must be a non-empty object")
	}
	args := make([]any, 0, len(filters)+1)
	conditions := make([]string, 0, len(filters)+1)
	if input.declaration.TenantMode == tenantModeShared {
		conditions = append(conditions, `"`+input.declaration.TenantColumn+`" = `+placeholder(input.declaration.Driver, len(args)+1))
		args = append(args, input.tenant)
	}
	appendFilters(&conditions, &args, filters, input.mapping, input.declaration.Driver)
	statement := `DELETE FROM "` + input.mapping.Table + `" WHERE ` + strings.Join(conditions, " AND ")
	result, err := input.database.ExecContext(ctx, rewritePlaceholders(input.declaration.Driver, statement), args...)
	if err != nil {
		return nil, fmt.Errorf("db.delete %s: %w", input.mapping.Schema.Name, err)
	}
	rowsAffected, _, err := resultValues(input.declaration.Driver, result)
	if err != nil {
		return nil, fmt.Errorf("db.delete %s: %w", input.mapping.Schema.Name, err)
	}
	return map[string]any{"rows_affected": rowsAffected}, nil
}

func requireReadableMapping(mapping tableMapping) error {
	if len(mapping.Columns) != len(mapping.Schema.Fields) {
		return fmt.Errorf("db: every field in schema %q must map to a column for mapped reads", mapping.Schema.Name)
	}
	return nil
}

func mappedRows(mapping tableMapping, result queryResult, actionName string) ([]map[string]any, error) {
	if err := requireReadableMapping(mapping); err != nil {
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
		return nil, fmt.Errorf("%s %s: query returned %d columns; mapping requires %d", actionName, mapping.Schema.Name, len(result.columns), len(mapping.Columns))
	}
	seenColumns := make(map[string]struct{}, len(result.columns))
	for _, name := range result.columns {
		if _, duplicate := seenColumns[name]; duplicate {
			return nil, fmt.Errorf("%s %s: query returned duplicate column %q", actionName, mapping.Schema.Name, name)
		}
		if _, mapped := columnsByName[name]; !mapped {
			return nil, fmt.Errorf("%s %s: query returned unmapped column %q", actionName, mapping.Schema.Name, name)
		}
		seenColumns[name] = struct{}{}
	}
	rows := make([]map[string]any, len(result.rows))
	for i, row := range result.rows {
		mappedRow := make(map[string]any, len(mapping.Columns))
		for _, column := range mapping.Columns {
			value, err := mapDatabaseValue(row[column.Name], fieldsByJSONName[column.JSONName])
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", actionName, mapping.Schema.Name, err)
			}
			mappedRow[column.JSONName] = value
		}
		rows[i] = mappedRow
	}
	return rows, nil
}

func mappedValues(raw any, mapping tableMapping, label string, required bool) (map[string]any, error) {
	values, ok := raw.(map[string]any)
	if !ok {
		if raw == nil && !required {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("%s must be an object", label)
	}
	for name, value := range values {
		field, found := mappedField(mapping, name)
		if !found {
			return nil, fmt.Errorf("%s: field %q is not mapped by schema %q", label, name, mapping.Schema.Name)
		}
		if err := validateMappedValue(value, field); err != nil {
			return nil, fmt.Errorf("%s field %q: %w", label, name, err)
		}
	}
	return values, nil
}

func validateMappedValue(value any, field flowschema.Field) error {
	if value == nil {
		if field.Pointer {
			return nil
		}
		return fmt.Errorf("null is not allowed")
	}
	switch field.Kind {
	case flowschema.KindString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case flowschema.KindBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case flowschema.KindInt:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
	case flowschema.KindFloat:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		default:
			return fmt.Errorf("expected number, got %T", value)
		}
	default:
		return fmt.Errorf("unsupported mapped value type %q", field.Type)
	}
	return nil
}

func appendFilters(conditions *[]string, args *[]any, filters map[string]any, mapping tableMapping, driver string) {
	for _, name := range sortedMapKeys(filters) {
		column := mappedColumnByJSONName(mapping, name)
		*conditions = append(*conditions, `"`+column.Name+`" = `+placeholder(driver, len(*args)+1))
		*args = append(*args, filters[name])
	}
}

func mappedColumnByJSONName(mapping tableMapping, name string) mappedColumn {
	for _, column := range mapping.Columns {
		if column.JSONName == name {
			return column
		}
	}
	return mappedColumn{}
}

func mappedField(mapping tableMapping, name string) (flowschema.Field, bool) {
	for _, field := range mapping.Schema.Fields {
		if field.JSONName == name {
			return field, true
		}
	}
	return flowschema.Field{}, false
}

func mappedOrderBy(raw any, mapping tableMapping) (string, error) {
	if raw == nil {
		return "", nil
	}
	reflected := reflect.ValueOf(raw)
	if reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return "", fmt.Errorf("db.find: order_by must be an array of {field, direction} items")
	}
	terms := make([]string, 0, reflected.Len())
	seen := make(map[string]bool, reflected.Len())
	for i := range reflected.Len() {
		item, ok := reflected.Index(i).Interface().(map[string]any)
		if !ok {
			return "", fmt.Errorf("db.find: order_by item %d must be an object", i+1)
		}
		name, ok := item["field"].(string)
		if !ok {
			return "", fmt.Errorf("db.find: order_by item %d field is required", i+1)
		}
		column, found := mappedColumnByJSONNameOrOK(mapping, name)
		if !found {
			return "", fmt.Errorf("db.find: order_by field %q is not mapped", name)
		}
		if seen[name] {
			return "", fmt.Errorf("db.find: order_by field %q is repeated", name)
		}
		seen[name] = true
		direction := "ASC"
		if value, exists := item["direction"]; exists {
			text, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("db.find: order_by direction must be asc or desc")
			}
			switch strings.ToLower(strings.TrimSpace(text)) {
			case "asc":
			case "desc":
				direction = "DESC"
			default:
				return "", fmt.Errorf("db.find: order_by direction must be asc or desc")
			}
		}
		terms = append(terms, `"`+column.Name+`" `+direction)
	}
	return strings.Join(terms, ", "), nil
}

func mappedColumnByJSONNameOrOK(mapping tableMapping, name string) (mappedColumn, bool) {
	for _, column := range mapping.Columns {
		if column.JSONName == name {
			return column, true
		}
	}
	return mappedColumn{}, false
}

func queryLimit(value any) (int64, error) {
	if value == nil {
		return maxQueryRows, nil
	}
	limit, err := optionalInt64(value)
	if err != nil || limit < 1 || limit > maxQueryRows {
		return 0, fmt.Errorf("db.find: limit must be an integer from 1 to %d", maxQueryRows)
	}
	return limit, nil
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
