package nexssflow

import (
	"context"
	"fmt"
	"strings"

	"github.com/nexssp/kernel/xctx"
)

func placeholder(driver string, index int) string {
	if driver == "postgres" {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}

// rewritePlaceholders maps the portable NFlow `?` placeholder syntax to
// PostgreSQL's $1, $2, ... parameters. In a PostgreSQL query, write `??` for a
// literal question-mark operator (for example, a JSONB `?` operator).
func rewritePlaceholders(driver, statement string) string {
	if driver != "postgres" || !strings.Contains(statement, "?") {
		return statement
	}
	var out strings.Builder
	out.Grow(len(statement) + 8)
	parameter := 0
	for i := 0; i < len(statement); {
		switch {
		case statement[i] == '\'':
			end := scanQuoted(statement, i, '\'')
			out.WriteString(statement[i:end])
			i = end
		case statement[i] == '"':
			end := scanQuoted(statement, i, '"')
			out.WriteString(statement[i:end])
			i = end
		case statement[i] == '`':
			end := scanQuoted(statement, i, '`')
			out.WriteString(statement[i:end])
			i = end
		case statement[i] == '[':
			end := scanBracketIdentifier(statement, i)
			out.WriteString(statement[i:end])
			i = end
		case strings.HasPrefix(statement[i:], "--"):
			end := strings.IndexByte(statement[i:], '\n')
			if end < 0 {
				out.WriteString(statement[i:])
				return out.String()
			}
			end += i
			out.WriteString(statement[i:end])
			i = end
		case strings.HasPrefix(statement[i:], "/*"):
			end := scanBlockComment(statement, i)
			out.WriteString(statement[i:end])
			i = end
		case statement[i] == '$':
			if end, ok := scanDollarQuoted(statement, i); ok {
				out.WriteString(statement[i:end])
				i = end
			} else {
				out.WriteByte(statement[i])
				i++
			}
		case statement[i] == '?':
			if i+1 < len(statement) && statement[i+1] == '?' {
				out.WriteByte('?')
				i += 2
				continue
			}
			parameter++
			out.WriteString(placeholder(driver, parameter))
			i++
		default:
			out.WriteByte(statement[i])
			i++
		}
	}
	return out.String()
}

// RewritePlaceholders converts portable question-mark parameters to the
// PostgreSQL form while preserving question marks inside SQL literals/comments.
func RewritePlaceholders(driver, statement string) string {
	return rewritePlaceholders(driver, statement)
}

func scanQuoted(statement string, start int, quote byte) int {
	for i := start + 1; i < len(statement); i++ {
		if statement[i] == '\\' && quote == '\'' && i+1 < len(statement) {
			i++
			continue
		}
		if statement[i] == quote {
			if i+1 < len(statement) && statement[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(statement)
}

func scanBracketIdentifier(statement string, start int) int {
	for i := start + 1; i < len(statement); i++ {
		if statement[i] == ']' {
			if i+1 < len(statement) && statement[i+1] == ']' {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(statement)
}

func scanBlockComment(statement string, start int) int {
	depth := 1
	for i := start + 2; i < len(statement); {
		if strings.HasPrefix(statement[i:], "/*") {
			depth++
			i += 2
			continue
		}
		if strings.HasPrefix(statement[i:], "*/") {
			depth--
			i += 2
			if depth == 0 {
				return i
			}
			continue
		}
		i++
	}
	return len(statement)
}

func scanDollarQuoted(statement string, start int) (int, bool) {
	endTag := start + 1
	for endTag < len(statement) && (statement[endTag] == '_' || statement[endTag] >= 'a' && statement[endTag] <= 'z' || statement[endTag] >= 'A' && statement[endTag] <= 'Z' || statement[endTag] >= '0' && statement[endTag] <= '9') {
		endTag++
	}
	if endTag >= len(statement) || statement[endTag] != '$' {
		return 0, false
	}
	tag := statement[start : endTag+1]
	end := strings.Index(statement[endTag+1:], tag)
	if end < 0 {
		return len(statement), true
	}
	return endTag + 1 + end + len(tag), true
}

func actionTenant(ctx context.Context, declaration databaseDecl, request map[string]any, allowSharedWithoutTenant bool) (string, error) {
	if _, supplied := request["tenant"]; supplied {
		return "", fmt.Errorf("db: tenant cannot be selected in action input; set a verified tenant ID on the trusted request context")
	}
	tenant := strings.TrimSpace(xctx.TenantIDFrom(ctx))
	switch declaration.TenantMode {
	case "", tenantModeNone:
		if tenant != "" && !validTenantID(tenant) {
			return "", fmt.Errorf("db: invalid tenant ID %q", tenant)
		}
		return tenant, nil
	case tenantModeDatabase:
		if err := validateTenantID(tenant); err != nil {
			return "", err
		}
		return tenant, nil
	case tenantModeShared:
		if tenant == "" && allowSharedWithoutTenant {
			return "", nil
		}
		if err := validateTenantID(tenant); err != nil {
			return "", err
		}
		return tenant, nil
	default:
		return "", fmt.Errorf("db: invalid tenant mode %q", declaration.TenantMode)
	}
}

func requireRawSQLAllowed(declaration databaseDecl) error {
	if declaration.TenantMode == tenantModeShared {
		return fmt.Errorf("db: raw SQL actions are disabled for shared-tenant database %q; use db.insert, db.find, db.update, or db.delete so tenant scope is enforced", declaration.Name)
	}
	return nil
}
