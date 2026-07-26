package agnt5

import (
	"reflect"
	"strings"
	"time"
)

var timeType = reflect.TypeFor[time.Time]()

// jsonSchemaForType derives the stable JSON shape Go's encoding/json exposes.
// It intentionally stays dependency-free and covers the component input/output
// shapes used by the SDK. Callers can enrich or replace it with
// WithInputSchema/WithOutputSchema.
func jsonSchemaForType(valueType reflect.Type) map[string]any {
	return schemaForType(valueType, make(map[reflect.Type]bool))
}

func schemaForType(valueType reflect.Type, visiting map[reflect.Type]bool) map[string]any {
	if valueType == nil {
		return map[string]any{}
	}
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}

	if valueType == timeType {
		return map[string]any{"type": "string", "format": "date-time"}
	}

	switch valueType.Kind() {
	case reflect.Struct:
		if visiting[valueType] {
			return map[string]any{"type": "object"}
		}
		visiting[valueType] = true
		defer delete(visiting, valueType)

		properties := make(map[string]any)
		required := make([]string, 0)
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			if field.PkgPath != "" {
				continue
			}

			name, omitEmpty, skip := jsonField(field)
			if skip {
				continue
			}
			property := schemaForType(field.Type, visiting)
			if description := strings.TrimSpace(field.Tag.Get("description")); description != "" {
				property["description"] = description
			}
			if format := strings.TrimSpace(field.Tag.Get("format")); format != "" {
				property["format"] = format
			}
			properties[name] = property
			if !omitEmpty {
				required = append(required, name)
			}
		}

		schema := map[string]any{
			"type":       "object",
			"properties": properties,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		return schema
	case reflect.Map:
		return map[string]any{"type": "object", "properties": map[string]any{}}
	case reflect.Array, reflect.Slice:
		if valueType.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string", "format": "byte"}
		}
		return map[string]any{
			"type":  "array",
			"items": schemaForType(valueType.Elem(), visiting),
		}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Interface:
		return map[string]any{}
	default:
		return map[string]any{}
	}
}

func jsonField(field reflect.StructField) (name string, omitEmpty bool, skip bool) {
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
	if parts[0] == "-" {
		return "", false, true
	}
	name = parts[0]
	if name == "" {
		name = field.Name
	}
	for _, option := range parts[1:] {
		if option == "omitempty" || option == "omitzero" {
			omitEmpty = true
		}
	}
	return name, omitEmpty, false
}

func cloneSchemaMap(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	cloned := make(map[string]any, len(schema))
	for key, value := range schema {
		cloned[key] = cloneSchemaValue(value)
	}
	return cloned
}

func cloneSchemaValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneSchemaMap(typed)
	case []string:
		return append([]string(nil), typed...)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneSchemaValue(item)
		}
		return cloned
	default:
		return value
	}
}
