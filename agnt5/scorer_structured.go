package agnt5

// The SDK-core structured-assertions contract is also implemented in pure Go.
// Only the AST forms below are accepted; Go code is never compiled or executed.
import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/token"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

func assertionPredicate(n string) bool {
	switch n {
	case "is_array", "is_object", "is_string", "is_number", "is_boolean", "is_null":
		return true
	}
	return false
}
func assertionCheck(n string, v any) bool {
	switch n {
	case "is_array":
		_, ok := v.([]any)
		return ok
	case "is_object":
		_, ok := v.(map[string]any)
		return ok
	case "is_string":
		_, ok := v.(string)
		return ok
	case "is_number":
		_, ok := v.(float64)
		return ok
	case "is_boolean":
		_, ok := v.(bool)
		return ok
	case "is_null":
		return v == nil
	}
	return false
}

type assertionParser struct {
	text                   string
	pos, depth, operations int
}

func (p *assertionParser) space() {
	for p.pos < len(p.text) && strings.ContainsRune(" \t\r\n\x0b\x0c", rune(p.text[p.pos])) {
		p.pos++
	}
}
func (p *assertionParser) take(s string) bool {
	p.space()
	if strings.HasPrefix(p.text[p.pos:], s) {
		p.pos += len(s)
		return true
	}
	return false
}
func (p *assertionParser) ident() (string, error) {
	p.space()
	start := p.pos
	for p.pos < len(p.text) {
		c := p.text[p.pos]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			break
		}
		p.pos++
	}
	if p.pos == start {
		return "", fmt.Errorf("expected identifier")
	}
	return p.text[start:p.pos], nil
}
func (p *assertionParser) expr(level int) (ast.Expr, error) {
	if level == 3 {
		return p.atom()
	}
	operators := [][]string{{"||"}, {"&&"}, {"==", "!=", "<=", ">=", "<", ">"}}[level]
	tokens := map[string]token.Token{"||": token.LOR, "&&": token.LAND, "==": token.EQL, "!=": token.NEQ, "<=": token.LEQ, ">=": token.GEQ, "<": token.LSS, ">": token.GTR}
	left, err := p.expr(level + 1)
	if err != nil {
		return nil, err
	}
	for {
		op := ""
		for _, candidate := range operators {
			if p.take(candidate) {
				op = candidate
				break
			}
		}
		if op == "" {
			return left, nil
		}
		p.operations++
		if p.operations > 64 {
			return nil, fmt.Errorf("expression exceeds 64 operators")
		}
		right, err := p.expr(level + 1)
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{X: left, Op: tokens[op], Y: right}
	}
}
func (p *assertionParser) atom() (ast.Expr, error) {
	p.depth++
	defer func() { p.depth-- }()
	if p.depth > 32 {
		return nil, fmt.Errorf("expression nesting exceeds 32")
	}
	if p.take("!") {
		x, err := p.atom()
		return &ast.UnaryExpr{Op: token.NOT, X: x}, err
	}
	if p.take("(") {
		x, err := p.expr(0)
		if err != nil {
			return nil, err
		}
		if !p.take(")") {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		return x, nil
	}
	p.space()
	if p.pos == len(p.text) {
		return nil, fmt.Errorf("expected expression")
	}
	c := p.text[p.pos]
	if c == '"' || c == '-' || c >= '0' && c <= '9' {
		start := p.pos
		decoder := json.NewDecoder(strings.NewReader(p.text[start:]))
		var v any
		if err := decoder.Decode(&v); err != nil {
			return nil, fmt.Errorf("invalid JSON literal")
		}
		p.pos += int(decoder.InputOffset())
		return &ast.BasicLit{Kind: token.STRING, Value: p.text[start:p.pos]}, nil
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	if name == "true" || name == "false" || name == "null" {
		return ast.NewIdent(name), nil
	}
	if p.take("(") {
		args := []ast.Expr{}
		if !p.take(")") {
			for {
				x, err := p.expr(0)
				if err != nil {
					return nil, err
				}
				args = append(args, x)
				if p.take(")") {
					break
				}
				if !p.take(",") || len(args) > 2 {
					return nil, fmt.Errorf("invalid arguments")
				}
			}
		}
		arity := 1
		if name == "all" || name == "any" {
			arity = 2
		} else if !assertionPredicate(name) && name != "size" && name != "unique" {
			return nil, fmt.Errorf("unknown function")
		}
		if len(args) != arity {
			return nil, fmt.Errorf("wrong argument count")
		}
		if arity == 2 {
			pred, ok := args[1].(*ast.Ident)
			if !ok || !assertionPredicate(pred.Name) {
				return nil, fmt.Errorf("all/any require a type predicate")
			}
		}
		return &ast.CallExpr{Fun: ast.NewIdent(name), Args: args}, nil
	}
	var path ast.Expr = ast.NewIdent(name)
	for p.take(".") {
		field, err := p.ident()
		if err != nil {
			return nil, err
		}
		path = &ast.SelectorExpr{X: path, Sel: ast.NewIdent(field)}
	}
	switch name {
	case "input", "output", "expected", "input_json", "output_json", "expected_json":
	default:
		if _, ok := path.(*ast.Ident); !ok || !assertionPredicate(name) {
			return nil, fmt.Errorf("unknown root")
		}
	}
	return path, nil
}

func assertionStructure(v any) bool {
	type entry struct {
		value any
		depth int
	}
	pending := []entry{{v, 0}}
	nodes := 0
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		nodes++
		if nodes > 100000 || item.depth > 64 {
			return false
		}
		switch x := item.value.(type) {
		case []any:
			for _, v := range x {
				pending = append(pending, entry{v, item.depth + 1})
			}
		case map[string]any:
			for _, v := range x {
				pending = append(pending, entry{v, item.depth + 1})
			}
		}
	}
	return true
}
func assertionNumber(v any) (float64, error) {
	n, ok := v.(float64)
	if !ok || math.IsNaN(n) || math.IsInf(n, 0) || math.Abs(n) > 9007199254740991 {
		return 0, fmt.Errorf("expected a safe finite number")
	}
	return n, nil
}
func assertionBool(v any) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected boolean")
	}
	return b, nil
}
func assertionSpend(budget *int) error {
	*budget--
	if *budget < 0 {
		return fmt.Errorf("evaluation budget exceeded")
	}
	return nil
}
func assertionEqual(a, b any, budget *int) (bool, error) {
	if err := assertionSpend(budget); err != nil {
		return false, err
	}
	switch a := a.(type) {
	case float64:
		if _, ok := b.(float64); !ok {
			return false, nil
		}
		n, e := assertionNumber(a)
		if e != nil {
			return false, e
		}
		m, e := assertionNumber(b)
		return n == m, e
	case []any:
		b, ok := b.([]any)
		if !ok || len(a) != len(b) {
			return false, nil
		}
		for i, v := range a {
			ok, e := assertionEqual(v, b[i], budget)
			if e != nil || !ok {
				return ok, e
			}
		}
		return true, nil
	case map[string]any:
		b, ok := b.(map[string]any)
		if !ok || len(a) != len(b) {
			return false, nil
		}
		keys := make([]string, 0, len(a))
		for k := range a {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := a[k]
			other, ok := b[k]
			if !ok {
				return false, nil
			}
			ok, e := assertionEqual(v, other, budget)
			if e != nil || !ok {
				return ok, e
			}
		}
		return true, nil
	case string:
		b, ok := b.(string)
		return ok && a == b, nil
	case bool:
		b, ok := b.(bool)
		return ok && a == b, nil
	case nil:
		return b == nil, nil
	}
	return false, nil
}
func assertionEval(e ast.Expr, input map[string]any, budget *int) (any, error) {
	if err := assertionSpend(budget); err != nil {
		return nil, err
	}
	switch x := e.(type) {
	case *ast.BasicLit:
		var v any
		err := json.Unmarshal([]byte(x.Value), &v)
		return v, err
	case *ast.Ident:
		switch x.Name {
		case "true":
			return true, nil
		case "false":
			return false, nil
		case "null":
			return nil, nil
		}
		key := strings.TrimSuffix(x.Name, "_json")
		v, ok := input[key]
		if !ok {
			v = nil
		}
		if key != x.Name {
			if s, ok := v.(string); ok {
				if err := json.Unmarshal([]byte(s), &v); err != nil {
					return nil, fmt.Errorf("invalid encoded JSON")
				}
				if !assertionStructure(v) {
					return nil, fmt.Errorf("JSON structure exceeds limits")
				}
			}
		}
		return v, nil
	case *ast.SelectorExpr:
		*budget++ // one path lookup, independent of the number of fields
		v, e := assertionEval(x.X, input, budget)
		if e != nil {
			return nil, e
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("missing input field")
		}
		v, ok = m[x.Sel.Name]
		if !ok {
			return nil, fmt.Errorf("missing input field")
		}
		return v, nil
	case *ast.UnaryExpr:

		v, e := assertionEval(x.X, input, budget)
		if e != nil {
			return nil, e
		}
		b, e := assertionBool(v)
		return !b, e
	case *ast.BinaryExpr:
		a, e := assertionEval(x.X, input, budget)
		if e != nil {
			return nil, e
		}
		if x.Op == token.LAND || x.Op == token.LOR {
			b, e := assertionBool(a)
			if e != nil {
				return nil, e
			}
			if (x.Op == token.LAND && !b) || (x.Op == token.LOR && b) {
				return b, nil
			}
			v, e := assertionEval(x.Y, input, budget)
			if e != nil {
				return nil, e
			}
			return assertionBool(v)
		}
		b, e := assertionEval(x.Y, input, budget)
		if e != nil {
			return nil, e
		}
		if x.Op == token.EQL || x.Op == token.NEQ {
			ok, e := assertionEqual(a, b, budget)
			if x.Op == token.NEQ {
				ok = !ok
			}
			return ok, e
		}
		n, e := assertionNumber(a)
		if e != nil {
			return nil, e
		}
		m, e := assertionNumber(b)
		if e != nil {
			return nil, e
		}
		switch x.Op {
		case token.LSS:
			return n < m, nil
		case token.GTR:
			return n > m, nil
		case token.LEQ:
			return n <= m, nil
		case token.GEQ:
			return n >= m, nil
		}
	case *ast.CallExpr:
		name := x.Fun.(*ast.Ident).Name
		v, e := assertionEval(x.Args[0], input, budget)
		if e != nil {
			return nil, e
		}
		if assertionPredicate(name) {
			return assertionCheck(name, v), nil
		}
		if name == "size" {
			switch v := v.(type) {
			case []any:
				return float64(len(v)), nil
			case map[string]any:
				return float64(len(v)), nil
			case string:
				return float64(utf8.RuneCountInString(v)), nil
			}
			return nil, fmt.Errorf("size requires array, object, or string")
		}
		items, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("function requires array")
		}
		if len(items) > 4096 {
			return nil, fmt.Errorf("array exceeds 4096 elements")
		}
		if name == "unique" {
			for i, a := range items {
				for _, b := range items[:i] {
					if err := assertionSpend(budget); err != nil {
						return nil, err
					}
					ok, e := assertionEqual(a, b, budget)
					if e != nil {
						return nil, e
					}
					if ok {
						return false, nil
					}
				}
			}
			return true, nil
		}
		predicate := x.Args[1].(*ast.Ident).Name
		for _, v := range items {
			ok := assertionCheck(predicate, v)
			if name == "all" && !ok {
				return false, nil
			}
			if name == "any" && ok {
				return true, nil
			}
		}
		return name == "all", nil
	}
	return nil, fmt.Errorf("invalid expression")
}

// StructuredAssertions executes SDK-core's bounded JSON assertion contract.
func StructuredAssertions(request ScorerRequest) ScorerResult {
	results := []any{}
	failure := func(label, msg string) ScorerResult {
		return ScorerResult{Score: 0, Passed: false, Label: label, Explanation: msg, Metadata: map[string]any{"assertions": results}}
	}
	raw, err := json.Marshal(map[string]any{"output": request.Output, "input": request.Input, "expected": request.Expected, "config": request.Config})
	if err != nil || len(raw) > 1048576 {
		return failure("input_error", "input exceeds 1 MiB or is not JSON")
	}
	var input map[string]any
	if json.Unmarshal(raw, &input) != nil {
		return failure("input_error", "invalid JSON input")
	}
	if !assertionStructure(input) {
		return failure("input_error", "JSON structure exceeds limits")
	}
	cfg, ok := input["config"].(map[string]any)
	if !ok {
		return failure("config_error", "config must be an object")
	}
	assertions, ok := cfg["assertions"].([]any)
	if !ok || len(assertions) == 0 || len(assertions) > 64 {
		return failure("config_error", "assertions must contain 1..64 entries")
	}
	threshold := 1.0
	if v, ok := cfg["score_threshold"]; ok {
		var err error
		threshold, err = assertionNumber(v)
		if err != nil || threshold < 0 || threshold > 1 {
			return failure("config_error", "score_threshold must be between 0 and 1")
		}
	}
	type compiled struct {
		name, expr string
		tree       ast.Expr
	}
	all := []compiled{}
	names := map[string]bool{}
	for i, a := range assertions {
		m, ok := a.(map[string]any)
		if !ok {
			return failure("config_error", "invalid assertion")
		}
		name := "assertion_" + strconv.Itoa(i+1)
		if v, ok := m["name"]; ok {
			var valid bool
			name, valid = v.(string)
			if !valid || len(name) == 0 || len(name) > 256 {
				return failure("config_error", "invalid assertion name")
			}
		}
		if names[name] {
			return failure("config_error", "duplicate assertion name")
		}
		names[name] = true
		expr, ok := m["expr"].(string)
		if !ok || len(expr) == 0 || len(expr) > 4096 {
			return failure("config_error", "expression must contain 1..4096 bytes")
		}
		parser := assertionParser{text: expr}
		tree, err := parser.expr(0)
		if err != nil {
			return failure("config_error", err.Error())
		}
		parser.space()
		if parser.pos != len(expr) {
			return failure("config_error", "unexpected expression suffix")
		}

		all = append(all, compiled{name, expr, tree})
	}
	passed := 0
	budget := 100000
	for _, a := range all {
		v, err := assertionEval(a.tree, input, &budget)
		ok := false
		if err == nil {
			ok, err = assertionBool(v)
		}
		result := map[string]any{"name": a.name, "expr": a.expr, "passed": ok}
		if err != nil {
			result["passed"] = false
			result["error"] = err.Error()
			results = append(results, result)
			return failure("input_error", err.Error())
		}
		if ok {
			passed++
		}
		results = append(results, result)
	}
	score := float64(passed) / float64(len(all))
	label := "fail"
	if score >= threshold {
		label = "pass"
	}
	return ScorerResult{Score: score, Passed: score >= threshold, Label: label, Explanation: fmt.Sprintf("%d/%d assertions passed", passed, len(all)), Metadata: map[string]any{"assertions": results}}
}
