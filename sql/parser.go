package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/kelvinwambua/nimbledb/query"
)

type Parser struct {
	tokens []string
	pos    int
}

func NewParser(sql string) *Parser {
	tokens := tokenize(sql)
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

func tokenize(sql string) []string {
	sql = strings.TrimSpace(sql)
	tokens := make([]string, 0)
	current := ""
	inString := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if ch == '\'' {
			if inString {
				current += string(ch)
				tokens = append(tokens, current)
				current = ""
				inString = false
			} else {
				if current != "" {
					tokens = append(tokens, current)
					current = ""
				}
				current += string(ch)
				inString = true
			}
			continue
		}

		if inString {
			current += string(ch)
			continue
		}

		if ch == ' ' || ch == ',' || ch == '(' || ch == ')' || ch == ';' {
			if current != "" {
				tokens = append(tokens, current)
				current = ""
			}
			if ch == ',' || ch == '(' || ch == ')' {
				tokens = append(tokens, string(ch))
			}
			continue
		}

		current += string(ch)
	}

	if current != "" {
		tokens = append(tokens, current)
	}

	return tokens
}

func (p *Parser) Parse() (query.Statement, error) {
	if p.pos >= len(p.tokens) {
		return nil, errors.New("empty query")
	}

	keyword := strings.ToUpper(p.tokens[p.pos])

	switch keyword {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	default:
		return nil, fmt.Errorf("unknown statement: %s", keyword)
	}
}

func (p *Parser) parseSelect() (*query.SelectStmt, error) {
	p.pos++

	stmt := &query.SelectStmt{
		Columns: make([]string, 0),
		Joins:   make([]query.JoinClause, 0),
		OrderBy: make([]query.OrderByClause, 0),
	}

	for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) != "FROM" {
		if p.tokens[p.pos] == "," {
			p.pos++
			continue
		}

		col := p.tokens[p.pos]
		if strings.Contains(col, ".") {
			parts := strings.Split(col, ".")
			col = parts[1]
		}
		stmt.Columns = append(stmt.Columns, col)
		p.pos++
	}

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "FROM" {
		return nil, errors.New("expected FROM clause")
	}
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected table name")
	}
	stmt.TableName = p.tokens[p.pos]
	p.pos++

	if p.pos < len(p.tokens) &&
		strings.ToUpper(p.tokens[p.pos]) != "WHERE" &&
		strings.ToUpper(p.tokens[p.pos]) != "JOIN" &&
		strings.ToUpper(p.tokens[p.pos]) != "INNER" &&
		strings.ToUpper(p.tokens[p.pos]) != "LEFT" &&
		strings.ToUpper(p.tokens[p.pos]) != "RIGHT" &&
		strings.ToUpper(p.tokens[p.pos]) != "ORDER" &&
		strings.ToUpper(p.tokens[p.pos]) != "LIMIT" {
		p.pos++
	}

	for p.pos < len(p.tokens) {
		keyword := strings.ToUpper(p.tokens[p.pos])

		switch keyword {
		case "WHERE":
			p.pos++
			where, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Where = where

		case "JOIN", "INNER", "LEFT", "RIGHT":
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, join)

		case "ORDER":
			p.pos++
			if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "BY" {
				return nil, errors.New("expected BY after ORDER")
			}
			p.pos++

			for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) != "LIMIT" {
				if p.tokens[p.pos] == "," {
					p.pos++
					continue
				}

				col := p.tokens[p.pos]
				p.pos++

				desc := false
				if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "DESC" {
					desc = true
					p.pos++
				} else if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "ASC" {
					p.pos++
				}

				stmt.OrderBy = append(stmt.OrderBy, query.OrderByClause{
					Column: col,
					Desc:   desc,
				})
			}

		case "LIMIT":
			p.pos++
			if p.pos >= len(p.tokens) {
				return nil, errors.New("expected number after LIMIT")
			}
			limit, err := strconv.Atoi(p.tokens[p.pos])
			if err != nil {
				return nil, err
			}
			stmt.Limit = limit
			p.pos++

		default:
			p.pos++
		}
	}

	return stmt, nil
}

func (p *Parser) parseInsert() (*query.InsertStmt, error) {
	p.pos++

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "INTO" {
		return nil, errors.New("expected INTO after INSERT")
	}
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected table name")
	}

	stmt := &query.InsertStmt{
		TableName: p.tokens[p.pos],
		Columns:   make([]string, 0),
		Values:    make([][]query.Expr, 0),
	}
	p.pos++

	if p.pos < len(p.tokens) && p.tokens[p.pos] == "(" {
		p.pos++
		for p.pos < len(p.tokens) && p.tokens[p.pos] != ")" {
			if p.tokens[p.pos] == "," {
				p.pos++
				continue
			}
			stmt.Columns = append(stmt.Columns, p.tokens[p.pos])
			p.pos++
		}
		p.pos++
	}

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "VALUES" {
		return nil, errors.New("expected VALUES clause")
	}
	p.pos++

	for p.pos < len(p.tokens) {
		if p.tokens[p.pos] != "(" {
			break
		}
		p.pos++

		row := make([]query.Expr, 0)
		for p.pos < len(p.tokens) && p.tokens[p.pos] != ")" {
			if p.tokens[p.pos] == "," {
				p.pos++
				continue
			}

			val, err := p.parseLiteral()
			if err != nil {
				return nil, err
			}
			row = append(row, val)
		}
		p.pos++

		stmt.Values = append(stmt.Values, row)

		if p.pos < len(p.tokens) && p.tokens[p.pos] == "," {
			p.pos++
		}
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (*query.UpdateStmt, error) {
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected table name")
	}

	stmt := &query.UpdateStmt{
		TableName:   p.tokens[p.pos],
		Assignments: make([]query.Assignment, 0),
	}
	p.pos++

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "SET" {
		return nil, errors.New("expected SET clause")
	}
	p.pos++

	for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) != "WHERE" {
		if p.tokens[p.pos] == "," {
			p.pos++
			continue
		}

		col := p.tokens[p.pos]
		p.pos++

		if p.pos >= len(p.tokens) || p.tokens[p.pos] != "=" {
			return nil, errors.New("expected = in assignment")
		}
		p.pos++

		val, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Assignments = append(stmt.Assignments, query.Assignment{
			Column: col,
			Value:  val,
		})
	}

	if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "WHERE" {
		p.pos++
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseDelete() (*query.DeleteStmt, error) {
	p.pos++

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "FROM" {
		return nil, errors.New("expected FROM after DELETE")
	}
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected table name")
	}

	stmt := &query.DeleteStmt{
		TableName: p.tokens[p.pos],
	}
	p.pos++

	if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "WHERE" {
		p.pos++
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (query.Statement, error) {
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected TABLE after CREATE")
	}

	if strings.ToUpper(p.tokens[p.pos]) != "TABLE" {
		return nil, errors.New("only CREATE TABLE is supported")
	}
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected table name")
	}

	stmt := &query.CreateTableStmt{
		TableName:   p.tokens[p.pos],
		Columns:     make([]query.ColumnDef, 0),
		Constraints: make([]query.Constraint, 0),
	}
	p.pos++

	if p.pos >= len(p.tokens) || p.tokens[p.pos] != "(" {
		return nil, errors.New("expected (")
	}
	p.pos++

	for p.pos < len(p.tokens) && p.tokens[p.pos] != ")" {
		if p.tokens[p.pos] == "," {
			p.pos++
			continue
		}

		if strings.ToUpper(p.tokens[p.pos]) == "PRIMARY" {
			p.pos++
			if strings.ToUpper(p.tokens[p.pos]) != "KEY" {
				return nil, errors.New("expected KEY after PRIMARY")
			}
			p.pos++

			if p.tokens[p.pos] != "(" {
				return nil, errors.New("expected (")
			}
			p.pos++

			cols := make([]string, 0)
			for p.tokens[p.pos] != ")" {
				if p.tokens[p.pos] == "," {
					p.pos++
					continue
				}
				cols = append(cols, p.tokens[p.pos])
				p.pos++
			}
			p.pos++

			stmt.Constraints = append(stmt.Constraints, query.Constraint{
				Type:    query.ConstraintPrimaryKey,
				Columns: cols,
			})
			continue
		}

		if strings.ToUpper(p.tokens[p.pos]) == "UNIQUE" {
			p.pos++
			if p.tokens[p.pos] != "(" {
				return nil, errors.New("expected (")
			}
			p.pos++

			cols := make([]string, 0)
			for p.tokens[p.pos] != ")" {
				if p.tokens[p.pos] == "," {
					p.pos++
					continue
				}
				cols = append(cols, p.tokens[p.pos])
				p.pos++
			}
			p.pos++

			stmt.Constraints = append(stmt.Constraints, query.Constraint{
				Type:    query.ConstraintUnique,
				Columns: cols,
			})
			continue
		}

		colName := p.tokens[p.pos]
		p.pos++

		if p.pos >= len(p.tokens) {
			return nil, errors.New("expected column type")
		}

		colType := strings.ToUpper(p.tokens[p.pos])
		p.pos++

		maxLen := 0
		if p.pos < len(p.tokens) && p.tokens[p.pos] == "(" {
			p.pos++
			maxLen, _ = strconv.Atoi(p.tokens[p.pos])
			p.pos++
			if p.tokens[p.pos] == ")" {
				p.pos++
			}
		}

		notNull := false
		if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "NOT" {
			p.pos++
			if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "NULL" {
				notNull = true
				p.pos++
			}
		}

		stmt.Columns = append(stmt.Columns, query.ColumnDef{
			Name:    colName,
			Type:    colType,
			MaxLen:  maxLen,
			NotNull: notNull,
		})
	}
	p.pos++

	return stmt, nil
}

func (p *Parser) parseDrop() (*query.DropTableStmt, error) {
	p.pos++

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "TABLE" {
		return nil, errors.New("expected TABLE after DROP")
	}
	p.pos++

	if p.pos >= len(p.tokens) {
		return nil, errors.New("expected table name")
	}

	return &query.DropTableStmt{
		TableName: p.tokens[p.pos],
	}, nil
}

func (p *Parser) parseJoin() (query.JoinClause, error) {
	joinType := query.InnerJoin

	keyword := strings.ToUpper(p.tokens[p.pos])
	if keyword == "LEFT" {
		joinType = query.LeftJoin
		p.pos++
	} else if keyword == "RIGHT" {
		joinType = query.RightJoin
		p.pos++
	} else if keyword == "INNER" {
		p.pos++
	}

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "JOIN" {
		return query.JoinClause{}, errors.New("expected JOIN")
	}
	p.pos++

	if p.pos >= len(p.tokens) {
		return query.JoinClause{}, errors.New("expected table name")
	}

	tableName := p.tokens[p.pos]
	p.pos++

	if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) != "ON" {
		p.pos++
	}

	if p.pos >= len(p.tokens) || strings.ToUpper(p.tokens[p.pos]) != "ON" {
		return query.JoinClause{}, errors.New("expected ON clause")
	}
	p.pos++

	onExpr, err := p.parseExpression()
	if err != nil {
		return query.JoinClause{}, err
	}

	return query.JoinClause{
		Type:      joinType,
		TableName: tableName,
		On:        onExpr,
	}, nil
}

func (p *Parser) parseExpression() (query.Expr, error) {
	return p.parseOrExpression()
}

func (p *Parser) parseOrExpression() (query.Expr, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "OR" {
		p.pos++
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &query.BinaryExpr{
			Left:  left,
			Op:    query.OpOr,
			Right: right,
		}
	}

	return left, nil
}

func (p *Parser) parseAndExpression() (query.Expr, error) {
	left, err := p.parseComparisonExpression()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "AND" {
		p.pos++
		right, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		left = &query.BinaryExpr{
			Left:  left,
			Op:    query.OpAnd,
			Right: right,
		}
	}

	return left, nil
}

func (p *Parser) parseComparisonExpression() (query.Expr, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	if p.pos >= len(p.tokens) {
		return left, nil
	}

	op := p.tokens[p.pos]
	var binOp query.BinaryOpType

	switch op {
	case "=":
		binOp = query.OpEqual
	case "!=", "<>":
		binOp = query.OpNotEqual
	case "<":
		binOp = query.OpLessThan
	case "<=":
		binOp = query.OpLessThanEqual
	case ">":
		binOp = query.OpGreaterThan
	case ">=":
		binOp = query.OpGreaterThanEqual
	default:
		if strings.ToUpper(op) == "LIKE" {
			binOp = query.OpLike
		} else {
			return left, nil
		}
	}

	p.pos++
	right, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	return &query.BinaryExpr{
		Left:  left,
		Op:    binOp,
		Right: right,
	}, nil
}

func (p *Parser) parsePrimaryExpression() (query.Expr, error) {
	if p.pos >= len(p.tokens) {
		return nil, errors.New("unexpected end of expression")
	}

	token := p.tokens[p.pos]

	if token == "(" {
		p.pos++
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if p.pos >= len(p.tokens) || p.tokens[p.pos] != ")" {
			return nil, errors.New("expected )")
		}
		p.pos++
		return expr, nil
	}

	if strings.ToUpper(token) == "NOT" {
		p.pos++
		expr, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		return &query.UnaryExpr{
			Op:   query.OpNot,
			Expr: expr,
		}, nil
	}

	if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") {
		p.pos++
		return &query.LiteralExpr{
			Value: strings.Trim(token, "'"),
		}, nil
	}

	if strings.ToUpper(token) == "NULL" {
		p.pos++
		return &query.LiteralExpr{Value: nil}, nil
	}

	if strings.ToUpper(token) == "TRUE" {
		p.pos++
		return &query.LiteralExpr{Value: true}, nil
	}

	if strings.ToUpper(token) == "FALSE" {
		p.pos++
		return &query.LiteralExpr{Value: false}, nil
	}

	if val, err := strconv.ParseInt(token, 10, 64); err == nil {
		p.pos++
		return &query.LiteralExpr{Value: val}, nil
	}

	if val, err := strconv.ParseFloat(token, 64); err == nil {
		p.pos++
		return &query.LiteralExpr{Value: val}, nil
	}

	if strings.Contains(token, ".") {
		parts := strings.Split(token, ".")
		if len(parts) == 2 {
			p.pos++
			return &query.ColumnExpr{
				Column: parts[1],
			}, nil
		}
	}

	if isIdentifier(token) {
		p.pos++
		return &query.ColumnExpr{
			Column: token,
		}, nil
	}

	return nil, fmt.Errorf("invalid expression: %s", token)
}

func (p *Parser) parseLiteral() (query.Expr, error) {
	if p.pos >= len(p.tokens) {
		return nil, errors.New("unexpected end of expression")
	}

	token := p.tokens[p.pos]
	p.pos++

	if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") {
		return &query.LiteralExpr{
			Value: strings.Trim(token, "'"),
		}, nil
	}

	if strings.ToUpper(token) == "NULL" {
		return &query.LiteralExpr{Value: nil}, nil
	}

	if strings.ToUpper(token) == "TRUE" {
		return &query.LiteralExpr{Value: true}, nil
	}

	if strings.ToUpper(token) == "FALSE" {
		return &query.LiteralExpr{Value: false}, nil
	}

	if strings.Contains(token, ".") {
		val, err := strconv.ParseFloat(token, 64)
		if err == nil {
			return &query.LiteralExpr{Value: val}, nil
		}
	}

	val, err := strconv.ParseInt(token, 10, 64)
	if err == nil {
		return &query.LiteralExpr{Value: val}, nil
	}

	return nil, fmt.Errorf("invalid literal: %s", token)
}

func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	if strings.HasPrefix(s, "'") {
		return false
	}
	for _, ch := range s {
		if !(ch >= 'a' && ch <= 'z') && !(ch >= 'A' && ch <= 'Z') && !(ch >= '0' && ch <= '9') && ch != '_' {
			return false
		}
	}
	return true
}
