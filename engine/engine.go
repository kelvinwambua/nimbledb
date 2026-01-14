package engine

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/kelvinwambua/nimbledb/index"
	"github.com/kelvinwambua/nimbledb/query"
	"github.com/kelvinwambua/nimbledb/storage"
)

type Engine struct {
	tables  map[string]*TableMetadata
	indexes map[string]*index.Index
	pagers  map[string]*storage.Pager
	mu      sync.RWMutex
	dataDir string
}

type TableMetadata struct {
	Name       string
	Columns    []storage.Column
	Table      *storage.Table
	Indexes    []*index.Index
	PrimaryKey []string
	UniqueKeys [][]string
}

func NewEngine(dataDir string) *Engine {
	return &Engine{
		tables:  make(map[string]*TableMetadata),
		indexes: make(map[string]*index.Index),
		pagers:  make(map[string]*storage.Pager),
		dataDir: dataDir,
	}
}

func (e *Engine) Execute(stmt query.Statement) ([]string, [][]interface{}, error) {
	switch s := stmt.(type) {
	case *query.SelectStmt:
		return e.executeSelect(s)
	case *query.InsertStmt:
		return e.executeInsert(s)
	case *query.UpdateStmt:
		return e.executeUpdate(s)
	case *query.DeleteStmt:
		return e.executeDelete(s)
	case *query.CreateTableStmt:
		return e.executeCreateTable(s)
	case *query.DropTableStmt:
		return e.executeDropTable(s)
	default:
		return nil, nil, errors.New("unsupported statement type")
	}
}

func (e *Engine) executeCreateTable(stmt *query.CreateTableStmt) ([]string, [][]interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tables[stmt.TableName]; exists {
		return nil, nil, fmt.Errorf("table %s already exists", stmt.TableName)
	}

	columns := make([]storage.Column, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		var dataType storage.DataType
		var maxLen uint16 = 0

		switch colDef.Type {
		case "INT", "INTEGER":
			dataType = storage.TypeInt
		case "VARCHAR", "TEXT", "STRING":
			dataType = storage.TypeVarchar
			if colDef.MaxLen > 0 {
				maxLen = uint16(colDef.MaxLen)
			} else {
				maxLen = 255
			}
		case "BOOL", "BOOLEAN":
			dataType = storage.TypeBool
		case "FLOAT", "DOUBLE":
			dataType = storage.TypeFloat
		case "DATE", "DATETIME":
			dataType = storage.TypeDate
		default:
			return nil, nil, fmt.Errorf("unsupported data type: %s", colDef.Type)
		}

		columns[i] = storage.Column{
			Name:    colDef.Name,
			Type:    dataType,
			MaxLen:  maxLen,
			NotNull: colDef.NotNull,
		}
	}

	pager, err := storage.NewPager(e.dataDir + "/" + stmt.TableName + ".db")
	if err != nil {
		return nil, nil, err
	}

	table, err := storage.NewTable(stmt.TableName, columns, pager)
	if err != nil {
		return nil, nil, err
	}

	metadata := &TableMetadata{
		Name:    stmt.TableName,
		Columns: columns,
		Table:   table,
		Indexes: make([]*index.Index, 0),
	}

	for _, constraint := range stmt.Constraints {
		switch constraint.Type {
		case query.ConstraintPrimaryKey:
			metadata.PrimaryKey = constraint.Columns

			for i, col := range columns {
				for _, pkCol := range constraint.Columns {
					if col.Name == pkCol {
						columns[i].IsPrimary = true
					}
				}
			}

			idx := index.NewIndex(
				stmt.TableName+"_pk",
				stmt.TableName,
				constraint.Columns,
				true,
				true,
			)
			metadata.Indexes = append(metadata.Indexes, idx)
			e.indexes[stmt.TableName+"_pk"] = idx

		case query.ConstraintUnique:
			metadata.UniqueKeys = append(metadata.UniqueKeys, constraint.Columns)

			for i, col := range columns {
				for _, ukCol := range constraint.Columns {
					if col.Name == ukCol {
						columns[i].IsUnique = true
					}
				}
			}

			idx := index.NewIndex(
				stmt.TableName+"_uk_"+constraint.Columns[0],
				stmt.TableName,
				constraint.Columns,
				true,
				false,
			)
			metadata.Indexes = append(metadata.Indexes, idx)
			e.indexes[stmt.TableName+"_uk_"+constraint.Columns[0]] = idx
		}
	}

	e.tables[stmt.TableName] = metadata
	e.pagers[stmt.TableName] = pager

	return []string{"message"}, [][]interface{}{{"Table created successfully"}}, nil
}

func (e *Engine) executeDropTable(stmt *query.DropTableStmt) ([]string, [][]interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tables[stmt.TableName]; !exists {
		return nil, nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if pager, exists := e.pagers[stmt.TableName]; exists {
		pager.Close()
	}

	delete(e.tables, stmt.TableName)
	delete(e.pagers, stmt.TableName)

	return []string{"message"}, [][]interface{}{{"Table dropped successfully"}}, nil
}

func (e *Engine) executeInsert(stmt *query.InsertStmt) ([]string, [][]interface{}, error) {
	e.mu.RLock()
	metadata, exists := e.tables[stmt.TableName]
	e.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	insertedRows := 0

	for _, valueRow := range stmt.Values {
		tuple := storage.NewTuple(metadata.Columns)

		colMap := make(map[string]int)
		if len(stmt.Columns) > 0 {
			for i, col := range stmt.Columns {
				colMap[col] = i
			}
		} else {
			for i, col := range metadata.Columns {
				colMap[col.Name] = i
			}
		}

		for colIdx, col := range metadata.Columns {
			valueIdx, found := colMap[col.Name]
			if !found {
				if col.NotNull {
					return nil, nil, fmt.Errorf("column %s cannot be null", col.Name)
				}
				tuple.SetValue(colIdx, nil)
				continue
			}

			valueExpr := valueRow[valueIdx]
			value, err := e.evaluateExpr(valueExpr, nil)
			if err != nil {
				return nil, nil, err
			}

			convertedValue, err := e.convertValue(value, col.Type)
			if err != nil {
				return nil, nil, err
			}

			if err := tuple.SetValue(colIdx, convertedValue); err != nil {
				return nil, nil, err
			}
		}

		// FIRST: Check unique constraints BEFORE inserting
		for _, idx := range metadata.Indexes {
			if len(idx.Columns) == 1 {
				colIdx := e.findColumnIndex(metadata.Columns, idx.Columns[0])
				if colIdx >= 0 {
					keyValue, _ := tuple.GetValue(colIdx)
					// Check if key already exists
					if _, found := idx.Search(keyValue); found {
						return nil, nil, fmt.Errorf("constraint violation: unique constraint violation")
					}
				}
			}
		}

		// SECOND: Insert into table and get the actual location
		pageID, slotID, err := metadata.Table.Insert(tuple)
		if err != nil {
			return nil, nil, err
		}

		// THIRD: Insert into indexes with the correct location
		for _, idx := range metadata.Indexes {
			if len(idx.Columns) == 1 {
				colIdx := e.findColumnIndex(metadata.Columns, idx.Columns[0])
				if colIdx >= 0 {
					keyValue, _ := tuple.GetValue(colIdx)
					if err := idx.Insert(keyValue, pageID, uint32(slotID)); err != nil {
						return nil, nil, fmt.Errorf("constraint violation: %v", err)
					}
				}
			}
		}

		insertedRows++
	}

	return []string{"rows_affected"}, [][]interface{}{{int64(insertedRows)}}, nil
}

func (e *Engine) executeSelect(stmt *query.SelectStmt) ([]string, [][]interface{}, error) {
	e.mu.RLock()
	metadata, exists := e.tables[stmt.TableName]
	e.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var filter func(*storage.Tuple) bool
	if stmt.Where != nil && len(stmt.Joins) == 0 {
		filter = func(tuple *storage.Tuple) bool {
			result, err := e.evaluateExprWithTable(stmt.Where, tuple, metadata)
			fmt.Printf("DEBUG: Filter called, result=%v, err=%v\n", result, err)
			if err != nil {
				fmt.Printf("DEBUG: Error details: %v\n", err)
				return false
			}
			if b, ok := result.(bool); ok {
				return b
			}
			return false
		}
	}

	tuples, err := metadata.Table.Scan(filter)
	if err != nil {
		return nil, nil, err
	}

	combinedColumns := metadata.Columns

	if len(stmt.Joins) > 0 {
		for _, join := range stmt.Joins {
			tuples, err = e.executeJoin(metadata, tuples, join)
			if err != nil {
				return nil, nil, err
			}

			e.mu.RLock()
			rightMeta, exists := e.tables[join.TableName]
			e.mu.RUnlock()
			if exists {
				combinedColumns = append(combinedColumns, rightMeta.Columns...)
			}
		}

		if stmt.Where != nil {
			filteredTuples := make([]*storage.Tuple, 0)
			combinedMeta := &TableMetadata{
				Columns: combinedColumns,
			}
			for _, tuple := range tuples {
				result, err := e.evaluateExprWithTable(stmt.Where, tuple, combinedMeta)
				if err == nil {
					if b, ok := result.(bool); ok && b {
						filteredTuples = append(filteredTuples, tuple)
					}
				}
			}
			tuples = filteredTuples
		}
	}

	var columns []string
	var columnIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		columns = make([]string, len(combinedColumns))
		columnIndices = make([]int, len(combinedColumns))
		for i, col := range combinedColumns {
			columns[i] = col.Name
			columnIndices[i] = i
		}
	} else {
		columns = stmt.Columns
		columnIndices = make([]int, len(stmt.Columns))
		for i, colName := range stmt.Columns {
			idx := e.findColumnIndex(combinedColumns, colName)
			if idx < 0 {
				return nil, nil, fmt.Errorf("column %s not found", colName)
			}
			columnIndices[i] = idx
		}
	}

	rows := make([][]interface{}, 0, len(tuples))
	for _, tuple := range tuples {
		row := make([]interface{}, len(columnIndices))
		for i, colIdx := range columnIndices {
			val, _ := tuple.GetValue(colIdx)
			row[i] = val
		}
		rows = append(rows, row)
	}

	if len(stmt.OrderBy) > 0 {
		rows = e.sortRows(rows, columns, stmt.OrderBy, metadata)
	}

	if stmt.Limit > 0 && len(rows) > stmt.Limit {
		rows = rows[:stmt.Limit]
	}

	return columns, rows, nil
}

func (e *Engine) executeUpdate(stmt *query.UpdateStmt) ([]string, [][]interface{}, error) {
	e.mu.RLock()
	metadata, exists := e.tables[stmt.TableName]
	e.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var filter func(*storage.Tuple) bool
	if stmt.Where != nil {
		filter = func(tuple *storage.Tuple) bool {
			result, err := e.evaluateExprWithTable(stmt.Where, tuple, metadata)
			if err != nil {
				return false
			}
			if b, ok := result.(bool); ok {
				return b
			}
			return false
		}
	} else {
		filter = func(*storage.Tuple) bool { return true }
	}

	updater := func(tuple *storage.Tuple) error {
		for _, assignment := range stmt.Assignments {
			colIdx := e.findColumnIndex(metadata.Columns, assignment.Column)
			if colIdx < 0 {
				return fmt.Errorf("column %s not found", assignment.Column)
			}

			value, err := e.evaluateExprWithTable(assignment.Value, tuple, metadata)
			if err != nil {
				return err
			}

			convertedValue, err := e.convertValue(value, metadata.Columns[colIdx].Type)
			if err != nil {
				return err
			}

			if err := tuple.SetValue(colIdx, convertedValue); err != nil {
				return err
			}
		}
		return nil
	}

	count, err := metadata.Table.Update(filter, updater)
	if err != nil {
		return nil, nil, err
	}

	return []string{"rows_affected"}, [][]interface{}{{int64(count)}}, nil
}

func (e *Engine) executeDelete(stmt *query.DeleteStmt) ([]string, [][]interface{}, error) {
	e.mu.RLock()
	metadata, exists := e.tables[stmt.TableName]
	e.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var filter func(*storage.Tuple) bool
	if stmt.Where != nil {
		filter = func(tuple *storage.Tuple) bool {
			result, err := e.evaluateExprWithTable(stmt.Where, tuple, metadata)
			if err != nil {
				return false
			}
			if b, ok := result.(bool); ok {
				return b
			}
			return false
		}
	} else {
		filter = func(*storage.Tuple) bool { return true }
	}

	count, err := metadata.Table.Delete(filter)
	if err != nil {
		return nil, nil, err
	}

	return []string{"rows_affected"}, [][]interface{}{{int64(count)}}, nil
}

func (e *Engine) executeJoin(leftMeta *TableMetadata, leftTuples []*storage.Tuple, join query.JoinClause) ([]*storage.Tuple, error) {
	e.mu.RLock()
	rightMeta, exists := e.tables[join.TableName]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("table %s does not exist", join.TableName)
	}

	rightTuples, err := rightMeta.Table.Scan(nil)
	if err != nil {
		return nil, err
	}

	result := make([]*storage.Tuple, 0)

	for _, leftTuple := range leftTuples {
		matched := false

		for _, rightTuple := range rightTuples {
			if join.On != nil {
				joinResult, err := e.evaluateJoinCondition(join.On, leftTuple, rightTuple, leftMeta, rightMeta)
				if err != nil {
					continue
				}

				if b, ok := joinResult.(bool); ok && b {
					matched = true
					combinedTuple := e.combineTuples(leftTuple, rightTuple, leftMeta, rightMeta)
					result = append(result, combinedTuple)
				}
			}
		}

		if !matched && join.Type == query.LeftJoin {
			result = append(result, leftTuple)
		}
	}

	return result, nil
}

func (e *Engine) combineTuples(left, right *storage.Tuple, leftMeta, rightMeta *TableMetadata) *storage.Tuple {
	combinedColumns := append(leftMeta.Columns, rightMeta.Columns...)
	combined := storage.NewTuple(combinedColumns)

	for i := 0; i < len(leftMeta.Columns); i++ {
		val, _ := left.GetValue(i)
		combined.SetValue(i, val)
	}

	for i := 0; i < len(rightMeta.Columns); i++ {
		val, _ := right.GetValue(i)
		combined.SetValue(len(leftMeta.Columns)+i, val)
	}

	return combined
}

func (e *Engine) evaluateExprWithTable(expr query.Expr, tuple *storage.Tuple, metadata *TableMetadata) (interface{}, error) {
	switch ex := expr.(type) {
	case *query.LiteralExpr:
		return ex.Value, nil

	case *query.ColumnExpr:
		if tuple == nil {
			return nil, errors.New("no tuple context for column evaluation")
		}

		colIdx := e.findColumnIndex(metadata.Columns, ex.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column %s not found", ex.Column)
		}

		return tuple.GetValue(colIdx)

	case *query.BinaryExpr:
		left, err := e.evaluateExprWithTable(ex.Left, tuple, metadata)
		if err != nil {
			return nil, err
		}

		right, err := e.evaluateExprWithTable(ex.Right, tuple, metadata)
		if err != nil {
			return nil, err
		}

		return e.evaluateBinaryOp(left, ex.Op, right)

	case *query.UnaryExpr:
		val, err := e.evaluateExprWithTable(ex.Expr, tuple, metadata)
		if err != nil {
			return nil, err
		}

		return e.evaluateUnaryOp(ex.Op, val)

	default:
		return nil, errors.New("unsupported expression type")
	}
}

func (e *Engine) evaluateExpr(expr query.Expr, tuple *storage.Tuple) (interface{}, error) {
	switch ex := expr.(type) {
	case *query.LiteralExpr:
		return ex.Value, nil

	case *query.ColumnExpr:
		if tuple == nil {
			return nil, errors.New("no tuple context for column evaluation")
		}

		metadata := e.getTableMetadata(ex.Column)
		if metadata == nil {
			return nil, fmt.Errorf("cannot find table for column %s", ex.Column)
		}

		colIdx := e.findColumnIndex(metadata.Columns, ex.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column %s not found", ex.Column)
		}

		return tuple.GetValue(colIdx)

	case *query.BinaryExpr:
		left, err := e.evaluateExpr(ex.Left, tuple)
		if err != nil {
			return nil, err
		}

		right, err := e.evaluateExpr(ex.Right, tuple)
		if err != nil {
			return nil, err
		}

		return e.evaluateBinaryOp(left, ex.Op, right)

	case *query.UnaryExpr:
		val, err := e.evaluateExpr(ex.Expr, tuple)
		if err != nil {
			return nil, err
		}

		return e.evaluateUnaryOp(ex.Op, val)

	default:
		return nil, errors.New("unsupported expression type")
	}
}

func (e *Engine) evaluateJoinCondition(expr query.Expr, leftTuple, rightTuple *storage.Tuple, leftMeta, rightMeta *TableMetadata) (interface{}, error) {
	switch ex := expr.(type) {
	case *query.BinaryExpr:
		if ex.Op == query.OpEqual {
			leftCol, ok1 := ex.Left.(*query.ColumnExpr)
			rightCol, ok2 := ex.Right.(*query.ColumnExpr)

			if ok1 && ok2 {
				leftIdx := e.findColumnIndex(leftMeta.Columns, leftCol.Column)
				rightIdx := e.findColumnIndex(rightMeta.Columns, rightCol.Column)

				if leftIdx >= 0 && rightIdx >= 0 {
					leftVal, _ := leftTuple.GetValue(leftIdx)
					rightVal, _ := rightTuple.GetValue(rightIdx)

					return e.compareValues(leftVal, rightVal) == 0, nil
				}
			}
		}
	}

	return false, nil
}

func (e *Engine) evaluateBinaryOp(left interface{}, op query.BinaryOpType, right interface{}) (interface{}, error) {
	switch op {
	case query.OpEqual:
		return e.compareValues(left, right) == 0, nil
	case query.OpNotEqual:
		return e.compareValues(left, right) != 0, nil
	case query.OpLessThan:
		return e.compareValues(left, right) < 0, nil
	case query.OpLessThanEqual:
		return e.compareValues(left, right) <= 0, nil
	case query.OpGreaterThan:
		return e.compareValues(left, right) > 0, nil
	case query.OpGreaterThanEqual:
		return e.compareValues(left, right) >= 0, nil
	case query.OpAnd:
		lb, _ := left.(bool)
		rb, _ := right.(bool)
		return lb && rb, nil
	case query.OpOr:
		lb, _ := left.(bool)
		rb, _ := right.(bool)
		return lb || rb, nil
	case query.OpAdd:
		return e.arithmeticOp(left, right, "+")
	case query.OpSubtract:
		return e.arithmeticOp(left, right, "-")
	case query.OpMultiply:
		return e.arithmeticOp(left, right, "*")
	case query.OpDivide:
		return e.arithmeticOp(left, right, "/")
	default:
		return nil, fmt.Errorf("unsupported binary operator: %d", op)
	}
}

func (e *Engine) evaluateUnaryOp(op query.UnaryOpType, val interface{}) (interface{}, error) {
	switch op {
	case query.OpNot:
		if b, ok := val.(bool); ok {
			return !b, nil
		}
		return nil, errors.New("NOT operator requires boolean operand")
	case query.OpNegate:
		if i, ok := val.(int64); ok {
			return -i, nil
		}
		if f, ok := val.(float64); ok {
			return -f, nil
		}
		return nil, errors.New("negate operator requires numeric operand")
	case query.OpIsNull:
		return val == nil, nil
	case query.OpIsNotNull:
		return val != nil, nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %d", op)
	}
}

func (e *Engine) compareValues(left, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	switch l := left.(type) {
	case int64:
		if r, ok := right.(int64); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
		// Handle int64 vs float64
		if r, ok := right.(float64); ok {
			lf := float64(l)
			if lf < r {
				return -1
			} else if lf > r {
				return 1
			}
			return 0
		}

	case string:
		if r, ok := right.(string); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}

	case float64:
		if r, ok := right.(float64); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
		// Handle float64 vs int64
		if r, ok := right.(int64); ok {
			rf := float64(r)
			if l < rf {
				return -1
			} else if l > rf {
				return 1
			}
			return 0
		}

	case bool:
		if r, ok := right.(bool); ok {
			if l == r {
				return 0
			}
			if !l && r {
				return -1
			}
			return 1
		}
	}

	return 0
}

func (e *Engine) arithmeticOp(left, right interface{}, op string) (interface{}, error) {
	l, lok := left.(int64)
	r, rok := right.(int64)

	if lok && rok {
		switch op {
		case "+":
			return l + r, nil
		case "-":
			return l - r, nil
		case "*":
			return l * r, nil
		case "/":
			if r == 0 {
				return nil, errors.New("division by zero")
			}
			return l / r, nil
		}
	}

	lf, lok := left.(float64)
	rf, rok := right.(float64)

	if lok && rok {
		switch op {
		case "+":
			return lf + rf, nil
		case "-":
			return lf - rf, nil
		case "*":
			return lf * rf, nil
		case "/":
			if rf == 0 {
				return nil, errors.New("division by zero")
			}
			return lf / rf, nil
		}
	}

	return nil, errors.New("arithmetic operation requires numeric operands")
}

func (e *Engine) convertValue(value interface{}, targetType storage.DataType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch targetType {
	case storage.TypeInt:
		switch v := value.(type) {
		case int64:
			return v, nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		case float64:
			return int64(v), nil
		}
	case storage.TypeVarchar:
		return fmt.Sprintf("%v", value), nil
	case storage.TypeBool:
		if b, ok := value.(bool); ok {
			return b, nil
		}
	case storage.TypeFloat:
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		}
	case storage.TypeDate:
		if t, ok := value.(time.Time); ok {
			return t, nil
		}
		if s, ok := value.(string); ok {
			return time.Parse("2006-01-02", s)
		}
	}

	return value, nil
}

func (e *Engine) findColumnIndex(columns []storage.Column, name string) int {
	for i, col := range columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (e *Engine) getTableMetadata(columnName string) *TableMetadata {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, meta := range e.tables {
		if e.findColumnIndex(meta.Columns, columnName) >= 0 {
			return meta
		}
	}
	return nil
}

func (e *Engine) sortRows(rows [][]interface{}, columns []string, orderBy []query.OrderByClause, metadata *TableMetadata) [][]interface{} {
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			shouldSwap := false

			for _, order := range orderBy {
				colIdx := -1
				for k, col := range columns {
					if col == order.Column {
						colIdx = k
						break
					}
				}

				if colIdx < 0 {
					continue
				}

				cmp := e.compareValues(rows[i][colIdx], rows[j][colIdx])

				if order.Desc {
					cmp = -cmp
				}

				if cmp > 0 {
					shouldSwap = true
					break
				} else if cmp < 0 {
					break
				}
			}

			if shouldSwap {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}

	return rows
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, pager := range e.pagers {
		if err := pager.Close(); err != nil {
			return err
		}
	}

	return nil
}
