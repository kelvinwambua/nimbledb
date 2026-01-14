package query

import (
	"fmt"
)

type NodeType int

const (
	NodeSelect NodeType = iota
	NodeInsert
	NodeUpdate
	NodeDelete
	NodeCreateTable
	NodeDropTable
)

type Statement interface {
	Type() NodeType
}

type SelectStmt struct {
	Columns   []string
	TableName string
	Where     Expr
	OrderBy   []OrderByClause
	Limit     int
	Joins     []JoinClause
}

func (s *SelectStmt) Type() NodeType { return NodeSelect }

type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]Expr
}

func (i *InsertStmt) Type() NodeType { return NodeInsert }

type UpdateStmt struct {
	TableName   string
	Assignments []Assignment
	Where       Expr
}

func (u *UpdateStmt) Type() NodeType { return NodeUpdate }

type DeleteStmt struct {
	TableName string
	Where     Expr
}

func (d *DeleteStmt) Type() NodeType { return NodeDelete }

type CreateTableStmt struct {
	TableName   string
	Columns     []ColumnDef
	Constraints []Constraint
}

func (c *CreateTableStmt) Type() NodeType { return NodeCreateTable }

type DropTableStmt struct {
	TableName string
}

func (d *DropTableStmt) Type() NodeType { return NodeDropTable }

type ColumnDef struct {
	Name    string
	Type    string
	MaxLen  int
	NotNull bool
}

type ConstraintType int

const (
	ConstraintPrimaryKey ConstraintType = iota
	ConstraintUnique
	ConstraintForeignKey
)

type Constraint struct {
	Type       ConstraintType
	Columns    []string
	RefTable   string
	RefColumns []string
}

type Assignment struct {
	Column string
	Value  Expr
}

type OrderByClause struct {
	Column string
	Desc   bool
}

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

type JoinClause struct {
	Type      JoinType
	TableName string
	On        Expr
}

type ExprType int

const (
	ExprLiteral ExprType = iota
	ExprColumn
	ExprBinary
	ExprUnary
	ExprFunction
)

type Expr interface {
	ExprType() ExprType
	String() string
}

type LiteralExpr struct {
	Value interface{}
}

func (l *LiteralExpr) ExprType() ExprType { return ExprLiteral }
func (l *LiteralExpr) String() string {
	return fmt.Sprintf("%v", l.Value)
}

type ColumnExpr struct {
	Table  string
	Column string
}

func (c *ColumnExpr) ExprType() ExprType { return ExprColumn }
func (c *ColumnExpr) String() string {
	if c.Table != "" {
		return c.Table + "." + c.Column
	}
	return c.Column
}

type BinaryOpType int

const (
	OpEqual BinaryOpType = iota
	OpNotEqual
	OpLessThan
	OpLessThanEqual
	OpGreaterThan
	OpGreaterThanEqual
	OpAnd
	OpOr
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
	OpLike
	OpIn
)

type BinaryExpr struct {
	Left  Expr
	Op    BinaryOpType
	Right Expr
}

func (b *BinaryExpr) ExprType() ExprType { return ExprBinary }
func (b *BinaryExpr) String() string {
	ops := map[BinaryOpType]string{
		OpEqual: "=", OpNotEqual: "!=", OpLessThan: "<",
		OpLessThanEqual: "<=", OpGreaterThan: ">", OpGreaterThanEqual: ">=",
		OpAnd: "AND", OpOr: "OR", OpAdd: "+", OpSubtract: "-",
		OpMultiply: "*", OpDivide: "/", OpLike: "LIKE", OpIn: "IN",
	}
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), ops[b.Op], b.Right.String())
}

type UnaryOpType int

const (
	OpNot UnaryOpType = iota
	OpNegate
	OpIsNull
	OpIsNotNull
)

type UnaryExpr struct {
	Op   UnaryOpType
	Expr Expr
}

func (u *UnaryExpr) ExprType() ExprType { return ExprUnary }
func (u *UnaryExpr) String() string {
	ops := map[UnaryOpType]string{
		OpNot: "NOT", OpNegate: "-", OpIsNull: "IS NULL", OpIsNotNull: "IS NOT NULL",
	}
	return fmt.Sprintf("(%s %s)", ops[u.Op], u.Expr.String())
}

type FunctionExpr struct {
	Name string
	Args []Expr
}

func (f *FunctionExpr) ExprType() ExprType { return ExprFunction }
func (f *FunctionExpr) String() string {
	return fmt.Sprintf("%s(...)", f.Name)
}
