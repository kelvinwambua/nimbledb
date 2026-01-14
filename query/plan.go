package query

import "fmt"

type PlanNodeType int

const (
	PlanTableScan PlanNodeType = iota
	PlanIndexScan
	PlanFilter
	PlanProject
	PlanSort
	PlanLimit
	PlanNestedLoopJoin
	PlanHashJoin
	PlanAggregate
)

type PlanNode interface {
	Type() PlanNodeType
	Cost() float64
	Cardinality() int
	Children() []PlanNode
	String() string
}

type TableScanPlan struct {
	TableName     string
	Alias         string
	EstimatedRows int
	EstimatedCost float64
}

func (t *TableScanPlan) Type() PlanNodeType   { return PlanTableScan }
func (t *TableScanPlan) Cost() float64        { return t.EstimatedCost }
func (t *TableScanPlan) Cardinality() int     { return t.EstimatedRows }
func (t *TableScanPlan) Children() []PlanNode { return nil }
func (t *TableScanPlan) String() string {
	return fmt.Sprintf("TableScan(%s) [cost=%.2f, rows=%d]", t.TableName, t.EstimatedCost, t.EstimatedRows)
}

type IndexScanPlan struct {
	TableName     string
	IndexName     string
	Conditions    []Expr
	EstimatedRows int
	EstimatedCost float64
}

func (i *IndexScanPlan) Type() PlanNodeType   { return PlanIndexScan }
func (i *IndexScanPlan) Cost() float64        { return i.EstimatedCost }
func (i *IndexScanPlan) Cardinality() int     { return i.EstimatedRows }
func (i *IndexScanPlan) Children() []PlanNode { return nil }
func (i *IndexScanPlan) String() string {
	return fmt.Sprintf("IndexScan(%s.%s) [cost=%.2f, rows=%d]", i.TableName, i.IndexName, i.EstimatedCost, i.EstimatedRows)
}

type FilterPlan struct {
	Child         PlanNode
	Predicate     Expr
	Selectivity   float64
	EstimatedCost float64
}

func (f *FilterPlan) Type() PlanNodeType { return PlanFilter }
func (f *FilterPlan) Cost() float64      { return f.EstimatedCost }
func (f *FilterPlan) Cardinality() int {
	return int(float64(f.Child.Cardinality()) * f.Selectivity)
}
func (f *FilterPlan) Children() []PlanNode { return []PlanNode{f.Child} }
func (f *FilterPlan) String() string {
	return fmt.Sprintf("Filter(%s) [cost=%.2f, selectivity=%.2f]", f.Predicate.String(), f.EstimatedCost, f.Selectivity)
}

type ProjectPlan struct {
	Child         PlanNode
	Columns       []string
	EstimatedCost float64
}

func (p *ProjectPlan) Type() PlanNodeType   { return PlanProject }
func (p *ProjectPlan) Cost() float64        { return p.EstimatedCost }
func (p *ProjectPlan) Cardinality() int     { return p.Child.Cardinality() }
func (p *ProjectPlan) Children() []PlanNode { return []PlanNode{p.Child} }
func (p *ProjectPlan) String() string {
	return fmt.Sprintf("Project(%v) [cost=%.2f]", p.Columns, p.EstimatedCost)
}

type SortPlan struct {
	Child         PlanNode
	OrderBy       []OrderByClause
	EstimatedCost float64
}

func (s *SortPlan) Type() PlanNodeType   { return PlanSort }
func (s *SortPlan) Cost() float64        { return s.EstimatedCost }
func (s *SortPlan) Cardinality() int     { return s.Child.Cardinality() }
func (s *SortPlan) Children() []PlanNode { return []PlanNode{s.Child} }
func (s *SortPlan) String() string {
	return fmt.Sprintf("Sort(%v) [cost=%.2f]", s.OrderBy, s.EstimatedCost)
}

type LimitPlan struct {
	Child         PlanNode
	Count         int
	EstimatedCost float64
}

func (l *LimitPlan) Type() PlanNodeType { return PlanLimit }
func (l *LimitPlan) Cost() float64      { return l.EstimatedCost }
func (l *LimitPlan) Cardinality() int {
	childCard := l.Child.Cardinality()
	if childCard < l.Count {
		return childCard
	}
	return l.Count
}
func (l *LimitPlan) Children() []PlanNode { return []PlanNode{l.Child} }
func (l *LimitPlan) String() string {
	return fmt.Sprintf("Limit(%d) [cost=%.2f]", l.Count, l.EstimatedCost)
}

type NestedLoopJoinPlan struct {
	Left          PlanNode
	Right         PlanNode
	JoinType      JoinType
	Condition     Expr
	EstimatedCost float64
}

func (n *NestedLoopJoinPlan) Type() PlanNodeType { return PlanNestedLoopJoin }
func (n *NestedLoopJoinPlan) Cost() float64      { return n.EstimatedCost }
func (n *NestedLoopJoinPlan) Cardinality() int {
	return n.Left.Cardinality() * n.Right.Cardinality() / 10
}
func (n *NestedLoopJoinPlan) Children() []PlanNode { return []PlanNode{n.Left, n.Right} }
func (n *NestedLoopJoinPlan) String() string {
	return fmt.Sprintf("NestedLoopJoin [cost=%.2f]", n.EstimatedCost)
}

type HashJoinPlan struct {
	Left          PlanNode
	Right         PlanNode
	JoinType      JoinType
	LeftKeys      []string
	RightKeys     []string
	EstimatedCost float64
}

func (h *HashJoinPlan) Type() PlanNodeType { return PlanHashJoin }
func (h *HashJoinPlan) Cost() float64      { return h.EstimatedCost }
func (h *HashJoinPlan) Cardinality() int {
	return h.Left.Cardinality() * h.Right.Cardinality() / 10
}
func (h *HashJoinPlan) Children() []PlanNode { return []PlanNode{h.Left, h.Right} }
func (h *HashJoinPlan) String() string {
	return fmt.Sprintf("HashJoin [cost=%.2f]", h.EstimatedCost)
}
