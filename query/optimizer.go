package query

import (
	"fmt"
	"math"
)

type Statistics struct {
	TableName   string
	RowCount    int
	ColumnStats map[string]*ColumnStats
	IndexStats  map[string]*IndexStats
}

type ColumnStats struct {
	DistinctValues int
	NullCount      int
	MinValue       interface{}
	MaxValue       interface{}
}

type IndexStats struct {
	IndexName   string
	Columns     []string
	Height      int
	LeafPages   int
	Selectivity float64
}

type Optimizer struct {
	stats map[string]*Statistics
}

func NewOptimizer() *Optimizer {
	return &Optimizer{
		stats: make(map[string]*Statistics),
	}
}

func (o *Optimizer) RegisterTableStats(stats *Statistics) {
	o.stats[stats.TableName] = stats
}

func (o *Optimizer) Optimize(stmt Statement) (PlanNode, error) {
	switch s := stmt.(type) {
	case *SelectStmt:
		return o.optimizeSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement type for optimization")
	}
}

func (o *Optimizer) optimizeSelect(stmt *SelectStmt) (PlanNode, error) {
	stats, ok := o.stats[stmt.TableName]
	if !ok {
		stats = &Statistics{
			TableName:   stmt.TableName,
			RowCount:    1000,
			ColumnStats: make(map[string]*ColumnStats),
			IndexStats:  make(map[string]*IndexStats),
		}
	}

	var plan PlanNode

	indexPlan := o.tryIndexScan(stmt, stats)
	tableScanPlan := o.createTableScan(stmt, stats)

	if indexPlan != nil && indexPlan.Cost() < tableScanPlan.Cost() {
		plan = indexPlan
	} else {
		plan = tableScanPlan
	}

	if stmt.Where != nil {
		selectivity := o.estimateSelectivity(stmt.Where, stats)
		filterCost := plan.Cost() + float64(plan.Cardinality())*0.01
		plan = &FilterPlan{
			Child:         plan,
			Predicate:     stmt.Where,
			Selectivity:   selectivity,
			EstimatedCost: filterCost,
		}
	}

	if len(stmt.Joins) > 0 {
		for _, join := range stmt.Joins {
			plan = o.optimizeJoin(plan, join, stats)
		}
	}

	if len(stmt.Columns) > 0 && !(len(stmt.Columns) == 1 && stmt.Columns[0] == "*") {
		projectCost := plan.Cost() + float64(plan.Cardinality())*0.001
		plan = &ProjectPlan{
			Child:         plan,
			Columns:       stmt.Columns,
			EstimatedCost: projectCost,
		}
	}

	if len(stmt.OrderBy) > 0 {
		sortCost := plan.Cost() + float64(plan.Cardinality())*math.Log2(float64(plan.Cardinality()))*0.01
		plan = &SortPlan{
			Child:         plan,
			OrderBy:       stmt.OrderBy,
			EstimatedCost: sortCost,
		}
	}

	if stmt.Limit > 0 {
		limitCost := plan.Cost() * 0.1
		plan = &LimitPlan{
			Child:         plan,
			Count:         stmt.Limit,
			EstimatedCost: limitCost,
		}
	}

	return plan, nil
}

func (o *Optimizer) createTableScan(stmt *SelectStmt, stats *Statistics) *TableScanPlan {
	cost := float64(stats.RowCount) * 0.1
	return &TableScanPlan{
		TableName:     stmt.TableName,
		EstimatedRows: stats.RowCount,
		EstimatedCost: cost,
	}
}

func (o *Optimizer) tryIndexScan(stmt *SelectStmt, stats *Statistics) *IndexScanPlan {
	if stmt.Where == nil {
		return nil
	}

	conditions := o.extractIndexableConditions(stmt.Where)
	if len(conditions) == 0 {
		return nil
	}

	var bestIndex *IndexStats
	var bestSelectivity float64 = 1.0

	for _, indexStat := range stats.IndexStats {
		for _, cond := range conditions {
			if colExpr, ok := cond.(*BinaryExpr); ok {
				if col, ok := colExpr.Left.(*ColumnExpr); ok {
					for _, indexCol := range indexStat.Columns {
						if col.Column == indexCol {
							if indexStat.Selectivity < bestSelectivity {
								bestIndex = indexStat
								bestSelectivity = indexStat.Selectivity
							}
						}
					}
				}
			}
		}
	}

	if bestIndex == nil {
		return nil
	}

	estimatedRows := int(float64(stats.RowCount) * bestSelectivity)
	cost := float64(bestIndex.Height)*10 + float64(estimatedRows)*0.05

	return &IndexScanPlan{
		TableName:     stmt.TableName,
		IndexName:     bestIndex.IndexName,
		Conditions:    conditions,
		EstimatedRows: estimatedRows,
		EstimatedCost: cost,
	}
}

func (o *Optimizer) extractIndexableConditions(expr Expr) []Expr {
	conditions := make([]Expr, 0)

	switch e := expr.(type) {
	case *BinaryExpr:
		if e.Op == OpAnd {
			conditions = append(conditions, o.extractIndexableConditions(e.Left)...)
			conditions = append(conditions, o.extractIndexableConditions(e.Right)...)
		} else if e.Op == OpEqual || e.Op == OpLessThan || e.Op == OpGreaterThan ||
			e.Op == OpLessThanEqual || e.Op == OpGreaterThanEqual {
			if _, ok := e.Left.(*ColumnExpr); ok {
				if _, ok := e.Right.(*LiteralExpr); ok {
					conditions = append(conditions, e)
				}
			}
		}
	}

	return conditions
}

func (o *Optimizer) estimateSelectivity(expr Expr, stats *Statistics) float64 {
	switch e := expr.(type) {
	case *BinaryExpr:
		return o.estimateBinarySelectivity(e, stats)
	case *UnaryExpr:
		return o.estimateUnarySelectivity(e, stats)
	default:
		return 0.1
	}
}

func (o *Optimizer) estimateBinarySelectivity(expr *BinaryExpr, stats *Statistics) float64 {
	switch expr.Op {
	case OpAnd:
		leftSel := o.estimateSelectivity(expr.Left, stats)
		rightSel := o.estimateSelectivity(expr.Right, stats)
		return leftSel * rightSel

	case OpOr:
		leftSel := o.estimateSelectivity(expr.Left, stats)
		rightSel := o.estimateSelectivity(expr.Right, stats)
		return leftSel + rightSel - (leftSel * rightSel)

	case OpEqual:
		if col, ok := expr.Left.(*ColumnExpr); ok {
			if colStats, ok := stats.ColumnStats[col.Column]; ok {
				if colStats.DistinctValues > 0 {
					return 1.0 / float64(colStats.DistinctValues)
				}
			}
		}
		return 0.01

	case OpNotEqual:
		return 1.0 - o.estimateBinarySelectivity(&BinaryExpr{
			Left: expr.Left, Op: OpEqual, Right: expr.Right,
		}, stats)

	case OpLessThan, OpLessThanEqual, OpGreaterThan, OpGreaterThanEqual:
		return 0.33

	case OpLike:
		return 0.1

	case OpIn:
		return 0.05

	default:
		return 0.1
	}
}

func (o *Optimizer) estimateUnarySelectivity(expr *UnaryExpr, stats *Statistics) float64 {
	switch expr.Op {
	case OpNot:
		return 1.0 - o.estimateSelectivity(expr.Expr, stats)

	case OpIsNull:
		if col, ok := expr.Expr.(*ColumnExpr); ok {
			if colStats, ok := stats.ColumnStats[col.Column]; ok {
				if stats.RowCount > 0 {
					return float64(colStats.NullCount) / float64(stats.RowCount)
				}
			}
		}
		return 0.01

	case OpIsNotNull:
		return 1.0 - o.estimateUnarySelectivity(&UnaryExpr{Op: OpIsNull, Expr: expr.Expr}, stats)

	default:
		return 0.1
	}
}

func (o *Optimizer) optimizeJoin(leftPlan PlanNode, join JoinClause, stats *Statistics) PlanNode {
	rightStats, ok := o.stats[join.TableName]
	if !ok {
		rightStats = &Statistics{
			TableName: join.TableName,
			RowCount:  1000,
		}
	}

	rightPlan := &TableScanPlan{
		TableName:     join.TableName,
		EstimatedRows: rightStats.RowCount,
		EstimatedCost: float64(rightStats.RowCount) * 0.1,
	}

	nestedLoopCost := leftPlan.Cost() + float64(leftPlan.Cardinality())*rightPlan.Cost()
	hashJoinCost := leftPlan.Cost() + rightPlan.Cost() + float64(leftPlan.Cardinality()+rightPlan.Cardinality())*0.01

	if hashJoinCost < nestedLoopCost && o.isEquiJoin(join.On) {
		leftKeys, rightKeys := o.extractJoinKeys(join.On)
		return &HashJoinPlan{
			Left:          leftPlan,
			Right:         rightPlan,
			JoinType:      join.Type,
			LeftKeys:      leftKeys,
			RightKeys:     rightKeys,
			EstimatedCost: hashJoinCost,
		}
	}

	return &NestedLoopJoinPlan{
		Left:          leftPlan,
		Right:         rightPlan,
		JoinType:      join.Type,
		Condition:     join.On,
		EstimatedCost: nestedLoopCost,
	}
}

func (o *Optimizer) isEquiJoin(expr Expr) bool {
	if binExpr, ok := expr.(*BinaryExpr); ok {
		if binExpr.Op == OpEqual {
			_, leftIsCol := binExpr.Left.(*ColumnExpr)
			_, rightIsCol := binExpr.Right.(*ColumnExpr)
			return leftIsCol && rightIsCol
		}
	}
	return false
}

func (o *Optimizer) extractJoinKeys(expr Expr) ([]string, []string) {
	leftKeys := make([]string, 0)
	rightKeys := make([]string, 0)

	if binExpr, ok := expr.(*BinaryExpr); ok {
		if binExpr.Op == OpEqual {
			if leftCol, ok := binExpr.Left.(*ColumnExpr); ok {
				if rightCol, ok := binExpr.Right.(*ColumnExpr); ok {
					leftKeys = append(leftKeys, leftCol.Column)
					rightKeys = append(rightKeys, rightCol.Column)
				}
			}
		} else if binExpr.Op == OpAnd {
			lk1, rk1 := o.extractJoinKeys(binExpr.Left)
			lk2, rk2 := o.extractJoinKeys(binExpr.Right)
			leftKeys = append(leftKeys, lk1...)
			leftKeys = append(leftKeys, lk2...)
			rightKeys = append(rightKeys, rk1...)
			rightKeys = append(rightKeys, rk2...)
		}
	}

	return leftKeys, rightKeys
}

func (o *Optimizer) ExplainPlan(plan PlanNode) string {
	return o.explainPlanIndent(plan, 0)
}

func (o *Optimizer) explainPlanIndent(plan PlanNode, indent int) string {
	result := ""
	for i := 0; i < indent; i++ {
		result += "  "
	}
	result += plan.String() + "\n"

	for _, child := range plan.Children() {
		result += o.explainPlanIndent(child, indent+1)
	}

	return result
}
