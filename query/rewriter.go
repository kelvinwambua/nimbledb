package query

type Rewriter struct{}

func NewRewriter() *Rewriter {
	return &Rewriter{}
}

func (r *Rewriter) Rewrite(expr Expr) Expr {
	expr = r.pushDownNegation(expr)
	expr = r.eliminateDoubleNegation(expr)
	expr = r.simplifyConstants(expr)
	expr = r.convertToConjunctiveNormalForm(expr)
	return expr
}

func (r *Rewriter) pushDownNegation(expr Expr) Expr {
	switch e := expr.(type) {
	case *UnaryExpr:
		if e.Op == OpNot {
			switch inner := e.Expr.(type) {
			case *BinaryExpr:
				if inner.Op == OpAnd {
					return &BinaryExpr{
						Left:  r.pushDownNegation(&UnaryExpr{Op: OpNot, Expr: inner.Left}),
						Op:    OpOr,
						Right: r.pushDownNegation(&UnaryExpr{Op: OpNot, Expr: inner.Right}),
					}
				} else if inner.Op == OpOr {
					return &BinaryExpr{
						Left:  r.pushDownNegation(&UnaryExpr{Op: OpNot, Expr: inner.Left}),
						Op:    OpAnd,
						Right: r.pushDownNegation(&UnaryExpr{Op: OpNot, Expr: inner.Right}),
					}
				} else if inner.Op == OpEqual {
					return &BinaryExpr{
						Left:  inner.Left,
						Op:    OpNotEqual,
						Right: inner.Right,
					}
				} else if inner.Op == OpLessThan {
					return &BinaryExpr{
						Left:  inner.Left,
						Op:    OpGreaterThanEqual,
						Right: inner.Right,
					}
				} else if inner.Op == OpGreaterThan {
					return &BinaryExpr{
						Left:  inner.Left,
						Op:    OpLessThanEqual,
						Right: inner.Right,
					}
				}
			case *UnaryExpr:
				if inner.Op == OpNot {
					return r.pushDownNegation(inner.Expr)
				}
			}
		}
	case *BinaryExpr:
		return &BinaryExpr{
			Left:  r.pushDownNegation(e.Left),
			Op:    e.Op,
			Right: r.pushDownNegation(e.Right),
		}
	}
	return expr
}

func (r *Rewriter) eliminateDoubleNegation(expr Expr) Expr {
	switch e := expr.(type) {
	case *UnaryExpr:
		if e.Op == OpNot {
			if inner, ok := e.Expr.(*UnaryExpr); ok && inner.Op == OpNot {
				return r.eliminateDoubleNegation(inner.Expr)
			}
			return &UnaryExpr{
				Op:   e.Op,
				Expr: r.eliminateDoubleNegation(e.Expr),
			}
		}
	case *BinaryExpr:
		return &BinaryExpr{
			Left:  r.eliminateDoubleNegation(e.Left),
			Op:    e.Op,
			Right: r.eliminateDoubleNegation(e.Right),
		}
	}
	return expr
}

func (r *Rewriter) simplifyConstants(expr Expr) Expr {
	switch e := expr.(type) {
	case *BinaryExpr:
		left := r.simplifyConstants(e.Left)
		right := r.simplifyConstants(e.Right)

		return &BinaryExpr{
			Left:  left,
			Op:    e.Op,
			Right: right,
		}
	case *UnaryExpr:
		return &UnaryExpr{
			Op:   e.Op,
			Expr: r.simplifyConstants(e.Expr),
		}
	}
	return expr
}

func (r *Rewriter) convertToConjunctiveNormalForm(expr Expr) Expr {
	switch e := expr.(type) {
	case *BinaryExpr:
		if e.Op == OpOr {
			left := r.convertToConjunctiveNormalForm(e.Left)
			right := r.convertToConjunctiveNormalForm(e.Right)

			if leftAnd, ok := left.(*BinaryExpr); ok && leftAnd.Op == OpAnd {
				return &BinaryExpr{
					Left: r.convertToConjunctiveNormalForm(&BinaryExpr{
						Left:  leftAnd.Left,
						Op:    OpOr,
						Right: right,
					}),
					Op: OpAnd,
					Right: r.convertToConjunctiveNormalForm(&BinaryExpr{
						Left:  leftAnd.Right,
						Op:    OpOr,
						Right: right,
					}),
				}
			}

			if rightAnd, ok := right.(*BinaryExpr); ok && rightAnd.Op == OpAnd {
				return &BinaryExpr{
					Left: r.convertToConjunctiveNormalForm(&BinaryExpr{
						Left:  left,
						Op:    OpOr,
						Right: rightAnd.Left,
					}),
					Op: OpAnd,
					Right: r.convertToConjunctiveNormalForm(&BinaryExpr{
						Left:  left,
						Op:    OpOr,
						Right: rightAnd.Right,
					}),
				}
			}

			return &BinaryExpr{Left: left, Op: OpOr, Right: right}
		} else if e.Op == OpAnd {
			return &BinaryExpr{
				Left:  r.convertToConjunctiveNormalForm(e.Left),
				Op:    OpAnd,
				Right: r.convertToConjunctiveNormalForm(e.Right),
			}
		}
		return &BinaryExpr{
			Left:  r.convertToConjunctiveNormalForm(e.Left),
			Op:    e.Op,
			Right: r.convertToConjunctiveNormalForm(e.Right),
		}
	case *UnaryExpr:
		return &UnaryExpr{
			Op:   e.Op,
			Expr: r.convertToConjunctiveNormalForm(e.Expr),
		}
	}
	return expr
}
