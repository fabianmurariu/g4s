package com.github.fabianmurariu.g4s.graph.matrix.traverser

trait GraphMatrixOp { self =>
  def toStrExpr(paren: Boolean = false): String = self match {
    case Nodes(label) => label.split("\\.").last
    case Edges(label) => label.split("\\.").last
    case MatMul(left, right) =>
      if (paren)
        s"(${left.toStrExpr(paren)}*${right.toStrExpr(paren)})"
      else
        s"${left.toStrExpr(paren)}*${right.toStrExpr(paren)}"

    case Transpose(op) => s"T(${op.toStrExpr(paren)})"
  }

  def algStr: String = self.toStrExpr(false)

  def cost:Int = self match {
    case _:Nodes => 1
    case _:Edges => 1
    case MatMul(left, right) => left.cost + right.cost
    case Transpose(op) => 1 + op.cost
  }

  // we can union 2 trees if they both point to the same output node
  // on the right hand side, go down this graph down to the right most node
  // and replace it with other
  def union(other:GraphMatrixOp):GraphMatrixOp = self match {
    case _:Nodes => other
    case e:Edges => e
    case MatMul(l, r) => MatMul(
      l, r.union(other))
    case Transpose(op) => Transpose(op.union(other))
  }
}

case class Nodes(label: String) extends GraphMatrixOp

case class Edges(tpe: String) extends GraphMatrixOp

case class MatMul(left: GraphMatrixOp, right: GraphMatrixOp)
    extends GraphMatrixOp

case class Transpose(op: GraphMatrixOp) extends GraphMatrixOp

case class MatRef(name:String) extends GraphMatrixOp
