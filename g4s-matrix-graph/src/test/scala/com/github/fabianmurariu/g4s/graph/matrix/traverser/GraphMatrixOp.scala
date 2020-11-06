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
}

case class Nodes(label: String) extends GraphMatrixOp

case class Edges(tpe: String) extends GraphMatrixOp

case class MatMul(left: GraphMatrixOp, right: GraphMatrixOp)
    extends GraphMatrixOp

case class Transpose(op: GraphMatrixOp) extends GraphMatrixOp
