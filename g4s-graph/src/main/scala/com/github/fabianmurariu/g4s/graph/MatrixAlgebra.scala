package com.github.fabianmurariu.g4s.graph

sealed trait MatrixAlgebra

/**
 * Diagonal matrix with true on the row number corresponding to vertex id
 */
case class NodeSelection(label:String) extends MatrixAlgebra

/**
 * Edge Matrix
 */
case class EdgeSelection(tpe:String, transpose:Boolean) extends MatrixAlgebra

/**
 * E = A + B (element wise add on OP)
 */
case class ElemWiseAlg(mats:Seq[MatrixAlgebra], op:MatrixOp) extends MatrixAlgebra

/**
 * E = A * B (where * is a semiring, most likely ANY_PAIR)
 */
case class MxMAlg(left: MatrixAlgebra, right:MatrixAlgebra, op:MatrixOp) extends MatrixAlgebra

sealed trait MatrixOp

case object And extends MatrixOp

case object Or extends MatrixOp


object MatrixAlgebra {

}
