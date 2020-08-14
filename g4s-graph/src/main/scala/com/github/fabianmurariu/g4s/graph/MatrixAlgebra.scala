package com.github.fabianmurariu.g4s.graph

sealed trait MatrixAlgebra

sealed trait Operand extends MatrixAlgebra

sealed trait Operation extends MatrixAlgebra
/**
 * Diagonal matrix with true on the row number corresponding to vertex id
 */
case object NodesMat extends Operand

case object EdgesMat extends Operand

case class NodeMat(label:String) extends Operand

case class EdgeMat(tpe:String) extends Operand

case class Transpose(ma:MatrixAlgebra) extends Operation

case class MatMul(left:MatrixAlgebra, right:MatrixAlgebra) extends Operation
case class MatIntersect(left:MatrixAlgebra, right:MatrixAlgebra) extends Operation
case class MatUnion(left:MatrixAlgebra, right:MatrixAlgebra) extends Operation
