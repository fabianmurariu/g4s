package com.github.fabianmurariu.g4s.optim.impls
import zio._
import com.github.fabianmurariu.g4s.optim.EvaluatorGraph
import com.github.fabianmurariu.g4s.optim.Name

trait PhysicalOp {

  /**
    * Run the operator
    *
    * @return
    */
  def eval: RIO[EvaluatorGraph[Task], Record]

  /**
    * Estimated output of the operator
    *
    * @return
    */
  def cardinality: RIO[EvaluatorGraph[Task], Long]

}

case class NodeMatrix(binding: Name, label: Option[String]) extends PhysicalOp {

  override def eval: RIO[EvaluatorGraph[Task], Record] = RIO.fromFunctionM {
    _.lookupNodes(label).map { case (mat, _) => Nodes(mat) }
  }

  override def cardinality: RIO[EvaluatorGraph[Task], Long] =
    RIO.fromFunctionM(_.lookupNodes(label).map { case (_, card) => card })

}
