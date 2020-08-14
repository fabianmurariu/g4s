package com.github.fabianmurariu.g4s.graph.query

import cats.free.Free
import cats.free.Free.liftF
import cats.{~>, Id}
import cats.data.State
import com.github.fabianmurariu.g4s.graph.core.{Graph, AdjacencyMap, DGraph}
import simulacrum.typeclass
import com.github.fabianmurariu.g4s.graph.{
  MatrixAlgebra,
  MatIntersect,
  NodesMat,
  NodeMat
}
import com.github.fabianmurariu.g4s.graph.MatUnion

case class QueryGraph(
    graph: Id[AdjacencyMap[QueryNode, QueryEdge]],
    id: Int
)

sealed trait AddOp
case object AND extends AddOp
case object OR extends AddOp

case class QueryNode(id: Int, labels: Set[String] = Set.empty, add: AddOp = AND)
case class QueryEdge(
    src: Int,
    dst: Option[Int],
    types: Set[String],
    add: AddOp = OR
)

object QueryGraph {

  def empty = QueryGraph(Map.empty, 0)

  val G = DGraph[AdjacencyMap, Id]

  object Dsl {

    type Query[T] = State[QueryGraph, T]

    def vs(labels: String*): Query[QueryNode] = State { qg =>
      val id = qg.id
      val qn = QueryNode(id, labels.toSet)
      qg.copy(
        graph = G.insertVertex(qg.graph)(qn),
        id = id + 1
      ) -> qn
    }

    def edge(
        src: QueryNode,
        dst: QueryNode,
        types: String*
    ): Query[QueryNode] = State { qg =>
      val qe = QueryEdge(src.id, Some(dst.id), types.toSet)
      qg.copy(
        graph = G.insertEdge(qg.graph)(src, dst, qe)
      ) -> dst
    }

  }

  /**
    * Naive mapping from query to matrix algebra
    * good place to start
    */
  def evalQueryGraph(qg: QueryGraph): Seq[MatrixAlgebra] = {
    val edges = G.edgesTriples(qg.graph)
    val (src, e, dst) = edges.head

    Seq.empty
  }

  def queryNodeToMatrixAlgebra(qn: QueryNode): MatrixAlgebra = qn match {
    case QueryNode(_, labels, _) if labels.isEmpty   => NodesMat
    case QueryNode(_, labels, _) if labels.size == 1 => NodeMat(labels.head)
    case QueryNode(_, labels, AND) =>
      labels
        .map(NodeMat(_))
        .map(_.asInstanceOf[MatrixAlgebra])
        .reduce((a, b) => MatIntersect(a, b))
    case QueryNode(_, labels, OR) =>
      labels
        .map(NodeMat(_))
        .map(_.asInstanceOf[MatrixAlgebra])
        .reduce((a, b) => MatUnion(a, b))
  }

}
