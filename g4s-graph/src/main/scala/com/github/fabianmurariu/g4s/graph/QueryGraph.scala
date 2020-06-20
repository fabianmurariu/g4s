package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.{Id, ~>}
import simulacrum.typeclass
import cats.data.State

case class QG(nodes: Vector[QueryNode], edges: Vector[QueryEdge])
case class QueryNode(id: Int, labels: Set[String])
case class QueryEdge(src: Int, dst: Int, types: Set[String])

sealed trait QStep[T]

@typeclass trait QueryGraph[F[_]] {

  def out[Node: QNode, Edge: QEdge](f: F[Node])(tpes: Set[String]): F[Edge]
  def in[Node: QNode, Edge: QEdge](f: F[Node])(tpes: Set[String]): F[Edge]
  def vs[Node: QNode, Edge: QEdge](f: F[Edge])(labels: Set[String]): F[Node]

}

@typeclass trait QNode[A] {
  def id(a: A): Int
  def labels(a: A): Set[String]

  // constructor
  def pure(id: Int, labels: Set[String]): A
}

object QNode {

  implicit val qNode: QNode[QueryNode] = new QNode[QueryNode] {

    override def id(a: QueryNode): Int = a.id

    override def labels(a: QueryNode): Set[String] = a.labels

    def pure(id: Int, labels: Set[String]): QueryNode = QueryNode(id, labels)
  }

}

@typeclass trait QEdge[A] {
  def src(a: A): Int
  def dst(a: A): Int
  def types(a: A): Set[String]

  def pure(src: Int, dst: Int, tpes: Set[String]): A
}

object QEdge {

  implicit val qEdge: QEdge[QueryEdge] = new QEdge[QueryEdge] {

    override def src(a: QueryEdge): Int = a.src

    override def dst(a: QueryEdge): Int = a.dst

    override def types(a: QueryEdge): Set[String] = a.types

    def pure(src: Int, dst: Int, tpes: Set[String]): QueryEdge =
      QueryEdge(src, dst, tpes)

  }

}

object QueryGraph {

  type QueryGraphF[A] = State[QG, A]

  implicit def queryGraph: QueryGraph[QueryGraphF] =
    new QueryGraph[QueryGraphF] {

      override def out[Node: QNode, Edge: QEdge](f: QueryGraphF[Node])(
          tpes: Set[String]
      ): QueryGraphF[Edge] = ???

      override def in[Node: QNode, Edge: QEdge](f: QueryGraphF[Node])(
          tpes: Set[String]
      ): QueryGraphF[Edge] = ???

      override def vs[Node: QNode, Edge: QEdge](f: QueryGraphF[Edge])(
          labels: Set[String]
      ): QueryGraphF[Node] = ???

    }

  /**
    * starting point an the only way to create a query graph
    */
  def nodes(labels: String*) = State[QG, QueryNode] { qg =>
    val id = qg.nodes.length
    val node = QueryNode(id, labels.toSet)
    (qg.copy(qg.nodes :+ node), node)
  }

}
