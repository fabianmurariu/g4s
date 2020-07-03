package com.github.fabianmurariu.g4s.graph.query

import cats.free.Free
import cats.free.Free.liftF
import cats.{~>, Id}
import cats.data.State
import com.github.fabianmurariu.g4s.graph.core.{Graph, AdjacencyMap}

case class QueryGraph(
    graph: Id[AdjacencyMap[QueryNode, QueryEdge]],
    id: Int
)

case class QueryNode(id: Int, labels: Set[String])
case class QueryEdge(src: Int, dst: Option[Int], types: Set[String])

object QueryGraph {

  trait NodeDsl[F[_]] {
    def out(f: F[QueryNode])(types: String*): F[QueryEdge]
    def in(f: F[QueryNode])(types: String*): F[QueryEdge]

  }

  trait EdgeDsl[F[_]] {

    def vs(f: F[QueryEdge])(types: String*): F[QueryNode]

    def vs(f: F[QueryEdge])(v: QueryNode): F[QueryNode]

  }

  object Dsl {

    val G = Graph[AdjacencyMap, Id]

    type Query[T] = State[QueryGraph, T]

    def vs(labels: String*): Query[QueryNode] = State { qg =>
      val id = qg.id
      val qn = QueryNode(id, labels.toSet)
      qg.copy(
        graph = G.insertVertex(qg.graph)(qn),
        id = id + 1,
      ) -> qn
    }

    def edge[F[_]](
        src: QueryNode,
        dst: QueryNode,
        types: String*
    ): Query[QueryNode] = State { qg =>
      val qe = QueryEdge(src.id, Some(dst.id), types.toSet)
      qg.copy(
        graph = G.insertEdge(qg.graph)(src, dst, qe),
      ) -> dst
    }

    trait QueryNodeOps[F[_]] extends Any {
      def self: F[QueryNode]
      def out(types: String*): F[QueryEdge] = ???
      def in(types: String*): F[QueryEdge] = ???
    }

    trait QueryEdgeOps[F[_]] extends Any {
      def self: F[QueryEdge]
    def vs(f: F[QueryEdge])(types: String*): F[QueryNode] = ???

    def vs(f: F[QueryEdge])(v: QueryNode): F[QueryNode] = ???
    }

    implicit class QueryNodeDsl[F[_]](val self: F[QueryNode])
        extends AnyVal
        with QueryNodeOps[F]
    implicit class QueryEdgeDsl[F[_]](val self: F[QueryEdge])
        extends AnyVal
        with QueryEdgeOps[F]
  }

}
