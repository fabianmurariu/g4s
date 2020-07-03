package com.github.fabianmurariu.g4s.graph.core

import simulacrum.typeclass
import cats.kernel.Hash
import cats.kernel.Eq
import cats.implicits._
import scala.annotation.tailrec
import cats.Monad
import cats.Functor

/**
  * Undirected Graph allowing self pointing edges (x) -> (x)
  */
trait Graph[G[_, _], F[_]] { self =>

  def neighbours[V, E](fg: F[G[V, E]])(v: V): F[Traversable[(V, E)]]

  def vertices[V, E](g: F[G[V, E]]): F[Traversable[V]]

  def edgesTriples[V, E](g: F[G[V, E]]): F[Traversable[(V, E, V)]]

  def containsV[V, E](g: F[G[V, E]])(v: V): F[Boolean]

  def getEdge[V, E](g: F[G[V, E]])(v1: V, v2: V): F[Option[E]]

  def insertVertex[V, E](g: F[G[V, E]])(v: V): F[G[V, E]]

  def insertEdge[V, E](g: F[G[V, E]])(src: V, dst: V, e: E): F[G[V, E]]

  def removeVertex[V, E](g: F[G[V, E]])(v: V): F[G[V, E]]

  def removeEdge[V, E](g: F[G[V, E]])(src: V, dst: V): F[G[V, E]]

  def orderG[V, E](g: F[G[V, E]]): F[Int]

  def sizeG[V, E](g: F[G[V, E]]): F[Int]

  def degree[V, E](g: F[G[V, E]])(v: V): F[Int]

  /* ************************************************** */

  def adjacentEdges[V, E](g: F[G[V, E]])(
      v: V
  )(implicit F: Functor[F]): F[Traversable[E]] =
    self.neighbours(g)(v).map(_.map(_._2))

  def density[V, E](g: F[G[V, E]])(implicit F: Monad[F]): F[Double] =
    for {
      n <- self.orderG(g)
      e <- self.sizeG(g)
    } yield (2 * (e - n + 1)) / (n * (n - 3) + 2)

  def dfs[V, E](
      g: F[G[V, E]]
  )(v: V)(implicit F: Monad[F]): F[Map[V, Option[V]]] = {

    val dfsLoop = F.iterateUntilM(List(v), Map(v -> Option.empty[V])) {
      case (Nil, history) =>
        F.pure((Nil, history)) // to ward off the warning but won't hit
      case (parent :: tail, history) =>
        self
          .neighbours(g)(parent)
          .map {
            _.foldLeft(tail -> history) {
              case ((t, h), (c, _)) if !history.contains(c) => // node not seen
                (c :: t, h + (c -> Some(parent)))
              case (orElse, _) => orElse // when node is already in instory
            }
          }
    } { case (stack, _) => stack.isEmpty }

    dfsLoop.map(_._2)
  }

  def empty[V, E]: F[G[V, E]]
}

object Graph {

  implicit def adjacencyMapIsAGraph[F[_]: Monad] =
    new ImmutableAdjacencyMapGraphInstance[F]

  def apply[G[_, _], F[_]](implicit G: Graph[G, F]): Graph[G, F] = G

  trait GraphOps[G[_, _], F[_], V, E] extends Any {

    def self: F[G[V, E]]

    def G: Graph[G, F]

    def neighbours(v: V): F[Traversable[(V, E)]] =
      G.neighbours(self)(v)

  }

  implicit def decorateGraphOps[G[_, _], F[_], V, E](
      fg: F[G[V, E]]
  )(implicit GG: Graph[G, F]) = new GraphOps[G, F, V, E] {

    override def self: F[G[V, E]] = fg

    override def G: Graph[G, F] = GG

  }
  // implicit class GraphOpsF[G[_, _], F[_], V, E](val fg: F[G[V, E]]) extends AnyVal with GraphOps[G, F] {

  // }
}
