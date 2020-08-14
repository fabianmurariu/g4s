package com.github.fabianmurariu.g4s.graph.core

import simulacrum.typeclass
import cats.Monad
import cats.implicits._

trait DGraph[G[_, _], F[_]] extends Graph[G, F] {

  def outNeighbours[V, E](g: F[G[V, E]])(v: V): F[Iterable[(V, E)]]

  def inNeighbours[V, E](g: F[G[V, E]])(v: V): F[Iterable[(V, E)]]

  def outEdges[V, E](fg:F[G[V, E]])(v: V):F[Iterable[E]]

  def inEdges[V, E](fg:F[G[V, E]])(v: V):F[Iterable[E]]

  def outDegree[V, E](fg: F[G[V, E]])(v: V): F[Int]

  def inDegree[V, E](fg: F[G[V, E]])(v: V): F[Int]

}

object DGraph {
  implicit def adjacencyMapIsAGraph[F[_]: Monad] =
    new ImmutableAdjacencyMapDirectedGraphInstance[F]

  def apply[G[_, _], F[_]](implicit G:DGraph[G, F]) = G

}
