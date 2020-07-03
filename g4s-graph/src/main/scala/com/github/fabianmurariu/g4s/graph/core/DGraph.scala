package com.github.fabianmurariu.g4s.graph.core

import simulacrum.typeclass

trait DGraph[G[_, _], F[_]] extends Graph[G, F] {

  def outNeighbours[V, E](g: F[G[V, E]])(v: V): F[Traversable[(V, E)]]

  def inNeighbours[V, E](g: F[G[V, E]])(v: V): F[Traversable[(V, E)]]
}
