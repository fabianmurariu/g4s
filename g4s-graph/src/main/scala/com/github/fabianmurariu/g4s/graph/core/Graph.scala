package com.github.fabianmurariu.g4s.graph.core

import simulacrum.typeclass

/**
  * Graph
  */
@typeclass trait Graph[G[_, _]] {
  def orderG[V, E](g: G[V, E]): Int

  def sizeG[V, E](g: G[V, E]): Int

  def density[V, E](g: G[V, E]): Double = {
    val n = orderG(g)
    val e = sizeG(g)
    (2 * (e - n + 1)) / (n * (n - 3) + 2)
  }

  def vertices[V, E](g: G[V, E]): Traversable[V]

}

trait GraphK[F[_]] {
  def orderG[G[_, _]:Graph, V, E](f: F[G[V, E]]): F[Int]
}
