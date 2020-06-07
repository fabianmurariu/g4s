package com.github.fabianmurariu.g4s.graph

import cats.free.Free
import cats.arrow.FunctionK
import cats.{Id, ~>}

/**
  *
  * Generic graph over effect F[_]
  * should be the underlying abstraction for any other graphs
  *
  * functions should in general return [[GraphTraverser[F]]]
  */
trait Graph[G[_, _]] {
// TODO: see GraphDB
}
