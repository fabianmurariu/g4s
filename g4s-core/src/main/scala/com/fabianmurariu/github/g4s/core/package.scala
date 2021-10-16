package com.fabianmurariu.github.g4s

package object core {

  type Id[A] = A

  type LargeBaseGraph[G[_], IO[_], Rep[_]] = BaseGraph0[G, IO, Rep, Long]
  type BaseGraph[G[_], IO[_], Rep[_]] = BaseGraph0[G, IO, Rep, Int]

  type LargeGraph[G[_]] = BaseGraph0[G, Id, Id, Long]
  type Graph[G[_]] = BaseGraph0[G, Id, Id, Int]
}
