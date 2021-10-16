package com.fabianmurariu.github.g4s.core

trait BaseGraph0[G[_], IO[_], Rep[_], NodeId] {

  type ResSet[A] = ResultSet[IO, Iterable[A]]

  def neighbours[E: Rep](g: G[E])(
      node: ResSet[NodeId]
  ): ResSet[NodeId]

  def relation[E: Rep](src: NodeId, e: E, dst: NodeId): IO[G[E]]

  def nodes: ResultSet[IO, NodeId]

  def edges[E: Rep]: ResultSet[IO, E]
}

trait NodeStore[F[_], IO[_], Rep[_], NodeId] {

  def insertNode[V: Rep](f: F[V])(v: V): IO[(NodeId, F[V])]

  def node[V: Rep](f: F[V])(id: NodeId): IO[Option[V]]

  def nodeId[V: Rep](f: F[V])(node: V): IO[NodeId]

}
