package com.github.fabianmurariu.g4s.graph

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix

/**
  * An matrix and a graph
  * that can produce an iterable
  * of output Vertices
  * @tparam F
  * effect
  * @tparam V
  * vertex
  * @tparam E
  * edge
  */
class NodeResult[F[_], V, E](
    private val g: ConcurrentDirectedGraph[F, V, E],
    nodeMat: BlockingMatrix[F, Boolean]
) extends Iterable[V] {
  override def iterator: Iterator[V] = new NodeResultIterator(g, nodeMat)
}

private[graph] class NodeResultIterator[F[_], V, E](
    g: ConcurrentDirectedGraph[F, V, E],
    nodeMat: BlockingMatrix[F, Boolean]
) extends Iterator[V] {
  override def hasNext: Boolean = ???

  override def next(): V = ???
}
