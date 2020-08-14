package com.github.fabianmurariu.g4s.graph.core

import cats.Monad
import cats.implicits._
import cats.Traverse
import cats.Foldable
import cats.effect.Resource

class ImmutableAdjacencyMapDirectedGraphInstance[F[_]: Monad]
    extends DGraph[AdjacencyMap, F] {

  def outEdges[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Iterable[E]] =
    fg.map { g => g.getOrElse(v, Map.empty).map { _._2 } }

  def inEdges[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Iterable[E]] = ???

  def outDegree[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Int] = ???

  def inDegree[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Int] = ???

  override def outNeighbours[V, E](
      fg: F[AdjacencyMap[V, E]]
  )(v: V): F[Iterable[(V, E)]] =
    fg.map(g => g.getOrElse(v, Map.empty))

  override def inNeighbours[V, E](
      fg: F[AdjacencyMap[V, E]]
  )(v: V): F[Iterable[(V, E)]] = fg.map { g =>
    g.getOrElse(v, Map.empty)
      .flatMap { case (n, _) => g.getOrElse(n, Map.empty).filterKeys(_ == v) }
  }

  override def neighbours[V, E](g: F[AdjacencyMap[V, E]])(
      v: V
  ): F[Iterable[(V, E)]] =
    for {
      out <- outNeighbours(g)(v)
      in <- inNeighbours(g)(v)
    } yield out ++ in

  override def edges[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Iterable[E]] =
    for {
      out <- outEdges(fg)(v)
      in <- inEdges(fg)(v)
    } yield out ++ in

  override def vertices[V, E](g: F[AdjacencyMap[V, E]]): F[Iterable[V]] =
    g.map(_.keySet)

  override def edgesTriples[V, E](
      fg: F[AdjacencyMap[V, E]]
  ): F[Iterable[(V, E, V)]] =
    fg.map(_.flatMap {
      case (v, edges) =>
        (edges.map { case (n, e) => (v, e, n) })
    })

  override def containsV[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Boolean] =
    fg.map(_.contains(v))

  override def getEdge[V, E](
      fg: F[AdjacencyMap[V, E]]
  )(v1: V, v2: V): F[Option[E]] = fg.map(_.getOrElse(v1, Map.empty).get(v2))

  override def orderG[V, E](fg: F[AdjacencyMap[V, E]]): F[Int] = fg.map(_.size)

  override def sizeG[V, E](fg: F[AdjacencyMap[V, E]]): F[Long] = fg.flatMap {
    g =>
      g.keys.toStream
        .foldLeftM(0L) { (sum, v) =>
          degree(fg)(v).map(dg => dg.getOrElse(0L) + sum)
        }
        .map(_ / 2)
  }

  override def degree[V, E](fg: F[AdjacencyMap[V, E]])(v: V): F[Option[Long]] =
    fg.map { g =>
      g.get(v).map { edges =>
        if (edges.contains(v))
          edges.size + 1
        else
          edges.size
      }
    }

  override def insertVertex[V, E](
      fg: F[AdjacencyMap[V, E]]
  )(v: V): F[AdjacencyMap[V, E]] = fg.map { g =>
    g.get(v) match {
      case None =>
        g + (v -> Map.empty[V, E])
      case _ => g
    }
  }

  override def insertEdge[V, E](
      fg: F[AdjacencyMap[V, E]]
  )(src: V, dst: V, e: E): F[AdjacencyMap[V, E]] = fg.map { g =>
    val op =
      for {
        srcEdges <- g.get(src)
      } yield g +
        (src -> (srcEdges + (dst -> e)))

    op.getOrElse(g)
  }

  override def removeVertex[V, E](fg: F[AdjacencyMap[V, E]])(
      v: V
  ): F[AdjacencyMap[V, E]] =
    for {
      g <- fg
      ns <- neighbours(fg)(v)
      x <- Monad[F].iterateUntilM(ns.toIterator -> g) {
        case (iter, g) =>
          val (n, e) = iter.next
          val neighbourEdges = g.getOrElse(n, Map.empty)
          val newG = g + (n -> (neighbourEdges - v))
          Monad[F].pure((iter, newG))
      } { case (i, _) => i.hasNext }
    } yield x._2

  override def removeEdge[V, E](
      fg: F[AdjacencyMap[V, E]]
  )(src: V, dst: V): F[AdjacencyMap[V, E]] = fg.map { g =>
    val op =
      for {
        srcEdges <- g.get(src)
      } yield g +
        (src -> (srcEdges - dst))

    op.getOrElse(g)
  }

  override def empty[V, E]: Resource[F, AdjacencyMap[V, E]] =
    Resource.pure[F, AdjacencyMap[V, E]](Map.empty)

}
