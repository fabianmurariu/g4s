package com.github.fabianmurariu.g4s.graph.core

import simulacrum.typeclass
import cats.kernel.Hash
import cats.kernel.Eq
import cats.implicits._
import scala.annotation.tailrec
import cats.Monad
import cats.Functor
import scala.collection.immutable.Queue
import cats.effect.Resource
import cats.Traverse
import cats.Foldable

/**
  * Graph allowing self pointing edges (x) -> (x)
  */
trait Graph[G[_, _], F[_]] { self =>

  def neighbours[V, E](fg: F[G[V, E]])(v: V): F[Iterable[(V, E)]]

  def vertices[V, E](g: F[G[V, E]]): F[Iterable[V]]

  def edges[V, E](fg: F[G[V, E]])(v: V): F[Iterable[E]]

  def edgesTriples[V, E](g: F[G[V, E]]): F[Iterable[(V, E, V)]]

  def containsV[V, E](g: F[G[V, E]])(v: V): F[Boolean]

  def getEdge[V, E](g: F[G[V, E]])(v1: V, v2: V): F[Option[E]]

  def insertVertex[V, E](g: F[G[V, E]])(v: V): F[G[V, E]]

  def insertEdge[V, E](g: F[G[V, E]])(src: V, dst: V, e: E): F[G[V, E]]

  def removeVertex[V, E](g: F[G[V, E]])(v: V): F[G[V, E]]

  def removeEdge[V, E](g: F[G[V, E]])(src: V, dst: V): F[G[V, E]]

  def orderG[V, E](g: F[G[V, E]]): F[Int]

  def sizeG[V, E](g: F[G[V, E]]): F[Long]

  def degree[V, E](g: F[G[V, E]])(v: V): F[Option[Long]]

  def empty[V, E]: Resource[F, G[V, E]]

  def load[V, E, C[_]: Foldable](ts: C[(V, E, V)])(
      implicit F: Monad[F]
  ): Resource[F, G[V, E]] =
    Graph.fromTriplets[V, E, C, G, F](ts)(Foldable[C], F, self)

  /* ************************************************** */

  def insertVertices[V, E, C[_]: Foldable](
      fg: F[G[V, E]]
  )(vs: C[V])(implicit M: Monad[F]) = fg.flatMap { g =>
    vs.foldM[F, G[V, E]](g) { (fg, v) => self.insertVertex(M.pure(g))(v) }
  }

  def adjacentEdges[V, E](g: F[G[V, E]])(
      v: V
  )(implicit F: Monad[F]): F[Iterable[E]] =
    self.neighbours(g)(v).map(_.map(_._2))

  def density[V, E](g: F[G[V, E]])(implicit F: Monad[F]): F[Double] =
    for {
      n <- self.orderG(g)
      e <- self.sizeG(g)
    } yield (2 * (e - n + 1)) / (n * (n - 3) + 2)

  def connectedComponents[V, E](
      fg: F[G[V, E]]
  )(implicit M: Monad[F]): F[List[Map[V, Option[V]]]] = {

    def step(
        ccsF: F[List[Map[V, Option[V]]]],
        v: V
    ): F[List[Map[V, Option[V]]]] = {
      ccsF.flatMap { ccs =>
        if (ccs.nonEmpty && ccs.map(cc => cc.contains(v)).reduceLeft(_ || _))
          M.pure(ccs)
        else {
          bfs(fg)(v).map(cc => cc :: ccs)
        }
      }
    }

    val vs = self.vertices(fg).map(_.toVector)
    self.vertices(fg).flatMap { vs =>
      vs.foldLeft(M.pure(List.empty[Map[V, Option[V]]])) { (ccsF, v) =>
        val out = step(ccsF, v)
        out
      }
    }
  }

  import TraverseOrder.ops._

  def dfs[V, E](
      g: F[G[V, E]]
  )(v: V)(implicit F: Monad[F]): F[Map[V, Option[V]]] =
    traverseGraph[V, E, List](g)(v)

  def bfs[V, E](
      g: F[G[V, E]]
  )(v: V)(implicit F: Monad[F]): F[Map[V, Option[V]]] =
    traverseGraph[V, E, Queue](g)(v)

  def traverseGraph[V, E, C[_]](
      g: F[G[V, E]]
  )(v: V)(implicit F: Monad[F], TO: TraverseOrder[C]): F[Map[V, Option[V]]] = {

    val traverseLoop =
      F.iterateUntilM(TraverseOrder[C].pure(v), Map(v -> Option.empty[V])) {
        case (order, history) if order.isEmpty =>
          F.pure((TO.empty, history)) // to ward off the warning but won't hit
        case (order, history) =>
          val (parent, tail) = order.pop
          self
            .neighbours(g)(parent)
            .map {
              _.foldLeft(tail -> history) {
                case ((t, h), (c, _))
                    if !history.contains(c) => // node not seen
                  (t.push(c), h + (c -> Some(parent)))
                case (orElse, _) => orElse // when node is already in instory
              }
            }
      } { case (stack, _) => stack.isEmpty }

    traverseLoop.map(_._2)
  }

}

object Graph {

  implicit def adjacencyMapIsAGraph[F[_]: Monad] =
    new ImmutableAdjacencyMapUndirectedGraphInstance[F]

  def apply[G[_, _], F[_]](implicit G: Graph[G, F]): Graph[G, F] = G

  trait GraphOps[G[_, _], F[_], V, E] extends Any {

    def self: F[G[V, E]]

    def G: Graph[G, F]

    def neighbours(v: V)(implicit F: Monad[F]): F[Iterable[(V, E)]] =
      G.neighbours(self)(v)

    def vertices: F[Iterable[V]] =
      G.vertices(self)

    def edges(implicit F: Functor[F]): F[Iterable[E]] =
      G.edgesTriples(self).map(_.map(_._2))

    def insertEdge(src: V, dst: V, e: E) =
      G.insertEdge(self)(src, dst, e)

    def insertVertex(v: V) =
      G.insertVertex(self)(v)

    def containsV(v: V) =
      G.containsV(self)(v)

    def getEdge(src: V, dst:V) =
      G.getEdge(self)(src, dst)

    def connectedComponents(implicit M: Monad[F]) =
      G.connectedComponents(self)
  }

  implicit def decorateGraphOps[G[_, _], F[_], V, E](
      fg: F[G[V, E]]
  )(implicit GG: Graph[G, F]) = new GraphOps[G, F, V, E] {

    override def self: F[G[V, E]] = fg

    override def G: Graph[G, F] = GG

  }

  def fromTriplets[V, E, C[_]:Foldable, G[_, _], F[_]: Monad](
      ts: C[(V, E, V)]
  )(implicit G: Graph[G, F]): Resource[F, G[V, E]] = {
    G.empty[V, E].evalMap { graph =>
      ts.foldM[F, G[V, E]](graph) {
          case (fg, (src, e, dst)) =>
            Monad[F]
              .pure(fg)
              .insertVertex(src)
              .insertVertex(dst)
              .insertEdge(src, dst, e)
        }
    }
  }
}

@typeclass trait TraverseOrder[F[_]] {
  def push[A](f: F[A])(a: A): F[A]
  def pop[A](f: F[A]): (A, F[A])
  def isEmpty[A](f: F[A]): Boolean

  def pure[A](a: A): F[A]
  def empty[A]: F[A]
}

object TraverseOrder {
  implicit val dfs: TraverseOrder[List] =
    new TraverseOrder[List] {

      def isEmpty[A](f: List[A]): Boolean = f.isEmpty

      def push[A](f: List[A])(a: A): List[A] = a :: f

      def pop[A](f: List[A]): (A, List[A]) = f.head -> f.tail

      def pure[A](a: A): List[A] = List(a)

      def empty[A] = List.empty[A]
    }

  implicit val bfs: TraverseOrder[Queue] =
    new TraverseOrder[Queue] {

      def isEmpty[A](f: Queue[A]): Boolean = f.isEmpty

      def push[A](f: Queue[A])(a: A): Queue[A] = f.enqueue(a)

      def pop[A](f: Queue[A]): (A, Queue[A]) = f.head -> f.tail

      def pure[A](a: A): Queue[A] = Queue(a)

      def empty[A] = Queue.empty[A]
    }
}
