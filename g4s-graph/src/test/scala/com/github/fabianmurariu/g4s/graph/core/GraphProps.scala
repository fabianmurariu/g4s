package com.github.fabianmurariu.g4s.graph.core

import cats.{Monad, Comonad, Foldable, Id}
import Graph.decorateGraphOps
import org.scalacheck.Prop
import cats.effect.Sync
import cats.Applicative
import cats.effect.Bracket

abstract class GraphProps[G[_, _], F[_]](
    implicit GG: Graph[G, F],
    C: Comonad[F],
    M: Monad[F]
) {

  import Monad.ops._
  import cats.instances.vector._
  import cats.instances.list._
  def G = GG

  def load[V, E, C[_]: Foldable](fg: F[G[V, E]])(ccs: C[(V, E, V)]) =
    Foldable[C]
      .foldLeftM[F, (V, E, V), F[G[V, E]]](ccs, fg) {
        case (fg, (src, e, dst)) =>
          M.pure(
            fg.insertVertex(src).insertVertex(dst).insertEdge(src, dst, e)
          )
      }
      .flatten


  def canFind2ConnectedComponentsInAGraph[V, E](fg: F[G[V, E]])(
      cc1: List[(V, E, V)],
      cc2: List[(V, E, V)]
  ): Boolean = {

    val ccsF = load(load(fg)(cc1))(cc2).connectedComponents
    val prop = ccsF.map { ccs => ccs.length == 2 }

    C.extract(prop)
  }

  def returnAVertexInserted[V, E](
      fg: F[G[V, E]]
  )(v: V): Prop = {
      C.extract(fg.insertVertex(v).containsV(v))
  }

  def returnEdgeBetweenVertices[V, E](fg: F[G[V, E]])(x: (V, E, V)): Prop = {
    val (src, e, dst) = x
    C.extract(fg.insertVertex(src).insertVertex(dst).insertEdge(src, dst, e).getEdge(src, dst)) == Some(e)
  }

  def undirectedLink2VerticesAreEachotherNeighbour[V, E](
      fg: F[G[V, E]]
  )(
      v1: V,
      v2: V,
      e: E
  ): Prop = {
    val g1: F[G[V, E]] = G.insertVertex(fg)(v1)
    val g2: F[G[V, E]] = G.insertVertex(g1)(v2)
    val g3: F[G[V, E]] = G.insertEdge(g2)(v1, v2, e)

    val out = for {
      neighboursV1 <- G.neighbours(g3)(v1)
      neighboursV2 <- G.neighbours(g3)(v2)
    } yield {
      neighboursV1.toVector == Vector(v2 -> e) &&
      neighboursV2.toVector == Vector(v1 -> e)
    }

    C.extract(out)
  }

  def canRepresentALineGraph[V, E](
      fg: F[G[V, E]]
  )(vs: Vector[V], e: E): Prop = {
    import cats.instances.vector._
    val loadedG = for {
      g <- fg
      x <- M.iterateWhileM((vs.iterator, Option.empty[V], g)) {
        case (iter, None, g) =>
          val v = iter.next
          G.insertVertex(M.pure(g))(v).map(g => (iter, Some(v), g))
        case (iter, Some(prev), g) =>
          val v = iter.next
          val g0 = G.insertVertex(M.pure(g))(v)
          G.insertEdge(g0)(prev, v, e).map(g => (iter, Some(v), g))
      } { case (iter, _, _) => iter.hasNext }
    } yield x._3

    vs.zipWithIndex.forall {
      case (v, id) if id > 0 && id < vs.length - 1 =>
        val a = for {
          isV <- G.containsV(loadedG)(v)
          neighbours <- G.neighbours(loadedG)(v)
        } yield isV && neighbours.toVector == Vector(
          vs(id - 1) -> e,
          vs(id + 1) -> e
        )
        C.extract(a)
      case (v, 0) =>
        C.extract(G.containsV(loadedG)(v))

      case (v, id) if id == vs.length - 1 =>
        val a = for {
          isV <- G.containsV(loadedG)(v)
          neighbours <- G.neighbours(loadedG)(v)
        } yield isV && neighbours.toVector == Vector(vs(id - 1) -> e)
        C.extract(a)
    }
  }

  def withEmptyGraph[V, E, A](f: F[G[V, E]] => A): A
}

object GraphProps {

  import zio._
  import zio.interop.catz._

  implicit val comonadForTask: Comonad[Task] = new Comonad[Task] {

    override def map[A, B](fa: Task[A])(f: A => B): Task[B] = fa.map(f)

    override def coflatMap[A, B](fa: Task[A])(f: Task[A] => B): Task[B] =
      Task {
        f(fa)
      }

    override def extract[A](x: Task[A]): A =
      zio.Runtime.default.unsafeRun(x)

  }

  implicit val comonadForIO: Comonad[cats.effect.IO] =
    new Comonad[cats.effect.IO] {

      override def map[A, B](fa: cats.effect.IO[A])(
          f: A => B
      ): cats.effect.IO[B] = fa.map(f)

      override def coflatMap[A, B](
          fa: cats.effect.IO[A]
      )(f: cats.effect.IO[A] => B): cats.effect.IO[B] =
        cats.effect.IO {
          f(fa)
        }

      override def extract[A](x: cats.effect.IO[A]): A =
        x.unsafeRunSync()
    }

  implicit def graphProps[G[_, _], F[_]: Sync: Comonad](
      implicit G: Graph[G, F]
  ): GraphProps[G, F] =
    new GraphProps[G, F] {

      override def withEmptyGraph[V, E, A](
          f: F[G[V, E]] => A
      ): A =
        Comonad[F].extract(
          G.empty[V, E]
            .use { g => Sync[F].delay(f(Sync[F].pure(g))) }
        )

    }

  def apply[G[_, _], F[_]](implicit GP: GraphProps[G, F]) = GP
}
