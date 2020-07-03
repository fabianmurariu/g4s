package com.github.fabianmurariu.g4s.graph.core

import org.scalacheck._
import cats.Comonad
import cats.Id
import cats.Monad
import cats.Traverse

object GraphSpec extends Properties("Adjacency Map is a Graph") {
  import Prop.forAll
  import AdjacencyMapIdProps._

  property("return a vertex inserted") = forAll { i: Int =>
    val g = Graph[AdjacencyMap, Id].empty[Int, Int]
    returnAVertexInserted(g)(i)
  }

  property("link 2 vertices, they are eachother neightbours") = forAll {
    (v1: Int, v2: Int, e: String) =>
      val g = Graph[AdjacencyMap, Id].empty[Int, String]
      undirectedLink2VerticesAreEachotherNeighbour(g)(v1, v2, e)
  }

  property("can represent a line graph") = forAll { (vs: Vector[Int]) =>
    val g = Graph[AdjacencyMap, Id].empty[Int, String]
    canRepresentALineGraph(g)(vs.distinct, "mock")
  }

}

abstract class GraphProps[G[_, _], F[_]](
    implicit G: Graph[G, F],
    C: Comonad[F],
    M: Monad[F]
) {

  import Monad.ops._

  def returnAVertexInserted[V, E](
      fg: F[G[V, E]]
  )(expected: V): Prop = {

    val newG = G.insertVertex(fg)(expected)
    C.extract(G.containsV(newG)(expected))
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
    } yield neighboursV1.toVector == Vector(v2 -> e) &&
      neighboursV2.toVector == Vector(v1 -> e)

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
        } yield isV && neighbours.toVector == Vector(vs(id - 1) -> e, vs(id + 1) -> e)
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
}

object AdjacencyMapIdProps extends GraphProps[AdjacencyMap, Id]
