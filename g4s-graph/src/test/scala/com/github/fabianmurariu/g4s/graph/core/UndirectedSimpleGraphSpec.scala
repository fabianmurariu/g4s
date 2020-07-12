package com.github.fabianmurariu.g4s.graph.core

import org.scalacheck._

abstract class UndirectedSimpleGraphSpec[G[_, _], F[_]](name: String)(
    implicit GP: GraphProps[G, F],
    G: Graph[G, F]
) extends Properties(name) {

  import Prop.forAll
  import GP._

  property("return a vertex inserted") = forAll { i: Int =>
    withEmptyGraph[Int, Int, Prop](returnAVertexInserted(_)(i))
  }

  property("link 2 vertices, they are eachother neightbours") = forAll {
    (v1: Int, v2: Int, e: String) =>
      withEmptyGraph[Int, String, Prop](
        undirectedLink2VerticesAreEachotherNeighbour(_)(v1, v2, e)
      )
  }

  property("can represent a line graph") = forAll { (vs: Vector[Int]) =>
    withEmptyGraph[Int, String, Prop](
      canRepresentALineGraph(_)(vs.distinct, "mock")
    )
  }

  property("can find 2 connected components in a graph") = forAll {
    dsv: DisjointVertices =>
      // two hyperconnected components
      val cc1 = for {
        s <- dsv.vs1
        d <- dsv.vs1
      } yield (s, "odd", d)

      val cc2 = for {
        s <- dsv.vs2
        d <- dsv.vs2
      } yield (s, "even", d)

      withEmptyGraph[Int, String, Prop](
        canFind2ConnectedComponentsInAGraph(_)(cc1, cc2)
      )
  }

  case class DisjointVertices(vs1: List[Int], vs2: List[Int])
  object DisjointVertices {
    implicit val gen: Arbitrary[DisjointVertices] = {
      val odds = Gen.choose(0, Int.MaxValue).map(i => 2 * i)
      val even = Gen.choose(0, Int.MaxValue).map(i => 2 * i + 1)
      Arbitrary(for {
        vs1 <- Gen.nonEmptyListOf(odds)
        vs2 <- Gen.nonEmptyListOf(even)
      } yield DisjointVertices(vs1, vs2))
    }
  }
}
