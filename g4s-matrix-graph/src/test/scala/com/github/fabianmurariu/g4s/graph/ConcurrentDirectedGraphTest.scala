package com.github.fabianmurariu.g4s.graph

import fix._
import cats.effect.IO
import cats.implicits._
import com.github.fabianmurariu.g4s.IOSupport
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import com.github.fabianmurariu.g4s.traverser._

import scala.util.Random

class ConcurrentDirectedGraphTest extends IOSupport with QueryGraphSamples {

  def graph = ConcurrentDirectedGraph[IO, Vertex, Relation]

  test("insert one node") {
    graph.use { g => g.insertVertex(new A).map(id => assertEquals(id, 0L)) }
  }

  test("insert 2 nodes with one edge") {
    graph.use { g =>
      for {
        src <- g.insertVertex(new A)
        dst <- g.insertVertex(new B)
        _ <- g.insertEdge(src, dst, new X)
      } yield assertEquals((src, dst), (0L, 1L))
    }
  }

  test("insert a node and get it back") {
    graph.use { g =>
      val av = new A
      for {
        a <- g.insertVertex(av)
        aOut <- g.getV(a)
      } yield assertEquals(aOut, Some(av))
    }
  }

  test("insert 3000 nodes") {
    graph.use { g =>
      val nodes = (0 until 3000)
        .map(_ => Random.nextInt(5))
        .map {
          case 0 => new A
          case 1 => new B
          case 2 => new C
          case 3 => new D
          case 4 => new E
        }
        .toVector

      nodes.traverse[IO, Long](g.insertVertex(_))
    }
  }

}
