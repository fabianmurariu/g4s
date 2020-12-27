package com.github.fabianmurariu.g4s.graph

import cats.implicits._
import cats.effect.IO
import com.github.fabianmurariu.g4s.IOSupport
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import com.github.fabianmurariu.g4s.traverser._
import scala.util.Random

class ConcurrentDirectedGraphTest extends IOSupport with QueryGraphSamples {

  def graph = ConcurrentDirectedGraph[IO, Vertex, Relation]

  test("insert one node") {
    graph.use { g => g.insertVertex(new Av).map(id => assertEquals(id, 0L)) }
  }

  test("insert 2 nodes with one edge") {
    graph.use { g =>
      for {
        src <- g.insertVertex(new Av)
        dst <- g.insertVertex(new Bv)
        _ <- g.insertEdge(src, dst, new X)
      } yield assertEquals((src, dst), (0L, 1L))
    }
  }

  test("insert a node and get it back") {
    graph.use { g =>
      val av = new Av
      for {
        a <- g.insertVertex(av)
        aOut <- g.getV(a)
      } yield assertEquals(aOut, Some(av))
    }
  }

  test("insert 3000 nodes") {
    graph.use { g =>
      val nodes = (0 until 3000).map(_ => Random.nextInt(5)).map {
        case 0 => new Av
        case 1 => new Bv
        case 2 => new Cv
        case 3 => new Dv
        case 4 => new Ev
      }.toVector

      nodes.traverse[IO, Long](g.insertVertex(_))
    }
  }
  
}
