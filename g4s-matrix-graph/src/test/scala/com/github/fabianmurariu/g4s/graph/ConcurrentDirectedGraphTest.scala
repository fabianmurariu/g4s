package com.github.fabianmurariu.g4s.graph

import cats.effect.IO
import com.github.fabianmurariu.g4s.IOSupport
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import com.github.fabianmurariu.g4s.traverser._

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
  
  test("insert 2 nodes with one edge and traverse the edge from (a)-[X]->(b)") {
    val a = new Av
    val b = new Bv
    graph.use { g =>
      for {
        src <- g.insertVertex(a)
        dst <- g.insertVertex(b)
        _ <- g.insertEdge(src, dst, new X)
        out <- g.eval(
          MasterPlan(
            Map(
              NodeRef(bTag) -> Expand(
                NodeLoad(aTag),
                NodeLoad(bTag),
                xTag,
                transpose = false
              )
            )
          )
        )
        _ <- IO.delay {
          val (_, actual: Iterable[Vertex]) = out.head
          assertEquals(actual.toVector, Vector(b))
        }
      } yield b
    }

  }
}
