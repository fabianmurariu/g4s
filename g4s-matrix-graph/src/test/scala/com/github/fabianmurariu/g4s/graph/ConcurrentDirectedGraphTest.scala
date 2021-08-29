package com.github.fabianmurariu.g4s.graph

import fix._
import cats.effect.IO
import cats.implicits._
import com.github.fabianmurariu.g4s.IOSupport
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random
import cats.effect.Resource

class ConcurrentDirectedGraphTest extends IOSupport {

  def graph: Resource[IO, ConcurrentDirectedGraph[Vertex, Relation]] =
    ConcurrentDirectedGraph[Vertex, Relation]

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
    def vec = Vector(new A, new B, new C, new D, new E)
    graph.use { g =>
      val nodes = (0 until 3000)
        .map(_ => vec(Random.nextInt(5)))
        .toVector

      nodes.traverse[IO, Long](g.insertVertex(_))
    }
  }

  override def munitTimeout: Duration = new FiniteDuration(30, TimeUnit.MINUTES)
 
}
