package com.github.fabianmurariu.g4s.graph

import fix._
import cats.effect.IO
import cats.implicits._
import com.github.fabianmurariu.g4s.IOSupport
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import com.github.fabianmurariu.g4s.traverser._
import com.github.fabianmurariu.g4s.traverser.Traverser._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
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

  test("insert 3000 nodes".ignore) {
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

  test("diamond network: path (a)-[:X]->(b)-[:Y]->(c)".ignore) {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, c)

    graph.use{ g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(new B)
        b2 <- g.insertVertex(new B)
        c <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X)
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query, true).compile.toList
        _ <- IO.delay(println(results))
      } yield ()
    }
  }
  test("diamon network top path (a)-[:X]->(b)-[:Y]->(c), avoid (a)-[:Y]->(b)-[:W]->(c)".ignore) {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, c)

    graph.use{ g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(new B)
        b2 <- g.insertVertex(new B)
        c <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new Z)
        _ <- g.insertEdge(b2, c, new W)
        results <- g.resolveTraverser(query, true).compile.toList
        _ <- IO.delay(println(results))
      } yield ()
    }
  }

  test("fork network path (a)-[:X]->(b)-[:Y]->(c)".ignore) {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, c)

    graph.use{ g =>
      for {
        a <- g.insertVertex(new A)
        b <- g.insertVertex(new B)
        c1 <- g.insertVertex(new C)
        c2 <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b, new X)
        _ <- g.insertEdge(b, c1, new Y)
        _ <- g.insertEdge(b, c2, new Y)
        results <- g.resolveTraverser(query, true).compile.toList
        _ <- IO.delay(println(results))
      } yield ()
    }
  }

  test("fork network path 3-1-2 (a)-[:X]->(b)-[:Y]->(c) ret a, c".ignore) {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, c)

    graph.use{ g =>
      for {
        a1 <- g.insertVertex(new A)
        a2 <- g.insertVertex(new A)
        a3 <- g.insertVertex(new A)
        b <- g.insertVertex(new B)
        c1 <- g.insertVertex(new C)
        c2 <- g.insertVertex(new C)
        _ <- g.insertEdge(a1, b, new X)
        _ <- g.insertEdge(a2, b, new X)
        _ <- g.insertEdge(a3, b, new X)
        _ <- g.insertEdge(b, c1, new Y)
        _ <- g.insertEdge(b, c2, new Y)
        results <- g.resolveTraverser(query, false).compile.toList
        _ <- IO.delay(println(results))
      } yield ()
    }
  }

  override def munitTimeout: Duration = new FiniteDuration(30, TimeUnit.MINUTES)

  test("fork network path 3-1-2 (a)-[:X]->(b)-[:Y]->(c) ret b".ignore) {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(b, c)

    graph.use{ g =>
      for {
        a1 <- g.insertVertex(new A)
        a2 <- g.insertVertex(new A)
        a3 <- g.insertVertex(new A)
        b <- g.insertVertex(new B)
        c1 <- g.insertVertex(new C)
        c2 <- g.insertVertex(new C)
        _ <- g.insertEdge(a1, b, new X)
        _ <- g.insertEdge(a2, b, new X)
        _ <- g.insertEdge(a3, b, new X)
        _ <- g.insertEdge(b, c1, new Y)
        _ <- g.insertEdge(b, c2, new Y)
        results <- g.resolveTraverser(query, true).compile.toList
        _ <- IO.delay(println(results))
      } yield ()
    }
  }
}
