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
import cats.effect.Resource

class ConcurrentDirectedGraphTest extends IOSupport with QueryGraphSamples {

  def graph: Resource[IO, ConcurrentDirectedGraph[IO, Vertex, Relation]] =
    ConcurrentDirectedGraph[IO, Vertex, Relation]

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

  test("single edge: path (a)-[:X]->(b) ret b on diamond network") {
    val query = for {
      a <- node[A]
      b <- node[B]
      _ <- edge[X](a, b)
    } yield Ret(b)

    val b1Node = new B
    val b2Node = new B

    graph.use { g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(b1Node)
        b2 <- g.insertVertex(b2Node)
        c <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X)
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet, Set[Vector[Vertex]](Vector(b1Node), Vector(b2Node)))
      }
    }
  }

  test("single edge: path (a)-[:X]->(b) ret a, b on diamond network") {
    val query = for {
      a <- node[A]
      b <- node[B]
      _ <- edge[X](a, b)
    } yield Ret(a, b)

    val b1Node = new B
    val b2Node = new B
    val aNode = new A
    graph.use { g =>
      for {
        a <- g.insertVertex(aNode)
        b1 <- g.insertVertex(b1Node)
        b2 <- g.insertVertex(b2Node)
        c <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X)
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet,
                     Set[Vector[Vertex]](Vector(aNode, b1Node), Vector(aNode, b2Node)))
      }
    }
  }

  test("2 endge path (a)-[:X]->(b)-[:Y]->(c) ret c on diamond network") {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(c)

    val cNode = new C

    graph.use { g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(new B)
        b2 <- g.insertVertex(new B)
        c <- g.insertVertex(cNode)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X)
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet, Set[Vector[Vertex]](Vector(cNode)))
      }
    }
  }

  test("2 endge path (a)-[:X]->(b)-[:Y]->(c) ret b on diamond network") {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(b)

    val b1Node = new B
    val b2Node = new B

    graph.use { g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(b1Node)
        b2 <- g.insertVertex(b2Node)
        c <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X)
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet, Set[Vector[Vertex]](Vector(b1Node), Vector(b2Node)))
      }
    }
  }

  test("diamond network with 3 returns, single path (a)-[:X]->(b)-[:Y]->(c) ret a, b, c") {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, b, c)

    val aNode = new A
    val b1Node = new B
    val b2Node = new B
    val cNode = new C

    graph.use { g =>
      for {
        a <- g.insertVertex(aNode)
        b1 <- g.insertVertex(b1Node)
        b2 <- g.insertVertex(b2Node)
        c <- g.insertVertex(cNode)
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new Z)
        _ <- g.insertEdge(b2, c, new W)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet,
                     Set[Vector[Vertex]](
                       Vector(aNode, b1Node, cNode)
                     ))
      }
    }
  }

  test("fork network: query (a)-[:X]->(b)-[:Y]->(c) ret a, c") {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, c)

    val aNode = new A
    val c1Node = new C
    val c2Node = new C
    graph.use { g =>
      for {
        a <- g.insertVertex(aNode)
        b <- g.insertVertex(new B)
        c1 <- g.insertVertex(c1Node)
        c2 <- g.insertVertex(c2Node)
        _ <- g.insertEdge(a, b, new X)
        _ <- g.insertEdge(b, c1, new Y)
        _ <- g.insertEdge(b, c2, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet,
                     Set[Vector[Vertex]](
                       Vector(aNode, c1Node),
                       Vector(aNode, c2Node)))
      }
    }
  }

  test("fork network path 3-1-2 (a)-[:X]->(b)-[:Y]->(c) ret a, c".only) {
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, c)

    val (a1Node, a2Node, a3Node) = (new A, new A, new A)
    val bNode = new B
    val (c1Node, c2Node) = (new C, new C)

    graph.use { g =>
      for {
        a1 <- g.insertVertex(a1Node)
        a2 <- g.insertVertex(a2Node)
        a3 <- g.insertVertex(a3Node)
        b <- g.insertVertex(bNode)
        c1 <- g.insertVertex(c1Node)
        c2 <- g.insertVertex(c2Node)
        _ <- g.insertEdge(a1, b, new X)
        _ <- g.insertEdge(a2, b, new X)
        _ <- g.insertEdge(a3, b, new X)
        _ <- g.insertEdge(b, c1, new Y)
        _ <- g.insertEdge(b, c2, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet,
                     Set[Vector[Vertex]](
                       Vector(a1Node, c1Node),
                       Vector(a2Node, c1Node),
                       Vector(a3Node, c1Node),
                       Vector(a1Node, c2Node),
                       Vector(a2Node, c2Node),
                       Vector(a3Node, c2Node)
                       ))
      }
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

    graph.use { g =>
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
        results <- g.resolveTraverser(query).compile.toList
        _ <- IO.delay(println(results))
      } yield ()
    }
  }
}
