package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.graph.ConcurrentDirectedGraph
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import fix._
import com.github.fabianmurariu.g4s.graph.GraphDB
import com.github.fabianmurariu.g4s.optim.impls.GetNodeMatrix
import com.github.fabianmurariu.g4s.optim.impls.GetEdgeMatrix
import com.github.fabianmurariu.g4s.optim.impls.ExpandMul
import com.github.fabianmurariu.g4s.optim.impls.FilterMul
import cats.effect.IO

class OptimSpec extends munit.FunSuite {

  implicit val runtime = cats.effect.unsafe.IORuntime.global

  test(
    "Optimize2 a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.B`) return b where selectivity of B (high) determines to optimizer outcome A * (X * B)"
  ) {

    val query = """match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`) return b"""

    val io = ConcurrentDirectedGraph[fix.Vertex, fix.Relation].use { graph =>
      for {
        start <- IO.monotonic
        a1 <- graph.insertVertex(new A)
        b1 <- graph.insertVertex(new B)

        a2 <- graph.insertVertex(new A)
        a3 <- graph.insertVertex(new A)
        a4 <- graph.insertVertex(new A)
        a5 <- graph.insertVertex(new A)

        d2 <- graph.insertVertex(new D)
        d3 <- graph.insertVertex(new D)
        d4 <- graph.insertVertex(new D)
        d5 <- graph.insertVertex(new D)

        _ <- graph.insertEdge(a1, b1, new X)
        _ <- graph.insertEdge(a2, d2, new X)
        _ <- graph.insertEdge(a3, d3, new X)
        _ <- graph.insertEdge(a4, d4, new X)
        _ <- graph.insertEdge(a5, d5, new X)
        qg <- GraphDB.parse(query)
        plan <- GraphDB.optim(graph)(qg)
        end <- IO.monotonic
        _ <- IO { println((end - start).toMillis) }
      } yield plan
    }

    val expected = ExpandMul(
      GetNodeMatrix(Binding("a"), Some("fix.A"), 5),
      FilterMul(
        GetEdgeMatrix(None, Some("fix.X"), false, 5),
        GetNodeMatrix(Binding("b"), Some("fix.B"), 1),
        0.1d
      ),
      1.0
    )

    io.flatMap { case (plan, memo) =>
        IO {
          println(plan.show(memo))
          assertEquals(plan, expected)
        }
      }
      .unsafeRunSync()

  }

  test(
    "Optimize2 a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.B`) return b where selectivity of B (low) determines to optimizer outcome (A * X) * B"
  ) {

    val query = """match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`) return b"""

    val io = ConcurrentDirectedGraph[fix.Vertex, fix.Relation].use { graph =>
      for {
        start <- IO.monotonic
        a1 <- graph.insertVertex(new A)

        b1 <- graph.insertVertex(new B)
        b2 <- graph.insertVertex(new B)
        b3 <- graph.insertVertex(new B)
        b4 <- graph.insertVertex(new B)

        _ <- graph.insertEdge(a1, b1, new X)
        _ <- graph.insertEdge(b3, b3, new X)
        _ <- graph.insertEdge(b2, b4, new X)
        qg <- GraphDB.parse(query)
        plan <- GraphDB.optim(graph)(qg)
        end <- IO.monotonic
        _ <- IO { println((end - start).toMillis) }
      } yield plan
    }

    val expected =
      FilterMul(
        ExpandMul(
          GetNodeMatrix(Binding("a"), Some("fix.A"), 1),
          GetEdgeMatrix(None, Some("fix.X"), false, 3),
          1.0
        ),
        GetNodeMatrix(Binding("b"), Some("fix.B"), 4),
        0.8
      )

    io.flatMap { case (plan, memo) =>
        IO {
          println(plan.show(memo))
          assertEquals(plan, expected)
        }
      }
      .unsafeRunSync()

  }

  test(
    """Optimize2 a two hop graph match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`)
      return c where selectivity of C (high) and B (low) determines to optimizer outcome A * X * B * (Y * C)"""
  ) {

    val query =
      """match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`) return c"""

    val io = ConcurrentDirectedGraph[fix.Vertex, fix.Relation].use { graph =>
      for {
        start <- IO.monotonic
        a1 <- graph.insertVertex(new A)

        b1 <- graph.insertVertex(new B)
        b2 <- graph.insertVertex(new B)
        b3 <- graph.insertVertex(new B)
        b4 <- graph.insertVertex(new B)

        c1 <- graph.insertVertex(new C)
        d1 <- graph.insertVertex(new D)

        _ <- graph.insertEdge(a1, b1, new X)
        _ <- graph.insertEdge(a1, b2, new Z)
        _ <- graph.insertEdge(a1, b3, new Z)
        _ <- graph.insertEdge(a1, b4, new Z)

        _ <- graph.insertEdge(b1, c1, new Y)
        _ <- graph.insertEdge(b2, d1, new Y)
        _ <- graph.insertEdge(b3, d1, new Y)
        _ <- graph.insertEdge(b4, d1, new Y)

        qg <- GraphDB.parse(query)
        plan <- GraphDB.optim(graph)(qg)
        end <- IO.monotonic
        _ <- IO {println((end - start).toMillis)}
      } yield plan
    }

    val expected =
      ExpandMul(
        FilterMul(
          ExpandMul(
            GetNodeMatrix(Binding("a"), Some("fix.A"), 1),
            GetEdgeMatrix(None, Some("fix.X"), false, 1),
            0.125
          ),
          GetNodeMatrix(Binding("b"), Some("fix.B"), 4),
          0.5714285714285714
        ),
        FilterMul(
          GetEdgeMatrix(None, Some("fix.Y"), true, 4),
          GetNodeMatrix(Binding("c"), Some("fix.C"), 1),
          0.14285714285714285
        ),
        1.0
      )

    io.flatMap { case (plan, memo) =>
        IO {
          println(plan.show(memo))
          assertEquals(plan, expected)
          assertEquals(plan.output, Vector[Name](Binding("a"), Binding("c")))
        }
      }
      .unsafeRunSync()

  }

  test(
    """Optimize2 a fork with 1 hop each graph match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`)
      return b where selectivity of C (high) and B (low) determines to optimizer outcome A * X * Diag(C * Y * B)""".ignore
  ) {

    val query =
      """match (a:`fix.A`)-[:`fix.X`]->(b:`fix.B`)<-[:`fix.Y`]-(c:`fix.C`) return b"""


    val io = ConcurrentDirectedGraph[fix.Vertex, fix.Relation].use { graph =>
      for {
        start <- IO.monotonic
        a1 <- graph.insertVertex(new A)

        b1 <- graph.insertVertex(new B)
        b2 <- graph.insertVertex(new B)
        b3 <- graph.insertVertex(new B)
        b4 <- graph.insertVertex(new B)

        c1 <- graph.insertVertex(new C)
        d1 <- graph.insertVertex(new D)

        _ <- graph.insertEdge(a1, b1, new X)
        _ <- graph.insertEdge(a1, b2, new X)
        _ <- graph.insertEdge(a1, b3, new X)
        _ <- graph.insertEdge(a1, b4, new X)

        _ <- graph.insertEdge(b1, c1, new Y)
        _ <- graph.insertEdge(b2, d1, new Y)
        _ <- graph.insertEdge(b3, d1, new Y)
        _ <- graph.insertEdge(b4, d1, new Y)

        qg <- GraphDB.parse(query)
        plan <- GraphDB.optim(graph)(qg)
        end <- IO.monotonic
        _ <- IO {
          println((end - start).toMillis)
        }
      } yield plan
    }

    val expected =
      ExpandMul(
        FilterMul(
          ExpandMul(
            GetNodeMatrix(Binding("a"), Some("fix.A"), 1),
            GetEdgeMatrix(None, Some("fix.X"), false, 1),
            0.125
          ),
          GetNodeMatrix(Binding("b"), Some("fix.B"), 4),
          0.5714285714285714
        ),
        FilterMul(
          GetEdgeMatrix(None, Some("fix.Y"), true, 4),
          GetNodeMatrix(Binding("c"), Some("fix.C"), 1),
          0.14285714285714285
        ),
        1.0
      )

    io.flatMap { case (plan, memo) =>
        IO {
          println(plan.show(memo))
          assertEquals(plan, expected)
          assertEquals(plan.output, Vector[Name](Binding("a"), Binding("c")))
        }
      }
      .unsafeRunSync()

  }

  // test(
  //   "Optimize a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"
  // ) {

  //   val query = """match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"""

  //   val Right(queryGraph) = QueryGraph.fromCypherText(query)

  //   val physicalPlan = for {
  //     graph <- ConcurrentDirectedGraph[fix.Vertex, fix.Relation]
  //     _ <- Resource.eval(
  //       for {
  //         src <- graph.insertVertex(new A)
  //         dst <- graph.insertVertex(new C)
  //         _ <- graph.insertEdge(src, dst, new X)
  //       } yield ()
  //     )
  //     memo <- Resource.eval(Optimizer.default.optimize(queryGraph, graph))
  //     op <- Resource.eval(memo.physical(Binding("a")))
  //     _ <- Resource.eval(IO.delay(println(op.show())))
  //     _ <- Resource.eval {
  //       MatrixTuples(op).eval(graph) {
  //         case OutputRecord(buf) =>
  //           IO.delay(buf.foreach(println))
  //       }
  //     }
  //   } yield op

  //   physicalPlan.use(_ => IO.unit).unsafeRunSync()
  // }

  // test(
  //   "Optimize a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`)<-[:`fix.Y`]-(b:`fix.B`) return c"
  // ) {

  //   val query =
  //     "match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`)<-[:`fix.Y`]-(b:`fix.B`) return c"

  //   val Right(queryGraph) = QueryGraph.fromCypherText(query)

  //   val physicalPlan = for {
  //     graph <- ConcurrentDirectedGraph[fix.Vertex, fix.Relation]
  //     _ <- Resource.eval(
  //       for {
  //         a <- graph.insertVertex(new A)
  //         c <- graph.insertVertex(new C)
  //         b <- graph.insertVertex(new B)
  //         _ <- graph.insertEdge(a, c, new X)
  //         _ <- graph.insertEdge(b, c, new Y)
  //       } yield ()
  //     )
  //     memo <- Resource.eval(Optimizer.default.optimize(queryGraph, graph))
  //     op <- Resource.eval(memo.physical(Binding("c")))
  //     _ <- Resource.eval(IO.delay(println(op.show())))
  //     _ <- Resource.eval {
  //       MatrixTuples(op).eval(graph) {
  //         case OutputRecord(buf) =>
  //           IO.delay(buf.foreach(println))
  //       }
  //     }
  //   } yield op

  //   physicalPlan.use(_ => IO.unit).unsafeRunSync()
  // }

}
