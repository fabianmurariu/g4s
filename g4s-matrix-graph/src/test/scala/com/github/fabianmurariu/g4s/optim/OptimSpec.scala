package com.github.fabianmurariu.g4s.optim

import cats.effect.IO
import com.github.fabianmurariu.g4s.graph.ConcurrentDirectedGraph
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import cats.effect.Resource
import fix._
import com.github.fabianmurariu.g4s.optim.impls.MatrixTuples
import com.github.fabianmurariu.g4s.optim.impls.OutputRecord

class OptimSpec extends munit.FunSuite {

  implicit val runtime =  cats.effect.unsafe.IORuntime.global

  test(
    "Optimize a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"
  ) {

    val query = """match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val physicalPlan = for {
      graph <- ConcurrentDirectedGraph[fix.Vertex, fix.Relation]
      _ <- Resource.eval(
        for {
          src <- graph.insertVertex(new A)
          dst <- graph.insertVertex(new C)
          _ <- graph.insertEdge(src, dst, new X)
        } yield ()
      )
      memo <- Resource.eval(Optimizer.default.optimize(queryGraph, graph))
      op <- Resource.eval(memo.physical(Binding("a")))
      _ <- Resource.eval(IO.delay(println(op.show())))
      _ <- Resource.eval {
        MatrixTuples(op).eval {
          case OutputRecord(buf) =>
            IO.delay(buf.foreach(println))
        }
      }
    } yield op

    physicalPlan.use(_ => IO.unit).unsafeRunSync()
  }

  test(
    "Optimize a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`)<-[:`fix.Y`]-(b:`fix.B`) return c".only
  ) {

    val query = "match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`)<-[:`fix.Y`]-(b:`fix.B`) return c"

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    val physicalPlan = for {
      graph <- ConcurrentDirectedGraph[fix.Vertex, fix.Relation]
      _ <- Resource.eval(
        for {
          a <- graph.insertVertex(new A)
          c <- graph.insertVertex(new C)
          b <- graph.insertVertex(new B)
          _ <- graph.insertEdge(a, c, new X)
          _ <- graph.insertEdge(b, c, new Y)
        } yield ()
      )
      memo <- Resource.eval(Optimizer.default.optimize(queryGraph, graph))
      op <- Resource.eval(memo.physical(Binding("c")))
      _ <- Resource.eval(IO.delay(println(op.show())))
      _ <- Resource.eval {
        MatrixTuples(op).eval {
          case OutputRecord(buf) =>
            IO.delay(buf.foreach(println))
        }
      }
    } yield op

    physicalPlan.use(_ => IO.unit).unsafeRunSync()
  }


}
