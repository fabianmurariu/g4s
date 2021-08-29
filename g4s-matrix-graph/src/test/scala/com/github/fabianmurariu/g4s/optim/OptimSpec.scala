package com.github.fabianmurariu.g4s.optim

import cats.effect.IO
import com.github.fabianmurariu.g4s.graph.ConcurrentDirectedGraph
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import scala.concurrent.ExecutionContext
import cats.effect.Resource
import fix._
import com.github.fabianmurariu.g4s.optim.impls.Render
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
        Render(op).eval {
          case OutputRecord(buf) =>
            IO.delay(buf.foreach(println))
        }
      }
    } yield op

    physicalPlan.use(_ => IO.unit).unsafeRunSync()
  }

}
