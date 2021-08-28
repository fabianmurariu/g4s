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

  test(
    "Optimize a one hop graph match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"
  ) {

    val query = """match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    implicit val ec = IO.contextShift(ExecutionContext.Implicits.global)
    val physicalPlan = for {
      graph <- ConcurrentDirectedGraph[IO, fix.Vertex, fix.Relation]
      _ <- Resource.liftF(
        for {
          src <- graph.insertVertex(new A)
          dst <- graph.insertVertex(new C)
          _ <- graph.insertEdge(src, dst, new X)
        } yield ()
      )
      memo <- Resource.liftF(Optimizer[IO].optimize(queryGraph, graph))
      op <- Resource.liftF(memo.physical(Binding("a")))
      _ <- Resource.liftF(IO.delay(println(op.show())))
      _ <- Resource.liftF {
        Render(op).eval {
          case OutputRecord(buf) =>
            IO.delay(buf.foreach(println))
        }
      }
    } yield op

    physicalPlan.use(_ => IO.unit).unsafeRunSync()
  }

}
