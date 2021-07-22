package com.github.fabianmurariu.g4s.optim

import cats.effect.IO
import com.github.fabianmurariu.g4s.graph.ConcurrentDirectedGraph
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import scala.concurrent.ExecutionContext
import cats.effect.Resource

class OptimSpec extends munit.FunSuite {

  test("Optimize a one hop graph") {

    val query = """match (a:`fix.A`)-[:`fix.X`]->(c:`fix.C`) return a"""

    val Right(queryGraph) = QueryGraph.fromCypherText(query)

    implicit val ec = IO.contextShift(ExecutionContext.Implicits.global)
    val physicalPlan = for {
      context <- ConcurrentDirectedGraph.evaluatorGraph[IO, fix.Vertex, fix.Relation]
      memo <- Resource.liftF(Optimizer[IO].optimize(queryGraph).runA(context))
      op <- Resource.liftF(memo.physical(Binding("a")))
    } yield op

    physicalPlan.use(IO.pure).unsafeRunSync()
  }

}
