package com.github.fabianmurariu.g4s.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import cats.effect.IO
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable

class GraphDBSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "GraphDB" should "create an empty database as a resource" in {
    val gResource = GraphDB.default[Task]
    import GraphStep._

    val program = gResource.use{ g =>

      val gQuery = for {
        michael <- createNode("Person", "Manager")
        jennifer <- createNode("Person", "Employee")
        msft <- createNode("Microsoft")
        food <- createNode("Mexican")
        _ <- createEdge(jennifer, "is_friend", michael)
        _ <- createEdge(jennifer, "likes", food)
        _ <- createEdge(jennifer, "works_for", msft)
        res <- query(vs)
      } yield res


      val a: Observable[QueryResult] = gQuery.foldMap(GraphStep.interpreter[GrBMatrix](g))

      a.headL
    }

    val actual = program.runSyncUnsafe()

    actual shouldBe Vertices(Vector(
                                  0L -> Set("Person", "Manager"),
                                  1L -> Set("Person", "Employee"),
                                  2L -> Set("Microsoft"),
                                  3L -> Set("Mexican")
                                ))
  }
}

