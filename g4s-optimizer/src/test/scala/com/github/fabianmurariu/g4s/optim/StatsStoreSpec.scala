package com.github.fabianmurariu.g4s.optim

import munit.ScalaCheckSuite
import org.scalacheck.Prop._
import org.scalacheck.Gen
import org.scalacheck.Arbitrary

class StatsStoreSpec extends ScalaCheckSuite {

  case class Selectivity50pc[T](vs: Vector[T])
  case class Selectivity60_30_10pc[T](vs: Vector[T])

  case class NodeWith2Edges[T](vs: Vector[(T, T, T)])

  implicit val selectivity50pc =
    Arbitrary(
      Gen
        .containerOfN[Vector, String](500, Gen.oneOf("a", "b"))
        .map(Selectivity50pc(_))
    )

  implicit val selectivity60_30_10pc =
    Arbitrary(
      Gen
        .containerOfN[Vector, String](
          500,
          Gen.frequency(
            6 -> "a",
            3 -> "b",
            1 -> "c"
          )
        )
        .map(Selectivity60_30_10pc(_))
    )

  implicit val outEdgeSelectivity2 =
    Arbitrary(
      Gen
        .containerOfN[Vector, (String, String, String)](
          500,
          for {
            src <- Gen.const("a")
            dst <- Gen.const("b")
            edge <- Gen.oneOf("x", "y")
          } yield (src, edge, dst)
        )
        .map(NodeWith2Edges(_))
    )

  property("nodes selectivity 50%") {
    forAll { sample: Selectivity50pc[String] =>
      val ss = sample.vs.foldLeft(StatsStore()) { (s, label) =>
        s.addNode(label)
      }

      val selA = ss.nodeSel(Some("a"))
      val selB = ss.nodeSel(Some("b"))
      s"selectivity for A is $selA" |: eqAprox(selA, 0.5d) && eqAprox(
        selB,
        0.5d
      )

    }
  }

  property("nodes selectivity 60% 30% 10%") {
    forAll { sample: Selectivity60_30_10pc[String] =>
      val ss = sample.vs.foldLeft(StatsStore()) { (s, label) =>
        s.addNode(label)
      }

      val selA = ss.nodeSel(Some("a"))
      val selB = ss.nodeSel(Some("b"))
      val selC = ss.nodeSel(Some("c"))

      val res = (selA, selB, selC)
      s"$res" |: eqAprox(selA, 0.6d) && eqAprox(selB, 0.3d) && eqAprox(
        selC,
        0.1d
      )
    }
  }

  property("edge selectivity 50%") {
    forAll { (sample: NodeWith2Edges[String]) =>
      val ss = sample.vs.foldLeft(StatsStore()) {
        case (s, (src, edge, dst)) =>
          s.addNode(src).addNode(dst).addTriplet(src, edge, dst)
      }

      val nodeTotal = ss.nodesTotal(None)
      val edgesTotal = ss.edgesTotal(None)
      val aTotal = ss.nodesTotal(Some("a"))
      val bTotal = ss.nodesTotal(Some("b"))
      val xTotal = ss.edgesTotal(Some("x"))
      val yTotal = ss.edgesTotal(Some("y"))
      val axTotal = ss.nodeEdgeOut("a", "x")
      val ayTotal = ss.nodeEdgeOut("a", "y")
      val xbTotal = ss.nodeEdgeIn("x", "b")
      val ybTotal = ss.nodeEdgeIn("y", "b")
      val selA = ss.nodeSel(Some("a"))
      val selB = ss.nodeSel(Some("b"))
      val selAX = ss.nodeEdgeOutSel("a", "x")
      val selAY = ss.nodeEdgeOutSel("a", "y")
      val selXB = ss.nodeEdgeInSel("x", "b")
      val selYB = ss.nodeEdgeInSel("y", "b")

      val totals = (
        nodeTotal,
        edgesTotal,
        aTotal,
        bTotal,
        xTotal + yTotal,
        axTotal + ayTotal + xbTotal + ybTotal
      )

      s"$totals" |: {
        eqAprox(selA, 0.5d) &&
        eqAprox(selB, 0.5d) &&
        eqAprox(selAX, 0.5d) &&
        eqAprox(selAY, 0.5d) &&
        eqAprox(selXB, 0.5d) &&
        eqAprox(selYB, 0.5d) &&
        totals == ((1000L, 500L, 500L, 500L, 500L, 1000L))
      }

    }
  }
  def eqAprox(actual: Double, expected: Double): Boolean =
    Math.abs(actual) - Math.abs(expected) < (0.15 * expected)

}
