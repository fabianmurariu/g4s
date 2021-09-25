package com.github.fabianmurariu.g4s.optim

import munit.ScalaCheckSuite
import org.scalacheck.Prop._
import org.scalacheck.Gen
import org.scalacheck.Arbitrary
import cats.Traverse
import cats.effect.unsafe.IORuntime

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
      val io = for {
        ss <- StatsStore()
        _ <- Traverse[Vector].foldM(sample.vs, ss) { (ss, label) =>
          ss.addNode(label)
        }
        selA <- ss.nodeSel(Some("a"))
        selB <- ss.nodeSel(Some("b"))
      } yield (selA, selB)
      val (selA, selB) = io.unsafeRunSync()(IORuntime.global)
      s"selectivity for A is $selA" |: eqAprox(selA, 0.5d) && eqAprox(
        selB,
        0.5d
      )
    }
  }

  property("nodes selectivity 60% 30% 10%") {
    forAll { sample: Selectivity60_30_10pc[String] =>
      val io = for {
        ss <- StatsStore()
        _ <- Traverse[Vector].foldM(sample.vs, ss) { (ss, label) =>
          ss.addNode(label)
        }
        selA <- ss.nodeSel(Some("a"))
        selB <- ss.nodeSel(Some("b"))
        selC <- ss.nodeSel(Some("c"))
      } yield (selA, selB, selC)
      val res @ (selA, selB, selC) = io.unsafeRunSync()(IORuntime.global)
      s"$res" |: eqAprox(selA, 0.6d) && eqAprox(selB, 0.3d) && eqAprox(
        selC,
        0.1d
      )
    }
  }

  property("edge selectivity 50%") {
    forAll { (sample: NodeWith2Edges[String]) =>
      val io = for {
        ss <- StatsStore()
        _ <- Traverse[Vector].foldM(sample.vs, ss) {
          case (ss, (src, edge, dst)) =>
            for {
              s1 <- ss.addNode(src)
              s2 <- s1.addNode(dst)
              s3 <- s2.addTriplet(src, edge, dst)
            } yield s3
        }
        nodeTotal <- ss.nodesTotal(None)
        edgesTotal <- ss.edgesTotal(None)
        aTotal <- ss.nodesTotal(Some("a"))
        bTotal <- ss.nodesTotal(Some("b"))
        xTotal <- ss.edgesTotal(Some("x"))
        yTotal <- ss.edgesTotal(Some("y"))
        axTotal <- ss.nodeEdgeOut("a", "x")
        ayTotal <- ss.nodeEdgeOut("a", "y")
        xbTotal <- ss.nodeEdgeIn("x", "b")
        ybTotal <- ss.nodeEdgeIn("y", "b")
        selA <- ss.nodeSel(Some("a"))
        selB <- ss.nodeSel(Some("b"))
        selAX <- ss.nodeEdgeOutSel("a", "x")
        selAY <- ss.nodeEdgeOutSel("a", "y")
        selXB <- ss.nodeEdgeInSel("x", "b")
        selYB <- ss.nodeEdgeInSel("y", "b")
      } yield (
        (nodeTotal, edgesTotal, aTotal, bTotal, xTotal + yTotal,
         axTotal + ayTotal + xbTotal + ybTotal),
        selA,
        selB,
        selAX,
        selAY,
        selXB,
        selYB
      )

      val res @ (totals, selA, selB, selAX, selAY, selXB, selYB) =
        io.unsafeRunSync()(IORuntime.global)
      s"$res" |: {
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
