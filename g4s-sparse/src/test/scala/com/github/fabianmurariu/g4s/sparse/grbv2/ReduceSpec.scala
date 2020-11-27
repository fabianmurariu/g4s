package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck.Prop._
import org.scalacheck.Arbitrary
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.{
  SparseMatrixHandler,
  Reduce,
  EqOp
}
import scala.concurrent.ExecutionContext
import munit.ScalaCheckSuite
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.MonoidBuilder
import scala.math.Numeric.Implicits._
import com.github.fabianmurariu.g4s.sparse.grb.SparseVectorHandler
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import cats.effect.IO

class ReduceSpec extends ScalaCheckSuite with SuiteUtils{

  implicit val ec = ExecutionContext.global

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(50)
      .withMaxDiscardRatio(10)


  def reduceSpec[
      A: Arbitrary: ClassTag: SparseMatrixHandler: Ordering: Reduce: EqOp: MonoidBuilder: SparseVectorHandler
  ](implicit N: Numeric[A], OP: BuiltInBinaryOps[A]) = {

    def checkEquals(actual: A, expected: A) = {

      val eps = Math.abs(expected.toDouble * 0.01)
      assert(
        (actual.toDouble.isInfinity && expected.toDouble.isInfinity) ||
          (N.abs(actual) - N.abs(expected)).toDouble < eps ||
          actual == expected,
        s"$expected != $actual"
      )
    }

    property(
      s"reduce all the values with plus monoid ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io = (for {
          plus <- GrBMonoid[IO, A](OP.plus, N.zero)
          mat <- GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
        } yield (plus, mat)).use {
          case (plus, mat) =>
            for {
              sum <- mat.reduce(N.zero, plus)
              tuples <- mat.extract
            } yield {
              val expected = tuples._3.reduce(N.plus)
              val eps = Math.abs(expected.toDouble * 0.01)
              assert(
                (sum.toDouble.isInfinity && expected.toDouble.isInfinity) ||
                  (N.abs(sum) - N.abs(expected)).toDouble < eps ||
                  sum == expected,
                s"$expected != $sum"
              )
            }
        }

        io.unsafeRunSync()
      }
    }

    property(
      s"reduce all the values accross rows and find the max ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io: IO[Unit] = (for {
          mat <- GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
          v <- mat.reduce(OP.max)
        } yield v).use {
          _.extract.map {
            case (_, actualVs) =>
              assertEquals(actualVs.max, vs.max)
          }
        }

        io.unsafeRunSync()
      }
    }

    property(
      s"reduce all the values accross rows and find the min ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io: IO[Unit] = (for {
          mat <- GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
          v <- mat.reduce(OP.min)
        } yield v).use {
          _.extract.map {
            case (_, actualVs) =>
              assertEquals(actualVs.min, vs.min)
          }
        }

        io.unsafeRunSync()
      }
    }

    property(
      s"reduce all the values accross rows and find the sum ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io: IO[Unit] = (for {
          mat <- GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
          v <- mat.reduce(OP.plus)
        } yield v).use {
          _.extract.map {
            case (_, actualVs) =>
              checkEquals(actualVs.sum, vs.sum)
          }
        }

        io.unsafeRunSync()
      }
    }
  }

  // reduceSpec[Boolean]
  reduceSpec[Byte]
  reduceSpec[Short]
  reduceSpec[Int]
  reduceSpec[Long]
  reduceSpec[Float]
  reduceSpec[Double]

}
