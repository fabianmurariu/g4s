package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck.Prop._
import org.scalacheck.Arbitrary
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler}
import scala.concurrent.ExecutionContext
import cats.effect.IO
import munit.ScalaCheckSuite
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import scala.concurrent.ExecutionContextExecutor

class ElemWiseSpec extends ScalaCheckSuite {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def intersect[A: Arbitrary: ClassTag: SparseMatrixHandler: Ordering: Reduce: EqOp](
      implicit OP: BuiltInBinaryOps[A],
      N: Numeric[A]
  ): Unit = {
    property(
      s"intersect/union matrix with itself ${implicitly[ClassTag[A]]} sum every element expect it to be x*2 and equal with eachoter"
    ) {
      forAll { mt: MatrixTuples[A] =>
        val rM1M2 = for {
          m1 <- GrBMatrix[IO, A](mt.rows, mt.cols)
          m2 <- GrBMatrix[IO, A](mt.rows, mt.cols)
          into <- GrBMatrix[IO, A](mt.rows, mt.cols)
        } yield (m1, m2, into)

        val io = rM1M2
          .use {
            case (mat1, mat2, into) =>
              for {
                _ <- mat1.set(mt.tuples)
                _ <- mat2.set(mt.tuples)
                intersection <- ElemWise[IO].intersect(into)(Left(OP.plus))(mat1, mat2)
                union <- ElemWise[IO].union(into)(Left(OP.plus))(mat1, mat2)
                ts1 <- intersection.extract
                ts2 <- union.extract
                check <- intersection.isEq(union)
              } yield (ts1._3, ts2._3, check)
          }
          .map { case (actualIntersect, actualUnion, areEqual) =>
            assert(areEqual, "intersect/union matrix with itself should yield the same matrix")
            val expected = mt.tuples.map(t => N.times(t._3, N.fromInt(2))).sorted
            assertEquals(
              actualIntersect.sorted.toVector,
              expected)

            assertEquals(
              actualUnion.sorted.toVector,
              expected)
          }

        io.unsafeRunSync()
      }
    }

  }

  // intersect[Boolean]
  intersect[Byte]
  intersect[Short]
  intersect[Int]
  intersect[Long]
  intersect[Float]
  intersect[Double]

}
