package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck.Prop._
import org.scalacheck.Arbitrary
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler}
import cats.effect.IO
import munit.ScalaCheckSuite
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.MonoidBuilder
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

class MxMSpec extends ScalaCheckSuite {
  implicit val runtime =  cats.effect.unsafe.IORuntime.global

  def associative[A: Arbitrary: ClassTag: SparseMatrixHandler: Ordering: MonoidBuilder: Reduce: EqOp](
      implicit OP: BuiltInBinaryOps[A],
      N: Numeric[A]
  ): Unit = {
    property(
      s"MxM is associative ${implicitly[ClassTag[A]]}"
    ) {
      forAll { mt: MxMSample[A] =>
        val rM1M2 = for {
          add <- GrBMonoid[IO, A](OP.plus, N.zero)
          sr <- GrBSemiring[IO, A, A, A](add, OP.times)
          ma <- GrBMatrix[IO, A](mt.size, mt.size)
          mb <- GrBMatrix[IO, A](mt.size, mt.size)
          mc <- GrBMatrix[IO, A](mt.size, mt.size)
          left <- GrBMatrix[IO, A](mt.size, mt.size)
          right <- GrBMatrix[IO, A](mt.size, mt.size)
        } yield (ma, mb, mc, left, right, sr)

        val io = rM1M2
          .use {
            case (ma, mb, mc, left, right, semiR) =>
              for {
                _ <- ma.set(mt.a.tuples)
                _ <- mb.set(mt.b.tuples)
                _ <- mc.set(mt.c.tuples)
                // left = (A*B)*C
                _ <- MxM[IO].mxm(left)(ma, mb)(semiR)
                _ <- MxM[IO].mxm(left)(left, mc)(semiR)
                // right = A*(B*C)
                _ <- MxM[IO].mxm(right)(mb, mc)(semiR)
                _ <- MxM[IO].mxm(right)(ma, right)(semiR)
                check <- left.isEq(right)
              } yield check
          }

        io.unsafeRunSync()
      }
    }

  }

  // associative[Boolean]
  associative[Byte]
  associative[Short]
  associative[Int]
  associative[Long]
  // FIXME: uncomment when isEqual is implemented with GRB
  // associative[Float]
  // associative[Double]

}
