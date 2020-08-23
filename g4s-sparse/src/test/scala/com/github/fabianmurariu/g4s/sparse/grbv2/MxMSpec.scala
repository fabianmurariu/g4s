package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck.Prop._
import org.scalacheck.Arbitrary
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.{SparseMatrixHandler}
import scala.concurrent.ExecutionContext
import cats.effect.IO
import munit.ScalaCheckSuite
import scala.collection.immutable.{Vector => SVector}
import com.github.fabianmurariu.g4s.sparse.grb.ElemWise
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.MonoidBuilder
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.EqOp

class MxMSpec extends ScalaCheckSuite {
  implicit val ec = ExecutionContext.global

  def associative[A: Arbitrary: ClassTag: SparseMatrixHandler: Ordering: MonoidBuilder: Reduce: EqOp](
      implicit OP: BuiltInBinaryOps[A],
      N: Numeric[A]
  ) = {
    property(
      s"MxM is associative ${implicitly[ClassTag[A]]}"
    ) {
      forAll { mt: MxMSample[A] =>
        val rM1M2 = for {
          add <- GrBMonoid[IO, A](OP.plus, N.zero)
          sr <- GrBSemiring[IO, A, A, A](add, OP.times)
          ma <- Matrix[IO, A](mt.size, mt.size)
          mb <- Matrix[IO, A](mt.size, mt.size)
          mc <- Matrix[IO, A](mt.size, mt.size)
          left <- Matrix[IO, A](mt.size, mt.size)
          right <- Matrix[IO, A](mt.size, mt.size)
        } yield (ma, mb, mc, left, right, sr)

        val io = rM1M2
          .use {
            case (ma, mb, mc, left, right, semiR) =>
              for {
                _ <- ma.set(mt.a.tuples)
                _ <- mb.set(mt.b.tuples)
                _ <- mc.set(mt.c.tuples)
                // left = (A*B)*C
                _ <- MxM[IO].mxm(left)(semiR)(ma, mb)
                _ <- MxM[IO].mxm(left)(semiR)(left, mc)
                // right = A*(B*C)
                _ <- MxM[IO].mxm(right)(semiR)(mb, mc)
                _ <- MxM[IO].mxm(right)(semiR)(ma, right)
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
