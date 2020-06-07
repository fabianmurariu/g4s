package com.github.fabianmurariu.g4s.sparse.mutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.scalacheck.Arbitrary
import com.github.fabianmurariu.unsafe.MatrixTuples
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.MatrixHandler
import com.github.fabianmurariu.unsafe.MatrixTuplesMul
import com.github.fabianmurariu.unsafe.MatrixDimensions
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.GrBBinaryOp
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import GrBBinaryOp._
import com.github.fabianmurariu.g4s.sparse.grb.GrBMonoid
import com.github.fabianmurariu.g4s.sparse.grb.MonoidBuilder

import zio._

//TODO: move to zio-tests
class GrBMatrixSpec
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers {
  val runtime = Runtime.default

  behavior of "GrBMatrix"

  // addBasicMatrixTest[Boolean]
  addBasicMatrixTest[Byte]
  addBasicMatrixTest[Short]
  addBasicMatrixTest[Int]
  addBasicMatrixTest[Long]
  addBasicMatrixTest[Float]
  addBasicMatrixTest[Double]

  def addBasicMatrixTest[A: MatrixBuilder: MonoidBuilder: Arbitrary: EqOp](
      implicit MT: MatrixHandler[GrBMatrix, A],
      A: Arbitrary[MatrixTuples[A]],
      B: Arbitrary[MatrixTuplesMul[A]],
      M: Matrix[GrBMatrix],
      OP: BuiltInBinaryOps[A],
      CT: ClassTag[A],
      N: Numeric[A]
  ) = {

    it should s"create a matrix of ${CT.toString()} set values into it, get values from it then release it" in forAll {
      mt: MatrixTuples[A] =>
        val m = GrBMatrix[A](mt.dim.rows, mt.dim.cols)

        runtime.unsafeRun(m.use { mat =>
          IO.effect {
            mt.vals.foreach {
              case (i, j, v) =>
                mat.set(i, j, v)
            }

            mt.vals.foreach {
              case (i, j, v) =>
                mat.get(i, j) shouldBe Some(v)
            }
          }
        })
    }

    it should s"create 2 matrices of ${CT.toString()} set n,m to a and -a and verify equality" in forAll {
      (mt: MatrixTuples[A], a1: A, a2: A) =>
        val mats = for {
          mata <- GrBMatrix[A](mt.dim.rows, mt.dim.cols)
          matb <- GrBMatrix[A](mt.dim.rows, mt.dim.cols)
        } yield (mata, matb)

        val io = mats.use {
          case (mata, matb) =>
            for {
              _ <- IO.effect {

              mt.vals.foreach {
                case (i, j, v) =>
                  mata.set(i, j, v)
                  matb.set(i, j, v)
              }

              val rows = mata.nrows
              val cols = matb.ncols

              // set the last and first value to a1 mata and a2 in matb
              mata.set(0, 0, a1)
              mata.set(rows - 1, cols - 1, a1)
              matb.set(0, 0, a2)
              matb.set(rows - 1, cols - 1, a2)
              }
              isEq <- mata.isEq(matb)
            } yield {
            if (a1 == a2) {
                isEq shouldBe true
              } else {
                isEq shouldBe false
              }
            }
          }
        runtime.unsafeRun(io)
    }

    it should s"multiply 2 matrices of ${CT.toString()} on the plusTimes semiring" in forAll {
      m: MatrixTuplesMul[A] =>
        val MatrixTuplesMul(
          MatrixTuples(MatrixDimensions(rows1, cols1), vals1),
          MatrixTuples(MatrixDimensions(rows2, cols2), vals2)
        ) = m

        val mats = for {
          mat1 <- GrBMatrix[A](rows1, cols1)
          mat2 <- GrBMatrix[A](rows2, cols2)
          plus <- GrBMonoid(OP.plus, N.zero)
          semi <- GrBSemiring(plus, OP.times)
          c <- Matrix[GrBMatrix].mxmNew(semi, None, None, None)(mat1, mat2)
        } yield c

        // the usual semiring plus times

        val io = mats.use { c =>
          IO.effect {
            c.nrows shouldBe rows1
            c.ncols shouldBe cols2
            c.nvals //
          }
        }

    }
  }

}
