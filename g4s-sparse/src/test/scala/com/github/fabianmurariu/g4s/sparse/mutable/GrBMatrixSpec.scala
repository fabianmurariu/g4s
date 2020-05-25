package com.github.fabianmurariu.g4s.sparse.mutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.scalacheck.Arbitrary
import com.github.fabianmurariu.unsafe.MatrixTuples
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder
import Matrix.ops._
import scala.reflect.ClassTag

class GrBMatrixSpec extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks with Matchers {
  behavior of "GrBMatrix"

  addBasicMatrixTest[Boolean]
  addBasicMatrixTest[Byte]
  addBasicMatrixTest[Short]
  addBasicMatrixTest[Int]
  addBasicMatrixTest[Long]
  addBasicMatrixTest[Float]
  addBasicMatrixTest[Double]

  def addBasicMatrixTest[A:MatrixBuilder](implicit MT:MatrixHandler[GrBMatrix, A], A:Arbitrary[MatrixTuples[A]], CT:ClassTag[A]) = {

    it should s"create a matrix of ${CT.toString()} set values into it, get values from it then release it" in forAll{ mt:MatrixTuples[A] =>

      val mat = GrBMatrix[A](mt.dim.rows, mt.dim.cols)

      mt.vals.foreach{
        case (i, j, v) =>
          mat.set(i, j, v)
      }

      mt.vals.foreach{
        case (i, j, v) =>
          mat.get(i, j) shouldBe Some(v)
      }

      mat.close() // releases the matrix
    }
  }
}
