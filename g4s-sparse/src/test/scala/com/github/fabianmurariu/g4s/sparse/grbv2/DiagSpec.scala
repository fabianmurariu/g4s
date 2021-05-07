package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck.Prop._
import org.scalacheck.Arbitrary
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import scala.concurrent.ExecutionContext
import munit.ScalaCheckSuite
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps
import com.github.fabianmurariu.g4s.sparse.grb.SparseVectorHandler
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import cats.effect.IO
import org.scalacheck
import scala.concurrent.ExecutionContextExecutor
import com.github.fabianmurariu.g4s.sparse.grb.Reduce

class DiagSpec extends ScalaCheckSuite with SuiteUtils {

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  override def scalaCheckTestParameters: scalacheck.Test.Parameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(50)
      .withMaxDiscardRatio(10)

  def diagProps[T: ClassTag: Reduce](
      implicit A: Arbitrary[VectorTuples[T]],
      SVH: SparseVectorHandler[T],
      SMH: SparseMatrixHandler[T],
      OP: BuiltInBinaryOps[T]
  ): Unit = {

    property(
      s"set vector to diagonal of matrix ${implicitly[ClassTag[T]]}"
    ) {
      forAll { v: VectorTuples[T] =>
        val is = v.tuples.map(_._1)
        val vs = v.tuples.map(_._2)
        val res = for {
          grbVec <- GrBVector[IO, T](v.size)
          grbMat <- GrBMatrix[IO, T](v.size, v.size)
        } yield (grbVec, grbMat)

        val io = res.use {
          case (vec, mat) =>
            for {
              _ <- vec.pointer.map { p =>
                SVH.setAll(p.ref)(is.toArray, vs.toArray)
              }
              _ <- Diag[IO].diag[T](mat)(vec)
              matTuples <- mat.extract
              _ <- IO.delay {
                val diagonal = matTuples
                  .zipped
                  .collect{case (i, j, _) if i == j => (i, j)}

                val actualIs = diagonal.map(_._1).toVector
                assertEquals(actualIs, is.sorted) // check with extract

              }
              res <- mat.reduceRows(OP.any).use(_.extract)
            } yield {
              assertEquals(res._1.toVector, is.sorted) // check after reduce
              assertEquals(res._2.toSet, vs.toSet)
            }
        }

        io.unsafeRunSync()
      }
    }
  }

  diagProps[Boolean]
  diagProps[Byte]
  diagProps[Short]
  diagProps[Int]
  diagProps[Long]
  diagProps[Float]
  diagProps[Double]
}
