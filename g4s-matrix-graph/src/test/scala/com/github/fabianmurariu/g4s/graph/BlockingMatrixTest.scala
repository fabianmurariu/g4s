package com.github.fabianmurariu.g4s.graph

import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import munit.ScalaCheckSuite
import org.scalacheck.Prop._
import com.github.fabianmurariu.g4s.sparse.grbv2.MatrixTuples
import cats.effect.IO
import scala.concurrent.ExecutionContext

class BlockingMatrixTest extends ScalaCheckSuite {

  override val scalaCheckInitialSeed =
    "cKQ9U86ACRkbbSogYKQvZIbnIkiiU2HJ1qpY0AKBrHM="

  implicit val cs = IO.contextShift(ExecutionContext.global)

  property(
    "stream a matrix row by row then re-assemble should equal the original"
  ) {
    forAll { mt: MatrixTuples[Boolean] =>
      val io = testBlock(mt, 1)
      io.unsafeRunSync
    }
  }

  property(
    "stream a matrix with a page size of 2 then re-assemble should equal the original"
  ) {
    forAll { mt: MatrixTuples[Boolean] =>
      val io = testBlock(mt, 2)
      io.unsafeRunSync
    }
  }

  property(
    "stream a matrix with a page size of 3 then re-assemble should equal the original"
  ) {
    forAll { mt: MatrixTuples[Boolean] =>
      val io = testBlock(mt, 3)
      io.unsafeRunSync
    }
  }

  property(
    "stream a matrix with a page size of 5 then re-assemble should equal the original"
  ) {
    forAll { mt: MatrixTuples[Boolean] =>
      val io = testBlock(mt, 5)
      io.unsafeRunSync
    }
  }


  property(
    "stream a matrix with a page size of 100 then re-assemble should equal the original"
  ) {
    forAll { mt: MatrixTuples[Boolean] =>
      val io = testBlock(mt, 100)
      io.unsafeRunSync
    }
  }

  def testBlock(mt: MatrixTuples[Boolean], pageSize: Long) = {
    BlockingMatrix[IO, Boolean](mt.rows, mt.cols).use { bm =>
      for {
        _ <- bm.use(_.set(mt.tuples))
        expected <- bm.use(_.extract)
        actual <- (bm
          .toStream(0, pageSize)
          .reduce { (chunk1, chunk2) =>
            (
              chunk1._1 ++ chunk2._1,
              chunk1._2 ++ chunk2._2,
              chunk1._3 ++ chunk2._3
            )
          }
          .compile
          .lastOrError)
      } yield {
        assertEquals(actual.zipped.toVector, expected.zipped.toVector)
      }
    }

  }
}
