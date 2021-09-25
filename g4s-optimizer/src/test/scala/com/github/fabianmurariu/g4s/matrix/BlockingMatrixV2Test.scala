package com.github.fabianmurariu.g4s.matrix

import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import org.scalacheck.Prop._
import com.github.fabianmurariu.g4s.sparse.grbv2.MatrixTuples
import cats.implicits._
import cats.effect.unsafe.IORuntime
class BlockingMatrixV2Test extends munit.ScalaCheckSuite {

  implicit val runtime = cats.effect.unsafe.IORuntime.global

  property(
    "stream a matrix row by row then re-assemble should equal the original"
  ) {
    forAll { mt: MatrixTuples[Boolean] =>
      val io = BlockingMatrixV2[Boolean](mt.rows, mt.cols).use { mat =>
        mt.tuples.foldLeftM(mat) {
          case (mat, (i, j, v)) =>
            mat.set(Array(i), Array(j), Array(v)).map(_ => mat)
        }

      }
      io.unsafeRunSync()(IORuntime.global)

      true
    }
  }
}
