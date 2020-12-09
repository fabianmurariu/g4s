package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import scala.reflect.ClassTag
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Arbitrary
import cats.effect.IO
import cats.effect.Resource
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

class AssignSpec extends munit.ScalaCheckSuite with SuiteUtils {

  property("extract top row of matrix") {
    forAll { mt: MatrixTuples[Boolean] =>
      val (is, js, vs) = tuples(mt)
      val io = GrBMatrix
        .fromTuples[IO, Boolean](mt.rows, mt.cols)(is, js, vs)
        .use { mat =>
          GrBMatrix[IO, Boolean](1, mt.cols).use { into =>
            val sel = mat(0L until 1L, 0L until mt.cols)
            (for {
                      m_shape <- into.shape
                      sel_shap <- sel.show()
                    } yield println(s"set $sel_shap into $m_shape")) *> into .set(sel).flatMap(_.extract)
          }

        }

        io.unsafeRunSync()
        true
    }
  }
  // test("4x4 matrix, select the upper half") {

  //   val mt = MatrixTuples[Boolean](
  //     4,
  //     4,
  //     Vector(
  //       (0, 0, true),
  //       (1, 1, true),
  //       (2, 2, true),
  //       (3, 3, true)
  //     )
  //   )

  //   val (is, js, vs) = tuples(mt)

  //   val io: IO[Unit] = (for {
  //     a <- Matrix.fromTuples[IO, Boolean](mt.rows, mt.cols)(is, js, vs)
  //     c <- Matrix[IO, Boolean](2, 4)
  //     _ <- Resource.liftF(c.extract(0 until 1, 0 until 3)(from = a))
  //     _ <- Resource.liftF(a.resize(2, 4))
  //     check <- Resource.liftF(c.isEq(a))
  //   } yield check).use(c =>
  //     IO {
  //       assertEquals(c, true)
  //     }
  //   )

  //   io.unsafeRunSync()
  // }

  extract[Boolean]
  extract[Byte]
  extract[Short]
  extract[Int]
  extract[Long]
  extract[Float]
  extract[Double]

  def extract[T: SparseMatrixHandler: ClassTag: Arbitrary: EqOp] = {
    property(
      s"extract top half of the matrix a to matrix b, resize a to b, the result should be equal, ${implicitly[ClassTag[T]]}"
    ) {
      forAll { mt: MatrixTuples[T] =>
        (mt.rows % 2 == 0 && mt.cols > 2) ==> {

          val (is, js, vs) = tuples(mt)
          val (r2, c2) = ((mt.rows / 2).toLong, mt.cols.toLong)
          val io: IO[Unit] = (for {
            a <- GrBMatrix.fromTuples[IO, T](mt.rows, mt.cols)(is, js, vs)
            b <- GrBMatrix[IO, T](r2, c2)
            _ <- Resource.liftF(
              b.set(a(0L until r2, 0L until c2))
            ) // b = A(0:r2, 0:c2)
            _ <- Resource.liftF(a.resize(r2, c2))
            check <- Resource.liftF(b.isEq(a))
          } yield check).use(c =>
            IO {
              assertEquals(c, true)
            }
          )

          io.unsafeRunSync()
        }
      }
    }

    property(
      s"set top half of matrix b to a, resize matrix b to a, the result should be equal, ${implicitly[ClassTag[T]]}"
    ) {
      forAll { mt: MatrixTuples[T] =>
        (mt.rows % 2 == 0 && mt.cols > 2) ==> {

          val (is, js, vs) = tuples(mt)
          val (r2, c2) = ((mt.rows * 2).toLong, mt.cols.toLong)
          val io: IO[Unit] = (for {
            b <- GrBMatrix.fromTuples[IO, T](mt.rows, mt.cols)(is, js, vs)
            a <- GrBMatrix[IO, T](r2, c2)
            _ <- Resource.liftF(
              a(0 until mt.rows.toInt, 0 until mt.cols.toInt).set(b)
            ) // b(0:r, 0:c)= A
            _ <- Resource.liftF(a.resize(mt.rows, mt.cols))
            check <- Resource.liftF(b.isEq(a))
          } yield check).use(c =>
            IO {
              assertEquals(c, true)
            }
          )

          io.unsafeRunSync()
        }
      }
    }

  }
}
