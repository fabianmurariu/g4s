package com.github.fabianmurariu.g4s.sparse.grbv2

import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import scala.reflect.ClassTag
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Arbitrary
import cats.effect.IO
import cats.effect.Resource
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
class AssignSpec extends munit.ScalaCheckSuite with SuiteUtils {

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

  def extract[T: SparseMatrixHandler: ClassTag: Arbitrary: EqOp] {
    property(
      s"extract top half of the matrix a to matrix b, resize a to b, the result should be equal, ${implicitly[ClassTag[T]]}"
    ) {
      forAll { mt: MatrixTuples[T] =>
        (mt.rows % 2 == 0 && mt.cols > 2) ==> {

          val (is, js, vs) = tuples(mt)
          val (r2, c2) = ((mt.rows / 2).toInt, mt.cols.toInt)
          val io: IO[Unit] = (for {
            a <- Matrix.fromTuples[IO, T](mt.rows, mt.cols)(is, js, vs)
            c <- Matrix[IO, T](r2, c2)
            _ <- Resource.liftF(c.extract(0 until r2, 0 until c2)(from = a))
            _ <- Resource.liftF(a.resize(r2, c2))
            check <- Resource.liftF(c.isEq(a))
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
