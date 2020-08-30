package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen}
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.{
  SparseMatrixHandler,
  Reduce,
  EqOp
}
import scala.concurrent.ExecutionContext
import cats.effect.{IO, Resource}
import scala.concurrent.Future
import munit.ScalaCheckSuite
import scala.collection.immutable.{Vector => SVector}
import org.scalacheck.Properties
import scala.collection.generic.CanBuildFrom

class CreateMatrixSpec extends ScalaCheckSuite {
  implicit val ec = ExecutionContext.global

  def tuples[A: ClassTag](m: MatrixTuples[A]) = {

    val is = m.tuples.map(_._1).toArray
    val js = m.tuples.map(_._2).toArray
    val vs = m.tuples.map(_._3).toArray

    (is, js, vs)
  }

  def createdProp[
      A: Arbitrary: ClassTag: SparseMatrixHandler: Ordering: Reduce: EqOp
  ] = {

    property(
      s"resize matrix has different shape ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)
        val io = Matrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs).use {
          mat =>
            for {
              _ <- mat.resize(m.rows + 5, m.cols + 5)
              s <- mat.shape
            } yield s
        }
        assertEquals(io.unsafeRunSync(), (m.rows + 5, m.cols + 5))
      }
    }

    property(
      s"duplicated matrix equals itself ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io = for {
          a <- Matrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
          b <- a.duplicateF
        } yield (a, b)

        io.use { case (a, b) => a.isEq(b) }.unsafeRunSync()

      }
    }

    property(
      s"transposed matrix has shape flipped itself ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)
        val io = (for {
          a <- Matrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
          b <- a.transpose()
        } yield (a, b)).use {
          case (a, b) =>
            for {
              init <- a.shape
              trans <- b.shape
            } yield {
              assertEquals(trans, init.swap)
            }
        }

        io.unsafeRunSync()

      }
    }

    property(
      s"can be created and set to all the values ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io = Matrix[IO, A](m.rows, m.cols)
          .use { mat =>
            for {
              _ <- mat.set(is, js, vs)
              nvals <- mat.nvals
              rows <- mat.nrows
              cols <- mat.ncols
              tuples <- mat.extract
            } yield {
              assert(nvals > 0)
              assert(rows == m.rows)
              assert(cols == m.cols)
              val (isA, jsA, vsA) = tuples
              assertEquals(isA.toVector, is.toVector.sorted)
              assertEquals(jsA.toVector.sorted, js.toVector.sorted)
              assertEquals(vsA.toVector.sorted, vs.toVector.sorted)
            }
          }

        io.unsafeRunSync()
      }
    }

    property("can be created with direct arrays") {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)

        val io = Matrix
          .fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
          .use { mat =>
            for {
              nvals <- mat.nvals
              tuples <- mat.extract
            } yield {
              assert(nvals > 0)
              val (isA, jsA, vsA) = tuples
              assertEquals(isA.toVector, is.toVector.sorted)
              assertEquals(jsA.toVector.sorted, js.toVector.sorted)
              assertEquals(vsA.toVector.sorted, vs.toVector.sorted)
            }
          }

        io.unsafeRunSync()
      }
    }

  }

  createdProp[Boolean]
  createdProp[Byte]
  createdProp[Short]
  createdProp[Int]
  createdProp[Long]
  createdProp[Float]
  createdProp[Double]

}

case class MatrixTuples[A](
    rows: Long,
    cols: Long,
    tuples: SVector[(Long, Long, A)]
)

trait WithDistinctBy {

  implicit class DistinctByDecorator[T, F[_] <: TraversableOnce[_]](
      val xs: F[T]
  ) {
    def distinctBy[A](
        f: T => A
    )(implicit CBF: CanBuildFrom[F[T], T, F[T]]): F[T] = {
      val xsf = xs.asInstanceOf[TraversableOnce[T]]
      val seen = scala.collection.mutable.HashSet.empty[A]
      val outBuilder = CBF()
      xsf.foreach[Unit] { t: T =>
        val a = f(t)
        if (!seen(a)) {
          outBuilder += t
          seen += a
        }
      }
      outBuilder.result()
    }
  }
}

object MatrixTuples extends WithDistinctBy {

  def genVal[T](rows: Long, cols: Long)(g: Gen[T]): Gen[(Long, Long, T)] =
    for {
      i <- Gen.choose(0, rows - 1)
      j <- Gen.choose(0, cols - 1)
      t <- g
    } yield (i, j, t)

  implicit def matrixTuplesArb[T](
      implicit T: Arbitrary[T],
      CT: ClassTag[T]
  ): Arbitrary[MatrixTuples[T]] = {

    val gen = for {
      rows <- Gen.posNum[Int]
      cols <- Gen.posNum[Int]
      vals <- Gen
        .nonEmptyContainerOf[SVector, (Long, Long, T)](
          genVal(rows, cols)(T.arbitrary)
        )
        .map(_.distinctBy(t => t._1 -> t._2))
    } yield MatrixTuples[T](rows, cols, vals)

    Arbitrary(gen)

  }
}

case class MxMSample[T](
    size: Long,
    a: MatrixTuples[T],
    b: MatrixTuples[T],
    c: MatrixTuples[T]
)

object MxMSample extends WithDistinctBy {

  implicit def mxmSampleArb[T](
      implicit T: Arbitrary[T]
  ): Arbitrary[MxMSample[T]] = {

    def matTuples(rows: Int, cols: Int) =
      for {
        vals <- Gen
          .nonEmptyContainerOf[SVector, (Long, Long, T)](
            MatrixTuples.genVal(rows, cols)(T.arbitrary)
          )
          .map(_.distinctBy(t => t._1 -> t._2))
      } yield MatrixTuples[T](rows, cols, vals)

    val gen = for {
      size <- Gen.posNum[Int]
      a <- matTuples(size, size)
      b <- matTuples(size, size)
      c <- matTuples(size, size)
    } yield MxMSample(size, a, b, c)

    Arbitrary(gen)
  }

}
