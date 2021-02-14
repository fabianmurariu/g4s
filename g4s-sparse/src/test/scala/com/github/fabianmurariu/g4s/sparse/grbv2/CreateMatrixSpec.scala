package com.github.fabianmurariu.g4s.sparse.grbv2

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

import cats.effect.IO
import cats.implicits._
import com.github.fabianmurariu.g4s.sparse.grb.EqOp
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
import com.github.fabianmurariu.g4s.sparse.grb.Reduce
import com.github.fabianmurariu.g4s.sparse.grb.SparseMatrixHandler
import munit.ScalaCheckSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import scala.concurrent.ExecutionContextExecutor

class CreateMatrixSpec extends ScalaCheckSuite {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def tuples[A: ClassTag](m: MatrixTuples[A]): (Array[Long], Array[Long], Array[A]) = {

    val is = m.tuples.map(_._1).toArray
    val js = m.tuples.map(_._2).toArray
    val vs = m.tuples.map(_._3).toArray

    (is, js, vs)
  }

  def createdProp[
      A: Arbitrary: ClassTag: SparseMatrixHandler: Ordering: Reduce: EqOp
  ]: Unit = {

    property(
      s"resize matrix has different shape ${implicitly[ClassTag[A]]}"
    ) {
      forAll { m: MatrixTuples[A] =>
        val (is, js, vs) = tuples(m)
        val io = GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs).use {
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
          a <- GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
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
          a <- GrBMatrix.fromTuples[IO, A](m.rows, m.cols)(is, js, vs)
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

        val io = GrBMatrix[IO, A](m.rows, m.cols)
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

        val io = GrBMatrix
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
