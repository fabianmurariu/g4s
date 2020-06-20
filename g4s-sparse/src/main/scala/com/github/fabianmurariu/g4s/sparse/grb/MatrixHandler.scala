package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRAPHBLAS
import scala.{specialized => sp}
import com.github.fabianmurariu.unsafe.GRBCORE
import zio.Task

trait MatrixHandler[M[_], @sp(Boolean, Byte, Short, Int, Long, Float, Double) A] {
  @inline
  def set(m: M[A])(i: Long, j: Long, x: A): Unit
  @inline
  def get(m: M[A])(i: Long, j: Long): Option[A]

  /**
    *
    * Copies the tuples in the Matrix and exports them in 3 arrays
    * (values, I indices, J indices)
    *
    */
  @inline
  def copyData(m: M[A]): (Array[A], Array[Long], Array[Long])

  @inline
  def buildMatrixUnsafe(rows: Long, cols: Long)(
      is: Array[Long],
      js: Array[Long],
      vs: Array[A]
  )(implicit MT: MatrixType[A]): Task[M[A]]

  def show(m: M[A]): String = {
    val (is, js, vs) = copyData(m)

    (0 until is.length).map(i => (is(i), js(i), vs(i))).mkString("M=", ",", "")
  }
}

object MatrixHandler {

  def apply[M[_], A](getFn: (M[A], Long, Long) => Option[A])(
      setFn: (M[A], Long, Long, A) => Unit
  )(copyFb: M[A] => (Array[A], Array[Long], Array[Long]))(
      buildFn: (
          MatrixType[A],
          Long,
          Long,
          Array[Long],
          Array[Long],
          Array[A]
      ) => Task[M[A]]
  ): MatrixHandler[M, A] =
    new MatrixHandler[M, A] {

      override def copyData(m: M[A]): (Array[A], Array[Long], Array[Long]) =
        copyFb(m)

      override def set(f: M[A])(i: Long, j: Long, x: A): Unit =
        setFn(f, i, j, x)

      override def get(f: M[A])(i: Long, j: Long): Option[A] = getFn(f, i, j)

      override def buildMatrixUnsafe(rows: Long, cols: Long)(
          is: Array[Long],
          js: Array[Long],
          vs: Array[A]
      )(implicit MT: MatrixType[A]): Task[M[A]] =
        buildFn(MT, rows, cols, is, js, vs)

    }

  // FIXME: handle errors on calling extractMatrixTuples*

  implicit val matrixHandlerBoolean: MatrixHandler[GrBMatrix, Boolean] =
    MatrixHandler[GrBMatrix, Boolean] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementBoolean(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementBoolean(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Boolean](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesBoolean(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Boolean](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesBoolean(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

  implicit val matrixHandlerByte: MatrixHandler[GrBMatrix, Byte] =
    MatrixHandler[GrBMatrix, Byte] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementByte(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementByte(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Byte](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesByte(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Byte](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesByte(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

  implicit val matrixHandlerShort: MatrixHandler[GrBMatrix, Short] =
    MatrixHandler[GrBMatrix, Short] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementShort(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementShort(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Short](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesShort(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Short](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesShort(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

  implicit val matrixHandlerInt: MatrixHandler[GrBMatrix, Int] =
    MatrixHandler[GrBMatrix, Int] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementInt(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementInt(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Int](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesInt(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Int](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesInt(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

  implicit val matrixHandlerLong: MatrixHandler[GrBMatrix, Long] =
    MatrixHandler[GrBMatrix, Long] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementLong(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementLong(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Long](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesLong(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Long](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesLong(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

  implicit val matrixHandlerFloat: MatrixHandler[GrBMatrix, Float] =
    MatrixHandler[GrBMatrix, Float] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementFloat(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementFloat(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Float](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesFloat(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Float](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesFloat(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

  implicit val matrixHandlerDouble: MatrixHandler[GrBMatrix, Double] =
    MatrixHandler[GrBMatrix, Double] { (f, i, j) =>
      GRAPHBLAS.getMatrixElementDouble(f.pointer, i, j).headOption
    } { (f, i, j, x) => GRAPHBLAS.setMatrixElementDouble(f.pointer, i, j, x) } {
      (f) =>
        val n = GRBCORE.nvalsMatrix(f.pointer)
        val vs = new Array[Double](n.toInt)
        val is = new Array[Long](n.toInt)
        val js = new Array[Long](n.toInt)
        GRAPHBLAS.extractMatrixTuplesDouble(f.pointer, vs, is, js)
        (vs, is, js)
    } { (mt, rows, cols, is, js, vs) =>
      GrBMatrix.unsafe[Double](rows, cols).map { m =>
        assert(
          GRAPHBLAS.buildMatrixFromTuplesDouble(
            m.pointer,
            is,
            js,
            vs,
            is.length,
            BuiltInBinaryOps.boolean.first.pointer
          ) == 0
        )
        m
      }
    }

}
