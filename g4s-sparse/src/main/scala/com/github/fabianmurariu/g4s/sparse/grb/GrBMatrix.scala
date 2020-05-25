package com.github.fabianmurariu.g4s.sparse.grb

import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.mutable.MatrixHandler
import com.github.fabianmurariu.unsafe.GRAPHBLAS

sealed class GrBMatrix[T](private[grb] val pointer: Buffer)
    extends AutoCloseable {

      def close(): Unit = {
        GRBCORE.freeMatrix(this.pointer)
      }

      override def finalize(): Unit = {
        this.close()
      }
}

object GrBMatrix {
  @inline def apply[A: MatrixBuilder](rows: Long, cols: Long): GrBMatrix[A] = {
    // ensure the GRB subsytem is init
    grb.GRB
    new GrBMatrix[A](pointer = MatrixBuilder[A].build(rows, cols))
  }

  implicit val matrix:Matrix[GrBMatrix] = new Matrix[GrBMatrix] {

    override def nvals[A](f: GrBMatrix[A]): Long = {
      GRBCORE.nvalsMatrix(f.pointer)
    }

    override def nrows[A](f: GrBMatrix[A]): Long = {
      GRBCORE.nrows(f.pointer)
    }

    override def ncols[A](f: GrBMatrix[A]): Long = {
      GRBCORE.ncols(f.pointer)
    }

    override def clear[A](f: GrBMatrix[A]): Unit = {
      GRBCORE.clearMatrix(f.pointer)
    }

    override def duplicate[A](f: GrBMatrix[A]): GrBMatrix[A] = {
      new GrBMatrix[A](pointer = GRBCORE.dupMatrix(f.pointer))
    }

    override def resize[A](f: GrBMatrix[A])(rows: Long, cols: Long): Unit = {
      GRBCORE.resizeMatrix(f.pointer, rows, cols)
    }

    override def release[A](f: GrBMatrix[A]): Unit = {
      f.close()
    }
  }


  implicit val matrixHandlerBoolean:MatrixHandler[GrBMatrix, Boolean] =
    MatrixHandler[GrBMatrix, Boolean]
      {(f, i, j) => GRAPHBLAS.getMatrixElementBoolean(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementBoolean(f.pointer, i, j, x)}

  implicit val matrixHandlerByte:MatrixHandler[GrBMatrix, Byte] =
    MatrixHandler[GrBMatrix, Byte]
      {(f, i, j) => GRAPHBLAS.getMatrixElementByte(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementByte(f.pointer, i, j, x)}

  implicit val matrixHandlerShort:MatrixHandler[GrBMatrix, Short] =
    MatrixHandler[GrBMatrix, Short]
      {(f, i, j) => GRAPHBLAS.getMatrixElementShort(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementShort(f.pointer, i, j, x)}

  implicit val matrixHandlerInt:MatrixHandler[GrBMatrix, Int] =
    MatrixHandler[GrBMatrix, Int]
      {(f, i, j) => GRAPHBLAS.getMatrixElementInt(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementInt(f.pointer, i, j, x)}

  implicit val matrixHandlerLong:MatrixHandler[GrBMatrix, Long] =
    MatrixHandler[GrBMatrix, Long]
      {(f, i, j) => GRAPHBLAS.getMatrixElementLong(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementLong(f.pointer, i, j, x)}

  implicit val matrixHandlerFloat:MatrixHandler[GrBMatrix, Float] =
    MatrixHandler[GrBMatrix, Float]
      {(f, i, j) => GRAPHBLAS.getMatrixElementFloat(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementFloat(f.pointer, i, j, x)}

  implicit val matrixHandlerDouble:MatrixHandler[GrBMatrix, Double] =
    MatrixHandler[GrBMatrix, Double]
      {(f, i, j) => GRAPHBLAS.getMatrixElementDouble(f.pointer, i, j).headOption}
      {(f, i, j, x) => GRAPHBLAS.setMatrixElementDouble(f.pointer, i, j, x)}
}
