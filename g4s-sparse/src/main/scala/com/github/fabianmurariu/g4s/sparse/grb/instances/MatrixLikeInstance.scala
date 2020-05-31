package com.github.fabianmurariu.g4s.sparse.grb.instances

import com.github.fabianmurariu.g4s.sparse.mutable.MatrixLike
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.unsafe.GRBCORE

trait MatrixLikeInstance extends MatrixLike[GrBMatrix] {

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
