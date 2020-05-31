package com.github.fabianmurariu.g4s.sparse.grb.instances

import com.github.fabianmurariu.g4s.sparse.mutable.Matrix
import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grb.MatrixBuilder

class MatrixInstance extends Matrix[GrBMatrix] with MatrixLikeInstance with ElemWiseInstance with MxMInstance {

  override def make[A](rows: Long, cols: Long)(implicit MB: MatrixBuilder[A]): GrBMatrix[A] =
    GrBMatrix(rows, cols)


}
