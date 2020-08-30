package com.github.fabianmurariu.g4s.sparse.grb


import java.nio.Buffer
import com.github.fabianmurariu.unsafe.GRBCORE
import com.github.fabianmurariu.unsafe.GRAPHBLAS

trait SparseMatrixHandler[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T] {
  def createMatrix(rows: Long, cols: Long): Buffer
  def createMatrixFromTuples(rows:Long, cols:Long)
                  (is: Array[Long], js: Array[Long], vs: Array[T]): Buffer

  def get(mat: Buffer)(i: Long, j: Long): Array[T]

  def set(mat: Buffer)(i: Long, j: Long, t: T): Unit

  def setAll(mat: Buffer)(is: Array[Long], js: Array[Long], ts: Array[T]): Unit = {
    assert(is.length == js.length && js.length == ts.length)
    var i = 0
    while (i < is.length) {
      set(mat)(is(i), js(i), ts(i))
      i+=1
    }
  }

  def remove(mat: Buffer)(i: Long, j: Long): Unit =
    GRBCORE.removeElementMatrix(mat, i, j)

  def extractTuples(mat: Buffer): (Array[Long], Array[Long], Array[T])

}

object SparseMatrixHandler {
  @inline
  def apply[T](implicit sparseMatrixBuilder: SparseMatrixHandler[T]): SparseMatrixHandler[T] = sparseMatrixBuilder

  implicit val booleanMatrixHandler: SparseMatrixHandler[Boolean] = new SparseMatrixHandler[Boolean] {

    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.booleanType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Boolean] = GRAPHBLAS.getMatrixElementBoolean(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Boolean): Unit = GRAPHBLAS.setMatrixElementBoolean(mat, i, j, t)

    override def extractTuples(mat: Buffer): (Array[Long], Array[Long], Array[Boolean]) = {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Boolean](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesBoolean(mat, vs, is, js))
      (is, js, vs)
    }

    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Boolean]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(
        GRAPHBLAS.buildMatrixFromTuplesBoolean(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpBoolean()))
      mat
    }
  }

  implicit val byteMatrixHandler: SparseMatrixHandler[Byte] = new SparseMatrixHandler[Byte] {
    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.byteType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Byte] = GRAPHBLAS.getMatrixElementByte(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Byte): Unit = GrBError.check(GRAPHBLAS.setMatrixElementByte(mat, i, j, t))

    override def extractTuples(mat: Buffer) = {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Byte](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesByte(mat, vs, is, js))
      (is, js, vs)
    }

    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Byte]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(GRAPHBLAS.buildMatrixFromTuplesByte(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpByte()))
      mat
    }

  }

  implicit val shortMatrixHandler: SparseMatrixHandler[Short] = new SparseMatrixHandler[Short] {
    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.shortType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Short] = GRAPHBLAS.getMatrixElementShort(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Short): Unit = GrBError.check(GRAPHBLAS.setMatrixElementShort(mat, i, j, t))

    override def extractTuples(mat: Buffer)= {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Short](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesShort(mat, vs, is, js))
      (is, js, vs)
    }


    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Short]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(GRAPHBLAS.buildMatrixFromTuplesShort(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpShort()))
      mat
    }
  }

  implicit val intMatrixHandler: SparseMatrixHandler[Int] = new SparseMatrixHandler[Int] {
    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.intType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Int] = GRAPHBLAS.getMatrixElementInt(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Int): Unit = GrBError.check(GRAPHBLAS.setMatrixElementInt(mat, i, j, t))

    override def extractTuples(mat: Buffer)= {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Int](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesInt(mat, vs, is, js))
      (is, js, vs)
    }


    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Int]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(GRAPHBLAS.buildMatrixFromTuplesInt(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpInt()))
      mat
    }
  }

  implicit val longMatrixHandler: SparseMatrixHandler[Long] = new SparseMatrixHandler[Long] {
    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.longType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Long] = GRAPHBLAS.getMatrixElementLong(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Long): Unit = GrBError.check(GRAPHBLAS.setMatrixElementLong(mat, i, j, t))

    override def extractTuples(mat: Buffer)= {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Long](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesLong(mat, vs, is, js))
      (is, js, vs)
    }


    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Long]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(GRAPHBLAS.buildMatrixFromTuplesLong(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpLong()))
      mat
    }
  }

  implicit val floatMatrixHandler: SparseMatrixHandler[Float] = new SparseMatrixHandler[Float] {
    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.floatType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Float] = GRAPHBLAS.getMatrixElementFloat(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Float): Unit = GrBError.check(GRAPHBLAS.setMatrixElementFloat(mat, i, j, t))

    override def extractTuples(mat: Buffer)= {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Float](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesFloat(mat, vs, is, js))
      (is, js, vs)
    }


    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Float]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(GRAPHBLAS.buildMatrixFromTuplesFloat(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpFloat()))
      mat
    }
  }

  implicit val doubleMatrixHandler: SparseMatrixHandler[Double] = new SparseMatrixHandler[Double] {
    def createMatrix(rows: Long, cols: Long): Buffer = GRBCORE.createMatrix(GRAPHBLAS.doubleType(), rows, cols)

    def get(mat: Buffer)(i: Long, j: Long): Array[Double] = GRAPHBLAS.getMatrixElementDouble(mat, i, j)

    def set(mat: Buffer)(i: Long, j: Long, t: Double): Unit = GrBError.check(GRAPHBLAS.setMatrixElementDouble(mat, i, j, t))

    override def extractTuples(mat: Buffer)= {
      val nvals = GRBCORE.nvalsMatrix(mat)
      val vs = new Array[Double](nvals.toInt)
      val is = new Array[Long](nvals.toInt)
      val js = new Array[Long](nvals.toInt)
      GrBError.check(GRAPHBLAS.extractMatrixTuplesDouble(mat, vs, is, js))
      (is, js, vs)
    }


    override def createMatrixFromTuples(rows:Long, cols:Long)
                             (is: Array[Long], js: Array[Long], vs: Array[Double]): Buffer = {
      val nvals = is.length
      assert(is.length == js.length && js.length == vs.length)
      val mat:Buffer = createMatrix(rows, cols)
      GrBError.check(GRAPHBLAS.buildMatrixFromTuplesDouble(mat, is, js, vs, nvals, GRAPHBLAS.firstBinaryOpDouble()))
      mat
    }
  }
}
