package com.github.fabianmurariu.g4s.sparse.grbv2

import scala.reflect.ClassTag
import scala.collection.generic.CanBuildFrom
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait SuiteUtils {

  def tuples[A: ClassTag](m: MatrixTuples[A]): (Array[Long], Array[Long], Array[A]) = {

    val is = m.tuples.map(_._1).toArray
    val js = m.tuples.map(_._2).toArray
    val vs = m.tuples.map(_._3).toArray

    (is, js, vs)
  }
}

case class MatrixTuples[A](
    rows: Long,
    cols: Long,
    tuples: Vector[(Long, Long, A)]
)


object MatrixTuples {

  def genVal[T](rows: Long, cols: Long)(g: Gen[T]): Gen[(Long, Long, T)] =
    for {
      i <- Gen.choose(0, rows - 1)
      j <- Gen.choose(0, cols - 1)
      t <- g
    } yield (i, j, t)

  implicit def matrixTuplesArb[T](
      implicit T: Arbitrary[T],
  ): Arbitrary[MatrixTuples[T]] = {

    val gen = for {
      rows <- Gen.posNum[Long]
      cols <- Gen.posNum[Long]
      vals <- Gen
        .nonEmptyContainerOf[Vector, (Long, Long, T)](
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

object MxMSample {

  implicit def mxmSampleArb[T](
      implicit T: Arbitrary[T]
  ): Arbitrary[MxMSample[T]] = {

    def matTuples(rows: Long, cols: Long) =
      for {
        vals <- Gen
          .nonEmptyContainerOf[Vector, (Long, Long, T)](
            MatrixTuples.genVal(rows, cols)(T.arbitrary)
          )
          .map(_.distinctBy(t => t._1 -> t._2))
      } yield MatrixTuples[T](rows, cols, vals)

    val gen = for {
      size <- Gen.posNum[Long]
      a <- matTuples(size, size)
      b <- matTuples(size, size)
      c <- matTuples(size, size)
    } yield MxMSample(size, a, b, c)

    Arbitrary(gen)
  }

}

case class VectorTuples[A](size: Long, tuples: Vector[(Long, A)])

object VectorTuples {

  def genVal[T](size: Long)(g: Gen[T]): Gen[(Long, T)] =
    for {
      i <- Gen.choose(0, size - 1)
      v <- g
    } yield (i, v)

  implicit def vectorTuplesArb[T](
      implicit T: Arbitrary[T],
  ): Arbitrary[VectorTuples[T]] = {

    val gen = for {
      size <- Gen.posNum[Long]
      vals <- Gen
        .nonEmptyContainerOf[Vector, (Long, T)](
          genVal(size)(T.arbitrary)
        )
        .map(_.distinctBy(_._1))
    } yield VectorTuples[T](size, vals)

    Arbitrary(gen)

  }
}
