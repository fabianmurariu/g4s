package com.github.fabianmurariu.g4s.sparse.grbv2

import scala.reflect.ClassTag
import scala.collection.generic.CanBuildFrom
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait SuiteUtils {

  def tuples[A: ClassTag](m: MatrixTuples[A]) = {

    val is = m.tuples.map(_._1).toArray
    val js = m.tuples.map(_._2).toArray
    val vs = m.tuples.map(_._3).toArray

    (is, js, vs)
  }
}

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

case class MatrixTuples[A](
    rows: Long,
    cols: Long,
    tuples: Vector[(Long, Long, A)]
)


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

object MxMSample extends WithDistinctBy {

  implicit def mxmSampleArb[T](
      implicit T: Arbitrary[T]
  ): Arbitrary[MxMSample[T]] = {

    def matTuples(rows: Int, cols: Int) =
      for {
        vals <- Gen
          .nonEmptyContainerOf[Vector, (Long, Long, T)](
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
