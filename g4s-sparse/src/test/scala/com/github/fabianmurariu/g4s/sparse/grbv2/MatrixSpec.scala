package com.github.fabianmurariu.g4s.sparse.grbv2

import org.scalacheck._
import cats.Id
import cats.implicits._
import scala.reflect.ClassTag
import com.github.fabianmurariu.g4s.sparse.grb.{MatrixBuilder, SparseMatrixHandler}
import cats.effect.IO
import scala.concurrent.ExecutionContext
import java.util.concurrent.TimeUnit

object MatrixSpec extends Properties("MatrixV2") {

  import Prop.forAll

  val c = IO.timer(ExecutionContext.global).clock


  def createdProp[A:Arbitrary:ClassTag:MatrixBuilder:SparseMatrixHandler] = {
    property("can be created and set to all the values") = forAll { m: MatrixTuples[A] =>
      val io = Matrix[IO, A](m.rows, m.cols).use{
        mat =>

        for {
          start <- c.monotonic(TimeUnit.MILLISECONDS)
          nvals <- mat .set(m.is, m.js, m.vs).flatMap(_.nvals)
          end <- c.monotonic(TimeUnit.MILLISECONDS)
          _ <- IO{
            println(s"${end-start}ms ${m.rows}, ${m.cols}, ${nvals}")
          }
        } yield nvals > 0
      }

      io.unsafeRunSync()
    }

  }

  createdProp[Boolean]
}

case class MatrixTuples[A](rows:Long, cols:Long, is: Vector[Long], js: Vector[Long], vs: Vector[A])

object MatrixTuples {
  implicit def matrixTuplesArb[A](implicit A:Arbitrary[A], CT:ClassTag[A]):Arbitrary[MatrixTuples[A]] = {
    val dimGen = Gen.posNum[Long]
      .map(_ + 1)
      .map(l => Math.min(l, 2^60))

    val gen:Gen[MatrixTuples[A]] = for {
      rows <- dimGen
      cols <- dimGen
      size <- Gen.choose(1, 1000000) // billions?
      is <- Gen.containerOfN[Vector, Long](size, Gen.choose(0, rows-1))
      js <- Gen.containerOfN[Vector, Long](size, Gen.choose(0, cols-1))
      vs <- Gen.containerOfN[Vector, A](size, A.arbitrary)
    } yield MatrixTuples(rows, cols, is, js, vs)
    Arbitrary.apply(gen)
  }
}
