package com.github.fabianmurariu.g4s.traverser

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.IOSupport
import cats.effect.IO
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async._
import com.github.fabianmurariu.g4s.graph.GrBTuples
import cats.effect.Resource

class GrBTuplesSpec extends IOSupport {

  test("merge 2 matrices with row by row cross join") {
    val res = for {
      a <- GrBMatrix.fromTuples[IO, Boolean](3, 3)(
        Array(0, 0, 1, 2),
        Array(0, 2, 2, 1),
        Array(true, true, true, true)
      )
      b <- GrBMatrix.fromTuples[IO, Boolean](3, 3)(
        Array(0, 0, 1, 2, 2),
        Array(1, 2, 1, 0, 2),
        Array(true, true, true, true, true)
      )
      _ <- Resource.liftF {
        for {
          tplsA <- a.extract.map { case (is, js, _) => new GrBTuples(is, js) }
          tplsb <- b.extract.map { case (is, js, _) => new GrBTuples(is, js) }
          rows <- a.nrows

          _ <- IO.delay {
            val actual = GrBTuples
              .crossRowNodesForMatrix(rows, Vector(tplsA, tplsb))
              .map(_.toVector)
              .toVector

            val expected: Vector[Vector[Long]] = Vector(
              Vector(0, 1),
              Vector(0, 2),
              Vector(2, 1),
              Vector(2, 2),
              Vector(2, 1),
              Vector(1, 0),
              Vector(1, 2)
            )
            assertEquals(actual, expected)
          }
        } yield ()
      }
    } yield (a, b)

    res.use(_ => IO.unit)
  }

  test("merge 3 matrices with row by row cross join") {
    val res = for {
      a <- GrBMatrix.fromTuples[IO, Boolean](3, 3)(
        Array(0, 0, 1, 2),
        Array(0, 2, 2, 1),
        Array(true, true, true, true)
      )
      b <- GrBMatrix.fromTuples[IO, Boolean](3, 3)(
        Array(0, 0, 1, 2, 2),
        Array(1, 2, 1, 0, 2),
        Array(true, true, true, true, true)
      )
      c <- GrBMatrix.fromTuples[IO, Boolean](3, 3)(
        Array(0, 0, 1, 2),
        Array(0, 2, 1, 2),
        Array(true, true, true, true)
      )
      _ <- Resource.liftF {
        for {
          tplsA <- a.extract.map { case (is, js, _) => new GrBTuples(is, js) }
          tplsB <- b.extract.map { case (is, js, _) => new GrBTuples(is, js) }
          tplsC <- c.extract.map { case (is, js, _) => new GrBTuples(is, js) }
          rows <- a.nrows

          _ <- IO.delay {
            val actual = GrBTuples
              .crossRowNodesForMatrix(rows, Vector(tplsA, tplsB, tplsC))
              .map(_.toVector)
              .toVector

            val expected: Vector[Vector[Long]] = Vector(
              // FIRST ROW CROSS JOIN
              Vector(0, 1, 0),
              Vector(0, 1, 2),
              Vector(0, 2, 0),
              Vector(0, 2, 2),
              Vector(2, 1, 0),
              Vector(2, 1, 2),
              Vector(2, 2, 0),
              Vector(2, 2, 2),
              // SECOND ROW CROSS JOIN
              Vector(2, 1, 1),
              // THIRD ROW CROSS JOIN
              Vector(1, 0, 2),
              Vector(1, 2, 2)
            )
            assertEquals(actual, expected)
          }
        } yield ()
      }
    } yield (a, b)

    res.use(_ => IO.unit)
  }
}
