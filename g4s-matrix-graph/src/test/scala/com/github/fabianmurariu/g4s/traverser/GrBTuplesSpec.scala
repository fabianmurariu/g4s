package com.github.fabianmurariu.g4s.traverser

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.IOSupport
import cats.effect.IO
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async._
import com.github.fabianmurariu.g4s.graph.GrBTuples
import cats.effect.Resource
import scala.collection.mutable.ArrayBuffer

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

  test("sort join merge of 2 GrBTuples with 0 rows") {
    val left = new GrBTuples(Array.empty, Array.empty)
    val right = new GrBTuples(Array.empty, Array.empty)
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(res, Seq.empty)
  }

  test("sort join merge of 2 GrBTuples with 1 rows not equals means empty out") {
    val left = new GrBTuples(Array(1), Array.empty)
    val right = new GrBTuples(Array(2), Array.empty)
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(res, Seq.empty)
  }

  test("sort join merge of 2 GrBTuples with 1 rows equals means 1 row out") {
    val left = new GrBTuples(Array(1), Array(0))
    val right = new GrBTuples(Array(1), Array(2))
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(res, Seq(ArrayBuffer[Long](1, 0, 2)))
  }

  test(
    "sort join merge of 2 GrBTuples with 2 rows 1 equals one not means first out"
  ) {
    val left = new GrBTuples(Array(1, 5), Array(0, 3))
    val right = new GrBTuples(Array(1), Array(2))
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(res, Seq(ArrayBuffer[Long](1, 0, 2)))
  }

  test(
    "sort join merge of 2 GrBTuples with 2 rows 1 equals one not means second out"
  ) {
    val left = new GrBTuples(Array(1, 5), Array(0, 3))
    val right = new GrBTuples(Array(5), Array(2))
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(res, Seq(ArrayBuffer[Long](5, 3, 2)))
  }

  test("sort join merge of 2 GrBTuples with 2 rows 2 equals means 2 out") {
    val left = new GrBTuples(Array(2, 4, 3), Array(0, 3, 1))
    val right = new GrBTuples(Array(2, 4), Array(7, 8))
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(
      res,
      Seq(
        ArrayBuffer[Long](2, 0, 7),
        ArrayBuffer[Long](4, 3, 8)
      )
    )
  }

  test("sort join merge of 2 GrBTuples with 3 rows 3 equals means 3 out") {
    val left = new GrBTuples(Array(2, 4, 3), Array(0, 3, 1))
    val right = new GrBTuples(Array(2, 2, 4), Array(7, 8, 9))
    val res = GrBTuples.rowInnerMergeJoin(left, right)
    assertEquals(
      res,
      Seq(
        ArrayBuffer[Long](2, 0, 7),
        ArrayBuffer[Long](2, 0, 8),
        ArrayBuffer[Long](4, 3, 9)
      )
    )
  }

  test("index join 2 GrBTuples non sorted, 0 rows") {
    val left = new GrBTuples(Array.empty, Array.empty)
    val right = new GrBTuples(Array.empty, Array.empty)
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(res, Seq.empty)
  }

  test("index join 2 GrBTuples non sorted, 1 row each no match") {
    val left = new GrBTuples(Array(1), Array(1))
    val right = new GrBTuples(Array(2), Array(1))
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(res, Seq.empty)
  }

  test("index joind 2 GrBTuples non sorted, 1 row each 1 match") {
    val left = new GrBTuples(Array(0), Array(1))
    val right = new GrBTuples(Array(1), Array(2))
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(res, Seq(ArrayBuffer[Long](0, 1, 2)))
  }

  test(
    "index join of 2 GrBTuples with 2 rows 1 equals one not means second out"
  ) {
    val left = new GrBTuples(Array(3, 5), Array(0, 1))
    val right = new GrBTuples(Array(1), Array(2))
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(res, Seq(ArrayBuffer[Long](5, 1, 2)))
  }

  test(
    "index join of 2 GrBTuples with 2 rows 1 equals one not means first out"
  ) {
    val left = new GrBTuples(Array(1, 5), Array(0, 3))
    val right = new GrBTuples(Array(0), Array(2))
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(res, Seq(ArrayBuffer[Long](1, 0, 2)))
  }

  test("index join of 2 GrBTuples with 2 rows 2 equals means 2 out") {
    val left = new GrBTuples(Array(0, 1, 3), Array(2, 4, 3))
    val right = new GrBTuples(Array(2, 4), Array(7, 8))
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(
      res,
      Seq(
        ArrayBuffer[Long](0, 2, 7),
        ArrayBuffer[Long](1, 4, 8)
      )
    )
  }
  test("index join of 2 GrBTuples 7 equals means 7 out") {
    val left = new GrBTuples(Array(0, 1, 3), Array(4, 2, 3))
    val right = new GrBTuples(Array(2, 2, 2, 2, 2, 2, 4), Array(1, 2, 3, 4, 5, 6, 7))
    val res = GrBTuples.rowJoinOnBinarySearch(left.asRows, 1, right)
    assertEquals(
      res,
      Seq(
        ArrayBuffer[Long](0, 4, 7),
        ArrayBuffer[Long](1, 2, 1),
        ArrayBuffer[Long](1, 2, 2),
        ArrayBuffer[Long](1, 2, 3),
        ArrayBuffer[Long](1, 2, 4),
        ArrayBuffer[Long](1, 2, 5),
        ArrayBuffer[Long](1, 2, 6)
      )
    )
  }
}
