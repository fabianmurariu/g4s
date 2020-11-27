package com.github.fabianmurariu.g4s.sparse.grbv2
import cats.syntax.apply._
import cats.effect.IO
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps.boolean._
import com.github.fabianmurariu.g4s.sparse.grb.BuiltInBinaryOps.float
import com.github.fabianmurariu.g4s.sparse.grb.MxM
import cats.Monad
import cats.effect.concurrent.Ref
import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb

class DocSpec extends munit.FunSuite {

  // graph
  val (is, js, vs) = (
    Array[Long](0, 0, 1, 1, 2, 3, 3, 4, 5, 6, 6, 6),
    Array[Long](2, 3, 4, 6, 5, 0, 2, 5, 2, 2, 3, 4),
    Array.fill(12)(true)
  )

  // weighted graph
  val (isW, jsW, vsW) = (
    Array[Long](0, 0, 1, 1, 2, 3, 3, 4, 5, 6, 6, 6),
    Array[Long](1, 3, 4, 6, 5, 0, 2, 5, 2, 2, 3, 4),
    Array[Float](0.3f, 0.8f, 0.1f, 0.7f, 0.5f, 0.2f, 0.4f, 0.1f, 0.5f, 0.1f,
      0.5f, 0.9f)
  )

  // can be safely reused

  test("out neighbours") {
    val start = 0L

    val setup = (
      GrBMatrix.fromTuples[IO, Boolean](7, 7)(is, js, vs), // edges
      GrBMatrix[IO, Boolean](1, 7), // frontier
      GrBSemiring[IO, Boolean, Boolean, Boolean](lor, land, false)
    ).tupled

    val work = setup.use {
      case (graph, frontier, semiring) =>
        for {
          _ <- frontier.set(0L, start, true)
          _ <- MxM[IO].mxm(frontier)(frontier, graph)(semiring)
          neighbours <- frontier.extract
        } yield neighbours
    }

    val (_, neighbours, _) = work.unsafeRunSync()

    assertEquals(neighbours.toVector, Vector(2L, 3L))

  }

  test("single source shortest path") {
    val start = 0L

    val setup = (
      GrBMatrix.fromTuples[IO, Float](7, 7)(isW, jsW, vsW), // edges
      GrBMatrix[IO, Float](1, 7),
      GrBMatrix[IO, Float](1, 7),
      GrBSemiring[IO, Float, Float, Float](float.min, float.plus, Float.PositiveInfinity)
    ).tupled

    val work = setup.use {
      case (graph, d0, d1, semiring) =>
        // set diagonal to 0.0
        for {
          rows <- graph.nrows
          //starting point
          _ <- d0.set(start, start, 0.0f)
          // set diag to 0.0
          _ <- Monad[IO].iterateUntilM(0L){i => graph.set(i, i, 0.0f).map(_ => i+1)}(_ >= 7)
          i <- Ref.of[IO, Int](0)
          dtmp <- Ref.of[IO, GrBMatrix[IO, Float]](d0)
          d <- Ref.of[IO, GrBMatrix[IO, Float]](d1)
          _ <- Monad[IO].untilM_{
            for {
              d0x <- dtmp.get
              d1x <- d.get
              _ <- MxM[IO].mxm(d1x)(d0x, graph)(semiring)
              // revert
              _ <- dtmp.set(d1x)
              _ <- d.set(d0x)
              _ <- i.update(_ + 1)
            } yield ()
          }(for {
              d0x <- dtmp.get
              d1x <- d.get
              done <- i.get.map(_ >= rows)
              stalling <- d0x.isEq(d1x)
            } yield stalling || done)

          out <- d0.extract
        } yield out
    }

    val (_, js, vs) = work.unsafeRunSync()
    assertEquals(js.toVector, Vector[Long](0, 1, 2, 3, 4, 5, 6))
    assertEquals(vs.toVector, Vector[Float](0.0f, 0.3f, 1.0f, 0.8f, 0.4f, 0.5f, 1.0f))
  }
}
