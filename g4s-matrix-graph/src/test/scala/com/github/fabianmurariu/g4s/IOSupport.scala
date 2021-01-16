package com.github.fabianmurariu.g4s

import cats.effect.{ContextShift, IO}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

abstract class IOSupport extends munit.FunSuite {

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[Any] => io.unsafeToFuture()
    })
  )

}
