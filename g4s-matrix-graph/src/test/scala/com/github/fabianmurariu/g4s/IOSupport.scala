package com.github.fabianmurariu.g4s

import cats.effect.IO

abstract class IOSupport extends munit.FunSuite {

  implicit val runtime =  cats.effect.unsafe.IORuntime.global

  override def munitValueTransforms: List[ValueTransform] = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[Any] => io.unsafeToFuture()
    })
  )

}
