package com.github.fabianmurariu.g4s.sparse.grbv2

import cats.effect.IO

class DescriptorSpec extends munit.FunSuite {

  test("set field + values that work for Default ") {
    val io = Descriptor[IO].use { desc =>
      for {
        _ <- desc.set[Output, Default]
        _ <- desc.set[Input0, Default]
        _ <- desc.set[Input1, Default]
        _ <- desc.set[Mask, Default]
        d1 <- desc.get[Output, Default]
        d2 <- desc.get[Input0, Default]
        d3 <- desc.get[Input1, Default]
        d4 <- desc.get[Mask, Default]
      } yield {
        assertEquals(d1, Option(Default()))
        assertEquals(d2, Option(Default()))
        assertEquals(d3, Option(Default()))
        assertEquals(d4, Option(Default()))
      }
    }

    io.unsafeRunSync()
  }

  test("set field + values that work for Transpose  ") {
    val io = Descriptor[IO].use { desc =>
      for {
        d1None <- desc.get[Input0, Transpose]
        d2None <- desc.get[Input1, Transpose]
        _ <- desc.set[Input0, Transpose]
        _ <- desc.set[Input1, Transpose]
        d1 <- desc.get[Input0, Transpose]
        d2 <- desc.get[Input1, Transpose]
      } yield {
        assertEquals(d1, Option(Transpose()))
        assertEquals(d1, Option(Transpose()))
        assertEquals(d2None, None)
        assertEquals(d2None, None)
      }
    }

    io.unsafeRunSync()
  }

  test("set field + values that work for Replace  ") {
    val io = Descriptor[IO].use { desc =>
      for {
        d1None <- desc.get[Output, Replace]
        _ <- desc.set[Output, Replace]
        d1 <- desc.get[Output, Replace]
      } yield {
        assertEquals(d1, Option(Replace()))
        assertEquals(d1None, None)
      }
    }

    io.unsafeRunSync()
  }

  test("set field + values that work for Structure, Complement  ") {
    val io = Descriptor[IO].use { desc =>
      for {
        d1None <- desc.get[Mask, Structure]
        d2None <- desc.get[Mask, Complement]
        _ <- desc.set[Mask, Complement]
        d2 <- desc.get[Mask, Complement]
        _ <- desc.set[Mask, Structure]
        d1 <- desc.get[Mask, Structure]
      } yield {
        // assertEquals(d1, Option(Structure())) //FIXME: apparently I can't overwrite a field?
        // assertEquals(d1None, None) // default is replace
        assertEquals(d2, Option(Complement()))
        assertEquals(d2None, None) // default is replace
      }
    }

    io.unsafeRunSync()
  }
}
