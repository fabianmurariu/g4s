package com.github.fabianmurariu.g4s.sparse.grb

import com.github.fabianmurariu.unsafe.GRBCORE

sealed abstract class GrBError(val code: Long, message: String)
    extends RuntimeException(message) {
  // Try{GRBCORE.error}
}

class GrBUninitializedObject()
    extends GrBError(2, "object has not been initialized")

class GrBInvalidObject()
    extends GrBError(3, "object is corrupted")

class GrBNullPointer()
    extends GrBError(4, "input pointer is NULL")

class GrBInvalidValue()
    extends GrBError(5, "generic error code; some value is bad")

class GrBInvalidIndex()
    extends GrBError(6, "a row or column index is out of bounds")

class GrBDomainMismatch()
    extends GrBError(7, "object domains are not compatible")

class GrBDimensionMismatch()
    extends GrBError(8, "matrix dimensions do not match")

class GrBOutputNotEmpty()
    extends GrBError(9, "output matrix already has values in it")

class GrBOutOfMemory()
    extends GrBError(10, "out of memory")

class GrBInsufficientSpace()
    extends GrBError(11, "output array not large enough")

class GrBIndexOutOfBounds()
    extends GrBError(12, "a row or column index is out of bounds")

class GrBPanic()
    extends GrBError(13, "GRB panic")


class InvalidErrorCode(val badCode:Long) extends GrBError(-1, "Malformed error, check your code")

object GrBError {

  def check(code:Long):Long = code match {
    case 0 | 1 => code
    case err => throw GrBError(err)
  }

  def apply(code:Long) = code match {
    case 2 => new GrBUninitializedObject()
    case 3 => new GrBInvalidObject
    case 4 => new GrBNullPointer
    case 5 => new GrBInvalidValue
    case 6 => new GrBInvalidIndex
    case 7 => new GrBDomainMismatch
    case 8 => new GrBDimensionMismatch
    case 9 => new GrBOutputNotEmpty
    case 10 => new GrBOutOfMemory
    case 11 => new GrBInsufficientSpace
    case 12 => new GrBIndexOutOfBounds
    case 13 => new GrBPanic
    case _ => new InvalidErrorCode(code)
  }
}
