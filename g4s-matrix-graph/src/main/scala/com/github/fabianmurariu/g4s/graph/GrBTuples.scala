package com.github.fabianmurariu.g4s.graph

import java.util.Arrays
import scala.collection.mutable.ArrayBuffer
import cats.implicits._
import cats.Functor

class GrBTuples(val is: Array[Long], val js: Array[Long]) {

  def show: String = s"{is: ${is.toVector}, js: ${js.toVector}}"

  def row(i: Long): Iterator[Long] = {
    val index = Arrays.binarySearch(is, i)
    var first = index
    var last = index
    if (index >= 0) {
      while (first > 0 && is(first - 1) == i)
        first -= 1
      while (last < is.length - 1 && is(last + 1) == i)
        last += 1

      new GrBTuplesRowIterator(first, last, js)
    } else Iterator.empty

  }

  def asRows: Seq[ArrayBuffer[Long]] = 
    (is, js).zipped.map{case (i, j) => ArrayBuffer(i, j)}

}

object GrBTuples {

  /**
    *  This is used before the Scan operation
    * to translate from the GrBMatrix output into
    * indices that can be scanned against the
    * graph data store
    *
    * 1. take each row of every matrix
    * 2. do a cross product between every column value for that row on each matrix
    * 3. return as an Array of Long
    * */
  @deprecated("Turns out it was not needed", "0.2")
  def crossRowNodesForMatrix(
      rows: Long,
      returns: Vector[GrBTuples]
  ): Iterator[Array[Long]] = {
    def joinReturnsPerRow(
        returns: Vector[GrBTuples]
    )(i: Long): Iterator[Array[Long]] = returns match {
      case first +: IndexedSeq() =>
        first.row(i).map(Array(_))
      case first +: rest =>
        first
          .row(i)
          .flatMap(x =>
            joinReturnsPerRow(rest)(i)
              .map(y => Array(x) ++ y)
          )
    }

    (0L until rows).map(joinReturnsPerRow(returns)).reduce(_ ++ _)

  }

  /**
    * 1. left.is and right.is are on the same domain
    * 2. left.is and right.is are assumed sorted
    * 3. output is i if left.is == right.is, left.js, right.js
    * 4. we assume the originating matrices had same dimensions
    * */
  def rowInnerMergeJoin(
      left: GrBTuples,
      right: GrBTuples
  ): Seq[ArrayBuffer[Long]] = {

    val out = Seq.newBuilder[ArrayBuffer[Long]]

    val l = left.is
    val r = right.is

    if (l.isEmpty || r.isEmpty) Seq.empty
    else {

      var mark = -1
      var c0 = 0
      var c1 = 0

      @inline def safe: Boolean = c0 < l.length && c1 < r.length

      do {
        if (mark < 0) {
          while (safe && l(c0) < r(c1)) { c0 += 1 }
          while (safe && l(c0) > r(c1)) { c1 += 1 }
          mark = c1
        }

        if (safe) {

          if (l(c0) == r(c1)) {
            out += ArrayBuffer(l(c0), left.js(c0), right.js(c1))
            c1 += 1
          } else {
            c1 = mark
            c0 += 1
            mark = -1
          }
        }

      } while (safe)
    }

    out.result()
  }

  def rowJoinOnBinarySearch(left: Seq[ArrayBuffer[Long]], leftColumn: Int, right: GrBTuples): Seq[ArrayBuffer[Long]] = {

    left.flatMap{
      row => 
        val key = row(leftColumn)
        val is = right.is
        val js = right.js

        var i = Arrays.binarySearch(is, key)

        if (i >= 0) {

          // move back to the first value
          while (i > 0 && is(i - 1) == key) {
            i-=1
          }

          val buf = Seq.newBuilder[ArrayBuffer[Long]]

          // grab all the tuples from right
          // that match the key
          while (i < is.length && is(i) == key) {
            buf += (row :+ js(i))
            i+=1
          }

          buf.result()
        } else Seq.empty
    }

  }

  @inline
  def fromGrBExtract[F[_]:Functor, T](extract: F[(Array[Long], Array[Long], Array[T])]) =
    extract.map{case (is, js, _) => new GrBTuples(is, js)}
}

class GrBTuplesRowIterator(var cur: Int, end: Int, js: Array[Long])
    extends Iterator[Long] {

  override def hasNext: Boolean = cur <= end

  override def next(): Long = {
    val out = js(cur)
    cur += 1
    out
  }

}
