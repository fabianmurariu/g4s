package com.github.fabianmurariu.g4s.graph

import java.util.Arrays

class GrBTuples(is: Array[Long], js: Array[Long]) {

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
