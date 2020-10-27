# Matrices as graphs

To install my project
```scala
libraryDependencies += "com.fabianmurariu" % "g4s" % "0.1.0-SNAPSHOT"
```
## Create a matrix and set 2 items
```scala mdoc
import com.github.fabianmurariu.g4s.sparse.grbv2._
import com.github.fabianmurariu.g4s.sparse.grb._
import cats.effect._

// a long typed empty matrix is created as a Resource to ensure after use it is freed
val mat = Matrix[IO, Long](4,4)(Sync[IO], SparseMatrixHandler[Long]).use {
m => 
 for {
  _ <- m.set(0, 0, 0L)
  _ <- m.set(1, 1, 1L)
  t <- m.show()
 } yield t
}
mat.unsafeRunSync

```

## The main purpose of this library is to be used as a Graph

```scala mdoc

import com.github.fabianmurariu.g4s.sparse.grbv2._
import com.github.fabianmurariu.g4s.sparse.grb._
import cats.effect._
import BuiltInBinaryOps._

// Graph where every (src, dst, true) triplet is an edge

type DGraph = Matrix[IO, Boolean]  // Boolean is the type of the edge to keep the matrix small

def neighboursOut(graph: DGraph)(v:Int):IO[Vector[Long]] = {
 (for {
  shape <- Resource.liftF(g.shape)
  (_, cols) = shape
  selector <- Matrix.fromTuples[IO, Boolean](1, cols)(Array(0), Array(v), Array(true)) // set the starting point
  frontier <- Matrix[IO, Boolean](1, cols) // neighbours will be written here
  plus <- GrBMonoid[IO, Boolean](lor)
  semi <- GrBSemiring[IO, Boolean, Boolean, Boolean](plus, land)
 } yield (selector, frontier, semi)).use {
 case (select, frontier, semiring) => 
 MxM[IO].mxm(frontier)()(selector, graph)
 .map(out => out.extract)
 }
}

val graph = Matrix.fromTuples[IO, Boolean](7, 7)(
    Vector(0, 0, 1, 1, 2, 3, 3, 4, 5, 6, 6, 6),
    Vector(2, 3, 4, 6, 5, 0, 2, 5, 2, 2, 3, 4),
    (0 until 7).map(_ => true).toVector
)

```

