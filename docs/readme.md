# Matrices as graphs

To install my project
```scala
libraryDependencies += "com.fabianmurariu" % "g4s" % "0.1.0-SNAPSHOT"
```

```scala mdoc
import com.github.fabianmurariu.g4s.sparse.grbv2._
import com.github.fabianmurariu.g4s.sparse.grb._
import cats.effect._
// a long typed empty matrix
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
