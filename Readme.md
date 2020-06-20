# Graph library built on top of GraphBLAS

[GraphBLAS](http://graphblas.org/index.php?title=Graph_BLAS_Forum) Is a new set of fundamental routines for graph algorithms expressed as linear algebra operations over various semirings. `(0, +, *)` is an example of such semiring.

This project attempts to elevate the basic operations from the low level C routines to the high level expresivity of a usual Graph library like [Apache Tinkerpop](https://tinkerpop.apache.org/).

The operations over the graph are expressed (for now) as a free algebra and are then interpreted as matrix multiplications or element wise unions. 

Some examples:

```scala

val gq = for {
    michael <- createNode("Michael", "Manager")
    jennifer <- createNode("Jennifer", "Employee")
    msft <- createNode("Microsoft")
    games <- createNode("Games")
    gym <- createNode("Gym")
    _ <- createEdge(jennifer, "is_friend", michael)
    _ <- createEdge(jennifer, "likes", games)
    _ <- createEdge(jennifer, "likes", gym)
    _ <- createEdge(jennifer, "works_for", msft)
    res <- query(vs.out("likes").v("Games"))
} yield res

val actual = evalQuery(gq)

actual shouldBe VerticesRes(
    Vector(
        3L -> Set("Games")
    )
)

```

There is no persistence (yet), everything is in memory

```scala

// traverse edges of type "is_friend" or "works_for" and return the other vertex

val gq = for {
    michael <- createNode("Michael", "Manager")
    jennifer <- createNode("Jennifer", "Employee")
    msft <- createNode("Microsoft")
    games <- createNode("Games")
    gym <- createNode("Gym")
    _ <- createEdge(jennifer, "is_friend", michael)
    _ <- createEdge(jennifer, "likes", games)
    _ <- createEdge(jennifer, "likes", gym)
    _ <- createEdge(jennifer, "works_for", msft)
    res <- query(vs.out("is_friend", "works_for").v()) // is_friend OR works_for
} yield res

val actual = evalQuery(gq)

actual shouldBe VerticesRes(
    Vector(
      0L -> Set("Michael", "Person"),
      2L -> Set("Microsoft")
    )
)

```

## Typeclasses (subject to change)

```

@typeclass trait MatrixLike[M[_]] {
  def nvals[A](f: M[A]): Long
  def nrows[A](f: M[A]): Long
  def ncols[A](f: M[A]): Long
  def clear[A](f: M[A]): Unit
  def resize[A](f: M[A])(rows: Long, cols: Long): Unit

  def get[A](f: M[A])(i: Long, j: Long)
         (implicit MH: MatrixHandler[M, A]): Option[A] = {
    MH.get(f)(i, j)
  }

  def set[@sp(Boolean, Byte, Short, Int, Long, Float, Double) A](f: M[A])
         (i: Long, j: Long, a: A)
         (implicit MH: MatrixHandler[M, A]): Unit = {
    MH.set(f)(i, j, a)
  }

  def duplicate[A](f: M[A]): Managed[Throwable, M[A]]
  def make[A:MatrixBuilder](rows:Long, cols:Long):Managed[Throwable, M[A]]
  // for the special case when you want to manage your own resources see GraphDB
  def makeUnsafe[A:MatrixBuilder](rows:Long, cols:Long): Task[M[A]]

}

```

The list of things to-do is daunting but here are some 

* [x] Correct resource management, lift matrices into `cats.effect.Resource` or `zio.ZManaged`
* [ ] The project depends on [graphblas-java-native](https://github.com/fabianmurariu/graphblas-java-native) which is not published in maven central
* [ ] Make `graphblas-java-native` throw exceptions on GrB failures
* [ ] Make the interpreter return chunks and stream the results via `Observable` or `fs2.Stream`
* [ ] Add functionality to load nodes and edges from CSV files
* [ ] Make each function and class type complete with respect to resource management, failure, effects and mutability
* [ ] Return intermediate results from the query
* [ ] Return full path of the query not just the last predicate
* [ ] Refactor the typeclasses to reduce the number of implicits required (preferably down to zero)
* [ ] add predicate to limit the results
* [ ] add simple algorithms such as BFS and single source shortest path
* [ ] add many other algorithms
* [ ] add support for multiple threads
* [ ] add support for transactions (STM)
* [ ] add persistence
* [ ] add ...
