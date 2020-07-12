# Generic graph library

The aim of this project is to provide generic typeclasses able to abstarct over simple undirected graphs that can fit in memory as well as graphs with edges on GPUs or suprercomputers

## Efectful simple undirected graph

```scala
trait Graph[G[_, _], F[_]] { self =>

  def neighbours[V, E](fg: F[G[V, E]])(v: V): F[Iterable[(V, E)]]

  def vertices[V, E](g: F[G[V, E]]): F[Iterable[V]]

  def edgesTriples[V, E](g: F[G[V, E]]): F[Iterable[(V, E, V)]]

  def containsV[V, E](g: F[G[V, E]])(v: V): F[Boolean]

  def getEdge[V, E](g: F[G[V, E]])(v1: V, v2: V): F[Option[E]]

  def insertVertex[V, E](g: F[G[V, E]])(v: V): F[G[V, E]]

  def insertEdge[V, E](g: F[G[V, E]])(src: V, dst: V, e: E): F[G[V, E]]

  def removeVertex[V, E](g: F[G[V, E]])(v: V): F[G[V, E]]

  def removeEdge[V, E](g: F[G[V, E]])(src: V, dst: V): F[G[V, E]]

  def orderG[V, E](g: F[G[V, E]]): F[Int]

  def sizeG[V, E](g: F[G[V, E]]): F[Long]

  def degree[V, E](g: F[G[V, E]])(v: V): F[Long]
}
```

This graph can be represented as an AdjacencyMap or as a SparseMatrix relying on [GraphBLAS](http://graphblas.org/index.php?title=Graph_BLAS_Forum). 
GraphBLAS Is a new set of fundamental routines for graph algorithms expressed as linear algebra operations over various semirings. `(0, +, *)` is an example of such semiring.
It has several implementations some run on GPUs some run on supercomputers.

The list of things to-do 

* [x] Correct resource management, lift matrices into `cats.effect.Resource` or `zio.ZManaged`
* [ ] The project depends on [graphblas-java-native](https://github.com/fabianmurariu/graphblas-java-native) which is not published in maven central
* [ ] Make `graphblas-java-native` throw exceptions on GrB failures
* [ ] Devise a decent query DLS
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
