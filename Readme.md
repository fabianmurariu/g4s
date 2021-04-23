# FP Generic Graph Library

Highly experimental FP directed graph library built over GraphBLAS (C BLAS for graphs over sparse matrices)
All resources are managed and safely released at the end.

Able to solve non cyclical graph isomorphisms  (not optimized)


## Examples
1. Create a simple graph

```scala
import fix._

def graph: Resource[IO, ConcurrentDirectedGraph[IO, Vertex, Relation]] = ConcurrentDirectedGraph[IO, Vertex, Relation] 

```

2. Insert 1 edge and 2 vertices

```scala
    graph.use { g =>
      for {
        src <- g.insertVertex(new A)
        dst <- g.insertVertex(new B)
        _ <- g.insertEdge(src, dst, new X)
      } yield assertEquals((src, dst), (0L, 1L))
    }
```

3. insert node and get it back

```scala

    graph.use { g =>
      val av = new A
      for {
        a <- g.insertVertex(av)
        aOut <- g.getV(a)
      } yield assertEquals(aOut, Some(av))
    }

```

4. single edge path matching in a diamond graph

```scala

    val query = for {
      a <- node[A]
      b <- node[B]
      _ <- edge[X](a, b)
    } yield Ret(b)

    graph.use { g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(b1Node)
        b2 <- g.insertVertex(b2Node)
        c <- g.insertVertex(new C)
        _ <- g.insertEdge(a, b1, new X) // first edge matched by the query
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X) // second edge matched by the query
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet, Set[Vector[Vertex]](
            Vector(b1Node), 
            Vector(b2Node)))
      }
    }
```

5. 2 edge patch matching in a diamond graph

```scala
    val query = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(c)

    val cNode = new C

    graph.use { g =>
      for {
        a <- g.insertVertex(new A)
        b1 <- g.insertVertex(new B)
        b2 <- g.insertVertex(new B)
        // node returned at the end of the path (a)-[X]->(b)-[Y]->(c)
        c <- g.insertVertex(cNode) 
        _ <- g.insertEdge(a, b1, new X)
        _ <- g.insertEdge(b1, c, new Y)
        _ <- g.insertEdge(a, b2, new X)
        _ <- g.insertEdge(b2, c, new Y)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet, Set[Vector[Vertex]](Vector(cNode)))
      }
    }
```

6. find the top path in a diamond graph

```scala
    val queryTop = for {
      a <- node[A]
      b <- node[B]
      c <- node[C]
      _ <- edge[X](a, b)
      _ <- edge[Y](b, c)
    } yield Ret(a, b, c)

    val aNode = new A
    val b1Node = new B
    val b2Node = new B
    val cNode = new C

    graph.use { g =>
      for {
        a <- g.insertVertex(aNode)
        b1 <- g.insertVertex(b1Node)
        b2 <- g.insertVertex(b2Node)
        c <- g.insertVertex(cNode)
        _ <- g.insertEdge(a, b1, new X) // first part (a)-[X]->(b)
        _ <- g.insertEdge(b1, c, new Y) // second part (b)-[Y]->(c)
        _ <- g.insertEdge(a, b2, new Z)
        _ <- g.insertEdge(b2, c, new W)
        results <- g.resolveTraverser(query).compile.toList
      } yield {
        assertEquals(results.toSet,
                     Set[Vector[Vertex]](
                       Vector(aNode, b1Node, cNode)
                     ))
      }
    }

```